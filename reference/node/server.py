"""Reference IE-Raft node: scripted-mode HTTP API (handout 5.1) and live mode (5.3).  Standard library only."""
import json
import os
import random
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ie_service import NodeService

DATA = os.environ.get("DATA_DIR", "/data")
MODE = os.environ.get("MODE", "scripted")
svc = NodeService(DATA)
lock = threading.RLock()


# ======================================================================= live mode
class Live:
    def __init__(self):
        e = os.environ
        self.id = int(e["NODE_ID"])
        self.addr = {int(k): v for k, v in (p.split("=", 1) for p in e["PEERS"].split(","))}
        self.hb = float(e.get("HEARTBEAT_MS", "50")) / 1000
        self.el = (float(e.get("ELECTION_MIN_MS", "300")) / 1000, float(e.get("ELECTION_MAX_MS", "600")) / 1000)
        if svc.node is None:
            svc.init({"node_id": self.id, "peers": sorted(j for j in self.addr if j != self.id),
                      "initial_level": int(e.get("INITIAL_LEVEL", "1")), "initial_view": {},
                      "commit_mode": e.get("COMMIT_MODE", "A"), "prevote": e.get("PREVOTE", "0") == "1"})
        self.pool = ThreadPoolExecutor(max_workers=16)
        self.el_deadline = time.monotonic() + random.uniform(*self.el)
        self.hb_deadline = 0.0
        self.last = self._snap()
        self.log("start")
        threading.Thread(target=self._ticker, daemon=True).start()

    # -- logging
    def _snap(self):
        n = svc.node
        return {"role": n.role, "term": n.current_term, "level": n.level, "view": dict(n.level_view),
                "commit": n.commit_index, "applied": n.last_applied}

    def log(self, event, **kw):
        n = svc.node
        d = {"t": int(time.time() * 1000), "node": self.id, "event": event, "term": n.current_term,
             "role": n.role, "level": n.level}
        d.update(kw)
        print(json.dumps(d), flush=True)

    def _diff(self):
        a, b, n = self.last, self._snap(), svc.node
        if b["role"] != a["role"]: self.log("role_change", old=a["role"])
        if b["term"] != a["term"]: self.log("term_change", old=a["term"])
        if b["level"] != a["level"]: self.log("level_change", old=a["level"])
        if b["view"] != a["view"]: self.log("view_change", view={str(k): v for k, v in sorted(b["view"].items())})
        for i in range(a["commit"] + 1, b["commit"] + 1):
            self.log("commit", index=i, command=n.log[i - 1]["command"])
        for i in range(a["applied"] + 1, b["applied"] + 1):
            self.log("apply", index=i, command=n.log[i - 1]["command"])
        self.last = b

    # -- one event, under the lock
    def handle(self, ev):
        with lock:
            was_leader = svc.node.role == "Leader"
            out = svc.event(ev)
            self._diff()
            n = svc.node
            k = ev["kind"]
            if k == "election_timeout":
                self._reset()
            elif k == "message":
                m = ev["message"]
                if m["type"] == "RequestVote" and out and out[0].get("vote_granted"):
                    self.log("vote_granted", candidate=m["candidate_id"])
                    self._reset()
                elif m["type"] == "AppendEntries" and out and out[0]["term"] == m["term"]:
                    self._reset()
            if was_leader and n.role != "Leader":
                self._reset()
        return out

    def _reset(self):
        self.el_deadline = time.monotonic() + random.uniform(*self.el)

    # -- network
    def dispatch(self, out):
        for m in out:
            if not m["type"].endswith("Reply"):
                self.pool.submit(self._send, m)

    def _send(self, m):
        try:
            req = urllib.request.Request(self.addr[m["to"]] + "/message", data=json.dumps(m).encode(),
                                         headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=1.0) as r:
                replies = json.loads(r.read())["out"]
        except Exception:
            return                                       # lost message
        for rep in replies:
            self.dispatch(self.handle({"kind": "message", "message": rep}))

    def client(self, command):
        with lock:
            if svc.node.role != "Leader":
                return {"accepted": False, "leader": svc.node.leader_id}
        out = self.handle({"kind": "client_request", "command": command})
        self.dispatch(out)
        return {"accepted": True, "leader": self.id}

    def _ticker(self):
        while True:
            time.sleep(0.005)
            now = time.monotonic()
            with lock:
                leader = svc.node.role == "Leader"
                due_hb = leader and now >= self.hb_deadline
                due_el = (not leader) and now >= self.el_deadline
                if due_hb:
                    self.hb_deadline = now + self.hb
            if due_hb:
                self.dispatch(self.handle({"kind": "heartbeat_timeout"}))
            elif due_el:
                self.dispatch(self.handle({"kind": "election_timeout"}))


live = None


# ======================================================================= HTTP
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _reply(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/health":
            return self._reply(200 if svc.node is not None or MODE == "scripted" else 503, {"ok": True})
        if self.path == "/state":
            with lock:
                if svc.node is None:
                    return self._reply(409, {"error": "not initialised"})
                return self._reply(200, svc.state())
        self._reply(404, {})

    def do_POST(self):
        try:
            body = self._body()
            if self.path == "/init" and MODE == "scripted":
                with lock:
                    svc.init(body)
                return self._reply(200, {"ok": True})
            if self.path == "/event" and MODE == "scripted":
                with lock:
                    if svc.node is None:
                        return self._reply(409, {"error": "not initialised"})
                    return self._reply(200, {"out": svc.event(body)})
            if self.path == "/message" and MODE == "live":
                out = live.handle({"kind": "message", "message": body})
                live.dispatch(out)
                return self._reply(200, {"out": [m for m in out if m["type"].endswith("Reply")]})
            if self.path == "/client" and MODE == "live":
                return self._reply(200, live.client(body["command"]))
            self._reply(404, {})
        except Exception as e:
            self._reply(500, {"error": repr(e)})


def main():
    global live
    if MODE == "live":
        live = Live()
    ThreadingHTTPServer(("0.0.0.0", 8000), H).serve_forever()


if __name__ == "__main__":
    main()
