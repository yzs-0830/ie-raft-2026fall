"""Scripted-replay harness for IE-Raft (course-provided; do not edit).

A Cluster drives n node processes through the scripted-mode HTTP API (handout section 5.1),
numbers the messages they return, delivers or drops them as the script says, and checks the
properties of Raft Figure 3 after every operation.
"""
import http.client
import json
import socket
import time
import urllib.request


class ScriptError(Exception):
    """The script itself is invalid (for example it delivers a message number that does not exist)."""


# ---------------------------------------------------------------- node proxies
class HttpNode:
    """One node container, reached over HTTP; crash/restart go through the Docker Engine API."""

    def __init__(self, i, host, docker=None, name=None, timeout=10):
        self.i, self.host, self.docker, self.name, self.timeout = i, host, docker, name or f"node{i}", timeout

    def _call(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(f"http://{self.host}{path}", data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read() or b"null")

    def wait_ready(self, tries=100):
        for _ in range(tries):
            try:
                self._call("GET", "/health")
                return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError(f"node {self.i} does not answer /health")

    def init(self, body):
        self._call("POST", "/init", body)

    def event(self, ev):
        return self._call("POST", "/event", ev)["out"]

    def state(self):
        return self._call("GET", "/state")

    def crash(self):
        self.docker.kill(self.name)

    def restart(self):
        self.docker.start(self.name)
        self.wait_ready()


class _UnixConn(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost")
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.path)


class Docker:
    """Minimal Docker Engine API client (kill / start a container by name)."""

    def __init__(self, sock="/var/run/docker.sock"):
        self.sock = sock

    def _post(self, path):
        c = _UnixConn(self.sock)
        c.request("POST", path)
        r = c.getresponse()
        body = r.read()
        c.close()
        if r.status not in (200, 204, 304):
            raise RuntimeError(f"docker {path}: {r.status} {body[:200]!r}")

    def kill(self, name):
        self._post(f"/containers/{name}/kill")

    def start(self, name):
        self._post(f"/containers/{name}/start")


# ---------------------------------------------------------------- the cluster
class Cluster:
    def __init__(self, nodes, config, trace=None):
        self.nodes = nodes                       # {id: node proxy}
        self.n = len(nodes)
        self.cfg = config
        self.trace = trace or (lambda s: None)
        self.pending = {}
        self.mid = 0
        self.down = set()
        self.snap = {}                           # last known state of every node
        self.leaders_by_term = {}
        self.committed = {}
        self.applied_at = {}
        self.violations = []
        self.opno = 0

    # -- setup
    def init_nodes(self):
        levels = self.cfg["levels"]
        ids = list(range(self.n))
        views = self.cfg.get("views", {})
        for i in ids:
            self.nodes[i].init({"node_id": i, "peers": [j for j in ids if j != i],
                                "initial_level": levels[i],
                                "initial_view": {str(k): v for k, v in views.get(str(i), views.get(i, {})).items()},
                                "commit_mode": self.cfg.get("commit_mode", "A"),
                                "prevote": bool(self.cfg.get("prevote", False))})
        self._refresh()

    def _refresh(self):
        for i, nd in self.nodes.items():
            if i not in self.down:
                self.snap[i] = nd.state()

    # -- message queue
    def _enq(self, msgs):
        ids = []
        for m in msgs:
            self.mid += 1
            self.pending[self.mid] = m
            ids.append(self.mid)
        return ids

    def _send(self, i, ev):
        if i in self.down:
            raise ScriptError(f"op {self.opno}: node {i} is down")
        out = self.nodes[i].event(ev)
        tos = [m["to"] for m in out]
        if tos != sorted(tos):
            raise ScriptError(f"op {self.opno}: node {i} returned messages that are not sorted by target id: {tos}")
        return out

    # -- operations
    def timeout(self, i):
        ids = self._enq(self._send(i, {"kind": "election_timeout"}))
        self._after(f"timeout({i})")
        return ids

    def heartbeat(self, i):
        ids = self._enq(self._send(i, {"kind": "heartbeat_timeout"}))
        self._after(f"heartbeat({i})")
        return ids

    def client(self, i, cmd):
        ids = self._enq(self._send(i, {"kind": "client_request", "command": cmd}))
        self._after(f"client({i},{cmd!r})")
        return ids

    def deliver(self, mid):
        if mid not in self.pending:
            raise ScriptError(f"op {self.opno}: message {mid} is not pending (numbering differs from the reference "
                              f"or the message was already delivered/dropped)")
        m = self.pending.pop(mid)
        self._enq(self._send(m["to"], {"kind": "message", "message": m}))
        self._after(f"deliver({m['type']} {m['from']}->{m['to']})")

    def drop(self, mid):
        self.pending.pop(mid, None)

    def crash(self, i):
        if i in self.down:
            raise ScriptError(f"op {self.opno}: node {i} is already down")
        self.snap[i] = self.nodes[i].state()
        self.nodes[i].crash()
        self.down.add(i)
        for k in [k for k, m in self.pending.items() if m["to"] == i]:
            del self.pending[k]
        self._after(f"crash({i})")

    def restart(self, i):
        if i not in self.down:
            raise ScriptError(f"op {self.opno}: node {i} is not down")
        self.nodes[i].restart()
        self.down.discard(i)
        self._after(f"restart({i})")

    # -- invariant checking (Raft Figure 3 + level range)
    def _viol(self, msg):
        if msg not in self.violations:
            self.violations.append(msg)

    @staticmethod
    def _ent(s, idx):
        e = s["log"][idx - 1]
        return e[1], e[2]                         # (term, command)

    def _after(self, label):
        self._refresh()
        S = self.snap
        for i, s in S.items():
            if s["role"] == "Leader":
                prev = self.leaders_by_term.setdefault(s["term"], set())
                if i not in prev:
                    for idx, (t, cmd) in self.committed.items():
                        if t < s["term"] and (len(s["log"]) < idx or self._ent(s, idx) != (t, cmd)):
                            self._viol(f"LeaderCompleteness: node {i} became leader of term {s['term']} without "
                                       f"committed entry {idx}:({t},{cmd!r})  [after {label}]")
                    prev.add(i)
                    if len(prev) > 1:
                        self._viol(f"ElectionSafety: term {s['term']} has leaders {sorted(prev)}  [after {label}]")
        for i, s in S.items():
            for idx in range(1, s["commit"] + 1):
                key = self._ent(s, idx)
                if idx not in self.committed:
                    self.committed[idx] = key
                elif self.committed[idx] != key:
                    self._viol(f"StateMachineSafety: index {idx} committed as {self.committed[idx]} and {key}  [after {label}]")
        for i, s in S.items():
            for idx in range(1, min(s["last_applied"], len(s["log"])) + 1):
                key = self._ent(s, idx)
                if idx in self.applied_at and self.applied_at[idx] != key:
                    self._viol(f"StateMachineSafety(applied): index {idx} applied as {self.applied_at[idx]} and {key}  [after {label}]")
                self.applied_at.setdefault(idx, key)
        ids = sorted(S)
        for a in range(len(ids)):
            for b in range(a + 1, len(ids)):
                la, lb = S[ids[a]]["log"], S[ids[b]]["log"]
                for k in range(min(len(la), len(lb)) - 1, -1, -1):
                    if la[k][1] == lb[k][1]:
                        if la[k][2] != lb[k][2] or la[:k + 1] != lb[:k + 1]:
                            self._viol(f"LogMatching: nodes {ids[a]},{ids[b]} at index {k+1}  [after {label}]")
                        break
        for i, s in S.items():
            if s["level"] not in (1, 2, 3):
                self._viol(f"level out of range at {i}")

    # -- running a script
    def run(self, ops):
        for self.opno, op in enumerate(ops):
            k = op["op"]
            if k == "timeout": ids = self.timeout(op["node"])
            elif k == "heartbeat": ids = self.heartbeat(op["node"])
            elif k == "client": ids = self.client(op["node"], op["command"])
            elif k == "deliver":
                before = self.mid; self.deliver(op["id"]); ids = list(range(before + 1, self.mid + 1))
            elif k == "drop": self.drop(op["id"]); ids = []
            elif k == "crash": self.crash(op["node"]); ids = []
            elif k == "restart": self.restart(op["node"]); ids = []
            else: raise ScriptError(f"op {self.opno}: unknown op {k!r}")
            self.trace(f"{self.opno:4d} {k:9s} {self._opdesc(op):22s} new messages: "
                       + (", ".join(f"#{j} {self.pending[j]['type']} {self.pending[j]['from']}->{self.pending[j]['to']}"
                                    for j in ids if j in self.pending) or "-"))
        return self.result()

    @staticmethod
    def _opdesc(op):
        return " ".join(f"{k}={v}" for k, v in op.items() if k not in ("op", "msg"))

    def result(self):
        self._refresh()
        S = self.snap
        return {"violations": list(self.violations),
                "final": [{"id": i, "term": S[i]["term"], "role": S[i]["role"], "level": S[i]["level"],
                           "commit": S[i]["commit"], "voted_for": S[i]["voted_for"], "log": S[i]["log"]}
                          for i in sorted(S)],
                "leaders": [i for i in sorted(S) if S[i]["role"] == "Leader"],
                "levels": [S[i]["level"] for i in sorted(S)]}


def check_expect(expect, res):
    """Compare a result with a script's `expect` block. Returns (ok, [reasons])."""
    why = []
    names = [v.split(":")[0] for v in res["violations"]]
    for e in expect.get("violations", []):
        if e not in names:
            why.append(f"expected violation {e} did not occur")
    if expect.get("no_violation") and res["violations"]:
        why.append(f"unexpected violation: {res['violations'][0]}")
    if "final_levels" in expect and res["levels"] != expect["final_levels"]:
        why.append(f"final levels {res['levels']} != expected {expect['final_levels']}")
    if "leaders" in expect and res["leaders"] != expect["leaders"]:
        why.append(f"leaders {res['leaders']} != expected {expect['leaders']}")
    if "final" in expect:
        for want, got in zip(expect["final"], res["final"]):
            for k, v in want.items():
                if k == "log":
                    v = [list(e) for e in v]
                if got.get(k) != v:
                    why.append(f"node {want.get('id', got['id'])}: {k} = {got.get(k)!r}, expected {v!r}")
    return (not why), why
