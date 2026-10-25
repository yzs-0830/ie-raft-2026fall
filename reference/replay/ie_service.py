"""Event/persistence layer around IERaftNode (shared by the HTTP server and the in-process test backend)."""
import copy
import json
import os

from ie_node import IERaftNode


def _norm_msg(m):
    m = dict(m)
    if "level_table" in m:
        m["level_table"] = {int(k): v for k, v in m["level_table"].items()}
    return m


class NodeService:
    def __init__(self, data_dir=None, node_cls=None):
        self.cls = node_cls or IERaftNode
        self.dir = data_dir
        self.node = None
        self.cfg = None
        self._mem = {}          # in-memory "volume" when data_dir is None
        if self.dir:
            self.load()

    # ---- storage ("the volume")
    def _write(self, name, obj):
        if not self.dir:
            self._mem[name] = copy.deepcopy(obj)
            return
        os.makedirs(self.dir, exist_ok=True)
        tmp = os.path.join(self.dir, name + ".tmp")
        with open(tmp, "w") as f:
            json.dump(obj, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, os.path.join(self.dir, name))

    def _read(self, name):
        if not self.dir:
            return copy.deepcopy(self._mem.get(name))
        try:
            with open(os.path.join(self.dir, name)) as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    def _make(self, cfg):
        view = {int(k): v for k, v in (cfg.get("initial_view") or {}).items()}
        return self.cls(cfg["node_id"], cfg["peers"], cfg["initial_level"], view,
                        cfg.get("commit_mode", "A"), prevote=bool(cfg.get("prevote", False)))

    def _save_state(self):
        self._write("state.json", self.node.persistent_state())

    # ---- lifecycle
    def load(self):
        """Container start: if config and state exist, rebuild the node and apply the restart rules."""
        cfg, st = self._read("config.json"), self._read("state.json")
        if cfg and st:
            self.cfg = cfg
            self.node = self._make(cfg)
            self.node.restart(st)

    def crash_restart(self):
        self.node = None
        self.load()

    def init(self, cfg):
        self.cfg = cfg
        self.node = self._make(cfg)
        self._write("config.json", cfg)
        self._save_state()

    # ---- events
    def event(self, ev):
        nd = self.node
        if nd is None:
            raise RuntimeError("not initialised")
        k = ev["kind"]
        if k == "election_timeout":
            out = nd.handle_election_timeout()
        elif k == "heartbeat_timeout":
            out = nd.handle_heartbeat_timeout()
        elif k == "client_request":
            out = nd.handle_client_request(ev["command"])
        elif k == "message":
            m = _norm_msg(ev["message"])
            t = m["type"]
            if t == "RequestVote": out = [nd.handle_request_vote(m)]
            elif t == "RequestVoteReply": out = nd.handle_request_vote_response(m)
            elif t == "AppendEntries": out = [nd.handle_append_entries(m)]
            elif t == "AppendEntriesReply": out = nd.handle_append_entries_response(m)
            else: raise ValueError(f"unknown message type {t}")
        else:
            raise ValueError(f"unknown event kind {k}")
        self._save_state()                      # persist before answering
        out = sorted(out, key=lambda m: m["to"])
        return json.loads(json.dumps(out))

    def state(self):
        return json.loads(json.dumps(self.node.get_state()))
