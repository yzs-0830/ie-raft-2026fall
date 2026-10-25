"""Reference node logic: the base (intentionally flawed) IE-Raft of the handout."""
import copy


class IERaftNode:
    FREE = 1   # number of free timeouts per epoch (spec: 1)

    def __init__(self, node_id, peers, initial_level=1, initial_view=None, commit_mode="A", n_total=None, prevote=False):
        self.node_id = node_id
        self.peers = list(peers)
        self.all_ids = sorted(self.peers + [node_id])
        self.current_term = 0
        self.voted_for = None
        self.log = []                      # [{"index","term","command"}]
        self.commit_index = 0
        self.last_applied = 0
        self.role = "Follower"
        self.leader_id = None
        self.state_machine = []
        self.next_index = {}
        self.match_index = {}
        self.votes_granted = {}
        self.level = initial_level
        self.level_view = dict(initial_view or {})
        self.level_view[node_id] = self.level
        self.timeouts_in_epoch = 0
        self.last_leader_term = 0
        self.commit_mode = commit_mode
        if prevote:
            raise ValueError("Pre-Vote is not part of this reference release")

    # ---- helpers ----
    def _view(self, j):
        return self.level if j == self.node_id else self.level_view.get(j, 1)

    def _set_level(self, v):
        self.level = v
        self.level_view[self.node_id] = v

    def _w_total(self):
        return sum(self._view(j) for j in self.all_ids)

    def _q(self):
        return self._w_total() // 2 + 1

    def _last(self):
        if self.log:
            return self.log[-1]["index"], self.log[-1]["term"]
        return 0, 0

    def _ae_for(self, j, heartbeat=True):
        ni = self.next_index.get(j, len(self.log) + 1)
        prev_i = ni - 1
        prev_t = self.log[prev_i - 1]["term"] if prev_i >= 1 else 0
        return {"type": "AppendEntries", "from": self.node_id, "to": j, "term": self.current_term,
                "leader_id": self.node_id, "prev_log_index": prev_i, "prev_log_term": prev_t,
                "entries": [dict(e) for e in self.log[ni - 1:]], "leader_commit": self.commit_index,
                "level_table": {k: (self.level if k == self.node_id else v) for k, v in self.level_view.items()}}

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.state_machine.append(self.log[self.last_applied - 1]["command"])

    # ---- election ----
    def handle_election_timeout(self):
        if self.role == "Leader":
            return []
        self.timeouts_in_epoch += 1
        if self.timeouts_in_epoch > self.FREE:
            self._set_level(max(1, self.level - 1))
        return self._start_election()

    def _start_election(self):
        self.role = "Candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_granted = {self.node_id: self.level}
        self.leader_id = None
        if sum(self.votes_granted.values()) >= self._q():
            return self._become_leader()
        li, lt = self._last()
        return [{"type": "RequestVote", "from": self.node_id, "to": j, "term": self.current_term,
                 "candidate_id": self.node_id, "last_log_index": li, "last_log_term": lt}
                for j in self.peers]

    def handle_request_vote(self, args):
        if args["term"] > self.current_term:
            self.current_term = args["term"]
            self.voted_for = None
            self.role = "Follower"
            self.leader_id = None
        granted = False
        if args["term"] >= self.current_term:
            li, lt = self._last()
            up = (args["last_log_term"], args["last_log_index"]) >= (lt, li)
            if self.voted_for in (None, args["candidate_id"]) and up:
                granted = True
                self.voted_for = args["candidate_id"]
        return {"type": "RequestVoteReply", "from": self.node_id, "to": args["candidate_id"],
                "term": self.current_term, "vote_granted": granted, "level": self.level}

    def handle_request_vote_response(self, resp):
        if resp["term"] > self.current_term:
            self.current_term = resp["term"]
            self.voted_for = None
            self.role = "Follower"
            return []
        if self.role != "Candidate" or resp["term"] != self.current_term:
            return []
        if resp["vote_granted"]:
            self.votes_granted[resp["from"]] = resp["level"]
            if sum(self.votes_granted.values()) >= self._q():
                return self._become_leader()
        return []

    def _become_leader(self):
        self.role = "Leader"
        self.leader_id = self.node_id
        self.next_index = {j: len(self.log) + 1 for j in self.peers}
        self.match_index = {j: 0 for j in self.peers}
        T = self.current_term
        for j, lv in self.votes_granted.items():      # the new Leader collects the voters' self-reported levels
            if j != self.node_id:
                self.level_view[j] = lv
        # 2.3 settlement for the winner (self-vote counts)
        if T > self.last_leader_term:
            if self.timeouts_in_epoch <= self.FREE:
                self._set_level(min(3, self.level + 1))
            self.timeouts_in_epoch = 0
            self.last_leader_term = T
        return [self._ae_for(j) for j in self.peers]

    # ---- replication ----
    def handle_append_entries(self, args):
        def reply(success, match=0):
            return {"type": "AppendEntriesReply", "from": self.node_id, "to": args["leader_id"],
                    "term": self.current_term, "success": success, "match_index": match, "level": self.level}
        T = args["term"]
        if T < self.current_term:
            return reply(False)
        if self.role == "Leader" and T == self.current_term:
            return reply(False)
        if T - self.current_term >= 2:
            self._set_level(1)
        voted_for_leader = (self.current_term == T and self.voted_for == args["leader_id"])
        if T > self.current_term:
            self.current_term = T
            self.voted_for = None
        self.role = "Follower"
        self.leader_id = args["leader_id"]
        if T > self.last_leader_term:
            if voted_for_leader and self.timeouts_in_epoch <= self.FREE:
                self._set_level(min(3, self.level + 1))
            self.timeouts_in_epoch = 0
            self.last_leader_term = T
        self.level_view = dict(args["level_table"])      # the Leader's table replaces the local view
        self.level_view[self.node_id] = self.level
        pi = args["prev_log_index"]
        if pi > 0 and (len(self.log) < pi or self.log[pi - 1]["term"] != args["prev_log_term"]):
            return reply(False)
        idx = pi
        for e in args["entries"]:
            idx += 1
            if len(self.log) >= idx:
                if self.log[idx - 1]["term"] != e["term"]:
                    del self.log[idx - 1:]
                    self.log.append(dict(e))
            else:
                self.log.append(dict(e))
        if args["leader_commit"] > self.commit_index:
            self.commit_index = min(args["leader_commit"], idx)
            self._apply()
        return reply(True, idx)

    def handle_append_entries_response(self, resp):
        if resp["term"] > self.current_term:
            self.current_term = resp["term"]
            self.voted_for = None
            self.role = "Follower"
            return []
        if self.role != "Leader" or resp["term"] != self.current_term:
            return []
        j = resp["from"]
        self.level_view[j] = resp["level"]
        out = []
        if resp["success"]:
            self.match_index[j] = max(self.match_index[j], resp["match_index"])
            self.next_index[j] = self.match_index[j] + 1
            self._advance_commit()
            if self.next_index[j] <= len(self.log):
                out.append(self._ae_for(j))
        else:
            self.next_index[j] = max(1, self.next_index[j] - 1)
            out.append(self._ae_for(j))
        return out

    def _advance_commit(self):
        for N in range(len(self.log), self.commit_index, -1):
            if self.log[N - 1]["term"] != self.current_term:
                continue
            S = [self.node_id] + [j for j in self.peers if self.match_index[j] >= N]
            if self.commit_mode == "A":
                ok = sum(self._view(j) for j in S) >= self._q()
            else:
                ok = len(S) * 2 > len(self.all_ids)
            if ok:
                self.commit_index = N
                self._apply()
                break

    def handle_heartbeat_timeout(self):
        if self.role != "Leader":
            return []
        return [self._ae_for(j) for j in self.peers]

    def handle_client_request(self, command):
        if self.role != "Leader":
            return []
        self.log.append({"index": len(self.log) + 1, "term": self.current_term, "command": command})
        return [self._ae_for(j) for j in self.peers]

    def persistent_state(self):
        return {"current_term": self.current_term, "voted_for": self.voted_for, "log": copy.deepcopy(self.log)}

    def restart(self, persisted):
        self.current_term = persisted["current_term"]
        self.voted_for = persisted["voted_for"]
        self.log = copy.deepcopy(persisted["log"])
        self.role = "Follower"
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.state_machine = []
        self._set_level(1)
        self.level_view = {self.node_id: 1}
        self.timeouts_in_epoch = 0
        self.last_leader_term = self.current_term

    def get_state(self):
        return {"id": self.node_id, "term": self.current_term, "role": self.role, "level": self.level,
                "view": dict(self.level_view), "log": [(e["index"], e["term"], e["command"]) for e in self.log],
                "commit": self.commit_index, "last_applied": self.last_applied, "voted_for": self.voted_for,
                "timeouts": self.timeouts_in_epoch, "last_leader_term": self.last_leader_term}
