"""Replay attack scripts on the reference implementation and apply the grader's required checks.
usage: python replay_ref.py <script.json | directory> ... [--pattern 'p*.json'] [--trace]"""
import glob
import json
import os
import sys

from ieharness import Cluster, ScriptError, check_expect
from inproc import InProcNode
from ie_node import IERaftNode


def start_problems(cfg):
    """Starting-state rule: every initial view equals the true initial levels (missing entries count as 1)."""
    lv = cfg["levels"]
    n = len(lv)
    views = {int(i): {int(k): v for k, v in vv.items()} for i, vv in cfg.get("views", {}).items()}
    bad = []
    for i in range(n):
        for j in range(n):
            v = lv[i] if j == i else views.get(i, {}).get(j, 1)
            if v != lv[j]:
                bad.append(f"view_{i}[{j}] = {v} but level_{j} = {lv[j]}")
    return bad


def req_p4_1(n):
    def check(sc, res, cl):
        why = []
        cfg = sc["config"]
        if len(cfg["levels"]) != n: why.append(f"must use n = {n} nodes")
        if any(l != 1 for l in cfg["levels"]) or any(cfg.get("views", {}).values()):
            why.append("P4-1 must start from all levels 1 and the default views")
        if not any(v.startswith("ElectionSafety") for v in res["violations"]): why.append("ElectionSafety was not violated")
        return why
    return check


def req_p4_2a(sc, res, cl):
    why = []
    cfg = sc["config"]
    if cfg["levels"] != [3, 3, 1, 1, 1]: why.append("levels must be [3,3,1,1,1]")
    if cfg.get("commit_mode") != "B": why.append('commit_mode must be "B"')
    if not any(v.startswith("LeaderCompleteness") for v in res["violations"]): why.append("LeaderCompleteness was not violated")
    return why


def req_p5(sc, res, cl):
    why = []
    cfg = sc["config"]
    if cfg.get("prevote"): why.append("p5.json must use prevote: false")
    if len(cfg["levels"]) != 5: why.append("must use five nodes")
    if res["violations"]: why.append(f"a safety property was violated: {res['violations'][0]}")
    first = cl.leaders_by_term[min(cl.leaders_by_term)] if cl.leaders_by_term else set()
    if not first: why.append("no Leader was ever elected")
    elif set(res["leaders"]) & first: why.append(f"the original Leader {sorted(first)} is still Leader at the end")
    return why


def _live_leaders(res, cl):
    return [i for i in res["leaders"] if i not in cl.down]      # a crashed node's last snapshot does not count


def req_p3(sc, res, cl):
    """Bonus P3: five nodes at level 3, nodes 3 and 4 crash for good, at least 10 more election timeouts, no Leader at the end."""
    why = []
    cfg, ops = sc["config"], sc["ops"]
    if cfg["levels"] != [3, 3, 3, 3, 3]: why.append("levels must be [3,3,3,3,3]")
    crashed = {o["node"] for o in ops if o["op"] == "crash"}
    if not {3, 4} <= crashed: why.append("nodes 3 and 4 must crash")
    if crashed - {3, 4}: why.append("only nodes 3 and 4 may crash")
    if any(o["op"] == "restart" for o in ops): why.append("crashed nodes must stay down (no restart)")
    last = max((k for k, o in enumerate(ops) if o["op"] == "crash"), default=-1)
    rounds = sum(1 for o in ops[last + 1:] if o["op"] == "timeout")
    if rounds < 10: why.append(f"only {rounds} election timeouts after the crashes (need at least 10)")
    if res["violations"]: why.append(f"a safety property was violated: {res['violations'][0]}")
    if _live_leaders(res, cl): why.append(f"a Leader exists at the end: {_live_leaders(res, cl)}")
    return why


def req_leader_completeness_variant_a(sc, res, cl):
    """Bonus P4-2(b) and the open problem: LeaderCompleteness is violated under commit variant A."""
    why = []
    if sc["config"].get("commit_mode", "A") != "A": why.append('commit_mode must be "A" (weighted commit)')
    if not any(v.startswith("LeaderCompleteness") for v in res["violations"]): why.append("LeaderCompleteness was not violated")
    return why


def req_p6(sc, res, cl):
    """Bonus P6: only delays and drops of messages; a node that starts above level 1 ends at level 1; nothing is violated."""
    why = []
    cfg = sc["config"]
    if any(o["op"] in ("crash", "restart") for o in sc["ops"]): why.append("P6 may only delay messages: no crash or restart")
    if res["violations"]: why.append(f"a safety property was violated: {res['violations'][0]}")
    if not any(a > 1 and b == 1 for a, b in zip(cfg["levels"], res["levels"])):
        why.append("no node that started above level 1 ended at level 1")
    return why


REQUIRED = {"p4_1_n5": req_p4_1(5), "p4_1_n7": req_p4_1(7), "p4_2a": req_p4_2a, "p5": req_p5,
            "p3": req_p3, "p4_2b": req_leader_completeness_variant_a, "open_problem": req_leader_completeness_variant_a,
            "p6": req_p6}


def run_one(path, trace):
    name = os.path.basename(path)
    stem = name[:-5]
    sc = json.load(open(path))
    if sc["config"].get("prevote") and not hasattr(IERaftNode, "handle_prevote"):
        print(f"SKIP  {name}: Pre-Vote scripts run on your own node (this reference has no Pre-Vote)")
        return None
    sp = start_problems(sc["config"])
    if sp:
        print(f"FAIL  {name}: illegal starting state: {sp[0]}" + (f" (+{len(sp)-1} more)" if len(sp) > 1 else ""))
        return False
    n = len(sc["config"]["levels"])
    cl = Cluster({i: InProcNode(i) for i in range(n)}, sc["config"], print if trace else None)
    try:
        cl.init_nodes()
        res = cl.run(sc["ops"])
    except ScriptError as e:
        print(f"FAIL  {name}: script error: {e}")
        return False
    why = []
    if stem in REQUIRED:
        why += REQUIRED[stem](sc, res, cl)
    ok_exp, why_exp = check_expect(sc.get("expect", {}), res)
    names = sorted({v.split(":")[0] for v in res["violations"]}) or "-"
    ok = not why
    print(f"{'PASS' if ok else 'FAIL'}  {name}  violations={names}" + ("" if ok_exp else "   (your expect block does not match)"))
    for w in why: print("      required check: " + w)
    for w in why_exp: print("      expect: " + w)
    return ok


def main():
    argv = sys.argv[1:]
    pattern = "*.json"
    if "--pattern" in argv:
        k = argv.index("--pattern")
        pattern = argv[k + 1]
        del argv[k:k + 2]
    paths = []
    for a in [x for x in argv if not x.startswith("--")]:
        if os.path.isdir(a):
            for pat in pattern.split(","):                      # several patterns may be joined with a comma
                paths += sorted(glob.glob(os.path.join(a, pat)))
        else:
            paths.append(a)
    results = [run_one(p, "--trace" in sys.argv) for p in paths]
    r = [x for x in results if x is not None]
    print(f"{sum(r)}/{len(r)} scripts pass the required checks" + (f", {len(results) - len(r)} skipped" if len(r) < len(results) else ""))
    return 0 if all(r) else 1


if __name__ == "__main__":
    sys.exit(main())
