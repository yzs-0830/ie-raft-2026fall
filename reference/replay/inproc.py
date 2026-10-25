"""In-process backend for the harness (TA side): same interface as HttpNode, no containers."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ie_service import NodeService


class InProcNode:
    def __init__(self, i):
        self.i, self.svc = i, NodeService(None)

    def wait_ready(self): pass
    def init(self, body): self.svc.init(body)
    def event(self, ev): return self.svc.event(ev)
    def state(self): return self.svc.state()
    def crash(self): self.svc.node = None
    def restart(self): self.svc.crash_restart()
