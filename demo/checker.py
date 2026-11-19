"""Consistency checker for the demo's finish: every GSI on users must hold
exactly one correctly placed entry per base row, and no gsi_ prepared
transaction may be left on any node.

Run from the repo root:  python demo/checker.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bench"))
from db import COORD, SHARDS, check_all, connect, q  # noqa: E402


def wait_drained(coord, timeout=600):
    """Wait until the async outboxes are empty (async GSIs are exact only then)."""
    deadline = time.time() + timeout
    while q(coord, "SELECT coalesce(sum(pending), 0) FROM gsi_lag()")[0][0] > 0:
        if time.time() > deadline:
            sys.exit("async outboxes did not drain")
        time.sleep(0.1)


if __name__ == "__main__":
    c = connect(COORD)
    ss = [connect(p) for p in SHARDS]
    wait_drained(c)
    ok = True
    for name, r in check_all(c, ss).items():
        print(f"{name}: rows={r['rows']} missing={len(r['missing'])} orphaned={len(r['orphaned'])} "
              f"misplaced={len(r['misplaced'])} prepared={len(r['prepared'])}")
        ok = ok and not any(r[k] for k in ("missing", "orphaned", "misplaced", "prepared"))
    sys.exit(0 if ok else 1)
