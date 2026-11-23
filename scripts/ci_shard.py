"""Cost-balanced shard partition of the suite's test modules.

The suite is wall-clock-bound: every interpreter leg runs the same ~1,100 test
modules at ``-n 2`` on a 4-vCPU runner, so the leg's wall time is the suite's
CPU time divided by two. Sharding the same selection across runner-isolated
jobs divides the wall time by the shard count without adding co-tenancy (each
runner keeps the calibrated ``-n 2`` density and its own PG/Redis pair).

The partition is deterministic and cost-weighted rather than count-weighted:
a module that provisions a real database pays a fixed ~4 s
(create-database + ``apply_pending`` + seed) before its first test, so counting
``def test_`` alone would hand one shard every DB module. Weighting that fixed
cost per PG-using module and greedily packing the heaviest first (LPT) keeps
the shards within a small factor of each other.

Usage: ``python scripts/ci_shard.py --shard 1 --total 4`` → the paths, one per
line, for that shard. ``--verify`` prints the per-shard weight totals.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS = ROOT / "tests"

#: The per-module fixtures whose lifecycle cost dominates a DB module's setup.
_DB_FIXTURES = re.compile(
    r"\b(pg_dsn|pg_pool|pg_conn|clean_pg_conn|module_pg_schema|wf_schema|wf_pool|load_sensitive|system)\b"
)
_TEST_DEF = re.compile(r"^\s*(?:async\s+)?def test_", re.M)
#: The fixed cost of a module that provisions its own database, in test-equivalents.
_DB_SETUP_WEIGHT = 20


def _weight(path: Path) -> int:
    text = path.read_text(encoding="utf-8", errors="ignore")
    tests = len(_TEST_DEF.findall(text))
    return max(1, tests) + (_DB_SETUP_WEIGHT if _DB_FIXTURES.search(text) else 0)


def _partition(total: int) -> list[list[Path]]:
    files = sorted(TESTS.glob("test_*.py"))
    if not files:
        raise SystemExit(f"no test modules under {TESTS}")
    weighted = sorted(((p, _weight(p)) for p in files), key=lambda item: (-item[1], item[0].name))
    bins: list[list[Path]] = [[] for _ in range(total)]
    loads = [0] * total
    for path, weight in weighted:
        lightest = min(range(total), key=lambda i: loads[i])
        bins[lightest].append(path)
        loads[lightest] += weight
    return bins


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=1)
    ap.add_argument("--total", type=int, default=1)
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if not 1 <= args.shard <= args.total:
        raise SystemExit(f"--shard must be in 1..{args.total}")
    bins = _partition(args.total)
    here = sorted(bins[args.shard - 1])
    if args.verify:
        for index, bin_files in enumerate(bins, start=1):
            weight = sum(_weight(p) for p in bin_files)
            print(f"shard {index}/{args.total}: {len(bin_files)} modules, weight {weight}")
        return 0
    for path in here:
        print(path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
