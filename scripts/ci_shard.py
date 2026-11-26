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

The partition covers the whole DEFAULT collection tier — nested suites
included, because a shard list that silently omits a directory removes those
modules from CI entirely. With ``--include-system-e2e`` (used ONLY by the
serial ``load-sensitive`` job, which also passes ``--system-e2e`` to pytest),
it also includes the ``tests/system_e2e`` modules marked ``load_sensitive``.
Keeping ``tests/system_e2e`` out of the default shard list is load-bearing:
passing a ``tests/system_e2e`` path explicitly on the pytest command line
forces pytest to import the module before ``pytest_collection_modifyitems``
deselects it, which executes ``tests/system_e2e/_wf_app.py`` and pollutes
the global workflow registry (colliding with ``examples/deep_research.py``).

Directories belonging exclusively to other lanes stay out: the OPT-IN lanes
(``tests/e2e`` behind ``--e2e``, ``tests/otel_validation`` behind
``--otel-validation`` — flags the sharded legs never pass) and the ``perf``
tier. ``perf`` is not an opt-in lane: no flag gates ``tests/perf``, and its
tests carry ``load_sensitive`` so they are marker-eligible in the serial lane —
but no CI lane owns the directory today (whether and where ``perf`` gets one is
a CI-owner decision). This constant is what keeps it out, not a flag.

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
#: Nested directories owned exclusively by opt-in lanes (``--e2e`` and
#: ``--otel-validation``) and the perf tier. System-e2e is different: its
#: load-sensitive subset belongs to the serial shards, which pass its flag.
_DEDICATED_LANE_DIRS = frozenset({"e2e", "otel_validation", "perf"})
#: The fixed cost of a module that provisions its own database, in test-equivalents.
_DB_SETUP_WEIGHT = 20


def _weight(path: Path) -> int:
    text = path.read_text(encoding="utf-8", errors="ignore")
    tests = len(_TEST_DEF.findall(text))
    return max(1, tests) + (_DB_SETUP_WEIGHT if _DB_FIXTURES.search(text) else 0)


def _modules(*, include_system_e2e: bool = False) -> list[Path]:
    """The default tier, plus system-e2e's load-sensitive modules when requested."""
    modules: list[Path] = []
    for path in TESTS.rglob("test_*.py"):
        relative = path.relative_to(TESTS)
        if _DEDICATED_LANE_DIRS.intersection(relative.parts[:-1]):
            continue
        if relative.parts[0] == "system_e2e":
            if not include_system_e2e:
                continue
            if "pytest.mark.load_sensitive" not in path.read_text(
                encoding="utf-8", errors="ignore"
            ):
                continue
        modules.append(path)
    return sorted(modules)


def _partition(total: int, *, include_system_e2e: bool = False) -> list[list[Path]]:
    """Cost-balanced LPT packing.

    The tie-break is the module's FULL path, not its basename: recursive
    discovery makes duplicate basenames reachable (``tests/web_admin`` and
    ``tests/web_progress`` both carry ``test_insights.py``, and two of them can
    tie on weight), and twenty runner-isolated legs each invoke this script and
    must agree. ``(-weight, posix path)`` is a total order over the module set,
    so the partition does not depend on discovery order.
    """
    files = _modules(include_system_e2e=include_system_e2e)
    if not files:
        raise SystemExit(f"no test modules under {TESTS}")
    weighted = sorted(
        ((p, _weight(p)) for p in files), key=lambda item: (-item[1], item[0].as_posix())
    )
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
    ap.add_argument("--include-system-e2e", action="store_true")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if not 1 <= args.shard <= args.total:
        raise SystemExit(f"--shard must be in 1..{args.total}")
    bins = _partition(args.total, include_system_e2e=args.include_system_e2e)
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
