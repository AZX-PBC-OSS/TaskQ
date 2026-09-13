"""Spot-check profiler for a single named A/B bench.

    python benchmarks/profile_hotspot.py                  # cProfile the default hotspot (di)
    python benchmarks/profile_hotspot.py jsonb            # cProfile the jsonb bench
    python benchmarks/profile_hotspot.py cron --engine pyinstrument
    python benchmarks/profile_hotspot.py --engine gil-sampler   # ~20s dispatch stress

Engines:
    cprofile      stdlib, always available. Sorted by cumulative then tottime.
    pyinstrument  sampling profiler — lower overhead, better for hot loops;
                  falls back to cProfile with a warning if not installed.
    gil-sampler   runs the full dispatch stress (benchmarks/stress_dispatch.py)
                  under benchmarks/gil_sample.py's in-process sampler and prints
                  the top GIL-holding leaf frames. Use this on macOS, where
                  py-spy needs root (task_for_pid). The named bench is ignored.

Artifacts land in benchmarks/results/ (gitignored):
    profile-<bench>-<engine>-<ts>.txt      cProfile / pyinstrument text
    gil_dispatch-<ts>.raw / gil_dispatch-<ts>.svg   collapsed stacks + flamegraph

Pure stdlib control path, cross-platform, Python 3.12+.
"""

from __future__ import annotations

import argparse
import cProfile
import io
import os
import pstats
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_BENCH_DIR = Path(__file__).resolve().parent
_RESULTS_DIR = _BENCH_DIR / "results"
sys.path.insert(0, str(_BENCH_DIR))

# bench_hotspots (and therefore taskq) is imported lazily inside bench_target()
# so --help and the gil-sampler path load without a working taskq import.


def bench_target(name: str) -> Callable[[], Any]:
    """Map a bench name to the same work load bench_hotspots/run_bench run for it."""
    import bench_hotspots as bh  # needs _BENCH_DIR on sys.path

    # Mirrors bench_hotspots.main()/run_bench.py: production log config so
    # timed regions are measured under the same logging setup dispatch uses.
    from taskq.obs._structlog import setup_logging

    setup_logging(level="INFO", log_format="json")

    if name == "jsonb":
        return bh.bench_jsonb
    if name == "decode":
        return lambda: [*bh.bench_decode_jsonb(), *bh.bench_encode_json_stdlib_vs_orjson()]
    if name == "cron":
        return bh.bench_cron
    if name == "rows":
        return bh.bench_job_row_decode
    if name == "retry":
        return bh.bench_retry_policy
    if name == "di":
        # The per-job DI solve is the dispatch hot path; introspection is its
        # dominant ingredient, so profile both.
        return lambda: (bh.bench_di_solver(), bh.bench_di_introspection_only())
    raise SystemExit(f"unknown bench {name!r}; choices: jsonb, decode, cron, rows, retry, di")


def profile_cprofile(name: str, target: Callable[[], Any], top: int) -> str:
    prof = cProfile.Profile()
    prof.enable()
    target()
    prof.disable()
    out = io.StringIO()
    stats = pstats.Stats(prof, stream=out)
    stats.sort_stats("cumulative")
    stats.print_stats(top)
    stats.sort_stats("tottime")
    stats.print_stats(top)
    text = f"── cProfile: {name} (top {top} by cumulative, then tottime) ──\n{out.getvalue()}"
    print(text)
    return text


def profile_pyinstrument(name: str, target: Callable[[], Any]) -> str:
    try:
        from pyinstrument import Profiler
    except ImportError:
        print(
            "pyinstrument not installed (pip install pyinstrument, or make install); "
            "falling back to cProfile.",
            file=sys.stderr,
        )
        return profile_cprofile(name, target, top=25)
    prof = Profiler()
    t0 = time.perf_counter()
    with prof:
        target()
    elapsed = time.perf_counter() - t0
    text = f"── pyinstrument: {name} (wall {elapsed:.2f}s) ──\n" + prof.output_text(
        unicode=True, color=False, show_all=False
    )
    print(prof.output_text(unicode=True, color=True, show_all=False))
    return text


def profile_gil_sampler() -> None:
    """Drive stress_dispatch.py under gil_sample.py; copy artifacts into results/."""
    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    stress_seconds = os.environ.get("STRESS_SECONDS", "20")
    print(
        f"running benchmarks/gil_sample.py (dispatch stress, ~{stress_seconds}s; py-spy fallback for macOS)…",
        flush=True,
    )
    proc = subprocess.run(  # noqa: S603  (fixed, repo-owned script — no untrusted input)
        [sys.executable, str(_BENCH_DIR / "gil_sample.py")], check=False
    )
    if proc.returncode != 0:
        raise SystemExit(f"gil_sample.py exited with {proc.returncode}")
    copied = []
    for src, stem in (
        (_BENCH_DIR / "dispatch_gil.raw", f"gil_dispatch-{ts}.raw"),
        (_BENCH_DIR / "dispatch_flame.svg", f"gil_dispatch-{ts}.svg"),
    ):
        if src.is_file():
            dest = _RESULTS_DIR / stem
            shutil.copy2(src, dest)
            copied.append(str(dest))
    if copied:
        print("artifacts: " + ", ".join(copied))
    else:
        print("note: no gil_sample artifacts found to copy", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Profile a single TaskQ benchmark hotspot.")
    parser.add_argument(
        "bench",
        nargs="?",
        default="di",
        help="named bench: jsonb|decode|cron|rows|retry|di (default: di; ignored by gil-sampler)",
    )
    parser.add_argument(
        "--engine", default="cprofile", choices=["cprofile", "pyinstrument", "gil-sampler"]
    )
    parser.add_argument(
        "--top", type=int, default=25, help="rows to show for cProfile (default 25)"
    )
    args = parser.parse_args()

    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    if args.engine == "gil-sampler":
        profile_gil_sampler()
        return 0

    target = bench_target(args.bench)
    if args.engine == "pyinstrument":
        text = profile_pyinstrument(args.bench, target)
    else:
        text = profile_cprofile(args.bench, target, top=args.top)

    artifact = _RESULTS_DIR / f"profile-{args.bench}-{args.engine}-{ts}.txt"
    artifact.write_text(text, encoding="utf-8")
    print(f"artifact: {artifact}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
