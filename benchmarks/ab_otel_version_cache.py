"""Post-fix A/B: the memoized ``get_tracer()`` path in obs/_otel.py.

Companion to ``ab_otel_hotspots.py`` (which measured the SHIPPED, unfixed
path: ~320µs/op of ``importlib.metadata.version`` inside every
``get_tracer()`` call, per span and per enqueue). The fix memoizes
``_version()`` and the tracer object itself; this bench confirms the
remaining per-call cost against the same harness.

Rows, in order of what they isolate:

  1. ``importlib.metadata.version`` alone — the removed per-call tax.
  2. ``get_tracer()`` as shipped — hot path: memo hit, module-global return.
  3. ``get_tracer()`` with the tracer memo cleared per call — isolates the
     ``trace.get_tracer`` + ``_version()`` resolution share (what a fresh
     subprocess would pay once).
  4. Prebound tracer return — the floor.
  5. ``safe_start_span`` no-SDK round trip with the memoized tracer — the
     per-span cost jobs actually pay with telemetry on and no exporter.

No provider is installed here (the OTel API forbids unsetting providers,
and the proxy path is the production no-SDK case).

Usage:
    python benchmarks/ab_otel_version_cache.py          # table + JSON
    python benchmarks/ab_otel_version_cache.py --json   # machine-readable
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_RESULTS_DIR = _BENCH_DIR / "results"
sys.path.insert(0, str(_BENCH_DIR))

import bench_hotspots as bh  # noqa: E402  # Why: house harness resolved via the sys.path.insert above.
from opentelemetry.trace import SpanKind  # noqa: E402

import taskq.obs._otel as otel_mod  # noqa: E402


def solo_ns(fn: Callable[[], object], batch: int, batches: int = 7) -> float:
    """Median ns/op for one callable, interleaved against nothing."""
    for _ in range(50):
        fn()
    times = bh.time_callable(fn, batch, batches)
    return statistics.median(times)


def bench() -> list[tuple[str, float, str]]:
    rows: list[tuple[str, float, str]] = []
    batch = 20

    rows.append(
        (
            "importlib.metadata.version('taskq-py') alone",
            solo_ns(lambda: importlib.metadata.version("taskq-py"), batch),
            "the removed tax — was paid inside every get_tracer() pre-fix",
        )
    )
    rows.append(
        (
            "get_tracer() as shipped (memoized version + tracer)",
            solo_ns(otel_mod.get_tracer, batch),
            "hot path: memo hit, module-global return",
        )
    )

    def _fresh_resolution() -> object:
        otel_mod._library_tracer = None  # type: ignore[attr-defined]  # Why: force the once-per-process resolution path each call.
        return otel_mod.get_tracer()

    rows.append(
        (
            "get_tracer() with tracer memo cleared per call",
            solo_ns(_fresh_resolution, batch),
            "the resolution share (trace.get_tracer + cached _version) — paid once per process now",
        )
    )

    tracer_singleton = otel_mod.get_tracer()
    rows.append(
        (
            "prebound tracer singleton return",
            solo_ns(lambda: tracer_singleton, batch),
            "floor: module-global return, zero per-call work",
        )
    )

    def _span_no_sdk() -> None:
        with otel_mod.safe_start_span("bench", kind=SpanKind.INTERNAL):
            pass

    rows.append(
        (
            "safe_start_span + end, no SDK (memoized tracer)",
            solo_ns(_span_no_sdk, batch),
            "per-span cost jobs pay with telemetry on and no exporter — was ~326µs pre-fix",
        )
    )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="JSON output only")
    args = parser.parse_args()

    out: dict[str, object] = {
        "schema": 1,
        "bench": "ab_otel_version_cache",
        "python_version": sys.version.split()[0],
        "rows": [],
    }

    rows = bench()
    out["rows"] = rows

    if not args.json:
        for name, ns, note in rows:
            print(f"  {ns / 1000:12.3f} µs/op  {name}  ({note})")

    _RESULTS_DIR.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    path = _RESULTS_DIR / f"otel_version_cache-{ts}.json"
    path.write_text(json.dumps(out, indent=2))
    if not args.json:
        print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
