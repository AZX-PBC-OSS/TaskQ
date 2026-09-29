"""A/B: the sweep statements' per-call SQL render vs the cached render.

The hot-path profile (benchmarks/results/hotpath-profile.md) named one
per-tick redundancy in the leader's loop paths: every sweep call re-ran
``str.format`` over its SQL template, and ``sweep_scheduled_to_pending``
runs on the leader's scheduled-wake tick (~1 s cadence) rendering a
~20 KB template each call, byte-identical every time (the only
interpolation is the schema identifier, validated against ``_IDENT_RE``).

The fix: ``taskq.backend._sweeps._render_sweep_sql`` memoises the render
per (template, schema), still validating the identifier before any
interpolation. This benchmark is the red/green pair the fix owes:

- GREEN (correctness): the cached render is byte-identical to the direct
  ``.format`` render for every converted statement, across schemas, and
  an invalid identifier raises the same ValueError it always did.
- RED/GREEN (cost): ns/call of the direct render (A, the before) vs the
  cached render (B, the after), interleaved batches, median-of-medians -
  the run_bench.py regression-rule shape (ratio of medians).

Writes its artifact to results/hotpath-fix-sweep-render.json.

Usage:
    python benchmarks/ab_sweep_sql_render.py
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

_BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_BENCH_DIR.parent))  # taskq importable from the worktree

from taskq.backend._sweeps import (  # noqa: E402
    _SWEEP_1_SQL,
    _SWEEP_2_SQL,
    _SWEEP_3_SQL,
    _SWEEP_4_SQL,
    _render_sweep_sql,
)

RESULTS_DIR = _BENCH_DIR / "results"

SCHEMAS = ["taskq", "tq_bench_hotpath", "s1", "a_much_longer_schema_identifier_for_the_bench"]

TEMPLATES = [
    ("sweep1", _SWEEP_1_SQL),
    ("sweep2", _SWEEP_2_SQL),
    ("sweep3", _SWEEP_3_SQL),
    ("sweep4", _SWEEP_4_SQL),
]

CALLS = 2000
INTERLEAVED_BATCHES = 5


def median_ns(fn: Callable[..., str], *args: Any) -> float:
    samples: list[float] = []
    for _ in range(CALLS):
        t0 = time.perf_counter_ns()
        fn(*args)
        samples.append(float(time.perf_counter_ns() - t0))
    return statistics.median(samples)


def main() -> int:
    # ── GREEN: byte-equality of cached vs direct render ────────────────
    for name, template in TEMPLATES:
        for schema in SCHEMAS:
            direct = template.format(schema=schema)
            cached = _render_sweep_sql(template, schema)
            assert cached == direct, f"{name}: cached render drifted for schema {schema!r}"
    # and an invalid identifier still raises, every call, same message
    for _ in range(2):
        try:
            _render_sweep_sql(_SWEEP_1_SQL, 'bad"; DROP SCHEMA')
        except ValueError as exc:
            assert str(exc) == "invalid schema identifier: 'bad\"; DROP SCHEMA'", str(exc)
        else:
            raise AssertionError("invalid schema identifier must still raise")

    # ── RED/GREEN: interleaved A/B batches ─────────────────────────────
    a_batches: list[float] = []
    b_batches: list[float] = []
    for _ in range(INTERLEAVED_BATCHES):
        for schema in SCHEMAS:
            for _name, template in TEMPLATES:
                a_batches.append(median_ns(lambda t=template, s=schema: t.format(schema=s)))
                b_batches.append(median_ns(_render_sweep_sql, template, schema))

    a_ns = statistics.median(a_batches)
    b_ns = statistics.median(b_batches)
    ratio = b_ns / a_ns if a_ns else float("inf")

    result = {
        "bench": "ab_sweep_sql_render",
        "calls_per_sample": CALLS,
        "interleaved_batches": INTERLEAVED_BATCHES,
        "a_direct_format_ns": round(a_ns, 1),
        "b_cached_render_ns": round(b_ns, 1),
        "b_over_a_ratio": round(ratio, 4),
        "byte_equality": "asserted above (cached == template.format for all converted statements)",
        "note": "A is the per-call render the leader's ~1s scheduled-wake tick paid; "
        "B is the memoised render the tick pays now.",
    }
    out = RESULTS_DIR / "hotpath-fix-sweep-render.json"
    out.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    assert b_ns < a_ns, "cached render must not be slower than the direct render"
    return 0


if __name__ == "__main__":
    sys.exit(main())
