"""Shared A/B harness for the tors adoption measurements.

Methodology follows the ``bench_hotspots.py`` house rules: interleaved A/B
batches (cancelling thermal/frequency drift), a correctness assertion that
the variant's output is identical to the baseline's before any timing is
trusted, and honest variants (both sides do the same semantic work).  This
harness adds the round-statistics reporting the tors adoption map requires:
per-round op-times across the interleaved batches, reported as p50 and p99.

Run: .venv/bin/python benchmarks/tors_ab_adoptables.py
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

_WARMUP = 200
_ROUNDS = 61
"""Odd round count: a median is an actual round, not an interpolation."""


@dataclass
class ABRow:
    name: str
    a_ns: list[float] = field(default_factory=list[float])
    b_ns: list[float] = field(default_factory=list[float])
    correct: bool = True
    note: str = ""


def percentile(sorted_xs: list[float], p: float) -> float:
    """Nearest-rank percentile over a pre-sorted list."""
    if not sorted_xs:
        return float("nan")
    idx = min(len(sorted_xs) - 1, max(0, round(p * (len(sorted_xs) - 1))))
    return sorted_xs[idx]


def ab(
    name: str,
    a: Callable[[], object],
    b: Callable[[], object],
    *,
    batch: int = 1,
    rounds: int = _ROUNDS,
    check: Callable[[], bool] | None = None,
    note: str = "",
) -> ABRow:
    """Interleave A and B batches; return per-round ns/op distributions.

    ``batch`` ops per timed round (large enough that one round dominates
    timer noise for fast calls); ``rounds`` interleaved A/B rounds total.
    ``check`` is a truthiness callable run once up front — a False marks
    the row incorrect so the runner reports the mismatch rather than
    silently timing a wrong variant.
    """
    row = ABRow(name=name, note=note)
    if check is not None:
        row.correct = bool(check())
    for _ in range(_WARMUP):
        a()
        b()
    for _ in range(rounds):
        t0 = time.perf_counter_ns()
        for _ in range(batch):
            a()
        row.a_ns.append((time.perf_counter_ns() - t0) / batch)
        t0 = time.perf_counter_ns()
        for _ in range(batch):
            b()
        row.b_ns.append((time.perf_counter_ns() - t0) / batch)
    return row


def fmt_ns(ns: float) -> str:
    if ns >= 1e6:
        return f"{ns / 1e6:,.2f} ms"
    if ns >= 1e3:
        return f"{ns / 1e3:,.2f} µs"
    return f"{ns:,.0f} ns"


def print_rows(rows: list[ABRow]) -> None:
    """Human table: p50/p99 for both sides, speedup at p50, parity flag."""
    print(
        f"{'bench':<44} {'A p50':>12} {'A p99':>12} {'B p50':>12} "
        f"{'B p99':>12} {'p50 speedup':>11}  ok"
    )
    print("-" * 118)
    for r in rows:
        a_sorted, b_sorted = sorted(r.a_ns), sorted(r.b_ns)
        a50, a99 = percentile(a_sorted, 0.50), percentile(a_sorted, 0.99)
        b50, b99 = percentile(b_sorted, 0.50), percentile(b_sorted, 0.99)
        speedup = a50 / b50 if b50 else 0.0
        flag = "OK" if r.correct else "MISMATCH!"
        warn = f"  {r.note}" if r.note else ""
        print(
            f"{r.name:<44} {fmt_ns(a50):>12} {fmt_ns(a99):>12} {fmt_ns(b50):>12} "
            f"{fmt_ns(b99):>12} {speedup:>10.2f}x  {flag}{warn}"
        )


def dump_results(rows: list[ABRow], out_name: str, meta: dict[str, str]) -> None:
    """Write the machine-readable run record to benchmarks/results/."""
    out = {
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": meta,
        "rows": [
            {
                "name": r.name,
                "correct": r.correct,
                "note": r.note,
                "a_ns_p50": percentile(sorted(r.a_ns), 0.50),
                "a_ns_p99": percentile(sorted(r.a_ns), 0.99),
                "b_ns_p50": percentile(sorted(r.b_ns), 0.50),
                "b_ns_p99": percentile(sorted(r.b_ns), 0.99),
                "rounds": len(r.a_ns),
            }
            for r in rows
        ],
    }
    path = Path(__file__).parent / "results" / out_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2) + "\n")
    print(f"\nresults → {path}")


def stats_summary(rows: list[ABRow]) -> dict[str, object]:
    """The JSON summary the contended bench also embeds (percentiles + min)."""
    return {
        r.name: {
            "correct": r.correct,
            "a_p50": percentile(sorted(r.a_ns), 0.50),
            "a_p99": percentile(sorted(r.a_ns), 0.99),
            "a_min": min(r.a_ns) if r.a_ns else None,
            "b_p50": percentile(sorted(r.b_ns), 0.50),
            "b_p99": percentile(sorted(r.b_ns), 0.99),
            "b_min": min(r.b_ns) if r.b_ns else None,
            "rounds": len(r.a_ns),
        }
        for r in rows
    }
