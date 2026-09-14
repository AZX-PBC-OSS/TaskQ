"""A/B: GC regimes under the per-job dispatch CPU-path stress.

Reuses the ``stress_dispatch`` workload (DI solve + jsonb dumps + record
decode + croniter) but runs fixed-size batches under four GC regimes,
interleaved round-robin so thermal/frequency drift cancels:

  default   gc.enable(), CPython defaults (700, 10, 10)
  freeze    gc.freeze() applied for the batch (undone via gc.unfreeze())
  tuned     gc.set_threshold(50000, 50, 50)
  tuned2    gc.set_threshold(5000, 10, 10)  (milder tuning)

``freeze`` is one-way in general, but ``gc.unfreeze()`` lets us toggle it per
batch, keeping the interleaving honest.  Thresholds are global state and are
switched per batch.

Methodology: house rules from ``bench_hotspots.py`` — interleaved batches,
median per regime.  There is no B-vs-A output comparison here (all regimes
run identical work), but every batch asserts the same per-job invariants the
stress script uses.

Run: .venv/bin/python benchmarks/ab_gc_stress.py   (override with JOBS_PER_BATCH)
"""

from __future__ import annotations

import asyncio
import gc
import logging
import statistics
import time
from datetime import UTC, datetime

import stress_dispatch as sd
import structlog

from taskq._di.solver import solve_dependencies
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import JobRow
from taskq.backend._records import _job_row_from_record
from taskq.cron import compute_next_fire_after

JOBS_PER_BATCH = 2000
ROUNDS = 9  # odd → median
CRON_EVERY_N = sd.CRON_EVERY_N

_REGIMES: dict[str, tuple[int, int, int] | None] = {
    "default": None,
    "freeze": None,
    "tuned(50k,50,50)": (50000, 50, 50),
    "tuned2(5k,10,10)": (5000, 10, 10),
}


def _apply(regime: str) -> None:
    gc.collect()
    if regime == "freeze":
        gc.unfreeze()
        gc.freeze()
    else:
        gc.unfreeze()
    thresholds = _REGIMES[regime]
    if thresholds is None:
        gc.set_threshold(700, 10, 10)
    else:
        gc.set_threshold(*thresholds)
    gc.enable()


async def run_batch(
    regime: str, jobs: int, registry, containers, record, payload, now
) -> tuple[float, int]:
    """Run one batch on the running loop; return (seconds, gen0 collections)."""
    _apply(regime)
    stats_before = gc.get_stats()[0]["collections"]
    t0 = time.perf_counter()
    for i in range(jobs):
        await solve_dependencies(
            func=sd._bench_actor,
            registry=registry,
            scope_containers=containers,
        )
        dumps_jsonb_str(payload)
        row = _job_row_from_record(record)
        assert isinstance(row, JobRow)
        if i % CRON_EVERY_N == 0:
            compute_next_fire_after(sd.CRON_EXPR, sd.CRON_TZ, now)
    elapsed = time.perf_counter() - t0
    gen0 = gc.get_stats()[0]["collections"] - stats_before
    return elapsed, gen0


def main() -> None:
    asyncio.run(_main_async())


async def _main_async() -> None:
    # Same logger posture as stress_dispatch.main(): WARNING root + filtering
    # bound logger so the solver's per-dep debug records never render.
    logging.root.setLevel(logging.WARNING)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
        cache_logger_on_first_use=True,
    )
    _, registry, containers = sd._make_di_stack()
    record = sd._make_record()
    payload = sd._make_payload_4kb()
    payload_size = len(dumps_jsonb_str(payload))
    assert 3500 <= payload_size <= 4600
    now = datetime.now(UTC)

    print(f"jobs/batch: {JOBS_PER_BATCH}, rounds/regime: {ROUNDS} (interleaved round-robin)")
    print("warmup...", flush=True)
    for regime in _REGIMES:
        await run_batch(regime, 300, registry, containers, record, payload, now)

    rates: dict[str, list[float]] = {r: [] for r in _REGIMES}
    gen0_counts: dict[str, list[int]] = {r: [] for r in _REGIMES}
    for _ in range(ROUNDS):
        for regime in _REGIMES:
            elapsed, gen0 = await run_batch(
                regime, JOBS_PER_BATCH, registry, containers, record, payload, now
            )
            rates[regime].append(JOBS_PER_BATCH / elapsed)
            gen0_counts[regime].append(gen0)

    print(
        f"\n{'regime':<18} {'median jobs/s':>14} {'Δ vs default':>13} {'gen0 collections/batch':>24}"
    )
    print("-" * 76)
    base = statistics.median(rates["default"])
    for regime in _REGIMES:
        med = statistics.median(rates[regime])
        delta = (med - base) / base * 100
        g0 = statistics.median(gen0_counts[regime])
        marker = "  (baseline)" if regime == "default" else ""
        print(f"{regime:<18} {med:>14,.0f} {delta:>+12.1f}% {g0:>24,.0f}{marker}")

    print("\nper-round rates (jobs/s):")
    for regime in _REGIMES:
        print(f"  {regime:<18}", " ".join(f"{r:,.0f}" for r in rates[regime]))

    # restore
    _apply("default")


if __name__ == "__main__":
    main()
