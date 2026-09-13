"""A/B: asyncio task-per-progress-event vs queue+single-consumer.

``src/taskq/context.py:163`` spawns one ``asyncio.create_task`` per
``ctx.progress()`` event (publish fan-out, tracked in a done-callback set).
This micro-bench measures the per-event scheduling overhead of that pattern
against two alternatives, in interleaved round-robin batches on one running
loop:

  create_task  current pattern: ``create_task(coro)`` + set add +
               ``add_done_callback(set.discard)`` (coroutine body runs later
               on the loop)
  queue        put the event on an ``asyncio.Queue`` drained by ONE
               pre-started consumer task (per-event cost = ``put_nowait``)
  inline       ``await coro`` directly (the fallback context.py already has)

Two numbers per variant: *schedule* µs/event (producer side only — what
``ctx.progress()`` pays inline) and *schedule+execute* µs/event (producer +
full drain of the side-channel).  The coroutine is a stand-in for
``_publish_progress_event``: a single ``await asyncio.sleep(0)`` I/O point.

Run: .venv/bin/python benchmarks/ab_task_churn.py
"""

from __future__ import annotations

import asyncio
import statistics
import time
from uuid import (
    uuid4,  # noqa: TID251  # Why: throwaway ids for scratch benchmark rows; PK B-tree locality is not the variable under test.
)

BATCH = 200  # events per batch → ~1-3 ms per timed region
ROUNDS = 9  # odd → median


async def _publish_stand_in() -> None:
    # shape of _publish_progress_event: build no objects here beyond the
    # coroutine; one await point so the task must actually be scheduled
    await asyncio.sleep(0)


async def bench_create_task(events: int) -> tuple[float, float]:
    """Return (schedule_s, schedule+execute_s) for task-per-event."""
    pending: set[asyncio.Task[None]] = set()
    job_id = uuid4().hex  # name cost amortised per event, as in context.py

    t0 = time.perf_counter()
    for _ in range(events):
        task = asyncio.create_task(_publish_stand_in(), name=f"taskq-progress-publish-{job_id}")
        pending.add(task)
        task.add_done_callback(pending.discard)
    t_schedule = time.perf_counter() - t0

    t1 = time.perf_counter()
    if pending:
        await asyncio.gather(*pending)
    t_drain = time.perf_counter() - t1
    return t_schedule, t_schedule + t_drain


async def bench_queue(events: int) -> tuple[float, float]:
    """Return (schedule_s, schedule+execute_s) for queue + single consumer."""
    q: asyncio.Queue[object] = asyncio.Queue(maxsize=events + 1)
    sentinel = object()

    async def consumer() -> None:
        while True:
            item = await q.get()
            if item is sentinel:
                return
            await _publish_stand_in()

    cons = asyncio.create_task(consumer())

    t0 = time.perf_counter()
    for _ in range(events):
        q.put_nowait(None)
    t_schedule = time.perf_counter() - t0

    t1 = time.perf_counter()
    q.put_nowait(sentinel)
    await cons
    t_drain = time.perf_counter() - t1
    return t_schedule, t_schedule + t_drain


async def bench_inline(events: int) -> tuple[float, float]:
    """Await each event inline; schedule == execute."""
    t0 = time.perf_counter()
    for _ in range(events):
        await _publish_stand_in()
    elapsed = time.perf_counter() - t0
    return elapsed, elapsed


async def main() -> None:
    variants: dict[str, object] = {
        "create_task": bench_create_task,
        "queue+consumer": bench_queue,
        "inline await": bench_inline,
    }
    sched: dict[str, list[float]] = {k: [] for k in variants}
    full: dict[str, list[float]] = {k: [] for k in variants}

    print(f"events/batch: {BATCH}, rounds: {ROUNDS} (interleaved round-robin)")
    print("warmup...", flush=True)
    for fn in variants.values():
        await fn(200)  # type: ignore[operator]

    for _ in range(ROUNDS):
        for name, fn in variants.items():
            s, f = await fn(BATCH)  # type: ignore[operator]
            sched[name].append(s / BATCH)
            full[name].append(f / BATCH)

    print(
        f"\n{'variant':<16} {'µs/event schedule':>18} {'µs/event sched+exec':>21}"
        f" {'% of 100µs budget (sched)':>27}"
    )
    print("-" * 88)
    for name in variants:
        s = statistics.median(sched[name]) * 1e6
        f = statistics.median(full[name]) * 1e6
        print(f"{name:<16} {s:>18.2f} {f:>21.2f} {s:>26.1f}%")

    b_sched = statistics.median(sched["create_task"]) * 1e6
    q_sched = statistics.median(sched["queue+consumer"]) * 1e6
    b_full = statistics.median(full["create_task"]) * 1e6
    q_full = statistics.median(full["queue+consumer"]) * 1e6
    print("\n== extrapolation at 10k progress events/sec (100µs budget/event) ==")
    print(f"  create_task schedule = {b_sched:.2f}µs/event = {b_sched / 100:.1f}% of budget")
    print(
        f"  schedule-only saving vs queue: {b_sched - q_sched:.2f} µs/event"
        f" → {(b_sched - q_sched) * 10_000 / 1000:.1f} CPU-ms per second"
        f" = {(b_sched - q_sched) * 10_000 / 1e6 * 100:.2f}% of one core"
    )
    print(
        f"  schedule+exec saving vs queue: {b_full - q_full:.2f} µs/event"
        f" (queue consumer serializes stand-in coros; not apples-to-apples)"
    )
    print(
        "  object churn: create_task allocates 1 Task + callback + set-node per event"
        " (queue: 1 node, reused consumer task)"
    )

    print("\nper-round schedule µs/event:")
    for name in variants:
        print(f"  {name:<16}", " ".join(f"{x * 1e6:6.2f}" for x in sched[name]))


if __name__ == "__main__":
    asyncio.run(main())
