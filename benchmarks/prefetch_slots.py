"""Prefetch slot accounting — peak rows locked and sub-ms throughput (#229).

Measures the producer's claim-sizing change end to end against a real
Postgres: OLD sizes claims by queue emptiness alone
(``maxsize - qsize`` — the pre-#229 producer, which locked up to 2x
``max_concurrency`` rows while every consumer was busy); NEW subtracts
the active-jobs count (``maxsize - qsize - active`` — the shipped
producer, whose behavior is pinned by
``tests/test_producer_slot_accounting.py``).

Both sides run the SAME harness — one real ``PostgresBackend``, real
``consumer_loop_stub`` consumers (real terminal writes), one shared
slot-freed event, the claim cooldown and jitter verbatim — differing
only in the availability expression, the A6 harness doctrine
(perf-evidence-dispatch.md: the measurement isolates the one changed
expression, everything else identical in-process).

Scenarios:

* **fast** — a sub-millisecond actor (the stub sentinel with
  ``stub_work_timeout=0``): the dispatch round trip the old prefetch
  hid is the trade; jobs/second is the number that decides it.
* **slow** — 3 s jobs (a fleet of long actors): throughput is identical
  by construction (the queue never added parallelism — only consumers
  execute), and peak running-rows-locked is the reclaim-exposure and
  head-of-line number: OLD locks 2x the slot count, NEW locks at most
  the slot count.

Usage (from the repo root):
    uv run --no-sync python benchmarks/prefetch_slots.py
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import time
from contextlib import AsyncExitStack
from datetime import timedelta
from uuid import UUID

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import (
    start_shared_services,
    stop_shared_services,
)
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import (
    _CLAIM_COOLDOWN_SECONDS,
    _jittered_poll_interval,
    consumer_loop_stub,
)

ACTOR = "prefetch_bench_actor"
QUEUE = "default"
DEFAULT_JOBS = 400


async def setup_schema(admin_dsn: str, schema: str) -> None:
    """Fresh schema + migrations + one registered actor (convention of
    tests/conftest.py and the sibling benchmarks)."""
    conn = await asyncpg.connect(admin_dsn)
    try:
        await conn.execute(  # Why: schema is a benchmark-controlled identifier, the conftest convention
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )
        await conn.execute(f'CREATE SCHEMA "{schema}"')  # Why: benchmark-controlled identifier
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608  # Why: schema is a benchmark-controlled identifier (conftest convention); all values are $n-bound.
            ACTOR,
            QUEUE,
        )
    finally:
        await conn.close()


async def seed_pending(conn: asyncpg.Connection, schema: str, n: int) -> None:
    """N due pending rows for the round's (actor, queue)."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a benchmark-controlled identifier; all values are $n-bound.
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind) "
        "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
        "0, clock_timestamp() - interval '1 minute', 3, 'transient' "
        "FROM generate_series(1, $3::int) AS g",
        ACTOR,
        QUEUE,
        n,
    )


async def build_worker(
    dsn: str, schema: str
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend, asyncpg.Pool]:
    """Real WorkerDeps + PostgresBackend over one pool (the
    benchmarks/e2e_dispatch.py shape)."""
    # TASKQ_LOG_LEVEL=WARNING: the stub consumers' per-job state-change
    # INFO lines are console I/O both sides pay equally, but silencing
    # them keeps the measured loop tighter and the output readable.
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema, "TASKQ_LOG_LEVEL": "WARNING"}
    )
    stack = AsyncExitStack()
    pool = await stack.enter_async_context(asyncpg.create_pool(dsn, min_size=4, max_size=16))
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: one shared pool stands in for the three production pools — the split under measurement is the claim-sizing expression, not pool topology (e2e_dispatch.py doctrine).
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    backend = PostgresBackend(
        deps,
        SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )
    return stack, deps, backend, pool


async def replica_producer(
    deps: WorkerDeps,
    local_queue: asyncio.Queue[JobRow],
    shutdown_event: asyncio.Event,
    backend: PostgresBackend,
    worker_id: UUID,
    slot_freed: asyncio.Event,
    *,
    subtract_active: bool,
    rounds_box: dict[str, int],
) -> None:
    """The producer loop, trimmed to the measurement's needs and
    parameterized by exactly the expression under test.

    Everything else is the shipped loop's own arithmetic: the claim
    cooldown (with jitter) on short rounds, the slot-freed wait under
    saturation, the disowned-discard on claim. The claim-round count
    accumulates into *rounds_box* (the task is cancelled at teardown, so
    a return value would be lost).
    """
    settings = deps.settings
    poll_interval = 0.05
    rng = random.Random(11)  # noqa: S311  # Why: timing jitter for the cooldown, never cryptography — the shipped producer's own seeding pattern.
    lock_lease_td = timedelta(seconds=settings.lock_lease)
    claim_not_before = 0.0
    while not shutdown_event.is_set():
        if subtract_active:
            available = local_queue.maxsize - local_queue.qsize() - deps.active_jobs.count()
        else:
            available = local_queue.maxsize - local_queue.qsize()
        if available <= 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(slot_freed.wait(), timeout=0.1)
            slot_freed.clear()
            continue
        cooldown_remaining = claim_not_before - time.monotonic()
        if cooldown_remaining > 0:
            await asyncio.sleep(cooldown_remaining)
            continue
        round_started = time.monotonic()
        jobs = await backend.dispatch_batch(
            worker_id=worker_id,
            queues=settings.queues,
            limit=available,
            lock_lease=lock_lease_td,
        )
        rounds_box["rounds"] = rounds_box.get("rounds", 0) + 1
        if len(jobs) < available:
            claim_not_before = round_started + _jittered_poll_interval(_CLAIM_COOLDOWN_SECONDS, rng)
        if jobs:
            for job in jobs:
                deps.disowned_jobs.discard(job.id)
                await local_queue.put(job)
            continue
        await asyncio.sleep(_jittered_poll_interval(poll_interval, rng))


async def run_scenario(
    dsn: str,
    schema: str,
    *,
    subtract_active: bool,
    max_concurrency: int,
    stub_work_timeout: float,
    jobs: int,
    label: str,
) -> dict[str, float]:
    """One measured run: seed, producer + consumers, sample peak locked."""
    stack, deps, backend, pool = await build_worker(dsn, schema)
    try:
        worker_id = new_uuid()
        async with pool.acquire() as conn:
            await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
            await seed_pending(conn, schema, jobs)
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a benchmark-controlled identifier; all values are $n-bound.
                "VALUES ($1, 'prefetch-bench', 1, ARRAY[$2])",
                worker_id,
                QUEUE,
            )
        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=max_concurrency)
        shutdown = asyncio.Event()
        slot_freed = asyncio.Event()

        peak_locked = 0
        stop_sampler = asyncio.Event()

        async def sampler() -> None:
            nonlocal peak_locked
            while not stop_sampler.is_set():
                async with pool.acquire() as conn:
                    n = await conn.fetchval(
                        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a benchmark-controlled identifier; the only interpolated value.
                        "WHERE status = 'running' AND locked_by_worker = $1",
                        worker_id,
                    )
                if n is not None and n > peak_locked:
                    peak_locked = n
                await asyncio.sleep(0.02)

        rounds_box: dict[str, int] = {}
        producer_task = asyncio.create_task(
            replica_producer(
                deps,
                local_queue,
                shutdown,
                backend,
                worker_id,
                slot_freed,
                subtract_active=subtract_active,
                rounds_box=rounds_box,
            )
        )
        consumers = [
            asyncio.create_task(
                consumer_loop_stub(
                    deps,
                    local_queue,
                    shutdown,
                    backend=backend,
                    worker_id=worker_id,
                    stub_work_timeout=stub_work_timeout,
                    slot_freed_event=slot_freed,
                )
            )
            for _ in range(max_concurrency)
        ]
        sampler_task = asyncio.create_task(sampler())
        t0 = time.monotonic()
        try:
            while True:
                async with pool.acquire() as conn:
                    succeeded = await conn.fetchval(
                        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a benchmark-controlled identifier; the only interpolated value.
                        "WHERE status = 'succeeded'"
                    )
                if (succeeded or 0) >= jobs:
                    break
                if time.monotonic() - t0 > 120:
                    raise TimeoutError(f"{label}: run did not drain in 120s")
                await asyncio.sleep(0.05)
        finally:
            shutdown.set()
            stop_sampler.set()
            producer_task.cancel()
            for consumer in consumers:
                consumer.cancel()
            for task in (producer_task, *consumers, sampler_task):
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        elapsed = time.monotonic() - t0
        return {
            "label": label,
            "jobs_s": jobs / elapsed,
            "elapsed_s": elapsed,
            "peak_locked": float(peak_locked),
            "claim_rounds": float(rounds_box.get("rounds", 0)),
        }
    finally:
        await stack.aclose()


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--jobs", type=int, default=DEFAULT_JOBS)
    args = parser.parse_args()

    services = start_shared_services()
    schema = f"tq_bench_prefetch_{new_uuid().hex[:8]}"
    try:
        await setup_schema(services.pg_dsn, schema)
        for label, stub_timeout, jobs in (
            ("fast-sub-ms", 0.0, args.jobs),
            ("slow-3s", 3.0, args.concurrency * 4),
        ):
            for side, subtract in (
                ("OLD (queue-emptiness)", False),
                ("NEW (free slots)", True),
            ):
                res = await run_scenario(
                    services.pg_dsn,
                    schema,
                    subtract_active=subtract,
                    max_concurrency=args.concurrency,
                    stub_work_timeout=stub_timeout,
                    jobs=jobs,
                    label=f"{label} / {side}",
                )
                print(
                    f"{res['label']:38s} jobs/s={res['jobs_s']:8.1f} "
                    f"peak-locked={res['peak_locked']:6.0f} "
                    f"rounds={res['claim_rounds']:6.0f} "
                    f"elapsed={res['elapsed_s']:6.2f}s"
                )
    finally:
        conn = await asyncpg.connect(services.pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()
        stop_shared_services(services)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
