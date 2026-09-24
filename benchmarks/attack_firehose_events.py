"""Attack-firehose harness, sections 4+5: the event INSERT pipeline and dispatch fairness.

Section 4 (event INSERT under flood): the production
``INSERT_EVENTS_DETAIL_BATCH_SQL`` at the sweep's batch cap (100 rows) with
the sweeps' own ``statement_timeout`` arm, measured at p50/p99 under a hot
``job_events`` table - the margin ratio against the 1750 ms timeout - and
deliberately oversize (10k rows in ONE unnest, the unbounded-caller
monster) to show why every caller must stay under the cap.

Section 5 (dispatch fairness): a hot queue with a 10k-deep pending flood
and a cold queue with 10 pending jobs, driven through the REAL
``PostgresBackend.dispatch_batch`` under round-robin: the flood must not
starve the cold queue's claims.

Usage: FIREHOSE_DSN=... uv run python benchmarks/attack_firehose_events.py
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import UTC, datetime, timedelta
from statistics import median

import asyncpg

from taskq._ids import new_job_id, new_uuid
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.backend._sweeps import _apply_batch_statement_timeout, _restore_statement_timeout
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import DEFAULT_EVENT_WRITER_BATCH_SIZE
from taskq.migrate import apply_pending
from taskq.worker.deps import open_worker_deps

DSN = os.environ.get("FIREHOSE_DSN", "postgresql://taskq:taskq@127.0.0.1:55433/taskq")
SCHEMA = "firehose_events"
WORKER_ID = new_uuid()

INSERT_JOBS_SQL = (
    f'INSERT INTO "{SCHEMA}".jobs '  # noqa: S608  # Why: the schema is this harness's own constant, never user input.
    """
    (id, actor, queue, payload, max_attempts, retry_kind, retry_base_seconds,
     retry_cap_seconds, retry_backoff, retry_jitter, status)
SELECT id::uuid, 'firehose', $1::text, '{}'::jsonb, 3, 'transient', 1.0,
       60.0, 'exponential', 0.2, 'pending'
FROM unnest($2::uuid[]) AS id
"""
)

DETAIL = {"from_state": "running", "to_state": "pending", "reason": "lock_expired"}
DETAILS_JSON = json.dumps(DETAIL)


async def section_event_insert(pool: asyncpg.Pool) -> dict[str, object]:
    sql = INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=SCHEMA)

    # job_events carries an FK to jobs, so seed the referenced rows first.
    job_ids = [str(new_uuid()) for _ in range(1_000)]
    async with pool.acquire() as c:
        for i in range(0, 1_000, 500):
            await c.execute(
                INSERT_JOBS_SQL,
                "firehose_evt_q",
                job_ids[i : i + 500],
            )

    latencies: list[float] = []
    rows_written = 0

    async def one_round(conn: asyncpg.Connection, batch_ids: list[str]) -> None:
        nonlocal rows_written
        t0 = time.perf_counter()
        await conn.execute(
            sql,
            batch_ids,
            [DETAILS_JSON] * len(batch_ids),
            "state_change",
        )
        rows_written += len(batch_ids)
        latencies.append((time.perf_counter() - t0) * 1000)

    # Measure 200 batches of 100 under a hot table (10k rows already
    # written this second).
    async with pool.acquire() as conn:
        prev_timeout = await _apply_batch_statement_timeout(conn, 1750)
        try:
            for i in range(0, 10_000, 100):
                await one_round(conn, job_ids[(i // 100) % len(job_ids) :][:100])
            for i in range(200):
                await one_round(conn, job_ids[(i * 100) % 900 :][:100])
        finally:
            await _restore_statement_timeout(conn, prev_timeout)

    lat_sorted = sorted(latencies)

    # The monster: ONE 10k-row unnest INSERT (what an unbounded caller
    # would issue), same statement shape, same table.
    async with pool.acquire() as conn:
        prev_timeout = await _apply_batch_statement_timeout(conn, 1750)
        # the monster's rows must reference real jobs: reuse the pool's ids
        monster_ids = [job_ids[i % len(job_ids)] for i in range(10_000)]
        t0 = time.perf_counter()
        try:
            await conn.execute(
                sql,
                monster_ids,
                [DETAILS_JSON] * 10_000,
                "state_change",
            )
            monster_ms = (time.perf_counter() - t0) * 1000
            monster_timed_out = False
        except asyncpg.QueryCanceledError:
            monster_ms = None
            monster_timed_out = True
        finally:
            await _restore_statement_timeout(conn, prev_timeout)

    return {
        "batch_rows": DEFAULT_EVENT_WRITER_BATCH_SIZE,
        "statement_timeout_ms": 1750,
        "measured_batches": len(latencies),
        "insert_ms_p50": round(median(lat_sorted), 3),
        "insert_ms_p99": round(lat_sorted[int(len(lat_sorted) * 0.99)], 3),
        "insert_ms_max": round(lat_sorted[-1], 3),
        "timeout_margin_ratio": round(1750 / lat_sorted[-1], 1),
        "rows_written": rows_written,
        "monster_10k_one_statement_ms": round(monster_ms, 1) if monster_ms else None,
        "monster_10k_timed_out": monster_timed_out,
    }


async def section_fairness() -> dict[str, object]:
    settings = await _make_settings()
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
        await apply_pending(conn, schema=SCHEMA)
    finally:
        await conn.close()

    from contextlib import AsyncExitStack

    stack = AsyncExitStack()
    deps = await stack.enter_async_context(open_worker_deps(settings))
    backend = PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=0.5),
        cleanup_grace_period=timedelta(seconds=0.5),
    )

    hot = "firehose_hot_q"
    cold = "firehose_cold_q"
    from taskq.backend._protocol import EnqueueArgs

    due = datetime.now(UTC)
    hot_args = [
        EnqueueArgs(
            id=new_job_id(),
            actor="firehose_fair",
            queue=hot,
            payload={"i": i},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=due,
        )
        for i in range(10_000)
    ]
    cold_args = [
        EnqueueArgs(
            id=new_job_id(),
            actor="firehose_fair_cold",
            queue=cold,
            payload={"i": i},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=due,
        )
        for i in range(10)
    ]
    await backend.enqueue_batch(hot_args[:5000])
    await backend.enqueue_batch(hot_args[5000:])
    await backend.enqueue_batch(cold_args)
    # The dispatch CTE gates each actor's capacity on its actor_config
    # row; a fleet probe must look registered, so seed both actors with
    # unbounded concurrency (NULL = unbounded) on their queues.
    async with deps.worker_pool.acquire() as c:
        await c.execute(
            f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) '  # noqa: S608  # Why: the schema is this harness's own constant, never user input.
            "VALUES ($1, $2), ($3, $4) ON CONFLICT (actor) DO NOTHING",
            "firehose_fair",
            hot,
            "firehose_fair_cold",
            cold,
        )

    # 20 dispatch rounds, limit 50, round-robin across BOTH queues.
    cold_claimed = 0
    hot_claimed = 0
    round_latencies: list[float] = []
    for _ in range(20):
        t0 = time.perf_counter()
        jobs = await backend.dispatch_batch(
            WORKER_ID,
            [hot, cold],
            50,
            timedelta(seconds=30),
        )
        round_latencies.append((time.perf_counter() - t0) * 1000)
        for job in jobs:
            if job.queue == cold:
                cold_claimed += 1
            elif job.queue == hot:
                hot_claimed += 1

    await stack.aclose()

    return {
        "hot_queue_pending": 10_000,
        "cold_queue_pending": 10,
        "dispatch_rounds": 20,
        "limit_per_round": 50,
        "hot_claimed": hot_claimed,
        "cold_claimed": cold_claimed,
        "cold_fully_drained": cold_claimed == 10,
        "round_ms_p50": round(median(round_latencies), 2),
        "round_ms_max": round(max(round_latencies), 2),
    }


async def _make_settings():
    from taskq.settings import WorkerSettings

    os.environ["TASKQ_PG_DSN"] = DSN
    os.environ["TASKQ_SCHEMA_NAME"] = SCHEMA
    os.environ["TASKQ_ENVIRONMENT"] = "dev"
    return WorkerSettings.load()


async def main() -> None:
    await _make_settings()
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=8)
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
    await apply_pending(conn, schema=SCHEMA)
    await conn.close()

    events = await section_event_insert(pool)
    fairness = await section_fairness()
    print(json.dumps({"event_insert_flood": events, "dispatch_fairness": fairness}, indent=2))
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
