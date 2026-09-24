"""Attack-firehose harness, section 2: the coalesced progress flush under flood.

Seeds 10k running jobs (the real migrated schema) with real dirty
``_ProgressBuffer`` accumulators - the "10k dirty buffers coalescing into
ONE flush" shape - then runs the production
:func:`taskq.progress._flush.progress_flush_loop` against the real pool
and measures:
  - the tick's statement bounds (max dirty size per tick vs the caps)
  - per-batch statement latency p50/p99 (the statement_timeout margin)
  - the drain rate (rows/tick) and the backlog's decay to zero
  - the buffer dict's footprint (bounded by ACTIVE jobs, never per event)

Usage: FIREHOSE_DSN=... uv run python benchmarks/attack_firehose_flush.py
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from statistics import median
from uuid import UUID as _UUID

import asyncpg

from taskq._ids import new_uuid
from taskq.migrate import apply_pending
from taskq.progress import _flush as flush_mod
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._flush import (
    _FLUSH_BATCH_ROWS,
    _FLUSH_MAX_BATCHES_PER_TICK,
    progress_flush_loop,
)

DSN = os.environ.get("FIREHOSE_DSN", "postgresql://taskq:taskq@127.0.0.1:55433/taskq")
SCHEMA = "firehose_flush"

WORKER_ID = new_uuid()
N_JOBS = 10_000
OBSERVE_SECONDS = 30.0

INSERT_JOBS_SQL = (
    f'INSERT INTO "{SCHEMA}".jobs '  # noqa: S608  # Why: the schema is this harness's own constant, never user input.
    """
    (id, actor, queue, payload, max_attempts, retry_kind, retry_base_seconds,
     retry_cap_seconds, retry_backoff, retry_jitter, status,
     attempt, locked_by_worker, progress_seq, progress_state)
SELECT
    id::uuid, 'firehose', 'q', '{}'::jsonb, 3, 'transient', 1.0,
    60.0, 'exponential', 0.2, 'running',
    1, $1::uuid, 0, '{}'::jsonb
FROM unnest($2::uuid[]) AS id
"""
)


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
    await apply_pending(conn, schema=SCHEMA)
    await conn.close()

    pool = await asyncpg.create_pool(DSN, min_size=4, max_size=8)

    job_ids = [new_uuid() for _ in range(N_JOBS)]
    async with pool.acquire() as c:
        for i in range(0, N_JOBS, 5000):
            await c.execute(
                INSERT_JOBS_SQL,
                WORKER_ID,
                [str(j) for j in job_ids[i : i + 5000]],
            )

    # The buffer dict exactly as dispatch seeds it: one per RUNNING job,
    # every one of them dirty (the storm's snapshot).
    buffers: dict[_UUID, _ProgressBuffer] = {}
    for jid in job_ids:
        b = _ProgressBuffer(job_id=jid, base_seq=0, attempt=1)
        b.dirty = True
        b.pending_seq_delta = 1
        b.pending_state = {"step": 1, "percent": 50.0}
        buffers[jid] = b

    shutdown = asyncio.Event()
    batch_latencies: list[tuple[float, int]] = []
    real = flush_mod._flush_dirty_set
    max_dirty_per_tick = [0]
    stmt_row_counts: list[int] = []

    async def instrumented(pool, schema, worker_id, progress_buffers, dirty):  # type: ignore[no-untyped-def]
        max_dirty_per_tick[0] = max(max_dirty_per_tick[0], len(dirty))
        before = sum(1 for b in progress_buffers.values() if not b.dirty)
        t0 = time.perf_counter()
        try:
            await real(pool, schema, worker_id, progress_buffers, dirty)
        finally:
            dt = time.perf_counter() - t0
            after = sum(1 for b in progress_buffers.values() if not b.dirty)
            batch_latencies.append((dt, len(dirty)))
            stmt_row_counts.append(after - before)

    flush_mod._flush_dirty_set = instrumented  # type: ignore[assignment]

    loop_task = asyncio.create_task(
        progress_flush_loop(
            lambda: pool,
            SCHEMA,
            WORKER_ID,
            buffers,
            0.5,
            shutdown,
        )
    )

    samples: list[tuple[float, int, int]] = []
    start = time.monotonic()
    while time.monotonic() - start < OBSERVE_SECONDS:
        dirty_now = sum(1 for b in buffers.values() if b.dirty)
        samples.append((time.monotonic() - start, len(buffers), dirty_now))
        if dirty_now == 0 and time.monotonic() - start > 3.0:
            break
        await asyncio.sleep(0.25)

    elapsed_to_drain = time.monotonic() - start
    shutdown.set()
    await asyncio.gather(loop_task, return_exceptions=True)
    flush_mod._flush_dirty_set = real  # type: ignore[assignment]

    lat_ms = sorted(lat * 1000 for lat, _ in batch_latencies)
    remaining_dirty = sum(1 for b in buffers.values() if b.dirty)

    result = {
        "jobs_seeded": N_JOBS,
        "coalesce_interval_s": 0.5,
        "batch_rows_cap": _FLUSH_BATCH_ROWS,
        "max_batches_per_tick": _FLUSH_MAX_BATCHES_PER_TICK,
        "drain_ceiling_rows_per_sec": _FLUSH_BATCH_ROWS * _FLUSH_MAX_BATCHES_PER_TICK / 0.5,
        "max_dirty_set_per_tick": max_dirty_per_tick[0],
        # Per-TICK retired-row count (the tick is 8 statements of <= 32
        # rows each, so this tops out at the tick cap, never the
        # statement bound - the per-statement bound is pinned by
        # tests/test_attack_firehose_pins.py against the statement list).
        "rows_retired_per_tick_max": max(stmt_row_counts) if stmt_row_counts else 0,
        "tick_retire_never_exceeds_tick_cap": max(stmt_row_counts)
        <= _FLUSH_MAX_BATCHES_PER_TICK * _FLUSH_BATCH_ROWS
        if stmt_row_counts
        else False,
        "flush_ticks": len(batch_latencies),
        "statement_latency_ms_p50": round(median(lat_ms), 2) if lat_ms else None,
        "statement_latency_ms_p99": round(lat_ms[int(len(lat_ms) * 0.99)], 2) if lat_ms else None,
        "statement_latency_ms_max": round(lat_ms[-1], 2) if lat_ms else None,
        "event_writer_statement_timeout_ms": 1750,
        "timeout_margin_ratio": round(1750 / max(lat_ms[-1] * 1000, 0.001), 1) if lat_ms else None,
        "seconds_to_drain_10k": round(elapsed_to_drain, 2),
        "remaining_dirty_at_end": remaining_dirty,
        "buffer_dict_final_size": len(buffers),
        "backlog_samples_1s": [
            {"t": round(t, 1), "dict_size": d, "dirty": dirty} for (t, d, dirty) in samples[::4]
        ],
    }
    print(json.dumps(result, indent=2))
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
