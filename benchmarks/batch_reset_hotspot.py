# ruff: noqa: S608  # Why: schema is a fixed benchmark identifier, not user input; every value is $-bound.
"""The batch-heavy hot path's dominant cost: the per-job failure-counter reset.

Reproduces the latency red-team's batch-shape finding
(``attack-hotpath-findings.md``): on the batch shape, the reset inside
``apply_batch_terminal_outcome`` costs 8.2 ms/job - 24x the terminal
UPDATE's 0.34 ms - with ``Lock:transactionid``/``Lock:tuple`` waits and
ungranted ``pg_locks`` counts up to 5.

Shape: B batches x M members, each member driven through the REAL terminal
path (terminal job UPDATE + ``apply_batch_terminal_outcome`` in its own
transaction) by C concurrent workers - the fleet's "many batches in flight,
every successful job pays reset+complete" shape. Measures:

* per-job terminal-path wall (mean over the drain);
* the reset statement's pg_stat_statements share (calls, mean_exec_time,
  total_exec_time) - the finding's 8.2 ms/job number is its mean_exec_time;
* ungranted-lock samples (the ``Lock:`` wait evidence).

Every successful job's reset runs against a batch whose
``consecutive_failures`` is 0 - the overwhelmingly common case the finding
names: the noop actors never fail, so the reset re-writes an already-zero
counter, taking the batches row lock (and paying the open-member count the
statement's LATERAL carries) once per job.

Usage:
    python benchmarks/batch_reset_hotspot.py --jobs 2000 --batch-size 200
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import asyncpg

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import JobId, JobRow
from taskq.batch import apply_batch_terminal_outcome

# Why: the fleet harnesses reuse the test bootstrap's backend construction
# verbatim (attack_hotpath_shapes.py's precedent).
from taskq.testing.fixtures import _open_pg_backend  # pyright: ignore[reportPrivateUsage]

RESULTS_DIR = Path(__file__).resolve().parent / "results"

# The reset statement's pg_stat_statements fingerprint: the counter write
# the finding measures, spelled so no other statement matches it (constant
# literals are normalized to $N placeholders).
_RESET_FINGERPRINT = "consecutive_failures = $1"
_INCREMENT_FINGERPRINT = "consecutive_failures = consecutive_failures + $1"
_COMPLETE_FINGERPRINT = "completed_at = clock_timestamp()"
_TERMINAL_FINGERPRINT = "finished_at = clock_timestamp()"


async def _stmt_stats(
    conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record], fingerprint: str
) -> dict[str, object]:
    row = await conn.fetchrow(
        "SELECT calls, round(mean_exec_time::numeric, 3) AS mean_ms, "
        "round(total_exec_time::numeric, 1) AS total_ms "
        "FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database "
        "WHERE datname = current_database()) AND query LIKE $1",
        f"%{fingerprint}%",
    )
    if row is None:
        return {"calls": 0, "mean_ms": None, "total_ms": None}
    return {
        "calls": row["calls"],
        "mean_ms": float(row["mean_ms"]),
        "total_ms": float(row["total_ms"]),
    }


def _hook_job(jid: UUID, bid: UUID) -> JobRow:
    """The minimal JobRow the terminal hook reads: id + metadata.batch_id."""
    return JobRow(
        id=JobId(jid),
        actor="bench_actor",
        queue="default",
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=1,
        retry_kind="non_retryable",
        created_at=datetime.now(UTC),
        scheduled_at=datetime.now(UTC),
        metadata={"batch_id": str(bid)},
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="postgresql://taskq:taskq@127.0.0.1:5499/taskq")
    parser.add_argument("--jobs", type=int, default=2_000)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    schema = f"batch_hotspot_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(args.dsn, schema_name=schema)
    n_batches = max(1, args.jobs // args.batch_size)

    # Seed the batches and their members the way enqueue_batch does: a
    # batches row with a failure threshold + members stamped with the
    # batch id, all parked on 'running' so each worker's terminal write
    # is the production move.
    job_rows: list[tuple[UUID, UUID]] = []
    async with deps.worker_pool.acquire() as conn:
        await conn.execute("SELECT pg_stat_statements_reset()")
        for _ in range(n_batches):
            bid = new_uuid()
            await backend.create_batch(bid, "default", args.batch_size, 10_000, None, None)
            meta = f'{{"batch_id": "{bid}"}}'
            ids = [new_job_id() for _ in range(args.batch_size)]
            await conn.executemany(
                f'INSERT INTO "{schema}".jobs '
                "(id, queue, actor, payload, max_attempts, retry_kind, metadata, status, "
                "started_at, last_heartbeat_at, locked_by_worker, lock_expires_at) "
                "VALUES ($1, 'default', 'bench_actor', '{}'::jsonb, 1, 'non_retryable', "
                "$2::jsonb, 'running', clock_timestamp(), clock_timestamp(), "
                "gen_random_uuid(), clock_timestamp() + interval '600 seconds')",
                [(jid, meta) for jid in ids],
            )
            job_rows.extend((jid, bid) for jid in ids)
        await conn.execute(f'ANALYZE "{schema}".jobs')

    pending: asyncio.Queue[tuple[UUID, UUID]] = asyncio.Queue()
    for pair in job_rows:
        pending.put_nowait(pair)

    ungranted_samples: list[int] = []
    stop_sampling = asyncio.Event()

    async def _sample_locks() -> None:
        async with asyncpg.create_pool(args.dsn, min_size=1, max_size=1) as pool:
            while not stop_sampling.is_set():
                n = await pool.fetchval("SELECT count(*) FROM pg_locks WHERE NOT granted")
                ungranted_samples.append(int(n))
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop_sampling.wait(), timeout=0.05)

    async def _one_terminal_job() -> None:
        jid, bid = await pending.get()
        try:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    jid,
                )
                await apply_batch_terminal_outcome(
                    backend, _hook_job(jid, bid), "succeeded", transaction_conn=conn
                )
        finally:
            pending.task_done()

    async def _worker() -> None:
        while not pending.empty():
            try:
                await _one_terminal_job()
            except asyncio.QueueEmpty:
                return

    sampler = asyncio.create_task(_sample_locks())
    t0 = time.perf_counter()
    await asyncio.gather(*(_worker() for _ in range(args.concurrency)))
    drain_s = time.perf_counter() - t0
    stop_sampling.set()
    await sampler

    async with deps.worker_pool.acquire() as conn:
        reset_stats = await _stmt_stats(conn, _RESET_FINGERPRINT)
        increment_stats = await _stmt_stats(conn, _INCREMENT_FINGERPRINT)
        complete_stats = await _stmt_stats(conn, _COMPLETE_FINGERPRINT)
        terminal_stats = await _stmt_stats(conn, _TERMINAL_FINGERPRINT)
        active_batches = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".batches WHERE status = 'active'"
        )

    result = {
        "shape": "batch-terminal-path",
        "jobs": len(job_rows),
        "batches": n_batches,
        "batch_size": args.batch_size,
        "concurrency": args.concurrency,
        "drain_s": round(drain_s, 3),
        "per_job_wall_ms": round(drain_s / len(job_rows) * 1000, 3),
        "reset_statement": reset_stats,
        "increment_statement": increment_stats,
        "complete_statement": complete_stats,
        "terminal_update_statement": terminal_stats,
        "ungranted_locks_max": max(ungranted_samples) if ungranted_samples else 0,
        "active_batches_after": active_batches,
        "generated_at": datetime.now(UTC).isoformat(),
        "schema": schema,
    }
    print(json.dumps(result, indent=2))

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = args.out or str(RESULTS_DIR / f"batch-hotspot-{args.jobs}j-{args.concurrency}c.json")
    Path(out).write_text(json.dumps(result, indent=2))  # noqa: ASYNC240  # Why: the run is over; the write is the artifact
    print(f"[bench] wrote {out}")

    await stack.aclose()
    cleanup = await asyncpg.connect(args.dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), timeout=timedelta(minutes=30).total_seconds()))
