"""The insights page's six-read N+1 at FLEET SCALE.

The audit measured the six sequential reads at 0.6 ms of a 13.7 ms page
(4.4%) "on a populated ledger" — one queue, 8 actors. This bench asks
whether the round-trip share's validity is scale-bound: populate the
ledger at three fleet sizes (1, 50, 200 queues with routed actors) and
time the six fetches the route actually runs, in the route's order, on
one checkout.

Not a red/green: the question is whether the "aggregates dominate"
verdict HOLDS as the fleet grows, or the six round trips' share grows.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_BENCH_DIR.parent))

DSN = os.environ.get("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5432/taskq")
RESULTS = _BENCH_DIR / "results" / "attack-insights-scale.json"


async def build_schema(dsn: str, schema: str, n_queues: int, n_done: int, n_pending: int) -> None:
    import asyncpg

    from taskq._ids import new_job_id, new_uuid
    from taskq.migrate import apply_pending_locked

    conn = await asyncpg.connect(dsn)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await conn.close()
    await apply_pending_locked(dsn, schema=schema)

    conn = await asyncpg.connect(dsn)
    now = datetime.now(UTC)
    for q in range(n_queues):
        queue = f"q{q:04d}"
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2)',  # noqa: S608  # Why: the schema ident is bench-owned, the values are parameterized.
            queue,
            "strict_fifo",
        )
        for a in range(4):  # 4 actors per queue, distinct per-queue names
            actor = f"act_{q:04d}_{a}"
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '  # noqa: S608  # Why: the schema ident is bench-owned, the values are parameterized.
                f"VALUES ($1, $2, $3)",
                actor,
                queue,
                8,
            )
    # one live worker subscribing to every queue
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, last_seen_at, queues) '  # noqa: S608  # Why: the schema ident is bench-owned, the values are parameterized.
        f"VALUES ($1, $2, $3, $4, $5)",
        new_uuid(),
        "attack-bench",
        12345,
        now,
        [f"q{q:04d}" for q in range(n_queues)],
    )
    # terminal jobs (finished inside the wait window) spread over queues/actors
    rows = []
    for k in range(n_done):
        q = k % n_queues
        a = k % 4
        rows.append(
            (
                new_job_id(),
                f"act_{q:04d}_{a}",
                f"q{q:04d}",
                "succeeded",
                json.dumps({"n": k}),
                now - timedelta(seconds=k % 3600),
                now - timedelta(seconds=k % 3600, milliseconds=50),
                now - timedelta(seconds=k % 3600, milliseconds=150),
            )
        )
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, status, payload, scheduled_at, started_at, finished_at, max_attempts, retry_kind) '  # noqa: S608  # Why: the schema ident is bench-owned, the values are parameterized.
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 3, 'transient')",
        rows,
    )
    rows = []
    for k in range(n_pending):
        q = k % n_queues
        rows.append(
            (
                new_job_id(),
                f"act_{q:04d}_{k % 4}",
                f"q{q:04d}",
                "pending",
                json.dumps({"n": k}),
                now - timedelta(seconds=k % 600),
            )
        )
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, status, payload, scheduled_at, max_attempts, retry_kind) '  # noqa: S608  # Why: the schema ident is bench-owned, the values are parameterized.
        "VALUES ($1, $2, $3, $4, $5, $6, 3, 'transient')",
        rows,
    )
    await conn.close()


async def time_route(dsn: str, schema: str, window: timedelta) -> dict:
    """Time the six reads exactly as insights_page runs them, one checkout."""
    import asyncpg

    from taskq.insights import (
        fetch_actor_backlog,
        fetch_cron_ledger,
        fetch_drain_estimates,
        fetch_overprovisioning,
        fetch_queue_imbalance,
        fetch_wait_distribution,
    )

    conn = await asyncpg.connect(dsn)
    timings = {}
    t0 = time.perf_counter()

    t = time.perf_counter()
    await fetch_wait_distribution(conn, schema=schema, window=window, per_actor=False)
    timings["wait_distribution_ms"] = round((time.perf_counter() - t) * 1000, 2)

    t = time.perf_counter()
    await fetch_queue_imbalance(conn, schema=schema, worker_liveness_seconds=30)
    timings["queue_imbalance_ms"] = round((time.perf_counter() - t) * 1000, 2)

    t = time.perf_counter()
    await fetch_actor_backlog(conn, schema=schema)
    timings["actor_backlog_ms"] = round((time.perf_counter() - t) * 1000, 2)

    t = time.perf_counter()
    await fetch_overprovisioning(conn, schema=schema, window=window, worker_liveness_seconds=30)
    timings["overprovisioning_ms"] = round((time.perf_counter() - t) * 1000, 2)

    t = time.perf_counter()
    await fetch_drain_estimates(conn, schema=schema, window=window)
    timings["drain_estimates_ms"] = round((time.perf_counter() - t) * 1000, 2)

    t = time.perf_counter()
    await fetch_cron_ledger(conn, schema=schema, window=window)
    timings["cron_ledger_ms"] = round((time.perf_counter() - t) * 1000, 2)

    timings["six_reads_sequential_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    await conn.close()
    return timings


async def main() -> None:
    dsn = DSN
    out = []
    window = timedelta(hours=1)
    for n_queues, n_done, n_pending in [
        (1, 6000, 100),
        (50, 6000, 2000),
        (200, 6000, 4000),
    ]:
        schema = f"tq_bench_insights_{n_queues}"
        await build_schema(dsn, schema, n_queues, n_done, n_pending)
        # two passes: first warms caches, second is the recorded read
        await time_route(dsn, schema, window)
        timings = await time_route(dsn, schema, window)
        row = {
            "queues": n_queues,
            "actors": n_queues * 4,
            "terminal_jobs": n_done,
            "pending_jobs": n_pending,
            **timings,
        }
        out.append(row)
        print(row)
    RESULTS.write_text(json.dumps(out, indent=2))
    print(f"wrote {RESULTS}")


if __name__ == "__main__":
    asyncio.run(main())
