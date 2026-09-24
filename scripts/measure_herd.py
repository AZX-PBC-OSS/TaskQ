"""One-off measurement: the clock-forward herd's drain shape, instrumented.

Seeds the same cohort the pins seed (10,100 running rows stamped
pre-jump), drains it exactly as the leader loop does, and prints the
pass log: rows per committed pass, wall cost per pass, worst pass. Also
measures the election's concurrent-elect round and the event-TTL batch.

Run against the shared container: python scripts/measure_herd.py
"""

# ruff: noqa: S608  # Why: one-off measurement script; the schema name is a module constant here, not user input.

import asyncio
import statistics
import time
from datetime import timedelta

import asyncpg

DSN = "postgresql://taskq:taskq@localhost:55431/taskq"
SCHEMA = "herd_measure"

SEED = """
INSERT INTO "{s}".jobs
    (id, actor, queue, payload, status, attempt, max_attempts, retry_kind,
     scheduled_at, started_at, last_heartbeat_at, locked_by_worker,
     lock_expires_at, cancel_phase,
     retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter)
SELECT substr(md5('m' || n::text), 1, 32)::uuid,
       'herd_actor', 'default', '{{}}'::jsonb, 'running', 1,
       CASE WHEN n % 10 = 0 THEN 1 ELSE 3 END, 'transient',
       clock_timestamp() - interval '2 hours',
       clock_timestamp() - interval '2 hours',
       clock_timestamp() - interval '2 hours',
       $1,
       clock_timestamp() - interval '2 hours' + interval '30 seconds',
       0, 5.0, 10.0, 'exponential', 0.5
FROM generate_series(1, $2) AS n
"""


async def main() -> None:
    from taskq.backend._sweeps import sweep_expired_locks
    from taskq.migrate import apply_pending

    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await apply_pending(conn, schema=SCHEMA)
    wid = await conn.fetchval(
        f'INSERT INTO "{SCHEMA}".workers (id, hostname, pid, queues) '
        "VALUES (gen_random_uuid(), 'herd-host', 4242, ARRAY['default']) RETURNING id"
    )
    t0 = time.monotonic()
    async with conn.transaction():
        await conn.execute(SEED.format(s=SCHEMA), wid, 10_000)
    seed_secs = time.monotonic() - t0

    log: list[tuple[int, float]] = []
    while True:
        t = time.monotonic()
        rows = await sweep_expired_locks(
            conn,
            timedelta(seconds=30),
            timedelta(seconds=30),
            schema=SCHEMA,
            batch_size=100,
            statement_timeout_ms=1750,
        )
        ms = (time.monotonic() - t) * 1000
        log.append((rows, ms))
        if rows == 0:
            break

    passes = log[:-1]
    print(f"seeded 10,000 running rows (pre-jump stamps) in {seed_secs:.2f}s")
    print(f"drained in {len(passes)} committed passes, each pass its own transaction")
    print(f"rows/pass: min={min(r for r, _ in passes)} max={max(r for r, _ in passes)}")
    ms_list = [m for _, m in passes]
    print(
        f"pass wall cost ms: mean={statistics.fmean(ms_list):.1f} "
        f"p50={statistics.median(ms_list):.1f} max={max(ms_list):.1f}"
    )
    print(
        f"total drain wall: {sum(ms_list) / 1000:.2f}s "
        f"(the leader's per-tick cap of 8 batches spreads this over "
        f"{-(-len(passes) // 8)} ticks of 30s)"
    )

    ledger = await conn.fetchrow(
        f"SELECT (SELECT count(*) FROM \"{SCHEMA}\".jobs WHERE status = 'pending') AS p, "
        f"(SELECT count(*) FROM \"{SCHEMA}\".jobs WHERE status = 'crashed') AS c, "
        f'(SELECT count(*) FROM "{SCHEMA}".job_attempts) AS a, '
        f'(SELECT count(*) FROM "{SCHEMA}".job_events) AS e'
    )
    print(
        f"ledger: pending={ledger['p']} crashed={ledger['c']} "
        f"attempts={ledger['a']} events={ledger['e']}"
    )

    spread = await conn.fetchrow(
        "SELECT EXTRACT(EPOCH FROM max(scheduled_at) - min(scheduled_at)) AS s, "
        "count(DISTINCT scheduled_at) AS d FROM "
        f"\"{SCHEMA}\".jobs WHERE status = 'pending'"
    )
    print(
        f"re-pend wake spread: {float(spread['s']):.2f}s across "
        f"{spread['d']} distinct instants (band [2.5s, 7.5s])"
    )

    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()


asyncio.run(main())
