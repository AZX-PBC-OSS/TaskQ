# ruff: noqa: S608  # Why: schema name is fixture-generated and validated by the
# migration runner's _IDENT_RE; asyncpg has no parameter binding for identifiers.

"""`01.00.02_01`'s index build takes a SHARE lock: readers keep flowing.

`docs/architecture.md`'s index-build paragraph claims, of the bundled
`01.00.02_01_pre_job_events_outbox.sql` `CREATE INDEX`:

* the build holds a `SHARE` lock on `job_events`;
* a plain reader (`SELECT count(*)`) keeps flowing while it runs
  (`ACCESS SHARE` is compatible with `SHARE`);
* a writer (`INSERT`) waits the build out (`SHARE` conflicts with the
  `ROW EXCLUSIVE` mode writers take).

This pin executes the migration's actual statement on a live Postgres and
observes what a reader and a writer can do through the lock hierarchy while
the build holds its lock — the two behaviours an operator scheduling the
migration cares about.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from taskq import migrate as mig
from taskq._ids import new_base62

pytestmark = pytest.mark.integration

_POPULATE_ROWS = 6_000_000


async def _migrated_schema(pg_dsn: str) -> str:
    schema = f"atk_idx_{new_base62()}"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await mig.apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    return schema


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def _watch_build_locks(
    conn: asyncpg.Connection,
    schema: str,
    stop: asyncio.Event,
) -> set[str]:
    """Collect every table-lock mode observed on job_events (any pid) until
    *stop* is set.

    Each poll is one short fully-resolved query on this dedicated
    connection - the loop never holds an operation open across iterations,
    so the connection is idle when ``stop`` ends the loop and closing it
    cannot race a mid-flight statement. The ``pg_locks`` row set is
    database-scoped via ``l.database`` (``pg_locks`` itself is
    cluster-wide; the regclass cast resolves in this database, the
    explicit oid match keeps the probe honest about it), pinned
    suite-wide by ``test_suite_hygiene.py``.
    """
    observed: set[str] = set()
    while not stop.is_set():
        rows = await conn.fetch(
            """
            SELECT l.mode FROM pg_locks l
            WHERE l.relation = $1::regclass
              AND l.database = (SELECT oid FROM pg_database
                                WHERE datname = current_database())
              AND l.granted
            """,
            f'"{schema}".job_events',
        )
        observed.update(str(r["mode"]) for r in rows)
        await asyncio.sleep(0.01)
    return observed


async def _wait_writer_blocked(
    conn: asyncpg.Connection,
) -> bool:
    """Poll pg_stat_activity until the writer INSERT is waiting on a lock.

    Runs on its OWN dedicated connection: the watcher loop above is a
    second session polling the same server, and two overlapping
    operations on one asyncpg connection raise ``InterfaceError``. The
    query is scoped to the current database (``pg_stat_activity`` is
    cluster-wide and the shared container hosts every xdist worker's
    database), and to *active* lock waiters other than the probing
    backend itself, so only our writer can satisfy it.
    """
    for _ in range(500):
        rows = await conn.fetch(
            """
            SELECT 1 FROM pg_stat_activity
            WHERE datname = current_database()
              AND wait_event_type = 'Lock'
              AND state = 'active'
              AND pid <> pg_backend_pid()
              AND query LIKE '%writer_probe%'
            """
        )
        if rows:
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.mark.asyncio
async def test_index_build_share_lock_lets_readers_flow_and_writers_queue(
    pg_dsn: str,
) -> None:
    """The migration's CREATE INDEX holds SHARE on job_events: a reader
    returns while the build holds the lock, a writer waits it out."""
    schema = await _migrated_schema(pg_dsn)
    conns: list[asyncpg.Connection] = []

    async def _close_all() -> None:
        # Every session here is a dedicated connection and every await is
        # fully resolved before this runs (the watcher's last poll resolves
        # before ``stop`` ends its loop), so no close can race a mid-flight
        # operation.
        for c in conns:
            if not c.is_closed():
                await c.close()

    try:
        setup = await asyncpg.connect(pg_dsn)
        conns.append(setup)
        # A parent row for job_events' FK, then enough events that the build
        # is observably long.
        await setup.execute(
            f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts,
                 status, retry_kind)
                VALUES (gen_random_uuid(), 'docs_contract', 'default', '{{}}'::jsonb, 3,
                        'succeeded', 'transient')"""
        )
        await setup.execute(
            f"""
            INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail)
            SELECT j.id, clock_timestamp(), 'state_change',
                   jsonb_build_object('reason', CASE WHEN g % 2 = 0
                       THEN 'lock_expired' ELSE 'other' END)
            FROM generate_series(1, {_POPULATE_ROWS}) g
            CROSS JOIN LATERAL (
                SELECT id FROM "{schema}".jobs LIMIT 1
            ) j
            """
        )
        # The migrations already applied, so the index exists: drop it so the
        # executed statement is a real build, not an IF NOT EXISTS no-op.
        await setup.execute(f'DROP INDEX "{schema}".job_events_reclaim_idx')

        build, reader, writer, watcher, prober = await asyncio.gather(
            asyncpg.connect(pg_dsn),
            asyncpg.connect(pg_dsn),
            asyncpg.connect(pg_dsn),
            asyncpg.connect(pg_dsn),
            asyncpg.connect(pg_dsn),
        )
        conns.extend([build, reader, writer, watcher, prober])
        ddl = next(m for m in mig.discover() if m.version == "01.00.02_01").render(schema)

        build_task = asyncio.create_task(build.execute(ddl))
        stop = asyncio.Event()
        watch_task = asyncio.create_task(_watch_build_locks(watcher, schema, stop))

        # Reader: the same plain reader query the doc describes. Writer: a
        # tagged INSERT so the blocked-writer probe can find it. Both are
        # submitted while the build is running.
        await asyncio.sleep(0.1)
        read_task = asyncio.create_task(
            reader.fetchval(f'SELECT count(*) FROM "{schema}".job_events')
        )
        write_task = asyncio.create_task(
            writer.execute(
                f"""INSERT INTO "{schema}".job_events (id, job_id, occurred_at, kind, detail)
                    SELECT 99999999, id, clock_timestamp(), 'writer_probe',
                           '{{}}'::jsonb FROM "{schema}".jobs LIMIT 1"""
            )
        )
        writer_blocked = await _wait_writer_blocked(prober)

        await asyncio.gather(build_task, read_task, write_task)
        stop.set()
        observed = await watch_task

        # Readers keep flowing: the reader's count returned while the build
        # was still holding its lock (a 6M-row build runs seconds; the count
        # is sub-second, so completion-before-the-build is the observable).
        assert "ShareLock" in observed, (
            f"the build never observed holding ShareLock on job_events "
            f"(observed: {sorted(observed)}) — the doc's lock-mode claim is stale"
        )
        assert read_task.done() and not read_task.exception(), "the reader failed during the build"
        cnt = read_task.result()
        assert cnt >= _POPULATE_ROWS, (
            f"the reader saw {cnt} rows; the build-time read is not reading the "
            "same table the writer is queueing into"
        )
        assert writer_blocked, (
            "the writer INSERT never observed waiting on a lock during the "
            "build: SHARE is no longer queueing ROW EXCLUSIVE writers, which "
            "invalidates the write-stall warning the doc carries"
        )
    finally:
        await _close_all()
        await _drop_schema(pg_dsn, schema)
