"""``cleanup_stale_workers`` return value and row selection, against real PG.

The sweep's deleted-row count feeds the leader's observability and its
callers' logging, and nothing asserted it — returning ``None`` for every
call broke no test.  Neither did anything pin, on a real table, that the
caller's own row and a live peer's row survive the DELETE.
"""

from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.settings import TaskQSettings
from taskq.worker.leader import cleanup_stale_workers

pytestmark = pytest.mark.integration


async def _insert_worker(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    *,
    last_seen_ago: timedelta,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, last_seen_at) '  # noqa: S608  # Why: schema identifier comes from TaskQSettings, validated against _IDENT_RE; every value is $n-bound.
        "VALUES ($1, $2, $3, $4, clock_timestamp() - $5::interval)",
        worker_id,
        "test-host",
        4242,
        ["default"],
        last_seen_ago,
    )


async def _worker_ids(conn: asyncpg.Connection, schema: str) -> set[UUID]:
    rows = await conn.fetch(f'SELECT id FROM "{schema}".workers')  # noqa: S608  # Why: see above.
    return {row["id"] for row in rows}


async def test_cleanup_reports_the_number_of_rows_it_deleted(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Two stale peers go; the caller and a live peer stay; the count is 2."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    schema = settings.schema_name

    caller_id = new_uuid()
    live_peer = new_uuid()
    stale_one = new_uuid()
    stale_two = new_uuid()

    # The caller's own row is deliberately stale too: the sweep must skip it
    # on identity, not on freshness.
    await _insert_worker(pg_conn, schema, caller_id, last_seen_ago=timedelta(hours=1))
    await _insert_worker(pg_conn, schema, live_peer, last_seen_ago=timedelta(seconds=1))
    await _insert_worker(pg_conn, schema, stale_one, last_seen_ago=timedelta(hours=1))
    await _insert_worker(pg_conn, schema, stale_two, last_seen_ago=timedelta(hours=2))

    deleted = await cleanup_stale_workers(
        pg_conn,
        worker_id=caller_id,
        staleness=timedelta(minutes=5),
        schema=schema,
    )

    assert deleted == 2
    assert await _worker_ids(pg_conn, schema) == {caller_id, live_peer}


async def test_cleanup_reports_zero_when_nothing_is_stale(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A no-op sweep reports 0, not None — the count is a real measurement."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    schema = settings.schema_name

    caller_id = new_uuid()
    live_peer = new_uuid()
    await _insert_worker(pg_conn, schema, caller_id, last_seen_ago=timedelta(seconds=1))
    await _insert_worker(pg_conn, schema, live_peer, last_seen_ago=timedelta(seconds=2))

    deleted = await cleanup_stale_workers(
        pg_conn,
        worker_id=caller_id,
        staleness=timedelta(minutes=5),
        schema=schema,
    )

    assert deleted == 0
    assert await _worker_ids(pg_conn, schema) == {caller_id, live_peer}
