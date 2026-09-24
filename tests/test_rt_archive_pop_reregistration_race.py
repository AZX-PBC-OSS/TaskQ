# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the fixtures; every value is $-bound or a module constant.
"""RED-TEAM pins: the archive pop's re-registration race (the S3 horizon).

The archive/prune pops an idempotency identity by DELETEing the live row
(the composite arbiter index lives on ``jobs`` only, the documented dedup
horizon ends at the prune). Two interleavings of a RE-REGISTRATION (a
fresh enqueue carrying the same ``(idempotency_scope, idempotency_key)``
pair) against that pop, both against real PostgreSQL:

1. The delete IN FLIGHT (uncommitted) when the re-registration's arbiter
   INSERT runs: the arbiter waits for the deleting transaction, re-checks
   on the committed outcome, and the re-registration WINS right there
   (a new row, exactly one live row, the archive row stands). Pinned
   through the real ``_enqueue_on_conn``.

2. The delete COMMITTING inside the arbiter's post-decision window: on
   the pool path the single enqueue runs the ``lock_timeout`` restore
   (a real awaited round trip) between the INSERT statement and the
   follow-up ``enqueue_select_by_key`` read. An archive transaction that
   runs entirely inside that yield commits before the follow-up read's
   READ COMMITTED snapshot, the read finds NOTHING, and the pre-fix code
   raised ``RuntimeError`` ("ON CONFLICT fired but follow-up SELECT found
   no row"): the re-registration lost a race the product semantic says it
   must win, with a bug-class internal error instead of a handle. The
   deterministic repro parks the enqueue inside the real ``mark_wrote``
   hook (the hook the pool-owning retry guard hands in, called after the
   INSERT's acknowledgement and before the restore/SELECT) and commits
   the archive from a helper thread there. The fix: when the follow-up
   read finds the freed pair empty, the enqueue retries once and the
   re-registration wins a NEW row.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq.backend._enqueue import _enqueue_on_conn
from taskq.backend._sql_templates import render
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._leader_shared import _ARCHIVE_CTE_SQL

pytestmark = pytest.mark.integration

_SCOPE = "pop_race_scope"
_KEY = "pop_race_key"
_ACTOR = "test_actor"

_START = datetime(2026, 1, 1, tzinfo=UTC)


async def _seed_terminal_holder(conn: asyncpg.Connection, schema: str) -> UUID:
    """The pair's live holder: a terminal job aged past retention."""
    rows = await conn.fetch(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "finished_at, idempotency_scope, idempotency_key) VALUES "
        "(gen_random_uuid(), $3, 'default', '{}'::jsonb, 'succeeded', 3, "
        "'transient', clock_timestamp() - interval '1 day', $1, $2) "
        "RETURNING id",
        _SCOPE,
        _KEY,
        _ACTOR,
    )
    job_id: UUID = rows[0]["id"]
    return job_id


def _commit_archive_from_thread(pg_dsn: str, schema: str, holder_id: UUID) -> threading.Event:
    """Run the real archive CTE to COMMIT on a helper thread's connection.

    The calling loop is suspended inside the enqueue's ``mark_wrote``
    hook (a synchronous call), which is exactly the race window: the
    enqueue's INSERT statement has completed server-side (its arbiter
    decided DO NOTHING against the live holder) and its follow-up SELECT
    has not started. The thread's commit lands the pop inside that
    window, deterministically: the join returns only after the commit.
    """
    done = threading.Event()

    def _run() -> None:
        async def _go() -> None:
            conn = await asyncpg.connect(pg_dsn)
            try:
                tx = conn.transaction()
                await tx.start()
                await conn.execute(
                    _ARCHIVE_CTE_SQL.format(schema=schema),
                    "succeeded",
                    timedelta(days=7),
                    [holder_id],
                    timedelta(0),
                )
                await tx.commit()
            finally:
                await conn.close()

        asyncio.run(_go())
        done.set()

    threading.Thread(target=_run, daemon=True).start()
    return done


async def _pair_rows(conn: asyncpg.Connection, schema: str) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT id, status::text AS status FROM "{schema}".jobs '
        "WHERE idempotency_scope = $1 AND idempotency_key = $2",
        _SCOPE,
        _KEY,
    )


async def test_reregistration_wins_when_the_archive_delete_is_in_flight(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Interleave 1: the arbiter waits out the in-flight delete and the
    re-registration wins AT the arbiter (the primary contract)."""
    schema = module_pg_schema.schema_name
    holder_id = await _seed_terminal_holder(clean_pg_conn, schema)
    sql = render(schema)

    # The archive tx: locks the holder, moves, deletes, UNCOMMITTED.
    tx = clean_pg_conn.transaction()
    await tx.start()
    await clean_pg_conn.execute(
        _ARCHIVE_CTE_SQL.format(schema=schema),
        "succeeded",
        timedelta(days=7),
        [holder_id],
        timedelta(0),
    )

    conn = await module_pg_pool.acquire()
    try:
        args = make_enqueue_args(
            actor=_ACTOR,
            idempotency_scope=_SCOPE,
            idempotency_key=_KEY,
        )
        # The arbiter blocks server-side on the deleting transaction; the
        # loop is free, so the archive commits while it waits.
        task = asyncio.create_task(
            _enqueue_on_conn(conn, sql, schema, FakeClock(start=_START), args)
        )
        await asyncio.sleep(0.5)
        await tx.commit()
        row = await asyncio.wait_for(task, timeout=10.0)
    finally:
        await module_pg_pool.release(conn)

    assert row.id != holder_id, "the re-registration must be a NEW job, not the holder"
    assert row.status in ("pending", "scheduled"), f"the new job must be live, got {row}"
    rows = await _pair_rows(clean_pg_conn, schema)
    assert len(rows) == 1 and rows[0]["id"] == row.id, (
        f"exactly one live row must hold the pair (the new one), got {rows}"
    )
    archived = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1',
        holder_id,
    )
    assert archived == 1, "the archive row must stand"


async def test_reregistration_wins_when_the_pop_commits_in_the_arbiters_window(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Interleave 2: the pop commits between the arbiter's DO NOTHING and
    the follow-up SELECT's snapshot. RED on main (RuntimeError), green
    with the retry: the re-registration wins a NEW row."""
    schema = module_pg_schema.schema_name
    holder_id = await _seed_terminal_holder(clean_pg_conn, schema)
    sql = render(schema)

    gate_started = threading.Event()

    def mark_wrote() -> None:
        # The real window: called after the INSERT's acknowledgement,
        # before the lock_timeout restore and the follow-up SELECT.
        gate_started.set()
        done = _commit_archive_from_thread(module_pg_schema.pg_dsn, schema, holder_id)
        done.wait(timeout=10.0)

    conn = await module_pg_pool.acquire()
    try:
        args = make_enqueue_args(
            actor=_ACTOR,
            idempotency_scope=_SCOPE,
            idempotency_key=_KEY,
        )
        row = await asyncio.wait_for(
            _enqueue_on_conn(
                conn, sql, schema, FakeClock(start=_START), args, mark_wrote=mark_wrote
            ),
            timeout=15.0,
        )
    finally:
        await module_pg_pool.release(conn)

    assert gate_started.is_set(), "the window hook must have run"
    assert row.id != holder_id, "the re-registration must be a NEW job, not the holder"
    assert row.status in ("pending", "scheduled"), f"the new job must be live, got {row}"
    rows = await _pair_rows(clean_pg_conn, schema)
    assert len(rows) == 1 and rows[0]["id"] == row.id, (
        f"exactly one live row must hold the pair (the new one), got {rows}"
    )
    archived = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1',
        holder_id,
    )
    assert archived == 1, "the archive row must stand"
