"""``cleanup_stale_workers``: one call = one bounded, committed batch.

The stale-worker sweep used to be an UNBOUNDED single DELETE whose DDL
``ON DELETE`` clauses fan out per deleted worker — each removed worker
rewrites every ``job_attempts`` row it holds (``ON DELETE SET NULL``) in
the same transaction, so a whole-fleet crash is one multi-second
transaction. The rewrite windows stale ids in a MATERIALIZED CTE
(``LIMIT $3``), so one call deletes at most ``batch_size`` workers and
the sweep loop drains the remainder one bounded batch per tick; the
window bounds workers per call, which bounds the referential fan-out per
transaction.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only the module fixture's validated schema identifier; all values are $n-bound.

import inspect
from datetime import timedelta
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._leader_shared import cleanup_stale_workers

_TOTAL_STALE = 12
_BATCH = 5
_STALENESS = timedelta(minutes=5)


class _ExecuteRecorder:
    """ConnLike proxy recording every ``execute`` statement (SQL + args).

    The bounded sweep is a single execute, so this is the whole surface it
    needs; the passthrough keeps behaviour real.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return await self._conn.execute(sql, *args)


async def _insert_worker(
    conn: asyncpg.Connection,
    schema: str,
    *,
    last_seen_ago: timedelta,
) -> UUID:
    worker_id = new_uuid()
    insert_sql = (
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, last_seen_at) '
        "VALUES ($1, 'test-host', 4242, ARRAY['default'], clock_timestamp() - $2::interval)"
    )
    await conn.execute(insert_sql, worker_id, last_seen_ago)
    return worker_id


async def _insert_attempt_owner(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
) -> UUID:
    """One terminal job + one job_attempts row owned by *worker_id* — the
    rows the ``ON DELETE SET NULL`` clause rewrites when the worker goes."""
    job_id = new_uuid()
    job_sql = (
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, priority, scheduled_at, finished_at) "
        "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 1, 'non_retryable', "
        "'succeeded', 0, now(), now())"
    )
    await conn.execute(job_sql, job_id)
    attempt_sql = (
        f'INSERT INTO "{schema}".job_attempts (job_id, attempt, started_at, '
        "outcome, duration_ms, worker_id) "
        "VALUES ($1, 1, now(), 'succeeded', 10, $2)"
    )
    await conn.execute(attempt_sql, job_id, worker_id)
    return job_id


async def _worker_ids(conn: asyncpg.Connection, schema: str) -> set[UUID]:
    rows = await conn.fetch(f'SELECT id FROM "{schema}".workers')
    return {row["id"] for row in rows}


def test_signature_carries_batch_size() -> None:
    """The bound must be part of the callable's contract, not a caller's
    hope: ``cleanup_stale_workers`` exposes a keyword-only ``batch_size``."""
    params = inspect.signature(cleanup_stale_workers).parameters
    assert "batch_size" in params, (
        "cleanup_stale_workers has no batch_size parameter — one call is an "
        "unbounded DELETE again; signature is "
        f"{inspect.signature(cleanup_stale_workers)}"
    )


@pytest.mark.integration
async def test_one_call_deletes_at_most_batch_size_and_drains(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """``batch_size=5`` against 12 stale workers: exactly 5 deleted per
    call, the caller and a live peer survive every call, and repeated calls
    drain all of them."""
    schema = module_pg_schema.schema_name
    caller_id = await _insert_worker(clean_pg_conn, schema, last_seen_ago=timedelta(hours=1))
    live_peer = await _insert_worker(clean_pg_conn, schema, last_seen_ago=timedelta(seconds=1))
    # The caller's own row is deliberately stale too: the sweep must skip it
    # on identity, not on freshness, in every bounded batch.
    for _ in range(_TOTAL_STALE):
        await _insert_worker(clean_pg_conn, schema, last_seen_ago=timedelta(hours=1))

    first = await cleanup_stale_workers(
        clean_pg_conn,
        worker_id=caller_id,
        staleness=_STALENESS,
        schema=schema,
        batch_size=_BATCH,
    )
    assert first == _BATCH, (
        f"one call deleted {first} workers; the window must cap a call at batch_size={_BATCH}"
    )
    assert len(await _worker_ids(clean_pg_conn, schema)) == _TOTAL_STALE + 2 - _BATCH, (
        "the uncapped remainder must be left for later calls"
    )

    second = await cleanup_stale_workers(
        clean_pg_conn,
        worker_id=caller_id,
        staleness=_STALENESS,
        schema=schema,
        batch_size=_BATCH,
    )
    assert second == _BATCH
    third = await cleanup_stale_workers(
        clean_pg_conn,
        worker_id=caller_id,
        staleness=_STALENESS,
        schema=schema,
        batch_size=_BATCH,
    )
    assert third == _TOTAL_STALE - 2 * _BATCH
    fourth = await cleanup_stale_workers(
        clean_pg_conn,
        worker_id=caller_id,
        staleness=_STALENESS,
        schema=schema,
        batch_size=_BATCH,
    )
    assert fourth == 0, "an empty window must delete nothing"

    assert await _worker_ids(clean_pg_conn, schema) == {caller_id, live_peer}


@pytest.mark.integration
async def test_statement_carries_the_limit(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The SQL the connection actually receives bounds the candidate window
    with a parameterized LIMIT (DELETE cannot take LIMIT directly, so the
    bound must live in a windowing CTE / IN-subquery)."""
    schema = module_pg_schema.schema_name
    caller_id = await _insert_worker(clean_pg_conn, schema, last_seen_ago=timedelta(seconds=1))
    await _insert_worker(clean_pg_conn, schema, last_seen_ago=timedelta(hours=1))

    recorder = _ExecuteRecorder(clean_pg_conn)
    deleted: int = await cleanup_stale_workers(
        cast(ConnLike, recorder),
        worker_id=caller_id,
        staleness=_STALENESS,
        schema=schema,
        batch_size=1,
    )
    assert deleted == 1
    assert recorder.execute_calls, "cleanup_stale_workers must issue its statement"
    sql, args = recorder.execute_calls[0]
    assert "LIMIT $3" in sql, (
        f"the deletion statement carries no parameterized LIMIT — one call is "
        f"unbounded again; got: {sql!r}"
    )
    assert args == (_STALENESS, caller_id, 1), (
        f"the LIMIT parameter must bind batch_size; got {args!r}"
    )


@pytest.mark.integration
async def test_fanout_is_bounded_by_the_window(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The referential fan-out is per deleted worker, so the window bounds
    it per call: after one bounded call only the deleted workers' attempt
    rows are rewritten; not-yet-deleted workers keep their history."""
    schema = module_pg_schema.schema_name
    caller_id = await _insert_worker(clean_pg_conn, schema, last_seen_ago=timedelta(seconds=1))
    stale_ids = [
        await _insert_worker(clean_pg_conn, schema, last_seen_ago=timedelta(hours=1))
        for _ in range(_TOTAL_STALE)
    ]
    for worker_id in stale_ids:
        await _insert_attempt_owner(clean_pg_conn, schema, worker_id)

    deleted = await cleanup_stale_workers(
        clean_pg_conn,
        worker_id=caller_id,
        staleness=_STALENESS,
        schema=schema,
        batch_size=_BATCH,
    )
    assert deleted == _BATCH

    surviving = await _worker_ids(clean_pg_conn, schema)
    deleted_ids = {wid for wid in stale_ids if wid not in surviving}
    assert len(deleted_ids) == _BATCH
    orphaned_sql = f'SELECT count(*) FROM "{schema}".job_attempts WHERE worker_id IS NULL'
    orphaned = await clean_pg_conn.fetchval(orphaned_sql)
    assert orphaned == _BATCH, (
        f"exactly the {_BATCH} deleted workers' attempt rows may be rewritten "
        f"in one bounded call; got {orphaned}"
    )
    still_owned_sql = f'SELECT count(*) FROM "{schema}".job_attempts WHERE worker_id IS NOT NULL'
    still_owned = await clean_pg_conn.fetchval(still_owned_sql)
    assert still_owned == _TOTAL_STALE - _BATCH, (
        "the not-yet-deleted workers' attempt rows must keep their worker_id"
    )
