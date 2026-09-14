"""``sweep_expired_results``: one call = one bounded, committed batch.

The result-TTL sweep used to be an UNBOUNDED single UPDATE — one call
nulled every expired result in the table inside one transaction, so its
duration, lock-hold, and WAL volume scaled with whatever backlog of
expired results had accumulated (a retention change expiring a fleet's
results at once is one multi-second transaction). The rewrite windows
candidate ids in a MATERIALIZED CTE (``LIMIT $1``) and updates by id, so
one call commits at most ``batch_size`` expirations and the sweep loop
drains the remainder one bounded batch per tick.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only the module fixture's validated schema identifier; all values are $n-bound.

import inspect
import json
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

_TOTAL_EXPIRED = 12
_BATCH = 5


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


async def _seed_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str,
    result: str | None,
    result_expires_at: datetime | None,
) -> UUID:
    """Insert one job row with the given result-related columns."""
    job_id = new_uuid()
    insert_sql = f"""INSERT INTO "{schema}".jobs (
        id, actor, queue, payload, max_attempts, retry_kind,
        status, priority, scheduled_at, result, result_expires_at
    ) VALUES (
        $1, 'test_actor', 'default', $2::jsonb, 3, 'transient',
        $3, 0, now(), $4::jsonb, $5
    )"""
    await conn.execute(
        insert_sql,
        job_id,
        json.dumps({"k": "v"}),
        status,
        result,
        result_expires_at,
    )
    return job_id


def _eligible_sql(schema: str) -> str:
    return (
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE result_expires_at < clock_timestamp() AND result IS NOT NULL"
    )


def test_signature_carries_batch_size() -> None:
    """The bound must be part of the callable's contract, not a caller's
    hope: ``sweep_expired_results`` exposes a keyword-only ``batch_size``."""
    params = inspect.signature(PostgresBackend.sweep_expired_results).parameters
    assert "batch_size" in params, (
        "sweep_expired_results has no batch_size parameter — one call is an "
        "unbounded UPDATE again; signature is "
        f"{inspect.signature(PostgresBackend.sweep_expired_results)}"
    )


@pytest.mark.integration
async def test_one_call_expires_at_most_batch_size_and_drains(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """``batch_size=5`` against 12 expired results: exactly 5 cleared per
    call, the rest stay intact, and repeated calls drain all of them."""
    schema = module_pg_schema.schema_name
    past = datetime.now(UTC) - timedelta(hours=1)
    future = datetime.now(UTC) + timedelta(hours=1)
    for _ in range(_TOTAL_EXPIRED):
        await _seed_job(
            clean_pg_conn,
            schema,
            status="succeeded",
            result='{"r":"v"}',
            result_expires_at=past,
        )
    keep_id = await _seed_job(
        clean_pg_conn, schema, status="succeeded", result='{"r":"v"}', result_expires_at=future
    )
    no_result_id = await _seed_job(
        clean_pg_conn, schema, status="succeeded", result=None, result_expires_at=past
    )

    first = await PostgresBackend.sweep_expired_results(
        clean_pg_conn, schema=schema, batch_size=_BATCH
    )
    assert first == _BATCH, (
        f"one call expired {first} results; the window must cap a call at batch_size={_BATCH}"
    )
    remaining = await clean_pg_conn.fetchval(_eligible_sql(schema))
    assert remaining == _TOTAL_EXPIRED - _BATCH, (
        f"{remaining} eligible results after a bounded call; expected "
        f"{_TOTAL_EXPIRED - _BATCH} — the uncapped remainder must be left for "
        "later calls"
    )

    second = await PostgresBackend.sweep_expired_results(
        clean_pg_conn, schema=schema, batch_size=_BATCH
    )
    assert second == _BATCH
    third = await PostgresBackend.sweep_expired_results(
        clean_pg_conn, schema=schema, batch_size=_BATCH
    )
    assert third == _TOTAL_EXPIRED - 2 * _BATCH
    fourth = await PostgresBackend.sweep_expired_results(
        clean_pg_conn, schema=schema, batch_size=_BATCH
    )
    assert fourth == 0, "an empty window must expire nothing"

    assert await clean_pg_conn.fetchval(_eligible_sql(schema)) == 0
    keep_sql = f'SELECT result, result_expires_at FROM "{schema}".jobs WHERE id = $1'
    keep_row = await clean_pg_conn.fetchrow(keep_sql, keep_id)
    assert keep_row is not None and keep_row["result"] is not None, (
        "a not-yet-expired result must survive every bounded call"
    )
    no_result_sql = f'SELECT result FROM "{schema}".jobs WHERE id = $1'
    no_result_row = await clean_pg_conn.fetchrow(no_result_sql, no_result_id)
    assert no_result_row is not None and no_result_row["result"] is None


@pytest.mark.integration
async def test_statement_carries_the_limit(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The SQL the connection actually receives bounds the candidate window
    with a parameterized LIMIT (UPDATE cannot take LIMIT directly, so the
    bound must live in a windowing CTE)."""
    schema = module_pg_schema.schema_name
    await _seed_job(
        clean_pg_conn,
        schema,
        status="succeeded",
        result='{"r":"v"}',
        result_expires_at=datetime.now(UTC) - timedelta(hours=1),
    )

    recorder = _ExecuteRecorder(clean_pg_conn)
    count: int = await PostgresBackend.sweep_expired_results(
        cast(ConnLike, recorder), schema=schema, batch_size=1
    )
    assert count == 1
    assert recorder.execute_calls, "sweep_expired_results must issue its statement"
    sql, args = recorder.execute_calls[0]
    assert "LIMIT $1" in sql, (
        f"the expiry statement carries no parameterized LIMIT — one call is "
        f"unbounded again; got: {sql!r}"
    )
    assert args == (1,), f"the LIMIT parameter must bind batch_size; got {args!r}"
