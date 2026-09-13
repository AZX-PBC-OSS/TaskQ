"""``complete_stale_batches``: one call = one bounded, committed batch.

The stale-batch safety net used to be an UNBOUNDED UPDATE with a
correlated NOT EXISTS — one call could complete every stale batch in the
table inside the leader's single-deadline iteration. The rewrite windows
candidates in a MATERIALIZED CTE (``LIMIT $1``) and updates by id, so one
call commits at most ``batch_size`` completions and the sweep loop drains
the remainder one bounded batch per tick.
"""

import inspect
import json
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._leader_shared import complete_stale_batches

_TOTAL_STALE = 12
_BATCH = 5


class _FetchvalRecorder:
    """ConnLike proxy recording every ``fetchval`` statement (SQL + args).

    The bounded sweep is a single fetchval, so this is the whole surface
    it needs; the passthrough keeps behaviour real.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        return await self._conn.fetchval(sql, *args)


def _count_active_sql(schema: str) -> str:
    return f"SELECT count(*) FROM \"{schema}\".batches WHERE status = 'active'"  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.


async def _seed_stale_batch(conn: asyncpg.Connection, schema: str) -> UUID:
    """Insert one ``active`` batch whose only member job is terminal.

    Seeds directly (batches row + jobs row with ``metadata.batch_id`` in a
    terminal status) so the NOT EXISTS predicate sees exactly the shape a
    lost completion hook leaves behind.
    """
    batch_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".batches (id, queue, status, expected_size) '  # noqa: S608  # Why: schema is the module fixture's validated identifier.
        "VALUES ($1, 'default', 'active', 1)",
        batch_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: schema is the module fixture's validated identifier.
        "retry_kind, status, priority, scheduled_at, metadata) "
        "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 1, 'non_retryable', "
        "'succeeded', 0, clock_timestamp(), $2::jsonb)",
        new_uuid(),
        json.dumps({"batch_id": str(batch_id)}),
    )
    return batch_id


def test_signature_carries_batch_size() -> None:
    """The bound must be part of the callable's contract, not a caller's hope:
    ``complete_stale_batches`` exposes a keyword-only ``batch_size``."""
    params = inspect.signature(complete_stale_batches).parameters
    assert "batch_size" in params, (
        "complete_stale_batches has no batch_size parameter — one call is an "
        "unbounded UPDATE again; signature is "
        f"{inspect.signature(complete_stale_batches)}"
    )


@pytest.mark.integration
async def test_one_call_completes_at_most_batch_size_and_drains(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """``batch_size=5`` against 12 stale batches: exactly 5 complete per
    call, the rest stay active, and repeated calls drain all of them."""
    schema = module_pg_schema.schema_name
    for _ in range(_TOTAL_STALE):
        await _seed_stale_batch(clean_pg_conn, schema)

    first = await complete_stale_batches(clean_pg_conn, schema=schema, batch_size=_BATCH)
    assert first == _BATCH, (
        f"one call completed {first} batches; the window must cap a call at batch_size={_BATCH}"
    )
    remaining = await clean_pg_conn.fetchval(_count_active_sql(schema))
    assert remaining == _TOTAL_STALE - _BATCH, (
        f"{remaining} batches still active after a bounded call; expected "
        f"{_TOTAL_STALE - _BATCH} — the uncapped remainder must be left for "
        "later calls"
    )

    second = await complete_stale_batches(clean_pg_conn, schema=schema, batch_size=_BATCH)
    assert second == _BATCH
    third = await complete_stale_batches(clean_pg_conn, schema=schema, batch_size=_BATCH)
    assert third == _TOTAL_STALE - 2 * _BATCH
    fourth = await complete_stale_batches(clean_pg_conn, schema=schema, batch_size=_BATCH)
    assert fourth == 0, "an empty window must complete nothing"

    assert await clean_pg_conn.fetchval(_count_active_sql(schema)) == 0


@pytest.mark.integration
async def test_statement_carries_the_limit(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The SQL the connection actually receives bounds the candidate window
    with a parameterized LIMIT (UPDATE cannot take LIMIT directly, so the
    bound must live in a windowing CTE)."""
    schema = module_pg_schema.schema_name
    await _seed_stale_batch(clean_pg_conn, schema)

    recorder = _FetchvalRecorder(clean_pg_conn)
    count: int = await complete_stale_batches(cast(ConnLike, recorder), schema=schema, batch_size=1)
    assert count == 1
    assert recorder.fetchval_calls, "complete_stale_batches must issue its statement"
    sql, args = recorder.fetchval_calls[0]
    assert "LIMIT $1" in sql, (
        f"the completion statement carries no parameterized LIMIT — one call "
        f"is unbounded again; got: {sql!r}"
    )
    assert args == (1,), f"the LIMIT parameter must bind batch_size; got {args!r}"
