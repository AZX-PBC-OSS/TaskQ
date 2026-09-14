"""Sweep 3 (scheduled → pending) must write its state-change events in one statement.

``sweep_scheduled_to_pending`` promotes a bounded batch of due jobs and
writes the batch's ``state_change`` events as ONE batched ``unnest``
INSERT inside the same transaction.  The leader's
``_scheduled_wake_loop`` wraps the sweep in a 5-second
``asyncio.timeout``: with a per-row event write inside the transaction, a
backlog large enough that N x RTT exceeds the deadline rolls the whole
promotion back, the next tick faces a strictly larger backlog, and the
sweep never commits again — the livelock shape the batching forecloses.

The wall-clock threshold is RTT-dependent, so the assertion here counts
statements, not seconds: the round-trip count is constant in N, in any
environment.

Every promoted row shares ``kind`` and ``detail`` (``from_state='scheduled'``,
``to_state='pending'`` -- the sweep CTE's own ``WHERE status = 'scheduled'``
guarantees it), so only the job ids vary and a single ``unnest`` over a
``uuid[]`` suffices, exactly as dispatch's ``INSERT_EVENTS_BATCH_SQL``
does for the identical shape.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_PROMOTED = 50


class _CountingConn:
    """Delegates to a real connection, counting event-INSERT round trips.

    The statement count IS the property under test: correctness never differed
    between the loop and a batch, only the number of awaited round trips taken
    inside the sweep's transaction while the deadline clock runs.

    Any INSERT into ``job_events`` counts -- the single-row form and the
    batched ``unnest`` form alike -- so the assertion pins the invariant (one
    statement per sweep, not one per row) rather than the spelling of the fix.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.event_inserts = 0

    async def execute(self, sql: str, *args: object) -> str:
        if sql.lstrip().upper().startswith("INSERT INTO") and ".job_events" in sql:
            self.event_inserts += 1
        result: str = await self._conn.execute(sql, *args)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


async def test_sweep_writes_one_event_statement_for_the_whole_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Promoting N scheduled jobs issues ONE event INSERT, not N.

    Driving the real ``sweep_scheduled_to_pending`` and counting round trips,
    rather than grepping its source for a `for rec in rows:` loop: a
    reintroduced loop spelled any other way -- enumerate, a comprehension of
    awaits, a helper -- is the same regression and the regex would not have
    seen it.
    """
    schema = module_pg_schema.schema_name
    job_ids = [new_uuid() for _ in range(_PROMOTED)]
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
    )

    counting = _CountingConn(clean_pg_conn)
    count = await PostgresBackend.sweep_scheduled_to_pending(
        counting,  # type: ignore[arg-type]  # Why: duck-typed connection; only execute/fetch/transaction are used.
        schema=schema,
    )

    assert count == _PROMOTED, f"all {_PROMOTED} due jobs must be promoted"
    assert counting.event_inserts == 1, (
        f"expected ONE batched event INSERT for {_PROMOTED} promoted jobs, "
        f"got {counting.event_inserts} — the per-row loop is still there, taking one "
        "awaited round trip per row inside a transaction the leader's 5-second "
        "deadline will eventually roll back in full"
    )

    written = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".job_events WHERE kind = 'state_change'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert written == _PROMOTED, "one event per promoted job must still be written"
    still_scheduled = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'scheduled'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert still_scheduled == 0, "every due job must leave 'scheduled'"
