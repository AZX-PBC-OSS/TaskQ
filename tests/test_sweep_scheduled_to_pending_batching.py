"""Sweep 3 (scheduled → pending) writes no durable event rows.

``sweep_scheduled_to_pending`` promotes a bounded batch of due jobs. A
promotion is the scheduler's bookkeeping, not an outcome transition, and
it is one of the two acts every admission-denial cycle repeats (claim +
promote): under the 429 denial contract a denied job cycles until capacity
frees or its deadline expires, so a ``job_events`` row per promotion is
exactly the unbounded-growth vector the aggregated denial counters on the
job row replaced. The transitions of record are the terminal writes and
the sweep/cancel audit entries.

The leader's ``_scheduled_wake_loop`` wraps the sweep in a 5-second
``asyncio.timeout``: the bounded-batch discipline still governs (one
statement per batch, a per-batch commit), and the assertion here counts
statements, not seconds, so it holds in any environment. (Rewritten from
the superseded promotion-event pins: those pinned one event row per
promoted job, the shape the denial-events decision abolished.)
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

    The statement count IS the property under test: any INSERT into
    ``job_events`` from the promotion sweep is the per-row bookkeeping the
    denial contract abolished, whatever its spelling - a loop, a batch,
    anything.
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


async def test_sweep_promotion_writes_no_event_rows(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Promoting N scheduled jobs writes ZERO event rows, in zero
    event-insert round trips.

    Driving the real ``sweep_scheduled_to_pending`` and counting round
    trips as well as rows: a reintroduced per-promotion write in any
    spelling - one statement or a loop - fails both counts.
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
    assert counting.event_inserts == 0, (
        f"the promotion sweep issued {counting.event_inserts} job_events "
        "inserts - a row per promotion is the unbounded-growth vector under "
        "sustained admission denial; the transition of record is the "
        "terminal write, and contention belongs on the row's counters"
    )

    written = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".job_events WHERE kind = 'state_change'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert written == 0, "promotion is bookkeeping: no durable event rows"
    still_scheduled = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'scheduled'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert still_scheduled == 0, "every due job must leave 'scheduled'"
