"""Red-team attacks on the sweeps' pg_notify channel semantics.

Sweep 1's single-notify-per-call contract is pinned
(``test_postgres_sweeps.py``'s ``test_single_pg_notify_per_sweep_with_rows``);
sweep 3's is not, and sweep 3 runs every second on the leader — a
per-row notify regression there is a wake storm on the hottest cadence
in the system.  This file pins:

* sweep 3 fires exactly ONE notification per call that promotes at
  least one row, NONE on an empty call, on the schema-qualified
  ``wake_channel`` — the channel ``subscribe_wake`` consumers listen on;
* sweep 2 fires NO notification at all: it is not a wake-channel writer
  today, and the channel's meaning ("new dispatchable work or something
  changed on job_events") is a documented semantic — bolting a notify
  onto the deadline sweep would wake every dispatch worker in the fleet
  for jobs that will never dispatch again.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.constants import wake_channel
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

# Long enough for asyncpg's listener delivery to land (notifications are
# delivered on the listener connection's socket traffic); short enough
# to keep the file quick.
_DELIVERY_BEAT = 0.3


async def _listen(dsn: str, channel: str) -> tuple[asyncpg.Connection, list[str]]:
    """Open a listener connection recording every notification channel."""
    conn = await asyncpg.connect(dsn)
    seen: list[str] = []

    def _on_notify(c: object, pid: int, ch: str, payload: str) -> None:
        seen.append(ch)

    await conn.add_listener(
        channel,
        _on_notify,  # type: ignore[arg-type]  # Why: asyncpg stubs over-narrow the callback type — same suppression as the notify tests under attack.
    )
    return conn, seen


async def _seed_due_scheduled(conn: asyncpg.Connection, schema: str, count: int) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid() for _ in range(count)],
    )


async def _seed_overdue_deadline(conn: asyncpg.Connection, schema: str, count: int) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, "
        " schedule_to_close) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp() - interval '60 seconds', clock_timestamp() - interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid() for _ in range(count)],
    )


async def test_sweep3_fires_exactly_one_schema_qualified_wake_per_call(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Sweep 3: one notify per promoting call, none on an empty call, on
    the schema-qualified wake channel."""
    schema = module_pg_schema.schema_name
    channel = wake_channel(schema)
    listener, seen = await _listen(module_pg_schema.pg_dsn, channel)
    try:
        await _seed_due_scheduled(clean_pg_conn, schema, 5)

        count = await PostgresBackend.sweep_scheduled_to_pending(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            schema=schema,
        )
        assert count == 5
        await asyncio.sleep(_DELIVERY_BEAT)
        assert seen == [channel], (
            f"a 5-row promotion must fire exactly one wake notification on "
            f"{channel!r}, saw {seen} — a per-row notify is a wake storm on the "
            "leader's every-second cadence"
        )

        seen.clear()
        empty = await PostgresBackend.sweep_scheduled_to_pending(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            schema=schema,
        )
        assert empty == 0
        await asyncio.sleep(_DELIVERY_BEAT)
        assert seen == [], "an empty sweep call must not fire any notification"
    finally:
        await listener.close()


async def test_sweep2_fires_no_notifications(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Sweep 2 is not a wake-channel writer and must stay one.

    The deadline sweep's victims are terminal ('failed') — waking
    dispatch workers for them is pure cost, and the wake channel's
    meaning is a documented contract (see sweep 1's channel-semantics
    note).  This pin makes any addition of a notify to sweep 2 a
    deliberate, test-visible decision.

    Observation hygiene: seeding the corpus inserts 'pending' jobs rows
    directly, and the schema's ``tr_notify_job_insert`` trigger
    (defense-in-depth for direct SQL inserts, see the initial migration)
    fires the SAME wake channel for them — deduplicated by PostgreSQL to
    one delivery per transaction, since duplicate (channel, payload)
    notifications inside one transaction coalesce.  The seed's delivery
    is drained and discarded before the sweep runs, so ``seen`` below
    observes only the sweep's own statements.
    """
    schema = module_pg_schema.schema_name
    channel = wake_channel(schema)
    listener, seen = await _listen(module_pg_schema.pg_dsn, channel)
    try:
        await _seed_overdue_deadline(clean_pg_conn, schema, 3)
        await asyncio.sleep(_DELIVERY_BEAT)
        seen.clear()

        count = await PostgresBackend.sweep_deadline_exceeded(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            schema=schema,
        )
        assert count == 3, "the seeded backlog must be swept"
        await asyncio.sleep(_DELIVERY_BEAT)
        assert seen == [], (
            f"the deadline sweep fired {len(seen)} notification(s) on {channel!r} "
            "— it is not a wake-channel writer today"
        )
    finally:
        await listener.close()
