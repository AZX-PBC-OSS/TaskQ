"""Unit tests for the NOTIFY trigger SQL migration.

Verifies the trigger SQL template contains the expected DDL statements
and that it renders correctly with schema name substitution.
"""

import asyncio
from importlib import resources

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema


def _load_migration_sql(filename: str) -> str:
    package = resources.files("taskq.migrations")
    path = package / filename
    return path.read_text()


_MIGRATION_FILE = "01.00.00_01_pre_initial.sql"


def test_trigger_migration_file_exists() -> None:
    """The migration file for the NOTIFY trigger exists and is readable."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert len(sql) > 0


# The eight DDL substring assertions that used to follow — CREATE FUNCTION,
# CREATE TRIGGER, pg_notify, the WHEN clause, TG_TABLE_SCHEMA, AFTER INSERT ON
# jobs, FOR EACH ROW — are all subsumed by the two integration tests below.
# Every one of them was a restatement of the migration text: they passed on a
# trigger that was syntactically present and semantically broken, and they
# would fail on a correct trigger written with different whitespace. Applied
# against real Postgres, the trigger either fires on a pending INSERT or it
# does not.


@pytest.mark.integration
async def test_a_pending_insert_wakes_listeners_on_the_schema_channel(
    module_pg_schema: ModulePgSchema,
) -> None:
    """A direct SQL INSERT of a pending job notifies the schema's wake channel.

    This is the trigger's whole reason to exist: the application-side
    pg_notify in the enqueue path is the primary wake, and this is
    defence-in-depth for rows inserted by SQL that never goes through TaskQ.
    Asserting it end to end covers every property the DDL greps restated —
    the function exists, the trigger is AFTER INSERT ON jobs FOR EACH ROW, and
    the channel is built from TG_TABLE_SCHEMA — and, unlike them, it fails if
    the trigger is present but wrong.
    """
    schema = module_pg_schema.schema_name
    woken = asyncio.Event()
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.add_listener(
            f"taskq_wake_{schema}",
            lambda _c, _pid, _ch, _payload: woken.set(),
        )
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is fixture-derived and migration-validated.
            "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
            "VALUES ($1, 'a', 'default', '{}'::jsonb, 'pending', 3, 'transient', clock_timestamp())",
            new_uuid(),
        )
        async with asyncio.timeout(5.0):
            await woken.wait()
    finally:
        await conn.close()


@pytest.mark.integration
async def test_a_non_pending_insert_does_not_wake_listeners(
    module_pg_schema: ModulePgSchema,
) -> None:
    """The WHEN clause is load-bearing: a scheduled row is not dispatchable
    yet, and waking every worker in the fleet for one is the thundering-herd
    the filter exists to prevent."""
    schema = module_pg_schema.schema_name
    woken = asyncio.Event()
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.add_listener(
            f"taskq_wake_{schema}",
            lambda _c, _pid, _ch, _payload: woken.set(),
        )
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is fixture-derived and migration-validated.
            "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
            "VALUES ($1, 'a', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
            "clock_timestamp() + interval '1 hour')",
            new_uuid(),
        )
        # Long enough that a wrongly-fired NOTIFY would have arrived: the
        # positive test above delivers in milliseconds.
        await asyncio.sleep(0.5)
    finally:
        await conn.close()

    assert not woken.is_set(), "only pending inserts may wake the fleet"
