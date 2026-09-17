"""Pin the cluster-level role handling behind the row-visit counter.

Covers: install_row_visit_counter is idempotent when its role already
exists; concurrent callers serialise role creation on a transaction-scoped
advisory lock.

Why this needs a pin: the role is cluster-wide state, and the suite's
xdist workers share one Postgres cluster across per-module databases. Two
workers whose first installs interleave used to race the check-then-create
inside the single DO block, and the loser failed on pg_authid_rolname_index
with UniqueViolationError. The failure needed full-suite ordering to line
the two windows up, so a subset run could not reproduce it.

The tests here never drop the role. Dropping cluster-level state from one
test would break a concurrent worker sitting between its own SET ROLE and
RESET ROLE, which is the same class of cross-worker damage the race caused.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest

from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import ROW_VISIT_COUNTER_ROLE, install_row_visit_counter

pytestmark = pytest.mark.integration

# How long to wait for the second connection to show up as blocked on the
# advisory lock before declaring the serialisation broken. Generous: the
# connection only has to reach one statement.
_BLOCK_POLL_TRIES = 250
_BLOCK_POLL_INTERVAL_S = 0.02


async def test_install_row_visit_counter_is_idempotent_when_the_role_already_exists(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A second install into a cluster that already has the role reuses it.

    Under full-suite ordering the role is almost always created by whatever
    module installed the counter first; every later install must take the
    reuse path without erroring.
    """
    schema = module_pg_schema.schema_name
    await install_row_visit_counter(clean_pg_conn, schema)

    await install_row_visit_counter(clean_pg_conn, schema)

    row = await clean_pg_conn.fetchrow(
        "SELECT rolcanlogin FROM pg_roles WHERE rolname = $1", ROW_VISIT_COUNTER_ROLE
    )
    assert row is not None, "the counter role must exist after the second install"
    assert row["rolcanlogin"] is False, "the reused role must stay NOLOGIN"


async def test_install_row_visit_counter_serialises_role_creation_across_connections(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A concurrent install waits for the holder of the advisory lock.

    This pins the mechanism that makes creation race-free: the second
    caller must be observed waiting on the lock before the first caller
    releases it, and must finish cleanly afterwards. Without the lock two
    concurrent installs could both read the role as absent and the loser
    died on pg_authid_rolname_index.
    """
    schema = module_pg_schema.schema_name
    # The second connection installs into its own scratch schema, the way a
    # second xdist worker installs into its own module schema: only the role
    # is shared.
    scratch = f"{schema}_serialisation_pin"
    await clean_pg_conn.execute(f'CREATE SCHEMA "{scratch}"')
    await clean_pg_conn.execute(f'CREATE TABLE "{scratch}".jobs (id int)')

    other = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        other_pid = await other.fetchval("SELECT pg_backend_pid()")
        async with clean_pg_conn.transaction():
            await clean_pg_conn.execute(
                # Why: the lock key derives from this module's own role-name constant, never caller input.
                f"SELECT pg_advisory_xact_lock(hashtext('{ROW_VISIT_COUNTER_ROLE}'), 0)"
            )
            install_task: asyncio.Task[Any] = asyncio.ensure_future(
                install_row_visit_counter(other, scratch)
            )
            await _wait_until_blocked_on_the_lock(clean_pg_conn, other_pid)
        # Leaving the transaction commits it, which releases the lock; the
        # blocked install must now run to completion.
        await asyncio.wait_for(install_task, 30)

        sequence = await clean_pg_conn.fetchval(
            "SELECT count(*) FROM pg_sequences WHERE schemaname = $1", scratch
        )
        assert sequence == 1, (
            "the install that waited on the lock must have finished its own "
            "schema's setup after the lock was released"
        )
    finally:
        await other.close()
        await clean_pg_conn.execute(f'DROP SCHEMA IF EXISTS "{scratch}" CASCADE')


async def _wait_until_blocked_on_the_lock(observer: asyncpg.Connection, blocked_pid: int) -> None:
    """Poll pg_locks until blocked_pid is waiting on the counter's lock key.

    The poll cannot false-fail on a slow machine: the blocked connection
    has nothing else to do, so any delay only makes the poll run longer.
    It false-passes never: if the waiter never appears, the last poll
    raises and the test fails with the state recorded in the message.
    """
    for _ in range(_BLOCK_POLL_TRIES):
        waiting = await observer.fetchval(
            "SELECT count(*) FROM pg_locks "
            "WHERE locktype = 'advisory' AND NOT granted "
            "AND pid = $1 "
            "AND classid = hashtext($2) AND objid = 0",
            blocked_pid,
            ROW_VISIT_COUNTER_ROLE,
        )
        if waiting:
            return
        await asyncio.sleep(_BLOCK_POLL_INTERVAL_S)
    raise AssertionError(
        f"connection {blocked_pid} was never observed waiting on the "
        f"{ROW_VISIT_COUNTER_ROLE} advisory lock; role creation is not "
        "serialised and two concurrent installs can race"
    )
