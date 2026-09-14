"""Schema-qualified advisory locks: two schemas in one database never serialize.

Advisory locks live in a per-DATABASE namespace, so the unqualified names
this module's history shipped (``taskq:maintenance_leader``, ``taskq:prune``,
``taskq:archive_expiry``, ``taskq:cron``) were shared by every schema in the
database: two schemas' workers then contested one election lock, and the
perpetual loser never ran its sweeps while its dispatch (not leader-gated)
kept flowing — a fleet that reports healthy while scheduled work stops
moving. ``schema_lock_name`` qualifies each lock with its schema.

The tests here pin the property at the PG level — two schemas in one
database both win their own lock; within one schema the lock still
serializes — and pin the production sources against a reversion to the
unqualified literals.
"""

import contextlib
from collections.abc import AsyncGenerator, AsyncIterator
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from taskq.constants import schema_lock_name
from taskq.migrate import apply_pending
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import seed_actors

#: The worker sources that must carry no unqualified lock-name literal.
#: Kept as repo-relative paths resolved from this file so the pins work
#: regardless of the pytest rootdir.
_SOURCE_FILES: tuple[str, ...] = (
    "src/taskq/worker/leader.py",
    "src/taskq/worker/_leader_sweeps.py",
    "src/taskq/worker/_leader_shared.py",
)

#: Exact-string pins for the killed unqualified literals. Each pin includes
#: the literal's own quotes so only the exact reversion matches — a
#: schema-qualified name (``taskq:prune:{schema}``) can never trip one.
_UNQUALIFIED_LITERALS: tuple[str, ...] = (
    'taskq:maintenance_leader"',
    '"taskq:prune"',
    '"taskq:archive_expiry"',
)


@pytest_asyncio.fixture(scope="module")
async def module_pg_schema_b(
    module_pg_schema: ModulePgSchema,
) -> AsyncIterator[ModulePgSchema]:
    """A SECOND migrated schema in the SAME database as ``module_pg_schema``.

    Derives its name from the primary fixture's name (hex suffix swapped for
    ``_b``), so it stays inside the same identifier budget and can never
    collide with the primary. The advisory-lock statements under test never
    touch these tables — the schemas are migrated so the two-schema topology
    under test is real, not just two name strings.
    """
    schema_name = module_pg_schema.schema_name[:-2] + "_b"
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await apply_pending(conn, schema=schema_name)
        await seed_actors(conn, schema_name)
    finally:
        await conn.close()

    yield ModulePgSchema(schema_name=schema_name, pg_dsn=module_pg_schema.pg_dsn)

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
    finally:
        await conn.close()


@contextlib.asynccontextmanager
async def _two_conns(
    pg_dsn: str,
) -> AsyncGenerator[tuple[asyncpg.Connection, asyncpg.Connection], None]:
    """Two independent connections on one database.

    Both close in teardown even after a failed assertion: advisory session
    locks release on close, so a leaked holder would poison later tests in
    this module.
    """
    conn_a = await asyncpg.connect(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn)
    try:
        yield conn_a, conn_b
    finally:
        for conn in (conn_a, conn_b):
            if not conn.is_closed():
                await conn.close()


async def _try_lock(conn: asyncpg.Connection, lock_name: str) -> bool:
    """Take the session advisory lock exactly as the election code does."""
    got: bool = await conn.fetchval(
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
    )
    return got


@pytest.mark.integration
async def test_maintenance_leader_locks_independent_across_schemas(
    module_pg_schema: ModulePgSchema,
    module_pg_schema_b: ModulePgSchema,
) -> None:
    """THE cross-schema regression: two schemas in one database must BOTH
    win their own maintenance-leader election lock.

    Under the unqualified name both schemas contended one lock and the
    perpetual loser never ran its sweeps — healthy-looking fleet, stalled
    scheduled work.
    """
    lock_s1 = schema_lock_name("maintenance_leader", module_pg_schema.schema_name)
    lock_s2 = schema_lock_name("maintenance_leader", module_pg_schema_b.schema_name)
    assert lock_s1 != lock_s2, "schema_lock_name must qualify the lock with the schema"

    async with _two_conns(module_pg_schema.pg_dsn) as (conn_a, conn_b):
        got_a = await _try_lock(conn_a, lock_s1)
        got_b = await _try_lock(conn_b, lock_s2)
        assert got_a is True, "schema 1 must win its own schema-qualified election lock"
        assert got_b is True, (
            "schema 2 lost the election lock to schema 1 in the same database — "
            "the cross-schema serialization regression"
        )


@pytest.mark.integration
async def test_maintenance_leader_lock_still_serializes_within_one_schema(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Qualifying the lock must not weaken it: within ONE schema a second
    connection still loses, and the winner's unlock re-arms it."""
    lock = schema_lock_name("maintenance_leader", module_pg_schema.schema_name)

    async with _two_conns(module_pg_schema.pg_dsn) as (conn_a, conn_b):
        got_a = await _try_lock(conn_a, lock)
        got_b = await _try_lock(conn_b, lock)
        assert got_a is True
        assert got_b is False, "the schema-qualified lock must still serialize one schema"

        await conn_a.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock)
        got_b_retry = await _try_lock(conn_b, lock)
        assert got_b_retry is True, "unlock on the qualified name must re-arm the lock"


@pytest.mark.integration
async def test_cron_locks_independent_across_schemas(
    module_pg_schema: ModulePgSchema,
    module_pg_schema_b: ModulePgSchema,
) -> None:
    """The same two-schema property for the cron lock name: one schema's
    cron tick lock must not mute another schema's cron."""
    lock_s1 = schema_lock_name("cron", module_pg_schema.schema_name)
    lock_s2 = schema_lock_name("cron", module_pg_schema_b.schema_name)
    assert lock_s1 != lock_s2

    async with _two_conns(module_pg_schema.pg_dsn) as (conn_a, conn_b):
        got_a = await _try_lock(conn_a, lock_s1)
        got_b = await _try_lock(conn_b, lock_s2)
        assert got_a is True
        assert got_b is True, (
            "schema 2 lost the cron lock to schema 1 in the same database — "
            "the cross-schema serialization regression"
        )


def test_no_unqualified_lock_name_literals_remain() -> None:
    """Source pin: the unqualified lock-name literals must not come back.

    Exact-string pins (each literal including its quotes) so any reversion
    to the unqualified constants fails this test instead of silently
    reintroducing cross-schema serialization. Runs in the unit tier too —
    it reads files, no PG needed.
    """
    repo_root = Path(__file__).resolve().parents[1]
    for rel in _SOURCE_FILES:
        text = (repo_root / rel).read_text(encoding="utf-8")
        for pin in _UNQUALIFIED_LITERALS:
            assert pin not in text, (
                f"{rel} reintroduces the unqualified lock literal {pin}; "
                "advisory locks must be schema-qualified via schema_lock_name"
            )
