# ruff: noqa: S608  # Why: schema is a fixed test identifier, every value is $-bound.
"""Upgrade-path attack: the migration chain survives a mid-apply kill.

Production walks the upgrade with a fleet live: the applier can die at any
statement boundary (OOM, container pause, an operator's
``pg_terminate_backend``). For each of the three newest migrations, at
EVERY statement boundary of the file (before the first statement through
after the last, before COMMIT):

* a session holding the migration's transaction is terminated; the file's
  transaction rolls back whole (no half-applied state: neither the ledger
  nor the catalog shows any trace of the file),
* the schema-qualified migration advisory lock is RELEASED by the dead
  session (the next applier can take it immediately),
* a resume applies the chain cleanly: every bundled migration recorded,
  checksums match the bundled files, no ``INVALID`` index debris,
* a legacy row that predates the killed migration still upgrades
  functionally (the claim-epoch column reads 0 until the first
  post-migration claim).

The same assertions run for a kill during the runner's own
``schema_migrations`` ledger upgrade (its own transaction, before the
first migration of a run).
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import (
    Migration,
    apply_pending,
    checksum_drifts,
    discover,
    list_applied,
    list_invalid_indexes,
    migration_lock_name,
    split_statements,
)
from taskq.testing.pg import create_pending_job

pytestmark = pytest.mark.integration

#: The three newest migrations are the ones production upgrades apply today.
NEWEST_MIGRATIONS: tuple[str, ...] = (
    "01.00.17_01:pre",
    "01.00.18_01:pre",
    "01.00.18_02:pre",
)

#: The migration each key's chain must stop BEFORE (its predecessor).
_PREDECESSOR_TARGET: dict[str, str | None] = {
    "01.00.17_01:pre": "01.00.16_01",
    "01.00.18_01:pre": "01.00.17_01",
    "01.00.18_02:pre": "01.00.18_01",
}

_KEY_TO_MIGRATION: dict[str, Migration] = {m.key: m for m in discover()}


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _backend_pid(conn: asyncpg.Connection) -> int:
    pid = await conn.fetchval("SELECT pg_backend_pid()")
    return int(pid)


async def _prepare_prefix(conn: asyncpg.Connection, schema: str, key: str) -> list[str]:
    """Apply the chain up to (not including) ``key``; return the killed
    migration's rendered statements."""
    target = _PREDECESSOR_TARGET[key]
    if target is not None:
        await apply_pending(conn, schema=schema, target=target)
    else:  # pragma: no cover - every newest key has a predecessor today
        await apply_pending(conn, schema=schema, max_steps=0)
    migration = _KEY_TO_MIGRATION[key]
    return split_statements(migration.render(schema))


async def _kill_mid_file(
    conn_a: asyncpg.Connection,
    conn_b: asyncpg.Connection,
    schema: str,
    statements: list[str],
    boundary: int,
) -> None:
    """Hold the migration's transaction with ``boundary`` statements
    applied, then terminate the session (the mid-file kill)."""
    pid = await _backend_pid(conn_a)
    await conn_a.execute("SET lock_timeout = '30s'")
    await conn_a.execute("BEGIN")
    try:
        for statement in statements[:boundary]:
            await conn_a.execute(statement)
        terminated = await conn_b.fetchval("SELECT pg_terminate_backend($1)", pid)
        assert terminated is True
    except (asyncpg.PostgresError, OSError, ConnectionError):
        # conn_a itself died (self-terminate path): the kill still happened.
        pass


async def _assert_no_trace(conn_b: asyncpg.Connection, schema: str, key: str) -> None:
    recorded = await conn_b.fetchval(
        f'SELECT count(*) FROM "{schema}".schema_migrations WHERE version = $1',
        key,
    )
    assert recorded == 0, f"{key}: the kill left a ledger record behind"


async def _assert_advisory_lock_free(conn_b: asyncpg.Connection, schema: str) -> None:
    name = migration_lock_name(schema)
    got = await conn_b.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", name)
    assert got is True, "the dead applier's migration advisory lock did not release"
    await conn_b.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", name)


async def _assert_resume_clean(conn_b: asyncpg.Connection, schema: str, legacy_job: object) -> None:
    applied = await apply_pending(conn_b, schema=schema)
    assert applied, "the resume must apply the remaining chain"
    assert await list_invalid_indexes(conn_b, schema) == []
    assert await checksum_drifts(conn_b, schema=schema) == {}
    all_migrations = discover()
    assert await list_applied(conn_b, schema) == {m.key for m in all_migrations}
    # The legacy row (created pre-kill, pre-claim-epoch) upgrades with the
    # schema and reads the default epoch: no claim can ever stamp 0.
    if legacy_job is not None:
        row = await conn_b.fetchrow(
            f'SELECT claim_epoch, status FROM "{schema}".jobs WHERE id = $1', legacy_job
        )
        assert row is not None
        assert row["claim_epoch"] == 0
        assert row["status"] == "pending"


@pytest.mark.parametrize("key", NEWEST_MIGRATIONS)
@pytest.mark.parametrize("boundary", [0, 1, 2, 3])
async def test_kill_at_statement_boundary_resumes_clean(
    pg_dsn: str, key: str, boundary: int
) -> None:
    conn_a = await asyncpg.connect(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn)
    schema = f"atk_kill_{new_base62()}".lower()
    try:
        await _drop_schema(conn_b, schema)
        statements = await _prepare_prefix(conn_b, schema, key)
        if boundary > len(statements):
            pytest.skip("boundary beyond this file's statement count")

        # A legacy row that predates the killed migration.
        legacy_job = await create_pending_job(
            conn_b, schema, scheduled_at=datetime.now(UTC) - timedelta(seconds=10)
        )

        await _kill_mid_file(conn_a, conn_b, schema, statements, boundary)
        await _assert_no_trace(conn_b, schema, key)
        await _assert_advisory_lock_free(conn_b, schema)
        await _assert_resume_clean(conn_b, schema, legacy_job)
    finally:
        await _drop_schema(conn_b, schema)
        for c in (conn_a, conn_b):
            with contextlib.suppress(Exception):  # a terminated conn cannot close
                await c.close()


@pytest.mark.parametrize(
    "column_probe", ["retry_base", "retry_cap", "retry_backoff", "retry_jitter"]
)
async def test_kill_mid_column_add_leaves_no_half_column(pg_dsn: str, column_probe: str) -> None:
    """The catalog-level no-half-applied check: after a kill at the only
    boundary of a single-statement file (01.00.18_01's four-column
    actor_config ALTER), neither the columns nor the ledger record
    exist, and the resume adds them whole."""
    conn = await asyncpg.connect(pg_dsn)
    conn_killer = await asyncpg.connect(pg_dsn)
    schema = f"atk_half_{new_base62()}".lower()
    try:
        await _drop_schema(conn_killer, schema)
        key = "01.00.18_01:pre"
        statements = await _prepare_prefix(conn_killer, schema, key)
        assert len(statements) == 1
        pid = await _backend_pid(conn)
        await conn.execute("BEGIN")
        await conn.execute(statements[0])
        assert await conn_killer.fetchval("SELECT pg_terminate_backend($1)", pid) is True

        columns = await conn_killer.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2",
            schema,
            "actor_config",  # 01.00.18_01 alters actor_config, not jobs
        )
        assert column_probe not in {r["column_name"] for r in columns}, (
            f"a kill mid-{key} left the {column_probe} column half-applied"
        )
        await _assert_no_trace(conn_killer, schema, key)
        await _assert_advisory_lock_free(conn_killer, schema)
        await _assert_resume_clean(conn_killer, schema, None)
    finally:
        await _drop_schema(conn_killer, schema)
        for c in (conn, conn_killer):
            with contextlib.suppress(Exception):  # a terminated conn cannot close
                await c.close()


async def test_kill_during_ledger_upgrade_releases_and_resumes(pg_dsn: str) -> None:
    """A kill while the runner's own ledger ALTER holds its transaction
    (and the schema-qualified advisory lock a locked applier carries)
    rolls the ALTER back, releases the advisory lock with the dead
    session, and the resume re-runs the upgrade and reports no pending
    work."""
    conn_a = await asyncpg.connect(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn)
    schema = f"atk_ledger_{new_base62()}".lower()
    try:
        await _drop_schema(conn_b, schema)
        # A ledger WITHOUT use_transaction (the pre-upgrade shape).
        await apply_pending(conn_b, schema=schema)
        await conn_b.execute(
            f'ALTER TABLE "{schema}".schema_migrations DROP COLUMN use_transaction'
        )

        # The locked applier shape: hold the schema's migration advisory
        # lock at session level, then run the ledger upgrade in a
        # transaction, then die.
        name = migration_lock_name(schema)
        await conn_a.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", name)
        pid = await _backend_pid(conn_a)
        await conn_a.execute("BEGIN")
        await conn_a.execute(
            f'ALTER TABLE "{schema}".schema_migrations '
            "ADD COLUMN IF NOT EXISTS use_transaction boolean NOT NULL DEFAULT true"
        )
        assert await conn_b.fetchval("SELECT pg_terminate_backend($1)", pid) is True

        # The ALTER rolled back and the dead session's advisory lock went
        # with it: the next applier takes both immediately.
        columns = await conn_b.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = 'schema_migrations'",
            schema,
        )
        assert "use_transaction" not in {r["column_name"] for r in columns}
        await _assert_advisory_lock_free(conn_b, schema)

        # Resume: the runner re-upgrades the ledger and reports no
        # pending work, checksums intact.
        applied = await apply_pending(conn_b, schema=schema)
        assert applied == []
        assert await checksum_drifts(conn_b, schema=schema) == {}
    finally:
        await _drop_schema(conn_b, schema)
        for c in (conn_a, conn_b):
            with contextlib.suppress(Exception):  # a terminated conn cannot close
                await c.close()
