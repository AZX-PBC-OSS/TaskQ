"""Integration tests (real Postgres) for non-transactional migrations.

A migration carrying the ``-- taskq:no-transaction`` header directive is
discovered with ``use_transaction=False`` and applied WITHOUT the
per-migration transaction wrapper, making ``CREATE INDEX CONCURRENTLY`` /
``DROP INDEX CONCURRENTLY`` expressible. These tests pin the contract:

- a non-transactional migration actually runs outside a transaction (a
  concurrent index build succeeds — impossible inside a transaction block);
- the default path still wraps in a transaction (CONCURRENTLY is rejected,
  and a failing mid-file statement rolls the whole file back);
- the ledger records a non-transactional migration only AFTER its statements
  succeed — a failure leaves the key unrecorded while partial effects
  persist, so such migrations must be idempotent and re-runnable;
- re-running after a failure is safe;
- the interrupted-``CREATE INDEX CONCURRENTLY`` failure mode (an INVALID
  index left behind) is remedied by the documented drop-and-rebuild pattern;
- the ledger surfaces the distinction via ``schema_migrations.use_transaction``,
  self-healed onto pre-upgrade ledgers.

Synthetic migrations are layered on top of the bundled set by monkeypatching
``discover()`` (same pattern as ``test_migrate_coverage.py``). Each test uses
its own ``new_base62()``-suffixed schema name.
"""

from __future__ import annotations

import contextlib

import asyncpg
import pytest
import structlog.testing

from taskq import migrate as migrate_mod
from taskq._ids import new_base62
from taskq.migrate import Migration

pytestmark = pytest.mark.integration


def _fake_migration(
    version: str, phase: str, sql: str, *, use_transaction: bool = True
) -> Migration:
    return Migration(
        version=version,
        phase=phase,  # type: ignore[arg-type] # Why: test fixture; Phase is Literal["pre", "post"].
        description="synthetic",
        filename=f"{version}_{phase}_synthetic.sql",
        sql_template=sql,
        use_transaction=use_transaction,
    )


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _bootstrap(conn: asyncpg.Connection, schema: str) -> list[Migration]:
    """Apply the bundled migrations (schema + ledger now exist) and return
    the real discovery list so tests can layer synthetics on top of it."""
    applied = await migrate_mod.apply_pending(conn, schema=schema)
    assert applied, "expected bundled migrations to apply"
    return migrate_mod.discover()


async def _index_validity(conn: asyncpg.Connection, schema: str, index: str) -> bool | None:
    """``pg_index.indisvalid`` for ``index`` in ``schema``; None if absent."""
    return await conn.fetchval(
        """
        SELECT i.indisvalid
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
        """,
        schema,
        index,
    )


async def _ledger_transactions(conn: asyncpg.Connection, schema: str) -> dict[str, bool]:
    rows = await conn.fetch(
        f'SELECT version, use_transaction FROM "{schema}".schema_migrations'  # noqa: S608 # Why: schema is a test-generated identifier, not user input; asyncpg has no parameter binding for identifiers.
    )
    return {r["version"]: r["use_transaction"] for r in rows}


# ── Non-transactional path: CONCURRENTLY works ──────────────────────────────


async def test_no_transaction_migration_runs_create_index_concurrently(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = f"mig_nt_cc_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)
        m = _fake_migration(
            "90.01.00_01",
            "post",
            "-- taskq:no-transaction\n"
            'DROP INDEX CONCURRENTLY IF EXISTS "{schema}".nt_jobs_queue_idx;\n'
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS nt_jobs_queue_idx "
            'ON "{schema}".jobs (queue);\n',
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])

        with structlog.testing.capture_logs() as captured:
            applied = await migrate_mod.apply_pending(conn, schema=schema)

        assert [x.key for x in applied] == [m.key]
        assert any(e.get("event") == "migration-no-transaction" for e in captured), (
            "applying a migration outside a transaction must be logged"
        )
        # A concurrent build cannot run inside a transaction block — success
        # here proves the migration ran outside one, and left a VALID index.
        assert await _index_validity(conn, schema, "nt_jobs_queue_idx") is True
        ledger = await _ledger_transactions(conn, schema)
        assert ledger[m.key] is False
        # Bundled migrations applied through the default path record True.
        assert all(ledger[x.key] is True for x in real)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_apply_pending_locked_applies_no_transaction_migration(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The startup path (--migrate / TASKQ_MIGRATE_ON_START) holds a session
    advisory lock — not a transaction — so CONCURRENTLY still works."""
    schema = f"mig_nt_lock_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)
        m = _fake_migration(
            "90.02.00_01",
            "post",
            "-- taskq:no-transaction\n"
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS nt_locked_idx "
            'ON "{schema}".jobs (status);\n',
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])
    finally:
        await conn.close()

    async def _factory() -> asyncpg.Connection:
        return await asyncpg.connect(pg_dsn)

    try:
        applied = await migrate_mod.apply_pending_locked(schema=schema, conn_factory=_factory)
        assert [x.key for x in applied] == [m.key]

        conn = await asyncpg.connect(pg_dsn)
        try:
            assert await _index_validity(conn, schema, "nt_locked_idx") is True
            ledger = await _ledger_transactions(conn, schema)
            assert ledger[m.key] is False
        finally:
            await conn.close()
    finally:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()


# ── Default path: still transactional ───────────────────────────────────────


async def test_transactional_migration_rejects_concurrently(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A migration WITHOUT the directive still runs inside a transaction, so
    Postgres rejects CREATE INDEX CONCURRENTLY — pinning that the default
    wrapper is intact and that the directive is what unlocks it."""
    schema = f"mig_nt_txcc_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)
        m = _fake_migration(
            "90.03.00_01",
            "post",
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS nt_blocked_idx ON "{schema}".jobs (queue);\n',
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])

        with pytest.raises(asyncpg.ActiveSQLTransactionError):
            await migrate_mod.apply_pending(conn, schema=schema)

        assert await _index_validity(conn, schema, "nt_blocked_idx") is None
        assert m.key not in await migrate_mod.list_applied(conn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_transactional_migration_rolls_back_on_failure(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression guard for the default path: a failure mid-file rolls back
    the whole migration — the table from the first statement must not persist."""
    schema = f"mig_nt_txrb_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)
        m = _fake_migration(
            "90.04.00_01",
            "post",
            'CREATE TABLE "{schema}".nt_rolled_back (id int);\nTHIS IS NOT VALID SQL;\n',
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])

        with pytest.raises(asyncpg.PostgresSyntaxError):
            await migrate_mod.apply_pending(conn, schema=schema)

        table_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = 'nt_rolled_back'
            )
            """,
            schema,
        )
        assert table_exists is False, "transactional migration must roll back fully"
        assert m.key not in await migrate_mod.list_applied(conn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Non-transactional failure modes ─────────────────────────────────────────


async def test_failed_no_transaction_migration_is_not_recorded_but_effects_persist(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE distinguishing failure mode: statements before the failure commit
    independently (no rollback), yet the ledger records nothing — completion
    is recorded only after every statement succeeds."""
    schema = f"mig_nt_fail_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)
        m = _fake_migration(
            "90.05.00_01",
            "post",
            "-- taskq:no-transaction\n"
            'CREATE TABLE "{schema}".nt_partial (id int);\n'
            "SELECT nonexistent_function_xyz();\n",
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])

        with pytest.raises(asyncpg.UndefinedFunctionError):
            await migrate_mod.apply_pending(conn, schema=schema)

        table_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = 'nt_partial'
            )
            """,
            schema,
        )
        assert table_exists is True, (
            "no transaction wrapper: the first statement must NOT roll back"
        )
        assert m.key not in await migrate_mod.list_applied(conn, schema), (
            "failed migration must not be marked applied"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_rerun_of_failed_no_transaction_migration_is_safe(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a failed non-transactional apply, re-running an idempotent
    version of the same migration key succeeds and is recorded — without
    duplicating effects from the first, partial run."""
    schema = f"mig_nt_rerun_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)
        broken = _fake_migration(
            "90.06.00_01",
            "post",
            "-- taskq:no-transaction\n"
            'CREATE TABLE IF NOT EXISTS "{schema}".nt_rerun (id int);\n'
            'INSERT INTO "{schema}".nt_rerun VALUES (1);\n'
            "SELECT nonexistent_function_xyz();\n",
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, broken])
        with pytest.raises(asyncpg.UndefinedFunctionError):
            await migrate_mod.apply_pending(conn, schema=schema)

        fixed = _fake_migration(
            "90.06.00_01",
            "post",
            "-- taskq:no-transaction\n"
            'CREATE TABLE IF NOT EXISTS "{schema}".nt_rerun (id int);\n'
            'INSERT INTO "{schema}".nt_rerun SELECT 1 '
            'WHERE NOT EXISTS (SELECT 1 FROM "{schema}".nt_rerun);\n',
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, fixed])
        applied = await migrate_mod.apply_pending(conn, schema=schema)

        assert [m.key for m in applied] == [fixed.key]
        assert fixed.key in await migrate_mod.list_applied(conn, schema)
        rows = await conn.fetch(f'SELECT id FROM "{schema}".nt_rerun')  # noqa: S608 # Why: schema is a test-generated identifier, not user input.
        assert [r["id"] for r in rows] == [1], (
            "first run's INSERT persisted; the idempotent re-run must not duplicate it"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_interrupted_concurrent_build_remedy_drop_and_rebuild(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interrupted CREATE INDEX CONCURRENTLY leaves an INVALID index. The
    documented remedy — DROP INDEX CONCURRENTLY IF EXISTS then CREATE INDEX
    CONCURRENTLY IF NOT EXISTS in one non-transactional migration — replaces
    it with a valid index and is then recorded."""
    schema = f"mig_nt_inv_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)

        # Stage the debris of an interrupted build: a 2M-row table makes the
        # concurrent build slow enough that a 100ms statement_timeout cancels
        # it mid-build, leaving an INVALID index behind. On a heavily loaded
        # runner the cancel can instead land during CIC's catalog-registration
        # phase (no index row survives), so retry the staging a few times —
        # each attempt is independent and well under a second.
        await conn.execute(
            f'CREATE TABLE "{schema}".nt_stage AS SELECT generate_series(1, 2000000) AS id'
        )
        staged = False
        for _attempt in range(3):
            await conn.execute(f'DROP INDEX IF EXISTS "{schema}".nt_stage_idx')
            await conn.execute("SET statement_timeout = '100ms'")
            try:
                with contextlib.suppress(asyncpg.QueryCanceledError):
                    await conn.execute(
                        f'CREATE INDEX CONCURRENTLY nt_stage_idx ON "{schema}".nt_stage (id)'
                    )
            finally:
                await conn.execute("RESET statement_timeout")
            validity = await _index_validity(conn, schema, "nt_stage_idx")
            if validity is False:
                staged = True
                break
            # None: cancelled before catalog registration (slow CI). True:
            # build finished inside the timeout (absurdly fast machine).
        assert staged, "could not stage an INVALID index via statement_timeout"

        m = _fake_migration(
            "90.07.00_01",
            "post",
            "-- taskq:no-transaction\n"
            'DROP INDEX CONCURRENTLY IF EXISTS "{schema}".nt_stage_idx;\n'
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS nt_stage_idx "
            'ON "{schema}".nt_stage (id);\n',
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])
        applied = await migrate_mod.apply_pending(conn, schema=schema)

        assert [x.key for x in applied] == [m.key]
        assert await _index_validity(conn, schema, "nt_stage_idx") is True, (
            "the remedy migration must replace the INVALID index with a valid one"
        )
        ledger = await _ledger_transactions(conn, schema)
        assert ledger[m.key] is False
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_no_transaction_migration_rejects_transaction_control_statements(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BEGIN/COMMIT inside a no-transaction file would re-open an explicit
    transaction on the caller's connection — defeating CONCURRENTLY and, on
    failure, leaving the connection in an aborted transaction. The runner
    rejects the file BEFORE executing anything."""
    schema = f"mig_nt_txctl_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)
        m = _fake_migration(
            "90.09.00_01",
            "post",
            "-- taskq:no-transaction\n"
            "BEGIN;\n"
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS nt_txctl_idx ON "{schema}".jobs (queue);\n'
            "COMMIT;\n",
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])

        with pytest.raises(ValueError, match="transaction-control"):
            await migrate_mod.apply_pending(conn, schema=schema)

        assert await _index_validity(conn, schema, "nt_txctl_idx") is None, (
            "nothing must execute when the guard rejects the file"
        )
        assert m.key not in await migrate_mod.list_applied(conn, schema)
        # The caller's connection is untouched (no open/aborted transaction):
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Ledger surfacing / upgrade path ─────────────────────────────────────────


async def test_runner_self_heals_ledger_column_and_backfills_default(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pre-upgrade deployments have a schema_migrations ledger without the
    use_transaction column. The runner adds it when recording the next
    migration; pre-existing rows backfill to true (they all ran inside a
    transaction), and new rows record how they actually ran."""
    schema = f"mig_nt_heal_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(
            f"""
            CREATE TABLE "{schema}".schema_migrations (
                version       text PRIMARY KEY,
                applied_at    timestamptz NOT NULL DEFAULT now(),
                checksum      text NOT NULL
            )
            """
        )
        await conn.execute(
            f'INSERT INTO "{schema}".schema_migrations (version, checksum) VALUES ($1, $2)',  # noqa: S608
            "00.00.00_01:pre",
            "0" * 64,
        )

        tx = _fake_migration(
            "90.08.00_01", "post", 'CREATE TABLE "{schema}".nt_heal_tx (id int);\n'
        )
        nt = _fake_migration(
            "90.08.00_02",
            "post",
            '-- taskq:no-transaction\nCREATE TABLE "{schema}".nt_heal_nt (id int);\n',
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [tx, nt])
        applied = await migrate_mod.apply_pending(conn, schema=schema)

        assert [m.key for m in applied] == [tx.key, nt.key]
        ledger = await _ledger_transactions(conn, schema)
        assert ledger == {
            "00.00.00_01:pre": True,
            tx.key: True,
            nt.key: False,
        }
    finally:
        await _drop_schema(conn, schema)
        await conn.close()
