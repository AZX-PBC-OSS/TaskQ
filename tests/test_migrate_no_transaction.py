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
``discover()`` (same pattern as ``test_migrate_coverage.py``) — except
``test_discover_directive_parsing_applies_end_to_end``, which patches
``importlib.resources.files`` instead so the REAL ``discover()`` directive
parsing is exercised against a real database. Each test uses its own
``new_base62()``-suffixed schema name.
"""

from __future__ import annotations

import asyncio
import contextlib
from importlib import resources
from pathlib import Path

import asyncpg
import pytest
import structlog.testing
from typer.testing import CliRunner

from taskq import migrate as migrate_mod
from taskq._ids import new_base62
from taskq.cli import app
from taskq.migrate import Migration
from taskq.testing.assertions import plain_cli_output

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


# ── Directive parsing end-to-end (REAL discover()) ─────────────────────────


async def test_discover_directive_parsing_applies_end_to_end(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Every other test here hand-sets ``use_transaction=False``, so the
    directive-parsing path itself was never exercised against a real DB. This
    test goes through the REAL ``discover()``: the synthetic file carries the
    directive WITH a trailing note (the common real-world form), and the
    migration must be applied non-transactionally based on that parse alone."""
    schema = f"mig_nt_disc_{new_base62()}".lower()
    # Resolve and copy the bundled *.sql files BEFORE patching
    # resources.files — apply_pending re-discovers on every call, so the
    # patched dir must contain the full bundled set plus the synthetic file.
    real_dir = resources.files("taskq.migrations")
    for entry in real_dir.iterdir():
        if entry.is_file() and entry.name.endswith(".sql"):
            (tmp_path / entry.name).write_bytes(entry.read_bytes())
    synth_name = "90.05.00_01_post_directive_file.sql"
    (tmp_path / synth_name).write_text(
        "-- taskq:no-transaction — CIC cannot run inside a transaction\n"
        'DROP INDEX CONCURRENTLY IF EXISTS "{schema}".nt_discovered_idx;\n'
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS nt_discovered_idx "
        'ON "{schema}".jobs (queue);\n',
        encoding="utf-8",
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        # Bootstrap via the REAL package dir; only then patch, so the
        # synthetic file is applied from the patched discovery below.
        await _bootstrap(conn, schema)
        monkeypatch.setattr(migrate_mod.resources, "files", lambda _pkg: tmp_path)

        applied = await migrate_mod.apply_pending(conn, schema=schema)

        # Parsed from the file — not hand-set. Asserted AFTER apply_pending
        # so a parsing regression surfaces as its DB-level symptom
        # (ActiveSQLTransactionError above) rather than a local assert.
        m = next(x for x in migrate_mod.discover() if x.filename == synth_name)
        assert m.use_transaction is False, "directive must be parsed from the file"
        assert [x.key for x in applied] == [m.key]
        assert await _index_validity(conn, schema, "nt_discovered_idx") is True
        ledger = await _ledger_transactions(conn, schema)
        assert ledger[m.key] is False
    finally:
        await _drop_schema(conn, schema)
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


async def test_list_invalid_indexes_reports_then_clears_staged_debris(pg_dsn: str) -> None:
    """The CLI's failure report leans on ``list_invalid_indexes``: after an
    interrupted CREATE INDEX CONCURRENTLY leaves an INVALID index behind, the
    helper must name it; once the debris is dropped, it must report nothing."""
    schema = f"mig_nt_lii_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await _bootstrap(conn, schema)

        # Same staging pattern as
        # test_interrupted_concurrent_build_remedy_drop_and_rebuild: a
        # 2M-row table + 100ms statement_timeout cancels CIC mid-build,
        # leaving an INVALID index (retry — the cancel can instead land
        # before catalog registration on a loaded runner).
        await conn.execute(
            f'CREATE TABLE "{schema}".lii_stage AS SELECT generate_series(1, 2000000) AS id'
        )
        staged = False
        for _attempt in range(3):
            await conn.execute(f'DROP INDEX IF EXISTS "{schema}".lii_stage_idx')
            await conn.execute("SET statement_timeout = '100ms'")
            try:
                with contextlib.suppress(asyncpg.QueryCanceledError):
                    await conn.execute(
                        f'CREATE INDEX CONCURRENTLY lii_stage_idx ON "{schema}".lii_stage (id)'
                    )
            finally:
                await conn.execute("RESET statement_timeout")
            validity = await _index_validity(conn, schema, "lii_stage_idx")
            if validity is False:
                staged = True
                break
        assert staged, "could not stage an INVALID index via statement_timeout"

        assert await migrate_mod.list_invalid_indexes(conn, schema) == ["lii_stage_idx"]

        await conn.execute(f'DROP INDEX "{schema}".lii_stage_idx')
        assert await migrate_mod.list_invalid_indexes(conn, schema) == []
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


# ── CLI failure report (end-to-end) ─────────────────────────────────────────


def test_migrate_up_cli_reports_failed_no_transaction_migration(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owner requirement, end-to-end: a failed no-transaction ``migrate up``
    must itself report what failed, the state it left the schema in, and the
    one action to take — never a traceback, never a manual-inspection
    runbook. The partial table existing afterwards proves the report told
    the truth."""
    schema = f"mig_nt_cli_{new_base62()}".lower()

    async def _setup() -> list[Migration]:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _drop_schema(conn, schema)
            return await _bootstrap(conn, schema)
        finally:
            await conn.close()

    real = asyncio.run(_setup())
    m = _fake_migration(
        "90.10.00_01",
        "post",
        "-- taskq:no-transaction\n"
        'CREATE TABLE "{schema}".nt_cli_persist (id int);\n'
        "THIS IS NOT VALID SQL;\n",
        use_transaction=False,
    )
    monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])
    monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)

    # CliRunner runs in-process, so the monkeypatched discover and the env
    # vars apply to the real CLI; it is synchronous, so this test drives
    # async setup/verify/teardown through asyncio.run and must itself stay
    # sync (asyncpg connections are bound to the loop that created them —
    # one asyncio.run per phase, like conftest's _pg_admin).
    result = CliRunner().invoke(app, ["migrate", "up"])

    async def _table_exists() -> bool:
        conn = await asyncpg.connect(pg_dsn)
        try:
            return bool(
                await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = 'nt_cli_persist'
                    )
                    """,
                    schema,
                )
            )
        finally:
            await conn.close()

    async def _cleanup() -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()

    try:
        assert result.exit_code == 1
        plain = plain_cli_output(result.output)
        assert m.filename in plain
        assert "WITHOUT a transaction" in plain
        assert "NOT recorded" in plain
        assert "taskq migrate up" in plain
        assert "Traceback" not in plain
        assert asyncio.run(_table_exists()) is True, (
            "the first statement must remain applied — the report said so"
        )
    finally:
        asyncio.run(_cleanup())


# ── apply_pending_locked startup failure self-diagnosis ──────────────────────


async def test_apply_pending_locked_failure_self_diagnoses(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owner requirement, startup path: a migration failing under
    ``apply_pending_locked`` (worker/UI startup) must abort with the SAME
    self-diagnosis the CLI prints — which migration failed, the partial
    state it left, the INVALID indexes it found, and the single action —
    joined into ONE greppable SystemExit line, never a raw traceback."""
    schema = f"mig_nt_se_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        real = await _bootstrap(conn, schema)

        # Stage INVALID-index debris (same statement_timeout pattern as
        # test_list_invalid_indexes_reports_then_clears_staged_debris): an
        # interrupted CIC leaves an INVALID index the diagnosis must name.
        await conn.execute(
            f'CREATE TABLE "{schema}".se_stage AS SELECT generate_series(1, 2000000) AS id'
        )
        staged = False
        for _attempt in range(3):
            await conn.execute(f'DROP INDEX IF EXISTS "{schema}".se_stage_idx')
            await conn.execute("SET statement_timeout = '100ms'")
            try:
                with contextlib.suppress(asyncpg.QueryCanceledError):
                    await conn.execute(
                        f'CREATE INDEX CONCURRENTLY se_stage_idx ON "{schema}".se_stage (id)'
                    )
            finally:
                await conn.execute("RESET statement_timeout")
            validity = await _index_validity(conn, schema, "se_stage_idx")
            if validity is False:
                staged = True
                break
        assert staged, "could not stage an INVALID index via statement_timeout"

        m = _fake_migration(
            "90.11.00_01",
            "post",
            "-- taskq:no-transaction\n"
            'CREATE TABLE "{schema}".se_persist (id int);\n'
            "THIS IS NOT VALID SQL;\n",
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m])

        async def _conn_factory() -> asyncpg.Connection:
            return await asyncpg.connect(pg_dsn)

        with pytest.raises(SystemExit) as excinfo:
            await migrate_mod.apply_pending_locked(schema=schema, conn_factory=_conn_factory)
        message = str(excinfo.value)
        assert "migration failed, aborting startup" in message
        assert m.filename in message
        assert "WITHOUT a transaction" in message
        assert "NOT recorded" in message
        assert f'INVALID index(es) in schema "{schema}": se_stage_idx' in message
        assert "restart is safe" in message
        assert "\n" not in message, "the startup report must be one greppable line"
        assert "Traceback" not in message
    finally:
        await _drop_schema(conn, schema)
        await conn.close()
