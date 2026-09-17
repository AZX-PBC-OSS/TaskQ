"""A transactional migration's DDL waits a bounded time for table locks.

``ALTER TABLE jobs`` takes ACCESS EXCLUSIVE, which queues behind any
session holding so much as ACCESS SHARE on ``jobs`` — an actor's open
transaction connection mid-job, an admin snapshot, ``pg_dump``. Postgres'
lock queue is FIFO, so once the DDL is queued every later ``jobs`` statement
(dispatch, enqueue, heartbeat) queues behind IT. With ``lock_timeout = 0``
the migration waited indefinitely; heartbeats (``command_timeout`` 2 s,
three failures) killed every worker in about 30 s, in-flight jobs were later
reclaimed as crashed, and the migration was still waiting.

The migration transaction now sets ``SET LOCAL lock_timeout`` to a bounded
value (:data:`taskq.migrate.DEFAULT_MIGRATION_DDL_LOCK_TIMEOUT`,
overridable per call), and a lock wait that outlives it fails the migration
with a typed error naming the bound and the fix. Migrations marked
``-- taskq:no-transaction`` keep the session's unbounded wait: their
``CONCURRENTLY`` phases wait on heavyweight locks by design.
"""

import asyncio
import time
from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62
from taskq.migrate import Migration


class _FakeTx:
    def __init__(self, executed: list[str]) -> None:
        self._executed = executed

    async def __aenter__(self) -> None:
        self._executed.append("BEGIN")

    async def __aexit__(self, *args: object) -> None:
        self._executed.append("COMMIT")


class _RecordingConn:
    """apply_pending stand-in that records every statement in order, with
    ``BEGIN``/``COMMIT`` markers where a transaction opens and closes."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    async def fetchval(self, sql: str, *args: object) -> bool:
        return True

    async def fetch(self, sql: str, *args: object) -> list[dict[str, str]]:
        return []

    def transaction(self) -> _FakeTx:
        return _FakeTx(self.executed)

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append(sql)
        return "OK"

    def bounded_transactions(self) -> list[list[str]]:
        """Every transaction's statements, each opened with a lock bound."""
        blocks: list[list[str]] = []
        current: list[str] | None = None
        for sql in self.executed:
            if sql == "BEGIN":
                current = []
            elif sql == "COMMIT":
                assert current is not None
                blocks.append(current)
                current = None
            elif current is not None:
                current.append(sql)
        assert current is None, "a transaction was left open"
        return [b for b in blocks if b and b[0].startswith("SET LOCAL lock_timeout")]

    def statements_outside_any_transaction(self) -> list[str]:
        outside: list[str] = []
        depth = 0
        for sql in self.executed:
            if sql == "BEGIN":
                depth += 1
            elif sql == "COMMIT":
                depth -= 1
            elif depth == 0:
                outside.append(sql)
        return outside


def _migration(sql: str, *, use_transaction: bool) -> Migration:
    return Migration(
        version="09.00.00",
        phase="pre",
        description="fabricated",
        filename="09.00.00_01_pre_fabricated.sql",
        sql_template=sql,
        use_transaction=use_transaction,
    )


async def test_transactional_migration_bounds_its_lock_wait(monkeypatch: Any) -> None:
    """The bound is set inside the migration's own transaction, before its
    DDL, and is transaction-local so it cannot outlive the migration."""
    migration = _migration("ALTER TABLE t ADD COLUMN zz int;", use_transaction=True)
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()

    await migrate_mod.apply_pending(conn, schema="taskq", ddl_lock_timeout=12.5)  # type: ignore[arg-type]

    assert "SET LOCAL lock_timeout = 12500" in conn.executed
    assert conn.executed.index("SET LOCAL lock_timeout = 12500") < conn.executed.index(
        "ALTER TABLE t ADD COLUMN zz int;"
    )


async def test_default_bound_is_thirty_seconds(monkeypatch: Any) -> None:
    migration = _migration("SELECT 1;", use_transaction=True)
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()

    await migrate_mod.apply_pending(conn, schema="taskq")  # type: ignore[arg-type]

    assert migrate_mod.DEFAULT_MIGRATION_DDL_LOCK_TIMEOUT == 30.0
    assert "SET LOCAL lock_timeout = 30000" in conn.executed


async def test_sub_millisecond_bound_is_refused(monkeypatch: Any) -> None:
    """``lock_timeout`` is an integer number of milliseconds; a bound below
    one millisecond would render as ``0``, which Postgres reads as "wait
    indefinitely" — the inverse of what the caller asked for, silently."""
    migration = _migration("SELECT 1;", use_transaction=True)
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()

    with pytest.raises(ValueError, match="at least one millisecond"):
        await migrate_mod.apply_pending(conn, schema="taskq", ddl_lock_timeout=0.0005)  # type: ignore[arg-type]

    assert conn.executed == [], "a refused bound must run nothing"


@pytest.mark.parametrize(
    ("ddl_lock_timeout", "rendered"),
    [(0.001, 1), (0.0019, 1), (1.5, 1500)],
    ids=["one-ms", "just-under-two-ms", "one-and-a-half-s"],
)
async def test_bounds_of_a_millisecond_or_more_render_a_nonzero_wait(
    monkeypatch: Any, ddl_lock_timeout: float, rendered: int
) -> None:
    migration = _migration("SELECT 1;", use_transaction=True)
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()

    await migrate_mod.apply_pending(conn, schema="taskq", ddl_lock_timeout=ddl_lock_timeout)  # type: ignore[arg-type]

    assert f"SET LOCAL lock_timeout = {rendered}" in conn.executed


async def test_zero_means_wait_indefinitely_and_sets_no_bound(monkeypatch: Any) -> None:
    migration = _migration("SELECT 1;", use_transaction=True)
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()

    await migrate_mod.apply_pending(conn, schema="taskq", ddl_lock_timeout=0)  # type: ignore[arg-type]

    assert not any("lock_timeout" in sql for sql in conn.executed)


_LEDGER_UPGRADE = "ADD COLUMN IF NOT EXISTS use_transaction"


async def test_existing_ledger_upgrade_runs_under_the_same_bound(monkeypatch: Any) -> None:
    """Before the first migration of a run, the runner adds the ledger's
    ``use_transaction`` column with ``ALTER TABLE schema_migrations`` —
    ACCESS EXCLUSIVE, queued behind any reader of the ledger (a worker's
    boot-time currency check, ``pg_dump``). That wait is bounded exactly
    like a migration's DDL: inside its own transaction, behind ``SET LOCAL
    lock_timeout``, never as a bare statement on the session."""
    migration = _migration("SELECT 1;", use_transaction=True)
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()

    await migrate_mod.apply_pending(conn, schema="taskq", ddl_lock_timeout=2.0)  # type: ignore[arg-type]

    assert not any(_LEDGER_UPGRADE in sql for sql in conn.statements_outside_any_transaction())
    assert any(
        any(_LEDGER_UPGRADE in sql for sql in block) and block[0] == "SET LOCAL lock_timeout = 2000"
        for block in conn.bounded_transactions()
    ), conn.executed


async def test_fresh_ledger_upgrade_after_a_no_transaction_migration_is_bounded(
    monkeypatch: Any,
) -> None:
    """A fresh install has no ledger until the initial migration creates
    one; when the first recorded migration is a ``-- taskq:no-transaction``
    file, the ledger upgrade runs after its statements, outside any
    migration transaction — and still under the bound."""
    migration = _migration(
        "-- taskq:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (c);",
        use_transaction=False,
    )
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()
    conn.fetchval = _no_ledger_yet  # type: ignore[method-assign]  # Why: the ledger-exists probe answers False for a fresh install.

    await migrate_mod.apply_pending(conn, schema="taskq", ddl_lock_timeout=2.0)  # type: ignore[arg-type]

    outside = conn.statements_outside_any_transaction()
    assert any("CREATE INDEX CONCURRENTLY" in sql for sql in outside)
    assert not any(_LEDGER_UPGRADE in sql for sql in outside)
    assert any(
        any(_LEDGER_UPGRADE in sql for sql in block) and block[0] == "SET LOCAL lock_timeout = 2000"
        for block in conn.bounded_transactions()
    ), conn.executed


async def _no_ledger_yet(sql: str, *args: object) -> bool:
    return False


async def test_no_transaction_migration_keeps_the_unbounded_wait(monkeypatch: Any) -> None:
    """``-- taskq:no-transaction`` files run statement by statement with no
    transaction to scope a bound to; their CONCURRENTLY waits are meant to
    be long."""
    migration = _migration(
        "-- taskq:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (c);",
        use_transaction=False,
    )
    monkeypatch.setattr(migrate_mod, "discover", lambda: [migration])
    conn = _RecordingConn()

    await migrate_mod.apply_pending(conn, schema="taskq")  # type: ignore[arg-type]

    outside = conn.statements_outside_any_transaction()
    assert any("CREATE INDEX CONCURRENTLY" in sql for sql in outside)
    assert not any("lock_timeout" in sql for sql in outside), (
        "a session-level lock_timeout would bound the CONCURRENTLY wait"
    )
    assert not any(
        any("CONCURRENTLY" in sql for sql in block) for block in conn.bounded_transactions()
    )


# ── Against Postgres: a held lock fails the migration within the bound ────


@pytest.mark.integration
async def test_held_table_lock_fails_the_migration_within_the_bound(
    pg_dsn: str, monkeypatch: Any
) -> None:
    schema = f"tddl_{new_base62()}".lower()
    setup = await asyncpg.connect(pg_dsn)
    try:
        await migrate_mod.apply_pending(setup, schema=schema)
    finally:
        await setup.close()

    synthetic = _migration(
        'ALTER TABLE "{schema}".jobs ADD COLUMN zz int;',
        use_transaction=True,
    )
    monkeypatch.setattr(migrate_mod, "discover", lambda: [synthetic])
    holder = await asyncpg.connect(pg_dsn)
    migrator = await asyncpg.connect(pg_dsn)
    reader = await asyncpg.connect(pg_dsn)
    try:
        # ACCESS SHARE on jobs, held open: the shape of an actor's open
        # transaction connection mid-job or an admin snapshot.
        holder_tx = holder.transaction()
        await holder_tx.start()
        await holder.execute(f'SELECT 1 FROM "{schema}".jobs LIMIT 1')  # noqa: S608  # Why: schema is a test-generated identifier.

        started = time.monotonic()
        with pytest.raises(migrate_mod.MigrationLockTimeoutError) as excinfo:
            async with asyncio.timeout(20):
                await migrate_mod.apply_pending(migrator, schema=schema, ddl_lock_timeout=1.0)
        elapsed = time.monotonic() - started
        assert elapsed < 10, f"the migration did not fail within the bound: {elapsed:.1f}s"

        message = str(excinfo.value)
        assert synthetic.filename in message
        assert "1.0s" in message
        assert "ddl_lock_timeout" in message
        assert isinstance(excinfo.value.__cause__, asyncpg.LockNotAvailableError)
        assert getattr(excinfo.value, "taskq_failed_migration", None) is synthetic

        # Nothing recorded, nothing applied, and the queue behind the DDL
        # drained: a jobs read completes now instead of parking behind it.
        recorded = await migrator.fetchval(
            f'SELECT count(*) FROM "{schema}".schema_migrations WHERE version = $1',  # noqa: S608  # Why: schema is a test-generated identifier.
            synthetic.key,
        )
        assert recorded == 0
        async with asyncio.timeout(5):
            await reader.fetchval(f'SELECT count(*) FROM "{schema}".jobs')  # noqa: S608  # Why: schema is a test-generated identifier.
        await holder_tx.rollback()
    finally:
        await holder.close()
        await migrator.close()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
            await reader.close()


@pytest.mark.integration
async def test_held_ledger_lock_fails_the_run_within_the_bound(
    pg_dsn: str, monkeypatch: Any
) -> None:
    """The runner's own ledger upgrade (``ALTER TABLE schema_migrations``)
    queues behind any reader holding the ledger open — the shape of
    ``pg_dump`` or an admin session mid-transaction. It is bounded like a
    migration's DDL, fails with the same typed error naming the ledger,
    and applies nothing."""
    schema = f"tddl_ledger_{new_base62()}".lower()
    setup = await asyncpg.connect(pg_dsn)
    try:
        await migrate_mod.apply_pending(setup, schema=schema)
    finally:
        await setup.close()

    synthetic = _migration('CREATE TABLE "{schema}".tddl_never (id int);', use_transaction=True)
    monkeypatch.setattr(migrate_mod, "discover", lambda: [synthetic])
    holder = await asyncpg.connect(pg_dsn)
    migrator = await asyncpg.connect(pg_dsn)
    try:
        holder_tx = holder.transaction()
        await holder_tx.start()
        await holder.execute(f'SELECT 1 FROM "{schema}".schema_migrations LIMIT 1')  # noqa: S608  # Why: schema is a test-generated identifier.

        started = time.monotonic()
        with pytest.raises(migrate_mod.MigrationLockTimeoutError) as excinfo:
            async with asyncio.timeout(20):
                await migrate_mod.apply_pending(migrator, schema=schema, ddl_lock_timeout=1.0)
        elapsed = time.monotonic() - started
        assert elapsed < 10, f"the ledger upgrade did not fail within the bound: {elapsed:.1f}s"

        assert excinfo.value.migration is None
        assert "schema_migrations ledger" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, asyncpg.LockNotAvailableError)
        await holder_tx.rollback()

        # The migrator's session is reusable and nothing was applied.
        recorded = await migrator.fetchval(
            f'SELECT count(*) FROM "{schema}".schema_migrations WHERE version = $1',  # noqa: S608  # Why: schema is a test-generated identifier.
            synthetic.key,
        )
        assert recorded == 0
        assert not await migrator.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", f'"{schema}".tddl_never'
        )
    finally:
        await holder.close()
        await migrator.close()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
