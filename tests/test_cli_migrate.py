"""Tests for taskq migrate CLI subcommand: status and up."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import asyncpg
import pytest
from typer.testing import CliRunner

import taskq.cli as cli_mod
from taskq.cli import app
from taskq.migrate import Migration
from taskq.settings import TaskQSettings
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()


class _FakeConn:
    """Stands in for the asyncpg connection returned by asyncpg.connect.

    close()/terminate() tracking with a hang gate for bounded-close tests:
    clear close_wait to make close() block forever (dead PG). Mirrors the
    _FakePool conventions in tests/test_cli_ui.py.
    """

    def __init__(self) -> None:
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.closed = False
        self.terminated = False
        self.executed: list[str] = []

    async def execute(self, sql: str, *args: object) -> str:
        """Record statements so the advisory-lock protocol can be asserted."""
        self.executed.append(sql)
        return "SELECT 1"

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()


def _make_migration(version: str, phase: str, filename: str) -> Migration:
    return Migration(
        version=version,
        phase=phase,  # type: ignore[arg-type] # Why: test fixture; Phase is Literal["pre", "post"].
        description=f"{filename} description",
        filename=filename,
        sql_template="SELECT 1;",
    )


def _patch_connect(monkeypatch: Any) -> _FakeConn:
    fake_conn = _FakeConn()
    monkeypatch.setattr(cli_mod.asyncpg, "connect", AsyncMock(return_value=fake_conn))
    return fake_conn


# ── migrate status ────────────────────────────────────────────────────────


def test_migrate_status_shows_applied_and_pending(monkeypatch: Any) -> None:
    """migrate status renders a checkmark for applied migrations and a blank for pending ones."""
    _patch_connect(monkeypatch)
    applied_migration = _make_migration("01.00.00_01", "pre", "01.00.00_01_applied.sql")
    pending_migration = _make_migration("01.00.00_02", "pre", "01.00.00_02_pending.sql")

    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "list_applied",
        AsyncMock(return_value={f"{applied_migration.version}:{applied_migration.phase}"}),
    )
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "discover",
        lambda: [applied_migration, pending_migration],
    )

    result = runner.invoke(app, ["migrate", "status"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    plain = plain_cli_output(result.output)
    assert "schema: taskq" in plain
    assert "applied: 1" in plain
    assert "01.00.00_01_applied.sql" in plain
    assert "01.00.00_02_pending.sql" in plain


def test_migrate_status_closes_connection(monkeypatch: Any) -> None:
    """migrate status closes the asyncpg connection even when the command succeeds.

    No-regression pin for the bounded close (#38 follow-up): a healthy conn
    is closed exactly once and never terminated (passes pre- and post-fix).
    """
    fake_conn = _patch_connect(monkeypatch)
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [])

    result = runner.invoke(app, ["migrate", "status"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert fake_conn.closed is True
    assert fake_conn.close_calls == 1
    assert fake_conn.terminated is False


async def test_migrate_status_terminates_hung_conn_close(monkeypatch: Any) -> None:
    """A hung conn close at migrate status exit (dead PG) is terminated after
    the bounded timeout and the command body completes."""
    fake_conn = _patch_connect(monkeypatch)
    fake_conn.close_wait.clear()  # close() blocks forever from now on
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [])
    monkeypatch.setattr(cli_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    # Why the outer timeout: pre-fix _status awaited conn.close() unbounded,
    # so the RED state would hang forever instead of failing fast. Driven via
    # the command coroutine directly (not runner.invoke) so asyncio.timeout
    # can bound the RED state.
    async with asyncio.timeout(5):
        await cli_mod._status(TaskQSettings.load())

    assert fake_conn.terminated is True
    assert fake_conn.close_calls == 1


async def test_migrate_status_hung_close_does_not_mask_body_error(monkeypatch: Any) -> None:
    """A hung conn close cannot mask an in-flight body error: list_applied's
    RuntimeError propagates while the bounded close times out, terminates
    the conn, and never raises (so the finally-block close cannot swallow
    or replace the original exception)."""
    fake_conn = _patch_connect(monkeypatch)
    fake_conn.close_wait.clear()  # close() blocks forever from now on
    monkeypatch.setattr(
        cli_mod.migrate_mod, "list_applied", AsyncMock(side_effect=RuntimeError("boom"))
    )
    monkeypatch.setattr(cli_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    # Why the outer timeout: pre-fix _status awaited conn.close() unbounded,
    # so the RED state would hang forever (masking "boom") instead of
    # failing fast.
    async with asyncio.timeout(5):
        with pytest.raises(RuntimeError, match="boom"):
            await cli_mod._status(TaskQSettings.load())

    assert fake_conn.terminated is True


def test_migrate_status_hung_close_does_not_mask_body_error_exit_code(
    monkeypatch: Any,
) -> None:
    """CLI surface of the masking test above: the body error reaches the
    Typer boundary (exit codes are only produced there) instead of wedging
    the process on the hung close."""
    fake_conn = _patch_connect(monkeypatch)
    fake_conn.close_wait.clear()  # close() blocks forever from now on
    monkeypatch.setattr(
        cli_mod.migrate_mod, "list_applied", AsyncMock(side_effect=RuntimeError("boom"))
    )
    monkeypatch.setattr(cli_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    result = runner.invoke(app, ["migrate", "status"])
    assert result.exit_code == 1
    assert fake_conn.terminated is True


# ── migrate up ─────────────────────────────────────────────────────────────


def test_migrate_up_no_pending_migrations(monkeypatch: Any) -> None:
    """migrate up prints 'no pending migrations' when apply_pending returns an empty list."""
    _patch_connect(monkeypatch)
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(return_value=[]))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "no pending migrations" in plain_cli_output(result.output)


def test_migrate_up_applies_migrations_and_lists_filenames(monkeypatch: Any) -> None:
    """migrate up prints the count and filenames of applied migrations."""
    _patch_connect(monkeypatch)
    applied = [
        _make_migration("01.00.00_01", "pre", "01.00.00_01_first.sql"),
        _make_migration("01.00.00_02", "pre", "01.00.00_02_second.sql"),
    ]
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(return_value=applied))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    plain = plain_cli_output(result.output)
    assert "applied 2 migration(s)" in plain
    assert "01.00.00_01_first.sql" in plain
    assert "01.00.00_02_second.sql" in plain


def test_migrate_up_forwards_phase_target_max_steps(monkeypatch: Any) -> None:
    """migrate up passes --phase, --target, --max-steps through as apply_pending kwargs."""
    _patch_connect(monkeypatch)
    apply_pending_mock = AsyncMock(return_value=[])
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", apply_pending_mock)

    result = runner.invoke(
        app,
        [
            "migrate",
            "up",
            "--phase",
            "pre",
            "--target",
            "01.00.00_01",
            "--max-steps",
            "3",
        ],
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    apply_pending_mock.assert_awaited_once()
    assert apply_pending_mock.await_args is not None
    _, kwargs = apply_pending_mock.await_args
    assert kwargs["schema"] == "taskq"
    assert kwargs["phase"] == "pre"
    assert kwargs["target"] == "01.00.00_01"
    assert kwargs["max_steps"] == 3


def test_migrate_up_takes_the_advisory_lock(monkeypatch: Any) -> None:
    """`taskq migrate up` must serialize on the migration advisory lock.

    The README names it as THE deploy step and calls it idempotent, but a
    container platform will start two replicas or retry a failed job. Unlocked,
    the loser of a race against a virgin schema hits the pre-initial
    migration's bare CREATE TABLE and crash-loops on DuplicateTableError.
    """
    fake_conn = _patch_connect(monkeypatch)
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(return_value=[]))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"

    joined = " | ".join(fake_conn.executed)
    assert "pg_advisory_lock" in joined, "migrate up ran unlocked"
    assert "pg_advisory_unlock" in joined, "the lock must be released"
    # The wait is bounded, then reset so long DDL is not killed midway.
    assert any(sql.startswith("SET lock_timeout =") for sql in fake_conn.executed)
    assert "SET lock_timeout = 0" in fake_conn.executed
    assert fake_conn.executed.index("SET lock_timeout = 0") < next(
        i for i, sql in enumerate(fake_conn.executed) if "pg_advisory_unlock" in sql
    )


def test_migrate_up_lock_contention_exits_with_a_named_reason(monkeypatch: Any) -> None:
    """Losing the race must abort with a precise message, not hang.

    `pg_advisory_lock` is a BLOCKING acquire: before the bound, a replica
    arriving mid-DDL waited indefinitely with no log line, blew past its
    startup probe, was killed, restarted, and blocked again.
    """
    fake_conn = _patch_connect(monkeypatch)

    async def _execute(sql: str, *args: object) -> str:
        fake_conn.executed.append(sql)
        if "pg_advisory_lock" in sql:
            raise asyncpg.LockNotAvailableError("canceling statement due to lock timeout")
        return "SELECT 1"

    monkeypatch.setattr(fake_conn, "execute", _execute)
    apply_pending_mock = AsyncMock(return_value=[])
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", apply_pending_mock)

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code != 0
    combined = plain_cli_output(result.output) + str(result.exception)
    assert "another process is applying migrations" in combined
    # It must NOT be reported as a broken migration -- that misdirects the operator.
    assert "migration failed, aborting startup" not in combined
    apply_pending_mock.assert_not_awaited()


def test_migrate_up_closes_connection(monkeypatch: Any) -> None:
    """migrate up closes the asyncpg connection even when no migrations are pending.

    No-regression pin for the bounded close (#38 follow-up): a healthy conn
    is closed exactly once and never terminated (passes pre- and post-fix).
    """
    fake_conn = _patch_connect(monkeypatch)
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(return_value=[]))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert fake_conn.closed is True
    assert fake_conn.close_calls == 1
    assert fake_conn.terminated is False


async def test_migrate_up_terminates_hung_conn_close(monkeypatch: Any) -> None:
    """A hung conn close at migrate up exit (dead PG) is terminated after
    the bounded timeout and the command body completes."""
    fake_conn = _patch_connect(monkeypatch)
    fake_conn.close_wait.clear()  # close() blocks forever from now on
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(return_value=[]))
    monkeypatch.setattr(cli_mod.migrate_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    # Why the outer timeout: pre-fix _up awaited conn.close() unbounded,
    # so the RED state would hang forever instead of failing fast. Driven via
    # the command coroutine directly (not runner.invoke) so asyncio.timeout
    # can bound the RED state.
    async with asyncio.timeout(5):
        await cli_mod._up(TaskQSettings.load(), phase=None, target=None, max_steps=None)

    assert fake_conn.terminated is True
    assert fake_conn.close_calls == 1
