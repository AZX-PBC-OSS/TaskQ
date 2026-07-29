"""Tests for taskq migrate CLI subcommand: status and up."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock

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

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()


def _make_migration(
    version: str, phase: str, filename: str, *, use_transaction: bool = True
) -> Migration:
    return Migration(
        version=version,
        phase=phase,  # type: ignore[arg-type] # Why: test fixture; Phase is Literal["pre", "post"].
        description=f"{filename} description",
        filename=filename,
        sql_template="SELECT 1;",
        use_transaction=use_transaction,
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


def test_migrate_status_marks_no_transaction_migrations(monkeypatch: Any) -> None:
    """migrate status annotates migrations that run outside a transaction so
    operators can tell online-safe migrations from blocking ones."""
    _patch_connect(monkeypatch)
    transactional = _make_migration("01.00.00_01", "pre", "01.00.00_01_pre_normal.sql")
    no_transaction = Migration(
        version="01.00.02_01",
        phase="post",
        description="concurrent index",
        filename="01.00.02_01_post_concurrent_idx.sql",
        sql_template="SELECT 1;",
        use_transaction=False,
    )

    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "discover",
        lambda: [transactional, no_transaction],
    )

    result = runner.invoke(app, ["migrate", "status"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    plain = plain_cli_output(result.output)
    assert "01.00.02_01_post_concurrent_idx.sql (no transaction)" in plain
    assert "01.00.00_01_pre_normal.sql (no transaction)" not in plain
    assert "01.00.00_01_pre_normal.sql" in plain


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


# ── migrate up failure diagnosis ────────────────────────────────────────────


def test_migrate_up_transactional_failure_reports_rollback_and_rerun(
    monkeypatch: Any,
) -> None:
    """A failed transactional migration rolls the whole file back: the CLI
    names the migration, says nothing was applied, and prescribes
    fix-and-re-run — never a traceback. The error line is truncated to its
    first line (asyncpg messages can be multiline)."""
    _patch_connect(monkeypatch)
    failing = _make_migration("01.00.00_01", "pre", "01.00.00_01_pre_failing.sql")
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "apply_pending",
        AsyncMock(side_effect=RuntimeError("deadlock detected\nProcess 123 waits for ShareLock")),
    )
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [failing])
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 1
    plain = plain_cli_output(result.output)
    assert "migration 01.00.00_01_pre_failing.sql failed: deadlock detected" in plain
    assert "Process 123 waits" not in plain, "only the exception's first line may be printed"
    assert "transaction" in plain
    assert "rolled back" in plain
    assert "nothing" in plain and "applied" in plain
    assert "taskq migrate up" in plain
    assert "Traceback" not in plain


def test_migrate_up_no_transaction_failure_lists_invalid_indexes(monkeypatch: Any) -> None:
    """A failed no-transaction migration keeps its partial effects: the CLI
    says the migration was NOT recorded, names any INVALID indexes an
    interrupted concurrent build left behind, and prescribes re-running the
    idempotent migration."""
    _patch_connect(monkeypatch)
    failing = _make_migration(
        "01.00.02_01", "post", "01.00.02_01_post_concurrent_idx.sql", use_transaction=False
    )
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "apply_pending",
        AsyncMock(side_effect=RuntimeError("canceling statement due to statement timeout")),
    )
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [failing])
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "list_invalid_indexes",
        AsyncMock(return_value=["jobs_queue_idx"]),
    )

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 1
    plain = plain_cli_output(result.output)
    assert "migration 01.00.02_01_post_concurrent_idx.sql failed:" in plain
    assert "WITHOUT a transaction" in plain
    assert "NOT recorded" in plain
    assert 'INVALID index(es) in schema "taskq": jobs_queue_idx' in plain
    assert "idempotent" in plain
    assert "taskq migrate up" in plain
    assert "Traceback" not in plain


def test_migrate_up_no_transaction_failure_without_invalid_indexes(monkeypatch: Any) -> None:
    """With no INVALID-index debris, the INVALID line is omitted while the
    re-run guidance is still printed."""
    _patch_connect(monkeypatch)
    failing = _make_migration(
        "01.00.02_01", "post", "01.00.02_01_post_concurrent_idx.sql", use_transaction=False
    )
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "apply_pending",
        AsyncMock(side_effect=RuntimeError("connection was closed mid-build")),
    )
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [failing])
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))
    monkeypatch.setattr(cli_mod.migrate_mod, "list_invalid_indexes", AsyncMock(return_value=[]))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 1
    plain = plain_cli_output(result.output)
    assert "migration 01.00.02_01_post_concurrent_idx.sql failed:" in plain
    assert "INVALID" not in plain
    assert "taskq migrate up" in plain
    assert "Traceback" not in plain


def test_migrate_up_failure_diagnosis_never_masks_original_error(monkeypatch: Any) -> None:
    """A diagnostic query failing (e.g. the connection died with the
    migration) must not replace the original error: the base message and
    re-run action are still printed, with no secondary exception and no
    traceback."""
    _patch_connect(monkeypatch)
    failing = _make_migration("01.00.00_01", "pre", "01.00.00_01_pre_failing.sql")
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "apply_pending",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [failing])
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "list_applied",
        AsyncMock(side_effect=RuntimeError("conn dead")),
    )

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 1
    plain = plain_cli_output(result.output)
    assert "boom" in plain
    assert "conn dead" not in plain
    assert "taskq migrate up" in plain
    assert "Traceback" not in plain


def test_migrate_up_failure_report_survives_diagnosis_itself_raising(
    monkeypatch: Any,
) -> None:
    """Belt-and-braces: if diagnose_apply_failure itself blows up (a bug, or
    a conn failure mode its suppressions don't cover), the CLI must still
    print the ORIGINAL error and the re-run action — a diagnostic must never
    mask the failure it diagnoses."""
    _patch_connect(monkeypatch)
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "apply_pending",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "diagnose_apply_failure",
        AsyncMock(side_effect=RuntimeError("diagnosis exploded")),
    )

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 1
    plain = plain_cli_output(result.output)
    assert "migration failed: boom" in plain
    assert "diagnosis exploded" not in plain
    assert "taskq migrate up" in plain
    assert "Traceback" not in plain


def test_migrate_up_failure_report_exact_stderr_lines(monkeypatch: Any) -> None:
    """Byte-exact pin of one full CLI failure report (content and order) —
    complements the substring pins above; the renderer-level line lists are
    pinned in tests/test_migrations_unit.py."""
    _patch_connect(monkeypatch)
    failing = _make_migration("01.00.00_01", "pre", "01.00.00_01_pre_failing.sql")
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "apply_pending",
        AsyncMock(side_effect=RuntimeError("deadlock detected")),
    )
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [failing])
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 1
    assert result.stderr.splitlines() == [
        "migration 01.00.00_01_pre_failing.sql failed: deadlock detected",
        "It ran in a transaction and rolled back: nothing from the migration was applied.",
        "Action: fix the error and re-run `taskq migrate up`.",
    ]


def test_migrate_up_connect_failure_reports_generic_and_skips_close(
    monkeypatch: Any,
) -> None:
    """asyncpg.connect itself can fail (PG down, bad DSN) BEFORE the apply:
    the CLI must still print the short report — original error plus re-run
    action, never a traceback — and must not attempt a close on a
    connection that was never acquired."""
    monkeypatch.setattr(
        cli_mod.asyncpg,
        "connect",
        AsyncMock(side_effect=OSError("connection refused")),
    )
    close_spy = AsyncMock()
    monkeypatch.setattr(cli_mod, "close_conn_bounded", close_spy)

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 1
    plain = plain_cli_output(result.output)
    assert "migration failed: connection refused" in plain
    assert "taskq migrate up" in plain
    assert "Traceback" not in plain
    close_spy.assert_not_called()


def test_migrate_up_phase_failure_report_names_tagged_migration(
    monkeypatch: Any,
) -> None:
    """Under ``migrate up --phase pre`` an earlier-version :post migration can
    still be pending and sorts FIRST in discover() order, so the naive
    first-unrecorded heuristic would name the wrong file. apply_pending tags
    the exception with the migration that actually failed; the report must
    name the tagged (later) file."""
    _patch_connect(monkeypatch)
    earlier_pending_post = _make_migration("01.00.00_01", "post", "01.00.00_01_post_pending.sql")
    failing_pre = _make_migration("01.00.02_01", "pre", "01.00.02_01_pre_failing.sql")
    exc = RuntimeError("deadlock detected")
    exc.__dict__["taskq_failed_migration"] = failing_pre
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(side_effect=exc))
    monkeypatch.setattr(
        cli_mod.migrate_mod,
        "discover",
        lambda: [earlier_pending_post, failing_pre],
    )
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))

    result = runner.invoke(app, ["migrate", "up", "--phase", "pre"])
    assert result.exit_code == 1
    plain = plain_cli_output(result.output)
    assert "migration 01.00.02_01_pre_failing.sql failed: deadlock detected" in plain
    assert "01.00.00_01_post_pending.sql" not in plain
    assert "Traceback" not in plain


async def test_migrate_up_terminates_hung_conn_close(monkeypatch: Any) -> None:
    """A hung conn close at migrate up exit (dead PG) is terminated after
    the bounded timeout and the command body completes."""
    fake_conn = _patch_connect(monkeypatch)
    fake_conn.close_wait.clear()  # close() blocks forever from now on
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(return_value=[]))
    monkeypatch.setattr(cli_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    # Why the outer timeout: pre-fix _up awaited conn.close() unbounded,
    # so the RED state would hang forever instead of failing fast. Driven via
    # the command coroutine directly (not runner.invoke) so asyncio.timeout
    # can bound the RED state.
    async with asyncio.timeout(5):
        await cli_mod._up(TaskQSettings.load(), phase=None, target=None, max_steps=None)

    assert fake_conn.terminated is True
    assert fake_conn.close_calls == 1
