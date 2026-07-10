"""Tests for taskq migrate CLI subcommand: status and up."""

from typing import Any
from unittest.mock import AsyncMock

from typer.testing import CliRunner

import taskq.cli as cli_mod
from taskq.cli import app
from taskq.migrate import Migration
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()


class _FakeConn:
    """Stands in for the asyncpg connection returned by asyncpg.connect."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


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
    """migrate status closes the asyncpg connection even when the command succeeds."""
    fake_conn = _patch_connect(monkeypatch)
    monkeypatch.setattr(cli_mod.migrate_mod, "list_applied", AsyncMock(return_value=set()))
    monkeypatch.setattr(cli_mod.migrate_mod, "discover", lambda: [])

    result = runner.invoke(app, ["migrate", "status"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert fake_conn.closed is True


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
    """migrate up closes the asyncpg connection even when no migrations are pending."""
    fake_conn = _patch_connect(monkeypatch)
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending", AsyncMock(return_value=[]))

    result = runner.invoke(app, ["migrate", "up"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert fake_conn.closed is True
