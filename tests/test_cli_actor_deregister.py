"""Tests for `taskq actor-config deregister` CLI command.

Monkeypatches the ops function and asyncpg.connect to pin the CLI's
argument parsing, error handling, and output shape without requiring
real Postgres (integration coverage is in test_actor_deregistration.py).
"""

from typing import Any

import pytest
from typer.testing import CliRunner

from taskq.actor_config_ops import DeregisterResult
from taskq.cli import app

runner = CliRunner()


def _patch_deregister(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: DeregisterResult | None = None,
    raises: Exception | None = None,
) -> dict[str, Any]:
    """Fake asyncpg.connect + deregister_actor; return captured call kwargs."""
    captured: dict[str, Any] = {}

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    async def fake_deregister(conn: Any, actor: str, **kwargs: Any) -> Any:
        captured["actor"] = actor
        captured["kwargs"] = kwargs
        if raises is not None:
            raise raises
        return result or DeregisterResult(
            actor=actor,
            queue="default",
            actor_config_deleted=True,
            schedules_disabled=0,
            jobs_cancelled=0,
            terminal_jobs_remaining=0,
            queue_purged=False,
        )

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.deregister_actor", fake_deregister)
    return captured


def test_deregister_default_no_force_no_purge(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_deregister(monkeypatch)
    result = runner.invoke(app, ["actor-config", "deregister", "my-actor.run-123"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["actor"] == "my-actor.run-123"
    assert captured["kwargs"]["force"] is False
    assert captured["kwargs"]["purge_queue"] is False


def test_deregister_force_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_deregister(monkeypatch)
    result = runner.invoke(app, ["actor-config", "deregister", "my-actor", "--force"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["kwargs"]["force"] is True


def test_deregister_purge_queue_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_deregister(monkeypatch)
    result = runner.invoke(app, ["actor-config", "deregister", "my-actor", "--purge-queue"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["kwargs"]["purge_queue"] is True


def test_deregister_not_found_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.exceptions import ActorNotFoundError

    _patch_deregister(monkeypatch, raises=ActorNotFoundError("ghost"))
    result = runner.invoke(app, ["actor-config", "deregister", "ghost"])
    assert result.exit_code == 1
    assert "no stored actor_config row" in result.stderr


def test_deregister_active_jobs_error_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.exceptions import ActorHasActiveJobsError

    _patch_deregister(
        monkeypatch,
        raises=ActorHasActiveJobsError(
            "busy", active_count=3, status_counts={"pending": 2, "running": 1}
        ),
    )
    result = runner.invoke(app, ["actor-config", "deregister", "busy"])
    assert result.exit_code == 1
    assert "non-terminal" in result.stderr
    assert "force=True" in result.stderr


def test_deregister_schedules_error_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.exceptions import ActorHasEnabledSchedulesError

    _patch_deregister(
        monkeypatch,
        raises=ActorHasEnabledSchedulesError("sched-actor", ["s1", "s2"]),
    )
    result = runner.invoke(app, ["actor-config", "deregister", "sched-actor"])
    assert result.exit_code == 1
    assert "enabled cron schedule" in result.stderr


def test_deregister_output_shows_result(monkeypatch: pytest.MonkeyPatch) -> None:
    result = DeregisterResult(
        actor="my-actor",
        queue="my-queue",
        actor_config_deleted=True,
        schedules_disabled=2,
        jobs_cancelled=5,
        terminal_jobs_remaining=10,
        queue_purged=True,
    )
    _patch_deregister(monkeypatch, result=result)
    output = runner.invoke(
        app, ["actor-config", "deregister", "my-actor", "--force", "--purge-queue"]
    )
    assert output.exit_code == 0
    assert "deregistered" in output.output.lower()
    assert "schedules_disabled=2" in output.output
    assert "jobs_cancelled=5" in output.output
    assert "terminal_jobs_remaining=10" in output.output
    assert "queue_purged=true" in output.output.lower()


def test_deregister_double_deregister_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second deregister on an already-deregistered actor exits 1 with ActorNotFoundError."""
    from taskq.exceptions import ActorNotFoundError

    _patch_deregister(monkeypatch, raises=ActorNotFoundError("already-gone"))
    result = runner.invoke(app, ["actor-config", "deregister", "already-gone"])
    assert result.exit_code == 1
    assert "no stored actor_config row" in result.stderr
