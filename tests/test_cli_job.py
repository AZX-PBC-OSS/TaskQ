"""Tests for the `taskq job show` CLI surface.

The asyncpg connection is monkeypatched at the ``taskq.cli`` import
boundary — these tests pin argument validation, the jobs → jobs_archive
fallback, the missing-job exit code, and the ``max_attempts`` rendering
contract: under ``retry_kind='indefinite'`` the stored ceiling is inert
(the retry path never consults it), so the command must render the
inertness rather than a number that claims a budget the job does not
carry. Postgres behavior is not under test here.
"""

from typing import Any

import pytest
from typer.testing import CliRunner

from taskq.cli import app

runner = CliRunner()

_JOB_ID = "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"


def _job_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": _JOB_ID,
        "actor": "send_email",
        "queue": "default",
        "status": "succeeded",
        "priority": 0,
        "attempt": 5,
        "max_attempts": 3,
        "retry_kind": "transient",
        "created_at": "2026-01-01 00:00:00+00:00",
        "scheduled_at": "2026-01-01 00:00:00+00:00",
        "started_at": "2026-01-01 00:00:01+00:00",
        "finished_at": "2026-01-01 00:00:02+00:00",
        "error_class": None,
        "error_message": None,
        "idempotency_key": None,
    }
    row.update(overrides)
    return row


def _patch_db(
    monkeypatch: pytest.MonkeyPatch,
    row: dict[str, Any] | None,
    archive_row: dict[str, Any] | None = None,
) -> None:
    """Fake asyncpg.connect; fetchrow answers the jobs probe, then the
    jobs_archive fallback, from the canned rows."""

    class _FakeConn:
        async def fetchrow(self, sql: str, *args: object) -> Any:
            if "jobs_archive" in sql:
                return archive_row
            return row

        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)


def test_show_renders_inert_max_attempts_for_indefinite_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """retry_kind='indefinite': the inert stored ceiling is rendered as
    such, and the bare number (which would claim a budget the job does
    not carry) must not appear."""
    _patch_db(monkeypatch, _job_row(retry_kind="indefinite", max_attempts=3, attempt=168))

    result = runner.invoke(app, ["job", "show", _JOB_ID])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "max_attempts: — (indefinite)" in result.output
    assert "max_attempts: 3" not in result.output
    assert "attempt: 168" in result.output


def test_show_renders_max_attempts_number_for_bounded_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """retry_kind='transient': the ceiling is live, so it is shown."""
    _patch_db(monkeypatch, _job_row(retry_kind="transient", max_attempts=3))

    result = runner.invoke(app, ["job", "show", _JOB_ID])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "max_attempts: 3" in result.output
    assert "indefinite" not in result.output


def test_show_falls_back_to_jobs_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pruned terminal job is shown from the archive and marked as such —
    an archived success must not read as 'no such job'."""
    _patch_db(monkeypatch, None, archive_row=_job_row())

    result = runner.invoke(app, ["job", "show", _JOB_ID])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert f"id: {_JOB_ID}" in result.output
    assert "archived: yes" in result.output


def test_show_unknown_job_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    """No row in either table: the shell contract is a non-zero exit — a
    show that found nothing must not read as success."""
    _patch_db(monkeypatch, None)

    result = runner.invoke(app, ["job", "show", _JOB_ID])

    assert result.exit_code == 1
    assert _JOB_ID in result.stderr


def test_show_rejects_a_malformed_job_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-UUID argument is a usage error named as such, before any I/O."""
    result = runner.invoke(app, ["job", "show", "not-a-uuid"])

    assert result.exit_code == 1
    assert "expected a UUID" in result.stderr
