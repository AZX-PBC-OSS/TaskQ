"""Tests for the ``taskq queues`` operator CLI surface.

The queue-ops SQL layer has its own unit (test_queue_ops_validation.py) and
integration (test_queue_ops_integration.py) tiers; nothing exercises the CLI
commands that wrap it. These tests pin the operator-visible contract of
``queues list`` / ``get`` / ``set-mode`` / ``set-max-concurrent``:

* an unconfigured deployment is reported as running on defaults, never as an
  error or an empty table with no explanation;
* a stored row renders its mode and its cap (or ``unlimited`` for NULL);
* ``set-max-concurrent`` refuses to guess between ``--max-concurrent`` and
  ``--clear`` before any connection is opened;
* a queue-ops ``ValueError`` (bad mode, bad name) surfaces as exit code 1
  with the guard's message, not a traceback.

The asyncpg connection is monkeypatched at the ``taskq.cli`` import boundary
(the established seam, tests/test_cli_job.py): the real queue-ops functions
run against the fake connection, so the commands' rendering and error paths
are pinned end-to-end while Postgres behavior stays out of scope.
"""

from typing import Any

import pytest
from typer.testing import CliRunner

from taskq.cli import app

runner = CliRunner()


class _FakeConn:
    """asyncpg.Connection stand-in answering the queues-table statements."""

    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        stored_row: dict[str, Any] | None = None,
    ) -> None:
        self.executed: list[str] = []
        self._rows = rows or []
        self._stored_row = stored_row

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        self.executed.append(query)
        return list(self._rows)

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        self.executed.append(query)
        return self._stored_row

    async def close(self) -> None: ...


def _patch_conn(monkeypatch: pytest.MonkeyPatch, conn: _FakeConn) -> None:
    async def fake_connect(dsn: str, *args: object, **kwargs: object) -> Any:
        return conn

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)


def _stored(
    name: str = "tenants", mode: str = "round_robin", cap: int | None = None
) -> dict[str, Any]:
    return {"name": name, "mode": mode, "max_concurrent": cap}


# ── queues list ──────────────────────────────────────────────────────────


def test_list_on_a_fresh_deployment_reports_defaults_not_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No stored rows is the normal state of a fresh deployment: queues are
    implicit and created by enqueueing. The command must say so (and that
    the defaults are strict_fifo / uncapped), not print nothing."""
    _patch_conn(monkeypatch, _FakeConn(rows=[]))

    result = runner.invoke(app, ["queues", "list"])

    assert result.exit_code == 0
    assert "no configured queues" in result.output
    assert "strict_fifo" in result.output
    assert "unlimited" in result.output


def test_list_renders_each_stored_row_with_its_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_conn(
        monkeypatch,
        _FakeConn(rows=[_stored("alpha", "round_robin", 4), _stored("beta", "strict_fifo")]),
    )

    result = runner.invoke(app, ["queues", "list"])

    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "round_robin" in result.output
    assert "max_concurrent=4" in result.output
    assert "beta" in result.output
    assert "max_concurrent=unlimited" in result.output, (
        "a NULL cap is the uncapped state; the row must render 'unlimited', "
        "not 'None' (which reads like a misconfigured cap of zero)"
    )


# ── queues get ───────────────────────────────────────────────────────────


def test_get_absent_row_says_the_queue_runs_on_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent row is not a missing queue: it runs on the defaults, and
    fairness has no effect there. The command must say exactly that."""
    _patch_conn(monkeypatch, _FakeConn())

    result = runner.invoke(app, ["queues", "get", "never-configured"])

    assert result.exit_code == 0
    assert "no stored row" in result.output
    assert "strict_fifo" in result.output
    assert "fairness_key has NO effect" in result.output


def test_get_renders_the_stored_row(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_conn(monkeypatch, _FakeConn(stored_row=_stored("tenants", "round_robin", 9)))

    result = runner.invoke(app, ["queues", "get", "tenants"])

    assert result.exit_code == 0
    assert "tenants" in result.output
    assert "mode=round_robin" in result.output
    assert "max_concurrent=9" in result.output


# ── queues set-mode ──────────────────────────────────────────────────────


def test_set_mode_upserts_and_renders_the_stored_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConn(stored_row=_stored("tenants", "round_robin"))
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["queues", "set-mode", "tenants", "round_robin"])

    assert result.exit_code == 0
    assert "mode=round_robin" in result.output
    assert any("INSERT INTO" in q for q in conn.executed), (
        "set-mode is an UPSERT (a plain UPDATE is a silent no-op on a fresh "
        "deployment): the statement must stay an INSERT ... ON CONFLICT"
    )


def test_set_mode_invalid_mode_exits_nonzero_with_the_guard_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["queues", "set-mode", "tenants", "lifo"])

    assert result.exit_code == 1
    assert "invalid queue mode" in result.output
    assert conn.executed == [], "the guard must fire before any statement is written"


# ── queues set-max-concurrent ────────────────────────────────────────────


def test_set_max_concurrent_refuses_to_guess_between_the_two_flags() -> None:
    """``--max-concurrent`` and ``--clear`` are mutually exclusive shapes of
    the same write; the command must refuse before opening a connection."""
    result = runner.invoke(
        app, ["queues", "set-max-concurrent", "q", "--max-concurrent", "4", "--clear"]
    )

    assert result.exit_code == 1
    assert "not both" in result.output


def test_set_max_concurrent_requires_one_of_the_two_flags() -> None:
    result = runner.invoke(app, ["queues", "set-max-concurrent", "q"])

    assert result.exit_code == 1
    assert "--max-concurrent N or --clear" in result.output


def test_set_max_concurrent_writes_and_renders_the_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConn(stored_row=_stored("q", "strict_fifo", 4))
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["queues", "set-max-concurrent", "q", "--max-concurrent", "4"])

    assert result.exit_code == 0
    assert "max_concurrent=4" in result.output


def test_clear_max_concurrent_renders_the_uncapped_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--clear`` writes NULL (the uncapped state); the rendered row must
    say 'unlimited', not bind 0 (which no worker would treat as a cap)."""
    conn = _FakeConn(stored_row=_stored("q", "strict_fifo", None))
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["queues", "set-max-concurrent", "q", "--clear"])

    assert result.exit_code == 0
    assert "max_concurrent=unlimited" in result.output


def test_set_max_concurrent_invalid_name_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A queue name the rest of TaskQ would reject must be refused before
    the UPSERT stores an unusable row."""
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(
        app, ["queues", "set-max-concurrent", "bad:name", "--max-concurrent", "4"]
    )

    assert result.exit_code == 1
    assert conn.executed == [], "the name guard must fire before any statement is written"
