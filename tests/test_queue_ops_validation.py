"""Unit and CLI tier for the ``queues`` operator surface input validation.

The integration tier (test_queue_ops_integration.py) covers the SQL path
on real Postgres; these tests pin the guards that must reject a bad value
*before anything is written*.

The headline case is ``max_concurrent=0``: it parses, passes a naive
``< 0`` guard, and only dies on the table's CHECK constraint
(``max_concurrent IS NULL OR max_concurrent >= 1`` —
01.00.04_01_pre_queue_concurrency.sql) after the round trip, surfacing a
raw asyncpg ``CheckViolationError`` traceback to the operator. NULL is
the uncapped state; an emergency drain to 0 belongs to
``actor_config.max_concurrent`` (per-actor), which legitimately allows
it.
"""

from typing import Any

import pytest
from typer.testing import CliRunner

from taskq.cli import app
from taskq.worker.queue_ops import set_queue_max_concurrent, set_queue_mode

runner = CliRunner()


class _RecordingConn:
    """Stub connection: any statement execution is a test failure."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def fetchrow(self, query: str, *args: object) -> dict[str, object]:
        self.statements.append(query)
        # A plausible stored row, so a call that misses the guard fails on
        # "DID NOT RAISE" rather than on a downstream assert.
        return {"name": "q", "mode": "strict_fifo", "max_concurrent": 0}


# ── ops tier ─────────────────────────────────────────────────────────────


async def test_rejects_zero_max_concurrent_without_writing() -> None:
    """0 is not a queue cap — NULL is. The guard must fire before the
    statement, or the DB CHECK answers with a raw CheckViolationError."""
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="max_concurrent"):
        await set_queue_max_concurrent(conn, "q", 0)
    assert conn.statements == []


async def test_rejects_negative_max_concurrent_without_writing() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="max_concurrent"):
        await set_queue_max_concurrent(conn, "q", -1)
    assert conn.statements == []


async def test_zero_via_bool_is_rejected_too() -> None:
    """``False`` is an ``int`` and would be written as 0 — the same DB
    CHECK violation, so the ``>= 1`` guard must see it."""
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="max_concurrent"):
        await set_queue_max_concurrent(conn, "q", False)  # type: ignore[arg-type]  # Why: the guard exists precisely because bool slips past isinstance(x, int).
    assert conn.statements == []


async def test_one_and_none_pass_validation() -> None:
    """Boundary values are not validation errors (their write semantics
    are covered by the integration tier)."""
    conn = _RecordingConn()
    await set_queue_max_concurrent(conn, "q", 1)
    await set_queue_max_concurrent(conn, "q", None)
    assert len(conn.statements) == 2


# ── queue name charset ───────────────────────────────────────────────────
#
# ``set_queue_mode``/``set_queue_max_concurrent`` UPSERT an operator-supplied
# name. They validated ``schema`` and ``mode`` but never the name, making them
# the only write path in TaskQ able to create a queue name the rest of the
# system rejects. ":" is the load-bearing case: queue names are concatenated
# into the flat ``taskq:global:queue:`` reservation namespace, where "foo:eu"
# is indistinguishable from queue "foo" in an "eu" sub-namespace, so two
# queues could share or steal one cap's slots. The row is inert today (the
# cap bootstrap matches against validated ``settings.queues``) -- a row that
# can never match anything, waiting for whoever finds it later.


async def test_set_queue_mode_rejects_a_colon_name_without_writing() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="invalid queue name"):
        await set_queue_mode(conn, "foo:eu", "round_robin")
    assert conn.statements == []


async def test_set_queue_max_concurrent_rejects_a_colon_name_without_writing() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="invalid queue name"):
        await set_queue_max_concurrent(conn, "foo:eu", 4)
    assert conn.statements == []


async def test_queue_name_rejection_names_the_character_and_the_allowed_set() -> None:
    """ "invalid" alone leaves the operator guessing which character lost."""
    conn = _RecordingConn()
    with pytest.raises(ValueError) as excinfo:
        await set_queue_mode(conn, "foo:eu", "round_robin")
    message = str(excinfo.value)
    assert "':'" in message, message
    assert "A-Za-z0-9_.-" in message, message


async def test_relaxed_charset_still_passes() -> None:
    """The charset was just relaxed to allow a leading digit, dots and
    hyphens -- the guard must not re-tighten it."""
    conn = _RecordingConn()
    await set_queue_mode(conn, "2024-backfill.eu", "round_robin")
    await set_queue_max_concurrent(conn, "_default", 4)
    assert len(conn.statements) == 2


# ── CLI tier ─────────────────────────────────────────────────────────────


def _patch_db(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fake asyncpg.connect + the ops function; return captured calls."""

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    captured: dict[str, Any] = {}

    async def fake_set(
        conn: Any, name: str, max_concurrent: int | None, *, schema: str = "taskq"
    ) -> None:
        captured["set"] = {"name": name, "max_concurrent": max_concurrent, "schema": schema}

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.set_queue_max_concurrent", fake_set)
    return captured


def test_cli_zero_max_concurrent_rejected_before_any_db_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--max-concurrent 0` must die at argument parsing, before the
    connection is even opened — not surface a DB traceback."""
    captured = _patch_db(monkeypatch)
    result = runner.invoke(app, ["queues", "set-max-concurrent", "q", "--max-concurrent", "0"])
    assert result.exit_code != 0, f"stdout: {result.output}"
    assert "set" not in captured, "0 must be rejected before any DB write"


def test_cli_ops_value_error_exits_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ValueError from the ops layer (e.g. an invalid schema identifier)
    must print the reason and exit 1 — mirroring `_queues_set_mode`'s
    handler — not propagate as a traceback."""

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    async def fake_set(
        conn: Any, name: str, max_concurrent: int | None, *, schema: str = "taskq"
    ) -> None:
        raise ValueError("max_concurrent must be >= 1 or None, got 0")

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.set_queue_max_concurrent", fake_set)

    result = runner.invoke(app, ["queues", "set-max-concurrent", "q", "--max-concurrent", "5"])
    assert result.exit_code == 1
    assert "max_concurrent must be >= 1" in result.stderr
    # The clean path: click/typer convert the handled exit to SystemExit. If
    # the ops ValueError escaped instead, result.exception would BE that
    # ValueError — the traceback-to-operator failure mode.
    assert isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output


class _ClosableRecordingConn(_RecordingConn):
    """``_RecordingConn`` plus the ``close`` the CLI's bounded teardown calls."""

    async def close(self) -> None: ...


def test_cli_set_mode_rejects_a_colon_queue_name_actionably(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`taskq queues set-mode foo:eu ...` must fail with a message naming
    the offending character and the allowed set -- and write nothing.

    The real ``set_queue_mode`` runs here (only the connection is faked),
    so this pins the whole CLI path, not a stubbed error.
    """
    conn = _ClosableRecordingConn()

    async def fake_connect(dsn: str) -> Any:
        return conn

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)

    result = runner.invoke(app, ["queues", "set-mode", "foo:eu", "round_robin"])

    assert result.exit_code == 1, f"output: {result.output}"
    assert conn.statements == [], "an invalid queue name must not reach the UPSERT"
    assert "invalid queue name" in result.stderr
    assert "':'" in result.stderr, result.stderr
    assert "A-Za-z0-9_.-" in result.stderr, result.stderr
    assert isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output
