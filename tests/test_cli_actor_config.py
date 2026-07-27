"""Tests for the `taskq actor-config` CLI surface.

The asyncpg connection and the ops functions are monkeypatched at the
``taskq.cli`` import boundary — these tests pin the CLI's argument
parsing, validation, error messages, and output shape, not Postgres
behavior (covered by the integration tier in test_actor_config_ops.py).
"""

from collections.abc import Mapping
from typing import Any, cast

import pytest
import typer.main
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.actor import ActorRef, actor
from taskq.cli import app
from taskq.worker.actor_config_ops import ActorConfigRow

runner = CliRunner()

_ROW = ActorConfigRow(
    actor="diff_actor",
    max_concurrent=5,
    max_pending=10,
    queue="default",
    result_ttl=60.0,
    metadata={},
    updated_at="2026-01-01 00:00:00+00",
)


class _DiffPayload(BaseModel):
    value: int


@actor(name="diff_actor", max_concurrent=4, max_pending=100, queue="critical")
async def _diff_actor(payload: _DiffPayload) -> None: ...


_REGISTRY: Mapping[str, ActorRef[Any, Any]] = {"diff_actor": _diff_actor}
_REGISTRY_PATH = "tests.test_cli_actor_config:_REGISTRY"


def _patch_db(
    monkeypatch: pytest.MonkeyPatch,
    *,
    set_result: ActorConfigRow | None = _ROW,
    get_result: ActorConfigRow | None = _ROW,
    list_result: list[ActorConfigRow] | None = None,
) -> dict[str, Any]:
    """Fake asyncpg.connect + the ops functions; return captured call kwargs."""
    captured: dict[str, Any] = {}

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    async def fake_set(conn: Any, actor: str, **kwargs: Any) -> ActorConfigRow | None:
        captured["set"] = {"actor": actor, **kwargs}
        return set_result

    async def fake_get(conn: Any, actor: str, **kwargs: Any) -> ActorConfigRow | None:
        captured["get"] = {"actor": actor, **kwargs}
        return get_result

    async def fake_list(conn: Any, **kwargs: Any) -> list[ActorConfigRow]:
        return list_result if list_result is not None else [_ROW]

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.set_actor_config_capacity", fake_set)
    monkeypatch.setattr("taskq.cli.get_actor_config", fake_get)
    monkeypatch.setattr("taskq.cli.list_actor_configs", fake_list)
    return captured


# ── set ──────────────────────────────────────────────────────────────────


def test_set_max_pending_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    """--max-pending reaches set_actor_config_capacity — the flag exists and
    drives the stored value the enqueue path now enforces."""
    captured = _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--max-pending", "7"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["set"]["actor"] == "diff_actor"
    assert captured["set"]["max_pending"] == 7


def test_set_max_pending_clear_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    """--clear-max-pending writes NULL (revert to the code literal)."""
    captured = _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--clear-max-pending"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["set"]["max_pending"] is None


def test_set_max_pending_and_clear_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_db(monkeypatch)
    result = runner.invoke(
        app, ["actor-config", "set", "diff_actor", "--max-pending", "7", "--clear-max-pending"]
    )
    assert result.exit_code == 1
    assert "mutually exclusive" in result.stderr


def test_set_negative_max_pending_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """typer's min=0 guard rejects negative values before any DB call."""
    captured = _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--max-pending", "-1"])
    assert result.exit_code != 0
    assert "set" not in captured


def test_set_without_flags_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor"])
    assert result.exit_code == 1
    assert "nothing to change" in result.stderr


def test_set_unknown_actor_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch, set_result=None)
    result = runner.invoke(app, ["actor-config", "set", "ghost", "--max-concurrent", "5"])
    assert result.exit_code == 1
    assert "no stored actor_config row" in result.stderr


def test_set_help_lists_all_capacity_fields() -> None:
    """`actor-config set` exposes a flag for every capacity field.

    Asserted against the declared parameters rather than rendered ``--help``
    text: Rich wraps help output to the terminal width, so a substring check
    on the rendering fails on narrow terminals (e.g. CI) even though the flag
    is present.
    """
    root = cast(Any, typer.main.get_command(app))
    set_cmd = root.commands["actor-config"].commands["set"]
    declared = {opt for param in set_cmd.params for opt in param.opts}
    for flag in ("--max-concurrent", "--max-pending", "--result-ttl", "--clear-max-pending"):
        assert flag in declared, f"{flag} missing from `actor-config set`"


# ── get / list ───────────────────────────────────────────────────────────


def test_get_unknown_actor_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch, get_result=None)
    result = runner.invoke(app, ["actor-config", "get", "ghost"])
    assert result.exit_code == 1
    assert "no stored actor_config row" in result.stderr


def test_list_prints_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "list"])
    assert result.exit_code == 0
    assert "diff_actor" in result.output
    assert "max_pending=10" in result.output


# ── diff ─────────────────────────────────────────────────────────────────


def test_diff_shows_literal_stored_and_effective(monkeypatch: pytest.MonkeyPatch) -> None:
    """The operator's debugging view: why is my change (not) taking effect.

    Registry literal max_pending=100 vs stored 10 → effective is the
    stored 10, and the output says so.
    """
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "diff_actor" in result.output
    assert "literal=100" in result.output
    assert "stored=10" in result.output
    assert "effective=10" in result.output


def test_diff_flags_structural_drift_as_startup_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registry queue=critical vs stored queue=default → the output warns
    that the next worker startup raises ActorConfigDriftList."""
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "queue" in result.output
    assert "ActorConfigDriftList" in result.output


def test_diff_marks_actor_without_stored_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """An actor the registry declares but no worker has synced yet: the
    literal applies and will seed the row at the next startup."""
    _patch_db(monkeypatch, list_result=[])
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "no stored row" in result.output
    assert "literal applies" in result.output


def test_diff_marks_leftover_row_not_in_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stored row whose actor is no longer registered is shown as leftover."""
    ghost = ActorConfigRow(
        actor="ghost",
        max_concurrent=1,
        max_pending=None,
        queue="default",
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01 00:00:00+00",
    )
    _patch_db(monkeypatch, list_result=[ghost])
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "ghost" in result.output
    assert "not in the registry" in result.output
