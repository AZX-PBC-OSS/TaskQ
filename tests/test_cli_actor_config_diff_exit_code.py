"""`taskq actor-config diff` must exit non-zero when drift is present.

The command's own output names queue/metadata mismatches as "structural
drift" that will refuse the next worker boot with ActorConfigDriftList —
exactly the state a CI gate exists to catch. A diff that detects drift,
prints that it blocks startup, and then exits 0 reports failure as
success to the shell; the exit code is the contract a CI pipeline can
gate on.
"""

from collections.abc import Mapping
from typing import Any

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.actor import ActorRef, actor
from taskq.actor_config_ops import ActorConfigRow
from taskq.cli import app

runner = CliRunner()


class _Payload(BaseModel):
    value: int


@actor(name="drift_actor", queue="new_tier")
async def _drift_actor(payload: _Payload) -> None: ...


_REGISTRY: Mapping[str, ActorRef[Any, Any]] = {"drift_actor": _drift_actor}
_REGISTRY_PATH = "tests.test_cli_actor_config_diff_exit_code:_REGISTRY"


def _patch_db(monkeypatch: pytest.MonkeyPatch, rows: list[ActorConfigRow]) -> None:
    """Fake asyncpg.connect + list_actor_configs at the taskq.cli boundary."""

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    async def fake_list(conn: Any, **kwargs: Any) -> list[ActorConfigRow]:
        return rows

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.list_actor_configs", fake_list)


def test_diff_exits_nonzero_on_structural_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stored row whose queue disagrees with the code literal is the exact
    state that refuses worker boot — the diff must not report success."""
    drifted = ActorConfigRow(
        actor="drift_actor",
        max_concurrent=None,
        max_pending=None,
        queue="old_tier",
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01 00:00:00+00",
    )
    _patch_db(monkeypatch, [drifted])

    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])

    assert "MISMATCH" in result.output
    assert result.exit_code != 0


def test_diff_exits_zero_when_stored_rows_match_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """No drift — the command genuinely has nothing to report — exits 0."""
    matching = ActorConfigRow(
        actor="drift_actor",
        max_concurrent=None,
        max_pending=None,
        queue="new_tier",
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01 00:00:00+00",
    )
    _patch_db(monkeypatch, [matching])

    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])

    assert "MISMATCH" not in result.output
    assert result.exit_code == 0


def test_diff_exits_nonzero_when_registry_actor_has_no_stored_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registry actor with no stored row is blocked at dispatch, not boot:
    the next worker startup seeds the row and boots fine, but until then the
    dispatch capacity gate never selects the actor. Blocking dispatch is
    blocking — the diff must fail the run."""
    _patch_db(monkeypatch, [])

    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])

    assert "DOES NOT DISPATCH" in result.output
    assert result.exit_code != 0
