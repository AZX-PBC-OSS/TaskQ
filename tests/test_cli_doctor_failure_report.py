"""Pins for ``taskq doctor``'s connection-failure report.

Red (proven live at the audit base): a doctor run against an unreachable
Postgres escaped as a raw traceback (the connect's OSError, exit 1) while
every other CLI failure surface renders migrate up's two-line pattern
(``<thing> failed: <cause>`` + ``Action: <fix>``). The doctor now adopts
that pattern: the caught failure prints ``doctor failed: <cause>`` plus
the Action line to stderr and exits 1, no traceback.
"""

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq import actor
from taskq.cli import app
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()

# A dead port on loopback: connect fails fast, no container needed.
_DEAD_DSN = "postgresql://taskq:taskq@localhost:1/taskq"


class _DocPayload(BaseModel):
    value: int = 0


@actor(name="doctor_failure_actor", queue="default")
async def _doctor_failure_actor(payload: _DocPayload) -> None: ...


_REGISTRY = {"doctor_failure_actor": _doctor_failure_actor}
_REGISTRY_PATH = "tests.test_cli_doctor_failure_report:_REGISTRY"


def _invoke_doctor(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TASKQ_PG_DSN", _DEAD_DSN)
    return runner.invoke(app, ["doctor", "--actors", _REGISTRY_PATH])


def test_unreachable_pg_exits_1_with_action_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dead-DSN run: exit 1, the cause, the fix, no traceback."""
    result = _invoke_doctor(monkeypatch)
    assert result.exit_code == 1
    out = plain_cli_output(result.output + result.stderr)
    assert "doctor failed:" in out
    assert "Action: fix the error and re-run `taskq doctor`." in out
    assert "Traceback" not in out


def test_failure_names_the_underlying_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The headline is the connect error's own cause, not a bare type name."""
    result = _invoke_doctor(monkeypatch)
    out = plain_cli_output(result.output + result.stderr)
    line = next(line for line in out.splitlines() if line.startswith("doctor failed:"))
    assert "Connect call failed" in line or "refused" in line.lower()
