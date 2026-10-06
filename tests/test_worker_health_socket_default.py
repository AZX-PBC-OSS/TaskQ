"""Pins for the per-worker-unique default health socket path.

Red (proven live at the audit base): two ``taskq worker`` boots on one
host with no explicit configuration both targeted the static default
``/tmp/taskq_health.sock``. The second logged
``health-server-unavailable`` and kept running, and ``taskq health``
then silently answered with the FIRST worker's state (exit 0, a healthy
body) — the cross-report that made the worker that just warned look
healthy. The fix: a worker booted with NO explicit configuration (no
``--health-socket-path``, no ``TASKQ_HEALTH_SOCKET_PATH`` in the
environment or the .env cascade) binds ``/tmp/taskq_health_<pid>.sock``
instead; every explicit spelling stays authoritative verbatim.
"""

import os
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq import actor
from taskq.cli import app
from taskq.settings import WorkerSettings
from taskq.testing.assertions import plain_cli_output
from taskq.worker.health import default_worker_health_socket_path

runner = CliRunner()

_STATIC_DEFAULT = "/tmp/taskq_health.sock"  # noqa: S108  # Why: the pin asserts the settings field's own static default; no file operations touch this path.


class _SockPayload(BaseModel):
    value: int = 0


@actor(name="sock_default_actor", queue="default")
async def _sock_default_actor(payload: "_SockPayload") -> None: ...


_REGISTRY = {"sock_default_actor": _sock_default_actor}
_REGISTRY_PATH = "tests.test_worker_health_socket_default:_REGISTRY"


def _invoke_worker(monkeypatch: pytest.MonkeyPatch, *extra: str):  # type: ignore[no-untyped-def]
    captured: dict[str, Any] = {}

    def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
        captured["settings"] = settings
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _REGISTRY_PATH, *extra])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    return captured["settings"]


def test_no_explicit_config_mints_a_pid_unique_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first-run shape: nothing configured, the worker does NOT bind
    the shared static default."""
    monkeypatch.delenv("TASKQ_HEALTH_SOCKET_PATH", raising=False)
    settings = _invoke_worker(monkeypatch)
    assert settings.health_socket_path == default_worker_health_socket_path()
    assert settings.health_socket_path == f"/tmp/taskq_health_{os.getpid()}.sock"  # noqa: S108  # Why: the pin asserts the minted default's own construction.
    assert settings.health_socket_path != _STATIC_DEFAULT


def test_minted_paths_differ_across_pids() -> None:
    """Two workers on one host = two pids = two sockets (the uniqueness
    law the cross-report defect violated)."""
    assert default_worker_health_socket_path(pid=111) != default_worker_health_socket_path(pid=222)
    assert default_worker_health_socket_path(pid=111) == "/tmp/taskq_health_111.sock"  # noqa: S108  # Why: pins the constructed path shape against accidental format drift.


def test_env_override_stays_authoritative(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit TASKQ_HEALTH_SOCKET_PATH wins verbatim — INCLUDING one
    spelled as the old static default (that operator asked for the shared
    path and keeps its documented collision warning)."""
    monkeypatch.setenv("TASKQ_HEALTH_SOCKET_PATH", _STATIC_DEFAULT)
    settings = _invoke_worker(monkeypatch)
    assert settings.health_socket_path == _STATIC_DEFAULT

    explicit = "/run/taskq/health.sock"
    monkeypatch.setenv("TASKQ_HEALTH_SOCKET_PATH", explicit)
    settings = _invoke_worker(monkeypatch)
    assert settings.health_socket_path == explicit


def test_cli_option_stays_authoritative(monkeypatch: pytest.MonkeyPatch) -> None:
    """--health-socket-path wins over everything, verbatim (the
    workgroup supervisor's own spawn contract hands child paths this
    way)."""
    monkeypatch.delenv("TASKQ_HEALTH_SOCKET_PATH", raising=False)
    settings = _invoke_worker(
        monkeypatch,
        "--health-socket-path",
        "/tmp/wg_child.sock",  # noqa: S108  # Why: literal never bound in this test - worker_main is stubbed.
    )
    assert settings.health_socket_path == "/tmp/wg_child.sock"  # noqa: S108  # Why: same stubbed-worker literal.


def test_static_default_still_exists_as_the_settings_fallback() -> None:
    """The FIELD default keeps its static value: the mint happens at
    worker-boot provenance (cli resolution), not by rewriting the
    settings model, so a settings-only reader (the ``taskq health`` CLI
    in its own process) still resolves the same cascade and reports
    'health socket unreachable' loudly rather than cross-reporting."""
    settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"})
    assert settings.health_socket_path == _STATIC_DEFAULT


def test_unreachable_without_env_hints_at_the_per_pid_discovery_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Red: with no env override, an unreachable probe printed a bare
    ENOENT — no pointer to the per-pid default the worker now binds, nor
    to the env var that finds it. ``taskq health`` is the discovery
    surface for the minted path, so its loudest failure must teach the
    contract: name the per-pid default shape and the override."""
    monkeypatch.delenv("TASKQ_HEALTH_SOCKET_PATH", raising=False)
    Path(_STATIC_DEFAULT).unlink(missing_ok=True)
    result = runner.invoke(app, ["health", "ready"])
    assert result.exit_code == 1
    err = plain_cli_output(result.stderr)
    assert "health socket unreachable" in err
    assert "taskq_health_<pid>" in err
    assert "TASKQ_HEALTH_SOCKET_PATH" in err


def test_unreachable_with_env_override_stays_plain(monkeypatch: pytest.MonkeyPatch) -> None:
    """The hint is the NO-CONFIG discovery contract's other half only: an
    operator who set an explicit path (here, one that does not exist)
    gets the plain unreachable line, no per-pid noise."""
    monkeypatch.setenv("TASKQ_HEALTH_SOCKET_PATH", "/nonexistent-rt667/probe.sock")
    result = runner.invoke(app, ["health", "ready"])
    assert result.exit_code == 1
    err = plain_cli_output(result.stderr)
    assert "health socket unreachable" in err
    assert "taskq_health_<pid>" not in err
