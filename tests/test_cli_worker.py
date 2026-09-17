"""Tests for taskq worker CLI subcommand."""

import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.actor import ActorRef, actor
from taskq.cli import app, main
from taskq.exceptions import ActorConfigDriftError, ActorConfigDriftList
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()


class _Payload(BaseModel):
    value: int


@actor(name="cli_worker_actor", queue="default")
async def _cli_worker_actor(payload: _Payload) -> None: ...


# A populated registry: the CLI refuses an empty one (a worker with no
# actors dispatches nothing), so a stand-in fixture must carry an actor.
_REGISTRY: Mapping[str, ActorRef[Any, Any]] = MappingProxyType(
    {"cli_worker_actor": _cli_worker_actor}
)
_REGISTRY_PATH = "tests.test_cli_worker:_REGISTRY"

_BAD_TYPE: int = 5

_BAD_TYPE_PATH = "tests.test_cli_worker:_BAD_TYPE"

_WATCH_PATH_ONE = "/tmp/one"  # noqa: S108 # Why: literal never touched on disk — dev_watch_loop is stubbed.
_WATCH_PATH_TWO = "/tmp/two"  # noqa: S108 # Why: literal never touched on disk — dev_watch_loop is stubbed.


def test_actors_resolution_passes_registry_to_worker_main(monkeypatch: Any) -> None:
    """--actors resolution passes resolved registry to worker_main."""
    captured_settings: Any = None
    captured_registry: Any = None

    def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
        nonlocal captured_settings, captured_registry
        captured_settings = settings
        captured_registry = actor_registry
        return 42

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 42, f"stderr: {result.stderr}"
    assert captured_registry is _REGISTRY


def test_module_not_found_exit_code_and_message() -> None:
    """missing module produces exit code 1 and module error."""
    result = runner.invoke(app, ["worker", "--actors", "no.such.module:registry"])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "module not found" in result.stderr.lower()


def test_bad_type_exit_code_and_message() -> None:
    """non-Mapping/non-Iterable attribute produces exit code 1 and type error."""
    result = runner.invoke(app, ["worker", "--actors", _BAD_TYPE_PATH])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "expected Mapping[str, ActorRef] or Iterable[ActorRef]" in result.stderr


def test_force_update_flag_true(monkeypatch: Any) -> None:
    """--force-update-actor-config passes True in settings."""
    captured_settings: Any = None

    def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
        nonlocal captured_settings
        captured_settings = settings
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(
        app, ["worker", "--actors", _REGISTRY_PATH, "--force-update-actor-config"]
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_settings is not None
    assert captured_settings.force_update_actor_config is True


def test_force_update_flag_default_false(monkeypatch: Any) -> None:
    """without --force-update-actor-config, settings is False."""
    captured_settings: Any = None

    def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
        nonlocal captured_settings
        captured_settings = settings
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_settings is not None
    assert captured_settings.force_update_actor_config is False


def test_env_var_force_update_config(monkeypatch: Any) -> None:
    """TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true reflected in settings via dotenvmodel."""
    captured_settings: Any = None

    def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
        nonlocal captured_settings
        captured_settings = settings
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    monkeypatch.setenv("TASKQ_FORCE_UPDATE_ACTOR_CONFIG", "true")
    result = runner.invoke(app, ["worker", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_settings is not None
    assert captured_settings.force_update_actor_config is True


def test_drift_error_produces_exit_one_and_hint(monkeypatch: Any) -> None:
    """ActorConfigDriftList caught at CLI — exit 1 with drift message and hint."""
    drift_error = ActorConfigDriftError(
        actor="test_actor",
        field="metadata",
        registered={"team": "platform"},
        stored={"team": "ops"},
    )
    drift_list = ActorConfigDriftList((drift_error,))

    def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
        raise drift_list

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert str(drift_error) in result.stderr
    assert "--force-update-actor-config" in result.stderr


# ── dev_watch subcommand ─────────────────────────────────────────────────


def test_dev_watch_module_not_found_exit_code_and_message() -> None:
    """dev: missing module produces exit code 1 and 'module not found' message."""
    result = runner.invoke(app, ["dev", "no.such.module:registry"])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "cannot import" in result.stderr.lower()
    assert "module not found" in result.stderr.lower()


def test_dev_watch_bad_actors_syntax_exit_code_and_message() -> None:
    """dev: missing 'module:attr' separator produces exit code 1."""
    result = runner.invoke(app, ["dev", "no_colon_here"])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "expected module:attr syntax" in result.stderr


def test_dev_watch_generic_import_error_exit_code_and_message(monkeypatch: Any) -> None:
    """dev: a generic (non-ModuleNotFoundError) import exception is reported and exits 1."""

    def fake_import_module(name: str) -> Any:
        if name == "tests.test_cli_worker":
            raise RuntimeError("boom during import")
        raise ModuleNotFoundError(name)

    monkeypatch.setattr("taskq.cli.importlib.import_module", fake_import_module)
    result = runner.invoke(app, ["dev", _REGISTRY_PATH])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "boom during import" in result.stderr


def test_dev_watch_attribute_not_found_exit_code_and_message() -> None:
    """dev: attribute missing from an otherwise-importable module exits 1."""
    result = runner.invoke(app, ["dev", "tests.test_cli_worker:_DOES_NOT_EXIST"])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "not found in module" in result.stderr


def test_dev_watch_happy_path_default_cwd(monkeypatch: Any) -> None:
    """dev: with no --watch, watch_paths defaults to [str(Path.cwd())] and dev_watch_loop runs."""
    captured: dict[str, Any] = {}

    async def fake_dev_watch_loop(
        module_attr: str, *, watch_paths: Sequence[str], grace_period: float
    ) -> None:
        captured["module_attr"] = module_attr
        captured["watch_paths"] = list(watch_paths)
        captured["grace_period"] = grace_period

    monkeypatch.setattr("taskq.cli.dev_watch_loop", fake_dev_watch_loop)
    result = runner.invoke(app, ["dev", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["module_attr"] == _REGISTRY_PATH
    assert captured["watch_paths"] == [str(Path.cwd())]
    assert captured["grace_period"] == 5.0
    assert "watching" in plain_cli_output(result.stderr).lower()


def test_dev_watch_happy_path_explicit_watch_paths(monkeypatch: Any) -> None:
    """dev: repeatable --watch collects multiple paths and grace_period is forwarded."""
    captured: dict[str, Any] = {}

    async def fake_dev_watch_loop(
        module_attr: str, *, watch_paths: Sequence[str], grace_period: float
    ) -> None:
        captured["watch_paths"] = list(watch_paths)
        captured["grace_period"] = grace_period

    monkeypatch.setattr("taskq.cli.dev_watch_loop", fake_dev_watch_loop)
    result = runner.invoke(
        app,
        [
            "dev",
            _REGISTRY_PATH,
            "--watch",
            _WATCH_PATH_ONE,
            "--watch",
            _WATCH_PATH_TWO,
            "--grace-period",
            "9",
        ],
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["watch_paths"] == [_WATCH_PATH_ONE, _WATCH_PATH_TWO]
    assert captured["grace_period"] == 9.0
    watching_msg = plain_cli_output(result.stderr).lower()
    assert _WATCH_PATH_ONE in watching_msg
    assert _WATCH_PATH_TWO in watching_msg


# ── main() console-script entry point ─────────────────────────────────────


def test_main_invokes_app(monkeypatch: Any) -> None:
    """main() is a thin wrapper that invokes the Typer app with no arguments."""
    import taskq.cli as cli_mod

    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(cli_mod, "app", lambda *a: calls.append(a))
    cli_mod.main()
    assert calls == [()]


# ── until-idle CLI flags ─────────────────────────────────────────────────


def test_until_idle_flag_passed_to_worker_main(monkeypatch: Any) -> None:
    """--until-idle passes until_idle=True to worker_main."""
    captured: dict[str, Any] = {}

    def fake_worker_main(
        settings: Any,
        *,
        actor_registry: Any = None,
        until_idle: bool = False,
        **kwargs: Any,
    ) -> int:
        captured["until_idle"] = until_idle
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _REGISTRY_PATH, "--until-idle"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["until_idle"] is True


def test_until_idle_default_false(monkeypatch: Any) -> None:
    """Without --until-idle, until_idle=False."""
    captured: dict[str, Any] = {}

    def fake_worker_main(
        settings: Any,
        *,
        actor_registry: Any = None,
        until_idle: bool = False,
        **kwargs: Any,
    ) -> int:
        captured["until_idle"] = until_idle
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["until_idle"] is False


def test_idle_settle_window_passed(monkeypatch: Any) -> None:
    """--idle-settle-window passes the value to worker_main."""
    captured: dict[str, Any] = {}

    def fake_worker_main(
        settings: Any,
        *,
        actor_registry: Any = None,
        idle_settle_window: float | None = None,
        **kwargs: Any,
    ) -> int:
        captured["idle_settle_window"] = idle_settle_window
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(
        app,
        [
            "worker",
            "--actors",
            _REGISTRY_PATH,
            "--until-idle",
            "--idle-settle-window",
            "5.0",
        ],
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["idle_settle_window"] == 5.0


def test_idle_max_runtime_passed(monkeypatch: Any) -> None:
    """--idle-max-runtime passes the value to worker_main."""
    captured: dict[str, Any] = {}

    def fake_worker_main(
        settings: Any,
        *,
        actor_registry: Any = None,
        idle_max_runtime: float | None = None,
        **kwargs: Any,
    ) -> int:
        captured["idle_max_runtime"] = idle_max_runtime
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(
        app,
        [
            "worker",
            "--actors",
            _REGISTRY_PATH,
            "--until-idle",
            "--idle-max-runtime",
            "300",
        ],
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["idle_max_runtime"] == 300.0


def test_idle_poll_interval_passed(monkeypatch: Any) -> None:
    """--idle-poll-interval passes the value to worker_main."""
    captured: dict[str, Any] = {}

    def fake_worker_main(
        settings: Any,
        *,
        actor_registry: Any = None,
        idle_poll_interval: float | None = None,
        **kwargs: Any,
    ) -> int:
        captured["idle_poll_interval"] = idle_poll_interval
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(
        app,
        [
            "worker",
            "--actors",
            _REGISTRY_PATH,
            "--until-idle",
            "--idle-poll-interval",
            "0.5",
        ],
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["idle_poll_interval"] == 0.5


# ── console-script entry resolves application modules from the cwd ──────


def test_console_script_resolves_actors_from_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``taskq worker --actors myapp.actors:registry`` works from a fresh
    project directory: the console script puts the cwd on ``sys.path``
    (``python -m`` semantics), so a ``module:attr`` ref resolves a module
    that lives only in the directory the operator ran the command from.

    Driven through the real console entry (``taskq.cli.main``), not the
    Typer app directly — the insertion happens at the entry point, so a
    test that bypasses it proves nothing. The scratch module is written
    into a tmp dir and evicted from ``sys.modules`` at teardown; the
    ``sys.path`` and cwd mutations revert via monkeypatch.
    """
    import sys

    (tmp_path / "scratch_actors.py").write_text(
        "from pydantic import BaseModel\n"
        "from taskq.actor import actor\n"
        "\n"
        "\n"
        "class _Payload(BaseModel):\n"
        "    value: int\n"
        "\n"
        "\n"
        '@actor(name="scratch_actor")\n'
        "async def scratch_actor(payload: _Payload) -> None: ...\n"
        "\n"
        "\n"
        'registry = {"scratch_actor": scratch_actor}\n'
    )
    monkeypatch.chdir(tmp_path)
    # The console script starts with the cwd absent from sys.path; scrub it
    # so the test cannot pass on a stray entry rather than on the fix.
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != str(tmp_path)])
    monkeypatch.setattr(sys, "argv", ["taskq", "worker", "--actors", "scratch_actors:registry"])

    captured: dict[str, Any] = {}

    def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
        captured["actor_registry"] = actor_registry
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)

    try:
        with pytest.raises(SystemExit) as exc_info:
            main()
    finally:
        sys.modules.pop("scratch_actors", None)

    assert exc_info.value.code == 0, (
        "the console entry must reach worker_main, not 'module not found'"
    )
    assert captured["actor_registry"] is not None
    assert list(captured["actor_registry"]) == ["scratch_actor"]


def test_console_script_cwd_insertion_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-running the entry (or a ``python -m taskq`` start, which already
    carries the cwd) never stacks duplicate ``sys.path`` entries."""
    import sys

    from taskq.cli import _ensure_cwd_on_sys_path

    # Both frames are synthetic: the live ``sys.path`` cannot serve as the
    # "cwd absent" frame under a full-suite run, because earlier tests invoke
    # ``main()`` against the real ``sys.path`` and its insertion (correct
    # production behaviour, no monkeypatch rollback) survives into this test's
    # starting copy — the starting count there is a run-order artifact, not a
    # property of the helper.
    monkeypatch.setattr(sys, "path", ["/does-not-exist"])

    _ensure_cwd_on_sys_path()
    _ensure_cwd_on_sys_path()

    assert sys.path.count(os.getcwd()) == 1
    assert sys.path[0] == os.getcwd()

    # The ``python -m taskq`` shape: the cwd is already carried, and a
    # re-run of the entry must not stack a duplicate behind it.
    monkeypatch.setattr(sys, "path", [os.getcwd(), "/does-not-exist"])

    _ensure_cwd_on_sys_path()
    _ensure_cwd_on_sys_path()

    assert sys.path.count(os.getcwd()) == 1
    assert sys.path[0] == os.getcwd()
