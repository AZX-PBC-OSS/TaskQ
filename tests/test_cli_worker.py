"""Tests for taskq worker CLI subcommand."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any

from typer.testing import CliRunner

from taskq.actor import ActorRef
from taskq.cli import app
from taskq.exceptions import ActorConfigDriftError, ActorConfigDriftList
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()

_NO_ACTORS: Mapping[str, ActorRef[Any, Any]] = MappingProxyType({})
_NO_ACTORS_BAD_TYPE: int = 5

_NO_ACTORS_PATH = "tests.test_cli_worker:_NO_ACTORS"
_BAD_TYPE_PATH = "tests.test_cli_worker:_NO_ACTORS_BAD_TYPE"

_WATCH_PATH_ONE = "/tmp/one"  # noqa: S108 # Why: literal never touched on disk — dev_watch_loop is stubbed.
_WATCH_PATH_TWO = "/tmp/two"  # noqa: S108 # Why: literal never touched on disk — dev_watch_loop is stubbed.


def test_actors_resolution_passes_registry_to_worker_main(monkeypatch: Any) -> None:
    """--actors resolution passes resolved registry to worker_main."""
    captured_settings: Any = None
    captured_registry: Any = None

    def fake_worker_main(settings: Any, *, actor_registry: Any = None) -> int:
        nonlocal captured_settings, captured_registry
        captured_settings = settings
        captured_registry = actor_registry
        return 42

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _NO_ACTORS_PATH])
    assert result.exit_code == 42, f"stderr: {result.stderr}"
    assert captured_registry is _NO_ACTORS


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

    def fake_worker_main(settings: Any, *, actor_registry: Any = None) -> int:
        nonlocal captured_settings
        captured_settings = settings
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(
        app, ["worker", "--actors", _NO_ACTORS_PATH, "--force-update-actor-config"]
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_settings is not None
    assert captured_settings.force_update_actor_config is True


def test_force_update_flag_default_false(monkeypatch: Any) -> None:
    """without --force-update-actor-config, settings is False."""
    captured_settings: Any = None

    def fake_worker_main(settings: Any, *, actor_registry: Any = None) -> int:
        nonlocal captured_settings
        captured_settings = settings
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _NO_ACTORS_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_settings is not None
    assert captured_settings.force_update_actor_config is False


def test_env_var_force_update_config(monkeypatch: Any) -> None:
    """TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true reflected in settings via dotenvmodel."""
    captured_settings: Any = None

    def fake_worker_main(settings: Any, *, actor_registry: Any = None) -> int:
        nonlocal captured_settings
        captured_settings = settings
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    monkeypatch.setenv("TASKQ_FORCE_UPDATE_ACTOR_CONFIG", "true")
    result = runner.invoke(app, ["worker", "--actors", _NO_ACTORS_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_settings is not None
    assert captured_settings.force_update_actor_config is True


def test_drift_error_produces_exit_one_and_hint(monkeypatch: Any) -> None:
    """ActorConfigDriftList caught at CLI — exit 1 with drift message and hint."""
    drift_error = ActorConfigDriftError(
        actor="test_actor",
        field="max_concurrent",
        registered=3,
        stored=5,
    )
    drift_list = ActorConfigDriftList((drift_error,))

    def fake_worker_main(settings: Any, *, actor_registry: Any = None) -> int:
        raise drift_list

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _NO_ACTORS_PATH])
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
    result = runner.invoke(app, ["dev", "tests.test_cli_worker:_NO_ACTORS"])
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
    result = runner.invoke(app, ["dev", _NO_ACTORS_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["module_attr"] == _NO_ACTORS_PATH
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
            _NO_ACTORS_PATH,
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
