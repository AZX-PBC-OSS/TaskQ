"""Tests for taskq workgroup CLI subcommand: start and validate."""

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from taskq.cli import app
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()

_MINIMAL_VALID_TOML = """
actors = "myapp.actors:registry"

[[workers]]
name = "api"
queues = ["default"]
"""


# ── config file not found ─────────────────────────────────────────────────


def test_workgroup_start_config_not_found(tmp_path: Path) -> None:
    """workgroup start exits 1 with a message naming the missing config path."""
    missing = tmp_path / "does-not-exist.toml"
    result = runner.invoke(app, ["workgroup", "start", str(missing)])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "config file not found" in result.stderr
    assert str(missing) in result.stderr


def test_workgroup_validate_config_not_found(tmp_path: Path) -> None:
    """workgroup validate exits 1 with a message naming the missing config path."""
    missing = tmp_path / "does-not-exist.toml"
    result = runner.invoke(app, ["workgroup", "validate", str(missing)])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "config file not found" in result.stderr
    assert str(missing) in result.stderr


# ── workgroup start ────────────────────────────────────────────────────────


def test_workgroup_start_happy_path(monkeypatch: Any, tmp_path: Path) -> None:
    """workgroup start loads config and awaits run_forever with the config path."""
    config_path = tmp_path / "workgroup.toml"
    config_path.write_text(_MINIMAL_VALID_TOML)

    captured: dict[str, Any] = {}

    async def fake_run_forever(config: Path) -> None:
        captured["config"] = config

    monkeypatch.setattr("taskq.worker.workgroup.run_forever", fake_run_forever)

    result = runner.invoke(app, ["workgroup", "start", str(config_path)])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["config"] == config_path


# ── workgroup validate ─────────────────────────────────────────────────────


def test_workgroup_validate_happy_path(tmp_path: Path) -> None:
    """workgroup validate reports 'config OK' and a summary line per worker."""
    config_path = tmp_path / "workgroup.toml"
    config_path.write_text(_MINIMAL_VALID_TOML)

    result = runner.invoke(app, ["workgroup", "validate", str(config_path)])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    plain = plain_cli_output(result.output)
    assert "config OK" in plain
    assert "1 worker(s)" in plain
    assert "actors='myapp.actors:registry'" in plain
    assert "api:" in plain
    assert "health=off" in plain


def test_workgroup_validate_invalid_toml_syntax(tmp_path: Path) -> None:
    """workgroup validate reports 'invalid config' and exits 1 on malformed TOML syntax."""
    config_path = tmp_path / "broken.toml"
    config_path.write_text("this is not [ valid toml")

    result = runner.invoke(app, ["workgroup", "validate", str(config_path)])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "invalid config" in result.stderr


def test_workgroup_validate_schema_violation_raises_value_error(tmp_path: Path) -> None:
    """workgroup validate reports 'invalid config' and exits 1 when validation raises ValueError.

    A config with zero [[workers]] entries fails WorkgroupConfig.from_toml's
    ValueError check ("must define at least one [[workers]] entry").
    """
    config_path = tmp_path / "no-workers.toml"
    config_path.write_text('actors = "myapp.actors:registry"\n')

    result = runner.invoke(app, ["workgroup", "validate", str(config_path)])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "invalid config" in result.stderr
    assert "at least one" in result.stderr


def test_workgroup_validate_os_error(monkeypatch: Any, tmp_path: Path) -> None:
    """workgroup validate reports 'failed to read config' and exits 1 on OSError."""
    config_path = tmp_path / "workgroup.toml"
    config_path.write_text(_MINIMAL_VALID_TOML)

    def fake_load_workgroup_config(path: Path) -> Any:
        raise OSError("perm denied")

    monkeypatch.setattr("taskq.worker.workgroup.load_workgroup_config", fake_load_workgroup_config)

    result = runner.invoke(app, ["workgroup", "validate", str(config_path)])
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "failed to read config" in result.stderr
    assert "perm denied" in result.stderr
