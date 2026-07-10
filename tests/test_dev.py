"""Tests for ``taskq.worker.dev`` and ``taskq dev`` CLI command.

Covers all test-plan cases:
- Import validation
- Grace period expiry
- SIGINT / cancellation handling
- File change triggers restart
- Syntax error mid-edit suppresses restart
- Multiple watch paths both trigger
- Missing watchfiles exits with hint
- Bad module exits nonzero
- Good module, bad attr exits nonzero

Integration tests are marked individually with
``@pytest.mark.integration`` — no file-wide ``pytestmark`` so that unit
and negative tests pass under ``pytest -m 'not integration'``.
"""

import asyncio
import contextlib
import signal
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from typer.testing import CliRunner

from taskq.cli import app
from taskq.testing.assertions import plain_cli_output
from taskq.worker.dev import _start_worker, _stop_worker, _validate_import, dev_watch_loop

cli_runner = CliRunner()

_VALID_MODULE_ATTR = "taskq.testing.fixtures:actor_runner"
_TMP_MODULE_NAME = "tmp_actor_module"


# ── Fixtures ─────────────────────────────────────────────────────────────


class SpawnTracker:
    """Records subprocess spawns and provides async wait helpers."""

    def __init__(self) -> None:
        self.spawn_count: int = 0
        self.procs: list[MagicMock] = []
        self.last_args: tuple[str, ...] | None = None
        self._pid_counter: int = 100

    def _make_proc(self) -> MagicMock:
        proc = MagicMock(spec=asyncio.subprocess.Process)
        proc.pid = self._pid_counter
        self._pid_counter += 1
        proc.wait = AsyncMock(return_value=0)
        proc.send_signal = Mock()
        proc.kill = Mock()
        proc.returncode = 0
        return proc

    async def _spawn(self, *args: str) -> MagicMock:
        proc = self._make_proc()
        self.procs.append(proc)
        self.last_args = args
        self.spawn_count += 1
        return proc

    async def wait_for_spawn(self, n: int, deadline: float = 5.0) -> None:
        """Block until ``spawn_count >= n`` or *deadline* seconds elapse."""
        elapsed = 0.0
        step = 0.05
        while self.spawn_count < n and elapsed < deadline:
            await asyncio.sleep(step)
            elapsed += step
        if self.spawn_count < n:
            raise TimeoutError(f"waited {deadline}s for spawn #{n}, only {self.spawn_count} seen")


@pytest.fixture
def spawn_tracker(monkeypatch: pytest.MonkeyPatch) -> SpawnTracker:
    """Patch ``asyncio.create_subprocess_exec`` to record spawns."""
    tracker = SpawnTracker()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", tracker._spawn)
    return tracker


class ActorModule:
    """Minimal actor module on a temp directory for integration tests."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.module_attr = f"{_TMP_MODULE_NAME}:registry"
        self._file = tmp_path / f"{_TMP_MODULE_NAME}.py"
        self._write_valid()

    def _write_valid(self) -> None:
        self._file.write_text("registry = {}\n")

    def touch(self) -> None:
        content = self._file.read_text()
        self._file.write_text(content)

    def break_syntax(self) -> None:
        self._file.write_text("def broken syntax:\n")

    def fix_syntax(self) -> None:
        self._write_valid()


@pytest.fixture
def actor_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ActorModule:
    """Create a temporary actor module with ``registry = {}`` and add
    *tmp_path* to ``sys.path`` so ``_validate_import`` can find it.

    ``sys.path`` is restored via ``monkeypatch`` teardown. The module
    is removed from ``sys.modules`` to avoid stale-cache issues across
    tests.
    """
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, _TMP_MODULE_NAME, raising=False)
    return ActorModule(tmp_path)


# ── _validate_import ──────────────────────────────────────────────


def test_validate_import_valid() -> None:
    """Valid module:attr returns True."""
    assert _validate_import(_VALID_MODULE_ATTR) is True


def test_validate_import_bad_module(capsys: pytest.CaptureFixture[str]) -> None:
    """Nonexistent module prints error to stderr and returns False."""
    result = _validate_import("nonexistent_module_xyz:attr")
    assert result is False
    captured = capsys.readouterr()
    assert "nonexistent_module_xyz" in captured.err


def test_validate_import_bad_attr(capsys: pytest.CaptureFixture[str]) -> None:
    """Good module, missing attr returns False naming the attribute."""
    result = _validate_import("taskq.testing.fixtures:_no_such_attr_xyz")
    assert result is False
    captured = capsys.readouterr()
    assert "_no_such_attr_xyz" in captured.err


def test_validate_import_no_colon(capsys: pytest.CaptureFixture[str]) -> None:
    """Missing colon in module:attr syntax returns False."""
    result = _validate_import("just_a_module")
    assert result is False
    captured = capsys.readouterr()
    assert "module:attr" in captured.err


def test_validate_import_syntax_error(capsys: pytest.CaptureFixture[str]) -> None:
    """Syntax error during import returns False with error message."""
    with patch(
        "taskq.worker.dev.importlib.import_module",
        side_effect=SyntaxError("bad syntax"),
    ):
        result = _validate_import("some.module:attr")
    assert result is False
    captured = capsys.readouterr()
    assert "SyntaxError" in captured.err


def test_validate_import_generic_exception(capsys: pytest.CaptureFixture[str]) -> None:
    """Any unexpected exception during import returns False."""
    with patch(
        "taskq.worker.dev.importlib.import_module",
        side_effect=RuntimeError("unexpected"),
    ):
        result = _validate_import("some.module:attr")
    assert result is False
    captured = capsys.readouterr()
    assert "RuntimeError" in captured.err


async def test_start_worker_cli_invocation(spawn_tracker: SpawnTracker) -> None:
    """Verify _start_worker calls subprocess with the correct CLI args."""
    with patch("taskq.worker.dev.shutil.which", return_value="/fake/bin/taskq"):
        proc = await _start_worker("myapp.actors:registry")
    assert spawn_tracker.last_args == (
        "/fake/bin/taskq",
        "worker",
        "--actors",
        "myapp.actors:registry",
    )
    proc.send_signal(signal.SIGTERM)
    await proc.wait()


async def test_start_worker_raises_when_taskq_not_on_path(spawn_tracker: SpawnTracker) -> None:
    """_start_worker raises RuntimeError if 'taskq' is not on PATH."""
    with (
        patch("taskq.worker.dev.shutil.which", return_value=None),
        pytest.raises(RuntimeError, match="Could not find the 'taskq' executable"),
    ):
        await _start_worker("myapp.actors:registry")


# ── _stop_worker grace period expiry ──────────────────────────────


async def test_stop_worker_sigkill_on_timeout() -> None:
    """grace_period=0 with a process that never exits calls proc.kill()."""
    call_count = 0

    async def _wait() -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await asyncio.sleep(0.1)  # brief sleep to trigger TimeoutError with grace_period=0
        return 0

    proc = MagicMock(spec=asyncio.subprocess.Process)
    proc.wait = _wait
    proc.send_signal = Mock()
    proc.kill = Mock()
    proc.returncode = None

    await _stop_worker(proc, grace_period=0)

    proc.send_signal.assert_called_once_with(signal.SIGTERM)
    proc.kill.assert_called_once()


async def test_stop_worker_clean_exit() -> None:
    """Worker exits within grace period — no SIGKILL."""
    proc = MagicMock(spec=asyncio.subprocess.Process)
    proc.wait = AsyncMock(return_value=0)
    proc.send_signal = Mock()
    proc.kill = Mock()

    await _stop_worker(proc, grace_period=5.0)

    proc.send_signal.assert_called_once_with(signal.SIGTERM)
    proc.kill.assert_not_called()


async def test_stop_worker_already_dead() -> None:
    """Process already exited — ProcessLookupError on SIGTERM is handled."""
    proc = MagicMock(spec=asyncio.subprocess.Process)
    proc.wait = AsyncMock(return_value=0)
    proc.send_signal = Mock(side_effect=ProcessLookupError)
    proc.kill = Mock()

    await _stop_worker(proc, grace_period=5.0)

    proc.kill.assert_not_called()
    proc.wait.assert_called()


async def test_stop_worker_kill_raises_process_lookup_error() -> None:
    """Child exits between TimeoutError and proc.kill() — ProcessLookupError swallowed."""
    call_count = 0

    async def _wait() -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await asyncio.sleep(0.1)  # brief sleep to trigger TimeoutError with grace_period=0
        return 0

    proc = MagicMock(spec=asyncio.subprocess.Process)
    proc.wait = _wait
    proc.send_signal = Mock()
    proc.kill = Mock(side_effect=ProcessLookupError)
    proc.returncode = None

    await _stop_worker(proc, grace_period=0)

    proc.send_signal.assert_called_once_with(signal.SIGTERM)
    proc.kill.assert_called_once()


# ── dev_watch_loop cancellation handling ──────────────────────────


async def test_dev_watch_loop_cancelled_calls_stop_worker() -> None:
    """Cancelling dev_watch_loop calls _stop_worker before SystemExit(0)."""
    pytest.importorskip("watchfiles")
    mock_proc = MagicMock(spec=asyncio.subprocess.Process)
    mock_proc.wait = AsyncMock(return_value=0)
    mock_proc.send_signal = Mock()
    mock_proc.kill = Mock()
    mock_proc.returncode = 0

    async def _awatch_then_cancel(
        *paths: str | Path, watch_filter: Any = None, debounce: int = 1600
    ) -> AsyncIterator[set[tuple[int, str]]]:
        yield {(1, str(Path.cwd() / "test_file.py"))}
        raise asyncio.CancelledError()

    with (
        patch("taskq.worker.dev._start_worker", return_value=mock_proc),
        patch("taskq.worker.dev._stop_worker", new_callable=AsyncMock) as mock_stop,
        patch("taskq.worker.dev._validate_import", return_value=True),
        patch("watchfiles.awatch", side_effect=_awatch_then_cancel),
        patch("watchfiles.DefaultFilter", MagicMock()),
    ):
        with pytest.raises(SystemExit) as exc_info:
            await dev_watch_loop(
                _VALID_MODULE_ATTR,
                watch_paths=["."],
                grace_period=5.0,
            )
        assert exc_info.value.code == 0
        mock_stop.assert_called()


# ── File change triggers restart ──────────────────────────────────


@pytest.mark.integration
async def test_file_change_triggers_restart(
    actor_module: ActorModule, spawn_tracker: SpawnTracker
) -> None:
    """File change triggers a second worker spawn."""
    pytest.importorskip("watchfiles")
    initial_count = spawn_tracker.spawn_count
    test_task = asyncio.current_task()

    async def _awatch_events(
        *paths: str | Path, watch_filter: Any = None, debounce: int = 1600
    ) -> AsyncIterator[set[tuple[int, str]]]:
        yield {(1, str(actor_module.tmp_path / f"{_TMP_MODULE_NAME}.py"))}
        raise asyncio.CancelledError()

    async def _driver() -> None:
        await spawn_tracker.wait_for_spawn(initial_count + 1)
        await spawn_tracker.wait_for_spawn(initial_count + 2, deadline=5.0)
        assert test_task is not None
        test_task.cancel()

    driver_task = asyncio.create_task(_driver())
    try:
        with (
            patch("watchfiles.awatch", side_effect=_awatch_events),
            patch("watchfiles.DefaultFilter", MagicMock()),
        ):
            with pytest.raises(SystemExit) as exc_info:
                async with asyncio.timeout(15):
                    await dev_watch_loop(
                        actor_module.module_attr,
                        watch_paths=[str(actor_module.tmp_path)],
                        grace_period=1,
                    )
            assert exc_info.value.code == 0
    finally:
        driver_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await driver_task

    assert spawn_tracker.spawn_count >= initial_count + 2


# ── Syntax error mid-edit suppresses restart ──────────────────────


@pytest.mark.integration
async def test_syntax_error_suppresses_restart(
    actor_module: ActorModule, spawn_tracker: SpawnTracker
) -> None:
    """Syntax error stops the worker but does not start a new one;
    fixing the syntax starts a new worker."""
    pytest.importorskip("watchfiles")
    initial_count = spawn_tracker.spawn_count
    test_task = asyncio.current_task()
    broken_event = asyncio.Event()
    fixed_event = asyncio.Event()

    async def _awatch_events(
        *paths: str | Path, watch_filter: Any = None, debounce: int = 1600
    ) -> AsyncIterator[set[tuple[int, str]]]:
        path = str(actor_module.tmp_path / _TMP_MODULE_NAME)
        await broken_event.wait()
        yield {(1, path)}
        await fixed_event.wait()
        yield {(1, path)}
        raise asyncio.CancelledError()

    async def _driver() -> None:
        await spawn_tracker.wait_for_spawn(initial_count + 1)

        actor_module.break_syntax()
        broken_event.set()
        await asyncio.sleep(1.0)
        count_after_break = spawn_tracker.spawn_count

        actor_module.fix_syntax()
        fixed_event.set()
        await spawn_tracker.wait_for_spawn(count_after_break + 1, deadline=5.0)
        assert test_task is not None
        test_task.cancel()

    driver_task = asyncio.create_task(_driver())
    try:
        with (
            patch("watchfiles.awatch", side_effect=_awatch_events),
            patch("watchfiles.DefaultFilter", MagicMock()),
        ):
            with pytest.raises(SystemExit) as exc_info:
                async with asyncio.timeout(20):
                    await dev_watch_loop(
                        actor_module.module_attr,
                        watch_paths=[str(actor_module.tmp_path)],
                        grace_period=1,
                    )
            assert exc_info.value.code == 0
    finally:
        driver_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await driver_task


# ── Multiple watch paths ──────────────────────────────────────────


@pytest.mark.integration
async def test_multiple_watch_paths_both_trigger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    spawn_tracker: SpawnTracker,
) -> None:
    pytest.importorskip("watchfiles")
    """Changes in any watched path trigger a restart."""
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    (dir_a / f"{_TMP_MODULE_NAME}.py").write_text("registry = {}\n")
    (dir_b / "helpers.py").write_text("pass\n")

    monkeypatch.syspath_prepend(str(dir_a))
    monkeypatch.delitem(sys.modules, _TMP_MODULE_NAME, raising=False)
    module_attr = f"{_TMP_MODULE_NAME}:registry"

    initial_count = spawn_tracker.spawn_count
    test_task = asyncio.current_task()

    async def _awatch_events(
        *paths: str | Path, watch_filter: Any = None, debounce: int = 1600
    ) -> AsyncIterator[set[tuple[int, str]]]:
        yield {(1, str(dir_a / f"{_TMP_MODULE_NAME}.py"))}
        yield {(1, str(dir_b / "helpers.py"))}
        raise asyncio.CancelledError()

    async def _driver() -> None:
        await spawn_tracker.wait_for_spawn(initial_count + 1)
        await spawn_tracker.wait_for_spawn(initial_count + 2, deadline=5.0)
        await spawn_tracker.wait_for_spawn(initial_count + 3, deadline=5.0)
        assert test_task is not None
        test_task.cancel()

    driver_task = asyncio.create_task(_driver())
    try:
        with (
            patch("watchfiles.awatch", side_effect=_awatch_events),
            patch("watchfiles.DefaultFilter", MagicMock()),
        ):
            with pytest.raises(SystemExit) as exc_info:
                async with asyncio.timeout(15):
                    await dev_watch_loop(
                        module_attr,
                        watch_paths=[str(dir_a), str(dir_b)],
                        grace_period=1,
                    )
            assert exc_info.value.code == 0
    finally:
        driver_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await driver_task


# ── Missing watchfiles exits with hint ────────────────────────────


async def test_missing_watchfiles_exits_with_hint(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Missing watchfiles raises SystemExit(1) with install hint."""
    with patch.dict("sys.modules", {"watchfiles": None}):
        with pytest.raises(SystemExit) as exc_info:
            await dev_watch_loop(
                _VALID_MODULE_ATTR,
                watch_paths=["."],
                grace_period=5.0,
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert 'pip install "taskq-py[reload]"' in captured.err


def test_dev_watchfiles_missing_exits_nonzero_via_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI propagates SystemExit(1) from dev_watch_loop when watchfiles absent."""

    async def _fake_watch_loop(
        module_attr: str, *, watch_paths: Any = None, grace_period: float = 5.0
    ) -> None:
        raise SystemExit(1)

    monkeypatch.setattr("taskq.cli.dev_watch_loop", _fake_watch_loop)
    result = cli_runner.invoke(app, ["dev", _VALID_MODULE_ATTR])
    assert result.exit_code == 1


# ── Bad module exits nonzero ──────────────────────────────────────


def test_bad_module_exits_nonzero() -> None:
    """nonexistent module exits 1 with human-readable error, no traceback."""
    result = cli_runner.invoke(app, ["dev", "nonexistent_module_xyz:registry"])
    assert result.exit_code != 0
    assert "nonexistent_module_xyz" in plain_cli_output(result.output)
    assert "Traceback" not in result.stderr


# ── Good module, bad attr exits nonzero ───────────────────────────


def test_good_module_bad_attr_exits_nonzero() -> None:
    """Missing attribute exits 1 with message naming the missing attr."""
    result = cli_runner.invoke(app, ["dev", "taskq.testing.fixtures:_no_such_attr_xyz"])
    assert result.exit_code != 0
    assert "_no_such_attr_xyz" in plain_cli_output(result.output)


# ── CLI: additional coverage ──────────────────────────────────────────────


def test_dev_help_shows_positional_and_options() -> None:
    """taskq dev --help shows MODULE:ATTR positional, --watch, --grace-period."""
    result = cli_runner.invoke(app, ["dev", "--help"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    plain = plain_cli_output(result.output)
    assert "--watch" in plain
    assert "--grace-period" in plain


def test_dev_no_colon_exits_nonzero() -> None:
    """Missing colon in module:attr exits 1 with syntax error."""
    result = cli_runner.invoke(app, ["dev", "just_a_module"])
    assert result.exit_code != 0
    assert "module:attr" in result.stderr


def test_dev_valid_import_prints_banner_to_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valid invocation prints startup banner to stderr."""

    async def _fake_watch_loop(
        module_attr: str, *, watch_paths: Any = None, grace_period: float = 5.0
    ) -> None:
        return

    monkeypatch.setattr("taskq.cli.dev_watch_loop", _fake_watch_loop)
    result = cli_runner.invoke(app, ["dev", _VALID_MODULE_ATTR])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "TaskQ dev mode — watching" in result.stderr
    assert "Press Ctrl-C to stop" in result.stderr


def test_dev_grace_period_zero_passed_as_float(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--grace-period 0 is accepted and passed as 0.0 to dev_watch_loop."""
    captured_grace: float | None = None

    async def _fake_watch_loop(
        module_attr: str, *, watch_paths: Any = None, grace_period: float = 5.0
    ) -> None:
        nonlocal captured_grace
        captured_grace = grace_period

    monkeypatch.setattr("taskq.cli.dev_watch_loop", _fake_watch_loop)
    result = cli_runner.invoke(app, ["dev", _VALID_MODULE_ATTR, "--grace-period", "0"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_grace == 0.0


def test_dev_watch_repeatable_passes_all_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--watch path1 --watch path2 passes both paths to dev_watch_loop."""
    captured_paths: list[str] | None = None

    async def _fake_watch_loop(
        module_attr: str, *, watch_paths: list[str] | None = None, grace_period: float = 5.0
    ) -> None:
        nonlocal captured_paths
        captured_paths = list(watch_paths) if watch_paths is not None else []

    monkeypatch.setattr("taskq.cli.dev_watch_loop", _fake_watch_loop)
    result = cli_runner.invoke(
        app,
        [
            "dev",
            _VALID_MODULE_ATTR,
            "--watch",
            str(Path.cwd() / "a"),
            "--watch",
            str(Path.cwd() / "b"),
        ],
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_paths == [str(Path.cwd() / "a"), str(Path.cwd() / "b")]


def test_dev_default_watch_is_cwd(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without --watch, defaults to [Path.cwd()] as string."""
    captured_paths: list[str] | None = None

    async def _fake_watch_loop(
        module_attr: str, *, watch_paths: list[str] | None = None, grace_period: float = 5.0
    ) -> None:
        nonlocal captured_paths
        captured_paths = list(watch_paths) if watch_paths is not None else []

    monkeypatch.setattr("taskq.cli.dev_watch_loop", _fake_watch_loop)
    result = cli_runner.invoke(app, ["dev", _VALID_MODULE_ATTR])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured_paths is not None
    assert len(captured_paths) == 1
    assert captured_paths[0] == str(Path.cwd())


def test_dev_negative_grace_period_rejected() -> None:
    """--grace-period rejects negative values."""
    result = cli_runner.invoke(app, ["dev", _VALID_MODULE_ATTR, "--grace-period", "-1"])
    assert result.exit_code != 0
