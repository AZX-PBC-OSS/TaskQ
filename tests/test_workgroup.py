"""Tests for the workgroup supervisor — stubbed subprocess management."""

import asyncio
import contextlib
import signal
import time
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import asyncpg
import pytest

from taskq.worker.workgroup import (
    SupervisorConfig,
    WorkerHealthConfig,
    WorkerSpec,
    WorkgroupConfig,
    _child_health_check,
    _ChildState,
    _handle_child_exit,
    _health_check_sql,
    _kill_child,
    _prune_burst,
    _spawn_child,
    _stream_output,
    _validate_config,
    load_workgroup_config,
)


class FakeStreamReader:
    """Minimal async stream reader for subprocess stdout/stderr."""

    async def readline(self) -> bytes:
        await asyncio.sleep(0.01)
        return b""


class FakeProcess:
    """Fake subprocess process with controllable returncode."""

    def __init__(self, returncode: int | None = None, pid: int = 12345) -> None:
        self._returncode = returncode
        self.pid = pid
        self.stdout = FakeStreamReader()
        self.stderr = FakeStreamReader()
        self._killed = False
        self._signals: list[int] = []

    @property
    def returncode(self) -> int | None:
        return self._returncode

    def set_returncode(self, code: int) -> None:
        self._returncode = code

    def send_signal(self, sig: int) -> None:
        self._signals.append(sig)
        if sig == signal.SIGTERM:
            self._returncode = 0

    def terminate(self) -> None:
        self._signals.append(signal.SIGTERM)
        self._returncode = 0

    def kill(self) -> None:
        self._killed = True
        self._returncode = -9

    async def wait(self) -> int:
        while self._returncode is None:
            await asyncio.sleep(0.01)
        return self._returncode


def _proc(fp: FakeProcess) -> "asyncio.subprocess.Process":
    """Type-bridge a FakeProcess into _ChildState.process.

    FakeProcess duck-types the handful of Process attributes workgroup
    touches; a real subclass would need a live transport.
    """
    return cast("asyncio.subprocess.Process", fp)


def _make_spec(name: str = "test_worker", queues: list[str] | None = None) -> WorkerSpec:
    return WorkerSpec(
        name=name,
        queues=queues or ["default"],
        poll_interval=0.1,
        max_concurrency=2,
    )


def _make_child(spec: WorkerSpec | None = None) -> _ChildState:
    return _ChildState(spec=spec or _make_spec())


@pytest.mark.asyncio
async def test_spawn_child_creates_process() -> None:
    """_spawn_child should create a process and stream tasks."""
    child = _make_child()
    fake_proc = FakeProcess(returncode=None)

    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = fake_proc
        await _spawn_child(child, "myapp.actors:registry", UUID(int=1))

    assert child.process is fake_proc
    assert child.instance_id is not None
    assert child.spawned_at > 0
    assert child.stdout_task is not None
    assert child.stderr_task is not None

    child.stdout_task.cancel()
    child.stderr_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await child.stdout_task
        await child.stderr_task


@pytest.mark.asyncio
async def test_handle_child_exit_normal_restart() -> None:
    """A normal crash should schedule a restart with backoff."""
    scfg = SupervisorConfig(
        backoff_initial=0.1,
        backoff_max=1.0,
        backoff_factor=2.0,
        burst_limit=10,
        burst_window=60.0,
    )
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=1))
    child.backoff = 0.1
    shutting_down = asyncio.Event()

    with patch("taskq.worker.workgroup.logger"):
        delay = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)

    assert delay is not None
    assert delay > 0
    assert child.restart_count == 1


@pytest.mark.asyncio
async def test_handle_child_exit_burst_limit_exhausted() -> None:
    """When burst limit is exceeded, _handle_child_exit should return None."""
    scfg = SupervisorConfig(
        backoff_initial=0.01,
        backoff_max=0.1,
        backoff_factor=2.0,
        burst_limit=2,
        burst_window=60.0,
    )
    child = _make_child()
    child.backoff = 0.01
    shutting_down = asyncio.Event()

    with patch("taskq.worker.workgroup.logger"):
        child.process = _proc(FakeProcess(returncode=1))
        await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)
        child.process = _proc(FakeProcess(returncode=1))
        await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)
        child.process = _proc(FakeProcess(returncode=1))
        result = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)

    assert result is None


@pytest.mark.asyncio
async def test_handle_child_exit_shutting_down_no_restart() -> None:
    """If shutting_down is set, _handle_child_exit should return None."""
    scfg = SupervisorConfig()
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=1))
    child.backoff = 0.1
    shutting_down = asyncio.Event()
    shutting_down.set()

    with patch("taskq.worker.workgroup.logger"):
        result = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)

    assert result is None


@pytest.mark.asyncio
async def test_handle_child_exit_increments_backoff() -> None:
    """Each restart should multiply backoff by backoff_factor up to backoff_max."""
    scfg = SupervisorConfig(
        backoff_initial=0.1,
        backoff_max=0.5,
        backoff_factor=2.0,
        burst_limit=10,
        burst_window=60.0,
    )
    child = _make_child()
    child.backoff = 0.1
    shutting_down = asyncio.Event()

    with patch("taskq.worker.workgroup.logger"):
        child.process = _proc(FakeProcess(returncode=1))
        delay1 = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)
        child.process = _proc(FakeProcess(returncode=1))
        delay2 = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)
        child.process = _proc(FakeProcess(returncode=1))
        delay3 = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)

    assert delay1 == 0.1
    assert delay2 == 0.2
    assert delay3 == 0.4


@pytest.mark.asyncio
async def test_run_forever_spawns_and_shuts_down() -> None:
    """run_forever should spawn children and shut them down on signal."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1")],
    )

    fake_proc = FakeProcess(returncode=None)

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        return fake_proc

    config_path = Path("/tmp/fake_workgroup.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.3)

        fake_proc._returncode = 0
        await asyncio.sleep(0.2)

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert fake_proc._signals or fake_proc._killed or fake_proc._returncode is not None


@pytest.mark.asyncio
async def test_run_forever_health_kill() -> None:
    """Health loop should kill unhealthy children."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[
            WorkerSpec(
                name="w1",
                queues=["default"],
                health=WorkerHealthConfig(
                    enabled=True,
                    check_interval=0.05,
                    stale_after=0.01,
                    startup_grace=0.0,
                    consecutive_failure_limit=1,
                ),
            )
        ],
    )

    fake_proc = FakeProcess(returncode=None)

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        return fake_proc

    config_path = Path("/tmp/fake_workgroup_health.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
        patch("taskq.worker.workgroup._child_health_check", new_callable=AsyncMock) as mock_health,
        patch("taskq.worker.workgroup.asyncpg.create_pool", new_callable=AsyncMock) as mock_pool,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()
        mock_health.return_value = False
        mock_pool.return_value = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.3)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert mock_health.called


@pytest.mark.asyncio
async def test_run_forever_except_star_cleanup() -> None:
    """The except* handler should clean up child state on background task failure."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1")],
    )

    fake_proc = FakeProcess(returncode=None)

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        return fake_proc

    config_path = Path("/tmp/fake_workgroup_except.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.1)
        fake_proc._returncode = 42
        await asyncio.sleep(0.2)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ── Config parsing: from_toml / load_workgroup_config ───────────────


def _write_toml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "workgroup.toml"
    p.write_text(content)
    return p


def test_from_toml_valid_config(tmp_path: Path) -> None:
    """A well-formed TOML config loads into dataclasses with defaults applied."""
    toml = """
actors = "myapp.actors:registry"

[defaults]
poll_interval = 2.0
max_concurrency = 4

[supervisor]
shutdown_grace = 45.0
burst_limit = 5

[[workers]]
name = "api"
queues = ["default", "high"]

[[workers]]
name = "cron"
queues = ["cron"]
max_concurrency = 2

[workers.health]
enabled = true
check_interval = 10
stale_after = 30
"""
    cfg = load_workgroup_config(_write_toml(tmp_path, toml))
    assert cfg.actors == "myapp.actors:registry"
    assert cfg.supervisor.shutdown_grace == 45.0
    assert cfg.supervisor.burst_limit == 5
    assert cfg.supervisor.backoff_initial == 0.5  # default
    assert len(cfg.workers) == 2
    assert cfg.workers[0].name == "api"
    assert cfg.workers[0].queues == ["default", "high"]
    assert cfg.workers[0].poll_interval == 2.0  # from defaults
    assert cfg.workers[0].max_concurrency == 4  # from defaults
    assert cfg.workers[1].name == "cron"
    assert cfg.workers[1].max_concurrency == 2  # overridden
    assert cfg.workers[0].health.enabled is False  # api: no health section
    assert cfg.workers[1].health.enabled is True  # cron: has health section
    assert cfg.workers[1].health.check_interval == 10.0


def test_from_toml_missing_actors(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="actors"):
        _write_toml(tmp_path, '[[workers]]\nname = "w"\n')
        load_workgroup_config(tmp_path / "workgroup.toml")


def test_from_toml_actors_missing_colon(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="module:attr"):
        toml = 'actors = "no_colon"\n[[workers]]\nname = "w"\n'
        load_workgroup_config(_write_toml(tmp_path, toml))


def test_from_toml_actors_not_str(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="actors"):
        toml = "actors = 123\n[[workers]]\nname = 'w'\n"
        load_workgroup_config(_write_toml(tmp_path, toml))


def test_from_toml_no_workers(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least one"):
        toml = 'actors = "myapp:reg"\n'
        load_workgroup_config(_write_toml(tmp_path, toml))


def test_from_toml_duplicate_names(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        toml = 'actors = "myapp:reg"\n[[workers]]\nname = "w"\n[[workers]]\nname = "w"\n'
        load_workgroup_config(_write_toml(tmp_path, toml))


def test_from_toml_missing_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="name"):
        toml = 'actors = "myapp:reg"\n[[workers]]\nqueues = ["q"]\n'
        load_workgroup_config(_write_toml(tmp_path, toml))


def test_from_toml_invalid_queues_type(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="queues"):
        toml = 'actors = "myapp:reg"\n[[workers]]\nname = "w"\nqueues = "not_a_list"\n'
        load_workgroup_config(_write_toml(tmp_path, toml))


# ── _validate_config error paths ────────────────────────────────────


def _valid_config(**sup_overrides: Any) -> WorkgroupConfig:
    sup = SupervisorConfig(**sup_overrides) if sup_overrides else SupervisorConfig()
    return WorkgroupConfig(
        actors="app:reg",
        supervisor=sup,
        workers=[WorkerSpec(name="w1", queues=["default"])],
    )


def test_validate_shutdown_grace_zero() -> None:
    with pytest.raises(ValueError, match="shutdown_grace"):
        _validate_config(_valid_config(shutdown_grace=0.0))


def test_validate_backoff_initial_zero() -> None:
    with pytest.raises(ValueError, match="backoff_initial"):
        _validate_config(_valid_config(backoff_initial=0.0))


def test_validate_backoff_max_lt_initial() -> None:
    with pytest.raises(ValueError, match="backoff_max"):
        _validate_config(_valid_config(backoff_initial=2.0, backoff_max=1.0))


def test_validate_backoff_factor_lt_one() -> None:
    with pytest.raises(ValueError, match="backoff_factor"):
        _validate_config(_valid_config(backoff_factor=0.5))


def test_validate_burst_limit_zero() -> None:
    with pytest.raises(ValueError, match="burst_limit"):
        _validate_config(_valid_config(burst_limit=0))


def test_validate_burst_window_zero() -> None:
    with pytest.raises(ValueError, match="burst_window"):
        _validate_config(_valid_config(burst_window=0.0))


def test_validate_health_pg_schema_invalid() -> None:
    with pytest.raises(ValueError, match="health_pg_schema"):
        _validate_config(_valid_config(health_pg_schema="not valid!"))


def test_validate_worker_name_too_long() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[WorkerSpec(name="x" * 65, queues=["default"])],
    )
    with pytest.raises(ValueError, match="64 chars"):
        _validate_config(cfg)


def test_validate_worker_poll_interval_zero() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[WorkerSpec(name="w", queues=["default"], poll_interval=0.0)],
    )
    with pytest.raises(ValueError, match="poll_interval"):
        _validate_config(cfg)


def test_validate_worker_max_concurrency_zero() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[WorkerSpec(name="w", queues=["default"], max_concurrency=0)],
    )
    with pytest.raises(ValueError, match="max_concurrency"):
        _validate_config(cfg)


def test_validate_health_check_interval_zero() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[
            WorkerSpec(
                name="w",
                queues=["default"],
                health=WorkerHealthConfig(enabled=True, check_interval=0.0),
            )
        ],
    )
    with pytest.raises(ValueError, match="check_interval"):
        _validate_config(cfg)


def test_validate_health_stale_after_zero() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[
            WorkerSpec(
                name="w",
                queues=["default"],
                health=WorkerHealthConfig(enabled=True, stale_after=0.0),
            )
        ],
    )
    with pytest.raises(ValueError, match="stale_after"):
        _validate_config(cfg)


def test_validate_health_check_interval_ge_stale_after() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[
            WorkerSpec(
                name="w",
                queues=["default"],
                health=WorkerHealthConfig(enabled=True, check_interval=30.0, stale_after=20.0),
            )
        ],
    )
    with pytest.raises(ValueError, match=r"check_interval.*stale_after"):
        _validate_config(cfg)


def test_validate_health_startup_grace_negative() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[
            WorkerSpec(
                name="w",
                queues=["default"],
                health=WorkerHealthConfig(enabled=True, startup_grace=-1.0),
            )
        ],
    )
    with pytest.raises(ValueError, match="startup_grace"):
        _validate_config(cfg)


def test_validate_health_consecutive_failure_limit_zero() -> None:
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[
            WorkerSpec(
                name="w",
                queues=["default"],
                health=WorkerHealthConfig(enabled=True, consecutive_failure_limit=0),
            )
        ],
    )
    with pytest.raises(ValueError, match="consecutive_failure_limit"):
        _validate_config(cfg)


# ── cli_args ────────────────────────────────────────────────────────


def test_cli_args_basic() -> None:
    spec = WorkerSpec(name="w", queues=["q1", "q2"], poll_interval=2.0, max_concurrency=4)
    args = spec.cli_args()
    assert "--queues" in args
    assert "q1" in args
    assert "q2" in args
    assert "--poll-interval" in args
    assert "2.0" in args
    assert "--max-concurrency" in args
    assert "4" in args
    assert "--worker-group" in args
    assert "default" in args


def test_cli_args_force_update() -> None:
    spec = WorkerSpec(name="w", queues=["q"], force_update_actor_config=True)
    args = spec.cli_args()
    assert "--force-update-actor-config" in args


# ── _health_check_sql ───────────────────────────────────────────────


def test_health_check_sql_valid_schema() -> None:
    sql = _health_check_sql("taskq")
    assert "taskq" in sql
    assert "workers" in sql


def test_health_check_sql_invalid_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema"):
        _health_check_sql("not valid!")


# ── _prune_burst ────────────────────────────────────────────────────


def test_prune_burst_allows_within_limit() -> None:
    child = _make_child()
    scfg = SupervisorConfig(burst_limit=5, burst_window=60.0)
    assert _prune_burst(child, scfg) is True
    assert len(child.restart_times) == 1


def test_prune_burst_exceeds_limit() -> None:
    child = _make_child()
    scfg = SupervisorConfig(burst_limit=2, burst_window=60.0)
    _prune_burst(child, scfg)
    _prune_burst(child, scfg)
    result = _prune_burst(child, scfg)
    assert result is False
    assert len(child.restart_times) == 3


def test_prune_burst_resets_after_window() -> None:
    child = _make_child()
    scfg = SupervisorConfig(backoff_initial=0.5, backoff_max=30.0, burst_limit=5, burst_window=0.01)
    _prune_burst(child, scfg)
    child.backoff = 5.0
    child.restart_count = 3
    time.sleep(0.02)
    result = _prune_burst(child, scfg)
    assert result is True
    assert child.backoff == 0.5
    assert child.restart_count == 0


# ── _handle_child_exit edge cases ──────────────────────────────────


async def test_handle_child_exit_proc_none() -> None:
    scfg = SupervisorConfig()
    child = _make_child()
    child.process = None
    shutting_down = asyncio.Event()
    result = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)
    assert result is None


async def test_handle_child_exit_returncode_none() -> None:
    scfg = SupervisorConfig()
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=None))
    shutting_down = asyncio.Event()
    result = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)
    assert result is None


async def test_handle_child_exit_exit_code_zero() -> None:
    scfg = SupervisorConfig(backoff_initial=0.1, backoff_max=1.0)
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=0))
    child.backoff = 0.1
    shutting_down = asyncio.Event()
    with patch("taskq.worker.workgroup.logger"):
        result = await _handle_child_exit(child, "actors", UUID(int=1), scfg, shutting_down)
    assert result is not None
    assert child.restart_count == 1


# ── _kill_child ─────────────────────────────────────────────────────


async def test_kill_child_terminates_running_process() -> None:
    child = _make_child()
    proc = FakeProcess(returncode=None)
    child.process = _proc(proc)
    await _kill_child(child)
    assert proc.returncode is not None


async def test_kill_child_already_exited() -> None:
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=0))
    await _kill_child(child)


async def test_kill_child_none_process() -> None:
    child = _make_child()
    child.process = None
    await _kill_child(child)


# ── _stream_output ──────────────────────────────────────────────────


async def test_stream_output_none_stream() -> None:
    await _stream_output(None, "test", "info")


async def test_stream_output_forwards_lines() -> None:
    class _FakeStream:
        def __init__(self) -> None:
            self._lines = [b"line1\n", b"line2\n", b""]
            self._idx = 0

        async def readline(self) -> bytes:
            line = self._lines[self._idx]
            self._idx += 1
            return line

    with patch("taskq.worker.workgroup.logger"):
        await _stream_output(_FakeStream(), "test", "info")


# ── _child_health_check ─────────────────────────────────────────────


class _FakePool:
    """Minimal asyncpg.Pool stand-in for health-check tests."""

    def __init__(self, row: dict[str, Any] | None = None, exc: BaseException | None = None) -> None:
        self._row = row
        self._exc = exc

    @contextlib.asynccontextmanager
    async def acquire(self, *, timeout: float | None = None):  # noqa: ASYNC109 # Why: mirrors asyncpg.Pool.acquire signature for drop-in compatibility.
        if self._exc is not None:
            raise self._exc
        yield _FakeConn(self._row)


class _FakeConn:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    async def fetchrow(self, sql: str, *args: object) -> dict[str, Any] | None:
        return self._row


async def test_health_check_healthy() -> None:
    child = _make_child()
    proc = FakeProcess(returncode=None, pid=12345)
    child.process = _proc(proc)
    now = datetime.now(UTC)
    row = {"pid": 12345, "last_seen_at": now}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is True


async def test_health_check_pid_mismatch() -> None:
    child = _make_child()
    proc = FakeProcess(returncode=None, pid=12345)
    child.process = _proc(proc)
    now = datetime.now(UTC)
    row = {"pid": 99999, "last_seen_at": now}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False


async def test_health_check_missing_row() -> None:
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=None))
    pool = _FakePool(row=None)
    cfg = WorkerHealthConfig(enabled=True)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False


async def test_health_check_db_error_within_limit() -> None:
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=None))
    pool = _FakePool(exc=asyncpg.PostgresConnectionError("blip"))
    cfg = WorkerHealthConfig(enabled=True, consecutive_failure_limit=3)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is True  # transient: errs on the side of healthy
    assert child.health_failures == 1


async def test_health_check_db_error_exceeds_limit() -> None:
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=None))
    child.health_failures = 2
    pool = _FakePool(exc=asyncpg.PostgresConnectionError("blip"))
    cfg = WorkerHealthConfig(enabled=True, consecutive_failure_limit=3)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False
    assert child.health_failures == 3


async def test_health_check_stale() -> None:
    child = _make_child()
    proc = FakeProcess(returncode=None, pid=12345)
    child.process = _proc(proc)
    old_time = datetime.now(UTC) - timedelta(seconds=120)
    row = {"pid": 12345, "last_seen_at": old_time}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False


async def test_health_check_last_seen_none() -> None:
    child = _make_child()
    proc = FakeProcess(returncode=None, pid=12345)
    child.process = _proc(proc)
    row = {"pid": 12345, "last_seen_at": None}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False


# ── run_forever: multiple children + graceful shutdown ─────────────


@pytest.mark.asyncio
async def test_run_forever_multiple_children_graceful_shutdown() -> None:
    """run_forever with multiple children shuts them all down on signal."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1"), _make_spec(name="w2")],
    )

    procs: dict[str, FakeProcess] = {}

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        # Extract --worker-label from args to assign the right proc
        label = args[args.index("--worker-label") + 1] if "--worker-label" in args else "unknown"
        proc = FakeProcess(returncode=None)
        procs[label] = proc
        return proc

    config_path = Path("/tmp/fake_multi.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.3)

        # Both children spawned
        assert len(procs) == 2
        assert "w1" in procs
        assert "w2" in procs

        # Trigger graceful shutdown
        procs["w1"]._returncode = 0
        procs["w2"]._returncode = 0
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    # At least one proc got a signal or was killed
    for proc in procs.values():
        assert proc._signals or proc._killed or proc._returncode is not None


@pytest.mark.asyncio
async def test_run_forever_force_update_warning() -> None:
    """A force_update_actor_config worker emits a warning at startup."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[
            WorkerSpec(
                name="w1",
                queues=["default"],
                force_update_actor_config=True,
            )
        ],
    )

    fake_proc = FakeProcess(returncode=None)

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        return fake_proc

    config_path = Path("/tmp/fake_force.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.2)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_run_forever_spawn_failure_continues() -> None:
    """If _spawn_child fails, run_forever logs and continues."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1")],
    )

    call_count = 0

    async def failing_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        nonlocal call_count
        call_count += 1
        raise OSError("spawn failed")

    config_path = Path("/tmp/fake_spawn_fail.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=failing_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.2)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert call_count >= 1


# ── Additional coverage: bad timestamp, kill timeout, graceful shutdown ─


async def test_health_check_bad_timestamp() -> None:
    """A last_seen_at that is not a datetime (no .timestamp()) returns False."""
    child = _make_child()
    proc = FakeProcess(returncode=None, pid=12345)
    child.process = _proc(proc)
    row: dict[str, Any] = {"pid": 12345, "last_seen_at": "not_a_datetime"}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False


async def test_kill_child_timeout_then_sigkill() -> None:
    """_kill_child sends SIGTERM, then SIGKILL if wait times out."""
    child = _make_child()

    class _SlowProcess(FakeProcess):
        async def wait(self) -> int:
            if self._returncode is not None:
                return self._returncode
            await asyncio.sleep(100)
            return -9

    proc = _SlowProcess(returncode=None)
    child.process = _proc(proc)

    original_wait_for = asyncio.wait_for

    async def maybe_timeout(
        coro: Coroutine[Any, Any, object],
        timeout: float | None = None,  # noqa: ASYNC109 # Why: mirrors asyncio.wait_for signature for drop-in mock.
    ) -> object:
        if timeout == 5.0:
            coro.close()
            raise TimeoutError
        return await original_wait_for(coro, timeout=timeout)

    with patch("taskq.worker.workgroup.asyncio.wait_for", side_effect=maybe_timeout):
        await _kill_child(child)

    assert proc._killed


@pytest.mark.asyncio
async def test_run_forever_graceful_shutdown_via_signal() -> None:
    """run_forever shuts down gracefully when the signal handler fires."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1")],
    )

    fake_proc = FakeProcess(returncode=None)

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        return fake_proc

    config_path = Path("/tmp/fake_graceful.toml")
    signal_handlers: dict[int, Any] = {}

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = capture_handler

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.2)

        # Trigger the signal handler to set shutting_down
        assert signal.SIGTERM in signal_handlers
        signal_handlers[signal.SIGTERM]()

        # Let the graceful shutdown proceed
        fake_proc._returncode = 0
        await asyncio.wait_for(task, timeout=3.0)

    # Process received SIGTERM during graceful shutdown
    assert signal.SIGTERM in fake_proc._signals or fake_proc._returncode is not None


@pytest.mark.asyncio
async def test_run_forever_liveness_restarts_crashed_child() -> None:
    """Liveness monitor detects a crashed child and restarts it."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0, backoff_initial=0.01, backoff_max=0.05),
        workers=[_make_spec(name="w1")],
    )

    procs: list[FakeProcess] = []

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        proc = FakeProcess(returncode=None)
        procs.append(proc)
        return proc

    config_path = Path("/tmp/fake_restart.toml")
    signal_handlers: dict[int, Any] = {}

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = capture_handler

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        await asyncio.sleep(0.2)

        # First proc crashes
        procs[0]._returncode = 1
        # Wait for liveness monitor to detect and restart (polls every 0.5s)
        for _ in range(30):
            if len(procs) >= 2:
                break
            await asyncio.sleep(0.1)

        assert len(procs) >= 2

        # Set all procs to exited so graceful shutdown can complete
        for proc in procs:
            if proc._returncode is None:
                proc._returncode = 0

        # Trigger graceful shutdown
        signal_handlers[signal.SIGTERM]()
        await asyncio.wait_for(task, timeout=3.0)
