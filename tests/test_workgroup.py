"""Tests for the workgroup supervisor - stubbed subprocess management."""

import asyncio
import contextlib
import signal
import sys
import textwrap
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import asyncpg
import pytest
import structlog.testing

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
    """Minimal async stream reader for subprocess stdout/stderr.

    Implements ``readuntil`` (the member production ``_read_line``
    calls), raising the EOF signal immediately so a pump over this fake
    exits cleanly instead of dying on a missing attribute - a fake that
    silently violates the reader contract turns every pump into the
    failure branch and drowns real pump-failure signals in noise.
    """

    async def readline(self) -> bytes:
        await asyncio.sleep(0.01)
        return b""

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        raise asyncio.IncompleteReadError(b"", None)


class FakeProcess:
    """Fake subprocess process with controllable returncode.

    ``signalled`` (optional) is set at the exact points a signal is
    delivered to the child (send_signal/terminate/kill) - the canonical
    wait surface for tests that need "the supervisor signalled this
    child" instead of sleeping a fixed interval that races the
    supervisor's async kill path under load.
    """

    def __init__(
        self,
        returncode: int | None = None,
        pid: int = 12345,
        *,
        signalled: asyncio.Event | None = None,
    ) -> None:
        self._returncode = returncode
        self.pid = pid
        self.stdout = FakeStreamReader()
        self.stderr = FakeStreamReader()
        self._killed = False
        self._signals: list[int] = []
        self.signalled = signalled

    @property
    def returncode(self) -> int | None:
        return self._returncode

    def set_returncode(self, code: int) -> None:
        self._returncode = code

    def send_signal(self, sig: int) -> None:
        self._signals.append(sig)
        if self.signalled is not None:
            self.signalled.set()
        if sig == signal.SIGTERM:
            self._returncode = 0

    def terminate(self) -> None:
        self._signals.append(signal.SIGTERM)
        self._returncode = 0
        if self.signalled is not None:
            self.signalled.set()

    def kill(self) -> None:
        self._killed = True
        self._returncode = -9
        if self.signalled is not None:
            self.signalled.set()

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
    """run_forever spawns its children and the supervisor task tears down
    cleanly on cancellation.

    The spawn wait is event-driven: the create_subprocess_exec double
    signals at the observable's flip point, so the wait resolves exactly
    when the child exists - a fixed window would race startup under
    load. Child-exit detection and graceful-shutdown-on-signal coverage
    lives in test_run_forever_liveness_restarts_crashed_child and
    test_run_forever_graceful_shutdown_via_signal below.
    """
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1")],
    )

    fake_proc = FakeProcess(returncode=None)
    spawned = asyncio.Event()

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        spawned.set()
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
        try:
            await asyncio.wait_for(spawned.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("run_forever did not spawn its child within 5.0s")

        # Bounded teardown: a supervisor that wedges on cancellation must
        # fail the test by name, not hang it.
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.CancelledError:
            pass  # Why: the cancellation the test itself requested - the clean outcome.
        except TimeoutError:
            pytest.fail("run_forever did not tear down within 5.0s of cancellation")

    # The two pins, stated at the end: the child was spawned, and the
    # supervisor resolved the cancellation without raising anything.
    assert spawned.is_set(), "run_forever never spawned its child"
    assert task.done() and (task.cancelled() or task.exception() is None), (
        "run_forever did not shut down cleanly on cancellation"
    )


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

    killed = asyncio.Event()
    fake_proc = FakeProcess(returncode=None, signalled=killed)

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
        # Event-driven wait for the kill: FakeProcess.signalled is set at
        # the exact point the health loop's _kill_child delivers the
        # signal - a fixed sleep plus a "mock_health.called" check races
        # the kill under load (called only proves the check ran, not
        # that the child was killed).
        try:
            await asyncio.wait_for(killed.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("the health loop did not kill the unhealthy child within 5.0s")
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert mock_health.called
        assert signal.SIGTERM in fake_proc._signals


@pytest.mark.asyncio
async def test_run_forever_except_star_cleanup() -> None:
    """The except* handler should clean up child state on background task failure.

    A liveness-monitor failure (patched _handle_child_exit raising) tears
    down the supervisor's TaskGroup; run_forever must log
    workgroup-background-task-failed, reset child state, and still
    complete its graceful shutdown instead of propagating. The failure
    is injected through _handle_child_exit because a plain child exit
    is handled by liveness_monitor and never reaches except*.
    """
    import structlog

    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1")],
    )

    fake_proc = FakeProcess(returncode=None)
    spawned = asyncio.Event()

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        spawned.set()
        return fake_proc

    exit_processed = asyncio.Event()

    async def exploding_handle_child_exit(
        child: _ChildState,
        actors: str,
        wg_instance: UUID,
        scfg: SupervisorConfig,
        shutting_down: asyncio.Event,
    ) -> float | None:
        exit_processed.set()
        raise RuntimeError("simulated liveness-monitor failure")

    config_path = Path("/tmp/fake_except.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
        patch("taskq.worker.workgroup._handle_child_exit", side_effect=exploding_handle_child_exit),
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        with structlog.testing.capture_logs() as captured:
            task = asyncio.create_task(run_forever(config_path))
            try:
                await asyncio.wait_for(spawned.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("run_forever did not spawn its child within 5.0s")

            # Child exits - the patched handler turns liveness_monitor's
            # exit processing into the background failure under test.
            fake_proc._returncode = 42
            try:
                await asyncio.wait_for(exit_processed.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("the liveness monitor did not process the child exit within 5.0s")
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except TimeoutError:
                pytest.fail(
                    "run_forever did not complete its except* cleanup and graceful "
                    "shutdown within 5.0s"
                )

        failures = [e for e in captured if e["event"] == "workgroup-background-task-failed"]
        assert failures, "the background task failure was not logged by the except* handler"
        assert failures[0]["error"] == "simulated liveness-monitor failure"


# ── Config parsing: from_toml / load_workgroup_config ───────────────


def _write_toml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "workgroup.toml"
    p.write_text(content)
    return p


def test_from_toml_valid_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A well-formed TOML config loads into dataclasses with defaults applied."""
    monkeypatch.chdir(tmp_path)
    _write_actors_module(
        tmp_path,
        "wg_actors_valid_config",
        """
        from pydantic import BaseModel
        from taskq.actor import actor

        class Payload(BaseModel):
            pass

        @actor(queue="default")
        async def default_job(payload: Payload) -> None:
            pass

        @actor(queue="high")
        async def high_job(payload: Payload) -> None:
            pass

        @actor(queue="cron")
        async def cron_job(payload: Payload) -> None:
            pass

        registry = {"default_job": default_job, "high_job": high_job, "cron_job": cron_job}
        """.strip("\n"),
    )
    toml = """
actors = "wg_actors_valid_config:registry"

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
    assert cfg.actors == "wg_actors_valid_config:registry"
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
    # 43 is the longest name whose health socket path binds on every
    # supported platform: 103 usable sun_path chars (macOS budget, incl.
    # NUL) minus the path's 60 fixed chars of prefix/uuid/separators -
    # one char past it is the first name the child cannot bind anywhere.
    cfg = WorkgroupConfig(
        actors="app:reg",
        workers=[WorkerSpec(name="x" * 44, queues=["default"])],
    )
    with pytest.raises(ValueError, match="socket"):
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


def _reader(payload: bytes, limit: int = 64) -> asyncio.StreamReader:
    stream = asyncio.StreamReader(limit=limit)
    stream.feed_data(payload)
    stream.feed_eof()
    return stream


async def test_stream_output_forwards_lines() -> None:
    with patch("taskq.worker.workgroup.logger"):
        await _stream_output(_reader(b"line1\nline2\n"), "test", "info")


async def test_stream_output_survives_an_overlong_line() -> None:
    """One 1 MiB JSON log line must not kill the child's output forever.

    ``StreamReader.readline()`` raises ValueError past its limit; uncaught,
    the streaming task dies and every subsequent stdout/stderr line from
    that child is lost for the process lifetime.
    """
    import structlog

    payload = b"x" * 300 + b"\n" + b"after-the-monster\n"
    with structlog.testing.capture_logs() as captured:
        await _stream_output(_reader(payload), "noisy", "info")

    lines = [e["line"] for e in captured if e["event"] == "workgroup.child_output"]
    assert "after-the-monster" in lines, f"stream died after the overlong line: {captured!r}"
    assert any(e.get("truncated") for e in captured), "truncation was not reported"


async def test_stream_output_keeps_reading_a_real_child_after_a_huge_line() -> None:
    """End-to-end against a real subprocess pipe, not a hand-fed reader."""
    import structlog

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import sys; print('before'); print('y' * 200_000); print('after'); sys.stdout.flush()",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        limit=4096,
    )
    with structlog.testing.capture_logs() as captured:
        await _stream_output(proc.stdout, "child", "info")
    await proc.wait()

    lines = [e["line"] for e in captured if e["event"] == "workgroup.child_output"]
    assert "before" in lines
    assert "after" in lines, f"stream stopped after the huge line: {lines!r}"


def test_worker_spec_stream_limit_from_toml_and_validated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_actors_module(tmp_path, "wg_actors_stream_limit", "registry = {}")
    base = (
        'actors = "wg_actors_stream_limit:registry"\n'
        '[[workers]]\nname = "w"\nqueues = ["default"]\nstream_limit = {}\n'
    )
    path = tmp_path / "wg.toml"
    path.write_text(base.format(65536))
    assert load_workgroup_config(path).workers[0].stream_limit == 65536

    path.write_text(base.format(0))
    with pytest.raises(ValueError, match="stream_limit must be > 0"):
        load_workgroup_config(path)


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
    row = {"pid": 12345, "fresh": True, "age_s": 5.0}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is True


async def test_health_check_pid_mismatch() -> None:
    child = _make_child()
    proc = FakeProcess(returncode=None, pid=12345)
    child.process = _proc(proc)
    row = {"pid": 99999, "fresh": True, "age_s": 5.0}
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
    row = {"pid": 12345, "fresh": False, "age_s": 120.0}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False


async def test_health_check_last_seen_none() -> None:
    """last_seen_at IS NULL (never registered a beat) - the server-side
    freshness expression is NULL, so the child is unhealthy."""
    child = _make_child()
    proc = FakeProcess(returncode=None, pid=12345)
    child.process = _proc(proc)
    row = {"pid": 12345, "fresh": None, "age_s": None}
    pool = _FakePool(row=row)
    cfg = WorkerHealthConfig(enabled=True, stale_after=60.0)
    result = await _child_health_check(child, pool, "taskq", cfg, UUID(int=1))
    assert result is False


# ── run_forever: multiple children + graceful shutdown ─────────────


@pytest.mark.asyncio
async def test_run_forever_multiple_children_graceful_shutdown() -> None:
    """run_forever with multiple children forwards SIGTERM to every child
    on shutdown and completes without force-kills."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1"), _make_spec(name="w2")],
    )

    procs: dict[str, FakeProcess] = {}
    both_spawned = asyncio.Event()

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        # Extract --worker-label from args to assign the right proc
        label = args[args.index("--worker-label") + 1] if "--worker-label" in args else "unknown"
        proc = FakeProcess(returncode=None)
        procs[label] = proc
        if len(procs) == 2:
            both_spawned.set()
        return proc

    config_path = Path("/tmp/fake_multi.toml")
    signal_handlers: dict[int, Any] = {}
    sigterm_registered = asyncio.Event()

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler
        if sig == signal.SIGTERM:
            sigterm_registered.set()

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = capture_handler

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        # Event-driven waits on the spawn double and the handler-capture
        # double - a fixed window races startup under load (fewer than
        # both children spawned, handler missing).
        try:
            await asyncio.wait_for(both_spawned.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("run_forever did not spawn both children within 5.0s")
        assert set(procs) == {"w1", "w2"}
        try:
            await asyncio.wait_for(sigterm_registered.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("run_forever did not register its SIGTERM handler within 5.0s")

        # Trigger graceful shutdown. The children are alive (returncode
        # None), so the supervisor must forward SIGTERM to each - the
        # forwarded signal is what stops them (FakeProcess sets its
        # returncode on SIGTERM). No manual returncode poking: setting
        # it would suppress the very forwarding the final assert
        # verifies.
        signal_handlers[signal.SIGTERM]()
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except TimeoutError:
            pytest.fail("run_forever did not complete graceful shutdown within 3.0s")

    # Every child received SIGTERM during graceful shutdown - the real
    # "shuts them all down" assertion.
    for label, proc in procs.items():
        assert signal.SIGTERM in proc._signals, f"child {label!r} did not receive SIGTERM"


@pytest.mark.asyncio
async def test_run_forever_force_update_warning() -> None:
    """A force_update_actor_config worker emits a warning at startup."""
    import structlog

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
    spawned = asyncio.Event()

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        spawned.set()
        return fake_proc

    config_path = Path("/tmp/fake_force.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        with structlog.testing.capture_logs() as captured:
            task = asyncio.create_task(run_forever(config_path))
            # The warning is emitted before the spawn loop, so the spawn
            # event implies it has been logged - no fixed sleep racing
            # startup.
            try:
                await asyncio.wait_for(spawned.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("run_forever did not spawn its child within 5.0s")
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    warnings = [e for e in captured if e["event"] == "workgroup.force_update_actor_config_enabled"]
    assert warnings, "the force_update_actor_config warning was not emitted at startup"


@pytest.mark.asyncio
async def test_run_forever_spawn_failure_continues() -> None:
    """If _spawn_child fails, run_forever logs and continues."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0),
        workers=[_make_spec(name="w1")],
    )

    call_count = 0
    first_attempt = asyncio.Event()

    async def failing_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        nonlocal call_count
        call_count += 1
        first_attempt.set()
        raise OSError("spawn failed")

    config_path = Path("/tmp/fake_spawn_fail.toml")
    signal_handlers: dict[int, Any] = {}
    sigterm_registered = asyncio.Event()

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler
        if sig == signal.SIGTERM:
            sigterm_registered.set()

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=failing_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = capture_handler

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        # Event-driven waits on the spawn double and the handler-capture
        # double - a fixed window races startup under load (fewer than
        # one spawn attempt, handler missing) and fails the completion
        # assert below for a reason that has nothing to do with the
        # continue-on-failure behaviour under test.
        try:
            await asyncio.wait_for(first_attempt.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("run_forever did not attempt its first spawn within 5.0s")
        # The signal handlers are installed AFTER the initial spawn loop,
        # so the spawn attempt alone does not yet prove the supervisor is
        # stoppable - wait for the registration the stop below drives.
        try:
            await asyncio.wait_for(sigterm_registered.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("run_forever did not register its SIGTERM handler within 5.0s")

        # Stop the supervisor the way an operator does: SIGTERM, then let
        # run_forever complete its own graceful shutdown - the same stop
        # shape test_run_forever_multiple_children_graceful_shutdown
        # drives. A bare task.cancel() aborts the supervisor mid-
        # _delay_then_respawn, whose asyncio.wait() does not cancel its
        # inner futures when the caller itself is cancelled: the backoff
        # sleep and the shutdown Event.wait went on running WITHOUT the
        # supervisor, two tasks orphaned on the module loop. The signal
        # path is clean in every interleaving - shutting_down set either
        # skips the restart arm under the restart lock, or wins the
        # _delay_then_respawn race and cancels-and-awaits its own sleep
        # before the monitor's loop condition exits.
        signal_handlers[signal.SIGTERM]()
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except TimeoutError:
            pytest.fail("run_forever did not complete graceful shutdown within 3.0s")

    assert call_count >= 1


async def test_spawn_failed_child_is_retried_by_liveness_monitor() -> None:
    """A child whose initial spawn failed must not stay dead.

    The liveness monitor retries never-spawned children under the same
    burst/backoff budget as exited ones - the worker's command line
    failing once (image pull retry, transient exec failure) must not
    mean the worker is absent until the whole supervisor restarts.
    """
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0, backoff_initial=0.01, backoff_max=0.02),
        workers=[_make_spec(name="w1")],
    )

    call_count = 0
    second_attempt = asyncio.Event()

    async def fail_then_succeed(*args: Any, **kwargs: Any) -> FakeProcess:
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            second_attempt.set()
            return FakeProcess(returncode=0)
        raise OSError("spawn failed")

    config_path = Path("/tmp/fake_spawn_retry.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fail_then_succeed),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        try:
            await asyncio.wait_for(second_attempt.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail(
                "the liveness monitor never retried the failed spawn within 10s - "
                "the worker stays dead until supervisor restart"
            )
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    assert call_count == 2, f"expected exactly the initial attempt plus one retry, got {call_count}"


async def test_spawn_failure_exhausts_burst_budget_and_stops_retrying() -> None:
    """A permanently broken command line consumes the restart budget.

    Retry-under-budget is the fix's other half: without the burst limit
    applying to spawn failures, a child that can never start would
    retry forever at monitor-tick cadence.
    """
    import structlog

    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(
            shutdown_grace=1.0,
            backoff_initial=0.01,
            backoff_max=0.02,
            burst_limit=3,
            burst_window=60.0,
        ),
        workers=[_make_spec(name="w1")],
    )

    call_count = 0

    async def always_fail(*args: Any, **kwargs: Any) -> FakeProcess:
        nonlocal call_count
        call_count += 1
        raise OSError("spawn failed")

    config_path = Path("/tmp/fake_spawn_budget.toml")

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=always_fail),
        patch("asyncio.get_running_loop") as mock_loop,
        structlog.testing.capture_logs() as captured,
    ):
        mock_loop.return_value.add_signal_handler = MagicMock()

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        try:
            # Bounded wait on the budget-exhausted decision itself - the
            # event that PROVES the monitor stopped scheduling; when it
            # fires, the last budgeted attempt has already happened.
            # (time.monotonic, not the loop clock: asyncio.get_running_loop
            # is patched by this test's run_forever harness.)
            deadline = time.monotonic() + 15.0
            while not any(e.get("event") == "workgroup-burst-limit-exceeded" for e in captured):
                if time.monotonic() > deadline:
                    pytest.fail(
                        f"spawn failures never exhausted the burst budget within 15s "
                        f"(attempts={call_count})"
                    )
                await asyncio.sleep(0.01)
            # Hold the supervisor across further monitor ticks (0.5 s
            # cadence) so the exactly-once pin below observes repeats,
            # not just the first refusal: a monitor that re-decides a
            # given-up child every tick would flood the critical log at
            # ~2 Hz and keep consuming budget slots forever.
            await asyncio.sleep(1.2)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    # Initial attempt + exactly burst_limit budgeted retries.
    assert call_count == 4, f"expected 1 initial + {3} budgeted attempts, got {call_count}"
    criticals = [e for e in captured if e.get("event") == "workgroup-burst-limit-exceeded"]
    assert len(criticals) == 1, (
        f"budget exhaustion must log the critical exactly once, got {len(criticals)} "
        f"across further monitor ticks - a per-tick re-decision is a "
        "critical-log flood"
    )


async def test_stream_pump_failure_is_logged_and_task_completes() -> None:
    """A failed output pump is loud and clean, not silently lost.

    The pump forwards the child's stdout/stderr - the supervisor's
    primary diagnostic surface. An unexpected read failure must surface
    as a warning (the child's output is lost from that point, which an
    operator must be able to see) and the task must complete without an
    unretrieved exception.
    """
    import structlog

    class _ExplodingReader:
        async def readuntil(self, separator: bytes) -> bytes:
            raise RuntimeError("transport exploded")

    with structlog.testing.capture_logs() as captured:
        pump = asyncio.create_task(_stream_output(_ExplodingReader(), "w1", "warning"))  # type: ignore[arg-type]  # Why: structural StreamReader double - readuntil is the only member _read_line touches.
        # RED on the pre-fix code: this await raised RuntimeError and the
        # task's exception was never retrieved anywhere.
        await asyncio.wait_for(pump, timeout=5.0)

    failures = [e for e in captured if e.get("event") == "workgroup.stream_pump_failed"]
    assert failures, "the pump failure was silent - no workgroup.stream_pump_failed event"
    assert failures[0]["worker"] == "w1"
    assert failures[0]["error_class"] == "RuntimeError"


# ── Additional coverage: bad timestamp, kill timeout, graceful shutdown ─


# test_health_check_bad_timestamp was removed with the Python-side age
# computation: freshness is now computed by the PG server
# (last_seen_at > clock_timestamp() - $3::interval), so a malformed Python
# timestamp can no longer reach the verdict. The behavioral pin - a skewed
# supervisor must not flag a healthy child - lives in
# tests/test_workgroup_health_pg.py.


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
    sigterm_registered = asyncio.Event()

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler
        if sig == signal.SIGTERM:
            sigterm_registered.set()

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = capture_handler

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        # Event-driven startup wait - a fixed sleep races handler
        # registration under load (the SIGTERM() call below would
        # KeyError).
        try:
            await asyncio.wait_for(sigterm_registered.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("run_forever did not register its SIGTERM handler within 5.0s")

        # Trigger the signal handler to set shutting_down
        signal_handlers[signal.SIGTERM]()

        # Let the graceful shutdown proceed - with NO manual returncode:
        # the forwarded SIGTERM is what stops the child, and the final
        # assert verifies it. (Setting _returncode by hand would
        # deterministically suppress the SIGTERM forwarding AND make the
        # final assert vacuous: `returncode is not None` would always be
        # true because the test had just set it.)
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except TimeoutError:
            pytest.fail("run_forever did not complete graceful shutdown within 3.0s")

    # Process received SIGTERM during graceful shutdown
    assert signal.SIGTERM in fake_proc._signals, (
        "the supervisor did not forward SIGTERM to the child"
    )


# ── Bounded health-pool close at supervisor shutdown ────────────────────
#
# run_forever closed its health-check pool with a bare
# ``await pg_pool.close()`` - a dead PG can block that indefinitely,
# wedging the supervisor between "shutdown_begin" and "shutdown_complete".
# These tests pin the bounded-close discipline (asyncio.wait_for +
# terminate on timeout) applied via ``close_pool_bounded``; the shrink
# seam is the same module-global monkeypatch convention as
# tests/test_worker_deps_teardown.py.


class _FakeHealthPool:
    """Fake asyncpg.Pool for run_forever health-pool lifecycle tests.

    close() blocks while close_wait is cleared (dead PG); terminate()
    releases the gate. Mirrors the _FakePool conventions in
    tests/test_worker_deps_teardown.py.
    """

    def __init__(self) -> None:
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.closed = False
        self.terminated = False

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()


def _make_health_enabled_config() -> WorkgroupConfig:
    """Config with one health-checked worker so run_forever builds a pool.

    health_pg_dsn is set explicitly so run_forever skips
    WorkerSettings.load() and never touches the environment.
    """
    return WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(
            shutdown_grace=1.0,
            health_pg_dsn="postgresql://fake:fake@fake:5432/fake",
        ),
        workers=[
            WorkerSpec(
                name="w1",
                queues=["default"],
                poll_interval=0.1,
                max_concurrency=2,
                health=WorkerHealthConfig(
                    enabled=True,
                    check_interval=0.05,
                    startup_grace=0.0,
                ),
            )
        ],
    )


def _install_run_forever_patches(
    config: WorkgroupConfig,
    fake_proc: FakeProcess,
    fake_pool: _FakeHealthPool,
    signal_handlers: dict[int, Any],
) -> tuple[contextlib.ExitStack, asyncio.Event, asyncio.Event]:
    """Wire the standard run_forever stub patches.

    Returns the exit stack plus the two startup observables:
    ``sigterm_registered`` fires at the exact point run_forever registers
    its SIGTERM handler, and ``health_pool_created`` when
    asyncpg.create_pool returns the fake pool - the points a test about
    to trigger shutdown must wait for. The fixed 0.2s sleep this
    replaces raced startup under load: the SIGTERM() call would
    KeyError on an unregistered handler, and a shutdown before pool
    creation would skip the close path entirely and fail the pool
    asserts for the wrong reason.
    """

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        return fake_proc

    sigterm_registered = asyncio.Event()
    health_pool_created = asyncio.Event()

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler
        if sig == signal.SIGTERM:
            sigterm_registered.set()

    def _create_pool(*args: object, **kwargs: object) -> _FakeHealthPool:
        health_pool_created.set()
        return fake_pool

    patches = contextlib.ExitStack()
    patches.enter_context(
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config)
    )
    patches.enter_context(patch("asyncio.create_subprocess_exec", side_effect=fake_exec))
    loop_mock = patches.enter_context(patch("asyncio.get_running_loop"))
    loop_mock.return_value.add_signal_handler = capture_handler
    pool_mock = patches.enter_context(
        patch("taskq.worker.workgroup.asyncpg.create_pool", new_callable=AsyncMock)
    )
    pool_mock.side_effect = _create_pool
    health_mock = patches.enter_context(
        patch("taskq.worker.workgroup._child_health_check", new_callable=AsyncMock)
    )
    health_mock.return_value = True
    return patches, sigterm_registered, health_pool_created


async def _wait_for_run_forever_startup(
    sigterm_registered: asyncio.Event,
    health_pool_created: asyncio.Event,
    *,
    timeout: float = 5.0,  # noqa: ASYNC109  # Why: repo wait_for_* idiom (see taskq.testing.assertions) - a deadline parameter, not an asyncio.timeout scope.
) -> None:
    """Bounded, named waits on the two run_forever startup observables."""
    for event, what in (
        (sigterm_registered, "register its SIGTERM handler"),
        (health_pool_created, "create the health-check pool"),
    ):
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError:
            pytest.fail(f"run_forever did not {what} within {timeout}s")


@pytest.mark.asyncio
async def test_run_forever_terminates_hung_health_pool_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung pg_pool.close() during supervisor shutdown (dead PG) is
    terminated after the bounded timeout so run_forever completes."""
    import taskq.worker.workgroup as workgroup_mod

    monkeypatch.setattr(workgroup_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    fake_proc = FakeProcess(returncode=None)
    fake_pool = _FakeHealthPool()
    fake_pool.close_wait.clear()  # close() blocks forever from now on
    signal_handlers: dict[int, Any] = {}

    patches, sigterm_registered, health_pool_created = _install_run_forever_patches(
        _make_health_enabled_config(), fake_proc, fake_pool, signal_handlers
    )
    with patches:
        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(Path("/tmp/fake_wg_bounded_pool.toml")))
        await _wait_for_run_forever_startup(sigterm_registered, health_pool_created)

        signal_handlers[signal.SIGTERM]()
        # Why wait_for: pre-fix shutdown awaited pg_pool.close() unbounded,
        # so the RED state would hang forever instead of failing fast.
        await asyncio.wait_for(task, timeout=5)

    assert fake_pool.terminated is True
    assert fake_pool.close_calls == 1


@pytest.mark.asyncio
async def test_run_forever_fast_health_pool_close_not_terminated() -> None:
    """Healthy pool close at supervisor shutdown: closed once, never
    terminated. Pins the no-regression behaviour (passes pre/post-fix)."""
    fake_proc = FakeProcess(returncode=None)
    fake_pool = _FakeHealthPool()
    signal_handlers: dict[int, Any] = {}

    patches, sigterm_registered, health_pool_created = _install_run_forever_patches(
        _make_health_enabled_config(), fake_proc, fake_pool, signal_handlers
    )
    with patches:
        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(Path("/tmp/fake_wg_fast_pool.toml")))
        await _wait_for_run_forever_startup(sigterm_registered, health_pool_created)

        signal_handlers[signal.SIGTERM]()
        await asyncio.wait_for(task, timeout=5)

    assert fake_pool.closed is True
    assert fake_pool.close_calls == 1
    assert fake_pool.terminated is False


@pytest.mark.asyncio
async def test_run_forever_liveness_restarts_crashed_child() -> None:
    """Liveness monitor detects a crashed child and restarts it."""
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=1.0, backoff_initial=0.01, backoff_max=0.05),
        workers=[_make_spec(name="w1")],
    )

    procs: list[FakeProcess] = []
    restarted = asyncio.Event()

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        proc = FakeProcess(returncode=None)
        procs.append(proc)
        if len(procs) >= 2:
            restarted.set()
        return proc

    config_path = Path("/tmp/fake_restart.toml")
    signal_handlers: dict[int, Any] = {}
    sigterm_registered = asyncio.Event()

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler
        if sig == signal.SIGTERM:
            sigterm_registered.set()

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
    ):
        mock_loop.return_value.add_signal_handler = capture_handler

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(config_path))
        # run_forever registers handlers AFTER the initial spawn loop, so
        # this wait also guarantees procs[0] below exists - the fixed
        # 0.2s sleep it replaced could IndexError under startup
        # starvation.
        try:
            await asyncio.wait_for(sigterm_registered.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("run_forever did not spawn its child and register handlers within 5.0s")

        # First proc crashes
        procs[0]._returncode = 1
        # Bounded wait for the liveness monitor (0.5s poll) to detect the
        # crash and spawn a replacement - the event fires at the
        # observable's flip (the second fake_exec call), replacing the
        # 30x0.1s poll loop.
        try:
            await asyncio.wait_for(restarted.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("the liveness monitor did not restart the crashed child within 5.0s")

        assert len(procs) >= 2

        # Trigger graceful shutdown. No manual returncode poking: the
        # replacement is alive, so the supervisor's forwarded SIGTERM is
        # what stops it (FakeProcess sets returncode on SIGTERM).
        signal_handlers[signal.SIGTERM]()
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except TimeoutError:
            pytest.fail("run_forever did not complete graceful shutdown within 3.0s")


# ── child-exit events carry the workgroup's identity ───────────────────
#
# `_handle_child_exit` received `actors` and `wg_instance` and dropped
# both from every event it logs. `workgroup.start` logs them once at
# startup, but the events that matter operationally - a child exiting and
# the CRITICAL burst-limit trip - carried only the worker name, so they
# could not be correlated back to the workgroup instance that owns them.


@pytest.mark.asyncio
async def test_child_exit_events_carry_workgroup_identity() -> None:
    """Both the exit event and the CRITICAL burst-limit event identify the
    workgroup instance and its actor set."""
    import structlog

    scfg = SupervisorConfig(
        backoff_initial=0.01,
        backoff_max=0.1,
        backoff_factor=2.0,
        burst_limit=1,
        burst_window=60.0,
    )
    child = _make_child()
    child.backoff = 0.01
    shutting_down = asyncio.Event()
    wg_instance = UUID(int=7)

    with structlog.testing.capture_logs() as captured:
        child.process = _proc(FakeProcess(returncode=1))
        await _handle_child_exit(child, "billing,email", wg_instance, scfg, shutting_down)
        child.process = _proc(FakeProcess(returncode=1))
        await _handle_child_exit(child, "billing,email", wg_instance, scfg, shutting_down)

    exits = [e for e in captured if e["event"] == "workgroup-child-exit"]
    assert exits, "no workgroup-child-exit event captured"
    assert all(e["instance_id"] == str(wg_instance) for e in exits)
    assert all(e["actors"] == "billing,email" for e in exits)

    burst = [e for e in captured if e["event"] == "workgroup-burst-limit-exceeded"]
    assert len(burst) == 1
    assert burst[0]["instance_id"] == str(wg_instance)
    assert burst[0]["actors"] == "billing,email"


# ── Health-check query bound ────────────────────────────────────────────
#
# `_child_health_check` bounded only the pool acquire (2.0 s); the
# fetchrow itself had no deadline. A server that accepts the query and
# never answers parked the health loop inside the child's `restart_lock`
# - and because both the liveness monitor and the shutdown path acquire
# the same locks sequentially, ONE black-holed query froze restart
# scheduling for every child AND wedged the supervisor's SIGTERM
# forwarding. The fix is a client-side deadline on the query itself: per
# `taskq.worker._transient`, a client-side TimeoutError is a transient
# PG error, so it must land in the existing consecutive-failure
# accounting (healthy side until `consecutive_failure_limit`) - a
# black-holed DB is not the child's fault.


class _BlackHoleConn:
    """Fake connection that answers every health query except one label's.

    The hung label models a server that accepts the query and never
    answers: fetchrow awaits an event nothing will set, recording its
    own cancellation so tests can pin that the bound check CANCELS the
    black-holed query rather than abandoning it (abandonment would hold
    the pool connection forever).
    """

    def __init__(self, hang_label: str) -> None:
        self._hang_label = hang_label
        self._never = asyncio.Event()
        self.hang_entered = asyncio.Event()
        self.hang_cancelled = asyncio.Event()
        self.answered = asyncio.Event()

    async def fetchrow(self, sql: str, *args: object) -> dict[str, Any] | None:
        if args[1] == self._hang_label:
            self.hang_entered.set()
            try:
                await self._never.wait()
            except asyncio.CancelledError:
                self.hang_cancelled.set()
                raise
            raise AssertionError("unreachable: the hang gate is never set")
        self.answered.set()
        return {"pid": 12345, "fresh": True, "age_s": 1.0}


class _BlackHolePool:
    """Fake asyncpg.Pool yielding a :class:`_BlackHoleConn`; close is clean."""

    def __init__(self, hang_label: str) -> None:
        self.conn = _BlackHoleConn(hang_label)

    @contextlib.asynccontextmanager
    async def acquire(self, *, timeout: float | None = None):  # noqa: ASYNC109 # Why: mirrors asyncpg.Pool.acquire signature for drop-in compatibility, same as _FakePool above.
        yield self.conn

    async def close(self) -> None:
        return None

    def terminate(self) -> None:
        return None


async def test_health_check_query_timeout_counts_as_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A black-holed health query is bounded and errs on the healthy side.

    RED pre-fix: the fetchrow had no client-side deadline, so the check
    (and the restart_lock its caller holds) parked forever - bounded
    only by TCP keepalives, minutes out. The timeout must land in the
    same transient accounting as any other query failure: logged,
    health_failures incremented, verdict healthy below the limit.
    """
    import taskq.worker.workgroup as workgroup_mod

    # Why raising=False: the RED run must fail on the missing bound
    # itself (the outer wait_for timing out), not on an AttributeError
    # for the constant the fix introduces.
    monkeypatch.setattr(workgroup_mod, "_HEALTH_QUERY_TIMEOUT_SECS", 0.05, raising=False)
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=None))
    pool = _BlackHolePool(hang_label="test_worker")
    cfg = WorkerHealthConfig(enabled=True, consecutive_failure_limit=3)

    # Why the outer wait_for: the RED state hangs, and a hung test
    # proves nothing - it must fail fast instead.
    result = await asyncio.wait_for(
        _child_health_check(child, pool, "taskq", cfg, UUID(int=1)), timeout=1.0
    )

    assert result is True  # transient: errs on the side of healthy
    assert child.health_failures == 1
    assert pool.conn.hang_cancelled.is_set(), "the bound check must cancel the hung query"


async def test_health_check_query_timeout_at_limit_declares_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Consecutive timeouts cross consecutive_failure_limit: the check
    returns False - the existing policy that a persistently failing DB
    must not mask hung workers applies to black-holed queries too, so
    the timeout cannot be classified as always-healthy."""
    import taskq.worker.workgroup as workgroup_mod

    monkeypatch.setattr(
        workgroup_mod, "_HEALTH_QUERY_TIMEOUT_SECS", 0.05, raising=False
    )  # Why raising=False: same RED-honesty seam as the first test in this section.
    child = _make_child()
    child.process = _proc(FakeProcess(returncode=None))
    child.health_failures = 2
    pool = _BlackHolePool(hang_label="test_worker")
    cfg = WorkerHealthConfig(enabled=True, consecutive_failure_limit=3)

    result = await asyncio.wait_for(
        _child_health_check(child, pool, "taskq", cfg, UUID(int=1)), timeout=1.0
    )

    assert result is False
    assert child.health_failures == 3


async def test_run_forever_black_holed_health_query_does_not_stall_the_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One black-holed health query must not stall the other children's
    checks nor wedge shutdown.

    The health loop walks children sequentially while holding each
    child's restart_lock across its check, and the shutdown path
    acquires the same locks to forward SIGTERM - pre-fix, one
    never-answered fetchrow froze health checks for EVERY child and the
    supervisor's graceful shutdown with it.
    """
    import taskq.worker.workgroup as workgroup_mod

    monkeypatch.setattr(
        workgroup_mod, "_HEALTH_QUERY_TIMEOUT_SECS", 0.05, raising=False
    )  # Why raising=False: same RED-honesty seam as the first test in this section.
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(
            shutdown_grace=1.0,
            health_pg_dsn="postgresql://fake:fake@fake:5432/fake",
        ),
        workers=[
            WorkerSpec(
                name=name,
                queues=["default"],
                poll_interval=0.1,
                max_concurrency=2,
                health=WorkerHealthConfig(
                    enabled=True,
                    check_interval=0.05,
                    startup_grace=0.0,
                ),
            )
            for name in ("w1", "w2")
        ],
    )

    procs: list[FakeProcess] = []
    both_spawned = asyncio.Event()
    black_hole = _BlackHolePool(hang_label="w1")
    signal_handlers: dict[int, Any] = {}
    sigterm_registered = asyncio.Event()
    health_pool_created = asyncio.Event()

    async def fake_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        proc = FakeProcess(returncode=None)
        procs.append(proc)
        if len(procs) == 2:
            both_spawned.set()
        return proc

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler
        if sig == signal.SIGTERM:
            sigterm_registered.set()

    def _create_pool(*args: object, **kwargs: object) -> _BlackHolePool:
        health_pool_created.set()
        return black_hole

    with (
        patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
        patch("asyncio.get_running_loop") as mock_loop,
        patch("taskq.worker.workgroup.asyncpg.create_pool", new_callable=AsyncMock) as pool_mock,
    ):
        # Why _child_health_check is NOT patched here (unlike the other
        # run_forever health tests): the behaviour under test lives
        # inside it - the real check against a pool that never answers.
        mock_loop.return_value.add_signal_handler = capture_handler
        pool_mock.side_effect = _create_pool

        from taskq.worker.workgroup import run_forever

        task = asyncio.create_task(run_forever(Path("/tmp/fake_wg_black_hole.toml")))
        try:
            for event, what in (
                (both_spawned, "spawn both children"),
                (sigterm_registered, "register its SIGTERM handler"),
                (health_pool_created, "create the health-check pool"),
                (black_hole.conn.hang_entered, "enter the black-holed w1 query"),
            ):
                try:
                    await asyncio.wait_for(event.wait(), timeout=5.0)
                except TimeoutError:
                    pytest.fail(f"run_forever did not {what} within 5.0s")

            # The headline symptom: while w1's query is parked, w2's
            # check must still complete - pre-fix the sequential loop
            # never reached w2 and this bounded wait fails by name.
            try:
                await asyncio.wait_for(black_hole.conn.answered.wait(), timeout=2.0)
            except TimeoutError:
                pytest.fail(
                    "the black-holed w1 query stalled health checks for w2 - "
                    "the sequential loop never moved past it"
                )

            signal_handlers[signal.SIGTERM]()
            try:
                await asyncio.wait_for(task, timeout=3.0)
            except TimeoutError:
                pytest.fail(
                    "run_forever did not complete graceful shutdown within 3.0s - "
                    "the hung health query wedged the supervisor behind the "
                    "child's restart_lock"
                )
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    # w1's first check timed out before w2 was ever answered (the loop is
    # sequential), so the cancellation pin is deterministic here.
    assert black_hole.conn.hang_cancelled.is_set(), "the bound check never cancelled the hung query"
    for proc in procs:
        assert signal.SIGTERM in proc._signals, "a child never received the forwarded SIGTERM"


def test_health_query_timeout_constant_is_the_documented_default() -> None:
    """Pin the documented default for _HEALTH_QUERY_TIMEOUT_SECS.

    Every health-query-bound behaviour test monkeypatches the constant (raising=False),
    so none of them would notice a silent default change - 2.0 -> 30.0
    would pass CI while multiplying the worst-case health-check stall
    fifteenfold. The docstring documents 2.0 (consistent with the
    neighboring pool-acquire bound); this pin makes changing it a
    deliberate, review-visible act instead of a constant edit nobody
    fails on.
    """
    import taskq.worker.workgroup as workgroup_mod

    assert workgroup_mod._HEALTH_QUERY_TIMEOUT_SECS == 2.0


# ── Actor registry reachability at config load ──────────────────────


def _write_actors_module(tmp_path: Path, module_name: str, body: str) -> None:
    """Write an importable actors module under tmp_path and put it on sys.path."""
    (tmp_path / f"{module_name}.py").write_text(textwrap.dedent(body))
    if str(tmp_path) not in sys.path:
        sys.path.insert(0, str(tmp_path))


def test_actor_queue_no_child_consumes_warns_loudly_and_still_loads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actor on a queue no child consumes warns loudly; the load still succeeds.

    A workgroup is never the whole fleet. Another workgroup, another
    deployment, or a worker started by hand may consume that queue, so this
    supervisor cannot prove the queue is stranded -- only that *it* does not
    serve it. Refusing here would stop a set of children that can do real
    work over a condition the process cannot actually decide, so the
    governing rule is that a worker able to do work never fails to start.

    Diagnosability then rests entirely on the log line, which is why it must
    be loud and must name both the actor and the queue: without it, jobs for
    that actor enqueue successfully and pend forever with no signal anywhere.
    Refusal stays reserved for structural drift in stored configuration.
    """
    import structlog

    monkeypatch.chdir(tmp_path)
    _write_actors_module(
        tmp_path,
        "wg_actors_stranded",
        """
        from pydantic import BaseModel
        from taskq.actor import actor

        class Payload(BaseModel):
            pass

        @actor(queue="cron")
        async def cron_job(payload: Payload) -> None:
            pass

        @actor(queue="default")
        async def default_job(payload: Payload) -> None:
            pass

        registry = {"cron_job": cron_job, "default_job": default_job}
        """.strip("\n"),
    )

    toml = textwrap.dedent(
        """
        actors = "wg_actors_stranded:registry"

        [[workers]]
        name = "api"
        queues = ["default"]

        [[workers]]
        name = "api2"
        queues = ["default"]
        """
    ).strip()

    with structlog.testing.capture_logs() as captured:
        cfg = load_workgroup_config(_write_toml(tmp_path, toml))

    # The load succeeds: these two children consume "default" and can work.
    assert cfg.actors == "wg_actors_stranded:registry"
    assert [w.name for w in cfg.workers] == ["api", "api2"]

    warnings = [e for e in captured if e.get("log_level") in {"warning", "error", "critical"}]
    assert warnings, "no loud log entry emitted for the unconsumed actor queue"
    blob = repr(warnings)
    assert "cron_job" in blob, f"warning does not name the actor: {warnings!r}"
    assert "cron" in blob, f"warning does not name the queue: {warnings!r}"
    # The covered actor is healthy and must not be implicated.
    assert "default_job" not in blob, f"healthy actor flagged as stranded: {warnings!r}"


def test_actor_queues_covered_by_the_child_union_load_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A heterogeneous fleet whose children's queues cover every actor loads clean.

    Different children consuming disjoint queue subsets while sharing one
    actor registry is the normal split-queue deployment, not a fault. The
    stranding check keys off the union across all children, so this
    configuration must load without complaint -- a check that fired here
    would flag every healthy workgroup and train operators to ignore it.
    Since a warning is now the only signal the unconsumed-queue condition
    has, a false one costs the real one its meaning.
    """
    import structlog

    monkeypatch.chdir(tmp_path)
    _write_actors_module(
        tmp_path,
        "wg_actors_covered",
        """
        from pydantic import BaseModel
        from taskq.actor import actor

        class Payload(BaseModel):
            pass

        @actor(queue="cron")
        async def cron_job(payload: Payload) -> None:
            pass

        @actor(queue="default")
        async def default_job(payload: Payload) -> None:
            pass

        registry = {"cron_job": cron_job, "default_job": default_job}
        """.strip("\n"),
    )

    toml = textwrap.dedent(
        """
        actors = "wg_actors_covered:registry"

        [[workers]]
        name = "api"
        queues = ["default"]

        [[workers]]
        name = "cron_worker"
        queues = ["cron"]
        """
    ).strip()

    with structlog.testing.capture_logs() as captured:
        cfg = load_workgroup_config(_write_toml(tmp_path, toml))

    assert cfg.actors == "wg_actors_covered:registry"
    loud = [e for e in captured if e.get("log_level") in {"warning", "error", "critical"}]
    assert not loud, f"healthy split-queue workgroup produced a warning: {loud!r}"


def test_unresolvable_actors_reference_is_rejected_at_config_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ``actors`` reference that cannot be resolved must fail config load.

    ``module:attr`` shape alone does not mean the module imports or that the
    attribute exists. When the reference is unresolvable, every child
    crashes on import the moment it is spawned, and the supervisor reads
    that as a run of child exits: it restarts each one on backoff until the
    burst budget is exhausted, so the operator sees a cascade of respawns
    rather than the single real cause. The supervisor can resolve the
    reference once, up front, and refuse with a message that names it.
    """
    monkeypatch.chdir(tmp_path)
    toml = textwrap.dedent(
        """
        actors = "wg_actors_absent_module:registry"

        [[workers]]
        name = "api"
        queues = ["default"]
        """
    ).strip()

    with pytest.raises(ValueError, match="wg_actors_absent_module"):
        load_workgroup_config(_write_toml(tmp_path, toml))


def test_missing_actors_attribute_is_rejected_at_config_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ``actors`` module that imports but lacks the named attribute must fail load.

    The importable-module half can succeed while the attribute half fails,
    which is the easier typo to make and produces exactly the same
    spawn-crash-respawn cascade. Both halves have to be resolved at load
    time for the check to be worth anything.
    """
    monkeypatch.chdir(tmp_path)
    _write_actors_module(
        tmp_path,
        "wg_actors_no_attr",
        """
        other_name = {}
        """,
    )

    toml = textwrap.dedent(
        """
        actors = "wg_actors_no_attr:registry"

        [[workers]]
        name = "api"
        queues = ["default"]
        """
    ).strip()

    with pytest.raises(ValueError, match="registry"):
        load_workgroup_config(_write_toml(tmp_path, toml))


# ── The shutdown-grace window warning (F3) ─────────────────────────────


def _worker_settings_for_grace_window():  # type: ignore[no-untyped-def]  # Why: the local import keeps the module's import surface light; the return is always a real WorkerSettings.
    from taskq.settings import WorkerSettings

    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"},
        validate=False,
    )


def test_shutdown_grace_below_the_release_floor_warns_with_the_numbers() -> None:
    """The workgroup's default 30s grace SIGKILLs children before a held
    release can land: CANCELLING alone consumes the children's 30s
    cancellation grace, so the RELEASING write never happens and every
    interrupted job rides the lease-expiry crash path (slower, and it
    spends the attempt the release would have refunded). The warning
    names the floor and the clean-exit number."""
    from taskq.worker.workgroup import _warn_shutdown_grace_window

    settings = _worker_settings_for_grace_window()
    assert settings.cancellation_grace_period + settings.cleanup_grace_period == 40.0

    scfg = SupervisorConfig()  # shutdown_grace default 30.0 < 40.0
    assert scfg.shutdown_grace == 30.0
    with structlog.testing.capture_logs() as logs:
        _warn_shutdown_grace_window(scfg, settings)

    entry = next(
        log for log in logs if log["event"] == "workgroup.shutdown_grace_below_release_floor"
    )
    assert entry["log_level"] == "warning"
    assert entry["shutdown_grace"] == 30.0
    assert entry["release_floor_seconds"] == 40.0
    assert entry["clean_exit_floor_seconds"] == settings.worst_case_shutdown_seconds
    assert "shutdown_grace" in entry["remedy"]


def test_shutdown_grace_at_or_above_the_release_floor_stays_quiet() -> None:
    """The control: a grace that lets the release land (45s against the
    40s floor) emits nothing: a warning that fires on correct configs is
    noise that buries the next real one."""
    from taskq.worker.workgroup import _warn_shutdown_grace_window

    settings = _worker_settings_for_grace_window()
    scfg = SupervisorConfig(shutdown_grace=45.0)
    with structlog.testing.capture_logs() as logs:
        _warn_shutdown_grace_window(scfg, settings)
    assert [e for e in logs if e["event"] == "workgroup.shutdown_grace_below_release_floor"] == []
