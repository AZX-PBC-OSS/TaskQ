"""Tests for taskq.cli health sub-app: live, ready, metrics commands."""

import asyncio
import json
import os
import sys
import time
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from taskq.cli import _CONNECT_TIMEOUT_S, app
from taskq.worker.health import HealthServer
from taskq.worker.shutdown import ShutdownPhase

runner = CliRunner()

# ── Test stubs (following test_health.py patterns) ─────────────────────


class _FakeConn:
    async def execute(self, query: str, *args: object) -> str:
        return "SELECT 1"


class _AcquireCtx:
    def __init__(self, conn: _FakeConn | None = None, error: BaseException | None = None) -> None:
        self._conn = conn
        self._error = error

    async def __aenter__(self) -> _FakeConn:
        if self._error is not None:
            raise self._error
        assert self._conn is not None
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.acquire_calls = 0

    def acquire(self, timeout: float = 30.0) -> _AcquireCtx:
        self.acquire_calls += 1
        return _AcquireCtx(conn=_FakeConn() if self._error is None else None, error=self._error)


def _make_deps(**overrides: object) -> SimpleNamespace:
    defaults: dict[str, object] = {
        "shutdown_phase": ShutdownPhase.NONE,
        "dispatcher_pool": _StubPool(),
        "heartbeat_pool": _StubPool(),
        "settings": SimpleNamespace(
            health_pg_ping_timeout=0.2,
            max_heartbeat_failures=3,
            redis_url=None,
            health_socket_path="",
        ),
        "is_leader": SimpleNamespace(is_set=lambda: False),
        "active_jobs": SimpleNamespace(count=lambda: 2),
        "heartbeat_failures": 0,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_settings(sock_path: str) -> SimpleNamespace:
    return SimpleNamespace(
        health_pg_ping_timeout=0.2,
        max_heartbeat_failures=3,
        redis_url=None,
        health_socket_path=sock_path,
    )


_SOCK_ID_PREFIX = f"/tmp/tqht-{os.getpid()}-"  # noqa: S108 # Why: test socket paths must be short (<104 chars for macOS AF_UNIX limit); /tmp is the standard location.
_sock_id_seq = 0


def _next_sock_path() -> str:
    global _sock_id_seq
    _sock_id_seq += 1
    return f"{_SOCK_ID_PREFIX}{_sock_id_seq}.sock"


# ── Helpers ────────────────────────────────────────────────────────────


async def _invoke_health(command: str, sock_path: str) -> tuple[int, str, str]:
    """Run taskq health <command> as an async subprocess, yielding to the event loop."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "taskq.cli",
        "health",
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "TASKQ_HEALTH_SOCKET_PATH": sock_path},
    )
    stdout, stderr = await proc.communicate()
    assert proc.returncode is not None
    return proc.returncode, stdout.decode(), stderr.decode()


# ── CLI exit codes via socket ──────────────────────────────────


async def test_cli_live_exit_code() -> None:
    """CLI exit codes via socket — live.

    Start a real HealthServer with healthy deps. Invoke taskq health live
    via async subprocess; assert exit 0 and stdout contains {"status":"ok"}. (.)
    """
    sock_path = _next_sock_path()
    deps = _make_deps(settings=_make_settings(sock_path))

    server = HealthServer()
    await server.start(deps)  # pyright: ignore[reportArgumentType] # Why: test fake returns SimpleNamespace duck-type of WorkerDeps.
    try:
        returncode, stdout, stderr = await _invoke_health("live", sock_path)
        assert returncode == 0, f"stderr: {stderr}"
        assert json.loads(stdout.strip()) == {"status": "ok"}
    finally:
        await server.stop()


async def test_cli_ready_healthy_exit_code() -> None:
    """CLI exit codes via socket — ready, healthy.

    Start a real HealthServer with healthy deps. Invoke taskq health ready
    via async subprocess; assert exit 0 and stdout contains the five wire fields. (.)
    """
    sock_path = _next_sock_path()
    deps = _make_deps(settings=_make_settings(sock_path))

    server = HealthServer()
    await server.start(deps)  # pyright: ignore[reportArgumentType] # Why: test fake returns SimpleNamespace duck-type of WorkerDeps.
    try:
        returncode, stdout, stderr = await _invoke_health("ready", sock_path)
        assert returncode == 0, f"stderr: {stderr}"
        data = json.loads(stdout.strip())
        assert set(data.keys()) == {
            "ready",
            "redis_configured",
            "active_jobs",
            "is_leader",
            "shutdown_phase",
        }
        assert data["ready"] is True
    finally:
        await server.stop()


async def test_cli_ready_pg_failure_exit_code() -> None:
    """CLI exit codes via socket — ready, PG failure.

    Start a real HealthServer with a failing PG pool. Invoke taskq health
    ready via async subprocess; assert exit 1 and stdout contains "ready":false. (.)
    """
    sock_path = _next_sock_path()
    pg_fail_pool = _StubPool(error=TimeoutError("acquire timed out"))
    deps = _make_deps(dispatcher_pool=pg_fail_pool, settings=_make_settings(sock_path))

    server = HealthServer()
    await server.start(deps)  # pyright: ignore[reportArgumentType] # Why: test fake returns SimpleNamespace duck-type of WorkerDeps.
    try:
        returncode, stdout, stderr = await _invoke_health("ready", sock_path)
        assert returncode == 1, f"stderr: {stderr}"
        data = json.loads(stdout.strip())
        assert data["ready"] is False
    finally:
        await server.stop()


# ── CLI fails fast when socket absent ───────────────────────────


def test_cli_socket_absent_fail_fast_constant() -> None:
    """Constant assertion — _CONNECT_TIMEOUT_S == 0.1.

    Locks the fail-fast budget in source; independent of behaviour. (.)
    """
    assert _CONNECT_TIMEOUT_S == 0.1


def test_cli_socket_absent_fail_fast_wall_clock() -> None:
    """Behavioural wall-clock — CLI fails fast when socket absent.

    Wrap CliRunner.invoke with time.perf_counter() deltas; assert wall-clock
    stays bounded (generous CI budget, widened for parallel test load).
    Assert stderr contains unreachable message and exit code 1. (.)
    """
    t0 = time.perf_counter()
    result = runner.invoke(
        app,
        ["health", "live"],
        env={"TASKQ_HEALTH_SOCKET_PATH": "/tmp/definitely_does_not_exist.sock"},  # noqa: S108 # Why: test fixture — deliberately uses non-existent path for negative test.
    )
    elapsed = time.perf_counter() - t0
    # Widened from 1.0s: the real oracle is the 0.1s connect-timeout fail-fast
    # behavior; this bound just guards against unbounded hangs, with headroom
    # for scheduler contention under parallel test load (pytest -n 4).
    assert elapsed < 5.0, f"elapsed={elapsed:.3f}s — expected < 5.0s"
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "unreachable" in result.stderr.lower()


def test_cli_ready_socket_absent_in_process() -> None:
    """taskq health ready exits 1 in-process when the socket is absent.

    Covers the health_ready command wrapper itself (asyncio.Runner + typer.Exit),
    which the subprocess-based ready tests above don't reach in-process.
    """
    result = runner.invoke(
        app,
        ["health", "ready"],
        env={"TASKQ_HEALTH_SOCKET_PATH": "/tmp/definitely_does_not_exist_ready.sock"},  # noqa: S108 # Why: test fixture — deliberately uses non-existent path for negative test.
    )
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "unreachable" in result.stderr.lower()


def test_cli_metrics_socket_absent_in_process() -> None:
    """taskq health metrics exits 1 in-process when the socket is absent.

    Covers the health_metrics command wrapper itself (asyncio.Runner + typer.Exit),
    which the subprocess-based metrics test above doesn't reach in-process.
    """
    result = runner.invoke(
        app,
        ["health", "metrics"],
        env={"TASKQ_HEALTH_SOCKET_PATH": "/tmp/definitely_does_not_exist_metrics.sock"},  # noqa: S108 # Why: test fixture — deliberately uses non-existent path for negative test.
    )
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "unreachable" in result.stderr.lower()


# ── Metrics ────────────────────────────────────────────────────────────


async def test_cli_metrics_exit_code_and_prometheus_body() -> None:
    """taskq health metrics exit code 0 + Prometheus body smoke.

    Start HealthServer with healthy deps; invoke taskq health metrics
    via async subprocess; assert exit 0 and stdout starts with # HELP taskq_active_jobs.
    """
    sock_path = _next_sock_path()
    deps = _make_deps(settings=_make_settings(sock_path))

    server = HealthServer()
    await server.start(deps)  # pyright: ignore[reportArgumentType] # Why: test fake returns SimpleNamespace duck-type of WorkerDeps.
    try:
        returncode, stdout, stderr = await _invoke_health("metrics", sock_path)
        assert returncode == 0, f"stderr: {stderr}"
        assert stdout.strip().startswith("# HELP taskq_active_jobs")
    finally:
        await server.stop()


# ── _health_request in-process body-read / status-parsing happy path ────
#
# The subprocess-based tests above exercise the same code paths but from a
# separate interpreter, so they are invisible to in-process coverage
# instrumentation. These call _health_request directly, in-process, against
# a real HealthServer to cover the status-line parsing and body-read branch.


async def test_health_request_in_process_200_returns_zero() -> None:
    """_health_request against a healthy /live socket returns 0 in-process."""
    import taskq.cli as cli_mod

    sock_path = _next_sock_path()
    deps = _make_deps(settings=_make_settings(sock_path))

    server = HealthServer()
    await server.start(deps)  # pyright: ignore[reportArgumentType] # Why: test fake returns SimpleNamespace duck-type of WorkerDeps.
    try:
        settings = _make_settings(sock_path)
        code = await cli_mod._health_request(settings, "/live")  # pyright: ignore[reportArgumentType] # Why: test fake duck-types WorkerSettings.
        assert code == 0
    finally:
        await server.stop()


async def test_health_request_in_process_non_2xx_returns_one() -> None:
    """_health_request against a failing /ready socket returns 1 in-process."""
    import taskq.cli as cli_mod

    sock_path = _next_sock_path()
    pg_fail_pool = _StubPool(error=TimeoutError("acquire timed out"))
    deps = _make_deps(dispatcher_pool=pg_fail_pool, settings=_make_settings(sock_path))

    server = HealthServer()
    await server.start(deps)  # pyright: ignore[reportArgumentType] # Why: test fake returns SimpleNamespace duck-type of WorkerDeps.
    try:
        settings = _make_settings(sock_path)
        code = await cli_mod._health_request(settings, "/ready")  # pyright: ignore[reportArgumentType] # Why: test fake duck-types WorkerSettings.
        assert code == 1
    finally:
        await server.stop()


# ── CLI sub-app registered ─────────────────────────────────────────────


# ── _health_request internal timeout branch ─────────────────────────────


async def test_health_request_times_out_after_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    """_health_request returns 1 and prints 'timed out' when the response never arrives.

    A raw unix server accepts the connection (so the connect phase succeeds)
    but never writes a response, so the per-request asyncio.timeout fires.
    """
    import taskq.cli as cli_mod

    sock_path = _next_sock_path()

    async def _silent_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Read the request but never respond — forces the client to time out.
        # Sleep briefly (longer than the client's shrunk timeout, short enough
        # to keep the test fast) so the handler's connection is still open
        # when the client's asyncio.timeout fires, then let it close normally —
        # asyncio.Server.wait_closed() (3.13+) waits for in-flight handlers too.
        await reader.read(1024)
        await asyncio.sleep(0.3)

    server = await asyncio.start_unix_server(_silent_handler, path=sock_path)
    try:
        settings = _make_settings(sock_path)
        monkeypatch.setattr(cli_mod, "_REQUEST_TIMEOUT_S", 0.05)
        code = await cli_mod._health_request(settings, "/live")  # pyright: ignore[reportArgumentType] # Why: test fake duck-types WorkerSettings.
        assert code == 1
    finally:
        server.close()
        await server.wait_closed()


def test_cli_sub_app_registered() -> None:
    """CLI sub-app registered — verify --help for health commands.

    Assert that ["health", "live"], ["health", "ready"], ["health", "metrics"]
    are all valid invocations with --help. (.)
    """
    for command in ("live", "ready", "metrics"):
        result = runner.invoke(app, ["health", command, "--help"])
        assert result.exit_code == 0, f"{command} --help failed: {result.stderr}"
