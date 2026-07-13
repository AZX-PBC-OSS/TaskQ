"""Tests for taskq.worker.health: compute_health, HealthReport, _check_live, HealthServer, build_ready_body."""

import asyncio
import contextlib
import glob
import json
import os
import sys
from collections.abc import Iterator
from types import SimpleNamespace

import asyncpg
import pytest

from taskq.worker.deps import WorkerDeps
from taskq.worker.health import HealthReport, HealthServer, _check_live, compute_health
from taskq.worker.shutdown import ShutdownPhase

pytestmark = pytest.mark.asyncio


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
        return _AcquireCtx(
            conn=_FakeConn() if self._error is None else None,
            error=self._error,
        )


def _make_deps(**overrides: object) -> WorkerDeps:  # pyright: ignore[reportReturnType] # Why: test fake returns a SimpleNamespace duck-type of WorkerDeps; constructing a real WorkerDeps requires real asyncpg pools.
    defaults: dict[str, object] = {
        "shutdown_phase": ShutdownPhase.NONE,
        "dispatcher_pool": _StubPool(),
        "heartbeat_pool": _StubPool(),
        "settings": SimpleNamespace(
            health_pg_ping_timeout=0.2,
            max_heartbeat_failures=3,
            redis_url=None,
            health_socket_path="",  # tests override via _make_settings
        ),
        "is_leader": SimpleNamespace(is_set=lambda: False),
        "active_jobs": SimpleNamespace(count=lambda: 2),
        "heartbeat_failures": 0,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)  # type: ignore[return-value] # Why: same underlying constraint as above; pyright flags the return statement separately.


# ── compute_health all healthy ──────────────────────────────────


async def test_compute_health_all_healthy() -> None:
    """compute_health with all subsystems healthy.

    Asserts live=True, ready=True, reasons=[], pg_ping_ok=True,
    redis_configured=False, active_jobs=2, is_leader=False.
    (.)
    """
    deps = _make_deps()

    report = await compute_health(deps)

    assert isinstance(report, HealthReport)
    assert report.live is True
    assert report.ready is True
    assert report.reasons == []
    assert report.pg_ping_ok is True
    assert report.redis_configured is False
    assert report.active_jobs == 2
    assert report.is_leader is False


# ── compute_health with shutdown_phase=DRAINING ─────────────────


async def test_compute_health_draining() -> None:
    """compute_health with deps.shutdown_phase=DRAINING.

    Asserts ready=False, 'shutdown_phase=DRAINING' in reasons,
    shutdown_phase field is ShutdownPhase.DRAINING. (.)
    """
    deps = _make_deps(shutdown_phase=ShutdownPhase.DRAINING)

    report = await compute_health(deps)

    assert report.ready is False
    assert "shutdown_phase=DRAINING" in report.reasons
    assert report.shutdown_phase == ShutdownPhase.DRAINING


# ── compute_health with PG ping timeout ────────────────────────


async def test_compute_health_pg_timeout() -> None:
    """compute_health with PG ping that times out.

    Asserts pg_ping_ok=False, 'pg_ping_timeout' in reasons,
    ready=False. (.)
    """
    timeout_pool = _StubPool(error=TimeoutError("acquire timed out"))
    deps = _make_deps(dispatcher_pool=timeout_pool)

    report = await compute_health(deps)

    assert report.pg_ping_ok is False
    assert "pg_ping_timeout" in report.reasons
    assert report.ready is False


# ── compute_health uses dispatcher_pool, never heartbeat_pool ──


async def test_compute_health_uses_dispatcher_pool() -> None:
    """compute_health acquires from dispatcher_pool, never heartbeat_pool.

    Stubs both pools with call counters; asserts acquire is called on
    dispatcher_pool exactly once and zero times on heartbeat_pool.
    (.)
    """
    dispatcher = _StubPool()
    heartbeat = _StubPool()
    deps = _make_deps(
        dispatcher_pool=dispatcher,
        heartbeat_pool=heartbeat,
        active_jobs=SimpleNamespace(count=lambda: 0),
    )

    await compute_health(deps)

    assert dispatcher.acquire_calls == 1
    assert heartbeat.acquire_calls == 0


# ── compute_health with redis_url=None is ready ─────────────────


async def test_compute_health_no_redis_ready() -> None:
    """compute_health with redis_url=None is fully ready (PG healthy).

    Asserts redis_configured=False AND ready=True. (.)
    """
    deps = _make_deps()

    report = await compute_health(deps)

    assert report.redis_configured is False
    assert report.ready is True


# ── _check_live() returns (True, "ok") ────────────────────────


async def test_check_live_responsive() -> None:
    """_check_live() returns (True, 'ok') when the loop is responsive. (.)"""
    live, msg = await _check_live()

    assert live is True
    assert msg == "ok"


# ── pg connection error path (PostgresConnectionError) ──────────


async def test_compute_health_pg_connection_error() -> None:
    """compute_health with asyncpg.PostgresConnectionError.

    Asserts pg_ping_ok=False, 'pg_connection_error' in reasons,
    ready=False. (.)
    """
    error_pool = _StubPool(error=asyncpg.PostgresConnectionError("connection refused"))
    deps = _make_deps(dispatcher_pool=error_pool)

    report = await compute_health(deps)

    assert report.pg_ping_ok is False
    assert "pg_connection_error" in report.reasons
    assert report.ready is False


# ── heartbeat_pool never called even on PG timeout ──────────────


async def test_heartbeat_pool_never_acquired_on_timeout() -> None:
    """heartbeat_pool.acquire never called even when PG ping times out."""
    dispatcher = _StubPool(error=TimeoutError("acquire timed out"))
    heartbeat = _StubPool()
    deps = _make_deps(dispatcher_pool=dispatcher, heartbeat_pool=heartbeat)

    await compute_health(deps)

    assert heartbeat.acquire_calls == 0


async def _http_get(sock_path: str, path: str) -> tuple[int, dict[str, str], str]:
    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        request = f"GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n"
        writer.write(request.encode())
        await writer.drain()

        status_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
        status_code = int(status_line.decode().split(" ", 2)[1])

        headers: dict[str, str] = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            if line == b"\r\n" or not line:
                break
            key, value = line.decode().strip().split(": ", 1)
            headers[key.lower()] = value

        content_length = int(headers.get("content-length", 0))
        body = b""
        if content_length > 0:
            body = await asyncio.wait_for(reader.readexactly(content_length), timeout=2.0)

        return status_code, headers, body.decode()
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()


def _make_settings(sock_path: str, **overrides: object) -> SimpleNamespace:
    return SimpleNamespace(
        health_pg_ping_timeout=0.2,
        max_heartbeat_failures=3,
        redis_url=None,
        health_socket_path=sock_path,
        **overrides,
    )


_SOCK_ID_PREFIX = f"/tmp/tqht-{os.getpid()}-"  # noqa: S108 # Why: test socket paths must be short (<104 chars for macOS AF_UNIX limit); /tmp is the standard location.
_sock_id_seq = 0


def _next_sock_path() -> str:
    global _sock_id_seq
    _sock_id_seq += 1
    return f"{_SOCK_ID_PREFIX}{_sock_id_seq}.sock"


@pytest.fixture(scope="session", autouse=True)
def _cleanup_sock_files() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Sweep this process's own tqht-<pid>-*.sock files after the session.

    HealthServer.start()/stop() already unlink these under normal test
    completion; this is a backstop for abnormal termination (a killed or
    timed-out run skips `finally` blocks and leaves the socket file
    behind). Scoped to _SOCK_ID_PREFIX (this PID only), so it can never
    touch a socket file from a different process or session.
    """
    yield
    for path in glob.glob(f"{_SOCK_ID_PREFIX}*.sock"):
        with contextlib.suppress(OSError):
            os.unlink(path)


# ── Readiness 503 body schema with CANCELLING ───────────────────


async def test_readiness_503_body_schema() -> None:
    """Readiness 503 body schema.

    Configure deps with shutdown_phase=CANCELLING and a healthy PG.
    Drive a request through _handle; parse the response body and assert
    all 5 wire fields are present, shutdown_phase=2, ready=False.
    """
    sock_path = _next_sock_path()
    deps = _make_deps(
        shutdown_phase=ShutdownPhase.CANCELLING,
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        status_code, _headers, body = await _http_get(sock_path, "/ready")
        assert status_code == 503

        data = json.loads(body)
        assert data["ready"] is False
        assert data["redis_configured"] is False
        assert data["active_jobs"] == 2
        assert data["is_leader"] is False
        assert data["shutdown_phase"] == 2
    finally:
        await server.stop()


# ── Stale socket cleanup ───────────────────────────────────────


async def test_stale_socket_cleanup() -> None:
    """Stale socket cleanup.

    Create a regular file at the chosen socket path. Call
    HealthServer.start(deps). Assert no OSError raised AND the
    server is listening. Then stop() and assert the file is gone.
    (.)
    """
    sock_path = _next_sock_path()
    with open(sock_path, "w") as f:  # noqa: ASYNC230 # Why: test fixture creates junk file synchronously for stale-socket cleanup test.
        f.write("garbage")

    deps = _make_deps(
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        status_code, _headers, body = await _http_get(sock_path, "/live")
        assert status_code == 200
        data = json.loads(body)
        assert data["status"] == "ok"
    finally:
        await server.stop()

    assert not os.path.exists(  # noqa: ASYNC240
        sock_path
    )  # Why: test assertion checks file state synchronously; Path.exists() is a fast metadata read.


# ── shutdown_phase JSON serialisation ──────────────────────────


async def test_shutdown_phase_serialisation_none() -> None:
    """shutdown_phase JSON serialisation — NONE.

    With deps.shutdown_phase=NONE (and otherwise healthy), drive /ready;
    parse JSON; assert body['shutdown_phase'] is None.
    (.)
    """
    sock_path = _next_sock_path()
    deps = _make_deps(
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        _status_code, _headers, body = await _http_get(sock_path, "/ready")
        data = json.loads(body)
        assert data["shutdown_phase"] is None
        assert data["ready"] is True
    finally:
        await server.stop()


async def test_shutdown_phase_serialisation_cancelling() -> None:
    """shutdown_phase JSON serialisation — CANCELLING.

    With deps.shutdown_phase=CANCELLING, assert body['shutdown_phase'] == 2.
    (.)
    """
    sock_path = _next_sock_path()
    deps = _make_deps(
        shutdown_phase=ShutdownPhase.CANCELLING,
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        _status_code, _headers, body = await _http_get(sock_path, "/ready")
        data = json.loads(body)
        assert data["shutdown_phase"] == 2
    finally:
        await server.stop()


# ── Import discipline ──────────────────────────────────────────


async def test_import_no_http_frameworks() -> None:
    """Import discipline.

    In a fresh subprocess, import taskq.worker.health and assert that
    none of fastapi, starlette, aiohttp are in sys.modules. (.)
    """
    check = (
        "import taskq.worker.health\n"
        "import sys\n"
        "for mod_name in ('fastapi', 'starlette', 'aiohttp'):\n"
        "    assert mod_name not in sys.modules, mod_name\n"
    )
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        check,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _stdout, stderr = await proc.communicate()
    assert proc.returncode == 0, f"stderr: {stderr.decode()}"


# ── Metrics format smoke test ─────────────────────────────────────────


async def test_metrics_format_smoke() -> None:
    """Metrics format smoke test.

    Drive /metrics; parse the body line by line; assert the three
    taskq_* lines are present and each is preceded by # HELP and
    # TYPE lines. Verify Content-Type header. (.)
    """
    sock_path = _next_sock_path()
    deps = _make_deps(
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        status_code, headers, body = await _http_get(sock_path, "/metrics")
        assert status_code == 200
        assert headers.get("content-type") == "text/plain; version=0.0.4; charset=utf-8"

        lines = body.rstrip("\n").split("\n")
        metric_names = ("taskq_active_jobs", "taskq_is_leader", "taskq_shutdown_phase")
        for name in metric_names:
            assert any(line.startswith(f"# HELP {name}") for line in lines)
            assert any(line.startswith(f"# TYPE {name} gauge") for line in lines)
            assert any(line.startswith(f"{name} ") for line in lines)
    finally:
        await server.stop()


# ── Liveness does not touch PG ────────────────────────────────────────


async def test_liveness_does_not_touch_pg(monkeypatch: pytest.MonkeyPatch) -> None:
    """Liveness does not touch PG.

    Stub compute_health with a counter; drive /live; assert the counter
    is zero. (.)
    """
    sock_path = _next_sock_path()
    deps = _make_deps(
        settings=_make_settings(sock_path),
    )

    call_count = 0

    async def _fake_compute_health(deps: object) -> HealthReport:
        nonlocal call_count
        call_count += 1
        return HealthReport(
            live=True,
            ready=True,
            reasons=[],
            shutdown_phase=ShutdownPhase.NONE,
            heartbeat_failures=0,
            max_heartbeat_failures=3,
            is_leader=False,
            redis_configured=False,
            pg_ping_ok=True,
            pg_ping_latency_ms=0.0,
            active_jobs=0,
        )

    monkeypatch.setattr("taskq.worker.health.compute_health", _fake_compute_health)

    server = HealthServer()
    await server.start(deps)
    try:
        status_code, _headers, body = await _http_get(sock_path, "/live")
        assert status_code == 200
        data = json.loads(body)
        assert data["status"] == "ok"
        assert call_count == 0
    finally:
        await server.stop()


# ── Slow-loris guard ──────────────────────────────────────────────────


async def test_slow_loris_guard() -> None:
    """Slow-loris guard.

    Open a Unix connection but never write request headers; assert the
    server closes the connection within ~1.5 s.
    """
    sock_path = _next_sock_path()
    deps = _make_deps(
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        reader, writer = await asyncio.open_unix_connection(sock_path)
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=1.5)
            assert data == b""
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
    finally:
        await server.stop()


# ── 404 on unknown path ───────────────────────────────────────────────


async def test_404_unknown_path() -> None:
    """GET /nonexistent returns HTTP 404 with empty body."""
    sock_path = _next_sock_path()
    deps = _make_deps(
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        status_code, _headers, body = await _http_get(sock_path, "/nonexistent")
        assert status_code == 404
        assert body == ""
    finally:
        await server.stop()


# ── 404 on non-GET method ─────────────────────────────────────────────


async def test_404_post_method() -> None:
    """POST /live returns HTTP 404."""
    sock_path = _next_sock_path()
    deps = _make_deps(
        settings=_make_settings(sock_path),
    )

    server = HealthServer()
    await server.start(deps)
    try:
        reader, writer = await asyncio.open_unix_connection(sock_path)
        try:
            writer.write(b"POST /live HTTP/1.0\r\nHost: localhost\r\n\r\n")
            await writer.drain()

            status_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            status_code = int(status_line.decode().split(" ", 2)[1])

            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if line == b"\r\n" or not line:
                    break

            assert status_code == 404
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
    finally:
        await server.stop()
