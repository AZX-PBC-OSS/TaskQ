"""TCP/HTTP health listener: the probe surface Azure Container Apps can actually reach.

ACA supports only ``httpGet``/``tcpSocket`` probes against a container TCP port — there is no
``exec`` probe type (https://learn.microsoft.com/en-us/azure/container-apps/health-probes,
"Restrictions": "``exec`` probes aren't supported"), so the Unix-socket-only health server was
unprobeable there. Every test below drives a *real* listener over a *real* socket with a plain
client; nothing here mocks the responder.
"""

import asyncio
import contextlib
import os
import pathlib
import socket
import time
from collections.abc import AsyncGenerator
from types import SimpleNamespace

import pytest

from taskq.worker._watchdog import LoopLiveness
from taskq.worker.health import HealthServer, register_readiness_check, unregister_readiness_check
from taskq.worker.shutdown import ShutdownPhase

# ── Stubs (mirroring tests/test_health.py) ─────────────────────────────


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

    def acquire(self, timeout: float = 30.0) -> _AcquireCtx:
        return _AcquireCtx(conn=_FakeConn() if self._error is None else None, error=self._error)


_SOCK_ID_PREFIX = f"/tmp/tqhh-{os.getpid()}-"  # noqa: S108 # Why: AF_UNIX paths must stay under the 104-char sun_path limit; /tmp is the standard short location.
_sock_id_seq = 0


def _next_sock_path() -> str:
    global _sock_id_seq
    _sock_id_seq += 1
    return f"{_SOCK_ID_PREFIX}{_sock_id_seq}.sock"


def _make_settings(
    sock_path: str,
    *,
    health_port: int | None = None,
    health_tasks_enabled: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        health_pg_ping_timeout=0.2,
        max_heartbeat_failures=3,
        redis_url=None,
        health_socket_path=sock_path,
        health_tasks_enabled=health_tasks_enabled,
        health_host="127.0.0.1",
        health_port=health_port,
        health_request_timeout=2.0,
        health_max_header_bytes=16 * 1024,
        health_readiness_check_timeout=5.0,
    )


def _make_deps(settings: SimpleNamespace, *, pool: _StubPool | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        shutdown_phase=ShutdownPhase.NONE,
        dispatcher_pool=pool if pool is not None else _StubPool(),
        settings=settings,
        is_leader=SimpleNamespace(is_set=lambda: False),
        active_jobs=SimpleNamespace(count=lambda: 0),
        heartbeat_failures=0,
        redis_client=None,
        liveness=LoopLiveness(),
        shutdown_started_at=None,
    )


@contextlib.asynccontextmanager
async def _running(
    *,
    pool: _StubPool | None = None,
    http: bool = True,
    tasks_enabled: bool = False,
) -> AsyncGenerator[tuple[HealthServer, SimpleNamespace]]:
    settings = _make_settings(
        _next_sock_path(),
        # Port 0 asks the OS for an ephemeral port so parallel test workers never collide.
        health_port=0 if http else None,
        health_tasks_enabled=tasks_enabled,
    )
    server = HealthServer()
    await server.start(_make_deps(settings, pool=pool))
    try:
        yield server, settings
    finally:
        await server.stop()


def _port(server: HealthServer) -> int:
    """The listening TCP port, asserted present — every caller here enabled HTTP."""
    port = server.bound_port
    assert port is not None
    return port


# ── Raw clients: no HTTP library, exactly what a probe sends ───────────


async def _tcp_request(port: int, raw: bytes) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    try:
        return await asyncio.wait_for(reader.read(-1), timeout=5.0)
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()


async def _tcp_get(port: int, path: str, *, method: str = "GET") -> bytes:
    return await _tcp_request(
        port, f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("ascii")
    )


async def _unix_get(sock_path: str, path: str) -> bytes:
    reader, writer = await asyncio.open_unix_connection(sock_path)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("ascii"))
    await writer.drain()
    try:
        return await asyncio.wait_for(reader.read(-1), timeout=5.0)
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()


# ── 1. /live and /ready over a real TCP socket ─────────────────────────


async def test_http_live_returns_200() -> None:
    """A probe's GET /live over TCP gets a 200 status line and a JSON body."""
    async with _running() as (server, _settings):
        resp = await _tcp_get(_port(server), "/live")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp
    assert b'{"status":"ok"}' in resp.replace(b'{"status": "ok"}', b'{"status":"ok"}')
    assert b"Content-Length:" in resp


async def test_http_ready_returns_200_when_healthy() -> None:
    """GET /ready over TCP is 200 while the worker is running and PG pings."""
    async with _running() as (server, _settings):
        resp = await _tcp_get(_port(server), "/ready")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp
    assert b'"ready"' in resp


async def test_http_ready_returns_503_when_pg_unreachable() -> None:
    """503 is driven by the real condition — an unusable pool — not a patched responder.

    ACA/K8s treat 200-399 as success, so an unready worker MUST answer outside that band or the
    orchestrator will route traffic to it.
    """
    async with _running(pool=_StubPool(error=OSError("pg down"))) as (server, _settings):
        resp = await _tcp_get(_port(server), "/ready")
    assert resp.startswith(b"HTTP/1.0 503 Service Unavailable\r\n"), resp
    assert b"pg_connection_error" in resp


async def test_http_unknown_path_returns_404() -> None:
    async with _running() as (server, _settings):
        resp = await _tcp_get(_port(server), "/nope")
    assert resp.startswith(b"HTTP/1.0 404 Not Found\r\n"), resp


async def test_http_head_live_returns_status_without_body() -> None:
    """HEAD is answered with the status line and headers, never a body."""
    async with _running() as (server, _settings):
        resp = await _tcp_get(_port(server), "/live", method="HEAD")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp
    head, _, body = resp.partition(b"\r\n\r\n")
    assert body == b"", resp
    assert b"Content-Length:" in head


async def test_http_response_declares_connection_close() -> None:
    """Probe clients reuse nothing; be explicit so no client waits for a keep-alive."""
    async with _running() as (server, _settings):
        resp = await _tcp_get(_port(server), "/live")
    assert b"Connection: close\r\n" in resp, resp


# ── 2. The privileged /tasks dump never leaves the Unix socket ─────────


async def test_tasks_dump_is_not_exposed_over_tcp() -> None:
    """``health_tasks_enabled`` documents /tasks as "Unix socket only".

    The TCP listener is network-reachable in a way the Unix socket is not, so the stack dump must
    stay 404 there even when the setting is on.
    """
    async with _running(tasks_enabled=True) as (server, settings):
        tcp = await _tcp_get(_port(server), "/tasks")
        unix = await _unix_get(settings.health_socket_path, "/tasks")
    assert tcp.startswith(b"HTTP/1.0 404 Not Found\r\n"), tcp
    assert unix.startswith(b"HTTP/1.0 200 OK\r\n"), unix


# ── 3. Fail closed: a configured-but-unbindable port kills startup ─────


async def test_unbindable_port_fails_startup_loudly() -> None:
    """The single most important behaviour: never run with probes silently dead.

    A worker that starts anyway would answer nothing on the probe port; an ACA/K8s readiness probe
    failing is recoverable, but a worker that *thinks* it is serving probes while the operator
    believes they are configured is not. Startup must raise.
    """
    squatter = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    squatter.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    squatter.bind(("127.0.0.1", 0))
    squatter.listen(1)
    taken_port = int(squatter.getsockname()[1])

    settings = _make_settings(_next_sock_path(), health_port=taken_port)
    server = HealthServer()
    try:
        with pytest.raises(OSError):
            await server.start(_make_deps(settings))
    finally:
        await server.stop()
        squatter.close()


async def test_failed_http_bind_releases_the_unix_socket() -> None:
    """A failed start must not leave the Unix socket path bound and orphaned."""
    squatter = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    squatter.bind(("127.0.0.1", 0))
    squatter.listen(1)
    taken_port = int(squatter.getsockname()[1])

    sock_path = _next_sock_path()
    settings = _make_settings(sock_path, health_port=taken_port)
    server = HealthServer()
    try:
        with pytest.raises(OSError):
            await server.start(_make_deps(settings))
        await server.stop()
        assert not pathlib.Path(  # noqa: ASYNC240  # Why: a single fast metadata read in a test assertion; matches tests/test_health.py:403.
            sock_path
        ).exists(), "unix socket left behind after a failed HTTP bind"
    finally:
        squatter.close()


# ── 4. The Unix socket is unchanged, both with and without HTTP ────────


async def test_unix_socket_still_works_with_http_enabled() -> None:
    async with _running(http=True) as (server, settings):
        assert _port(server) > 0
        resp = await _unix_get(settings.health_socket_path, "/ready")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp


async def test_unix_socket_works_with_http_disabled() -> None:
    async with _running(http=False) as (server, settings):
        assert server.bound_port is None
        resp = await _unix_get(settings.health_socket_path, "/ready")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp


async def test_no_tcp_listener_when_port_unset() -> None:
    """Off by default: nothing binds a port nobody asked for."""
    async with _running(http=False) as (server, _settings):
        assert server.bound_port is None


# ── 5. Hostile input must not take the listener down ───────────────────


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"\r\n\r\n", id="empty-request-line"),
        pytest.param(b"GARBAGE\r\n\r\n", id="no-spaces"),
        pytest.param(b"\x00\x01\x02\xff\r\n\r\n", id="binary"),
        pytest.param(b"GET\r\n\r\n", id="truncated-request-line"),
        pytest.param(b"GET /live\r\n\r\n", id="missing-version"),
    ],
)
async def test_malformed_request_does_not_kill_listener(raw: bytes) -> None:
    async with _running() as (server, _settings):
        with contextlib.suppress(OSError):
            await _tcp_request(_port(server), raw)
        # The listener must still answer a well-formed probe afterwards.
        resp = await _tcp_get(_port(server), "/live")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp


async def test_oversized_header_is_dropped_and_listener_survives() -> None:
    """A header larger than ``health_max_header_bytes`` is refused, not buffered forever."""
    async with _running() as (server, _settings):
        flood = b"GET /live HTTP/1.1\r\n" + (b"X-Pad: " + b"a" * 512 + b"\r\n") * 200 + b"\r\n"
        with contextlib.suppress(OSError):
            await _tcp_request(_port(server), flood)
        resp = await _tcp_get(_port(server), "/live")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp


async def test_drip_fed_headers_do_not_hold_the_connection_open() -> None:
    """A per-line timeout alone does not bound the header loop.

    A peer sending one *valid* header line just under the per-line timeout never trips it while
    holding a connection — and its server task — indefinitely. The TCP listener is
    network-reachable, so the whole head-read is bounded by ``health_request_timeout`` regardless
    of per-line progress. Discovered downstream in cennan's bridge; it belongs here.
    """
    async with _running() as (server, settings):
        deadline: float = settings.health_request_timeout
        started = time.monotonic()
        reader, writer = await asyncio.open_connection("127.0.0.1", _port(server))
        try:
            writer.write(b"GET /live HTTP/1.1\r\n")
            await writer.drain()
            # Drip a valid header line every deadline/4 — never enough to trip a per-line
            # timeout — for 2.5x longer than the total deadline allows.
            with contextlib.suppress(OSError, TimeoutError):
                for _ in range(10):
                    await asyncio.sleep(deadline / 4)
                    writer.write(b"X-Drip: 1\r\n")
                    await writer.drain()
            # The server hangs up on its own: a clean EOF, or a reset when it closes with a
            # drip still in flight. Either proves it stopped waiting; a timeout here would
            # mean the connection (and its server task) is held open indefinitely.
            try:
                leftover = await asyncio.wait_for(reader.read(-1), timeout=2.0)
            except ConnectionError:
                pass
            except TimeoutError:
                pytest.fail("server held a drip-fed connection open past health_request_timeout")
            else:
                assert leftover == b"", leftover
            elapsed = time.monotonic() - started
            assert elapsed < deadline * 3, f"hangup took {elapsed:.2f}s, deadline {deadline}s"
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

        resp = await _tcp_get(_port(server), "/live")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp


# ── 6. Consumer-registered readiness checks ────────────────────────────


async def test_registered_readiness_check_can_fail_the_probe() -> None:
    """Consumers extend readiness without forking the responder or the report."""

    async def _dependency_down() -> str | None:
        return "search index unreachable"

    register_readiness_check("search_index", _dependency_down)
    try:
        async with _running() as (server, _settings):
            resp = await _tcp_get(_port(server), "/ready")
    finally:
        unregister_readiness_check("search_index")

    assert resp.startswith(b"HTTP/1.0 503 Service Unavailable\r\n"), resp
    assert b"search index unreachable" in resp


async def test_registered_readiness_check_passing_keeps_200() -> None:
    async def _ok() -> str | None:
        return None

    register_readiness_check("ok_check", _ok)
    try:
        async with _running() as (server, _settings):
            resp = await _tcp_get(_port(server), "/ready")
    finally:
        unregister_readiness_check("ok_check")
    assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp


async def test_raising_readiness_check_fails_closed() -> None:
    """A check that raises must read as unready, never as ready."""

    async def _boom() -> str | None:
        raise RuntimeError("kaboom")

    register_readiness_check("boom", _boom)
    try:
        async with _running() as (server, _settings):
            resp = await _tcp_get(_port(server), "/ready")
    finally:
        unregister_readiness_check("boom")
    assert resp.startswith(b"HTTP/1.0 503 Service Unavailable\r\n"), resp
    assert b"boom" in resp


async def test_hanging_readiness_check_fails_closed() -> None:
    """A wedged check must time out into 503 rather than hang the probe."""

    async def _hang() -> str | None:
        await asyncio.sleep(3600)
        return None

    register_readiness_check("hang", _hang)
    try:
        settings = _make_settings(_next_sock_path(), health_port=0)
        settings.health_readiness_check_timeout = 0.05
        server = HealthServer()
        await server.start(_make_deps(settings))
        try:
            resp = await _tcp_get(_port(server), "/ready")
        finally:
            await server.stop()
    finally:
        unregister_readiness_check("hang")
    assert resp.startswith(b"HTTP/1.0 503 Service Unavailable\r\n"), resp
    assert b"hang" in resp
