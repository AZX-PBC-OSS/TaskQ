"""TCP/HTTP health listener: the probe surface Azure Container Apps can actually reach.

ACA supports only ``httpGet``/``tcpSocket`` probes against a container TCP port — there is no
``exec`` probe type (https://learn.microsoft.com/en-us/azure/container-apps/health-probes,
"Restrictions": "``exec`` probes aren't supported"), so the Unix-socket-only health server was
unprobeable there. Every test below drives a *real* listener over a *real* socket with a plain
client; nothing here mocks the responder.
"""

import asyncio
import contextlib
import errno
import os
import pathlib
import socket
import time
from collections.abc import AsyncGenerator
from types import SimpleNamespace

import pytest
import structlog.testing

from taskq.worker._watchdog import LoopLiveness
from taskq.worker.health import (
    HealthServer,
    HealthTcpBindError,
    HealthUnixBindCollisionError,
    register_readiness_check,
    unregister_readiness_check,
)
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
        # Maintenance-health fields read by build_ready_body's
        # maintenance_health view (only when the process-global OTel sweep
        # caches are non-empty — i.e. when other sweep-recording tests ran
        # earlier in this xdist worker process; without these the /ready
        # handler raises AttributeError and answers 500). Mirrors the stub
        # settings in tests/test_health.py and tests/test_web_health.py.
        sweep_interval=30.0,
        event_writer_batch_size=100,
    )


def _make_deps(settings: SimpleNamespace, *, pool: _StubPool | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        shutdown_phase=ShutdownPhase.NONE,
        dispatcher_pool=pool if pool is not None else _StubPool(),
        # The per-slot transaction pool is conditional: None on every
        # shape these tests exercise, so the readiness ping skips it.
        slot_pool=None,
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
        with pytest.raises(HealthTcpBindError):
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
        with pytest.raises(HealthTcpBindError) as excinfo:
            await server.start(_make_deps(settings))
        assert excinfo.value.host == "127.0.0.1"
        assert excinfo.value.port == taken_port
        assert isinstance(excinfo.value.__cause__, OSError)
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


# ── 4b. The two transports bind independently (#245) ────────────────────


async def _peer_http_ok(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """A live peer worker's stand-in: answers every request with a bare 200."""
    with contextlib.suppress(
        Exception
    ):  # Why: the peer must survive probe clients that vanish mid-request.
        await asyncio.wait_for(reader.readline(), timeout=5.0)
        writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


async def test_unix_collision_with_health_port_still_serves_tcp() -> None:
    """#245: a Unix-path collision must not take the TCP probe listener
    down with it.

    Before, ``start()`` bound the Unix socket first and aborted on the
    collision before ever attempting the TCP bind — so a worker deployed
    with ``health_port`` (the ACA/K8s probe surface) booted, registered,
    claimed work, and answered NOTHING at the port its probes target.
    The TCP bind must be attempted anyway, the collision must surface as
    its own partial-start type, and ``stop()`` must clean up exactly what
    the partial start owns: the TCP listener closed, the live peer's
    socket file left exactly where it was.
    """
    peer_path = _next_sock_path()
    peer = await asyncio.start_unix_server(_peer_http_ok, path=peer_path)
    server = HealthServer()
    try:
        settings = _make_settings(peer_path, health_port=0)
        with pytest.raises(HealthUnixBindCollisionError) as excinfo:
            await server.start(_make_deps(settings))
        assert excinfo.value.path == peer_path
        assert excinfo.value.errno == errno.EADDRINUSE
        # The port-routed probe surface is up and answering despite the
        # collision — this is the whole fix. Captured while the listener
        # lives: stop() nulls the server object it came from.
        port = _port(server)
        resp = await _tcp_get(port, "/live")
        assert resp.startswith(b"HTTP/1.0 200 OK\r\n"), resp
        # The peer keeps its serving surface while this worker runs.
        assert (await _unix_get(peer_path, "/live")).startswith(b"HTTP/1.0 200 OK\r\n")

        with structlog.testing.capture_logs() as stop_logs:
            await server.stop()

        # Partial-start cleanup: the TCP listener is gone (no leaked fd
        # serving probes for a dead worker), the peer's file was never this
        # server's to unlink, and stop() does not WARN about "not the one
        # this worker bound" for a socket it never bound at all. Checked
        # with the peer still live — its own close() unlinks its path, and
        # that must stay the only thing that ever does.
        port_refused = False
        try:
            await asyncio.open_connection("127.0.0.1", port)
        except OSError:
            port_refused = True
        assert port_refused, "stop() left the TCP probe listener serving after teardown"
        assert pathlib.Path(peer_path).exists(), (  # noqa: ASYNC240  # Why: a single fast metadata read in a test assertion; matches tests/test_health_http.py's existing convention.
            "stop() unlinked the live peer's socket file"
        )
        assert (await _unix_get(peer_path, "/live")).startswith(b"HTTP/1.0 200 OK\r\n"), (
            "the peer stopped answering after this worker's stop() — its "
            "serving surface was disturbed"
        )
        assert not any(e["event"] == "health-server-stop-skipped-unlink" for e in stop_logs), (
            "a never-bound path must be skipped silently, not reported as a shutdown-race near-miss"
        )
    finally:
        peer.close()
        await peer.wait_closed()


async def test_unix_collision_without_health_port_keeps_the_bare_oserror_contract() -> None:
    """No ``health_port`` configured: the collision keeps today's answer.

    Nothing of the server's is serving anywhere, so the boot's
    warn-and-continue (the #207 contract) fires on a plain ``OSError`` and
    no stop is owed — ``stop()`` stays a safe no-op that never touches the
    peer's file. Pinning the TYPE matters: ``HealthUnixBindCollisionError``
    subclasses ``OSError``, so a bare ``pytest.raises(OSError)`` cannot
    tell the two contracts apart.
    """
    peer_path = _next_sock_path()
    peer = await asyncio.start_unix_server(_peer_http_ok, path=peer_path)
    server = HealthServer()
    try:
        settings = _make_settings(peer_path)  # health_port unset
        with pytest.raises(OSError) as excinfo:
            await server.start(_make_deps(settings))
        assert type(excinfo.value) is OSError, (
            "with no TCP listener to save, the collision must stay the bare "
            f"OSError the bootstrap's warn-and-continue expects, got {type(excinfo.value)}"
        )
        assert excinfo.value.errno == errno.EADDRINUSE
        await server.stop()
        assert pathlib.Path(peer_path).exists(), (  # noqa: ASYNC240  # Why: a single fast metadata read in a test assertion; matches this file's existing convention.
            "stop() unlinked a path it never bound"
        )
        assert (await _unix_get(peer_path, "/live")).startswith(b"HTTP/1.0 200 OK\r\n")
    finally:
        peer.close()
        await peer.wait_closed()


async def test_stop_with_the_socket_already_unlinked_is_clean() -> None:
    """F3: a path already gone at ``stop()`` is the clean outcome, not the race.

    Python 3.13's ``Server.close()`` unlinks the socket it served (inode-guarded
    — the attacker proved a replacement's file survives it), so by the time
    ``stop()`` looks, every clean stop on 3.13+ finds no file. The file is
    removed by hand here so the pin holds on every version: no
    ``health-server-stop-skipped-unlink`` WARN — that is reserved for a
    DIFFERENT file sitting at the path — and the ``health-server-stopped``
    record still fires, because the stop did complete.
    """
    sock_path = _next_sock_path()
    settings = _make_settings(sock_path)
    server = HealthServer()
    await server.start(_make_deps(settings))
    try:
        assert pathlib.Path(sock_path).exists(), "sanity: the server bound the path"
        # What asyncio's own close() does on 3.13+ before stop() looks.
        os.unlink(sock_path)
        with structlog.testing.capture_logs() as captured:
            await server.stop()
        assert not any(e["event"] == "health-server-stop-skipped-unlink" for e in captured), (
            "a path already unlinked cleanly was reported as a shutdown-race near-miss"
        )
        assert any(e["event"] == "health-server-stopped" for e in captured), (
            "the clean-stop record must still be emitted — the stop completed"
        )
    finally:
        with contextlib.suppress(OSError):
            await server.stop()  # idempotent backstop; also ENOENT-clean now


async def test_unix_and_tcp_both_collide_refuses_startup_and_leaves_the_peer_alone() -> None:
    """When both transports collide, the TCP refusal governs the boot.

    The newcomer must refuse to start (``HealthTcpBindError`` — the
    manifest routes probes to that port), and its failed boot must not
    disturb the worker already serving on the Unix path: the peer's
    socket file survives the newcomer's cleanup untouched.
    """
    peer_path = _next_sock_path()
    peer = await asyncio.start_unix_server(_peer_http_ok, path=peer_path)

    squatter = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    squatter.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    squatter.bind(("127.0.0.1", 0))
    squatter.listen(1)
    taken_port = int(squatter.getsockname()[1])

    server = HealthServer()
    try:
        settings = _make_settings(peer_path, health_port=taken_port)
        with pytest.raises(HealthTcpBindError):
            await server.start(_make_deps(settings))
        assert pathlib.Path(peer_path).exists(), (  # noqa: ASYNC240  # Why: a single fast metadata read in a test assertion; matches this file's existing convention.
            "the refused boot unlinked the live peer's unix socket file"
        )
        assert (await _unix_get(peer_path, "/live")).startswith(b"HTTP/1.0 200 OK\r\n")
    finally:
        await server.stop()
        squatter.close()
        peer.close()
        await peer.wait_closed()


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
