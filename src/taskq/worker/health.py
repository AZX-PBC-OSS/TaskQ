"""Worker health endpoints: compute_health, HealthReport."""

import asyncio
import contextlib
import errno
import os
import socket
import time
from dataclasses import dataclass

import asyncpg
import structlog

from taskq import _json
from taskq.obs import get_logger
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import ShutdownPhase

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


async def _write_response(
    writer: asyncio.StreamWriter,
    status: int,
    reason: bytes,
    *,
    body: bytes = b"",
    content_type: bytes = b"application/json",
) -> None:
    writer.write(b"HTTP/1.0 %d " % status + reason + b"\r\n")
    if body:
        writer.write(b"Content-Type: " + content_type + b"\r\n")
        writer.write(b"Content-Length: %d\r\n\r\n" % len(body))
        writer.write(body)
    else:
        writer.write(b"Content-Length: 0\r\n\r\n")
    await writer.drain()


@dataclass(frozen=True, slots=True)
class HealthReport:
    """Canonical health-report struct consumed by both transports."""

    live: bool
    ready: bool
    reasons: list[str]
    shutdown_phase: ShutdownPhase
    heartbeat_failures: int
    max_heartbeat_failures: int
    is_leader: bool
    redis_configured: bool
    pg_ping_ok: bool
    pg_ping_latency_ms: float
    active_jobs: int


async def _check_live() -> tuple[bool, str]:
    """Probe event-loop responsiveness via a scheduled callback.

    Schedules ``loop.call_later(0.01, ...)`` and waits up to 1.0 s for
    the callback to fire.  Returns ``(True, "ok")`` if the loop is
    responsive.
    """
    loop = asyncio.get_running_loop()
    responded = asyncio.Event()

    def _on_fired() -> None:
        responded.set()

    loop.call_later(0.01, _on_fired)
    try:
        await asyncio.wait_for(responded.wait(), timeout=1.0)
        return True, "ok"
    except TimeoutError:
        return False, "event loop unresponsive (timeout after 1.0s)"


async def compute_health(deps: WorkerDeps) -> HealthReport:
    """Single shared health function consumed by both transports.

    Reads ``deps``, performs a bounded PG ping, and returns a
    fully-populated :class:`HealthReport`.  No transport concerns, no
    caching, no global state.
    """
    phase: ShutdownPhase = deps.shutdown_phase
    pg_ping_ok_ = True
    reasons: list[str] = []

    t0 = time.perf_counter()
    try:
        async with deps.dispatcher_pool.acquire(
            timeout=deps.settings.health_pg_ping_timeout,
        ) as conn:
            await asyncio.wait_for(
                conn.execute("SELECT 1"),
                timeout=deps.settings.health_pg_ping_timeout,
            )
    except TimeoutError:
        pg_ping_ok_ = False
        reasons.append("pg_ping_timeout")
    except (
        asyncpg.PostgresConnectionError,
        asyncpg.InterfaceError,
        asyncpg.TooManyConnectionsError,
        OSError,
    ):
        pg_ping_ok_ = False
        reasons.append("pg_connection_error")
    except Exception as exc:
        logger.warning(
            "health-pg-ping-unexpected",
            error=str(exc),
        )
        pg_ping_ok_ = False
        reasons.append("pg_connection_error")
    t1 = time.perf_counter()
    pg_ping_latency_ms = (t1 - t0) * 1000.0

    ready = (phase == ShutdownPhase.NONE) and pg_ping_ok_

    if phase != ShutdownPhase.NONE:
        reasons.append(f"shutdown_phase={phase.name}")

    report = HealthReport(
        live=True,
        ready=ready,
        reasons=reasons,
        shutdown_phase=phase,
        heartbeat_failures=deps.heartbeat_failures,
        max_heartbeat_failures=deps.settings.max_heartbeat_failures,
        is_leader=deps.is_leader.is_set(),
        # Why the client check: managed-identity deployments inject a
        # client via redis_client_factory (or pass a caller-owned one)
        # without setting TASKQ_REDIS_URL — the URL alone would report
        # redis_configured: false despite a working client.
        redis_configured=bool(deps.settings.redis_url) or deps.redis_client is not None,
        pg_ping_ok=pg_ping_ok_,
        pg_ping_latency_ms=pg_ping_latency_ms,
        active_jobs=deps.active_jobs.count(),
    )

    logger.debug(
        "health-report",
        endpoint="compute_health",
        pg_ping_ok=pg_ping_ok_,
        pg_ping_latency_ms=pg_ping_latency_ms,
        shutdown_phase=phase.value,
        ready=ready,
    )

    return report


def _unlink_stale_socket(path: str) -> None:
    """Remove *path* only if it is a dead (unconnectable) unix socket.

    A socket path can outlive its process (e.g. after a crash) without
    being cleaned up. Blindly unlinking on every start is what causes the
    shutdown-race in :meth:`HealthServer.stop`, so this same "is it dead"
    check is applied at bind time too: if something is actually listening,
    leave the path alone and let ``start_unix_server`` fail loudly instead
    of silently stealing the socket out from under a live process.

    ``ENOTSOCK`` means *path* exists but is a regular file, not a socket
    at all (e.g. leftover from a crash before the socket was ever bound,
    or a stray file created at that path) — also stale, also safe to
    remove.
    """
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(path)
    except OSError as exc:
        if exc.errno in (errno.ECONNREFUSED, errno.ENOENT, errno.ENOTSOCK):
            with contextlib.suppress(FileNotFoundError):
                os.unlink(path)
    else:
        probe.close()
    finally:
        probe.close()


class HealthServer:
    """Unix-domain-socket HTTP server for K8s health probes."""

    __slots__ = ("_deps", "_server", "_socket_inode", "_socket_path")

    def __init__(self) -> None:
        self._deps: WorkerDeps | None = None
        self._server: asyncio.Server | None = None
        self._socket_path: str | None = None
        self._socket_inode: int | None = None

    async def start(self, deps: WorkerDeps) -> None:
        self._deps = deps
        self._socket_path = deps.settings.health_socket_path

        _unlink_stale_socket(self._socket_path)

        self._server = await asyncio.start_unix_server(self._handle, path=self._socket_path)
        # Capture the inode we just bound so `stop()` can later verify it
        # still owns this path before unlinking — a slow-shutting-down
        # worker must never delete a *replacement* worker's fresh socket
        # bound to the same path.
        with contextlib.suppress(OSError):
            self._socket_inode = os.stat(self._socket_path).st_ino
        logger.info("health-server-started", socket_path=self._socket_path)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

        if self._socket_path is not None:
            current_inode: int | None = None
            with contextlib.suppress(OSError):
                current_inode = os.stat(self._socket_path).st_ino

            if self._socket_inode is None or current_inode == self._socket_inode:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(self._socket_path)
                logger.info("health-server-stopped", socket_path=self._socket_path)
            else:
                logger.warning(
                    "health-server-stop-skipped-unlink",
                    socket_path=self._socket_path,
                    reason="socket inode changed since bind; a replacement worker owns this path now",
                )

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        t0 = time.perf_counter()
        endpoint = ""
        status_code = 0
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=1.0)
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=1.0)
                if line == b"\r\n" or not line:
                    break

            method, path, _ = request_line.decode("ascii", errors="replace").split(" ", 2)
            endpoint = path

            if method != "GET":
                await _write_response(writer, 404, b"Not Found")
                status_code = 404
            elif path == "/live":
                status_code = await self._handle_live(writer)
            elif path == "/ready":
                status_code = await self._handle_ready(writer)
            elif path == "/metrics":
                status_code = await self._handle_metrics(writer)
            else:
                await _write_response(writer, 404, b"Not Found")
                status_code = 404
        except (TimeoutError, ValueError, ConnectionError):
            pass
        except Exception:  # Why: catch-all guard writes best-effort HTTP 500; writer drain/write may already have failed.
            with contextlib.suppress(
                Exception
            ):  # Why: 500 body write is best-effort; suppress write errors when client disconnected.
                body = _json.dumps({"error": "internal"})
                await _write_response(writer, 500, b"Internal Server Error", body=body)
                status_code = 500
        finally:
            with contextlib.suppress(OSError):
                await writer.drain()
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        logger.debug(
            "health-request",
            endpoint=endpoint,
            status_code=status_code,
            response_time_ms=elapsed_ms,
        )

    async def _handle_live(self, writer: asyncio.StreamWriter) -> int:
        ok, _msg = await _check_live()
        if ok:
            body = _json.dumps({"status": "ok"})
            await _write_response(writer, 200, b"OK", body=body)
            return 200
        else:
            body = _json.dumps({"status": "unresponsive"})
            await _write_response(writer, 503, b"Service Unavailable", body=body)
            return 503

    async def _handle_ready(self, writer: asyncio.StreamWriter) -> int:
        deps = self._deps
        assert deps is not None
        report = await compute_health(deps)
        body = build_ready_body(report, deps)
        status_code = 200 if report.ready else 503
        reason = b"OK" if status_code == 200 else b"Service Unavailable"
        await _write_response(writer, status_code, reason, body=body)
        return status_code

    async def _handle_metrics(self, writer: asyncio.StreamWriter) -> int:
        deps = self._deps
        assert deps is not None
        active = deps.active_jobs.count()
        leader = 1 if deps.is_leader.is_set() else 0
        phase = deps.shutdown_phase.value

        body = (
            "# HELP taskq_active_jobs Currently in-flight jobs on this worker.\n"
            "# TYPE taskq_active_jobs gauge\n"
            f"taskq_active_jobs {active}\n"
            "# HELP taskq_is_leader 1 if this worker holds the maintenance leader lock.\n"
            "# TYPE taskq_is_leader gauge\n"
            f"taskq_is_leader {leader}\n"
            "# HELP taskq_shutdown_phase Current shutdown phase enum value (0=NONE).\n"
            "# TYPE taskq_shutdown_phase gauge\n"
            f"taskq_shutdown_phase {phase}\n"
        )
        body_bytes = body.encode()
        await _write_response(
            writer,
            200,
            b"OK",
            body=body_bytes,
            content_type=b"text/plain; version=0.0.4; charset=utf-8",
        )
        return 200


def build_ready_body(report: HealthReport, deps: WorkerDeps) -> bytes:
    body = {
        "ready": report.ready,
        "redis_configured": report.redis_configured,
        "active_jobs": report.active_jobs,
        "is_leader": report.is_leader,
        "shutdown_phase": (
            deps.shutdown_phase.value if deps.shutdown_phase != ShutdownPhase.NONE else None
        ),
    }
    return _json.dumps(body)
