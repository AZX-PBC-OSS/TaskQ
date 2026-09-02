"""Worker health endpoints: compute_health, HealthReport.

Two transports serve the same handler:

* a Unix domain socket (``health_socket_path``), always on with ``health_enabled``, which the
  ``taskq health live/ready`` CLI and exec-style probes use; and
* an optional TCP listener (``health_port``), off until a port is set.

The TCP listener exists because Azure Container Apps supports only ``httpGet``/``tcpSocket``
probes — "``exec`` probes aren't supported"
(https://learn.microsoft.com/en-us/azure/container-apps/health-probes) — so a Unix socket is
unreachable to a probe there. Kubernetes and ACA both treat 200-399 as success, so an unready
worker answers 503.
"""

import asyncio
import contextlib
import errno
import os
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

import asyncpg
import structlog

from taskq import _json
from taskq.obs import get_logger
from taskq.worker._watchdog import dump_task_stacks
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
    include_body: bool = True,
) -> None:
    """Write one minimal, close-delimited HTTP/1.0 response.

    Deliberately hand-rolled rather than served by FastAPI/uvicorn: the ``web`` extra is optional
    and a worker must never need it to answer a probe. A probe client needs only the status line,
    ``Content-Length`` and a clean close.

    ``include_body=False`` serves HEAD: the headers a GET would produce, including the
    ``Content-Length`` of the body that was *not* sent, per RFC 9110 §9.3.2.
    """
    writer.write(b"HTTP/1.0 %d " % status + reason + b"\r\n")
    if body:
        writer.write(b"Content-Type: " + content_type + b"\r\n")
    writer.write(b"Content-Length: %d\r\n" % len(body))
    # Why explicit: nothing here ever serves a second request on a connection, and a probe
    # client that waits for a keep-alive it will not get burns its own timeoutSeconds.
    writer.write(b"Connection: close\r\n\r\n")
    if body and include_body:
        writer.write(body)
    await writer.drain()


type ReadinessCheck = Callable[[], Awaitable[str | None]]
"""A consumer-supplied readiness check: return ``None`` when healthy, else a failure reason."""

_readiness_checks: dict[str, ReadinessCheck] = {}


def register_readiness_check(name: str, check: ReadinessCheck) -> None:
    """Register an extra check that ``/ready`` (and ``taskq health ready``) must also pass.

    Why a module-level registry rather than a field on ``WorkerDeps``: the worker bootstrap owns
    the :class:`HealthServer` instance, so a consumer embedding TaskQ has no handle to attach a
    check to. Registering by name before ``worker_main`` is the only seam that reaches both
    transports *and* the CLI, which reads the same :func:`compute_health`.

    A check that raises, or that outruns ``health_readiness_check_timeout``, counts as a
    failure — readiness fails closed.
    """
    _readiness_checks[name] = check


def unregister_readiness_check(name: str) -> None:
    """Remove a check registered by :func:`register_readiness_check`; unknown names are ignored."""
    _readiness_checks.pop(name, None)


async def _run_readiness_checks(per_check_timeout: float) -> list[str]:
    failures: list[str] = []
    for name, check in list(_readiness_checks.items()):
        try:
            reason = await asyncio.wait_for(check(), timeout=per_check_timeout)
        except TimeoutError:
            failures.append(f"{name}: timed out after {per_check_timeout}s")
        except Exception as exc:  # Why: a consumer check must never take the probe down with it.
            logger.warning("health-readiness-check-error", check=name, error=str(exc))
            failures.append(f"{name}: {exc}")
        else:
            if reason is not None:
                failures.append(f"{name}: {reason}")
    return failures


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
    # Watchdog observability: per-loop seconds since last liveness tick,
    # and seconds since shutdown began (None when not shutting down). A
    # zombie-ready worker is visible here instead of reporting healthy.
    loop_tick_ages: dict[str, float]
    shutdown_elapsed_seconds: float | None


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

    Reads ``deps``, performs a bounded PG ping, runs any checks added via
    :func:`register_readiness_check`, and returns a fully-populated
    :class:`HealthReport`.  No transport concerns and no caching — the check
    registry is the only global, and it is what lets a consumer extend
    readiness without a handle on the bootstrap-owned server.
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

    live, live_msg = await _check_live()
    if not live:
        ready = False
        reasons.append(f"event_loop_unresponsive: {live_msg}")

    # Required, not optional: every WorkerDeps carries a LoopLiveness, and
    # the stale-loop check below is the only thing that stops a zombie
    # worker reporting itself ready. Reading it defensively would let a
    # deps object with no liveness silently return a report that cannot
    # detect the zombie state — so a missing field is a wiring bug and
    # must surface as one.
    #
    # Deliberately NOT gated on watchdog_enabled: that switch controls the
    # force-exit detectors, not observability. Gating readiness too would
    # mean a worker with dead loops reports Ready whenever the switch is
    # off, which loses the zombie detection and keeps the traffic.
    tick_ages = deps.liveness.ages()
    stale_loops = deps.liveness.stale()
    if stale_loops:
        ready = False
        reasons.append(f"stale_loops={','.join(stale_loops)}")

    check_failures = await _run_readiness_checks(deps.settings.health_readiness_check_timeout)
    if check_failures:
        ready = False
        reasons.extend(check_failures)

    shutdown_elapsed: float | None = None
    if deps.shutdown_started_at is not None:
        shutdown_elapsed = max(0.0, time.monotonic() - deps.shutdown_started_at)

    report = HealthReport(
        live=live,
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
        loop_tick_ages=tick_ages,
        shutdown_elapsed_seconds=shutdown_elapsed,
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


async def _read_request_head(
    reader: asyncio.StreamReader,
    *,
    total_timeout: float,
    max_bytes: int,
) -> bytes | None:
    """Read the request line and drain the headers, bounded in *time* and in *bytes*.

    Returns the request line, or ``None`` for a malformed, oversized or too-slow request (the
    caller then closes without answering — there is nothing honest to say).

    Why both bounds, and why not a per-line timeout alone: a peer that sends one *valid* header
    line just under a per-line timeout never trips it, while holding the connection — and its
    server task — open indefinitely. The TCP listener is reachable from the pod network in a way
    the Unix socket is not, so an unauthenticated peer must not be able to exhaust accept
    capacity or file descriptors that way. ``max_bytes`` covers the mirror case: many tiny lines
    sent fast enough to stay inside the deadline. Both are settings
    (``health_request_timeout``, ``health_max_header_bytes``), not fixed constants, because the
    right values track the orchestrator's own probe ``timeoutSeconds``.
    """
    try:
        async with asyncio.timeout(total_timeout):
            request_line = await reader.readline()
            if not request_line:
                return None
            seen = len(request_line)
            while True:
                if seen > max_bytes:
                    return None
                line = await reader.readline()
                seen += len(line)
                if line in (b"\r\n", b""):
                    break
    except (TimeoutError, ValueError, ConnectionError, OSError):
        # ValueError: StreamReader.readline() raises it when a single line exceeds the stream
        # limit — an oversized header by another name.
        return None
    return request_line


class HealthServer:
    """HTTP health server for orchestrator probes, over a Unix socket and optionally TCP."""

    __slots__ = ("_deps", "_http_server", "_server", "_socket_inode", "_socket_path")

    def __init__(self) -> None:
        self._deps: WorkerDeps | None = None
        self._server: asyncio.Server | None = None
        self._http_server: asyncio.Server | None = None
        self._socket_path: str | None = None
        self._socket_inode: int | None = None

    @property
    def bound_port(self) -> int | None:
        """The TCP port actually listening, or ``None`` when the HTTP listener is disabled.

        Resolves an ephemeral ``health_port=0`` to the port the OS chose.
        """
        if self._http_server is None or not self._http_server.sockets:
            return None
        return int(self._http_server.sockets[0].getsockname()[1])

    async def start(self, deps: WorkerDeps) -> None:
        self._deps = deps
        self._socket_path = deps.settings.health_socket_path

        _unlink_stale_socket(self._socket_path)

        if deps.settings.health_tasks_enabled:
            old_umask = os.umask(0o077)
            try:
                self._server = await asyncio.start_unix_server(
                    self._handle_unix, path=self._socket_path
                )
            finally:
                os.umask(old_umask)
        else:
            self._server = await asyncio.start_unix_server(
                self._handle_unix, path=self._socket_path
            )
        # Capture the inode we just bound so `stop()` can later verify it
        # still owns this path before unlinking — a slow-shutting-down
        # worker must never delete a *replacement* worker's fresh socket
        # bound to the same path.
        with contextlib.suppress(OSError):
            self._socket_inode = os.stat(self._socket_path).st_ino
        logger.info("health-server-started", socket_path=self._socket_path)

        await self._start_http(deps)

    async def _start_http(self, deps: WorkerDeps) -> None:
        """Bind the optional TCP listener, or fail startup trying.

        Off unless ``health_port`` is set: a worker that binds a port nobody asked for is both a
        surprise and a network surface. Setting the port *is* the opt-in.
        """
        port = deps.settings.health_port
        if port is None:
            return

        host = deps.settings.health_host
        try:
            self._http_server = await asyncio.start_server(self._handle_tcp, host=host, port=port)
        except OSError as exc:
            # Why fail the whole startup rather than log and continue: the operator has told an
            # orchestrator to probe this port. A worker that came up with the listener dead
            # answers nothing there, so every probe fails — or worse, under a tcpSocket probe on
            # a port some *other* process holds, they all pass and traffic is routed to a worker
            # nobody is actually checking. Refusing to start is the only honest outcome.
            logger.error("health-http-bind-failed", host=host, port=port, error=str(exc))
            await self.stop()
            raise
        logger.info("health-http-server-started", host=host, port=self.bound_port)

    async def stop(self) -> None:
        if self._http_server is not None:
            self._http_server.close()
            await self._http_server.wait_closed()
            self._http_server = None

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

    async def _handle_unix(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await self._handle(reader, writer, transport="unix")

    async def _handle_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await self._handle(reader, writer, transport="tcp")

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        transport: Literal["unix", "tcp"],
    ) -> None:
        t0 = time.perf_counter()
        endpoint = ""
        status_code = 0
        deps = self._deps
        assert deps is not None
        try:
            request_line = await _read_request_head(
                reader,
                total_timeout=deps.settings.health_request_timeout,
                max_bytes=deps.settings.health_max_header_bytes,
            )
            if request_line is None:
                return

            parts = request_line.decode("ascii", errors="replace").split()
            if len(parts) < 2:
                return
            method, target = parts[0], parts[1]
            # Probe configs may append a query string; route on the path alone.
            path = target.split("?", 1)[0]
            endpoint = path
            body_wanted = method != "HEAD"

            if method not in ("GET", "HEAD"):
                await _write_response(writer, 404, b"Not Found")
                status_code = 404
            elif path == "/live":
                status_code = await self._handle_live(writer, include_body=body_wanted)
            elif path == "/ready":
                status_code = await self._handle_ready(writer, include_body=body_wanted)
            elif path == "/tasks":
                status_code = await self._handle_tasks(
                    writer, transport=transport, include_body=body_wanted
                )
            elif path == "/metrics":
                status_code = await self._handle_metrics(writer, include_body=body_wanted)
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

    async def _handle_live(self, writer: asyncio.StreamWriter, *, include_body: bool = True) -> int:
        ok, _msg = await _check_live()
        if ok:
            body = _json.dumps({"status": "ok"})
            await _write_response(writer, 200, b"OK", body=body, include_body=include_body)
            return 200
        else:
            body = _json.dumps({"status": "unresponsive"})
            await _write_response(
                writer, 503, b"Service Unavailable", body=body, include_body=include_body
            )
            return 503

    async def _handle_ready(
        self, writer: asyncio.StreamWriter, *, include_body: bool = True
    ) -> int:
        deps = self._deps
        assert deps is not None
        report = await compute_health(deps)
        body = build_ready_body(report, deps)
        status_code = 200 if report.ready else 503
        reason = b"OK" if status_code == 200 else b"Service Unavailable"
        await _write_response(writer, status_code, reason, body=body, include_body=include_body)
        return status_code

    async def _handle_tasks(
        self,
        writer: asyncio.StreamWriter,
        *,
        transport: Literal["unix", "tcp"] = "unix",
        include_body: bool = True,
    ) -> int:
        deps = self._deps
        assert deps is not None
        # Why the transport gate: ``health_tasks_enabled`` documents this dump as Unix-socket
        # only, and the socket's reachability (filesystem permissions, 0600 when enabled) is
        # what makes exposing code structure and task names acceptable. The TCP listener has
        # none of that, so the route simply does not exist there.
        if transport != "unix" or not deps.settings.health_tasks_enabled:
            # Disabled by default (TASKQ_HEALTH_TASKS_ENABLED): the dump
            # endpoint is privileged, so a disabled state is
            # indistinguishable from a missing route.
            await _write_response(writer, 404, b"Not Found")
            return 404
        records = dump_task_stacks("http-tasks", detector="http")
        body = _json.dumps({"tasks": [rec.as_dict() for rec in records]})
        await _write_response(
            writer,
            200,
            b"OK",
            body=body,
            content_type=b"application/json",
            include_body=include_body,
        )
        return 200

    async def _handle_metrics(
        self, writer: asyncio.StreamWriter, *, include_body: bool = True
    ) -> int:
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
            include_body=include_body,
        )
        return 200


def build_ready_body(report: HealthReport, deps: WorkerDeps) -> bytes:
    body = {
        "ready": report.ready,
        "live": report.live,
        "reasons": report.reasons,
        "redis_configured": report.redis_configured,
        "active_jobs": report.active_jobs,
        "is_leader": report.is_leader,
        "loop_tick_ages": report.loop_tick_ages,
        "shutdown_elapsed_seconds": report.shutdown_elapsed_seconds,
        "shutdown_phase": (
            deps.shutdown_phase.value if deps.shutdown_phase != ShutdownPhase.NONE else None
        ),
    }
    return _json.dumps(body)
