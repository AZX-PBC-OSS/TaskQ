"""Worker dependencies: pools, dedicated connections, and lifecycle.

``open_worker_deps`` is an async context manager that constructs the three
asyncpg pools and two dedicated connections, returning a fully-wired
:class:`WorkerDeps` struct.  Startup order and LIFO teardown follow the
AsyncExitStack pattern.
"""

from __future__ import annotations

import asyncio
import socket
import sys
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import structlog

from taskq._dsn import dsn_host as _dsn_host
from taskq.constants import wake_channel
from taskq.obs import get_logger
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.worker.budget import compute_connection_budget
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.shutdown import ShutdownPhase

if TYPE_CHECKING:
    import redis.asyncio as redis_async

__all__ = ["WorkerDeps", "open_dedicated_conn", "open_worker_deps"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

# TCP keepalive parameters
_TCP_KEEPIDLE = 30
_TCP_KEEPINTVL = 5
_TCP_KEEPCNT = 3


def _apply_keepalive(sock: socket.socket) -> None:
    """Set TCP keepalive on a socket.

    Linux uses ``socket.TCP_KEEPIDLE``; macOS uses ``socket.TCP_KEEPALIVE``.
    Both platforms support ``TCP_KEEPINTVL`` and ``TCP_KEEPCNT``.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if sys.platform == "linux":
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, _TCP_KEEPIDLE)
    elif sys.platform == "darwin":
        if hasattr(socket, "TCP_KEEPALIVE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, _TCP_KEEPIDLE)
    else:
        # Other POSIX: try TCP_KEEPIDLE if available
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, _TCP_KEEPIDLE)

    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, _TCP_KEEPINTVL)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, _TCP_KEEPCNT)


async def open_dedicated_conn(
    dsn: str,
    *,
    label: str,
    apply_keepalive: bool = True,
) -> asyncpg.Connection:
    """Open a dedicated (non-pooled) asyncpg connection.

    If ``apply_keepalive`` is True, sets TCP keepalive
    on the underlying socket after the connection is established.
    """
    conn = await asyncpg.connect(dsn)
    if apply_keepalive:
        transport = conn._transport  # type: ignore[attr-defined]  # Why: asyncpg exposes _transport for socket access; no public API for keepalive.
        sock: socket.socket | None = transport.get_extra_info("socket")
        if sock is not None:
            _apply_keepalive(sock)
            logger.info(
                "dedicated-connection-opened",
                label=label,
                host=_dsn_host(dsn),
                keepalive=True,
                keepidle=_TCP_KEEPIDLE,
                keepintvl=_TCP_KEEPINTVL,
                keepcnt=_TCP_KEEPCNT,
            )
        else:
            logger.info(
                "dedicated-connection-opened",
                label=label,
                host=_dsn_host(dsn),
                keepalive=False,
                reason="socket-not-available",
            )
    else:
        logger.info(
            "dedicated-connection-opened",
            label=label,
            host=_dsn_host(dsn),
            keepalive=False,
        )
    return conn


@dataclass
class WorkerDeps:
    """Stable named handle for worker pools and connections.

    Passed through the worker main loop; the heartbeat, leader, and NOTIFY
    subsystems reach into this by name.
    """

    settings: WorkerSettings
    dispatcher_pool: asyncpg.Pool
    heartbeat_pool: asyncpg.Pool
    worker_pool: asyncpg.Pool
    notify_conn: asyncpg.Connection | None
    leader_conn: asyncpg.Connection | None
    is_leader: asyncio.Event = field(default_factory=asyncio.Event)
    producer_stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    active_jobs: ActiveJobRegistry = field(default_factory=ActiveJobRegistry)
    shutdown_phase: ShutdownPhase = ShutdownPhase.NONE
    heartbeat_failures: int = 0
    progress_buffers: dict[UUID, _ProgressBuffer] = field(
        default_factory=dict[UUID, _ProgressBuffer]
    )
    redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; the type arg cannot be supplied without a stubs update.
    pending_publish_tasks: set[asyncio.Task[None]] = field(default_factory=set[asyncio.Task[None]])
    """In-flight fire-and-forget Redis progress-publish tasks, shared across all
    concurrently-running jobs on this worker (keyed implicitly by task identity,
    not job_id — a job may have zero or more in-flight publishes at once).
    Referenced here (rather than only on JobContext) so a task started by a
    short-lived JobContext outlives the context and cannot be garbage-collected
    mid-publish; see JobContext.progress(). Drained best-effort on shutdown."""


@asynccontextmanager
async def open_worker_deps(
    settings: WorkerSettings,
) -> AsyncGenerator[WorkerDeps, None]:
    """Async context manager that constructs :class:`WorkerDeps`.

    Startup ordering: validate settings → open dispatcher_pool →
    open heartbeat_pool → open worker_pool → open notify_conn → open
    leader_conn.  Uses :class:`~contextlib.AsyncExitStack` so that a
    failure during step N closes steps 1..N-1 before the exception
    propagates.  Teardown is LIFO.
    """
    # Log the connection budget at startup
    budget = compute_connection_budget(settings, num_worker_pods=1)
    logger.info(
        "worker-startup-budget",
        host_direct=_dsn_host(settings.pg_dsn_direct),
        host_pooled=_dsn_host(settings.pg_dsn_pooled),
        direct=budget.total_direct,
        pooled=budget.total_pooled,
        total_pg=budget.total_pg,
        pgbouncer_recommended=budget.pgbouncer_recommended,
    )

    # _post_load guarantees these are non-None after load(); guard explicitly
    # so that str(None) == "None" is never silently passed to asyncpg.
    if settings.pg_dsn_direct is None:
        raise ValueError("pg_dsn_direct is None — was WorkerSettings.load() called?")
    if settings.pg_dsn_pooled is None:
        raise ValueError("pg_dsn_pooled is None — was WorkerSettings.load() called?")
    direct_dsn = str(settings.pg_dsn_direct)
    pooled_dsn = str(settings.pg_dsn_pooled)

    async with AsyncExitStack() as stack:
        # ── dispatcher_pool (pg_dsn_direct) ────────────────────────────
        dispatcher_pool = await asyncpg.create_pool(
            dsn=direct_dsn,
            min_size=1,
            max_size=settings.dispatcher_pool_size,
            max_inactive_connection_lifetime=settings.pool_max_inactive_lifetime,
        )
        await stack.enter_async_context(dispatcher_pool)
        logger.info(
            "pool-opened",
            pool="dispatcher",
            min_size=1,
            max_size=settings.dispatcher_pool_size,
            host=_dsn_host(direct_dsn),
        )

        # ── heartbeat_pool (pg_dsn_direct, command_timeout=2s) ────────
        heartbeat_pool = await asyncpg.create_pool(
            dsn=direct_dsn,
            min_size=1,
            max_size=settings.heartbeat_pool_size,
            max_inactive_connection_lifetime=settings.pool_max_inactive_lifetime,
            command_timeout=2,
        )
        await stack.enter_async_context(heartbeat_pool)
        logger.info(
            "pool-opened",
            pool="heartbeat",
            min_size=1,
            max_size=settings.heartbeat_pool_size,
            host=_dsn_host(direct_dsn),
        )

        # ── worker_pool (pg_dsn_pooled) ───────────────────────────────
        worker_pool = await asyncpg.create_pool(
            dsn=pooled_dsn,
            min_size=1,
            max_size=settings.worker_pool_size,
            max_inactive_connection_lifetime=settings.pool_max_inactive_lifetime,
        )
        await stack.enter_async_context(worker_pool)
        logger.info(
            "pool-opened",
            pool="worker",
            min_size=1,
            max_size=settings.worker_pool_size,
            host=_dsn_host(pooled_dsn),
        )

        # ── notify_conn (pg_dsn_direct, TCP keepalive) ────────────────
        notify_conn = await open_dedicated_conn(
            direct_dsn,
            label="notify",
            apply_keepalive=True,
        )

        # Issue LISTEN so the connection is in subscription state
        channel = wake_channel(settings.schema_name)
        await notify_conn.execute(f'LISTEN "{channel}"')
        logger.info("notify-listen-issued", channel=channel)

        # ── leader_conn (pg_dsn_direct, TCP keepalive) ─────────────────
        leader_conn = await open_dedicated_conn(
            direct_dsn,
            label="leader",
            apply_keepalive=True,
        )

        redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; the type arg cannot be supplied without a stubs update.
        if settings.redis_url is not None:
            import redis.asyncio as redis_async  # type: ignore[no-redef]  # Why: runtime import guarded by settings.redis_url; TYPE_CHECKING import is for annotations only

            redis_client = redis_async.from_url(
                str(settings.redis_url),
                decode_responses=False,
            )

        deps = WorkerDeps(
            settings=settings,
            dispatcher_pool=dispatcher_pool,
            heartbeat_pool=heartbeat_pool,
            worker_pool=worker_pool,
            notify_conn=notify_conn,
            leader_conn=leader_conn,
            redis_client=redis_client,
        )

        # LIFO teardown guards for dedicated connections.
        # orchestrate_shutdown closes and nulls leader_conn / notify_conn early
        # (to release the advisory lock before SIGTERM budget expires).  The
        # guards below prevent a double-close error during AsyncExitStack teardown.
        async def _close_leader_conn() -> None:
            if deps.leader_conn is not None:
                await deps.leader_conn.close()
                deps.leader_conn = None

        async def _close_notify_conn() -> None:
            if deps.notify_conn is not None:
                await deps.notify_conn.close()
                deps.notify_conn = None

        # Push in reverse startup order so LIFO teardown fires: redis → leader → notify → pools.
        # Pools sit deepest on the stack (enter_async_context) and are closed last.
        stack.push_async_callback(_close_notify_conn)
        stack.push_async_callback(_close_leader_conn)
        if redis_client is not None:
            _rc = redis_client
            stack.push_async_callback(_rc.aclose)

            async def _drain_pending_publishes() -> None:
                """Give in-flight fire-and-forget progress publishes a bounded
                window to finish before the Redis client closes underneath them."""
                if deps.pending_publish_tasks:
                    await asyncio.wait(deps.pending_publish_tasks, timeout=2.0)

            stack.push_async_callback(_drain_pending_publishes)

        yield deps
