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
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import structlog

from taskq._dsn import dsn_host as _dsn_host
from taskq.connections import ConnFactory, PoolFactory, RedisFactory, WorkerConnections
from taskq.constants import wake_channel
from taskq.obs import get_logger
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.worker.budget import compute_connection_budget
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.shutdown import ShutdownPhase

if TYPE_CHECKING:
    import redis.asyncio as redis_async

__all__ = [
    "WorkerDeps",
    "apply_keepalive_to_conn",
    "open_dedicated_conn",
    "open_worker_deps",
    "reload_credentials",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

# Hold references to background drain tasks so they are not garbage-collected
# before completing. Cleared as each task finishes via done-callbacks.
_drain_tasks: set[asyncio.Task[None]] = set()

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


def apply_keepalive_to_conn(conn: asyncpg.Connection, *, label: str) -> bool:
    """Set TCP keepalive on an established connection's socket.

    Split out from :func:`open_dedicated_conn` so factory-built dedicated
    connections (credential-provider ``ConnFactory`` results) get the same
    keepalive policy as DSN-built ones — the worker owns this policy, not
    the user's factory. Returns True when keepalive was applied.
    """
    transport = getattr(
        conn, "_transport", None
    )  # Why: asyncpg exposes _transport for socket access; no public API for keepalive. getattr: fake/wrapped connections may not have one.
    sock: socket.socket | None = (
        transport.get_extra_info("socket") if transport is not None else None
    )
    if sock is None:
        logger.info("keepalive-skipped", label=label, reason="socket-not-available")
        return False
    _apply_keepalive(sock)
    logger.info(
        "keepalive-applied",
        label=label,
        keepidle=_TCP_KEEPIDLE,
        keepintvl=_TCP_KEEPINTVL,
        keepcnt=_TCP_KEEPCNT,
    )
    return True


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
    applied = apply_keepalive_to_conn(conn, label=label) if apply_keepalive else False
    logger.info(
        "dedicated-connection-opened",
        label=label,
        host=_dsn_host(dsn),
        keepalive=applied,
        **(
            {}
            if applied
            else {"reason": "disabled" if not apply_keepalive else "socket-not-available"}
        ),
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
    # Event set by the SIGHUP handler to signal the hot-reload coordinator
    # that a credential refresh has been requested.
    reload_event: asyncio.Event = field(default_factory=asyncio.Event)
    # Populated by notify_listener_loop so reload_credentials can trigger a
    # callback-aware reconnect (re-registers LISTEN + callbacks on the new
    # connection). None before the listener starts or after it stops.
    notify_reconnect_fn: Callable[[], Awaitable[None]] | None = None
    # The AsyncExitStack from open_worker_deps, stored so
    # reload_credentials can register hot-swapped pools for LIFO teardown.
    # None before open_worker_deps yields or after it exits.
    _exit_stack: AsyncExitStack | None = None
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
    notify_conn_factory: ConnFactory | None = None
    """Resolved factory that (re)builds ``notify_conn`` — the user-supplied
    ``WorkerConnections.notify_conn_factory`` if set, else a closure over the
    DSN-based :func:`open_dedicated_conn` call, else ``None`` when
    ``notify_conn`` is a caller-owned concrete connection (nothing to
    rebuild). Used by :mod:`taskq.worker.notify`'s reconnect loop and by
    :func:`reload_credentials` so a dropped or expiring connection is always
    rebuilt through the same credential source it was opened with."""
    leader_conn_factory: ConnFactory | None = None
    """Resolved factory that (re)builds ``leader_conn``. Same contract as
    ``notify_conn_factory``; used by :mod:`taskq.worker.leader`'s election
    watchdog and by :func:`reload_credentials`."""
    dispatcher_pool_factory: PoolFactory | None = None
    """Resolved factory that rebuilds ``dispatcher_pool`` on
    :func:`reload_credentials`. ``None`` when the pool is caller-owned."""
    heartbeat_pool_factory: PoolFactory | None = None
    """Resolved factory that rebuilds ``heartbeat_pool`` on
    :func:`reload_credentials`. ``None`` when the pool is caller-owned."""
    worker_pool_factory: PoolFactory | None = None
    """Resolved factory that rebuilds ``worker_pool`` on
    :func:`reload_credentials`. ``None`` when the pool is caller-owned."""
    redis_client_factory: RedisFactory | None = None
    """Resolved factory that rebuilds ``redis_client`` on
    :func:`reload_credentials`. ``None`` when the client is caller-owned or
    DSN-constructed (static credentials — nothing to rotate)."""
    owns_notify_conn: bool = False
    """True when ``notify_conn`` is TaskQ-owned (DSN- or factory-built) and
    may be closed by TaskQ paths. False when caller-owned — the ownership
    contract ("TaskQ never closes caller-owned resources") forbids closing
    it even on error paths."""
    owns_leader_conn: bool = False
    """True when ``leader_conn`` is TaskQ-owned. Same ownership contract as
    :attr:`owns_notify_conn`."""
    reload_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    """Serializes :func:`reload_credentials` — a second invocation while one
    is in flight returns immediately instead of double-draining pools and
    leaking the loser's replacements."""
    notify_reconnect_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    """Serializes notify_conn reconnects — the health-check loop and
    :func:`reload_credentials` (via ``notify_reconnect_fn``) can both trigger
    a reconnect; without mutual exclusion both build a new conn and the
    loser's LISTEN-registered conn leaks."""

    def request_reload(self) -> None:
        """Programmatic credential hot-reload trigger for embedders.

        Equivalent to sending SIGHUP: sets :attr:`reload_event`, which the
        worker's reload coordinator loop consumes. Also the rotation path
        on platforms without SIGHUP (e.g. Windows).
        """
        self.reload_event.set()


@asynccontextmanager
async def open_worker_deps(
    settings: WorkerSettings,
    *,
    connections: WorkerConnections | None = None,
) -> AsyncGenerator[WorkerDeps, None]:
    """Async context manager that constructs :class:`WorkerDeps`.

    Startup ordering: validate settings → open dispatcher_pool →
    open heartbeat_pool → open worker_pool → open notify_conn → open
    leader_conn → open redis_client.  Uses
    :class:`~contextlib.AsyncExitStack` so that a failure during step N
    closes steps 1..N-1 before the exception propagates.  Teardown is
    LIFO.

    ``connections`` provides per-role overrides — pre-constructed,
    caller-owned resources or zero-arg async factories — replacing the
    default DSN-based construction for any role that is set.  See
    :class:`~taskq.connections.WorkerConnections` for the ownership
    model and :mod:`taskq.aad` for Azure managed-identity factory
    builders.
    """
    conns = connections if connections is not None else WorkerConnections()

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
        connections_overrides=conns.has_any(),
    )

    # DSNs are only needed for the DSN-fallback paths.  When every PG role
    # is overridden the DSNs are never read, so tolerate ``None`` there;
    # otherwise guard explicitly so that str(None) == "None" is never
    # silently passed to asyncpg.
    direct_dsn: str | None = None
    pooled_dsn: str | None = None
    if _needs_pg_dsn(conns, for_direct=True):
        if settings.pg_dsn_direct is None:
            raise ValueError("pg_dsn_direct is None — was WorkerSettings.load() called?")
        direct_dsn = str(settings.pg_dsn_direct)
    if _needs_pg_dsn(conns, for_direct=False):
        if settings.pg_dsn_pooled is None:
            raise ValueError("pg_dsn_pooled is None — was WorkerSettings.load() called?")
        pooled_dsn = str(settings.pg_dsn_pooled)

    # Fail fast: a caller-owned leader_conn with no factory and no direct
    # DSN can never be rebuilt after a drop — the election watchdog would
    # otherwise retry asyncpg.connect(str(None)) forever, so leadership
    # would silently never recover. (notify_conn gets a pass: LISTEN is
    # best-effort with a poll fallback, and the listener disables itself
    # gracefully when a caller-owned conn drops with no rebuild path.)
    if (
        conns.leader_conn is not None
        and conns.leader_conn_factory is None
        and settings.pg_dsn_direct is None
    ):
        raise ValueError(
            "leader_conn is caller-owned with no leader_conn_factory and no "
            "pg_dsn_direct — a dropped leader connection could never be "
            "rebuilt. Provide leader_conn_factory (or configure pg_dsn_direct)."
        )

    # Track ownership of dedicated connections so the LIFO teardown guards
    # only close TaskQ-owned conns (caller-owned conns are left alone).
    owns_notify = conns.notify_conn is None  # DSN or factory → TaskQ-owned
    owns_leader = conns.leader_conn is None

    async with AsyncExitStack() as stack:
        # DSN-fallback factories — built inline with explicit kwargs so pyright
        # can trace types through ``asyncpg.create_pool`` (a ``**dict`` splat
        # would erase them). ``None`` when the DSN is unused (every role for
        # that DSN is overridden) or when the role itself is overridden.
        dispatcher_dsn_factory: PoolFactory | None = None
        heartbeat_dsn_factory: PoolFactory | None = None
        worker_dsn_factory: PoolFactory | None = None
        if direct_dsn is not None:
            _direct = direct_dsn
            _lifetime = settings.pool_max_inactive_lifetime

            async def _dispatcher_dsn_factory() -> asyncpg.Pool:
                pool = await asyncpg.create_pool(
                    dsn=_direct,
                    min_size=1,
                    max_size=settings.dispatcher_pool_size,
                    max_inactive_connection_lifetime=_lifetime,
                )
                assert pool is not None
                return pool

            async def _heartbeat_dsn_factory() -> asyncpg.Pool:
                pool = await asyncpg.create_pool(
                    dsn=_direct,
                    min_size=1,
                    max_size=settings.heartbeat_pool_size,
                    max_inactive_connection_lifetime=_lifetime,
                    command_timeout=2,
                )
                assert pool is not None
                return pool

            dispatcher_dsn_factory = _dispatcher_dsn_factory
            heartbeat_dsn_factory = _heartbeat_dsn_factory
        if pooled_dsn is not None:
            _pooled = pooled_dsn
            _lifetime = settings.pool_max_inactive_lifetime

            async def _worker_dsn_factory() -> asyncpg.Pool:
                pool = await asyncpg.create_pool(
                    dsn=_pooled,
                    min_size=1,
                    max_size=settings.worker_pool_size,
                    max_inactive_connection_lifetime=_lifetime,
                )
                assert pool is not None
                return pool

            worker_dsn_factory = _worker_dsn_factory

        # ── dispatcher_pool (pg_dsn_direct) ────────────────────────────
        dispatcher_pool = await _resolve_pool(
            conns.dispatcher_pool,
            conns.dispatcher_pool_factory,
            dispatcher_dsn_factory,
            stack,
            label="dispatcher",
            host=_dsn_host(direct_dsn) if direct_dsn else None,
        )

        # ── heartbeat_pool (pg_dsn_direct, command_timeout=2s) ────────
        heartbeat_pool = await _resolve_pool(
            conns.heartbeat_pool,
            conns.heartbeat_pool_factory,
            heartbeat_dsn_factory,
            stack,
            label="heartbeat",
            host=_dsn_host(direct_dsn) if direct_dsn else None,
        )

        # ── worker_pool (pg_dsn_pooled) ───────────────────────────────
        worker_pool = await _resolve_pool(
            conns.worker_pool,
            conns.worker_pool_factory,
            worker_dsn_factory,
            stack,
            label="worker",
            host=_dsn_host(pooled_dsn) if pooled_dsn else None,
        )

        # ── notify_conn (pg_dsn_direct, TCP keepalive) ────────────────
        # ``resolved_notify_factory`` is stored on WorkerDeps so notify.py's
        # reconnect loop and reload_credentials() rebuild the connection
        # through the same credential source it was originally opened with
        # — never falling back to a stale/absent DSN. ``None`` only when
        # notify_conn is caller-owned (nothing TaskQ can rebuild).
        resolved_notify_factory: ConnFactory | None
        notify_conn: asyncpg.Connection
        if conns.notify_conn is not None:
            notify_conn = conns.notify_conn  # caller-owned
            resolved_notify_factory = None
        elif conns.notify_conn_factory is not None:
            resolved_notify_factory = conns.notify_conn_factory
            notify_conn = await resolved_notify_factory()
            apply_keepalive_to_conn(notify_conn, label="notify")
        else:
            assert direct_dsn is not None  # guarded by _needs_pg_dsn
            _direct_notify = direct_dsn

            async def _notify_dsn_factory() -> asyncpg.Connection:
                return await open_dedicated_conn(
                    _direct_notify,
                    label="notify",
                    apply_keepalive=True,
                )

            resolved_notify_factory = _notify_dsn_factory
            notify_conn = await resolved_notify_factory()

        # Issue LISTEN so the connection is in subscription state
        channel = wake_channel(settings.schema_name)
        await notify_conn.execute(f'LISTEN "{channel}"')
        logger.info("notify-listen-issued", channel=channel, owns_notify=owns_notify)

        # ── leader_conn (pg_dsn_direct, TCP keepalive) ─────────────────
        resolved_leader_factory: ConnFactory | None
        leader_conn: asyncpg.Connection
        if conns.leader_conn is not None:
            leader_conn = conns.leader_conn  # caller-owned
            resolved_leader_factory = None
        elif conns.leader_conn_factory is not None:
            resolved_leader_factory = conns.leader_conn_factory
            leader_conn = await resolved_leader_factory()
            apply_keepalive_to_conn(leader_conn, label="leader")
        else:
            assert direct_dsn is not None  # guarded by _needs_pg_dsn
            _direct_leader = direct_dsn

            async def _leader_dsn_factory() -> asyncpg.Connection:
                return await open_dedicated_conn(
                    _direct_leader,
                    label="leader",
                    apply_keepalive=True,
                )

            resolved_leader_factory = _leader_dsn_factory
            leader_conn = await resolved_leader_factory()

        # ── redis_client ───────────────────────────────────────────────
        redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; the type arg cannot be supplied without a stubs update.
        owns_redis = False
        if conns.redis_client is not None:
            redis_client = conns.redis_client  # caller-owned
        elif conns.redis_client_factory is not None:
            redis_client = await conns.redis_client_factory()
            owns_redis = True
        elif settings.redis_url is not None:
            import redis.asyncio as redis_async  # type: ignore[no-redef]  # Why: runtime import guarded by settings.redis_url; TYPE_CHECKING import is for annotations only

            redis_client = redis_async.from_url(
                str(settings.redis_url),
                decode_responses=False,
            )
            owns_redis = True

        deps = WorkerDeps(
            settings=settings,
            dispatcher_pool=dispatcher_pool,
            heartbeat_pool=heartbeat_pool,
            worker_pool=worker_pool,
            notify_conn=notify_conn,
            leader_conn=leader_conn,
            redis_client=redis_client,
            notify_conn_factory=resolved_notify_factory,
            leader_conn_factory=resolved_leader_factory,
            # Reload (SIGHUP) only ever rebuilds via the user's own factory —
            # a fresh credential fetch. The DSN-fallback path uses static
            # credentials baked into the DSN, so there is nothing to rotate;
            # only conns.*_factory (not the DSN closures above) is stored here.
            dispatcher_pool_factory=conns.dispatcher_pool_factory,
            heartbeat_pool_factory=conns.heartbeat_pool_factory,
            worker_pool_factory=conns.worker_pool_factory,
            redis_client_factory=conns.redis_client_factory,
            owns_notify_conn=owns_notify,
            owns_leader_conn=owns_leader,
            _exit_stack=stack,
        )

        # LIFO teardown guards for TaskQ-owned dedicated connections.
        # orchestrate_shutdown closes and nulls a TaskQ-owned leader_conn early
        # (to release the advisory lock before the SIGTERM budget expires), and
        # reload_credentials swaps conns/pools mid-run. The guards below read
        # through ``deps`` at teardown time (never a captured instance), so
        # they close whatever is current and never double-close.
        # Caller-owned connections are never closed here.
        if owns_notify:

            async def _close_notify_conn() -> None:
                if deps.notify_conn is not None:
                    await deps.notify_conn.close()
                    deps.notify_conn = None

            stack.push_async_callback(_close_notify_conn)
        if owns_leader:

            async def _close_leader_conn() -> None:
                if deps.leader_conn is not None:
                    await deps.leader_conn.close()
                    deps.leader_conn = None

            stack.push_async_callback(_close_leader_conn)
        if owns_redis and redis_client is not None:

            async def _close_redis_client() -> None:
                # Closes through ``deps.redis_client`` — NOT the startup
                # instance — so a client swapped in by reload_credentials is
                # the one closed here (reload drains the old one itself).
                # Mirrors the notify/leader guards above.
                if deps.redis_client is not None:
                    await deps.redis_client.aclose()

            stack.push_async_callback(_close_redis_client)

            async def _drain_pending_publishes() -> None:
                """Give in-flight fire-and-forget progress publishes a bounded
                window to finish before the Redis client closes underneath them."""
                if deps.pending_publish_tasks:
                    await asyncio.wait(deps.pending_publish_tasks, timeout=2.0)

            stack.push_async_callback(_drain_pending_publishes)

        try:
            yield deps
        finally:
            # After exit the stack is closed — a late reload_credentials call
            # must fail fast instead of registering new pools on a dead stack
            # (they would never be closed).
            deps._exit_stack = None


# ── Internal helpers ───────────────────────────────────────────────────


def _needs_pg_dsn(conns: WorkerConnections, *, for_direct: bool) -> bool:
    """True if any direct/pooled role still needs the DSN fallback.

    When a role has a concrete resource or a factory, the DSN for that
    role is never read.  ``for_direct=True`` checks the four direct roles
    (dispatcher, heartbeat, notify, leader); ``for_direct=False`` checks
    worker_pool.  The direct DSN is needed if *any* direct role falls back.
    """
    if for_direct:
        direct_roles = [
            (conns.dispatcher_pool, conns.dispatcher_pool_factory),
            (conns.heartbeat_pool, conns.heartbeat_pool_factory),
            (conns.notify_conn, conns.notify_conn_factory),
            (conns.leader_conn, conns.leader_conn_factory),
        ]
        return any(concrete is None and factory is None for concrete, factory in direct_roles)
    return conns.worker_pool is None and conns.worker_pool_factory is None


async def _resolve_pool(
    concrete: asyncpg.Pool | None,
    factory: PoolFactory | None,
    dsn_factory: PoolFactory | None,
    stack: AsyncExitStack,
    *,
    label: str,
    host: str | None = None,
) -> asyncpg.Pool:
    """Resolve a pool from concrete / user factory / DSN factory and register teardown.

    * ``concrete`` — caller-owned; returned as-is, never closed by TaskQ.
    * ``factory`` — user-provided zero-arg async factory; TaskQ-owned.
    * ``dsn_factory`` — TaskQ-built DSN fallback factory; TaskQ-owned.

    Exactly one of the three must be non-``None``; the caller ensures this
    by building ``dsn_factory`` only when the DSN is available and the role
    is not overridden. TaskQ-owned pools are entered on ``stack`` for LIFO
    close.
    """
    if concrete is not None:
        logger.info("pool-using-provided", pool=label, ownership="caller")
        return concrete
    chosen = factory if factory is not None else dsn_factory
    assert chosen is not None, (
        f"{label} pool has no source — provide a concrete pool, factory, or DSN"
    )
    pool = await chosen()
    await stack.enter_async_context(pool)
    logger.info(
        "pool-opened",
        pool=label,
        ownership="factory" if factory is not None else "dsn",
        host=host,
    )
    return pool


# ── SIGHUP credential hot-reload ────────────────────────────────────────


async def reload_credentials(
    deps: WorkerDeps,
    *,
    drain_timeout: float = 5.0,
    factory_timeout: float = 30.0,
) -> tuple[list[str], list[str]]:
    """Hot-swap every factory-backed PG pool, dedicated connection, and Redis
    client on *deps* with freshly-built replacements.

    For each factory-backed resource:
    1. Build a new resource by calling the factory (which fetches a fresh
       credential — AAD token, AWS IAM token, Vault dynamic creds), bounded
       by ``factory_timeout`` so a hung token endpoint cannot wedge the
       reload (and, via the coordinator, all future SIGHUPs).
    2. Atomically swap it onto ``deps``.
    3. Close the old resource in a background task with a bounded drain
       timeout — in-flight queries on old pool connections are given
       ``drain_timeout`` seconds to finish before the old pool is
       terminated.

    Resources that are caller-owned (no factory stored on ``deps``) are
    skipped — the caller is responsible for their lifecycle.

    ``notify_conn`` is rebuilt through the listener's
    ``reconnect_notify_conn`` helper, which re-issues LISTEN and
    re-registers callbacks. ``leader_conn`` is closed and nulled (not
    swapped): the leader election watchdog observes the None and reopens
    through ``leader_conn_factory`` on its next tick, re-acquiring the
    advisory lock.

    New pools are registered on ``deps._exit_stack`` (the ``AsyncExitStack``
    from ``open_worker_deps``) for LIFO teardown at shutdown. Old pools are
    closed in the background and do NOT sit on the stack.

    Each resource is reloaded independently — a factory failure for one
    (e.g. a transient credential-fetch error) is logged
    (``credential-reload-resource-failed``) and does NOT abort the
    remaining resources or raise out of this function; that resource
    simply keeps its current (not-yet-expired) pool/connection until the
    next SIGHUP.

    Concurrent invocations are serialized on ``deps.reload_lock``: a
    second call while one is in flight returns ``([], [])`` immediately
    rather than double-draining pools and leaking replacements.

    Returns ``(reloaded, failed)`` — the resource labels that were and
    were not rotated. A non-empty ``failed`` list means a partial reload;
    the operator can send SIGHUP again to retry.

    This function is triggered by the SIGHUP handler installed by
    :func:`~taskq.worker.shutdown.install_signal_handlers` and run by the
    reload coordinator loop in :func:`~taskq.worker._bootstrap._main`.
    """
    stack = deps._exit_stack
    if stack is None:
        raise RuntimeError(
            "reload_credentials called outside of open_worker_deps — deps._exit_stack is None"
        )

    if deps.reload_lock.locked():
        logger.info("credential-reload-skipped", reason="reload-already-in-progress")
        return [], []

    async with deps.reload_lock:
        reloaded: list[str] = []
        failed: list[str] = []

        # ── Pools ──────────────────────────────────────────────────
        # Each pool is reloaded independently — a factory failure for one
        # (e.g. a transient credential-fetch error) is logged and does NOT
        # abort the remaining resources. Without this, a single flaky
        # provider call would silently leave later pools/conns on stale
        # credentials with no indication anything was skipped.
        for label, pool_attr, factory_attr in (
            ("dispatcher", "dispatcher_pool", "dispatcher_pool_factory"),
            ("heartbeat", "heartbeat_pool", "heartbeat_pool_factory"),
            ("worker", "worker_pool", "worker_pool_factory"),
        ):
            factory: PoolFactory | None = getattr(deps, factory_attr)
            if factory is None:
                continue
            try:
                old_pool: asyncpg.Pool = getattr(deps, pool_attr)
                new_pool = await asyncio.wait_for(factory(), timeout=factory_timeout)
                await stack.enter_async_context(new_pool)
                setattr(deps, pool_attr, new_pool)
                _drain_old_pool(old_pool, label, drain_timeout)
                reloaded.append(label)
            except Exception as exc:
                logger.warning(
                    "credential-reload-resource-failed",
                    kind="credential_reload_resource_failed",
                    resource=label,
                    error=repr(exc),
                )
                failed.append(label)

        # ── notify_conn ────────────────────────────────────────────
        # Caller-owned notify_conn has no factory — nothing to rotate, so
        # skip cleanly (never record a spurious failure). When a factory
        # exists, prefer the listener's callback-aware reconnect closure
        # (re-issues LISTEN + re-registers callbacks); fall back to a
        # direct swap when the listener isn't running.
        if deps.notify_conn_factory is None:
            pass
        else:
            try:
                if deps.notify_reconnect_fn is not None:
                    await asyncio.wait_for(deps.notify_reconnect_fn(), timeout=factory_timeout)
                    reloaded.append("notify_conn")
                else:
                    # Listener not started yet (or already stopped) — swap directly.
                    old_notify = deps.notify_conn
                    new_notify = await asyncio.wait_for(
                        deps.notify_conn_factory(), timeout=factory_timeout
                    )
                    apply_keepalive_to_conn(new_notify, label="notify")
                    channel = wake_channel(deps.settings.schema_name)
                    await new_notify.execute(f'LISTEN "{channel}"')
                    deps.notify_conn = new_notify
                    if old_notify is not None and old_notify is not new_notify:
                        _drain_old_conn(old_notify, "notify", drain_timeout)
                    reloaded.append("notify_conn")
            except Exception as exc:
                logger.warning(
                    "credential-reload-resource-failed",
                    kind="credential_reload_resource_failed",
                    resource="notify_conn",
                    error=repr(exc),
                )
                failed.append("notify_conn")

        # ── leader_conn ────────────────────────────────────────────
        # The leader election loop has its own watchdog that detects a dead
        # leader_conn, clears is_leader, and reopens via _open_leader_conn
        # (which uses deps.leader_conn_factory when set). We trigger that
        # failover path by closing the current leader_conn — the watchdog
        # reopens with a fresh credential and re-acquires the advisory lock.
        # This is the same path as a PG connection drop, so it's well-tested.
        #
        # The old conn is closed inline (bounded) BEFORE nulling — a
        # background close would let the watchdog's re-election race the
        # old session's lock release, guaranteeing a first-attempt
        # pg_try_advisory_lock failure. On timeout the close moves to a
        # background task that terminates the conn.
        #
        # This ALSO rebuilds MaintenanceLeader's other dedicated connections
        # (_leader_monitor_conn, _cron_conn) as a side effect: re-election
        # (triggered by leader_conn becoming None while is_leader is still
        # set) reopens both through the same leader_conn_factory before
        # re-setting is_leader — see leader.py's _election_loop `if got_lock:`
        # branch. So a single SIGHUP rotates every leader-owned connection,
        # not just leader_conn, even though this function never touches
        # _leader_monitor_conn/_cron_conn directly.
        if deps.leader_conn_factory is not None and deps.leader_conn is not None:
            old_leader = deps.leader_conn
            try:
                await asyncio.wait_for(old_leader.close(), timeout=drain_timeout)
            except TimeoutError:
                logger.warning("conn-drain-timeout", label="leader", drain_timeout=drain_timeout)
                _terminate_conn_background(old_leader, "leader")
            except Exception as exc:
                logger.warning("conn-drain-error", label="leader", error=repr(exc))
            # Identity-guard: the close above suspends, and the election
            # watchdog may already have reopened leader_conn via the factory.
            # Nulling unconditionally would orphan the fresh (possibly
            # lock-holding) conn until GC.
            if deps.leader_conn is old_leader:
                deps.leader_conn = None
            reloaded.append("leader_conn")

        # ── Redis ──────────────────────────────────────────────────
        if deps.redis_client_factory is not None:
            try:
                old_redis = deps.redis_client
                new_redis = await asyncio.wait_for(
                    deps.redis_client_factory(), timeout=factory_timeout
                )
                deps.redis_client = new_redis
                if old_redis is not None:
                    _drain_old_redis(old_redis, drain_timeout)
                reloaded.append("redis_client")
            except Exception as exc:
                logger.warning(
                    "credential-reload-resource-failed",
                    kind="credential_reload_resource_failed",
                    resource="redis_client",
                    error=repr(exc),
                )
                failed.append("redis_client")

        logger.info(
            "credentials-reloaded",
            kind="credentials_reloaded",
            resources=reloaded,
            failed=failed,
            drain_timeout=drain_timeout,
        )
        return reloaded, failed


def _drain_old_pool(pool: asyncpg.Pool, label: str, drain_timeout: float) -> None:
    """Close an old pool in the background with a bounded drain timeout.

    On timeout the pool is *terminated* — ``close()`` waits for checked-out
    connections to be released, which a stuck holder can delay indefinitely,
    keeping old-credential sessions alive past the rotation point.
    ``terminate()`` kills them immediately.
    """

    async def _close() -> None:
        logger.info("pool-draining", pool=label, drain_timeout=drain_timeout)
        try:
            await asyncio.wait_for(pool.close(), timeout=drain_timeout)
        except TimeoutError:
            logger.warning(
                "pool-drain-timeout-terminating", pool=label, drain_timeout=drain_timeout
            )
            with suppress(Exception):
                pool.terminate()
        except Exception as exc:
            logger.warning("pool-drain-error", pool=label, error=repr(exc))

    _t = asyncio.create_task(_close())
    _drain_tasks.add(_t)
    _t.add_done_callback(_drain_tasks.discard)


def _drain_old_conn(conn: asyncpg.Connection, label: str, drain_timeout: float) -> None:
    """Close an old dedicated connection in the background; terminate on timeout."""

    async def _close() -> None:
        try:
            await asyncio.wait_for(conn.close(), timeout=drain_timeout)
        except TimeoutError:
            logger.warning(
                "conn-drain-timeout-terminating", label=label, drain_timeout=drain_timeout
            )
            with suppress(Exception):
                conn.terminate()
        except Exception as exc:
            logger.warning("conn-drain-error", label=label, error=repr(exc))

    _t = asyncio.create_task(_close())
    _drain_tasks.add(_t)
    _t.add_done_callback(_drain_tasks.discard)


def _terminate_conn_background(conn: asyncpg.Connection, label: str) -> None:
    """Terminate a connection whose graceful close already timed out."""

    async def _terminate() -> None:
        with suppress(Exception):
            conn.terminate()

    _t = asyncio.create_task(_terminate())
    _drain_tasks.add(_t)
    _t.add_done_callback(_drain_tasks.discard)


def _drain_old_redis(client: object, drain_timeout: float) -> None:
    """Close an old Redis client in the background."""

    async def _close() -> None:
        try:
            await asyncio.wait_for(client.aclose(), timeout=drain_timeout)  # type: ignore[attr-defined]  # Why: redis-py Redis exposes aclose(); object erasure boundary.
        except TimeoutError:
            logger.warning("redis-drain-timeout", drain_timeout=drain_timeout)
            with suppress(Exception):
                await client.aclose()  # type: ignore[attr-defined]
        except Exception as exc:
            logger.warning("redis-drain-error", error=repr(exc))

    _t = asyncio.create_task(_close())
    _drain_tasks.add(_t)
    _t.add_done_callback(_drain_tasks.discard)
