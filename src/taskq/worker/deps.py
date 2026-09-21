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
from datetime import datetime
from typing import TYPE_CHECKING, Final
from uuid import UUID

import asyncpg
import structlog

from taskq._close import (
    CLOSE_TIMEOUT_SECS,
    PUBLISH_DRAIN_TIMEOUT_SECS,
    close_conn_bounded,
    close_pool_bounded,
    close_provider_bounded,
    close_redis_bounded,
)
from taskq._dsn import dsn_host as _dsn_host
from taskq.auth import credential_provider_of
from taskq.connections import (
    ConnFactory,
    PoolFactory,
    RedisFactory,
    WorkerConnections,
    lock_budget_command_timeout_secs,
    statement_cache_kwargs,
)
from taskq.constants import wake_channel
from taskq.obs import get_logger, set_slot_pool_occupancy_source
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.worker._stall_tally import StallAttributionTally
from taskq.worker._watchdog import LoopLiveness
from taskq.worker.budget import compute_connection_budget
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.shutdown import ShutdownPhase

if TYPE_CHECKING:
    import redis.asyncio as redis_async

__all__ = [
    "POOL_INFRA_EXCEPTIONS",
    "WorkerDeps",
    "apply_keepalive_to_conn",
    "open_dedicated_conn",
    "open_worker_deps",
    "reload_credentials",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

POOL_INFRA_EXCEPTIONS: tuple[type[BaseException], ...] = (
    TimeoutError,
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    asyncpg.InternalClientError,
    OSError,
)
"""What a bounded pool operation failing for infrastructure reasons
raises, the shared classification for every direct-DSN acquire site:
the dispatch slot acquire and both readiness pings. The dispatch acquire
runs no queries, so any PostgresError there is connect-time
infrastructure, an acquire that must OPEN a fresh connection (a holder
reconnect after a drop, idle-expiry, or terminate) surfaces server-side
refusals as coded errors (InvalidPasswordError on a revoked static
credential, AdminShutdownError, CannotConnectNowError) that are not
PostgresConnectionError subclasses, and never a job outcome. The pings
apply the family to their whole bounded body, the acquire plus a fixed
literal ``SELECT 1``, a query with no caller input, so any PostgresError
from it is server-side infrastructure; readiness fails closed for every
member either way, and the dispatch acquire's own per-occurrence cause
is logged at its raise site.
"""

# Hold references to background drain tasks so they are not garbage-collected
# before completing. Cleared as each task finishes via done-callbacks.
_drain_tasks: set[asyncio.Task[None]] = set()

# TCP keepalive parameters
_TCP_KEEPIDLE = 30
_TCP_KEEPINTVL = 5
_TCP_KEEPCNT = 3

# No ``server_settings=`` on any TaskQ-built pool, deliberately. asyncpg
# rides every ``server_settings`` entry in the STARTUP PACKET, and a pooler
# that rejects unknown startup parameters (PgBouncer: "unsupported startup
# parameter: jit") fails the connect: with a single TASKQ_PG_DSN pointed
# at the pooler, every TaskQ-built boot connection dies there. The
# dispatcher pool previously carried ``jit = off`` this way; the guard it
# provided is available server-side without the pooler hazard:
# ``ALTER ROLE ... SET jit = off`` or ``?options=-c jit=off`` on the DSN
# (docs/guides/ops.md §"Database performance knobs"), and a per-claim
# ``SET LOCAL jit = off`` is structurally unavailable: the claim runs in
# autocommit (one atomic UPDATE ... RETURNING), so there is no transaction
# for a SET LOCAL to scope to. The measured win never needed the guard
# anyway: the dispatch statement's estimate cascade is fixed at the source
# (perf-evidence-dispatch.md: the depth oracle passes with JIT enabled on
# a plain connection), and that oracle
# (tests/test_dispatch_backlog_depth_bound.py) keeps re-proving it. The
# slot pool's inherited ``search_path``/``role`` (worker/_bootstrap.py) are
# the one deliberate exception: they are session state a LOOP-scope
# connection declared, must survive the release-time ``RESET ALL`` (which
# startup-packet values do; a post-connect ``SET`` does not), and ride the
# direct DSN only.

# ── Transaction-mode pooler hygiene (TASKQ_PG_IS_POOLED) ───────────────
#
# A worker whose DSN(s) route through a transaction-mode pooler (PgBouncer
# pool_mode=transaction) cannot keep asyncpg's per-connection prepared-
# statement cache: the pooler remaps server connections between statements,
# and a name cached on one lands on another where it was never prepared
# (SQLSTATE 26000 InvalidSQLStatementNameError, or 42P05
# DuplicatePreparedStatementError on the re-Prepare). Two consequences,
# both gated on the operator's TASKQ_PG_IS_POOLED declaration - a pooler
# speaks plain Postgres on the wire, so no client can detect it:
#
# 1. Every pool TaskQ builds passes statement_cache_size=0 and
#    max_cached_statement_lifetime=0 instead of the tuned pair: the
#    override lives in taskq.connections.statement_cache_kwargs, which is
#    the single resolver every TaskQ-built pool's create_pool call reads
#    through (the dispatcher, heartbeat, and worker role pools below, the
#    per-slot pool, the client pool, the admin UI pool). Bring-your-own
#    pools keep their own kwargs (the caller-owned doctrine).
# 2. The producer loop classifies the pooler-remap statement errors as
#    transient (taskq.worker._transient.is_transient_pg_error, pooled=...),
#    so a remap storm degrades a dispatch round to a warning and a retry
#    next tick instead of a loud dispatch-batch-error per tick.
#
# Both halves are belt-and-suspenders for each other: the hygiene makes
# the error impossible on TaskQ-built pools, and the classification covers
# shapes the hygiene cannot reach (a caller-owned pool behind a pooler
# that TaskQ was never able to re-kwarg). Leave the knob False when every
# DSN reaches Postgres directly - the tuned cache is the better default
# there, and 26000 stays a loud bug.


_ADMISSION_LOCK_BUDGET_FIELDS: Final[tuple[str, ...]] = (
    "token_bucket_lock_timeout_ms",
    "sliding_window_lock_timeout_ms",
)
"""The ``WorkerSettings`` field names of the two admission-path lock budgets
, listed once so the dispatcher pool's bound derivation cannot drift onto a
different spelling (the same listing doctrine as
``taskq.client._taskq._ENQUEUE_LOCK_BUDGET_FIELDS``)."""


def _admission_lock_budget_pairs(settings: WorkerSettings) -> list[tuple[float, float]]:
    """``(configured, shipped default)`` per admission lock budget, the
    input :func:`taskq.connections.lock_budget_command_timeout_secs`
    derives the dispatcher pool's per-query bound from. The defaults are
    read off the model's field metadata, never restated."""
    pairs: list[tuple[float, float]] = []
    fields = type(settings).get_fields()
    for field_name in _ADMISSION_LOCK_BUDGET_FIELDS:
        _field_type, field_info = fields[field_name]
        pairs.append((float(getattr(settings, field_name)), float(field_info.default)))
    return pairs


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
    keepalive policy as DSN-built ones, the worker owns this policy, not
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
    command_timeout: float | None = None,
) -> asyncpg.Connection:
    """Open a dedicated (non-pooled) asyncpg connection.

    If ``apply_keepalive`` is True, sets TCP keepalive
    on the underlying socket after the connection is established.
    If ``command_timeout`` is set, applies it so a stalled PG cannot
    hang the caller indefinitely.
    """
    if command_timeout is not None:
        conn = await asyncpg.connect(dsn, command_timeout=command_timeout, timeout=command_timeout)
    else:
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


@dataclass(slots=True, frozen=True)
class LeaderTerm:
    """One period of maintenance leadership, fenced and locally time-boxed.

    ``elected_at`` is the server clock instant the lease row was written
    with; it is the fence token every renewal and the resign carry, so a
    statement issued after a takeover cannot touch the successor's row.

    ``trusted_until`` is on this process's monotonic clock and is always
    earlier than the server's ``expires_at`` for the same attempt: it is
    measured from *before* the round trip and is one margin shorter, so
    this process stops acting as leader strictly before any peer may
    legally take over. Clock offset between the two machines is
    irrelevant, each side measures a duration from its own instant.
    """

    elected_at: datetime
    trusted_until: float


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
    # The INCREMENTAL AsyncExitStack from open_worker_deps, every
    # teardown registered after the deps shell opens (the dedicated-conn
    # / redis guards, the per-slot pool via _maybe_open_slot_pool, and
    # hot-reload swaps via reload_credentials). It unwinds BEFORE the
    # shell's own role pools at context exit; unwinding it directly
    # (mid-flight, bounded) detaches exactly those late registrations
    # and leaves the role pools serving. None before open_worker_deps
    # yields or after it exits.
    _exit_stack: AsyncExitStack | None = None
    # Credential providers declared by the TaskQ-owned factories this deps
    # was opened with (read via credential_provider_of, deduped by
    # identity). open_worker_deps closes each once, after every pool and
    # client built through it; _maybe_open_slot_pool consults the tuple so
    # the slot pool's provider is never closed twice. Empty for a DSN-built
    # worker.
    _credential_providers: tuple[object, ...] = ()
    is_leader: asyncio.Event = field(default_factory=asyncio.Event)
    # Set and cleared together with is_leader by the election loop. Kept
    # separate because is_leader is what the watchdog parks on and what the
    # gauge and health report expose, while the term is what leader-gated
    # work must consult per iteration (see `leading`).
    leader_term: LeaderTerm | None = None
    producer_stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    active_jobs: ActiveJobRegistry = field(default_factory=ActiveJobRegistry)
    shutdown_phase: ShutdownPhase = ShutdownPhase.NONE
    # Monotonic timestamp of shutdown initiation (orchestrate_shutdown
    # start or the ShutdownWatchdog observing shutdown_event, whichever
    # is first). Feeds the /ready shutdown_elapsed_seconds surface.
    shutdown_started_at: float | None = None
    heartbeat_failures: int = 0
    disowned_jobs: set[UUID] = field(default_factory=set[UUID])
    """Jobs this worker has finished with but could not record an outcome
    for, every attempt of the terminal write failed with an infra error,
    so the row is still ``running`` and locked to this worker. The
    heartbeat renews leases by ``locked_by_worker`` and excludes these ids,
    so the row's lease lapses on schedule and the reclaim sweep hands it
    back to the fleet, the recovery the terminal-write-failed log promises,
    which a lease renewed for the life of the process would never deliver.
    The consumer adds an id on the exhausted-write path, the heartbeat drops
    ids whose row is no longer this worker's running row, and the producer
    drops an id it claims again (the row is a live job of ours once more)."""
    progress_buffers: dict[UUID, _ProgressBuffer] = field(
        default_factory=dict[UUID, _ProgressBuffer]
    )
    redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; the type arg cannot be supplied without a stubs update.
    pending_publish_tasks: set[asyncio.Task[None]] = field(default_factory=set[asyncio.Task[None]])
    """In-flight fire-and-forget Redis progress-publish tasks, shared across all
    concurrently-running jobs on this worker (keyed implicitly by task identity,
    not job_id, a job may have zero or more in-flight publishes at once).
    Referenced here (rather than only on JobContext) so a task started by a
    short-lived JobContext outlives the context and cannot be garbage-collected
    mid-publish; see JobContext.progress(). Drained best-effort on shutdown."""
    notify_conn_factory: ConnFactory | None = None
    """Resolved factory that (re)builds ``notify_conn``, the user-supplied
    ``WorkerConnections.notify_conn_factory`` if set, else a closure over the
    DSN-based :func:`open_dedicated_conn` call, else ``None`` when
    ``notify_conn`` is a caller-owned concrete connection (nothing to
    rebuild). Used by :mod:`taskq.worker.notify`'s reconnect loop and by
    :func:`reload_credentials` so a dropped or expiring connection is always
    rebuilt through the same credential source it was opened with."""
    liveness: LoopLiveness = field(default_factory=LoopLiveness)
    """Per-loop liveness stamps for the in-worker watchdog (detector 2):
    interval-driven sibling loops tick once per iteration and the watchdog
    trips when any tracked loop goes stale. Gated loops must ``forget``
    their entry when their gate closes."""
    stall_tally: StallAttributionTally = field(default_factory=StallAttributionTally)
    """Rolling tally of this process's attributed event-loop stalls. The
    lag watchdog's daemon thread records into it from off-loop; the
    heartbeat loop reads it once per tick and merges the value into this
    worker's ``workers`` row metadata, which is the only thread-safe seam
    between the two: the tally object is the shared holder, never a
    reference into loop-owned state."""
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
    slot_pool: asyncpg.Pool | None = None
    """Worker-internal pool of per-job transaction connections, opened at
    bootstrap when a LOOP-scope ``asyncpg.Connection`` is registered and
    ``max_concurrency > 1``, one connection per consumer slot plus one
    reserved for the readiness probe, all on the direct DSN so no
    transaction boundary can be broken by transaction-mode pooling.
    ``None`` on every other shape (no LOOP-scope connection, or the
    single-slot ``max_concurrency == 1`` worker, which keeps using the
    registered connection directly). Always TaskQ-built and TaskQ-owned:
    deliberately not overridable through
    :class:`~taskq.connections.WorkerConnections`, unlike the three
    role pools above."""
    slot_pool_factory: PoolFactory | None = None
    """Factory that rebuilds ``slot_pool`` on :func:`reload_credentials`.
    Set when the pool is provider-backed (rebuildable with a fresh
    credential); ``None`` when it is DSN-built, static credentials,
    nothing to rotate, the same rule as the role pools, or when the
    per-slot path is inactive."""
    slot_pool_probe_task: asyncio.Task[tuple[bool, str | None]] | None = None
    """The in-flight (or most recently completed) slot-pool readiness
    probe, owned by :mod:`taskq.worker.health`'s single-flight ping.
    State lives here rather than in a module global so one worker's
    probe can never coalesce another worker's readiness request (two
    workers can share a process) and never outlive this deps object's
    event loop."""
    slot_pool_connection_init: Callable[[asyncpg.Connection], Awaitable[None]] | None = None
    """The per-connection init hook the CURRENT ``slot_pool``'s connections
    already carry from connect time, the registration-declared hook
    :func:`taskq.worker._bootstrap._maybe_open_slot_pool` threads into the
    pool factory's ``init=``. Recorded here (not on the pool: asyncpg pools
    are ``__slots__``-sealed) so the dispatch path never re-applies it ,
    the hook must run exactly once per physical connection. ``None`` means
    the pool's connections are NOT guaranteed to carry the registration's
    declared hook: either none was declared, or the pool was not opened by
    bootstrap (an injected/foreign pool), which is the shape dispatch
    repairs by applying the hook itself. Reload-safe: a credential reload
    rebuilds the pool through the same init-carrying factory, so the
    disposition survives the swap."""
    redis_client_factory: RedisFactory | None = None
    """Resolved factory that rebuilds ``redis_client`` on
    :func:`reload_credentials`. ``None`` when the client is caller-owned or
    DSN-constructed (static credentials, nothing to rotate)."""
    owns_notify_conn: bool = False
    """True when ``notify_conn`` is TaskQ-owned (DSN- or factory-built) and
    may be closed by TaskQ paths. False when caller-owned, the ownership
    contract ("TaskQ never closes caller-owned resources") forbids closing
    it even on error paths."""
    owns_leader_conn: bool = False
    """True when ``leader_conn`` is TaskQ-owned. Same ownership contract as
    :attr:`owns_notify_conn`."""
    reload_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    """Serializes :func:`reload_credentials`, a second invocation while one
    is in flight returns immediately instead of double-draining pools and
    leaking the loser's replacements."""
    notify_reconnect_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    """Serializes notify_conn reconnects, the health-check loop and
    :func:`reload_credentials` (via ``notify_reconnect_fn``) can both trigger
    a reconnect; without mutual exclusion both build a new conn and the
    loser's LISTEN-registered conn leaks."""
    drain_failures: int = 0
    """Count of jobs that reached a non-success terminal state during
    until-idle drain mode. Incremented by di_consumer_loop when
    dispatch_one_job returns ``"failed"``, the only AttemptOutcome value
    that indicates a terminal failure. ``"cancelled"`` propagates as
    CancelledError (not caught by the consumer's ``except Exception``),
    so it never reaches the increment. ``"scheduled"`` (snooze/retry) is
    excluded because a retried job is not a drain failure. The increment
    also fires on the exception path (when dispatch_one_job raises),
    counting unhandled errors as failures.

    Read by the drain monitor to determine the exit code. The counter is
    incremented unconditionally in all modes, but is only read in
    until-idle mode, in non-idle mode it is never consulted."""

    def lead(self, term: LeaderTerm) -> None:
        """Take the leader role for *term*.

        The invariant: the flag is never set without a term, and clearing
        (``stop_leading``) always drops both, no path can leave a half-set
        role behind. A renewal does NOT move the pair; it replaces the term
        in place (the election loop assigns ``leader_term`` directly), which
        keeps the flag set while the term's trust window rolls forward.
        """
        self.leader_term = term
        self.is_leader.set()

    def stop_leading(self) -> None:
        """Give up the leader role."""
        self.is_leader.clear()
        self.leader_term = None

    def leading(self) -> bool:
        """Whether leader-gated work may run in THIS iteration.

        Every leader-gated loop consults this per iteration rather than the
        bare ``is_leader`` event: a process suspended between two iterations
        (SIGSTOP, a long GC pause, a partitioned event loop) resumes with the
        event still set and its lease long since taken over by a peer. The
        monotonic trust window is what makes that resumption a no-op.

        The event remains the authority on the role; the term only narrows
        how long that authority is trusted between renewals. A role held
        without a term is therefore led, not refused, an embedder that
        drives ``is_leader`` itself has taken on the coordination the term
        would otherwise bound, and silently declining to do the maintenance
        work would strand every sweep with nothing in any log to say why.
        """
        if not self.is_leader.is_set():
            return False
        term = self.leader_term
        return term is None or asyncio.get_running_loop().time() < term.trusted_until

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
    leader_conn → open redis_client.  Uses two
    :class:`~contextlib.AsyncExitStack` instances so that a failure
    during step N closes steps 1..N-1 before the exception propagates:
    the base stack owns the role pools (their lifecycle is this
    context), the incremental stack, exposed as ``deps._exit_stack`` ,
    owns every teardown registered after the deps shell exists
    (dedicated-conn / redis guards, the per-slot pool, hot-reload
    swaps).  Teardown is LIFO across both: at context exit the
    incremental stack unwinds first (late registrations close before
    the role pools), and unwinding ``deps._exit_stack`` on its own
    detaches only those late registrations, the bounded,
    graceful-window-then-terminate surface a mid-flight caller can
    drive without closing the role pools still serving in-flight
    dispatches.

    ``connections`` provides per-role overrides, pre-constructed,
    caller-owned resources or zero-arg async factories, replacing the
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
            raise ValueError("pg_dsn_direct is None, was WorkerSettings.load() called?")
        direct_dsn = str(settings.pg_dsn_direct)
    if _needs_pg_dsn(conns, for_direct=False):
        if settings.pg_dsn_pooled is None:
            raise ValueError("pg_dsn_pooled is None, was WorkerSettings.load() called?")
        pooled_dsn = str(settings.pg_dsn_pooled)

    # Fail fast: a caller-owned leader_conn with no factory and no direct
    # DSN can never be rebuilt after a drop, the election watchdog would
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
            "pg_dsn_direct, a dropped leader connection could never be "
            "rebuilt. Provide leader_conn_factory (or configure pg_dsn_direct)."
        )

    # Track ownership of dedicated connections so the LIFO teardown guards
    # only close TaskQ-owned conns (caller-owned conns are left alone).
    owns_notify = conns.notify_conn is None  # DSN or factory → TaskQ-owned
    owns_leader = conns.leader_conn is None

    # Credential providers the TaskQ-owned factories were built over:
    # collected up front, deduped by identity (one provider may serve both
    # the PG and the Redis role), and closed ONCE each, after every pool
    # and client built through them, never while a connection that
    # authenticates through the provider could still be opened. A factory
    # built some other way declares no provider (credential_provider_of
    # answers None), and the caller who built one keeps owning it.
    providers: list[object] = []
    for factory in (
        conns.dispatcher_pool_factory,
        conns.heartbeat_pool_factory,
        conns.worker_pool_factory,
        conns.notify_conn_factory,
        conns.leader_conn_factory,
        conns.redis_client_factory,
    ):
        if factory is None:
            continue
        provider = credential_provider_of(factory)
        if provider is not None and not any(p is provider for p in providers):
            providers.append(provider)

    # Two stacks, one LIFO sequence at teardown. ``base_stack`` owns the
    # open-sequence ROLE POOLS (dispatcher / heartbeat / worker): their
    # lifecycle is the open_worker_deps context itself, so they close
    # when the context exits, never on a manual unwind of
    # ``deps._exit_stack``. ``incremental_stack`` (exposed as
    # ``deps._exit_stack``) owns every teardown registered AFTER the
    # deps shell exists: the dedicated-conn / redis guards below, the
    # per-slot pool (``_maybe_open_slot_pool``), and every hot-reload
    # swap (``reload_credentials``), the detachable surface a caller
    # can unwind mid-flight (bounded, graceful-window-then-terminate)
    # without tearing down the role pools still serving in-flight
    # dispatches. At context exit the incremental stack unwinds FIRST
    # (it is entered last), then the base pools, exactly the LIFO
    # order the single-stack shape produced.
    incremental_stack = AsyncExitStack()
    async with AsyncExitStack() as base_stack, incremental_stack:
        # Pushed FIRST so they unwind LAST: every pool and client below is
        # built through these providers, so each provider's own resources
        # (the Entra ID providers' credential session) are released only
        # once nothing that authenticates through it can be opened again.
        # Bounded and never raising, a no-op for a provider with nothing
        # to release, the same teardown discipline taskq ui serve's
        # lifespan applies to the providers it loads.
        for _provider in providers:
            base_stack.push_async_callback(
                close_provider_bounded, _provider, "worker", CLOSE_TIMEOUT_SECS
            )
        # DSN-fallback factories, built inline with explicit kwargs so pyright
        # can trace types through ``asyncpg.create_pool`` (a ``**dict`` splat
        # would erase them). ``None`` when the DSN is unused (every role for
        # that DSN is overridden) or when the role itself is overridden.
        # Statement-cache values resolve through statement_cache_kwargs so
        # TASKQ_STATEMENT_CACHE_SIZE / TASKQ_MAX_CACHED_STATEMENT_LIFETIME
        # apply; the pair is still forwarded as explicit kwargs. Under
        # TASKQ_PG_IS_POOLED the resolver overrides the pair to 0/0 - the
        # transaction-mode pooler hygiene documented at the top of the
        # module.
        _stmt_kwargs = statement_cache_kwargs(settings)
        dispatcher_dsn_factory: PoolFactory | None = None
        heartbeat_dsn_factory: PoolFactory | None = None
        worker_dsn_factory: PoolFactory | None = None
        # The dispatcher pool's per-query bound is DERIVED, not read
        # directly: the admission-path rate-limit acquires run on this pool
        # (worker/_leader_sweeps.py) with server-side lock_timeout budgets
        # from settings (token_bucket_lock_timeout_ms /
        # sliding_window_lock_timeout_ms), and asyncpg enforces
        # command_timeout as its own client-side per-statement timer, an
        # admission budget widened past the floor would be silently
        # truncated by the pool's own timer before the server-side refusal
        # could ever fire. lock_budget_command_timeout_secs re-derives the
        # bound as max(floor, widest_widened_budget / share), the same
        # reconciliation the client pool applies to the enqueue budgets
        # (client/_taskq.py): at the shipped defaults (5000 ms budgets,
        # 5.0 s floor) the bound is exactly the configured value, so a
        # deployment that sets nothing keeps byte-identical behavior. The
        # leader/notify dedicated connections keep the CONFIGURED value:
        # no admission acquire runs on them, and the leader-loop staleness
        # invariant is defined against the configured timeout.
        dispatcher_pool_command_timeout = lock_budget_command_timeout_secs(
            _admission_lock_budget_pairs(settings),
            floor_secs=settings.dispatcher_command_timeout,
        )
        if direct_dsn is not None:
            _direct = direct_dsn
            _lifetime = settings.pool_max_inactive_lifetime

            async def _dispatcher_dsn_factory() -> asyncpg.Pool:
                pool = await asyncpg.create_pool(
                    dsn=_direct,
                    min_size=1,
                    max_size=settings.dispatcher_pool_size,
                    max_inactive_connection_lifetime=_lifetime,
                    command_timeout=dispatcher_pool_command_timeout,
                    statement_cache_size=_stmt_kwargs["statement_cache_size"],
                    max_cached_statement_lifetime=_stmt_kwargs["max_cached_statement_lifetime"],
                )
                assert pool is not None
                return pool

            async def _heartbeat_dsn_factory() -> asyncpg.Pool:
                pool = await asyncpg.create_pool(
                    dsn=_direct,
                    min_size=1,
                    max_size=settings.heartbeat_pool_size,
                    max_inactive_connection_lifetime=_lifetime,
                    command_timeout=settings.heartbeat_command_timeout,
                    statement_cache_size=_stmt_kwargs["statement_cache_size"],
                    max_cached_statement_lifetime=_stmt_kwargs["max_cached_statement_lifetime"],
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
                    statement_cache_size=_stmt_kwargs["statement_cache_size"],
                    max_cached_statement_lifetime=_stmt_kwargs["max_cached_statement_lifetime"],
                )
                assert pool is not None
                return pool

            worker_dsn_factory = _worker_dsn_factory

        # ── dispatcher_pool (pg_dsn_direct) ────────────────────────────
        dispatcher_pool = await _resolve_pool(
            conns.dispatcher_pool,
            conns.dispatcher_pool_factory,
            dispatcher_dsn_factory,
            base_stack,
            settings=settings,
            label="dispatcher",
            host=_dsn_host(direct_dsn) if direct_dsn else None,
        )

        # ── heartbeat_pool (pg_dsn_direct, heartbeat_command_timeout) ──
        heartbeat_pool = await _resolve_pool(
            conns.heartbeat_pool,
            conns.heartbeat_pool_factory,
            heartbeat_dsn_factory,
            base_stack,
            settings=settings,
            label="heartbeat",
            host=_dsn_host(direct_dsn) if direct_dsn else None,
        )

        # ── worker_pool (pg_dsn_pooled) ───────────────────────────────
        worker_pool = await _resolve_pool(
            conns.worker_pool,
            conns.worker_pool_factory,
            worker_dsn_factory,
            base_stack,
            settings=settings,
            label="worker",
            host=_dsn_host(pooled_dsn) if pooled_dsn else None,
        )

        # WorkerDeps is built BEFORE the dedicated connections open, with
        # the conn/redis fields filled in as each open step completes:
        # each TaskQ-owned dedicated conn registers its LIFO teardown
        # guard at the moment it is opened, the pools' push-at-open
        # discipline, so a failure in any LATER open step (the LISTEN
        # execute, the leader factory, the redis factory) unwinds the
        # stack with the guard already registered instead of leaking the
        # session. The guards read through ``deps`` at teardown time
        # (never a captured instance), which is why deps must exist this
        # early.
        deps = WorkerDeps(
            settings=settings,
            dispatcher_pool=dispatcher_pool,
            heartbeat_pool=heartbeat_pool,
            worker_pool=worker_pool,
            notify_conn=None,
            leader_conn=None,
            redis_client=None,
            notify_conn_factory=None,
            leader_conn_factory=None,
            # Reload (SIGHUP) only ever rebuilds via the user's own factory ,
            # a fresh credential fetch. The DSN-fallback path uses static
            # credentials baked into the DSN, so there is nothing to rotate;
            # only conns.*_factory (not the DSN closures above) is stored here.
            dispatcher_pool_factory=conns.dispatcher_pool_factory,
            heartbeat_pool_factory=conns.heartbeat_pool_factory,
            worker_pool_factory=conns.worker_pool_factory,
            redis_client_factory=conns.redis_client_factory,
            owns_notify_conn=owns_notify,
            owns_leader_conn=owns_leader,
            _credential_providers=tuple(providers),
            _exit_stack=incremental_stack,
        )

        # ── notify_conn (pg_dsn_direct, TCP keepalive) ────────────────
        # ``resolved_notify_factory`` is stored on WorkerDeps so notify.py's
        # reconnect loop and reload_credentials() rebuild the connection
        # through the same credential source it was originally opened with
        # , never falling back to a stale/absent DSN. ``None`` only when
        # notify_conn is caller-owned (nothing TaskQ can rebuild).
        resolved_notify_factory: ConnFactory | None
        notify_conn: asyncpg.Connection
        if conns.notify_conn is not None:
            notify_conn = conns.notify_conn  # caller-owned
            resolved_notify_factory = None
        elif conns.notify_conn_factory is not None:
            resolved_notify_factory = conns.notify_conn_factory
            # Why bounded: this open runs before any watchdog is armed, so
            # an unbounded factory call wedges worker startup with nothing
            # to detect or recover it. reload_factory_timeout is the SAME
            # bound the reload path and the notify reconnect loop apply to
            # every factory call, not a second mechanism. The DSN path
            # below needs none of this: open_dedicated_conn applies
            # asyncpg's own connect timeout.
            try:
                notify_conn = await asyncio.wait_for(
                    resolved_notify_factory(),
                    timeout=float(settings.reload_factory_timeout),
                )
            except TimeoutError as exc:
                # Bootstrap is fatal: a worker that cannot establish its
                # notify connection must refuse to start, naming the bound
                # and the credential source that never returned.
                raise TimeoutError(
                    f"notify connection factory did not return within "
                    f"{settings.reload_factory_timeout}s during worker bootstrap "
                    ", a worker that cannot establish its notify connection must "
                    "not boot. Check the credential provider behind "
                    "WorkerConnections.notify_conn_factory."
                ) from exc
            apply_keepalive_to_conn(notify_conn, label="notify")
        else:
            assert direct_dsn is not None  # guarded by _needs_pg_dsn
            _direct_notify = direct_dsn

            async def _notify_dsn_factory() -> asyncpg.Connection:
                return await open_dedicated_conn(
                    _direct_notify,
                    label="notify",
                    apply_keepalive=True,
                    command_timeout=settings.dispatcher_command_timeout,
                )

            resolved_notify_factory = _notify_dsn_factory
            notify_conn = await resolved_notify_factory()

        deps.notify_conn = notify_conn
        deps.notify_conn_factory = resolved_notify_factory

        # LIFO teardown guard, registered at open and BEFORE the LISTEN
        # execute: the guard is what closes this conn when the LISTEN
        # itself (or any later open step) fails. Reads through ``deps``
        # so a conn swapped in by reload_credentials is the one closed;
        # bounded close, then null the attr so nothing can touch the
        # closed conn after teardown. Caller-owned conns never get a
        # guard, the ownership contract.
        if owns_notify:

            async def _close_notify_conn() -> None:
                conn = deps.notify_conn
                if conn is not None:
                    await close_conn_bounded(conn, "notify", CLOSE_TIMEOUT_SECS)
                    deps.notify_conn = None

            incremental_stack.push_async_callback(_close_notify_conn)

        # Issue LISTEN so the connection is in subscription state. Why
        # bounded: the open runs before any watchdog is armed, and a
        # connection, factory-built, DSN-built, or caller-owned, can
        # complete its handshake and still black-hole on the execute.
        # notify_listener_setup_timeout is the SAME bound the notify
        # listener applies to every LISTEN during setup and reconnect ,
        # not a second mechanism.
        channel = wake_channel(settings.schema_name)
        try:
            await asyncio.wait_for(
                notify_conn.execute(f'LISTEN "{channel}"'),
                timeout=float(settings.notify_listener_setup_timeout),
            )
        except TimeoutError as exc:
            raise TimeoutError(
                f'notify LISTEN "{channel}" did not complete within '
                f"{settings.notify_listener_setup_timeout}s during worker "
                "bootstrap, a worker whose notify connection cannot enter "
                "subscription state must not boot. Check the notify connection "
                "and notify_listener_setup_timeout."
            ) from exc
        logger.info("notify-listen-issued", channel=channel, owns_notify=owns_notify)

        # ── leader_conn (pg_dsn_direct, TCP keepalive) ─────────────────
        resolved_leader_factory: ConnFactory | None
        leader_conn: asyncpg.Connection
        if conns.leader_conn is not None:
            leader_conn = conns.leader_conn  # caller-owned
            resolved_leader_factory = None
        elif conns.leader_conn_factory is not None:
            resolved_leader_factory = conns.leader_conn_factory
            # Why bounded: this open runs before any watchdog is armed, so
            # an unbounded factory call wedges worker startup with nothing
            # to detect or recover it. reload_factory_timeout is the SAME
            # bound the reload path, the bootstrap slot-pool open, and the
            # notify reconnect loop apply to every factory call, not a
            # second mechanism. The DSN path below needs none of this:
            # open_dedicated_conn applies asyncpg's own connect timeout.
            try:
                leader_conn = await asyncio.wait_for(
                    resolved_leader_factory(),
                    timeout=float(settings.reload_factory_timeout),
                )
            except TimeoutError as exc:
                # Bootstrap is fatal: a worker that cannot establish its
                # leader connection must refuse to start, naming the bound
                # and the credential source that never returned.
                raise TimeoutError(
                    f"leader connection factory did not return within "
                    f"{settings.reload_factory_timeout}s during worker bootstrap "
                    ", a worker that cannot establish its leader connection "
                    "must not boot. Check the credential provider behind "
                    "WorkerConnections.leader_conn_factory."
                ) from exc
            apply_keepalive_to_conn(leader_conn, label="leader")
        else:
            assert direct_dsn is not None  # guarded by _needs_pg_dsn
            _direct_leader = direct_dsn
            _leader_command_timeout = settings.dispatcher_command_timeout

            async def _leader_dsn_factory() -> asyncpg.Connection:
                # command_timeout is applied HERE, not at the leader.py call
                # sites: every leader connection (election conn, cron,
                # monitor) goes through this factory on a stock deployment,
                # so this is the only place the timeout cannot be bypassed.
                return await open_dedicated_conn(
                    _direct_leader,
                    label="leader",
                    apply_keepalive=True,
                    command_timeout=_leader_command_timeout,
                )

            resolved_leader_factory = _leader_dsn_factory
            leader_conn = await resolved_leader_factory()

        deps.leader_conn = leader_conn
        deps.leader_conn_factory = resolved_leader_factory

        # Same push-at-open guard as notify_conn: a failure in any later
        # open step (the redis factory) unwinds the stack with this guard
        # already registered.
        if owns_leader:

            async def _close_leader_conn() -> None:
                conn = deps.leader_conn
                if conn is not None:
                    await close_conn_bounded(conn, "leader", CLOSE_TIMEOUT_SECS)
                    deps.leader_conn = None

            incremental_stack.push_async_callback(_close_leader_conn)

        # ── redis_client ───────────────────────────────────────────────
        redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; the type arg cannot be supplied without a stubs update.
        owns_redis = False
        if conns.redis_client is not None:
            redis_client = conns.redis_client  # caller-owned
        elif conns.redis_client_factory is not None:
            # Why bounded: this open runs before any watchdog is armed, so
            # an unbounded factory call wedges worker startup with nothing
            # to detect or recover it. reload_factory_timeout is the SAME
            # bound the notify and leader factory opens above and the
            # reload path apply to every factory call, not a second
            # mechanism. The redis_url path below needs none of this:
            # from_url is lazy (no network round trip at construction).
            try:
                redis_client = await asyncio.wait_for(
                    conns.redis_client_factory(),
                    timeout=float(settings.reload_factory_timeout),
                )
            except TimeoutError as exc:
                # Bootstrap is fatal: a worker wired with a redis factory
                # must refuse to start when the factory cannot deliver a
                # client, naming the bound and the credential source that
                # never returned.
                raise TimeoutError(
                    f"redis client factory did not return within "
                    f"{settings.reload_factory_timeout}s during worker bootstrap "
                    ", a worker wired with a redis client factory must not "
                    "boot with no client. Check the credential provider behind "
                    "WorkerConnections.redis_client_factory."
                ) from exc
            owns_redis = True
        elif settings.redis_url is not None:
            import redis.asyncio as redis_async  # type: ignore[no-redef]  # Why: runtime import guarded by settings.redis_url; TYPE_CHECKING import is for annotations only

            redis_client = redis_async.from_url(
                str(settings.redis_url),
                decode_responses=False,
            )
            owns_redis = True
        deps.redis_client = redis_client

        # Why here: TASKQ_RELOAD_INTERVAL / SIGHUP only rotate resources that
        # have a factory on deps, and the DSN fallbacks above are stored for
        # notify/leader only, they reconnect with the SAME static DSN
        # credential. A worker configured to rotate on a schedule but with no
        # credential provider wired in therefore rotates nothing while logging
        # a healthy-looking "credentials-reloaded". Say so once, at startup.
        if settings.reload_interval is not None and not any(
            (
                conns.dispatcher_pool_factory,
                conns.heartbeat_pool_factory,
                conns.worker_pool_factory,
                conns.notify_conn_factory,
                conns.leader_conn_factory,
                conns.redis_client_factory,
            )
        ):
            logger.warning(
                "reload-interval-set-without-credential-provider",
                kind="reload_interval_without_provider",
                reload_interval=settings.reload_interval,
                reason=(
                    "every connection is DSN-built, so a scheduled reload rebuilds "
                    "connections with the same static credential"
                ),
                remedy=(
                    "set TASKQ_PG_CREDENTIAL_PROVIDER (and TASKQ_REDIS_CREDENTIAL_PROVIDER) "
                    "or pass connections=WorkerConnections(...); see "
                    "docs/guides/managed-identities.md"
                ),
            )

        # LIFO teardown guards for the TaskQ-owned redis client. The
        # dedicated conns above already registered theirs at open time;
        # no open step can fail between the redis build and this push,
        # so the redis guards need no window of their own.
        # orchestrate_shutdown closes and nulls a TaskQ-owned leader_conn early
        # (to release the advisory lock before the SIGTERM budget expires), and
        # reload_credentials swaps conns/pools mid-run. Every guard reads
        # through ``deps`` at teardown time (never a captured instance), so
        # they close whatever is current and never double-close.
        # Caller-owned resources are never closed here.
        if owns_redis and redis_client is not None:

            async def _close_redis_client() -> None:
                # Closes through ``deps.redis_client``, NOT the startup
                # instance, so a client swapped in by reload_credentials is
                # the one closed here (reload drains the old one itself).
                # Mirrors the notify/leader guards above: bounded close, then
                # null the attr so nothing can touch the closed client after
                # teardown.
                client = deps.redis_client
                if client is not None:
                    await close_redis_bounded(client, "worker", CLOSE_TIMEOUT_SECS)
                    deps.redis_client = None

            incremental_stack.push_async_callback(_close_redis_client)

            async def _drain_pending_publishes() -> None:
                """Give in-flight fire-and-forget progress publishes a bounded
                window to finish before the Redis client closes underneath them."""
                if deps.pending_publish_tasks:
                    await asyncio.wait(
                        deps.pending_publish_tasks, timeout=PUBLISH_DRAIN_TIMEOUT_SECS
                    )

            incremental_stack.push_async_callback(_drain_pending_publishes)

        try:
            yield deps
        finally:
            # After exit the stack is closed, a late reload_credentials call
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
    settings: WorkerSettings,
    label: str,
    host: str | None = None,
) -> asyncpg.Pool:
    """Resolve a pool from concrete / user factory / DSN factory and register teardown.

    * ``concrete``, caller-owned; returned as-is, never closed by TaskQ.
    * ``factory``, user-provided zero-arg async factory; TaskQ-owned.
    * ``dsn_factory``, TaskQ-built DSN fallback factory; TaskQ-owned.

    Exactly one of the three must be non-``None``; the caller ensures this
    by building ``dsn_factory`` only when the DSN is available and the role
    is not overridden. TaskQ-owned pools register a bounded-close callback
    (:func:`taskq._close.close_pool_bounded`) on ``stack`` for LIFO teardown.

    The user-factory call is bounded by ``settings.reload_factory_timeout``:
    it runs before any watchdog is armed, and the SAME bound already governs
    every other pool factory call (the reload path, the bootstrap slot-pool
    open). The DSN fallback needs none of this, asyncpg's own connect
    timeout bounds ``create_pool``.
    """
    if concrete is not None:
        logger.info("pool-using-provided", pool=label, ownership="caller")
        return concrete
    if factory is not None:
        try:
            pool = await asyncio.wait_for(factory(), timeout=float(settings.reload_factory_timeout))
        except TimeoutError as exc:
            # Bootstrap is fatal: a worker that cannot open a role pool
            # must refuse to start, naming the pool and the credential
            # source that never returned.
            raise TimeoutError(
                f"{label} pool factory did not return within "
                f"{settings.reload_factory_timeout}s during worker bootstrap "
                f", a worker that cannot open its {label} pool must not "
                "boot. Check the credential provider behind "
                f"WorkerConnections.{label}_pool_factory."
            ) from exc
    else:
        assert dsn_factory is not None, (
            f"{label} pool has no source, provide a concrete pool, factory, or DSN"
        )
        pool = await dsn_factory()

    async def _close_pool(p: asyncpg.Pool = pool, lbl: str = label) -> None:
        # Why default-arg binding: keeps this closure loop-safe, matching the
        # reload_credentials registration site (a late-bound capture inside
        # that loop would close the wrong pool).
        # Why module-global reads at call time: tests monkeypatch
        # close_pool_bounded / CLOSE_TIMEOUT_SECS as observation
        # and timeout-shrink seams.
        await close_pool_bounded(p, lbl, CLOSE_TIMEOUT_SECS)

    # Why a pushed callback instead of stack.enter_async_context(pool):
    # Pool.__aexit__ closes unbounded; the bounded helper above terminates
    # on timeout so a dead PG cannot wedge final teardown.
    stack.push_async_callback(_close_pool)
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

    No longer *required* for Postgres token refresh: the :mod:`taskq.auth`
    factories pass ``password=`` to asyncpg as an async callable, so every
    physical connection already authenticates with a freshly fetched
    credential. This remains the way to force a full pool rebuild - dropping
    sessions opened under a revoked credential, and picking up a **changed
    username**, which asyncpg resolves once per pool and cannot refresh per
    connection.

    For each factory-backed resource:
    1. Build a new resource by calling the factory (which fetches a fresh
       credential, AAD token, AWS IAM token, Vault dynamic creds), bounded
       by ``factory_timeout`` so a hung token endpoint cannot wedge the
       reload (and, via the coordinator, all future SIGHUPs).
    2. Atomically swap it onto ``deps``.
    3. Close the old resource in a background task with a bounded drain
       timeout, in-flight queries on old pool connections are given
       ``drain_timeout`` seconds to finish before the old pool is
       terminated.

    Resources that are caller-owned (no factory stored on ``deps``) are
    skipped, the caller is responsible for their lifecycle.

    ``notify_conn`` is rebuilt through the listener's
    ``reconnect_notify_conn`` helper, which re-issues LISTEN and
    re-registers callbacks. ``leader_conn`` is closed and nulled (not
    swapped): the leader election loop observes the None, stands down,
    and reopens through ``leader_conn_factory`` on its next tick,
    re-electing on the lease row.

    New pools are registered on ``deps._exit_stack`` (the ``AsyncExitStack``
    from ``open_worker_deps``) for LIFO teardown at shutdown. Old pools are
    closed in the background and do NOT sit on the stack.

    Each resource is reloaded independently, a factory failure for one
    (e.g. a transient credential-fetch error) is logged
    (``credential-reload-resource-failed``) and does NOT abort the
    remaining resources or raise out of this function; that resource
    keeps its current (not-yet-expired) pool/connection until the
    next SIGHUP.

    Concurrent invocations are serialized on ``deps.reload_lock``: a
    second call while one is in flight returns ``([], [])`` immediately
    rather than double-draining pools and leaking replacements.

    Returns ``(reloaded, failed)``, the resource labels that were and
    were not rotated. A non-empty ``failed`` list means a partial reload;
    the operator can send SIGHUP again to retry.

    This function is triggered by the SIGHUP handler installed by
    :func:`~taskq.worker.shutdown.install_signal_handlers` and run by the
    reload coordinator loop in :func:`~taskq.worker._bootstrap._main`.
    """
    stack = deps._exit_stack
    if stack is None:
        raise RuntimeError(
            "reload_credentials called outside of open_worker_deps, deps._exit_stack is None"
        )

    if deps.reload_lock.locked():
        logger.info("credential-reload-skipped", reason="reload-already-in-progress")
        return [], []

    async with deps.reload_lock:
        reloaded: list[str] = []
        failed: list[str] = []

        # ── Pools ──────────────────────────────────────────────────
        # Each pool is reloaded independently, a factory failure for one
        # (e.g. a transient credential-fetch error) is logged and does NOT
        # abort the remaining resources. Without this, a single flaky
        # provider call would silently leave later pools/conns on stale
        # credentials with no indication anything was skipped.
        for label, pool_attr, factory_attr in (
            ("dispatcher", "dispatcher_pool", "dispatcher_pool_factory"),
            ("heartbeat", "heartbeat_pool", "heartbeat_pool_factory"),
            ("worker", "worker_pool", "worker_pool_factory"),
            ("slot", "slot_pool", "slot_pool_factory"),
        ):
            factory: PoolFactory | None = getattr(deps, factory_attr)
            if factory is None:
                continue
            try:
                old_pool: asyncpg.Pool = getattr(deps, pool_attr)
                new_pool = await asyncio.wait_for(factory(), timeout=factory_timeout)

                async def _close_pool(p: asyncpg.Pool = new_pool, lbl: str = label) -> None:
                    # Why default-arg binding: this closure is defined inside
                    # the pool loop, a late-bound capture of new_pool/label
                    # would close the LAST iteration's pool N times and leak
                    # the rest.
                    await close_pool_bounded(p, lbl, CLOSE_TIMEOUT_SECS)

                # Same bounded-teardown contract as _resolve_pool: never an
                # unbounded enter_async_context close.
                stack.push_async_callback(_close_pool)
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

        # The slot-pool occupancy gauge reads a module-level source that
        # bootstrap pointed at the boot-time pool; a swap (or a skipped
        # slot reload) must leave it aimed at whatever pool is current.
        if deps.slot_pool is not None:
            set_slot_pool_occupancy_source(deps.slot_pool)

        # ── notify_conn ────────────────────────────────────────────
        # Caller-owned notify_conn has no factory, nothing to rotate, so
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
                    # Listener not started yet (or already stopped), swap directly.
                    old_notify = deps.notify_conn
                    new_notify = await asyncio.wait_for(
                        deps.notify_conn_factory(), timeout=factory_timeout
                    )
                    apply_keepalive_to_conn(new_notify, label="notify")
                    channel = wake_channel(deps.settings.schema_name)
                    # Why bounded: a freshly built conn can complete the
                    # factory handshake and still black-hole on the LISTEN
                    # execute. notify_listener_setup_timeout is the SAME
                    # bound the bootstrap open and the reconnect loop apply
                    # to the identical execute, not a second mechanism;
                    # exhaustion follows this path's failure style: the
                    # resource is logged and marked failed, the reload
                    # continues.
                    await asyncio.wait_for(
                        new_notify.execute(f'LISTEN "{channel}"'),
                        timeout=float(deps.settings.notify_listener_setup_timeout),
                    )
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
        # The leader election loop detects a dead leader_conn, stands down,
        # and reopens via _open_leader_conn (which uses
        # deps.leader_conn_factory when set). We trigger that path by
        # closing the current leader_conn, the loop reopens with a fresh
        # credential and re-elects itself on the lease row (the recorded
        # holder's arm admits its own row while that lease is live, so the
        # reload costs a re-election, not a lease lapse). This is the same
        # path as a PG connection drop, so it's well-tested.
        #
        # The old conn is closed inline (bounded) BEFORE nulling, a
        # background close would leave the re-election racing the old
        # session's teardown. On timeout the close moves to a background
        # task that terminates the conn.
        #
        # This ALSO rebuilds MaintenanceLeader's other dedicated connections
        # (_leader_monitor_conn, _cron_conn) as a side effect: re-election
        # (triggered by leader_conn becoming None while is_leader is still
        # set) reopens both through the same leader_conn_factory before
        # re-setting is_leader, see leader.py's _election_loop win branch.
        # So a single SIGHUP rotates every leader-owned connection, not
        # just leader_conn, even though this function never touches
        # _leader_monitor_conn/_cron_conn directly.
        if deps.leader_conn_factory is not None and deps.leader_conn is not None:
            old_leader = deps.leader_conn
            try:
                await asyncio.wait_for(old_leader.close(), timeout=drain_timeout)
            except TimeoutError:
                logger.warning("conn-drain-timeout", label="leader", drain_timeout=drain_timeout)
                _terminate_conn_background(old_leader)
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

    On timeout the pool is *terminated*, ``close()`` waits for checked-out
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


def _terminate_conn_background(conn: asyncpg.Connection) -> None:
    """Terminate a connection whose graceful close already timed out."""

    async def _terminate() -> None:
        with suppress(Exception):
            conn.terminate()

    _t = asyncio.create_task(_terminate())
    _drain_tasks.add(_t)
    _t.add_done_callback(_drain_tasks.discard)


def _drain_old_redis(client: object, drain_timeout: float) -> None:
    """Close an old Redis client in the background, bounded.

    Delegates to :func:`taskq._close.close_redis_bounded` rather than
    hand-rolling a close. The previous implementation retried ``aclose()``
    with no bound after the bounded attempt had already timed out:

        except TimeoutError:
            logger.warning("redis-drain-timeout", ...)
            with suppress(Exception):
                await client.aclose()      # unbounded

    ``suppress`` catches exceptions; it cannot stop a call that never returns.
    The first ``aclose()`` times out precisely BECAUSE the socket is wedged --
    an Azure Cache failover mid-rotation leaves a half-dead connection -- so
    the retry hangs on exactly the condition that caused the timeout. Because
    the task is fire-and-forget in a module-level set, that leaked one task
    plus one unclosed socket per credential rotation for the life of the
    process. A slow leak rather than a hang: it never blocked the reload or
    shutdown, which is what kept it invisible.

    The two sibling drains, ``_drain_old_pool`` and ``_drain_old_conn``, are
    also hand-rolled but call ``terminate()`` on timeout, which is bounded and
    non-blocking. Redis has no ``terminate()`` equivalent, which is why a retry
    was reached for here; the correct answer is to give up, which is what
    ``close_redis_bounded`` does.
    """

    async def _close() -> None:
        await close_redis_bounded(client, "reload-drain", drain_timeout)  # type: ignore[arg-type]  # Why: object erasure boundary; redis-py Redis satisfies the _AsyncCloseable protocol at runtime.

    _t = asyncio.create_task(_close())
    _drain_tasks.add(_t)
    _t.add_done_callback(_drain_tasks.discard)
