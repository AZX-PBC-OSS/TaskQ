"""Worker bootstrap: _main coroutine and process entry point.

The ``_main`` coroutine wires the full TaskGroup of long-lived siblings
(signal handlers, cron registration, pool setup, producer/consumer tasks).
``worker_main`` is the process entry point that runs ``_main`` under an
``asyncio.Runner``.

``_maybe_open_slot_pool`` opens the worker's per-slot transaction pool
when a LOOP-scope connection is registered and ``max_concurrency > 1``,
and announces the mode; ``_emit_sub_enqueue_startup_warnings`` checks
LOOP-scope connection resolution and warns about the PgBouncer
transaction-mode connection footgun.
``_emit_unconsumed_queue_startup_warnings`` warns, once, aggregated ,
when served actors declare queues outside the worker's consumed set,
or distinctly when the worker consumes no queues at all.
"""

import asyncio
import contextlib
import importlib
import math
import signal
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, cast

import asyncpg
import structlog

from taskq._close import (
    CLOSE_TIMEOUT_SECS,
    close_pool_bounded,
    close_provider_bounded,
    worst_case_teardown_tail,
)
from taskq._di import ProviderRegistry, Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._dsn import dsn_host as _dsn_host
from taskq.actor import ActorRef
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import list_actor_configs
from taskq.auth import (
    PgCredentialProvider,
    ReloadSchedule,
    credential_provider_of,
    make_pg_pool_factory,
    reload_schedule_of,
)
from taskq.backend._protocol import Backend, JobRow, ScheduleCreateArgs
from taskq.backend._records import parse_rowcount
from taskq.backend.clock import Clock, SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.connections import (
    PoolFactory,
    WorkerConnections,
    connection_init_hook,
    statement_cache_kwargs,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex for defence-in-depth schema validation at this SQL interpolation site, per architecture.md §8 Invariant 4
    TERMINAL_WRITE_BUDGET_SECS,  # Why: the release write's own budget: one of the two numbers the release-park lease warning's remedy arithmetic names.
)
from taskq.context import CancelOrigin
from taskq.cron import (
    CronScheduleSpec,
    compute_next_fire_after,
)
from taskq.exceptions import DIError, MissingProvider
from taskq.obs import (
    ErrorReporter,
    get_meter,
    set_exception_message_max_chars,
    set_exception_redaction_enabled,
    set_otel_enabled,
    set_slot_pool_occupancy_source,
    set_worker_capacity_source,
    setup_logging,
)
from taskq.progress._flush import progress_flush_loop
from taskq.ratelimit._provider import register_rate_limit_registry, register_redis_pool
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.registry import registry as rl_registry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.worker._watchdog import (
    LoopLagWatchdog,
    ShutdownWatchdog,
    await_tracked_actor_reap,
    loop_watchdog_loop,
)
from taskq.worker.cancel import ActiveJobRegistry, make_cancel_controller
from taskq.worker.cron_loop import ActorFirePolicy
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.health import HealthServer, HealthTcpBindError, HealthUnixBindCollisionError
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.leader import MaintenanceLeader
from taskq.worker.notify import notify_listener_loop
from taskq.worker.queue_ops import list_queues
from taskq.worker.shutdown import ShutdownPhase, install_signal_handlers
from taskq.worker.startup import read_stored_queue_assignments, sync_actor_config

__all__ = [
    "_emit_sub_enqueue_startup_warnings",
    "_emit_unconsumed_queue_startup_warnings",
    "_main",
    "_maybe_open_slot_pool",
    "_slot_pool_factory",
    "worker_main",
    "worker_main_async",
]

_startup_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.worker.run.startup")

_sibling_crashes = get_meter().create_counter(
    "taskq.worker.sibling_crashes_total",
    unit="1",
    description="Sibling task exits by exception, labelled by loop.",
)


def _redis_extra_installed() -> bool:
    """Whether the ``[redis]`` extra is importable in this environment.

    Probes by importing: an "is the optional extra installed" check can
    only be answered reliably by the import itself. The parent package is
    imported first because
    both cheap alternatives lie: ``find_spec`` on a dotted name raises
    ``ModuleNotFoundError`` when the parent is absent (the very state this
    check exists to detect), and a ``redis.asyncio`` entry lingering in
    ``sys.modules`` answers "present" even when ``redis`` itself is
    unimportable.
    """
    try:
        importlib.import_module("redis")
        importlib.import_module("redis.asyncio")
    except ImportError:
        return False
    return True


def _validate_error_reporter_scope(registry: ProviderRegistry) -> None:
    """Refuse an ErrorReporter registered at a scope the hook cannot outlive.

    A terminal-failure hook runs after the actor's invocation scope has
    closed, so a TRANSIENT registration can never resolve. The per-dispatch
    guard degrades that shape to a skipped hook behind a window-gated
    WARNING; if the guard were the only check, a misregistered reporter
    would surface as a fleet that reports nothing while looking configured.
    Failing worker startup names the registration before any job exists,
    where fixing it costs nothing.
    """
    if not registry.has_provider(ErrorReporter):
        return
    entry = registry.get(ErrorReporter)
    if entry.scope in (Scope.PROCESS, Scope.THREAD, Scope.LOOP):
        return
    # A DI misregistration, typed like the ones registry.validate raises.
    raise DIError(
        f"ErrorReporter is registered at {entry.scope.name} scope; a terminal-failure "
        "hook outlives the actor invocation and must be PROCESS, THREAD or LOOP scoped"
    )


def _redis_configured(settings: WorkerSettings, registry: ProviderRegistry) -> bool:
    """Redis is available to rate limiters via TASKQ_REDIS_URL or DI.

    A user-supplied ``redis.asyncio.Redis`` provider is the documented
    alternative to ``TASKQ_REDIS_URL`` (``register_redis_pool`` defers to
    user registrations), so a registered provider satisfies the requirement
    even when the env var is unset.
    """
    if settings.redis_url is not None:
        return True
    if not _redis_extra_installed():
        return False
    import redis.asyncio as redis_async

    return registry.has_provider(redis_async.Redis)


def _emit_progress_fanout_unconfigured_warning(redis_client: object | None) -> None:
    """Warn once when this worker's progress publishes cannot fan out.

    The per-call publish in ``context.py`` keys on the resolved Redis
    client: when it is ``None`` the publish block is skipped silently
    (the documented contract), so live consumers fall back to 500 ms
    Postgres polling. Durable progress state still rides the Postgres
    flush, so this is a latency degradation, not data loss; the warning
    exists because every downstream consumer degrades loudly (SSE 503
    ``redis_not_configured``, client poll fallback) while the worker
    side, the one place that knows the fanout is off, said nothing.

    The predicate is *redis_client*, ``WorkerDeps.redis_client`` at the
    call site: the exact object the publish block tests. It is
    deliberately NOT ``_redis_configured``, the rate-limit gate's
    predicate: the two consumers resolve Redis through different
    machinery, so "configured" is not one predicate. Rate limiters
    resolve through DI (``register_redis_pool`` → ``get_redis_pool``),
    and a user-registered ``redis.asyncio.Redis`` provider serves them
    while never reaching ``WorkerDeps.redis_client``, so the gate's
    predicate would suppress this warning for a deployment whose fanout
    is actually off (a false negative). Conversely, a caller-owned
    ``WorkerConnections.redis_client`` or ``redis_client_factory`` wires
    the fanout with no URL and no DI provider, and the gate's predicate
    would warn on a working fanout (a false positive). ``deps`` is fully
    resolved before the call site runs, so the check is exact at the
    moment it fires.

    Placement: after the rate-limit gate, so a boot about to crash on
    Redis-backed limits does not also warn about progress fanout. Every
    crash before that line (settings validation, import failures, the
    schema-currency refusal, a failed deps open) leaves no worker
    serving jobs at all, so there is no live fanout to warn about; the
    call sits before the TaskGroup starts any actor, so no publish can
    precede the check.
    """
    if redis_client is not None:
        return
    _startup_log.warning(
        "progress-fanout-unconfigured",
        remedy=(
            "Set TASKQ_REDIS_URL or pass a Redis client via "
            "WorkerConnections (redis_client or redis_client_factory); "
            "without one, progress events reach consumers only through "
            "the durable Postgres flush, so live streams fall back to "
            "500 ms polling"
        ),
    )


def _served_redis_rate_limits(
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None,
    rl_registry: RateLimitRegistry,
) -> list[str]:
    """Names of redis-backed rate limits declared by this worker's actors.

    Scoped to served actors: the rate-limit registry is process-global and
    may carry limits for actors this worker never dispatches, a global
    scan would brick an unrelated worker. Walks both named limits
    (resolved against the registry) and :class:`KeyedRateLimitRef`
    declarations, whose concrete buckets materialize only at first acquire
    and are therefore invisible to a registry scan.

    ``rl_registry`` is the **resolved** registry for this worker (from
    :func:`_resolve_rl_registry`), NOT the module-level singleton, a
    user-supplied custom registry via DI must be scanned, otherwise
    Redis-backed limits on the custom registry are invisible at startup
    (false negative → per-dispatch crash) and singleton-only limits cause
    spurious startup errors (false positive).
    """
    if not actor_registry:
        return []
    offending: set[str] = set()
    for ref in actor_registry.values():
        for limit in ref.rate_limits:
            if isinstance(limit, str):
                prim = rl_registry.rate_limits.get(limit)
                if prim is not None and prim.backend == "redis":
                    offending.add(limit)
            elif isinstance(limit, KeyedRateLimitRef):
                if limit.backend == "redis":
                    offending.add(limit.base_name)
            else:
                if limit.backend == "redis":
                    offending.add(limit.name)
    return sorted(offending)


def _emit_sub_enqueue_startup_warnings(
    loop_scope: LoopScope,
    settings: WorkerSettings,
    actor_registry: Mapping[str, ActorRef[Any, Any]],
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Emit startup warnings for sub-enqueue connection resolution.

    Two checks:

    1. No LOOP-scope ``asyncpg.Connection`` provider registered → warn
       that ``ctx.jobs.enqueue`` will use autonomous commit ().
       Mutually exclusive with 2 (early return).
    2. LOOP-scope conn registered but DSNs differ → warn about the
       PgBouncer transaction-mode footgun (). Can fire alongside the
       per-slot mode announcement from :func:`_maybe_open_slot_pool`.
    """
    resolved = loop_scope.resolved_cache()
    has_loop_conn = resolved.get(asyncpg.Connection) is not None

    if not has_loop_conn:
        log.warning(
            "sub_enqueue_autonomous_fallback",
            actors=sorted(ref.name for ref in actor_registry.values()),
            note=(
                "no LOOP-scope asyncpg.Connection provider is "
                "registered; ctx.jobs.enqueue will use autonomous "
                "commit via worker_pool. Register an asyncpg.Connection "
                "at Scope.LOOP to activate transactional consume, "
                "registering an asyncpg.Pool instead does NOT activate "
                "it: the transactional path keys on a Connection, so a "
                "Pool-only registration silently keeps this autonomous "
                "fallback in force (transactional consume disabled "
                "without a failure anywhere)."
            ),
        )
        return

    pooled = settings.resolved_pg_dsn_pooled
    direct = settings.resolved_pg_dsn_direct
    if pooled != direct:
        log.warning(
            "loop_scope_conn_dsn_mismatch",
            pooled_host=_dsn_host(pooled),
            direct_host=_dsn_host(direct),
            note=(
                "a LOOP-scope asyncpg.Connection provider is "
                "registered, but pg_dsn_pooled and pg_dsn_direct "
                "differ. If worker_pool routes through PgBouncer "
                "in transaction mode, transaction boundaries will "
                "break silently. Set pg_dsn_pooled = pg_dsn_direct "
                "for workers that use LOOP-scope connections, or "
                "ensure both DSNs target the same direct PG "
                "endpoint."
            ),
        )


def _caller_supplied_pg_pools(conns: WorkerConnections | None) -> bool:
    """Whether any PG pool role is caller-supplied via WorkerConnections.

    The slot pool does not read WorkerConnections, so a fleet whose
    credential story lives entirely in its own pool objects needs to
    know that at startup, this predicate drives that warning. Scoped
    to the pool roles (not has_any(), which also covers dedicated
    connections and Redis): notify/leader conns carry no pool-credential
    story.
    """
    if conns is None:
        return False
    return (
        conns.dispatcher_pool is not None
        or conns.dispatcher_pool_factory is not None
        or conns.heartbeat_pool is not None
        or conns.heartbeat_pool_factory is not None
        or conns.worker_pool is not None
        or conns.worker_pool_factory is not None
    )


def _slot_pool_factory(
    settings: WorkerSettings,
    pg_credential_provider: PgCredentialProvider | None,
    session_settings: Mapping[str, str] | None = None,
    init: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
) -> PoolFactory:
    """Build the factory for the worker's per-slot transaction pool.

    Always TaskQ-built on the direct DSN, a pool routed through
    transaction-mode pooling would break every transaction boundary,
    which is what the ``loop_scope_conn_dsn_mismatch`` warning guards.
    Sized ``max_concurrency + 1`` with ``min_size == max_size`` so the
    pool is fully warmed at creation: one connection per consumer slot
    plus one reserved for the readiness probe, and connection
    establishment (plus, on a managed-identity deployment, a credential
    fetch) never lands inside the dispatch hot path. The sizing is
    captured at build time: ``max_concurrency`` is a boot-only setting
    (no reload path re-reads settings), so a rebuilt pool keeps its
    boot-time size across credential rotations.

    Provider-backed when *pg_credential_provider* is given, the
    documented managed-identity path, so every physical connection
    authenticates with a freshly fetched token and a SIGHUP rebuild
    rotates a username-bearing pair. DSN-built otherwise, like the role
    pools when the caller supplies none.

    *session_settings* are startup GUCs applied to every connection the
    pool opens, the registered connection's session state, carried
    across by :func:`_maybe_open_slot_pool`. They belong at build time
    rather than as a post-build mutation for two reasons that both
    matter in production: the pool's warm connections are opened during
    the build, so settings applied afterwards would only take effect on
    connections re-established lazily inside ``acquire()``, putting
    connection establishment, and a managed-identity credential fetch,
    back into the dispatch path the warm sizing exists to keep clear;
    and the factory is what a SIGHUP credential rebuild re-invokes, so a
    mutation applied to one pool instance is silently lost on the next
    rotation.

    *init* is the per-connection hook the LOOP-scope connection's
    registration declared (see
    :func:`taskq.connections.with_connection_init`), forwarded verbatim
    to ``asyncpg.create_pool`` / :func:`~taskq.auth.make_pg_pool_factory`
    so it runs once per physical slot connection, the channel type
    codecs reach slot connections through. It is build-time state for
    the same two reasons as *session_settings*, with one more of its
    own: a codec applied after the build reaches only lazily opened
    connections, so warm and cold connections would decode the same
    type DIFFERENTLY within one pool.
    """
    direct = str(settings.resolved_pg_dsn_direct)
    size = settings.max_concurrency + 1
    lifetime = settings.pool_max_inactive_lifetime
    command_timeout = settings.dispatcher_command_timeout
    # Both branches pass the same statement-cache pair, resolved from
    # settings so TASKQ_STATEMENT_CACHE_SIZE / TASKQ_MAX_CACHED_STATEMENT_LIFETIME
    # apply. Forwarded as explicit kwargs (not splatted) so pyright can trace
    # types through create_pool / make_pg_pool_factory.
    stmt_kwargs = statement_cache_kwargs(settings)
    # Absent rather than empty when there is nothing to carry: the driver
    # treats no mapping and an empty one alike, and a worker with nothing
    # to inherit builds exactly the pool it always built.
    inherited = dict(session_settings) if session_settings else None
    if pg_credential_provider is not None:
        # The slot pool is rebuilt by the same reload coordinator as the
        # role pools, on the same cadence: the operator's interval, else
        # the lease it is granted (the coordinator composes every factory's
        # schedule, see _worker_reload_schedule).
        schedule = ReloadSchedule(configured=settings.reload_interval)
        if inherited is None and init is None:
            return make_pg_pool_factory(
                direct,
                pg_credential_provider,
                min_size=size,
                max_size=size,
                max_inactive_connection_lifetime=lifetime,
                command_timeout=command_timeout,
                statement_cache_size=stmt_kwargs["statement_cache_size"],
                max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
                reload_schedule=schedule,
            )
        # Explicit kwargs, never splatted (pyright traces the types
        # through); make_pg_pool_factory treats a None hook as absent, so
        # inheriting only one of the pair needs no third call site.
        return make_pg_pool_factory(
            direct,
            pg_credential_provider,
            min_size=size,
            max_size=size,
            max_inactive_connection_lifetime=lifetime,
            command_timeout=command_timeout,
            statement_cache_size=stmt_kwargs["statement_cache_size"],
            max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
            server_settings=inherited,
            init=init,
            reload_schedule=schedule,
        )

    async def _dsn_slot_pool_factory() -> asyncpg.Pool:
        pool = await asyncpg.create_pool(
            dsn=direct,
            min_size=size,
            max_size=size,
            max_inactive_connection_lifetime=lifetime,
            command_timeout=command_timeout,
            statement_cache_size=stmt_kwargs["statement_cache_size"],
            max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
            server_settings=inherited,
            init=init,  # pyright: ignore[reportArgumentType]  # Why: asyncpg-stubs types init as CoroutineType-returning (_InitCallback); the codebase-wide hook contract (make_pg_pool_factory, with_connection_init) is Awaitable-returning, and asyncpg awaits the result either way at runtime.
        )
        assert pool is not None
        return pool

    return _dsn_slot_pool_factory


# The session state a slot connection must match, paired with the query
# that reads its live value. ``search_path`` decides which schema an
# actor's unqualified table references resolve against; ``role`` decides
# which RLS policies apply to every one of its statements. Both are
# readable back from the server and both are settable as startup GUCs, so
# both round-trip onto a pool the worker builds. Values are read from the
# live session rather than the driver's connect-time parameters because
# an application configures these as often by a post-connect ``SET``, on
# an ``init`` hook, or right after ``connect()``, as by a connect
# keyword, and a connect-time read sees nothing at all in that case.
_INHERITED_SESSION_STATE: tuple[tuple[str, str], ...] = (
    ("search_path", "SHOW search_path"),
    ("role", "SELECT current_setting('role')"),
)


async def _registered_session_state(
    registered: object,
    settings: WorkerSettings,
    log: structlog.stdlib.BoundLogger,
) -> dict[str, str]:
    """Read the live session state a slot connection has to reproduce.

    At ``max_concurrency == 1`` the actor receives the LOOP-registered
    connection itself, so its ``SET ROLE``, ``search_path`` and any other
    session state apply by construction. Above that the actor runs on a
    slot connection instead, and a slot pool built from the bare direct
    DSN resolves unqualified names against a different ``search_path``
    and runs under a different role, the actor reads and writes the
    wrong schema, under the wrong RLS policy, with nothing raising.
    Carrying this across makes raising the concurrency knob a throughput
    change and nothing else.

    Every read is bounded and every failure is loud. A silent no-op here
    would be precisely the failure mode this exists to eliminate: the
    slot connections would differ from the registered one with no signal
    anywhere, which is indistinguishable from the bug. A state that
    cannot be determined is therefore warned about and left uninherited,
    never skipped quietly.
    """
    # Why the callable guard: a LOOP-scope registration is only nominally
    # an asyncpg.Connection, the boot path's own harnesses register
    # duck-typed stands-in, and one that cannot answer a query must
    # degrade to the server defaults rather than break boot.
    fetchval = cast(object, getattr(registered, "fetchval", None))
    if not callable(fetchval):
        log.warning(
            "slot-pool-registered-session-unreadable",
            note=(
                "the registered connection cannot be queried for its session "
                "state, so the per-slot pool opens its connections with the "
                "server's defaults. An actor's unqualified table references "
                "and its role may resolve differently above max_concurrency 1."
            ),
        )
        return {}
    read = cast(Callable[[str], Coroutine[Any, Any, object]], fetchval)
    state: dict[str, str] = {}
    for name, query in _INHERITED_SESSION_STATE:
        try:
            value = await asyncio.wait_for(read(query), timeout=settings.dispatcher_command_timeout)
        except Exception as exc:
            log.warning(
                "slot-pool-registered-session-unreadable",
                setting=name,
                error=repr(exc),
                note=(
                    "this session setting could not be read off the registered "
                    "connection, so the per-slot pool opens its connections "
                    "with the server's default for it. An actor's unqualified "
                    "table references or its role may resolve differently "
                    "above max_concurrency 1."
                ),
            )
            continue
        if isinstance(value, str) and value:
            state[name] = value
    return state


def _registered_connection_init_hook(
    di_registry: ProviderRegistry | None,
) -> Callable[[asyncpg.Connection], Awaitable[None]] | None:
    """The init hook the LOOP-scope connection registration declares, if any.

    Session state is read back off the live connection (see
    :func:`_registered_session_state`), but a type codec or an init hook
    cannot be: the driver seals per-connection state the moment
    ``connect()`` returns. The only registrations whose per-connection
    setup is recoverable are FACTORY registrations whose factory declares
    its hook, :func:`taskq.connections.with_connection_init`, or
    :func:`taskq.auth.make_dedicated_conn_factory` with a ``setup``. A
    value registration (an already-built connection) and a factory that
    declares nothing are equally opaque here; the caller warns on both
    rather than letting a codec diverge silently at max_concurrency > 1.
    """
    if di_registry is None or not di_registry.has_provider(asyncpg.Connection):
        return None
    entry = di_registry.get(asyncpg.Connection)
    if entry.kind != "factory":
        return None
    return connection_init_hook(entry.impl)


async def _maybe_open_slot_pool(
    loop_scope: LoopScope,
    settings: WorkerSettings,
    deps: WorkerDeps,
    *,
    factory: PoolFactory,
    pg_credential_provider: PgCredentialProvider | None,
    caller_supplied_pg_pools: bool,
    di_registry: ProviderRegistry | None = None,
    log: structlog.stdlib.BoundLogger,
) -> bool:
    """Open the per-slot transaction pool when the per-slot path activates.

    Activation is the sharing precondition the dispatch path would
    otherwise hit: a resolvable LOOP-scope ``asyncpg.Connection`` AND
    ``max_concurrency > 1``. On that shape the worker's transactional
    consume moves to per-slot connections, one transaction per
    connection, so concurrent slots can never nest savepoints on a
    shared one, and the slot connection shadows the LOOP-registered
    connection for each actor invocation, so an actor's own writes join
    its job's transaction and no two concurrent slots' actors ever
    interleave operations on one connection. Every other
    shape (no LOOP-scope connection, or the single-slot
    ``max_concurrency == 1`` worker) keeps today's behaviour and this
    function returns ``False`` without touching *deps*.

    The open is bounded by ``reload_factory_timeout``, building a
    fully-warmed pool means opening every connection (each a credential
    fetch on a managed-identity deployment), and an unbounded wait here
    would wedge boot. A worker that cannot open the pool fails to boot,
    loudly, naming the pool and the DSN host: the alternative is a
    worker that accepts jobs it cannot transact.

    The registered connection's session state is read first and the
    supplied *factory* rebuilt around it, so every connection the pool
    warms at build time already resolves unqualified names and RLS
    policies the way the application configured them. Applying the state
    to a pool already built would leave the warm connections carrying
    the server's defaults instead. Alongside the session state, an init
    hook DECLARED on the registration's factory (see
    :func:`_registered_connection_init_hook`) is threaded into the
    rebuild, so a codec the application installs per-connection decodes
    identically on every slot connection.

    What cannot be carried is said, not dropped: when the registration
    exposes no init hook (a raw ``register_value`` connection, or a
    factory that declares none), per-connection setup applied to the
    registered connection, ``set_type_codec`` registrations above all ,
    is absent on every slot connection with no error raised anywhere
    else. One boot-time warning names that boundary and the supported
    channel, so the divergence can never be silent.

    Also announces the mode (info), because the retired warning string
    is what runbooks searched for and the mode must stay confirmable
    from the logs, the mode signal fires exactly when the pool opened,
    never as a predicate guess.
    """
    resolved = loop_scope.resolved_cache()
    registered = resolved.get(asyncpg.Connection)
    if registered is None or settings.max_concurrency <= 1:
        return False

    host = _dsn_host(settings.resolved_pg_dsn_direct)
    # Why: bootstrap owns deps and its exit-stack lifecycle; _main calls this inside open_worker_deps.
    stack = deps._exit_stack  # pyright: ignore[reportPrivateUsage]
    if stack is None:
        raise RuntimeError("slot pool cannot be opened outside of open_worker_deps")

    # Read before the pool is built, never after: the warm connections are
    # opened by the build, so session state applied afterwards would reach
    # only connections re-established lazily inside acquire(). The factory
    # is rebuilt with the state rather than mutated, so the pool a SIGHUP
    # credential rotation rebuilds carries it too. Same argument, one
    # notch sharper, for the init hook: a codec applied after the build
    # would leave warm and cold connections decoding the same type
    # differently within one pool.
    session_state = await _registered_session_state(registered, settings, log)
    inherited_init = _registered_connection_init_hook(di_registry)
    if session_state or inherited_init is not None:
        factory = _slot_pool_factory(
            settings, pg_credential_provider, session_state, init=inherited_init
        )

    # The slot pool's provider, when it is NOT already tracked on deps (it
    # usually is: the documented worker path builds every role through the
    # same provider, which open_worker_deps closes after the role pools).
    # Pushed BEFORE the pool's teardown guard so LIFO unwinds the slot
    # pool first and the credential that built it second, and never while
    # the role pools a shared provider also serves are still open, which
    # is why a tracked provider is skipped rather than closed here.
    provider = credential_provider_of(factory)
    if (
        pg_credential_provider is not None
        and provider is not None
        and not any(p is provider for p in deps._credential_providers)
    ):
        stack.push_async_callback(close_provider_bounded, provider, "slot", CLOSE_TIMEOUT_SECS)
        deps._credential_providers = (*deps._credential_providers, provider)

    try:
        pool = await asyncio.wait_for(factory(), timeout=settings.reload_factory_timeout)
    except Exception as exc:
        raise RuntimeError(
            f"slot pool failed to open on host {host!r} within "
            f"{settings.reload_factory_timeout}s (sized "
            f"{settings.max_concurrency + 1} for {settings.max_concurrency} consumer "
            "slots plus the readiness probe), a worker that cannot open its "
            "transaction connections must not boot. Check the direct DSN and "
            "credentials."
        ) from exc

    if session_state or inherited_init is not None:
        log.info(
            "slot_pool_inherits_registered_session",
            kind="slot_pool_inherits_registered_session",
            server_settings=sorted(session_state),
            init_hook=inherited_init is not None,
            note=(
                "the per-slot transaction pool opens its connections with the "
                "session state read off the registered connection (and, when "
                "init_hook is true, the init hook the registration's factory "
                "declared), so an actor's unqualified table references, its "
                "role, and its per-connection type codecs resolve the same way "
                "at every concurrency."
            ),
        )
    if inherited_init is None:
        log.warning(
            "slot_pool_registered_setup_not_inherited",
            kind="slot_pool_registered_setup_not_inherited",
            note=(
                "type codecs or init hooks applied directly to the "
                "LOOP-registered connection (set_type_codec, or setup run "
                "after connect) live in the driver's per-connection state, "
                "which cannot be read back, the per-slot connections do NOT "
                "have them, and a query relying on one still succeeds while "
                "returning the driver's default representation, silently "
                "diverging above max_concurrency 1. Register the connection "
                "through a factory that declares its init hook, "
                "taskq.connections.with_connection_init(...), or "
                "taskq.auth.make_dedicated_conn_factory(..., setup=...), "
                "which applies the setup to the fresh connection itself "
                "(asyncpg.connect takes no setup parameter), and "
                "the worker replays the hook on every slot connection. "
                "Session state (search_path, role) is read back and inherited "
                "either way; this warning covers per-connection codecs and "
                "hooks only."
            ),
        )

    async def _close_slot_pool(p: asyncpg.Pool = pool) -> None:
        # Why default-arg binding and module-global reads at call time:
        # same loop-safety and monkeypatch seams as _resolve_pool's
        # teardown callback in taskq.worker.deps.
        await close_pool_bounded(p, "slot", CLOSE_TIMEOUT_SECS)

    stack.push_async_callback(_close_slot_pool)
    deps.slot_pool = pool
    # Why record the carried hook on deps: the pool's warm connections got
    # inherited_init at connect time (the factory's init=), and dispatch must
    # never re-apply it, exactly once per physical connection. asyncpg pools
    # are __slots__-sealed, so the disposition lives on deps next to the pool.
    deps.slot_pool_connection_init = inherited_init
    deps.slot_pool_factory = factory if pg_credential_provider is not None else None
    set_slot_pool_occupancy_source(pool)
    log.info(
        "transactional_consume_per_slot",
        kind="transactional_consume_per_slot",
        max_concurrency=settings.max_concurrency,
        slot_pool_size=settings.max_concurrency + 1,
        host=host,
        note=(
            "a LOOP-scope asyncpg.Connection is registered and max_concurrency > 1: "
            "transactional consume runs per-slot on a dedicated direct-DSN pool, "
            "one connection per consumer slot plus one reserved for the readiness "
            "probe, fully warmed at boot. Every job's actor receives its own slot "
            "connection, the connection that job's transaction runs on, so the "
            "actor's own writes, the terminal write, and transactional sub-enqueues "
            "all join one transaction per job, and concurrent slots can never "
            "interleave operations on one connection (asyncpg permits one "
            "operation per connection). The registered LOOP-scope connection is "
            "this mode's activation signal; it remains the transaction connection "
            "only on a max_concurrency=1 worker, the one shape whose writes "
            "inherit that connection's session state (SET ROLE, search_path, an "
            "RLS-driving GUC)."
        ),
    )
    if caller_supplied_pg_pools and pg_credential_provider is None:
        log.warning(
            "slot_pool_own_credentials",
            kind="slot_pool_own_credentials",
            host=host,
            note=(
                "caller-supplied WorkerConnections pools are in play, but the "
                "per-slot transaction pool does not read WorkerConnections, it is "
                "worker-internal and authenticates from the direct DSN (or "
                "pg_credential_provider, which is not set). A fleet whose credential "
                "story lives entirely in its own pool factories must pass "
                "pg_credential_provider to worker_main, or run the transactional "
                "actor on a max_concurrency=1 worker."
            ),
        )
    return True


def _emit_unconsumed_queue_startup_warnings(
    settings: WorkerSettings,
    actor_registry: Mapping[str, ActorRef[Any, Any]],
    log: structlog.stdlib.BoundLogger,
    stored_queues: Mapping[str, str] | None = None,
) -> None:
    """Emit at most one startup warning about served actors on queues this
    worker does not consume.

    The dispatch CTE unnests ``$1::text[]``, the worker's own
    ``settings.queues``, and claims only jobs whose queue matches
    (backend/_dispatch_sql.py), so such an actor's jobs enqueue
    successfully and then sit pending forever, no error, no log,
    anywhere. Decoration-time validation checks queue name format only
    (actor.py), so this bootstrap pass, where the worker holds both each
    served actor's queue assignment and its own consumed queues, is the
    first place the mismatch is knowable.

    *stored_queues* maps actor name → the queue assignment recorded in
    ``actor_config``, and wins over the ``@actor(queue=...)`` literal
    wherever a row exists. The stored assignment is the operator-owned
    one: it routes the cron leader's fires and every re-pended row, and
    it is what a queue move rewrites. Judging coverage on the literal
    instead makes the signal fire on the healthy rolling-deploy window
    of a move (literal and stored legitimately disagree there) while
    staying silent on the one state where routed work provably cannot be
    claimed, an actor moved onto a queue this worker does not consume,
    whose literal it still names. Actors with no stored row yet fall
    back to the literal, which is what the first boot will seed (this
    pass runs before ``sync_actor_config``, so a first-ever boot has
    none to read).

    Aggregated, not per-actor: one event whose ``actors`` field maps each
    affected actor name to the queue its work routes to (the shape
    documented in docs/guides/workers.md), with the distinct unconsumed
    queue names in ``queues``. Empty ``settings.queues`` is a different,
    unambiguous failure, a worker that dispatches nothing, and gets its
    own single event instead.
    """
    # Why: warning, never an error, heterogeneous fleets run different
    # workers consuming different queues while all serving the same actor
    # registry, and a sibling worker may legitimately consume any given
    # queue; only "no worker anywhere consumes it" is broken, which this
    # process cannot know. Must never fail or block startup.
    if not settings.queues:
        # Why: a distinct event, not the per-actor aggregate, an empty
        # TASKQ_QUEUES means this worker provably dispatches nothing, so
        # the aggregate's "another worker may legitimately consume the
        # queue" caveat is false text here, and per-actor noise would
        # bury the one line an operator needs. Fires even with an empty
        # registry: a worker consuming nothing is broken regardless.
        log.warning(
            "worker-consumes-no-queues",
            worker_queues=[],
            note=(
                "TASKQ_QUEUES resolved to an empty list, so this worker "
                "dispatches nothing: every job on every queue stays "
                "pending. Set TASKQ_QUEUES (or --queues) to the queues it "
                "should consume."
            ),
        )
        return

    consumed = set(settings.queues)
    routed = stored_queues or {}
    offending = [
        (ref.name, routed.get(ref.name, ref.queue))
        for ref in actor_registry.values()
        if routed.get(ref.name, ref.queue) not in consumed
    ]
    if not offending:
        return
    # Why: ONE aggregated event per boot, not one per actor, workgroup
    # deployments have every child import the full actor registry while
    # consuming its own queue subset, so per-actor warnings would fire
    # once per actor per child per boot: a warning storm on exactly the
    # healthy heterogeneous fleets this warning blesses, training
    # operators to filter it. The per-actor detail survives in the
    # structured fields so alerting on a specific actor still works:
    # ``actors`` maps name → routed queue (docs/guides/workers.md's
    # documented shape, two parallel name/queue lists could not express
    # who is on which queue), sorted for byte-stable event content.
    log.warning(
        "actors-on-unconsumed-queues",
        actors=dict(sorted(offending)),
        queues=sorted({queue for _name, queue in offending}),
        worker_queues=list(settings.queues),
        note=(
            "this worker serves these actors but never dispatches their "
            "jobs: the dispatch claim matches only the worker's own queues, "
            "so their jobs sit pending until a worker consuming each "
            "routed queue appears. Intended when another worker in the "
            "fleet consumes the queue; if none does, those jobs never run "
            ", add the queue to some worker's TASKQ_QUEUES / --queues."
        ),
    )


def _resolve_rl_registry(
    explicit: RateLimitRegistry | None,
    di_registry: ProviderRegistry,
) -> RateLimitRegistry:
    """Resolve this worker's ``RateLimitRegistry`` (documented order).

    1. An explicit ``rate_limit_registry=`` argument wins, but co-present
       with a DI ``RateLimitRegistry`` provider it raises ``TypeError``
       (ambiguous: bootstrap and dispatch would diverge).
    2. A ``RateLimitRegistry`` provider pre-registered in *di_registry* ,
       **value providers only, at ``Scope.LOOP``**: a factory/class provider
       would split-brain (bootstrap using one instance while LOOP-scope
       dispatch resolution produced another), so it fails fast with
       ``TypeError``.  A non-LOOP scope fails fast too, dispatch resolves
       the registry from the LOOP-scope cache, so any other scope would
       bootstrap against one instance while dispatch found none (silently
       disabling rate limiting).
    3. The module singleton (unchanged backwards-compatible default).

    Naming: ``_registry`` / *di_registry* is the DI ``ProviderRegistry``
    (container); the returned object is the ``RateLimitRegistry``
    (primitive store). They are unrelated despite the similar names.
    """
    if explicit is not None and di_registry.has_provider(RateLimitRegistry):
        raise TypeError(
            "rate_limit_registry= was passed explicitly AND a RateLimitRegistry "
            "provider is registered in di_registry, ambiguous configuration; "
            "pass one or the other, not both"
        )
    if explicit is not None:
        return explicit
    if di_registry.has_provider(RateLimitRegistry):
        entry = di_registry.get(RateLimitRegistry)
        if entry.kind != "value":
            raise TypeError(
                "RateLimitRegistry must be registered as a value provider "
                f"(register_value), got kind={entry.kind!r}, the worker must "
                "resolve one concrete instance at bootstrap"
            )
        if entry.scope is not Scope.LOOP:
            raise TypeError(
                "RateLimitRegistry value provider must be registered at "
                f"Scope.LOOP (got {entry.scope!r}), dispatch resolves the "
                "registry from the LOOP-scope cache; a non-LOOP registration "
                "bootstraps against one instance while dispatch finds none, "
                "silently disabling rate limiting"
            )
        return cast(RateLimitRegistry, entry.impl)
    return rl_registry


async def _revert_stale_auto_disable(
    deps: WorkerDeps,
    settings: WorkerSettings,
    spec: CronScheduleSpec,
) -> bool:
    """Re-enable a schedule row the cron loop auto-disabled, at registration.

    The ownership model (issue #342): ``cron_schedules.disabled_by`` records
    who disabled a row. ``'auto'`` is the cron loop's failure-count
    auto-disable, and the code re-declaring the schedule at startup proves the
    declaration is live again, so the boot reverts the disable (``enabled=true``,
    ``consecutive_failures=0``, ``last_fire_error=NULL``, ``disabled_by=NULL``):
    a transient partial-DB blip (fires fail, strike writes commit) must not
    permanently halt recurring work until a human intervenes. ``'operator'``
    is a deliberate disable (schedule handle, CLI, admin UI, actor
    deregistration) and is NEVER reverted by a boot, exactly the intent the
    create-only registration design guards.

    A disabled row with a NULL marker is read in two populations (issue #460).
    Rows disabled before the column existed were stamped ``'operator'`` by the
    backfill migration (``01.00.19_05``), so they land in the operator case
    above. A residual NULL-disabled row can then only come from an OLD pod
    during a mixed-version rolling deploy: the previous release's failure
    UPDATE writes ``enabled=false`` and cannot name this column. When such a
    row also carries that arm's fingerprint (``consecutive_failures`` at or
    past the auto-disable threshold, ``last_fire_error`` set), it IS an old
    pod's auto-disable -- the deploy's own transient state -- and the boot
    reverts it like an ``'auto'`` row. A NULL-disabled row without the
    fingerprint reads as an old pod's operator disable during the window and
    stays untouched.

    Only a code-owned, code-enabled spec may revert: an ``owner='operator'``
    spec merely ships the declaration, and a spec declared ``enabled=False``
    does not assert the schedule should run. Returns whether a row was
    re-enabled.
    """
    if spec.owner != "code" or not spec.enabled:
        return False
    async with deps.dispatcher_pool.acquire(timeout=settings.dispatcher_command_timeout) as conn:
        tag: str = await conn.execute(
            f'UPDATE "{settings.schema_name}".cron_schedules '  # noqa: S608  # Why: schema validated against _IDENT_RE at WorkerSettings load; asyncpg cannot bind identifiers, the values below are $-bound.
            f"SET enabled = true, consecutive_failures = 0, last_fire_error = NULL, "
            f"disabled_by = NULL "
            f"WHERE actor = $1 AND name = $2 AND enabled = false AND "
            f"(disabled_by = 'auto' OR (disabled_by IS NULL AND "
            f"consecutive_failures >= $3 AND last_fire_error IS NOT NULL))",
            spec.actor,
            spec.name,
            settings.cron_auto_disable_threshold,
        )
    return parse_rowcount(tag) > 0


async def _register_cron_schedules(
    backend: Backend,
    deps: WorkerDeps,
    settings: WorkerSettings,
    specs: Sequence[CronScheduleSpec],
) -> None:
    """Register every code-declared cron spec against the database.

    Create-first: a fresh spec inserts its row, seeded with the PG server
    clock (the cron tick's due-check and catch-up cutoff are server-side, so
    a Python-clock seed would shift the first fire by the app↔DB skew). On
    conflict the pass is otherwise write-free -- an operator's runtime change
    (disable, retime) must not be reverted by a redeploy -- except the one
    ownership-driven recovery in :func:`_revert_stale_auto_disable`: a stale
    auto-disable of a code-owned schedule (an ``'auto'`` marker, or the
    mixed-version deploy's unmarked fingerprint of one). Structural drift
    (cron_expr, timezone, dst_strategy the row kept despite a changed
    declaration) is warned, never written, by :func:`_warn_on_cron_drift`.
    """
    async with deps.dispatcher_pool.acquire(
        timeout=settings.dispatcher_command_timeout
    ) as _seed_conn:
        seed_now: datetime = await _seed_conn.fetchval("SELECT clock_timestamp()")
    for spec in specs:
        next_fires = compute_next_fire_after(
            spec.cron_expr,
            spec.timezone,
            seed_now,
            dst_strategy=spec.dst_strategy,
        )
        next_fire = next_fires[0]
        metadata: dict[str, object] = {}
        if spec.static_payload is not None:
            metadata["static_payload"] = spec.static_payload
        try:
            await backend.create_schedule(
                ScheduleCreateArgs(
                    actor=spec.actor,
                    cron_expr=spec.cron_expr,
                    timezone=spec.timezone,
                    next_fire_at=next_fire,
                    dst_strategy=spec.dst_strategy,
                    payload_factory=spec.payload_factory,
                    enabled=spec.enabled,
                    owner=spec.owner,
                    name=spec.name,
                    identity_key=spec.identity_key,
                    metadata=metadata,
                )
            )
        except asyncpg.UniqueViolationError:
            # Why: the (actor, name) UNIQUE constraint means a schedule
            # for this (actor, name) already exists; this registration
            # pass never modifies existing rows except the narrow
            # auto-disable recovery below.
            #
            # Create-only is deliberate -- an operator's runtime change
            # (disable, retime) must not be reverted by a redeploy. But
            # it also means that changing a @cron decorator's cron_expr,
            # timezone or dst_strategy in code deploys "successfully"
            # and silently keeps the OLD cadence. Structural
            # actor_config drift raises ActorConfigDriftList and refuses
            # to start; cron drift produced one DEBUG line that named
            # the code's values and never compared them to the stored
            # row, so the mismatch itself was undetectable at any log
            # level. Compare and warn, without changing the write
            # semantics.
            reverted = await _revert_stale_auto_disable(deps, settings, spec)
            if reverted:
                _startup_log.info(
                    "cron-schedule-auto-disable-reverted",
                    actor=spec.actor,
                    name=spec.name,
                )
            await _warn_on_cron_drift(backend, spec)
        else:
            _startup_log.info(
                "cron-schedule-registered",
                actor=spec.actor,
                expr=spec.cron_expr,
                next_fire_at=next_fire.isoformat(),
            )


async def _warn_on_cron_drift(backend: Backend, spec: CronScheduleSpec) -> None:
    """Compare a code-declared cron spec against its stored row and warn on drift.

    Only the fields a redeploy cannot change are compared. ``enabled`` is
    deliberately excluded: it is operator-controlled at runtime (the admin UI
    and CLI toggle it), so a disabled schedule is an intentional state, not
    drift, and warning about it every startup would train operators to ignore
    the event.

    Never raises. Drift detection must not be able to prevent a worker from
    starting -- unlike the actor_config equivalent, whose fail-fast is
    essential for dispatch correctness. A cron mismatch means the wrong
    cadence, not a broken worker.
    """
    try:
        stored = [
            rec for rec in await backend.list_schedules(actor=spec.actor) if rec.name == spec.name
        ]
    except Exception as exc:
        _startup_log.debug(
            "cron-schedule-drift-check-failed",
            actor=spec.actor,
            name=spec.name,
            error=repr(exc),
        )
        return

    if not stored:
        # Raced with a delete, or the conflict came from another constraint.
        return
    row = stored[0]

    drifted: dict[str, dict[str, str]] = {}
    for field, code_value in (
        ("cron_expr", spec.cron_expr),
        ("timezone", spec.timezone),
        ("dst_strategy", spec.dst_strategy),
    ):
        stored_value = getattr(row, field)
        if stored_value != code_value:
            drifted[field] = {"stored": str(stored_value), "code": str(code_value)}

    if not drifted:
        _startup_log.debug(
            "cron-schedule-already-registered",
            actor=spec.actor,
            name=spec.name,
            expr=spec.cron_expr,
        )
        return

    _startup_log.warning(
        "cron-schedule-drift",
        kind="cron_schedule_drift",
        actor=spec.actor,
        name=spec.name,
        schedule_id=str(row.id),
        drift=drifted,
        remedy=(
            "cron registration is create-only, so the stored row keeps the OLD "
            "values and this worker fires on the stored cadence, not the one in "
            "code. Apply the new values with TaskQ.update_schedule(...) (or "
            "delete_schedule and restart to let registration recreate it)."
        ),
    )


def _emit_startup_warnings(settings: WorkerSettings) -> None:
    """Emit the once-at-startup operator warnings for a worker's settings.

    Extracted from ``_main`` so the warnings can be exercised directly: they
    are pure functions of the settings with no side effect but the log line,
    and reaching them through ``_main`` means standing up a whole worker.
    """

    # Why: the bounded-close tail is additive on top of the shutdown phase
    # graces and is deliberately not modelled by the settings validator,
    # which cannot reject a sub-worst-case budget without taking away a
    # legitimate operator choice (a tight deployment that would rather be
    # SIGKILLed mid-unwind than wait out a hung close), see
    # WorkerSettings.worst_case_shutdown_seconds. The shipped default
    # covers the model, so this warning only fires for custom budgets that
    # fall short. Left unsurfaced, the first symptom is a SIGKILL
    # mid-unwind during an incident, with terminal writes truncated and
    # jobs recovered later by crash reclaim instead of finalizing cleanly.
    # Warn once at startup, with the numbers an operator needs to size the
    # pod grace.
    if not settings.shutdown_budget_is_sufficient:
        _startup_log.warning(
            "shutdown-budget-exceeds-termination-grace",
            worst_case_seconds=settings.worst_case_shutdown_seconds,
            termination_grace_period=settings.termination_grace_period,
            cancellation_grace_period=settings.cancellation_grace_period,
            cleanup_grace_period=settings.cleanup_grace_period,
            close_tail_seconds=worst_case_teardown_tail(),
            remedy=(
                "Set the pod/container termination grace to at least "
                f"{settings.worst_case_shutdown_seconds}s (Kubernetes "
                "terminationGracePeriodSeconds, Azure Container Apps "
                "terminationGracePeriodSeconds), or lower "
                "TASKQ_CANCELLATION_GRACE_PERIOD / TASKQ_CLEANUP_GRACE_PERIOD. "
                "If this warning appeared right after an upgrade, see the "
                "termination-grace default entry in docs/guides/upgrading.md: "
                "a platform grace pinned to the old 75s default needs raising too."
            ),
        )

    # Why: the release park's lease cap (the first of the two
    # lease-vs-park inequalities the release-until-exited design turned
    # into guarantees: this one CLOSED STRUCTURALLY, by the cap in
    # _actor_exit_wait_budget, so this is a trade-off surface, not a
    # safety hole). The park's budget bound is the remaining termination
    # budget minus the release write's own budget; the lease cap
    # (lock_lease - heartbeat - write budget) binds first whenever
    # lock_lease < termination - cancellation - cleanup + heartbeat. The
    # config is SAFE either way: the cap is exactly the bound that keeps
    # the parked consumer's release write ahead of the earliest lease
    # reclaim, but a binding cap means deploy-interrupted sync actors
    # release earlier with longer holds instead of getting the full
    # budget to finish. Surface the arithmetic so an operator staring at
    # held rows knows which knob moved. No cross-field rejection here:
    # the cap makes every loadable config safe, and refusing the config
    # (the alternative remedy) would reject configs the cap already
    # protects: see WorkerSettings.release_park_lease_cap.
    if (
        settings.watchdog_enabled
        and settings.release_park_lease_cap < settings.release_park_budget_bound
    ):
        _startup_log.warning(
            "release-park-lease-capped",
            lock_lease=settings.lock_lease,
            heartbeat_interval=settings.heartbeat_interval,
            park_lease_cap_seconds=settings.release_park_lease_cap,
            park_budget_bound_seconds=settings.release_park_budget_bound,
            termination_grace_period=settings.termination_grace_period,
            cancellation_grace_period=settings.cancellation_grace_period,
            cleanup_grace_period=settings.cleanup_grace_period,
            remedy=(
                "safe as configured (the release park is capped so the parked "
                "consumer's release always beats the lease reclaim), but the cap "
                f"({settings.release_park_lease_cap}s) binds before the budget "
                f"bound ({settings.release_park_budget_bound}s): deploy-interrupted "
                "sync actors release earlier with longer holds instead of "
                "getting the full termination budget to finish. Raise "
                "TASKQ_LOCK_LEASE to at least "
                f"{settings.release_park_budget_bound + settings.heartbeat_interval + TERMINAL_WRITE_BUDGET_SECS:.0f}s "
                "for the full-budget park"
            ),
        )

    # Why: the disown path's lease bound (the second of the two
    # lease-vs-park inequalities the release-until-exited design turned
    # into guarantees). When BOTH release writers fail their writes (the
    # RELEASING phase's write and the consumer's, whose exhausted retries
    # disown the row), the row stays running behind a lease the heartbeat
    # has already stopped renewing, and the leader's reclaim sweep becomes
    # the only exit: at the earliest last-heartbeat + lock_lease. For
    # that to stay behind the process's true exit (the deadline trip plus
    # the exit tail: where an outlived actor thread dies), the lease must
    # cover termination - cancellation - cleanup + heartbeat + the exit
    # tail. The shipped default is 63 against lock_lease 60: a ~3s residue
    # that requires the double write failure AND a sweep tick landing
    # inside it. Deliberately a warning, not a hard fail: the shipped
    # default would not load otherwise, and whether to spend 3 more
    # seconds of lease on that residue is an operator call the maintainer
    # surfaces here rather than makes (see
    # WorkerSettings.release_disown_lease_floor).
    # Deliberately NOT gated on watchdog_enabled, unlike its sibling
    # above: the lease arithmetic does not read the flag, and an operator
    # running without the watchdog is exactly the config where the
    # double-failure residue matters most - no deadline trip bounds an
    # outlived actor at all, so the leader's reclaim sweep is the only
    # exit and this floor is the only thing keeping it behind the
    # process's true exit. Only the remedy's watchdog clause is
    # conditional: with the watchdog armed the deadline trip kills the
    # still-running actor inside the residue window; without it, nothing
    # does.
    if settings.lock_lease < settings.release_disown_lease_floor:
        residue = settings.release_disown_lease_floor - settings.lock_lease
        if settings.watchdog_enabled:
            remedy = (
                "Raise TASKQ_LOCK_LEASE to at least "
                f"{settings.release_disown_lease_floor:.0f}s (or lower "
                "TASKQ_TERMINATION_GRACE_PERIOD) so the leader's reclaim "
                "sweep cannot take a disowned row before the shutdown "
                f"watchdog's deadline trip kills its still-running actor; "
                f"the current settings leave a {residue:.1f}s window that "
                "requires both release writes to fail AND a sweep tick to "
                "land inside it"
            )
        else:
            remedy = (
                "Raise TASKQ_LOCK_LEASE to at least "
                f"{settings.release_disown_lease_floor:.0f}s (or lower "
                "TASKQ_TERMINATION_GRACE_PERIOD) so the leader's reclaim "
                "sweep cannot take a disowned row whose still-running "
                "actor has no shutdown watchdog to kill it; the current "
                f"settings leave a {residue:.1f}s window that requires "
                "both release writes to fail AND a sweep tick to land "
                "inside it"
            )
        _startup_log.warning(
            "lock-lease-below-disown-exit-floor",
            lock_lease=settings.lock_lease,
            disown_floor=settings.release_disown_lease_floor,
            residue_seconds=round(residue, 2),
            termination_grace_period=settings.termination_grace_period,
            cancellation_grace_period=settings.cancellation_grace_period,
            cleanup_grace_period=settings.cleanup_grace_period,
            heartbeat_interval=settings.heartbeat_interval,
            exit_tail_seconds=settings.release_exit_tail_seconds,
            remedy=remedy,
        )

    # Why: TASKQ_MIGRATE_ON_START is defined on TaskQSettings, so WorkerSettings
    # inherits and happily VALIDATES it -- an operator setting it on a worker
    # gets no error and no migration. The worker deliberately does not honour
    # it (N replicas racing to migrate is exactly the hazard the migration
    # advisory lock exists to prevent), so say so rather than ignoring it
    # silently. Reading the README during an incident is precisely when this
    # would be set.
    if settings.migrate_on_start:
        _startup_log.warning(
            "migrate-on-start-ignored-by-worker",
            setting="TASKQ_MIGRATE_ON_START",
            reason=(
                "the worker never applies migrations; this setting is consumed only by `taskq ui serve`"
            ),
            remedy=(
                "run `taskq migrate up` from a pre-deploy job or init container before starting workers"
            ),
        )

    # Why: the dispatch probe-narrowing this flag applied no longer exists
    # (dispatch is assignment-routed; the routing decision rides the jobs
    # row), so the field is a deprecated no-op kept only so configurations
    # that set it keep loading. Silent acceptance would hide the flag's
    # retirement from the one operator who opted in; the warning names the
    # removal so the env var gets deleted instead of accumulating.
    if settings.dispatch_scope_by_home_queue:
        _startup_log.warning(
            "deprecated-setting-ignored",
            setting="TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE",
            reason=(
                "dispatch is assignment-routed; the per-actor-capacity "
                "scoping this flag applied no longer exists"
            ),
            remedy="remove TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE from the environment",
        )

    # Why: this is the one setting that deliberately widens what leaves the
    # trust boundary, and its effect is invisible in normal operation -- an
    # operator who flips it during an incident gets no other signal that raw
    # row values are now being shipped to the telemetry vendor. Warn on every
    # startup, in the same shape as the admin-ui-no-auth warning, so it cannot
    # be left on and forgotten across a deploy.
    if not settings.exception_redaction_enabled:
        _startup_log.warning(
            "exception-redaction-disabled",
            setting="TASKQ_EXCEPTION_REDACTION_ENABLED",
            detail=(
                "exception redaction is OFF: raw Postgres exception text is "
                "being written to spans and logs, and shipped to every "
                "configured telemetry backend. That includes 'DETAIL:' lines, "
                "which quote caller-supplied row values -- idempotency_key, "
                "identity_key and fairness_key routinely hold tenant or "
                "subject identifiers. URI credential masking is still applied "
                "(a password in a DSN is never shipped), but nothing else is. "
                "This is a debugging aid, not a supported production setting."
            ),
            remedy=(
                "unset TASKQ_EXCEPTION_REDACTION_ENABLED (or set it true) and "
                "restart as soon as the investigation is finished"
            ),
        )


async def _refuse_boot_on_pending_migrations(deps: WorkerDeps, settings: WorkerSettings) -> None:
    """Raise ``RuntimeError`` when the schema is behind this code's migrations.

    The worker never applies migrations by design (``N`` replicas racing to
    migrate is the hazard the migration advisory lock exists to prevent ,
    see ``_emit_startup_warnings``'s migrate-on-start warning), so a schema
    stopped one release behind is a deployment ordering mistake, and the
    boot path's own doctrine ("a deployment mistake that must crash startup
    loudly, not a best-effort condition to warn about", the queue-cap
    guard's comment) covers it: every pending ``pre``-phase migration
    refuses boot, not just the one whose missing column happens to be
    probed (01.00.04's ``queues.max_concurrent``). A fresh database with
    no ledger at all is the loudest case of the same mistake, every
    bundled pre-phase migration is pending, and refuses identically
    instead of failing later on raw ``UndefinedTableError`` from the
    first boot step that writes.

    Only the ``pre`` phase is a currency verdict. The phased rollout the
    migration headers instruct operators to follow leaves the fleet in a
    deliberate middle state, pre applied, new release rolling, post
    withheld until the last old pod is gone, and post-phase migrations
    only remove structures the old release still needed. Counting a
    pending post-phase migration as "schema behind code" would refuse
    boot for exactly the state the procedure requires, deadlocking every
    rolling deploy: the post phase can never be applied because the roll
    can never finish.

    Why hand-rolled ``fetch``-only queries instead of reusing
    :func:`taskq.migrate.list_applied`: the boot path's established
    duck-typing contract. The unit lane's pool stubs
    (``tests/conftest.py``'s ``_FakeConn``) implement exactly
    ``fetch``/``execute``/``transaction``, the same surface
    ``_apply_batch_statement_timeout`` documents as the complete
    ConnLike wrapper contract, and ``list_applied`` needs
    ``fetchval``. The EXISTS probe below therefore returns one row on
    every real connection (``SELECT EXISTS`` always answers) and an
    empty list on a stub, which is precisely the queue-cap probe's own
    convention for "this connection cannot answer schema questions"
    (empty result → no information → the read degrades and boot
    proceeds; the integration lane exercises the guard against real
    Postgres).
    """
    from taskq.migrate import discover

    if not _IDENT_RE.match(settings.schema_name):
        raise ValueError(f"invalid schema identifier: {settings.schema_name!r}")
    ledger_probe = (
        "SELECT EXISTS ("
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = $1 AND table_name = 'schema_migrations'"
        ") AS ledger_exists"
    )
    async with deps.dispatcher_pool.acquire(timeout=settings.dispatcher_command_timeout) as conn:
        probe_rows = await conn.fetch(ledger_probe, settings.schema_name)
        if not probe_rows:
            # A real connection always answers SELECT EXISTS with one
            # row; no rows means a duck-typed stub cannot answer schema
            # questions at all, the queue-cap read's own degradation
            # path, not a currency verdict.
            return
        if not probe_rows[0]["ledger_exists"]:
            # No ledger anywhere: a fresh database. Every bundled
            # migration is pending by definition.
            applied: set[str] = set()
        else:
            ledger_rows = await conn.fetch(
                f'SELECT version FROM "{settings.schema_name}".schema_migrations',  # noqa: S608  # Why: schema validated against _IDENT_RE at the top of this helper; asyncpg cannot bind identifiers.
            )
            applied = {str(r["version"]) for r in ledger_rows}
    pending = [m.key for m in discover() if m.phase == "pre" and m.key not in applied]
    if pending:
        raise RuntimeError(
            f"schema {settings.schema_name!r} is missing {len(pending)} pre-phase "
            f"migration(s) this worker's code requires ({', '.join(pending)}); the "
            "worker never applies migrations itself. Apply pending migrations before "
            "starting workers (`taskq migrate up`, or a pre-deploy job/init container)."
        )


async def _ensure_own_reservation_slots(
    deps: WorkerDeps,
    own_reservations: list[ConcurrencyReservation],
) -> None:
    """Run the bootstrap's ensure-slots pass over this worker's reservations.

    Every wait here is bounded by ``dispatcher_command_timeout``, the same
    budget every sibling bootstrap await carries. The bound matters because
    the dispatcher pool's ``command_timeout`` does NOT bound a pool ACQUIRE:
    under pool starvation a bare ``pool.acquire`` waits forever,
    observed once as a 300s CI hang in this exact path). The pool-level
    bound comes from ``ensure_slots``' ``timeout`` forward; a typed
    ``ensure_slots_timeout`` warning (distinct from the generic
    ``ensure_slots_failed`` so a starvation stall is diagnosable from logs
    instead of surfacing as a mystery hang) is the failure surface:
    observability, not a gate, a worker able to do work must start, the
    same doctrine the resolved-capacity read above follows. The next
    restart re-runs the idempotent materialisation.
    """
    budget = deps.settings.dispatcher_command_timeout
    for res in own_reservations:
        try:
            await res.ensure_slots(deps.dispatcher_pool, timeout=budget)
        except TimeoutError as exc:
            _startup_log.warning(
                "ensure_slots_timeout",
                bucket_name=res.name,
                timeout_seconds=budget,
                error=str(exc),
            )
        except Exception as exc:
            _startup_log.warning(
                "ensure_slots_failed",
                bucket_name=res.name,
                error=str(exc),
            )


async def _main(
    settings: WorkerSettings,
    *,
    _local_queue_seed: list[JobRow] | None = None,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    _registry: ProviderRegistry | None = None,
    _cron_registry: list[CronScheduleSpec] | None = None,
    connections: WorkerConnections | None = None,
    pg_credential_provider: PgCredentialProvider | None = None,
    rate_limit_registry: RateLimitRegistry | None = None,
    until_idle: bool = False,
    idle_settle_window: float | None = None,
    idle_poll_interval: float | None = None,
    idle_max_runtime: float | None = None,
) -> int:
    """Worker bootstrap: open deps, wire TaskGroup of siblings, run to shutdown.

    ``_local_queue_seed`` is a test seam, keyword-only, defaults to ``None``,
    prefixed with ``_`` to mark it as non-production API.  When not ``None``,
    each job in the seed list is pushed onto ``local_queue`` BEFORE the
    TaskGroup starts, so consumer stubs immediately consume them.  The seed
    is size-checked at entry: ``local_queue`` is bounded at
    ``max_concurrency`` and is drained only by consumers created after the
    seed loop, so an oversized seed (more than ``max_concurrency`` jobs)
    raises ``ValueError`` here rather than parking the bootstrap on a full
    queue with zero consumers started.  Production callers (``worker_main``)
    MUST NOT pass this parameter.

    ``pg_credential_provider`` is the resolved Postgres credential
    provider the worker's internal per-slot transaction pool
    authenticates with when the per-slot path activates (a LOOP-scope
    connection registered plus ``max_concurrency > 1``). The role pools
    take their credentials from ``connections``; the slot pool
    deliberately does not (it is worker-internal), so this parameter is
    how the documented managed-identity path reaches it. ``None`` means
    the slot pool authenticates from the direct DSN.

    ``actor_registry`` is a mapping from short name to :class:`ActorRef`
    containing every ``@actor``-decorated handler this worker intends to
    run. When not ``None``, :func:`sync_actor_config` is called after
    ``register_worker`` and before the ``TaskGroup`` opens so dispatch
    queries always see registered concurrency caps.

    ``_registry`` is a test seam, keyword-only, defaults to ``None``,
    prefixed with ``_`` to mark it as non-production API.  When not ``None``,
    the caller-supplied registry is used instead of creating a fresh one.
    This allows integration tests to inject a pre-configured (possibly
    misconfigured) registry to verify that ``validate()`` errors propagate
    through the real ``_main`` bootstrap path.  Production callers
    (``worker_main``) MUST NOT pass this parameter.

    ``_cron_registry`` is the resolved list of :class:`CronScheduleSpec`
    objects to auto-register at startup.  Populated by ``worker_main``
    from either the explicit ``cron_registry`` argument or
    ``get_registered_crons()``.  For each spec, ``backend.create_schedule``
    is called with a :class:`ScheduleCreateArgs` inside ``try/except
    asyncpg.UniqueViolationError``, the ``(actor, name)`` UNIQUE constraint
    makes this registration pass create-only and skip-on-conflict. The conflict
    branch is no longer a bare ``pass``: it calls
    :func:`_warn_on_cron_drift`, which compares the code-declared spec against
    the stored row and warns when they differ. Write semantics are unchanged ,
    detection only.

    ``rate_limit_registry`` is the :class:`RateLimitRegistry` this worker
    owns and dispatches against.  Resolution order: explicit argument →
    ``RateLimitRegistry`` value provider at ``Scope.LOOP`` in ``_registry``
    → module singleton (see :func:`_resolve_rl_registry`).  Co-present with a
    ``RateLimitRegistry`` provider in ``_registry`` this raises
    ``TypeError`` (ambiguous, bootstrap and dispatch would diverge);
    pass one or the other.  Actor-declared primitive instances
    (``@actor(rate_limits=[TokenBucket(...)])``) are collected and
    registered into the resolved registry before ``validate()`` runs.

    ``until_idle`` enables drain mode: a :func:`drain_monitor_loop`
    sibling polls the backend for active jobs and triggers graceful
    shutdown when the queues stay empty for ``idle_settle_window``
    seconds.  ``idle_settle_window`` is the seconds to wait after
    queues appear empty before declaring drained.  ``idle_poll_interval``
    is how often to check queue depth.  ``idle_max_runtime`` is an
    optional wall-clock cap; when exceeded the drain monitor triggers
    shutdown with exit code 4.

    Returns the exit code from the orchestrator (read from the holder), or
    0 when no signal arrived (clean shutdown via external shutdown_event.set()).
    With ``until_idle=True``, returns 0 (all jobs succeeded), 3 (some
    jobs failed), or 4 (max-runtime exceeded). Without ``until_idle``,
    returns 0 on clean shutdown.
    """
    from taskq.worker.run import (
        _emit_resolved_capacity_startup_lines,
        consumer_loop_stub,
        deregister_worker,
        di_consumer_loop,
        producer_loop,
        register_worker,
    )

    if actor_registry is not None:
        # Why: a mismapped entry (key != ref.name) surfaces deep in
        # sync_actor_config as a raw CardinalityViolation ("ON CONFLICT DO
        # UPDATE command cannot affect row a second time") when two refs
        # share a .name. Dispatch looks actors up by registry key, so
        # key == ref.name is the essential invariant; enforcing it here
        # also makes duplicate names impossible (same name means same key,
        # so the dict itself dedupes at construction).
        mismatched = sorted(
            (key, ref.name) for key, ref in actor_registry.items() if key != ref.name
        )
        if mismatched:
            pairs = ", ".join(f"{key!r} -> {name!r}" for key, name in mismatched)
            raise ValueError(
                f"actor_registry keys must equal each ActorRef's name; mismatches: {pairs}"
            )

    if _local_queue_seed is not None and len(_local_queue_seed) > settings.max_concurrency:
        # Why at the boundary, before any I/O: the seed loop below pushes
        # onto local_queue (maxsize = max_concurrency) BEFORE the consumer
        # TaskGroup exists, so the excess put would park bootstrap forever
        # on a full queue with zero consumers running, a silent hang, not
        # a slow start. Rejecting here fails before a worker row is
        # registered or signal handlers are installed.
        raise ValueError(
            f"_local_queue_seed has {len(_local_queue_seed)} job(s) but "
            f"max_concurrency is {settings.max_concurrency}: the seed is "
            f"pushed onto local_queue (bounded at max_concurrency) before "
            f"any consumer starts, so an oversized seed would park "
            f"bootstrap forever. Seed at most max_concurrency jobs."
        )

    registry = _registry if _registry is not None else ProviderRegistry()
    if not registry.has_provider(WorkerSettings):
        registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    if not registry.has_provider(Clock):
        registry.register_value(Clock, Scope.PROCESS, SystemClock())

    resolved_rl_registry = _resolve_rl_registry(rate_limit_registry, registry)

    # Actor-declared primitive instances (the primary registration path):
    # collect every TokenBucket / SlidingWindow / ConcurrencyReservation
    # declared on actors in this worker's actor_registry into the resolved
    # registry BEFORE validate() runs. Conflict semantics are register()'s
    # own (_same_config): identical config = debug-log no-op; same name
    # with different config = ValueError at startup (fail fast). Actors
    # decorated but absent from the mapping are NOT collected. The startup
    # log counts DECLARATIONS (not distinct new registrations), the same
    # instance declared on two actors logs rate_limit_count=2 but
    # registers once (idempotent no-op).
    if actor_registry is not None:
        collected_rl_names: list[str] = []
        collected_res_names: list[str] = []
        for actor_ref in actor_registry.values():
            for rl_entry in actor_ref.rate_limits:
                if isinstance(rl_entry, TokenBucket | SlidingWindow):
                    resolved_rl_registry.register(rl_entry)
                    collected_rl_names.append(rl_entry.name)
            for res_entry in actor_ref.reservations:
                if isinstance(res_entry, ConcurrencyReservation):
                    resolved_rl_registry.register(res_entry)
                    collected_res_names.append(res_entry.name)
        _startup_log.info(
            "ratelimit-actor-primitives-registered",
            rate_limit_count=len(collected_rl_names),
            reservation_count=len(collected_res_names),
            rate_limit_names=collected_rl_names,
            reservation_names=collected_res_names,
        )

    scope_containers: dict[Scope, ProcessScope | ThreadScope | LoopScope] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type]  # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol, pyright cannot verify dict covariance across the Protocol boundary

    loop = asyncio.get_running_loop()

    set_otel_enabled(settings.otel_enabled)
    set_exception_redaction_enabled(settings.exception_redaction_enabled)
    set_exception_message_max_chars(settings.exception_message_max_chars)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    _producer_log = structlog.get_logger("taskq.worker.run.producer")

    _emit_startup_warnings(settings)

    async with open_worker_deps(settings, connections=connections) as deps:
        # The capacity gauges read the registry the consumers fill; pointed
        # here, before the TaskGroup opens, so the first scrape after boot
        # already reports 0 of max_concurrency rather than nothing.
        set_worker_capacity_source(deps.active_jobs, settings.max_concurrency)
        # ── until_idle override resolution ──────────────────────────────
        settle: float = settings.idle_settle_window
        poll: float = settings.idle_poll_interval
        runtime: float | None = settings.idle_max_runtime
        if until_idle:
            settle = (
                idle_settle_window
                if idle_settle_window is not None
                else settings.idle_settle_window
            )
            poll = (
                idle_poll_interval
                if idle_poll_interval is not None
                else settings.idle_poll_interval
            )
            runtime = (
                idle_max_runtime if idle_max_runtime is not None else settings.idle_max_runtime
            )

            if not math.isfinite(settle) or settle < 0:
                raise ValueError(f"idle_settle_window must be >= 0 and finite, got {settle}")
            if not math.isfinite(poll) or poll < 0.1:
                raise ValueError(f"idle_poll_interval must be >= 0.1 and finite, got {poll}")
            if runtime is not None and (not math.isfinite(runtime) or runtime <= 0):
                raise ValueError(f"idle_max_runtime must be > 0 and finite, got {runtime}")

            if _cron_registry:
                _startup_log.warning(
                    "until-idle-with-cron",
                    kind="until_idle_with_cron",
                    message="until_idle mode is incompatible with cron-driven workloads; "
                    "the queue will never drain. Use --idle-max-runtime as a cap.",
                )

        # Schema-currency guard: refuse boot BEFORE any boot step writes to
        # the database (sync_rate_limit_buckets, register_worker, ...). A
        # schema one release behind used to pass every boot step except the
        # queue-cap query's 01.00.04 guard: the enqueue INSERT's column list
        # omits whatever the missing migration adds, so writes half-work,
        # and the first dispatch claim's RETURNING then dies in the strict
        # ``_job_row_from_record`` read AFTER the claim already committed
        # the row to running+locked, every dispatched job loops through
        # lock-expiry crash-reclaim and never executes. Same doctrine as
        # the queue-cap guard below: a deployment mistake must crash
        # startup loudly, not best-effort warn and serve against a stale
        # schema.
        await _refuse_boot_on_pending_migrations(deps, settings)

        # Only register the worker pool in DI when the user hasn't provided
        # their own asyncpg.Pool provider, and only then may the reload
        # coordinator refresh the DI cache after a hot-reload swap.
        worker_pool_registered_in_di = not registry.has_provider(asyncpg.Pool)
        if worker_pool_registered_in_di:
            registry.register_value(asyncpg.Pool, Scope.LOOP, deps.worker_pool)

        actors_list: list[ActorRef[Any, Any]] | None = (
            list(actor_registry.values()) if actor_registry else None
        )
        register_rate_limit_registry(registry, resolved_rl_registry)
        if not _redis_configured(settings, registry):
            # Why: a Redis-backed rate limit with no Redis configured only
            # fails per-dispatch (get_redis_pool raises after the job has
            # burned retries), fail fast at bootstrap, naming the
            # offending limiter(s).
            redis_backed = _served_redis_rate_limits(actor_registry, resolved_rl_registry)
            if redis_backed:
                msg = (
                    "Redis-backed rate limit(s) declared by served actors but no Redis "
                    f"is configured: {', '.join(redis_backed)}. Set TASKQ_REDIS_URL or "
                    "register a redis.asyncio.Redis DI provider."
                )
                raise RuntimeError(msg)
        # Why: after the rate-limit gate so a boot about to crash on
        # Redis-backed limits does not also warn about progress fanout.
        # Without this, the worker-side publish skip in context.py is the
        # only silent link in issue #341's loud degradation chain. The
        # predicate is the resolved client (the exact object the publish
        # block tests), not the gate's _redis_configured - the derivation
        # is in the helper's docstring.
        _emit_progress_fanout_unconfigured_warning(redis_client=deps.redis_client)
        if settings.redis_url is not None:
            if not _redis_extra_installed():
                # Why: without this check the missing extra surfaces later as
                # a bare MissingProvider at DI validate, no hint that the
                # fix is installing the package. Only raise when Redis is
                # actually required: a URL set without redis-backed limits
                # is harmless (register_redis_pool silently skips).
                redis_backed = _served_redis_rate_limits(actor_registry, resolved_rl_registry)
                if redis_backed:
                    msg = (
                        "TASKQ_REDIS_URL is set but the [redis] extra is not "
                        "installed; it is required by rate limit(s): "
                        f"{', '.join(redis_backed)}. Install it with: "
                        "pip install 'taskq[redis]'"
                    )
                    raise RuntimeError(msg)
            # Why: LoopScope.bootstrap eagerly resolves every LOOP provider,
            # and get_redis_pool raises when redis_url is None, registering
            # unconditionally would crash workers that don't use Redis.
            register_redis_pool(registry)
        registry.validate(actors=actors_list, rate_limit_registry=resolved_rl_registry)
        _validate_error_reporter_scope(registry)

        from taskq.ratelimit import sync_rate_limit_buckets, sync_slots

        # The resolved rate-limit registry may carry reservations declared
        # for OTHER schemas/databases (e.g. sibling apps sharing one
        # registry). Only this worker's own schema is in scope: touching
        # another schema's slot tables here would write into the wrong
        # database or fail noisily.
        own_reservations = [
            res
            for res in resolved_rl_registry.reservations.values()
            if res.schema == settings.schema_name
        ]

        try:
            await sync_rate_limit_buckets(
                resolved_rl_registry, deps.worker_pool, schema=settings.schema_name
            )
        except Exception as exc:
            _startup_log.warning(
                "sync_rate_limit_buckets_failed",
                error=str(exc),
            )
        try:
            await sync_slots(
                own_reservations,
                deps.worker_pool,
                schema=settings.schema_name,
            )
        except Exception as exc:
            _startup_log.warning(
                "sync_slots_failed",
                error=str(exc),
            )

        # The scope bootstraps' first use of user-registered factories
        # runs in the pre-watchdog window (the loop keeps scheduling, so
        # the lag watchdog never trips; the stale-tick detectors arm
        # only after bootstrap), so each factory await carries the same
        # reload_factory_timeout bound every WorkerConnections factory
        # open carries: a black-holed "database pools, HTTP
        # clients" DI factory fails boot loudly within the bound instead
        # of wedging worker startup undetected.
        process_scope = ProcessScope(
            resolver=resolver, factory_timeout=settings.reload_factory_timeout
        )
        scope_containers[Scope.PROCESS] = process_scope
        await process_scope.bootstrap(registry, settings)

        thread_scope = ThreadScope(
            resolver=resolver, factory_timeout=settings.reload_factory_timeout
        )
        scope_containers[Scope.THREAD] = thread_scope
        await thread_scope.bootstrap(registry, process_scope)

        loop_scope = LoopScope(resolver=resolver, factory_timeout=settings.reload_factory_timeout)
        scope_containers[Scope.LOOP] = loop_scope
        await loop_scope.bootstrap(registry, process_scope, thread_scope)

        _clock_obj = process_scope.get(Clock)
        if _clock_obj is None or not isinstance(_clock_obj, Clock):
            raise MissingProvider(
                type_name="Clock",
                required_by="worker._main bootstrap (auto-registration guard "
                "must run before ProcessScope.bootstrap)",
            )
        _clock: Clock = _clock_obj
        backend: Backend = PostgresBackend(
            deps,
            clock=_clock,
            cancellation_grace_period=timedelta(seconds=settings.cancellation_grace_period),
            cleanup_grace_period=timedelta(seconds=settings.cleanup_grace_period),
            reclaim_event_visibility_delay=timedelta(
                seconds=settings.reclaim_event_visibility_delay
            ),
        )

        # The per-slot transaction pool activates on exactly the shape
        # that would otherwise share one LOOP-scope connection across
        # concurrent slots; every other worker shape is untouched. Opened
        # after loop_scope.bootstrap(), that is the first point the
        # app-registered LOOP-scope connection is resolvable, and before
        # the startup warnings so the mode signal and any credential
        # warning land with the rest of the startup story.
        await _maybe_open_slot_pool(
            loop_scope,
            settings,
            deps,
            factory=_slot_pool_factory(settings, pg_credential_provider),
            pg_credential_provider=pg_credential_provider,
            caller_supplied_pg_pools=_caller_supplied_pg_pools(connections),
            di_registry=registry,
            log=_startup_log,
        )

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=loop_scope.resolved_cache(),
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        if actor_registry is not None:
            _emit_sub_enqueue_startup_warnings(
                loop_scope,
                settings,
                actor_registry,
                _startup_log,
            )

        worker_id = await register_worker(deps.dispatcher_pool, settings)

        structlog.contextvars.bind_contextvars(worker_id=str(worker_id))

        if actor_registry is not None:
            # Why: in this block, not the earlier actor_registry block
            # above. Tradeoff: the earlier block needs nothing from the
            # database, it would warn even when worker registration
            # fails on a bad DSN or a pool stall, but it runs before
            # bind_contextvars, so its warnings carry no worker_id
            # correlation with the workers-table row; this block has the
            # correlation and still precedes the sync_actor_config
            # round-trip below, whose drift raise or pool-acquire stall
            # would swallow a warning placed after it.
            #
            # The stored assignments are read first because they, not the
            # decorator literals, decide where an actor's work routes.
            # Read-only and best effort: a coverage warning must never be
            # the thing that stops a worker able to do work from booting,
            # so a failed read degrades to the literals. A pre-sync read is
            # correct here: an actor with no stored row yet cannot be
            # judged against a stored assignment it doesn't have, and
            # falls back to the literal inside the emitter.
            stored_queues: dict[str, str] = {}
            try:
                async with deps.dispatcher_pool.acquire(
                    timeout=settings.dispatcher_command_timeout
                ) as conn:
                    stored_queues = await read_stored_queue_assignments(
                        conn,
                        list(actor_registry),
                        schema=settings.schema_name,
                    )
            except Exception as exc:
                # WARN, never quieter: with the read degraded the coverage
                # check below runs on decorator literals it cannot trust, so
                # its warning can fire on an actor the stored config routes
                # to a queue this worker consumes (or stay silent on one it
                # does not). A degraded read that looked like a healthy one
                # would send operators chasing actors instead of the read.
                _startup_log.warning(
                    "stored-queue-assignments-unreadable",
                    error=repr(exc),
                    note=(
                        "the stored queue assignments could not be read, so the "
                        "queue-coverage check falls back to the decorator "
                        "literals. The stored assignment, not the literal, is "
                        "what routes re-pended rows and cron fires, so a "
                        "coverage warning from this boot can be a false positive "
                        "and a missing one a false negative. Fix the read (the "
                        "dispatcher pool acquire or the actor_config query) "
                        "rather than filtering the warning."
                    ),
                )
            _emit_unconsumed_queue_startup_warnings(
                settings, actor_registry, _startup_log, stored_queues
            )
            actor_configs = [
                ActorConfig(
                    actor=ref.name,
                    max_concurrent=ref.max_concurrent,
                    max_pending=ref.max_pending,
                    queue=ref.queue,
                    result_ttl=ref.result_ttl.total_seconds()
                    if ref.result_ttl is not None
                    else None,
                    metadata=dict(ref.metadata),
                    retry_base=ref.retry.base,
                    retry_cap=ref.retry.cap,
                    retry_backoff=ref.retry.backoff,
                    retry_jitter=ref.retry.jitter,
                    max_attempts=ref.retry.max_attempts,
                    retry_kind=ref.retry.kind,
                )
                for ref in actor_registry.values()
            ]
            async with deps.dispatcher_pool.acquire(
                timeout=settings.dispatcher_command_timeout
            ) as conn:
                await sync_actor_config(
                    conn,
                    actor_configs,
                    force=settings.force_update_actor_config,
                    schema=settings.schema_name,
                )
                # Read back after the sync, never before: only then is every
                # registered actor guaranteed a stored row, and a pre-sync
                # read would label a freshly deployed actor as
                # never-dispatching on its very first boot.
                try:
                    stored_rows = {
                        row.actor: row
                        for row in await list_actor_configs(conn, schema=settings.schema_name)
                    }
                    queue_rows = {
                        row.name: row
                        for row in await list_queues(conn, schema=settings.schema_name)
                    }
                except Exception as exc:
                    # Observability, not a gate: a worker able to do work
                    # must start even when it cannot describe its own caps.
                    _startup_log.warning(
                        "resolved-capacity-read-failed",
                        error=repr(exc),
                    )
                else:
                    _emit_resolved_capacity_startup_lines(
                        settings,
                        actor_registry,
                        stored_rows=stored_rows,
                        queue_rows=queue_rows,
                        log=_startup_log,
                    )

            await _ensure_own_reservation_slots(deps, own_reservations)

        # Fleet-wide per-queue concurrency caps (DB-driven): query the
        # queues table for queues this worker consumes that have a
        # max_concurrent set, register a ConcurrencyReservation for each,
        # and sync their slot rows to match. The DB is the single source
        # of truth, read at worker startup, avoiding configuration drift
        # across a fleet of workers during rolling deploys (the footgun
        # the settings-based design had). The lease is set to lock_lease
        # so the heartbeat extends it in lockstep with job locks; if a
        # worker dies, both the job lock and the queue reservation slot
        # expire at roughly the same time and the recovery sweep reclaims
        # them. register_queue_cap_reservation() is idempotent for identical
        # config (the public register() rejects names in the reserved
        # queue-cap namespace to prevent user shadowing). sync_slots
        # (not ensure_slots) is used so that BOTH growing AND shrinking a
        # cap take effect on restart, ensure_slots can never remove
        # excess slots (its conflict arm only flips the fleet-reclaim
        # keyed mark; INSERT ... ON CONFLICT otherwise) so lowering
        # max_concurrent was a silent no-op.
        # sync_slots inserts missing slots, deletes excess free slots, and
        # skips held slots (reporting them), a strict superset of
        # ensure_slots, so initial registration works identically.
        from taskq.ratelimit.registry import queue_concurrency_reservation_name

        if not _IDENT_RE.match(settings.schema_name):
            raise ValueError(f"invalid schema identifier: {settings.schema_name!r}")
        # This query is as hard-required as sync_actor_config / register_worker
        # elsewhere in this same _main function, neither of those is wrapped
        # in a broad try/except. The only exception we catch specifically is
        # UndefinedColumnError, which signals that migration 01.00.04 has not
        # been applied (the queues.max_concurrent column is absent). That is
        # a deployment mistake that must crash startup loudly, not a
        # best-effort condition to warn about. Any other exception (connection
        # errors, etc.) propagates and crashes startup exactly like every
        # other hard-required startup step in this function already does ,
        # this is a deliberate consistency choice, not an oversight.
        try:
            async with deps.dispatcher_pool.acquire(
                timeout=settings.dispatcher_command_timeout
            ) as conn:
                cap_rows = await conn.fetch(
                    f'SELECT name, max_concurrent FROM "{settings.schema_name}".queues '  # noqa: S608  # Why: schema validated at construction and re-checked above; asyncpg cannot bind identifiers.
                    f"WHERE name = ANY($1) AND max_concurrent IS NOT NULL",
                    settings.queues,
                )
        except asyncpg.exceptions.UndefinedColumnError as exc:
            raise RuntimeError(
                f"queues.max_concurrent column is missing in schema "
                f"{settings.schema_name!r}, migration "
                f"01.00.04_01_pre_queue_concurrency.sql has not been applied. "
                f"Apply pending migrations before starting workers."
            ) from exc

        queue_cap_reservations: list[ConcurrencyReservation] = []
        for row in cap_rows:
            res_name = queue_concurrency_reservation_name(row["name"])
            reservation = ConcurrencyReservation(
                name=res_name,
                slots=row["max_concurrent"],
                lease=timedelta(seconds=settings.lock_lease),
                schema=settings.schema_name,
            )
            resolved_rl_registry.register_queue_cap_reservation(reservation)
            queue_cap_reservations.append(reservation)

        if queue_cap_reservations:
            # Fail loudly, deliberately NOT warn-and-continue. The
            # reservations were registered above, and dispatch prepends the
            # cap name as a plain string, so the acquire path has no
            # ensure_slots retry: a sync_slots failure here would leave the
            # cap registered with zero (or stale) slot rows, and EVERY
            # dispatch on those queues would snooze with
            # ReservationUnavailable until a human restarted the worker ,
            # a whole queue silently refusing work. Crashing startup
            # instead lets the process supervisor retry, and sync_slots is
            # idempotent, so the next boot reconciles the rows. Same
            # rationale as the missing-column raise above, with a larger
            # blast radius: a queue silently refusing all work is worse
            # than a worker that will not start.
            try:
                await sync_slots(
                    queue_cap_reservations,
                    deps.dispatcher_pool,
                    schema=settings.schema_name,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"failed to sync slot rows for queue-cap reservations "
                    f"{[r.name for r in queue_cap_reservations]} in schema "
                    f"{settings.schema_name!r}: {exc!r}, refusing to start "
                    f"with queue caps registered but unslotted, which would "
                    f"deny every dispatch on those queues until restart. Fix "
                    f"the underlying error and restart the worker; startup "
                    f"retries re-run this sync idempotently."
                ) from exc

        if _cron_registry:
            await _register_cron_schedules(backend, deps, settings, _cron_registry)

        install_signal_handlers(
            loop,
            deps,
            worker_id,
            shutdown_event,
            escalate_event,
            backend,
            orchestrator_holder,
        )

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(
            maxsize=settings.max_concurrency,
        )

        if _local_queue_seed is not None:
            for job in _local_queue_seed:
                await local_queue.put(job)

        async with contextlib.AsyncExitStack() as stack:
            stack.push_async_callback(process_scope.shutdown)
            stack.push_async_callback(thread_scope.shutdown)
            stack.push_async_callback(loop_scope.shutdown)

            if deps.settings.health_enabled:
                health_server = HealthServer()
                try:
                    await health_server.start(deps)
                except HealthTcpBindError:
                    # The probe port the deployment manifest routed to THIS
                    # replica cannot be served: refusing to start is the
                    # honest outcome (probes passing against whatever else
                    # holds the port is the alternative, and a booting
                    # worker with dead probes is the silent kind of down).
                    raise
                except HealthUnixBindCollisionError as exc:
                    # The Unix surface alone is lost (a live peer owns the
                    # socket path), but the TCP probe listener IS up and
                    # this server owns it: the boot continues with
                    # port-routed probes answering, the collision stays the
                    # WARN it has been since the earlier fix, and the stop
                    # callback is STILL pushed (a raised start() gets no
                    # `else`, and the TCP listener must not outlive the
                    # worker). The WARN is the action item it always was:
                    # give each replica a unique socket path.
                    _startup_log.warning(
                        "health-server-unavailable",
                        socket_path=deps.settings.health_socket_path,
                        health_port=deps.settings.health_port,
                        errno=exc.errno,
                        error=str(exc),
                        tcp_listener="serving",
                    )
                    stack.push_async_callback(health_server.stop)
                except OSError as exc:
                    # A unix-socket collision with no TCP listener
                    # configured, by contrast, means a live PEER worker
                    # owns the path (HealthServer.start's loud refusal
                    # stops a newcomer silently stealing it) and nothing
                    # of ours is serving anywhere: refusing to boot would
                    # crash-loop a healthy pair during a rolling restart.
                    # The collision is a WARN and the boot carries on
                    # registering and claiming. No stop callback is
                    # pushed: start() raised before this server owned
                    # anything (its own failure paths already cleaned
                    # up), so there is nothing of ours to stop.
                    _startup_log.warning(
                        "health-server-unavailable",
                        socket_path=deps.settings.health_socket_path,
                        health_port=deps.settings.health_port,
                        errno=exc.errno,
                        error=str(exc),
                    )
                else:
                    stack.push_async_callback(health_server.stop)

            cancel_wake_event: asyncio.Event | None = None
            _subscribe_cancel = getattr(backend, "subscribe_cancel_wake", None)
            if callable(_subscribe_cancel):
                cancel_wake_event = await stack.enter_async_context(
                    cast(
                        "contextlib.AbstractAsyncContextManager[asyncio.Event]", _subscribe_cancel()
                    )
                )

            deps.liveness.grace_factor = settings.watchdog_tick_grace_factor
            deps.liveness.stale_floor = settings.watchdog_stale_floor
            # Stall attribution: the watchdog matches sampled code objects
            # against the registered actor functions (ActorRef.fn's code),
            # and joins running jobs via a snapshot callable instead of a
            # registry reference, so the watchdog never touches loop-owned
            # state directly. The registry is quiescent while the loop is
            # blocked (the loop thread is its only mutator), but a resize
            # racing the snapshot raises RuntimeError, and that must never
            # kill the watchdog thread, hence the guard.
            actor_code_names: dict[int, str] = (
                {id(ref.fn.__code__): ref.name for ref in actor_registry.values()}
                if actor_registry is not None
                else {}
            )

            def _running_job_actors() -> list[tuple[str, str]]:
                try:
                    return [(job.ctx.actor, str(job.job_id)) for job in deps.active_jobs.all()]
                except RuntimeError:
                    return []

            lag_watchdog = LoopLagWatchdog(
                asyncio.get_running_loop(),
                deps.liveness,
                budget=settings.watchdog_loop_lag_budget,
                warn_budget=settings.watchdog_loop_lag_warn_budget,
                startup_grace=settings.watchdog_loop_lag_startup_grace,
                poll_interval=settings.watchdog_check_interval,
                enabled=settings.watchdog_enabled,
                actor_code_names=actor_code_names,
                stall_tally=deps.stall_tally,
                list_running_jobs=_running_job_actors,
            )

            def _stamp_shutdown_started(t: float) -> None:
                if deps.shutdown_started_at is None:
                    deps.shutdown_started_at = t

            shutdown_watchdog = ShutdownWatchdog(
                shutdown_event,
                deadline=settings.termination_grace_period,
                dump_interval=settings.watchdog_dump_interval,
                enabled=settings.watchdog_enabled,
                on_shutdown_started=_stamp_shutdown_started,
                started_at=lambda: deps.shutdown_started_at,
                shutdown_started_event=deps.producer_stop_event,
                dump_after_fraction=settings.watchdog_dump_after_fraction,
            )
            # Cron parity: singleton / max_pending reach client
            # enqueues via the ActorRef stamps in client/_args.py; the cron
            # tick builds its EnqueueArgs directly, so it needs the flags
            # here or its fires silently bypass both. Derived once, at the
            # one site holding the registry; None (no registry) keeps the
            # tick's no-stamping behavior.
            actor_fire_policies: Mapping[str, ActorFirePolicy] | None = (
                {
                    name: ActorFirePolicy(singleton=ref.singleton, max_pending=ref.max_pending)
                    for name, ref in actor_registry.items()
                }
                if actor_registry is not None
                else None
            )
            # A plain ``async with`` plus one finally satisfies both
            # requirements, see that finally for the ordering rationale. A
            # manual __aenter__/__aexit__ pair is NOT needed here and is a
            # trap: awaiting __aexit__ inside the finally makes everything
            # after it conditional on the group exiting without raising,
            # which silently drops deregister_worker on the crash path.
            try:
                async with asyncio.TaskGroup() as tg:
                    lag_watchdog.start()
                    shutdown_watchdog.start()
                    _spawn = _make_sibling_spawner(tg, shutdown_event, deps)

                    _spawn(
                        heartbeat_loop(
                            deps,
                            worker_id,
                            shutdown_event,
                            cancel_controller=make_cancel_controller(deps, worker_id, backend),
                            cancel_wake_event=cancel_wake_event,
                        )
                    )
                    _spawn(
                        progress_flush_loop(
                            # Resolved per flush tick so a credential
                            # hot-reload swap is picked up immediately ,
                            # capturing the pool here would leave the loop
                            # flushing through a drained pool after SIGHUP.
                            lambda: deps.worker_pool,
                            settings.schema_name,
                            worker_id,
                            deps.progress_buffers,
                            settings.progress_coalesce_interval,
                            shutdown_event,
                            liveness=deps.liveness,
                        )
                    )
                    # may_return: the notify listener falling back to
                    # poll-based dispatch is the one legitimate early return.
                    _spawn(
                        notify_listener_loop(
                            deps,
                            backend,  # type: ignore[arg-type]  # Why: notify_listener_loop expects PostgresBackend; the instance is PostgresBackend at runtime, pyright cannot narrow the Backend Protocol to the concrete class here
                            shutdown_event,
                            worker_id,
                        ),
                        may_return=True,
                    )
                    _spawn(
                        MaintenanceLeader(
                            deps,
                            worker_id,
                            backend,
                            clock=_clock,
                            rate_limit_registry=resolved_rl_registry,
                            actor_policies=actor_fire_policies,
                        ).run(shutdown_event)
                    )
                    # One event shared by the producer and every consumer
                    # loop: each consumer sets it when its
                    # local_queue.get() drains a slot (the slot-release
                    # point from the producer's accounting, see
                    # producer_loop's saturation branch), waking a
                    # saturated producer to claim immediately instead of
                    # on the next fallback poll.
                    slot_freed_event = asyncio.Event()
                    _spawn(
                        producer_loop(
                            deps,
                            local_queue,
                            shutdown_event,
                            deps.producer_stop_event,
                            backend=backend,
                            worker_id=worker_id,
                            slot_freed_event=slot_freed_event,
                        )
                    )
                    for _ in range(settings.max_concurrency):
                        if actor_registry is not None:
                            _spawn(
                                di_consumer_loop(
                                    deps,
                                    local_queue,
                                    shutdown_event,
                                    backend=backend,
                                    worker_id=worker_id,
                                    registry=registry,
                                    process_scope=process_scope,
                                    thread_scope=thread_scope,
                                    loop_scope=loop_scope,
                                    actor_registry=actor_registry,
                                    enqueuer=enqueuer,
                                    slot_freed_event=slot_freed_event,
                                )
                            )
                        else:
                            _spawn(
                                consumer_loop_stub(
                                    deps,
                                    local_queue,
                                    shutdown_event,
                                    backend=backend,
                                    worker_id=worker_id,
                                    slot_freed_event=slot_freed_event,
                                )
                            )

                    _spawn(
                        _reload_coordinator_loop(
                            deps,
                            shutdown_event,
                            loop_scope=loop_scope,
                            refresh_worker_pool_di=worker_pool_registered_in_di,
                        ),
                        name="worker.reload_coordinator",
                    )
                    if settings.watchdog_enabled:
                        # Never spawn the loop disabled: an early return with
                        # no shutdown in progress trips detector 3 (the master
                        # kill-switch must be the one path that cannot fail).
                        _spawn(
                            loop_watchdog_loop(
                                deps.liveness,
                                shutdown_event,
                                check_interval=settings.watchdog_check_interval,
                            ),
                            name="worker.loop_watchdog",
                        )

                    if until_idle:
                        from taskq.worker.drain import drain_monitor_loop

                        _spawn(
                            drain_monitor_loop(
                                deps,
                                settings,
                                worker_id,
                                shutdown_event,
                                escalate_event,
                                orchestrator_holder,
                                backend,
                                idle_settle_window=settle,
                                idle_poll_interval=poll,
                                max_runtime=runtime,
                            ),
                            may_return=True,
                            name="worker.drain_monitor",
                        )

                    try:
                        await shutdown_event.wait()
                    except asyncio.CancelledError:
                        # A raw cancel of _main (a parent task cancelling the
                        # worker, not a signal-driven shutdown) must still
                        # terminate the worker. The cancelling sibling's
                        # discipline above documents the trap: several loops
                        # absorb a CancelledError by design and re-check
                        # ``while not shutdown_event.is_set()``; with the
                        # event still clear they park again and the group's
                        # __aexit__ waits forever, leaving _main unkillable
                        # and the shutdown watchdog never disarmed. Raise the
                        # flag BEFORE the group starts collecting siblings so
                        # every park loop observes a shutdown in progress,
                        # drains, and exits; the shutdown watchdog sees the
                        # event too, so its deadline trip arms and bounds the
                        # whole exit exactly as on the signal path.
                        # Stamp the in-flight jobs as shutdown-interrupted
                        # BEFORE the re-raise starts the group's teardown:
                        # this path never runs orchestrate_shutdown (the
                        # orchestrator is reachable only from the signal and
                        # drain paths), so without the stamp every running
                        # job's registry entry stays origin-less and the
                        # consumer's terminal routing falls through to
                        # mark_cancelled, a phantom operator cancel. The
                        # bare cancel is the supervisor tearing the worker
                        # process down, the same external event the signal
                        # path delivers, and the signal path lands every
                        # in-flight job interrupted/pending; see
                        # _stamp_interrupt_origins for why only origin-less
                        # entries are stamped.
                        _stamp_interrupt_origins(deps)
                        shutdown_event.set()
                        # Stamp the shutdown start HERE, synchronously: the
                        # watchdog's own stamping is asynchronous (its task
                        # wakes through asyncio.wait hops and can lose the
                        # race to this finally on a group whose siblings are
                        # already dead). Without the stamp, the tracked-actor
                        # reap gate below reads None and skips its wait,
                        # falling back to the unbounded executor join the
                        # gate exists to prevent. Idempotent against the
                        # watchdog's own conditional stamp.
                        _stamp_shutdown_started(asyncio.get_running_loop().time())
                        raise
            finally:
                # The order here matters, and every statement must be
                # non-raising so the ones after it still run.
                #
                # The watchdogs are disarmed only AFTER the TaskGroup's exit
                # has completed: a wedged sibling hanging that exit is exactly
                # what detector 1 exists to catch, so cancelling them any
                # earlier would make the detector dead code on the only path
                # that matters. Both calls swallow their own errors.
                #
                # ── The tracked-actor reap gate (the exit bound) ───
                # Disarming now (the pre-existing shape) is only safe
                # when no actor can outlive the TaskGroup. A sync actor's
                # executor thread can: task.cancel() cancels the await,
                # never the thread, and with the watchdog disarmed the
                # clean path then parks in the default executor's join
                # (THREAD_JOIN_TIMEOUT, 300s) waiting for the very thread
                # the release hold assumed was gone: the row becomes
                # claimable at its held scheduled_at while its actor still
                # runs. The gate keeps the watchdog armed until every
                # tracked handle is reaped; the deadline trip is then the
                # process exit the hold always modeled, and the reap's
                # own wait is bounded by nothing else: deliberately,
                # because the trip is the bound. Gated on a shutdown
                # actually having started (a crashed TaskGroup with no
                # signal never armed the countdown; waiting there would
                # be unbounded, and the pre-existing executor join owns
                # that path) and on the watchdog being enabled (with it
                # disabled there is no trip to bound the wait, and the
                # hold has already degraded to lock_lease: the promise
                # is scoped accordingly in docs/guides/workers.md).
                # await_tracked_actor_reap is non-raising by construction
                # (a liveness poll), matching this block's discipline.
                if settings.watchdog_enabled and deps.shutdown_started_at is not None:
                    await await_tracked_actor_reap()
                await shutdown_watchdog.cancel()
                lag_watchdog.stop()
                # deregister_worker must run even when the group exit RAISED
                # (the sibling-crash path): a crashed worker that leaves its
                # workers row behind makes the supervisor's staleness check ,
                # the fleet-level backstop, start from a staler picture.
                try:
                    await deregister_worker(deps.dispatcher_pool, settings, worker_id)
                except Exception:
                    _producer_log.warning(
                        "deregister_worker_failed_in_cleanup",
                        worker_id=worker_id,
                    )

    if orchestrator_holder:
        exit_code = await orchestrator_holder[0]
    else:
        exit_code = 0
    return exit_code


def _stamp_interrupt_origins(deps: WorkerDeps) -> None:
    """Stamp every active job's cancel origin SHUTDOWN before the
    TaskGroup teardown delivers its cancellations.

    A crashing TaskGroup tears the consumers down by cancellation, and the
    consumer's terminal routing reads the registry entry's ``cancel_origin``:
    OPERATOR keeps the cancel ladder, SHUTDOWN releases the attempt back to
    the fleet via ``mark_interrupted``, and NONE falls through to
    ``mark_cancelled``, which fences only on id/status/worker/attempt. The
    pre-fix shape terminalised every in-flight job 'cancelled' with
    ``cancel_phase = 0`` and ``cancel_requested_at = NULL``, a phantom
    operator cancel no operator ever issued: it spent an attempt, wrote a
    ``job_attempts`` row with ``outcome='cancelled'``, fired ``on_cancel``,
    and was indistinguishable in the database from a real request (the
    phantom signature: a 'cancelled' row with a NULL ``cancel_requested_at``).

    A sibling crash is an infrastructure interruption, the same class of
    event the shutdown orchestrator's CANCELLING phase and ``isolate_self``
    stamp SHUTDOWN for, so the crash stamps the same origin. The stamp runs
    synchronously in the failing sibling, BEFORE ``shutdown_event.set()``
    and before the group's ``__aexit__`` starts cancelling the remaining
    siblings, so every job claimed BEFORE the stamp point carries its
    origin before its cancellation is delivered. That narrows the
    phantom-cancel window; it does not close it. A consumer can complete a
    fresh claim in the gap between the stamp and the delivery of that
    task's own cancellation, and the entry registered there is still
    origin-less when the consumer's terminal routing reads it: that one
    in-flight dispatch falls through to ``mark_cancelled`` exactly as
    pre-fix. Closing the window fully would need a claim-side fence the
    crash path does not have, so the residual exposure is bounded to the
    claims racing the stamp. Only
    origin-less entries are stamped: a real operator cancel already carries
    OPERATOR and keeps its ladder untouched; the row-side fences
    (``mark_interrupted``'s ``cancel_phase = 0``, the escalation probe)
    remain the final arbiters either way.

    Deliberately no cancel-phase or task changes here: the TaskGroup's own
    teardown delivers the cancellations, and a local phase stamp the row
    does not carry would lie about a ladder no one advanced.
    """
    registry = getattr(deps, "active_jobs", None)
    if registry is None:
        if isinstance(deps, WorkerDeps):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: not unnecessary at runtime - a MagicMock(spec=WorkerDeps) aliases __class__ to the spec, so this is the test that tells a production-shaped double from a registry-less stub; the static type is the real WorkerDeps, where the check is trivially true.
            # Loud, not silent: a production-shaped double (a
            # MagicMock(spec=WorkerDeps), say) that forgot to configure
            # active_jobs lands HERE - the field is a default_factory
            # dataclass field, so it is not in the spec's dir(), the read
            # raises AttributeError and getattr's default swallows it.
            # Stamping over that hole would quietly no-op: the exact
            # silent regression this stamp exists to prevent, surfacing
            # later as a phantom cancel on the next real crash. A real
            # WorkerDeps can never get here (the field always exists).
            raise TypeError(
                "deps.active_jobs is missing on a production-shaped deps "
                "surface: the interrupt stamp needs the real "
                "ActiveJobRegistry, and a silent no-op would turn the next "
                "real crash into a phantom cancel"
            )
        # A deps surface without a registry (the hand-built test stubs)
        # has nothing to stamp. The crash signal that follows is the
        # spawner's primary contract and must never depend on the stamp.
        return
    if not isinstance(registry, ActiveJobRegistry):
        # Loud for the same reason: whatever sits here is not the registry
        # the stamp must walk, and iterating it would silently no-op.
        raise TypeError(
            "deps.active_jobs must be an ActiveJobRegistry for the "
            f"interrupt stamp, got {type(registry).__name__}"
        )
    for active in registry.all():
        if active.cancel_origin is CancelOrigin.NONE:
            active.cancel_origin = CancelOrigin.SHUTDOWN
            active.ctx._set_cancel_origin(CancelOrigin.SHUTDOWN)  # pyright: ignore[reportPrivateUsage]  # Why: the crash path is the other designated shutdown-side writer of the context's origin stamp, same contract as the orchestrator's CANCELLING phase and isolate_self.


def _make_sibling_spawner(
    tg: asyncio.TaskGroup,
    shutdown_event: asyncio.Event,
    deps: WorkerDeps,
) -> Callable[..., asyncio.Task[None]]:
    """Build the spawner used for every long-lived sibling in ``_main``.

    A sibling that raises already tears the ``TaskGroup`` down, but the
    group's ``__aexit__`` then *waits* for the remaining siblings, and a
    cancelled sibling does not reliably stop. Several loops race a park
    against ``shutdown_event`` and clean up losers with
    ``suppress(asyncio.CancelledError)``; a cancellation delivered inside
    that suppress is swallowed, and a consumer that treats
    ``CancelledError`` as a cooperative job-cancel absorbs it by design.
    Either way the loop re-checks ``while not shutdown_event.is_set()``,
    sees it clear, and parks again forever: the worker never exits, and
    the original exception never surfaces because ``__aexit__`` never
    returns (observed as a 120 s+ hang with no traceback when a leader
    sweep hit a dead PG).

    Setting ``shutdown_event`` on the way out of a failing sibling closes
    that gap: the shutdown flag every loop already honours is raised, so
    the group drains promptly and the ExceptionGroup propagates. Only the
    failure path signals, a sibling that returns cleanly (e.g. the notify
    listener disabling itself and falling back to poll-based dispatch)
    must not bring the worker down.
    """

    async def _guarded(coro: Coroutine[Any, Any, None], *, may_return: bool) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            # Normal cancellation (the group's own cancel logic): not a
            # crash, not a contract issue, and NOT counted, one real fault
            # cancelling N siblings must not report N+1 crashes.
            raise
        except BaseException:
            _sibling_crashes.add(1, {"loop": getattr(coro, "__qualname__", repr(coro))})
            # Stamp BEFORE shutdown_event.set(): both are synchronous, but
            # the stamp must be in place before the group's teardown starts
            # cancelling the consumer siblings, whose terminal routing reads
            # it (see _stamp_interrupt_origins).
            _stamp_interrupt_origins(deps)
            shutdown_event.set()
            raise
        if may_return:
            # may_return skips ONLY the clean-return check; exceptions
            # above still set shutdown_event and count the crash.
            return
        shutdown_in_progress = (
            shutdown_event.is_set()
            or deps.shutdown_phase is not ShutdownPhase.NONE
            or deps.producer_stop_event.is_set()
        )
        if not shutdown_in_progress:
            msg = (
                f"sibling {getattr(coro, '__qualname__', repr(coro))} returned "
                "cleanly while no shutdown was in progress"
            )
            # Why the log is unconditional: watchdog_enabled=False gates the
            # enforcement below, not the signal. Without this record a
            # clean-returned sibling leaves the worker running half-staffed
            # with no log, no metric, no signal: exactly the silence the
            # detector exists to prevent, one switch earlier.
            _startup_log.error(
                "sibling-returned-unexpectedly",
                kind="sibling_returned_unexpectedly",
                sibling=msg,
            )
            _settings = getattr(deps, "settings", None)
            if getattr(_settings, "watchdog_enabled", True):
                shutdown_event.set()
                raise RuntimeError(msg)

    def _spawn(
        coro: Coroutine[Any, Any, None],
        *,
        may_return: bool = False,
        **kwargs: Any,
    ) -> asyncio.Task[None]:
        return tg.create_task(_guarded(coro, may_return=may_return), **kwargs)

    return _spawn


def _worker_reload_schedule(deps: WorkerDeps) -> ReloadSchedule:
    """The cadence every factory-backed resource on *deps* is rebuilt on.

    ``settings.reload_interval`` when the operator set one; otherwise
    derived from the leases the factories were granted when the pools were
    built (each factory declares the :class:`~taskq.auth.ReloadSchedule` it
    records on, read back with :func:`~taskq.auth.reload_schedule_of`).
    The role factories built by :func:`taskq.auth.build_worker_connections`
    share one schedule; a hand-assembled ``WorkerConnections`` may carry
    one per factory, so the schedules are composed and the shortest lease
    across them wins. Live: a lease recorded on a later rebuild is seen
    through the composite.
    """
    declared: dict[int, ReloadSchedule] = {}
    for factory in (
        deps.dispatcher_pool_factory,
        deps.heartbeat_pool_factory,
        deps.worker_pool_factory,
        deps.slot_pool_factory,
        deps.notify_conn_factory,
        deps.leader_conn_factory,
    ):
        schedule = reload_schedule_of(factory) if factory is not None else None
        if schedule is not None:
            declared.setdefault(id(schedule), schedule)
    return ReloadSchedule(
        configured=deps.settings.reload_interval, sources=tuple(declared.values())
    )


async def _reload_coordinator_loop(
    deps: WorkerDeps,
    shutdown: asyncio.Event,
    *,
    loop_scope: LoopScope | None = None,
    refresh_worker_pool_di: bool = False,
) -> None:
    """Trigger credential hot-reload on SIGHUP, on a timer, or on request.

    Runs as a sibling task in the worker's ``TaskGroup``. Reloads are
    triggered by ``deps.reload_event`` (set by the SIGHUP handler or by
    :meth:`~taskq.worker.deps.WorkerDeps.request_reload`) and by a periodic
    timer on the worker's :class:`~taskq.auth.ReloadSchedule` ,
    ``settings.reload_interval`` when set, otherwise the cadence derived
    from the shortest lease any factory-backed pool or connection was
    granted (half its TTL), and no timer when neither is known, the
    rotation path for platforms without SIGHUP and for hands-off
    scheduled rotation. Each trigger calls
    :func:`~taskq.worker.deps.reload_credentials` to hot-swap every
    factory-backed pool / connection / Redis client with freshly-built
    replacements.

    Semantics:

    * The event is cleared *before* each reload and never cleared after,
      so a SIGHUP arriving mid-reload, success OR failure, is honored
      by exactly one follow-up reload. (Event coalescing: N signals
      during one reload produce one follow-up, not N.)
    * Reloads are skipped while shutdown orchestration is in progress
      (``deps.shutdown_phase`` is not NONE): churning pools on a draining
      worker is wasteful, and the leader watchdog could re-acquire
      leadership mid-shutdown.
    * When the reload swapped the worker pool and the worker (not the
      user) registered ``asyncpg.Pool`` in DI, the LOOP-scope cache is
      refreshed so actors injected with ``db: asyncpg.Pool`` resolve the
      live pool instead of the drained one.
    * A reload exception is logged and the worker continues, old
      resources are still live; the operator can SIGHUP again.
    """
    from taskq.worker.deps import reload_credentials

    schedule = _worker_reload_schedule(deps)
    if schedule.derived:
        _startup_log.info(
            "reload-interval-derived-from-lease",
            reload_interval=schedule.interval,
            lease_duration=schedule.lease_duration,
            reason="TASKQ_RELOAD_INTERVAL is unset; pools pinned to an issued lease pair "
            "are rebuilt at half the shortest lease TTL the provider granted",
        )

    while not shutdown.is_set():
        # Re-read per wait: a lease granted shorter on a rebuild tightens
        # the cadence from the next wait on.
        interval = schedule.interval
        # Wait for a reload request, the interval timer, or shutdown.
        waiters: list[
            asyncio.Task[Any]
        ] = [  # Why: heterogeneous task results (Event.wait → bool, sleep → None); results are never read, only completion matters.
            asyncio.create_task(deps.reload_event.wait()),
            asyncio.create_task(shutdown.wait()),
        ]
        if interval is not None:
            waiters.append(asyncio.create_task(asyncio.sleep(interval)))
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in waiters:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

        if shutdown.is_set():
            return

        deps.reload_event.clear()

        if deps.shutdown_phase is not ShutdownPhase.NONE:
            _startup_log.info(
                "credentials-reload-skipped",
                reason="shutdown-in-progress",
                shutdown_phase=deps.shutdown_phase.name,
            )
            continue

        try:
            reloaded, _failed = await reload_credentials(
                deps, factory_timeout=deps.settings.reload_factory_timeout
            )
        except Exception:
            _startup_log.exception("credentials-reload-failed")
            continue

        if refresh_worker_pool_di and loop_scope is not None and "worker" in reloaded:
            # The worker pool was hot-swapped, refresh the DI cache so
            # actors injected with db: asyncpg.Pool get the live pool, not
            # the one now draining in the background.
            try:
                loop_scope.replace_value(asyncpg.Pool, deps.worker_pool)
            except KeyError:
                # Unreachable via _main (bootstrap eagerly caches all LOOP
                # providers), but the kwarg contract permits an
                # un-bootstrapped loop_scope, a raise here would tear down
                # the worker's TaskGroup.
                _startup_log.warning("di-worker-pool-refresh-skipped", reason="not-cached")
            else:
                _startup_log.info("di-worker-pool-refreshed")


def worker_main(
    settings: WorkerSettings,
    *,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    di_registry: ProviderRegistry | None = None,
    cron_registry: list[CronScheduleSpec] | None = None,
    connections: WorkerConnections | None = None,
    pg_credential_provider: PgCredentialProvider | None = None,
    rate_limit_registry: RateLimitRegistry | None = None,
    until_idle: bool = False,
    idle_settle_window: float | None = None,
    idle_poll_interval: float | None = None,
    idle_max_runtime: float | None = None,
) -> int:
    """Worker process entry point (synchronous).

    Thin wrapper: runs :func:`worker_main_async` under an ``asyncio.Runner``
    and returns its int result. Uses ``Runner`` (not ``asyncio.run``) for
    finer control over teardown.

    Because this function owns the loop, an application that must build
    loop-bound dependencies (asyncpg pools) before constructing its actors
    cannot use it - await :func:`worker_main_async` from its own coroutine
    instead. Both take exactly these parameters.

    ``actor_registry`` is a mapping from short name to :class:`ActorRef`
    containing every ``@actor``-decorated handler this worker intends to
    run. Forwarded to :func:`_main` for the  bootstrap config sync.

    ``pg_credential_provider`` is the resolved Postgres credential
    provider for the worker-internal per-slot transaction pool (see
    :func:`_main`). Pass the same provider used to build
    ``connections``, the slot pool does not read WorkerConnections.
    ``None`` authenticates it from the direct DSN.

    ``di_registry`` is an optional pre-configured :class:`ProviderRegistry`
    containing application-specific provider registrations (database pools,
    HTTP clients, etc.).  When supplied, the worker uses it instead of
    creating a fresh registry, callers must NOT call ``validate()`` before
    passing it here; the worker calls ``validate()`` as part of its bootstrap
    sequence.  ``WorkerSettings`` and ``Clock`` are registered automatically
    if not already present.

    ``rate_limit_registry`` is an optional owned :class:`RateLimitRegistry`
    for this worker (e.g. one instance per process in a multi-process
    deployment).  When ``None``, resolution falls back to a
    ``RateLimitRegistry`` value provider at ``Scope.LOOP`` in
    ``di_registry``, then to the module singleton, import-time
    ``.register()`` on the singleton keeps working exactly as before.
    Co-present with a ``RateLimitRegistry`` provider in ``di_registry``
    this raises ``TypeError`` (ambiguous, bootstrap and dispatch would
    diverge); pass one or the other.
    Forwarded to :func:`_main`.

    ``cron_registry`` is an optional list of :class:`CronScheduleSpec`
    objects to auto-register at startup.  When ``None`` (the default),
    ``get_registered_crons()`` is used instead, schedules declared via
    the ``@cron`` decorator are auto-discovered.  When an explicit list
    is passed (even empty ``[]``), only those schedules are registered;
    decorator-registered schedules are skipped.  For each spec, a direct
    ``INSERT INTO … cron_schedules`` is executed inside
    ``try/except asyncpg.UniqueViolationError: pass``, the DB ``(actor, name)``
    UNIQUE constraint prevents duplicates, so concurrent worker replicas
    can safely race.  Startup auto-discovery is **create-only,
    skip-on-conflict**: existing ``cron_schedules`` rows are never
    modified by the registration pass.  If a ``@cron`` decorator's
    parameters change after the schedule was first registered, the
    operator must manually update or delete and recreate the schedule.

    ``until_idle`` enables drain mode (see :func:`_main` for details).
    ``idle_settle_window``, ``idle_poll_interval``, and
    ``idle_max_runtime`` override the corresponding settings when
    ``until_idle`` is True; they are ignored otherwise.

    Returns the exit code from :func:`_main`, 0 on clean shutdown,
    3 if any jobs failed in drain mode, 4 if ``idle_max_runtime`` was
    exceeded.
    """
    with asyncio.Runner() as runner:
        code = runner.run(
            worker_main_async(
                settings,
                actor_registry=actor_registry,
                di_registry=di_registry,
                connections=connections,
                cron_registry=cron_registry,
                pg_credential_provider=pg_credential_provider,
                rate_limit_registry=rate_limit_registry,
                until_idle=until_idle,
                idle_settle_window=idle_settle_window,
                idle_poll_interval=idle_poll_interval,
                idle_max_runtime=idle_max_runtime,
            )
        )
    # The Runner is closed, and closing the loop removed the SIGTERM handler
    # (asyncio restores SIG_DFL on close) - but the process is not gone yet:
    # returning the code, interpreter teardown, atexit hooks and thread joins
    # all still run, and a loaded host stretches every one of them. A signal
    # in that window kills the process with -15 and ERASES the drain's
    # verdict: the pod's exit status stops saying what the drain said (the
    # observed fleet-wide-storm shape - the orchestrator's redundant
    # stop-signal landing after one pod had already drained cleanly). The
    # escalation contract only has meaning while the loop lives; once it is
    # closed there is nothing left to escalate, so SIGTERM is ignored and the
    # exit status stays the drain's. SIGINT is left alone: its default
    # disposition is KeyboardInterrupt, a live interactive contract, not a
    # silent status eraser.
    with contextlib.suppress(ValueError):  # Why: not the main thread -> no window to guard.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return code


async def worker_main_async(
    settings: WorkerSettings,
    *,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    di_registry: ProviderRegistry | None = None,
    cron_registry: list[CronScheduleSpec] | None = None,
    connections: WorkerConnections | None = None,
    pg_credential_provider: PgCredentialProvider | None = None,
    rate_limit_registry: RateLimitRegistry | None = None,
    until_idle: bool = False,
    idle_settle_window: float | None = None,
    idle_poll_interval: float | None = None,
    idle_max_runtime: float | None = None,
) -> int:
    """Run a worker to completion on the **caller's** event loop.

    Same parameters, same semantics and same exit codes as
    :func:`worker_main` - which is now the thin synchronous wrapper that
    drives this coroutine under its own ``asyncio.Runner``. See
    :func:`worker_main` for what each parameter means.

    Why this exists as public API: ``worker_main`` owns the loop, and
    asyncpg pools are **loop-bound**. An application whose actors are
    closures over its own dependencies has to build those dependencies
    (pools, HTTP clients) on the loop the worker will run on, *before* the
    actor registry is constructed - and there is no hook in ``worker_main``
    to do that. Pre-creating a pool in a throwaway loop and handing it to
    ``worker_main``'s Runner is not an option: the pool would belong to a
    dead loop. Awaiting this from inside the caller's own coroutine puts
    dependency construction, actor construction, and the worker on one
    loop::

        async def main() -> int:
            pool = await asyncpg.create_pool(dsn)          # this loop
            registry = build_actors(pool)                  # closures over it
            try:
                return await worker_main_async(settings, actor_registry=registry)
            finally:
                await pool.close()

        raise SystemExit(asyncio.run(main()))

    Signal handling, logging setup and cron auto-discovery are identical to
    :func:`worker_main`; nothing about running here changes the worker's
    behaviour. The caller owns whatever it built - TaskQ closes only the
    resources it created itself (see :class:`~taskq.connections.WorkerConnections`
    for the ownership rule that governs anything passed via ``connections``).
    """
    from taskq.scheduler import get_registered_crons

    schedule_specs = cron_registry if cron_registry is not None else get_registered_crons()
    setup_logging(level=settings.log_level, log_format=settings.log_format)
    return await _main(
        settings,
        actor_registry=actor_registry,
        _registry=di_registry,
        _cron_registry=schedule_specs,
        connections=connections,
        pg_credential_provider=pg_credential_provider,
        rate_limit_registry=rate_limit_registry,
        until_idle=until_idle,
        idle_settle_window=idle_settle_window,
        idle_poll_interval=idle_poll_interval,
        idle_max_runtime=idle_max_runtime,
    )
