"""Worker bootstrap: _main coroutine and process entry point.

The ``_main`` coroutine wires the full TaskGroup of long-lived siblings
(signal handlers, cron registration, pool setup, producer/consumer tasks).
``worker_main`` is the process entry point that runs ``_main`` under an
``asyncio.Runner``.

``_emit_sub_enqueue_startup_warnings`` checks LOOP-scope connection
resolution and warns about PgBouncer transaction-mode footguns.
"""

import asyncio
import contextlib
import importlib.util
from collections.abc import Callable, Coroutine, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import structlog

from taskq._di import ProviderRegistry, Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._dsn import dsn_host as _dsn_host
from taskq.actor import ActorRef
from taskq.backend._protocol import Backend, JobRow, ScheduleCreateArgs
from taskq.backend.clock import Clock, SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.connections import WorkerConnections
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex for defence-in-depth schema validation at this SQL interpolation site, per architecture.md §8 Invariant 4
)
from taskq.cron import (
    CronScheduleSpec,
    compute_next_fire_after,
)
from taskq.exceptions import MissingProvider
from taskq.obs import get_meter, set_otel_enabled, setup_logging
from taskq.progress._flush import progress_flush_loop
from taskq.ratelimit._provider import register_rate_limit_registry, register_redis_pool
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.registry import registry as rl_registry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.worker._watchdog import LoopLagWatchdog, ShutdownWatchdog, loop_watchdog_loop
from taskq.worker.actor_config import ActorConfig
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.health import HealthServer
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.leader import MaintenanceLeader
from taskq.worker.notify import notify_listener_loop
from taskq.worker.shutdown import ShutdownPhase, install_signal_handlers
from taskq.worker.startup import sync_actor_config

__all__ = ["_emit_sub_enqueue_startup_warnings", "_main", "worker_main"]

_startup_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.worker.run.startup")

_sibling_crashes = get_meter().create_counter(
    "taskq.worker.sibling_crashes_total",
    unit="1",
    description="Sibling task exits by exception, labelled by loop.",
)


def _redis_extra_installed() -> bool:
    """Whether the ``[redis]`` extra is importable in this environment."""
    return importlib.util.find_spec("redis.asyncio") is not None


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


def _served_redis_rate_limits(
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None,
) -> list[str]:
    """Names of redis-backed rate limits declared by this worker's actors.

    Scoped to served actors: the rate-limit registry is process-global and
    may carry limits for actors this worker never dispatches — a global
    scan would brick an unrelated worker. Walks both named limits
    (resolved against the registry) and :class:`KeyedRateLimitRef`
    declarations, whose concrete buckets materialize only at first acquire
    and are therefore invisible to a registry scan.
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
            elif limit.backend == "redis":
                offending.add(limit.base_name)
    return sorted(offending)


def _emit_sub_enqueue_startup_warnings(
    loop_scope: LoopScope,
    settings: WorkerSettings,
    actor_registry: Mapping[str, ActorRef[Any, Any]],
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Emit startup warnings for sub-enqueue connection resolution.

    Two checks, mutually exclusive:

    1. No LOOP-scope ``asyncpg.Connection`` provider registered → warn
       that ``ctx.jobs.enqueue`` will use autonomous commit ().
    2. LOOP-scope conn registered but DSNs differ → warn about the
       PgBouncer transaction-mode footgun ().
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
                "commit via worker_pool. Register asyncpg.Connection "
                "at Scope.LOOP for transactional sub-job enqueue."
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


def _resolve_rl_registry(
    explicit: RateLimitRegistry | None,
    di_registry: ProviderRegistry,
) -> RateLimitRegistry:
    """Resolve this worker's ``RateLimitRegistry`` (documented order).

    1. An explicit ``rate_limit_registry=`` argument wins — but co-present
       with a DI ``RateLimitRegistry`` provider it raises ``TypeError``
       (ambiguous: bootstrap and dispatch would diverge).
    2. A ``RateLimitRegistry`` provider pre-registered in *di_registry* —
       **value providers only**: a factory/class provider would split-brain
       (bootstrap using one instance while LOOP-scope dispatch resolution
       produced another), so it fails fast with ``TypeError``.
    3. The module singleton (unchanged backwards-compatible default).

    Naming: ``_registry`` / *di_registry* is the DI ``ProviderRegistry``
    (container); the returned object is the ``RateLimitRegistry``
    (primitive store). They are unrelated despite the similar names.
    """
    if explicit is not None and di_registry.has_provider(RateLimitRegistry):
        raise TypeError(
            "rate_limit_registry= was passed explicitly AND a RateLimitRegistry "
            "provider is registered in di_registry — ambiguous configuration; "
            "pass one or the other, not both"
        )
    if explicit is not None:
        return explicit
    if di_registry.has_provider(RateLimitRegistry):
        entry = di_registry.get(RateLimitRegistry)
        if entry.kind != "value":
            raise TypeError(
                "RateLimitRegistry must be registered as a value provider "
                f"(register_value), got kind={entry.kind!r} — the worker must "
                "resolve one concrete instance at bootstrap"
            )
        return cast(RateLimitRegistry, entry.impl)
    return rl_registry


async def _main(
    settings: WorkerSettings,
    *,
    _local_queue_seed: list[JobRow] | None = None,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    _registry: ProviderRegistry | None = None,
    _cron_registry: list[CronScheduleSpec] | None = None,
    connections: WorkerConnections | None = None,
    rate_limit_registry: RateLimitRegistry | None = None,
) -> int:
    """Worker bootstrap: open deps, wire TaskGroup of siblings, run to shutdown.

    ``_local_queue_seed`` is a test seam — keyword-only, defaults to ``None``,
    prefixed with ``_`` to mark it as non-production API.  When not ``None``,
    each job in the seed list is pushed onto ``local_queue`` BEFORE the
    TaskGroup starts, so consumer stubs immediately consume them.
    Production callers (``worker_main``) MUST NOT pass this parameter.

    ``actor_registry`` is a mapping from short name to :class:`ActorRef`
    containing every ``@actor``-decorated handler this worker intends to
    run. When not ``None``, :func:`sync_actor_config` is called after
    ``register_worker`` and before the ``TaskGroup`` opens so dispatch
    queries always see registered concurrency caps.

    ``_registry`` is a test seam — keyword-only, defaults to ``None``,
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
    asyncpg.UniqueViolationError: pass`` — the ``(actor, name)`` UNIQUE
    constraint makes this registration pass create-only and skip-on-conflict.

    ``rate_limit_registry`` is the :class:`RateLimitRegistry` this worker
    owns and dispatches against.  Resolution order: explicit argument →
    ``RateLimitRegistry`` value provider in ``_registry`` → module
    singleton (see :func:`_resolve_rl_registry`).  Co-present with a
    ``RateLimitRegistry`` provider in ``_registry`` this raises
    ``TypeError`` (ambiguous — bootstrap and dispatch would diverge);
    pass one or the other.  Actor-declared primitive instances
    (``@actor(rate_limits=[TokenBucket(...)])``) are collected and
    registered into the resolved registry before ``validate()`` runs.

    Returns the exit code from the orchestrator (read from the holder), or
    0 when no signal arrived (clean shutdown via external shutdown_event.set()).
    """
    from taskq.worker.run import (
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
        # key == ref.name is the load-bearing invariant; enforcing it here
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
    # log counts DECLARATIONS (not distinct new registrations) — the same
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
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type]  # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol — pyright cannot verify dict covariance across the Protocol boundary

    loop = asyncio.get_running_loop()

    set_otel_enabled(settings.otel_enabled)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    _producer_log = structlog.get_logger("taskq.worker.run.producer")

    async with open_worker_deps(settings, connections=connections) as deps:
        # Only register the worker pool in DI when the user hasn't provided
        # their own asyncpg.Pool provider — and only then may the reload
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
            # burned retries) — fail fast at bootstrap, naming the
            # offending limiter(s).
            redis_backed = _served_redis_rate_limits(actor_registry)
            if redis_backed:
                msg = (
                    "Redis-backed rate limit(s) declared by served actors but no Redis "
                    f"is configured: {', '.join(redis_backed)}. Set TASKQ_REDIS_URL or "
                    "register a redis.asyncio.Redis DI provider."
                )
                raise RuntimeError(msg)
        if settings.redis_url is not None:
            if not _redis_extra_installed():
                # Why: without this check the missing extra surfaces later as
                # a bare MissingProvider at DI validate — no hint that the
                # fix is installing the package. Only raise when Redis is
                # actually required: a URL set without redis-backed limits
                # is harmless (register_redis_pool silently skips).
                redis_backed = _served_redis_rate_limits(actor_registry)
                if redis_backed:
                    msg = (
                        "TASKQ_REDIS_URL is set but the [redis] extra is not "
                        "installed; it is required by rate limit(s): "
                        f"{', '.join(redis_backed)}. Install it with: "
                        "pip install 'taskq[redis]'"
                    )
                    raise RuntimeError(msg)
            # Why: LoopScope.bootstrap eagerly resolves every LOOP provider,
            # and get_redis_pool raises when redis_url is None — registering
            # unconditionally would crash workers that don't use Redis.
            register_redis_pool(registry)
        registry.validate(actors=actors_list, rate_limit_registry=resolved_rl_registry)

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

        process_scope = ProcessScope(resolver=resolver)
        scope_containers[Scope.PROCESS] = process_scope
        await process_scope.bootstrap(registry, settings)

        thread_scope = ThreadScope(resolver=resolver)
        scope_containers[Scope.THREAD] = thread_scope
        await thread_scope.bootstrap(registry, process_scope)

        loop_scope = LoopScope(resolver=resolver)
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
                )
                for ref in actor_registry.values()
            ]
            async with deps.dispatcher_pool.acquire() as conn:
                await sync_actor_config(
                    conn,
                    actor_configs,
                    force=settings.force_update_actor_config,
                    schema=settings.schema_name,
                )

            for res in own_reservations:
                try:
                    await res.ensure_slots(deps.dispatcher_pool)
                except Exception as exc:
                    _startup_log.warning(
                        "ensure_slots_failed",
                        bucket_name=res.name,
                        error=str(exc),
                    )

        # Fleet-wide per-queue concurrency caps (DB-driven): query the
        # queues table for queues this worker consumes that have a
        # max_concurrent set, register a ConcurrencyReservation for each,
        # and sync their slot rows to match. The DB is the single source
        # of truth — read at worker startup, avoiding configuration drift
        # across a fleet of workers during rolling deploys (the footgun
        # the settings-based design had). The lease is set to lock_lease
        # so the heartbeat extends it in lockstep with job locks; if a
        # worker dies, both the job lock and the queue reservation slot
        # expire at roughly the same time and the recovery sweep reclaims
        # them. register_queue_cap_reservation() is idempotent for identical
        # config (the public register() rejects names in the reserved
        # queue-cap namespace to prevent user shadowing). sync_slots
        # (not ensure_slots) is used so that BOTH growing AND shrinking a
        # cap take effect on restart — ensure_slots is purely additive
        # (INSERT ... ON CONFLICT DO NOTHING) and could never remove
        # excess slots, so lowering max_concurrent was a silent no-op.
        # sync_slots inserts missing slots, deletes excess free slots, and
        # skips held slots (reporting them) — a strict superset of
        # ensure_slots, so initial registration works identically.
        from taskq.ratelimit.registry import queue_concurrency_reservation_name

        if not _IDENT_RE.match(settings.schema_name):
            raise ValueError(f"invalid schema identifier: {settings.schema_name!r}")
        # This query is as hard-required as sync_actor_config / register_worker
        # elsewhere in this same _main function — neither of those is wrapped
        # in a broad try/except. The only exception we catch specifically is
        # UndefinedColumnError, which signals that migration 01.00.04 has not
        # been applied (the queues.max_concurrent column is absent). That is
        # a deployment mistake that must crash startup loudly, not a
        # best-effort condition to warn about. Any other exception (connection
        # errors, etc.) propagates and crashes startup exactly like every
        # other hard-required startup step in this function already does —
        # this is a deliberate consistency choice, not an oversight.
        try:
            async with deps.dispatcher_pool.acquire() as conn:
                cap_rows = await conn.fetch(
                    f'SELECT name, max_concurrent FROM "{settings.schema_name}".queues '  # noqa: S608  # Why: schema validated at construction and re-checked above; asyncpg cannot bind identifiers.
                    f"WHERE name = ANY($1) AND max_concurrent IS NOT NULL",
                    settings.queues,
                )
        except asyncpg.exceptions.UndefinedColumnError as exc:
            raise RuntimeError(
                f"queues.max_concurrent column is missing in schema "
                f"{settings.schema_name!r} — migration "
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
            # Fail loudly — deliberately NOT warn-and-continue. The
            # reservations were registered above, and dispatch prepends the
            # cap name as a plain string, so the acquire path has no
            # ensure_slots retry: a sync_slots failure here would leave the
            # cap registered with zero (or stale) slot rows, and EVERY
            # dispatch on those queues would snooze with
            # ReservationUnavailable until a human restarted the worker —
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
                    f"{settings.schema_name!r}: {exc!r} — refusing to start "
                    f"with queue caps registered but unslotted, which would "
                    f"deny every dispatch on those queues until restart. Fix "
                    f"the underlying error and restart the worker; startup "
                    f"retries re-run this sync idempotently."
                ) from exc

        if _cron_registry:
            for spec in _cron_registry:
                next_fires = compute_next_fire_after(
                    spec.cron_expr,
                    spec.timezone,
                    datetime.now(tz=UTC),
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
                            name=spec.name,
                            identity_key=spec.identity_key,
                            metadata=metadata,
                        )
                    )
                except asyncpg.UniqueViolationError:
                    # Why: the (actor, name) UNIQUE constraint means a schedule
                    # for this (actor, name) already exists; this registration
                    # pass is insert-only and never modifies existing rows.
                    _startup_log.debug(
                        "cron-schedule-already-registered",
                        actor=spec.actor,
                        name=spec.name,
                        expr=spec.cron_expr,
                    )
                else:
                    _startup_log.info(
                        "cron-schedule-registered",
                        actor=spec.actor,
                        expr=spec.cron_expr,
                        next_fire_at=next_fire.isoformat(),
                    )

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
                await health_server.start(deps)
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
            lag_watchdog = LoopLagWatchdog(
                asyncio.get_running_loop(),
                deps.liveness,
                budget=settings.watchdog_loop_lag_budget,
                startup_grace=settings.watchdog_loop_lag_startup_grace,
                poll_interval=settings.watchdog_check_interval,
                enabled=settings.watchdog_enabled,
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
            # A plain ``async with`` plus one finally satisfies both
            # requirements — see that finally for the ordering rationale. A
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
                            # hot-reload swap is picked up immediately —
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
                            backend,  # type: ignore[arg-type]  # Why: notify_listener_loop expects PostgresBackend; the instance is PostgresBackend at runtime — pyright cannot narrow the Backend Protocol to the concrete class here
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
                        ).run(shutdown_event)
                    )
                    _spawn(
                        producer_loop(
                            deps,
                            local_queue,
                            shutdown_event,
                            deps.producer_stop_event,
                            backend=backend,
                            worker_id=worker_id,
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

                    await shutdown_event.wait()
            finally:
                # The order here matters, and every statement must be
                # non-raising so the ones after it still run.
                #
                # The watchdogs are disarmed only AFTER the TaskGroup's exit
                # has completed: a wedged sibling hanging that exit is exactly
                # what detector 1 exists to catch, so cancelling them any
                # earlier would make the detector dead code on the only path
                # that matters. Both calls swallow their own errors.
                await shutdown_watchdog.cancel()
                lag_watchdog.stop()
                # deregister_worker must run even when the group exit RAISED
                # (the sibling-crash path): a crashed worker that leaves its
                # workers row behind makes the supervisor's staleness check —
                # the fleet-level backstop — start from a staler picture.
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


def _make_sibling_spawner(
    tg: asyncio.TaskGroup,
    shutdown_event: asyncio.Event,
    deps: WorkerDeps,
) -> Callable[..., asyncio.Task[None]]:
    """Build the spawner used for every long-lived sibling in ``_main``.

    A sibling that raises already tears the ``TaskGroup`` down, but the
    group's ``__aexit__`` then *waits* for the remaining siblings — and a
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
    failure path signals — a sibling that returns cleanly (e.g. the notify
    listener disabling itself and falling back to poll-based dispatch)
    must not bring the worker down.
    """

    async def _guarded(coro: Coroutine[Any, Any, None], *, may_return: bool) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            # Normal cancellation (the group's own cancel logic): not a
            # crash, not a contract issue, and NOT counted — one real fault
            # cancelling N siblings must not report N+1 crashes.
            raise
        except BaseException:
            _sibling_crashes.add(1, {"loop": getattr(coro, "__qualname__", repr(coro))})
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
    :meth:`~taskq.worker.deps.WorkerDeps.request_reload`) and, when
    ``settings.reload_interval`` is set, by a periodic timer — the
    rotation path for platforms without SIGHUP and for hands-off
    scheduled rotation. Each trigger calls
    :func:`~taskq.worker.deps.reload_credentials` to hot-swap every
    factory-backed pool / connection / Redis client with freshly-built
    replacements.

    Semantics:

    * The event is cleared *before* each reload and never cleared after,
      so a SIGHUP arriving mid-reload — success OR failure — is honored
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
    * A reload exception is logged and the worker continues — old
      resources are still live; the operator can SIGHUP again.
    """
    from taskq.worker.deps import reload_credentials

    interval = deps.settings.reload_interval

    while not shutdown.is_set():
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
            # The worker pool was hot-swapped — refresh the DI cache so
            # actors injected with db: asyncpg.Pool get the live pool, not
            # the one now draining in the background.
            try:
                loop_scope.replace_value(asyncpg.Pool, deps.worker_pool)
            except KeyError:
                # Unreachable via _main (bootstrap eagerly caches all LOOP
                # providers), but the kwarg contract permits an
                # un-bootstrapped loop_scope — a raise here would tear down
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
    rate_limit_registry: RateLimitRegistry | None = None,
) -> int:
    """Worker process entry point.

    Runs ``_main`` under an ``asyncio.Runner`` and returns its int result.
    Uses ``Runner`` (not ``asyncio.run``) for finer control over teardown.

    ``actor_registry`` is a mapping from short name to :class:`ActorRef`
    containing every ``@actor``-decorated handler this worker intends to
    run. Forwarded to :func:`_main` for the  bootstrap config sync.

    ``di_registry`` is an optional pre-configured :class:`ProviderRegistry`
    containing application-specific provider registrations (database pools,
    HTTP clients, etc.).  When supplied, the worker uses it instead of
    creating a fresh registry — callers must NOT call ``validate()`` before
    passing it here; the worker calls ``validate()`` as part of its bootstrap
    sequence.  ``WorkerSettings`` and ``Clock`` are registered automatically
    if not already present.

    ``rate_limit_registry`` is an optional owned :class:`RateLimitRegistry`
    for this worker (e.g. one instance per process in a multi-process
    deployment).  When ``None``, resolution falls back to a
    ``RateLimitRegistry`` value provider in ``di_registry``, then to the
    module singleton — import-time ``.register()`` on the singleton keeps
    working exactly as before.  Co-present with a ``RateLimitRegistry``
    provider in ``di_registry`` this raises ``TypeError`` (ambiguous —
    bootstrap and dispatch would diverge); pass one or the other.
    Forwarded to :func:`_main`.

    ``cron_registry`` is an optional list of :class:`CronScheduleSpec`
    objects to auto-register at startup.  When ``None`` (the default),
    ``get_registered_crons()`` is used instead — schedules declared via
    the ``@cron`` decorator are auto-discovered.  When an explicit list
    is passed (even empty ``[]``), only those schedules are registered;
    decorator-registered schedules are skipped.  For each spec, a direct
    ``INSERT INTO … cron_schedules`` is executed inside
    ``try/except asyncpg.UniqueViolationError: pass`` — the DB ``(actor, name)``
    UNIQUE constraint prevents duplicates, so concurrent worker replicas
    can safely race.  Startup auto-discovery is **create-only,
    skip-on-conflict**: existing ``cron_schedules`` rows are never
    modified by the registration pass.  If a ``@cron`` decorator's
    parameters change after the schedule was first registered, the
    operator must manually update or delete and recreate the schedule.
    """
    from taskq.scheduler import get_registered_crons

    schedule_specs = cron_registry if cron_registry is not None else get_registered_crons()
    setup_logging(level=settings.log_level, log_format=settings.log_format)
    with asyncio.Runner() as runner:
        return runner.run(
            _main(
                settings,
                actor_registry=actor_registry,
                _registry=di_registry,
                _cron_registry=schedule_specs,
                connections=connections,
                rate_limit_registry=rate_limit_registry,
            )
        )
