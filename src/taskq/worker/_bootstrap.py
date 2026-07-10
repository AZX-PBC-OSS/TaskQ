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
from collections.abc import Mapping
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
from taskq.cron import (
    CronScheduleSpec,
    compute_next_fire_after,
)
from taskq.exceptions import MissingProvider
from taskq.obs import set_otel_enabled, setup_logging
from taskq.progress._flush import progress_flush_loop
from taskq.ratelimit._provider import register_rate_limit_registry
from taskq.ratelimit.registry import registry as rl_registry
from taskq.settings import WorkerSettings
from taskq.worker.actor_config import ActorConfig
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import open_worker_deps
from taskq.worker.health import HealthServer
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.leader import MaintenanceLeader
from taskq.worker.notify import notify_listener_loop
from taskq.worker.shutdown import install_signal_handlers
from taskq.worker.startup import sync_actor_config

__all__ = ["_emit_sub_enqueue_startup_warnings", "_main", "worker_main"]

_startup_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.worker.run.startup")


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


async def _main(
    settings: WorkerSettings,
    *,
    _local_queue_seed: list[JobRow] | None = None,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    _registry: ProviderRegistry | None = None,
    _cron_registry: list[CronScheduleSpec] | None = None,
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

    registry = _registry if _registry is not None else ProviderRegistry()
    if _registry is None:
        registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    if not registry.has_provider(Clock):
        registry.register_value(Clock, Scope.PROCESS, SystemClock())

    scope_containers: dict[Scope, ProcessScope | ThreadScope | LoopScope] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type]  # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol — pyright cannot verify dict covariance across the Protocol boundary

    loop = asyncio.get_running_loop()

    set_otel_enabled(settings.otel_enabled)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    _producer_log = structlog.get_logger("taskq.worker.run.producer")

    async with open_worker_deps(settings) as deps:
        if not registry.has_provider(asyncpg.Pool):
            registry.register_value(asyncpg.Pool, Scope.LOOP, deps.worker_pool)

        actors_list: list[ActorRef[Any, Any]] | None = (
            list(actor_registry.values()) if actor_registry else None
        )
        register_rate_limit_registry(registry, rl_registry)
        registry.validate(actors=actors_list, rate_limit_registry=rl_registry)

        from taskq.ratelimit import sync_rate_limit_buckets, sync_slots

        try:
            await sync_rate_limit_buckets(
                rl_registry, deps.worker_pool, schema=settings.schema_name
            )
        except Exception as exc:
            _startup_log.warning(
                "sync_rate_limit_buckets_failed",
                error=str(exc),
            )
        try:
            await sync_slots(
                list(rl_registry.reservations.values()),
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

            for res in rl_registry.reservations.values():
                try:
                    await res.ensure_slots(deps.dispatcher_pool)
                except Exception as exc:
                    _startup_log.warning(
                        "ensure_slots_failed",
                        bucket_name=res.name,
                        error=str(exc),
                    )

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

            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        heartbeat_loop(
                            deps,
                            worker_id,
                            shutdown_event,
                            cancel_controller=make_cancel_controller(deps, worker_id, backend),
                            cancel_wake_event=cancel_wake_event,
                        )
                    )
                    tg.create_task(
                        progress_flush_loop(
                            deps.worker_pool,
                            settings.schema_name,
                            worker_id,
                            deps.progress_buffers,
                            settings.progress_coalesce_interval,
                            shutdown_event,
                        )
                    )
                    tg.create_task(
                        notify_listener_loop(
                            deps,
                            backend,  # type: ignore[arg-type]  # Why: notify_listener_loop expects PostgresBackend; the instance is PostgresBackend at runtime — pyright cannot narrow the Backend Protocol to the concrete class here
                            shutdown_event,
                            worker_id,
                        )
                    )
                    tg.create_task(
                        MaintenanceLeader(
                            deps,
                            worker_id,
                            backend,
                            clock=_clock,
                        ).run(shutdown_event)
                    )
                    tg.create_task(
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
                            tg.create_task(
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
                            tg.create_task(
                                consumer_loop_stub(
                                    deps,
                                    local_queue,
                                    shutdown_event,
                                    backend=backend,
                                    worker_id=worker_id,
                                )
                            )

                    await shutdown_event.wait()
            finally:
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


def worker_main(
    settings: WorkerSettings,
    *,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    di_registry: ProviderRegistry | None = None,
    cron_registry: list[CronScheduleSpec] | None = None,
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
            )
        )
