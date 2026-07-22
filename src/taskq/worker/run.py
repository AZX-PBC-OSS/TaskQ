"""Worker-runtime wiring helpers and process entry point.

This module forms the production seam between WorkerDeps assembly
(open_worker_deps, deps.py) and the heartbeat task spawn. It also hosts
the worker process entry point ``worker_main`` and the ``_main`` bootstrap
coroutine that wires the full TaskGroup of long-lived siblings.

Deviations from the original sketch:
  - Signal handlers go through ``install_signal_handlers``, not
    inline lambdas.
  - ``orchestrate_shutdown`` takes no ``tg`` parameter.
  - ``ProcessScope``/``ThreadScope``/``LoopScope`` are bootstrapped inside
    ``open_worker_deps`` rather than before it (M3 single-process deployment
    — process exit and deps exit are coterminal; see ``_main`` comment block).
  - The ``_local_queue_seed`` keyword-only parameter is a test seam, not
    public API.
  - ``local_queue`` maxsize uses ``max_concurrency`` (not ``batch_size`` —
    no ``batch_size`` field exists on ``WorkerSettings``).

M1 stub consumers accept a ``stub_work_timeout`` keyword-only parameter
(default 60.0 s) that controls the sentinel sleep duration. Integration
tests may pass a shorter override (e.g. ``stub_work_timeout=2.0``) when
seeding jobs that must complete naturally during a short test run. The
bootstrap accepts the default; M2 replaces the stubs with the
real producer/consumer.
"""

import asyncio
import contextlib
import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel

from taskq._di import ProviderRegistry
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import Backend, JobRow
from taskq.backend._records import jsonb_param
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: canonical identifier regex; copying would drift the validation pattern.
)
from taskq.context import JobContext
from taskq.exceptions import MissingProvider
from taskq.obs import bind_job_context, get_logger
from taskq.retry import OnRetryExhausted, OnSuccess, RetryClassifierHook, RetryPolicy
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import worker_main
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import dispatch_one_job

__all__ = [  # pyright: ignore[reportUnsupportedDunderAll]  # Why: _main is lazily re-exported via __getattr__
    "_main",
    "consumer_loop_stub",
    "deregister_worker",
    "di_consumer_loop",
    "producer_loop",
    "producer_loop_stub",
    "register_worker",
    "worker_main",
]


if TYPE_CHECKING:
    # Static re-export so pyright/mkdocstrings resolve the lazily-provided
    # names below; runtime resolution stays in __getattr__ to defer the
    # _bootstrap import cost.
    from taskq.worker._bootstrap import (
        _emit_sub_enqueue_startup_warnings as _emit_sub_enqueue_startup_warnings,
    )
    from taskq.worker._bootstrap import (
        _main as _main,
    )


def __getattr__(name: str) -> object:
    if name in ("_main", "_emit_sub_enqueue_startup_warnings"):
        from taskq.worker._bootstrap import _emit_sub_enqueue_startup_warnings, _main

        return {
            "_main": _main,
            "_emit_sub_enqueue_startup_warnings": _emit_sub_enqueue_startup_warnings,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


_producer_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.producer")
_consumer_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.consumer")
_reg_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.registration")
_startup_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.startup")


class _StubPayload(BaseModel):
    """Minimal payload model for stub JobContext (no actor handler runs)."""


@dataclass(frozen=True, slots=True)
class _DispatchActorConfig:
    """Frozen dataclass satisfying ActorConfigLike for dispatch_one_job.

    Built from ActorRef fields; provides the retry policy the consumer's
    exception classifier needs along with non_retryable_exceptions and
    on_retry_exhausted from the @actor decorator.
    """

    retry: RetryPolicy
    non_retryable_exceptions: tuple[type[BaseException], ...] = ()
    retry_classifier: RetryClassifierHook | None = None
    on_retry_exhausted: OnRetryExhausted | None = None
    on_retry_exhausted_timeout: float = 3.0
    on_success: OnSuccess | None = None
    on_success_timeout: float = 3.0


def make_heartbeat_kwargs(
    deps: WorkerDeps,
    worker_id: UUID,
    backend: Backend,
    cancel_wake_event: asyncio.Event | None = None,
) -> dict[str, object]:
    """Return keyword arguments that wire a cancel controller and optional
    cancel-wake event into heartbeat_loop for a given worker.

    Usage (production, owned by the orchestration layer)::

        kwargs = make_heartbeat_kwargs(deps, worker_id, backend, cancel_wake_event)
        await heartbeat_loop(deps, worker_id, shutdown, **kwargs)

    Returns:
        ``{"cancel_controller": ..., "cancel_wake_event": ...}``.
    """
    return {
        "cancel_controller": make_cancel_controller(deps, worker_id, backend),
        "cancel_wake_event": cancel_wake_event,
    }


async def producer_loop(
    deps: WorkerDeps,
    local_queue: asyncio.Queue[JobRow],
    shutdown_event: asyncio.Event,
    producer_stop_event: asyncio.Event,
    *,
    backend: Backend,
    worker_id: UUID,
) -> None:
    """Dispatch pending jobs from the database and feed them into ``local_queue``.

    On each iteration the producer:

    1. Waits for a wake signal (NOTIFY-driven ``asyncio.Event``) or the
       ``poll_interval`` fallback timer — whichever fires first.
    2. Calls ``backend.dispatch_batch()`` to atomically claim up to
       ``local_queue.maxsize - local_queue.qsize()`` pending jobs
       (pending → running) using ``FOR UPDATE SKIP LOCKED``.
    3. Puts each returned :class:`JobRow` onto ``local_queue`` for the
       consumer tasks.

    Exits cleanly when either ``shutdown_event`` or ``producer_stop_event``
    is set.
    """
    settings = deps.settings
    queues = settings.queues
    lock_lease_td = timedelta(seconds=settings.lock_lease)
    notify_enabled = getattr(settings, "notify_enabled", False)
    poll_interval = settings.notify_poll_interval if notify_enabled else settings.poll_interval

    _producer_log.info(
        "producer-loop-start",
        queues=queues,
        poll_interval=poll_interval,
        notify_enabled=notify_enabled,
        max_concurrency=settings.max_concurrency,
        worker_id=str(worker_id),
    )

    async with contextlib.AsyncExitStack() as stack:
        wake_event: asyncio.Event | None = None
        if notify_enabled:
            _subscribe_wake = getattr(backend, "subscribe_wake", None)
            if callable(_subscribe_wake):
                wake_event = await stack.enter_async_context(
                    cast(
                        "contextlib.AbstractAsyncContextManager[asyncio.Event]",
                        _subscribe_wake(),
                    )
                )
                _producer_log.info("producer-subscribed-wake", worker_id=str(worker_id))
            else:
                _producer_log.warning(
                    "producer-no-wake-subscribe",
                    note="subscribe_wake not available; falling back to poll-only",
                    worker_id=str(worker_id),
                )

        while not (shutdown_event.is_set() or producer_stop_event.is_set()):
            available = local_queue.maxsize - local_queue.qsize()
            if available <= 0:
                await asyncio.sleep(0.1)
                continue

            try:
                jobs = await backend.dispatch_batch(
                    worker_id=worker_id,
                    queues=queues,
                    limit=available,
                    lock_lease=lock_lease_td,
                )
            except Exception:
                _producer_log.exception("dispatch-batch-error", worker_id=str(worker_id))
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.sleep(poll_interval)
                continue

            if jobs:
                for job in jobs:
                    await local_queue.put(job)
                if wake_event is not None:
                    wake_event.clear()
                continue

            wake_wait = asyncio.create_task(wake_event.wait()) if wake_event is not None else None
            poll_wait = asyncio.create_task(asyncio.sleep(poll_interval))
            stop_wait = asyncio.create_task(producer_stop_event.wait())
            shutdown_wait = asyncio.create_task(shutdown_event.wait())

            all_waits = [
                w for w in (wake_wait, poll_wait, stop_wait, shutdown_wait) if w is not None
            ]

            try:
                _done, pending = await asyncio.wait(
                    all_waits,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            finally:
                for task in all_waits:
                    if not task.done():
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task

            if wake_event is not None:
                wake_event.clear()

    reason = "producer_stop_event" if producer_stop_event.is_set() else "shutdown_event"
    _producer_log.info("producer-loop-exit", reason=reason)


async def producer_loop_stub(
    deps: WorkerDeps,
    local_queue: asyncio.Queue[JobRow],
    shutdown_event: asyncio.Event,
    producer_stop_event: asyncio.Event,
    *,
    backend: Backend,
    worker_id: UUID,
) -> None:
    """Observe producer_stop_event and shutdown_event; exit cleanly.

    Outer loop: ``while not (producer_stop_event.is_set() or shutdown_event.is_set())``.
    Body races ``producer_stop_event.wait()`` against ``shutdown_event.wait()`` via
    ``asyncio.wait(..., return_when=FIRST_COMPLETED)``; the loser is cancelled
    to avoid a pending-task leak.
    """
    while not (producer_stop_event.is_set() or shutdown_event.is_set()):
        stop_wait = asyncio.create_task(producer_stop_event.wait())
        shutdown_wait = asyncio.create_task(shutdown_event.wait())
        try:
            _done, pending = await asyncio.wait(
                [stop_wait, shutdown_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        finally:
            for task in [stop_wait, shutdown_wait]:
                if not task.done():
                    task.cancel()

    reason = "producer_stop_event" if producer_stop_event.is_set() else "shutdown_event"
    _producer_log.info("producer-loop-exit", reason=reason)


async def consumer_loop_stub(
    deps: WorkerDeps,
    local_queue: asyncio.Queue[JobRow],
    shutdown_event: asyncio.Event,
    *,
    backend: Backend,
    worker_id: UUID,
    stub_work_timeout: float = 60.0,
) -> None:
    """Pull one job per iteration, register, sleep sentinel, write terminal status.

    Outer loop: ``while not shutdown_event.is_set()``.
    Races ``local_queue.get()`` against ``shutdown_event.wait()``; on shutdown
    win the queue waiter is cancelled and the stub returns cleanly.

    On job get the stub registers in ``deps.active_jobs``, awaits a cancellable
    sentinel, writes terminal state via ``backend`` (shielded), and deregisters
    in ``finally``.
    """
    while not shutdown_event.is_set():
        q_get = asyncio.create_task(local_queue.get())
        shut_wait = asyncio.create_task(shutdown_event.wait())
        try:
            _done, pending = await asyncio.wait(
                [q_get, shut_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if shut_wait in _done:
                return
        finally:
            for task in [q_get, shut_wait]:
                if not task.done():
                    task.cancel()

        job: JobRow = q_get.result()

        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("consumer_loop_stub must run inside a TaskGroup")

        ctx: JobContext[_StubPayload] = JobContext(
            job_id=job.id,
            actor=job.actor,
            queue=job.queue,
            attempt=job.attempt,
            worker_id=worker_id,
            payload=_StubPayload(),
            jobs=SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=None,
                backend=backend,
            ),
            log=bind_job_context(
                _consumer_log,
                job_id=job.id,
                actor=job.actor,
                queue=job.queue,
                attempt=job.attempt,
                identity_key=job.identity_key,
                trace_id=job.trace_id or "",
            ),
        )

        await deps.active_jobs.register(job.id, current_task, ctx)  # type: ignore[arg-type]  # Why: JobContext[_StubPayload] is a JobContext[BaseModel]; pyright cannot widen Generic[TChild] to Generic[TParent] without explicit covariance.

        try:
            try:
                await asyncio.wait_for(
                    ctx.cancel_event.wait(),
                    timeout=stub_work_timeout,
                )
            except asyncio.CancelledError:
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(backend.mark_cancelled(job.id, worker_id))
                raise
            except TimeoutError:
                pass

            if ctx.cancellation_requested:
                await asyncio.shield(backend.mark_cancelled(job.id, worker_id))
            else:
                await asyncio.shield(backend.mark_succeeded(job.id, worker_id, None))

        except asyncio.CancelledError:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(backend.mark_cancelled(job.id, worker_id))
            raise

        except Exception:
            _consumer_log.exception("consumer-stub-error", job_id=str(job.id))

        finally:
            await deps.active_jobs.deregister(job.id)


async def di_consumer_loop(
    deps: WorkerDeps,
    local_queue: asyncio.Queue[JobRow],
    shutdown_event: asyncio.Event,
    *,
    backend: Backend,
    worker_id: UUID,
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
    actor_registry: Mapping[str, ActorRef[Any, Any]],
    enqueuer: SubJobEnqueuer,
) -> None:
    """Pull one job per iteration and dispatch via dispatch_one_job.

    Outer loop: ``while not shutdown_event.is_set()``.
    Races ``local_queue.get()`` against ``shutdown_event.wait()``; on shutdown
    win the queue waiter is cancelled and the loop returns cleanly.

    Each job is dispatched through dispatch_one_job which composes
    build_actor_scope + consume_one_job, providing DI-aware actor
    invocation with per-invocation TRANSIENT scope teardown.
    """
    clock_obj = process_scope.get(Clock)
    if clock_obj is None or not isinstance(clock_obj, Clock):
        raise MissingProvider(
            type_name="Clock",
            required_by="worker.di_consumer_loop (ProcessScope must have a cached Clock after bootstrap)",
        )
    clock: Clock = clock_obj

    while not shutdown_event.is_set():
        q_get = asyncio.create_task(local_queue.get())
        shut_wait = asyncio.create_task(shutdown_event.wait())
        try:
            _done, pending = await asyncio.wait(
                [q_get, shut_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if shut_wait in _done:
                return
        finally:
            for task in [q_get, shut_wait]:
                if not task.done():
                    task.cancel()

        job: JobRow = q_get.result()

        if job.actor not in actor_registry:
            _consumer_log.error(
                "dispatch-actor-not-found",
                job_id=str(job.id),
                actor=job.actor,
            )
            # Release the claimed job instead of leaving it 'running' until
            # lease expiry — a worker whose registry has the actor can then
            # pick it up. The short delay keeps this worker from re-claiming
            # it in a hot loop.
            try:
                await backend.mark_snoozed(
                    job.id,
                    worker_id,
                    timedelta(seconds=10),
                    metadata_update={"released_reason": "actor-not-found"},
                )
            except Exception:
                _consumer_log.exception(
                    "dispatch-actor-not-found-release-failed",
                    job_id=str(job.id),
                    actor=job.actor,
                )
            continue

        actor_ref = actor_registry[job.actor]
        actor_config = _DispatchActorConfig(
            retry=actor_ref.retry,
            non_retryable_exceptions=actor_ref.non_retryable_exceptions,
            retry_classifier=actor_ref.retry_classifier,
            on_retry_exhausted=actor_ref.on_retry_exhausted,
            on_retry_exhausted_timeout=actor_ref.on_retry_exhausted_timeout,
            on_success=actor_ref.on_success,
            on_success_timeout=actor_ref.on_success_timeout,
        )
        try:
            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound — actor_ref carries the correct payload_type and fn.
                actor_config=actor_config,
                clock=clock,
                active_jobs=deps.active_jobs,
                enqueuer=enqueuer,
            )
        except Exception:
            _consumer_log.exception("dispatch-failed", job_id=str(job.id))


async def register_worker(pool: asyncpg.Pool, settings: WorkerSettings) -> UUID:
    """Register the current worker in ``taskq.workers`` and return its UUID.

    Generates a UUIDv7, inserts a row into ``{schema}.workers``, and returns
    the new UUID.  Acquires from *pool* with a 2.0 s timeout; on timeout or
    connection error the failure is logged and re-raised (registering is
    fatal).

    If ``settings.worker_label`` or ``settings.workgroup_instance`` are set,
    they are stored directly for cross-process correlation and health checking.
    """
    worker_id = new_uuid()
    schema = settings.schema_name
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    hostname = socket.gethostname()
    pid = os.getpid()
    queues = settings.queues

    maybe_label: str | None = settings.worker_label
    maybe_instance_raw = settings.workgroup_instance
    maybe_instance: UUID | None = UUID(maybe_instance_raw) if maybe_instance_raw else None

    notify_enabled = getattr(settings, "notify_enabled", False)
    metadata: dict[str, object] = {"notify_enabled": notify_enabled}

    sql = (
        f'INSERT INTO "{schema}".workers '  # noqa: S608  # Why: schema validated against _IDENT_RE before interpolation; asyncpg cannot bind identifiers as parameters.
        "(id, hostname, pid, queues, worker_label, workgroup_instance, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)"
    )
    metadata_json = jsonb_param(metadata)

    try:
        async with pool.acquire(timeout=2.0) as conn:
            await conn.execute(
                sql, worker_id, hostname, pid, queues, maybe_label, maybe_instance, metadata_json
            )
    except (TimeoutError, asyncpg.PostgresConnectionError, OSError) as e:
        _reg_log.error("register-worker-failed", error=str(e))
        raise

    return worker_id


async def deregister_worker(pool: asyncpg.Pool, settings: WorkerSettings, worker_id: UUID) -> None:
    """Remove the worker row from ``{schema}.workers`` (best-effort).

    Acquires from *pool* with a 2.0 s timeout.  On timeout or connection
    error, logs a structured warning ``deregister_worker_failed`` and
    returns without raising — the recovery sweep is the backstop and
    shutdown MUST NOT block on this cleanup.
    """
    schema = settings.schema_name
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    sql = f'DELETE FROM "{schema}".workers WHERE id = $1'  # noqa: S608  # Why: schema validated against _IDENT_RE before interpolation; asyncpg cannot bind identifiers as parameters.

    try:
        async with pool.acquire(timeout=2.0) as conn:
            await conn.execute(sql, worker_id)
    except (TimeoutError, asyncpg.PostgresConnectionError, OSError) as e:
        _reg_log.warning(
            "deregister_worker_failed",
            worker_id=worker_id,
            error=str(e),
        )
