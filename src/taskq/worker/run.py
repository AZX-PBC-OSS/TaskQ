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
import random
import secrets
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel

from taskq._di import ProviderRegistry
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
from taskq._ids import new_uuid
from taskq._shield import shield_with_retrieval
from taskq.actor import ActorRef
from taskq.backend._protocol import Backend, JobRow
from taskq.backend._records import jsonb_param
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer, parent_tags
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: canonical identifier regex; copying would drift the validation pattern.
)
from taskq.context import JobContext
from taskq.exceptions import MissingProvider
from taskq.obs import bind_job_context, get_logger
from taskq.retry import OnRetryExhausted, OnSuccess, RetryClassifierHook, RetryPolicy
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import worker_main, worker_main_async
from taskq.worker._transient import TRANSIENT_PG_ERRORS
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import SlotPoolAcquireError, dispatch_one_job

__all__ = [  # pyright: ignore[reportUnsupportedDunderAll]  # Why: _main is lazily re-exported via __getattr__
    "_main",
    "consumer_loop_stub",
    "deregister_worker",
    "di_consumer_loop",
    "producer_loop",
    "producer_loop_stub",
    "register_worker",
    "worker_main",
    "worker_main_async",
]


if TYPE_CHECKING:
    # Static re-export so pyright/mkdocstrings resolve the lazily-provided
    # names below; runtime resolution stays in __getattr__ to defer the
    # _bootstrap import cost.
    from taskq.worker._bootstrap import (
        _emit_sub_enqueue_startup_warnings as _emit_sub_enqueue_startup_warnings,
    )
    from taskq.worker._bootstrap import (
        _emit_unconsumed_queue_startup_warnings as _emit_unconsumed_queue_startup_warnings,
    )
    from taskq.worker._bootstrap import (
        _main as _main,
    )


def __getattr__(name: str) -> object:
    if name in (
        "_main",
        "_emit_sub_enqueue_startup_warnings",
        "_emit_unconsumed_queue_startup_warnings",
    ):
        from taskq.worker._bootstrap import (
            _emit_sub_enqueue_startup_warnings,
            _emit_unconsumed_queue_startup_warnings,
            _main,
        )

        return {
            "_main": _main,
            "_emit_sub_enqueue_startup_warnings": _emit_sub_enqueue_startup_warnings,
            "_emit_unconsumed_queue_startup_warnings": _emit_unconsumed_queue_startup_warnings,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


_producer_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.producer")
_consumer_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.consumer")
_reg_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.registration")
_startup_log: structlog.stdlib.BoundLogger = get_logger(f"{__name__}.startup")

_SLOT_REFILL_POLL_SECONDS: Final[float] = 0.1
"""Fallback bound for the producer's slot-refill wait, the value the
fixed sleep it replaced polled at: a slot-freed event that is never set
(broken wiring, a consumer-less worker) still leaves the producer
re-checking on this cadence rather than parked forever."""

_POLL_JITTER_FRACTION: Final[float] = 0.1
"""Multiplicative jitter band for the fallback poll wait."""

_PRODUCER_RNG = random.Random(secrets.randbits(128))  # noqa: S311  # Why: random.Random is for timing jitter, not cryptography; seeded once from the OS entropy pool so two workers never share a jitter phase — same seeding pattern as retry.py's _production_rng.


def _jittered_poll_interval(interval: float, rng: random.Random) -> float:
    """The fallback poll interval with ±_POLL_JITTER_FRACTION jitter.

    Every producer in an idle fleet otherwise sleeps the same interval
    in phase, and any transient event (a GC pause, a network blip, a
    coordinated restart) re-synchronizes them into periodic DB load
    spikes — river jitters its fetch poll for exactly this reason
    (vendor/river/producer.go, jitteredFetchPollInterval). The
    jitter is multiplicative-symmetric, the repo's jitter convention
    (retry.compute_backoff), so the mean wait stays the configured
    interval; river's band is +0..10%.
    """
    return interval * rng.uniform(1.0 - _POLL_JITTER_FRACTION, 1.0 + _POLL_JITTER_FRACTION)


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
    slot_freed_event: asyncio.Event | None = None,
    rng: random.Random | None = None,
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

    ``slot_freed_event`` is set by the consumer loops each time a
    ``local_queue.get()`` drains a slot; the bootstrap wires one shared
    event into this loop and every consumer. ``rng`` supplies the
    fallback-poll jitter (a test seam; production uses the module RNG
    seeded per process). Both default to standalone behaviour: a private
    event nobody sets degrades the slot-refill wait to the fallback
    cadence, and the module RNG jitters as in production.
    """
    settings = deps.settings
    queues = settings.queues
    lock_lease_td = timedelta(seconds=settings.lock_lease)
    notify_enabled = getattr(settings, "notify_enabled", False)
    poll_interval = settings.notify_poll_interval if notify_enabled else settings.poll_interval
    rng_source = rng if rng is not None else _PRODUCER_RNG
    # Wakes this producer the moment a consumer's local_queue.get()
    # drains a slot (see the saturation branch below). None keeps the
    # loop standalone: a private event nobody sets degrades the bounded
    # wait to exactly the fixed-cadence poll it replaces.
    slot_freed = slot_freed_event if slot_freed_event is not None else asyncio.Event()

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
            deps.liveness.tick("producer", period=poll_interval)
            available = local_queue.maxsize - local_queue.qsize()
            if available <= 0:
                # All consumer slots busy and the local queue full. A
                # consumer's get() is what frees a slot from this
                # producer's accounting — qsize drops there, not at job
                # completion — and the consumer loops set slot_freed at
                # exactly that point, so the next claim begins the
                # moment a slot frees instead of on the next poll tick
                # (river wakes its producer the same way when a job
                # result frees a worker slot: vendor/river/producer.go,
                # jobResultCh case). Bounded, not bare: an
                # event that is never set (broken wiring, a
                # consumer-less worker) must still leave this loop
                # re-checking on the fallback cadence.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(slot_freed.wait(), timeout=_SLOT_REFILL_POLL_SECONDS)
                slot_freed.clear()
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
            # Jittered per wait, not fixed: an idle fleet sharing one
            # interval polls in phase, and any transient event
            # re-synchronizes it into periodic DB load spikes (see
            # _jittered_poll_interval for the vendor precedent).
            poll_wait = asyncio.create_task(
                asyncio.sleep(_jittered_poll_interval(poll_interval, rng_source))
            )
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
    deps.liveness.forget("producer")


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

    Why the unused parameters: this is a drop-in stand-in for
    :func:`producer_loop` (used when a worker runs consume-only), so it
    takes that function's signature verbatim and the caller can swap one
    for the other without a shim. ``deps``, ``local_queue``, ``backend``
    and ``worker_id`` are exactly the arguments it does not need because
    it never dispatches — a divergent signature here would move that
    branch into every call site.

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
    slot_freed_event: asyncio.Event | None = None,
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
            if shut_wait in _done and q_get not in _done:
                return
            # Why fall through on a both-done turn: the get has already
            # TAKEN the job out of local_queue — returning here would
            # discard it with no consumer run, no terminal write, and no
            # release, leaving recovery to lock-lease expiry. The taken
            # job runs this final iteration; the outer while's shutdown
            # check then exits the loop.
        finally:
            for task in [q_get, shut_wait]:
                if not task.done():
                    task.cancel()

        job: JobRow = q_get.result()

        # Slot-release point: the get() above dropped qsize by one, so a
        # saturated producer can claim again — wake it now (see
        # producer_loop's saturation branch). Not at job completion: the
        # slot was handed back at get(), and a completion-time signal
        # races the producer's availability check against this loop's
        # next get().
        if slot_freed_event is not None:
            slot_freed_event.set()

        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("consumer_loop_stub must run inside a TaskGroup")

        with parent_tags(tuple(job.tags)):
            ctx: JobContext[_StubPayload] = JobContext(
                job_id=job.id,
                actor=job.actor,
                queue=job.queue,
                attempt=job.attempt,
                snooze_count=job.snooze_count,
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
                    # shield_with_retrieval, not plain asyncio.shield: a second
                    # cancel landing while this write is detached must not
                    # strand its outcome unretrieved (see taskq._shield).
                    with contextlib.suppress(asyncio.CancelledError):
                        await shield_with_retrieval(
                            backend.mark_cancelled(job.id, worker_id, attempt=job.attempt)
                        )
                    raise
                except TimeoutError:
                    pass

                if ctx.cancellation_requested:
                    await shield_with_retrieval(
                        backend.mark_cancelled(job.id, worker_id, attempt=job.attempt)
                    )
                else:
                    await shield_with_retrieval(
                        backend.mark_succeeded(job.id, worker_id, None, attempt=job.attempt)
                    )
                # fallback_result_ttl is not forwarded here: the stub path has
                # no actor registry and therefore no @actor(result_ttl=...)
                # literal to supply. If the stored actor_config.result_ttl is
                # cleared (NULL), the enqueue-pinned expiry survives — the
                # original bug, on the one path that structurally cannot fix
                # itself. This is acceptable because the stub is a test/dev
                # sentinel, not a production consumer. The real consumer
                # (_consumer.py) resolves the literal from the registry and
                # passes it through mark_succeeded_with_conn.

            except asyncio.CancelledError:
                with contextlib.suppress(asyncio.CancelledError):
                    await shield_with_retrieval(
                        backend.mark_cancelled(job.id, worker_id, attempt=job.attempt)
                    )
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
    slot_freed_event: asyncio.Event | None = None,
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
            if shut_wait in _done and q_get not in _done:
                return
            # Why fall through on a both-done turn: the get has already
            # TAKEN the job out of local_queue — returning here would
            # discard it with no dispatch, no terminal write, and no
            # release, leaving recovery to lock-lease expiry. The taken
            # job runs this final iteration; the outer while's shutdown
            # check then exits the loop.
        finally:
            for task in [q_get, shut_wait]:
                if not task.done():
                    task.cancel()

        job: JobRow = q_get.result()

        # Slot-release point: the get() above dropped qsize by one, so a
        # saturated producer can claim again — wake it now (see
        # producer_loop's saturation branch). Not at job completion: the
        # slot was handed back at get(), and a completion-time signal
        # races the producer's availability check against this loop's
        # next get(). Fires on every iteration — every exit path
        # (success, failure, snooze, not-found release) passes it.
        if slot_freed_event is not None:
            slot_freed_event.set()

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
            #
            # Contract: an unregistered actor parks the job at the snooze
            # cadence, budget-free — mark_snoozed's default 'snoozed'
            # outcome refunds the claim's attempt increment, so a job whose
            # actor is missing (through no fault of its own) never burns
            # retry budget while it waits for a worker that can run it;
            # the stranded-jobs detector surfaces it. This is not an
            # actor-requested deferral semantically, but the snooze write
            # is the closest bounded outcome — a delay, a release, and a
            # released_reason marker in one transition.
            try:
                await backend.mark_snoozed(
                    job.id,
                    worker_id,
                    timedelta(seconds=10),
                    metadata_update={"released_reason": "actor-not-found"},
                    attempt=job.attempt,
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
            outcome = await dispatch_one_job(
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
                max_retry_backoff=deps.settings.max_retry_backoff,
                enqueuer=enqueuer,
            )
            if outcome == "failed":
                deps.drain_failures += 1
        except SlotPoolAcquireError:
            # Infrastructure, not a job outcome: the job is already
            # claimed, its lock lease expires, and the reclaim sweep
            # re-dispatches it. Counting this as a drain failure would
            # make a Kubernetes Job / CI drain step report job failures
            # that never happened. The acquire was recorded (counter)
            # and logged (per-occurrence cause, job id) at the raise
            # site; nothing to do here but leave the job to lease
            # reclaim.
            continue
        except Exception:
            _consumer_log.exception("dispatch-failed", job_id=str(job.id))
            deps.drain_failures += 1


async def register_worker(pool: asyncpg.Pool, settings: WorkerSettings) -> UUID:
    """Register the current worker in ``taskq.workers`` and return its UUID.

    Generates a UUIDv7, inserts a row into ``{schema}.workers``, and returns
    the new UUID.  Acquires from *pool* with a 2.0 s timeout; on timeout or
    connection error the failure is logged and re-raised (registering is
    fatal).

    If ``settings.worker_label`` or ``settings.workgroup_instance`` are set,
    they are stored directly for cross-process correlation and health checking.

    The row's metadata records the worker's runtime facts: whether NOTIFY
    dispatch is enabled, and ``max_concurrency`` — the capacity the worker
    runs at, which sizes ``local_queue`` and bounds every dispatch.
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
    # The workers row carries the capacity this worker actually runs at:
    # ``max_concurrency`` sizes ``local_queue`` and bounds every dispatch,
    # so a fleet's effective parallelism stays queryable from the database
    # (good_job reports ``max_threads`` in its process rows; sidekiq
    # heartbeats ``concurrency``).
    metadata: dict[str, object] = {
        "notify_enabled": notify_enabled,
        "max_concurrency": settings.max_concurrency,
    }

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
    except TRANSIENT_PG_ERRORS as e:
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
    except TRANSIENT_PG_ERRORS as e:
        _reg_log.warning(
            "deregister_worker_failed",
            worker_id=worker_id,
            error=str(e),
        )
