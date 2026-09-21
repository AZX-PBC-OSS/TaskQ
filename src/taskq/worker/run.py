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
   , process exit and deps exit are coterminal; see ``_main`` comment block).
  - The ``_local_queue_seed`` keyword-only parameter is a test seam, not
    public API.
  - ``local_queue`` maxsize uses ``max_concurrency`` (not ``batch_size`` ,
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
import time
from collections.abc import Mapping
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
from taskq.actor_config_ops import ActorConfigRow
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
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import worker_main, worker_main_async
from taskq.worker._handlers import (  # pyright: ignore[reportPrivateUsage]  # Why: the SlotPoolAcquireError arm disowns the claimed row exactly as dispatch's terminal-write-infra arm does; same private-seam rationale.
    _disown_job,
)
from taskq.worker._transient import (
    TRANSIENT_PG_ERRORS,
    UnexpectedLoopErrorGuard,
    is_transient_pg_error,
)
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import SlotPoolAcquireError, dispatch_one_job
from taskq.worker.queue_ops import QueueRow
from taskq.worker.shutdown import drain_local_queue_to_pending
from taskq.worker.startup import capacity_field_diverges

__all__ = [  # pyright: ignore[reportUnsupportedDunderAll]  # Why: _main is lazily re-exported via __getattr__
    "_emit_resolved_capacity_startup_lines",
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
"""Multiplicative jitter band for the fallback poll wait and the claim cooldown."""

_CLAIM_COOLDOWN_SECONDS: Final[float] = 0.05
"""Floor between a claim round that came back short and the next one.

A short round, fewer rows than asked, or none, says the backlog is
drained or a peer won it; the triggers that keep arriving meanwhile (a
schema-wide NOTIFY wakes every worker, every completion frees a slot) are
folded into one round after this wait instead of each paying a full
dispatch round plus the loser's window expansions. A full round is exempt:
backlog drain re-claims immediately. Sized for this worker's small slot
count: it is the worst-case added claim latency for a job that arrives
right after a short round. A module constant rather than a setting because
the value is a latency-vs-load trade with a sensible fixed default; the
settings surface is owned elsewhere and grows a knob only when a
deployment shows it needs one.
"""

_PRODUCER_RNG = random.Random(secrets.randbits(128))  # noqa: S311  # Why: random.Random is for timing jitter, not cryptography; seeded once from the OS entropy pool so two workers never share a jitter phase, same seeding pattern as retry.py's _production_rng.


def _jittered_poll_interval(interval: float, rng: random.Random) -> float:
    """A producer wait, the fallback poll interval, or the claim cooldown ,
    with ±_POLL_JITTER_FRACTION jitter.

    Every producer in an idle fleet otherwise sleeps the same interval
    in phase, and any transient event (a GC pause, a network blip, a
    coordinated restart, or one NOTIFY waking the whole fleet into the
    same cooldown) re-synchronizes them into periodic DB load spikes.
    Jittering the wait breaks that synchronization and spreads requests
    across time. The jitter is multiplicative-symmetric, the repo's
    jitter convention (retry.compute_backoff), so the mean wait stays the
    configured interval; the band is ±_POLL_JITTER_FRACTION.
    """
    return interval * rng.uniform(1.0 - _POLL_JITTER_FRACTION, 1.0 + _POLL_JITTER_FRACTION)


class _StubPayload(BaseModel):
    """Minimal payload model for stub JobContext (no actor handler runs)."""


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
       ``poll_interval`` fallback timer, whichever fires first.
    2. Calls ``backend.dispatch_batch()`` to atomically claim up to the
       worker's genuinely free slots, ``local_queue.maxsize -
       local_queue.qsize() - active_jobs.count()`` pending jobs
       (pending → running) using ``FOR UPDATE SKIP LOCKED``.
    3. Puts each returned :class:`JobRow` onto ``local_queue`` for the
       consumer tasks.

    A round that returns fewer rows than it asked for arms a short
    jittered cooldown (:data:`_CLAIM_COOLDOWN_SECONDS`) before the next
    round, so a burst of wakes or freed slots costs one round rather than
    one per trigger; a full round re-claims immediately.

    Slot accounting: a worker with ``max_concurrency`` slots may
    hold at most that many rows locked at once, the claim sizes by the
    slots actually free (queue emptiness minus jobs actively running),
    not by queue emptiness alone, which allowed up to 2x
    ``max_concurrency`` rows locked (a full local queue while every
    consumer was busy still looked like ``max_concurrency`` free slots:
    double reclaim exposure on a crash, and head-of-line latency behind
    long jobs while peer workers idle). The transient window between a
    consumer's ``get()`` and the job's ``active_jobs`` register is
    uncounted for one scheduler step (bounded by the consumer count);
    the completion-side slot frees at the consumer's ``deregister``, and
    the wake the consumer sets there re-arms this producer the moment
    accounting settles, no claim waits for the next poll tick.

    Exits cleanly when either ``shutdown_event`` or ``producer_stop_event``
    is set.

    ``slot_freed_event`` is set by the consumer loops at the two
    slot-release points, when a ``local_queue.get()`` drains a queue
    slot, and when a job's ``active_jobs`` registration ends; the
    bootstrap wires one shared event into this loop and every consumer.
    ``rng`` supplies the
    fallback-poll jitter (a test seam; production uses the module RNG
    seeded per process). Both default to standalone behaviour: a private
    event nobody sets degrades the slot-refill wait to the fallback
    cadence, and the module RNG jitters as in production.
    """
    settings = deps.settings
    queues = settings.queues
    lock_lease_td = timedelta(seconds=settings.lock_lease)
    notify_enabled = getattr(settings, "notify_enabled", False)
    # Same getattr-with-default convention as notify_enabled above: the
    # producer-loop unit tests drive SimpleNamespace stand-ins for
    # WorkerSettings, and the pooled flag defaults to False (direct DSN).
    pooled = bool(getattr(settings, "pg_is_pooled", False))
    poll_interval = settings.notify_poll_interval if notify_enabled else settings.poll_interval
    rng_source = rng if rng is not None else _PRODUCER_RNG
    # Wakes this producer at the consumers' two slot-release points: a
    # local_queue.get() draining a queue slot, and a job's active_jobs
    # deregister freeing an active slot (see the saturation branch
    # below). None keeps the loop standalone: a private event nobody
    # sets degrades the bounded wait to exactly the fixed-cadence poll
    # it replaces.
    slot_freed = slot_freed_event if slot_freed_event is not None else asyncio.Event()

    _producer_log.info(
        "producer-loop-start",
        queues=queues,
        poll_interval=poll_interval,
        notify_enabled=notify_enabled,
        max_concurrency=settings.max_concurrency,
        worker_id=str(worker_id),
    )
    # Set once any claim round commits rows. Guards the exit hand-back
    # below: a producer that never claimed cannot hold a locked row, so
    # its exit owes the fleet no write (the common idle-shutdown shape).
    made_a_claim = False
    # The unexpected-error backstop, the same one every leader
    # maintenance loop carries (see taskq.worker._transient): a
    # NON-transient dispatch failure (a revoked grant, a code bug, a
    # data error) is logged loudly each round but must not retry
    # forever into a zombie that keeps ticking, keeps its liveness
    # registration fresh and stays ready while claiming nothing. After
    # ``max_consecutive`` consecutive unexpected rounds the ORIGINAL
    # error propagates and tears the worker down deliberately;
    # transient failures never feed it, and only a fully successful
    # round resets the streak, exactly the leader loops' semantics.
    guard = UnexpectedLoopErrorGuard("worker.producer")
    # Monotonic deadline before which no claim round may start, armed by
    # a short round (see _CLAIM_COOLDOWN_SECONDS), so the triggers that
    # land while it runs (wakes, freed slots) coalesce into one round.
    claim_not_before = 0.0

    async with contextlib.AsyncExitStack() as stack:
        wake_event: asyncio.Event | None = None
        if notify_enabled:
            _subscribe_wake = getattr(backend, "subscribe_wake", None)
            if callable(_subscribe_wake):
                wake_event = await stack.enter_async_context(
                    cast(
                        "contextlib.AbstractAsyncContextManager[asyncio.Event]",
                        _subscribe_wake(queues=queues),
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
            # The worker's genuinely free slots: every slot is
            # either empty, lent to a queued row (qsize), or occupied by
            # a running job (active_jobs). Sizing the claim by queue
            # emptiness alone counted a fully-busy worker's slots as
            # free whenever its queue had drained, up to 2x
            # max_concurrency rows locked fleet-wide, 2x the reclaim
            # exposure on a crash, and pending work locked behind long
            # jobs while peer workers idled. The get()-to-register
            # window (a row taken from the queue but not yet in
            # active_jobs) is one scheduler step wide and bounded by the
            # consumer count; the reverse, a row finished but not yet
            # deregistered, delays only its own slot's re-claim until
            # the deregister-side wake, never a poll tick.
            available = local_queue.maxsize - local_queue.qsize() - deps.active_jobs.count()
            if available <= 0:
                # All consumer slots busy and the local queue full. A
                # consumer's get() frees a queue slot and a job's
                # completion (deregister) frees an active slot, the
                # consumer loops set slot_freed at exactly those two
                # points, so the next claim begins the moment either
                # lands instead of on the next poll tick.
                # Bounded, not bare: an event that is never set (broken
                # wiring, a consumer-less worker) must still leave this
                # loop re-checking on the fallback cadence.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(slot_freed.wait(), timeout=_SLOT_REFILL_POLL_SECONDS)
                slot_freed.clear()
                continue

            cooldown_remaining = claim_not_before - time.monotonic()
            if cooldown_remaining > 0:
                # Every trigger that lands during this wait, more wakes,
                # more freed slots, is answered by the single round that
                # follows, sized to the slots free by then. Re-entering
                # the loop re-reads availability and the stop flags.
                await asyncio.sleep(cooldown_remaining)
                continue

            # Cleared BEFORE the round, not after: a NOTIFY that lands
            # while the round runs may announce a row the round's
            # snapshot predates, and must survive as exactly one
            # follow-up round. Everything that arrived before this point
            # is answered by the round itself.
            if wake_event is not None:
                wake_event.clear()
            round_started = time.monotonic()
            try:
                jobs = await backend.dispatch_batch(
                    worker_id=worker_id,
                    queues=queues,
                    limit=available,
                    lock_lease=lock_lease_td,
                )
            except Exception as exc:
                # Transient shapes degrade quietly: log at warning and
                # retry next tick (the TRANSIENT_PG_ERRORS doctrine; the
                # pooled extension covers a transaction-mode pooler
                # remapping a prepared statement's server connection out
                # from under a cached name, SQLSTATE 26000/42P05, declared
                # via TASKQ_PG_IS_POOLED). Everything else keeps the loud,
                # exception-level record: a real bug must stay visible.
                if is_transient_pg_error(exc, pooled=pooled):
                    _producer_log.warning(
                        "dispatch-batch-transient",
                        worker_id=str(worker_id),
                        error_class=type(exc).__name__,
                        error=str(exc),
                    )
                else:
                    _producer_log.exception("dispatch-batch-error", worker_id=str(worker_id))
                    # Outside the transient set: feed the backstop. At the
                    # consecutive-failure cap this re-raises the original
                    # error, ending the zombie-tick state the loop would
                    # otherwise never leave (see the guard construction
                    # above and taskq.worker._transient).
                    guard.unexpected(exc)
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.sleep(poll_interval)
                continue

            # The round completed without error: reset the backstop's
            # streak. An empty round counts, a claimed round counts, and
            # neither a transient nor an unexpected failure reaches here.
            guard.ok()

            if len(jobs) < available:
                # A short round: the backlog is drained or a peer won it.
                # A round on its heels would only re-run the claim CTE and
                # the loser's window expansions for nothing. A full round
                # arms no floor, there is backlog to drain, and the next
                # freed slot claims immediately.
                claim_not_before = round_started + _jittered_poll_interval(
                    _CLAIM_COOLDOWN_SECONDS, rng_source
                )

            if jobs:
                made_a_claim = True
                for job in jobs:
                    # A row this worker disowned, the sweep re-pended and
                    # this claim took back is a live job of ours again:
                    # its lease must be renewed from here on.
                    deps.disowned_jobs.discard(job.id)
                    # Fence the claim before the first await: the put is
                    # this loop's first yield since the claim committed,
                    # and the lost-claim probe (the heartbeat tick) must
                    # see the row held from the instant it is running and
                    # locked here, through the queue residence, until the
                    # consumer's take moves the coverage to the intent
                    # map.
                    deps.active_jobs.mark_enqueued(job.id)
                    await local_queue.put(job)
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

            if poll_wait in _done:
                # The fallback poll is its own cadence, already jittered
                # and never shorter than a burst: a poll-timed round owes
                # no further wait, whatever the previous round returned.
                claim_not_before = 0.0

    # Exit hand-back, on the DRAINING path only (producer_stop_event is
    # what the orchestrator sets at DRAINING entry): the DRAINING pass
    # re-pends what this worker held when it ran, but a claim round already
    # in flight at that moment commits AFTER it, the producer only
    # observes the stop event between rounds. Those rows are locked to a
    # process on its way out and no later phase sees them (they never reach
    # the active-jobs registry), so the producer hands back whatever it
    # still holds as its own last act on that path: by loop exit no further
    # claim of this worker's can commit, which is exactly the ordering the
    # single DRAINING pass could not give. The statement is the same
    # bounded, registry-excluding, attempt-refunding one (idempotent, a
    # row the first pass already released no longer matches), and its
    # failure mode is the helper's own (log + return 0; the lease-expiry
    # sweep remains the backstop). A bare shutdown_event exit (the
    # external-stop path, no orchestration) keeps the lease-reclaim shape
    # it has always had.
    if producer_stop_event.is_set() and made_a_claim:
        handed_back = await drain_local_queue_to_pending(deps, worker_id)
        if handed_back:
            _producer_log.info(
                "producer-exit-handback",
                worker_id=str(worker_id),
                rows_re_pended=handed_back,
            )

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
    it never dispatches, a divergent signature here would move that
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


async def _stub_terminal_write(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    *,
    cancelled: bool,
) -> None:
    """The stub loop's shielded terminal write, with its fenced outcome read.

    The stub is a test/dev sentinel with no hooks or publishers, so a
    fenced-out write has nothing further to unwind, but the outcome is
    still bound and logged, never dropped: a bare ``await`` discards the
    boolean that says whether the row actually moved, the discard shape
    the terminal-write class-closure guard forbids in every worker module.
    """
    if cancelled:
        landed = await shield_with_retrieval(
            backend.mark_cancelled(
                job.id, worker_id, attempt=job.attempt, claim_epoch=job.claim_epoch
            )
        )
    else:
        landed = await shield_with_retrieval(
            backend.mark_succeeded(
                job.id, worker_id, None, attempt=job.attempt, claim_epoch=job.claim_epoch
            )
        )
    if not landed:
        _consumer_log.debug(
            "consumer-stub-terminal-write-noop",
            job_id=str(job.id),
            cancelled=cancelled,
        )


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
    while not (shutdown_event.is_set() or deps.producer_stop_event.is_set()):
        q_get = asyncio.create_task(local_queue.get())
        shut_wait = asyncio.create_task(shutdown_event.wait())
        stop_wait = asyncio.create_task(deps.producer_stop_event.wait())
        try:
            _done, pending = await asyncio.wait(
                [q_get, shut_wait, stop_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if q_get not in _done:
                # A stop signal won the race and nothing was taken.
                return
            if deps.producer_stop_event.is_set():
                # DRAINING owns every row this worker holds: the get TAKEN
                # a row on the same turn the drain began. The hand-back
                # re-pends it to the fleet (the take moved only the
                # in-process queue entry, never the DB row), so running
                # the stale local copy here would execute the job body
                # twice. Return without dispatching.
                return
            # Why fall through on a shutdown_event-only both-done turn:
            # the get has already TAKEN the job out of local_queue and no
            # drain owns it (producer_stop_event is unset), returning here
            # would discard it with no consumer run, no terminal write, and
            # no release, leaving recovery to lock-lease expiry. The taken
            # job runs this final iteration; the outer while's shutdown
            # check then exits the loop.
        finally:
            for task in (q_get, shut_wait, stop_wait):
                if not task.done():
                    task.cancel()

        job: JobRow = q_get.result()

        # Record the claim intent BEFORE any await: between this take and
        # register() the DB row is running, locked here, and invisible to
        # active_jobs, and a hand-back pass running in that window would
        # re-pend it to the fleet while this loop is about to execute it.
        deps.active_jobs.mark_claimed(job.id)

        # Slot-release point #1 of 2: the get() above dropped
        # qsize by one, so a producer held up on queue capacity can
        # claim again, wake it now. Point #2 is the deregister at the
        # end of this iteration (the active-side slot), because the
        # producer's availability subtracts active jobs too.
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
                claim_epoch=job.claim_epoch,
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
                        await _stub_terminal_write(backend, job, worker_id, cancelled=True)
                    raise
                except TimeoutError:
                    pass

                await _stub_terminal_write(
                    backend, job, worker_id, cancelled=ctx.cancellation_requested
                )
                # fallback_result_ttl is not forwarded here: the stub path has
                # no actor registry and therefore no @actor(result_ttl=...)
                # literal to supply. If the stored actor_config.result_ttl is
                # cleared (NULL), the enqueue-pinned expiry survives, the
                # original bug, on the one path that structurally cannot fix
                # itself. This is acceptable because the stub is a test/dev
                # sentinel, not a production consumer. The real consumer
                # (_consumer.py) resolves the literal from the registry and
                # passes it through mark_succeeded_with_conn.

            except asyncio.CancelledError:
                with contextlib.suppress(asyncio.CancelledError):
                    await _stub_terminal_write(backend, job, worker_id, cancelled=True)
                raise

            except Exception:
                _consumer_log.exception("consumer-stub-error", job_id=str(job.id))

            finally:
                deps.active_jobs.resolve_claim(job.id)
                await deps.active_jobs.deregister(job.id)
                # Slot-release point #2: the producer's
                # availability subtracts active jobs, so this slot
                # frees at the deregister above, not at the get() that
                # only lent the queue slot, wake the producer the
                # moment accounting settles.
                if slot_freed_event is not None:
                    slot_freed_event.set()


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

    while not (shutdown_event.is_set() or deps.producer_stop_event.is_set()):
        q_get = asyncio.create_task(local_queue.get())
        shut_wait = asyncio.create_task(shutdown_event.wait())
        stop_wait = asyncio.create_task(deps.producer_stop_event.wait())
        try:
            _done, pending = await asyncio.wait(
                [q_get, shut_wait, stop_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if q_get not in _done:
                # A stop signal won the race and nothing was taken.
                return
            if deps.producer_stop_event.is_set():
                # DRAINING owns every row this worker holds: the get TAKEN
                # a row on the same turn the drain began. Running it would
                # double-execute, the row is locked to this worker and
                # never reached the active-jobs registry, so the hand-back
                # (the orchestrator's DRAINING pass for rows claimed before
                # it ran, the producer's exit pass for a claim round that
                # committed after) re-pends it to the fleet while this
                # loop's stale local copy would also run here. Return
                # without dispatching. The taken row needs no release from
                # THIS loop: the take moved only the in-process queue
                # entry, never the DB row, so whichever hand-back pass
                # runs finds it exactly as claimed.
                return
            # Why fall through on a shutdown_event-only both-done turn:
            # the get has already TAKEN the job out of local_queue and no
            # drain owns it (producer_stop_event is unset, this is the
            # external-exit path, not a graceful-shutdown orchestration) ,
            # returning here would discard it with no dispatch, no
            # terminal write, and no release, leaving recovery to
            # lock-lease expiry. The taken job runs this final iteration;
            # the outer while's shutdown check then exits the loop.
        finally:
            for task in (q_get, shut_wait, stop_wait):
                if not task.done():
                    task.cancel()

        job: JobRow = q_get.result()

        # Record the claim intent BEFORE any await: between this take and
        # register() the DB row is running, locked here, and invisible to
        # active_jobs, and a hand-back pass running in that window would
        # re-pend it to the fleet while this loop is about to execute it.
        deps.active_jobs.mark_claimed(job.id)

        # Slot-release point #1 of 2: the get() above dropped
        # qsize by one, so a producer held up on queue capacity can
        # claim again, wake it now. Point #2 is the finally around
        # dispatch_one_job below (the active-side slot), because the
        # producer's availability subtracts active jobs too. Fires on
        # every iteration, every exit path (success, failure, snooze,
        # not-found release) passes it.
        if slot_freed_event is not None:
            slot_freed_event.set()

        if job.actor not in actor_registry:
            _consumer_log.error(
                "dispatch-actor-not-found",
                job_id=str(job.id),
                actor=job.actor,
            )
            # Release the claimed job instead of leaving it 'running' until
            # lease expiry, a worker whose registry has the actor can then
            # pick it up. The short delay keeps this worker from re-claiming
            # it in a hot loop.
            #
            # Contract: an unregistered actor parks the job at the snooze
            # cadence, budget-free, mark_snoozed's default 'snoozed'
            # outcome refunds the claim's attempt increment, so a job whose
            # actor is missing (through no fault of its own) never burns
            # retry budget while it waits for a worker that can run it;
            # the stranded-jobs detector surfaces it. This is not an
            # actor-requested deferral semantically, but the snooze write
            # is the closest bounded outcome, a delay, a release, and a
            # released_reason marker in one transition.
            try:
                release_outcome = await backend.mark_snoozed(
                    job.id,
                    worker_id,
                    timedelta(seconds=10),
                    metadata_update={"released_reason": "actor-not-found"},
                    attempt=job.attempt,
                    claim_epoch=job.claim_epoch,
                )
            except Exception:
                _consumer_log.exception(
                    "dispatch-actor-not-found-release-failed",
                    job_id=str(job.id),
                    actor=job.actor,
                )
                # The row is still running under this worker's lock with
                # nothing left to move it: disown it so the heartbeat stops
                # renewing the lease and the reclaim sweep hands it back.
                deps.disowned_jobs.add(job.id)
            else:
                # The snooze tri-state is the write's fence and must be
                # read, not dropped: "scheduled" released the row,
                # "failed" terminalised it on the deadline arm, and
                # "noop" means the row stopped being this worker's to
                # move between claim and release (a reclaim re-pended
                # it, or a racing cancel terminalised it), the new
                # owner holds it, so there is nothing to release, but
                # a discarded outcome would make the fenced-out write
                # indistinguishable from a landed one.
                if release_outcome == "noop":
                    _consumer_log.debug(
                        "dispatch-actor-not-found-release-noop",
                        job_id=str(job.id),
                        actor=job.actor,
                    )
                elif release_outcome == "failed":
                    _consumer_log.info(
                        "dispatch-actor-not-found-release-deadline-exceeded",
                        job_id=str(job.id),
                        actor=job.actor,
                    )
            # This exit path bypasses the dispatch finally below: drop the
            # claim intent here, or a stale id would fence a future claim
            # of the same row out of every hand-back pass.
            deps.active_jobs.resolve_claim(job.id)
            continue

        actor_ref = actor_registry[job.actor]
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
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound, actor_ref carries the correct payload_type and fn.
                actor_config=actor_ref.config,
                clock=clock,
                active_jobs=deps.active_jobs,
                max_retry_backoff=deps.settings.max_retry_backoff,
                enqueuer=enqueuer,
            )
            if outcome == "failed":
                deps.drain_failures += 1
        except SlotPoolAcquireError:
            # Infrastructure, not a job outcome: counting this as a drain
            # failure would make a Kubernetes Job / CI drain step report
            # job failures that never happened. The acquire was recorded
            # (counter) and logged (per-occurrence cause, job id) at the
            # raise site. The row though is still running under this
            # worker's lock with no runner left to move it: disown it so
            # the heartbeat stops renewing the lease and the reclaim
            # sweep hands it back - the same treatment the
            # actor-not-found release-failure arm above gets (the
            # pre-fix comment here said "leave the job to lease
            # reclaim", but the tick's renewal kept that lease alive
            # forever, so the recovery it named never came and the row
            # was lost).
            _disown_job(deps.disowned_jobs, job)
            continue
        except Exception:
            _consumer_log.exception("dispatch-failed", job_id=str(job.id))
            deps.drain_failures += 1
        finally:
            # The claim intent's life ends here on every path: register()
            # absorbed it when the job ran, and every non-running path
            # (skip, release, acquire failure, error) is past the window
            # the intent exists to fence. Resolving in the loop's own
            # finally cannot straddle a drain: the row is no longer
            # running-and-unowned by the time this runs.
            deps.active_jobs.resolve_claim(job.id)
            # Slot-release point #2: the producer's availability
            # subtracts active jobs, so the slot this job held frees at
            # the deregister dispatch_one_job's own finally has run by
            # every path that reaches here, not at the get() that only
            # lent the queue slot. The wake is what re-arms the producer
            # the moment accounting settles; without it a finished job's
            # replacement claim would wait for the fallback poll tick.
            # (The SlotPoolAcquireError path never registered, so its
            # wake is redundant with the get()-point one, bounded, and
            # the price of one unconditional release point.)
            if slot_freed_event is not None:
                slot_freed_event.set()


async def register_worker(pool: asyncpg.Pool, settings: WorkerSettings) -> UUID:
    """Register the current worker in ``taskq.workers`` and return its UUID.

    Generates a UUIDv7, inserts a row into ``{schema}.workers``, and returns
    the new UUID.  Acquires from *pool* with a 2.0 s timeout; on timeout or
    connection error the failure is logged and re-raised (registering is
    fatal).

    If ``settings.worker_label`` or ``settings.workgroup_instance`` are set,
    they are stored directly for cross-process correlation and health checking.

    The row's metadata records the worker's runtime facts: whether NOTIFY
    dispatch is enabled, and ``max_concurrency``, the capacity the worker
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
    # for monitoring and coordination.
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
    returns without raising, the recovery sweep is the backstop and
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


#: Sentinel reported for an actor-level cap the operator deliberately left
#: unset. A blank field reads as missing data; this reads as configuration.
_UNCAPPED = "uncapped"


def _emit_resolved_capacity_startup_lines(
    settings: WorkerSettings,
    actor_registry: Mapping[str, ActorRef[Any, Any]],
    *,
    stored_rows: Mapping[str, ActorConfigRow],
    queue_rows: Mapping[str, QueueRow],
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Publish the concurrency each registered actor actually resolves to.

    Four independent surfaces cap an actor and no configuration view shows
    them together: the worker's own ``max_concurrency``, the stored
    ``actor_config.max_concurrent``, the ``queues`` row's cap for the queue
    the actor is assigned to, and the actor's declared reservations or
    ``singleton=True``. Whichever is smallest binds, so raising any of the
    other three changes nothing, the failure an operator reports as "I
    bumped the cap and nothing happened". Each line carries both the
    resolved number and the layer that produced it, so the answer is a grep
    rather than a four-surface reconstruction.

    A second line rides the same pass: the startup UPSERT leaves the
    capacity columns alone once a row exists, so a changed ``@actor(...)``
    literal is silently overridden by the stored value. That precedence is
    deliberate, stored capacity is operator-owned, but it is exactly the
    state in which a deployed change appears to be ignored.

    Never raises and never refuses boot: every condition reported here is
    diagnosable-but-workable, and a worker able to do work must start.
    """
    for name in sorted(actor_registry):
        ref = actor_registry[name]
        stored = stored_rows.get(name)
        reservation_entries = list(ref.reservations or ())
        # ``ref.reservations`` carries whatever the actor declared: a bare
        # ``str`` name (resolved against the rate-limit registry elsewhere,
        # so its slot count is not knowable from this ref alone), or a
        # materialised ``ConcurrencyReservation`` / ``KeyedReservationRef``
        # object, both of which carry ``.slots`` directly. The object forms
        # are not JSON-serializable (``taskq._json.dumps`` has no fallback
        # for them, nor for ``KeyedReservationRef``'s ``key_fn`` callable),
        # and structlog's JSON renderer is not exception-wrapped, logging
        # the raw object drops the entire line. Report each entry as its
        # name plus its slot count when the count is knowable.
        reservation_names: list[str] = []
        reservation_slots: list[int] = []
        for entry in reservation_entries:
            if isinstance(entry, ConcurrencyReservation | KeyedReservationRef):
                entry_name = (
                    entry.name if isinstance(entry, ConcurrencyReservation) else entry.base_name
                )
                reservation_names.append(entry_name)
                reservation_slots.append(entry.slots)
            else:
                reservation_names.append(entry)

        if stored is None:
            # The dispatch capacity gate joins actor_config, so an actor
            # with no stored row dispatches nothing at all. Reporting the
            # code literal here would name a number that never applies.
            log.info(
                "actor-resolved-capacity",
                actor=name,
                queue=ref.queue,
                resolved=0,
                binding="no-stored-row",
                actor_cap=_UNCAPPED,
                process_cap=settings.max_concurrency,
                reservations=reservation_names,
                drain_mode=False,
                note=(
                    "no actor_config row: the dispatch capacity gate joins "
                    "actor_config, so this actor dispatches nothing until a "
                    "row exists"
                ),
            )
            continue

        queue_cap = (
            queue_rows.get(stored.queue) or QueueRow(stored.queue, "", None)
        ).max_concurrent

        # Layers in reported-precedence order, least first. Ties keep the
        # earlier entry, which is why the numeric layers are ordered
        # narrowest-scope first: an actor cap equal to the process cap is
        # the one an operator can act on.
        layers: list[tuple[int, str]] = [(settings.max_concurrency, "process")]
        if stored.max_concurrent is not None:
            layers.insert(0, (stored.max_concurrent, "actor"))
        if queue_cap is not None:
            layers.insert(0, (queue_cap, "queue"))
        if ref.singleton:
            layers.insert(0, (1, "singleton"))
        if reservation_slots:
            # A materialised reservation's slot count is authoritative and
            # is what actually gates admission, reporting the process cap
            # here would print a number that is wrong whenever the
            # reservation is narrower (or wider) than it.
            layers.insert(0, (min(reservation_slots), "reservation"))
        elif reservation_names:
            # Only bare-name entries are present, the concrete slot count
            # lives in the rate-limit registry, not on this ref, so it
            # cannot be reported as a specific number without risking a
            # wrong one. Still named as the binding layer whenever no
            # numeric layer is provably narrower.
            layers.insert(0, (settings.max_concurrency, "reservation"))

        resolved, binding = min(layers, key=lambda layer: layer[0])

        log.info(
            "actor-resolved-capacity",
            actor=name,
            queue=stored.queue,
            resolved=resolved,
            binding=binding,
            actor_cap=_UNCAPPED if stored.max_concurrent is None else stored.max_concurrent,
            queue_cap=_UNCAPPED if queue_cap is None else queue_cap,
            process_cap=settings.max_concurrency,
            reservations=reservation_names,
            singleton=ref.singleton,
            # Zero is the value most likely to be read as "unset", by an
            # operator and by a falsy check alike; drain mode is stated so
            # an actor that dispatches nothing on purpose does not look
            # identical to a broken one.
            drain_mode=binding == "actor" and resolved == 0,
        )

        if capacity_field_diverges(ref.max_concurrent, stored.max_concurrent):
            log.warning(
                "actor-config-capacity-divergence",
                actor=name,
                declared=ref.max_concurrent,
                stored=_UNCAPPED if stored.max_concurrent is None else stored.max_concurrent,
                note=(
                    "the stored actor_config capacity wins: the startup upsert "
                    "leaves the capacity columns alone once a row exists, so "
                    "the declared value has no effect until the stored row is "
                    "changed"
                ),
            )
