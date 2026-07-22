"""Test-runner helpers for InMemoryBackend.

These functions and types are NOT part of the Backend protocol — they
exist solely to drive deterministic test execution (``run_until_drained``,
cancel-polling simulation, stub/actor-config registration, archive
simulation, and ``wait_for_batch``).

:class:`InMemoryBackend` keeps thin delegate methods that forward here so
the public call surface (``backend.run_until_drained()`` etc.) is
preserved without test-file changes.
"""

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from pydantic import BaseModel

from taskq.backend._protocol import (
    EventRow,
    JobId,
    JobRow,
    QueueMode,
)
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.batch import BatchCompletionStatus
from taskq.context import JobContext
from taskq.exceptions import Snooze
from taskq.retry import OnRetryExhausted, OnSuccess, RetryClassifierHook, RetryPolicy
from taskq.worker.actor_config import ActorConfig

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend
    from taskq.worker.leader import ArchiveExpiryResult, PruneResult

__all__ = [
    "PassthroughPayload",
    "StubFn",
    "wait_for_batch",
]

logger = structlog.get_logger("taskq.testing.in_memory")

# ── Type aliases ───────────────────────────────────────────────────────

StubFn = Callable[..., object]
"""Type alias for actor stubs registered via :meth:`InMemoryBackend.register_stub`.

Intentionally broad (``Callable[..., object]``) because stubs are test-only
code where strict parameter checking is not required and the actor signature
varies.  Stubs MAY be ``async def`` or plain ``def``; ``run_until_drained``
inspects the return value with ``isinstance(result, Awaitable)`` and awaits
accordingly.
"""


# ── Helper classes ─────────────────────────────────────────────────────


class _StubContext:
    """Minimal context passed to actor stubs by ``run_until_drained``.

    The full ``JobContext`` arrives later; here, stubs receive a duck-typed
    object with the fields they read: ``job_id``, ``attempt``, ``payload``,
    ``cancel_event``, and ``cancellation_requested``.  Aligned with the
    production ``taskq.context.JobContext`` shape at the duck-typed
    ``cancel_event`` / ``cancellation_requested`` level.
    """

    __slots__ = ("attempt", "cancel_event", "job_id", "payload")

    def __init__(
        self,
        job_id: JobId,
        attempt: int,
        payload: dict[str, object],
        cancel_event: asyncio.Event | None,
    ) -> None:
        self.job_id = job_id
        self.attempt = attempt
        self.payload = payload
        self.cancel_event = cancel_event

    @property
    def cancellation_requested(self) -> bool:
        return self.cancel_event is not None and self.cancel_event.is_set()


class PassthroughPayload(BaseModel):
    """Permissive payload model used by the in-memory test runner.

    Tests register stubs with raw ``dict[str, object]`` payloads; the
    production consumer expects an actor-supplied :class:`pydantic.BaseModel`.
    This model bridges the gap — ``model_config = {"extra": "allow"}``
    means any field shape validates, and ``model_dump()`` round-trips
    through the same JSON adapter as a real payload model. Tests that
    care about typed payloads pass an explicit ``payload_type`` to
    :meth:`InMemoryBackend.register_stub`; tests that don't get this
    permissive default.
    """

    model_config = {"extra": "allow"}


@dataclass(frozen=True, slots=True)
class _InMemoryActorConfig:
    """Minimal frozen dataclass satisfying ActorConfigLike.

    Stored alongside stub functions so ``run_until_drained`` can build
    the ``ActorConfigLike`` the classifier needs without a concrete
    ActorConfig class (which lands with the @actor decorator).
    """

    retry: RetryPolicy
    non_retryable_exceptions: tuple[type[BaseException], ...] = ()
    retry_classifier: RetryClassifierHook | None = None
    on_retry_exhausted: OnRetryExhausted | None = None
    on_retry_exhausted_timeout: float = 3.0
    on_success: OnSuccess | None = None
    on_success_timeout: float = 3.0
    payload_type: type[BaseModel] = PassthroughPayload


@dataclass(frozen=True, slots=True)
class _ArchivedJobRow:
    """Wrapper around JobRow adding archive-specific columns.

    Mirrors the ``jobs_archive`` table shape: the original ``JobRow`` plus
    ``archived_at`` and ``expire_at`` columns added by the prune CTE.
    """

    row: JobRow
    archived_at: datetime
    expire_at: datetime


def _build_run_actor(
    stub: StubFn,
    cancel_events: dict[JobId, asyncio.Event],
) -> Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]]:
    """Return a ``run_actor`` callback for ``consume_one_job`` that wraps
    *stub* and builds a :class:`_StubContext` from the job row.

    Binding *stub* as a function parameter avoids Ruff B023 (loop-variable
    capture) because the closure captures the already-bound parameter,
    not the loop variable in ``run_until_drained``.
    """

    async def run_actor(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:  # pyright: ignore[reportUnusedParameter]  # Why: _build_run_actor receives the production JobContext from consume_one_job but passes a duck-typed _StubContext to the stub; the ctx parameter is unused here.
        stub_ctx = _StubContext(
            job_id=job_row.id,
            attempt=job_row.attempt,
            payload=job_row.payload,
            cancel_event=cancel_events.get(job_row.id),
        )
        result = stub(job_row.payload, stub_ctx)
        if isinstance(result, Awaitable):
            from typing import cast

            result = await cast(Awaitable[object], result)
        return result

    return run_actor


# ── Clock helpers ──────────────────────────────────────────────────────


def advance_clock_to(backend: "InMemoryBackend", when: datetime) -> None:
    """Advance the internal clock to *when* (test-only; requires FakeClock).

    This is the public surface for time-travel in tests, avoiding
    direct access to the private ``_clock`` attribute and the
    ``FakeClock.move_to`` method that is not on the ``Clock`` protocol.
    Raises :class:`TypeError` if the backend's clock is not a
    :class:`FakeClock`.
    """
    from taskq.testing.clock import FakeClock

    if not isinstance(backend._clock, FakeClock):  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        raise TypeError(
            "advance_clock_to requires a FakeClock; "
            "not supported with SystemClock or other clock types"
        )
    backend._clock.move_to(when)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


# ── Stub registration ──────────────────────────────────────────────────


def register_stub(
    backend: "InMemoryBackend",
    actor_name: str,
    fn: StubFn,
    *,
    retry: RetryPolicy | None = None,
    non_retryable_exceptions: tuple[type[BaseException], ...] = (),
    retry_classifier: RetryClassifierHook | None = None,
    on_retry_exhausted: OnRetryExhausted | None = None,
    on_retry_exhausted_timeout: float = 3.0,
    on_success: OnSuccess | None = None,
    on_success_timeout: float = 3.0,
    payload_type: type[BaseModel] | None = None,
) -> None:
    """Record a stub function for *actor_name*. Re-registration overwrites
    (test ergonomics). Stubs MAY be ``async def`` or plain ``def``;
    ``run_until_drained`` inspects the return value with
    ``isinstance(result, Awaitable)`` and awaits accordingly.

    The stub receives ``(payload, ctx)`` where *ctx* is a minimal object
    with ``job_id: JobId``, ``attempt: int``, ``payload: dict``, and
    ``cancel_event: asyncio.Event | None``.

    ``payload_type`` is the Pydantic model the consumer validates the
    raw row payload against before invoking the stub. Tests that don't
    care about payload validation may omit it; the runner falls back
    to :class:`PassthroughPayload` (``extra="allow"``).

    Actor config fields (retry, non_retryable_exceptions,
    retry_classifier, on_retry_exhausted, on_retry_exhausted_timeout,
    on_success, on_success_timeout) are stored alongside the stub and
    used by ``run_until_drained`` when calling ``decide_after_failure``.
    The default ``RetryPolicy(jitter=0.0)`` matches the historical inline
    ``5 * 2^(attempt-1)`` backoff formula exactly, preserving existing
    test behaviour.
    """
    backend._actor_stubs[actor_name] = fn  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
    backend._actor_configs[actor_name] = _InMemoryActorConfig(  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        retry=retry if retry is not None else RetryPolicy(jitter=0.0),
        non_retryable_exceptions=non_retryable_exceptions,
        retry_classifier=retry_classifier,
        on_retry_exhausted=on_retry_exhausted,
        on_retry_exhausted_timeout=on_retry_exhausted_timeout,
        on_success=on_success,
        on_success_timeout=on_success_timeout,
        payload_type=payload_type if payload_type is not None else PassthroughPayload,
    )
    # Ensure the stub-registered actor can dispatch —
    # the dispatch gate requires _actor_configs_meta entries
    # when any actor_config is registered.
    if actor_name not in backend._actor_configs_meta:  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        backend._actor_configs_meta[actor_name] = ActorConfig(  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            actor=actor_name, max_concurrent=None, queue="default"
        )


def register_cancel_event(backend: "InMemoryBackend", job_id: JobId, event: asyncio.Event) -> None:
    """Store a per-job cancel event so ``tick_cancel_polling`` can fire it.

    ``actor_runner`` calls this before executing the actor;
    ``tick_cancel_polling`` looks up the event and calls ``event.set()``
    on first observation of ``cancel_phase == 1``.
    """
    backend._cancel_events[job_id] = event  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


def register_actor_config(
    backend: "InMemoryBackend",
    *,
    actor: str,
    max_concurrent: int | None = None,
    queue: str = "default",
    metadata: dict[str, object] | None = None,
) -> None:
    """Register a single actor configuration for dispatch simulation.

    Builds an ``ActorConfig`` from keyword arguments and stores it
    in ``_actor_configs_meta``.  Only ``max_concurrent`` is read by
    dispatch; ``queue`` and ``metadata`` are stored for future use.
    """
    backend._actor_configs_meta[actor] = ActorConfig(  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        actor=actor,
        max_concurrent=max_concurrent,
        queue=queue,
        metadata=metadata if metadata is not None else {},
    )


def register_actor_configs(backend: "InMemoryBackend", configs: Iterable[ActorConfig]) -> None:
    """Register multiple ``ActorConfig`` instances at once.

    Equivalence tests use this to mirror PG ``actor_config``
    pre-population without isinstance-branching.
    """
    for cfg in configs:
        backend._actor_configs_meta[cfg.actor] = cfg  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


def set_queue_mode(backend: "InMemoryBackend", queue_name: str, mode: QueueMode) -> None:
    """Set the dispatch mode for a queue (test-only).

    ``strict_fifo`` (the default) dispatches by priority then
    scheduled_at. ``round_robin`` interleaves fairness_key cohorts
    per actor within the dispatch window.
    """
    backend._queues[queue_name] = mode  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


# ── Event and archive accessors ────────────────────────────────────────


async def get_events(backend: "InMemoryBackend", job_id: JobId) -> list[EventRow]:
    """Return events for *job_id* (test-only accessor)."""
    return [e for e in backend._events if e.job_id == job_id]  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


def archive_terminal_jobs(
    backend: "InMemoryBackend",
    retention: timedelta,
    archive_retention: timedelta,
    *,
    statuses: frozenset[str] | None = None,
) -> "PruneResult":
    """Move terminal jobs older than *retention* to the archive.

    Simulates the PG archive-move CTE for unit tests.  Identifies
    terminal jobs where ``finished_at < clock.now() - retention``.
    For each qualifying job: copies to ``_archive`` with
    ``archived_at = clock.now()`` and ``expire_at = clock.now() +
    archive_retention``; copies ``_attempts`` entries to
    ``_archive_attempts``; removes from ``_jobs`` and ``_attempts``.

    When *statuses* is provided, only jobs in those terminal statuses
    are considered — allowing the caller to simulate per-status
    retention by making separate calls per status group (matching
    the PG ``prune_terminal_jobs`` per-status CTE pattern).

    NOT on the Backend Protocol — the leader calls this as a
    concrete method.
    """
    from taskq.worker.leader import PruneResult

    now = backend._clock.now()  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
    cutoff = now - retention
    by_actor: dict[str, int] = {}
    by_status: dict[str, int] = {}
    cutoffs: dict[str, datetime] = {}
    archived_count = 0

    to_archive: list[JobId] = []
    for job_id, row in list(backend._jobs.items()):  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if row.status not in TERMINAL_STATUSES:
            continue
        if statuses is not None and row.status not in statuses:
            continue
        if row.finished_at is not None and row.finished_at < cutoff:
            to_archive.append(job_id)

    for job_id in to_archive:
        row = backend._jobs.pop(job_id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        attempts = backend._attempts.pop(job_id, [])  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        backend._archive[job_id] = _ArchivedJobRow(  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            row=row,
            archived_at=now,
            expire_at=now + archive_retention,
        )
        if attempts:
            backend._archive_attempts[job_id] = attempts  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        by_actor[row.actor] = by_actor.get(row.actor, 0) + 1
        by_status[row.status] = by_status.get(row.status, 0) + 1
        archived_count += 1

    for status in by_status:
        cutoffs[status] = cutoff

    return PruneResult(
        total_deleted=archived_count,
        archived=archived_count,
        by_actor=by_actor,
        by_status=by_status,
        cutoffs=cutoffs,
        duration_ms=0,
    )


def expire_archived_jobs(backend: "InMemoryBackend") -> "ArchiveExpiryResult":
    """Hard-delete archived rows where ``expire_at < clock.now()``.

    Simulates the PG archive expiry CTE for unit tests.  Removes
    expired rows from ``_archive`` and their corresponding entries
    from ``_archive_attempts``.

    NOT on the Backend Protocol — the leader calls this as a
    concrete method.
    """
    from taskq.worker.leader import ArchiveExpiryResult

    now = backend._clock.now()  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
    by_status: dict[str, int] = {}
    total = 0

    to_delete: list[JobId] = []
    for job_id, archived in list(backend._archive.items()):  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if archived.expire_at < now:
            to_delete.append(job_id)

    for job_id in to_delete:
        archived = backend._archive.pop(job_id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        backend._archive_attempts.pop(job_id, None)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        by_status[archived.row.status] = by_status.get(archived.row.status, 0) + 1
        total += 1

    return ArchiveExpiryResult(
        total_deleted=total,
        by_status=by_status,
        expire_before=now,
        duration_ms=0,
    )


async def get_archived(backend: "InMemoryBackend", job_id: JobId) -> _ArchivedJobRow | None:
    """Return the archived job row for *job_id*, or ``None``.

    Supports the admin UI fallback pattern: a job absent from
    ``_jobs`` but present in ``_archive`` is retrievable.
    """
    return backend._archive.get(job_id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


# ── Cancel polling simulation ──────────────────────────────────────────


async def tick_cancel_polling(backend: "InMemoryBackend") -> None:
    """Simulate the heartbeat's cancel-poll-and-escalate step.

    Iterates jobs where ``cancel_requested_at IS NOT NULL AND
    status == "running"``.  On first observation of ``cancel_phase == 1``,
    records ``_cancel_observed_at[job_id] = clock.now()`` and fires the
    per-job cancel event (registered via ``register_cancel_event``).
    Subsequent calls escalate ``cancel_phase = 2`` if the cancellation
    grace period has elapsed, or mark ``abandoned`` if the cleanup
    grace period has also elapsed.

    MUST NOT sleep or yield to the event loop.
    """
    now = backend._clock.now()  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
    for job_id, row in list(backend._jobs.items()):  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if row.cancel_requested_at is None or row.status != "running":
            continue

        if row.cancel_phase == 1 and job_id not in backend._cancel_observed_at:  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            # First observation: record time and fire cancel event
            backend._cancel_observed_at[job_id] = now  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            cancel_event = backend._cancel_events.get(job_id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            if cancel_event is not None:
                cancel_event.set()
            logger.debug(
                "cancel_observed",
                kind="state_change",
                from_state="running",
                to_state="running",
                job_id=str(job_id),
                cancel_phase=1,
            )

        elif (
            row.cancel_phase == 1
            and job_id in backend._cancel_observed_at  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            and now - backend._cancel_observed_at[job_id] > backend._cancellation_grace  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        ):
            # Escalate to phase 2 — delegate to write_cancel_escalation
            # so the EventRow is written.
            await backend.write_cancel_escalation(job_id, backend._worker_id, 2)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.

        elif (
            row.cancel_phase == 2
            and job_id in backend._cancel_observed_at  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            and now - backend._cancel_observed_at[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            > backend._cancellation_grace + backend._cleanup_grace  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        ):
            # Mark abandoned via the public method so attempt/event rows
            # are written.  mark_abandoned's own
            # cancel_phase==2 guard is satisfied by the condition above.
            await backend.mark_abandoned(job_id)

    # Cleanup: remove cancel-tracking state for terminal jobs to prevent
    # unbounded growth of _cancel_events and _cancel_observed_at.
    for job_id in list(backend._cancel_observed_at):  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        row = backend._jobs.get(job_id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if row is not None and row.status in TERMINAL_STATUSES:
            del backend._cancel_observed_at[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            backend._cancel_events.pop(job_id, None)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


# ── Run until drained ──────────────────────────────────────────────────


def _earliest_scheduled_at(backend: "InMemoryBackend") -> datetime | None:
    """Return the earliest ``scheduled_at`` among scheduled jobs, or
    ``None`` if no scheduled jobs exist.
    """
    scheduled_times = [
        r.scheduled_at
        for r in backend._jobs.values()  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if r.status == "scheduled"
    ]
    return min(scheduled_times) if scheduled_times else None


async def run_until_drained(backend: "InMemoryBackend") -> None:
    """Execute the dispatch-then-execute loop until drained.

    The loop:
    1. Promotes scheduled→pending.
    2. Dispatches the next highest-priority job (via ``dispatch_batch``
       with ``limit=1``).
    3. If no job dispatchable: checks termination conditions.  If a
       future-scheduled job exists, advances the FakeClock and
       continues; otherwise returns.
    4. Delegates per-job execution to ``consume_one_job``,
       which handles ``Snooze``, ``RetryAfter``,
       ``ReservationUnavailable``, generic exceptions, cancellation,
       and success.
    5. Terminates when: no pending, no running, no scheduled-due jobs.

    Clock advancement: if the backend's clock is a ``FakeClock`` with
    ``move_to``, the loop advances to the earliest ``scheduled_at`` when
    nothing is dispatchable but future-scheduled jobs exist.  If the
    clock lacks ``move_to`` (production code), the loop returns instead.
    This branch is documented because ``run_until_drained`` is a
    test-only method that should not be called with a real clock.

    Dispatch uses ``dispatch_batch(self._worker_id, queues, limit=1,
    lock_lease=timedelta(seconds=60))`` where *queues* is derived from
    the set of queues currently in use (all unique queue names from
    ``_jobs.values()``).  This mirrors the single-worker model.
    """
    from taskq.worker._consumer import consume_one_job

    while True:
        # Step 1: promote scheduled→pending
        await backend.scheduled_to_pending(backend._clock.now())  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.

        # Step 2: dispatch one job
        queues = list({r.queue for r in backend._jobs.values()})  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        dispatched = await backend.dispatch_batch(
            backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            queues,
            limit=1,
            lock_lease=timedelta(seconds=60),
        )

        if not dispatched:
            # Step 3: check termination / clock-advance conditions.
            next_at = _earliest_scheduled_at(backend)
            if next_at is None:
                # No scheduled jobs at all — fully drained.
                return

            # Advance clock if FakeClock, else return.
            move_to = getattr(backend._clock, "move_to", None)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            if callable(move_to):
                move_to(next_at)
                continue
            else:
                # Production code wouldn't call run_until_drained
                return

        # Step 4: delegate per-job execution to consume_one_job
        job = dispatched[0]
        stub = backend._actor_stubs.get(job.actor)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if stub is None:
            raise RuntimeError(f"no stub registered for actor: {job.actor}")

        actor_cfg = backend._actor_configs.get(job.actor)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if actor_cfg is None:
            actor_cfg = _InMemoryActorConfig(retry=RetryPolicy(jitter=0.0))

        await consume_one_job(
            backend,
            job,
            backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            run_actor=_build_run_actor(stub, backend._cancel_events),  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            actor_config=actor_cfg,
            payload_type=actor_cfg.payload_type,
            clock=backend._clock,  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        )


# ── In-memory wait_for_batch simulation ─────────────────────────────────

_min_snooze = timedelta(seconds=1)

_wfb_logger = structlog.get_logger("taskq.testing.in_memory")


async def wait_for_batch(
    backend: "InMemoryBackend",
    batch_id: UUID,
    *,
    snooze_interval: timedelta = timedelta(seconds=10),
    snooze_via_exception: bool = True,  # Why: parameter kept for API consistency with taskq.batch.wait_for_batch; in-memory always raises Snooze
) -> BatchCompletionStatus:
    """In-memory simulation of :func:`taskq.batch.wait_for_batch`.

    Scans ``backend._jobs`` for rows matching ``batch_id`` and computes
    :class:`~taskq.batch.BatchCompletionStatus` using the same
    terminal-status set as the PG path.

    The in-memory variant always raises :class:`~taskq.exceptions.Snooze`
    when ``pending > 0``, regardless of ``snooze_via_exception`` — the
    in-memory backend has no sleep cost.
    """
    if snooze_interval < _min_snooze:
        original = snooze_interval
        snooze_interval = _min_snooze
        _wfb_logger.warning(
            "snooze-interval-clamped",
            original=str(original),
            clamped=str(snooze_interval),
        )

    batch_id_str = str(batch_id)
    matched = [
        r
        for r in backend._jobs.values()  # pyright: ignore[reportPrivateUsage]  # Why: wait_for_batch is a co-located module-level helper that requires access to the private job store; same pattern as list_jobs
        if r.metadata.get("batch_id") == batch_id_str
    ]

    succeeded = sum(1 for r in matched if r.status == "succeeded")
    failed = sum(1 for r in matched if r.status == "failed")
    cancelled = sum(1 for r in matched if r.status == "cancelled")
    crashed = sum(1 for r in matched if r.status == "crashed")
    abandoned = sum(1 for r in matched if r.status == "abandoned")
    pending = sum(1 for r in matched if r.status not in TERMINAL_STATUSES)

    status = BatchCompletionStatus(
        total=len(matched),
        pending=pending,
        succeeded=succeeded,
        failed=failed,
        cancelled=cancelled,
        crashed=crashed,
        abandoned=abandoned,
    )

    if status.total == 0:
        _wfb_logger.warning(
            "wait-for-batch-empty",
            batch_id=batch_id_str,
        )

    if pending > 0:
        raise Snooze(snooze_interval)

    return status
