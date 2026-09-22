"""Test-runner helpers for InMemoryBackend.

These functions and types are NOT part of the Backend protocol, they
exist solely to drive deterministic test execution (``run_until_drained``,
cancel-polling simulation, stub/actor-config registration, archive
simulation, and ``wait_for_batch``).

:class:`InMemoryBackend` keeps thin delegate methods that forward here so
the public call surface (``backend.run_until_drained()`` etc.) is
preserved without test-file changes.
"""

import asyncio
import contextlib
import traceback
import warnings
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import structlog
from opentelemetry.trace import Span
from pydantic import BaseModel

from taskq.actor_config import ActorConfig
from taskq.backend._protocol import (
    ErrorInfo,
    EventRow,
    JobId,
    JobRow,
    QueueMode,
)
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.batch import BatchCompletionStatus, apply_batch_terminal_outcome, decide_batch_status
from taskq.context import JobContext
from taskq.exceptions import PayloadValidationError, Snooze
from taskq.obs import bind_job_context
from taskq.retry import OnCancel, OnRetryExhausted, OnSuccess, RetryClassifierHook, RetryPolicy
from taskq.testing._reads import _event_read_copy, _read_copy

if TYPE_CHECKING:
    # taskq.actor stays TYPE_CHECKING here: a runtime import would pull
    # asyncpg into the driver-free testing boundary (pinned by
    # test_testing_no_transitive_asyncpg). Runtime discrimination uses
    # isinstance(str) instead, see register_stub.
    from taskq.actor import ActorRef
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.testing.in_memory import InMemoryBackend
    from taskq.worker.leader import ArchiveExpiryResult, PruneResult

__all__ = [
    "PassthroughPayload",
    "StubFn",
    "StubPayloadTypeWarning",
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
    object with the fields they read: ``job_id``, ``attempt``,
    ``snooze_count``, ``payload``, ``cancel_event``, ``span``, and
    ``cancellation_requested``.  Aligned with the production
    ``taskq.context.JobContext`` shape at the duck-typed
    ``cancel_event`` / ``cancellation_requested`` level, at the
    deferral-cycle contract (``snooze_count`` carries the row's count of
    completed non-consuming deferrals at dispatch time, so an actor
    keyed off it behaves identically under the test runner and the PG
    worker), and at the trace-correlation contract (``span`` is the
    documented OTel-disabled ``None``, the in-memory runner is
    uninstrumented, so actors reading ``ctx.span`` observe exactly what
    a production worker without a tracer hands them), and at the
    documented method surface: ``check_cancelled()``,
    ``should_abort()``, and ``await ctx.progress(...)`` (recorded on
    ``progress_reports``, never published, the runner has no
    Redis/Postgres wiring) behave as the production contract documents,
    so actors using them are exercisable under the runner.

    The sub-job and logging surfaces are wired for real, not stubbed:
    ``jobs`` is a production :class:`~taskq.client._enqueuer.SubJobEnqueuer`
    bound to the runner's own backend (the same shape
    ``consume_one_job`` builds for the in-memory path), so an actor
    calling ``await ctx.jobs.enqueue(...)`` enqueues a real row the
    drain then dispatches, and ``log`` is a structlog logger bound with
    the job-scope fields (``job_id``, ``actor``, ``queue``, ``attempt``)
    the production context binds.
    """

    __slots__ = (
        "attempt",
        "cancel_event",
        "job_id",
        "jobs",
        "log",
        "payload",
        "progress_reports",
        "snooze_count",
        "span",
    )

    def __init__(
        self,
        job_id: JobId,
        attempt: int,
        payload: dict[str, object],
        cancel_event: asyncio.Event | None,
        snooze_count: int = 0,
        *,
        jobs: "SubJobEnqueuer | None" = None,
        log: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self.job_id = job_id
        self.attempt = attempt
        self.payload = payload
        self.cancel_event = cancel_event
        self.snooze_count = snooze_count
        self.span: Span | None = None
        self.jobs = jobs
        self.log = log
        # One record per progress() call, the harness half of the
        # documented progress contract: the report lands observably
        # (the stub or its test inspects this list), with `seq` strictly
        # monotone per call as production guarantees. The runner has no
        # Redis/Postgres wiring, so nothing is published.
        self.progress_reports: list[dict[str, object]] = []

    @property
    def cancellation_requested(self) -> bool:
        return self.cancel_event is not None and self.cancel_event.is_set()

    def check_cancelled(self) -> None:
        """Raise :class:`asyncio.CancelledError` when cancellation has
        been requested, the production contract, so stubs using the
        raising style are exercisable under the runner."""
        if self.cancellation_requested:
            raise asyncio.CancelledError

    def should_abort(self) -> bool:
        """Synchronous cancellation check for sync actors. Production
        reads the same phase-1 cancellation state through a threading
        event; the runner has one cancellation event, so both checks
        read it."""
        return self.cancellation_requested

    async def progress(
        self,
        *,
        step: int | None = None,
        percent: float | None = None,
        detail: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        """Record a progress report on the context (see the
        ``progress_reports`` attribute). Signature and ``seq``
        monotonicity mirror the production
        :meth:`taskq.context.JobContext.progress`; the runner never
        blocks on the network because it never publishes."""
        self.progress_reports.append(
            {
                "seq": len(self.progress_reports) + 1,
                "step": step,
                "percent": percent,
                "detail": detail,
                "data": data,
            }
        )


class PassthroughPayload(BaseModel):
    """Permissive payload model used by the in-memory test runner.

    Tests register stubs with raw ``dict[str, object]`` payloads; the
    production consumer expects an actor-supplied :class:`pydantic.BaseModel`.
    This model bridges the gap, ``model_config = {"extra": "allow"}``
    means any field shape validates, and ``model_dump()`` round-trips
    through the same JSON adapter as a real payload model. It is the
    deliberate escape hatch: pass ``payload_type=PassthroughPayload`` to
    :meth:`InMemoryBackend.register_stub` to opt into it. When
    ``payload_type`` is omitted the runner first resolves the actor's
    declared model from a passed :class:`~taskq.actor.ActorRef`, and only
    falls back to this permissive default, with a
    :class:`StubPayloadTypeWarning`, when the actor is a bare name the
    runner cannot resolve.
    """

    model_config = {"extra": "allow"}


class StubPayloadTypeWarning(UserWarning):
    """``register_stub`` could not resolve the actor's declared payload model.

    Emitted when a stub is registered by bare actor name with no
    ``payload_type=``: the runner has no declared model to validate
    against, so ``run_until_drained`` validates payloads with
    :class:`PassthroughPayload` (``extra="allow"``), a payload the
    actor's real model would reject passes the in-memory test and fails
    only in production, where the worker validates against the declared
    model on every dispatch.

    Remedies, most faithful first: pass the :class:`~taskq.actor.ActorRef`
    itself (``register_stub(my_actor, ...)``) so the declared model is
    resolved automatically; pass ``payload_type=MyPayload`` explicitly; or
    pass ``payload_type=PassthroughPayload`` to keep the permissive
    behaviour deliberately and silence this warning.
    """


@dataclass(frozen=True, slots=True)
class _InMemoryActorConfig:
    """Minimal frozen dataclass satisfying ActorConfigLike.

    Stored alongside stub functions so ``run_until_drained`` can build
    the ``ActorConfigLike`` the classifier needs without a concrete
    ActorConfig class (which lands with the @actor decorator).
    """

    retry: RetryPolicy
    # Why the separate override: an omitted ``retry=`` keeps the historical
    # RetryPolicy(jitter=0.0) default for backoff classification and must
    # NOT restamp rows (a ref-declared budget stands), while an explicit
    # ``retry=`` is the test's declared budget for the actor and stamps
    # max_attempts/retry fields onto every row dispatched for it, exactly
    # like an ActorRef's retry stamps rows at enqueue time.
    non_retryable_exceptions: tuple[type[BaseException], ...] = ()
    retry_classifier: RetryClassifierHook | None = None
    on_retry_exhausted: OnRetryExhausted | None = None
    on_retry_exhausted_timeout: float = 3.0
    on_success: OnSuccess | None = None
    on_success_timeout: float = 3.0
    on_cancel: OnCancel | None = None
    on_cancel_timeout: float = 3.0
    result_ttl: timedelta | None = None
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
    backend: "InMemoryBackend",
) -> Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]]:
    """Return a ``run_actor`` callback for ``consume_one_job`` that wraps
    *stub* and builds a :class:`_StubContext` from the job row.

    Binding *stub* as a function parameter avoids Ruff B023 (loop-variable
    capture) because the closure captures the already-bound parameter,
    not the loop variable in ``run_until_drained``.

    The sub-job enqueuer and the bound logger are wired to the runner's
    own backend, one enqueuer per attempt (the same shape the production
    consumer builds), so an actor calling ``ctx.jobs.enqueue(...)`` or
    ``ctx.log.info(...)`` under ``run_until_drained`` exercises the real
    surfaces instead of failing with a bare ``AttributeError``.
    """
    # Why a function-level import: taskq.client pulls the wider client
    # package; keeping it out of this module's import graph preserves the
    # driver-free taskq.testing import boundary (pinned by
    # test_testing_no_transitive_asyncpg).
    from taskq.client._enqueuer import SubJobEnqueuer

    # Why a marker, not None: SubJobEnqueuer only None-checks
    # worker_pool, it is the gate on the enqueuer's autonomous fallback
    # arm, which writes through the backend and never dereferences the
    # pool. The in-memory backend has no asyncpg pool by design; without
    # the marker every ctx.jobs.enqueue would raise "ctx.jobs is only
    # available inside an actor body" under the runner.
    autonomous_pool_marker: Any = object()
    sub_job_enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=autonomous_pool_marker,
        backend=backend,
    )

    async def run_actor(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:  # pyright: ignore[reportUnusedParameter]  # Why: _build_run_actor receives the production JobContext from consume_one_job but passes a duck-typed _StubContext to the stub; the ctx parameter is unused here.
        stub_ctx = _StubContext(
            job_id=job_row.id,
            attempt=job_row.attempt,
            payload=job_row.payload,
            cancel_event=cancel_events.get(job_row.id),
            snooze_count=job_row.snooze_count,
            jobs=sub_job_enqueuer,
            log=bind_job_context(
                structlog.get_logger("taskq.testing.stub_context"),
                job_id=job_row.id,
                actor=job_row.actor,
                queue=job_row.queue,
                attempt=job_row.attempt,
                identity_key=None,
                trace_id="",
            ),
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
    actor_name: "str | ActorRef[Any, Any]",
    fn: StubFn,
    *,
    retry: RetryPolicy | None = None,
    non_retryable_exceptions: tuple[type[BaseException], ...] = (),
    retry_classifier: RetryClassifierHook | None = None,
    on_retry_exhausted: OnRetryExhausted | None = None,
    on_retry_exhausted_timeout: float = 3.0,
    on_success: OnSuccess | None = None,
    on_success_timeout: float = 3.0,
    on_cancel: OnCancel | None = None,
    on_cancel_timeout: float = 3.0,
    result_ttl: timedelta | None = None,
    payload_type: type[BaseModel] | None = None,
) -> None:
    """Record a stub function for *actor_name*. Re-registration overwrites
    (test ergonomics). Stubs MAY be ``async def`` or plain ``def``;
    ``run_until_drained`` inspects the return value with
    ``isinstance(result, Awaitable)`` and awaits accordingly.

    *actor_name* is the actor's registered name or its
    :class:`~taskq.actor.ActorRef`. Passing the ref is the fidelity-safe
    form: the runner then knows the actor's declared contract and
    auto-resolves ``payload_type`` from it (below).

    The stub receives ``(payload, ctx)`` where *ctx* is a minimal object
    with ``job_id: JobId``, ``attempt: int``, ``payload: dict``, and
    ``cancel_event: asyncio.Event | None``.

    ``payload_type`` is the Pydantic model the consumer validates the
    raw row payload against before invoking the stub, the same
    validation a production worker runs on every dispatch. Resolution
    order when omitted: the ActorRef's declared ``payload_type`` when the
    actor was passed as a ref; otherwise :class:`PassthroughPayload`
    (``extra="allow"``) with a :class:`StubPayloadTypeWarning`, because a
    bare name leaves the runner unable to see the actor's real model and
    the permissive default accepts payload shapes that model would
    reject, a green in-memory test over code a worker would refuse.
    Pass ``payload_type=PassthroughPayload`` explicitly to opt into the
    permissive behaviour deliberately, without the warning.

    Actor config fields (retry, non_retryable_exceptions,
    retry_classifier, on_retry_exhausted, on_retry_exhausted_timeout,
    on_success, on_success_timeout, on_cancel, on_cancel_timeout,
    result_ttl) are stored alongside the stub and used by
    ``run_until_drained`` when calling ``decide_after_failure`` and the
    terminal writes. ``result_ttl`` is the worker-side literal passed as
    the terminal write's ``fallback_result_ttl`` (applied when no stored
    override exists).

    An explicit ``retry=`` is the test's declared retry budget for the
    actor and stamps ``max_attempts`` and the retry scalars onto every
    row enqueued or dispatched for it, exactly like an ActorRef's own
    retry policy stamps rows at enqueue time; the twin's exhaustion
    check reads the row, so the stub budget is the budget the job runs
    under. Omitting ``retry=`` keeps the historical
    ``RetryPolicy(jitter=0.0)`` default for backoff classification and
    leaves the row's enqueue-time stamp alone: a stub registered via the
    ActorRef keeps the actor's declared budget (an actor declaring
    ``max_attempts=5`` runs 5 attempts, not the default 3), and a bare
    name keeps whatever the row was enqueued with.
    The default ``RetryPolicy(jitter=0.0)`` matches the historical inline
    ``5 * 2^(attempt-1)`` backoff formula exactly, preserving existing
    test behaviour.
    """
    # Why the Any-parameterized ref: ActorRef is invariant in its payload
    # and result type parameters, so no narrower parameterization accepts
    # every concrete ref, the same spell worker/run.py uses for its
    # registry mapping. Only ``name``/``payload_type`` are read. The
    # discriminant is isinstance(str) (not isinstance(ActorRef)) so this
    # module never imports taskq.actor at runtime, that import would
    # pull asyncpg into the driver-free testing boundary.
    actor_ref: ActorRef[Any, Any] | None = None
    if isinstance(actor_name, str):
        name = actor_name
    else:
        actor_ref = actor_name
        name = actor_ref.name

    if payload_type is not None:
        # Explicit always wins, including PassthroughPayload, which is
        # then the deliberate, warning-free escape hatch.
        resolved_payload_type = payload_type
    elif actor_ref is not None:
        resolved_payload_type = actor_ref.payload_type
    else:
        resolved_payload_type = PassthroughPayload
        # stacklevel: warn → this function → the InMemoryBackend
        # delegate method → the caller whose line should be reported.
        warnings.warn(
            StubPayloadTypeWarning(
                f"register_stub({name!r}, ...) without payload_type= cannot "
                "resolve the actor's declared payload model from a bare "
                "name; falling back to PassthroughPayload, which accepts "
                "payload shapes the actor's real model would reject (a "
                "green in-memory test over code a production worker would "
                "refuse). Pass the ActorRef, register_stub(my_actor, ...) "
                ", or payload_type=MyPayload to validate against the "
                "declared model, or payload_type=PassthroughPayload to opt "
                "into the permissive default deliberately."
            ),
            stacklevel=3,
        )
        logger.warning(
            "stub-payload-type-unresolved",
            actor=name,
            fallback="PassthroughPayload",
        )

    backend._actor_stubs[name] = fn  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
    backend._actor_configs[name] = _InMemoryActorConfig(  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        retry=retry if retry is not None else RetryPolicy(jitter=0.0),
        non_retryable_exceptions=non_retryable_exceptions,
        retry_classifier=retry_classifier,
        on_retry_exhausted=on_retry_exhausted,
        on_retry_exhausted_timeout=on_retry_exhausted_timeout,
        on_success=on_success,
        on_success_timeout=on_success_timeout,
        on_cancel=on_cancel,
        on_cancel_timeout=on_cancel_timeout,
        result_ttl=result_ttl,
        payload_type=resolved_payload_type,
    )
    # Ensure the stub-registered actor can dispatch ,
    # the dispatch gate requires _actor_configs_meta entries
    # when any actor_config is registered.
    if name not in backend._actor_configs_meta:  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        backend._actor_configs_meta[name] = ActorConfig(  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            actor=name, max_concurrent=None, queue="default"
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
    max_pending: int | None = None,
    queue: str = "default",
    metadata: dict[str, object] | None = None,
) -> None:
    """Register a single actor configuration for dispatch simulation.

    Builds an ``ActorConfig`` from keyword arguments and stores it
    in ``_actor_configs_meta``.  ``max_concurrent`` is read by dispatch;
    ``max_pending`` is read by the client-side capacity cache
    (:meth:`InMemoryBackend.get_actor_max_pending`), registering here is
    the in-memory analog of ``taskq actor-config set``. ``queue`` and
    ``metadata`` are stored for future use.
    """
    backend._actor_configs_meta[actor] = ActorConfig(  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        actor=actor,
        max_concurrent=max_concurrent,
        max_pending=max_pending,
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
    """Return events for *job_id* (test-only accessor).

    Isolated copies, like every other read seam, a mutated read result
    must never reach ``_events`` storage.
    """
    return [_event_read_copy(e) for e in backend._events if e.job_id == job_id]  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.


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
    are considered, allowing the caller to simulate per-status
    retention by making separate calls per status group (matching
    the PG ``prune_terminal_jobs`` per-status CTE pattern).

    NOT on the Backend Protocol, the leader calls this as a
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
        if row.idempotency_key is not None:
            # The archive mirror of the PG prune's archive-move DELETE: the
            # live row is gone, so the pair is gone from the live unique
            # index and the key is FREE again (the documented horizon,
            # "dedupes until pruned"). The twin's index must drop the entry
            # with the row, the same guard _withdraw_inserted's rollback pop
            # applies: only when the entry still points at THIS row. A
            # single enqueue reusing the pair between the pop candidates and
            # here has rewritten the index to ITS row (PG: the new row holds
            # the pair, the archive never touched it), and its dedup must
            # survive. Without the pop a phantom entry pins the pair FOREVER
            # in the bulk tiers: enqueue_batch_fast refuses the freed key
            # with a DuplicateIdempotencyKeyError Postgres' COPY never
            # raises, and _batch_cap_refusals discounts a real item against
            # a row that does not exist, over-admitting past max_pending.
            pair = (row.idempotency_scope, row.idempotency_key)
            if backend._idempotency_index.get(pair) == job_id:  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]
                backend._idempotency_index.pop(pair)  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]
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

    NOT on the Backend Protocol, the leader calls this as a
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
    ``_jobs`` but present in ``_archive`` is retrievable. The returned
    wrapper carries an isolated copy of the row (every read seam
    severs aliasing), so mutating it cannot corrupt the archive.
    """
    archived = backend._archive.get(job_id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
    if archived is None:
        return None
    return _ArchivedJobRow(
        row=_read_copy(archived.row),
        archived_at=archived.archived_at,
        expire_at=archived.expire_at,
    )


# ── Cancel polling simulation ──────────────────────────────────────────


async def tick_cancel_polling(backend: "InMemoryBackend") -> None:
    """Simulate the heartbeat's cancel-poll-and-escalate step.

    Iterates jobs where ``cancel_requested_at IS NOT NULL AND
    status == "running"``.  On first observation of ``cancel_phase == 1``,
    records ``_cancel_observed_at[job_id] = clock.now()`` and fires the
    per-job cancel event (registered via ``register_cancel_event``).
    Subsequent calls escalate ``cancel_phase = 2`` if the cancellation
    grace period has elapsed, or mark ``abandoned`` if the cleanup
    grace period has also elapsed, and, when the abandoned job's
    attempt is executing under ``run_until_drained``, cancel that
    attempt's task: the runner's mirror of production phase 2's hard
    cancel of a non-cooperative attempt.

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
            # Escalate to phase 2, delegate to write_cancel_escalation
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
            abandoned = await backend.mark_abandoned(job_id)
            # Production phase 2 terminates a non-cooperative attempt by
            # hard-cancelling the actor task once the graces elapse
            # (cancel.py's active.task.cancel()); the cooperative event
            # alone cannot reach an attempt that never reads it, and the
            # runner awaits attempts inline, so without this cancel the
            # drain parks forever beside a row that already says
            # abandoned.  The row is terminal BEFORE the cancel: the
            # attempt's mark_cancelled path then no-ops against the
            # abandoned row instead of racing a second terminal write,
            # and the drain task's own cancellation propagates through
            # run_until_drained's caller-cancel arm (Task.cancelling()).
            if abandoned:
                inflight = backend._inflight_attempt  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                if inflight is not None and inflight[0] == job_id:
                    inflight[1].cancel()

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


def _undrainable_jobs(backend: "InMemoryBackend") -> dict[str, list[JobRow]]:
    """Non-terminal jobs whose actor has no registered dispatch capacity.

    Dispatch candidates come FROM the ``_actor_configs_meta`` registry
    (``_dispatch._dispatch_batch``: zero registered actors means zero
    capacity rows means zero candidates), so an actor with no entry can
    never be claimed, the strictly-worse silent twin of the
    dispatched-but-stubless case ``run_until_drained`` already raises on.

    A registered-but-denied job (a saturated rate limit or reservation)
    is never returned here: its denial required a dispatch attempt, and a
    dispatch attempt requires the registry entry, so this predicate
    cannot false-positive on the denial-starvation guard's jobs.
    """
    stranded: dict[str, list[JobRow]] = {}
    for row in backend._jobs.values():  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if row.status in TERMINAL_STATUSES:
            continue
        if row.actor not in backend._actor_configs_meta:  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            stranded.setdefault(row.actor, []).append(row)
    return stranded


def _raise_for_undrainable_work(backend: "InMemoryBackend") -> None:
    """Fail the drain loudly when remaining work can never be dispatched.

    Called at every ``run_until_drained`` exit point: "nothing scheduled"
    (or "every scheduled job starved", or "real clock, cannot advance")
    reads as *drained* only when nothing left behind is undispatchable.
    Otherwise a job whose actor was never registered sits ``pending``
    forever while the drain reports success, and a caller in
    ``handle.wait()`` polls it without a deadline, unable to tell "will
    never run" apart from "ran to completion".
    """
    stranded = _undrainable_jobs(backend)
    if not stranded:
        return
    detail = "; ".join(
        f"{actor}: {len(rows)} job(s) ({', '.join(sorted({r.status for r in rows}))})"
        for actor, rows in sorted(stranded.items())
    )
    # Same "no stub registered for actor" contract as the
    # dispatched-but-stubless raise below, so both missing-registration
    # failures match one pattern.
    raise RuntimeError(
        f"no stub registered for actor: {', '.join(sorted(stranded))}, "
        f"run_until_drained cannot end drained with work nothing can "
        f"dispatch ({detail}). Register the actor with register_stub() or "
        f"register_actor_config() before draining; without a registry "
        f"entry dispatch grants the actor zero capacity and these jobs "
        f"can never run."
    )


def _every_scheduled_job_starved(backend: "InMemoryBackend", starved: "set[JobId]") -> bool:
    """True when every scheduled job was denied again at or after the
    reschedule point its own previous denial set.

    A first denial only proves "no capacity right now", the reschedule
    point it produces is the limiter's own Retry-After promise, and a
    limiter that refills by then must get its chance: the drain advances
    the clock to that point and re-claims. A job denied AGAIN at or after
    the point has exhausted the promise, admission only ever answers
    "no" for it, so advancing the clock further cannot drain it. This
    separates "waiting for the clock", which draining should advance
    through, from "waiting for capacity this test never grants", which
    it cannot.
    """
    scheduled = [
        r
        for r in backend._jobs.values()  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
        if r.status == "scheduled"
    ]
    return bool(scheduled) and all(r.id in starved for r in scheduled)


async def _cancel_polling_loop(backend: "InMemoryBackend") -> None:
    """Tick cancel polling until cancelled, for drains that opt in.

    Runs as a concurrent task beside the drain: the drain awaits each
    attempt inline, so ticks between iterations alone can never observe a
    cancel request raised against the attempt in flight (only a tick
    running WHILE the attempt executes can fire its cooperative cancel
    event or escalate the phases). The cadence is coarse real time and
    only affects when ticks happen; the escalation timing itself is
    arbitrated by the backend's clock, which the test advances.
    """
    while True:
        await tick_cancel_polling(backend)
        await asyncio.sleep(0.01)


async def run_until_drained(backend: "InMemoryBackend", *, cancel_polling: bool = False) -> None:
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
    5. Terminates when: no pending, no running, no scheduled-due jobs ,
       or when every remaining scheduled job is starved (denied again at
       or after the reschedule point its own previous denial set; see the
       ``starved`` bookkeeping below). Termination is checked for
       undispatchable work first: a non-terminal job whose actor has no
       registered stub/config can never be claimed (dispatch candidates
       come from the actor registry), so ending "drained" beside one
       would report success over a job that will never run, the loop
       raises ``RuntimeError`` instead, the same contract as a
       dispatched job with no stub.

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

    ``cancel_polling``: opt-in. When set, the drain drives
    ``tick_cancel_polling`` itself, both between iterations and, while an
    attempt is in flight, from a concurrent ticker task; a test can then
    cancel a mid-drain job (``write_cancel_request`` from the test task
    or from another stub) and watch it end ``cancelled`` without calling
    the tick manually. Default off keeps the historical behaviour: the
    drain never ticks, and tests drive cancellation entirely by hand.
    """
    from taskq.worker._consumer import consume_one_job

    # An admission denial reschedules the job indefinitely, that is the
    # 429 contract, and nothing about the job's own state ever ends the
    # loop. The drain would then spin forever against a limiter that is
    # saturated for the whole test, advancing the FakeClock one deferral
    # at a time. Draining means "run what can run", so a job that only
    # ever gets denied is drained as far as it can go, but a single
    # denial cannot prove that: the reschedule point the denial sets is
    # the limiter's own Retry-After promise, and a limiter that refills
    # by then must get its chance. The drain therefore trusts the promise
    # once per job (advance to the point, re-claim) and counts a job as
    # starved only when it is denied AGAIN at or after that point; once
    # every remaining scheduled job is starved, no clock advance can be
    # proven to help, and the drain stops.
    denied_reschedule: dict[JobId, datetime] = {}
    starved: set[JobId] = set()

    # The concurrent ticker is what makes a mid-drain cancellation
    # reachable at all: the drain awaits each attempt inline, so a tick
    # squeezed between iterations never runs while an attempt is in
    # flight, and only a tick running BESIDE the attempt can observe a
    # cancel request (fire the cooperative event, escalate the phases).
    # The cadence is coarse real time; escalation timing is arbitrated
    # by the backend's own clock (FakeClock advances from the test).
    poller: asyncio.Task[None] | None = None
    if cancel_polling:
        poller = asyncio.create_task(_cancel_polling_loop(backend))
    try:
        while True:
            if cancel_polling:
                # Between-iteration tick: covers rows left running by a
                # previous attempt, deterministically, without waiting
                # for the ticker's next wake.
                await tick_cancel_polling(backend)

            # Step 1: promote scheduled→pending (the backend's own clock is
            # the arbiter, no caller-supplied now).
            await backend.scheduled_to_pending()

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
                    # No scheduled jobs at all, fully drained, unless what
                    # remains can never be dispatched (never-registered actor).
                    _raise_for_undrainable_work(backend)
                    return
                if _every_scheduled_job_starved(backend, starved):
                    _raise_for_undrainable_work(backend)
                    return

                # Advance clock if FakeClock, else return.
                move_to = getattr(backend._clock, "move_to", None)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                if callable(move_to):
                    move_to(next_at)
                    continue
                else:
                    # Production code wouldn't call run_until_drained
                    _raise_for_undrainable_work(backend)
                    return

            # Step 4: delegate per-job execution to consume_one_job
            job = dispatched[0]
            stub = backend._actor_stubs.get(job.actor)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            if stub is None:
                raise RuntimeError(f"no stub registered for actor: {job.actor}")

            actor_cfg = backend._actor_configs.get(job.actor)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            if actor_cfg is None:
                actor_cfg = _InMemoryActorConfig(retry=RetryPolicy(jitter=0.0))

            # The row's retry budget is a bound stamped by the ENQUEUING
            # call (the client stamps it from the ActorRef): no stub
            # registration may move it, before or after the enqueue. A
            # stub's retry= configures the stub's own retry
            # classification only.

            # PayloadValidationError escapes consume_one_job for two pre-actor
            # reasons, both OUTSIDE its actor-body try/except: (1) the fallback
            # validate_actor_payload call runs before rate-limit acquisition, so
            # an invalid payload raises before the actor body (and before any
            # token is consumed); (2) acquire_for_actor re-validates the payload
            # against a keyed ref's own payload_type and raises on a cross-model
            # mismatch. The production dispatch_one_job catches such escapes via
            # its outer except-Exception and routes through
            # _handle_generic_exception; the test runner has no such wrapper, so
            # we catch it here and transition the job to failed, matching the
            # non-retryable contract documented on PayloadValidationError.
            # The in-flight attempt registration: tick_cancel_polling's
            # both-graces arm cancels this task to terminate a
            # non-cooperative attempt (the mirror of production's
            # active.task.cancel()). Keyed by job id so only the abandon of
            # the job actually executing can cancel the drain. The cancel
            # count at registration is this dispatch's baseline: the
            # CancelledError classification below reads every elevation
            # relative to it, the way asyncio.timeout compares against the
            # cancelling() count it captured at __aenter__.
            current_task = asyncio.current_task()
            registration: tuple[JobId, asyncio.Task[object]] | None = None
            baseline_cancelling = 0
            if current_task is not None:
                registration = (job.id, current_task)
                baseline_cancelling = current_task.cancelling()
                backend._inflight_attempt = registration  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            try:
                outcome = await consume_one_job(
                    backend,
                    job,
                    backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                    run_actor=_build_run_actor(stub, backend._cancel_events, backend),  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                    actor_config=actor_cfg,
                    payload_type=actor_cfg.payload_type,
                    clock=backend._clock,  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                    fallback_result_ttl=actor_cfg.result_ttl,
                )
            except PayloadValidationError as exc:
                error_info = ErrorInfo(
                    error_class="PayloadValidationError",
                    error_message=str(exc),
                    error_traceback="".join(
                        traceback.format_exception(type(exc), exc, exc.__traceback__)
                    ),
                )
                await backend.mark_failed_or_retry(
                    job_id=job.id,
                    worker_id=backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                    error_info=error_info,
                    retry_delay=None,
                    attempt=job.attempt,
                    claim_epoch=job.claim_epoch,
                )
                # Production's generic-exception escape routes this failure
                # through _handle_generic_exception and applies the batch
                # hook with the handler's terminal outcome before returning
                # , a batch completes when any member reaches terminal status
                # , so the mirror sets the failed outcome and falls through
                # to the shared hook call below.
                outcome = "failed"
            except asyncio.CancelledError:
                # Three cancellation origins reach this boundary. Production
                # separates them by construction, its dispatch loop and its
                # per-attempt tasks are distinct, while the runner awaits
                # every attempt inline in the drain task, so the origins must
                # be told apart here by state, not by where the raise surfaced:
                #
                # 1. Actor-originated: the documented check_cancelled() style,
                #    or the actor ending itself with its own
                #    asyncio.CancelledError, the two are indistinguishable
                #    inside consume_one_job, and production treats them
                #    identically (same shielded mark, same re-raise, same
                #    absorption at the worker's task boundary, worker keeps
                #    dispatching). No cancel() was requested on the drain
                #    task, so its cancel count is still at this dispatch's
                #    baseline, absorb and keep draining.
                # 2. Caller-originated: a cancel() requested on the drain task
                #    itself. The count sits ABOVE the baseline and the row was
                #    not abandoned by the escalation tick, the caller's stop
                #    always wins and must propagate, exactly as a production
                #    worker stops when its dispatch task is cancelled, even if
                #    the interrupted job also had a cancel requested.
                # 3. Escalation-originated (the phase-2 force-cancel):
                #    tick_cancel_polling's both-graces arm marks the row
                #    abandoned and only then cancels the inflight attempt ,
                #    which the runner registered as the drain task itself, so
                #    the count is above baseline exactly like origin 2. The
                #    abandoned row, written BEFORE the cancel is delivered,
                #    is the record that this cancel is the runner's own. The
                #    runner is both the requester and the consumer of this
                #    cancellation, so asyncio's contract has two halves:
                #    absorb the raise (production cancels only the offending
                #    attempt task and its dispatch loop keeps claiming, so the
                #    drain continues), and balance the tick's cancel() with
                #    one uncancel(), the bookkeeping asyncio.timeout and
                #    TaskGroup do for every cancel they inject, so an elevated
                #    cancelling() count does not follow the caller's task past
                #    the drain. If a caller cancel landed ON TOP of the
                #    force-cancel, the count is still above baseline after the
                #    balancing uncancel, the caller's stop wins and the raise
                #    propagates. The outcome is "cancelled", matching
                #    production's CancelledError escape hook; the row is
                #    already terminal abandoned, so this arm issues no second
                #    terminal write and the shared hook call below applies the
                #    outcome (a batch completes on any terminal member).
                row_after = backend._jobs.get(job.id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                if (
                    current_task is not None
                    and row_after is not None
                    and row_after.status == "abandoned"
                    and current_task.cancelling() > baseline_cancelling
                    and current_task.uncancel() <= baseline_cancelling
                ):
                    outcome = "cancelled"
                elif current_task is not None and current_task.cancelling() > baseline_cancelling:
                    raise
                else:
                    outcome = "cancelled"
            finally:
                # Identity-guarded: a concurrent run_until_drained on the same
                # backend may have registered its own attempt over ours, only
                # clear what this dispatch registered.
                if registration is not None and backend._inflight_attempt is registration:  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
                    backend._inflight_attempt = None  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.

            # A denial leaves the row's denial counter one higher and nothing
            # else advanced; any other outcome is forward progress and clears
            # the whole map, so one job finally getting admitted re-opens the
            # drain for every job still waiting.
            after = backend._jobs.get(job.id)  # pyright: ignore[reportPrivateUsage]  # Why: test runner helper intentionally accesses private InMemoryBackend state; this module is co-located with the backend and owns this access pattern.
            if after is not None and after.rate_limit_blocked_count > job.rate_limit_blocked_count:
                previous_point = denied_reschedule.get(job.id)
                if previous_point is not None and backend._clock.now() >= previous_point:  # pyright: ignore[reportPrivateUsage]  # Why: same runner-helper access pattern as above, the denial timestamp lives on the backend's clock.
                    # Denied again at/after the reschedule point the previous
                    # denial itself set: the limiter's own Retry-After promise
                    # was honored once and failed, so no further clock advance
                    # is provably useful for this job.
                    starved.add(job.id)
                denied_reschedule[job.id] = after.scheduled_at
            else:
                denied_reschedule.clear()
                starved.clear()

            try:
                await apply_batch_terminal_outcome(backend, job, outcome)
            except Exception:
                logger.exception("batch-policy-hook-failed", job_id=str(job.id))
    finally:
        # Stop the ticker on every exit path before returning or
        # propagating; suppressing the ticker's own CancelledError keeps
        # the drain's in-flight exception (if any) intact.
        if poller is not None:
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller


# ── In-memory wait_for_batch simulation ─────────────────────────────────

_min_snooze = timedelta(seconds=1)

_wfb_logger = structlog.get_logger("taskq.testing.in_memory")


async def wait_for_batch(
    backend: "InMemoryBackend",
    batch_id: UUID,
    *,
    snooze_interval: timedelta = timedelta(seconds=10),
    snooze_via_exception: bool = True,
    expect_at_least: int | None = None,
    on_empty: Literal["error", "ok"] = "error",
    exclude_job_id: UUID | None = None,
) -> BatchCompletionStatus:
    """In-memory simulation of :func:`taskq.batch.wait_for_batch`.

    Scans ``backend._jobs`` for rows matching ``batch_id`` and computes
    :class:`~taskq.batch.BatchCompletionStatus` using the same
    terminal-status set as the PG path.

    Mirrors both snooze modes of the PG variant:
    ``snooze_via_exception=True`` (the default) raises
    :class:`~taskq.exceptions.Snooze` while members are in flight;
    ``False`` blocks on ``asyncio.sleep(snooze_interval)`` and rescans
    until every member is terminal. The sleep is real event-loop time ,
    the injected clock drives row timestamps, not the loop clock, the
    same division the PG path has (PG's clock_timestamp vs the loop) ,
    so blocking-mode tests advance the batch from a concurrent task.
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

    while True:
        batch_row = backend._batches.get(batch_id)  # pyright: ignore[reportPrivateUsage]  # Why: co-located helper accessing private batch store

        exclusion_id = exclude_job_id
        if exclusion_id is None and batch_row is not None:
            exclusion_id = batch_row.finalizer_job_id

        matched = [
            r
            for r in backend._jobs.values()  # pyright: ignore[reportPrivateUsage]  # Why: wait_for_batch is a co-located module-level helper that requires access to the private job store; same pattern as list_jobs
            if r.metadata.get("batch_id") == batch_id_str
            and (exclusion_id is None or r.id != exclusion_id)
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

        status = decide_batch_status(
            batch_id=batch_id,
            batch_row=batch_row,
            status=status,
            snooze_interval=snooze_interval,
            expect_at_least=expect_at_least,
            on_empty=on_empty,
            snooze_via_exception=snooze_via_exception,
        )

        # Members in flight: raise (exception mode, the consumer
        # reschedules the caller) or block and rescan (blocking mode),
        # the two arms of the PG poll loop.
        if status.pending > 0:
            if snooze_via_exception:
                raise Snooze(snooze_interval)
            await asyncio.sleep(snooze_interval.total_seconds())
            continue

        return status
