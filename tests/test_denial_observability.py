"""Denial observability and the non-consuming budget gate, unit-level.

Three properties of the reservation-denial path, pinned without a
database:

1. **Every** ``ReservationUnavailable`` a worker fields is counted on
   the ``taskq.reservation.denials`` counter, labeled by source only
   (``reservation`` / ``rate_limit``) — admission denials are a
   first-class operational signal, and the bucket name is deliberately
   not a dimension (caller-controlled cardinality; see
   ``obs/_otel.py``'s recorder).
2. A snooze-path terminal write that matches nothing (job already
   moved) surfaces as ``"noop"`` from the handler — a noop is a noop,
   not a reschedule; the pre-actor denial path propagates it instead of
   hardcoding ``"scheduled"``.
3. The non-consuming snooze arms enforce the retry budget:
   ``retry_kind`` is enum-complete in every arm — a ``non_retryable``
   job at ``attempt >= max_attempts`` with no ``schedule_to_close``
   must reach a terminal ``MaxAttemptsExceeded`` exit through BOTH
   ``mark_snoozed`` (the denial path) and
   ``mark_retry_after(consume_budget=True)`` (whose exhaustion arm
   previously matched only ``retry_kind = 'transient'`` and left
   ``non_retryable`` falling through to a reschedule), while a job
   carrying a future ``schedule_to_close`` keeps rescheduling until its
   deadline — the close deadline is that job's own terminal exit.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import structlog
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint
from opentelemetry.trace import INVALID_SPAN_CONTEXT, NonRecordingSpan

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.exceptions import ReservationUnavailable
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._handlers import (  # pyright: ignore[reportPrivateUsage]  # Why: the handler is the unit under test; no public seam routes a denial without a full consumer stack.
    _handle_reservation_class_denied,
)

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_DELAY = timedelta(seconds=30)


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation.

    Patches ``obs._otel.get_meter`` onto a fresh MeterProvider backed by an
    ``InMemoryMetricReader``, so instruments created lazily by the code
    under test land in this reader.  Mirrors ``tests/test_obs.py`` and
    ``tests/test_silent_failure_guards.py``.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # pyright: ignore[reportPrivateUsage]  # Why: mirrors tests/test_obs.py's otel_reader fixture, which reads the same private version helper.

    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: new_meter)
    otel_mod.set_otel_enabled(True)
    return reader


async def _mem_job(
    *,
    max_attempts: int = 1,
    retry_kind: str = "non_retryable",
    schedule_to_close: datetime | None = None,
    running: bool = True,
) -> tuple[InMemoryBackend, JobId, UUID]:
    """Enqueue (and optionally dispatch) one job on the in-memory twin."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    args = EnqueueArgs(
        id=new_job_id(),
        actor="denial_actor",
        queue="default",
        payload={},
        max_attempts=max_attempts,
        retry_kind=retry_kind,  # type: ignore[arg-type]  # Why: str literal at runtime satisfies RetryKind; the helper keeps the call sites terse.
        scheduled_at=_NOW - timedelta(seconds=1) if running else _NOW + timedelta(hours=1),
        schedule_to_close=schedule_to_close,
    )
    await backend.enqueue(args)
    if not running:
        return backend, args.id, new_uuid()
    worker_id = new_uuid()
    dispatched = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(dispatched) == 1
    return backend, args.id, worker_id


def _point_by_source(points: list[NumberDataPoint], source: str) -> NumberDataPoint:
    matches = [p for p in points if dict(p.attributes or {}).get("source") == source]
    assert len(matches) == 1, f"expected exactly one {source!r} data point, got {matches}"
    return matches[0]


# ── 1. denial counter, labeled by source ────────────────────────────────


async def test_denial_handler_counts_reservation_denial_by_source(
    otel_reader: InMemoryMetricReader,
) -> None:
    """A fielded denial bumps ``taskq.reservation.denials`` once, with the
    source as the only dimension."""
    backend, job_id, worker_id = await _mem_job(max_attempts=10, retry_kind="transient")
    job = await backend.get(job_id)
    assert job is not None

    result = await _handle_reservation_class_denied(
        backend,
        job,
        worker_id,
        ReservationUnavailable("gpu_pool", _DELAY, source="reservation"),
        NonRecordingSpan(INVALID_SPAN_CONTEXT),
        structlog.get_logger("taskq.test"),
        StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        awaiting_prefix="reservation:",
        outcome="reservation_denied",
        debug_event="consume-reservation-denied-noop",
    )
    assert result == "scheduled"

    from taskq.testing.otel import counter_data_points

    points = counter_data_points(otel_reader, "taskq.reservation.denials")
    assert len(points) == 1, (
        f"expected one taskq.reservation.denials data point, got {points!r} — "
        "every ReservationUnavailable a worker fields must be counted; the "
        "denial is the operational signal that a bucket is saturated."
    )
    assert _point_by_source(points, "reservation").value == 1


async def test_denial_handler_counts_rate_limit_denial_by_source(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The rate_limit source lands on its own labeled series of the same
    counter — both denial classes a worker fields are counted, and the
    source label is the only dimension."""
    backend, job_id, worker_id = await _mem_job(max_attempts=10, retry_kind="transient")
    job = await backend.get(job_id)
    assert job is not None

    result = await _handle_reservation_class_denied(
        backend,
        job,
        worker_id,
        ReservationUnavailable("rl_bucket", _DELAY, source="rate_limit"),
        NonRecordingSpan(INVALID_SPAN_CONTEXT),
        structlog.get_logger("taskq.test"),
        StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        awaiting_prefix="rate_limit:",
        outcome="rate_limit_denied",
        debug_event="consume-rate-limit-denied-noop",
    )
    assert result == "scheduled"

    from taskq.testing.otel import counter_data_points

    points = counter_data_points(otel_reader, "taskq.reservation.denials")
    assert len(points) == 1
    assert _point_by_source(points, "rate_limit").value == 1


# ── 2. the noop outcome surfaces as "noop" ──────────────────────────────


async def test_denial_handler_noop_returns_noop() -> None:
    """A snooze write that matches nothing (the job is not running-owned)
    is a noop, and the handler must report it as ``"noop"`` — not dress
    it up as a reschedule."""
    backend, job_id, _worker_id = await _mem_job(running=False)
    job = await backend.get(job_id)
    assert job is not None
    assert job.status != "running"

    result = await _handle_reservation_class_denied(
        backend,
        job,
        new_uuid(),
        ReservationUnavailable("gpu_pool", _DELAY),
        NonRecordingSpan(INVALID_SPAN_CONTEXT),
        structlog.get_logger("taskq.test"),
        StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        awaiting_prefix="reservation:",
        outcome="reservation_denied",
        debug_event="consume-reservation-denied-noop",
    )
    assert result == "noop"


# ── 3. the non-consuming arms enforce the retry budget ──────────────────


async def test_denial_on_non_retryable_at_budget_fails_max_attempts() -> None:
    """A ``non_retryable`` job at ``attempt >= max_attempts`` with no
    ``schedule_to_close`` has no remaining exit except the terminal one:
    the denial path must fail it with ``MaxAttemptsExceeded`` instead of
    rescheduling it forever."""
    backend, job_id, worker_id = await _mem_job(max_attempts=1, retry_kind="non_retryable")

    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
    )
    assert result == "failed:MaxAttemptsExceeded"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "MaxAttemptsExceeded"
    assert row.error_message == "retry budget exhausted"
    assert row.finished_at is not None


async def test_retry_after_consume_true_on_non_retryable_at_budget_fails() -> None:
    """The consuming arm's exhaustion exit is enum-complete over
    ``retry_kind``: a ``non_retryable`` job at budget under
    ``RetryAfter(consume_budget=True)`` must fail terminally — previously
    the arm matched only ``retry_kind = 'transient'`` and the job fell
    through to a reschedule."""
    backend, job_id, worker_id = await _mem_job(max_attempts=1, retry_kind="non_retryable")

    result = await backend.mark_retry_after(job_id, worker_id, _DELAY, consume_budget=True)
    assert result == "failed:MaxAttemptsExceeded"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "MaxAttemptsExceeded"


async def test_denial_on_job_with_close_deadline_keeps_rescheduling() -> None:
    """The budget guard's carrier disjunct: a job carrying a
    ``schedule_to_close`` keeps rescheduling under denial until the
    deadline itself ends it — the close deadline is that job's own
    terminal exit, and budget exhaustion must not preempt it."""
    backend, job_id, worker_id = await _mem_job(
        max_attempts=1,
        retry_kind="non_retryable",
        schedule_to_close=_NOW + timedelta(hours=1),
    )

    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
    )
    assert result == "scheduled"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.schedule_to_close == _NOW + timedelta(hours=1)


async def test_actor_deferral_is_unbounded_and_never_spends_budget() -> None:
    """The deliberate capability: a job honouring downstream 429s
    (``Snooze`` / ``RetryAfter(consume_budget=False)``) snoozes
    INDEFINITELY — a deferral never spends retry budget, no matter how
    many cycles pass or how small the budget is.

    The budget is preserved by refunding the claim's attempt increment
    (the Oban/River snooze convention), NOT by raising the ceiling: a
    transient job with ``max_attempts=1`` and no close deadline, driven
    through many more deferral cycles than its budget, must still be
    rescheduled with ``attempt`` back at its pre-claim base and
    ``max_attempts`` untouched. A budget-gated deferral arm, or a
    ceiling-raising refund, fails here.
    """
    backend, job_id, worker_id = await _mem_job(
        max_attempts=1,
        retry_kind="transient",
    )
    cycles = 5  # far beyond the 1-attempt budget
    zero_delay = timedelta(0)

    for _ in range(cycles):
        result = await backend.mark_snoozed(
            job_id,
            worker_id,
            zero_delay,
            outcome="snoozed",
        )
        assert result == "scheduled"
        # Re-claim, exactly as the dispatcher does: attempt = attempt + 1.
        worker_id = new_uuid()
        dispatched = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
        assert [r.id for r in dispatched] == [job_id]

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "running"
    # The refund returns the claim's increment every cycle: attempt
    # oscillates 0 -> 1 -> 0 and never walks toward the smallint
    # ceiling, so the deferral can continue indefinitely.
    assert row.attempt == 1  # the current claim's increment, pre-refund
    assert row.max_attempts == 1
    assert row.snooze_count == cycles
    assert row.rate_limit_blocked_count == 0

    # The consume_budget=False arm carries the same contract.
    result = await backend.mark_retry_after(job_id, worker_id, _DELAY, consume_budget=False)
    assert result == "scheduled"
    row = await backend.get(job_id)
    assert row is not None
    assert row.attempt == 0
    assert row.max_attempts == 1
    assert row.snooze_count == cycles + 1
