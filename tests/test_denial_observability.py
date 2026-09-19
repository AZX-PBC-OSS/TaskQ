"""Denial observability and the non-consuming budget gate, unit-level.

Three properties of the reservation-denial path, pinned without a
database:

1. **Every** ``ReservationUnavailable`` a worker fields is counted on
   the ``taskq.reservation.denials`` counter, labeled by source only
   (``reservation`` / ``rate_limit``) - admission denials are a
   first-class operational signal, and the bucket name is deliberately
   not a dimension (caller-controlled cardinality; see
   ``obs/_otel.py``'s recorder).
2. A snooze-path terminal write that matches nothing (job already
   moved) surfaces as ``"noop"`` from the handler - a noop is a noop,
   not a reschedule; the pre-actor denial path propagates it instead of
   hardcoding ``"scheduled"``.
3. An admission denial carries HTTP-429 semantics: it never consumes
   the job's retry budget and never by itself fails the job. A denied
   job is rescheduled indefinitely - whatever its ``retry_kind``,
   whatever its ``max_attempts`` - until capacity frees or its
   ``schedule_to_close`` expires, at which point the ordinary deadline
   path fails it terminally. A rate-limit or reservation
   misconfiguration must not be able to kill work that merely never
   got a slot. The budget-consuming exhaustion arm still governs real
   executions: ``mark_retry_after(consume_budget=True)`` is a retry the
   job spent an attempt on, so it reaches ``MaxAttemptsExceeded``
   enum-completely over ``retry_kind``.
4. A zero-delay non-consuming deferral reschedules at least
   ``MIN_DEFERRAL_INTERVAL`` out, as ``scheduled`` - a deferral can
   never park a job ``pending`` at ``clock_timestamp()`` at the head of
   the dispatch order. If a job sat pending at now, it would monopolise
   the claim/refund hot loop: every dispatch cycle would re-claim it
   instantly, and each refund would land it back at the head. A positive
   delay guards against this saturation edge. A consuming ``RetryAfter``
   keeps its raw delay: an immediate retry is a real execution, bounded
   by the budget it spends.
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
from taskq.constants import MIN_DEFERRAL_INTERVAL
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
    clock: FakeClock | None = None,
) -> tuple[InMemoryBackend, JobId, UUID]:
    """Enqueue (and optionally dispatch) one job on the in-memory twin.

    A caller-supplied *clock* is the same instance the backend holds, so
    the test can drive time (``clock.advance``) the way the leader's
    promotion sweep does between dispatch rounds.
    """
    backend = InMemoryBackend(clock=FakeClock(_NOW) if clock is None else clock)
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement - candidates come FROM the registry).
    backend.register_actor_config(actor="denial_actor")
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
        f"expected one taskq.reservation.denials data point, got {points!r} - "
        "every ReservationUnavailable a worker fields must be counted; the "
        "denial is the operational signal that a bucket is saturated."
    )
    assert _point_by_source(points, "reservation").value == 1


async def test_denial_handler_counts_rate_limit_denial_by_source(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The rate_limit source lands on its own labeled series of the same
    counter - both denial classes a worker fields are counted, and the
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
    is a noop, and the handler must report it as ``"noop"`` - not dress
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


# ── 3. admission denials carry 429 semantics ────────────────────────────


async def test_denial_on_non_retryable_at_budget_reschedules_and_spends_no_budget() -> None:
    """An admission denial is "come back later", not a failure: a
    ``non_retryable`` job at ``attempt >= max_attempts`` with no
    ``schedule_to_close`` is still rescheduled, and its budget is
    returned.

    A denial says nothing about the work - the actor never ran. Letting
    it consume the retry budget or terminalise the job means a saturated
    reservation pool or a mistuned rate limit can kill a job that merely
    never got a slot, which is exactly the failure mode operators cannot
    diagnose from the outside. The attempt increment the claim took is
    refunded, so the job can be denied indefinitely without walking the
    attempt column toward its ceiling.
    """
    backend, job_id, worker_id = await _mem_job(max_attempts=1, retry_kind="non_retryable")

    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
        attempt=1,
    )
    assert result == "scheduled", (
        "an admission denial must reschedule the job, never terminalise it"
    )

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.error_class is None
    assert row.finished_at is None
    assert row.attempt == 0, "the claim's attempt increment is refunded: a denial spends no budget"
    assert row.max_attempts == 1, "the ceiling is a bound, not a counter - a denial never raises it"
    assert row.rate_limit_blocked_count == 1, (
        "contention stays visible through the aggregated denial counter on the job row"
    )


async def test_repeated_denials_never_exhaust_the_budget() -> None:
    """A job denied far more times than its retry budget allows keeps
    being rescheduled, with its attempt column oscillating around its
    pre-claim base rather than walking upward.

    Indefinite rescheduling under backpressure is the whole point of 429
    semantics: capacity frees on its own schedule, and the only clock
    that may end a denied job is its ``schedule_to_close``. A gate that
    terminalised after N denials would make the retry budget - a
    property of how flaky the actor is - silently govern how long a job
    is willing to wait for a slot.
    """
    clock = FakeClock(_NOW)
    backend, job_id, worker_id = await _mem_job(
        max_attempts=1,
        retry_kind="non_retryable",
        clock=clock,
    )
    cycles = 5  # far beyond the 1-attempt budget

    claimed_attempt = 1
    for _ in range(cycles):
        result = await backend.mark_snoozed(
            job_id,
            worker_id,
            _DELAY,
            outcome="rate_limit_denied",
            attempt=claimed_attempt,
        )
        assert result == "scheduled", "no number of denials may terminalise a job"
        clock.advance(_DELAY + timedelta(seconds=1))
        await backend.scheduled_to_pending()
        worker_id = new_uuid()
        dispatched = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
        assert [r.id for r in dispatched] == [job_id]
        claimed_attempt = dispatched[0].attempt

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "running"
    assert row.attempt == 1, "the current claim's increment only - every denial refunded its own"
    assert row.max_attempts == 1
    assert row.rate_limit_blocked_count == cycles, (
        "every denial is counted, so an operator can see the contention the job absorbed"
    )


async def test_denial_past_the_close_deadline_fails_through_the_deadline_path() -> None:
    """The one terminal exit a denied job has is its own
    ``schedule_to_close``: once the next deferral would land past the
    deadline, the job fails as ``DeadlineExceeded``.

    This is the honest terminal reason - the job ran out of time, it did
    not run out of retries - and it keeps the "denials never kill work"
    rule from meaning "denied work is never reaped".
    """
    backend, job_id, worker_id = await _mem_job(
        max_attempts=1,
        retry_kind="non_retryable",
        schedule_to_close=_NOW + timedelta(seconds=1),
    )

    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
        attempt=1,
    )
    assert result == "failed"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded", (
        "a denied job's terminal exit is its schedule-to-close deadline, never MaxAttemptsExceeded"
    )


async def test_retry_after_consume_true_on_non_retryable_at_budget_fails() -> None:
    """The consuming arm's exhaustion exit is enum-complete over
    ``retry_kind``: a ``non_retryable`` job at budget under
    ``RetryAfter(consume_budget=True)`` must fail terminally - previously
    the arm matched only ``retry_kind = 'transient'`` and the job fell
    through to a reschedule."""
    backend, job_id, worker_id = await _mem_job(max_attempts=1, retry_kind="non_retryable")

    result = await backend.mark_retry_after(
        job_id, worker_id, _DELAY, consume_budget=True, attempt=1
    )
    assert result == "failed:MaxAttemptsExceeded"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "MaxAttemptsExceeded"


async def test_denial_on_job_with_close_deadline_keeps_rescheduling() -> None:
    """The budget guard's carrier disjunct: a job carrying a
    ``schedule_to_close`` keeps rescheduling under denial until the
    deadline itself ends it - the close deadline is that job's own
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
        attempt=1,
    )
    assert result == "scheduled"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.schedule_to_close == _NOW + timedelta(hours=1)


async def test_actor_deferral_is_unbounded_and_never_spends_budget() -> None:
    """The deliberate capability: a job honouring downstream 429s
    (``Snooze`` / ``RetryAfter(consume_budget=False)``) snoozes
    INDEFINITELY - a deferral never spends retry budget, no matter how
    many cycles pass or how small the budget is.

    The budget is preserved by refunding the claim's attempt increment,
    NOT by raising the ceiling: a transient job with ``max_attempts=1``
    and no close deadline, driven through many more deferral cycles than
    its budget, must still be rescheduled with ``attempt`` back at its
    pre-claim base and ``max_attempts`` untouched. A budget-gated deferral
    arm, or a ceiling-raising refund, fails here.

    Re-dispatch drives the FakeClock past each deferral and promotes the
    job, exactly as the leader's ``scheduled_to_pending`` sweep does
    between dispatch rounds - a zero-delay deferral is floored at
    ``MIN_DEFERRAL_INTERVAL``, so the loop advances time instead of
    relying on the job sitting at the head of the dispatch order.
    """
    clock = FakeClock(_NOW)
    backend, job_id, worker_id = await _mem_job(
        max_attempts=1,
        retry_kind="transient",
        clock=clock,
    )
    cycles = 5  # far beyond the 1-attempt budget
    zero_delay = timedelta(0)

    for _ in range(cycles):
        result = await backend.mark_snoozed(
            job_id,
            worker_id,
            zero_delay,
            outcome="snoozed",
            attempt=1,
        )
        assert result == "scheduled"
        # Advance past the floored deferral and promote, exactly as the
        # leader sweep does; pre-floor this is a no-op on an already-
        # pending job, post-floor it is the promotion that makes the
        # job dispatchable again.
        clock.advance(MIN_DEFERRAL_INTERVAL)
        await backend.scheduled_to_pending()
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
    result = await backend.mark_retry_after(
        job_id, worker_id, _DELAY, consume_budget=False, attempt=1
    )
    assert result == "scheduled"
    row = await backend.get(job_id)
    assert row is not None
    assert row.attempt == 0
    assert row.max_attempts == 1
    assert row.snooze_count == cycles + 1


# ── 4. a zero-delay deferral is floored, never parked at the head ───────


async def test_zero_delay_deferrals_reschedule_at_least_min_deferral_interval_out() -> None:
    """Every non-consuming deferral shape - ``Snooze``, an admission
    denial whose ``retry_after`` is 0, and
    ``RetryAfter(consume_budget=False)`` - reschedules at least
    ``MIN_DEFERRAL_INTERVAL`` out as ``scheduled``.

    A delay of 0 must not land the job ``pending`` at ``clock_timestamp()``:
    dispatch orders by ``scheduled_at``, so the job would sort first in
    every round and be instantly re-claimable - one claim/refund round
    trip per cycle monopolising a worker slot. A positive delay guards
    against this saturation edge.
    """
    floor = _NOW + MIN_DEFERRAL_INTERVAL

    # Snooze: the actor-requested deferral.
    backend, job_id, worker_id = await _mem_job(max_attempts=10, retry_kind="transient")
    result = await backend.mark_snoozed(job_id, worker_id, timedelta(0), attempt=1)
    assert result == "scheduled"
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.scheduled_at >= floor

    # A denial whose retry_after is 0 - the same snooze arm, same floor.
    backend, job_id, worker_id = await _mem_job(max_attempts=10, retry_kind="transient")
    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        timedelta(0),
        outcome="reservation_denied",
        attempt=1,
    )
    assert result == "scheduled"
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.scheduled_at >= floor

    # RetryAfter(consume_budget=False): the arm's twin.
    backend, job_id, worker_id = await _mem_job(max_attempts=10, retry_kind="transient")
    result = await backend.mark_retry_after(
        job_id, worker_id, timedelta(0), consume_budget=False, attempt=1
    )
    assert result == "scheduled"
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.scheduled_at >= floor


async def test_consuming_retry_after_keeps_its_raw_zero_delay() -> None:
    """The floor is scoped to NON-consuming deferrals: a consuming
    ``RetryAfter`` with delay 0 stays an immediate retry (``pending`` at
    now) - a real execution choosing to retry right away is bounded by
    the budget it spends, not by the deferral floor."""
    backend, job_id, worker_id = await _mem_job(max_attempts=10, retry_kind="transient")

    result = await backend.mark_retry_after(
        job_id, worker_id, timedelta(0), consume_budget=True, attempt=1
    )
    assert result == "scheduled"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "pending"
    assert row.scheduled_at == _NOW


# ── 5. the denial counter spans the terminal arm ────────────────────────


async def test_every_admission_denial_is_counted_including_the_last_before_expiry() -> None:
    """``jobs.rate_limit_blocked_count`` is the coalesced count of every
    admission denial since enqueue, and it survives onto the terminal
    row when the job's schedule-to-close finally expires.

    Because a denial writes no ``job_events`` and no ``job_attempts``
    rows, this counter is the only durable record an operator has of the
    contention a job absorbed. If the last denial - the one that ran the
    job up against its deadline - went uncounted, the job whose history
    matters most would be the one reported wrong, and an operator sizing
    a reservation pool from the column would see fewer denials than the
    job actually took.
    """
    clock = FakeClock(_NOW)
    backend, job_id, worker_id = await _mem_job(
        max_attempts=1,
        retry_kind="non_retryable",
        schedule_to_close=_NOW + _DELAY + timedelta(seconds=5),
        clock=clock,
    )

    # A denial that still fits inside the close deadline: rescheduled,
    # counted, budget untouched.
    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
        attempt=1,
    )
    assert result == "scheduled"
    row = await backend.get(job_id)
    assert row is not None
    assert row.rate_limit_blocked_count == 1

    # Re-dispatch and deny again; this deferral would land past the
    # close deadline, so the deadline path terminalises the job.
    clock.advance(_DELAY + timedelta(seconds=1))
    await backend.scheduled_to_pending()
    worker_id = new_uuid()
    dispatched = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(dispatched) == 1
    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
        attempt=dispatched[0].attempt,
    )
    assert result == "failed"

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"
    assert row.rate_limit_blocked_count == 2, (
        "every admission denial the job received must be counted, including the "
        "final one, since no per-denial event or attempt row records it"
    )


async def test_admission_denial_writes_no_event_or_attempt_rows() -> None:
    """A denial leaves no per-denial ``job_events`` or ``job_attempts``
    row - contention is recorded only by the aggregated counter on the
    job row.

    Per-denial rows are an unbounded-growth vector: a job parked behind a
    saturated bucket can be denied thousands of times, and each denial
    describes nothing about the work, because the actor never ran. The
    aggregated counter is the deliberate substitute, which is why the
    counter must be right (above) and these tables must stay clean.
    """
    backend, job_id, worker_id = await _mem_job(max_attempts=10, retry_kind="transient")
    events_before = len(await backend.get_events(job_id))

    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="rate_limit_denied",
        attempt=1,
    )
    assert result == "scheduled"

    assert await backend.get_attempts(job_id) == [], (
        "an admission denial writes no job_attempts row: the actor never ran"
    )
    assert len(await backend.get_events(job_id)) == events_before, (
        "an admission denial writes no job_events row; the aggregated denial "
        "counter on the job row is its whole durable record"
    )
