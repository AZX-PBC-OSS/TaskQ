"""Unit tests for the retry classifier wired into InMemoryBackend.run_until_drained.

Exercises the in-memory consumer loop's classify → mark_failed_or_retry →
invoke_on_retry_exhausted seam without PG, and the snooze-family terminal
handlers' hook-row and pre-actor-denial routing contracts via direct
``consume_one_job`` calls.
"""

# pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType, reportUnknownVariableType, reportAttributeAccessIssue]
# Why: ActorRef creation with pydantic BaseModel in tests uses generic inference;
# JobHandle has a public job_id property accessed directly.

import json
from datetime import UTC, datetime, timedelta
from typing import Literal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.actor import actor
from taskq.backend._protocol import (
    AttemptOutcome,
    DenialReason,
    EnqueueArgs,
    ErrorInfo,
    JobId,
    JobRow,
)
from taskq.backend.clock import Clock
from taskq.client._jobs import JobsClient
from taskq.constants import progress_channel
from taskq.exceptions import ReservationUnavailable, RetryAfter, Snooze
from taskq.retry import Retry, RetryClassifier, RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import consume_one_job

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _TestPayload(BaseModel):
    """Minimal payload model for @actor type-inference in tests."""

    pass


# ── (in-memory variant): hook fires exactly once after exhaustion ──


async def test_hook_fires_once_after_exhaustion() -> None:
    """in-memory variant: actor raises on every attempt with
    max_attempts=3; hook records calls; assert hook called exactly once
    after all attempts exhaust (not on intermediate retries).
    """
    hook_calls: list[tuple[JobRow, BaseException]] = []

    def on_exhausted(job_row: JobRow, exc: BaseException) -> None:
        hook_calls.append((job_row, exc))

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def always_fail(payload: object, ctx: object) -> None:
        nonlocal call_count
        call_count += 1
        raise RuntimeError(f"attempt {call_count}")

    backend.register_stub(
        "flaky",
        always_fail,
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_retry_exhausted=on_exhausted,
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="flaky",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.attempt == 3
    assert call_count == 3
    assert len(hook_calls) == 1


# ── RetryAfter consume_budget handling via consume_one_job ─────────────


async def test_run_until_drained_retry_after_consume_budget_false_refunds_attempt() -> None:
    """RetryAfter(consume_budget=False) refunds the claim's attempt
    increment on the scheduled row, so after one full drain cycle (one
    deferral + one re-dispatch) the attempt reflects exactly the final
    claim's increment — 1, not 2.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def retry_stub(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RetryAfter(timedelta(seconds=5), consume_budget=False)
        return {"ok": True}

    backend.register_stub("retry_actor", retry_stub)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="retry_actor",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.attempt == 1


# ── B-TG-12: RetryAfter + decide_after_failure non-interference ───────────


async def test_retry_after_consume_budget_true_no_double_increment() -> None:
    """B-TG-12: mark_retry_after(consume_budget=True) does not increment
    the attempt at write time; the dispatch CTE is the sole increment
    point, so after one full drain cycle the attempt reflects exactly
    one budget consumption per dispatch cycle.

    Verifies that the attempt count reflects only the dispatch
    increments — the scheduled→dispatch step increments once more
    (normal dispatch increment), resulting in exactly attempt=2 for a
    job that started at attempt=1, then was dispatched once more.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def counting_actor(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RetryAfter(timedelta(seconds=1), consume_budget=True)
        return {"ok": True}

    backend.register_stub("b12_actor", counting_actor)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="b12_actor",
        queue="default",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.attempt == 2, f"expected attempt=2, got {row.attempt}"
    assert call_count == 2, f"actor should have been called exactly twice, got {call_count}"


async def test_retry_after_consume_budget_false_refunds_increment() -> None:
    """B-TG-12 (non-consuming arm): mark_retry_after(consume_budget=False)
    REFUNDS the claim's increment at write time; the subsequent dispatch
    re-claims it, so one deferral cycle + one final dispatch leaves the
    attempt at 1.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def counting_actor(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RetryAfter(timedelta(seconds=1), consume_budget=False)
        return {"ok": True}

    backend.register_stub("b12_actor_false", counting_actor)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="b12_actor_false",
        queue="default",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.attempt == 1, f"expected attempt=1, got {row.attempt}"
    assert call_count == 2


# ── indefinite tier with time_budget → schedule_to_close set ──


async def test_enqueue_indefinite_with_time_budget_sets_schedule_to_close() -> None:
    """enqueue with kind='indefinite', time_budget=timedelta(hours=2)
    → JobRow.schedule_to_close == clock.now() + 2h (via InMemoryBackend;
    FakeClock is deterministic, so values are exact).
    """

    @actor(retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2)))
    async def poll_actor(payload: _TestPayload) -> None: ...

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend, clock=clock)

    handle = await client.enqueue(poll_actor, _TestPayload())
    job_id = handle.job_id
    row = await backend.get(job_id)
    assert row is not None
    assert row.schedule_to_close is not None
    assert row.schedule_to_close == _START + timedelta(hours=2)


# ── transient tier with time_budget → schedule_to_close is None ──


async def test_enqueue_transient_with_time_budget_leaves_schedule_to_close_none() -> None:
    """enqueue with kind='transient', time_budget=timedelta(hours=2)
    → JobRow.schedule_to_close is None (time_budget_as_interval returns
    None for non-indefinite kinds;).
    """

    @actor(retry=RetryPolicy(kind="transient", time_budget=timedelta(hours=2)))
    async def transient_actor(payload: _TestPayload) -> None: ...

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend, clock=clock)

    handle = await client.enqueue(transient_actor, _TestPayload())
    job_id = handle.job_id
    row = await backend.get(job_id)
    assert row is not None
    assert row.schedule_to_close is None


# ── Mutual exclusivity: EnqueueArgs raises on both fields set ──


def test_enqueue_args_mutual_exclusivity_raises_valueerror() -> None:
    """EnqueueArgs(schedule_to_close=dt, schedule_to_close_interval=td)
    raises ValueError from __post_init__.
    """
    dt = datetime(2025, 1, 2, tzinfo=UTC)
    td = timedelta(hours=2)
    with pytest.raises(ValueError, match="mutually exclusive"):
        EnqueueArgs(
            id=new_job_id(),
            actor="test",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="indefinite",
            scheduled_at=_START,
            schedule_to_close=dt,
            schedule_to_close_interval=td,
        )


# ── Caller override: explicit schedule_to_close wins over time_budget ──


async def test_caller_override_schedule_to_close_wins() -> None:
    """Caller passes explicit schedule_to_close=<datetime> for an
    indefinite-tier actor → JobRow.schedule_to_close == <datetime>
    (caller wins).
    """

    @actor(retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2)))
    async def poll_actor2(payload: _TestPayload) -> None: ...

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend, clock=clock)

    explicit_dt = datetime(2025, 1, 3, tzinfo=UTC)
    handle = await client.enqueue(
        poll_actor2,
        _TestPayload(),
        schedule_to_close=explicit_dt,
    )

    job_id = handle.job_id
    row = await backend.get(job_id)
    assert row is not None
    assert row.schedule_to_close == explicit_dt


# ── indefinite retry with time_budget → scheduled, attempt unchanged ─


async def test_indefinite_retry_attempt_unchanged() -> None:
    """indefinite actor fails → row transitions to scheduled (not failed);
    attempt on the row is unchanged by the scheduling write."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def always_fail(payload: object, ctx: object) -> None:
        raise RuntimeError("fail")

    backend.register_stub(
        "indef_u1",
        always_fail,
        retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2), jitter=0.0),
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="indef_u1",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="indefinite",
        scheduled_at=_START,
        schedule_to_close=_START + timedelta(hours=2),
    )
    await backend.enqueue(args)

    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only access to simulate dispatch with registered stub
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    assert dispatched[0].attempt == 1

    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="fail",
        error_traceback=None,
    )
    decision = RetryClassifier.classify(
        policy=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2), jitter=0.0),
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
    )
    assert isinstance(decision, Retry)

    row = await backend.mark_failed_or_retry(
        args.id,
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only
        error_info,
        decision.retry_delay,
        attempt=1,
    )
    assert row.status == "scheduled"
    assert row.attempt == 1


# ── indefinite retry exceeds deadline → Failed(DeadlineExceeded) ─


async def test_indefinite_retry_exceeds_deadline() -> None:
    """indefinite actor fails; advance FakeClock past schedule_to_close.
    The classifier has no deadline opinion (it still returns Retry); the
    deadline guard inside mark_failed_or_retry — the InMemory mirror of
    the PG mark_retry deadline CTE — fails the row in the same write.

    Dispatches the job while schedule_to_close is still in the future,
    then advances clock past the deadline."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def always_fail(payload: object, ctx: object) -> None:
        raise RuntimeError("fail")

    backend.register_stub(
        "indef_u2",
        always_fail,
        retry=RetryPolicy(kind="indefinite", time_budget=timedelta(seconds=2), jitter=0.0),
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="indef_u2",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="indefinite",
        scheduled_at=_START,
        schedule_to_close=_START + timedelta(seconds=2),
    )
    await backend.enqueue(args)

    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1

    clock.advance(timedelta(seconds=3))

    decision = RetryClassifier.classify(
        policy=RetryPolicy(kind="indefinite", time_budget=timedelta(seconds=2), jitter=0.0),
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
    )
    # C2: the classifier is not a deadline arbiter — it still decides Retry.
    assert isinstance(decision, Retry)

    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="fail",
        error_traceback=None,
    )
    # The backend's deadline guard (its own clock is the single arbiter)
    # fails the write: clock.now() + delay lands past schedule_to_close.
    row = await backend.mark_failed_or_retry(
        args.id,
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only
        error_info,
        decision.retry_delay,
        attempt=1,
    )
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"
    assert row.attempt == 1


# ── indefinite ignores max_attempts ──────────────────────────────


async def test_indefinite_ignores_max_attempts_five_failures() -> None:
    """indefinite with max_attempts=3, fail 5 times.
    All 5 transitions are to scheduled; never to failed.

    Stub raises 5 times then succeeds on attempt 6.
    Verifies max_attempts guard is not consulted for indefinite tier."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def fail_then_succeed(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count <= 5:
            raise RuntimeError(f"attempt {call_count}")
        return {"ok": True}

    backend.register_stub(
        "indef_u3",
        fail_then_succeed,
        retry=RetryPolicy(
            kind="indefinite", max_attempts=3, time_budget=timedelta(hours=2), jitter=0.0
        ),
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="indef_u3",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="indefinite",
        scheduled_at=_START,
        schedule_to_close=_START + timedelta(hours=2),
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.attempt == 6
    assert call_count == 6

    events = await backend.get_events(args.id)
    scheduled_transitions = [
        e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "scheduled"
    ]
    failed_transitions = [
        e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "failed"
    ]
    assert len(scheduled_transitions) == 5
    assert len(failed_transitions) == 0


# ── remaining-time behavior ─────────────────────────────────────


async def test_remaining_time_dispatch_not_blocked_by_start_to_close() -> None:
    """Job with schedule_to_close = clock.now() + 5s and
    start_to_close = 10min. dispatch_batch returns the job — the dispatch
    filter only checks schedule_to_close > now(), not start_to_close.

    asyncio.wait_for uses full start_to_close, NOT clamped to remaining_time."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="indef_u5")

    args = EnqueueArgs(
        id=new_job_id(),
        actor="indef_u5",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="indefinite",
        scheduled_at=_START,
        schedule_to_close=_START + timedelta(seconds=5),
        start_to_close=timedelta(minutes=10),
    )
    await backend.enqueue(args)

    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    assert dispatched[0].schedule_to_close == _START + timedelta(seconds=5)
    assert dispatched[0].start_to_close == timedelta(minutes=10)


# ── Snooze with indefinite tier ─────────────────────────────────


async def test_indefinite_snooze_refunds_attempt() -> None:
    """indefinite-tier actor raises Snooze(30s): the deferral refunds the
    claim's increment, the row → scheduled, and on re-dispatch the
    classifier fires normally — one snooze cycle + one final dispatch
    leaves the attempt at 1."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def snooze_then_succeed(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Snooze(timedelta(seconds=30))
        return {"ok": True}

    backend.register_stub(
        "indef_u9",
        snooze_then_succeed,
        retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2), jitter=0.0),
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="indef_u9",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="indefinite",
        scheduled_at=_START,
        schedule_to_close=_START + timedelta(hours=2),
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.attempt == 1

    events = await backend.get_events(args.id)
    # The snooze's row transition writes no event row; the to_state=
    # 'scheduled' events come from the enqueue's future-scheduled stamp
    # only.
    snooze_scheduled_events = [
        e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "scheduled"
    ]
    assert len(snooze_scheduled_events) == 0


# ── RetryAfter(consume_budget=True) with indefinite tier ───────


async def test_indefinite_retry_after_consume_budget_increments_attempt() -> None:
    """indefinite-tier actor raises RetryAfter(consume_budget=True).
    mark_retry_after is called; ctE skips max_attempts guard; row → scheduled
    with attempt unchanged (dispatch CTE is the sole increment point)."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def retry_after_then_succeed(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RetryAfter(timedelta(seconds=10))
        return {"ok": True}

    backend.register_stub(
        "indef_u16",
        retry_after_then_succeed,
        retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2), jitter=0.0),
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="indef_u16",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="indefinite",
        scheduled_at=_START,
        schedule_to_close=_START + timedelta(hours=2),
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.attempt == 2


# ── indefinite with no time_budget retries forever ──────────────


async def test_indefinite_no_time_budget_retries_forever() -> None:
    """kind='indefinite', time_budget=None retries forever.
    Classifier returns Retry for all 1000 attempts; no DeadlineExceeded raised.

    Direct classifier test — 1000 pure calls are fast.
    Verifies schedule_to_close=None is handled."""
    policy = RetryPolicy(kind="indefinite", time_budget=None, jitter=0.0)
    for attempt in range(1, 1001):
        decision = RetryClassifier.classify(
            policy=policy,
            non_retryable_exceptions=(),
            exception=RuntimeError(f"fail {attempt}"),
            attempt=attempt,
        )
        assert isinstance(decision, Retry), f"attempt {attempt} should be Retry, got {decision}"
        assert decision.retry_delay > timedelta(0)


# ── snooze-family terminal hooks see the POST-write row ──────────────────


async def test_on_retry_exhausted_sees_post_write_row_on_denial_budget() -> None:
    """A denial-budget terminalisation hands on_retry_exhausted the
    POST-write row: status='failed', error_class='MaxAttemptsExceeded',
    the standing (un-refunded) attempt — not the dispatch-time 'running'
    snapshot a hook cannot distinguish from a live job."""
    hook_calls: list[tuple[JobRow, BaseException]] = []

    def on_exhausted(job_row: JobRow, exc: BaseException) -> None:
        hook_calls.append((job_row, exc))

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def denied_actor(payload: object, ctx: object) -> None:
        raise ReservationUnavailable("gpu_pool", timedelta(seconds=30), source="reservation")

    backend.register_stub(
        "denied_budget",
        denied_actor,
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_retry_exhausted=on_exhausted,
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="denied_budget",
        queue="default",
        payload={},
        max_attempts=1,
        retry_kind="non_retryable",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "MaxAttemptsExceeded"

    assert len(hook_calls) == 1
    hook_row, _hook_exc = hook_calls[0]
    assert hook_row.status == "failed"
    assert hook_row.error_class == "MaxAttemptsExceeded"
    # An admission denial leaves the claim's increment standing: the
    # post-write attempt is the dispatched value.
    assert hook_row.attempt == 1


async def test_on_retry_exhausted_sees_post_write_row_on_snooze_deadline() -> None:
    """A Snooze past schedule_to_close terminally fails the job; the
    exhausted hook sees the post-write failed row (DeadlineExceeded, the
    un-refunded attempt), and the job-failed log's snooze_count field
    reads the row column — the metadata mirror is gone."""
    hook_calls: list[tuple[JobRow, BaseException]] = []

    def on_exhausted(job_row: JobRow, exc: BaseException) -> None:
        hook_calls.append((job_row, exc))

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def snooze_past_deadline(payload: object, ctx: object) -> None:
        raise Snooze(timedelta(seconds=30))

    backend.register_stub(
        "snooze_deadline",
        snooze_past_deadline,
        retry=RetryPolicy(kind="transient", max_attempts=5, jitter=0.0),
        on_retry_exhausted=on_exhausted,
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="snooze_deadline",
        queue="default",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=_START,
        schedule_to_close=_START + timedelta(seconds=5),
    )
    await backend.enqueue(args)

    with structlog.testing.capture_logs() as captured:
        await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"
    # The deadline arm does not refund: the attempt keeps the claim's
    # increment, and the deferral that was rejected never landed — the
    # snooze counter did not move.
    assert row.attempt == 1
    assert row.snooze_count == 0

    assert len(hook_calls) == 1
    hook_row, _hook_exc = hook_calls[0]
    assert hook_row.status == "failed"
    assert hook_row.error_class == "DeadlineExceeded"
    assert hook_row.attempt == 1

    job_failed = [e for e in captured if e.get("event") == "job-failed"]
    assert len(job_failed) == 1
    assert job_failed[0]["snooze_count"] == row.snooze_count


async def test_on_retry_exhausted_sees_post_write_row_on_retry_after_budget() -> None:
    """A consuming RetryAfter on a job with no remaining budget fails it
    with MaxAttemptsExceeded; the exhausted hook sees the post-write
    failed row, not the dispatch-time running snapshot."""
    hook_calls: list[tuple[JobRow, BaseException]] = []

    def on_exhausted(job_row: JobRow, exc: BaseException) -> None:
        hook_calls.append((job_row, exc))

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def retry_after_actor(payload: object, ctx: object) -> None:
        raise RetryAfter(timedelta(seconds=30), consume_budget=True)

    backend.register_stub(
        "retry_after_budget",
        retry_after_actor,
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_retry_exhausted=on_exhausted,
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="retry_after_budget",
        queue="default",
        payload={},
        max_attempts=1,
        retry_kind="non_retryable",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "MaxAttemptsExceeded"

    assert len(hook_calls) == 1
    hook_row, _hook_exc = hook_calls[0]
    assert hook_row.status == "failed"
    assert hook_row.error_class == "MaxAttemptsExceeded"


# ── pre-actor denial routing through the terminal path ───────────────────


class _AcquireDeniesRegistry:
    """RateLimitRegistry stand-in whose acquire always denies — drives
    consume_one_job's pre-actor denial path without a real bucket."""

    def __init__(self, exc: ReservationUnavailable) -> None:
        self._exc = exc

    async def acquire_for_actor(
        self,
        rate_limits: list[str],
        reservations: list[str],
        *,
        job_id: UUID,
        worker_id: UUID,
        payload: object = None,
        redis_client: object = None,
        pg_pool: object = None,
        clock: Clock | None = None,
        settings: WorkerSettings | None = None,
    ) -> list[object]:
        raise self._exc

    async def release_for_actor(self, acquired: list[object], *, pg_pool: object = None) -> None:
        pass


class _SnoozeWriteInfraFails(InMemoryBackend):
    """In-memory twin whose snooze terminal write fails with an infra
    error — the DB dropping the socket mid-write — to drive the
    pre-actor denial path's infra handling."""

    async def mark_snoozed(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        outcome: AttemptOutcome = "snoozed",
        attempt: int | None = None,
        denial_reason: DenialReason = "capacity",
    ) -> Literal["scheduled", "failed", "failed:MaxAttemptsExceeded", "noop"]:
        raise OSError("db socket closed mid-snooze-write")


async def _never_runs(job_row: object, ctx: object) -> object:
    raise AssertionError("actor body must not run on a pre-actor denial")


def _denial() -> ReservationUnavailable:
    return ReservationUnavailable("gpu_pool", timedelta(seconds=5), source="reservation")


async def _enqueue_and_dispatch_running(
    backend: InMemoryBackend, *, actor: str
) -> tuple[JobId, UUID]:
    """Enqueue one transient job and dispatch it to a running-owned row."""
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    if actor not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage] # Why: test-only private access; the established fixture pattern.
        backend.register_actor_config(actor=actor)
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={},
        max_attempts=10,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access; the runner's own dispatch uses the same worker id
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) == 1
    return args.id, worker_id


async def test_pre_actor_denial_infra_failure_is_terminal_write_failure() -> None:
    """An infra failure from the denial's snooze write on the PRE-ACTOR
    path is logged as a terminal-write infra failure and the dispatch
    reports the snooze-path outcome — the infra error is NOT re-run
    through the retry decision as if it were the actor's failure."""
    clock = FakeClock(start=_START)
    backend = _SnoozeWriteInfraFails(clock=clock)
    job_id, worker_id = await _enqueue_and_dispatch_running(backend, actor="pre_actor_denied")
    job = await backend.get(job_id)
    assert job is not None

    with structlog.testing.capture_logs() as captured:
        outcome = await consume_one_job(
            backend,
            job,
            worker_id,
            run_actor=_never_runs,
            actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
            payload_type=EmptyPayload,
            clock=clock,
            rate_limit_registry=_AcquireDeniesRegistry(_denial()),
            rate_limits=[],
            reservations=["gpu_pool"],
        )

    assert outcome == "scheduled"

    terminal_write_failures = [e for e in captured if e.get("event") == "terminal-write-failed"]
    assert len(terminal_write_failures) == 1
    entry = terminal_write_failures[0]
    assert entry["infra_error_class"] == "OSError"
    assert entry["job_error_class"] == "ReservationUnavailable"
    # Not re-classified: no job_exception/job-failed event carries the
    # infra error as the actor's failure.
    reclassified = [
        e
        for e in captured
        if e.get("event") in ("job_exception", "job-failed") and e.get("error_class") == "OSError"
    ]
    assert reclassified == []


def _publish_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_SCHEMA_NAME": "retry_inmemory_test", "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false"}
    )


async def test_pre_actor_denial_publishes_state_change_event() -> None:
    """A pre-actor admission denial publishes the scheduled state-change
    event — the same Redis signal an in-actor denial publishes, so
    stream consumers see the requeue either way."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    job_id, worker_id = await _enqueue_and_dispatch_running(backend, actor="pre_actor_denied")
    job = await backend.get(job_id)
    assert job is not None

    redis_mock = AsyncMock()
    redis_mock.publish.return_value = 1

    outcome = await consume_one_job(
        backend,
        job,
        worker_id,
        run_actor=_never_runs,
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        payload_type=EmptyPayload,
        clock=clock,
        rate_limit_registry=_AcquireDeniesRegistry(_denial()),
        rate_limits=[],
        reservations=["gpu_pool"],
        redis_client=redis_mock,
        settings=_publish_settings(),
    )

    assert outcome == "scheduled"
    redis_mock.publish.assert_called_once()
    channel, payload = redis_mock.publish.call_args.args
    assert channel == progress_channel("retry_inmemory_test", job.id)
    event: dict[str, object] = json.loads(payload)
    assert event["kind"] == "state_change"
    assert event["status"] == "scheduled"
    assert event["terminal"] is False


async def test_noop_terminal_write_publishes_no_state_change_event() -> None:
    """A noop outcome means NO transition happened (the row moved
    underneath this dispatch) — publishing a scheduled state-change for
    a row that did not move would be a false event.

    Drives the IN-ACTOR denial path (the exception routes through
    ``_run_terminal_path``); the row is not running-owned, so the
    handler's snooze write matches nothing and reports noop. The
    pre-actor "running" announce still publishes — only the false
    scheduled event is suppressed.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="in_actor_denied",
        queue="default",
        payload={},
        max_attempts=10,
        retry_kind="transient",
        scheduled_at=_START + timedelta(hours=1),
    )
    await backend.enqueue(args)
    job = await backend.get(args.id)
    assert job is not None
    assert job.status == "scheduled"
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access, mirrors the runner's dispatch worker id

    async def deny(job_row: object, ctx: object) -> object:
        raise ReservationUnavailable("gpu_pool", timedelta(seconds=5), source="reservation")

    redis_mock = AsyncMock()
    redis_mock.publish.return_value = 1

    outcome = await consume_one_job(
        backend,
        job,
        worker_id,
        run_actor=deny,
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        payload_type=EmptyPayload,
        clock=clock,
        redis_client=redis_mock,
        settings=_publish_settings(),
    )

    assert outcome == "noop"
    published_statuses = [
        json.loads(call.args[1])["status"] for call in redis_mock.publish.call_args_list
    ]
    assert "scheduled" not in published_statuses
