"""Unit tests for the retry classifier wired into InMemoryBackend.run_until_drained.

Exercises the in-memory consumer loop's classify → mark_failed_or_retry →
invoke_on_retry_exhausted seam without PG.
"""

# pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType, reportUnknownVariableType, reportAttributeAccessIssue]
# Why: ActorRef creation with pydantic BaseModel in tests uses generic inference;
# JobHandle has a public job_id property accessed directly.

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.actor import actor
from taskq.backend._protocol import EnqueueArgs, ErrorInfo
from taskq.client._jobs import JobsClient
from taskq.exceptions import RetryAfter, Snooze
from taskq.retry import Fail, Retry, RetryClassifier, RetryPolicy
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

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
    hook_calls: list[tuple[object, object]] = []

    def on_exhausted(job_row: object, exc: object) -> None:
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


async def test_run_until_drained_retry_after_consume_budget_false_preserves_attempt() -> None:
    """RetryAfter(consume_budget=False) does not increment the attempt
    counter on the scheduled row, so after one full drain cycle the
    attempt reflects only the dispatches (not a budget consumption).
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
    assert row.attempt == 2


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


async def test_retry_after_consume_budget_false_no_double_increment() -> None:
    """B-TG-12: mark_retry_after(consume_budget=False) does NOT increment
    attempt at write time; subsequent dispatch increments once, resulting
    in attempt=2.
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
    assert row.attempt == 2, f"expected attempt=2, got {row.attempt}"
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
        schedule_to_close=_START + timedelta(hours=2),
        now=clock.now(),
    )
    assert isinstance(decision, Retry)

    row = await backend.mark_failed_or_retry(
        args.id,
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only
        error_info,
        decision.next_scheduled_at,
    )
    assert row.status == "scheduled"
    assert row.attempt == 1


# ── indefinite retry exceeds deadline → Failed(DeadlineExceeded) ─


async def test_indefinite_retry_exceeds_deadline() -> None:
    """indefinite actor fails; advance FakeClock past schedule_to_close.
    Classifier returns Fail(DeadlineExceeded); row transitions to failed.

    Dispatches the job while schedule_to_close is still in the future,
    then advances clock past the deadline. The classify call detects
    the deadline and returns Fail(DeadlineExceeded)."""
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
        schedule_to_close=_START + timedelta(seconds=2),
        now=clock.now(),
    )
    assert isinstance(decision, Fail)
    assert decision.error_class == "DeadlineExceeded"
    assert not decision.retryable

    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="fail",
        error_traceback=None,
    )
    row = await backend.mark_failed_or_retry(
        args.id,
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only
        error_info,
        None,
    )
    assert row.status == "failed"
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


async def test_indefinite_snooze_preserves_attempt() -> None:
    """indefinite-tier actor raises Snooze(30s).
    attempt unchanged; row → scheduled; on re-dispatch classifier fires normally."""
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
    assert row.attempt == 2

    events = await backend.get_events(args.id)
    snooze_scheduled_events = [
        e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "scheduled"
    ]
    assert len(snooze_scheduled_events) == 1


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
            schedule_to_close=None,
            now=_START,
        )
        assert isinstance(decision, Retry), f"attempt {attempt} should be Retry, got {decision}"
        assert decision.next_scheduled_at > _START
