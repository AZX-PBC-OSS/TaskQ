"""hypothesis property test for the snooze invariant.

For any ``(delay >= 0, schedule_to_close)`` pair, the snooze outcome is
deterministic: *failed* iff ``schedule_to_close`` is set and the new
``scheduled_at`` would exceed it, *scheduled* otherwise. Additionally,
a snooze + re-dispatch round-trip returns the attempt to its pre-snooze
value: the deferral refunds the claim's increment and the re-dispatch
re-claims it.

Runs on the in-memory backend only (hypothesis controls backend
lifecycle; PG requires testcontainers and cannot be reset between
examples).

anchors: (snooze), (state machine),
(in-memory backend), (deadline guard).
"""

from datetime import UTC, datetime, timedelta

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)


@settings(max_examples=200, deadline=None)
@given(
    delay_seconds=st.floats(
        min_value=1,
        max_value=3600,
        allow_nan=False,
        allow_infinity=False,
    ),
    deadline_offset=st.one_of(
        st.none(),
        st.floats(
            min_value=0,
            max_value=7200,
            allow_nan=False,
            allow_infinity=False,
        ),
    ),
)
async def test_snooze_deterministic_outcome_and_attempt_round_trip(
    delay_seconds: float,
    deadline_offset: float | None,
) -> None:
    """for any (delay, schedule_to_close) pair, the snooze outcome
    is deterministic and a snooze + re-dispatch round-trip returns the
    attempt to its pre-snooze value (the refund restores the claim's
    base; the re-dispatch re-claims exactly one increment).

    *delay_seconds* is the snooze delay (>= 0). *deadline_offset* is the
    offset (in seconds) from ``_START`` for ``schedule_to_close``, or
    ``None`` for no deadline.
    """
    backend = InMemoryBackend(clock=FakeClock(_START))
    # Dispatch candidates come FROM the actor_config registry on both
    # backends (PG's per_actor_capacity CTE) - the worker registers the
    # actor it runs, so the twin's registry must carry it too.
    backend.register_actor_config(actor="test_actor")
    delay = timedelta(seconds=delay_seconds)
    schedule_to_close: datetime | None = (
        None if deadline_offset is None else _START + timedelta(seconds=deadline_offset)
    )

    # Why: dispatch_batch filters jobs where schedule_to_close <= now
    # (strict), so the first dispatch needs schedule_to_close > _START
    # after datetime-microsecond quantization. The round-trip leg
    # additionally advances the clock by 1s before redispatching, so a
    # deadline in the dead zone (_START, _START + delay + 1s] is neither
    # exceeded at snooze time nor far enough in the future for the
    # redispatch to succeed. Exclude the dead zone; keep the
    # cleanly-exceeded and cleanly-future ranges.
    if schedule_to_close is not None:
        assume(schedule_to_close > _START)
        assume(
            schedule_to_close < _START + delay
            or schedule_to_close > _START + delay + timedelta(seconds=1)
        )

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=_START,
            schedule_to_close=schedule_to_close,
        )
    )

    wid = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access; InMemoryBackend dispatch requires a worker_id
    dispatched = await backend.dispatch_batch(
        worker_id=wid,
        queues=["default"],
        limit=1,
        lock_lease=_LOCK_LEASE,
    )
    assert len(dispatched) == 1

    row = await backend.get(job_id)
    assert row is not None
    assert row.attempt == 1

    # attempt= the row's current epoch - the worker presents job.attempt
    # from its in-hand row (worker/_consumer.py); the fence refuses a
    # caller that cannot prove which attempt it terminates.
    result = await backend.mark_snoozed(job_id, wid, delay, attempt=row.attempt)

    new_scheduled_at = _START + delay
    deadline_exceeded = schedule_to_close is not None and new_scheduled_at > schedule_to_close

    if deadline_exceeded:
        assert result == "failed"
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"
        assert row.error_message == "schedule_to_close reached before next dispatch"
    else:
        assert result == "scheduled"
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        # The refund: the claim's increment (0 → 1) is returned.
        assert row.attempt == 0

        # Round-trip: advance clock past scheduled_at, promote, dispatch again
        backend.advance_clock_to(new_scheduled_at + timedelta(seconds=1))
        await backend.scheduled_to_pending()

        dispatched2 = await backend.dispatch_batch(
            worker_id=wid,
            queues=["default"],
            limit=1,
            lock_lease=_LOCK_LEASE,
        )
        assert len(dispatched2) == 1

        row = await backend.get(job_id)
        assert row is not None
        assert row.attempt == 1, (
            f"round-trip attempt invariant violated: expected 1, got {row.attempt}"
        )
