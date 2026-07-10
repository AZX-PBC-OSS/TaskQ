"""Compatibility test for retry exhaustion + idempotency-key dedup.

The in-memory consumer loop now drives retry classification end-to-end
via decide_after_failure (); the explicit mark_failed_or_retry
call that was previously required after run_until_drained is no longer needed.

Verifies (a) idempotent enqueue, (b) terminal writes
(c) per-attempt history, and (d) job_events
work end-to-end against a workflow that combines retry exhaustion with
idempotency-key dedup.
"""

from datetime import UTC, datetime
from typing import cast

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, RetryKind
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_retry_exhausted_with_idempotency_dedup() -> None:
    """Retry exhaustion + idempotency-key dedup end-to-end.

    Scenario:
    1. Stub actor with max_attempts=2, always raises a transient error.
    2. Enqueue with idempotency_key="idem-1" via backend.enqueue.
    3. run_until_drained dispatches the job; stub raises on every call.
    4. The retry classifier decides Retry on attempt 1 (attempt < max_attempts)
       and Fail on attempt 2 (attempt == max_attempts). The in-memory loop
       calls mark_failed_or_retry internally and transitions the job to
       'failed' after the second attempt.
    5. Assert final state: status='failed', 2 attempts, 1 event with
       kind='state_change', to_state='failed'.
    6. Enqueue again with idempotency_key="idem-1" — the existing
       failed job's row is returned (no new row).
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    async def failing_stub(payload: dict[str, object], ctx: object) -> None:
        raise RuntimeError("transient failure")

    backend.register_stub("failing_actor", failing_stub)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="failing_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=2,
        retry_kind=cast(RetryKind, "transient"),
        scheduled_at=_START,
        idempotency_key=IdempotencyKey("idem-1"),
    )
    row = await backend.enqueue(args)
    job_id = row.id

    await backend.run_until_drained()

    fetched = await backend.get(job_id)
    assert fetched is not None
    assert fetched.status == "failed"

    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 2
    assert all(a.outcome == "failed" for a in attempts)

    events = await backend.get_events(job_id)
    failed_events = [
        e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "failed"
    ]
    assert len(failed_events) == 1

    row2 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="failing_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=2,
            retry_kind=cast(RetryKind, "transient"),
            scheduled_at=_START,
            idempotency_key=IdempotencyKey("idem-1"),
        )
    )

    assert row2.id == job_id

    rows_with_key = [
        r
        for r in backend._jobs.values()  # type: ignore[reportPrivateUsage] # Why: test-only access to verify idempotency dedup constraint
        if r.idempotency_key == "idem-1"
    ]
    assert len(rows_with_key) == 1
