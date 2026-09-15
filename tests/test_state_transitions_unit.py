"""Unit tests for every legal state transition on the in-memory backend.

Each test asserts the post-state via ``backend.get``, the AttemptRow side
effect via ``backend.get_attempts``, and the EventRow side effect via
``backend.get_events``.

PG-specific isolate_self bypass paths are NOT in scope here — they are
exercised against PG via the existing isolate_self test patterns.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from taskq.backend._protocol import (
    CancelPhase,
    ErrorInfo,
    JobId,
    RetryKind,
)
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def _enqueue_and_dispatch(
    backend: InMemoryBackend,
    *,
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    schedule_to_close: datetime | None = None,
    scheduled_at: datetime = _START,
) -> tuple[JobId, UUID]:
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    if "test_actor" not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]  # Why: test-only private access; the established fixture pattern.
        backend.register_actor_config(actor="test_actor")
    args = make_enqueue_args(
        payload={"key": "value"},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        schedule_to_close=schedule_to_close,
        scheduled_at=scheduled_at,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access for dispatch_batch
    dispatched = await backend.dispatch_batch(
        worker_id,
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    return dispatched[0].id, worker_id


# ── None → pending via enqueue ──────────────────────────────────


async def test_none_to_pending(memory_jobs: InMemoryBackend) -> None:
    """None → pending via enqueue with scheduled_at <= now()."""
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=_START)
    row = await memory_jobs.enqueue(args)

    assert row.status == "pending"
    assert row.attempt == 0

    attempts = await memory_jobs.get_attempts(args.id)
    assert len(attempts) == 0


# ── None → scheduled via enqueue ────────────────────────────────


async def test_none_to_scheduled(memory_jobs: InMemoryBackend) -> None:
    """None → scheduled via enqueue with scheduled_at > now()."""
    future = _START + timedelta(hours=1)
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=future)
    row = await memory_jobs.enqueue(args)

    assert row.status == "scheduled"
    assert row.attempt == 0


# ── pending → running via dispatch_batch ────────────────────────


async def test_pending_to_running(memory_jobs: InMemoryBackend) -> None:
    """pending → running via dispatch_batch."""
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=_START)
    await memory_jobs.enqueue(args)

    worker_id = memory_jobs._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access for dispatch_batch
    dispatched = await memory_jobs.dispatch_batch(
        worker_id,
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "running"
    assert row.attempt == 1
    assert row.locked_by_worker == worker_id


# ── running → succeeded via mark_succeeded ──────────────────────


async def test_running_to_succeeded(memory_jobs: InMemoryBackend) -> None:
    """running → succeeded via mark_succeeded."""
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)

    result = await memory_jobs.mark_succeeded(job_id, worker_id, result={"value": 42}, attempt=1)
    assert result is True

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.result == {"value": 42}
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "succeeded"
    assert attempts[0].error_class is None

    events = await memory_jobs.get_events(job_id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert any(
        e.detail["from_state"] == "running" and e.detail["to_state"] == "succeeded"
        for e in state_changes
    )


# ── running → failed (retry exhausted, transient + at-limit) ───


async def test_running_to_failed_retry_exhausted(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → failed (retry exhausted, transient + at-limit)."""
    job_id, worker_id = await _enqueue_and_dispatch(
        memory_jobs, max_attempts=1, retry_kind="transient"
    )

    updated = await memory_jobs.mark_failed_or_retry(
        job_id,
        worker_id,
        ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None),
        retry_delay=None,
        attempt=1,
    )
    assert updated.status == "failed"
    assert updated.error_class == "ValueError"
    assert updated.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_class == "ValueError"

    events = await memory_jobs.get_events(job_id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert any(
        e.detail["from_state"] == "running"
        and e.detail["to_state"] == "failed"
        and e.detail.get("error_class") == "ValueError"
        for e in state_changes
    )


# ── running → failed (non-retryable exception) ──────────────────


async def test_running_to_failed_non_retryable(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → failed (non-retryable exception)."""
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs, retry_kind="non_retryable")

    updated = await memory_jobs.mark_failed_or_retry(
        job_id,
        worker_id,
        ErrorInfo(error_class="TypeError", error_message="non-retryable", error_traceback=None),
        retry_delay=None,
        attempt=1,
    )
    assert updated.status == "failed"
    assert updated.error_class == "TypeError"
    assert updated.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_class == "TypeError"


# ── running → failed via mark_snoozed deadline guard ────────────


async def test_running_to_failed_snooze_deadline(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → failed via mark_snoozed deadline guard."""
    deadline = _START + timedelta(seconds=10)
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs, schedule_to_close=deadline)

    result = await memory_jobs.mark_snoozed(
        job_id, worker_id, delay=timedelta(seconds=20), attempt=1
    )
    assert result == "failed"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_class == "DeadlineExceeded"


# ── running → failed via mark_retry_after deadline guard ────────


async def test_running_to_failed_retry_after_deadline(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → failed via mark_retry_after deadline guard."""
    deadline = _START + timedelta(seconds=10)
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs, schedule_to_close=deadline)

    result = await memory_jobs.mark_retry_after(
        job_id, worker_id, delay=timedelta(seconds=20), attempt=1
    )
    assert result == "failed:DeadlineExceeded"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_class == "DeadlineExceeded"


# ── running → failed via mark_retry_after MaxAttemptsExceeded ───


async def test_running_to_failed_max_attempts(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → failed via mark_retry_after MaxAttemptsExceeded."""
    job_id, worker_id = await _enqueue_and_dispatch(
        memory_jobs, max_attempts=1, retry_kind="transient"
    )

    result = await memory_jobs.mark_retry_after(
        job_id, worker_id, delay=timedelta(seconds=5), consume_budget=True, attempt=1
    )
    assert result == "failed:MaxAttemptsExceeded"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "MaxAttemptsExceeded"
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_class == "MaxAttemptsExceeded"


# ── running → scheduled via mark_snoozed (Snooze) ───────────────


async def test_running_to_scheduled_snooze(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → scheduled via mark_snoozed (Snooze): the deferral refunds
    the claim's attempt increment, so the row returns to its pre-claim
    attempt value."""
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)

    result = await memory_jobs.mark_snoozed(
        job_id, worker_id, delay=timedelta(seconds=30), attempt=1
    )
    assert result == "scheduled"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.scheduled_at == _START + timedelta(seconds=30)
    # The refund: a running row dispatched at attempt 1 goes back to its
    # pre-claim 0 — the snooze is budget-free and unbounded.
    assert row.attempt == 0

    # The row transition is real but writes no event row: the only
    # state_change events belong to dispatches and terminal exits.
    events = await memory_jobs.get_events(job_id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert not any(
        e.detail["from_state"] == "running" and e.detail["to_state"] == "scheduled"
        for e in state_changes
    )
    assert row.snooze_count == 1
    assert row.rate_limit_blocked_count == 0


# ── running → scheduled via mark_retry_after (consume_budget=True) ─


async def test_running_to_scheduled_retry_after_consume(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → scheduled via mark_retry_after (consume_budget=True)."""
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)
    assert (await memory_jobs.get(job_id)) is not None
    pre_attempt = (await memory_jobs.get(job_id)).attempt  # type: ignore[union-attr] # Why: just dispatched, row exists

    result = await memory_jobs.mark_retry_after(
        job_id, worker_id, delay=timedelta(seconds=5), consume_budget=True, attempt=1
    )
    assert result == "scheduled"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.attempt == pre_attempt
    assert row.scheduled_at == _START + timedelta(seconds=5)


# ── running → scheduled via mark_retry_after (consume_budget=False) ─


async def test_running_to_scheduled_retry_after_no_consume(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → scheduled via mark_retry_after (consume_budget=False): a
    non-consuming RetryAfter is a deferral — it refunds the claim's
    attempt increment exactly like a Snooze."""
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)
    pre_attempt = (await memory_jobs.get(job_id)).attempt  # type: ignore[union-attr] # Why: just dispatched, row exists

    result = await memory_jobs.mark_retry_after(
        job_id, worker_id, delay=timedelta(seconds=5), consume_budget=False, attempt=1
    )
    assert result == "scheduled"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    # The refund returns the pre-claim value (attempt - 1, floored at 0).
    assert row.attempt == pre_attempt - 1
    # The consume-false deferral is counted on the same row column a
    # Snooze uses — one counter for all non-consuming deferrals.
    assert row.snooze_count == 1


# ── running → scheduled via mark_snoozed(outcome='reservation_denied') ─


async def test_running_to_scheduled_reservation_denied(
    memory_jobs: InMemoryBackend,
) -> None:
    """A reservation denial reschedules without spending retry budget.

    Being refused admission is a "come back later" answer about capacity,
    not an execution of the job: the actor never ran, so nothing about
    the job was tried and nothing may be charged against its retry
    budget. The claim's attempt increment is refunded, no attempt row is
    written, and the aggregated denial counter on the row is the whole
    durable record of the contention.
    """
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)

    result = await memory_jobs.mark_snoozed(
        job_id,
        worker_id,
        delay=timedelta(seconds=5),
        outcome="reservation_denied",
        metadata_update={"awaiting": "slot"},
        attempt=1,
    )
    assert result == "scheduled"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.metadata.get("awaiting") == "slot"

    # A denial is admission control, not an execution: no attempt row;
    # the denial counter on the row is its durable record.
    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 0
    assert row.rate_limit_blocked_count == 1
    assert row.snooze_count == 0
    # No budget consumed: the claim's increment is refunded, exactly as
    # an actor-requested deferral refunds it, so a job that is denied a
    # slot a thousand times still has its full budget for its first real
    # execution.
    assert row.attempt == 0
    assert row.max_attempts == 3


# ── denial at spent budget reschedules, never terminally fails ──


async def test_denial_at_spent_budget_reschedules_not_fails(
    memory_jobs: InMemoryBackend,
) -> None:
    """An admission denial can never by itself terminally fail a job.

    A rate-limit or reservation denial carries the semantics of an HTTP
    429 with Retry-After: the job is rescheduled indefinitely with
    backoff until capacity frees, and only its schedule-to-close
    deadline may end it. A queue or rate-limit misconfiguration must not
    be able to kill work that simply never got a slot, so a job whose
    attempt counter already sits at its ceiling — and which carries no
    close deadline — is still rescheduled rather than failed with
    MaxAttemptsExceeded.
    """
    job_id, worker_id = await _enqueue_and_dispatch(
        memory_jobs, max_attempts=1, retry_kind="transient"
    )

    row = await memory_jobs.get(job_id)
    assert row is not None
    # The claim put the row at its ceiling with no deadline to exit by:
    # the only exit a denial could invent here is a terminal one.
    assert row.attempt == row.max_attempts
    assert row.schedule_to_close is None

    result = await memory_jobs.mark_snoozed(
        job_id,
        worker_id,
        delay=timedelta(seconds=5),
        outcome="rate_limit_denied",
        attempt=1,
    )
    assert result == "scheduled"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "scheduled"
    assert row.error_class is None
    assert row.finished_at is None
    # Contention stays visible through the aggregated counter, not
    # through a terminal state.
    assert row.rate_limit_blocked_count == 1

    # No per-denial bookkeeping rows on either surface.
    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 0
    events = await memory_jobs.get_events(job_id)
    assert not any(
        e.detail.get("error_class") == "MaxAttemptsExceeded"
        for e in events
        if e.kind == "state_change"
    )


# ── a denied job's only terminal exit is its schedule-to-close ──


async def test_denial_terminal_exit_is_the_deadline(
    memory_jobs: InMemoryBackend,
) -> None:
    """Expiry, not budget, is what finally ends a perpetually denied job.

    Rescheduling a denied job indefinitely is only safe because the
    schedule-to-close deadline still bounds it. When the next admission
    retry would land past that deadline the job fails terminally through
    the normal deadline path — DeadlineExceeded, the honest cause —
    never through the retry budget.
    """
    deadline = _START + timedelta(seconds=10)
    job_id, worker_id = await _enqueue_and_dispatch(
        memory_jobs, max_attempts=1, retry_kind="transient", schedule_to_close=deadline
    )

    result = await memory_jobs.mark_snoozed(
        job_id,
        worker_id,
        delay=timedelta(seconds=30),
        outcome="rate_limit_denied",
        attempt=1,
    )
    assert result == "failed"

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"
    assert row.finished_at is not None


# ── running → scheduled via mark_failed_or_retry Branch B ───────


async def test_running_to_scheduled_transient_retry(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → scheduled via mark_failed_or_retry Branch B (transient retry)."""
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)

    next_at = _START + timedelta(seconds=30)
    updated = await memory_jobs.mark_failed_or_retry(
        job_id,
        worker_id,
        ErrorInfo(
            error_class="ConnectionError",
            error_message="transient",
            error_traceback=None,
        ),
        # The decision is a delay — the backend's own clock (FakeClock at
        # _START) derives scheduled_at = now + 30s == next_at.
        retry_delay=next_at - _START,
        attempt=1,
    )
    assert updated.status == "scheduled"
    assert updated.scheduled_at == next_at
    assert updated.locked_by_worker is None
    assert updated.lock_expires_at is None

    events = await memory_jobs.get_events(job_id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert any(
        e.detail["from_state"] == "running" and e.detail["to_state"] == "scheduled"
        for e in state_changes
    )


# ── scheduled → pending via scheduled_to_pending ────────────────


async def test_scheduled_to_pending(memory_jobs: InMemoryBackend) -> None:
    """scheduled → pending via scheduled_to_pending."""
    future = _START + timedelta(hours=1)
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=future)
    await memory_jobs.enqueue(args)

    memory_jobs.advance_clock_to(future)
    count = await memory_jobs.scheduled_to_pending()
    assert count == 1

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "pending"


# ── pending → cancelled via write_cancel_request ────────────────


async def test_pending_to_cancelled(memory_jobs: InMemoryBackend) -> None:
    """pending → cancelled via write_cancel_request."""
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=_START)
    await memory_jobs.enqueue(args)

    ok = await memory_jobs.write_cancel_request(args.id, reason="user")
    assert ok is True

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "cancelled"
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(args.id)
    assert len(attempts) == 0

    events = await memory_jobs.get_events(args.id)
    state_changes = [e for e in events if e.kind == "state_change"]
    cancel_requests = [e for e in events if e.kind == "cancel_request"]
    assert len(state_changes) == 1
    assert len(cancel_requests) == 1
    assert state_changes[0].detail["from_state"] == "pending"
    assert state_changes[0].detail["to_state"] == "cancelled"


# ── scheduled → cancelled via write_cancel_request ──────────────


async def test_scheduled_to_cancelled(memory_jobs: InMemoryBackend) -> None:
    """scheduled → cancelled via write_cancel_request."""
    future = _START + timedelta(hours=1)
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=future)
    await memory_jobs.enqueue(args)

    ok = await memory_jobs.write_cancel_request(args.id, reason="user")
    assert ok is True

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "cancelled"
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(args.id)
    assert len(attempts) == 0

    events = await memory_jobs.get_events(args.id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert len(state_changes) == 1
    assert state_changes[0].detail["from_state"] == "scheduled"
    assert state_changes[0].detail["to_state"] == "cancelled"


# ── running → cancelled cooperative (cp=1 preserved) ────────────


async def test_running_to_cancelled_cooperative(
    memory_jobs: InMemoryBackend,
) -> None:
    """A cooperative cancel is auditable from the row alone.

    Every other terminal path stamps an error_class naming why the job
    ended; a cancel that leaves it NULL makes a job that stopped itself
    on request indistinguishable from one that was forcibly abandoned,
    so an operator reconstructing what happened has only the logs. The
    terminal write records a distinguishing cause alongside the
    preserved cancel phase.
    """
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)

    ok = await memory_jobs.write_cancel_request(job_id, reason="user")
    assert ok is True
    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.cancel_phase == CancelPhase.COOPERATIVE

    ok = await memory_jobs.mark_cancelled(job_id, worker_id, attempt=1)
    assert ok is True

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "cancelled"
    assert row.finished_at is not None
    assert row.cancel_phase == CancelPhase.COOPERATIVE
    assert row.error_class is not None, (
        "a cooperative cancel wrote no cause — cancel origin is not reconstructable from the row"
    )

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "cancelled"
    assert attempts[0].error_class == row.error_class


# ── running → cancelled forced (cp=2 preserved) ─────────────────


async def test_running_to_cancelled_forced(
    memory_jobs: InMemoryBackend,
) -> None:
    """A forced cancel records a cause distinct from a cooperative one.

    The two paths mean different things operationally: a cooperative
    cancel is an actor that noticed the request and stopped cleanly, a
    forced one is an actor that had to be interrupted. Telling them
    apart in the database — not only in logs — is what lets an operator
    see that actors are ignoring cancellation requests.
    """
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)

    await memory_jobs.write_cancel_request(job_id, reason="user")
    ok = await memory_jobs.write_cancel_escalation(job_id, worker_id, phase=2)
    assert ok is True
    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.cancel_phase == CancelPhase.FORCED

    ok = await memory_jobs.mark_cancelled(job_id, worker_id, attempt=1)
    assert ok is True

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "cancelled"
    assert row.finished_at is not None
    assert row.cancel_phase == CancelPhase.FORCED
    assert row.error_class is not None, (
        "a forced cancel wrote no cause — cancel origin is not reconstructable from the row"
    )

    # The same run through the cooperative path must land a different
    # cause, or the column carries no information.
    coop_id, coop_worker = await _enqueue_and_dispatch(memory_jobs)
    await memory_jobs.write_cancel_request(coop_id, reason="user")
    assert await memory_jobs.mark_cancelled(coop_id, coop_worker, attempt=1) is True
    coop_row = await memory_jobs.get(coop_id)
    assert coop_row is not None
    assert coop_row.error_class != row.error_class, (
        "a forced cancel and a cooperative cancel wrote the same cause — "
        "the two are indistinguishable from the database"
    )


# ── running → abandoned (cp=2, cleanup grace elapsed) ───────────


async def test_running_to_abandoned(
    memory_jobs: InMemoryBackend,
) -> None:
    """An abandoned job's attempt row names abandonment as the cause.

    An abandon is the outcome where the worker gave up waiting for an
    actor that would not stop; its audit row must say so rather than
    reading like any other cancellation, since it is the signal that
    cleanup grace was exhausted.
    """
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)

    await memory_jobs.write_cancel_request(job_id, reason="timeout")
    await memory_jobs.write_cancel_escalation(job_id, worker_id, phase=2)

    ok = await memory_jobs.mark_abandoned(job_id)
    assert ok is True

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "abandoned"
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "cancelled"
    assert attempts[0].error_class is not None, (
        "an abandoned job's attempt row wrote no cause — an abandon is "
        "indistinguishable from a clean cancel in the audit trail"
    )


# ── running → crashed via reclaim_expired_locks ──────────────────


async def test_running_to_crashed_reclaim(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → crashed via reclaim_expired_locks (retries exhausted); job row error_class=None, AttemptRow error_class='WorkerCrashed'."""
    job_id, _worker_id = await _enqueue_and_dispatch(
        memory_jobs, max_attempts=1, retry_kind="transient"
    )

    row = await memory_jobs.get(job_id)
    assert row is not None
    expired = row.lock_expires_at
    assert expired is not None

    memory_jobs.advance_clock_to(expired + timedelta(seconds=1))

    count = await memory_jobs.reclaim_expired_locks(timedelta(seconds=30), timedelta(seconds=30))
    assert count == 1

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "crashed"
    assert (
        row.error_class is None
    )  # PG sweep does not set error_class on jobs row; only AttemptRow carries it
    assert row.finished_at is not None

    attempts = await memory_jobs.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "crashed"
    assert attempts[0].error_class == "WorkerCrashed"

    events = await memory_jobs.get_events(job_id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert any(
        e.detail["from_state"] == "running" and e.detail["to_state"] == "crashed"
        for e in state_changes
    )


# ── running → pending [BYPASS] via reclaim_expired_locks ────────


async def test_running_to_pending_bypass_reclaim(
    memory_jobs: InMemoryBackend,
) -> None:
    """running → pending [BYPASS] via reclaim_expired_locks (retries remain, scheduled_at = now + 5s, lock fields cleared)."""
    job_id, _worker_id = await _enqueue_and_dispatch(
        memory_jobs, max_attempts=3, retry_kind="transient"
    )

    row = await memory_jobs.get(job_id)
    assert row is not None
    expired = row.lock_expires_at
    assert expired is not None

    memory_jobs.advance_clock_to(expired + timedelta(seconds=1))

    count = await memory_jobs.reclaim_expired_locks(timedelta(seconds=30), timedelta(seconds=30))
    assert count == 1

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "pending"
    # The sweep's 5s backoff is stamped from the backend's own clock,
    # which the test advanced to expired + 1s.
    assert row.scheduled_at == expired + timedelta(seconds=6)
    assert row.locked_by_worker is None
    assert row.lock_expires_at is None

    events = await memory_jobs.get_events(job_id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert any(
        e.detail["from_state"] == "running" and e.detail["to_state"] == "pending"
        for e in state_changes
    )
