"""Tests for InMemoryBackend terminal-write side effects.

Covers job_attempts written inside terminal methods (not by
external callers), job_events writes, cancel_phase
preservation on mark_cancelled, mark_abandoned cancel_phase=2
guard, write_cancel_request / write_cancel_escalation
event rows, and WorkerOwnershipMismatch from mark_failed_or_retry.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId, RetryKind
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend(
    cancellation_grace: timedelta = timedelta(seconds=30),
    cleanup_grace: timedelta = timedelta(seconds=30),
) -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=cancellation_grace,
        cleanup_grace_period=cleanup_grace,
    )


async def _enqueue_and_dispatch(
    backend: InMemoryBackend,
    actor: str = "test_actor",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    schedule_to_close: datetime | None = None,
) -> tuple[JobId, UUID]:
    """Enqueue a job and dispatch it, returning (job_id, worker_id)."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={"key": "value"},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        scheduled_at=_START,
        schedule_to_close=schedule_to_close,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
    dispatched = await backend.dispatch_batch(
        worker_id,
        [queue],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    return dispatched[0].id, worker_id


def _set_cancel_phase(backend: InMemoryBackend, job_id: JobId, phase: int) -> None:
    row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
    backend._jobs[job_id] = replace(row, cancel_phase=phase)  # type: ignore[reportPrivateUsage]  # Why: test-only private access


# ── terminal write idempotency + single attempt/event ──────────


class TestTerminalWriteIdempotency:
    """enqueue → dispatch → mark_succeeded returns True; second call
    returns False; exactly one AttemptRow and one EventRow.
    """

    async def test_mark_succeeded_single_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        r1 = await backend.mark_succeeded(job_id, wid, result={"ok": True})
        assert r1 is True

        r2 = await backend.mark_succeeded(job_id, wid, result=None)
        assert r2 is False

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "succeeded"

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[1].detail["from_state"] == "running"
        assert state_changes[1].detail["to_state"] == "succeeded"

    async def test_mark_failed_or_retry_terminal_single_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=1)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(job_id, wid, error_info, None)
        assert result.status == "failed"

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"
        assert attempts[0].error_class == "ValueError"

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[1].detail["to_state"] == "failed"
        assert state_changes[1].detail["error_class"] == "ValueError"

    async def test_mark_cancelled_single_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        r1 = await backend.mark_cancelled(job_id, wid)
        assert r1 is True

        r2 = await backend.mark_cancelled(job_id, wid)
        assert r2 is False

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "cancelled"

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[1].detail["to_state"] == "cancelled"


# ── ownership mismatch ─────────────────────────────────────────


class TestOwnershipMismatch:
    """dispatch with worker_A; call mark_succeeded with worker_B → False.
    mark_failed_or_retry with wrong worker raises WorkerOwnershipMismatch.
    """

    async def test_bool_returning_wrong_worker_returns_false(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        wrong_worker = new_uuid()

        assert await backend.mark_succeeded(job_id, wrong_worker, None) is False
        assert await backend.mark_cancelled(job_id, wrong_worker) is False

        assert await backend.mark_snoozed(job_id, wrong_worker, timedelta(seconds=30)) == "noop"

    async def test_mark_failed_or_retry_wrong_worker_raises(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        wrong_worker = new_uuid()

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(job_id, wrong_worker, error_info, None)

    async def test_mark_failed_or_retry_already_terminal_raises(self) -> None:
        """Already-terminal raises WorkerOwnershipMismatch (PG rowcount=0)."""
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        await backend.mark_succeeded(job_id, wid, None)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch) as exc_info:
            await backend.mark_failed_or_retry(job_id, wid, error_info, None)
        assert exc_info.value.job_id == job_id
        assert exc_info.value.expected == wid
        assert exc_info.value.actual == wid


# ── mark_failed_or_retry on terminal states raises ────────────


class TestMarkFailedOrRetryOnTerminalStatesRaises:
    """mark_failed_or_retry raises WorkerOwnershipMismatch on every
    terminal status (succeeded, failed, cancelled, crashed, abandoned).
    """

    @pytest.mark.parametrize(
        "terminal_status", ["succeeded", "failed", "cancelled", "crashed", "abandoned"]
    )
    async def test_terminal_status_raises(self, terminal_status: str) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            row,
            status=terminal_status,
            locked_by_worker=None,
            lock_expires_at=None,
            finished_at=_START,
        )

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch) as exc_info:
            await backend.mark_failed_or_retry(job_id, wid, error_info, None)
        assert exc_info.value.job_id == job_id
        assert exc_info.value.expected == wid
        assert exc_info.value.actual is None


# ── every terminal/snooze writes exactly one AttemptRow ────────


class TestSingleAttemptRowPerTransition:
    """every terminal/snooze transition writes exactly one AttemptRow
    and the appropriate EventRow(s).  write_cancel_request on running writes
    no AttemptRow, exactly one EventRow with kind='cancel_request'.
    """

    async def test_mark_succeeded_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        await backend.mark_succeeded(job_id, wid, None)

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "succeeded"

        events = await backend.get_events(job_id)
        assert len(events) == 2
        assert events[1].kind == "state_change"

    async def test_mark_failed_terminal_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=1)
        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        await backend.mark_failed_or_retry(job_id, wid, error_info, None)

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"

        events = await backend.get_events(job_id)
        assert len(events) == 2
        assert events[1].kind == "state_change"

    async def test_mark_failed_retry_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=3)
        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="transient",
            error_traceback=None,
        )
        await backend.mark_failed_or_retry(
            job_id,
            wid,
            error_info,
            next_scheduled_at=_START + timedelta(seconds=10),
        )

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"

        events = await backend.get_events(job_id)
        assert len(events) == 2
        assert events[1].kind == "state_change"
        assert events[1].detail["to_state"] == "scheduled"

    async def test_mark_cancelled_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        await backend.mark_cancelled(job_id, wid)

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "cancelled"

        events = await backend.get_events(job_id)
        assert len(events) == 2
        assert events[1].kind == "state_change"

    async def test_mark_snoozed_attempt_and_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "snoozed"

        events = await backend.get_events(job_id)
        assert len(events) == 2
        assert events[1].kind == "state_change"

    async def test_write_cancel_request_running_no_attempt_one_event(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        await backend.write_cancel_request(job_id, "test reason")

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 0

        events = await backend.get_events(job_id)
        assert len(events) == 2
        cancel_events = [e for e in events if e.kind == "cancel_request"]
        assert len(cancel_events) == 1
        assert cancel_events[0].detail["reason"] == "test reason"


# ── mark_snoozed idempotency and metadata merge ───────────────


class TestMarkSnoozedIdempotencyAndMetadataMerge:
    """mark_snoozed idempotency, metadata merge, AttemptRow outcome."""

    async def test_first_call_true_second_false(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        delay = timedelta(seconds=30)
        r1 = await backend.mark_snoozed(job_id, wid, delay)
        assert r1 == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.locked_by_worker is None

        r2 = await backend.mark_snoozed(job_id, wid, delay)
        assert r2 == "noop"

    async def test_metadata_update_merges(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        delay = timedelta(seconds=30)
        await backend.mark_snoozed(
            job_id,
            wid,
            delay,
            metadata_update={"a": 1},
        )

        row = await backend.get(job_id)
        assert row is not None
        assert row.metadata["a"] == 1

    async def test_metadata_update_none_preserves_existing(self) -> None:
        """metadata_update=None preserves existing metadata (COALESCE behaviour)."""
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        delay = timedelta(seconds=30)
        await backend.mark_snoozed(
            job_id,
            wid,
            delay,
            metadata_update={"existing": 42},
        )

        # Snooze again won't work (already scheduled), but test the
        # metadata_update=None path with a fresh job
        job_id2, wid2 = await _enqueue_and_dispatch(backend)
        # Set some metadata on the row first
        row = backend._jobs[job_id2]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id2] = replace(row, metadata={"pre": "existing"})  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        await backend.mark_snoozed(job_id2, wid2, delay, metadata_update=None)
        row2 = await backend.get(job_id2)
        assert row2 is not None
        assert row2.metadata == {"pre": "existing"}

    async def test_snoozed_attempt_outcome(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        delay = timedelta(seconds=30)
        await backend.mark_snoozed(job_id, wid, delay)

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "snoozed"


# ── mark_abandoned idempotency with cancel_phase=2 guard ──────


class TestMarkAbandonedCancelPhaseGuard:
    """mark_abandoned idempotency with cancel_phase=2 guard."""

    async def test_cancel_phase2_succeeds(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        _set_cancel_phase(backend, job_id, 2)

        r1 = await backend.mark_abandoned(job_id)
        assert r1 is True

        r2 = await backend.mark_abandoned(job_id)
        assert r2 is False  # status now 'abandoned'

    async def test_cancel_phase1_fails(self) -> None:
        """cancel_phase=1 on a running row → predicate miss."""
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        _set_cancel_phase(backend, job_id, 1)

        result = await backend.mark_abandoned(job_id)
        assert result is False

    async def test_cancel_phase0_fails(self) -> None:
        """cancel_phase=0 on a running row → predicate miss."""
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        # cancel_phase is already 0 by default

        result = await backend.mark_abandoned(job_id)
        assert result is False

    async def test_abandoned_attempt_outcome_is_cancelled(self) -> None:
        """Abandoned writes outcome='cancelled', not 'abandoned'."""
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        _set_cancel_phase(backend, job_id, 2)

        await backend.mark_abandoned(job_id)

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "cancelled"

    async def test_abandoned_event_row(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        _set_cancel_phase(backend, job_id, 2)

        await backend.mark_abandoned(job_id)

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[1].detail["from_state"] == "running"
        assert state_changes[1].detail["to_state"] == "abandoned"


# ── wrong worker_id handling across all terminal methods ───────


class TestWrongWorkerIdHandling:
    """every bool-returning terminal write returns False (not raise)
       when called on a running row with the wrong worker_id, EXCEPT
    mark_failed_or_retry which raises WorkerOwnershipMismatch per
    """

    async def test_mark_succeeded_wrong_worker_false(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        assert await backend.mark_succeeded(job_id, new_uuid(), None) is False

    async def test_mark_cancelled_wrong_worker_false(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        assert await backend.mark_cancelled(job_id, new_uuid()) is False

    async def test_mark_snoozed_wrong_worker_false(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        assert await backend.mark_snoozed(job_id, new_uuid(), timedelta(seconds=30)) == "noop"

    async def test_mark_failed_or_retry_wrong_worker_raises(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(job_id, new_uuid(), error_info, None)


# ── mark_cancelled preserves cancel_phase ─────────────────────


class TestMarkCancelledPreservesCancelPhase:
    """mark_cancelled on a running row with cancel_phase=1
    leaves cancel_phase=1 on the cancelled row.  Same with cancel_phase=2.
    """

    async def test_cancel_phase_preserved_on_mark_cancelled_phase1(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        _set_cancel_phase(backend, job_id, 1)

        await backend.mark_cancelled(job_id, wid)
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.cancel_phase == 1

    async def test_cancel_phase_preserved_on_mark_cancelled_phase2(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        _set_cancel_phase(backend, job_id, 2)

        await backend.mark_cancelled(job_id, wid)
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.cancel_phase == 2


# ── PayloadValidationError through mark_failed_or_retry ────────


class TestPayloadValidationErrorThroughMarkFailedOrRetry:
    """PayloadValidationError path through mark_failed_or_retry.
    Verifies the write surface — the non-retryable classifier.
    """

    async def test_payload_validation_error_terminal_failure(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=1)

        raw_payload = {"bad": "data"}
        error_info = ErrorInfo(
            error_class="PayloadValidationError",
            error_message=str(raw_payload),
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(job_id, wid, error_info, None)

        assert result.status == "failed"
        assert result.error_class == "PayloadValidationError"
        assert str(raw_payload) in (result.error_message or "")

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"
        assert attempts[0].error_class == "PayloadValidationError"

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[1].detail["to_state"] == "failed"
        assert state_changes[1].detail["error_class"] == "PayloadValidationError"


# ── write_cancel_request on pending/scheduled ──────────────────


class TestWriteCancelRequestOnPendingScheduled:
    """write_cancel_request on a pending job: status becomes
    'cancelled', finished_at is set, no AttemptRow, two EventRows
    (one state_change, one cancel_request).  Same for scheduled.
    """

    async def test_pending_cancel_no_attempt_two_events(self) -> None:
        backend = _make_backend()
        args = EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="q",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
        row = await backend.enqueue(args)
        assert row.status == "pending"

        result = await backend.write_cancel_request(row.id, "user request")
        assert result is True

        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "cancelled"
        assert updated.finished_at is not None

        attempts = await backend.get_attempts(row.id)
        assert len(attempts) == 0

        events = await backend.get_events(row.id)
        assert len(events) == 2
        kinds = [e.kind for e in events]
        assert "state_change" in kinds
        assert "cancel_request" in kinds

        state_event = next(e for e in events if e.kind == "state_change")
        assert state_event.detail["from_state"] == "pending"
        assert state_event.detail["to_state"] == "cancelled"

        cancel_event = next(e for e in events if e.kind == "cancel_request")
        assert cancel_event.detail["reason"] == "user request"

    async def test_scheduled_cancel_no_attempt_two_events(self) -> None:
        backend = _make_backend()
        future = _START + timedelta(hours=1)
        args = EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="q",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=future,
        )
        row = await backend.enqueue(args)
        assert row.status == "scheduled"

        result = await backend.write_cancel_request(row.id, None)
        assert result is True

        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "cancelled"

        attempts = await backend.get_attempts(row.id)
        assert len(attempts) == 0

        events = await backend.get_events(row.id)
        assert len(events) == 2
        kinds = [e.kind for e in events]
        assert "state_change" in kinds
        assert "cancel_request" in kinds


# ── write_cancel_request on running ────────────────────────────


class TestWriteCancelRequestOnRunning:
    """write_cancel_request on a running job:
    cancel_requested_at set, cancel_phase==1, one EventRow (cancel_request).
    Second call returns False, no duplicate EventRow.
    """

    async def test_running_cancel_sets_phase1(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)

        r1 = await backend.write_cancel_request(job_id, "please stop")
        assert r1 is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_requested_at is not None
        assert row.cancel_phase == 1
        assert row.status == "running"

        events = await backend.get_events(job_id)
        assert len(events) == 2
        cancel_events = [e for e in events if e.kind == "cancel_request"]
        assert len(cancel_events) == 1
        assert cancel_events[0].detail["reason"] == "please stop"

    async def test_second_cancel_returns_false_no_duplicate_event(self) -> None:
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)

        await backend.write_cancel_request(job_id, None)
        r2 = await backend.write_cancel_request(job_id, None)
        assert r2 is False

        events = await backend.get_events(job_id)
        assert len(events) == 2  # dispatch event + cancel_request (no duplicate)


# ── write_cancel_escalation events ────────────────────────────


class TestWriteCancelEscalationEvents:
    """write_cancel_escalation(phase=1) raises ValueError.
    write_cancel_escalation(phase=2) after setting cancel_phase=1
    returns True; cancel_phase is now 2; one EventRow written with
    cancel_phase_from=1, cancel_phase_to=2.
    """

    async def test_phase1_raises_valueerror(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        with pytest.raises(ValueError, match="phase=2"):
            await backend.write_cancel_escalation(job_id, wid, 1)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright

    async def test_phase2_writes_event(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        # Set cancel_phase=1 first
        await backend.write_cancel_request(job_id, None)

        r = await backend.write_cancel_escalation(job_id, wid, 2)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright
        assert r is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == 2

        events = await backend.get_events(job_id)
        # dispatch wrote one, write_cancel_request wrote one, write_cancel_escalation wrote one
        assert len(events) == 3

        escalation_events = [
            e for e in events if e.kind == "state_change" and "cancel_phase_from" in e.detail
        ]
        assert len(escalation_events) == 1
        assert escalation_events[0].detail["cancel_phase_from"] == 1
        assert escalation_events[0].detail["cancel_phase_to"] == 2
        assert escalation_events[0].detail["from_state"] == "running"
        assert escalation_events[0].detail["to_state"] == "running"

    async def test_phase2_requires_phase1_precondition(self) -> None:
        """write_cancel_escalation(phase=2) on a cancel_phase=0 row is a no-op.

        The SQL WHERE clause requires cancel_phase=1. cancel_phase=0 means
        no cancel request has been issued yet — escalating directly to phase=2
        would skip the cooperative grace window. The predicate MUST reject this.
        """
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        # cancel_phase is 0 at dispatch — no write_cancel_request call

        r = await backend.write_cancel_escalation(job_id, wid, 2)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright
        assert r is False  # predicate miss: cancel_phase != 1

        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == 0  # unchanged


# ── Branch B: cancel_phase preserved on transient retry ───────────────


class TestCancelPhasePreservedOnRetry:
    """cancel_phase is preserved on Branch B (transient retry)."""

    async def test_cancel_phase1_survives_retry(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=3)
        _set_cancel_phase(backend, job_id, 1)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="transient",
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(
            job_id,
            wid,
            error_info,
            next_scheduled_at=_START + timedelta(seconds=10),
        )
        assert result.status == "scheduled"
        assert result.cancel_phase == 1
        assert result.locked_by_worker is None
        assert result.lock_expires_at is None

    async def test_cancel_phase2_survives_retry(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=3)
        _set_cancel_phase(backend, job_id, 2)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="transient",
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(
            job_id,
            wid,
            error_info,
            next_scheduled_at=_START + timedelta(seconds=10),
        )
        assert result.status == "scheduled"
        assert result.cancel_phase == 2


# ── progress_seq / progress_state plumb-through ───────────────────────


class TestProgressFieldsOnTerminalWrites:
    """progress_seq and progress_state are applied to the row on
    terminal writes.
    """

    async def test_mark_succeeded_applies_progress(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        await backend.mark_succeeded(
            job_id,
            wid,
            result={"ok": True},
            progress_seq=5,
            progress_state={"pct": 100},
        )

        row = await backend.get(job_id)
        assert row is not None
        assert row.progress_seq == 5
        assert row.progress_state == {"pct": 100}

    async def test_mark_failed_applies_progress(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=1)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        await backend.mark_failed_or_retry(
            job_id,
            wid,
            error_info,
            None,
            progress_seq=3,
            progress_state={"step": "failed"},
        )

        row = await backend.get(job_id)
        assert row is not None
        assert row.progress_seq == 3
        assert row.progress_state == {"step": "failed"}

    async def test_progress_state_none_preserves_existing(self) -> None:
        """progress_state=None preserves the existing row value."""
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        # Set some progress_state first
        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = replace(row, progress_state={"existing": True})  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        await backend.mark_succeeded(
            job_id,
            wid,
            None,
            progress_seq=1,
            progress_state=None,
        )

        updated = await backend.get(job_id)
        assert updated is not None
        assert updated.progress_state == {"existing": True}


# ── Regression: tick_cancel_polling escalation must write EventRow ────


class TestTickCancelPollingEscalationEvent:
    """Regression: tick_cancel_polling phase-2 escalation must delegate to
    write_cancel_escalation so an EventRow is written.  Previously the
    escalation wrote directly to _jobs, skipping the event log.
    """

    async def test_escalation_via_tick_writes_event_row(self) -> None:
        backend = _make_backend(
            cancellation_grace=timedelta(seconds=10),
            cleanup_grace=timedelta(seconds=10),
        )
        job_id, _wid = await _enqueue_and_dispatch(backend)

        # Request cancellation → cancel_phase=1
        await backend.write_cancel_request(job_id, "test")

        # Register cancel event so tick_cancel_polling can observe
        import asyncio

        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        # First tick: observe cancel_phase=1, record _cancel_observed_at
        await backend.tick_cancel_polling()

        # Advance clock past cancellation grace period
        backend.advance_clock_to(_START + timedelta(seconds=15))

        # Second tick: escalate to phase 2
        await backend.tick_cancel_polling()

        # Verify cancel_phase is now 2
        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == 2

        # Verify the escalation EventRow was written
        events = await backend.get_events(job_id)
        escalation_events = [
            e
            for e in events
            if e.kind == "state_change"
            and e.detail.get("cancel_phase_from") == 1
            and e.detail.get("cancel_phase_to") == 2
        ]
        assert len(escalation_events) == 1
        assert escalation_events[0].detail["from_state"] == "running"
        assert escalation_events[0].detail["to_state"] == "running"

    async def test_abandonment_via_tick_writes_attempt_and_event(self) -> None:
        """tick_cancel_polling → mark_abandoned path writes both AttemptRow
        and EventRow (already covered by mark_abandoned tests, but this
        verifies the tick-driven path end-to-end).
        """
        backend = _make_backend(
            cancellation_grace=timedelta(seconds=10),
            cleanup_grace=timedelta(seconds=10),
        )
        job_id, _wid = await _enqueue_and_dispatch(backend)

        # Request cancellation → cancel_phase=1
        await backend.write_cancel_request(job_id, "test")

        import asyncio

        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        # Observe cancel_phase=1
        await backend.tick_cancel_polling()

        # Advance past cancellation grace → escalate to phase 2
        backend.advance_clock_to(_START + timedelta(seconds=15))
        await backend.tick_cancel_polling()

        # Advance past both grace periods → mark abandoned
        backend.advance_clock_to(_START + timedelta(seconds=25))
        await backend.tick_cancel_polling()

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "abandoned"

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "cancelled"

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        # Two events: cancel_escalation (running→running) and abandonment (running→abandoned)
        to_states = [e.detail["to_state"] for e in state_changes]
        assert "running" in to_states  # escalation
        assert "abandoned" in to_states  # abandonment


# ── G-1: mark_snoozed leaves attempt untouched ─────────────────────────


class TestSnoozePreservesAttempt:
    """G-1: mark_snoozed leaves attempt untouched (§6.2); dispatch
    unconditionally increments, so the round-trip count reflects dispatch
    cycles, not snooze cycles.
    """

    async def test_in_memory_snooze_preserves_attempt(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        row = await backend.get(job_id)
        assert row is not None
        assert row.attempt == 1

        result = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.attempt == 1


# ── G-6: mark_snoozed clears last_heartbeat_at ──────────────────────────


class TestSnoozeClearsLastHeartbeatAt:
    """G-6: mark_snoozed clears last_heartbeat_at on both arms (
    §6.2 lines 1517-1521).
    """

    async def test_in_memory_snooze_clears_last_heartbeat_at(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        row = await backend.get(job_id)
        assert row is not None
        assert row.last_heartbeat_at is not None

        result = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.last_heartbeat_at is None


# ── : snooze-past-deadline guard ────────────────────────────────────


class TestSnoozePastDeadline:
    """when schedule_to_close is set and the snooze delay would
    exceed it, the row transitions to failed instead of scheduled.
    """

    async def test_in_memory_snooze_past_deadline_fails(self) -> None:
        backend = _make_backend()
        deadline = _START + timedelta(seconds=5)
        job_id, wid = await _enqueue_and_dispatch(
            backend,
            schedule_to_close=deadline,
        )

        result = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))
        assert result == "failed"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"
        assert row.error_message == "schedule_to_close reached before next dispatch"
        assert row.locked_by_worker is None
        assert row.lock_expires_at is None
        assert row.last_heartbeat_at is None

    async def test_in_memory_snooze_within_deadline_succeeds(self) -> None:
        backend = _make_backend()
        deadline = _START + timedelta(seconds=30)
        job_id, wid = await _enqueue_and_dispatch(
            backend,
            schedule_to_close=deadline,
        )

        result = await backend.mark_snoozed(job_id, wid, timedelta(seconds=5))
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"

    async def test_in_memory_snooze_no_deadline_succeeds(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        result = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))
        assert result == "scheduled"

    async def test_snooze_past_deadline_consumer_side_attempt_shape(self) -> None:
        backend = _make_backend()
        deadline = _START + timedelta(seconds=5)
        job_id, wid = await _enqueue_and_dispatch(
            backend,
            schedule_to_close=deadline,
        )

        await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"
        assert attempts[0].error_class == "DeadlineExceeded"
        assert attempts[0].error_message == "schedule_to_close reached before next dispatch"
        assert attempts[0].started_at is not None
        assert attempts[0].worker_id is not None
        assert attempts[0].worker_id == wid

    async def test_in_memory_snooze_past_deadline_event_row(self) -> None:
        backend = _make_backend()
        deadline = _START + timedelta(seconds=5)
        job_id, wid = await _enqueue_and_dispatch(
            backend,
            schedule_to_close=deadline,
        )

        await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[1].detail["from_state"] == "running"
        assert state_changes[1].detail["to_state"] == "failed"
        assert state_changes[1].detail["error_class"] == "DeadlineExceeded"


# ── G-5: mark_snoozed outcome parameter ─────────────────────────────────


class TestSnoozeOutcomeParameter:
    """G-5: mark_snoozed accepts an outcome parameter (default "snoozed").
    The ReservationUnavailable handler passes outcome="reservation_denied".
    """

    async def test_in_memory_snooze_outcome_reservation_denied(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        result = await backend.mark_snoozed(
            job_id,
            wid,
            timedelta(seconds=30),
            metadata_update={"awaiting": "reservation:gpu_pool"},
            outcome="reservation_denied",
        )
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.metadata.get("awaiting") == "reservation:gpu_pool"

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "reservation_denied"


# ── Idempotent noop on second mark_snoozed call ─────────────────────────


class TestSnoozeIdempotentNoop:
    """Second mark_snoozed call on an already-moved row returns "noop"."""

    async def test_in_memory_snooze_idempotent_returns_noop(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        result1 = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))
        assert result1 == "scheduled"

        result2 = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30))
        assert result2 == "noop"

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1


# ── mark_retry_after ───────────────────────────────────────────────────


class TestMarkRetryAfterConsumeTrueIncrements:
    """consume_budget=True with budget remaining: attempt unchanged
    (dispatch CTE is the sole increment point), status='scheduled',
    attempt-row outcome='snoozed', error_class='RetryAfter'.
    Returns "scheduled".
    """

    async def test_in_memory_mark_retry_after_consume_true_increments(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        row = await backend.get(job_id)
        assert row is not None
        assert row.attempt == 1

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=True
        )
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.attempt == 1
        assert row.scheduled_at == _START + timedelta(seconds=10)
        assert row.locked_by_worker is None
        assert row.lock_expires_at is None
        assert row.last_heartbeat_at is None

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "snoozed"
        assert attempts[0].error_class == "RetryAfter"

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[0].detail["from_state"] == "pending"
        assert state_changes[0].detail["to_state"] == "running"
        assert state_changes[1].detail["from_state"] == "running"
        assert state_changes[1].detail["to_state"] == "scheduled"


class TestMarkRetryAfterConsumeFalsePreserves:
    """consume_budget=False: attempt unchanged, status='scheduled'.
    Returns "scheduled".
    """

    async def test_in_memory_mark_retry_after_consume_false_preserves(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        row = await backend.get(job_id)
        assert row is not None
        assert row.attempt == 1

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=False
        )
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.attempt == 1
        assert row.scheduled_at == _START + timedelta(seconds=10)

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "snoozed"
        assert attempts[0].error_class == "RetryAfter"


class TestMarkRetryAfterMaxAttemptsFails:
    """With max_attempts=3, retry_kind='transient', attempt=3:
    mark_retry_after(consume_budget=True) → 'failed:MaxAttemptsExceeded',
    error_class='MaxAttemptsExceeded'.
    """

    async def test_in_memory_mark_retry_after_max_attempts_fails(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=3, retry_kind="transient")

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = replace(row, attempt=3)  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=True
        )
        assert result == "failed:MaxAttemptsExceeded"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "MaxAttemptsExceeded"
        assert row.error_message == "retry budget exhausted"
        assert row.attempt == 3
        assert row.last_heartbeat_at is None
        assert row.locked_by_worker is None
        assert row.lock_expires_at is None

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"
        assert attempts[0].error_class == "MaxAttemptsExceeded"
        assert attempts[0].error_message == "retry budget exhausted"

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[0].detail["from_state"] == "pending"
        assert state_changes[0].detail["to_state"] == "running"
        assert state_changes[1].detail["from_state"] == "running"
        assert state_changes[1].detail["to_state"] == "failed"
        assert state_changes[1].detail["error_class"] == "MaxAttemptsExceeded"


class TestMarkRetryAfterIndefiniteTierIgnoresMaxAttempts:
    """With retry_kind='indefinite', even when attempt + 1 > max_attempts,
    the row goes to 'scheduled', not 'failed'.
    """

    async def test_in_memory_mark_retry_after_indefinite_tier_ignores_max_attempts(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=3, retry_kind="indefinite")

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = replace(row, attempt=5)  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=True
        )
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.attempt == 5


class TestMarkRetryAfterPastDeadlineFails:
    """schedule_to_close = now() + 5s, delay=30s → 'failed:DeadlineExceeded',
    error_class='DeadlineExceeded'.
    """

    async def test_retry_after_past_deadline_consumer_side_fails(self) -> None:
        backend = _make_backend()
        deadline = _START + timedelta(seconds=5)
        job_id, wid = await _enqueue_and_dispatch(backend, schedule_to_close=deadline)

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=30), consume_budget=True
        )
        assert result == "failed:DeadlineExceeded"

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"
        assert row.error_message == "schedule_to_close reached before next dispatch"
        assert row.last_heartbeat_at is None
        assert row.locked_by_worker is None
        assert row.lock_expires_at is None

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"
        assert attempts[0].error_class == "DeadlineExceeded"
        assert attempts[0].error_message == "schedule_to_close reached before next dispatch"
        assert attempts[0].started_at is not None
        assert attempts[0].worker_id is not None
        assert attempts[0].worker_id == wid

        events = await backend.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 2
        assert state_changes[0].detail["from_state"] == "pending"
        assert state_changes[0].detail["to_state"] == "running"
        assert state_changes[1].detail["from_state"] == "running"
        assert state_changes[1].detail["to_state"] == "failed"
        assert state_changes[1].detail["error_class"] == "DeadlineExceeded"


class TestMarkRetryAfterIdempotentNoop:
    """Second call returns "noop", no second attempt row."""

    async def test_in_memory_mark_retry_after_idempotent_noop(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        result1 = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=True
        )
        assert result1 == "scheduled"

        result2 = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=True
        )
        assert result2 == "noop"

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1


class TestMarkRetryAfterClearsLastHeartbeat:
    """last_heartbeat_at is cleared on every transition."""

    async def test_in_memory_mark_retry_after_clears_last_heartbeat(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        row = await backend.get(job_id)
        assert row is not None
        assert row.last_heartbeat_at is not None

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=True
        )
        assert result == "scheduled"

        row = await backend.get(job_id)
        assert row is not None
        assert row.last_heartbeat_at is None
