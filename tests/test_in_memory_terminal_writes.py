"""Tests for InMemoryBackend terminal-write side effects.

Covers job_attempts written inside terminal methods (not by
external callers), job_events writes, cancel_phase
preservation on mark_cancelled, cancel-slate reset on
transient retry, mark_abandoned cancel_phase=2
guard, write_cancel_request / write_cancel_escalation
event rows, and WorkerOwnershipMismatch from mark_failed_or_retry.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.backend._protocol import CancelPhase, EnqueueArgs, ErrorInfo, JobId, RetryKind
from taskq.client import JobHandle, JobsClient
from taskq.exceptions import ResultUnavailable, WorkerOwnershipMismatch
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


def _set_cancel_state(backend: InMemoryBackend, job_id: JobId, phase: CancelPhase) -> None:
    """Stamp a full in-flight cancel onto the row: phase + requested-at.

    write_cancel_request sets both columns together, so retry-arm tests
    must exercise the pair, not the phase alone.
    """
    row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
    backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        row, cancel_phase=phase, cancel_requested_at=_START
    )


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
            retry_delay=timedelta(seconds=10),
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


# ── Branch B: cancel slate reset on transient retry ────────────────────


class TestCancelSlateResetOnRetry:
    """Branch B (transient retry) resets the cancel slate: a retry reuses
    the SAME job row, so an escalated cancel that survived the retry write
    would hand the next attempt an already-FORCED phase — the cancel
    controller's fast-advance would then skip cooperative cancel entirely
    and the attempt could never be cancelled again. Both cancel columns
    (phase + requested-at) must come back clean, asserted on the returned
    row and the persisted row (c06ba0e).

    These previously asserted the opposite — that the phase survived the
    retry — the exact behaviour that made a job permanently uncancellable.
    Clearing matches the crash-reclaim sweep (_SWEEP_1_SQL) and
    isolate_self, which have always reset both columns on their retry arm
    for the same reason: "the next dispatch doesn't immediately re-cancel
    the retried job". A caller whose cancel lost the race can still cancel
    the pending/scheduled row, which the cancel path handles directly.
    """

    async def test_cancel_phase1_reset_on_retry(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=3)
        _set_cancel_state(backend, job_id, CancelPhase.COOPERATIVE)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="transient",
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(
            job_id,
            wid,
            error_info,
            retry_delay=timedelta(seconds=10),
        )
        assert result.status == "scheduled"
        assert result.cancel_phase == CancelPhase.NONE
        assert result.cancel_requested_at is None
        assert result.locked_by_worker is None
        assert result.lock_expires_at is None

    async def test_cancel_phase2_reset_on_retry(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend, max_attempts=3)
        _set_cancel_state(backend, job_id, CancelPhase.FORCED)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="transient",
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(
            job_id,
            wid,
            error_info,
            retry_delay=timedelta(seconds=10),
        )
        assert result.status == "scheduled"
        assert result.cancel_phase == CancelPhase.NONE
        assert result.cancel_requested_at is None

        persisted = await backend.get(job_id)
        assert persisted is not None
        assert persisted.cancel_phase == CancelPhase.NONE
        assert persisted.cancel_requested_at is None


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


class TestMarkSucceededResultExpiryFallback:
    """result_expires_at resolution at completion: stored override →
    worker-supplied fallback (the @actor literal) → enqueue-time value,
    each applied from the COMPLETION timestamp, never re-pinned to the
    enqueue one.
    """

    async def _enqueue_with_ttl_and_age(
        self,
        backend: InMemoryBackend,
        clock: FakeClock,
        ttl: timedelta,
        queue_wait: timedelta,
    ) -> tuple[JobId, UUID]:
        """Enqueue with a literal TTL, then age the clock as if the job
        sat in the queue past its TTL before completing."""
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now(),
            result_ttl=ttl,
        )
        await backend.enqueue(args)
        worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert len(dispatched) == 1
        clock.advance(queue_wait)
        return dispatched[0].id, worker_id

    async def test_cleared_stored_ttl_falls_back_to_literal_at_completion(self) -> None:
        """The reported bug: stored result_ttl cleared to NULL, job sat in
        the queue longer than its TTL — the result must NOT complete
        already expired. The worker's fallback literal is applied from
        the completion timestamp, so the result outlives the sweep."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        # Row exists with result_ttl NULL — the operator's cleared override.
        backend.register_actor_configs(
            [ActorConfig(actor="test_actor", max_concurrent=None, queue="default")]
        )

        ttl = timedelta(seconds=5)
        job_id, wid = await self._enqueue_with_ttl_and_age(
            backend, clock, ttl, queue_wait=timedelta(seconds=45)
        )

        ok = await backend.mark_succeeded(job_id, wid, {"ok": True}, fallback_result_ttl=ttl)
        assert ok is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.result == {"ok": True}
        # Completion happened at _START + 45s; expiry must be completion+5s,
        # a future timestamp — not the enqueue-pinned _START+5s (40s past).
        assert row.result_expires_at == clock.now() + ttl

    async def test_stored_ttl_wins_over_fallback_at_completion(self) -> None:
        """Operator override beats the worker's fallback literal."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        backend.register_actor_configs(
            [
                ActorConfig(
                    actor="test_actor",
                    max_concurrent=None,
                    queue="default",
                    result_ttl=300.0,
                )
            ]
        )

        ttl = timedelta(seconds=5)
        job_id, wid = await self._enqueue_with_ttl_and_age(
            backend, clock, ttl, queue_wait=timedelta(seconds=45)
        )

        ok = await backend.mark_succeeded(job_id, wid, {"ok": True}, fallback_result_ttl=ttl)
        assert ok is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.result_expires_at == clock.now() + timedelta(seconds=300)

    async def test_no_stored_no_fallback_keeps_enqueue_pinned_value(self) -> None:
        """Third COALESCE arm: neither a stored override nor a fallback
        literal (e.g. a completing worker that predates the literal) —
        the enqueue-time value is all there is to keep."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)

        ttl = timedelta(seconds=5)
        job_id, wid = await self._enqueue_with_ttl_and_age(
            backend, clock, ttl, queue_wait=timedelta(seconds=45)
        )

        ok = await backend.mark_succeeded(job_id, wid, {"ok": True})
        assert ok is True

        # The pinned expiry is already 40s in the past at completion, so
        # the public get() read reports the post-sweep view (result gone —
        # pinned by TestGetExpiredResults below). This test pins the WRITE:
        # what mark_succeeded kept in storage, read directly.
        stored = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access to pin the stored write; the public get() path now applies read-side result expiry
        assert stored.result_expires_at == _START + ttl

    async def test_run_until_drained_wires_stub_result_ttl_as_fallback(self) -> None:
        """Wiring pin: ``register_stub(result_ttl=...)`` must reach the
        terminal write as ``fallback_result_ttl`` through
        ``run_until_drained`` → ``consume_one_job`` → ``mark_succeeded``.

        Every other test in this class calls ``mark_succeeded`` directly,
        so a dropped kwarg anywhere in the dispatch/runner chain would
        silently restore the complete-already-expired bug with a green
        suite. This job sits 45s in the queue past its 5s TTL: only the
        fallback applied from the completion timestamp saves the result.
        """
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        ttl = timedelta(seconds=5)

        def stub(payload: object, ctx: object) -> dict[str, object]:
            return {"ok": True}

        backend.register_stub("ttl_stub_actor", stub, result_ttl=ttl)

        args = EnqueueArgs(
            id=new_job_id(),
            actor="ttl_stub_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now(),
            result_ttl=ttl,
        )
        await backend.enqueue(args)
        clock.advance(timedelta(seconds=45))

        await backend.run_until_drained()

        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.result == {"ok": True}
        # Completion happened at _START + 45s; the stub's literal applied
        # from completion — not the enqueue-pinned _START + 5s (40s past).
        assert row.result_expires_at == clock.now() + ttl


class _ProbeResult(BaseModel):
    """Result model for the wait()-expiry tests (non-None R)."""

    ok: bool = True


class TestGetExpiredResults:
    """Read-side result TTL: ``get`` / ``JobHandle.wait`` honor the clock.

    PostgresBackend nulls an expired result via the leader's
    ``sweep_expired_results`` (``_SWEEP_RESULT_TTL_SQL``); until that sweep
    fires, a read can still observe the result. The in-memory backend has no
    leader loop, so ``get`` applies the sweep's predicate directly against
    the injected Clock — a read past ``result_expires_at`` returns the exact
    post-sweep row shape (``result`` / ``result_size_bytes`` /
    ``result_expires_at`` all ``None``) while every other column, terminal
    status included, is untouched.

    Write-side resolution (stored row → fallback literal → enqueue pin) is
    pinned by ``TestMarkSucceededResultExpiryFallback``; these tests pin the
    read side end-to-end through ``run_until_drained`` so a stub's
    ``result_ttl`` flows exactly the way a real worker's literal would.
    """

    async def _complete_ttl_job(
        self,
        backend: InMemoryBackend,
        clock: FakeClock,
        *,
        actor: str = "ttl_read_actor",
        stub_result_ttl: timedelta | None = timedelta(seconds=60),
        enqueue_result_ttl: timedelta | None = None,
    ) -> JobId:
        """Register a succeeding stub, enqueue, and drain.

        Leaves the clock parked at the job's completion time, so callers
        control expiry purely by advancing the clock afterwards.
        """
        backend.register_stub(
            actor,
            lambda payload, ctx: {"ok": True},
            result_ttl=stub_result_ttl,
        )
        args = EnqueueArgs(
            id=new_job_id(),
            actor=actor,
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now(),
            result_ttl=enqueue_result_ttl,
        )
        await backend.enqueue(args)
        await backend.run_until_drained()
        return args.id

    async def test_result_present_before_expiry(self) -> None:
        """Before the clock passes result_expires_at, get returns the live
        result with its expiry metadata intact."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        job_id = await self._complete_ttl_job(backend, clock)

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.result == {"ok": True}
        assert row.result_size_bytes is not None
        assert row.result_expires_at == _START + timedelta(seconds=60)

    async def test_result_expired_after_clock_passes_expiry(self) -> None:
        """Past result_expires_at, get reports the post-sweep row shape:
        result, result_size_bytes, and result_expires_at all None."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        job_id = await self._complete_ttl_job(backend, clock)

        clock.advance(timedelta(seconds=61))
        row = await backend.get(job_id)
        assert row is not None
        assert row.result is None
        assert row.result_size_bytes is None
        assert row.result_expires_at is None

    async def test_expired_read_does_not_revive_or_mutate_terminal_state(
        self,
    ) -> None:
        """Expiry drops only the result columns: the terminal status and
        finished_at survive, and repeated reads are stable."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        job_id = await self._complete_ttl_job(backend, clock)
        completed_at = clock.now()

        clock.advance(timedelta(seconds=120))
        first = await backend.get(job_id)
        assert first is not None
        assert first.status == "succeeded"
        assert first.finished_at == completed_at

        second = await backend.get(job_id)
        assert second is not None
        assert second.status == "succeeded"
        assert second.result is None

    async def test_expiry_boundary_is_strictly_less_than(self) -> None:
        """At exactly result_expires_at the result is still available —
        the sweep predicate is ``result_expires_at < now``, matching
        ``_SWEEP_RESULT_TTL_SQL``; one second past it, the result is gone."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        job_id = await self._complete_ttl_job(backend, clock)

        clock.move_to(_START + timedelta(seconds=60))
        at_boundary = await backend.get(job_id)
        assert at_boundary is not None
        assert at_boundary.result == {"ok": True}

        clock.advance(timedelta(seconds=1))
        past_boundary = await backend.get(job_id)
        assert past_boundary is not None
        assert past_boundary.result is None

    async def test_stored_actor_config_ttl_drives_expiry(self) -> None:
        """End-to-end precedence on the read side: the stored actor_config
        row's result_ttl (10s) drives expiry even when the stub's literal
        (300s) would have kept the result alive far longer."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        backend.register_actor_configs(
            [
                ActorConfig(
                    actor="stored_ttl_read_actor",
                    max_concurrent=None,
                    queue="default",
                    result_ttl=10.0,
                )
            ]
        )
        job_id = await self._complete_ttl_job(
            backend,
            clock,
            actor="stored_ttl_read_actor",
            stub_result_ttl=timedelta(seconds=300),
        )

        clock.advance(timedelta(seconds=11))
        row = await backend.get(job_id)
        assert row is not None
        assert row.result is None

    async def test_enqueue_pinned_ttl_in_past_at_completion_reads_expired(
        self,
    ) -> None:
        """Third COALESCE arm on the read side: with neither a stored
        override nor a worker literal, the enqueue-pinned expiry is kept —
        a completion that lands after it reads back already expired, the
        same state the PG sweep would leave behind."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        # Advance the clock so the job completes 45s after enqueue while
        # carrying a 5s enqueue-pinned expiry.
        backend.register_stub("enqueue_pinned_actor", lambda payload, ctx: {"ok": True})
        args = EnqueueArgs(
            id=new_job_id(),
            actor="enqueue_pinned_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now(),
            result_ttl=timedelta(seconds=5),
        )
        await backend.enqueue(args)
        clock.advance(timedelta(seconds=45))
        await backend.run_until_drained()

        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.result is None
        assert row.result_expires_at is None

    async def test_wait_returns_value_before_expiry_and_raises_after(self) -> None:
        """The downstream-facing contract: JobHandle.wait returns R before
        expiry and raises ResultUnavailable once the clock has passed it —
        previously untestable in-memory because get never expired results."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        client = JobsClient(backend)
        job_id = await self._complete_ttl_job(backend, clock)
        row = await backend.get(job_id)
        assert row is not None
        handle: JobHandle[_ProbeResult] = JobHandle(
            client=client,
            row=row,
            result_adapter=TypeAdapter(_ProbeResult),
            was_existing=False,
        )

        value = await handle.wait(timeout=1.0)
        assert value == _ProbeResult(ok=True)

        clock.advance(timedelta(seconds=61))
        with pytest.raises(ResultUnavailable):
            await handle.wait(timeout=1.0)

    async def test_row_without_ttl_is_never_expired_by_reads(self) -> None:
        """Ported from the superseded sweep-based suite: a job with no
        ``result_expires_at`` at all keeps its result on every read,
        however far the clock advances — the predicate's
        ``result_expires_at is not None`` half."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        job_id = await self._complete_ttl_job(backend, clock, stub_result_ttl=None)

        clock.advance(timedelta(hours=24))
        row = await backend.get(job_id)
        assert row is not None
        assert row.result == {"ok": True}
        assert row.result_size_bytes is not None
        assert row.result_expires_at is None

    async def test_none_result_with_passed_ttl_keeps_stale_expiry_on_reads(self) -> None:
        """Ported from the superseded sweep-based suite: a row whose result
        is already ``None`` but whose TTL has passed must NOT have its
        stale ``result_expires_at`` cleared by a read — the predicate's
        ``result is not None`` half, pinning that expiry alone never
        matches (PG's ``AND result IS NOT NULL``)."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        backend.register_stub(
            "null_result_read_actor",
            lambda payload, ctx: None,
            result_ttl=timedelta(seconds=60),
        )
        args = EnqueueArgs(
            id=new_job_id(),
            actor="null_result_read_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now(),
        )
        await backend.enqueue(args)
        await backend.run_until_drained()

        stored = backend._jobs[args.id]  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access pinning the stored write — the public get() path would apply the read-side view.
        assert stored.result is None
        assert stored.result_expires_at == _START + timedelta(seconds=60)

        clock.advance(timedelta(seconds=120))
        row = await backend.get(args.id)
        assert row is not None
        assert row.result is None
        assert row.result_expires_at == _START + timedelta(seconds=60), (
            "the stale expiry must survive reads: expiry alone never matches"
        )

    async def test_expired_read_appends_no_events_and_never_touches_wake(self) -> None:
        """Red-team F11 pin: an expired read is side-effect-free. The PG
        sweep is a bare UPDATE — no ``job_events`` row, no NOTIFY — and a
        read must be stricter still: evaluating the view may never write.
        ``get`` past expiry appends nothing to the event log and pings
        neither a wake nor a cancel-wake subscriber, so a read cannot
        fabricate dispatch work or cancel polling."""
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        job_id = await self._complete_ttl_job(backend, clock)
        events_before = len(backend._events)  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access — the global event count is the side-effect oracle; per-job get_events would miss another job's row.

        async with (
            backend.subscribe_wake() as wake_event,
            backend.subscribe_cancel_wake() as cancel_wake_event,
        ):
            clock.advance(timedelta(seconds=61))
            for _ in range(3):
                row = await backend.get(job_id)
                assert row is not None
                assert row.result is None

            assert len(backend._events) == events_before, (  # pyright: ignore[reportPrivateUsage]  # Why: same oracle as above.
                "an expired read must not append job_events rows"
            )
            assert not wake_event.is_set(), "an expired read must not ping wake subscribers"
            assert not cancel_wake_event.is_set(), (
                "an expired read must not ping cancel-wake subscribers"
            )


# ── read isolation: every get() returns a copy ─────────────────────────


class TestGetReturnsIsolatedRowCopies:
    """Every ``get`` read returns a copy whose dict-typed fields are
    copied, so a caller mutating a freshly-read row cannot corrupt the
    backend's stored state.

    Red-team finding reported during the #92 result-TTL work and
    deliberately left unfixed there: ``JobRow`` is frozen, but its dict
    fields (``payload``, ``progress_state``, ``result``, ``metadata``)
    are shared by reference — and only the expired branch of the read
    path produced a copy. Every non-expired read handed back the live
    stored row, so ``row.result["injected"] = True`` silently rewrote
    storage, and every later read (and the runner's internal paths) saw
    the corruption.
    """

    async def _enqueue_and_succeed(
        self,
        backend: InMemoryBackend,
        *,
        result_ttl: timedelta | None = None,
    ) -> JobId:
        """Enqueue a job carrying dict payload/metadata, dispatch it, and
        succeed it with a dict result and progress_state."""
        args = EnqueueArgs(
            id=new_job_id(),
            actor="read_isolation_actor",
            queue="default",
            payload={"payload_key": "v"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            result_ttl=result_ttl,
            metadata={"meta_key": "v"},
        )
        await backend.enqueue(args)
        worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert len(dispatched) == 1
        ok = await backend.mark_succeeded(
            dispatched[0].id,
            worker_id,
            result={"ok": True},
            progress_seq=2,
            progress_state={"pct": 100},
        )
        assert ok is True
        return dispatched[0].id

    async def test_mutating_non_expired_read_leaves_stored_state_intact(self) -> None:
        """The defect branch: a fresh (non-expired) read used to hand
        back the live stored row, so mutating its dict fields rewrote
        storage — the next read saw the injected keys."""
        backend = _make_backend()
        job_id = await self._enqueue_and_succeed(backend)

        read = await backend.get(job_id)
        assert read is not None
        assert read.result is not None
        read.result["injected"] = True
        read.progress_state["injected"] = True
        read.metadata["injected"] = True
        read.payload["injected"] = True

        fresh = await backend.get(job_id)
        assert fresh is not None
        assert fresh.result == {"ok": True}
        assert fresh.progress_state == {"pct": 100}
        assert fresh.metadata == {"meta_key": "v"}
        assert fresh.payload == {"payload_key": "v"}

        stored = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: pin the stored row directly — the public read is the code under test
        assert stored.result == {"ok": True}
        assert stored.progress_state == {"pct": 100}
        assert stored.metadata == {"meta_key": "v"}
        assert stored.payload == {"payload_key": "v"}

    async def test_expired_read_view_composes_with_the_row_copy(self) -> None:
        """The expired branch keeps its post-sweep shape (result columns
        nulled, every other column intact) and its remaining dict fields
        are copies too — the old ``replace`` view copied only the row
        shell, leaving ``payload``/``progress_state``/``metadata``
        aliased to storage."""
        backend = _make_backend()
        job_id = await self._enqueue_and_succeed(backend, result_ttl=timedelta(seconds=60))

        backend.advance_clock_to(_START + timedelta(seconds=61))
        expired = await backend.get(job_id)
        assert expired is not None
        assert expired.status == "succeeded"
        assert expired.result is None
        assert expired.result_size_bytes is None
        assert expired.result_expires_at is None

        expired.progress_state["injected"] = True
        expired.metadata["injected"] = True
        expired.payload["injected"] = True

        stored = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: pin the stored row directly — the expired view must never become a write
        assert stored.result == {"ok": True}
        assert stored.result_expires_at == _START + timedelta(seconds=60)
        assert stored.progress_state == {"pct": 100}
        assert stored.metadata == {"meta_key": "v"}
        assert stored.payload == {"payload_key": "v"}
