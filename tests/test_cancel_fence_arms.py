"""Fence pins for the consumer deferral arms and the interrupt release arm.

Issue #278: ``mark_snoozed`` and ``mark_retry_after_{true,false}`` reset
``cancel_phase``/``cancel_requested_at`` with no ``cancel_phase = 0`` fence, so a
snooze/retry landing mid-cancel launders an in-flight operator cancel (the
engine of the bulk-cancel double-report: the same id shows up in both the
``cancel_requested`` and ``cancelled_directly`` lists). The arms must refuse a
phase-carrying row the way ``mark_interrupted`` does.

Issue #287: ``mark_interrupted``'s release arm refunds the claim's attempt
increment, re-creating the exact attempt epoch the interrupted (zombie) handler
holds. The zombie's later terminal write then passes the attempt fence and
lands on the re-dispatched attempt, and the live execution's own terminal write
no-ops. The interrupt arm must not refund: the attempt did start executing.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

from taskq._ids import new_job_id
from taskq.backend._protocol import CancelPhase, EnqueueArgs, JobFilter, JobId
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


async def _enqueue_and_dispatch(
    backend: InMemoryBackend,
    queue: str = "default",
) -> tuple[JobId, UUID]:
    if "test_actor" not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend.register_actor_config(actor="test_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue=queue,
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        schedule_to_close=None,
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


def _set_in_flight_cancel(backend: InMemoryBackend, job_id: JobId) -> None:
    """Stamp an operator cancel in flight on the row (phase + timestamp).

    write_cancel_request sets both columns together, so the fence tests
    exercise the pair.
    """
    row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
    backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        row, cancel_phase=CancelPhase.COOPERATIVE, cancel_requested_at=_START
    )


# ── Issue #278: the deferral arms must not launder an in-flight cancel ──


class TestDeferralArmsCarryTheCancelFence:
    """A snooze/retry landing on a phase-carrying row must refuse it.

    The operator's audit columns survive untouched and the row stays
    'running' for the cancel ladder to terminalise; exactly the fence
    mark_interrupted's release arm already carries.
    """

    async def test_mark_snoozed_refuses_in_flight_cancel(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        _set_in_flight_cancel(backend, job_id)

        result = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30), attempt=1)

        assert result == "noop", (
            "a snooze landing mid-cancel must not reschedule the row: the "
            "deferral arm lacks the cancel_phase = 0 fence and launders the "
            "operator's in-flight cancel"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "running"
        assert row.cancel_phase == CancelPhase.COOPERATIVE
        assert row.cancel_requested_at == _START

    async def test_mark_retry_after_consume_true_refuses_in_flight_cancel(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        _set_in_flight_cancel(backend, job_id)

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=True, attempt=1
        )

        assert result == "noop", (
            "a consuming RetryAfter landing mid-cancel must not reschedule "
            "the row behind the operator's cancel request"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "running"
        assert row.cancel_phase == CancelPhase.COOPERATIVE
        assert row.cancel_requested_at == _START

    async def test_mark_retry_after_consume_false_refuses_in_flight_cancel(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        _set_in_flight_cancel(backend, job_id)

        result = await backend.mark_retry_after(
            job_id, wid, timedelta(seconds=10), consume_budget=False, attempt=1
        )

        assert result == "noop", (
            "a non-consuming RetryAfter landing mid-cancel must not reschedule "
            "the row behind the operator's cancel request"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "running"
        assert row.cancel_phase == CancelPhase.COOPERATIVE
        assert row.cancel_requested_at == _START

    async def test_deferral_arms_still_defer_clean_rows(self) -> None:
        """The fence narrows nothing for the clean case: a phase-0 row
        defers as before (the retry-budget semantics are untouched)."""
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        result = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30), attempt=1)
        assert result == "scheduled"
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.cancel_phase == CancelPhase.NONE
        assert row.cancel_requested_at is None


class TestBulkCancelDoubleReport:
    """The issue-thread double-report repro, ported to the mock harness.

    Round 1's running arm cancel-requests the row; a consumer snooze mid-drain
    wipes the phase and parks the row 'pending' behind the keyset cursor;
    round 2's pending arm cancels it directly. The same id appears in BOTH
    result lists; the exactly-once totals are falsifiable. With the fence,
    the snooze cannot land on the phase-carrying row and the id appears in
    exactly one list.
    """

    async def test_snoozed_mid_cancel_id_lands_in_one_list_only(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        # A disjoint pending row proves the drain arms still fire round over
        # round (round 2 is not trivially empty).
        pending_id, _ = await _enqueue_pending(backend)

        # Round 1: the running arm cancel-requests the running row; the
        # pending arm takes the pending row directly.
        first = await backend.cancel_where(_match_all_filter(), reason="bulk-drain")
        assert first.cancel_requested_ids == (job_id,)
        assert first.cancelled_ids == (pending_id,)

        # The consumer snoozes mid-drain. Before the fix this wipes the
        # operator's cancel columns and re-pends the row behind the cursor.
        snoozed = await backend.mark_snoozed(job_id, wid, timedelta(seconds=30), attempt=1)

        # Round 2: the drain re-walks from the cursor.
        second = await backend.cancel_where(_match_all_filter(), reason="bulk-drain")

        reported_twice = (
            (set(first.cancel_requested_ids) & set(first.cancelled_ids))
            | (set(second.cancel_requested_ids) & set(second.cancelled_ids))
            | (set(first.cancel_requested_ids) & set(second.cancelled_ids))
            | (set(first.cancelled_ids) & set(second.cancel_requested_ids))
        )
        assert not reported_twice, (
            f"job {reported_twice} was reported by both the cancel_requested "
            "and cancelled_directly arms across the two drain rounds: the "
            "snooze laundered the in-flight cancel and the double-report is back"
        )
        assert snoozed == "noop", (
            "the mid-cancel snooze must not have landed; the row stays "
            "running with the operator's cancel request intact"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "running"
        assert row.cancel_phase == CancelPhase.COOPERATIVE
        # The disjoint row was already terminal: round 2 reports nothing new.
        assert second.cancelled_ids == ()
        assert second.cancel_requested_ids == ()


async def _enqueue_pending(backend: InMemoryBackend) -> tuple[JobId, None]:
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        schedule_to_close=None,
    )
    await backend.enqueue(args)
    return args.id, None


def _match_all_filter() -> JobFilter:
    # cancel_where ignores limit/cursor/order_by; an empty filter matches
    # every job in the backend.
    return JobFilter()


# ── Issue #287: the interrupt arm's attempt refund re-creates the epoch ──


class TestInterruptArmAttemptEpoch:
    """The zombie-write repro: an interrupt refund re-creates the attempt
    epoch the interrupted handler holds, so its later terminal write lands
    on the re-dispatched attempt and the live execution's write no-ops."""

    async def test_zombie_terminal_write_cannot_land_after_interrupt_reclaim(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        row = await backend.get(job_id)
        assert row is not None and row.attempt == 1

        # Shutdown interrupts the running attempt (zero hold: the row lands
        # 'pending' immediately; the process is going away).
        released = await backend.mark_interrupted(job_id, wid, attempt=1, hold=timedelta(0))
        assert released == "pending"
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "pending"
        # The attempt did start executing: its increment must STAND.
        assert row.attempt == 1, (
            "the interrupt release arm must not refund the claim's attempt "
            "increment: the attempt started executing, and refunding it "
            "re-creates the exact epoch the interrupted (zombie) handler holds"
        )

        # The same worker re-claims: a NEW attempt epoch.
        redispatched = await backend.dispatch_batch(
            wid, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert len(redispatched) == 1
        assert redispatched[0].attempt == 2, (
            "the re-dispatch must advance the attempt epoch past the epoch "
            "the zombie handler still holds (1)"
        )

        # The zombie's terminal write at the old epoch must be fenced out.
        zombie_landed = await backend.mark_succeeded(job_id, wid, {"zombie": True}, attempt=1)
        assert zombie_landed is False, (
            "the interrupted handler's terminal write landed on the "
            "re-dispatched attempt: the interrupt refund re-created the "
            "epoch the zombie holds, and the live execution's own outcome "
            "is now missing from the audit trail"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "running"

        # The live execution's own terminal write lands.
        assert await backend.mark_succeeded(job_id, wid, {"live": True}, attempt=2) is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.result == {"live": True}
