"""Fence pins for the consumer deferral arms, the failure-retry arm, and
the interrupt release arm.

Issue: ``mark_snoozed`` and ``mark_retry_after_{true,false}`` reset
``cancel_phase``/``cancel_requested_at`` with no ``cancel_phase = 0`` fence, so a
snooze/retry landing mid-cancel launders an in-flight operator cancel (the
engine of the bulk-cancel double-report: the same id shows up in both the
``cancel_requested`` and ``cancelled_directly`` lists). The arms must refuse a
phase-carrying row the way ``mark_interrupted`` does.

Issue: ``mark_failed_or_retry``'s retry arm resets the same columns with the
same missing fence, so a retryable actor failure landing during the
cooperative cancel grace wipes the operator's acknowledged cancel, disarms
the phase-2 escalation, and the actor runs again. The arm must refuse a
phase-carrying row like its siblings.

Issue: ``mark_interrupted``'s release arm refunds the claim's attempt
increment, re-creating the exact attempt epoch the interrupted (zombie) handler
holds. The zombie's later terminal write then passes the attempt fence and
lands on the re-dispatched attempt, and the live execution's own terminal write
no-ops. The interrupt arm must not refund: the attempt did start executing.

Issue: the fused deferral statements' deadline arm had no cancel-first
arbitration, so a row carrying a cancel phase whose ``schedule_to_close``
lapsed at deferral time terminalised ``failed:DeadlineExceeded`` instead of
``cancelled``: operator intent lost to a lapsed clock, and the deadline
hooks fired on a cancel in flight. The deadline arm must order the cancel
arm first, the same arbitration ``_SWEEP_1_SQL``'s CASE carries.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import CancelPhase, ErrorInfo, JobFilter, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.constants import CANCEL_ORIGIN_COOPERATIVE, CANCEL_ORIGIN_FORCED
from taskq.exceptions import WorkerOwnershipMismatch
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


# ── Issue: the deferral arms must not launder an in-flight cancel ──


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


# ── Issue: the failure-retry arm must not launder an in-flight cancel ──


class TestFailureRetryArmCarriesTheCancelFence:
    """``mark_failed_or_retry``'s retry arm resets both cancel columns, so
    a retryable failure landing during the cooperative grace used to wipe
    the operator's acknowledged cancel, disarm the phase-2 escalation, and
    reschedule the job behind the operator's back. The arm carries the
    same ``cancel_phase = 0`` fence the deferral arms and the interrupt
    release carry: a phase-carrying row is refused and stays 'running' for
    the cancel ladder to terminalise.

    The refusal surfaces as WorkerOwnershipMismatch rather than the
    deferral arms' ``noop`` return, each method mirroring its own PG
    contract (mark_retry's rowcount-0 raises; the deferral statements
    return an empty outcome). The backend-level differential pins for both
    backends live in tests/test_cancel_state_reset_on_retry.py.
    """

    async def test_mark_retry_refuses_in_flight_cancel(self) -> None:
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        _set_in_flight_cancel(backend, job_id)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="transient",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(
                job_id,
                wid,
                error_info,
                retry_delay=timedelta(seconds=10),
                attempt=1,
            )

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "running", (
            "a failure retry landing mid-cancel must not reschedule the "
            "row: the retry arm lacks the cancel_phase = 0 fence and "
            "launders the operator's in-flight cancel"
        )
        assert row.cancel_phase == CancelPhase.COOPERATIVE
        assert row.cancel_requested_at == _START

        # The escalation controller still sees the in-flight cancel.
        flags = await backend.poll_cancel_flags(wid)
        assert [f.job_id for f in flags if f.job_id == job_id] != [], (
            "the cancel ladder lost sight of the in-flight cancel: the "
            "retry write disarmed the phase-2 escalation"
        )

    async def test_mark_retry_still_retries_clean_rows(self) -> None:
        """The fence narrows nothing for the clean case: a phase-0 row with
        a retryable failure retries as before."""
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

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
            attempt=1,
        )

        assert result.status == "scheduled"
        assert result.cancel_phase == CancelPhase.NONE
        assert result.cancel_requested_at is None


# ── Issue: the interrupt arm's attempt refund re-creates the epoch ──


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


# ── Issue: a lapsed deferral deadline must not outbid an in-flight cancel ──


async def _pair_enqueue_and_dispatch(backend: Backend) -> tuple[JobId, UUID]:
    """Enqueue and dispatch one job on EITHER backend (the pair fixture)."""
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"k": "v"},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    if isinstance(backend, InMemoryBackend):
        worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; mirrors tests/test_backend_equivalence.py
    else:
        assert isinstance(backend, PostgresBackend)
        schema: str = backend._schema_name  # type: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_backend_equivalence.py
        pool = backend._worker_pool  # type: ignore[reportPrivateUsage]  # Why: same
        worker_id = new_uuid()
        async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; every value is $N-bound
                "VALUES ($1, $2, $3, $4)",
                worker_id,
                "test-host",
                12345,
                ["default"],
            )
    dispatched = await backend.dispatch_batch(
        worker_id,
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert job_id in {row.id for row in dispatched}
    return job_id, worker_id


async def _pair_force_deadline_lapsed(backend: Backend, job_id: JobId) -> None:
    """Put the running row's schedule_to_close in the past.

    The deferral arms compare the would-be reschedule time against the
    deadline; a lapsed deadline sends every deferral shape to the fused
    statement's terminal arms, which is where the pre-fix deadline arm
    terminalised 'failed' on a phase-carrying row.
    """
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: forcing a race-window state the public API cannot reach directly; mirrors tests/test_backend_equivalence.py
        backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]  # Why: same
            row, schedule_to_close=_START - timedelta(seconds=1)
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_backend_equivalence.py
    pool = backend._worker_pool  # type: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType]  # Why: asyncpg stubs
        await conn.execute(
            f'UPDATE "{schema}".jobs SET schedule_to_close = $2 WHERE id = $1',  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; values are $N-bound
            job_id,
            _START - timedelta(seconds=1),
        )


async def _pair_set_in_flight_cancel(backend: Backend, job_id: JobId, phase: CancelPhase) -> None:
    """Stamp an operator cancel in flight (phase + timestamp) on the row."""
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: as above
        backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]  # Why: same
            row, cancel_phase=phase, cancel_requested_at=_START
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage]  # Why: as above
    pool = backend._worker_pool  # type: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType]  # Why: asyncpg stubs
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; values are $N-bound
            "SET cancel_phase = $2, cancel_requested_at = clock_timestamp() "
            "WHERE id = $1",
            job_id,
            int(phase),
        )


_ARM_CALLS = {
    "snoozed": lambda backend, job_id, wid: backend.mark_snoozed(
        job_id, wid, timedelta(seconds=30), attempt=1
    ),
    "retry_after_consume_true": lambda backend, job_id, wid: backend.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=True, attempt=1
    ),
    "retry_after_consume_false": lambda backend, job_id, wid: backend.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=False, attempt=1
    ),
}


@pytest.mark.integration
@pytest.mark.parametrize("arm", sorted(_ARM_CALLS), ids=sorted(_ARM_CALLS))
@pytest.mark.parametrize(
    "phase,origin",
    [
        (CancelPhase.COOPERATIVE, CANCEL_ORIGIN_COOPERATIVE),
        (CancelPhase.FORCED, CANCEL_ORIGIN_FORCED),
    ],
    ids=["phase1", "phase2"],
)
class TestDeadlineArmsHonourInFlightCancel:
    """A phase-carrying row whose deadline lapsed at deferral time must
    terminalise 'cancelled', never 'failed:DeadlineExceeded'.

    Operator intent outranks the deadline (and the retry budget), the
    same cancel-first arbitration _SWEEP_1_SQL's CASE carries: the
    deadline arm terminalises the cancel first, preserves the cancel
    columns as the audit trail, and stamps the cancel-origin marker
    rather than DeadlineExceeded.
    """

    async def test_terminalises_cancelled_with_columns_preserved(
        self,
        backend_pair: Backend,
        arm: str,
        phase: CancelPhase,
        origin: str,
    ) -> None:
        backend = backend_pair
        job_id, wid = await _pair_enqueue_and_dispatch(backend)
        await _pair_force_deadline_lapsed(backend, job_id)
        await _pair_set_in_flight_cancel(backend, job_id, phase)

        result = await _ARM_CALLS[arm](backend, job_id, wid)

        # The deferral did not land: the caller reads back "noop" (the
        # same contract the deadline-fenced deferral returns), never a
        # "failed" that would report DeadlineExceeded for a cancelled
        # job.
        assert result == "noop", (
            f"{arm} on a phase-carrying row past its deadline must report "
            "that the deferral did not land, not a deadline failure"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled", (
            f"{arm} terminalised a phase-carrying row past its deadline as "
            f"{row.status!r}: the deadline arm outbid the operator's "
            "in-flight cancel (cancel-first ordering missing)"
        )
        assert row.error_class == origin, (
            f"{arm} stamped error_class={row.error_class!r} on a cancelled "
            "row: the cancel-origin marker, never DeadlineExceeded"
        )
        # The cancel columns survive: they are the row's audit trail.
        assert row.cancel_phase == phase
        assert row.cancel_requested_at is not None

    async def test_clean_row_still_fails_on_lapsed_deadline(
        self,
        backend_pair: Backend,
        arm: str,
        phase: CancelPhase,
        origin: str,
    ) -> None:
        """The arbitration narrows nothing for the clean case: a phase-0
        row past its deadline fails with DeadlineExceeded exactly as
        before."""
        backend = backend_pair
        job_id, wid = await _pair_enqueue_and_dispatch(backend)
        await _pair_force_deadline_lapsed(backend, job_id)

        result = await _ARM_CALLS[arm](backend, job_id, wid)

        expected = "failed" if arm == "snoozed" else "failed:DeadlineExceeded"
        assert result == expected
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"
        assert row.cancel_phase == CancelPhase.NONE
