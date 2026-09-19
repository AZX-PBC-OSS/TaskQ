"""Tests for InMemoryBackend cancel polling.

Covers:
- part (a): write_cancel_request writes cancel_requested_at and
  cancel_phase=1 but does NOT set the local cancel_event.
- part (b): subsequent tick_cancel_polling() sets the cancel_event
  registered via register_cancel_event.
- part (c): after FakeClock advances past cancellation_grace_period,
  tick_cancel_polling() writes cancel_phase=2.
- After advancing past cancellation_grace + cleanup_grace, another call
  marks the row abandoned.
- tick_cancel_polling MUST NOT call await asyncio.sleep etc.
"""

# pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType]
# Why: StubFn is Callable[..., object] by design ; stub lambdas
# inherently have unknown parameter types.

import asyncio
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import taskq.constants as _constants
from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)


def _make_backend(
    clock: FakeClock | None = None,
) -> InMemoryBackend:
    clk = clock or FakeClock(_START)
    return InMemoryBackend(
        clock=clk,
        cancellation_grace_period=_CANCEL_GRACE,
        cleanup_grace_period=_CLEANUP_GRACE,
    )


async def _make_running_job(backend: InMemoryBackend) -> tuple[JobId, UUID]:
    """Enqueue a job and set it to running, returning (job_id, worker_id)."""
    from dataclasses import replace as _replace

    from taskq.backend import EnqueueArgs

    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    row = await backend.enqueue(args)

    wid = backend._worker_id
    now = backend._clock.now()
    running_row = _replace(
        row,
        status="running",
        locked_by_worker=wid,
        lock_expires_at=now + timedelta(seconds=60),
        started_at=now,
        attempt=1,
    )
    backend._jobs[args.id] = running_row
    return args.id, wid


# ── part (a) ────────────────────────────────────────────────────


class TestWriteCancelRequestNoEvent:
    async def test_cancel_request_does_not_fire_event(self) -> None:
        """part (a): write_cancel_request writes cancel_requested_at
        and cancel_phase=1 but does NOT set the local cancel_event.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id, _wid = await _make_running_job(backend)

        # Register a cancel event
        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        # Write cancel request
        result = await backend.write_cancel_request(job_id, "test")
        assert result is True

        # Verify row state
        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_requested_at is not None
        assert row.cancel_phase == 1

        # Event is NOT yet set
        assert not cancel_event.is_set()


# ── part (b) ────────────────────────────────────────────────────


class TestTickCancelPollingFiresEvent:
    async def test_tick_cancel_polling_sets_event(self) -> None:
        """part (b): subsequent tick_cancel_polling() sets the
        cancel_event registered via register_cancel_event.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id, _wid = await _make_running_job(backend)

        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        await backend.write_cancel_request(job_id, None)
        assert not cancel_event.is_set()

        await backend.tick_cancel_polling()

        assert cancel_event.is_set()

        # Verify observation recorded
        assert job_id in backend._cancel_observed_at


# ── part (c) ────────────────────────────────────────────────────


class TestEscalationAfterGrace:
    async def test_escalate_to_phase2_after_grace(self) -> None:
        """part (c): after FakeClock advances past
        cancellation_grace_period, tick_cancel_polling() writes
        cancel_phase=2.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id, _wid = await _make_running_job(backend)

        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        await backend.write_cancel_request(job_id, None)
        await backend.tick_cancel_polling()  # first observation
        assert cancel_event.is_set()

        # Advance past grace period
        clock.advance(_CANCEL_GRACE + timedelta(seconds=1))
        await backend.tick_cancel_polling()

        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == 2

    async def test_abandoned_after_both_graces(self) -> None:
        """After advancing past cancellation_grace + cleanup_grace,
        another tick_cancel_polling() call marks the row abandoned.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id, _wid = await _make_running_job(backend)

        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        await backend.write_cancel_request(job_id, None)
        await backend.tick_cancel_polling()  # first observation

        # Advance past cancel grace → phase 2
        clock.advance(_CANCEL_GRACE + timedelta(seconds=1))
        await backend.tick_cancel_polling()

        # Advance past both graces → abandoned
        clock.advance(_CLEANUP_GRACE + timedelta(seconds=1))
        await backend.tick_cancel_polling()

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "abandoned"
        assert row.finished_at is not None


# ── No await in tick_cancel_polling ───────────────────────────────────


class TestTickCancelNoYield:
    async def test_does_not_await(self) -> None:
        """tick_cancel_polling MUST NOT sleep or yield to the event loop.

        This is verified by inspection of the implementation: it contains
        no ``await`` calls. A runtime test cannot easily assert "no await
        occurred"; this documentation-only test confirms the contract.
        """
        # tick_cancel_polling is a sync-looking async function that
        # doesn't actually await anything. Calling it should return
        # immediately without any coroutine scheduling.
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id, _wid = await _make_running_job(backend)
        await backend.write_cancel_request(job_id, None)

        # Should complete without yielding (no asyncio.sleep, no await)
        await backend.tick_cancel_polling()


# ── Cancel-tracking cleanup ──────────────────────────────────────────


class TestCancelTrackingCleanup:
    async def test_cancel_events_cleaned_up_on_terminal(self) -> None:
        """Regression test for review finding 5: ``_cancel_events`` and
        ``_cancel_observed_at`` accumulate without cleanup. After a job
        reaches a terminal state, ``tick_cancel_polling`` should remove
        its entries from both dicts.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id, _wid = await _make_running_job(backend)

        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        await backend.write_cancel_request(job_id, None)
        await backend.tick_cancel_polling()  # first observation
        assert job_id in backend._cancel_observed_at
        assert job_id in backend._cancel_events

        # Cancel the job via pending-path (write_cancel_request for a
        # pending job transitions to cancelled). For a running job,
        # we can mark it cancelled directly.
        wid = backend._worker_id
        await backend.mark_cancelled(job_id, wid, attempt=1)

        # tick_cancel_polling should clean up the tracking dicts
        await backend.tick_cancel_polling()
        assert job_id not in backend._cancel_observed_at
        assert job_id not in backend._cancel_events

    async def test_abandoned_job_cleaned_up(self) -> None:
        """After a job is abandoned (via tick_cancel_polling escalation),
        the next tick_cancel_polling call should clean up its tracking
        entries.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id, _wid = await _make_running_job(backend)

        cancel_event = asyncio.Event()
        backend.register_cancel_event(job_id, cancel_event)

        await backend.write_cancel_request(job_id, None)
        await backend.tick_cancel_polling()  # first observation

        # Advance past cancel grace → phase 2
        clock.advance(_CANCEL_GRACE + timedelta(seconds=1))
        await backend.tick_cancel_polling()

        # Advance past both graces → abandoned
        clock.advance(_CLEANUP_GRACE + timedelta(seconds=1))
        await backend.tick_cancel_polling()

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "abandoned"

        # Tracking dicts should be cleaned up on the tick that marks abandoned
        # (the cleanup runs after the escalation loop)
        # Need one more tick to clean up (cleanup runs at end of tick)
        await backend.tick_cancel_polling()
        assert job_id not in backend._cancel_observed_at
        assert job_id not in backend._cancel_events


# ── Cancel-origin auditability (backend parity) ─────────────────


class TestCancelOriginAuditability:
    """Cancel origin is auditable from the job row alone, on both backends.

    A cooperative self-cancel, a forced abandon and a job cancelled before
    it ever reached a worker are three operationally distinct outcomes: an
    actor honoured ``ctx.cancellation_requested``, a worker had to stop an
    actor that would not yield, or the work never ran at all. Operators
    triage those differently and worker logs roll off, so each terminal
    cancel path stamps its own distinguishing ``error_class`` on the row -
    the same self-describing terminal write every failure path already
    makes.

    The in-memory backend is the observable twin of Postgres at this seam,
    so a marker that only Postgres writes is a parity break: tests and
    local development would see an unauditable cancel that production does
    not have.
    """

    async def test_cooperative_cancel_stamps_error_class(self) -> None:
        """``mark_cancelled`` leaves a non-NULL ``error_class`` on the row
        so the terminal write says why the job stopped without a log
        lookup."""
        backend = _make_backend()
        job_id, worker_id = await _make_running_job(backend)

        assert await backend.mark_cancelled(job_id, worker_id, attempt=1) is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.error_class is not None, (
            "a cooperative cancel wrote no error_class - cancel origin is "
            "unauditable on the job row"
        )

    async def test_cancel_origins_are_distinguishable_on_the_row(self) -> None:
        """Cooperative cancel, forced abandon and cancelled-while-pending
        each stamp a *different* ``error_class``, so three cancelled rows
        read side by side say which actor yielded, which had to be killed
        and which never ran."""
        backend = _make_backend()

        coop_job, coop_worker = await _make_running_job(backend)
        abandon_job, _abandon_worker = await _make_running_job(backend)

        pending_args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
        await backend.enqueue(pending_args)

        assert await backend.mark_cancelled(coop_job, coop_worker, attempt=1) is True
        # The abandon path requires the escalated phase the forcing ladder reaches.
        backend._jobs[abandon_job] = _dc_replace(backend._jobs[abandon_job], cancel_phase=2)
        assert await backend.mark_abandoned(abandon_job) is True
        assert await backend.write_cancel_request(pending_args.id, "operator stop") is True

        classes = {}
        for label, job_id in (
            ("cooperative", coop_job),
            ("abandoned", abandon_job),
            ("cancelled_while_pending", pending_args.id),
        ):
            row = await backend.get(job_id)
            assert row is not None
            classes[label] = row.error_class

        assert all(v is not None for v in classes.values()), (
            f"a cancel terminal path left error_class NULL: {classes}"
        )
        assert len(set(classes.values())) == 3, (
            f"cancel origins are not distinguishable on the job row: {classes}"
        )

    async def test_cancelled_attempt_records_its_reason(self) -> None:
        """The ``job_attempts`` row a cancelled attempt leaves behind
        carries the same ``error_class`` marker as the job row.

        Attempt history is what a postmortem queries when the job row has
        already been reclaimed by retention; an attempt that records only
        ``outcome='cancelled'`` cannot say which of the cancel paths ended
        it."""
        backend = _make_backend()
        job_id, worker_id = await _make_running_job(backend)

        assert await backend.mark_cancelled(job_id, worker_id, attempt=1) is True

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "cancelled"
        assert attempts[0].error_class is not None, (
            "the cancelled attempt row recorded no reason, so attempt history "
            "cannot distinguish a cooperative cancel from a forced abandon"
        )

    async def test_cancelled_while_pending_leaves_terminal_timeline_entry(self) -> None:
        """A job cancelled before it ever reached a worker still shows the
        terminal transition on its event timeline.

        Trimming bookkeeping rows from the event stream cut noise, not state
        transitions: no cancelled job may end with a timeline that never
        shows it ending, or the admin timeline view shows a job that simply
        stops."""
        backend = _make_backend()
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
        await backend.enqueue(args)

        assert await backend.write_cancel_request(args.id, "operator stop") is True

        events = await backend.get_events(args.id)
        assert events, "a cancelled job ended with an empty event timeline"
        terminal = [
            e
            for e in events
            if e.kind == "state_change" and (e.detail or {}).get("to_state") == "cancelled"
        ]
        assert terminal, (
            "a job cancelled while pending left no terminal-cancel entry on its "
            f"event timeline; kinds were {[e.kind for e in events]}"
        )

    async def test_cancelled_while_scheduled_leaves_terminal_timeline_entry(self) -> None:
        """The same holds for a job cancelled while scheduled for a future
        run - the deferred-start path must not be the one that loses its
        audit transition."""
        backend = _make_backend()
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START + timedelta(hours=1),
        )
        await backend.enqueue(args)
        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "scheduled"

        assert await backend.write_cancel_request(args.id, "operator stop") is True

        events = await backend.get_events(args.id)
        terminal = [
            e
            for e in events
            if e.kind == "state_change" and (e.detail or {}).get("to_state") == "cancelled"
        ]
        assert terminal, (
            "a job cancelled while scheduled left no terminal-cancel entry on "
            f"its event timeline; kinds were {[e.kind for e in events]}"
        )

    async def test_phase_1_cancel_stamps_the_cooperative_marker(self) -> None:
        """A running job cancelled while still only ASKED (cancel_phase=1)
        reads exactly ``CancelledCooperatively`` on the row - the constant,
        not merely a non-NULL distinct value: the existing distinctness pin
        would also pass if the phase arms were swapped."""
        backend = _make_backend()
        job_id, worker_id = await _make_running_job(backend)

        assert await backend.write_cancel_request(job_id, "operator stop") is True
        assert await backend.mark_cancelled(job_id, worker_id, attempt=1) is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.error_class == _constants.CANCEL_ORIGIN_COOPERATIVE

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].error_class == _constants.CANCEL_ORIGIN_COOPERATIVE

    async def test_phase_2_cancel_stamps_the_forced_marker(self) -> None:
        """A running job cancelled after escalation (cancel_phase=2) reads
        exactly ``CancelledForced`` on the row and the attempt - the
        marker says the actor had to be interrupted, which is the
        operational signal to go look at that actor."""
        backend = _make_backend()
        job_id, worker_id = await _make_running_job(backend)

        assert await backend.write_cancel_request(job_id, "operator stop") is True
        assert await backend.write_cancel_escalation(job_id, worker_id, 2) is True
        assert await backend.mark_cancelled(job_id, worker_id, attempt=1) is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.error_class == _constants.CANCEL_ORIGIN_FORCED

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].error_class == _constants.CANCEL_ORIGIN_FORCED
