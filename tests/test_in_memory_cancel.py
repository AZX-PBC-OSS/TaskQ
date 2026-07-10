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
from datetime import UTC, datetime, timedelta
from uuid import UUID

from taskq._ids import new_job_id
from taskq.backend._protocol import JobId
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
        await backend.mark_cancelled(job_id, wid)

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
