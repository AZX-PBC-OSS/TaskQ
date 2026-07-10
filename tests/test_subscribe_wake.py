"""Tests for InMemoryBackend subscribe_wake.

Covers:
- per-subscriber semantics — two concurrent subscribe_wake
  contexts both set on enqueue; exited subscriber no longer notified.
- subscriber cancelled mid-async with — event removed from
  _wake_subscribers (cleanup happens in __aexit__ even on CancelledError).
"""

# pyright: ignore[reportUnknownArgumentType]
# Why: Test stubs use Callable[..., object] by design.

import asyncio
from contextlib import suppress
from datetime import UTC, datetime

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


# ── per-subscriber semantics ───────────────────────────────────


class TestPerSubscriberSemantics:
    async def test_two_subscribers_both_notified(self) -> None:
        """open two subscribe_wake() contexts (event_A, event_B).
        Enqueue one job. Assert event_A.is_set() and event_B.is_set()
        immediately.
        """
        backend = _make_backend()

        async with (
            backend.subscribe_wake() as event_a,
            backend.subscribe_wake() as event_b,
        ):
            await backend.enqueue(make_enqueue_args(payload={}, scheduled_at=_START))
            assert event_a.is_set()
            assert event_b.is_set()

    async def test_exited_subscriber_not_notified(self) -> None:
        """exit event_A's context. Enqueue a second job.
        Assert event_B.is_set(). Confirm the freed event_A object is
        no longer in _wake_subscribers (a third enqueue does not set it).
        """
        backend = _make_backend()

        # First subscriber enters and exits
        async with backend.subscribe_wake() as event_a:
            pass  # event_a is now removed from _wake_subscribers

        # Second subscriber enters
        async with backend.subscribe_wake() as event_b:
            # Enqueue a job
            await backend.enqueue(make_enqueue_args(payload={}, scheduled_at=_START))
            assert event_b.is_set()

            # event_a should NOT be set (it's no longer in subscribers)
            assert not event_a.is_set()

            # Confirm event_a is not in the subscriber set
            assert event_a not in backend._wake_subscribers


# ── subscriber cancelled mid-async with ──────────────────────


class TestCancelledSubscriber:
    async def test_cancelled_subscriber_event_removed(self) -> None:
        """subscriber cancelled mid-async with. Wrap a subscriber
        in an asyncio.Task, cancel it. Assert the cancelled subscriber's
        event is removed from _wake_subscribers (cleanup happens in
        __aexit__ even on CancelledError).
        """
        backend = _make_backend()
        captured_event: asyncio.Event | None = None

        async def subscriber_coro() -> None:
            nonlocal captured_event
            async with backend.subscribe_wake() as event:
                captured_event = event
                # Wait indefinitely — will be cancelled
                await asyncio.sleep(100)

        task = asyncio.create_task(subscriber_coro())
        # Give the task a chance to enter the context
        await asyncio.sleep(0)
        assert captured_event is not None

        # The event should be in subscribers now
        assert captured_event in backend._wake_subscribers

        # Cancel the task
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

        # After cancellation, event should be removed from subscribers
        assert captured_event not in backend._wake_subscribers


# ── subscribe_wake fires on scheduled_to_pending ────────────────────────


class TestSubscribeWakeScheduledToPending:
    async def test_subscribe_wake_fires_on_scheduled_to_pending(self) -> None:
        """subscribe_wake event should be set when a scheduled job is
        promoted to pending via scheduled_to_pending().

        requires wake events whenever a job becomes dispatchable
        — including the scheduled→pending promotion.
        """
        from datetime import timedelta

        backend = _make_backend()

        # Enqueue a scheduled-future job (not yet pending)
        future_scheduled = _START + timedelta(hours=1)
        future_args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=future_scheduled,
        )
        await backend.enqueue(future_args)

        async with backend.subscribe_wake() as event:
            # Clear the event set by enqueue()
            event.clear()
            assert not event.is_set()

            # Advance clock and promote to pending
            backend.advance_clock_to(_START + timedelta(hours=2))
            await backend.scheduled_to_pending(_START + timedelta(hours=2))

            # Wake event should be set after scheduled→pending promotion
            assert event.is_set(), "subscribe_wake event was not set by scheduled_to_pending()"
