"""Tests for PostgresBackend subscribe_wake (unit: no PG required).

Covers:
- enter adds to registry; exit removes; exit-on-exception also removes
- two PostgresBackend instances, registry isolation
- _wake_lock is acquired on enter and exit
"""

import asyncio
from contextlib import suppress
from datetime import timedelta
from unittest.mock import Mock

import pytest

from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend

# ── Helpers ────────────────────────────────────────────────────────────

_GRACE = timedelta(seconds=30)


def _make_backend() -> PostgresBackend:
    """Construct a PostgresBackend with mock deps (no live PG needed)."""
    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.worker_pool = Mock()
    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0
    return PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


# ── subscribe_wake enter/exit ─────────────────────────────────


class TestSubscribeWakeEnterExit:
    async def test_enter_adds_to_registry(self) -> None:
        """subscribe_wake() enter allocates an ``asyncio.Event`` and
        adds it to ``_wake_subscribers`` under ``_wake_lock``.
        """
        backend = _make_backend()
        assert len(backend._wake_subscribers) == 0  # type: ignore[reportPrivateUsage] # Why: test-only private access

        async with backend.subscribe_wake() as event:
            assert event in backend._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access

        assert len(backend._wake_subscribers) == 0  # type: ignore[reportPrivateUsage] # Why: test-only private access

    async def test_exit_removes_from_registry(self) -> None:
        """subscribe_wake() exit removes the event from
        ``_wake_subscribers`` under ``_wake_lock``.
        """
        backend = _make_backend()

        async with backend.subscribe_wake() as event:
            pass

        assert event not in backend._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access

    async def test_exit_on_exception_removes_from_registry(self) -> None:
        """subscribe_wake() exit removes the event from
        ``_wake_subscribers`` even when the consumer's ``with`` block raises.
        """
        backend = _make_backend()

        with pytest.raises(ValueError, match="consumer failure"):
            async with backend.subscribe_wake() as event:
                raise ValueError("consumer failure")

        assert event not in backend._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access
        assert len(backend._wake_subscribers) == 0  # type: ignore[reportPrivateUsage] # Why: test-only private access

    async def test_exit_on_cancellation_removes_from_registry(self) -> None:
        """subscribe_wake() exit removes the event from
        ``_wake_subscribers`` when the consumer is cancelled
        (``CancelledError`` propagates through ``__aexit__``).
        """
        backend = _make_backend()
        captured_event: asyncio.Event | None = None

        async def subscriber() -> None:
            nonlocal captured_event
            async with backend.subscribe_wake() as event:
                captured_event = event
                await asyncio.sleep(100)

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(0)
        assert captured_event is not None
        assert captured_event in backend._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

        assert captured_event not in backend._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access


# ── per-instance registry isolation ───────────────────────────


class TestSubscribeWakeRegistryIsolation:
    async def test_two_instances_isolated_registry(self) -> None:
        """opening ``subscribe_wake()`` on instance A does not add
        the event to instance B's ``_wake_subscribers``.
        """
        backend_a = _make_backend()
        backend_b = _make_backend()

        async with backend_a.subscribe_wake() as event_a:
            assert event_a in backend_a._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access
            assert event_a not in backend_b._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access
            assert len(backend_b._wake_subscribers) == 0  # type: ignore[reportPrivateUsage] # Why: test-only private access

        assert event_a not in backend_a._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access

    async def test_two_instances_independent_subscribers(self) -> None:
        """two instances each have independent subscriber sets.
        Opening on both creates disjoint events in each.
        """
        backend_a = _make_backend()
        backend_b = _make_backend()

        async with (
            backend_a.subscribe_wake() as event_a,
            backend_b.subscribe_wake() as event_b,
        ):
            assert event_a in backend_a._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access
            assert event_b in backend_b._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access
            assert event_a not in backend_b._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access
            assert event_b not in backend_a._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only private access


# ── lock contract ─────────────────────────────────────────────


class TestSubscribeWakeLockContract:
    async def test_lock_acquired_on_enter_and_exit(self) -> None:
        """instrument ``_wake_lock.acquire`` with a counter;
        confirm ``subscribe_wake()`` enter/exit increments it twice.

        The lock guards mutation of ``_wake_subscribers``. This test
        verifies the lock is wired through both enter and exit paths,
        which is the design surface for the callback-path assertion
        (the callback must NOT acquire the lock).
        """
        backend = _make_backend()
        counter = 0
        original_acquire = backend._wake_lock.acquire  # type: ignore[reportPrivateUsage] # Why: test-only instrumentation

        async def counting_acquire() -> None:
            nonlocal counter
            counter += 1
            await original_acquire()

        backend._wake_lock.acquire = counting_acquire  # type: ignore[reportPrivateUsage] # Why: test-only instrumentation; restored below

        try:
            async with backend.subscribe_wake():
                pass

            assert counter == 2, f"expected lock acquired twice (enter + exit), got {counter}"
        finally:
            backend._wake_lock.acquire = original_acquire  # type: ignore[reportPrivateUsage] # Why: restoration of test instrumentation
