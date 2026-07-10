"""Shared async context manager for wake/cancel subscriber registration.

Both :class:`~taskq.backend.postgres.PostgresBackend` and
:class:`~taskq.testing.in_memory.InMemoryBackend` expose
``subscribe_wake`` / ``subscribe_cancel_wake`` as async context managers
that register an :class:`asyncio.Event` on a subscriber set for the
duration of the ``async with`` block.  The Postgres backend guards the
add/remove with an :class:`asyncio.Lock` for cross-coroutine safety; the
in-memory backend is single-threaded by contract and passes no lock.
"""

import asyncio

__all__ = ["_SubscriberContext"]


class _SubscriberContext:
    """Async context manager that adds/removes an event on a subscriber set.

    When *lock* is provided, the add (on enter) and discard (on exit) run
    under the lock — matching the Postgres backend's cross-coroutine
    safety.  When *lock* is ``None`` (in-memory backend), the operations
    are unsynchronised per the single-threaded contract.
    """

    def __init__(
        self,
        event: asyncio.Event,
        subscribers: set[asyncio.Event],
        lock: asyncio.Lock | None = None,
    ) -> None:
        self._event = event
        self._subscribers = subscribers
        self._lock = lock

    async def __aenter__(self) -> asyncio.Event:
        if self._lock is not None:
            async with self._lock:
                self._subscribers.add(self._event)
        else:
            self._subscribers.add(self._event)
        return self._event

    async def __aexit__(self, *exc: object) -> None:
        if self._lock is not None:
            async with self._lock:
                self._subscribers.discard(self._event)
        else:
            self._subscribers.discard(self._event)
