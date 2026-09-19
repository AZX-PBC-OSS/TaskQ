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
    under the lock, matching the Postgres backend's cross-coroutine
    safety.  When *lock* is ``None`` (in-memory backend), the operations
    are unsynchronised per the single-threaded contract.
    """

    def __init__(
        self,
        event: asyncio.Event,
        subscribers: set[asyncio.Event],
        lock: asyncio.Lock | None = None,
        *,
        queue_registry: dict[asyncio.Event, frozenset[str] | None] | None = None,
        queues: frozenset[str] | None = None,
    ) -> None:
        self._event = event
        self._subscribers = subscribers
        self._lock = lock
        # The queue-scoped wake registry: when the caller passes one, the
        # subscription records which queues this subscriber serves so the
        # wake callback can skip notifications for queues it would never
        # claim. ``None`` queues (or no registry) keeps the wake-everything
        # contract.
        self._queue_registry = queue_registry
        self._queues = queues

    async def __aenter__(self) -> asyncio.Event:
        if self._lock is not None:
            async with self._lock:
                self._subscribers.add(self._event)
                if self._queue_registry is not None:
                    self._queue_registry[self._event] = self._queues
        else:
            self._subscribers.add(self._event)
            if self._queue_registry is not None:
                self._queue_registry[self._event] = self._queues
        return self._event

    async def __aexit__(self, *exc: object) -> None:
        if self._lock is not None:
            async with self._lock:
                self._subscribers.discard(self._event)
                if self._queue_registry is not None:
                    self._queue_registry.pop(self._event, None)
        else:
            self._subscribers.discard(self._event)
            if self._queue_registry is not None:
                self._queue_registry.pop(self._event, None)
