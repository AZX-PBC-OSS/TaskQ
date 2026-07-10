"""Clock protocol and SystemClock production implementation.

Blueprint anchor: ("injected clock dependency").  Both Backends
accept a :class:`Clock` instance at construction time so that
:class:`InMemoryBackend` can be wired with :class:`FakeClock` (defined in
``taskq.testing.clock``) and the production :class:`PostgresBackend`
receives a :class:`SystemClock` (or whatever the wiring layer provides —
Clock is a constructor parameter, not a global).

The protocol is ``@runtime_checkable`` so that tests and wiring code can
use ``isinstance`` guards without importing a concrete class.
"""

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

__all__ = ["Clock", "SystemClock"]


@runtime_checkable
class Clock(Protocol):
    """Time abstraction injected into Backends.

    ``now()`` returns wall-clock UTC; ``monotonic()`` returns a
    monotonically non-decreasing float suitable for local elapsed-time
    deltas (e.g. cancel-phase tracking in).
    """

    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...


@dataclass(frozen=True, slots=True)
class SystemClock:
    """Production clock delegating to the standard library.

    ``now()`` calls ``datetime.now(UTC)`` (timezone-aware).
    ``monotonic()`` calls ``time.monotonic()``.
    """

    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()
