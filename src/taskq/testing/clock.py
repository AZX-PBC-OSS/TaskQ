"""FakeClock — deterministic time control for tests.

Blueprint anchor: ("injected clock dependency").  ``FakeClock`` lives
in ``taskq.testing.clock`` (not ``taskq.backend.clock``) so the production
import path does not pull in test-only helpers.

``Clock`` and ``SystemClock`` remain in ``taskq.backend.clock`` (production).
"""

from datetime import UTC, datetime, timedelta

__all__ = ["FakeClock"]

# Epoch for monotonic() so the return value is a plausible non-zero float
# (~157,766,400.0 at the standard fixture start of 2025-01-01), not 0.0.
_EPOCH = datetime(2020, 1, 1, tzinfo=UTC)


class FakeClock:
    """Deterministic clock for tests.

    Accepts a ``start`` datetime (typically
    ``datetime(2025, 1, 1, tzinfo=UTC)``).  ``now()`` returns the current
    internal time; ``move_to`` and ``advance`` let tests control the clock
    explicitly.  ``monotonic()`` returns elapsed seconds from ``_EPOCH``
    so that elapsed-time guards see a non-zero starting value.
    """

    def __init__(self, start: datetime) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def move_to(self, when: datetime) -> None:
        """Set the clock to *when*."""
        self._now = when

    def advance(self, delta: timedelta) -> None:
        """Add *delta* to the clock."""
        self._now = self._now + delta

    def monotonic(self) -> float:
        """Elapsed seconds since ``_EPOCH`` — consistent with ``now()``.

        Same wall-clock position always returns the same float; never
        decreases within a test.
        """
        return (self._now - _EPOCH).total_seconds()
