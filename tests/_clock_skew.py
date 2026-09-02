"""Deliberate clock-domain divergence for behavioral tests.

TaskQ runs two independent clocks: the injectable Python ``Clock``
(``taskq.backend.clock``) and the PG server clock (``clock_timestamp()``).
Production divergence (NTP drift, VM pause) is reproduced here by wrapping
a base Clock with a fixed offset, so a test can skew exactly one domain
and assert that the server-side predicate no longer mixes them.

A test that pins a C-fix must skew the Python domain *in the direction
that made the old code misbehave* and assert the observable server-side
outcome is unchanged. Skew direction cheat-sheet (S = python - server):
  S > 0 (ahead): premature actions — immediate enqueues look future,
                 retry backoffs look due, ages look inflated.
  S < 0 (behind): late actions — backoff voided, dedup/retention windows
                 stretched, jobs linger past deadlines.
"""

from datetime import datetime, timedelta

from taskq.backend.clock import Clock

__all__ = ["SkewedClock"]


class SkewedClock:
    """Wrap *base*, offsetting ``now()`` by *skew* (positive = ahead).

    ``monotonic()`` is passed through untouched: monotonic math is local
    by design and never crosses a domain boundary.
    """

    def __init__(self, base: Clock, skew: timedelta) -> None:
        self._base: Clock = base
        self._skew: timedelta = skew

    def now(self) -> datetime:
        return self._base.now() + self._skew

    def monotonic(self) -> float:
        return self._base.monotonic()
