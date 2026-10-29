"""The claim cursor: per-worker, per-queue memory of the last
successfully-claimed job id, and the jitter reset that bounds what the
cursor's bound can strand.

WHY (the evidence base: brandur.org/postgres-queues, re-run by
PlanetScale's 2026 "Keeping a Postgres queue healthy"): a long or
overlapping transaction pins the MVCC horizon, VACUUM cannot reclaim,
and the claim query's B-tree scans degenerate through the dead tuples
the claims themselves left behind (brandur measured 15x lock-time
degradation; PlanetScale: SKIP LOCKED "lifts the floor, not the
ceiling" - identical dead-tuple scans under both). The worker-side cure
is brandur's own: bound the claim predicate at the worker's last
successfully-claimed id (UUIDv7, time-ordered, so the bound is a
position in creation order) and the seek lands on live tuples instead
of walking the dead zone.

THE TRADE, stated plainly: the bound is a SELECTION predicate, not a
hint. Any pending row whose id falls below this worker's cursor -
a skewed producer's UUIDv7 minted behind it, a row the round's LIMIT
window never reached, a priority or backdated-scheduled_at divergence
between claim order and id order - is invisible to the claim query
until the jitter reset forgets the cursor. The design accepts a
stranding bounded by ONE reset window (default 60s, the only knob) in
exchange for a claim scan that no longer degrades with the table's dead
tuple count. The bound is proven, not assumed:
``tests/test_dispatch_claim_cursor_pg.py::test_a_lower_id_job_is_claimed_within_one_reset_window``
claims a mid-flight lower-id row inside one window, and the jitter pin
(``tests/test_claim_cursor.py``) is the mutation target - break the
jitter, the stranding pin reds.

Round-robin queues are EXEMPT: their cohort fairness is id-order-blind
by design and a bounded RR round would strand whole cohorts for the
window - the cohort starvation the mode exists to prevent. Only the
strict-FIFO renders carry the bound.

Concurrency: one cursor per backend instance; ``advance``/``bound`` are
synchronous, no awaits, and every mutation is one event-loop step - the
same discipline as QueueModeCache, no lock.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from uuid import UUID

__all__ = ["CLAIM_CURSOR_RESET_SECONDS", "ClaimCursor"]

CLAIM_CURSOR_RESET_SECONDS: Final[float] = 60.0
"""The jitter reset's base interval, in seconds - the cursor's ONE knob.

The bound a live cursor imposes cannot outlive this window: every entry
is armed at the advance that created it and expires after
``reset_seconds`` scaled by a jitter draw in [0.5, 1.5], so a fleet's
resets do not synchronize (a fleet resetting in the same tick would all
run the naive claim shape in the same round - the synchronized cost
spike the jitter exists to spread). The window is armed once per entry
and expires on schedule no matter how many claims land inside it: a
sliding window would let a hot queue's cursor live forever, which turns
the bounded stranding into an unbounded one.

60s is the brandur cadence (his jitter reset runs "every 60s or so" so
a straggler cannot be stranded). The stranding bound this number buys
is the runbook's contract: any row the cursor hides is claimed within
one window. The knob's live value rides
``WorkerSettings.claim_cursor_reset_seconds`` (the settings field's
default mirrors this constant); ``0`` is the documented OFF switch.
"""


class ClaimCursor:
    """Per-queue high-water mark of claimed job ids, with a jittered reset.

    * ``advance(queue, job_id)`` - the claim path, once per claimed row.
      Keeps the maximum: a lower id arriving later is a skew insert, not
      a time reversal.
    * ``bound(queue)`` - the claim query's lower bound for that queue,
      ``None`` when the queue has no live cursor (never claimed, reset,
      or the feature disabled). An expired entry is forgotten here, the
      jitter reset's firing site.
    * ``forget(queue)`` - drop one queue's entry.

    ``reset_seconds <= 0`` disables the cursor entirely: ``bound`` always
    answers ``None`` and ``advance`` never stores - the naive claim shape.
    """

    __slots__ = ("_clock", "_entries", "_jitter", "_reset_seconds")

    def __init__(
        self,
        *,
        reset_seconds: float = CLAIM_CURSOR_RESET_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = lambda: random.uniform(0.5, 1.5),  # noqa: S311  # Why: schedule jitter, not a secret.
    ) -> None:
        self._reset_seconds = reset_seconds
        self._clock = clock
        # The draw of the scale FACTOR: zero-arg, called once per armed
        # entry, expected to return values in [0.5, 1.5].
        self._jitter = jitter
        # queue -> (last claimed id, expires_at). expires_at is armed once
        # at the creating advance and never extended (see the constant's
        # docstring).
        self._entries: dict[str, tuple[UUID, float]] = {}

    @property
    def reset_seconds(self) -> float:
        """The knob's live value; the wiring refreshes it per round so a
        settings change takes effect without rebuilding the backend."""
        return self._reset_seconds

    @reset_seconds.setter
    def reset_seconds(self, value: float) -> None:
        self._reset_seconds = value

    def advance(self, queue: str, job_id: UUID) -> None:
        """Record a successfully-claimed row's id as the queue's cursor.

        A no-op when disabled. The maximum is kept (a high-water mark);
        the window is armed only by the advance that CREATED the entry,
        never extended by later claims.
        """
        if self._reset_seconds <= 0:
            return
        existing = self._entries.get(queue)
        if existing is not None:
            if existing[0] >= job_id:
                return
            # Higher-id refresh: the id moves, the window does NOT re-arm.
            # The reset is periodic from the entry's creation (see the
            # constant's docstring); extending it on every claim would let
            # a hot queue's cursor live forever, the never-heals state the
            # jitter pin forbids.
            self._entries[queue] = (job_id, existing[1])
            return
        expires_at = self._clock() + self._reset_seconds * self._jitter()
        self._entries[queue] = (job_id, expires_at)

    def bound(self, queue: str) -> UUID | None:
        """The queue's claim lower bound, ``None`` when no live cursor.

        Forgets an entry whose window has elapsed - the jitter reset's
        firing site, and the pin that keeps the stranding bounded.
        """
        if self._reset_seconds <= 0:
            return None
        entry = self._entries.get(queue)
        if entry is None:
            return None
        job_id, expires_at = entry
        if self._clock() >= expires_at:
            del self._entries[queue]
            return None
        return job_id

    def forget(self, queue: str) -> None:
        """Drop one queue's cursor (the next advance re-arms a fresh window)."""
        self._entries.pop(queue, None)
