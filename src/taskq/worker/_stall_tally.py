"""The process-wide rolling tally of attributed event-loop stalls, and the
shared vocabulary of the stall classifier.

The loop-lag watchdog's daemon thread attributes each stall to the actor
holding the interpreter and records the result in two directions at once:
an OTel counter (per-stall event stream) and a
:class:`StallAttributionTally` (the rolling per-actor state). The tally is
the whole seam between the watchdog thread and the heartbeat loop: the
watchdog writes it from off-loop, the heartbeat reads it once per tick and
merges the value into this worker's ``workers`` row metadata, so the
fleet's stall hotspots are queryable from the ``workers`` table without
scraping each worker's metrics endpoint. Neither side ever touches the
other's thread.

This module is deliberately dependency-free (stdlib only) so the watchdog,
the worker deps, the heartbeat, and the CLI can all import it without
import-cycle risk.
"""

import threading

__all__ = [
    "KIND_BLOCKING_CALL",
    "KIND_GIL_HELD",
    "STALL_TALLY_MAX_ACTORS",
    "STALL_TALLY_UNATTRIBUTED",
    "StallAttributionTally",
    "remedy_for_kind",
]

KIND_BLOCKING_CALL = "blocking_call"
"""A synchronous call that RELEASED the GIL: the loop could not schedule
because the actor waited on something off-Python (``time.sleep``, socket or
request I/O, a subprocess), while the watchdog's own thread kept ticking."""

KIND_GIL_HELD = "gil_held"
"""A synchronous computation that HELD the GIL: a C extension that does not
detach (or a hot pure-Python loop) starved even the watchdog thread's own
wakeups, which is the second classifier signal."""

STALL_TALLY_MAX_ACTORS = 20
"""The workers row's metadata carries at most this many actors, hottest
first: it rides a row rewritten every heartbeat, not a metrics series."""

STALL_TALLY_UNATTRIBUTED = "_unattributed_"
"""The tally key for a stall whose sampled stack named no registered actor
(the block sat under taskq's own code or a non-actor coroutine)."""

_BLOCKING_CALL_REMEDY = (
    "move the blocking call off the event loop (asyncio.to_thread / "
    "run_in_executor) or make it async"
)
_GIL_HELD_REMEDY = (
    "the actor holds the GIL in a long synchronous computation: chunk it or move it off the loop"
)


def remedy_for_kind(kind: str) -> str:
    """The one-line operator remedy for a stall of this kind.

    Shared by the watchdog's warning and the doctor finding so the two
    surfaces cannot drift apart.
    """
    return _GIL_HELD_REMEDY if kind == KIND_GIL_HELD else _BLOCKING_CALL_REMEDY


class StallAttributionTally:
    """Thread-safe, bounded, rolling ``actor -> {kind: count}`` tally.

    Written by the watchdog's daemon thread on every attributed stall,
    read by the heartbeat loop on the event-loop thread. Both sides touch
    only the lock-guarded methods, so the tally is safe even though its
    two users live on different threads.

    The bound keeps the metadata value small: when more than
    *max_actors* actors have been attributed, the least-attributed actor
    is evicted (ties by name for determinism), keeping the hottest
    actors the tally exists to name.
    """

    def __init__(self, *, max_actors: int = STALL_TALLY_MAX_ACTORS) -> None:
        self._max_actors = max_actors
        self._lock = threading.Lock()
        self._counts: dict[str, dict[str, int]] = {}

    def record(self, actor: str | None, *, kind: str) -> None:
        """Count one attributed stall (call from any thread)."""
        key = actor if actor else STALL_TALLY_UNATTRIBUTED
        with self._lock:
            entry = self._counts.setdefault(key, {})
            entry[kind] = entry.get(kind, 0) + 1
            while len(self._counts) > self._max_actors:
                self._evict_smallest()

    def _evict_smallest(self) -> None:
        smallest = min(self._counts, key=lambda a: (sum(self._counts[a].values()), a))
        del self._counts[smallest]

    def snapshot(self) -> dict[str, dict[str, int]]:
        """A thread-safe copy of the tally (call from any thread)."""
        with self._lock:
            return {actor: dict(kinds) for actor, kinds in self._counts.items()}

    def metadata_value(self) -> dict[str, object]:
        """The value the heartbeat merges into the workers row metadata.

        Empty while this process has attributed nothing, so the heartbeat
        merges a no-op and the row carries no key.
        """
        snapshot = self.snapshot()
        if not snapshot:
            return {}
        return {"loop_stalls": snapshot}
