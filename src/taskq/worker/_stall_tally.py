"""The process-wide rolling tally of attributed event-loop stalls, the
sampled idle-fraction window, and the shared vocabulary of the stall
classifier.

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

The same seam carries :class:`LoopIdleWindow`: the watchdog thread
classifies every poll of the event-loop thread as parked-in-the-idle-
selector or not, the heartbeat drains the window once per tick and
publishes the fraction beside the stall tally. Cumulative counts alone
cannot say whether a worker is stalling NOW - a worker that stalled once
yesterday reads the same as one stalling on every beat - so the tally's
metadata value carries a windowed delta beside the cumulative counts, and
the idle fraction is windowed by construction.

This module is deliberately dependency-free (stdlib only) so the watchdog,
the worker deps, the heartbeat, and the CLI can all import it without
import-cycle risk.
"""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

__all__ = [
    "KIND_BLOCKING_CALL",
    "KIND_GIL_HELD",
    "STALL_TALLY_MAX_ACTORS",
    "STALL_TALLY_UNATTRIBUTED",
    "DrainedIdleWindow",
    "LoopIdleWindow",
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


@dataclass(frozen=True, slots=True)
class DrainedIdleWindow:
    """One drained window's aggregate, typed for the heartbeat's two
    consumers (the jsonb payload and the histogram record)."""

    idle_fraction: float
    samples: int
    samples_parked: int
    samples_unreadable: int
    window_seconds: float

    def as_metadata(self) -> dict[str, object]:
        """The value the heartbeat publishes as ``loop_idle`` in the
        workers row metadata."""
        return {
            "idle_fraction": self.idle_fraction,
            "samples": self.samples,
            "samples_parked": self.samples_parked,
            "samples_unreadable": self.samples_unreadable,
            "window_seconds": self.window_seconds,
        }


class LoopIdleWindow:
    """Thread-safe accumulator for one heartbeat window of loop-park
    samples, and the shared holder between the lag watchdog's daemon
    thread (writer) and the heartbeat loop (reader) - the same seam the
    stall tally runs on.

    WHAT IT MEASURES. The watchdog thread wakes on its fixed poll cadence
    and, once armed, reads the event-loop thread's current Python frame
    chain (the same ``sys._current_frames`` read the stall sampler uses).
    A CPython asyncio loop that has no ready callbacks is PARKED inside
    its selector wait - the innermost frame is ``selectors.py:select``
    under ``base_events.py:_run_once``. Any other observed shape means the
    loop thread was executing a callback (or its Python frames were
    unreadable, which conservatively counts as not parked). One sample per
    poll; the fraction of a window's samples classified parked is the
    published ``idle_fraction``.

    WHAT IT DOES NOT MEASURE, stated honestly:

    - It is a SAMPLED PROPORTION, not an integral: two samples per second
      at the default poll interval, each a binary parked/not-parked
      verdict on an instant. Sub-second busy bursts below the sampling
      resolution are invisible; the fraction carries binomial noise of
      order ``1/sqrt(samples)`` (``samples`` is published beside it).
    - It measures the LOOP THREAD, not job concurrency: a loop busy
      stepping many coroutines reads the same as one running a single
      hot callback. It is the complement's shape that matters operationally
      (a loop parked ~100% of the window cannot be the thing that is slow).
    - The classification is defined against CPython's selector-based
      event loop (the only loop TaskQ runs on). A frame shape it does not
      recognise counts as not parked, so ``idle_fraction`` biases toward
      busy, never toward idle.
    - A poll landing in the microseconds-wide zero-timeout select call
      ``_run_once`` makes when ready callbacks are queued reads as parked;
      a one-sample under-count of busy at the default cadence.

    The window is drained by the heartbeat once per tick: ``drain``
    returns the aggregate and resets, so the published value always
    describes exactly the span since the previous publish.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._window_started: float | None = None
        self._samples = 0
        self._parked = 0
        self._unreadable = 0

    def record_sample(self, *, parked: bool | None) -> None:
        """Count one watchdog poll (call from the watchdog thread).

        *parked* is the classification of the loop thread's frame chain;
        ``None`` when the frames could not be read at all (the sample
        still counts toward the window's span, and as not parked).
        """
        with self._lock:
            if self._window_started is None:
                self._window_started = self._clock()
            self._samples += 1
            if parked is None:
                self._unreadable += 1
            elif parked:
                self._parked += 1

    def drain(self) -> DrainedIdleWindow | None:
        """Return the window's aggregate and start a fresh window.

        ``None`` when no sample was recorded since the last drain (the
        heartbeat merges a no-op and records no histogram point - an
        absent series is the honest "nothing observed", never a stale 0).
        ``idle_fraction`` spans ALL samples (unreadable ones count as not
        parked); ``samples_unreadable`` carries that share so a reader can
        judge the signal's coverage.
        """
        with self._lock:
            if self._samples == 0:
                return None
            assert self._window_started is not None  # set with the first sample
            out = DrainedIdleWindow(
                idle_fraction=self._parked / self._samples,
                samples=self._samples,
                samples_parked=self._parked,
                samples_unreadable=self._unreadable,
                window_seconds=self._clock() - self._window_started,
            )
            self._window_started = None
            self._samples = 0
            self._parked = 0
            self._unreadable = 0
            return out


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

    def __init__(
        self,
        *,
        max_actors: int = STALL_TALLY_MAX_ACTORS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_actors = max_actors
        self._clock = clock
        self._lock = threading.Lock()
        self._counts: dict[str, dict[str, int]] = {}
        # The publish window's anchor: (monotonic stamp, cumulative snapshot)
        # of the last metadata_value() call. The heartbeat reads the tally
        # once per tick, so consecutive reads delimit one heartbeat window,
        # and the delta between the two snapshots is the windowed stall
        # rate - computable here for free, no second sampling path.
        self._last_published: tuple[float, dict[str, dict[str, int]]] | None = None
        self._created_at: float | None = clock()

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

    def _window_delta(self, snapshot: dict[str, dict[str, int]]) -> dict[str, object]:
        """The per-window stall delta and its span, and re-anchor the window.

        Called by ``metadata_value`` (heartbeat thread) with the snapshot
        already taken; the lock orders this against the watchdog thread's
        ``record``. Delta counts clamp at zero: the tally's bounded eviction
        drops the coldest actors, and an evicted actor re-attributed in a
        later window starts from its eviction-time count, never a negative.
        """
        with self._lock:
            now = self._clock()
            if self._last_published is None:
                # First publish of this process: the window is the whole
                # run so far and the delta is the full cumulative tally -
                # the honest summary a fresh worker's first beat can give.
                window_secs = now - self._created_at if self._created_at is not None else 0.0
                delta = {actor: dict(kinds) for actor, kinds in snapshot.items()}
            else:
                since, previous = self._last_published
                window_secs = now - since
                delta = {}
                for actor, kinds in snapshot.items():
                    prev_kinds = previous.get(actor, {})
                    win: dict[str, int] = {}
                    for kind, count in kinds.items():
                        inc = count - prev_kinds.get(kind, 0)
                        if inc > 0:
                            win[kind] = inc
                    if win:
                        delta[actor] = win
            self._last_published = (now, snapshot)
        return {"window_seconds": window_secs, "stalls": delta}

    def metadata_value(self) -> dict[str, object]:
        """The value the heartbeat merges into the workers row metadata.

        Empty while this process has attributed nothing, so the heartbeat
        merges a no-op and the row carries no key. Once anything has been
        attributed, every subsequent call carries BOTH the cumulative
        ``loop_stalls`` (unchanged shape; existing consumers keep reading
        it) and ``loop_stalls_window``: the delta since the previous call,
        the per-heartbeat-interval stall rate a healed worker shows as an
        empty (or absent-actor) ``stalls`` map while its cumulative counts
        stand still.
        """
        snapshot = self.snapshot()
        if not snapshot:
            return {}
        window = self._window_delta(snapshot)
        return {"loop_stalls": snapshot, "loop_stalls_window": window}
