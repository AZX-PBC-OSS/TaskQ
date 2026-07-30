"""In-worker hang/deadlock detection and last-resort teardown.

One module owns all four detectors plus the diagnostic dump, so nothing
else in the worker grows a health responsibility:

- :func:`dump_task_stacks` — one structured log record per live asyncio
  task (name, coro repr, await-site frames), shared by the trip dump,
  the SIGUSR2 handler, the ``/tasks`` endpoint, and the straggler
  logger. A raw stderr fallback follows the records so the bundle
  survives a broken logging pipeline.
- :class:`LoopLiveness` — per-loop monotonic tick stamps and ages.
- :class:`ShutdownWatchdog` — detectors 1 (shutdown hard deadline) and
  the straggler logger. Lives OUTSIDE the worker TaskGroup by design:
  as a group child it would be cancelled by the very sibling crash it
  exists to catch. Parks on ``shutdown_event``; once set, counts down
  ``termination_grace_period`` (finally making that setting enforceable
  rather than validation-only), logging still-alive siblings every
  ``watchdog_dump_interval`` once the shutdown is into the back half
  of its budget.
- :class:`LoopLagWatchdog` — detector 4, a daemon thread measuring
  event-loop scheduling lag. Thread-based (and ``faulthandler``-based
  for its dump) because a fully blocked loop cannot run an in-loop
  detector, and ``asyncio.all_tasks`` is not thread-safe.
- :func:`loop_watchdog_loop` — detector 2, the stale-tick sweep over
  :class:`LoopLiveness`. Interval-driven loops opt in via
  :meth:`LoopLiveness.tick`; deliberately parked loops (notify listener,
  consumers, reload coordinator) have no cadence and are covered by
  detectors 1, 3, and 4 instead.
- Detector 3 lives in the sibling spawner
  (:func:`_make_sibling_spawner`): a sibling returning cleanly while
  ``shutdown_event`` is clear is a contract violation and is re-raised.

Trip semantics (:func:`trip`): critical log + metric, dump, flush, then
``os._exit(EXIT_WATCHDOG)`` with no further awaits — a wedged process
cannot be trusted to unwind, and in-flight jobs are reclaimed by the
leader sweep on lock-lease expiry (the existing, tested recovery path).
The non-zero code guarantees the supervisor restarts the worker.
"""

import asyncio
import contextlib
import faulthandler
import os
import sys
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import structlog
from opentelemetry import metrics as otel_metrics
from opentelemetry.metrics import CallbackOptions, Observation

from taskq.obs import get_logger, get_meter

__all__ = [
    "EXIT_WATCHDOG",
    "LoopLagWatchdog",
    "LoopLiveness",
    "ShutdownWatchdog",
    "dump_task_stacks",
    "loop_watchdog_loop",
    "trip",
]

EXIT_WATCHDOG = 2

# Hard wall on the pre-exit metrics flush. force_flush has no usable
# timeout against a hung OTLP collector (the gRPC exporter ignores
# timeout_millis — opentelemetry#2663 — and self-bounds at 10s+), so the
# flush runs on a daemon thread with this join deadline: a wedged exporter
# costs at most this many seconds, never more, and the thread dies with
# the process on os._exit.
_METRICS_FLUSH_TIMEOUT_SECS = 2.0

# Default lower bound on any staleness budget, overridden per worker by
# TASKQ_WATCHDOG_STALE_FLOOR. Kept as the single source of the documented
# default so the constant and the constructor cannot drift apart.
DEFAULT_STALE_FLOOR_SECS = 10.0

_log: structlog.stdlib.BoundLogger = get_logger(__name__)

_meter = get_meter()
_shutdown_duration = _meter.create_histogram(
    name="taskq.worker.shutdown_duration_seconds",
    unit="s",
    description="Wall-clock seconds from the first shutdown signal to clean "
    "worker teardown. Recorded only on clean exit (watchdog cancel); a "
    "watchdog trip force-exits without recording.",
)
_watchdog_trips = _meter.create_counter(
    "taskq.worker.watchdog_trips_total",
    unit="1",
    description="Watchdog trips leading to force-exit, labelled by detector.",
)

# Tick-age observable gauge follows the codebase's cache + callback
# pattern (see taskq.obs.update_queue_depth_cache): ages() writes the
# cache, the synchronous OTel callback reads it.
_tick_age_cache: dict[str, float] = {}
# Guards _tick_age_cache against concurrent access: the OTel SDK reader
# thread invokes the gauge callback while the worker loop thread mutates
# the cache via ages()/forget(). Unsynchronized iteration raises
# RuntimeError: dictionary changed size during iteration — the same
# failure shape as the _ticks race, metrics-only. Lock order is always
# LoopLiveness._lock → _tick_age_cache_lock; the callback takes
# _tick_age_cache_lock alone. Never the reverse.
_tick_age_cache_lock = threading.Lock()


def _observe_tick_age(
    _options: CallbackOptions,
) -> Iterator[Observation]:
    with _tick_age_cache_lock:
        snapshot = list(_tick_age_cache.items())
    for loop_name, age in snapshot:
        yield Observation(age, {"loop": loop_name})


_loop_tick_age_gauge = _meter.create_observable_gauge(
    name="taskq.worker.loop_tick_age_seconds",
    description="Seconds since each interval-driven sibling loop last ticked.",
    unit="s",
    callbacks=[_observe_tick_age],
)


class TaskDumpRecord:
    """One dumped task: name, coroutine qualifier, await-site frames."""

    def __init__(self, task: str, coro: str, sites: list[str]) -> None:
        self.task = task
        self.coro = coro
        self.sites = sites

    def as_dict(self) -> dict[str, object]:
        return {"task": self.task, "coro": self.coro, "sites": self.sites}


def _task_await_sites(task: asyncio.Task[Any]) -> list[str]:
    """'file:line' frames of a suspended task, innermost first."""
    try:
        stack = task.get_stack()
    except Exception:
        return ["?"]
    if not stack:
        return ["completed"]
    return [f"{f.f_code.co_filename}:{f.f_lineno}" for f in stack]


def dump_task_stacks(
    reason: str,
    *,
    detector: str | None = None,
    tasks: list[asyncio.Task[Any]] | None = None,
) -> list[TaskDumpRecord]:
    """Emit one structured log record per live task; return the records.

    The single implementation behind the trip dump, the SIGUSR2 handler,
    the ``/tasks`` endpoint, and the straggler logger. Callers that only
    need the payload (the endpoint) use the return value; ops paths also
    get a raw stderr fallback in case logging itself is the casualty.
    """
    live = tasks if tasks is not None else [t for t in asyncio.all_tasks() if not t.done()]
    records: list[TaskDumpRecord] = []
    for task in live:
        coro = task.get_coro()
        records.append(
            TaskDumpRecord(
                task=task.get_name(),
                coro=getattr(coro, "__qualname__", repr(coro)),
                sites=_task_await_sites(task),
            )
        )
    _log.warning(
        "worker-task-dump",
        kind="worker_task_dump",
        reason=reason,
        detector=detector,
        task_count=len(records),
        tasks=[rec.as_dict() for rec in records],
    )
    print(
        f"=== task dump ({reason}{'/' + detector if detector else ''}): "
        f"{len(records)} live task(s) ===",
        file=sys.stderr,
        flush=True,
    )
    for rec in records:
        print(f"--- {rec.task} {rec.coro} @ {', '.join(rec.sites)}", file=sys.stderr)
    sys.stderr.flush()
    return records


def _flush_metrics_before_exit() -> None:
    """Best-effort OTel flush before ``os._exit``, with a hard wall.

    ``os._exit`` skips the SDK's periodic exporter, so the trip counter
    increment only survives if it is flushed first — but an unbounded
    ``force_flush`` against a hung collector would stall the force-exit it
    precedes (exactly when the process is already known to be wedged).
    Run the flush on a daemon thread and join with the hard
    ``_METRICS_FLUSH_TIMEOUT_SECS`` deadline; both trip paths (``trip()``
    and detector 4's inline exit) share this. Providers without
    ``force_flush`` (the NoOp default) skip cleanly.
    """
    with contextlib.suppress(Exception):
        provider = otel_metrics.get_meter_provider()
        flush = getattr(provider, "force_flush", None)
        if flush is None:
            return

        def _flush() -> None:
            with contextlib.suppress(Exception):
                flush()

        thread = threading.Thread(target=_flush, name="taskq-metrics-flush", daemon=True)
        thread.start()
        thread.join(_METRICS_FLUSH_TIMEOUT_SECS)


def trip(detector: str, reason: str) -> None:
    """Critical log + metric + dump + force-exit. Never returns."""
    _watchdog_trips.add(1, {"detector": detector})
    _log.critical(
        "worker-watchdog-trip",
        kind="worker_watchdog_trip",
        detector=detector,
        reason=reason,
    )
    try:
        dump_task_stacks(reason, detector=detector)
    finally:
        sys.stderr.flush()
        sys.stdout.flush()
        _flush_metrics_before_exit()
        os._exit(EXIT_WATCHDOG)


class LoopLiveness:
    """Per-loop monotonic liveness stamps (detector 2 registry).

    Interval-driven loops call :meth:`tick` once per iteration with
    their period; the staleness budget is ``period * grace_factor`` with
    a 10s floor so tiny test intervals cannot false-trip under load.
    Loops that never tick are not tracked (not started, or event-driven
    and therefore out of scope for this detector).
    """

    def __init__(
        self,
        grace_factor: float = 5.0,
        stale_floor: float = DEFAULT_STALE_FLOOR_SECS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.grace_factor = grace_factor
        self.stale_floor = stale_floor
        self._clock = clock
        self._ticks: dict[str, float] = {}
        self._periods: dict[str, float] = {}
        self._lock = threading.Lock()

    def tick(self, name: str, *, period: float) -> None:
        with self._lock:
            self._ticks[name] = self._clock()
            self._periods[name] = period

    def forget(self, name: str) -> None:
        """Drop a loop's registration entirely.

        For gated loops (e.g. the leadership watchdog): when the gate
        closes, the loop stops ticking through no fault of its own, and a
        lingering registration would false-trip the detector. The loop
        re-registers on its next tick when the gate reopens.
        """
        with self._lock:
            self._ticks.pop(name, None)
            self._periods.pop(name, None)
            with _tick_age_cache_lock:
                _tick_age_cache.pop(name, None)

    def ages(self) -> dict[str, float]:
        """Loop name -> seconds since its last tick (observability).

        Thread-safe: LoopLagWatchdog._armed() calls this from a daemon
        thread while the event-loop thread mutates _ticks via tick() and
        forget(). Without synchronization the dict iteration raises
        RuntimeError: dictionary changed size during iteration, silently
        disabling detector 4 for the life of the process.
        """
        with self._lock:
            now = self._clock()
            ages = {name: now - ts for name, ts in self._ticks.items()}
            with _tick_age_cache_lock:
                _tick_age_cache.clear()
                _tick_age_cache.update(ages)
            return ages

    def stale(self) -> list[str]:
        with self._lock:
            now = self._clock()
            return [
                name
                for name, ts in self._ticks.items()
                if now - ts > max(self._periods[name] * self.grace_factor, self.stale_floor)
            ]


async def loop_watchdog_loop(
    liveness: LoopLiveness,
    shutdown_event: asyncio.Event,
    *,
    check_interval: float = 1.0,
    enabled: bool = True,
) -> None:
    """Trip when an interval-driven sibling loop goes stale (detector 2)."""
    if not enabled:
        return
    while not shutdown_event.is_set():
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(shutdown_event.wait(), timeout=check_interval)
        if shutdown_event.is_set():
            return
        liveness.ages()  # keep the tick-age gauge populated between /ready scrapes
        stale = liveness.stale()
        if stale:
            trip("stale-loop-tick", f"sibling loop(s) stopped ticking: {', '.join(stale)}")
            # Unreachable in production (trip force-exits). If os._exit is
            # ever intercepted, stop rather than re-trip every tick.
            return


class ShutdownWatchdog:
    """Detectors 1 (shutdown hard deadline) and the straggler logger.

    Lives outside the worker TaskGroup: parked on ``shutdown_event``,
    then counts down ``termination_grace_period``. While counting, logs
    still-alive siblings (names + await sites) every *dump_interval* —
    but only once the shutdown has consumed at least
    ``dump_after_fraction`` of its hard budget. A drain still in its
    front half is within expectations and gets silence; one in its back
    half is already abnormal enough to observe, and a genuinely hung
    shutdown still accumulates dumps right up to the trip. On deadline:
    trip. Cancelled on clean exit.
    """

    def __init__(
        self,
        shutdown_event: asyncio.Event,
        *,
        deadline: float,
        dump_interval: float,
        enabled: bool = True,
        on_shutdown_started: Callable[[float], None] | None = None,
        started_at: Callable[[], float | None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        shutdown_started_event: asyncio.Event | None = None,
        dump_after_fraction: float = 0.5,
    ) -> None:
        if not 0.0 < dump_after_fraction < 1.0:
            raise ValueError(
                f"dump_after_fraction must be in (0, 1): at 1.0 the deadline trip "
                f"always fires first, silently disabling straggler dumps — got "
                f"{dump_after_fraction}"
            )
        self._shutdown_event = shutdown_event
        self._deadline = deadline
        self._dump_interval = dump_interval
        self._enabled = enabled
        self._on_shutdown_started = on_shutdown_started
        self._started_at = started_at
        self._clock = clock
        self._shutdown_started_event = shutdown_started_event
        self._dump_gate_secs = deadline * dump_after_fraction
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._enabled:
            self._task = asyncio.create_task(self._run(), name="worker.shutdown_watchdog")

    async def cancel(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            if self._started_at is not None:
                anchored = self._started_at()
                if anchored is not None:
                    _shutdown_duration.record(self._clock() - anchored)

    async def _run(self) -> None:
        if self._shutdown_started_event is not None:
            shutdown_started = asyncio.create_task(self._shutdown_started_event.wait())
            shutdown_task = asyncio.create_task(self._shutdown_event.wait())
            try:
                await asyncio.wait(
                    {shutdown_started, shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                # Cancel the loser AND cover our own cancellation: without
                # the finally, cancelling this task at the wait point leaks
                # both inner event-wait tasks until loop teardown.
                for t in (shutdown_started, shutdown_task):
                    if not t.done():
                        t.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await t
        else:
            await self._shutdown_event.wait()
        t0 = self._clock()
        if self._on_shutdown_started is not None:
            self._on_shutdown_started(t0)
        # Anchor the deadline on the FIRST shutdown signal when known
        # (orchestration spends the cancel/cleanup graces before
        # shutdown_event is set, so anchoring on the event alone would
        # double-count them against termination_grace_period).
        anchored = self._started_at() if self._started_at is not None else None
        if anchored is not None:
            t0 = anchored
        # One record per countdown, not per interval: the front half of the
        # budget is deliberately free of straggler dumps (see the gate
        # below), but it must never be blind — this is how ops can tell the
        # deadline is counting and when the dumps will begin.
        _log.info(
            "shutdown-watchdog-armed",
            kind="shutdown_watchdog_armed",
            deadline_secs=self._deadline,
            dump_after_secs=self._dump_gate_secs,
        )
        while True:
            elapsed = self._clock() - t0
            if elapsed >= self._deadline:
                trip(
                    "shutdown-deadline",
                    f"shutdown still incomplete {elapsed:.1f}s after shutdown_event "
                    f"(deadline {self._deadline:.1f}s)",
                )
            await asyncio.sleep(self._dump_interval)
            # Re-read the clock AFTER the sleep so the gate reflects when
            # the dump would actually fire, not when the iteration began.
            if self._clock() - t0 < self._dump_gate_secs:
                continue
            pending = [
                t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()
            ]
            if pending:
                dump_task_stacks(
                    "shutdown-straggler",
                    detector="shutdown-deadline",
                    tasks=pending,
                )


class LoopLagWatchdog:
    """Detector 4: daemon thread measuring event-loop scheduling lag.

    Arms after *startup_grace* seconds or the first liveness tick,
    whichever comes first — import-heavy startup and DI bootstrap must
    never trip it. When the loop has not scheduled a beat within
    *budget* seconds, dumps ``faulthandler`` thread frames (thread-safe
    off-loop) and trips.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        liveness: LoopLiveness,
        *,
        budget: float,
        startup_grace: float,
        poll_interval: float = 0.5,
        enabled: bool = True,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loop = loop
        self._liveness = liveness
        self._budget = budget
        self._startup_grace = startup_grace
        self._poll_interval = poll_interval
        self._enabled = enabled
        self._clock = clock
        self._last_beat = clock()
        self._started = clock()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="taskq-loop-lag-watchdog",
            daemon=True,
        )

    def start(self) -> None:
        if self._enabled and not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _beat(self) -> None:
        self._last_beat = self._clock()

    def _armed(self) -> bool:
        if self._clock() - self._started >= self._startup_grace:
            return True
        return bool(self._liveness.ages())

    def _run(self) -> None:
        """Thread entry point: never let an exception escape silently.

        A daemon thread that dies takes detector 4 with it — the worker
        would keep running with no protection against a blocked loop and
        no indication that the protection was gone. Log loudly instead of
        vanishing.
        """
        try:
            self._watch()
        # Why bare BaseException: a watchdog thread must never die quietly.
        # Anything escaping _watch is logged rather than lost to the
        # thread's default handler.
        except BaseException as exc:
            _log.error(
                "watchdog-lag-thread-exited",
                kind="watchdog_lag_thread_exited",
                error=repr(exc),
                note="event-loop-lag detection is no longer active in this process",
            )

    def _watch(self) -> None:
        while not self._stop.wait(self._poll_interval):
            if not self._armed():
                continue
            lag = self._clock() - self._last_beat
            if lag > self._budget:
                _watchdog_trips.add(1, {"detector": "event-loop-lag"})
                _log.critical(
                    "worker-watchdog-trip",
                    kind="worker_watchdog_trip",
                    detector="event-loop-lag",
                    reason=f"event loop has not scheduled for {lag:.1f}s",
                )
                print(
                    f"=== watchdog trip: event loop blocked for {lag:.1f}s ===",
                    file=sys.stderr,
                    flush=True,
                )
                faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                sys.stderr.flush()
                # Same flush-before-exit as trip(): without it the
                # event-loop-lag increment of watchdog_trips_total never
                # reaches the exporter — the trip you least want to be
                # guessing about after the fact.
                _flush_metrics_before_exit()
                os._exit(EXIT_WATCHDOG)
                # Unreachable in production. If os._exit is ever intercepted
                # (a test, an embedded host), stop rather than re-trip and
                # re-dump on every subsequent poll.
                return
            try:
                self._loop.call_soon_threadsafe(self._beat)
            except RuntimeError:
                return
