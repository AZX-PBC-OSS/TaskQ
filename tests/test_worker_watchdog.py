"""Unit tests for the in-worker watchdog detectors.

Each detector gets both cases: it trips when it should, and it does NOT
trip on slow-but-alive (the staleness floor and the startup grace are
the assertions that matter — a terminal detector must never false-fire).
``os._exit`` is patched to a recorder that raises a sentinel, so trip
paths are asserted without killing the test process.
"""

import asyncio
import contextlib
import functools
import sys
import time
from collections.abc import Callable
from typing import Any

import pytest
import structlog

from taskq.worker._stall_tally import StallAttributionTally
from taskq.worker._watchdog import (
    EXIT_WATCHDOG,
    LoopLagWatchdog,
    LoopLiveness,
    ShutdownWatchdog,
    attribute_stall,
    dump_task_stacks,
    loop_watchdog_loop,
    trip,
)


class _ExitSentinelError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"os._exit({code})")
        self.code = code


@pytest.fixture
def exit_codes(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    codes: list[int] = []

    def _fake_exit(code: int) -> None:
        codes.append(code)
        raise _ExitSentinelError(code)

    monkeypatch.setattr("taskq.worker._watchdog.os._exit", _fake_exit)
    return codes


def _clock(start: float = 0.0) -> tuple[list[float], Callable[[], float]]:
    t = [start]
    return t, lambda: t[0]


# ── LoopLiveness: floor math ──────────────────────────────────────────


def test_liveness_staleness_respects_floor() -> None:
    """A tiny interval (0.5s x grace 5 = 2.5s) must NOT trip before the
    10s floor — Docker starvation must never fire a terminal detector."""
    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=5.0, clock=clock)
    liveness.tick("heartbeat", period=0.5)

    t[0] += 5.0
    assert liveness.stale() == []

    t[0] += 6.0
    assert liveness.stale() == ["heartbeat"]


def test_liveness_never_watches_a_loop_that_never_ticked() -> None:
    """Event-driven siblings must be invisible to the stale-tick detector.

    The NOTIFY listener parks on shutdown, consumers park on an empty
    queue, and the reload coordinator parks on its event — all park
    indefinitely by design. They deliberately never call ``tick``, and a
    loop with no registration must never become stale no matter how long
    the worker idles; otherwise an idle worker would force-exit itself.
    Detectors 1, 3 and 4 cover these loops instead.
    """
    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=5.0, clock=clock)

    t[0] += 86_400.0  # a day of idling with nothing registered
    assert liveness.stale() == []
    assert liveness.ages() == {}

    # A tracked loop alongside them is still judged on its own budget.
    liveness.tick("heartbeat", period=1.0)
    t[0] += 86_400.0
    assert liveness.stale() == ["heartbeat"]


def test_liveness_staleness_respects_grace_factor() -> None:
    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=5.0, clock=clock)
    liveness.tick("sweep", period=30.0)

    t[0] += 100.0
    assert liveness.stale() == []

    t[0] += 60.0
    assert liveness.stale() == ["sweep"]


def test_liveness_ages_feed_observability() -> None:
    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=5.0, clock=clock)
    liveness.tick("heartbeat", period=0.5)
    t[0] += 2.0
    ages = liveness.ages()
    assert ages == {"heartbeat": 2.0}


# ── Detector 2: stale loop tick ──────────────────────────────────────


async def test_loop_watchdog_trips_on_stale_loop(exit_codes: list[int]) -> None:
    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=1.0, clock=clock)
    liveness.tick("heartbeat", period=0.5)
    t[0] += 60.0
    shutdown = asyncio.Event()
    with pytest.raises(_ExitSentinelError) as exc_info:
        await loop_watchdog_loop(liveness, shutdown, check_interval=0.01)
    assert exc_info.value.code == EXIT_WATCHDOG
    assert exit_codes == [EXIT_WATCHDOG]


async def test_loop_watchdog_does_not_trip_on_alive_loop(exit_codes: list[int]) -> None:
    _, clock = _clock()
    liveness = LoopLiveness(grace_factor=5.0, clock=clock)
    liveness.tick("heartbeat", period=1.0)
    shutdown = asyncio.Event()
    task = asyncio.create_task(loop_watchdog_loop(liveness, shutdown, check_interval=0.01))
    await asyncio.sleep(0.05)
    liveness.tick("heartbeat", period=1.0)
    await asyncio.sleep(0.05)
    assert not task.done()
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)
    assert exit_codes == []


# ── Detector 1: shutdown hard deadline ───────────────────────────────


async def test_shutdown_watchdog_trips_past_deadline(exit_codes: list[int]) -> None:
    """Past termination_grace_period with shutdown incomplete → trip."""
    shutdown = asyncio.Event()
    watchdog = ShutdownWatchdog(shutdown, deadline=0.05, dump_interval=0.01)
    watchdog.start()
    shutdown.set()
    assert watchdog._task is not None
    with pytest.raises(_ExitSentinelError) as exc_info:
        await asyncio.wait_for(watchdog._task, timeout=2.0)
    assert exc_info.value.code == EXIT_WATCHDOG
    assert exit_codes == [EXIT_WATCHDOG]


async def test_shutdown_watchdog_cancelled_on_clean_exit(exit_codes: list[int]) -> None:
    shutdown = asyncio.Event()
    watchdog = ShutdownWatchdog(shutdown, deadline=60.0, dump_interval=1.0)
    watchdog.start()
    shutdown.set()
    await watchdog.cancel()
    assert exit_codes == []


async def test_shutdown_watchdog_logs_stragglers_by_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Straggler dumps fire once shutdown has consumed the back half of its
    hard budget (elapsed >= deadline * 0.5). An ordinary drain inside the
    front half gets silence instead of per-interval stderr spam."""
    _, clock = _clock(start=31.0)  # 31s into a 60s deadline: past the gate

    async def _parked() -> None:
        await asyncio.sleep(60.0)

    straggler = asyncio.create_task(_parked(), name="sibling.parked")
    shutdown = asyncio.Event()
    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=60.0,
        dump_interval=0.01,
        started_at=lambda: 0.0,
        clock=clock,
    )
    watchdog.start()
    shutdown.set()
    await asyncio.sleep(0.1)
    await watchdog.cancel()
    straggler.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await straggler
    err = capsys.readouterr().err
    assert "sibling.parked" in err
    assert "shutdown-straggler" in err


# ── Detector 4: blocked event loop ───────────────────────────────────


class _NeverResponsiveLoop:
    """Loop stand-in whose scheduled beats never land (a blocked loop)."""

    def call_soon_threadsafe(self, callback: Callable[[], None]) -> None:
        pass


def test_loop_lag_watchdog_trips_when_loop_blocked(exit_codes: list[int]) -> None:
    liveness = LoopLiveness()
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        liveness,
        budget=0.05,
        warn_budget=0.01,
        startup_grace=0.0,
    )
    watchdog.start()
    deadline = time.monotonic() + 5.0
    while not exit_codes and time.monotonic() < deadline:
        time.sleep(0.01)
    watchdog.stop()
    assert exit_codes == [EXIT_WATCHDOG], (
        "the loop-lag watchdog thread never force-exited within 5s of crossing the terminal budget"
    )


def test_loop_lag_watchdog_respects_startup_grace(exit_codes: list[int]) -> None:
    liveness = LoopLiveness()
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        liveness,
        budget=0.01,
        warn_budget=0.005,
        startup_grace=60.0,
    )
    watchdog.start()
    time.sleep(0.1)
    watchdog.stop()
    assert exit_codes == []


def test_loop_lag_watchdog_arms_early_on_first_tick(exit_codes: list[int]) -> None:
    """Grace is not fully consumed when the worker demonstrably schedules:
    the first liveness tick arms the detector immediately."""
    _, clock = _clock()
    liveness = LoopLiveness(clock=clock)
    liveness.tick("heartbeat", period=0.5)
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        liveness,
        budget=0.05,
        warn_budget=0.01,
        startup_grace=60.0,
        clock=clock,
    )
    assert watchdog._armed()


# ── Detector 4: the event-loop lag histogram ─────────────────────────


class _HistogramRecorder:
    def __init__(self) -> None:
        self.samples: list[float] = []

    def record(self, value: float, attrs: dict[str, object] | None = None) -> None:
        self.samples.append(value)


async def test_event_loop_lag_histogram_samples_a_blocked_loop(
    monkeypatch: pytest.MonkeyPatch, exit_codes: list[int]
) -> None:
    """taskq.worker.event_loop_lag_seconds is the continuous signal under
    the warn/trip thresholds: a healthy loop samples microseconds per
    beat, and a loop blocked for 0.3 s produces one sample at least that
    long (the beat request outstanding when the block began lands when it
    ends — the requests polled during the block must not re-stamp it)."""
    from taskq.worker import _watchdog as watchdog_mod

    histogram = _HistogramRecorder()
    monkeypatch.setattr(watchdog_mod, "_event_loop_lag", histogram)
    block, poll = 0.3, 0.01
    watchdog = LoopLagWatchdog(
        asyncio.get_running_loop(),
        LoopLiveness(),
        budget=100.0,
        warn_budget=50.0,
        startup_grace=0.0,
        poll_interval=poll,
    )
    watchdog.start()
    try:
        deadline = time.monotonic() + 5.0
        while len(histogram.samples) < 3 and time.monotonic() < deadline:  # noqa: ASYNC110  # Why: the producer is a thread with no event to await; a bounded poll is the seam.
            await asyncio.sleep(poll)
        healthy = list(histogram.samples)
        assert len(healthy) >= 3, "a responsive loop must produce a sample per landed beat"
        assert max(healthy) < block / 2, healthy

        time.sleep(block)  # noqa: ASYNC251  # Why: blocking the loop IS the scenario under test.

        # The beat outstanding across the block lands when it ends and is
        # sampled on the watchdog's next poll; a beat that had landed just
        # before the block may be sampled first, so wait for the block-
        # sized sample rather than the first new one.
        deadline = time.monotonic() + 5.0
        while (  # noqa: ASYNC110  # Why: as above.
            not any(s >= block - poll for s in histogram.samples[len(healthy) :])
            and time.monotonic() < deadline
        ):
            await asyncio.sleep(poll)
        stalled = histogram.samples[len(healthy) :]
        assert stalled and max(stalled) >= block - poll, (
            f"the sample must carry the block, not the last poll's request: {stalled}"
        )
        assert max(stalled) < block + 1.0
        assert exit_codes == []
    finally:
        watchdog.stop()


async def test_event_loop_lag_trip_records_the_stall_before_exit(
    monkeypatch: pytest.MonkeyPatch, exit_codes: list[int]
) -> None:
    """A loop that never recovers trips; the stall observed at the trip is
    recorded so the histogram is not silent about the one lag that
    mattered most."""
    from taskq.worker import _watchdog as watchdog_mod

    histogram = _HistogramRecorder()
    monkeypatch.setattr(watchdog_mod, "_event_loop_lag", histogram)
    monkeypatch.setattr(watchdog_mod, "_flush_metrics_before_exit", lambda: None)
    monkeypatch.setattr("taskq.worker._watchdog.faulthandler.dump_traceback", lambda *a, **k: None)
    t, clock = _clock()
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        LoopLiveness(clock=clock),
        budget=1.0,
        warn_budget=0.5,
        startup_grace=0.0,
        poll_interval=0.01,
        clock=clock,
    )
    watchdog.start()
    try:
        t[0] = 5.0
        deadline = time.monotonic() + 5.0
        while not exit_codes and time.monotonic() < deadline:
            time.sleep(0.01)  # noqa: ASYNC251  # Why: the trip fires on the watchdog thread against a fake loop; nothing here is awaitable.
    finally:
        watchdog.stop()
    assert exit_codes == [EXIT_WATCHDOG]
    assert histogram.samples and max(histogram.samples) >= 1.0, histogram.samples


# ── Detector 4, tier 1: non-terminal lag warning ─────────────────────


class _RecordingLoop:
    """Loop stand-in recording call_soon_threadsafe callbacks.

    Mirrors the real ``AbstractEventLoop.call_soon_threadsafe`` signature:
    a callback plus positional args only (no arbitrary keyword arguments),
    which is why the deferred dump must travel as a ``functools.partial``.
    """

    def __init__(self) -> None:
        self.scheduled: list[tuple[Callable[[], object], tuple[object, ...]]] = []

    def call_soon_threadsafe(
        self, callback: Callable[[], object], *args: object, context: object = None
    ) -> None:
        self.scheduled.append((callback, args))


class _ClosedLoop(_RecordingLoop):
    """Loop stand-in whose call_soon_threadsafe raises like a closed loop."""

    def call_soon_threadsafe(
        self, callback: Callable[[], object], *args: object, context: object = None
    ) -> None:
        raise RuntimeError("Event loop is closed")


def _warn_recorder(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[int], list[dict[str, object]]]:
    """Patch the tier-1 warn counter and faulthandler dump with recorders.

    Returns (counter increments, faulthandler dump kwargs). Counter follows
    the _Recorder idiom of the sibling-crash test; the faulthandler patch
    keeps real thread dumps off the test's stderr and makes the warn tier's
    dump observable.
    """
    from taskq.worker import _watchdog as watchdog_mod

    increments: list[int] = []

    class _CounterRecorder:
        def add(self, amount: int, attrs: dict[str, object]) -> None:
            increments.append(amount)

    monkeypatch.setattr(watchdog_mod, "_loop_lag_warns", _CounterRecorder())

    dumps: list[dict[str, object]] = []
    monkeypatch.setattr(
        "taskq.worker._watchdog.faulthandler.dump_traceback",
        lambda *args, **kwargs: dumps.append(kwargs),
    )
    return increments, dumps


def test_loop_lag_warn_fires_once_and_never_exits(
    monkeypatch: pytest.MonkeyPatch, exit_codes: list[int]
) -> None:
    """Crossing the warn budget emits exactly one tier-1 warning — warn
    counter + faulthandler thread dump — and the watchdog keeps running;
    crossing the terminal budget afterwards still force-exits (tier 2
    unchanged). The latch matters: without it every poll past the warn
    budget would re-warn, spamming metrics and stderr for one stall."""
    increments, dumps = _warn_recorder(monkeypatch)
    t, clock = _clock()
    liveness = LoopLiveness(clock=clock)
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        liveness,
        budget=100.0,
        warn_budget=1.0,
        startup_grace=0.0,
        poll_interval=0.01,
        clock=clock,
    )
    watchdog.start()
    try:
        t[0] = 2.0  # lag 2s: past the warn budget, far under the terminal one
        deadline = time.monotonic() + 5.0
        while len(increments) < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        # Settle through several more polls: a broken latch would re-warn.
        time.sleep(0.1)
        assert increments == [1], (
            f"expected exactly one tier-1 warn for one stall (got {increments})"
        )
        assert dumps and all(
            kwargs.get("all_threads") is True and kwargs.get("file") is sys.stderr
            for kwargs in dumps
        )
        assert exit_codes == []

        t[0] = 101.0  # past the terminal budget: tier 2 must still exit
        deadline = time.monotonic() + 5.0
        while not exit_codes and time.monotonic() < deadline:
            time.sleep(0.01)
        assert exit_codes == [EXIT_WATCHDOG], (
            "tier 2 never force-exited within 5s of crossing the terminal budget "
            "(budget=100.0, lag=101.0)"
        )
    finally:
        watchdog.stop()


def test_loop_lag_warn_latch_resets_on_beat(
    monkeypatch: pytest.MonkeyPatch, exit_codes: list[int]
) -> None:
    """A beat (the loop scheduling again after the stall) clears the warn
    latch, so the NEXT stall gets a fresh tier-1 warning instead of riding
    the first stall's latch — one warn per stall, not one per process."""
    increments, _dumps = _warn_recorder(monkeypatch)
    t, clock = _clock()
    liveness = LoopLiveness(clock=clock)
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        liveness,
        budget=100.0,
        warn_budget=1.0,
        startup_grace=0.0,
        poll_interval=0.01,
        clock=clock,
    )
    watchdog.start()
    try:
        t[0] = 2.0
        deadline = time.monotonic() + 5.0
        while len(increments) < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert increments == [1]

        watchdog._beat()  # loop recovered: last_beat moves to t=2, latch clears
        t[0] = 4.0  # a second stall: lag 2s past the warn budget again
        deadline = time.monotonic() + 5.0
        while len(increments) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.1)  # settle: exactly two warns, not a stream
        assert increments == [1, 1], (
            f"expected the latched warn plus exactly one fresh warn after the beat "
            f"(got {increments})"
        )
        assert exit_codes == []
    finally:
        watchdog.stop()


def test_loop_lag_warn_never_fires_while_unarmed(
    monkeypatch: pytest.MonkeyPatch, exit_codes: list[int]
) -> None:
    """The warn tier obeys the same arming gate as the terminal tier:
    during startup grace with no liveness ticks, no warning and no exit —
    import-heavy startup must never trip either tier (a warn_budget that
    would fire in 10ms if armed proves the gate, not the budget, held)."""
    increments, dumps = _warn_recorder(monkeypatch)
    _, clock = _clock()  # frozen: the clock never leaves the grace window
    liveness = LoopLiveness(clock=clock)  # never ticked: no early arming
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        liveness,
        budget=0.05,
        warn_budget=0.01,
        startup_grace=60.0,
        poll_interval=0.01,
        clock=clock,
    )
    watchdog.start()
    time.sleep(0.2)  # ~20 polls, all disarmed (clock frozen inside grace)
    watchdog.stop()
    assert increments == []
    assert dumps == []
    assert exit_codes == []


def test_loop_lag_warn_schedules_deferred_task_dump(
    monkeypatch: pytest.MonkeyPatch, exit_codes: list[int]
) -> None:
    """The warn tier defers the asyncio task-stack dump onto the loop via
    call_soon_threadsafe — asyncio.all_tasks is not thread-safe, so the
    dump must run ON the loop once it schedules again (hence the
    'loop-lag-recovered' reason). The payload travels as a functools.partial
    carrying the detector label because call_soon_threadsafe forwards no
    keyword arguments of its own. A closed loop (RuntimeError) is swallowed:
    the faulthandler dump already captured the state, and a stall crossing
    both tiers in one poll must still deliver the terminal exit."""
    increments, _dumps = _warn_recorder(monkeypatch)
    loop = _RecordingLoop()
    t, clock = _clock()
    liveness = LoopLiveness(clock=clock)
    watchdog = LoopLagWatchdog(
        loop,  # type: ignore[arg-type]
        liveness,
        budget=100.0,
        warn_budget=1.0,
        startup_grace=0.0,
        poll_interval=0.01,
        clock=clock,
    )
    watchdog.start()
    try:
        t[0] = 2.0
        deadline = time.monotonic() + 5.0
        while not loop.scheduled and time.monotonic() < deadline:
            time.sleep(0.01)
        assert increments == [1], (
            f"the warn tier never fired within 5s of crossing the warn budget (got {increments})"
        )
        # The loop also receives the watchdog's regular beat callbacks
        # (bound methods); the warn tier's contribution is exactly one
        # deferred dump.
        deferred = [
            callback
            for callback, _args in loop.scheduled
            if isinstance(callback, functools.partial)
        ]
        assert len(deferred) == 1
        callback = deferred[0]
        assert callback.func is dump_task_stacks
        assert callback.args == ("loop-lag-recovered",)
        assert callback.keywords == {"detector": "event-loop-lag-warn"}
        assert exit_codes == []
    finally:
        watchdog.stop()

    closed = _ClosedLoop()
    t2, clock2 = _clock()
    watchdog2 = LoopLagWatchdog(
        closed,  # type: ignore[arg-type]
        liveness,
        budget=1.5,
        warn_budget=1.0,
        startup_grace=0.0,
        poll_interval=0.01,
        clock=clock2,
    )
    watchdog2.start()
    try:
        # One stall crossing BOTH tiers at once on a closed loop: the warn
        # tier's deferred dump raises RuntimeError and is swallowed, and
        # the terminal check in the same poll iteration still exits. (The
        # thread then ends on the beat-scheduling return — with the loop
        # closed there is nothing left to watch; the swallow's job is to
        # keep that first iteration alive through the terminal tier.)
        t2[0] = 2.0
        deadline = time.monotonic() + 5.0
        while not exit_codes and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(increments) == 2, (  # both watchdogs warned exactly once
            f"expected both watchdogs to warn exactly once (got {increments})"
        )
        assert exit_codes == [EXIT_WATCHDOG], (
            "the closed-loop watchdog never force-exited within 5s of crossing "
            "both tiers in one poll"
        )
    finally:
        watchdog2.stop()


# ── Trip path and dump shape ──────────────────────────────────────────


async def test_trip_exits_with_watchdog_code(
    exit_codes: list[int], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(_ExitSentinelError) as exc_info:
        trip("test-detector", "unit test trip")
    assert exc_info.value.code == EXIT_WATCHDOG
    assert "task dump (unit test trip/test-detector)" in capsys.readouterr().err


async def test_dump_task_stacks_shape(capsys: pytest.CaptureFixture[str]) -> None:
    async def _parked() -> None:
        await asyncio.sleep(60.0)

    task = asyncio.create_task(_parked(), name="dump.target")
    # Single deterministic yield: one event-loop pass lets the task run its
    # first step (scheduled at create_task, before this coroutine's
    # resumption) so the dump below sees a started task with a real frame —
    # an arbitrary fixed delay added nothing but a race window.
    await asyncio.sleep(0)
    records = dump_task_stacks("unit-test", detector="test")
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert any(rec.task == "dump.target" for rec in records)
    target = next(rec for rec in records if rec.task == "dump.target")
    assert target.coro.endswith("_parked")
    assert target.sites
    assert "dump.target" in capsys.readouterr().err


# ── Detector 3: sibling contract ─────────────────────────────────────


def _spawner_deps() -> Any:
    """Deps stand-in for the spawner contract: no shutdown in progress."""
    from types import SimpleNamespace

    from taskq.worker.shutdown import ShutdownPhase

    return SimpleNamespace(
        shutdown_phase=ShutdownPhase.NONE,
        producer_stop_event=asyncio.Event(),
    )


async def test_spawner_clean_return_while_clear_raises() -> None:
    """A sibling returning cleanly while NO shutdown is in progress is a
    contract violation: shutdown_event is set AND an error raised."""
    from taskq.worker._bootstrap import _make_sibling_spawner

    shutdown = asyncio.Event()
    task: asyncio.Task[None] | None = None
    with pytest.raises(BaseExceptionGroup):
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown, _spawner_deps())

            async def _legit_job() -> None:
                return None

            task = spawn(_legit_job())
    assert shutdown.is_set()
    assert task is not None
    assert type(task.exception()) is RuntimeError
    assert "no shutdown was in progress" in str(task.exception())


async def test_spawner_raise_sets_shutdown_event() -> None:
    from taskq.worker._bootstrap import _make_sibling_spawner

    shutdown = asyncio.Event()
    task: asyncio.Task[None] | None = None
    with pytest.raises(BaseExceptionGroup):
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown, _spawner_deps())

            async def _boom() -> None:
                raise ValueError("sibling died")

            task = spawn(_boom())
    assert shutdown.is_set()
    assert task is not None
    assert type(task.exception()) is ValueError


async def test_spawner_may_return_opt_out() -> None:
    from taskq.worker._bootstrap import _make_sibling_spawner

    shutdown = asyncio.Event()
    async with asyncio.TaskGroup() as tg:
        spawn = _make_sibling_spawner(tg, shutdown, _spawner_deps())

        async def _poll_fallback() -> None:
            return None

        task = spawn(_poll_fallback(), may_return=True)
        await task
    assert not shutdown.is_set()


async def test_spawner_clean_return_allowed_during_drain() -> None:
    """The graceful SIGTERM path must NOT trip the contract: producer_loop
    exits on producer_stop_event / phase entry long before shutdown_event
    is set at the end of orchestration."""
    from types import SimpleNamespace

    from taskq.worker._bootstrap import _make_sibling_spawner
    from taskq.worker.shutdown import ShutdownPhase

    shutdown = asyncio.Event()
    for deps in (
        SimpleNamespace(
            shutdown_phase=ShutdownPhase.DRAINING,
            producer_stop_event=asyncio.Event(),
        ),
        SimpleNamespace(
            shutdown_phase=ShutdownPhase.NONE,
            producer_stop_event=asyncio.Event(),
        ),
    ):
        if deps.shutdown_phase is ShutdownPhase.NONE:
            deps.producer_stop_event.set()
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown, deps)

            async def _producer_exit() -> None:
                return None

            await spawn(_producer_exit())
    assert not shutdown.is_set()


async def test_spawner_may_return_still_guards_exceptions() -> None:
    """may_return skips only the clean-return check: a PG-facing loop's
    exception still sets shutdown_event (and counts the crash)."""
    from taskq.worker._bootstrap import _make_sibling_spawner

    shutdown = asyncio.Event()
    task: asyncio.Task[None] | None = None
    with pytest.raises(BaseExceptionGroup):
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown, _spawner_deps())

            async def _notify_died() -> None:
                raise OSError(111, "Connect call failed")

            task = spawn(_notify_died(), may_return=True)
    assert shutdown.is_set()
    assert task is not None
    assert isinstance(task.exception(), OSError)


async def test_spawner_unwedges_a_cancellation_swallowing_sibling() -> None:
    """The crash path must free siblings that absorb their cancellation.

    Regression (PG-restart chaos): a leader sweep raised into the worker's
    TaskGroup, the group cancelled every sibling, but a sibling that
    swallows ``CancelledError`` — the ``suppress`` in the park-vs-shutdown
    races, or a consumer treating cancellation as a cooperative job-cancel
    — re-checked ``shutdown_event``, found it clear, and parked again.
    ``__aexit__`` then waited forever: the worker hung with no traceback,
    the ExceptionGroup never delivered. Setting shutdown_event on the
    failure path is what lets such a sibling notice and return.
    """
    from taskq.worker._bootstrap import _make_sibling_spawner

    shutdown = asyncio.Event()
    iterations = 0

    async def _swallows_cancellation() -> None:
        nonlocal iterations
        while not shutdown.is_set():
            iterations += 1
            park = asyncio.create_task(asyncio.Event().wait())
            shut = asyncio.create_task(shutdown.wait())
            try:
                await asyncio.wait({park, shut}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in (park, shut):
                    if not task.done():
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task

    async def _dies_on_dead_pg() -> None:
        await asyncio.sleep(0.01)
        raise OSError(-2, "Name or service not known")

    async def _body() -> None:
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown, _spawner_deps())
            spawn(_swallows_cancellation())
            spawn(_dies_on_dead_pg())

    # Pre-fix this never returns: the swallower re-parks forever.
    with pytest.raises(BaseExceptionGroup) as exc_info:
        await asyncio.wait_for(_body(), timeout=5.0)

    assert any(isinstance(e, OSError) for e in exc_info.value.exceptions)
    assert shutdown.is_set()
    assert iterations >= 1


# ── leader.watchdog demotion must leave the registry ───────────────────────


async def test_demoted_leader_watchdog_never_goes_stale() -> None:
    """Leadership loss must remove leader.watchdog from LoopLiveness.

    Regression: the registration lingered after demotion, so ~grace
    seconds after every ordinary leadership change (probe failure,
    election churn) detector 2 would force-exit a healthy non-leader
    worker. forget() runs when the inner loop is left, covering break,
    gate-close and demotion alike.
    """
    _, clock = _clock()
    liveness = LoopLiveness(grace_factor=5.0, clock=clock)
    liveness.tick("leader.watchdog", period=5.0)
    liveness.forget("leader.watchdog")
    assert liveness.stale() == []
    assert "leader.watchdog" not in liveness.ages()


# ── Graceful drain does NOT trip detector 1 ────────────────────────────────


async def test_shutdown_watchdog_silent_during_graceful_drain(
    exit_codes: list[int],
) -> None:
    """A drain completing inside the grace budget must not trip — the
    detector's negative case, guarding the watchdog itself against being
    the regression source."""
    shutdown = asyncio.Event()
    watchdog = ShutdownWatchdog(shutdown, deadline=0.3, dump_interval=0.05)
    watchdog.start()
    shutdown.set()
    await asyncio.sleep(0.1)  # legitimate drain work inside the budget
    await watchdog.cancel()
    assert exit_codes == []


async def test_shutdown_watchdog_anchors_on_first_shutdown_signal(
    exit_codes: list[int],
) -> None:
    """The deadline counts from shutdown_started_at (first signal), not
    from shutdown_event — the graces must not be double-counted."""
    shutdown = asyncio.Event()
    started_at = time.monotonic() - 100.0  # signal arrived 100s ago
    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=10.0,
        dump_interval=0.01,
        started_at=lambda: started_at,
    )
    watchdog.start()
    shutdown.set()  # event set only now, but the anchor is 100s old
    assert watchdog._task is not None
    with pytest.raises(_ExitSentinelError) as exc_info:
        await asyncio.wait_for(watchdog._task, timeout=2.0)
    assert exc_info.value.code == EXIT_WATCHDOG


# ── Tick-age gauge stays populated between /ready scrapes ──────────────────


async def test_loop_watchdog_populates_tick_age_cache(exit_codes: list[int]) -> None:
    from taskq.worker import _watchdog as watchdog_mod

    watchdog_mod._tick_age_cache.clear()
    _, clock = _clock()
    liveness = LoopLiveness(grace_factor=5.0, clock=clock)
    liveness.tick("heartbeat", period=0.5)
    shutdown = asyncio.Event()
    task = asyncio.create_task(loop_watchdog_loop(liveness, shutdown, check_interval=0.01))
    # Bounded poll on the observable (the cache the watchdog loop writes
    # on each check pass) instead of a fixed 0.05s sleep that raced the
    # first check under load.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5.0
    while "heartbeat" not in watchdog_mod._tick_age_cache:
        assert loop.time() < deadline, (
            "loop_watchdog_loop never populated the tick-age cache within 5s"
        )
        await asyncio.sleep(0.01)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)
    assert "heartbeat" in watchdog_mod._tick_age_cache
    watchdog_mod._tick_age_cache.clear()


# ── Crash counter: crash counts, cancellation does not ─────────────────────


async def test_sibling_crash_counter_counts_crashes_not_cancellations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import _bootstrap as bootstrap_mod

    increments: list[int] = []

    class _Recorder:
        def add(self, amount: int, attrs: dict[str, object]) -> None:
            increments.append(amount)

    monkeypatch.setattr(bootstrap_mod, "_sibling_crashes", _Recorder())

    from taskq.worker._bootstrap import _make_sibling_spawner

    shutdown = asyncio.Event()

    async def _boom() -> None:
        raise ValueError("crash")

    async def _parked() -> None:
        await asyncio.sleep(60.0)

    with pytest.raises(BaseExceptionGroup):
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown, _spawner_deps())
            spawn(_boom())
            spawn(_parked())

    assert increments == [1], (
        f"one crash cancelled one sibling; counter must record 1 crash, "
        f"not 1 crash + 1 cancellation: {increments}"
    )


# ── Detector 4: the stall classifier and attribution ─────────────────


def _stall_watchdog(clock: Callable[[], float], **overrides: Any) -> LoopLagWatchdog:
    """A lag watchdog for classifier tests: poll and warn budgets sized so
    a single advanced-clock step crosses the warn tier, and the terminal
    budget so large the trip tier is out of the way."""
    defaults: dict[str, Any] = {
        "budget": 100.0,
        "warn_budget": 1.0,
        "startup_grace": 0.0,
        "poll_interval": 0.5,
        "clock": clock,
    }
    defaults.update(overrides)
    return LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        LoopLiveness(clock=clock),
        **defaults,
    )


def _attributed_events(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in logs if e["event"] == "event-loop-stall-attributed"]


def test_classifier_blocking_call_when_the_watchdog_thread_keeps_ticking() -> None:
    """Loop lag with a quiet watchdog thread (its own waits on time) is a
    synchronous call that RELEASED the GIL: the sampling is exact because
    the watchdog thread keeps running while the loop cannot."""
    t, clock = _clock()
    tally = StallAttributionTally()
    watchdog = _stall_watchdog(clock, stall_tally=tally)
    with structlog.testing.capture_logs() as logs:
        t[0] += 2.0  # one poll, lag 2s past the 1s warn budget
        watchdog._poll()
    events = _attributed_events(logs)
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "blocking_call"
    assert event["actor"] is None  # no registered actor in the sampled stack
    assert event["frame"] is not None
    assert event["samples"] == 1
    assert "asyncio.to_thread" in event["remedy"]
    assert tally.snapshot() == {"_unattributed_": {"blocking_call": 1}}


def test_classifier_gil_held_when_the_watchdogs_own_waits_overshoot() -> None:
    """Loop lag with the watchdog thread's OWN wakeup starved is the
    interpreter held: the overshoot of one wait past a full extra interval
    is the bystander signal a blocking call that released the GIL cannot
    produce."""
    t, clock = _clock()
    watchdog = _stall_watchdog(clock)
    watchdog._note_own_wait(1.5)  # waited 1.5s for a 0.5s interval
    with structlog.testing.capture_logs() as logs:
        t[0] += 2.0
        watchdog._poll()
    events = _attributed_events(logs)
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "gil_held"
    assert "chunk" in event["remedy"]


def test_classifier_threshold_tolerates_scheduler_jitter() -> None:
    """A wait may overshoot by scheduler jitter up to a full extra
    interval without flipping the kind; past it, the same lag is GIL
    pressure. The boundary sits AT the extra interval, not inside it."""
    t, clock = _clock()
    watchdog = _stall_watchdog(clock)
    watchdog._note_own_wait(1.0)  # overshoot exactly one interval: still jitter
    with structlog.testing.capture_logs() as logs:
        t[0] += 2.0
        watchdog._poll()
    assert _attributed_events(logs)[0]["kind"] == "blocking_call"

    watchdog_two = _stall_watchdog(clock)
    watchdog_two._note_own_wait(1.01)  # one interval plus one tick: pressure
    with structlog.testing.capture_logs() as logs:
        t[0] += 2.0
        watchdog_two._poll()
    assert _attributed_events(logs)[0]["kind"] == "gil_held"


def test_attribution_does_not_fire_below_the_warn_budget() -> None:
    t, clock = _clock()
    watchdog = _stall_watchdog(clock)
    with structlog.testing.capture_logs() as logs:
        t[0] += 0.5  # lag 0.5s, under the 1s warn budget
        watchdog._poll()
    assert _attributed_events(logs) == []


def test_stall_sampling_keeps_a_bounded_ring() -> None:
    t, clock = _clock()
    watchdog = _stall_watchdog(clock, warn_budget=0.1)
    for _ in range(30):
        t[0] += 0.2
        watchdog._poll()
    with structlog.testing.capture_logs():
        pass
    assert len(watchdog._stall_samples) <= 16


def test_attribute_stall_walks_outward_to_the_actor_boundary() -> None:
    """The sampled stack is walked from the blocking frame OUTWARD: the
    first frame whose code object is a registered actor names the actor,
    and the innermost non-taskq frame is the cited file:line:function."""
    marker_code = test_attribute_stall_walks_outward_to_the_actor_boundary.__code__
    samples = [
        (
            ("/app/worker.py", 10, "dispatcher", 1),
            ("/app/myapp/actors.py", 42, "send_email", id(marker_code)),
        ),
    ]
    actor, frame, _stack = attribute_stall(samples, {id(marker_code): "send_email"})
    assert actor == "send_email"
    assert frame == "/app/worker.py:10:dispatcher"


def test_attribute_stall_without_an_actor_still_cites_the_frame() -> None:
    samples = [(("/usr/lib/python3/subprocess.py", 7, "wait", 1),)]
    actor, frame, _stack = attribute_stall(samples, {})
    assert actor is None
    assert frame == "/usr/lib/python3/subprocess.py:7:wait"


def test_unique_job_id_carried_only_when_exactly_one_job_matches() -> None:
    _t, clock = _clock()
    watchdog = _stall_watchdog(
        clock,
        list_running_jobs=lambda: [("send_email", "job-1"), ("resize_image", "job-2")],
    )
    assert watchdog._unique_job_id("send_email") == "job-1"
    assert watchdog._unique_job_id("nobody") is None
    assert watchdog._unique_job_id(None) is None

    ambiguous = _stall_watchdog(
        clock,
        list_running_jobs=lambda: [("send_email", "job-1"), ("send_email", "job-2")],
    )
    assert ambiguous._unique_job_id("send_email") is None


def test_trip_tier_emits_the_attribution_before_the_exit(
    monkeypatch: pytest.MonkeyPatch, exit_codes: list[int]
) -> None:
    """The terminal tier attributes the stall too: the trip is the last
    thing the process ever reports, and it must name the actor that
    forced it. _warn_recorder keeps the real thread dumps off stderr."""
    _increments, _dumps = _warn_recorder(monkeypatch)
    liveness = LoopLiveness()
    watchdog = LoopLagWatchdog(
        _NeverResponsiveLoop(),  # type: ignore[arg-type]
        liveness,
        budget=0.05,
        warn_budget=0.01,
        startup_grace=0.0,
    )
    with structlog.testing.capture_logs() as logs:
        watchdog.start()
        deadline = time.monotonic() + 5.0
        while not exit_codes and time.monotonic() < deadline:
            time.sleep(0.01)
        watchdog.stop()
    assert exit_codes == [EXIT_WATCHDOG]
    events = _attributed_events(logs)
    assert events, "the trip tier must emit an attributed stall warning"
    assert any(e["lag_seconds"] >= 0.05 for e in events)
