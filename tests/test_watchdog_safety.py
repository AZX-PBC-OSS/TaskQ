"""Regression tests for worker watchdog and health safety invariants.

Each test validates a specific behavioral contract that was found violated
during review. These are permanent regression tests — they describe what
the code must do, not what a PR comment said.
"""

import asyncio
import contextlib
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import asyncpg
import pytest

from taskq.worker._watchdog import (
    LoopLiveness,
    ShutdownWatchdog,
)
from taskq.worker.deps import WorkerDeps


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


# ── Producer liveness must be forgotten when the loop exits during drain ─
#
# orchestrate_shutdown sets producer_stop_event in Phase 1 but shutdown_event
# only at the very end. The producer exits its while-loop (stops ticking)
# while loop_watchdog_loop is still running. Without forget("producer"),
# detector 2 sees a stale loop and kills the worker mid-drain.


async def test_producer_loop_forgets_liveness_registration_on_exit() -> None:
    """Behavioral pin for the drain fix: the REAL producer_loop must call
    liveness.forget('producer') when it exits. If the forget line in
    run.py is removed, the registration goes stale and detector 2 kills a
    normally-draining worker — this test fails without it."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.worker.deps import WorkerDeps
    from taskq.worker.run import producer_loop

    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=1.0, stale_floor=10.0, clock=clock)

    class _Backend:
        async def dispatch_batch(
            self,
            *,
            worker_id: object,
            queues: object,
            limit: object,
            lock_lease: object,
        ) -> list[object]:
            return []

    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=0.05,
        notify_poll_interval=0.05,
        max_concurrency=4,
    )
    deps = cast(WorkerDeps, SimpleNamespace(settings=settings, liveness=liveness))
    local_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=4)
    shutdown_event = asyncio.Event()
    producer_stop_event = asyncio.Event()

    task = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown_event,
            producer_stop_event,
            backend=cast(Backend, _Backend()),
            worker_id=uuid4(),
        )
    )
    try:
        for _ in range(100):
            if "producer" in liveness.ages():
                break
            await asyncio.sleep(0.01)
        assert "producer" in liveness.ages(), "producer must tick once per iteration"

        # Phase 1 of orchestrate_shutdown: stop the producer while the
        # rest of the worker keeps draining (shutdown_event NOT set).
        producer_stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)

        assert "producer" not in liveness.ages(), (
            "producer_loop must forget its liveness registration on exit — "
            "a lingering registration goes stale during the drain and trips "
            "detector 2"
        )
        t[0] += 11.0  # past the staleness budget
        assert liveness.stale() == [], (
            f"detector 2 would trip on the exited producer: {liveness.stale()}"
        )
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# ── LoopLiveness must be thread-safe ─────────────────────────────────────
#
# LoopLagWatchdog._armed() calls ages() from a daemon thread while the
# event-loop thread mutates _ticks via tick() and forget(). Without
# synchronization, ages() raises RuntimeError: dictionary changed size
# during iteration, the _run exception handler logs once, and detector 4
# is silently disabled for the life of the process.


def test_loopliveness_ages_thread_safe_against_concurrent_tick() -> None:
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(0.000001)
    try:
        liveness = LoopLiveness()
        liveness.tick("heartbeat", period=1.0)

        errors: list[Exception] = []
        stop = threading.Event()

        def _reader() -> None:
            while not stop.is_set():
                try:
                    liveness.ages()
                except RuntimeError as e:
                    errors.append(e)
                    return

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()

        for i in range(50_000):
            liveness.tick(f"loop_{i}", period=1.0)
            if errors:
                break

        stop.set()
        reader.join(timeout=5.0)

        assert not errors, f"ages() must be thread-safe against concurrent tick(): {errors}"
    finally:
        sys.setswitchinterval(old_interval)


def test_loopliveness_ages_thread_safe_against_concurrent_forget() -> None:
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(0.000001)
    try:
        liveness = LoopLiveness()
        for i in range(20):
            liveness.tick(f"loop_{i}", period=1.0)

        errors: list[Exception] = []
        stop = threading.Event()

        def _reader() -> None:
            while not stop.is_set():
                try:
                    liveness.ages()
                except RuntimeError as e:
                    errors.append(e)
                    return

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()

        for i in range(20):
            liveness.forget(f"loop_{i}")
            if errors:
                break

        stop.set()
        reader.join(timeout=2.0)

        assert not errors, f"ages() must be thread-safe against concurrent forget(): {errors}"
    finally:
        sys.setswitchinterval(old_interval)


def test_tick_age_gauge_callback_thread_safe_against_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The OTel observable-gauge callback runs on the SDK reader thread and
    reads the module-global _tick_age_cache while ages() and forget() mutate
    it. Same failure shape as the _ticks race — RuntimeError from a dict
    changing size mid-iteration — except it silently breaks the gauge
    instead of taking detector 4 down. The slow items() forces the collision
    window deterministically instead of relying on scheduler luck."""
    import taskq.worker._watchdog as mod

    class _SlowItemsDict(dict[str, float]):  # type: ignore[type-arg]
        def items(self) -> Iterator[tuple[str, float]]:  # type: ignore[override]
            for item in super().items():
                time.sleep(0.001)  # widen the iteration window: force a thread switch
                yield item

    monkeypatch.setattr(mod, "_tick_age_cache", _SlowItemsDict())

    liveness = LoopLiveness()
    for i in range(20):
        liveness.tick(f"loop_{i}", period=1.0)
    liveness.ages()  # populate the cache

    errors: list[Exception] = []
    stop = threading.Event()

    def _reader() -> None:
        while not stop.is_set():
            try:
                for _observation in mod._observe_tick_age(None):  # type: ignore[arg-type]
                    pass
            except RuntimeError as e:
                errors.append(e)
                return

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()

    for i in range(200):
        liveness.ages()
        liveness.forget(f"loop_{i % 20}")
        liveness.tick(f"loop_{i % 20}", period=1.0)
        if errors:
            break

    stop.set()
    reader.join(timeout=10.0)

    assert not errors, (
        f"the tick-age gauge callback must be thread-safe against ages()/forget(): {errors}"
    )


# ── trip() must flush OTel metrics before os._exit ───────────────────────
#
# os._exit skips atexit handlers and finalizers, so the OTel SDK's periodic
# exporter never runs. The watchdog_trips_total increment is lost, meaning
# you cannot alert on watchdog trips — the primary thing you'd alert on.
# trip() must call force_flush before exiting, and must still exit when the
# provider has no flush capability.


async def test_trip_flushes_metrics_before_force_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import _watchdog as mod

    events: list[str] = []

    class _Provider:
        def force_flush(self, timeout_millis: int = 30_000) -> bool:
            events.append("force_flush")
            return True

    def _fake_exit(code: int) -> None:
        events.append(f"exit:{code}")
        raise _ExitSentinelError(code)

    monkeypatch.setattr(mod.os, "_exit", _fake_exit)
    monkeypatch.setattr(mod.otel_metrics, "get_meter_provider", lambda: _Provider())

    with pytest.raises(_ExitSentinelError):
        mod.trip("test-detector", "ordering check")

    assert events == ["force_flush", f"exit:{mod.EXIT_WATCHDOG}"], (
        "force_flush must run BEFORE os._exit or the watchdog_trips_total "
        f"increment is never exported. Got: {events}"
    )


async def test_trip_still_exits_when_meter_provider_cannot_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import _watchdog as mod

    events: list[str] = []

    def _fake_exit(code: int) -> None:
        events.append(f"exit:{code}")
        raise _ExitSentinelError(code)

    monkeypatch.setattr(mod.os, "_exit", _fake_exit)
    monkeypatch.setattr(mod.otel_metrics, "get_meter_provider", lambda: object())

    with pytest.raises(_ExitSentinelError):
        mod.trip("test-detector", "no-op provider")

    assert events == [f"exit:{mod.EXIT_WATCHDOG}"], (
        f"A provider without force_flush must not block the exit. Got: {events}"
    )


async def test_trip_flush_is_bounded_against_a_hung_exporter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """force_flush has no usable timeout against a hung OTLP collector (the
    gRPC exporter ignores timeout_millis — opentelemetry#2663), so an
    unbounded flush would stall the force-exit it precedes — exactly when
    the process is already known to be wedged. The flush must run on a
    thread with a hard join deadline so trip() still exits promptly."""
    from taskq.worker import _watchdog as mod

    exporter_replied = threading.Event()

    class _HungProvider:
        def force_flush(self, timeout_millis: int = 30_000) -> bool:
            exporter_replied.wait()  # collector accepted the connection, never replies
            return True

    monkeypatch.setattr(mod.otel_metrics, "get_meter_provider", lambda: _HungProvider())
    monkeypatch.setattr(mod, "_METRICS_FLUSH_TIMEOUT_SECS", 0.2)

    codes: list[int] = []

    def _fake_exit(code: int) -> None:
        codes.append(code)
        raise _ExitSentinelError(code)

    monkeypatch.setattr(mod.os, "_exit", _fake_exit)

    outcome: list[str] = []

    def _run() -> None:
        try:
            mod.trip("test-detector", "hung exporter")
        except _ExitSentinelError:
            outcome.append("exited")

    thread = threading.Thread(target=_run, daemon=True)
    started = time.monotonic()
    thread.start()
    try:
        thread.join(timeout=10.0)

        assert outcome == ["exited"], (
            "trip() must force-exit even when the exporter never answers — "
            "an unbounded force_flush turns the watchdog's exit into a second hang"
        )
        assert codes == [mod.EXIT_WATCHDOG]
        assert time.monotonic() - started < 5.0, (
            f"trip() must not wait on the exporter beyond the flush deadline "
            f"(took {time.monotonic() - started:.1f}s)"
        )
    finally:
        exporter_replied.set()  # release the parked flush thread; do not leak it


# ── detector 4 (event-loop lag) must flush before its own exit path ─────
#
# _watch() does not go through trip(): it logs, dumps faulthandler frames,
# and calls os._exit inline. Without the same force_flush the
# watchdog_trips_total{detector="event-loop-lag"} increment is dropped —
# the trip you least want to be guessing about after the fact.


async def test_lag_watchdog_flushes_metrics_before_force_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import _watchdog as mod

    events: list[str] = []

    class _Provider:
        def force_flush(self, timeout_millis: int = 30_000) -> bool:
            events.append("force_flush")
            return True

    def _fake_exit(code: int) -> None:
        events.append(f"exit:{code}")
        raise _ExitSentinelError(code)

    monkeypatch.setattr(mod.os, "_exit", _fake_exit)
    monkeypatch.setattr(mod.otel_metrics, "get_meter_provider", lambda: _Provider())
    monkeypatch.setattr(
        mod.faulthandler, "dump_traceback", lambda **_kwargs: None
    )  # keep test stderr readable; the frames are not under test

    t, clock = _clock()
    watchdog = mod.LoopLagWatchdog(
        asyncio.get_running_loop(),
        LoopLiveness(clock=clock),
        budget=30.0,
        startup_grace=0.0,  # armed immediately
        poll_interval=0.01,
        clock=clock,
    )
    t[0] = 100.0  # first poll sees 100s of lag — over the 30s budget

    with pytest.raises(_ExitSentinelError):
        watchdog._watch()

    assert events == ["force_flush", f"exit:{mod.EXIT_WATCHDOG}"], (
        "detector 4's inline exit path must flush metrics BEFORE os._exit, "
        f"same as trip(). Got: {events}"
    )


# ── _shutdown_duration histogram must actually be recorded ───────────────
#
# The histogram is created but .record() is never called anywhere in the
# module, so it exports nothing — a missing series rather than a zero. The
# clean-teardown path (cancel) must record the anchored elapsed seconds.


async def test_shutdown_duration_recorded_on_clean_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import _watchdog as mod

    recorded: list[float] = []

    class _Histogram:
        def record(self, value: float, attributes: object = None) -> None:
            recorded.append(value)

    monkeypatch.setattr(mod, "_shutdown_duration", _Histogram())

    t, clock = _clock(start=100.0)
    watchdog = ShutdownWatchdog(
        asyncio.Event(),
        deadline=60.0,
        dump_interval=0.01,
        started_at=lambda: 100.0,
        clock=clock,
    )
    watchdog.start()
    t[0] = 123.0

    await watchdog.cancel()

    assert recorded == [23.0], (
        "cancel() must record clock()-started_at so the "
        f"shutdown_duration_seconds series is populated. Got: {recorded}"
    )


async def test_shutdown_duration_skipped_when_shutdown_never_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import _watchdog as mod

    recorded: list[float] = []

    class _Histogram:
        def record(self, value: float, attributes: object = None) -> None:
            recorded.append(value)

    monkeypatch.setattr(mod, "_shutdown_duration", _Histogram())

    watchdog = ShutdownWatchdog(
        asyncio.Event(),
        deadline=60.0,
        dump_interval=0.01,
        started_at=lambda: None,
        clock=_clock()[1],
    )
    watchdog.start()

    await watchdog.cancel()

    assert recorded == [], (
        f"No shutdown signal → no duration sample (missing series, not a "
        f"bogus zero). Got: {recorded}"
    )


# ── Health socket must be secured before or at bind time ─────────────────
#
# os.chmod(0o600) runs AFTER start_unix_server has already bound the socket
# with 0777 & ~umask (typically 0755). Between bind and chmod any local
# process can connect and pull a /tasks stack dump when
# TASKQ_HEALTH_TASKS_ENABLED=true.


async def test_health_socket_secured_when_tasks_enabled(tmp_path: Path) -> None:
    """When TASKQ_HEALTH_TASKS_ENABLED=true, the socket must be created
    owner-only from bind time — no window where it is world-accessible.
    The fix uses umask before bind instead of chmod after bind.
    """
    import os
    import stat
    from types import SimpleNamespace
    from typing import cast

    from taskq.settings import WorkerSettings
    from taskq.worker.health import HealthServer

    sock_path = str(tmp_path / "health.sock")
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEALTH_SOCKET_PATH": sock_path,
            "TASKQ_HEALTH_TASKS_ENABLED": "true",
        }
    )
    deps = cast(WorkerDeps, SimpleNamespace(settings=settings))

    server = HealthServer()
    await server.start(deps)
    try:
        mode = stat.S_IMODE(os.stat(sock_path).st_mode)
        assert mode & 0o077 == 0, (
            f"Health socket must be owner-only (no group/other bits) when "
            f"health_tasks_enabled=true, got {oct(mode)}"
        )
    finally:
        await server.stop()


# ── watchdog_enabled must gate detector 3's enforcement — not its signal ──
#
# The setting description says it switches off all four detectors, but
# detector 3 (sibling spawner clean-return contract) ran regardless of the
# flag. The exception path of the spawner is deliberately NOT gated: a
# crashing sibling must always signal shutdown, watchdog or not. The
# clean-return ERROR LOG is not gated either: with enforcement off, the log
# is the only signal that the worker is running half-staffed.
#
# Readiness is likewise NOT gated: watchdog_enabled switches off the
# force-exit detectors, but a worker with dead loops must still report
# NotReady — otherwise the zombie keeps taking traffic with the one
# remaining signal suppressed.


def _spawner_deps(*, watchdog_enabled: bool) -> WorkerDeps:
    from types import SimpleNamespace
    from typing import cast

    from taskq.worker.shutdown import ShutdownPhase

    return cast(
        WorkerDeps,
        SimpleNamespace(
            shutdown_phase=ShutdownPhase.NONE,
            producer_stop_event=asyncio.Event(),
            settings=SimpleNamespace(watchdog_enabled=watchdog_enabled),
        ),
    )


async def test_sibling_clean_return_tolerated_when_watchdog_disabled() -> None:
    from taskq.worker._bootstrap import _make_sibling_spawner

    async def _clean() -> None:
        return

    shutdown_event = asyncio.Event()
    async with asyncio.TaskGroup() as tg:
        spawn = _make_sibling_spawner(tg, shutdown_event, _spawner_deps(watchdog_enabled=False))
        spawn(_clean())

    assert not shutdown_event.is_set(), (
        "watchdog_enabled=False: a clean sibling return must not trip detector 3"
    )


async def test_sibling_clean_return_still_logs_when_watchdog_disabled() -> None:
    """Enforcement is gated, the signal is not: with watchdog_enabled=False a
    sibling returning cleanly outside shutdown must still emit the error log —
    otherwise the worker runs half-staffed with no log, no metric, no signal."""
    import structlog.testing

    from taskq.worker._bootstrap import _make_sibling_spawner

    async def _clean() -> None:
        return

    shutdown_event = asyncio.Event()
    with structlog.testing.capture_logs() as captured:
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown_event, _spawner_deps(watchdog_enabled=False))
            spawn(_clean())

    assert not shutdown_event.is_set()
    assert any(
        entry.get("event") == "sibling-returned-unexpectedly" and entry.get("log_level") == "error"
        for entry in captured
    ), (
        "the sibling-returned-unexpectedly error log must be unconditional — "
        f"only shutdown_event.set() and the raise are gated: {captured}"
    )


async def test_sibling_clean_return_trips_detector3_when_watchdog_enabled() -> None:
    from taskq.worker._bootstrap import _make_sibling_spawner

    async def _clean() -> None:
        return

    shutdown_event = asyncio.Event()
    with pytest.raises(BaseExceptionGroup) as exc_info:
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown_event, _spawner_deps(watchdog_enabled=True))
            spawn(_clean())

    assert shutdown_event.is_set(), (
        "watchdog_enabled=True: a clean sibling return with no shutdown in "
        "progress must set shutdown_event"
    )
    assert any(
        isinstance(e, RuntimeError) and "returned cleanly" in str(e)
        for e in exc_info.value.exceptions
    ), f"expected RuntimeError from detector 3, got {exc_info.value.exceptions!r}"


async def test_sibling_exception_always_signals_shutdown() -> None:
    """The exception path is NOT gated by watchdog_enabled: a crashing
    sibling must set shutdown_event even when the watchdog is disabled."""
    from taskq.worker._bootstrap import _make_sibling_spawner

    async def _failing() -> None:
        raise ValueError("boom")

    shutdown_event = asyncio.Event()
    with pytest.raises(BaseExceptionGroup) as exc_info:
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown_event, _spawner_deps(watchdog_enabled=False))
            spawn(_failing())

    assert shutdown_event.is_set(), (
        "a sibling exception must set shutdown_event regardless of watchdog_enabled"
    )
    assert any(isinstance(e, ValueError) for e in exc_info.value.exceptions)


class _PingConn:
    async def execute(self, *args: object, **kwargs: object) -> str:
        return "SELECT 1"


class _PingPool:
    def acquire(self, **kwargs: object) -> object:
        class _CM:
            async def __aenter__(self) -> _PingConn:
                return _PingConn()

            async def __aexit__(self, *args: object) -> bool:
                return False

        return _CM()


def _health_deps(liveness: LoopLiveness, *, watchdog_enabled: bool) -> WorkerDeps:
    from types import SimpleNamespace
    from typing import cast

    from taskq.worker.shutdown import ShutdownPhase

    return cast(
        WorkerDeps,
        SimpleNamespace(
            shutdown_phase=ShutdownPhase.NONE,
            dispatcher_pool=_PingPool(),
            heartbeat_pool=_PingPool(),
            settings=SimpleNamespace(
                health_pg_ping_timeout=0.2,
                max_heartbeat_failures=3,
                redis_url=None,
                watchdog_enabled=watchdog_enabled,
            ),
            is_leader=SimpleNamespace(is_set=lambda: False),
            active_jobs=SimpleNamespace(count=lambda: 0),
            heartbeat_failures=0,
            redis_client=None,
            liveness=liveness,
            shutdown_started_at=None,
        ),
    )


async def test_compute_health_stale_loop_flips_readiness_when_enabled() -> None:
    from taskq.worker.health import compute_health

    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=1.0, stale_floor=10.0, clock=clock)
    liveness.tick("cron", period=1.0)
    t[0] += 100.0  # cron is far past its staleness budget

    report = await compute_health(_health_deps(liveness, watchdog_enabled=True))

    assert report.ready is False
    assert any("stale_loops=cron" in r for r in report.reasons), (
        f"enabled watchdog must surface the stale loop in readiness: {report.reasons}"
    )


async def test_compute_health_stale_loop_flips_readiness_when_watchdog_disabled() -> None:
    """watchdog_enabled gates the force-exit detectors, NOT the readiness
    signal: a worker whose loops have died must report NotReady even with
    the watchdog off, or the zombie keeps taking traffic with no signal."""
    from taskq.worker.health import compute_health

    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=1.0, stale_floor=10.0, clock=clock)
    liveness.tick("cron", period=1.0)
    t[0] += 100.0

    report = await compute_health(_health_deps(liveness, watchdog_enabled=False))

    assert report.ready is False, (
        "a stale loop must flip readiness regardless of watchdog_enabled — "
        f"gating the signal hides the zombie and keeps traffic flowing: {report.reasons}"
    )
    assert any("stale_loops=cron" in r for r in report.reasons)


# ── ShutdownWatchdog must count from the first shutdown signal ───────────
#
# shutdown_event is set at the END of orchestrate_shutdown. Anchoring the
# deadline on the event alone would give the drain a free pass: the cancel
# and cleanup graces would not count against termination_grace_period.
# With started_at wired, the countdown starts at the first shutdown signal.


async def test_shutdown_watchdog_anchors_deadline_on_first_signal(
    exit_codes: list[int],
) -> None:
    t, clock = _clock()
    shutdown = asyncio.Event()

    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=10.0,
        dump_interval=0.01,
        started_at=lambda: 0.0,  # first shutdown signal arrived at t=0
        clock=clock,
    )
    watchdog.start()

    t[0] += 8.0  # 8s of drain before shutdown_event is set
    shutdown.set()
    t[0] += 3.0  # 11s since the first signal — over the 10s deadline
    await asyncio.sleep(0.05)

    assert exit_codes == [2], (
        "Watchdog anchored on the first signal must trip at 11s > 10s "
        f"deadline even though only 3s passed since shutdown_event. Got {exit_codes}"
    )
    # Retrieve the tripped task so its intercepted os._exit does not leak
    # as an unretrieved task exception.
    with contextlib.suppress(_ExitSentinelError):
        await watchdog.cancel()


async def test_shutdown_watchdog_without_anchor_counts_from_event(
    exit_codes: list[int],
) -> None:
    t, clock = _clock()
    shutdown = asyncio.Event()

    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=10.0,
        dump_interval=0.01,
        clock=clock,
    )
    watchdog.start()

    t[0] += 8.0
    shutdown.set()
    t[0] += 3.0  # 3s since the event — under the deadline
    await asyncio.sleep(0.05)

    assert exit_codes == [], f"Unanchored watchdog must not trip at 3s < 10s. Got {exit_codes}"
    await watchdog.cancel()


async def test_shutdown_watchdog_cancel_leaves_no_pending_waiters() -> None:
    """Cancelling the watchdog while it is parked in the two-event wait must
    not leak the inner event.wait() tasks: _run is cancelled at the
    asyncio.wait() point, so without a finally-cleanup both child tasks
    stay pending until loop teardown."""
    shutdown = asyncio.Event()
    shutdown_started = asyncio.Event()
    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=60.0,
        dump_interval=0.05,
        started_at=lambda: None,
        shutdown_started_event=shutdown_started,
    )
    watchdog.start()
    await asyncio.sleep(0.05)  # let _run park in the two-task wait

    await watchdog.cancel()

    leftovers = [t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()]
    assert leftovers == [], f"cancel() must clean up the inner event-wait tasks: {leftovers}"


# ── Straggler dumps must wait for the back half of the deadline ──────────
#
# Arming the ShutdownWatchdog on the first shutdown signal closes the hung-
# orchestration blind spot, but it also means the watchdog is awake for the
# whole drain. Dumping every dump_interval from t=0 spams ~34KB of stderr
# and 8 structured records on every ordinary job-bearing shutdown (measured:
# 8 dumps over a 40s drain against a 60s deadline). A drain still in the
# front half of its hard budget is within expectations; one in its back
# half is already abnormal. Gate the dumps there — the trip path still gets
# its dumps before dying.


async def test_shutdown_watchdog_straggler_dumps_wait_for_back_half(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import _watchdog as mod

    dumps: list[str] = []
    monkeypatch.setattr(
        mod,
        "dump_task_stacks",
        lambda reason, *, detector=None, tasks=None: dumps.append(reason),
    )

    t, clock = _clock()
    shutdown = asyncio.Event()
    started = asyncio.Event()
    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=0.6,
        dump_interval=0.05,
        started_at=lambda: 0.0,
        clock=clock,
        shutdown_started_event=started,
    )
    watchdog.start()
    try:
        started.set()  # t=0: phase-1 signal; shutdown_event never arrives
        await asyncio.sleep(0.15)  # ~3 dump intervals, still elapsed 0
        assert dumps == [], f"no straggler dumps before half the deadline is consumed: {dumps}"
        t[0] = 0.45  # past the 0.5 gate (0.3s), short of the 0.6s deadline
        await asyncio.sleep(0.15)
        assert dumps, "dumps must fire once shutdown is in the back half of its budget"
    finally:
        await watchdog.cancel()


def test_dump_after_fraction_validation() -> None:
    """The gate fraction must lie in (0, 1): 0 would dump from the first
    interval (the spam the gate exists to stop), 1.0 equals the deadline —
    the trip always fires first, silently disabling straggler dumps."""
    for bad in (0.0, -0.5, 1.0, 1.5):
        with pytest.raises(ValueError, match="dump_after_fraction"):
            ShutdownWatchdog(
                asyncio.Event(),
                deadline=60.0,
                dump_interval=5.0,
                dump_after_fraction=bad,
            )
    ShutdownWatchdog(asyncio.Event(), deadline=60.0, dump_interval=5.0, dump_after_fraction=0.5)


async def test_shutdown_watchdog_dumps_right_up_to_the_trip(
    exit_codes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate must not eat the diagnostics that matter: once it opens,
    dumps keep firing every interval until the deadline trip — a hung
    shutdown dies with a fresh picture of what was still alive."""
    from taskq.worker import _watchdog as mod

    dumps: list[str] = []
    monkeypatch.setattr(
        mod,
        "dump_task_stacks",
        lambda reason, *, detector=None, tasks=None: dumps.append(reason),
    )

    t, clock = _clock()
    shutdown = asyncio.Event()
    started = asyncio.Event()
    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=0.6,
        dump_interval=0.05,
        started_at=lambda: 0.0,
        clock=clock,
        shutdown_started_event=started,
    )
    watchdog.start()
    started.set()
    t[0] = 0.55  # gate open (0.3), 0.05s short of the deadline
    try:
        for _ in range(20):  # up to 1s; a few intervals with the gate open
            if len(dumps) >= 2:
                break
            await asyncio.sleep(0.05)
        assert len(dumps) >= 2, f"dumps must fire once the gate is open: {dumps}"
        t[0] = 0.6  # deadline reached: the trip must land with fresh dumps behind it
        for _ in range(20):
            if exit_codes:
                break
            await asyncio.sleep(0.05)
        assert exit_codes == [2], f"deadline trip must still fire: {exit_codes}"
    finally:
        with contextlib.suppress(_ExitSentinelError):
            await watchdog.cancel()


async def test_shutdown_watchdog_logs_once_when_countdown_starts() -> None:
    """One record when the countdown starts — the front half of the budget
    is deliberately free of per-interval dumps, not blind: ops can see the
    watchdog is armed, the deadline, and when straggler dumps begin."""
    import structlog.testing

    shutdown = asyncio.Event()
    started = asyncio.Event()
    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=0.6,
        dump_interval=0.05,
        started_at=lambda: None,
        shutdown_started_event=started,
    )
    watchdog.start()
    try:
        with structlog.testing.capture_logs() as captured:
            await asyncio.sleep(0.1)  # parked: nothing armed yet
            assert not any(e.get("event") == "shutdown-watchdog-armed" for e in captured), (
                f"no countdown, no record: {captured}"
            )
            started.set()
            await asyncio.sleep(0.15)
            armed = [e for e in captured if e.get("event") == "shutdown-watchdog-armed"]
            assert len(armed) == 1, f"exactly one countdown-start record: {captured}"
    finally:
        await watchdog.cancel()


#
# These loops tick before the is_leader check, so they keep ticking even
# when demoted. This is correct — they don't need forget() on demotion
# because they never stop ticking (unlike leader.watchdog, which forgets
# on demotion because it parks until re-election).


async def test_demoted_leader_loops_still_tick_liveness() -> None:
    """A demoted (non-leader) worker's cron and scheduled_wake loops must
    keep registering liveness ticks — and must not touch the backend —
    so detector 2 never false-trips on an ordinary leadership change."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker.leader import MaintenanceLeader

    liveness = LoopLiveness()

    class _Backend:
        async def scheduled_to_pending(self, *, now: object) -> int:
            raise AssertionError("demoted leader must not run scheduled_to_pending")

    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=asyncio.Event(),  # never set: this worker is not the leader
            settings=SimpleNamespace(schema_name="taskq"),
            dispatcher_pool=None,
        ),
    )
    leader = MaintenanceLeader(
        deps,
        uuid4(),
        cast(Backend, _Backend()),
        clock=SystemClock(),
    )

    shutdown = asyncio.Event()
    cron_task = asyncio.create_task(leader._cron_loop(shutdown))
    wake_task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            ages = liveness.ages()
            if "leader.cron" in ages and "leader.scheduled_wake" in ages:
                break
            await asyncio.sleep(0.01)
        ages = liveness.ages()
        assert "leader.cron" in ages, f"demoted cron loop must keep ticking: {ages}"
        assert "leader.scheduled_wake" in ages, (
            f"demoted scheduled_wake loop must keep ticking: {ages}"
        )
    finally:
        shutdown.set()
        for task in (cron_task, wake_task):
            await asyncio.wait_for(task, timeout=5.0)


# ── Loop DB awaits must be bounded (detector 2 false-trip guard) ─────────
#
# Leader loops tick once per iteration, then await PG. Without a per-query
# timeout, a stalled PG hangs the await indefinitely — the loop stops
# ticking, the staleness budget expires, and detector 2 kills a healthy
# worker. The dispatcher pool and the leader's dedicated conns are bounded
# by dispatcher_command_timeout.


def test_dispatcher_command_timeout_setting_loads() -> None:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_DISPATCHER_COMMAND_TIMEOUT": "7.5",
        }
    )
    assert settings.dispatcher_command_timeout == 7.5


async def test_open_dedicated_conn_passes_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.worker import deps as deps_mod

    captured: dict[str, object] = {}

    class _Conn:
        def set_ssl_context(self, *args: object) -> None:
            pass

    async def _fake_connect(dsn: str, **kwargs: object) -> _Conn:
        captured.update(kwargs)
        return _Conn()

    monkeypatch.setattr(deps_mod.asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(deps_mod, "apply_keepalive_to_conn", lambda *a, **k: False)

    await deps_mod.open_dedicated_conn(
        "postgresql://x:x@localhost/x", label="cron", command_timeout=7.5
    )

    assert captured.get("command_timeout") == 7.5, (
        f"open_dedicated_conn must forward command_timeout to asyncpg.connect: {captured}"
    )


async def test_leader_dedicated_conn_uses_dispatcher_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker import leader as leader_mod
    from taskq.worker.leader import MaintenanceLeader

    captured: dict[str, object] = {}

    async def _fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> object:
        captured["command_timeout"] = command_timeout
        return object()

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", _fake_open)

    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            leader_conn_factory=None,
            settings=SimpleNamespace(
                pg_dsn_direct="postgresql://x:x@localhost/x",
                dispatcher_command_timeout=3.25,
            ),
        ),
    )
    leader = MaintenanceLeader(
        deps,
        uuid4(),
        cast(Backend, SimpleNamespace()),
        clock=SystemClock(),
    )

    await leader._open_dedicated_conn("cron")

    assert captured["command_timeout"] == 3.25, (
        f"leader dedicated conns must be bounded by dispatcher_command_timeout: {captured}"
    )


async def test_open_leader_conn_dsn_fallback_applies_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The election conn (leader_conn) DSN fallback must be bounded too —
    it never passed the timeout at all, so a stalled advisory-lock probe
    could hang the election loop's tick past the detector-2 budget."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker import leader as leader_mod
    from taskq.worker.leader import MaintenanceLeader

    captured: dict[str, object] = {}

    async def _fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> object:
        captured["command_timeout"] = command_timeout
        return object()

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", _fake_open)

    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            leader_conn_factory=None,
            settings=SimpleNamespace(
                pg_dsn_direct="postgresql://x:x@localhost/x",
                dispatcher_command_timeout=3.25,
            ),
        ),
    )
    leader = MaintenanceLeader(
        deps,
        uuid4(),
        cast(Backend, SimpleNamespace()),
        clock=SystemClock(),
    )

    await leader._open_leader_conn()

    assert captured["command_timeout"] == 3.25, (
        f"the election conn must be bounded by dispatcher_command_timeout: {captured}"
    )


async def test_open_worker_deps_leader_factory_applies_dispatcher_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default-path wiring, not the fallback: on a stock deployment
    deps.leader_conn_factory is always set to the TaskQ-built
    _leader_dsn_factory, and _open_dedicated_conn/_open_leader_conn return
    through it before their own timeout lines run. The factory itself must
    carry dispatcher_command_timeout, or cron, monitor, and election conns
    stay unbounded while looking fixed."""
    from typing import cast

    import asyncpg

    from taskq.connections import WorkerConnections
    from taskq.settings import WorkerSettings
    from taskq.worker import deps as deps_mod
    from taskq.worker.deps import open_worker_deps

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_DISPATCHER_COMMAND_TIMEOUT": "3.25",
        }
    )

    captured: list[dict[str, object]] = []

    class _FakeConn:
        def __init__(self) -> None:
            self._closed = False

        async def execute(self, *args: object, **kwargs: object) -> str:
            return "OK"

        def is_closed(self) -> bool:
            return self._closed

        async def close(self) -> None:
            self._closed = True

        def terminate(self) -> None:
            self._closed = True

    async def _fake_open(
        dsn: str,
        *,
        label: str,
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> _FakeConn:
        captured.append({"label": label, "command_timeout": command_timeout})
        return _FakeConn()

    monkeypatch.setattr(deps_mod, "open_dedicated_conn", _fake_open)

    class _FakePool:
        """Caller-owned pool: open_worker_deps never closes it."""

    connections = WorkerConnections(
        dispatcher_pool=cast(asyncpg.Pool, _FakePool()),
        heartbeat_pool=cast(asyncpg.Pool, _FakePool()),
        worker_pool=cast(asyncpg.Pool, _FakePool()),
        notify_conn=cast(asyncpg.Connection, _FakeConn()),
        # leader role unset: the one role that falls back to the TaskQ-built
        # DSN factory — the stock-deployment wiring under test.
    )

    async with open_worker_deps(settings, connections=connections) as deps:
        assert deps.leader_conn_factory is not None, (
            "stock deployments must have a leader_conn_factory (this is why "
            "leader.py's own timeout lines never run by default)"
        )
        leader_calls = [c for c in captured if c["label"] == "leader"]
        assert leader_calls, "the leader conn must be built via open_dedicated_conn"
        assert all(c["command_timeout"] == 3.25 for c in leader_calls), (
            f"the TaskQ-built leader factory must carry dispatcher_command_timeout: {captured}"
        )

        # Rebuilds (election reopen, cron/monitor conns via the leader) go
        # through the same factory — they must stay bounded too.
        captured.clear()
        await deps.leader_conn_factory()
        assert captured == [{"label": "leader", "command_timeout": 3.25}], (
            f"factory rebuilds must stay bounded: {captured}"
        )


# ── ShutdownWatchdog must wake if orchestrate_shutdown hangs before shutdown_event ─
#
# _run() parks on shutdown_event.wait(). shutdown_event is only set at the
# very end of orchestrate_shutdown (finally block, line 265) or in the
# leader_conn close path (line 254). If orchestrate_shutdown hangs during
# drain/cancel/force phases — e.g. on an unbounded DB await — shutdown_event
# is never set, the watchdog never wakes, and the termination_grace_period
# deadline is never enforced. The watchdog exists to catch exactly this
# scenario but is blind to it.


async def test_shutdown_watchdog_wakes_when_shutdown_phase_changes(
    exit_codes: list[int],
) -> None:
    """The ShutdownWatchdog must wake when shutdown starts (producer_stop_event
    set in Phase 1), not only when shutdown_event is set at the very end. If
    orchestrate_shutdown hangs on an unbounded DB await before setting
    shutdown_event, the watchdog must still enforce the deadline."""
    shutdown = asyncio.Event()
    shutdown_started = asyncio.Event()
    t0 = time.monotonic()
    watchdog = ShutdownWatchdog(
        shutdown,
        deadline=0.1,
        dump_interval=0.01,
        started_at=lambda: t0,
        shutdown_started_event=shutdown_started,
    )
    watchdog.start()

    # Simulate Phase 1: producer_stop_event is set, but shutdown_event
    # is NEVER set (orchestrate_shutdown hangs)
    shutdown_started.set()
    await asyncio.sleep(0.3)

    assert exit_codes == [2], (
        "ShutdownWatchdog must trip when the deadline is exceeded after "
        "shutdown_started_event fires, even if shutdown_event is never set. "
        f"Got exit_codes={exit_codes}"
    )
    # Retrieve the tripped task so its intercepted os._exit does not leak
    # as an unretrieved task exception.
    with contextlib.suppress(_ExitSentinelError):
        await watchdog.cancel()


# ── The command timeout + trailing 1s sleep must fit the stale budget ───
#
# leader.scheduled_wake ticks once per iteration, awaits PG (bounded by
# dispatcher_command_timeout), then sleeps 1.0s — worst-case tick gap =
# timeout + 1.0s against a budget of max(period * grace_factor, floor).
# Measured on the real loops with the real loop_watchdog_loop: timeout 10.0
# vs budget 10.0 → gap 11.002s → detector 2 force-exited a healthy leader
# at age 10.008s; timeout 5.0 → gap 6.002s → no trip. These two tests pin
# the mechanism with scaled values; the settings invariant (test_settings)
# pins the default's arithmetic, and the loop shape below is what makes the
# arithmetic matter: change the trailing sleep or the tick placement and
# one of these fails.


class _CommandTimeoutBackend:
    """Backend stand-in whose scheduled_to_pending stalls for stall_secs and
    then raises TimeoutError — exactly what asyncpg does when the pool's
    command_timeout fires mid-query."""

    def __init__(self, stall_secs: float) -> None:
        self._stall_secs = stall_secs

    async def scheduled_to_pending(self, *, now: object) -> int:
        await asyncio.sleep(self._stall_secs)
        raise TimeoutError("simulated asyncpg command_timeout")


async def _wake_loop_under_watchdog(
    *,
    stall_secs: float,
    grace_factor: float,
    stale_floor: float,
) -> tuple[LoopLiveness, asyncio.Event, asyncio.Task[None], asyncio.Task[None]]:
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker import _watchdog as mod
    from taskq.worker.leader import MaintenanceLeader

    liveness = LoopLiveness(grace_factor=grace_factor, stale_floor=stale_floor)
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(schema_name="taskq", dispatcher_command_timeout=stall_secs),
            dispatcher_pool=None,
        ),
    )
    leader = MaintenanceLeader(
        deps,
        uuid4(),
        cast(Backend, _CommandTimeoutBackend(stall_secs)),
        clock=SystemClock(),
    )
    shutdown = asyncio.Event()
    wake_task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    watchdog_task = asyncio.create_task(
        mod.loop_watchdog_loop(liveness, shutdown, check_interval=0.05)
    )
    return liveness, shutdown, wake_task, watchdog_task


async def _stop_wake_harness(*tasks: asyncio.Task[None]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    for task in tasks:
        with contextlib.suppress(asyncio.CancelledError, _ExitSentinelError):
            await task


async def test_timeout_gap_over_budget_trips_stale_detector(
    exit_codes: list[int],
) -> None:
    """stall 2.5s + trailing sleep 1.0s = 3.5s gap vs budget max(1.0*3, 1)=3.0s
    → detector 2 trips. The reviewer's measurement, scaled: timeout 10 vs
    floor 10 → 11s gap → force-exit of a healthy, still-leader worker."""
    _liveness, shutdown, wake_task, wd_task = await _wake_loop_under_watchdog(
        stall_secs=2.5, grace_factor=3.0, stale_floor=1.0
    )
    try:
        for _ in range(120):  # up to 6s; the trip lands at ~3s
            if exit_codes:
                break
            await asyncio.sleep(0.05)
        assert exit_codes == [2], (
            "a worst-case tick gap (timeout + 1.0s) above the staleness budget "
            f"must trip detector 2; got exit_codes={exit_codes}"
        )
    finally:
        shutdown.set()
        await _stop_wake_harness(wake_task, wd_task)


async def test_timeout_gap_within_budget_does_not_trip(
    exit_codes: list[int],
) -> None:
    """stall 0.5s + trailing sleep 1.0s = 1.5s gap vs budget 3.0s → no trip,
    and the loop keeps ticking. The control case: timeout below the floor
    with headroom for the trailing sleep behaves."""
    liveness, shutdown, wake_task, wd_task = await _wake_loop_under_watchdog(
        stall_secs=0.5, grace_factor=3.0, stale_floor=1.0
    )
    try:
        await asyncio.sleep(4.0)  # covers two full worst-case gaps
        assert exit_codes == [], (
            f"no trip while timeout + 1.0s period < staleness budget: {exit_codes}"
        )
        ages = liveness.ages()
        assert ages.get("leader.scheduled_wake", float("inf")) < 3.0, (
            f"the loop must still be ticking inside its budget: {ages}"
        )
    finally:
        shutdown.set()
        await _stop_wake_harness(wake_task, wd_task)


# ── A fired command_timeout must not crash the leader loops ─────────────
#
# asyncpg surfaces a fired command_timeout in two shapes: TimeoutError
# (client-side deadline) AND asyncpg.QueryCanceledError (server-side 57014
# after the driver's cancel request lands). QueryCanceledError is a
# PostgresError — NOT a PostgresConnectionError and NOT an OSError — so the
# loops' conn-loss tuples miss it. Before the leader conns were bounded
# this shape could not occur; now it can, and an uncaught one escapes into
# the leader TaskGroup and tears the worker down mid-PG-degradation — the
# exact failure bounding the conns was supposed to absorb. heartbeat.py
# already catches both shapes for the same reason.


class _QueryCanceledConn:
    """Conn stand-in whose calls raise QueryCanceledError (server 57014)."""

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, *args: object, **kwargs: object) -> str:
        self.calls += 1
        raise asyncpg.QueryCanceledError("canceling statement due to user request")

    async def fetchval(self, *args: object, **kwargs: object) -> object:
        self.calls += 1
        raise asyncpg.QueryCanceledError("canceling statement due to user request")

    def is_closed(self) -> bool:
        return False

    async def close(self) -> None:
        pass


def _leader_deps(**settings_overrides: object) -> WorkerDeps:
    from types import SimpleNamespace
    from typing import cast

    settings = SimpleNamespace(
        schema_name="taskq",
        heartbeat_interval=0.05,
        dispatcher_command_timeout=2.5,
        **settings_overrides,
    )
    return cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=LoopLiveness(),
            is_leader=asyncio.Event(),
            leader_conn=None,
            leader_conn_factory=None,
            owns_leader_conn=True,
            settings=settings,
            dispatcher_pool=None,
        ),
    )


async def test_election_loop_survives_query_canceled_probe() -> None:
    """A QueryCanceledError from the bounded election conn must be treated
    as transient PG loss (drop, warn, retry) — not escape into the leader
    TaskGroup and tear the worker down."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker.leader import MaintenanceLeader

    deps = _leader_deps()
    deps.is_leader.set()
    deps.leader_conn = cast(asyncpg.Connection, _QueryCanceledConn())
    leader = MaintenanceLeader(deps, uuid4(), cast(Backend, SimpleNamespace()), clock=SystemClock())

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        # Give the loop a few probe cycles to hit the QueryCanceledError.
        await asyncio.sleep(0.3)
        assert not task.done() or task.exception() is None, (
            f"election loop must ride out a QueryCanceledError, not die: "
            f"{task.exception() if task.done() else 'still running'}"
        )
        assert deps.leader_conn is None, (
            "the wedged conn must be dropped so a later attempt rebuilds it"
        )
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=5.0)


async def test_scheduled_wake_survives_query_canceled(
    exit_codes: list[int],
) -> None:
    """Same shape one loop over: scheduled_to_pending raising
    QueryCanceledError must log and retry on schedule, not crash the loop
    (and not trip detector 2 — the loop keeps ticking)."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    import asyncpg

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker import _watchdog as mod
    from taskq.worker.leader import MaintenanceLeader

    class _Backend:
        async def scheduled_to_pending(self, *, now: object) -> int:
            raise asyncpg.QueryCanceledError("canceling statement due to user request")

    liveness = LoopLiveness(grace_factor=3.0, stale_floor=1.0)
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(schema_name="taskq", dispatcher_command_timeout=2.5),
            dispatcher_pool=None,
        ),
    )
    leader = MaintenanceLeader(deps, uuid4(), cast(Backend, _Backend()), clock=SystemClock())
    shutdown = asyncio.Event()
    wake_task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    watchdog_task = asyncio.create_task(
        mod.loop_watchdog_loop(liveness, shutdown, check_interval=0.05)
    )
    try:
        await asyncio.sleep(1.5)  # several ticks' worth of QueryCanceledError
        assert exit_codes == [], f"no detector-2 trip while the loop retries: {exit_codes}"
        assert not wake_task.done(), "the loop must still be retrying"
        ages = liveness.ages()
        assert ages.get("leader.scheduled_wake", float("inf")) < 3.0, (
            f"the loop must keep ticking through QueryCanceledError: {ages}"
        )
    finally:
        shutdown.set()
        await _stop_wake_harness(wake_task, watchdog_task)


# ── The iteration must be bounded, not just each statement ──────────────
#
# leader.scheduled_wake awaits PG twice per iteration when work is due
# (scheduled_to_pending, then pool.acquire + pg_notify) and leader.cron
# runs a whole transaction. Per-statement timeouts alone admit a gap of
# k * timeout + 1.0s — over the budget whenever k > 1. The iteration body
# gets a single deadline (dispatcher_command_timeout), making the
# timeout + 1.0s model the invariant checks actually true.


class _StallAcquirePool:
    """Pool stand-in whose acquire() parks for stall_secs before yielding
    a conn — a degraded/exhausted dispatcher pool."""

    def __init__(self, stall_secs: float) -> None:
        self._stall_secs = stall_secs

    def acquire(self, **kwargs: object) -> object:
        stall = self._stall_secs

        class _CM:
            async def __aenter__(self) -> object:
                await asyncio.sleep(stall)
                return object()

            async def __aexit__(self, *args: object) -> bool:
                return False

        return _CM()


async def test_scheduled_wake_notify_path_gap_stays_within_budget(
    exit_codes: list[int],
) -> None:
    """The count > 0 path awaits twice: scheduled_to_pending (2.4s) then
    pool.acquire (2.4s). Without an iteration-level deadline the gap is
    2.4 + 2.4 + 1.0 = 5.8s — over the 4.0s budget, a false trip. With the
    body bounded at 2.5s the gap is 3.5s and the leader rides it out."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker import _watchdog as mod
    from taskq.worker.leader import MaintenanceLeader

    class _SlowButSuccessfulBackend:
        async def scheduled_to_pending(self, *, now: object) -> int:
            await asyncio.sleep(2.4)
            return 1  # due jobs: the loop proceeds to the notify leg

    liveness = LoopLiveness(grace_factor=4.0, stale_floor=1.0)  # budget 4.0s
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(schema_name="taskq", dispatcher_command_timeout=2.5),
            dispatcher_pool=_StallAcquirePool(2.4),
        ),
    )
    leader = MaintenanceLeader(
        deps, uuid4(), cast(Backend, _SlowButSuccessfulBackend()), clock=SystemClock()
    )
    shutdown = asyncio.Event()
    wake_task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    watchdog_task = asyncio.create_task(
        mod.loop_watchdog_loop(liveness, shutdown, check_interval=0.05)
    )
    try:
        await asyncio.sleep(4.5)  # past the first full iteration either way
        assert exit_codes == [], (
            "two slow statements in one iteration must not trip detector 2 — "
            f"the iteration needs a single deadline, not per-statement ones: {exit_codes}"
        )
        ages = liveness.ages()
        assert ages.get("leader.scheduled_wake", float("inf")) < 4.0, (
            f"the loop must keep ticking inside its budget: {ages}"
        )
    finally:
        shutdown.set()
        await _stop_wake_harness(wake_task, watchdog_task)


async def test_cron_multi_statement_tick_gap_stays_within_budget(
    exit_codes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cron tick is BEGIN + N statements + COMMIT — each under the
    per-statement timeout, summing past the staleness budget. The 5.5s
    stall stands in for a catch-up burst on a degraded PG: bounded at the
    iteration level (2.5s) the gap is 3.5s, unbounded it is 6.5s — a
    false trip of a healthy leader."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker import _watchdog as mod
    from taskq.worker import leader as leader_mod
    from taskq.worker.leader import MaintenanceLeader

    async def _stall_tick_cron(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(5.5)  # many statements, each just under the timeout

    monkeypatch.setattr(leader_mod, "tick_cron", _stall_tick_cron)

    class _CronConn:
        def transaction(self) -> object:
            class _Tx:
                async def __aenter__(self) -> None:
                    return None

                async def __aexit__(self, *args: object) -> bool:
                    return False

            return _Tx()

        def is_closed(self) -> bool:
            return False

        async def close(self) -> None:
            pass

    liveness = LoopLiveness(grace_factor=4.0, stale_floor=1.0)  # budget 4.0s
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(schema_name="taskq", dispatcher_command_timeout=2.5),
        ),
    )
    leader = MaintenanceLeader(deps, uuid4(), cast(Backend, SimpleNamespace()), clock=SystemClock())
    leader._cron_conn = _CronConn()  # type: ignore[assignment]

    shutdown = asyncio.Event()
    cron_task = asyncio.create_task(leader._cron_loop(shutdown))
    watchdog_task = asyncio.create_task(
        mod.loop_watchdog_loop(liveness, shutdown, check_interval=0.05)
    )
    try:
        await asyncio.sleep(4.5)
        assert exit_codes == [], (
            "a multi-statement cron tick must be bounded at the iteration "
            f"level, not per statement: {exit_codes}"
        )
    finally:
        shutdown.set()
        await _stop_wake_harness(cron_task, watchdog_task)


# ── The unexpected-error backstop ────────────────────────────────────────
#
# The transient set can never be complete — a driver upgrade, a new PG
# shape, an outright bug in the loop. Before, such errors either escaped
# into the TaskGroup (a crash with no distinct record) or, in cron's
# blanket catch, retried forever (a zombie that ticks but does no work —
# detector 2 cannot see it because the tick lands at the loop top). The
# guard makes the choice explicit: a few tolerated, LOUD occurrences, then
# deliberately fatal.


def test_unexpected_loop_error_guard_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import structlog.testing

    from taskq.worker import _transient as transient_mod
    from taskq.worker._transient import UnexpectedLoopErrorGuard

    counted: list[dict[str, object]] = []

    class _Counter:
        def add(self, amount: int, attributes: dict[str, object]) -> None:
            counted.append(attributes)

    monkeypatch.setattr(transient_mod, "_unexpected_loop_errors", _Counter())

    guard = UnexpectedLoopErrorGuard("leader.test", max_consecutive=3)
    with structlog.testing.capture_logs() as captured:
        guard.unexpected(ValueError("surprise one"))
        guard.unexpected(RuntimeError("surprise two"))
        assert [e.get("event") for e in captured] == [
            "leader-loop-unexpected-error",
            "leader-loop-unexpected-error",
        ]
        assert captured[0]["consecutive"] == 1
        assert captured[1]["consecutive"] == 2
        assert counted == [{"loop": "leader.test"}, {"loop": "leader.test"}]

        guard.ok()  # a clean iteration resets the streak
        guard.unexpected(ValueError("fresh streak"))
        assert captured[-1]["consecutive"] == 1

        guard.unexpected(ValueError("second"))
        with pytest.raises(ValueError, match="third time"):
            guard.unexpected(ValueError("third time"))


async def test_scheduled_wake_backstop_tolerates_then_goes_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Behavioral: a backend raising an unexpected error (a bug shape, not
    PG) is ridden out loudly for a few ticks, then the loop dies
    deliberately — never an infinite silent retry, never a quiet crash."""
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    import structlog.testing

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker import _transient as transient_mod
    from taskq.worker.leader import MaintenanceLeader

    monkeypatch.setattr(transient_mod, "DEFAULT_MAX_CONSECUTIVE_UNEXPECTED", 3)

    calls = 0

    class _BuggyBackend:
        async def scheduled_to_pending(self, *, now: object) -> int:
            nonlocal calls
            calls += 1
            raise ValueError("this is a bug, not a PG moment")

    liveness = LoopLiveness()
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(schema_name="taskq", dispatcher_command_timeout=2.5),
            dispatcher_pool=None,
        ),
    )
    leader = MaintenanceLeader(deps, uuid4(), cast(Backend, _BuggyBackend()), clock=SystemClock())
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    try:
        with (
            structlog.testing.capture_logs() as captured,
            pytest.raises(ValueError, match="this is a bug"),
        ):
            await asyncio.wait_for(task, timeout=15.0)
        assert isinstance(task.exception(), ValueError), (
            f"the third consecutive unexpected error must be deliberately "
            f"fatal, not retried: {task.exception()!r}"
        )
        assert calls == 3, f"exactly max_consecutive attempts before death: {calls}"
        loud = [e for e in captured if e.get("event") == "leader-loop-unexpected-error"]
        assert len(loud) == 3, f"every tolerated surprise is on the record: {captured}"
    finally:
        shutdown.set()
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# ── The transient set itself ─────────────────────────────────────────────
#
# asyncpg raises degraded-PG conditions in more shapes than the OSError
# family: server-side cancels, admin shutdown, crash recovery, connection
# saturation, transaction-rollback states, session timeouts. Every one of
# them is "PG is having a moment; retry next tick" — and every one of them
# used to be a coin-flip away from crashing a loop that enumerated only
# the OSError flavours. The taxonomy pin locks the classification facts
# the set relies on (so an asyncpg re-parenting breaks it loudly), and the
# parametrized behavioral test drives each shape through a real loop.


def test_transient_pg_error_taxonomy_facts() -> None:
    """The facts that justify the set's members: if an asyncpg upgrade
    re-parents any of these out of their family, the classification must
    be revisited — this test is the alarm."""
    import asyncpg

    from taskq.worker._transient import TRANSIENT_PG_ERRORS

    # None of the OperatorIntervention/InsufficientResources/TransactionRollback
    # shapes are covered by the OSError-only tuples loops used to enumerate —
    # that is exactly why they must be named explicitly.
    for cls in (
        asyncpg.QueryCanceledError,
        asyncpg.AdminShutdownError,
        asyncpg.CannotConnectNowError,
        asyncpg.TooManyConnectionsError,
        asyncpg.DeadlockDetectedError,
        asyncpg.SerializationError,
        asyncpg.IdleSessionTimeoutError,
        asyncpg.IdleInTransactionSessionTimeoutError,
    ):
        assert not issubclass(cls, asyncpg.PostgresConnectionError), (
            f"{cls.__name__} left the PostgresError-only family — re-check the set"
        )
        assert not issubclass(cls, OSError), f"{cls.__name__} is now an OSError — re-check the set"
        instance = cls("simulated")
        assert isinstance(instance, TRANSIENT_PG_ERRORS)

    # The conn-family members the set relies on for coverage.
    assert issubclass(asyncpg.ConnectionDoesNotExistError, TRANSIENT_PG_ERRORS)
    assert issubclass(asyncpg.ConnectionFailureError, TRANSIENT_PG_ERRORS)

    # Auth failures stay OUT: they must not retry silently (static DSN) —
    # the credential-provider reopen path has its own deliberate catch.
    assert not issubclass(asyncpg.InvalidPasswordError, TRANSIENT_PG_ERRORS)


_TRANSIENT_SHAPES: list[tuple[str, object]] = [
    ("TimeoutError", lambda: TimeoutError("local deadline")),
    ("OSError", lambda: OSError("socket died")),
    ("ConnectionDoesNotExistError", lambda: asyncpg.ConnectionDoesNotExistError("gone")),
    ("QueryCanceledError", lambda: asyncpg.QueryCanceledError("57014")),
    ("AdminShutdownError", lambda: asyncpg.AdminShutdownError("57P01")),
    ("CannotConnectNowError", lambda: asyncpg.CannotConnectNowError("57P03")),
    ("TooManyConnectionsError", lambda: asyncpg.TooManyConnectionsError("53300")),
    ("DeadlockDetectedError", lambda: asyncpg.DeadlockDetectedError("40P01")),
    ("SerializationError", lambda: asyncpg.SerializationError("40001")),
    ("IdleSessionTimeoutError", lambda: asyncpg.IdleSessionTimeoutError("57P05")),
    (
        "IdleInTransactionSessionTimeoutError",
        lambda: asyncpg.IdleInTransactionSessionTimeoutError("25P03"),
    ),
    ("InterfaceError", lambda: asyncpg.InterfaceError("bad state")),
]


@pytest.mark.parametrize(
    ("shape_name", "make_exc"),
    _TRANSIENT_SHAPES,
    ids=[name for name, _ in _TRANSIENT_SHAPES],
)
async def test_scheduled_wake_rides_out_every_transient_shape(
    shape_name: str,
    make_exc: object,
) -> None:
    """Every classified-transient error shape gets a warning and a retry on
    schedule — never a crash into the leader TaskGroup."""
    from collections.abc import Callable as _Callable
    from types import SimpleNamespace
    from typing import cast
    from uuid import uuid4

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker.leader import MaintenanceLeader

    calls = 0
    exc_factory = cast(_Callable[[], BaseException], make_exc)

    class _FlakyBackend:
        async def scheduled_to_pending(self, *, now: object) -> int:
            nonlocal calls
            calls += 1
            raise exc_factory()

    liveness = LoopLiveness()
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(schema_name="taskq", dispatcher_command_timeout=2.5),
            dispatcher_pool=None,
        ),
    )
    leader = MaintenanceLeader(deps, uuid4(), cast(Backend, _FlakyBackend()), clock=SystemClock())
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    try:
        for _ in range(50):  # up to 2.5s: first call, 1s sleep, second call
            if calls >= 2 or task.done():
                break
            await asyncio.sleep(0.05)
        assert not task.done(), (
            f"{shape_name} crashed the loop instead of being treated as transient: "
            f"{task.exception()!r}"
        )
        assert calls >= 2, f"{shape_name}: the loop must retry on schedule (calls={calls})"
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=5.0)
