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
from collections.abc import Callable
from pathlib import Path

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


# ── watchdog_enabled must gate detector 3 and the readiness stale-check ──
#
# The setting description says it switches off all four detectors, but
# detector 3 (sibling spawner clean-return contract) and stale_loops in
# compute_health ran regardless of the flag. The exception path of the
# spawner is deliberately NOT gated: a crashing sibling must always signal
# shutdown, watchdog or not.


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


async def test_compute_health_stale_loop_ignored_when_watchdog_disabled() -> None:
    from taskq.worker.health import compute_health

    t, clock = _clock()
    liveness = LoopLiveness(grace_factor=1.0, stale_floor=10.0, clock=clock)
    liveness.tick("cron", period=1.0)
    t[0] += 100.0

    report = await compute_health(_health_deps(liveness, watchdog_enabled=False))

    assert report.ready is True, (
        f"disabled watchdog must not flip readiness on a stale loop: {report.reasons}"
    )
    assert not any("stale_loops" in r for r in report.reasons)


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


# ── cron and scheduled_wake must tick unconditionally ────────────────────
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
