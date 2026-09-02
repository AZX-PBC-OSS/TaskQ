"""Drain-monitor liveness safety tests (detector-2 interaction).

The drain monitor is an interval-driven sibling: it ticks
``LoopLiveness`` once per iteration and RETURNS as soon as it has
triggered ``orchestrate_shutdown`` — while the orchestration itself (and
the stale-tick watchdog sweeping alongside it) keeps running until the
orchestrator's finally sets ``shutdown_event``. These tests pin the two
invariants that interaction depends on:

- the monitor's liveness registration is forgotten when it returns, so a
  parked orchestration cannot look like a dead loop (F1);
- the registered tick period covers the worst-case iteration gap — the
  ``count_active_jobs`` bound plus the trailing poll sleep — so a
  slow-but-recovering count (a transient the monitor itself rides out)
  cannot go stale mid-iteration (F2).
"""

import asyncio
import contextlib
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.worker._watchdog import LoopLiveness, loop_watchdog_loop
from taskq.worker.deps import WorkerDeps
from taskq.worker.drain import drain_monitor_loop
from taskq.worker.shutdown import ShutdownPhase


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


class _ManualClock:
    """Monotonic-clock stand-in that only moves when the test advances it.

    ``LoopLiveness`` measures tick ages with it; the monitor's own timing
    (settle window, max_runtime, real poll sleeps) uses real
    ``time.monotonic()`` and is unaffected.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _ActiveJobs:
    def __init__(self, count: int) -> None:
        self._count = count

    def count(self) -> int:
        return self._count


class _CountBackend:
    """Backend stand-in whose count_active_jobs returns a fixed count."""

    def __init__(self, count: int) -> None:
        self._count = count

    async def count_active_jobs(self, queues: object) -> int:
        return self._count


def _drain_deps(liveness: LoopLiveness, *, active_jobs: int = 0) -> WorkerDeps:
    """Minimal WorkerDeps stand-in for the drain monitor.

    ``shutdown_phase`` must be the real ``ShutdownPhase.NONE`` member:
    the double-orchestration guard compares by identity.
    """
    return cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            active_jobs=_ActiveJobs(active_jobs),
            drain_failures=0,
            shutdown_phase=ShutdownPhase.NONE,
        ),
    )


def _settings() -> SimpleNamespace:
    return SimpleNamespace(queues=["default"])


async def _stop_quietly(*tasks: asyncio.Task[object]) -> None:
    """Cancel and retrieve every harness task, absorbing the sentinel
    a tripped watchdog raises instead of ``os._exit``."""
    for task in tasks:
        if not task.done():
            task.cancel()
    for task in tasks:
        with contextlib.suppress(asyncio.CancelledError, _ExitSentinelError):
            await task


async def test_drain_trigger_parking_orchestration_does_not_trip_stale_detector(
    exit_codes: list[int],
) -> None:
    """F1, full reproduction: the monitor triggers, returns; the
    orchestration parks past the staleness budget before its finally sets
    shutdown_event (its phases — cancelling, forcing — legitimately take
    that long). The watchdog keeps sweeping the whole time. Without
    forget("drain_monitor") on the monitor's exit path, the leaked
    registration goes stale and detector 2 force-exits the worker
    mid-grace: exit code 2 instead of the drain's, in-flight terminal
    writes lost to crash reclaim."""
    # poll=0.05 with grace 5 / floor 0.1 → the leaked entry's budget is
    # max(0.25, 0.1) = 0.25s; the orchestration parks 0.6s — well past it,
    # with sweep headroom for the trip to land reliably.
    liveness = LoopLiveness(grace_factor=5.0, stale_floor=0.1)
    deps = _drain_deps(liveness, active_jobs=0)
    backend = _CountBackend(0)
    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async def _parked_orchestrate(
        deps: object,
        settings: object,
        worker_id: object,
        shutdown_event: asyncio.Event,
        escalate_event: object,
        *,
        backend: object,
    ) -> int:
        try:
            await asyncio.sleep(0.6)  # phases run; shutdown_event still clear
        finally:
            shutdown_event.set()
        return 0

    with patch("taskq.worker.drain.orchestrate_shutdown", side_effect=_parked_orchestrate):
        monitor = asyncio.create_task(
            drain_monitor_loop(
                deps,
                _settings(),
                new_uuid(),
                shutdown_event,
                escalate_event,
                orchestrator_holder,
                cast(Backend, backend),
                idle_settle_window=0.0,
                idle_poll_interval=0.05,
                max_runtime=None,
            )
        )
        watchdog = asyncio.create_task(
            loop_watchdog_loop(liveness, shutdown_event, check_interval=0.02)
        )
        try:
            await asyncio.wait_for(monitor, timeout=5.0)
            assert len(orchestrator_holder) == 1
            exit_code = await asyncio.wait_for(orchestrator_holder[0], timeout=5.0)

            assert exit_codes == [], (
                "detector 2 force-exited the worker while the triggered "
                "orchestration was still in its grace budget — the monitor's "
                f"liveness registration leaked on its return path: {exit_codes}"
            )
            assert exit_code == 0
            assert shutdown_event.is_set()
        finally:
            shutdown_event.set()
            await _stop_quietly(watchdog, monitor)
