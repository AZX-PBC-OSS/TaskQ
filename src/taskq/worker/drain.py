"""Drain monitor for until-idle mode.

When spawned as a sibling in the worker's TaskGroup, the drain monitor
polls the backend for active jobs in the worker's subscribed queues.
When the count stays zero for the settle window (and no jobs are active
on this worker), the monitor triggers the normal graceful shutdown
via orchestrate_shutdown — the same path SIGTERM takes.

Exit codes:
  0 — all jobs succeeded (drain_failures == 0)
  2 — some jobs failed (drain_failures > 0)
  3 — max_runtime exceeded before drain completed
"""

import asyncio
import time
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import structlog

from taskq.worker._leader_sweeps import _sleep_interruptible
from taskq.worker.shutdown import _orchestration_in_progress, orchestrate_shutdown

if TYPE_CHECKING:
    from taskq.backend._protocol import Backend
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

__all__ = [
    "EXIT_DRAIN_CLEAN",
    "EXIT_DRAIN_TIMEOUT",
    "EXIT_DRAIN_WITH_FAILURES",
    "drain_monitor_loop",
]

_log = structlog.get_logger("taskq.worker.drain")

EXIT_DRAIN_CLEAN = 0
EXIT_DRAIN_WITH_FAILURES = 2
EXIT_DRAIN_TIMEOUT = 3

# Transient/recoverable errors during count_active_jobs — the monitor
# continues polling. Non-recoverable errors (TypeError, ValueError, etc.
# from a buggy backend) propagate and tear down the TaskGroup.
_COUNT_RECOVERABLE_EXCEPTIONS = (
    TimeoutError,
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
    OSError,
)


async def drain_monitor_loop(
    deps: "WorkerDeps",
    settings: "WorkerSettings",
    worker_id: UUID,
    shutdown_event: asyncio.Event,
    escalate_event: asyncio.Event,
    orchestrator_holder: list[asyncio.Task[int]],
    backend: "Backend",
    *,
    idle_settle_window: float,
    idle_poll_interval: float,
    max_runtime: float | None,
) -> None:
    """Monitor for queue drain and trigger graceful shutdown when idle.

    Polls backend.count_active_jobs(queues) and deps.active_jobs.count()
    every idle_poll_interval. When both are zero, starts the settle timer.
    If still zero after idle_settle_window, triggers shutdown.

    If max_runtime is set and exceeded, triggers shutdown with exit code 3.

    Returns after creating the orchestrate_shutdown task. Spawns with
    may_return=True in the sibling spawner. Does NOT set shutdown_event
    — orchestrate_shutdown's finally block sets it at the correct point
    (after all phase work completes), exactly as the SIGTERM signal
    handler does.

    Double-orchestration guard (H2): if orchestrator_holder is already
    non-empty or deps.shutdown_phase is not ShutdownPhase.NONE, the
    monitor skips triggering — a SIGTERM-driven orchestration is already
    in progress.
    """
    queues = settings.queues
    start_time = time.monotonic()
    idle_since: float | None = None

    _log.info(
        "drain-monitor-start",
        queues=queues,
        settle_window=idle_settle_window,
        poll_interval=idle_poll_interval,
        max_runtime=max_runtime,
        worker_id=str(worker_id),
    )

    while not shutdown_event.is_set():
        # Check max_runtime
        if max_runtime is not None:
            elapsed = time.monotonic() - start_time
            if elapsed >= max_runtime:
                _log.info(
                    "drain-monitor-timeout",
                    elapsed=elapsed,
                    max_runtime=max_runtime,
                    worker_id=str(worker_id),
                )
                await _trigger_drain_shutdown(
                    deps,
                    settings,
                    worker_id,
                    shutdown_event,
                    escalate_event,
                    orchestrator_holder,
                    backend,
                    exit_code=EXIT_DRAIN_TIMEOUT,
                )
                return

        # Check idle condition — catch only recoverable transient errors
        # (F3). Non-recoverable errors propagate and tear down the TaskGroup.
        try:
            queue_count = await backend.count_active_jobs(queues)
        except _COUNT_RECOVERABLE_EXCEPTIONS:
            _log.warning("drain-monitor-count-error", worker_id=str(worker_id))
            queue_count = -1  # unknown — don't trigger

        active_count = deps.active_jobs.count()
        is_idle = queue_count == 0 and active_count == 0

        if is_idle:
            if idle_since is None:
                idle_since = time.monotonic()
                _log.debug(
                    "drain-monitor-idle-detected",
                    worker_id=str(worker_id),
                    queue_count=queue_count,
                    active_count=active_count,
                )
            else:
                idle_elapsed = time.monotonic() - idle_since
                if idle_elapsed >= idle_settle_window:
                    _log.info(
                        "drain-monitor-drained",
                        worker_id=str(worker_id),
                        idle_elapsed=idle_elapsed,
                        drain_failures=deps.drain_failures,
                    )
                    exit_code = (
                        EXIT_DRAIN_WITH_FAILURES if deps.drain_failures > 0 else EXIT_DRAIN_CLEAN
                    )
                    await _trigger_drain_shutdown(
                        deps,
                        settings,
                        worker_id,
                        shutdown_event,
                        escalate_event,
                        orchestrator_holder,
                        backend,
                        exit_code=exit_code,
                    )
                    return
        else:
            if idle_since is not None:
                _log.debug(
                    "drain-monitor-idle-reset",
                    worker_id=str(worker_id),
                    queue_count=queue_count,
                    active_count=active_count,
                )
            idle_since = None

        # Wait for poll interval or shutdown
        await _sleep_interruptible(shutdown_event, idle_poll_interval)

    _log.info("drain-monitor-exit", reason="shutdown_event", worker_id=str(worker_id))


async def _trigger_drain_shutdown(
    deps: "WorkerDeps",
    settings: "WorkerSettings",
    worker_id: UUID,
    shutdown_event: asyncio.Event,
    escalate_event: asyncio.Event,
    orchestrator_holder: list[asyncio.Task[int]],
    backend: "Backend",
    *,
    exit_code: int,
) -> None:
    """Create the orchestrate_shutdown task with a drain exit code.

    Mirrors the SIGTERM signal handler exactly: create the wrapper task,
    append to orchestrator_holder, and do NOT set shutdown_event —
    orchestrate_shutdown's finally sets it at the correct point (after
    all phase work completes).

    Double-orchestration guard (H2): if orchestrator_holder is already
    non-empty or deps.shutdown_phase is not ShutdownPhase.NONE, skip
    triggering. The create_task/append pair is effectively atomic under
    CPython's signal delivery model — signal handlers fire between event
    loop iterations, not between synchronous Python statements, so no
    signal can interleave between create_task() and .append().
    """
    if _orchestration_in_progress(orchestrator_holder, deps):
        _log.info(
            "drain-monitor-skip-trigger",
            reason="orchestration-already-active",
            holder_len=len(orchestrator_holder),
            shutdown_phase=deps.shutdown_phase,
            worker_id=str(worker_id),
        )
        return

    loop = asyncio.get_running_loop()

    async def _drain_orchestrate() -> int:
        await orchestrate_shutdown(
            deps,
            settings,
            worker_id,
            shutdown_event,
            escalate_event,
            backend=backend,
        )
        return exit_code

    task = loop.create_task(_drain_orchestrate())
    orchestrator_holder.append(task)
    _log.info(
        "drain-monitor-triggered-shutdown",
        exit_code=exit_code,
        worker_id=str(worker_id),
    )
