"""Two-phase shutdown orchestration.

Consumers (``orchestrate_shutdown``, health endpoints) MUST observe
``deps.shutdown_phase`` set at the START of each phase, BEFORE any
per-phase work.  Value ``NONE (0)`` means the worker is running normally.

Phase ordering invariant:
NONE (0) → DRAINING (1) → CANCELLING (2) → FORCING (3) → ABANDONING (4).

SIGQUIT is not registered; produces a core dump on Linux. Use tini or
``ulimit -c 0`` for containerised deployments.

The second-SIGTERM contract: if the second SIGTERM arrives during
FORCING or ABANDONING, setting ``escalate_event`` is a no-op — the
orchestrator is already past CANCELLING.
"""

import asyncio
import contextlib
import os
import signal
import sys
from enum import IntEnum
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import structlog

from taskq.backend._protocol import Backend, CancelPhase
from taskq.backend._sql import (
    parse_rowcount,  # pyright: ignore[reportPrivateUsage]  # Why: parse_rowcount is the canonical command-tag parser; used identically in worker/cancel.py.
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining.
)
from taskq.obs import get_logger

if TYPE_CHECKING:
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

__all__ = [
    "ShutdownPhase",
    "drain_local_queue_to_pending",
    "install_signal_handlers",
    "orchestrate_shutdown",
]

_log: structlog.stdlib.BoundLogger = get_logger(__name__)


class ShutdownPhase(IntEnum):
    """Worker shutdown phase.

    NONE       — running normally.
    DRAINING   — stop accepting new dispatch, finish in-flight jobs.
    CANCELLING — cooperative cancel of remaining jobs.
    FORCING    — force-cancel grace, terminal writes shielded.
    ABANDONING — pod must be replaced to reclaim slots.
    """

    NONE = 0
    DRAINING = 1
    CANCELLING = 2
    FORCING = 3
    ABANDONING = 4


async def drain_local_queue_to_pending(deps: "WorkerDeps", worker_id: UUID) -> int:
    """Re-pend every job this worker locked but never started.

    Issues a single bounded-timeout UPDATE that clears the lock on rows
    where ``locked_by_worker = $worker_id AND status = 'running' AND
    started_at IS NULL``.  On pool exhaustion or connection error the
    helper logs a warning and returns 0 so the recovery sweep acts as
    the backstop rather than a deadlocked shutdown.

    Returns:
        Number of rows updated, or 0 on timeout / connection error.
    """
    schema = deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    sql = (
        f"UPDATE \"{schema}\".jobs SET status='pending', locked_by_worker=NULL, "  # noqa: S608  # Why: schema validated against _IDENT_RE before interpolation; asyncpg has no parameter binding for identifiers (same rationale as migrate.py).
        f"lock_expires_at=NULL "
        f"WHERE locked_by_worker=$1 AND status='running' AND started_at IS NULL"
    )

    try:
        async with deps.dispatcher_pool.acquire(timeout=2.0) as conn:
            tag = await conn.execute(sql, worker_id)
            rowcount = parse_rowcount(tag)
            _log.info(
                "drain-local-queue-completed",
                worker_id=worker_id,
                rows_re_pended=rowcount,
            )
            return rowcount
    except (TimeoutError, asyncpg.PostgresConnectionError, OSError) as exc:
        _log.warning(
            "drain-local-queue-failed",
            worker_id=worker_id,
            error=str(exc),
        )
        return 0


async def orchestrate_shutdown(
    deps: "WorkerDeps",
    settings: "WorkerSettings",
    worker_id: UUID,
    shutdown_event: asyncio.Event,
    escalate_event: asyncio.Event | None = None,
    *,
    backend: Backend,
) -> int:
    """Run the four-phase shutdown orchestration.

    Phases are DRAINING → CANCELLING → FORCING → ABANDONING, followed by
    leader_conn close and ``shutdown_event.set()``.  Each phase is assigned
    to ``deps.shutdown_phase`` BEFORE any per-phase work.
    Returns 0 on clean exit.
    """
    loop = asyncio.get_running_loop()
    t0 = loop.time()

    try:
        # ── Phase 1: DRAINING ──────────────────────────────────────────
        deps.shutdown_phase = ShutdownPhase.DRAINING
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="DRAINING",
            active_jobs_count=deps.active_jobs.count(),
            elapsed_seconds=0.0,
        )
        deps.producer_stop_event.set()
        await drain_local_queue_to_pending(deps, worker_id)

        # ── Phase 2: CANCELLING ────────────────────────────────────────
        deps.shutdown_phase = ShutdownPhase.CANCELLING
        cancel_grace = settings.cancellation_grace_period
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="CANCELLING",
            active_jobs_count=deps.active_jobs.count(),
            elapsed_seconds=loop.time() - t0,
        )
        for active in deps.active_jobs.all():
            active.ctx.cancel_event.set()
            if active.cancel_phase < CancelPhase.COOPERATIVE:
                active.cancel_phase = CancelPhase.COOPERATIVE
                active.cancel_observed_at = loop.time()
            elif active.cancel_observed_at is None:
                active.cancel_observed_at = loop.time()

        deadline = loop.time() + cancel_grace
        while loop.time() < deadline and deps.active_jobs.count() > 0:
            if escalate_event is not None and escalate_event.is_set():
                break
            await asyncio.sleep(0.1)

        # ── Phase 3: FORCING ───────────────────────────────────────────
        deps.shutdown_phase = ShutdownPhase.FORCING
        cleanup_grace = settings.cleanup_grace_period
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="FORCING",
            active_jobs_count=deps.active_jobs.count(),
            elapsed_seconds=loop.time() - t0,
        )
        for active in deps.active_jobs.all():
            try:
                await asyncio.shield(
                    backend.write_cancel_escalation(active.job_id, worker_id, phase=2)
                )
            except Exception as e:
                _log.warning(
                    "force-cancel-pg-write-failed",
                    job_id=active.job_id,
                    error=str(e),
                )
                continue
            active.task.cancel()
            active.cancel_phase = CancelPhase.FORCED

        deadline = loop.time() + cleanup_grace
        while loop.time() < deadline and deps.active_jobs.count() > 0:  # noqa: ASYNC110  # Why: poll-for-exit with deadline is the intentional design for shutdown phases; the timed grace period cannot be expressed with Event alone.
            await asyncio.sleep(0.1)

        # ── Phase 4: ABANDONING ────────────────────────────────────────
        deps.shutdown_phase = ShutdownPhase.ABANDONING
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="ABANDONING",
            active_jobs_count=deps.active_jobs.count(),
            elapsed_seconds=loop.time() - t0,
        )
        for active in deps.active_jobs.all():
            try:
                await asyncio.shield(backend.mark_abandoned(active.job_id))
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                _log.warning(
                    "abandon-pg-write-failed",
                    job_id=active.job_id,
                    error=str(exc),
                )

        # ── leader_conn close ──────────────────────────────────
        if deps.leader_conn is not None:
            with contextlib.suppress(asyncpg.PostgresConnectionError, OSError):
                await deps.leader_conn.close()
            deps.leader_conn = None

        return 0
    finally:
        # ── Signal siblings and exit ───────────────────────────────────
        shutdown_event.set()
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="EXITED",
            active_jobs_count=deps.active_jobs.count(),
            elapsed_seconds=loop.time() - t0,
        )


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    deps: "WorkerDeps",
    worker_id: UUID,
    shutdown_event: asyncio.Event,
    escalate_event: asyncio.Event,
    backend: Backend,
    orchestrator_holder: list[asyncio.Task[int]],
) -> None:
    """Register SIGTERM/SIGINT handlers with a three-signal escalation counter.

    First signal schedules ``orchestrate_shutdown`` via ``loop.create_task``
    and appends the created task to ``orchestrator_holder`` so that ``_main``
    can later await it for the exit code.  Second signal sets
    ``escalate_event`` to fast-advance CANCELLING → FORCING.  Third signal
    calls ``sys.exit(1)`` (Kubernetes SIGKILL is the hard backstop).

    The signal counter is closure-scoped — each call to this function creates
    a fresh, independent counter.  The handler callable contains zero
    ``await`` or I/O.
    """
    _sig_count = 0

    def _on_signal() -> None:
        nonlocal _sig_count
        _sig_count += 1
        if _sig_count == 1:
            task = loop.create_task(
                orchestrate_shutdown(
                    deps,
                    deps.settings,
                    worker_id,
                    shutdown_event,
                    escalate_event,
                    backend=backend,
                )
            )
            orchestrator_holder.append(task)
        elif _sig_count == 2:
            escalate_event.set()
        else:
            sys.exit(1)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            _log.warning(
                "signal-handlers-unavailable",
                os_name=os.name,
            )
            return
