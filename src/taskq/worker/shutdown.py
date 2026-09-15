"""Two-phase shutdown orchestration.

Consumers (``orchestrate_shutdown``, health endpoints) MUST observe
``deps.shutdown_phase`` set at the START of each phase, BEFORE any
per-phase work.  Value ``NONE (0)`` means the worker is running normally.

Phase ordering invariant:
NONE (0) → DRAINING (1) → CANCELLING (2) → FORCING (3) → RELEASING (4).

A shutdown never decides a job's terminal state. Work interrupted by a
deploy, a drain or an eviction is released back to the fleet with its
attempt refunded, because "this process is leaving" says nothing about
whether the job can succeed. Only an operator cancel — a decision about
the job itself — keeps the terminal ladder that ends in ``abandoned``.

SIGQUIT is not registered; produces a core dump on Linux. Use tini or
``ulimit -c 0`` for containerised deployments.

The second-SIGTERM contract: if the second SIGTERM arrives during
FORCING or RELEASING, setting ``escalate_event`` is a no-op — the
orchestrator is already past CANCELLING.
"""

import asyncio
import os
import signal
import sys
from datetime import timedelta
from enum import IntEnum
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded
from taskq._shield import shield_with_retrieval
from taskq.backend._protocol import Backend, CancelPhase
from taskq.backend._sql import (
    parse_rowcount,  # pyright: ignore[reportPrivateUsage]  # Why: parse_rowcount is the canonical command-tag parser; used identically in worker/cancel.py.
)
from taskq.backend._sql_templates import ATTEMPT_REFUND_SQL
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining.
)
from taskq.obs import get_logger
from taskq.worker._transient import TRANSIENT_PG_ERRORS
from taskq.worker._watchdog import dump_task_stacks
from taskq.worker.cancel import CancelOrigin

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


def _orchestration_in_progress(
    orchestrator_holder: list[asyncio.Task[int]],
    deps: "WorkerDeps",
) -> bool:
    """Check whether a shutdown orchestration is already running.

    Shared guard used by both the SIGTERM signal handler and the drain
    monitor's ``_trigger_drain_shutdown`` to prevent double-orchestration
    (H2). Returns True if ``orchestrator_holder`` is non-empty (an
    orchestration task was already created) or ``deps.shutdown_phase`` is
    past ``NONE`` (an orchestration has started executing phases).
    """
    return bool(orchestrator_holder) or deps.shutdown_phase is not ShutdownPhase.NONE


class ShutdownPhase(IntEnum):
    """Worker shutdown phase.

    NONE       — running normally.
    DRAINING   — stop accepting new dispatch, finish in-flight jobs.
    CANCELLING — cooperative cancel of remaining jobs.
    FORCING    — force-cancel grace, terminal writes shielded.
    RELEASING  — hand whatever is still in flight back to the fleet.

    The integers are the wire format: ``/health`` reports them and the
    CLI tables them, so they never change. Only value 4's NAME changed,
    when the phase stopped terminalising work and started releasing it —
    an operator reading a phase at the moment a pod dies must not be told
    a confident wrong thing about what happened to their jobs.
    """

    NONE = 0
    DRAINING = 1
    CANCELLING = 2
    FORCING = 3
    RELEASING = 4


async def drain_local_queue_to_pending(deps: "WorkerDeps", worker_id: UUID) -> int:
    """Re-pend every job this worker claimed but never started.

    The claim that put a row in this worker's buffer already stamped
    ``attempt + 1``; the hand-back is the statement that no execution
    happened, so the increment is returned through the shared refund
    expression. Without it ``attempt`` climbs once per deploy for a job
    that never ran, and the budget an operator sized for real failures is
    spent absorbing their own rollouts.

    Issues a single bounded-timeout UPDATE that clears the lock on rows
    where ``locked_by_worker = $worker_id AND status = 'running'``,
    excluding the jobs with live consumers (``deps.active_jobs``):
    CANCELLING owns those, and re-pending one would unlock a row
    another worker can claim while its consumer still executes it. On
    pool exhaustion or connection error the helper logs a warning and
    returns 0 so the recovery sweep acts as the backstop rather than a
    deadlocked shutdown.

    Why no ``started_at IS NULL`` conjunct: the dispatch claim CTE
    stamps ``started_at = clock_timestamp()`` AT CLAIM
    (backend/_dispatch_sql.py), so every local_queue row is running +
    locked + ``started_at IS NOT NULL`` — an ``IS NULL`` predicate
    matched nothing and stranded the whole claimed-but-unstarted
    backlog until lock-lease expiry. The DB row carries no
    "a consumer took it" mark, so the only honest discriminator for
    "never started" is this process's own active-jobs registry; the
    claim-to-register window (a job taken off local_queue but not yet
    in ``active_jobs``) is invisible to every shutdown arm — CANCELLING
    iterates the same registry — and stays outside this predicate's
    guarantee.

    Returns:
        Number of rows updated, or 0 on timeout / connection error.
    """
    schema = deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    # Why the explicit list[UUID] annotation: JobId is NewType(UUID), so
    # the bare comprehension infers list[JobId], and list invariance
    # would refuse the uuid[] bind parameter's declared type below.
    active_ids: list[UUID] = [active.job_id for active in deps.active_jobs.all()]
    sql = (
        f"UPDATE \"{schema}\".jobs j SET status='pending', locked_by_worker=NULL, "  # noqa: S608  # Why: schema validated against _IDENT_RE before interpolation; asyncpg has no parameter binding for identifiers (same rationale as migrate.py).
        f"lock_expires_at=NULL, attempt={ATTEMPT_REFUND_SQL} "
        f"WHERE j.locked_by_worker=$1 AND j.status='running'"
    )
    # The exclusion clause is only bound when there is something to
    # exclude: an empty registry (the common drained-worker case) keeps
    # the single-parameter statement shape the helper has always issued.
    params: list[UUID | list[UUID]] = [worker_id]
    if active_ids:
        sql += " AND j.id <> ALL($2::uuid[])"
        params.append(active_ids)

    try:
        async with deps.dispatcher_pool.acquire(timeout=2.0) as conn:
            tag = await conn.execute(sql, *params)
            rowcount = parse_rowcount(tag)
            _log.info(
                "drain-local-queue-completed",
                worker_id=worker_id,
                rows_re_pended=rowcount,
                active_jobs_excluded=len(active_ids),
            )
            return rowcount
    except TRANSIENT_PG_ERRORS as exc:
        _log.warning(
            "drain-local-queue-failed",
            worker_id=worker_id,
            error=str(exc),
        )
        return 0


def _release_hold(
    deps: "WorkerDeps",
    settings: "WorkerSettings",
    loop: asyncio.AbstractEventLoop,
) -> timedelta:
    """How long a released row must stay unclaimable.

    A job released while its coroutine is still alive in this process
    must not be claimable elsewhere until this process can no longer
    touch it. With the watchdog on, that instant is known: it force-exits
    at ``termination_grace_period`` measured from the start of shutdown,
    so the hold is whatever remains of that budget.

    With the watchdog disabled nothing guarantees an exit, so the hold
    falls back to ``lock_lease`` — the same bound the lease-expiry path
    imposes today, now without spending the attempt to get it.
    """
    if not settings.watchdog_enabled:
        return timedelta(seconds=settings.lock_lease)
    started_at = deps.shutdown_started_at
    if started_at is None:
        return timedelta(seconds=settings.termination_grace_period)
    remaining = settings.termination_grace_period - (loop.time() - started_at)
    return timedelta(seconds=max(0.0, remaining))


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

    Phases are DRAINING → CANCELLING → FORCING → RELEASING, followed by
    TaskQ-owned ``leader_conn`` close and ``shutdown_event.set()``.  Each
    phase is assigned to ``deps.shutdown_phase`` BEFORE any per-phase work.
    Returns 0 on clean exit.
    """
    loop = asyncio.get_running_loop()
    t0 = loop.time()

    try:
        # ── Phase 1: DRAINING ──────────────────────────────────────────
        if deps.shutdown_started_at is None:
            deps.shutdown_started_at = t0
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
            # Only where no origin is recorded yet: an operator cancel the
            # controller already observed keeps its origin, because the
            # row says that cancel is about the job rather than about
            # this process leaving.
            if active.cancel_origin is CancelOrigin.NONE:
                active.cancel_origin = CancelOrigin.SHUTDOWN
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
            # The escalation write advances an OPERATOR cancel from phase 1
            # to phase 2. A shutdown-origin entry's row sits at phase 0 —
            # nobody wrote a cancel request for it — so the statement's
            # `cancel_phase = 1` guard could only ever match nothing, and
            # issuing it would be a guaranteed no-op whose False reads
            # like a failure. Skipping it is what leaves the escalation's
            # unmatched result meaningful.
            if active.cancel_origin is CancelOrigin.OPERATOR:
                try:
                    # shield_with_retrieval, not plain asyncio.shield: shutdown
                    # races escalating cancellation, so a detached write here can
                    # be double-cancelled — its outcome must be retrieved (see
                    # taskq._shield).
                    escalated = await shield_with_retrieval(
                        backend.write_cancel_escalation(active.job_id, worker_id, phase=2)
                    )
                except Exception as e:
                    _log.warning(
                        "force-cancel-pg-write-failed",
                        job_id=str(active.job_id),
                        error=str(e),
                    )
                    continue
                if not escalated:
                    _log.warning(
                        "force-cancel-escalation-unmatched",
                        kind="force_cancel_escalation_unmatched",
                        job_id=str(active.job_id),
                    )
            active.task.cancel()
            active.cancel_phase = CancelPhase.FORCED

        deadline = loop.time() + cleanup_grace
        while loop.time() < deadline and deps.active_jobs.count() > 0:  # noqa: ASYNC110  # Why: poll-for-exit with deadline is the intentional design for shutdown phases; the timed grace period cannot be expressed with Event alone.
            await asyncio.sleep(0.1)

        # ── Phase 4: RELEASING ─────────────────────────────────────────
        deps.shutdown_phase = ShutdownPhase.RELEASING
        hold = _release_hold(deps, settings, loop)
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="RELEASING",
            active_jobs_count=deps.active_jobs.count(),
            hold_seconds=hold.total_seconds(),
            elapsed_seconds=loop.time() - t0,
        )
        released = 0
        noop = 0
        abandoned = 0
        for active in deps.active_jobs.all():
            if active.cancel_origin is CancelOrigin.OPERATOR:
                # The operator asked for this job to stop, so the ladder
                # they started owns its terminal state — an unresponsive
                # actor under an operator cancel is still abandoned.
                try:
                    if await shield_with_retrieval(backend.mark_abandoned(active.job_id)):
                        abandoned += 1
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    _log.warning(
                        "abandon-pg-write-failed",
                        job_id=str(active.job_id),
                        error=str(exc),
                    )
                continue
            try:
                outcome = await shield_with_retrieval(
                    backend.mark_interrupted(
                        active.job_id,
                        worker_id,
                        attempt=active.ctx.attempt,
                        hold=hold,
                    )
                )
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                _log.warning(
                    "interrupt-pg-write-failed",
                    job_id=str(active.job_id),
                    error=str(exc),
                )
            else:
                if outcome == "noop":
                    # An operator cancel landed on the row after this
                    # entry was stamped, or the job's own consumer
                    # released it first. Either way the row has moved on
                    # and this process must not write over it.
                    noop += 1
                    _log.debug(
                        "interrupt-fenced-out",
                        kind="interrupt_fenced_out",
                        job_id=str(active.job_id),
                    )
                else:
                    released += 1
        _log.info(
            "shutdown-phase-releasing-result",
            kind="shutdown_phase",
            phase="RELEASING",
            released=released,
            noop=noop,
            abandoned=abandoned,
            hold_seconds=hold.total_seconds(),
        )

        # ── leadership handback ────────────────────────────────
        # Resigning the role is what makes a replacement pod's takeover take
        # one election cycle instead of the remainder of a lease nobody is
        # renewing. It runs BEFORE the connection close below, because the
        # statement needs that connection; it is fenced on this pod's term,
        # so it can only ever give up a role this pod still holds. Imported
        # here rather than at module scope: the leader module reaches this
        # one through deps, so a top-level import would close a cycle.
        from taskq.worker.leader import resign_leadership

        await resign_leadership(deps, worker_id)

        # ── leader_conn close ──────────────────────────────────
        # Why the owns_leader_conn guard: the ownership contract ("TaskQ
        # never closes caller-owned resources") forbids closing a
        # caller-provided leader_conn even during shutdown. The reference
        # is also left in place for caller-owned conns: the leader
        # election loop keeps running until shutdown_event fires (finally
        # block below), and a None leader_conn would make it open a
        # *fresh* conn and possibly re-acquire the role mid-shutdown. For
        # TaskQ-owned conns, close+null also releases the transition
        # advisory lock with the session.
        #
        # Why set → null → close, in that order (two races, one ordering):
        # (a) the bounded close can park for seconds, so shutdown_event is
        # set FIRST to stop the election loop — a still-live loop could
        # otherwise drop the closing conn and swap in a fresh (possibly
        # lock-holding) one mid-park. (b) the early set also releases
        # _main's ``await shutdown_event.wait()`` INSIDE the
        # open_worker_deps context (the orchestrator is awaited only after
        # that context exits), so the deps exit-stack guard unwinds
        # CONCURRENTLY with this parked close — nulling BEFORE the park is
        # what stops the guard entering a second close_conn_bounded on the
        # same conn (one closer's terminate would abort the other's
        # in-flight close and log a spurious conn-teardown-close-error on
        # real asyncpg). A conn swapped in mid-park keeps its reference
        # (nothing nulls after the park); the exit-stack guard closes it.
        conn = deps.leader_conn
        if conn is not None and deps.owns_leader_conn:
            # Why this can be a module-level import from taskq._close:
            # taskq._close imports nothing from taskq.worker, so the
            # deps↔shutdown cycle (deps imports ShutdownPhase from THIS
            # module) is not re-introduced. The bounded helper never raises
            # (timeout → terminate, error → log), so a dead PG cannot wedge
            # shutdown on an unbounded close.
            shutdown_event.set()  # stop the election loop BEFORE the close park
            # BEFORE the park: the deps exit-stack guard unwinds concurrently
            # once shutdown_event is set; it must not enter a second
            # close_conn_bounded on this same conn while the close below is
            # parked.
            deps.leader_conn = None
            await close_conn_bounded(conn, "leader", CLOSE_TIMEOUT_SECS)

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
    """Register SIGTERM/SIGINT/SIGHUP handlers.

    **SIGTERM/SIGINT**: three-signal escalation counter.  First signal
    schedules ``orchestrate_shutdown`` via ``loop.create_task`` and
    appends the created task to ``orchestrator_holder`` so that ``_main``
    can later await it for the exit code.  Second signal sets
    ``escalate_event`` to fast-advance CANCELLING → FORCING.  Third signal
    calls ``sys.exit(1)`` (Kubernetes SIGKILL is the hard backstop).

    **SIGHUP**: sets ``deps.reload_event`` to signal the hot-reload
    coordinator (:func:`~taskq.worker.deps.reload_credentials`) that a
    credential refresh has been requested. The coordinator runs as a
    sibling task in the worker's ``TaskGroup`` and performs the actual
    pool/connection swap. SIGHUP can be sent multiple times — each one
    sets the event. The coordinator clears the event *before* each
    reload and never after, so a SIGHUP arriving mid-reload (success OR
    failure) is honored with exactly one follow-up reload: N signals
    during one reload coalesce into one follow-up, not N. Reload
    requests arriving while shutdown orchestration is in progress
    (``deps.shutdown_phase`` is not NONE) are skipped.

    The signal counter is closure-scoped — each call to this function
    creates a fresh, independent counter.  The handler callable contains
    zero ``await`` or I/O.
    """
    _sig_count = 0

    def _on_shutdown_signal() -> None:
        nonlocal _sig_count
        _sig_count += 1
        if _sig_count == 1:
            # H2 guard: skip if orchestration is already in progress
            # (e.g., drain monitor triggered first). The _sig_count is
            # NOT reset — the next SIGTERM correctly escalates the
            # already-running orchestration rather than starting a new one.
            if _orchestration_in_progress(orchestrator_holder, deps):
                return
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

    def _on_reload_signal() -> None:
        deps.reload_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_shutdown_signal)
        except NotImplementedError:
            _log.warning(
                "signal-handlers-unavailable",
                os_name=os.name,
            )
            return
    # SIGHUP — credential hot-reload. Not available on Windows.
    if hasattr(signal, "SIGHUP"):
        try:
            loop.add_signal_handler(signal.SIGHUP, _on_reload_signal)
        except NotImplementedError:
            _log.warning("sighup-handler-unavailable", os_name=os.name)

    # SIGUSR2 — on-demand asyncio task-stack dump (names, coros, await
    # sites; no locals or payload values). Live debugging without an
    # image rebuild. Not available on Windows.
    if hasattr(signal, "SIGUSR2"):

        def _on_dump_signal() -> None:
            dump_task_stacks("sigusr2", detector="sigusr2")

        try:
            loop.add_signal_handler(signal.SIGUSR2, _on_dump_signal)
        except NotImplementedError:
            _log.warning("sigusr2-handler-unavailable", os_name=os.name)
