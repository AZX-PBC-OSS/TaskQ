"""Two-phase shutdown orchestration.

Consumers (``orchestrate_shutdown``, health endpoints) MUST observe
``deps.shutdown_phase`` set at the START of each phase, BEFORE any
per-phase work.  Value ``NONE (0)`` means the worker is running normally.

Phase ordering invariant:
NONE (0) → DRAINING (1) → CANCELLING (2) → FORCING (3) → RELEASING (4).

SIGQUIT is not registered; produces a core dump on Linux. Use tini or
``ulimit -c 0`` for containerised deployments.

The second-SIGTERM contract: if the second SIGTERM arrives during
FORCING or RELEASING, setting ``escalate_event`` is a no-op, the
orchestrator is already past CANCELLING.

What the phases owe the work: a deploy is an infrastructure event, so it
never terminalises a job. Rows claimed but
never started are handed back at DRAINING (attempt refunded; the producer
repeats the hand-back on exit so a claim round in flight at the signal is
caught too). Rows mid-execution get the cooperative cancel at CANCELLING
and the forced cancel at FORCING; an actor that unwinds is *interrupted* ,
released back to the fleet, the spent attempt standing; and one still
alive past
both graces is interrupted with a hold at RELEASING (released only once
the process is provably gone). ``abandoned`` stays on the operator-cancel
ladder (the row carries ``cancel_requested_at``): the FORCING escalation
probe and the RELEASING ``mark_interrupted`` fence keep an operator's
request ahead of any release.
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
from taskq.backend._protocol import Backend, CancelPhase, JobId
from taskq.backend._sql import (
    parse_rowcount,  # pyright: ignore[reportPrivateUsage]  # Why: parse_rowcount is the canonical command-tag parser; used identically in worker/cancel.py.
)
from taskq.backend._sql_fragments import ATTEMPT_REFUND_SQL
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining.
)
from taskq.context import CancelOrigin
from taskq.obs import get_logger
from taskq.progress._buffer import (
    _terminal_seq_and_state,  # pyright: ignore[reportPrivateUsage]  # Why: the release write carries the coalesced buffer exactly as the consumer's own terminal writes do, one seq/state projection, not a second copy.
)
from taskq.worker._transient import TRANSIENT_PG_ERRORS
from taskq.worker._watchdog import dump_task_stacks

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

    NONE      , running normally.
    DRAINING  , stop accepting new dispatch, hand back claimed-but-unstarted
                 jobs (attempt refunded).
    CANCELLING, cooperative cancel of remaining jobs; stamps the shutdown
                 origin on each signalled job.
    FORCING   , force-cancel grace, terminal writes shielded; the cancel
                 escalation write doubles as the origin probe (it lands only
                 on rows already carrying an operator's cancel request).
    RELEASING , release jobs whose actors never unwound back to the fleet
                 (``mark_interrupted``: the spent attempt stands, held
                 until this process is provably gone); jobs under an
                 operator cancel still reach ``abandoned`` here.

    The value 4 was ``ABANDONING`` before the release phase stopped
    abandoning, the integer is unchanged, so ``/health`` JSON and the CLI
    table keep their numbers; only the label moved to what the phase does.
    """

    NONE = 0
    DRAINING = 1
    CANCELLING = 2
    FORCING = 3
    RELEASING = 4


async def drain_local_queue_to_pending(deps: "WorkerDeps", worker_id: UUID) -> int:
    """Re-pend every job this worker claimed but never started.

       Issues a single bounded-timeout UPDATE that clears the lock on rows
       where ``locked_by_worker = $worker_id AND status = 'running'``,
       excluding the jobs with live consumers (``deps.active_jobs``):
       CANCELLING owns those, and re-pending one would unlock a row
       another worker can claim while its consumer still executes it. On
       pool exhaustion or connection error the helper logs a warning and
       returns 0 so the recovery sweep acts as the backstop rather than a
       deadlocked shutdown.

       Two callers, one pass each, both on this worker's way out: the
       orchestrator's DRAINING phase, and the producer loop's own exit (a
       claim round in flight when the stop event landed commits after the
       DRAINING pass; the producer's exit pass is the one write that cannot
       be overtaken by this worker's next claim, because there is none).
       Both refund the claim's attempt increment through the shared
       ``ATTEMPT_REFUND_SQL`` fragment, a claim that never reached an
       actor bought nothing, so it spends nothing (the same idiom the
       snooze arms carry; the refund is floored at 0
       and a second pass matches no rows, so the two passes together are
       exactly-once). ``mark_interrupted`` is the deliberate exception: its
    attempt DID start executing, so it keeps the increment.

       Why the ``started_at IS NOT NULL`` conjunct: the dispatch claim CTE
       stamps ``started_at = clock_timestamp()`` AT CLAIM
       (backend/_dispatch_sql.py), so every row the drain exists for is
       stamped and the conjunct never fences a claimed-but-unstarted row.
       What it fences is the OTHER refund writer's output: the heartbeat's
       claim-loss reconcile (worker/heartbeat.py) refunds through the same
       fragment and UN-STAMPS ``started_at`` while leaving the row
       ``running`` and locked (Sweep 1 owns that reclaim). On a
       running-and-locked row a NULL ``started_at`` means exactly "this
       claim was already refunded", so without the conjunct the drain
       re-pends the reconciled row and refunds the SAME claim a second
       time: the counter drops below the epoch a genuine execution's
       ``job_attempts`` row holds, and the row is yanked out of the
       Sweep-1 reclaim the reconcile preserved. The conjunct is a
       row-version check, not a memory snapshot, so it holds on the
       concurrent interleave too: the drain's UPDATE blocking on the
       reconcile's open transaction re-evaluates the row when the
       reconcile commits (EvalPlanQual) and the committed NULL stamp
       drops it from the write set. The refund is exactly-once per claim
       across both writers. The DB row carries no
       "a consumer took it" mark, so the only honest discriminator for
       "never started" is this process's own active-jobs registry; the
       claim-to-register window (a job taken off local_queue but not yet
       in ``active_jobs``) is invisible to every shutdown arm, CANCELLING
       iterates the same registry, and stays outside this predicate's
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
    # held_ids() covers BOTH maps: registered consumers (executing now)
    # and claim intents (taken off local_queue, not yet registered, the
    # window the registry's comment documents).
    active_ids: list[JobId] = deps.active_jobs.held_ids()
    # The attempt refund: the claim stamped attempt + 1 for an execution
    # this hand-back says never happened, so the increment goes back ,
    # the same non-consuming-release idiom the snooze/unavailable and
    # interruption arms carry through ATTEMPT_REFUND_SQL. Without it
    # every rolling deploy spends
    # one retry of every claimed-but-unstarted job's budget. The alias
    # ``j`` is what the shared fragment qualifies on.
    sql = (
        f"UPDATE \"{schema}\".jobs j SET status='pending', locked_by_worker=NULL, "  # noqa: S608  # Why: schema validated against _IDENT_RE before interpolation; asyncpg has no parameter binding for identifiers (same rationale as migrate.py).
        # A drain re-pend hands the row back to the fleet, so it routes
        # by the actor's current assignment from here on (the routing
        # contract in taskq/backend/_dispatch_sql.py) -- the same
        # re-pend class as _SWEEP_1_SQL and the isolate template.
        f"lock_expires_at=NULL, assignment_routed=true, attempt = {ATTEMPT_REFUND_SQL} "
        f"WHERE locked_by_worker=$1 AND status='running' AND j.cancel_phase = 0 "
        "AND NOT EXISTS ("
        f'    SELECT 1 FROM "{schema}".job_attempts a'
        "    WHERE a.job_id = j.id AND a.attempt = j.attempt"
        ")"
        # The exactly-once refund fence across the two refund writers
        # (the docstring's conjunct paragraph): a running-and-locked row
        # with a NULL started_at is the heartbeat reconcile's refunded
        # output, already refunded once, and the drain must leave it for
        # Sweep 1's reclaim.
        " AND j.started_at IS NOT NULL"
    )
    # The ledger guard (the NOT EXISTS above): the refund's premise - "a
    # claim that never reached an actor bought nothing" - is enforced
    # with data, not just the registry's absence. An attempt row already
    # recorded for the row's current attempt number (the isolate-self
    # write, the sweep's reclaim INSERT, a retried terminal write) IS a
    # started attempt: its charge stands, and refunding it would make
    # the next claim revisit the number the PK already carries - the
    # exact collision the ATTEMPT_REFUND_SQL fragment's safety argument
    # excludes. A never-started claim (no registry entry, no ledger row
    # - the drain's whole design) matches nothing and refunds unchanged;
    # a guard-blocked row stays running under its lease, which lapse
    # hands to Sweep 1, the same owner every other started attempt's
    # reclaim has.
    # The cancel fence (``cancel_phase = 0``): a row carrying an operator
    # cancel in flight must NOT re-enter the fleet through the drain: the
    # same fence every other deferral/release arm carries (the snooze and
    # retry-after arms, mark_interrupted's release arm, sweep-1's
    # cancel-first CASE, the isolate template). Without it, a cancel
    # stamped between the operator's write and the consumer's next flag
    # poll would be handed back to dispatch, and the row would execute
    # again under the next holder before any ladder terminalises it. The
    # fenced row stays here: running, owned by this worker, refund never
    # applied; its lease expiry hands it to sweep-1's cancel arm, which
    # terminalises it with the operator's audit intact.
    # The exclusion clause is only bound when there is something to
    # exclude: an empty registry (the common drained-worker case) keeps
    # the single-parameter statement shape the helper has always issued.
    params: list[UUID | list[JobId]] = [worker_id]
    if active_ids:
        sql += " AND id <> ALL($2::uuid[])"
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


def _watchdog_exit_tail(settings: "WorkerSettings") -> float:
    """Seconds past the termination deadline the process can still be alive.

    The deadline trip is not instantaneous. ``ShutdownWatchdog`` checks the
    deadline once per ``watchdog_dump_interval`` sleep (clipped to the
    deadline, so real check lag is loop jitter and the dump-interval term
    is margin), then ``trip()`` renders every live task's stack
    (synchronous, on the loop thread) and joins the bounded metrics flush
    (``WATCHDOG_METRICS_FLUSH_TIMEOUT_SECS``) before ``os._exit``. The
    stack render and the critical log write have no bound of their own, so
    the fixed slack covers them. A hold that ends at the bare deadline
    leaves exactly this window in which the row is claimable while the
    dying process can still touch it: the overlap the hold exists to
    prevent. The composition itself lives on the settings
    (``WorkerSettings.release_exit_tail_seconds``) so the disown-path
    lease floor reads the identical arithmetic; this reader exists so the
    worker layer's call sites stay named.
    """
    return settings.release_exit_tail_seconds


def _release_hold(
    deps: "WorkerDeps | None",
    settings: "WorkerSettings",
    loop: asyncio.AbstractEventLoop,
) -> timedelta:
    """The hold a RELEASING-phase release carries.

    A job released while its coroutine may still be alive in this process
    must not be claimable by another pod until this process cannot touch it
    any more. The shutdown watchdog force-exits at
    ``termination_grace_period`` counted from the shutdown's start, so the
    hold is the budget's remaining share plus the exit tail past the
    deadline itself: the dump-interval lag before the trip is observed and
    the bounded flush the trip performs before ``os._exit``
    (:func:`_watchdog_exit_tail`). Zero is fine: the release then lands
    pending, and a past-budget process is already on borrowed time the tail
    still covers. With ``watchdog_enabled = False`` there is no guaranteed
    exit, so the hold is ``lock_lease``: the bound the lease-expiry path
    already imposes today, now without spending the attempt.

    *deps* may be ``None`` (the consumer's release arm on a bare direct
    call): there is no shutdown start to anchor on, so the defensive full
    budget applies: the same bound the watchdog enforces from the first
    signal.
    """
    if not settings.watchdog_enabled:
        return timedelta(seconds=settings.lock_lease)
    tail = _watchdog_exit_tail(settings)
    started_at = deps.shutdown_started_at if deps is not None else None
    if started_at is None:
        # Unreachable through orchestrate_shutdown (DRAINING stamps it
        # first); the defensive shape is the full budget, the same bound
        # the watchdog enforces from the first signal.
        return timedelta(seconds=settings.termination_grace_period + tail)
    remaining = settings.termination_grace_period - (loop.time() - started_at)
    return timedelta(seconds=max(0.0, remaining) + tail)


def _cancel_origin_counts(deps: "WorkerDeps") -> dict[str, int]:
    """Active-job cancel-origin tallies for the CANCELLING phase log."""
    counts: dict[str, int] = {"operator": 0, "shutdown": 0}
    for active in deps.active_jobs.all():
        if active.cancel_origin is CancelOrigin.OPERATOR:
            counts["operator"] += 1
        elif active.cancel_origin is CancelOrigin.SHUTDOWN:
            counts["shutdown"] += 1
    return counts


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
        for active in deps.active_jobs.all():
            active.ctx.cancel_event.set()
            if active.cancel_phase < CancelPhase.COOPERATIVE:
                active.cancel_phase = CancelPhase.COOPERATIVE
                active.cancel_observed_at = loop.time()
            elif active.cancel_observed_at is None:
                active.cancel_observed_at = loop.time()
            # Stamp the shutdown as the cancel's origin, unless an
            # operator's cancel was already observed (the controller's
            # PG-observation arms stamp OPERATOR; the row says who asked).
            # The consumer's terminal routing reads the origin: SHUTDOWN
            # releases the attempt back to the fleet (interrupted), an
            # operator's keeps the cancel ladder. A later operator cancel
            # still wins: the controller overrides the stamp on its next
            # observation, and mark_interrupted's cancel_phase = 0 fence
            # declines the row either way.
            if active.cancel_origin is CancelOrigin.NONE:
                active.cancel_origin = CancelOrigin.SHUTDOWN
                active.ctx._set_cancel_origin(CancelOrigin.SHUTDOWN)  # pyright: ignore[reportPrivateUsage]  # Why: the orchestrator is the designated shutdown-side writer of the context's origin stamp (set alongside cancel_event.set(), per the field's contract).
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="CANCELLING",
            active_jobs_count=deps.active_jobs.count(),
            cancel_origin_counts=_cancel_origin_counts(deps),
            elapsed_seconds=loop.time() - t0,
        )

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
                # shield_with_retrieval, not plain asyncio.shield: shutdown
                # races escalating cancellation, so a detached write here can
                # be double-cancelled, its outcome must be retrieved (see
                # taskq._shield).
                #
                # The escalation write doubles as the origin probe: it lands
                # only when the row is at cancel_phase = 1, i.e. carrying an
                # operator's cancel request. A SHUTDOWN-stamped entry whose
                # probe lands was racing an unobserved operator cancel (the
                # heartbeat poll had not seen the row yet), the row says
                # who asked, so the entry re-stamps OPERATOR and stays on
                # the operator ladder (RELEASING abandons it past the
                # graces). A probe that misses on an OPERATOR-stamped entry
                # means the row moved under the ladder (already terminal or
                # reclaimed); that is logged, never silently discarded.
                escalated = await shield_with_retrieval(
                    backend.write_cancel_escalation(active.job_id, worker_id, phase=2)
                )
            except Exception as e:
                _log.warning(
                    "force-cancel-pg-write-failed",
                    job_id=str(active.job_id),
                    error=str(e),
                )
                # The local cancel is delivered even when the row-side
                # write failed: skipping it let a cancellable actor run
                # untouched into RELEASING and be released-with-hold
                # while still alive: the exact overlap the hold exists
                # to prevent. The escalation probe is the row-side
                # half of FORCING; task.cancel() is the process-side
                # half, and only both together advance the entry. The
                # phase stamp mirrors the success path below so the
                # registry's local ladder stays truthful either way.
                active.task.cancel()
                active.cancel_phase = CancelPhase.FORCED
                continue
            if escalated and active.cancel_origin is CancelOrigin.SHUTDOWN:
                active.cancel_origin = CancelOrigin.OPERATOR
                active.ctx._set_cancel_origin(CancelOrigin.OPERATOR)  # pyright: ignore[reportPrivateUsage]  # Why: the orchestrator is the designated shutdown-side writer of the context's origin stamp (per the field's contract).
                _log.info(
                    "force-cancel-escalation-observed-operator-cancel",
                    kind="cancel_origin",
                    job_id=str(active.job_id),
                    worker_id=str(worker_id),
                )
            elif not escalated and active.cancel_origin is CancelOrigin.OPERATOR:
                _log.warning(
                    "force-cancel-escalation-unmatched",
                    job_id=str(active.job_id),
                    worker_id=str(worker_id),
                )
            active.task.cancel()
            active.cancel_phase = CancelPhase.FORCED

        deadline = loop.time() + cleanup_grace
        while loop.time() < deadline and deps.active_jobs.count() > 0:  # noqa: ASYNC110  # Why: poll-for-exit with deadline is the intentional design for shutdown phases; the timed grace period cannot be expressed with Event alone.
            await asyncio.sleep(0.1)

        # ── Phase 4: RELEASING ─────────────────────────────────────────
        # Every entry still registered belongs to an actor that ignored
        # both cancels. The shutdown owes it a release, not a verdict:
        # mark_interrupted hands the row back to the fleet with the spent
        # attempt standing (no refund; the attempt started executing),
        # HELD behind the rest of this
        # process's termination budget: plus the watchdog's exit tail
        # past the deadline itself (the dump-interval lag before the trip
        # is observed and the bounded flush before os._exit), so no other
        # pod can claim the row while this one might still touch it. The
        # release is ordered before the process dies, never after, and the
        # hold closes the overlap where the row is claimable while the
        # dying process could still touch it.
        deps.shutdown_phase = ShutdownPhase.RELEASING
        hold = _release_hold(deps, settings, loop)
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="RELEASING",
            active_jobs_count=deps.active_jobs.count(),
            elapsed_seconds=loop.time() - t0,
        )
        released_count = 0
        noop_count = 0
        abandoned_count = 0
        for active in deps.active_jobs.all():
            # An OPERATOR entry belongs to the cancel ladder, never to a
            # release: one shielded write (mark_abandoned's phase-2 /
            # NULL-lease guard decides, the FORCING probe put the row at
            # phase 2 by construction), the phase's pre-rename shape
            # exactly.
            if active.cancel_origin is CancelOrigin.OPERATOR:
                try:
                    if await shield_with_retrieval(backend.mark_abandoned(active.job_id)):
                        abandoned_count += 1
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    _log.warning(
                        "abandon-pg-write-failed",
                        job_id=str(active.job_id),
                        error=str(exc),
                    )
                continue
            _buf = deps.progress_buffers.get(active.job_id)
            _seq, _state = _terminal_seq_and_state(_buf)
            try:
                outcome = await shield_with_retrieval(
                    backend.mark_interrupted(
                        active.job_id,
                        worker_id,
                        attempt=active.ctx.attempt,
                        claim_epoch=active.ctx.claim_epoch,
                        hold=hold,
                        progress_seq=_seq,
                        progress_state=_state if _buf is not None and _buf.dirty else None,
                    )
                )
            except asyncio.CancelledError:
                continue
            except Exception as exc:
                # No retry, no disown. The row stays running and locked.
                # Its recovery is the lease-expiry reclaim sweep. What
                # keeps that fallback safe is NOT a settings-load
                # rejection. No such validation exists, by design. Two
                # real protections do the work, and both live in code:
                #
                # 1. The parked consumer's later release write usually
                #    saves the row outright. That park is lease-capped
                #    (WorkerSettings.release_park_lease_cap, applied in
                #    _actor_exit_wait_budget): the cap is exactly the
                #    bound that puts the parked release write ahead of the
                #    earliest lease reclaim for every config that loads.
                #    Do not remove the cap on the belief that startup
                #    validation catches a bad config here. It does not.
                #    With the cap gone, a lease shorter than the
                #    termination budget lets the sweep re-pend this row
                #    while its actor thread still executes.
                # 2. When both writes fail (this one and the consumer's,
                #    whose exhausted retries disown the row), the residue
                #    is the WorkerSettings.release_disown_lease_floor
                #    bound. That one is surfaced as a startup warning by
                #    _emit_startup_warnings, again not a rejection.
                #
                # If you are tempted to add a retry here, first read the
                # mark_cancelled arm's comment in _consumer.py: a retry
                # wait inside this phase races the watchdog's deadline and
                # the teardown that deadline bounds. The sweep is the
                # designed backstop.
                _log.warning(
                    "release-pg-write-failed",
                    job_id=str(active.job_id),
                    error=str(exc),
                )
                continue
            if outcome == "noop":
                # The fence declined: the row is no longer this worker's to
                # release (its own consumer already terminalised it, or an
                # operator cancel landed on it between FORCING and
                # RELEASING). The row is the arbiter of origin: a
                # cancel-carrying row gets the ladder's terminal write,
                # whose own guard decides (a phase-1 row rides the cancel
                # controller's remaining rungs; a terminal row matches
                # nothing).
                noop_count += 1
                _log.debug(
                    "release-interrupted-noop",
                    job_id=str(active.job_id),
                    worker_id=str(worker_id),
                    cancel_origin=int(active.cancel_origin),
                )
                try:
                    if await shield_with_retrieval(backend.mark_abandoned(active.job_id)):
                        abandoned_count += 1
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    _log.warning(
                        "abandon-pg-write-failed",
                        job_id=str(active.job_id),
                        error=str(exc),
                    )
            else:
                released_count += 1
        _log.info(
            "shutdown-phase",
            kind="shutdown_phase",
            phase="RELEASING",
            released=released_count,
            held_seconds=hold.total_seconds(),
            noop=noop_count,
            abandoned=abandoned_count,
            elapsed_seconds=loop.time() - t0,
        )

        # ── leader_conn close ──────────────────────────────────
        # Why the owns_leader_conn guard: the ownership contract ("TaskQ
        # never closes caller-owned resources") forbids closing a
        # caller-provided leader_conn even during shutdown. The reference
        # is also left in place for caller-owned conns: the leader
        # election loop keeps running until shutdown_event fires (finally
        # block below), and a None leader_conn would make it open a
        # *fresh* conn and possibly re-acquire the advisory lock
        # mid-shutdown. For TaskQ-owned conns, close+null frees the
        # session (and with it the courtesy election lock) before the
        # SIGTERM budget expires; the lease ROW is freed separately, the
        # leader runtime's own teardown resigns it over a conn that
        # survives this close (leader.py's resign), so the ordering here
        # cannot strand it.
        #
        # Why set → null → close, in that order (two races, one ordering):
        # (a) the bounded close can park for seconds, so shutdown_event is
        # set FIRST to stop the election loop, a still-live loop could
        # otherwise drop the closing conn and swap in a fresh (possibly
        # lock-holding) one mid-park. (b) the early set also releases
        # _main's ``await shutdown_event.wait()`` INSIDE the
        # open_worker_deps context (the orchestrator is awaited only after
        # that context exits), so the deps exit-stack guard unwinds
        # CONCURRENTLY with this parked close, nulling BEFORE the park is
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
    pool/connection swap. SIGHUP can be sent multiple times, each one
    sets the event. The coordinator clears the event *before* each
    reload and never after, so a SIGHUP arriving mid-reload (success OR
    failure) is honored with exactly one follow-up reload: N signals
    during one reload coalesce into one follow-up, not N. Reload
    requests arriving while shutdown orchestration is in progress
    (``deps.shutdown_phase`` is not NONE) are skipped.

    The signal counter is closure-scoped, each call to this function
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
            # NOT reset, the next SIGTERM correctly escalates the
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
    # SIGHUP, credential hot-reload. Not available on Windows.
    if hasattr(signal, "SIGHUP"):
        try:
            loop.add_signal_handler(signal.SIGHUP, _on_reload_signal)
        except NotImplementedError:
            _log.warning("sighup-handler-unavailable", os_name=os.name)

    # SIGUSR2, on-demand asyncio task-stack dump (names, coros, await
    # sites; no locals or payload values). Live debugging without an
    # image rebuild. Not available on Windows.
    if hasattr(signal, "SIGUSR2"):

        def _on_dump_signal() -> None:
            dump_task_stacks("sigusr2", detector="sigusr2")

        try:
            loop.add_signal_handler(signal.SIGUSR2, _on_dump_signal)
        except NotImplementedError:
            _log.warning("sigusr2-handler-unavailable", os_name=os.name)
