"""Heartbeat loop and cancel-poll seam.

Each tick acquires one connection from heartbeat_pool, opens a single
transaction, and atomically extends workers.last_seen_at, jobs lock /
heartbeat columns, and reservation_slots leases for jobs locked by this
worker, except the jobs the worker has disowned (``WorkerDeps.disowned_jobs``:
finished with, outcome unrecordable), whose leases must lapse so the
reclaim sweep can hand them back. The tick's whole command sequence runs
under ONE command-timeout budget (see the tick block and
:func:`_lease_renewal_threshold` for the lease-model arithmetic that
depends on it), with a bounded rollback-or-close teardown. The jobs-lock
renewal is threshold-gated (:func:`_lease_renewal_threshold`): a row
whose lease is still comfortably fresh is left alone so a healthy beat
stops paying a non-HOT update per running row per tick, while
rows carrying a per-job ``heartbeat_timeout`` (the reclaim sweep's
heartbeat arm needs their beats fresh) and rows at/under the threshold
renew on every beat. After max_heartbeat_failures consecutive failed
ticks, transient connection failures and unexpected errors alike, with
the count reset only by a fully successful tick, isolate_self
proactively transitions running jobs and signals shutdown.
"""

import asyncio
import contextlib
import time
from datetime import timedelta
from uuid import UUID

import asyncpg
import structlog

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded
from taskq._dsn import dsn_host
from taskq._shield import shield_with_retrieval
from taskq.backend._protocol import CancelPhase, JobId
from taskq.backend._records import jsonb_param
from taskq.backend._sql import (
    INSERT_ATTEMPT_SQL,
    INSERT_EVENTS_DETAIL_BATCH_SQL,
    build_heartbeat_sql,
    parse_rowcount,
)
from taskq.backend._sql_fragments import ATTEMPT_REFUND_SQL
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: isolate and the reclaim sweep must decide a job's budget and its hand-back delay identically, one fragment, no second hand-maintained copy.
    _RECLAIM_DELAY_SQL,
    _RECLAIM_HAS_BUDGET_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    ERROR_CLASS_HEARTBEAT_LOST,
)
from taskq.context import CancelOrigin
from taskq.obs import (
    get_logger,
    get_meter,
    record_heartbeat_miss,
    record_lock_expires_in_seconds,
    update_heartbeat_consecutive_failures,
)
from taskq.worker._transient import TRANSIENT_PG_ERRORS
from taskq.worker.cancel import CancelController
from taskq.worker.deps import WorkerDeps

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_meter = get_meter()

#: The fraction of ``heartbeat_interval`` a FAILED tick waits before its
#: prompt retry. A failed tick is not a beat - the one-beat-per-interval
#: cadence promise counts beats, and a tick that raised stamped nothing -
#: so the failed tick must not also consume the full inter-beat sleep:
#: that made the gap between good beats after one transient blip twice
#: the interval, exactly the ``heartbeat_timeout >= 2x interval`` floor
#: the ops guidance calls safe, and a sweep running in the
#: deadline-crossing window reclaimed a live, lease-valid worker. The
#: retry waits a quarter interval: prompt enough that the recovery beat
#: lands at ``<= (1 + this) * interval`` (comfortably inside the 2x
#: floor), bounded so repeated failures never spin (each failure pays
#: the backoff again, and the tick's own pool acquire is bounded at the
#: interval), and small enough that the failed-cycle gap stays under the
#: ``interval + heartbeat_command_timeout`` worst-beat-cycle the lease
#: arithmetic sizes against (see _lease_renewal_threshold - the bound is
#: unchanged by this pacing, every failed cycle only gets shorter).
_FAILED_TICK_RETRY_FRACTION = 0.25


def _lease_renewal_threshold(
    lock_lease: timedelta,
    heartbeat_interval: float,
    max_heartbeat_failures: int,
    heartbeat_command_timeout: float,
) -> timedelta:
    """The remaining-lease floor below which the heartbeat renews a row.

    Renewing every held row on every beat rewrites ``lock_expires_at`` ,
    the key of ``jobs_running_lock_expires_idx``, so every beat is a
    non-HOT update per running row (new index entries in every index a
    running row satisfies, fleet-wide, forever). Renewing only
    rows whose remaining lease is at or under this threshold spaces the
    rewrites out instead of paying them every beat.

    Sizing, measured against the ENFORCED failed-tick bound, not a
    hoped-for one. The heartbeat's tick runs its whole command sequence
    (BEGIN, the three writes, the still-held probe, the cancel hook's
    statements, COMMIT) under ONE ``asyncio.timeout(
    heartbeat_command_timeout)`` budget, and its teardown - the
    rollback AND the bounded close - SHARES that one budget's remainder
    (the close's bound is recomputed from the deadline at its own
    instant, never a second full budget). A failed tick is therefore
    bounded by, with every term enforced:

    * the pool acquire: at most ``heartbeat_interval`` (its own timeout;
      an acquire that takes the whole timeout fails the tick with no
      commands and no teardown at all);
    * the command sequence PLUS its teardown: at most one
      ``heartbeat_command_timeout`` (the budget, shared).

    so a failed tick costs at most ``acquire + budget`` and the worst
    failed cycle (tick start to tick start) is
    ``heartbeat_interval + heartbeat_command_timeout``. The cascade to
    the isolate decision adds ONE more span the failed cycles do not
    cover: the LAST good beat's TAIL, from its renewal point (mid-tick,
    where the gate re-stamped the lease) to the next tick's start -
    bounded by ``max(heartbeat_interval, heartbeat_command_timeout)``
    (the cadence when the last beat was cheap, the budget's remainder
    when the acquire ate most of it). The floor is that tail plus
    ``(F+1)`` failed cycles, the loop isolates on the (F+1)-th
    consecutive failure, and the lease must still be valid at that
    decision.

    Fix-round premise correction: the round-1 derivation assumed a
    failed tick was bounded by "acquire-block then a timed-out command"
    (interval + ONE command timeout). That premise was false, the tick
    issues >= 3 commands each separately bounded by the pool's
    per-query command timeout, and the transaction's teardown adds its
    own round trip, so a brownout tick (a contended acquire, then two
    just-under-timeout statements, then a timeout) lasted acquire +
    ~3 command-timeouts, which the round-1 floor of (F+1) * (interval +
    command_timeout) did not cover: a legal skip at 49.5s of a 60s lease
    followed by four such failed ticks let the lease expire 3.2-9.2s
    BEFORE the isolate decision while the unconditional renewal
    survived. The per-tick command budget above makes the bound true by
    enforcement, and this floor sizes against it.
    * Round 2 (the integration attack round) measured the ENFORCED
      cascade with worst-case ticks - a stalled-but-successful acquire,
      a budget-cut sequence, a teardown close stalling to its bound -
      and found the observed cascade running PAST the validator's own
      floor: the accounting counted (F+1) failed cycles from the last
      renewal but not the last good beat's tail, and the pre-fix
      teardown's close burned a SECOND full command budget after the
      rollback had consumed the remainder (observed: a cascade of
      2.758s against a 2.4s floor at F=2, I=0.5, c=0.15). The
      shared-remainder teardown makes the per-tick cost
      ``acquire + ONE budget`` by enforcement, and the tail term in the
      floor below makes the accounting cover the measured span.

    Consequences, stated directly:

    * Healthy beats: a skip happens only while remaining > threshold, so
      the next beat's remaining is > ``threshold - worst_beat_gap``; at
      the floor that is ``F * worst_beat_gap + tail`` (>= one full worst
      gap for F >= 1), a healthy-but-slow worker never lets a lease
      lapse.
    * The failure cascade: the worst case is a skip at remaining
      ``threshold + eps`` followed by the last good beat's tail and
      ``F+1`` failed beats, each cycle STRICTLY under one worst gap
      (an acquire that consumed its whole timeout fails with no
      commands and no teardown, a cycle of exactly the interval; a tick
      that ran commands consumed strictly less than its whole acquire
      allowance). The lease at the isolate decision is then > 0 by
      construction, and a worker that recovers after F failures still
      holds a full worst gap of lease and renews it.
    * At the DEFAULT settings the floor is
      ``10 + 4 * (10 + 2) = 58s`` against a 60s lease: the gate renews
      every beat, byte-identical cadence to the unconditional renewal
      (a beat consumes the 10s interval, the lease-threshold slack is
      2s, so no row is ever far enough above the threshold to skip),
      and the enforced budget independently makes the default-config
      cascade survivable with margin (the tail 10s + 4 cycles x 12s =
      58s against the 60s lease), which the un-enforced per-statement
      bound (statement-count-dependent, up to ``interval + (k+1) *
      command_timeout`` per tick) is not.
    * The gate saves again from ``lock_lease`` ≈ 70s upward (2x at 70,
      4x at 90, and the ``lock_lease / 2`` arm dominates from ≈ 112,
      giving ~6x at 120+). Raising ``heartbeat_command_timeout`` (for a
      loaded or cross-region Postgres whose beat needs more than one
      command-timeout in total) raises the floor with it, the gate
      harvests only slack that actually exists.
    * Whenever the floor meets or exceeds the lease itself, the gate
      renews every row on every beat, zero savings, exactly the
      unconditional behaviour, because there is no slack that is safe to
      harvest.

    The comparison itself is server-side (``lock_expires_at <=
    clock_timestamp() + $4`` in the gated statement), so worker-clock
    skew cannot move the threshold: the same clock that stamped the
    lease judges it.

    Pacing correction (the phantom-cancel round): a FAILED tick is not a
    beat. The loop no longer sleeps the full remaining interval after a
    tick that raised - it retries promptly, after
    ``min(remaining, _FAILED_TICK_RETRY_FRACTION * interval)`` - so the
    worst-case arithmetic above has to be re-derived against the new
    pacing. A failed cycle's gap is ``duration + min(max(0, interval -
    duration), retry_backoff)``, which is bounded by
    ``max(interval, duration)``: a fast failed tick (the common
    transient shape - a refused connection, an immediately-raised
    acquire) now gaps at ``duration + retry_backoff`` (< the interval),
    a failed tick that consumed its whole acquire allowance gaps at
    exactly the interval, and a tick that ran past the interval gaps at
    its own duration. The worst failed cycle is unchanged in SHAPE - a
    failed tick still costs at most ``acquire + budget`` = up to
    ``heartbeat_interval + heartbeat_command_timeout``, and the failed
    cycle is bounded by ``max(interval, duration)`` - so the
    floor formula and every number above stay true; the prompt retry
    only ever SHORTENS failed cycles, and every bound here is an
    upper bound on the cascade's wall clock. What the retry buys is
    the recovery side of the ledger: after ONE transient blip the next
    GOOD beat lands within ``(1 + _FAILED_TICK_RETRY_FRACTION) *
    interval`` of the last one instead of at twice the interval, which
    is what makes the ops guidance's ``heartbeat_timeout >= 2x
    interval`` sizing actually tolerate the blip it exists to absorb
    (see the wait block at the bottom of ``heartbeat_loop``).
    """
    worst_beat_gap = heartbeat_interval + heartbeat_command_timeout
    # The last good beat's tail: from its renewal point (mid-tick) to
    # the next tick's start - the cadence when the last beat was cheap,
    # the budget's remainder when the acquire ate most of it.
    last_beat_tail = max(heartbeat_interval, heartbeat_command_timeout)
    safety_floor = timedelta(
        seconds=last_beat_tail + (max_heartbeat_failures + 1) * worst_beat_gap,
    )
    return max(safety_floor, lock_lease / 2)


# Which disowned ids still name a running row locked to this worker: the
# rest have been reclaimed (re-pended, or claimed by another worker) and
# leave the set. Only issued on a tick whose set is non-empty.
_SELECT_STILL_HELD_SQL_TEMPLATE = (
    'SELECT id FROM "{schema}".jobs '
    "WHERE id = ANY($1::uuid[]) AND locked_by_worker = $2 AND status = 'running'"
)
# The claim-loss reconcile: rows this worker CLAIMED (running, locked
# here, ``started_at`` stamped by the claim CTE) that no in-memory
# structure holds - no registry entry, no claim intent, never queued,
# never disowned. Exactly one producer-side event loses the ids: a
# ``dispatch_batch`` whose commit landed server-side but whose response
# died with the connection (the transient arm logs
# ``dispatch-batch-transient`` and moves on). Unreconciled, the renewal
# below keeps such a row's lease alive for as long as the process
# lives - the lock-lease-expiry backstop can never fire and the job is
# lost (the grand-mixin soak's settle-timeout signature: running rows
# the heartbeat itself extends). The probe disowns what it finds: the
# renewal stops (the shared exclusion core), the lease lapses within
# one lease of the disown, and Sweep 1 reclaims the row with its own
# attempt rows, reclaim events and budget predicates - the recovery
# every "recovers by lock-lease expiry" docstring already promises.
#
# The disown REFUNDS the claim-time increment. Dispatch stamps
# ``attempt = j.attempt + 1`` at claim (backend/_dispatch_sql.py), before
# any actor sees the job, and this reconcile's rows are exactly the ones
# the increment was charged for and no execution ever covered: no
# registry entry means no actor, no intent means no take, never queued
# means never handed out. Without the refund, the disowned row carries
# the charged attempt into Sweep 1's budget predicate, and a
# ``max_attempts=1`` job (or a ``non_retryable`` one at its budget edge)
# terminalises 'crashed' there having never executed, with a crashed
# ``job_attempts`` row asserting the execution and attributing it to a
# worker that never held a running actor (issue 458's measured record).
# The refund is the shutdown drain's rule, applied at the same
# discriminator: a claim that never reached an actor bought nothing, so
# it spends nothing (worker/shutdown.py's ``drain_local_queue_to_pending``,
# the third sink for a lost grip, refunds through the same
# ``ATTEMPT_REFUND_SQL`` fragment this statement reuses - one fragment,
# the alias-qualified spelling, for every never-started hand-back).
#
# The row itself stays ``running`` and locked: unlike the drain, which
# re-pends and hands the row to the fleet directly, the reconcile's tick
# has already run its renewal, so the disowned row keeps the lease it
# holds until it lapses and Sweep 1 owns the reclaim with its own
# attempt rows, reclaim events and budget predicates (issue 418's
# recovery contract, preserved).
#
# The refund is exactly-once per claim. Within a process the tick's
# exclusion array folds the disowned set, so the refunded row stops
# matching. Across a worker restart ``disowned_jobs`` is empty and the
# row still satisfies this predicate, so the statement un-stamps
# ``started_at`` as it refunds: the claim's stamp named an execution that
# never started, and with it gone the ``started_at <`` bound below stops
# matching the row whatever id re-probes it. NULL is the honest value,
# the same "never started" the attempt-ledger arms already exclude (the
# sweep's attempt INSERT coalesces a NULL stamp through the per-row clock
# fallback, and the next claim stamps it fresh), so no reader learns a
# new shape - only the fabrication goes away.
#
# THE CANCEL EXCLUSION (``cancel_phase = 0``): a row carrying an operator
# cancel is NEVER this statement's to refund. The flag is stamped only on
# a ``status = 'running'`` row (cancel_running's guard), so a flagged row's
# claim DID reach a holder, the premise "no registry entry means no actor
# ever ran this claim" is structurally false for it, and the body may have
# run to completion and exited through the cancel fence (mark_retry's
# phase-carrying rows match no arm) before this probe ever sees the row -
# its ``started_at`` age is then the BODY's duration, not the age of the
# unheld state, so no grace arithmetic between this probe and the ladder's
# abandon can order them (the fence signature's own comment in
# worker/cancel.py assumed the abandon lands inside one lease of the
# sighting; a body that outlived the lease makes the age test true on the
# first tick after the exit). The flagged row's writers are the cancel
# ladder's unheld walk while this worker lives (the poll returns every
# flagged row it locks, entry or not) and Sweep 1's cancel arm when it
# dies (its carve-out terminalises the row 'cancelled' with the operator's
# audit intact); the refund here would erase the executed attempt's charge
# and the abandon's ledger INSERT would then collide with the genuine
# earlier attempt's row, leaving the attempt whose body ran with no
# ``job_attempts`` row anywhere. The same fence every other never-started
# hand-back carries (the shutdown drain's ``cancel_phase = 0``, the
# deferral arms, mark_interrupted's release) applies here.
_RECONCILE_LOST_CLAIMS_SQL_TEMPLATE = (
    'UPDATE "{schema}".jobs j SET '  # noqa: S608  # Why: schema validated against _IDENT_RE before interpolation; asyncpg has no parameter binding for identifiers (the still-held template's same shape).
    f"attempt = {ATTEMPT_REFUND_SQL}, started_at = NULL "
    "WHERE j.locked_by_worker = $1 AND j.status = 'running' "
    "AND j.cancel_phase = 0 "
    "AND NOT (j.id = ANY($2::uuid[])) "
    "AND j.started_at < clock_timestamp() - $3::interval "
    "RETURNING j.id"
)
_tick_duration = _meter.create_histogram(
    name="taskq.heartbeat.tick_duration_seconds",
    unit="s",
    description="Wall-clock seconds for one heartbeat tick.",
)


async def _failed_tick_ledger(
    deps: WorkerDeps,
    worker_id: UUID,
    shutdown: asyncio.Event,
    exc: BaseException,
) -> bool:
    """Record a failed heartbeat tick and decide whether the worker isolates.

    BOTH failure arms of the tick funnel here, the transient arm (a PG
    blip: connection loss, 40001, a timeout) and the unexpected arm
    (anything else). The ledger is deliberately ONE threshold, not two: a
    persistent non-transient fault (a REVOKE'd UPDATE, a driver contract
    violation) fails every tick exactly as a dead PG does, and the
    pre-fix asymmetric ledger (the transient arm counted, the unexpected
    arm only logged) let such a fault loop forever on a worker that
    looked perfectly healthy: ``heartbeat_failures`` pinned at 0, the
    consecutive-failures gauge at 0, ``record_heartbeat_miss`` never
    called, /ready green with no reasons, while the lock expired
    underneath every running job and the reclaim sweep handed them back.
    Both arms now share ``max_heartbeat_failures``, the
    consecutive-failures gauge, the miss counter, the halfway early
    warning, and the isolate decision; only a fully successful tick
    resets the count (the loop body's own reset, the same
    reset-on-success contract :class:`UnexpectedLoopErrorGuard` enforces
    for the leader loops).

    Why not wrap this loop in :class:`UnexpectedLoopErrorGuard` as the
    leader loops are: the guard's contract is deliberately fatal, it
    re-raises into the worker TaskGroup and leaves recovery to the
    lease-expiry sweeps. The heartbeat's failure doctrine is older and
    stronger than the guard's, and it is the right one for THIS loop:
    ``isolate_self`` proactively transitions the running rows (pending /
    crashed / cancelled, with ``HeartbeatLost`` attempt rows) on a fresh
    direct connection BEFORE signalling shutdown, because the loop that
    keeps every lease alive is the one component that must never die by
    exception. The lease arithmetic also binds to this one threshold:
    :func:`_lease_renewal_threshold` sizes the renewal gate against
    ``max_heartbeat_failures + 1`` failed beats, so a second, independent
    budget for the unexpected arm would desynchronise the isolate
    decision from the very floor the ``lock_lease`` invariant enforces.

    The in-tx cancel-hook failure keeps its carve-out: it increments
    ``deps.heartbeat_failures`` itself, then re-raises as ``OSError``,
    which the transient arm classifies. The transient call site guards its
    own increment on the ``_in_tx_failed`` flag - when the in-tx hook
    already counted this tick, the arm skips the second increment so the
    tick counts exactly once - and then funnels into this ledger either
    way, so the miss record, the early warning, and the isolate decision
    run for every failed tick.

    Returns True when the threshold tripped and isolate_self ran; the
    caller exits the loop.
    """
    record_heartbeat_miss(str(worker_id))
    early_warn_threshold = deps.settings.max_heartbeat_failures // 2
    if early_warn_threshold > 0 and deps.heartbeat_failures == early_warn_threshold:
        logger.warning(
            "heartbeat-failures-approaching-limit",
            worker_id=str(worker_id),
            consecutive_failures=deps.heartbeat_failures,
            max_heartbeat_failures=deps.settings.max_heartbeat_failures,
            error_class=type(exc).__name__,
        )
    if deps.heartbeat_failures > deps.settings.max_heartbeat_failures:
        await isolate_self(deps, worker_id, shutdown)
        return True
    return False


async def heartbeat_loop(
    deps: WorkerDeps,
    worker_id: UUID,
    shutdown: asyncio.Event,
    *,
    cancel_controller: CancelController | None = None,
    cancel_wake_event: asyncio.Event | None = None,
) -> None:
    interval = deps.settings.heartbeat_interval
    lock_lease = timedelta(seconds=deps.settings.lock_lease)
    schema = deps.settings.schema_name
    # The tick's single command budget: ONE
    # heartbeat_command_timeout for the tick's whole command sequence
    # (BEGIN + writes + probes + hook + COMMIT, and the post-tx drain's
    # share). See the tick block below for why the sequence, not each
    # statement, is the unit the lease model needs bounded.
    tick_command_budget = deps.settings.heartbeat_command_timeout
    # The renewal threshold: a healthy beat only rewrites leases
    # whose remaining time is at or under this. See
    # _lease_renewal_threshold for the sizing derivation (against the
    # tick budget's enforced bound) and the failure-cascade margin
    # proof.
    renewal_threshold = _lease_renewal_threshold(
        lock_lease,
        interval,
        deps.settings.max_heartbeat_failures,
        deps.settings.heartbeat_command_timeout,
    )
    (
        update_worker_liveness_sql,
        update_jobs_lock_sql,
        update_reservation_leases_sql,
    ) = build_heartbeat_sql(schema, renewal_threshold=renewal_threshold)
    select_still_held_sql = _SELECT_STILL_HELD_SQL_TEMPLATE.format(schema=schema)
    refund_lost_claims_sql = _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE.format(schema=schema)

    # Monotonic stamp of the last jobs-lock renewal that landed: the
    # reference the next tick measures its remaining lease against. None
    # until the first renewal, there is nothing to measure before it.
    last_renewal_at: float | None = None
    while not shutdown.is_set():
        deps.liveness.tick("heartbeat", period=interval)
        _in_tx_failed = False
        _tick_failed = False
        tick_start = time.monotonic()
        try:
            _tick_raised = False
            # The tick's single command budget: None
            # until the acquire lands -- the acquire is bounded by its
            # own timeout (the interval) OUTSIDE this budget -- then the
            # deadline every later phase of the tick shares.
            budget_deadline: float | None = None
            try:
                async with deps.heartbeat_pool.acquire(timeout=interval) as conn:
                    budget_deadline = time.monotonic() + tick_command_budget
                    tx = conn.transaction()
                    try:
                        # ONE command-timeout for the tick's whole command
                        # sequence: BEGIN, the liveness write, the gated
                        # renewal, the reservation-lease write, the
                        # still-held probe, the lost-claim reconcile
                        # probe, the cancel hook's statements,
                        # COMMIT. Each statement's own pool-level
                        # command_timeout stays as the inner backstop; the
                        # budget is what bounds the SEQUENCE, because a
                        # failed tick's cost to the lease model is the
                        # whole tick, not any one statement: a brownout
                        # tick whose first two statements each take just
                        # under one timeout and whose third times out
                        # lasts acquire + ~3 command-timeouts when only
                        # the per-statement bounds apply, which is the
                        # gap bound the round-1 threshold arithmetic
                        # assumed away (the reproduced default-settings
                        # lease-lapse window). A tick whose statements
                        # legitimately need more than one command-timeout
                        # IN TOTAL now fails fast here instead: raise
                        # TASKQ_HEARTBEAT_COMMAND_TIMEOUT for that regime
                        # (the renewal threshold's floor absorbs the
                        # raised value automatically -- see
                        # _lease_renewal_threshold).
                        async with asyncio.timeout(tick_command_budget):
                            await tx.start()
                            # Snapshot: consumers add to the set while this tick
                            # awaits, and an id that arrives mid-tick belongs to
                            # the next tick's exclusion, the prune below must
                            # not read it as "still held" and drop it.
                            disowned = list(deps.disowned_jobs)
                            # The stall-attribution tally rides the liveness write:
                            # one dict merge in the statement the tick already
                            # issues, no extra round trip. An empty tally merges a
                            # no-op, so a quiet process leaves the registered
                            # metadata keys untouched.
                            await conn.execute(
                                update_worker_liveness_sql,
                                worker_id,
                                jsonb_param(deps.stall_tally.metadata_value()),
                            )
                            renewal_at = time.monotonic()
                            # The gated renewal: binds the threshold as
                            # $4. Rows with a per-job heartbeat_timeout, rows
                            # with no lease stamp, and rows at/under the
                            # threshold renew; fresh leases are left alone so a
                            # healthy beat stops paying a non-HOT update per
                            # running row per tick. parse_rowcount on the tag
                            # now counts rows RENEWED this tick, not rows held.
                            jobs_tag = await conn.execute(
                                update_jobs_lock_sql,
                                worker_id,
                                lock_lease,
                                disowned,
                                renewal_threshold,
                            )
                            await conn.execute(
                                update_reservation_leases_sql,
                                worker_id,
                                lock_lease,
                                disowned,
                            )
                            if disowned:
                                held_rows = await conn.fetch(
                                    select_still_held_sql, disowned, worker_id
                                )
                                still_held = {row["id"] for row in held_rows}
                                deps.disowned_jobs.difference_update(set(disowned) - still_held)
                            # The claim-loss reconcile: running rows locked
                            # here that nothing holds. The exclusion binds
                            # the THREE maps in one array - registered +
                            # intents (held_ids), queued (parked in the
                            # local queue), and this tick's disowned
                            # snapshot - and the lease-length grace on
                            # started_at spares every in-flight handoff
                            # (see the template's comment). Found ids are
                            # refunded (the claim-time increment of a
                            # never-started execution goes back) and
                            # disowned: this tick's renewal has already
                            # run, the NEXT tick stops renewing them, and
                            # Sweep 1 owns the reclaim from there.
                            lost_excluded = sorted(
                                {
                                    *deps.active_jobs.held_ids(),
                                    *deps.active_jobs.queued_ids(),
                                    *disowned,
                                }
                            )
                            lost_rows = await conn.fetch(
                                refund_lost_claims_sql, worker_id, lost_excluded, lock_lease
                            )
                            if lost_rows:
                                lost_ids: list[UUID] = [row["id"] for row in lost_rows]
                                deps.disowned_jobs.update(lost_ids)
                                logger.warning(
                                    "claim-loss-reconciled",
                                    kind="claim_loss_reconciled",
                                    worker_id=str(worker_id),
                                    job_ids=[str(j) for j in lost_ids],
                                )
                            if cancel_controller is not None:
                                try:
                                    await cancel_controller.run_in_tx(conn)  # type: ignore[arg-type]  # Why: asyncpg PoolConnectionProxy is a Connection subclass at runtime; pyright types don't reflect this delegation.
                                except Exception as hook_exc:
                                    _in_tx_failed = True
                                    deps.heartbeat_failures += 1
                                    update_heartbeat_consecutive_failures(
                                        str(worker_id), deps.heartbeat_failures
                                    )
                                    logger.warning(
                                        "heartbeat-hook-failure",
                                        kind="state_change",
                                        cause="heartbeat_hook_failure",
                                        worker_id=str(worker_id),
                                        error=repr(hook_exc),
                                    )
                                    raise OSError(
                                        f"cancel_controller.run_in_tx failed: {hook_exc!r}"
                                    ) from hook_exc
                            # The commit rides the SAME budget: a commit
                            # that would push the sequence past the
                            # command-timeout is cut, the tick fails
                            # without the commit having been confirmed,
                            # and the teardown below closes the
                            # connection (the writes are idempotent
                            # re-stamps, so an unconfirmed commit is
                            # benign: the next tick rewrites them).
                            await tx.commit()
                    except BaseException:
                        # Teardown, bounded by the SAME budget's
                        # remainder, deliberately OUTSIDE the expired
                        # asyncio.timeout scope: an await started after
                        # the scope expired is NOT re-cancelled (measured
                        # , asyncio.timeout fires once), so the teardown
                        # must carry its own bound rather than hide
                        # inside the dead scope. The ROLLBACK and the
                        # CLOSE SHARE that one remainder - the close's
                        # bound is recomputed from the deadline at its
                        # own instant, so a rollback that consumed the
                        # remainder leaves the close an immediate
                        # terminate, never a second full command budget.
                        # This is what holds the failed tick's enforced
                        # cost to acquire + ONE budget (see
                        # _lease_renewal_threshold): the pre-fix close
                        # took a FULL second budget after the rollback
                        # had already consumed the remainder, and a
                        # cascade of such ticks ran
                        # (F+1) * (interval + 2 * command_timeout) past
                        # the last renewal PLUS the last good beat's
                        # tail - measurably past the very cascade floor
                        # the settings validator enforces (the
                        # attack test pins the observed overrun).
                        left = budget_deadline - time.monotonic()
                        rolled_back = False
                        if left > 0:
                            try:
                                async with asyncio.timeout(left):
                                    await tx.rollback()
                                rolled_back = True
                            except TimeoutError:
                                # The rollback did not fit what the tick
                                # had left; fall through to the close.
                                pass
                        if not rolled_back:
                            # Close instead of awaiting a rollback round
                            # trip: the server rolls the transaction back
                            # on disconnect, the close is a local
                            # Terminate write (bounded, terminate() on
                            # timeout, close_conn_bounded never raises),
                            # and the pool discards the closed connection
                            # on release. Bounded by what the budget
                            # still has left (shared with the rollback
                            # above, NOT a second budget): this is the
                            # budget-exhausted path, so it is also the
                            # path that keeps the failed tick inside the
                            # lease model's h + c bound.
                            await close_conn_bounded(
                                conn,  # type: ignore[arg-type]  # Why: PoolConnectionProxy delegates close()/terminate() to the underlying Connection at runtime; pyright's stubs model the proxy as unrelated, the same delegation the run_in_tx call below relies on.
                                "heartbeat-tick-budget",
                                max(0.0, budget_deadline - time.monotonic()),
                                mid_run=True,
                            )
                        # Re-raise the tick's ORIGINAL exception (a bare
                        # raise re-raises it identically): a teardown
                        # timeout or the close's bounds must never
                        # displace what actually failed the tick, least
                        # of all an external cancellation, which must
                        # stay a cancellation.
                        raise
            except BaseException:
                _tick_raised = True
                raise
            finally:
                # Why finally: run_post_tx MUST follow run_in_tx on every tick
                # (see taskq.worker.cancel's module docstring).  Phase-3 jobs
                # queued before an error would otherwise never be drained, and
                # the drain is also where an abandon that could not apply is
                # handed back for a later tick.  Row locks are gone either way
                # by the time this runs, the transaction has committed or
                # rolled back, so mark_abandoned cannot self-deadlock here.
                if cancel_controller is not None:
                    # post_tx shares the tick's ONE command budget: it
                    # runs under whatever the deadline has left, and is
                    # deferred to the next tick when nothing is left (an
                    # expired asyncio.timeout does not re-cancel an
                    # await that starts after expiry, measured, so the
                    # remaining time is enforced with a fresh scope).
                    # The controller's deque persists, so a deferred
                    # drain loses nothing. When the acquire itself
                    # failed, no budget was consumed: post_tx drains the
                    # PREVIOUS ticks' queue under a full budget of its
                    # own (the acquire's own timeout already bounds that
                    # tick).
                    left = (
                        budget_deadline - time.monotonic()
                        if budget_deadline is not None
                        else tick_command_budget
                    )
                    if left <= 0:
                        logger.debug(
                            "heartbeat-post-tx-deferred",
                            worker_id=str(worker_id),
                            reason="tick_command_budget_exhausted",
                        )
                    else:
                        try:
                            async with asyncio.timeout(left):
                                await cancel_controller.run_post_tx()
                        except Exception as post_exc:
                            # A post-tx failure on a healthy tick is the tick's
                            # failure and propagates to the handlers below; on an
                            # already-failing tick it must NOT displace the
                            # original error, which is the root cause worth
                            # reporting. (A mid-drain budget cut lands here as
                            # TimeoutError: the in-flight abandon is shielded
                            # from the tick and re-queued by the drain itself,
                            # so the deferred work rides the next tick.)
                            if not _tick_raised:
                                raise
                            logger.warning(
                                "heartbeat-post-tx-failure",
                                worker_id=str(worker_id),
                                error=repr(post_exc),
                            )
            deps.heartbeat_failures = 0
            update_heartbeat_consecutive_failures(str(worker_id), 0)
            tick_duration_s = time.monotonic() - tick_start
            _tick_duration.record(tick_duration_s)
            # The lease the previous beat's UPDATE stamped had this much
            # left when this one landed: lease minus the gap between the
            # two UPDATEs (both measured at the same point, so network
            # latency cancels). A failed or late tick widens the gap and
            # lowers the sample, which is the signal the lock-expiry
            # alert reads; the config constant would never move. Clamped
            # at 0: a renewal that lands after expiry renews an
            # already-expired lease.
            # Under threshold-gated renewal the sample keeps this
            # beat-cadence meaning deliberately: it is stamped on every
            # successful tick, whether or not the gate renewed any rows,
            # so a late or failing beat lowers it exactly as before and
            # alert thresholds calibrated to the old per-tick cadence
            # keep their semantics. For rows the gate skipped (still
            # above the threshold) the true remaining is anywhere up to
            # the full lease, the sample is the "if this beat renewed
            # everything" floor, never an overstatement of a RENEWED
            # row's remaining, and the threshold's own sizing is pinned
            # by the _lease_renewal_threshold unit and property tests
            # rather than by this histogram.
            if last_renewal_at is not None:
                record_lock_expires_in_seconds(
                    str(worker_id),
                    max(0.0, lock_lease.total_seconds() - (renewal_at - last_renewal_at)),
                )
            last_renewal_at = renewal_at
            logger.debug(
                "heartbeat-tick-success",
                worker_id=str(worker_id),
                tick_duration_ms=int(tick_duration_s * 1000),
                jobs_extended=parse_rowcount(jobs_tag),
                is_leader=deps.is_leader.is_set(),
            )
        except TRANSIENT_PG_ERRORS as e:
            tick_duration_s = time.monotonic() - tick_start
            _tick_duration.record(tick_duration_s)
            _tick_failed = True
            if not _in_tx_failed:
                deps.heartbeat_failures += 1
                update_heartbeat_consecutive_failures(str(worker_id), deps.heartbeat_failures)
            logger.warning(
                "heartbeat-tick-failure",
                worker_id=str(worker_id),
                consecutive_failures=deps.heartbeat_failures,
                error_class=type(e).__name__,
                error=str(e),
            )
            if await _failed_tick_ledger(deps, worker_id, shutdown, e):
                return
        except Exception as e:
            # The unexpected arm counts toward the SAME isolate threshold
            # as the transient arm (see _failed_tick_ledger): a persistent
            # non-transient error must isolate the worker, not loop
            # forever as a functional zombie whose counter never moves.
            tick_duration_s = time.monotonic() - tick_start
            _tick_duration.record(tick_duration_s)
            _tick_failed = True
            deps.heartbeat_failures += 1
            update_heartbeat_consecutive_failures(str(worker_id), deps.heartbeat_failures)
            logger.exception(
                "heartbeat-tick-unexpected-error",
                worker_id=str(worker_id),
                consecutive_failures=deps.heartbeat_failures,
                error_class=type(e).__name__,
            )
            if await _failed_tick_ledger(deps, worker_id, shutdown, e):
                return
        # The wait is anchored to the tick's START, not its end, so the
        # beat cadence is the interval however long a SUCCESSFUL tick
        # took. A fixed post-tick sleep instead makes the cadence
        # tick_duration + interval, and a tick may legitimately run for
        # nearly a whole interval, the pool acquire above is bounded at
        # exactly that. A tick that overruns the interval waits zero and
        # re-enters immediately, which is the correct urgency, it is
        # already late, and cannot become a hot loop, because the next
        # tick's own pool acquire is bounded at the interval and paces it.
        #
        # A FAILED tick is NOT a beat, so it does not get the inter-beat
        # wait either. Sleeping the full remaining interval after a tick
        # that raised made the gap between good beats twice the interval
        # after ONE transient blip (the failed tick stamped nothing, then
        # the loop slept a whole interval anyway): the recovery beat
        # landed at the ``heartbeat_timeout >= 2x interval`` floor the
        # ops guidance calls safe rather than inside it, and a sweep
        # running in the deadline-crossing window reclaimed a live,
        # lease-valid worker's job - a phantom cancel, a duplicate
        # execution of work that was never lost. So a failed tick
        # retries promptly: it waits at most a quarter interval (bounded
        # by what the cadence still owes, so an overrun tick still waits
        # zero), then re-enters. The next tick after a successful
        # recovery beat resumes the normal anchored cadence. The retry
        # cannot hammer: each failure pays the bounded backoff again,
        # and the per-tick pool acquire (bounded at the interval) paces
        # every tick that does run. The ledger counts the blip exactly
        # once - the failed tick incremented the counter, its prompt
        # retry succeeds and resets it - so one transient failure never
        # consumes the isolate budget, and repeated failures still
        # isolate on the (max_heartbeat_failures + 1)-th consecutive
        # one (each failed cycle is now SHORTER, which only moves that
        # decision earlier inside the lease the cascade floor sizes -
        # see _lease_renewal_threshold).
        remaining = max(0.0, interval - (time.monotonic() - tick_start))
        if _tick_failed:
            remaining = min(remaining, _FAILED_TICK_RETRY_FRACTION * interval)
        if cancel_wake_event is not None:
            # Wait out the remainder, but wake immediately on a cancel NOTIFY.
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(cancel_wake_event.wait(), timeout=remaining)
            cancel_wake_event.clear()
        else:
            await asyncio.sleep(remaining)


_SELECT_RUNNING_JOBS_SQL_TEMPLATE = (
    "SELECT id, attempt, started_at, max_attempts, retry_kind, cancel_phase "
    'FROM "{schema}".jobs '
    "WHERE locked_by_worker = $1 AND status = 'running'"
    " AND id <> ALL($2::uuid[])"
)

# Recovery transitions via isolate_self: running→cancelled when a
# cancel is in-flight (any retry budget); running→pending when no
# cancel is in-flight and retries remain; running→crashed otherwise.
# All are present in VALID_TRANSITIONS.
# The heartbeat-pool failure forces a fresh asyncpg
# connection, so the worker cannot rely on its in-memory status being
# current.  The SQL self-guards via WHERE status='running' AND
# locked_by_worker=$2, which atomically serialises the read+write and
# ensures only rows still belonging to this worker transition.  Note:
# error_class='HeartbeatLost' is intentionally distinct from Sweep 1's
# 'WorkerCrashed', a heartbeat-lost worker may still be alive but
# partitioned, while Sweep 1 assumes the worker is gone.

# Branch-for-branch mirror of _sweeps.py's _SWEEP_1_SQL SET clause,
# sharing its budget predicate and hand-back delay verbatim, the
# property test tests/test_leader_property.py asserts row-state
# equivalence between this path and the sweep, so any branch change
# there (the operator-intent-first CASE ordering: cancel_phase != 0
# terminalises 'cancelled' ahead of the budget arm, cancel-column
# preservation on the cancel arm, clock_timestamp() for terminal
# timestamps) must be mirrored here. The mirror covers the SET
# clause's SHAPE; the crashed arm's error_class/error_message VALUES
# are deliberately distinct: 'HeartbeatLost' plus this module's own
# message, where the sweep stamps 'WorkerCrashed' plus the
# deadline-naming message from _ATTEMPT_MESSAGES, for the same
# reason the attempt rows differ: the sweep's reclaim means the
# LEADER declared the holder dead, isolate means the worker itself
# declared PG unreachable and is walking away. The job row must
# self-describe on the crashed arm either way (the
# pre-fix template left both fields NULL while claiming the mirror).
# Note the mirror covers the SET clause, NOT the
# selection predicate: the sweep leaves cancel-in-flight jobs alone until
# cancel_grace + cleanup_grace + 60s has passed (a merely-slow
# cancellation isn't pre-empted), while isolate applies the 'cancelled'
# arm immediately, deliberate asymmetry, since isolate means THIS worker
# is going away now and there is no lock-holder left to complete the
# cooperative protocol.
#
# Isolate WRITES the reclaim event row. Every reclaim rides the
# crash-reclaim outbox channel (kind='state_change', detail
# reason='lock_expired', the slice poll_reclaim_events tails and the
# event-retention carve-out keeps): without it, an isolate-reclaimed job
# is invisible to poll_reclaim_events and watch_reclaims, and a consumer
# fanning out on the feed counts the job outstanding forever while the
# row, the attempt history and the admin views all agree it is long
# gone. The event's cause key names the origin ('isolate_self', where
# the sweep's rows name 'lock_expired' / 'heartbeat_timeout', the
# deadline that fired), so a feed consumer can tell a worker's own
# heartbeat-loss walk-away from a leader-declared crash reclaim. The
# visibility-delay co-monotonicity motivation for clock_timestamp()
# (id and occurred_at stamped by the same INSERT) now applies to this
# path too, and is satisfied by the same batched event writer the sweep
# uses, microsecond ladder included.
#
# The re-pend arm wakes nobody: an UPDATE never fires the INSERT-only
# wake trigger and this worker is on its way out, so the fleet claims
# the handed-back row within the producer's poll floor
# (notify_poll_interval / poll_interval), the same wake source every
# release arm in backend/_sql_templates.py relies on.
#
#: The job-row error message the isolate's crashed arm stamps, the
#: isolate-path twin of ``_sweeps._ATTEMPT_MESSAGES``' deadline-naming
#: messages ("lock expired before worker reported terminal state" /
#: "heartbeat timeout passed before worker reported terminal state").
#: Isolate has no per-arm deadline to name (it is a whole-worker event,
#: not a row-level one), so the message names what actually fired: the
#: worker lost its heartbeat connection and never reported a terminal
#: state. One constant, not a map: the template renders it by name
#: (``str.replace``) so ``{schema}`` stays the only ``format``
#: placeholder the caller renders, the same discipline
#: ``_SWEEP_1_SQL``'s message fragments follow.
_ISOLATE_CRASHED_MESSAGE = "worker heartbeat connection lost before terminal state was reported"
#
#: The statement is built as ONE constant: the literal with the sweep's
#: shared fragments substituted by name (``str.replace``, not ``format``,
#: so ``{schema}`` stays the only placeholder the caller renders).
#: ``$3`` is the effective-cap ceiling (max_retry_backoff, seconds),
#: this statement's third parameter after the job id and worker id, the
#: sweep's shared delay fragment carries the placeholder as
#: ``{max_backoff_seconds}`` precisely so each statement binds the index
#: its own parameter layout assigns.
_ISOLATE_JOB_SQL_TEMPLATE = (
    """\
UPDATE "{schema}".jobs j
SET status = CASE
        -- Operator intent outranks retry budget, mirroring
        -- _SWEEP_1_SQL's CASE ordering verbatim: a cancel in flight
        -- terminalises 'cancelled' whatever the budget, so an
        -- operator's request cannot be laundered away by this
        -- worker's own departure. The re-pend arm therefore reads
        -- cancel_phase = 0 by construction.
        WHEN j.cancel_phase != 0
            THEN 'cancelled'::"{schema}".job_status
        WHEN {has_budget}
            THEN 'pending'::"{schema}".job_status
        ELSE 'crashed'::"{schema}".job_status
    END,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    -- Cancel columns survive the arm that honoured them (the
    -- mark_cancelled/mark_abandoned audit-trail doctrine); the
    -- re-pend and crashed arms read 0/NULL by construction, so the
    -- CASE arms are no-ops there, kept as defence-in-depth.
    cancel_phase = CASE WHEN j.cancel_phase != 0 THEN j.cancel_phase ELSE 0 END,
    cancel_requested_at = CASE
        WHEN j.cancel_phase != 0 THEN j.cancel_requested_at
        ELSE NULL END,
    -- An isolate re-pend hands the row back to the fleet, so it routes
    -- by the actor's current assignment from here on (the routing
    -- contract in taskq/backend/_dispatch_sql.py) -- the same SET this
    -- template mirrors branch-for-branch from _SWEEP_1_SQL. After the
    -- CASE reorder the re-pend arm alone reschedules.
    assignment_routed = true,
    scheduled_at = CASE
        WHEN j.cancel_phase = 0 AND {has_budget}
            THEN clock_timestamp() + {reclaim_delay}
        ELSE j.scheduled_at
    END,
    finished_at = CASE
        WHEN j.cancel_phase != 0 OR NOT ({has_budget})
            THEN clock_timestamp()
        ELSE j.finished_at
    END,
    -- The crashed arm self-describes on the row, mirroring the sweep's
    -- crashed arm in shape: 'HeartbeatLost' plus this module's own
    -- message, not the sweep's 'WorkerCrashed': the same distinction
    -- the attempt rows have always carried (documented above); the
    -- pre-fix template stamped nothing here while claiming the
    -- branch-for-branch mirror. The re-pend and cancelled arms keep
    -- their error fields: a handed-back row has no failure to
    -- describe, and a cancel-honouring row's record is the in-flight
    -- request the row preserves above.
    error_class = CASE
        WHEN j.cancel_phase = 0 AND NOT ({has_budget})
            THEN '{heartbeat_lost_class}'
        ELSE j.error_class
    END,
    error_message = CASE
        WHEN j.cancel_phase = 0 AND NOT ({has_budget})
            THEN '{isolate_crashed_message}'
        ELSE j.error_message
    END
WHERE j.id = $1 AND j.status = 'running' AND j.locked_by_worker = $2""".replace(
        "{has_budget}", _RECLAIM_HAS_BUDGET_SQL
    )
    .replace("{reclaim_delay}", _RECLAIM_DELAY_SQL)
    .replace("{max_backoff_seconds}", "$3")
    .replace("{isolate_crashed_message}", _ISOLATE_CRASHED_MESSAGE)
    .replace("{heartbeat_lost_class}", ERROR_CLASS_HEARTBEAT_LOST)
)


async def isolate_self(
    deps: WorkerDeps,
    worker_id: UUID,
    shutdown: asyncio.Event,
) -> None:
    assert deps.settings.pg_dsn_direct is not None
    pg_dsn = str(deps.settings.pg_dsn_direct)
    host = dsn_host(pg_dsn)
    schema = deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    # Stop the claim path FIRST, before any snapshot or join: this worker
    # is walking away, so the producer must start no new claim rounds, and
    # the producer's own exit pass (drain_local_queue_to_pending) hands
    # back what it claimed but never dispatched. The stop event also
    # closes the residual gap the late exclusion capture inside _inner
    # cannot close (a claim committed to the jobs table but not yet marked
    # in either map, one scheduler step wide): after it, a row a consumer
    # has taken is never dispatched (run.py's stop guard), so a re-pend of
    # such a row cannot double-run.
    deps.producer_stop_event.set()
    select_running_jobs_sql = _SELECT_RUNNING_JOBS_SQL_TEMPLATE.format(schema=schema)
    isolate_job_sql = _ISOLATE_JOB_SQL_TEMPLATE.format(schema=schema)
    insert_attempt_sql = INSERT_ATTEMPT_SQL.format(schema=schema)
    # The reclaim delay's effective ceiling, bound per statement, the
    # same operator knob the sweep binds as its own parameter, so a
    # heartbeat-lost hand-back lands on the same schedule the leader's
    # reclaim would have stamped.
    max_backoff_seconds = deps.settings.max_retry_backoff.total_seconds()
    jobs_pending_count = 0
    jobs_crashed_count = 0
    jobs_cancelled_count = 0
    # Rows whose guarded UPDATE no-oped, transitioned by the leader's
    # sweep between this path's SELECT and UPDATE; their attempt rows
    # belong to the winner, and the count keeps the complete log's
    # arithmetic explainable (selected rows = pending + crashed +
    # cancelled + lost_race).
    jobs_lost_race_count = 0
    # The re-pend's exclusion set is deliberately NOT captured here: the
    # pre-join snapshot this spot used to take re-pended every row claimed
    # during the join window below (the producer kept claiming for the
    # window's whole bound), handing back rows whose local handlers were
    # live. The capture lives inside _inner, after the join, immediately
    # before the SELECT that binds it.
    # Actors still executing in THIS process: their rows are excluded by
    # that late capture, and they take the same route the shutdown
    # orchestrator's
    # CANCELLING phase gives them: the SHUTDOWN origin stamp plus the cancel event
    # routes each consumer's unwinding through its mark_interrupted arm,
    # which earns the exit-proving hold and releases the row with the
    # spent attempt standing. Where PG is truly unreachable and the
    # interrupt writes fail, the rows stay running and lock-lease expiry
    # reclaims them, the honest residual.
    active_entries = deps.active_jobs.all()
    loop = asyncio.get_running_loop()
    for active in active_entries:
        active.ctx.cancel_event.set()
        if active.cancel_origin is CancelOrigin.NONE:
            active.cancel_origin = CancelOrigin.SHUTDOWN
            active.ctx._set_cancel_origin(CancelOrigin.SHUTDOWN)  # pyright: ignore[reportPrivateUsage]  # Why: the isolate path is the other designated shutdown-side writer of the context's origin stamp, same contract as the orchestrator's CANCELLING phase.
        if active.cancel_phase < CancelPhase.COOPERATIVE:
            active.cancel_phase = CancelPhase.COOPERATIVE
            active.cancel_observed_at = loop.time()
        if not active.task.done():
            active.task.cancel()
    if active_entries:
        # The interrupt writes run inside the consumers' own unwinding,
        # bounded by the terminal-write retry budget; the join here is the
        # outer bound (the grace periods the actor's own unwinding may
        # take, plus close slack). Entries still alive when the join ends
        # keep their rows excluded from the re-pend: the exclusion is
        # captured AFTER this join (inside _inner), so both they and every
        # claim this process took while it ran are in it. Lock-lease
        # expiry is the backstop for rows whose unwinding never finishes.
        join_bound = (
            deps.settings.cancellation_grace_period
            + deps.settings.cleanup_grace_period
            + CLOSE_TIMEOUT_SECS
        )
        live_tasks = [active.task for active in active_entries if not active.task.done()]
        if live_tasks:
            _done, still_running = await asyncio.wait(live_tasks, timeout=join_bound)
            if still_running:
                logger.warning(
                    "isolate-self-actor-join-timeout",
                    kind="isolate_self_actor_join_timeout",
                    worker_id=worker_id,
                    still_running=[
                        str(active.job_id) for active in active_entries if not active.task.done()
                    ],
                )

    try:
        # command_timeout, not just the connect timeout: the connect budget
        # bounds the handshake only, and every statement inside _inner() would
        # otherwise block on the socket read for asyncpg's default 60s each,
        # against the browned-out PG (accepts connections, answers slowly)
        # that a heartbeat-failure cascade produces. The isolate must finish
        # or give up promptly either way: shutdown.set() in the finally is
        # what ends the worker, and an unbounded transaction park would turn
        # the graceful-walk-away path into the watchdog's force exit.
        conn = await asyncpg.connect(
            pg_dsn,
            timeout=5.0,  # pyright: ignore[reportCallIssue]  # Why: asyncpg-stubs does not declare timeout kwarg on connect(); the parameter exists at runtime at 0.31.0.
            command_timeout=deps.settings.dispatcher_command_timeout,
        )
        try:

            async def _inner() -> tuple[int, int, int, int]:
                pending = 0
                crashed = 0
                cancelled = 0
                lost_race = 0
                # The reclaim events, batched into ONE insert at the end
                # of the transaction (the sweep's own event writer shape:
                # the microsecond ladder on the row ordinal keeps
                # occurred_at co-monotonic with the bigserial id the
                # watermark protocol reads).
                event_job_ids: list[JobId] = []
                event_details: list[object] = []
                # The re-pend's exclusion set, captured as LATE as the
                # statement boundary allows: AFTER the actor join above,
                # with no await between this capture and the SELECT below
                # that binds it. held_ids() covers BOTH maps, the
                # registered consumers (executing now) AND the claim
                # intents (taken off local_queue, not yet registered). The
                # intents are the easy one to miss: a consumer parked in
                # the take-to-register window has already claimed its row
                # (running, locked, this worker) but owns no registry
                # entry, so a snapshot of ``all()`` alone misses it and the
                # re-pend would hand the row back to the fleet while the
                # local body is about to execute it, a peer claims the
                # re-pended row the moment its scheduled_at arrives and
                # runs it concurrently with the local body, a double run,
                # and the local terminal write then loses the
                # attempt-epoch fence (silently discarded).
                # queued_ids() is included HERE even though the DRAINING
                # exit hand-back excludes it on purpose (held_ids' own
                # docstring): a row parked in local_queue during this
                # window is still this process's to execute - the consumer
                # loops are free to take and run it (only their NEXT
                # iteration honours the stop events, and the
                # shutdown_event-only fall-through runs a job taken on the
                # final turn even after shutdown.set()) - so re-pending it
                # hands it to a peer while the local body runs. If the
                # process dies before the take, lock-lease expiry is the
                # backstop, the same honest residual the interrupt arms
                # carry. The residual one-scheduler-step gap (a claim
                # committed but not yet marked in either map) is closed by
                # the producer_stop_event set at entry, not by this
                # capture: after it, a taken row is never dispatched.
                excluded_ids: list[JobId] = [
                    *deps.active_jobs.held_ids(),
                    *deps.active_jobs.queued_ids(),
                ]
                async with conn.transaction():
                    rows = await conn.fetch(  # pyright: ignore[reportUnknownVariableType]  # Why: conn type suppressed above due to asyncpg-stubs limitation on connect().
                        select_running_jobs_sql,
                        worker_id,
                        # The still-owned rows: their release belongs to the
                        # consumers' interrupt arms (the hold and the spent
                        # attempt), not to this re-pend. An empty array
                        # vacuously matches every row, so the statement
                        # shape never varies.
                        excluded_ids,
                    )
                    for row in rows:  # pyright: ignore[reportUnknownVariableType]  # Why: rows type suppressed above, propagates from conn.fetch() suppression.
                        # The guarded UPDATE is the race arbiter, so its
                        # rowcount, not the SELECT that picked the row ,
                        # decides whether this worker owns the transition.
                        # A row the leader's Sweep 1 reclaimed between the
                        # SELECT and this UPDATE reads UPDATE 0 here, and
                        # the sweep's own attempt row already holds
                        # job_attempts' PRIMARY KEY (job_id, attempt): an
                        # unconditional INSERT on the lost row is a
                        # non-transient constraint error that aborts the
                        # WHOLE transaction and collapses the isolation of
                        # rows that are still this worker's. Only the
                        # winner of the transition writes the attempt row.
                        tag = await conn.execute(
                            isolate_job_sql, row["id"], worker_id, max_backoff_seconds
                        )
                        if parse_rowcount(tag) == 0:
                            lost_race += 1
                            continue
                        # Mirrors _RECLAIM_HAS_BUDGET_SQL, which the UPDATE
                        # above applied as its SECOND CASE arm: an
                        # 'indefinite' job's budget is its
                        # schedule_to_close deadline, not max_attempts.
                        is_pending = (  # pyright: ignore[reportUnknownVariableType]  # Why: row column accessor types unknown, propagates from conn.fetch() suppression.
                            row["retry_kind"] != "non_retryable"
                            and (
                                row["retry_kind"] == "indefinite"
                                or row["attempt"] < row["max_attempts"]
                            )
                        )
                        # The classification mirrors the UPDATE's CASE
                        # ORDER exactly: operator cancel first, a
                        # cancel-in-flight row terminalises 'cancelled'
                        # whatever its budget, then the re-pend arm, then
                        # crashed.
                        new_status: str
                        if row["cancel_phase"] != 0:
                            cancelled += 1
                            new_status = "cancelled"
                        elif is_pending:
                            pending += 1
                            new_status = "pending"
                        else:
                            crashed += 1
                            new_status = "crashed"
                        # The reclaim rides the crash-reclaim outbox
                        # channel exactly like the sweep's rows (see the
                        # module comment). reason='lock_expired' is the
                        # CHANNEL key -- the slice poll_reclaim_events
                        # tails and the retention carve-out keeps -- not
                        # a per-row lease claim: this row's lease may
                        # still be live (isolate fires on heartbeat-
                        # connection loss, not lease expiry), exactly as
                        # the sweep's heartbeat_timeout arm stamps the
                        # same channel value for rows whose global lease
                        # never expired. The per-event truth is
                        # cause='isolate_self' (the sweep's rows name the
                        # deadline that fired); worker_id is the
                        # last-known holder, the sweep's detail shape.
                        detail: dict[str, object] = {
                            "from_state": "running",
                            "to_state": new_status,
                            "reason": "lock_expired",
                            "cause": "isolate_self",
                            "worker_id": str(worker_id),
                        }
                        event_job_ids.append(JobId(row["id"]))
                        event_details.append(jsonb_param(detail))
                        await conn.execute(
                            insert_attempt_sql,
                            row["id"],
                            row["attempt"],
                            row["started_at"],
                            "crashed",
                            ERROR_CLASS_HEARTBEAT_LOST,
                            None,
                            None,
                            None,
                            worker_id,
                            "{}",  # metadata, matches the sweep paths' literal
                        )
                    if event_job_ids:
                        await conn.execute(
                            INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=schema),
                            event_job_ids,
                            event_details,
                            "state_change",
                        )
                return pending, crashed, cancelled, lost_race

            # shield_with_retrieval, not plain asyncio.shield: on outer
            # cancel the isolation tx keeps running detached on a conn the
            # finally below closes, its late failure must be retrieved and
            # logged, not lost as "Task exception was never retrieved" noise
            # (see taskq._shield).
            (
                jobs_pending_count,
                jobs_crashed_count,
                jobs_cancelled_count,
                jobs_lost_race_count,
            ) = await shield_with_retrieval(_inner())
        finally:
            # Why bounded: isolate_self only runs when PG is already
            # suspected dead (heartbeat failures exceeded), so this close is
            # exactly the dead-PG hang case, unbounded, it would
            # wedge shutdown.set() below. The helper never raises, so a
            # close error can no longer mask an in-flight exception or be
            # misreported as an isolate-self failure.
            await close_conn_bounded(conn, "isolate-self", CLOSE_TIMEOUT_SECS, mid_run=True)
    except Exception as exc:
        logger.warning(
            "isolate-self-failure",
            kind="isolate_self_failure",
            worker_id=str(worker_id),
            dsn_host=host,
            error=repr(exc),
        )
    finally:
        shutdown.set()
        logger.warning(
            "isolate-self-complete",
            worker_id=str(worker_id),
            jobs_pending_count=jobs_pending_count,
            jobs_crashed_count=jobs_crashed_count,
            jobs_cancelled_count=jobs_cancelled_count,
            jobs_lost_race_count=jobs_lost_race_count,
        )
