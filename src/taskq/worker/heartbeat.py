"""Heartbeat loop and cancel-poll seam.

Each tick acquires one connection from heartbeat_pool, opens a single
transaction, and atomically extends workers.last_seen_at, jobs lock /
heartbeat columns, and reservation_slots leases for jobs locked by this
worker — except the jobs the worker has disowned (``WorkerDeps.disowned_jobs``:
finished with, outcome unrecordable), whose leases must lapse so the
reclaim sweep can hand them back. The tick's whole command sequence runs
under ONE command-timeout budget (see the tick block and
:func:`_lease_renewal_threshold` for the lease-model arithmetic that
depends on it), with a bounded rollback-or-close teardown. The jobs-lock
renewal is threshold-gated (:func:`_lease_renewal_threshold`): a row
whose lease is still comfortably fresh is left alone so a healthy beat
stops paying a non-HOT update per running row per tick (#227), while
rows carrying a per-job ``heartbeat_timeout`` (the reclaim sweep's
heartbeat arm needs their beats fresh) and rows at/under the threshold
renew on every beat. After max_heartbeat_failures
consecutive connection failures, isolate_self proactively transitions
running jobs and signals shutdown.
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
from taskq.backend._records import jsonb_param
from taskq.backend._sql import (
    INSERT_ATTEMPT_SQL,
    build_heartbeat_sql,
    parse_rowcount,
)
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: isolate and the reclaim sweep must decide a job's budget and its hand-back delay identically — one fragment, no second hand-maintained copy.
    _RECLAIM_DELAY_SQL,
    _RECLAIM_HAS_BUDGET_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
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


def _lease_renewal_threshold(
    lock_lease: timedelta,
    heartbeat_interval: float,
    max_heartbeat_failures: int,
    heartbeat_command_timeout: float,
) -> timedelta:
    """The remaining-lease floor below which the heartbeat renews a row.

    Renewing every held row on every beat rewrites ``lock_expires_at`` —
    the key of ``jobs_running_lock_expires_idx`` — so every beat is a
    non-HOT update per running row (new index entries in every index a
    running row satisfies, fleet-wide, forever; #227). Renewing only
    rows whose remaining lease is at or under this threshold spaces the
    rewrites out instead of paying them every beat.

    Sizing — measured against the ENFORCED failed-tick bound, not a
    hoped-for one. The heartbeat's tick runs its whole command sequence
    (BEGIN, the three writes, the still-held probe, the cancel hook's
    statements, COMMIT) under ONE ``asyncio.timeout(
    heartbeat_command_timeout)`` budget, and its teardown is bounded by
    the same budget's remainder (a rollback that fits) or by a bounded
    close (server-side rollback on disconnect — no awaited round trip).
    A failed tick is therefore bounded by, with every term enforced:

    * the pool acquire: at most ``heartbeat_interval`` (its own timeout;
      an acquire that takes the whole timeout fails the tick with no
      commands and no teardown at all);
    * the command sequence: at most one ``heartbeat_command_timeout``
      (the budget);
    * the teardown: at most one ``heartbeat_command_timeout`` (the
      bounded rollback-or-close).

    so the worst beat-to-beat gap is ``heartbeat_interval + 2 *
    heartbeat_command_timeout``, and the floor is ``(F+1)`` of those —
    the loop isolates on the (F+1)-th consecutive failure, and the lease
    must still be valid at that decision.

    Fix-round premise correction: the round-1 derivation assumed a
    failed tick was bounded by "acquire-block then a timed-out command"
    (interval + ONE command timeout). That premise was false — the tick
    issues >= 3 commands each separately bounded by the pool's
    per-query command timeout, and the transaction's teardown adds its
    own round trip — so a brownout tick (a contended acquire, then two
    just-under-timeout statements, then a timeout) lasted acquire +
    ~3 command-timeouts, which the round-1 floor of (F+1) * (interval +
    command_timeout) did not cover: a legal skip at 49.5s of a 60s lease
    followed by four such failed ticks let the lease expire 3.2-9.2s
    BEFORE the isolate decision while the unconditional renewal
    survived. The per-tick command budget above makes the bound true by
    enforcement, and this floor sizes against it.

    Consequences, stated honestly:

    * Healthy beats: a skip happens only while remaining > threshold, so
      the next beat's remaining is > ``threshold - worst_gap``; at the
      floor that is ``F * worst_gap`` (>= one full worst gap for F >= 1)
      — a healthy-but-slow worker never lets a lease lapse.
    * The failure cascade: the worst case is a skip at remaining
      ``threshold + eps`` followed by ``F+1`` failed beats, each gap
      STRICTLY under one worst gap (an acquire that consumed its whole
      timeout fails with no commands and no teardown — a gap of exactly
      the interval; a tick that ran commands consumed strictly less than
      its whole acquire allowance). The lease at the isolate decision is
      then > 0 by construction, and a worker that recovers after F
      failures still holds a full worst gap of lease and renews it.
    * At the DEFAULT settings the floor is ``4 * (10 + 2 + 2) = 56s``
      against a 60s lease: the gate renews every beat — byte-identical
      cadence to the unconditional renewal, so the round-1
      default-settings lapse window closes outright — and the enforced
      budget independently makes the default-config cascade survivable
      with margin (4 gaps x 14s = 56s against the 60s lease), which the
      un-enforced per-statement bound (statement-count-dependent, up to
      ``interval + (k+1) * command_timeout`` per tick) is not.
    * The gate saves again from ``lock_lease`` ≈ 70s upward (2x at 70,
      4x at 90, and the ``lock_lease / 2`` arm dominates from ≈ 112,
      giving ~6x at 120+). Raising ``heartbeat_command_timeout`` (for a
      loaded or cross-region Postgres whose beat needs more than one
      command-timeout in total) raises the floor with it — the gate
      harvests only slack that actually exists.
    * Whenever the floor meets or exceeds the lease itself, the gate
      renews every row on every beat — zero savings, exactly the
      unconditional behaviour, because there is no slack that is safe to
      harvest.

    The comparison itself is server-side (``lock_expires_at <=
    clock_timestamp() + $4`` in the gated statement), so worker-clock
    skew cannot move the threshold: the same clock that stamped the
    lease judges it.
    """
    worst_beat_gap = heartbeat_interval + 2 * heartbeat_command_timeout
    safety_floor = timedelta(
        seconds=(max_heartbeat_failures + 1) * worst_beat_gap,
    )
    return max(safety_floor, lock_lease / 2)


# Which disowned ids still name a running row locked to this worker: the
# rest have been reclaimed (re-pended, or claimed by another worker) and
# leave the set. Only issued on a tick whose set is non-empty.
_SELECT_STILL_HELD_SQL_TEMPLATE = (
    'SELECT id FROM "{schema}".jobs '
    "WHERE id = ANY($1::uuid[]) AND locked_by_worker = $2 AND status = 'running'"
)
_tick_duration = _meter.create_histogram(
    name="taskq.heartbeat.tick_duration_seconds",
    unit="s",
    description="Wall-clock seconds for one heartbeat tick.",
)


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
    # The tick's single command budget (#227 fix round): ONE
    # heartbeat_command_timeout for the tick's whole command sequence
    # (BEGIN + writes + probes + hook + COMMIT, and the post-tx drain's
    # share). See the tick block below for why the sequence — not each
    # statement — is the unit the lease model needs bounded.
    tick_command_budget = deps.settings.heartbeat_command_timeout
    # The renewal threshold (#227): a healthy beat only rewrites leases
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

    # Monotonic stamp of the last jobs-lock renewal that landed: the
    # reference the next tick measures its remaining lease against. None
    # until the first renewal — there is nothing to measure before it.
    last_renewal_at: float | None = None
    while not shutdown.is_set():
        deps.liveness.tick("heartbeat", period=interval)
        _in_tx_failed = False
        tick_start = time.monotonic()
        try:
            _tick_raised = False
            # The tick's single command budget (#227 fix round): None
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
                        # still-held probe, the cancel hook's statements,
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
                            # the next tick's exclusion — the prune below must
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
                            # The gated renewal (#227): binds the threshold as
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
                        # remainder — deliberately OUTSIDE the expired
                        # asyncio.timeout scope: an await started after
                        # the scope expired is NOT re-cancelled (measured
                        # — asyncio.timeout fires once), so the teardown
                        # must carry its own bound rather than hide
                        # inside the dead scope.
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
                            # timeout — close_conn_bounded never raises),
                            # and the pool discards the closed connection
                            # on release. This is the budget-exhausted
                            # path, so it is also the path that keeps the
                            # failed tick inside the lease model's
                            # h + 2c bound.
                            await close_conn_bounded(
                                conn,  # type: ignore[arg-type]  # Why: PoolConnectionProxy delegates close()/terminate() to the underlying Connection at runtime; pyright's stubs model the proxy as unrelated, the same delegation the run_in_tx call below relies on.
                                "heartbeat-tick-budget",
                                tick_command_budget,
                                mid_run=True,
                            )
                        # Re-raise the tick's ORIGINAL exception (a bare
                        # raise re-raises it identically): a teardown
                        # timeout or the close's bounds must never
                        # displace what actually failed the tick — least
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
                # by the time this runs — the transaction has committed or
                # rolled back — so mark_abandoned cannot self-deadlock here.
                if cancel_controller is not None:
                    # post_tx shares the tick's ONE command budget: it
                    # runs under whatever the deadline has left, and is
                    # deferred to the next tick when nothing is left (an
                    # expired asyncio.timeout does not re-cancel an
                    # await that starts after expiry — measured — so the
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
            # Under threshold-gated renewal (#227) the sample keeps this
            # beat-cadence meaning deliberately: it is stamped on every
            # successful tick, whether or not the gate renewed any rows,
            # so a late or failing beat lowers it exactly as before and
            # alert thresholds calibrated to the old per-tick cadence
            # keep their semantics. For rows the gate skipped (still
            # above the threshold) the true remaining is anywhere up to
            # the full lease — the sample is the "if this beat renewed
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
            if not _in_tx_failed:
                deps.heartbeat_failures += 1
                update_heartbeat_consecutive_failures(str(worker_id), deps.heartbeat_failures)
            record_heartbeat_miss(str(worker_id))
            logger.warning(
                "heartbeat-tick-failure",
                worker_id=str(worker_id),
                consecutive_failures=deps.heartbeat_failures,
                error_class=type(e).__name__,
                error=str(e),
            )
            early_warn_threshold = deps.settings.max_heartbeat_failures // 2
            if early_warn_threshold > 0 and deps.heartbeat_failures == early_warn_threshold:
                logger.warning(
                    "heartbeat-failures-approaching-limit",
                    worker_id=str(worker_id),
                    consecutive_failures=deps.heartbeat_failures,
                    max_heartbeat_failures=deps.settings.max_heartbeat_failures,
                    error_class=type(e).__name__,
                )
            if deps.heartbeat_failures > deps.settings.max_heartbeat_failures:
                await isolate_self(deps, worker_id, shutdown)
                return
        except Exception:
            tick_duration_s = time.monotonic() - tick_start
            _tick_duration.record(tick_duration_s)
            logger.exception(
                "heartbeat-tick-unexpected-error",
                worker_id=str(worker_id),
            )
        # The wait is anchored to the tick's START, not its end, so the
        # beat cadence is the interval however long the tick took. A
        # fixed post-tick sleep instead makes the cadence
        # tick_duration + interval, and a tick may legitimately run for
        # nearly a whole interval — the pool acquire above is bounded at
        # exactly that. One slow or failed tick then stretches the gap
        # between good beats to roughly twice the interval, which is the
        # very sizing the ops guide calls the safe floor for a per-job
        # heartbeat_timeout: the knob's own guidance would be unable to
        # tolerate a single transient blip, and a worker that is alive,
        # lease-valid and beating again would lose its job to the sweep.
        # The heartbeat's promise to the reclaim arm is a beat every
        # interval; this is where that promise is kept. A tick that
        # overruns the interval waits zero and re-enters immediately,
        # which is the correct urgency — it is already late — and cannot
        # become a hot loop, because the next tick's own pool acquire is
        # bounded at the interval and paces it.
        remaining = max(0.0, interval - (time.monotonic() - tick_start))
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
)

# Recovery transitions via isolate_self: running→pending when retries
# remain; running→cancelled when exhausted with a cancel in-flight;
# running→crashed otherwise.  All are present in VALID_TRANSITIONS.
# The heartbeat-pool failure forces a fresh asyncpg
# connection, so the worker cannot rely on its in-memory status being
# current.  The SQL self-guards via WHERE status='running' AND
# locked_by_worker=$2, which atomically serialises the read+write and
# ensures only rows still belonging to this worker transition.  Note:
# error_class='HeartbeatLost' is intentionally distinct from Sweep 1's
# 'WorkerCrashed' — a heartbeat-lost worker may still be alive but
# partitioned, while Sweep 1 assumes the worker is gone.

# Branch-for-branch mirror of _sweeps.py's _SWEEP_1_SQL SET clause,
# sharing its budget predicate and hand-back delay verbatim — the
# property test tests/test_leader_property.py asserts row-state
# equivalence between this path and the sweep, so any branch change there
# (cancel-state reset on retry, 'cancelled' label for an exhausted
# cancel-in-flight reclaim, clock_timestamp() for terminal timestamps)
# must be mirrored here.  Note the mirror covers the SET clause, NOT the
# selection predicate: the sweep leaves cancel-in-flight jobs alone until
# cancel_grace + cleanup_grace + 60s has passed (a merely-slow
# cancellation isn't pre-empted), while isolate applies the 'cancelled'
# arm immediately — deliberate asymmetry, since isolate means THIS worker
# is going away now and there is no lock-holder left to complete the
# cooperative protocol.  Isolate writes no job_events row (a graceful
# shutdown is not a crash-reclaim), so the visibility-delay
# co-monotonicity motivation for clock_timestamp() does not apply — it is
# kept anyway so the two templates stay structurally identical.
#
# The re-pend arm wakes nobody: an UPDATE never fires the INSERT-only
# wake trigger and this worker is on its way out, so the fleet claims
# the handed-back row within the producer's poll floor
# (notify_poll_interval / poll_interval) — the same wake source every
# release arm in backend/_sql_templates.py relies on.
#
#: The statement is built as ONE constant: the literal with the sweep's
#: shared fragments substituted by name (``str.replace``, not ``format``,
#: so ``{schema}`` stays the only placeholder the caller renders).
#: ``$3`` is the effective-cap ceiling (max_retry_backoff, seconds),
#: this statement's third parameter after the job id and worker id — the
#: sweep's shared delay fragment carries the placeholder as
#: ``{max_backoff_seconds}`` precisely so each statement binds the index
#: its own parameter layout assigns.
_ISOLATE_JOB_SQL_TEMPLATE = (
    """\
UPDATE "{schema}".jobs j
SET status = CASE
        WHEN {has_budget}
            THEN 'pending'::"{schema}".job_status
        WHEN j.cancel_phase != 0
            THEN 'cancelled'::"{schema}".job_status
        ELSE 'crashed'::"{schema}".job_status
    END,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    cancel_phase = 0,
    cancel_requested_at = NULL,
    -- An isolate re-pend hands the row back to the fleet, so it routes
    -- by the actor's current assignment from here on (the routing
    -- contract in taskq/backend/_dispatch_sql.py) -- the same SET this
    -- template mirrors branch-for-branch from _SWEEP_1_SQL.
    assignment_routed = true,
    scheduled_at = CASE
        WHEN {has_budget}
            THEN clock_timestamp() + {reclaim_delay}
        ELSE j.scheduled_at
    END,
    finished_at = CASE
        WHEN NOT ({has_budget})
            THEN clock_timestamp()
        ELSE j.finished_at
    END
WHERE j.id = $1 AND j.status = 'running' AND j.locked_by_worker = $2""".replace(
        "{has_budget}", _RECLAIM_HAS_BUDGET_SQL
    )
    .replace("{reclaim_delay}", _RECLAIM_DELAY_SQL)
    .replace("{max_backoff_seconds}", "$3")
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
    select_running_jobs_sql = _SELECT_RUNNING_JOBS_SQL_TEMPLATE.format(schema=schema)
    isolate_job_sql = _ISOLATE_JOB_SQL_TEMPLATE.format(schema=schema)
    insert_attempt_sql = INSERT_ATTEMPT_SQL.format(schema=schema)
    # The reclaim delay's effective ceiling, bound per statement — the
    # same operator knob the sweep binds as its own parameter, so a
    # heartbeat-lost hand-back lands on the same schedule the leader's
    # reclaim would have stamped.
    max_backoff_seconds = deps.settings.max_retry_backoff.total_seconds()
    jobs_pending_count = 0
    jobs_crashed_count = 0
    jobs_cancelled_count = 0
    # Rows whose guarded UPDATE no-oped — transitioned by the leader's
    # sweep between this path's SELECT and UPDATE; their attempt rows
    # belong to the winner, and the count keeps the complete log's
    # arithmetic explainable (selected rows = pending + crashed +
    # cancelled + lost_race).
    jobs_lost_race_count = 0

    try:
        conn = await asyncpg.connect(pg_dsn, timeout=5.0)  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # Why: asyncpg-stubs does not declare timeout kwarg on connect(); the parameter exists at runtime at 0.31.0.  asyncpg default is 60s — far too long when PG is already problematic.
        try:

            async def _inner() -> tuple[int, int, int, int]:
                pending = 0
                crashed = 0
                cancelled = 0
                lost_race = 0
                async with conn.transaction():
                    rows = await conn.fetch(  # pyright: ignore[reportUnknownVariableType]  # Why: conn type suppressed above due to asyncpg-stubs limitation on connect().
                        select_running_jobs_sql, worker_id
                    )
                    for row in rows:  # pyright: ignore[reportUnknownVariableType]  # Why: rows type suppressed above — propagates from conn.fetch() suppression.
                        # The guarded UPDATE is the race arbiter, so its
                        # rowcount — not the SELECT that picked the row —
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
                        # above applied: an 'indefinite' job's budget is its
                        # schedule_to_close deadline, not max_attempts.
                        is_pending = (  # pyright: ignore[reportUnknownVariableType]  # Why: row column accessor types unknown — propagates from conn.fetch() suppression.
                            row["retry_kind"] != "non_retryable"
                            and (
                                row["retry_kind"] == "indefinite"
                                or row["attempt"] < row["max_attempts"]
                            )
                        )
                        if is_pending:
                            pending += 1
                        elif row["cancel_phase"] != 0:
                            # Mirrors _ISOLATE_JOB_SQL_TEMPLATE's CASE arm:
                            # exhausted + cancel in-flight → 'cancelled'.
                            cancelled += 1
                        else:
                            crashed += 1
                        await conn.execute(
                            insert_attempt_sql,
                            row["id"],
                            row["attempt"],
                            row["started_at"],
                            "crashed",
                            "HeartbeatLost",
                            None,
                            None,
                            None,
                            worker_id,
                            "{}",  # metadata — matches the sweep paths' literal
                        )
                return pending, crashed, cancelled, lost_race

            # shield_with_retrieval, not plain asyncio.shield: on outer
            # cancel the isolation tx keeps running detached on a conn the
            # finally below closes — its late failure must be retrieved and
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
            # exactly the dead-PG hang case — unbounded, it would
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
