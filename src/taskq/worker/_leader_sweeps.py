"""Sweep loop functions for MaintenanceLeader.

The sweep loop functions (``_sweep_loop``, ``_prune_loop``,
``_archive_expiry_loop``, ``_queue_depth_loop``,
``_reservation_slots_loop``, ``_stranded_jobs_loop``,
``_backlog_detection_loop``) live here as
module-level functions taking a :class:`~taskq.worker._leader_shared.SweepContext`
as the first parameter — the subset of ``MaintenanceLeader`` state the
sweeps need, so this module has no dependency on ``leader.py``.
"""

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Final, cast

import asyncpg
import croniter as cr
import structlog

from taskq.backend._protocol import ConnLike
from taskq.backend._sweeps import SweepBatchSizer
from taskq.backend.statemachine import ACTIVE_STATUSES
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    schema_lock_name,
)
from taskq.obs import (
    StrandedReason,
    get_logger,
    record_lock_contention,
    record_sweep_success,
    record_sweep_timeout,
    update_actor_backlog_cache,
    update_actor_oldest_pending_age_cache,
    update_actor_oldest_running_age_cache,
    update_jobs_by_status_cache,
    update_jobs_running_cache,
    update_oldest_due_age_cache,
    update_queue_depth_cache,
    update_queue_live_workers_cache,
    update_reservation_slots_cache,
    update_running_lease_expired_cache,
    update_scheduled_count_cache,
    update_stranded_jobs_cache,
)
from taskq.ratelimit.registry import (
    _KEYED_IDLE_THRESHOLD,  # pyright: ignore[reportPrivateUsage]  # Why: shared constant — centralised in registry.py so the sweep and the opportunistic eviction path never drift.
)
from taskq.ratelimit.registry import (
    registry as rl_registry,
)
from taskq.worker._leader_shared import (
    _EK2,
    _EK3,
    _QUERY_QUEUE_DEPTH_SQL_TEMPLATE,
    _QUERY_RESERVATION_SLOTS_SQL_TEMPLATE,
    SweepContext,
    _build_retention_per_status,
    _dbg,
    _err,
    _load_actor_retention_overrides,
    _metric_duration,
    _metric_rows,
    _schedule_utc_to_cron,
    _sweep_duration_hist,
    _sweep_rows_counter,
    archive_expiry_sweep,
    cleanup_stale_workers,
    complete_stale_batches,
    prune_terminal_jobs,
)
from taskq.worker._transient import TRANSIENT_PG_ERRORS, UnexpectedLoopErrorGuard

__all__ = [
    "_archive_expiry_loop",
    "_backlog_detection_loop",
    "_prune_loop",
    "_queue_depth_loop",
    "_reservation_slots_loop",
    "_stranded_jobs_loop",
    "_sweep_duration_hist",
    "_sweep_loop",
    "_sweep_rows_counter",
]

log: structlog.stdlib.BoundLogger = get_logger(__name__)

#: How long before an already-warned stranded actor is warned about again.
#: The condition is persistent by nature (it needs an operator to create
#: the actor_config row or point a worker's subscription at the queue),
#: so re-warning every tick would be noise; never re-warning made a
#: permanent, growing backlog invisible after one line.
_STRANDED_REWARN_SECS: float = 3600.0

#: Server-side batch bound for the prune family, as a fraction of the
#: dispatcher pool's client-side command_timeout. The batches run on
#: dispatcher-pool connections, whose command_timeout fires as an opaque
#: client ``TimeoutError``; keeping the server-side bound below it means
#: an overloaded database aborts the batch server-side
#: (``QueryCanceledError`` — the transient family the SweepBatchSizer
#: breaker counts) and the next attempt runs at the latched reduced tier,
#: instead of the client cancelling with no degradation signal. 80% leaves
#: a full round-trip margin at the default 5 s pool timeout (4 s server
#: bound). Pairing a per-query timeout with a reduced-batch circuit breaker
#: ensures the server-side bound is the tighter ceiling this family lives
#: under, so the reduced tier — not a longer timeout — is what makes a
#: loaded database drainable.
_PRUNE_TIMEOUT_FRACTION: Final[float] = 0.8

#: Intra-day retry backoff for a FAILED prune/archive-expiry attempt: 60 s
#: doubling, capped at 30 min. The once-per-SUCCESSFUL-attempt-per-day
#: guard is deliberate policy and stays — the retry fills only the
#: failure half, so a prune that keeps failing under load retries within
#: the day instead of waiting for tomorrow's cron fire, while a day that
#: succeeded is never pruned twice. 60 s is fast enough to drain behind a
#: passing load spike, yet slow enough not to pile onto the database that
#: just aborted the batch. Module-level (not settings) so tests shrink it
#: without threading a knob through every loop, the same contract
#: DEFAULT_MAX_CONSECUTIVE_UNEXPECTED carries.
_PRUNE_RETRY_BACKOFF_INITIAL_SECS: float = 60.0
_PRUNE_RETRY_BACKOFF_CAP_SECS: float = 1800.0


def _prune_statement_timeout_ms(command_timeout_secs: float) -> int:
    """The prune family's per-batch server-side bound under the pool it
    runs on (see ``_PRUNE_TIMEOUT_FRACTION``). Never below 1 ms: a
    sub-millisecond bound is ``statement_timeout = 0``-adjacent
    (0 disables the safety net), and the batch helpers reject it at the
    typed boundary anyway."""
    return max(1, int(command_timeout_secs * _PRUNE_TIMEOUT_FRACTION * 1000.0))


def _next_retry_backoff(current: float | None) -> float:
    """The backoff after one more failed attempt: the initial delay, then
    doubling, capped — the ladder the prune loops retry on."""
    if current is None:
        return _PRUNE_RETRY_BACKOFF_INITIAL_SECS
    return min(current * 2.0, _PRUNE_RETRY_BACKOFF_CAP_SECS)


async def _sleep_until_next_attempt(
    shutdown: asyncio.Event,
    next_fire: datetime,
    retry_backoff: float | None,
) -> bool:
    """Sleep until the next scheduled cron fire, or — after a failed
    attempt — the next backoff retry, whichever is earlier; interruptible
    by shutdown.

    Returns True when the wake was a backoff retry (the failure sequence
    continues from its current rung); False when the scheduled fire
    governs (a fresh sequence starts, so yesterday's capped rung does not
    carry into today's attempt).
    """
    until_fire = max(0.0, (next_fire - datetime.now(UTC)).total_seconds())
    secs = until_fire if retry_backoff is None else min(retry_backoff, until_fire)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(shutdown.wait(), timeout=secs)
    return retry_backoff is not None and retry_backoff < until_fire


def _batch_drain_gate(
    ctx: SweepContext,
    shutdown: asyncio.Event,
    *,
    loop_name: str,
    period_secs: float,
) -> Callable[[], bool]:
    """The between-batches gate the prune-family drains receive: False once
    shutdown is set (the drain stops; committed batches stay committed),
    a detector-2 liveness tick otherwise.

    The prune loops are cron-driven — up to a day between attempts — so an
    always-registered liveness entry with the cron cadence would give
    detector 2 a multi-day staleness budget and detect nothing. Instead
    the drain registers on its first tick and the loop forgets the entry
    when the attempt ends (the gated-loop pattern the leadership watchdog
    uses), with the period set to the per-batch bound so a wedged drain
    trips the detector within a few batches' worth of budget.
    """

    def gate() -> bool:
        if shutdown.is_set():
            return False
        ctx.deps.liveness.tick(loop_name, period=period_secs)
        return True

    return gate


async def _sleep_interruptible(shutdown: asyncio.Event, seconds: float) -> None:
    """Sleep that returns as soon as *shutdown* is set.

    ``MaintenanceLeader.run``'s TaskGroup waits for its children on exit,
    so a bare ``asyncio.sleep(interval)`` keeps the worker hanging for the
    full in-flight sleep after SIGTERM — with an operator-configured
    interval (e.g. ``TASKQ_STRANDED_JOBS_INTERVAL=3600``) that is an
    hour-long shutdown hang. Same pattern as the cron waits in
    ``_prune_loop`` / ``_archive_expiry_loop``; callers re-check
    ``shutdown.is_set()`` at the top of their ``while`` loop.
    """
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)


def _is_deadline_family(exc: BaseException) -> bool:
    """Whether *exc* is the deadline family (client deadline or server cancel).

    Why ``type(exc) is``, not ``isinstance``: ``TimeoutError`` is an
    ``OSError`` subclass — ``isinstance`` would also match a raw ``OSError``
    (socket death), which is a different failure family with different
    remediation.
    """
    return type(exc) is TimeoutError or isinstance(exc, asyncpg.QueryCanceledError)


def _sampler_read_failed(
    ctx: SweepContext, sampler_name: str, event: str, exc: BaseException
) -> None:
    """Report a gauge sampler whose read did not complete.

    A sampler failure is the one failure in this module that leaves no trace
    an alert rule can read: the gauge it feeds keeps serving its last value,
    so depth stops rising and age stops growing — the same flat picture a
    drained queue paints. No job fails and nothing is retried, so the warning
    below is the only other signal, and warnings are not alertable. Every
    failure is counted, not just the deadline family a completed-but-aborted
    sweep reports: for a detector, "the read did not happen" is the whole
    fault regardless of which error carried it.
    """
    record_sweep_timeout(sampler_name)
    log.warning(
        event,
        kind=f"{sampler_name}_sampling_failed",
        worker_id=str(ctx.worker_id),
        error=repr(exc),
    )


async def _drain_bounded(
    ctx: SweepContext,
    shutdown: asyncio.Event,
    *,
    sweep_name: str,
    call: Callable[[], Awaitable[int]],
    warn_event: str,
    warn_kind: str,
) -> bool:
    """Drain additional committed batches after a non-empty sweep call.

    Every batch commits, so a stopped drain is a pause, not a rollback: the
    next tick resumes where this one stopped. Up to
    ``sweep_drain_batches - 1`` further calls (the initial call already
    ran), each its own committed batch — the backend applies the batch size
    internally, so the same callable is re-invoked with no extra arguments.

    Returns False when a transient PG error ended the drain early; the
    caller marks the iteration unclean so the backstop streak is not reset.
    """
    for _ in range(ctx.deps.settings.sweep_drain_batches - 1):
        # Demotion stops the drain at the same batch boundary shutdown does:
        # each batch is committed, so stopping is a pause the successor
        # resumes from rather than work lost.
        if shutdown.is_set() or not ctx.deps.leading():
            break
        # Ticking between calls keeps detector 2 from ageing the loop out
        # during a long drain; name and period match the loop's outer tick.
        ctx.deps.liveness.tick("leader.sweep", period=ctx.deps.settings.sweep_interval)
        start = time.monotonic()
        rows: int | None = None
        try:
            rows = await call()
        except TRANSIENT_PG_ERRORS as exc:
            if _is_deadline_family(exc):
                record_sweep_timeout(sweep_name)
            log.warning(
                warn_event,
                kind=warn_kind,
                worker_id=str(ctx.worker_id),
                error=repr(exc),
            )
            return False
        finally:
            # Same sample discipline as the parent sweep call: duration
            # always; rows only when the awaited call returned, so a
            # timed-out drain call cannot masquerade as an empty one.
            _metric_duration(sweep_name, start)
            if rows is not None:
                _metric_rows(sweep_name, rows)
                record_sweep_success(sweep_name)
        if rows == 0:
            break
    return True


async def _sweep_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Leader sweep loop: sweeps 1/2/4, result TTL, stale workers, batches.

    Unexpected (non-transient) errors are backstopped by
    :class:`UnexpectedLoopErrorGuard` exactly like the election, watchdog,
    cron, and scheduled-wake loops: tolerated and logged loudly for a few
    consecutive iterations, then deliberately fatal — never an instant
    silent leader teardown (which leaves the cluster with no sweeper and
    orphans 'running' forever), never an infinite silent retry. Only a
    fully successful work iteration resets the streak; a transiently
    failing one must not buy a fault more time.
    """
    warned_sweep_1 = warned_sweep_2 = False
    guard = UnexpectedLoopErrorGuard("leader.sweep")
    # Loop-invariant sweep-1 arguments; hoisted so the drain closure below
    # binds outer names, not per-iteration locals.
    cancellation_grace = timedelta(seconds=ctx.deps.settings.cancellation_grace_period)
    cleanup_grace = timedelta(seconds=ctx.deps.settings.cleanup_grace_period)
    stale_workers_grace = timedelta(
        seconds=ctx.deps.settings.heartbeat_interval
        * (ctx.deps.settings.max_heartbeat_failures + 3)
    )

    async def sweep_1_call() -> int:
        return await ctx.backend.reclaim_expired_locks(cancellation_grace, cleanup_grace)

    async def sweep_2_call() -> int:
        return await ctx.backend.deadline_sweep()

    # PG-only sweeps (gated on hasattr at the call site below): each call
    # acquires its own dispatcher connection so every committed batch of a
    # drain is independent, and the bound is the operator-tunable
    # event_writer_batch_size, mirroring the stale-batch sweep's wiring.
    async def sweep_rt_call() -> int:
        async with ctx.deps.dispatcher_pool.acquire(
            timeout=ctx.deps.settings.dispatcher_command_timeout
        ) as conn:
            return cast(
                "int",
                await ctx.backend.sweep_expired_results(  # type: ignore[reportAttributeAccessIssue]  # Why: guarded by the hasattr gate at the call site; only PostgresBackend implements these maintenance sweeps.
                    conn,
                    schema=ctx.deps.settings.schema_name,
                    batch_size=ctx.deps.settings.event_writer_batch_size,
                ),
            )

    async def stale_workers_call() -> int:
        async with ctx.deps.dispatcher_pool.acquire(
            timeout=ctx.deps.settings.dispatcher_command_timeout
        ) as conn:
            return await cleanup_stale_workers(
                conn,
                worker_id=ctx.worker_id,
                staleness=stale_workers_grace,
                schema=ctx.deps.settings.schema_name,
                batch_size=ctx.deps.settings.event_writer_batch_size,
            )

    # Sweep 4 and the stale-batch completion sweep: each drain call
    # acquires its own dispatcher connection so every committed batch is
    # independent — the same per-call acquire the sweeps above use.
    async def leaked_slots_call() -> int:
        async with ctx.deps.dispatcher_pool.acquire(
            timeout=ctx.deps.settings.dispatcher_command_timeout
        ) as conn:
            return cast(
                "int",
                await ctx.backend.sweep_leaked_reservation_slots(  # type: ignore[reportAttributeAccessIssue]  # Why: guarded by the hasattr gate at the call site; only PostgresBackend implements these maintenance sweeps.
                    conn,
                    schema=ctx.deps.settings.schema_name,
                    batch_size=ctx.deps.settings.event_writer_batch_size,
                ),
            )

    async def stale_batches_call() -> int:
        async with ctx.deps.dispatcher_pool.acquire(
            timeout=ctx.deps.settings.dispatcher_command_timeout
        ) as conn:
            return await complete_stale_batches(
                conn,
                schema=ctx.deps.settings.schema_name,
                batch_size=ctx.deps.settings.event_writer_batch_size,
            )

    while not shutdown.is_set():
        ctx.deps.liveness.tick("leader.sweep", period=ctx.deps.settings.sweep_interval)
        if ctx.deps.leading():
            iteration_clean = True
            try:
                # Sweep 1: reclaim_expired_locks
                start = time.monotonic()
                rows_1: int | None = None
                try:
                    # No `now` argument: the sweep's server-side predicates
                    # (clock_timestamp()) are the single arbiter.
                    rows_1 = await sweep_1_call()
                except NotImplementedError as exc:
                    iteration_clean = False
                    if not warned_sweep_1:
                        _err("sweep_expired_locks_unimplemented", _EK2, ctx.worker_id, exc)
                        warned_sweep_1 = True
                except TRANSIENT_PG_ERRORS as exc:
                    # Why: PG loss is transient here, exactly as for the
                    # already-guarded sweeps below. Unguarded, it escapes into
                    # MaintenanceLeader.run's TaskGroup and on into the worker's,
                    # cancelling every sibling WITHOUT setting shutdown_event —
                    # the heartbeat dies before it can reach isolate_self, so no
                    # job is re-pended and the worker cannot exit cleanly.
                    iteration_clean = False
                    if _is_deadline_family(exc):
                        record_sweep_timeout("expired_locks")
                    log.warning(
                        "sweep-expired-locks-failed",
                        kind="sweep_expired_locks_failed",
                        worker_id=str(ctx.worker_id),
                        error=repr(exc),
                    )
                finally:
                    # rows_1 is bound only by the awaited call above; the
                    # deadline that aborts it also aborts the binding, so the
                    # failure path records duration WITHOUT a row sample (a
                    # 0-row sample would be indistinguishable from a healthy
                    # empty sweep).
                    _metric_duration("expired_locks", start)
                    if rows_1 is not None:
                        _metric_rows("expired_locks", rows_1)
                        record_sweep_success("expired_locks")
                        _dbg(
                            "sweep_expired_locks_tick",
                            "sweep_expired_locks_tick",
                            rows_1,
                            start,
                        )
                # Every batch commits, so a stopped drain is a pause, not a
                # rollback.
                if rows_1 and not await _drain_bounded(
                    ctx,
                    shutdown,
                    sweep_name="expired_locks",
                    call=sweep_1_call,
                    warn_event="sweep-expired-locks-failed",
                    warn_kind="sweep_expired_locks_failed",
                ):
                    iteration_clean = False

                # Sweep 2: deadline_sweep
                start = time.monotonic()
                rows_2: int | None = None
                try:
                    rows_2 = await sweep_2_call()
                except NotImplementedError as exc:
                    iteration_clean = False
                    if not warned_sweep_2:
                        _err("sweep_deadline_exceeded_unimplemented", _EK3, ctx.worker_id, exc)
                        warned_sweep_2 = True
                except TRANSIENT_PG_ERRORS as exc:
                    # Same transient-PG rationale as sweep 1 above.
                    iteration_clean = False
                    if _is_deadline_family(exc):
                        record_sweep_timeout("deadline_exceeded")
                    log.warning(
                        "sweep-deadline-exceeded-failed",
                        kind="sweep_deadline_exceeded_failed",
                        worker_id=str(ctx.worker_id),
                        error=repr(exc),
                    )
                finally:
                    # Same rows-bound-by-the-awaited-call discipline as
                    # sweep 1 above.
                    _metric_duration("deadline_exceeded", start)
                    if rows_2 is not None:
                        _metric_rows("deadline_exceeded", rows_2)
                        record_sweep_success("deadline_exceeded")
                        log.debug(
                            "sweep_deadline_exceeded_tick",
                            kind="deadline_exceeded_sweep",
                            count=rows_2,
                            sweep_duration_ms=int((time.monotonic() - start) * 1000),
                        )
                # Every batch commits, so a stopped drain is a pause, not a
                # rollback.
                if rows_2 and not await _drain_bounded(
                    ctx,
                    shutdown,
                    sweep_name="deadline_exceeded",
                    call=sweep_2_call,
                    warn_event="sweep-deadline-exceeded-failed",
                    warn_kind="sweep_deadline_exceeded_failed",
                ):
                    iteration_clean = False
                if hasattr(ctx.backend, "sweep_leaked_reservation_slots"):
                    start = time.monotonic()
                    rows_4: int | None = None
                    try:
                        rows_4 = await leaked_slots_call()
                    except TRANSIENT_PG_ERRORS as exc:
                        iteration_clean = False
                        if _is_deadline_family(exc):
                            record_sweep_timeout("leaked_slots")
                        log.warning(
                            "sweep-leaked-slots-failed",
                            kind="sweep_leaked_slots_failed",
                            worker_id=str(ctx.worker_id),
                            error=repr(exc),
                        )
                    finally:
                        # rows_4 is bound only by the awaited call above;
                        # the deadline that aborts it also aborts the
                        # binding, so the failure path records duration
                        # WITHOUT a row sample — same discipline as sweeps
                        # 1/2: a 0-row sample would be indistinguishable
                        # from a healthy empty sweep, and success-path-only
                        # instrumentation is the original invisibility.
                        _metric_duration("leaked_slots", start)
                        if rows_4 is not None:
                            _metric_rows("leaked_slots", rows_4)
                            record_sweep_success("leaked_slots")
                            _dbg(
                                "sweep_leaked_slots_tick",
                                "sweep_leaked_slots_tick",
                                rows_4,
                                start,
                            )
                    # Every batch commits, so a stopped drain is a pause,
                    # not a rollback — the same drain-to-zero-within-a-tick
                    # wiring as sweeps 1/2/rt.
                    if rows_4 and not await _drain_bounded(
                        ctx,
                        shutdown,
                        sweep_name="leaked_slots",
                        call=leaked_slots_call,
                        warn_event="sweep-leaked-slots-failed",
                        warn_kind="sweep_leaked_slots_failed",
                    ):
                        iteration_clean = False
                    # Result TTL expiry: one bounded batch per call, drained
                    # like sweeps 1/2 — every batch commits, so a stopped
                    # drain is a pause, not a rollback.
                    start = time.monotonic()
                    rows_rt: int | None = None
                    try:
                        rows_rt = await sweep_rt_call()
                    except TRANSIENT_PG_ERRORS as exc:
                        iteration_clean = False
                        if _is_deadline_family(exc):
                            record_sweep_timeout("expired_results")
                        log.warning(
                            "sweep-expired-results-failed",
                            kind="sweep_expired_results_failed",
                            worker_id=str(ctx.worker_id),
                            error=repr(exc),
                        )
                    finally:
                        # Same rows-bound-by-the-awaited-call discipline as
                        # sweep 1 above: a timed-out sweep records duration
                        # WITHOUT a row sample.
                        _metric_duration("expired_results", start)
                        if rows_rt is not None:
                            _metric_rows("expired_results", rows_rt)
                            record_sweep_success("expired_results")
                            _dbg(
                                "sweep_expired_results_tick",
                                "sweep_expired_results_tick",
                                rows_rt,
                                start,
                            )
                    if rows_rt and not await _drain_bounded(
                        ctx,
                        shutdown,
                        sweep_name="expired_results",
                        call=sweep_rt_call,
                        warn_event="sweep-expired-results-failed",
                        warn_kind="sweep_expired_results_failed",
                    ):
                        iteration_clean = False
                    # Event retention: ONE committed batch per tick,
                    # deliberately NOT a _drain_bounded drain. The design
                    # settles slow-and-constant: at the default 30 s
                    # sweep_interval and 10 000-row batch that is ~333
                    # deletions/s against the measured ~200 events/s
                    # steady insert rate, so a 6.5 M-row backlog (a
                    # retention reduction from 30 d to 7 d) drains in ~5.4 h
                    # idle / ~13.6 h loaded — every batch committed, every
                    # tick short, and the tick's cost stays independent of
                    # the backlog it is recovering from. hasattr gate like
                    # the stale-batches block below: only PostgresBackend
                    # implements these maintenance sweeps. The period gate
                    # is the settings-level disable sentinel: timedelta(0)
                    # disables the sweep, and a disabled sweep acquires no
                    # connection and logs nothing.
                    if hasattr(
                        ctx.backend, "sweep_expired_events"
                    ) and ctx.deps.settings.event_retention_period > timedelta(0):
                        start = time.monotonic()
                        rows_er: int | None = None
                        try:
                            async with ctx.deps.dispatcher_pool.acquire(
                                timeout=ctx.deps.settings.dispatcher_command_timeout
                            ) as conn:
                                rows_er = cast(
                                    "int",
                                    await ctx.backend.sweep_expired_events(  # type: ignore[reportAttributeAccessIssue]  # Why: guarded by the hasattr gate above; only PostgresBackend implements these maintenance sweeps.
                                        conn,
                                        schema=ctx.deps.settings.schema_name,
                                        retention=ctx.deps.settings.event_retention_period,
                                        batch_size=ctx.deps.settings.event_retention_batch_size,
                                    ),
                                )
                        except TRANSIENT_PG_ERRORS as exc:
                            # Same transient-PG rationale as the sibling
                            # sweeps above.
                            iteration_clean = False
                            if _is_deadline_family(exc):
                                record_sweep_timeout("job_events_retention")
                            log.warning(
                                "sweep-job-events-retention-failed",
                                kind="sweep_job_events_retention_failed",
                                worker_id=str(ctx.worker_id),
                                error=repr(exc),
                            )
                        finally:
                            # Same rows-bound-by-the-awaited-call discipline
                            # as sweeps 1/2/rt: a timed-out sweep records
                            # duration WITHOUT a row sample (a 0-row sample
                            # would be indistinguishable from a healthy
                            # empty sweep, and success-path-only
                            # instrumentation is the original invisibility).
                            _metric_duration("job_events_retention", start)
                            if rows_er is not None:
                                _metric_rows("job_events_retention", rows_er)
                                record_sweep_success("job_events_retention")
                                _dbg(
                                    "job_events_retention_tick",
                                    "job_events_retention_tick",
                                    rows_er,
                                    start,
                                )
                    # Fleet-wide keyed-row reclaim: ONE bounded, committed
                    # batch per table per tick, deliberately NOT a
                    # _drain_bounded drain — the same slow-and-constant
                    # discipline the event-retention block above settled.
                    # This is the fleet half of keyed reclamation: the
                    # per-worker eviction+drain below only ever names rows
                    # its OWN process materialised, so keyed
                    # reservation_slots / rate_limit_buckets rows orphan
                    # when the worker that created them dies (the
                    # residual the keyed-row lifecycle exists to close).
                    # The rows carry their own staleness — the
                    # keyed mark plus last_used_at, refreshed by the
                    # acquire/release/upsert statements that already touch
                    # them — and sweep_idle_keyed_rows deletes marked rows
                    # unused past the horizon, bounded per tick (static
                    # buckets and redis-backend keyed rows are never
                    # marked, never deleted). hasattr gate like the
                    # retention block above: only PostgresBackend
                    # implements this maintenance sweep. The period gate is
                    # the settings-level disable sentinel: timedelta(0)
                    # disables the sweep, and a disabled sweep acquires no
                    # connection and logs nothing. UndefinedColumnError
                    # rides the except below (pre-migration tolerance, the
                    # stale-batches block's pattern): a rolling deploy runs
                    # this code against a schema whose keyed/last_used_at
                    # columns have not landed yet, which is a per-tick warn
                    # until migration 01.00.10_02 (keyed_row_fleet_reclaim)
                    # applies — not the
                    # deliberately-fatal unexpected-error streak.
                    if hasattr(
                        ctx.backend, "sweep_idle_keyed_rows"
                    ) and ctx.deps.settings.keyed_row_reclaim_period > timedelta(0):
                        start = time.monotonic()
                        rows_kr: int | None = None
                        try:
                            async with ctx.deps.dispatcher_pool.acquire(
                                timeout=ctx.deps.settings.dispatcher_command_timeout
                            ) as conn:
                                rows_kr = cast(
                                    "int",
                                    await ctx.backend.sweep_idle_keyed_rows(  # type: ignore[reportAttributeAccessIssue]  # Why: guarded by the hasattr gate above; only PostgresBackend implements these maintenance sweeps.
                                        conn,
                                        schema=ctx.deps.settings.schema_name,
                                        horizon=ctx.deps.settings.keyed_row_reclaim_period,
                                        batch_size=ctx.deps.settings.keyed_row_reclaim_batch_size,
                                    ),
                                )
                        except (
                            *TRANSIENT_PG_ERRORS,
                            asyncpg.exceptions.UndefinedColumnError,
                        ) as exc:
                            # Same transient-PG rationale as the sibling
                            # sweeps above, plus the pre-migration
                            # UndefinedColumnError tolerance (see the block
                            # comment).
                            iteration_clean = False
                            if _is_deadline_family(exc):
                                record_sweep_timeout("keyed_row_reclaim")
                            log.warning(
                                "sweep-keyed-row-reclaim-failed",
                                kind="sweep_keyed_row_reclaim_failed",
                                worker_id=str(ctx.worker_id),
                                error=repr(exc),
                            )
                        finally:
                            # Same rows-bound-by-the-awaited-call discipline
                            # as every sibling sweep above: duration always,
                            # rows and the success stamp only when the call
                            # returned.
                            _metric_duration("keyed_row_reclaim", start)
                            if rows_kr is not None:
                                _metric_rows("keyed_row_reclaim", rows_kr)
                                record_sweep_success("keyed_row_reclaim")
                                _dbg(
                                    "keyed_row_reclaim_tick",
                                    "keyed_row_reclaim_tick",
                                    rows_kr,
                                    start,
                                )
                    # Stale-worker cleanup: one bounded batch per call,
                    # drained the same way. The window bounds workers per
                    # call, which bounds the DDL ON DELETE fan-out (the
                    # job_attempts SET NULL rewrites) per transaction.
                    start = time.monotonic()
                    rows_sr: int | None = None
                    try:
                        rows_sr = await stale_workers_call()
                    except TRANSIENT_PG_ERRORS as exc:
                        iteration_clean = False
                        if _is_deadline_family(exc):
                            record_sweep_timeout("stale_workers")
                        log.warning(
                            "cleanup-stale-workers-failed",
                            kind="cleanup_stale_workers_failed",
                            worker_id=str(ctx.worker_id),
                            error=repr(exc),
                        )
                    finally:
                        _metric_duration("stale_workers", start)
                        if rows_sr is not None:
                            _metric_rows("stale_workers", rows_sr)
                            record_sweep_success("stale_workers")
                            _dbg(
                                "cleanup_stale_workers_tick",
                                "cleanup_stale_workers_tick",
                                rows_sr,
                                start,
                            )
                    if rows_sr and not await _drain_bounded(
                        ctx,
                        shutdown,
                        sweep_name="stale_workers",
                        call=stale_workers_call,
                        warn_event="cleanup-stale-workers-failed",
                        warn_kind="cleanup_stale_workers_failed",
                    ):
                        iteration_clean = False
                # Stale-batch completion is a LEADER sweep (docs/guides/workers.md;
                # docs/architecture.md "``complete_stale_batches`` leader sweep"):
                # batches whose completion hook was lost (consumer crash between
                # the terminal write and complete_batch/abort_batch) stay `active`
                # forever without it — wait_for_batch can snooze indefinitely and
                # prune_old_batches only deletes completed rows. Deliberately NOT
                # nested under the keyed-registry conditions below: those are
                # process-local and, in the default deployment, empty.
                # hasattr guard: complete_stale_batches needs a real PG connection
                # (dispatcher_pool). InMemoryBackend does not implement
                # sweep_leaked_reservation_slots, so this gate keeps the sweep off
                # the in-memory backend — same pattern as the block above.
                if hasattr(ctx.backend, "sweep_leaked_reservation_slots"):
                    start = time.monotonic()
                    stale_rows: int | None = None
                    try:
                        stale_rows = await stale_batches_call()
                        if stale_rows:
                            log.info("stale-batches-completed", kind="batch", count=stale_rows)
                        # Drain the remainder within this tick, the same
                        # _drain_bounded wiring as sweeps 1/2/rt: one
                        # bounded call per tick left a large stale set
                        # draining at one batch per sweep_interval, while
                        # every sibling sweep drains to zero per tick.
                        # UndefinedTableError rides the outer except below
                        # (pre-migration tolerance) — _drain_bounded's own
                        # transient set deliberately does not carry it.
                        if stale_rows and not await _drain_bounded(
                            ctx,
                            shutdown,
                            sweep_name="stale_batches",
                            call=stale_batches_call,
                            warn_event="stale-batches-sweep-failed",
                            warn_kind="batch",
                        ):
                            iteration_clean = False
                    except (
                        *TRANSIENT_PG_ERRORS,
                        asyncpg.exceptions.UndefinedTableError,
                    ) as exc:
                        # Why the shared transient set (plus this block's
                        # local UndefinedTableError tolerance for
                        # pre-migration deployments): the hand-rolled tuple
                        # here predated the shared set and missed
                        # QueryCanceledError — a server-side cancel of the
                        # (now batched) completion statement escaped to the
                        # unexpected-error backstop as though it were a
                        # bug, counting toward the deliberately-fatal
                        # streak.
                        iteration_clean = False
                        if _is_deadline_family(exc):
                            record_sweep_timeout("stale_batches")
                        log.warning("stale-batches-sweep-failed", kind="batch", error=repr(exc))
                    finally:
                        # Same rows-bound-by-the-awaited-call discipline as
                        # every sibling sweep above: duration always, rows
                        # and the success stamp only when the call
                        # returned.
                        _metric_duration("stale_batches", start)
                        if stale_rows is not None:
                            _metric_rows("stale_batches", stale_rows)
                            record_sweep_success("stale_batches")
                if iteration_clean:
                    guard.ok()
            except Exception as exc:
                # Backstop (see _transient.py): a non-transient error here is
                # a bug, not a PG moment — loud, counted, tolerated briefly,
                # then deliberately fatal. Pre-fix it escaped straight into
                # MaintenanceLeader.run's TaskGroup and tore down the whole
                # worker on the first hit.
                guard.unexpected(exc)
        # Keyed-primitive eviction is process-local bookkeeping, NOT
        # leader-gated: every worker sweeps its OWN registry each tick (a
        # non-leader's registry would otherwise receive no periodic
        # eviction). Always safe to call; with the singleton default this
        # is a no-op behavior change — N workers idempotently evict the
        # same shared registry (in a multi-process fleet each process has
        # its OWN singleton copy, so non-leader processes previously got
        # NO periodic eviction and now sweep their own copy).
        # ctx.rate_limit_registry is None for direct SweepContext
        # constructions → fall back to the module singleton when None.
        rl = ctx.rate_limit_registry if ctx.rate_limit_registry is not None else rl_registry
        if rl.has_keyed_reservations:
            try:
                # Why max_pending_reclaims: the pending-reclaim set's cap
                # tracks the operator's keyed-reservation ceiling — the
                # constant fallback (10 000) would let pending grow far
                # past a deliberately small setting while the tracked
                # entries themselves are capped at it.
                evicted = rl.evict_idle_keyed_reservations(
                    idle_for=_KEYED_IDLE_THRESHOLD,
                    max_pending_reclaims=ctx.deps.settings.max_keyed_reservations,
                )
                if evicted:
                    log.debug(
                        "sweep-evicted-idle-keyed-reservations",
                        kind="evict_idle_keyed_reservations",
                        count=evicted,
                    )
            except Exception as exc:
                log.warning(
                    "sweep-evict-idle-keyed-reservations-failed",
                    kind="evict_idle_keyed_reservations_failed",
                    worker_id=str(ctx.worker_id),
                    error=repr(exc),
                )
        if rl.has_keyed_rate_limits:
            try:
                # Why max_pending_reclaims: the same settings-derived bound
                # the reservation eviction above applies and the rate-limit
                # opportunistic eviction already applies — the pending set
                # mirrors the tracked entries, so the constant fallback
                # (10 000) would let pending grow far past a deliberately
                # small max_keyed_rate_limits while the tracked entries
                # themselves are capped at it.
                evicted = rl.evict_idle_keyed_rate_limits(
                    idle_for=_KEYED_IDLE_THRESHOLD,
                    max_pending_reclaims=ctx.deps.settings.max_keyed_rate_limits,
                )
                if evicted:
                    log.debug(
                        "sweep-evicted-idle-keyed-rate-limits",
                        kind="evict_idle_keyed_rate_limits",
                        count=evicted,
                    )
            except Exception as exc:
                log.warning(
                    "sweep-evict-idle-keyed-rate-limits-failed",
                    kind="evict_idle_keyed_rate_limits_failed",
                    worker_id=str(ctx.worker_id),
                    error=repr(exc),
                )
        # The evictions above drop registry ENTRIES only; the evicted
        # buckets' reservation_slots rows are deleted by the drain, on
        # this same non-leader-gated path (the pending set is this
        # process's own evictions, and the keyed machinery only runs on
        # workers that dispatch jobs). Nothing pending → no connection
        # acquired. The pool wait is bounded by the dispatcher command
        # timeout, the loop's convention for every pool acquire here.
        # The drain records its own failure/duration/rows metrics and
        # raises on failure; this guard (same shape as the eviction
        # blocks above) keeps a transient PG blip from tearing down the
        # sweep loop — the next tick retries with the pending set intact.
        if rl.has_pending_reservation_reclaims:
            try:
                drained = await rl.drain_pending_reservation_reclaims(
                    ctx.deps.dispatcher_pool,
                    acquire_timeout=ctx.deps.settings.dispatcher_command_timeout,
                )
                if drained:
                    log.debug(
                        "sweep-drained-pending-reservation-reclaims",
                        kind="drain_pending_reservation_reclaims",
                        count=drained,
                    )
            except Exception as exc:
                log.warning(
                    "sweep-drain-pending-reservation-reclaims-failed",
                    kind="drain_pending_reservation_reclaims_failed",
                    worker_id=str(ctx.worker_id),
                    error=repr(exc),
                )
        await _sleep_interruptible(shutdown, ctx.deps.settings.sweep_interval)


#: The session-level advisory lock unlock for the cron-driven maintenance
#: loops (prune / archive-expiry) — the hashtextextended key convention
#: their ``pg_try_advisory_lock`` acquires use.
_ADVISORY_UNLOCK_SQL = "SELECT pg_advisory_unlock(hashtextextended($1, 0))"

#: The connection-gone family of the transient set. An unlock failing this
#: way resolved itself: the session died and took its session-scoped locks
#: with it, so nothing is stranded — unlike a cancel or client-side
#: timeout, which leaves the session alive and still holding the lock.
_SESSION_GONE_ERRORS: Final[tuple[type[BaseException], ...]] = (
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
    OSError,
)


async def _release_session_lock(conn: ConnLike, lock_name: str, *, kind: str) -> None:
    """Release a session-level advisory lock on *conn*, loudly.

    A session advisory lock outlives transactions and dies only with its
    session, so an unlock failure on a connection that returns to the pool
    strands the fleet's lock until that session is recycled — every later
    attempt on any pod reads as lock-held for the whole retention horizon.
    The release is therefore never a silent suppress: the plain unlock
    statement runs first (one round trip, the pre-existing happy path),
    and a transient failure of it — a server-side cancel or a client-side
    command timeout landing mid-unlock — is warned about and recovered by
    re-issuing the unlock while READING ``pg_advisory_unlock``'s boolean
    verdict. The verdict is what makes the retry a recovery rather than a
    second guess: after a canceled or timed-out statement the attempt's
    effect is unknown (a client-side timeout can race the statement's
    completion server-side), and only the verdict settles whether the lock
    is still held — True, the retry released it; False, this session no
    longer holds it because the raced attempt already had. A retry failing
    with the connection-gone family resolved itself (the session died with
    its locks); any other retry failure leaves the release unconfirmed on
    a live session and is logged as an error naming the strand, an
    operator-visible condition instead of a silent one. Errors outside the
    transient set propagate unchanged — the loops' loud-crash doctrine for
    non-transient surprises.
    """
    try:
        await conn.execute(_ADVISORY_UNLOCK_SQL, lock_name)
        return
    except TRANSIENT_PG_ERRORS as exc:
        log.warning(
            "advisory-unlock-attempt-failed",
            kind=kind,
            lock=lock_name,
            error=repr(exc),
        )
    try:
        released = await conn.fetchval(_ADVISORY_UNLOCK_SQL, lock_name)
    except _SESSION_GONE_ERRORS:
        log.warning(
            "advisory-unlock-session-gone",
            kind=kind,
            lock=lock_name,
        )
        return
    except TRANSIENT_PG_ERRORS as exc:
        log.error(
            "advisory-unlock-unconfirmed",
            kind=kind,
            lock=lock_name,
            error=repr(exc),
        )
        return
    if released is None:
        # No verdict came back (a driver shape that returned no row): the
        # release is unconfirmed, not recovered.
        log.error(
            "advisory-unlock-unconfirmed",
            kind=kind,
            lock=lock_name,
            error="pg_advisory_unlock returned no verdict",
        )
        return
    log.info(
        "advisory-unlock-recovered",
        kind=kind,
        lock=lock_name,
        released_by_retry=released is True,
    )


async def _prune_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Daily prune with intra-day retry on failure.

    The once-per-SUCCESSFUL-prune-per-day guard (``last_pruned_date``) is
    deliberate policy: a day that pruned is done. The failure half
    retries with backoff (60 s doubling, capped) until success or the
    next scheduled fire, so a prune that keeps failing under load does
    not wait for tomorrow — see ``_PRUNE_RETRY_BACKOFF_INITIAL_SECS``.
    Every batch is a committed, server-side-bounded statement
    (:func:`~taskq.worker._leader_shared.prune_terminal_jobs`), and the
    drain stops between batches on shutdown.
    """
    last_pruned_date: date | None = None
    retry_backoff: float | None = None
    lock_name = schema_lock_name("prune", ctx.deps.settings.schema_name)
    # Loop-invariant policy, built once for the process lifetime: the
    # breaker's latch state must outlive any single attempt (a database
    # that needed smaller bites yesterday needs them today — the one-way
    # latch is the point), and the batch bound derives from the pool the
    # batches run on (see _PRUNE_TIMEOUT_FRACTION).
    prune_sizer = SweepBatchSizer(
        default_size=ctx.deps.settings.prune_batch_size,
        divisor=ctx.deps.settings.event_writer_reduced_batch_divisor,
        failure_threshold=ctx.deps.settings.sweep_breaker_failure_threshold,
        window_secs=ctx.deps.settings.sweep_breaker_window_secs,
    )
    statement_timeout_ms = _prune_statement_timeout_ms(ctx.deps.settings.dispatcher_command_timeout)
    drain_gate = _batch_drain_gate(
        ctx,
        shutdown,
        loop_name="leader.prune",
        period_secs=statement_timeout_ms / 1000.0,
    )

    while not shutdown.is_set():
        now_utc = datetime.now(UTC)
        cron_expr = ctx.deps.settings.prune_cron_expr or _schedule_utc_to_cron(
            ctx.deps.settings.prune_schedule_utc
        )
        it = cr.croniter(cron_expr, now_utc)
        next_fire: datetime = it.get_next(datetime).replace(tzinfo=UTC)

        woke_for_retry = await _sleep_until_next_attempt(shutdown, next_fire, retry_backoff)
        if shutdown.is_set():
            break
        if not woke_for_retry:
            # The scheduled fire governs this wake: a failure below starts
            # a fresh backoff sequence, not a continuation of the previous
            # one's capped rung.
            retry_backoff = None

        if not ctx.deps.leading():
            # A wake that finds the pod leaderless is a MISSED fire, not a
            # done day: the loop top recomputes the next fire from the cron
            # expression, and for a daily cron that is tomorrow, so a
            # seconds-scale leadership flap spanning the fire second would
            # silently defer retention work by 24 hours. The failure half's
            # backoff ladder covers the missed half too — each leaderless
            # wake advances the rung, so the retry cadence during a
            # sustained flap is bounded, and the first wake with leadership
            # back lands the attempt within the day.
            retry_backoff = _next_retry_backoff(retry_backoff)
            log.warning(
                "prune-fire-missed-leaderless",
                kind="prune",
                worker_id=str(ctx.worker_id),
                retry_in_secs=retry_backoff,
            )
            continue
        today_utc = datetime.now(UTC).date()
        if last_pruned_date == today_utc:
            # The day's prune is done, so a ladder armed by an earlier miss
            # or failure has nothing left to retry; clearing it here keeps
            # the loop from waking at the rung cadence until the next fire.
            retry_backoff = None
            continue

        try:
            async with ctx.deps.dispatcher_pool.acquire(
                timeout=ctx.deps.settings.dispatcher_command_timeout
            ) as conn:
                lock_acquired: bool = await conn.fetchval(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
                )
                if not lock_acquired:
                    # The losing side is the detector: a sustained contention
                    # rate equal to the attempt rate means this worker never
                    # prunes at all.
                    record_lock_contention(lock_name)
                    log.warning(
                        "prune-skipped-advisory-lock-held",
                        kind="prune",
                        worker_id=str(ctx.worker_id),
                        lock=lock_name,
                    )
                    continue

                attempt_start = time.monotonic()
                try:
                    retention_per_status = _build_retention_per_status(ctx.deps.settings)
                    actor_overrides = await _load_actor_retention_overrides(
                        conn, schema=ctx.deps.settings.schema_name
                    )
                    result = await prune_terminal_jobs(
                        conn,
                        retention_per_status=retention_per_status,
                        archive_retention=ctx.deps.settings.archive_retention_period,
                        schema=ctx.deps.settings.schema_name,
                        actor_overrides=actor_overrides if actor_overrides else None,
                        drain_gate=drain_gate,
                        statement_timeout_ms=statement_timeout_ms,
                        sizer=prune_sizer,
                    )
                    last_pruned_date = today_utc
                    retry_backoff = None
                    for status, count in result.by_status.items():
                        log.info(
                            "prune-completed",
                            kind="prune",
                            status=status,
                            count=count,
                            cutoff=result.cutoffs[status].isoformat(),
                            duration_ms=result.duration_ms,
                        )
                    # `cutoffs` carries one entry per terminal status, each
                    # anchored to the database clock inside
                    # prune_terminal_jobs, so the widest (most recent) of
                    # them is the batch cutoff: a batch is prunable once no
                    # status could still be holding a job for it.
                    max_cutoff = max(result.cutoffs.values())
                    try:
                        batch_count = await ctx.backend.prune_old_batches(max_cutoff)
                        if batch_count:
                            log.info("batches pruned", kind="batch", count=batch_count)
                    except (
                        NotImplementedError,
                        TimeoutError,
                        asyncpg.PostgresConnectionError,
                        asyncpg.InterfaceError,
                        asyncpg.exceptions.UndefinedTableError,
                        OSError,
                    ) as exc:
                        log.warning("batch-prune-failed", kind="batch", error=repr(exc))
                except Exception as exc:
                    # The failure half of the once-a-day policy: this
                    # attempt did NOT prune, so the day is not marked and
                    # the loop retries on the backoff ladder instead of
                    # sleeping to tomorrow's fire. Partially-drained
                    # batches are already committed; the retry resumes the
                    # remainder at the (possibly latched) reduced tier.
                    retry_backoff = _next_retry_backoff(retry_backoff)
                    if _is_deadline_family(exc):
                        record_sweep_timeout("prune")
                    log.error(
                        "prune-failed",
                        kind="prune",
                        error=repr(exc),
                        duration_ms=int((time.monotonic() - attempt_start) * 1000),
                        retry_in_secs=retry_backoff,
                    )
                finally:
                    # The drain's detector-2 registration is attempt-scoped
                    # (the gated-loop pattern — see _batch_drain_gate):
                    # forget it whether the attempt succeeded, failed, or
                    # stopped on shutdown, so the once-a-day loop cannot
                    # read as a stale sibling between attempts.
                    ctx.deps.liveness.forget("leader.prune")
                    # A session-scoped lock outlives this attempt's
                    # transactions and dies only with its session, so the
                    # release is loud and self-recovering (see
                    # _release_session_lock): a suppressed failure here
                    # would strand the fleet's prune lock on the pooled
                    # session until pool recycle.
                    await _release_session_lock(conn, lock_name, kind="prune")
        except TRANSIENT_PG_ERRORS as exc:
            # The lock attempt itself failed — same failure half, same
            # ladder (a PG blip at 03:00 must not defer the prune to
            # tomorrow).
            retry_backoff = _next_retry_backoff(retry_backoff)
            log.warning(
                "prune-lock-attempt-failed",
                kind="prune_lock_failed",
                worker_id=str(ctx.worker_id),
                error=repr(exc),
                retry_in_secs=retry_backoff,
            )


async def _archive_expiry_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Daily archive expiry with intra-day retry on failure — the same
    policy shape as :func:`_prune_loop` (once per successful attempt per
    day; failures retry on the shared backoff ladder).
    """
    last_expiry_date: date | None = None
    retry_backoff: float | None = None
    lock_name = schema_lock_name("archive_expiry", ctx.deps.settings.schema_name)
    # Same loop-invariant policy as _prune_loop: the latch outlives any
    # single attempt, and the batch bound derives from the dispatcher
    # pool's command timeout.
    expiry_sizer = SweepBatchSizer(
        default_size=ctx.deps.settings.prune_batch_size,
        divisor=ctx.deps.settings.event_writer_reduced_batch_divisor,
        failure_threshold=ctx.deps.settings.sweep_breaker_failure_threshold,
        window_secs=ctx.deps.settings.sweep_breaker_window_secs,
    )
    statement_timeout_ms = _prune_statement_timeout_ms(ctx.deps.settings.dispatcher_command_timeout)
    drain_gate = _batch_drain_gate(
        ctx,
        shutdown,
        loop_name="leader.archive_expiry",
        period_secs=statement_timeout_ms / 1000.0,
    )

    while not shutdown.is_set():
        now_utc = datetime.now(UTC)
        cron_expr = ctx.deps.settings.archive_expiry_cron_expr or _schedule_utc_to_cron(
            ctx.deps.settings.archive_expiry_schedule_utc
        )
        it = cr.croniter(cron_expr, now_utc)
        next_fire: datetime = it.get_next(datetime).replace(tzinfo=UTC)

        woke_for_retry = await _sleep_until_next_attempt(shutdown, next_fire, retry_backoff)
        if shutdown.is_set():
            break
        if not woke_for_retry:
            # The scheduled fire governs this wake: a failure below starts
            # a fresh backoff sequence (same rule as _prune_loop).
            retry_backoff = None

        if not ctx.deps.leading():
            # Same missed-fire contract as _prune_loop: the loop top would
            # recompute the next fire from the daily cron (tomorrow), so a
            # leadership flap at the fire second defers archive expiry by
            # a day unless the miss arms the backoff ladder.
            retry_backoff = _next_retry_backoff(retry_backoff)
            log.warning(
                "archive-expiry-fire-missed-leaderless",
                kind="archive_expiry",
                worker_id=str(ctx.worker_id),
                retry_in_secs=retry_backoff,
            )
            continue
        today_utc = datetime.now(UTC).date()
        if last_expiry_date == today_utc:
            # The day's expiry is done — same ladder-clearing rule as
            # _prune_loop's date gate.
            retry_backoff = None
            continue

        try:
            async with ctx.deps.dispatcher_pool.acquire(
                timeout=ctx.deps.settings.dispatcher_command_timeout
            ) as conn:
                lock_acquired: bool = await conn.fetchval(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                    lock_name,
                )
                if not lock_acquired:
                    # The losing side is the detector: a sustained contention
                    # rate equal to the attempt rate means this worker never
                    # expires archives at all.
                    record_lock_contention(lock_name)
                    log.warning(
                        "archive-expiry-skipped-advisory-lock-held",
                        kind="archive_expiry",
                        worker_id=str(ctx.worker_id),
                        lock=lock_name,
                    )
                    continue

                attempt_start = time.monotonic()
                try:
                    result = await archive_expiry_sweep(
                        conn,
                        schema=ctx.deps.settings.schema_name,
                        drain_gate=drain_gate,
                        statement_timeout_ms=statement_timeout_ms,
                        sizer=expiry_sizer,
                    )
                    last_expiry_date = today_utc
                    retry_backoff = None
                    for status, count in result.by_status.items():
                        log.info(
                            "archive-expiry-completed",
                            kind="archive_expiry",
                            status=status,
                            count=count,
                            expire_before=result.expire_before.isoformat(),
                            duration_ms=result.duration_ms,
                        )
                except Exception as exc:
                    # Same failure half as _prune_loop: retry on the ladder,
                    # never a second successful expiry in one day.
                    retry_backoff = _next_retry_backoff(retry_backoff)
                    if _is_deadline_family(exc):
                        record_sweep_timeout("archive_expiry")
                    log.error(
                        "archive-expiry-failed",
                        kind="archive_expiry",
                        error=repr(exc),
                        duration_ms=int((time.monotonic() - attempt_start) * 1000),
                        retry_in_secs=retry_backoff,
                    )
                finally:
                    # Attempt-scoped detector-2 registration, same as
                    # _prune_loop's forget.
                    ctx.deps.liveness.forget("leader.archive_expiry")
                    # Same session-scoped-lock discipline as _prune_loop's
                    # finally: loud, self-recovering release — a suppressed
                    # failure would strand the fleet's archive-expiry lock
                    # on the pooled session until pool recycle.
                    await _release_session_lock(conn, lock_name, kind="archive_expiry")
        except TRANSIENT_PG_ERRORS as exc:
            retry_backoff = _next_retry_backoff(retry_backoff)
            log.warning(
                "archive-expiry-lock-attempt-failed",
                kind="archive_expiry_lock_failed",
                worker_id=str(ctx.worker_id),
                error=repr(exc),
                retry_in_secs=retry_backoff,
            )


#: Live workers per subscribed queue. statement_timestamp() (STABLE) for
#: the liveness bound, the same two-clock rule as every sibling sampler: a
#: volatile bound cannot be a btree index condition on workers_last_seen_idx.
#: The window is the admin UI's own liveness setting
#: (``admin_worker_liveness_seconds``), so the gauge, the admin banner and
#: the stranded-jobs detector all agree on which worker counts as alive.
_QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE = (
    "SELECT q AS queue, count(*) AS count "
    'FROM "{schema}".workers w, unnest(w.queues) AS q '
    "WHERE w.last_seen_at > statement_timestamp() - make_interval(secs => $1) "
    "GROUP BY q"
)


async def _queue_depth_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Sample per-queue depth and per-queue live workers every
    ``queue_depth_interval`` on the leader.

    Both reads run on one connection in one tick so the two gauges can be
    joined on ``queue`` (``TaskQQueueUnserved``: depth with no live
    worker) without describing different moments.
    """
    schema = ctx.deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        # Why error, not warning: this returns, permanently muting the
        # sampler for the process lifetime while the worker keeps running
        # normally — a silent loss of a safety net, not a skipped tick
        # (same rationale as the stranded-jobs detector).
        log.error(
            "queue-depth-sampler-disabled",
            kind="queue_depth_sampler_disabled",
            schema=schema,
            reason="schema name failed identifier validation",
        )
        return
    sql = _QUERY_QUEUE_DEPTH_SQL_TEMPLATE.format(schema=schema)
    live_workers_sql = _QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE.format(schema=schema)
    liveness_secs = ctx.deps.settings.admin_worker_liveness_seconds
    while not shutdown.is_set():
        ctx.deps.liveness.tick("leader.queue_depth", period=ctx.deps.settings.queue_depth_interval)
        if ctx.deps.leading():
            try:
                async with ctx.deps.dispatcher_pool.acquire(
                    timeout=ctx.deps.settings.dispatcher_command_timeout
                ) as conn:
                    rows = await conn.fetch(sql)
                    worker_rows = await conn.fetch(live_workers_sql, liveness_secs)
                cache: dict[str, int] = {row["queue"]: row["count"] for row in rows}
                update_queue_depth_cache(cache)
                update_queue_live_workers_cache(
                    {str(row["queue"]): int(row["count"]) for row in worker_rows}
                )
            except Exception as exc:
                _sampler_read_failed(ctx, "queue_depth", "queue-depth-sampling-failed", exc)
        await _sleep_interruptible(shutdown, ctx.deps.settings.queue_depth_interval)


#: Jobs-by-status sample for the backlog detectors: one row per LIVE status
#: (``ACTIVE_STATUSES`` — pending/scheduled/running, the statemachine's
#: single source of truth for the live set), counted EXACTLY, and no
#: terminal statuses at all.
#:
#: Why exact for the live set: the series feeds
#: ``TaskQScheduledBacklogGrowing``, whose stalled-plateau arm is
#: ``changes(taskq_jobs_scheduled_count[5m]) == 0`` — a capped or sampled
#: count reads a constant once the backlog crosses the cap, so a bounded
#: estimate would fire the "stalled" alert continuously on a deep but
#: healthy backlog and stop tracking growth exactly when growth matters.
#: Exactness is the alert operand's requirement, so this is the one read
#: in the tick whose cost tracks the live backlog; each branch is served
#: index-only by its status's partial index (jobs_dispatch_idx /
#: jobs_scheduled_wake_idx / jobs_running_lock_expires_idx), which never
#: carries a terminal row.
#:
#: Why terminal statuses left the sample: the pre-bound shape was a
#: full-table ``GROUP BY status``, so every unpruned terminal row — the
#: one dimension that grows without bound between retention sweeps — was
#: re-counted by every worker every ``queue_depth_interval`` (this loop
#: is deliberately not leader-gated). And a terminal count from this
#: series was never a history truth anyway: pruned rows vanish from it,
#: so the tables and the admin surfaces carry that story. No alert
#: operand reads a terminal status from this gauge. The reader-visible
#: contract is "the live statuses, exact";
#: tests/test_jobs_by_status_sampler_history_bound.py pins both halves
#: (history-independent row work, exact live counts).
_QUERY_JOBS_BY_STATUS_SQL_TEMPLATE = " UNION ALL ".join(
    f"SELECT '{status}' AS status, count(*) AS count FROM \"{{schema}}\".jobs "  # noqa: S608  # Why: the only interpolations are {schema} (an identifier validated at WorkerSettings load and re-checked against _IDENT_RE in the loop before use) and the status literals from ACTIVE_STATUSES — the statemachine's fixed JobStatus vocabulary, never caller input.
    f"WHERE status = '{status}' HAVING count(*) > 0"
    for status in sorted(ACTIVE_STATUSES)
)
# statement_timestamp() (STABLE) — not clock_timestamp() (VOLATILE) — for
# the due bound, the same two-clock split as every sibling sampler: a
# volatile comparison cannot be a btree index condition, so the bound
# would degrade jobs_scheduled_wake_idx (partial on status='scheduled',
# keyed on scheduled_at) from an Index Cond that terminates at the
# boundary to a post-scan Filter walking the whole scheduled population —
# per worker, per interval, during the exact promotion-stall incident
# this gauge exists to expose. The EXTRACT(...) age it feeds is a
# measured value and stays clock_timestamp().
_QUERY_OLDEST_DUE_AGE_SQL_TEMPLATE = (
    "SELECT EXTRACT(EPOCH FROM (clock_timestamp() - MIN(scheduled_at)))::float8 "
    'FROM "{schema}".jobs '
    "WHERE status = 'scheduled' AND scheduled_at <= statement_timestamp()"
)
# statement_timestamp() (STABLE) — not clock_timestamp() (VOLATILE) — for the
# expiry bound, the same two-clock rule as the oldest-due bound above and the
# reclaim sweep's own predicate (backend/_sweeps.py): a volatile comparison
# cannot be a btree index condition, so the bound would degrade
# jobs_running_lock_expires_idx (partial on status='running', keyed on
# lock_expires_at) from an Index Cond that terminates at the boundary to a
# post-scan Filter walking the whole running population — per worker, per
# interval. The zombie count is the zombie-running detector: running
# rows with a past lease are invisible in jobs.by_status (a healthy running
# count) and in the miss counters (a dead worker emits nothing), and this one
# statement is the direct count.
#
# The cancel_phase = 0 carve-out: a row with a cancel in flight
# (cancel_phase != 0 — an operator asked to cancel, and the worker owns the
# terminal write) is in the cancellation protocol's own window, where an
# expired lease is EXPECTED, not a zombie — the reclaim sweep's lease arm
# deliberately waits cancel_grace + cleanup_grace + 60 s past lease expiry
# before it pre-empts one (backend/_sweeps.py's
# `cancel_phase = 0 OR lock_expires_at < now - <grace ladder>`), so a merely
# slow cancel is not mistaken for a crash. Counting those rows here made
# TaskQRunningLeaseExpired page on reclaim working exactly as designed: with
# the alert's 5-minute `for`, the gauge stays non-zero for roughly grace +
# 60 s plus a sweep interval and a sampling interval, and combined graces
# from about four minutes up crossed the firing line on every cancel. The
# filter keys on the PHASE, not on the sweep's grace ladder, so it stays
# correct whatever that ladder becomes — and a cancelling row that outlives
# the whole ladder is not lost: reclaim takes it (to 'cancelled', the
# caller's request honored) and the never-completing cancel has its own
# pager in TaskQAbandonedJobs. The term rides as a post-scan Filter over
# the expired-lease candidates the Index Cond already bounded — cancel_phase
# is NOT NULL DEFAULT 0, and count(*) had to visit those rows anyway.
_QUERY_RUNNING_LEASE_EXPIRED_SQL_TEMPLATE = (
    'SELECT count(*) FROM "{schema}".jobs '
    "WHERE status = 'running' AND lock_expires_at < statement_timestamp() "
    "AND cancel_phase = 0"
)
# Running jobs per actor, and the age of the oldest running attempt per
# actor, from ONE grouped read so count and age never describe two moments.
# The running population is bounded by the fleet's total concurrency, never
# by history, so an exact grouped read is cheap; jobs_running_lock_expires_idx
# (partial on status='running') serves it. Per actor, not (actor, queue): a
# running row's queue label is not what dispatched it (re-pended rows route
# by the actor's assignment), and the capacity question is which actors hold
# the slots. started_at is the attempt clock: an attempt older than the
# actor's normal runtime with taskq.jobs.timeouts flat is an actor with no
# start_to_close, which nothing else can show. The age is a measured value
# and stays on clock_timestamp().
_QUERY_RUNNING_BY_ACTOR_SQL_TEMPLATE = (
    "SELECT actor, count(*) AS running, "
    "EXTRACT(EPOCH FROM (clock_timestamp() - MIN(started_at)))::float8 AS oldest_age "
    'FROM "{schema}".jobs '
    "WHERE status = 'running' GROUP BY actor"
)
#: Per-pair sample cap for the actor-backlog sampler (rows read per
#: (actor, queue) pair per tick). The sampler runs on EVERY worker every
#: ``queue_depth_interval`` — deliberately not leader-gated (see
#: :func:`_backlog_detection_loop`) — so its cost must be independent of
#: the adopter's pending depth: an exact GROUP BY count is O(depth) with
#: no possible LIMIT (measured 10k→~1.6 ms/300 buffers, 100k→~13.5 ms/
#: 2668 buffers per worker per tick), which is the unbounded cost centre
#: tests/test_backlog_detection_sampler_depth_bound.py forbids. 1000 is
#: far above the per-pair depth any alert threshold keys on (the bundled
#: depth alert fires on oldest-pending AGE, which the head sample carries
#: exactly), and bounds one tick at (#pairs with pending work) x 1000 row
#: visits — deployment-shaped, never depth-shaped.
_ACTOR_BACKLOG_SAMPLE_CAP: Final[int] = 1000

# Depth and oldest-pending age per (actor, queue), from ONE grouped
# aggregate so the two can never describe different moments: depth alone
# is ambiguous (a deep queue that drains is healthy throughput) and age
# alone cannot say how much is waiting.
#
# The shape is bounded, not exact, by construction:
#
# * ``backlog_pairs`` enumerates the DISTINCT pending (actor, queue)
#   pairs with a recursive loose index scan over jobs_actor_dispatch_idx
#   — (actor, queue, priority DESC, scheduled_at, id) partial on
#   status='pending' — one bounded seek per pair (the same geometry as
#   the dispatch CTE's keys walks), so enumeration costs one visit per
#   pair, never one per row, and no pair's depth is ever walked. Every
#   pair appears, however small: an unconsumed actor whose backlog is
#   still shallow stays attributable (bounded enumeration does not
#   trade away small backlogs for the bound).
# * ``sampled`` reads each pair's first _ACTOR_BACKLOG_SAMPLE_CAP
#   pending rows in the index's own (priority DESC, scheduled_at, id)
#   order — an ordered index probe that stops at the cap, with no sort
#   and no heap access (scheduled_at rides the index). The probe order
#   is dispatch-head order: the rows a consumer would take first.
# * the outer GROUP BY aggregates the bounded sample, so one tick's row
#   work is Σ min(depth_pair, cap) + #pairs — flat at fixed pair counts
#   as the backlog grows (the depth-bound pin's oracle).
#
# Series semantics under the cap, which the gauge descriptions in
# docs/guides/ops.md state for operators: ``depth`` is exact below the
# cap and reads the cap at or above it (a lower bound, never an
# under-count); ``oldest_age`` is the age of the oldest row in the
# pair's dispatch-head sample — exactly the oldest pending row whenever
# the pair is below the cap or priorities are uniform, and in every case
# the head-of-line age an unconsumed actor grows without bound, which is
# the condition TaskQQueueDepthHigh fires on. scheduled_at, not
# created_at, is the waiting-since clock for a pending row: it is when
# the job became eligible, and it is the column the index already
# carries. PENDING, not scheduled: the unconsumed-actor condition is
# rows no consumer takes, distinct from scheduled rows awaiting
# promotion.
_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE = (
    "WITH RECURSIVE backlog_pairs AS ( "  # noqa: S608  # Why: the one non-placeholder interpolation is the module-level int cap baked in at import (the f-string LIMIT clause below); the only call-site interpolation is {schema}, an identifier validated at WorkerSettings load and re-checked against _IDENT_RE in the loop before use.
    "( "
    'SELECT j.actor, j.queue FROM "{schema}".jobs j '
    "WHERE j.status = 'pending' "
    "ORDER BY j.actor, j.queue "
    "LIMIT 1 "
    ") "
    "UNION ALL "
    "SELECT nxt.actor, nxt.queue "
    "FROM backlog_pairs cur "
    "CROSS JOIN LATERAL ( "
    'SELECT j2.actor, j2.queue FROM "{schema}".jobs j2 '
    "WHERE j2.status = 'pending' "
    "AND (j2.actor, j2.queue) > (cur.actor, cur.queue) "
    "ORDER BY j2.actor, j2.queue "
    "LIMIT 1 "
    ") nxt "
    "), "
    "sampled AS ( "
    "SELECT p.actor, p.queue, s.scheduled_at "
    "FROM backlog_pairs p "
    "CROSS JOIN LATERAL ( "
    'SELECT j3.scheduled_at FROM "{schema}".jobs j3 '
    "WHERE j3.status = 'pending' "
    "AND j3.actor = p.actor "
    "AND j3.queue = p.queue "
    "ORDER BY j3.priority DESC, j3.scheduled_at, j3.id "
    f"LIMIT {_ACTOR_BACKLOG_SAMPLE_CAP} "
    ") s "
    ") "
    "SELECT actor, queue, count(*) AS depth, "
    "EXTRACT(EPOCH FROM (clock_timestamp() - MIN(scheduled_at)))::float8 AS oldest_age "
    "FROM sampled "
    "GROUP BY actor, queue"
)


async def _backlog_detection_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Sample the backlog detectors (jobs-by-status, oldest due age,
    running-lease-expired, per-actor backlog) every ``queue_depth_interval``.

    Why NOT leader-gated, unlike every sibling sampler here: a detector
    hosted behind the leadership gate emits nothing under the very failure
    it exists to expose — another schema's worker holding the old-style
    lock, or any election loss, mutes every leader-gated sampler at once.
    Every worker samples instead, accepting N times the query cost, and the
    per-target series are deduplicated by the scrape target.

    The N-times cost is why every read in the tick must not grow with the
    wrong dimension. The by-status read counts only the LIVE statuses,
    exactly — the alert operands forbid a capped estimate, since a capped
    count reads a constant past its cap and false-fires the stalled-plateau
    arm of ``TaskQScheduledBacklogGrowing`` (see
    ``_QUERY_JOBS_BY_STATUS_SQL_TEMPLATE``) — through per-status partial
    indexes, so its cost tracks the live backlog and never touches the
    terminal history that made the pre-bound full-table GROUP BY grow
    without bound between retention sweeps. The per-actor read enumerates
    distinct pending (actor, queue) pairs with a loose index scan and
    probes each pair up to ``_ACTOR_BACKLOG_SAMPLE_CAP`` rows (see
    ``_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE``), so one tick's pair walk costs
    at most #pairs x cap row visits, never O(pending depth) per worker.
    """
    schema = ctx.deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        # Why error, not warning: same permanently-muted-sampler rationale
        # as _queue_depth_loop above — a silent loss of a safety net, not a
        # skipped tick.
        log.error(
            "backlog-detection-sampler-disabled",
            kind="backlog_detection_sampler_disabled",
            schema=schema,
            reason="schema name failed identifier validation",
        )
        return
    by_status_sql = _QUERY_JOBS_BY_STATUS_SQL_TEMPLATE.format(schema=schema)
    oldest_due_sql = _QUERY_OLDEST_DUE_AGE_SQL_TEMPLATE.format(schema=schema)
    expired_lease_sql = _QUERY_RUNNING_LEASE_EXPIRED_SQL_TEMPLATE.format(schema=schema)
    actor_backlog_sql = _QUERY_ACTOR_BACKLOG_SQL_TEMPLATE.format(schema=schema)
    running_by_actor_sql = _QUERY_RUNNING_BY_ACTOR_SQL_TEMPLATE.format(schema=schema)
    while not shutdown.is_set():
        ctx.deps.liveness.tick(
            "leader.backlog_detection", period=ctx.deps.settings.queue_depth_interval
        )
        try:
            # One connection for every statement: the gauges answer one
            # question (is work moving?) and must not straddle two
            # snapshots.
            async with ctx.deps.dispatcher_pool.acquire(
                timeout=ctx.deps.settings.dispatcher_command_timeout
            ) as conn:
                status_rows = await conn.fetch(by_status_sql)
                oldest_due: float | None = await conn.fetchval(oldest_due_sql)
                expired_lease: int | None = await conn.fetchval(expired_lease_sql)
                # Isolated like the per-actor backlog read below, and for the
                # same reason: a grouped per-actor read that fails must not
                # cost the tick its fleet-wide samples. The empty fallback
                # clears the running series rather than freezing it; the
                # rows are shaped here so a malformed row is contained too.
                try:
                    running_snapshot = [
                        (str(row["actor"]), int(row["running"]), row["oldest_age"])
                        for row in await conn.fetch(running_by_actor_sql)
                    ]
                except Exception as exc:
                    log.warning(
                        "running-by-actor-sampling-failed",
                        kind="running_by_actor_sampling_failed",
                        worker_id=str(ctx.worker_id),
                        error=repr(exc),
                    )
                    running_snapshot = []
                # This read is isolated from the fleet-wide ones above: a
                # grouped read over every pending (actor, queue) pair is
                # the widest-shaped statement in the tick and the first
                # to hit the statement timeout under the incident it
                # exists to expose (the pairs walk and the per-pair caps
                # bound its row visits — not its latency, when the engine
                # itself is stalling), and its failure must not cost the
                # tick the samples below — frozen scheduled-count /
                # oldest-due-age operands make the backlog-growing
                # comparison silently false while the loop stays alive.
                # The empty fallback clears the per-actor caches below
                # rather than freezing them at readings the worker can no
                # longer see — which is also why the failure must ride the
                # metric plane, not only the log: clearing the series
                # resolves TaskQQueueDepthHigh (its operand is the
                # oldest-pending age) at the exact moment the incident it
                # alerts on is killing the read, and a WARN line is not
                # alertable. The except below routes through
                # _sampler_read_failed so the failure counts on
                # taskq.maintenance_leader.sweep_timeouts under this
                # sampler's own sweep_name (TaskQSweepTimeouts): the tick
                # degrades visibly AND alertably, never silently.
                try:
                    actor_rows = await conn.fetch(actor_backlog_sql)
                except Exception as exc:
                    _sampler_read_failed(ctx, "actor_backlog", "actor-backlog-sampling-failed", exc)
                    actor_rows = []
            status_counts = {str(row["status"]): int(row["count"]) for row in status_rows}
            update_jobs_by_status_cache(status_counts)
            # Label-less twin of the "scheduled" row above — see
            # update_scheduled_count_cache's docstring for why it exists
            # separately from the by-status gauge.
            update_scheduled_count_cache(status_counts.get("scheduled", 0))
            # MIN(scheduled_at) over an empty set is NULL — nothing is due,
            # which the gauge expresses as 0.0, not as a missing sample.
            update_oldest_due_age_cache(oldest_due if oldest_due is not None else 0.0)
            # count(*) never returns NULL; the None arm only tolerates a
            # stubbed/aborted read, expressed as 0 for the same
            # not-a-missing-sample reason as the age above.
            update_running_lease_expired_cache(
                int(expired_lease) if expired_lease is not None else 0
            )
            # Rebuilt whole from the snapshot: an actor that finished its
            # last running job vanishes from both series instead of freezing.
            # MIN(started_at) is NULL only when every running row of the
            # actor has no started_at (a raced write); 0.0 says "no measured
            # age", never a missing sample.
            update_jobs_running_cache({actor: running for actor, running, _age in running_snapshot})
            update_actor_oldest_running_age_cache(
                {
                    actor: float(age) if age is not None else 0.0
                    for actor, _running, age in running_snapshot
                }
            )
            # Per-actor attribution is isolated end to end from the
            # fleet-wide detectors above, which are already written by this
            # point: a failed fetch substituted the empty snapshot (counted
            # on the actor sampler's own failure path) and a malformed row
            # is caught here, so neither costs the tick its promotion-stall
            # and zombie-running samples. An actor that drains to empty must
            # also stop reporting rather than freeze at its last depth, so
            # both caches are rebuilt whole from the snapshot and a
            # vanished (actor, queue) pair vanishes from the series instead
            # of ageing forever at a stale value.
            try:
                update_actor_backlog_cache(
                    {
                        (str(row["actor"]), str(row["queue"])): int(row["depth"])
                        for row in actor_rows
                    }
                )
                update_actor_oldest_pending_age_cache(
                    {
                        (str(row["actor"]), str(row["queue"])): float(row["oldest_age"] or 0.0)
                        for row in actor_rows
                    }
                )
            except Exception as exc:
                # Same failure surface as the fetch above, so same routing:
                # a malformed row leaves the per-actor caches stale exactly
                # as a failed fetch leaves them empty — either way this
                # read did not happen, and a read that did not happen is
                # the whole fault a detector must report (warnings are not
                # alertable). Counted under the sampler's own sweep_name,
                # never only logged.
                _sampler_read_failed(ctx, "actor_backlog", "actor-backlog-sampling-failed", exc)
        except Exception as exc:
            _sampler_read_failed(ctx, "backlog_detection", "backlog-detection-sampling-failed", exc)
        await _sleep_interruptible(shutdown, ctx.deps.settings.queue_depth_interval)


async def _reservation_slots_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    schema = ctx.deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        # Why error, not warning: same permanently-muted-sampler rationale
        # as _queue_depth_loop above.
        log.error(
            "reservation-slots-sampler-disabled",
            kind="reservation_slots_sampler_disabled",
            schema=schema,
            reason="schema name failed identifier validation",
        )
        return
    sql = _QUERY_RESERVATION_SLOTS_SQL_TEMPLATE.format(schema=schema)
    while not shutdown.is_set():
        ctx.deps.liveness.tick(
            "leader.reservation_slots", period=ctx.deps.settings.reservation_slots_interval
        )
        if ctx.deps.leading():
            try:
                async with ctx.deps.dispatcher_pool.acquire(
                    timeout=ctx.deps.settings.dispatcher_command_timeout
                ) as conn:
                    rows = await conn.fetch(sql)
                cache: dict[str, int] = {row["bucket_name"]: row["count"] for row in rows}
                update_reservation_slots_cache(cache)
            except Exception as exc:
                _sampler_read_failed(
                    ctx, "reservation_slots", "reservation-slots-sampling-failed", exc
                )
        await _sleep_interruptible(shutdown, ctx.deps.settings.reservation_slots_interval)


async def _stranded_jobs_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Periodically surface pending/scheduled jobs that can never dispatch.

    Two strand shapes, one gauge (actor -> stranded row count):

    - **no actor_config row**: dispatch derives its candidate set from
      ``per_actor_capacity``, which is ``FROM actor_config``, so a row
      whose actor has no config row is invisible to every dispatch round.
    - **unserved queue**: dispatch probes only the queues in the
      dispatching worker's subscription (the candidates lateral's
      ``j2.queue = sq.queue_name`` annihilates every other pair), so a
      row on a queue NO registered worker serves is invisible fleet-wide
      while its actor_config row exists and no deadline can fail it.
      The queue tested is the one dispatch would actually route on, which
      for a re-pended row (``pending`` with ``started_at`` set) is its
      actor's stored assignment rather than the label the row carries —
      that label survives only as an audit trail of where the row was
      first placed. Testing the label instead reports healthy for exactly
      the strand a queue move leaves behind when the target queue's
      consumers were never started: the rows are pending and due, and
      every label-keyed surface names a queue the fleet does serve.

    The unserved-queue predicate is fleet-wide by construction: the
    ``workers`` table carries every registered worker's subscription, and
    a worker row counts as serving only while its ``last_seen_at`` is
    inside the liveness window, so a fleet-wide restart does not read as
    stranded once the restarting workers re-register, while a queue whose
    every subscriber has gone quiet reads unserved immediately instead of
    hiding behind a dead row until the stale-worker sweep prunes it.

    Off the hot dispatch path — runs every 60 s when this worker is leader.
    """
    # One scan over the pending/scheduled set, each row's strand
    # conditions computed once in the inner SELECTs and the outer WHERE
    # admitting exactly the stranded rows. The per-condition FILTER
    # counts (and the queue names on the unserved condition) exist so
    # each warning event can say WHICH condition held and, for the
    # unserved shape, on which queues — an operator who sees a
    # no-actor-config event and finds the actor_config row present
    # concludes the detector lies, which is the exact failure a
    # per-shape event prevents.
    # A re-pended row (assignment_routed) is not dispatched by its own
    # queue label -- it routes by its actor's CURRENT stored assignment
    # (the routing contract in taskq/backend/_dispatch_sql.py). Testing
    # such a row's label against `workers` asks the detector's question
    # about the wrong queue: the label can name a queue the fleet serves
    # while the row is only claimable by the assignment queue nobody
    # runs. routing_queue IS dispatch's own discriminator — the
    # assignment for a re-pended row, the row's own label otherwise — so
    # the queue the unserved arm TESTS and the queue the event NAMES are
    # one expression, never two that can drift.
    # The two shapes are mutually exclusive: with no config row there is
    # no assignment to route by (the CASE yields NULL, and NULL = ANY(...)
    # would report the row unserved no matter what the fleet serves), so
    # the unserved arm is evaluated only when the config row exists. A
    # config-missing row counts once, in the config category — naming a
    # queue for it would point the operator at a label dispatch never
    # reads. "Serves" means a LIVE worker subscribes to the queue: a
    # dead-but-unswept worker row would otherwise hide an unserved queue
    # for the whole stale-worker sweep window.
    _stranded_sql = """\
    SELECT s.actor,
           count(*) AS cnt,
           count(*) FILTER (WHERE s.no_actor_config) AS no_actor_config_cnt,
           count(*) FILTER (WHERE s.unserved_queue) AS unserved_queue_cnt,
           coalesce(
             array_agg(DISTINCT s.routing_queue) FILTER (WHERE s.unserved_queue),
             ARRAY[]::text[]
           ) AS unserved_queues
    FROM (
        SELECT r.actor,
               r.routing_queue,
               r.no_actor_config,
               NOT r.no_actor_config
                 AND NOT EXISTS (
                   SELECT 1 FROM "{schema}".workers w
                   WHERE r.routing_queue = ANY(w.queues)
                     -- A worker row whose heartbeat has gone stale is not
                     -- dispatching; until the stale-worker sweep removes
                     -- it, it must not count as serving the queue.
                     -- statement_timestamp() (STABLE), the two-clock rule
                     -- every sampler follows; the window is the admin UI's
                     -- own liveness setting.
                     AND w.last_seen_at > statement_timestamp() - make_interval(secs => $1)
                 ) AS unserved_queue
        FROM (
            SELECT j.actor,
                   -- The queue dispatch routes on: the actor's stored
                   -- assignment for a re-pended row, the row's own label
                   -- otherwise — the assignment_routed marker is the
                   -- single discriminator (the routing contract in
                   -- taskq/backend/_dispatch_sql.py).
                   CASE WHEN j.assignment_routed THEN ac.queue ELSE j.queue END
                     AS routing_queue,
                   NOT EXISTS (
                     SELECT 1 FROM "{schema}".actor_config ac2 WHERE ac2.actor = j.actor
                   ) AS no_actor_config
            FROM "{schema}".jobs j
            LEFT JOIN "{schema}".actor_config ac ON ac.actor = j.actor
            WHERE j.status IN ('pending', 'scheduled')
        ) r
    ) s
    WHERE s.no_actor_config OR s.unserved_queue
    GROUP BY s.actor
    """

    # actor -> (last warned count, monotonic timestamp of that warning).
    # NOT a plain set: a set that is only ever added to means the first tick
    # logs once per actor and every later tick for the life of the process is
    # silent, so a condition that starts small and grows unboundedly looks
    # identical to one that resolved itself.
    warned: dict[str, tuple[int, float]] = {}
    schema = ctx.deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        # Why error, not warning: this returns, permanently disabling the
        # detector for the process lifetime while the worker keeps running
        # normally. That is a silent loss of a safety net, not a skipped tick.
        log.error(
            "stranded-jobs-detector-disabled",
            kind="stranded_jobs_detector_disabled",
            schema=schema,
            reason="schema name failed identifier validation",
        )
        return
    sql = _stranded_sql.format(schema=schema)
    liveness_secs = ctx.deps.settings.admin_worker_liveness_seconds

    while not shutdown.is_set():
        ctx.deps.liveness.tick(
            "leader.stranded_jobs", period=ctx.deps.settings.stranded_jobs_interval
        )
        await _sleep_interruptible(shutdown, ctx.deps.settings.stranded_jobs_interval)
        if not ctx.deps.leading():
            continue
        try:
            async with ctx.deps.worker_pool.acquire(
                timeout=ctx.deps.settings.dispatcher_command_timeout
            ) as conn:
                rows = await conn.fetch(sql, liveness_secs)
        except Exception as exc:
            # error_class/error_message (not the shared helper's error=repr(exc))
            # and the stranded-jobs-query-failed name are a documented log
            # contract (docs/guides/upgrading.md, "Sub-enqueue failure events
            # carry error_class/error_message") — preserved here even though
            # the read-failed-detector shape is otherwise shared.
            record_sweep_timeout("stranded_jobs")
            log.warning(
                "stranded-jobs-query-failed",
                kind="stranded_jobs_query_failed",
                worker_id=str(ctx.worker_id),
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
            continue
        current: dict[str, int] = {}
        # actor -> (no-actor-config rows, unserved-queue rows, unserved
        # queue names) — the per-shape facts the warning events report.
        shapes: dict[str, tuple[int, int, list[str]]] = {}
        # (actor, reason) -> rows: the gauge's own shape, so a series says
        # which condition held.
        by_reason: dict[tuple[str, StrandedReason], int] = {}
        for row in rows:
            actor = row["actor"]
            current[actor] = row["cnt"]
            no_config_cnt = int(row["no_actor_config_cnt"])
            unserved_cnt = int(row["unserved_queue_cnt"])
            shapes[actor] = (no_config_cnt, unserved_cnt, list(row["unserved_queues"]))
            if no_config_cnt:
                by_reason[(actor, "no_actor_config")] = no_config_cnt
            if unserved_cnt:
                by_reason[(actor, "unserved_queue")] = unserved_cnt

        # Always publish the gauge, including the empty case: an operator needs
        # to see the condition persist, grow, and clear. A log line at onset
        # cannot express any of that.
        update_stranded_jobs_cache(by_reason)

        now = time.monotonic()
        for actor, cnt in current.items():
            previous = warned.get(actor)
            if previous is None:
                should_warn = True
            else:
                last_count, last_time = previous
                # Re-warn when the backlog grows, or on a slow cadence so a
                # steady-state stall is not invisible in logs forever.
                should_warn = cnt > last_count or (now - last_time) >= _STRANDED_REWARN_SECS
            if should_warn:
                warned[actor] = (cnt, now)
                no_config_cnt, unserved_cnt, unserved_queues = shapes[actor]
                if no_config_cnt:
                    log.warning(
                        "stranded-jobs-no-actor-config",
                        kind="stranded_jobs_no_actor_config",
                        actor=actor,
                        pending_count=no_config_cnt,
                        first_seen=previous is None,
                    )
                if unserved_cnt:
                    # Why its own event (not the no-actor-config one): the
                    # remediation is different — subscribe a worker to the
                    # queue or re-route the enqueue — and the queue names are
                    # the actionable payload for it.
                    log.warning(
                        "stranded-jobs-unserved-queue",
                        kind="stranded_jobs_unserved_queue",
                        actor=actor,
                        pending_count=unserved_cnt,
                        queues=unserved_queues,
                        first_seen=previous is None,
                    )

        # Drop actors that recovered, so a recurrence warns again instead of
        # being suppressed for the life of the process.
        for actor in list(warned):
            if actor not in current:
                del warned[actor]
                log.info(
                    "stranded-jobs-cleared",
                    kind="stranded_jobs_cleared",
                    actor=actor,
                )
