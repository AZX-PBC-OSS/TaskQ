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
from typing import cast

import asyncpg
import croniter as cr
import structlog

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    schema_lock_name,
)
from taskq.obs import (
    get_logger,
    record_lock_contention,
    record_sweep_success,
    record_sweep_timeout,
    update_jobs_by_status_cache,
    update_oldest_due_age_cache,
    update_queue_depth_cache,
    update_reservation_slots_cache,
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
#: The condition is persistent by nature (it needs an operator to create the
#: actor_config row), so re-warning every tick would be noise; never
#: re-warning made a permanent, growing backlog invisible after one line.
_STRANDED_REWARN_SECS: float = 3600.0


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
        if shutdown.is_set():
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

    while not shutdown.is_set():
        ctx.deps.liveness.tick("leader.sweep", period=ctx.deps.settings.sweep_interval)
        if ctx.deps.is_leader.is_set():
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
                        async with ctx.deps.dispatcher_pool.acquire(
                            timeout=ctx.deps.settings.dispatcher_command_timeout
                        ) as conn:
                            rows_4 = cast(
                                "int",
                                await ctx.backend.sweep_leaked_reservation_slots(  # type: ignore[reportAttributeAccessIssue]  # Why: guarded by hasattr; only PostgresBackend implements these maintenance sweeps.
                                    conn, schema=ctx.deps.settings.schema_name
                                ),
                            )
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
                        async with ctx.deps.dispatcher_pool.acquire(
                            timeout=ctx.deps.settings.dispatcher_command_timeout
                        ) as conn:
                            stale_rows = await complete_stale_batches(
                                conn,
                                schema=ctx.deps.settings.schema_name,
                                batch_size=ctx.deps.settings.event_writer_batch_size,
                            )
                        if stale_rows:
                            log.info("stale-batches-completed", kind="batch", count=stale_rows)
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
                evicted = rl.evict_idle_keyed_reservations(idle_for=_KEYED_IDLE_THRESHOLD)
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
                evicted = rl.evict_idle_keyed_rate_limits(idle_for=_KEYED_IDLE_THRESHOLD)
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
        await _sleep_interruptible(shutdown, ctx.deps.settings.sweep_interval)


async def _prune_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    last_pruned_date: date | None = None
    lock_name = schema_lock_name("prune", ctx.deps.settings.schema_name)

    while not shutdown.is_set():
        now_utc = datetime.now(UTC)
        cron_expr = ctx.deps.settings.prune_cron_expr or _schedule_utc_to_cron(
            ctx.deps.settings.prune_schedule_utc
        )
        it = cr.croniter(cron_expr, now_utc)
        next_fire: datetime = it.get_next(datetime).replace(tzinfo=UTC)

        try:
            secs = max(0.0, (next_fire - datetime.now(UTC)).total_seconds())
            await asyncio.wait_for(shutdown.wait(), timeout=secs)
        except TimeoutError:
            pass

        if shutdown.is_set():
            break
        if not ctx.deps.is_leader.is_set():
            continue
        today_utc = datetime.now(UTC).date()
        if last_pruned_date == today_utc:
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

                try:
                    retention_per_status = _build_retention_per_status(ctx.deps.settings)
                    actor_overrides = await _load_actor_retention_overrides(
                        conn, schema=ctx.deps.settings.schema_name
                    )
                    result = await prune_terminal_jobs(
                        conn,
                        retention_per_status=retention_per_status,
                        archive_retention=ctx.deps.settings.archive_retention_period,
                        batch_size=ctx.deps.settings.prune_batch_size,
                        schema=ctx.deps.settings.schema_name,
                        actor_overrides=actor_overrides if actor_overrides else None,
                    )
                    last_pruned_date = today_utc
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
                    log.error("prune-failed", kind="prune", error=repr(exc))
                finally:
                    with contextlib.suppress(*TRANSIENT_PG_ERRORS):
                        await conn.execute(
                            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                            lock_name,
                        )
        except TRANSIENT_PG_ERRORS as exc:
            log.warning(
                "prune-lock-attempt-failed",
                kind="prune_lock_failed",
                worker_id=str(ctx.worker_id),
                error=repr(exc),
            )


async def _archive_expiry_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    last_expiry_date: date | None = None
    lock_name = schema_lock_name("archive_expiry", ctx.deps.settings.schema_name)

    while not shutdown.is_set():
        now_utc = datetime.now(UTC)
        cron_expr = ctx.deps.settings.archive_expiry_cron_expr or _schedule_utc_to_cron(
            ctx.deps.settings.archive_expiry_schedule_utc
        )
        it = cr.croniter(cron_expr, now_utc)
        next_fire: datetime = it.get_next(datetime).replace(tzinfo=UTC)

        try:
            secs = max(0.0, (next_fire - datetime.now(UTC)).total_seconds())
            await asyncio.wait_for(shutdown.wait(), timeout=secs)
        except TimeoutError:
            pass

        if shutdown.is_set():
            break
        if not ctx.deps.is_leader.is_set():
            continue
        today_utc = datetime.now(UTC).date()
        if last_expiry_date == today_utc:
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

                try:
                    result = await archive_expiry_sweep(
                        conn,
                        batch_size=ctx.deps.settings.prune_batch_size,
                        schema=ctx.deps.settings.schema_name,
                    )
                    last_expiry_date = today_utc
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
                    log.error("archive-expiry-failed", kind="archive_expiry", error=repr(exc))
                finally:
                    with contextlib.suppress(*TRANSIENT_PG_ERRORS):
                        await conn.execute(
                            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                            lock_name,
                        )
        except TRANSIENT_PG_ERRORS as exc:
            log.warning(
                "archive-expiry-lock-attempt-failed",
                kind="archive_expiry_lock_failed",
                worker_id=str(ctx.worker_id),
                error=repr(exc),
            )


async def _queue_depth_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
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
    while not shutdown.is_set():
        ctx.deps.liveness.tick("leader.queue_depth", period=ctx.deps.settings.queue_depth_interval)
        if ctx.deps.is_leader.is_set():
            try:
                async with ctx.deps.dispatcher_pool.acquire(
                    timeout=ctx.deps.settings.dispatcher_command_timeout
                ) as conn:
                    rows = await conn.fetch(sql)
                cache: dict[str, int] = {row["queue"]: row["count"] for row in rows}
                update_queue_depth_cache(cache)
            except Exception as exc:
                log.warning(
                    "queue-depth-sampling-failed",
                    kind="queue_depth_sampling_failed",
                    worker_id=str(ctx.worker_id),
                    error=repr(exc),
                )
        await _sleep_interruptible(shutdown, ctx.deps.settings.queue_depth_interval)


_QUERY_JOBS_BY_STATUS_SQL_TEMPLATE = 'SELECT status, count(*) FROM "{schema}".jobs GROUP BY status'
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


async def _backlog_detection_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Sample the backlog detectors (jobs-by-status, oldest due age) every
    ``queue_depth_interval``.

    Why NOT leader-gated, unlike every sibling sampler here: a detector
    hosted behind the leadership gate emits nothing under the very failure
    it exists to expose — another schema's worker holding the old-style
    lock, or any election loss, mutes every leader-gated sampler at once.
    Every worker samples instead, accepting N times the query cost, and the
    per-target series are deduplicated by the scrape target.
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
    while not shutdown.is_set():
        ctx.deps.liveness.tick(
            "leader.backlog_detection", period=ctx.deps.settings.queue_depth_interval
        )
        try:
            # One connection for both statements: the two gauges answer one
            # question (is promotion stalled?) and must not straddle two
            # snapshots.
            async with ctx.deps.dispatcher_pool.acquire(
                timeout=ctx.deps.settings.dispatcher_command_timeout
            ) as conn:
                status_rows = await conn.fetch(by_status_sql)
                oldest_due: float | None = await conn.fetchval(oldest_due_sql)
            update_jobs_by_status_cache(
                {str(row["status"]): int(row["count"]) for row in status_rows}
            )
            # MIN(scheduled_at) over an empty set is NULL — nothing is due,
            # which the gauge expresses as 0.0, not as a missing sample.
            update_oldest_due_age_cache(oldest_due if oldest_due is not None else 0.0)
        except Exception as exc:
            log.warning(
                "backlog-detection-sampling-failed",
                kind="backlog_detection_sampling_failed",
                worker_id=str(ctx.worker_id),
                error=repr(exc),
            )
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
        if ctx.deps.is_leader.is_set():
            try:
                async with ctx.deps.dispatcher_pool.acquire(
                    timeout=ctx.deps.settings.dispatcher_command_timeout
                ) as conn:
                    rows = await conn.fetch(sql)
                cache: dict[str, int] = {row["bucket_name"]: row["count"] for row in rows}
                update_reservation_slots_cache(cache)
            except Exception as exc:
                log.warning(
                    "reservation-slots-sampling-failed",
                    kind="reservation_slots_sampling_failed",
                    worker_id=str(ctx.worker_id),
                    error=repr(exc),
                )
        await _sleep_interruptible(shutdown, ctx.deps.settings.reservation_slots_interval)


async def _stranded_jobs_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    """Periodically warn about pending jobs whose actor has no actor_config row.

    Off the hot dispatch path — runs every 60 s when this worker is leader.
    """
    _stranded_sql = """\
    SELECT j.actor, count(*) AS cnt
    FROM "{schema}".jobs j
    WHERE j.status IN ('pending', 'scheduled')
      AND NOT EXISTS (
        SELECT 1 FROM "{schema}".actor_config ac WHERE ac.actor = j.actor
      )
    GROUP BY j.actor
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

    while not shutdown.is_set():
        ctx.deps.liveness.tick(
            "leader.stranded_jobs", period=ctx.deps.settings.stranded_jobs_interval
        )
        await _sleep_interruptible(shutdown, ctx.deps.settings.stranded_jobs_interval)
        if not ctx.deps.is_leader.is_set():
            continue
        try:
            async with ctx.deps.worker_pool.acquire(
                timeout=ctx.deps.settings.dispatcher_command_timeout
            ) as conn:
                rows = await conn.fetch(sql)
        except Exception as exc:
            log.warning(
                "stranded-jobs-query-failed",
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
            continue
        current: dict[str, int] = {row["actor"]: row["cnt"] for row in rows}

        # Always publish the gauge, including the empty case: an operator needs
        # to see the condition persist, grow, and clear. A log line at onset
        # cannot express any of that.
        update_stranded_jobs_cache(current)

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
                log.warning(
                    "stranded-jobs-no-actor-config",
                    kind="stranded_jobs_no_actor_config",
                    actor=actor,
                    pending_count=cnt,
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
