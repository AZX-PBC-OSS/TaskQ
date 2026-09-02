"""Sweep loop functions for MaintenanceLeader.

The sweep loop functions (``_sweep_loop``, ``_prune_loop``,
``_archive_expiry_loop``, ``_queue_depth_loop``,
``_reservation_slots_loop``, ``_stranded_jobs_loop``) live here as
module-level functions taking a :class:`~taskq.worker._leader_shared.SweepContext`
as the first parameter — the subset of ``MaintenanceLeader`` state the
sweeps need, so this module has no dependency on ``leader.py``.
"""

import asyncio
import contextlib
import time
from datetime import UTC, date, datetime, timedelta
from typing import cast

import asyncpg
import croniter as cr
import structlog

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.obs import (
    get_logger,
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
    ARCHIVE_EXPIRY_LOCK_NAME,
    PRUNE_LOCK_NAME,
    SweepContext,
    _build_retention_per_status,
    _dbg,
    _err,
    _load_actor_retention_overrides,
    _metric,
    _schedule_utc_to_cron,
    _sweep_duration_hist,
    _sweep_rows_counter,
    archive_expiry_sweep,
    cleanup_stale_workers,
    complete_stale_batches,
    prune_terminal_jobs,
)
from taskq.worker._transient import TRANSIENT_PG_ERRORS

__all__ = [
    "_archive_expiry_loop",
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


async def _sweep_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    warned_sweep_1 = warned_sweep_2 = False
    while not shutdown.is_set():
        ctx.deps.liveness.tick("leader.sweep", period=ctx.deps.settings.sweep_interval)
        if ctx.deps.is_leader.is_set():
            start = time.monotonic()  # Sweep 1: reclaim_expired_locks
            try:
                # No `now` argument: the sweep's server-side predicates
                # (clock_timestamp()) are the single arbiter.
                count_1 = await ctx.backend.reclaim_expired_locks(
                    timedelta(seconds=ctx.deps.settings.cancellation_grace_period),
                    timedelta(seconds=ctx.deps.settings.cleanup_grace_period),
                )
            except NotImplementedError as exc:
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
                log.warning(
                    "sweep-expired-locks-failed",
                    kind="sweep_expired_locks_failed",
                    worker_id=str(ctx.worker_id),
                    error=repr(exc),
                )
            else:
                _metric("expired_locks", count_1, start)
                _dbg("sweep_expired_locks_tick", "sweep_expired_locks_tick", count_1, start)
            start = time.monotonic()  # Sweep 2: deadline_sweep
            try:
                count_2 = await ctx.backend.deadline_sweep()
            except NotImplementedError as exc:
                if not warned_sweep_2:
                    _err("sweep_deadline_exceeded_unimplemented", _EK3, ctx.worker_id, exc)
                    warned_sweep_2 = True
            except TRANSIENT_PG_ERRORS as exc:
                # Same transient-PG rationale as sweep 1 above.
                log.warning(
                    "sweep-deadline-exceeded-failed",
                    kind="sweep_deadline_exceeded_failed",
                    worker_id=str(ctx.worker_id),
                    error=repr(exc),
                )
            else:
                _metric("deadline_exceeded", count_2, start)
                log.debug(
                    "sweep_deadline_exceeded_tick",
                    kind="deadline_exceeded_sweep",
                    count=count_2,
                    sweep_duration_ms=int((time.monotonic() - start) * 1000),
                )
            if hasattr(ctx.backend, "sweep_leaked_reservation_slots"):
                start = time.monotonic()
                try:
                    async with ctx.deps.dispatcher_pool.acquire(
                        timeout=ctx.deps.settings.dispatcher_command_timeout
                    ) as conn:
                        count_4 = cast(
                            "int",
                            await ctx.backend.sweep_leaked_reservation_slots(  # type: ignore[reportAttributeAccessIssue]  # Why: guarded by hasattr; only PostgresBackend implements these maintenance sweeps.
                                conn, schema=ctx.deps.settings.schema_name
                            ),
                        )
                    _metric("leaked_slots", count_4, start)
                    _dbg("sweep_leaked_slots_tick", "sweep_leaked_slots_tick", count_4, start)
                except TRANSIENT_PG_ERRORS as exc:
                    log.warning(
                        "sweep-leaked-slots-failed",
                        kind="sweep_leaked_slots_failed",
                        worker_id=str(ctx.worker_id),
                        error=repr(exc),
                    )
                start = time.monotonic()
                try:
                    async with ctx.deps.dispatcher_pool.acquire(
                        timeout=ctx.deps.settings.dispatcher_command_timeout
                    ) as conn:
                        count_rt = cast(
                            "int",
                            await ctx.backend.sweep_expired_results(  # type: ignore[reportAttributeAccessIssue]  # Why: guarded by hasattr above.
                                conn, schema=ctx.deps.settings.schema_name
                            ),
                        )
                    _metric("expired_results", count_rt, start)
                    _dbg(
                        "sweep_expired_results_tick",
                        "sweep_expired_results_tick",
                        count_rt,
                        start,
                    )
                except TRANSIENT_PG_ERRORS as exc:
                    log.warning(
                        "sweep-expired-results-failed",
                        kind="sweep_expired_results_failed",
                        worker_id=str(ctx.worker_id),
                        error=repr(exc),
                    )
                start = time.monotonic()
                try:
                    async with ctx.deps.dispatcher_pool.acquire(
                        timeout=ctx.deps.settings.dispatcher_command_timeout
                    ) as conn:
                        count_sr = await cleanup_stale_workers(
                            conn,
                            worker_id=ctx.worker_id,
                            staleness=timedelta(
                                seconds=ctx.deps.settings.heartbeat_interval
                                * (ctx.deps.settings.max_heartbeat_failures + 3)
                            ),
                            schema=ctx.deps.settings.schema_name,
                        )
                    _metric("stale_workers", count_sr, start)
                    _dbg(
                        "cleanup_stale_workers_tick",
                        "cleanup_stale_workers_tick",
                        count_sr,
                        start,
                    )
                except TRANSIENT_PG_ERRORS as exc:
                    log.warning(
                        "cleanup-stale-workers-failed",
                        kind="cleanup_stale_workers_failed",
                        worker_id=str(ctx.worker_id),
                        error=repr(exc),
                    )
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
            # hasattr guard: complete_stale_batches needs a real PG connection
            # (dispatcher_pool). InMemoryBackend does not implement
            # sweep_leaked_reservation_slots, so this gate prevents the
            # stale-batches sweep from running on the in-memory backend in
            # tests — matching the same pattern used for sweep_leaked_slots
            # and sweep_expired_results above.
            if hasattr(ctx.backend, "sweep_leaked_reservation_slots"):
                start = time.monotonic()
                try:
                    async with ctx.deps.dispatcher_pool.acquire(
                        timeout=ctx.deps.settings.dispatcher_command_timeout
                    ) as conn:
                        stale_count = await complete_stale_batches(
                            conn, schema=ctx.deps.settings.schema_name
                        )
                    _metric("stale_batches", stale_count, start)
                    if stale_count:
                        log.info("stale-batches-completed", kind="batch", count=stale_count)
                except (
                    TimeoutError,
                    asyncpg.PostgresConnectionError,
                    asyncpg.InterfaceError,
                    asyncpg.exceptions.UndefinedTableError,
                    OSError,
                ) as exc:
                    log.warning("stale-batches-sweep-failed", kind="batch", error=repr(exc))
        await _sleep_interruptible(shutdown, ctx.deps.settings.sweep_interval)


async def _prune_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    last_pruned_date: date | None = None

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
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", PRUNE_LOCK_NAME
                )
                if not lock_acquired:
                    log.warning(
                        "prune-skipped-advisory-lock-held",
                        kind="prune",
                        worker_id=str(ctx.worker_id),
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
                    # Skip batch prune when all retentions are disabled —
                    # an empty cutoffs dict means no status had a retention
                    # period, so the fallback (datetime.now(UTC)) would
                    # delete every completed batch. Only prune when at
                    # least one cutoff exists.
                    if result.cutoffs:
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
                            PRUNE_LOCK_NAME,
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
                    ARCHIVE_EXPIRY_LOCK_NAME,
                )
                if not lock_acquired:
                    log.warning(
                        "archive-expiry-skipped-advisory-lock-held",
                        kind="archive_expiry",
                        worker_id=str(ctx.worker_id),
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
                            ARCHIVE_EXPIRY_LOCK_NAME,
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
        log.warning("invalid-schema-skipped", schema=schema)
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


async def _reservation_slots_loop(ctx: SweepContext, shutdown: asyncio.Event) -> None:
    schema = ctx.deps.settings.schema_name
    if not _IDENT_RE.match(schema):
        log.warning("invalid-schema-skipped", schema=schema)
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
