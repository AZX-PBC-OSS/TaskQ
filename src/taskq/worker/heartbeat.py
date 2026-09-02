"""Heartbeat loop and cancel-poll seam.

Each tick acquires one connection from heartbeat_pool, opens a single
transaction, and atomically extends workers.last_seen_at, jobs lock /
heartbeat columns, and reservation_slots leases for jobs locked by this
worker. After max_heartbeat_failures consecutive connection failures,
isolate_self proactively transitions running jobs and signals shutdown.
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
from taskq.backend._sql import (
    INSERT_ATTEMPT_SQL,
    build_heartbeat_sql,
    parse_rowcount,
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
    (
        update_worker_liveness_sql,
        update_jobs_lock_sql,
        update_reservation_leases_sql,
        update_leader_ping_sql,
    ) = build_heartbeat_sql(schema)

    while not shutdown.is_set():
        deps.liveness.tick("heartbeat", period=interval)
        _in_tx_failed = False
        tick_start = time.monotonic()
        try:
            async with deps.heartbeat_pool.acquire(timeout=interval) as conn, conn.transaction():
                await conn.execute(update_worker_liveness_sql, worker_id)
                jobs_tag = await conn.execute(update_jobs_lock_sql, worker_id, lock_lease)
                await conn.execute(update_reservation_leases_sql, worker_id, lock_lease)
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
                if deps.is_leader.is_set():
                    await conn.execute(update_leader_ping_sql, worker_id)
            # Transaction committed: row locks released.  Run post-tx work
            # (phase-3 mark_abandoned calls) now that deadlock is impossible.
            if cancel_controller is not None:
                await cancel_controller.run_post_tx()
            deps.heartbeat_failures = 0
            update_heartbeat_consecutive_failures(str(worker_id), 0)
            tick_duration_s = time.monotonic() - tick_start
            _tick_duration.record(tick_duration_s)
            record_lock_expires_in_seconds(str(worker_id), lock_lease.total_seconds())
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
        if cancel_wake_event is not None:
            # Wait up to interval, but wake immediately on a cancel NOTIFY.
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(cancel_wake_event.wait(), timeout=interval)
            cancel_wake_event.clear()
        else:
            await asyncio.sleep(interval)


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

# Branch-for-branch mirror of _sweeps.py's _SWEEP_1_SQL SET clause — the
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
_ISOLATE_JOB_SQL_TEMPLATE = """\
UPDATE "{schema}".jobs
SET status = CASE
        WHEN attempt < max_attempts AND retry_kind != 'non_retryable'
            THEN 'pending'::"{schema}".job_status
        WHEN cancel_phase != 0
            THEN 'cancelled'::"{schema}".job_status
        ELSE 'crashed'::"{schema}".job_status
    END,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    cancel_phase = 0,
    cancel_requested_at = NULL,
    scheduled_at = CASE
        WHEN attempt < max_attempts AND retry_kind != 'non_retryable'
            THEN clock_timestamp() + interval '5 seconds'
        ELSE scheduled_at
    END,
    finished_at = CASE
        WHEN NOT (attempt < max_attempts AND retry_kind != 'non_retryable')
            THEN clock_timestamp()
        ELSE finished_at
    END
WHERE id = $1 AND status = 'running' AND locked_by_worker = $2"""


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
    jobs_pending_count = 0
    jobs_crashed_count = 0
    jobs_cancelled_count = 0

    try:
        conn = await asyncpg.connect(pg_dsn, timeout=5.0)  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # Why: asyncpg-stubs does not declare timeout kwarg on connect(); the parameter exists at runtime at 0.31.0.  asyncpg default is 60s — far too long when PG is already problematic.
        try:

            async def _inner() -> tuple[int, int, int]:
                pending = 0
                crashed = 0
                cancelled = 0
                async with conn.transaction():
                    rows = await conn.fetch(  # pyright: ignore[reportUnknownVariableType]  # Why: conn type suppressed above due to asyncpg-stubs limitation on connect().
                        select_running_jobs_sql, worker_id
                    )
                    for row in rows:  # pyright: ignore[reportUnknownVariableType]  # Why: rows type suppressed above — propagates from conn.fetch() suppression.
                        is_pending = (  # pyright: ignore[reportUnknownVariableType]  # Why: row column accessor types unknown — propagates from conn.fetch() suppression.
                            row["attempt"] < row["max_attempts"]
                            and row["retry_kind"] != "non_retryable"
                        )
                        if is_pending:
                            pending += 1
                        elif row["cancel_phase"] != 0:
                            # Mirrors _ISOLATE_JOB_SQL_TEMPLATE's CASE arm:
                            # exhausted + cancel in-flight → 'cancelled'.
                            cancelled += 1
                        else:
                            crashed += 1
                        await conn.execute(isolate_job_sql, row["id"], worker_id)
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
                return pending, crashed, cancelled

            jobs_pending_count, jobs_crashed_count, jobs_cancelled_count = await asyncio.shield(
                _inner()
            )
        finally:
            # Why bounded: isolate_self only runs when PG is already
            # suspected dead (heartbeat failures exceeded), so this close is
            # exactly the dead-PG hang case (#38) — unbounded, it would
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
        )
