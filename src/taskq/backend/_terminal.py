"""Terminal-write operations for PostgresBackend.

All ``mark_*`` methods, ``write_cancel_escalation``, ``write_attempt``,
and their shared helpers (attempt inserts, event inserts, owner lookup)
live here as module-level functions taking explicit
``(conn, sql: SqlTemplates, ...)`` or ``(pool, sql: SqlTemplates, ...)``
parameters.  :class:`~taskq.backend.postgres.PostgresBackend` methods are
thin wrappers that acquire the appropriate pool and delegate.
"""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import structlog

from taskq.backend._protocol import (
    AttemptOutcome,
    AttemptRow,
    ConnLike,
    ErrorInfo,
    JobId,
    JobRow,
)
from taskq.backend._records import (
    _job_row_from_record,
    compute_duration_ms,
    jsonb_param,
    parse_rowcount,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.backend.clock import Clock
from taskq.constants import MAX_RESULT_BYTES
from taskq.exceptions import (
    ResultTooLarge,
    WorkerOwnershipMismatch,
)
from taskq.obs import (
    get_logger,
    log_cancel_phase_change,
    log_state_change,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "_insert_attempt",
    "_insert_cancel_request_event",
    "_insert_state_change_event",
    "_mark_abandoned",
    "_mark_cancelled",
    "_mark_failed",
    "_mark_failed_or_retry",
    "_mark_retry",
    "_mark_retry_after",
    "_mark_snoozed",
    "_mark_succeeded",
    "_mark_succeeded_on_conn",
    "_select_owner",
    "_write_attempt",
    "_write_cancel_escalation",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


# ── Shared helpers ─────────────────────────────────────────────────────


async def _insert_attempt(
    conn: ConnLike,
    sql: SqlTemplates,
    job_id: JobId,
    attempt: int,
    started_at: datetime | None,
    outcome: str,
    error_class: str | None,
    error_message: str | None,
    error_traceback: str | None,
    duration_ms: int | None,
    worker_id: UUID | None,
) -> None:
    """INSERT a job_attempts row inside an existing transaction."""
    await conn.execute(
        sql.insert_attempt,
        job_id,
        attempt,
        started_at,
        outcome,
        error_class,
        error_message,
        error_traceback,
        duration_ms,
        worker_id,
        "{}",
    )


async def _insert_state_change_event(
    conn: ConnLike,
    sql: SqlTemplates,
    job_id: JobId,
    from_state: str,
    to_state: str,
    error_class: str | None = None,
    worker_id: UUID | None = None,
    extra_detail: dict[str, object] | None = None,
) -> None:
    """INSERT a job_events row with kind='state_change'."""
    detail: dict[str, object] = {
        "from_state": from_state,
        "to_state": to_state,
    }
    if error_class is not None:
        detail["error_class"] = error_class
    if worker_id is not None:
        detail["worker_id"] = str(worker_id)
    if extra_detail is not None:
        detail.update(extra_detail)
    await conn.execute(
        sql.insert_event,
        job_id,
        "state_change",
        jsonb_param(detail),
    )


async def _insert_cancel_request_event(
    conn: ConnLike,
    sql: SqlTemplates,
    job_id: JobId,
    reason: str | None,
) -> None:
    """INSERT a job_events row with kind='cancel_request'."""
    detail: dict[str, object] = {}
    if reason is not None:
        detail["reason"] = reason
    await conn.execute(
        sql.insert_event,
        job_id,
        "cancel_request",
        jsonb_param(detail),
    )


async def _select_owner(conn: ConnLike, sql: SqlTemplates, job_id: JobId) -> UUID | None:
    """Fetch ``locked_by_worker`` for a job to populate
    :class:`WorkerOwnershipMismatch.actual`.  Returns ``None`` if
    the row does not exist.
    """
    row = await conn.fetchrow(sql.select_owner, job_id)
    if row is None:
        return None
    return row["locked_by_worker"]


# ── mark_succeeded ─────────────────────────────────────────────────────


async def _mark_succeeded_on_conn(
    conn: ConnLike,
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:
    serialized_result = jsonb_param(result)
    result_size = len(serialized_result.encode("utf-8")) if serialized_result is not None else None
    if result_size is not None and result_size > MAX_RESULT_BYTES:
        raise ResultTooLarge(f"result size {result_size} bytes exceeds {MAX_RESULT_BYTES} byte cap")
    rec = await conn.fetchrow(
        sql.mark_succeeded,
        job_id,
        worker_id,
        serialized_result,
        result_size,
        progress_seq,
        jsonb_param(progress_state),
    )
    if rec is None:
        return False

    attempt: int = rec["attempt"]
    started_at: datetime | None = rec["started_at"]
    finished_at: datetime | None = rec["finished_at"]
    duration_ms = compute_duration_ms(started_at, finished_at)

    await _insert_attempt(
        conn,
        sql,
        job_id,
        attempt,
        started_at,
        "succeeded",
        None,
        None,
        None,
        duration_ms,
        worker_id,
    )
    await _insert_state_change_event(
        conn,
        sql,
        job_id,
        "running",
        "succeeded",
        worker_id=worker_id,
    )

    log_state_change(
        logger,
        from_state="running",
        to_state="succeeded",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=attempt,
    )
    return True


async def _mark_succeeded(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await _mark_succeeded_on_conn(
                conn, sql, job_id, worker_id, result, progress_seq, progress_state
            )


# ── mark_failed_or_retry ──────────────────────────────────────────────


async def _mark_failed_or_retry(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    clock: Clock,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    next_scheduled_at: datetime | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> JobRow:
    if next_scheduled_at is None:
        return await _mark_failed(
            pool, sql, job_id, worker_id, error_info, progress_seq, progress_state
        )
    return await _mark_retry(
        pool,
        sql,
        clock,
        job_id,
        worker_id,
        error_info,
        next_scheduled_at,
        progress_seq,
        progress_state,
    )


async def _mark_failed(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    progress_seq: int,
    progress_state: dict[str, object] | None,
) -> JobRow:
    async with pool.acquire() as conn:
        async with conn.transaction():
            rec = await conn.fetchrow(
                sql.mark_failed,
                job_id,
                worker_id,
                error_info.error_class,
                error_info.error_message,
                error_info.error_traceback,
                progress_seq,
                jsonb_param(progress_state),
            )
            if rec is None:
                actual = await _select_owner(conn, sql, job_id)
                raise WorkerOwnershipMismatch(job_id, worker_id, actual)

            row = _job_row_from_record(rec)
            duration_ms = compute_duration_ms(row.started_at, row.finished_at)

            await _insert_attempt(
                conn,
                sql,
                job_id,
                row.attempt,
                row.started_at,
                "failed",
                error_info.error_class,
                error_info.error_message,
                error_info.error_traceback,
                duration_ms,
                worker_id,
            )
            await _insert_state_change_event(
                conn,
                sql,
                job_id,
                "running",
                "failed",
                error_class=error_info.error_class,
                worker_id=worker_id,
            )

    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=row.attempt,
    )
    return row


async def _mark_retry(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    clock: Clock,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    next_scheduled_at: datetime,
    progress_seq: int,
    progress_state: dict[str, object] | None,
) -> JobRow:
    async with pool.acquire() as conn:
        async with conn.transaction():
            rec = await conn.fetchrow(
                sql.mark_retry,
                job_id,
                worker_id,
                next_scheduled_at,
                error_info.error_class,
                error_info.error_message,
                error_info.error_traceback,
                progress_seq,
                jsonb_param(progress_state),
            )
            if rec is None:
                actual = await _select_owner(conn, sql, job_id)
                raise WorkerOwnershipMismatch(job_id, worker_id, actual)

            row = _job_row_from_record(rec)
            duration_ms = (
                compute_duration_ms(row.started_at, clock.now())
                if row.started_at is not None
                else None
            )

            await _insert_attempt(
                conn,
                sql,
                job_id,
                row.attempt,
                row.started_at,
                "failed",
                error_info.error_class,
                error_info.error_message,
                error_info.error_traceback,
                duration_ms,
                worker_id,
            )
            await _insert_state_change_event(
                conn,
                sql,
                job_id,
                "running",
                "scheduled",
                error_class=error_info.error_class,
                worker_id=worker_id,
            )

    log_state_change(
        logger,
        from_state="running",
        to_state="scheduled",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=row.attempt,
    )
    return row


# ── mark_cancelled ─────────────────────────────────────────────────────


async def _mark_cancelled(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:
    async with pool.acquire() as conn:
        async with conn.transaction():
            rec = await conn.fetchrow(
                sql.mark_cancelled,
                job_id,
                worker_id,
                progress_seq,
                jsonb_param(progress_state),
            )
            if rec is None:
                return False

            attempt: int = rec["attempt"]
            started_at: datetime | None = rec["started_at"]
            finished_at: datetime | None = rec["finished_at"]
            duration_ms = compute_duration_ms(started_at, finished_at)

            await _insert_attempt(
                conn,
                sql,
                job_id,
                attempt,
                started_at,
                "cancelled",
                None,
                None,
                None,
                duration_ms,
                worker_id,
            )
            await _insert_state_change_event(
                conn,
                sql,
                job_id,
                "running",
                "cancelled",
                worker_id=worker_id,
            )

    log_state_change(
        logger,
        from_state="running",
        to_state="cancelled",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=attempt,
    )
    return True


# ── write_cancel_escalation ────────────────────────────────────────────


async def _write_cancel_escalation(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    phase: Literal[2],
) -> bool:
    if phase != 2:
        raise ValueError(
            "write_cancel_escalation only accepts phase=2; use write_cancel_request for phase=1"
        )

    async with pool.acquire() as conn:
        async with conn.transaction():
            tag = await conn.execute(sql.cancel_escalation, job_id, worker_id)
            if parse_rowcount(tag) != 1:
                return False
            await _insert_state_change_event(
                conn,
                sql,
                job_id,
                "running",
                "running",
                worker_id=worker_id,
                extra_detail={
                    "cancel_phase_from": 1,
                    "cancel_phase_to": 2,
                },
            )

    log_cancel_phase_change(
        logger,
        from_phase=1,
        to_phase=2,
        job_id=str(job_id),
        worker_id=str(worker_id),
    )
    return True


# ── mark_abandoned ─────────────────────────────────────────────────────


async def _mark_abandoned(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:
    async with pool.acquire() as conn:
        async with conn.transaction():
            rec = await conn.fetchrow(
                sql.mark_abandoned,
                job_id,
                progress_seq,
                jsonb_param(progress_state),
            )
            if rec is None:
                return False

            attempt: int = rec["attempt"]
            started_at: datetime | None = rec["started_at"]
            finished_at: datetime | None = rec["finished_at"]
            locked_by_worker: UUID | None = rec["locked_by_worker"]
            duration_ms = compute_duration_ms(started_at, finished_at)

            await _insert_attempt(
                conn,
                sql,
                job_id,
                attempt,
                started_at,
                "cancelled",
                None,
                None,
                None,
                duration_ms,
                locked_by_worker,
            )
            await _insert_state_change_event(
                conn,
                sql,
                job_id,
                "running",
                "abandoned",
                worker_id=locked_by_worker,
            )

    log_state_change(
        logger,
        from_state="running",
        to_state="abandoned",
        job_id=str(job_id),
        worker_id=str(locked_by_worker) if locked_by_worker is not None else None,
        attempt=attempt,
    )
    return True


# ── mark_snoozed ───────────────────────────────────────────────────────


async def _mark_snoozed(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    clock: Clock,
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    metadata_update: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    outcome: AttemptOutcome = "snoozed",
) -> Literal["scheduled", "failed", "noop"]:
    branch: str
    async with pool.acquire() as conn:
        async with conn.transaction():
            rec = await conn.fetchrow(
                sql.mark_snoozed,
                job_id,
                worker_id,
                delay,
                jsonb_param(metadata_update),
                progress_seq,
                jsonb_param(progress_state),
            )
            if rec is None:
                return "noop"

            branch = rec["outcome_branch"]
            attempt: int = rec["attempt"]
            started_at: datetime | None = rec["started_at"]
            finished_at: datetime | None = rec["finished_at"]
            duration_ms = (
                compute_duration_ms(started_at, clock.now())
                if started_at is not None and branch == "snoozed"
                else compute_duration_ms(started_at, finished_at)
            )

            if branch == "snoozed":
                await _insert_attempt(
                    conn,
                    sql,
                    job_id,
                    attempt,
                    started_at,
                    outcome,
                    None,
                    None,
                    None,
                    duration_ms,
                    worker_id,
                )
                await _insert_state_change_event(
                    conn,
                    sql,
                    job_id,
                    "running",
                    "scheduled",
                    worker_id=worker_id,
                )
            else:
                await _insert_attempt(
                    conn,
                    sql,
                    job_id,
                    attempt,
                    started_at,
                    "failed",
                    "DeadlineExceeded",
                    "schedule_to_close reached before next dispatch",
                    None,
                    duration_ms,
                    worker_id,
                )
                await _insert_state_change_event(
                    conn,
                    sql,
                    job_id,
                    "running",
                    "failed",
                    error_class="DeadlineExceeded",
                    worker_id=worker_id,
                )

    if branch == "snoozed":
        log_state_change(
            logger,
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=attempt,
        )
        return "scheduled"
    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=attempt,
    )
    return "failed"


# ── mark_retry_after ───────────────────────────────────────────────────


async def _mark_retry_after(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    clock: Clock,
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    consume_budget: bool = True,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
    branch: str
    async with pool.acquire() as conn:
        async with conn.transaction():
            sql_stmt = (
                sql.mark_retry_after_consume_true
                if consume_budget
                else sql.mark_retry_after_consume_false
            )
            rec = await conn.fetchrow(
                sql_stmt,
                job_id,
                worker_id,
                delay,
                progress_seq,
                jsonb_param(progress_state),
            )
            if rec is None:
                return "noop"

            branch = rec["outcome_branch"]
            attempt: int = rec["attempt"]
            attempt_for_record: int = rec["running_attempt"] if consume_budget else attempt
            started_at: datetime | None = rec["started_at"]
            finished_at: datetime | None = rec["finished_at"]
            duration_ms = (
                compute_duration_ms(started_at, clock.now())
                if started_at is not None and branch == "snoozed"
                else compute_duration_ms(started_at, finished_at)
            )

            if branch == "snoozed":
                await _insert_attempt(
                    conn,
                    sql,
                    job_id,
                    attempt_for_record,
                    started_at,
                    "snoozed",
                    "RetryAfter",
                    None,
                    None,
                    duration_ms,
                    worker_id,
                )
                await _insert_state_change_event(
                    conn,
                    sql,
                    job_id,
                    "running",
                    "scheduled",
                    worker_id=worker_id,
                )
            elif branch == "max_attempts_failed":
                await _insert_attempt(
                    conn,
                    sql,
                    job_id,
                    attempt_for_record,
                    started_at,
                    "failed",
                    "MaxAttemptsExceeded",
                    "retry budget exhausted",
                    None,
                    duration_ms,
                    worker_id,
                )
                await _insert_state_change_event(
                    conn,
                    sql,
                    job_id,
                    "running",
                    "failed",
                    error_class="MaxAttemptsExceeded",
                    worker_id=worker_id,
                )
            else:
                await _insert_attempt(
                    conn,
                    sql,
                    job_id,
                    attempt_for_record,
                    started_at,
                    "failed",
                    "DeadlineExceeded",
                    "schedule_to_close reached before next dispatch",
                    None,
                    duration_ms,
                    worker_id,
                )
                await _insert_state_change_event(
                    conn,
                    sql,
                    job_id,
                    "running",
                    "failed",
                    error_class="DeadlineExceeded",
                    worker_id=worker_id,
                )

    if branch == "snoozed":
        log_state_change(
            logger,
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=attempt,
            cause="retry_after",
        )
        return "scheduled"
    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=attempt,
        cause="retry_after",
    )
    if branch == "max_attempts_failed":
        return "failed:MaxAttemptsExceeded"
    return "failed:DeadlineExceeded"


# ── write_attempt ──────────────────────────────────────────────────────


async def _write_attempt(pool: "asyncpg.Pool", sql: SqlTemplates, attempt: AttemptRow) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                sql.insert_attempt_explicit,
                attempt.job_id,
                attempt.attempt,
                attempt.started_at,
                attempt.finished_at,
                attempt.outcome,
                attempt.error_class,
                attempt.error_message,
                attempt.error_traceback,
                attempt.duration_ms,
                attempt.worker_id,
                jsonb_param(attempt.metadata),
            )
