"""Terminal-write operations for PostgresBackend.

All ``mark_*`` methods, ``write_cancel_escalation``, ``write_attempt``,
and their shared helpers (attempt inserts, event inserts, owner lookup)
live here as module-level functions taking explicit
``(conn, sql: SqlTemplates, ...)`` or ``(pool, sql: SqlTemplates, ...)``
parameters.  :class:`~taskq.backend.postgres.PostgresBackend` methods are
thin wrappers that acquire the appropriate pool and delegate.

One statement per terminal write
===============================

Each ``mark_*`` write is ONE data-modifying-CTE statement
(``sql.mark_*`` in ``_sql_templates.py``): the fenced ``jobs`` UPDATE,
the ``job_attempts`` INSERT, and the ``job_events`` INSERT execute inside
a single statement instead of three awaited round trips inside one
transaction.  E2E profiling of the dispatch loop
(benchmarks/e2e_dispatch.py, 2000-job drain against local compose
Postgres) measured the three-statement form at ~1.4 ms per job — ~89% of
dispatch wall-clock and ~10x the terminal UPDATE's own 0.14 ms — with the
round trips, not the statements' work, dominating.  Fusing them, plus
dropping the pool path's now-redundant explicit BEGIN/COMMIT (a single
SQL statement — CTEs included — is atomic by itself, so the transaction
context manager's two round trips bought nothing the statement doesn't
already provide; the LOOP-scope ``mark_succeeded_with_conn`` path still
runs inside the actor's transaction as before), removes four round trips
per job with NO change to commit semantics: statement failure still
aborts the write whole, still classifies through
``_TERMINAL_WRITE_INFRA_EXCEPTIONS``, and still leaves the job
``running`` for lease-sweep reclaim (at-least-once).

Invariants preserved verbatim from the three-statement form:

* The UPDATE stays the single arbiter: its fencing WHERE (``status =
  'running' AND locked_by_worker = $2``) decides everything, and an
  empty ``upd`` CTE makes the INSERT CTEs insert nothing and the final
  ``SELECT`` return no row — the exact ``rec is None`` /
  ``WorkerOwnershipMismatch`` / ``False`` contract, without a second
  read.
* ``job_attempts.worker_id`` resolves through the holder-CTE idiom
  (``FOR KEY SHARE`` probe, LEFT-scan semantics via scalar subquery): a
  present worker row records the id, an already-deleted one records
  NULL, mirroring the column's ON DELETE SET NULL — never an FK
  violation (the constraint-violation tear-down risk documented on
  ``_sql.py``'s INSERT_ATTEMPT_SQL).
* Every timestamp is database-written (``clock_timestamp()``; the
  retry/snooze arms' ``now_ts``); ``duration_ms`` is computed in the
  statement from the same started/finished pair Python used to receive
  and re-multiply — but server-side, with exact numeric arithmetic
  instead of Python's float path.  Values can differ from the old
  Python computation by 1ms on exactly-whole-millisecond boundaries
  (where the float product drifted just below the integer); the
  server-side values are strictly more accurate.  ``trunc()`` keeps
  the same truncation toward zero (a bare ``::int`` cast rounds to
  nearest).
* Per-arm arbitration of the two- and three-arm variants
  (``mark_retry``, ``mark_snoozed``, ``mark_retry_after_*``): each arm
  chains its own attempt/event CTEs with that arm's outcome, error
  fields, and detail jsonb, so the ``outcome_branch`` tri-state the
  Python returns is computed by the same UPDATE predicates as before.
* Event ``detail`` is built server-side with ``jsonb_build_object``;
  ``jsonb_strip_nulls`` drops the keys Python conditionally omitted
  (``error_class`` when absent, ``worker_id`` for NULL holders).  The
  stored jsonb is key-order-normalized either way, so readers parsing
  the detail see the identical object.

Deliberately NOT done here (the evaluated alternative): coalescing
outcomes across jobs into one multi-row flusher.  Completion is the
job's final act — the consumer ``await``s (under ``asyncio.shield``)
its terminal write before reporting the outcome, so a returned
``mark_succeeded`` means the row is durably terminal, and the at-least-
once window (crash → lease sweep → re-run) is exercised only when the
write itself fails.  A non-awaited flusher would widen that window to
every successful job (a crash inside the flush window re-runs already-
succeeded actors) and reorder ``on_success`` hooks, terminal Redis
publishes, and progress-buffer bookkeeping that all assume post-commit
ordering; an awaited flusher adds queueing latency without removing the
round trips this merge already collapsed.  The CTE fusion captures the
measured win with zero semantic movement, so the coalescing design was
rejected rather than shipped behind a setting.
"""

from datetime import timedelta
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
    jsonb_param,
    parse_rowcount,
)
from taskq.backend._sql_templates import SqlTemplates
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
    "_insert_cancel_request_event",
    "_insert_state_change_event",
    "_mark_abandoned",
    "_mark_cancelled",
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
    fallback_result_ttl: timedelta | None = None,
    max_result_bytes: int = MAX_RESULT_BYTES,
) -> bool:
    """Terminal success write: ONE statement (UPDATE + attempt + event).

    The fused ``sql.mark_succeeded`` keeps the pool and LOOP-scope
    transactional paths byte-identical in semantics — see the module
    docstring.  ``None`` here still means the fencing UPDATE matched no
    row (wrong worker, missing job, already moved): nothing was written.
    """
    serialized_result = jsonb_param(result)
    result_size = len(serialized_result.encode("utf-8")) if serialized_result is not None else None
    if result_size is not None and result_size > max_result_bytes:
        raise ResultTooLarge(f"result size {result_size} bytes exceeds {max_result_bytes} byte cap")
    rec = await conn.fetchrow(
        sql.mark_succeeded,
        job_id,
        worker_id,
        serialized_result,
        result_size,
        progress_seq,
        jsonb_param(progress_state),
        fallback_result_ttl,
    )
    if rec is None:
        return False

    log_state_change(
        logger,
        from_state="running",
        to_state="succeeded",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=rec["attempt"],
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
    fallback_result_ttl: timedelta | None = None,
    max_result_bytes: int = MAX_RESULT_BYTES,
) -> bool:
    # No explicit transaction: the fused statement is atomic by itself (a
    # single SQL statement — CTEs included — either completes entirely or
    # not at all), and dropping the BEGIN/COMMIT pair removes two of the
    # four remaining per-job round trips.  It also shortens the fencing
    # row lock's hold from UPDATE..COMMIT to the statement's own duration.
    # The LOOP-scope path (_mark_succeeded_on_conn via
    # mark_succeeded_with_conn) still runs inside the actor's transaction
    # exactly as before.
    async with pool.acquire() as conn:
        return await _mark_succeeded_on_conn(
            conn,
            sql,
            job_id,
            worker_id,
            result,
            progress_seq,
            progress_state,
            fallback_result_ttl,
            max_result_bytes,
        )


# ── mark_failed_or_retry ──────────────────────────────────────────────


async def _mark_failed_or_retry(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    retry_delay: timedelta | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> JobRow:
    if retry_delay is None:
        return await _mark_failed(
            pool, sql, job_id, worker_id, error_info, progress_seq, progress_state
        )
    return await _mark_retry(
        pool,
        sql,
        job_id,
        worker_id,
        error_info,
        retry_delay,
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
    # No explicit transaction — see _mark_succeeded.  The rare
    # WorkerOwnershipMismatch diagnostic (_select_owner) runs after the
    # no-op statement on the same connection: both the fused statement and
    # the owner read are single-statement implicit transactions, and the
    # write could not have modified the row when it matched nothing.
    async with pool.acquire() as conn:
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
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    retry_delay: timedelta,
    progress_seq: int,
    progress_state: dict[str, object] | None,
) -> JobRow:
    branch: str
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            sql.mark_retry,
            job_id,
            worker_id,
            retry_delay,
            error_info.error_class,
            error_info.error_message,
            error_info.error_traceback,
            progress_seq,
            jsonb_param(progress_state),
        )
        if rec is None:
            actual = await _select_owner(conn, sql, job_id)
            raise WorkerOwnershipMismatch(job_id, worker_id, actual)

        branch = rec["outcome_branch"]
        row = _job_row_from_record(rec)
        # The attempt row (duration_ms computed from the winning arm's
        # own timestamps) and the state_change event are written by the
        # fused statement's per-arm CTEs — see _sql_templates.mark_retry.

    if branch == "retried":
        log_state_change(
            logger,
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=row.attempt,
        )
        return row
    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=row.attempt,
        reason="schedule_to_close",
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
        rec = await conn.fetchrow(
            sql.mark_cancelled,
            job_id,
            worker_id,
            progress_seq,
            jsonb_param(progress_state),
        )
        if rec is None:
            return False

    log_state_change(
        logger,
        from_state="running",
        to_state="cancelled",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=rec["attempt"],
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
        rec = await conn.fetchrow(
            sql.mark_abandoned,
            job_id,
            progress_seq,
            jsonb_param(progress_state),
        )
        if rec is None:
            return False

        locked_by_worker: UUID | None = rec["locked_by_worker"]

    log_state_change(
        logger,
        from_state="running",
        to_state="abandoned",
        job_id=str(job_id),
        worker_id=str(locked_by_worker) if locked_by_worker is not None else None,
        attempt=rec["attempt"],
    )
    return True


# ── mark_snoozed ───────────────────────────────────────────────────────


async def _mark_snoozed(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
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
        rec = await conn.fetchrow(
            sql.mark_snoozed,
            job_id,
            worker_id,
            delay,
            jsonb_param(metadata_update),
            progress_seq,
            jsonb_param(progress_state),
            outcome,
        )
        if rec is None:
            return "noop"

        branch = rec["outcome_branch"]
        # The attempt row (outcome=$7, duration_ms computed from the
        # winning arm's own timestamps) and the state_change event are
        # written by the fused statement's per-arm CTEs — see
        # _sql_templates.mark_snoozed.  Snoozed leaves finished_at NULL;
        # "now" comes off the statement, never this process's clock.

    if branch == "snoozed":
        log_state_change(
            logger,
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=rec["attempt"],
        )
        return "scheduled"
    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=rec["attempt"],
    )
    return "failed"


# ── mark_retry_after ───────────────────────────────────────────────────


async def _mark_retry_after(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    consume_budget: bool = True,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
    branch: str
    async with pool.acquire() as conn:
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
        # The attempt row and state_change event are written by the
        # fused statement's per-arm CTEs (attempt outcome/error fields
        # and the snoozed arms' now_ts-based duration included) — see
        # _sql_templates.mark_retry_after_consume_*.

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
