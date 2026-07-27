"""Read-only query operations for PostgresBackend.

``get``, ``list_jobs``, ``count_pending_jobs``, ``get_attempts``, and
``get_events`` live here as module-level functions taking
``(pool, sql: SqlTemplates, ...)`` parameters.
"""

from datetime import timedelta
from typing import TYPE_CHECKING

from taskq._json import dumps_str
from taskq.backend._cursor import decode_cursor
from taskq.backend._protocol import (
    AttemptRow,
    EventRow,
    JobFilter,
    JobId,
    JobRow,
    JobSortField,
    LongRunningJobEventsWriter,
)
from taskq.backend._records import (
    _job_row_from_record,
    jsonb_to_dict,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.constants import RECLAIM_EVENT_VISIBILITY_DELAY

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "_check_reclaim_visibility_risk",
    "_count_pending_jobs",
    "_get",
    "_get_attempts",
    "_get_events",
    "_list_jobs",
    "_poll_reclaim_events",
]


async def _get(pool: "asyncpg.Pool", sql: SqlTemplates, job_id: JobId) -> JobRow | None:
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(sql.get_job, job_id)
    if rec is None:
        return None
    return _job_row_from_record(rec)


async def _list_jobs(
    pool: "asyncpg.Pool",
    schema: str,
    filters: JobFilter,
) -> list[JobRow]:
    conditions: list[str] = []
    params: list[object] = []
    n = 0

    def _next_param(expr: str) -> str:
        nonlocal n
        n += 1
        return f"{expr} = ${n}"

    if filters.queue is not None:
        conditions.append(_next_param("queue"))
        params.append(filters.queue)
    if filters.status is not None:
        conditions.append(_next_param("status"))
        params.append(filters.status)
    if filters.actor is not None:
        conditions.append(_next_param("actor"))
        params.append(filters.actor)
    if filters.identity_key is not None:
        conditions.append(_next_param("identity_key"))
        params.append(filters.identity_key)
    if filters.batch_id is not None:
        n += 1
        conditions.append(f"metadata @> ${n}::jsonb")
        params.append(dumps_str({"batch_id": str(filters.batch_id)}))

    if filters.tags is not None and len(filters.tags) > 0:
        n += 1
        conditions.append(f"tags && ${n}::text[]")
        params.append(list(filters.tags))

    if filters.cursor is not None:
        cursor_priority, cursor_scheduled_at, cursor_id = decode_cursor(filters.cursor)
        n += 1
        p_idx = n
        n += 1
        s_idx = n
        n += 1
        i_idx = n
        conditions.append(
            f"(priority < ${p_idx} OR "
            f"(priority = ${p_idx} AND scheduled_at > ${s_idx}) OR "
            f"(priority = ${p_idx} AND scheduled_at = ${s_idx} AND id > ${i_idx}))"
        )
        params.extend([cursor_priority, cursor_scheduled_at, cursor_id])

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    n += 1
    limit_idx = n
    params.append(filters.limit)

    if filters.order_by is JobSortField.CREATED_AT_DESC:
        order_clause = "ORDER BY created_at DESC, id ASC"
    elif filters.order_by is JobSortField.FINISHED_AT_DESC:
        order_clause = "ORDER BY finished_at DESC NULLS LAST, id ASC"
    else:
        order_clause = "ORDER BY priority DESC, scheduled_at ASC, id ASC"

    sql_text = (
        f'SELECT * FROM "{schema}".jobs {where_clause} '  # Why: schema validated at construction; dynamic WHERE clauses use positional params.
        f"{order_clause} "
        f"LIMIT ${limit_idx}"
    )

    async with pool.acquire() as conn:
        records = await conn.fetch(sql_text, *params)

    return [_job_row_from_record(r) for r in records]


async def _count_pending_jobs(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    actors: list[str],
) -> dict[str, int]:
    if not actors:
        return {}
    async with pool.acquire() as conn:
        records = await conn.fetch(sql.count_pending_jobs, actors)
    return {str(rec["actor"]): int(rec["cnt"]) for rec in records}


async def _get_attempts(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
) -> list[AttemptRow]:
    async with pool.acquire() as conn:
        records = await conn.fetch(sql.get_attempts, job_id)
    return [
        AttemptRow(
            job_id=JobId(rec["job_id"]),
            attempt=rec["attempt"],
            started_at=rec["started_at"],
            finished_at=rec["finished_at"],
            outcome=rec["outcome"],  # type: ignore[arg-type]  # Why: asyncpg returns PG value as str; AttemptOutcome is Literal[str, ...]
            error_class=rec["error_class"],
            error_message=rec["error_message"],
            error_traceback=rec["error_traceback"],
            duration_ms=rec["duration_ms"],
            worker_id=rec["worker_id"],
            metadata=jsonb_to_dict(rec["metadata"]) or {},
        )
        for rec in records
    ]


async def _get_events(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
) -> list[EventRow]:
    async with pool.acquire() as conn:
        records = await conn.fetch(sql.get_events, job_id)
    return [
        EventRow(
            event_id=rec["event_id"],
            job_id=JobId(rec["job_id"]),
            occurred_at=rec["occurred_at"],
            kind=rec["kind"],  # type: ignore[arg-type]  # Why: asyncpg returns PG value as str; EventKind is Literal[str, ...]
            detail=jsonb_to_dict(rec["detail"]) or {},
        )
        for rec in records
    ]


async def _poll_reclaim_events(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    after_id: int,
    limit: int = 100,
    *,
    visibility_delay: timedelta | None = None,
) -> list[EventRow]:
    delay = visibility_delay if visibility_delay is not None else RECLAIM_EVENT_VISIBILITY_DELAY
    async with pool.acquire() as conn:
        records = await conn.fetch(sql.poll_reclaim_events, after_id, limit, delay)
    return [
        EventRow(
            event_id=rec["event_id"],
            job_id=JobId(rec["job_id"]),
            occurred_at=rec["occurred_at"],
            kind=rec["kind"],  # type: ignore[arg-type]  # Why: asyncpg returns PG value as str; EventKind is Literal[str, ...]
            detail=jsonb_to_dict(rec["detail"]) or {},
        )
        for rec in records
    ]


async def _check_reclaim_visibility_risk(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    *,
    visibility_delay: timedelta | None = None,
) -> list[LongRunningJobEventsWriter]:
    """Diagnostic: transactions holding a lock on ``job_events`` for
    longer than *visibility_delay* — a candidate cause of a silently
    missed ``poll_reclaim_events`` event. See
    :class:`LongRunningJobEventsWriter`. Not part of the ``Backend``
    protocol (diagnostic, not a core operation) — ``InMemoryBackend``
    therefore has no equivalent, so a dedicated monitoring loop consuming
    this must branch on backend type (or ``getattr``-probe, as
    ``taskq.client._taskq._probe_visibility_risk`` does). Wired into
    ``TaskQ.watch_reclaims`` on a slow cadence by default (see
    ``taskq.client._taskq._probe_visibility_risk``); also callable
    directly from a dedicated monitoring/alerting loop that wants a
    tighter cadence or its own sink. Deliberately not on the per-poll
    hot path.
    """
    delay = visibility_delay if visibility_delay is not None else RECLAIM_EVENT_VISIBILITY_DELAY
    async with pool.acquire() as conn:
        records = await conn.fetch(sql.check_reclaim_visibility_risk, delay)
    return [
        LongRunningJobEventsWriter(
            pid=rec["pid"],
            xact_start=rec["xact_start"],
            xact_age_seconds=rec["xact_age_seconds"],
        )
        for rec in records
    ]
