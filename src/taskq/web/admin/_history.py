"""Historical job list and per-actor metrics for completed/archived jobs."""

import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment

from taskq.web.admin._constants import (
    _ALL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _FETCH_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    parse_job_statuses,
)
from taskq.web.admin._factory import get_pg_pool, get_realtime_ctx, get_schema, get_templates

logger = structlog.get_logger("taskq.web.admin.history")

_STATS_LIMIT: int = 200
_COUNT_CAP: int = 1001  # fetch one over 1000 so we can display "1000+"
_CURSOR_NULL_SENTINEL: str = "__NULL__"
_CURSOR_FAR_FUTURE: datetime = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)

# ── History list SQL ────────────────────────────────────────────────────

# Shared column list — both tables have identical core columns.
_SELECT_COLS = (
    "id, actor, queue, status, finished_at, created_at, started_at, "
    "CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL "
    "  THEN extract(epoch from finished_at - started_at) * 1000 "
    "  ELSE NULL END AS duration_ms, "
    "attempt, max_attempts, "
    "true AS is_archived, "
    "CASE WHEN status IN ('pending', 'scheduled', 'running') THEN 0 ELSE 1 END AS status_priority"
)
_SELECT_COLS_LIVE = (
    "id, actor, queue, status, finished_at, created_at, started_at, "
    "CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL "
    "  THEN extract(epoch from finished_at - started_at) * 1000 "
    "  ELSE NULL END AS duration_ms, "
    "attempt, max_attempts, "
    "false AS is_archived, "
    "CASE WHEN status IN ('pending', 'scheduled', 'running') THEN 0 ELSE 1 END AS status_priority"
)

_HISTORY_SQL_FIRST = f"""\
SELECT {_SELECT_COLS}
FROM "{{schema}}".jobs_archive
WHERE status = ANY($1)
  AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')
  AND ($3::text IS NULL OR queue = $3)
UNION ALL
SELECT {_SELECT_COLS_LIVE}
FROM "{{schema}}".jobs
WHERE status = ANY($1)
  AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')
  AND ($3::text IS NULL OR queue = $3)
ORDER BY status_priority,
  finished_at DESC NULLS LAST, created_at DESC, id DESC
LIMIT {{limit}}"""

_HISTORY_SQL_CURSOR = f"""\
SELECT {_SELECT_COLS}
FROM "{{schema}}".jobs_archive
WHERE status = ANY($1)
  AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')
  AND ($3::text IS NULL OR queue = $3)
  AND (COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz), id) < ($4, $5)
UNION ALL
SELECT {_SELECT_COLS_LIVE}
FROM "{{schema}}".jobs
WHERE status = ANY($1)
  AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')
  AND ($3::text IS NULL OR queue = $3)
  AND (COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz), id) < ($4, $5)
ORDER BY status_priority,
  finished_at DESC NULLS LAST, created_at DESC, id DESC
LIMIT {{limit}}"""

_SUMMARY_SQL = (
    f"SELECT status, count(*) AS cnt "
    f"FROM ("
    f'    SELECT status FROM "{{schema}}".jobs_archive'
    f"    WHERE status = ANY($1)"
    f"      AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')"
    f"      AND ($3::text IS NULL OR queue = $3)"
    f"    UNION ALL"
    f'    SELECT status FROM "{{schema}}".jobs'
    f"    WHERE status = ANY($1)"
    f"      AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')"
    f"      AND ($3::text IS NULL OR queue = $3)"
    f"    LIMIT {_COUNT_CAP}"
    f") sub GROUP BY status"
)

# ── Stats SQL ───────────────────────────────────────────────────────────

_STATS_SQL = f"""\
SELECT
    j.actor,
    j.queue,
    count(*) AS total,
    count(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
    count(*) FILTER (WHERE j.status = 'failed') AS failed,
    count(*) FILTER (WHERE j.status = 'cancelled') AS cancelled,
    count(*) FILTER (WHERE j.status = 'crashed') AS crashed,
    count(*) FILTER (WHERE j.status = 'abandoned') AS abandoned,
    round(avg(a.duration_ms))::bigint AS avg_duration_ms,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY a.duration_ms)::bigint AS p50_duration_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)::bigint AS p95_duration_ms
FROM "{{schema}}".jobs_archive j
LEFT JOIN "{{schema}}".job_attempts_archive a ON a.job_id = j.id
GROUP BY j.actor, j.queue
ORDER BY total DESC
LIMIT {_STATS_LIMIT}"""


def _compute_success_rate(summary: dict[str, int]) -> float | None:
    terminal = sum(summary.get(s, 0) for s in ("succeeded", "failed", "crashed", "abandoned"))
    if terminal == 0:
        return None
    return round(summary.get("succeeded", 0) / terminal * 100, 1)


def register(router: APIRouter) -> None:
    """Attach history list and stats routes to *router*."""

    @router.get("/history", response_class=HTMLResponse)
    async def history_list(  # pyright: ignore[reportUnusedFunction]  # Why: FastAPI decorator pattern prevents pyright from seeing registration via router.get().
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        status: list[str] = Query(default=[]),
        actor: str | None = Query(default=None, max_length=128),
        queue: str | None = Query(default=None),
        cursor_at: str | None = Query(default=None),
        cursor_id: str | None = Query(default=None),
    ) -> HTMLResponse:
        if actor == "":
            actor = None
        if queue == "":
            queue = None
        if cursor_at == "":
            cursor_at = None
        if cursor_id == "":
            cursor_id = None

        statuses = parse_job_statuses(status)

        parsed_at: datetime | None = None
        parsed_id: uuid.UUID | None = None
        if cursor_at is not None or cursor_id is not None:
            if cursor_at is None or cursor_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="both cursor_at and cursor_id must be provided together",
                )
            if cursor_at == _CURSOR_NULL_SENTINEL:
                parsed_at = _CURSOR_FAR_FUTURE
            else:
                try:
                    parsed_at = datetime.fromisoformat(cursor_at)
                except (ValueError, TypeError):
                    raise HTTPException(
                        status_code=400,
                        detail=f"cursor_at is not a valid ISO 8601 timestamp: {cursor_at!r}",
                    ) from None
            try:
                parsed_id = uuid.UUID(cursor_id)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=400,
                    detail=f"cursor_id is not a valid UUID: {cursor_id!r}",
                ) from None

        list_first_sql = _HISTORY_SQL_FIRST.format(schema=schema, limit=_FETCH_SIZE)
        list_cursor_sql = _HISTORY_SQL_CURSOR.format(schema=schema, limit=_FETCH_SIZE)
        summary_sql = _SUMMARY_SQL.format(schema=schema)

        async with pool.acquire() as conn:
            if parsed_at is not None and parsed_id is not None:
                rows = await conn.fetch(
                    list_cursor_sql,
                    statuses,
                    actor,
                    queue,
                    parsed_at,
                    parsed_id,
                )
            else:
                rows = await conn.fetch(list_first_sql, statuses, actor, queue)
            summary_rows = await conn.fetch(summary_sql, statuses, actor, queue)

        has_next = len(rows) > _PAGE_SIZE
        display_rows = list(rows[:_PAGE_SIZE])

        next_cursor_at: str | None = None
        next_cursor_id: str | None = None
        if has_next and display_rows:
            last = display_rows[-1]
            next_cursor_at = (
                _CURSOR_NULL_SENTINEL
                if last["finished_at"] is None
                else last["finished_at"].isoformat()
            )
            next_cursor_id = str(last["id"])

        summary: dict[str, int] = {r["status"]: r["cnt"] for r in summary_rows}
        total_shown = sum(summary.values())
        total_display = (
            f"{min(total_shown, _COUNT_CAP - 1):,}+"
            if total_shown >= _COUNT_CAP
            else f"{total_shown:,}"
        )
        success_rate = _compute_success_rate(summary)

        jobs = [dict(r) for r in display_rows]
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("history.html").render(
            jobs=jobs,
            statuses=statuses,
            all_statuses=sorted(_ALL_STATUSES),
            actor_filter=actor,
            queue_filter=queue,
            has_next=has_next,
            next_cursor_at=next_cursor_at,
            next_cursor_id=next_cursor_id,
            summary=summary,
            total_display=total_display,
            success_rate=success_rate,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)

    @router.get("/api/history/stats")
    async def history_stats(  # pyright: ignore[reportUnusedFunction]  # Why: FastAPI decorator pattern prevents pyright from seeing registration via router.get().
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
    ) -> JSONResponse:
        stats_sql = _STATS_SQL.format(schema=schema)
        async with pool.acquire() as conn:
            rows = await conn.fetch(stats_sql)
        data: list[dict[str, Any]] = [dict(r) for r in rows]
        return JSONResponse(content={"actors": data})
