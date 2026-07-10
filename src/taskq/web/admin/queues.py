"""Queue overview and queue detail admin pages with keyset pagination."""

import uuid
from datetime import datetime

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from taskq.web.admin._factory import (
    get_pg_pool,
    get_realtime_ctx,
    get_schema,
    get_templates,
)

logger = structlog.get_logger("taskq.web.admin.queues")

_ALLOWED_STATUSES: frozenset[str] = frozenset(
    {"pending", "scheduled", "running", "failed", "crashed", "abandoned", "cancelled", "succeeded"}
)
_PAGE_SIZE: int = 100
_FETCH_SIZE: int = _PAGE_SIZE + 1

_QUEUE_OVERVIEW_SQL = (
    "SELECT queue, "
    "count(*) FILTER (WHERE status = 'pending') AS pending_count, "
    "count(*) FILTER (WHERE status = 'scheduled') AS scheduled_count, "
    "count(*) FILTER (WHERE status = 'running') AS running_count, "
    "count(*) FILTER (WHERE status = 'failed') AS failed_count "
    'FROM "{schema}".jobs '
    "WHERE status IN ('pending','scheduled','running','failed') "
    "GROUP BY queue ORDER BY queue"
)

_ORPHAN_QUEUES_SQL = (
    "SELECT DISTINCT j.queue "
    'FROM "{schema}".jobs j '
    "WHERE j.status IN ('pending', 'scheduled') "
    "AND NOT EXISTS ("
    '    SELECT 1 FROM "{schema}".workers w '
    "    WHERE j.queue = ANY(w.queues) "
    "    AND w.last_seen_at > now() - interval '30 seconds'"
    ") "
    "ORDER BY j.queue"
)

_QUEUE_HAS_ALIVE_WORKER_SQL = (
    "SELECT EXISTS ("
    '    SELECT 1 FROM "{schema}".workers w '
    "    WHERE $1 = ANY(w.queues) "
    "    AND w.last_seen_at > now() - interval '30 seconds'"
    ")"
)

_QUEUE_DETAIL_SQL_FIRST = (
    "SELECT id, queue, actor, status, scheduled_at, attempt, max_attempts, "
    "created_at "
    'FROM "{schema}".jobs '
    "WHERE queue = $1 AND status = $2 "
    "ORDER BY scheduled_at, id LIMIT {limit}"
)

_QUEUE_DETAIL_SQL_CURSOR = (
    "SELECT id, queue, actor, status, scheduled_at, attempt, max_attempts, "
    "created_at "
    'FROM "{schema}".jobs '
    "WHERE queue = $1 AND status = $2 "
    "AND (scheduled_at, id) > ($3, $4) "
    "ORDER BY scheduled_at, id LIMIT {limit}"
)


def register(router: APIRouter) -> None:
    """Attach queue overview and queue detail routes to *router*."""

    @router.get("/queues", response_class=HTMLResponse)
    async def queue_overview(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        overview_sql = _QUEUE_OVERVIEW_SQL.format(schema=schema)
        orphan_sql = _ORPHAN_QUEUES_SQL.format(schema=schema)
        rows: list[asyncpg.Record] = []
        orphan_rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            rows = await conn.fetch(overview_sql)
            orphan_rows = await conn.fetch(orphan_sql)
        queues = [dict(r) for r in rows]
        orphan_queues: frozenset[str] = frozenset(str(r["queue"]) for r in orphan_rows)
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("queues.html").render(
            queues=queues,
            orphan_queues=orphan_queues,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)

    @router.get("/queues/{queue:path}", response_class=HTMLResponse)
    async def queue_detail(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        queue: str,
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        status: str = Query(default="pending"),
        cursor_at: str | None = Query(default=None),
        cursor_id: str | None = Query(default=None),
    ) -> HTMLResponse:
        if status not in _ALLOWED_STATUSES:
            raise HTTPException(status_code=400, detail=f"invalid status filter: {status!r}")

        detail_first_sql = _QUEUE_DETAIL_SQL_FIRST.format(schema=schema, limit=_FETCH_SIZE)
        detail_cursor_sql = _QUEUE_DETAIL_SQL_CURSOR.format(schema=schema, limit=_FETCH_SIZE)
        has_worker_sql = _QUEUE_HAS_ALIVE_WORKER_SQL.format(schema=schema)

        if cursor_at == "":
            cursor_at = None
        if cursor_id == "":
            cursor_id = None

        parsed_at: datetime | None = None
        parsed_id: uuid.UUID | None = None

        if cursor_at is not None or cursor_id is not None:
            if cursor_at is None or cursor_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="both cursor_at and cursor_id must be provided together",
                )
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

        rows: list[asyncpg.Record] = []
        has_alive_worker: bool = False
        async with pool.acquire() as conn:
            has_alive_worker = await conn.fetchval(has_worker_sql, queue) or False
            if parsed_at is not None and parsed_id is not None:
                rows = await conn.fetch(
                    detail_cursor_sql,
                    queue,
                    status,
                    parsed_at,
                    parsed_id,
                )
            else:
                rows = await conn.fetch(
                    detail_first_sql,
                    queue,
                    status,
                )

        has_next = len(rows) > _PAGE_SIZE
        display_rows = list(rows[:_PAGE_SIZE])
        next_cursor_at: str | None = None
        next_cursor_id: str | None = None
        if has_next and display_rows:
            last = display_rows[-1]
            if last["scheduled_at"] is not None:
                next_cursor_at = last["scheduled_at"].isoformat()
                next_cursor_id = str(last["id"])

        jobs = [dict(r) for r in display_rows]
        for j in jobs:
            if isinstance(j.get("id"), uuid.UUID):
                j["id"] = str(j["id"])
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("queue_detail.html").render(
            queue_name=queue,
            status=status,
            jobs=jobs,
            has_next=has_next,
            next_cursor_at=next_cursor_at,
            next_cursor_id=next_cursor_id,
            allowed_statuses=sorted(_ALLOWED_STATUSES),
            has_alive_worker=has_alive_worker,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)
