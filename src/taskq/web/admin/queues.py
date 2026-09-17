"""Queue overview and queue detail admin pages with keyset pagination."""

import uuid
from datetime import datetime

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web.admin._constants import parse_text_filter
from taskq.web.admin._factory import (
    get_admin_pool,
    get_realtime_ctx,
    get_schema,
    get_settings,
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

# A read-only page with no filters bounds each roll-up's row count like
# the batches page: one row per queue, capped.
_QUEUE_ROW_CAP: int = 200

# Live workers per subscribed queue - the leader's queue-depth sampler
# read (worker/_leader_sweeps.py, _QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE):
# statement_timestamp() (STABLE) so the liveness bound stays a btree
# condition on workers_last_seen_idx, and the admin UI's own liveness
# window, so this page, the orphan banner and the stranded-jobs detector
# all agree on which worker counts as alive.
_QUEUE_LIVE_WORKERS_SQL = (
    "SELECT q AS queue, count(*) AS worker_count "
    'FROM "{schema}".workers w, unnest(w.queues) AS q '
    "WHERE w.last_seen_at > statement_timestamp() - make_interval(secs => $1) "
    "GROUP BY q "
    f"LIMIT {_QUEUE_ROW_CAP}"
)

# Stranded pending/scheduled rows per routing queue - the stranded-jobs
# detector's SQL shape (worker/_leader_sweeps.py, _stranded_jobs_loop)
# grouped by the queue dispatch routes on instead of by actor. The
# routing discriminator (the actor's stored assignment for a re-pended
# row, the row's own label otherwise) and the mutual exclusion of the two
# strand shapes are the detector's own: a row whose actor has no
# actor_config row counts once in the no-config shape and is never tested
# against the workers table. The pending/scheduled predicate is served
# index-only by jobs_dispatch_idx / jobs_scheduled_wake_idx (the partial
# indexes the dispatch CTE uses); the liveness bound by
# workers_last_seen_idx. No terminal row enters the read.
_QUEUE_STRANDED_SQL = f"""\
SELECT r.routing_queue AS queue, count(*) AS stranded_count
FROM (
    SELECT j.actor,
           CASE WHEN j.assignment_routed THEN ac.queue ELSE j.queue END
             AS routing_queue,
           NOT EXISTS (
             SELECT 1 FROM "{{schema}}".actor_config ac2 WHERE ac2.actor = j.actor
           ) AS no_actor_config
    FROM "{{schema}}".jobs j
    LEFT JOIN "{{schema}}".actor_config ac ON ac.actor = j.actor
    WHERE j.status IN ('pending', 'scheduled')
) r
WHERE r.no_actor_config
   OR (
       NOT r.no_actor_config
       AND NOT EXISTS (
         SELECT 1 FROM "{{schema}}".workers w
         WHERE r.routing_queue = ANY(w.queues)
           AND w.last_seen_at > statement_timestamp() - make_interval(secs => $1)
       )
   )
GROUP BY r.routing_queue
ORDER BY stranded_count DESC
LIMIT {_QUEUE_ROW_CAP}"""

_ORPHAN_QUEUES_SQL = (
    "SELECT DISTINCT j.queue "
    'FROM "{schema}".jobs j '
    "WHERE j.status IN ('pending', 'scheduled') "
    "AND NOT EXISTS ("
    '    SELECT 1 FROM "{schema}".workers w '
    "    WHERE j.queue = ANY(w.queues) "
    "    AND w.last_seen_at > clock_timestamp() - make_interval(secs => {live_secs})"
    ") "
    "ORDER BY j.queue"
)

_QUEUE_HAS_ALIVE_WORKER_SQL = (
    "SELECT EXISTS ("
    '    SELECT 1 FROM "{schema}".workers w '
    "    WHERE $1 = ANY(w.queues) "
    "    AND w.last_seen_at > clock_timestamp() - make_interval(secs => {live_secs})"
    ")"
)

_QUEUE_DETAIL_SQL_FIRST = (
    "SELECT id, queue, actor, status, scheduled_at, attempt, max_attempts, "
    "retry_kind, "
    "created_at "
    'FROM "{schema}".jobs '
    "WHERE queue = $1 AND status = $2 "
    "ORDER BY scheduled_at, id LIMIT {limit}"
)

_QUEUE_DETAIL_SQL_CURSOR = (
    "SELECT id, queue, actor, status, scheduled_at, attempt, max_attempts, "
    "retry_kind, "
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
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        settings: TaskQSettings = Depends(get_settings),
    ) -> HTMLResponse:
        overview_sql = _QUEUE_OVERVIEW_SQL.format(schema=schema)
        orphan_sql = _ORPHAN_QUEUES_SQL.format(
            schema=schema, live_secs=settings.admin_worker_liveness_seconds
        )
        live_workers_sql = _QUEUE_LIVE_WORKERS_SQL.format(schema=schema)
        stranded_sql = _QUEUE_STRANDED_SQL.format(schema=schema)
        rows: list[asyncpg.Record] = []
        orphan_rows: list[asyncpg.Record] = []
        worker_rows: list[asyncpg.Record] = []
        stranded_rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            rows = await conn.fetch(overview_sql)
            orphan_rows = await conn.fetch(orphan_sql)
            worker_rows = await conn.fetch(live_workers_sql, settings.admin_worker_liveness_seconds)
            stranded_rows = await conn.fetch(stranded_sql, settings.admin_worker_liveness_seconds)
        queues = [dict(r) for r in rows]
        orphan_queues: frozenset[str] = frozenset(str(r["queue"]) for r in orphan_rows)
        live_by_queue: dict[str, int] = {
            str(r["queue"]): int(r["worker_count"]) for r in worker_rows
        }
        stranded_by_queue: dict[str, int] = {
            str(r["queue"]): int(r["stranded_count"]) for r in stranded_rows
        }
        for q in queues:
            q["live_workers"] = live_by_queue.get(str(q["queue"]), 0)
            q["stranded_count"] = stranded_by_queue.get(str(q["queue"]), 0)
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
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        settings: TaskQSettings = Depends(get_settings),
        status: str = Query(default="pending"),
        cursor_at: str | None = Query(default=None),
        cursor_id: str | None = Query(default=None),
    ) -> HTMLResponse:
        # The queue name from the path binds as a text parameter in every
        # query below - the same NUL guard the list filters apply, or a
        # %00 in the URL is an opaque driver 500.
        parse_text_filter(queue, "queue")
        if status not in _ALLOWED_STATUSES:
            raise HTTPException(status_code=400, detail=f"invalid status filter: {status!r}")

        detail_first_sql = _QUEUE_DETAIL_SQL_FIRST.format(schema=schema, limit=_FETCH_SIZE)
        detail_cursor_sql = _QUEUE_DETAIL_SQL_CURSOR.format(schema=schema, limit=_FETCH_SIZE)
        has_worker_sql = _QUEUE_HAS_ALIVE_WORKER_SQL.format(
            schema=schema, live_secs=settings.admin_worker_liveness_seconds
        )

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
