"""Workers overview and leader detail admin pages."""

from datetime import UTC, datetime

import asyncpg
import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from taskq.web.admin._factory import get_pg_pool, get_realtime_ctx, get_schema, get_templates
from taskq.web.admin._jsonb import decode_jsonb

logger = structlog.get_logger("taskq.web.admin.workers")


_WATCHDOG_THRESHOLD_SECONDS: int = 30

_WORKERS_SQL = (
    "SELECT w.*, (ml.worker_id IS NOT NULL) AS is_leader "
    'FROM "{schema}".workers w '
    'LEFT JOIN "{schema}".maintenance_leader ml ON ml.worker_id = w.id '
    "ORDER BY w.last_seen_at DESC"
)

_LEADER_SQL = (
    "SELECT ml.*, w.hostname, w.pid, "
    "w.last_seen_at AS worker_last_seen "
    'FROM "{schema}".maintenance_leader ml '
    'JOIN "{schema}".workers w ON ml.worker_id = w.id'
)


def _is_watchdog_healthy(last_seen_at: datetime | None) -> bool | None:
    if last_seen_at is None:
        return None
    now = datetime.now(UTC)
    if last_seen_at.tzinfo is None:
        last_seen_at = last_seen_at.replace(tzinfo=UTC)
    diff = (now - last_seen_at).total_seconds()
    return diff <= _WATCHDOG_THRESHOLD_SECONDS


def register(router: APIRouter) -> None:
    """Attach workers overview and leader detail routes to *router*."""

    @router.get("/workers", response_class=HTMLResponse)
    async def workers_overview(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        workers_sql = _WORKERS_SQL.format(schema=schema)
        rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            rows = await conn.fetch(workers_sql)
        workers = [dict(r) for r in rows]
        for w in workers:
            md = decode_jsonb(w.get("metadata"))
            w["notify_enabled"] = (
                bool(md.get("notify_enabled", False))  # pyright: ignore[reportUnknownArgumentType]  # Why: decode_jsonb returns object; isinstance(md, dict) narrows the container but pyright cannot narrow the dict value type, so the argument is statically unknown.
                if isinstance(md, dict)
                else False
            )
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("workers.html").render(
            workers=workers,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)

    @router.get("/leader", response_class=HTMLResponse)
    async def leader_detail(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        leader_sql = _LEADER_SQL.format(schema=schema)
        row: asyncpg.Record | None = None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(leader_sql)

        realtime_mode, mode_label = realtime_ctx

        if row is None:
            html = tmpl.get_template("leader.html").render(
                leader=None,
                watchdog_healthy=None,
                realtime_mode=realtime_mode,
                mode_label=mode_label,
            )
            return HTMLResponse(content=html)

        leader = dict(row)
        watchdog_healthy = _is_watchdog_healthy(leader.get("last_seen_at"))
        html = tmpl.get_template("leader.html").render(
            leader=leader,
            watchdog_healthy=watchdog_healthy,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)
