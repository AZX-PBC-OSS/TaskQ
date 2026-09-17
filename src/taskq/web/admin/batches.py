"""Batches overview admin page: batch lifecycle state from the batches table."""

import uuid
from datetime import datetime

import asyncpg
import structlog
from asyncpg.exceptions import UndefinedTableError
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from taskq.web.admin._factory import (
    get_pg_pool,
    get_realtime_ctx,
    get_schema,
    get_templates,
)

logger = structlog.get_logger("taskq.web.admin.batches")

# A read-only page with no filters pages badly; cap the render instead and
# tell the operator when the cap bit. Active batches sort first: they are
# the ones an incident cares about.
_BATCHES_PAGE_SIZE = 200

_BATCHES_SQL = (
    "SELECT id, queue, status, expected_size, consecutive_failures, "
    "failure_threshold, finalizer_job_id, originating_actor, created_at, "
    "completed_at "
    'FROM "{schema}".batches '
    "ORDER BY (status = 'active') DESC, created_at DESC "
    f"LIMIT {_BATCHES_PAGE_SIZE}"
)


def _normalize_batch(row: dict[str, object]) -> dict[str, object]:
    """Render a batch row's UUID and timestamp values as template-safe text."""
    for key, val in row.items():
        if isinstance(val, datetime):
            row[key] = val.isoformat()
        elif isinstance(val, uuid.UUID):
            row[key] = str(val)
    return row


def register(router: APIRouter) -> None:
    """Attach the batches overview route to *router*."""

    @router.get("/batches", response_class=HTMLResponse)
    async def batches_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        batches_sql = _BATCHES_SQL.format(schema=schema)

        batches_installed = True
        rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            try:
                rows = await conn.fetch(batches_sql)
            except UndefinedTableError:
                logger.debug("batches-table-missing")
                batches_installed = False

        batches = [_normalize_batch(dict(r)) for r in rows]
        truncated = batches_installed and len(rows) == _BATCHES_PAGE_SIZE
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("batches.html").render(
            batches=batches,
            batches_installed=batches_installed,
            notice_text="batches not installed; run taskq migrate up to enable",
            truncated=truncated,
            page_size=_BATCHES_PAGE_SIZE,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)
