"""Batches overview and batch drilldown admin pages: batch lifecycle state
from the batches table, member jobs from the jobs ledger."""

import uuid
from datetime import datetime

import asyncpg
import structlog
from asyncpg.exceptions import UndefinedTableError
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from taskq.web._pool import BoundedPool
from taskq.web.admin._constants import reject_unknown_query_params
from taskq.web.admin._factory import (
    get_admin_pool,
    get_realtime_ctx,
    get_schema,
    get_templates,
)

logger = structlog.get_logger("taskq.web.admin.batches")

# A read-only page with no filters pages badly; cap the render instead and
# tell the operator when the cap bit. Active batches sort first: they are
# the ones an incident cares about.
_BATCHES_PAGE_SIZE = 200

# The drilldown's member cap: the batches page's own truncation idiom,
# applied to the batch's member jobs.
_BATCH_MEMBERS_CAP = 200

_BATCHES_SQL = (
    "SELECT id, queue, status, expected_size, consecutive_failures, "
    "failure_threshold, finalizer_job_id, originating_actor, created_at, "
    "completed_at "
    'FROM "{schema}".batches '
    "ORDER BY (status = 'active') DESC, created_at DESC "
    f"LIMIT {_BATCHES_PAGE_SIZE}"
)

_BATCH_SQL = (
    "SELECT id, queue, status, expected_size, consecutive_failures, "
    "failure_threshold, finalizer_job_id, originating_actor, created_at, "
    "completed_at, metadata "
    'FROM "{schema}".batches '
    "WHERE id = $1"
)

# Member status counts, live members only: a member that has left the jobs
# table is accounted for separately by the archived-member count, so the
# page never renders an active batch as if it had no members.
_BATCH_STATUS_COUNTS_SQL = (
    "SELECT status, count(*) AS count "
    'FROM "{schema}".jobs '
    "WHERE metadata->>'batch_id' = $1 "
    "GROUP BY status ORDER BY status"
)

# Batch members key on metadata->>'batch_id' (the enqueue path stamps it;
# jobs_batch_open_members_idx serves the open-member probe on the same
# expression). Bound as text against the extracted text, the shape the
# index is built on.
_BATCH_MEMBERS_SQL = (
    "SELECT id, actor, queue, status, attempt, max_attempts, retry_kind, "
    "created_at, finished_at "
    'FROM "{schema}".jobs '
    "WHERE metadata->>'batch_id' = $1 "
    "ORDER BY created_at DESC, id DESC "
    f"LIMIT {_BATCH_MEMBERS_CAP}"
)

_BATCH_ARCHIVED_MEMBERS_SQL = (
    "SELECT count(*) FROM \"{schema}\".jobs_archive WHERE metadata->>'batch_id' = $1"
)

_NOTICE_TEXT = "batches not installed; run taskq migrate up to enable"


def _normalize_batch(row: dict[str, object]) -> dict[str, object]:
    """Render a row's UUID and timestamp values as template-safe text."""
    for key, val in row.items():
        if isinstance(val, datetime):
            row[key] = val.isoformat()
        elif isinstance(val, uuid.UUID):
            row[key] = str(val)
    return row


def register(router: APIRouter) -> None:
    """Attach the batches overview and batch drilldown routes to *router*."""

    @router.get("/batches", response_class=HTMLResponse)
    async def batches_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        request: Request,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        # The page declares no filters: any query param is undeclared, and
        # an undeclared param is refused, never silently dropped.
        reject_unknown_query_params(request, ())
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
            notice_text=_NOTICE_TEXT,
            truncated=truncated,
            page_size=_BATCHES_PAGE_SIZE,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)

    @router.get("/batches/{batch_id}", response_class=HTMLResponse)
    async def batch_detail(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        batch_id: uuid.UUID,
        request: Request,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        reject_unknown_query_params(request, ())
        batch_sql = _BATCH_SQL.format(schema=schema)
        counts_sql = _BATCH_STATUS_COUNTS_SQL.format(schema=schema)
        members_sql = _BATCH_MEMBERS_SQL.format(schema=schema)
        archived_members_sql = _BATCH_ARCHIVED_MEMBERS_SQL.format(schema=schema)
        # The member queries bind the batch id as the text the metadata
        # extraction yields, the shape jobs_batch_open_members_idx is built
        # on (a uuid bind against extracted text is a type mismatch).
        batch_id_text = str(batch_id)

        realtime_mode, mode_label = realtime_ctx
        batch: asyncpg.Record | None = None
        status_counts: list[asyncpg.Record] = []
        members: list[asyncpg.Record] = []
        archived_members = 0
        async with pool.acquire() as conn:
            try:
                batch = await conn.fetchrow(batch_sql, batch_id)
            except UndefinedTableError:
                # The list page's degrade: a schema the batches migration
                # has not reached renders the notice instead of 500ing.
                logger.debug("batches-table-missing")
                html = tmpl.get_template("batch_detail.html").render(
                    batch=None,
                    batches_installed=False,
                    notice_text=_NOTICE_TEXT,
                    status_counts=[],
                    members=[],
                    truncated=False,
                    page_size=_BATCH_MEMBERS_CAP,
                    archived_members=0,
                    realtime_mode=realtime_mode,
                    mode_label=mode_label,
                )
                return HTMLResponse(content=html)
            if batch is None:
                raise HTTPException(status_code=404, detail="Batch not found")
            status_counts = await conn.fetch(counts_sql, batch_id_text)
            members = await conn.fetch(members_sql, batch_id_text)
            archived_members = int(await conn.fetchval(archived_members_sql, batch_id_text) or 0)

        truncated = len(members) == _BATCH_MEMBERS_CAP
        html = tmpl.get_template("batch_detail.html").render(
            batch=_normalize_batch(dict(batch)),
            batches_installed=True,
            status_counts=[dict(r) for r in status_counts],
            members=[_normalize_batch(dict(r)) for r in members],
            truncated=truncated,
            page_size=_BATCH_MEMBERS_CAP,
            archived_members=archived_members,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)
