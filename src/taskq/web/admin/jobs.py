"""Job detail admin page: full state, attempt history, event log.

Also includes the /jobs list page with live/archived tabs
to ensure route registration order (static paths before {job_id}).
"""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from jinja2 import Environment

from taskq.backend._protocol import Backend, JobId
from taskq.constants import events_channel
from taskq.settings import TaskQSettings
from taskq.web.admin._constants import (
    _ACTIVE_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _ALL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _FETCH_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _TERMINAL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    parse_job_statuses,
)
from taskq.web.admin._factory import (
    get_backend,
    get_csrf_token,
    get_pg_pool,
    get_realtime_ctx,
    get_schema,
    get_settings,
    get_templates,
    validate_csrf,
)
from taskq.web.admin._jsonb import decode_jsonb

# ── Jobs list page constants ─────────────────────────────────────────────

# Sortable columns per tab. Value: SQL column name, cursor type.
# Cursor type 'ts' = timestamp, 'text' = text, 'int' = integer.
_SORTABLE_LIVE: dict[str, tuple[str, str]] = {
    "created_at": ("created_at", "ts"),
    "actor": ("actor", "text"),
    "queue": ("queue", "text"),
    "status": ("status", "text"),
    "attempt": ("attempt", "int"),
}
_SORTABLE_ARCHIVE: dict[str, tuple[str, str]] = {
    "finished_at": ("finished_at", "ts"),
    "created_at": ("created_at", "ts"),
    "actor": ("actor", "text"),
    "queue": ("queue", "text"),
    "status": ("status", "text"),
    "attempt": ("attempt", "int"),
}

_TIME_RANGE_MAP: dict[str, timedelta] = {
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}

_LIVE_COLS = (
    "id, actor, queue, status, created_at, scheduled_at, started_at, finished_at, "
    "CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL "
    "  THEN extract(epoch from finished_at - started_at) * 1000 "
    "  ELSE NULL END AS duration_ms, "
    "attempt, max_attempts, priority, identity_key, fairness_key, "
    "locked_by_worker, cancel_requested_at, progress_state, error_message, "
    "tags"
)

_ARCHIVE_COLS = (
    "id, actor, queue, status, created_at, scheduled_at, started_at, finished_at, "
    "CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL "
    "  THEN extract(epoch from finished_at - started_at) * 1000 "
    "  ELSE NULL END AS duration_ms, "
    "attempt, max_attempts, priority, identity_key, fairness_key, "
    "archived_at, error_message, tags"
)

_TRACEBACK_DISPLAY_LIMIT: int = 2000

_JOB_SQL = 'SELECT * FROM "{schema}".jobs WHERE id = $1'

_JOB_ARCHIVE_SQL = 'SELECT * FROM "{schema}".jobs_archive WHERE id = $1'

_ATTEMPTS_SQL = 'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt'

_ATTEMPTS_ARCHIVE_SQL = (
    'SELECT * FROM "{schema}".job_attempts_archive WHERE job_id = $1 ORDER BY attempt'
)

_EVENTS_SQL = 'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at'

# ── Jobs list page helpers ───────────────────────────────────────────────


def _build_where(
    statuses: list[str],
    actor: str | None,
    queue: str | None,
    time_from: str | None,
    time_to: str | None,
    identity_key: str | None,
    fairness_key: str | None,
    search: str | None,
    tags: list[str] | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = ["status = ANY($1)"]
    params: list[Any] = [statuses]
    idx = 2
    if actor:
        clauses.append(f"actor ILIKE '%' || ${idx} || '%'")
        params.append(actor)
        idx += 1
    if queue:
        clauses.append(f"queue = ${idx}")
        params.append(queue)
        idx += 1
    if time_from:
        clauses.append(f"created_at >= ${idx}::timestamptz")
        params.append(time_from)
        idx += 1
    if time_to:
        clauses.append(f"created_at <= ${idx}::timestamptz")
        params.append(time_to)
        idx += 1
    if identity_key:
        clauses.append(f"identity_key = ${idx}")
        params.append(identity_key)
        idx += 1
    if fairness_key:
        clauses.append(f"fairness_key = ${idx}")
        params.append(fairness_key)
        idx += 1
    if search:
        clauses.append(f"(id::text ILIKE '%' || ${idx} || '%' OR actor ILIKE '%' || ${idx} || '%')")
        params.append(search)
        idx += 1
    if tags:
        clauses.append(f"tags && ${idx}::text[]")
        params.append(tags)
        idx += 1
    return " AND ".join(clauses), params


def _build_order(
    sort: str, order: str, sortable: dict[str, tuple[str, str]]
) -> tuple[str, str, str | None]:
    """Return (order_clause, cursor_col, cursor_type) for validated sort params."""
    col, ctype = sortable.get(sort, (None, None))
    if col is None:
        col, ctype = next(iter(sortable.values()))
    direction = "DESC" if order == "desc" else "ASC"
    return f"{col} {direction}, id {direction}", col, ctype


def _build_paginated_sql(
    schema: str,
    table: str,
    cols: str,
    sortable: dict[str, tuple[str, str]],
    where: str,
    params: list[Any],
    cursor_at: str | None,
    cursor_id: str | None,
    cursor_dir: str,
    sort: str,
    order: str,
) -> tuple[str, list[Any]]:
    """Build a keyset-paginated SELECT for the given table and column list."""
    order_clause, cursor_col, cursor_type = _build_order(sort, order, sortable)
    from_clause = f'SELECT {cols} FROM "{schema}".{table}'

    cursor_clause = ""
    if cursor_at and cursor_id:
        op = "<" if cursor_dir == "next" else ">"
        if cursor_type == "ts":
            cursor_clause = f" AND ({cursor_col}, id) {op} (${len(params) + 1}::timestamptz, ${len(params) + 2}::uuid)"
        elif cursor_type == "int":
            cursor_clause = (
                f" AND ({cursor_col}, id) {op} (${len(params) + 1}::int, ${len(params) + 2}::uuid)"
            )
            cursor_at = str(int(float(cursor_at)))  # normalize
        else:
            cursor_clause = (
                f" AND ({cursor_col}, id) {op} (${len(params) + 1}, ${len(params) + 2}::uuid)"
            )
        params = [*params, cursor_at, cursor_id]

    outer_order = order_clause
    # Reverse for prev direction
    if cursor_dir == "prev":
        rev_order = (
            order_clause.replace("DESC", "~~TMP~~").replace("ASC", "DESC").replace("~~TMP~~", "ASC")
        )
        inner = (
            f"{from_clause} WHERE {where} {cursor_clause} ORDER BY {rev_order} LIMIT {_FETCH_SIZE}"
        )
        return f"SELECT * FROM ({inner}) sub ORDER BY {outer_order}", params
    sql = f"{from_clause} WHERE {where} {cursor_clause} ORDER BY {outer_order} LIMIT {_FETCH_SIZE}"
    return sql, params


def _parse_time_range(
    time_range: str | None,
    time_from: str | None,
    time_to: str | None,
) -> tuple[str | None, str | None]:
    if time_from and time_to:
        return time_from, time_to
    if time_range and time_range in _TIME_RANGE_MAP:
        delta = _TIME_RANGE_MAP[time_range]
        to_dt = datetime.now(UTC)
        from_dt = to_dt - delta
        return from_dt.isoformat(), to_dt.isoformat()
    return None, None


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    for key, val in row.items():
        if isinstance(val, datetime):
            row[key] = val.isoformat()
        elif isinstance(val, uuid.UUID):
            row[key] = str(val)
        elif key == "tags" and isinstance(val, list):
            # asyncpg returns text[] as list; pass through as-is for template
            pass
    return row


def _truncate_traceback(tb: str | None) -> str | None:
    if tb is None:
        return None
    if len(tb) <= _TRACEBACK_DISPLAY_LIMIT:
        return tb
    remaining = len(tb) - _TRACEBACK_DISPLAY_LIMIT
    suffix = f"\n... ({remaining} more characters)"
    return tb[: _TRACEBACK_DISPLAY_LIMIT - len(suffix)] + suffix


def register(router: APIRouter) -> None:
    """Attach job detail, cancel, list, count, and SSE routes to *router*."""

    # ── Jobs list page ──────────────────────────────────────────────────
    # Must be registered BEFORE /jobs/{job_id} to avoid route conflicts.

    @router.get("/jobs", response_class=HTMLResponse)
    async def jobs_list(  # pyright: ignore[reportUnusedFunction]  # Why: FastAPI decorator pattern prevents pyright from seeing registration via router.get().
        request: Request,
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        tab: str = Query(default="live"),
        status: list[str] = Query(default=[]),
        actor: str | None = Query(default=None, max_length=128),
        queue: str | None = Query(default=None),
        time_range: str | None = Query(default=None),
        time_from: str | None = Query(default=None),
        time_to: str | None = Query(default=None),
        identity_key: str | None = Query(default=None),
        fairness_key: str | None = Query(default=None),
        search: str | None = Query(default=None),
        tags: str | None = Query(default=None),
        cursor_at: str | None = Query(default=None),
        cursor_id: str | None = Query(default=None),
        cursor_dir: str = Query(default="next"),
        sort: str = Query(default=""),
        order: str = Query(default="desc"),
        live: str = Query(default="on"),
    ) -> HTMLResponse:
        if tab not in ("live", "archived"):
            tab = "live"

        default_statuses = sorted(_ALL_STATUSES if tab == "live" else _TERMINAL_STATUSES)
        statuses = (
            parse_job_statuses(status, default=default_statuses) if status else default_statuses
        )
        t_from, t_to = _parse_time_range(time_range, time_from, time_to)

        tag_list: list[str] | None = None
        if tags:
            tag_list = [t.strip() for t in tags.split(",") if t.strip()]

        where, params = _build_where(
            statuses,
            actor,
            queue,
            t_from,
            t_to,
            identity_key,
            fairness_key,
            search,
            tags=tag_list,
        )

        if tab == "live":
            query_sql, query_params = _build_paginated_sql(
                schema,
                "jobs",
                _LIVE_COLS,
                _SORTABLE_LIVE,
                where,
                params,
                cursor_at,
                cursor_id,
                cursor_dir,
                sort,
                order,
            )
        else:
            query_sql, query_params = _build_paginated_sql(
                schema,
                "jobs_archive",
                _ARCHIVE_COLS,
                _SORTABLE_ARCHIVE,
                where,
                params,
                cursor_at,
                cursor_id,
                cursor_dir,
                sort,
                order,
            )

        async with pool.acquire() as conn:
            rows = await conn.fetch(query_sql, *query_params)

        overfetched = len(rows) > _PAGE_SIZE
        display_rows = [_normalize_row(dict(r)) for r in rows[:_PAGE_SIZE]]

        # `overfetched` only tells us whether more rows exist on the side of
        # the result set we just queried (the direction of `cursor_dir`).
        # A page reached via "prev" already knows a "next" page exists (we
        # came from it), and vice versa — so has_next/has_prev must be
        # direction-aware rather than both derived from the same flag.
        if cursor_dir == "prev":
            has_prev = overfetched
            has_next = bool(cursor_at)
        else:
            has_next = overfetched
            has_prev = bool(cursor_at)

        next_cursor_at: str = ""
        next_cursor_id: str = ""
        prev_cursor_at: str = ""
        prev_cursor_id: str = ""
        if display_rows:
            # Use the active sort column as the cursor key
            sortable = _SORTABLE_LIVE if tab == "live" else _SORTABLE_ARCHIVE
            cursor_col, _ = sortable.get(sort, next(iter(sortable.values())))
            last = display_rows[-1]
            next_cursor_at = str(last.get(cursor_col, ""))
            next_cursor_id = str(last["id"])
            first = display_rows[0]
            prev_cursor_at = str(first.get(cursor_col, ""))
            prev_cursor_id = str(first["id"])

        realtime_mode, mode_label = realtime_ctx
        is_htmx = request.headers.get("HX-Request") == "true"

        context = {
            "jobs": display_rows,
            "tab": tab,
            "statuses": statuses,
            "all_statuses": sorted(_ALL_STATUSES if tab == "live" else _TERMINAL_STATUSES),
            "active_statuses": sorted(_ACTIVE_STATUSES),
            "terminal_statuses": sorted(_TERMINAL_STATUSES),
            "actor_filter": actor or "",
            "queue_filter": queue or "",
            "time_range": time_range or "",
            "time_from": t_from or "",
            "time_to": t_to or "",
            "identity_key": identity_key or "",
            "fairness_key": fairness_key or "",
            "search": search or "",
            "tags_filter": tags or "",
            "live": live,
            "has_next": has_next,
            "has_prev": has_prev,
            "next_cursor_at": next_cursor_at,
            "next_cursor_id": next_cursor_id,
            "prev_cursor_at": prev_cursor_at,
            "prev_cursor_id": prev_cursor_id,
            "cursor_dir": cursor_dir,
            "sort": sort,
            "order": order,
            "total_rows": len(display_rows),
            "realtime_mode": realtime_mode,
            "mode_label": mode_label,
            "suppress_refresh": True,
        }

        if is_htmx:
            html = tmpl.get_template("_partials/job_table.html").render(**context)
        else:
            html = tmpl.get_template("jobs.html").render(**context)

        return HTMLResponse(content=html)

    # ── Job count endpoint ────────────────────────────────────────────

    @router.get("/jobs/count")
    async def jobs_count(  # pyright: ignore[reportUnusedFunction]  # Why: FastAPI decorator pattern prevents pyright from seeing registration via router.get().
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tab: str = Query(default="live"),
        status: list[str] = Query(default=[]),
        actor: str | None = Query(default=None),
        queue: str | None = Query(default=None),
        time_range: str | None = Query(default=None),
        time_from: str | None = Query(default=None),
        time_to: str | None = Query(default=None),
    ) -> dict[str, Any]:
        statuses = (
            parse_job_statuses(status)
            if status
            else sorted(_ALL_STATUSES if tab == "live" else _TERMINAL_STATUSES)
        )
        t_from, t_to = _parse_time_range(time_range, time_from, time_to)
        where, params = _build_where(statuses, actor, queue, t_from, t_to, None, None, None)
        table = f'"{schema}".jobs' if tab == "live" else f'"{schema}".jobs_archive'
        count_sql = f"SELECT COUNT(*) FROM {table} WHERE {where}"
        async with pool.acquire() as conn:
            cnt = await conn.fetchval(count_sql, *params)
        return {"count": int(cnt) if cnt else 0}

    # ── SSE endpoint for live job updates ─────────────────────────────

    @router.get("/jobs/sse/live")
    async def jobs_sse(  # pyright: ignore[reportUnusedFunction]  # Why: FastAPI decorator pattern prevents pyright from seeing registration via router.get().
        request: Request,
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
    ) -> StreamingResponse:
        channel = events_channel(schema)

        async def event_stream() -> AsyncGenerator[str, None]:
            from taskq.web.admin._listen import listen_with_reconnect

            async for payload in listen_with_reconnect(pool, channel):
                if await request.is_disconnected():
                    return
                if payload is None:
                    yield ": keepalive\n\n"
                else:
                    yield f"event: state_change\ndata: {payload}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # ── Job detail ─────────────────────────────────────────────────────

    @router.get("/jobs/{job_id}", response_class=HTMLResponse)
    async def job_detail(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        job_id: uuid.UUID,
        request: Request,
        csrf_token: str = Depends(get_csrf_token),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        job_sql = _JOB_SQL.format(schema=schema)
        attempts_sql = _ATTEMPTS_SQL.format(schema=schema)
        events_sql = _EVENTS_SQL.format(schema=schema)

        is_archived = False
        archived_at: datetime | None = None

        async with pool.acquire() as conn:
            job: asyncpg.Record | None = await conn.fetchrow(job_sql, job_id)
            if job is not None:
                attempts = await conn.fetch(attempts_sql, job_id)
                events = await conn.fetch(events_sql, job_id)
            else:
                job_archive_sql = _JOB_ARCHIVE_SQL.format(schema=schema)
                job = await conn.fetchrow(job_archive_sql, job_id)
                if job is None:
                    raise HTTPException(status_code=404, detail="Job not found")
                is_archived = True
                archived_at = job["archived_at"]
                attempts_archive_sql = _ATTEMPTS_ARCHIVE_SQL.format(schema=schema)
                attempts = await conn.fetch(attempts_archive_sql, job_id)
                events: list[asyncpg.Record] = []

        job_dict = _normalize_row(dict(job))
        job_dict["error_traceback"] = _truncate_traceback(job_dict.get("error_traceback"))
        for _jsonb_key in ("progress_state", "payload", "metadata", "result"):
            job_dict[_jsonb_key] = decode_jsonb(job_dict.get(_jsonb_key))
        attempts_list = [_normalize_row(dict(a)) for a in attempts]
        for a in attempts_list:
            a["error_traceback"] = _truncate_traceback(a.get("error_traceback"))
        events_list = [_normalize_row(dict(e)) for e in events]
        for e in events_list:
            e["detail"] = decode_jsonb(e.get("detail"))

        realtime_mode, mode_label = realtime_ctx

        html = tmpl.get_template("job_detail.html").render(
            job=job_dict,
            attempts=attempts_list,
            events=events_list,
            terminal_statuses=_TERMINAL_STATUSES,
            is_archived=is_archived,
            archived_at=archived_at,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
            csrf_token=csrf_token,
        )
        return HTMLResponse(content=html)

    @router.post("/jobs/{job_id}/cancel")
    async def job_cancel(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        job_id: uuid.UUID,
        _csrf: None = Depends(validate_csrf),
        reason: str | None = Query(default=None),
        backend: Backend | None = Depends(get_backend),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(
                status_code=403,
                detail="Admin actions are disabled. Set TASKQ_ADMIN_ACTIONS_ENABLED=true to enable.",
            )
        if backend is None:
            raise HTTPException(
                status_code=503, detail="Backend not configured for admin operations"
            )

        job = await backend.get(JobId(job_id))
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status in _TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail="Job is already in a terminal state")

        await backend.write_cancel_request(JobId(job_id), reason)

        return RedirectResponse(url=f"../../jobs/{job_id}", status_code=303)
