"""Job detail admin page: full state, attempt history, event log.

Also includes the /jobs list page with live/archived tabs
to ensure route registration order (static paths before {job_id}).
"""

import uuid
from collections.abc import AsyncGenerator
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from jinja2 import Environment

from taskq.backend._cursor import CursorValue, JobOrdering, SortColumn
from taskq.backend._protocol import Backend, JobId
from taskq.constants import events_channel
from taskq.settings import TaskQSettings
from taskq.web._sse_limit import acquire_sse_slot
from taskq.web.admin._constants import (
    _ACTIVE_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _ALL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _FETCH_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _TERMINAL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    parse_job_statuses,
    parse_job_tags,
    parse_text_filter,
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

# Sortable columns per tab, as the same :class:`SortColumn` the backend's
# ``list_jobs`` orderings are built from — so the admin's keyset and the
# client's page through one implementation instead of two that drift.
# ``descending`` here is a placeholder: the request's ``order`` supplies
# it in _build_order.  ``nullable`` marks the columns that take NULLS
# LAST placement and therefore have a NULL range to page through.
_SORTABLE_LIVE: dict[str, SortColumn] = {
    "created_at": SortColumn("created_at", "ts", descending=True),
    "actor": SortColumn("actor", "text", descending=True),
    "queue": SortColumn("queue", "text", descending=True),
    "status": SortColumn("status", "text", descending=True),
    "attempt": SortColumn("attempt", "int", descending=True),
}
_SORTABLE_ARCHIVE: dict[str, SortColumn] = {
    "finished_at": SortColumn("finished_at", "ts", descending=True, nullable=True),
    "created_at": SortColumn("created_at", "ts", descending=True),
    "actor": SortColumn("actor", "text", descending=True),
    "queue": SortColumn("queue", "text", descending=True),
    "status": SortColumn("status", "text", descending=True),
    "attempt": SortColumn("attempt", "int", descending=True),
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
    *,
    within: timedelta | None = None,
) -> tuple[str, list[Any]]:
    """Build the WHERE clause and its positional parameters.

    ``within`` is the relative "last N" filter and is evaluated against
    ``clock_timestamp()`` server-side rather than against a Python
    ``datetime.now(UTC)`` bound: ``created_at`` is written by the database
    clock, so anchoring the window in this process's clock would shift the
    whole window by the app-to-database skew and silently drop rows written
    in the last few seconds. ``time_from``/``time_to`` stay absolute --
    those are the caller's explicit instants, not a "now" of ours.
    """
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
    if within is not None:
        clauses.append(f"created_at >= clock_timestamp() - ${idx}::interval")
        params.append(within)
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


def _is_descending(order: str) -> bool:
    """Single source of truth for the sort direction.

    ``_build_order`` and the keyset predicate must agree exactly: a
    predicate derived independently of the ORDER BY is how ascending Next
    ended up filtering backwards.
    """
    return order == "desc"


def _build_order(sort: str, order: str, sortable: dict[str, SortColumn]) -> JobOrdering:
    """Resolve the requested sort to the ordering that pages it.

    ``id`` is appended in the SAME direction as the primary column, never
    against it: the keyset predicate is a row-wise tuple comparison, which
    can only describe a page seam when every column of the tuple sorts the
    same way.  ``id`` is UUIDv7 and therefore time-ordered, so ordering it
    with the primary column reorders nothing in practice.
    """
    col = sortable.get(sort) or next(iter(sortable.values()))
    descending = _is_descending(order)
    return JobOrdering(
        (
            replace(col, descending=descending),
            SortColumn("id", "uuid", descending=descending),
        )
    )


def _cursor_values(
    ordering: JobOrdering, cursor_at: str | None, cursor_id: str | None
) -> tuple[CursorValue, ...] | None:
    """Parse the query-string cursor to the columns' own Python types.

    The cursor round-trips through the query string as text, but asyncpg
    infers each placeholder's type from its ``::`` cast and refuses a
    ``str`` for a ``timestamptz``/``uuid``/``int`` parameter -- so a raw
    hand-off raised ``DataError`` and 500'd every timestamp page turn.

    An empty ``cursor_at`` on a nullable column is not a missing cursor:
    it is a seam *inside* the NULL range (``finished_at DESC NULLS LAST``
    ends in the unfinished rows), and parses to ``None``.  A malformed
    cursor -- hand-edited URL, stale bookmark, or an empty value on a
    column that has no NULL range -- returns ``None`` so the caller falls
    back to the unpaged first page rather than surfacing a driver error.
    """
    if not cursor_id:
        return None
    try:
        return (ordering.columns[0].parse(cursor_at or ""), ordering.columns[-1].parse(cursor_id))
    except ValueError:
        return None


def _build_paginated_sql(
    schema: str,
    table: str,
    cols: str,
    sortable: dict[str, SortColumn],
    where: str,
    params: list[Any],
    cursor_at: str | None,
    cursor_id: str | None,
    cursor_dir: str,
    sort: str,
    order: str,
) -> tuple[str, list[Any]]:
    """Build a keyset-paginated SELECT for the given table and column list."""
    ordering = _build_order(sort, order, sortable)
    # "next" walks the ORDER BY forwards and "prev" walks it backwards.
    # The ordering renders both the reversed comparison and the reversed
    # NULLS placement from that one flag -- reversing only the directions
    # would strand the NULL range at the wrong end of a "prev" page.
    forward = cursor_dir != "prev"
    from_clause = f'SELECT {cols} FROM "{schema}".{table}'

    cursor_clause = ""
    values = _cursor_values(ordering, cursor_at, cursor_id)
    if values is not None:
        predicate, cursor_params = ordering.sql_after(values, len(params) + 1, forward=forward)
        cursor_clause = f" AND {predicate}"
        params = [*params, *cursor_params]

    outer_order = ordering.order_by_sql()
    if not forward:
        inner = (
            f"{from_clause} WHERE {where} {cursor_clause} "
            f"ORDER BY {ordering.order_by_sql(forward=False)} LIMIT {_FETCH_SIZE}"
        )
        return f"SELECT * FROM ({inner}) sub ORDER BY {outer_order}", params
    sql = f"{from_clause} WHERE {where} {cursor_clause} ORDER BY {outer_order} LIMIT {_FETCH_SIZE}"
    return sql, params


def _parse_time_range(
    time_range: str | None,
    time_from: str | None,
    time_to: str | None,
) -> tuple[str | None, str | None, timedelta | None]:
    """Resolve the time filter to ``(time_from, time_to, within)``.

    An explicit from/to pair is passed through as absolute instants. A
    named range ("1h", "7d", ...) resolves to a ``timedelta`` that
    :func:`_build_where` evaluates against the database clock -- see its
    docstring for why this is not resolved to an absolute bound here.
    """
    if time_from and time_to:
        return time_from, time_to, None
    if time_range and time_range in _TIME_RANGE_MAP:
        return None, None, _TIME_RANGE_MAP[time_range]
    return None, None, None


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


def _cursor_field(value: Any) -> str:
    """Render a row value as the ``cursor_at`` query parameter.

    A NULL sort value is an empty parameter, never the string ``"None"``:
    under NULLS LAST the unfinished rows are a real range that has to be
    paged through, and ``str(None)`` made its seam unparseable — the page
    turn then silently re-served the page the operator was already on.
    """
    return "" if value is None else str(value)


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
        # Capped like `actor` above: `search` drives TWO unanchored ILIKEs,
        # one of them against a cast `id::text`, evaluated per row.
        search: str | None = Query(default=None, max_length=128),
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

        # NUL guard before the text binds: each of these reaches a `text`
        # (or `text::timestamptz`) parameter, which asyncpg rejects with an
        # opaque 22021 — the same class the client path's JobFilter guards.
        actor = parse_text_filter(actor, "actor")
        queue = parse_text_filter(queue, "queue")
        identity_key = parse_text_filter(identity_key, "identity_key")
        fairness_key = parse_text_filter(fairness_key, "fairness_key")
        search = parse_text_filter(search, "search")
        time_from = parse_text_filter(time_from, "time_from")
        time_to = parse_text_filter(time_to, "time_to")

        default_statuses = sorted(_ALL_STATUSES if tab == "live" else _TERMINAL_STATUSES)
        statuses = (
            parse_job_statuses(status, default=default_statuses) if status else default_statuses
        )
        t_from, t_to, within = _parse_time_range(time_range, time_from, time_to)

        # Shared parser: dedupes, caps the item count and per-item length
        # (the enqueue-side tag contract), and 400s on abuse.
        tag_list: list[str] | None = parse_job_tags(tags)

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
            within=within,
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
        #
        # `cursor_id` and not `cursor_at` is what marks a page as paged-into:
        # on a NULLS LAST column the seam value itself is legitimately empty
        # (a cursor inside the `finished_at IS NULL` range), and reading
        # emptiness as "no cursor" hid the link back.
        paged_in = bool(cursor_id)
        if cursor_dir == "prev":
            has_prev = overfetched
            has_next = paged_in
        else:
            has_next = overfetched
            has_prev = paged_in

        next_cursor_at: str = ""
        next_cursor_id: str = ""
        prev_cursor_at: str = ""
        prev_cursor_id: str = ""
        if display_rows:
            # Use the active sort column as the cursor key
            sortable = _SORTABLE_LIVE if tab == "live" else _SORTABLE_ARCHIVE
            cursor_col = (sortable.get(sort) or next(iter(sortable.values()))).name
            last = display_rows[-1]
            next_cursor_at = _cursor_field(last.get(cursor_col))
            next_cursor_id = str(last["id"])
            first = display_rows[0]
            prev_cursor_at = _cursor_field(first.get(cursor_col))
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
        # Same 128-char cap the /jobs and /history routes already put on this
        # parameter. It feeds an unanchored `actor ILIKE '%' || $n || '%'` in
        # _build_where, which no index can serve, so an uncapped pattern is
        # matched against every row of a full scan.
        actor: str | None = Query(default=None, max_length=128),
        queue: str | None = Query(default=None),
        time_range: str | None = Query(default=None),
        time_from: str | None = Query(default=None),
        time_to: str | None = Query(default=None),
    ) -> dict[str, Any]:
        # Same NUL guard as /jobs: the count query binds the same text params.
        actor = parse_text_filter(actor, "actor")
        queue = parse_text_filter(queue, "queue")
        time_from = parse_text_filter(time_from, "time_from")
        time_to = parse_text_filter(time_to, "time_to")
        statuses = (
            parse_job_statuses(status)
            if status
            else sorted(_ALL_STATUSES if tab == "live" else _TERMINAL_STATUSES)
        )
        t_from, t_to, within = _parse_time_range(time_range, time_from, time_to)
        where, params = _build_where(
            statuses, actor, queue, t_from, t_to, None, None, None, within=within
        )
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
        settings: TaskQSettings = Depends(get_settings),
    ) -> StreamingResponse:
        channel = events_channel(schema)
        # Why capped here rather than in admin/sse.py: this endpoint lives in
        # jobs.py and never went through the `/sse/{topic}` handler, so the
        # `admin_max_sse_connections` semaphore that endpoint applies has never
        # covered it. Each connection holds a PG LISTEN connection and an
        # asyncio task for as long as the client stays open.
        semaphore = await acquire_sse_slot("admin-jobs-live", settings.admin_max_sse_connections)

        async def event_stream() -> AsyncGenerator[str, None]:
            from taskq.web.admin._listen import listen_with_reconnect

            try:
                async for payload in listen_with_reconnect(pool, channel):
                    if await request.is_disconnected():
                        return
                    if payload is None:
                        yield ": keepalive\n\n"
                    else:
                        yield f"event: state_change\ndata: {payload}\n\n"
            finally:
                # In the generator, not the handler: the slot is held for the
                # life of the stream, and a disconnect arrives as
                # CancelledError thrown in here.
                semaphore.release()

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
