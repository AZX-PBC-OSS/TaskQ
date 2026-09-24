"""Historical job list and per-actor metrics for completed/archived jobs."""

import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment

from taskq.client._taskq import orjson_response_class
from taskq.web._pool import BoundedPool
from taskq.web.admin._actor_stats import fetch_actor_stats, resolve_stats_window
from taskq.web.admin._constants import (
    _ALL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _FETCH_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    _PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
    parse_job_statuses,
    parse_text_filter,
)
from taskq.web.admin._factory import (
    get_admin_pool,
    get_realtime_ctx,
    get_schema,
    get_templates,
)

logger = structlog.get_logger("taskq.web.admin.history")

_COUNT_CAP: int = 1001  # fetch one over 1000 so we can display "1000+"
_CURSOR_NULL_SENTINEL: str = "__NULL__"
_CURSOR_FAR_FUTURE: datetime = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)

# JSON responses render through orjson (taskq._json), never stdlib json ,
# byte-identical bodies to starlette's stdlib JSONResponse for these payloads.
_OrjsonJSONResponse: "type[JSONResponse]" = orjson_response_class()

# ── History list SQL ────────────────────────────────────────────────────

# Shared column list, both tables have identical core columns.
# retry_kind feeds the attempt_budget macro: an indefinite row's stored
# max_attempts is inert, and a column the query never fetches can never be
# rendered (the jobs-list pin's shape).
_SELECT_COLS = (
    "id, actor, queue, status, finished_at, created_at, started_at, "
    "CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL "
    "  THEN extract(epoch from finished_at - started_at) * 1000 "
    "  ELSE NULL END AS duration_ms, "
    "attempt, max_attempts, retry_kind, "
    "true AS is_archived"
)
_SELECT_COLS_LIVE = (
    "id, actor, queue, status, finished_at, created_at, started_at, "
    "CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL "
    "  THEN extract(epoch from finished_at - started_at) * 1000 "
    "  ELSE NULL END AS duration_ms, "
    "attempt, max_attempts, retry_kind, "
    "false AS is_archived"
)

# The walk's sort key, stated once: the ORDER BY and the keyset cursor
# predicate must be the SAME tuple or the seam is not a seam -- the row-wise
# comparison can only describe "strictly after the last row shown" when it
# covers every column the ordering uses. The finished NULL range is pinned
# to the walk's top by the same COALESCE ceiling the cursor compares, so a
# NULL-finished row can never sit on the far side of a seam its cursor
# cannot describe. The old shape ordered by a status_priority CASE the
# cursor never carried and dropped created_at from the predicate entirely:
# rows whose id order disagreed with their created_at order (id is the
# ENQUEUING worker's UUIDv7 clock, created_at is the DATABASE clock --
# skewed workers invert the two) replayed the previous page and skipped
# rows no page ever served.
#
# The ordering cannot sit as a bare ORDER BY over the UNION ALL --
# PostgreSQL rejects an ORDER BY that is not an output column of the set
# operation ("invalid UNION/INTERSECT/EXCEPT ORDER BY clause") -- so the
# union is wrapped and the sort applied to the wrapper, the same shape the
# jobs list's reversed prev pages use.
#
# The wrapper sort is also where the walk's cost used to live: a sort
# over a set operation cannot be served by index order (the planner does
# not propagate pathkeys through computed set-operation output columns),
# so EVERY page turn read and top-N sorted every matching row of the
# archive, 18 ms at a 100k-row archive and growing linearly with
# retention. The walk therefore sorts and limits EACH BRANCH (the
# per-branch ORDER BY is the same tuple over a bare table, which the
# seam indexes 01.00.20_01 serves as an index scan that starts at the
# seam and stops at the branch limit) and the outer sort only merges the
# two branches' pages, at most 2 * limit rows: same total order, same
# seam, the page's cost independent of the archive's size.
_HISTORY_SEAM_PREDICATE = (
    "  AND (COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz), "
    "created_at, id) < ($4, $5, $6)"
)

_HISTORY_ORDER_BY = (
    "ORDER BY COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz) DESC, "
    "created_at DESC, id DESC"
)

_HISTORY_UNION_TEMPLATE = """\
SELECT * FROM (
  (SELECT {cols}
      FROM "{schema}".jobs_archive
      WHERE status = ANY($1)
        AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')
        AND ($3::text IS NULL OR queue = $3){seam}
      {order_by}
      LIMIT {limit})
UNION ALL
  (SELECT {cols_live}
      FROM "{schema}".jobs
      WHERE status = ANY($1)
        AND ($2::text IS NULL OR actor ILIKE '%' || $2 || '%')
        AND ($3::text IS NULL OR queue = $3){seam}
      {order_by}
      LIMIT {limit})
) sub
{order_by}
LIMIT {limit}"""


def _history_list_sql(schema: str, *, cursor: bool, limit: int) -> str:
    """Return the history list SELECT for *schema*, paged or not.

    ``cursor=True`` binds the keyset seam ($4 finished-or-ceiling, $5
    created_at, $6 id) into both sides' WHERE; each branch's ORDER BY is
    exactly the tuple that predicate compares, so the seam can neither
    replay nor skip a row the ordering places on one side of it. Each
    branch is sorted and limited to the page itself, and the outer sort
    merges the two pages under the same tuple -- the union of the two
    branches' top-``limit`` rows contains the global top-``limit`` rows,
    so the page is identical to sorting the whole union.
    """
    seam = f"\n{_HISTORY_SEAM_PREDICATE}" if cursor else ""
    union = _HISTORY_UNION_TEMPLATE.format(
        schema=schema,
        seam=seam,
        cols=_SELECT_COLS,
        cols_live=_SELECT_COLS_LIVE,
        order_by=_HISTORY_ORDER_BY,
        limit=limit,
    )
    return union


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
# The per-actor aggregate lives in _actor_stats.py, shared with the
# actors page; the endpoint below is a thin wrapper over it.


def _compute_success_rate(summary: dict[str, int]) -> float | None:
    terminal = sum(summary.get(s, 0) for s in ("succeeded", "failed", "crashed", "abandoned"))
    if terminal == 0:
        return None
    return round(summary.get("succeeded", 0) / terminal * 100, 1)


def register(router: APIRouter) -> None:
    """Attach history list and stats routes to *router*."""

    @router.get("/history", response_class=HTMLResponse)
    async def history_list(  # pyright: ignore[reportUnusedFunction]  # Why: FastAPI decorator pattern prevents pyright from seeing registration via router.get().
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        status: list[str] = Query(default=[]),
        actor: str | None = Query(default=None, max_length=128),
        queue: str | None = Query(default=None),
        cursor_at: str | None = Query(default=None),
        cursor_id: str | None = Query(default=None),
        cursor_created: str | None = Query(default=None),
    ) -> HTMLResponse:
        if actor == "":
            actor = None
        if queue == "":
            queue = None
        if cursor_at == "":
            cursor_at = None
        if cursor_id == "":
            cursor_id = None
        if cursor_created == "":
            cursor_created = None

        # NUL guard before the text binds ($2/$3): asyncpg rejects a NUL in
        # a text parameter with an opaque 22021, the same class the jobs
        # list filters guard against via parse_text_filter.
        actor = parse_text_filter(actor, "actor")
        queue = parse_text_filter(queue, "queue")

        statuses = parse_job_statuses(status)

        parsed_at: datetime | None = None
        parsed_id: uuid.UUID | None = None
        parsed_created: datetime | None = None
        if cursor_at is not None or cursor_id is not None or cursor_created is not None:
            # All three keys of the walk's sort tuple travel together: a
            # cursor carrying any two of them cannot describe a seam (the
            # missing key would silently re-admit rows the ordering puts
            # before it), so a partial cursor is the same clean 400 the
            # page's malformed-cursor contract already answers with.
            if cursor_at is None or cursor_id is None or cursor_created is None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "cursor_at, cursor_created and cursor_id must all be provided together"
                    ),
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
                parsed_created = datetime.fromisoformat(cursor_created)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"cursor_created is not a valid ISO 8601 timestamp: {cursor_created!r}"
                    ),
                ) from None
            try:
                parsed_id = uuid.UUID(cursor_id)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=400,
                    detail=f"cursor_id is not a valid UUID: {cursor_id!r}",
                ) from None

        list_first_sql = _history_list_sql(schema, cursor=False, limit=_FETCH_SIZE)
        list_cursor_sql = _history_list_sql(schema, cursor=True, limit=_FETCH_SIZE)
        summary_sql = _SUMMARY_SQL.format(schema=schema)

        async with pool.acquire() as conn:
            if parsed_at is not None and parsed_id is not None and parsed_created is not None:
                rows = await conn.fetch(
                    list_cursor_sql,
                    statuses,
                    actor,
                    queue,
                    parsed_at,
                    parsed_created,
                    parsed_id,
                )
            else:
                rows = await conn.fetch(list_first_sql, statuses, actor, queue)
            summary_rows = await conn.fetch(summary_sql, statuses, actor, queue)

        has_next = len(rows) > _PAGE_SIZE
        display_rows = list(rows[:_PAGE_SIZE])

        next_cursor_at: str | None = None
        next_cursor_id: str | None = None
        next_cursor_created: str | None = None
        if has_next and display_rows:
            last = display_rows[-1]
            next_cursor_at = (
                _CURSOR_NULL_SENTINEL
                if last["finished_at"] is None
                else last["finished_at"].isoformat()
            )
            next_cursor_created = last["created_at"].isoformat()
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
            next_cursor_created=next_cursor_created,
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
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        window: str | None = Query(default=None),
    ) -> JSONResponse:
        # Same closed-set window selector the actors page renders as a
        # toggle; None / "all" is the documented default.
        window_delta = resolve_stats_window(window)
        async with pool.acquire() as conn:
            data: list[dict[str, Any]] = await fetch_actor_stats(
                conn, schema=schema, per_queue=True, window=window_delta
            )
        return _OrjsonJSONResponse(content={"actors": data})
