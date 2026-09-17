"""Actors overview and deregister admin pages."""

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment

from taskq.actor_config_ops import deregister_actor, list_actor_summaries
from taskq.exceptions import ActorDeregistrationError, ActorNotFoundError
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web.admin._actor_stats import STATS_LIMIT, fetch_actor_stats
from taskq.web.admin._constants import parse_text_filter
from taskq.web.admin._factory import (
    get_admin_pool,
    get_base_path,
    get_csrf_token,
    get_realtime_ctx,
    get_schema,
    get_settings,
    get_templates,
    validate_csrf,
)

_NOTICE_MESSAGES: dict[str, str] = {
    "deregistered": "Actor deregistered successfully.",
}


def _merge_actor_stats(
    config_rows: list[dict[str, object]],
    stats_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Merge archive executor stats into the actor_config summaries by name.

    The config rows stay in name order; actors with archive history but no
    ``actor_config`` row are appended after them, hottest first (the stats
    read's own order), so an actor driving traffic is never invisible just
    because its config row has not been synced. ``failure_share`` is the
    failed fraction of the actor's archived jobs, rendered as a percent
    string; ``None`` when the actor has no archived jobs.
    """
    stats_by_actor: dict[str, dict[str, object]] = {str(r["actor"]): r for r in stats_rows}
    merged: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in config_rows:
        actor = str(row["actor"])
        seen.add(actor)
        stats = stats_by_actor.get(actor)
        row = dict(row)
        row["has_config"] = True
        row["jobs_total"] = stats["total"] if stats else None
        row["failed_count"] = stats["failed"] if stats else None
        row["p50_duration_ms"] = stats["p50_duration_ms"] if stats else None
        row["p95_duration_ms"] = stats["p95_duration_ms"] if stats else None
        row["last_activity_at"] = stats["last_activity_at"] if stats else None
        row["failure_share"] = _failure_share(row["jobs_total"], row["failed_count"])
        merged.append(row)
    for actor, stats in stats_by_actor.items():
        if actor in seen:
            continue
        merged.append(
            {
                "actor": actor,
                "queue": None,
                "max_concurrent": None,
                "max_pending": None,
                "active_job_count": 0,
                "enabled_schedule_count": 0,
                "updated_at": None,
                "has_config": False,
                "jobs_total": stats["total"],
                "failed_count": stats["failed"],
                "p50_duration_ms": stats["p50_duration_ms"],
                "p95_duration_ms": stats["p95_duration_ms"],
                "last_activity_at": stats["last_activity_at"],
                "failure_share": _failure_share(stats["total"], stats["failed"]),
            }
        )
    return merged


def _failure_share(total: object, failed: object) -> str | None:
    """Render the failed fraction of *total* as a percent string, or None."""
    if not isinstance(total, int) or not isinstance(failed, int) or total <= 0:
        return None
    return f"{round(failed / total * 100, 1)}%"


def register(router: APIRouter) -> None:
    """Attach actors overview and deregister routes to *router*."""

    @router.get("/actors", response_class=HTMLResponse)
    async def actors_overview(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        csrf_token: str = Depends(get_csrf_token),
        notice: str | None = None,
    ) -> HTMLResponse:
        actors: list[dict[str, object]] = []
        stats_rows: list[dict[str, object]] = []
        async with pool.acquire() as conn:
            actors = await list_actor_summaries(conn, schema=schema)
            stats_rows = await fetch_actor_stats(conn, schema=schema)
        merged = _merge_actor_stats(actors, stats_rows)
        stats_truncated = len(stats_rows) >= STATS_LIMIT
        realtime_mode, mode_label = realtime_ctx
        notice_text: str | None = _NOTICE_MESSAGES.get(notice) if notice else None
        html = tmpl.get_template("actors.html").render(
            actors=merged,
            stats_truncated=stats_truncated,
            stats_limit=STATS_LIMIT,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
            csrf_token=csrf_token,
            active_page="actors",
            notice=notice_text,
        )
        return HTMLResponse(content=html)

    # Note: {actor} matches a single path segment — actor names containing "/"
    # cannot be deregistered via the admin UI (use the CLI or client API instead).
    # This is an accepted limitation; %2F in URLs is decoded before routing.
    @router.post("/actors/{actor}/deregister")
    async def actor_deregister(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        actor: str,
        request: Request,
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")
        # The actor name from the path binds as a text parameter - the same
        # NUL guard the list filters apply, or a %00 is an opaque driver 500.
        parse_text_filter(actor, "actor")

        form = await request.form()
        force = form.get("force") == "true"
        purge_queue = form.get("purge_queue") == "true"

        async with pool.acquire() as conn:
            try:
                await deregister_actor(
                    conn, actor, force=force, purge_queue=purge_queue, schema=schema
                )
            except ActorNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from None
            except ActorDeregistrationError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from None

        return RedirectResponse(
            url=f"{base_path}/actors?notice=deregistered",
            status_code=303,
        )
