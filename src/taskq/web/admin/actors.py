"""Actors overview and deregister admin pages."""

from urllib.parse import quote_plus

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment

from taskq.exceptions import ActorDeregistrationError
from taskq.settings import TaskQSettings
from taskq.web.admin._factory import (
    get_base_path,
    get_csrf_token,
    get_pg_pool,
    get_realtime_ctx,
    get_schema,
    get_settings,
    get_templates,
    validate_csrf,
)
from taskq.worker.actor_config_ops import deregister_actor

_ACTORS_SQL = """
SELECT ac.actor, ac.max_concurrent, ac.max_pending, ac.queue,
       ac.result_ttl, ac.metadata::text AS metadata, ac.updated_at::text AS updated_at,
       (SELECT count(*) FROM "{schema}".jobs j
        WHERE j.actor = ac.actor
        AND j.status IN ('pending', 'scheduled', 'running')) AS active_job_count,
       (SELECT count(*) FROM "{schema}".cron_schedules cs
        WHERE cs.actor = ac.actor AND cs.enabled = true) AS enabled_schedule_count
  FROM "{schema}".actor_config ac
 ORDER BY ac.actor
""".strip()


def register(router: APIRouter) -> None:
    """Attach actors overview and deregister routes to *router*."""

    @router.get("/actors", response_class=HTMLResponse)
    async def actors_overview(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        csrf_token: str = Depends(get_csrf_token),
        notice: str | None = None,
    ) -> HTMLResponse:
        actors_sql = _ACTORS_SQL.format(schema=schema)
        rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            rows = await conn.fetch(actors_sql)
        actors = [dict(r) for r in rows]
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("actors.html").render(
            actors=actors,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
            csrf_token=csrf_token,
            active_page="actors",
            notice=notice,
        )
        return HTMLResponse(content=html)

    @router.post("/actors/{actor}/deregister")
    async def actor_deregister(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        actor: str,
        request: Request,
        _csrf: None = Depends(validate_csrf),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")

        form = await request.form()
        force = form.get("force") == "true"
        purge_queue = form.get("purge_queue") == "true"

        async with pool.acquire() as conn:
            try:
                await deregister_actor(
                    conn, actor, force=force, purge_queue=purge_queue, schema=schema
                )
            except ActorDeregistrationError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from None

        return RedirectResponse(
            url=f"{base_path}/actors?notice=deregistered+{quote_plus(actor)}",
            status_code=303,
        )
