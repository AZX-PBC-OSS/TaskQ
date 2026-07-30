"""Actors overview and deregister admin pages."""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment

from taskq.actor_config_ops import deregister_actor, list_actor_summaries
from taskq.exceptions import ActorDeregistrationError, ActorNotFoundError
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

_NOTICE_MESSAGES: dict[str, str] = {
    "deregistered": "Actor deregistered successfully.",
}


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
        actors: list[dict[str, object]] = []
        async with pool.acquire() as conn:
            actors = await list_actor_summaries(conn, schema=schema)
        realtime_mode, mode_label = realtime_ctx
        notice_text: str | None = _NOTICE_MESSAGES.get(notice) if notice else None
        html = tmpl.get_template("actors.html").render(
            actors=actors,
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
            except ActorNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from None
            except ActorDeregistrationError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from None

        return RedirectResponse(
            url=f"{base_path}/actors?notice=deregistered",
            status_code=303,
        )
