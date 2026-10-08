"""The workflow-run explorer's PAGES (T11 + the run-explorer addendum):
the runs list (the inventory question) and the run page (the graph, the
holds, the audit trail — all from the rows). The machine routes (the
node panel, the SSE stream, the typed actions) register from
``_wf_actions``; the split is the read/write concern boundary.

THE NO-JS STORY (stated honestly): the estate's admin is JS-dependent
for live views — this page's no-JS surface is the SERVER-RENDERED
INITIAL SNAPSHOT (the state at page load, from the same grouped query
the boot JSON carries); no interactive no-JS mode is invented. The page
sets ``suppress_refresh=True`` — the meta refresh is the killer (it
destroys the live SVG and re-renders from snapshot); the SSE stream is
the live transport.

THE GRAPH: the boot JSON carries the run's Mermaid text FROM THE ROWS
(``_wf_rows.rows_mermaid`` — the live graph is a row fact, not a
definition import); the vendored mermaid renders it once, ``bindSvg``
stamps ``data-node-key``, and every later update is a classList/text
patch on those nodes (the render-once, patch-forever contract).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, cast

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
    get_csrf_token,
    get_realtime_ctx,
    get_schema,
    get_settings,
    get_templates,
)
from taskq.web.admin._wf_actions import register_actions
from taskq.web.admin._wf_rows import (
    NOT_INSTALLED,
    RunView,
    fetch_run_view,
    rows_mermaid,
    run_state_json,
    status_class,
)

logger = structlog.get_logger("taskq.web.admin.workflows")

#: The runs list's cap (the batches page's read-only pagination idiom:
#: a read-only page with no filters pages badly; cap the render and say
#: when the cap bit).
_RUNS_PAGE_SIZE = 200

_RUNS_SQL = (
    "SELECT id, actor, status, created_at, finished_at, cancel_requested_at, "
    "metadata->>'workflow' AS workflow "
    'FROM "{schema}".jobs WHERE step_key = \'__flow__\' '
    "ORDER BY created_at DESC LIMIT $1"
)

_RUN_EVENTS_SQL = (
    "SELECT id, principal_subject, action, target_id, reason, detail, occurred_at "
    'FROM "{schema}".admin_audit '
    "WHERE (target_type = 'workflow_run' AND target_id = $1) "
    "OR (target_type = 'job' AND target_id LIKE $2) "
    "ORDER BY id DESC LIMIT 50"
)


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _json_of(raw: object) -> object:
    """The jsonb column's decode for the template (asyncpg hands str for
    untyped jsonb params)."""
    if isinstance(raw, str):
        import contextlib

        with contextlib.suppress(json.JSONDecodeError):
            return json.loads(raw)
    return raw


def _field_of(payload: object, field_name: str) -> str | int | float | bool | None:
    """The hold's reason/tool/args — they ride INSIDE the held row's
    payload jsonb until the deliver overwrites it (T10's deliver-no-drop
    shape; the panel reads the SAME rows the knock carries)."""
    doc = _json_of(payload)
    if not isinstance(doc, dict):
        return None
    value: object = cast(dict[str, object], doc).get(field_name)
    return value if isinstance(value, str | int | float | bool) else None


def register(router: APIRouter) -> None:
    """Attach the explorer's pages + the machine routes to *router*."""
    register_actions(router)

    @router.get("/workflows", response_class=HTMLResponse)
    async def workflows_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        request: Request,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        # The page declares no filters: any query param is undeclared, and
        # an undeclared param is refused, never silently dropped.
        reject_unknown_query_params(request, ())
        installed = True
        rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            try:
                rows = await conn.fetch(_RUNS_SQL.format(schema=schema), _RUNS_PAGE_SIZE)
            except UndefinedTableError:
                logger.debug("workflow-tables-missing")
                installed = False
        runs = [
            {
                "id": str(r["id"]),
                "workflow": r["workflow"] or r["actor"],
                "root_status": r["status"],
                "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ]
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("workflows.html").render(
            runs=runs,
            installed=installed,
            notice_text=NOT_INSTALLED,
            truncated=installed and len(rows) == _RUNS_PAGE_SIZE,
            page_size=_RUNS_PAGE_SIZE,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)

    @router.get("/workflows/{run_id}", response_class=HTMLResponse)
    async def workflow_detail(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        csrf_token: str = Depends(get_csrf_token),
        settings: Any = Depends(get_settings),
    ) -> HTMLResponse:
        reject_unknown_query_params(request, ())
        installed = True
        view: RunView | None = None
        events: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            try:
                view = await fetch_run_view(conn, schema, run_id)
                if view is not None:
                    events = await conn.fetch(
                        _RUN_EVENTS_SQL.format(schema=schema), str(run_id), f"{run_id}:%"
                    )
            except UndefinedTableError:
                installed = False
        if view is None:
            if not installed:
                html = tmpl.get_template("workflow_detail.html").render(
                    run=None,
                    installed=False,
                    notice_text=NOT_INSTALLED,
                    derived="",
                    nodes=[],
                    holds=[],
                    events=[],
                    boot=None,
                    csrf_token=csrf_token,
                    actions_enabled=False,
                    realtime_mode=realtime_ctx[0],
                    mode_label=realtime_ctx[1],
                )
                return HTMLResponse(content=html)
            raise HTTPException(status_code=404, detail="Workflow run not found")

        nodes = [
            {
                "key": n.key,
                "status": n.status,
                "status_class": status_class(n.status),
                "hold": n.hold is not None,
                "join_wait": n.view().is_join_wait,
                "map_done": n.map_done,
                "map_children": n.map_children,
                "error_class": n.error_class,
            }
            for n in view.nodes
        ]
        holds = [
            {
                "hold_id": h["id"],
                "signal": h["signal_name"],
                "node": h["node_key"],
                "epoch": h["hold_epoch"],
                "status": h["status"],
                "payload": _json_of(h["payload"]),
                "payload_schema": _json_of(h["payload_schema"]),
                "reason": _field_of(h["payload"], "reason"),
                "tool": _field_of(h["payload"], "tool"),
                "args": _field_of(h["payload"], "args"),
                "created_at": _iso(h["created_at"]),
                "expires_at": _iso(h["expires_at"]),
            }
            for h in view.holds
        ]
        boot = {
            "runId": view.run_id,
            "mermaid": rows_mermaid(view),
            "state": run_state_json(view, seq=0),
            "streamUrl": f"api/runs/{view.run_id}/stream",
        }
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("workflow_detail.html").render(
            run=view,
            installed=True,
            derived=view.derive(),
            nodes=nodes,
            holds=holds,
            events=[dict(e) | {"occurred_at": _iso(e["occurred_at"])} for e in events],
            boot=boot,
            csrf_token=csrf_token,
            actions_enabled=settings.admin_actions_enabled,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
            # THE META-REFRESH KILLER (the dragon, pinned): the page's
            # live transport is SSE — the full-document reload would
            # destroy the live SVG and re-render from snapshot.
            suppress_refresh=True,
        )
        return HTMLResponse(content=html)
