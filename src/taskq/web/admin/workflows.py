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

import contextlib
import uuid
from datetime import datetime
from typing import Any, Final, cast

import asyncpg
import structlog
from asyncpg.exceptions import UndefinedTableError
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from taskq._json import loads as _json_loads
from taskq.web._pool import BoundedPool
from taskq.web.admin._constants import reject_unknown_query_params
from taskq.web.admin._factory import (
    get_admin_pool,
    get_base_path,
    get_csrf_token,
    get_realtime_ctx,
    get_schema,
    get_settings,
    get_templates,
)
from taskq.web.admin._wf_actions import (
    RERUN_NOT_APPLIED_BANNER,
    RERUN_PARTIAL_BANNER,
    RERUN_RACED_BANNER,
    RERUN_REFUSED_BANNER,
    register_actions,
)
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
    "SELECT r.id, r.actor, r.status, r.created_at, r.finished_at, r.cancel_requested_at, "
    "r.metadata->>'workflow' AS workflow, "
    # THE RUN-LEVEL DEFERRAL FACES (C4 — the visibility gap's cure): a
    # pathologically deferring run is visible from the INVENTORY — the
    # sitting nodes' count (a node is sitting when it rests 'scheduled' on
    # a LIVE deferral stamp: the stamp's own attempt discriminator, the
    # run page's label law, guarded in SQL so a hostile non-numeric stamp
    # degrades to NULL — never a cast death), the earliest next fire, and
    # the deferral depth (the max snooze count among the sitting rows).
    # ONE bounded lateral per root row (the run's own node scan, the
    # workflow-rows partial index's shape) — the list's read budget
    # unchanged.
    # THE CAST'S BOUND (the re-review's 500): the discriminator's regex
    # admits at most NINE digits — int4's own domain tops at 2147483647
    # (10 digits) and the attempt ordinal's estate ceiling is 32767 (5),
    # so a 10+-digit stamp (an 11-digit overflow-shaped write
    # 500'd the WHOLE list on the ::int cast) is a HOSTILE stamp, refused
    # the badge: the CASE degrades it to NULL, NULL never equals the
    # row's attempt, the row drops out of the sitting count (the badge's
    # law keeps its discriminator; the page renders). The doubled braces
    # are the .format convention (the JSONB-landmine rule's own face).
    "d.deferring, d.next_fire, d.snooze_count "
    'FROM "{schema}".jobs r '
    "LEFT JOIN LATERAL ("
    "    SELECT count(*) AS deferring, min(j.scheduled_at) AS next_fire, "
    "           max(j.snooze_count) AS snooze_count "
    '    FROM "{schema}".jobs j '
    "    WHERE (j.metadata->>'flow_id')::uuid = r.id AND j.metadata ? 'flow_id' "
    "      AND j.step_key <> '__flow__' "
    "      AND j.status = 'scheduled' AND j.metadata ? 'wf_deferral' "
    "      AND CASE WHEN j.metadata->'wf_deferral'->>'attempt' ~ '^[0-9]{{1,9}}$' "
    "               THEN (j.metadata->'wf_deferral'->>'attempt')::int END = j.attempt"
    ") d ON true "
    "WHERE r.step_key = '__flow__' "
    "ORDER BY r.created_at DESC LIMIT $1"
)

_RUN_EVENTS_SQL = (
    "SELECT id, principal_subject, action, target_id, reason, detail, occurred_at "
    'FROM "{schema}".admin_audit '
    "WHERE (target_type = 'workflow_run' AND target_id = $1) "
    "OR (target_type = 'job' AND target_id LIKE $2) "
    "OR detail->>'run_id' = $1 "
    "ORDER BY id DESC LIMIT 50"
)


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _json_of(raw: object) -> object:
    """The jsonb column's decode for the template (asyncpg hands str for
    untyped jsonb params)."""
    if isinstance(raw, str):
        with contextlib.suppress(ValueError):
            return _json_loads(raw)
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


#: The refusal banner's closed vocabulary (the redirect the action
#: routes carry — the operator lands on the DEFINED refused-op state,
#: never a 400 wall: the extra conviction — a refusal redirect that the
#: page's own reject_unknown_query_params 400s is the teach-them-to-
#: retry mutation trap). The KEYS live on the action routes
#: (``_wf_actions``) so a route and its copy cannot drift apart.
#: THE COPY HONESTY (H-C9): the not-applied refusal names only causes
#: the CAS can actually hit — the attempt ceiling is NOT one (the
#: ordinal CONTINUES; no ceiling is consulted), and the impossible
#: cause is off the record. The ROOT KEY's own line (H-C1): the run's
#: root is not a node — its re-run is the RUN-rerun's concern.
REFUSAL_BANNERS: Final[dict[str, str]] = {
    "cancel-not-applied": (
        "Cancel not applied — the run was already terminal (or a cancel "
        "is in flight); nothing changed."
    ),
    RERUN_NOT_APPLIED_BANNER: (
        "Rerun not applied — nothing was resumable (the node is live, "
        "unknown, not terminal-failed, or the run's own root key — a run "
        "is not its own node; re-run its failed nodes, or start a new run)."
    ),
    RERUN_REFUSED_BANNER: (
        "Rerun refused — the run is cancelled; re-opening it is a second "
        "decision (create a new run, or un-cancel deliberately)."
    ),
    # THE DEADLOCK'S NAMED REFUSAL (the round-2 conviction): the race's
    # honest loser — the retry's writers raced, PG killed this one; the
    # winner's own voice is honest and the operator re-checks the settled
    # state. Never a 500 wall.
    RERUN_RACED_BANNER: (
        "Rerun refused — the run's writers raced and PG resolved the "
        "deadlock in another writer's favour; the retry may have landed "
        "OR the run was cancelled by the winner. Reload the page to see "
        "the settled state, then re-run this action if it is still needed."
    ),
}
_REFUSAL_PARAM: Final[str] = "error"

#: THE PARTIAL BANNER'S PREFIX (H-C5): ``rerun-partial:<rows>:<keys>:<total>``
#: — the half-rerun's counts ride the closed vocabulary's key.
_RERUN_PARTIAL_PREFIX: Final[str] = RERUN_PARTIAL_BANNER + ":"


def _rerun_partial_copy(value: str) -> str | None:
    """The partial rerun's banner text (the counts parsed back out of the
    redirect key); None when the shape is not the route's own (a hostile
    or malformed param falls through to the generic refusal line). The
    key's shape is ``rerun-partial:<rows>:<keys>:<total>`` — the copy
    speaks ROWS-WHERE-HONEST (the round-2 conviction): when the row and
    key counts agree the plain "N of M" stands; when a map key's grant
    moved several rows the copy names the rows."""
    parts = value.split(":")
    if len(parts) != 4 or not all(p.isdigit() for p in parts[1:]):
        return None
    rows, keys, total = int(parts[1]), int(parts[2]), int(parts[3])
    if rows == keys:
        return (
            f"Rerun partially applied — {rows} of {total} re-opened before the "
            "refusal; the rest is still failed (rerun again to continue, or cancel "
            "the run). The audit trail carries the counts."
        )
    return (
        f"Rerun partially applied — {rows} node rows re-opened across {keys} of "
        f"{total} failed keys before the refusal; the rest is still failed (rerun "
        "again to continue, or cancel the run). The audit trail carries the counts."
    )


def _refusal_banner(request: Request) -> str | None:
    """The refused-op banner's text for the redirect's ``?error=``
    param (None when absent). The VALUE is closed-vocabulary: a known
    key renders its named refusal (the partial rerun's counts parsed
    back out of its key); an unknown value still renders the
    defined refusal line naming the key (autoescaped) — the redirect
    must never dead-end on a 400 wall."""
    value = request.query_params.get(_REFUSAL_PARAM)
    if value is None:
        return None
    if value.startswith(_RERUN_PARTIAL_PREFIX):
        partial = _rerun_partial_copy(value)
        if partial is not None:
            return partial
    known = REFUSAL_BANNERS.get(value)
    if known is not None:
        return known
    return f"The last action was refused ({value}); nothing changed."


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
                # THE RUN-LEVEL DEFERRAL FACES (C4): the badge's inputs —
                # a run with no sitting deferral reads all-None/0 (the
                # defined absent face).
                "deferring": int(r["deferring"] or 0),
                "next_fire": _iso(r["next_fire"]),
                "snooze_count": int(r["snooze_count"] or 0),
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
        base_path: str = Depends(get_base_path),
    ) -> HTMLResponse:
        # The refusal banner's param is DECLARED (the cancel route's 303
        # carries it — the operator lands on the refused-op banner, the
        # defined state, never the 400 wall the empty allowed-set built).
        reject_unknown_query_params(request, (_REFUSAL_PARAM,))
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
            # THE UNINSTALLED PROBE (the degrade's real surface): the
            # fetch's early return (no ROOT row) never touched the
            # workflow tables — an uninstalled schema reads as an unknown
            # id and 404s. One bounded probe decides.
            async with pool.acquire() as conn:
                try:
                    await conn.fetchval(f'SELECT 1 FROM "{schema}".wf_signals LIMIT 1')
                except UndefinedTableError:
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
                # THE DEFERRAL LINE (the snoozed step's sitting state —
                # the "why is this step sitting here" answer): the
                # label is the row's truth (the stamp's own
                # discriminator), rendered beside the status chip.
                "deferral_label": n.deferral_label,
                # THE DEADLINE LINE (the wf start_to_close cure — the
                # bound where declared + the running face's reality):
                # the same row's-truth pattern, rendered beside the
                # deferral line.
                "deadline_label": n.deadline_label,
            }
            # THE COLLAPSE (Q1d): the map children render through their
            # source's hexagon — the page's node projection is the
            # collapsed one, the derivation reads every row.
            for n in view.nodes
            if not n.map_child
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
                "created_at": h["created_at"],
                # THE CLOCK FIELDS PASS THROUGH (the deadline face's
                # cure): the view's holds are ALREADY normalized
                # (_normalize_hold's contract — the clocks as ISO text,
                # JSON-safe), so a second _iso() here read a STRING,
                # answered None, and the page's deadline line rendered
                # the none-face for EVERY hold — the with-deadline face
                # was dead code (the deadline-face pin's red).
                "expires_at": h["expires_at"],
                "resolved_at": h["resolved_at"],
                # THE TIMER'S BRANCH (the timer-cures lane's cure 3): the
                # normalized row's own timer mark (the reserved prefix's
                # derivation) rides the projection — the template branches
                # on the ROW's truth: a timer renders AS a timer, no
                # resolve affordance.
                "is_timer": h.get("is_timer", False),
                "timer_key": h.get("timer_key"),
                "timer_fired_by": h.get("timer_fired_by"),
            }
            for h in view.holds
        ]
        boot = {
            "runId": view.run_id,
            # THE BASE_PATH LAW (the relative-URL 404 bug's conviction):
            # every URL the JS builds rides the mounted prefix — the
            # stream URL is server-built absolute-with-base-path, and the
            # basePath rides the boot for the JS's own fetches (the node
            # panel's drill-down, the Resolve door).
            "basePath": base_path,
            "mermaid": rows_mermaid(view),
            "state": run_state_json(view, seq=0),
            "streamUrl": f"{base_path}/api/runs/{view.run_id}/stream",
        }
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("workflow_detail.html").render(
            run=view,
            installed=True,
            derived=view.derive(),
            nodes=nodes,
            holds=holds,
            # THE REFUSED-OP BANNER (the extra conviction's cure): the
            # cancel route's redirect lands HERE, on the named refusal —
            # never on a 400 wall.
            refusal_banner=_refusal_banner(request),
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
