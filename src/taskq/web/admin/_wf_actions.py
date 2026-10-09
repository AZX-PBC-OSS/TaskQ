"""The workflow-run explorer's MACHINE routes (T11): the node panel API,
the SSE node_state stream (the revisioned-snapshot replay), and the
typed actions (deliver / resolve / cancel — each guarded, each audited).

The pages live in ``workflows``; the split is the read/write concern
boundary (the same shape the jobs page + backend routes keep).

THE STREAM (the replay contract, PINNED): every frame is a revisioned
FULL SNAPSHOT; the rows are the durable ring — a reconnect replays BY
CONSTRUCTION (the next snapshot carries the whole state), and the
seq-cursor drops stale frames client-side. A killed connection cannot
strand the page on a stale render, and a memory ring would be a second
copy of a ledger that already exists.

THE TYPED DOOR: deliver + resolve validate through the bound
``TypedGate``s of the WorkflowApp the host mounted (``create_router(
workflow_app=...)``). Without one the endpoints answer ``501`` with the
named reason — NO untyped deliver surface ships.

THE AUDIT (G4): every resolve/deliver/cancel writes its ``admin_audit``
row (principal + reason + run/node) — "who approved this" is a ROW, not
a log line. The resolve/deliver rides HitlClient's own same-tx row; the
cancel rides the engine cascade's row; the audit entries carry the
operator principal the auth dependency captured.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Annotated, Any, cast

import asyncpg
import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse

from taskq._json import dumps as _json_dumps
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web._sse_limit import acquire_sse_slot, release_after
from taskq.web.admin._factory import (
    get_admin_pool,
    get_base_path,
    get_principal,
    get_schema,
    get_settings,
    get_workflow_app,
    require_actions_enabled,
    validate_csrf,
)
from taskq.web.admin._wf_rows import (
    NOT_INSTALLED,
    RunView,
    fetch_run_view,
    run_state_json,
)

logger = structlog.get_logger("taskq.web.admin.wf_actions")

#: The SSE frame cadence (seconds) — the poll is the transport's pace;
#: the snapshot-per-frame shape is the replay contract's body.
_STREAM_POLL_S = 1.0

_STREAM_KEEPALIVE_S = 30.0

#: The SSE revision's source (the two ledgers' max clocks — the row
#: ids are uuid7, and max(uuid) is not a PG function; the clocks are
#: monotonic-enough revision facts for the emit decision, and the
#: frame's own seq carries the ordering): a composite string, one query.
_RUN_REVISION_SQL = (
    'SELECT COALESCE((SELECT max(l.updated_at)::text FROM "{schema}".wf_step_ledger l '
    "  WHERE l.flow_id = $1), 'none') || ':' || "
    'COALESCE((SELECT max(s.created_at)::text FROM "{schema}".wf_signals s '
    "  WHERE s.workflow_id = $1), 'none') || ':' || "
    'COALESCE((SELECT max(n.finished_at)::text FROM "{schema}".jobs n '
    "  WHERE (n.metadata->>'flow_id')::uuid = $1 AND n.metadata ? 'flow_id'), 'none') "
    "AS revision"
)

#: The node detail panel's read (the drill-down: status header, attempts,
#: the trace id, captured error, one upstream hop — §10.3's causal chain
#: via parent_id).
_NODE_PANEL_SQL = (
    "SELECT id, step_key, status, attempt, max_attempts, retry_kind, "
    "trace_id, error_class, error_message, error_traceback, "
    "metadata->>'error' AS captured_error, parent_id, "
    "created_at, started_at, finished_at "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND step_key <> '__flow__'"
)

_NODE_PARENT_SQL = (
    'SELECT id, step_key, status, error_class FROM "{schema}".jobs '
    'WHERE id = (SELECT parent_id FROM "{schema}".jobs WHERE id = $1)'
)

_NODE_LEDGER_SQL = (
    "SELECT step_key, attempt, status, error_class, error_message, created_at "
    'FROM "{schema}".wf_step_ledger WHERE job_id = $1 ORDER BY id'
)


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _validate_through_gates(
    wf_app: Any, hold: Any, decision: dict[str, object]
) -> dict[str, object]:
    """THE TYPED DOOR (the hitl-proof's send boundary): the payload
    re-validates against the bound gate's declared models BEFORE any row
    moves. A wrong payload raises the named 422 the operator SEES;
    nothing is delivered."""
    from pydantic import BaseModel, ValidationError

    from taskq.workflows._cli import gate_models_for

    models: list[type[BaseModel]] = []
    for workflow in getattr(wf_app, "_workflows", {}):
        try:
            models.extend(gate_models_for(wf_app, workflow, hold.node_key))
        except KeyError:
            continue
    if not models:
        raise HTTPException(
            status_code=422,
            detail=f"node {hold.node_key!r} declares no bound gate in the mounted "
            "definitions — the delivery refuses (no untyped door ships)",
        )
    members = hold.signal_name.split("|")
    candidates = [m for m in models if m.__name__ in members] or models
    errors: list[str] = []
    for model in candidates:
        try:
            validated = model.model_validate(decision)
        except ValidationError as exc:
            errors.append(
                f"{model.__name__}: "
                + "; ".join(
                    f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}" for e in exc.errors()
                )
            )
            continue
        dumped: dict[str, object] = validated.model_dump(mode="json")
        return dumped
    raise HTTPException(
        status_code=422, detail="pydantic refused the payload: " + " | ".join(errors)
    )


async def _form_body(request: Request) -> dict[str, Any]:
    """The action body's read — the guarded-POST discipline: the fields
    arrive FORM-ENCODED (the admin's own convention; the CSRF token is a
    form field, so the body must be one). ``decision``/``payload`` are
    JSON TEXT fields (the payload's shape is the typed door's business,
    parsed here — a malformed one is the named 400, never a traceback)."""
    form = await request.form()
    body: dict[str, Any] = {k: v for k, v in form.items() if isinstance(v, str)}
    for json_field in ("decision", "payload"):
        raw = body.get(json_field)
        if isinstance(raw, str):
            try:
                body[json_field] = _json_loads(raw)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail=f"{json_field} is not valid JSON"
                ) from None
    return body


async def _stream_generator(
    pool: BoundedPool, schema: str, run_id: uuid.UUID, cursor: int
) -> AsyncGenerator[str, None]:
    """The feed: snapshot-on-revision, full replacement each frame (the
    replay contract's body — see the module docstring).

    THE GENERATOR TRAP (P2's pin 6): state crosses as PARAMETERS — a
    reassignment of a variable captured from the endpoint's scope is the
    UnboundLocalError dragon; nothing here shadows it."""
    seq = cursor
    last_revision: str | None = None
    since_keepalive = 0.0
    while True:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(_RUN_REVISION_SQL.format(schema=schema), run_id)
                revision = str(row["revision"]) if row is not None else "0"
                view: RunView | None = None
                if revision != last_revision:
                    view = await fetch_run_view(conn, schema, run_id)
        except asyncpg.exceptions.UndefinedTableError:
            yield _frame(seq, {"status": "uninstalled", "detail": NOT_INSTALLED})
            return
        except asyncpg.exceptions.PostgresConnectionError:
            # The poll's degrade: the keepalive continues (the stream
            # survives a PG blip; the next good poll catches up — the
            # snapshot is the whole state, nothing was lost).
            logger.warning("wf-stream-poll-degraded", run_id=str(run_id))
            yield ": keepalive\n\n"
            await asyncio.sleep(_STREAM_POLL_S)
            continue
        if view is not None:
            seq += 1
            last_revision = revision
            yield _frame(seq, run_state_json(view, seq=seq))
            since_keepalive = 0.0
        elif revision != last_revision:
            # THE NOT-STARTED RUN (the states matrix's defined state): the
            # revision moved but the run has no rows yet (or none at all)
            # — emit the EMPTY snapshot once per revision change, never a
            # silent hang (a blank stream that reads as a dead run is
            # the #673 class).
            seq += 1
            last_revision = revision
            yield _frame(
                seq,
                {
                    "seq": seq,
                    "run_id": str(run_id),
                    "status": "unknown",
                    "root_status": "unknown",
                    "nodes": [],
                    "holds": [],
                },
            )
        else:
            since_keepalive += _STREAM_POLL_S
            if since_keepalive >= _STREAM_KEEPALIVE_S:
                yield ": keepalive\n\n"
                since_keepalive = 0.0
        await asyncio.sleep(_STREAM_POLL_S)


def _frame(seq: int, payload: dict[str, Any]) -> str:
    return f"id: {seq}\nevent: run_state\ndata: {_json_dumps(payload).decode('utf-8')}\n\n"


def register_actions(router: APIRouter) -> None:
    """Attach the machine routes to *router*."""

    @router.get("/api/runs/{run_id}/nodes/{node_key}")
    async def node_panel(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        node_key: str,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
    ) -> dict[str, Any]:
        """The node detail panel's data (the click → panel surface)."""
        async with pool.acquire() as conn:
            node = await conn.fetchrow(_NODE_PANEL_SQL.format(schema=schema), node_key, run_id)
            if node is None:
                raise HTTPException(status_code=404, detail="Node not found")
            parent = await conn.fetchrow(_NODE_PARENT_SQL.format(schema=schema), node["id"])
            ledger = await conn.fetch(_NODE_LEDGER_SQL.format(schema=schema), node["id"])
        return {
            "key": node["step_key"],
            "status": node["status"],
            "attempt": node["attempt"],
            "max_attempts": node["max_attempts"],
            "retry_kind": node["retry_kind"],
            "trace_id": str(node["trace_id"]) if node["trace_id"] else None,
            "error_class": node["error_class"],
            "error_message": node["error_message"],
            "error_traceback": node["error_traceback"],
            "captured_error": node["captured_error"],
            "parent": dict(parent) | {"id": str(parent["id"])} if parent else None,
            "timeline": [dict(r) | {"created_at": _iso(r["created_at"])} for r in ledger],
            "created_at": _iso(node["created_at"]),
            "started_at": _iso(node["started_at"]),
            "finished_at": _iso(node["finished_at"]),
        }

    @router.get("/api/runs/{run_id}/stream")
    async def run_stream(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        settings: TaskQSettings = Depends(get_settings),
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        """The run's SSE state feed (the revisioned-snapshot replay —
        the module docstring states the contract)."""
        cursor = 0
        if last_event_id is not None:
            try:
                cursor = max(0, int(last_event_id))
            except ValueError:
                cursor = 0
        # THE CAP: each stream pins a poll task + a socket for as long as
        # the client holds it — the uncapped scan's guard (the SSE-cap
        # law) refuses an endpoint without this, and a burst of open
        # streams would exhaust exactly what the cap bounds.
        sse_slot = await acquire_sse_slot(
            "wf-run-stream", settings.admin_max_sse_connections, surface="admin"
        )
        return StreamingResponse(
            release_after(
                sse_slot,
                _stream_generator(pool, schema, run_id, cursor),
                "wf-run-stream",
                surface="admin",
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.post("/api/runs/{run_id}/resolve")
    async def run_resolve(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        wf_app: Any = Depends(get_workflow_app),
    ) -> dict[str, Any]:
        """Reply to a hold BY ID (the typed door; the audit row)."""
        if wf_app is None:
            raise HTTPException(
                status_code=501,
                detail="no workflow definitions mounted (mount the router with "
                "workflow_app=...) — the resolve refuses to deliver untyped",
            )
        body = await _form_body(request)
        hold_id = body.get("hold_id")
        decision = body.get("decision")
        reason = body.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise HTTPException(status_code=400, detail="reason must be a string")
        if not isinstance(hold_id, str) or not isinstance(decision, dict):
            raise HTTPException(
                status_code=400,
                detail="the resolve needs {'hold_id': str, 'decision': object, 'reason'?: str}",
            )
        decision_doc = cast(dict[str, object], decision)
        from taskq.workflows.api._hitl import HitlClient

        client = HitlClient(pool.pool, schema=schema)
        hold = await client.get(hold_id)
        if hold is None or hold.run_id != str(run_id):
            raise HTTPException(status_code=404, detail="Hold not found on this run")
        validated = _validate_through_gates(wf_app, hold, decision_doc)
        result = await client.resolve(
            hold_id,
            validated,
            reason=reason,
            principal=principal,
        )
        return {"status": result.status, "reason": result.reason}

    @router.post("/api/runs/{run_id}/deliver")
    async def run_deliver(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        wf_app: Any = Depends(get_workflow_app),
    ) -> dict[str, Any]:
        """Deliver to a run's held node BY (run, node) — the wire form's
        address; the SAME typed door the resolve rides."""
        if wf_app is None:
            raise HTTPException(
                status_code=501,
                detail="no workflow definitions mounted (mount the router with "
                "workflow_app=...) — the deliver refuses to deliver untyped",
            )
        body = await _form_body(request)
        node_key = body.get("node")
        payload = body.get("payload")
        reason = body.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise HTTPException(status_code=400, detail="reason must be a string")
        if not isinstance(node_key, str) or not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="the deliver needs {'node': str, 'payload': object, 'reason'?: str}",
            )
        payload_doc = cast(dict[str, object], payload)
        from taskq.workflows.api._hitl import HitlClient

        client = HitlClient(pool.pool, schema=schema)
        held = [h for h in await client.list(str(run_id)) if h.node_key == node_key]
        if not held:
            raise HTTPException(
                status_code=404, detail=f"node {node_key!r} holds nothing on this run"
            )
        if len(held) > 1:
            raise HTTPException(
                status_code=409,
                detail="ambiguous: the node holds "
                f"{len(held)} signals — address one BY ID (the resolve's door)",
            )
        validated = _validate_through_gates(wf_app, held[0], payload_doc)
        result = await client.resolve(
            held[0].hold_id,
            validated,
            reason=reason,
            principal=principal,
        )
        return {"status": result.status, "reason": result.reason}

    @router.post("/api/runs/{run_id}/cancel")
    async def run_cancel(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        base_path: str = Depends(get_base_path),
    ) -> Any:
        """Stop the run (the engine's cancel cascade; the audit row)."""
        from taskq.workflows import cancel_workflow_run

        form = await request.form()
        raw_reason = form.get("reason")
        reason = raw_reason if isinstance(raw_reason, str) and raw_reason else None
        stopped = await cancel_workflow_run(
            pool.pool,
            schema=schema,
            flow_id=JobId(run_id),
            reason=reason,
            principal=principal,
        )
        if stopped == 0:
            # Idempotent: an already-terminal run cancels nothing — the
            # operator hears the refusal where they land (the jobs page's
            # refused-op contract: the redirect carries the banner key).
            return RedirectResponse(
                url=f"{base_path}/workflows/{run_id}?error=cancel-not-applied",
                status_code=303,
            )
        return RedirectResponse(url=f"{base_path}/workflows/{run_id}", status_code=303)
