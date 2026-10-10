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
copy of a ledger that already exists. THE SESSION LAW (Q4's cure — the
same re-check ``/sse/{topic}`` got for #316): the stream re-invokes the
host's ``session_verifier`` before the first frame and once per poll
iteration — a session revoked MID-STREAM ends the stream at the next
tick, fail-closed on any verifier error or a check that outlives
``SESSION_RECHECK_TIMEOUT_SECS``. THE SUBSCRIBE LAW (the existence
oracle's cure, Q7): an unknown run id is a 404 AT SUBSCRIBE — the
estate's own SSE convention (``/sse/{topic}`` refuses an unknown topic
at subscribe; the empty-snapshot-forever stream was an existence oracle
behind the auth gate and a wasted poll task per probe). A run that
EXISTS but whose node rows are not inserted yet still streams (the
generator's not-started state).

THE TYPED DOOR: deliver + resolve validate through the bound
``TypedGate``s of the WorkflowApp the host mounted (``create_router(
workflow_app=...)``). Without one the endpoints answer ``501`` with the
named reason — NO untyped deliver surface ships. THE AUDITED REFUSAL
(Q3's cure): the gate's 422 IS an operator action — the refusal writes
its own ``admin_audit`` row (``refused`` + the reason) before the
HTTPException propagates, so an attacker's shape-probing lands IN the
trail the page claims covers every Resolve/deliver; the hold survives.

THE AUDIT (G4): every resolve/deliver/cancel writes its ``admin_audit``
row (principal + reason + run/node) — "who approved this" is a ROW, not
a log line. The resolve/deliver rides HitlClient's own same-tx row; the
cancel rides the engine cascade's row; the audit entries carry the
operator principal the auth dependency captured.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime
from typing import Annotated, Any, Final, cast

import asyncpg
import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from tors import truncate_to_bounds

from taskq._json import dumps as _json_dumps
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web._sse_limit import SESSION_RECHECK_TIMEOUT_SECS, acquire_sse_slot, release_after
from taskq.web.admin._factory import (
    get_admin_pool,
    get_base_path,
    get_principal,
    get_schema,
    get_session_verifier,
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

#: The subscribe probe (Q7's cure): one indexed root-row read decides
#: the stream's 404-before-first-frame (the existence oracle's refusal).
_RUN_EXISTS_SQL = "SELECT 1 FROM \"{schema}\".jobs WHERE id = $1 AND step_key = '__flow__'"

#: The node detail panel's read (the drill-down: status header, attempts,
#: the trace id, captured error, one upstream hop — §10.3's causal chain
#: via parent_id). MAP CHILDREN share one step key (``<src>.item``), so
#: the panel addresses a child by (step, map_index): the bare read
#: lands on the FIRST child in map order (deterministic), and the
#: ``map_index=`` query param selects the row itself — the run page's
#: observation surface (``watch this item's attempt go 1 → 2``) is
#: (step, map_index)-addressable, not step-key-arbitrary.
_NODE_PANEL_SQL = (
    "SELECT id, step_key, status, attempt, max_attempts, retry_kind, map_index, "
    "trace_id, error_class, error_message, error_traceback, "
    "metadata->>'error' AS captured_error, parent_id, "
    "created_at, started_at, finished_at "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND step_key <> '__flow__' "
    "ORDER BY map_index LIMIT 1"
)

_NODE_PANEL_CHILD_SQL = (
    "SELECT id, step_key, status, attempt, max_attempts, retry_kind, map_index, "
    "trace_id, error_class, error_message, error_traceback, "
    "metadata->>'error' AS captured_error, parent_id, "
    "created_at, started_at, finished_at "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND map_index = $3 "
    "AND step_key <> '__flow__'"
)

#: The map's children census (the panel's addressing surface): one row
#: per child, in map order — the operator clicks THROUGH to a child.
_NODE_CHILDREN_SQL = (
    "SELECT map_index, status, attempt, max_attempts, error_class "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND step_key <> '__flow__' "
    "ORDER BY map_index"
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


#: The panel's display cap (CHARS): error_message / error_traceback /
#: metadata->>'error' render bounded — the full detail stays in the ROWS
#: (the drill-down fetch reads the same rows; the cap is the DISPLAY's,
#: Q5's read-side cure: engine-written rows carry the write-side caps,
#: but a row written by ANY other path — or a hostile DB write — served
#: unbounded through this endpoint, a 15MB response from a foreign row).
_PANEL_FIELD_CAP_CHARS: Final[int] = 10_000
_PANEL_SUFFIX_RESERVE: Final[int] = 80


def _bound_for_panel(text: str | None) -> str | None:
    """The read-side bound (the display-capped delivery): an over-cap
    field truncates WITH the dropped-character count named — the same
    honesty the capture's ``__truncated__`` marker and the CLI's event
    lines keep. ``truncate_to_bounds`` is grapheme-safe (tors, the house
    call); the row itself is never touched."""
    if text is None or len(text) <= _PANEL_FIELD_CAP_CHARS:
        return text
    bound = _PANEL_FIELD_CAP_CHARS - _PANEL_SUFFIX_RESERVE
    remaining = len(text) - bound
    suffix = f"… [truncated: +{remaining} characters stay in the row]"
    return truncate_to_bounds(text, bound) + suffix


def _panel_truncated(texts: list[str | None]) -> bool:
    """Whether any of the panel's bounded fields actually truncated (the
    'truncated' marker's boolean face — the JS can render the honesty
    without parsing the suffix)."""
    return any(t is not None and len(t) > _PANEL_FIELD_CAP_CHARS for t in texts)


async def _audit_gate_refusal(
    pool: BoundedPool,
    *,
    schema: str,
    principal: Any,
    action: str,
    target_id: str,
    run_id: str,
    node_key: str,
    reason: Any,
    refusal: str,
) -> None:
    """THE AUDITED REFUSAL (Q3's cure): the typed door's 422 is an
    operator ACTION — it writes its ``admin_audit`` row (``refused`` +
    the gate's reason) before the HTTPException propagates. The refusal
    moves no rows, so the row stands alone (nothing to share a tx with —
    the same-tx discipline's degenerate case); it is written with the
    UNsafe recorder so a failure SURFACES (a refusal the trail missed is
    the exact invisible-probe conviction). The detail carries the run_id
    the trail's page query keys on — the refusal renders on THE RUN's
    trail, not just the hold's target lookup."""
    from taskq.web.admin._audit import record_admin_action

    async with pool.acquire() as conn:
        await record_admin_action(
            conn,
            schema=schema,
            principal=principal,
            action=action,
            target_type="hold",
            target_id=target_id,
            reason=reason if isinstance(reason, str) else None,
            detail={
                "refused": str(refusal),
                "node": node_key,
                "stage": "admin-payload-gate",
                "run_id": run_id,
            },
        )


async def _validated_through_gates_audited(
    pool: BoundedPool,
    *,
    schema: str,
    principal: Any,
    wf_app: Any,
    hold: Any,
    decision: dict[str, object],
    action: str,
    target_id: str,
    reason: Any,
) -> dict[str, object]:
    """The typed door + the audit-on-refusal (Q3): validate through the
    bound gates; a 422 refusal lands its audit row FIRST, then
    propagates. The success path is exactly ``_validate_through_gates``."""
    try:
        return _validate_through_gates(wf_app, hold, decision)
    except HTTPException as exc:
        await _audit_gate_refusal(
            pool,
            schema=schema,
            principal=principal,
            action=action,
            target_id=target_id,
            run_id=hold.run_id,
            node_key=hold.node_key,
            reason=reason,
            refusal=str(exc.detail),
        )
        raise


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
    pool: BoundedPool,
    schema: str,
    run_id: uuid.UUID,
    cursor: int,
    session_verifier: Callable[[], Awaitable[bool]] | None = None,
) -> AsyncGenerator[str, None]:
    """The feed: snapshot-on-revision, full replacement each frame (the
    replay contract's body — see the module docstring).

    THE SESSION LAW (Q4's cure, the #316 pattern ported from
    ``sse.py``): ``session_verifier`` re-runs before the first frame and
    once per poll iteration — a session revoked mid-stream ends the
    stream at the next tick, before ANY further frame. Fail-closed: a
    verifier error is revocation, and a check that outlives
    ``SESSION_RECHECK_TIMEOUT_SECS`` (a wedged IdP introspection) is
    unknown state, not a pass.

    THE GENERATOR TRAP (P2's pin 6): state crosses as PARAMETERS — a
    reassignment of a variable captured from the endpoint's scope is the
    UnboundLocalError dragon; nothing here shadows it."""
    seq = cursor
    last_revision: str | None = None
    since_keepalive = 0.0
    _recheck_count = {"n": 0}

    async def _session_still_valid() -> bool:
        if session_verifier is None:
            return True
        first_check = _recheck_count["n"] == 0
        try:
            _recheck_count["n"] += 1
            return bool(
                await asyncio.wait_for(
                    session_verifier(),
                    timeout=SESSION_RECHECK_TIMEOUT_SECS,
                )
            )
        except TimeoutError:
            logger.warning(
                "wf-run-sse-session-recheck-timeout",
                run_id=str(run_id),
                timeout_secs=SESSION_RECHECK_TIMEOUT_SECS,
                stream_phase="initial" if first_check else "streaming",
            )
            return False
        except Exception:
            # Fail closed: an unknown session state must not keep an
            # admin stream open (the caller names the revocation).
            return False

    while True:
        # THE SESSION RE-CHECK GATES EVERY ITERATION (before the poll and
        # every frame it would yield — the revoked session's stream
        # terminates at the next tick, never keeps receiving frames).
        if not await _session_still_valid():
            logger.warning(
                "wf-run-sse-session-revoked",
                run_id=str(run_id),
                stream_phase="streaming",
            )
            return
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
            # revision moved but the run has no rows yet — the subscribe
            # 404s an UNKNOWN run id (the existence oracle's cure), so
            # this branch is the mid-stream race only: the root row the
            # subscribe probe found is pruned between subscribe and this
            # poll. Still a defined state, never a silent hang.
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
        map_index: int | None = None,
    ) -> dict[str, Any]:
        """The node detail panel's data (the click → panel surface).

        THE MAP-INDEX ADDRESSING: a map's children share one step key —
        the panel addresses a child by (step, ``map_index``); the bare
        read lands on the FIRST child in map order and carries the
        ``children`` census, so the operator can click through to the
        child whose story they are watching (the demo README's
        observation: ``watch doc-doomed's attempt go 1 → 2``)."""
        if map_index is not None and map_index < 0:
            raise HTTPException(status_code=400, detail="map_index must be >= 0")
        async with pool.acquire() as conn:
            if map_index is None:
                node = await conn.fetchrow(_NODE_PANEL_SQL.format(schema=schema), node_key, run_id)
            else:
                node = await conn.fetchrow(
                    _NODE_PANEL_CHILD_SQL.format(schema=schema), node_key, run_id, map_index
                )
            if node is None:
                raise HTTPException(status_code=404, detail="Node not found")
            parent = await conn.fetchrow(_NODE_PARENT_SQL.format(schema=schema), node["id"])
            ledger = await conn.fetch(_NODE_LEDGER_SQL.format(schema=schema), node["id"])
            siblings = await conn.fetch(_NODE_CHILDREN_SQL.format(schema=schema), node_key, run_id)
        children = [
            {
                "map_index": r["map_index"],
                "status": r["status"],
                "attempt": r["attempt"],
                "max_attempts": r["max_attempts"],
                # THE CLASS FIELD RIDES THE SAME BOUND (the rv2 finding's
                # cure): a hostile row's 200KB error_class served whole
                # through the census while the sibling fields were bound.
                "error_class": _bound_for_panel(r["error_class"]),
            }
            for r in siblings
        ]
        return {
            "key": node["step_key"],
            "map_index": node["map_index"],
            "status": node["status"],
            "attempt": node["attempt"],
            "max_attempts": node["max_attempts"],
            "retry_kind": node["retry_kind"],
            "trace_id": str(node["trace_id"]) if node["trace_id"] else None,
            "error_class": _bound_for_panel(node["error_class"]),
            # THE READ-SIDE BOUND (Q5's cure): the display-capped
            # delivery — a foreign/hostile row's 5MB error fields render
            # bounded with the dropped-count named; the full detail
            # stays in the ROWS (the CLI/SQL surface reads them
            # uncapped).
            "error_message": _bound_for_panel(node["error_message"]),
            "error_traceback": _bound_for_panel(node["error_traceback"]),
            "captured_error": _bound_for_panel(node["captured_error"]),
            "error_truncated": _panel_truncated(
                [
                    node["error_class"],
                    node["error_message"],
                    node["error_traceback"],
                    node["captured_error"],
                ]
            ),
            "parent": dict(parent) | {"id": str(parent["id"])} if parent else None,
            "timeline": [dict(r) | {"created_at": _iso(r["created_at"])} for r in ledger],
            "children": children if len(children) > 1 else [],
            "created_at": _iso(node["created_at"]),
            "started_at": _iso(node["started_at"]),
            "finished_at": _iso(node["finished_at"]),
        }

    @router.get("/api/runs/{run_id}/stream")
    async def run_stream(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        settings: TaskQSettings = Depends(get_settings),
        session_verifier: Callable[[Request], Awaitable[bool]] | None = Depends(
            get_session_verifier
        ),
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        """The run's SSE state feed (the revisioned-snapshot replay —
        the module docstring states the contract).

        THE SUBSCRIBE LAW (Q7's cure): an unknown run id is a 404 HERE —
        the estate's own SSE convention (``/sse/{topic}`` refuses an
        unknown topic at subscribe; this endpoint refuses an unknown RUN
        the same way). The empty-snapshot-forever stream was an
        existence oracle behind the auth gate AND a poll task per probe,
        held until the client let go; the 404 is the defined refusal. A
        run that exists but has no node rows yet still streams (the
        generator's not-started state)."""
        # THE EXISTENCE PROBE (bounded, one indexed read, BEFORE the slot
        # acquire — a refused stream must not consume the SSE budget the
        # cap guards).
        async with pool.acquire() as conn:
            try:
                exists = await conn.fetchval(_RUN_EXISTS_SQL.format(schema=schema), run_id)
            except asyncpg.exceptions.UndefinedTableError:
                exists = True  # the generator's uninstalled degrade answers the stream
        if not exists:
            raise HTTPException(status_code=404, detail="Workflow run not found")
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
        # THE SESSION LAW's binding (the #316 pattern, sse.py): the
        # verifier reads the same session cookie the request arrived
        # with; a session revoked mid-stream fails the re-check at the
        # next tick even though those bytes are unchanged.
        _session_verifier: Callable[[], Awaitable[bool]] | None = (
            (lambda: session_verifier(request)) if session_verifier is not None else None
        )
        return StreamingResponse(
            release_after(
                sse_slot,
                _stream_generator(pool, schema, run_id, cursor, _session_verifier),
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
        # THE AUDITED REFUSAL (Q3's cure): the gate's 422 writes its
        # admin_audit row (refused + the reason) before propagating —
        # the shape-probing operator lands IN the trail.
        validated = await _validated_through_gates_audited(
            pool,
            schema=schema,
            principal=principal,
            wf_app=wf_app,
            hold=hold,
            decision=decision_doc,
            action="hitl.resolve",
            target_id=hold_id,
            reason=reason,
        )
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
        # THE AUDITED REFUSAL (Q3's cure): the deliver door's 422 audits
        # exactly like the resolve's (the page's claim names deliver).
        validated = await _validated_through_gates_audited(
            pool,
            schema=schema,
            principal=principal,
            wf_app=wf_app,
            hold=held[0],
            decision=payload_doc,
            action="hitl.resolve",
            target_id=held[0].hold_id,
            reason=reason,
        )
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
