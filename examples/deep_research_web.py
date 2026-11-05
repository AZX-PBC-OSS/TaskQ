"""The embedding demo's web half: the
maintainer's cowork-agent scenario, the part the admin UI is not — the
hosting application's own approval board, one user at a time.

The product shape this demonstrates: one backend listener
(``taskq.workflows.HitlListener`` — a single dedicated LISTEN
connection per process, the fan-out's hub) serving every watching user;
each user's SSE stream is the listener's fan-out scoped by the app's
own authz (the run filter — ``holds(run=…)``'s discipline; ``frames()``
is the keepalive-carrying form of the same per-subscriber queue). The
admin's ``/sse/holds`` topic is the ops surface (every hold in the
schema, operator-session gated); this is the embedding face — the user
sees only their run's cards:

1. **the app-lifespan listener**: the FastAPI lifespan starts one
   ``HitlListener`` (one pool slot for the stream's life — the
   landmine doc's capacity tax) and stops it on shutdown (every
   subscriber gets its own end sentinel).
2. **the per-user SSE endpoint** (``GET /runs/{run_id}/events``): the
   demo's token→run map stands in for the real identity system — the
   authz is the run scope: a token minted for run A can never subscribe
   to run B's stream (403), because the stream only forwards the
   run-filtered fan-out (and the schema-wide ``Backfilled`` reconcile).
3. **the HoldCreated handler reads the row**: the event is a pointer —
   the reason/deadline for display come from
   ``HitlClient.get(hold_id)`` (the row is the truth; the redact law
   holds at the row read exactly as it holds at ``list()``).
4. **the resolve post** (``POST /runs/{run_id}/holds/{hold_id}/resolve``):
   the same typed door the admin's Resolve form rides
   (``HitlClient.resolve``) — the boundary validates the payload
   against the hold's declared models by shape.
5. **the broadcast clears the card**: the resolve's ``HoldResolved``
   fan-out frame reaches the user's stream; the board drops the card.
   And the ``Backfilled(open_hold_ids=…)`` reconcile (the union's
   fourth member) is taught verbatim: on subscribe — and after every
   reconnect — the board drops any card the snapshot disowns (a hold
   resolved during an outage never announces its own death).

Runnable: ``TASKQ_HITL_DEMO=1 uv run uvicorn examples.deep_research_web:app
--port 8088`` (the DSN from ``TASKQ_PG_DSN``, the schema from
``TASKQ_WF_SCHEMA``). The verification lives at
``tests/test_wf_hitl_web_demo.py`` (it builds the app over its fixture
pool through :func:`build_app` — the module-level ``app`` below stays
import-inert without the demo switch).
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.workflows.api._hitl import HitlClient
from taskq.workflows.api._hitl_listen import (
    Backfilled,
    HitlListener,
    HoldCreated,
    HoldEvent,
)

#: The per-user stream's keepalive tick (a quiet stream must still say
#: something, or the proxies eat it — the admin feed's own discipline).
_KEEPALIVE_S: float = 15.0


class DemoAuthz:
    """The demo's identity system (deliberately small): a token→run map
    standing in for the real thing. The shape is the product's: the
    request presents a credential; the app resolves the run it may
    watch/resolve; the SSE stream is scoped by construction — the
    forwarder only lets a run's own events (plus the reconcile
    snapshot) through, so the data path cannot leak another run's holds
    even if the endpoint wanted it to. Replace the dict with your
    session store; keep the scoping."""

    def __init__(self) -> None:
        self._tokens: dict[str, str] = {}

    def issue(self, run_id: str) -> str:
        """Mint a token scoped to one run (the demo's 'login')."""
        token = os.urandom(16).hex()
        self._tokens[token] = run_id
        return token

    def run_for(self, token: str, run_id: str) -> str:
        """The authz gate: the token's run must be the addressed run —
        otherwise 403 (never a silent cross-run stream)."""
        scoped = self._tokens.get(token)
        if scoped is None or scoped != run_id:
            raise HTTPException(status_code=403, detail="the token is not scoped to this run")
        return scoped


def build_app(pool: asyncpg.Pool | str, schema: str) -> FastAPI:
    """The embedding app factory: the host's pool (or a DSN — the
    runnable form; the pool then opens in the lifespan and closes at
    shutdown), one listener, the per-user faces."""
    owned = not isinstance(pool, asyncpg.Pool)
    authz = DemoAuthz()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        live_pool = await asyncpg.create_pool(pool) if owned else pool
        _app.state.pool = live_pool
        listener = HitlListener(live_pool, schema)
        await listener.start()
        _app.state.listener = listener
        try:
            yield
        finally:
            await listener.stop()  # every subscriber gets its own sentinel
            if owned and isinstance(live_pool, asyncpg.Pool):
                await live_pool.close()

    app = FastAPI(title="taskq HITL embedding demo", lifespan=lifespan)

    def _listener() -> HitlListener:
        listener = getattr(app.state, "listener", None)
        if listener is None:
            raise HTTPException(status_code=503, detail="the listener is not running")
        return listener

    def _client() -> HitlClient:
        return HitlClient(pool if isinstance(pool, asyncpg.Pool) else app.state.pool, schema=schema)

    def _gate(request: Request, run_id: str) -> None:
        token = request.query_params.get("token", "")
        if not token:
            raise HTTPException(status_code=403, detail="the token is missing")
        authz.run_for(token, run_id)

    @app.get("/runs/{run_id}/token")
    async def issue_token(run_id: str) -> dict[str, str]:
        """The demo's login (a real app maps its session to the run).
        The id is canonicalized at the door: both issued scopes and the
        event stream compare one spelling, and a malformed id is a 404,
        never a stream that silently yields nothing."""
        try:
            canonical = str(UUID(run_id))
        except ValueError:
            raise HTTPException(status_code=404, detail="no such run") from None
        return {"token": authz.issue(canonical), "run_id": canonical}

    @app.get("/runs/{run_id}/events")
    async def run_events(run_id: str, request: Request) -> StreamingResponse:
        """The per-user SSE stream: the fan-out scoped by authz. The
        ``hold`` frame carries the typed union's JSON; ``hold_context``
        carries the row-read display facts; ``backfilled`` carries the
        reconcile snapshot the board drops its dead cards on. The id is
        canonicalized at the door (a malformed id is a 404 before any
        stream starts — the listener's own refusal class, surfaced)."""
        try:
            canonical = str(UUID(run_id))
        except ValueError:
            raise HTTPException(status_code=404, detail="no such run") from None
        _gate(request, canonical)
        return StreamingResponse(
            stream_run_cards(_listener(), _client(), canonical),
            media_type="text/event-stream; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/runs/{run_id}/holds/{hold_id}/resolve")
    async def resolve_hold(run_id: str, hold_id: str, request: Request) -> dict[str, str]:
        """The resolve post: the same typed door the admin's Resolve
        form rides. The broadcast clears the card: the winning CAS's
        ``HoldResolved`` fan-out reaches every subscribed stream; the
        board drops the card on the frame (no polling, no refresh). The
        ids canonicalize at the door (a malformed id is a 404 — never a
        500, never a silent stream)."""
        try:
            canonical_run = str(UUID(run_id))
            canonical_hold = str(UUID(hold_id))
        except ValueError:
            raise HTTPException(status_code=404, detail="no such run or hold") from None
        _gate(request, canonical_run)
        # The run scope is the hold's own run: a token scoped to run A
        # does not resolve run B's hold — the door checks the hold's run
        # (the admin door's rule), not just the token's.
        hold_row = await _client().get(canonical_hold)
        if hold_row is None or hold_row.run_id != canonical_run:
            raise HTTPException(status_code=404, detail="no such hold on this run")
        body = await request.body()
        try:
            decision = _json_loads(body)
        except ValueError:
            # The demo parses with the admin doors' own bounded parser: a
            # malformed body — the shallow junk and the nesting bomb the
            # stdlib decoder answers with RecursionError — is the named
            # 400, never a 500 out of the door.
            raise HTTPException(status_code=400, detail="the body is not JSON") from None
        if not isinstance(decision, dict):
            # The typed door's own requirement: the decision is the
            # declared model's shape — a non-object body cannot fit it
            # (the cold-catalog fallback cannot judge a non-object), so
            # the door refuses it instead of delivering a payload the
            # resumed body never declared.
            raise HTTPException(status_code=422, detail="the decision must be a JSON object")
        try:
            dumps_jsonb_str(decision)
        except (ValueError, TypeError) as exc:
            # The payload's wire shape: a control character (a NUL) is
            # the serializer's ValueError; an unencodable value (a lone
            # surrogate) its TypeError — both refuse loud, the hold
            # survives.
            raise HTTPException(status_code=422, detail=str(exc)) from None
        result = await _client().resolve(canonical_hold, decision)
        return {"status": result.status, "reason": result.reason or ""}

    @app.get("/runs/{run_id}/board")
    async def board(run_id: str, request: Request) -> HTMLResponse:
        """The board page (the demo's one page — the cards + the SSE)."""
        try:
            canonical_run = str(UUID(run_id))
        except ValueError:
            raise HTTPException(status_code=404, detail="no such run") from None
        token = request.query_params.get("token", "")
        _gate(request, canonical_run)
        return HTMLResponse(_BOARD_HTML.format(run_id=canonical_run, token=token))

    return app


async def stream_run_cards(
    listener: HitlListener, client: HitlClient, run_id: str
) -> AsyncGenerator[str, None]:
    """One user's stream (the endpoint's body, exposed for the
    verification): the run-filtered fan-out + the keepalive tick + the
    row read — a ``HoldCreated`` pointer triggers
    ``HitlClient.get(hold_id)`` and the frames carry the reason and the
    deadline the board renders (the event is a pointer; the row is the
    truth).

    The keepalive mechanics (the part worth copying): the generator is
    consumed by a pump task into a plain queue, and the tick sits on
    the queue read — never ``wait_for`` around the listener generator's
    own ``__anext__`` (a timeout's cancellation propagates through the
    async generator's frame and closes it — the stream dies at its
    first quiet interval; a bare ``asyncio.Queue.get()`` is safe to
    cancel, the admin's ``_iterate`` runs its tick on exactly that
    seam)."""
    import asyncio
    from contextlib import suppress

    queue: asyncio.Queue[str | None] = asyncio.Queue()

    async def pump() -> None:
        try:
            async for event in listener.holds(run=run_id):
                queue.put_nowait(await _render_frame(event, client))
        finally:
            queue.put_nowait(None)  # the pump's own end sentinel

    pump_task = asyncio.create_task(pump())
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=_KEEPALIVE_S)
            except TimeoutError:
                yield ": keepalive\n\n"
                continue
            if item is None:
                return
            yield item
    finally:
        pump_task.cancel()
        with suppress(BaseException):
            await pump_task


async def _render_frame(event: HoldEvent, client: HitlClient) -> str:
    if isinstance(event, HoldCreated):
        # The row read (leg 3): the pointer event fetches the context
        # the board displays (redacted exactly as the list/get surface
        # redacts).
        context = await client.get(event.hold_id)
        row: dict[str, object] = (
            {
                "reason": context.reason,
                "expires_at": (
                    context.expires_at.isoformat()
                    if hasattr(context.expires_at, "isoformat")
                    else context.expires_at
                ),
            }
            if context is not None
            else {}
        )
        ctx_doc = json.dumps(
            {
                "hold_id": event.hold_id,
                "reason": row.get("reason"),
                "expires_at": row.get("expires_at"),
            }
        )
        return (
            f"event: hold\ndata: {event.model_dump_json()}\n\n"
            f"event: hold_context\ndata: {ctx_doc}\n\n"
        )
    if isinstance(event, Backfilled):
        # The reconcile (the fourth member): forwarded verbatim — the
        # board drops any card the snapshot disowns.
        return f"event: backfilled\ndata: {event.model_dump_json()}\n\n"
    # HoldResolved (the card clears) / HoldExpired (ditto — the
    # deadline passed): the pointer, verbatim.
    return f"event: hold\ndata: {event.model_dump_json()}\n\n"


_BOARD_HTML = """<!doctype html>
<html><head><title>approvals — run {run_id}</title>
<style>
body {{ font: 14px/1.5 system-ui, sans-serif; margin: 2rem; }}
.card {{ border: 1px solid #d0d0d0; border-radius: 8px; padding: 1rem;
         margin-bottom: 1rem; max-width: 40rem; }}
.card button {{ margin-right: .5rem; }}
</style></head><body>
<h1>Approvals — run {run_id}</h1>
<div id="board"></div>
<script>
// The board: the card set — open on HoldCreated (rendering the row
// context), cleared on HoldResolved/HoldExpired, reconciled on
// Backfilled (drop anything the snapshot disowns — a hold resolved
// during an outage never announces its own death).
const board = document.getElementById("board");
const cards = new Map();
function render() {{ board.innerHTML = ""; for (const [id, c] of cards) {{
  const div = document.createElement("div"); div.className = "card";
  div.innerHTML = "<b>" + c.signal + "</b> — " + (c.reason || "approval owed")
    + "<br><button data-v='yes'>approve</button><button data-v='no'>reject</button>";
  div.querySelectorAll("button").forEach(b => b.onclick = () =>
    fetch("/runs/{run_id}/holds/" + id + "/resolve?token={token}",
          {{method: "POST", headers: {{"content-type": "application/json"}},
           body: JSON.stringify({{verdict: b.dataset.v === "yes" ? "approve" : "reject"}})}}));
  board.appendChild(div); }} }}
function drop(id) {{ cards.delete(id); render(); }}
const es = new EventSource("/runs/{run_id}/events?token={token}");
es.addEventListener("hold", ev => {{ const e = JSON.parse(ev.data);
  if (e.event === "hold_created") {{ cards.set(e.hold_id, e); render(); }}
  else drop(e.hold_id); }});
es.addEventListener("hold_context", ev => {{ const c = JSON.parse(ev.data);
  const card = cards.get(c.hold_id);
  if (card) {{ card.reason = c.reason; card.expires_at = c.expires_at; render(); }} }});
es.addEventListener("backfilled", ev => {{ const keep = new Set(JSON.parse(ev.data).open_hold_ids);
  for (const id of [...cards.keys()]) if (!keep.has(id)) cards.delete(id); render(); }});
</script></body></html>"""


if os.environ.get("TASKQ_HITL_DEMO"):  # the runnable switch (tests never set it)
    app: FastAPI = build_app(
        os.environ.get("TASKQ_PG_DSN", "postgresql://localhost/taskq"),
        os.environ.get("TASKQ_WF_SCHEMA", "public"),
    )
else:
    app = FastAPI(title="taskq HITL embedding demo (import-inert)")
