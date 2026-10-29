"""THE EMBEDDING DEMO'S WEB HALF (T26's cure lane — C10): the
maintainer's cowork-agent scenario, the part the admin UI is NOT — the
hosting application's OWN approval board, one user at a time.

THE PRODUCT SHAPE THIS DEMONSTRATES: ONE backend listener
(``taskq.workflows.HitlListener`` — a single dedicated LISTEN
connection per process, the fan-out's hub) serving EVERY watching user;
each user's SSE stream is the listener's fan-out SCOPED BY THE APP'S
OWN AUTHZ (the run filter — ``holds(run=…)``'s discipline; ``frames()``
is the keepalive-carrying form of the same per-subscriber queue). The
admin's ``/sse/holds`` topic is the OPS surface (every hold in the
schema, operator-session gated); THIS is the embedding face — the user
sees only their run's cards:

1. **THE APP-LIFESPAN LISTENER**: the FastAPI lifespan starts ONE
   ``HitlListener`` (one pool slot for the stream's life — the
   landmine doc's capacity tax) and stops it on shutdown (every
   subscriber gets its own end sentinel).
2. **THE PER-USER SSE ENDPOINT** (``GET /runs/{run_id}/events``): the
   demo's token→run map stands in for the real identity system — the
   authz is the RUN SCOPE: a token minted for run A can never subscribe
   to run B's stream (403), because the stream only forwards the
   run-filtered fan-out (and the schema-wide ``Backfilled`` reconcile).
3. **THE HoldCreated HANDLER READS THE ROW**: the event is a POINTER —
   the reason/deadline for display come from
   ``HitlClient.get(hold_id)`` (the row is the truth; the redact law
   holds at the row read exactly as it holds at ``list()``).
4. **THE RESOLVE POST** (``POST /runs/{run_id}/holds/{hold_id}/resolve``):
   the same typed door the admin's Resolve form rides
   (``HitlClient.resolve``) — the boundary validates the payload
   against the hold's declared models BY SHAPE.
5. **THE BROADCAST CLEARS THE CARD**: the resolve's ``HoldResolved``
   fan-out frame reaches the user's stream; the board drops the card.
   And the ``Backfilled(open_hold_ids=…)`` reconcile (the union's
   fourth member) is taught verbatim: on subscribe — and after every
   reconnect — the board drops any card the snapshot disowns (a hold
   resolved during an outage never announces its own death).

Runnable: ``TASKQ_HITL_DEMO=1 uv run uvicorn examples.deep_research_web:app
--port 8088`` (the DSN from ``TASKQ_PG_DSN``, the schema from
``TASKQ_WF_SCHEMA``). The verification lives at
``tests/test_wf_hitl_web_demo.py`` (it builds the app over ITS fixture
pool through :func:`build_app` — the module-level ``app`` below stays
import-inert without the demo switch).
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

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
    """THE DEMO'S IDENTITY SYSTEM (deliberately small): a token→run map
    standing in for the real thing. The SHAPE is the product's: the
    request presents a credential; the app resolves the run it may
    watch/resolve; the SSE stream is SCOPED by construction — the
    forwarder only lets a run's own events (plus the reconcile
    snapshot) through, so the data path cannot leak another run's holds
    even if the endpoint wanted it to. Replace the dict with your
    session store; keep the scoping."""

    def __init__(self) -> None:
        self._tokens: dict[str, str] = {}

    def issue(self, run_id: str) -> str:
        """Mint a token scoped to ONE run (the demo's 'login')."""
        token = os.urandom(16).hex()
        self._tokens[token] = run_id
        return token

    def run_for(self, token: str, run_id: str) -> str:
        """The authz gate: the token's run must BE the addressed run —
        otherwise 403 (never a silent cross-run stream)."""
        scoped = self._tokens.get(token)
        if scoped is None or scoped != run_id:
            raise HTTPException(status_code=403, detail="the token is not scoped to this run")
        return scoped


def build_app(pool: asyncpg.Pool | str, schema: str) -> FastAPI:
    """THE EMBEDDING APP FACTORY: the host's pool (or a DSN — the
    runnable form; the pool then opens in the lifespan and closes at
    shutdown), ONE listener, the per-user faces."""
    owned = not isinstance(pool, asyncpg.Pool)
    authz = DemoAuthz()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        live_pool = await asyncpg.create_pool(pool) if owned else pool
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
        """The demo's login (a real app maps its session to the run)."""
        return {"token": authz.issue(run_id), "run_id": run_id}

    @app.get("/runs/{run_id}/events")
    async def run_events(run_id: str, request: Request) -> StreamingResponse:
        """THE PER-USER SSE STREAM: the fan-out scoped by authz. The
        ``hold`` frame carries the typed union's JSON; ``hold_context``
        carries the ROW-read display facts; ``backfilled`` carries the
        reconcile snapshot the board drops its dead cards on."""
        _gate(request, run_id)
        return StreamingResponse(
            stream_run_cards(_listener(), _client(), run_id),
            media_type="text/event-stream; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/runs/{run_id}/holds/{hold_id}/resolve")
    async def resolve_hold(run_id: str, hold_id: str, request: Request) -> dict[str, str]:
        """THE RESOLVE POST: the same typed door the admin's Resolve
        form rides. THE BROADCAST CLEARS THE CARD: the winning CAS's
        ``HoldResolved`` fan-out reaches every subscribed stream; the
        board drops the card on the frame (no polling, no refresh)."""
        _gate(request, run_id)
        decision = await request.json()
        result = await _client().resolve(hold_id, decision)
        return {"status": result.status, "reason": result.reason or ""}

    @app.get("/runs/{run_id}/board")
    async def board(run_id: str, request: Request) -> HTMLResponse:
        """The board page (the demo's one page — the cards + the SSE)."""
        token = request.query_params.get("token", "")
        _gate(request, run_id)
        return HTMLResponse(_BOARD_HTML.format(run_id=run_id, token=token))

    return app


async def stream_run_cards(
    listener: HitlListener, client: HitlClient, run_id: str
) -> AsyncGenerator[str, None]:
    """ONE USER'S STREAM (the endpoint's body, exposed for the
    verification): the run-filtered fan-out + the keepalive tick + THE
    ROW READ — a ``HoldCreated`` pointer triggers
    ``HitlClient.get(hold_id)`` and the frames carry the reason and the
    deadline the board renders (the event is a pointer; the ROW is the
    truth).

    THE KEEPALIVE MECHANICS (the part worth copying): the generator is
    consumed by a pump task into a plain queue, and the TICK sits on
    the queue read — never ``wait_for`` around the LISTENER generator's
    own ``__anext__`` (a timeout's cancellation propagates through the
    async generator's frame and CLOSES it — the stream dies at its
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
        # THE ROW READ (leg 3): the pointer event fetches the context
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
        # THE RECONCILE (the fourth member): forwarded verbatim — the
        # board drops any card the snapshot disowns.
        return f"event: backfilled\ndata: {event.model_dump_json()}\n\n"
    # HoldResolved (the card CLEARS) / HoldExpired (ditto — the
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
// THE BOARD: the card set — open on HoldCreated (rendering the ROW
// context), cleared on HoldResolved/HoldExpired, RECONCILED on
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
