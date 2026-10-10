"""THE EMBEDDING DEMO'S WEB HALF, VERIFIED (T26's cure lane — C10):
``examples/deep_research_web.py`` — the app-lifespan listener → the
per-user SSE endpoint (authz scoped by the run) → the HoldCreated
handler reading the ROW for the display facts → the resolve POST → the
broadcast clearing the card.

The demo app is built over the TEST's fixture pool through
``build_app`` (the module-level ``app`` stays import-inert without the
``TASKQ_HITL_DEMO`` switch). The stream is consumed through the demo's
OWN ``stream_run_cards`` generator (the endpoint's body — the same
frames the wire carries) so the SSE teardown swamp never touches these
pins; the ASGI faces (the authz gates + the resolve POST) run over
httpx's ASGI transport (which runs no lifespan — the test wires the
app's listener the way the lifespan does, and pins the authz gates
against the app's OWN identity store).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg
import httpx
from examples.deep_research_web import build_app, stream_run_cards
from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.api._hitl import HitlClient
from taskq.workflows.api._hitl_listen import HitlListener


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


async def _held_flow(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    name: str,
) -> tuple[str, FlowRunner]:
    """A flow whose single node HOLDS (the demo's run)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0, reason="the demo's gate")

    app = WorkflowApp()

    @app.workflow(name)
    def web_demo_flow() -> Promise[object]:
        return build(step(hold_body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    return str(flow_id), runner


def _blocks(text: str) -> list[tuple[str, dict[str, Any]]]:
    """One SSE yield → its (event, data-doc) blocks."""
    out: list[tuple[str, dict[str, Any]]] = []
    for block in text.strip().split("\n\n"):
        lines = block.splitlines()
        event = next((ln for ln in lines if ln.startswith("event: ")), None)
        data = next((ln for ln in lines if ln.startswith("data: ")), None)
        if event and data:
            out.append((event[7:], json.loads(data[6:])))
    return out


class _Board:
    """THE BOARD'S SERVER-SIDE TWIN: the card set driven ONLY by the
    demo's frames — create opens (with the ROW context), resolve/expired
    clear, Backfilled reconciles (drops what the snapshot disowns)."""

    def __init__(self) -> None:
        self.cards: dict[str, dict[str, Any]] = {}
        self.contexts: dict[str, dict[str, Any]] = {}
        self.reconciled = False

    def apply(self, event: str, doc: dict[str, Any]) -> None:
        if event == "hold":
            kind = doc.get("event")
            if kind == "hold_created":
                self.cards[doc["hold_id"]] = doc
            else:  # hold_resolved / hold_expired — THE CARD CLEARS
                self.cards.pop(doc["hold_id"], None)
        elif event == "hold_context":
            self.contexts[doc["hold_id"]] = doc
        elif event == "backfilled":
            self.reconciled = True
            for hid in list(self.cards):
                if hid not in doc["open_hold_ids"]:
                    del self.cards[hid]


async def _read_until(
    listener: HitlListener, client: HitlClient, run_id: str, until: Any, bound: float = 5.0
) -> _Board:
    """Consume the demo's stream (the endpoint's own generator — the
    same frames the wire carries) until *until(board)* holds."""
    board = _Board()
    async with asyncio.timeout(bound):
        async for text in stream_run_cards(listener, client, run_id):
            for event, doc in _blocks(text):
                board.apply(event, doc)
            if until(board):
                break
    return board


async def test_the_web_half_lives_and_clears(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE WEB HALF'S SPINE: the authz gates are the RUN SCOPE (a
    token minted for another run — or no token — never subscribes); the
    per-user stream carries the run's HoldCreated WITH the ROW-read
    context (the reason the wait site declared — the pointer triggered
    the read) and the reconcile snapshot; the resolve POST lands
    through the demo's typed door; and the broadcast CLEARS the card —
    the HoldResolved frame on the same stream, no polling."""
    flow_id, _runner = await _held_flow(wf_conn, wf_schema, wf_pool, name="T26_web_demo_flow")
    app = build_app(wf_pool, wf_schema)
    demo_client = HitlClient(wf_pool, schema=wf_schema)
    # httpx's ASGITransport runs NO lifespan — the test wires the app's
    # listener exactly as the lifespan does (one backend listener).
    listener = HitlListener(wf_pool, wf_schema)
    app.state.listener = listener

    async with (
        listener,
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://demo") as http,
    ):
        # THE DEMO'S LOGIN: a token scoped to the run.
        minted = (await http.get(f"/runs/{flow_id}/token")).json()
        assert set(minted) == {"token", "run_id"} and minted["run_id"] == flow_id
        # THE AUTHZ IS THE RUN SCOPE: a token for ANOTHER run (minted
        # through the app's OWN identity store) cannot subscribe here;
        # neither can the tokenless.
        other = (await http.get("/runs/another-run/token")).json()
        scoped_elsewhere = (
            await http.get(f"/runs/{flow_id}/events", params={"token": other["token"]})
        ).status_code
        tokenless = (await http.get(f"/runs/{flow_id}/events")).status_code
        assert scoped_elsewhere == 403 and tokenless == 403

        # THE SUBSCRIBE + THE ROW READ: the board's stream carries the
        # create AND the context fetched from the row.
        board = await _read_until(
            listener, demo_client, flow_id, lambda b: b.reconciled and bool(b.contexts)
        )
        assert len(board.cards) == 1, board.cards
        (hold_id, card) = next(iter(board.cards.items()))
        assert card["run_id"] == flow_id and card["signal"] == "Approval"
        assert board.contexts[hold_id]["reason"] == "the demo's gate", board.contexts

        # THE RESOLVE POST: the typed door, the demo's endpoint.
        answer = await http.post(
            f"/runs/{flow_id}/holds/{hold_id}/resolve",
            params={"token": minted["token"]},
            json={"verdict": "approve", "note": "the web half's yes"},
        )
        assert answer.status_code == 200, answer.text
        assert answer.json()["status"] == "delivered", answer.json()

        # THE CARD CLEARS ON THE BROADCAST: the stream's HoldResolved —
        # the POST's answer is the door's receipt, the CARD closes on
        # the frame.
        cleared = await _read_until(listener, demo_client, flow_id, lambda b: not b.cards)
        assert not cleared.cards, f"the card never cleared: {cleared.cards}"


async def test_the_board_reconciles_after_an_outage(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE WEB HALF'S RECONCILE (C1's consumer face on the demo): a
    hold resolved while the app's listener was DOWN never clears its
    card from a notify — the Backfilled frame does the clearing on the
    next subscribe. The board's law, exercised through the demo's
    stream: the stale events replayed from the history cannot keep a
    dead card open (the ghost dies the same death)."""
    flow_id, _runner = await _held_flow(wf_conn, wf_schema, wf_pool, name="T26_web_reconcile_flow")
    demo_client = HitlClient(wf_pool, schema=wf_schema)
    listener = HitlListener(wf_pool, wf_schema)

    await listener.start()
    try:
        board = await _read_until(listener, demo_client, flow_id, lambda b: b.reconciled)
        assert len(board.cards) == 1, board.cards
        hold_id = next(iter(board.cards))
        # THE OUTAGE: the listener down, the rows move on.
        await listener.stop()
        resolved = await demo_client.resolve(hold_id, {"verdict": "approve"})
        assert resolved.status == "delivered"
        # THE RECONNECT: the same listener (its history replays the
        # ghost create), the fresh backfill — the card clears on the
        # SNAPSHOT, never on a notify.
        await listener.start()
        board2 = await _read_until(listener, demo_client, flow_id, lambda b: b.reconciled)
        assert board2.cards == {}, f"the dead hold's card survived the reconcile: {board2.cards}"
    finally:
        await listener.stop()
