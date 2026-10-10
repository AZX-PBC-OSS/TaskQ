"""The fleet demo's workflow face (Act 8): the doc-ingest shape's
SMALLEST leg — the source + ONE conditional chain + ONE hold resolved.

* the SOURCE: the paged emit (each yield = ONE emit tx; the records ride
  the chain);
* the CONDITIONAL: the screen step's typed outcome routes — READABLE →
  enrich, UNREADABLE → DONE (the router's totals are the fence);
* the HOLD: the enrich step awaits the typed ``ReviewDecision`` — the
  demo's act resolves it (the operator's stand-in: `HitlClient.resolve`).

The workflow definitions ride the module's import (the D1 registry); the
fleet worker's boot projection reads the app from `iter_imported_apps` —
the fleet's own worker executes these rows through the intercept.
"""

from __future__ import annotations

import enum

from pydantic import BaseModel

from taskq.workflows import (
    DONE,
    Chain,
    Expired,
    GateDecl,
    HitlClient,
    Promise,
    Route,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
)

wf_app = WorkflowApp()

FLEETWF_QUEUES = "fleetwf"
"""The workflow cohorts' queue (the workers subscribe; the boot
projection declares the actor)."""


class ReviewDecision(BaseModel):
    verdict: str  # "approve" | "reject"
    note: str = ""


REVIEW_GATE = GateDecl(name="ReviewDecision", payload_models=(ReviewDecision,), timeout_s=600.0)

_DOCS: dict[str, str] = {
    "doc-a": "the text of doc-a",
    "doc-b": "the text of doc-b",
    "doc-unreadable": "",  # the UNREADABLE arm's record
}


class ScreenOutcome(enum.Enum):
    READABLE = "readable"
    UNREADABLE = "unreadable"


class EnrichOutcome(enum.Enum):
    OK = "ok"


async def screen_body(ctx: StepContext, doc_id: str) -> ScreenOutcome:
    """The conditional's step: the record's text present → READABLE,
    else UNREADABLE (the router's decision IS the body's return)."""
    text = _DOCS.get(doc_id)
    if not text:
        print(f"  [screen] {doc_id} → UNREADABLE")
        return ScreenOutcome.UNREADABLE
    print(f"  [screen] {doc_id} → READABLE")
    return ScreenOutcome.READABLE


async def enrich_body(ctx: StepContext, doc_id: str) -> EnrichOutcome:
    """THE HOLD (the smallest leg's point): the typed review — the
    workflow pauses here until the operator resolves. THE EXPIRY IS A
    VALUE (T26): the expiry member is the typed stop."""
    outcome = await ctx.wait_signal(
        (ReviewDecision,), timeout_s=600.0, reason="the fleet demo's review hold"
    )
    match outcome:
        case ReviewDecision() as decision:
            print(f"  [enrich] {doc_id} resumed: {decision.verdict} ({decision.note})")
            return EnrichOutcome.OK
        case Expired():
            print(f"  [enrich] {doc_id} expired: finishing with what we have")
            return EnrichOutcome.OK


SCREEN_CHAIN = Chain(
    name="fleet-doc-screen",
    start="screen",
    steps={
        "screen": Step(
            body=screen_body,
            outcomes=ScreenOutcome,
            route=Route({ScreenOutcome.READABLE: "enrich", ScreenOutcome.UNREADABLE: DONE}),
        ),
        "enrich": Step(
            body=enrich_body,
            outcomes=EnrichOutcome,
            route=Route({EnrichOutcome.OK: DONE}),
        ),
    },
)


@wf_app.workflow("fleet_doc_ingest")
def fleet_doc_ingest() -> Promise[object]:
    return build(chain_source(SCREEN_CHAIN, source_body, key="screen_source"))


async def source_body(ctx: StepContext) -> None:
    """The paged source: ONE yield = ONE emit tx (the chain's children +
    the edges + the cursor checkpoint)."""
    from taskq.workflows.chain import chain_start

    page = []
    for i, doc_id in enumerate(_DOCS):
        page.append(chain_start(SCREEN_CHAIN, doc_id, map_index=i, trace_id=doc_id))
    await ctx.emit_batch(page, cursor={"page": 0})


async def resolve_any_hold(dsn: str, schema: str, run_id: str) -> str | None:
    """The operator's stand-in: the run's pending hold → resolved."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        client = HitlClient(pool, schema=schema)
        holds = await client.list(run_id)
        if not holds:
            return None
        result = await client.resolve(
            holds[0].hold_id, {"verdict": "approve", "note": "the fleet demo's resolve"}
        )
        return result.status
    finally:
        await pool.close()
