# The doc-ingest pipeline — workflows in one graph

A document-ingestion pipeline: documents arrive, get enriched, pass an
editorial review, and publish — with the failures REPORTED, never
swallowed. This page is the minimal copy-paste form; the
[demo app](../../../examples/) runs the SAME graph live with
the admin's run explorer attached.

The example demonstrates the nine load-bearing shapes of the flow API:

1. fork-of-N enrichment with heterogeneous placement (three queues),
2. per-item fork + collect at batch scale — 997 `Ok` + 3 `Failed` fan in
   as ONE collect, the partial result carried,
3. the HITL approval hold with a typed payload (`ReviewDecision`),
4. the retry ladder + partial-retry isolation (a failed item re-runs
   alone),
5. the barrier join — the edges' policies are the worked duality
   (`fail_closed` over the required path, `maybe` over the
   classifiable one),
6. the budget-capped loop with the held approval (the budget PAUSES
   while a human reviews — the hold counts for nothing on wake),
7. the union routing shape — `Summary | Unreadable` consumed with
   `match` + `assert_never` (per-node exhaustiveness; the portable
   idiom BOTH checkers guard a missing arm through),
8. the typed terminal payload — the run ends in a `PublishReport`
   carrying per-document outcomes, never a bare success-or-nothing,
9. the cron-fired nightly refresh — the cron-slot key IS the run key:
   the same slot twice produces ONE run.

```python
import asyncio
from typing import assert_never

from pydantic import BaseModel

from taskq.workflows import (
    Done,
    Refine,
    WorkflowApp,
    build,
    gather,
    loop,
    map_source,
    sink,
    step,
)
from taskq.workflows.api import GateDecl


# ── the payload contract (the typed doors' shapes) ──────────────────────


class IngestBatch(BaseModel):
    doc_ids: list[str]


class Summary(BaseModel):
    doc_id: str
    text: str


class Unreadable(BaseModel):
    doc_id: str
    reason: str


class ReviewDecision(BaseModel):
    verdict: str  # "approve" | "reject"
    note: str = ""


class PublishReport(BaseModel):
    """Shape 8 — the TYPED TERMINAL VERDICT: the run cannot look healthy
    while the work was wrong (the failures ride the report, named)."""

    published: list[str]
    dead_lettered: list[str]
    failed: list[str]
    review_note: str = ""


# ── the bodies (one queue each — shape 1's heterogeneous placement) ─────

_DOC_SOURCE: dict[str, str] = {f"doc-{i:03}": f"the text of document {i}" for i in range(12)}


async def ingest_body(ctx: object, params: IngestBatch) -> list[str]:
    """The real body reads the source; the shape is the point. The LIST
    return is what the map fans per item."""
    return params.doc_ids


async def enrich_item(ctx: object, doc_id: str) -> Summary | Unreadable:
    """Per-document enrichment — shape 2's map item. The UNION return is
    shape 7's type story: a document is readable, or it is NOT (named)."""
    text = _DOC_SOURCE.get(doc_id)
    if text is None:
        return Unreadable(doc_id=doc_id, reason="missing from the source")
    return Summary(doc_id=doc_id, text=text)


async def route_body(ctx: object, enriched: list[Summary | Unreadable]) -> list[str]:
    """Shape 7 — the CONSUMED union: `match` + `assert_never` is the
    exhaustiveness idiom BOTH checkers reject a missing arm through.
    Deleting the `Unreadable` arm must red the residual. The dead
    letters ride a durable step; the router's own result is declared
    fire-and-forget (the wiring's `sink` — produced, recorded, never
    silently dropped)."""
    readable: list[str] = []
    dead: list[str] = []
    for item in enriched:
        match item:
            case Summary():
                readable.append(item.doc_id)
            case Unreadable():
                dead.append(item.doc_id)
            case _ as it:
                assert_never(it)
    if dead:
        print(f"dead-lettered: {dead}")
    return readable


async def summarize_body(ctx: object, enriched: list[Summary | Unreadable]) -> list[str]:
    """The exhaustiveness idiom is PER-NODE: every consumer of the union
    matches it — a node that forgets an arm is a checker error."""
    return [_readable_doc(it) for it in enriched]


async def extract_entities_body(ctx: object, enriched: list[Summary | Unreadable]) -> list[str]:
    return [_readable_doc(it) for it in enriched]


async def classify_body(ctx: object, enriched: list[Summary | Unreadable]) -> list[str]:
    # The MAYBE path's failure arm: an unclassifiable document's terminal
    # failure is SURFACED in the report and the barrier does NOT fail.
    return [_readable_doc(it) for it in enriched]


def _readable_doc(item: Summary | Unreadable) -> str:
    match item:
        case Summary():
            return item.doc_id
        case Unreadable():
            return item.doc_id  # the MAYBE siblings carry it as surfaced
        case _ as it:
            assert_never(it)


async def review_iteration(ctx: object, carry: int) -> Done[str] | Refine[str]:
    """Shapes 3+6 — the hold INSIDE the loop: the typed review pauses
    the budget (the hold counts for nothing on wake); a reject refines
    with the note; the walls are the cap + the budget, named."""
    decision = await ctx.wait_signal(
        (ReviewDecision,), timeout_s=7 * 24 * 3600.0, reason="editorial review before publish"
    )
    if decision.verdict == "approve":
        return Done(decision.note)
    return Refine(carry + 1)


async def publish_body(
    ctx: object, review_note: str, ready: list[str], labels: list[str]
) -> PublishReport:
    return PublishReport(published=ready, dead_lettered=[], failed=[], review_note=review_note)


# ── the wiring (ONE flow — the join-body cure makes that true) ──────────

app = WorkflowApp()

REVIEW_GATE = GateDecl(
    name="ReviewDecision", payload_models=(ReviewDecision,), timeout_s=7 * 24 * 3600.0
)


@app.workflow("doc_ingest")
def doc_ingest() -> object:
    ingested = step(ingest_body, IngestBatch(doc_ids=sorted(_DOC_SOURCE)), key="ingest")
    enriched = map_source(ingested, enrich_item, key="enrich", queue="enrich")

    # The collect's consumers: the router (dead-letters, sunk) + the three
    # enrichers (one queue each — shape 1's heterogeneous placement).
    routed = step(route_body, enriched, key="route", queue="cpu")

    summaries = step(summarize_body, enriched, key="summarize", queue="cpu")
    entities = step(extract_entities_body, enriched, key="extract_entities", queue="io")
    labels = step(classify_body, enriched, key="classify", queue="classify")

    # Shape 5 — THE WORKED DUALITY, both declarations in the wiring:
    # the summarize/entities gather is REQUIRED (a terminal failure fails
    # the barrier closed); the classification path is the MAYBE edge (the
    # barrier does not fail — the failure is surfaced in the report).
    barrier = gather([summaries, entities], on_failure="fail_closed")
    maybe_labels = gather([labels], on_failure="maybe")
    sink(routed)  # THE FIRE-AND-FORGET DECLARATION: recorded, never silent

    review = loop(
        "review",
        review_iteration,
        carry=0,
        max_iterations=3,
        budget_s=3600.0,
        on_exhausted="escalate",
        gates=(REVIEW_GATE,),  # the typed door's compile visibility
    )
    published = step(publish_body, review, barrier, maybe_labels, key="publish")
    return build(published)


# Shape 9 — the CRON-FIRED NIGHTLY REFRESH (~15 lines): the schedule's
# cron-slot key IS the run key. The same slot twice produces ONE run (the
# run-key arbiter's claim); the stale-key silent-replay fossil is
# prevented by construction — the second call returns the FIRST run's id.


async def run_nightly_refresh(schema: str, pool: object, slot: str) -> str:
    """The nightly cron's arm: key = <flow>:<slot-timestamp> — run-level
    idempotency demonstrated (the same slot twice → one run)."""
    from taskq.workflows import FlowRunner

    runner = FlowRunner(app.get("doc_ingest"), pool, schema)  # type: ignore[arg-type]
    run_id = await runner.create_flow(
        input=IngestBatch(doc_ids=sorted(_DOC_SOURCE)),
        run_key=f"doc_ingest:nightly:{slot}",
    )
    # The FIRST wall: drive to the review HOLD (a drive to `terminal`
    # around a hold would spin its tick loop on a quiescent run — the
    # held answer IS the quiescent stop).
    await runner.drive(run_id, until="held")
    return str(run_id)


async def main() -> None:
    import asyncpg
    import os

    from taskq.migrate import apply_pending
    from taskq.workflows.api._hitl import HitlClient

    schema = os.environ["TASKQ_SCHEMA_NAME"]
    dsn = os.environ["TASKQ_PG_DSN"]
    conn = await asyncpg.connect(dsn)
    await apply_pending(conn, schema=schema)
    await conn.close()

    pool = await asyncpg.create_pool(dsn)
    flow_id = await run_nightly_refresh(schema, pool, slot="2026-10-08T00:00Z")

    # The run HOLDS at the review (the typed gate). Answer it:
    client = HitlClient(pool, schema=schema)
    (hold,) = await client.list(flow_id)
    print("hold:", hold.hold_id, "-", hold.signal_name)
    result = await client.resolve(hold.hold_id, {"verdict": "approve", "note": "ship it"})
    print("resolve:", result.status)
    await runner.drive(flow_id)  # the resume → the terminal

    # The SAME slot again → the SAME run (the run-key's claim).
    same = await run_nightly_refresh(schema, pool, slot="2026-10-08T00:00Z")
    assert same == flow_id, "the cron-slot key's idempotency broke"

    await pool.close()
    print("done:", flow_id)


main_task = asyncio.run(main())
```

What each shape shows (and where to look):

| # | Shape | Where |
|---|-------|-------|
| 1 | heterogeneous fork (three queues) | `summarize` / `extract_entities` / `classify` — `queue=` on each |
| 2 | per-item fork + collect | `map_source(ingested, enrich_item, …)` |
| 3 | typed HITL hold | `ctx.wait_signal((ReviewDecision,), …)` inside the loop |
| 4 | retry ladder, isolated | `max_attempts` on the map item (a child re-runs alone) |
| 5 | the worked duality | `gather([summaries, entities], on_failure="fail_closed")` vs `gather([labels], on_failure="maybe")` |
| 6 | budget-capped loop, held budget | `loop("review", …, max_iterations=3, budget_s=3600.0)` |
| 7 | union routing | `match item:` + `case _ as it: assert_never(it)` in `route_body` |
| 8 | typed terminal verdict | `PublishReport` |
| 9 | cron-slot run key | `run_nightly_refresh`'s `run_key=f"doc_ingest:nightly:{slot}"` |

**Run it live**: the demo app wires this same graph behind HTTP with the
admin's run explorer — see `examples/admin_app.py` (the fastapi_app's
workflow routes) and the admin guide's run-explorer section. **The verify
loop**: the
example's fence executes in CI (`make test-docs-examples`), the fast tier
runs its `build()` + `validate()` without containers, and the compiled
Mermaid is byte-pinned.
