"""The demo app's workflow example (T16): the doc-ingest pipeline LIVE —
the same abstract graph the docs example teaches (docs/examples/doc-ingest.md),
wired behind HTTP with the admin's run explorer attached.

The three demonstrable properties, each on a real run:

1. A map with a FAILING CHILD + collect — ``doc-doomed``'s first
   enrichment fails through its ladder; the failure fans in as the
   surfaced partial result (never a silent retry storm; the siblings
   never re-run).
2. The budget-capped loop with the HELD approval — the review stage
   holds on a typed ``ReviewDecision``; the loop's budget PAUSES while a
   human decides (the hold counts for nothing on wake); the Resolve
   form (the admin's run page) delivers the typed payload and the loop
   resumes toward publish — or exhausts to the NAMED escalation.
3. The admin graph view LIVE — the run renders on the admin's workflow
   page (``/taskq/workflows/{run_id}``) with the SSE patches: the
   collapsed map hexagon, the taken paths, the failure badge, the held
   node's amber.

The demo drives its own flows in-process (a background drive loop in
the app's lifespan) — the worker-wiring round is ticket 19's lane; the
rows are the same rows a worker process would drive.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.workflows import (
    Done,
    FlowRunner,
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

#: The demo's batch: one doc is ARMED to fail its first enrichment
#: attempt (the failing child + collect's demonstration; the retry takes
#: the success path — the operator watched the fix land, or the ladder
#: healed it).
DEMO_DOCS: list[str] = [f"doc-{i:03}" for i in range(6)] + ["doc-doomed"]

_DOC_SOURCE: dict[str, str] = {
    doc_id: f"the text of {doc_id} (the demo's stand-in corpus)" for doc_id in DEMO_DOCS
}


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
    """The typed terminal verdict: the failures ride the report, named."""

    published: list[str]
    failed: list[str]
    review_note: str = ""


async def ingest_body(ctx: Any, params: IngestBatch) -> list[str]:
    return params.doc_ids


async def enrich_item(ctx: Any, doc_id: str) -> Summary | Unreadable:
    # THE FAILING CHILD (property 1): armed on the FIRST attempt only —
    # the ladder re-runs it ALONE (the siblings and the succeeded items
    # never re-run).
    if doc_id == "doc-doomed" and ctx.attempt < 2:
        raise RuntimeError("the demo's armed transient failure (doc-doomed)")
    text = _DOC_SOURCE.get(doc_id)
    if text is None:
        return Unreadable(doc_id=doc_id, reason="missing from the corpus")
    return Summary(doc_id=doc_id, text=text)


def _readable(items: list[Summary | Unreadable]) -> list[str]:
    """The per-node exhaustiveness idiom (match + assert_never — the
    portable guard BOTH checkers reject a missing arm through)."""
    out: list[str] = []
    for item in items:
        match item:
            case Summary():
                out.append(item.doc_id)
            case Unreadable():
                out.append(item.doc_id)  # surfaced, never dropped
            case _ as it:
                from typing import assert_never

                assert_never(it)
    return out


async def _route_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    """The router: dead-letters the unreadable, declares itself
    fire-and-forget in the wiring (the sink)."""
    return _readable(enriched)


async def summarize_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    return _readable(enriched)


async def extract_entities_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    return _readable(enriched)


async def classify_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    return _readable(enriched)


async def review_iteration(ctx: Any, carry: int) -> Done[str] | Refine[int]:
    # THE HOLD INSIDE THE LOOP (property 2): the typed review pauses the
    # budget; a reject refines with the note; the walls are named.
    decision = await ctx.wait_signal(
        (ReviewDecision,), timeout_s=7 * 24 * 3600.0, reason="editorial review (the demo)"
    )
    if decision.verdict == "approve":
        return Done(decision.note)
    return Refine(carry + 1)


async def publish_body(
    ctx: Any, review_note: str, ready: list[str], labels: list[str]
) -> PublishReport:
    return PublishReport(published=ready, failed=[], review_note=review_note)


wf_app = WorkflowApp()

REVIEW_GATE = GateDecl(
    name="ReviewDecision", payload_models=(ReviewDecision,), timeout_s=7 * 24 * 3600.0
)


@wf_app.workflow("doc_ingest")
def doc_ingest() -> object:
    ingested = step(ingest_body, IngestBatch(doc_ids=list(DEMO_DOCS)), key="ingest")
    enriched = map_source(ingested, enrich_item, key="enrich", queue="demo-enrich")
    routed = step(_route_body, enriched, key="route", queue="demo-cpu")

    summaries = step(summarize_body, enriched, key="summarize", queue="demo-cpu")
    entities = step(extract_entities_body, enriched, key="extract_entities", queue="demo-io")
    labels = step(classify_body, enriched, key="classify", queue="demo-classify")

    barrier = gather([summaries, entities], on_failure="fail_closed")
    maybe_labels = gather([labels], on_failure="maybe")
    sink(routed)

    review = loop(
        "review",
        review_iteration,
        carry=0,
        max_iterations=3,
        budget_s=3600.0,
        on_exhausted="escalate",
        # THE TYPED DOOR'S COMPILE VISIBILITY (T16's gates= addition):
        # the mid-loop hold is resolvable by the admin's resolve door.
        gates=(REVIEW_GATE,),
    )
    published = step(publish_body, review, barrier, maybe_labels, key="publish")
    return build(published)


async def trigger_run(pool: asyncpg.Pool, schema: str, run_key: str | None = None) -> str:
    """The demo's trigger: one run of the demo graph (the run key makes
    the trigger idempotent when given)."""
    runner = FlowRunner(wf_app.get("doc_ingest"), pool, schema)
    flow_id = await runner.create_flow(input=IngestBatch(doc_ids=list(DEMO_DOCS)), run_key=run_key)
    return str(flow_id)


async def _drive_pending(pool: asyncpg.Pool, schema: str) -> None:
    """One drive pass over the pending runs (the demo's in-process
    driver; a worker process would drive the same rows)."""
    rows = await pool.fetch(
        f"SELECT id FROM \"{schema}\".jobs WHERE step_key = '__flow__' "  # noqa: S608  # Why: the schema is the app's settings-validated identifier; every value is a bound parameter.
        "AND status = 'running' LIMIT 5"
    )
    compiled = wf_app.get("doc_ingest")
    for row in rows:
        runner = FlowRunner(compiled, pool, schema)
        with contextlib.suppress(asyncio.CancelledError):
            await runner.drive(row["id"], until="held", max_ticks=200)


async def drive_loop(pool: asyncpg.Pool, schema: str) -> AsyncIterator[None]:
    """The demo's drive loop: every 0.5 s, one pass over the pending
    runs. Cancelled with the app's shutdown. LOUD: a silently-dead
    driver looks exactly like a wedged run (the stranger test's
    stumble #3 — the loop died without a trace and every run froze
    mid-flight)."""
    import structlog

    log = structlog.get_logger("examples.workflow_drive_loop")
    log.info("workflow-drive-loop-started", schema=schema)
    while True:
        try:
            await _drive_pending(pool, schema)
        except asyncio.CancelledError:
            raise
        except Exception:
            # The demo's loop must survive a bad tick; the rows are the
            # truth and the next pass re-drives — but it says so LOUDLY.
            log.exception("workflow-drive-pass-failed")
        await asyncio.sleep(0.5)
