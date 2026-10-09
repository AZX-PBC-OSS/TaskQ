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
import enum
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import structlog
from pydantic import BaseModel

from taskq import ActorRef
from taskq import actor as vanilla_actor
from taskq.workflows import (
    Done,
    FlowRunner,
    Refine,
    RunClaim,
    WorkflowApp,
    build,
    chain_source,
    gather,
    loop,
    map_source,
    sink,
    step,
)
from taskq.workflows.api import GateDecl
from taskq.workflows.chain import DONE, Chain, Route, Step
from taskq.workflows.ledger import RunClaim

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
    """The typed terminal verdict: the failures ride the report, named —
    DERIVED from the run's own collect (the envelope is the truth,
    never a hardcoded `[]`)."""

    published: list[str]
    dead_lettered: list[str]
    failed: list[str]
    review_note: str = ""


async def ingest_body(ctx: Any, params: IngestBatch | None = None) -> list[str]:
    """The run's OWN input is the truth: the trigger's
    `create_flow(input=…)` IS consumed (the body reads `ctx.input` —
    never accepted-and-ignored); the wiring's declared batch is the
    default (a bare `create_flow()` still runs the demo corpus)."""
    raw = ctx.input if ctx.input is not None else (params.model_dump() if params else None)
    return sorted(IngestBatch.model_validate(raw).doc_ids) if raw is not None else []


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
    portable guard BOTH checkers reject a missing arm through): BOTH
    arms surface (the router's and the classifier's shape)."""
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


def _summary_doc(item: Summary | Unreadable) -> str | None:
    """The READABLE arm's key, or None — the enrichers' own walk (the
    exhaustiveness idiom is per-node; a node that forgets an arm is a
    checker error)."""
    match item:
        case Summary():
            return item.doc_id
        case Unreadable():
            return None  # no text was readable — the ROUTER's verdict names it
        case _ as it:
            from typing import assert_never

            assert_never(it)


async def _route_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    """The router: dead-letters the unreadable, declares itself
    fire-and-forget in the wiring (the sink)."""
    return _readable(enriched)


async def _screen_step_body(ctx: Any, item: Any) -> ScreenOutcome:
    """The SCREEN step's body: the record's text present → READABLE,
    else UNREADABLE. The typed outcome IS the router's decision."""
    text = _DOC_SOURCE.get(str(item))
    return ScreenOutcome.READABLE if text else ScreenOutcome.UNREADABLE


async def _index_step_body(ctx: Any, item: Any) -> IndexOutcome:
    return IndexOutcome.OK


async def _dead_step_body(ctx: Any, item: Any) -> DeadOutcome:
    return DeadOutcome.FILED


async def _screen_source_body(ctx: Any) -> None:
    """The chain source's paged generator: ONE yield = ONE page's emit
    tx (the children + the edges + the cursor checkpoint). The demo's
    page: every doc id, one record each (the corpus is the module's own
    constant — the source body reads the CLOSURE, not a params arg)."""
    for doc_id in DEMO_DOCS:
        from taskq.workflows.chain import chain_start

        child = chain_start(
            SCREEN_CHAIN, doc_id, map_index=abs(hash(doc_id)) % 32000, trace_id=doc_id
        )
        # THE CURSOR IS THE BODY'S OWN BOOKKEEPING: the emit's tx
        # checkpoints it (the resume continues from the last COMMITTED
        # page — the crash-recovery contract).
        await ctx.emit_batch([child], cursor={"page": 0, "doc": doc_id})


wf_app = WorkflowApp()

# ── DEMO LEG 4 — THE CONDITIONAL ROUTER (T20's chain, live) ─────────────
#
# The chain declared ONCE; each step's body returns its typed OUTCOME and
# the Route sends the record to the next step (or DONE). The
# doc-ingest's conditional: a SCREEN step sorts each document — the
# READABLE ones enrich, the unreadable ones dead-letter, the total route
# is the fence (an outcome with no arm is the loud RouterNotTotal).


class ScreenOutcome(enum.Enum):
    READABLE = "readable"
    UNREADABLE = "unreadable"


class IndexOutcome(enum.Enum):
    OK = "ok"


class DeadOutcome(enum.Enum):
    FILED = "filed"


SCREEN_CHAIN = Chain(
    name="doc-screen-chain",
    start="screen",
    actor="wf-demo-screen",
    queue="demo-screen",
    steps={
        "screen": Step(
            body=_screen_step_body,
            outcomes=ScreenOutcome,
            route=Route(
                {
                    ScreenOutcome.READABLE: "index",
                    ScreenOutcome.UNREADABLE: "dead_letter",
                }
            ),
        ),
        "index": Step(
            body=_index_step_body,
            outcomes=IndexOutcome,
            route=Route({IndexOutcome.OK: DONE}),
        ),
        "dead_letter": Step(
            body=_dead_step_body,
            outcomes=DeadOutcome,
            route=Route({DeadOutcome.FILED: DONE}),
        ),
    },
)


async def summarize_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    """The summaries' keys — the readable arm ONLY (a summary exists
    only where the text was readable)."""
    return [k for it in enriched if (k := _summary_doc(it)) is not None]


async def extract_entities_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    """The entity pass — the second REQUIRED consumer: it too runs
    where the text was readable; the walk is its own."""
    return [k for it in enriched if (k := _summary_doc(it)) is not None]


async def classify_body(ctx: Any, enriched: list[Summary | Unreadable]) -> list[str]:
    """The classifier labels EVERY document — both arms surface (the
    publish's AND-join reads them)."""
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
    ctx: Any,
    review_note: str,
    ready: list[str],
    labels: list[str],
    enriched: list[Summary | Unreadable],
) -> PublishReport:
    """The report DERIVES from the run's own collect (the envelope is
    the truth, never a hardcoded `[]`): `dead_lettered` names the
    Unreadable arm; `failed` is the batch's residual (a doc that
    neither published nor dead-lettered); `published` is the barrier's
    fan-in DEDUPED to identities (the gather fans in BOTH enrichers —
    each covered the corpus — so a document publishes ONCE), gated by
    the classifier's label (the AND-join's publish rule)."""
    published = sorted(set(ready) & set(labels))
    dead_lettered = sorted(it.doc_id for it in enriched if isinstance(it, Unreadable))
    raw = ctx.input
    batch_ids = (
        IngestBatch.model_validate(raw).doc_ids
        if raw is not None
        else [it.doc_id for it in enriched]
    )
    failed = sorted(set(batch_ids) - set(published) - set(dead_lettered))
    return PublishReport(
        published=published,
        dead_lettered=dead_lettered,
        failed=failed,
        review_note=review_note,
    )


@wf_app.workflow("doc_screen_router")
def doc_screen_router() -> object:
    """LEG 4's workflow: the chain source fans each record's chain; the
    ROUTE (the conditional edge map) sends the readable docs to index,
    the unreadable to the dead-letter — the conditional routing LIVE."""
    return build(chain_source(SCREEN_CHAIN, _screen_source_body, key="screen_source"))


class DemoHeartbeat(BaseModel):
    seq: int = 0


@vanilla_actor
async def demo_heartbeat(payload: DemoHeartbeat) -> DemoHeartbeat:
    """The workers' registry face: the fleet's vanilla actor the demo's
    worker processes subscribe with (the WORKFLOW definitions ride the
    app's import — the D1 registry — and the boot projection reads the
    app from `iter_imported_apps`; the registry needs one REAL actor to
    boot)."""
    return payload


ACTORS: dict[str, ActorRef[Any, Any]] = {"demo_heartbeat": demo_heartbeat}
"""The worker's `--actors` registry (the demo's vanilla-actor face)."""


REVIEW_GATE = GateDecl(
    name="ReviewDecision", payload_models=(ReviewDecision,), timeout_s=7 * 24 * 3600.0
)


@wf_app.workflow("doc_ingest")
def doc_ingest() -> object:
    # THE SPLIT PLACEMENT: the map's children inherit the SOURCE's actor —
    # the enrich queue's actor rides the ingest step (one actor, one queue).
    ingested = step(
        ingest_body,
        IngestBatch(doc_ids=list(DEMO_DOCS)),
        key="ingest",
        actor="wf-demo-enrich",
        queue="demo-enrich",
    )
    # The item ladder's knob, named where the ladder lives (each child
    # re-runs ALONE up to this bound — doc-doomed's attempt walks 1 → 2
    # against it, the siblings never re-run).
    enriched = map_source(ingested, enrich_item, key="enrich", queue="demo-enrich", max_attempts=3)
    routed = step(_route_body, enriched, key="route", actor="wf-demo-cpu", queue="demo-cpu")

    summaries = step(
        summarize_body, enriched, key="summarize", actor="wf-demo-cpu", queue="demo-cpu"
    )
    entities = step(
        extract_entities_body, enriched, key="extract_entities", actor="wf-demo-io", queue="demo-io"
    )
    labels = step(
        classify_body, enriched, key="classify", actor="wf-demo-classify", queue="demo-classify"
    )

    barrier = gather([summaries, entities], on_failure="fail_closed")
    maybe_labels = gather([labels], on_failure="maybe")
    sink(routed)

    review = loop(
        "review",
        review_iteration,
        initial=0,
        max_iterations=3,
        budget_s=3600.0,
        on_exhausted="escalate",
        # THE TYPED DOOR'S COMPILE VISIBILITY (T16's gates= addition):
        # the mid-loop hold is resolvable by the admin's resolve door.
        gates=(REVIEW_GATE,),
    )
    # The publish consumes the collect TOO (its fourth parent): the
    # report derives from the map join's items — the envelope is the
    # truth.
    published = step(
        publish_body,
        review,
        barrier,
        maybe_labels,
        enriched,
        key="publish",
        actor="wf-demo-publish",
        queue="demo-publish",
    )
    return build(published)


async def trigger_run(pool: asyncpg.Pool, schema: str, run_key: str | None = None) -> RunClaim:
    """The demo's trigger: one run of the demo graph (the run key makes
    the trigger idempotent when given). THE CLAIM SURFACE HONEST: the
    TYPED claim returns — the claim's ``kind`` states the demo's
    202-vs-409 distinction (``created``/``existing-running`` → 202;
    ``existing-terminal`` → the REFUSED-TO-REUSE 409, the prior run's id
    + status on the envelope) — never a bare id and a silent 202."""
    runner = FlowRunner(wf_app.get("doc_ingest"), pool, schema)
    return await runner.create_flow(input=IngestBatch(doc_ids=list(DEMO_DOCS)), run_key=run_key)


async def trigger_router_run(pool: asyncpg.Pool, schema: str, run_key: str | None = None) -> str:
    """LEG 4's trigger: one doc_screen_router run (the T20 chain's
    conditional routing LIVE; the run key makes the trigger idempotent
    when given). The route the README points at: `POST
    /workflows/doc_screen_router/run`."""
    runner = FlowRunner(wf_app.get("doc_screen_router"), pool, schema)
    claim = await runner.create_flow(run_key=run_key)
    return str(claim.flow_id)


#: The drive loop's OWN logger (module-level: both the pass and the loop
#: log through one name).
log = structlog.get_logger("examples.workflow_drive_loop")

#: The dedup set behind the loop's loud-once discipline (the foreign
#: names, the per-run error signatures, the max_ticks notices). BOUNDED:
#: a demo that outgrows 256 distinct signatures has a different problem.
_LOUD_ONCE: set[str] = set()


async def _drive_pending(pool: asyncpg.Pool, schema: str) -> None:
    """One drive pass over the pending runs (the demo's in-process
    driver; a worker process would drive the same rows).

    THE ROWS-ARE-TRUTH LAW (the poison-loop cure): the pass derives from
    THIS app's registry — each run's runner is built from the run's OWN
    stamped workflow name, a foreign workflow's run (any other app's run
    sharing the schema) is tolerated LOUDLY-ONCE and skipped (its step
    keys resolve against ITS registry, never ours), and ONE run's
    failure is THAT run's story: the pass continues to the runs behind
    it (the claim sequence's ORDER — a poison run at the head starves
    nothing), and the error is DEDUPED (logged once per signature), not
    spammed every 0.5 s forever."""
    rows = await pool.fetch(
        f"SELECT id, metadata->>'workflow' AS workflow FROM \"{schema}\".jobs "  # noqa: S608  # Why: the schema is the app's settings-validated identifier; every value is a bound parameter.
        "WHERE step_key = '__flow__' "
        "AND status = 'running' ORDER BY id LIMIT 5"
    )
    for row in rows:
        name = row["workflow"]
        run_sig = f"{row['id']}:{name}"
        try:
            compiled = wf_app.get(name)
        except KeyError:
            # THE FOREIGN RUN: shared schema, not this app's workflow —
            # skip loudly ONCE per workflow name, never per pass.
            if name not in _LOUD_ONCE:
                _LOUD_ONCE.add(name)
                log.warning(
                    "workflow-drive-foreign-run-skipped",
                    workflow=name,
                    run_id=str(row["id"]),
                    detail="the run's workflow is not registered on THIS "
                    "app — its rows belong to another definition's driver",
                )
            continue
        runner = FlowRunner(compiled, pool, schema)
        try:
            outcome = await runner.drive(row["id"], until="held", max_ticks=200)
        except asyncio.CancelledError:
            raise  # THE CANCEL IS NEVER SWALLOWED (the shutdown discipline)
        except Exception as exc:
            # THE PER-RUN ISOLATION: a run's failure is THAT run's story
            # — the loop continues, the runs behind it still get their
            # pass; the error is DEDUPED (once per run + error class +
            # message), not a 0.5 s spam.
            sig = f"{run_sig}:{type(exc).__name__}:{exc}"
            if sig not in _LOUD_ONCE:
                _LOUD_ONCE.add(sig)
                log.exception("workflow-drive-run-failed", run_id=str(row["id"]), workflow=name)
            continue
        if outcome == "max_ticks" and f"max_ticks:{run_sig}" not in _LOUD_ONCE:
            _LOUD_ONCE.add(f"max_ticks:{run_sig}")
            log.warning(
                "workflow-drive-max-ticks",
                run_id=str(row["id"]),
                workflow=name,
                detail="the pass exhausted max_ticks on a quiescent run — "
                "a bounded stop is a defect (the run wedged), never a wait",
            )


async def drive_loop(pool: asyncpg.Pool, schema: str) -> AsyncIterator[None]:
    """The demo's drive loop: every 0.5 s, one pass over the pending
    runs. Cancelled with the app's shutdown. LOUD: a silently-dead
    driver looks exactly like a wedged run (the stranger test's
    stumble #3 — the loop died without a trace and every run froze
    mid-flight). The cancel AWAITS this loop (the lifespan's
    shutdown discipline — a consumed cancellation is never re-delivered)."""
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
