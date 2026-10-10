"""The workflow marches' shared app module (T15 — the system tier).

ONE module, imported by BOTH the test process and the tier's worker
subprocesses (``_wf_entry`` imports it so the worker's boot stamps
``workflow_execution: true`` and projects the (actor, queue) cohorts).
Every domain here is ABSTRACT (doc/source/review — the spec doc's
abstraction contract; zero case-study content).

The flows:

* ``deep_research`` — THE DEEP-RESEARCH MARCH's flow: the map with an
  armed transient AND an armed permanent failure under a ``collect``
  edge (the partial report), the budget-capped loop whose iteration
  body HOLDS on a typed gate (the multi-epoch multi-HITL), and the
  publish step whose typed wait can TIME OUT (the timeout face — the
  body pivots to the degraded report, never wedges).
* ``triage_chain`` — the T20 router lived on the tier: a ``chain_source``
  paged body (the streaming emit) whose per-record chains route on the
  body's OWN typed outcomes (the conditional chains).
* ``doc_ingest`` — THE DOC-INGEST MARCH's flow: the map + the fan-in
  barrier join (fail_closed) + the ``maybe`` edge (the collect policy)
  + the declared read-side aggregate (the progress aggregation).
* ``reclaim_target`` — the worker-deploy cell's flow: ONE long node a
  killed worker must lose to the lease/reclaim machinery, its join
  counter never moved.
* ``drain_map`` / ``drain_loop`` / ``drain_hold`` — the SIGTERM
  clean-drain cell's three shapes: mid-map-child, mid-loop-iteration,
  mid-hold-resume.

The bodies record their effects AS ROWS (the step ledger, the progress
channels, the results) — the invariants module reconciles them; no body
reaches outside the database (the tier's hermeticity).
"""

from __future__ import annotations

import enum
import json
from typing import Any

from pydantic import BaseModel

from taskq import JobContext, actor
from taskq.exceptions import SignalTimeoutError
from taskq.workflows import (
    Done,
    Promise,
    Refine,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
    gather,
    loop,
    map_source,
    step,
)
from taskq.workflows._types import EmitChild
from taskq.workflows.api import GateDecl
from taskq.workflows.api._sql_runner import render_sql
from taskq.workflows.chain import DONE, Chain, Route, Step

#: The march pods' queues: BOTH the vanilla actors' system_e2e and the
#: flows' framework-default queue (the join nodes the API creates are
#: hardcoded to "default" — one queue for the wf actor is the
#: projection's own law; the split placement is DISTINCT actor names per
#: queue, which no march flow needs).
MARCH_WORKER_QUEUES = "system_e2e,default"

#: The flows' queue: the framework default the join nodes share.
WF_QUEUE = "default"


class Kickoff(BaseModel):
    query: str


class SourceDoc(BaseModel):
    source_id: str
    text: str


class ReviewDecision(BaseModel):
    verdict: str  # "approve" | "refine"
    note: str = ""


class PublishApproval(BaseModel):
    approve: bool = True


class ResearchReport(BaseModel):
    fetched: list[str]
    failed: list[str]
    iterations: int
    review_note: str = ""
    degraded: bool = False


# ── the deep-research march's flows ──────────────────────────────────────

#: The armed failures: the map's body fails TRANSIENTLY on this source
#: once (the ladder heals it — the retried child succeeds alone), and
#: PERMANENTLY on this one (the collect edge fans the failure in — the
#: partial report names it).
_TRANSIENT_SOURCE = "src-transient"
_PERMANENT_SOURCE = "src-permanent"

_SOURCES = ["src-1", "src-2", _TRANSIENT_SOURCE, _PERMANENT_SOURCE]


async def kickoff_body(ctx: Any, params: Kickoff) -> list[str]:
    await ctx.progress(10, "sources enumerated", {"count": len(_SOURCES)})
    return list(_SOURCES)


async def fetch_source_body(ctx: Any, source_id: str) -> SourceDoc:
    if source_id == _TRANSIENT_SOURCE and ctx.attempt < 2:
        raise RuntimeError("the march's armed transient failure")
    if source_id == _PERMANENT_SOURCE:
        raise RuntimeError("the march's armed permanent failure")
    return SourceDoc(source_id=source_id, text=f"the text of {source_id}")


def _source_names(items: list[Any]) -> tuple[list[str], list[str]]:
    """The collect join's decode: the succeeded sources and the surfaced
    failures (the FailureInfo envelope names the latter — its
    ``node_key`` IS the failed child's key; the item's source id rides
    the error message). THE ROUND-TRIP HONESTY: the join's items arrive
    as PLAIN DICTS (the jsonb decode — the ledger is the truth), never
    the body's own models; the decode reads the SHAPES."""
    from taskq.workflows._types import FailureInfo

    ok: list[str] = []
    failed: list[str] = []
    for item in items:
        if isinstance(item, FailureInfo):
            failed.append(item.node_key)
        elif isinstance(item, SourceDoc):
            ok.append(item.source_id)
        elif isinstance(item, dict):
            if "node_key" in item:  # the collect's failure envelope, decoded
                failed.append(str(item["node_key"]))
            elif "source_id" in item:
                ok.append(str(item["source_id"]))
    return ok, failed


async def triage_body(ctx: Any, fetched: list[Any]) -> dict[str, object]:
    ok, failed = _source_names(fetched)
    # THE COLLECT'S ABSORPTION RECORD (the fan-in's own home): the failed
    # children ride the MAP JOIN ROW's metadata.failures (the typed
    # envelope), never the packed results — the body reads its PARENT
    # (the join) through the edge ledger (the edge ledger is the wiring's
    # own record; the triage node's parent IS the map join).
    async with ctx._pool.acquire() as conn:  # pyright: ignore[reportAttributeAccessIssue]
        failures_raw = await conn.fetchval(
            render_sql(
                "SELECT j.metadata->'failures' "
                "FROM {schema}.wf_edge e JOIN {schema}.jobs j ON j.id = e.parent_id "
                "WHERE e.child_id = $1",
                ctx._wsql.schema,  # pyright: ignore[reportAttributeAccessIssue]
            ),
            ctx.job_id,
        )
    if failures_raw:
        for item in failures_raw if isinstance(failures_raw, list) else json.loads(failures_raw):
            if not isinstance(item, dict) or "node_key" not in item:
                continue
            # THE MAP CHILD'S IDENTITY: the children share ONE step key
            # ('<parent>.item'); the item's own identity is the MAP
            # INDEX — the march's source order decodes it back to the
            # source id (the report names the SOURCE, never the row).
            idx = item.get("map_index")
            failed.append(
                str(_SOURCES[idx])
                if isinstance(idx, int) and idx < len(_SOURCES)
                else str(item["node_key"])
            )
    await ctx.progress(60, "triaged", {"fetched": len(ok), "failed": len(failed)})
    return {"fetched": ok, "failed": failed}


async def review_iteration(ctx: Any, carry: int) -> Done[str] | Refine[int]:
    # THE TYPED HOLD INSIDE THE LOOP: every iteration mints a NEW hold
    # epoch (the multi-HITL: N iterations = N distinct holds, each
    # addressed by id). An approve ENDS the loop; a refine advances the
    # carry (the carry advances exactly once per iteration).
    decision = await ctx.wait_signal(
        (ReviewDecision,),
        timeout_s=120.0,
        reason="the march's editorial review",
    )
    if decision.verdict == "approve":
        return Done(decision.note)
    return Refine(carry + 1)


async def escalation_body(ctx: Any, carry: int) -> str:
    return f"escalated-at-iteration-{carry}"


async def publish_body(ctx: Any, triage: dict[str, object], review_note: str) -> ResearchReport:
    # THE TIMEOUT FACE: the typed wait is BOUNDED; the expiry sweep's
    # abandonment RAISES SignalTimeoutError here — the body CATCHES it
    # and pivots (the degraded report), never wedges, never re-holds in
    # a loop (the re-wait dragon's fence).
    approved = True
    note = review_note
    degraded = False
    try:
        approval = await ctx.wait_signal(
            (PublishApproval,),
            timeout_s=5.0,
            reason="the march's publish approval (a SHORT wall)",
        )
        approved = bool(approval.approve)
    except SignalTimeoutError:
        degraded = True
        note = (
            f"{note} (degraded: the publish approval timed out)"
            if note
            else ("degraded: the publish approval timed out")
        )
    # An explicit DISAPPROVAL is also the degraded pivot (the operator
    # said no — the report says so, named).
    degraded = degraded or not approved
    fetched = triage.get("fetched", [])
    failed = triage.get("failed", [])
    return ResearchReport(
        fetched=list(fetched),  # pyright: ignore[reportArgumentType]  # Why: the triage dict's jsonb round-trip types its lists loosely; the report model re-validates.
        failed=list(failed),  # pyright: ignore[reportArgumentType]  # Why: same round-trip shape.
        iterations=0,  # stamped by the report's reader from the ledger
        review_note=note,
        degraded=degraded,
    )


research_app = WorkflowApp()


@research_app.workflow("deep_research")
def deep_research() -> Promise[object]:
    sources = step(kickoff_body, Kickoff(query="the march's query"), key="sources")
    fetched = map_source(sources, fetch_source_body, on_failure="collect")
    triage = step(triage_body, fetched, key="triage")
    review = loop(
        "review",
        review_iteration,
        initial=0,
        max_iterations=3,
        budget_s=600.0,
        on_exhausted="escalate",
        escalates_to=escalation_body,
        gates=(
            GateDecl(
                name="ReviewDecision",
                payload_models=(ReviewDecision,),
                timeout_s=120.0,
            ),
        ),
    )
    published = step(
        publish_body,
        triage,
        review,
        key="publish",
        gates=(
            GateDecl(
                name="PublishApproval",
                payload_models=(PublishApproval,),
                timeout_s=5.0,
            ),
        ),
    )
    return build(published)


@research_app.workflow("deep_research_cron")
def deep_research_cron() -> Promise[object]:
    """The CRON-FIRED variant: the SAME graph; the run key IS the cron
    slot (the G3 composition — the fire arm passes the slot as the run
    key, a double-fired slot is ONE run)."""
    return deep_research()


# ── the T20 router lived on the tier (the streaming emit source) ─────────


class TriageOutcome(enum.Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


async def _screen(ctx: Any, item: dict[str, object]) -> TriageOutcome:
    risk = float(item["risk"])
    return TriageOutcome.FLAGGED if risk > 0.8 else TriageOutcome.CLEAN


async def _manual_review(ctx: Any, item: dict[str, object]) -> ReviewOutcome:
    return ReviewOutcome.REVIEWED


class ReviewOutcome(enum.Enum):
    REVIEWED = "reviewed"


TRIAGE_CHAIN = Chain(
    name="march-source-triage",
    start="screen",
    steps={
        "screen": Step(
            body=_screen,
            outcomes=TriageOutcome,
            route=Route({TriageOutcome.CLEAN: DONE, TriageOutcome.FLAGGED: "manual_review"}),
        ),
        "manual_review": Step(
            body=_manual_review,
            outcomes=ReviewOutcome,
            route=Route({ReviewOutcome.REVIEWED: DONE}),
        ),
    },
)


async def triage_source(ctx: Any) -> None:
    """The paged source: TWO pages, emitted while the source stays
    running — each yield is ONE emit tx (the children + edges + the
    cursor checkpoint)."""
    pages: list[list[dict[str, object]]] = [
        [{"source_id": "s1", "risk": 0.1}, {"source_id": "s2", "risk": 0.95}],
        [{"source_id": "s3", "risk": 0.3}, {"source_id": "s4", "risk": 0.99}],
    ]
    for page_no, page in enumerate(pages):
        children: list[EmitChild] = [
            chain_start(
                TRIAGE_CHAIN,
                item,
                map_index=page_no * 100 + i,
                trace_id=f"src-{item['source_id']}",
            )
            for i, item in enumerate(page)
        ]
        await ctx.emit_batch(children, cursor={"page": page_no})


march_app = WorkflowApp()


@march_app.workflow("triage_chain")
def triage_chain() -> Promise[object]:
    src = chain_source(TRIAGE_CHAIN, triage_source, key="triage_source")
    return build(src)


# ── the doc-ingest march's flow ──────────────────────────────────────────


class IngestBatch(BaseModel):
    doc_ids: list[str]


class Enriched(BaseModel):
    doc_id: str


class Verdict(BaseModel):
    published: list[str]
    review_note: str = ""


def _sum_aggregate(rows: list[Any]) -> dict[str, object]:
    """The map's DECLARED READ-SIDE aggregate (T21 decision c): a pure fn
    over the children's decoded result rows, evaluated at read time."""
    return {"count": len(rows)}


async def ingest_body(ctx: Any, params: IngestBatch) -> list[str]:
    return params.doc_ids


async def enrich_body(ctx: Any, doc_id: str) -> Enriched:
    await ctx.progress(min(99, len(doc_id) * 5), f"enriching {doc_id}", None)
    return Enriched(doc_id=doc_id)


async def summarize_body(ctx: Any, enriched: list[Enriched]) -> list[str]:
    return [e.doc_id for e in enriched]


async def extract_entities_body(ctx: Any, enriched: list[Enriched]) -> list[str]:
    return [e.doc_id for e in enriched]


async def classify_body(ctx: Any, enriched: list[Enriched]) -> list[str]:
    # The map join packs the children AS-IS (a map over list items is a
    # collect of lists — the join's contract): the downstream reads the
    # ENRICHED records, never bare ids.
    return [e.doc_id for e in enriched]


async def ingest_publish_body(ctx: Any, docs: list[str], labels: list[str]) -> Verdict:
    # THE WIRE SHAPES (the barrier's contract): the barrier gather's
    # parents EACH returned a list — the gather flattens one level, so
    # `docs` is summaries+entities flattened (strs); the maybe gather's
    # single parent passes through the same shape (labels: strs).
    return Verdict(published=sorted(set(docs) & set(labels)))


ingest_app = WorkflowApp()


@ingest_app.workflow("doc_ingest_march")
def doc_ingest_march() -> Promise[object]:
    ingested = step(ingest_body, IngestBatch(doc_ids=["d1", "d2", "d3", "d4"]), key="ingest")
    enriched = map_source(ingested, enrich_body, aggregate=_sum_aggregate)
    summaries = step(summarize_body, enriched, key="summarize")
    entities = step(extract_entities_body, enriched, key="extract_entities")
    barrier = gather([summaries, entities], on_failure="fail_closed")
    labels = step(classify_body, enriched, key="classify")
    maybe_labels = gather([labels], on_failure="maybe")
    published = step(ingest_publish_body, barrier, maybe_labels, key="publish")
    return build(published)


# ── the deploy-matrix cells' flows ───────────────────────────────────────


async def long_node_body(ctx: Any, params: Kickoff) -> str:
    """The worker-deploy cell's long node: a body that outlives a killed
    worker's lease (the reclaim machinery re-pends it; the join counter
    never moves)."""
    import asyncio as _asyncio

    await _asyncio.sleep(float(params.query or 30))
    return "done"


async def downstream_body(ctx: Any, up: str) -> str:
    return f"down:{up}"


matrix_app = WorkflowApp()


@matrix_app.workflow("reclaim_target")
def reclaim_target() -> Promise[object]:
    long_node = step(long_node_body, Kickoff(query="30"), key="long", max_attempts=3)
    after = step(downstream_body, long_node, key="after")
    return build(after)


@matrix_app.workflow("drain_map")
def drain_map() -> Promise[object]:
    """The SIGTERM-drain cell's mid-map shape: a map whose child is
    mid-iteration when the drain lands; the retried child's map slot is
    preserved (retry-in-place), the items are applied exactly once."""
    sources = step(kickoff_body, Kickoff(query="drain"), key="sources")
    fetched = map_source(sources, fetch_source_body)
    after = step(downstream_body, fetched, key="after")
    return build(after)


async def drain_loop_iteration(ctx: Any, carry: int) -> Done[str] | Refine[int]:
    """The drain cell's mid-loop shape: the carry must advance EXACTLY
    ONCE across a requeue — the iteration body appends nothing to the
    DB itself (the ledger IS the timeline), so the once-only advance is
    read from the iteration rows. The body is SLOW (20s an iteration:
    the 3-iteration run outlives the 15s termination grace) so the
    SIGTERM lands MID-ITERATION — the drain requeues the node, the
    survivor's resume replays the applied iterations from the memo
    (never a re-run body) and finishes the rest."""
    import asyncio as _asyncio

    await _asyncio.sleep(20.0)
    if carry < 2:
        return Refine(carry + 1)
    return Done(f"carried-{carry}")


@matrix_app.workflow("drain_loop")
def drain_loop() -> Promise[object]:
    carried = loop("carry_loop", drain_loop_iteration, initial=0, max_iterations=5)
    after = step(downstream_body, carried, key="after")
    return build(after)


async def drain_hold_body(ctx: Any, params: Kickoff) -> str:
    decision = await ctx.wait_signal(
        (ReviewDecision,), timeout_s=60.0, reason="the drain cell's hold"
    )
    return decision.verdict


@matrix_app.workflow("drain_hold")
def drain_hold() -> Promise[object]:
    held = step(
        drain_hold_body,
        Kickoff(query="hold"),
        key="hold",
        gates=(
            GateDecl(
                name="ReviewDecision",
                payload_models=(ReviewDecision,),
                timeout_s=60.0,
            ),
        ),
    )
    after = step(downstream_body, held, key="after")
    return build(after)


@matrix_app.workflow("drift_target")
def drift_target() -> Promise[object]:
    """The config-drift cell's flow: the downstream node's queue NOTHING
    serves after the operator re-routes it — the node enters `blocked`
    with the blocking reason naming the unserved queue."""
    long_node = step(long_node_body, Kickoff(query="1"), key="long")
    gone = step(downstream_body, long_node, key="gone", actor="wf-drift", queue="nowhere_queue")
    return build(gone)


#: The march flows' COMPILED definitions, by name (the test side's
#: create-run surface resolves here — one registry, one compile).
MARCH_FLOWS: dict[str, Any] = {
    "deep_research": research_app.get("deep_research"),
    "deep_research_cron": research_app.get("deep_research_cron"),
    "triage_chain": march_app.get("triage_chain"),
    "doc_ingest_march": ingest_app.get("doc_ingest_march"),
    "reclaim_target": matrix_app.get("reclaim_target"),
    "drain_map": matrix_app.get("drain_map"),
    "drain_loop": matrix_app.get("drain_loop"),
    "drain_hold": matrix_app.get("drain_hold"),
    "drift_target": matrix_app.get("drift_target"),
}


# ── the cron kickoff's bridge actor (the G3 composition's fire leg) ──────


class CronKick(BaseModel):
    slot: str


@actor(name="wf_research_cron", queue="default")
async def wf_research_cron(payload: CronKick, ctx: JobContext[CronKick]) -> dict[str, str]:
    """The cron schedule's TARGET: the fire arm enqueues THIS actor (the
    schedule's every-minute slot), the body kicks the workflow with the
    SLOT as the run key (G3: the two dedup regimes compose — a
    double-fired slot is ONE run). The body resolves the flow's compiled
    graph from the D1 registry (the process imported the march app —
    the capability stamp) and creates the run on its OWN pool (the body
    is DB-local, the tier's hermeticity)."""
    import asyncpg

    from taskq.settings import WorkerSettings
    from taskq.workflows._worker_execution import get_compiled_workflow
    from taskq.workflows.api._runner import FlowRunner

    settings = WorkerSettings.load()
    pool = await asyncpg.create_pool(str(settings.pg_dsn), min_size=1, max_size=1)
    try:
        compiled = get_compiled_workflow("deep_research")
        runner = FlowRunner(compiled, pool, settings.schema_name)
        flow_id = (await runner.create_flow(run_key=f"deep-research:{payload.slot}")).flow_id
        return {"flow_id": str(flow_id)}
    finally:
        await pool.close()


#: The bridge actor's REGISTRY face (the _wf_entry's import merges it):
#: the cron fire resolves the actor by NAME from the boot's registry.
MARCH_ACTORS: dict[str, object] = {"wf_research_cron": wf_research_cron}
