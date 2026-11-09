"""The workflow marches' shared app module (the system tier).

one module, imported by both the test process and the tier's worker
subprocesses (``_wf_entry`` imports it so the worker's boot stamps
``workflow_execution: true`` and projects the (actor, queue) cohorts).
Every domain here is ABSTRACT (doc/source/review — the spec doc's
abstraction contract; zero case-study content).

The flows:

* ``deep_research`` — the DEEP-RESEARCH MARCH's flow: the map with an
  armed transient and an armed permanent failure under a ``collect``
  edge (the partial report), the budget-capped loop whose iteration
  body HOLDS on a typed gate (the multi-epoch multi-HITL), and the
  publish step whose typed wait can TIME out (the timeout face — the
  body pivots to the degraded report, never wedges).
* ``triage_chain`` — the streaming router router lived on the tier: a ``chain_source``
  paged body (the streaming emit) whose per-record chains route on the
  body's own typed outcomes (the conditional chains).
* ``doc_ingest`` — the DOC-INGEST MARCH's flow: the map + the fan-in
  barrier join (fail_closed) + the ``maybe`` edge (the collect policy)
  + the declared read-side aggregate (the progress aggregation).
* ``reclaim_target`` — the worker-deploy cell's flow: one long node a
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

import asyncio
import enum
import json
from datetime import timedelta
from typing import Any, Final

from pydantic import BaseModel

from taskq import JobContext, RetryPolicy, actor
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.workflows import (
    Done,
    ExecutionPolicy,
    Expired,
    Promise,
    Refine,
    RetrySpec,
    StepContext,
    WorkflowApp,
    WorkflowRetryOverride,
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
from tests.system_e2e import (
    _p5_cancel_app as _p5_cancel_app_module,  # registers the effect fleet's definitions
)

#: The march pods' queues: both the vanilla actors' system_e2e and the
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

#: The armed failures: the map's body fails transiently on this source
#: once (the ladder heals it — the retried child succeeds alone), and
#: permanently on this one (the collect edge fans the failure in — the
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
    the error message). the ROUND-TRIP HONESTY: the join's items arrive
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
    # the failed children ride the map JOIN row's metadata.failures (the typed
    # envelope), never the packed results — the body reads its parent (the
    # join) through the edge ledger (the edge ledger is the wiring's own
    # record; the triage node's parent is the map join).
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
            # the children share one step key ('<parent>.item'); the item's own
            # identity is the map INDEX — the march's source order decodes it
            # back to the source id (the report names the source, never the
            # row).
            idx = item.get("map_index")
            failed.append(
                str(_SOURCES[idx])
                if isinstance(idx, int) and idx < len(_SOURCES)
                else str(item["node_key"])
            )
    await ctx.progress(60, "triaged", {"fetched": len(ok), "failed": len(failed)})
    return {"fetched": ok, "failed": failed}


async def review_iteration(ctx: StepContext, carry: int) -> Done[str] | Refine[int]:
    # every iteration mints a new hold epoch (the multi-hitl: N iterations = N
    # distinct holds, each addressed by id). An approve ends the loop; a refine
    # advances the carry (the carry advances exactly once per iteration).
    decision = await ctx.wait_signal(
        (ReviewDecision,),
        timeout_s=120.0,
        reason="the march's editorial review",
    )
    match decision:
        case ReviewDecision() as review:
            if review.verdict == "approve":
                return Done(review.note)
            return Refine(carry + 1)
        case Expired():
            return Done("the review expired — the march ships what it has")


async def escalation_body(ctx: Any, carry: int) -> str:
    return f"escalated-at-iteration-{carry}"


#: The march's publish-approval wait. 900s (the load-sensitive lane's
#: per-test ceiling) / 3 = 300s — a hosted 2-core runner's worst beats
#: (the sweep's abandonment tick under fleet starvation) must land inside
#: it with headroom, while the march's whole spine stays under the
#: ceiling. A constant, never a wall-clock scale.
PUBLISH_APPROVAL_TIMEOUT_S: Final[float] = 300.0


async def publish_body(
    ctx: StepContext, triage: dict[str, object], review_note: str
) -> ResearchReport:
    # the typed wait is bounded; the expiry sweep's abandonment returns the
    # Expired member here — the body matches it and pivots (the degraded
    # report), never wedges, never re-holds in a loop (the re-wait dragon's
    # fence).
    approved = True
    note = review_note
    degraded = False
    outcome = await ctx.wait_signal(
        (PublishApproval,),
        timeout_s=PUBLISH_APPROVAL_TIMEOUT_S,
        reason="the march's publish approval",
    )
    match outcome:
        case PublishApproval() as approval:
            approved = bool(approval.approve)
        case Expired():
            degraded = True
            note = (
                f"{note} (degraded: the publish approval timed out)"
                if note
                else ("degraded: the publish approval timed out")
            )
    # An explicit disapproval is also the degraded pivot (the operator
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
                timeout_s=PUBLISH_APPROVAL_TIMEOUT_S,
            ),
        ),
    )
    return build(published)


@research_app.workflow("deep_research_cron")
def deep_research_cron() -> Promise[object]:
    """The CRON-FIRED variant: the same graph; the run key IS the cron
    slot (the G3 composition — the fire arm passes the slot as the run
    key, a double-fired slot is one run)."""
    return deep_research()


# ── the streaming router router lived on the tier (the streaming emit source) ─────────


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
    running — each yield is one emit tx (the children + edges + the
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
    """The map's DECLARED READ-SIDE aggregate (the progress reporting decision c): a pure fn
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
    # The map join packs the children as-is (a map over list items is a
    # collect of lists — the join's contract): the downstream reads the
    # enriched records, never bare ids.
    return [e.doc_id for e in enriched]


async def ingest_publish_body(ctx: Any, docs: list[str], labels: list[str]) -> Verdict:
    # the barrier gather's parents each returned a list — the gather flattens
    # one level, so `docs` is summaries+entities flattened (strs); the maybe
    # gather's single parent passes through the same shape (labels: strs).
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
    once across a requeue — the iteration body appends nothing to the
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


async def drain_hold_body(ctx: StepContext, params: Kickoff) -> str:
    decision = await ctx.wait_signal(
        (ReviewDecision,), timeout_s=60.0, reason="the drain cell's hold"
    )
    match decision:
        case ReviewDecision() as review:
            return review.verdict
        case Expired():
            return "the drain cell's hold expired"


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


# ── execution-policy fleet controls (generic records only) ──────────────


class PolicyOutcome(enum.Enum):
    DONE = "done"


async def policy_reserved_body(ctx: Any, item: dict[str, object]) -> PolicyOutcome:
    async def body_window() -> dict[str, object]:
        await asyncio.sleep(0.2)
        return {"record_id": item["record_id"]}

    await ctx.substep("policy-reserved-window", body_window)
    return PolicyOutcome.DONE


policy_app = WorkflowApp()


@policy_app.actor(name="wf-policy-capped-handle", max_concurrent=1)
async def policy_capped_body(ctx: Any, item: dict[str, object]) -> PolicyOutcome:
    return await policy_reserved_body(ctx, item)


POLICY_RESERVED_CHAIN = Chain(
    name="execution-policy-reserved-chain",
    start="work",
    steps={
        "work": Step(
            body=policy_capped_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(
                actor="wf-policy-reserved",
                reservations=(
                    ConcurrencyReservation(
                        "wf-policy-reservation",
                        slots=1,
                        lease=timedelta(seconds=30),
                    ),
                ),
                priority=4,
                start_to_close=timedelta(seconds=5),
                retry=RetrySpec(policy=RetryPolicy(max_attempts=1)),
            ),
        )
    },
)


async def policy_reserved_source(ctx: Any) -> None:
    await ctx.emit_batch(
        [
            chain_start(
                POLICY_RESERVED_CHAIN,
                {"record_id": index},
                map_index=index,
                trace_id=f"policy-reserved-{index}",
            )
            for index in range(8)
        ],
        cursor={"page": 0},
    )


async def policy_priority_body(ctx: Any, item: dict[str, object]) -> PolicyOutcome:
    """The priority chain's own workload: one held claim for a long, bounded
    beat — the late-priority phase's participation gate (the first worker
    stays busy on the high row until the second worker has joined, so both
    real processes deterministically share the backlog)."""
    await asyncio.sleep(10.0)
    return PolicyOutcome.DONE


POLICY_PRIORITY_CHAIN = Chain(
    name="execution-policy-priority-chain",
    start="work",
    steps={
        "work": Step(
            body=policy_priority_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(actor="wf-policy-priority", priority=9),
        )
    },
)


async def policy_priority_source(ctx: Any) -> None:
    await ctx.emit_batch(
        [
            chain_start(
                POLICY_PRIORITY_CHAIN,
                {"record_id": index},
                map_index=index,
                trace_id=f"policy-priority-{index}",
            )
            for index in range(1)
        ],
        cursor={"page": 0},
    )


@policy_app.workflow("policy_reserved_fleet")
def policy_reserved_fleet() -> Promise[object]:
    return build(chain_source(POLICY_RESERVED_CHAIN, policy_reserved_source, key="source"))


# ── the compact two-cohort control: one saturated cohort, one free cohort ──


async def policy_cohort_a_body(ctx: StepContext, item: dict[str, object]) -> PolicyOutcome:
    """The saturated cohort's workload: one strict slot, held for a long,
    bounded beat — longer than the fleet's spawn-verification window, so the
    other cohort's rows claim, complete, and leave durable attempts inside
    the observed window, and the second A row's refusal is observable
    on the test's own connection."""
    await asyncio.sleep(20.0)
    return PolicyOutcome.DONE


async def policy_cohort_b_body(ctx: StepContext, item: dict[str, object]) -> PolicyOutcome:
    """The free cohort's workload: fast bodies whose durable attempts land
    while the other cohort's slot is held."""
    await asyncio.sleep(0.1)
    return PolicyOutcome.DONE


POLICY_COHORT_A_CHAIN = Chain(
    name="policy-cohort-a-chain",
    start="awork",
    steps={
        "awork": Step(
            body=policy_cohort_a_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(
                actor="wf-policy-cap-a",
                queue="default",
                priority=5,
                max_concurrent=1,
                # The operator owns this cohort's stored capacity: the boot's
                # diagnostic fires on the declared/stored mismatch and the
                # stored value is never overwritten.
                capacity_owner="operator",
                reservations=(
                    ConcurrencyReservation(
                        "policy-cohort-a-slot",
                        slots=1,
                        lease=timedelta(seconds=30),
                    ),
                ),
            ),
        )
    },
)

POLICY_COHORT_B_CHAIN = Chain(
    name="policy-cohort-b-chain",
    start="bwork",
    steps={
        "bwork": Step(
            body=policy_cohort_b_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(
                actor="wf-policy-cap-b",
                queue="default",
                priority=3,
                max_concurrent=1,
                # The code owns this cohort's stored capacity: a matching
                # stored row fires no diagnostic; a drifted one deploys by
                # boot.
                capacity_owner="code",
            ),
        )
    },
)


async def policy_cohort_a_source(ctx: Any) -> None:
    await ctx.emit_batch(
        [
            chain_start(
                POLICY_COHORT_A_CHAIN,
                {"record_id": index},
                map_index=index,
                trace_id=f"cap-a-{index}",
            )
            for index in range(2)
        ],
        cursor={"page": 0},
    )


async def policy_cohort_b_source(ctx: Any) -> None:
    await ctx.emit_batch(
        [
            chain_start(
                POLICY_COHORT_B_CHAIN,
                {"record_id": index},
                map_index=index,
                trace_id=f"cap-b-{index}",
            )
            for index in range(2)
        ],
        cursor={"page": 0},
    )


@policy_app.workflow("policy_two_cohort_fleet")
def policy_two_cohort_fleet() -> Promise[object]:
    return build(
        chain_source(POLICY_COHORT_A_CHAIN, policy_cohort_a_source, key="source-a"),
        chain_source(POLICY_COHORT_B_CHAIN, policy_cohort_b_source, key="source-b"),
    )


@policy_app.workflow("policy_priority_fleet")
def policy_priority_fleet() -> Promise[object]:
    # The source rides a dedicated control actor/queue cohort (one queue per
    # actor — the estate's own law): the deterministic priority phase boots
    # ONLY this source's worker first, observes the emitted priority-9 row's
    # commit on the test's own connection, and starts the default-queue
    # workers only after that visibility is established.
    return build(
        chain_source(
            POLICY_PRIORITY_CHAIN,
            policy_priority_source,
            key="source",
            actor="wf-policy-control",
            queue="policy-control",
        )
    )


class PolicyBacklogPayload(BaseModel):
    record_id: str = "backlog"


@actor(name="wf_policy_backlog", queue="default")
async def wf_policy_backlog(
    payload: PolicyBacklogPayload, ctx: JobContext[PolicyBacklogPayload]
) -> dict[str, object]:
    # Each backlog record holds its claim briefly: the phase's two worker
    # processes overlap on the backlog, and a wrong-first-claim ordering
    # stays observable for the whole wrong sequence.
    await asyncio.sleep(1.0)
    return {"accepted": True}


# ── rolling retry curves, advertised non-consuming delay, the engine's
# deadline, and the code/operator capacity-owner cohorts (generic records) ──


class PolicyRollTransientError(RuntimeError):
    """The mapped transient the non-consuming record's classifier matches."""


async def policy_rolling_body(ctx: StepContext, item: dict[str, object]) -> PolicyOutcome:
    """Three real failed attempts, then the record's success (curve-visible)."""
    if ctx.attempt < 4:
        raise PolicyRollTransientError("rolling curve failure")
    return PolicyOutcome.DONE


async def policy_non_consuming_body(ctx: StepContext, item: dict[str, object]) -> PolicyOutcome:
    """Always raises; the declared map classifies it into advertised free waits."""
    raise PolicyRollTransientError("advertised wait")


async def policy_deadline_body(ctx: StepContext, item: dict[str, object]) -> PolicyOutcome:
    """Never raises and never finishes inside its declared start_to_close."""
    await asyncio.sleep(2.5)
    return PolicyOutcome.DONE  # pragma: no cover - the deadline fires first


async def policy_cap_body(ctx: StepContext, item: dict[str, object]) -> PolicyOutcome:
    return PolicyOutcome.DONE


def _roll_chain(name: str, step_key: str, backoff: str) -> Chain:
    """One record-leaf chain with the declared curve (base 0.05s, cap 1s, no jitter)."""
    retry_policy = RetryPolicy(
        max_attempts=4,
        base=timedelta(seconds=0.05),
        cap=timedelta(seconds=1.0),
        jitter=0,
        backoff=backoff,  # type: ignore[arg-type]  # the declared literal is one of the three validated curve names
    )
    return Chain(
        name=name,
        start=step_key,
        steps={
            step_key: Step(
                body=policy_rolling_body,
                outcomes=PolicyOutcome,
                policy=ExecutionPolicy(
                    actor="wf-policy-roll",
                    retry=RetrySpec(policy=retry_policy),
                ),
            )
        },
    )


POLICY_EXP_CHAIN = _roll_chain("policy-exp-chain", "exp", "exponential")
POLICY_LIN_CHAIN = _roll_chain("policy-lin-chain", "lin", "linear")
POLICY_FIX_CHAIN = _roll_chain("policy-fix-chain", "fix", "fixed")

POLICY_NON_CONSUMING_CHAIN = Chain(
    name="policy-non-consuming-chain",
    start="work",
    steps={
        "work": Step(
            body=policy_non_consuming_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(
                actor="wf-policy-roll",
                retry=RetrySpec(
                    policy=RetryPolicy(max_attempts=1, jitter=0),
                    exceptions={
                        PolicyRollTransientError: WorkflowRetryOverride(
                            delay=timedelta(seconds=0.05),
                            consume_budget=False,
                        )
                    },
                    non_consuming_limit=2,
                ),
            ),
        )
    },
)

POLICY_DEADLINE_CHAIN = Chain(
    name="policy-deadline-chain",
    start="work",
    steps={
        "work": Step(
            body=policy_deadline_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(
                actor="wf-policy-roll",
                start_to_close=timedelta(seconds=0.4),
                retry=RetrySpec(policy=RetryPolicy(max_attempts=1, jitter=0)),
            ),
        )
    },
)

# The two capacity-owner cohorts: the code's literal owns one actor's
# max_concurrent (the boot's code-owned upsert deploys it); the operator's
# stored row owns the other's.
POLICY_CODE_CAP_CHAIN = Chain(
    name="policy-code-cap-chain",
    start="work",
    steps={
        "work": Step(
            body=policy_cap_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(
                actor="wf-policy-code-cap",
                queue="default",
                max_concurrent=1,
                capacity_owner="code",
            ),
        )
    },
)

POLICY_OPERATOR_CAP_CHAIN = Chain(
    name="policy-operator-cap-chain",
    start="work",
    steps={
        "work": Step(
            body=policy_cap_body,
            outcomes=PolicyOutcome,
            policy=ExecutionPolicy(
                actor="wf-policy-operator-cap",
                queue="default",
                max_concurrent=1,
                capacity_owner="operator",
            ),
        )
    },
)


def _single_record_source(chain: Chain, trace: str) -> Any:
    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(chain, {"record_id": 0}, map_index=0, trace_id=trace)],
            cursor={"page": 0},
        )

    return source


async def policy_exp_source(ctx: StepContext) -> None:
    await _single_record_source(POLICY_EXP_CHAIN, "roll-exp")(ctx)  # type: ignore[operator]


async def policy_lin_source(ctx: StepContext) -> None:
    await _single_record_source(POLICY_LIN_CHAIN, "roll-lin")(ctx)  # type: ignore[operator]


async def policy_fix_source(ctx: StepContext) -> None:
    await _single_record_source(POLICY_FIX_CHAIN, "roll-fix")(ctx)  # type: ignore[operator]


async def policy_non_consuming_source(ctx: StepContext) -> None:
    await _single_record_source(POLICY_NON_CONSUMING_CHAIN, "roll-nc")(ctx)  # type: ignore[operator]


async def policy_deadline_source(ctx: StepContext) -> None:
    await _single_record_source(POLICY_DEADLINE_CHAIN, "roll-deadline")(ctx)  # type: ignore[operator]


async def policy_code_cap_source(ctx: StepContext) -> None:
    await _single_record_source(POLICY_CODE_CAP_CHAIN, "roll-code-cap")(ctx)  # type: ignore[operator]


async def policy_operator_cap_source(ctx: StepContext) -> None:
    await _single_record_source(POLICY_OPERATOR_CAP_CHAIN, "roll-operator-cap")(ctx)  # type: ignore[operator]


@policy_app.workflow("policy_exp_fleet")
def policy_exp_fleet() -> Promise[object]:
    return build(chain_source(POLICY_EXP_CHAIN, policy_exp_source, key="source"))


@policy_app.workflow("policy_lin_fleet")
def policy_lin_fleet() -> Promise[object]:
    return build(chain_source(POLICY_LIN_CHAIN, policy_lin_source, key="source"))


@policy_app.workflow("policy_fix_fleet")
def policy_fix_fleet() -> Promise[object]:
    return build(chain_source(POLICY_FIX_CHAIN, policy_fix_source, key="source"))


@policy_app.workflow("policy_non_consuming_fleet")
def policy_non_consuming_fleet() -> Promise[object]:
    return build(
        chain_source(POLICY_NON_CONSUMING_CHAIN, policy_non_consuming_source, key="source")
    )


@policy_app.workflow("policy_deadline_fleet")
def policy_deadline_fleet() -> Promise[object]:
    return build(chain_source(POLICY_DEADLINE_CHAIN, policy_deadline_source, key="source"))


@policy_app.workflow("policy_code_cap_fleet")
def policy_code_cap_fleet() -> Promise[object]:
    return build(chain_source(POLICY_CODE_CAP_CHAIN, policy_code_cap_source, key="source"))


@policy_app.workflow("policy_operator_cap_fleet")
def policy_operator_cap_fleet() -> Promise[object]:
    return build(chain_source(POLICY_OPERATOR_CAP_CHAIN, policy_operator_cap_source, key="source"))


#: The march flows' compiled definitions, by name (the test side's
#: create-run surface resolves here — one registry, one compile).
MARCH_FLOWS: dict[str, Any] = {
    "policy_reserved_fleet": policy_app.get("policy_reserved_fleet"),
    "policy_priority_fleet": policy_app.get("policy_priority_fleet"),
    "policy_exp_fleet": policy_app.get("policy_exp_fleet"),
    "policy_lin_fleet": policy_app.get("policy_lin_fleet"),
    "policy_fix_fleet": policy_app.get("policy_fix_fleet"),
    "policy_non_consuming_fleet": policy_app.get("policy_non_consuming_fleet"),
    "policy_deadline_fleet": policy_app.get("policy_deadline_fleet"),
    "policy_two_cohort_fleet": policy_app.get("policy_two_cohort_fleet"),
    "policy_code_cap_fleet": policy_app.get("policy_code_cap_fleet"),
    "policy_operator_cap_fleet": policy_app.get("policy_operator_cap_fleet"),
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
    double-fired slot is one run). The body resolves the flow's compiled
    graph from the D1 registry (the process imported the march app —
    the capability stamp) and creates the run on its own pool (the body
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


#: The bridge actor's registry face (the _wf_entry's import merges it):
#: the cron fire resolves the actor by name from the boot's registry.
MARCH_ACTORS: dict[str, object] = {
    "wf_research_cron": wf_research_cron,
    "wf_policy_backlog": wf_policy_backlog,
}

#: The imported effect-app module object (the registration's witness — the
#: import above must never be dropped as unused).
P5_CANCEL_APP_MODULE = _p5_cancel_app_module
