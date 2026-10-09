# The doc-ingest pipeline — workflows in one graph

A document-ingestion pipeline: documents arrive, get enriched, pass an
editorial review, and publish — with the failures REPORTED, never
swallowed. This page is the minimal copy-paste form; the
[demo app](https://github.com/#/examples) runs the SAME graph live with
the admin's run explorer attached.

## Prerequisites (the env vars the fence reads)

Install with the `fastapi` extra (the fence's resolve writes its audit
row through the admin surface — core alone stops at
`ModuleNotFoundError: fastapi`):

```bash
uv pip install "taskq-py[fastapi]"     # or: uv add "taskq-py[fastapi]"
```

The fence's `main()` drives a REAL Postgres. Before copy-pasting, set the
variables it reads — this is the whole list (the repo's
[.env.example](https://github.com/#/.env.example) carries the full
reference; the same names every TaskQ surface loads through
`TaskQSettings`):

```bash
export TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq
# REQUIRED — the fence fails fast with a named error without it.
# Any reachable Postgres 18 works; `docker run -d -p 5432:5432 -e
# POSTGRES_USER=taskq -e POSTGRES_PASSWORD=taskq -e POSTGRES_DB=taskq postgres:18.6`
# is the two-minute one.

export TASKQ_SCHEMA_NAME=fence_first_hour
# OPTIONAL — defaults to "taskq" (the .env.example's documented default).
# A FRESH schema is the disposable first hour: the fence migrates it on
# start and you can drop it afterwards.

export TASKQ_QUEUES=enrich,cpu,io,classify
# OPTIONAL but honest — the fence's four placement queues (shape 1). The
# in-process driver executes every body regardless, but declaring the
# queues keeps the graph's validate() at ZERO findings — the zero-warning
# budget counts WARNINGs too (W2-unknown-queue: a queue no worker may
# listen on).
```

Without `TASKQ_PG_DSN` the fence stops at the named error, not a bare
`KeyError` — every variable the code reads is documented HERE, at the
top, because the first hour should never start with an interpreter
traceback.

The example demonstrates the nine load-bearing shapes of the flow API:

1. fork-of-N enrichment with heterogeneous placement (three queues),
2. per-item fork + collect — the map's 13 items (12 readable + 1 ghost)
   fan in as ONE collect, the partial result carried,
3. the HITL approval hold with a typed payload (`ReviewDecision`),
4. the retry ladder + partial-retry isolation (a failed item re-runs
    alone; `max_attempts` named on the map),
5. the barrier join — the edges' policies are the worked duality
   (`fail_closed` over the required path, `maybe` over the
   classifiable one),
6. the budget-capped loop with the held approval (the budget PAUSES
   while a human reviews — the hold counts for nothing on wake),
7. the union routing shape — `Summary | Unreadable` consumed with
   `match` + `assert_never` (per-node exhaustiveness; the portable
   idiom BOTH checkers guard a missing arm through),
8. the typed terminal payload — the run ends in a `PublishReport`
   DERIVED from the run's collect (the dead letters NAMED — the ghost
   rides the report, a hardcoded `[]` is a report that lies),
9. the cron-fired nightly refresh — the cron-slot key IS the run key:
   the same slot twice produces ONE run.

```python
import asyncio
import os
from typing import assert_never

from pydantic import BaseModel

from taskq.workflows import (
    Done,
    FlowRunner,
    HitlClient,
    Refine,
    StepContext,
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
    while the work was wrong. The report DERIVES from the run's own
    collect (the envelope is the truth) — the dead letters NAMED, the
    failed chains the batch's residual. A field hardcoded `[]` here
    would be a report that lies by construction."""

    published: list[str]
    dead_lettered: list[str]
    failed: list[str]
    review_note: str = ""


# ── the bodies (one queue each — shape 1's heterogeneous placement) ─────

_DOC_SOURCE: dict[str, str] = {f"doc-{i:03}": f"the text of document {i}" for i in range(12)}

#: The batch's GHOST: a doc id the source does NOT have — the Unreadable
#: arm's LIVE demonstration. The route dead-letters it, and the terminal
#: report must NAME it (`dead_lettered == ["doc-999"]`); a report that
#: could not say this is decoration.
_BATCH = sorted(_DOC_SOURCE) + ["doc-999"]


async def ingest_body(ctx: StepContext, params: IngestBatch | None = None) -> list[str]:
    """The run's OWN input is the truth (cut #7): `create_flow(input=…)`
    carries the batch and the body reads `ctx.input`. The wiring's
    declared batch below is the DEFAULT (a bare `create_flow()` still
    runs the demonstration corpus) — the caller's input is never
    ACCEPTED-AND-IGNORED: hand this flow a different batch and it
    enriches YOUR documents."""
    raw = ctx.input if ctx.input is not None else (params.model_dump() if params else None)
    return sorted(IngestBatch.model_validate(raw).doc_ids) if raw is not None else []


async def enrich_item(ctx: StepContext, doc_id: str) -> Summary | Unreadable:
    """Per-document enrichment — shape 2's map item. The UNION return is
    shape 7's type story: a document is readable, or it is NOT (named).
    `doc-999` (the batch's ghost) exercises the Unreadable arm LIVE."""
    text = _DOC_SOURCE.get(doc_id)
    if text is None:
        return Unreadable(doc_id=doc_id, reason="missing from the source")
    return Summary(doc_id=doc_id, text=text)


async def route_body(ctx: StepContext, enriched: list[Summary | Unreadable]) -> list[str]:
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


def _summary_doc(item: Summary | Unreadable) -> str | None:
    """The READABLE arm's key, or None — the enrichers' own walk (the
    exhaustiveness idiom is PER-NODE: every consumer of the union
    matches it; a node that forgets an arm is a checker error)."""
    match item:
        case Summary():
            return item.doc_id
        case Unreadable():
            return None  # no text was readable — the ROUTER's verdict names it
        case _ as it:
            assert_never(it)


async def summarize_body(ctx: StepContext, enriched: list[Summary | Unreadable]) -> list[str]:
    """The summaries' keys — the readable arm ONLY (a summary exists
    only where the text was readable)."""
    return [k for it in enriched if (k := _summary_doc(it)) is not None]


async def extract_entities_body(
    ctx: StepContext, enriched: list[Summary | Unreadable]
) -> list[str]:
    """The entity pass — the second REQUIRED consumer (its own queue):
    it too runs where the text was readable; the walk is its own."""
    return [k for it in enriched if (k := _summary_doc(it)) is not None]


async def classify_body(ctx: StepContext, enriched: list[Summary | Unreadable]) -> list[str]:
    """The classifier labels EVERY document — both arms surface here
    (the MAYBE path's shape: the Unreadable id rides the labels, and
    the publish's AND-join reads them)."""
    return [_labeled_doc(it) for it in enriched]


def _labeled_doc(item: Summary | Unreadable) -> str:
    match item:
        case Summary():
            return item.doc_id
        case Unreadable():
            return item.doc_id  # the MAYBE path carries it as surfaced
        case _ as it:
            assert_never(it)


async def review_iteration(ctx: StepContext, carry: int) -> Done[str] | Refine[str]:
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
    ctx: StepContext,
    review_note: str,
    ready: list[str],
    labels: list[str],
    enriched: list[Summary | Unreadable],
) -> PublishReport:
    """Shape 8 — the report DERIVES, from the run's own rows (the
    envelope is the truth, never a hardcoded `[]`):
    * `dead_lettered` — the collect's Unreadable arm, NAMED (the ghost's
      doc id is in here, or the run did not happen as wired);
    * `failed` — the batch's residual: a doc that neither published nor
      dead-lettered is a chain that produced nothing (named, per-doc);
    * `published` — the barrier's fan-in DEDUPED to identities. The
      gather fans in BOTH enrichers (each covered the corpus, so the
      flatten carries every doc twice) — a document publishes ONCE, and
      the intersection with the classifier's labels is the AND-join's
      publish rule."""
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
        published=published, dead_lettered=dead_lettered, failed=failed, review_note=review_note
    )


# ── the wiring (ONE flow — the join-body cure makes that true) ──────────

app = WorkflowApp()

REVIEW_GATE = GateDecl(
    name="ReviewDecision", payload_models=(ReviewDecision,), timeout_s=7 * 24 * 3600.0
)


@app.workflow("doc_ingest")
def doc_ingest() -> object:
    ingested = step(ingest_body, IngestBatch(doc_ids=_BATCH), key="ingest")
    # Shape 4 — the ITEM LADDER's knob, named where the ladder lives:
    # `max_attempts` on the map (each child re-runs ALONE up to this
    # bound; the siblings and the succeeded items never re-run).
    enriched = map_source(ingested, enrich_item, queue="enrich", max_attempts=3)

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
        initial=0,
        max_iterations=3,
        budget_s=3600.0,
        on_exhausted="escalate",
        gates=(REVIEW_GATE,),  # the typed door's compile visibility
    )
    # The publish consumes the collect TOO (its fourth parent): the
    # report derives from the map join's items — the dead letters named,
    # the failed chains the batch's residual (the envelope is the truth).
    published = step(publish_body, review, barrier, maybe_labels, enriched, key="publish")
    return build(published)


# Shape 9 — the CRON-FIRED NIGHTLY REFRESH (~15 lines): the schedule's
# cron-slot key IS the run key. The same slot twice produces ONE run (the
# run-key arbiter's claim); the stale-key silent-replay fossil is
# prevented by construction — the second call returns the FIRST run's id.


async def run_nightly_refresh(schema: str, pool: object, slot: str) -> str:
    """The nightly cron's arm: key = <flow>:<slot-timestamp> — run-level
    idempotency demonstrated (the same slot twice → one run). The
    caller's input IS the corpus (the body reads `ctx.input`)."""
    runner = FlowRunner(app.get("doc_ingest"), pool, schema)
    claim = await runner.create_flow(
        input=IngestBatch(doc_ids=_BATCH),
        run_key=f"doc_ingest:nightly:{slot}",
    )
    run_id = claim.flow_id
    # The FIRST wall: drive to the review HOLD (a drive to `terminal`
    # around a hold would spin its tick loop on a quiescent run — the
    # held answer IS the quiescent stop). A `max_ticks` return is a
    # DEFECT with a name — surfaced, never a silent stop. The TYPED
    # CLAIM: create_flow returns the RunClaim (the claim's `kind` states
    # created/existing — the 202-vs-409 distinction), and the run's id
    # rides `claim.flow_id`.
    state = await runner.drive(run_id, until="held")
    if state == "max_ticks":
        raise RuntimeError(
            "the drive exhausted max_ticks on a quiescent run — a bounded "
            "stop is a defect (the run wedged), never a wait"
        )
    return str(run_id)


async def main() -> None:
    import asyncpg

    from taskq.migrate import apply_pending

    # The prerequisites (the block at the top of this page): the DSN is
    # REQUIRED — the named error, never a bare KeyError traceback; the
    # schema is OPTIONAL — the same "taskq" default .env.example
    # documents.
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    dsn = os.environ.get("TASKQ_PG_DSN", "")
    if not dsn:
        raise SystemExit(
            "TASKQ_PG_DSN is not set — the fence drives a real Postgres "
            "(see the prerequisites block at the top of this page)"
        )
    conn = await asyncpg.connect(dsn)
    await apply_pending(conn, schema=schema)
    await conn.close()

    pool = await asyncpg.create_pool(dsn)
    flow_id = await run_nightly_refresh(schema, pool, slot="2026-10-08T00:00Z")

    # THE FIRST WALL: the drive STOPPED AT THE HOLD (`until="held"` is
    # the quiescent answer). The narrative stops HERE — the run is held;
    # the resume line follows the resolve.
    client = HitlClient(pool, schema=schema)
    holds = await client.list(flow_id)
    assert holds, (
        "the run holds nothing — was this cron slot already run? The "
        "run-key's claim returned the FIRST run (terminal): use a fresh "
        "schema (the prerequisites' disposable first hour) or a new slot"
    )
    (hold,) = holds
    print("the run is now held — resolve the hold below to resume")
    print("hold:", hold.hold_id, "-", hold.signal_name)
    result = await client.resolve(hold.hold_id, {"verdict": "approve", "note": "ship it"})
    print("resolve:", result.status)

    resume_runner = FlowRunner(app.get("doc_ingest"), pool, schema)
    outcome = await resume_runner.drive(flow_id)  # the resume → the terminal
    if outcome != "terminal":
        raise RuntimeError(
            f"the resume ended {outcome!r} — a 'max_ticks' stop is a "
            "defect with a name, never a silent stop"
        )

    # THE TERMINAL REPORT, READ OFF THE RUN (the envelope is the truth):
    # the ghost is NAMED, every doc publishes ONCE.
    report = PublishReport.model_validate(await resume_runner.result(flow_id))
    print("report:", report.model_dump())
    assert report.dead_lettered == ["doc-999"], report
    assert len(report.published) == len(set(report.published)), report

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
| 4 | retry ladder, isolated | `max_attempts=3` on `map_source` (the item's ladder — a child re-runs alone) |
| 5 | the worked duality | `gather([summaries, entities], on_failure="fail_closed")` vs `gather([labels], on_failure="maybe")` |
| 6 | budget-capped loop, held budget | `loop("review", …, max_iterations=3, budget_s=3600.0)` |
| 7 | union routing | `match item:` + `case _ as it: assert_never(it)` in `route_body` |
| 8 | typed terminal verdict | `PublishReport` — derived from the run's collect (the ghost named) |
| 9 | cron-slot run key | `run_nightly_refresh`'s `run_key=f"doc_ingest:nightly:{slot}"` |

**Run it live**: the demo app wires this same graph behind HTTP with the
admin's run explorer — see the demo's README. **The verify loop**: the
example's fence executes in CI (`make test-docs-examples`), the fast tier
runs its `build()` + `validate()` without containers, and the compiled
Mermaid is byte-pinned.

---

## The four demonstrations (the capabilities, live)

Each leg is a RUNNABLE path in the demo (`tests/test_wf_demo_legs.py` —
the evidence-matrix's demo work order; every run's output is captured
verbatim under `.measurements/demo-legs/`):

| # | Capability | The runnable path | The captured evidence |
|---|-----------|-------------------|-----------------------|
| 1 | **Cancellation mid-flight** | trigger → the run holds mid-flight → `taskq flows cancel <run_id> --reason ...` → the explorer shows the story: the root `cancelled`, the nodes' named states (the held signal resolved, the downstream cancelled), the derived status `cancelled`, the audit ROW (the principal + the reason) | `.measurements/demo-legs/leg1-cancellation.json` |
| 2 | **Resumability (the kill-and-resume)** | a PRODUCTION worker (`taskq worker --actors examples.workflows:ACTORS`) claims a node mid-run → **SIGKILL** → a fresh worker's lease-expiry + reclaim re-runs the killed node → **the run COMPLETES** (the ledger records the reclaim; nothing is lost — the killed attempt's rows are the record) | `.measurements/demo-legs/leg2-kill-resume.json` |
| 3 | **Observability (the live scrape)** | a real run + the maintenance leader's sampler → `taskq.wf_progress_nodes_total{workflow,state}` rendered at the metrics surface → the /metrics ENDPOINT's response captured verbatim | `.measurements/demo-legs/leg3-wf-gauge-scrape.prom` |
| 4 | **The conditional router (the T20 chain)** | the demo's `doc_screen_router` run: the chain's SCREEN step routes each record — READABLE → `index`, UNREADABLE → `dead_letter`; the totals are the FENCE (a non-total route refused at declaration; a body outcome with no arm = the loud `RouterNotTotal`) | `.measurements/demo-legs/leg4-router.json` |

**The two doors' contracts differ — stated AT the example (the pick:
document, don't shim):** the `step(...)` door COERCES — the example's
`IngestBatch(doc_ids=...)` (a pydantic model) re-validates at the
boundary (the dict round-trips as the model; the body sees the type it
declared). The `ctx.emit_batch(...)` door REFUSES pydantic models
LOUDLY — its payloads are DICTS/BYTES (the emit path's zero-copy
discipline: the emission rides the wire's own vocabulary, never a
model-shaped second encoding; a wrong shape is the raised refusal, the
authoring error is the body's problem). The pattern this example
teaches: pass `model.model_dump()` at emit; hand the MODEL to `step`
(the door coerces). (The small coercion shim at emit was considered and
DECLINED: hiding the wire's vocabulary behind a shim moves the
zero-copy discipline from the contract to the accident; the ergonomics
cure is the T20 lane's ledger.)

The demo's worker subscription: the compose stack's workers SUBSCRIBE
to the workflow cohorts' queues — per worker, per the placement
(`worker-1` carries `demo-enrich,demo-cpu,demo-publish`; `worker-2`
carries `demo-io,demo-classify,demo-screen`; each cohort lands on
exactly ONE worker of the stack, so a queue's node runs on ITS worker —
the pin lives in `tests/test_wf_demo_legs.py`). A worker subscribes
with `TASKQ_QUEUES=demo-screen,demo-cpu,...` — the boot projection
declares the cohorts in `actor_config` (the worker's import of
`examples.workflows` populates the D1 registry the bodies resolve
from); the subscription is the operator's deployment knob.
