# Task stacks — the DAG of tasks, the gates, the operator

A task stack is the workhorse shape: a multi-step job — ingest, enrich,
review, publish — where each step is a unit of work, some steps fan out
over items, some steps need a human, and an operator has to be able to
answer "where is run X and what happens next?" without reading code.

The three laws this guide builds on:

1. **A workflow row IS a `jobs` row** — one task system, one claim path,
   one operator surface. The DAG engine is not a second runtime.
2. **The status is DERIVED from rows** — never a column to go stale;
   the stuck-running class is structurally impossible while the
   derivation stands.
3. **The types reach the edges** — a step consumes what its producer
   declares, and the compile refuses the rest.

The runnable fence below is a complete stack — three stages, a map in
the middle, run end to end in one call:

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    run,
    step,
)


class Batch(BaseModel):
    doc_ids: list[str] = []


class Enriched(BaseModel):
    doc_id: str
    text: str


class Report(BaseModel):
    published: list[str]


async def ingest_body(ctx: StepContext, params: Batch) -> list[str]:
    return params.doc_ids or ["doc-1", "doc-2", "doc-3"]


async def enrich_item(ctx: StepContext, doc_id: str) -> Enriched:
    return Enriched(doc_id=doc_id, text=f"the text of {doc_id}")


async def publish_body(ctx: StepContext, enriched: list[Enriched]) -> Report:
    return Report(published=sorted(e.doc_id for e in enriched))


app = WorkflowApp()


@app.workflow("doc_pipeline")
def doc_pipeline() -> Promise[object]:
    ingested = step(ingest_body, Batch(), key="ingest")
    enriched = map_source(ingested, enrich_item)  # the join: ingest.join
    return build(step(publish_body, enriched, key="publish"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    compiled = app.get("doc_pipeline")
    # ONE call: create + drive + the decoded result.
    outcome = await run(compiled, pool, schema, input=Batch(doc_ids=["doc-1", "doc-2", "doc-3"]))
    print(f"claim={outcome.claim.kind} outcome={outcome.outcome} result={outcome.result}")

    # THE RUN KEY: the same slot twice is ONE run (the arbiter, never a
    # second silent run). A terminal run's key replay is the
    # refused-to-reuse verdict, stated loudly.
    again = await run(compiled, pool, schema, input=Batch(doc_ids=["doc-1"]), key="doc-slot-1")
    print(f"first key use: claim={again.claim.kind}")
    rerun = await run(compiled, pool, schema, input=Batch(doc_ids=["doc-1"]), key="doc-slot-1")
    print(f"terminal replay: claim={rerun.claim.kind} status={rerun.claim.status}")


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema):

```
claim=created outcome=terminal result={'published': ['doc-1', 'doc-2', 'doc-3']}
first key use: claim=created
terminal replay: claim=existing-terminal status=succeeded
```

## The wiring: the edges ARE the arguments

A stack's graph is compiled from the wiring — the promise you pass to a
`step` is the edge; pass two promises and the step has two parents (the
AND-join):

```python no-exec — not executed: verbatim fragment of examples/workflows.py (verified: tests/test_wf_demo_legs.py drives this file)
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
    gates=(REVIEW_GATE,),
)
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
```

The E-rules check the graph at compile: a consumer's params must match
what its producers declare (a single-model producer feeds the param
BARE; the list shape is the JOIN's — the mis-wiring reds as
`E5-incompatible-consumer` before any run exists), dangling edges and
eternal waits are named errors, and the SAME module compiles
byte-identically (the Mermaid diagram cannot disagree with the types).
`actor=` and `queue=` place each step — the map's children inherit the
source's actor.

`sink(routed)` is the fire-and-forget declaration: a consumer whose
result nobody reads is still checked and still runs — declared, not
dropped.

## Retries across steps: the ladder and the per-item isolation

Every step rides the ladder (`max_attempts`, per-node). The law that
matters for a stack is the ISOLATION: a map child that fails re-runs
ALONE — its siblings and every succeeded item never re-run — because
the step ledger's claim arbiter discriminates children by
`map_index` (a map child's outcome can never overwrite its sibling's).

```python no-exec — not executed: verbatim fragment of examples/workflows.py (verified: tests/test_wf_demo_legs.py — property 1: the armed child fails attempt 1, the ladder re-runs it ALONE)
async def enrich_item(ctx: Any, doc_id: str, deps: EnrichClient) -> Summary | Unreadable:
    # THE FAILING CHILD (property 1): armed on the FIRST attempt only —
    # the ladder re-runs it ALONE (the siblings and the succeeded items
    # never re-run).
    if doc_id == "doc-doomed" and ctx.attempt < 2:
        raise RuntimeError("the demo's armed transient failure (doc-doomed)")
    text = deps.fetch(doc_id)
    if text is None:
        return Unreadable(doc_id=doc_id, reason="missing from the corpus")
    return Summary(doc_id=doc_id, text=text)
```

`ctx.attempt` is the body's own view of the ladder. And the infra-fault
rule rides the same machinery: a lease reclaim (the worker died) re-runs
the attempt WITHOUT burning the ladder — infra fault is not body
failure. The retry policy itself is the retries guide's subject
([Retries](retries.md)); the workflow face adds the isolation and the
join's restraint (a child's retries never fire the join early).

## Idempotency: the two-claims law

Two mechanisms, two duplicate classes — the confusion between them is
the founding bug class this design refuses:

- **The run-key arbiter dedups CONCURRENT triggers.** Two callers
  racing one `run_key` — a double-clicked submit, a redelivered cron
  tick, two workers retrying a trigger — get ONE run. The loser gets
  the winner's id and the typed claim (`existing-running`), never a
  second silent run.
- **The enqueue idempotency key dedups TEMPORAL re-submissions.** A job
  enqueued with `idempotency_key` (unique within its `idempotency_scope`)
  is an upsert: a re-submission — after a crash, after terminal — does
  not double the work.

```python no-exec — not executed: the trigger face (verified shape: examples/workflows.py's trigger_run + the run-key pins)
async def trigger_run(pool: asyncpg.Pool, schema: str, run_key: str | None = None) -> RunClaim:
    runner = FlowRunner(wf_app.get("doc_ingest"), pool, schema)
    return await runner.create_flow(input=IngestBatch(doc_ids=list(DEMO_DOCS)), run_key=run_key)
```

The claim's three kinds carry the whole API-contract story:
`created` → 202; `existing-running` → 202 (the same run's id);
`existing-terminal` → **409, the refused-to-reuse verdict** — a terminal
run's key is never silently re-fired, the prior run's id + status ride
the envelope, and the re-run is the caller's documented choice of a NEW
key. The step level carries the same discipline: `ctx.step("name", fn)`
records one ledger row per (flow, step, map index, attempt) — the
replayed body's side effects dedup against the ledger, ON by default.

## The human gate in the middle

A stack's approval step is a hold: the run parks as a row, the worker's
slot releases, the budget pauses, and the resume is one typed resolve —
the deep-dive is [Agent fleets](agent-fleet.md) §the worked example.
The stack-specific face:

```python no-exec — not executed: verbatim fragment of examples/workflows.py (verified: tests/test_wf_demo_legs.py — property 2: the hold inside the loop pauses the budget; the Resolve form delivers the typed payload)
async def review_iteration(ctx: StepContext, carry: int) -> Done[str] | Refine[int]:
    outcome = await ctx.wait_signal(
        (ReviewDecision,), timeout_s=7 * 24 * 3600.0, reason="editorial review (the demo)"
    )
    match outcome:
        case ReviewDecision() as decision:
            if decision.verdict == "approve":
                return Done(decision.note)
            return Refine(carry + 1)
        case Expired():
            return Done("the review expired — shipping with what we have")
```

Declaring the gate in the wiring (`gates=(REVIEW_GATE,)`) makes the
hold compile-visible: the Mermaid graph renders the hold node, the
admin's Resolve form knows the payload's shape, and the CLI's
`taskq flows resolve` validates against the declared models before any
row moves.

## The operator's day

**Morning: what's wrong?**

```bash no-exec — not executed: the operator's verbs (a shell session, not a fence)
taskq flows list --limit 20        # the recent runs, derived status
taskq flows status <run_id>        # what happened / where it's blocked / what happens next
taskq flows holds <run_id>         # what this run waits on
```

Every answer names its evidence source, and a stuck finding names its
remedy — `taskq flows resolve <hold_id> '<json>' --app yourapp.workflows:app`,
`taskq flows retry <run_id> <node>`. The write verbs ride the audit
rows (who did this is a row, principal `cli:<user>`); the read verbs
are read-only, safe mid-incident.

**The run explorer** — the admin's `/taskq/workflows/{run_id}`: the
graph view with the taken paths, the failure badge, the held node's
amber, the map's children addressed by `?map_index=N` (a child's
attempt ledger is reachable, never step-key-arbitrary). The timeline
reconstructs from rows alone — after the run it tells the whole story.

**The queue levers** — `taskq queues depth/list/set-mode/
set-max-concurrent` (the damper the rate-limit path honors on
workflow rows too), `taskq job cancel-where` (the bounded bulk
stop), `taskq doctor` (the named-refusal diagnostics), and the SQL
insights surface for the questions the verbs don't ask.

**The pager** — the shipped alert rows cover the stack's real failure
classes: `TaskQQueueDepthHigh` (a misrouted or starved queue's pile),
`TaskQRunningLeaseExpired` (the kill-storm page),
`TaskQFailedJobRateHigh`/`TaskQRetryRateHigh` (a body gone bad),
`TaskQAbandonedJobs` (the settled failures nobody consumed). Each has
the confirm-SQL and the under-provisioned-vs-stalled distinction —
[Alert Runbooks](runbooks.md).

## Where the limits are (honest)

- **Subflows are not typed.** Calling `FlowRunner.create_flow` inside a
  body works and is idempotent via the run key, but the child edge is
  invisible to `validate()` and the graph view.
- **The saga surface is not built** — compensation steps are
  hand-rolled; the ledger and the cancel cascade's reverse-edge walk are
  the engine precedent ([Patterns](patterns.md) §saga).
- **Dynamic control flow lives in the loop's typed union** (`Done`/
  `Refine`), not in imperative `goto` — an unchecked cycle is the
  failure mode this design declines.
