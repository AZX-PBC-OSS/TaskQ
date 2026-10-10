# Migrating graph-checkpoint workflows

> Coming from a graph-checkpoint framework (LangGraph-style: a state
> graph with `interrupt()` and a checkpointer beside the process)? The
> port is mechanical and the checkpoint layer deletes entirely — the
> hold is a row, the resume is one typed resolve, the durability is the
> database you already run.

## 1. The feature map (what you're leaving → the face here)

| The feature you use | The face here | Where to see it |
|---|---|---|
| `StateGraph` + `add_node(...)` + string edges | `@app.workflow` + `step(...)` — the promise arguments ARE the edges; the E-rules refuse the wrong shapes at compile | [Task stacks](task-stacks.md) §the wiring |
| a node function over a shared state dict | a **step body** `(ctx, params: Model) -> result` — typed pydantic payloads; an undeclared key silently vanishing on the hop is unrepresentable | [Task stacks](task-stacks.md) §the wiring |
| conditional edges / `Command(goto=...)` | **typed arms** — a body returns a union; the consumer `match`es it (`case _: assert_never(it)`) | [Event pipelines](event-pipeline.md) §the typed route |
| `interrupt()` + `Command(resume=...)` | **the HOLD** — `ctx.wait_signal(...)`; the resume is ONE `HitlClient.resolve(hold_id, decision)`, idempotent, typed at both ends | [Agent fleets](agent-fleet.md) §the minimal gate (verified fence) |
| `MemorySaver` / the in-process checkpointer | **DELETED** — the step ledger + the node rows are the persistence; crash-safe, queryable, nothing dies with the pod | [Patterns](patterns.md) §the headline (one row, five mechanisms) |
| thread replay / resume-from-checkpoint | the body replays from the top; the ledger memo returns the SAME results — side effects once across attempts, delivered answers in epoch order | the checkpoint-replay pins |
| `Send` (dynamic map-reduce) | **`map_source`** — runtime-cardinality fork, the exactly-once join | [Event pipelines](event-pipeline.md) §fan-out with a join |
| time-travel / the visual debugger | **the run explorer** (the admin) — the timeline reconstructs from rows alone | [Task stacks](task-stacks.md) §the operator's day |
| deterministic replay | **not the model here** — rows-only re-derivation: bodies may use `random`, wall-clock, threads (no determinism contract) | [Patterns](patterns.md) §the rest of the parity table |

## 2. The worked port

The most-migrated shape: a document-ingestion pipeline — enrich each
document, **human-approves the publish step**, the run completes. This
fence runs as-is under the docs-example harness:

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    FlowRunner,
    HitlClient,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)


class DocIn(BaseModel):
    doc_id: str


class PublishApproval(BaseModel):
    verdict: str  # "approve" | "reject"
    note: str = ""


async def enrich_body(ctx: StepContext, params: DocIn) -> dict:
    return {"enrichment": f"enriched({params.doc_id})"}


async def review_body(ctx: StepContext, enriched: dict) -> dict:
    # The HOLD: the slot releases; a pod restart changes nothing — the
    # approval is a ROW. The resume delivers the TYPED payload.
    decision = await ctx.wait_signal(PublishApproval, timeout_s=86400.0)
    return {"verdict": decision.verdict}


app = WorkflowApp()


@app.workflow("doc_ingest")
def doc_ingest() -> Promise[object]:
    enriched = step(enrich_body, DocIn(doc_id="d1"), key="enrich")
    reviewed = step(review_body, enriched, key="review")
    return build(reviewed)


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    runner = FlowRunner(app.get("doc_ingest"), pool, schema)
    flow_id = (await runner.create_flow(input=DocIn(doc_id="d1"))).flow_id
    await runner.drive(flow_id, until="held")  # the run pauses: a ROW

    hitl = HitlClient(pool, schema=schema)
    holds = await hitl.list(run=flow_id)
    result = await hitl.resolve(holds[0].hold_id, {"verdict": "approve", "note": "ship it"})
    print(f"resolve: {result.status}")

    await runner.drive(flow_id, until="terminal")
    print(f"result: {await runner.result(flow_id)}")


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema):

```
resolve: delivered
result: {'verdict': 'approve'}
```

What deleted: the in-memory checkpoint store, the TTL eviction, the
session lock, the hand-driven resume protocol re-implemented by every
client. What replaced them: **one hold row, one resolve by id.**

The dynamic fan-out port (`Send` → `map_source`, the exactly-once join)
is the same two-line move — the verified shape is
`docs/guides/verify_guide.py` beside this doc (blocks B1–B3: the
cardinality, the join-fired-once, the run-key arbiter; 11/11 verdicts
captured there).

## 3. The honest gaps (what does NOT map — stated, not papered)

| The capability | The face here | The honest state |
|---|---|---|
| Multi-key state channels (per-key reducer fns) | the loop **carry**: a single-channel reducer, frozen at spawn, advanced exactly once per iteration | **the known gap** — the carry covers the common case; multi-key merges are author-owned one-TX memos for now |
| streaming token-level output mid-step | the progress/stream channels + the SSE faces; a token stream is the embedding app's own | composes today; no first-class token channel |
| the agent loop INSIDE a node (message turns, tool calls) | keep your agent-loop runtime inside a step — TaskQflow owns the durable DAG AROUND the loop | the documented boundary; the two layers compose |
| arbitrary mid-graph `goto` | the loop's `Done`/`Refine` typed control union | deliberate — an unchecked cycle is the failure mode this design declines |

## 4. Why the checkpoint layer deletes (the problem, stated plainly)

A graph runtime whose state lives beside the process loses every
in-flight session on restart, and each client then re-implements the
resume dance: poll for the interrupt, fetch the state, ship the resume
value, survive the races. Here the interrupted node is a ROW — the
restart-replay contract is the engine's, not yours. The parity claims'
evidence discipline lives in [the pattern catalog](patterns.md).
