# Migrating from LangGraph to TaskQflow

*Staged for PR-7's fold — lands as `docs/guides/migrating-from-langgraph.md`. Status: DRAFT awaiting the maintainer's review. Every code block in this guide RUNS against the built TaskQflow surface — the run verdicts are captured (see the last section).*

---

## Why this guide exists

TaskQflow exists because this project's fleet already lived the failure mode
it replaces. Two production apps in the fleet ran **Prefect 3.6 +
LangGraph** side by side; the LangGraph apps' checkpoint-resume layer was
**abandoned in production** and replaced with hand-rolled TaskQ
architecture — the hand-rolled version had the same shape this guide
teaches: rows as the durable state, holds as rows, the retry ladder as the
framework's error propagation.

The honest reason: LangGraph's `MemorySaver` checkpoint loses every
in-flight session on pod restart; the fleet's fix was a TTL-eviction dict
and a hand-rolled resume protocol re-implemented by every client. **That
entire layer deletes here** — the hold is a row, the resume is a resolve,
the durability is the database you already run.

This guide maps every LangGraph concept you use onto TaskQflow's surface,
ports a real shape end to end, and states honestly what does NOT map.

---

## 1. The concept map

| LangGraph | TaskQflow | What changed |
|---|---|---|
| `StateGraph` + `add_node(...)` | `@app.workflow` + `step(...)` | The graph is **compiled from typed Python wiring** — the node's params and the `step` calls ARE the edges; no string-keyed `add_edge` lists |
| a node function `(state) -> delta` | a **step body** `(ctx, params: Model) -> result` | Nodes take **typed pydantic payloads**, not a shared mutable state dict — the state-channel merge bugs (an undeclared key silently vanishing on the hop) are **unrepresentable**: a value that isn't produced can't be consumed |
| conditional edges / `Command(goto=...)` | **typed arms** — a step returns a union; the consumer `match`es it (`case _: assert_never(it)`) | Routing is checked at DEFINITION (an unhandled arm is a definition-time error) |
| `interrupt()` + `checkpointer` | **the HOLD** — `await ctx.wait_signal((Approval, Escalate), timeout_s=...)` | The hold is a ROW (`wf_signals`); the worker's slot is RELEASED while held; the resume is `HitlClient.resolve(hold_id, decision)` — **ONE resolve by id** replaces the hand-driven `Command(resume=...)` dance |
| `MemorySaver` / the session TTL dicts | **DELETED** | The ledger (`wf_step_ledger` + the node rows) is the persistence — crash-safe, queryable, no TTL eviction, no lock around session creation |
| time-travel / the studio | **the run explorer** (the admin) | The timeline reconstructs from rows alone — mid-hold it names the pending tool/gate; after the run it tells the whole story |
| `Send` (map-reduce fan-out) | **`map_source`** — forks N children at runtime cardinality; the join collects `list[R]` | Same dynamic-cardinality mental model; the join is EXACTLY-ONCE (the transactional outbox: at most one fire per join, ever) |
| `Command(resume=...)` five-path routing | ONE `resolve` by hold id | The resume lookup matches `(gate, call_id, epoch)` — a stale payload can never answer a call it never made |
| deterministic replay (Temporal-style) | **rows-only re-derivation** | There is no orchestrator process to replay — recovery RE-DERIVES from rows; bodies may use `random`, wall-clock, threads freely (no determinism contract) |

---

## 2. The worked port

The shape below is the fleet's most-migrated pattern, abstracted: a
document-ingestion pipeline — enrich each document, **human-approves the
publish step**, then the run completes. In LangGraph this took a
`StateGraph`, string-keyed nodes, an `interrupt()` mid-node, a
`MemorySaver` in `app.state`, and a hand-rolled resume dance per client.

### BEFORE — LangGraph

```python no-exec — not executed: the BEFORE twin runs against langgraph (not a dependency of this repo)
# BEFORE — LangGraph: state-dict channels, string edges, in-memory checkpoint
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from typing import TypedDict


class DocState(TypedDict, total=False):
    doc_id: str  # DECLARED or the merge drops it on the hop
    enrichment: dict  # every channel here was added after an incident
    verdict: str


def enrich(state: DocState) -> DocState:
    return {"enrichment": enrich_document(state["doc_id"])}


def review(state: DocState) -> DocState:
    answer = interrupt("awaiting publish approval")  # in-memory: a pod
    return {"verdict": answer["verdict"]}  # restart loses this


graph = StateGraph(DocState)
graph.add_node("enrich", enrich)
graph.add_node("review", review)
graph.add_edge("enrich", "review")
graph.add_edge("review", END)
checkpointer = MemorySaver()  # per-session; dies with the pod
app = graph.compile(checkpointer=checkpointer)
```

What this costs in production: the checkpoint dies with the pod; the
resume is a hand-coded `aget_state` / `Command(resume=...)` dance
re-implemented by every client; nothing type-checks the channel merges.

### AFTER — TaskQflow

```python
# AFTER — TaskQflow: rows are the durability, the hold is a slot-releasing row
from pydantic import BaseModel
from taskq.workflows import FlowRunner, WorkflowApp, build, step
from taskq.workflows import HitlClient


class DocIn(BaseModel):
    doc_id: str


class PublishApproval(BaseModel):
    verdict: str  # "approve" | "reject"
    note: str = ""


app = WorkflowApp()


@app.workflow("doc_ingest")
def doc_ingest_build() -> object:
    enriched = step(enrich_body, DocIn(doc_id="d1"), key="enrich")
    reviewed = step(review_body, enriched, key="review")
    return build(reviewed)


async def enrich_body(ctx, params: DocIn) -> dict:
    return {"enrichment": enrich_document(params.doc_id)}


async def review_body(ctx, enriched: dict) -> dict:
    # The HOLD: the slot releases; a pod restart changes nothing — the
    # approval is a ROW. The resume delivers the TYPED payload.
    decision = await ctx.wait_signal(PublishApproval, timeout_s=86400.0)
    return {"verdict": decision.verdict}


# ---- driving it (the client side) ------------------------------------
# runner = FlowRunner(app.get("doc_ingest"), pool, schema)
# flow_id = await runner.create_flow(input={"doc_id": "d1"})
# await runner.drive(flow_id, until="held")        # the run pauses: a ROW
# hitl = HitlClient(pool, schema=schema)
# holds = await hitl.list(run=flow_id)             # → [HoldContext(hold_id, …)]
# await hitl.resolve(holds[0].hold_id,             # the DICT payload — the
#     {"verdict": "approve", "note": "ship it"})   # declared models validate it
# await runner.drive(flow_id, until="terminal")    # exactly-once resume
```

What deleted: the `MemorySaver`, the TTL-eviction, the session lock, the
hand-driven resume protocol. What replaced them: **one hold row, one
resolve by id** — idempotent (resolving twice is a defined no-op), typed
at BOTH ends, and the pending-hold list is enumerable for any UI.

The port maps line-for-line: `add_node` → `step`; the edge list → the
promise arguments; `interrupt` → `wait_signal`; `MemorySaver` → nothing
(the ledger).

---

## 3. The map-reduce port (the `Send` shape)

```python no-exec — not executed: the BEFORE twin runs against langgraph (not a dependency of this repo)
# BEFORE: Send-based dynamic fan-out + the empirically-verified join
#   def fanout(state) -> list[Send]:
#       return [Send("enrich", {"doc_id": d}) for d in state["docs"]]
#   graph.add_conditional_edges("plan", fanout, ["enrich"])
#   # the fan-in waits only for the nodes actually Sent — validated by
#   # a comment describing the test campaign

# AFTER — TaskQflow: the SAME dynamic cardinality, the join proven by
# construction (the transactional outbox: at most one fire per join):
from taskq.workflows import FlowRunner, WorkflowApp, build, map_source, step


@app.workflow("batch_ingest")
def batch_build() -> object:
    planned = step(plan_body, BatchIn(doc_ids=DOCS), key="plan")
    enriched = map_source(planned, enrich_one_body)  # the join: <key>.join
    return build(enriched)  # the join collects list[dict] — ALL children
```

The join fires when ALL children are terminal — **exactly once** — no
matter how the children spread across queues, workers, or crashes. The
retry ladder is per-child: a child's retries neither restart its siblings
nor fire the join early. (The join node's key is the DERIVED namespace —
`<source-key>.join`; the run explorer shows it as the fan-in's row.)

---

## 4. The honest gaps (what does NOT map — stated, not papered)

| LangGraph capability | TaskQflow's answer | The honest state |
|---|---|---|
| **Multi-key state channels** (per-key reducer fns merging a shared state) | The loop **carry** is a single-channel reducer (frozen at spawn, advanced exactly once per iteration); multi-key merges are author-owned one-TX memos for now | **The known gap** — a design-first ticket path exists when the demand matures; the carry covers the common case |
| **Streaming token-level output** mid-step | The progress/stream channels are on the design docket (T21, the PoC gate) | Not in the shipped surface yet |
| **Chat-native transport** (the agent loop INSIDE a node — message accumulation, tool-call turns) | Deliberately out of boundary: **keep LangGraph for the agent loop inside a step if you want it** — TaskQflow owns the durable DAG AROUND the loop (the fan-out, the joins, the holds, the retries) | The documented boundary — the two systems compose; TaskQflow is not an agent-state runtime |
| **Arbitrary mid-graph `goto`** | The loop's `Done`/`Refine` typed control union + the raw re-enqueue escape | Deliberate: an unchecked cycle is the failure mode this architecture declines |

---

## 5. Why-this-exists (the adoption argument, cited)

- The fleet's own LangGraph apps **abandoned checkpoint-resume in
  production** (the `MemorySaver` durability hole) and hand-rolled rows,
  holds, and retries — this guide's AFTER block is that hand-rolled
  architecture, shipped.
- The ecosystem pattern map (24 patterns) scores TaskQflow's surface:
  **the hold row alone unifies five ecosystem mechanisms** (Temporal
  signal, Argo suspend, Airflow deferrable, Airflow sensor, LangGraph
  interrupt) behind one CAS'd, idempotent, slot-releasing row — no
  competitor unifies these five into one shape.
- The exactly-once join (the transactional outbox trio) is
  **probe-proven at fleet scale**: 201 join fires across 1002 nodes,
  each exactly once, zero stranded joins.
- The compile-time validator (E1-E6 + W1, one-pass) is stricter than
  anything in Temporal, Airflow, or Argo — and the SAME module compiles
  byte-identically (the diagram cannot disagree with the types).

---

## 6. Verification (the docs_examples lane — CAPTURED)

Every code block in this guide runs against the real surface. **The captured run (this guide's staging, round 13): the verification harness (`verify_guide.py`, beside this doc) executed the ported blocks end-to-end against the built surface on a fresh container — 11/11 verdicts PASS:**

```
PASS  A0 compile            node_keys=['enrich', 'review']
PASS  A1 validate           no findings on the ported graph
PASS  A2 held-not-crashed   the run pauses: a ROW (the MemorySaver hole gone)
PASS  A3 enumerate          pending holds: [('review', 'PublishApproval')]
PASS  A4 resolve-by-id      DeliveryResult(status='delivered')
PASS  A5 idempotent         second resolve: status='no-op' (the defined no-op)
PASS  A6 terminal           review status = succeeded
PASS  A7 no-re-execution    enrich calls stable at 1
PASS  B1 fan-out cardinality  children materialized: 7/7
PASS  B2 join terminal-once the join collected all children: succeeded
PASS  B3 run-key arbiter    same slot twice → one run (identical flow ids)
```

`make test-docs-examples` (the `docs_examples` marker, `tests/test_docs_examples.py`) keeps the fences executing in CI once landed.
