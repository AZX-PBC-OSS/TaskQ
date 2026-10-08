# Workflows API reference (`taskq[flows]`)

The flow API — the authoring surface over the DAG engine. The import
law: `import taskq` never imports this package; the only sanctioned
entry is `import taskq.workflows` (or a submodule) by a caller that
opted into the `taskq[flows]` extra. The engine is core-deps-only; the
extra carries no requirements of its own.

## The thirty-second tour

```python
from pydantic import BaseModel
from taskq.workflows import WorkflowApp, build, gather, sink, step

app = WorkflowApp()

class Ingest(BaseModel):        # plain types for DATA
    doc_id: str

class Report(BaseModel):
    ref: str

@app.actor(queue="cpu")          # the workflow decorator — NOT taskq.actor
async def fetch(ctx, params: Ingest) -> Report:
    return Report(ref=f"r-{params.doc_id}")

@app.workflow("doc_ingest")      # the per-workflow declaration
def doc_ingest() -> object:      # SYNC and PURE — the compile-time wiring
    fetched = step(fetch, Ingest(doc_id="d1"))   # Promise[Report]
    both = gather([fetched])                      # the ALL-upstream join
    return build(step(summarize, both))           # the completeness point

compiled = app.get("doc_ingest")
compiled.validate()              # the checker-independent validator
print(compiled.mermaid())        # the compile-time Mermaid emission
```

`Promise[T]` is the wiring: a promise consumed downstream is an edge; a
call with several promise arguments is the fan-in join whose user body
IS that node's body. Plain types travel as data; nothing about the tree
is STORED — the graph is spelled by the wiring and compiled fresh (same
module → same compile, byte-stable).

## The wiring verbs

| verb | signature | notes |
| --- | --- | --- |
| `step(body, *args, key=None, actor="wf", queue="default", on_failure="fail_closed", max_attempts=3, retry_kind=None, skip=None, gates=())` | `→ Promise[R]` where `R` is the body's declared return | promise args become the node's incoming edges (several = the fan-in); plain args are the node's data (in signature order). `skip` is the DISPATCH-TIME predicate (cut #4). |
| `gather(promises, *, on_failure="fail_closed")` | `list[Promise[A]] → Promise[list]` | the ALL-upstream join, the FLAT shape. `gather([])` is refused at the verb (the stranded invisible join). |
| `map_source(source, body, *, key=None, queue="default", on_failure="fail_closed", max_attempts=3)` | `Promise[list[T]] × body → Promise[list[R]]` | the map: the source's finalize forks N children (fresh jobs, per-item ledger identity); the join collects. One map per source (a node finalizes once). |
| `sink(*promises)` | `→ None` | explicit fire-and-forget — RECORDED in the compiled metadata, never silent. |
| `build(result, *residuals)` | `Promise[R] → Promise[R]` | the terminal completeness point: names the result and accounts for every residual (the `Promise[Never]` typing forces the static side). |

The workflow declaration: `@app.workflow(name, *, capture="none" |
"errors-only" | "all" = "errors-only", redact=None)` — §10.3's policies
per workflow. The redact hook **post-composes on the default chain's
output** (chain → hook, unconditional): it can only redact MORE, never
less — a hook that returns raw fixture text still lands a MASKED row
(the pin: an un-redacted canary never reaches a row).

`@app.actor` is a DIFFERENT decorator from `taskq.actor` but projects
into the SAME registry/config-sync surface (the config-drift machinery,
the deregistration guards, the admin's actors page, and
`TASKQ_QUEUES_STRICT`'s fail-fast all see workflow actors — no second,
invisible actor population). The vanilla path stays byte-identical.

## `wf.validate()` — the checker-independent validator

Runs in pytest, CI, and at worker boot; the ~6 rules, each classified —
**the zero-false-positive doctrine: over-refusing valid graphs is the
compile's version of over-rejection.**

| rule | class | convicts |
| --- | --- | --- |
| `E1-acyclicity` | error | a cyclic wiring (owned UNCONDITIONALLY here — the checker claim is dead errata) |
| `E2-produced-never-consumed` | error | a promise nobody consumes, sinks, or terminals — work that will never be acknowledged |
| `E3-edgeless-join` | error | a join with zero incoming edges (the stranded invisible join) |
| `E4-unannotated-step` | error | a body without a return annotation — the annotation IS the wiring |
| `E5-incompatible-consumer` | error | an unrelated payload model consumed (the checker-independent half of the typing story) |
| `E6-fan-in-bound` | error | a join over `MAX_FAN_IN_PER_JOIN` (1000) parents |
| `W1-eternal-wait` | warning | a gate with no declared timeout — "a workflow that waits forever on a human is a support ticket" |

The report is ONE-PASS (tsc-style): every rule's verdict, not the first
failure alone. The mutation matrix (each mutation flips exactly one
rule, nothing else) is pinned in `tests/test_wf_validate_pins.py`.

## The Mermaid emission

`compiled.mermaid()` — a pure function of the wiring, byte-stable,
golden-testable. Node shapes by kind: stadium `([])` = map source,
hexagon `{{}}` = the collapsed map's join, rectangle `[]` = plain actor,
`[[...]]` = the gather join, `([(...)])` = the HITL hold nodes (rendered
with their gate name + timeout policy — T10's gate consumes the same
declaration). Edge labels = the promise types. The diagram-lies property
is pinned: every compiled node/edge appears in the emission, nothing
else does.

## The runner (create / drive / result)

`FlowRunner(compiled, pool, schema)` — the API's reference worker and
the test/demo driver:

- `create_flow(*, input=None, run_key=None) → JobId` — the run carries
  its INPUT on the root row (cut #7: cross-flow data never dies in a
  Python closure); bodies read `ctx.input`.
- `drive(flow_id, *, until="held" | "terminal", tick, max_ticks) →
  str` — the driver (cut #10): no consumer hand-rolls a dispatch-poll
  loop; the drive is BOUNDED (`max_ticks` — a hang is a defect with no
  stack trace).
- `result(flow_id)` — the terminal node's result, DECODED (cuts
  #14/#19: never a raw jsonb string).

The dispatch contract: bodies resolve from the REGISTERED DEFINITION
(D1 — the registry is the only body source); the claim is fenced; the
finalize is the engine's two-tx shape; a body exception ladders
(attempt failures emit NO terminal — P3 rule 7) with `retry_kind`
routing (`"permanent"` takes no ladder) and per-node `max_attempts`.
