# Workflows API reference (`taskq[flows]`)

The flow API — the authoring surface over the DAG engine. The import
law: `import taskq` never imports this package; the only sanctioned
entry is `import taskq.workflows` (or a submodule) by a caller that
opted into the `taskq[flows]` extra. The engine is core-deps-only; the
extra carries no requirements of its own.

## The thirty-second tour

```python
from pydantic import BaseModel
from taskq.workflows import StepContext, WorkflowApp, build, gather, sink, step

app = WorkflowApp()


class Ingest(BaseModel):  # plain types for DATA
    doc_id: str


class Report(BaseModel):
    ref: str


@app.actor(queue="cpu")  # the workflow decorator — NOT taskq.actor
async def fetch(ctx: StepContext, params: Ingest) -> Report:
    return Report(ref=f"r-{params.doc_id}")


@app.actor(queue="cpu")
async def summarize(ctx: StepContext, reports: list[Report]) -> dict[str, int]:
    return {"count": len(reports)}


@app.workflow("doc_ingest")  # the per-workflow declaration
def doc_ingest() -> object:  # SYNC and PURE — the compile-time wiring
    fetched = step(fetch, Ingest(doc_id="d1"))  # Promise[Report]
    both = gather([fetched])  # the ALL-upstream join
    return build(step(summarize, both))  # the completeness point


compiled = app.get("doc_ingest")
compiled.validate()  # the checker-independent validator
print(compiled.mermaid())  # the compile-time Mermaid emission
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
| `gather(promises, *, on_failure="fail_closed")` | `list[Promise[A]] → Promise[list[A]]` | the ALL-upstream join, the FLAT shape — the ELEMENT TYPE PRESERVED (a homogeneous join over `Promise[Report]`s is a `Promise[list[Report]]`; a heterogeneous join upcasts to `Promise[list[object]]` honestly). `gather([])` is refused at the verb (the stranded invisible join). |
| `map_source(source, body, *, queue="default", on_failure="fail_closed", max_attempts=3)` | `Promise[list[T]] × body → Promise[list[R]]` | the map: the source's finalize forks N children (fresh jobs, per-item ledger identity); the join collects. One map per source (a node finalizes once). The join key is DERIVED (`<source>.join`) — the engine's fork addresses the map join by the source's own key; a custom join key is refused, never silently ignored. |
| `sink(*promises)` | `→ None` | explicit fire-and-forget — RECORDED in the compiled metadata, never silent. |
| `build(result, *residuals)` | `Promise[R] × Promise[Never] → Promise[R]` | the terminal completeness point: names the result and accounts for every residual. The residual slot is `Promise[Never]` — a produces-nothing body's handle (`-> NoReturn`); because `Promise` is covariant, every REAL data handle in the slot is the checker's error (the static half of `E2-produced-never-consumed`). |
| `loop(name, body, *, carry=None, until=None, max_iterations=None, budget_s=None, on_exhausted="escalate", escalates_to=None, gates=())` | `→ Promise[Any]` | the LOOP node (see the section below — the carry contract, the two walls, the named exhaustion). `until` is AWAITED per iteration (`Callable[[], Awaitable[bool]]`); `gates=` declares the body's HOLD gates for the admin's resolve/deliver doors. |

## The loop (`wf.loop`, `Done`, `Refine`)

`loop(name, body, ...)` wires a LOOP node: each iteration is FRESH jobs
(the iteration-scoped step keys `<loop>.iter<i>` keep the idempotency
ledger per-iteration). The body receives `(ctx, carry)` and returns the
CONTROL UNION:

- `Done(payload)` — the typed EXIT: the loop stops; the payload is the
  loop node's result.
- `Refine(feedback)` — the typed CONTINUE: the feedback threads the
  carry into the NEXT iteration, advanced EXACTLY ONCE per iteration in
  the ADVANCE statement — one atomic write shared with the CAP GUARD
  (`iteration < max_iterations` is the same statement's WHERE leg); a
  carry advanced at hold/retry time is the optimistic-apply dragon,
  kept red forever.
- **anything else is the TYPED SHAPE ERROR** — `LoopBodyShapeError`: the
  iteration's ledger terminal records it FAILED (the ledger records the
  truth; a wrong-shape return is never laundered into a succeeded row
  the memo replay would re-thread as a Refine), the loop exhausts with
  the named class, and the flow terminalizes.

**THE CARRY'S TYPE IS THE CONTRACT**: the declared `carry=` value's type
is re-applied at the ONE point a carry reaches the body — at iteration 0
AND after EVERY resume and on the memo-replay path (the jsonb round-trip
is typeless; the driver re-hydrates through the declared type's
validator: a pydantic model, a dict subclass, or a JSON-native type). A
declared type the jsonb boundary cannot honor is the loud
`LoopCarryContractError` — a silent type change is refused. The compile
side is E8 (the carrier-type check).

**THE TWO WALLS ARE DIFFERENT**: `max_iterations` bounds TOTAL SPAWNS
(the iteration cap); `budget_s` is the TIME wall — and it is BLIND while
the loop holds on a human (`budget_paused` — holds are free). Exhaustion
is NAMED, never silent: the cap or the budget wall terminates the loop
into the `iteration_cap_exhausted` / `budget_exhausted` state (the
metadata's `iteration_state` + the typed failure class
`IterationLimitExhausted` / `LoopBudgetExhausted`) and the FLOW
TERMINALIZES in the same transaction. The ADVANCE and EXHAUST statements
carry the claim identity's fence (worker + attempt + claim_epoch — the
same legs every other terminal write carries): a zombie driver's stale
advance or exhaust is refused, never a killed healthy loop, never a
backward counter.

**`on_exhausted="escalate"`** (the default) enqueues the escalation
through the same outbox the fired joins use, addressed to the
workflow's REGISTERED escalation step (`loop.escalation` —
`escalates_to=` declares the body, the framework's default warns and
records otherwise). The escalation consumer is DISPATCHABLE after the
flow's terminal: a flow's death must not orphan its pages-a-human duty
(the dispatch fence's escalation-kind exemption — the ONE workflow row
a terminal flow's claim still admits). `on_exhausted="fail"`
terminal-fails the flow and enqueues NOTHING.

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

Runs in pytest, CI, and at worker boot; the FIFTEEN rules shipped (read
from `taskq/workflows/api/_validate.py`'s `_run_rules` — E1–E11, W1, the
two W2 faces, and W3; an earlier revision claimed about six rules against
a table that listed 7 — the reference now ships COMPLETE, from the code,
not remembered), each classified —
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
| `E7-cross-graph-promise` | error | a promise wired from ANOTHER app's recorder — two apps' graphs spliced invisibly |
| `E8-carrier-type` | error | the loop's declared `carry_type=` model (or the model instance passed as `initial=`) vs the body's `Refine[...]` feedback type — unrelated carrier models: the thread promises data the next iteration cannot receive |
| `E9-ctx-annotation` | error | a body whose `ctx` annotation is not `StepContext` (or a subclass) — the annotation is verification, not documentation; the fabricated stand-in is refused at compile |
| `E10-arity` | error | a body's params (beyond `ctx`) not matching the wired sources' count — the wiring's own promise, refused at compile, never a mid-flow ladder discovery |
| `E11-loop-promise-carry` | error | a promise handle wired as the loop's `initial=` — the initial carry is a VALUE, never a handle (the handle cannot ride the row; wire the parent's result through a first step's return, or read it in the body) |
| `W1-eternal-wait` | warning | a gate with no declared timeout — "a workflow that waits forever on a human is a support ticket" |
| `W2-unknown-queue` | warning | a node projected onto a queue this app cannot see (the actor-not-found parking shape, named at validate) |
| `W2-join-for-progress` | warning | a join SUNK for display only — the DAG still blocks on it; declare the map's `aggregate=` fn instead |
| `W3-eternal-loop` | warning | a loop with no `until=` and neither wall set (`max_iterations`/`budget_s`) — a loop that can never stop on its own; declare the bound explicitly |

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
