# Workflows API reference (`taskq[flows]`)

The flow API — the authoring surface over the DAG engine. The import
law: `import taskq` never imports this package; the only sanctioned
entry is `import taskq.workflows` (or a submodule) by a caller that
opted into the `taskq[flows]` extra. The engine is core-deps-only; the
extra carries no requirements of its own.

**THE GUIDES ARE THE TUTORIAL, THIS PAGE IS THE DICTIONARY** — every
verb row below is taught, with runnable fences, in
[the workflows guide](../guides/workflows.md) (the wiring, the joins,
the routes, the retry ladder, the deps seam, the body's three input
channels); [the rate-limiting guide](../guides/rate-limiting.md) owns
the `rate_limits=` authoring face and WHICH execution door honors the
buckets; [the progress guide](../guides/progress.md) owns the run
display + the listener faces. Start there if this page is your first
contact with the engine.

## The thirty-second tour

```python
from pydantic import BaseModel
from taskq.workflows import Promise, StepContext, WorkflowApp, build, gather, sink, step

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
def doc_ingest() -> Promise[object]:  # SYNC and PURE — the compile-time wiring
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
| `step(body, *args, key=None, actor="wf", queue="default", on_failure="fail_closed", max_attempts=3, retry_kind=None, skip=None, gates=(), progress_schema=None, rate_limits=(), max_concurrent=None, reservations=(), result_ttl=None, priority=None, cache=False, cache_ttl=None)` | `→ Promise[R]` where `R` is the body's declared return | promise args become the node's incoming edges (several = the fan-in); plain args are the node's data (in signature order). `skip` is the DISPATCH-TIME predicate (cut #4). `progress_schema=` is the node's DECLARED progress payload schema (T21 — the body's `ctx.progress` emissions are validated against it). `priority=` is the node rows' claim ORDER (the value stamped into the `jobs.priority` column, which the dispatch claim's `priority DESC` orders by; `None` = the actor's declaration applies, or the column default `0` — the precedence law: the per-call run/create_flow override > the step's declaration > the actor's declaration > the default). THE CAPACITY TERMS (the same surface `@app.actor` declares — the explicit step param overrides the handle's, never the reverse): `rate_limits=` stamps the node's bucket names onto the row's metadata (honored at the fleet's dispatch claim; a denial snoozes the row budget-free); `max_concurrent=` is the per-round admission DAMPER seeded on the cohort's `actor_config` — NOT a strict cap (declare `reservations=` when the bound must hold); `reservations=` is the STRICT slots (a list of `ConcurrencyReservation` — the boot materializes their slot rows in the ensure-slots pass; an unnamed/malformed entry parks the row fail-closed, LOUD); `result_ttl=` is the node result's in-DB TTL in seconds (stamped on the SUCCEEDED terminal row from the cohort's stored term). `max_pending` is deliberately absent — the engine's inserts bypass the enqueue admission and a claim-side gate would wedge the backlog, so the knob stays vanilla-actor-only (never a dead knob). `cache=True` is the CROSS-RUN CACHE opt-in (T25 — see the guide's cache section): the result serves a later run over the same body + input; `cache_ttl` is the in-DB expiry's seconds (default 24h). THE CACHE HAZARDS (each refused at the wiring — E17/E18/E19/E20): the address covers the body's OWN source + the WIRED args — a changed HELPER is invisible to it (bump the body's own source when the closure changes); the FLOW input (`ctx.input`) never enters it (a body reading `ctx.input` is refused — the cross-tenant leak); a typed-route SOURCE carrying `cache=True` is refused (the hit path cannot rebuild the route fork — the wedged-forever run); an Exit-returning body is refused (the silent no-op); an unverifiable body source is refused (the silently-uncached step). |
| `gather(promises, *, on_failure="fail_closed")` | `list[Promise[A]] → Promise[list[A]]` | the ALL-upstream join, the FLAT shape — the ELEMENT TYPE PRESERVED (a homogeneous join over `Promise[Report]`s is a `Promise[list[Report]]`; a heterogeneous join upcasts to `Promise[list[object]]` honestly). `gather([])` is refused at the verb (the stranded invisible join). |
| `route(source, arms, *, queue="default", on_failure="fail_closed", max_attempts=3, aggregate=None, child_driven=False)` | `Promise[list[S]] × Mapping[type, RouteArm[R] \| Callable] → Promise[list[R]]` | the TYPED ROUTE (T27): the source's body returns the elements' list (`list[Image \| Audio]`), and *arms* keys the union MEMBERS' types to the arm bodies — each element runs ITS type's arm as a FRESH job (`<source>.item:<module.qualname>`), the arm's placement stamps its children's queue/actor, and the derived `<source>.join` packs the arms' returns VERBATIM (the typed sum — an arm returning `list[X]` packs that list as ONE element; see the guide's nesting note). Totality is fenced at three doors: the verb refuses a non-total route (`RouterNotTotal` at runtime for a body that lied), `E15-route-totality` re-proves it from the compiled graph, and the runtime door raises. Takes a LIST source — a single-value `Promise[A\|B]` is refused at build ("the route is a MAP face"; the chain step's in-body `Route` is the single-value routing face). `aggregate=` is the user's DECLARED read-side fn over the children's decoded results (progress aggregation — evaluated AT READ TIME, writes nothing; `taskq.workflows.read_map_aggregate` evaluates it). `child_driven=True` is the fan-in bound's escape (the join counts its terminal children from the edge ledger). Per-arm placement/admission: `RouteArm(body=fn, actor=None, queue=None, rate_limits=(), max_concurrent=None, reservations=(), result_ttl=None, priority=None, capacity_owner="operator")` — `queue=None` inherits the route's `queue=`, `actor=None` the source's actor; a bare `Callable` arm normalizes to `RouteArm(body=fn)`; the capacity fields are the arm's OWN declaration (a capped handle wired under this arm's actor override has its terms re-declared here, and an override that would silently drop a capped handle's terms is refused at the wiring); `priority=None` = the arm does not override (the SOURCE node's resolution applies — the arm overrides DOWN from the source, graph-DIRECTION, not a value ceiling: the arm's value may raise or lower freely, and the JOIN keeps the source's resolution); `capacity_owner` is the arm's own owner declaration — an overriding arm whose body handle carries `capacity_owner="code"` must re-declare it HERE (per-arm: the other arms and the join never inherit it). |
| `chain_source(chain, source_body, *args, key=None, actor="wf", queue="default", max_attempts=3)` | `Chain × BodyFn → Promise[Any]` | the T20 streaming chain's source: *source_body* is the paged generator (each `ctx.emit_batch` yield = one page's emit tx); the chain's step rows exist only from the emit onward (never graph nodes — the runner routes them from the compiled chain). |
| `map_source(source, body, *, queue="default", on_failure="fail_closed", max_attempts=3, aggregate=None, rate_limits=(), child_driven=False)` | `Promise[list[T]] × body → Promise[list[R]]` | the map: the source's finalize forks N children (fresh jobs, per-item ledger identity); the join collects. One map per source (a node finalizes once). The join key is DERIVED (`<source>.join`) — the engine's fork addresses the map join by the source's own key; a custom join key is refused, never silently ignored. `aggregate=` is the user's DECLARED read-side fn over the children's decoded results (progress aggregation — evaluated AT READ TIME, writes nothing; `taskq.workflows.read_map_aggregate` evaluates it). `rate_limits=` stamps the CHILDREN's admission terms (the buckets' names ride each child row's metadata — honored at the FLEET's dispatch claim, see the rate-limiting guide's doors note). `child_driven=True` is the fan-in bound's escape (the join counts its terminal children from the edge ledger). The DICT form (`{Type: arm}`) is the typed route — the same lowering as `route`. |
| `sink(*promises)` | `→ None` | explicit fire-and-forget — RECORDED in the compiled metadata, never silent. |
| `build(result, *residuals)` | `Promise[R] × Promise[Never] → Promise[R]` | the terminal completeness point: names the result and accounts for every residual. The residual slot is `Promise[Never]` — a produces-nothing body's handle (`-> NoReturn`); because `Promise` is covariant, every REAL data handle in the slot is the checker's error (the static half of `E2-produced-never-consumed`). |
| `loop(name, body, *, initial=None, carry_type=None, until=None, max_iterations=None, budget_s=None, on_exhausted="escalate", escalates_to=None, gates=())` | `→ Promise[Any]` | the LOOP node (see the section below — the carry contract, the two walls, the named exhaustion). `initial=` is the initial carry VALUE, `carry_type=` the DECLARED carrier-type (derived from the instance when `initial=` is a model); `until` is AWAITED per iteration (`Callable[[], Awaitable[bool]]`); `gates=` declares the body's HOLD gates for the admin's resolve/deliver doors. |

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

**THE CARRY'S TYPE IS THE CONTRACT**: the declared carrier-type
(`carry_type=`, or derived from the `initial=` model instance) is
re-applied at the ONE point a carry reaches the body — at iteration 0
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
Its landed signature:

`@app.actor(fn=None, /, *, name=None, queue="default", max_attempts=3,
retry_kind="transient", capture_policy="none" | "errors-only" | "all" =
"errors-only", redact=None, max_concurrent=None, reservations=None,
result_ttl=None, priority=None, capacity_owner="operator")`

The capacity terms are the step surface's own (see the wiring verbs
table): `max_concurrent` the per-round damper, `reservations` the
strict slots (a `ConcurrencyReservation` list — a keyed-rate-limit
object here is the loud `ValueError` at the declaration site, never a
silently-dropped cap), `result_ttl` the cohort's in-DB result expiry.
`priority=` is the actor's declared claim-order DEFAULT for its nodes'
rows (the step's own `priority=` overrides it, the per-call override
overrides both — the precedence law in the wiring verbs table).
`capacity_owner=` is the stored-capacity ownership: `"operator"` (the
default) is the seed-only law — the operator owns the row thereafter, a
later literal change logs a mismatch, never writes; `"code"` hands
`max_concurrent`/`result_ttl` to the declaration — the boot sync
REWRITES them from the literal on every boot (the queue and
`max_pending` stay operator-owned under both owners).
`max_pending` is not on the surface (the engine's inserts bypass the
enqueue admission; a claim-side gate would wedge the backlog). The
declaration seeds the cohort's `actor_config` row and the
ONE-CAP-PER-ACTOR law
refuses two different declarations for one actor name.

## `wf.validate()` — the checker-independent validator

Runs in pytest, CI, and at worker boot; the TWENTY-SEVEN rules shipped (read
from `taskq/workflows/api/_validate.py`'s `_run_rules` — E1–E20, W1, the
two W2 faces, W3, W4, W5, and W6), each classified —
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
| `E12-deps-contract` | error | a body declaring the deps shape (one param beyond `ctx` + the wired sources — the DI capability's opt-in) where the app binds NO deps (`WorkflowApp(deps=…)` never ran), or whose declared deps type the bound instance does not satisfy — the message names the fix: bind ONE instance at the door, or drop the parameter (it is never a fourth data source) |
| `E13-gate-door` | error | a node's `gates=` carrying the channel's BOUND-DOOR object (`app.channel().gate(Model)`'s `TypedGate` — the runtime delivery handle) where the DECLARATION (`GateDecl(name=…, payload_models=…, timeout_s=…)`) belongs — the declaration is what the compile's hold nodes, the timeout warning and the delivery runtime read |
| `E14-gate-wiring` | error | the gate seat and the bodies' waits, walked against each other (E2's analog for the hold wiring), BOTH provable directions: a DECLARED gate whose body carries no `wait_signal` reference at all (the hold seat with no waiter), and a body's `wait_signal` with NO declared gate (the hold the compile's surfaces cannot see — the Mermaid hold nodes, W1's read, the admin's resolve/deliver doors). The conditional-interior wait is not statically provable — the loop-kind mis-index face is W5's |
| `E15-route-totality` | error | THE TOTALITY FENCE AT THE GRAPH LEVEL (T27): the typed route's arms walked against the source's declared union, BOTH provable directions — the arms' keys must cover the union's members EXACTLY (a MISSING member would route NOTHING — the silent drop the route exists to refuse: the skipped-both-arms run that terminalized succeeded-having-routed-nothing; an UNKNOWN member would never fire), and every arm body declares its arm's MODEL on the item param (the decode's target — a duck-typed arm (`dict`/unannotated/`Any`) consumes the element UNVALIDATED; the declared model must be the EXACT union member — a superclass passes the old issubclass fence and the runtime decode drops the subclass's own fields (`extra='ignore'`, the silent data loss); an unrelated model is the wiring promising data the arm cannot accept). The wiring verbs refuse the same breaches at the wiring site; this rule re-proves the fence from the compiled graph itself (public, mutable data — E3's precedent). The runtime door for the body that LIED about its union is `RouterNotTotal` |
| `E16-union-discriminator` | error/warning | THE CONSUMER DISCRIMINATOR LAW: a body param decoded as a UNION of models (the typed sum's consumer face — the join's downstream reads `list[ImageResult \| AudioResult]`) is decoded by the codec's left-to-right smart union — the FIRST member whose required fields the payload satisfies WINS. Picked by the reads (the members' declared fields are the only evidence): FIELD-IDENTICAL required faces are the hard refusal (the mispick is guaranteed and silent — every payload decodes as the first member); the one-directional SUBSET relation is the warning naming the mispick risk; distinct members (each requires a field the other lacks) are clean — the thinner validation fails, the union falls through to the true member. The fix either way: a `Literal` discriminator field on each member (`kind: Literal['image']`), so the union decodes by the tag, never by field-count luck |
| `E17-cache-route-source` | error | `cache=True` on a typed-route SOURCE — at the first cache hit the hit path cannot rebuild the route fork (the arm dispatch reads the element's runtime type; the cached envelope carries only encoded dicts), so the route's children and join never spawn and the run WEDGES FOREVER with the source reading `succeeded` wearing a `wf_cache_hit` receipt. Cache the route's arms' work, not the fork-spawning source |
| `E18-cache-ctx-input` | error | `cache=True` on a body reading `ctx.input` — the flow input never enters the address (the wired args do): the cached answer produced under one flow input would serve a LATER run whose input differs (the cross-tenant leak — A's answer delivered to B, terminal-success, audits clean). The fix it names: lift the flow input into the dataflow with an UNcached upstream step and wire its result as a param |
| `E19-cache-exit-body` | error | `cache=True` on an Exit-returning body — the sentinel terminalizes through the exit finalize, which the success-only store never sees: the cache silently no-ops (every run re-executes the body while the operator believes it caches) |
| `E20-cache-unverifiable-source` | error | `cache=True` on a body whose own source is not verifiable (a builtin, a partial, an exec'd fragment) — the address requires the §22.1 canon and the pre-refusal shape was the SILENTLY uncached step: the wiring accepted `cache=True`, every run re-executed the body, the only trace a log line. The refusal is deterministic (the claim path's hash reads the same source through the same seam) |
| `W6-actor-queue-cohort` | warning | THE DERIVED JOIN'S COHORT LAW (the docshelf adoption team's F3): a ROUTE attachment whose derived join (the verb's `queue=`, default `default`) differs from the SOURCE NODE's own queue under the SAME actor splits one actor name over two queues — `actor_config` is keyed by actor (one queue per actor), so a FLEET deployment's boot projection will refuse the graph (or strand its rows) at 3am. This rule reports the conflict AT THE WIRING instead (the W2 precedent's severity: probably wrong, never a refusal — the boot projection's one-queue law stays the refusal door). The remedy for a fleet deployment: DISTINCT actor names per queue (the arm's `RouteArm(actor=..., queue=...)`), or wire the route's `queue=` to the source's own. The plain map's explicit `queue=` (the doc-ingest guide's Shape-4 knob) is an explicit author placement, not the ambush — it stays silent here |
| `W1-eternal-wait` | warning | a gate with no declared timeout — "a workflow that waits forever on a human is a support ticket" |
| `W2-unknown-queue` | warning | a node projected onto a queue this app cannot see (the actor-not-found parking shape, named at validate) |
| `W2-join-for-progress` | warning | a join SUNK for display only — the DAG still blocks on it; declare the map's `aggregate=` fn instead |
| `W3-eternal-loop` | warning | a loop with no `until=` and neither wall set (`max_iterations`/`budget_s`) — a loop that can never stop on its own; declare the bound explicitly |
| `W4-gate-timeout-split` | warning | the double-sourced gate timeout cross-checked where it is cross-checkable: the `GateDecl.timeout_s` and the body's statically-readable literal wait `timeout_s=` DISAGREE — both values named in the message. The precedence: the WAIT SITE's value arms the runtime (the hold's expiry), the DECLARATION feeds the compile surfaces (the Mermaid face, W1) and never arms the runtime |
| `W5-loop-wait-shape` | warning | the loop's shape law, NAMED (never prose): a LOOP body whose `wait_signal` sits in a conditional interior mis-indexes the answer cursor (the row's iteration counter IS the cursor — an iteration that does not wait drifts the sequence). The unconditional wait is the shape kept — a plain STEP's conditional wait is the per-attempt cursor's honest replay |

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

- `create_flow(*, input=None, run_key=None, root_metadata_extra=None,
  priority=None) → RunClaim` — the run carries
  its INPUT on the root row (cut #7: cross-flow data never dies in a
  Python closure); bodies read `ctx.input`. `root_metadata_extra=` is
  extra keys stamped onto the root row's metadata in the SAME create
  transaction (the child flows' provenance pointers land atomically
  with the root — never a post-create UPDATE). `priority=` is the
  per-call claim-order override: EVERY row this create materializes
  lands at the value in the `jobs.priority` column, and the root's
  metadata stamps it (`wf_priority`) so the rows materialized later (the
  map/route children a worker forks) resolve the same override — the
  precedence: this override > the step's declaration > the actor's
  declaration > the column default `0`. The claim reports `created`
  (vs an existing run over the same `run_key`).
- `drive(flow_id, *, until="held" | "terminal", tick, max_ticks, execute=True) →
  str` — the driver (cut #10): no consumer hand-rolls a dispatch-poll
  loop; the drive is BOUNDED (`max_ticks` — a hang is a defect with no
  stack trace). **THE RETURN IS THE VERDICT — CHECK IT**: `"terminal"`
  (the flow row reached `succeeded`/`failed`/`cancelled`), `"held"`
  (only with `until="held"` — a node waits on a human/deadline), or
  `"max_ticks"` (the tick budget SPENT WITHOUT reaching `until` — not a
  status, a bounded give-up). A `"max_ticks"` return with rows still
  `running` means the run's work is not moving under THIS driver: the
  in-process drive runs NO lock-reclaim sweep (that is the worker
  maintenance leader's), so a killed process's `running` rows never
  re-pend through `drive(execute=True)` — the drive ticks to
  `max_ticks` silently and changes nothing. Resume a crashed run
  through the FLEET door (`execute=False` + a real worker — see the
  rate-limiting guide's doors note and the runbooks' mid-run crash
  page), not by re-driving in-process.
- `result(flow_id)` — the terminal node's result, DECODED (cuts
  #14/#19: never a raw jsonb string). A FAILED run's read raises the
  typed `WorkflowRunError` carrying the failing node's error class +
  message — the failure face is AT the read.

The packaged verbs (re-exported at `taskq.workflows`' top level — the
ID-ADDRESSED doors that need no `FlowRunner` in hand):

- `run(flow, pool, schema, *, input=None, key=None, until="terminal",
  max_ticks=5000, execute=True, deps=None, priority=None) →
  WorkflowRunResult` — the ONE-CALL door: create the run (*key* is the
  run key), drive it to `until`, and return the claim + the drive's
  outcome + the decoded result. `execute=True` (the default) runs the
  bodies in THIS process (the dev loop); `execute=False` is the
  orchestration-only pass — the fleet's workers execute the rows.
  `deps=` overrides the DI binding for this call (the app's bound
  instance is the default); `priority=` is the per-call claim-order
  override (the `create_flow` face above).
- `retry_workflow_node(pool, *, schema, flow_id, node_key, reason=None,
  principal=None) → int` — the ID-ADDRESSED manual resume (the
  ExitOps' own CAS statements for the caller holding no compiled
  definition): re-arms the node's terminal-FAILED rows, re-opens the
  run, and re-arms its exit-skipped branches — pending, the skip
  envelope cleared — in one transaction. A cancelled run REFUSES (the
  cancel was deliberate). Returns the number of NODE ROWS re-armed (0
  when nothing was resumable — a map's failed children share one step
  key, so one grant re-arms every failed row of the key at once); the
  action is audited (the row).
- `cancel_workflow_run(pool, *, schema, flow_id, reason=None,
  principal=None) → int` — the ID-ADDRESSED cancel: the flow flip is
  the linearization point, every non-terminal node + held signal
  resolves in the same snapshot, and the children's cancels follow
  iteratively (the worklist walk, never Python recursion). Idempotent
  (cancel twice = one cancel); "who cancelled this" is a ROW, not a
  log line. Returns the sum over every run the cascade visited (the
  root + each cascaded child) of that run's held signals cancelled
  plus one — an already-terminal run contributes 0 (the idempotent
  no-op): a family of two runs holding 3 and 1 signals returns 6; a
  terminal run returns 0.

The dispatch contract: bodies resolve from the REGISTERED DEFINITION
(the registry is the only body source); the claim is fenced; the
finalize is the engine's two-tx shape; a body exception ladders
(attempt failures emit NO terminal — P3 rule 7) with `retry_kind`
routing (`"permanent"` takes no ladder) and per-node `max_attempts`.
