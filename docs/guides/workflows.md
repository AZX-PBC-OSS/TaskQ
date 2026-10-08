# Workflows — the DAG engine (`taskq[flows]`)

The workflow engine turns a fan-out/fan-in shape into queue rows with a
counter and a ledger: fork N children atomically, join when the ledger
says they are done, fire exactly once, deliver the declared consumers.
Nothing here is a second task system — every workflow row IS a `jobs` row,
dispatched by the vanilla claim path, and every invariant below is pinned
by a test that can fail (the pin inventory at the foot of this page).

```python
import taskq.workflows as wf
```

THE IMPORT LAW: `import taskq` never imports this package. The only
sanctioned entry is `import taskq.workflows` by a caller that opted into
the `taskq[flows]` extra. The engine is core-deps-only (tors, asyncpg,
uuid_utils); the extra exists as the operator-facing opt-in marker.

## The join-wait representation

A joined node waits as `status='pending'` + `deps_pending > 0` (+
`metadata.blocking_reason='join'`). No new ENUM value: `pending` is legal
in the vanilla machine, and the dispatch claim's `AND deps_pending = 0`
exclusion keeps join-wait rows unclaimable. When the counter hits 0 the
row becomes claimable by the claim predicate — no status write.

## The two-transaction finalize

`finalize_node` = **tx1** then **tx2**:

* **tx1** — the result write, carrying the TERMINAL-MARK FENCE
  (`status='running' AND locked_by_worker=… AND attempt=… AND
  claim_epoch=…`). A row the reclaim flipped, or a fenced attempt,
  updates nothing. The fork's child INSERTs + edge rows + join node share
  tx1 (a kill at any statement boundary rolls the whole tx back; the
  reclaim re-forks). The step ledger's terminal write rides tx1 (the
  ledger-terminal-atomic rule), and the failure IO-capture rides tx1. The
  ledger's write keys the claim's OWN RETURNING id (or the full arbiter
  tuple with `COALESCE(map_index, -1)`) — a map child's outcome can never
  overwrite its sibling's.
  **tx2 runs ONLY when tx1's fenced UPDATE returned a row** (the ROWCOUNT
  GATE): 50 duplicate finalizes → 1 decrement.
* **tx2** — the guarded decrement: one atomic
  `UPDATE jobs SET deps_pending = deps_pending - 1 WHERE deps_pending > 0`
  over the edge ledger, with the flow-status EXISTS leg inside the
  statement; rows hitting 0 → the guarded fire → only the guard winner
  runs the reducer body (INSIDE tx2) and writes the outbox rows.

## The reducer body's boundary

A raising reducer rolls tx2 back — the decrement, the fire row and the
outbox rows all roll back — and the re-derivation re-fires: the body
RE-RUNS. **At-least-once body execution; exactly-once for DB-local
effects** (the ledger claim + the composite arbiter), the stated boundary.

The tx1→tx2 crash window keeps the same guarantee ACROSS PROCESSES: the
leader's maintenance sweep registers `wf_join_rederive`, which re-derives
the counter from the edge ledger, fires the firable joins and runs their
reducer bodies. The sweep heals schema-wide — ANY leader heals ANY flow's
join-wait rows — so the body must resolve in the HEALER'S process, never
the finalizer's memory. The resolution is therefore DURABLE:
`insert_flow_run` stamps the flow root's metadata with its workflow's
registered name (`metadata.workflow`), and the fire arm resolves the body
FROM THE REGISTERED DEFINITION via that name — the definition registry
every worker process carries (the same definitions imported fleet-wide;
D1's BODY-FROM-DEFINITION discipline). **The registry is the truth; the
process-local reducer memo is a cache** — it answers only for a flow the
registry cannot resolve (a root stamped before the stamp existed, an
anonymous flow), in the process whose finalize warmed it, and it never
shadows the definition. The cache is bounded: the phantom reaper drops a
terminal flow's entry at the reap (a terminal flow's joins can never
fire). A raising body there rolls the sweep pass back; the next tick
re-fires.

## The fan-in bound + the two join shapes (T07)

**The bound: ≤ 1000 declared parents per join** (`MAX_FAN_IN_PER_JOIN`) —
enforced at validate (`wf.validate()` / the validators): a join declaring
more is REFUSED, the error naming the bound and the child-driven
alternative.

THE HONEST DERIVATION (F7, restated — never a line through points that
don't share one): P1's three measured points — 200 → 14.9 ms, 1000 →
23.8 ms, 5000 → 39.3 ms — are sub-linear-in-log but NOT one line: an
endpoint fit (~5.1 µs/edge + ~14 ms base) predicts 19.1 ms at 1000
against the measured 23.8 — **the 1000 point is the outlier, named as
such** (least-squares gives ≈4.7 µs + 16.3 ms and still misses the
endpoints). The landed implementation's own refit
(`.measurements/edge-scale-curve.json` — the scale pin re-runs it) reads
**~1.7 µs/edge + ~2.7 ms base** (the artifact's refit block: marginal
1.684, base 2.705, the refit at the 1000 bound 4.39 ms, the outlier
residual −1.05 ms — the recorded points are 200 → 3.04 ms, 1000 →
3.34 ms, 5000 → 11.1 ms; the base-dominated criterion genuinely holds:
base 2.7 ms vs the 1000-edge marginal term ~1.7 ms), with the same
shape. THE GOVERNING BUDGET,
STATED HONESTLY: at the ~3-16 ms BASE term no fan-in meets a 5 ms-class
budget; the declared-edge cost is dominated by the BASE (paid once per
sweep pass regardless of fan-in), not the edges — the marginal edge cost
is µs-class. The bound's candidate therefore stands on the BASE-COST
argument (what the bound bounds is the PER-JOIN marginal work inside one
pass), NOT on the 5 ms-class comparison — that comparison was false and
is struck. **The refit procedure is the pin's own**: re-fit the curve on
the landed code, re-derive the marginal from the fit, and re-state the
argument against the refit numbers; the bound moves only by that argument
in review.

**The child-driven escape** (`JoinSpec(child_driven=True)`): past the
bound, the join does not trust the per-joined-row counter cache — the
fire counts its terminal children FROM THE EDGE LEDGER (each child's
finalize names its join target; the choice is RECORDED on the joined
node's `metadata.join_shape='child_driven'`). The exactly-once pins
(duplicate-finalize, double-fire PK) hold on BOTH shapes. OPERATIONAL
GUIDANCE: prefer partitioning the map over the escape — a fan-in past
32767 cannot even carry its count in the smallint counter cache (the
cache is a cache; the ledger is the truth — a >32767-unterminal recount
needs the counter's own widening before it can reconcile), so a genuinely
huge fan-in is a schema conversation, not a knob.

## The failed-parent propagation (T06)

A parent's TERMINAL failure — the retry ladder EXHAUSTED, or a
non-retryable class — resolves the joins counting it by the edges' DECLARED
failure policy, recorded on the edge ledger (`wf_edge.failure_policy`) at
fork/join time. The absorption is on the record; the derivation never
infers it.

**`fail_closed` (the default)** — the join fails CLOSED by a flow-scoped
transition set, ONE statement in the failing child's tx2:

* the joined node blocks: `metadata.blocking_reason='failed_parent'` +
  `metadata.failed_parent=<the failed node>` — its side of the counter is
  resolved by THIS stamp (the rederive arm locks `blocking_reason='join'`
  rows only), so the record never shows a hanging join;
* the running peers are PEER-CANCELLED: `status='cancelled'` with the
  cancel-origin marker `error_class='CancelledByPeerFailure'` (the `by`
  leg — the same outcome reads the same way whichever path produced it)
  and the structured record `metadata.peer_cancel =
  {"by": "peer_failure", "cascade_from": <the failed node>}`;
* the workflow fails: the flow root flips to `failed` (§17.2's cascade) —
  the linearization point every flow-status leg then reads.

The sweep composes: a child whose tx1 committed the terminal failure but
whose tx2 never ran (the crash window) is healed by the rederive arm's
`failed_required` count — the fail_closed join is blocked-with-reason by
the HEAL, never fired over the failed parent.

**THE D6 SENTENCE:** the peer-cascade fires only on TERMINAL failure — a
child mid-ladder does not trigger it (a ladder retry emits no terminal, P3
decision 7), so a fail-closed map with a long-ladder child keeps its peers
running until exhaustion. BY DESIGN, not an oversight: over-rejecting
(murdering peers on an attempt-failure) is the worse asymmetry; the peers'
work is real work the flow may still need.

**`collect`** — child failures do NOT cascade. Each child runs to its own
terminal; at exhaustion the failure fans in as the typed `FailureInfo`
item, APPENDED to the join row's `metadata.failures` array, and the join
FIRES with the typed partial result. `FailureInfo` EMBEDS the estate's
error envelope — never re-spelled: `ErrorInfo(error_class, error_message,
error_traceback)` (`taskq.backend._protocol`, its bound constants carried
as-is), with `attempts` / `node_key` / `map_index` as the workflow-only
extensions. `attempts` carries the FULL attempt history (one entry per
ladder attempt, every error payload). The fan-in + the fire ride the
terminalizing tx: the fire is strictly AFTER the last ladder attempt
(the ts-ordered pin). A SKIP fans in with ZERO ledger rows — a skip is
not an attempt. The workflow SUCCEEDS with the failure report (the
absorbed-failure clause in the status derivation,
`docs/guides/observability.md`); the failure detail's home is the
ledger/attempts — the join row's array is the bounded summary (the JSONB
size-cap policy, `docs/guides/maintenance-sweeps.md`).

## The retention coupling (T18)

The pruner is WORKFLOW-AWARE: the result-expiry sweep may not eat a map's
children before the join fires (a child's `result_expires_at` is held
while a join-wait node counts it — after the join fires, the subtree ages
together, §10.3), and retention may not prune a parent row of a
NON-terminal run (the sweep's ledger recount reads parent rows as
truth). Terminal runs prune on the normal schedule. The guards are
WHERE-class conditions on the EXISTING arms — no new sweep process; the
failure mode the guards prevent is Oban-Pro's documented
`preserve_workflows` lesson (the reduce reading holes — the silent
partial). See `docs/guides/maintenance-sweeps.md` §7.

**The JSONB size cap / compaction policy (GAPS-ESTATE D5's one home):**
the collect's FailureInfo list on the join row is BOUNDED — an append
that would exceed the byte cap (`FANIN_FAILURES_BYTE_CAP`, 64 KiB)
compacts the row to the bounded summary (the `"__truncated__": N` marker
counting the items no longer spelled + the ledger pointer); the FULL
detail stays on the ledger/attempts — the record never loses it. The
compacted row stays bounded forever (a later append increments the
marker).

## The three cancel legs

Cancel = one transaction — the flow flip is the linearization point; every
other statement re-checks flow status inside its own statement. THREE
legs, all shipped, any two insufficient:

1. the fire guard's flow-status leg (finalize tx2 and the sweep's fire);
2. the dispatch fence — the claim's candidate WHERE carries the
   flow-status EXISTS for workflow rows (a pending child of a cancelled
   flow is unclaimable; it refuses cleanly and the sweep re-derives);
3. the finalize fence (the terminal-mark CAS: worker + attempt +
   claim-epoch — the attempt is the fencing token).

## The step ledger + the run-key arbiter

The step ledger's claim is ONE round trip
(`INSERT … ON CONFLICT (flow_id, step_key, COALESCE(map_index, -1),
attempt) DO UPDATE … RETURNING`) — the ledger row IS the claim, the
arbiter physically blocks double-recording, and map children of one step
key are DIFFERENT claims. `ctx.step("name", fn)` is the user-facing
shape: default idempotent ON, `idempotent=False` opts a step out.

Run-level idempotency (`workflows.run(flow, input, key=…)`) claims against
the same composite arbiter with the scope `workflow-run:<flow name>` — a
conflict returns the EXISTING run's id + status, never a second silent
run, and two different flows sharing a naive key never collide (each
flow's keys namespace its own scope). Cron composition: the cron entry
fires the slot key as the run key — same slot twice → ONE run.

## The sweep arms (wired)

Three healing arms register in the leader's maintenance sweep loop
(the `_SweepSpec` tick table), gated on the backend's workflow capability
marker (`workflow_sweeps_capable` — the same `hasattr` seam every
maintenance sweep is admitted through, each capability its own marker; a
backend that implements only the legacy maintenance surface keeps the
arms off):

* `wf_join_rederive` — the lock-first re-derive (`FOR UPDATE SKIP LOCKED`
  the join-wait children, count un-terminal parents from the edge ledger,
  reconcile the cache, stamp orphans, fire the firable) + the set-based
  fire;
* `wf_outbox_drain` — the delivery half: consumer rows inserted
  idempotently on the composite arbiter, the undelivered flag flipped in
  the insert's transaction;
* `wf_phantom_reap` — 'running' ledger rows on terminal flows fenced (the
  rows-alone reconstruction reconciles).

The arms' imports stay inside the spec calls (nothing outside the package
may import `taskq.workflows` at module scope).

## THE WORKER EXECUTION — the queue is the runtime, in production

A workflow row IS a `jobs` row, and the vanilla claim path dispatches it
— but a claimed row is work only if the claiming worker can RESOLVE the
body. Three cures ship, one machinery:

**1. THE EXECUTION FENCE (the claim's capability leg).** A worker that
never imported the flow definitions cannot resolve any body — handing it
a workflow row buys a claim-snooze-claim loop. So the claim statement's
fence admits flow rows ONLY when THIS round's worker registered itself
capable: the workers row's `metadata.workflow_execution` boolean, stamped
at boot. A non-capable worker NEVER claims a flow row — the row waits for
a capable worker (the defined pre-deployment state; the stranded-jobs
detector surfaces a flow nobody can run). The fence short-circuits on the
`step_key` probe — a vanilla row's plan cost is one column test, the same
cost class the P3 fence's own comment pins (the zero-tax law: the vanilla
claim path is unchanged; the dispatch band stays green).

**2. THE BOOT PROJECTION (the F3 law's call site).** A worker whose
process imported at least one `WorkflowApp` boots CAPABLE: the projection
compiles every imported app's declared workflows (which is what populates
the D1 definition registry in the worker's process — the body resolution
answers from these very compiles) and projects the compiled graphs'
`(actor, queue)` cohorts into `actor_config` rows — the SAME carrier, the
same `sync_actor_config` surface, the same drift guards the vanilla
`@actor` refs ride. The dispatch capacity LATERAL sees the workflow
cohorts; the admin's actors page, the drift machinery and
`TASKQ_QUEUES_STRICT` see them too — no second, invisible actor
population.

**3. THE INTERCEPT (the claimed row's execution door).** A claimed row
carrying the flow lineage (`metadata.flow_id`) executes through the
WORKFLOW machinery, never the actor registry: the body resolves from the
registered definition (D1 — the flow root's stamped `metadata.workflow`
name, the same durable leg the leader's fire arm uses) and runs via the
runner's ledger-claim + finalize machinery — the same two-transaction
finalize, the same fences, the same ladder. NOT a second execution
semantics: the in-process driver and the worker's door differ only in WHO
CLAIMED (the runner's own claim stamps `claim_epoch` 0; the fleet's
dispatch claim carries the row's epoch — every fence reads the epoch it
fences against). A row whose stamped name resolves to nothing (the
definitions not imported anywhere — a deployment defect, or a hand-crafted
row) parks at the snooze cadence budget-free, `released_reason=
'workflow-body-unresolvable'`, the stranded-jobs detector the witness —
LOUD, never a silent wedge.

### THE SPLIT PLACEMENT — one actor, one queue (the design law)

`actor_config` is keyed by actor name: ONE queue per actor, the estate's
own law. A flow's heterogeneous placement (§9.1 — a source on `default`,
a chain on `gpu`) is therefore expressed with **distinct actor names per
queue** — the chain's gpu step is a gpu-named actor:

```python
CHAIN = Chain(
    name="gpu-chain", start="screen", steps={...},
    actor="wf-gpu", queue="gpu",   # the chain's steps: a gpu-NAMED actor
)

@app.workflow("ingest")
def ingest() -> object:
    src = chain_source(CHAIN, source_body, key="doc_source")  # actor 'wf', queue 'default'
    return build(src)
```

One actor name declared over TWO queues is a cohort conflict — refused
loudly (the boot refuses, the drift-guard precedent; the error names the
cure). The override-warning path (one queue silently winning) is dead by
construction.

### The orchestration-only drive

`drive(flow_id, execute=False)` is the fleet-mode driver: the driving
process claims and executes NOTHING — the tick runs only the sweep arms,
and the flow's work executes in the fleet's workers through the
queue-routed dispatch. The `execute=True` default is the dev-loop driver,
unchanged. Each node on ITS pool: a `default`-subscribed worker executes
the source, a `gpu`-subscribed worker the chain — the queue column is
load-bearing end to end.

## The build-time validators

A malformed graph is a coding error, refused before any row is written:
`validate_fork` refuses the empty fork (zero children owe a join that can
never fire); `validate_join_spec` refuses a join node with zero incoming
edges (the stranded invisible join — before the sweep's LEFT-JOIN
hardening it was not even diagnosable). The runtime backstop: the rederive
arm enumerates edge-less join-wait rows (a LEFT JOIN on the edge ledger)
and stamps them `metadata.blocking_reason='orphan_parent'` — the record
never looks healthy while the work is wrong.

`wf.validate()`'s own rule matrix (T09, checker-independent; attack-3's
M2-M5 cures included): **E1** acyclicity · **E2** produced-never-consumed ·
**E3** the edge-less join (the compiled graph is public data — the rule
owns the shape injected into it, not just the verb's door) · **E4** the
unannotated step · **E5** the incompatible consumer — including the
DUCK-TYPED hole (an unannotated/`Any` param consumes a model producer
unseen) · **E6** the fan-in bound · **E7** the cross-graph promise (a
promise wired from ANOTHER app's recorder — recorded by the verbs,
convicted here; the colliding-key smuggle builds a silently wrong edge) ·
**E8** the loop CARRIER-TYPE (a body refining an unrelated model against
the declared `carry=` — the recorded declaration is enforced) · **W1**
the eternal wait (the warning class) · **W2** the unknown queue (a queue
no actor declares and TASKQ_QUEUES does not name — the warning class;
the worker-boot fail-fast stays the runtime door).

## The pin inventory

Every invariant above is pinned by a test that can fail, red-first: each
pin's red is a REAL mutation of the shipped engine (a guard flipped, a
statement mutated, a resolver swapped), observed and recorded to
`.measurements/` — never a local re-implementation of the dragon. The
families:

| file | family |
| --- | --- |
| `tests/test_wf_schema_migration.py` | the schema round: the three-file lock-class split, the partial-index doctrine, the import law, the dispatch exclusion |
| `tests/test_wf_finalize_pins.py` | the finalize family: the deps fingerprint (pin 1), the rowcount gate (6), the attempt fence (13), the gremlin guards (23: the `>= 0` flip; 24: the flow-status leg) |
| `tests/test_wf_sweep_pins.py` | the sweep family: the post-cancel fire refusal (2+5), the misnamed child (8), held-row exclusivity (4), the phantom reaper (15), the empty join (17), the dispatch fence (2's claim leg), the sweep-arm wiring (21), the bounded reducer cache (22) |
| `tests/test_wf_fork_pins.py` | the fork family: the id-collision (18), fork atomicity (19), the outbox drain exactly-once (20) |
| `tests/test_workflows_ledger_pins.py` | the ledger family: the double-run (1), the lost-completion window (2), the concurrent claim (3), the run-key replay (4), the claim-atomic window (5), the terminal-atomic split-write (6), the map-children arbiter (7) |
| `tests/test_wf_engine_units.py` | the in-process pins: the seam-only generation (10), redact-before-persist (11), the canonical hash (12), the deadlock budget (14), body-from-definition (16) |
| `tests/test_wf_propagation_pins.py` | the T06 propagation family: the stranded join (pin 1), the peer-cancel record (pin 2), the collect exhaustion fan-in + the ts ordering (pin 3), the skip's zero ledger rows (pin 4), the mid-ladder composition (pin 5), the sweep's crash-window heal (pin 6), the policy validator (pin 7) |
| `tests/typeprobe/` | T01's negative type probes (pyright + ty, the CI `type-probes` gate) |
| `tests/test_wf_perf_bands.py` | the perf bands: the 1000-child fan-out tx, the join-fire latency, the enqueue/dispatch noise bands, the T07 edge-join scale curve (the refit + the 100k-edge plan assert) |

History note: T03's schema-pin file (`tests/test_workflows_schema_pins.py`)
was folded when T05 landed — its pins live in
`tests/test_wf_schema_migration.py` (the schema/lock-class/import families)
and `tests/test_wf_perf_bands.py` (the T03 bands); no pin was dropped.

| `tests/test_wf_t20_maintain_liveness.py` | T20's core fix: the failed arm gates on live UNRESOLVED work (the 149-siblings scenario red-first; the H1 wedge-cure-stands regression) |
| `tests/test_wf_t20_emit_pins.py` | T20's emit tx: the kill-storm at every statement window (zero re-emitted, zero lost), the between-pages kill, the zombie fence, the map_index discipline |
| `tests/test_wf_t20_fence_probe.py` | T20's fences: the 30-sweep premature-terminal probe (no worker — nothing terminalizes), the mutation drill's teeth, the source-terminal subject |
| `tests/test_wf_t20_router_pins.py` | T20's router: both totality doors + the flips, the route through the certified fork, the end-to-end author surface, the loud refusal's row |

## The ergonomic contract (T17 — the authoring session's paper cuts)

The API's bar is **"first-try correct, no boilerplate, IDE
autocompletion resolves the wiring."** The authoring session's 20 paper
cuts (2 BLOCKER / 5 CRITICAL / 6 friction / 7 nit) are standing law; each
cure's CONTRACT, and where to read it:

| Paper cut (the stumble) | The cure's contract | Landed in |
| --- | --- | --- |
| "Nothing runs after the join" | A join's user reducer body is spelled **in the wiring** — the fan-in node IS the reducer; its consumers dispatch as normal steps | flow API (T09) |
| "No sequencing — a depth-3 DAG didn't fit one flow" | Sequencing is dataflow: consume a node's promise downstream and it dispatches strictly after; nested maps compile | flow API (T09) |
| "`deliver()` dropped my payload" | The signal row CARRIES the payload; the resumed body reads it | HITL (T10) |
| "A second hold on the same name exploded" | A new hold mints a NEW epoch — the identity is the hold epoch, not the name; the same signal can hold again | HITL (T10) |
| "The guard was decided at create time" | Skip guards are evaluated **at dispatch**, against the flow's state | flow API (T09) |
| "My holds ate the retry budget" | RESUME-NOT-RETRY: a hold's resume consumes NO ladder attempts — the ladder counts failures only | HITL (T10) |
| "The parent came from splitting a string" | The parent is a COLUMN; bad node-key spelling is refused at compile, never mis-derived (this engine — verified) | core (T03/T04) |
| "`create_flow` took no input" | The run carries its input; bodies read `ctx.input` | flow API (T09) |
| "Everything was a dict at the boundary" | Bodies are annotated; the compile reads the annotations; an unannotated actor is a compile error | flow API (T09) |
| "The collect result was raw rows" | Fan-in bodies receive DECODED results; a skipped child arrives as a typed absorbed item — never silently missing | flow API (T09) |
| "No `drive(until=…)`" | The driver takes `until="held" \| "terminal"` — no hand-rolled dispatch-poll loops | loops (T19) |
| "The ladder laddered everything" | Each node takes `retry=` (the transient/permanent classifier) + `max_attempts` | flow API (T09) |
| "jsonb decoded differently on every path" | Read paths decode once, through the estate's JSON seam | flow API (T09) |
| "No way to read the flow's answer" | The flow result surface: by id, decoded | flow API (T09) |

The two spike-hygiene nits (a vestigial join arg; a module-constant
SCHEMA) have no repo counterpart — the engine carries the parent as a
column, the registry is instance-owned, and the schema is
settings-driven (`TaskQSettings.schema_name`, validated at render).

The full disposition ledger — every cut, its confirmed severity, its
disposition (applied 15 / declined-with-reason 1 / recorded 2 /
verify-absent 2 / deferred 1), and the re-test that proves it closed —
is `.measurements/t17-dispositions.md`; the session's own ledger
(append-only) carries the same table.

## §1 — The flow API: the concept (T09)

`taskq.workflows` now ships the authoring surface: **plain types for
data, `Promise[T]` for wiring**. A workflow is a build function — SYNC
and PURE — that SPELLS the graph by dataflow:

- a promise consumed downstream is an EDGE (the consumer dispatches
  strictly after the producer terminalizes);
- a call with SEVERAL promise arguments is the FAN-IN join — and its
  user body IS that node's body, run inside the join's finalize
  transaction with the DECODED parent results (cut #1's cure: the
  reducer is in the wiring, not engine code; its result cascades
  downstream as NORMAL steps);
- `map_source(source, body)` is the map: the source's finalize forks N
  children (fresh jobs — per-item ledger identity), the join collects,
  and the promise is the FLAT `Promise[list[R]]` (never
  `Promise[list[Promise[R]]]` — the checker rejected the nested shape);
- `sink(...)` is the explicit fire-and-forget (RECORDED in the compiled
  metadata — never silent); `build(result, *residuals)` is the terminal
  completeness point (the `Promise[Never]` residuals are the static
  side of E2's produced-never-consumed rule).

Nothing about the tree is STORED: the graph is spelled by the wiring
and compiled fresh — same module → same compile, byte-stable (the
Mermaid golden's law). Sequencing, hence a depth-N DAG, is dataflow —
cut #2's cure: the migration's depth-3 graph fits ONE flow.

The dispatch-time predicates (cut #4's cure): `step(..., skip=pred)` —
`pred: bool | Callable[[state], bool]` — is evaluated WHEN THE NODE
DISPATCHES against the flow's state (`{"input": …, "results": {…}}`), a
sibling's COMPLETED result decides it. A skipped child SUCCEEDS WITH
THE RECORD (the result names the skip — the envelope never lies about
what ran) and fans into its absorbing joins (collect | maybe) as a
typed item — a skip is not an attempt (zero ledger rows).

The typed boundary (cut #8's cure): the body's param annotations are
the payload codec — the runner re-validates the jsonb round-trip into
the DECLARED model; a body sees the type it declared, never a raw
dict. `wf.validate()` refuses an unannotated body (E4 — the annotation
IS the wiring).

**THE TYPED EARLY-EXIT — `Exit[DoneT]`** (§17.1, the None-end's
sanctioned escape): a body returns `Exit(payload)` to TERMINAL-SUCCEED
the node with a typed result NOW — the runner unwraps the sentinel, the
node's ledger terminal records the exit, and every non-terminal
DOWNSTREAM node is marked SKIPPED-WITH-THE-RECORD
(`{"skipped": true, "exit_from": …}` — the record never lies about the
nodes that didn't get to run; zero ledger rows, a skip is not an
attempt). All rows terminal → the derivation reports the workflow
COMPLETE. A plain `T` return is the DATA result — sentinels appear only
where the body's control flow says so; a body annotated `-> Exit[Report]`
returning a bare value (or a bare `return`) is the checkers' MUST_ERROR.

**THE MANUAL RESUME — `FlowRunner.retry_node(run_id, node_key)`** (§17.2,
the §22.6 redispatch row 3 — "the ladder then manual"): the operator's
audited, CAS-guarded re-arm of a TERMINAL-FAILED node. The attempt
ordinal CONTINUES (never resets — the ladder's own 'failed' count is
the budget, so each manual retry buys exactly ONE more attempt); the
re-run is safe by the step ledger (`ctx.step` returns recorded
results); the cascade's blocked closure re-opens (the stamps return to
join-wait — the sweep's re-derive re-derives them, a stamp is the
cache); a terminal-FAILED flow root returns to `running` (a CANCELLED
root stays closed — the cancel was deliberate). "Who retried this" is a
ROW (`workflow.retry_node`). Surfaced as the `taskq flows retry` verb
(T12).

The retry knob (cut #12's cure): `step(..., retry_kind=..., max_attempts=...)`
— `retry_kind="permanent"` takes NO ladder; a transient failure's
attempts re-pend with backoff and emit NO terminal until exhaustion
(P3 rule 7), then T06's propagation takes over.

The reads (cuts #14/#19's cures): `FlowRunner.result(flow_id)` — the
terminal's result, DECODED ONCE through the estate's JSON seam; the
driver `drive(flow_id, until="held" | "terminal")` (cut #10's cure —
landed with T19's loop; the bound `max_ticks` is the hang's fence).

## §2 — The type contract (T01's two tables)

The wiring's typing story has TWO faces, and both are load-bearing:

1. **The checker face** (static): the plain-function bodies are
   checker-typed end to end; the negative probes live in
   `tests/typeprobe/` (pyright 1.1.414 + ty 0.0.85, the CI
   `type-probes` gate). The checker is ABSENT at runtime — which is
   exactly why the second face exists.
2. **The validator face** (runtime, checker-independent): `wf.validate()`
   re-proves the wiring's totality from the compiled graph (the rules
   table in the API reference). A graph that typechecks AND validates
   is pinned clean by BOTH; a mutation of either face reds its own pin.

### THE TWO FACES — what each one owns (the honest boundary)

The mechanism shipped in the verbs is probe-proven (the type-mechanism
corpus, `wf_generic_step_negative_types.py`, on both pinned checkers):
`Promise` is COVARIANT; `step[R]` infers `R` from the body's declared
return (`step(fetch, params)` wires a `Promise[Report]` when `fetch`
returns `Report`); `gather[R]` preserves the element type
(`Promise[list[R]]`, never an erase to `Promise[Any]`); `build`'s
residual slot is `Promise[Never]` — covariant, so every REAL data
handle in the slot reds. **But the two faces own different questions,
and neither covers the other's:**

| the edge's question | WHO catches it | WHERE |
| --- | --- | --- |
| the handle flow: an unconsumed data handle passed to `build` as a residual (`Promise[Report]` in the `Promise[Never]` slot) | **THE CHECKER, at typing** | `build(...)` call — red in the IDE, red in CI (`reportArgumentType` / `invalid-argument-type`) |
| the handle flow: a consumer that DECLARES the handle type (`Promise[Config]`) handed the wrong handle (`Promise[Report]`) — the contravariant-consumer rejection | **THE CHECKER, at typing** (live because `Promise` is covariant) | the handle-passing call site |
| the handle flow: `gather`'s element type threaded to a typed join consumer (`Promise[list[Report]]` where `Promise[list[Config]]` is declared) | **THE CHECKER, at typing** | the join-consumer call site |
| the promise-as-data mistake (a promise passed where the DECODED model / list is wanted — the direct unit-call form) | **THE CHECKER, at typing** (the corpus's `wf_api_negative_types.py`) | the unit-call site |
| the DECODED-payload compat: a producer's body returns `Report`, a consumer's body declares `Config` | **`validate()`, at build** — `E5-incompatible-consumer` | `compiled.validate()` / worker boot |
| the DECODED-payload hole: the consumer's param carries NO model annotation (`Any` / a plain dict — the duck-shaped hole) | **`validate()`, at build** — `E5-incompatible-consumer` | `compiled.validate()` / worker boot |
| a promise nobody consumes, nobody sunk, nobody named terminal (not passed to `build` at all) | **`validate()`, at build** — `E2-produced-never-consumed` | `compiled.validate()` / worker boot |

The boundary has a REASON, not just a history: the bodies take DECODED
payloads, not handles (cut #14's decode-once — the fan-in's args are the
parents' decoded results). The wiring verbs' `*args` are therefore
ERASED (`object`) — statically they are either promise handles (the
handle flow, the checker's face) or plain data, and the checker cannot
see a decoded model inside an erased argument list. That is why the
canonical wrong graph "producer `Report` → consumer wanting `Config`"
red at the WIRING SITE (`step(consume, produced_promise)`) is NOT a
checker error — the wiring call is erased — and IS a validate() refusal
(`E5`) the moment the graph compiles. The corpus asserts this boundary
as its own probe (`probe_wiring_site_decoded_payload_is_the_validators_face`):
the line must stay CLEAN under both checkers, and the gate's
unmarked-error rule reds the corpus if it ever reddens. What the IDE
catches at typing is the HANDLE flow; what `validate()` catches at build
is the DECODED-payload compat. Read the gate's red as the handle-flow
law; read validate()'s refusal as the payload law; neither report
subsumes the other.

## §3 — Fan-out & reduce: the one-flow shape

The common pipeline — fan-out N independent steps, reduce into a join,
cascade downstream — is spelled (never glued):

```python
from pydantic import BaseModel

from taskq.workflows import StepContext, WorkflowApp, build, step

app = WorkflowApp()


class Ingest(BaseModel):
    doc_id: str


class Stats(BaseModel):
    n: int


@app.actor(queue="cpu")
async def stage_a(ctx: StepContext, params: Ingest) -> Stats:
    return Stats(n=1)


@app.actor(queue="cpu")
async def stage_b(ctx: StepContext, params: Ingest) -> Stats:
    return Stats(n=2)


@app.actor(queue="cpu")
async def reduce(ctx: StepContext, a: Stats, b: Stats) -> Stats:  # the join's user body — the DECODED parents
    return Stats(n=a.n + b.n)


@app.actor(queue="cpu")
async def tail(ctx: StepContext, total: Stats) -> Stats:
    return total


@app.workflow("doc_ingest")
def doc_ingest() -> object:
    a = step(stage_a, Ingest(doc_id="d1"), key="a")
    b = step(stage_b, Ingest(doc_id="d1"), key="b")
    reducer = step(reduce, a, b, key="reducer")  # the fan-in: TWO parents
    return build(step(tail, reducer, key="tail"))  # the cascade: a NORMAL step


compiled = app.get("doc_ingest")
compiled.validate()  # the zero-false-positive bar: this graph is clean
```

The fan-in's failure policy is declared ON the join
(`on_failure="fail_closed" | "collect"` — T06's duality: a failed
parent either fails the join closed (the cascade, the peers
peer-cancelled) or fans in as a typed `FailureInfo` item and the join
FIRES with the typed partial). The fan-in bound is 1000 declared
parents (`MAX_FAN_IN_PER_JOIN`) — past it, partition the map.

For a RUNTIME-determined N: `map_source(source, item_body)` — the
source's body returns the list, the fork spawns one fresh job per item
(map_index = the item's ledger identity), and the join packs the
decoded item results for the downstream body.

## §9 — Loops & back-edges (T19)

`wf.loop(name, body, carry, until, max_iterations, budget,
on_exhausted)` — the loop is v1 (the maintainer's ruling: *"you do not
cut must haves"*). Each iteration is FRESH jobs: the iteration-scoped
step keys `(workflow, loop_key, iteration, step)` keep the idempotency
ledger per-iteration (T05's contract unchanged). The body returns the
CONTROL UNION:

- `Done(payload)` — the loop stops; the payload is the loop's result.
- `Refine(feedback)` — the carry threads into the next iteration.

**THE CARRY IS FROZEN AT SPAWN** and advanced EXACTLY ONCE per
iteration — in the ADVANCE STATEMENT, one atomic write shared with THE
CAP GUARD (a refused advance IS the exhaustion). A carry advanced at
hold/retry time is the optimistic-apply dragon (double-apply /
lost-apply) — the pin keeps it red forever. Crash recovery: a resume
reads the iteration's LEDGER row (the memo) — the body is not
re-consulted mid-iteration without the at-least-once boundary being
stated.

**THE TWO WALLS ARE DIFFERENT** (the spike's cut 4):

- the **iteration cap** (`max_iterations`) bounds TOTAL SPAWNS
  regardless of time — the only wall left when the budget is paused;
- the **budget** (`budget_s`) is the TIME wall — and it is BLIND while
  the loop holds on a human (`budget_paused`): a held loop is invisible
  to the budget sweep even when its deadline is forced into the past.
  THE CONSUME-BUDGET DRAGON (the consume alternative — the arm reads
  the budget instead of pausing) killed a held loop mid-hold and
  refused the operator's later approval: work silently lost. It is kept
  RED forever by the mutation drill (`.measurements/t19-pin-reds.json`).
  (The operator's OWN latency between two holds, though — that time the
  budget DOES consume once the pause lifts: the wall measures the loop's
  working time including the gaps, never the holds themselves.)

**EXHAUSTION IS NAMED, NEVER SILENT**: the cap or the budget wall
terminates the loop into the `iteration_cap_exhausted` /
`budget_exhausted` state — and THE FLOW TERMINALIZES IN THE SAME
TRANSACTION (STRANDED-FLOW: the spike's `_loop_advance` left the flow
`running` forever — the worker ticks forever, the admin shows a live
run; the pin convicts it).

**THE EXHAUSTION POLICY IS READ — BY THE DRIVER AND THE SWEEP** (the
attack-3 H1 cure): `on_exhausted="escalate"` enqueues the escalation
through the SAME outbox the fired joins use — addressed to the
workflow's REGISTERED escalation step (`loop.escalation`; the body is
your `escalates_to=fn`, or the framework default, registered at compile
by the definition registry — D1), and the escalation consumer job RUNS:
the drained job resolves the registered body and terminal-succeeds
carrying the exhaustion record — no dead letter, on EITHER path (the
live driver's cap check AND the sweep's orphaned-loop arm). DECLARATION
ORDER DECIDES NOTHING in the payload door — by shape.
`on_exhausted="fail"` terminal-fails the flow and enqueues NOTHING.

**THE CAP LETS THE FINAL ITERATION RUN** (the attack-3 vacuous-pin
cure): the advance guard admits the advance TO the cap, so the
metadata's counter REACHES `max_iterations` — the live driver exhausts
at its top-of-loop cap check, and the crash window (a worker death
after the final advance) leaves exactly that state on the running row
for the SWEEP's cap arm to own. The sweep's
`iteration >= max_iterations` predicate is REACHABLE in production —
never a hand-crafted state.

**THE SEMANTICS DECISION, STATED ONCE — BY ESCAPE POINT**: infra fault
≠ body failure, and the classifier reads WHERE the error escaped, never
merely its type. An exception the BODY raised is a BODY failure even
when it wears a `ConnectionError` face — the loop's typed failure
(`LoopBodyFailure`, the flow terminalized); a body cannot forge an
infra fault (the poison-body wedge — 20 crashed rows, a `running` flow
forever — is the convicted variant, kept red by the attack probe). An
infra fault raised by the DRIVER'S OWN machinery reclaims: the ledger
says `crashed`, the ladder does NOT burn, the lease machinery re-claims
from the ledger, the loop completes on the re-claim.

**`until=` is AWAITED** (`Callable[[], Awaitable[bool]]`): a bare sync
closure returning a coroutine object is TRUTHY — the spike's cancel
test cancelled an IDLE flow, proving nothing. The predicate is awaited
by the driver, per iteration.

**NAIVE-MEMO IS AUTHOR GUIDANCE, PINNED** (the spike's cut 7): the
durable memo — one model invocation per iteration, never one per
resume — is the ONE-TX shape (read + write inside one transaction; the
ledger's claim statements ARE that shape). The engine gives you the
carry + the TX boundary; the memo shape is author work — the negative
example (a read-modify-write across transactions: a crash between
"model ran" and "proposal committed" strands the counter, the re-run
UniqueViolations, the ladder then fails the loop) is the shape to
never write.

The driver (`drive(flow_id, until="held" | "terminal")`, cut #10's
cure) is bounded — `max_ticks` fences the hang.

## §10 — The streaming source & the chain (T20)

A "source sync" — a paged upstream whose run executes a body that is a
**paged generator**: fetch page → `ctx.emit_batch(children, cursor=…)`
→ checkpoint the page cursor → fetch the next page. Pages stream: each
page's records are **chain-start rows** that claim and terminalize WHILE
the source is still mid-stream (the spike measured 236 chain claims +
165 chain terminals before the last page's emit) — nothing waits for
the pager to exhaust.

### The emit tx — the one new primitive

`ctx.emit_batch(children, cursor=…)` is ONE transaction:

1. the **children INSERT** — the certified fork-children shape
   (parallel-array, app-side uuid7 ids), parent = the source row;
2. the **edge rows** — child → source, `fail_closed` (a mid-stream
   source death is the honest fail-closed parent; the edges reconcile
   emit debt exactly as the fork's reconcile fork debt);
3. the **cursor checkpoint** — the source row's OWN metadata (the
   `emit_cursor` key; no new table, no new column), guarded by the FULL
   dispatch fence (`status='running' AND locked_by_worker AND attempt
   AND claim_epoch`).

**Why one tx** (the fork-atomicity law at page granularity): a kill at
ANY statement window — including *after* the cursor write but *before*
the commit — rolls the whole batch back; the REAL reclaim
(`sweep_expired_locks`) + the promotion sweep re-pend the source (it
never finalized), and the re-claim re-emits exactly the lost page. The
cursor is never advanced outside the emit tx. The pins kill the emit at
every window with a real `pg_terminate_backend` and count: zero
re-emitted children, zero lost children, the cursor == the last
committed page.

**The fence needs nothing new**: with no fan-in there is no join to
fire early — the successor hazard is the PREMATURE TERMINAL (the run
finalizing while work is live), and the fence for it is the shipped
rows-only derivation itself: the root finalizes only when every row is
terminal (or resolved-blocked). A derivation cannot fire early because
it is not a fire at all. The 30-sweep probe (a mid-stream death, then
30 hard sweep passes with NO worker) holds: nothing terminalizes,
`firable=0`, zero join-wait rows — the join machinery is inert in this
design.

### THE REFUTED-CLAIM DISCIPLINE: `map_index` is load-bearing

The per-record identity rides **`map_index`, stamped at emit** (with the
record's `trace_id`). The certified fork's idempotency key
(`wf:{flow}:emit:{map_index}:{step}`) and the step-ledger's claim
arbiter (`(flow, step_key, COALESCE(map_index,-1), attempt)`) BOTH
discriminate siblings by it — the spike's first run emitted 200
applications without it and observed **198 UniqueViolations**: every
record's `enrich` child collided onto one row. `EmitChild` therefore
REQUIRES both stamps (a child without them is refused before any row
exists), and `chain_start` carries them forward. Every row of one
record's chain shares its `trace_id` — the operator's drill-down is one
query.

### The router: conditional edges, total or refused

```python no-exec — not executed: fragment, the author's bodies (screen_app, enrich_app, score_app, manual_review) are the workflow's own
CHAIN = Chain(
    name="application-enrichment",
    start="screen",
    steps={
        "screen": Step(
            body=screen_app,
            outcomes=ScreenOutcome,
            route=Route({ScreenOutcome.CLEAN: "enrich", ScreenOutcome.FLAGGED: "manual_review"}),
        ),
        "enrich": Step(
            body=enrich_app,
            outcomes=EnrichOutcome,
            route=Route({EnrichOutcome.OK: "score", EnrichOutcome.SPARSE: DONE}),
        ),
        "score": Step(body=score_app, outcomes=ScoreOutcome, route=Route({ScoreOutcome.OK: DONE})),
        "manual_review": Step(
            body=manual_review,
            outcomes=ReviewOutcome,
            route=Route({ReviewOutcome.APPROVE: DONE, ReviewOutcome.REJECT: DONE}),
        ),
    },
)


# the SOURCE is the paged generator — each yield = ONE emit tx:
@app.workflow("application_sync")
def application_sync() -> object:
    return build(chain_source(CHAIN, source_body, key="source"))
```

The bodies are ordinary typed-outcome coroutines
(`async def screen_app(ctx: StepContext, item) -> ScreenOutcome`); the router's
decision is the body's return. Totality is the fence, at two doors:

1. **Declaration time** — `Chain` refuses a route that is not total
   over its step's outcome enum: `chain step 'screen': route is not
   total over ScreenOutcome — missing ['flagged'] … the outcome it
   drops would silently strand a record's chain.` A route to a
   non-step key, and a non-step start, are the same door's refusals.
2. **Run time** — `RouterNotTotal`: an outcome with no arm fails the
   step LOUDLY with `error_class='RouterNotTotal'` — the record names
   the defect; the record's chain visibly dies, never silently drops,
   and the run's derivation cannot claim success over it.

The chain is declared ONCE and instantiates per record through the
certified fork-at-finalize machinery: each chain step's finalize forks
AT MOST ONE child (the route's arm — no fan-in, no join), the record's
payload, `map_index` and trace riding forward.

### Chain or DAG? (the one-paragraph decision guide)

**The chain is the per-record stream** — a route arm per outcome, at
most one child per step, no fan-in, no join; pick it when every record
flows ALONG ONE PATH and the arms are the branching. **The DAG is the
shared shape** — `step`/`gather`/`map_source` wiring (§3) with real
fan-in joins: pick it when independent results CONVERGE (a reducer
consuming two parents, a `gather`'s all-upstream join, a map's
collected join). The wrong guess costs a restructuring, not a
migration: a fan-in spelled as a chain has NOWHERE to put the second
parent (the chain's finalize forks at most one child — the convergence
is unwritable), and a route spelled as a DAG arm drags the whole graph
into the per-record stream where the shared joins never fire. The two
compose — the chain's records fan OUT of a `chain_source` (§10's emit
tx) into any downstream DAG node, and a chain step's body can be the
consumer of an upstream `gather`: the stream is the row source, the
graph is the convergence.

### The partial-success run (the envelope must not lie)

With no fan-in there is no absorbing edge, so the honest run vocabulary
stays the shipped one: the run **`failed`** when any chain failed
(`UnabsorbedNodeFailure` on the root — the alert hook), **`succeeded`**
when none did. The run's REPORT is the read-side rollup over the rows
(per-trace `bool_or(status='failed')` — `{failed_chains: 7, total: 200}`
is one query), never a new engine state. A "partial" derived status
would be a new primitive (a new terminal class, a new render rule across
the explorer/admin/API surface) rejected: the report reads the same
truth from the rows without it. Partial success as an AUTHORED ROUTE is
the ergonomic answer: route the degraded outcome to a fallback step and
the run succeeds with the degraded records visibly on their own route.

### THE MIGRATION PATH: from the actor's ad-hoc children to the chain

The TaskQ-native pattern this replaces — an actor body dispatching its
own child jobs ad hoc (the hand-rolled "spawn a follow-up job per
record, then a counter row to know when they're done") — maps onto the
chain surface:

| the ad-hoc actor pattern | the flow's shape | what it buys |
| --- | --- | --- |
| `ctx`/client child dispatch mid-body | the source's `ctx.emit_batch` per page (the children + edges + cursor, ONE tx) | the fork-atomicity law at page granularity: a crash rolls the page back WHOLE and the resume re-emits exactly the lost page — the ad-hoc pattern's window (children enqueued, cursor lost, or the inverse) is structurally gone |
| the hand-rolled "done counter" row / the polling parent | nothing — the run root derives from the ROWS (the maintenance leg) | no counter to corrupt, no polling job, no wedged "waiting on children" state: the root's terminal IS the derivation |
| ad-hoc retry/backoff per child | the certified retry ladder (the row's own curve) | the ledger carries the full per-attempt history; the reclaim owns crashes |
| the follow-up job's invisible dependency | the chain's declared route (the fork's edge row) | the lineage is QUERYABLE: `trace_id` per record, `map_index` per record, `parent_id` per row — the ad-hoc pattern's children have no lineage and no completion semantics |
| the payload re-marshalled into each child | the record rides the row's payload verbatim (the fork carries it) | the drill-down reads one record's whole chain by its trace |

**THE LINEAGE LAW**: every row a source/chain writes carries
`flow_id` (the run), `trace_id` (the record), `map_index` (the record's
index — the discriminator) and `parent_id` (the emitting row). The
migration BUYS the lineage and the completion semantics and PAYS the
declared-edges discipline: the children are the fork's (an edge row
each, the ledger's truth), the completions are the derivation's — there
is no ad-hoc escape hatch, and a child dispatched outside the fork is a
foreign row no derivation counts. The honest boundary: an actor's
ad-hoc children that must remain in the NATIVE queue (no flow) keep
their no-lineage semantics — the migration is opt-in per workload, not
a fleet conversion.

The bands (measured on the built code, the spike's §7 protocol): the
emit tx stays single-digit-to-low-teens ms per page (a 40-record page:
children + edges + cursor in ONE tx), and the chain-shaped dispatch
backlog costs the same ms-class as plain jobs at 200 chains — the
streaming source adds ONE bounded tx per page, not per record. See
`perf-evidence-workflows-streaming.md`.

## §4 — HITL: humans are rows (T10)

`ctx.wait_signal((Approval, Escalate), timeout_s=…, tool=…, args=…,
reason=…)` — THE TYPED WAIT. The tuple form is the typed wait (PEP 604
unions in value position carry no static payload information — the
return type is the union and narrows with `isinstance`); the
single-payload form is the one-member overload.

**THE RESUME CONTRACT, stated as a feature** (cut #18's disposition):
the body re-executes **FROM THE TOP** on resume — there is NO
determinism requirement on the body; pre-wait side effects are
`ctx.step`-ledgered and replay cheap. The delivered holds are the
node's ANSWER QUEUE: each attempt's wait sequence consumes them in
epoch order (the per-attempt cursor) — a RETRY replays the answers (the
operator never re-answers); a wait past the queue's end registers a NEW
hold (a NEW epoch — the multi-hold: the same signal name can hold
again, the chained gate works because the waits are sequential).

**THE PAYLOAD RIDES THE ROW** (cut #3's cure): the human's answer IS
the row's payload; `ctx.signal(name)` reads it on resume (the read
before delivery is the typed `SignalUnavailableError` — never a silent
None).

**THE REPLY HANDLE**: every hold's `id` (uuid7, time-ordered) is THE
reference — `HitlClient.resolve(id, decision)` addresses ONE hold; the
`(node, signal, epoch)` identity disambiguates MULTIPLE holds. The
resolve is IDEMPOTENT (an already-resolved hold is the DEFINED no-op)
and AUDITED (G4: "who resolved this" is a ROW, not a log line).

**THE CONTEXT CONTRACT** (round-8): the hold carries the payload-model
schema (what a UI renders from), the author-supplied
reason/tool/args, the provenance (run id, node key, gate name, epoch,
created_at, the deadline). **THE REDACT LAW EXTENDS — BEFORE PERSIST**
(the attack-3 H3 cure): the context passes the chain-then-hook redact
pipeline (the vetted masks + the hold surface's token-head pass + the
workflow's own `redact=` hook, wired from the registered definition) IN
THE SAME TRANSACTION as the hold's insert — the row never carries a
canary, so the default `HitlClient` (no hook handed, none wired) cannot
leak it at list/get; the read-side chain pass is the belt over the
persist-time law.

**THE KNOB, NEVER THE TRUTH** (round-8): a hold's appearance is
NOTIFIABLE (`pg_notify` on the `taskq_wf_holds` channel — the estate's
notify discipline); the notification carries THE POINTER (hold id + run
id + event) — a lost knock costs latency, never correctness: the
consumer converges by polling `HitlClient.list(run=…)`.

**THE TIMERS**: the deadline is DB-CLOCK compared (the signal sweep's
expiry arm is the ONLY live timer on a held row); the expired hold →
the DEFINED `abandoned` state (the typed `SignalTimeoutError` /
`SignalAbandonedError` — the glossary shape, never a silent orphan);
`timeout=None` must be EXPLICIT (the W1 validate warning).

**THE TIMEOUT FACE IS THE RAISE** (the attack-3 B1 cure): after the
sweep marks the hold `abandoned`, the resume's wait site RAISES
`SignalTimeoutError` — the glossary exception — and NEVER mints an
automatic new epoch (hold → expire → re-hold → ∞ is the convicted
dragon, kept red by the attack probe). The body's own ladder/except
owns the raise from there: a step ladders and terminal-fails (the
`fail` policy's shape); a loop's failure-class rules route it as the
BODY failure it is. A DELIBERATE re-wait — the body CAUGHT the timeout
and waits again within the same attempt — is a NEW body decision: it
registers a NEW hold with a NEW epoch. The other timer policies
(`resume_with_default` / the escalation arm on the TIMER) are recorded
LATER for v1 (the don't-pay law): the body-level escape — catch
`SignalTimeoutError`, return the default / enqueue the escalation
yourself — is the sanctioned composition until then, and it is exactly
what the raise-based face enables.

**THE DELIVER BOUNDARY IS REAL** (the attack-3 B2/H2 cure): a resolve
validates the payload against the hold's DECLARED models — BY SHAPE,
never by declaration order — BEFORE the CAS: a payload that fits NO
model, or fits MORE than one without the gate's `discriminator=`, is
the typed `refused` (the hold SURVIVES, nothing is consumed, the
refusal is AUDITED and the operator sees WHY). The union wait narrows
to the model the payload's shape fits — a lenient first member cannot
swallow a strict second member's delivery.

**THE AUDIT IS EXACTLY-ONCE** (the attack-3 H4 cure): the resolve's
audit row + the knock ride the CAS-WINNING transaction — two concurrent
resolves → exactly ONE `hitl.resolve` audit row, ONE knock, ONE
resolution; the loser writes nothing.

**THE CANCEL CASCADE** (P3 rule 4): `FlowRunner.cancel_workflow(run_id,
reason=…)` — ONE transaction (the flip is the linearization point), the
held signals → `cancelled` in the same snapshot, a LATE operator
deliver returns the typed `refused` (no zombie wake, no resume event),
terminal rows untouched, IDEMPOTENT (cancel twice = one cancel), the
audit row rides the same tx.

**RESUME-NOT-RETRY** (cut #5's cure): a hold's resume consumes NO
ladder attempts — the ledger distinguishes `awaited` from `failed`;
the ladder counts `failed` only. The shared-counter variant (2 holds +
max_attempts=3 = terminal failure with zero retries) is red forever.

The hold→resume latency band (G11c) is measured and pinned in
`perf-evidence-workflows.md` (the band from
`.measurements/t10-hold-resume-band.json`).

## §5 — Progress: the nodes report (T21)

A workflow body reports its own progress: `await ctx.progress(75, "page
3/4", {"page": 3})` — `pct` (int 0..100 or None), `message` (chars-capped
at 1024), `data` (jsonb, capped at 8 KiB). All optional; call it as often
as you like.

**THE EMISSION IS BEST-EFFORT, AND THAT IS THE DESIGN** (decision e — the
law: observability degrades FIRST, never correctness). The call updates
an in-memory buffer latest-wins and arms a cadence flush (~20 writes/s at
the 50 ms cadence — a 1000-emissions/s body coalesces to the same write
rate); it NEVER awaits the network and NEVER touches the finalize path.
The coalescing is HONEST: the node's `occurrences` counter counts every
emission that coalesced into the row, so the record shows both the
delivered state and the emission rate. A flush that fails is counted and
warned once (`progress_flush_lost`); a node whose every flush fails still
terminalizes normally. A lost emission costs freshness; a blocked node
costs correctness.

**THE TWO CHANNELS** (decision d — both bounded by construction):

| channel | table | shape | bound |
| --- | --- | --- | --- |
| the STATE channel | `wf_node_progress` | one row per `(node_id, channel)`, upserted latest-wins + the occurrence counter + the dropped counter | nodes × channels — CONSTANT under any emission rate |
| the STREAM channel | `wf_node_stream` | append rows in a per-node ring, drop-oldest with the dropped count ON THE RECORD | the ring bound (64/node; the retention sweep's prune arm is the backstop) |

The state channel answers "where is this node NOW" (a gauge). The stream
channel answers "what did it report along the way" (a replay). **The
state channel keeps NO history**: "what did the body report at 14:32" is
answerable only where the author streamed it (the stream) — the honest
boundary, stated in the progress guide.

**THE DECLARED SCHEMA** (the TypedGate-door pattern): `step(body, ...,
progress_schema=Page)` declares the shape of the node's `data`
emissions — a wrong-shaped emission is refused with
`ProgressRefusedError` (raised INTO the body: an authoring error is the
body's problem). The declaration is what makes a separate UI render the
emission — the context-contract law applied to progress. On a MAP SOURCE
the declaration reaches the children (`<source>.item`).

**AUTO-PROGRESS IS A PROJECTION, NOT A SECOND STREAM** (decision b): the
engine's node-start/terminal events project into the SAME stream with
`class='auto'` (the body's emissions are `class='user'`); the kind
vocabulary is CLOSED (`progress`, `wf.node.started`, `wf.node.terminal` —
validated at emit AND by the storage CHECK), and ONE seq generator backs
the table. ONE stream, ONE cursor serves both classes — a second
seq-cursor space would re-create the fleet's SSE-vocabulary
fragmentation. The projection is additive best-effort writes at the
claim/finalize seams: the finalize's own transactions are untouched (the
zero-finalize-changes probe is a shipped pin).

**THE PROGRESS-LIE FENCE** (decision b's display law): the node's STATUS
derives from the LEDGER; the progress renders INSIDE the terminal state,
never over it. A body that reports 99% "almost done" and then FAILS
shows **failed with pct=99 inside** — never 99%-running. A crash window
(the worker died mid-emission) leaves the display STALE — freshness
only — and the terminal heals it. The progress row is ADVISORY, always.

**THE AGGREGATION IS DERIVED AT READ** (decision c): a map's progress is
its children's grouped read (`"120/200 · 0 running · avg pct 62"` — one
query, LEFT JOINed to the state channel); a fan-in's is the edge ledger.
The user's aggregate of intermediate results WITHOUT a join node: declare
`map_source(source, item, aggregate=fn)` — a PURE, read-side fn evaluated
AT READ TIME over the children's decoded result rows (mid-flight,
unblocked, writing nothing). **The join node is for DATAFLOW; progress
aggregation is OBSERVABILITY** — a join whose only reader is the display
is the W2 validate warning (the join-for-progress anti-pattern: the DAG
would block on a display question).

**THE FACES** (decision f — every face a view over the same rows):

- the SSE stream: `GET /api/flow/{flow_id}/progress/stream` on the admin
  router — a `display` frame first (ledger + state), `progress` frames in
  seq order (`id:` the seq — the browser EventSource reconnect
  contract), and — when the ring pruned past a reconnecting cursor — a
  NAMED `resync` frame (the partial mode + the state payload): the
  display converges, never a silent empty-success.
- the run display: `taskq.workflows.run_display(pool, wsql, flow_id)` —
  the ledger-derived states with the progress inside (the explorer's
  data contract).
- the gauge: workflow-dimensioned ONLY (the `_other_` collapse) —
  **per-child progress is NEVER a metric label series**; it lives in the
  rows (the map line / the display), read on demand.

**THE VOCABULARY SHIMS** (the migration win, documented): the fleet's two
live SSE vocabularies are MAPPED, not converted — each is a pure function
over the one stream's rows:

| fleet vocabulary A | the stream | fleet vocabulary B |
| --- | --- | --- |
| `{"type": "progress", "job": <node>, "pct": <pct>, "note": <message>}` | `class='user'`, `kind='progress'` | `{"event": "update", "id": <node>, "percent": <pct>, "detail": <message>}` |
| — | `class='auto'`, `kind='wf.node.started'` | `{"event": "lifecycle", "id": <node>, "phase": "started"}` |
| — | `class='auto'`, `kind='wf.node.terminal'` | `{"event": "lifecycle", "id": <node>, "phase": "done"}` |

A consumer of either old vocabulary is served by the ONE stream through
its shim unchanged — the consolidation is the migration win (the third
vocabulary cannot sprout: the kind set is closed at the emit path and by
the storage CHECK).
---

## The context contract (what a body asserts on)

The body's `ctx` is the observability primitive: every field the contract names is on the LIVE surface (a body asserting on ctx is a pin, not a mock):

| Field | What it is |
|-------|-----------|
| `ctx.flow_id` | the run's id (the address every surface takes) |
| `ctx.job_id` | this node attempt's job row |
| `ctx.node_key` | the node's key — the map's children run under `<source>.item` (the per-item ledger identity) |
| `ctx.attempt` | the attempt ordinal (the ladder's count; holds do not advance it) |
| `ctx.input` | the FLOW input (`create_flow(input=...)`) |
| `ctx.flow_name` | the workflow's registered name |
| `ctx.queue` | the queue this node rides (the wiring's placement) |
| `ctx.claimed_at` | the attempt's claim timestamp |
| `ctx.map_index` | the map item's index (None off a map) |
| `ctx.budget_remaining_ms` | the loop's budget wall's remaining read (loop runs; None off a loop) |
| `ctx.hold_epoch` | the last consumed hold's epoch (a resumed body's answer identity) |

## Visualizing workflows: the admin's run explorer

The admin UI's **Workflows** tab is the run explorer: the runs list (the inventory question — newest first, capped at 200), and the run page (`/taskq/workflows/{run_id}`):

- **The graph** — the run's Mermaid, emitted FROM THE ROWS (the live graph is a row fact; the admin never imports the workflow's module): the collapsed map renders as ONE hexagon with a `done/total` counter — zero child boxes; the child detail lives in the node panel. Rendered ONCE by the vendored mermaid (no bundler, no React, no CDN — the air-gapped-ops argument); every later update is a classList patch on `data-node-key` — zero re-renders. The state colors: green succeeded, blue running, red failed, amber HELD (a human is waiting).
- **The node panel** — click a node (or Tab to it and press Enter): the status header, the attempts + the retry kind, the trace id, the captured error (errors-only by default), one upstream hop (the causal chain via parent_id), and the attempt ledger.
- **The holds & decisions** — the held row's waiting-on state: the gate, the node, the epoch, the deadline, the author's reason/tool/args, the DECLARED payload schema, and a prefilled valid example. The Resolve form delivers through the typed door (the wrong payload answers the named pydantic error inline; the hold survives). Resolved holds render as decisions (the rows-alone audit).
- **The audit trail** — every operator action on the run (resolve/deliver/cancel) is a ROW here: the principal, the action, the reason. Reads are never audited.
- **The live transport** — SSE (`/taskq/api/runs/{id}/stream`): every frame is a revisioned FULL snapshot; the seq-cursor drops stale frames; `Last-Event-ID` (EventSource supplies it for free on reconnect) continues the cursor and the next frame carries the whole state — **the reconnect never loses state**. The page sets `suppress_refresh=True` (the meta refresh would destroy the live SVG).
- **The no-JS story, stated honestly**: the no-JS surface is the SERVER-RENDERED INITIAL SNAPSHOT (the state at page load, from the same grouped query the boot JSON carries). No interactive no-JS mode is invented; without JS the page is a snapshot, and says so where the live badge would be.

## §7 — the diagnosis runbook

A workflow is stuck. The order of questions:

1. **What is the run's state?** `taskq flows status <run_id>` — the §17.5 derivation over the node rows; every stuck node names its waiting-on state + its remedy. The root row's own status is a CACHE (the rows are the truth — the page says so beside the derived status).
2. **Is it waiting on a human?** `taskq flows holds <run_id>` — the pending holds with their deadlines. A hold with NO deadline is the W1 warning's subject (a workflow that waits forever on a human is a support ticket — the deadline is `timeout_s=`, explicit).
3. **Is a node failed?** The status names it + the ladder's headroom (`attempt` vs `max_attempts`). The manual resume: `taskq flows retry <run_id> <node>` — the ledger is KEPT (the spent attempts stay recorded), the ceiling raises past the spent attempt, the blocked closure re-opens.
4. **Is it a join that can never fire?** The status renders the join-wait rows with the deps counter. A join blocked `failed_parent` re-opens when the failed parent's retry lands; a join blocked `orphan_parent`/`flow_dead` is the fence's record — see the failure-propagation section.
5. **Still stuck?** The admin's run page renders the SAME derivation + the node panel's attempt ledger + the audit trail. The rows-alone law: if the CLI and the page disagree, the ROWS decide — file it as a defect with both outputs.
