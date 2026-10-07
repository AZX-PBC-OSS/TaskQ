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

## The build-time validators

A malformed graph is a coding error, refused before any row is written:
`validate_fork` refuses the empty fork (zero children owe a join that can
never fire); `validate_join_spec` refuses a join node with zero incoming
edges (the stranded invisible join — before the sweep's LEFT-JOIN
hardening it was not even diagnosable). The runtime backstop: the rederive
arm enumerates edge-less join-wait rows (a LEFT JOIN on the edge ledger)
and stamps them `metadata.blocking_reason='orphan_parent'` — the record
never looks healthy while the work is wrong.

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
| `tests/test_wf_perf_bands.py` | the perf bands: the 1000-child fan-out tx, the join-fire latency, the enqueue/dispatch noise bands |

History note: T03's schema-pin file (`tests/test_workflows_schema_pins.py`)
was folded when T05 landed — its pins live in
`tests/test_wf_schema_migration.py` (the schema/lock-class/import families)
and `tests/test_wf_perf_bands.py` (the T03 bands); no pin was dropped.
