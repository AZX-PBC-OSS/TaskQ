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

The tx1→tx2 crash window keeps the same guarantee: the leader's
maintenance sweep registers `wf_join_rederive`, which re-derives the
counter from the edge ledger, fires the firable joins and runs their
reducer bodies from the flow run's reducer memo (the finalize registered
it; the registered definition is the resolver's fallback — bodies come
from the definition, D1). A raising body there rolls the sweep pass back;
the next tick re-fires.

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
(the `_SweepSpec` tick table), gated on the PG-only maintenance marker:

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
| `tests/test_wf_sweep_pins.py` | the sweep family: the post-cancel fire refusal (2+5), the misnamed child (8), held-row exclusivity (4), the phantom reaper (15), the empty join (17), the dispatch fence (2's claim leg), the sweep-arm wiring (21) |
| `tests/test_wf_fork_pins.py` | the fork family: the id-collision (18), fork atomicity (19), the outbox drain exactly-once (20) |
| `tests/test_workflows_ledger_pins.py` | the ledger family: the double-run (1), the lost-completion window (2), the concurrent claim (3), the run-key replay (4), the claim-atomic window (5), the terminal-atomic split-write (6), the map-children arbiter (7) |
| `tests/test_wf_engine_units.py` | the in-process pins: the seam-only generation (10), redact-before-persist (11), the canonical hash (12), the deadlock budget (14), body-from-definition (16) |
| `tests/typeprobe/` | T01's negative type probes (pyright + ty, the CI `type-probes` gate) |
| `tests/test_wf_perf_bands.py` | the perf bands: the 1000-child fan-out tx, the join-fire latency, the enqueue/dispatch noise bands |

History note: T03's schema-pin file (`tests/test_workflows_schema_pins.py`)
was folded when T05 landed — its pins live in
`tests/test_wf_schema_migration.py` (the schema/lock-class/import families)
and `tests/test_wf_perf_bands.py` (the T03 bands); no pin was dropped.
