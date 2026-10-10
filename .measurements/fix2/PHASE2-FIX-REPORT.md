# PHASE-2 FIX REPORT (round 1) — the attacker's refusal cured

Fixer: the phase-2 fix subagent, on feat/taskqflow @ 49a90a1d → d1b7a55c.
Every attack red GREEN on the shipped tree; every vacuous pin replaced
with a real-mutation pin; the promtool gate honestly green; nothing
regressed (green-before/green-after per commit; the final full battery
275/275 — grep THE FILES in fix2/, not the scroll).

## H1 — the crash-window root wedge (the maintenance leg)

- THE CURE (6bb50a61): the maintenance leg (WORKFLOW_ROOT_MAINTAIN_SQL)
  applies the §17.5 derivation's PRECEDENCE from the rows — the rows are
  truth, the root row is a cache. The defect was the finalize GATE (the
  CASE required EVERY row terminal; the blocked_required-stamped join row
  is terminalizable by nothing → the root wedged 'running' forever).
  The failed row outranks the blocked row (row 2 over row 3): a
  non-absorbed failure now finalizes the root 'failed' THROUGH the
  resolved-blocked rows, gated only on the derivation's row 1 (a
  running/crashed/abandoned node keeps the run live). The finalize names
  its state + reason (error_class 'UnabsorbedNodeFailure' when the root
  has no error of its own). One sweep pass releases the retention.
- RED: fix2/h1-pin-red-before.txt (the new pin's first run — the wedge
  reproduced); GREEN: fix2/h1-pin-green-after.txt, h1-family-run2.txt
  (the attack's crash-window probe + 60 standing pins).
- PIN: test_t08_maintenance_leg_finalizes_the_wedged_root — the wedged
  scenario → the root terminal after one sweep pass + the LIVE mutation
  drill (the failed-root arm dropped reproduces the wedge on a fresh
  flow; the shipped statement is the control arm).

## H2 — the mixed-policy node (the fence is not absorption)

- THE CURE (4a20fbf9), three pieces:
  1. THE ABSORPTION RECORD (`_absorbed_exists`, one home shared VERBATIM
     by the per-node rollup and the maintenance leg's has_failed): a
     failed node is absorbed only when its absorbing join is not itself
     blocked with a TERMINAL reason — the edge's POLICY declaration alone
     absorbs nothing; the record must show the absorption RAN. The
     derivation can now say 'failed' for the failed-closed run — the
     envelope honest.
  2. THE FLOW-FENCED JOIN ARM (REDERIVE_SWEEP_SQL): a never-fired join
     row on a TERMINAL flow can never fire again (the fire's flow-status
     leg refuses terminal flows) — the arm stamps it 'failed_parent'
     naming the failed parent when one exists (the mixed shape), else
     'flow_dead' (new named reason). The reconciled + firable arms
     exclude the stamped rows: the dead-run rescan ends.
  3. DECREMENT_ABSORBED's flow-alive guard is NAMED as what it is: the
     fence (kept — the fenced decrement is not absorbed).
- RED: fix2/h2-pin-red-before.txt + the attack's t06-attacks-run1.txt;
  GREEN: h2-run4.txt, h2-family-final.txt (98), final-full-battery.txt.
- PIN: test_t06_pin8_mixed_policy_node_the_fence_is_not_absorption —
  after ONE sweep pass: the flow terminal 'failed' (NOT
  succeeded/blocked), the reconstruction 'failed', joinB
  blocked-with-reason NAMING the failed parent, fires 0, no hanging
  claimable rows — plus the live drill (the absorbed predicate without
  the fence clause reproduces the envelope lie: derives 'blocked').
- SweepResult grew `flow_fenced`; the leader's tick count includes it;
  the audit differential's double carries it.

## H3a — the promtool gate (the gauge's real emission path)

- THE CURE (6a8bea21 + the H3a commit — no rule touched, no rule
  deleted): TaskQWorkflowBlockedStuck stays TRUE. The emitter probe now
  drives the gauge's REAL public emission path —
  `update_wf_progress_cache`, the same (workflow, state)-keyed write the
  leader's sampler calls — so `taskq_wf_progress_nodes_total` is a
  series the real emitter SERVES: the operand gate, the label gate
  (extended to the emitter's series) and the promtool evaluation all
  bind to emitted truth. The harness-binding test's count assert
  (never moved when the BlockedStuck cases landed) → 24 firing + 11
  guards; the label-value binding list extends to the fourth family.
- RED: fix2/h3a-before.txt (both gate tests); GREEN: h3a-after3.txt
  (promtool SUCCESS on the real names + labels + the honest annotations),
  final-estate-slice2.txt.

## H3b — the flow-link index rebuilt on the READ shape

- THE REPRESENTATION, STATED: the flow_id linkage is the uuid STRING in
  `metadata.flow_id` (the stamp's own shape); the reads cast it to uuid.
- THE CURE (61f40297, migration 01.00.25_02): the index expression
  carries the same cast, AND the partial names the index's TRUE
  population — `metadata ? 'flow_id'` (01.00.25_01's step_key-only
  partial was a no-filter in production: vanilla rows carry step keys,
  so the "partial" index held the whole fleet). Every flow-scoped read
  (the rollup, the per-node variant, the maintenance leg's per-flow
  join, the gauge sampler, the reconstruction's never-granted read)
  carries the exact clauses that imply the partial. The rollup's WHERE
  also names what its docstring always claimed: the root row is read
  BESIDE the nodes, never counted among them.
- BEFORE/AFTER at the 220k-row fleet (40 runs × 500 nodes + 200k vanilla,
  VACUUMed — fix2/index-fleet-bands.json + the plan captures
  index-before-text-index.txt / index-after-uuid-index.txt):
    rollup            21.2 ms → 0.12 ms   (the attack measured 15.2 @ 221k — same class)
    per-node read     20.7 ms → 0.29 ms
    root-maintain     57.9 ms → 17.6 ms   (the bounded batch's real work, index-driven)
    wf-progress gauge 23.9  → 23.4 ms     (fleet-wide BY DESIGN — no run id to key
                                         it; the band recorded honestly)
- THE COST-GATE PIN REWRITTEN: it measures the FLEET shape (the measured
  run one among many — the pre-rewrite pin's shape made the measured
  flow the WHOLE table, where a seq scan and an index scan cost the same
  and the plan assert could not fail) and asserts the plan. CANNOT PASS
  ON THE SEQ-SCAN SHAPE, proven end-to-end: fix2/
  h3b-costgate-red-on-old-index.txt — the pin's own plan-assert walk on
  the old index: rollup/nodes/root-maintain ALL RED (jobs seq-scanned);
  on the cured index: ALL GREEN. The bands re-measured: wf-rollup-band
  (fleet shape, 10 ms band), edge-scale-curve (the 100k-edge rederive
  re-run, index-driven), final-perf-bands.txt.

## H4 — the G7 always-on assertion is WIRED

- THE CURE (df332ef0): the hook appends to `item.fixturenames` (the
  closure list is read at setup time) — the usefixtures MARKER was
  inert. Scoped to the ASYNC items of the test_wf_/test_workflows_ files
  (the check's fixtures need PG; the sync pins assert the derivation in
  memory).
- THE PROOF, captured end-to-end (fix2/h4-endtoend-red-proof.txt): with
  the wiring live, the teeth pin's own deliberately-lying root REDS its
  TEARDOWN — the check runs and bites on a real pin's real run.
- THE LAW (the cache's honest semantics, from each terminal root's
  writer): a 'succeeded' root requires the rows to derive 'complete'
  (the premature-complete lie — the status cache); a 'failed' root may
  never contradict a completed run; a 'cancelled' root is the cancel
  arm's linearization point (the rows may lag it); a LIVE root may lag
  the rows' terminal verdict by one sweep pass (the H1 cure heals it —
  the H1 pin carries that teeth). The old strict equality could never
  survive a real run (a fired-join run derives 'pending' under a
  'running' root — lawful).
- The three pins whose mechanics hand-wrote terminal roots now end
  lawful (the ledger-claim pin's cancel story seeds 'cancelled'; the two
  phantom-reap pins seed the cancel-terminal flow); the teeth pin
  restores the truth after its in-body drill.

## M1 — the rolling-deploy tolerance is real (7159b72a)

- The expiry arm: the T18 guard is COMPOSED (base + fragment, one home)
  and the arm tolerates the missing tables PER CALL — the fallback runs
  the UNGUARDED base (semantically exact: no workflow tables, no
  join-wait rows to hold for), logged once per process
  (`expiry-workflow-guard-fallback`); the next call re-tries guarded.
- The per-actor prune arm: the same per-batch tolerance (the
  pre-tolerance shape was the leader's death with actor_overrides on a
  pre-workflow schema); the log-once latch shared
  (`_log_workflow_guard_fallback_once`).
- THE MID-PRUNE LEAK: the fallback rides a LOCAL, never a reassignment —
  the next batch re-tries the GUARDED statement (the pre-rewrite shape
  left the rest of the drain unguarded after a migration landed
  mid-prune).
- THE PINS (tests/test_wf_pre_workflow_tolerance_pins.py): a REAL
  pre-workflow schema — the migrations to target 01.00.23_01 (the
  round's own crash window: the column file applied, the table file
  pending). The expiry arm RUNS and expires the row; the actor arm
  prunes the override's aged rows; the CONVICTED VARIANT (the guarded
  statement raw) reds the exact UndefinedTableError — the leader-killer
  observed. The doc (maintenance-sweeps.md §7) now names the real
  mechanism.

## M2 — the phase-2 guards joined the mutation gate (2300857f)

- [tool.pytest-gremlins] paths: + _sql_finalize.py, _sql_sweep.py,
  _sql_status.py, backend/_sweeps.py, worker/_leader_shared.py, with the
  killers' reproducible run (the propagation/status/sweep/pruner/
  tolerance pin families).
- THE RED1 EVIDENCE MADE REAL: pin 1's in-test drill (the audit's
  vacuous-theater finding) is REPLACED with the real engine mutation —
  the cascade's block arm dead in the monkeypatched module constant, the
  bundle re-rendered, the REAL finalize's tx2 running the mutant: the
  convicted strand observed in the row (blocking_reason still 'join',
  the flow's flip never landing — the flow arm's gate is part of the
  conviction). Captured: fix2/m2-t06-run2.txt. (Also found + fixed
  en route: pin 3's fetchrow unpacking — a pre-existing pyright error in
  the file.)

## L1 — the guide's numbers are the artifact's (62052754)

workflows.md's refit now states the artifact's numbers: 1.684 µs/edge +
2.705 ms base (refit at the bound 4.39 ms, the 1000 point the named
outlier, the recorded points 3.04/3.34/11.1 ms) — the base-dominated
criterion's real arithmetic (base 2.7 ms vs the 1000-edge marginal term
~1.7 ms).

## L2 — the typed absorbing policy (83073690)

FailureInfo.policy: AbsorbingPolicy = Literal["collect", "maybe"]
(exported). A fail_closed claim is unconstructible at the door. The
engine's two fan-in sites assert the runtime guard (the statement's
WHERE already constrains the column); the parse walk repairs pre-policy
items and refuses out-of-vocabulary values. The typeprobe's fail_closed
line is now a MUST_ERROR marker — RED on pyright 1.1.414 AND ty 0.0.85;
the type gate holds (final-typeprobe-gate.txt: every MUST_ERROR marker,
5, reds on BOTH).

## THE VACUOUS AUDIT — all four cured

1. test_t08_query_count → drives the REAL surfaces (the reconstruction,
   the gauge's fleet sampler, the grouped rollup) at TWO node counts;
   the pinned invariant: the statement count must not MOVE with the
   population (N+1 reds; a second instrument reds).
2. test_t08_cardinality → ATTEMPTS the convicted per-node key and
   observes the REJECTION (the strict pair-unpack raises; a lenient
   rewrite reds the pin), then restores the process-global cache.
3. test_t06_pin1's drill → the real in-engine mutation (see M2).
4. H4's wiring → the real registration + the end-to-end red proof.

## THE GATES (all local, all captured in fix2/)

- ruff + format: ALL CHECKS PASSED (src + tests).
- pyright: src/taskq 0 errors; every file my commits touched 0 errors.
  (tests/ carries ~160 PRE-EXISTING strict-mode errors across the wider
  estate — test_cli_migrate 46, test_otel_integration 11, ... — present
  on the committed tree before this round; not this phase's regression,
  recorded here for the record.)
- The changed-pin files ×3: final-pins-x3-run1/2/3.txt — 35/35 three
  times.
- The pin suite + the attack tests + the estate slice: the final full
  battery final-full-battery.txt — 275/275 (the phase-1 attack files,
  the phase-2 attack reds, every pin family, the schema-migration and
  index-audit and render-cache and sweeps gates, the promtool gate).
- The bands: re-measured on the cured tree; the index cure MOVED the
  fleet numbers (H3b's table above); before/after to
  fix2/index-fleet-bands.json.

## HYGIENE

- One writer per worktree for this whole round; the attacker's evidence
  corpus (.measurements/attack2/) committed unchanged.
- The dev loop's own PG container (fix2-pg @ :5692) destroyed after; the
  scratch schemas died with it.
