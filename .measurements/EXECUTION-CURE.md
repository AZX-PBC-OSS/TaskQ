# THE EXECUTION CURE — the queue IS the runtime, made TRUE IN PRODUCTION

**Builder:** production-execution builder, 2026-10-08 (base `fd0a06b8`,
the superset head through the hygiene round's `62cc78f0`), pg **:5710**
(`exec-cure-pg`, the round's own cluster), the probes'
`PROBE_DSN=postgresql://taskq:taskq@localhost:5710/taskq`.

Input: `.measurements/EXECUTION-VERDICT.md` (the execution investigator's
"TRUE IN TESTS ONLY" — three gaps, one dragon). The verdict's own probes
are the reds; every cure below makes a named probe green, captured here.

---

## THE REDS, RE-CAPTURED ON :5710 (pre-cure)

* `execution-probe/red-run1.txt` — the verdict's probe.py re-run:
  **PHASE A: "NO BODY EVER EXECUTED"** (the pending `doc_source` row
  never claimed — no `actor_config` row, the capacity LATERAL drops the
  cohort); **PHASE B: 3 bodies, all in the driving process** (the
  in-process runner, queue-blind).
* `execution-probe/red-a2.txt` — the verdict's probe_a2 re-run (the
  cohorts stamped by hand, a vanilla worker): the pre-cure claim-snooze
  loop's shape (the verdict captured 3 snoozes/30 s on :5708).

---

## THE CURES

### Gap 2 — the actor_config projection (the F3 law's call site)

`taskq/workflows/_worker_execution.py` (NEW): every constructed
`WorkflowApp` registers (weakly); the boot's projection
(`project_workflow_actor_configs`) compiles every imported app's declared
workflows — whose side effect is the **D1 definition registry's
population in the worker's process** — and projects the compiled graphs'
`(actor, queue)` cohorts into `ActorConfig` rows, synced through the SAME
`sync_actor_config` surface (the drift guards, the admin page,
`TASKQ_QUEUES_STRICT`). The boot event: `workflow-actor-configs-synced`
(`cohorts: ["wf", "wf-gpu"]` in the capable worker's log,
`execution-cure/worker-capable-a.log`).

**THE GREEN:** the invisible-cohort probe — phase A's flow row
(`actor='wf'`, `queue='default'`) is now CLAIMED by the capable worker
(attempt 1, running), and the probe-cure's phase A ends
`flow root status: succeeded`. Capture: `execution-probe/cure-run5.txt`.

### Gap 1 — the worker-side execution (the intercept + the door)

* The runner's tail is ONE machinery: `_run_node`'s post-claim body is
  extracted to `FlowRunner._execute_claimed` (the auto projection, the
  emitter, the router, the ladder, the two-tx finalize), and the
  worker-hosted door `FlowRunner.run_fleet_claimed_step` borrows it
  verbatim — no claim CAS (the fleet's `dispatch_batch` already claimed:
  running, attempt incremented, epoch bumped), the ledger claim keys the
  fleet claim's attempt, and **every fence carries the fleet claim's
  epoch** (threaded as a parameter through the finalize/ladder/exit/chain
  mixins and the emit's cursor checkpoint — the runner's own path stays
  epoch 0, byte-identical).
* The intercept (`worker/run.py` `di_consumer_loop`): a claimed row with
  `metadata.flow_id` routes to `_dispatch_flow_job` — the child-task +
  `active_jobs` registration discipline the vanilla path keeps, the seam
  imported LAZILY (`_workflow_execution_seam`, the §16.1 law: `import
  taskq` never imports the package; the fresh-interpreter pin stays
  green). The body resolves from the registered definition (D1 — the
  flow root's stamped `metadata.workflow` name, one bounded pkey read for
  the step identity + the stamp, the same cost class the leader's fire
  arm pays).
* The capability marker: the boot stamps the workers row
  `metadata.workflow_execution` (`register_worker(workflow_execution=…)`)
  — the dispatch fence's data leg. The non-capable worker NEVER claims a
  flow row (the fence); the unresolvable row (a defect deployment) parks
  budget-free `released_reason='workflow-body-unresolvable'` — the
  actor-not-found semantics, LOUD.

**THE GREEN (probe A + A2):** the pending row is claimed + EXECUTED on
the worker, **exactly once** (`bodies that ran MORE THAN ONCE: none` —
the snooze loop is dead by construction), the flow root derives
`succeeded`. Capture: `execution-probe/cure-run5.txt` phase A.

### Gap 3 — the queue truth + the design decision

**THE DESIGN PICK: (b) — the flow's queues map to DISTINCT ACTOR NAMES.**
Why, against option (a) (actor_config rows per (actor, queue)):

1. The schema's own law is one queue per actor — the row's `queue`
   column is the ASSIGNMENT (re-pends, cron fires, `move-queue`), and
   every re-pend path follows it. Splitting one actor's assignment across
   two queues would make the assignment machinery's tail contract
   (the move-drain, the repend arm) undefined for the split.
2. The label-routed claim arm never reads `actor_config.queue` — the
   ROW's own queue label routes. The heterogeneous placement needs the
   row's stamp to be truthful, not the registry row to be splittable.
3. (a) buys nothing (b) doesn't: a migration + a conflict-target change +
   every drift-guard/admin/move-queue surface updated, to represent a
   state the estate's law forbids. **Simplify-without-loss:** (b) is
   zero schema change, zero new surfaces, and the split is fully
   expressible.

The enforcement: a workflow actor declared over TWO queues is the
`WorkflowActorQueueConflictError` — refused at the projection (the boot
refuses, the drift-guard precedent, the cure named in the error). The
override-warning path (one queue silently winning) is dead.

**THE GREEN (the split-placement probe):** source (`actor='wf'`,
`queue='default'`) + chain (`actor='wf-gpu'`, `queue='gpu'`), TWO capable
workers (default-only, gpu-only): the source body executed ONLY on the
default worker's pid, the screen bodies ONLY on the gpu worker's pid,
each ONCE, the flow root `succeeded`. Capture: `execution-probe/
cure-run5.txt` phase S (`source bodies: 1 — pids: [1327993]`,
`screen bodies: 2 — pids: [1327994, 1327994]`).

**PROBE B's green (the orchestration-only drive):** `drive(flow_id,
execute=False)` — the runner's ORCHESTRATION-ONLY mode (the tick runs
only the sweep arms): the flow succeeded with all 3 bodies in the WORKER
process, the driving process executing nothing. The `execute=True`
default is the dev-loop driver, unchanged. Capture: `cure-run5.txt`
phase B.

The fence's own proof: the verdict's probe_a2 shape (cohorts stamped, a
vanilla worker only) now ends **never claimed** — attempt 0, zero
actor-not-found lines, the row WAITING (the defined pre-deployment
state), the snooze loop structurally gone. Capture:
`execution-probe/probe-a2-cure.txt`.

---

## THE FULL BATTERY (the captures)

* lint + format repo-wide: ruff check + format --check, ALL GREEN
* pyright FULL tree: **0 errors, 0 warnings, 0 informations**
* the wf pin suite + the dispatch/chain/fork/join family: **826 passed**
  (`tests/ -k "wf or flow or chain or fork or join or dispatch"`)
* the typeprobe gate: **13 MUST_ERROR markers red on pyright 1.1.414 AND
  ty 0.0.85** — the gate holds
* the SQL-template smoke (the coverage guard: the new `_WF_EXEC_CAPABLE_CTE`
  + `_WF_PROBE_FENCE_TEMPLATE` registered in `_COVERED_BY` with the
  rendered products that validate them): green
* the EXPLAIN pin (the strict-FIFO lateral's scheduled_at bound stays
  index-served; the wrapper composes the PRODUCTION capability CTE
  verbatim): green
* pin 5's dispatch-claim band (the zero-tax §16.3 probe): **green** (the
  fence short-circuits on the step_key probe — the vanilla rows' plan
  cost is one column test, the P3 fence's own cost class)
* the estate slice (the actor-config sync/drift surfaces + the wf
  surfaces): 82 passed
* mkdocs --strict: clean
* the fence pin EVOLVED with the law (`test_wf_sweep_pins.py` pin 2): a
  capable worker claims the live child; an unregistered AND a registered
  non-capable worker claim NO flow rows; a non-capable worker's PLAIN row
  still claims (the leg taxes no vanilla claim)

## THE UNSPECIFICATIONS (stated, not hidden)

* A running flow attempt's OPERATOR cancel on a worker: the cooperative
  event is not read by the wf body context; the phase-2/3 escalation and
  the reconcile's refund own the row (the ledger's arbiter dedupes
  completed side effects — at-least-once, the ledger's stated boundary).
* The intercept's consumed-message metric maps the tail's labels
  (`laddered → failed`, `held → scheduled`); the ROW is the truth, the
  label is the record's echo.
* Mixed-version fleets: an OLD worker's claim SQL predates the execution
  fence — the rolling-deploy reality every fence evolution shares.

---

## THE FIX ROUND (the fresh reviewer's findings F2-1..F2-6, cured on :5738)

* **F2-1 (the receipts):** `tests/test_wf_execution_py.py` (NEW, 13
  pins) — the Python half driven directly: the door's green (a claimed
  row resolves via D1, executes ONCE, the ledger 'succeeded'), the fence
  epoch's receipt (a STALE epoch's terminal write updates NOTHING — the
  row stays running, the ledger 'fenced'), the resolution errors × the
  five shapes (no flow_id / the row gone / the unstamped root / the
  unimported workflow / the foreign step key — all the ONE typed
  parking error), the boot projection (the healthy cohorts + the split
  placement's pairs + the metadata stamp), the per-workflow isolation
  (F2-3's pin: the broken build fn + the conflicted workflow skip
  LOUDLY — `workflow-projection-skipped` naming app/workflow/remedy —
  and the healthy workflows still project; the CROSS-app conflict still
  refuses), the WeakSet registry (never pins an app) + the capability
  marker's truth, the cancel-absorption arm (the child cancelled alone
  absorbs — 'cancelled', the entry deregistered), and THE LOOP SURVIVES
  (the real `di_consumer_loop` driven over a flow row + a witness: the
  door's transient / the unresolvable parking / the slot-acquire disown
  each leave the loop ALIVE and the next job processes).
* **F2-2 (the loop's life):** the intercept mirrors the vanilla leg's
  absorption — the door's `SlotPoolAcquireError` → disown + continue;
  any other `Exception` → counted + logged (`dispatch-flow-failed`) +
  the claim resolved + continue. The uncaught-transient loop-kill (and
  the claim-intent leak with it) is the pin's red, kept dead by
  `test_the_loop_survives_a_transient_from_the_door`.
* **F2-3:** per-workflow isolation in the projection (see the pin
  above); the ONE remaining refusal is the cross-workflow/app conflict —
  two HEALTHY declarations fighting over one cohort name is the drift
  the guards refuse, never a silent skip.
* **F2-5:** the door returns `FlowExecution` (the outcome label beside
  the REAL step_key / workflow_name / flow_id it resolved on the way);
  `flow-step-executed` logs THOSE (the dispatch decode drops the step
  identity — the metadata read was always empty).
* **F2-6:** the EXPLAIN pin asserts the INDEX-SERVED PROPERTY (no Seq
  Scan of jobs; the scheduled_at bound in an Index Cond), not the
  planner's index name.
* **THE GATES:** the wf/dispatch family 923 passed; the
  actor-config/boot/worker mains green; pyright FULL 0/0/0; ruff
  check+format ALL GREEN; the type gate 13 markers x2 checkers; mkdocs
  strict clean; THE PLACEMENT PROOF re-run on :5738 — phases A/B/S all
  GREEN (`cure-run-fixround.txt`, the probe updated for the runner's
  RunClaim-returning create_flow). The one load-sensitive run (pin 5's
  band) stays green from the cure round — this round touched no
  dispatch SQL.
