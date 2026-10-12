# Leader tick's wf-arms tax — before/after measurement

The regression-lens question: do the TaskQflow round's ten wf sweep arms
(registered into the leader's table-driven tick in
`src/taskq/worker/_leader_sweeps.py`, each `hasattr`-gated on
`workflow_sweeps_capable`) tax the base queue's leader on an estate that
never runs a workflow? Measured answer: **yes, +2.1 ms per tick — 0.007% of
the 30 s tick interval; the base arms themselves are unchanged within
noise.**

## Method

- **Control**: the pre-TaskQflow base, `49b894d05` (the parent of
  `32786387e`, the wf layer's first commit) — a full non-wf tree.
- **Treatment**: the feature tip `0e2f5cc09` (the verify-regression lane's
  worktree).
- Both trees ran the SAME micro-benchmark (`bench_leader_tick.py`, the
  verify-regression lane's): apply the tree's own migrations to a fresh
  schema, seed a modest non-wf estate (500 terminal jobs across 3 queues
  and 5 actors, 1 worker row, ZERO wf rows), open the real
  `open_worker_deps` + `PostgresBackend`, and invoke each arm of the sweep
  tick through the exact call surface `_sweep_loop` uses — backend methods
  for sweeps 1/2 and the PG-only sweeps, the module-level
  `cleanup_stale_workers` / `complete_stale_batches`, and (tip only) the
  ten `taskq.workflows._sweep` / `_hitl` functions with the rendered
  `WorkflowSql`. 40 interleaved iterations (base arms then wf arms, sharing
  PG cache state), medians and p90s reported.
- Engine: `postgres:17` on localhost:5830, the campaign container's tuning
  (`fsync=off`, `synchronous_commit=off`, `full_page_writes=off`,
  `max_wal_size=4GB`, `checkpoint_timeout=3600`, `max_connections=1000`).
  Host: Linux x86_64, bare-metal Docker. Machine otherwise idle.

## The numbers

**The base arms (the pre-TaskQflow tick), control tree vs tip:**

| tree | run | base-arms tick total (median) |
|---|---|---|
| base `49b894d05` | 1 | 4.597 ms |
| base `49b894d05` | 2 | 4.650 ms |
| tip `0e2f5cc09`, wf arms off | 1 | 4.374 ms |
| tip `0e2f5cc09`, wf arms on | 1 | 5.012 ms |

The base arms' medians straddle the control (4.37–5.01 ms across the four
runs, spread ±0.6 ms); no base arm regressed beyond run-to-run noise. The
per-arm medians agree to within ~0.1 ms on every arm (the largest,
`expired_locks`, read 2.43–2.66 ms; every other arm stayed under 0.7 ms).

**The wf arms' addition (tip, zero-wf estate, median per arm):**

| arm | median ms | p90 ms | round trips (pinned) |
|---|---|---|---|
| wf_join_rederive | 0.541 | 2.468 | BEGIN + 2 stmts + COMMIT |
| wf_outbox_drain | 0.244 | 0.350 | BEGIN + 1 stmt + COMMIT |
| wf_signal_sweep | 0.168 | 0.315 | 1 stmt |
| wf_hold_stamp_reconcile | 0.174 | 0.401 | 1 stmt |
| wf_outbox_retention | 0.135 | 0.235 | 1 stmt |
| wf_step_cache_retention | 0.125 | 0.241 | 1 stmt |
| wf_loop_budget | 0.265 | 0.515 | BEGIN + 1 stmt + COMMIT |
| wf_phantom_reap | 0.133 | 0.227 | 1 stmt |
| wf_nodeless_root_reap | 0.175 | 0.468 | 1 stmt |
| wf_progress_ring_prune | 0.172 | 0.362 | 1 stmt |
| **TOTAL** | **2.130** | | **16 round trips** |

## The verdict the numbers support

- The adopting deployment's leader tick goes from ~4.6 ms to ~7.1 ms per
  tick — a +46% relative cost on the tick's own statements, but the tick
  runs once per `sweep_interval` (30 s default): the wf arms spend
  **0.0071% of the interval**, and every arm returns zero rows on the
  zero-wf estate (each is a bounded empty read or a one-batch no-op
  delete).
- The legacy (non-adopting) deployment's tick is UNCHANGED by construction
  and pinned so: `tests/test_leader_wf_arms_tick_shape.py` drives the real
  `_sweep_loop` against a statement-recording fake and proves a backend
  without `workflow_sweeps_capable` runs ZERO wf statements, while a
  capable backend runs exactly the ten arms' 16-round-trip conversation in
  the registered order. The wire conversation — not a wall-clock number —
  is the pin, because it is deterministic: a re-ordered arm, a lost gate,
  an extra per-tick statement, or an eleventh arm goes red there before it
  re-prices any production leader.
- The wall-clock medians above are the honest measured face of that
  conversation on real Postgres; the conversation pin bounds its SHAPE.
