# Terminal-write fence probe index — before/after measurement

Before/after evidence for migration `01.00.19_01_pre_fence_probe_index.sql`
(the running-holder partial index rebuilt with the job id as a trailing KEY
column), measured per the performance gate: EXPLAIN (ANALYZE, BUFFERS) on
real Postgres 18, the production fused statement constants, interleaved
A/B seeds, medians over recorded samples.

## The statement and the fence

Every fenced single-row terminal/lease write (`mark_succeeded`,
`mark_failed`, `mark_retry`, `mark_cancelled`, the per-job lease checks)
targets one row by
`id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $k
AND claim_epoch = $m`. The row is unique by the primary key (`id = $1`),
but the planner may drive the UPDATE from any index the quals admit.

## Method

- **OLD (A)**: the pre-migration index shape, recreated in a throwaway
  schema: `(locked_by_worker) WHERE status = 'running'` (the
  01.00.00_01 definition).
- **NEW (B)**: this change's shape: `(locked_by_worker, id) WHERE
  status = 'running'`.
- The measured statement is the production `sql.mark_succeeded` (the
  fused UPDATE + attempt + event CTE statement) rendered per schema;
  only the index differs between sides.
- Engine: `postgres:18.6-alpine` container (PostgreSQL 18), the test
  suite's image, Linux x86_64, Docker. Every sample is
  `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` of one execution; the
  statement really executes, so the backlog is re-seeded
  (TRUNCATE + INSERT + VACUUM ANALYZE) before every sample; A and B run
  in interleaved seeds (one warm seed per shape, five recorded); medians.
  Harness: `benchmarks/ab_fence_index.py`, with a per-sample verification
  gate (the post-explain row must be `succeeded` and both child writes
  must exist; a sample that matched nothing aborts the run — an earlier
  harness draft silently recorded cross-schema no-ops, which the gate
  caught and discarded).
- Two stats regimes per shape: **analyzed** (the vacuumed steady state)
  and **unanalyzed** (the bulk-churn window: a large enqueue burst has
  landed, autovacuum's next ANALYZE has not; `reltuples` is stale and
  every equality selectivity collapses toward the one-row default). The
  unanalyzed regime is the interesting one: `enqueue_batch_fast` (COPY)
  is the documented bulk path, and a backlog drain immediately after a
  bulk enqueue lives inside this window.

## The OLD plan's defect, measured

Unanalyzed, the planner mispicks `jobs_locked_by_worker_running_idx` as
the fence UPDATE's driver and evaluates `id = $1` as a post-scan Filter:
the write walks **every running row the worker holds**. At a 2,000-row
running population: `Rows Removed by Filter: 1999`, fence scan 1.07 ms of
the statement's 2.56 ms — per terminal write, for the whole drain. The
cost is O(the worker's running population), growing with
`max_concurrency × batch pressure`, for every fenced write family.

## A/B — the fused `mark_succeeded` statement (median exec ms)

| running pop | analyzed | A exec ms | B exec ms | A fence-scan ms | B fence-scan ms | A buffers | B buffers |
|---|---|---|---|---|---|---|---|
| 8     | yes  | 1.03  | 1.35  | 0.02  | 0.03  | — | — |
| 8     | no   | 1.44  | 1.09  | 0.03  | 0.02  | — | — |
| 64    | yes  | 1.17  | 1.31  | 0.05  | 0.05  | — | — |
| 64    | no   | 1.28  | 1.06  | 0.06  | 0.03  | — | — |
| 512   | yes  | 1.51  | 1.21  | 0.04 (pkey driver) | 0.03 | — | — |
| 512   | no   | 1.64  | 1.46  | **0.26** | 0.03 | — | — |
| 2,000 | yes  | 1.63  | 1.68  | 0.04 (pkey driver) | 0.04 | — | — |
| 2,000 | no   | **2.56** | **1.42** | **1.07** | **0.03** | 91 | 22 |

Raw run JSON: `benchmarks/results/fence-index-ab.json`.

- The statement's floor (~1.0–1.7 ms) is its own FK-trigger + child-write
  work, identical on both sides. The mispicked plan adds the fence-scan
  walk on top: **+1.03 ms per terminal write at the 2,000-row shape
  (statement 2.56 → 1.42 ms, −44%)**, +0.20 ms at 512, +0.02 ms at 64.
- Under honest statistics (analyzed) at large populations the planner
  already picks `jobs_pkey` on both sides — the shapes are flat in
  class, and B is within run noise of A there (±0.3 ms across reps).
- B's plan is planner-choice-proof: whichever index the planner picks,
  both equality quals are Index Conds (a non-leading key column is still
  evaluated inside the index), so the fence scan touches one index entry
  and one heap row. `Rows Removed by Filter` is 0 on every B sample at
  every shape; A's is `pop − 1` on every mispicked sample.

## Index cost

Partial (running-only) index size at a 2,000-row running population:
A 32 KB → B 114 KB (~3.5× an entry set the claim's status transition
already maintains; the absolute size is tiny and proportional to the
in-flight population, not the table). Every existing reader keeps its
plan class: the heartbeat renewal
(`locked_by_worker = $1 AND status = 'running'`) and the web-admin
running count scan the leading-column prefix unchanged, and the per-job
lease check (`id = ANY($1::uuid[]) AND locked_by_worker = $2`) gains the
same trailing-key filtering.

## The e2e drain (same harness, full drain)

`benchmarks/ab_fence_e2e.py`: the e2e harness's drain (enqueue 5,000 →
dispatch batches of 50 → fenced terminal write per job, real Postgres),
index swapped between interleaved reps, 5 reps per side:

- A: 772 jobs/s median (runs 567–1,024), terminal writes 66.4% of wall.
- B: 742 jobs/s median (runs 626–1,024), terminal writes 66.5% of wall.

Within run-to-run noise: this drain's per-worker running population is
one claim batch (~50 rows, each claimed row is marked before the next
batch), so the fence scan's O(running) term stays at the +0.02–0.2 ms
per-write level — under the bench's own variance. The win the migration
exists for is the high-in-flight regime (workers whose running
population reaches hundreds — high `max_concurrency` pods, and any
terminal-write burst inside the stale-stats window), where the
statement-level measurement above is the direct evidence.

## Harness fidelity fix (this change, `benchmarks/e2e_dispatch.py`)

While baselining, the e2e harness's own drain was found to have been
calling `mark_succeeded` **without the fence epochs** since the
attempt/claim-epoch fence landed: `attempt=None` binds NULL, and the
fence's `attempt = $8` conjunct never matches NULL (the
cannot-prove-which-attempt doctrine), so every harness terminal write
was a silently no-op'd fence probe — the bench measured a no-op UPDATE
plan (0.94 ms) while believing it measured the terminal write. The
harness now binds `attempt=row.attempt, claim_epoch=row.claim_epoch`
(the production consumer's own fence view) and asserts the write landed;
`--mode explain`'s drifted argument list is fixed the same way.

## Verdict

- Kept: `01.00.19_01` (the two-key fence probe index). Measured: the
  fenced terminal write's fence scan drops from O(running population)
  heap-filter walking to one index-probe row under the bulk-churn
  stale-stats regime (1.07 → 0.03 ms at 2,000 running; statement −44%);
  no reader regresses; the partial index stays proportional to the
  in-flight set.
- Kept: the plan pins (`tests/test_fence_write_probe_index.py`): the
  catalog definition pin, the invariant pin (no jobs-scan node of the
  fenced write may heap-filter more than the one target row, at the
  stale-stats bulk seed — holds under every planner driver choice), and
  the red control (the pre-migration one-key shape must still exhibit
  the walk, so the control's failure means the planner fixed the mispick
  and the numbers above should be re-taken).
- Kept: the e2e harness fidelity fix (fence binds + landed-write
  assertion + explain-mode arg drift).
- The e2e drain number is honestly reported as within noise at the
  bench's ~50-row running population; the statement-level A/B is the
  evidence for the kept change.
