# SQL hot-path follow-ups: dispatch depth scaling, event retention, storage tuning

Design-spike outcomes for three measured campaign findings, with prototype
measurements on PostgreSQL 18.6 (`EXPLAIN (ANALYZE, BUFFERS)`, COPY-seeded
scratch schema, interleaved rolled-back rounds). Evaluation scripts live in
`benchmarks/pg_dispatch_depth_spike.py`; raw artifacts in
`benchmarks/results/depth-spike-*.json`.

Related docs: [architecture.md](../architecture.md) (schema design, dispatch
mechanics), `src/taskq/backend/_dispatch_sql.py` (the CTE under test),
`src/taskq/backend/_sweeps.py` (sweep doctrine), `src/taskq/migrate.py`
(runner constraints).

---

## 1. Dispatch CTE: the `locked` join is O(backlog depth)

### Finding

The shipped strict-FIFO CTE (`src/taskq/backend/_dispatch_sql.py`, the
`locked` CTE) re-joins the small `ranked` candidate set back to `jobs` with
`WHERE j.status = 'pending'`. At scale the planner serves that join — and the
terminal `UPDATE ... FROM eligible` — as **hash joins over a Seq Scan of the
entire pending backlog**, twice per dispatch round:

- `locked`: `Seq Scan on jobs` (filter `status = 'pending'`) hash-built,
  probed with ~100 candidate rows, top-N sorted, LIMIT 50.
- the final `UPDATE ... FROM eligible`: a **second** full pending Seq Scan,
  hash-joined against the 50 eligible ids.

Measured (PG 18.6, EXPLAIN ANALYZE BUFFERS, JIT off — see the JIT note):

| backlog depth | exec time | buffers |
|---:|---:|---:|
| 1 000 | 1.04 ms | 1 543 |
| 10 000 | 2.92 ms | 2 143 |
| 50 000 | 11.67 ms | 4 809 |
| 200 000 | 55.81 ms | 14 909 |

O(depth) in both time and buffers — the campaign's "2.2 ms at 2k deep" point
reproduces on the same curve.

**JIT note.** With the server default (`jit = on`), every execution of the
dispatch CTE pays ~33 ms of LLVM emission (plan cost ≫ `jit_above_cost`),
which drowns the scan behavior entirely. All measurements below run with
`SET jit = off`. Operators seeing ~30 ms dispatch latencies should check
whether the JIT tax (not the scan) dominates; `jit = off` for the dispatch
role is worth evaluating independently of this design.

### Alternatives prototyped

All prototypes keep the candidate/identity/rank semantics and were
output-compared against the shipped CTE per round (`benchmarks/
pg_dispatch_depth_spike.py` asserts the claimed job-id set matches).

**v1 — top-ids-first locking (subquery LIMITs).** Same candidate body, but
the LIMIT-ed id set is finalized *before* touching the heap:

```sql
ranked AS MATERIALIZED (
  SELECT id.*, ROW_NUMBER() OVER (
    PARTITION BY id.actor
    ORDER BY id.priority DESC, id.scheduled_at, id.id
  ) AS pending_rank
  FROM identity_dedup id
),
top_ids AS (
  SELECT id FROM ranked
  ORDER BY pending_rank, priority DESC, scheduled_at, id
  LIMIT (SELECT limit_n FROM params)
),
locked AS (
  SELECT rk.id, rk.actor, j.identity_key, rk.fairness_key, rk.fairness_rank,
         rk.priority, rk.scheduled_at, rk.pending_rank, rk.residual
  FROM top_ids t
  JOIN ranked rk ON rk.id = t.id
  JOIN "{schema}".jobs j ON j.id = rk.id
  WHERE j.status = 'pending'
  ORDER BY rk.pending_rank, rk.priority DESC, rk.scheduled_at, rk.id
  FOR UPDATE OF j SKIP LOCKED
)
```

The lock step becomes a merge join over `jobs_pkey` that stops after
O(limit) index entries — structurally bounded. **But v1 has a broken generic
plan**: planned with `plan_cache_mode = force_generic_plan` (and reachable
under the default `auto` after the prepared statement's custom-plan window),
the statement under-dispatches — it returns **2 rows instead of 50**, from a
plan whose candidates lateral stops after 2 index entries. Deterministic per
statement text, fatal if it surfaces in production. **Rejected** — do not
ship subquery LIMITs in this CTE family.

**v2 — lock-first index-ordered.** Skip the candidate prelude; lock
index-ordered straight off the queue-keyed partial index
(`ORDER BY priority DESC, scheduled_at, id LIMIT limit*oversample FOR UPDATE
SKIP LOCKED`), then apply actor-capacity admission after the lock. The
`ORDER BY` (with the `id` tiebreak) does not match `jobs_dispatch_idx`
(`queue, priority DESC, scheduled_at` — no `id`), so the planner sorts a
Seq Scan of the whole backlog — at 200k it spilled 11 MB to disk
(`Sort Method: external merge`):

| backlog depth | v2 p50 |
|---:|---:|
| 1 000 | 1.73 ms |
| 10 000 | 7.42 ms |
| 50 000 | 34.95 ms |
| 200 000 | 129.25 ms |

Worse than shipped at every depth. **Rejected.** (It also changes semantics:
per-actor residual and identity dedup become post-lock filters.)

**v3 — covering index for an index-only candidates lateral.** v1's SQL plus

```sql
CREATE INDEX jobs_bench_covering_idx
    ON "{schema}".jobs (queue, priority DESC, scheduled_at, id)
    INCLUDE (actor, identity_key, fairness_key, schedule_to_close)
    WHERE status = 'pending';
```

The lateral ran index-only as intended, but **the covering index does not
fix the depth scaling** — v1's generic-plan fragility and estimate cascade
remain, and once the lock step is structurally bounded (v1b), the lateral's
~100 heap fetches are already O(limit). Not worth a new index + the VM
freshness dependency. **Dropped.**

**v1b — top-ids-first locking with literal LIMITs (winner).** Identical to
v1 except the lateral and `top_ids` LIMIT bounds are rendered as literals
(`LIMIT 100`, `LIMIT 50`) instead of `(SELECT ... FROM params)` subqueries.
The planner estimates a `Limit` node as its LIMIT value *only when the value
is a literal*; with the subquery form it keeps the child's (index-range
sized) estimate, and that garbage cascades through the CTE chain until the
final UPDATE join believes `eligible` has millions of rows and hash-builds
the whole backlog. With literals, every join in the chain plans as
small-side-driven nested loops with PK probes.

| backlog depth | shipped p50 | shipped buffers | v1b p50 | v1b buffers | shipped/v1b |
|---:|---:|---:|---:|---:|---:|
| 1 000 | 1.39 ms | 1 543 | 1.34 ms | 1 394 | 1.0× |
| 10 000 | 3.91 ms | 2 143 | 1.43 ms | 1 402 | 2.7× |
| 50 000 | 10.99 ms | 4 809 | 1.60 ms | 1 401 | 6.9× |
| 200 000 | 46.25 ms | 14 909 | **1.46 ms** | **1 551** | **31.7×** |

v1b is flat across a 200× depth range (O(limit)), with p95 ≤ 3 ms, and its
generic plan is well-behaved (50-row dispatches under both
`plan_cache_mode = auto` and `force_generic_plan`, 25 consecutive
executions).

### Expected patch outline for `_dispatch_sql.py`

1. In `_DISPATCH_SQL_TEMPLATE`, insert the `ranked AS MATERIALIZED` +
   `top_ids` + restructured `locked` shape above; `locked` loses its own
   `LIMIT` (the bound moved to `top_ids`) and keeps
   `FOR UPDATE OF j SKIP LOCKED` plus the pending re-check.
2. Render the two LIMIT literals at render time: `_render_dispatch_sql`
   already substitutes per-variant fragments; extend it to take
   `limit_n: int` and `oversample: int` and substitute
   `__TOP_LIMIT__`/`__LAT_LIMIT__` tokens with `int(...)`-validated
   literals. The SQL is already per-schema rendered; per-`(limit_n,
   oversample)` rendering is the same pattern and stays within asyncpg's
   statement cache for any given deployment (the values change only when
   settings change).
3. Keep the `per_actor_capacity` residual CASE (the per-actor admission
   bound) — it now feeds `candidates` only; the lateral's LIMIT becomes the
   literal `oversample * limit_n` upper bound, with the per-actor bound
   enforced by `residual > 0` filtering and the post-`eligible`
   `actor_rank <= residual - in_flight` cap (unchanged).
4. Re-render both variants (strict-FIFO and round-robin) through the same
   template; the round-robin lateral keeps its per-fairness_key window but
   gets the literal bound the same way.
5. Add a regression test pinning the **generic plan** (`SET plan_cache_mode
   = force_generic_plan`): dispatch over a deep seeded backlog must return
   `limit_n` rows, not a truncated set. The v1 experiment shows this shape
   of bug is reachable by planner choice; the test is the guardrail.
6. Note for reviewers: `UPDATE ... RETURNING` row order is plan-dependent
   (the shipped CTE's id-ordered RETURNING is an accident of its seq-scan
   probe side). The worker path consumes rows order-agnostically; if a
   caller ever needs rank order, sort client-side by the returned
   `pending_rank`-equivalent columns.

---

## 2. Independent `job_events` retention sweep

### Problem

`job_events` has no retention of its own: rows die only via the parent-job
`ON DELETE CASCADE` when the prune sweep removes a terminal job (30–90 day
per-status windows — `_leader_shared.py`'s `_ARCHIVE_CTE_SQL`, cutoff =
`finished_at < statement_timestamp() - retention`). At the measured ~2
events/job and 100 jobs/s that is ~17.3 M events/day and ~110 GB/30 days of
event storage that exists only because job retention is long. Jobs that
never reach a terminal state (stranded pending/scheduled rows) hold their
events forever.

### Principle

Events are **narration**; the durable forensic record for a terminal job is
`jobs`/`jobs_archive` + `job_attempts`/`job_attempts_archive` (which keep
outcome/error/traceback for the full 30/90-day prune windows plus the
365-day archive retention). The event window therefore does not need to
match the job retention family — it needs to bound event volume
independently of it. A 7-day default keeps the steady-state event table at
~26 GB (121 M rows) instead of 110 GB at 30 days, while staying ≤ the
shortest job-retention window (30 d) so events never outlive the shortest
lived observation any operator could reasonably run.

### Sweep SQL (PR-#120 / `_sweeps.py` pattern)

Batched `DELETE` by `occurred_at`, `MATERIALIZED` window, `statement_timestamp()`
bound (index-cond eligible, see the two-clock doctrine in `_sweeps.py`),
outer re-check so a concurrent duplicate sweep is a no-op, one committed
batch per call:

```sql
WITH expired AS MATERIALIZED (
    SELECT id
    FROM "{schema}".job_events
    WHERE occurred_at < statement_timestamp() - $1::interval
    ORDER BY occurred_at, id
    LIMIT $2
)
DELETE FROM "{schema}".job_events e
USING expired
WHERE e.id = expired.id
  AND e.occurred_at < statement_timestamp() - $1::interval
RETURNING e.id
```

Required index (migration — see §4):

```sql
CREATE INDEX IF NOT EXISTS job_events_occurred_at_idx
    ON "{schema}".job_events (occurred_at, id);
```

The `id` suffix makes the drain deterministic (oldest-first, stable under
tied timestamps) and pins the snap to this index — the same ORDER-BY-pins-
the-scan rule `_sweeps.py` documents for the sweep snaps. Write cost: one
extra index maintained per event INSERT (~10% write amplification on the
event path) — accepted; it is what makes the sweep boundable at any table
size.

### Scheduling and bounds

- **Leader-gated, per-tick cadence** (runs in `_sweep_loop` after sweep 5,
  like sweeps 1–4) — *not* the daily prune-cron shape: at 100 jobs/s a
  once-daily sweep would need to delete ~17 M rows in one leader pass.
  One batch per tick bounds each call.
- Batch size: `event_retention_batch_size`, default 10 000
  (`prune_batch_size`'s value; its own setting so the two can be tuned
  independently). One batch per 5 s tick = 2 000 rows/s capacity vs the
  200 events/s steady insert rate — 10× headroom.
- `statement_timeout` applied/restored via the `set_config(..., true)` pair
  with `DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS` (1 750 ms), same as the
  other batched sweeps.
- Steady state drains to a no-op (2-buffer index range check). After a
  retention *reduction* (e.g. 30 d → 7 d) the first ~80 M-row backlog
  drains at one 10k batch per tick (~11 h) — each batch committed, each
  tick short; no user action needed.

### Volume math (from the measured 2 events/job)

| window | steady-state rows | steady-state size* |
|---:|---:|---:|
| 7 d (default) | ~121 M | ~26 GB |
| 30 d (= shortest job retention) | ~518 M | ~110 GB |

*at 100 jobs/s, using the campaign's measured ~3.7 GB/day `job_events`
growth (heap + btree + PK + `(job_id, occurred_at)` index).

### Configuration and metrics

- `event_retention_period: timedelta`, default 7 d,
  `TASKQ_EVENT_RETENTION_PERIOD`. `timedelta(0)` **disables** the sweep
  (restores the cascade-only status quo; documented as the escape hatch).
  Negative values rejected at settings load (`_non_negative_timedelta`).
- `event_retention_batch_size: int`, default 10 000, `ge=1`.
- Counter `taskq.swept.job_events` (mirror of `taskq.pruned.jobs`) plus a
  `event-retention-swept` info log (`kind="event_retention"`, `count=`)
  per non-empty batch, matching the prune sweep's observability shape.
- Patch outline: `_SWEEP_EVENT_TTL_SQL` + `sweep_expired_events()` in
  `taskq/backend/_sweeps.py` (mirroring `sweep_expired_results`'s shape);
  call site in `_sweep_loop`; metric in `obs/_otel.py`; settings in
  `taskq/settings.py`; in-memory twin no-op with the same settings surface.

---

## 3. Per-table autovacuum overrides and `jobs` fillfactor

### Problem

No per-table autovacuum tuning ships with the schema. `jobs` is UPDATE-hot
(dispatch, heartbeat lock extension, terminal writes) and at 100 jobs/s the
global defaults vacuum far too late (scale factor 0.2 = 40k dead rows at
200k) while `job_events` grows append-only with cascade deletes. The churn
probe measured FF50 keeping the `jobs` heap flat but +50% seed footprint —
too aggressive; the update pattern needs HOT headroom, not 50%.

### Migration statements

Transactional-safe: `ALTER TABLE ... SET (...)` is a catalog-only change
(`SHARE UPDATE EXCLUSIVE`, momentary), rewrites nothing, and needs no
`CONCURRENTLY` carve-out — it runs in the runner's default transaction
wrapper. Place after the retention index so a single version ships both
(see §4 for sequencing):

```sql
-- 01.00.07_02_pre_jobs_storage_tuning.sql
-- Per-table autovacuum overrides + HOT headroom for the two high-churn
-- tables. Catalog-only (no rewrite); safe to apply online. Forward-only.

-- jobs: UPDATE-hot (dispatch / heartbeat / terminal writes). fillfactor 80
-- leaves HOT-update headroom (~20% free page space) at ~25% heap growth —
-- the churn probe measured FF50 flat-heap/+50%-seed-footprint, too big a
-- footprint for the same benefit. Vacuum at 2% dead tuples (vs global 20%)
-- keeps the cycle tight at 100 jobs/s (4k dead rows at 200k ≈ ~20 s of
-- churn); analyze at 2% keeps the dispatch planner's stats fresh (the
-- estimate cascade in §1 is an argument for fresh stats, not stale ones).
-- insert_scale_factor keeps VM/visibility maintenance moving during
-- append-heavy backlog growth (index-only scan eligibility + anti-wraparound).
ALTER TABLE "{schema}".jobs SET (
    fillfactor                            = 80,
    autovacuum_vacuum_scale_factor        = 0.02,
    autovacuum_vacuum_threshold           = 200,
    autovacuum_vacuum_insert_scale_factor = 0.02,
    autovacuum_analyze_scale_factor       = 0.02
);

-- job_events: append-only inserts + ON DELETE CASCADE churn. 1% vacuum
-- scale factor recycles cascade-dead tuples promptly and keeps the
-- visibility map fresh for job_events_occurred_at_idx (the retention
-- sweep's index) and job_events_job_id_idx (the event tailing path).
ALTER TABLE "{schema}".job_events SET (
    autovacuum_vacuum_scale_factor        = 0.01,
    autovacuum_vacuum_threshold           = 1000,
    autovacuum_vacuum_insert_scale_factor = 0.01,
    autovacuum_analyze_scale_factor       = 0.01
);
```

Setting rationale:

| setting | jobs | job_events | why |
|---|---:|---:|---|
| `fillfactor` | 80 | (default 100) | HOT headroom on the UPDATE-hot table; append-only table needs none |
| `autovacuum_vacuum_scale_factor` | 0.02 | 0.01 | trigger on churn volume, not table fraction |
| `autovacuum_vacuum_threshold` | 200 | 1000 | floor so tiny tables still vacuum |
| `autovacuum_vacuum_insert_scale_factor` | 0.02 | 0.01 | PG13+: insert-driven vacuum during backlog growth (VM freshness, wraparound) |
| `autovacuum_analyze_scale_factor` | 0.02 | 0.01 | fresh stats for the dispatch planner |

### Sequencing

Both migrations are `pre`-phase (safe to apply before the code that depends
on them ships, cheap to apply online):

1. `01.00.07_01_pre_event_retention_index.sql` — `job_events_occurred_at_idx`.
   Must precede the sweep code. Plain transactional `CREATE INDEX` per the
   `01.00.02` precedent (the runner's no-transaction + CIC template was
   tried and reverted in `01.00.06` — it deadlocks against the runner's own
   advisory-lock discipline); carries the same maintenance-window OPS NOTE
   (EXCLUSIVE lock for the build; on a large `job_events` table, apply
   during a window or let `IF NOT EXISTS` no-op after a manual
   `CREATE INDEX CONCURRENTLY`).
2. `01.00.07_02_pre_jobs_storage_tuning.sql` — the ALTERs above.

---

## Appendix: reproduction

```sh
# depth spike (this design's measurements; ~15 s)
./.venv/bin/python benchmarks/pg_dispatch_depth_spike.py \
    --mode explain --depths 1000 10000 50000 200000 --rounds 21

# generic-plan cliff (v1 shape, 2-row under-dispatch)
./.venv/bin/python - <<'EOF'
# see "Alternatives prototyped" — v1 under plan_cache_mode=force_generic_plan
EOF
```

Artifacts: `benchmarks/results/depth-spike-*.json` (per-depth timings,
round-0 id sets for correctness, full EXPLAIN plans). The probe is
read-only with respect to `src/`; it creates and drops its own scratch
schema (`tq_bench_depth`).
