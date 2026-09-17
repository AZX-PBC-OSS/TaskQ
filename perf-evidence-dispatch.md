# Dispatch claim restructure — before/after measurement

Before/after evidence for the dispatch claim-path restructure (the lock step
split into the capped `top_ids → locked` window and the uncapped
`sliding_locked` slide, plus the `rr_keys` queue-scoping predicates), measured
per the initiative's performance gate: same shapes the dispatch contract is
measured on, EXPLAIN (ANALYZE, BUFFERS) on real Postgres, rows / buffers /
timing side by side.

## Method

- **OLD**: `src/taskq/backend/_dispatch_sql.py` at the branch point
  (`git show b9d5a91:src/taskq/backend/_dispatch_sql.py`).
- **NEW**: the working-tree `_dispatch_sql.py` (this change).
- Engine: `postgres:18-alpine` testcontainer (PostgreSQL 18.6,
  aarch64-unknown-linux-musl), the test suite's exact image and server tuning
  (`fsync=off`, `synchronous_commit=off`, `full_page_writes=off`,
  `max_wal_size=4GB`, `checkpoint_timeout=3600`, `max_connections=1000`).
  Host: macOS aarch64, Docker Desktop.
- Every measured run is `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` of the
  production statement constants rendered into a migrated throwaway schema.
  EXPLAIN ANALYZE executes the UPDATE, so the backlog is re-seeded
  (TRUNCATE + INSERT + VACUUM ANALYZE) before every sample; one unrecorded
  warm run, then three recorded runs, median by execution time reported.
  Buffers and per-node row counts are deterministic for a fixed seed.
- `buffers` = shared hit+read blocks at the top plan node; `widest` = the
  plan node with the most row work (Actual Rows × Actual Loops), the same
  metric the depth-oracle tests pin.

## Shape A — the two-dispatcher contract (40 pending, limit 5, oversample 2)

One uncapped actor, one queue, 40 due pending rows. "Contended": a peer
dispatcher holds the top-5 window rows locked in an open transaction while
the measured dispatcher runs the same round.

| shape | variant | rows (OLD → NEW) | buffers (OLD → NEW) | exec ms (OLD → NEW) |
|---|---|---|---|---|
| uncontended | strict_fifo | 5 → 5 | 117 → 133 | 0.795 → 1.068 |
| uncontended | round_robin | 5 → 5 | 119 → 135 | 0.969 → 1.032 |
| contended   | strict_fifo | 0 → **5** | 30 → 118 | 0.446 → 0.849 |
| contended   | round_robin | 0 → **5** | 33 → 121 | 0.671 → 0.923 |

The defect, measured: with a peer holding the window, OLD's second
dispatcher claims **0 rows** — fleet yield stays 5 per round no matter how
many dispatchers are added. NEW claims **5**: the SKIP LOCKED slide walks
past the held rows inside the window and the fleet yields 10 per round with
two dispatchers. The contended round stays sub-millisecond.

## Shape B — the depth oracle (limit 50, oversample 2, one actor/queue/cohort)

| depth | variant | widest node rows (OLD → NEW) | buffers (OLD → NEW) | exec ms (OLD → NEW) |
|---|---|---|---|---|
| 1,000  | strict_fifo | 100 → 100 | 1053 → 1199 | 1.464 → 1.527 |
| 30,000 | strict_fifo | 100 → 100 | 1148 → 1303 | see JIT note |
| 1,000  | round_robin | 100 → 100 | 1058 → 1207 | 1.679 → 1.664 |
| 30,000 | round_robin | 100 → 100 | 1155 → 1310 | 1.565 → 1.789 |

Row work is depth-independent at 100 rows (the standing oracle's contract:
`limit × oversample` candidates plus `limit` locked/eligible) on both sides
of the change; buffers are likewise flat across depths (NEW carries ~+150
constant per round — the slide re-finds each window row by primary key
instead of reusing the pre-lock window's heap references; a per-round
constant, not a depth coupling).

### JIT note (pre-existing artifact, affects the wall-clock at depth)

With default settings (`jit = on`, `jit_above_cost = 100000`), the strict
variant's plan at the 30k shape carries a garbage cost estimate (~2.0M —
the estimate cascade the module docstring describes) that crosses
`jit_above_cost`, so PostgreSQL LLVM-compiles the plan's expressions on
every execution:

| depth 30k, strict_fifo | exec ms (jit on) | JIT functions | JIT optimize+emit ms | exec ms (jit = off) |
|---|---|---|---|---|
| OLD | 674.0 – 778.9 (3 runs) | 193 | 312.5 + 378.6 | **1.449 – 1.553** |
| NEW | 776.6 – 821.9 (3 runs) | 225 | 362.1 + 437.0 | **1.490 – 1.710** |

The compilation (~0.7s OLD, ~0.8s NEW, paid per execution) dwarfs the
actual work (~1.5ms both). This is a **pre-existing** property of the
branch-point statement — OLD pays it identically — not introduced by this
change; NEW adds 32 compiled expressions to a compilation that should not
be happening at all. The execution-work comparison (jit = off) is flat:
OLD 1.449 ms vs NEW 1.490 ms at 30k strict, and 1.562 vs 1.780 at 30k
round-robin (which stays under the JIT threshold at this shape). Reported
as a follow-up candidate: the estimate cascade's side effect on JIT
compilability (e.g. `jit = off` / raised `jit_above_cost` for the
dispatcher connection, or fixing the cascade) — out of scope here.

Under `plan_cache_mode = force_generic_plan` (the documented prior
regression surface), 1k depth, strict: OLD 1.283 ms / 50 rows, NEW
1.537 ms / 50 rows — full yield on both, no under-dispatch.

## Shape C — fleet scope (round's own queues fixed; the rest of the fleet grows)

Two polled queues × two actors × 3 cohorts × 20 rows held fixed; the fleet
grows by 400 unpolled single-cohort (actor, queue) pairs × 25 rows.

| fleet cohorts | variant | widest node rows (OLD → NEW) | buffers (OLD → NEW) | exec ms (OLD → NEW) |
|---|---|---|---|---|
| 0   | strict_fifo | 120 → 120 | 969 → 1058 | 1.440 → 1.416 |
| 400 | strict_fifo | 120 → 120 | 1123 → 1331 | 1.559 → 1.814 |
| 0   | round_robin | 120 → 120 | 987 → 1076 | 1.712 → 1.710 |
| 400 | round_robin | **406 → 120** | **2511 → 1519** | **3.245 → 2.771** |

The `rr_keys` queue-scoping predicates are the visible improvement: OLD's
round-robin cohort enumeration walks every pending cohort in the fleet
(widest node 406 = the Recursive Union over all 406 cohorts at fleet=400),
coupling every queue's dispatch latency to fleet-wide backlog; NEW's walk
is scoped to the round's queues (120 — the round's own cohorts), and the
400-cohort round is cheaper than OLD in both buffers and time. Strict-FIFO
has no cohort enumeration and is flat in both versions.

## Shape D — the new claimable-rows probe (window-expansion gate)

`DISPATCH_CLAIMABLE_PROBE_SQL` runs only on empty rounds, to tell "nothing
pending" (stop) from "window locked out by peers" (expand and re-claim).

| shape | rows | buffers | exec ms | widest node |
|---|---|---|---|---|
| 1,000 due pending | 1 | 3 | 0.032 | 1 (Limit) |
| 30,000 due pending | 1 | 3 | 0.035 | 1 (Limit) |
| empty backlog (idle round) | 0 | 1 | 0.041 | 4 (Function Scan) |

Depth-flat by construction: one LIMIT-1 first-entry index probe per
(registered actor × round queue), the same per-round cost class as the
claim statement's own idle-actor prefilter.

## Verdict

- Two-dispatcher contract: fixed (0 → 5 rows for the second dispatcher;
  fleet yield 5 → 10 per round), with the uncontended round's cost
  unchanged in class (sub-millisecond, ~+16 constant buffers).
- Depth contract: row work and buffers are depth-independent on both sides
  of the change; execution time with JIT disabled is flat vs baseline
  (largest delta +0.22 ms at 30k round-robin). The only large wall-clock
  numbers on either side are the pre-existing JIT-compilation artifact,
  reported above.
- Fleet-scope contract: the round-robin cohort walk no longer grows with
  unpolled fleet cohorts (widest node 406 → 120 at 400 cohorts); all other
  shapes are unchanged within noise.
- The expansion loop adds work only on empty rounds: one probe (3 buffers)
  when idle, and bounded geometric re-claims (≤ 1 + 3 executions) only
  while pending routable rows remain.

---

# Estimate-cascade / registry-scope restructure — before/after measurement

Second wave on the same statement family: the JIT-compile-per-round defect
(the estimate cascade the "JIT note" above flagged as a follow-up) and the
registered-actor-count scaling, fixed together at the source — every
actor_config read in the statement is now a primary-key probe or an
id-array bitmap driven by the round's own pending-rows population, and the
candidate probes' scan bounds are foldable parameter expressions with the
exact `residual * oversample` admission window re-imposed as a rank cut over
the bounded probe output. Plus the operational guard: TaskQ-built dispatcher
pools now carry `server_settings={"jit": "off"}` (worker/deps.py).

## Method

- **OLD**: `src/taskq/backend/_dispatch_sql.py` at `77ccbb2` (pre-fix).
- **NEW**: the working-tree `_dispatch_sql.py` (this change), with migration
  `01.00.13_02` applied (the `jobs_queue_actor_dispatch_idx` walk index).
- Same engine, host, EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) protocol,
  re-seed discipline, and median-of-3 recording as the page above.
  **JIT is left ON** for every measurement — that is the point of the
  exercise (the shapes below are what production connections saw before the
  fix; nothing in the harness touches the `jit` GUC).

## M1 shape — depth axis (limit 50, oversample 2, one actor/queue/cohort)

strict_fifo:

| depth | total cost (OLD → NEW) | JIT ms (OLD → NEW) | exec ms (OLD → NEW) |
|---|---|---|---|
| 1,000   | 62,132 → 4,040 | 0 → 0 | 1.77 / 1.86 → 1.77 |
| 30,000  | 2,016,196 → 4,118 | **1,006 / 986 → 0** | **988-1,008 → 2.03** |
| 200,000 | 15,855,276 → 4,131 | **975 / 998 → 0** | **977-1,001 → 2.20** |

round_robin:

| depth | total cost (OLD → NEW) | JIT ms (OLD → NEW) | exec ms (OLD → NEW) |
|---|---|---|---|
| 1,000   | 13,510 → 4,154 | 0 → 0 | 1.87 / 1.97 → 1.95 |
| 30,000  | 48,428 → 9,921 | 0 → 0 (shape luck, see below) | 1.96-2.01 → 2.37 |
| 200,000 | 296,140 → 9,713 | **80 / 82 → 0** | **82.7-84.4 → 2.24** |

OLD crossed the default `jit_above_cost` (100,000) on *estimated* cost alone
while actual row work stayed bounded at 100 rows: strict from 30k (~2.0M at
the terminal ModifyTable), round-robin at 200k (296k) — the round-robin 30k
escape was shape luck, not a bound, exactly as the depth oracle's
parametrization warned. NEW holds the estimate at ≤ ~10k — a ≥10× margin
under the threshold — at every depth, for both variants, and the JIT block
is simply absent. Under `plan_cache_mode = force_generic_plan` (bounds
opaque) NEW stays at 4.1k / 9.7k with 0 ms JIT and ~2.5 ms exec at 30k/200k
— and because the generic plan's cost never beats the custom plan's, the
planner's own five-execution heuristic never switches a hot dispatcher to
generic, so the per-execution JIT trap the un-fixed cascade set cannot
re-arm itself.

Row work is unchanged at the bounded 100 rows at every depth on both sides
(the standing depth contract); buffers move only by a small constant per
round (strict 200k: 1,636 → 1,804; 1k: 1,339 → 1,242 — the keys walks plus
the bounded rank windows, depth-independent on both sides of the change).

## M2 shape — registry axis (round's own backlog fixed; actor_config grows 0 → 500 → 2,000 unrelated idle actors)

| variant | registry | buffers (OLD → NEW) | exec ms (OLD → NEW) | widest node rows (OLD → NEW) |
|---|---|---|---|---|
| strict_fifo | 0     | 871 → 926 | 1.47 → 1.81 | 70 → 70 |
| strict_fifo | 500   | 2,901 → 1,035 | 2.70 → 1.79 | 1,004 → 70 |
| strict_fifo | 2,000 | 8,985 → 1,035 | 5.69 → 1.91 | 4,004 → 70 |
| round_robin | 0     | 982 → 1,029 | 1.66 → 1.94 | 70 → 70 |
| round_robin | 500   | 3,017 → 1,138 | 3.35 → 2.21 | 1,004 → 70 |
| round_robin | 2,000 | 9,122 → 1,138 | 6.41 → 1.87 | 4,004 → 70 |

OLD ran five unindexed `Seq Scan on actor_config` nodes per round (registry
row work 10 → 2,011-2,512 → 8,011-10,012 as the registry grew); NEW's
actor_config row work is flat at 56 (primary-key probes for the round's own
actors only) and the widest node no longer moves with the registry at all.
The registry-scope oracle's fixture and the fleet-scope oracle's fixture
(now growing actor_config with fleet size, so it can see this dimension)
both pin the NEW numbers.

## Verdict

- The dispatch round no longer pays JIT compilation at any depth: the
  estimate cascade is fixed structurally (folded bounds, honest driver
  cardinalities), not suppressed — the depth oracle's JIT assertion passes
  with JIT *enabled* on a plain connection. The `jit = off` server_settings
  entry on TaskQ-built dispatcher pools is a guard against future estimate
  surprises, not the mechanism of this fix.
- Dispatch cost is now independent of the registered-actor count: the round
  reads actor_config by primary key for its own actors only.
- No regression on the shallow shapes: 1k execution is flat (1.77-1.97 ms
  OLD vs 1.71-2.12 ms NEW across runs), row work and the fleet/cohort/scope
  oracles are unchanged in class, and the fleet-scope fixture now also
  covers the registry dimension.

## A6 — claim-time reservation headroom (consumer-slot isolation gate)

Before/after for the `reservation_holdings` / `reservation_headroom` CTEs
folded into `per_actor_capacity` / `repend_capacity` (this change): the
claim's capacity computation reads live `reservation_slots` occupancy, so a
currently-full actor's rows are not claimed into shared consumer coroutines
whose `acquire_for_actor` must deny them.

- **Method**: the exact harness of
  `tests/test_fleet_saturated_actor_consumer_slot_isolation.py` (one pod,
  `max_concurrency=4`, real `consume_one_job`, a permanently exhausted
  `ConcurrencyReservation(slots=1)` — one hog job holds the slot for the
  whole run — plus 300 flood jobs and 150 healthy jobs on one queue),
  driven twice in one process: **OLD** = `DISPATCH_STRICT_FIFO_SQL` at HEAD
  (`87048c9`), **NEW** = this change, swapped through the
  `SqlTemplates` seam so nothing else differs. Engine: `postgres:18-alpine`
  container (PostgreSQL 18), macOS aarch64, Docker Desktop.
- **Claim-path cost check**: the dispatch oracle suite
  (`test_dispatch_backlog_depth_bound.py`,
  `test_dispatch_actor_registry_scope_bound.py`,
  `test_dispatch_fleet_scope_bound.py`,
  `test_dispatch_cohort_scope_bound.py`,
  `test_dispatch_window_expansion.py`) stays green — the gate adds bounded
  `reservation_slots` probes (live-held slots × jobs-pkey, distinct held
  buckets × slot count) and zero jobs-table row visits when no reservations
  exist, which is the oracles' fixture shape.

| run | baseline jobs/s | contended jobs/s (OLD → NEW) | healthy-throughput loss (OLD → NEW) | denied flood jobs (OLD → NEW) |
|---|---|---|---|---|
| 1 | 197.8 | 142.3 → 197.7 | **28.1% → −6.7%** | 150 → **1** |
| 2 | 198.2 | 149.4 → 185.2 | **24.6% → 0.3%** | 149 → **1** |
| 3 | 204.3 | 108.7 → 201.8 | **46.8% → −0.9%** | 148 → **1** |
| 4 | 208.5 | 150.3 → 198.9 | **27.9% → 0.3%** | 149 → **1** |

(The negative drops are measurement noise on a sub-second drain — the
healthy drain with one coroutine parked on the hog measured no slower than
the uncontended baseline.) OLD matches the finding's four-run range
(25.6–45.4% loss, 150–202 denials per run). NEW leaves exactly the one
bounded first-round denial the gate's design allows (the claim round in
flight when the winning acquire commits); every later round reads the full
bucket and claims none of the saturated actor's rows. The pin (≤ 15% loss)
passes on every repetition; the dispatch-isolation sibling
(`test_fleet_rate_limit_round_occupancy.py`) stays green, and the saturated
actor drains the moment the slot frees (no starvation inversion — pinned by
`tests/test_dispatch_reservation_headroom.py`).

## A7 — backlog-detection sampler, depth-bounded

Before/after for `_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE`
(`src/taskq/worker/_leader_sweeps.py`): the plain
`GROUP BY actor, queue` aggregate over the whole pending set replaced by a
recursive loose-index-scan pair enumeration plus a per-pair probe capped at
`_ACTOR_BACKLOG_SAMPLE_CAP` (1000) rows in dispatch-head order.

- **Method**: the depth-bound pin's own fixture and RLS row-visit oracle
  (`tests/test_backlog_detection_sampler_depth_bound.py`), one
  (actor, queue) pair, TRUNCATE + INSERT + VACUUM ANALYZE per depth;
  wall time is p50 of 7 fetches after one warm run; buffers are
  EXPLAIN (ANALYZE, BUFFERS) shared hit+read blocks summed over all plan
  nodes. Engine: `postgres:18-alpine` container (PostgreSQL 18), macOS
  aarch64, Docker Desktop.

| pending depth | row visits (OLD → NEW) | p50 fetch ms (OLD → NEW) | buffers (OLD → NEW) |
|---|---|---|---|
| 10,000  | 9,999 → **1,001** | 1.65 → **0.40** | 608 → **80** |
| 100,000 | 100,000 → **1,001** | 14.06 → **0.40** | 6,062 → **97** |

OLD visits track depth exactly (10× rows for 10× depth — the red pin's
measured failure); NEW is flat (the cap plus one pair-enumeration seek), a
~35× latency cut at 100k pending *per worker per tick* — the sampler runs
on every worker every `TASKQ_QUEUE_DEPTH_INTERVAL` (default 15 s) by
deliberate design (never leader-gated, so the detector still emits under an
election failure), which is why the per-read cost had to stop scaling with
depth. Series semantics under the cap — depth exact below 1000 / reading
the cap at or above it, oldest_age as head-of-line age — are documented on
the template and in `docs/guides/ops.md`; the `TaskQQueueDepthHigh` alert
(oldest-pending age) is unaffected.
