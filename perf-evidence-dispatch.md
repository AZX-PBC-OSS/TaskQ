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
