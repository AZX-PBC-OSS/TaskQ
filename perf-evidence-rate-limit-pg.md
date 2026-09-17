# Perf evidence: Postgres rate-limit fallback round trips (#228)

Before/after for the PG rate-limit acquire paths — the fallback store for
every rate limiter when Redis is unavailable
(`rate_limit_pg_fallback_enabled`, default on), and the primary store for
`backend="postgres"` primitives (the Redis-less fleet shape). The
pre-fusion shapes (#228's finding):

* **Token bucket** (`src/taskq/ratelimit/token_bucket.py::_acquire_pg`):
  BEGIN + `set_config` + SAVEPOINT + preseed INSERT + `SELECT … FOR
  UPDATE` + RELEASE + upsert + COMMIT — **8 round trips** in
  bounded-lock-timeout mode, 5 in indefinite mode (`lock_timeout_ms <= 0`).
* **Log-style sliding window**
  (`src/taskq/ratelimit/_sliding_window_pg.py::_acquire_pg_log`): BEGIN +
  try-lock + DELETE + INSERT + COUNT + COMMIT — **6 round trips**
  (+1 retry SELECT on denial), all but the try-lock inside the advisory
  lock's critical section.
* **GCRA sliding window** (`_acquire_pg_gcra`): the token bucket's shape —
  preseed + `SELECT … FOR UPDATE` + upsert — **8 round trips** bounded,
  5 indefinite.

The fused shapes (this change):

* **Token bucket** — one `INSERT … ON CONFLICT (bucket_name) DO UPDATE …
  RETURNING` whose conflict arm does the elapsed-refill + spend
  arithmetic server-side (the atomic-counter upsert idiom: the conflict
  arm re-fetches the row's latest committed version under the row lock it
  takes, so concurrent first acquires and concurrent spends serialize
  exactly as preseed + `FOR UPDATE` did). The decision rides home as a
  transient `granted` key in the state document (RETURNING sees only the
  final row; `allowed` is not derivable from the token count alone).
  Bounded mode wraps the statement in the enqueue path's pinned
  BEGIN + `set_config` + statement + COMMIT shape (no savepoint — a
  refusal aborts the transaction and the SET LOCAL bound dies with it);
  indefinite mode is the bare statement on autocommit. **4 round trips**
  bounded, **1** indefinite.
* **Log-style window** — the whole locked critical section (prune +
  admission insert + count + retry-hint inputs) is one CTE statement
  under the unchanged two-tier advisory lock. The lock stays a separate
  statement on purpose: a lock probe folded into the work statement would
  read a snapshot that predates its own lock grant (the cron-tick
  lesson, `tests/test_round_trip_budgets.py`). **4 round trips**
  (BEGIN + try-lock + fused + COMMIT), denial included — no extra retry
  statement.
* **GCRA** — the same fused upsert, with the ALLOWANCE as the conflict
  arm's WHERE clause: RETURNING yields a row exactly when granted (a
  cold start is always granted), a denial updates nothing — the
  pre-fused behavior — and one follow-up read (denial only) carries the
  retry hint and the kind-guard discriminator. **4 round trips**
  bounded (+1 on denial), **1** indefinite.

Denial semantics are pinned unchanged by the existing suites
(`rate_limit_blocked_count` accounting, snooze/reschedule routing,
`retry_after=None` for fixed quotas, fail-closed denial with a
one-more-budget hint on lock-timeout — never an admission, never an
exception): `tests/test_ratelimit_pg_row_lock_bounded.py` (rewritten
fakes pin the new shapes),
`tests/test_rt_pools_ratelimit_lock_timeout_guc.py` (GUC hygiene on a
real session), `tests/test_ratelimit_token_bucket_pg.py`,
`tests/test_ratelimit_sliding_window_pg.py`,
`tests/test_ratelimit_sliding_window_pg_lock.py`,
`tests/test_ratelimit_token_bucket_chaos.py` (50 concurrent acquires on
one bucket — the fused statement's serialization),
`tests/test_rt_keyed_bucket_reclamation.py` /
`tests/test_keyed_fixed_quota_eviction.py` (concurrent
evict + re-materialization grants exactly capacity — never double), and
the new round-trip pins in `tests/test_round_trip_budgets.py`.

## Measurement

* **Method**: throwaway `postgres:18-alpine` container (PostgreSQL 18,
  Docker 29.8 on Linux x86_64, a busy shared CI-style host), sequential
  acquires on a single-connection pool, 20 warm-up + 200 measured per
  path; round trips counted by a pool/transaction proxy that counts every
  statement AND every BEGIN/COMMIT/SAVEPOINT/RELEASE boundary (asyncpg
  sends transaction control as its own round trips). Harness:
  `/tmp/opencode/measure_228.py` (scratch, not committed); OLD = the
  pre-fusion files restored from `HEAD` in this worktree, NEW = this
  change — same venv, same container, interleaved runs. The wire shapes
  are additionally pinned deterministically by
  `tests/test_round_trip_budgets.py` on the statement-recording fake.
* **Contention probe** (log-style, the advisory-lock path): 8 concurrent
  racers x 40 acquires on one bucket, total wall time.

| path | round trips/acquire (OLD → NEW) | mean latency run 1 (OLD → NEW) | mean latency run 2 (OLD → NEW) |
|---|---|---|---|
| token bucket, bounded (default) | 8 → **4** | 4.95 ms → **2.73 ms** (−45%) | 4.57 ms → **2.54 ms** (−44%) |
| token bucket, indefinite | 5 → **1** | 4.68 ms → **2.36 ms** (−50%) | 4.59 ms → **2.35 ms** (−49%) |
| log-style window, bounded | 6 → **4** | 2.62 ms → 2.62 ms (flat) | 2.48 ms → 2.65 ms (flat) |
| GCRA window, bounded | 8 → **4** | 4.56 ms → **2.46 ms** (−46%) | 4.58 ms → **2.47 ms** (−46%) |
| log-style window, 8-way contended | — (same 6 → 4 under the lock) | 373 acquires/s | 387 acquires/s (+4%, inside the run-to-run band at this racer count) |

Reading the table honestly:

* The two upsert paths (token bucket, GCRA) halve both their round
  trips and their sequential latency — the removed round trips were pure
  statement+boundary overhead on every acquire.
* The log-style path's sequential latency is FLAT despite 6 → 4: its
  remaining round trips (BEGIN, try-lock, COMMIT) and the one fused
  statement's execution dominate, and the fused statement does the same
  row work the three separate statements did. Its win is structural —
  the advisory lock is now held across ONE statement instead of four,
  which shrinks each holder's critical section (the two-tier lock's
  documented rationale: holder critical-section length IS the contention
  tail). At 8 local racers the measured effect is inside the noise band
  (lock handoff dominates at sub-ms critical sections on loopback); the
  effect grows with round-trip latency (networked PG) and racer count,
  which this host's container cannot demonstrate without inflating the
  numbers.
* Absolute latencies on this shared host (~12 concurrent sibling test
  runs) are inflated relative to a quiet machine; the OLD/NEW deltas are
  the meaningful quantity (same host, same container, interleaved).

## What is deliberately NOT fused

* `_refund_pg` (token bucket) keeps its `SELECT … FOR UPDATE` + UPDATE
  shape: refunds run only on the composition ROLLBACK path (a denied
  acquire unwinding earlier grants — `release_for_actor` sets
  `refund_on_release=False` after the actor ran, so the success path
  never refunds), which is cold, not per-acquire hot path. Same for the
  log-style refund (a single DELETE) and GCRA refund (a single guarded
  UPDATE) — already one statement.
* The maintenance sweeps (`_sweeps.py`) and the keyed reclaim drain:
  bounded batch statements by design, unchanged here.
