# The batch-hot path's dominant cost: the per-job failure-counter reset — root cause, red/green proof

The latency red-team's batch-shape finding (`attack-hotpath-findings.md`)
measured `reset_batch_failures` — the `UPDATE batches SET
consecutive_failures = 0` every successful job of a batch pays through
`apply_batch_terminal_outcome` — at **8.2 ms/job, 24× the terminal UPDATE's
0.34 ms**, with `Lock:transactionid`/`Lock:tuple` waits and ungranted
`pg_locks` counts up to 5.

## Root cause (named from evidence, not preference)

Three measurements decompose the 8.2 ms:

1. **The statement's real work is ~0.09 ms.** `EXPLAIN (ANALYZE, BUFFERS)`
   of the reset on a seeded 200-member batch: the batches-row UPDATE
   0.049 ms (4 buffer hits) + the LATERAL open-member count 0.038 ms
   (200 index entries, 8 hits, `jobs_batch_open_members_idx`). No missing
   index, no seq scan (the existing family
   `tests/test_batch_completion_cost_pg.py` already pins the plan).
2. **The cost is the batches-row LOCK QUEUE.** On the fleet shape
   (2,000 jobs / 200-member batches / 8 concurrent terminal
   transactions, harness `benchmarks/batch_reset_hotspot.py`, PG 18.6),
   `pg_stat_statements` puts the reset at **34.5–61.1 ms mean**
   (68.9–122.1 s of statement time across a 10.3–18.0 s drain) while the
   same run's terminal UPDATE measures **0.195 ms** and
   `complete_batch` — which touches the same row through `FOR UPDATE
   SKIP LOCKED`, never waiting — **0.04 ms**. The row is not the cost;
   WAITING for it is: every successful job's reset wrote the batches row
   even when `consecutive_failures` was **already 0** (the overwhelmingly
   common case — the noop actors never fail), took the row's tuple lock,
   and held it to its transaction's COMMIT, so the batch's concurrent
   terminal transactions queued on that row for the holders' whole
   remaining transaction (terminal write + hook + commit). The reset's
   own write + member count were the *pace* of the queue, not its depth.
3. **The round trips are secondary.** Sequentially (one connection, one
   transaction), the reset costs 0.23 ms/call; through the hook's
   own-transaction shape, 5.6 ms/call of per-transaction overhead
   (savepoint + `lock_timeout` GUC pair + commit) — which is the ~5 ms
   per-job wall the fleet's per-worker latency shows once the queue is
   removed. These round trips are unchanged by this fix and are not the
   24× term.

So the hypothesis resolves as **(a)+(b), one mechanism**: the reset ran
unconditionally per job, and its unconditional batches-row write is what
serialized the batch's concurrent terminal transactions. (c) is refuted
by the plan pins (the probe is index-served); (d) exists but is the
~5 ms/jp baseline, not the 24× defect.

The fix: `AND consecutive_failures <> 0` in the reset's WHERE. A zero
counter matches no row → **no tuple lock is taken** (nothing queues) and
the LATERAL member count **never executes** (it hangs off the UPDATE's
returned rows). A non-zero counter resets byte-identically.

### The consumers that pin the counter contract (grep'd, named)

Readers of `reset_batch_failures`' return / the counter column, all
verified compatible:

- `src/taskq/batch.py` (`apply_batch_terminal_outcome`) — the only
  production caller; discards the return.
- `increment_batch_failures` — the threshold decision reads the counter;
  the guard changes no non-zero→0 transition (a 0→0 write is a no-op on
  the value), so every threshold decision is unchanged.
- `get_batch` / `list_batches` — expose the column; value transitions
  identical.
- `tests/test_batch_pg.py::TestPostgresResetBatchFailures`,
  `tests/test_in_memory_batch.py::TestInMemoryResetBatchFailures` —
  assert the reset RETURNS the member count and zeroes the counter after
  real increments (count > 0 — the guard passes; untouched).
- `tests/test_rt_diff_batch.py`, `tests/test_rt_diff_harness.py` — record
  resets on aborted/missing rows only (both backends return the same 0).
- `tests/test_in_memory_seam_registry.py` — "returns a count" type claim;
  still true.

**Refuted alternative** — "reset at batch completion only (one write
instead of N)": the increment's threshold decision reads the counter
mid-batch (`count >= threshold`), so deferring the reset changes which
count fires the abort — observable, contract-breaking. Rejected.

## The completion invariant the fix had to restore (and did)

The reset's lock queue accidentally **sequenced** every batch member's
completion attempt after the earlier members' commits (a row lock is
granted only at the previous holder's COMMIT), which is what made
"`apply_batch_terminal_outcome`'s attempt after the last terminal write is
the one that lands" hold. Guarding the reset removed the serialization and
exposed a lost-wakeup hole: the last committer's `complete_batch` attempt
snapshots before its peers commit, vetoes on members it saw open, and no
attempt exists after the final commit — an all-terminal batch stayed
`'active'` (measured: 10/10 batches uncompleted in the first fixed
harness run; the existing pin
`test_batch_completion_cost_pg.py::test_concurrent_member_terminal_writes_complete_the_batch_exactly_once`
catches it too).

Restored minimally in `complete_batch` (`src/taskq/backend/_batch_sql.py`):

- **Tail gate**: one cheap open-member count; a mid-drain snapshot
  (> 16 open members, `_COMPLETE_TAIL_RECHECK_MAX_OPEN` — one power of two
  above the fleet's 8 concurrent terminal writers) would veto anyway, so
  it returns and never touches the batches row. Outcome-identical to the
  old attempt (the old guard vetoed the same attempt); the count probe is
  the one probe the reset used to pay, moved.
- **Bounded blocking handshake** for tail-shaped attempts: `SELECT ... FOR
  UPDATE` under a savepoint-scoped `lock_timeout` (the counter writes'
  own machinery and budget), then the guarded completion write on the
  fresh post-grant snapshot. Grant ⇒ every earlier holder committed ⇒ the
  last committer lands. On expiry — the streaming appender's membership
  lock, the one unbounded holder class (the conflict the former
  `FOR UPDATE SKIP LOCKED` existed for) — the same disclosed delay as
  before (`complete_batch_delayed_membership_lock`), never an exception
  into the caller's transaction.

A pure re-issue/sleep loop was tried first and REJECTED on evidence: the
peers' terminal writes commit only after their own hooks return, so
simultaneously-reissuing tail attempts exhaust their window against each
other (livelock — 20-way and 24-way repro, batches stayed 'active' with
40 × 2 ms re-issues) and a mid-drain veto re-issued 40 × 2 ms would
re-create the per-job cost the fix removes.

## Red/green numbers

Harness `benchmarks/batch_reset_hotspot.py`: B batches × 200 members, each
member driven through the REAL terminal path (terminal job UPDATE +
`apply_batch_terminal_outcome("succeeded")` in its own transaction) by 8
concurrent workers; `pg_stat_statements` per-statement means + ungranted
`pg_locks` sampling. Same box (32-core), same PG 18.6 container, both arms
re-run after the fingerprint fix. Artifacts:
`batch-hotspot-main-2000j-8c.json`, `batch-hotspot-fix-2000j-8c.json`.

| metric (2,000 jobs, 10×200 batches, 8 workers) | main (unguarded) | fix (guarded) | |
|---|---|---|---|
| reset `pg_stat_statements` mean | 61.07 ms | **0.007 ms** | ~8700× |
| reset total across the drain | 122.1 s | 0.014 s | |
| reset mean (earlier, less-loaded run) | 34.46 ms | 0.005 ms | |
| terminal UPDATE mean (same run) | 0.195 ms | 0.191 ms | reference |
| reset ÷ terminal UPDATE | **313× (61.07/0.195)** | **0.04×** | the finding's 24× class, pinned |
| `complete_batch` mean | 0.04 ms (2,000 calls) | 0.037 ms (123 calls) | the tail gate: 123 of 2,000 attempts sought the lock |
| per-job terminal-path wall | 9.02 ms | **1.35 ms** | 6.7× |
| drain wall | 18.05 s | 2.70 s | 6.7× |
| ungranted `pg_locks` max | 7 | 7 | now the tail handshake's bounded queue only |
| batches completed after the drain | 10/10 | **10/10** | the invariant, 5/5 fixed runs |

(The first fixed-tree harness run measured reset mean 0.005 ms with the
drain at 1.14 s and per-job wall 0.569 ms; the committed artifacts are the
re-run pair above, taken minutes apart under identical flags.)

## The pins (`tests/test_batch_reset_hotspot_pg.py`, integration lane)

1. **Mechanism**: `EXPLAIN (ANALYZE)` of the reset against an ACTIVE batch
   whose counter is 0 — the batches UPDATE must return 0 rows (no tuple
   lock) and the member probe must be "(never executed)". RED on main
   (rows=1, probe executed).
2. **Cost**, the documented arithmetic as a same-run ratio: the hook's
   contended per-job wall ≤ 5× the bare terminal UPDATE's, both measured
   in this run on this PG (main measured 7.56–7.87× — RED, 3/3; the
   finding's fleet ratio is 24×; the fixed tree measures 3.2–3.3×, 3/3
   GREEN).
3. **The trap**: a batch with REAL failures still counts (2 increments →
   counter 2 through the hook) and still resets (successes → 0 through
   the hook); a post-drain protocol-level reset still returns the member
   count.
4. Concurrent failures under contention: 8 concurrent failed outcomes →
   counter exactly 8 (the increment path is untouched).
5. **The tail invariant**: 24 concurrent successes through the hook → the
   batch is `'complete'` with one `completed_at` and no sweep — RED on
   the guard alone, GREEN with the handshake.

## Gates

- `ruff check` + `ruff format --check` clean (`src`, `tests`, `benchmarks`).
- `pyright src/taskq tests benchmarks/batch_reset_hotspot.py` — 0 errors.
- Batch families + the new module (`test_batch_pg`, `test_batch_completion_cost_pg`,
  `test_in_memory_batch`, `test_batch_complete_guard`, `test_rt_diff_batch`,
  `test_rt_queryrace_batch_complete_vs_append`, `test_backend_semantic_parity_registry`,
  `test_in_memory_seam_registry`, `test_rt_diff_harness`,
  `test_batch_reset_hotspot_pg`) — **124 passed ×3**.
- Fast lane `pytest -m "not integration" -n 4` — **8,521 passed, 3 skipped**
  (the sweep-audit registry gained `_LOCK_BATCH_ROW_SQL`, a SELECT FOR
  UPDATE — no write — matching the walk only through its own keyword).

## Red-team addendum (the transactional-caller stuck-batch class, found and fixed)

The red-team pass over the tail gate found a hole the free-running pins
cannot see: the gate's early return (`open_members > 16`) is UNORDERED with
its caller's own commit. On the transactional-caller shape
(`apply_batch_terminal_outcome(..., transaction_conn=conn)` inside the
terminal write's transaction — the shape this benchmark and the pins use,
and the hook's documented `connection=` arm), a writer that gated out never
touched the batches row, so its commit is sequenced against no attempt at
all. Two deterministic shapes left an all-terminal batch `'active'` with no
attempt left to land (origin/main completes both — its unconditional reset
queues every writer on the row; measured on PG 18.6, same-run pairs):

- **The burst**: N-member batch, all terminal writes overlapping behind one
  barrier — every gate probe counts N-1 open (17 for N=18 > the 16 bound),
  so EVERY attempt gates out. Stuck 5/5 for N=18, 20, 24, 32; 0/5 for N=17
  (the cliff is exactly the bound + 1).
- **The late committer**: one writer gates out while its 23 peers are open,
  then its COMMIT outlives every peer's completion statement (the peers'
  tail attempts all veto on its uncommitted member). Stuck 5/5.

The fix: the gate-out is surfaced to the caller — `_batch_sql.complete_batch`
returns arbitrated/gated-out, `PostgresBackend.complete_batch` returns
"reissue owed" (`True` only when the attempt gated out on a connection still
inside its transaction), and `apply_batch_terminal_outcome` propagates it.
The caller honors it with ONE awaited post-commit
`backend.complete_batch(batch_id)` (no `connection=`): that attempt's
snapshot postdates every commit, so the last committer's reissue lands —
the autonomous shape (the consumer's hook runs after `consume_one_job`'s
commit) is never owed one and pays nothing. A detached-task reissue was
tried first and rejected on evidence: it leaves tasks pending at test
teardown (the suite's no-orphan-task discipline) and polling the caller's
connection races its pool release.

Reruns after the fix (same harness, callers honoring the reissue):
burst N∈{17,18,20,24,32} ×10 and late-committer ×10 — 0 stuck, all
`'complete'`, no sweep; the fail-last shape (final member fails: completion
below threshold, abort at threshold) ×10 — abort fires exactly once, never
missed; a 3s unbounded batches-row holder (the streaming-append class) —
increments skip (the disclosed M7 loss: counter frozen at 0), terminal
writes survive, completion delays without raising, the leader sweep
recovers the batch 10/10 (identical to main's behavior under the same
holder; main's completion skips instantly where the tail gate parks the
full 2s budget — the delay class is disclosed, now measured).

Benchmark honesty rerun (this box is faster than the original run's, so the
absolute walls differ; the mechanism evidence reproduces exactly):

| run | drain s | per-job ms | reset mean ms | reset/terminal ratio |
| --- | --- | --- | --- | --- |
| main (rerun) | 5.176 | 2.588 | 16.981 | 129x |
| fix (rerun, reissue included) | 4.099 | 2.049 | 0.006 | 0.04x |

The ≤5x same-run ratio budget holds (0.04x); `active_batches_after` 0/10 on
both. The PR's original absolute numbers (9.02 → 1.35 ms/job) do not
reproduce on this hardware — the before/after direction and the reset
statement's collapse (16.98 → 0.006 ms mean) do.

Gates after the red-team fix: ruff/format clean; pyright 0 errors on the
touched files; batch families + concurrency pins **169 passed ×3**; fast
lane **8,522 passed**.
