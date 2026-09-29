# The worker's hot paths, profiled under real load

Harness: `benchmarks/hotpath_load.py` — the REAL worker bootstrap
(`worker_main`) as its own process: 8 actors on one queue,
`max_concurrency=8`, a client process continuously enqueuing non-batch
jobs at the backend layer (the common dispatch shape). Sampled four ways:
cProfile inside the worker, py-spy (200 Hz) sampling the process from a
privileged container, tracemalloc snapshot-diffing across a 4,000-job
window, and `pg_stat_statements` / `pg_locks` / `pg_stat_activity` read
from the server during the drain. Machine noise bound by an interleaved
3×3 A/B against the unfixed tree (base at `e50e4263`, temp worktree).

## 1. Where the worker's CPU actually goes (py-spy, 745 samples during the drain)

| share | frame | what it is |
|---|---|---|
| 22.7% | structlog `_proxy_to_logger` | **the logging pipeline** — 2 `state-change` INFO lines/job (backend terminal + consumer) + the dispatch lines, each through the full structlog→stdlib→JSON render |
| 22.6% | `_bootstrap._guarded` | the sibling-lifetime wrapper — inclusive time = the loops' own time |
| 18.1% | `dispatch_one_job` | the per-job DI dispatch |
| 21.3% | `mark_succeeded` | the terminal write (19.7% of it is its state-change log line) |
| 12.8% | `producer_loop` / `dispatch_batch` | the claim rounds |
| 7.9%  | `consume_one_job` | the consumer's attempt machinery |

Exclusive (self) time: asyncio machinery (`events._run` 3.1%, socket
read/write ~6%, `create_task` 1.9% — the two loops' wait-racing task
churn), asyncpg `_do_execute`/`_get_statement` ~2.9%. taskq's own code is
thin: `dispatch_one_job` self ≈ **84 µs/job**, `_job_row_from_record`
≈ 65 µs/job (4 jsonb decodes), `consume_one_job` ≈ 45 µs/job.

**The numbers' #1 named cost is logging: ~23% of on-CPU under load.**
It is a byte-contract (the audit trail's two state-change lines per job),
not redundant work — left untouched.

*(cProfile note: CPython 3.13's profiler double-counts coroutine ncalls;
verified exactly 1 `dispatch_one_job` per job with a runtime counter.
The pstats file: `benchmarks/results/hotpath-worker-base.pstats`.)*

## 2. Round-trip amplification (pg_stat_statements, 6,000-job drain)

| statement | per job | cost |
|---|---|---|
| terminal UPDATE (the `WITH upd …` claim-fenced write) | 1.000 | 0.34 ms |
| dispatch CTE round | 0.259 | 0.53 ms/round |
| pool-release reset (`CLOSE ALL; pg_advisory_unlock_all(); UNLISTEN *; RESET ALL` — one round trip) | 1.30 | asyncpg library behavior |

≈ **6.5 statements / ~3.3 round trips per job**, of which taskq's own
writes are 1.26 statements/job. The dominant family is asyncpg's
release-reset (every slot/terminal checkout pays it); removing it would
drop the release-reset safety net (lingering SETs / LISTENs / advisory
locks across jobs) — a behavior change, so documented, not fixed.
The autonomous path runs no per-job transaction (BEGIN/COMMIT only
65/6000). The **slot-pool (transactional) shape** adds BEGIN/COMMIT
1.02/job and SAVEPOINT/RELEASE 1.00/job — the deliberate per-job
`SAVEPOINT _tq_actor` integrity machinery, not a redundancy.

## 3. Per-tick allocations (tracemalloc, 4,000-job window, in-process)

**+52.7 B/job, +0.9 objects/job** net growth. The per-claim dicts are
freed within the cycle: the per-job path is allocation-neutral, no leak
(`benchmarks/results/hotpath-tracemalloc.json`).

## 4. Lock contention (250 ms sampling during the drain)

**Zero ungranted `pg_locks`** at every sample; `pg_stat_activity` wait
events are exclusively `Client:ClientRead`. No DB-side contention in the
dispatch path at this scale.

## 5. Polling waste (idle worker, NOTIFY on)

An idle worker issues **18.3 statements/s**: the leader loops' 1 s
cadences (cron schedule read, `scheduled_to_pending` snapshot, sweep,
per-checkout BEGIN/COMMIT + `set_config`). The cadences themselves are
settings-driven defaults (behavior, not code) — but the per-tick SQL
**re-render inside them was code**: `sweep_scheduled_to_pending`
re-rendered a **19,793-char template plus 2 more on every ~1 s tick**,
byte-identical every time.

## 6. The insights page's six reads (the N+1 candidate)

Measured on a populated ledger: **13.7 ms** sequential, of which the six
round trips are **0.6 ms (4.4%)** — the aggregates dominate. asyncpg
cannot pipeline distinct parameterized statements on one connection
(one-operation-at-a-time), and gathering across pool connections breaks
the route's one-checkout snapshot discipline. **Not fixed** — it would
be a speculative optimization.

## 7. The fix (the one the numbers named)

`_render_sweep_sql` / `_render_event_ttl_base_sql` (lru_cache 64) in
`backend/_sweeps.py`: the ten sweep render sites render once per
(template, schema) instead of per call; the `_IDENT_RE` check still runs
before every interpolation (an invalid identifier raises the same
ValueError per call, the cache never serves an unvalidated render).
`backend/postgres.py`'s `heartbeat_jobs` / `extend_reservation_leases`
render once at backend init (the schema is fixed for its lifetime).

**Red/green** (`benchmarks/ab_sweep_sql_render.py` →
`benchmarks/results/hotpath-fix-sweep-render.json`):
direct render **1,242 ns/call** → cached **70 ns/call** (**17.8×**),
byte-equality asserted per (template, schema), invalid-identifier
behavior unchanged. The leader's ~1 s tick stops paying ~3 renders
(~1.2 µs) plus ~25 KB of per-tick string churn.

**e2e A/B on the same harness** (interleaved 3×3, 6,000 jobs each):
base median **298 jps**, after median **287 jps** — unchanged within run
noise, as expected: the fix removes the leader's per-tick render, not
per-job work. Conservation: the per-job statement shape is identical
before/after (terminal 1.000, dispatch rounds 0.259, resets 1.30).

## 8. Gates

- pure/unit tier (`pytest -m "not integration"`): **8,457 passed ×3**
- touched families (sweeps, heartbeat, postgres backend, validation):
  **247 passed ×3**
- `ruff check` + `ruff format --check`: clean, repo-wide
- `pyright src/taskq tests`: **0 errors**

Raw artifacts (gitignored, regenerable via the harness):
`hotpath-worker-base.pstats`, `hotpath-pyspy-base.json`,
`hotpath-tracemalloc.json`, `hotpath-worker-slot.pstats`.
