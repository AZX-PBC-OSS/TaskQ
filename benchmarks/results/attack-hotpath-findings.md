# The hot-path audit's claims, attacked: the alternative-shape re-profile

The audit (`hotpath-profile.md`, base `584757c6`) profiled ONE load shape —
8 actors drained CONTINUOUSLY with non-batch jobs. This adversarial pass
re-profiled the shapes the audit skipped, re-derived its numbers from its
own raw artifacts, and re-ran its gates. Harnesses:
`benchmarks/attack_hotpath_shapes.py` (batch-heavy + bursty client shapes
against the audit's own `worker_main` bootstrap and actors verbatim) and
`benchmarks/attack_insights_scale.py` (the six-read page at 1/50/200
queues). Environment: same 32-core box, PG 18.6, `TASKQ_PG_DSN` on port
5499. py-spy could NOT be re-run (ptrace_scope=1, no root — the audit's
own py-spy artifact was instead re-derived from raw samples, below).

## Verdicts, per claim

### 1. "the profile's numbers represent the hot paths" — OVERTURNED for the batch shape, HOLDS for the bursty shape

- **Bursty** (20 × 400-job slams, 2 s gaps, 8,000 jobs): the cProfile rank
  order HOLDS — epoll poll, lock acquire, `dispatch_one_job` (2.8% self),
  `consume_one_job` (1.8% self) — and the per-job statement shape is the
  audit's (terminal 1.000, dispatch rounds 0.27/job, BEGIN/COMMIT
  ~0.02/job). A profile of the continuous shape does not mislead about the
  bursty shape's per-job costs. (`attack-shape-burst.json`,
  `attack-worker-burst.pstats`)
- **Batch-heavy** (the REAL batch path: client `enqueue_batch` with
  `batch_id` stamped + a batches row, so the worker pays
  `apply_batch_terminal_outcome`; 8,000 jobs in 200-job batches): the
  audit's shape MISSES this shape's dominant cost. The batch-failure
  reset (`UPDATE batches SET consecutive_failures = 0`) ran **8.2 ms/job
  (65.3 s of execution across a 42.5 s drain) — 24× the terminal UPDATE's
  0.34 ms** — every one of a batch's terminal writes serializing on the
  batches row, plus BEGIN/COMMIT + `set_config` 1.0/job the autonomous
  path doesn't pay. Wait events include `Lock:transactionid` /
  `Lock:tuple` and ungranted `pg_locks` up to 5 — the audit's "zero
  ungranted locks, Client:ClientRead only" verdict is shape-bound and
  does NOT extend to batch-heavy fleets. (`attack-shape-batch.json`,
  `attack-worker-batch.pstats`) The audit never claims the batch path —
  but its "taskq's own per-job code is thin" verdict reads as general
  when it is continuous-shape-only. The batch terminal-write
  serialization is an UNPROFILED, UNDISCLOSED per-job cost class.

### 2. "logging is ~23% of on-CPU" — the py-spy arithmetic reproduces; the number is profiler-fragile and the contract grounding is PARTLY honest

- Re-derived from the audit's own raw speedscope samples
  (`hotpath-pyspy-base.json`, 7.46 s weighted, 2 threads): `_proxy_to_logger`
  **22.9%**, `log_state_change` 19.8%, `_guarded` 22.5%, `mark_succeeded`
  21.3% — the md's table reproduces exactly.
- But the audit's OWN cProfile artifact
  (`hotpath-worker-base.pstats`) puts `log_state_change` at **3.6%**
  cumulative, and an independent continuous-shape run on this tree puts
  it at 7.9%. The 23% is a py-spy-only number (wall-clock sampling that
  counts the render + file-write syscall inside the logging frames);
  presenting it as "~23% of on-CPU" without the cProfile disagreement
  overstates its certainty by ~3-6×.
- The contract grounding: REAL. `docs/guides/observability.md` documents
  `state_change` at info for "Any job status transition", and
  `docs/guides/upgrading.md` records the `state_change` → `state-change`
  rename as breaking for log pipelines ("your saved searches and alert
  rules stop matching"). The two lines per job are a documented event
  contract. BUT the contract mandates the lines' EXISTENCE, not the
  pipeline's implementation cost — the refusal shields the render
  pipeline (processors, JSON serialization, handler lock) with a
  documentation claim that only covers emission. The honest refusal is
  "the events are contractual; the per-line render cost is separable and
  unexamined." As written, the md's "byte-contract … left untouched"
  conflates the two.

### 3. "+52.7 B/job, allocation-neutral, no leak" — NO LEAK HOLDS; the NUMBER does not

- Three independent continuous-shape runs (5,000-job windows): **92.3 /
  95.2 / 77.1 B/job** — the audit's 52.7 is 1.5-1.8× below every rerun.
  The absolute number is environment-unstable, and the audit's own top
  diffs are tracemalloc's self-bookkeeping (`tracemalloc.py:115/:193`,
  +250 KB of the diff) — the profiler measuring itself.
- Across shapes/windows (`attack-tracemalloc-{batch,burst}.json`,
  cumulative windows every 1,000 jobs): batch 69→244→135→132→69→99→83
  B/job; burst 83→201→164→107→83→79→65 B/job. NO monotonic growth
  anywhere. The "no leak, the per-claim dicts are freed within the
  cycle" verdict **HOLDS under every shape tested** — including the
  batch path the audit never measured.
- Note: the checkpoint hook counts only the noop actors' completions;
  under the batch shape the reset/complete writes' allocations land in
  the same windows and still show no slope.

### 4. "insights six-read N+1: 0.6 ms of 13.7 ms (4.4%), not fixed" — refusal HOLDS, and the share only SHRINKS with scale; the md omits that

`attack_insights_scale.py` times the route's six fetches, in order, one
checkout, warm pass, 6,000 terminal jobs:

| fleet | six reads | share the six round trips could save |
|---|---|---|
| 1 queue, 4 actors | 12.0 ms | ~5% |
| 50 queues, 200 actors | 24.1 ms | ~2.5% |
| 200 queues, 800 actors | 29.1 ms | ~2% |

The aggregates grow with the fleet; the six round trips stay ~constant —
pipelining's bounded ~0.6 ms saving becomes MORE pointless as the fleet
grows. The refusal's conclusion survives every scale tested. The 4.4%
figure is a scale snapshot presented without its scale bound (the md
says "on a populated ledger" and stops); the honest caveat — the saving
is bounded by ~5 RTTs and the share SHRINKS with fleet size — is absent
from both the md and the JSON's verdict field.

### 5. "base 298 vs after 287 jps — unchanged within run noise" — NOT DEMONSTRABLE

- The base arm's recorded runs: **[482, 298, 281]** (`hotpath-profile.json`
  `workload.sustained_throughput_jps`) — a 42% spread with an unexplained
  482 warmup outlier that no stated rule excludes.
- The after arm's runs are recorded NOWHERE: no artifact carries the
  three after-tree runs; only the median "287" appears in prose (md §7,
  fixes[0].throughput_note). The red/green artifact
  (`hotpath-fix-sweep-render.json`) covers the microbenchmark only.
- With n=3 per arm, a base spread of 482→281, and no after-arm raw data,
  the claim "unchanged within run noise" has no auditable statistical
  content. Corroborating: three same-tree continuous runs on this box
  (162/167/172 jps — `attack-*.json` + control) sit 46% below the audit's
  recorded base median, so cross-run/cross-environment noise dwarfs the
  3.7% delta being asserted away. The honest statement: "the fix targets
  the leader's per-tick render, not per-job work; the A/B is underpowered
  (n=3, ±40% spread) and cannot detect a small regression either way."
- Same class of problem: the md's cProfile self-time figures (84 / 65 /
  45 µs/job) do NOT reproduce from the artifact they cite — the committed
  `hotpath-worker-base.pstats` gives **42.2 / 13.6 / 20.1 µs/job**
  (self ÷ 6,000 jobs). The md's numbers are 2-5× the artifact's and no
  derivation is recorded.

## What was re-run (reproducibility)

- `attack_hotpath_shapes.py orchestrate --shape batch --jobs 8000
  --tracemalloc-jobs 1000` → `attack-shape-batch.json`,
  `attack-tracemalloc-batch.json`, `attack-worker-batch.pstats`
- same, `--shape burst` → `attack-shape-burst.json`,
  `attack-tracemalloc-burst.json`, `attack-worker-burst.pstats`
- `hotpath_load.py orchestrate` (continuous control + 2 tracemalloc
  repeats) → `attack-tracemalloc-cont-{1,2}.json`
- `attack_insights_scale.py` → `attack-insights-scale.json`
- gates on this tree: `pytest -m "not integration"` **8477 passed, 3
  skipped** (once; the audit claimed 8457 ×3), `ruff check` +
  `ruff format --check` clean repo-wide, `pyright src/taskq tests`
  **0 errors**.

No src changes: the batch-path serialization finding is a measurement
disclosure, not a proven defect (no red proof was produced against it —
fixing it is a behavior-relevant design change that outscopes this pass).
