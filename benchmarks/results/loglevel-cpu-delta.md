# `TASKQ_LOG_EVENTS_LEVEL`: the warning level's CPU delta, measured

Harness: `benchmarks/loglevel_ab.py` (the audit harness's shape — a real
`worker_main` subprocess, 8 actors, one queue, continuous non-batch
enqueue; runs interleaved `info`/`warning` so drift hits both arms
equally; worker CPU sampled from `/proc/<pid>/stat` around each drain).

The claim this measures honestly: the hot-path audit
(`hotpath-profile.json` on `feat/audit-performance`) attributed ~23% of
the worker's on-CPU time to the logging pipeline — ~2 `state-change`
INFO lines per job through structlog → stdlib → orjson, every line a
streaming duplicate of a `job_events` row the same transaction just
committed. `TASKQ_LOG_EVENTS_LEVEL=warning` suppresses that duplicate
BEFORE the serialization cost; the durable ledger is untouched.

## The numbers (4000 jobs/run, 3 interleaved runs/level)

| metric | `info` (default) | `warning` |
|---|---|---|
| worker CPU per job, median | 0.743 ms | **0.560 ms** |
| CPU delta | — | **−0.183 ms/job = 24.6% of total worker CPU** |
| throughput, median jps | 263.9 | 393.4 |
| `state-change` lines on the stream | 8000 (exactly 2/job — the audit's number) | **0** |
| total log lines on the stream | ~9100 | ~50 |

Cold-run sanity (run 0, cProfile-wrapped): 1.420 → 0.993 ms/job (−30%),
same direction.

cProfile attribution (first run of each level; cProfile inflates
inclusive shares, the audit notes the same — the DIRECTION and the
ratio are the evidence):

| structlog pipeline inclusive share | `info` | `warning` |
|---|---|---|
| `structlog/stdlib.py` info/_proxy_to_logger frames | 12.8% of total_tt | **1.2%** |

The residual 1.2% at `warning` is the filter's own cost (one frozenset
lookup per event) plus the surviving anomaly/lifecycle lines.

## Reading it

- The audit's 23% was py-spy's inclusive sampling share; this harness's
  cProfile puts the same pipeline at 12.8% inclusive at `info` and the
  end-to-end worker CPU drops 24.6% at `warning` — the honest statement
  is "the per-job duplicate stream costs on the order of a fifth to a
  quarter of total worker CPU under this noop-job shape, and the
  `warning` level buys it back entirely."
- What `warning` does NOT remove: the anomaly stream (`job-failed`,
  `heartbeat-tick-failure`, the isolates, the reclaims, the watchdog
  trips, the backpressure refusals) still emits — including the anomaly
  events logged at INFO. `off` is stricter (WARNING-and-above only) and
  still never blinds the operator to failures.
- The ledger pin: `job_events` receives every event at every level
  (asserted against the live DB in `tests/system_e2e/test_log_events_levels.py`).
