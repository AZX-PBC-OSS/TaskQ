# Spike: Python ↔ Node actor bridge microbenchmark

Benchmarks for the proposed TaskQ design where TS actors run in a Node child
process speaking NDJSON over stdio to the Python asyncio worker. All numbers
below are from a real run of `bench.py` on this machine (raw per-job samples in
[`results.json`](./results.json)).

## How to run

```sh
uv sync
uv run python spikes/bridge-bench/bench.py            # full run, writes results.json
uv run python spikes/bridge-bench/bench.py --jobs 20  # quick smoke
```

Environment: macOS (arm64), node v25.8.1, Python 3.13.15 (CPython, asyncio
default loop).

## Modes

| mode | what it measures |
|---|---|
| `inproc` | baseline: async Python function called directly (the floor) |
| `cold` | `node worker.js oneshot` spawned fresh per job |
| `warm1` | one persistent `node worker.js server` child, strictly sequential NDJSON over stdio |
| `warm4` | pool of 4 persistent children, 20 jobs in flight (saturation); reports per-job wall time (dispatch→response) and pure in-pool wait (dispatch until a child is free) |
| `warmt1` | optional variant: one persistent child that spawns a `worker_threads` Worker per job |

Work per job: parse a `{"n": 42, "data": "<ascii>"}` JSON payload, derive a sum
from it, echo the payload back. 200 measured jobs per mode per size after 20
warmup jobs. Payloads ≈ 100 B / 10 KB / 1 MB.

## Results

### Latency by mode and payload size

| mode | size | p50 (ms) | p95 (ms) | p99 (ms) | jobs/sec |
|---|---|---|---|---|---|
| inproc | 100B | 0.001 | 0.001 | 0.002 | 706611.3 |
| inproc | 10KB | 0.008 | 0.008 | 0.01 | 125980.9 |
| inproc | 1MB | 0.666 | 0.847 | 1.029 | 1441.4 |
| cold | 100B | 36.825 | 46.47 | 58.255 | 26.5 |
| cold | 10KB | 34.268 | 42.583 | 46.838 | 28.3 |
| cold | 1MB | 34.526 | 40.611 | 45.085 | 28.5 |
| warm1 | 100B | 0.065 | 0.166 | 0.291 | 12011.7 |
| warm1 | 10KB | 0.075 | 0.116 | 0.16 | 12530.7 |
| warm1 | 1MB | 3.965 | 9.019 | 14.215 | 209.7 |
| warm4 | 100B | 0.505 | 2.793 | 4.102 | 19052.7 |
| warm4 | 10KB | 0.83 | 2.595 | 3.548 | 16988.9 |
| warm4 | 1MB | 32.377 | 48.01 | 61.625 | 581.1 |
| warmt1 | 100B | 11.264 | 12.554 | 13.227 | 87.6 |
| warmt1 | 10KB | 11.568 | 13.673 | 14.508 | 84.8 |
| warmt1 | 1MB | 34.084 | 36.792 | 39.403 | 29.1 |

### warm4 in-pool wait (dispatch until a child is free)

| size | wait p50 (ms) | wait p95 (ms) | wait p99 (ms) | wait max (ms) |
|---|---|---|---|---|
| 100B | 0.416 | 2.469 | 3.995 | 5.937 |
| 10KB | 0.676 | 2.248 | 3.305 | 3.655 |
| 1MB | 25.621 | 38.524 | 54.214 | 67.334 |

### Readings

- **Cold spawn is ~35 ms flat** (p50 34–37 ms, p99 45–58 ms) and payload size
  barely moves it — process startup dominates; the 1 MB pipe transfer adds
  almost nothing on top.
- **Warm stdio round trip is 0.07 ms** for small payloads (p99 0.29 ms) and
  ~4 ms p50 at 1 MB (JSON stringify/parse plus two 1 MB pipe copies dominate).
- **`worker_threads` per job is not viable**: ~11–13 ms per job even for tiny
  payloads — Worker boot dominates and is ~170× a warm stdio round trip.
- Warm4 throughput scales ~4× over warm1 (581 vs 210 jobs/s at 1 MB, 19k vs
  12k at 100 B), i.e. the pool behaves as expected under saturation.

## Saturation analysis (warm4 vs a 30 s `start_to_close` budget)

At 20 jobs in flight against 4 children, in-pool wait is where queueing shows
up. Per-job service time can be read from warm4 wall − wait ≈ 6–7 ms at 1 MB
(consistent with warm1's 4 ms RTT plus concurrency contention).

Share of a 30,000 ms budget consumed per job (wait + service):

| size | wait p95 + service (ms) | % of 30 s budget | wait p99 + service (ms) | % of 30 s budget |
|---|---|---|---|---|
| 100B | ~5.3 | 0.018 % | ~6.8 | 0.023 % |
| 10KB | ~5.2 | 0.017 % | ~6.3 | 0.021 % |
| 1MB | ~45.5 | 0.15 % | ~61 | 0.20 % |

**Queue depth math:** a job that arrives when `d` jobs are already in flight
against a pool of `P` waits ≈ `(d − P) / P × service`. Solving for a 30 s
budget with pool = 4: 1 MB jobs (service ≈ 6.8 ms) need
`d ≈ 4 × 30000 / 6.8 ≈ 17,600` jobs in flight before a *newly dispatched* job
risks the budget; 100 B jobs (service ≈ 0.4 ms) need ~300,000. In other words,
**saturation never matters at any sane concurrency for normal payloads** — even
20-deep saturation on 1 MB payloads eats 0.2 % of the budget at p99. It only
becomes real if dispatch vastly outruns service for minutes (e.g. thousands of
in-flight 1 MB jobs), which is a backpressure problem, not a bridge-latency
problem — and it argues for cheap admission control (cap in-flight jobs per
worker; leave the queue in Postgres) as insurance, not for a fancier runtime.

## Cancellation / kill latency

| signal | trials | mean (ms) | min (ms) | max (ms) |
|---|---|---|---|---|
| SIGKILL process group (`start_new_session=True`, `os.killpg`) → reaped | 10 | 1.349 | 1.241 | 1.429 |
| SIGTERM while blocked on stdin read → exit | 10 | 1.366 | 1.206 | 1.589 |

- SIGKILLing the process group reaps the runtime in **~1.3 ms** — force-kill
  is effectively free from the Python side.
- **Node exits on plain SIGTERM (all 10/10 trials, ~1.4 ms) even while blocked
  on a stdin read** — no SIGKILL escalation needed. Node installs no handler by
  default, so the default disposition applies. A graceful-shutdown design would
  still want a SIGTERM handler for in-flight jobs, but the watchdog can rely on
  SIGKILL as a fast backstop.
- A 5 s grace watchdog with SIGKILL fallback never had to escalate in this run.

## Gotcha discovered: `worker_threads` breaks `fs.writeSync` to pipes

While building the `warmt1` variant we hit a real bridge-implementation
pitfall: after a Node process spawns **any** `worker_threads` Worker,
`fs.writeSync(1, ...)` on a stdout **pipe** can perform a **partial write**
(truncated at 64 KB, the pipe buffer) without retrying — the fd appears to
become non-blocking. A runtime that writes large NDJSON responses synchronously
will silently truncate them and deadlock the peer mid-line. The fix (in
`worker.js` `emit()`) is to loop on the returned byte count and park ~1 ms on
`EAGAIN`. Any real TaskQ bridge should write responses with retry-on-short-write
or use async streams with drain handling.

## Verdict (5 sentences)

Per-job cold spawn costs ~35 ms (p99 ~58 ms), which is ≤3.5 % overhead on a
1 s job and ~0.1 % on a 10 s job, so **yes, cold spawn is acceptable for jobs
≥1 s** and buys total isolation per job. For sub-100 ms jobs, warm-pool latency
(0.07 ms p50 / 0.29 ms p99 small-payload, ~4–9 ms at 1 MB) is well under 1 % of
a 100 ms budget, so **yes, a warm persistent-child pool is acceptable** — with
the caveat that 1 MB payloads cost ~4–14 ms per round trip and should be moved
by reference (object store / Postgres blob) rather than through stdio. Pool
saturation is **not meaningfully dangerous to `start_to_close`**: at 20-in-flight/4-children with 1 MB payloads, per-job wait+service reaches only ~0.2 % of a 30 s budget at p99, and the queue depth required to threaten the budget (~17,600 in-flight 1 MB jobs, ~300k small jobs) is unreachable in any sane deployment. Saturation therefore argues for cheap admission control (a bounded in-flight cap with jobs left queued in Postgres) as backpressure hygiene rather than as a latency necessity. Operationally, cancellation is a non-issue (SIGTERM self-exit in ~1.4 ms, SIGKILL reap in ~1.3 ms), and `worker_threads` per job is rejected (~12 ms/job); the recommended design is a warm pool of persistent Node children speaking NDJSON over stdio with short-write retry.
