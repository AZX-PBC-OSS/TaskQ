# The `examples.actors` Package

The toy actor fleet behind the example compose stack. Every module registers
its actors on the `"examples"` queue at import time; `worker.py` and `app.py`
import the package so the worker dispatches them and the trigger UI renders a
form card for each.

## Run the fleet

```bash
cd examples
docker compose up
```

Then open the trigger UI at <http://localhost:8000> (one card per actor) and
the embedded admin UI at <http://localhost:8000/taskq/queues> (live
pending / running / succeeded counts). Submitting a card's form enqueues that
actor's job; the admin UI is where you watch it transition.

## Modules by feature domain

| Module | Demonstrates | Actors | What to observe |
|---|---|---|---|
| `basic.py` | Simplest patterns: a cancellable long-running job and future scheduling | `counter`, `deferred` | `counter` walks `pending → running → succeeded` (cancel mid-run → `cancelled`); `deferred` stays `scheduled` until `scheduled_at` passes. |
| `failure.py` | Retry policy and cooperative snooze | `flaky`, `snoozer` | `flaky` fails twice on purpose — the attempt history shows 2 failed attempts, then success; `snoozer` enters `scheduled` per snooze cycle, then succeeds. |
| `ratelimit.py` | Redis-backed sliding window, Redis-backed token bucket, in-memory per-worker window, PG-backed concurrency reservation | `window_rate_limited`, `token_rate_limited`, `inmemory_rate_limited`, `reserved` | Enqueue 5+: only a few dispatch immediately; the rest re-pend (or stay `pending`) until a token / window slot / reservation frees up. `GET /rate-limits` peeks the bucket states. |
| `chained.py` | Sub-job enqueueing from inside an actor via `ctx.jobs.enqueue()` / `enqueue_batch()` (transactional, on the worker's pool) | `step_one` → `step_two`, `fan_out` | Enqueue `step_one`: it succeeds and a `step_two` job appears without you enqueueing it. `fan_out` spawns a batch of children. |
| `di.py` | Dependency injection: LOOP-scope (per event loop) and TRANSIENT-scope (per invocation) providers | `fetch` (`fetch_actor`), `db_lookup` | Both inject fake providers declared in `build_registry()`; the job succeeds with the injected dependencies. These actors require the DI registry, so `worker.py`'s `__main__` path serves them (workgroup subprocesses cannot). |
| `batch.py` | `enqueue_batch` fan-out-then-finalize with the `wait_for_batch` snooze loop | `batch_counter` → `batch_finalizer` | Enqueue `batch_counter`: N children plus a finalizer appear at once; the finalizer's attempt history shows snooze-separated attempts until all children are terminal. |
| `advanced.py` | Fleet-wide singleton, soft concurrency cap, per-identity dedup, typed result with TTL | `singleton_job`, `capped_job`, `deduplicated`, `summer` | A second `singleton_job` while one runs → HTTP 409 (`SingletonCollisionError`); `capped_job` shows at most 2 `running`; `deduplicated` with the same key returns the existing job; `summer` opens a result page that polls `handle.wait()`. |
| `ticker.py` | Cron-scheduled periodic actor | `ticker` | Fires by itself every 30 s — no enqueue needed. Visible under the schedules view of the admin UI. |
| `progress.py` | `ctx.progress()` reporting and `JobHandle.progress_stream()` | `file_processor` | Enqueue and open the job page: progress advances through parse / validate / transform / write steps. |
| `tags_demo.py` | Job tagging and tag-based filtering | `tagged_lower`, `tagged_upper` | Both share the `"alpha"` tag — filter with `?tags=alpha` in the admin UI (see the Job Tags section of the top-level README). |
| `sync_demo.py` | Plain `def` actor dispatched via `asyncio.to_thread` | `count_words` | Runs CPU-bound word counting on a thread; returns a typed `WordCountResult`. |
| `realworld.py` | Real-world scenarios: email digest with DI + dedup, CSV ETL fan-out, CPU-bound thumbnail processing | `send_digest_email`, `process_csv_upload` → `process_csv_chunk`, `generate_thumbnail` | `send_digest_email` injects an `SmtpClient` and dedups per user; `process_csv_upload` spawns chunk children visible in the queue overview; `generate_thumbnail` runs via `asyncio.to_thread` and returns the output path. |

## Notes

- Actor sources double as the API documentation: every module carries a
  docstring naming the feature and the observables, so start reading at the
  module that matches the feature you are wiring.
- Rate-limit primitives in `ratelimit.py` are registered on the module-level
  `registry` singleton at import time — the worker must import the module
  before dispatch begins (importing the package does that).
- The full per-actor payload fields and admin-UI observations are tabled in
  the [top-level README](../README.md#actor-table).
- For a narrated production tour of the same engine — rate-limit denials with
  `retry_after_seconds`, operator cancels, cron tick budgets, SIGTERM deploys,
  `taskq doctor`, `taskq insights` — run the flagship demo:
  `uv run python -m examples.fleet_demo.run_demo` (see
  [the fleet demo README](../fleet_demo/README.md)).
