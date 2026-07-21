# Example Application

A self-contained demo that exercises every TaskQ feature.

## Quick Start

```bash
cd examples
docker compose up
```

Open your browser:

- **Trigger UI** — <http://localhost:8000> — one card per actor with an enqueue form.
- **Admin UI** — <http://localhost:8000/taskq/queues> — live pending / running / succeeded counts.

Submitting any form enqueues a job and redirects to the admin job-detail page where you can watch it execute.

## CLI Routes

| Method | Path | Description |
|---|---|---|
| `POST` | `/batch-fast` | Enqueue N counter jobs via `enqueue_batch_fast` (COPY FROM). Accepts JSON `{"n": 5}`. Returns `{"count": N, "actor": "counter"}`. |
| `GET` | `/rate-limits` | Peek all registered rate-limit bucket states. Returns JSON `{"bucket_name": {...state...}}`. |
| `POST` | `/cancel/{job_id}` | Cancel a running or pending job by ID. Returns `{"job_id", "previous_status", "new_status", "cancellation_initiated"}`. |

## Additional Example Files

| File | What it demonstrates |
|---|---|
| `client_script.py` | Standalone CLI script for enqueuing jobs, backfills, cancellation, and job listing outside a web app. Run with `uv run python examples/client_script.py [--backfill N \| --cancel ID \| --list \| --realworld]`. |
| `test_example.py` | Unit tests using `InMemoryBackend` + `FakeClock` — no Postgres or Redis required. Run with `uv run pytest examples/test_example.py -v`. |
| `workgroup.toml` | Workgroup supervisor config for multi-queue worker management. Run with `uv run taskq workgroup examples/workgroup.toml`. |
| `otel_setup.py` | OpenTelemetry SDK initialization for tracing with Jaeger or any OTLP collector. Run with `uv run python examples/otel_setup.py` (requires `[otel]` extra). |
| `fastapi_app/aad.py` | Azure managed-identity (Entra ID) deployment scaffold — AAD-authenticated worker and web app wired through `taskq[aad]` credential-provider factories, including the serve-mode lifespan ownership pattern. Run with `uv run python -m examples.fastapi_app.aad worker` or `... serve` (requires Azure resources; see the [Managed Identities guide](https://AZX-PBC-OSS.github.io/TaskQ/guides/managed-identities/)). |

## Actor Table

| Name | Feature demonstrated | Payload fields | What to observe in the admin UI |
|---|---|---|---|
| `counter` | Normal success, cooperative cancellation | `n` (int, default 10) | Job transitions `pending` → `running` → `succeeded`. Cancel mid-run to see it transition to `cancelled`. |
| `flaky` | Retry on failure | `fail_count` (int, default 2) | Two failed attempts visible in attempt history, then `succeeded`. |
| `snoozer` | Snooze / deferred re-execution | `delay_seconds` (int, default 10), `wake_after_attempt` (int, default 1) | Job enters `scheduled` state during the snooze window, then wakes up and succeeds. |
| `deferred` | Future scheduling via `scheduled_at` | `delay_seconds` (int, default 30) | Job stays `scheduled` until the delay elapses, then transitions to `running` → `succeeded`. |
| `window_rate_limited` | Redis-backed sliding window rate limit | *(none)* | Enqueue 5+ jobs: at most 3 dispatched in the first 15 s; remainder wait in `pending`. |
| `token_rate_limited` | Redis-backed token bucket rate limit | *(none)* | Enqueue 5+ jobs: at most 3 dispatched immediately; further jobs dispatched as tokens refill at 1/s. |
| `inmemory_rate_limited` | In-memory (per-worker) rate limit | *(none)* | At most 2 executions per 10 s **per worker**. With two workers the effective fleet-wide limit is doubled (4 per 10 s). This is expected — see Deployment Shapes below. |
| `reserved` | PG-backed concurrency reservation | *(none)* | Enqueue 5+ jobs: at most 2 `running` simultaneously; others wait in `pending` until a slot is released. |
| `batch_counter` | `enqueue_batch` (N child jobs from one orchestrator) | `n` (int, default 5), `steps` (int, default 5) | Enqueues N counter jobs as a single batch (`enqueue_batch`), then enqueues a `batch_finalizer` that waits for all children to complete. Watch all spawned jobs in the admin queue overview. |
| `batch_finalizer` | Fan-out-then-finalize via `wait_for_batch` (snooze-loop) | *(auto-enqueued by `batch_counter`)* | Calls `wait_for_batch` which raises `Snooze` while children are in-flight, then logs a summary when all are terminal. When children are slow, the attempt history shows repeated attempts each separated by the `snooze_interval` — this is the snooze-loop pattern in action. Not triggered from the UI; enqueued automatically by `batch_counter`. |
| `ticker` | Cron-scheduled periodic job | *(none)* | Fires automatically every 30 seconds via the cron loop. No manual enqueue needed. Observe `running` → `succeeded` transitions in the admin UI every 30 s. |
| `tagged_lower` | Job tagging — enqueued with `tags=["alpha", "lower"]` | `label` (str, default "demo") | Returns a `TaggedResult(label, reversed)`. Filter jobs by tag in the admin UI or via `JobFilter(tags=("alpha",))`. |
| `tagged_upper` | Job tagging — enqueued with `tags=["alpha", "upper"]` | `label` (str, default "demo") | Reports per-character progress. Shares the `"alpha"` tag with `tagged_lower` — filtering by `"alpha"` finds both actors' jobs. |
| `count_words` | Sync actor (plain `def`, dispatched via `asyncio.to_thread`) | `text` (str) | Counts words and characters synchronously. Returns `WordCountResult(word_count, char_count)`. Polls `ctx.should_abort()` for cooperative cancellation. |
| `send_digest_email` | Real-world: email digest with retry, dedup, DI, typed result | `user_id` (str), `email` (str), `period` (str, default "weekly") | Sends a digest email via injected `SmtpClient`. Deduplicated per `user_id` within 30 min. Retries up to 3 times on failure. Returns `DigestEmailResult(message_id, recipients, articles_included)`. |
| `process_csv_upload` | Real-world: ETL pipeline with progress and fan-out | `filename` (str), `row_count` (int, default 1000), `chunk_size` (int, default 500) | Parses, validates, chunks, and dispatches sub-jobs via `ctx.jobs.enqueue_batch()`. Watch the spawned `process_csv_chunk` jobs in the admin queue overview. |
| `generate_thumbnail` | Real-world: CPU-bound sync actor (image processing) | `image_url` (str), `width` (int, default 200), `height` (int, default 200), `format` (str, default "webp") | Runs synchronously via `asyncio.to_thread`. Polls `ctx.should_abort()` for cancellation. Returns `ThumbnailResult(output_path, width, height, format, source_bytes)`. |

## Job Tags

Tagged actors (`tagged_lower`, `tagged_upper`) pass `tags=["alpha", "lower"]` / `tags=["alpha", "upper"]` at enqueue time. Tags are stored as a Postgres `text[]` column and indexed with a GIN index. Filter by tag with:

```bash
# List all jobs tagged "alpha"
curl -s localhost:8000/taskq/jobs/api/jobs?tags=alpha | python -m json.tool
```

## Sync Actors

The `count_words` actor is a plain `def` (not `async def`). The worker dispatches sync actors via `asyncio.to_thread`, running CPU-bound work on a thread while the event loop stays responsive. Sync actors must poll `ctx.should_abort()` for cooperative cancellation — they cannot `await ctx.check_cancelled()`.

## Embedded Admin UI

The admin UI is served from the same process at `/taskq` using `create_router` and
`setup_admin_state` from `taskq.web.admin`. After `TaskQ` opens its pool in the
lifespan, `create_router` wraps it in an `AdminBundle` and `setup_admin_state`
populates `app.state` so the admin route dependencies resolve. The router is then
mounted with `app.include_router(bundle.router, prefix="/taskq")`.

## Deployment Shapes

Both the trigger routes and the admin UI run in-process, which is the simplest
deployment shape. For larger deployments where you want to isolate the admin UI —
for example to apply a separate auth layer or scale it independently — you can run
a dedicated FastAPI app that calls `create_router` with the same Postgres DSN and
mounts nothing else.

## Snooze-Loop Pattern

When `batch_finalizer` runs while child jobs are still in-flight, `wait_for_batch` raises `Snooze(snooze_interval)`. The worker catches this and transitions the finalizer from `running` to `scheduled`, rescheduling it after the snooze interval without consuming retry budget. When children are slow, the `batch_finalizer` job's attempt history in the admin UI shows multiple attempts, each separated by the `snooze_interval` — this is the fan-out-then-finalize snooze-loop pattern in action. Once all children reach a terminal state, the finalizer succeeds on its next attempt and logs the completion summary.
