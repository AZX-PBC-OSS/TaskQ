# Example Application

A self-contained demo that exercises every TaskQ feature.

Prefer a narrated tour of the engine's production behavior — rate-limit
denials with Retry-After, operator cancels, cron tick budgets, SIGTERM
deploys, `taskq doctor`, `taskq insights`? Run the flagship fleet demo:

```bash
uv run python -m examples.fleet_demo.run_demo
```

See [the fleet demo README](fleet_demo/README.md) for the seven-act story.

## Quick Start

```bash
cd examples
docker compose up
```

Faster repeated runs: pre-build the image once under its content hash and
compose skips the docker build on every subsequent `up` (see
`benchmarks/example_image_spec.py` and the Container section of the
deployment guide):

```bash
export TASKQ_EXAMPLE_IMAGE="$(uv run python benchmarks/example_image_spec.py --print-tag)"
uv run python benchmarks/example_image_spec.py          # build if the hash is new
cd examples && docker compose up                        # no docker build paid
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
| `actors/` | The toy actor fleet, organized by feature domain (one module per domain: basic, failure, ratelimit, chained, DI, batch, advanced, cron, progress, tags, sync, real-world). See the [actors package README](actors/README.md). |
| `admin_app.py` | The admin UI as a **separate process** (the decoupled deployment shape from Deployment Shapes below). Run with `TASKQ_PG_DSN=... TASKQ_ENVIRONMENT=dev uv run uvicorn examples.admin_app:app --host 0.0.0.0 --port 8001` (`TASKQ_PG_DSN` at the stack's Postgres; without the dev label the admin UI fails closed on the missing auth dependency) — the compose stack's `admin` service does exactly this; the sidecar then serves `/admin` on port 8001. The demo's workflow definitions mount in the sidecar (`workflow_app=...`) so the run page's Resolve form delivers the typed payload here too — without a mounted WorkflowApp the resolve endpoints answer `501` (the typed door refuses to deliver untyped). |
| `client_script.py` | Standalone CLI script for enqueuing jobs, backfills, cancellation, and job listing outside a web app. Run with `uv run python -m examples.client_script [--backfill N \| --cancel ID \| --list \| --realworld]` (module form: the script imports `examples.actors`, which needs the repo root on the import path). |
| `test_example.py` | Unit tests using `InMemoryBackend` + `FakeClock` — no Postgres or Redis required. Run with `uv run pytest examples/test_example.py -v`. |
| `workgroup.toml` | Workgroup supervisor config for multi-queue worker management. Run with `uv run taskq workgroup start examples/workgroup.toml` (or `taskq workgroup validate examples/workgroup.toml` to check the config without starting). Serves the DI-free actor subset — workgroup children are plain `taskq worker` subprocesses and cannot register DI providers. |
| `otel_setup.py` | OpenTelemetry SDK initialization for tracing with Jaeger or any OTLP collector. Run with `uv run python -m examples.otel_setup` (requires `[otel]` extra). |
| `fastapi_app/aad.py` | Azure managed-identity (Entra ID) deployment scaffold — AAD-authenticated worker and web app wired through `taskq[aad]` credential-provider factories, including the serve-mode lifespan ownership pattern. Run with `uv run python -m examples.fastapi_app.aad worker` or `... serve` (requires Azure resources; see the [Managed Identities guide](https://AZX-PBC-OSS.github.io/TaskQ/guides/managed-identities/)). |
| `fastapi_app/` | Minimal FastAPI app (separate from this trigger UI) demonstrating enqueue + SSE job-event streaming + cancellation against the public client surface, with its own [README](fastapi_app/README.md). The compose stack serves it on port 8002 (profile `example`: `docker compose --profile example up`). |

## Actor Table

| Name | Feature demonstrated | Payload fields | What to observe in the admin UI |
|---|---|---|---|
| `counter` | Normal success, cooperative cancellation | `n` (int, default 10) | Job transitions `pending` → `running` → `succeeded`. Cancel mid-run to see it transition to `cancelled`. |
| `flaky` | Retry on failure | `fail_count` (int, default 2) | Two failed attempts visible in attempt history, then `succeeded`. |
| `snoozer` | Snooze / deferred re-execution | `delay_seconds` (int, default 10), `snooze_cycles` (int, default 1) | Job enters `scheduled` once per configured snooze cycle, then wakes up and succeeds. |
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
# List the admin jobs view filtered to tag "alpha"
curl -s 'localhost:8000/taskq/jobs?tags=alpha'
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

---

## Workflow Demo: the doc-ingest pipeline

`examples/workflows.py` wires the **doc-ingest pipeline** (the same abstract
graph the docs example teaches — see `docs/examples/doc-ingest.md`) LIVE
behind HTTP, with the admin's run explorer attached.

The HITL broadcast's worked example lives beside it:
`examples/deep_research.py` — the **deep-research loop** (T26): three free
research passes, then the typed `ContinueApproval` gate broadcast over PG
LISTEN/NOTIFY (watch it live on the admin's `/sse/holds` topic); the typed
expiry is the FAIL-CLOSE — nobody watching and the run still succeeds,
carrying the `finished_with_what_you_have` result.

### Run it

```bash
cd examples
docker compose up -d --build        # or just `docker compose up -d` — the
                                    # default pull policy is `build`, so a
                                    # cold start always rebuilds THIS tree
                                    # (a stale image from another checkout
                                    # can never serve silently)
```

Every taskq-example service also runs pre-built, content-hashed: set
`TASKQ_EXAMPLE_IMAGE` to the hash tag and `TASKQ_EXAMPLE_PULL_POLICY=missing`
(see `benchmarks/example_image_spec.py` — the fast path above).

Without compose, run the app directly (the env vars it needs: `TASKQ_PG_DSN`
at the stack's Postgres, `TASKQ_SCHEMA_NAME` (fresh is fine — the app
migrates on start), `TASKQ_MIGRATE_ON_START=true`, and
`TASKQ_ADMIN_ACTIONS_ENABLED=true` — **must be `true` for the demo's
Resolve**, the destructive-action opt-in; `TASKQ_ENVIRONMENT=dev` for the
local demo). The compose `app` service sets exactly these.

| Surface | URL | The prefix |
|---|---|---|
| the trigger UI + the run page (the Resolve form lives here) | <http://localhost:8000> | `/taskq/...` — the trigger's 202 envelope's url resolves HERE |
| the admin sidecar (the decoupled shape) | <http://localhost:8001/admin> | `/admin/...` — its own run page resolves too (the demo's workflow definitions mount in the sidecar for the typed door) |

The two prefixes are each surface's OWN base path — the run page's
Resolve form posts a RELATIVE url, so it works on either surface; the
trigger's envelope names the app's `/taskq/...` path.

### Trigger + watch

```bash
curl -X POST http://localhost:8000/workflows/doc_ingest/run
# → 202 {"run_id": "...", "url": "/taskq/workflows/..."}   (the F3 envelope)
```

Open the returned URL ON THE APP (:8000) — the run page renders the
run's graph and patches it LIVE over SSE.

### The three demonstrable properties

1. **A map with a failing child + collect** — `doc-doomed`'s first
   enrichment fails through its ladder; the run NEVER re-runs the
   succeeded siblings; the failure surfaces in the typed report (the
   demo's report carries `failed: []` because the ladder HEALS the armed
   child — watch the item's attempt go 1 → 2 in the node panel: the
   map's children are addressed by (step, `map_index`), so the doomed
   child is `GET /taskq/api/runs/{run_id}/nodes/ingest.item?map_index=6`
   (`ingest.item` #6 IS `doc-doomed`, the demo corpus's last id; its
   attempt ledger shows both attempts, and the bare read carries the
   whole children census to click through).
2. **The budget-capped loop with the held approval** — the run HOLDS at
   the review (the held node renders AMBER with its countdown); the
   Resolve form (on the run page) delivers the typed `ReviewDecision`;
   the loop resumes toward publish. A `reject` refines the loop (the
   budget was PAUSED while the hold waited — the hold counts for
   nothing on wake); three rejects exhaust the cap into the NAMED
   escalation.
3. **The admin graph view live** — the collapsed map hexagon (the
   done/total counter), the taken paths, the failure badge, the SSE
   patches: all demonstrated on a real run.

### The four demonstrations (the capabilities, live)

| Leg | What shows | How to run |
|-----|-----------|------------|
| **Cancellation** | a run cancelled mid-flight: the named states + the audit row in the explorer | `taskq flows cancel <run_id> --reason ...` while the run is mid-flight; watch the run page |
| **Resumability** | a worker SIGKILLed mid-node; a fresh worker re-claims; the run COMPLETES | start `taskq worker --actors examples.workflows:ACTORS --queues demo-screen,demo-cpu,demo-io,demo-classify,demo-publish,demo-enrich,default`, trigger, `kill -9` the worker mid-run, start a fresh one |
| **Observability** | the wf gauge's LIVE scrape off the real `/metrics` endpoint (`taskq.wf_progress_nodes_total{workflow,state}` — rendered by the Prometheus bridge as `taskq_wf_progress_nodes_total`; the maintenance leader's sampler feeds it) | the capture: `.measurements/demo-legs/leg3-wf-gauge-scrape.prom` (the endpoint's byte-verbatim response, never a hand-rendered transcript) |
| **The conditional router** | the T20 chain's conditional edges LIVE: READABLE → index, UNREADABLE → dead-letter; the totals are the fence | trigger `POST /workflows/doc_screen_router/run` (the router's own workflow — the route serves it; 202 + the run url) |
