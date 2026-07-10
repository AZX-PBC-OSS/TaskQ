# TaskQ

# :simple-python: Async-native, Postgres-backed background job library for Python 3.12+

**TaskQ** lets application code enqueue work as strongly-typed jobs that are persisted in
Postgres and executed by one or more worker processes. Because Postgres is the durable store,
you get exactly the transactional guarantees you already rely on — no separate broker, no
message loss across restarts, no split-brain between your application database and your job
state. A single `TASKQ_PG_DSN` is sufficient to run the full stack.

---

## Features

<div class="grid cards" markdown>

-   :material-atom:{ .lg .middle } **Actors**

    ---

    Define typed job handlers with `@actor`. Payload and result types are inferred from
    annotations and validated at decoration time. Both `async def` and sync functions are
    supported.

-   :material-database:{ .lg .middle } **Postgres-Native**

    ---

    The entire job lifecycle — enqueue, dispatch, heartbeat, retry, cancellation — is
    expressed in SQL. Advisory locks and `SKIP LOCKED` replace broker semantics. No
    separate infrastructure required.

-   :material-lightning-bolt:{ .lg .middle } **Async-First**

    ---

    Built on `asyncio` and `asyncpg`. Actors are `async def` functions. The worker and
    client are both fully async. `LISTEN/NOTIFY` provides near-zero-latency dispatch
    wakeups.

-   :material-shield-key:{ .lg .middle } **Type-Safe End-to-End**

    ---

    `@actor` infers `P` and `R` from the handler's annotations. `ActorRef[P, R]` flows
    into `JobsClient.enqueue`, which returns `JobHandle[R]`. `handle.wait()` returns
    `R`. The entire chain is checked by pyright in strict mode.

-   :material-speedometer:{ .lg .middle } **Rate Limiting**

    ---

    Token bucket, sliding window, and concurrency reservation primitives backed by Redis
    or Postgres. Compose multiple limits per actor. Automatic Redis-to-Postgres fallback.

-   :material-sitemap:{ .lg .middle } **Dependency Injection**

    ---

    FastAPI-style signature convention: declare what you need as typed keyword parameters.
    The worker's DI engine resolves them at dispatch time. Three scope lifetimes
    (PROCESS, LOOP, TRANSIENT) with cycle detection at startup.

-   :material-monitor-dashboard:{ .lg .middle } **Admin UI**

    ---

    Read-only by default observability dashboard built with FastAPI and Jinja2. Live queue, job,
    worker, schedule, and rate-limit views. Real-time SSE updates when Redis is
    configured; polling fallback otherwise. Does not expose write operations
    unless `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET=true`.

    !!! warning "Unauthenticated by default"
        The admin UI has no built-in authentication. Anyone with network access
        can view job data. In production you **must** protect it with
        authentication middleware or a reverse proxy. See
        [Admin UI — Security](guides/admin-ui.md#security).

-   :material-chart-line:{ .lg .middle } **Observability**

    ---

    OpenTelemetry-native, vendor-neutral. Spans, metrics, and structured logs are emitted
    via OTLP. Point `OTEL_EXPORTER_OTLP_ENDPOINT` at any OTel-compatible collector.

-   :material-clock-outline:{ .lg .middle } **Cron Scheduling**

    ---

    Declare periodic schedules with `cron(...)`. Standard 5-field cron expressions (plus an
    optional 6th seconds field), timezone support with DST gap/overlap handling, payload
    factories, and auto-discovery at worker startup.

-   :material-package-variant:{ .lg .middle } **Batch Enqueue**

    ---

    `enqueue_batch()` for transactional fan-out with idempotency keys. `enqueue_batch_fast()`
    uses PG `COPY FROM` for up to 50K rows at maximum throughput.

-   :material-cancel:{ .lg .middle } **Cancellation**

    ---

    Three-phase protocol: cooperative (`cancel_event`) then forced (`task.cancel()`) then
    abandoned. Pending and scheduled jobs are cancelled immediately without worker
    involvement.

-   :material-progress-check:{ .lg .middle } **Progress Tracking**

    ---

    Actors emit structured progress updates via `ctx.progress()`. Real-time Redis pub/sub
    delivery to Python async iterators or HTTP SSE endpoints. Postgres retains the latest
    snapshot.

-   :material-group:{ .lg .middle } **Workgroups**

    ---

    Lightweight process orchestrator for multi-queue deployments. Per-worker configuration
    from a TOML file with crash restart, health checking, and graceful shutdown
    propagation.

</div>

---

## Installation

```bash
pip install taskq-py
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add taskq-py
```

!!! tip "Python 3.12+"
    TaskQ requires Python 3.12 or newer. Core dependencies include `asyncpg`, `pydantic`,
    `opentelemetry-api`, and `structlog`.

**Optional extras:**

| Extra | Adds | When to use |
|-------|------|-------------|
| `taskq-py[redis]` | `redis>=7.4` | Real-time progress fanout via Redis pub/sub, Redis-backed rate limiters |
| `taskq-py[otel]` | `opentelemetry-sdk`, `opentelemetry-exporter-otlp` | Configuring OTel providers programmatically; in-process test utilities |
| `taskq-py[fastapi]` | `fastapi`, `jinja2`, `sse-starlette`, `uvicorn` | Admin UI (`taskq ui serve`), SSE progress bridge, Prometheus metrics router |
| `taskq-py[prometheus]` | `opentelemetry-exporter-prometheus` | Prometheus metric scrapes |
| `taskq-py[reload]` | `watchfiles` | Autoreload of workers and the admin UI during local development |

```bash
pip install "taskq-py[redis,otel,fastapi,prometheus]"     # full
```

---

## Quick Start

```python
from pydantic import BaseModel
from taskq import actor, TaskQ

class SendEmailPayload(BaseModel):
    to: str
    subject: str
    body: str

class SendEmailResult(BaseModel):
    message_id: str

# Define an actor — payload and result types are inferred from annotations.
@actor
async def send_email(payload: SendEmailPayload) -> SendEmailResult:
    print(f"Sending '{payload.subject}' to {payload.to}")
    return SendEmailResult(message_id="msg-123")

# Enqueue a job and wait for the result.
async def main() -> None:
    from taskq.settings import TaskQSettings
    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        handle = await tq.enqueue(
            send_email,
            SendEmailPayload(to="user@example.com", subject="Hello", body="World"),
        )
        result = await handle.wait(timeout=30.0)
        print(f"sent: {result.message_id}")

# Run the worker:
#   taskq migrate up
#   taskq worker --actors myapp.actors:registry
```

---

## Next Steps

- [:material-download: Installation](getting-started/installation.md) — Set up TaskQ in your project
- [:material-rocket-launch: Quick Start](getting-started/quick-start.md) — Go from zero to running worker in minutes
- [:material-atom: Actors](guides/actors.md) — `@actor` decorator, retry policies, concurrency caps, DI
- [:material-swap-horizontal: Jobs & Clients](guides/jobs-clients.md) — `JobsClient.enqueue`, `JobHandle.wait`, batch, cancellation
- [:material-engine: Workers](guides/workers.md) — Worker configuration, pools, heartbeat, graceful shutdown
- [:material-speedometer: Rate Limiting](guides/rate-limiting.md) — Token bucket, sliding window, concurrency reservations
- [:material-clock-outline: Cron Scheduling](guides/cron.md) — Periodic schedules with `cron()`
- [:material-sitemap: Dependency Injection](guides/dependency-injection.md) — Provider registry, scopes, lifecycle
- [:material-api: API Reference](api-reference/taskq.md) — Full autogenerated API docs
- [:material-account-tree: Architecture](architecture.md) — Dispatch CTE, advisory locks, leader election internals
