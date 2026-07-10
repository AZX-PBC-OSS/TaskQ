# Getting Started

This guide walks from a fresh install to a running worker dispatching its first job.

---

## Prerequisites

- Python 3.12 or later
- `uv` or `pip` for package management
- Postgres (the bundled Docker Compose uses `postgres:18.4`)
- Redis (optional — required only for real-time progress fanout and admin UI live updates)

---

## Installation

```bash
pip install taskq-py                      # core only
pip install "taskq-py[redis]"             # + real-time progress fanout, Redis rate limiters
pip install "taskq-py[fastapi]"           # + admin UI
pip install "taskq-py[redis,otel,fastapi]"  # full (add prometheus for scrapes)
```

**Extras**

| Extra | Installs | When you need it |
|-------|----------|-----------------|
| `taskq-py[redis]` | `redis>=7.4` | Real-time progress fanout, Redis-backed rate limiters |
| `taskq-py[otel]` | `opentelemetry-sdk`, `opentelemetry-exporter-otlp` | OTel provider setup, in-process test utilities |
| `taskq-py[fastapi]` | `fastapi`, `jinja2`, `sse-starlette`, `uvicorn` | Admin UI, SSE progress bridge |
| `taskq-py[prometheus]` | `opentelemetry-exporter-prometheus` | Prometheus metric scrapes |

---

## Docker Compose quickstart

The bundled `docker-compose.yml` starts Postgres, Redis, and the admin UI in one command. This is the fastest path to a running local environment.

```bash
docker compose up -d
```

Services started:

| Service | Port | Notes |
|---------|------|-------|
| `postgres` | 5432 | Postgres with `max_connections=200`, `shared_buffers=256MB` |
| `redis` | 6379 | Redis without persistence (`appendonly no`) |
| `admin` | 8080 | TaskQ admin UI — runs `taskq ui serve --migrate` on startup |

The `admin` service runs `taskq ui serve --migrate` which applies pending migrations before starting the UI. When using the full compose stack you do not need to run migrations manually.

To start Postgres and Redis only (for running the worker locally outside Docker):

```bash
docker compose up -d postgres redis
```

---

## Environment setup

Copy the example env file and adjust as needed:

```bash
cp .env.example .env
```

No env var is strictly required — `TASKQ_PG_DSN` defaults to `postgresql://taskq:taskq@localhost:5432/taskq`. For any real deployment, set it to your actual database.

```dotenv
# Direct PG DSN — sessions, LISTEN/NOTIFY, and advisory locks require this.
TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq

# Schema name for all TaskQ tables. Override if multi-tenanting.
TASKQ_SCHEMA_NAME=taskq

# Optional. Enables real-time progress fanout and admin UI live updates.
TASKQ_REDIS_URL=redis://localhost:6379/0
```

> **PgBouncer warning:** Advisory locks and `LISTEN/NOTIFY` require a direct Postgres connection. Do not point `TASKQ_PG_DSN` at a PgBouncer endpoint in transaction-pooling mode.

TaskQ loads configuration through `dotenvmodel` with cascading `.env` discovery:
`.env` → `.env.local` → `.env.{env}` → `.env.{env}.local`.

The worker validates cross-field constraints at startup (e.g. `TASKQ_LOCK_LEASE` must be `>= 4 × TASKQ_HEARTBEAT_INTERVAL`). See [Worker](../guides/workers.md) for the full settings reference.

---

## Run migrations

Apply all pending migrations before starting a worker:

```bash
taskq migrate up
```

The command is idempotent — re-running against an up-to-date schema is a no-op. To inspect applied and pending migrations without making changes:

```bash
taskq migrate status
```

Alternatively, set `TASKQ_MIGRATE_ON_START=true` to have the admin UI apply migrations automatically at startup. Production workers should still run `taskq migrate up` manually before the worker process starts.

---

## Define your first actor

An actor is a function decorated with `@actor`. Both `async def` and plain `def` are supported — sync functions run in a thread via `asyncio.to_thread()` to avoid blocking the event loop. The payload and result must be `pydantic.BaseModel` subclasses. `@actor` can be applied bare or with keyword arguments:

```python
# myapp/actors.py
from pydantic import BaseModel
from taskq import actor


class SendEmailPayload(BaseModel):
    to: str
    subject: str
    body: str


class SendEmailResult(BaseModel):
    message_id: str


# Bare form — omit retry for the default: 3 attempts, exponential backoff.
@actor
async def send_email(payload: SendEmailPayload) -> SendEmailResult:
    # Replace with your real email logic.
    print(f"Sending '{payload.subject}' to {payload.to}")
    return SendEmailResult(message_id="msg-123")


# Parameterised form — override queue, retry policy, etc.
# @actor(queue="priority")
# async def send_email(...) -> ...:
#     ...
```

The `@actor` decorator validates the signature at import time. It rejects unannotated parameters and payload types that are not `BaseModel` subclasses. Both `async def` and `def` are accepted.

**Sync actors** run via `asyncio.to_thread()` — the event loop is never blocked. Cancellation for sync actors is cooperative: poll `ctx.should_abort()` in long-running loops. LOOP-scoped DI dependencies (e.g. `asyncpg.Connection`) are not thread-safe and should not be used by sync actors; the worker logs a warning at startup validation when a sync actor declares one. See [Actor API — Sync actors](../guides/actors.md#sync-actors) for details.

**Tags** can be attached at enqueue time for filtering and categorization:

```python
handle = await client.enqueue(
    send_email,
    SendEmailPayload(to="user@example.com", subject="Hello", body="World"),
    tags=["notification", "priority:high"],
)
```

Tags appear in the admin UI as filterable badges. Tag validation: `^[\w][\w\-]+[\w]$`, min 3 chars, max 255 chars per tag. See [Jobs — Tags](../guides/jobs-clients.md#tags) for details.

See [Actor API](../guides/actors.md) for the full decorator reference: queue assignment, retry policies, concurrency caps, singletons, rate limits, and DI dependencies.

---

## Register actors and start a worker

The worker needs a reference to your actor registry. The `--actors` flag takes a `module:attribute` import path. The attribute must resolve to a `Mapping[str, ActorRef]` or a `list`/`tuple` of `ActorRef` objects. Generators are not accepted — they are exhausted during type-checking and cannot be iterated again for dispatch.

Define a registry in your actors module:

```python
# myapp/actors.py  (continued)
registry = [send_email]
# or equivalently:
# registry = {"send_email": send_email}
```

Start the worker:

```bash
taskq worker --actors myapp.actors:registry
```

The worker reads configuration from environment variables (or `.env`). You can override key settings at the command line:

```bash
taskq worker --actors myapp.actors:registry \
  --queues default --queues priority \
  --max-concurrency 16
```

To start a worker from Python (e.g., in a process supervisor or test harness):

```python
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main
from myapp.actors import send_email

settings = WorkerSettings.load()
# actor_registry keys must match each ActorRef's registered name
# (defaults to the function's __qualname__).
# Passing actor_registry=None runs stub consumers only — not for production use.
exit_code = worker_main(settings, actor_registry={"send_email": send_email})
```

See [Worker](../guides/workers.md) for pool sizing, heartbeat configuration, and graceful shutdown.

---

## Enqueue a job

`JobsClient` is the public API for enqueuing jobs. It wraps a `Backend` instance. For demos and tests, use `InMemoryBackend` — it is in-process only and not persistent. For production enqueue from application code outside the worker, see [Client API](../guides/jobs-clients.md) for the production pattern.

**For tests and local demos:**

```python
import asyncio
from datetime import UTC, datetime
from taskq import JobsClient
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.clock import FakeClock
from myapp.actors import send_email, SendEmailPayload


async def demo() -> None:
    clock = FakeClock(start=datetime.now(UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    handle = await client.enqueue(
        send_email,
        SendEmailPayload(to="user@example.com", subject="Hello", body="World"),
    )
    print(handle.job_id)       # UUID of the enqueued job
    print(handle.was_existing) # False for a fresh enqueue
```

`InMemoryBackend` is for tests and demos only — it holds state in-process and does not persist across restarts.

**In production application code** (e.g., a FastAPI route that enqueues a job):

The production path goes through the worker's `open_worker_deps` context manager, which constructs the three asyncpg pools. `PostgresBackend` is built internally by the worker and is not constructible from a bare pool in standalone code. For a FastAPI application that shares the worker's pool, inject the `JobsClient` via FastAPI's dependency system. See [Client API](../guides/jobs-clients.md) for the full production wiring pattern.

---

## Wait for a result

`JobHandle.wait()` polls until the job reaches a terminal status and returns the deserialized result:

```python
result = await handle.wait(timeout=30.0)
print(result.message_id)  # SendEmailResult.message_id
```

`wait()` raises:

- `JobFailed` — the job reached a non-success terminal state (`failed`, `cancelled`, `crashed`, or `abandoned`); the raw job row is attached as `exc.row`.
- `ResultUnavailable` — the job succeeded but no result was stored (e.g., result TTL expired, or the actor returned `None` while `R` is non-`None`).
- `TimeoutError` — `timeout` elapsed before any terminal transition was observed.

---

## Verify with health checks

Once a worker is running, probe it via the CLI health subcommands. `taskq health` requires a subcommand:

```bash
taskq health live     # returns 0 if the worker process is alive
taskq health ready    # returns 0 if the worker can reach the database
taskq health metrics  # returns current worker metrics
```

All health commands connect to the worker's Unix socket (default `/tmp/taskq_health.sock`) and return exit code 0 on success, 1 on failure. The commands must be run on the same host as the worker. The socket path is configured via `TASKQ_HEALTH_SOCKET_PATH`.

---

## Open the admin UI

If you used `docker compose up -d` or ran `taskq ui serve` manually, the admin UI is available at:

```
http://localhost:8080/admin
```

The UI provides a live view of queues, jobs, workers, and actor configurations. Live updates are delivered over Server-Sent Events when `TASKQ_REDIS_URL` is set.

To run the admin UI as a standalone process:

```bash
taskq ui serve --host 0.0.0.0 --port 8080
```

See [Admin UI](../guides/admin-ui.md) for the full reference.

---

## Next steps

| Topic | Doc |
|-------|-----|
| Actor options: retry policies, concurrency caps, singletons, DI | [Actor API](../guides/actors.md) |
| Enqueueing, `JobHandle.wait()`, cancellation, unique jobs | [Client API](../guides/jobs-clients.md) |
| Worker configuration, pools, heartbeat, graceful shutdown | [Worker](../guides/workers.md) |
| CLI command reference | [CLI](../guides/cli.md) |
| Admin UI | [Admin UI](../guides/admin-ui.md) |
| Testing with `InMemoryBackend` and pytest fixtures | [Development](../api-reference/testing.md) |
| Architecture internals | [Architecture](../architecture.md) |
