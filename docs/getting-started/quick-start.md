# Getting Started

This guide walks from a fresh install to a running worker dispatching its first job.

---

## Prerequisites

- Python 3.12 or later
- `uv` or `pip` for package management
- Postgres **15 minimum; 15–18 covered by CI** (the bundled Docker Compose pins
  the `postgres:18` image for dev) — see
  [Installation: prerequisites](installation.md#prerequisites) for the full
  support matrix
- Redis (optional; required only for real-time progress fanout and admin UI live updates)

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
| `taskq-py[redis]` | `redis>=8.0.1` | Real-time progress fanout, Redis-backed rate limiters |
| `taskq-py[otel]` | `opentelemetry-sdk`, `opentelemetry-exporter-otlp` | OTel provider setup, in-process test utilities |
| `taskq-py[fastapi]` | `fastapi`, `jinja2`, `sse-starlette`, `uvicorn` | Admin UI, SSE progress bridge |
| `taskq-py[prometheus]` | `opentelemetry-exporter-prometheus` | Prometheus metric scrapes |
| `taskq-py[oidc]` | `authlib>=1.8`, `httpx2`, `itsdangerous` | OIDC/SSO auth for the admin UI; see [SSO / SAML](../guides/sso.md) |
| `taskq-py[saml]` | `python3-saml`, `itsdangerous` | SAML/SSO auth for the admin UI; see [SSO / SAML](../guides/sso.md) |
| `taskq-py[aad]` | `azure-identity`, `aiohttp` | Azure Entra ID managed-identity DB auth; see [Managed Identities](../guides/managed-identities.md) |
| `taskq-py[aws]` | `boto3` | AWS IAM RDS auth for Postgres; see [Managed Identities](../guides/managed-identities.md) |
| `taskq-py[vault]` | `hvac` | HashiCorp Vault dynamic credentials; see [Managed Identities](../guides/managed-identities.md) |
| `taskq-py[reload]` | `watchfiles` | Autoreload of workers and the admin UI during local development |
| `taskq-py[bench]` | `py-spy`, `pyinstrument` | Profilers for the `benchmarks/` toolkit |

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
| `admin` | 8080 | TaskQ admin UI (runs `taskq ui serve --migrate` on startup) on startup |

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

No env var is strictly required: `TASKQ_PG_DSN` defaults to `postgresql://taskq:taskq@localhost:5432/taskq`. For any real deployment, set it to your actual database.

```dotenv
# Direct PG DSN: sessions, LISTEN/NOTIFY, and advisory locks require this.
TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq

# Schema name for all TaskQ tables. Override if multi-tenanting.
TASKQ_SCHEMA_NAME=taskq

# Optional. Enables real-time progress fanout and admin UI live updates.
TASKQ_REDIS_URL=redis://localhost:6379/0
```

> **PgBouncer warning:** Advisory locks and `LISTEN/NOTIFY` require a direct Postgres connection. Do not point `TASKQ_PG_DSN` at a PgBouncer endpoint in transaction-pooling mode.

TaskQ loads configuration through `dotenvmodel` with cascading `.env` discovery:
`.env` → `.env.local` → `.env.{env}` → `.env.{env}.local`, where `{env}` comes from the `ENV` variable (default `dev`). Real environment variables take precedence over `.env` files; see [Configuration](../guides/configuration.md) for the full resolution rules.

The worker validates cross-field constraints at startup (e.g. `TASKQ_LOCK_LEASE` must cover the worst coherent failed-beat cascade: `max(TASKQ_HEARTBEAT_INTERVAL, TASKQ_HEARTBEAT_COMMAND_TIMEOUT) + (TASKQ_MAX_HEARTBEAT_FAILURES + 1) × (TASKQ_HEARTBEAT_INTERVAL + TASKQ_HEARTBEAT_COMMAND_TIMEOUT)`, 58 s at the defaults). See [Worker](../guides/workers.md) for the full settings reference.

---

## Run migrations

Apply all pending migrations before starting a worker:

```bash
taskq migrate up
```

The command is idempotent: re-running against an up-to-date schema is a no-op. To inspect applied and pending migrations without making changes:

```bash
taskq migrate status
```

Alternatively, set `TASKQ_MIGRATE_ON_START=true` to have the admin UI apply migrations automatically at startup. Production workers should still run `taskq migrate up` manually before the worker process starts.

---

## Define your first actor

An actor is a function decorated with `@actor`. Both `async def` and plain `def` are supported; sync functions run in a thread via `asyncio.to_thread()` to avoid blocking the event loop. The payload and result must be `pydantic.BaseModel` subclasses. `@actor` can be applied bare or with keyword arguments:

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


# Bare form: omit retry for the default: 3 attempts, exponential backoff.
@actor
async def send_email(payload: SendEmailPayload) -> SendEmailResult:
    # Replace with your real email logic.
    print(f"Sending '{payload.subject}' to {payload.to}")
    return SendEmailResult(message_id="msg-123")


# Parameterised form: override queue, retry policy, etc.
# @actor(queue="priority")
# async def send_email(...) -> ...:
#     ...
```

The `@actor` decorator validates the signature at import time. It rejects unannotated parameters and payload types that are not `BaseModel` subclasses. Both `async def` and `def` are accepted.

**Sync actors** run via `asyncio.to_thread()`, so the event loop is never blocked. Cancellation for sync actors is cooperative: poll `ctx.should_abort()` in long-running loops. LOOP-scoped DI dependencies (e.g. `asyncpg.Connection`) are not thread-safe and should not be used by sync actors; the worker logs a warning at startup validation when a sync actor declares one. See [Actor API: Sync actors](../guides/actors.md#sync-actors) for details.

**Tags** can be attached at enqueue time for filtering and categorization:

```python no-exec — not executed: fragment, names bound by an earlier fence
handle = await client.enqueue(
    send_email,
    SendEmailPayload(to="user@example.com", subject="Hello", body="World"),
    tags=["notification", "priority-high"],
)
```

Tags appear in the admin UI as filterable badges. Tag validation: `\A\w(?:[\w\-]*\w)?\Z`, max 255 chars per tag (short tags like `ci` are fine). See [Jobs: Tags](../guides/jobs-clients.md#tags) for details.

See [Actor API](../guides/actors.md) for the full decorator reference: queue assignment, retry policies, concurrency caps, singletons, rate limits, and DI dependencies.

---

## Register actors and start a worker

The worker needs a reference to your actor registry. The `--actors` flag takes a `module:attribute` import path. The attribute must resolve to a `Mapping[str, ActorRef]` or a `list`/`tuple` of `ActorRef` objects. Generators are not accepted: they are exhausted during type-checking and cannot be iterated again for dispatch.

Define a registry in your actors module:

```python no-exec — not executed: fragment, names bound by an earlier fence
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

```python no-exec — not executed: continues the user-local module the guide is building
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main
from myapp.actors import send_email

settings = WorkerSettings.load()
# actor_registry keys must match each ActorRef's registered name
# (defaults to the function's __qualname__).
# Passing actor_registry=None runs stub consumers only: not for production use.
exit_code = worker_main(settings, actor_registry={"send_email": send_email})
```

See [Worker](../guides/workers.md) for pool sizing, heartbeat configuration, and graceful shutdown.

---

## Enqueue a job

The `TaskQ` facade is the production entry point for enqueuing from application
code (a FastAPI route, a script, anything outside a worker): it owns the
Postgres pool, resolves the schema the way the worker does, and hands you
enqueue-plus-wait in one object.

**Production (real Postgres):**

```python
import asyncio

import asyncpg
from pydantic import BaseModel

from taskq import TaskQ, actor
from taskq.migrate import apply_pending_locked
from taskq.settings import TaskQSettings


class SendEmailPayload(BaseModel):
    to: str
    subject: str
    body: str


class SendEmailResult(BaseModel):
    message_id: str


# In a real application this lives in myapp/actors.py (the module the
# worker's --actors flag resolves).
@actor
async def send_email(payload: SendEmailPayload) -> SendEmailResult:
    return SendEmailResult(message_id="msg-123")


async def main() -> None:
    settings = TaskQSettings.load()

    # First run on a fresh database: apply the schema migrations
    # (`taskq migrate up` does the same from the CLI).
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await apply_pending_locked(conn=conn, schema=str(settings.schema_name))
    finally:
        await conn.close()

    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        handle = await tq.enqueue(
            send_email,
            SendEmailPayload(to="user@example.com", subject="Hello", body="World"),
        )
        print(handle.job_id)  # UUID of the enqueued job
        print(handle.was_existing)  # False for a fresh enqueue

        # Nothing runs the job until a worker consumes it. Start the worker
        # from the previous section and re-run to see the job complete;
        # without a consumer, wait() times out with a bare TimeoutError.
        try:
            result = await handle.wait(timeout=10.0)
            print(result.message_id)  # SendEmailResult.message_id
        except TimeoutError:
            print("no worker consumed the job within 10s")


asyncio.run(main())
```

`handle.wait()` returns only once a consumer has run the job; see
[Wait for a result](#wait-for-a-result) below for what it raises. For the
full production wiring pattern — including a FastAPI application that shares
the worker's pools via dependency injection — see
[Client API](../guides/jobs-clients.md).

**For tests and local demos (no infrastructure):** `JobsClient` over
`InMemoryBackend` runs entirely in-process, no Postgres or Redis needed.
It is not persistent and holds state in memory only — never for production:

```python no-exec — not executed: continues the user-local module the guide is building
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
    print(handle.job_id)  # UUID of the enqueued job
    print(handle.was_existing)  # False for a fresh enqueue
```

The production path never goes through `open_worker_deps` from application
code: `PostgresBackend` is built internally by the worker and is not
constructible from a bare pool in standalone code — the `TaskQ` facade above
is the supported way to reach Postgres from a client process.

---

## Wait for a result

`JobHandle.wait()` polls until the job reaches a terminal status and returns the deserialized result:

```python no-exec — not executed: fragment, names bound by an earlier fence
result = await handle.wait(timeout=30.0)
print(result.message_id)  # SendEmailResult.message_id
```

`wait()` returns only once a **consumer** has run the job. Nothing executes it until then: in production the worker from the previous section is the consumer; in an in-process demo, register a stub and drain the backend before waiting:

```python no-exec — not executed: fragment, names bound by an earlier fence
backend.register_stub(send_email, lambda payload, ctx: SendEmailResult(message_id="msg-123"))
await backend.run_until_drained()
result = await handle.wait(timeout=30.0)  # now resolves
```

Without a consumer, `wait()` simply times out after `timeout` seconds — a bare `TimeoutError` with no other signal.

`wait()` raises:

- `JobFailed`: the job reached a non-success terminal state (`failed`, `cancelled`, `crashed`, or `abandoned`); the raw job row is attached as `exc.row`.
- `ResultUnavailable`: the job succeeded but no result was stored (e.g., result TTL expired, or the actor returned `None` while `R` is non-`None`).
- `TimeoutError`: `timeout` elapsed before any terminal transition was observed.

---

## Verify with health checks

Once a worker is running, probe it via the CLI health subcommands. `taskq health` requires a subcommand:

```bash
taskq health live     # returns 0 if the worker process is alive
taskq health ready    # returns 0 if the worker can reach the database
taskq health metrics  # returns current worker metrics
```

All health commands connect to the worker's Unix socket and return exit code 0 on success, 1 on failure. The commands must be run on the same host as the worker. An unconfigured worker binds the per-process default `/tmp/taskq_health_<pid>.sock` (logged at boot as `health-server-started`'s `socket_path`), so set `TASKQ_HEALTH_SOCKET_PATH` to that path before probing it.

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
| Going to production: timeouts, sizing, fan-out patterns, footguns | [Operations & Adoption](../guides/ops.md) |
| CLI command reference | [CLI](../guides/cli.md) |
| Admin UI | [Admin UI](../guides/admin-ui.md) |
| Testing with `InMemoryBackend` and pytest fixtures | [Development](../api-reference/testing.md) |
| Architecture internals | [Architecture](../architecture.md) |
