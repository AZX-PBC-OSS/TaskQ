# Installation

This guide covers prerequisites, installation, and Docker setup for local development
and integration testing.

---

## Prerequisites

- **Python 3.12+** (3.13 is also supported)
- **Postgres** — the bundled Docker Compose uses `postgres:18.4`. Production requires
  a direct connection (not PgBouncer in transaction-pooling mode) for advisory locks and
  `LISTEN/NOTIFY`.
- **Redis** (optional) — required only for real-time progress fanout and admin UI live
  updates.
- **uv** or **pip** for package management.

---

## Install

```bash
pip install taskq-py                      # core only
pip install "taskq-py[redis]"             # + real-time SSE fanout
pip install "taskq-py[fastapi]"           # + admin UI
pip install "taskq-py[redis,fastapi]"     # full
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add taskq-py                           # core only
uv add "taskq-py[redis,fastapi]"          # full
```

**Extras:**

| Extra | Installs | Features enabled |
|-------|----------|-----------------|
| `taskq-py[redis]` | `redis>=7.4` | Real-time progress fanout via Redis pub/sub, Redis-backed rate limiters (`TokenBucket`, `SlidingWindow`) |
| `taskq-py[otel]` | `opentelemetry-sdk`, `opentelemetry-exporter-otlp` | Configuring OTel providers programmatically; in-process test utilities in `taskq.testing.otel` |
| `taskq-py[fastapi]` | `fastapi`, `jinja2`, `sse-starlette`, `uvicorn` | Admin UI (`taskq ui serve`), SSE progress bridge |
| `taskq-py[prometheus]` | `opentelemetry-exporter-prometheus` | Prometheus metric scrapes via `taskq.contrib.prometheus` |

**Without an extra installed**, the corresponding feature degrades gracefully or raises a clear `ImportError` with install instructions:

| Feature | Without extra | Behavior |
|---------|--------------|----------|
| Progress fanout | No `[redis]` | `ctx.progress()` still coalesces and flushes to Postgres. `JobHandle.progress_stream()` falls back to 500 ms PG polling. HTTP SSE endpoint returns 503. |
| Redis rate limiters | No `[redis]` | `TokenBucket(backend="redis")` / `SlidingWindow(backend="redis")` raise `ImportError` at acquire time. Use `backend="postgres"` or `backend="memory"` instead. |
| Admin UI | No `[fastapi]` | `taskq ui serve` raises `ImportError`. The admin UI requires FastAPI. |
| OTel SDK providers | No `[otel]` | `taskq.testing.otel` raises `ImportError`. The library emits OTel API spans/metrics regardless (they are no-ops without a configured provider). |
| Prometheus bridge | No `[prometheus]` | `taskq.contrib.prometheus` raises `ImportError`. |

---

## Docker Compose for integration tests

The bundled `docker-compose.yml` starts Postgres, Redis, and the admin UI in one command.
This is the fastest path to a running local environment.

```bash
docker compose up -d
```

Services started:

| Service | Port | Notes |
|---------|------|-------|
| `postgres` | 5432 | Postgres with `max_connections=200`, `shared_buffers=256MB` |
| `redis` | 6379 | Redis without persistence (`appendonly no`) |
| `admin` | 8080 | TaskQ admin UI — runs `taskq ui serve --migrate` on startup |

The `admin` service runs `taskq ui serve --migrate` which applies pending migrations before starting the UI. When
using the full compose stack you do not need to run migrations manually.

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

No env var is strictly required — `TASKQ_PG_DSN` defaults to
`postgresql://taskq:taskq@localhost:5432/taskq`. For any real deployment, set it to your
actual database.

```dotenv
# Direct PG DSN — sessions, LISTEN/NOTIFY, and advisory locks require this.
TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq

# Schema name for all TaskQ tables. Override if multi-tenanting.
TASKQ_SCHEMA_NAME=taskq

# Optional. Enables real-time progress fanout and admin UI live updates.
TASKQ_REDIS_URL=redis://localhost:6379/0
```

!!! warning "PgBouncer"
    Advisory locks and `LISTEN/NOTIFY` require a direct Postgres connection. Do not point
    `TASKQ_PG_DSN` at a PgBouncer endpoint in transaction-pooling mode. Use
    `TASKQ_PG_DSN_DIRECT` and `TASKQ_PG_DSN_POOLED` to split traffic — see
    [Configuration](../guides/configuration.md).

TaskQ loads configuration through `dotenvmodel` with cascading `.env` discovery:
`.env` → `.env.local` → `.env.{env}` → `.env.{env}.local`.

---

## Run migrations

Apply all pending migrations before starting a worker:

```bash
taskq migrate up
```

The command is idempotent — re-running against an up-to-date schema is a no-op. To inspect
applied and pending migrations without making changes:

```bash
taskq migrate status
```

Alternatively, set `TASKQ_MIGRATE_ON_START=true` to have the admin UI apply migrations
automatically at startup. Production workers should still run `taskq migrate up` manually
before the worker process starts.

---

## Next steps

- [:material-rocket-launch: Quick Start](quick-start.md) — Define an actor, start a worker, enqueue a job
- [:material-atom: Actors](../guides/actors.md) — `@actor` decorator reference
- [:material-engine: Workers](../guides/workers.md) — Worker configuration and lifecycle
- [:material-cog: Configuration](../guides/configuration.md) — All `TASKQ_*` environment variables
