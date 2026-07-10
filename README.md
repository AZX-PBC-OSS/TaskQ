# TaskQ

Async-native, Postgres-backed background job library for Python 3.12+.

[![CI](https://github.com/AZX-PBC-OSS/TaskQ/actions/workflows/ci.yaml/badge.svg)](https://github.com/AZX-PBC-OSS/TaskQ/actions/workflows/ci.yaml)
[![PyPI version](https://img.shields.io/pypi/v/taskq-py.svg)](https://pypi.org/project/taskq-py/)
[![Python versions](https://img.shields.io/pypi/pyversions/taskq-py.svg)](https://pypi.org/project/taskq-py/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://AZX-PBC-OSS.github.io/TaskQ/)

> **Stability:** TaskQ is pre-1.0 and follows SemVer 0.x conventions — breaking
> changes may land in minor version bumps (`0.x.0`), not just majors. Pin an
> exact or narrow version range in production until 1.0.

> [!WARNING]
> **The bundled admin UI ships without authentication.** Anyone with network
> access to the admin port can view job payloads, error tracebacks, and
> worker state, and can trigger cancel/retry actions. Wrap it with the
> `auth_dependency` hook (or a reverse-proxy auth layer) before exposing it
> outside local development. See [guides/admin-ui.md](docs/guides/admin-ui.md#security).

## Features

- **Actors** — decorate plain `async def` (or sync) functions with `@actor`;
  payloads are validated with Pydantic models and dispatched as typed
  `ActorRef` handles.
- **Postgres-backed** — durable jobs, `SKIP LOCKED` dispatch, advisory-lock
  leader election, and a forward-only SQL migration runner. No external
  broker required.
- **Async-native** — built on `asyncio` and `asyncpg` from the ground up; no
  thread pools or sync wrappers on the hot path.
- **Rate limiting** — sliding-window and token-bucket algorithms with
  composition, a provider/registry layer, and Postgres fallback when Redis is
  unavailable.
- **Dependency injection** — scoped providers (LOOP, TRANSIENT, ...), cycle
  detection, and validation via the `_di` subsystem.
- **Admin UI** — FastAPI + htmx dashboard for inspecting jobs, queues, and
  workers, with live progress streaming over SSE.
- **Observability** — vendor-neutral OpenTelemetry spans/metrics and
  structured logging via `structlog`. Wire any OTLP-compatible backend
  (Datadog, Sentry, App Insights, ...) without importing vendor SDKs.
- **Cron scheduling** — declarative periodic actors with `cron(...)` /
  `ScheduleHandle` and a leader-elected cron loop.
- **Batch processing** — `enqueue_batch` / `enqueue_batch_fast` for fan-out.
  `wait_for_batch(db, batch_id)` is an in-actor finalizer helper (call it from
  a finalizer actor holding an `asyncpg` connection); client-side code that
  isn't inside an actor should instead poll `BatchHandle.status(db_connection)`.
  See [Jobs & Clients](docs/guides/jobs-clients.md#enqueue_batch).
- **Cancellation** — cooperative cancellation with grace periods and
  force-cancel sweeps; `ctx.check_cancelled()` inside actor bodies.
- **Progress tracking** — `ctx.progress(...)` events buffered and published
  to subscribers and the admin UI.
- **Workgroups** — multi-worker process supervision with a shared heartbeat
  and shutdown coordinator.
- **Retries** — pluggable `RetryPolicy` with backoff, snooze, and
  `RetryDecision` control flow.

## Installation

```bash
pip install taskq-py
```

Or with [`uv`](https://docs.astral.sh/uv/):

```bash
uv add taskq-py
```

Optional extras:

| Extra          | Adds                                                                |
| -------------- | ------------------------------------------------------------------- |
| `[redis]`      | Redis client for real-time progress fanout and Redis rate limiters  |
| `[fastapi]`    | FastAPI, Jinja2, sse-starlette, uvicorn for the admin UI and SSE    |
| `[otel]`       | OpenTelemetry SDK + OTLP exporter + instrumentation for provider setup, export, and testing |
| `[prometheus]` | OpenTelemetry Prometheus exporter for metric scrapes                |
| `[oidc]`       | OIDC SSO auth for the admin UI (authlib, httpx2, itsdangerous)     |
| `[saml]`       | SAML SSO auth for the admin UI (python3-saml, itsdangerous)        |
| `[reload]`     | `watchfiles` for autoreload during local development                |

The core install depends only on `opentelemetry-api` — no SDK or exporters
(see [Observability](docs/guides/observability.md)).

```bash
pip install "taskq-py[redis,fastapi,otel,prometheus]"
```

## Quick start

### Prerequisites

- Python 3.12+
- [`uv`](https://docs.astral.sh/uv/) for dependency management
- Docker (for the bundled Postgres 18 / Redis stack)
- PostgreSQL: tested against **PostgreSQL 18** (CI and `docker-compose.yml`
  both pin PG 18). No PG18-specific SQL has been identified in the bundled
  migrations, but earlier major versions are not covered by CI — treat
  PG 18 as the supported baseline until a version matrix is added.

### Bring up local infra

```bash
docker compose up -d postgres redis
cp .env.example .env
```

### Install and run migrations

```bash
uv sync
uv run taskq migrate status
uv run taskq migrate up
```

`migrate up` is idempotent — re-running is a no-op until new migrations land.

### Define an actor

```python
from pydantic import BaseModel

from taskq import JobContext, actor


class EmailPayload(BaseModel):
    to: str
    subject: str
    body: str


@actor(name="send_email", queue="default")
async def send_email(payload: EmailPayload, ctx: JobContext[EmailPayload]) -> None:
    ctx.check_cancelled()
    await ctx.progress(step=1, percent=50.0, detail="rendering template")
    # ... send the email ...
    await ctx.progress(step=2, percent=100.0, detail="sent")


# The worker's --actors flag resolves this dotted path (myapp.actors:registry).
registry = [send_email]
```

### Enqueue a job

```python
import asyncio

from taskq import TaskQ
from taskq.settings import WorkerSettings

from myapp.actors import EmailPayload, send_email


async def main() -> None:
    settings = WorkerSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        handle = await tq.enqueue(
            send_email,
            EmailPayload(to="alice@example.com", subject="Hi", body="Hello"),
        )
        print(f"enqueued job {handle.job_id}")
        await handle.wait(timeout=30.0)
        print("job finished")


asyncio.run(main())
```

### Run a worker

```bash
uv run taskq worker --actors myapp.actors:registry --queues default
```

The worker applies pending migrations at startup when
`TASKQ_MIGRATE_ON_START=true`, elects a leader via Postgres advisory locks,
and consumes jobs with `SKIP LOCKED` dispatch.

## Layout

```
src/taskq/
  __init__.py          - public API surface (re-exports, __version__)
  actor.py             - @actor decorator, ActorRef, ActorHandler
  backend/             - PostgreSQL backend (postgres.py), protocol, dispatch SQL, records,
                         sweeps, schedules, notify, SQL templates, state machine, clock
  client/              - TaskQ facade, JobsClient, JobHandle, sub-job enqueuer
  worker/              - consumer, leader election, shutdown, heartbeat, workgroup, cron loop
  ratelimit/           - sliding window, token bucket, composition, registry, reservations
  _di/                 - dependency injection, scopes, registry, solver, validation
  di.py                - public DI re-exports (ProviderRegistry, Scope)
  web/                 - admin UI (FastAPI + htmx), progress router, health, static/templates
  obs/                 - OpenTelemetry helpers, structlog configuration
  progress/            - progress events, buffering, flush, publishing
  testing/             - in-memory backend, fixtures, assertions, chaos helpers
  contrib/             - Prometheus metrics, Kubernetes alerting rules
  migrations/          - bundled SQL migration files ({schema} placeholder templated)
  cli.py               - `taskq` console entry point (typer)
  settings.py          - dotenvmodel-based TASKQ_* config
  retry.py             - RetryPolicy, RetryDecision, backoff
  exceptions.py        - control-flow + error hierarchy
  batch.py             - BatchHandle, EnqueueItem, wait_for_batch
  cron.py              - cron() function, ScheduleHandle, CronScheduleSpec
  scheduler.py         - register_cron registration helper
  context.py           - JobContext (cancellation, progress, sub-enqueue)
  migrate.py           - forward-only SQL migration runner
  _json.py             - orjson-backed dumps/loads (stdlib json never imported)
examples/              - runnable FastAPI trigger app + worker entrypoint
docker-compose.yml     - Postgres 18 + Redis 8 for local dev
```

## Toolchain

| Tool         | Purpose                                                |
| ------------ | ------------------------------------------------------ |
| uv           | Dependency + virtualenv management                     |
| ruff         | Linting AND formatting (single source of truth)        |
| pyright      | Strict type checking                                   |
| pytest + asyncio + testcontainers | Integration testing against real PG |
| typer        | CLI definitions                                        |
| pydantic v2  | Data models and validation                             |
| dotenvmodel  | Typed env config with cascading `.env` discovery       |
| orjson       | JSON serialization                                     |
| structlog    | Structured logging                                     |
| OpenTelemetry SDK (+ optional OTLP exporter) | Vendor-neutral observability |

## Observability

TaskQ never imports vendor SDKs (Sentry, Datadog, PostHog, App Insights).
Wiring is via OTLP — point `OTEL_EXPORTER_OTLP_ENDPOINT` at the Datadog
Agent, Sentry's OTel ingest, App Insights, or PostHog Cloud and the
spans/metrics flow through unchanged. The `ErrorReporter` Protocol is the
place to plug vendor-specific error routing without coupling the library to
any one backend.

## Configuration

All runtime config is namespaced with the `TASKQ_` prefix and loaded
through [`dotenvmodel`](https://pypi.org/project/dotenvmodel/) — drop a
`.env` in the project root, or set vars in your environment.

| Variable                      | Default                                              | Purpose                                       |
| ----------------------------- | ---------------------------------------------------- | --------------------------------------------- |
| `TASKQ_PG_DSN`                | `postgresql://taskq:taskq@localhost:5432/taskq`      | Direct PG DSN (sessions, LISTEN, advisory locks) |
| `TASKQ_SCHEMA_NAME`           | `taskq`                                              | Schema for all TaskQ tables                   |
| `TASKQ_REDIS_URL`             | _unset_                                              | Optional Redis URL for progress fanout        |
| `TASKQ_MIGRATE_ON_START`      | `false`                                              | Apply pending migrations on startup           |

See `src/taskq/settings.py` for the full set of knobs (pool sizes, heartbeat
intervals, grace periods, rate-limit fallback, metrics port, admin UI
options).

## Testing

The test suite is integration-first: pytest spins up a Postgres 18 container
via [`testcontainers`](https://testcontainers-python.readthedocs.io/) and
applies the bundled migrations against it.

```bash
uv run pytest                      # all tests
uv run pytest -m "not integration" # skip the testcontainers tier
```

Type checking and linting:

```bash
uv run pyright
uv run ruff check
uv run ruff format --check
```

## Documentation

Full documentation is hosted at
[https://AZX-PBC-OSS.github.io/TaskQ/](https://AZX-PBC-OSS.github.io/TaskQ/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Changes are tracked in
[CHANGELOG.md](CHANGELOG.md).

## License

[MIT](LICENSE)
