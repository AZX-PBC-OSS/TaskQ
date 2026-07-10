# Admin UI

The TaskQ admin UI is a read-only-by-default observability dashboard built with FastAPI and Jinja2. It shows live job, queue, worker, schedule, rate-limit, and reservation state drawn from Postgres. CSRF-protected write operations are available for job cancellation, job retry, and cron schedule management (enable, disable, skip, run-now), but are gated by `TASKQ_ADMIN_ACTIONS_ENABLED` (default `false` — set to `true` to enable them). The rate-limit reset endpoint is additionally gated by `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET=true`.

The frontend uses [Alpine.js](https://alpinejs.dev/) for reactive components, [HTMX](https://htmx.org/) for partial-page updates, and Jinja2 partial templates for composable UI pieces. SSE (Server-Sent Events) provides real-time updates when Redis is available; a polling fallback keeps the UI functional when it is not.

---

## Starting the UI

```sh
taskq ui serve
```

This starts a Uvicorn server bound to `TASKQ_ADMIN_HOST:TASKQ_ADMIN_PORT` (defaults: `0.0.0.0:8080`) and serves the admin router at `/admin`.

The server reads configuration from the standard `TASKQ_` environment variables (or `.env` files). See [cli.md](cli.md) for the full command reference.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `TASKQ_ADMIN_HOST` | `0.0.0.0` | Bind address for the admin server. |
| `TASKQ_ADMIN_PORT` | `8080` | Bind port for the admin server. |
| `TASKQ_ADMIN_URL` | `http://localhost:8080` | Public base URL as seen from a browser. Used by the example trigger app to build redirect URLs after enqueueing. Override when admin and trigger app are on different hosts or ports. |
| `TASKQ_PG_DSN` | `postgresql://taskq:taskq@localhost:5432/taskq` | Postgres connection string. |
| `TASKQ_REDIS_URL` | _(none)_ | Optional. When set, enables real-time mode (SSE push) and live Redis state on the rate-limits page. |
| `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` | `2.0` | Page refresh interval in polling and polling-degraded modes. |
| `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET` | `false` | When `True`, enables the reset button on the `/rate-limits` page. |
| `TASKQ_SCHEMA_NAME` | `taskq` | Postgres schema containing TaskQ tables. |
| `TASKQ_ENVIRONMENT` | _(none)_ | Set to `dev` or `development` to bypass the fail-closed auth check (local development only). |
| `TASKQ_ADMIN_UI_REQUIRE_AUTH` | `true` | When `true` (the default), `create_router()` raises `RuntimeError` in non-dev environments if no `auth_dependency` is configured. Set to `false` to suppress the error and allow an unauthenticated admin UI behind a reverse proxy (not recommended unless you have an external auth layer). |
| `TASKQ_ADMIN_ACTIONS_ENABLED` | `false` | When `true`, enables destructive admin actions: job cancel, job retry, and schedule run-now. When `false` (the default), these endpoints return `403`. Separate from `auth_dependency`, which controls read access to all admin routes. |
| `TASKQ_ADMIN_MAX_SSE_CONNECTIONS` | `50` | Per-topic cap on concurrent SSE connections. |
| `TASKQ_HEALTH_TOKEN` | _(none)_ | Bearer token for machine-to-machine access to `/jobs/health/*` endpoints. When set, health and metrics routes require a matching `Authorization: Bearer <token>` header. Leave empty for unauthenticated cluster-internal access. |
| `TASKQ_HEALTH_REQUIRE_TOKEN` | `true` | When `true` (the default), `taskq ui serve` raises `RuntimeError` if `TASKQ_HEALTH_TOKEN` is empty in a non-dev environment, failing closed. Set to `false` to allow unauthenticated health/metrics in non-dev (e.g. when relying on network policy). |

### Docker Compose

```yaml
services:
  taskq-admin:
    image: your-app
    command: taskq ui serve
    environment:
      TASKQ_PG_DSN: postgresql://taskq:taskq@postgres:5432/taskq
      TASKQ_REDIS_URL: redis://redis:6379/0
      TASKQ_ADMIN_HOST: "0.0.0.0"
      TASKQ_ADMIN_PORT: "8080"
    ports:
      - "8080:8080"
```

---

## Security

!!! danger "Unauthenticated by default"
    The admin UI has **no built-in authentication**. Anyone with network access
    to the admin port can view all job data, payloads, error tracebacks, and
    worker state. In production you **must** protect the admin UI with
    authentication middleware or a reverse proxy.

!!! tip "Built-in SSO support"
    TaskQ ships optional OIDC and SAML single sign-on backends behind a shared
    abstraction. See [SSO / SAML](sso.md) for Entra ID walkthroughs,
    configuration reference, and container requirements. `TASKQ_SSO_BACKEND=none`
    (the default) preserves the unauthenticated / BYO-auth behavior described
    below.

### Fail-closed authentication default

When `auth_dependency=None` (the default) and `TASKQ_ENVIRONMENT` is not `dev`
or `development`, the admin UI **fails closed**: `create_router()` raises
`RuntimeError` because `admin_ui_require_auth` defaults to `True`. This
prevents accidentally deploying an unauthenticated admin UI in production.

```sh
# The default — fails closed in non-dev:
TASKQ_ENVIRONMENT=production taskq ui serve
# RuntimeError: admin UI requires auth_dependency in non-dev environments
```

To opt out (e.g. when relying on a reverse proxy for authentication), set
`TASKQ_ADMIN_UI_REQUIRE_AUTH=false`:

```sh
export TASKQ_ENVIRONMENT=production
export TASKQ_ADMIN_UI_REQUIRE_AUTH=false
taskq ui serve
# WARNING log: admin-ui-no-auth — but server starts
```

Dev environments (`TASKQ_ENVIRONMENT=dev` or `development`) bypass the check
entirely — no `RuntimeError`, no warning. Use this only in local development:

```sh
TASKQ_ENVIRONMENT=development taskq ui serve
```

### Protecting the router with FastAPI authentication

When embedding the admin router in your own FastAPI app, pass an
`auth_dependency` callable. This is applied as a FastAPI `Depends()` to every
route in the router:

```python
from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

bearer = HTTPBearer()

async def require_token(
    credentials: HTTPAuthorizationCredentials = Security(bearer),
) -> str:
    if credentials.credentials != "your-secret-admin-token":
        raise HTTPException(status_code=401, detail="invalid admin token")
    return credentials.credentials

# Pass to create_router():
bundle = create_router(
    pg_pool,
    schema="taskq",
    redis_client=None,
    auth_dependency=require_token,   # protects all routes
    base_path="/admin",
)
```

Any FastAPI dependency callable works — `HTTPBearer`, `HTTPBasic`, OAuth2,
custom session middleware, etc.

### Protecting `taskq ui serve` with a reverse proxy

When running `taskq ui serve` as a standalone process (no custom FastAPI
app), place a reverse proxy in front that enforces authentication:

**nginx example:**

```nginx
server {
    listen 443 ssl;
    server_name admin.example.com;

    # ... TLS config ...

    location /admin/ {
        auth_basic "TaskQ Admin";
        auth_basic_user_file /etc/nginx/.htpasswd;
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

**Caddy example** (automatic HTTPS + basic auth):

```caddy
admin.example.com {
    basicauth {
        admin $2a$14$...hashed-password...
    }
    reverse_proxy 127.0.0.1:8080
}
```

### Rate-limit reset endpoint

The `POST /rate-limits/{bucket_name}/reset` endpoint is a **write operation**.
It is disabled by default (`TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET=false`). When
enabled, it clears a rate-limit bucket to full capacity via a CSRF-protected
form POST. Resets are logged at `WARNING` level.

Keep this disabled in production unless operators need fast incident-response
capability. If enabled, ensure the authentication layer (middleware or reverse
proxy) covers this endpoint — it is protected by the same `auth_dependency`
as all other routes.

### Job cancel and retry endpoints

The `POST /jobs/{job_id}/cancel` and `POST /jobs/{job_id}/retry` endpoints are
**write operations** gated by `TASKQ_ADMIN_ACTIONS_ENABLED` (default `false`).
When `admin_actions_enabled` is `false`, both endpoints return `403`. Set
`TASKQ_ADMIN_ACTIONS_ENABLED=true` to enable them. Both are CSRF-protected.
Cancel writes a cancel request to Postgres; retry resets a terminal job to
`pending` via `backend.retry_job`. Ensure the authentication layer covers
these endpoints in production — they can modify job state.

---

## Routes

All `GET` routes are read-only HTML pages. `POST` routes (cancel, retry, schedule management, rate-limit reset) are CSRF-protected write operations.

### `GET /admin/`

Redirects (302) to `/admin/queues`.

### `GET /admin/queues`

Queue overview. Lists all queues that have jobs in `pending`, `scheduled`, or `running` state. For each queue shows the count of jobs in each of those three statuses.

### `GET /admin/queues/{queue}`

Queue detail page. Lists jobs in the named queue filtered by `status` (query parameter; allowed values: `pending`, `scheduled`, `running`; default: `pending`). Results are paginated at 100 rows using keyset pagination on `(scheduled_at, id)`.

**Query parameters:**

| Parameter | Required | Description |
|---|---|---|
| `status` | No (default `pending`) | Filter by job status. |
| `cursor_at` | No | ISO 8601 timestamp cursor for the next page. Must be provided together with `cursor_id`. |
| `cursor_id` | No | UUID cursor for the next page. Must be provided together with `cursor_at`. |

Returns `400` if `status` is not an allowed value or if only one of `cursor_at` / `cursor_id` is provided.

### `GET /admin/history`

Historical job list. Shows completed (terminal) jobs from both the live `jobs` table (not yet pruned) and the `jobs_archive` table (already pruned). Rows are ordered most-recent-first by `finished_at`. Results are paginated at 50 rows using keyset pagination on `(finished_at DESC, id DESC)`.

A metrics bar at the top of the page shows the total result count (capped at "1000+" for large result sets), per-status counts, and the overall success rate for the filtered result set.

**Query parameters:**

| Parameter | Required | Description |
|---|---|---|
| `status` | No (default: all terminal) | Filter by one or more terminal statuses. Repeatable: `?status=succeeded&status=failed`. Allowed values: `succeeded`, `failed`, `cancelled`, `crashed`, `abandoned`. |
| `actor` | No | Exact match on actor name. |
| `queue` | No | Exact match on queue name. |
| `cursor_at` | No | ISO 8601 timestamp cursor for the next page. Must be provided together with `cursor_id`. |
| `cursor_id` | No | UUID cursor for the next page. Must be provided together with `cursor_at`. |

Returns `400` if a `status` value is not a terminal status, or if only one of `cursor_at` / `cursor_id` is provided.

Archived rows (from `jobs_archive`) are shown with an "archived" badge in the Source column. Still-live terminal rows (not yet pruned from `jobs`) are shown as "live".

### `GET /admin/jobs`

Job listing page with "Live Jobs" and "Archived" tabs. Supports filtering by status (multi-select), actor (substring match), queue, time range, identity key, fairness key, free-text search (matches job ID or actor), and tags. Results are paginated at 100 rows using keyset pagination and can be sorted by created_at, actor, queue, status, or attempt. HTMX partial refreshes update the table without a full page reload. A live SSE stream (`/admin/jobs/sse/live`) pushes state-change events for real-time badge updates.

**Query parameters (selected):**

| Parameter | Default | Description |
|---|---|---|
| `tab` | `live` | `live` (jobs table) or `archived` (jobs_archive table). |
| `status` | all (live) or terminal (archived) | Repeatable status filter: `?status=pending&status=running`. |
| `actor` | — | Substring match on actor name (ILIKE). |
| `queue` | — | Exact match on queue name. |
| `sort` / `order` | `created_at` / `desc` | Sort column and direction. |
| `cursor_at` / `cursor_id` | — | Keyset pagination cursor (both required together). |

### `GET /admin/jobs/{job_id}`

Job detail. Shows the full job record, attempt history from `job_attempts`, and the event log from `job_events`. Tracebacks are truncated to 2 000 characters with a `(N more characters)` suffix. Returns `404` if the job does not exist.

If the job has already been pruned to `jobs_archive`, the page loads from the archive table instead; attempt history comes from `job_attempts_archive` and the event log is empty (events are not archived). An "archived" banner is shown at the top of the page.

The job detail page includes a **Cancel** button (for non-terminal jobs) and a **Retry** button (for terminal jobs in `failed`, `crashed`, or `cancelled` state). Both are CSRF-protected POST forms. When `admin_actions_enabled` is `false` (the default), both buttons return `403` on submit; set `TASKQ_ADMIN_ACTIONS_ENABLED=true` to enable them.

### `POST /admin/jobs/{job_id}/cancel`

Cancels a non-terminal job by writing a cancel request via `backend.write_cancel_request`. The heartbeat loop will observe the cancel flag and drive the three-phase cancellation protocol. Returns `403` if `admin_actions_enabled` is `false`, `404` if the job does not exist, `409` if the job is already in a terminal state. Redirects to the job detail page on success.

### `POST /admin/jobs/{job_id}/retry`

Resets a terminal job (`failed`, `crashed`, or `cancelled`) back to `pending` via `backend.retry_job`, allowing it to be re-dispatched by a worker. Returns `403` if `admin_actions_enabled` is `false`, `404` if the job does not exist, `409` if the job is not in a retryable state. Redirects to the job detail page on success. The retry resets `attempt` to 0, clears error fields, and sets `status='pending'`.

### `GET /admin/jobs/count`

Returns `{"count": <int>}` for the given `tab` (`live` or `archived`) and the same filter
query params as `GET /admin/jobs` (`status`, `actor`, `queue`, `time_range`/`time_from`/`time_to`).
Used by the jobs list page to render the result count without re-fetching the full page.

### `GET /admin/api/history/stats`

Per-actor metrics as JSON. Returns aggregate execution statistics for all completed jobs in `jobs_archive`, grouped by `(actor, queue)`. Does not paginate; returns at most 200 rows ordered by total job count descending.

Response shape:

```json
{
  "actors": [
    {
      "actor": "send_email",
      "queue": "email",
      "total": 18420,
      "succeeded": 18100,
      "failed": 210,
      "cancelled": 80,
      "crashed": 20,
      "abandoned": 10,
      "avg_duration_ms": 340,
      "p50_duration_ms": 280,
      "p95_duration_ms": 950
    }
  ]
}
```

Duration percentiles are derived from `job_attempts_archive.duration_ms` via `percentile_cont`. Actors with no recorded attempt rows will have `null` for duration fields.

### `GET /admin/workers`

Workers overview. Lists all rows from the `workers` table ordered by `last_seen_at DESC`, with an `is_leader` flag computed by a LEFT JOIN on `maintenance_leader`.

### `GET /admin/leader`

Maintenance leader detail. Shows the current leader worker (hostname, pid, last seen). If no leader is elected, renders the template with `leader=None`. A watchdog-health indicator marks the leader as healthy when `last_seen_at` is within 30 seconds of the current wall-clock time.

### `GET /admin/schedules`

Cron schedule list. Reads from `cron_schedules` ordered by `next_fire_at`. If the table does not exist (cron migration not yet applied), renders with a notice: `"cron scheduling not installed — run taskq migrate up to enable"`.

Each schedule row includes buttons for the following CSRF-protected POST operations:

### `POST /admin/schedules/{schedule_id}/enable`

Sets `enabled=true`, resets `consecutive_failures` to 0, and clears `last_fire_error`. Returns `404` if the schedule does not exist.

### `POST /admin/schedules/{schedule_id}/disable`

Sets `enabled=false`. Returns `404` if the schedule does not exist.

### `POST /admin/schedules/{schedule_id}/skip`

Advances `next_fire_at` to the next computed fire time after the current one. Repeatedly advances until `next_fire_at` is in the future (up to 1000 iterations; returns `400` if the cron expression produces no future fire time). Returns `404` if the schedule does not exist.

### `POST /admin/schedules/{schedule_id}/run`

Enqueues a job for the schedule's actor immediately, using the schedule's `payload_factory` and the actor's stored `actor_config` row for queue, `max_attempts`, and `retry_kind`. Returns `403` if `admin_actions_enabled` is `false`, `404` if the schedule does not exist, `303` redirect with an error query parameter if the payload factory fails or the actor is not configured. A per-process 10-second cooldown prevents rapid re-triggering of the same schedule.

!!! warning "Per-process cooldown — not distributed"
    The run-now cooldown is tracked in-process (`asyncio` loop time), not in
    Postgres or Redis. In multi-replica deployments, each process has its own
    cooldown timer, so N replicas get N× the trigger rate. If you need a
    distributed cooldown, enforce it at the application layer or via an
    external rate limiter.

### `GET /admin/rate-limits`

Rate-limit state page. Reads all rows from `rate_limit_buckets` (bucket name, kind, state JSON, updated timestamp). When `TASKQ_REDIS_URL` is configured, also fetches live Redis hash state for each bucket and displays it alongside the Postgres state. If Redis is unavailable at render time, falls back to Postgres-only state without raising an error.

**Reset button.** When `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET=true` (default `false`), each bucket row shows a reset button that clears the bucket to full capacity via a CSRF-protected `POST` to `/rate-limits/{bucket_name}/reset`. Resets are logged at WARNING level. Keep disabled in production unless operators need fast incident-response capability.

### `GET /admin/reservations`

Reservation slot summary. For each `bucket_name` in `reservation_slots`, shows the count of held slots (where `job_id IS NOT NULL`), free slots, and total slots.

### `GET /admin/sse/{topic}`

SSE (Server-Sent Events) endpoint. Accepts any `topic` string. On connect it emits an initial `event: status` frame with `{"status": "awaiting_progress_backend"}`, then sends `: keepalive` comments every 30 seconds to prevent connection timeout. See [Real-time vs polling mode](#real-time-vs-polling-mode) below.

### `GET /admin/static/{path}`

Serves static assets (CSS, JS, images) from the bundled static directory. Path traversal is prevented: requests whose resolved path falls outside the static directory return `404`.

### Health routes

`taskq ui serve` mounts lightweight health endpoints at `/jobs/health/`:

| Route | Response | Description |
|---|---|---|
| `GET /jobs/health/live` | JSON `{"status": "ok"}` (200) or `{"status": "unresponsive"}` (503) | Liveness probe (event-loop responsiveness check). |
| `GET /jobs/health/ready` | JSON readiness report (200 or 503) | Readiness probe including Postgres ping. |
| `GET /jobs/health/metrics` | Prometheus text format (200) | Prometheus metrics (requires `taskq[prometheus]`). |

These endpoints use a lightweight PG pool ping for readiness (not the full
`WorkerDeps` health report that the worker process serves on its Unix socket).
The Prometheus metrics endpoint is mounted automatically when
`taskq[prometheus]` is installed.

#### Protecting health endpoints with a bearer token

Set `TASKQ_HEALTH_TOKEN` to require a matching `Authorization: Bearer <token>`
header on all health and metrics routes. This is intended for machine-to-machine
access (Prometheus scrapers, kubelet probes, CI scripts) where an interactive
OIDC/SAML login flow isn't practical:

```sh
export TASKQ_HEALTH_TOKEN='$(python -c "import secrets; print(secrets.token_urlsafe(32))")'
taskq ui serve
```

When `TASKQ_HEALTH_TOKEN` is empty (the default), health and metrics endpoints
are unauthenticated — standard for cluster-internal endpoints behind a network
policy. However, in non-dev environments (`TASKQ_ENVIRONMENT` not set to `dev`
or `development`), `taskq ui serve` **fails closed**: it raises `RuntimeError`
if `TASKQ_HEALTH_TOKEN` is empty and `TASKQ_HEALTH_REQUIRE_TOKEN` is `true`
(the default). This prevents accidentally deploying health/metrics endpoints
wide open.

To explicitly allow unauthenticated health/metrics in non-dev (e.g. when
relying on network policy or cluster-internal-only access):

```sh
export TASKQ_ENVIRONMENT=production
export TASKQ_HEALTH_REQUIRE_TOKEN=false
taskq ui serve
```

!!! warning "k8s liveness/readiness probes"
    When `TASKQ_HEALTH_TOKEN` is set, k8s liveness/readiness probes must be
    configured to send the bearer token, or set
    `TASKQ_HEALTH_REQUIRE_TOKEN=false` to explicitly disable the requirement.
    Many k8s probe configurations don't send auth headers by default.

---

## Real-time vs polling mode

The admin UI automatically selects its update strategy based on Redis availability, and shows a
badge in the top-right corner of every page indicating the current mode.

### Three-state badge

| Badge label | `data-mode` value | Meaning |
|---|---|---|
| **real-time mode** | `realtime` | Redis is configured and reachable. Pages update via SSE (`EventSource`). |
| **polling mode** | `polling` | No `TASKQ_REDIS_URL` configured. Pages refresh by polling Postgres on an interval. |
| **polling mode (Redis unavailable)** | `polling-degraded` | `TASKQ_REDIS_URL` is set but Redis is currently unreachable. Automatic fallback to Postgres polling. |

The server re-checks Redis health every 5 seconds (cached per process). The badge reflects the
result of the most recent check.

### Real-time mode (Redis configured)

When Redis is available, the page JS opens an `EventSource` connection to
`GET /admin/sse/{topic}`. Updates are pushed over that connection, which triggers
[HTMX](https://htmx.org/) partial-page refreshes without a full reload.

If the `EventSource` connection emits an error, the JS closes it and automatically falls back to
Postgres polling at `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` intervals. The badge transitions
to `polling-degraded`. A 30-second health heartbeat is sent on the SSE connection to keep it
alive through proxies that would otherwise time out idle connections.

### Polling mode (no Redis)

When Redis is not configured, the page JS polls Postgres directly using HTMX `hx-trigger="every Ns"`.
The poll interval is controlled by `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` (default `2.0` s).
All pages remain fully functional; data is just slightly less fresh than in real-time mode.

### Configuration

| Variable | Default | Description |
|---|---|---|
| `TASKQ_REDIS_URL` | _(none)_ | When set, enables real-time mode. Must be a valid Redis URL (e.g. `redis://localhost:6379/0`). |
| `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` | `2.0` | Page refresh interval (seconds) in polling and polling-degraded modes. Minimum: 0.1 s. |
| `TASKQ_ADMIN_MAX_SSE_CONNECTIONS` | `50` | Per-topic cap on concurrent SSE connections in real-time mode. |

### SSE connection limit

The `GET /admin/sse/{topic}` endpoint bounds concurrent connections per topic
with an independent `asyncio.Semaphore` sized to `TASKQ_ADMIN_MAX_SSE_CONNECTIONS`.
Valid topics are `jobs`, `workers`, `queues`, and `history`; each gets its own
semaphore.

- Default: `50`
- Minimum: `1` (validated at settings load time)
- When the semaphore is full, new connections receive `429 Too Many Requests` immediately.

!!! warning "`/admin/jobs/sse/live` has no connection cap"
    The jobs-list live-refresh endpoint `GET /admin/jobs/sse/live` is a
    **separate route** from `GET /admin/sse/{topic}`. It backs the LISTEN/NOTIFY
    stream that drives real-time job-table badge updates and has **no semaphore
    and no connection limit**. Only the generic `/admin/sse/{topic}` routes
    described above are capped by `TASKQ_ADMIN_MAX_SSE_CONNECTIONS`. If you need
    to bound live-refresh connections, enforce the limit at the reverse proxy or
    load balancer layer.

---

## Frontend architecture

### Technology stack

| Layer | Technology | Role |
|---|---|---|
| Templating | Jinja2 (partials via `{% include %}`) | Server-rendered HTML fragments |
| Reactivity | Alpine.js 3.x | `jobsPage`, `statusCombobox`, dark mode toggle |
| Partial updates | HTMX 2.x | `hx-get`, `hx-target`, `hx-trigger` for AJAX table refreshes |
| Real-time push | SSE (`EventSource`) | State-change events pushed from server |
| Icons | Lucide | Feather-compatible SVG icons |
| Styling | Tailwind CSS (utility classes inlined) | Dark-mode-aware responsive layout |

### Alpine.js components

**`jobsPage`** (`admin.js:47-185`) — the main job listing page component. Registered via
`Alpine.data("jobsPage", ...)` and wired in `jobs.html` with `x-data="jobsPage"`.
Configuration is passed from the Jinja2 template via `window.__taskqJobConfig`, set in
a `<script>` block in the page's `{% block head %}`.

Key features:
- **Tab switching** (`switchTab`) — switches between "Live Jobs" and "Archived" views
  by submitting the filter form with `tab` parameter.
- **Live refresh toggle** — when enabled, connects to SSE for real-time state-change
  events; when paused, polls on `poll_interval_ms` via `setInterval`. Pending-count
  badge shows accumulated events while paused.
- **SSE integration** — `connectSSE()` opens an `EventSource` to
  `{base_path}/sse/jobs`. On `error`, falls back to polling. On `state_change`
  events, updates the matching table row's status badge or increments the pending
  counter.
- **Table refresh** — `refreshTable()` fetches `{base_path}/jobs` with current
  filter parameters via HTMX (`HX-Request: true` header), swaps the
  `#job-table-container` element.

**`statusCombobox`** (`admin.js:187-217`) — a reactive multi-select dropdown for
job status filtering. Supports Select All, Active, Terminal, and Clear presets.
Statuses are rendered with color-coded classes from `STATUS_COLORS` and `CHIP_COLORS`
lookup maps.

### Partial templates

Reusable Jinja2 partials live in `src/taskq/web/templates/_partials/`:

| File | Purpose |
|---|---|
| `job_table.html` | Full job listing table with sortable headers, pagination, status badges, progress bars, and tag chips. Included via `{% include %}` from `jobs.html`. |
| `job_card.html` | `status_badge`, `duration_fmt`, and `timestamp_cell` macros shared by `job_table.html` and `job_detail.html`. |
| `table.html` | Generic `styled_table` macro with `table_header`, `table_body`, `table_row`, and `table_cell` call blocks. Used by `workers.html`, `queues.html`, and other list pages for consistent styling. |
| `sse_console.html` | SSE console panel (used for debugging real-time connections). |

### Real-time mode badge

Every page displays a mode badge in the top-right corner of the header (set in
`_base.html:35`). The badge's `data-mode` attribute and label are driven by
`realtime_mode` and `mode_label` template variables injected by route handlers.

| Badge label | `data-mode` | Meaning |
|---|---|---|
| **real-time mode** | `realtime` | Redis configured and reachable; SSE push active |
| **polling mode** | `polling` | No `TASKQ_REDIS_URL` configured; HTMX polling |
| **polling mode (Redis unavailable)** | `polling-degraded` | Redis configured but unreachable; automatic fallback |

The server re-checks Redis health every 5 seconds (cached per-process in
`_factory.py:_RedisHealthCache`). The badge reflects the most recent check.

The dark mode toggle (in `_base.html:37`) persists preference to `localStorage` and
uses Alpine's `x-data` and `x-init` for immediate class application (no flash of
unstyled content on page load).

---

## `create_router()` — embedding in your own FastAPI app

If you have an existing FastAPI application, you can mount the admin router directly instead of running `taskq ui serve`.

```python
from taskq.web.admin import create_router

router = create_router(
    pg_pool,
    schema="taskq",
    redis_client=None,        # pass a redis.asyncio.Redis instance to enable live Redis state
    auth_dependency=None,     # pass a FastAPI dependency callable to protect all routes
    base_path="/admin",
    backend=None,             # pass an existing Backend to reuse it instead of constructing one
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pg_pool` | `asyncpg.Pool` | required | Asyncpg connection pool. Must be open for the lifetime of the router. |
| `schema` | `str` | `"taskq"` | Postgres schema. Must match `[A-Za-z_][A-Za-z0-9_]*`; raises `ValueError` otherwise. |
| `redis_client` | `redis.asyncio.Redis \| None` | `None` | Optional Redis client. Enables live Redis state on the rate-limits page. |
| `auth_dependency` | `Callable \| None` | `None` | FastAPI dependency applied to all routes. If `None` and `TASKQ_ENVIRONMENT` is not `dev`/`development`, `create_router()` raises `RuntimeError` (default fail-closed). Set `TASKQ_ADMIN_UI_REQUIRE_AUTH=false` to suppress the error and allow unauthenticated access behind a reverse proxy. |
| `base_path` | `str` | `""` | Must match the prefix passed to `include_router`. Injected as a Jinja2 global so templates build correct URLs. |
| `backend` | `Backend \| None` | `None` | Optional pre-built `Backend` to reuse (e.g. one already created by your `JobsClient`). When `None`, the router builds its own `PostgresBackend` from `pg_pool`/`schema`. |

`create_router()` returns an `AdminBundle` containing the router and all values needed for `app.state`. Call it inside your lifespan so the pool is already open, then populate `app.state` via `setup_admin_state()` and mount the router:

```python
from contextlib import asynccontextmanager
import asyncpg
from fastapi import FastAPI
from taskq.settings import TaskQSettings
from taskq.web.admin import create_router, setup_admin_state

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = TaskQSettings.load()
    pool = await asyncpg.create_pool(str(settings.pg_dsn))
    try:
        bundle = create_router(
            pool,
            schema=settings.schema_name,
            redis_client=None,        # pass a redis.asyncio.Redis instance for live state
            auth_dependency=None,     # pass a FastAPI dependency to protect all routes
            base_path="/admin",
        )
        setup_admin_state(app, bundle)          # populates app.state.pg_pool, .schema, etc.
        app.include_router(bundle.router, prefix="/admin")
        yield
    finally:
        await pool.close()

app = FastAPI(lifespan=lifespan)
```

`setup_admin_state()` writes `pg_pool`, `schema`, `redis_client`, `templates`, `settings`, `base_path`, and `backend` onto `app.state`. Route handlers resolve these via `Depends(get_pg_pool)`, `Depends(get_templates)`, etc. You do not need to set `app.state` manually — `setup_admin_state()` handles it.

