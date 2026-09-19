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
| `TASKQ_REDIS_URL` | _(none)_ | Optional. When set, per-job progress streams over Redis pub/sub (the "real-time mode" badge) and the rate-limits page shows live Redis state. Page tables poll Postgres either way. |
| `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` | `2.0` | Page refresh interval (every page polls Postgres on it). |
| `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET` | `false` | When `True`, enables the reset button on the `/rate-limits` page. |
| `TASKQ_SCHEMA_NAME` | `taskq` | Postgres schema containing TaskQ tables. |
| `TASKQ_ENVIRONMENT` | _(none)_ | Set to `dev` or `development` to bypass the fail-closed auth check (local development only). |
| `TASKQ_ADMIN_UI_REQUIRE_AUTH` | `true` | When `true` (the default), `create_router()` raises `RuntimeError` in non-dev environments if no `auth_dependency` is configured. Set to `false` to suppress the error and allow an unauthenticated admin UI behind a reverse proxy (not recommended unless you have an external auth layer). |
| `TASKQ_ADMIN_ACTIONS_ENABLED` | `false` | When `true`, enables destructive admin actions: job cancel, job retry, and schedule run-now. When `false` (the default), these endpoints return `403`. Separate from `auth_dependency`, which controls read access to all admin routes. |
| `TASKQ_ADMIN_MAX_SSE_CONNECTIONS` | `50` | Per-topic cap on concurrent SSE connections. |
| `TASKQ_ADMIN_ACQUIRE_TIMEOUT` | `5.0` | Seconds a request waits for a Postgres pool checkout or a Redis read before answering `503` (`Retry-After: 2`). A wedged pool or a black-holed broker fails the request visibly instead of hanging it and every request behind it. |
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

!!! tip "Built-in SSO support"
    TaskQ ships optional OIDC and SAML single sign-on backends behind a shared
    abstraction. See [SSO / SAML](sso.md) for Entra ID walkthroughs,
    configuration reference, and container requirements. `TASKQ_SSO_BACKEND=none`
    (the default) preserves the BYO-auth / reverse-proxy behavior described
    below.

### Fail-closed by default

The admin UI **fails closed** in non-dev environments when no authentication
is configured. `admin_ui_require_auth` defaults to `True`, so
`create_router()` raises `RuntimeError` if `auth_dependency=None` and
`TASKQ_ENVIRONMENT` is not `dev` or `development`. This prevents accidentally
deploying an unauthenticated admin UI in production.

```sh
# The default: fails closed in non-dev:
TASKQ_ENVIRONMENT=production taskq ui serve
# RuntimeError: admin UI requires auth_dependency in non-dev environments
```

To opt out (e.g. when relying on a reverse proxy for authentication), set
`TASKQ_ADMIN_UI_REQUIRE_AUTH=false`:

```sh
export TASKQ_ENVIRONMENT=production
export TASKQ_ADMIN_UI_REQUIRE_AUTH=false
taskq ui serve
# WARNING log: admin-ui-no-auth: but server starts
```

Dev environments (`TASKQ_ENVIRONMENT=dev` or `development`) bypass the
`RuntimeError`, but still log the `admin-ui-no-auth` warning. That is the
configuration actually serving an unauthenticated admin UI, so it is the one
that most needs the signal. Use this only in local development:

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
    auth_dependency=require_token,  # protects all routes
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

All `GET` routes are read-only HTML pages. `POST` routes (cancel, retry, schedule management, rate-limit reset) are CSRF-protected write operations. Every page is reachable from the top navigation bar (Queues, Jobs, History, Workers, Actors, Batches, Schedules, Rate Limits, Reservations, Leader); the job detail page is linked from any job ID in the lists.

Every route checks its database connection out with a bound: a checkout that does not arrive within `TASKQ_ADMIN_ACQUIRE_TIMEOUT` (default 5 s — the pool is exhausted or Postgres is not answering) is answered with `503` and `Retry-After: 2`, and logged as `pool-acquire-timeout` with the pool's occupancy. The query itself is bounded by the pool's `command_timeout` (`taskq ui serve` sets one).

### `GET /admin/`

Redirects (302) to `/admin/queues`.

### `GET /admin/queues`

Queue overview. Lists all queues that have jobs in `pending`, `scheduled`, `running`, or `failed` state. For each queue shows the count of jobs in each of those four statuses, the number of live workers subscribed to it, and a stranded count.

The **Live Workers** column counts workers whose `last_seen_at` falls inside the `TASKQ_ADMIN_WORKER_LIVENESS_SECONDS` window, per queue subscription. It is the same read the leader's queue-depth sampler runs (`statement_timestamp()` bound over `workers_last_seen_idx`), so the page, the orphan banner, and the stranded-jobs detector all agree on which worker counts as alive. A queue with pending depth and zero live workers is unserved; that is the condition the `TaskQQueueUnserved` alert fires on.

The **Stranded** column counts pending and scheduled rows whose routing queue cannot dispatch them: the actor has no `actor_config` row, or no live worker serves the queue dispatch routes the row on (a re-pended row routes on its actor's stored assignment, not its own label). It is the stranded-jobs detector's SQL shape grouped by routing queue instead of by actor; the predicate reads only the `pending`/`scheduled` partial indexes (`jobs_dispatch_idx`, `jobs_scheduled_wake_idx`) and `workers_last_seen_idx`. A non-zero count is red; both strand shapes mean the depth column will not move no matter how many workers you add until the underlying condition is fixed.

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

Job listing page with "Live Jobs" and "Archived" tabs. Supports filtering by status (multi-select), actor (substring match), queue, time range, identity key, fairness key, free-text search (matches job ID or actor), and tags. Results are paginated at 100 rows using keyset pagination and can be sorted by created_at, started_at, actor, queue, status, or attempt. HTMX partial refreshes update the table without a full page reload. The table is polled at `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` whenever the Live Jobs tab is open; with the live toggle on, an SSE stream (`/admin/sse/jobs`, PG `LISTEN` on the schema's events channel) additionally brings a refresh forward the moment an event arrives. The events channel carries only the running-job cancel fast-path today — terminal writes and dispatch do not NOTIFY it, which is why polling stays the source of truth and SSE is an accelerator, never a replacement.

Sorting by `started_at` ascending together with a `status=running` filter is the "running longest" view: the jobs that have held a worker the longest come first. `started_at` is NULL for jobs that have not started, and those rows sort last in both directions (NULLS LAST) so paging through live rows is never interrupted by the not-yet-started tail.

The **Duration** column carries a live twin for exactly that view: a running row has no `finished_at`, so its settled `duration_ms` is NULL while it runs — the cell renders `running_for_ms` instead, the elapsed span since `started_at` computed by the database at render time (`clock_timestamp() - started_at`, the same single-arbiter shape the lease column uses; a Python-clock span would skew by the admin process's offset from the database clock). The live span renders amber with a "still running" tooltip so it cannot be misread as a settled duration, and disappears the row transitions terminal, where `duration_ms` takes over. Because the value is computed server-side on each request, both refresh modes — polling and the SSE-accelerated refresh — re-render it through the same table partial, as fresh as the last refresh.

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

The job detail page includes a **Cancel** button (for non-terminal jobs) and a **Retry** button (for jobs in any terminal state — `succeeded`, `failed`, `cancelled`, `crashed`, or `abandoned`). Both are CSRF-protected POST forms guarded by a browser `confirm()` dialog, so a double-click or stray Enter cannot fire the write. When `admin_actions_enabled` is `false` (the default), both buttons return `403` on submit; set `TASKQ_ADMIN_ACTIONS_ENABLED=true` to enable them.

### `POST /admin/jobs/{job_id}/cancel`

Cancels a non-terminal job by writing a cancel request via `backend.write_cancel_request`. The heartbeat loop will observe the cancel flag and drive the three-phase cancellation protocol. Returns `403` if `admin_actions_enabled` is `false`, `404` if the job does not exist, `409` if the job is already in a terminal state. Redirects to the job detail page on success.

### `POST /admin/jobs/{job_id}/retry`

Puts a job that has come to rest back to `pending` via `backend.retry_job`, allowing it to be re-dispatched by a worker. Every resting state is a valid source — `failed`, `crashed`, `cancelled`, `abandoned` and `succeeded`, so the replay path after a bad deploy and the put-back path after a worker restart are both supported. Returns `403` if `admin_actions_enabled` is `false`, `404` if the job does not exist, `409` if the job is `running` (re-pending a live attempt could run it twice) or already queued as `pending`/`scheduled`. Redirects to the job detail page on success. The retry leaves `attempt` where it is and raises `max_attempts` just enough to fund one more run, clears error fields and any stored result, and sets `status='pending'`. It also clears `schedule_to_close` **only when that deadline has already elapsed** — dispatch never claims a row whose deadline has passed, so keeping a stale deadline would leave the retried row undispatchable (swept back to `failed` on the next tick) even though the retry reported success. A still-future `schedule_to_close` is preserved unchanged: the original time budget still applies to the re-run.

### `GET /admin/jobs/count`

Returns `{"count": <int>}` for the given `tab` (`live` or `archived`) and the same filter
query params as `GET /admin/jobs` (`status`, `actor`, `queue`, `time_range`/`time_from`/`time_to`).
Used by the jobs list page to render the result count without re-fetching the full page.

### `GET /admin/api/history/stats`

Per-actor metrics as JSON. Returns aggregate execution statistics for all completed jobs the fleet still has a record of — `jobs_archive` UNIONed with the terminal rows still live in `jobs` — grouped by `(actor, queue)`. A terminal row stays in `jobs` for its whole prune retention before the prune sweep archives it, so an archive-only aggregate would hide an actor's freshest failures for exactly as long as they matter most; the live side counts terminal rows only (running/pending work never inflates executor totals, and its population is capped by prune retention).

Accepts an optional `window` query parameter (`1h`, `24h`, `7d`, `30d`; omitted or `all` is the default) that bounds both sides by `finished_at` on the database's clock. Unknown values are a clean `400`, never a silent fallback to all-time. Does not paginate; returns at most 200 rows ordered by total job count descending. This endpoint and the actors page render the same aggregate read through a shared helper, so the two cannot drift.

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
      "p95_duration_ms": 950,
      "last_activity_at": "2026-09-16T12:00:00+00:00",
      "last_error_class": "ValueError"
    }
  ]
}
```

Duration percentiles are derived from the attempt rows' `duration_ms` via `percentile_cont` (`job_attempts_archive` on the archive side, `job_attempts` on the live side). Actors with no recorded attempt rows will have `null` for duration fields. `last_activity_at` is the freshest `finished_at` the actor has on either side; `total` counts attempt rows (which equals the job count in the common single-attempt case). `last_error_class` is the error class of the actor's most recent completed row that carries one — the job row's own `error_class`, which every terminal failure path stamps (the classifier's exception name, `WorkerCrashed` on crash-reclaim, `DeadlineExceeded` on the deadline sweep), so "what is this actor dying of" reads without opening a job; `null` when none of the actor's rows ever carried one.

### `GET /admin/workers`

Workers overview. Lists all rows from the `workers` table ordered by `last_seen_at DESC`, with an `is_leader` flag computed by a LEFT JOIN on `maintenance_leader`, and a **Running / Max** column: the count of `jobs` rows in `status = 'running'` locked by that worker (one index seek per worker over `jobs_locked_by_worker_running_idx`, the same population the `taskq.worker.active_jobs` metric counts per process) against the `max_concurrency` the worker registered in its row metadata. Amber when the worker is at capacity; a dash when an older registration carried no capacity. Reserved-but-unclaimed capacity (rate-limit slots, in-flight dispatch probes) is in neither number.

The **Stall hotspots** column renders the worker's rolling tally of attributed event-loop stalls from the same metadata (`send_email x12 (gil_held)`, hottest actor first) — the actors whose synchronous code blocked that worker's event loop, as the lag watchdog attributed them. Empty when the worker attributed none. The tally counts ATTRIBUTED stalls per actor; the Running / Max column counts running rows. See [runbooks.md: Event-loop stall attribution](runbooks.md#event-loop-stall-attribution-worker-warnings).

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

!!! warning "Per-process cooldown, not distributed"
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

### `GET /admin/actors`

The `/admin/actors` page lists all stored `actor_config` rows with:

- Actor name, queue, max concurrent, max pending
- Active job count (pending + scheduled + running)
- Enabled schedule count
- Last updated timestamp

Each row also carries the executor statistics the shared per-actor stats read computes over the archive UNIONed with the live terminal population (the same aggregate `GET /admin/api/history/stats` serves, grouped by actor instead of by `(actor, queue)`), so the page answers "which actor is hot, which is failing, which is slow" without leaving the table:

- **Jobs**: total completed attempt rows for the actor (live terminal rows included, so a fresh failure counts before the prune sweep archives it), hottest actor first
- **Failures**: failed count with its share of the total, red when non-zero
- **Last Error**: the most recent error class the actor's completed rows carry (`null` → a dash) — the job row's own `error_class`, which every terminal failure path stamps, so "what is this actor dying of" reads without opening a job
- **p50 / p95 (ms)**: execution duration percentiles from the attempt rows' `duration_ms`
- **Last Activity**: the freshest `finished_at` the actor has on either side of the read

A **window toggle** beside the heading (`All time | 1h | 24h | 7d | 30d`, `?window=`) bounds both sides of the read by `finished_at`. The default is **all time** — the whole retained completed-job history; a named window is the recency view ("who failed in the last day"), anchored to the database clock server-side. Unknown window values are a clean `400`, never a silent fallback to all-time.

The stats read is capped at the 200 most active actors; the page says so when the cap is reached. Actors with completed-job history but no `actor_config` row (for example, one deregistered while its history is retained) render as "history only": their stats show, but they have no capacity fields and no Deregister button.

Each row with a config row has a **Deregister** button with `force` and `purge queue` checkboxes.
The form asks for confirmation in the browser before submitting.
Deregistration requires `TASKQ_ADMIN_ACTIONS_ENABLED=true`. The form is
CSRF-protected via the synchronizer-token pattern.

### POST /admin/actors/{actor}/deregister

Deregisters an actor. Form fields:
- `csrf_token` — CSRF synchronizer token (set by GET)
- `force` — checkbox; cancels pending/scheduled jobs and disables schedules
- `purge_queue` — checkbox; deletes the orphaned queues row

Response codes:
- `303` — success, redirects to `/actors?notice=deregistered+{actor}`
- `403` — admin actions disabled or CSRF validation failed
- `404` — actor not found (no `actor_config` row)
- `409` — actor has active jobs or enabled schedules (force=False)

### `GET /admin/batches`

Batch overview. Reads all rows from the `batches` table: batch ID (linked to its finalizer job's detail page when one is set), queue, status (`active`, `complete`, or `aborted`), expected size, consecutive failures against the failure threshold, originating actor, and created/completed timestamps. Active batches sort first, then the most recent rows. The page renders at most 200 batches and says so when the cap is reached; there is no pagination. If the batches migration has not been applied, renders a notice instead of raising.

### `GET /admin/sse/{topic}`

SSE (Server-Sent Events) endpoint over a PG `LISTEN` on the schema's events channel. `topic` is one of `jobs`, `workers`, `queues`, `history` (anything else is `400`). On connect it emits an initial `event: status` frame with `{"status": "awaiting_progress_backend"}`, then `event: state_change` frames as NOTIFY payloads arrive and `: keepalive` comments every 30 seconds. The jobs page is its only consumer today. Capped per topic by `TASKQ_ADMIN_MAX_SSE_CONNECTIONS`; see [How pages refresh](#how-pages-refresh) below.

### `GET /admin/sse/mode`

JSON probe answering `{"realtime": true|false}` from the same per-process cached Redis ping the pages render from. The job-detail page's script polls it every 30 seconds so a page that rendered in `polling-degraded` mode upgrades itself to real-time when Redis returns (and degrades itself when Redis goes away) without a manual reload. Registered before the `{topic}` route so the catch-all cannot claim the path; it is an admin-router route, so it works identically under `taskq ui serve` and when the router is mounted into a host application.

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
`taskq[prometheus]` is installed. The route mounting is the automatic part;
the `taskq_*` series it serves are populated by the shipped `taskq ui serve`
and worker startup wiring, which configure the metrics reader for you — see
[observability.md](observability.md) for the series inventory and alerting
rules.

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

## How pages refresh

Every page refreshes by **polling Postgres**; that is the source of truth in every
configuration. Two things sit on top of it, and neither replaces it:

* **The jobs page's SSE accelerator.** With the *Live refresh* toggle on, the jobs
  list also opens an `EventSource` to `GET /admin/sse/jobs`, a PG `LISTEN` on the
  schema's events channel. That channel carries only the running-job cancel
  fast-path today — terminal writes and dispatch never `NOTIFY` it, so an event can
  bring a refresh forward or update one row's badge in place, never stand in for
  the poll. If the stream drops (a proxy idle timeout, a server restart) the
  browser's own `EventSource` reconnect runs; the poll carries the page meanwhile.
  With the toggle **paused**, the table is frozen: the poll stops, the stream is
  closed, and the table stays exactly as the operator left it until they resume
  (which reloads it) or act on it themselves.
* **The job detail page's progress stream.** With Redis configured, the detail page
  streams per-job progress over `GET /admin/jobs/api/job/{job_id}/progress/stream`
  (Redis pub/sub, see [progress.md](progress.md)); without Redis it polls
  `GET /admin/jobs/api/job/{job_id}/state`.

The polling cadence is `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` (default `2.0` s,
minimum `0.1` s) on every page.

### The mode badge

Every page shows a badge in the top-right corner. It reports the Redis health
check that gates the **progress stream** — it says nothing about the jobs page's
`LISTEN` accelerator, which needs no Redis.

| Badge label | `data-mode` value | Meaning |
|---|---|---|
| **real-time mode** | `realtime` | Redis is configured and reachable: per-job progress streams over SSE. |
| **polling mode** | `polling` | No `TASKQ_REDIS_URL` configured: per-job progress is polled from Postgres. |
| **polling mode (Redis unavailable)** | `polling-degraded` | `TASKQ_REDIS_URL` is set but Redis is not answering: per-job progress falls back to polling until it is. |

The server re-checks Redis health every 5 seconds (cached per process). The badge reflects the
result of the most recent check. The job-detail page also re-checks client-side every 30 seconds
through `GET /admin/sse/mode` and transitions the badge in place, so a page that rendered during
a Redis outage upgrades itself without a manual reload. The job-detail progress driver loads in
all three modes: in real-time mode it opens the SSE stream, in the two polling modes it polls the
per-job state endpoint (`GET /admin/jobs/api/job/{job_id}/state`) directly.

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
| `TASKQ_REDIS_URL` | _(none)_ | When set, per-job progress streams over Redis pub/sub and the rate-limits page shows live Redis state. Must be a valid Redis URL (e.g. `redis://localhost:6379/0`). |
| `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` | `2.0` | Page refresh interval (seconds). Minimum: 0.1 s. |
| `TASKQ_ADMIN_MAX_SSE_CONNECTIONS` | `50` | Per-topic cap on concurrent `GET /admin/sse/{topic}` connections. |
| `TASKQ_PROGRESS_MAX_SSE_CONNECTIONS` | `50` | Cap on concurrent per-job progress streams in this process. |

### SSE connection limits

`GET /admin/sse/{topic}` bounds concurrent connections per topic with an
independent `asyncio.Semaphore` sized to `TASKQ_ADMIN_MAX_SSE_CONNECTIONS`.
Valid topics are `jobs`, `workers`, `queues`, and `history`; each gets its own
semaphore. When the semaphore is full, new connections receive
`429 Too Many Requests` immediately. The per-job progress stream is capped the
same way by `TASKQ_PROGRESS_MAX_SSE_CONNECTIONS`. Every stream holds a Postgres
`LISTEN` connection or a Redis pub/sub subscription and an asyncio task for as
long as the client stays connected, which is what the caps bound.

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
- **Polling** — the Live Jobs tab polls on `poll_interval_ms` via
  `setInterval` while live refresh is on; the poll is the source of truth for
  the table. The poll refetches the page the operator is on: the keyset cursor
  synced from the last pagination click rides along, so live mode never yanks
  a reader back to page one, and the SSE refresh-forward is skipped while a
  cursor is active (the poll already refreshes that page in place).
- **Live refresh toggle** — on, the poll runs and SSE is connected so a
  state-change event brings a refresh forward; paused, both stop and the table is
  frozen until the operator resumes (which reloads it) or acts on it.
- **SSE integration** — `connectSSE()` opens an `EventSource` to
  `{base_path}/sse/jobs`. On `error` the stream is left to the browser's own
  reconnect and polling carries on unchanged. A `state_change` event whose `status` matches a row in the table
  updates that row's badge in place; any other event — a payload without a
  `status` (the cancel NOTIFY names the job only) or a transition for a job the
  table does not show — refreshes the table from the server.
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
| **real-time mode** | `realtime` | Redis configured and reachable; per-job progress streams over Redis pub/sub |
| **polling mode** | `polling` | No `TASKQ_REDIS_URL` configured; per-job progress is polled |
| **polling mode (Redis unavailable)** | `polling-degraded` | Redis configured but unreachable; per-job progress falls back to polling |

Page tables refresh by polling in every mode; see [How pages refresh](#how-pages-refresh).

The server re-checks Redis health every 5 seconds (cached per-process in
`_factory.py:_RedisHealthCache`). The badge reflects the most recent check.

The dark mode toggle (in `_base.html:37`) persists preference to `localStorage` and
uses Alpine's `x-data` and `x-init` for immediate class application (no flash of
unstyled content on page load).

---

## `create_router()`: embedding in your own FastAPI app

If you have an existing FastAPI application, you can mount the admin router directly instead of running `taskq ui serve`.

```python
from taskq.web.admin import create_router

router = create_router(
    pg_pool,
    schema="taskq",
    redis_client=None,  # pass a redis.asyncio.Redis instance to enable live Redis state
    auth_dependency=None,  # pass a FastAPI dependency callable to protect all routes
    base_path="/admin",
    backend=None,  # pass an existing Backend to reuse it instead of constructing one
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
            redis_client=None,  # pass a redis.asyncio.Redis instance for live state
            auth_dependency=None,  # pass a FastAPI dependency to protect all routes
            base_path="/admin",
        )
        setup_admin_state(app, bundle)  # populates app.state.pg_pool, .schema, etc.
        app.include_router(bundle.router, prefix="/admin")
        yield
    finally:
        await pool.close()


app = FastAPI(lifespan=lifespan)
```

`setup_admin_state()` writes `pg_pool`, `schema`, `redis_client`, `templates`, `settings`, `base_path`, and `backend` onto `app.state`. Route handlers resolve these via `Depends(get_pg_pool)`, `Depends(get_templates)`, etc. You do not need to set `app.state` manually — `setup_admin_state()` handles it.

!!! note "When the host already writes `app.state.settings`"
    `setup_admin_state()` overwrites `app.state.settings` with TaskQ's
    `TaskQSettings`. A host application that keeps its own settings object
    under that key (its auth or config code reads it per request) must not
    call `setup_admin_state()`. Set every other key from the bundle
    individually, install the bundle's settings only for the portal's own
    dependency, and skip the collision:

    ```python
    for key in ("pg_pool", "schema", "redis_client", "templates", "base_path", "backend"):
        setattr(app.state, key, getattr(bundle, key))
    app.dependency_overrides[get_settings] = lambda: bundle.settings
    ```

    Route handlers read the other keys off `app.state` exactly as documented,
    and the portal's `get_settings` dependency serves `TaskQSettings` only to
    the admin pages.

