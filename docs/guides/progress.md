# Progress & Streaming

TaskQ provides a real-time progress pipeline: actors emit structured updates via
`ctx.progress()`, a worker-side buffer coalesces and flushes state to Postgres, and a Redis
pub/sub bridge delivers events to subscribers immediately. Consumers can receive events
server-to-client via:

- **`JobHandle.progress_stream()`** — Python async iterator (worker or service process)
- **HTTP SSE endpoint** — browser or HTTP client via `GET /api/job/{job_id}/progress/stream`
  relative to wherever the router is mounted. Under `taskq ui serve` (the admin UI), the
  admin router mounts this progress router at `/jobs`, and the admin router itself mounts
  at `/admin`, so the effective path is `GET /admin/jobs/api/job/{job_id}/progress/stream`
  (and `/admin/jobs/api/job/{job_id}/state`). See [Mounting the router](#mounting-the-router).

!!! info "Redis is optional"
    When the `[redis]` extra is not installed or `TASKQ_REDIS_URL` is not set:

    - `ctx.progress()` still works — updates are coalesced and flushed to Postgres.
    - `JobHandle.progress_stream()` falls back to 500 ms Postgres polling (higher latency, same data).
    - The HTTP SSE endpoint returns HTTP 503 with `{"error": "redis_not_configured"}`.
    - `TaskQ.stream()` falls back to PG LISTEN/NOTIFY (near-real-time, no Redis required).

    Install Redis for immediate event delivery:

    ```bash
    pip install "taskq-py[redis]"
    ```

---

## Contents

1. [How progress works](#how-progress-works)
2. [Reporting progress from an actor](#reporting-progress-from-an-actor)
3. [Consuming progress in Python](#consuming-progress-in-python)
4. [HTTP SSE endpoint](#http-sse-endpoint)
5. [Mounting the router](#mounting-the-router)
6. [Browser client example](#browser-client-example)
7. [Poll-state fallback](#poll-state-fallback)
8. [Gotchas & best practices](#gotchas-best-practices)
9. [Configuration](#configuration)

---

## How progress works

1. The actor calls `await ctx.progress(step=…, percent=…)`.
2. The per-job in-memory buffer is updated synchronously (last-writer-wins per field) and `seq` is
   incremented.
3. A `kind="progress"` event is scheduled as a background task to publish to the Redis channel
   `{schema}:progress:{job_id}`. `ctx.progress()` returns as soon as the buffer is updated — it
   does not wait for the publish to complete or even start.
4. A periodic flush loop writes the latest coalesced state to the `jobs.progress_state` JSONB
   column and `jobs.progress_seq` counter.
5. At job completion or crash, the buffer is flushed one final time before the terminal status
   is written.

This means **Redis subscribers see every `ctx.progress()` call** while **Postgres retains only
the most recent snapshot**. Clients reconnecting via `Last-Event-ID` receive a catch-up
snapshot from Postgres and then resume the live Redis stream.

**The Redis publish is fire-and-forget.** Because it runs as a background task, `ctx.progress()`
never blocks the calling actor code on the network — this holds even when an actor calls
`ctx.progress()` at high frequency in a tight loop. It is safe by design for the publish to
complete out of order relative to other in-flight publishes for the same job, or to be dropped
outright (e.g. on a transient Redis error): consumers of the SSE/pub-sub stream already discard
any event whose `seq` is not strictly greater than the last one they've seen, so an out-of-order
or missing event never corrupts displayed state. The Postgres-persisted `progress_state` /
`progress_seq` — flushed on the periodic coalesce interval described above — remains the durable
source of truth regardless of what happens on the Redis side. Failures publishing to Redis are
logged and recorded as a metric, never raised to the caller.

---

## Reporting progress from an actor

```python
from taskq import actor
from taskq.context import JobContext

@actor(queue="media")
async def transcode_video(payload: TranscodePayload, ctx: JobContext[TranscodePayload]) -> None:
    segments = await split_into_segments(payload.url)
    total = len(segments)

    for i, segment in enumerate(segments):
        await transcode_segment(segment)
        await ctx.progress(
            step=i + 1,
            percent=round((i + 1) / total * 100, 1),
            detail=f"Segment {i + 1}/{total}",
        )

    await ctx.progress(percent=100.0, detail="Done")
```

See [`ctx.progress()` in the Actor API](actors.md#progress-reporting) for the full
parameter reference.

---

## Consuming progress in Python

Use `JobHandle.progress_stream()` to iterate events in a service or worker process:

```python
handle = await client.enqueue(transcode_video, payload)

async for event in handle.progress_stream():
    if event.percent is not None:
        print(f"  {event.percent:.0f}% — {event.detail}")
    if event.terminal:
        print(f"finished: {event.status}")
        break
```

`progress_stream()` yields `ProgressEvent` objects and stops automatically when a
`terminal=True` event is received.

### `ProgressEvent` fields

| Field | Type | Description |
|---|---|---|
| `job_id` | `UUID` | Job this event belongs to. |
| `actor` | `str` | Actor name. |
| `ts` | `datetime` | Server-side timestamp. |
| `seq` | `int` | Strictly-monotone sequence number. |
| `status` | `str` | Current job status at publish time. |
| `step` | `int \| None` | Step counter. |
| `percent` | `float \| None` | Completion percentage. |
| `detail` | `str \| None` | Human-readable status string. |
| `data` | `dict[str, object] \| None` | Custom structured data. |
| `terminal` | `bool` | `True` when job has reached a terminal state. |

**Transport.** When Redis is configured, events are delivered via Redis pub/sub with no
polling delay. Without Redis the method falls back to polling Postgres at 500 ms intervals.
`NotImplementedError` is raised against `InMemoryBackend`.

---

## HTTP SSE endpoint

The `taskq.web.progress` module provides a FastAPI router with two routes:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/job/{job_id}/progress/stream` | Server-Sent Events stream |
| `GET` | `/api/job/{job_id}/state` | One-shot JSON poll |

### SSE stream

```
GET /api/job/{job_id}/progress/stream
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `last_event_id` | `int \| None` | Resume from this sequence number. Also read from the `Last-Event-ID` request header (WHATWG EventSource spec). |

**SSE event types:**

| `event` | `id` present | Description |
|---|---|---|
| `progress` | yes (`seq`) | Incremental progress update. `data` is a JSON-serialised `ProgressEvent`. |
| `terminal` | yes (`seq`) | Job reached a terminal state. `data` is a JSON-serialised `ProgressEvent`. Close the connection after receiving this. |
| `done` | no | Stream is closing. Close the connection. |
| `: keepalive` | — | SSE comment emitted every 15 s (configurable). No `event` field. |

**HTTP status codes:**

| Code | Meaning |
|---|---|
| `200` | Stream established. |
| `404` | Job not found. |
| `503` | Redis not configured or unavailable. `Retry-After: 2` header is set. |

**Reconnect semantics.** The browser's native `EventSource` sends `Last-Event-ID` on
reconnect automatically. The endpoint subscribes to Redis **before** querying Postgres so there
is no race window: if an event arrived between the disconnect and the reconnect it is caught by
the Redis subscription. A catch-up snapshot is emitted from Postgres when
`progress_seq > last_event_id`.

---

## Mounting the router

```python
from taskq.web.progress import create_router

progress_router = create_router(
    pg_pool,          # asyncpg.Pool
    redis_client,     # redis.asyncio.Redis | None
    schema="taskq",   # must match PostgresBackend schema
    auth_dependency=require_authenticated_user,  # optional FastAPI dep
    sse_heartbeat_interval=timedelta(seconds=15),
)

app.include_router(progress_router, prefix="/jobs")
# Produces:
#   GET /jobs/api/job/{job_id}/progress/stream
#   GET /jobs/api/job/{job_id}/state
```

**Under `taskq ui serve` / the admin UI.** `create_router()` in `taskq.web.admin` mounts this
same progress router internally at `/jobs` (`src/taskq/web/admin/_factory.py`), and the admin
router is itself mounted at `/admin` (`taskq ui serve` / `docs/guides/admin-ui.md`). The
resulting paths are `GET /admin/jobs/api/job/{job_id}/progress/stream` and
`GET /admin/jobs/api/job/{job_id}/state` — not the bare `/api/job/...` or `/jobs/api/job/...`
paths shown above, which only apply when you mount `taskq.web.progress.create_router()`
yourself at a different prefix. Without Redis configured, the stream endpoint returns
`503 {"error": "redis_not_configured"}` while the `/state` poll endpoint still works.

### `create_router()` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pg_pool` | `asyncpg.Pool` | required | Connection pool for snapshot reads. |
| `redis_client` | `redis.asyncio.Redis \| None` | required | Redis client. Pass `None` to disable streaming (SSE returns 503). |
| `schema` | `str` | `"taskq"` | PostgreSQL schema; must match the backend. |
| `auth_dependency` | `Callable \| None` | `None` | FastAPI `Depends`-compatible callable applied to all routes. |
| `sse_heartbeat_interval` | `timedelta` | `timedelta(seconds=15)` | Interval for keepalive SSE comments. |

---

## Browser client example

```javascript
const jobId = "018f1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b";
const url = `/jobs/api/job/${jobId}/progress/stream`;

const es = new EventSource(url);

es.addEventListener("progress", (e) => {
  const event = JSON.parse(e.data);
  document.getElementById("progress").textContent =
    `${event.percent ?? "?"}% — ${event.detail ?? ""}`;
});

es.addEventListener("terminal", (e) => {
  const event = JSON.parse(e.data);
  console.log("job finished:", event.status);
  es.close();
});

es.addEventListener("done", () => es.close());

es.onerror = (err) => {
  // EventSource reconnects automatically on transient errors.
  // The server uses Last-Event-ID to resume from where streaming left off.
  console.warn("SSE connection error, reconnecting…", err);
};
```

The browser's `EventSource` API handles reconnection automatically and sends `Last-Event-ID`
on each reconnect so no progress events are lost.

---

## Poll-state fallback

For environments where SSE is not available (e.g. HTTP/1.1 proxies that buffer responses,
or clients that do not support `EventSource`), the poll-state endpoint returns the current
snapshot as JSON:

```
GET /api/job/{job_id}/state
```

Response body:

```json
{
  "status": "running",
  "progress_state": {
    "step": 3,
    "percent": 60.0,
    "detail": "Segment 3/5"
  },
  "progress_seq": 3
}
```

Poll this endpoint at whatever interval suits your UI. Use `progress_seq` to detect staleness
between polls.

---

## Gotchas & best practices

!!! note "The Redis publish is fire-and-forget"
    `ctx.progress()` never blocks on the network, even under a tight loop calling it many times
    per second. The trade-off is that individual Redis publishes may arrive out of order or be
    dropped. This is safe: SSE/pub-sub consumers discard any event whose `seq` does not exceed
    the last one seen, and the periodically-flushed Postgres `progress_state`/`progress_seq`
    is always the durable source of truth — a dropped Redis publish never loses progress data,
    it only delays a subscriber's view of it until the next successful publish or the next poll.

- **Redis is optional, not required.** Without `TASKQ_REDIS_URL` configured (or the `[redis]`
  extra installed), `ctx.progress()` still coalesces and flushes to Postgres exactly as
  described above — only the Redis publish step is skipped. `JobHandle.progress_stream()`
  transparently falls back to 500 ms Postgres polling, and the HTTP SSE endpoint returns
  `503 {"error": "redis_not_configured"}` (the poll-state endpoint still works).
- **`InMemoryBackend` does not support streaming.** `progress_stream()` raises
  `NotImplementedError` when the client is constructed against `InMemoryBackend` (used in unit
  tests). Use the Postgres-backed test fixtures if a test needs to exercise progress streaming
  end to end.
- **High-frequency progress in tight loops is fine.** Because the Redis publish never blocks the
  actor and the coalesce buffer collapses intermediate values, calling `ctx.progress()` on every
  iteration of a hot loop is safe — only the periodic flush interval (`TASKQ_PROGRESS_COALESCE_INTERVAL`)
  bounds how much Postgres write traffic this generates, and Redis subscribers see every call
  regardless.

---

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `TASKQ_REDIS_URL` | `None` | Redis connection URL. Required for real-time progress delivery. |
| `TASKQ_PROGRESS_DATA_MAX_BYTES` | `16384` (16 KiB) | Maximum serialised size of the `data` dict passed to `ctx.progress()`. Range: 1 KiB – 1 MiB. Exceeding this raises `ProgressTooLarge`. |
| `TASKQ_PROGRESS_COALESCE_INTERVAL` | `0.5` | Seconds between periodic flush ticks that write coalesced state to Postgres. Minimum: 0.1 s. |
| `TASKQ_PROGRESS_PUBLISH_GLOBAL` | `true` | When `true`, every event is also published to a schema-wide fanout channel in addition to the per-job channel. Disable in high-throughput deployments without a global subscriber. |

Install the `redis` extra to enable real-time delivery:

```
uv add "taskq-py[redis]"
```

Without the `redis` extra:
- `ctx.progress()` still coalesces and flushes to Postgres but does **not** publish to Redis.
- `JobHandle.progress_stream()` falls back to 500 ms Postgres polling.
- The HTTP SSE endpoint returns HTTP 503.
