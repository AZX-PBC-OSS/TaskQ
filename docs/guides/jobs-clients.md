# Jobs & Clients

`JobsClient` is the primary entry point for enqueuing, querying, and cancelling jobs. It wraps a
`Backend` and adds typed payload serialisation, `JobHandle[R]` construction, and
`CancelResult` building.

---

## Job lifecycle

```
pending   → running   → succeeded
                       → failed        (retried → pending | terminal fail)
                       → cancelled
                       → crashed
                       → abandoned
                       → scheduled     (Snooze: job reschedules itself)

scheduled → pending   (scheduled_to_pending sweep, every ~1 s)

running   → scheduled (Snooze or RetryAfter with future scheduled_at)
```

| Status | Meaning |
|---|---|
| `pending` | Waiting in the queue; eligible for dispatch. |
| `scheduled` | Enqueued with a future `scheduled_at`; not yet eligible. |
| `running` | Claimed by a worker; actor is executing. |
| `succeeded` | Actor returned successfully; result stored. |
| `failed` | Actor raised an unhandled exception and retry budget is exhausted, or `DeadlineExceeded`. |
| `cancelled` | Cancelled before or during execution. |
| `crashed` | Worker process died mid-execution (SIGKILL, OOM, etc.). |
| `abandoned` | Heartbeat expired and no worker reclaimed the job within the lock lease window. |

Terminal statuses (`succeeded`, `failed`, `cancelled`, `crashed`, `abandoned`) have no further transitions.

**Archival lifecycle.** After a terminal job's per-status retention period elapses (default: 30–90 days depending on status), the maintenance leader's prune sweep moves it from `jobs` to `jobs_archive`. After the archive retention period elapses (default: 1 year), the archive expiry sweep hard-deletes the row. The admin UI job-detail page follows this chain automatically. See [Configuration](configuration.md) for retention settings.

**Deferred jobs.** Pass a timezone-aware `scheduled_at` datetime to delay execution. The job is stored with `status="scheduled"` and is not dispatched until `scheduled_at` is reached. The elected maintenance leader runs a `scheduled_to_pending` sweep every **1 second**, promoting any job whose `scheduled_at <= now` from `scheduled` to `pending`. After promotion the leader sends a `pg_notify` wake signal so idle workers pick up work immediately.

**Timing precision.** The sweep fires roughly once per second, so a job may be promoted up to ~1 second after its `scheduled_at`. Always pass UTC-aware datetimes (`datetime.now(UTC)`); naive datetimes are not accepted at the backend boundary.

---

## Contents

1. [Job lifecycle](#job-lifecycle)
2. [`JobsClient`](#jobsclient)
3. [`enqueue()`](#enqueue)
4. [`enqueue_batch()`](#enqueue_batch)
5. [`enqueue_batch_fast()`](#enqueue_batch_fast)
6. [`JobHandle[R]`](#jobhandler)
7. [`get()`](#get)
8. [`cancel()`](#cancel)
9. [`list()`](#list)
10. [`SubJobEnqueuer`](#subjobenqueuer)
11. [Error handling](#error-handling)
12. [Full enqueue-and-wait example](#full-enqueue-and-wait-example)
13. [Idempotency example](#idempotency-example)
14. [Batch enqueue example](#batch-enqueue-example)
15. [Tags](#tags)

---

## `JobsClient`

```python
from taskq import TaskQ
from taskq.settings import TaskQSettings

settings = TaskQSettings.load()
async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
    ...  # tq is a JobsClient-compatible client
```

`JobsClient` is lightweight — it performs no I/O at construction. Create one instance per
application and share it for the lifetime of the process. The connection pool is owned by the
`Backend`, not the client. Creating a `JobsClient` per-request adds unnecessary overhead and does
not provide isolation benefits.

### Constructor

```python
JobsClient(
    backend: Backend,
    *,
    clock: Clock | None = None,
    settings: TaskQSettings | None = None,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `backend` | `Backend` | required | The backend to delegate to. In production this is a `PostgresBackend`. In tests, use `InMemoryBackend`. |
| `clock` | `Clock \| None` | `SystemClock()` | Clock used to generate `scheduled_at` for immediate enqueues. Inject a `FakeClock` in tests for deterministic timestamps. |
| `settings` | `TaskQSettings \| None` | `None` | Settings instance threaded through to `JobHandle` for features (e.g. Redis-backed progress fanout) that need config beyond the backend connection. |

### `backend` property

```python
@property
def backend(self) -> Backend: ...
```

Returns the injected backend. Exposed so `JobHandle` and tooling can read the backend without
accessing the private `_backend` attribute.

---

## `enqueue()`

```python
async def enqueue(
    self,
    ref: ActorRef[P, R],
    payload: P,
    *,
    queue: QueueName | None = None,
    scheduled_at: datetime | None = None,
    priority: int | None = None,
    schedule_to_close: datetime | None = None,
    start_to_close: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    identity_key: IdentityKey | None = None,
    fairness_key: str | None = None,
    idempotency_key: IdempotencyKey | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    metadata: dict[str, object] | None = None,
    tags: list[str] | None = None,
) -> JobHandle[R]: ...
```

Serialises the payload through `ref.payload_type`, enqueues the job, and returns a typed
`JobHandle[R]`.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ref` | `ActorRef[P, R]` | required | The actor to dispatch. |
| `payload` | `P` | required | The payload model instance. Re-validated through `ref.payload_type` before insertion. |
| `queue` | `QueueName \| None` | `ref.queue` | Override the actor's default queue. Must match `[A-Za-z_][A-Za-z0-9_.-]*`. |
| `scheduled_at` | `datetime \| None` | `clock.now()` | When to make the job eligible for dispatch. `None` means immediate. Pass a future `datetime` for deferred execution. |
| `priority` | `int \| None` | `None` | Dispatch priority. Higher values are dispatched first within the same queue. |
| `schedule_to_close` | `datetime \| None` | derived from `retry.time_budget` | Hard deadline: if the job has not reached a terminal state by this datetime it fails with `DeadlineExceeded`. Overrides the actor's `time_budget`-derived interval when both are set. |
| `start_to_close` | `timedelta \| None` | `None` | Per-attempt execution timeout measured from when the worker locks the job, enforced via `asyncio.wait_for` around the actor invocation. Distinct from `schedule_to_close` — see [`start_to_close` vs `schedule_to_close`](retries.md#7-start_to_close-vs-schedule_to_close) for the precedence chain and full explanation. |
| `heartbeat_timeout` | `timedelta \| None` | `None` | Maximum time allowed between heartbeats before the job is considered crashed. |
| `identity_key` | `IdentityKey \| None` | `None` | Opaque string identifying the logical entity this job belongs to (e.g. `"account:42"`). Required for `unique_for` deduplication to take effect. Also used for fairness scheduling. |
| `fairness_key` | `str \| None` | `None` | Partitions the dispatch order so no single key monopolises the queue. |
| `idempotency_key` | `IdempotencyKey \| None` | `None` | Globally-unique string preventing duplicate insertion. See [Idempotency key](#idempotency_key). |
| `trace_id` | `str \| None` | extracted from OTel span | Trace ID for distributed tracing. Automatically extracted from the active OTel span when one is valid; pass explicitly to override. |
| `span_id` | `str \| None` | extracted from OTel span | Span ID for distributed tracing. See `trace_id`. |
| `metadata` | `dict[str, object] \| None` | `{}` | Per-job metadata stored in the `jobs.metadata` JSONB column. Merged with the library-injected `singleton` key when applicable. The caller's dict is never mutated. |
| `tags` | `list[str] \| None` | `[]` | Per-job tags stored in `jobs.tags text[]`. Must match `^[\w][\w\-]+[\w]$` (3–255 chars). Used for filtering and categorization in queries and the admin UI. See [Tags](#tags). |

### Enqueue evaluation order

Each call to `enqueue()` runs these checks in order. A match at step 1 short-circuits all
remaining steps. Later steps only execute when earlier ones did not match or raise.

1. **Payload validation** — Pydantic re-validates the payload through `ref.payload_type`. Raises
   `PayloadValidationError` on failure (non-retryable).
2. **`unique_for` dedup check** — if `identity_key` is provided and the actor has `unique_for`
   set, scans for an existing job with the same `(actor, identity_key)` within the window and the
   configured `unique_states`. On match, returns the existing handle with `was_existing=True` and
   skips all remaining steps.
3. **Singleton pre-flight** — if `ref.singleton` is `True`, checks for an existing active job for
   this actor. Raises `SingletonCollisionError` on collision.
4. **`max_pending` count check** — if `ref.max_pending` is set, counts `pending + scheduled` jobs
   for this actor. Raises `MaxPendingExceededError` when `count >= max_pending`.
5. **`idempotency_key` upsert** — if `idempotency_key` is provided and matches an existing row,
   returns the existing handle with `was_existing=True`.
6. **Job INSERT** — inserts the new row and returns a handle with `was_existing=False`.

### `idempotency_key`

- Keys are **globally unique**, not scoped to an actor. Namespace to avoid collisions:
  `"send_receipt:order_123"`, not `"order_123"`.
- Maximum length: **256 characters**.
- Empty strings and whitespace-only strings raise `ValueError` before any backend call.
- `idempotency_key` does **not** bypass `max_pending`. The idempotency check fires at step 5,
  after the `max_pending` check at step 4. On the **first** call with a new key, `max_pending` is
  evaluated normally — if the queue is full, `MaxPendingExceededError` is raised and the job is
  not inserted. On **subsequent** calls with the same key (after the key was successfully
  inserted), the existing `JobHandle` is returned at step 5 without re-checking `max_pending`.
  Only `unique_for` (step 2) bypasses `max_pending` unconditionally.

### `unique_for` and `singleton` interaction

`singleton=True` and `unique_for` can coexist on the same actor. They enforce different
constraints:

- `singleton=True` blocks a second enqueue as long as **any** active job for this actor exists,
  regardless of `identity_key`.
- `unique_for` + `identity_key` deduplicates within the configured window **per identity**.

When both are set and `identity_key` is provided, `unique_for` is evaluated first (step 2). A
dedup hit returns the existing handle without reaching the singleton pre-flight check at step 3.
If the `unique_for` window has elapsed and a new job is being inserted, the singleton check at
step 3 fires and may raise `SingletonCollisionError`.

### `was_existing`

`JobHandle.was_existing` is `True` when either the `unique_for` dedup (step 2) or the
`idempotency_key` upsert (step 5) matched an existing job row. Use this instead of comparing
`created_at` timestamps to detect a deduplicated return:

```python
handle = await client.enqueue(my_actor, payload, idempotency_key="order-123")
if handle.was_existing:
    print("job already enqueued, reusing:", handle.job_id)
```

The same field is set for `unique_for` dedup:

```python
handle = await client.enqueue(
    sync_account,
    SyncPayload(account_id="acct_123"),
    identity_key="acct_123",
)
if handle.was_existing:
    print("deduplicated within unique_for window:", handle.job_id)
```

### OTel trace propagation

When an active OTel span is valid, `trace_id` and `span_id` are extracted automatically. Pass
`trace_id=` and `span_id=` explicitly to override or to propagate an external trace context.

---

## `enqueue_batch()`

```python
async def enqueue_batch(
    self,
    items: list[EnqueueItem],
    *,
    batch_id: UUID | None = None,
    connection: "asyncpg.Connection | None" = None,
) -> BatchHandle: ...
```

Enqueues multiple jobs in a single batched INSERT and returns a `BatchHandle` containing one
`JobHandle` per item.

All items share a single `batch_id` UUID written into each job's `metadata.batch_id` field.
Supply `batch_id` to set it explicitly; omit it and a UUIDv7 is generated automatically.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `items` | `list[EnqueueItem]` | required | 1–1000 items to enqueue. |
| `batch_id` | `UUID \| None` | auto-generated UUIDv7 | Shared identifier for all jobs in this batch. |
| `connection` | `asyncpg.Connection \| None` | `None` | Specific connection to use; useful for transactional enqueues. |

### `EnqueueItem`

```python
from taskq import EnqueueItem

EnqueueItem(
    actor_ref=my_actor,                     # ActorRef[P, R]
    payload=MyPayload(...),                # validated against actor.payload_type
    scheduled_at=None,                     # datetime | None
    priority=None,
    fairness_key=None,
    idempotency_key=None,                  # str | None, ≤ 256 chars
    identity_key=None,
    metadata={},
)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `actor_ref` | `ActorRef[Any, Any]` | required | The actor to dispatch. |
| `payload` | `BaseModel` | required | Payload instance; validated against `actor_ref.payload_type` before any INSERT. |
| `scheduled_at` | `datetime \| None` | `None` | Deferred execution time. |
| `priority` | `int \| None` | `None` | Dispatch priority within the queue. |
| `fairness_key` | `str \| None` | `None` | Fairness grouping key. |
| `idempotency_key` | `IdempotencyKey \| str \| None` | `None` | Per-item idempotency token (≤ 256 chars). |
| `identity_key` | `IdentityKey \| None` | `None` | Opaque identity string; required for `unique_for` dedup to take effect. |
| `metadata` | `dict[str, object]` | `{}` | Per-job metadata. Do **not** set `batch_id` here — the library overwrites it. |
| `tags` | `list[str] \| None` | `None` | Per-job tags. See [Tags](#tags). |

### Validation

- `len(items) == 0` raises `ValueError`.
- `len(items) > 1000` raises `ValueError`.
- **All** payloads are validated before any INSERT. A single validation failure raises
  `PayloadValidationError` and leaves no rows inserted.
- `max_pending` is checked in one aggregated query across all actors in the batch.
  Any violation raises `MaxPendingExceededError` before the INSERT.
- Idempotency-key collisions return the existing `JobHandle` with `was_existing=True`,
  same as single-item `enqueue()`.

### `BatchHandle`

`enqueue_batch()` returns a `BatchHandle`:

| Field | Type | Description |
|---|---|---|
| `batch_id` | `UUID` | Shared ID for all jobs in this batch. |
| `job_handles` | `list[JobHandle[Any]]` | One handle per item in the original list. |
| `size` | `int` | `len(job_handles)`. |

#### `BatchHandle.status()`

```python
async def status(
    self,
    db: asyncpg.Connection,
    *,
    schema: str = "taskq",
) -> BatchCompletionStatus: ...
```

Issues a single GROUP BY query against the `jobs` table (using the GIN-indexed `metadata @>`
containment filter) and returns aggregated counts:

| Field | Type | Description |
|---|---|---|
| `total` | `int` | Total jobs in the batch. |
| `pending` | `int` | Jobs still in flight (`pending + scheduled + running`). |
| `succeeded` | `int` | Jobs that completed successfully. |
| `failed` | `int` | Jobs that exhausted retries. |
| `cancelled` | `int` | Cancelled jobs. |
| `crashed` | `int` | Jobs that crashed without a clean failure. |
| `abandoned` | `int` | Jobs abandoned after heartbeat timeout. |
| `is_complete` | `bool` (computed) | `True` when `pending == 0`. |

```python
status = await batch_handle.status(db_connection)
if status.is_complete:
    print(f"batch done — {status.succeeded} succeeded, {status.failed} failed")
else:
    print(f"{status.pending}/{status.total} jobs still running")
```

**`BatchHandle.status()` vs `wait_for_batch()`.** `BatchHandle.status()` is a one-shot poll —
call it from client-side code (a request handler, a script, a poll loop) whenever you want a
snapshot of a batch's completion. `taskq.batch.wait_for_batch(db, batch_id)` is a different,
**in-actor** helper: call it from *inside* a finalizer actor holding an `asyncpg` connection —
it raises `Snooze(snooze_interval)` while jobs are still in flight so the actor's own retry/snooze
loop drives the wait, and returns `BatchCompletionStatus` once all jobs are terminal. Use
`wait_for_batch()` for the fan-out-then-finalize pattern (see [batch enqueue
example](#batch-enqueue-example)); use `BatchHandle.status()` everywhere else.

---

## `enqueue_batch_fast()`

```python
async def enqueue_batch_fast(
    self,
    items: list[EnqueueItem],
    *,
    batch_id: UUID | None = None,
    connection: "asyncpg.Connection | None" = None,
) -> int: ...
```

Enqueues jobs via the PG `COPY FROM` protocol for maximum throughput. Returns the count of inserted rows — no `BatchHandle` or `JobHandle` instances.

### Tradeoffs vs `enqueue_batch()`

| Feature | `enqueue_batch()` | `enqueue_batch_fast()` |
|---------|-------------------|------------------------|
| Protocol | UNNEST INSERT | COPY FROM |
| Throughput | ~10K-50K rows/s | ~100K-500K rows/s |
| Max batch size | 1,000 | 50,000 |
| Idempotency key | Yes (ON CONFLICT) | No (duplicate key aborts entire batch) |
| Return value | `BatchHandle` with `JobHandle` per item | `int` (row count) |
| `max_pending` check | Yes | No |
| Partial success | Yes | No (all-or-nothing atomicity) |

### Limitations

- **No idempotency-key collision handling.** A duplicate key raises `asyncpg.UniqueViolationError` and aborts the entire batch. Callers must pre-deduplicate.
- **No max_pending check.** The caller is responsible for ensuring actor limits are not exceeded.
- **No JobHandle instances.** Only the inserted count is returned. Use `batch_id` to query rows post-insert.
- **All-or-nothing.** The COPY fails entirely on any constraint violation — singleton, unique index, or CHECK constraint.

### Validation

- `len(items) == 0` raises `ValueError`.
- `len(items) > 50_000` raises `ValueError`.
- ALL payloads are validated before any INSERT. A single `PayloadValidationError` aborts the batch.

Use for bulk import / backfill with 1K–50K rows where throughput matters more than idempotency guarantees.

---

## `JobHandle[R]`

Returned by `enqueue()`, `enqueue_batch()`, and `get()`. The type parameter `R` flows from the actor's declared return
type.

```python
handle: JobHandle[OrderResult] = await client.enqueue(process_order, payload)
```

### Properties

| Property | Type | Description |
|---|---|---|
| `job_id` | `JobId` (UUID) | The job's unique identifier. |
| `actor_name` | `str` | The actor this job targets. |
| `queue` | `str` | The queue the job was enqueued on. |
| `was_existing` | `bool` | `True` when the handle wraps a deduplicated (existing) job rather than a fresh insert. |

### Methods

#### `wait()`

```python
async def wait(self, *, timeout: float | None = None) -> R: ...
```

Polls the backend at 0.5 s intervals until the job reaches a terminal status, then validates the
stored result through `result_adapter` and returns `R`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `timeout` | `float \| None` | `None` | Maximum seconds to wait. `None` means wait indefinitely. |

**Returns:** `R` — the actor's validated return value. Never `R | None`.

**Raises:**

| Exception | When |
|---|---|
| `JobFailed` | Terminal status is not `"succeeded"` (`"failed"`, `"cancelled"`, `"crashed"`, `"abandoned"`). The `row` attribute carries the full `JobRow` for inspection. |
| `ResultUnavailable` | Status is `"succeeded"` but no result is stored (TTL expired, actor returned `None` while `R` is non-`None`). The `row` attribute is available. |
| `TimeoutError` | `timeout` elapsed before a terminal transition was observed. |

#### `status()`

```python
async def status(self) -> JobStatus: ...
```

Single non-blocking backend read returning the current `JobStatus`. Does not poll. Raises
`RuntimeError` if the handle was created via `ctx.jobs.enqueue()` (no client available).

#### `refresh()`

```python
async def refresh(self) -> JobRow: ...
```

Re-reads the full `JobRow` from the backend. Returns the current row regardless of status — does
not block on terminal state. Raises `RuntimeError` without a client.

#### `attempts()`

```python
async def attempts(self) -> list[AttemptRow]: ...
```

Returns all `AttemptRow` records for this job, ordered by attempt number. Raises `RuntimeError`
without a client.

#### `cancel()`

```python
async def cancel(self, reason: str | None = None) -> CancelResult: ...
```

Delegates to `JobsClient.cancel()`. Raises `RuntimeError` without a client.

#### `progress_stream()`

```python
async def progress_stream(self) -> AsyncIterator[ProgressEvent]: ...
```

Streams live progress events for this job. When Redis is configured, subscribes to the per-job
Redis pub/sub channel and yields `ProgressEvent` objects in real time. When Redis is not
available, falls back to polling Postgres every 500 ms and synthesising events from row diffs.

Yields until a `terminal=True` event is produced (the job reached a terminal status).

**`ProgressEvent` fields:**

| Field | Type | Description |
|---|---|---|
| `job_id` | `UUID` | The job this event belongs to. |
| `actor` | `str` | Actor name. |
| `ts` | `datetime` | Server-side timestamp. |
| `seq` | `int` | Strictly-monotone sequence number. Use for dedup and `Last-Event-ID` resumption. |
| `status` | `str` | Current job status at publish time. |
| `step` | `int \| None` | Step counter, if reported by the actor. |
| `percent` | `float \| None` | Completion percentage, if reported. |
| `detail` | `str \| None` | Human-readable status message, if reported. |
| `data` | `dict[str, object] \| None` | Custom progress data, if reported. |
| `terminal` | `bool` | `True` when the job has reached a terminal state. |

```python
async for event in handle.progress_stream():
    if event.percent is not None:
        print(f"{event.percent:.0f}% — {event.detail}")
    if event.terminal:
        print(f"job finished with status: {event.status}")
        break
```

**Limitations:**
- Raises `NotImplementedError` when using `InMemoryBackend` — the in-memory backend does not
  support pub/sub.
- Requires the `redis` extra (`uv add "taskq-py[redis]"`) for real-time delivery. Without Redis the
  fallback polls Postgres at 500 ms intervals.

For the HTTP SSE endpoint that browser clients can subscribe to, see
[Progress & Streaming](progress.md).

---

## `get()`

```python
async def get(
    self,
    job_id: JobId,
    *,
    result_adapter: TypeAdapter[R] | None = None,
) -> JobHandle[R] | None: ...
```

Look up a job by ID. Returns `None` when the job does not exist. `result_adapter` is optional
because a lookup by ID does not carry actor identity; when omitted it defaults to
`TypeAdapter(type(None))`, which is suitable for status-only lookups. Typical sources:

```python
# When you know the actor:
handle = await client.get(job_id, result_adapter=process_order.result_adapter)

# When you only need row metadata (e.g. status, timestamps):
from pydantic import TypeAdapter
handle = await client.get(job_id, result_adapter=TypeAdapter(type(None)))
```

---

## `cancel()`

```python
async def cancel(
    self,
    job_id: JobId,
    reason: str | None = None,
) -> CancelResult: ...
```

Requests cancellation of a job and returns a `CancelResult`.

**Semantics:**

1. Reads the current row. Raises `KeyError` if the job does not exist.
2. Calls `backend.write_cancel_request(job_id, reason)`.
3. Reads the row again to capture the new status.

`previous_status` reflects the row at step 1, not atomically at write time (TOCTOU).

### `CancelResult`

Frozen Pydantic model returned by `cancel()`.

| Field | Type | Description |
|---|---|---|
| `job_id` | `UUID` | The job that was targeted. |
| `previous_status` | `JobStatus` | Status at the first read (before the cancel write). |
| `new_status` | `JobStatus` | Status after the cancel write. |
| `cancellation_initiated` | `bool` | `True` when the cancel write transitioned the job to `"cancelled"`. `False` when the job was already in a terminal state and no transition occurred. |

```python
result = await client.cancel(handle.job_id, reason="user_requested")
if result.cancellation_initiated:
    print(f"job {result.job_id} cancelled (was {result.previous_status})")
else:
    print(f"job already terminal: {result.new_status}")
```

---

## `list()`

```python
async def list(self, filter: JobFilter) -> JobPage: ...
```

Lists jobs matching the filter. Returns a `JobPage`.

### `JobFilter`

Frozen dataclass. All fields are optional.

| Field | Type | Default | Description |
|---|---|---|---|
| `queue` | `str \| None` | `None` | Filter by queue name. |
| `status` | `JobStatus \| None` | `None` | Filter by current status. |
| `actor` | `str \| None` | `None` | Filter by actor name. |
| `identity_key` | `IdentityKey \| None` | `None` | Filter by identity key. |
| `batch_id` | `UUID \| None` | `None` | Filter by batch ID. |
| `tags` | `tuple[str, ...] \| None` | `None` | Filter by tags. Uses `&&` (array overlap) with a GIN index. Returns jobs that match any of the given tags. |
| `order_by` | `JobSortField \| None` | `None` | Sort order for results. `None` resolves to `JobSortField.SCHEDULED_AT_ASC` — see [JobSortField](#jobsortfield). |
| `limit` | `int` | `100` | Maximum number of rows to return. |
| `cursor` | `str \| None` | `None` | Opaque keyset-pagination token from `JobPage.next_cursor`. |

### `JobSortField`

Enum controlling the sort order of `list()` results. Import from
`taskq.backend._protocol`:

```python
from taskq.backend._protocol import JobSortField
```

| Member | Sort order | Use case |
|---|---|---|
| `JobSortField.SCHEDULED_AT_ASC` | Earliest `scheduled_at` first | Default (`None` resolves to this). FIFO / queue-depth inspection. |
| `JobSortField.CREATED_AT_DESC` | Latest `created_at` first | "Most recently enqueued" queries. |
| `JobSortField.FINISHED_AT_DESC` | Latest `finished_at` first | "Latest completed run" queries. Jobs with `finished_at IS NULL` sort last (`NULLS LAST`). |

!!! warning "Cursor pagination requires default ordering"
    Keyset cursor pagination (the `cursor` field) is only valid with the
    default `SCHEDULED_AT_ASC` ordering. Combining a non-default `order_by`
    with a `cursor` raises `ValueError` at `JobFilter.__post_init__` time.
    Use `limit` to cap result sets when sorting by `CREATED_AT_DESC` or
    `FINISHED_AT_DESC`.

### Querying the latest run by `identity_key`

Combine `identity_key` filtering with `FINISHED_AT_DESC` to find the most
recent completed run of a logical entity:

```python
from taskq.backend._protocol import JobFilter, JobSortField

page = await client.list(JobFilter(
    actor="sync_tenant",
    identity_key="tenant:acme",
    order_by=JobSortField.FINISHED_AT_DESC,
    limit=1,
))
if page.jobs:
    latest = page.jobs[0]
    print(f"last run: {latest.status} at {latest.finished_at}")
```

For "most recently enqueued" (regardless of completion), use
`CREATED_AT_DESC`:

```python
page = await client.list(JobFilter(
    actor="sync_tenant",
    identity_key="tenant:acme",
    order_by=JobSortField.CREATED_AT_DESC,
    limit=1,
))
```

### `JobPage`

Frozen dataclass.

| Field | Type | Description |
|---|---|---|
| `jobs` | `list[JobRow]` | The matched job rows. |
| `next_cursor` | `str \| None` | Pagination token for the next page. `None` when no more rows exist. |

```python
page = await client.list(JobFilter(queue="payments", status="pending", limit=50))
for job in page.jobs:
    print(job.id, job.actor, job.status)

if page.next_cursor:
    page2 = await client.list(JobFilter(queue="payments", limit=50, cursor=page.next_cursor))
```

---

## `SubJobEnqueuer`

`SubJobEnqueuer` is accessed as `ctx.jobs` inside an actor body. It is not instantiated directly
by application code. For actor-side usage see [Actor API — Sub-job enqueuing](actors.md#sub-job-enqueuing).

### Handle limitations

Handles returned by `ctx.jobs.enqueue()` do **not** have a client bound to them. Calling
`.status()`, `.refresh()`, `.attempts()`, or `.cancel()` on these handles raises `RuntimeError`.
`.wait()` works because it reads through the backend directly. To poll a sub-job's result from
outside the actor body, pass its `job_id` to a full `JobsClient` instance:

```python
sub_handle = await ctx.jobs.enqueue(process_item, ItemPayload(item_id=item_id))
sub_job_id = sub_handle.job_id  # safe — job_id is always available

# Later, from application code with a full client:
result_handle = await client.get(sub_job_id, result_adapter=process_item.result_adapter)
```

### `enqueue()`

```python
async def enqueue(
    self,
    actor_ref: ActorRef[P, R],
    payload: P,
    *,
    connection: asyncpg.Connection | None = None,
    scheduled_at: datetime | None = None,
    priority: int | None = None,
    fairness_key: str | None = None,
    metadata: dict[str, object] | None = None,
    identity_key: IdentityKey | None = None,
    idempotency_key: IdempotencyKey | str | None = None,
    unique_for: timedelta | None = None,
    unique_states: tuple[JobStatus, ...] | None = None,
    max_pending: int | None = None,
) -> JobHandle[R]: ...
```

Enqueues a single sub-job. Accepts the same options as `JobsClient.enqueue()` except:

- No `queue` override (sub-jobs use the actor's declared queue).
- No `schedule_to_close`, `start_to_close`, or `heartbeat_timeout` (set on the actor declaration).
- No explicit `trace_id` / `span_id` (extracted from the active OTel span).
- `connection` may be passed to use a specific `asyncpg.Connection` rather than the LOOP-scope
  connection.

### `enqueue_batch()`

```python
async def enqueue_batch(
    self,
    items: Sequence[EnqueueItem[Any, Any]],
    *,
    batch_id: UUID | None = None,
    connection: asyncpg.Connection | None = None,
) -> list[JobHandle[Any]]: ...
```

Enqueues multiple sub-jobs. Currently issues N sequential round-trips (single-statement batch
INSERT is a future enhancement). Each `EnqueueItem` carries `actor_ref`, `payload`, and the
per-job options (`scheduled_at`, `priority`, `fairness_key`, `metadata`).

All items share a single `batch_id` UUID written into each job's `metadata.batch_id` field. When
`batch_id` is omitted a UUIDv7 is auto-generated; pass it explicitly to correlate the sub-jobs
with a finalizer job enqueued separately.

```python
from taskq import EnqueueItem

await ctx.jobs.enqueue_batch([
    EnqueueItem(actor_ref=send_email, payload=EmailPayload(to="a@example.com")),
    EnqueueItem(actor_ref=send_email, payload=EmailPayload(to="b@example.com"), priority=1),
])
```

---

## Error handling

All exceptions are in `taskq.exceptions`. Import directly:

```python
from taskq.exceptions import (
    MaxPendingExceededError,
    SingletonCollisionError,
    PayloadValidationError,
    JobFailed,
    ResultUnavailable,
)
```

| Exception | Raised when |
|---|---|
| `MaxPendingExceededError` | `enqueue()` called when `pending + scheduled` count >= `max_pending`. Fields: `actor` (str), `current_count` (int), `max_pending` (int). |
| `SingletonCollisionError` | `enqueue()` called for a singleton actor that already has an active job. Fields: `actor` (str), `blocking_job_id` (UUID or None), `retry_after` (timedelta or None). |
| `PayloadValidationError` | Pydantic validation of the payload fails at enqueue time or at dispatch time. Non-retryable regardless of retry policy. Fields: `actor`, `payload_schema_ver`, `validation_errors`. |
| `JobFailed` | `JobHandle.wait()` observed a non-success terminal status. Field: `row` (JobRow) with `status`, `error_class`, `error_message`, `error_traceback`. |
| `ResultUnavailable` | `JobHandle.wait()` observed `"succeeded"` but no usable result is stored (TTL expired, `None` returned where `R` is non-`None`). Field: `row` (JobRow). |

```python
from taskq.exceptions import JobFailed, ResultUnavailable

try:
    result = await handle.wait(timeout=30.0)
except JobFailed as exc:
    print(f"job {exc.row.id} failed: {exc.row.error_class}: {exc.row.error_message}")
except ResultUnavailable as exc:
    print(f"job {exc.row.id} succeeded but result is gone (TTL?)")
except TimeoutError:
    print("job did not finish within 30 seconds")
```

---

## Full enqueue-and-wait example

```python
import asyncio
from pydantic import BaseModel
from taskq import TaskQ, actor
from taskq.exceptions import JobFailed, MaxPendingExceededError

class TranscribePayload(BaseModel):
    media_url: str
    language: str = "en"

class TranscribeResult(BaseModel):
    transcript: str
    confidence: float

@actor(queue="media", max_pending=500)
async def transcribe_audio(payload: TranscribePayload) -> TranscribeResult:
    # ... call transcription service ...
    return TranscribeResult(transcript="Hello world", confidence=0.98)

async def main() -> None:
    from taskq.settings import TaskQSettings
    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:

        try:
            handle = await tq.enqueue(
                transcribe_audio,
                TranscribePayload(media_url="https://media.example.com/clip.mp3"),
            )
        except MaxPendingExceededError as exc:
            print(f"queue full ({exc.current_count}/{exc.max_pending}), try later")
            return

        print(f"enqueued job {handle.job_id}, was_existing={handle.was_existing}")

        try:
            result: TranscribeResult = await handle.wait(timeout=120.0)
            print(f"transcript: {result.transcript} (confidence {result.confidence:.0%})")
        except JobFailed as exc:
            print(f"failed: {exc.row.error_class}: {exc.row.error_message}")
        except TimeoutError:
            print("timed out waiting for transcript")

asyncio.run(main())
```

---

## Idempotency example

Use `idempotency_key` to prevent duplicate execution when a caller may retry the enqueue call
(e.g. after a network error):

```python
async def send_order_confirmation(client: JobsClient, order_id: str) -> str:
    """Enqueue a confirmation email, safe to call multiple times for the same order."""
    handle = await client.enqueue(
        send_confirmation_email,
        ConfirmationPayload(order_id=order_id),
        # Namespace the key to avoid collisions with other actors.
        idempotency_key=f"send_confirmation_email:{order_id}",
    )
    if handle.was_existing:
        print(f"order {order_id}: email already enqueued, returning existing job")
    return str(handle.job_id)
```

**Rules:**

- The key is globally unique across all actors. Always namespace it: `"actor_name:entity_id"`.
- Maximum 256 characters. Empty and whitespace-only keys raise `ValueError`.
- A duplicate key returns a handle with `was_existing=True` pointing at the original job.
- `idempotency_key` does not bypass `max_pending` on the **first** call for a given key. If the
  queue is full when the key is first used, `MaxPendingExceededError` is raised and no row is
  inserted. On subsequent calls with the same key (after the key was successfully inserted),
  the existing handle is returned without checking `max_pending`. Only `unique_for` (evaluated at
  step 2, before `max_pending`) bypasses the queue-depth check unconditionally.

---

## Batch enqueue example

Enqueue a fan-out of notifications and poll until the whole batch is done:

```python
import asyncio
import asyncpg
from pydantic import BaseModel
from taskq import TaskQ, actor, EnqueueItem
from taskq.exceptions import MaxPendingExceededError

class NotifyPayload(BaseModel):
    user_id: str
    message: str

@actor(queue="notifications", max_pending=10_000)
async def send_notification(payload: NotifyPayload) -> None:
    # ... deliver notification ...
    pass

async def notify_users(tq: TaskQ, user_ids: list[str], message: str) -> None:
    items = [
        EnqueueItem(
            actor_ref=send_notification,
            payload=NotifyPayload(user_id=uid, message=message),
            idempotency_key=f"send_notification:{uid}:{message[:32]}",
        )
        for uid in user_ids
    ]

    try:
        batch = await tq.enqueue_batch(items)
    except MaxPendingExceededError as exc:
        print(f"notification queue full ({exc.current_count}/{exc.max_pending})")
        return

    print(f"enqueued {batch.size} notifications, batch_id={batch.batch_id}")

    # Poll until complete (replace with your preferred polling strategy)
    pool = await asyncpg.create_pool(dsn="postgresql://user:pass@localhost/taskq")
    async with pool.acquire() as conn:
        while True:
            status = await batch.status(conn)
            if status.is_complete:
                break
            print(f"  {status.pending}/{status.total} pending…")
            await asyncio.sleep(2.0)

    print(f"batch done — {status.succeeded} ok, {status.failed} failed")
```

---

## Tags

Tags are user-defined keyword labels stored in `jobs.tags text[]`. They have no functional behaviour — no routing, no priority, no lifecycle side effects — and are meant entirely as a user-specified construct for grouping, filtering, and categorizing jobs.

### Adding tags at enqueue

```python
handle = await client.enqueue(
    send_email,
    EmailPayload(to="user@example.com"),
    tags=["notification", "priority:high", "tenant:acme"],
)
```

Tags can also be set per-item in batch enqueues:

```python
items = [
    EnqueueItem(actor_ref=process, payload=p, tags=["batch-abc", "chunk-1"]),
    EnqueueItem(actor_ref=process, payload=q, tags=["batch-abc", "chunk-2"]),
]
await client.enqueue_batch(items)
```

### Tag validation

Tags must match `^[\w][\w\-]+[\w]$`:
- At least 3 characters
- Starts and ends with a word character (`[a-zA-Z0-9_]`)
- Middle can contain word characters or hyphens
- Maximum 255 characters per tag
- Duplicates are silently removed (first-occurrence order preserved)
- Empty or invalid tags raise `ValueError` at enqueue time

### Filtering by tags

Use `JobFilter.tags` with array-overlap semantics (matches jobs that have **any** of the given tags):

```python
page = await client.list(JobFilter(
    actor="send_email",
    status="failed",
    tags=["priority:high", "tenant:acme"],
    limit=50,
))
```

SQL: `WHERE tags && $n::text[]` backed by a GIN index. Tags filtering works in the admin UI via `?tags=comma,separated,values`.

