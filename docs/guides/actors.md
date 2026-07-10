# Actor API

An **actor** is an async handler registered with the worker. The `@actor` decorator introspects the
handler signature, validates it at decoration time, and returns an `ActorRef[P, R]` that carries
the actor's payload and result types end-to-end.

---

## Contents

1. [The `@actor` decorator](#the-actor-decorator)
2. [Handler signatures](#handler-signatures)
3. [Sync actors](#sync-actors)
4. [`ActorRef[P, R]`](#actorrefp-r)
5. [`JobContext[P]`](#jobcontextp)
6. [Payload and result types](#payload-and-result-types)
7. [Singleton actors](#singleton-actors)
8. [`unique_for` deduplication](#unique_for-deduplication)
9. [`max_pending` backpressure](#max_pending-backpressure)
10. [Rate limits and reservations](#rate-limits-and-reservations)
11. [Retry policy](#retry-policy)
12. [Control-flow exceptions](#control-flow-exceptions)
13. [Dependency injection](#dependency-injection)
14. [Sub-job enqueuing](#sub-job-enqueuing)
15. [Progress reporting](#progress-reporting)
16. [Testing actors without a database](#testing-actors-without-a-database)
17. [Full worked example](#full-worked-example)

---

## The `@actor` decorator

Supports both plain and parameterised forms:

```python
# Plain — all options take their defaults.
@actor
async def send_email(payload: EmailPayload) -> EmailResult: ...

# Parameterised.
@actor(queue="priority", max_concurrent=10, retry=RetryPolicy(max_attempts=5))
async def process_order(payload: OrderPayload) -> OrderResult: ...
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str \| None` | `fn.__qualname__` | Actor name stored in the `actor_config` table and on every job row. Override when the qualified name would be unstable across refactors. |
| `queue` | `str` | `"default"` | Queue this actor is dispatched on. Must match `[A-Za-z_][A-Za-z0-9_.-]*`. Can be overridden per-enqueue. |
| `retry` | `RetryPolicy \| None` | `RetryPolicy()` | Retry policy — see [Retry policy](#retry-policy). `None` resolves to the default `RetryPolicy()`. |
| `result_ttl` | `timedelta \| None` | `None` | How long the result JSONB is retained after a job succeeds. `None` means retain indefinitely. |
| `singleton` | `bool` | `False` | Enforce at most one active job of this actor fleet-wide — see [Singleton actors](#singleton-actors). |
| `max_concurrent` | `int \| None` | `None` | Fleet-wide concurrency cap. `None` = unbounded. `0` = drain mode (no jobs dispatched). May transiently exceed the configured value by up to `(num_active_producers - 1) * max_concurrent` under contention; use a `ConcurrencyReservation` for strict enforcement. |
| `max_pending` | `int \| None` | `None` | Queue-depth backpressure cap — see [`max_pending` backpressure](#max_pending-backpressure). |
| `metadata` | `dict[str, object] \| None` | `{}` | Arbitrary key-value metadata stored in `actor_config.metadata` (JSONB). Must be a plain `dict`; mapping proxies and frozendicts are rejected at decoration time. The key `"singleton"` is reserved by the library. |
| `unique_for` | `timedelta \| None` | `None` | Deduplication window — see [`unique_for` deduplication](#unique_for-deduplication). |
| `unique_states` | `tuple[JobStatus, ...]` | `("pending", "scheduled", "running")` | Job statuses considered "active" for `unique_for` dedup. Terminal states are excluded by default so a completed job does not block re-enqueue. |
| `start_to_close` | `timedelta \| None` | `None` | Per-attempt execution timeout. Precedence (first wins): per-enqueue `start_to_close` > this actor default > `TASKQ_DEFAULT_START_TO_CLOSE`. `None` means no per-attempt timeout unless a worker-wide default is set. See [Retries — `start_to_close` vs `schedule_to_close`](retries.md#7-start_to_close-vs-schedule_to_close). |
| `rate_limits` | `list[str] \| None` | `[]` | Named rate-limit buckets this actor consumes — see [Rate limits and reservations](#rate-limits-and-reservations). |
| `reservations` | `list[str \| KeyedReservationRef] \| None` | `[]` | Named concurrency reservation slots this actor claims. A `KeyedReservationRef` derives per-key (session/tenant) reservation buckets from the job payload at dispatch time — see [Rate limits and reservations](#rate-limits-and-reservations). |
| `non_retryable_exceptions` | `tuple[type[BaseException], ...]` | `()` | Exception types that fail the job immediately instead of retrying. |
| `retry_classifier` | `RetryClassifierHook \| None` | `None` | Hook for exception-instance-level retry classification. Invoked with `(exception, attempt)` for exceptions that survive `non_retryable_exceptions`/`PayloadValidationError` checks; return `RetryOverride` to refine `kind`/`delay` per occurrence or `None` to fall back to the static `RetryPolicy`. See [Retries — `retry_classifier` hook](retries.md#5-retry_classifier-hook--per-instance-retry-overrides). |
| `on_retry_exhausted` | `OnRetryExhausted \| None` | `None` | Callback invoked when the retry budget is exhausted, before the job is marked `failed`. |
| `on_retry_exhausted_timeout` | `float` | `3.0` | Seconds allowed for `on_retry_exhausted` to complete before it is abandoned. |
| `on_success` | `OnSuccess \| None` | `None` | Callback invoked when the job succeeds, after the transaction commits. Receives `(job_row, result)`. Mirrors `on_retry_exhausted` with a timeout guard — see [Retries — `on_success` hook](retries.md#on_success-hook). |
| `on_success_timeout` | `float` | `3.0` | Seconds allowed for `on_success` to complete before it is abandoned. |
| `priority` | `int` | `0` | Default dispatch priority for jobs enqueued without an explicit `priority=`. Must fit `smallint` range (-32768..32767). |

### Decoration-time validation

The decorator raises at import time (not at runtime) when:

- The payload parameter lacks a type annotation.
- The return annotation is missing.
- The payload annotation is not a `pydantic.BaseModel` subclass.
- A `JobContext` parameter's payload type does not match the handler's payload type.
- An unannotated DI parameter is declared.
- `max_concurrent` is a negative integer.
- `max_pending` is a negative integer.
- `metadata` is not a plain `dict` or `None`.
- `singleton` is not a `bool`.

Both `async def` and plain `def` functions are accepted. Sync functions are dispatched via `asyncio.to_thread()` and the actor's `is_sync` property is `True`.

---

## Handler signatures

The decorator accepts any of these four shapes. Declare only what the handler body needs.

### Payload only

```python
from pydantic import BaseModel
from taskq import actor

class ResizePayload(BaseModel):
    image_id: str
    width: int
    height: int

class ResizeResult(BaseModel):
    url: str

@actor
async def resize_image(payload: ResizePayload) -> ResizeResult:
    # No ctx, no DI deps injected.
    return ResizeResult(url=f"https://cdn.example.com/{payload.image_id}")
```

### Payload and context

```python
from taskq import actor
from taskq.context import JobContext

@actor
async def resize_image(payload: ResizePayload, ctx: JobContext[ResizePayload]) -> ResizeResult:
    if ctx.cancellation_requested:
        raise Snooze(timedelta(minutes=1))
    return ResizeResult(url=f"https://cdn.example.com/{payload.image_id}")
```

### Payload and DI dependencies

```python
from taskq import actor

@actor
async def resize_image(
    payload: ResizePayload,
    *,
    db: DbSession,
    http: HttpClient,
) -> ResizeResult:
    row = await db.fetch_one("SELECT * FROM images WHERE id = $1", payload.image_id)
    ...
```

DI parameters must be keyword-only (`*` separator) or positional — the worker always passes them as
keyword arguments. Every DI parameter must have a type annotation that is a concrete class; the
worker's DI resolver maps the annotation to a registered provider at dispatch time.

### Payload, context, and DI dependencies

```python
from taskq import actor
from taskq.context import JobContext

@actor(queue="priority")
async def resize_image(
    payload: ResizePayload,
    ctx: JobContext[ResizePayload],
    *,
    db: DbSession,
) -> ResizeResult:
    ctx.log.info("starting", attempt=ctx.attempt)
    ...
```

---

## Sync actors

`@actor` also accepts plain `def` functions. Sync actors run in a thread via `asyncio.to_thread()` — the event loop is never blocked.

```python
from pydantic import BaseModel
from taskq import actor

class PdfPayload(BaseModel):
    html: str
    filename: str

class PdfResult(BaseModel):
    s3_key: str

@actor(queue="media")
def generate_pdf(payload: PdfPayload) -> PdfResult:
    # CPU-bound PDF generation — runs in a thread, not the event loop.
    import weasyprint
    out = weasyprint.HTML(string=payload.html).write_pdf()
    # ... upload to S3 ...
    return PdfResult(s3_key=f"pdfs/{payload.filename}")
```

### Cancellation for sync actors

Sync actors cannot be force-cancelled via `asyncio.Task.cancel()`. They must cooperate by polling `ctx.should_abort()`:

```python
@actor
def long_loop(payload: BigPayload, ctx: JobContext[BigPayload]) -> None:
    for item in payload.items:
        if ctx.should_abort():
            return  # cooperative exit; job will be marked cancelled
        process(item)
```

- **Phase 1 (COOPERATIVE):** `ctx.should_abort()` returns `True`. The actor should return or raise.
- **Phase 2 (FORCED):** The cancel controller writes `cancel_phase=2` to PG but **cannot** interrupt the thread. The sync actor continues until it polls `should_abort()` or hits `start_to_close` timeout.
- **Phase 3 (ABANDON):** If the actor never polls, the job is abandoned after `cancel_grace + cleanup_grace`.

### DI thread safety

DI resolution happens in the event loop **before** the sync function is dispatched to the thread. Resolved kwargs are passed through:

| Scope | Safe in sync actor? | Notes |
|-------|---------------------|-------|
| PROCESS | Depends on object | Shared across event loop and thread |
| LOOP | **NOT safe** | `asyncpg.Connection`, `redis.asyncio.Redis` are not thread-safe |
| TRANSIENT | Safe if object is thread-safe | Fresh per invocation |

The worker logs a WARNING when a sync actor declares a LOOP-scoped dependency parameter. For thread-safe database access, register a sync driver connection at `Scope.THREAD` or use `Scope.TRANSIENT`.

### Rate limiting and sub-jobs

Rate-limit acquisition/release runs in the event loop before/after `asyncio.to_thread()`. No changes needed.

Sub-job enqueues from a sync actor use the autonomous commit path (acquires a fresh pool connection). The transactional LOOP-scope connection path is unavailable from threads.

---

## `ActorRef[P, R]`

`ActorRef[P, R]` is the object returned by `@actor`. It is not a callable that enqueues jobs —
pass it to [`JobsClient.enqueue`](jobs-clients.md#enqueue) for that. Direct in-process invocation
(`await my_actor(payload, ...)`) runs the handler without going through the queue and is intended
for tests and simulators.

### Properties

| Property | Type | Description |
|---|---|---|
| `name` | `str` | Actor name (from `name=` or `fn.__qualname__`). |
| `queue` | `str` | Default queue. |
| `payload_type` | `type[P]` | The Pydantic model class for `P`. Used to validate raw payloads at dispatch time. |
| `result_adapter` | `TypeAdapter[R]` | Pydantic `TypeAdapter` for `R`. Serialises the result to JSONB on the worker side and deserialises it in `JobHandle.wait()`. |
| `retry` | `RetryPolicy` | The actor's retry policy. |
| `result_ttl` | `timedelta \| None` | Result retention window. |
| `singleton` | `bool` | Whether singleton enforcement is active. |
| `max_concurrent` | `int \| None` | Fleet-wide concurrency cap. |
| `max_pending` | `int \| None` | Queue-depth backpressure cap. |
| `metadata` | `dict[str, object]` | Actor-level metadata. |
| `unique_for` | `timedelta \| None` | Deduplication window. |
| `unique_states` | `tuple[JobStatus, ...]` | Active statuses for dedup. |
| `rate_limits` | `list[str]` | Rate-limit bucket names. |
| `reservations` | `list[str]` | Concurrency reservation names. |
| `wants_ctx` | `bool` | Whether the handler declared a `JobContext` parameter. |
| `is_sync` | `bool` | `True` when the handler is a plain `def` (not `async def`). Sync actors run via `asyncio.to_thread()`. |
| `dependencies` | `dict[str, type[object]]` | DI parameter names mapped to their annotated types. |
| `fn` | `Callable[..., object]` | The underlying handler (sync or async). Prefer `__call__` for invocation. |

### Direct invocation (`__call__`)

```python
# Handler with no ctx:
result = await resize_image(ResizePayload(image_id="abc", width=800, height=600))

# Handler with ctx (supply a real or stub JobContext):
result = await resize_image(payload, ctx, db=db_stub)
```

Passing `ctx` to a no-ctx handler raises `TypeError`. Omitting `ctx` for a ctx handler also raises
`TypeError`. Missing DI dependencies surface as `TypeError` from Python's argument binding.

---

## `JobContext[P]`

`JobContext[P]` is a frozen dataclass constructed per attempt by the worker. Handlers receive it as
the second positional parameter when they declare `ctx: JobContext[YourPayload]`.

### Fields

| Field | Type | Description |
|---|---|---|
| `job_id` | `UUID` | The job's unique identifier. |
| `actor` | `str` | Actor name. |
| `queue` | `str` | Queue the job is running on. |
| `attempt` | `int` | Current attempt number (1-indexed). |
| `worker_id` | `UUID` | The worker running this attempt. |
| `payload` | `P` | Fully-validated payload instance. Typed as `P` — no cast required. |
| `jobs` | `SubJobEnqueuer` | Enqueue sub-jobs from within the actor body — see [Sub-job enqueuing](#sub-job-enqueuing). |
| `log` | `structlog.BoundLogger` | Structured logger pre-bound with `job_id`, `actor`, and `attempt`. |
| `span` | `opentelemetry.trace.Span \| None` | Active OTel span for this attempt. `None` when tracing is disabled. |
| `cancel_event` | `asyncio.Event` | Set by the cancel-poll hook when the job enters cooperative cancellation. |
| `progress(...)` | `async method` | Report incremental progress for this job — see [Progress reporting](#progress-reporting). |
| `cancellation_requested` | `bool` (property) | Returns `True` when `cancel_event` is set. |
| `check_cancelled()` | `method → None` | Raises `asyncio.CancelledError` if `cancel_event` is set. Convenience for cooperative exit inside actor loops. |
| `should_abort()` | `method → bool` | **Sync-only.** Thread-safe cooperative cancellation check. Returns `True` when cancellation has been requested. Sync actors must poll this; they cannot `await cancel_event.wait()` from a thread. |

### `cancellation_requested` property

```python
@property
def cancellation_requested(self) -> bool: ...
```

Returns `True` when `cancel_event` is set, meaning a cancellation request has reached phase 1
(cooperative). Poll this in long-running loops to exit cleanly:

```python
@actor
async def long_job(payload: LongPayload, ctx: JobContext[LongPayload]) -> None:
    for item in payload.items:
        if ctx.cancellation_requested:
            raise Snooze(timedelta(minutes=5))
        await process(item)
```

Alternatively, `await ctx.cancel_event.wait()` blocks until cancellation is requested.

---

## Payload and result types

Both `P` and `R` must be `pydantic.BaseModel` subclasses. `R` may additionally be `None` for
fire-and-forget actors.

```python
from pydantic import BaseModel

class OrderPayload(BaseModel):
    order_id: str
    items: list[str]
    total_cents: int

class OrderResult(BaseModel):
    confirmation_number: str
    estimated_delivery: str

@actor
async def process_order(payload: OrderPayload) -> OrderResult:
    ...

# Fire-and-forget: R = None
@actor
async def audit_log(payload: AuditPayload) -> None:
    ...
```

Plain `dict`, `dataclass`, and `TypedDict` are not supported as payload or result types. Pydantic
v2 models are required for JSONB round-trip serialisation.

---

## Singleton actors

`singleton=True` enforces at most one active job of this actor fleet-wide across all queues and
workers.

```python
@actor(singleton=True)
async def daily_report(payload: ReportPayload) -> None:
    ...
```

**Semantics:**

- "Active" means `status IN ('pending', 'scheduled', 'running')`. A snoozed singleton job in
  `scheduled` state blocks new enqueues until it terminates.
- Singleton enforcement is **actor-scoped**, not identity-scoped. Different `identity_key` values
  for the same singleton actor are still blocked.
- For per-identity singleton semantics (one active job per user, not per actor), use
  `max_concurrent=1` with an `identity_key` instead.
- The library injects `metadata["singleton"] = True` on every enqueue. Callers must not set
  this key manually — the library unconditionally overwrites it.
- On collision, [`SingletonCollisionError`](jobs-clients.md#error-handling) is raised.

```python
from taskq.exceptions import SingletonCollisionError

try:
    handle = await client.enqueue(daily_report, ReportPayload(date="2025-01-01"))
except SingletonCollisionError as exc:
    print(f"blocked by job {exc.blocking_job_id}, retry_after={exc.retry_after}")
```

`SingletonCollisionError.blocking_job_id` is the UUID of the existing job from the pre-flight
query, or `None` on the race path (Layer 2 unique constraint catch). `retry_after` is derived from
the blocking job's `schedule_to_close` when available, otherwise `None`.

---

## `unique_for` deduplication

`unique_for` deduplicates enqueues for the same `(actor, identity_key)` within a sliding window.

```python
from datetime import timedelta

@actor(
    unique_for=timedelta(minutes=15),
    unique_states=("pending", "scheduled", "running"),
)
async def sync_account(payload: SyncPayload) -> None:
    ...
```

**Semantics:**

- `unique_for` only has effect when `identity_key` is also provided at enqueue time. If
  `identity_key` is omitted, `unique_for` is a **silent no-op** — the library logs a warning with
  event name `actor-config-unique-for-ignored` and creates a fresh job every time. This is a common
  footgun: configure `unique_for` on the actor but forget to pass `identity_key` at the call site.
- Deduplication is **best-effort** — concurrent enqueues for the same `(actor, identity_key)` may
  both insert. The dispatch CTE's `running_identities` filter ensures only one runs.
- When a dedup match is found, `JobHandle.was_existing` is `True` and the handle wraps the
  existing job row.
- `unique_states` controls which statuses count as "active" for the window check. Terminal states
  (`succeeded`, `failed`, `cancelled`) are excluded from the default so a finished job does not
  block re-enqueue.

```python
handle = await client.enqueue(
    sync_account,
    SyncPayload(account_id="acct_123"),
    identity_key="acct_123",  # required — unique_for is a no-op without this
)
if handle.was_existing:
    print("deduped — returning existing job handle")
```

The `identity_key` and `unique_for` window can be overridden per-enqueue via
[`JobsClient.enqueue`](jobs-clients.md#enqueue).

---

## `max_pending` backpressure

`max_pending` limits the number of `pending` + `scheduled` jobs before `enqueue` rejects.

```python
@actor(max_pending=1000)
async def ingest_event(payload: EventPayload) -> None:
    ...
```

**Semantics:**

- `None` (default) means unbounded — `enqueue` never rejects on capacity.
- `max_pending=0` means no jobs are ever accepted (every enqueue raises immediately).
- Negative values raise `ValueError` at decoration time.
- When the limit is reached, `MaxPendingExceededError` is raised synchronously. The caller decides
  whether to retry, back off, or drop.

```python
from taskq.exceptions import MaxPendingExceededError

try:
    handle = await client.enqueue(ingest_event, EventPayload(data=raw))
except MaxPendingExceededError as exc:
    print(f"queue full: {exc.current_count}/{exc.max_pending} pending for {exc.actor}")
```

**Evaluation order at enqueue:** `unique_for` dedup → singleton pre-flight →
`max_pending` count check → `idempotency_key` upsert → job INSERT. A `unique_for` hit
bypasses all remaining checks. A singleton collision fires before `max_pending`.

---

## Rate limits and reservations

Declare named rate-limit buckets and concurrency reservation slots on the actor:

```python
@actor(
    rate_limits=["openai", "vendor_x"],
    reservations=["gpu_pool"],
)
async def run_inference(payload: InferencePayload) -> InferenceResult:
    ...
```

`rate_limits` and `reservations` are lists of bucket/slot names defined in the rate-limiting
configuration. For bucket and slot configuration syntax see [Rate Limiting](rate-limiting.md).

---

## Retry policy

Pass a `RetryPolicy` to control how the worker retries failed jobs.

```python
from datetime import timedelta
from taskq import actor
from taskq.retry import RetryPolicy

@actor(
    retry=RetryPolicy(
        kind="transient",       # "transient" | "indefinite" | "non_retryable"
        max_attempts=5,         # ignored for kind="indefinite"
        backoff="exponential",  # "exponential" | "linear" | "fixed"
        base=timedelta(seconds=10),
        cap=timedelta(hours=2),
        jitter=0.2,
        time_budget=None,       # only used for kind="indefinite"
    )
)
async def flaky_call(payload: CallPayload) -> CallResult:
    ...
```

### `RetryPolicy` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `kind` | `"transient" \| "indefinite" \| "non_retryable"` | `"transient"` | Retry tier. `"transient"` retries up to `max_attempts`. `"indefinite"` retries until `time_budget` or `schedule_to_close` elapses. `"non_retryable"` never retries. |
| `max_attempts` | `int` | `3` | Maximum attempts. Must be >= 1. Used only when `kind="transient"`. |
| `time_budget` | `timedelta \| None` | `None` | Total wall-clock budget. Used only when `kind="indefinite"`. |
| `backoff` | `"exponential" \| "linear" \| "fixed"` | `"exponential"` | Backoff shape. |
| `base` | `timedelta` | `5s` | Base delay for backoff computation. |
| `cap` | `timedelta` | `1h` | Maximum per-attempt delay (must be >= `base`). |
| `jitter` | `float` | `0.2` | Multiplicative jitter factor in `[0.0, 1.0]`. Applies symmetric jitter: `delay * uniform(1-jitter, 1+jitter)`. |

For retry internals (decision logic, backoff formula, `on_retry_exhausted` hook) see
[Worker](workers.md).

---

## Control-flow exceptions

These exceptions are signals, not errors. Raise them from an actor body to drive state transitions
without consuming retry budget (for `Snooze`) or while consuming it (for `RetryAfter`).

### `Snooze`

Reschedules the job at `now + delay` without consuming retry budget. The job re-enters
`scheduled` state.

```python
from datetime import timedelta
from taskq.exceptions import Snooze

@actor
async def poll_external_api(payload: PollPayload, ctx: JobContext[PollPayload]) -> None:
    result = await check_status(payload.task_id)
    if result.status == "pending":
        raise Snooze(timedelta(minutes=2))
    # ... handle completion
```

`Snooze(delay)` raises `ValueError` if `delay < timedelta(0)`.

A snoozed singleton actor re-enters `scheduled` state and blocks new enqueues until it either
succeeds, fails, or is cancelled.

### `RetryAfter`

Schedules a retry at a specific delay. Consumes the retry budget by default.

```python
from datetime import timedelta
from taskq.exceptions import RetryAfter

@actor
async def call_rate_limited_api(payload: ApiPayload) -> ApiResult:
    response = await api_client.call(payload.endpoint)
    if response.status == 429:
        retry_in = timedelta(seconds=int(response.headers.get("Retry-After", 60)))
        raise RetryAfter(retry_in)
    return ApiResult(data=response.json())
```

`RetryAfter(delay, consume_budget=False)` reschedules without counting the attempt against
`max_attempts`. `consume_budget=True` is the default.

`RetryAfter(delay)` raises `ValueError` if `delay < timedelta(0)`.

---

## Dependency injection

Actors declare DI dependencies as keyword-only parameters. The worker resolves them from a
`ProviderRegistry` at dispatch time.

### Registering providers

Create a `ProviderRegistry`, register your providers, validate it, then pass it to the worker. See
[Dependency Injection](dependency-injection.md) for the full
wiring. The three registration methods are:

```python
from taskq.di import ProviderRegistry, Scope

registry = ProviderRegistry()

# Register a pre-built singleton value (PROCESS scope — lives for the
# duration of the worker process).
registry.register_value(Database, Scope.PROCESS, db_instance)

# Register an async factory. The factory is called once per scope lifetime.
# Use an async generator to run teardown code.
async def create_http_client():
    client = HttpClient()
    try:
        yield client
    finally:
        await client.aclose()

registry.register_factory(HttpClient, Scope.LOOP, create_http_client)

# Register a class; the DI engine instantiates it and detects lifecycle
# methods (aclose, close) automatically.
registry.register_class(MyService, Scope.LOOP)

# Validate and seal before starting the worker.
registry.validate(actors=[my_actor])
```

After `validate()` the registry is sealed; further registrations raise `RuntimeError`.

### Scope lifetimes

| Scope | Lifetime | Typical use |
|---|---|---|
| `Scope.PROCESS` | Worker process start to exit | Config, shared read-only singletons |
| `Scope.LOOP` | Event loop start to loop close | asyncpg pools, HTTP clients, Redis clients |
| `Scope.TRANSIENT` | Per actor invocation | Per-request helpers, one-shot contexts |

A provider may depend only on providers of the same or wider scope. Violations raise
`ScopeViolation` at `validate()` time.

### Declaring dependencies in an actor

```python
@actor
async def send_email(
    payload: SendEmailPayload,
    *,
    db: Database,
    http: HttpClient,
) -> None:
    record = await db.get(payload.recipient_id)
    await http.post("/send", json={"to": record.email})
```

`payload` and `ctx` are always supplied by the consumer and must not be registered as providers.
All other annotated keyword parameters are resolved from the registry. Missing providers raise
`MissingProvider` at `validate()` time, not at runtime.

---

## Sub-job enqueuing

Enqueue sub-jobs from within an actor body via `ctx.jobs`, which is a `SubJobEnqueuer`.

```python
@actor
async def process_batch(payload: BatchPayload, ctx: JobContext[BatchPayload]) -> None:
    for item_id in payload.item_ids:
        await ctx.jobs.enqueue(
            process_item,
            ItemPayload(item_id=item_id),
            priority=1,
        )
```

See the `SubJobEnqueuer` reference in [Client API — SubJobEnqueuer](jobs-clients.md#subjobenqueuer).

### Transaction semantics

Sub-job enqueues use the **LOOP-scope `asyncpg.Connection`** by default. This connection is the
same one the worker holds open for the parent job's transaction. The consequence is that sub-job
INSERTs are part of the parent's database transaction:

- If the parent actor **succeeds**, the transaction commits and the sub-jobs become visible.
- If the parent actor **raises an exception** (and will be retried or failed), the transaction
  rolls back and the sub-jobs vanish atomically — they are never seen by the queue.

This is the correct default for fan-out patterns where sub-jobs should only exist if the parent
completes successfully.

**Autonomous fallback.** If no LOOP-scope `asyncpg.Connection` is registered in the DI
container, `ctx.jobs.enqueue()` falls back to the worker pool and commits each INSERT
independently. In this mode, sub-jobs are persisted even if the parent subsequently raises
an exception. The worker emits a `sub_enqueue_autonomous_fallback` warning to structlog every
100 autonomous enqueues to alert you that transactional guarantees are not in effect.

To ensure the transactional path is active, register an `asyncpg.Connection` at `Scope.LOOP`
in the DI registry (see [Dependency Injection](dependency-injection.md)).

### Handle limitations

Handles returned by `ctx.jobs.enqueue()` do **not** have a client bound to them. Calling
`.status()`, `.refresh()`, `.attempts()`, or `.cancel()` on these handles raises `RuntimeError`.
`.wait()` works because it reads through the backend directly. To poll a sub-job's result from
outside the actor body, pass its `job_id` to a full `JobsClient` instance:

```python
sub_handle = await ctx.jobs.enqueue(process_item, ItemPayload(item_id=item_id))
job_id = sub_handle.job_id  # safe — job_id is always available

# Later, from application code with a full client:
result_handle = await client.get(job_id, result_adapter=process_item.result_adapter)
```

---

## Progress reporting

Actors can emit structured progress updates that are buffered in memory, published to Redis in real
time, and periodically flushed to Postgres. Callers can subscribe to these events via
`JobHandle.progress_stream()` or the HTTP SSE endpoint — see [Progress & Streaming](progress.md).

### `ctx.progress()`

```python
async def progress(
    self,
    *,
    step: int | None = None,
    percent: float | None = None,
    detail: str | None = None,
    data: dict[str, object] | None = None,
) -> None: ...
```

All arguments are optional. Each call merges the supplied fields into the accumulated
`pending_state` using last-writer-wins semantics. Fields not supplied in a call are left
unchanged.

| Parameter | Type | Description |
|---|---|---|
| `step` | `int \| None` | Incremental step counter (e.g. items processed). |
| `percent` | `float \| None` | Completion percentage in `[0.0, 100.0]`. |
| `detail` | `str \| None` | Human-readable status message. |
| `data` | `dict[str, object] \| None` | Arbitrary structured data. Must serialise to JSON. |

**Coalescing.** Multiple `ctx.progress()` calls between periodic flush ticks are coalesced:
only the latest value for each field is written to Postgres. Real-time Redis events are still
emitted for every call. This means consumers that subscribe via SSE see fine-grained updates while
Postgres retains only the most recent snapshot.

**Sequence numbers.** Each call increments a strictly monotone `seq` counter. SSE consumers use
`seq` to detect duplicate or out-of-order delivery and to resume after reconnecting via
`Last-Event-ID`.

**`ProgressTooLarge`.** Raises `taskq.exceptions.ProgressTooLarge` if the serialised `data`
payload exceeds `WorkerSettings.progress_data_max_bytes`. Keep `data` small; use `detail` for
human-readable strings and `data` only for structured metadata.

```python
@actor(queue="media")
async def transcode_video(payload: TranscodePayload, ctx: JobContext[TranscodePayload]) -> None:
    segments = await split_into_segments(payload.url)
    total = len(segments)

    for i, segment in enumerate(segments):
        await transcode_segment(segment)
        await ctx.progress(
            step=i + 1,
            percent=round((i + 1) / total * 100, 1),
            detail=f"Transcoded segment {i + 1}/{total}",
        )

    await ctx.progress(percent=100.0, detail="Done")
```

**No-op without Redis.** When the worker is running without Redis (no `redis` extra installed or
`TASKQ_REDIS_URL` not set), `ctx.progress()` silently returns without publishing. Progress state
is still flushed to Postgres at the end of the job.

---

## Testing actors without a database

Use `InMemoryBackend` and `FakeClock` to test actor behaviour in unit tests without Postgres.
`InMemoryBackend` simulates the full enqueue-dispatch-execute cycle including `unique_for` dedup,
singleton enforcement, and `max_pending` backpressure.

```python
import pytest
from datetime import datetime, timezone
from pydantic import BaseModel
from taskq import actor
from taskq.client import JobsClient
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.clock import FakeClock


class MyPayload(BaseModel):
    value: int


class MyResult(BaseModel):
    doubled: int


@actor
async def double_value(payload: MyPayload) -> MyResult:
    return MyResult(doubled=payload.value * 2)


async def test_double_value():
    clock = FakeClock(start=datetime.now(timezone.utc))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    # Register a stub so run_until_drained knows how to execute the actor.
    backend.register_stub(
        double_value.name,
        lambda payload, ctx: {"doubled": payload["value"] * 2},
    )

    handle = await client.enqueue(double_value, MyPayload(value=21))
    await backend.run_until_drained()
    result = await handle.wait()
    assert result.doubled == 42
```

The stub receives `(payload: dict, ctx)` where `ctx` is a minimal duck-typed object with
`job_id`, `attempt`, `payload`, and `cancel_event`. For direct in-process invocation without the
stub mechanism:

```python
async def test_actor_direct():
    result = await double_value(MyPayload(value=21))
    assert result.doubled == 42
```

Direct invocation bypasses the queue entirely and is the simplest option when the actor has no DI
dependencies and you do not need to test enqueue/dispatch behaviour.

**`JobsClient` lifecycle.** `JobsClient` is lightweight — it performs no I/O at construction. Create
one instance per application and share it for the lifetime of the process. The connection pool is
owned by the `Backend`, not the client. Creating a `JobsClient` per-request adds unnecessary
overhead and does not provide isolation benefits.

---

## Full worked example

An actor that ties together payload, result, context, a DI dependency, retry, and `unique_for`:

```python
from datetime import timedelta
from pydantic import BaseModel
from taskq import actor
from taskq.context import JobContext
from taskq.exceptions import RetryAfter, Snooze
from taskq.retry import RetryPolicy

# --- Models ---

class OrderPayload(BaseModel):
    order_id: str
    customer_id: str
    amount_cents: int

class OrderResult(BaseModel):
    confirmation_number: str
    charged_at: str

# --- DI dependency (registered with the worker's DI container) ---

class PaymentGateway:
    async def charge(self, order_id: str, amount_cents: int) -> dict[str, str]: ...
    async def status(self, order_id: str) -> str: ...

# --- Actor ---

@actor(
    name="process_order",
    queue="payments",
    retry=RetryPolicy(kind="transient", max_attempts=5, base=timedelta(seconds=10)),
    result_ttl=timedelta(hours=24),
    unique_for=timedelta(minutes=10),
    unique_states=("pending", "scheduled", "running"),
    max_pending=5000,
)
async def process_order(
    payload: OrderPayload,
    ctx: JobContext[OrderPayload],
    *,
    gateway: PaymentGateway,
) -> OrderResult:
    ctx.log.info("charging", order_id=payload.order_id, attempt=ctx.attempt)

    if ctx.cancellation_requested:
        raise Snooze(timedelta(minutes=1))

    current_status = await gateway.status(payload.order_id)
    if current_status == "already_charged":
        return OrderResult(confirmation_number="DEDUP", charged_at="")

    try:
        charge = await gateway.charge(payload.order_id, payload.amount_cents)
    except RateLimitError as exc:
        raise RetryAfter(timedelta(seconds=exc.retry_after_seconds))

    # Enqueue a follow-up job for the receipt.
    await ctx.jobs.enqueue(
        send_receipt,
        ReceiptPayload(order_id=payload.order_id, email=payload.customer_id),
    )

    return OrderResult(
        confirmation_number=charge["confirmation"],
        charged_at=charge["charged_at"],
    )

# --- Enqueue ---

async def submit_order(client, order_id: str, customer_id: str, amount_cents: int):
    from taskq.exceptions import MaxPendingExceededError

    try:
        handle = await client.enqueue(
            process_order,
            OrderPayload(
                order_id=order_id,
                customer_id=customer_id,
                amount_cents=amount_cents,
            ),
            identity_key=order_id,  # required for unique_for dedup to take effect
        )
    except MaxPendingExceededError:
        raise RuntimeError("payment queue is full")

    if handle.was_existing:
        print(f"order {order_id} already queued: {handle.job_id}")
        return handle.job_id

    result: OrderResult = await handle.wait(timeout=30.0)
    print(f"confirmed: {result.confirmation_number}")
    return handle.job_id
```
