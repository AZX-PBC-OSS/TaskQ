# Retry System

## Overview

When an actor raises an exception, TaskQ evaluates the actor's `RetryPolicy` to decide whether to reschedule the job (retry) or mark it permanently failed. The retry system applies only to genuine exceptions — control-flow signals `Snooze` and `RetryAfter` are handled separately (see [Control-flow signals](#9-control-flow-signals)) and follow different rules regarding attempt counting.

---

## 1. RetryPolicy — field reference

`RetryPolicy` is a frozen Pydantic model imported from `taskq.retry`.

| Field | Type | Default | Semantics |
|---|---|---|---|
| `kind` | `"transient" \| "indefinite" \| "non_retryable"` | `"transient"` | Controls the retry strategy; see [Retry kinds](#2-retry-kinds). |
| `max_attempts` | `int` | `3` | Maximum total attempts for `"transient"`. Must be >= 1. Ignored by `"indefinite"`. |
| `time_budget` | `timedelta \| None` | `None` | Only active when `kind="indefinite"`. Passed to the enqueue path to auto-compute `schedule_to_close = now + time_budget`. Ignored for other kinds (a warning is emitted at decoration time if set on a non-indefinite actor). |
| `backoff` | `"exponential" \| "linear" \| "fixed"` | `"exponential"` | Backoff algorithm; see [Backoff algorithms](#3-backoff-algorithms). |
| `base` | `timedelta` | `timedelta(seconds=5)` | Starting delay for the chosen backoff algorithm. |
| `cap` | `timedelta` | `timedelta(hours=1)` | Per-actor ceiling on the computed delay before jitter. Must be >= `base`. |
| `jitter` | `float` | `0.2` | Multiplicative jitter factor. Must be in `[0.0, 1.0]`. |

**Validation constraints enforced at construction time:**
- `max_attempts >= 1` — `RetryPolicy(max_attempts=0)` raises `ValidationError`.
- `cap >= base` — `RetryPolicy(cap=timedelta(seconds=1), base=timedelta(seconds=5))` raises `ValidationError`.
- `jitter` in `[0.0, 1.0]` — `RetryPolicy(jitter=1.5)` raises `ValidationError`.

---

## 2. Retry kinds

### `"transient"` (default)

Retries up to `max_attempts` total attempts. The classifier retries when `attempt < max_attempts` and fails permanently when `attempt >= max_attempts`. Setting `max_attempts=1` means the first failure is terminal — no retries occur.

If a retry is due but the next dispatch would land past `schedule_to_close`, the job fails with `error_class="DeadlineExceeded"` instead of retrying — the deadline is enforced in SQL at the retry write (see [`schedule_to_close` interaction](#6-schedule_to_close-interaction)).

```python
from taskq import actor
from taskq.retry import RetryPolicy


@actor(retry=RetryPolicy(kind="transient", max_attempts=5))
async def my_actor(payload: Payload) -> Result: ...
```

### `"indefinite"`

Retries forever — `max_attempts` is ignored entirely. Use for jobs that must eventually succeed (e.g. eventually-consistent sync operations).

The only stopping condition is the `schedule_to_close` deadline, enforced in SQL at the retry
write and by the leader's deadline sweep for queued jobs (see
[`schedule_to_close` interaction](#6-schedule_to_close-interaction)):
- a retry whose delay would land past the deadline fails the job, or
- a queued job past the deadline is failed by the sweep.

Either condition produces `Fail(error_class="DeadlineExceeded")`.

`time_budget` is the recommended way to set an upper bound without computing an absolute datetime at enqueue time. When `kind="indefinite"` and `time_budget` is set, the enqueue path passes it to PostgreSQL as an interval so that `schedule_to_close = clock_timestamp() + time_budget` is computed at insert time.

If neither `schedule_to_close` nor `time_budget` is set, the job retries without any time limit. A warning is logged at decoration time when `kind="indefinite"` and `time_budget=None`.

```python
from datetime import timedelta
from taskq import actor
from taskq.retry import RetryPolicy


@actor(retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2)))
async def sync_data(payload: Payload) -> Result: ...
```

### `"non_retryable"`

Fails immediately on the first exception — no retries regardless of the exception type.

Use for actors where a retry would be harmful (e.g. payment operations whose idempotency is handled externally and a duplicate execution would cause double-charging).

```python
from taskq import actor
from taskq.retry import RetryPolicy


@actor(retry=RetryPolicy(kind="non_retryable"))
async def charge_card(payload: Payload) -> Result: ...
```

---

## 3. Backoff algorithms

All formulas use attempt `N` (1-indexed). The raw value is then subject to jitter and a global ceiling (described below).

### `"exponential"` (default)

```
raw = min(cap, base × 2^(N-1))
```

| Attempt | base=5s, cap=1h, jitter=0 |
|---|---|
| 1 | 5s |
| 2 | 10s |
| 3 | 20s |
| 4 | 40s |
| 5 | 1m 20s |
| 6 | 2m 40s |

### `"linear"`

```
raw = min(cap, base × N)
```

| Attempt | base=5s, cap=1h, jitter=0 |
|---|---|
| 1 | 5s |
| 2 | 10s |
| 3 | 15s |
| 4 | 20s |
| 5 | 25s |
| 6 | 30s |

### `"fixed"`

```
raw = base   (ignores attempt number)
```

| Attempt | base=5s, cap=1h, jitter=0 |
|---|---|
| 1–6 | 5s |

### Jitter

Jitter is multiplicative-symmetric:

```
delay = raw × uniform(1 - jitter, 1 + jitter)
```

The result is clamped to `[0, effective_cap]`. With the default `jitter=0.2`, each computed delay varies by ±20% of the raw value. For example, a raw delay of 10s produces a value in `[8s, 12s]`.

**Why not Full Jitter (`uniform(0, raw)`)?** Full Jitter collapses toward zero on attempt 1, causing a thundering-herd effect for high-volume actors. Multiplicative-symmetric jitter preserves the expected delay while still spreading retries across the fleet. (See Marc Brooker, "Exponential Backoff And Jitter", AWS Architecture Blog, and AWS .NET SDK issue #4341.)

With `jitter=0.0`, `uniform(1, 1) = 1.0`, so the raw delay is returned exactly — there is no collapse to zero.

### Global ceiling: `max_retry_backoff`

The worker applies a global ceiling on top of `policy.cap`:

```
effective_cap = min(policy.cap, settings.max_retry_backoff)
```

`WorkerSettings.max_retry_backoff` defaults to 24 hours. This prevents a misconfigured actor (e.g. `cap=timedelta(days=365)`) from stranding jobs for an unreasonably long time with no operator visibility. See `workers.md` for the `max_retry_backoff` setting.

Large exponents for `"exponential"` at high attempt numbers are safely clamped by the `min(cap_s, ...)` guard before any `timedelta` construction — Python's arbitrary-precision integers mean no overflow.

---

## 4. Non-retryable exceptions

`PayloadValidationError` (from `taskq.exceptions`) is always non-retryable regardless of the actor's `RetryPolicy`. It causes an immediate `Fail` with `error_class="PayloadValidationError"`.

The `RetryClassifier` also accepts a `non_retryable_exceptions` tuple. This is part of the `ActorConfigLike` protocol consumed by the worker's consumer loop. Subclasses of listed exception types are matched via `isinstance`, so listing `ValueError` also catches `MyValueError(ValueError)`.

**Important:** `non_retryable_exceptions`, `retry_classifier`, and `on_retry_exhausted` are properties of `ActorConfigLike` and are exposed through the `@actor` decorator as `non_retryable_exceptions`, `retry_classifier`, `on_retry_exhausted`, and `on_retry_exhausted_timeout` parameters. `retry_classifier` is documented in the next section; `on_retry_exhausted` is covered in [`on_retry_exhausted` hook](#8-on_retry_exhausted-hook).

!!! danger "Do not classify HTTP status codes by `4xx` / `5xx` — 429 is a 4xx"
    "Client errors are our bug, so never retry them" is the intuitive rule and it
    is wrong. **`429 Too Many Requests` is a 4xx**, and it is the single most
    important status to retry: a provider rate-limit response classified as
    non-retryable permanently fails work that would have succeeded seconds
    later, and the failure is silent — the job goes to `failed` with a plausible
    error message and no retry ever happens.

    `408 Request Timeout` is the same trap.

    Classify by *status*, not by *class*:

    | Status | Treat as | Why |
    |---|---|---|
    | `429`, `408` | **retryable** — `"indefinite"`, honouring `Retry-After` | Transient; the request is fine, the timing is not |
    | other `4xx` (`400`, `401`, `403`, `404`, `422`) | **non-retryable** | The request itself is wrong; retrying reproduces it |
    | `5xx` | **retryable** — `"transient"` with a bounded budget | Server-side, usually recovers |

    A blanket `non_retryable_exceptions=(HttpClientError,)` cannot express this,
    because the distinction is per *instance*, not per exception *type*. Use a
    [`retry_classifier`](#5-retry_classifier-hook-per-instance-retry-overrides)
    hook — the worked example in the next section implements exactly this table.

    The mirrored mistake is as costly: retrying `401`/`404` with a `"transient"`
    policy burns the whole retry budget on a request that can never succeed, and
    on an authentication failure can trip the provider's abuse protection.

---

## 5. `retry_classifier` hook — per-instance retry overrides

`non_retryable_exceptions` and `RetryPolicy.kind` classify by exception *type*. Sometimes a single
exception type needs different retry behaviour depending on *which instance* was raised — a
common example is an HTTP client's status-code error, where a 429 should retry indefinitely, a 404
should fail immediately, and a 500 should retry with a bounded budget, all while honouring a
server-provided `Retry-After` value as the actual backoff delay instead of the policy's computed
exponential/linear backoff.

Register a `retry_classifier` hook on the actor to get this:

```python
type RetryClassifierHook = Callable[[BaseException, int], RetryOverride | None]
```

The hook is invoked with `(exception, attempt)` for every exception that survives the
`non_retryable_exceptions`/`PayloadValidationError` checks (see below). Return `None` to fall
back to the actor's static `RetryPolicy` unchanged, or a `RetryOverride(kind=..., delay=...)` to
refine this specific occurrence. Both fields are optional — set only the ones you want to
override.

```python
from datetime import timedelta
from taskq import actor
from taskq.retry import RetryOverride, RetryPolicy


class HttpStatusError(Exception):
    def __init__(self, status_code: int, retry_after: float | None = None) -> None:
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"HTTP {status_code}")


def classify_http_error(exc: BaseException, attempt: int) -> RetryOverride | None:
    if not isinstance(exc, HttpStatusError):
        return None

    if exc.status_code == 429:
        delay = timedelta(seconds=exc.retry_after) if exc.retry_after is not None else None
        return RetryOverride(kind="indefinite", delay=delay)

    if exc.status_code == 404:
        return RetryOverride(kind="non_retryable")

    if exc.status_code >= 500:
        return RetryOverride(kind="transient")

    return None


@actor(
    retry=RetryPolicy(kind="transient", max_attempts=5),
    retry_classifier=classify_http_error,
)
async def call_partner_api(payload: Payload) -> Result: ...
```

**Precedence — the hook is not always consulted.** `non_retryable_exceptions` and TaskQ's
internal `PayloadValidationError` handling are checked *before* the hook and always win: if
either matches, the job fails immediately and `retry_classifier` is never called for that
exception.

!!! note "A broken hook can never crash the retry pipeline"
    If `retry_classifier` itself raises, the exception is caught and logged at `WARNING` by
    `decide_after_failure`, and classification falls back to the actor's static `RetryPolicy` as
    if the hook had returned `None`. This is a deliberate reliability guarantee — a buggy or
    poorly-tested classifier hook degrades to default behaviour instead of taking the job (or the
    worker) down.

**`max_retry_backoff` still clamps an override delay.** A `RetryOverride(delay=...)` — for
example one derived from a server's `Retry-After` header — is clamped to
`min(override_delay, max_retry_backoff)` before use, exactly like the policy's computed backoff.
A malicious or malformed header (e.g. `Retry-After: 999999999`) cannot strand a job past the
worker-wide ceiling. See [`WorkerSettings.max_retry_backoff`](workers.md) for that setting.

---

## 6. `schedule_to_close` interaction

`schedule_to_close` is the absolute deadline for the entire job lifetime — all attempts combined.

The deadline is not arbitrated by the Python classifier — the retry write itself refuses to
schedule past it, so the database clock is the single authority:
- If `clock_timestamp() + retry_delay` would land past `schedule_to_close`, the job fails
  terminally with `error_class="DeadlineExceeded"` instead of retrying.
- Queued jobs (`pending`/`scheduled`) past the deadline are failed by the leader's deadline
  sweep, and expired pending jobs are never dispatched.

The same guards gate `Snooze` and `RetryAfter` reschedules — any "come back later" that would
land past the deadline fails the job. A **running** attempt is never killed by
`schedule_to_close`; the deadline only arbitrates future dispatches, so an attempt that finishes
after the deadline still succeeds.

This applies to all retry kinds, including `"indefinite"`.

Set at enqueue time via the client:

```python
from datetime import UTC, datetime, timedelta
from taskq.client import JobsClient

await client.enqueue(
    my_actor,
    payload,
    schedule_to_close=datetime.now(UTC) + timedelta(hours=4),
)
```

See `jobs-clients.md` for the full `enqueue` signature.

---

## 7. `start_to_close` vs `schedule_to_close`

These two settings both look like "timeouts" but bound different things. Confusing them leads to
either jobs that never give up, or jobs that get cut off mid-retry-budget unexpectedly — so it's
worth being precise:

| | `schedule_to_close` | `start_to_close` |
|---|---|---|
| **Scope** | The job's entire retry lifecycle, across *all* attempts | A single attempt's execution |
| **Type** | `datetime` — an absolute deadline | `timedelta` — a duration |
| **Question it answers** | "When should this job give up entirely?" | "How long can one run of this job take before we give up on *it* and try again (or not)?" |
| **Enforced by** | SQL deadline guards on the retry/snooze/retry-after writes, dispatch exclusion of expired jobs, and the leader's deadline sweep (see [above](#6-schedule_to_close-interaction)) | `asyncio.wait_for` wrapped around a single actor invocation, at the consumer level |
| **What happens when it fires** | The job fails permanently (`error_class="DeadlineExceeded"`) — no further attempts, regardless of `max_attempts` remaining | That one attempt is cancelled and treated as a `TimeoutError` failure, fed through the normal retry classifier. The job does **not** necessarily stop — it may retry (subject to `schedule_to_close` and the retry policy) or fail permanently if attempts/deadline are exhausted |

In short: `schedule_to_close` is a ceiling on the whole job; `start_to_close` is a ceiling on each
individual attempt. A job can hit its `start_to_close` timeout three times in a row and still
retry a fourth time, as long as `max_attempts` and `schedule_to_close` allow it.

### Precedence chain

The effective `start_to_close` for a given attempt is resolved in this order — the first value
found wins:

1. **Per-enqueue override** — `client.enqueue(ref, payload, start_to_close=...)` for that specific
   call.
2. **Actor default** — `@actor(start_to_close=...)` declared on the actor.
3. **Worker-wide fallback** — `WorkerSettings.default_start_to_close` (env var
   `TASKQ_DEFAULT_START_TO_CLOSE`), applied only when neither of the above set anything.
4. **Unbounded** — if nothing anywhere sets a value, the attempt has no execution timeout
   (`None`).

```python
from datetime import timedelta
from taskq import actor
from taskq.client import JobsClient


# 2. Actor default: every attempt gets at most 2 minutes.
@actor(start_to_close=timedelta(minutes=2))
async def render_thumbnail(payload: Payload) -> Result: ...


# 1. Per-enqueue override: this specific call gets 30 seconds instead of the
#    actor's 2-minute default.
await client.enqueue(
    render_thumbnail,
    payload,
    start_to_close=timedelta(seconds=30),
)
```

```bash
# 3. Worker-wide fallback — applies only to actors/enqueue calls that set no
#    start_to_close of their own.
export TASKQ_DEFAULT_START_TO_CLOSE=5m
```

**Why set a worker-level default?** `WorkerSettings.default_start_to_close` gives every actor on
that worker a safety-net execution budget per attempt — without it, a hung or infinite-looping
actor can occupy a worker's coroutine slot indefinitely. Setting it once at the worker level means
you don't have to remember to add `start_to_close` to every individual `@actor` declaration; you
only need to override it (per actor or per enqueue call) for the actors that genuinely need a
different budget.

---

## 8. `on_retry_exhausted` hook

The `OnRetryExhausted` type alias is defined in `taskq.retry`:

```python
type OnRetryExhausted = Callable[[JobRow, BaseException], Awaitable[None] | None]
```

When a job exhausts its retry budget and transitions to `failed`, the consumer loop invokes this hook if one is registered. The hook receives the persisted `JobRow` and the exception that triggered the final failure.

Invocation behaviour:
- If the hook returns a coroutine, it is awaited under `asyncio.wait_for` with a timeout of `on_retry_exhausted_timeout` seconds (default `3.0`).
- `TimeoutError` and all other exceptions raised by the hook are caught and logged at `WARNING` level — they never propagate to the consumer loop.
- If a `WorkerOwnershipMismatch` occurs during the terminal `mark_failed_or_retry` write, the hook is skipped entirely.

`job_row.payload` is a raw `dict[str, object]`. If you need a typed payload inside the hook, re-validate it:

```python
typed_payload = actor_ref.payload_type.model_validate(job_row.payload)
```

The hook is registered via the `@actor` decorator's `on_retry_exhausted` and `on_retry_exhausted_timeout` parameters (see [Non-retryable exceptions](#4-non-retryable-exceptions) above).

### `on_success` hook

The `OnSuccess` type alias is defined in `taskq.retry`:

```python
type OnSuccess = Callable[[JobRow, object], Awaitable[None] | None]
```

When a job succeeds and transitions to `succeeded`, the consumer loop invokes this hook if one is registered. The hook receives the persisted `JobRow` and the actor's result. The result is typed `object` (the consumer loop erases the actor's return type at this boundary) — re-validate it via the actor's `result_adapter` if you need a typed value:

```python
typed_result = actor_ref.result_adapter.validate_python(job_row.result)
```

Invocation behaviour mirrors `on_retry_exhausted`:
- If the hook returns an awaitable, it is awaited under `asyncio.wait_for` with a timeout of `on_success_timeout` seconds (default `3.0`). Non-coroutine awaitables are detected via `inspect.isawaitable()`.
- `TimeoutError` and all other exceptions raised by the hook are caught and logged at `WARNING` level — they never propagate to the consumer loop.
- The hook runs after the transaction commits but before the success state-change event is published, so a failing hook does not roll back the job.

```python
from taskq import actor


async def emit_success_metric(job_row, result) -> None:
    # job_row.result is the type-erased actor return value.
    metrics.counter("jobs.succeeded").add(1, actor=job_row.actor)


@actor(on_success=emit_success_metric, on_success_timeout=5.0)
async def process_order(payload: OrderPayload) -> OrderResult: ...
```

---

## 9. Control-flow signals

These exceptions are raised inside the actor body to influence scheduling without going through the normal retry path.

### `Snooze(delay: timedelta)`

Defined in `taskq.exceptions`. Raises immediately reschedule the job to `now + delay` without evaluating the retry policy. The job transitions to `scheduled` status and the backoff formula is not consulted.

`Snooze` never exhausts the retry budget: the reschedule leaves `attempt` unchanged and bumps `max_attempts` by one, so the classifier's `attempt < max_attempts` test holds no matter how many times the job snoozes. (The dispatch that follows a snooze does increment `attempt` — the `max_attempts` bump compensates for it, which is why `metadata.snooze_count` exists for visibility rather than the attempt counter.)

A negative `delay` raises `ValueError` at construction.

If `now + delay > schedule_to_close`, the backend immediately fails the job with `error_class="DeadlineExceeded"` and `error_message="schedule_to_close reached before next dispatch"` instead of rescheduling.

```python
from datetime import timedelta
from taskq.exceptions import Snooze


async def poll_invoice(payload: Payload) -> Result:
    invoice = await fetch_invoice(payload.invoice_id)
    if invoice.status == "pending":
        raise Snooze(delay=timedelta(minutes=5))
    return process(invoice)
```

### `RetryAfter(delay: timedelta, *, consume_budget: bool = True)`

Defined in `taskq.exceptions`. Schedules a retry at `now + delay`, bypassing the normal backoff formula. Use when the actor knows the exact wait time (e.g. a `Retry-After` response header).

- `consume_budget=True` (default): the reschedule is gated by the attempt budget like a normal retry — with `kind="transient"` and no attempts left, the job fails terminally with `error_class="MaxAttemptsExceeded"`.
- `consume_budget=False`: snooze semantics — the reschedule leaves `attempt` unchanged and bumps `max_attempts` by one, so it can never exhaust the budget.

A negative `delay` raises `ValueError` at construction.

`RetryAfter`'s delay is **not** clamped by `max_retry_backoff` — only `schedule_to_close` gates it. This differs from a `retry_classifier` `RetryOverride(delay=...)`, which *is* clamped (see [§5](#5-retry_classifier-hook-per-instance-retry-overrides)).

```python
from datetime import timedelta
from taskq.exceptions import RetryAfter


async def call_api(payload: Payload) -> Result:
    resp = await http.post(...)
    if resp.status == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        raise RetryAfter(delay=timedelta(seconds=retry_after))
    return parse(resp)
```

---

## 10. Complete example

A realistic actor combining transient retries, exponential backoff, `Snooze` for a known service-unavailable condition, and `RetryAfter` for rate limits:

```python
from datetime import timedelta
from pydantic import BaseModel
from taskq import actor
from taskq.retry import RetryPolicy
from taskq.exceptions import Snooze, RetryAfter


class WebhookPayload(BaseModel):
    url: str
    body: dict


class WebhookResult(BaseModel):
    status_code: int


@actor(
    retry=RetryPolicy(
        kind="transient",
        max_attempts=4,
        backoff="exponential",
        base=timedelta(seconds=10),
        cap=timedelta(minutes=10),
        jitter=0.25,
    ),
)
async def deliver_webhook(payload: WebhookPayload) -> WebhookResult:
    resp = await http_post(payload.url, payload.body)
    if resp.status == 503:
        # Known maintenance window — snooze without consuming retry budget.
        raise Snooze(delay=timedelta(minutes=1))
    if resp.status == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        raise RetryAfter(delay=timedelta(seconds=retry_after))
    if resp.status >= 500:
        raise RuntimeError(f"Server error: {resp.status}")
    return WebhookResult(status_code=resp.status)
```

Backoff schedule for this policy (jitter=0 for illustration):

| Attempt | Delay before retry |
|---|---|
| 1 | 10s |
| 2 | 20s |
| 3 | 40s |
| 4 (final) | permanent failure |

---

## 11. Quick-reference table

| Scenario | Configuration |
|---|---|
| Retry 3 times with exponential backoff | `RetryPolicy()` (all defaults) |
| Retry 10 times, 30s linear backoff | `RetryPolicy(max_attempts=10, backoff="linear", base=timedelta(seconds=30))` |
| Retry forever, give up after 4 hours | `RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4))` |
| Never retry | `RetryPolicy(kind="non_retryable")` |
| Fixed 1-minute wait between retries | `RetryPolicy(backoff="fixed", base=timedelta(minutes=1))` |
| Single attempt, no retries | `RetryPolicy(max_attempts=1)` |

---

**See also:**
- [ops.md](ops.md) — operations & adoption guide: timeout policy by workload class, failure-classification doctrine, footguns
- `actors.md` — full `@actor` decorator reference.
- `workers.md` — `WorkerSettings.max_retry_backoff`, `WorkerSettings.default_start_to_close`, and other worker-level settings.
- `jobs-clients.md` — `schedule_to_close`, `start_to_close`, and other enqueue-time options.
