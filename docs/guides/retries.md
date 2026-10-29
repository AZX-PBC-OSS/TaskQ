# Retry System

## Overview

When an actor raises an exception, TaskQ evaluates the actor's `RetryPolicy` to decide whether to reschedule the job (retry) or mark it permanently failed. The retry system applies only to genuine exceptions; control-flow signals `Snooze` and `RetryAfter` are handled separately (see [Control-flow signals](#9-control-flow-signals)) and follow different rules regarding attempt counting.

This includes exceptions that do not derive from `Exception`: a `BaseException` subclass raised by an actor body (a custom panic-grade type, or `SystemExit` from a sync actor calling `sys.exit()` in its executor thread) is captured at the attempt boundary and classified like any other failure — the row lands `failed` with the exception's own type name as `error_class`, and the worker survives to run its remaining jobs. One buggy actor can never kill the worker or strand its row `running` for the lease sweep to relabel `WorkerCrashed`. The single deliberate exception is `KeyboardInterrupt`: it is interpreter/operator intent, never an actor outcome, and it propagates (the row is recovered by the lease-reclaim path).

The discipline holds at the seam families where user code runs inside the worker *and TaskQ owns the frame around it*, each with its own recorded outcome instead of worker death: a cron payload factory's `sys.exit()` (sync on the executor pool or async in the tick's frame) is that schedule's own tick failure, the strike and auto-disable telemetry record it and the tick survives; a notify connection factory's is an ordinary reconnect failure the retry loop survives; a `retry_classifier` hook's or an actor lifecycle hook's (sync or async, `on_success`/`on_cancel`/`on_retry_exhausted`) is a logged hook failure that never changes the already decided outcome; a DI provider factory's (a sync callable, or a sync generator's `__enter__` on its executor thread) is an attempt-level failure recorded through the same dispatch handler as any pre-actor failure — the attempt is terminal (no actor code ran), not the job: under the default policy the handler re-schedules it — and a sync generator's `__exit__` or an async generator's `__aexit__` raising `SystemExit` at teardown is swallowed by `aclose`'s log-and-continue policy (logged at ERROR, the remaining teardowns fire, the worker survives). Each converted seam raises a typed carrier whose `.original` is the user code's own exception, and the boundary that records the outcome unwraps it, so the carrier's name never reaches a row, a log or a span.

The claim is scoped, and one family is deliberately outside it: the credential-reload machinery (`reload_credentials`, the worker's `_reload_coordinator_loop`, `run_reload_schedule`, and the boot-time slot-pool open) runs its factories under `asyncio.wait_for` task boundaries guarded by `except Exception` only, so a factory's `sys.exit()` there propagates raw — loop-fatal at the task step — and ends the worker with exit 1. At the slot-pool open that *is* the documented contract (a worker that cannot open its transaction connections fails to boot, loudly); in the reload loops the survive-and-retry contract covers reload failures, never a `BaseException` — a credential factory must not call `sys.exit()`. (DI bootstrap is the other non-outcome surface: no dispatch handler exists yet to unwrap the carrier, so it fails the boot with exit 1.) `KeyboardInterrupt` propagates raw at every one of these seams; it is never converted to an outcome. That raw interrupt is also nobody's verdict at the per-job seams: the worker's task dies with it and the job's row strands `running` until lease expiry — the lease-reclaim path is the recovery, interrupts reclaim, they never fabricate a verdict.

---

## 1. RetryPolicy: field reference

`RetryPolicy` is a frozen Pydantic model imported from `taskq.retry` (the
module's full surface — classifiers, backoff computation, lifecycle-hook
invokers, the attempt ceilings — is indexed on the
[Retry API reference](../api-reference/retry.md)).

| Field | Type | Default | Semantics |
|---|---|---|---|
| `kind` | `"transient" \| "indefinite" \| "non_retryable"` | `"transient"` | Controls the retry strategy; see [Retry kinds](#2-retry-kinds). |
| `max_attempts` | `int` | `3` | Maximum total attempts for `"transient"`. Must be >= 1 and <= 32767 (the `smallint` `jobs.max_attempts` column ceiling, `MAX_ATTEMPTS_SMALLINT_CEILING`); a fresh enqueue or policy may carry at most 32766 (`MAX_ENQUEUABLE_MAX_ATTEMPTS`, one below the column ceiling so reclaim bookkeeping keeps headroom to add one). Ignored by `"indefinite"`. |
| `time_budget` | `timedelta \| None` | `None` | Only active when `kind="indefinite"`. Passed to the enqueue path to auto-compute `schedule_to_close = now + time_budget`. Ignored for other kinds (a warning is emitted at decoration time if set on a non-indefinite actor). |
| `backoff` | `"exponential" \| "linear" \| "fixed"` | `"exponential"` | Backoff algorithm; see [Backoff algorithms](#3-backoff-algorithms). |
| `base` | `timedelta` | `timedelta(seconds=5)` | Starting delay for the chosen backoff algorithm. Must be > 0. |
| `cap` | `timedelta` | `timedelta(hours=1)` | Per-actor ceiling on the computed delay before jitter. Must be >= `base`. |
| `jitter` | `float` | `0.2` | Multiplicative jitter factor on the computed curve. Must be in `[0.0, 1.0]`. Explicit `RetryOverride.delay`s are honored exactly (never jittered). |

**Validation constraints enforced at construction time:**
- `max_attempts >= 1`: `RetryPolicy(max_attempts=0)` raises `ValidationError`.
- `base > 0`: `RetryPolicy(base=timedelta(0))` raises `ValidationError`. A zero or negative base degenerates the curve to a zero-period retry loop that monopolises a worker slot with no backoff; rows stamped by earlier releases that accepted the shape are floored at the retry and reclaim writes instead.
- `cap >= base`: `RetryPolicy(cap=timedelta(seconds=1), base=timedelta(seconds=5))` raises `ValidationError`.
- `jitter` in `[0.0, 1.0]`: `RetryPolicy(jitter=1.5)` raises `ValidationError`.

---

## 2. Retry kinds

### `"transient"` (default)

Retries up to `max_attempts` total attempts. The classifier retries when `attempt < max_attempts` and fails permanently when `attempt >= max_attempts`. Setting `max_attempts=1` means the first failure is terminal; no retries occur.

If a retry is due but the next dispatch would land past `schedule_to_close`, the job fails with `error_class="DeadlineExceeded"` instead of retrying: the deadline is enforced in SQL at the retry write (see [`schedule_to_close` interaction](#6-schedule_to_close-interaction)).

```python no-exec — not executed: fragment, names bound by an earlier fence
from taskq import actor
from taskq.retry import RetryPolicy


@actor(retry=RetryPolicy(kind="transient", max_attempts=5))
async def my_actor(payload: Payload) -> Result: ...
```

### `"indefinite"`

Retries forever: `max_attempts` is ignored entirely. Use for jobs that must eventually succeed (e.g. eventually-consistent sync operations).

The stored row still carries the actor's configured `max_attempts` value (default `3`), but for this kind the field is inert: a live job can reach `attempt` far beyond it. The admin UI renders the inert field as `— (indefinite)` so the row does not advertise a budget it is not enforcing.

The only stopping condition is the `schedule_to_close` deadline, enforced in SQL at the retry
write and by the leader's deadline sweep for queued jobs (see
[`schedule_to_close` interaction](#6-schedule_to_close-interaction)):
- a retry whose delay would land past the deadline fails the job, or
- a queued job past the deadline is failed by the sweep.

Either condition produces `Fail(error_class="DeadlineExceeded")`.

`time_budget` is the recommended way to set an upper bound without computing an absolute datetime at enqueue time. When `kind="indefinite"` and `time_budget` is set, the enqueue path passes it to PostgreSQL as an interval so that `schedule_to_close = clock_timestamp() + time_budget` is computed at insert time.

If neither `schedule_to_close` nor `time_budget` is set, the job retries without any time limit. A warning is logged at decoration time when `kind="indefinite"` and `time_budget=None`.

```python no-exec — not executed: fragment, names bound by an earlier fence
from datetime import timedelta
from taskq import actor
from taskq.retry import RetryPolicy


@actor(retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2)))
async def sync_data(payload: Payload) -> Result: ...
```

### `"non_retryable"`

Fails immediately on the first exception, with no retries regardless of the exception type.

Use for actors where a retry would be harmful (e.g. payment operations whose idempotency is handled externally and a duplicate execution would cause double-charging).

```python no-exec — not executed: fragment, names bound by an earlier fence
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
| 1-6 | 5s |

### Jitter

Jitter is multiplicative-symmetric:

```
delay = raw × uniform(1 - jitter, 1 + jitter)
```

The band `[raw × (1 − jitter), raw × (1 + jitter)]` is fitted under `effective_cap` *before* the draw, so the result always lies in `[0, effective_cap]`. With the default `jitter=0.2`, each computed delay varies by ±20% of the raw value. For example, a raw delay of 10s produces a value in `[8s, 12s]`.

At the cap the band is one-sided: once the curve saturates (the default exponential policy from attempt 11 on, any `fixed`/`linear` policy whose base reaches the cap, a curve above `max_retry_backoff`), the delay is drawn uniformly from `[cap × (1 − jitter), cap]` (e.g. `[48min, 60min]`) for the default policy. The cap bounds the band, not the drawn value: clamping the drawn value would collapse the upper half of the band onto the cap exactly, so half of a cohort retrying at the cap, the retries most likely to follow a fleet-wide event, would come due at the same instant.

**Why not Full Jitter (`uniform(0, raw)`)?** Full Jitter collapses toward zero on attempt 1, causing a thundering-herd effect for high-volume actors. Multiplicative-symmetric jitter preserves the expected delay while still spreading retries across the fleet. (See Marc Brooker, "Exponential Backoff And Jitter", AWS Architecture Blog.)

With `jitter=0.0`, `uniform(1, 1) = 1.0`, so the raw delay is returned exactly; there is no collapse to zero.

**Scope: jitter spreads the computed curve, never an explicit override.** The band above applies to delays TaskQ computes — the backoff curve here, the crash-reclaim hand-back in [§12](#12-crash-vs-shutdown-what-happens-to-the-attempt-count). An explicit `RetryOverride.delay` — e.g. the server's `Retry-After` value your classifier handed back — is honored **exactly**, with no draw: an explicit direction is not something a default gets to mutate, and drawing ±20% over a server's horizon would schedule retries *before* it, the exact bad-citizen behavior the header exists to prevent. A fleet that wants spread on a hint applies it in its own classifier — see the opt-in spread pattern in [§5](#5-retry_classifier-hook-per-instance-retry-overrides).

### Global ceiling: `max_retry_backoff`

The worker applies a global ceiling on top of `policy.cap`:

```
effective_cap = min(policy.cap, settings.max_retry_backoff)
```

`WorkerSettings.max_retry_backoff` defaults to 24 hours. This prevents a misconfigured actor (e.g. `cap=timedelta(days=365)`) from stranding jobs for an unreasonably long time with no operator visibility. See `workers.md` for the `max_retry_backoff` setting.

Large exponents for `"exponential"` at high attempt numbers are safely clamped by the `min(cap_s, ...)` guard before any `timedelta` construction: Python's arbitrary-precision integers mean no overflow.

---

## 4. Non-retryable exceptions

`PayloadValidationError` (from `taskq.exceptions`) is always non-retryable regardless of the actor's `RetryPolicy`. It causes an immediate `Fail` with `error_class="PayloadValidationError"`.

The `RetryClassifier` also accepts a `non_retryable_exceptions` tuple. This is part of the `ActorConfigLike` protocol consumed by the worker's consumer loop. Subclasses of listed exception types are matched via `isinstance`, so listing `ValueError` also catches `MyValueError(ValueError)`.

**Important:** `non_retryable_exceptions`, `retry_classifier`, and `on_retry_exhausted` are properties of `ActorConfigLike` and are exposed through the `@actor` decorator as `non_retryable_exceptions`, `retry_classifier`, `on_retry_exhausted`, and `on_retry_exhausted_timeout` parameters. `retry_classifier` is documented in the next section; `on_retry_exhausted` is covered in [`on_retry_exhausted` hook](#8-on_retry_exhausted-hook).

!!! danger "Do not classify HTTP status codes by `4xx` / `5xx`: 429 is a 4xx"
    "Client errors are our bug, so never retry them" is the intuitive rule and it
    is wrong. **`429 Too Many Requests` is a 4xx**, and it is the single most
    important status to retry: a provider rate-limit response classified as
    non-retryable permanently fails work that would have succeeded seconds
    later, and the failure is silent: the job goes to `failed` with a plausible
    error message and no retry ever happens.

    `408 Request Timeout` is the same trap.

    Classify by *status*, not by *class*:

    | Status | Treat as | Why |
    |---|---|---|
    | `429`, `408` | **retryable** (`"indefinite"`, honouring `Retry-After`) | Transient; the request is fine, the timing is not |
    | other `4xx` (`400`, `401`, `403`, `404`, `422`) | **non-retryable** | The request itself is wrong; retrying reproduces it |
    | `5xx` | **retryable** (`"transient"` with a bounded budget) | Server-side, usually recovers |

    A blanket `non_retryable_exceptions=(HttpClientError,)` cannot express this,
    because the distinction is per *instance*, not per exception *type*. Use a
    [`retry_classifier`](#5-retry_classifier-hook-per-instance-retry-overrides)
    hook; the worked example in the next section implements exactly this table.

    The mirrored mistake is as costly: retrying `401`/`404` with a `"transient"`
    policy burns the whole retry budget on a request that can never succeed, and
    on an authentication failure can trip the provider's abuse protection.

---

## 5. `retry_classifier` hook: per-instance retry overrides

`non_retryable_exceptions` and `RetryPolicy.kind` classify by exception *type*. Sometimes a single
exception type needs different retry behaviour depending on *which instance* was raised;
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
refine this specific occurrence. Both fields are optional: set only the ones you want to
override.

```python no-exec — not executed: fragment, names bound by an earlier fence
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

**Precedence: the hook is not always consulted.** `non_retryable_exceptions` and TaskQ's
built-in unconditional-failure classes — `PayloadValidationError`, a pydantic
`ValidationError` from payload decoding, `ResultTooLarge`, and `UnencodableValue` — are
checked *before* the hook and always win: if any matches, the job fails immediately and
`retry_classifier` is never called for that exception.

!!! note "A broken hook can never crash the retry pipeline"
    If `retry_classifier` itself raises, the exception is caught and logged at `WARNING` by
    `decide_after_failure`, and classification falls back to the actor's static `RetryPolicy` as
    if the hook had returned `None`. This is a deliberate reliability guarantee: a buggy or
    poorly-tested classifier hook degrades to default behaviour instead of taking the job (or the
    worker) down.

**`max_retry_backoff` still clamps an override delay.** A `RetryOverride(delay=...)`, for
example one derived from a server's `Retry-After` header, is clamped to
`min(override_delay, max_retry_backoff)` before use, exactly like the policy's computed backoff.
A malicious or malformed header (e.g. `Retry-After: 999999999`) cannot strand a job past the
worker-wide ceiling. See [`WorkerSettings.max_retry_backoff`](workers.md) for that setting.

### `JobRetryState`: the row projection behind classification

Before classification, the failure path projects the job row into a
`JobRetryState` (`taskq.retry`, re-exported from `taskq`) — the
`NamedTuple` `decide_after_failure` consumes to reconstruct the effective
policy and to feed your `retry_classifier` hook:

```python
from datetime import timedelta

from taskq import JobRetryState

# What the failure path projects from the job row before classification
# (illustrative values; TaskQ constructs this from the row itself):
state = JobRetryState(
    attempt=2,  # 1-based; the hook receives this as its second argument
    max_attempts=5,  # row-stored; authoritative over the @actor literal
    retry_kind="transient",  # row-stored kind the policy is reconstructed from
    schedule_to_close=None,  # observability only: the SQL deadline guard arbitrates the deadline
    start_to_close=timedelta(seconds=30),  # reserved for per-attempt enforcement at the consumer
)
print(state.attempt, state.max_attempts, state.retry_kind)
```

Two fields are not classification inputs: `schedule_to_close` is carried for
observability (the SQL deadline guard in `mark_failed_or_retry` is the single
deadline arbiter — see [§6](#6-schedule_to_close-interaction)), and
`start_to_close` is reserved for per-attempt timeout enforcement at the
consumer level (`asyncio.wait_for`), not used by the classifier. The row's
`max_attempts` and `retry_kind` are authoritative over the registered
`@actor` literal: a row/registration mismatch is re-validated loudly rather
than silently trusted.

### Composing classifiers: `compose_retry_classifiers` and `rate_limit_aware_classifier`

An actor that needs both a domain-specific classifier and rate-limit awareness would
otherwise hand-roll the "try mine, then fall back" chain inside one function. Two helpers
(`taskq.retry`, re-exported from `taskq`) compose classifiers instead:

```python no-exec — not executed: fragment, names bound by an earlier fence
from taskq import actor, rate_limit_aware_classifier
from taskq.retry import RetryPolicy, compose_retry_classifiers


@actor(
    retry=RetryPolicy(kind="transient", max_attempts=5),
    retry_classifier=compose_retry_classifiers(
        classify_http_error,  # your domain classifier first...
        rate_limit_aware_classifier,  # ...the built-in as the catch-all
    ),
)
async def call_partner_api(payload: Payload) -> Result: ...
```

**Semantics: first override wins, order matters.** Each classifier is invoked in
registration order with `(exception, attempt)`. The first classifier returning a
`RetryOverride` decides and later classifiers are not consulted (short-circuit); a `None`
falls through to the next; when every classifier returns `None` the composition returns
`None` and the declared `RetryPolicy` governs, exactly as a single hook returning `None`
does. Put the most specific classifier first — here `classify_http_error` claims 429s with
a server-provided `Retry-After` delay before the built-in ever sees them.

**A buggy classifier is isolated, not fatal.** The single-hook reliability guarantee
composes per classifier: one that raises is logged at `WARNING` and skipped, and one that
returns something that is not a `RetryOverride` is logged and skipped too — composition
continues with the next classifier rather than discarding the healthy ones' overrides and
falling back to the declared policy. `KeyboardInterrupt` and `asyncio.CancelledError` are
never a classifier outcome and propagate raw. `compose_retry_classifiers()` with no
arguments returns a hook that always returns `None`, so a call site can compose a
possibly-empty list.

**`rate_limit_aware_classifier` fixes the 429-burns-the-budget trap.** Without a
classifier, a declared `transient` policy burns one attempt of `max_attempts` per 429:
with the defaults (`max_attempts=3`, `base=5s`, exponential) a sustained rate limit kills
the job after two delays — about 15 seconds (5s + 10s; the 20s rung is never reached, the
third failure is terminal and schedules no delay) — which is almost never the operator's
intent — a rate limit means "wait it out", the unbounded-in-attempts behaviour only an
`indefinite` kind provides. The built-in recognizes:

- TaskQ's own `ReservationUnavailable` raised with `source="rate_limit"` (a shared
  limiter's denial surfacing in the actor's frame); a `source="reservation"` denial is a
  concurrency-slot condition and is not recognized.
- Common HTTP 429 duck-types, no import required: an exception carrying
  `.response.status_code == 429` (the `httpx`/`httpx2`/`requests` shapes),
  `.response.status == 429` or a bare `.status == 429` (the `aiohttp`
  `ClientResponseError` shape), or a bare `.status_code == 429`.
- An exception whose class is literally named `RateLimitError` (the openai/anthropic-style
  SDK shape), even without HTTP attributes. This is the loosest signal: the HTTP shapes at
  least require a 429, but the name match requires nothing, so an unrelated domain error
  that merely carries the name (no rate-limit semantics at all) is overridden to
  `indefinite` too. If your domain has such a class, put a classifier that claims it
  *before* the built-in in the composition, or rename it.

Everything else returns `None`: a 500 keeps its bounded transient budget and a 404 its
non-retryable verdict even when the built-in is composed in. The override sets
`kind="indefinite"` only — the declared policy's backoff curve (jitter, cap,
`max_retry_backoff`) keeps computing *when* to retry. For a known-duration wait that
spends no attempt budget, raise `RetryAfter(delay, consume_budget=False)` from the actor
body instead ([§9](#9-control-flow-signals)).

**The claim is configurable: `make_rate_limit_aware_classifier(claim_kind=...)`.** The
built-in is the factory's `claim_kind="indefinite"` instance, kept as a module-level name
so existing registrations are untouched. The factory accepts:

- `claim_kind="indefinite"` (default) — the built-in exactly as documented here;
- `claim_kind="transient"` — `RetryOverride(kind="transient")`, `delay=hint` when the
  server sent one: `max_attempts` stays the stopper and the hint sets *when* within it —
  the bounded AND server-honoring shape;
- `claim_kind=None` — the classifier never claims: the identity for composition.

Validation is at construction: a `claim_kind` outside the three modes raises `ValueError`
(fail loud at build time, never a silently-misclaiming classifier at override time). The
recognition surface, the hint parsing, the garbage rules, and the bounds are identical for
every mode — only the kind stamped on the override changes:

| `claim_kind` | Override on a claimed signal | What bounds it | The haunt hazard (per mode) |
|---|---|---|---|
| `"indefinite"` (default) | `kind="indefinite"`, `delay=hint` when present | `schedule_to_close` — the delay schedules *when*, never *whether* | a `transient` actor has no `schedule_to_close` (a `time_budget` is only honored for an `indefinite`-declared policy), so a sustained 429 storm retries the job forever — a deadline is REQUIRED in this mode |
| `"transient"` | `kind="transient"`, `delay=hint` when present | `max_attempts` — the budget stays the stopper; the hint sets *when* | none: the budget terminates the storm even with no deadline |
| `None` | never claims | the declared policy governs everything | none: the classifier contributes no claims to compose over |

!!! danger "The override's only stopping condition is a deadline — give the job one"
    `kind="indefinite"` has no attempt ceiling. Its single stopping condition is the job's
    `schedule_to_close`, arbitrated in SQL — and **a `transient` actor never has one**:
    `retry.time_budget` is only stamped as a `schedule_to_close` for actors declared
    `kind="indefinite"` (setting it on a `transient` actor warns `actor-config-time-budget-ignored`
    at registration and is dropped at enqueue), so a `transient` job's
    `schedule_to_close` is `NULL` unless a per-enqueue `schedule_to_close=` was passed.
    A `NULL` deadline never trips the deadline sweep and never blocks dispatch. Composing
    this built-in into a `transient` actor therefore means: a sustained 429 storm retries
    that job **forever** — unbounded attempts *and* unbounded wall-clock time, with no
    warning at registration (the declared kind is `transient`, so
    `actor-config-indefinite-no-budget` cannot fire) and no warning at override time. The
    example at the top of this section is exactly this configuration. Give such an actor a
    stopping condition: declare it `kind="indefinite"` with a `retry.time_budget` (the
    deadline then exists and the registration warning stays honest), bound the 429s with a
    domain classifier registered *before* the built-in (e.g. override to `transient` past
    a budget you track yourself), or pass `schedule_to_close=` per enqueue (deprecated
    form). There is a fourth remedy, purpose-built: build the built-in's bounded variant —
    `make_rate_limit_aware_classifier(claim_kind="transient")` — bounded AND
    server-honoring: `max_attempts` stays the stopper, the hint sets when, and the
    recognition/parsing/bounds are exactly the built-in's (see the knob table above).

### Sniffing the server's hint: `Retry-After` / `X-Retry-After`

On a claimed signal the built-in also reads the server's delay hint, duck-typed over the
common shapes with no import required (TaskQ never imports an HTTP client; your
transitive dependency tree stays yours): `exception.response.headers` (the
`httpx`/`httpx2`/`requests` shape) and a bare `exception.headers` (the `aiohttp`
`ClientResponseError` shape, or any exception carrying headers directly). Both the
standard `retry-after` and the de-facto `x-retry-after` header names are read,
case-insensitively; the standard header wins when both are present. Values are parsed
as decimal-fraction seconds (`"120"`, `"0.5"`) and HTTP-dates (RFC 9110 IMF-fixdate, via
the stdlib's `email.utils.parsedate_to_datetime`).

Every recognized signal, what the built-in returns for it, and what bounds the result
(the default `claim_kind="indefinite"` mode; the transient mode swaps the kind and the
bound — knob table above):

| Signal | Override returned | What bounds it |
|---|---|---|
| `429` + hint parsed to a positive delay | `indefinite` **with** `delay` | the delay is honored exactly — no jitter draw (the library never mutates a value your classifier specified) — `max_retry_backoff` clamps it, `MIN_DEFERRAL_INTERVAL` floors it — and `schedule_to_close` still terminates the job (the delay does not move the deadline) |
| `429` with no hint header | `indefinite`, no `delay` | the declared policy's curve (`jitter`, `cap`, `max_retry_backoff`); `schedule_to_close` terminates |
| `429` + hint of `0`, negative-shape, or unparsable | `indefinite`, no `delay` (curve fallback) | same row as above — garbage degrades the *delay*, never the classification |
| `RateLimitError`-named exception (with or without headers) | per the two rows above | same bounds |

The curve-fallback rule is deliberate good citizenship: a server that answers
`Retry-After: 0` (or a value no grammar knows) does not get to degenerate the retry loop
— the declared policy's curve keeps computing *when*, and the job's deadline keeps
deciding *whether*. The parsed hint itself is honored **exactly** —
an explicit override delay is an explicit direction, and the library never mutates a
value your classifier specified (drawing ±20% over the server's horizon would schedule
retries *before* it). So the default needs no `jitter=0.0` escape for exact
`Retry-After` compliance any more, and a fleet that wants its workers fielding the same
hint not to come due in lockstep spreads the hint itself, in its own classifier. This
fragment is deliberately not executed — it binds names from the earlier examples and
illustrates the wrapping pattern only, so it is set as indented text rather than a
`​```python` fence (the executor collects those):

    import random

    from taskq.retry import RetryOverride, apply_jitter

    _spread_rng = random.Random()

    def spread_rate_limit_hint(exc: BaseException, attempt: int) -> RetryOverride | None:
        """The built-in's recognition, plus opt-in fleet-spread on the hint."""
        override = rate_limit_aware_classifier(exc, attempt)
        if override is not None and override.delay is not None:
            return RetryOverride(
                kind=override.kind,
                delay=apply_jitter(override.delay, 0.2, _spread_rng),
            )
        return override

    # register the wrapper instead of the bare built-in:
    #     retry_classifier=spread_rate_limit_hint

The spread is yours to shape (`apply_jitter` is the same multiplicative-symmetric band
the curve uses, and it is your explicit direction now, so the default is not choosing
for you).

**A finite hint is never garbage — the ceiling clamps it.** A hint beyond the old
one-day parse cap (`"100000"`, or a `1e20`-class value) is honored as the delay and
CLAMPED to `max_retry_backoff` on the decision path — the ceiling's documented job ("a
malicious or malformed header cannot strand a job") and the operator's explicit knob, so
`max_retry_backoff=120s` + a `1e20`-class hint retries at exactly 120s. The parse adds
only a *representability* saturation (a value beyond `timedelta`'s range saturates
instead of crashing a classifier); it adds no semantic cap — the ceiling IS the bound.

**The seconds grammar is a decimal fraction, closed on purpose.** Recognized:
`"120"`, `"0.5"` (digits, optional `.`-fraction — a sub-second hint flows through and
lands on the `MIN_DEFERRAL_INTERVAL` floor, 1s, on the decision path). Rejected as
garbage (curve fallback): a comma decimal (`"1,5"`), scientific notation (`"1e3"`), a
sign (`"+30"`), an underscore digit separator (`"1_000"`), non-ASCII decimal digits
(`"١٢٣"`), embedded whitespace, and everything unparsable — the last three were
silently honored by the previous `int()` parse and the closed grammar is a deliberate
tightening of exactly those accidents (none is an RFC 9110 delta-seconds form). A
rate-limit hint is a
human-scale count of seconds; every richer parse is a divergence surface between
consumers, not a feature. `Retry-After: 0` stays curve-fallback (the monopolisation
hazard); the explicit-zero *floor* — an override carrying `delay=timedelta(0)` honored
as "as fast as the deferral floor allows" — remains the decision path's rule.

**HTTP-date parsing stays, and diverging consumers have the fence.** The date form is
kept: a consumer preferring fallback-over-parse fences HTTP-date forms in their own
classifier first (composition order decides — [pinned
above](#composing-classifiers-compose_retry_classifiers-and-rate_limit_aware_classifier)).

When two legitimate intents collide — the server's directive vs the operator's
`max_retry_backoff` ceiling — the default resolves **toward the server**: the parsed
hint is honored as the delay, exactly (no jitter draws over it), and `max_retry_backoff`
stays exactly what it already was for every override delay, the operator's safety
ceiling (the established contract, unchanged). The escape hatches are user
configuration, never a silent override: a user who wants the raw server value already
has it — the default honors the hint exactly (raise `max_retry_backoff` if the
operator's ceiling is genuinely lower than the server's ask); a user who wants the hint
spread across a fleet applies `apply_jitter` to it in their own classifier (the pattern
above); a user who wants the curve regardless of the server
fences the recognition with their own classifier registered first in the composition
or `exclude_names`/`transient_status` on the taxonomy; composition order decides, the
same way it decides for every override.

### The common shapes: `failure_taxonomy_classifier`

Where the built-in above owns the one signal with dedicated semantics (429 →
`indefinite`, deadline-bounded), `failure_taxonomy_classifier` (a factory; call it to
get a hook) sniffs the mundane taxonomy:

- **transient** — connection-class errors (`isinstance` of the builtin
  `ConnectionError` family), TimeoutError-shaped errors (`isinstance` of the builtin
  `TimeoutError` — which covers `socket.timeout` and `asyncio.TimeoutError` — or an
  **exact** class name in `DEFAULT_TRANSIENT_EXCEPTION_NAMES`, so `httpx.ReadTimeout`,
  `requests.ConnectTimeout`, `aiohttp.ServerTimeoutError`, and urllib3/botocore's
  `ReadTimeoutError`/`ConnectTimeoutError` claim without an import), and
  HTTP `5xx` / `408` / `425` statuses;
- **non-retryable** — any other `4xx` status;
- **`None`** — everything else. Unsure → `None`: over-claiming is the haunt class, and
  a taxonomy that guesses sends work where the declared policy never agreed to go.

The matching rule is a contract: **exact curated names by default; the suffix
inference is opt-in and documented with its counterexample.** The default never
widens a consumer's semantics — consumers who route timeout-shaped classes narrowly
(static-policy, pinned per class) are the supported shape, and the library's defaults
never override that explicit direction.

The suffix inference is exactly that stance's counter-case, which is why it is behind
a flag. `infer_timeout_by_suffix=True` claims `transient` for any class name *ending*
in `Timeout`/`TimeoutError` — the convenience for consumers who don't want to
enumerate names. Its documented cost is `pymongo.errors.ExecutionTimeout`: the
*server* killed an operation for exceeding its `maxTimeMS`, and re-running the same
query re-fails deterministically — a deadline-exceeded that MEANS failure. Its name
ends in `Timeout`, so the suffix flag claims it `transient` and every such retry
burns budget on unwinnable work (bounded by `max_attempts` — a spent budget, not a
runaway — but spent on work no retry can win). Under the default exact-name matching
the taxonomy returns `None` for it and the declared policy governs. Opt in only when
your timeout-shaped names are genuinely transient-by-construction; pin narrow
otherwise.

The defaults are documented module constants (`taskq.retry`):
`DEFAULT_TRANSIENT_STATUSES` (`408`, `425`, and the `5xx` band),
`DEFAULT_NON_RETRYABLE_STATUSES` (the `4xx` band minus `{408, 425, 429}`),
`DEFAULT_TRANSIENT_EXCEPTION_NAMES`, `DEFAULT_EXCLUDED_EXCEPTION_NAMES` (empty).
`429` is deliberately carved out of the non-retryable band — [§4](#4-non-retryable-exceptions)'s
table calls it the single most important status to retry — so the taxonomy composes
safely with the rate-limit built-in in either order.

Defaults, composed after the rate-limit built-in (the recommended order); the
configured variant shows sets replacing the defaults and name lists extending and
excluding by exact class name. This fragment is deliberately not executed — it binds
names from the earlier examples and illustrates configuration shape only, so it is
set as indented text rather than a `​```python` fence (the executor collects those,
and this fragment's names are bound by the earlier fences):

    composed = compose_retry_classifiers(
        rate_limit_aware_classifier,  # 429s, Retry-After hints
        failure_taxonomy_classifier(),  # connection/timeout/5xx/4xx shapes
    )
    taxonomy = failure_taxonomy_classifier(
        transient_status=frozenset({408, 425, *range(500, 600)}),
        include_names=DEFAULT_TRANSIENT_EXCEPTION_NAMES | {"VendorFlake"},
        exclude_names={"ReadTimeout"},
    )

Precedence, pinned by tests: `exclude_names` → transient signals (connection class,
TimeoutError `isinstance`, name include-set, the opt-in suffix inference,
`transient_status`) → `non_retryable_status` → `None`. `exclude_names` outranks
everything, the suffix flag included. A status present in both sets is transient
("retry later" is the safer wrong
answer than killing a retryable job). Passing a set replaces the default entirely —
there is no implicit merge, so what a classifier claims is always exactly what its
configuration says.

!!! note "Why the taxonomy never returns `indefinite`"
    Every claim the taxonomy makes is `transient` (bounded by the declared
    `max_attempts`) or `non_retryable` (terminal) — and `None` defers to the declared
    policy. It never returns `indefinite`, so composing it needs no deadline argument:
    a transient actor's budget still applies. The indefinite kind is reserved for the
    rate-limit built-in above, whose danger block documents the deadline its override
    requires. Pin:
    `tests/test_failure_taxonomy_classifier.py::test_taxonomy_never_returns_indefinite`.

### Don't want 429 doing (indefinite) backoff? You have three explicit ways out

Everything above gives 429s to the built-in's `indefinite` override because a rate
limit usually means "wait it out". That is a default, never a requirement — and
none of the ways out is silent. All three are configuration you write, in one
place here:

1. **Don't compose the built-in.** `rate_limit_aware_classifier` is opt-in by
   construction: nothing registers it for you. Leave it out and a 429 is an
   ordinary exception — your declared policy governs untouched. For the default
   `transient` policy that is the trap restated, not escaped: a sustained 429
   burns the budget (5s + 10s, the third failure is terminal — about 15 seconds,
   the math computed above when introducing the built-in). This way out restores
   exactly the death the built-in exists to prevent; take it only when "give up
   quickly on 429s" is genuinely the intent for that actor.

2. **Claim 429 in your taxonomy's `transient_status`.** Run the taxonomy without
   the built-in and claim the 429s as bounded-transient yourself:

       taxonomy = failure_taxonomy_classifier(
           transient_status=frozenset({408, 425, 429, *range(500, 600)}),  # the defaults, plus 429
       )
       taxonomy(Http429Error(), 1)  # → RetryOverride(kind="transient"): max_attempts governs

   Passing the set replaces the defaults wholesale (no implicit merge), so spell
   out the whole set, defaults included. The claim is
   `RetryOverride(kind="transient")`: the declared `max_attempts` governs and the
   job dies when the budget does — bounded, never indefinite. (Composed with the
   built-in anyway, order decides: the taxonomy must come first, or the built-in's
   `indefinite` claim wins the 429. The purpose-built form of this route is the
   built-in's own bounded mode — `make_rate_limit_aware_classifier(claim_kind="transient")`
   — which needs no composition order argument and keeps the hint sniffing.)

3. **Fence with your own classifier first.** Composition order is the general
   escape: register a classifier that claims 429s however you like — your own
   `Retry-After` parsing, your own budget — before the built-in, and the built-in
   never sees one (first override wins; the short-circuit is the same mechanism
   [pinned above](#composing-classifiers-compose_retry_classifiers-and-rate_limit_aware_classifier)).

       hook = compose_retry_classifiers(
           my_rate_limit_fence,  # claims 429s on your terms
           rate_limit_aware_classifier,  # never consulted for a 429 yours claimed
       )

And the runbook-exact case — retry at exactly the server's `Retry-After`, no
spread — is the default now: an explicit override delay is honored exactly, no
jitter draws over it anywhere on that path (the
[escape note above](#sniffing-the-servers-hint-retry-after-x-retry-after)).
`jitter=0.0` on the declared policy still silences the *computed curve's*
jitter, for fully deterministic suites.

---

## 6. `schedule_to_close` interaction

`schedule_to_close` is the absolute deadline for the entire job lifetime, all attempts combined.

The deadline is not arbitrated by the Python classifier; the retry write itself refuses to
schedule past it, so the database clock is the single authority:
- If `clock_timestamp() + retry_delay` would land past `schedule_to_close`, the job fails
  terminally with `error_class="DeadlineExceeded"` instead of retrying.
- Queued jobs (`pending`/`scheduled`) past the deadline are failed by the leader's deadline
  sweep, and expired pending jobs are never dispatched.

The same guards gate `Snooze` and `RetryAfter` reschedules: any "come back later" that would
land past the deadline fails the job. A **running** attempt is never killed by
`schedule_to_close`; the deadline only arbitrates future dispatches, so an attempt that finishes
after the deadline still succeeds.

This applies to all retry kinds, including `"indefinite"`.

Set at enqueue time via the client:

```python no-exec — not executed: fragment, names bound by an earlier fence
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
either jobs that never give up, or jobs that get cut off mid-retry-budget unexpectedly, so it's
worth being precise:

| | `schedule_to_close` | `start_to_close` |
|---|---|---|
| **Scope** | The job's entire retry lifecycle, across *all* attempts | A single attempt's execution |
| **Type** | `datetime` (an absolute deadline) | `timedelta` (a duration) |
| **Question it answers** | "When should this job give up entirely?" | "How long can one run of this job take before we give up on *it* and try again (or not)?" |
| **Enforced by** | SQL deadline guards on the retry/snooze/retry-after writes, dispatch exclusion of expired jobs, and the leader's deadline sweep (see [above](#6-schedule_to_close-interaction)) | `asyncio.wait_for` wrapped around a single actor invocation, at the consumer level |
| **What happens when it fires** | The job fails permanently (`error_class="DeadlineExceeded"`): no further attempts, regardless of `max_attempts` remaining | That one attempt is cancelled and treated as a `TimeoutError` failure, fed through the normal retry classifier. The job does **not** necessarily stop; it may retry (subject to `schedule_to_close` and the retry policy) or fail permanently if attempts/deadline are exhausted |

In short: `schedule_to_close` is a ceiling on the whole job; `start_to_close` is a ceiling on each
individual attempt. A job can hit its `start_to_close` timeout three times in a row and still
retry a fourth time, as long as `max_attempts` and `schedule_to_close` allow it.

One sync-actor caveat: the timeout cancels the *await*, never a sync `def` actor's executor
thread. Before the retry write re-pends the row, the consumer parks on the thread's tracked
exit handle (bounded by the exit-wait budget) and defers the retry behind the release hold when
the thread outlives that window. The hold is a bound, not a proof, for an actor that outlives
both the budget and the window. See
the no-concurrent-run promise's scoping in [architecture.md](../architecture.md).

### Precedence chain

The effective `start_to_close` for a given attempt is resolved in this order; the first value
found wins:

1. **Per-enqueue override**: `client.enqueue(ref, payload, start_to_close=...)` for that specific
   call.
2. **Actor default**: `@actor(start_to_close=...)` declared on the actor.
3. **Worker-wide fallback**: `WorkerSettings.default_start_to_close` (env var
   `TASKQ_DEFAULT_START_TO_CLOSE`), applied only when neither of the above set anything.
4. **Unbounded**: if nothing anywhere sets a value, the attempt has no execution timeout
   (`None`).

```python no-exec — not executed: fragment, names bound by an earlier fence
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
# 3. Worker-wide fallback: applies only to actors/enqueue calls that set no
#    start_to_close of their own.
export TASKQ_DEFAULT_START_TO_CLOSE=5m
```

**Why set a worker-level default?** `WorkerSettings.default_start_to_close` gives every actor on
that worker a safety-net execution budget per attempt; without it, a hung or infinite-looping
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
- `TimeoutError` and all other exceptions raised by the hook are caught and logged at `WARNING` level; they never propagate to the consumer loop.
- If a `WorkerOwnershipMismatch` occurs during the terminal `mark_failed_or_retry` write, the hook is skipped entirely.

`job_row.payload` is a raw `dict[str, object]`. If you need a typed payload inside the hook, re-validate it:

```python no-exec — not executed: fragment, names bound by an earlier fence
typed_payload = actor_ref.payload_type.model_validate(job_row.payload)
```

The hook is registered via the `@actor` decorator's `on_retry_exhausted` and `on_retry_exhausted_timeout` parameters (see [Non-retryable exceptions](#4-non-retryable-exceptions) above).

### `on_success` hook

The `OnSuccess` type alias is defined in `taskq.retry`:

```python
type OnSuccess = Callable[[JobRow, object], Awaitable[None] | None]
```

When a job succeeds and transitions to `succeeded`, the consumer loop invokes this hook if one is registered. The hook receives the persisted `JobRow` and the actor's result. The result is typed `object` (the consumer loop erases the actor's return type at this boundary); re-validate it via the actor's `result_adapter` if you need a typed value:

```python no-exec — not executed: fragment, names bound by an earlier fence
typed_result = actor_ref.result_adapter.validate_python(job_row.result)
```

Invocation behaviour mirrors `on_retry_exhausted`:
- If the hook returns an awaitable, it is awaited under `asyncio.wait_for` with a timeout of `on_success_timeout` seconds (default `3.0`). Non-coroutine awaitables are detected via `inspect.isawaitable()`.
- `TimeoutError` and all other exceptions raised by the hook are caught and logged at `WARNING` level; they never propagate to the consumer loop.
- The hook runs after the transaction commits but before the success state-change event is published, so a failing hook does not roll back the job.

```python no-exec — not executed: fragment, names bound by an earlier fence
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

`Snooze` never exhausts the retry budget: the reschedule refunds the claim's attempt increment, so `attempt` returns to what it was before the claim, and `max_attempts` never moves. A job can snooze indefinitely; `metadata.snooze_count` exists for visibility rather than the attempt counter.

A negative `delay` raises `ValueError` at construction.

If `now + delay > schedule_to_close`, the backend immediately fails the job with `error_class="DeadlineExceeded"` and `error_message="schedule_to_close reached before next dispatch"` instead of rescheduling. If the row already carries an operator's cancel request, the deferral is refused entirely (the write is a noop) and the cancel ladder terminalises the row; a cancel request on a row whose `schedule_to_close` lapses at deferral time terminalises `cancelled`, not `DeadlineExceeded`: operator intent outranks the deadline.

```python no-exec — not executed: fragment, names bound by an earlier fence
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

- `consume_budget=True` (default): the reschedule is gated by the attempt budget like a normal retry: with `kind="transient"` and no attempts left, the job fails terminally with `error_class="MaxAttemptsExceeded"`.
- `consume_budget=False`: snooze semantics: the reschedule refunds the claim's attempt increment and `max_attempts` never moves, so it can never exhaust the budget.

A negative `delay` raises `ValueError` at construction.

`RetryAfter`'s delay is **not** clamped by `max_retry_backoff`; only `schedule_to_close` gates it. This differs from a `retry_classifier` `RetryOverride(delay=...)`, which *is* clamped (see [§5](#5-retry_classifier-hook-per-instance-retry-overrides)).

```python no-exec — not executed: fragment, names bound by an earlier fence
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
        # Known maintenance window: snooze without consuming retry budget.
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

## 12. Crash vs. shutdown: what happens to the attempt count

An attempt is spent at *claim* time: dispatch increments `attempt` when a worker locks the row. What happens to that spent attempt when the worker goes away mid-execution depends on *how* it went away:

| Event mid-execution | Attempt budget | Audit trail |
|---|---|---|
| **Crash** (SIGKILL, power loss, OOM kill) | **Spent** (the re-run climbs to the next attempt number) | `job_attempts` row with `outcome='crashed'` and `error_class='WorkerCrashed'` (or `'HeartbeatLost'` when the worker is alive but cannot reach Postgres) |
| **Graceful shutdown** (SIGTERM/SIGINT) | **Spent**; the interrupted attempt did start executing, so the re-run claims a fresh attempt number | No attempt row; one `interrupted` state-change event, and `interrupt_count` incremented on the job row |

A crash is indistinguishable from the worker vanishing, so the fleet learns about it from the lease: once `lock_lease` (or the actor's `heartbeat_timeout`) expires without a heartbeat, the leader's reclaim sweep hands the row back through the job's own `RetryPolicy`, the same base/cap/backoff curve as an application-level failure, with jitter derived deterministically from the row's `(job_id, attempt)` so a fleet-wide reclaim does not re-synchronize every job to the same instant. Because the claim already spent the attempt, that hand-back consumes budget: with the default `max_attempts=3`, two crashes on a flaky node leave one attempt; the third crash's reclaim terminally fails the job whose actor body ran at most once. If node crashes should not eat your failure budget, size `max_attempts` for the crashes you expect, or use `kind="indefinite"` with a `time_budget`.

A graceful shutdown is different in mechanism but not in cost: the worker announces it is leaving, in-flight actors observe the cancel event, and any job still running when the grace periods expire is *released* back to the fleet. The interrupted attempt is spent, not refunded: it did start executing, and refunding it would let the re-run share the attempt epoch with the process that is still shutting down. The re-run claims the next attempt number, and `interrupt_count` on the job row carries the aggregate. A deploy therefore trades one attempt of budget for the guarantee that no attempt epoch is ever shared between a dying process and its replacement.

The guarantee has a second leg: the row's `claim_epoch`, a non-saturating internal claim identity bumped by exactly 1 per claim, which every terminal write fences on beside the attempt number, so two executions can never hold the same fence, even once the displayed `attempt` counter parks at the smallint ceiling. See `01.00.18_02_pre_claim_epoch.sql` for the invariant.

The operator controls for how quickly a crash is *detected* are the heartbeat knobs: `heartbeat_interval` (how often a worker proves it is alive), `lock_lease` (how long a claim survives without one: the startup-validated cascade invariant `lock_lease >= max(heartbeat_interval, heartbeat_command_timeout) + (max_heartbeat_failures + 1) × (heartbeat_interval + heartbeat_command_timeout)` (58 s at the defaults) keeps a slow-but-live worker from being reclaimed), and the per-actor `heartbeat_timeout`. Tightening them reclaims crashed work sooner at the cost of reclaiming slow-but-alive workers. See [workers.md](workers.md) for the settings and [ops.md](ops.md) for the `crashed` and `abandoned` state definitions.

---

## 13. Porting a retry budget from another queue library

Attempt counts do not transfer across backoff curves: compare the wall-clock coverage, not the attempt count. TaskQ's default curve is `base × 2^(N-1)` with `base = 5s`, capped at `cap` (default 1 h), with ±20% multiplicative jitter by default; the crash-reclaim path derives the same band deterministically from `(job_id, attempt)`. A steeper curve (a power of the attempt count) or a flatter one (a worker-overridable exponential) covers a very different wall-clock window at the same attempt count: a budget of 25 attempts can span weeks on one curve and roughly 15 hours on TaskQ's default. Decide the wall-clock window you actually want and set `base`/`cap` (or a `time_budget`) for it, rather than copying the attempt count. Budget for crashes too: as [§12](#12-crash-vs-shutdown-what-happens-to-the-attempt-count) covers, a crashed worker spends an attempt just as an actor failure does.

Two further behaviours to know up front. `kind="indefinite"` ignores `max_attempts` entirely: the row still carries the configured value, but it is inert (the admin UI renders it with an `(indefinite)` marker); the only stopping condition is the `schedule_to_close` deadline. And TaskQ has **no dead-letter queue**: a job whose budget is exhausted stays queryable in `jobs` (then `jobs_archive`) with its terminal `error_class`; routing to a real DLQ is your code, at the two designed hook points: `on_retry_exhausted` per actor and the worker's `ErrorReporter`. See [ops.md](ops.md).

---

**See also:**
- [ops.md](ops.md): operations & adoption guide: timeout policy by workload class, failure-classification doctrine, footguns
- `actors.md`: full `@actor` decorator reference.
- `workers.md`: `WorkerSettings.max_retry_backoff`, `WorkerSettings.default_start_to_close`, and other worker-level settings.
- `jobs-clients.md`: `schedule_to_close`, `start_to_close`, and other enqueue-time options.

---

## 14. Workflows: the ladder inside a fan-out

A workflow node's retry ladder is the SAME ladder this guide documents —
the fork declares `max_attempts`/`retry_kind` per fan-out, and the map's
children retry in place (the same row, the same arbiter tuple). The
workflow-specific rules compose on top:

* **Ladder retries emit no workflow terminal** (P3 decision 7): a child
  mid-ladder decrements nothing, cascades nothing, fans in nothing — the
  join's counter moves ONLY at exhaustion. See
  [workflows.md](workflows.md#the-failed-parent-propagation-t06).
* **At exhaustion the failure resolves per the edge's declared policy**:
  `fail_closed` (the default) peer-cancels the running siblings and fails
  the flow; `collect` fans the failure in as a `FailureInfo` item — the
  full attempt history, the estate's `ErrorInfo` envelope — and the join
  fires with the partial result. The two semantics side by side, with the
  peer-cancel record and the skip rule: [workflows.md's failure
  section](workflows.md#the-failed-parent-propagation-t06).
