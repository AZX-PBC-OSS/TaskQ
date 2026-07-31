# Rate Limiting

TaskQ provides three rate-limiting primitives backed by Redis, Postgres, or an in-memory store. They compose through a registry and wire to actors by name.

**When to use each primitive:**

| Primitive | Controls | Typical use |
|---|---|---|
| `TokenBucket` | Throughput with burst tolerance | API calls where a short burst is acceptable |
| `SlidingWindow` | Throughput with a rolling time window | Strict per-minute/per-hour limits |
| `ConcurrencyReservation` | Slot-based concurrency | Limiting how many jobs run simultaneously |

---

## Prerequisites

- For Redis backends: install the `taskq-py[redis]` extra (`uv sync --extra redis`).
- For Postgres backends: run `taskq migrate up` to create the `rate_limit_buckets` and `rate_limit_window_entries` tables.
- Primitives referenced **by name** must be registered before the worker starts;
  actor-declared **instances** are registered by the worker at bootstrap. See [Wiring to actors](#wiring-to-actors).

---

## `TokenBucket`

Implements the token-bucket algorithm. The bucket starts full; tokens drain on each `acquire()` and refill continuously at `refill_per_second`. Setting `refill_per_second=0` creates a fixed quota that never refills.

### Constructor

```python
from taskq.ratelimit import TokenBucket
from datetime import timedelta

TokenBucket(
    name: str,
    capacity: float,
    refill_per_second: float,
    backend: Literal["redis", "postgres", "memory"] = "redis",
    ttl: timedelta | None = None,
)
```

| Parameter | Description |
|---|---|
| `name` | Unique bucket name. Used as part of the Redis key and the Postgres `bucket_name` column. |
| `capacity` | Maximum tokens. Must be `> 0`. |
| `refill_per_second` | Token refill rate. Must be `>= 0`. Use `0` for a fixed daily/window quota. |
| `backend` | Storage backend. Default `"redis"`. **`"memory"` is per-process only — state is not shared across worker processes. Not suitable for multi-worker deployments.** |
| `ttl` | Override the Redis key TTL. Default: `ceil(capacity / refill * 2) + 60` seconds. For `refill=0`, defaults to 86 400 s (24 h). |

Raises `ValueError` if `capacity <= 0` or `refill_per_second < 0`.

### `acquire(count=1.0, *, redis_client, pg_pool, clock, settings) -> RateLimitDecision`

Attempts to withdraw `count` tokens. Returns a `RateLimitDecision`. All four keyword arguments default to `None`.

- For `backend="memory"`: only `clock` is required; pass it explicitly.
- For `backend="redis"`: `redis_client`, `clock`, and `settings` are required. `pg_pool` is only used if `TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED=true` and Redis is unreachable.
- For `backend="postgres"`: `pg_pool`, `clock`, and `settings` are required.

If denied, `decision.retry_after` holds how long to wait before trying again (`None` when `refill_per_second=0` — the quota is exhausted with no automatic recovery).

### `refund(decision, *, count, redis_client, pg_pool, clock, settings) -> None`

Returns `count` tokens to the bucket. Used on the rollback path only — do not call after the actor completes successfully. Postgres backend refund is a no-op (logs a warning). GCRA and memory log-style sliding window refunds are also no-ops.

### Example

```python
import asyncio
from datetime import timedelta, UTC, datetime
from taskq.ratelimit import TokenBucket
from taskq.testing.clock import FakeClock

bucket = TokenBucket(
    name="stripe_api",
    capacity=100,
    refill_per_second=10,
    backend="memory",   # use "redis" in production
)

clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

async def main() -> None:
    result = await bucket.acquire(clock=clock)
    if result.allowed:
        print(f"allowed, {result.remaining} tokens left")
    else:
        print(f"denied, retry in {result.retry_after}")

asyncio.run(main())
```

---

## `SlidingWindow`

Implements a sliding-window rate limiter. Two algorithms are available via the `style` parameter.

### Constructor

```python
from taskq.ratelimit import SlidingWindow
from datetime import timedelta

SlidingWindow(
    name: str,
    limit: int,
    window: timedelta,
    backend: Literal["redis", "postgres", "memory"] = "redis",
    style: Literal["log", "gcra"] = "log",
    ttl: timedelta | None = None,
)
```

| Parameter | Description |
|---|---|
| `name` | Unique bucket name. |
| `limit` | Maximum requests within `window`. Must be `>= 1`. |
| `window` | The rolling time window. Must be `> timedelta(0)`. |
| `backend` | Storage backend. Default `"redis"`. **`"memory"` is per-process only — state is not shared across worker processes. Not suitable for multi-worker deployments.** |
| `style` | Algorithm. `"log"` tracks individual request timestamps; `"gcra"` uses a single theoretical-arrival-time cell. |
| `ttl` | Override the Redis key TTL. Default for `"log"`: `2 * window + 60 s`. Default for `"gcra"`: `window + 60 s`. |

Raises `ValueError` if `limit < 1`, `window <= timedelta(0)`, or `style` is not `"log"` or `"gcra"`.

### `SlidingWindowStyle` — `"log"` vs `"gcra"`

**`"log"` (timestamp log):** Stores a timestamped entry for every accepted request in a Redis sorted set (or Postgres `rate_limit_window_entries` table). On each acquire, entries older than the window boundary are evicted, and the remaining count is checked against `limit`. Exact, but memory scales with request volume. Log-style decisions carry a `request_id` that enables rollback via `refund()` (Redis only: calls `ZREM`).

**`"gcra"` (Generic Cell Rate Algorithm):** Stores a single value — the theoretical arrival time (TAT) — in Redis or Postgres. No per-request log. More memory-efficient for high-throughput buckets. Does not support `refund()` (no-op). The `request_id` field is `None` on GCRA decisions.

### `acquire(*, redis_client, pg_pool, clock, settings) -> RateLimitDecision`

All four keyword arguments default to `None`. `clock` is required for all backends and raises `RuntimeError` if not provided.

- For `backend="memory"`: only `clock` is required.
- For `backend="redis"`: `redis_client`, `clock`, and `settings` are required.
- For `backend="postgres"`: `pg_pool`, `clock`, and `settings` are required.

### Log-style example

```python
from datetime import timedelta, UTC, datetime
from taskq.ratelimit import SlidingWindow
from taskq.testing.clock import FakeClock

sw_log = SlidingWindow(
    name="vendor_x_per_min",
    limit=60,
    window=timedelta(minutes=1),
    backend="memory",
    style="log",
)

clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

result = await sw_log.acquire(clock=clock)
print(result.allowed, result.remaining)
```

### GCRA example

```python
sw_gcra = SlidingWindow(
    name="vendor_y_per_min",
    limit=60,
    window=timedelta(minutes=1),
    backend="memory",
    style="gcra",
)

result = await sw_gcra.acquire(clock=clock)
print(result.allowed, result.remaining)
```

---

## `ConcurrencyReservation`

Controls how many jobs can hold a resource simultaneously using pre-allocated slot rows in Postgres (`taskq.reservation_slots`). Slots are acquired with `FOR UPDATE SKIP LOCKED` and held for a configurable `lease` duration. The worker heartbeat loop extends slot leases automatically.

### Constructor

```python
from taskq.ratelimit import ConcurrencyReservation
from datetime import timedelta

ConcurrencyReservation(
    name: str,
    slots: int,
    lease: timedelta | float,
    lock_lease: timedelta | None = None,
    *,
    clock: Clock | None = None,
    schema: str = "taskq",
)
```

| Parameter | Description |
|---|---|
| `name` | Unique reservation name. Must match `[A-Za-z_][A-Za-z0-9_]*`. |
| `slots` | Number of concurrent slots. Must be `>= 1`. |
| `lease` | Duration a slot is held. Accepts `timedelta` or seconds as `float`. Must be `> 0`. |
| `lock_lease` | If provided and `lease < lock_lease`, a warning is logged. Used to detect misconfiguration with the worker lock lease. |
| `clock` | Pass a `Clock` (or `FakeClock`) to use the in-memory backend for testing. If `None`, a real Postgres pool must be provided at acquire time. |
| `schema` | Postgres schema name. Default `"taskq"`. **Must match `TASKQ_SCHEMA_NAME`** when using a non-default schema. Pass `settings.schema_name` from `WorkerSettings.load()`. |

Raises `ValueError` if `slots < 1` or `lease <= 0`. Raises `asyncpg.UndefinedTableError` if the `reservation_slots` table has not been created — run `taskq migrate up` first.

### `acquire(job_id, worker_id, pool=None) -> int`

Acquires a slot, returning the `slot_index`. Raises `ReservationUnavailable` when all slots are held. When `pool=None`, uses the in-memory backend (requires `clock=` at construction).

### `release(slot_index, worker_id, pool=None) -> None`

Releases a slot. No-op if `worker_id` does not match the held worker (prevents accidental double-release).

### `sync_slots(reservations, pool, *, schema="taskq") -> SyncResult`

Module-level function. Synchronises slot rows in Postgres to match the current `slots` configuration — inserts missing rows, deletes excess free rows, and skips rows held by active jobs. Returns a `SyncResult(inserted, deleted, skipped_held)`. Call this after changing slot counts on a running deployment.

```python
from taskq.ratelimit import sync_slots

result = await sync_slots([gpu_reservation], pool=pg_pool)
print(result.inserted, result.deleted, result.skipped_held)
```

### Example

```python
from datetime import timedelta, UTC, datetime
from uuid import uuid4
from taskq.ratelimit import ConcurrencyReservation
from taskq.testing.clock import FakeClock

clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

gpu_res = ConcurrencyReservation(
    name="gpu_slots",
    slots=4,
    lease=timedelta(seconds=60),
    clock=clock,  # omit in production; pass pg_pool to acquire() instead
)

job_id = uuid4()
worker_id = uuid4()

slot_index = await gpu_res.acquire(job_id, worker_id)
try:
    # ... run job ...
    pass
finally:
    await gpu_res.release(slot_index, worker_id)
```

---

## Queue-level concurrency cap

`actor_config.max_concurrent` (set via `@actor(max_concurrent=...)`) caps concurrency per
actor, per worker. There was no way to cap "at most N jobs from queue X, fleet-wide"
independent of which or how many actors publish to that queue. The queue-level concurrency
cap fills this gap.

### Mechanism

Set the `max_concurrent` column on the `"{schema}".queues` table row for a queue:

```sql
UPDATE "taskq".queues SET max_concurrent = 20 WHERE name = 'external-api';
```

The column is nullable — `NULL` means uncapped, matching the `actor_config.max_concurrent`
convention. This is DB configuration, not a decorator argument, deliberately: a per-worker
settings/env-var approach was considered and rejected because it risks configuration drift
across a fleet during rolling deploys (two workers disagreeing on a queue's cap would make
`RateLimitRegistry.register()`'s idempotency check raise `ValueError`). A single
Postgres-resident value read at worker startup avoids that drift, mirroring how `queues.mode`
already works for dispatch fairness.

### At worker startup

For every queue in `settings.queues` with a non-null `max_concurrent`, the worker registers a
`ConcurrencyReservation` named via the internal `queue_concurrency_reservation_name(queue)`
helper (which returns `f"taskq:global:queue:{queue}"`) and pre-allocates its slots — reusing
the exact same distributed, leased-slot machinery as any other `ConcurrencyReservation`
(Postgres `FOR UPDATE SKIP LOCKED`, heartbeat-renewed leases), not a new mechanism.

If the slot-sync step fails, **worker startup crashes loudly** rather than continuing: the cap
is registered before its slot rows are synced, and dispatch has no retry path, so a
warn-and-continue would leave every dispatch on that queue denied until a manual restart. A
crashed worker is retried by the process supervisor, and the sync is idempotent — the next boot
reconciles the rows.

### At dispatch time

If a job's queue has a registered cap, the worker prepends that reservation to the job's
acquire list before running the actor — transparent to actor code, no `@actor` argument
needed. This is the "implicit, not per-actor opt-in" behavior the issue asked for.

### Prior art

This mirrors Oban Pro's `global_limit` with queue partitioning — a fleet-wide cap applied
per-queue rather than opted into per worker/actor.

---

## `RateLimitDecision`

Returned by every `acquire()` call. Frozen dataclass.

```python
@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: float
    retry_after: timedelta | None
    bucket_name: str
    backend: RateLimitBackend
    request_id: str | None = None
```

| Field | Description |
|---|---|
| `allowed` | `True` if the request was granted. |
| `remaining` | Tokens/slots remaining after this call. For GCRA, this is an estimate. |
| `retry_after` | How long to wait before retrying. `timedelta(0)` on allowed decisions. `None` when `refill_per_second=0` and the quota is exhausted — there is no automatic recovery time. |
| `bucket_name` | Name of the rate-limit primitive. |
| `backend` | Which backend processed the request: `"redis"`, `"postgres"`, or `"memory"`. |
| `request_id` | UUID string set on log-style sliding window decisions. Required for `refund()` on the Redis log-style path. `None` for all other primitives and styles. |

**`retry_after` vs `ReservationUnavailable.retry_after`:** `RateLimitDecision.retry_after` can be `None` (when `refill_per_second=0`). `ReservationUnavailable.retry_after` is always a non-`None` `timedelta` — the registry substitutes `DEFAULT_RESERVATION_BACKOFF = timedelta(seconds=5)` before raising, so callers of `acquire_for_actor` never receive a `None` backoff on the exception.

---

## `RateLimitState` (peek)

Returned by `peek()` on all rate-limit primitives. A read-only snapshot of current bucket state — no tokens are consumed.

```python
from taskq.ratelimit.decision import RateLimitState

@dataclass(frozen=True, slots=True)
class RateLimitState:
    bucket_name: str
    backend: RateLimitBackend
    is_exhausted: bool          # True when no tokens/capacity remain
    tokens_remaining: float     # TB only: current tokens after refill
    remaining: float            # SW only: remaining capacity
    retry_after: timedelta | None  # If exhausted, time until next availability
    capacity: float | None      # TB only
    limit: int | None           # SW only
    window: timedelta | None    # SW only
    style: str | None           # SW only: "log" or "gcra"
    refill_per_second: float | None  # TB only
```

### `peek()` usage

```python
# Read current bucket state without consuming tokens
state = await bucket.peek(clock=clock)
print(state.tokens_remaining, state.is_exhausted)
```

For Redis backends, pass `redis_client=..., clock=..., settings=...`. For Postgres backends, pass `pg_pool=..., clock=..., settings=...`. For memory backend, only `clock` is required.

### `reset()` usage

Reset a bucket to full capacity instantly:

```python
await bucket.reset()  # or with DI: redis_client=..., pg_pool=..., settings=...
```

Redis: single `DEL` call. Postgres: `DELETE FROM rate_limit_buckets`. Memory: restore `capacity` and reset timestamp. Idempotent — no error if the bucket doesn't exist.

### Registry-level peek/reset

```python
# Peek all registered rate limits
states = await registry.peek_all(clock=clock)

# Peek a single bucket by name
state = await registry.peek("stripe_api", clock=clock)

# Reset a bucket (available programmatically and via admin UI)
await registry.reset("stripe_api", redis_client=..., settings=...)
```

### Admin UI reset

The `/admin/rate-limits` page shows decoded peek state. A **Reset** button per bucket is available when `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET=true` (default `false`). Resets are CSRF-protected and logged at WARNING level.

---

## Testing

Construct a fresh registry per test — isolation by construction, nothing to
clean up:

```python
import pytest
from taskq.ratelimit import RateLimitRegistry

@pytest.fixture
def rl_registry() -> RateLimitRegistry:
    return RateLimitRegistry()
```

- Unit tests: `backend="memory"` primitives need no Redis/Postgres — pass a
  `FakeClock` via `acquire(clock=...)` (`TokenBucket` / `SlidingWindow`) or
  the `clock=` constructor parameter (`ConcurrencyReservation`).
- Worker-level tests: pass the fresh instance to
  `_main(..., rate_limit_registry=...)`; actor-declared instances
  auto-populate it.
- Calling `acquire_for_actor` directly (no bootstrap) with actor-declared
  instances? Register them first — registration is the worker's job, not the
  acquisition path's. Filter to instances: mixed lists may also contain
  `str` names and keyed refs, which `register()` does not accept:

  ```python
  from taskq.ratelimit import ConcurrencyReservation, SlidingWindow, TokenBucket

  for entry in actor_ref.rate_limits:
      if isinstance(entry, TokenBucket | SlidingWindow):
          rl_registry.register(entry)
  for entry in actor_ref.reservations:
      if isinstance(entry, ConcurrencyReservation):
          rl_registry.register(entry)
  ```

- If you must use the module singleton, `registry.clear()` resets all state
  between tests (test aid only — NOT safe to call while a worker runs).

For full `FakeClock` walkthroughs, see
[Testing Rate Limits](#testing-rate-limits).

---

## Backends

| Backend | Value | Storage | Notes |
|---|---|---|---|
| Redis | `"redis"` | Redis sorted set / hash | Fastest. Requires `taskq-py[redis]` extra and `TASKQ_REDIS_URL`. Atomic Lua scripts prevent race conditions. |
| Postgres | `"postgres"` | `rate_limit_buckets`, `rate_limit_window_entries` | No extra dependencies. Slower; uses `FOR UPDATE` row locks. Also serves as fallback when Redis is unavailable. |
| Memory | `"memory"` | Per-process `asyncio.Lock`-guarded data structure | No external dependencies. State is lost on restart and **not shared across worker processes**. Use in tests and single-process development only. |

!!! warning "Redis backend without the `[redis]` extra"
    Creating a `TokenBucket(backend="redis")` or `SlidingWindow(backend="redis")`
    without the `redis` package installed raises `ImportError` at acquire time with
    a clear install instruction. Use `backend="postgres"` as a zero-dependency
    alternative, or install the extra:

    ```bash
    pip install "taskq-py[redis]"
    ```

### Redis PG fallback

When `backend="redis"` and Redis raises `ConnectionError` or `TimeoutError`, TaskQ automatically retries the acquire against Postgres if `TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED=true` (the default). Set it to `false` to let Redis errors propagate instead.

```
TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED=false
```

The fallback logs a `WARNING` event with `backend="redis"` and `fallback="postgres"` before delegating to Postgres.

---

## `RateLimitRegistry`

The registry holds all registered primitives and exposes AND-composition for actors.

```python
class RateLimitRegistry:
    def register(self, primitive: TokenBucket | SlidingWindow | ConcurrencyReservation) -> None: ...
    def get_rate_limit(self, name: str) -> TokenBucket | SlidingWindow: ...
    def get_reservation(self, name: str) -> ConcurrencyReservation: ...
    async def peek(self, name: str, *, ...) -> RateLimitState: ...
    async def peek_all(self, *, ...) -> dict[str, RateLimitState]: ...
    async def reset(self, name: str, *, ...) -> None: ...
```

- `register()` raises `ValueError` if a primitive with the same name is already registered in the same namespace. `TokenBucket`/`SlidingWindow` and `ConcurrencyReservation` live in separate namespaces, so the same name can be used in both.
- `get_rate_limit()` and `get_reservation()` raise `KeyError` if the name is not found.

### Ownership: where the registry lives

`RateLimitRegistry` is an ordinary object — you can own one:

- **Declare on the actor (primary).** `@actor(rate_limits=[...], reservations=[...])`
  accepts primitive *instances* alongside names and keyed refs:

  ```python
  @actor(
      queue="io",
      rate_limits=[TokenBucket("graph", capacity=300, refill_per_second=5, backend="redis")],
      reservations=[ConcurrencyReservation("sharepoint", slots=4, lease=timedelta(minutes=2))],
  )
  async def sync_binding(payload: SyncPayload) -> None: ...
  ```

  At bootstrap the worker collects every instance declared across its actor
  registry and registers them into its resolved registry before validation
  runs. Same name + identical config re-registered = no-op; same name +
  different config = `ValueError` at startup (fail fast).

- **Own an instance (shared / non-job use).** Construct a
  `RateLimitRegistry()`, `.register()` primitives on it, and pass it to
  `worker_main(..., rate_limit_registry=rl)` and/or
  `create_router(..., rate_limit_registry=rl)`. Use this when a primitive is
  shared outside actor dispatch (a non-job `registry.acquire()` in a FastAPI
  handler, or one primitive referenced by name from many actors). In a
  multi-process deployment, construct a same-configured instance in EACH
  process — Python objects cannot cross process boundaries; the underlying
  limiter state (Redis hashes, PG rows) is shared.

- **Inject via DI (worker bootstrap).** Register the owned instance as a
  **value** provider at `Scope.LOOP` in your `di_registry`; the worker
  resolves it at bootstrap, equivalent to the explicit argument. Factory/
  class providers and non-LOOP scopes raise `TypeError` (dispatch resolves
  the registry from the LOOP-scope cache only), as does passing both the
  argument and a DI provider (ambiguous).

- **The default registry (convenience).** `from taskq.ratelimit import registry`
  is a module-level singleton and remains the default at every entry point:
  import-time `.register()` plus `worker_main(...)` with no
  `rate_limit_registry` argument behaves exactly as before. New code should
  prefer one of the two owned patterns above.

**Decision rule:** default to declaring instances on the actor; use an
explicit registry + `.register()` when a primitive is shared outside
dispatch.

**Warning for tests:** use a fresh `RateLimitRegistry()` instance rather
than the module-level `registry` singleton in tests, to avoid cross-test
contamination. The singleton is shared across the entire test process.

### `acquire()` context manager (non-job code)

For use outside actor dispatch — e.g. in a FastAPI handler that shares a rate limit with job actors:

```python
async with registry.acquire(
    "stripe_api",
    count=1.0,
    clock=clock,
    # For redis backend, also pass: redis_client=..., settings=...
    # For postgres backend, also pass: pg_pool=..., settings=...
) as decision:
    if decision.allowed:
        # proceed
        pass
```

All four keyword arguments (`redis_client`, `pg_pool`, `clock`, `settings`) default to `None`. Pass whichever ones the underlying backend requires (see the backend requirements listed under `TokenBucket.acquire()` above).

Cannot be used with `ConcurrencyReservation` names (raises `TypeError`).

### `acquire_for_actor()` return type

`acquire_for_actor()` returns `list[AcquiredResource]` — a list of handle objects (either `RateLimitHandle` or `ReservationHandle`). It does not return `CompositionResult`. `CompositionResult` is defined in `taskq.ratelimit.composition` as a reserved dataclass for future introspection use; it is not currently returned by any public API.

---

## Wiring to Actors

Attach rate limits and reservations to an actor by name:

```python
from taskq.actor import actor
from pydantic import BaseModel

class SendEmailPayload(BaseModel):
    to: str

@actor(
    rate_limits=["mailgun_per_minute"],
    reservations=["email_slots"],
)
async def send_email(payload: SendEmailPayload) -> None:
    ...
```

You can also declare the primitive **instances** directly — no separate
registration step needed; the worker registers them at bootstrap:

```python
@actor(
    rate_limits=[TokenBucket("mailgun_per_minute", capacity=100, refill_per_second=2, backend="redis")],
    reservations=[ConcurrencyReservation("email_slots", slots=4, lease=timedelta(minutes=2))],
)
async def send_email(payload: SendEmailPayload) -> None:
    ...
```

Names, instances, and keyed refs may be mixed freely in one list.

The `rate_limits` parameter accepts plain names, `TokenBucket` / `SlidingWindow` instances, and/or
`KeyedRateLimitRef` entries; `reservations` accepts plain names, `ConcurrencyReservation`
instances, and/or `KeyedReservationRef` entries. Names are
resolved against the registry at dispatch time. See
[`KeyedRateLimitRef`](#keyedratelimitref-dynamic-per-key-token-buckets) and
[`KeyedReservationRef`](#keyedreservationref-dynamic-per-key-concurrency-caps) for the keyed
variants.

A mixed list of static names and keyed refs is allowed:

```python
from datetime import timedelta
from pydantic import BaseModel
from taskq.actor import actor
from taskq.ratelimit import KeyedRateLimitRef, KeyedReservationRef

class MyPayload(BaseModel):
    tenant_id: str
    session_id: str

@actor(
    rate_limits=["global-bucket", KeyedRateLimitRef.typed(
        MyPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10,
        refill_per_second=1.0,
    )],
    reservations=["global-slots", KeyedReservationRef.typed(
        MyPayload,
        base_name="session-slots",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )],
)
async def my_actor(payload: MyPayload) -> None:
    ...
```

At dispatch time the worker calls `registry.acquire_for_actor()`:

1. Reservations are acquired first, in declaration order.
2. Rate limits are acquired next, in declaration order.
3. If any acquisition is denied, all previously acquired resources are released in reverse order (rollback) and `ReservationUnavailable` is raised.
4. A rate-limited job transitions to `snoozed` status (not failed or retried) and is re-promoted to `pending` when the snooze period expires. You will see `snoozed` in the admin UI for these jobs.
5. After the actor completes, reservation slots are released. Rate-limit tokens are consumed permanently (not refunded).

If `RateLimitDecision.retry_after` is `None` (fixed quota with `refill_per_second=0`), the registry substitutes `DEFAULT_RESERVATION_BACKOFF = timedelta(seconds=5)` before raising `ReservationUnavailable`.

**Queue depth under sustained rate limiting:** Jobs accumulate as `snoozed` under sustained rate-limit pressure. They do not consume retry budget. There is no built-in backpressure beyond `max_pending` on the actor — monitor queue depth via the admin UI or OTel metrics.

Primitives referenced **by name** must be registered before the worker starts (actor-declared
**instances** are registered by the worker at bootstrap instead — see
[Ownership](#ownership-where-the-registry-lives)). Registration on the module-level `registry`
singleton looks like:

```python
from taskq.ratelimit import registry, TokenBucket, SlidingWindow, ConcurrencyReservation
from datetime import timedelta

registry.register(TokenBucket(
    name="mailgun_per_minute",
    capacity=100,
    refill_per_second=100 / 60,
    backend="redis",
))
registry.register(SlidingWindow(
    name="mailgun_sliding",
    limit=1000,
    window=timedelta(hours=1),
    backend="redis",
    style="log",
))
registry.register(ConcurrencyReservation(
    name="email_slots",
    slots=5,
    lease=timedelta(seconds=120),
))
```

---

## `RateLimitRef` and `ReservationRef`

`RateLimitRef` and `ReservationRef` are typed name-reference helpers defined in `taskq.ratelimit.refs`:

```python
from taskq.ratelimit import RateLimitRef, ReservationRef

ref = RateLimitRef(name="stripe_api", count=2.0)
res_ref = ReservationRef(name="gpu_slots")
```

These are Pydantic models for callers that resolve primitives manually and need structured metadata. The `@actor` decorator accepts `list[str | KeyedRateLimitRef]` for `rate_limits` and `list[str | KeyedReservationRef]` for `reservations` — `RateLimitRef` objects are not accepted by `@actor`, and the `count` field has no effect at dispatch time. The dispatch path always acquires exactly `1.0` token per rate-limit name.

---

## `KeyedReservationRef` — dynamic per-key concurrency caps

A static `reservations=["name"]` entry caps concurrency globally: every job that declares it
competes for the same fixed pool of slots. Some workloads need a cap that is scoped to a value
computed from the job's own payload — e.g. capping total concurrent calls to an external API
globally *and* capping concurrent calls per customer session, so that one noisy session can't
starve every other session even though the global cap has room to spare.

`KeyedReservationRef` (from `taskq.ratelimit`) does this by deriving a concrete reservation name
per job from the validated payload, layered on top of — not instead of — a static reservation:

```python
from datetime import timedelta
from pydantic import BaseModel
from taskq.actor import actor
from taskq.ratelimit import registry, ConcurrencyReservation, KeyedReservationRef


class GeocodeRequest(BaseModel):
    session_id: str
    address: str


registry.register(ConcurrencyReservation(
    name="geocode-global",
    slots=20,
    lease=timedelta(minutes=2),
))


@actor(
    reservations=[
        "geocode-global",
        KeyedReservationRef.typed(
            GeocodeRequest,
            base_name="geocode-session",
            key_fn=lambda p: p.session_id,
            slots=3,
            lease=timedelta(minutes=5),
        ),
    ],
)
async def geocode_address(payload: GeocodeRequest) -> None:
    # At most 20 concurrent geocode calls across all sessions, AND at most 3
    # concurrent geocode calls for any single session_id.
    ...
```

`key_fn` receives the actor's validated Pydantic model and must return a non-empty string.
The model has defaults, aliases, and validation applied. Use the `.typed()` classmethod for
compile-time type checking of `key_fn` against the payload model:

```python
from datetime import timedelta
from pydantic import BaseModel, Field
from taskq.ratelimit import KeyedReservationRef

class GeocodeRequest(BaseModel):
    session_id: str = Field(alias="sessionId")  # wire name differs from attribute

KeyedReservationRef.typed(
    GeocodeRequest,
    base_name="geocode-session",
    key_fn=lambda p: p.session_id,  # p is GeocodeRequest — typed, alias applied
    slots=3,
    lease=timedelta(minutes=5),
)
```

The `payload_type` field (required) declares the model class the registry validates the
payload against before calling `key_fn`. Pydantic defaults populate fields absent from the
serialized payload, aliases map wire names to model attributes, and validation errors surface
as `PayloadValidationError` (a non-retryable payload error) rather than `KeyError` (a limiter fault).
`base_name` namespaces the derived
reservations — the concrete name registered for a given key is `f"{base_name}:{key}"` — so
distinct `KeyedReservationRef` declarations never collide. `slots` and `lease` apply identically
to every key derived from a given ref; use a separate `KeyedReservationRef` if different keys
need different caps.

### Lazy registration and reuse

The concrete `ConcurrencyReservation` for a given key is registered the first time that key is
seen, and reused for every subsequent job with the same key — it is not re-created on every
dispatch. Registration is idempotent for identical config, which every acquisition for a given
`KeyedReservationRef` always produces (its `slots`/`lease` are fixed).

!!! warning "Registry growth under high key cardinality"
    Concrete per-key reservations are registered lazily and, absent eviction, never removed.
    Under high key cardinality — for example, one reservation per customer session over a
    long-running worker's lifetime — the in-memory registry entry count would grow without
    bound.

    Eviction is scheduled for you: every worker's 30-second sweep calls
    `RateLimitRegistry.evict_idle_keyed_reservations(idle_for=...)` against its own registry
    (process-local, not leader-gated) with a 1-hour idle threshold (`_KEYED_IDLE_THRESHOLD`).
    Call it directly — against the registry your workers use — only for custom eviction
    windows, e.g. a shorter `idle_for` from your own maintenance code (a scheduled task, an
    admin CLI command, whatever fits your deployment):

    ```python
    from datetime import timedelta
    from taskq.ratelimit import registry

    # e.g. a tighter 15-minute window, run hourly from a cron actor or external
    # scheduler — on your owned instance if you pass one to worker_main().
    evicted = registry.evict_idle_keyed_reservations(idle_for=timedelta(minutes=15))
    ```

    Eviction only removes the in-memory registry bookkeeping (the registered
    `ConcurrencyReservation` object and its last-used timestamp) for keys that have not been
    acquired within `idle_for`. It does **not** touch the underlying Postgres
    `reservation_slots` rows for that name — those are reclaimed independently by the existing
    lock-expiry sweep. A key that is acquired again after eviction is simply re-registered on
    next use, so calling `evict_idle_keyed_reservations()` is always safe, including while other
    keys are mid-acquisition.

---

## Migrating from dict `key_fn` (pre-1.0)

If you have existing keyed-ref declarations from before 1.0, update them as follows:

1. **Add `payload_type`** — pass the actor's payload model class as the first argument to `.typed()`:

   ```python
   from datetime import timedelta
   from pydantic import BaseModel
   from taskq.ratelimit import KeyedRateLimitRef

   class MyPayload(BaseModel):
       tenant_id: str

   # BEFORE (broken):
   KeyedRateLimitRef(base_name="api-per-tenant", key_fn=lambda p: p["tenant_id"], capacity=10, refill_per_second=1.0)

   # AFTER:
   KeyedRateLimitRef.typed(MyPayload, base_name="api-per-tenant", key_fn=lambda p: p.tenant_id, capacity=10, refill_per_second=1.0)
   ```

2. **Change dict access to model-attribute access** — `p["tenant_id"]` → `p.tenant_id`. Pydantic defaults, aliases, and validators are now applied before `key_fn` runs.

3. **Aliases are transparent** — `Field(alias="tenantId")` maps the wire name to the model attribute. `key_fn` uses `p.tenant_id`, not `p["tenantId"]`.

4. **Budget reset hazard:** if you change a model default or alias that affects a key-deriving field, existing concrete bucket names in Redis/PG will differ from new ones. Drain affected queues before deploying such changes.

---

## `KeyedRateLimitRef` — dynamic per-key token buckets

A static `rate_limits=["name"]` entry caps request rate globally: every job that declares it
draws from the same token bucket. Some workloads need a rate limit scoped to a value computed
from the job's own payload — e.g. capping API calls per tenant so that one noisy tenant can't
exhaust a shared budget even though the global cap has room to spare.

`KeyedRateLimitRef` (from `taskq.ratelimit`) does this by deriving a concrete `TokenBucket`
per job from the validated payload, layered on top of — not instead of — a static rate limit:

> **Concurrency caps vs. rate limits.** A reservation (`ConcurrencyReservation` /
> `KeyedReservationRef`) bounds *how many jobs run at once*. A token bucket (`TokenBucket` /
> `KeyedRateLimitRef`) bounds *how many requests per unit time*. N concurrent slots with fast
> responses can still burst well past a per-second budget, so the two primitives answer
> different questions and are often needed together on the same actor.

```python
from pydantic import BaseModel
from taskq.actor import actor
from taskq.ratelimit import registry, TokenBucket, KeyedRateLimitRef


class ApiRequest(BaseModel):
    tenant_id: str
    endpoint: str


registry.register(TokenBucket(
    name="api-global",
    capacity=100,
    refill_per_second=10,
    backend="redis",
))


@actor(
    rate_limits=[
        "api-global",
        KeyedRateLimitRef.typed(
            ApiRequest,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10,
            refill_per_second=1.0,
        ),
    ],
)
async def call_external_api(payload: ApiRequest) -> None:
    # At most 100 burst / 10/sec globally, AND at most 10 burst / 1/sec
    # for any single tenant_id.
    ...
```

`key_fn` receives the actor's validated Pydantic model and must return a non-empty string.
The model has defaults, aliases, and validation applied. Use the `.typed()` classmethod for
compile-time type checking of `key_fn` against the payload model:

```python
from pydantic import BaseModel, Field
from taskq.ratelimit import KeyedRateLimitRef

class ApiRequest(BaseModel):
    tenant_id: str = Field(alias="tenantId")  # wire name differs from attribute

KeyedRateLimitRef.typed(
    ApiRequest,
    base_name="api-per-tenant",
    key_fn=lambda p: p.tenant_id,  # p is ApiRequest — typed, alias applied
    capacity=10,
    refill_per_second=1.0,
)
```

The `payload_type` field (required) declares the model class the registry validates the
payload against before calling `key_fn`. Pydantic defaults populate fields absent from the
serialized payload, aliases map wire names to model attributes, and validation errors surface
as `PayloadValidationError` (a non-retryable payload error) rather than `KeyError` (a limiter fault).
`base_name` namespaces the derived buckets —
the concrete name registered for a given key is `f"{base_name}:{key}"` — so distinct
`KeyedRateLimitRef` declarations never collide. `capacity` and `refill_per_second` apply
identically to every key derived from a given ref; use a separate `KeyedRateLimitRef` if
different keys need different budgets. The optional `backend` field (default `"redis"`)
controls which storage backend the materialized `TokenBucket` uses, mirroring the `backend`
constructor parameter on a static `TokenBucket` — set `backend="postgres"` or
`backend="memory"` in deployments without Redis configured.

### Lazy registration and reuse

The concrete `TokenBucket` for a given key is registered the first time that key is seen, and
reused for every subsequent job with the same key — it is not re-created on every dispatch.
Registration is idempotent for identical config, which every acquisition for a given
`KeyedRateLimitRef` always produces (its `capacity`/`refill_per_second` are fixed).

Unlike keyed reservations, there is no PG slot pre-allocation step — a `TokenBucket` is
immediately usable after `register()` (there is no `ensure_slots` equivalent). The bucket is
ready as soon as it is registered.

**Admin UI visibility.** Each freshly materialized keyed bucket is also published to the
`rate_limit_buckets` table (best-effort, idempotent) on first acquisition, so the
`/admin/rate-limits` page surfaces it alongside statically registered buckets — including in a
standalone `taskq ui serve` deployment whose in-process registry never dispatches jobs. When
Redis is configured, the page also fetches the live per-key state (tokens / GCRA TAT) for these
PG-published rows. A publish failure only logs a warning; it never fails the acquisition.

!!! warning "Concrete-name collisions"
    Because `:` is an allowed character in keys, one ref's `base_name` can be a prefix of
    another ref's concrete name: `base_name="a"` with key `"b:c"` and `base_name="a:b"` with
    key `"c"` both resolve to `a:b:c`. If two refs collide with **identical** configs they
    silently share the primitive (bounded, but probably not what you meant); with
    **different** configs the second resolution raises `ValueError` rather than silently
    over- or under-admitting relative to one ref's declared config. Choose distinct
    `base_name`s to avoid ambiguity. The same rule applies to `KeyedReservationRef`.
    A concrete name you registered **statically** (via `registry.register(...)`) is reused
    as-is when a keyed ref resolves to it, and is never removed by idle-key eviction.

### Bounding registry growth

Concrete per-key `TokenBucket` instances are registered lazily and, absent eviction, never
removed. Under high key
cardinality — for example, one bucket per tenant over a long-running worker's lifetime — the
in-memory registry dict grows without bound unless pruned. Three distinct, complementary
mechanisms bound this growth:

1. **Per-worker sweep eviction.** Every worker's 30-second sweep calls
   `RateLimitRegistry.evict_idle_keyed_rate_limits(idle_for=...)` against its own registry —
   eviction is process-local bookkeeping and is deliberately not leader-gated (a non-leader's
   registry would otherwise receive no periodic eviction), so this always runs in any topology
   capable of materializing keyed primitives in the first place. The default idle threshold is
   1 hour (`_KEYED_IDLE_THRESHOLD`).

2. **Opportunistic eviction on the acquisition path.** When `_resolve_rate_limit_name` would
   otherwise deny a new key because the `max_keyed_rate_limits` cap has been reached, it
   first attempts an opportunistic eviction of idle entries — so hitting the cap is never
   purely an artefact of sweep timing. Only if the cap is still exceeded after the
   opportunistic eviction does the method raise `ReservationUnavailable`. The scan is
   amortized to at most one per 30 seconds (`_OPPORTUNISTIC_EVICT_MIN_INTERVAL`), so a
   registry sitting at its cap under sustained denials stays O(1) per request rather than
   rescanning the whole tracking dict on every denied acquisition.

3. **Redis TTL (self-bounding Redis memory).** Independently of the in-process registry,
   when the underlying `TokenBucket` uses the Redis backend, each key's Redis hash has its own
   `EXPIRE` TTL set by the Lua script (computed from `capacity`/`refill_per_second`). Redis
   memory is therefore self-bounding regardless of the in-process registry dict — even if
   eviction has not yet run, stale keys expire in Redis on their own schedule.

These are three independent bounds, not one mechanism: the sweep and opportunistic eviction
bound the Python-process-local registry dict; the Redis TTL bounds Redis memory.

!!! note "Memory fixed-quota buckets are exempt from idle eviction"
    A `backend="memory"` bucket with `refill_per_second=0` that has consumed any of its quota
    is **not** idle-evicted (neither by the per-worker sweep nor by opportunistic eviction): its
    token state lives only on the in-process bucket instance, so eviction would silently reset
    the drained quota to full — whereas the Redis backend deliberately retains that same state
    for 24h. The trade-off is deliberate: such buckets count against `max_keyed_rate_limits`
    until their quota returns to full (refund/reset) or the process restarts, so under
    sustained high-cardinality fixed-quota keys the cap can fill permanently and deny *new*
    keys. The cap fails closed rather than silently resetting quotas. Buckets that are full
    (no quota consumed) and refilling buckets are evicted normally — the latter self-heal
    because their state converges back toward full on its own.

!!! note "Independent caps for keyed reservations and keyed rate limits"
    `settings.max_keyed_reservations` (default `10_000`) governs keyed
    **reservations** only, and `settings.max_keyed_rate_limits` (default
    `10_000`) governs keyed **rate limits** only. Each kind is tracked
    against its own independent counter and cap, so a high cardinality of
    one kind cannot starve the other.

!!! note "Redis-unavailable behavior is identical to a static bucket"
    A keyed bucket is a plain `TokenBucket` under the hood — `_resolve_rate_limit_name`
    constructs it with the `backend` from the `KeyedRateLimitRef` (default `"redis"`) and calls
    its normal `.acquire()`. The existing `with_pg_fallback` path in
    `token_bucket._acquire_redis_wrapped` (see `src/taskq/ratelimit/_redis_utils.py`) is
    therefore inherited automatically: on Redis `ConnectionError`/`TimeoutError`, the acquire
    falls back to the PG `rate_limit_buckets` table governed by
    `settings.rate_limit_pg_fallback_enabled`. No second fallback mechanism is built or needed —
    a keyed bucket behaves identically to a static bucket when Redis is unavailable, with zero
    special-casing. In a deployment without Redis configured at all, set
    `backend="postgres"` or `backend="memory"` on the `KeyedRateLimitRef` to avoid the
    Redis-required failure mode (a `"redis"` backend with no `redis_client` raises
    `RuntimeError` on acquire, which is not caught by `with_pg_fallback`).

---

## Complete Setup Example

```python
# actors.py
from pydantic import BaseModel
from datetime import timedelta
from taskq.actor import actor
from taskq.ratelimit import registry, TokenBucket, SlidingWindow, ConcurrencyReservation
from taskq.settings import TaskQSettings

_tq_schema = TaskQSettings.load().schema_name

# 1. Define and register primitives.
registry.register(TokenBucket(
    name="stripe_calls",
    capacity=100,
    refill_per_second=10,
    backend="redis",
))
registry.register(SlidingWindow(
    name="stripe_hourly",
    limit=3600,
    window=timedelta(hours=1),
    backend="redis",
    style="gcra",
))
registry.register(ConcurrencyReservation(
    name="stripe_concurrent",
    slots=8,
    lease=timedelta(seconds=30),
    schema=_tq_schema,  # Must match TASKQ_SCHEMA_NAME
))

# 2. Wire to actor.
class ChargePayload(BaseModel):
    amount: int
    currency: str

@actor(
    queue="payments",
    rate_limits=["stripe_calls", "stripe_hourly"],
    reservations=["stripe_concurrent"],
)
async def charge_card(payload: ChargePayload) -> None:
    # This actor runs at most 8 concurrently, at most 100 burst calls,
    # at most 3600 per hour.
    ...
```

```python
# worker.py
from taskq.worker.run import worker_main
from taskq.settings import WorkerSettings
import actors  # noqa: F401 — registers primitives as a side effect

settings = WorkerSettings.load()
exit_code = worker_main(
    settings=settings,
    actor_registry={"charge_card": charge_card},
)
```

---

## Testing Rate Limits

Use `backend="memory"` and `FakeClock` for fully deterministic tests with no external dependencies.

```python
from datetime import UTC, datetime, timedelta
from taskq.ratelimit import TokenBucket
from taskq.testing.clock import FakeClock

START = datetime(2025, 1, 1, tzinfo=UTC)

async def test_token_bucket_refill() -> None:
    tb = TokenBucket(
        name="test",
        capacity=10,
        refill_per_second=10,
        backend="memory",
    )
    clock = FakeClock(START)

    # Drain the bucket.
    for _ in range(10):
        r = await tb.acquire(clock=clock)
        assert r.allowed

    # 11th acquire is denied.
    r = await tb.acquire(clock=clock)
    assert not r.allowed
    assert r.retry_after is not None

    # Advance clock 1 second — 10 tokens refill.
    clock.advance(timedelta(seconds=1))
    r = await tb.acquire(clock=clock)
    assert r.allowed
```

`FakeClock` is importable from `taskq.testing.clock`. `clock.advance(delta)` moves the clock forward without sleeping. Backward steps are safe; the implementation clamps elapsed time to zero.

For sliding window tests:

```python
from taskq.ratelimit import SlidingWindow

async def test_sliding_window_deny_then_allow() -> None:
    sw = SlidingWindow(
        name="test_sw",
        limit=5,
        window=timedelta(seconds=10),
        backend="memory",
        style="log",
    )
    clock = FakeClock(START)

    for _ in range(5):
        r = await sw.acquire(clock=clock)
        assert r.allowed

    r = await sw.acquire(clock=clock)
    assert not r.allowed

    # Advance past the window.
    clock.advance(timedelta(seconds=10, milliseconds=1))
    r = await sw.acquire(clock=clock)
    assert r.allowed
```

---

## `ReservationUnavailable`

Raised by `ConcurrencyReservation.acquire()` and `RateLimitRegistry.acquire_for_actor()` when a slot or rate-limit token cannot be acquired.

```python
from taskq.exceptions import ReservationUnavailable
from taskq.constants import DEFAULT_RESERVATION_BACKOFF
```

| Attribute | Type | Description |
|---|---|---|
| `bucket_name` | `str` | Name of the primitive that denied the request. |
| `retry_after` | `timedelta` | How long to wait. Always a non-`None` `timedelta >= timedelta(0)`. |

`DEFAULT_RESERVATION_BACKOFF` is `timedelta(seconds=5)`. The registry substitutes it when `RateLimitDecision.retry_after` is `None` (fixed quota exhausted with `refill_per_second=0`). Do not use a truthiness coalesce (`x or DEFAULT_RESERVATION_BACKOFF`) to compute the backoff — `timedelta(0)` is falsy and would be incorrectly replaced.
