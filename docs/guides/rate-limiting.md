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
- Primitives must be registered before the worker starts. See [Wiring to actors](#wiring-to-actors).

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

### `registry` singleton

```python
from taskq.ratelimit import registry
```

A module-level `RateLimitRegistry` instance. The DI system used internally by the worker resolves the same object. Primitives registered on this singleton are visible to actors at dispatch time.

**Warning for tests:** use a fresh `RateLimitRegistry()` instance rather than the module-level `registry` singleton in tests, to avoid cross-test contamination. The singleton is shared across the entire test process.

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

The `rate_limits` and `reservations` parameters on `@actor` accept plain `list[str]` name lists. Names are resolved against the registry at dispatch time.

At dispatch time the worker calls `registry.acquire_for_actor()`:

1. Reservations are acquired first, in declaration order.
2. Rate limits are acquired next, in declaration order.
3. If any acquisition is denied, all previously acquired resources are released in reverse order (rollback) and `ReservationUnavailable` is raised.
4. A rate-limited job transitions to `snoozed` status (not failed or retried) and is re-promoted to `pending` when the snooze period expires. You will see `snoozed` in the admin UI for these jobs.
5. After the actor completes, reservation slots are released. Rate-limit tokens are consumed permanently (not refunded).

If `RateLimitDecision.retry_after` is `None` (fixed quota with `refill_per_second=0`), the registry substitutes `DEFAULT_RESERVATION_BACKOFF = timedelta(seconds=5)` before raising `ReservationUnavailable`.

**Queue depth under sustained rate limiting:** Jobs accumulate as `snoozed` under sustained rate-limit pressure. They do not consume retry budget. There is no built-in backpressure beyond `max_pending` on the actor — monitor queue depth via the admin UI or OTel metrics.

Primitives must be registered on `registry` before the worker starts:

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

These are Pydantic models for callers that resolve primitives manually and need structured metadata. The `@actor` decorator stores plain `list[str]` name lists — `RateLimitRef` objects are not accepted by `@actor`, and the `count` field has no effect at dispatch time. The dispatch path always acquires exactly `1.0` token per rate-limit name.

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
        KeyedReservationRef(
            base_name="geocode-session",
            key_fn=lambda payload: payload["session_id"],
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

`key_fn` receives the actor's validated payload as a `dict[str, object]` (the same shape stored
on the job row) and must return a non-empty string. `base_name` namespaces the derived
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
    Concrete per-key reservations are never removed automatically. Under high key cardinality —
    for example, one reservation per customer session over a long-running worker's lifetime —
    the in-memory registry entry count grows without bound unless you prune it.

    Call `RateLimitRegistry.evict_idle_keyed_reservations(idle_for)` periodically from your own
    maintenance code (a scheduled task, an admin CLI command, whatever fits your deployment —
    TaskQ does not schedule this automatically anywhere) to bound registry growth:

    ```python
    from datetime import timedelta
    from taskq.ratelimit import registry

    # e.g. run this once an hour from a cron actor or an external scheduler.
    evicted = registry.evict_idle_keyed_reservations(idle_for=timedelta(hours=1))
    ```

    Eviction only removes the in-memory registry bookkeeping (the registered
    `ConcurrencyReservation` object and its last-used timestamp) for keys that have not been
    acquired within `idle_for`. It does **not** touch the underlying Postgres
    `reservation_slots` rows for that name — those are reclaimed independently by the existing
    lock-expiry sweep. A key that is acquired again after eviction is simply re-registered on
    next use, so calling `evict_idle_keyed_reservations()` is always safe, including while other
    keys are mid-acquisition.

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
