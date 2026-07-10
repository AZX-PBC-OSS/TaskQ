# Testing

TaskQ ships a dedicated `taskq.testing` package with deterministic fakes,
pytest fixtures, OTel helpers, chaos wrappers, and assertion utilities.
Unit tests run against `InMemoryBackend` with a `FakeClock` — no Postgres,
no Redis, no sleeping. Integration tests use `testcontainers` to spin up real
Postgres 18 and Redis 7.4 containers.

Every symbol in `taskq.testing` lives outside the production import path so
application code never pulls in test-only helpers.

---

## Contents

1. [InMemoryBackend](#inmemorybackend)
2. [FakeClock](#fakeclock)
3. [run_until_drained](#run_until_drained)
4. [Pytest fixtures](#pytest-fixtures)
5. [OTel test utilities](#otel-test-utilities)
6. [Assertions](#assertions)
7. [Chaos testing](#chaos-testing)
8. [Property-based testing](#property-based-testing)
9. [Integration tests](#integration-tests)

---

## InMemoryBackend

`InMemoryBackend` is a single-threaded, in-memory implementation of the
`Backend` protocol. It simulates the full enqueue → dispatch → execute →
terminal-write cycle including `unique_for` dedup, singleton enforcement,
`max_pending` backpressure, retry/snooze transitions, cancellation, sweeps,
schedule CRUD, and archive/expiry simulation.

```python
from datetime import datetime, timezone
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.clock import FakeClock

clock = FakeClock(start=datetime.now(timezone.utc))
backend = InMemoryBackend(clock=clock)
```

Two `InMemoryBackend` instances in the same process are fully isolated — all
state is per-instance, never module-level. Single-threaded by contract: do
not share across threads or event loops.

### Registering actor stubs

`run_until_drained` executes jobs by calling registered **stubs**, not the
real actor handlers. A stub is a callable that receives `(payload: dict, ctx)`
and returns a result dict (or raises a control-flow exception):

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


async def test_double_value() -> None:
    clock = FakeClock(start=datetime.now(timezone.utc))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    # The stub receives payload as a dict and ctx as a minimal duck-typed
    # object with job_id, attempt, payload, and cancel_event.
    backend.register_stub(
        double_value.name,
        lambda payload, ctx: {"doubled": payload["value"] * 2},
    )

    handle = await client.enqueue(double_value, MyPayload(value=21))
    await backend.run_until_drained()
    result = await handle.wait()
    assert result.doubled == 42
```

`register_stub` also accepts `retry`, `non_retryable_exceptions`,
`on_retry_exhausted`, `on_retry_exhausted_timeout`, and `payload_type` to
match the actor's configured behaviour.

### Direct invocation (no queue)

For actors with no DI dependencies, call the `ActorRef` directly to bypass
the queue entirely:

```python
async def test_actor_direct() -> None:
    result = await double_value(MyPayload(value=21))
    assert result.doubled == 42
```

This runs the real handler in-process and is the simplest option when you do
not need to test enqueue/dispatch behaviour. See
[Actors — direct invocation](actors.md#direct-invocation-__call__).

### Actor-config registration

When using `JobsClient` with `InMemoryBackend`, register actor configs so the
backend knows each actor's `max_concurrent`, `queue`, and `metadata`:

```python
from taskq.testing.pg import DEFAULT_ACTORS

backend = InMemoryBackend(clock=clock)
for cfg in DEFAULT_ACTORS:
    backend.register_actor_config(actor=cfg.actor)
```

The `memory_jobs` fixture (below) does this automatically.

### Archive and expiry simulation

`InMemoryBackend` exposes synchronous archive/expiry methods that mirror the
maintenance leader's prune (Sweep 5) and archive-expiry (Sweep 6) sweeps:

```python
from datetime import timedelta

result = backend.archive_terminal_jobs(
    retention=timedelta(days=30),
    archive_retention=timedelta(days=365),
)
expired = backend.expire_archived_jobs()
archived_row = await backend.get_archived(job_id)
```

---

## FakeClock

`FakeClock` is a deterministic clock for tests. It lives in
`taskq.testing.clock` (not `taskq.backend.clock`) so production imports stay
clean.

```python
from datetime import UTC, datetime, timedelta
from taskq.testing.clock import FakeClock

clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

clock.advance(timedelta(seconds=1))   # move forward 1s
clock.move_to(datetime(2025, 1, 2, tzinfo=UTC))  # jump to an instant
clock.now()       # → datetime(2025, 1, 2, tzinfo=UTC)
clock.monotonic() # → elapsed seconds since an internal epoch (always non-zero)
```

`advance(delta)` adds `delta` to the clock; `move_to(when)` sets it to an
absolute instant. Backward steps are safe — elapsed-time guards clamp to zero.
`monotonic()` returns elapsed seconds from a fixed epoch so duration guards
see a plausible non-zero starting value.

Inject `FakeClock` anywhere a `Clock` is accepted: `InMemoryBackend`,
`JobsClient`, rate-limit primitives with `backend="memory"`, and
`ConcurrencyReservation(clock=clock)`.

---

## run_until_drained

`backend.run_until_drained()` is the test-only entry point that drives the
dispatch-then-execute loop to completion. The loop:

1. Promotes `scheduled → pending`.
2. Dispatches the next highest-priority job (`dispatch_batch` with `limit=1`).
3. If nothing is dispatchable but future-scheduled jobs exist, advances the
   `FakeClock` to the earliest `scheduled_at` and continues.
4. Delegates per-job execution to `consume_one_job`, which handles `Snooze`,
   `RetryAfter`, `ReservationUnavailable`, generic exceptions, cancellation,
   and success.
5. Terminates when no jobs are pending, running, or scheduled.

```python
await backend.run_until_drained()
```

!!! note "Stubs are required"
    `run_until_drained` raises `RuntimeError` if it dispatches a job whose
    actor has no registered stub. Register stubs with `backend.register_stub()`
    before calling it.

When the clock is a `FakeClock`, the loop auto-advances through snoozes and
scheduled jobs so a single call drains the entire queue. With a real clock
(non-test) the loop returns instead of advancing — `run_until_drained` is
intended for tests only.

---

## Pytest fixtures

Pytest fixtures live in `taskq.testing.fixtures` and are imported into
`tests/conftest.py` so they are available to all test modules. They are **not**
re-exported from `taskq.testing.__init__` to avoid importing `pytest` and
`asyncpg` at the top level.

### Unit-test fixtures (no containers)

| Fixture | Scope | Yields | Notes |
|---|---|---|---|
| `memory_jobs` | function | `InMemoryBackend` | Fresh backend with a `FakeClock` at `2025-01-01 UTC`. Default actors pre-registered. |
| `actor_runner` | function | `ActorRunnerCallable` | Callable that builds a synthetic `JobContext` and invokes `actor_fn(payload, ctx)`. Forwards `**deps` as DI kwargs. |

```python
async def test_with_memory_jobs(memory_jobs: InMemoryBackend) -> None:
    memory_jobs.register_stub("my_actor", lambda p, ctx: {"ok": True})
    ...
```

### Integration fixtures (Postgres + Redis containers)

| Fixture | Scope | Yields | Notes |
|---|---|---|---|
| `pg_container` | session | `PostgresContainer` | Postgres 18 Alpine, `max_connections=1000`. Defined in `conftest.py`. |
| `pg_dsn` | session | `str` | Asyncpg-friendly DSN. |
| `settings` | function | `TaskQSettings` | Per-test env via `monkeypatch`. |
| `pg_conn` | function | `asyncpg.Connection` | Drops schema before each test. Prefer `clean_pg_conn`. |
| `jobs_app` | function | `JobsApp(deps, backend)` | Drops/migrates/seeds a per-test schema. |
| `module_pg_schema` | module | `ModulePgSchema` | Per-file schema (hashed name). Migrates + seeds once. |
| `module_pg_pool` | module | `asyncpg.Pool` | Shared pool on the module schema. |
| `module_jobs_app` | module | `JobsApp` | Shared `WorkerDeps` + `PostgresBackend`. |
| `clean_pg_conn` | function | `asyncpg.Connection` | Truncates + re-seeds within the module schema. |
| `clean_jobs_app` | function | `JobsApp` | Truncate + re-seed, then open `WorkerDeps` + backend. |
| `worker_with_running_job` | function | `(worker_id, job_id, conn)` | Pre-created worker + running job on `clean_pg_conn`. |
| `redis_container` | session | `RedisContainer` | Redis 7.4 Alpine. |
| `redis_url` | function | `str` | Per-test URL on the session container (db 0). |
| `module_redis_url` | module | `str` | Unique Redis DB (1–15) per module. `FLUSHDB` on teardown. |
| `clean_redis_url` | function | `str` | `FLUSHDB` before each test. |
| `clean_redis_client` | function | `redis.asyncio.Redis` | Fresh async client on the module DB. |
| `backend_pair` | function | `Backend` | Parametrised `["memory", "pg"]`. The `pg` branch skips unless `@pytest.mark.integration` is set. |

```python
import pytest

@pytest.mark.integration
async def test_pg_backend(clean_jobs_app: JobsApp) -> None:
    backend = clean_jobs_app.backend
    ...
```

`JobsApp` is a named tuple — access fields as `jobs_app.deps` and
`jobs_app.backend` rather than unpacking.

---

## OTel test utilities

`taskq.testing.otel` provides a self-contained OTel stack so individual tests
do not need to set up providers, exporters, or patching themselves. These
helpers require the `[otel]` extra (`opentelemetry-sdk`).

### Test tracer

`setup_tracer` creates an in-process `TracerProvider` backed by
`ListSpanExporter` and patches `obs.get_tracer` for the duration of the test:

```python
import pytest
from opentelemetry import trace
from taskq.testing.otel import setup_tracer, ListSpanExporter

async def test_span_emitted(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, exporter = setup_tracer(monkeypatch)

    # ... run enqueue or dispatch ...

    consumer = exporter.span_named("process my_actor")
    assert consumer is not None
    assert consumer.kind == trace.SpanKind.CONSUMER
```

`ListSpanExporter` query helpers:

| Method | Returns |
|---|---|
| `span_named(name)` | First `ReadableSpan` with that name, or `None` |
| `spans_named(name)` | All `ReadableSpan` objects with that name |
| `events_on(span_name, event_name)` | All events named `event_name` on the first span named `span_name` |
| `spans_with_kind(kind)` | All spans with the given `SpanKind` |

### Test meter

`setup_meter` creates a per-test `MeterProvider` backed by
`InMemoryMetricReader` and patches the four core dispatch-path instruments:

```python
from taskq.testing.otel import setup_meter, counter_value, histogram_points

async def test_metrics_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = setup_meter(monkeypatch)

    # ... run enqueue and dispatch ...

    assert counter_value(reader, "messaging.client.published.messages") >= 1
    assert counter_value(reader, "messaging.client.consumed.messages") >= 1
    assert len(histogram_points(reader, "messaging.process.duration")) >= 1
```

Metric query helpers:

| Helper | Returns |
|---|---|
| `collect_metrics(reader)` | All `Metric` objects from the reader |
| `counter_value(reader, name)` | Summed integer value for a counter |
| `counter_data_points(reader, name)` | `list[NumberDataPoint]` for a counter |
| `histogram_points(reader, name)` | `list[HistogramDataPoint]` for a histogram |

### Autouse fixtures

`taskq.testing.otel` exports two `autouse` pytest fixtures imported into
`conftest.py`:

- `_otel_enabled_guard` — snapshots and restores `_otel_enabled` around each test.
- `_logging_configured_guard` — resets structlog configuration and removes
  `ProcessorFormatter` handlers around each test.

These run automatically for any test that imports from `taskq.testing.otel`.
See [Observability — testing observability](observability.md#6-testing-observability)
for the full trace-context-propagation pattern.

---

## Assertions

`taskq.testing.assertions` provides behavioral assertions that query
observable state (job rows, events, spans) rather than implementation
details.

### Job-status assertions

```python
from taskq.testing.assertions import (
    assert_job_status,
    assert_job_terminal,
    assert_attempt,
    wait_for_job_status,
)

# Assert a row has the expected status and optional fields.
# Returns the row (non-None) for chained access.
row = assert_job_status(row, "failed", error_class="ValueError", attempt=2, finished=True)

# Assert a terminal status with finished_at set.
assert_job_terminal(row, "succeeded")

# Assert on the attempt row at a given index.
assert_attempt(attempts, 0, outcome="succeeded", attempt_num=1)

# Poll backend.get until the job reaches a status (with timeout).
row = await wait_for_job_status(backend, job_id, "succeeded", timeout=2.0)
```

### Event assertions

```python
from taskq.testing.assertions import (
    assert_has_event,
    assert_transition_sequence,
    parse_detail,
)

# Find at least one event matching kind and optional state filters.
assert_has_event(events, "state_change", from_state="running", to_state="succeeded")

# Assert the (from_state, to_state) sequence from state_change events matches.
assert_transition_sequence(
    events,
    expected=[("pending", "running"), ("running", "succeeded")],
)
```

### OTel span assertions

```python
from taskq.testing.assertions import assert_has_span, assert_has_otel_event

# Find a span by name; assert kind/status if provided.
span = assert_has_span(exporter, "process my_actor", kind=trace.SpanKind.CONSUMER)

# Find an OTel event by span and event name; assert state attributes.
assert_has_otel_event(
    exporter, "process my_actor", "lifecycle.running",
    from_state="pending", to_state="running",
)
```

### Async and PG helpers

| Helper | Description |
|---|---|
| `wait_for(event, timeout=2.0)` | Wait for an `asyncio.Event` with test-failure semantics on timeout. |
| `wait_for_leader(deps, timeout=5.0)` | Wait for the leader event on `WorkerDeps`. |
| `pg_now(conn)` | Return PG's `clock_timestamp()` — use instead of `datetime.now(UTC)` for cutoffs compared against SQL-written rows. |
| `plain_cli_output(output)` | Strip ANSI escapes and collapse whitespace for stable CLI-output assertions. |

---

## Chaos testing

`taskq.testing.asyncpg_chaos` provides `ChaosConnection` and `ChaosPool` for
simulating mid-transaction failures in integration tests. The wrapper raises
`ChaosException` on the configured call number, allowing tests to verify that
transaction rollback works correctly when a failure occurs between SQL
statements inside a transaction.

### ChaosConnection

```python
from taskq.testing.asyncpg_chaos import ChaosConnection, ChaosException

# Wrap a real connection; raise on the 3rd query call.
chaos_conn = ChaosConnection(real_conn, fail_on_call=3)

async with chaos_conn.transaction():
    await chaos_conn.execute("INSERT ...")   # call 1
    await chaos_conn.execute("UPDATE ...")   # call 2
    await chaos_conn.execute("DELETE ...")   # call 3 → ChaosException
# transaction rolls back; calls 1 and 2 are undone
```

`fail_on_call` counts query methods (`execute`, `fetchrow`, `fetch`,
`fetchval`) in execution order. The exception is raised **before** the query
is sent to PG. Pass `fail_with=` to raise a different exception type (e.g.
`asyncpg.PostgresConnectionError`).

`transaction()` delegates to the real connection so asyncpg's commit/rollback
works correctly when `ChaosException` propagates through
`async with conn.transaction():`.

### ChaosPool

`ChaosPool` is a pool-like object that yields a `ChaosConnection` from
`acquire()`. Temporarily replace `backend._worker_pool` to test mid-transaction
failures in backend methods:

```python
from taskq.testing.asyncpg_chaos import ChaosPool

chaos_conn = ChaosConnection(real_conn, fail_on_call=5)
saved_pool = backend._worker_pool
backend._worker_pool = ChaosPool(chaos_conn)
try:
    # ... exercise backend method that acquires from the pool ...
    ...
except ChaosException:
    pass  # expected
finally:
    backend._worker_pool = saved_pool
```

`ChaosException` carries the `call_number` for debugging. It does not swallow
`CancelledError` — that propagates naturally from the wrapped connection.

### Shortened timing for chaos tests

`shorten_chaos_settings` temporarily reduces heartbeat/lock-lease/grace timing
on `WorkerDeps` so chaos scenarios trigger sweeps quickly:

```python
from taskq.testing.settings import shorten_chaos_settings

with shorten_chaos_settings(deps_a, deps_b):
    # heartbeat→1s, lock_lease→4s, grace periods→0
    ...
```

---

## Property-based testing

TaskQ uses [Hypothesis](https://hypothesis.readthedocs.io/) extensively for
invariant testing. Property tests run against `InMemoryBackend` with a
`FakeClock` — Hypothesis controls backend lifecycle, and PG requires
testcontainers so it cannot be reset between examples.

The pattern: build a strategy of operations, drive the backend, and assert a
deterministic invariant holds for every generated input.

```python
from datetime import UTC, datetime, timedelta
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)


@settings(max_examples=200, deadline=None)
@given(
    delay_seconds=st.floats(min_value=1, max_value=3600, allow_nan=False),
    deadline_offset=st.one_of(
        st.none(),
        st.floats(min_value=0, max_value=7200, allow_nan=False),
    ),
)
async def test_snooze_deterministic_outcome(
    delay_seconds: float,
    deadline_offset: float | None,
) -> None:
    backend = InMemoryBackend(clock=FakeClock(_START))
    delay = timedelta(seconds=delay_seconds)
    schedule_to_close = (
        None if deadline_offset is None else _START + timedelta(seconds=deadline_offset)
    )

    # Use assume() to exclude the dead zone where the invariant is ambiguous.
    if schedule_to_close is not None:
        assume(schedule_to_close > _START)

    job_id = new_job_id()
    await backend.enqueue(EnqueueArgs(
        id=job_id, actor="test_actor", queue="default",
        payload={"key": "value"}, max_attempts=5, retry_kind="transient",
        scheduled_at=_START, schedule_to_close=schedule_to_close,
    ))

    wid = backend._worker_id
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert len(dispatched) == 1

    result = await backend.mark_snoozed(job_id, wid, delay)
    deadline_exceeded = schedule_to_close is not None and _START + delay > schedule_to_close

    if deadline_exceeded:
        assert result == "failed"
    else:
        assert result == "scheduled"
```

Tips for TaskQ property tests:

- Use `@settings(max_examples=200, deadline=None)` — async tests and PG
  latency make Hypothesis's default deadline flaky.
- Use `allow_nan=False, allow_infinity=False` on `st.floats` to avoid
  timedelta edge cases.
- Use `assume()` to exclude ranges where the invariant is ambiguous (e.g. the
  "dead zone" between a snooze and re-dispatch).
- Drive the backend directly (`enqueue`, `dispatch_batch`, `mark_*`) rather
  than through `JobsClient` so strategies map to primitive values.
- `FakeClock` + `backend.advance_clock_to()` makes time-dependent invariants
  fully deterministic.

---

## Integration tests

Integration tests use `testcontainers` to spin up real Postgres and Redis
containers. They are marked `@pytest.mark.integration` so unit-only runs skip
them:

```bash
# Run unit tests only (no containers)
uv run pytest -m "not integration"

# Run integration tests (boots containers)
uv run pytest -m integration
```

### Testcontainers setup

The session-scoped `pg_container` and `redis_container` fixtures boot
Postgres 18 and Redis 7.4 once per session:

```python
import pytest

@pytest.mark.integration
async def test_real_pg(clean_jobs_app: JobsApp) -> None:
    backend = clean_jobs_app.backend
    # backend is a real PostgresBackend against a migrated, seeded schema
    ...
```

The `clean_jobs_app` fixture truncates and re-seeds the module's PG schema
before each test, then opens `WorkerDeps` + `PostgresBackend`. For Redis:

```python
@pytest.mark.integration
async def test_real_redis(clean_redis_client) -> None:
    # clean_redis_client is a fresh redis.asyncio.Redis on a clean DB
    ...
```

### Fast integration-test settings

`make_integration_settings` constructs `WorkerSettings` with short intervals
for bounded test timeouts (heartbeat 0.5s, lock-lease 2s, grace 0.5s):

```python
from taskq.testing.settings import make_integration_settings

settings = make_integration_settings(pg_dsn, schema_name="tq_test")
```

### PG row helpers

`taskq.testing.pg` provides helpers for creating test fixtures directly in PG:

| Helper | Description |
|---|---|
| `create_pending_job(conn, schema, ...)` | Insert a pending job row. |
| `create_running_job(conn, schema, ...)` | Insert a running job + worker. |
| `create_workered_running_job(conn, schema)` | Insert worker + running job, returns `(worker_id, job_id)`. |
| `create_worker(conn, schema, worker_id)` | Insert a worker row. |
| `seed_actors(conn, schema)` | Seed default `actor_config` rows. |
| `reset_schema(conn, schema)` | Truncate all tables (FK-safe CASCADE) + re-seed. |
| `truncate_schema(conn, schema)` | Truncate all tables. |
| `get_job_triple(conn, schema, job_id)` | Fetch job + actor config + worker. |
| `DEFAULT_ACTORS` | Pre-built actor configs for tests. |

### Two-pod / chaos scenarios

`_open_two_pg_workers` (in `taskq.testing.fixtures`) opens two independent
`WorkerDeps` + `PostgresBackend` instances against the same schema for
two-pod leader-election and chaos-kill tests. It does not start the leader
loops — callers construct `MaintenanceLeader` themselves with different
initial states.

### xdist isolation

Integration tests are grouped by module via the `pytest_collection_modifyitems`
hook in `conftest.py`, which assigns `xdist_group(name=<module basename>)` to
every integration test without an explicit group. This ensures module-scoped
PG schemas land on the same xdist worker. See `pyproject.toml` for the
`--dist=loadgroup` setting.

---

## See also

- [Actors — testing actors without a database](actors.md#testing-actors-without-a-database) — `InMemoryBackend` + direct invocation
- [Rate Limiting — testing rate limits](rate-limiting.md#testing-rate-limits) — `backend="memory"` + `FakeClock`
- [Observability — testing observability](observability.md#6-testing-observability) — `setup_tracer`, `setup_meter`, trace-context propagation
- [API Reference — Testing](../api-reference/testing.md) — full `taskq.testing` API surface
- [Workers](workers.md) — worker lifecycle, `WorkerDeps`, maintenance leader
