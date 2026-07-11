# Tutorial: Building a Notification Digest System

This tutorial builds a **notification digest system** from scratch, one pattern
at a time. Each part adds a new capability to the same codebase. By the end you
will have a cron-scheduled, retried, deduplicated, DI-wired, batch-fan-out job
pipeline with progress reporting, cancellation, and unit tests.

All code lives in a single `myapp/actors.py` module (plus a few scripts). Each
part shows the **new or changed code** — earlier definitions remain in place
unless explicitly replaced.

---

## Prerequisites

- Python 3.12+, TaskQ installed (`uv add taskq-py`)
- Postgres with schema applied (`taskq migrate up`)
- Redis (optional — for real-time progress streaming)

See [Getting Started](../getting-started/quick-start.md) for initial setup.

---

## Part 1: First Actor

Define an actor that compiles a notification digest for a user. Payloads and
results must be `pydantic.BaseModel` subclasses.

```python
# myapp/actors.py
from datetime import date
from pydantic import BaseModel
from taskq import actor

class DigestPayload(BaseModel):
    user_id: str
    target_date: date

class DigestResult(BaseModel):
    user_id: str
    notification_count: int
    summary: str

@actor(queue="digests")
async def compile_digest(payload: DigestPayload) -> DigestResult:
    return DigestResult(
        user_id=payload.user_id,
        notification_count=0,
        summary="No new notifications",
    )

registry = [compile_digest]
```

Start a worker:

```bash
taskq worker --actors myapp.actors:registry
```

Enqueue a job from a script using `TaskQ`:

```python
# myapp/enqueue.py
import asyncio
from taskq import TaskQ
from taskq.settings import TaskQSettings
from myapp.actors import compile_digest, DigestPayload

async def main() -> None:
    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        handle = await tq.enqueue(
            compile_digest,
            DigestPayload(user_id="u_001", target_date="2026-07-10"),
        )
        print(f"enqueued job {handle.job_id}")

asyncio.run(main())
```

```bash
python -m myapp.enqueue
```

---

## Part 2: Results & Waiting

`JobHandle.wait()` polls until terminal status, then returns the deserialized
result typed as `R`. Add `result_ttl` to control how long the result is retained.

Replace the `@actor` decorator from Part 1:

```python
from datetime import timedelta

@actor(queue="digests", result_ttl=timedelta(hours=24))
async def compile_digest(payload: DigestPayload) -> DigestResult:
    return DigestResult(
        user_id=payload.user_id,
        notification_count=3,
        summary="3 new notifications",
    )
```

```bash
taskq worker --actors myapp.actors:registry
```

Update the enqueue script to wait for the typed result:

```python
# myapp/enqueue.py
import asyncio
from taskq import TaskQ
from taskq.exceptions import JobFailed, ResultUnavailable
from taskq.settings import TaskQSettings
from myapp.actors import compile_digest, DigestPayload, DigestResult

async def main() -> None:
    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        handle = await tq.enqueue(
            compile_digest,
            DigestPayload(user_id="u_001", target_date="2026-07-10"),
        )
        try:
            result: DigestResult = await handle.wait(timeout=30.0)
            print(f"digest: {result.notification_count} notifications")
        except JobFailed as exc:
            print(f"failed: {exc.row.error_class}: {exc.row.error_message}")
        except ResultUnavailable:
            print("result gone (TTL expired?)")
        except TimeoutError:
            print("timed out waiting")

asyncio.run(main())
```

`wait()` raises `JobFailed` for non-success terminal states (with `exc.row`),
`ResultUnavailable` when the result is missing, and `TimeoutError` when the
timeout elapses.

---

## Part 3: Retries & Error Handling

The digest compilation calls a notifications API that can be rate-limited.
Configure a `RetryPolicy`, mark permanent errors as non-retryable, and use
`RetryAfter` for `Retry-After` headers. Add these classes and replace the actor:

```python
from taskq.exceptions import RetryAfter
from taskq.retry import RetryPolicy

class NotificationsAPIError(Exception):
    def __init__(self, status_code: int, retry_after: int | None = None) -> None:
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"HTTP {status_code}")

class InvalidUserError(Exception):
    pass

async def fetch_notifications(user_id: str, target_date: date) -> list[dict]:
    raise NotImplementedError

@actor(
    queue="digests",
    result_ttl=timedelta(hours=24),
    retry=RetryPolicy(kind="transient", max_attempts=5, backoff="exponential",
                      base=timedelta(seconds=10), cap=timedelta(minutes=10), jitter=0.25),
    non_retryable_exceptions=(InvalidUserError,),
)
async def compile_digest(payload: DigestPayload) -> DigestResult:
    try:
        notifications = await fetch_notifications(payload.user_id, payload.target_date)
    except NotificationsAPIError as exc:
        if exc.status_code == 429:
            raise RetryAfter(timedelta(seconds=exc.retry_after or 60))
        raise
    return DigestResult(
        user_id=payload.user_id,
        notification_count=len(notifications),
        summary=f"{len(notifications)} new notifications",
    )
```

```bash
taskq worker --actors myapp.actors:registry
```

`RetryAfter(delay)` reschedules at `now + delay`, bypassing backoff. Pass
`consume_budget=False` to skip the attempt count. `non_retryable_exceptions`
fails immediately for listed types. See [Retries](retries.md).

---

## Part 4: Deduplication

If the cron fires twice or an operator re-triggers a run, you do not want
duplicate digests. Configure `unique_for` on the actor and pass
`identity_key` at enqueue time.

```python
@actor(
    queue="digests",
    result_ttl=timedelta(hours=24),
    unique_for=timedelta(hours=25),
    retry=RetryPolicy(kind="transient", max_attempts=5, backoff="exponential"),
    non_retryable_exceptions=(InvalidUserError,),
)
async def compile_digest(payload: DigestPayload) -> DigestResult:
    ...
```

```bash
taskq worker --actors myapp.actors:registry
```

Enqueue with `identity_key` so `unique_for` can match duplicate requests:

```python
# myapp/enqueue.py
import asyncio
from taskq import TaskQ
from taskq.settings import TaskQSettings
from myapp.actors import compile_digest, DigestPayload

async def main() -> None:
    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        identity = "u_001:2026-07-10"
        h1 = await tq.enqueue(
            compile_digest,
            DigestPayload(user_id="u_001", target_date="2026-07-10"),
            identity_key=identity,
        )
        h2 = await tq.enqueue(
            compile_digest,
            DigestPayload(user_id="u_001", target_date="2026-07-10"),
            identity_key=identity,
        )
        print(f"h1: was_existing={h1.was_existing}")
        print(f"h2: was_existing={h2.was_existing}, same={h2.job_id == h1.job_id}")

asyncio.run(main())
```

!!! warning "`identity_key` is required for `unique_for`"
    If `identity_key` is omitted at enqueue time, `unique_for` is a **silent
    no-op** — the library logs a warning and creates a fresh job every time.

---

## Part 5: Dependency Injection

The digest system needs an SMTP client. Register it at `Scope.LOOP` so one
client lives for the event loop duration with proper teardown. Actors with DI
dependencies must run through the programmatic `worker_main` entry point.

Add a new actor and `SmtpClient` class to `actors.py`:

```python
class SendDigestEmailPayload(BaseModel):
    user_id: str
    target_date: date
    summary: str
    notification_count: int

class SmtpClient:
    async def send(self, to: str, subject: str, body: str) -> str: ...
    async def aclose(self) -> None: ...

@actor(queue="email", retry=RetryPolicy(kind="transient", max_attempts=3))
async def send_digest_email(
    payload: SendDigestEmailPayload, *, smtp: SmtpClient,
) -> None:
    await smtp.send(
        to=f"{payload.user_id}@example.com",
        subject=f"Your digest for {payload.target_date}",
        body=payload.summary,
    )

registry = [compile_digest, send_digest_email]
```

Create a worker entry point that builds and passes the DI registry:

```python
# myapp/worker.py
from taskq.di import ProviderRegistry, Scope
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main
from myapp.actors import SmtpClient, registry

async def make_smtp_client():
    client = SmtpClient()
    try:
        yield client
    finally:
        await client.aclose()

if __name__ == "__main__":
    di = ProviderRegistry()
    di.register_factory(SmtpClient, Scope.LOOP, make_smtp_client)
    raise SystemExit(worker_main(
        WorkerSettings.load(),
        actor_registry={r.name: r for r in registry},
        di_registry=di,
    ))
```

```bash
python -m myapp.worker
```

!!! warning "Do not pre-validate the registry"
    The worker calls `registry.validate()` after auto-registering
    `WorkerSettings`, `Clock`, and `asyncpg.Pool`. Pre-validating raises
    `MissingProvider`.

| Scope | Lifetime | Use |
|---|---|---|
| `PROCESS` | Worker process lifetime | Config, shared singletons |
| `LOOP` | Event loop lifetime | HTTP clients, SMTP, Redis |
| `TRANSIENT` | Per actor invocation | Per-request helpers |

See [Dependency Injection](dependency-injection.md).

---

## Part 6: Batch Fan-Out

Instead of sending one email at a time, fan out individual send jobs using
`ctx.jobs.enqueue_batch()`. Add a `compile_batch_digest` actor to `actors.py`:

```python
from taskq import EnqueueItem
from taskq.context import JobContext

class BatchDigestPayload(BaseModel):
    user_ids: list[str]
    target_date: date

class BatchDigestResult(BaseModel):
    total_users: int
    batch_id: str

@actor(queue="digests", retry=RetryPolicy(kind="transient", max_attempts=3))
async def compile_batch_digest(
    payload: BatchDigestPayload, ctx: JobContext[BatchDigestPayload],
) -> BatchDigestResult:
    items = [
        EnqueueItem(
            actor_ref=send_digest_email,
            payload=SendDigestEmailPayload(
                user_id=uid, target_date=payload.target_date,
                summary=f"Your daily digest for {payload.target_date}",
                notification_count=0,
            ),
            identity_key=f"{uid}:{payload.target_date}",
        )
        for uid in payload.user_ids
    ]
    batch = await ctx.jobs.enqueue_batch(items)
    return BatchDigestResult(
        total_users=len(payload.user_ids),
        batch_id=str(batch[0].job_id),
    )

registry = [compile_digest, send_digest_email, compile_batch_digest]
```

```bash
python -m myapp.worker
```

Enqueue the batch digest from a script:

```python
# myapp/enqueue_batch.py
import asyncio
from taskq import TaskQ
from taskq.settings import TaskQSettings
from myapp.actors import compile_batch_digest, BatchDigestPayload

async def main() -> None:
    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        handle = await tq.enqueue(
            compile_batch_digest,
            BatchDigestPayload(user_ids=["u_001", "u_002", "u_003"], target_date="2026-07-10"),
        )
        print(f"batch digest job: {handle.job_id}")
        while True:
            row = await handle.refresh()
            if row.status in ("succeeded", "failed", "cancelled", "crashed", "abandoned"):
                print(f"finished: {row.status}")
                break
            await asyncio.sleep(1.0)

asyncio.run(main())
```

```bash
python -m myapp.enqueue_batch
```

!!! note "Sub-job enqueue is transactional"
    Sub-jobs via `ctx.jobs.enqueue_batch()` are part of the parent's database
    transaction. If the parent raises, sub-jobs are rolled back atomically.

---

## Part 7: Cron Scheduling

Wire up a cron schedule so the batch digest fires every day at 03:00 UTC.
Declare it with `cron()` — the worker auto-discovers and persists it at startup.

```python
from taskq import cron

async def make_batch_payload() -> dict:
    from datetime import UTC, datetime
    return {
        "user_ids": ["u_001", "u_002", "u_003"],
        "target_date": datetime.now(UTC).date().isoformat(),
    }

cron(
    "0 3 * * *",
    "compile_batch_digest",
    payload_factory="myapp.actors.make_batch_payload",
)
```

```bash
python -m myapp.worker
```

The `cron()` call auto-registers at import time. At worker startup the
bootstrap creates it in `cron_schedules` (create-only, skip-on-conflict). The
maintenance leader fires the job daily at 03:00 UTC.

| Parameter | Description |
|---|---|
| `expression` | Standard 5-field cron expression. |
| `actor` | Actor name — must match a registered `ActorRef.name`. |
| `payload_factory` | Dotted path to a callable returning `dict` or `BaseModel`. |
| `static_payload` | Fixed payload dict. Mutually exclusive with `payload_factory`. |
| `identity_key` | Opaque key passed to `enqueue()` on every fire. Enables cron↔on-demand dedup. |

See [Cron Scheduling](cron.md) for per-property schedules and DST.

---

## Part 8: Progress & Cancellation

The batch digest may process thousands of users. Add a progress and
cancellation loop to the start of `compile_batch_digest`'s body, before the
existing `items` list comprehension:

```python
    total = len(payload.user_ids)

    for i, uid in enumerate(payload.user_ids):
        if ctx.cancellation_requested:
            ctx.log.info("cancellation requested, stopping", processed=i)
            break

        await ctx.progress(
            step=i + 1,
            percent=round((i + 1) / total * 100, 1),
            detail=f"Processing user {i + 1}/{total}",
        )
```

```bash
python -m myapp.worker
```

Cancel a running job from client code:

```python
# myapp/cancel.py
import asyncio
from taskq import TaskQ
from taskq.settings import TaskQSettings
from myapp.actors import compile_batch_digest, BatchDigestPayload

async def main() -> None:
    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn)) as tq:
        handle = await tq.enqueue(
            compile_batch_digest,
            BatchDigestPayload(
                user_ids=[f"u_{i:03d}" for i in range(1, 501)], target_date="2026-07-10",
            ),
        )
        await asyncio.sleep(2.0)
        result = await handle.cancel(reason="operator cancelled")
        print(f"cancellation_initiated={result.cancellation_initiated}")

asyncio.run(main())
```

```bash
python -m myapp.cancel
```

!!! note "Redis required for real-time progress streaming"
    `progress_stream()` requires `[redis]` extra and `TASKQ_REDIS_URL`. Without
    Redis it falls back to 500 ms Postgres polling.

See [Progress](progress.md) and [Cancellation](cancellation.md).

---

## Part 9: Testing

Test the digest system without Postgres or Redis using `InMemoryBackend` and
`FakeClock`. Register stubs, enqueue jobs, call `run_until_drained()`, assert.

```python
# tests/test_digest.py
from datetime import UTC, datetime
from taskq import JobsClient
from taskq.testing import InMemoryBackend, FakeClock
from myapp.actors import DigestPayload, DigestResult, compile_digest

async def test_compile_digest_returns_result() -> None:
    clock = FakeClock(start=datetime(2026, 7, 10, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)
    backend.register_stub(
        compile_digest.name,
        lambda p, ctx: {"user_id": p["user_id"], "notification_count": 3, "summary": "3 new"},
    )
    handle = await client.enqueue(compile_digest, DigestPayload(user_id="u_001", target_date="2026-07-10"))
    await backend.run_until_drained()
    result = await handle.wait()
    assert result.user_id == "u_001"
    assert result.notification_count == 3

async def test_compile_digest_deduplication() -> None:
    clock = FakeClock(start=datetime(2026, 7, 10, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)
    backend.register_stub(
        compile_digest.name,
        lambda p, ctx: {"user_id": p["user_id"], "notification_count": 0, "summary": ""},
    )
    identity = "u_001:2026-07-10"
    h1 = await client.enqueue(compile_digest, DigestPayload(user_id="u_001", target_date="2026-07-10"), identity_key=identity)
    h2 = await client.enqueue(compile_digest, DigestPayload(user_id="u_001", target_date="2026-07-10"), identity_key=identity)
    assert h1.was_existing is False
    assert h2.was_existing is True
    assert h2.job_id == h1.job_id

async def test_direct_invocation_no_queue() -> None:
    result = await compile_digest(DigestPayload(user_id="u_001", target_date="2026-07-10"))
    assert isinstance(result, DigestResult)
    assert result.user_id == "u_001"
```

```bash
uv run pytest tests/test_digest.py -v
```

| Testing primitive | Purpose |
|---|---|
| `InMemoryBackend` | In-process backend simulating the full enqueue-dispatch-execute cycle. |
| `FakeClock` | Deterministic clock — `advance()` and `move_to()` control time. |
| `register_stub(name, fn)` | Register `(payload, ctx) -> dict` that `run_until_drained` executes. |
| `run_until_drained()` | Drive the dispatch loop to completion, auto-advancing through snoozes. |

!!! note "Stubs receive payload as `dict`"
    The stub receives `(payload: dict, ctx)` where `ctx` has `job_id`,
    `attempt`, `payload`, and `cancel_event`. Call the `ActorRef` directly to
    test real handler logic: `await compile_digest(payload)`.

See [Testing](testing.md) for the full toolkit.

---

## Summary

| Part | Pattern | Key API |
|---|---|---|
| 1 | First actor | `@actor`, `TaskQ`, `tq.enqueue()` |
| 2 | Results & waiting | `handle.wait()`, `result_ttl`, `JobFailed` |
| 3 | Retries & errors | `RetryPolicy`, `non_retryable_exceptions`, `RetryAfter` |
| 4 | Deduplication | `unique_for`, `identity_key`, `was_existing` |
| 5 | Dependency injection | `ProviderRegistry`, `Scope.LOOP`, `worker_main(di_registry=...)` |
| 6 | Batch fan-out | `EnqueueItem`, `ctx.jobs.enqueue_batch()` |
| 7 | Cron scheduling | `cron()`, `payload_factory` |
| 8 | Progress & cancellation | `ctx.progress()`, `ctx.cancellation_requested`, `handle.cancel()` |
| 9 | Testing | `InMemoryBackend`, `FakeClock`, `register_stub`, `run_until_drained` |
