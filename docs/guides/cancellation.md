# Job Cancellation

## 1. Overview

Cancellation in TaskQ is a request, not an immediate kill. When a caller invokes `JobsClient.cancel()`, the library records the request and — for jobs that are not yet running — immediately moves them to `cancelled`. For a running job, the worker must cooperate: the heartbeat loop polls Postgres on every tick, detects the cancel flag, and signals the actor via `JobContext.cancel_event`. If the actor does not exit within `cancellation_grace_period` seconds, the worker escalates by raising `asyncio.CancelledError` inside the actor task. If the actor still does not exit within the subsequent `cleanup_grace_period`, the job is marked `abandoned`. This cooperative-then-forced sequence is the three-phase cancellation protocol (`COOPERATIVE` → `FORCED` → `ABANDON_PENDING`).

---

## 2. Requesting cancellation

### Via `JobsClient.cancel()`

```python
from taskq.client import JobsClient

result = await client.cancel(job_id, reason="user requested")
```

`cancel()` reads the job row first. If the job does not exist, `KeyError` is raised. Otherwise it calls `Backend.write_cancel_request`, re-reads the row, and returns a `CancelResult`.

`reason` is optional free-form text recorded in the Postgres `job_events` table.

### Via `JobHandle.cancel()`

```python
handle = await client.enqueue(my_actor, payload)
# ... later
result = await handle.cancel(reason="deadline exceeded")
```

`JobHandle.cancel()` delegates directly to `JobsClient.cancel(handle.job_id, reason)`. The handle must have been constructed with a `JobsClient` (i.e. via `client.enqueue()` or `client.get()`); handles obtained from inside an actor body via `ctx.jobs.enqueue()` do not have a client and will raise `RuntimeError`.

### Effect by prior status

| Prior status | Effect |
|---|---|
| `pending` | Immediately transitioned to `cancelled`; `cancellation_initiated=True` |
| `scheduled` | Immediately transitioned to `cancelled`; `cancellation_initiated=True` |
| `running` | Cancel request recorded in PG; worker must cooperate; `cancellation_initiated=True` |
| `succeeded`, `failed`, `cancelled`, `crashed`, `abandoned` | No state change; `cancellation_initiated=False` |

When `cancellation_initiated=False`, the job was already in a terminal state and the cancel request had no effect. A second `cancel()` call on the same job after it is already `cancelled` returns `cancellation_initiated=False`.

---

## 3. The three-phase cancellation protocol

The protocol runs inside the heartbeat loop, which ticks every `heartbeat_interval` seconds (default: 10 s). On each tick, `CancelController.run_in_tx()` executes inside the open heartbeat transaction and `run_post_tx()` is called after the transaction commits.

### Phase 0 — None (`CancelPhase.NONE`)

No cancellation is in progress. The job is running normally.

### Phase 1 — Cooperative (`CancelPhase.COOPERATIVE`)

The heartbeat loop calls `POLL_CANCEL_FLAGS_SQL` to fetch outstanding cancel flags for this worker. When it finds `cancel_phase >= 1` in Postgres for a running job:

- `ctx.cancel_event.set()` is called on the job's `JobContext`.
- `cancel_observed_at` is recorded using `asyncio.get_running_loop().time()` (monotonic event-loop clock, not wall clock).
- The job's in-process `cancel_phase` is set to `CancelPhase.COOPERATIVE`.
- `log_cancel_phase_change` is called and the `taskq.cancellation.phase_transitions` counter is incremented.

No Postgres write occurs at phase 1. The actor can now observe `ctx.cancellation_requested == True` and exit cleanly. If it does, the consumer calls `backend.mark_cancelled()`.

### Phase 2 — Forced (`CancelPhase.FORCED`)

If `loop.time() - cancel_observed_at > cancellation_grace_period` (default: 30 s), the heartbeat loop escalates:

1. Writes `cancel_phase = 2` to the Postgres `jobs` row via `CANCEL_ESCALATION_SQL`. If the UPDATE rowcount is 0 (another worker already wrote phase 2), the escalation is skipped.
2. Writes a `state_change` event to `job_events` including `cancel_phase_from=1`, `cancel_phase_to=2`, and `worker_id`.
3. Calls `task.cancel()` on the actor's asyncio task.

The PG write is guaranteed to happen before `task.cancel()` with no intervening `await` between them. If the PG write raises, `task.cancel()` is not called and the exception propagates to the heartbeat loop's error handler.

When `asyncio.CancelledError` propagates out of the actor, the consumer catches it and calls `backend.mark_cancelled()`, then re-raises.

**PG-observation fast-advance:** If the heartbeat loop observes `db_phase == 2` while the local phase is still `NONE` or `COOPERATIVE` (e.g. because another worker's heartbeat already wrote phase 2), the local phase is advanced to `FORCED` without any PG write or `task.cancel()` call.

### Phase 3 — Abandon pending (`CancelPhase.ABANDON_PENDING`)

This is an in-process sentinel. It is never persisted to Postgres (the `cancel_phase` column has a `CHECK (cancel_phase BETWEEN 0 AND 2)` constraint).

If `loop.time() - cancel_observed_at >= cancellation_grace_period + cleanup_grace_period` (defaults: 30 s + 10 s = 40 s), the job is queued into `_pending_abandons`. After the heartbeat transaction commits and its row lock is released, `run_post_tx()` drains the queue: for each entry it calls `backend.mark_abandoned()` under `asyncio.shield`, then deregisters the job from `ActiveJobRegistry`.

`mark_abandoned` uses a separate pool connection, which is why it cannot run inside the heartbeat transaction (doing so would self-deadlock on the row lock).

When the consumer's `asyncio.CancelledError` handler fires after phase 3 is queued, it checks `entry.cancel_phase >= CancelPhase.ABANDON_PENDING` and re-raises without calling `mark_cancelled`. The `run_post_tx` path owns the terminal write.

---

## 4. Handling cancellation in an actor

### Cooperative cancellation

Check `ctx.cancellation_requested` at natural loop boundaries or between I/O calls:

```python
from taskq import actor
from taskq.context import JobContext

@actor
async def long_running(payload: Payload, ctx: JobContext[Payload]) -> Result:
    for chunk in payload.chunks:
        if ctx.cancellation_requested:
            await cleanup()
            return  # or raise — either exits the actor
        await process(chunk)
    return Result(...)
```

`ctx.cancellation_requested` is `ctx.cancel_event.is_set()` — a non-blocking, non-awaited property. It is safe to check inside tight loops.

If the actor returns normally after observing the cancel signal, the job is still marked `cancelled`. The consumer checks `entry.cancel_phase >= CancelPhase.COOPERATIVE` after the actor returns and routes to `mark_cancelled` rather than `mark_succeeded`. The return value is discarded.

For actors with a single long `await`, awaiting `ctx.cancel_event.wait()` directly allows the actor to wake as soon as the signal arrives:

```python
@actor
async def long_io(payload: Payload, ctx: JobContext[Payload]) -> None:
    try:
        await asyncio.wait_for(do_work(), timeout=None)
    except asyncio.CancelledError:
        raise  # always re-raise
```

### Do not suppress `asyncio.CancelledError`

If the grace period expires, `task.cancel()` raises `asyncio.CancelledError` inside the actor at the next `await` point. Suppressing it prevents the force-cancel path from working:

```python
# BAD: suppressing CancelledError prevents force-cancel
try:
    await some_long_io()
except asyncio.CancelledError:
    pass  # Never do this — re-raise it
```

Always re-raise `asyncio.CancelledError` or let it propagate. The consumer's exception handler takes care of the terminal write.

---

## 5. Cancelling scheduled or pending jobs

Cancelling a job that has not yet been dispatched to a worker is immediate. No worker involvement is required:

```python
from datetime import UTC, datetime, timedelta

handle = await client.enqueue(
    my_actor,
    payload,
    scheduled_at=datetime.now(UTC) + timedelta(hours=1),
)
result = await handle.cancel()
assert result.new_status == "cancelled"
assert result.cancellation_initiated is True  # immediate, no worker involved
```

The status transitions `pending → cancelled` or `scheduled → cancelled` are applied atomically by `Backend.write_cancel_request`.

---

## 6. Checking cancellation status

After calling `cancel()`, inspect `CancelResult.cancellation_initiated` to determine whether the request did anything:

```python
result = await client.cancel(job_id)
if not result.cancellation_initiated:
    # Job was already terminal — nothing to wait for
    print(f"job was already {result.previous_status}")
else:
    # For running jobs, wait for the worker to finish cancelling
    handle = await client.get(job_id, result_adapter=TypeAdapter(None))
    if handle is not None:
        final_row = await handle.refresh()
        # final_row.status will be "cancelled" or "abandoned" once the worker finishes
```

Alternatively, `handle.wait()` blocks until any terminal status is reached and raises `JobFailed` when the status is `cancelled`, `failed`, `crashed`, or `abandoned`:

```python
from taskq.exceptions import JobFailed

try:
    await handle.wait(timeout=60.0)
except JobFailed as exc:
    if exc.row.status == "cancelled":
        print("job was cancelled as requested")
```

`wait()` polls Postgres every 0.5 s. It raises `TimeoutError` if no terminal status is reached within the given `timeout`.

---

## 7. `CancelResult` reference

`CancelResult` is a frozen Pydantic model returned by `JobsClient.cancel()` and `JobHandle.cancel()`.

| Field | Type | Description |
|---|---|---|
| `job_id` | `UUID` | The job identifier passed to `cancel()`. |
| `previous_status` | `JobStatus` | The job's status at the time of the first `backend.get()` call — before any write. Subject to TOCTOU: concurrent writes may have changed the status between the read and the `write_cancel_request`. |
| `new_status` | `JobStatus` | The job's status after the cancel write, read back via a second `backend.get()`. |
| `cancellation_initiated` | `bool` | `True` if `write_cancel_request` changed state (the job was not already terminal). `False` if the job was already in a terminal status and the request had no effect. |

---

## 8. Job statuses and cancellation

| Status | Meaning in cancellation context |
|---|---|
| `cancelled` | The job was cancelled successfully via the cooperative or forced path. |
| `abandoned` | The actor did not exit within `cancellation_grace_period + cleanup_grace_period`; the worker wrote `abandoned` via `mark_abandoned`. |
| `failed` | The job failed before the cancel request was processed. A `cancel()` call on a `failed` job returns `cancellation_initiated=False`. |
| `crashed` | The worker's lock expired and the recovery sweep reclaimed the job. The cancel request, if any, was not processed. |

A cancel request against a job in any terminal status (`succeeded`, `failed`, `cancelled`, `crashed`, `abandoned`) has no effect and returns `cancellation_initiated=False`.

---

## 9. Cancellation and retries

Cancellation does not consume retry budget. Once a job transitions to `cancelled` or `abandoned`, it is immediately terminal and will not be retried, regardless of `max_attempts` or `retry_kind`.

This is distinct from a `TimeoutError` or unhandled exception, both of which go through the normal retry decision logic (`decide_after_failure`) and may reschedule the job if budget remains.

---

## 10. Observability

Every `JobsClient.cancel()` call increments the `taskq.cancellation.requested` OTel counter exactly once, regardless of whether `cancellation_initiated` is `True` or `False`, and regardless of whether `KeyError` is raised for a missing job.

Each phase transition in the heartbeat loop calls `log_cancel_phase_change()` (structlog) and increments the `taskq.cancellation.phase_transitions` counter with `from_phase` and `to_phase` integer attributes. The four valid transition pairs are:

| `from_phase` | `to_phase` | Trigger |
|---|---|---|
| `0` | `1` | Phase-1 cooperative observation |
| `1` | `2` | Phase-2 forced escalation |
| `2` | `3` | Phase-3 abandonment queued |
| `0` | `2` | PG-observation fast-advance (worker saw `db_phase=2` before its own phase-1) |

The heartbeat tick duration is recorded in the `taskq.heartbeat.tick_duration_seconds` histogram. Consecutive heartbeat failures are exposed as the `taskq.heartbeat.consecutive_failures` observable gauge.

For OTel configuration, exporter setup, and the full list of metrics and log events, see [observability.md](observability.md).

---

## See also

- [actors.md](actors.md) — `JobContext` fields, `@actor` decorator, actor lifecycle
- [jobs-clients.md](jobs-clients.md) — `JobsClient`, `JobHandle`, `enqueue()`, `wait()`
- [workers.md](workers.md) — heartbeat loop, `WorkerSettings`, grace period configuration
- [retries.md](retries.md) — retry policies, `RetryAfter`, `Snooze`
