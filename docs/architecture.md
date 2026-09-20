# Architecture Reference

Internal architecture reference for TaskQ. Covers component topology,
the backend protocol, state machine, dispatch mechanics, DI engine, cancellation
protocol, leader election, NOTIFY wiring, shutdown orchestration, watchdog hang
detection, transient error handling, rate limiting, batch subsystem, schema
design, and observability.

This document is useful both for contributors working on TaskQ internals and
for users who want to understand the system's correctness guarantees before
relying on it in production.

Related docs: [api-reference/testing.md](api-reference/testing.md), [index.md](index.md),
[guides/workers.md](guides/workers.md), [guides/actors.md](guides/actors.md), [guides/rate-limiting.md](guides/rate-limiting.md).

---

## High-Level Component Diagram

```
                   ┌─────────────┐
                   │ JobsClient  │
                   └──────┬──────┘
                          │ enqueue()
                          ▼
┌──────────────────────────────────────────┐
│              Backend (Protocol)          │
│   PostgresBackend / InMemoryBackend      │
└──────────────┬───────────────────────────┘
               │ asyncpg
               ▼
          ┌──────────┐
          │ Postgres │
          └──────────┘
               ▲
               │ LISTEN / NOTIFY
               │
┌──────────────────────────────────────────────────────────┐
│                     Worker TaskGroup                     │
│                                                          │
│  ┌───────────────┐   ┌───────────────┐                  │
│  │  HeartbeatLoop │   │ NotifyListener │                  │
│  │  (cancel-poll, │   │ (wake/events) │                  │
│  │   lock renewal)│   └───────┬────────┘                 │
│  └───────────────┘           │ asyncio.Event             │
│                               ▼                          │
│  ┌───────────────┐   ┌───────────────────────────────┐   │
│  │ MaintenanceLeader │   │     ProducerLoop             │   │
│  │ (advisory lock,│   │  dispatch_batch() →           │   │
│  │  sweeps, cron) │   │  ConsumerLoop × N             │   │
│  └───────────────┘   └───────────────────────────────┘   │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Watchdog (det. 2: stale-tick, det. 3: sibling)     │  │
│  └────────────────────────────────────────────────────┘  │
│  ┌───────────────┐                                       │
│  │  DrainMonitor  │  (until-idle only)                  │
│  │  count_active  │                                       │
│  │  _jobs() →     │                                       │
│  │  shutdown      │                                       │
│  └───────────────┘                                       │
└──────────────────────────────────────────────────────────┘
               ▲
               │ FastAPI routes
               │
          ┌──────────┐
          │ Admin UI │
          └──────────┘

  Outside TaskGroup (not cancelled by sibling crashes):
    ShutdownWatchdog (det. 1)    LoopLagWatchdog (det. 4, daemon thread)
    SIGUSR2 → dump_task_stacks()
```

The `Backend` protocol is the single abstraction layer. `PostgresBackend` wires to
Postgres via asyncpg; `InMemoryBackend` holds all state in Python dicts and is used
exclusively in tests.

The watchdog subsystem runs both inside and outside the worker's `TaskGroup`.
The stale-tick sweep (detector 2) and sibling-contract check (detector 3) are
TaskGroup children. `ShutdownWatchdog` (detector 1) and `LoopLagWatchdog`
(detector 4, daemon thread) live outside the TaskGroup so a sibling crash
cannot cancel the very watchdog that catches it. See
[Watchdog Subsystem](#watchdog-subsystem) below.

---

## Backend Protocol

Defined in `src/taskq/backend/_protocol.py`.

### Protocol declaration

```python
@runtime_checkable
class Backend(Protocol):
    BACKEND_PROTOCOL_VERSION: ClassVar[int]
    supports_transactional_simulation: ClassVar[bool]

    # Enqueue
    async def enqueue(self, args: EnqueueArgs) -> JobRow: ...
    async def enqueue_batch(
        self, args_list: list[EnqueueArgs], *, connection=None
    ) -> list[JobRow]: ...  # one JobRow per item; idempotency collisions return existing row
    async def enqueue_batch_fast(
        self, args_list: list[EnqueueArgs], *, connection=None
    ) -> int: ...  # COPY FROM; returns inserted count; no ON CONFLICT, no RETURNING
    async def enqueue_with_conn(self, conn, args: EnqueueArgs) -> JobRow: ...

    # Dispatch
    async def dispatch_batch(
        self, worker_id: UUID, queues: list[str], limit: int, lock_lease: timedelta
    ) -> list[JobRow]: ...

    # Heartbeat
    async def heartbeat_jobs(
        self, worker_id: UUID, lock_lease: timedelta, *, disowned: Collection[UUID] = ()
    ) -> int: ...
    async def extend_reservation_leases(
        self, worker_id: UUID, lock_lease: timedelta, *, disowned: Collection[UUID] = ()
    ) -> int: ...

    # Terminal writes
    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict | None = None,
        progress_seq: int = 0,
        progress_state: dict | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
    ) -> bool: ...  # result OR result_bytes (its orjson encoding); never both
    async def mark_succeeded_with_conn(
        self,
        conn,
        job_id: JobId,
        worker_id: UUID,
        result: dict | None = None,
        progress_seq: int = 0,
        progress_state: dict | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
    ) -> bool: ...
    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict | None = None,
    ) -> JobRow: ...
    async def mark_cancelled(
        self,
        job_id: JobId,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict | None = None,
    ) -> bool: ...
    async def write_cancel_escalation(
        self,
        job_id: JobId,
        worker_id: UUID,
        phase: Literal[2],
    ) -> bool: ...
    async def mark_abandoned(
        self,
        job_id: JobId,
        progress_seq: int = 0,
        progress_state: dict | None = None,
    ) -> bool: ...
    async def mark_snoozed(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict | None = None,
        progress_seq: int = 0,
        progress_state: dict | None = None,
        outcome: AttemptOutcome = "snoozed",
    ) -> Literal["scheduled", "failed", "noop"]: ...
    async def mark_retry_after(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        consume_budget: bool = True,
        progress_seq: int = 0,
        progress_state: dict | None = None,
    ) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]: ...

    # Attempt history
    async def write_attempt(self, attempt: AttemptRow) -> None: ...
    async def get_attempts(self, job_id: JobId) -> list[AttemptRow]: ...
    async def get_events(self, job_id: JobId) -> list[EventRow]: ...

    async def poll_reclaim_events(
        self,
        after_id: int,
        limit: int = 100,
        *,
        visibility_delay: timedelta | None = None,
    ) -> list[EventRow]: ...  # durable cursor for crash-reclaim events; visibility-delay filter

    # Cancel signals
    async def write_cancel_request(self, job_id: JobId, reason: str | None) -> bool: ...
    async def cancel_where(self, filter: JobFilter, reason: str | None) -> BulkCancelResult: ...
    async def poll_cancel_flags(self, worker_id: UUID) -> list[CancelFlag]: ...

    # Admin operations
    async def retry_job(self, job_id: JobId) -> bool: ...

    # Scheduling / sweeps: no `now` parameter; the backend's own clock
    # (PG: clock_timestamp() in the statement; InMemory: the injected
    # Clock) is the arbiter.
    async def scheduled_to_pending(self) -> int: ...
    async def deadline_sweep(self) -> int: ...
    async def reclaim_expired_locks(self, cancel_grace, cleanup_grace) -> int: ...

    # Read
    async def get(self, job_id: JobId) -> JobRow | None: ...
    async def list_jobs(self, filters: JobFilter) -> list[JobRow]: ...
    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]: ...
    async def get_actor_max_pending(self) -> dict[str, int | None]: ...

    # Count
    async def count_active_jobs(self, queues: list[str]) -> int: ...

    # NOTIFY hook
    def subscribe_wake(self) -> AsyncContextManager[asyncio.Event]: ...
    def subscribe_cancel_wake(self) -> AsyncContextManager[asyncio.Event]: ...

    # Schedule CRUD
    async def create_schedule(self, args: ScheduleCreateArgs) -> ScheduleRecord: ...
    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> list[ScheduleRecord]: ...
    async def update_schedule(
        self,
        schedule_id: UUID,
        args: ScheduleUpdateArgs,
    ) -> ScheduleRecord: ...
    async def delete_schedule(self, schedule_id: UUID) -> None: ...

    # Batch operations
    async def enqueue_batch_atomic(
        self,
        items: Iterable[EnqueueArgs],
        *,
        batch_id: UUID,
        queue: str,
        batch_row: BatchRow | None,
        finalizer_args: EnqueueArgs | None,
        chunk_size: int = 1000,
    ) -> list[JobRow]: ...
    async def create_batch(
        self,
        batch_id: UUID,
        queue: str,
        expected_size: int,
        failure_threshold: int | None,
        finalizer_job_id: UUID | None,
        originating_actor: str | None,
        *,
        connection=None,
    ) -> None: ...
    async def increment_batch_failures(
        self, batch_id: UUID, *, connection=None
    ) -> tuple[int, int | None, int]: ...
    async def reset_batch_failures(self, batch_id: UUID, *, connection=None) -> int: ...
    async def abort_batch(self, batch_id: UUID, *, connection=None) -> int: ...
    async def complete_batch(self, batch_id: UUID, *, connection=None) -> None: ...
    async def get_batch(self, batch_id: UUID) -> BatchRow | None: ...
    async def list_batches(self, filter: BatchFilter) -> list[tuple[BatchRow, BatchCounts]]: ...
    async def count_batch_non_terminal(self, batch_id: UUID, *, connection=None) -> int: ...
    async def prune_old_batches(self, cutoff: datetime) -> int: ...
```

`list_jobs` accepts a `JobFilter` whose `status` field may be a single
`JobStatus` or a sequence of statuses (e.g. `["pending", "running"]`).  The
PG backend renders a single status as `status = $n` and a sequence as
`status = ANY($n)` with bound parameters.  An empty sequence
(`status=[]`) matches no jobs, not all jobs; unknown status values are
rejected with `ValueError` at `JobFilter` construction so both backends
fail identically.  `status` and `active` are mutually exclusive:
specifying both raises `ValueError`.

The `active` meta-filter selects statuses by terminality. `active=True`
selects all non-terminal statuses (pending, scheduled, running: 'not yet
finished') and `active=False` the terminal ones; the non-terminal set is
derived from `ACTIVE_STATUSES` in `statemachine.py`.

`BACKEND_PROTOCOL_VERSION` is a `ClassVar[int]` (currently `3`). Both backends
assert this constant matches at import time, preventing silent protocol drift.

**When the version bumps:** increment it whenever a change alters an existing
protocol member's observable contract such that an implementation written
against the previous version would *silently* misbehave (return wrong rows,
ignore inputs) instead of failing loudly.  Purely additive changes that an old
implementation can ignore without producing incorrect behaviour (a new optional
method with a default, a new carrier field old code simply never reads) do not
require a bump.  History: **v3** (unreleased; folds in every protocol
change since the last shipped release): `list_jobs`; `JobFilter.status`
widened to accept a sequence and the `active` meta-filter was added (a v2
implementation returns 0 rows for `status=[...]` and ignores `active`, with
no error).  `get_actor_max_pending` added as a required method (a v2
implementation lacks it; the client capacity cache's fail-open would
otherwise swallow the `AttributeError` and silently enforce code literals
forever; the cache raises `TypeError` at first use instead).
`mark_succeeded` / `mark_succeeded_with_conn` gained the
`fallback_result_ttl` keyword (without it, a cleared stored `result_ttl`
keeps the enqueue-pinned `result_expires_at`, silently expiring results at
completion; a v2 implementation errors loudly on the unexpected keyword at
the first succeeded job).  The same methods gained the `result_bytes`
keyword: the result's orjson encoding, produced once by the worker
consumer.  An implementation that ignores it stores a NULL result (and NULL
`result_size_bytes`) for every consumer-completed job, silently; it must
bind `result_bytes.decode("utf-8")`, store `result_size_bytes =
len(result_bytes)`, NUL-guard the bytes exactly as the dict form is
guarded, reject bytes that are not valid JSON with the `ValueError`
family (bound as text and cast server-side, non-JSON surfaces as a
`PostgresError` the terminal-write classification reads as transient
infrastructure: a permanent data defect looping the job through
reclaim), and reject a call passing both `result` and `result_bytes`.
Any valid JSON value the guard accepts (an array or scalar, not just
an object) stores and reads back verbatim, exactly as PG's jsonb
column holds it: `result`'s `dict[str, object] | None` type describes
the actor contract, not a runtime guarantee for direct `result_bytes`
callers, so a read path must not assume a dict. All of this validation
precedes the fencing write, so a misuse or an unstorable value raises
loudly whatever the job's state.

Third-party backends should declare the version they implement as
`BACKEND_PROTOCOL_VERSION: ClassVar[int]` and assert it against the canonical
constant at import time, the same pattern `PostgresBackend` and
`InMemoryBackend` use (`_EXPECTED_PROTOCOL_VERSION` + `RuntimeError`), so a
contract bump fails fast instead of drifting silently.

`retry_job` puts a job that has come to rest back to `pending` so it can be
re-dispatched. Every resting state is a valid source (`failed`, `crashed`,
`cancelled`, `abandoned` and `succeeded`), because an operator re-run means
"run this again": `succeeded` records that the actor returned without raising,
not that the result was right, and `abandoned` means a deploy interrupted the
job. A `running` job is refused, because re-pending a row while an attempt is
live races that attempt's terminal write and the job could execute twice; a
`pending` or `scheduled` job is refused because it is already queued to run.
Returns `True` if the job was retried, `False` otherwise. The admin UI exposes
this via the `POST /jobs/{job_id}/retry` endpoint.

`subscribe_cancel_wake` is the cancel-signal analogue of `subscribe_wake`: it
yields an `asyncio.Event` that is set whenever a cancel NOTIFY arrives, allowing
the heartbeat loop to interrupt its sleep immediately on cancellation rather
than waiting for the next scheduled tick.

### Why Protocol, not ABC

The `Backend` is a `Protocol` (structural subtyping) rather than an abstract base
class. This means:

- `InMemoryBackend` and `PostgresBackend` satisfy it without inheriting from it.
- Third-party backends can satisfy the interface without importing TaskQ internals.
- `@runtime_checkable` allows `isinstance(obj, Backend)` checks at wiring time.

### `supports_transactional_simulation`

`PostgresBackend` sets this to `False`: atomicity comes from real PG transactions.
`InMemoryBackend` sets it to `True`: `SubJobEnqueuer` buffers sub-job `EnqueueArgs`
and flushes on success or discards on failure to simulate rollback semantics.

---

## State Machine

Defined in `src/taskq/backend/statemachine.py` and mirrored as a PG enum in
`src/taskq/migrations/01.00.00_01_pre_initial.sql`.

### Statuses

| Status | Terminal | Description |
|---|---|---|
| `pending` | No | Queued, ready for dispatch |
| `scheduled` | No | Deferred; `scheduled_at` is in the future |
| `running` | No | Dispatched, held by a worker lock |
| `succeeded` | Yes | Actor returned successfully |
| `failed` | Yes | Actor raised a non-retryable error, or retry budget exhausted |
| `cancelled` | Yes | Cancelled before or during execution |
| `crashed` | Yes | Worker died (lock expired) with no retries remaining |
| `abandoned` | Yes | Forced cancellation completed (cancel_phase=2 + grace elapsed) |

### Valid transitions

```
pending   → running (dispatch), cancelled (cancel request), failed (deadline sweep)
scheduled → pending (scheduled_to_pending sweep), cancelled, failed (deadline sweep)
running   → succeeded, failed, cancelled, crashed, abandoned, scheduled (snooze/retry/RetryAfter),
            pending/scheduled (shutdown interrupt), failed (hold past schedule_to_close)
succeeded → (terminal)
failed    → (terminal)
cancelled → (terminal)
crashed   → (terminal)
abandoned → (terminal)
```

`assert_valid_transition(from_status, to_status, job_id)` is the application-level
guard. The SQL `WHERE status = 'X'` predicate is the authoritative serialization
gate: two concurrent writers cannot both transition the same row because only one
can hold the row lock from the dispatch CTE's `FOR UPDATE SKIP LOCKED`.

`statemachine.py` also exports `ACTIVE_STATUSES`: the complement of
`TERMINAL_STATUSES` over the full `JobStatus` set (pending, scheduled, running),
derived from `VALID_TRANSITIONS` keys.  `JobFilter(active=True)` uses this set
so that adding a new non-terminal state to the state machine automatically
extends the active filter without a second edit.

### Which component drives each transition

| Transition | Driver |
|---|---|
| pending → running | Dispatch CTE (producer loop) |
| scheduled → pending | `scheduled_to_pending` sweep (leader) |
| running → succeeded | Consumer after actor returns |
| running → failed | Consumer after error / deadline |
| running → scheduled | Consumer on `Snooze` / `RetryAfter` / transient retry |
| running → cancelled | Consumer after cancel_phase=1 (cooperative) |
| running → cancelled | `reclaim_expired_locks` sweep (leader, Sweep 1: cancel in-flight, retries exhausted) |
| running → abandoned | `CancelController.run_post_tx` (heartbeat, post-phase-3) / shutdown RELEASING phase (operator cancel in flight only) |
| running → crashed | `reclaim_expired_locks` sweep (leader, Sweep 1) |
| running → pending/scheduled | `mark_interrupted` (consumer on a shutdown-origin cancel; shutdown RELEASING phase); the attempt is NOT refunded (it started executing), `interrupt_count` bumps; `pending` when the actor has provably exited (async actor unwound, sync actor's thread finished, transactional unwind done; the consumer parks on the tracked exit handles, bounded by the remaining termination budget, before writing), `scheduled` behind the process's exit window (deadline + watchdog exit tail) when it has not |
| pending/scheduled → cancelled | `write_cancel_request` (client) |
| pending/scheduled → failed | `deadline_sweep` (leader, Sweep 2) |

Sweep 1 (`running → crashed` / `running → cancelled` / `running → pending`)
additionally writes a fleet-wide-pollable `job_events` outbox row in the same
transaction as the reclaim UPDATE.  Consumers observe crash-reclaimed jobs via
`Backend.poll_reclaim_events(after_id)` or `TaskQ.watch_reclaims(after_id)`
without enumerating every `job_id`.

A `running → pending` reclaim (crash or heartbeat) reschedules through the
job's own `RetryPolicy` (base, cap, backoff kind and jitter), exactly as an
application-level failure does, not a hardcoded flat interval, and clamped at
the same effective cap: the lesser of the policy's `cap` and the operator's
`TASKQ_MAX_RETRY_BACKOFF` ceiling (24 h default). The sweep
runs on a leader that need not host the crashed job's actor at all, so the
curve cannot be read from a live registration; instead it is stamped onto
the row at enqueue time (`retry_base_seconds` / `retry_cap_seconds` /
`retry_backoff` / `retry_jitter`, migration
`01.00.12_03_pre_reclaim_retry_policy.sql`) from the enqueuing client's
`ActorRef.retry`, the same way `max_attempts` and `retry_kind` already are.
Jitter is evaluated per row, so a fleet-wide event that reclaims a whole
cohort at once spreads their `scheduled_at` across a band instead of
stamping every row with the same instant.

Delivery is **at-least-once, and gap-free under a bounded-transaction-duration
assumption**, not an unconditional guarantee. `job_events.id` (`bigserial`)
is allocated at INSERT time, so under concurrent sweep transactions, id
allocation order and commit order can diverge: a transaction holding a lower
id can commit *after* one holding a higher id. A naive `id > cursor` poll
would return the higher-id row immediately, advance the cursor past it, and
permanently lose the lower-id row once its transaction finally committed.

Two SQL-only fixes were tried and both failed to close this gap, for
reasons worth recording so they aren't retried:

- A per-row filter comparing each row's inserting-transaction id against
  `pg_snapshot_xmin(pg_current_snapshot())` only checks whether *that row's
  own* transaction is complete: it cannot detect a *different*,
  still-uncommitted transaction sitting at a lower `event_id`, because that
  row is invisible under MVCC to any plain `SELECT`, not merely filtered out.
  No predicate computed only over visible rows can bound something it cannot
  see.
- A gap-detection variant (treat any hole in the visible `id` sequence as
  "possibly still forthcoming" while `pg_current_snapshot()` reports any
  transaction in progress) fails for a structural reason: `job_events` is a
  shared, multi-kind table (state changes, cancel requests, progress) written
  continuously by unrelated code paths, so some transaction is touching it
  almost continuously in a live system: the "anything in progress" signal is
  effectively always true, which would stall reclaim-event delivery
  indefinitely rather than only during genuine contention. (Separately, this
  environment's `pg_snapshot_xmin`/`pg_snapshot_xmax` did not reflect a
  confirmed-active concurrent transaction in ad hoc testing; a further reason
  not to depend on them here without deeper investigation.)

Instead, `poll_reclaim_events` uses a **trailing-watermark (visibility delay)
filter**: `id` (`nextval`) and `occurred_at` (`clock_timestamp()`) are stamped
by the same INSERT statement, so they are co-monotonic: an earlier id has an
earlier-or-equal `occurred_at`. (Co-monotonicity itself is an assumption, not
a guarantee: the two values come from separate volatile calls that Postgres
does not evaluate atomically across concurrent transactions, so one
transaction can in principle evaluate both of its own between another's
`nextval` and `clock_timestamp()`, stamping a lower id with a later
`occurred_at`; the window is nanosecond-scale and no occurrence is known,
but the watermark is only as exact as this non-interleaving assumption.)
Only rows older than
`taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY` (2 seconds by default) are
returned: by the time a row clears that margin, any transaction that could
have inserted a still-lower id has had at least as long to commit, so it must
have either committed (returned, correctly ordered, in this or an earlier
poll) or aborted (permanently gone, safe to skip). **This assumes no
`job_events` writer takes longer than the margin between its INSERT and its
commit**: true for TaskQ's short, single-round-trip sweep and terminal-write
transactions, and for the batch event-writers (sweeps 1-3, bulk cancel,
deregistration) it is now an *enforced* property rather than a hope: each
batch is capped at `event_writer_batch_size` rows and its transaction carries
a server-side `statement_timeout` at 7/8 of the margin, so a batch that
cannot fit inside the watermark margin is aborted by the server instead of
silently blowing it (see
[Maintenance Sweeps](guides/maintenance-sweeps.md)). The assumption remains
conditional, not a guarantee the SQL can make on its own: a stalled or
GC-paused worker holding a transaction open, or a non-batch writer, could
still in principle exceed the margin and reproduce the gap, which is what the
`check_reclaim_visibility_delay_risk` detector below watches for. Consumers
must be idempotent (dedupe on `event_id`).
Configurable via `WorkerSettings.reclaim_event_visibility_delay` /
`TASKQ_RECLAIM_EVENT_VISIBILITY_DELAY` (and per-call via
`poll_reclaim_events(..., visibility_delay=...)`); raise it under heavy
sweep contention or large batches, lower it if latency matters more and
writes are known to be fast.

**Detecting a violation.** A silent-failure mode nobody can see is worse
than a slower one that's visible, so detection is wired in by default,
not left for operators to discover: every `TaskQ.watch_reclaims()`
consumer runs `PostgresBackend.check_reclaim_visibility_delay_risk` on a
slow cadence (once a minute; cheap, and far outside the per-poll hot
path, which is why the diagnostic is *not* part of the `Backend`
protocol) and logs a loud structured
`watch-reclaims-visibility-delay-at-risk` warning for every transaction
it finds holding `job_events` open past the margin. The diagnostic
itself (`PostgresBackend.check_reclaim_visibility_delay_risk`) queries
`pg_locks`/`pg_stat_activity` and returns `LongRunningJobEventsWriter`
rows; it remains available standalone for a dedicated
monitoring/alerting loop that wants tighter cadence or its own sink.
Either way it is a *proxy* signal, not proof of an actual miss; it
cannot see whether that transaction will insert a `job_events` row
before committing, only that it has held the table open unusually long,
so it can both false-positive (an unrelated long-running transaction
that merely touched `job_events` once) and false-negative (a writer
that inserts and commits within the margin every time, even if some
other assumption about the deployment is wrong).

**Worked example: fan-out completion.** The motivating use case: a
producer fans out N jobs and must fire a callback when *all* of them
reach a terminal state. Without a reclaim feed, a SIGKILLed worker
leaves the counter stuck at 1 forever (the job is retried or crashed in
SQL, and nobody in application code hears about it):

```python
outstanding = len(job_ids)


async def track_completions(tq: TaskQ) -> None:
    global outstanding
    cursor = await load_reclaim_cursor()  # your own durable store
    async for evt in tq.watch_reclaims(after_id=cursor):
        # evt.detail: from_state/to_state ('pending' retry, or terminal
        # 'crashed'/'cancelled'), reason='lock_expired', worker_id.
        if evt.detail["to_state"] != "pending":  # terminal reclaim only;
            outstanding -= 1  # retries redispatch normally
        cursor = evt.event_id
        await save_reclaim_cursor(cursor)  # persist AFTER processing;
        # a crash before this re-delivers the event (at-least-once;
        # dedupe on event_id if your decrement isn't idempotent)
        if outstanding == 0:
            await fire_completion_callback()
```

Terminal states reached on the normal path (success, failure,
cooperative cancel) are counted as each job's own result is recorded:
`watch_reclaims` exists to close the crash gap, where *no* application
code runs. Only terminal reclaims decrement the counter: a
`to_state='pending'` event means the job was rescheduled and will be
counted when it eventually lands terminal. The producer must only prune
`job_events` rows older than every live consumer's persisted cursor;
rows pruned before a slow consumer reads them are permanently lost to
that consumer.

`TaskQ.watch_reclaims()`'s PG LISTEN transport wakes on the reclaim
`pg_notify` for low latency, but a NOTIFY-triggered poll can still come up
empty if the event hasn't cleared the visibility-delay margin yet; it then
retries on a short, bounded cadence (`_catch_up_after_notify`) until the
margin elapses, rather than falling back to a full (possibly much longer)
`poll_timeout` wait.
This is the standard "polling publisher" mitigation for the transactional
outbox pattern's well-known bigserial-ordering hazard; the alternative,
fully exact fix is to read commit order directly off the WAL (logical
decoding / CDC), which is out of scope here.

Sweep 1 now fires one `pg_notify` per sweep call that reclaims at least one
row of *either* kind (previously only when the retry branch produced a
row), which is a **wake-channel semantics change**, not purely a bugfix:
`wake_channel` previously meant only "new dispatchable work"; it now also
means "something changed on job_events," so every crash-reclaim wakes every
subscriber (including pure-dispatch workers with no interest in it).
Crashes are rare so the added wakeup cost is low, but the channel's meaning
has broadened.

The index added by migration `01.00.02_01_pre_job_events_outbox.sql` uses
`CREATE INDEX` (not `CONCURRENTLY`) and takes an exclusive lock on
`job_events` for the duration of the build, which can stall writes to that
heavily-written table. The migration runner supports a per-migration opt-out
from its default transaction wrapper: the `-- taskq:no-transaction` header
directive, which unlocks `CONCURRENTLY` forms (see
[Non-transactional migrations](guides/upgrading.md#non-transactional-migrations)),
but bundled migrations deliberately remain transactional (pinned by
`test_bundled_migrations_are_all_transactional`), so operators with a
large/populated table should still run the equivalent `CREATE INDEX
CONCURRENTLY` manually during a maintenance window (see the migration file
for details). Future index migrations on hot tables can adopt the directive
instead.

Migration `01.00.06_01_pre_cancel_and_cascade_indexes.sql` follows the same
precedent with the same caveat: its plain transactional `CREATE INDEX`
statements take a write-blocking lock on `jobs` (the hottest table; enqueue,
dispatch and heartbeat all write it) and `job_attempts` for the duration of
each build. It stays transactional deliberately: the `CONCURRENTLY` form
deadlocks under the runner's own serialized-migrator advisory lock, so apply
it during a maintenance window (or when `jobs` is small/quiescent, e.g. right
after a prune sweep) on any deployment where `jobs` is large; the migration
file's header carries the full derivation.

---

## Dispatch CTE

Source: `src/taskq/backend/_dispatch_sql.py`.

The dispatch CTE is a single atomic `UPDATE … RETURNING *` statement. It acquires
row locks and transitions `pending` → `running` for a batch of jobs. TaskQ ships
two dispatch SQL variants selected per-queue at dispatch time:

- **`DISPATCH_STRICT_FIFO_SQL`**: priority-then-time ordering. Best for queues
  with no fairness requirements.
- **`DISPATCH_ROUND_ROBIN_SQL`**: per-fairness-key interleaving (lateral dispatch).
  Prevents deep queues of one actor or tenant from starving others. See [Queue modes](#queue-modes) below.

### Queue modes

Each queue has a `mode` column in the `queues` table: `strict_fifo` (default) or
`round_robin`. The dispatch batch method selects the SQL variant via
`_resolve_queue_modes()`, served from a per-worker TTL cache (5 s): a cache hit
adds no query to the round, the miss path runs the one indexed
`queues` read and refills the cache, and `taskq queues set-mode` invalidates the
caches of the process it runs in, so a mode flip reaches every worker within
the TTL.
Queues absent from the table default to `strict_fifo`.

A dispatch round runs in autocommit: no `BEGIN`/`COMMIT` around the claim.
The claim is one atomic `UPDATE … RETURNING` whose row locks end with the
statement, the mode resolve and the claimable probe are read-only, and an
empty round holds no locks between window expansions, so a transaction added
two round trips per round and nothing else (asyncpg sends `BEGIN` and `COMMIT`
as separate statements). A cache-hit round that claims is exactly one
statement; an empty round is the claim plus one probe (pinned by
`tests/test_round_trip_budgets.py`).

| Mode | Ordering | Use case |
|---|---|---|
| `strict_fifo` | `priority DESC, scheduled_at, id` within an actor; across actors sharing the queue the round rotates (each eligible actor's rank-1 job admits before any actor's rank-2, so the LIMIT cuts per-actor prefixes, never a global priority order) | No fairness requirement; simple priority queue |
| `round_robin` | `fairness_rank, priority DESC, scheduled_at` within an actor; the same cross-actor rotation applies | Multi-tenant or multi-cohort queues where one busy actor must not starve others |

The round-robin mode computes `fairness_rank` via:
```sql
ROW_NUMBER() OVER (PARTITION BY COALESCE(fairness_key, '__null__')
                   ORDER BY priority DESC, scheduled_at)
```
Jobs without a `fairness_key` collapse into a single `__null__` cohort, equivalent
to `strict_fifo` within that cohort. See [guides/jobs-clients.md](guides/jobs-clients.md) for `fairness_key` usage.

### Common CTE structure

Both variants share the same CTE shape up to the `candidates` phase:

```
params              → bind $1 queues, $2 limit_n, $3 worker_id, $4 lock_lease, $5 oversample
running_per_actor   → count running jobs per actor (for max_concurrent cap)
running_identities  → set of (actor, identity_key) in running status
per_actor_capacity  → residual = max_concurrent - in_flight per actor
candidates          → CROSS JOIN LATERAL per-actor, filtered by queue/status/scheduled_at,
                      limited to residual * oversample per actor
```

The `candidates` CTE differs between modes:
- **`strict_fifo`:** sorts by `priority DESC, scheduled_at, id`; `fairness_rank` is `NULL`.
- **`round_robin`:** computes `fairness_rank` via `ROW_NUMBER() OVER (PARTITION BY COALESCE(fairness_key, '__null__') …)`.

After `candidates`, both variants share identical downstream CTEs:

```
identity_dedup      → DISTINCT ON (actor, identity_key) for identity-gated jobs
                      UNION ALL non-identity jobs
ranked              → ROW_NUMBER() OVER (PARTITION BY actor ORDER BY …) as pending_rank
                      (round_robin: ORDER BY fairness_rank, priority; strict_fifo: ORDER BY priority)
                      MATERIALIZED: the optimization fence that finalizes ranks before the cut
capped_ranked       → the ranked rows of actors carrying a max_concurrent cap
top_ids             → capped actors only: the pre-lock window, LIMIT limit_n
locked              → capped: FOR UPDATE SKIP LOCKED over the top_ids window
sliding_locked      → uncapped: ORDER BY + LIMIT limit_n + FOR UPDATE SKIP LOCKED at one
                      query level, so the lock node slides past peers' locked rows down
                      the window instead of stopping at it
claimed             → locked UNION ALL sliding_locked
eligible_candidates → LEFT JOIN actor_config for max_concurrent
                      LEFT JOIN running_per_actor for in_flight count
                      BOOLEAN gate: in_flight < max_concurrent
                      ROW_NUMBER() OVER (PARTITION BY actor …) for per-actor ranking
eligible            → cap: actor_rank <= max_concurrent - in_flight
                      ORDER BY pending_rank, fairness_rank, priority DESC,
                               actor_claimed_at, scheduled_at; LIMIT limit_n
                      (the rotation order the lock stages cut on: each eligible
                       actor's rank-1 job admits before any actor's rank-2)
UPDATE jobs         → WHERE j.id IN eligible AND j.status = 'pending'
                      SET status='running', started_at=clock_timestamp(), attempt=attempt+1, …
```

### Key correctness invariants

1. `FOR UPDATE SKIP LOCKED` is confined to the two lock-step CTEs (`locked` for
   capped actors, `sliding_locked` for uncapped). PostgreSQL forbids
   window functions and `FOR UPDATE` in the same `SELECT`; the `candidates`
   passthrough CTE is mandatory.

2. The boolean gate (`in_flight < max_concurrent`) is necessary but not sufficient
   alone. Two concurrent producers seeing `in_flight=0` would both dispatch up to
   `limit_n` jobs for the same actor. The `actor_rank <= max_concurrent - in_flight`
   cap in `eligible` closes this gap.

3. The final `WHERE j.status = 'pending'` race guard prevents re-dispatch if
   another producer transitioned the row between lock acquisition and the UPDATE.

4. Expected over-count: `(num_producers - 1) * max_concurrent` jobs may be
   dispatched beyond the cap per round under concurrent producers. This is a
   documented, bounded tradeoff: the sweep loop reclaims stale locks.

5. Per-actor oversampling (`LIMIT pac.residual * oversample`, default `oversample=2`
   via `TASKQ_DISPATCH_OVERSAMPLE`) absorbs filtering from max_concurrent caps and
   identity serialization. `residual` is the actor's remaining dispatch slots this
   round; oversampling reads a multiple of that per-actor LATERAL, not a multiple of
   the overall `limit_n`. Under pathological workloads (all candidates share one
   identity) the producer retries on the next tick. The window also bounds the
   SKIP LOCKED slide under concurrent dispatchers: a round whose whole window is
   row-locked by peers re-runs the claim with a geometrically doubled window (up to
   8×, gated by a bounded `LIMIT 1` claimable-rows probe so idle rounds stay one
   statement), then defers to the next tick.

---

## DI Engine

Source: `src/taskq/_di/`.

### Component overview

| File | Role |
|---|---|
| `registry.py` | `ProviderRegistry`: registration, validation, plan cache |
| `scope.py` | Re-export shim for `Scope`: the canonical definition lives in `src/taskq/_scope.py` (PROCESS=0, THREAD=1, LOOP=2, TRANSIENT=3) |
| `scopes.py` | `ScopeContainer`, `ProcessScope`, `ThreadScope`, `LoopScope`, `build_actor_scope` |
| `solver.py` | `solve_dependencies`: resolves kwargs dict for a callable |
| `lifecycle.py` | Detects provider lifecycle from class/factory shape |
| `_validate.py` | Five-phase startup validation (cycle detection, scope rules, missing providers) |

### Scope nesting

```
PROCESS (widest)
  └── THREAD
        └── LOOP
              └── TRANSIENT (narrowest, per actor invocation)
```

A narrower scope may depend on a wider scope (a TRANSIENT provider may inject a
LOOP-scoped connection). A wider scope must not depend on a narrower scope; this
would mean the longer-lived singleton depends on something that might not exist.
Violations are detected at `registry.validate()` time and raise `ScopeViolation`.

### Solver algorithm

`solve_dependencies(func, registry, scope_containers, passthrough_kwargs)`:

1. Calls `get_type_hints(func, include_extras=True)` to collect annotated parameter
   types.
2. For each parameter (excluding `return` and any name present in the caller-supplied
   `passthrough_kwargs` dict; in practice this is how `payload` and `ctx` are
   excluded from DI lookup, since callers pass them through by name rather than the
   solver hardcoding those parameter names):
   - Unwraps `Annotated[T, Scope.X]` to extract the type `T` and any scope override.
   - Looks up `T` in the registry to get the `ProviderEntry`.
   - Selects the effective scope (override if present, else the entry's registered
     scope).
   - Calls `scope_containers[effective_scope].get_or_create(T, entry)`.
3. Returns a `kwargs` dict ready for `**kwargs` injection.

The solver never calls factories directly. Factory invocation, caching, and
teardown registration are the `ScopeContainer`'s responsibility (Decision 6).

### Per-invocation actor scope

`build_actor_scope` (an async context manager) opens a `TRANSIENT` scope container,
resolves all DI kwargs for the actor function, yields a `ResolvedActorScope`, and
on exit closes the TRANSIENT scope in LIFO order via the log-and-continue teardown
policy (every teardown runs even if earlier teardowns fail; `CancelledError` is
re-raised after all teardowns complete).

The TRANSIENT container teardown is shielded with `asyncio.shield` to prevent
a cancellation in the actor body from short-circuiting cleanup and leaking resources.

---

## Cancellation Protocol

Source: `src/taskq/worker/cancel.py`.

Cancellation proceeds through three in-DB phases plus one in-process sentinel:

| Phase | Value | Location | Meaning |
|---|---|---|---|
| `NONE` | 0 | PG + in-process | No cancellation requested |
| `COOPERATIVE` | 1 | PG + in-process | Cancel requested; actor's `cancel_event` will be set |
| `FORCED` | 2 | PG + in-process | Grace elapsed; asyncio task cancelled |
| `ABANDON_PENDING` | 3 | In-process only | Queued for post-transaction `mark_abandoned` |

`ABANDON_PENDING` is never written to PG (`cancel_phase BETWEEN 0 AND 2` check
constraint enforces this).

### Three-phase walkthrough

`CancelController.run_in_tx(conn)` runs inside the heartbeat transaction on every
tick:

**Phase 1: Cooperative**

The heartbeat reads `cancel_requested_at IS NOT NULL AND status='running'` rows for
this worker via `POLL_CANCEL_FLAGS_SQL`. On first observation of `db_phase >= 1`:
- Sets `active.ctx.cancel_event.set()` (signals the actor).
- Records `cancel_observed_at = loop.time()` (monotonic, not wall clock).
- Sets local `cancel_phase = COOPERATIVE`. No PG write in this phase.

**Fast-advance**

If `db_phase == FORCED` while local is still `< FORCED`, the controller advances
locally without writing to PG (another controller already escalated).

**Phase 2: Forced**

After `cancellation_grace_period` elapses since `cancel_observed_at`:
- Executes `CANCEL_ESCALATION_SQL` (`SET cancel_phase = 2 WHERE cancel_phase = 1`).
- Inserts a `job_events` row (`kind='state_change'`, phase 1→2 detail).
- Calls `active.task.cancel()` (asyncio task cancellation).
- **PG write happens BEFORE `task.cancel()` with no intervening `await`.**

**Phase 3: Abandonment**

After `cancellation_grace_period + cleanup_grace_period` elapses:
- Sets `active.cancel_phase = ABANDON_PENDING` (in-process sentinel).
- Appends `job_id` to `_pending_abandons` deque.
- Does NOT call `mark_abandoned` here; the heartbeat transaction holds an UPDATE
  lock on the row; calling `mark_abandoned` (which opens a separate pool connection)
  would self-deadlock.

`CancelController.run_post_tx()` runs after the heartbeat transaction commits:
- Drains `_pending_abandons`.
- Calls `mark_abandoned(job_id)` (gated on `cancel_phase = 2`).
- Calls `active_jobs.deregister(job_id)`.

### Consumer skip guard

The consumer skips `mark_cancelled` when `cancel_phase >= ABANDON_PENDING` (phase 3).
`run_post_tx` owns the terminal write for phase-3 jobs. This prevents a race where
both the consumer and the heartbeat attempt a terminal write.

### Crash-reclaim interaction

The phases above only ever advance on the lock-holding worker. If that worker
dies mid-protocol, Sweep 1 (`reclaim_expired_locks`) eventually reclaims the job
, after a flat extra 60s of headroom on top of `cancel_grace + cleanup_grace`,
so a merely-slow cancellation isn't mistaken for a crash. What the reclaim does
with the in-flight cancel state puts operator intent first:

- **Cancel branch** (`cancel_phase != 0`, any retry budget): the job lands on
  **`cancelled`**, not `crashed`; the caller's explicit request is the honest
  terminal label: anyone reconciling terminal states sees the cancel was
  honored. The row **keeps** `cancel_phase`/`cancel_requested_at` as the audit
  trail of the honored request, exactly as the worker-honored terminal write
  (`mark_cancelled`) does. The `job_attempts` row still records
  `outcome='crashed'` (`WorkerCrashed`): that IS what happened to the attempt.
- **Retry branch** (`cancel_phase = 0`, retries remaining): `running → pending`
  on the row's own backoff curve. The row's cancel columns were already clean
  (the cancel branch took every cancel-carrying row), so the next claimant
  never inherits a phase.

The earlier design evaluated the retry budget *before* `cancel_phase`, so a
retryable job with a cancel in flight went back to `pending` with its cancel
columns wiped, the operator's request silently lost: the cancel call itself
had returned normally (the request was initiated), while the row was re-pended
uncancelled with no surviving marker of the request. The reset-on-re-pend that
design carried existed to prevent a re-cancel loop (a re-pended row still
carrying `cancel_phase` would be re-cancelled by each new claimant, then
reclaimed again). Ordering the cancel branch first removes the loop
structurally instead: the cancel branch never re-pends (it terminalises), so
no arm of the sweep can produce a re-pended row carrying cancel columns, and
the retry branch resets columns that are already clean. `isolate_self`
(worker heartbeat loss) mirrors this ordering branch for branch; its one
deliberate asymmetry is that it applies the cancel branch immediately, with no
grace headroom, because the isolating worker is by definition going away now
and no lock-holder remains to complete the cooperative protocol.

### `CancelController` Protocol

```python
@runtime_checkable
class CancelController(Protocol):
    async def run_in_tx(self, conn: asyncpg.Connection) -> None: ...
    async def run_post_tx(self) -> None: ...
```

Test stubs need only implement these two methods. The production implementation is
`_CancelController`, constructed via `make_cancel_controller(deps, worker_id, backend)`.

---

## Leader Election

Source: `src/taskq/worker/leader.py`.

### Mechanism

Leadership is a lease on the `maintenance_leader` row. The holder writes an
`expires_at` of its own choosing (`leader_lease`, default 40 s) and renews it
every `heartbeat_interval` over `deps.leader_conn` (a dedicated, non-pooled
connection) under a term fenced by `(worker_id, elected_at)`. Any pod may
take the row over once that instant has passed.

On each heartbeat tick, a pod that is not leading runs one statement that
claims the row if it is unclaimed or its holder has gone quiet, and returns
nothing if a live holder owns it. A pod that is leading renews instead, and
its renewal returning nothing means a successor holds the row.

"Gone quiet" means both of the liveness signals the row can carry have
stopped: the lease the holder chose has expired, and its `last_seen_at` has
been silent for four heartbeat intervals. Two are needed because during a
roll the row can be written by two protocols: a pod from a release that
predates the lease names four columns in its upsert and leaves whatever
`expires_at` it found in place, so the expiry alone would judge a pod that is
pinging every tick to be dead. Requiring both costs nothing against the
failover bound: the lease is never shorter than four heartbeat intervals, so
for a pod that does lease the expiry is always the later horizon.

Split-brain is prevented by a local trust window rather than a per-statement
fence: the leader trusts its term only until `attempt_started + leader_lease -
1 s` on its own monotonic clock and stands down before that, while peers may
take over only after the server's `expires_at`, which was written after that
same instant plus the full lease. Every leader-gated loop consults
`deps.leading()` on each iteration rather than the bare `is_leader` event, so
a process resuming from a suspension finds the gate already closed. Clock
offset between machines is irrelevant; each side measures a duration from its
own instant.

The election loop sets the event and the term together and clears them
together, and it clears them before any await that could release the courtesy
lock: a peer must never be able to elect while this process still reads
`leading()` as true. The two are exposed separately because `is_leader` is
what the watchdog parks on and what the gauge and health report publish, while
the term is what narrows how long that role is trusted between renewals.

The schema-qualified advisory lock (`taskq:maintenance_leader:<schema>`, built
by `taskq.constants.schema_lock_name`) is still taken after winning. It keeps a
pod from a release that knows only the lock from leading beside a lease holder
during a roll. It is never required and never waited on: the row alone decides
the election in every state (held, lapsed, or absent), so a lock that
outlives the row behind it (a candidate dead between its lock attempt and its
election write, a departed leader's lingering session) blocks nothing.
Recovery from a holder that died without closing its session depends only on
the lease, and so needs no privilege beyond `UPDATE` on the row. A graceful
stop hands over faster than the lapse: the leader's teardown resigns the row,
so a follower's next election cycle already finds it free. Failover from a
holder that dies without a FIN is bounded by `leader_lease +
heartbeat_interval` plus one round trip.

A pod that keeps *winning* the row but cannot finish the assume (the
dedicated monitor and cron connections refuse to open (connection-count
pressure, a credential-factory outage) is handed back on the same budget.
The own-row arm lets it re-win every heartbeat, which refreshes the row's
lease and ping, so peers' lapse predicate never matches: left alone, nobody
leads for as long as the failure lasts, fleet-wide. The election loop
therefore anchors the first such failure's trust window and, once it is
spent, resigns the row through the fence (`_hand_back_unassumable_lease`,
fenced on the current win's term so a peer that already took over is never
deleted); the anchor is cleared by a successful assume or by an observed
election loss (a peer holds the row; a later win starts a fresh episode),
so a pod that re-wins while still broken hands the row straight back rather
than buying a fresh window. One failed open inside the window is a blip that
costs nothing: the own-row arm's cheap route back (a credential reload) is
exactly what that window preserves, and the worst case is the dead-leader
SLA plus the duration of one failing cycle's connection attempts (each
bounded by `reload_factory_timeout`, 30 s at defaults, so roughly
`leader_lease + heartbeat_interval + 30 s` ≈ 80 s at defaults): the bound
rests on the hand-back, not the lapse, because sub-lease re-wins keep
refreshing `expires_at` and the lapse backstop cannot fire while the pod
keeps winning.

### What the leader does

`MaintenanceLeader` runs eleven cooperative loops in a `TaskGroup`:

1. **Election loop**: claims and renews the leadership lease.
2. **Watchdog**: detects a lost connection to Postgres on a dedicated monitor
   connection, faster than the renewal cadence would.
3. **Scheduled-wake (Sweep 3)**: promotes `scheduled` → `pending` when
   `scheduled_at <= statement_timestamp()` (a STABLE bound, so
   `jobs_scheduled_wake_idx` serves it as an index condition). Sends
   `pg_notify` after promoting to wake consumer loops. One bounded batch per
   one-second tick (`event_writer_batch_size` rows); a larger due backlog
   drains across ticks.
4. **Cron**: fires cron-scheduled actors at their declared cadence, at most
   `cron_tick_limit` schedules per one-second tick under the schema-qualified
   cron advisory lock (`taskq:cron:<schema>`, transaction-scoped).
5. **Sweep (Sweeps 1, 2, 4)**: **leader-only** (gated on `ctx.deps.is_leader`),
   runs every `sweep_interval` (default 30 s): `reclaim_expired_locks`
   (Sweep 1, uses `FOR UPDATE SKIP LOCKED`), `deadline_sweep` (Sweep 2), and,
   when the backend supports them, `sweep_leaked_reservation_slots` (Sweep 4),
   `sweep_expired_results`, `cleanup_stale_workers`, and
   `complete_stale_batches` (see [Batch Subsystem](#batch-subsystem)). Every
   event-writing sweep is a bounded batch writer: one call transitions at
   most `event_writer_batch_size` rows in one short transaction carrying a
   server-side `statement_timeout`, and a non-empty call drains up to
   `sweep_drain_batches` batches per tick before leaving the remainder to the
   next tick (see [Maintenance Sweeps](guides/maintenance-sweeps.md) for the
   derivation).
6. **Prune (Sweep 5)**: runs daily (default 03:00 UTC). Moves terminal jobs
   (`succeeded`, `failed`, `cancelled`, `crashed`, `abandoned`) from `jobs` to
   `jobs_archive` once their per-status retention period has elapsed. Batched at
   10 000 rows per CTE; atomic move+delete within each batch. After job pruning
   completes, `prune_old_batches` deletes completed batch rows past the same
   cutoff (see [Batch Subsystem](#batch-subsystem)). Controlled by
   `TASKQ_PRUNE_*` settings.
7. **Archive expiry (Sweep 6)**: runs daily (default 04:00 UTC, 1 hour after
   prune). Hard-deletes rows from `jobs_archive` once their `expire_at` has
   passed. Cascades to `job_attempts_archive`. Controlled by
   `TASKQ_ARCHIVE_EXPIRY_*` settings.
8. **Queue-depth sampling**: samples queue counts every 15 seconds for OTel
   gauges.
9. **Reservation sampling**: samples reservation-slot usage every 15 seconds
   for OTel gauges.
10. **Backlog detection**: samples jobs-by-status and oldest-due-age gauges
    every `queue_depth_interval`. Deliberately **not** leader-gated, unlike
    every sibling sampler: a detector hosted behind the leadership gate emits
    nothing under the very failure (election loss, another schema holding the
    old-style lock) it exists to expose, so every worker samples.
11. **Stranded-jobs detector**: runs every 60 s. Warns about pending/scheduled
    jobs whose actor has no `actor_config` row (e.g. the actor was removed from
    the registry but jobs remain enqueued).

Failover SLA: leader gap ≤ `heartbeat_interval + 1s` on worker kill.

---

## NOTIFY / Wake Mechanism

Source: `src/taskq/worker/notify.py`, `src/taskq/constants.py`.

### Channels

Three Postgres LISTEN channels are subscribed per worker:

| Channel | Format | Payload |
|---|---|---|
| `taskq_wake_{tag}` | `wake_channel(schema)` | Empty (payload ignored; notification alone triggers dispatch) |
| `taskq_events_{tag}` | `events_channel(schema)` | JSON: `{"type": "cancel", "worker_id": "...", "job_id": "..."}` |
| `taskq_worker_{tag}_{worker_id}` | `worker_channel(schema, worker_id)` | Same JSON format; no worker_id filtering needed |

`{tag}` is `schema_channel_tag(schema)`: the first 10 hex digits of
`sha224(schema)` (`taskq` → `124a200651`). Channels are Postgres identifiers
bounded by 63 bytes (`LISTEN` silently truncates a longer name and
`pg_notify` rejects it), while a schema name may itself be 63 characters, so
a channel that interpolated the schema stopped matching its listener past a
schema length (14 characters for the per-worker channel). The fixed-width tag
makes every channel's length independent of the schema; `check_channels_fit`
runs at settings load so a template change cannot reintroduce the cliff. The
wake trigger derives the same tag in SQL
(`left(encode(sha224(...), 'hex'), 10)`), pinned end to end by
`tests/test_notify_channel_length.py`. The progress channels
(`taskq:{tag}:progress:{job_id}`, `taskq:{tag}:progress`) and the cron
commit-gate channel (`taskq_cron_commit_{tag}`) use the same tag.

Channel name helpers validate the schema identifier against `_IDENT_RE` before
hashing. Each schema gets its own set of channels, enabling multi-tenant
deployments on a single PG instance. The per-worker channel enables targeted
event delivery without fleet-wide fanout.

### Enqueue path

The wake is the `tr_notify_job_insert` row trigger's: `AFTER INSERT ON jobs
… WHEN (NEW.status = 'pending')`, it issues `pg_notify(wake_channel(schema),
NEW.queue)` for every row that lands dispatchable: the payload names the row's queue, which the
listener side filters on. The single and batch INSERT
paths issue no notify of their own and decide `status` server-side in the
INSERT (a future `scheduled_at` lands as `scheduled`), so the WHEN clause
is what keeps a future-dated enqueue from waking the fleet. The COPY path
cannot decide status in its write: its rows land as `scheduled` (the
trigger stays silent) and the fixup UPDATE that flips the runnable rows to
`pending` issues the one wake itself, only when it flipped any, so a
future-dated COPY batch wakes nobody and a mixed batch wakes the fleet
once. Postgres coalesces identical
`(channel, payload)` notifications within one transaction, so a batch costs
one delivery. (An app-side notify after the INSERT was the same pair the
trigger emits: coalesced with it in a transaction, a second delivery to
every listener on a caller's bare connection (pinned by
`tests/test_enqueue_wake_source.py`.) A plain pool enqueue is therefore
exactly one statement, `INSERT … RETURNING *`, in autocommit
(`tests/test_round_trip_budgets.py`). pg-boss folds its notify into the
INSERT gated on the row being due; Oban notifies only for `available` rows.

An empty payload wakes every subscriber unfiltered: it comes from the COPY fixup's bulk wake (a
batch spanning queues cannot name one queue) or from an older trigger still live in a rolling
deploy. The non-empty payload is the row's queue. Paths that
re-pend a row by UPDATE (admin retry, the reclaim sweeps) issue their own
`pg_notify`, since the trigger fires on INSERT only.

### Consumer path

`notify_listener_loop` holds a dedicated `deps.notify_conn` (non-pooled, direct
DSN, TCP keepalives enabled) and subscribes to all three channels:

- **Wake channel**: the callback iterates `backend._wake_subscribers` and calls
  `event.set()` on each. The payload selects which subscribers wake (the queue filter); an empty payload wakes all of them.
- **Events channel**: `_make_events_callback` parses the JSON payload, checks the
  `"type"` discriminator (currently only `"cancel"`), and filters by `worker_id`.
  Matching cancel events set `backend._cancel_subscribers` events.
- **Worker channel**: `_make_worker_events_callback` does the same but without
  worker_id filtering (the channel is already targeted to this worker).

Consumer loops register via `backend.subscribe_wake()` (an async context manager)
which adds a fresh `asyncio.Event` to `_wake_subscribers` on enter and removes it
on exit. The consumer loop awaits the event; on wake it polls `dispatch_batch`.

The wake channel is schema-wide; the payload names the row's queue, so a subscriber's event
fires only when its served queue set includes that queue (an empty payload wakes everyone). A
worker for an unserved queue skips the claim round entirely; a filtered wake costs one fallback
poll interval, never work. After a round that came back short (fewer
rows than requested, or none) the producer waits a small jittered cooldown
(`_CLAIM_COOLDOWN_SECONDS`, 50 ms) before its next round; wakes and freed
slots that land meanwhile are folded into that one round. A full round
re-claims immediately, a wake that lands mid-round always yields one
follow-up round, and the fallback poll keeps its own cadence. This is
River's `FetchCooldown` / Oban's `dispatch_cooldown` shape; the cost is up to
one cooldown of claim latency for a job that arrives right after a short
round.

A `_health_check_loop` runs concurrently with the listener, executing `SELECT 1`
on the notify connection at `notify_health_check_interval`. On failure it
reconnects with bounded exponential backoff (initial delay × 2, max 30s). After
reconnect, the callback is re-registered and fires once to drain any jobs that
arrived while disconnected. The reconnect uses `deps.notify_conn_factory` when
set (credential-provider-backed) so a factory deployment reconnects through the
same source rather than falling back to a stale DSN.

The health-check loop is deliberately exempt from watchdog detector 2 (stale-tick)
; see [Watchdog Subsystem](#watchdog-subsystem) for details.

---

## Shutdown Orchestration

Source: `src/taskq/worker/shutdown.py`.

### Shutdown phases

The worker uses a four-phase shutdown model. Each phase is recorded in
`deps.shutdown_phase` (a `ShutdownPhase` enum) **before** any per-phase work
begins, so health endpoints and consumers can observe the current phase:

| Phase | Value | Meaning |
|---|---|---|
| `NONE` | 0 | Running normally |
| `DRAINING` | 1 | Stop accepting new dispatch; re-pend locked-but-unstarted jobs (attempt refunded) |
| `CANCELLING` | 2 | Cooperative cancel of remaining in-flight jobs (set `cancel_event`, stamp the shutdown origin) |
| `FORCING` | 3 | Force-cancel grace: `task.cancel()` (delivered even when the escalation PG write fails; the local cancel is never skipped) + `write_cancel_escalation(phase=2)` (lands only on rows carrying an operator's cancel request) |
| `RELEASING` | 4 | Release never-unwound jobs back to the fleet via `mark_interrupted` (the spent attempt stands, no refund; held behind the remaining termination budget **plus the watchdog's exit tail** past the deadline; the dump-interval check lag and the bounded pre-`os._exit` flush); operator-cancelled jobs still reach `abandoned` |

Phase ordering invariant: `NONE → DRAINING → CANCELLING → FORCING → RELEASING`.

### Signal handling

`install_signal_handlers` registers three signal handlers:

- **SIGTERM / SIGINT**: three-signal escalation counter.
  1. First signal: schedules `orchestrate_shutdown` via `loop.create_task`.
  2. Second signal: sets `escalate_event` to fast-advance CANCELLING → FORCING.
  3. Third signal: `sys.exit(1)` (Kubernetes SIGKILL is the hard backstop).
- **SIGHUP**: sets `deps.reload_event` to trigger a credential hot-reload
  (see `reload_credentials` in `deps.py`). Multiple SIGHUPs during a reload
  coalesce into one follow-up reload.
- **SIGUSR2**: calls `dump_task_stacks("sigusr2")` for an on-demand asyncio
  task-stack dump: live debugging without an image rebuild. Emits one
  structured log record per live task (name, coro qualifier, await-site
  frames) plus a raw stderr fallback. No locals or payload values are
  included.

SIGQUIT is not registered; it produces a core dump on Linux. Use `tini` or
`ulimit -c 0` for containerised deployments.

### Drain phase

`drain_local_queue_to_pending` issues a single bounded-timeout `UPDATE` that
clears the lock on rows where `locked_by_worker = $worker_id AND status =
'running' AND started_at IS NULL`: jobs the worker locked in its local queue
but never started executing. On pool exhaustion or connection error, it logs a
warning and returns 0 so the recovery sweep acts as the backstop.

### Orchestration

`orchestrate_shutdown` runs the four phases in order, then closes the
`leader_conn` (if TaskQ-owned) and sets `shutdown_event`. The close uses
`close_conn_bounded` so a dead PG cannot wedge shutdown on an unbounded close.
The `shutdown_event.set()` is ordered before the close park to stop the
election loop and release `_main`'s `await shutdown_event.wait()` inside the
`open_worker_deps` context, allowing the deps exit-stack guard to unwind
concurrently.

### The no-concurrent-run promise

An interruption release (`mark_interrupted`) is held back until the
interrupted actor has *provably exited*: the promise is that no other pod can
claim a row while this process might still run or touch the work. An async
actor proves it by unwinding (the `CancelledError` propagated through its
frames); a sync `def` actor proves it only when its executor thread finishes,
because `task.cancel()` cancels the await, never the thread; the transactional
path proves it when its tx task finishes unwinding the rollback. The consumer's
cancellation arm parks on the tracked exit handles, bounded by the remaining
termination budget and capped by the lease
(`lock_lease - heartbeat - write budget`: the heartbeat stops at
`shutdown_event`, so the park must never outlive the lease the reclaim sweep
reads), and releases `pending` (hold=0) only on a provable exit inside that
window; otherwise the release is `scheduled` behind the rest of the process's
exit window: the termination deadline **plus the watchdog's exit tail**
(check lag + bounded flush + render slack), because the deadline trip is not
instantaneous. An actor that outlives the window is still released, never
stranded.

The exit bound is **enforced, not modelled**: the shutdown watchdog stays
armed until every tracked actor handle is reaped (`await_tracked_actor_reap`
gates the disarm in the worker's exit path), so the process either exits
cleanly with no live actor; its remaining lifetime is bounded pool closes,
which touch no row, or the deadline trip `os._exit`s it with the
still-running thread inside, under the dedicated
`tracked-actor-outlived-teardown` reason. Without that gate the clean path
disarmed the watchdog and then joined the detached thread in
`asyncio.Runner.close()` (`THREAD_JOIN_TIMEOUT`, 300s): a released row became
claimable while its actor still ran. The alternative closure (a hard
`os._exit(0)` on every clean exit) is rejected because TaskQ is a library:
embedders run the worker in-process (cennan's CLI, TAStack's `ta_worker`), and
a hard exit on the clean path would kill the host's own cleanup. In-process
embedders with the watchdog enabled get the closure for free: the watchdog is
TaskQ's own task on the embedder's loop, and the trip semantics are unchanged.

The promise is scoped: it holds when the watchdog is enabled, or when the
platform SIGKILL lands by `termination_grace_period + exit tail`. With
`watchdog_enabled = false` there is no trip to enforce the bound, the hold
degrades to `lock_lease` (the bound the lease-expiry path already imposes),
and the platform grace must supply the ceiling the watchdog would have (see
the platform-grace window in docs/guides/workers.md).

The timeout path re-pends, and its coverage is narrower by construction. A
`start_to_close` timeout fires while the process is otherwise healthy: there
is no shutdown deadline to anchor an enforced exit on, so the timeout handler
cannot prove a sync actor's exit the way the interruption release can. What it
does is reuse the same exit-proof machinery before the re-pend: it parks on
the job's tracked exit handles, bounded by the exit-wait budget (the cleanup
grace when no shutdown anchors it), and re-pends with the retry decision's own
delay only on a provable exit inside that window. A sync actor still running
at the window's expiry defers the re-pend behind the release hold, the same
exit window the interruption release parks behind, but that deferral is a
bound, not a proof: a sync actor that outlives both the park and the release
window can still overlap a later re-claim of its retry. The enforced exit
boundary (the watchdog gate) protects the interruption release; the timeout
path's promise is exactly the park plus the deferral, and no more.

#### Deliberate divergence from the queue ancestors

No peer ships this contract. Celery requeues a job whose worker died mid-run
(`worker/request.py`: an unacked message returns on `worker-lost`, by design;
the at-least-once overlap is accepted), and River rescues stuck rows purely by
a visibility timeout racing the attempt. TaskQ deliberately diverges: an
interruption *releases with a hold sized to the releaser's own enforced exit*
rather than accepting the overlap, at the cost of one exit-window of latency
for an actor that outlives its budget. The other half of the design (the
heartbeat continuing through the drain so a slow-but-alive worker's leases are
not reclaimed out from under it) aligns with dramatiq's worker shutdown
(`worker.py`: the broker drain waits for in-flight messages) and Celery 5.6+'s
documented warm-shutdown waiting for active tasks
(`docs/userguide/workers.rst`).

The `ShutdownWatchdog` (detector 1) runs concurrently outside the TaskGroup
and enforces `termination_grace_period` as a hard wall (see
[Watchdog Subsystem](#watchdog-subsystem)).

---

## Watchdog Subsystem

Source: `src/taskq/worker/_watchdog.py`, `src/taskq/worker/_transient.py`.

The watchdog is an in-worker hang/deadlock detection system with four
independent detectors and a terminal force-exit on trip. A wedged process
cannot be trusted to unwind gracefully, so a trip dumps diagnostics and calls
`os._exit(EXIT_WATCHDOG)` (exit code 2) with no further awaits: the
supervisor restarts the worker, and in-flight jobs are reclaimed by the
leader sweep on lock-lease expiry (the existing, tested recovery path).

### Detectors

| # | Detector | Mechanism | Location | Trip condition |
|---|---|---|---|---|
| 1 | Shutdown deadline | `ShutdownWatchdog` | Outside TaskGroup | Shutdown still incomplete after `termination_grace_period` |
| 2 | Stale loop ticks | `loop_watchdog_loop` + `LoopLiveness` | Inside TaskGroup | Interval-driven sibling loop hasn't ticked within `period × grace_factor` (floor `watchdog_stale_floor`) |
| 3 | Sibling contract | `_make_sibling_spawner` | In sibling spawner | A sibling returns cleanly while `shutdown_event` is clear (contract violation) |
| 4 | Event-loop lag | `LoopLagWatchdog` | Daemon thread | Event loop hasn't scheduled a beat within `watchdog_loop_lag_budget` |

**Why detectors 1 and 4 live outside the TaskGroup:** as TaskGroup children,
they would be cancelled by the very sibling crash they exist to catch.
`ShutdownWatchdog` parks on `shutdown_event` and is only active during
shutdown. `LoopLagWatchdog` runs on a daemon thread because a fully blocked
event loop cannot run an in-loop detector, and `asyncio.all_tasks` is not
thread-safe.

### Trip semantics

`trip(detector, reason)` is the terminal path for detectors 1, 2, and 3:

1. Increments `taskq.worker.watchdog_trips_total` (labelled by detector).
2. Emits a critical structured log record.
3. Calls `dump_task_stacks` (one record per live task).
4. Flushes stdout/stderr.
5. Best-effort OTel metrics flush on a daemon thread with a 2-second join
   deadline: a hung OTLP collector costs at most 2 seconds, never more.
6. `os._exit(EXIT_WATCHDOG)`: no further awaits.

Detector 4 (`LoopLagWatchdog`) follows the same pattern but inlines the dump
(`faulthandler.dump_traceback` for thread frames) because it runs off-loop
and cannot use `asyncio.all_tasks`.

### Task-stack dump

`dump_task_stacks(reason, *, detector, tasks)` is the single implementation
behind four callers:

- The trip dump (detectors 1, 2, 3).
- The SIGUSR2 handler (on-demand, via `shutdown.py`'s signal handler).
- The `/tasks` health endpoint (when `health_tasks_enabled=True`).
- The straggler logger (still-alive siblings during shutdown).

Each call emits one structured log record per live asyncio task (task name,
coro qualifier, await-site frames as `file:line`), plus a raw stderr fallback
so the dump survives a broken logging pipeline. No locals or payload values
are included: the dump reveals code structure and file paths only.

### LoopLiveness

`LoopLiveness` is the per-loop monotonic tick registry used by detector 2.
Interval-driven loops call `tick(name, period=...)` once per iteration. The
staleness budget is `period × grace_factor` with a `watchdog_stale_floor`
minimum (default 10s) so tiny test intervals cannot false-trip under load.
Loops that never tick (event-driven: notify listener, consumers, reload
coordinator) are not tracked; they are covered by detectors 1, 3, and 4
instead.

Gated loops (e.g. the leadership watchdog) call `forget(name)` when their
gate closes, so a legitimately stopped loop doesn't false-trip. The loop
re-registers on its next tick when the gate reopens.

Thread-safety: `ages()` is called from the `LoopLagWatchdog` daemon thread
while the event-loop thread mutates `_ticks` via `tick()` and `forget()`. A
`threading.Lock` guards all access; a separate lock guards the OTel gauge
cache. Lock order is always `LoopLiveness._lock → _tick_age_cache_lock`.

### ShutdownWatchdog (detector 1)

Lives outside the worker TaskGroup. Parks on `shutdown_event`; once set,
counts down `termination_grace_period` (finally making that setting
enforceable rather than validation-only). While counting:

- Logs one `shutdown-watchdog-armed` record at the start (deadline, dump
  threshold).
- Logs straggler dumps (names + await sites of still-alive siblings) every
  `watchdog_dump_interval`, but only once the shutdown has consumed at least
  `watchdog_dump_after_fraction` of its hard budget (default 0.5; only in
  the back half). A drain in its front half is within expectations and stays
  quiet.
- On deadline: `trip("shutdown-deadline", ...)`.
- On clean exit: cancelled by `_main`, records `shutdown_duration_seconds`.

The deadline is anchored on the **first shutdown signal** (not
`shutdown_event.set()`) because orchestration spends the cancel/cleanup
graces before `shutdown_event` is set; anchoring on the event alone would
double-count them against `termination_grace_period`.

### LoopLagWatchdog (detector 4)

A daemon thread that measures event-loop scheduling lag. Arms after
`watchdog_loop_lag_startup_grace` seconds or the first liveness tick,
whichever comes first: import-heavy startup and DI bootstrap must never
trip it. On each poll interval (`watchdog_check_interval`, default 1s):

1. Checks if armed (startup grace elapsed or any liveness tick landed).
2. Computes lag = `now - last_beat`.
3. If lag > `watchdog_loop_lag_budget` (default 30s): trips inline (critical
   log + `faulthandler.dump_traceback` + metrics flush + `os._exit`).
4. Otherwise: `loop.call_soon_threadsafe(self._beat)` schedules the next
   beat on the event loop. If the loop is closed (`RuntimeError`), the
   thread exits cleanly.

The thread catches `BaseException` and logs loudly rather than dying
silently: a dead watchdog thread takes detector 4 with it, leaving the
worker unprotected with no indication.

### Stale-tick sweep (detector 2)

`loop_watchdog_loop` runs inside the worker TaskGroup. Every
`watchdog_check_interval` (default 1s), it calls `liveness.ages()` (which
also updates the OTel tick-age gauge) and `liveness.stale()`. If any loop is
stale, it trips. Deliberately parked loops (notify listener, consumers,
reload coordinator) have no cadence and are not tracked; they are covered
by detectors 1, 3, and 4.

### Sibling-contract check (detector 3)

Lives in the sibling spawner (`_make_sibling_spawner`), not in
`_watchdog.py`. A sibling returning cleanly while `shutdown_event` is clear
is a contract violation: siblings are long-lived loops that should only
exit on shutdown. The violation is re-raised, which propagates into the
TaskGroup and tears the worker down (the watchdog's other detectors then
ensure the process exits).

### Transient error handling

Source: `src/taskq/worker/_transient.py`.

Every long-lived worker loop that awaits Postgres treats the same set of
errors as "PG is having a moment; log and retry next tick".
`TRANSIENT_PG_ERRORS` is the single tuple that defines this set: one home
for every transient shape, so any error a site learns, every site learns:

- `TimeoutError` (client-side deadlines)
- `PostgresConnectionError` (connection gone)
- `InterfaceError` / `OSError` (unusable connection / dead socket)
- `QueryCanceledError` (server-side command_timeout)
- `AdminShutdownError` (57P01, PG restart/shutdown)
- `CannotConnectNowError` (57P03, PG in crash recovery)
- `TooManyConnectionsError` (53300, server saturated)
- `DeadlockDetectedError` / `SerializationError` (40P01/40001, retry pair)
- `IdleSessionTimeoutError` / `IdleInTransactionSessionTimeoutError`

Deliberately excluded: auth failures (`InvalidPasswordError` et al.) are not
transient for static DSNs and must not retry silently.

`UnexpectedLoopErrorGuard` is the per-loop backstop for errors **outside**
the transient set. It tolerates isolated surprises with a loud, distinct,
alertable record per occurrence, but re-raises after `max_consecutive`
(default 5) consecutive unexpected errors, so a permanent fault (a code
bug, not a PG blip) still kills the worker deliberately instead of retrying
forever into a zombie that ticks but does no work. Only a fully successful
work iteration resets the streak; an idle or transiently-failing iteration
must not buy the fault more time.

### Watchdog settings

| Setting | Default | Description |
|---|---|---|
| `watchdog_enabled` | `True` | Master switch for all four detectors |
| `watchdog_loop_lag_budget` | 30s | How long the event loop may go without scheduling before detector 4 trips |
| `watchdog_loop_lag_startup_grace` | 30s | Grace before detector 4 arms (import/DI startup) |
| `watchdog_tick_grace_factor` | 5.0 | Multiplier on loop period before its tick is stale (detector 2) |
| `watchdog_stale_floor` | 10s | Minimum staleness budget for any loop |
| `watchdog_dump_interval` | 5s | Interval between straggler logs during shutdown (detector 1) |
| `watchdog_dump_after_fraction` | 0.5 | Fraction of shutdown deadline before straggler dumps begin |
| `watchdog_check_interval` | 1s | Poll cadence for stale-tick sweep and loop-lag thread |

A load-time invariant checks that `dispatcher_command_timeout + loop_period`
fits within the staleness budget `max(period × grace_factor, stale_floor)`
for every bounded loop, so a timeout-capped iteration can never false-trip
the stale-loop detector on a healthy worker.

### Relationship to worker lifecycle

The watchdog monitors all worker sibling loops but is deliberately not a
component those loops depend on:

- **Leader loops** (election, sweeps, cron, scheduled-wake): tick
  `LoopLiveness` once per iteration. The `dispatcher_command_timeout`
  invariant ensures a stalled-PG iteration errors the loop instead of
  hanging past the staleness budget. Transient PG errors are swallowed by
  `TRANSIENT_PG_ERRORS`; unexpected errors go through
  `UnexpectedLoopErrorGuard`.
- **Heartbeat loop**: ticks `LoopLiveness`. Cancel-poll and lock-renewal
  are bounded by `dispatcher_command_timeout`.
- **Notify listener**: deliberately exempt from detector 2 (stale-tick).
  Its cadence tracks IO, not progress; it returns early on legitimate paths
  (conn dropped, poll-fallback), and its reconnect backoff (up to 30s)
  would put the budget at the floor and trip during exactly the PG outage
  the reconnect logic exists to survive. Detectors 1, 3, and 4 cover it.
- **Producer loop**: ticks `LoopLiveness` with its poll interval as the
  period. The staleness invariant is checked at load time.
- **Consumers**: event-driven (not interval-driven), so not tracked by
  detector 2. Covered by detector 4 (loop lag) and the sibling-contract
  check (detector 3).
- **ShutdownWatchdog** interacts with `shutdown.py`'s
  `orchestrate_shutdown`: the orchestrator runs the four phases, and the
  watchdog ensures the total wall-clock doesn't exceed
  `termination_grace_period`.

---

## Rate Limiting Architecture

Source: `src/taskq/ratelimit/`.

### Backend options

`RateLimitBackend = Literal["redis", "postgres", "memory"]`

- `redis`: token-bucket and sliding-window using Lua scripts against Redis. Requires
  the `redis` extra.
- `postgres`: falls back to the `rate_limit_buckets` table (token bucket) or
  `rate_limit_window_entries` table (sliding window) in PG.
- `memory`: in-process only; useful for tests.

### `RateLimitRegistry`

Actors declare rate limits via `rate_limits: list[str | KeyedRateLimitRef | TokenBucket | SlidingWindow]`
and concurrency reservations via `reservations: list[str | KeyedReservationRef | ConcurrencyReservation]`
on the `@actor` decorator.
Plain entries are name strings resolved against statically pre-registered primitives;
`KeyedReservationRef` / `KeyedRateLimitRef` entries lazily materialize a per-key primitive
from the job payload on first acquisition. At startup,
`ProviderRegistry.validate(...)` (`src/taskq/_di/registry.py`) runs the DI
validation algorithm in `src/taskq/_di/_validate.py::run_validation`, which
includes a phase that checks each actor's static `rate_limits` and `reservations`
name entries against the `RateLimitRegistry`'s registered names, raising
`MissingProvider` for unknown names.

The registry is an ownable, injectable dependency. `worker_main`/`_main`
accept `rate_limit_registry=`; resolution order is explicit argument →
`RateLimitRegistry` **value** provider at `Scope.LOOP` in the user DI registry
(factory/class providers raise `TypeError` at bootstrap; non-LOOP scope and
explicit+DI co-presence likewise)
→ the module-level `registry` singleton (the backwards-compatible default).
Actor-declared primitive instances are collected into the resolved registry
at bootstrap before `validate()` runs. The admin app mirrors the same default
via `create_router(..., rate_limit_registry=)` → `app.state.rate_limit_registry`
→ `Depends(get_rl_registry)`. Keyed idle-eviction sweeps run on every
worker's own registry (not leader-gated) since eviction is process-local
bookkeeping.

In addition to actor-declared reservations, the worker registers a fleet-wide
`ConcurrencyReservation` per queue at startup when the `queues.max_concurrent` column is set
(via `queue_concurrency_reservation_name(queue)`), and prepends it to the acquire list at
dispatch time; see the [Queue-level concurrency cap](guides/rate-limiting.md#queue-level-concurrency-cap)
guide section for details.

### Dispatch integration

Before executing the actor body, `consume_one_job` checks the rate-limit decision.
The already-validated payload model (constructed in `dispatch_one_job` and passed as
`validated_payload`) is passed to `acquire_for_actor`. `KeyedRateLimitRef` and
`KeyedReservationRef` declare a required `payload_type`; the registry validates the payload
against it and `key_fn` always receives the validated Pydantic model:
- If `RateLimitDecision.allowed`: proceed.
- If denied with `retry_after`: call `mark_retry_after(delay=retry_after)` and
  release the job back to `scheduled` status without consuming the retry budget
  (when `consume_budget=False`).

Reservation slots are pre-allocated in `reservation_slots` rows and held with a
lease for the job's duration; `extend_reservation_leases` renews them on heartbeat.

---

## Clock Domains

TaskQ runs two independent clocks: the PG server clock (`clock_timestamp()` /
`now()` in SQL) and each process's Python wall clock (the injectable
`Clock` in `src/taskq/backend/clock.py`). Divergence between them (NTP drift,
VM pause) is a production reality, so the architecture rule is **one arbiter
per predicate (the data store that owns the row also owns the time it is
compared against)**:

- Every skew-sensitive timestamp decision lives in the SQL statement that owns
  its predicate: lease/liveness writes and expiry checks, sweep and dispatch
  gates, retry/retry-after guards, rate-limit window predicates and
  TAT/token epoch math (`EXTRACT(EPOCH FROM clock_timestamp())`), and the cron
  due-check, catch-up cutoff, and beyond-window recompute (read inside the
  tick's transaction). This now includes the retry path end-to-end:
  `mark_failed_or_retry` takes a *delay*: the backend derives
  `scheduled_at = now() + delay`, the scheduled/pending status, AND the
  `schedule_to_close` deadline outcome from its own clock in one statement;
  the Python retry classifier (`taskq.retry.RetryClassifier`) decides
  retry-kind and backoff only and takes no deadline and no clock input, so
  no second, skewable arbiter can disagree with the SQL.
- Known residual: the enqueue-time singleton collision hint
  (`SingletonCollisionError.retry_after`) mixes domains *by design* (a
  server-read `schedule_to_close` minus a Python now) to steer the
  caller's retry timing. It is advisory metadata only and is never stored
  or compared as a predicate.
- Rate-limit Lua scripts read Redis `TIME` (`redis.call('TIME')`) rather than a
  client-supplied timestamp, so multi-node fleets share one clock; scripts are
  non-deterministic but replication-safe under Redis ≥ 5 effect replication
  (see the [EVAL docs](https://redis.io/docs/latest/commands/eval/)). The
  in-memory limiter backends keep their injected `Clock`: a single process is
  a single domain by construction.
- `JobsClient.create_schedule` / `update_schedule` seed the first
  `next_fire_at` anchored to the backend's own clock: for Postgres backends
  the value is computed from the **server** clock (the same arbiter the cron
  due-check and catch-up recompute use), so app↔DB skew does not shift the
  first fire after creation. The in-memory backend keeps its injected
  `Clock`: a single process is a single domain by construction.
- Known residual: ephemeral progress events (Redis pub/sub → SSE, built in
  `src/taskq/progress/_publish.py`) carry a `ts` stamped from the publishing
  process's clock (`datetime.now(UTC)`), not the PG clock. They are ordered
  by `seq`, never persisted, and never used as a predicate, so neither
  ordering nor correctness is affected, but under app↔DB skew the
  timestamps the UI shows for progress/state-change events can disagree with
  the `job_events.occurred_at` values recorded server-side for the same job.

---

## Schema Design Decisions

Source: `src/taskq/migrations/`.

### Forward-only migrations

Migrations only ever ADD to the schema. Destructive changes (DROP COLUMN, DROP
TABLE) require a `post` migration applied after all workers are on the new version.
The `pre`/`post` phase distinction is explicit in the filename and prevents
rolling-deploy races.

### `{schema}` placeholder

Every migration uses `{schema}` as a placeholder for the Postgres schema name.
The migration runner substitutes it at apply time after validating the name against
`_IDENT_RE`. This enables multi-tenancy: multiple isolated TaskQ instances can
coexist in the same Postgres cluster in different schemas.

**Never hardcode the schema name** in SQL files or application code. Always use
the placeholder in SQL files and `_IDENT_RE`-validated interpolation in Python.

### `jobs` vs `job_attempts` vs `job_events`

- `jobs` is the hot table. Columns hold the current snapshot: `status`,
  `attempt`, `locked_by_worker`, `error_class`, `result`, etc.
- `job_attempts` records every execution attempt with outcome, duration, and
  error. Pruned via `ON DELETE CASCADE`.
- `job_events` records every state transition and cancel request as an immutable
  audit log. Also pruned via `ON DELETE CASCADE`.

This separation keeps the `jobs` hot path narrow (fewer columns updated per
transaction) while providing full per-attempt forensics in `job_attempts` and
a queryable audit trail in `job_events`.

### `jobs_archive` and `job_attempts_archive`

When the prune sweep (Sweep 5) moves a terminal job out of `jobs`, it inserts
an identical row into `jobs_archive` plus two extra columns:

- `archived_at` (`timestamptz`): wall-clock time the row was moved.
- `expire_at` (`timestamptz`): when the row becomes eligible for hard-deletion
  by Sweep 6. Computed as `archived_at + archive_retention_period` (default
  1 year).

`job_attempts_archive` mirrors `job_attempts` with the same schema and an FK to
`jobs_archive(id) ON DELETE CASCADE`. Sweeps 5 and 6 are both batched atomic
CTEs, so `jobs_archive` and `job_attempts_archive` stay in sync by construction.

`job_events` rows are **not** archived: they are deleted by cascade when the
parent `jobs` row is pruned. Historical event data is not available in the
archive. The admin UI job-detail page shows an empty event log for archived
jobs and displays an "archived" banner to make this clear.

### No FK on `locked_by_worker`

`jobs.locked_by_worker` is a UUID column with no foreign key to `workers(id)`.
A real FK would cause an implicit `FOR KEY SHARE` lock on the `workers` row
during every dispatch UPDATE, creating SLRU contention under concurrent dequeue.
Worker liveness is tracked separately via `workers.last_seen_at`.

### Identifier validation

`_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")` is the canonical guard
before any schema-name interpolation into SQL. asyncpg does not support
parameter binding for SQL identifiers, so interpolation is unavoidable. Rather
than relying on a single check, TaskQ uses **defence-in-depth**: the schema
name is validated at `PostgresBackend.__init__` (and `migrate.py`) construction
time, **and** independently re-checked at every call site that interpolates the
schema into a SQL string; 20+ sites across `backend/`, `worker/`, `ratelimit/`,
`web/admin/`, `testing/pg.py`, and `batch.py`. Each call site runs
`_IDENT_RE.match(schema)` immediately before the f-string/`.format()` that
embeds it, so a schema that bypassed construction-time validation (e.g. one
sourced from a different code path or a test fixture) is still rejected before
it reaches the database. All user-supplied values continue to use `$N`
parameter binding; only the schema identifier is interpolated, and it is
validated at both the construction boundary and each use site.

---

## Batch Subsystem

Source: `src/taskq/batch.py`, `src/taskq/batch_policy.py`,
`src/taskq/backend/_batch_sql.py`, `src/taskq/worker/_leader_shared.py`.

The batch subsystem adds opt-in tracking, failure policies, and finalizer
enqueuing for groups of jobs. A row in the `batches` table is created only
when the caller supplies a `failure_policy` or `finalizer` to
`enqueue_batch()` / `enqueue_batch_streaming()`: plain batch enqueues without
these arguments insert no `batches` row and carry only the `metadata.batch_id`
tag on each job.

### `batches` table

| Column | Type | Description |
|---|---|---|
| `id` | `uuid` | Primary key; matches `metadata.batch_id` on child jobs. |
| `queue` | `text` | Queue the batch was enqueued on. |
| `status` | `text` | `active`, `complete`, or `aborted`. |
| `expected_size` | `int` | Number of child jobs enqueued. |
| `consecutive_failures` | `int` | Running failure count, reset on success. |
| `failure_threshold` | `int` | Threshold from `AbortBatchAfter`; `NULL` if no policy. |
| `finalizer_job_id` | `uuid` | Job ID of the finalizer, if one was enqueued. |
| `originating_actor` | `text` | Reserved for future use; currently always `NULL`. Will be populated from the actor context in a future release. |
| `created_at` | `timestamptz` | Batch creation time. |
| `completed_at` | `timestamptz` | When the batch reached `complete` or `aborted`. |
| `metadata` | `jsonb` | Arbitrary batch-level metadata. |

### Batch lifecycle

```
active → complete   (all child jobs terminal, failure threshold not reached)
active → aborted    (consecutive_failures >= failure_threshold)
```

A batch starts as `active` when `create_batch` inserts the row. The
`apply_batch_terminal_outcome` hook drives completion and abort; the
`complete_stale_batches` leader sweep is the safety net.

### `apply_batch_terminal_outcome` hook

Called after every terminal write (consumer and in-memory runner). For
non-batched jobs (no `metadata.batch_id`) it returns immediately: zero
overhead. For batched jobs:

| Outcome | Action |
|---|---|
| `succeeded` | Resets `consecutive_failures` to 0. If no non-terminal jobs remain, marks batch `complete`. |
| `failed` | Increments `consecutive_failures`. If `>= failure_threshold`, aborts the batch. Otherwise, if no non-terminal jobs remain, marks batch `complete`. |
| `cancelled` / `crashed` | Counts non-terminal jobs; if none remain, marks batch `complete`. Does not touch the failure counter. |
| `snoozed` / `reservation_denied` / `rate_limit_denied` / `scheduled` | Returns immediately: the job is rescheduled, not terminal. |

"No non-terminal jobs remain" is decided by `complete_batch`'s own `NOT
EXISTS` guard in its statement's snapshot (the count the counter writes
return is advisory). Both the guard and that count are the open-member
probe served by `jobs_batch_open_members_idx`: a partial index over
exactly the non-terminal members, keyed by `batch_id`, so each terminal
write costs an index range over the members still open, never a walk of
the batch's whole membership.

Aborting cancels all pending and scheduled child jobs (`pending` /
`scheduled` → `cancelled`) with a hardcoded `error_message = 'Batch
aborted due to consecutive failures'` and sets the batch row to
`aborted`. Running jobs continue to completion.

### `complete_stale_batches` leader sweep

Runs every 30 s on the leader (inside the Sweep loop, alongside Sweep 4).
Marks any `active` batch with zero non-terminal child jobs as `complete`.
This is the safety net for batches whose `apply_batch_terminal_outcome` hook
was lost (e.g. consumer crash before the hook ran) and for
intentionally-empty batches (`expected_size=0`).

### `prune_old_batches`

Called by the prune loop (Sweep 5) **after** job pruning completes. Deletes
`batches` rows whose `completed_at` is older than the job prune cutoff and
that have no remaining child jobs in the `jobs` table. This ordering ensures
batch rows are not removed while their child jobs are still visible.

### `wait_for_batch` decision table

`wait_for_batch` queries both the `batches` row and live job counts, then
decides according to this table (first matching row wins):

| Condition | Action |
|---|---|
| Batch row `status = 'aborted'`, `pending = 0` | Raise `BatchAbortedError` |
| Batch row `status = 'aborted'`, `pending > 0` | Raise `Snooze(interval)` |
| `expect_at_least` set, `pending = 0`, `total < expect_at_least` | Raise `EmptyBatchError` |
| `total = 0` and batch row exists | Return empty `BatchCompletionStatus` |
| `total = 0`, no batch row, `on_empty = "ok"` | Return empty `BatchCompletionStatus` |
| `total = 0`, no batch row, `on_empty = "error"` | Raise `EmptyBatchError` |
| `pending > 0` (snooze mode) | Raise `Snooze(interval)` |
| `pending > 0` (blocking mode) | Sleep and re-query |
| Otherwise | Return `BatchCompletionStatus` |

The finalizer job is automatically excluded from counts via the batch row's
`finalizer_job_id`; pass `exclude_job_id` to override.

### `BatchFilter`

Batch queries use `BatchFilter`, not `JobFilter`. `BatchFilter` carries only
fields relevant to batch queries: `queue`, `active` (status terminality),
`batch_id`, and `limit`. Job-oriented fields (`status`, `actor`, `tags`,
`cursor`, `order_by`, `identity_key`) are intentionally absent: using
`JobFilter` for batch queries would silently ignore those fields.

### `enqueue_batch_streaming`

Accepts an `Iterable[EnqueueItem]` (including generators) and inserts in
chunks of `chunk_size` (1-1000). All items share the same `batch_id`. When
`failure_policy` or `finalizer` is set and no caller-owned `connection` is
provided, the entire operation is delegated to
`Backend.enqueue_batch_atomic` for single-transaction atomicity. Otherwise,
chunks are inserted via `Backend.enqueue_batch` on the caller-owned
connection, with the batch row and finalizer created as the last statements.

### Security

Schema identifiers are validated against `_IDENT_RE` before interpolation
into SQL, following the same defence-in-depth pattern as the rest of the
codebase (see [Identifier validation](#identifier-validation)). All
user-supplied values use `$N` parameter binding. The abort
`error_message` is a hardcoded SQL literal (`'Batch aborted due to
consecutive failures'`), not user input, so it carries no injection risk.

---

## Observability Architecture

Source: `src/taskq/obs/`.

### OTel span hierarchy

```
PRODUCER span: "send <queue>" (SpanKind.PRODUCER)
  → trace_id + span_id stored on jobs row at enqueue

DISPATCH span: "dispatch" (SpanKind.INTERNAL)
  → wraps the dispatch_batch SQL call

CONSUMER span: "process <actor>" (SpanKind.CONSUMER)
  → linked to PRODUCER span via trace_id/span_id from job row
  → wraps the full actor execution (payload validation → terminal write)
```

The consumer span is linked (not a child) to the producer span, matching
messaging semconv: the producer and consumer are separate traces that happen
to be causally related.

### OTel metric names

| Metric | Kind | Description |
|---|---|---|
| `taskq.dispatch.duration` | Histogram | SQL-execution latency for dispatch_batch |
| `messaging.process.duration` | Histogram | Full actor execution duration |
| `messaging.client.consumed.messages` | Counter | Count of completed jobs by actor/queue/outcome |
| `taskq.backpressure.errors` | Counter | Enqueue backpressure rejections by actor/kind: `max_pending` (cap reached) and `max_pending_lock_timeout` (serialization-lock wait budget exhausted) |
| `taskq.deadline_exceeded_sweep.jobs_failed` | Counter | Jobs failed by the deadline sweep |
| `taskq.cancellation.requested` | Counter | Bumped once per `JobsClient.cancel()` call (regardless of outcome) |
| `taskq.cancellation.phase_transitions` | Counter | Cancel phase changes |
| `taskq.notify.received` | Counter | NOTIFY callbacks from asyncpg |
| `taskq.notify.reconnects` | Counter | NOTIFY connection reconnects |
| `taskq.notify.connected` | Observable Gauge | 1 if NOTIFY listener healthy |
| `taskq.notify.cancel_received` | Counter | Cancel NOTIFY callbacks delivered to this worker |
| `taskq.maintenance_leader.is_leader` | Observable Gauge | 1 on elected pod |
| `taskq.worker.watchdog_trips_total` | Counter | Watchdog trips leading to force-exit, by detector |
| `taskq.worker.loop_tick_age_seconds` | Observable Gauge | Seconds since each interval-driven loop last ticked |
| `taskq.worker.shutdown_duration_seconds` | Histogram | Wall-clock seconds from first shutdown signal to clean exit |
| `taskq.worker.loop_unexpected_errors_total` | Counter | Unexpected (non-transient) errors tolerated by a long-lived loop's backstop (the leader maintenance loops and the worker producer loop), by loop label |

The table above is illustrative, not exhaustive: the codebase defines 25+ instruments. For the complete list, see `src/taskq/obs/_otel.py` and the worker observability modules in `src/taskq/worker/` (`notify.py`, `cancel.py`, `leader.py`, `_leader_shared.py`, `heartbeat.py`, `_watchdog.py`, `_transient.py`, `shutdown.py`).

### structlog context propagation

`bind_job_context` adds `job_id`, `actor`, `queue`, `attempt`, `identity_key`,
and `trace_id` to the structlog context for the duration of a job execution. Every
log line emitted inside an actor or consumer path carries these fields automatically.

### Vendor-neutral design

TaskQ never imports Sentry, Datadog, PostHog, or App Insights SDKs. All
observability is emitted via OTLP. Point `OTEL_EXPORTER_OTLP_ENDPOINT` at
whichever backend's collector is in the stack.

---

## Key Invariants

These invariants must remain true across all changes.

1. **`lock_lease >= 4 × heartbeat_interval`**: the lock lease must outlive
   several heartbeat intervals so a slow heartbeat tick does not expire the lock
   before the next renewal arrives.

2. **PG-write before task.cancel()**: in the phase-2 cancel path, the
   `CANCEL_ESCALATION_SQL` UPDATE is executed and the `job_events` row is
   inserted BEFORE `active.task.cancel()` is called, with no intervening `await`.
   If the write fails, the exception propagates and `task.cancel()` is never
   called; the job retains phase 1 and the heartbeat retries on the next tick.

3. **Terminal writes own their row**: `mark_succeeded`, `mark_failed_or_retry`,
   `mark_cancelled`, `mark_abandoned` all guard with
   `WHERE status = 'running' AND locked_by_worker = $worker_id`. A rowcount of 0
   means the write was a no-op (concurrent writer already moved the row).
   `WorkerOwnershipMismatch` is raised for unexpected ownership failures.
   A pool-path terminal write that fails with an infrastructure error is
   retried a bounded number of times (`terminal-write-retry`); when the budget
   is spent the row stays `running` and the worker **disowns** it
   (`WorkerDeps.disowned_jobs`): the heartbeat's lease renewal excludes
   disowned ids, so lock-lease expiry, not the process's lifetime, bounds how
   long the row stays orphaned, and the reclaim sweep hands it back to the
   fleet. The producer re-owns an id it claims again; the heartbeat drops ids
   whose row is no longer this worker's running row.

4. **Schema identifier validation is defence-in-depth, not single-point**:
   `PostgresBackend.__init__` validates `schema_name` against `_IDENT_RE` once
   at construction, and every call site that interpolates the schema into SQL
   re-validates it independently (20+ sites). asyncpg cannot bind identifiers
   as parameters, so interpolation is unavoidable; the redundant per-site
   checks ensure a schema reaching SQL through any path is always rejected if
   it is not a plain `[A-Za-z_][A-Za-z0-9_]*` identifier. All user-supplied
   values use `$N` parameter binding.

5. **`ABANDON_PENDING` is in-process only**: `CancelPhase.ABANDON_PENDING = 3`
   is never written to PG. `parse_cancel_phase(value)` raises `ValueError` if it
   encounters value `3` from a PG row.

6. **`InMemoryBackend` is single-threaded**: do not share an `InMemoryBackend`
   across threads or event loops. The single-writer contract is enforced by
   documentation; the `_single_threaded()` guard is a no-op.

7. **Migration files are append-only**: never modify an applied migration.
   The migration runner stores a SHA-256 checksum of each applied file's
   rendered SQL in `schema_migrations` and logs a `migration-checksum-drift`
   warning when an applied file no longer matches, so tampering surfaces in
   logs (drift is warned on, not rejected; applied migrations never re-run).

8. **`BACKEND_PROTOCOL_VERSION` is checked at import time**: both
   `PostgresBackend` and `InMemoryBackend` assert the version constant at module
   load, not at runtime. A version bump without updating both implementations
   raises `RuntimeError` on import, not on the first query. The version bumps
   only when an existing member's contract changes in a way old implementations
   would silently mishandle (see *When the version bumps* under
   §Backend protocol above).

## Performance Benchmarking Toolkit

The CPU hot paths (DI solving, jsonb serialization, cron next-fire, job-row
decode) are tracked by an A/B benchmark harness in `benchmarks/`. Each hotspot
is measured as *current code* vs a *proposed variant* in interleaved batches,
with a correctness assertion proving the variant is output-identical before any
timing is trusted.

- Spot-check: `make bench` (full suite) or `make bench-profile` (cProfile /
  pyinstrument a single bench).
- Regression gate: `make bench-save` commits `benchmarks/results/baseline.json`,
  then `make bench-check` fails (exit 1) when any bench is slower by more than
  1.20× **and** more than 100 ns/op: medians of interleaved batches, both
  conditions required so sub-microsecond benches don't flip on timer jitter.
- Cross-version: `make bench-matrix` runs the suite under Python 3.12/3.13/3.14
  via uv and prints a bench × version table.
- Event-loop stall probes and a GIL sampler (py-spy needs root on macOS) are
  included; see `benchmarks/README.md` for the full ops manual, the rules for
  adding a new A/B bench, and the results JSON schema.
