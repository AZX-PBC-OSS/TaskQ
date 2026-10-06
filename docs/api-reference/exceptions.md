# Exceptions

`taskq.exceptions` is the single home for every class the library raises.
Import errors from it (or from `taskq` for the names the package re-exports;
the [Package Overview](taskq.md) shows which) and catch `TaskQError` as the
fallback in handlers that must never let a library error escape.

```python
from taskq.exceptions import TaskQError, JobFailed
```

Every error's full contract — fields, message shape, and the reasoning
behind the classification — renders under the module directive below; this
page's tables are the when/why/what-to-do index.

## Consumer-facing taxonomy

The errors application code meets in ordinary operation. Ordered roughly by
the surface that raises them: enqueue-time, then result-reading, then
infrastructure.

| Exception | Raised when | Why this class | What to do |
|---|---|---|---|
| `MaxPendingExceededError` | `enqueue()` on an actor whose `pending + scheduled` count is at its `max_pending` cap. | Capacity is exhausted; the library never blocks on it. | Retry later, shed load, or inside an actor body raise `Snooze` instead of letting this reach the retry classifier. |
| `MaxPendingLockTimeoutError` | The advisory-lock wait bounding a capped actor's exact count-then-insert expired its budget: the cap check never ran. | A `BackpressureError` sibling of `MaxPendingExceededError` — same response, but nothing was counted. | Retry later or shed load, exactly as for a cap rejection. Fields: `actor`, `timeout_ms`. |
| `UniqueForLockTimeoutError` | A `unique_for` enqueue's single-flight advisory lock wait expired: the dedup answer was not determinable in time. Nothing was inserted. | Deliberately NOT a `BackpressureError` — nothing about capacity is wrong, so load-shedding handlers must not misfire. | Retry the SAME enqueue: by then the winner's row is typically committed and the retry dedupes against it. |
| `IdempotencyKeyLockTimeoutError` | The bounded wait for an idempotency-token INSERT expired: the dedup answer for one `(idempotency_scope, idempotency_key)` pair was not determinable in time. Nothing was inserted. | Third member of the enqueue-serialization family (with the two lock timeouts above); takes `UniqueForLockTimeoutError`'s treatment. | Retry the same enqueue: once the holder's transaction resolves, the retry dedupes or inserts fresh. |
| `SingletonCollisionError` | A singleton actor already has a job in `pending`/`scheduled`/`running`. | Enqueue-time backpressure for the singleton discipline. | Fields: `blocking_job_id` (the UUID of the existing job), `retry_after` (from its `schedule_to_close`, may be `None`). |
| `BatchMaxPendingExceededError` | A bulk enqueue partitioned admission per actor and refused some: within-cap actors' items were inserted FIRST, then this raised. | The bulk-tier sibling of `PartialBatchError`: partial admission is typed so a blind whole-batch retry cannot silently duplicate. | Retry only the refused indices (`refusals`, `refused_indices`, `admitted_count`) or give items `idempotency_key`s. |
| `PartialBatchError` | An autonomous `ctx.jobs.enqueue_batch()` partially failed: items enqueued before the first failure are committed, the rest are not inserted. | The house shape for partial batch admission: succeeded count + failed indices + typed per-failure exceptions. | Fields: `succeeded_count`, `failed_items` (index → exception), `total`. Retry only the failed indices. |
| `SubEnqueueError` | `flush_buffer()` failed for one or more buffered sub-job enqueues AFTER the parent job committed and was marked succeeded: child jobs were lost. | The parent's success is already durable; this error must be loud, not silent. | Fields: `failed_items` (each failed `EnqueueArgs` + its exception). Re-enqueue the lost children. |
| `PayloadValidationError` | Pydantic validation of a payload failed at enqueue ('fail at the door') or dispatch. | Non-retryable in both cases regardless of retry policy. | Fix the payload. Fields: `actor`, `payload_schema_ver`, `validation_errors`, `item_index`. |
| `IdempotencyKeyActorMismatchError` | An idempotency hit resolved to a job of a DIFFERENT actor. | A cross-actor hit is not a dedup: the caller would get another actor's result, indistinguishable from success. | Namespace keys per actor or give the actors different `idempotency_scope`s. Nothing was enqueued. |
| `JobFailed` | `JobHandle.wait()` observed a non-success terminal state. | Carries the row so callers can inspect `status`/`error_class`/`error_message`. | Inspect `exc.row`; distinct from `ResultUnavailable`. |
| `ResultUnavailable` | `JobHandle.wait()` observed a terminal state but no usable result (TTL expired, actor returned `None` for non-`None` `R`, or none stored). | Distinguishes "terminal, no result" from "failed". | Read `exc.reason` (`"result_ttl_expired"` / `"not_stored"`); re-run the work if the result matters. |
| `CorruptJobDataError` | A stored jsonb column decoded to something its boundary refuses (invalid JSON, or a non-object in a row-contract dict field). | Non-retryable by construction: the bytes on disk cannot change by re-reading them. The dispatch claim boundary fails the row terminally and moves on. | Investigate the row (field: `column`) — a hand-corrupted row, schema drift, or a foreign writer. |
| `UnencodableValue` | A value no UTF-8 JSON encoding accepts (the canonical case: a lone surrogate like `"\udcff"`, exactly what `os.fsdecode` of a non-UTF-8 filename byte yields). | Subclasses **`TypeError`** deliberately, keeping the historical orjson contract: an `except TypeError` swallows it — write handlers accordingly. | Non-retryable wherever the producer already ran; fix the value at the source. |
| `SchemaNotMigratedError` | The backend raised `UndefinedTableError`: the TaskQ schema is missing. | Translated from raw asyncpg so operators see an actionable message (the original is chained via `__cause__`). | Run `taskq migrate up` from a pre-deploy job; workers never self-migrate. |
| `StreamUnavailable` | A `TaskQ.stream()` / `progress_stream()` poll transport could not re-read its job row for longer than its failure budget. | A blip shorter than the budget is retried silently; this is the END of the stream — the caller's `async for` cannot wait forever on a database that is not coming back. | Handle the stream ending; the last failure is chained as `__cause__`. |
| `RateLimitDependencyUnavailable` | A rate limiter's PG store was never wired (no pool injected) at acquire time. | Subclasses `RuntimeError` deliberately (the historical contract); the acquire boundary fails CLOSED — a non-consuming denial, never a burnt attempt. | Fix the wiring: inject the pool the limiter's fallback needs. |
| `RateLimitStoreCorrupt` | A limiter store answered, but the reply violates the reply contract (a token count outside `[0, capacity]`, a malformed `TIME` tuple, ...). | A store that LIES is failed closed exactly like a store that cannot serve, and re-runs admission against the durable PG row. | Treat as a store outage; investigate the store's integrity. |
| `ActorDeregistrationError` | Base for actor deregistration refusals. | Lets cleanup code catch every refusal shape with one clause. | See the [Actor API: Actor deregistration](../guides/actors.md#actor-deregistration). |
| `ActorHasActiveJobsError` | Non-terminal jobs reference the actor being deregistered. | Carries `active_count` and per-status `status_counts` so the caller can decide: cancel first, or `force=True` (which still refuses on RUNNING jobs). | Cancel the blocking jobs, or pass `force=True` for pending/scheduled ones and wait out the running. |
| `ActorHasEnabledSchedulesError` | Enabled cron schedules reference the actor being deregistered. | Carries `schedule_ids` so the caller can disable or delete them first. | Disable/delete the named schedules, or pass `force=True` to disable them automatically. |
| `ActorNotFoundError` | Deregistering an actor with no stored `actor_config` row. | Lets idempotent cleanup loops treat "already gone" as success. | Catch and continue. |
| `TaskQError` | — (base class). | The catch-all for every library-raised error EXCEPT the deliberate builtin-base escapees: `UnencodableValue` (a `TypeError`) and `RateLimitDependencyUnavailable` + `RateLimitStoreCorrupt` (a `RuntimeError`). | An `except TaskQError` handler misses exactly those three; add explicit clauses if they matter to you. |

Control-flow signals `Snooze` and `RetryAfter` also live in this module but
are NOT errors — they are signals an actor raises to translate into state
transitions; see [Actors: Control-flow exceptions](../guides/actors.md#control-flow-exceptions)
and [Retries](../guides/retries.md).

!!! note "The five names outside the package `__all__`"

    `CorruptJobDataError`, `UnencodableValue`, `IdempotencyKeyLockTimeoutError`,
    `RateLimitDependencyUnavailable`, and `RateLimitStoreCorrupt` are public
    here in `taskq.exceptions` but deliberately not re-exported from
    `taskq.__all__` — the export surface is frozen pending a separate
    decision. Import them from `taskq.exceptions` (as this page does); every
    other name in the table above resolves from both homes.

## Module reference

::: taskq.exceptions
