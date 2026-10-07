# Package Overview

TaskQ public API. The `taskq` package re-exports the primary types used by application
code: the `@actor` decorator, `JobsClient` / `TaskQ`, `JobHandle`, exceptions,
`RetryPolicy`, cron scheduling, and batch helpers.

```python
from taskq import actor, TaskQ, JobHandle, JobFailed, RetryPolicy
from taskq import cron, ScheduleHandle
from taskq.context import JobContext
from taskq.di import ProviderRegistry, Scope
```

::: taskq

## `cron()`

`taskq.cron` is both a submodule (`src/taskq/cron.py`) and, via `from taskq.cron import
cron`, the name of a re-exported function on the `taskq` package. This name collision means
the `cron()` function does not render under the `::: taskq` package-level directive above
mkdocstrings resolves `taskq.cron` to the submodule. The explicit directive below documents
the function itself; see the [Cron Scheduling guide](../guides/cron.md) for usage.

::: taskq.cron.cron

## Error contracts at a glance

The surface answers "is this job there?" in three deliberately different
ways. Mixing them up is the most common integration bug:

| Operation | Missing / nonexistent target |
|---|---|
| `TaskQ.get` / `get_row` | returns `None` (jobs table first, then the archive tier) |
| `TaskQ.cancel` / `JobHandle.cancel` | raises `KeyError` carrying the job id — a typo'd id and a pruned job are indistinguishable |
| `TaskQ.stream` | raises `KeyError` at the opening read **and** if the row is pruned mid-stream (it cannot fabricate the promised terminal event) |
| `TaskQ.retry_job` | returns `False` — a nonexistent id, a non-terminal status, and the attempt-ceiling conflict are all the same `False` by design |
| `wait_for_batch` (foreign/typo'd batch id) | raises `EmptyBatchError` ("is empty or unknown to this client") — pass `on_empty="ok"` when an empty batch is legitimate |

`enqueue` performs **no actor-registration check**: a ref whose actor no
worker declares is enqueued successfully and then parked at the snooze
cadence (`released_reason: "actor-not-found"`, budget-free, surfaced by
the stranded-jobs detector) — check the actor spelling and
`taskq job show` before looking at queues or capacity.

The full exception taxonomy — every class, its fields, and its remedy — is
indexed on the [Exceptions API reference](exceptions.md); the retry engine's
module surface is on the [Retry API reference](retry.md).

!!! warning "Settings typos are silent"

    Unknown `TASKQ_*` environment variables are **ignored** by the
    settings loader (dotenvmodel loads only its known fields and logs no
    warning). A misspelled knob — `TASKQ_MAX_PENDNG_LOCK_TIMEOUT_MS` —
    silently applies that knob's default instead of failing. Run
    [`taskq doctor`](../guides/cli.md#taskq-doctor), whose environment
    finding family names every unknown `TASKQ_*` variable in the
    environment (with the closest real setting name as a remedy hint);
    verify spellings against the field descriptions on
    [`TaskQSettings`][taskq.settings.TaskQSettings] when a setting
    appears to have no effect.

