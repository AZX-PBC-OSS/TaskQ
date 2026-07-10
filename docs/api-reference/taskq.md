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
the `cron()` function does not render under the `::: taskq` package-level directive above —
mkdocstrings resolves `taskq.cron` to the submodule. The explicit directive below documents
the function itself; see the [Cron Scheduling guide](../guides/cron.md) for usage.

::: taskq.cron.cron
