# Retry

`taskq.retry` is the retry engine: the `RetryPolicy` model actors declare,
the classifier hooks that decide per-failure outcomes, the backoff
computations, and the lifecycle hooks. The [Retries guide](../guides/retries.md)
walks the behavior; this page is the module index.

```python
from taskq.retry import RetryPolicy, RetryClassifier
```

## The names the guides don't spell out

Eight public names in this module are easy to miss because the guides talk
about their EFFECTS without naming them. All eight render under the module
directive below:

| Name | Role |
|---|---|
| `compute_backoff` | The single delay computation every retry path shares: curve (exponential/linear/fixed) → effective cap (`min(policy.cap, max_retry_backoff)`) → jitter drawn from a band fitted under the cap (deliberately NOT Full Jitter, which collapses toward zero on attempt 1). |
| `invoke_on_success` / `invoke_on_cancel` / `invoke_on_retry_exhausted` | The three `@actor` lifecycle-hook invokers: sync or async hooks, run under a timeout, a hook failure is logged and never changes the already-decided outcome. |
| `safe_mark_failed_or_retry` | Wraps the backend's `mark_failed_or_retry`, catching `WorkerOwnershipMismatch`: returns the persisted `JobRow` on success, `None` on ownership mismatch or a fenced-out epoch, which signals the caller to skip the `on_retry_exhausted` hook. |
| `time_budget_as_interval` | Renders an indefinite policy's `time_budget` as the Postgres `interval` the enqueue path passes so `schedule_to_close = clock_timestamp() + time_budget` is computed at insert time. |
| `MAX_ATTEMPTS_SMALLINT_CEILING` | `32767` — the `jobs.max_attempts` column's `smallint` domain ceiling. |
| `MAX_ENQUEUABLE_MAX_ATTEMPTS` | `32766` — one below the ceiling, the largest `max_attempts` a fresh enqueue or policy may carry (the defensive headroom a reclaim needs to add one). |

## Module reference

::: taskq.retry
