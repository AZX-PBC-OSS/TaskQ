# Spec: Sub-job Tags (#57) & Bulk Cancel by Filter (#54)

**Date:** 2026-07-29
**Status:** Draft, revised post-review (2026-07-29)
**Issues:** [#57](https://github.com/rich/taskq/issues/57), [#54](https://github.com/rich/taskq/issues/54)

---

## Goal

Make sub-jobs enqueued from inside actor bodies visible to tag-based filters by adding `tags` (and other missing fields) to `SubJobEnqueuer.enqueue()`, with parent-tag inheritance by default; and add a set-based `cancel_where(filter)` operation that cancels all jobs matching a `JobFilter` in a single SQL round-trip, with guardrails against accidental full-table cancel.

## Non-goals

- Metadata-based filtering (`JobFilter` metadata predicates) — out of scope; tags solve the discovery problem.
- Bulk cancel via the admin web UI — the API method is added here; UI wiring is a follow-up.
- Bulk *retry* or bulk *delete* by filter — same pattern but separate spec.
- Changing the cooperative cancellation state machine phases — `cancel_where` reuses the existing phase-1 cooperative path for running jobs and the existing direct-to-terminal path for pending/scheduled.
- Sub-job `queue` override — sub-jobs use the actor's declared queue (documented design choice, unchanged).
- Widening the tag charset — issues #54/#57 use colon-form tags in prose (`tenant:acme`, `run:{run_id}`), but TaskQ's validator (`_args.py:35`, `^[\w][\w\-]+[\w]$`) accepts word chars and hyphens only, and downstream (cennan) already ships hyphenated tags after a production incident with the colon form. All examples in this spec use the valid hyphenated form; a charset widening would be a separate, separately-motivated change.

---

## Architecture Overview

### Current state

```
JobsClient.enqueue(tags=...)     ──►  build_enqueue_args(tags=...)  ──►  EnqueueArgs.tags
EnqueueItem(tags=...)            ──►  build_batch_args                ──►  EnqueueArgs.tags
SubJobEnqueuer.enqueue()         ──►  build_enqueue_args(tags=<missing>) ──►  EnqueueArgs.tags = ()
                                                                               ^^^^^^^^^^
                                                                               Issue #57: tags always empty

JobsClient.cancel(job_id)        ──►  Backend.write_cancel_request(job_id, reason)
                                       ├─ pending/scheduled → UPDATE to 'cancelled' (terminal)
                                       └─ running → UPDATE cancel_phase=1 (cooperative) + NOTIFY
                                                                               ^^^^^^^^^^
                                                                               Issue #54: one job at a time only
```

### Target state

```
SubJobEnqueuer.enqueue(tags=..., inherit_tags=True)
    │
    ├─ inherit_tags=True, tags=None   → use parent job's tags (from ContextVar)
    ├─ inherit_tags=True, tags=[...]  → merge parent tags + explicit tags (union, parent first)
    └─ inherit_tags=False, tags=None  → empty tags (current behavior)

JobsClient.cancel_where(JobFilter(tags=("tenant-acme",), active=True), reason="offboard")
    │
    └─► Backend.cancel_where(filter, reason)
            ├─ pending/scheduled rows → UPDATE to 'cancelled' (terminal) + state_change events
            └─ running rows → UPDATE cancel_phase=1 (cooperative) + cancel_request events + NOTIFY
            └─► BulkCancelResult(cancelled_directly=N, cancel_requested=M, ...)
```

### File structure — files to create or modify

```
src/taskq/
├── backend/
│   ├── _protocol.py          MODIFY: add cancel_where to Backend protocol; define BulkCancelResult
│   ├── _reads.py             MODIFY: extract filter→SQL WHERE builder for reuse (refactor)
│   ├── _filter_sql.py        CREATE: shared filter→SQL WHERE builder (extracted from _reads)
│   ├── _cancel_bulk.py       CREATE: bulk cancel implementation for PostgresBackend
│   └── postgres.py           MODIFY: wire cancel_where to _cancel_bulk
├── client/
│   ├── __init__.py           MODIFY: re-export BulkCancelResult alongside CancelResult
│   ├── _jobs.py              MODIFY: add cancel_where method to JobsClient
│   ├── _enqueuer.py          MODIFY: add tags, inherit_tags, schedule_to_close, start_to_close, heartbeat_timeout to enqueue()
│   ├── _taskq.py             MODIFY: add cancel_where delegate to TaskQ
│   └── _args.py              MODIFY: (no change needed — build_enqueue_args already accepts tags)
├── worker/
│   ├── _consumer.py          MODIFY: set parent tags ContextVar before actor invocation (gated by setting)
│   └── run.py                MODIFY: set parent tags in stub consumer (unconditional — test harness)
├── settings.py               MODIFY: add sub_job_inherit_tags field to WorkerSettings (fleet kill switch)
├── testing/
│   ├── in_memory.py          MODIFY: add cancel_where to InMemoryBackend
│   └── _cancel_bulk.py       CREATE: in-memory bulk cancel implementation
├── types.py                  MODIFY: re-export BulkCancelResult from _protocol
├── exceptions.py             MODIFY: add EmptyFilterError (guardrail)
└── __init__.py               MODIFY: export BulkCancelResult, EmptyFilterError

tests/
├── test_sub_job_tags.py          CREATE: unit tests for sub-job tags + inheritance
├── test_cancel_where.py          CREATE: unit tests for cancel_where (in-memory)
├── test_cancel_where_pg.py       CREATE: integration tests for cancel_where (postgres)
├── test_cancel_where_client.py   CREATE: client-level cancel_where tests (guardrail, counter, schema errors)
├── test_filter_sql.py            CREATE: filter→SQL builder extraction tests
├── test_bulk_cancel_types.py     CREATE: BulkCancelResult, EmptyFilterError type tests
├── test_sub_job_enqueuer.py      MODIFY: add tags parameter tests + backward compat
├── test_backend_protocol.py      MODIFY: add cancel_where protocol conformance test; update member count
└── e2e/
    ├── actors.py                 MODIFY: add tagged pipeline actors
    ├── test_sub_job_tags.py      CREATE: e2e tests for sub-job tags in a real pipeline
    └── test_cancel_where.py      CREATE: e2e tests for bulk cancel

docs/
├── guides/jobs-clients.md        MODIFY: document cancel_where and sub-job tags
└── architecture.md               MODIFY: document bulk cancel in cancel protocol section
```

> **Note:** `context.py` is NOT modified — parent tags are propagated via a `contextvars.ContextVar` defined in `_enqueuer.py`, not via `JobContext`. The SQL is inlined in `_cancel_bulk.py` (matching the dynamic-SQL precedent in `_reads.py`); no `SqlTemplates.cancel_where` field or `_sql.py` change is needed.

---

## API Surface

### Issue #57: SubJobEnqueuer.enqueue() — tags and missing fields

#### Modified signature

```python
# src/taskq/client/_enqueuer.py

class SubJobEnqueuer:
    async def enqueue[P: BaseModel, R: BaseModel | None](
        self,
        actor_ref: ActorRef[P, R],
        payload: P,
        *,
        connection: asyncpg.Connection | None = None,
        scheduled_at: datetime | None = None,
        priority: int | None = None,
        fairness_key: str | None = None,
        metadata: dict[str, object] | None = None,
        identity_key: IdentityKey | None = None,
        idempotency_key: IdempotencyKey | str | None = None,
        idempotency_scope: str | None = None,
        unique_for: timedelta | None = None,
        unique_states: tuple[JobStatus, ...] | None = None,
        max_pending: int | None = None,
        # ── NEW parameters ──────────────────────────────────
        tags: list[str] | None = None,
        inherit_tags: bool = True,
        schedule_to_close: datetime | None = None,
        start_to_close: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        # ── END new parameters ──────────────────────────────
    ) -> JobHandle[R]: ...
```

#### Tag inheritance semantics

| `inherit_tags` | `tags` | Resulting job tags |
|---|---|---|
| `True` (default) | `None` | Parent job's tags (or `()` if parent has none) |
| `True` (default) | `["new-tag"]` | Parent tags + explicit tags, merged (union, parent-first order, deduped) |
| `False` | `None` | `()` (current behavior) |
| `False` | `["new-tag"]` | `("new-tag",)` (explicit only, no inheritance) |

#### Parent tag propagation via `contextvars.ContextVar`

The `SubJobEnqueuer` is shared across concurrent consumers in the same event loop. A per-instance field would be racy. Instead, use a `contextvars.ContextVar` that the consumer sets before each actor invocation — asyncio Tasks copy the context, so concurrent consumers each see their own value.

```python
# src/taskq/client/_enqueuer.py

import contextvars

_parent_tags_var: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "taskq_parent_tags",
    default=(),
)

def set_parent_tags(tags: tuple[str, ...]) -> contextvars.Token[tuple[str, ...]]:
    """Set the parent job's tags for sub-job tag inheritance.

    Called by the consumer before actor invocation. The returned token
    can be used to reset the context after the actor completes.
    """
    return _parent_tags_var.set(tags)
```

Inside `enqueue()`:

```python
async def enqueue[P: BaseModel, R: BaseModel | None](self, ...) -> JobHandle[R]:
    # Resolve tags with inheritance
    resolved_tags = self._resolve_tags(tags, inherit_tags)
    args = build_enqueue_args(
        actor_ref,
        payload,
        # ... existing params ...
        tags=resolved_tags,                           # NEW
        schedule_to_close=schedule_to_close,          # NEW
        start_to_close=start_to_close,                # NEW
        heartbeat_timeout=heartbeat_timeout,          # NEW
        clock=self._clock,
    )
    # ... rest unchanged ...

def _resolve_tags(
    self,
    tags: list[str] | None,
    inherit_tags: bool,
) -> list[str] | None:
    """Resolve tags with parent inheritance.

    Returns a list suitable for build_enqueue_args, or None for empty.
    """
    parent_tags = _parent_tags_var.get() if inherit_tags else ()

    if tags is None:
        if parent_tags:
            return list(parent_tags)
        return None

    if not inherit_tags or not parent_tags:
        return tags

    # Merge: parent tags first, then explicit tags, deduped
    seen: set[str] = set(parent_tags)
    merged = list(parent_tags)
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            merged.append(tag)
    return merged
```

#### Consumer integration

```python
# src/taskq/worker/_consumer.py

from taskq.client._enqueuer import _parent_tags_var, set_parent_tags

# Inside consume_job(), before constructing JobContext. consume_job is a
# module-level function; the setting is read from _effective_settings
# (WorkerSettings | None, resolved from deps.settings or the settings
# parameter at _consumer.py:347). Settings absent (tests) → inherit.
_inherit = (
    _effective_settings is None or _effective_settings.sub_job_inherit_tags
)
token = set_parent_tags(tuple(job.tags)) if _inherit else None
try:
    ctx: JobContext[BaseModel] = JobContext(
        # ... existing fields ...
        jobs=live_enqueuer,
        # ...
    )
    # ... run actor ...
finally:
    if token is not None:
        _parent_tags_var.reset(token)
```

The setting itself is added to `WorkerSettings` in `src/taskq/settings.py:338` (NOT `worker/_bootstrap.py` — that module has no settings class):

```python
# src/taskq/settings.py — in WorkerSettings

sub_job_inherit_tags: bool = Field(
    default=True,
    description=(
        "When false, sub-jobs enqueued via ctx.jobs.enqueue() do not inherit "
        "the parent job's tags (pre-1.0 behavior). Fleet-level kill switch "
        "for the inherit_tags=True default; env var TASKQ_SUB_JOB_INHERIT_TAGS."
    ),
)
```

Because `TaskQSettings` is a pydantic-settings class with `env_prefix = "TASKQ_"`, the field is automatically settable via the `TASKQ_SUB_JOB_INHERIT_TAGS` environment variable — no extra wiring for operators.

The stub consumer in `worker/run.py` (a test harness with no settings object) follows the same set/reset pattern but calls `set_parent_tags(tuple(job.tags))` unconditionally.

### Issue #54: Bulk cancel by filter

#### New type: `BulkCancelResult`

`BulkCancelResult` is defined in `taskq.backend._protocol` (next to `ScheduleRecord`, which is already a Pydantic `BaseModel` in that module) and re-exported through the same chain as `CancelResult`: `taskq.types` → `taskq.client` → `taskq`. This avoids the circular import that would arise from defining it in `types.py`: `types.py:18` already imports `from taskq.backend._protocol import JobId, JobStatus`, so a back-edge `_protocol → types` would fail at import time before `JobId` is defined. The `types.py` docstring claim that the protocol stays "pydantic-free" is already stale (`ScheduleRecord` at `_protocol.py:594` is a Pydantic model) — the implementation updates that docstring as part of Task 2.

```python
# src/taskq/backend/_protocol.py — define next to ScheduleRecord

class BulkCancelResult(BaseModel):
    """Structured outcome of a bulk cancellation request.

    Returned by ``JobsClient.cancel_where()`` so callers can inspect
    how many jobs were cancelled directly (pending/scheduled → terminal
    cancelled) vs how many had cooperative cancel requested (running →
    cancel_phase=1).
    """

    model_config = ConfigDict(frozen=True)

    cancelled_directly: int
    """Count of pending/scheduled jobs moved straight to terminal 'cancelled'."""

    cancel_requested: int
    """Count of running jobs with cancel_phase=1 set (cooperative cancel)."""

    cancelled_ids: list[UUID]
    """IDs of jobs cancelled directly (pending/scheduled → cancelled)."""

    cancel_requested_ids: list[UUID]
    """IDs of running jobs with cancel requested."""

    @property
    def total_affected(self) -> int:
        """Total jobs affected by the bulk cancel."""
        return self.cancelled_directly + self.cancel_requested
```

```python
# src/taskq/types.py — re-export (add to import and __all__)

from taskq.backend._protocol import BulkCancelResult  # noqa: F401 — re-export
__all__ = ["BulkCancelResult", "CancelResult", "StateChangeEvent"]

# src/taskq/client/__init__.py — re-export alongside CancelResult

from taskq.types import BulkCancelResult, CancelResult
__all__ = ["BulkCancelResult", "CancelResult", "JobEvent", "JobHandle", "JobsClient", "SubJobEnqueuer", "TaskQ"]

# src/taskq/__init__.py — re-export at top level via the client surface
# (same import line pattern as CancelResult at __init__.py:40)
from taskq.client import BulkCancelResult, CancelResult, JobEvent, JobHandle, JobsClient, TaskQ
```

#### New exception: `EmptyFilterError`

```python
# src/taskq/exceptions.py

class EmptyFilterError(TaskQError):
    """Raised when cancel_where is called with a filter that has no predicates.

    A filter with no queue, status, actor, identity_key, batch_id, tags, or
    active predicate would match every job in the table — almost certainly
    a bug. The guardrail is intentionally loud: the caller must add at least
    one predicate or explicitly bypass with ``allow_empty_filter=True``.
    """

    def __init__(self) -> None:
        super().__init__(
            "cancel_where requires at least one filter predicate "
            "(queue, status, actor, identity_key, batch_id, tags, or active); "
            "an empty filter would cancel the entire table. "
            "Pass allow_empty_filter=True to override this guardrail."
        )
```

#### Client API: `JobsClient.cancel_where()`

```python
# src/taskq/client/_jobs.py

class JobsClient:
    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None = None,
        *,
        allow_empty_filter: bool = False,
    ) -> BulkCancelResult:
        """Cancel all jobs matching *filter* in a single set-based operation.

        Pending/scheduled jobs are moved straight to terminal 'cancelled'
        (no running actor to cooperate with). Running jobs get
        ``cancel_phase=1`` set (cooperative cancel) — the worker's
        heartbeat-driven cancel controller observes the phase change and
        sets the in-process ``cancel_event``.

        **Guardrail:** a filter with no predicates (no queue, status,
        actor, identity_key, batch_id, tags, or active) is rejected with
        :class:`EmptyFilterError` unless ``allow_empty_filter=True`` is
        passed. This prevents accidental full-table cancels.

        **Filter fields used:** ``queue``, ``status``, ``actor``,
        ``identity_key``, ``batch_id``, ``tags``, ``active``. The
        ``limit``, ``cursor``, and ``order_by`` fields are ignored — a
        bulk cancel is not paginated.

        **Snapshot boundary:** jobs matching *filter* that are enqueued
        *after* the statement's snapshot escape this call. Stop producers
        first, or issue a follow-up call; the returned counts make
        non-convergence detectable.

        Returns a :class:`BulkCancelResult` with counts and affected IDs
        for observability.

        Increments ``taskq.cancellation.requested`` once per call
        (regardless of the number of jobs affected).
        """
        ...
```

#### `TaskQ` delegate

```python
# src/taskq/client/_taskq.py

class TaskQ:
    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None = None,
        *,
        allow_empty_filter: bool = False,
    ) -> BulkCancelResult:
        """Cancel all jobs matching *filter*. See :meth:`JobsClient.cancel_where`."""
        return await self._require_open().cancel_where(
            filter, reason, allow_empty_filter=allow_empty_filter
        )
```

#### Backend protocol addition

`BulkCancelResult` is already defined in `_protocol.py` (see above), so the protocol method's return annotation has no import dependency issue.

```python
# src/taskq/backend/_protocol.py

class Backend(Protocol):
    # ── Cancel signals ──────────────────────────────────────────
    async def write_cancel_request(
        self,
        job_id: JobId,
        reason: str | None,
    ) -> bool: ...

    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None,
    ) -> BulkCancelResult:
        """Cancel all jobs matching *filter* in a set-based operation.

        Pending/scheduled jobs → terminal 'cancelled'.
        Running jobs → cancel_phase=1 (cooperative cancel + NOTIFY).

        The filter's ``limit``, ``cursor``, and ``order_by`` fields are
        ignored — this is a bulk write, not a paginated read.

        Returns a :class:`BulkCancelResult` with counts and affected IDs.
        """
        ...
```

**Protocol version:** No bump required. `cancel_where` is purely additive — a v3 implementation lacks the method, and the client raises `AttributeError` loudly (not a silent misbehavior). See the bump rule in `_protocol.py` lines 79-84.

#### PostgresBackend SQL

The SQL uses two CTEs in a single statement: one for pending/scheduled (→ terminal cancelled), one for running (→ cooperative cancel_phase=1). Events are inserted within the same transaction via `executemany`.

**EPQ safety (Critical design requirement):** Under READ COMMITTED, when an UPDATE reaches a row that a concurrent transaction has locked/updated, Postgres waits, then re-evaluates the UPDATE's **own WHERE clause** (EvalPlanQual) against the new row version. Predicates inside a materialized CTE are **not** re-evaluated. Therefore, the status/cancel_phase predicates must appear **both** in the `matching` CTE (for snapshot-time row selection) **and** in each UPDATE's own WHERE clause (for EPQ re-evaluation). This mirrors the existing single-job path: `cancel_pending_scheduled` (`_sql_templates.py:407-415`) locks `FOR UPDATE` and repeats `AND status IN ('pending','scheduled')` in the UPDATE's WHERE; `cancel_running` (`_sql_templates.py:416-420`) puts `AND status = 'running' AND cancel_phase = 0` directly in the UPDATE.

**Residual window:** A job claimed by a worker (pending→running) between the statement snapshot and the row lock will be skipped by this call (the EPQ re-check sees `running` and rejects it from the pending/scheduled UPDATE). This is correct and safe — the job escapes this cancel call and requires a subsequent `cancel_where` (or the caller stops producers first). This is strictly preferable to overwriting a running job to terminal `cancelled` while a worker executes it.

**Lock ordering / deadlock handling:** The `matching` CTE scans with `ORDER BY id` so the UPDATEs acquire row locks in ascending `id` order, reducing deadlock probability against dispatch (`FOR UPDATE SKIP LOCKED`, `_dispatch_sql.py:125`) and heartbeat lock ordering. Eliminating deadlocks outright is not claimed — when Postgres detects one it aborts this statement with `asyncpg.DeadlockDetectedError`, and the **backend** retries the whole transaction (max 3 attempts, jittered backoff; safe because the single transaction rolls back atomically and the EPQ predicates re-filter on every attempt). The retry is backend-owned, not client-owned: the exception type is asyncpg-specific and the backend owns the transaction boundary.

**Large result sets:** A single `cancel_where` call is bounded by transaction size. For tenant-scale cancels (10⁵+ matching rows), the operator should partition via filter (e.g., `JobFilter(queue=..., tags=...)` to split by queue). The implementation does not chunk internally — a single transaction covering 10⁶ rows would hold locks too long. The `BulkCancelResult` counts let the caller verify completeness and issue follow-up calls for remaining partitions. Document this guidance in `jobs-clients.md`.

**Event parity:** Both backends insert the same event kinds as the existing single-job `write_cancel_request` path: for pending/scheduled jobs, both `state_change` (with actual `from_state`) and `cancel_request`; for running jobs, only `cancel_request`. This matches `postgres.py:555-561` and `in_memory.py:595-601`.

**Post-snapshot enqueue boundary:** Jobs matching the filter that are enqueued *after* the statement's snapshot escape the cancel. Convergence is the caller's responsibility — stop producers before calling `cancel_where`, or issue a second call to catch stragglers. The `BulkCancelResult` counts let the caller detect non-convergence.

```sql
-- src/taskq/backend/_cancel_bulk.py — cancel_where SQL (inlined, dynamic)

-- $1..$N: filter parameters (same positional binding as list_jobs).
-- The reason is never interpolated into SQL or JSON text — it is bound
-- per-row as a jsonb parameter at event-insert time (see below).

WITH matching AS (
    SELECT id, status, locked_by_worker
    FROM "{schema}".jobs
    WHERE {filter_conditions}
    ORDER BY id           -- deterministic lock ordering to reduce deadlocks
),
cancelled AS (
    UPDATE "{schema}".jobs AS j
    SET status = 'cancelled',
        finished_at = clock_timestamp()
    FROM (
        SELECT id, status AS prev_status
        FROM matching
        WHERE status IN ('pending', 'scheduled')
    ) AS prev
    WHERE j.id = prev.id
      AND j.status IN ('pending', 'scheduled')   -- EPQ re-check (Critical)
    RETURNING j.id, prev.prev_status
),
cancel_requested AS (
    UPDATE "{schema}".jobs AS j
    SET cancel_requested_at = now(),
        cancel_phase = 1
    WHERE j.id IN (
        SELECT id FROM matching
        WHERE status = 'running' AND cancel_phase = 0
    )
    AND j.status = 'running' AND j.cancel_phase = 0  -- EPQ re-check (Critical)
    RETURNING j.id, j.locked_by_worker
)
SELECT
    (SELECT count(*)::int FROM cancelled) AS cancelled_directly,
    (SELECT count(*)::int FROM cancel_requested) AS cancel_requested,
    (SELECT array_agg(id ORDER BY id) FROM cancelled) AS cancelled_ids,
    (SELECT array_agg(prev_status ORDER BY id) FROM cancelled) AS cancelled_prev_statuses,
    (SELECT array_agg(id ORDER BY id) FROM cancel_requested) AS cancel_requested_ids,
    (SELECT array_agg(locked_by_worker ORDER BY id) FROM cancel_requested) AS cancel_requested_workers
```

Three points of correctness in this shape:

- **`FROM prev` captures the previous status** in the same round-trip (mirroring the single-job `cancel_pending_scheduled` template at `_sql_templates.py:407-415`). `UPDATE ... RETURNING` alone can only return the *new* row; the `prev` subquery carries the snapshot status through so `state_change` events record the actual `from_state` (`'pending'` or `'scheduled'`), not a synthetic placeholder. The target table is aliased (`AS j`) so `RETURNING j.id` is unambiguous against `prev.id`.
- **The EPQ-re-checked predicates are the ones on the target table** (`j.status ...`, `j.cancel_phase ...`). Under READ COMMITTED, when the UPDATE blocks on a concurrently-locked row, Postgres re-evaluates the UPDATE's own WHERE clause against the newest row version; `prev.*` values come from the statement snapshot and are not re-evaluated — which is exactly why the status predicates must live on `j`, not only inside `matching`/`prev`. If a job was claimed (→ `running`) or finished (→ terminal) mid-statement, the re-check rejects it and the row is skipped.
- **Aggregate arrays are `ORDER BY id`-aligned**, so Python can zip `cancelled_ids` with `cancelled_prev_statuses` (per-job `from_state`) and `cancel_requested_ids` with `cancel_requested_workers` (NOTIFY targets) — no second query.

Events are inserted in separate statements within the same transaction (after the main CTE query returns counts/IDs), reusing the existing `sql.insert_event` template with `executemany`; the `detail` JSON is serialized in Python via `jsonb_param` (never f-string interpolation — see the H2 design note in Task 5).

**NOTIFY for running jobs:** After the transaction commits, `PostgresBackend.cancel_where` sends `pg_notify` to the fleet channel and each affected job's per-worker channel, reusing the channel helpers and payload shape from `write_cancel_request` (`events_channel`/`worker_channel` in `constants.py:221,236`; pattern at `postgres.py:585-603`). For bulk cancel, the NOTIFY calls are batched into a single statement to avoid N round-trips:

```sql
SELECT pg_notify(channel, payload)
FROM unnest($1::text[], $2::text[]) AS t(channel, payload)
```

The send lives in `postgres.py` (not `_cancel_bulk.py`) because the `taskq.cancel.notify_sent` counter is module-level there (`postgres.py:146-149,603`) — importing it from `_cancel_bulk` would create a module cycle. The counter is incremented once per job notified (batch `.add(len(notify_targets))`), matching the single-job path's per-job semantics.

**Event insertion:** Events are inserted via `executemany` within the same transaction:
- For cancelled (pending/scheduled) jobs: one `state_change` event with `from_state` set to the actual previous status (from `cancelled_prev_statuses`) and one `cancel_request` event — matching the single-job path (`postgres.py:555-561`). Details: `jsonb_param({"from_state": prev_status, "to_state": "cancelled"})` and `jsonb_param({"reason": reason} if reason is not None else {})`.
- For cancel_requested (running) jobs: one `cancel_request` event with `jsonb_param({"reason": reason} if reason is not None else {})`.

#### In-memory backend implementation

The in-memory backend must **not** call `_list_jobs` directly with the caller's filter, because `_list_jobs` applies `filters.limit` (default 100) and cursor slicing (`testing/_reads.py:87-98`) — silently capping the cancel to 100 rows and contradicting the contract that `limit`, `cursor`, and `order_by` are ignored. Instead, call `_list_jobs` with a **sanitized filter** that unsets `limit`, `cursor`, and `order_by`:

```python
# src/taskq/testing/_cancel_bulk.py

from dataclasses import replace as dc_replace
from typing import TYPE_CHECKING
from uuid import UUID

from taskq.backend._protocol import BulkCancelResult, CancelPhase, JobFilter
from taskq.testing._reads import _list_jobs

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

async def _cancel_where(
    self: "InMemoryBackend",  # module-fn style, like testing/_reads.py
    filter: JobFilter,
    reason: str | None,
) -> BulkCancelResult:
    # Sanitize the filter: cancel_where ignores limit, cursor, and order_by.
    # Use a very large limit (2**31) instead of None because JobFilter.limit
    # is typed as int (not int | None) with a __post_init__ guard against
    # negatives. cursor=None disables keyset slicing. order_by=None selects
    # the default priority/scheduled_at/id sort, which is harmless for cancel.
    sanitized = dc_replace(filter, limit=2**31, cursor=None, order_by=None)
    rows = await _list_jobs(self, sanitized)

    cancelled_ids: list[UUID] = []
    cancel_requested_ids: list[UUID] = []

    for row in rows:
        if row.status in ("pending", "scheduled"):
            now = self._clock.now()
            self._jobs[row.id] = dc_replace(
                row,
                status="cancelled",
                finished_at=now,
            )
            self._append_state_change_event(
                job_id=row.id,
                from_state=row.status,          # actual previous status
                to_state="cancelled",
                now=now,
            )
            self._append_cancel_request_event(row.id, now, reason)
            cancelled_ids.append(row.id)

        elif row.status == "running" and row.cancel_phase == CancelPhase.NONE:
            now = self._clock.now()
            self._jobs[row.id] = dc_replace(
                row,
                cancel_requested_at=now,
                cancel_phase=CancelPhase.COOPERATIVE,
            )
            self._append_cancel_request_event(row.id, now, reason)
            for event in self._cancel_wake_subscribers:
                event.set()
            cancel_requested_ids.append(row.id)

    return BulkCancelResult(
        cancelled_directly=len(cancelled_ids),
        cancel_requested=len(cancel_requested_ids),
        cancelled_ids=cancelled_ids,
        cancel_requested_ids=cancel_requested_ids,
    )
```

**Event parity:** The in-memory implementation inserts the same event kinds as the existing single-job `write_cancel_request` path and the Postgres bulk path: for pending/scheduled jobs, both `state_change` (with actual `from_state`) and `cancel_request`; for running jobs, only `cancel_request`. This matches `in_memory.py:595-601`.

#### Filter→WHERE reuse

The filter-to-SQL-WHERE builder in `_reads._list_jobs` (lines 53-141) builds conditions dynamically. For `cancel_where`, we need the same WHERE clause. Extract the **predicate-only** condition builder (queue, status, actor, identity_key, batch_id, tags, active) into a shared helper. The `schema` parameter is **not** needed — the existing builder in `_reads.py:58-115` produces schema-less fragments (the schema is applied by the caller in the surrounding SQL string).

```python
# src/taskq/backend/_filter_sql.py  (NEW)

@dataclass(frozen=True, slots=True)
class FilterSQL:
    """Built SQL fragments and parameters from a JobFilter."""
    conditions: list[str]
    params: list[object]

def build_filter_conditions(filter: JobFilter) -> FilterSQL:
    """Build WHERE clause conditions and parameters from a JobFilter.

    Shared between _list_jobs (reads) and cancel_where (writes) so the
    filter semantics are identical for query and mutation.

    Only predicate fields (queue, status, actor, identity_key, batch_id,
    tags, active) are translated to conditions. The ``cursor``, ``limit``,
    and ``order_by`` fields are NOT handled here — callers apply them
    separately:

    - ``_list_jobs`` appends the cursor keyset condition and LIMIT/OFFSET
      after calling this helper (preserving the existing behavior at
      ``_reads.py:102-115``).
    - ``cancel_where`` ignores cursor/limit/order_by entirely (bulk writes
      are not paginated).
    """
    # ... extracted from _reads._list_jobs lines 58-100 (the predicate
    # fields). The cursor keyset block (lines 102-115) and LIMIT/ORDER BY
    # (lines 117-138) stay in _list_jobs and are appended after this call.
```

Both `_list_jobs` and `_cancel_bulk` call this helper, ensuring filter semantics are DRY. After extraction, `_list_jobs` regains cursor/limit/order_by handling by appending the keyset condition (`_reads.py:102-115`) and `LIMIT $N` after the shared `build_filter_conditions` call — the existing `test_job_filter.py` and `test_postgres_reads.py` suites verify no regression.

---

## Implementation Plan

### Task 1: Extract filter→SQL builder (refactor)

**Goal:** Extract the WHERE-clause builder from `_reads._list_jobs` into a shared module so `cancel_where` reuses the exact same filter logic.

**Files:**
- CREATE: `src/taskq/backend/_filter_sql.py`
- MODIFY: `src/taskq/backend/_reads.py` — import and use the shared builder
- CREATE: `tests/test_filter_sql.py`

#### TDD — Red

```python
# tests/test_filter_sql.py

from taskq.backend._filter_sql import build_filter_conditions, FilterSQL
from taskq.backend._protocol import JobFilter

class TestBuildFilterConditions:
    def test_empty_filter_produces_no_conditions(self) -> None:
        result = build_filter_conditions(JobFilter())
        assert result.conditions == []
        assert result.params == []

    def test_queue_filter(self) -> None:
        result = build_filter_conditions(JobFilter(queue="default"))
        assert len(result.conditions) == 1
        assert "queue" in result.conditions[0]
        assert result.params == ["default"]

    def test_tags_filter(self) -> None:
        result = build_filter_conditions(JobFilter(tags=("alpha", "beta")))
        assert len(result.conditions) == 1
        assert "tags" in result.conditions[0]
        assert result.params == [["alpha", "beta"]]

    def test_batch_id_filter(self) -> None:
        from uuid import uuid4
        bid = uuid4()
        result = build_filter_conditions(JobFilter(batch_id=bid))
        assert len(result.conditions) == 1
        assert "metadata" in result.conditions[0]

    def test_active_true_filter(self) -> None:
        result = build_filter_conditions(JobFilter(active=True))
        assert len(result.conditions) == 1
        assert "status" in result.conditions[0]

    def test_combined_filters(self) -> None:
        result = build_filter_conditions(
            JobFilter(queue="e2e", actor="my_actor", tags=("run-123",)),
        )
        assert len(result.conditions) == 3

    def test_cursor_and_order_by_ignored(self) -> None:
        """cancel_where doesn't use cursor/order_by — the builder should
        not include them in conditions."""
        result = build_filter_conditions(
            JobFilter(cursor="some-cursor", order_by=None),
        )
        # cursor/order_by are not part of filter conditions
        assert result.conditions == []
```

#### TDD — Green

Extract the condition-building logic from `_reads._list_jobs` into `build_filter_conditions()`. Update `_list_jobs` to call it. Run existing `test_job_filter.py` and `test_postgres_reads.py` to verify no regression.

#### Acceptance criteria
- All existing `test_job_filter.py` tests pass
- All existing `test_postgres_reads.py` tests pass
- New `test_filter_sql.py` tests pass
- `build_filter_conditions` is pure (no I/O, no global state)

---

### Task 2: Add `BulkCancelResult` type and `EmptyFilterError` exception

**Goal:** Define the result type and guardrail exception before implementing the operation.

**Files:**
- MODIFY: `src/taskq/backend/_protocol.py` — define `BulkCancelResult` (next to `ScheduleRecord`)
- MODIFY: `src/taskq/types.py` — re-export `BulkCancelResult`; reconcile the stale "pydantic-free" docstring
- MODIFY: `src/taskq/client/__init__.py` — re-export `BulkCancelResult` alongside `CancelResult`
- MODIFY: `src/taskq/exceptions.py` — add `EmptyFilterError`
- MODIFY: `src/taskq/__init__.py` — export both
- CREATE: `tests/test_bulk_cancel_types.py`

> **Why `_protocol.py`, not `types.py`:** `types.py:18` imports `from taskq.backend._protocol import JobId, JobStatus`. If `_protocol.py` imported `BulkCancelResult` from `types.py`, the cycle `_protocol → types → _protocol` would fail at import time before `JobId` (line 198) is defined. Defining `BulkCancelResult` in `_protocol.py` (where `ScheduleRecord`, another Pydantic `BaseModel`, already lives at line 594) avoids the cycle. The `types.py` docstring claim that the protocol is "pydantic-free" is already stale due to `ScheduleRecord` and should be updated.

#### TDD — Red

```python
# tests/test_bulk_cancel_types.py

from uuid import uuid4
import pytest
from taskq.types import BulkCancelResult
from taskq.exceptions import EmptyFilterError

class TestBulkCancelResult:
    def test_construction(self) -> None:
        ids = [uuid4() for _ in range(3)]
        result = BulkCancelResult(
            cancelled_directly=2,
            cancel_requested=1,
            cancelled_ids=ids[:2],
            cancel_requested_ids=ids[2:],
        )
        assert result.cancelled_directly == 2
        assert result.cancel_requested == 1
        assert result.total_affected == 3
        assert len(result.cancelled_ids) == 2
        assert len(result.cancel_requested_ids) == 1

    def test_frozen(self) -> None:
        result = BulkCancelResult(
            cancelled_directly=0,
            cancel_requested=0,
            cancelled_ids=[],
            cancel_requested_ids=[],
        )
        with pytest.raises(Exception):
            result.cancelled_directly = 1  # type: ignore[misc]

    def test_zero_counts(self) -> None:
        result = BulkCancelResult(
            cancelled_directly=0,
            cancel_requested=0,
            cancelled_ids=[],
            cancel_requested_ids=[],
        )
        assert result.total_affected == 0

class TestEmptyFilterError:
    def test_is_taskq_error(self) -> None:
        from taskq.exceptions import TaskQError
        assert issubclass(EmptyFilterError, TaskQError)

    def test_message_mentions_guardrail(self) -> None:
        err = EmptyFilterError()
        assert "allow_empty_filter" in str(err)
        assert "filter predicate" in str(err)
```

#### TDD — Green

Add the types. Run the tests.

#### Acceptance criteria
- `BulkCancelResult` is a frozen Pydantic model with `total_affected` property
- `EmptyFilterError` is a `TaskQError` subclass with a helpful message
- Both are exported from `taskq` top-level

---

### Task 3: Add `cancel_where` to `Backend` protocol

**Goal:** Add the method signature to the `Backend` protocol.

**Files:**
- MODIFY: `src/taskq/backend/_protocol.py` — add `cancel_where` method; update docstring count
- MODIFY: `tests/test_backend_protocol.py` — update member count and expected member set

#### Implementation

```python
# In Backend protocol, after write_cancel_request:
async def cancel_where(
    self,
    filter: JobFilter,
    reason: str | None,
) -> BulkCancelResult:
    """Cancel all jobs matching *filter* in a set-based operation."""
    ...
```

#### Protocol docstring update

The `Backend` class docstring (`_protocol.py:693-697`) currently says "31 async methods plus two sync methods (33 methods total)". Adding `cancel_where` (async) makes it **32 async methods plus two sync methods (34 methods total)**. Update the docstring accordingly.

#### Test updates

`tests/test_backend_protocol.py:218-262` asserts exactly 36 public members and an exact member-name set. Adding `cancel_where` brings the count to **37**. Update:
- `test_exactly_thirty_six_public_members` → `test_exactly_thirty_seven_public_members` with `assert len(public) == 37`
- Add `"cancel_where"` to the `expected` set in `test_all_member_names_present`

#### TDD — Red

```python
# tests/test_backend_protocol.py — add to existing test file

async def test_protocol_has_cancel_where() -> None:
    """Backend protocol declares cancel_where."""
    from taskq.backend._protocol import Backend
    assert hasattr(Backend, "cancel_where")
```

#### Acceptance criteria
- `Backend` protocol includes `cancel_where` method
- Protocol version not bumped (purely additive, loud failure on missing method)
- `test_backend_protocol.py` updated: member count is 37, `cancel_where` in expected set, docstring count updated to 34 methods total
- All `test_backend_protocol.py` tests pass after update

---

### Task 4: Implement `cancel_where` for InMemoryBackend

**Goal:** Add bulk cancel to the in-memory backend for unit testing.

**Files:**
- CREATE: `src/taskq/testing/_cancel_bulk.py`
- MODIFY: `src/taskq/testing/in_memory.py` — wire `cancel_where` method
- CREATE: `tests/test_cancel_where.py`

#### TDD — Red

```python
# tests/test_cancel_where.py

import pytest
from uuid import uuid4
from taskq.backend._protocol import JobFilter
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

from datetime import UTC, datetime

_NOW = datetime(2026, 1, 1, tzinfo=UTC)

async def test_cancel_where_pending_jobs() -> None:
    """cancel_where moves pending jobs straight to 'cancelled'."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    # Enqueue 3 jobs with tag "tenant-acme", 2 without
    for i in range(3):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme", "run-001"), scheduled_at=_NOW))
    for i in range(2):
        await backend.enqueue(make_enqueue_args(tags=("tenant-other",), scheduled_at=_NOW))

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 3
    assert result.cancel_requested == 0
    assert result.total_affected == 3
    assert len(result.cancelled_ids) == 3

    # Verify the untagged jobs are still pending
    remaining = await backend.list_jobs(JobFilter(tags=("tenant-other",)))
    assert len(remaining) == 2
    assert all(r.status == "pending" for r in remaining)

async def test_cancel_where_running_jobs_cooperative() -> None:
    """cancel_where sets cancel_phase=1 for running jobs (cooperative)."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    # Enqueue and manually dispatch to running
    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)
    from dataclasses import replace
    # Simulate dispatch: set to running
    backend._jobs[row.id] = replace(backend._jobs[row.id], status="running", locked_by_worker=uuid4())

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 0
    assert result.cancel_requested == 1
    assert len(result.cancel_requested_ids) == 1

    # Verify the job is still running but has cancel_phase=1
    updated = await backend.get(row.id)
    assert updated is not None
    assert updated.status == "running"
    assert updated.cancel_phase == 1  # CancelPhase.COOPERATIVE

async def test_cancel_where_mixed_statuses() -> None:
    """cancel_where handles both pending and running in one call."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    # 2 pending + 1 running, all tagged "tenant-acme"
    for _ in range(2):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    args3 = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row3 = await backend.enqueue(args3)
    from dataclasses import replace
    backend._jobs[row3.id] = replace(backend._jobs[row3.id], status="running", locked_by_worker=uuid4())

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 2
    assert result.cancel_requested == 1
    assert result.total_affected == 3

async def test_cancel_where_no_matches_returns_zero() -> None:
    """cancel_where with a filter matching nothing returns zero counts."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    result = await backend.cancel_where(
        JobFilter(tags=("nonexistent",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 0
    assert result.cancel_requested == 0
    assert result.total_affected == 0

async def test_cancel_where_already_cancelled_not_affected() -> None:
    """Already-terminal jobs are not re-cancelled."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)
    from dataclasses import replace
    backend._jobs[row.id] = replace(backend._jobs[row.id], status="cancelled", finished_at=_NOW)

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.total_affected == 0

async def test_cancel_where_filter_by_batch_id() -> None:
    """cancel_where works with batch_id filter."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    bid = uuid4()
    for i in range(3):
        args = make_enqueue_args(
            tags=("tenant-acme",),
            scheduled_at=_NOW,
            metadata={"batch_id": str(bid)},
        )
        await backend.enqueue(args)
    # Untagged job with different batch
    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW, metadata={"batch_id": str(uuid4())}))

    result = await backend.cancel_where(
        JobFilter(batch_id=bid),
        reason="batch abort",
    )

    assert result.cancelled_directly == 3

async def test_cancel_where_filter_by_queue_and_actor() -> None:
    """cancel_where works with queue and actor filters."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    await backend.enqueue(make_enqueue_args(queue="default", actor="worker-a", scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(queue="default", actor="worker-b", scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(queue="priority", actor="worker-a", scheduled_at=_NOW))

    result = await backend.cancel_where(
        JobFilter(queue="default", actor="worker-a"),
        reason="abort",
    )

    assert result.cancelled_directly == 1

async def test_cancel_where_active_filter() -> None:
    """cancel_where with active=True targets only non-terminal jobs."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    # 2 active (pending) + 1 terminal (succeeded)
    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))
    args3 = make_enqueue_args(scheduled_at=_NOW)
    row3 = await backend.enqueue(args3)
    from dataclasses import replace
    backend._jobs[row3.id] = replace(backend._jobs[row3.id], status="succeeded", finished_at=_NOW)

    result = await backend.cancel_where(
        JobFilter(active=True),
        reason="drain",
    )

    assert result.cancelled_directly == 2  # only the 2 pending

async def test_cancel_where_ignores_filter_limit() -> None:
    """cancel_where cancels ALL matching jobs even when filter.limit is small.

    This guards against the H3 bug: _list_jobs applies filters.limit (default
    100). If _cancel_where reuses _list_jobs without sanitizing the filter,
    a caller passing JobFilter(limit=5, tags=...) would cancel only 5 jobs.
    """
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    # Enqueue 11 jobs matching the tag
    for _ in range(11):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    # Pass a restrictive limit — cancel_where must ignore it
    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",), limit=5),
        reason="offboard",
    )

    assert result.cancelled_directly == 11  # all of them, not just 5
    assert result.total_affected == 11
```

#### TDD — Green

Implement `_cancel_where` in `src/taskq/testing/_cancel_bulk.py`, wire it in `in_memory.py`.

#### Acceptance criteria
- All red tests pass
- `InMemoryBackend.cancel_where` satisfies the `Backend` protocol
- Events are inserted for both directly-cancelled and cooperative-cancel jobs
- Cancel wake subscribers are notified for running jobs
- `cancel_where` ignores `filter.limit` and `filter.cursor` — all matching jobs are cancelled regardless of pagination fields (verified by `test_cancel_where_ignores_filter_limit`)

---

### Task 5: Implement `cancel_where` for PostgresBackend

**Goal:** Add the set-based SQL bulk cancel to the Postgres backend.

**Files:**
- CREATE: `src/taskq/backend/_cancel_bulk.py`
- MODIFY: `src/taskq/backend/postgres.py` — wire `cancel_where` method
- CREATE: `tests/test_cancel_where_pg.py`

> **No `SqlTemplates` field needed:** The SQL is inlined in `_cancel_bulk.py` with dynamic filter conditions baked in via f-string (matching the dynamic-SQL precedent in `_reads.py`). A `SqlTemplates.cancel_where` field would require template-level `{filter_conditions}` placeholder substitution that doesn't fit the static-template rendering model — the filter conditions are built at call time from `build_filter_conditions()`, not at schema-render time.

#### Implementation

```python
# src/taskq/backend/_cancel_bulk.py

import asyncio
import random

import asyncpg

from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._records import jsonb_param

# Returns (result, notify_targets) where notify_targets is
# [(job_id, worker_id)] for running jobs that got cooperative cancel.
# NOTIFY itself is sent by PostgresBackend.cancel_where (see wiring below)
# because the taskq.cancel.notify_sent counter is module-level in postgres.py.
async def _cancel_where(
    pool: asyncpg.Pool,
    schema: str,
    sql: SqlTemplates,
    filter: JobFilter,
    reason: str | None,
) -> tuple[BulkCancelResult, list[tuple[UUID, UUID]]]:
    filter_sql = build_filter_conditions(filter)
    conditions_str = " AND ".join(filter_sql.conditions) if filter_sql.conditions else "TRUE"
    params = filter_sql.params

    # Single CTE statement: snapshot matching IDs, then two UPDATEs with
    # EPQ-safe predicates duplicated in each UPDATE's own WHERE clause.
    # ORDER BY id in the matching CTE ensures deterministic lock ordering
    # to reduce deadlock probability.
    cancel_sql = f"""
    WITH matching AS (
        SELECT id, status, locked_by_worker
        FROM "{schema}".jobs
        WHERE {conditions_str}
        ORDER BY id
    ),
    cancelled AS (
        UPDATE "{schema}".jobs AS j
        SET status = 'cancelled', finished_at = clock_timestamp()
        FROM (
            SELECT id, status AS prev_status
            FROM matching
            WHERE status IN ('pending', 'scheduled')
        ) AS prev
        WHERE j.id = prev.id
          AND j.status IN ('pending', 'scheduled')       -- EPQ re-check (Critical)
        RETURNING j.id, prev.prev_status
    ),
    cancel_requested AS (
        UPDATE "{schema}".jobs AS j
        SET cancel_requested_at = now(), cancel_phase = 1
        WHERE j.id IN (
            SELECT id FROM matching
            WHERE status = 'running' AND cancel_phase = 0
        )
        AND j.status = 'running' AND j.cancel_phase = 0  -- EPQ re-check (Critical)
        RETURNING j.id, j.locked_by_worker
    )
    SELECT
        (SELECT count(*)::int FROM cancelled) AS cancelled_directly,
        (SELECT count(*)::int FROM cancel_requested) AS cancel_requested,
        (SELECT array_agg(id ORDER BY id) FROM cancelled) AS cancelled_ids,
        (SELECT array_agg(prev_status ORDER BY id) FROM cancelled) AS cancelled_prev_statuses,
        (SELECT array_agg(id ORDER BY id) FROM cancel_requested) AS cancel_requested_ids,
        (SELECT array_agg(locked_by_worker ORDER BY id) FROM cancel_requested) AS cancel_requested_workers
    """

    # Deadlock retry: the bulk UPDATE locks rows in id order while dispatch
    # and heartbeat transactions lock rows in their own orders, so Postgres
    # may abort this statement with DeadlockDetectedError. The retry lives in
    # the BACKEND (not JobsClient) because the exception type is
    # asyncpg-specific and the client layer stays backend-agnostic. The whole
    # UPDATE+events runs in one transaction, so a deadlocked attempt rolls
    # back completely and re-execution from a fresh snapshot is safe (the
    # EPQ predicates re-filter on every attempt). NOTIFY is sent by the
    # caller only after a successful commit, so no notify can fire for a
    # rolled-back attempt.
    for attempt in range(3):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(cancel_sql, *params)

                    assert row is not None  # aggregate SELECT always returns one row
                    cancelled_ids: list[UUID] = list(row["cancelled_ids"] or [])
                    cancel_requested_ids: list[UUID] = list(row["cancel_requested_ids"] or [])
                    prev_statuses: dict[UUID, str] = dict(
                        zip(cancelled_ids, row["cancelled_prev_statuses"] or [], strict=True)
                    )
                    notify_targets = [
                        (jid, wid)
                        for jid, wid in zip(
                            cancel_requested_ids,
                            row["cancel_requested_workers"] or [],
                            strict=True,
                        )
                        if wid is not None
                    ]

                    # Events — same kinds as single-job write_cancel_request
                    # (postgres.py:555-561): state_change + cancel_request for
                    # pending/scheduled; cancel_request only for running.
                    # detail JSON is serialized in Python via jsonb_param —
                    # never f-string interpolation (a reason containing "
                    # or \ would otherwise produce malformed jsonb and abort
                    # the transaction; see H2 design note below).
                    cr_detail = jsonb_param({"reason": reason} if reason is not None else {})
                    if cancelled_ids:
                        await conn.executemany(
                            sql.insert_event,  # (job_id, kind, detail) — kind is $2
                            [
                                (
                                    jid,
                                    "state_change",
                                    jsonb_param(
                                        {"from_state": prev_statuses[jid], "to_state": "cancelled"}
                                    ),
                                )
                                for jid in cancelled_ids
                            ],
                        )
                        await conn.executemany(
                            sql.insert_event,
                            [(jid, "cancel_request", cr_detail) for jid in cancelled_ids],
                        )
                    if cancel_requested_ids:
                        await conn.executemany(
                            sql.insert_event,
                            [(jid, "cancel_request", cr_detail) for jid in cancel_requested_ids],
                        )
            break  # committed successfully
        except asyncpg.DeadlockDetectedError:
            if attempt == 2:
                raise
            await asyncio.sleep(0.1 * (2**attempt) + random.random() * 0.05)

    result = BulkCancelResult(
        cancelled_directly=len(cancelled_ids),
        cancel_requested=len(cancel_requested_ids),
        cancelled_ids=cancelled_ids,
        cancel_requested_ids=cancel_requested_ids,
    )
    return result, notify_targets
```

#### Wiring in `postgres.py`

```python
# src/taskq/backend/postgres.py

async def cancel_where(
    self,
    filter: JobFilter,
    reason: str | None,
) -> BulkCancelResult:
    result, notify_targets = await _cancel_bulk._cancel_where(
        self._worker_pool, self._schema_name, self._sql, filter, reason
    )
    if notify_targets:
        # Post-commit NOTIFY, same pattern as write_cancel_request
        # (postgres.py:585-603) but batched into one statement.
        channels: list[str] = []
        payloads: list[str] = []
        for job_id, worker_id in notify_targets:
            payload = dumps_str(
                {"type": "cancel", "job_id": str(job_id), "worker_id": str(worker_id)}
            )
            channels.extend(
                [
                    events_channel(self._schema_name),
                    worker_channel(self._schema_name, str(worker_id)),
                ]
            )
            payloads.extend([payload, payload])
        async with self._worker_pool.acquire() as notify_conn:
            await notify_conn.execute(
                "SELECT pg_notify(channel, payload) "
                "FROM unnest($1::text[], $2::text[]) AS t(channel, payload)",
                channels,
                payloads,
            )
        _cancel_notify_sent_counter.add(len(notify_targets), {"schema": self._schema_name})
    return result
```

**Design note — capturing `prev_status` (L5):** The `cancelled` CTE uses the `FROM prev` pattern from the existing single-job `cancel_pending_scheduled` template (`_sql_templates.py:407-415`) so the actual previous status (`'pending'` or `'scheduled'`) flows into the `state_change` event detail in a single round-trip — not a synthetic `'pending_or_scheduled'` placeholder, and no second query. Two details matter: the target table must be aliased (`AS j`) because `prev` also exposes an `id` column (`RETURNING id` would be ambiguous), and the EPQ-re-checked predicate must reference the target table (`j.status`), since EPQ re-evaluates only the UPDATE's own WHERE clause against the newest row version — `prev.*` values are snapshot values.

**Design note — safe JSON serialization (H2 fix):** The `reason` string is serialized in Python via `jsonb_param({"reason": reason})` (which uses `dumps_str`/orjson), then bound as a jsonb parameter. This avoids the f-string injection bug where `reason` containing `"` or `\` would produce invalid JSON → `asyncpg.DataError` mid-transaction (rolling back the entire bulk cancel), or structurally valid but operator-shaped JSON. The existing single-job path does this safely at `_terminal.py:129-144`. The red test `test_pg_cancel_where_reason_with_quotes` pins the fix.

**Design note — deadlock retry (M6):** The retry loop lives in `_cancel_bulk._cancel_where` (the backend), not in `JobsClient`, for two reasons: the exception type is `asyncpg.DeadlockDetectedError` — catching it in the client would couple the backend-agnostic client layer to asyncpg — and the backend owns the transaction boundary, so it alone can guarantee that a retried attempt starts from a fresh snapshot with no partial effects (the single transaction rolls back UPDATEs and event inserts atomically). Max 3 attempts with jittered exponential backoff (100ms base). The `ORDER BY id` in the `matching` CTE reduces (but does not eliminate) deadlock probability against dispatch/heartbeat lock ordering. NOTIFY is sent by `PostgresBackend.cancel_where` only after a successful commit, so a retried-or-failed attempt never fires a spurious notify. The in-memory backend never deadlocks and needs no retry.

#### TDD — Red

```python
# tests/test_cancel_where_pg.py

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from taskq.backend._protocol import JobFilter
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_enqueue_args

_NOW = datetime(2026, 1, 1, tzinfo=UTC)

@pytest.mark.integration
class TestCancelWherePostgres:
    async def test_pg_cancel_where_pending(self, backend_pair) -> None:
        """PostgresBackend.cancel_where cancels pending jobs by tag."""
        from taskq.types import BulkCancelResult

        for _ in range(3):
            await backend_pair.enqueue(
                make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
            )
        await backend_pair.enqueue(
            make_enqueue_args(tags=("other",), scheduled_at=_NOW)
        )

        result = await backend_pair.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason="offboard",
        )

        assert result.cancelled_directly == 3
        assert result.cancel_requested == 0

        # Verify via list
        remaining = await backend_pair.list_jobs(JobFilter(tags=("other",)))
        assert len(remaining) == 1
        assert remaining[0].status == "pending"

    async def test_pg_cancel_where_events_inserted(self, backend_pair) -> None:
        """cancel_where inserts job_events for cancelled jobs — both
        state_change (with actual from_state) and cancel_request,
        matching single-job write_cancel_request semantics."""
        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend_pair.enqueue(args)

        await backend_pair.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason="test",
        )

        events = await backend_pair.get_events(row.id)
        kinds = [e.kind for e in events]
        # Both event kinds must be present (event parity with single-job path)
        assert "state_change" in kinds
        assert "cancel_request" in kinds
        # state_change should have actual from_state, not a synthetic placeholder
        sc = [e for e in events if e.kind == "state_change"]
        assert sc[0].detail.get("from_state") in ("pending", "scheduled")
        assert sc[0].detail.get("to_state") == "cancelled"

    async def test_pg_cancel_where_reason_with_quotes(self, backend_pair) -> None:
        """Reason containing double-quotes does not cause DataError.

        Guards against H2: f-string JSON interpolation would produce
        invalid JSON for reasons containing " or \\.
        """
        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend_pair.enqueue(args)

        result = await backend_pair.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason='offboard "tenant-acme" \\ done',
        )

        assert result.cancelled_directly == 1
        events = await backend_pair.get_events(row.id)
        cr = [e for e in events if e.kind == "cancel_request"]
        assert cr[0].detail.get("reason") == 'offboard "tenant-acme" \\ done'

    async def test_pg_cancel_where_batch_id_filter(self, backend_pair) -> None:
        """cancel_where works with batch_id filter on Postgres."""
        bid = uuid4()
        for _ in range(3):
            await backend_pair.enqueue(
                make_enqueue_args(
                    scheduled_at=_NOW,
                    metadata={"batch_id": str(bid)},
                )
            )

        result = await backend_pair.cancel_where(
            JobFilter(batch_id=bid),
            reason="batch abort",
        )

        assert result.cancelled_directly == 3

    async def test_pg_cancel_where_running_cooperative(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """cancel_where sets cancel_phase=1 for running jobs on Postgres.

        Uses a direct SQL UPDATE to simulate dispatch (status 'running'
        with a worker ID). clean_jobs_app provides a PG-only backend plus
        WorkerDeps with direct pool access; the in-memory path is covered
        by the Task 4 tests and the backend_pair tests above.
        """
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend.enqueue(args)

        # Simulate dispatch via direct SQL ('running' as a SQL literal so the
        # schema-scoped job_status enum coerces without a parameter cast).
        worker_id = uuid4()
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs '
                f"SET status = 'running', locked_by_worker = $1 WHERE id = $2",
                worker_id,
                row.id,
            )

        result = await backend.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason="offboard",
        )

        assert result.cancelled_directly == 0
        assert result.cancel_requested == 1

        # Verify the job is still running but has cancel_phase=1
        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "running"
        assert updated.cancel_phase == 1

        # Verify cancel_request event was inserted (no state_change for running)
        events = await backend.get_events(row.id)
        kinds = [e.kind for e in events]
        assert "cancel_request" in kinds
        assert "state_change" not in kinds

    async def test_pg_cancel_where_does_not_clobber_concurrent_claim(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """EPQ regression (C1): a job claimed (pending→running) while
        cancel_where executes must NOT be overwritten to terminal 'cancelled'.

        The bulk UPDATE blocks on the row lock held by the simulated claim
        transaction; after the claim commits, EvalPlanQual re-evaluates the
        UPDATE's own WHERE clause against the new row version, sees
        status='running', and skips the row. The job escapes this call
        entirely (documented residual window) instead of being clobbered.
        """
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend.enqueue(args)
        worker_id = uuid4()

        claim_conn = await deps.worker_pool.acquire()
        try:
            # Simulate a dispatch claim in a held-open transaction: the row is
            # locked and updated to 'running' but not yet committed.
            claim_tx = claim_conn.transaction()
            await claim_tx.start()
            await claim_conn.execute(
                f'UPDATE "{schema}".jobs '
                f"SET status = 'running', locked_by_worker = $1 WHERE id = $2",
                worker_id,
                row.id,
            )

            # Run cancel_where concurrently. Its CTE snapshot is taken at
            # statement start (before the claim commits, so the snapshot sees
            # 'pending'); its UPDATE then blocks on the claim's row lock.
            cancel_task = asyncio.create_task(
                backend.cancel_where(JobFilter(tags=("tenant-acme",)), reason="offboard")
            )
            await asyncio.sleep(0.2)  # let cancel_where reach the row lock
            await claim_tx.commit()
            result = await cancel_task

            # Safety property: the claimed job was NOT clobbered to terminal
            # 'cancelled'. It escaped this call (EPQ re-check rejected it for
            # the pending/scheduled UPDATE; the snapshot excluded it from the
            # running UPDATE), so a follow-up call is needed to cancel it.
            assert result.cancelled_directly == 0
            updated = await backend.get(row.id)
            assert updated is not None
            assert updated.status == "running"
        finally:
            await deps.worker_pool.release(claim_conn)

    async def test_pg_cancel_where_notify_sent_for_running(
        self, clean_jobs_app: JobsApp, pg_dsn: str
    ) -> None:
        """Batched NOTIFY fires on the fleet and per-worker channels for
        running jobs — same listener pattern as test_cancel_notify_integration.py."""
        import asyncpg
        from taskq.constants import events_channel, worker_channel

        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend.enqueue(args)
        worker_id = uuid4()
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs '
                f"SET status = 'running', locked_by_worker = $1 WHERE id = $2",
                worker_id,
                row.id,
            )

        received: list[str] = []
        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            await listen_conn.add_listener(
                events_channel(schema),
                lambda _c, _p, _ch, payload: received.append(payload),
            )
            await listen_conn.add_listener(
                worker_channel(schema, str(worker_id)),
                lambda _c, _p, _ch, payload: received.append(payload),
            )
            result = await backend.cancel_where(
                JobFilter(tags=("tenant-acme",)), reason="offboard"
            )
            assert result.cancel_requested == 1
            await asyncio.sleep(0.3)  # allow asyncpg NOTIFY delivery
        finally:
            await listen_conn.close()

        assert len(received) == 2  # one fleet-channel + one per-worker-channel payload
```

#### TDD — Green

Implement the SQL and wire the method. Run the integration tests: the `backend_pair` tests run against both backends (the `pg` param requires `@pytest.mark.integration`, enforced by the fixture guard); the `clean_jobs_app` tests run PG-only with direct pool access for dispatch simulation.

#### Acceptance criteria
- All red tests pass against both in-memory and Postgres backends
- `job_events` rows are inserted for cancelled jobs — both `state_change` (with actual `from_state`) and `cancel_request` for pending/scheduled; `cancel_request` only for running (event parity with single-job `write_cancel_request`)
- NOTIFY is sent for running jobs, batched into one statement (verified by `test_pg_cancel_where_notify_sent_for_running`, same listener pattern as `test_cancel_notify_integration.py`); `taskq.cancel.notify_sent` counter incremented per job
- Single SQL statement for the UPDATEs (with EPQ-safe duplicated predicates on the target table, `FROM prev` for `prev_status`); `executemany` for events within the same transaction via the shared `sql.insert_event` template
- `reason` JSON is serialized safely via `jsonb_param` (not f-string interpolation)
- `ORDER BY id` in the matching CTE for deterministic lock ordering
- Backend retries the transaction on `asyncpg.DeadlockDetectedError` (max 3 attempts, jittered backoff); NOTIFY fires only after a successful commit
- No clobbering of concurrently-claimed rows (verified by `test_pg_cancel_where_does_not_clobber_concurrent_claim`: the claimed job stays `running`, not `cancelled`)

---

### Task 6: Add `cancel_where` to `JobsClient` and `TaskQ`

**Goal:** Add the client-layer method with the empty-filter guardrail.

**Files:**
- MODIFY: `src/taskq/client/_jobs.py` — add `cancel_where`
- MODIFY: `src/taskq/client/_taskq.py` — add `cancel_where` delegate
- CREATE: `tests/test_cancel_where_client.py`

#### TDD — Red

```python
# tests/test_cancel_where_client.py

import pytest
from taskq.backend._protocol import JobFilter
from taskq.client._jobs import JobsClient
from taskq.exceptions import EmptyFilterError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.types import BulkCancelResult

from datetime import UTC, datetime

_NOW = datetime(2026, 1, 1, tzinfo=UTC)

async def test_client_cancel_where_with_tags() -> None:
    """JobsClient.cancel_where cancels jobs by tag filter."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    for _ in range(3):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    result = await client.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert isinstance(result, BulkCancelResult)
    assert result.cancelled_directly == 3

async def test_client_cancel_where_empty_filter_raises() -> None:
    """Empty filter (no predicates) raises EmptyFilterError."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    with pytest.raises(EmptyFilterError, match="filter predicate"):
        await client.cancel_where(JobFilter(), reason="oops")

async def test_client_cancel_where_empty_filter_override() -> None:
    """allow_empty_filter=True bypasses the guardrail."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))

    result = await client.cancel_where(
        JobFilter(),
        reason="drain all",
        allow_empty_filter=True,
    )

    assert result.cancelled_directly == 2

async def test_client_cancel_where_increments_counter() -> None:
    """cancel_where increments taskq.cancellation.requested once."""
    # Same OTel fixture pattern as test_jobs_client_cancel.py
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    await client.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="test",
    )

    # Counter should be 1 (one call, regardless of jobs affected)
    # ... OTel reader assertion ...

async def test_client_cancel_where_translates_schema_errors() -> None:
    """cancel_where wraps UndefinedTableError in SchemaNotMigratedError."""
    # ... same pattern as enqueue test ...
```

#### TDD — Green

```python
# In JobsClient:

async def cancel_where(
    self,
    filter: JobFilter,
    reason: str | None = None,
    *,
    allow_empty_filter: bool = False,
) -> BulkCancelResult:
    from taskq.exceptions import EmptyFilterError
    from taskq.obs import record_cancel_requested

    # Guardrail: reject empty filter
    if not allow_empty_filter:
        if (
            filter.queue is None
            and filter.status is None
            and filter.actor is None
            and filter.identity_key is None
            and filter.batch_id is None
            and (filter.tags is None or len(filter.tags) == 0)
            and filter.active is None
        ):
            raise EmptyFilterError()

    record_cancel_requested()

    with self._translate_schema_errors():
        return await self._backend.cancel_where(filter, reason)
```

#### Acceptance criteria
- Empty filter raises `EmptyFilterError` by default
- `allow_empty_filter=True` overrides the guardrail
- `taskq.cancellation.requested` counter incremented once per call
- `SchemaNotMigratedError` wrapping works (same pattern as other client methods)
- `TaskQ.cancel_where` delegates correctly (requires open client)
- Client stays thin: no `DeadlockDetectedError` handling here — deadlock retry is backend-owned (see Task 5 design note); the client must not import asyncpg for this

---

### Task 7: Add `tags` to `SubJobEnqueuer.enqueue()` with parent-tag inheritance

**Goal:** Add the `tags`, `inherit_tags`, `schedule_to_close`, `start_to_close`, and `heartbeat_timeout` parameters to `SubJobEnqueuer.enqueue()`.

**Files:**
- MODIFY: `src/taskq/client/_enqueuer.py` — add parameters, ContextVar, tag resolution logic
- MODIFY: `src/taskq/worker/_consumer.py` — set parent tags before actor invocation (gated by `sub_job_inherit_tags` setting)
- MODIFY: `src/taskq/worker/run.py` — set parent tags in stub consumer (unconditional — test harness)
- MODIFY: `src/taskq/settings.py` — add `sub_job_inherit_tags: bool = True` field to `WorkerSettings` (fleet-level kill switch; `TASKQ_SUB_JOB_INHERIT_TAGS` env var via pydantic-settings)
- CREATE: `tests/test_sub_job_tags.py`
- MODIFY: `tests/test_sub_job_enqueuer.py` — add tags tests

#### Worker-level kill switch (`sub_job_inherit_tags`)

`inherit_tags=True` as a default is a production behavior change: after upgrade, every existing sub-job enqueued inside an actor whose parent has tags becomes tag-findable — and via #54, tag-cancellable. A shared/utility sub-job enqueued by a tenant-tagged parent will now be swept up in that tenant's `cancel_where`. Per-call `inherit_tags=False` is not a practical rollback for a fleet.

The `sub_job_inherit_tags` worker setting (default `True`) provides a fleet-level opt-out. When set to `False`, the consumer does **not** call `set_parent_tags()` — the ContextVar remains at its `()` default, so `inherit_tags=True` on `enqueue()` produces `()` (identical to pre-upgrade behavior). This allows operators to disable inheritance across an entire worker fleet without code changes.

**Rollout guidance:**
1. Deploy with `sub_job_inherit_tags=False` (preserves existing behavior).
2. Verify no regressions in production.
3. Enable `sub_job_inherit_tags=True` per-queue or per-worker-group as confidence grows.
4. Document the blast-radius implication in `jobs-clients.md`: sub-jobs inherit parent tags → they are visible to `cancel_where` filters matching those tags.

#### Batch enqueue asymmetry (`enqueue_batch`)

`ctx.jobs.enqueue_batch` (via `EnqueueItem.tags`) does **not** inherit parent tags in this spec. This is a deliberate scoping decision for this iteration:

- `enqueue_batch` fans out N items, each potentially with its own `tags` field. Applying parent-tag inheritance per-item would require merging parent tags into each `EnqueueItem.tags` — a different code path (`batch.py`) than the single-enqueue path (`_enqueuer.py`).
- The primary use case for batch enqueue (fan-out chunks) already sets tags per `EnqueueItem` at call sites (e.g., cennan's `EnqueueItem(tags=...)` per sync-run/binding). These callers explicitly tag their batch items.
- Extending `inherit_tags` to `enqueue_batch` is a follow-up spec that can add a per-call `inherit_tags: bool` parameter to `enqueue_batch` and merge parent tags into each item's `tags` field. This is noted as a non-goal for this spec to keep the scope bounded.

**The asymmetry is documented** in Design Decisions (#57, decision 7) and in the updated `jobs-clients.md` guide so callers are aware that single `enqueue()` inherits by default while `enqueue_batch()` does not.

#### TDD — Red

```python
# tests/test_sub_job_tags.py

import pytest
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.client._enqueuer import SubJobEnqueuer, _parent_tags_var, set_parent_tags
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

class _Payload(BaseModel):
    value: str = "test"

class _Result(BaseModel):
    ok: bool = True

def _make_actor_ref(name: str = "child") -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()
    return ActorRef(
        name=name, queue="default", fn=_handler, wants_ctx=False,
        dependencies={}, payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=__import__("taskq.retry", fromlist=["RetryPolicy"]).RetryPolicy(),
        result_ttl=None, singleton=False, unique_for=None, max_pending=None,
    )

_NOW = datetime(2025, 1, 1, tzinfo=UTC)

def _make_enqueuer(backend: InMemoryBackend | None = None) -> SubJobEnqueuer:
    if backend is None:
        backend = InMemoryBackend(clock=FakeClock(_NOW))
    return SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # sentinel
        backend=backend,
        clock=FakeClock(_NOW),
    )

class TestSubJobExplicitTags:
    async def test_explicit_tags_no_inheritance(self) -> None:
        """tags= with inherit_tags=False sets only explicit tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            tags=["alpha", "beta"],
            inherit_tags=False,
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("alpha", "beta")

    async def test_tags_validated(self) -> None:
        """Invalid tags raise ValueError."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        with pytest.raises(ValueError, match="invalid tag"):
            await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                tags=["ab"],  # too short
            )

    async def test_tags_deduplicated(self) -> None:
        """Duplicate tags are deduplicated."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            tags=["alpha", "alpha", "beta"],
            inherit_tags=False,
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("alpha", "beta")

class TestSubJobTagInheritance:
    async def test_inherit_parent_tags_default(self) -> None:
        """With no explicit tags and default inherit_tags=True, sub-job inherits parent tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        # Set parent tags (simulating consumer setting them before actor invocation)
        token = set_parent_tags(("run-001", "tenant-acme"))
        try:
            handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("run-001", "tenant-acme")

    async def test_inherit_and_merge_tags(self) -> None:
        """Explicit tags merge with parent tags (parent first, deduped)."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001", "tenant-acme"))
        try:
            handle = await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                tags=["stage-2", "tenant-acme"],  # tenant-acme is a dup
            )
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("run-001", "tenant-acme", "stage-2")

    async def test_no_parent_tags_no_explicit_tags(self) -> None:
        """With no parent tags and no explicit tags, sub-job has empty tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        # No set_parent_tags call — ContextVar default is ()
        handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ()

    async def test_inherit_false_no_parent_tags(self) -> None:
        """inherit_tags=False with no explicit tags → empty tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001",))
        try:
            handle = await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                inherit_tags=False,
            )
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ()

    async def test_inherit_false_with_explicit_tags(self) -> None:
        """inherit_tags=False with explicit tags → only explicit tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001",))
        try:
            handle = await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                tags=["custom-tag"],
                inherit_tags=False,
            )
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("custom-tag",)

class TestSubJobMissingFields:
    async def test_schedule_to_close(self) -> None:
        """schedule_to_close is accepted and stored on the row."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        deadline = _NOW + timedelta(hours=1)
        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            schedule_to_close=deadline,
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.schedule_to_close == deadline

    async def test_start_to_close(self) -> None:
        """start_to_close is accepted and stored on the row."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            start_to_close=timedelta(minutes=30),
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.start_to_close == timedelta(minutes=30)

    async def test_heartbeat_timeout(self) -> None:
        """heartbeat_timeout is accepted and stored on the row."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            heartbeat_timeout=timedelta(seconds=10),
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.heartbeat_timeout == timedelta(seconds=10)

class TestContextVarIsolation:
    async def test_concurrent_jobs_separate_parent_tags(self) -> None:
        """ContextVar ensures concurrent consumers don't share parent tags."""
        import asyncio
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        async def enqueue_with_parent(parent_tags: tuple[str, ...]) -> UUID:
            token = set_parent_tags(parent_tags)
            try:
                handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
                return handle.job_id
            finally:
                _parent_tags_var.reset(token)

        # Run two "jobs" concurrently with different parent tags
        id1, id2 = await asyncio.gather(
            enqueue_with_parent(("run-a",)),
            enqueue_with_parent(("run-b",)),
        )

        row1 = await backend.get(JobId(id1))
        row2 = await backend.get(JobId(id2))
        assert row1.tags == ("run-a",)
        assert row2.tags == ("run-b",)
```

#### TDD — Green

1. Add `contextvars.ContextVar` and `set_parent_tags()` to `_enqueuer.py`
2. Add `tags`, `inherit_tags`, `schedule_to_close`, `start_to_close`, `heartbeat_timeout` to `enqueue()`
3. Add `_resolve_tags()` method
4. Pass the new parameters to `build_enqueue_args()`
5. In `_consumer.py`, call `set_parent_tags(tuple(job.tags))` before constructing `JobContext`
6. Reset the ContextVar after the actor completes (in a `finally` block)

#### Acceptance criteria
- All red tests pass
- Default behavior: `inherit_tags=True` — sub-job inherits parent tags when no explicit tags
- Explicit tags merge with parent tags (union, parent-first, deduped)
- `inherit_tags=False` disables inheritance
- `schedule_to_close`, `start_to_close`, `heartbeat_timeout` are passed through to `build_enqueue_args`
- ContextVar isolation works — concurrent consumers don't cross-contaminate parent tags
- All existing `test_sub_job_enqueuer.py` tests still pass (backward compatible — default `tags=None` with no parent tags → `()`)

---

### Task 8: Backward compatibility — default behavior unchanged

**Goal:** Verify that existing code that doesn't use tags or `inherit_tags` sees no behavior change.

**Files:**
- MODIFY: `tests/test_sub_job_enqueuer.py` — add backward compat tests

#### TDD — Red

```python
# tests/test_sub_job_enqueuer.py — add:

class TestBackwardCompatibility:
    async def test_no_tags_no_parent_tags_empty(self) -> None:
        """Existing code with no tags and no parent context → empty tags."""
        # No set_parent_tags call → ContextVar default is ()
        enqueuer = _make_enqueuer()
        handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
        row = await enqueuer._backend.get(handle.job_id)
        assert row.tags == ()

    async def test_existing_enqueue_no_tags_param(self) -> None:
        """Calling enqueue without tags= still works (backward compat)."""
        enqueuer = _make_enqueuer()
        handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
        assert handle is not None
        assert handle.job_id is not None
```

#### Acceptance criteria
- All existing sub-job enqueuer tests pass without modification
- Existing code that doesn't set parent tags or pass `tags=` gets `tags=()` (same as before)
- No new required parameters — all additions have defaults

---

### Task 9: E2E tests — sub-job tags in a real pipeline

**Goal:** Verify that sub-jobs enqueued from inside actor bodies are tagged and findable by `JobFilter(tags=...)` in a real worker container.

**Files:**
- MODIFY: `tests/e2e/actors.py` — add a tagged pipeline actor
- CREATE: `tests/e2e/test_sub_job_tags.py`

#### E2E actors

```python
# tests/e2e/actors.py — add:

class PipelineStagePayload(BaseModel):
    run_id: str
    stage: int
    total_stages: int = 3

@actor(name="pipeline_stage", queue="e2e")
async def pipeline_stage(
    payload: PipelineStagePayload,
    ctx: JobContext[PipelineStagePayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Linear pipeline: each stage enqueues the next via ctx.jobs.enqueue()."""
    await _record_effect(pool, ctx, "stage", {
        "run_id": payload.run_id,
        "stage": payload.stage,
    })
    await asyncio.sleep(0.05)

    if payload.stage < payload.total_stages:
        # Enqueue next stage — inherits parent tags by default
        await ctx.jobs.enqueue(
            pipeline_stage,
            PipelineStagePayload(
                run_id=payload.run_id,
                stage=payload.stage + 1,
                total_stages=payload.total_stages,
            ),
        )
```

#### E2E test

```python
# tests/e2e/test_sub_job_tags.py

from __future__ import annotations
from typing import TYPE_CHECKING
import pytest
from taskq.backend._protocol import JobFilter
from ._assertions import wait_for_effects, poll_until
from .actors import (
    PipelineStagePayload,
    TaggedPipelineStagePayload,
    pipeline_stage,
    tagged_pipeline_stage,
)

if TYPE_CHECKING:
    import asyncpg
    from taskq import TaskQ
    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

async def test_sub_job_inherits_tags_in_pipeline(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Sub-jobs enqueued via ctx.jobs.enqueue() inherit parent tags.

    A pipeline of 3 stages is enqueued with tag "run-{run_id}".
    Each stage enqueues the next via ctx.jobs.enqueue() with no
    explicit tags. All 3 jobs should be findable by the tag filter.
    """
    tag = f"run-{run_id[:8]}"
    handle = await e2e_client.enqueue(
        pipeline_stage,
        PipelineStagePayload(run_id=run_id, stage=1, total_stages=3),
        tags=[tag],
    )

    # Wait for all 3 stages to complete
    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="stage",
        min_count=3,
        timeout=30,
    )

    # All 3 jobs should be findable by the tag
    page = await e2e_client.list(JobFilter(tags=(tag,)))
    assert len(page.jobs) == 3, (
        f"Expected 3 jobs with tag {tag!r}, found {len(page.jobs)}: "
        f"{[j.id for j in page.jobs]}"
    )
```

#### E2E actors (additional for merge test)

```python
# tests/e2e/actors.py — add:

class TaggedPipelineStagePayload(BaseModel):
    run_id: str
    stage: int
    total_stages: int = 3

@actor(name="tagged_pipeline_stage", queue="e2e")
async def tagged_pipeline_stage(
    payload: TaggedPipelineStagePayload,
    ctx: JobContext[TaggedPipelineStagePayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Pipeline that passes explicit tags to sub-job enqueue."""
    await _record_effect(pool, ctx, "stage", {
        "run_id": payload.run_id,
        "stage": payload.stage,
    })
    await asyncio.sleep(0.05)

    if payload.stage < payload.total_stages:
        # Explicit per-stage tag — merges with the inherited parent tag
        await ctx.jobs.enqueue(
            tagged_pipeline_stage,
            TaggedPipelineStagePayload(
                run_id=payload.run_id,
                stage=payload.stage + 1,
                total_stages=payload.total_stages,
            ),
            tags=[f"stage-{payload.stage + 1}"],
        )
```

#### E2E test (merge)

```python
async def test_sub_job_explicit_tags_merge_with_parent(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Explicit tags on sub-job merge with inherited parent tags.

    Stage 1 is enqueued with parent tag "run-{run_id}". Each stage
    enqueues the next with an explicit stage tag (e.g. "stage-2").
    The sub-job should carry both the inherited run tag and the
    explicit stage tag.
    """
    parent_tag = f"run-{run_id[:8]}"

    handle = await e2e_client.enqueue(
        tagged_pipeline_stage,
        TaggedPipelineStagePayload(run_id=run_id, stage=1, total_stages=3),
        tags=[parent_tag],
    )

    # Wait for all 3 stages to complete
    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="stage",
        min_count=3,
        timeout=30,
    )

    # All 3 jobs should have the parent tag
    page = await e2e_client.list(JobFilter(tags=(parent_tag,)))
    assert len(page.jobs) == 3, (
        f"Expected 3 jobs with parent tag {parent_tag!r}, found {len(page.jobs)}"
    )

    # Stage 2 should also have the "stage-2" explicit tag
    stage2 = await e2e_client.list(JobFilter(tags=("stage-2",)))
    assert len(stage2.jobs) == 1, (
        f"Expected 1 job with stage-2 tag, found {len(stage2.jobs)}"
    )
    # Verify it also has the parent tag (merged)
    assert parent_tag in stage2.jobs[0].tags
    assert "stage-2" in stage2.jobs[0].tags
```

#### Acceptance criteria
- Sub-jobs enqueued from actor bodies are findable by the parent's tag
- All pipeline stages share the run tag
- E2E test passes against real Postgres + worker container

---

### Task 10: E2E tests — bulk cancel by filter

**Goal:** Verify `cancel_where` works end-to-end against real Postgres + worker.

**Files:**
- MODIFY: `tests/e2e/actors.py` — add bulk-cancel test actors if needed
- CREATE: `tests/e2e/test_cancel_where.py`

#### E2E test

```python
# tests/e2e/test_cancel_where.py

from __future__ import annotations
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
import pytest
from taskq.backend._protocol import JobFilter
from taskq.types import BulkCancelResult
from ._assertions import poll_until, wait_for_handle_status
from .actors import GenerateReportPayload, generate_report

if TYPE_CHECKING:
    import asyncpg
    from taskq import TaskQ
    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

async def test_cancel_where_pending_jobs_by_tag(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where cancels all pending jobs matching a tag filter."""
    tag = f"tenant-{run_id[:8]}"

    # Enqueue 5 jobs with the tag, scheduled far in the future (won't dispatch)
    future = datetime.now(UTC) + timedelta(seconds=120)
    for i in range(5):
        await e2e_client.enqueue(
            generate_report,
            GenerateReportPayload(run_id=run_id, report_id=f"r-{i}"),
            scheduled_at=future,
            tags=[tag],
        )

    # Enqueue 2 jobs without the tag (should not be affected)
    for i in range(2):
        await e2e_client.enqueue(
            generate_report,
            GenerateReportPayload(run_id=f"other-{run_id}", report_id=f"o-{i}"),
            scheduled_at=future,
        )

    result = await e2e_client.cancel_where(
        JobFilter(tags=(tag,)),
        reason="tenant offboarded",
    )

    assert result.cancelled_directly == 5
    assert result.cancel_requested == 0
    assert result.total_affected == 5

    # Verify via list: tagged jobs are cancelled, untagged are still scheduled
    tagged = await e2e_client.list(JobFilter(tags=(tag,), status="cancelled"))
    assert len(tagged.jobs) == 5

    untagged = await e2e_client.list(
        JobFilter(status="scheduled", queue="e2e")
    )
    assert len(untagged.jobs) == 2

async def test_cancel_where_running_jobs_cooperative(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where sets cooperative cancel for running jobs."""
    tag = f"run-{run_id[:8]}"

    # Enqueue a long-running job
    handle = await e2e_client.enqueue(
        generate_report,
        GenerateReportPayload(
            run_id=run_id,
            report_id=f"r-{run_id[:8]}",
            stages=4,
            stage_latency_ms=2000,
        ),
        tags=[tag],
    )
    await wait_for_handle_status(handle, "running", timeout=30)

    result = await e2e_client.cancel_where(
        JobFilter(tags=(tag,), status="running"),
        reason="abort run",
    )

    assert result.cancel_requested >= 1
    assert result.cancelled_directly == 0

    # The running job should eventually reach 'cancelled'
    await wait_for_handle_status(handle, "cancelled", timeout=30)

async def test_cancel_where_empty_filter_raises(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
) -> None:
    """Empty filter is rejected even in e2e."""
    from taskq.exceptions import EmptyFilterError

    with pytest.raises(EmptyFilterError):
        await e2e_client.cancel_where(JobFilter(), reason="oops")

async def test_cancel_where_batch_id_filter(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where works with batch_id filter."""
    from uuid import uuid4
    from taskq.batch import EnqueueItem
    from .actors import ImportContactsChunkPayload, import_contacts_chunk

    future = datetime.now(UTC) + timedelta(seconds=120)
    bid = uuid4()

    items = [
        EnqueueItem(
            actor_ref=import_contacts_chunk,
            payload=ImportContactsChunkPayload(
                run_id=run_id, upload_id=str(bid), chunk_id=i,
                start_row=i*100, end_row=(i+1)*100,
            ),
            scheduled_at=future,
        )
        for i in range(5)
    ]
    batch = await e2e_client.enqueue_batch(items, batch_id=bid)

    result = await e2e_client.cancel_where(
        JobFilter(batch_id=bid),
        reason="batch abort",
    )

    assert result.cancelled_directly == 5
```

#### Acceptance criteria
- Bulk cancel works against real Postgres with a real worker container
- Pending/scheduled jobs go straight to `cancelled`
- Running jobs get cooperative cancel and eventually reach `cancelled`
- Batch_id filter works end-to-end
- Empty filter guardrail fires in e2e context

---

### Task 11: Update documentation

**Goal:** Update the user-facing docs to reflect the new APIs.

**Files:**
- MODIFY: `docs/guides/jobs-clients.md` — document `cancel_where`, sub-job `tags`, `inherit_tags`
- MODIFY: `docs/architecture.md` — document bulk cancel in the cancel protocol section

#### Key doc additions

1. **SubJobEnqueuer.enqueue()** — update the signature to show `tags`, `inherit_tags`, `schedule_to_close`, `start_to_close`, `heartbeat_timeout`. Document the inheritance semantics table. Update the existing exclusion list at `jobs-clients.md:789` ("No `schedule_to_close`, `start_to_close`, or `heartbeat_timeout`") to reflect that these are now accepted on the sub-job path — this is a deliberate reversal of the documented exclusion; call it out in the guide, including the snooze/`schedule_to_close` warning (see Design Decisions #57-4). Document the `sub_job_inherit_tags` worker setting and the blast-radius implication (sub-jobs inherit parent tags → visible to `cancel_where`). Document that `enqueue_batch` does **not** inherit parent tags (asymmetry noted). All doc examples must use tags valid under the existing charset (`^[\w][\w\-]+[\w]$` — hyphenated, e.g. `tenant-acme`, not `tenant:acme`).

2. **New `cancel_where` section** in jobs-clients.md:
   ````markdown
   ### `cancel_where()`

   ```python
   result = await client.cancel_where(
       JobFilter(tags=("tenant-acme",), active=True),
       reason="tenant offboarded",
   )
   ```

   Cancel all jobs matching a filter in a single set-based operation...
   ````

3. **Architecture.md** — add `cancel_where` to the Backend protocol listing and the cancel protocol section.

---

## Test Coverage Requirements

### Unit tests (in-memory backend)
- `test_filter_sql.py` — filter→SQL builder extraction (6+ tests)
- `test_bulk_cancel_types.py` — BulkCancelResult, EmptyFilterError (4+ tests)
- `test_cancel_where.py` — in-memory bulk cancel (9+ tests, including limit-ignoring test)
- `test_cancel_where_client.py` — client-layer guardrail, counter, schema errors (5+ tests)
- `test_sub_job_tags.py` — sub-job tags, inheritance, ContextVar isolation (12+ tests)
- `test_sub_job_enqueuer.py` — backward compat additions (2+ tests)

### Integration tests (Postgres backend)
- `test_cancel_where_pg.py` — PostgresBackend bulk cancel (7 tests: pending, events-parity, reason-with-quotes, batch_id via `backend_pair`; running-cooperative, concurrent-claim EPQ race, and batched NOTIFY via `clean_jobs_app`)

### Protocol conformance
- `test_backend_protocol.py` — cancel_where method presence (1 test)

### E2E tests (real worker + PG)
- `test_sub_job_tags.py` — sub-job tag inheritance in a real pipeline (2+ tests)
- `test_cancel_where.py` — bulk cancel by tag/batch_id, cooperative cancel, guardrail (4+ tests)

### Coverage targets
- New code paths must achieve ≥95% line coverage
- Tag inheritance logic (`_resolve_tags`) must have 100% branch coverage
- Guardrail validation must have 100% branch coverage
- SQL builder must have 100% branch coverage for all filter combinations

---

## Backward Compatibility Analysis

### SubJobEnqueuer.enqueue() changes

| Change | Impact on existing code | Mitigation |
|---|---|---|
| New `tags=None` param | None — default is `None`, no behavior change when parent tags are also empty | None needed |
| New `inherit_tags=True` param | Sub-jobs now inherit parent tags by default. **Behavior change** when parent job has tags and code relies on sub-job tags being empty. Via #54, inherited tags make sub-jobs visible to `cancel_where` — a shared/utility sub-job enqueued by a tenant-tagged parent becomes tag-cancellable. | The ContextVar defaults to `()`, so if the consumer doesn't call `set_parent_tags()`, the behavior is identical to before. Only code that runs inside a real worker with the updated consumer will see inheritance. **Fleet-level kill switch:** `sub_job_inherit_tags` worker setting (default `True`) — when `False`, the consumer skips `set_parent_tags()`, preserving pre-upgrade behavior across the entire fleet. Deploy with `False` first, verify, then enable. |
| New `schedule_to_close=None` param | None — default is `None`, passes through to `build_enqueue_args` which already handles `None` | None needed |
| New `start_to_close=None` param | None — default is `None`, resolved to actor-declared value in `build_enqueue_args` | None needed |
| New `heartbeat_timeout=None` param | None — default is `None` | None needed |

**Key backward-compat guarantee:** The `contextvars.ContextVar` default is `()`. Existing tests that construct `SubJobEnqueuer` directly (without going through the consumer) will see `()` parent tags, so `inherit_tags=True` with no explicit tags produces `()` — identical to the current behavior. Only code that runs inside a real worker with the updated consumer will see tag inheritance. For production rollouts, the `sub_job_inherit_tags` worker setting (default `True`) can be set to `False` to disable inheritance fleet-wide without code changes.

### cancel_where addition

| Change | Impact on existing code | Mitigation |
|---|---|---|
| New `Backend.cancel_where` method | Custom `Backend` implementations lack this method | Method is purely additive; `AttributeError` is loud. Third-party backends must add the method to support bulk cancel. |
| New `JobsClient.cancel_where` method | None — new method, no existing code calls it | None needed |
| New `BulkCancelResult` type | None — new type | None needed |
| New `EmptyFilterError` exception | None — new type | None needed |
| Filter→SQL builder extraction | `_list_jobs` internals change | All existing `test_job_filter.py` and `test_postgres_reads.py` tests verify no regression |

### Protocol version

No `BACKEND_PROTOCOL_VERSION` bump required. The `cancel_where` method is purely additive — a v3 implementation lacks the method, and calling it raises `AttributeError` (a loud failure, not a silent misbehavior). See the bump rule in `_protocol.py` lines 79-84: "Purely additive changes an old implementation can ignore without producing incorrect behaviour do not require a bump."

---

## Downstream Consumer Impact Analysis

> **Methodology and framing:** The contract for this section is the downstream need documented in issues #54/#57 and in the downstream codebases' own comments — not the code those repos happen to run today. Each entry states the documented need, the verified current baseline (local checkouts were grepped; claims that did not hold up were corrected), the end-state this spec enables, and the migration required to get there.

### warden (~/src/warden) — Hybrid LLM proxy

**Documented need (from issue #54):** tenant-scoped job grouping at enqueue time and tenant offboarding / run abort as a single operation. #54's motivating example is exactly this shape: jobs tagged per tenant, offboarded via a paginate-and-cancel loop that is slow (one round trip per job) and racy (workers keep dispatching behind the cursor; new matching jobs land behind the cursor, so convergence needs an outer retry loop). The documented need — not warden's current code — is the contract here.

**How it uses TaskQ today (verified against the local checkout):**
- Enqueues jobs via `tq.enqueue` (e.g., `routes/admin.py:1438`, `routes/inference.py:1867,1996,2327`)
- Does **not** currently pass TaskQ `tags=` on any enqueue call (the `tags=` hits in `src/` are FastAPI route metadata, not TaskQ)
- Does **not** currently use `ctx.jobs` (sub-job enqueue) in `src/` — actor test harnesses construct `JobContext(jobs=None)` with a "never enqueues sub-jobs" comment (`app.py:217,252`)
- Does **not** currently call TaskQ `cancel()` in `src/` (only asyncio task cancels)

**End-state this spec enables:**
- **#57:** Once warden tags jobs per tenant at enqueue (`tags=["tenant-<id>"]` — hyphenated; the colon form in #54's prose is rejected by the current validator) and actors fan out via `ctx.jobs.enqueue()`, sub-jobs inherit tenant tags automatically and are findable by `JobFilter(tags=("tenant-<id>",))`.
- **#54:** `cancel_where(JobFilter(tags=("tenant-<id>",), active=True), reason="tenant offboarded")` replaces the paginate-and-cancel loop: one set-based write, no cursor race, counts/IDs returned for observability. Convergence for post-snapshot enqueues is the caller's job (stop producers, or repeat the call — see the snapshot-boundary contract).

**Migration path:**
- No change required to existing enqueue calls (nothing is tagged today; `sub_job_inherit_tags` has no observable effect until sub-jobs exist)
- Adopt tags: pass `tags=["tenant-<id>"]` at the `tq.enqueue` call sites
- Adopt bulk cancel: new offboarding flows call `cancel_where` instead of per-job loops

### cennan (~/src/cennan) — Enterprise knowledge management

**How it uses TaskQ today:**
- Ingestion pipeline: list → fetch → extract → chunk → embed → store
- Tags jobs per sync-run and per binding via `EnqueueItem.tags` in batch enqueue (`cennan/api/enqueue.py:299,519,565,574,604`), built by `_job_tags()` (`enqueue.py:240-252`) as hyphenated `sync-{sync_run_id}` / `binding-{binding_id}` — the same function documents a production incident where colon-form tags (`sync:{id}`) raised `ValueError` at enqueue and 500'd every sync trigger
- Pipeline stages use `ctx.jobs` chains (`pipeline/actors.py`); `pipeline/models.py:4-5` carries a comment documenting the exact #57 limitation ("`ctx.jobs` … does not accept `tags` in the installed TaskQ version"), which is why payloads re-carry `sync_run_id`/`binding_id`
- Needs to stop runaway ingestion runs

**What this spec enables:**
- **#57:** Pipeline stages enqueued via `ctx.jobs.enqueue()` will inherit the `sync-*`/`binding-*` tags. The full pipeline is findable by tag, not just the batch-fan-out chunks. The `pipeline/models.py` workaround comment can be deleted; payload ids stay (they feed counters/liveness, not just discovery).
- **#54:** `cancel_where(JobFilter(tags=("sync-<run-id>",), active=True))` stops a runaway ingestion run in one call. Pending/scheduled stages go straight to cancelled; running stages get cooperative cancel.

**Migration path:**
- No code change required for tag inheritance (automatic once workers are upgraded); keep `sub_job_inherit_tags` at its default `True`
- Delete the `pipeline/models.py` limitation comment; drop any secondary lookup paths maintained only because sub-jobs were untaggable
- Replace manual cancel loops with `cancel_where`
- Can now tag sub-jobs with stage-specific tags (e.g., `tags=["stage-embed"]`) that merge with inherited sync/binding tags

### aacrtool (~/src/aacrtool) — Agentic code review tool

**Baseline (verified):** TaskQ is present only in `.venv` (dependency declared); **no TaskQ usage exists in `src/` yet**. Everything below is planned usage, not a description of current code.

**End-state this spec enables (when TaskQ is adopted):**
- Review jobs tagged per repo at enqueue (`tags=["repo-<org>-<name>"]` — hyphenated per the existing tag charset).
- **#57:** If review actors fan out sub-jobs (e.g., per-file analysis), those sub-jobs inherit the repo tag automatically.
- **#54:** `cancel_where(JobFilter(tags=("repo-<org>-<name>",), active=True))` aborts a review run in one call.

**Migration path:**
- N/A — TaskQ adoption is future work; adopt `cancel_where` and sub-job tags from day one rather than building paginate-and-cancel loops.

---

## Design Decisions Summary

### #57: Sub-job tags

1. **Inheritance default: `True`** — sub-jobs inherit parent tags by default because the primary use case (run/tenant correlation) requires sub-jobs to be findable by the same tags as the parent. Opting out with `inherit_tags=False` is available for cases where sub-jobs should be untagged or only carry explicit tags. A worker-level `sub_job_inherit_tags` setting (default `True`) provides a fleet-level kill switch for operators who need to roll out the behavior change gradually.

2. **Merge semantics: union, parent-first** — when both parent tags and explicit tags are provided, the union preserves parent tags first, then adds new tags. This lets callers add stage-specific tags while keeping the run/tenant correlation tag. Deduplication follows the same `_validate_and_dedup_tags` logic.

3. **Propagation via `contextvars.ContextVar`** — the `SubJobEnqueuer` is shared across concurrent consumers in the same event loop, so a per-instance field would be racy. `ContextVar` is the asyncio-native solution: each Task gets its own context copy, so concurrent consumers each see their own parent tags. This is the same mechanism Python uses for `contextvars.copy_context()` in `asyncio.Task`.

4. **Also add `schedule_to_close`, `start_to_close`, `heartbeat_timeout`** — these are passed through to `build_enqueue_args`, which already handles them. **This reverses a documented deliberate exclusion:** `docs/guides/jobs-clients.md:789` currently lists "No `schedule_to_close`, `start_to_close`, or `heartbeat_timeout` (set on the actor declaration)" as an intentional constraint of the sub-job surface. Issue #57 asks whether this is deliberate or an omission, and floats the hypothesis that "'you can't set it from inside an actor' may be a feature rather than an omission." This spec takes the position that the parameters should be available — the client and sub-job surfaces should not drift without a reason, and the issue's concrete cost ("a sub-job that needs a different timeout than its actor's declared default … has to be enqueued from outside the actor") is real — but acknowledges the trade-off:

    **Snooze/finalizer hazard (analyzed, not ignored):** Issue #57 names the hazard directly — "A finalizer that snoozes on `wait_for_batch` for a long time would be killed by one." The verified mechanics: (i) `mark_snoozed` already guards the deadline at snooze time — a running job whose requested snooze delay would cross `schedule_to_close` is failed immediately with `error_class='DeadlineExceeded'` ("schedule_to_close reached before next dispatch", `_sql_templates.py:254-275`), rather than being parked past its deadline; (ii) `sweep_deadline_exceeded` (`_sweeps.py:325+`) fails pending/scheduled jobs whose `schedule_to_close` has passed, which includes snoozed jobs (a snooze returns the row to `scheduled`). So a sub-job carrying a tight caller-supplied `schedule_to_close` fails **deterministically and loudly** — at the snooze attempt or at the deadline — never silently mid-snooze. That is precisely what a wall-clock deadline means; the documented exclusion (a) prevented actor code from opting into it. Reversing the exclusion means the hazard is opt-in per call. **Mitigations:** (a) the default is `None` — no override, so `build_enqueue_args` falls back to the actor's retry time budget exactly as today, and the hazard only manifests when a caller explicitly passes `schedule_to_close`; (b) the docs update (Task 11) must warn that `schedule_to_close` bounds total wall-clock time *including* time snoozed on `wait_for_batch`, so finalizer-style sub-jobs should set it generously or not at all; (c) a future spec could add a `snooze_extends_deadline` flag to make the interaction explicit — out of scope here.

5. **No `queue` override** — sub-jobs use the actor's declared queue. This is a documented design choice (`docs/guides/jobs-clients.md:788`) and is not changed by this spec.

6. **`idempotency_key` type: keep `IdempotencyKey | str | None`** — the sub-job enqueuer's wider type (accepting bare `str`) is more ergonomic for actor code. The `JobsClient` uses `IdempotencyKey | None` (the narrower `NewType`); the sub-job enqueuer keeps its wider type. `build_enqueue_args` already handles both.

7. **Batch enqueue asymmetry** — `ctx.jobs.enqueue_batch` does **not** inherit parent tags in this spec. This is a deliberate scoping decision: `enqueue_batch` uses a different code path (`batch.py`) with per-item `EnqueueItem.tags`, and extending inheritance to batch would require merging parent tags into each item. The asymmetry is documented in the updated `jobs-clients.md` so callers are aware. A follow-up spec can add `inherit_tags` to `enqueue_batch` if needed.

### #54: Bulk cancel by filter

1. **Pending/scheduled → terminal `cancelled`** — these jobs have no running actor to cooperate with. The existing `write_cancel_request` already does this for single jobs; bulk cancel follows the same pattern. The issue asks: "should matching rows in pending/scheduled go straight to terminal cancelled?" — yes, they should, for consistency with the existing single-cancel path.

2. **Running → cooperative `cancel_phase=1`** — running jobs have an actor executing. The cooperative path sets `cancel_phase=1`, which the worker's heartbeat-driven `CancelController` observes and sets the in-process `cancel_event`. The actor checks `ctx.check_cancelled()` at its next stage boundary. This is the existing phase-1 cooperative cancel, just applied in bulk.

3. **Guardrail: empty filter rejected** — a `JobFilter` with all defaults matches every job in the table. `EmptyFilterError` is raised unless `allow_empty_filter=True` is explicitly passed. This prevents accidental full-table cancels while allowing intentional "cancel everything" operations. Edge case, documented rather than handled: `JobFilter(status=[])` passes the guardrail (`status` is not `None`) but matches no jobs (an empty status sequence renders `status = ANY('{}')`) — the call is a benign no-op returning zero counts.

4. **Single SQL statement for the UPDATEs (with EPQ-safe predicates)** — the two UPDATEs (pending/scheduled → cancelled, running → cancel_phase=1) are in a single CTE-based statement within a single transaction. **Status/cancel_phase predicates are duplicated in each UPDATE's own WHERE clause** (not just in the `matching` CTE) so that EvalPlanQual re-evaluates them against concurrently-modified rows. Events are inserted via `executemany` in the same transaction. NOTIFY is sent after commit (same pattern as `write_cancel_request`, batched). `ORDER BY id` in the matching CTE reduces deadlock probability; the **backend** retries the transaction on `asyncpg.DeadlockDetectedError` (max 3 attempts, jittered backoff — backend-owned because the exception type is asyncpg-specific and the backend owns the transaction boundary; the client stays backend-agnostic).

5. **Filter reuse: `JobFilter`** — the same `JobFilter` used by `list_jobs` is used by `cancel_where`. The `limit`, `cursor`, and `order_by` fields are ignored (bulk cancel is not paginated). The `build_filter_conditions` helper (without `schema` parameter — conditions are schema-less fragments) ensures filter semantics are identical between query and mutation. `_list_jobs` regains cursor/limit/order_by handling by appending them after the shared builder call.

6. **Returns `BulkCancelResult` with counts and IDs** — the counts let callers verify the operation affected the expected number of jobs. The IDs enable observability and follow-up operations (e.g., waiting for cooperative cancels to complete).

7. **Counter: `taskq.cancellation.requested` incremented once per call** — not once per job. This matches the existing `cancel()` semantics (one increment per API call) and avoids counter inflation for bulk operations. The `taskq.cancel.notify_sent` counter is incremented per job notified (matching the single-job path).

8. **Event parity with single-job path** — both backends insert the same event kinds as `write_cancel_request`: for pending/scheduled, both `state_change` (with actual `from_state`) and `cancel_request`; for running, only `cancel_request`. This ensures `job_events` consumers (audit, reclaim tooling) see consistent event streams regardless of backend or bulk-vs-single path.

9. **No protocol version bump** — `cancel_where` is purely additive. A v3 backend implementation lacks the method, and the client raises `AttributeError` loudly. See the bump rule in `_protocol.py` lines 79-84.

10. **Post-snapshot enqueue boundary** — jobs matching the filter that are enqueued *after* the statement's snapshot escape the cancel. Convergence is the caller's responsibility: stop producers before calling `cancel_where`, or issue a second call to catch stragglers. The `BulkCancelResult` counts let the caller detect non-convergence.

---

## Revision log

### 2026-07-29 — Post-review revision (verdict: NEEDS REWORK — 1 Critical / 4 High / 8 Medium / 7 Low)

Revised against `.review/spec-review.md` under the standing 1.0.0 design directive: breaking changes are allowed when the result is strictly better, but no hacks, shims, or dual-path compat code; downstream sections describe the documented needs (issues #54/#57 and downstream code comments) and the correct end-state, not preservation of current usage.

Resolved:

- **C1 (EPQ race):** bulk-cancel SQL redesigned — status/`cancel_phase` predicates duplicated on the target table in each UPDATE's own WHERE clause (EvalPlanQual re-evaluates only the UPDATE's WHERE, not CTE contents); `cancelled` CTE uses the `FROM prev` pattern from `cancel_pending_scheduled` (`_sql_templates.py:407-415`) to carry the real `prev_status`; residual claim-boundary window documented (job escapes the call, never clobbered). New PG race test `test_pg_cancel_where_does_not_clobber_concurrent_claim` pins the safety property.
- **H1 (circular import):** `BulkCancelResult` defined in `_protocol.py` (next to `ScheduleRecord`), re-exported via `types.py` → `client/__init__.py` → `__init__.py` (same chain as `CancelResult`); stale "pydantic-free" docstring flagged for update in Task 2.
- **H2 (JSON injection):** `reason` serialized in Python via `jsonb_param`, bound as jsonb — never f-string interpolation; pinned by `test_pg_cancel_where_reason_with_quotes`.
- **H3 (silent limit cap):** in-memory `_cancel_where` sanitizes the filter (`limit=2**31`, `cursor=None`, `order_by=None`) before reusing `_list_jobs`; pinned by `test_cancel_where_ignores_filter_limit` (11 jobs, `limit=5` → all 11 cancelled).
- **H4 (fabricated justification):** removed the misquoted "confirmed by the issue author"; Design Decision #57-4 now explicitly reverses the documented exclusion (`jobs-clients.md:789`), analyzes the finalizer-snooze hazard against the verified mechanism (snooze-time `DeadlineExceeded` guard at `_sql_templates.py:254-275`; `sweep_deadline_exceeded` at `_sweeps.py:325+`), and scopes the hazard as opt-in per call.
- **Medium:** M1 event parity specified for both backends; M2 protocol member-count/docstring updates planned (36→37, 33→34); M3 fleet kill switch `sub_job_inherit_tags` on `WorkerSettings` (`settings.py`, not `_bootstrap.py`); M4 batch asymmetry justified as deliberate scoping (Decision #57-7); M5 all examples converted to the valid hyphenated tag charset, with the colon-form explicitly declared a non-goal; M6 deadlock retry (backend-owned, max 3 attempts) + `ORDER BY id` lock ordering + tenant-scale partitioning guidance; M7 downstream section rewritten per the directive (warden corrected to verified "today" + documented-need framing; cennan verified incl. the colon-tag incident; aacrtool marked planned); M8 builder contract states `_list_jobs` re-appends cursor/limit after the shared call.
- **Low:** L1 stale CTE rationale removed; L2 file-structure/task-list mismatches fixed (`_filter_sql.py`, new test files listed; `context.py` note; no `SqlTemplates.cancel_where` detour); L3 Task 9 placeholder replaced with a real merge test (`tagged_pipeline_stage`); L4 batched-NOTIFY statement written + per-job counter parity + listener-based PG test (`test_pg_cancel_where_notify_sent_for_running`); L5 actual `from_state` via `FROM prev`; L6 snapshot-boundary contract documented (docstring + Decision #54-10); L7 `build_filter_conditions(filter)` without the unused `schema` param.

Design changes chosen under the directive (all breaking-or-behavioral by intent, no shims): deadlock retry lives in the backend (not the client) because the exception is asyncpg-specific and the backend owns the transaction boundary; NOTIFY send lives in `postgres.py` where the `notify_sent` counter is defined (avoids a module cycle); `schedule_to_close`/`start_to_close`/`heartbeat_timeout` are added to the sub-job surface as a deliberate, documented reversal of the prior exclusion.

Left unresolved (deliberately): tag-charset widening (colon-form tags) — separate change, declared a non-goal; `inherit_tags` for `enqueue_batch` — follow-up spec (Decision #57-7); `snooze_extends_deadline` flag — noted as future work in Decision #57-4.
