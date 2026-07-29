# Spec: Worker option to exit when queues are drained

**Date:** 2026-07-29  
**Status:** Draft, revised post-review (2026-07-29)  
**Issue:** #53  
**Target:** Pre-1.0

---

## Goal

Add an `until_idle` mode to `worker_main()` and the `taskq worker` CLI command that drains all queued jobs through the normal graceful shutdown path and exits with a status code: `0` if every job succeeded, `2` if any job failed, `3` if a max-runtime cap was hit. The mode polls the backend for active (pending + scheduled + running) jobs in the worker's subscribed queues and, when the count stays zero for a configurable settle window, triggers the existing four-phase shutdown orchestration (DRAINING → CANCELLING → FORCING → ABANDONING) — the same path SIGTERM takes — so no new shutdown plumbing or signal hacks are needed. The only existing-file modification outside the new drain module is changing `dispatch_one_job` to return its `AttemptOutcome` (currently `None`) so `di_consumer_loop` can increment the failure counter.

## Non-goals

- **Removing or replacing `InMemoryBackend.run_until_drained()`.** That method remains a test-only helper for deterministic in-memory execution. `worker_main(until_idle=True)` is the production-grade, Postgres-compatible drain mechanism.
- **Making `run_until_drained()` work with Postgres.** The `until_idle` mode supersedes that need entirely.
- **Changing the default worker behaviour.** Without `until_idle=True` (or `--until-idle`), `worker_main()` runs forever exactly as today.
- **Cron-aware drain.** Cron schedules create recurring jobs indefinitely; `until_idle` mode is incompatible with cron-driven workloads and should not be combined with them. The spec adds a startup WARNING when both `until_idle` and cron schedules are active, but does not add detection or prevention beyond that — documentation covers it.
- **Multi-worker drain coordination.** If multiple workers consume the same queue, `until_idle` on one worker waits for that queue to drain across ALL workers (the count query is queue-scoped, not worker-scoped). This is correct for the finite-batch use case but not for "drain only this worker's jobs."
- **New exit codes for the non-idle path.** SIGTERM-driven shutdown continues to exit `0` as today. Only `until_idle` mode introduces non-zero exit codes for job failures and timeout.

---

## Architecture overview

### Current state

The worker's main loop (`_bootstrap.py:_main`) blocks on `await shutdown_event.wait()`. That event is set by either:
1. Signal handlers (SIGTERM/SIGINT) → `orchestrate_shutdown()` task
2. A sibling crash → `_make_sibling_spawner`'s `_guarded` wrapper

`orchestrate_shutdown()` runs the four-phase sequence (DRAINING → CANCELLING → FORCING → ABANDONING), sets `shutdown_event` in its `finally` block, and returns `0`. The `_main` function then awaits `orchestrator_holder[0]` for the exit code.

The producer loop already observes `producer_stop_event` alongside `shutdown_event` — when `producer_stop_event` is set (DRAINING phase), the producer stops dispatching but consumers continue processing in-flight jobs.

### Proposed change

Add a **drain monitor** — a new sibling coroutine spawned only when `until_idle=True`. The drain monitor:

1. Periodically polls the backend (`count_active_jobs(queues)`) for non-terminal jobs in the worker's subscribed queues.
2. Checks `deps.active_jobs.count()` for in-flight jobs on this worker.
3. When both are zero, starts a settle timer (`idle_settle_window`).
4. If still zero after the settle window, triggers graceful shutdown via `orchestrate_shutdown` with a drain-specific exit code. The monitor creates the orchestration task and appends it to `orchestrator_holder` but does **NOT** set `shutdown_event` — `orchestrate_shutdown`'s `finally` block sets it at the correct point (after all phase work completes), exactly as the SIGTERM signal handler does.
5. If `max_runtime` is set and exceeded, triggers shutdown with exit code `3`.

The drain monitor is spawned with `may_return=True` in the sibling spawner (it is the one legitimate sibling that returns cleanly to trigger shutdown, alongside the NOTIFY listener fallback).

### File structure

```
src/taskq/worker/
├── drain.py              # NEW — drain monitor loop + exit code logic
├── _bootstrap.py         # MODIFIED — accept until_idle params, spawn drain monitor;
│                        #           worker_main signature extended (defined here, re-exported by run.py)
├── run.py                # MODIFIED — di_consumer_loop captures dispatch_one_job outcome,
│                        #           increments drain_failures; re-exports worker_main
├── deps.py               # MODIFIED — add drain_failures counter
├── shutdown.py           # MODIFIED — add double-orchestration guard to _on_shutdown_signal (H2);
│                        #           orchestrate_shutdown itself unchanged
└── dispatch.py           # MODIFIED — dispatch_one_job returns AttemptOutcome (was -> None)

src/taskq/
├── cli.py                # MODIFIED — add --until-idle, --idle-settle-window, --idle-poll-interval, --max-runtime
├── settings.py           # MODIFIED — add idle_settle_window, idle_poll_interval, idle_max_runtime fields

src/taskq/backend/
├── _protocol.py          # MODIFIED — add count_active_jobs to Backend protocol
├── _sql_templates.py     # MODIFIED — add count_active_jobs SQL template
├── _reads.py             # MODIFIED — add _count_active_jobs helper
├── postgres.py           # MODIFIED — add count_active_jobs method
└── statemachine.py       # UNCHANGED — ACTIVE_STATUSES reused (not hardcoded)

src/taskq/testing/
├── in_memory.py          # MODIFIED — add count_active_jobs to InMemoryBackend
└── actor.py              # MODIFIED — add count_active_jobs stub to FakeBackend

tests/
├── test_worker_drain.py           # NEW — unit tests for drain monitor
├── test_cli_worker.py             # MODIFIED — add --until-idle CLI tests
├── test_worker_main.py            # MODIFIED — add until_idle wiring tests
├── test_worker_settings_drain.py  # NEW — settings field tests
├── test_count_active_jobs.py      # NEW — unit tests for backend count_active_jobs
├── test_backend_protocol.py       # MODIFIED — update TestMethodCount (36→37 members, add name)
└── e2e/
    ├── worker_entry.py            # MODIFIED — TASKQ_UNTIL_IDLE env var → until_idle param
    └── test_until_idle.py         # NEW — e2e tests for drain mode with real PG
```

---

## API surface

### Settings additions (`src/taskq/settings.py`)

Three new fields on `WorkerSettings`:

```python
class WorkerSettings(TaskQSettings):
    # ... existing fields ...

    # ── Until-idle drain mode ────────────────────────────────────────
    idle_settle_window: float = Field(
        default=2.0,
        ge=0.0,
        description=(
            "TASKQ_IDLE_SETTLE_WINDOW (seconds). Time the drain monitor "
            "waits after queues appear empty before declaring drained. "
            "Handles race conditions where a producer enqueues between "
            "the check and the shutdown trigger. Only used when "
            "--until-idle is active."
        ),
    )
    idle_poll_interval: float = Field(
        default=1.0,
        ge=0.1,
        description=(
            "TASKQ_IDLE_POLL_INTERVAL (seconds). How often the drain "
            "monitor checks queue depth and active job count. Only used "
            "when --until-idle is active."
        ),
    )
    idle_max_runtime: float | None = Field(
        default=None,
        gt=0,
        description=(
            "TASKQ_IDLE_MAX_RUNTIME (seconds). Maximum wall-clock time "
            "for until-idle mode. When exceeded, the worker triggers "
            "graceful shutdown with exit code 3. None = no limit. "
            "Only used when --until-idle is active."
        ),
    )
```

### `worker_main()` additions (`src/taskq/worker/_bootstrap.py`)

> **Note:** `worker_main` is *defined* in `_bootstrap.py:930` and re-exported by `run.py`.
> The signature extension lives in `_bootstrap.py`; `run.py` only re-exports it.

```python
def worker_main(
    settings: WorkerSettings,
    *,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    di_registry: ProviderRegistry | None = None,
    cron_registry: list[CronScheduleSpec] | None = None,
    connections: WorkerConnections | None = None,
    until_idle: bool = False,
    idle_settle_window: float | None = None,
    idle_poll_interval: float | None = None,
    max_runtime: float | None = None,
) -> int:
    """Worker process entry point.

    When until_idle=True, the worker drains all jobs in its subscribed
    queues and exits:
      - exit 0: all jobs succeeded
      - exit 2: some jobs failed (dispatch outcome "failed" or
        "cancelled" — see WorkerDeps.drain_failures; "scheduled"
        snooze/retry outcomes do not count)
      - exit 3: max_runtime exceeded before drain completed

    idle_settle_window, idle_poll_interval, and max_runtime override
    the corresponding WorkerSettings fields when not None.
    """
```

### `_main()` additions (`src/taskq/worker/_bootstrap.py`)

```python
async def _main(
    settings: WorkerSettings,
    *,
    _local_queue_seed: list[JobRow] | None = None,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    _registry: ProviderRegistry | None = None,
    _cron_registry: list[CronScheduleSpec] | None = None,
    connections: WorkerConnections | None = None,
    until_idle: bool = False,
    idle_settle_window: float | None = None,
    idle_poll_interval: float | None = None,
    max_runtime: float | None = None,
) -> int:
```

### CLI additions (`src/taskq/cli.py`)

```python
@worker_app.callback(invoke_without_command=True)
def worker(
    actors: str = typer.Option(..., "--actors", ...),
    # ... existing options ...
    until_idle: bool = typer.Option(
        False,
        "--until-idle",
        help="Run until all subscribed queues are drained, then exit. "
        "Exit 0 if all jobs succeeded, 2 if any failed, 3 if max-runtime "
        "was exceeded. Incompatible with cron-driven workloads.",
    ),
    idle_settle_window: float | None = typer.Option(
        None,
        "--idle-settle-window",
        help="Seconds to wait after queues appear empty before declaring "
        "drained. Overrides TASKQ_IDLE_SETTLE_WINDOW. Default 2.0.",
    ),
    idle_poll_interval: float | None = typer.Option(
        None,
        "--idle-poll-interval",
        help="How often to check queue depth. Overrides "
        "TASKQ_IDLE_POLL_INTERVAL. Default 1.0.",
    ),
    max_runtime: float | None = typer.Option(
        None,
        "--max-runtime",
        help="Maximum wall-clock seconds before forcing exit (code 3). "
        "Overrides TASKQ_IDLE_MAX_RUNTIME. Only used with --until-idle.",
    ),
) -> None:
```

### Backend protocol addition (`src/taskq/backend/_protocol.py`)

```python
class Backend(Protocol):
    # ... existing methods ...

    async def count_active_jobs(self, queues: list[str]) -> int:
        """Count non-terminal jobs (pending, scheduled, running) in the given queues.

        Returns the total count across all specified queues. Used by the
        drain monitor to detect when queues are empty. An empty queues
        list returns 0.
        """
        ...
```

### WorkerDeps addition (`src/taskq/worker/deps.py`)

```python
@dataclass
class WorkerDeps:
    # ... existing fields ...
    drain_failures: int = 0
    """Count of jobs that reached a non-success terminal state during
    until-idle drain mode. Incremented by di_consumer_loop when
    dispatch_one_job returns 'failed' or 'cancelled' (the two
    AttemptOutcome values that indicate a terminal failure — not
    'scheduled', which means snooze/retry and is not a drain failure).
    Read by the drain monitor to determine the exit code. Always 0
    in non-idle mode (the counter is only read by the drain monitor)."""
```

### New module: drain monitor (`src/taskq/worker/drain.py`)

```python
"""Drain monitor for until-idle mode.

When spawned as a sibling in the worker's TaskGroup, the drain monitor
polls the backend for active jobs in the worker's subscribed queues.
When the count stays zero for the settle window (and no jobs are active
on this worker), the monitor triggers the normal graceful shutdown
via orchestrate_shutdown — the same path SIGTERM takes.

Exit codes:
  0 — all jobs succeeded (drain_failures == 0)
  2 — some jobs failed (drain_failures > 0)
  3 — max_runtime exceeded before drain completed
"""

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from taskq.worker.shutdown import ShutdownPhase, orchestrate_shutdown

if TYPE_CHECKING:
    from taskq.backend._protocol import Backend
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

_log = structlog.get_logger("taskq.worker.drain")

EXIT_DRAIN_CLEAN = 0
EXIT_DRAIN_WITH_FAILURES = 2
EXIT_DRAIN_TIMEOUT = 3


async def drain_monitor_loop(
    deps: "WorkerDeps",
    settings: "WorkerSettings",
    worker_id: UUID,
    shutdown_event: asyncio.Event,
    escalate_event: asyncio.Event,
    orchestrator_holder: list[asyncio.Task[int]],
    backend: "Backend",
    *,
    idle_settle_window: float,
    idle_poll_interval: float,
    max_runtime: float | None,
) -> None:
    """Monitor for queue drain and trigger graceful shutdown when idle.

    Spawns as a sibling in the worker's TaskGroup with may_return=True.
    Returns cleanly after creating the orchestrate_shutdown task.
    Does NOT set shutdown_event — orchestrate_shutdown's finally block
    sets it at the correct point (after all phase work completes),
    exactly as the SIGTERM signal handler does.
    """
    ...
```

---

## Implementation plan

### Task 1: Add `count_active_jobs` to Backend protocol and implementations

**Red-green TDD.** Write failing tests first, then implement.

> **Protocol version:** `BACKEND_PROTOCOL_VERSION` stays at **3**. The
> documented convention (`docs/architecture.md:143-149`) states bumps are
> required only when an old implementation would *silently* misbehave.
> An old implementation lacking `count_active_jobs` raises `AttributeError`
> on first call — a loud failure, not a silent one. This is a purely
> additive change; no bump is needed.

> **Expected test modifications (M1):** `TestMethodCount` in
> `tests/test_backend_protocol.py:214-260` pins exactly 36 public members
> and the full name set. Adding `count_active_jobs` makes it 37. Both
> `test_exactly_thirty_six_public_members` and
> `test_all_member_names_present` must be updated:
> - Change the count assertion from 36 to 37.
> - Add `"count_active_jobs"` to the `expected` set.
> These are the only existing test modifications outside the new test files.

#### 1a. Tests (RED)

**File:** `tests/test_count_active_jobs.py`

```python
"""Unit tests for Backend.count_active_jobs."""

from datetime import datetime, timezone

import pytest

from taskq.testing import InMemoryBackend, FakeClock, make_enqueue_args


async def test_count_active_jobs_empty_queues() -> None:
    """Empty queues list returns 0."""
    backend = InMemoryBackend(FakeClock())
    assert await backend.count_active_jobs([]) == 0


async def test_count_active_jobs_no_jobs() -> None:
    """Queues with no jobs returns 0."""
    backend = InMemoryBackend(FakeClock())
    assert await backend.count_active_jobs(["default"]) == 0


async def test_count_active_jobs_pending() -> None:
    """Pending jobs are counted."""
    backend = InMemoryBackend(FakeClock())
    await backend.enqueue(make_enqueue_args(queue="default"))
    await backend.enqueue(make_enqueue_args(queue="default"))
    assert await backend.count_active_jobs(["default"]) == 2


async def test_count_active_jobs_running() -> None:
    """Running jobs are counted."""
    from datetime import timedelta

    backend = InMemoryBackend(FakeClock())
    await backend.enqueue(make_enqueue_args(queue="default"))
    dispatched = await backend.dispatch_batch(
        backend._worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert await backend.count_active_jobs(["default"]) == 1


async def test_count_active_jobs_scheduled() -> None:
    """Scheduled (future) jobs are counted."""
    backend = InMemoryBackend(FakeClock())
    await backend.enqueue(
        make_enqueue_args(queue="default", scheduled_at=datetime(2030, 1, 1, tzinfo=timezone.utc))
    )
    assert await backend.count_active_jobs(["default"]) == 1


async def test_count_active_jobs_terminal_excluded() -> None:
    """Terminal jobs (succeeded, failed, etc.) are NOT counted."""
    from datetime import timedelta

    backend = InMemoryBackend(FakeClock())
    await backend.enqueue(make_enqueue_args(queue="default"))
    dispatched = await backend.dispatch_batch(
        backend._worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    await backend.mark_succeeded(dispatched[0].id, backend._worker_id, None)
    assert await backend.count_active_jobs(["default"]) == 0


async def test_count_active_jobs_multi_queue() -> None:
    """Counts across multiple queues."""
    backend = InMemoryBackend(FakeClock())
    await backend.enqueue(make_enqueue_args(queue="default"))
    await backend.enqueue(make_enqueue_args(queue="priority"))
    await backend.enqueue(make_enqueue_args(queue="other"))
    assert await backend.count_active_jobs(["default", "priority"]) == 2


async def test_count_active_jobs_queue_subset() -> None:
    """Only counts jobs in the specified queues."""
    backend = InMemoryBackend(FakeClock())
    await backend.enqueue(make_enqueue_args(queue="default"))
    await backend.enqueue(make_enqueue_args(queue="priority"))
    assert await backend.count_active_jobs(["default"]) == 1
```

**Integration test (with real PG):**

Uses the `clean_jobs_app` fixture (`src/taskq/testing/fixtures.py:848`),
which provides a `JobsApp` with a `PostgresBackend` against a freshly
migrated schema — the established pattern in
`tests/test_postgres_missing_methods.py:62-89`.

```python
"""Integration tests for PostgresBackend.count_active_jobs."""

import pytest

from taskq.testing import make_enqueue_args
from taskq.testing.fixtures import JobsApp


@pytest.mark.integration
async def test_pg_count_active_jobs(clean_jobs_app: JobsApp) -> None:
    """PostgresBackend.count_active_jobs matches inserted rows."""
    backend = clean_jobs_app.backend
    await backend.enqueue(make_enqueue_args(queue="default", actor="test_actor"))
    await backend.enqueue(make_enqueue_args(queue="default", actor="test_actor"))
    await backend.enqueue(make_enqueue_args(queue="priority", actor="test_actor"))
    assert await backend.count_active_jobs(["default"]) == 2
    assert await backend.count_active_jobs(["default", "priority"]) == 3
    assert await backend.count_active_jobs([]) == 0
```

#### 1b. Implementation (GREEN)

**File:** `src/taskq/backend/_protocol.py` — add method to `Backend` protocol:

```python
class Backend(Protocol):
    # ... after count_pending_jobs ...

    async def count_active_jobs(self, queues: list[str]) -> int:
        """Count non-terminal jobs (pending, scheduled, running) in the given queues."""
        ...
```

**File:** `src/taskq/backend/_sql_templates.py` — add SQL template:

```python
# NOTE: the status set must match ACTIVE_STATUSES from statemachine.py
# (pending, scheduled, running). If a new non-terminal status is added
# to the state machine, update this SQL list too.
count_active_jobs=(
    f'SELECT count(*)::int FROM "{s}".jobs '
    f"WHERE queue = ANY($1::text[]) "
    f"AND status IN ('pending', 'scheduled', 'running')"
),
```

**File:** `src/taskq/backend/_reads.py` — add helper:

```python
async def _count_active_jobs(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    queues: list[str],
) -> int:
    if not queues:
        return 0
    async with pool.acquire() as conn:
        return int(await conn.fetchval(sql.count_active_jobs, queues))
```

**File:** `src/taskq/backend/postgres.py` — add method:

```python
async def count_active_jobs(self, queues: list[str]) -> int:
    return await _count_active_jobs(self._worker_pool, self._sql, queues)
```

**File:** `src/taskq/testing/in_memory.py` — add method. Use the
canonical `ACTIVE_STATUSES` from `statemachine.py` instead of a
hardcoded tuple:

```python
from taskq.backend.statemachine import ACTIVE_STATUSES

async def count_active_jobs(self, queues: list[str]) -> int:
    if not queues:
        return 0
    queue_set = set(queues)
    return sum(
        1 for r in self._jobs.values()
        if r.queue in queue_set
        and r.status in ACTIVE_STATUSES
    )
```

**File:** `src/taskq/testing/actor.py` — add method to `FakeBackend`:

```python
async def count_active_jobs(self, queues: list[str]) -> int:
    return 0
```

**File:** `tests/test_backend_protocol.py` — update `TestMethodCount`
(see expected modifications note at the top of Task 1):
- `test_exactly_thirty_six_public_members`: change 36 → 37.
- `test_all_member_names_present`: add `"count_active_jobs"` to `expected`.

#### 1c. Verify

Run `uv run pytest tests/test_count_active_jobs.py -v` — all tests pass.

---

### Task 2: Add settings fields for idle drain mode

#### 2a. Tests (RED)

**File:** `tests/test_worker_settings_notify.py` (or a new `tests/test_worker_settings_drain.py`)

```python
"""Tests for WorkerSettings idle drain fields."""

from taskq.settings import WorkerSettings


def test_idle_settle_window_default():
    s = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"})
    assert s.idle_settle_window == 2.0


def test_idle_poll_interval_default():
    s = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"})
    assert s.idle_poll_interval == 1.0


def test_idle_max_runtime_default_none():
    s = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"})
    assert s.idle_max_runtime is None


def test_idle_settle_window_env_override():
    s = WorkerSettings.load_from_dict({
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_IDLE_SETTLE_WINDOW": "5.0",
    })
    assert s.idle_settle_window == 5.0


def test_idle_max_runtime_env_override():
    s = WorkerSettings.load_from_dict({
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_IDLE_MAX_RUNTIME": "300",
    })
    assert s.idle_max_runtime == 300.0
```

#### 2b. Implementation (GREEN)

**File:** `src/taskq/settings.py` — add three fields to `WorkerSettings` (see API surface above).

#### 2c. Verify

Run `uv run pytest tests/test_worker_settings_drain.py -v` — all tests pass.

---

### Task 3: Add `drain_failures` counter to WorkerDeps

#### 3a. Tests (RED)

**File:** `tests/test_worker_drain.py` (partial — will grow in later tasks)

```python
"""Unit tests for drain monitor and until-idle mode."""

from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import ShutdownPhase


def test_worker_deps_drain_failures_default_zero():
    """WorkerDeps initializes drain_failures to 0."""
    from tests.conftest import _FakePool
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict({
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEALTH_SOCKET_PATH": "/tmp/test_drain_deps.sock",
    })
    pool = _FakePool()
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    assert deps.drain_failures == 0
```

#### 3b. Implementation (GREEN)

**File:** `src/taskq/worker/deps.py` — add field:

```python
@dataclass
class WorkerDeps:
    # ... existing fields ...
    drain_failures: int = 0
    """Count of jobs that reached a non-success terminal state during
    until-idle drain mode. Incremented by di_consumer_loop when
    dispatch_one_job returns 'failed' or 'cancelled'. 'scheduled'
    (snooze/retry) does NOT increment — a retried job is not a drain
    failure."""
```

#### 3c. Verify

Run `uv run pytest tests/test_worker_drain.py::test_worker_deps_drain_failures_default_zero -v`.

---

### Task 4: Return `AttemptOutcome` from `dispatch_one_job` and increment `drain_failures`

#### 4a. Modify `dispatch_one_job` return type (C1 fix)

**File:** `src/taskq/worker/dispatch.py`

`dispatch_one_job` currently returns `None` (`dispatch.py:120`). The
function-local `outcome` variable (`dispatch.py:170`) — set to the
`consume_one_job` result (`dispatch.py:289`), `"cancelled"` from the
`CancelledError` arm (`dispatch.py:297`), or `"failed"` from the generic
exception arm (`dispatch.py:301`) — is consumed only by metrics in the
`finally` block (`dispatch.py:326-329`) and never returned.

**Change:** The return type annotation changes from `-> None` to
`-> AttemptOutcome` (the `Literal["succeeded","failed","cancelled","scheduled"]`
type defined at `_consumer.py:77-82`). Add `return outcome` in the
`finally` block, after the metrics calls:

```python
async def dispatch_one_job(
    ...
) -> AttemptOutcome:
    ...
    outcome: AttemptOutcome = "failed"
    try:
        ...
    except asyncio.CancelledError:
        outcome = "cancelled"
        ...
        raise
    except Exception as exc:
        outcome = "failed"
        ...
    finally:
        elapsed = time.monotonic() - t0
        record_consumed_message(job.actor, job.queue, outcome=_to_consumed_outcome(outcome))
        record_process_duration(job.actor, job.queue, elapsed)
        return outcome  # NEW — return the outcome to the caller
```

> **Important:** `return` inside a `finally` block suppresses any
> exception propagation. The `CancelledError` arm does `raise` after
> setting `outcome = "cancelled"`, but the `finally`'s `return outcome`
> will suppress it. This is actually the **desired behavior** for the
> drain case — `di_consumer_loop` should see `"cancelled"` as a return
> value, not as a raised exception. However, this changes the
> `CancelledError` propagation contract: callers that currently rely on
> `CancelledError` propagating out of `dispatch_one_job` (e.g., the
> consumer loop being cancelled during shutdown) will no longer see it
> raised. Verify that `di_consumer_loop`'s outer `except Exception`
> (`run.py:506-507`) does not need to catch `CancelledError` — it
> doesn't, because `CancelledError` is a `BaseException` subclass, not
> `Exception`. The `return` in `finally` is safe here.
>
> Alternatively, to avoid the `finally`-`return` subtlety, move the
> `return outcome` to the end of the `try` block (after the
> `except` arms, before `finally`), and let `finally` only do metrics.
> This preserves `CancelledError` propagation while still returning the
> outcome on the normal path. **This alternative is preferred** — it
> avoids the well-known `finally`-swallows-exception footgun:

```python
    # Preferred: return after the try/except, not in finally
    try:
        ...
        outcome = result  # from consume_one_job
        ...
    except asyncio.CancelledError:
        outcome = "cancelled"
        ...
        raise  # CancelledError still propagates
    except Exception as exc:
        outcome = "failed"
        ...
    finally:
        elapsed = time.monotonic() - t0
        record_consumed_message(job.actor, job.queue, outcome=_to_consumed_outcome(outcome))
        record_process_duration(job.actor, job.queue, elapsed)

    return outcome  # reached by the success path AND the handled
    # generic-exception path (which does not re-raise); only the
    # CancelledError arm leaves without returning
```

> With this approach, the outcome reaches the caller on **both** the
> normal path and the dominant actor-failure path: the generic
> `except Exception` arm (`dispatch.py:300-325`) does NOT re-raise —
> it routes through `_handle_generic_exception` (retry/fail) and
> completes, so control falls through to the trailing `return outcome`
> and the caller receives `"failed"` as a **value**. Only the
> `CancelledError` arm re-raises, so cancellation propagates through
> `di_consumer_loop` exactly as today (the worker is being cancelled).
> `di_consumer_loop`'s outer `except Exception` (`run.py:506-507`) is
> the backstop for infrastructure errors that escape `dispatch_one_job`
> entirely (e.g. scope-bootstrap failures before the try block) — it
> also increments `drain_failures` (see 4c below).

#### 4b. Tests (RED)

**File:** `tests/test_worker_drain.py`

> **Patch target:** `di_consumer_loop` imports `dispatch_one_job` into its
> own module namespace (`from taskq.worker.dispatch import dispatch_one_job`,
> `run.py:61`). Patch **`taskq.worker.run.dispatch_one_job`** — patching
> `taskq.worker.dispatch.dispatch_one_job` would leave `run.py`'s
> already-bound reference untouched and the tests would exercise the real
> function.

```python
async def test_di_consumer_loop_increments_drain_failures_on_failure(
    settings: WorkerSettings,
) -> None:
    """di_consumer_loop captures dispatch_one_job outcome and increments
    deps.drain_failures when a job returns 'failed'."""
    # Setup: mock deps, local_queue, backend, dispatch_one_job
    # Seed a job into local_queue
    # Patch dispatch_one_job to return "failed"
    # Run di_consumer_loop for one iteration (set shutdown_event after first job)
    # Assert deps.drain_failures == 1


async def test_di_consumer_loop_no_increment_on_success(
    settings: WorkerSettings,
) -> None:
    """di_consumer_loop does NOT increment drain_failures on success or scheduled."""
    # Same setup, patch dispatch_one_job to return "succeeded"
    # Assert deps.drain_failures == 0
    # Also test "scheduled" (snooze/retry) → no increment


async def test_di_consumer_loop_increments_on_exception(
    settings: WorkerSettings,
) -> None:
    """di_consumer_loop increments drain_failures when dispatch_one_job raises."""
    # Patch dispatch_one_job to raise RuntimeError
    # Assert deps.drain_failures == 1


async def test_dispatch_one_job_returns_attempt_outcome() -> None:
    """dispatch_one_job returns AttemptOutcome, not None."""
    # Call dispatch_one_job with a mocked actor that succeeds
    # Assert the return value is "succeeded" (not None)
```

#### 4c. Implementation (GREEN)

**File:** `src/taskq/worker/run.py` — modify `di_consumer_loop`:

```python
# Current (run.py:490-507):
try:
    await dispatch_one_job(...)
except Exception:
    _consumer_log.exception("dispatch-failed", job_id=str(job.id))

# New:
try:
    outcome = await dispatch_one_job(...)
    if outcome in ("failed", "cancelled"):
        deps.drain_failures += 1
except Exception:
    _consumer_log.exception("dispatch-failed", job_id=str(job.id))
    deps.drain_failures += 1
```

> **Outcome vocabulary:** `AttemptOutcome = Literal["succeeded",
> "failed", "cancelled", "scheduled"]` (`_consumer.py:77-82`). The
> failure set is `{"failed", "cancelled"}` — NOT `"abandoned"`, which
> does not exist in `AttemptOutcome` (it exists only in the
> metrics-side `ConsumedOutcome` mapping, `dispatch.py:61-71`).
> `"scheduled"` (snooze/retry/reservation-denial) is excluded because a
> retried job is not a drain failure — it will be re-dispatched.

#### 4d. Verify

Run `uv run pytest tests/test_worker_drain.py -k "di_consumer_loop or dispatch_one_job" -v`.

---

### Task 5: Implement the drain monitor loop (`src/taskq/worker/drain.py`)

#### 5a. Tests (RED)

**File:** `tests/test_worker_drain.py`

All Task 5 tests patch `orchestrate_shutdown` to avoid running the real
four-phase machinery against mock deps. The mock sets
`shutdown_event` in its `finally` (mirroring the real function) and
records its call. The drain monitor's `_trigger_drain_shutdown` creates
a wrapper task around the (mocked) `orchestrate_shutdown` — the test
awaits `orchestrator_holder[0]` to get the exit code.

```python
import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from taskq.worker.drain import drain_monitor_loop
from taskq.worker.shutdown import ShutdownPhase


def _make_mock_deps(*, active_jobs_count=0, drain_failures=0):
    """Build a minimal mock WorkerDeps for drain monitor tests.

    shutdown_phase MUST be the real ShutdownPhase.NONE enum member —
    _trigger_drain_shutdown's double-orchestration guard is
    ``deps.shutdown_phase is not ShutdownPhase.NONE``, so a stand-in
    (string, MagicMock attribute) would trip the guard in EVERY test
    that expects a trigger and the tests would fail for the wrong reason.
    """
    deps = MagicMock()
    deps.active_jobs.count.return_value = active_jobs_count
    deps.drain_failures = drain_failures
    deps.shutdown_phase = ShutdownPhase.NONE
    deps.settings = MagicMock()
    deps.settings.queues = ["default"]
    return deps


@contextlib.asynccontextmanager
async def _mock_orchestrate(exit_code: int = 0):
    """Patch orchestrate_shutdown to set shutdown_event and return 0.

    The drain monitor's wrapper task calls orchestrate_shutdown and then
    returns the drain exit code. The mock simulates the finally-block
    shutdown_event.set() so the monitor's loop exits.
    """
    async def _mock(deps, settings, worker_id, shutdown_event, escalate_event, *, backend):
        try:
            await asyncio.sleep(0)  # yield once
        finally:
            shutdown_event.set()
        return 0  # mirror the real orchestrate_shutdown's clean-exit return

    with patch("taskq.worker.drain.orchestrate_shutdown", side_effect=_mock):
        yield


async def test_drain_monitor_triggers_shutdown_when_idle() -> None:
    """Drain monitor creates orchestrator task when queues are empty
    and no active jobs after settle window. Does NOT set shutdown_event
    itself — orchestrate_shutdown's finally does that."""
    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps, deps.settings, uuid4(),
            shutdown_event, escalate_event, orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        )

    # Await the wrapper task FIRST: _trigger_drain_shutdown only schedules
    # it (loop.create_task); it has not run when drain_monitor_loop
    # returns. Asserting shutdown_event before this await would race.
    assert len(orchestrator_holder) == 1
    exit_code = await orchestrator_holder[0]
    assert exit_code == 0  # no failures
    # shutdown_event is set by the mocked orchestrate_shutdown's finally
    assert shutdown_event.is_set()


async def test_drain_monitor_exit_code_2_on_failures() -> None:
    """Exit code 2 when drain_failures > 0."""
    deps = _make_mock_deps(active_jobs_count=0, drain_failures=3)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps, deps.settings, uuid4(),
            shutdown_event, escalate_event, orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        )

    assert len(orchestrator_holder) == 1
    exit_code = await orchestrator_holder[0]
    assert exit_code == 2


async def test_drain_monitor_exit_code_3_on_timeout() -> None:
    """Exit code 3 when max_runtime is exceeded."""
    deps = _make_mock_deps(active_jobs_count=1)  # never idle
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=5)  # always busy

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps, deps.settings, uuid4(),
            shutdown_event, escalate_event, orchestrator_holder,
            backend,
            idle_settle_window=10.0,  # never reached
            idle_poll_interval=0.05,
            max_runtime=0.2,
        )

    # Await the wrapper task FIRST (see test_drain_monitor_triggers_shutdown_when_idle)
    assert len(orchestrator_holder) == 1
    exit_code = await orchestrator_holder[0]
    assert exit_code == 3
    assert shutdown_event.is_set()


async def test_drain_monitor_resets_settle_on_new_jobs() -> None:
    """If a new job appears after idle was detected, the settle timer resets."""
    call_count = 0

    async def mock_count(queues):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return 0  # idle
        return 1  # job appeared

    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = mock_count

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list = []

    async with _mock_orchestrate():
        # Run briefly — the job appears before settle window expires
        task = asyncio.create_task(drain_monitor_loop(
            deps, deps.settings, uuid4(),
            shutdown_event, escalate_event, orchestrator_holder,
            backend,
            idle_settle_window=0.5,  # long enough that job appears first
            idle_poll_interval=0.05,
            max_runtime=None,
        ))
        await asyncio.sleep(0.3)
        # Should NOT have triggered shutdown yet
        assert len(orchestrator_holder) == 0
        assert not shutdown_event.is_set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_drain_monitor_does_not_trigger_when_active_jobs() -> None:
    """Active jobs on this worker prevent drain even if queue is empty."""
    deps = _make_mock_deps(active_jobs_count=2)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list = []

    async with _mock_orchestrate():
        task = asyncio.create_task(drain_monitor_loop(
            deps, deps.settings, uuid4(),
            shutdown_event, escalate_event, orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        ))
        await asyncio.sleep(0.3)
        # Should NOT have triggered shutdown
        assert len(orchestrator_holder) == 0
        assert not shutdown_event.is_set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_drain_monitor_skips_when_orchestration_already_active() -> None:
    """Drain monitor does NOT trigger a second orchestrate_shutdown when
    one is already in progress (H2: double-orchestration guard)."""
    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    # Simulate SIGTERM already started orchestration
    fake_task = asyncio.create_task(asyncio.sleep(100))
    orchestrator_holder: list = [fake_task]
    deps.shutdown_phase = ShutdownPhase.CANCELLING

    async with _mock_orchestrate():
        task = asyncio.create_task(drain_monitor_loop(
            deps, deps.settings, uuid4(),
            shutdown_event, escalate_event, orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        ))
        await asyncio.sleep(0.3)
        # Should NOT have appended a second task
        assert len(orchestrator_holder) == 1
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    fake_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await fake_task
```

#### 5b. Implementation (GREEN)

**File:** `src/taskq/worker/drain.py`

```python
"""Drain monitor for until-idle mode.

When spawned as a sibling in the worker's TaskGroup, the drain monitor
polls the backend for active jobs in the worker's subscribed queues.
When the count stays zero for the settle window (and no jobs are active
on this worker), the monitor triggers the normal graceful shutdown
via orchestrate_shutdown — the same path SIGTERM takes.

Exit codes:
  0 — all jobs succeeded (drain_failures == 0)
  2 — some jobs failed (drain_failures > 0)
  3 — max_runtime exceeded before drain completed
"""

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from taskq.worker.shutdown import ShutdownPhase, orchestrate_shutdown

if TYPE_CHECKING:
    from taskq.backend._protocol import Backend
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

_log = structlog.get_logger("taskq.worker.drain")

EXIT_DRAIN_CLEAN = 0
EXIT_DRAIN_WITH_FAILURES = 2
EXIT_DRAIN_TIMEOUT = 3


async def drain_monitor_loop(
    deps: "WorkerDeps",
    settings: "WorkerSettings",
    worker_id: UUID,
    shutdown_event: asyncio.Event,
    escalate_event: asyncio.Event,
    orchestrator_holder: list[asyncio.Task[int]],
    backend: "Backend",
    *,
    idle_settle_window: float,
    idle_poll_interval: float,
    max_runtime: float | None,
) -> None:
    """Monitor for queue drain and trigger graceful shutdown when idle.

    Polls backend.count_active_jobs(queues) and deps.active_jobs.count()
    every idle_poll_interval. When both are zero, starts the settle timer.
    If still zero after idle_settle_window, triggers shutdown.

    If max_runtime is set and exceeded, triggers shutdown with exit code 3.

    Returns after creating the orchestrate_shutdown task. Spawns with
    may_return=True in the sibling spawner. Does NOT set shutdown_event
    — orchestrate_shutdown's finally block sets it at the correct point
    (after all phase work completes), exactly as the SIGTERM signal
    handler does (shutdown.py:311-325).

    Double-orchestration guard (H2): if orchestrator_holder is already
    non-empty or deps.shutdown_phase is not ShutdownPhase.NONE, the
    monitor skips triggering — a SIGTERM-driven orchestration is already
    in progress.
    """
    queues = settings.queues
    start_time = time.monotonic()
    idle_since: float | None = None

    _log.info(
        "drain-monitor-start",
        queues=queues,
        settle_window=idle_settle_window,
        poll_interval=idle_poll_interval,
        max_runtime=max_runtime,
        worker_id=str(worker_id),
    )

    while not shutdown_event.is_set():
        # Check max_runtime
        if max_runtime is not None:
            elapsed = time.monotonic() - start_time
            if elapsed >= max_runtime:
                _log.info(
                    "drain-monitor-timeout",
                    elapsed=elapsed,
                    max_runtime=max_runtime,
                    worker_id=str(worker_id),
                )
                await _trigger_drain_shutdown(
                    deps, settings, worker_id,
                    shutdown_event, escalate_event, orchestrator_holder,
                    backend, exit_code=EXIT_DRAIN_TIMEOUT,
                )
                return

        # Check idle condition
        try:
            queue_count = await backend.count_active_jobs(queues)
        except Exception:
            _log.exception("drain-monitor-count-error", worker_id=str(worker_id))
            queue_count = -1  # unknown — don't trigger

        active_count = deps.active_jobs.count()
        is_idle = queue_count == 0 and active_count == 0

        if is_idle:
            if idle_since is None:
                idle_since = time.monotonic()
                _log.debug(
                    "drain-monitor-idle-detected",
                    worker_id=str(worker_id),
                    queue_count=queue_count,
                    active_count=active_count,
                )
            else:
                idle_elapsed = time.monotonic() - idle_since
                if idle_elapsed >= idle_settle_window:
                    _log.info(
                        "drain-monitor-drained",
                        worker_id=str(worker_id),
                        idle_elapsed=idle_elapsed,
                        drain_failures=deps.drain_failures,
                    )
                    exit_code = (
                        EXIT_DRAIN_WITH_FAILURES
                        if deps.drain_failures > 0
                        else EXIT_DRAIN_CLEAN
                    )
                    await _trigger_drain_shutdown(
                        deps, settings, worker_id,
                        shutdown_event, escalate_event, orchestrator_holder,
                        backend, exit_code=exit_code,
                    )
                    return
        else:
            if idle_since is not None:
                _log.debug(
                    "drain-monitor-idle-reset",
                    worker_id=str(worker_id),
                    queue_count=queue_count,
                    active_count=active_count,
                )
            idle_since = None

        # Wait for poll interval or shutdown
        await _sleep_or_shutdown(shutdown_event, idle_poll_interval)

    _log.info("drain-monitor-exit", reason="shutdown_event", worker_id=str(worker_id))


async def _trigger_drain_shutdown(
    deps: "WorkerDeps",
    settings: "WorkerSettings",
    worker_id: UUID,
    shutdown_event: asyncio.Event,
    escalate_event: asyncio.Event,
    orchestrator_holder: list[asyncio.Task[int]],
    backend: "Backend",
    *,
    exit_code: int,
) -> None:
    """Create the orchestrate_shutdown task with a drain exit code.

    Mirrors the SIGTERM signal handler exactly (shutdown.py:311-325):
    create the wrapper task, append to orchestrator_holder, and do NOT
    set shutdown_event — orchestrate_shutdown's finally sets it at the
    correct point (after all phase work completes). This preserves the
    proven phase ordering: _main's teardown path (deregister_worker,
    scope shutdowns, pool closure) runs strictly AFTER the phases' PG
    writes, not concurrently with them.

    Double-orchestration guard (H2): if orchestrator_holder is already
    non-empty or deps.shutdown_phase is not ShutdownPhase.NONE, skip
    triggering — a SIGTERM-driven orchestration is already in progress.
    The drain monitor returns cleanly (may_return=True covers it).

    Exit-code precedence: if SIGTERM fires first, the signal handler's
    orchestrate_shutdown returns 0 (its default). The drain monitor's
    guard prevents a second orchestration, so the drain exit code is
    never produced — the worker exits 0 (SIGTERM's code). If the drain
    monitor fires first, its wrapper task returns the drain exit code
    (0/2/3), and a subsequent SIGTERM's orchestration is blocked by the
    matching guard on the signal handler (see Task 6 implementation
    notes). Exit-code precedence is therefore first-trigger-wins, with
    the first trigger's holder[0] determining the exit code.
    """
    # ── H2: double-orchestration guard ─────────────────────────────
    if orchestrator_holder or deps.shutdown_phase is not ShutdownPhase.NONE:
        _log.info(
            "drain-monitor-skip-trigger",
            reason="orchestration-already-active",
            holder_len=len(orchestrator_holder),
            shutdown_phase=deps.shutdown_phase,
            worker_id=str(worker_id),
        )
        return

    loop = asyncio.get_running_loop()

    async def _drain_orchestrate() -> int:
        await orchestrate_shutdown(
            deps,
            settings,
            worker_id,
            shutdown_event,
            escalate_event,
            backend=backend,
        )
        return exit_code

    task = loop.create_task(_drain_orchestrate())
    orchestrator_holder.append(task)
    # Do NOT set shutdown_event here — orchestrate_shutdown's finally
    # block sets it after all phase work completes (shutdown.py:265),
    # exactly as the SIGTERM signal handler does. Setting it here would
    # release _main's teardown path concurrently with the phases,
    # racing pool/scope teardown against PG writes.
    _log.info(
        "drain-monitor-triggered-shutdown",
        exit_code=exit_code,
        worker_id=str(worker_id),
    )


async def _sleep_or_shutdown(shutdown_event: asyncio.Event, duration: float) -> None:
    """Sleep for duration, or return early if shutdown_event is set."""
    if shutdown_event.is_set():
        return
    sleep_task = asyncio.create_task(asyncio.sleep(duration))
    shutdown_task = asyncio.create_task(shutdown_event.wait())
    try:
        await asyncio.wait(
            [sleep_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        for task in (sleep_task, shutdown_task):
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
```

#### 5c. Verify

Run `uv run pytest tests/test_worker_drain.py -k "drain_monitor" -v`.

---

### Task 6: Wire `until_idle` through `worker_main` → `_main`

#### 6a. Tests (RED)

**File:** `tests/test_worker_main.py` (additions)

> **Harness interaction (M3):** The default `_use_test_harness` fakes
> `install_signal_handlers` to set `shutdown_event` immediately and
> pre-resolves `orchestrator_holder` with `exit_value=0`
> (`test_worker_main.py:173-187, 227-228`). With the default harness,
> the drain monitor's loop exits at its first check without triggering
> — the test passes vacuously. All Task 6 tests must use
> `set_shutdown=False` (so `shutdown_event` is NOT pre-set and the
> holder is NOT pre-populated) and patch `orchestrate_shutdown` to run
> a minimal mock that sets `shutdown_event` in its `finally` (mirroring
> the real function's contract). The real drain monitor drives the
> mocked orchestration.
>
> The two H2 ordering tests additionally use the harness's `install_fn`
> override hook (invoked in place of the default fake installer) to
> drive the signal path deterministically. A custom `install_fn` MUST
> set `h.shutdown_event = sh_ev` (the default `_fake_install` does
> this) — otherwise the parked sibling fakes return immediately and
> trip the sibling-crash contract, tearing down the TaskGroup for the
> wrong reason.
>
> **RED/GREEN shape:** at RED all Task 6 tests error with `TypeError`
> (`_main() got an unexpected keyword argument 'until_idle'`). After
> wiring, the two H2 tests still fail if either double-orchestration
> guard is missing — the holder-length assertions go to 2.

```python
import asyncio
import contextlib
import signal
from collections.abc import Callable
from unittest.mock import AsyncMock, patch

from taskq.worker import shutdown as shutdown_mod
from taskq.worker.shutdown import install_signal_handlers as real_install_signal_handlers


@contextlib.asynccontextmanager
async def _patch_orchestrate_for_drain(work_seconds: float = 0.0):
    """Patch orchestrate_shutdown to set shutdown_event and return 0.

    BOTH import namespaces must be patched: ``drain.py`` binds
    ``orchestrate_shutdown`` at module import time
    (``from taskq.worker.shutdown import ..., orchestrate_shutdown``),
    so patching only ``taskq.worker.shutdown.orchestrate_shutdown``
    leaves the drain monitor's wrapper task calling the REAL four-phase
    machinery against stub deps. The signal-handler path resolves the
    name from ``shutdown.py``'s globals, so that namespace needs the
    patch too (H2 tests).

    ``work_seconds`` simulates phase-work duration before the finally
    sets shutdown_event — needed by the H2 ordering tests so the
    competing trigger has time to fire mid-orchestration.
    """
    async def _mock_orchestrate(deps, settings, worker_id, shutdown_event,
                                escalate_event, *, backend):
        try:
            await asyncio.sleep(work_seconds)
        finally:
            shutdown_event.set()
        return 0

    with (
        patch("taskq.worker.drain.orchestrate_shutdown",
              side_effect=_mock_orchestrate),
        patch("taskq.worker.shutdown.orchestrate_shutdown",
              side_effect=_mock_orchestrate),
    ):
        yield


async def test_until_idle_spawns_drain_monitor(settings: WorkerSettings) -> None:
    """_main with until_idle=True spawns the drain monitor as a sibling."""
    with _use_test_harness(settings, set_shutdown=False) as h:
        h.backend.count_active_jobs = AsyncMock(return_value=0)
        async with _patch_orchestrate_for_drain():
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=0.1,
                idle_poll_interval=0.05,
                max_runtime=5.0,
            )
    # Should exit 0 (no failures)
    assert result == 0
    # Non-vacuous: the drain monitor triggered exactly one orchestration
    # (wiring-broken would hang on shutdown_event and time out instead)
    assert len(h.captured_holder[0]) == 1


async def test_until_idle_exit_code_2_on_failures(settings: WorkerSettings) -> None:
    """_main with until_idle=True and drain_failures > 0 exits 2.

    The harness builds the WorkerDeps instance BEFORE _main runs
    (``h.deps = _stub_deps(settings)`` in ``_use_test_harness``) and the
    patched ``open_worker_deps`` hands that same instance to _main — so
    the test can set ``h.deps.drain_failures`` directly before _main
    starts; no consumer side-effect hook is needed.
    """
    with _use_test_harness(settings, set_shutdown=False) as h:
        h.backend.count_active_jobs = AsyncMock(return_value=0)
        h.deps.drain_failures = 1  # one job failed during the drain
        async with _patch_orchestrate_for_drain():
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=0.1,
                idle_poll_interval=0.05,
                max_runtime=5.0,
            )
    assert result == 2


async def test_until_idle_exit_code_3_on_timeout(settings: WorkerSettings) -> None:
    """_main with until_idle=True and max_runtime exceeded exits 3."""
    with _use_test_harness(settings, set_shutdown=False) as h:
        # Backend always returns > 0 (never idle)
        h.backend.count_active_jobs = AsyncMock(return_value=10)
        async with _patch_orchestrate_for_drain():
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=10.0,
                idle_poll_interval=0.05,
                max_runtime=0.3,
            )
    assert result == 3
    assert len(h.captured_holder[0]) == 1  # timeout trigger fired exactly once


async def test_without_until_idle_no_drain_monitor(settings: WorkerSettings) -> None:
    """_main without until_idle does NOT spawn the drain monitor."""
    with _use_test_harness(settings, set_shutdown=True) as h:
        # With set_shutdown=True the harness pre-sets shutdown_event,
        # so _main exits immediately without the drain monitor triggering.
        result = await _main(settings)
    assert result == 0
    # Verify the drain monitor was not spawned: the harness's
    # captured_holder should contain only the fake signal-install
    # future, not a drain-triggered orchestration task.


async def test_sigterm_during_until_idle_signal_first(settings: WorkerSettings) -> None:
    """H2: SIGTERM orchestration already in progress before the drain
    monitor would trigger — the drain monitor's guard must skip, and the
    exit code is SIGTERM's 0 (first-trigger-wins).

    Non-vacuous: the orchestration mock holds shutdown_event open for
    0.4s — well past the drain monitor's trigger point (settle 0.1 /
    poll 0.05 ≈ 0.15–0.2s). Without the drain-side guard the monitor
    WOULD append a second orchestration task and the holder-length
    assertion fails.
    """
    holder_ref: list[list[asyncio.Task[int]]] = []

    def _install_signal_first(loop, deps, wid, sh_ev, esc_ev, backend, holder):
        # Mimic _on_shutdown_signal's first-signal arm (shutdown.py:314-321):
        # SIGTERM arrived during startup, before the drain monitor polls.
        task = loop.create_task(
            shutdown_mod.orchestrate_shutdown(  # the patched mock (call-time lookup)
                deps, deps.settings, wid, sh_ev, esc_ev, backend=backend
            )
        )
        holder.append(task)
        holder_ref.append(holder)
        h.shutdown_event = sh_ev  # keep the harness's parked sibling fakes alive

    with _use_test_harness(settings, set_shutdown=False) as h:
        h.install_fn = _install_signal_first
        h.backend.count_active_jobs = AsyncMock(return_value=0)
        async with _patch_orchestrate_for_drain(work_seconds=0.4):
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=0.1,
                idle_poll_interval=0.05,
                max_runtime=5.0,
            )
    assert result == 0  # SIGTERM's code, not a drain code
    assert len(holder_ref[0]) == 1  # drain monitor's guard skipped its trigger


async def test_sigterm_during_until_idle_drain_first(settings: WorkerSettings) -> None:
    """H2: drain monitor triggers first, then SIGTERM arrives
    mid-orchestration — the REAL signal handler's guard must skip the
    second orchestration, and the exit code is the drain code (2 here),
    not SIGTERM's 0 (first-trigger-wins).

    Uses the real install_signal_handlers with loop.add_signal_handler
    shimmed to CAPTURE the registered callback instead of installing
    real OS signal handlers in the test process; the captured
    first-signal callback is fired via loop.call_later while the
    drain-triggered orchestration (0.4s of mocked phase work) is still
    running. Without the signal-side guard, the callback appends a
    second orchestration task and the holder-length assertion fails.
    """
    captured: dict[int, Callable[[], None]] = {}
    holder_ref: list[list[asyncio.Task[int]]] = []

    def _install_capturing(loop, deps, wid, sh_ev, esc_ev, backend, holder):
        orig_add = loop.add_signal_handler

        def _capture(sig, callback, *args):
            captured[sig] = callback  # do NOT install real OS handlers

        loop.add_signal_handler = _capture  # type: ignore[method-assign]
        try:
            real_install_signal_handlers(
                loop, deps, wid, sh_ev, esc_ev, backend, holder
            )
        finally:
            loop.add_signal_handler = orig_add  # type: ignore[method-assign]
        holder_ref.append(holder)
        h.shutdown_event = sh_ev  # keep the harness's parked sibling fakes alive
        # Fire the simulated SIGTERM mid-drain-orchestration: the drain
        # monitor triggers at ~0.15–0.2s (settle 0.1 + polls at 0.05);
        # the mocked orchestration runs for 0.4s.
        loop.call_later(0.3, captured[signal.SIGTERM])

    with _use_test_harness(settings, set_shutdown=False) as h:
        h.install_fn = _install_capturing
        h.backend.count_active_jobs = AsyncMock(return_value=0)
        h.deps.drain_failures = 1  # drain code 2 — distinguishable from SIGTERM's 0
        async with _patch_orchestrate_for_drain(work_seconds=0.4):
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=0.1,
                idle_poll_interval=0.05,
                max_runtime=5.0,
            )
    assert result == 2  # the drain code wins; SIGTERM's 0 is never produced
    assert len(holder_ref[0]) == 1  # signal handler's guard skipped the second orchestration
```

#### 6b. Implementation (GREEN)

**File:** `src/taskq/worker/_bootstrap.py` — modify `_main`:

1. Add `until_idle`, `idle_settle_window`, `idle_poll_interval`, `max_runtime` parameters.
2. Resolve overrides against settings:

```python
if until_idle:
    settle = idle_settle_window if idle_settle_window is not None else settings.idle_settle_window
    poll = idle_poll_interval if idle_poll_interval is not None else settings.idle_poll_interval
    runtime = max_runtime if max_runtime is not None else settings.idle_max_runtime
```

3. Inside the TaskGroup, after spawning all other siblings, conditionally
   spawn the drain monitor. Use a top-level import (no cycle: `drain.py`
   imports only `taskq.worker.shutdown`, which `_bootstrap.py` already
   imports):

```python
from taskq.worker.drain import drain_monitor_loop

# ... inside the TaskGroup, after all other _spawn calls:
if until_idle:
    _spawn(
        drain_monitor_loop(
            deps,
            settings,
            worker_id,
            shutdown_event,
            escalate_event,
            orchestrator_holder,
            backend,
            idle_settle_window=settle,
            idle_poll_interval=poll,
            max_runtime=runtime,
        ),
        may_return=True,
        name="worker.drain_monitor",
    )
```

4. **H2: Add a matching guard to `install_signal_handlers`.** The
   signal handler's `_on_shutdown_signal` (first-signal arm,
   `shutdown.py:314-325`) currently creates an `orchestrate_shutdown`
   task unconditionally. Add a guard so it skips when
   `orchestrator_holder` is already non-empty or
   `deps.shutdown_phase is not ShutdownPhase.NONE`:

```python
def _on_shutdown_signal() -> None:
    nonlocal _sig_count
    _sig_count += 1
    if _sig_count == 1:
        # H2 guard: skip if orchestration is already in progress
        # (e.g., drain monitor triggered first)
        if orchestrator_holder or deps.shutdown_phase is not ShutdownPhase.NONE:
            return
        task = loop.create_task(
            orchestrate_shutdown(...)
        )
        orchestrator_holder.append(task)
    elif _sig_count == 2:
        ...
```

**File:** `src/taskq/worker/_bootstrap.py` — modify `worker_main`:

```python
def worker_main(
    settings: WorkerSettings,
    *,
    actor_registry: Mapping[str, ActorRef[Any, Any]] | None = None,
    di_registry: ProviderRegistry | None = None,
    cron_registry: list[CronScheduleSpec] | None = None,
    connections: WorkerConnections | None = None,
    until_idle: bool = False,
    idle_settle_window: float | None = None,
    idle_poll_interval: float | None = None,
    max_runtime: float | None = None,
) -> int:
    # ... existing setup ...
    with asyncio.Runner() as runner:
        return runner.run(
            _main(
                settings,
                actor_registry=actor_registry,
                _registry=di_registry,
                _cron_registry=schedule_specs,
                connections=connections,
                until_idle=until_idle,
                idle_settle_window=idle_settle_window,
                idle_poll_interval=idle_poll_interval,
                max_runtime=max_runtime,
            )
        )
```

> **Cron warning (L6):** When `until_idle and _cron_registry` is
> non-empty, emit a startup WARNING. Cron schedules create recurring
> jobs indefinitely, making the queue never drain. Add this check in
> `_main` after resolving `until_idle`:
>
> ```python
> if until_idle and _cron_registry:
>     _startup_log.warning(
>         "until-idle-with-cron",
>         kind="until_idle_with_cron",
>         message="until_idle mode is incompatible with cron-driven workloads; "
>                 "the queue will never drain. Use --max-runtime as a cap.",
>     )
> ```

#### 6c. Verify

Run `uv run pytest tests/test_worker_main.py -k "until_idle" -v`.

---

### Task 7: Add CLI `--until-idle` options

#### 7a. Tests (RED)

**File:** `tests/test_cli_worker.py` (additions)

```python
def test_until_idle_flag_passed_to_worker_main(monkeypatch: Any) -> None:
    """--until-idle passes until_idle=True to worker_main."""
    captured: dict[str, Any] = {}

    def fake_worker_main(settings: Any, *, actor_registry: Any = None,
                         until_idle: bool = False, **kwargs: Any) -> int:
        captured["until_idle"] = until_idle
        captured["kwargs"] = kwargs
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _NO_ACTORS_PATH, "--until-idle"])
    assert result.exit_code == 0
    assert captured["until_idle"] is True


def test_until_idle_default_false(monkeypatch: Any) -> None:
    """Without --until-idle, until_idle=False."""
    captured: dict[str, Any] = {}

    def fake_worker_main(settings: Any, *, actor_registry: Any = None,
                         until_idle: bool = False, **kwargs: Any) -> int:
        captured["until_idle"] = until_idle
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _NO_ACTORS_PATH])
    assert result.exit_code == 0
    assert captured["until_idle"] is False


def test_idle_settle_window_passed(monkeypatch: Any) -> None:
    """--idle-settle-window passes the value to worker_main."""
    captured: dict[str, Any] = {}

    def fake_worker_main(settings: Any, *, actor_registry: Any = None,
                         idle_settle_window: float | None = None, **kwargs: Any) -> int:
        captured["idle_settle_window"] = idle_settle_window
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, [
        "worker", "--actors", _NO_ACTORS_PATH,
        "--until-idle", "--idle-settle-window", "5.0",
    ])
    assert result.exit_code == 0
    assert captured["idle_settle_window"] == 5.0


def test_max_runtime_passed(monkeypatch: Any) -> None:
    """--max-runtime passes the value to worker_main."""
    captured: dict[str, Any] = {}

    def fake_worker_main(settings: Any, *, actor_registry: Any = None,
                         max_runtime: float | None = None, **kwargs: Any) -> int:
        captured["max_runtime"] = max_runtime
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, [
        "worker", "--actors", _NO_ACTORS_PATH,
        "--until-idle", "--max-runtime", "300",
    ])
    assert result.exit_code == 0
    assert captured["max_runtime"] == 300.0
```

#### 7b. Implementation (GREEN)

**File:** `src/taskq/cli.py` — add options to the `worker` callback (see API surface above). Pass through to `worker_main`:

```python
try:
    code = _worker_main(
        settings,
        actor_registry=registry,
        until_idle=until_idle,
        idle_settle_window=idle_settle_window,
        idle_poll_interval=idle_poll_interval,
        max_runtime=max_runtime,
    )
except ActorConfigDriftList as e:
    typer.echo(str(e), err=True)
    raise typer.Exit(code=1) from None
raise typer.Exit(code=code)
```

#### 7c. Verify

Run `uv run pytest tests/test_cli_worker.py -k "until_idle or idle_settle or max_runtime" -v`.

---

### Task 8: E2E tests for `--until-idle` mode

These tests use the real e2e infrastructure (Docker containers, real PG) to verify the drain mode works end-to-end.

#### 8a. Implementation: `worker_entry.py` modification

**File:** `tests/e2e/worker_entry.py` — add `TASKQ_UNTIL_IDLE` env var support:

```python
if __name__ == "__main__":
    settings = WorkerSettings.load()
    until_idle = os.environ.get("TASKQ_UNTIL_IDLE") == "true"
    sys.exit(
        worker_main(
            settings,
            actor_registry=ACTORS,
            di_registry=build_registry(),
            cron_registry=_e2e_cron_registry(),
            until_idle=until_idle,
        )
    )
```

#### 8b. Tests

**File:** `tests/e2e/test_until_idle.py`

```python
"""E2E tests for --until-idle worker drain mode.

Verifies that a worker started with --until-idle:
1. Processes all enqueued jobs
2. Exits with code 0 when all succeed
3. Exits with code 2 when some jobs fail
4. Exits with code 3 when max-runtime is exceeded
5. Waits for and processes scheduled jobs before exiting
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from testcontainers.core.container import DockerContainer

from taskq import JobFailed

from ._assertions import poll_until
from .actors import (
    SlowDeliverPayload,
    SyncUserProfilePayload,
    WelcomeEmailPayload,
    send_welcome_email,
    slow_deliver_webhook,
    sync_user_profile,
)
from .conftest import _stop_container

if TYPE_CHECKING:
    from containerspec import BuiltImage
    from testcontainers.core.container import Network

    from taskq import TaskQ

    from .conftest import E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def _start_idle_worker(
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    *,
    extra_env: dict[str, str] | None = None,
) -> DockerContainer:
    """Start a worker container with until-idle mode enabled.

    ``TASKQ_UNTIL_IDLE=true`` is REQUIRED — ``worker_entry.py`` only
    enables ``until_idle`` when this env var is set; without it the
    container runs a normal forever-worker and the exit-polling
    assertions below time out. Settle/poll intervals are shortened for
    fast tests. ``extra_env`` carries per-test overrides (e.g.
    ``TASKQ_IDLE_MAX_RUNTIME``).

    Deliberately a helper, not a fixture: each test enqueues different
    jobs BEFORE the worker starts, so container construction must stay
    inside the test body after the enqueues.
    """
    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_network(e2e_network).with_network_aliases(
        f"worker-idle-{e2e_schema.schema_name}-{uuid4().hex[:6]}"
    )
    for key, value in e2e_schema.worker_env.items():
        container.with_env(key, value)
    container.with_env("TASKQ_UNTIL_IDLE", "true")
    container.with_env("TASKQ_IDLE_SETTLE_WINDOW", "1.0")
    container.with_env("TASKQ_IDLE_POLL_INTERVAL", "0.5")
    for key, value in (extra_env or {}).items():
        container.with_env(key, value)
    await asyncio.to_thread(container.start)
    return container


async def _wait_for_container_exit(container, timeout: float = 30.0) -> int:
    """Poll container status until it exits, then return the exit code.

    Calls wrapped.reload() before reading status/attrs — Docker attrs
    are cached at fetch time and must be explicitly refreshed.
    """
    async def _container_exited() -> bool:
        wrapped = container.get_wrapped_container()
        wrapped.reload()
        return wrapped.status == "exited"

    await poll_until(
        _container_exited,
        timeout=timeout,
        description="worker container exits after drain",
    )

    wrapped = container.get_wrapped_container()
    wrapped.reload()  # refresh attrs before reading ExitCode
    return int(wrapped.attrs["State"]["ExitCode"])


async def test_until_idle_drains_and_exits_zero(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    run_id: str,
) -> None:
    """Worker with --until-idle processes all jobs and exits 0.

    Enqueues 3 short jobs, starts a worker with TASKQ_UNTIL_IDLE=true,
    and verifies:
    1. All 3 jobs reach 'succeeded'
    2. The worker container exits with code 0
    3. The worker container stops on its own (no SIGTERM needed)
    """
    # Enqueue 3 jobs BEFORE the worker starts
    handles = []
    for i in range(3):
        handle = await e2e_client.enqueue(
            send_welcome_email,
            WelcomeEmailPayload(run_id=run_id, user_id=f"u-{i}", email=f"u{i}@example.com"),
        )
        handles.append(handle)

    container = await _start_idle_worker(e2e_network, e2e_schema, e2e_worker_image)
    try:
        # Wait for all jobs to complete
        for handle in handles:
            await handle.wait(timeout=60)

        # Wait for worker container to exit on its own
        exit_code = await _wait_for_container_exit(container, timeout=30.0)
        assert exit_code == 0, f"expected exit 0, got {exit_code}"

    finally:
        await asyncio.to_thread(_stop_container, container)


async def test_until_idle_exits_nonzero_on_failures(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    run_id: str,
) -> None:
    """Worker with --until-idle exits 2 when a job reaches 'failed'.

    Uses ``sync_user_profile`` with ``fail_kind="permanent"``:
    ``PermanentSyncError`` is in that actor's ``non_retryable_exceptions``,
    so the first attempt moves the job straight to the terminal
    'failed' status — ``dispatch_one_job`` returns ``"failed"``,
    ``drain_failures`` increments, and the drain exit code becomes 2.
    (``deliver_webhook`` cannot produce a failure — it records a
    'delivered' effect unconditionally, and the previous sketch's
    ``WebhookPayload(url=...)`` class does not exist; the real payload
    is ``DeliverWebhookPayload(run_id, endpoint_id)``.)
    """
    # Enqueue 1 succeeding + 1 permanently-failing job
    good = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(run_id=run_id, user_id="ok", email="ok@example.com"),
    )
    bad = await e2e_client.enqueue(
        sync_user_profile,
        SyncUserProfilePayload(
            run_id=run_id, user_id="bad", fail_times=1, fail_kind="permanent"
        ),
    )

    container = await _start_idle_worker(e2e_network, e2e_schema, e2e_worker_image)
    try:
        await good.wait(timeout=60)
        # handle.wait() raises JobFailed on non-success terminal states
        with pytest.raises(JobFailed):
            await bad.wait(timeout=60)
        exit_code = await _wait_for_container_exit(container, timeout=30.0)
        assert exit_code == 2, f"expected exit 2, got {exit_code}"
    finally:
        await asyncio.to_thread(_stop_container, container)


async def test_until_idle_timeout_exit_3(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    run_id: str,
) -> None:
    """Worker with --until-idle --max-runtime exits 3 when timeout hits.

    Enqueues a long-running job (slow_deliver_webhook, sleeps 3s), starts
    a worker with TASKQ_UNTIL_IDLE=true and TASKQ_IDLE_MAX_RUNTIME=2,
    and verifies the exit code is 3 (timeout): the queue never reads as
    idle within the cap because the job is still 'running'.
    """
    await e2e_client.enqueue(
        slow_deliver_webhook,
        SlowDeliverPayload(run_id=run_id, endpoint_id="slow"),
    )

    container = await _start_idle_worker(
        e2e_network,
        e2e_schema,
        e2e_worker_image,
        extra_env={"TASKQ_IDLE_MAX_RUNTIME": "2"},
    )
    try:
        exit_code = await _wait_for_container_exit(container, timeout=30.0)
        assert exit_code == 3, f"expected exit 3, got {exit_code}"
    finally:
        await asyncio.to_thread(_stop_container, container)


async def test_until_idle_scheduled_jobs_drain(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    run_id: str,
) -> None:
    """Worker with --until-idle waits for future-scheduled jobs.

    Enqueues a job with scheduled_at 3s in the future, starts a worker
    with TASKQ_UNTIL_IDLE=true, and verifies:
    1. The worker waits (does not exit immediately when pending=0 —
       'scheduled' counts as active under count_active_jobs)
    2. The scheduled job becomes due, is dispatched, and succeeds
    3. The worker then exits with code 0
    """
    from datetime import datetime, timedelta, timezone

    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=3)
    handle = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(run_id=run_id, user_id="sched", email="sched@example.com"),
        scheduled_at=scheduled_at,
    )

    container = await _start_idle_worker(e2e_network, e2e_schema, e2e_worker_image)
    try:
        # The job should eventually succeed
        await handle.wait(timeout=30)
        # Worker should exit after processing the scheduled job
        exit_code = await _wait_for_container_exit(container, timeout=30.0)
        assert exit_code == 0, f"expected exit 0, got {exit_code}"
    finally:
        await asyncio.to_thread(_stop_container, container)
```

#### 8c. Verify

Run `uv run pytest tests/e2e/test_until_idle.py -v --mark e2e`.

---

### Task 9: Documentation updates

**Files to update:**

1. `docs/guides/workers.md` — add "Until-idle mode" section
2. `docs/guides/cli.md` — document `--until-idle` and related flags
3. `docs/architecture.md` — mention drain monitor in component diagram

**`docs/guides/workers.md` addition:**

```markdown
## Until-idle mode

For CLI tools and batch processing scripts that enqueue a finite set of jobs
and need to exit with a status code, pass `--until-idle` (or `until_idle=True`
to `worker_main()`):

```shell
taskq worker --actors myapp.actors:registry --until-idle
```

```python
exit_code = worker_main(settings, actor_registry=registry, until_idle=True)
# 0 = all jobs succeeded
# 2 = some jobs failed
# 3 = max-runtime exceeded
```

The worker polls its subscribed queues every `idle_poll_interval` (default 1s).
When no pending, scheduled, or running jobs remain for the settle window
(`idle_settle_window`, default 2s), the worker triggers its normal graceful
shutdown and exits.

An optional `--max-runtime` cap (or `TASKQ_IDLE_MAX_RUNTIME`) forces exit
with code 3 if the drain takes too long — useful for CI pipelines with
time budgets.

**Scheduled jobs:** Jobs with a future `scheduled_at` count as "active" —
the worker waits for them to become due, dispatches them, and processes
them before exiting. Use `--max-runtime` if you don't want to wait for
far-future scheduled jobs.

**Cron:** `--until-idle` is incompatible with cron-driven workloads, which
enqueue jobs indefinitely. Do not combine `--until-idle` with `@cron`
decorators. A startup WARNING is emitted when both `until_idle` and cron
schedules are active.

**Multi-worker:** If multiple workers consume the same queue, `--until-idle`
waits for the queue to drain across ALL workers, not just this one. This
is correct for finite-batch scenarios but may cause unexpected waiting
in shared-queue deployments.

**Known limitations of `drain_failures` (worker-local counter):**

The `drain_failures` counter tracks only jobs dispatched *by this worker*
that returned `"failed"` or `"cancelled"` from `dispatch_one_job`. It does
NOT capture:

- **Deadline-exceeded sweep failures:** Jobs that reach `failed` via the
  maintenance leader's deadline sweep (pending/scheduled → failed without
  any consumer dispatch) are never seen by `di_consumer_loop` and do not
  increment `drain_failures`. Exit 0 is possible with deadline-exceeded
  failures in the batch.
- **Other-worker failures:** If multiple workers share the queue and
  another worker's consumer fails a job, this worker's `drain_failures`
  stays 0. Exit 0 is possible with failures on other workers.
- **Actor-not-found jobs:** A misconfigured job whose actor is not in this
  worker's registry is released via `mark_snoozed(10s)` and cycles
  scheduled→pending→claimed→snoozed indefinitely. Since `scheduled`
  counts as active, the queue never reads as drained, and the worker
  hangs until `--max-runtime` is hit. This is **accepted** — operators
  should use `--max-runtime` as the escape hatch. Counting actor-not-found
  as a drain failure after N consecutive releases was considered but
  rejected (it requires cross-job state tracking that adds complexity
  without a clear operator benefit; `--max-runtime` already covers the
  stuck-worker case).

These are inherent to a worker-local counter and documented as such.
For batch workflows requiring exact failure accounting, query the backend
for terminal job statuses after the drain completes.
```

---

## Test coverage requirements

### Unit tests

| Component | Test file | Coverage |
|---|---|---|
| `count_active_jobs` (InMemoryBackend) | `tests/test_count_active_jobs.py` | Empty queues, no jobs, pending, running, scheduled, terminal excluded, multi-queue, queue subset |
| `count_active_jobs` (PostgresBackend) | `tests/test_count_active_jobs.py` | Integration test with real PG |
| Settings fields | `tests/test_worker_settings_drain.py` | Defaults, env var overrides |
| `WorkerDeps.drain_failures` | `tests/test_worker_drain.py` | Default value |
| `di_consumer_loop` outcome tracking | `tests/test_worker_drain.py` | Success (no increment), failure (increment), exception (increment), `dispatch_one_job` returns AttemptOutcome |
| `drain_monitor_loop` | `tests/test_worker_drain.py` | Idle triggers shutdown, exit code 0, exit code 2 with failures, exit code 3 timeout, settle timer reset on new jobs, active jobs prevent drain, double-orchestration guard (H2) |
| `_main` wiring | `tests/test_worker_main.py` | `until_idle=True` spawns drain monitor, `until_idle=False` does not, exit codes propagated, SIGTERM-during-drain both orders (H2) |
| CLI | `tests/test_cli_worker.py` | `--until-idle` flag, defaults, `--idle-settle-window`, `--max-runtime` |

### E2E tests

| Scenario | Test file | Coverage |
|---|---|---|
| Clean drain | `tests/e2e/test_until_idle.py` | 3 jobs → all succeed → exit 0 |
| Drain with failures | `tests/e2e/test_until_idle.py` | Mix of succeed/fail → exit 2 |
| Max runtime timeout | `tests/e2e/test_until_idle.py` | Long job + short max-runtime → exit 3 |
| Scheduled jobs drain | `tests/e2e/test_until_idle.py` | Future-scheduled job → worker waits → processes → exits |

---

## Backward compatibility analysis

### Default behavior unchanged

- `worker_main(settings, actor_registry=registry)` — no `until_idle` parameter → runs forever exactly as today.
- `taskq worker --actors myapp.actors:registry` — no `--until-idle` flag → runs forever.
- All new settings fields have defaults that preserve existing behavior.
- `WorkerDeps.drain_failures` defaults to `0` and is only read by the drain monitor.

### Breaking changes (accepted under the 1.0.0 directive)

TaskQ 1.0.0 is a breaking release. This feature deliberately makes two
breaking changes rather than contorting the design to avoid them —
no shims, no dual-path compatibility code:

1. **`dispatch_one_job` returns `AttemptOutcome` instead of `None`.**
   The correct design needs the dispatch outcome at the call site; the
   alternative (a parallel callback/side-channel) is strictly worse.
   Callers that `await dispatch_one_job(...)` and ignore the result are
   unaffected at runtime; the change breaks type-level consumers and
   any test double that returns `None` (doubles must now return one of
   `"succeeded" | "failed" | "cancelled" | "scheduled"`).
2. **`Backend` gains a required protocol member.** Third-party backend
   implementations must add `count_active_jobs` — a loud, immediate
   `AttributeError` on first until-idle use, not silent misbehavior.
   The two `TestMethodCount` pins (36→37 members, name set) are updated
   as part of Task 1; the four `BACKEND_PROTOCOL_VERSION == 3` pins
   stay green because the version is NOT bumped (see below).

These pins break intentionally and are listed as expected modifications
rather than worked around — the additive-change convention is not used
to force a worse design.

### Protocol version

`count_active_jobs` is an **additive** method on the `Backend` protocol.
The protocol version does **NOT** need to be bumped. Per the documented
convention (`docs/architecture.md:143-149`):

> Increment it whenever a change alters an existing protocol member's
> observable contract such that an implementation written against the
> previous version would *silently* misbehave … Purely additive changes
> that an old implementation can ignore without producing incorrect
> behaviour … do not require a bump.

An implementation written against protocol v3 that lacks
`count_active_jobs` would raise `AttributeError` when the drain monitor
calls it — a loud failure, not a silent misbehavior. The method is only
called when `until_idle=True`, which is an opt-in mode. `FakeBackend`
(used in unit tests) gets a stub implementation returning `0`.

`BACKEND_PROTOCOL_VERSION` stays at **3**.

### Existing test modifications

Two existing tests in `tests/test_backend_protocol.py` must be updated
(see Task 1 for details):

1. `TestMethodCount.test_exactly_thirty_six_public_members` — the count
   assertion changes from 36 to 37 (adding `count_active_jobs`).
2. `TestMethodCount.test_all_member_names_present` — `"count_active_jobs"`
   is added to the `expected` name set.

No other existing tests require modification. The `drain_failures`
counter is only incremented in `di_consumer_loop` — existing tests that
mock `dispatch_one_job` may need updating if they assert on the exact
call pattern, but the return value capture is additive. The `_main`
function's new parameters are keyword-only with defaults.

### Settings compatibility

New settings fields (`idle_settle_window`, `idle_poll_interval`, `idle_max_runtime`) are all opt-in with defaults that don't affect existing behavior. They load from `TASKQ_*` env vars using the existing dotenvmodel mechanism.

---

## Downstream consumer impact analysis

### warden (`~/src/warden`) — Hybrid LLM proxy

**Current usage:** `src/warden/cli.py:2616-2653` runs `worker_main(...)`
as a long-lived daemon (`warden worker`) and already captures
`exit_code`. No multiprocessing/terminate supervision of the taskq
worker exists — the `proc.terminate()` hits in the codebase target
MLX-backend/test subprocesses, not the taskq worker.

**Impact:** Additive/opt-in, essentially nil. warden's daemon use case
is not a drain scenario. `--max-runtime` could be useful for future
finite-batch commands in warden, but no current pain point exists.

### cennan (`~/src/cennan`) — Enterprise knowledge management

**Current usage:** cennan calls the **private** `taskq.worker._bootstrap._main`
from inside its own coroutine (`src/cennan/cli.py:92-202`) because no
public async entrypoint exists (there is an explicit comment asking for
one upstream). It registers cron specs in production, so `until_idle` is
correctly incompatible there. Its e2e suite drives `_main` as a
background task for finite indexing runs and stops it with
`task.cancel()` (`tests/e2e/test_full_index.py:440-470`).

**Impact:** cennan's e2e drain use case is served by `_main(...,
until_idle=True)` — which Task 6 adds. This replaces the
`task.cancel()` stop mechanism with a clean drain-and-exit. The
downstream section previously showed a `worker_main(...)` migration that
does not fit cennan's in-loop pattern; the actual benefit is the
`_main` parameter, not the sync `worker_main` wrapper. Note the
private-API dependency: cennan should migrate to `worker_main` when a
public async entrypoint is available.

### aacrtool (`~/src/aacrtool`) — Agentic code review tool

**Current usage:** aacrtool's design spec (`docs/specs/2026-07-29-aacrtool-design.md:165`)
lists "Drain-and-exit transient worker" as GAP #14 with an explicit link
to TaskQ #53, and plans an AACRTool-side "CLI drain loop" workaround
"deleted rather than kept" if #53 lands (lines 2835-2841, 2974). Its
own CLI wants exit codes 0/1/2 (line 2535), compatible with the spec's
0/2/3. However, no `.py` file in aacrtool references taskq yet — the
dependency is declared in `pyproject.toml` (`taskq-py[redis,fastapi]>=0.2.2,<1`)
but the integration is pre-implementation.

**Impact:** aacrtool is the verified driver of #53. On landing, aacrtool
adopts `worker_main(until_idle=True)` (or the CLI `--until-idle`) as
the drain mechanism instead of building its own poll-and-signal
supervisor. This is **adoption**, not migration of existing code — the
"current workaround" code block in the previous spec draft does not
exist in aacrtool source yet.

```python
# Planned (aacrtool, on #53 landing):
exit_code = worker_main(settings, actor_registry=registry, until_idle=True)
# exit_code 0 = all reviews succeeded
# exit_code 2 = some reviews failed (report to user)
sys.exit(exit_code)
```

Or via CLI:
```shell
taskq worker --actors myapp.actors:registry --until-idle --max-runtime 600
echo $?
```

---

## Design decisions

### 1. "Idle" definition: pending + scheduled + running = 0

**Decision:** Idle means zero jobs in `pending`, `scheduled`, or `running` status across the worker's subscribed queues, AND zero active (in-flight) jobs on this worker.

**Rationale:** For the finite-batch use case, the user wants ALL jobs processed — including future-scheduled ones. If a job was enqueued with `scheduled_at` in the future as part of the batch, the worker should wait for it, dispatch it, and process it. The `--max-runtime` cap is the escape hatch for far-future scheduled jobs.

**Alternative considered:** "Idle means only `pending` + `running` = 0 (ignore scheduled)." Rejected because it would exit before processing scheduled jobs that are part of the finite batch.

### 2. Settle window: configurable, default 2 seconds

**Decision:** After queues appear empty, wait `idle_settle_window` seconds (default 2.0), then re-check. If still empty, trigger shutdown.

**Rationale:** Handles the race condition where a producer enqueues between the drain monitor's check and the shutdown trigger. 2 seconds is generous enough for NOTIFY → producer wakeup → dispatch → consumer registration, while not adding noticeable latency to the drain.

**Alternative considered:** No settle window (exit immediately when idle). Rejected because the race condition is real in multi-process scenarios.

### 3. Exit codes: 0 / 2 / 3

**Decision:**
- `0`: All jobs succeeded (drain completed, `drain_failures == 0`)
- `2`: Some jobs failed (drain completed, `drain_failures > 0`)
- `3`: Max runtime exceeded (drain interrupted)

**Rationale:** Exit code 0 is the universal "success" code. Exit code 2 is distinct from 1 (which is used for startup errors, config drift, etc.). Exit code 3 is distinct from 2 (timeout vs. job failure). These map cleanly to shell `&&` / `||` patterns and CI pipeline status checks.

**Alternative considered:** Using exit code 1 for failures. Rejected because exit 1 is already used by the CLI for startup errors (actor config drift, bad `--actors` path, etc.) — overloading it would make it impossible to distinguish "couldn't start" from "started but jobs failed."

### 4. Drain monitor as a sibling with `may_return=True`

**Decision:** The drain monitor runs as a sibling in the worker's `TaskGroup`, spawned with `may_return=True` (like the NOTIFY listener fallback). It triggers shutdown by creating an `orchestrate_shutdown` task appended to `orchestrator_holder` — but does **NOT** set `shutdown_event` itself. `orchestrate_shutdown`'s `finally` block sets `shutdown_event` at the correct point (after all phase work completes), exactly as the SIGTERM signal handler does (`shutdown.py:311-325`).

**Rationale:** This reuses the entire existing shutdown path — DRAINING → CANCELLING → FORCING → ABANDONING — without any new shutdown plumbing. The `may_return=True` flag tells the sibling spawner that a clean return is expected (not a bug), matching the NOTIFY listener's pattern. Setting `shutdown_event` immediately (as an earlier draft proposed) would release `_main`'s teardown path concurrently with the orchestration task's phases, racing pool/scope teardown against PG writes — the proven phase ordering from the SIGTERM path must be preserved.

**Double-orchestration guard (H2):** Both the drain monitor and the signal handler check `orchestrator_holder` (non-empty) and `deps.shutdown_phase` (not `ShutdownPhase.NONE`) before triggering. If either guard is tripped, the trigger is skipped — a concurrent orchestration is already in progress. Exit-code precedence is first-trigger-wins: `holder[0]` determines the exit code. If SIGTERM fires first, the signal's `orchestrate_shutdown` returns 0 and the drain exit code is never produced. If the drain monitor fires first, its wrapper returns the drain exit code (0/2/3) and the signal handler's guard prevents a second orchestration.

**Alternative considered:** Direct `os._exit()` from the drain monitor. Rejected because it bypasses the graceful shutdown (no DRAINING, no deregister_worker, no scope cleanup) and would leave jobs stranded in `running` status.

### 5. `count_active_jobs` as a new Backend protocol method

**Decision:** Add `count_active_jobs(queues: list[str]) -> int` to the `Backend` protocol rather than using existing `list_jobs` with `JobFilter`.

**Rationale:** `list_jobs` returns full `JobRow` objects (wasteful for a count), accepts a single queue (not a list), and paginates. A dedicated count method is:
- Efficient (one round-trip, `count(*)` only, indexed)
- Multi-queue (matches the worker's queue subscription)
- Simple to implement on both backends

**Alternative considered:** Loop `list_jobs(queue=q, active=True, limit=1)` per queue. Rejected for efficiency reasons (N round-trips, full row materialization).

### 6. `drain_failures` counter on `WorkerDeps`

**Decision:** Track job failures via a simple integer counter on `WorkerDeps`, incremented by `di_consumer_loop` when `dispatch_one_job` returns a non-success outcome. The outcome type is `AttemptOutcome = Literal["succeeded","failed","cancelled","scheduled"]` (`_consumer.py:77-82`). The failure set is `{"failed", "cancelled"}` — `"scheduled"` (snooze/retry/reservation-denial) is excluded because a retried job is not a drain failure. `"abandoned"` does not exist in `AttemptOutcome` (it exists only in the metrics-side `ConsumedOutcome` mapping, `dispatch.py:61-71`).

**Rationale:** The drain monitor needs to know if any jobs failed during the drain. A counter is the simplest possible mechanism — no event bus, no callback registration, no backend queries. The counter is only read by the drain monitor, so it's zero overhead in non-idle mode.

**Known limitations:** The counter is worker-local. It does not capture deadline-exceeded sweep failures, other-worker failures, or actor-not-found cycles. See the "Known limitations" section in the workers.md documentation sketch above.

**Alternative considered:** Query the backend for failed jobs after drain. Rejected because it requires knowing which jobs were processed during this drain session (hard to determine without tracking anyway), and it adds a round-trip after shutdown.

### 7. `run_until_drained()` stays as-is

**Decision:** The existing `InMemoryBackend.run_until_drained()` remains unchanged. It is not modified to work with Postgres.

**Rationale:** `worker_main(until_idle=True)` is the production-grade drain mechanism that works with any backend (Postgres, InMemoryBackend, future backends). `run_until_drained()` is a test-only helper for deterministic in-memory execution — making it work with Postgres would require reimplementing the entire producer/consumer/dispatch pipeline, which is exactly what `worker_main` already does.

### 8. TOCTOU between final count check and shutdown trigger

**Decision:** Accept the residual window with documentation. After the settle window confirms idle, up to `idle_poll_interval` (default 1.0s) elapses before the drain monitor triggers `orchestrate_shutdown`. An enqueue landing in that window leaves a `pending` job while the worker exits 0.

**Rationale:** The settle window (`idle_settle_window`, default 2.0s) handles the common race (enqueue-before-detection). The residual window after the last `count==0` poll is bounded by `idle_poll_interval` and is inherent to poll-based drain detection. Closing it fully would require a re-check inside the DRAINING phase (after `producer_stop_event` is set, before declaring drained), which adds complexity for a narrow window. The sizing guidance is: `idle_settle_window` should be significantly larger than the producer's enqueue-to-NOTIFY latency (typically sub-millisecond for local PG), and `idle_poll_interval` should be small enough that the residual window is acceptable for the use case. For CI/batch workloads where the producer has finished enqueuing before the worker starts, this window does not exist.

**Alternative considered:** Add a final `count_active_jobs` re-check at the start of the drain-triggered DRAINING phase (after `producer_stop_event.set()`, before the CANCELLING phase). If non-zero, abort back to monitoring. Rejected for this version — it adds a new code path inside `orchestrate_shutdown` (which is currently shared unchanged with SIGTERM) and the residual window is acceptable for the finite-batch use case. Can be added in a follow-up if real-world TOCTOU incidents occur.

---

## Acceptance criteria

1. **`worker_main(settings, actor_registry=registry, until_idle=True)`** processes all jobs in the worker's subscribed queues and exits with the correct exit code (0/2/3).

2. **`taskq worker --actors myapp.actors:registry --until-idle`** starts a worker that drains and exits.

3. **`--max-runtime N`** (or `max_runtime=N` / `TASKQ_IDLE_MAX_RUNTIME=N`) forces exit with code 3 after N seconds.

4. **Default behavior is unchanged:** `worker_main(settings, actor_registry=registry)` and `taskq worker --actors myapp.actors:registry` run forever exactly as today.

5. **All existing tests pass** with two expected modifications to `TestMethodCount` in `tests/test_backend_protocol.py` (count 36→37, add `"count_active_jobs"` to the name set — see Task 1) and `FakeBackend` gaining a `count_active_jobs` stub. `BACKEND_PROTOCOL_VERSION` stays at 3 (no bump — additive change, loud failure on old implementations per `docs/architecture.md:143-149`).

6. **New unit tests** cover: `count_active_jobs` (both backends), settings fields, `drain_failures` counter, `dispatch_one_job` returns `AttemptOutcome`, `di_consumer_loop` outcome tracking (failure set `{"failed","cancelled"}`), `drain_monitor_loop` (idle detection, settle window, timeout, failure exit code, double-orchestration guard), `_main` wiring (including SIGTERM-during-drain both orders), CLI flags.

7. **New e2e tests** cover: clean drain → exit 0, drain with failures → exit 2, timeout → exit 3, scheduled jobs drain.

8. **Documentation** is updated: workers.md (until-idle section), cli.md (new flags), architecture.md (drain monitor component).

9. **Exit codes are documented** and distinct from existing exit codes (1 for startup errors).

10. **The graceful shutdown path is reused** — the drain monitor triggers `orchestrate_shutdown` (same path SIGTERM takes) and does NOT set `shutdown_event` itself; `orchestrate_shutdown`'s `finally` block sets it at the correct point after all phase work completes, preserving the proven phase ordering.

---

## Revision log

### 2026-07-29 — Post-review revision (verdict: NEEDS REWORK → revised)

Revised against the code review in `.review/spec-review.md` (1 Critical /
2 High / 5 Medium / 6 Low), applying the standing 1.0.0 directive:
breaking changes are made where the strictly-better design requires them,
with no shims or dual-path compatibility code, and documented downstream
needs (not current downstream usage) are the contract.

Design changes:

- **C1:** `dispatch_one_job` now returns `AttemptOutcome` (breaking change,
  owned explicitly). The drain failure set is `{"failed", "cancelled"}` —
  `"abandoned"` is not an `AttemptOutcome` value; `"scheduled"`
  (snooze/retry) is excluded because a retried job is not a drain failure.
- **H1:** `_trigger_drain_shutdown` mirrors the SIGTERM handler exactly —
  it creates the wrapper task and appends it to `orchestrator_holder` but
  does NOT set `shutdown_event`; `orchestrate_shutdown`'s `finally` sets
  it after all phase work, preserving the proven phase ordering against
  pool/scope teardown.
- **H2:** Double-orchestration guards on BOTH triggers (drain monitor and
  signal handler): skip when `orchestrator_holder` is non-empty or
  `deps.shutdown_phase is not ShutdownPhase.NONE`. Exit-code precedence is
  first-trigger-wins via `holder[0]`, tested in both orders.
- **M1:** `BACKEND_PROTOCOL_VERSION` stays at 3 per the documented
  convention; the two `TestMethodCount` pins are listed as expected
  modifications; the compatibility section was rewritten as an honest
  breaking-changes statement instead of a "nothing breaks" claim.
- **M2:** Residual post-settle TOCTOU window explicitly accepted and
  documented with sizing guidance (design decision 8).
- **M3/L3/L4:** Test plan made executable against the real harness:
  `set_shutdown=False` + dual-namespace `orchestrate_shutdown` patching;
  H2 ordering tests drive the real signal handler via the `install_fn`
  hook; mock `shutdown_phase` uses the real enum; wrapper-task awaits
  precede event assertions; e2e sketches set `TASKQ_UNTIL_IDLE`,
  `reload()` container attrs, use `sync_user_profile(fail_kind="permanent")`
  for real failures (`deliver_webhook` cannot fail), and share one
  `_start_idle_worker` helper.
- **M4/L6:** Actor-not-found wedge accepted and documented (operator uses
  `--max-runtime`); worker-local `drain_failures` blind spots (deadline
  sweep, other workers) documented; cron + until_idle startup WARNING added.
- **M5:** Downstream section rewritten per the directive: warden = no
  current impact (its `worker_main` daemon already captures exit codes);
  cennan = served by `_main(..., until_idle=True)` for its in-loop e2e
  worker (replacing `task.cancel()`), with its private-API dependency
  called out; aacrtool = verified driver of #53 (its spec's GAP #14),
  framed as adoption on landing, not migration of existing code.
