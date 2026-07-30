# Actor Deregistration — `client.actors.deregister()` and Cleanup for Ephemeral Deployments

**Date:** 2026-07-29
**Status:** Draft, revised post-review (2026-07-29)
**Issue:** [#56](https://github.com/rich/taskq/issues/56)

> **Scope note:** Issue #56 asks for `client.actors.deregister()` with
> defined safety semantics. This spec additionally builds the CLI command
> (`taskq actor-config deregister`) and admin UI page (`/admin/actors` with
> deregister button). These are justified under "operator surface" and are
> entirely additive (no changes to existing code paths), but they represent
> scope beyond the issue's literal ask and roughly half the plan's tasks.
> The issue author should confirm this scope expansion is desired.

---

## Goal

Provide a first-class actor deregistration API (`client.actors.deregister()`,
`taskq actor-config deregister`, admin UI button) with defined safety semantics
so that ephemeral, per-run actor deployments can clean up their `actor_config`
and orphaned `queues` rows without hand-rolled SQL. The default path refuses
deregistration while non-terminal jobs or enabled cron schedules reference the
actor; `force=True` documents and handles the consequences for terminal job
history, schedules, and stranded pending work.

## Non-goals

1. **No schema migration.** The existing schema has no FKs from `jobs.actor` or
   `cron_schedules.actor` to `actor_config.actor` — deregistration is pure
   application logic (a transactional set of checks + DELETEs). Adding FKs with
   `ON DELETE` actions would require a migration and risk lock contention on
   the hot `jobs` table; it is not needed for this feature.

2. **No automatic GC sweep.** Deregistration is an explicit operator/client
   action, not a background leader sweep. Ephemeral deployments know when their
   run is done; a sweep would need heuristics to decide liveness, which is
   application-specific.

3. **No soft-delete / tombstone column.** The `actor_config` row is deleted
   outright. Terminal job history (`jobs.actor` is a plain `text` column, not
   an FK) remains queryable by actor name after deregistration — that is the
   documented, intentional behavior.

4. **No re-registration resurrection.** If an actor is re-registered by a
   worker startup after deregistration, it creates a fresh `actor_config` row
   with seed values — the same behavior as any first-time registration.

5. **No changes to the drift-check semantics.** `_STRUCTURAL_FIELDS` and
   `ActorConfigDriftList` remain as-is. Deregistration is the cleanup path for
   the pattern the drift check funnels ephemeral deployments into; it does not
   weaken the drift check.

---

## Architecture Overview

### Current state

```
 ┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
 │  TaskQ      │     │  JobsClient      │     │  Backend        │
 │  (client)   │────▶│  (enqueue/get/   │────▶│  (PostgresBackend)
 │             │     │   list/cancel)   │     │                 │
 └─────────────┘     └──────────────────┘     └─────────────────┘
                             │                          │
                             │                          ▼
                             │                   ┌──────────────┐
                             │                   │  Postgres    │
                             │                   │  actor_config│
                             │                   │  queues      │
                             │                   │  jobs        │
                             │                   │  cron_schedules│
                             │                   └──────────────┘
                             │
                     ┌───────┴────────┐
                     │ actor_config_ops│  (list/get/set_capacity)
                     │ (ConnLike-level)│  NO delete
                     └────────────────┘

 CLI: taskq actor-config list/get/set/diff   (no deregister)
 Admin UI: queues/jobs/workers/schedules/...  (no actors page)
```

### Proposed state

```
 ┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
 │  TaskQ      │     │  JobsClient      │     │  Backend        │
 │  (client)   │────▶│  (enqueue/get/   │────▶│  (PostgresBackend)
 │  .actors ───┼───▶│   list/cancel)   │     │                 │
 └─────────────┘     └──────────────────┘     └─────────────────┘
        │                                              │
        ▼                                              ▼
 ┌──────────────┐                            ┌──────────────┐
 │ ActorsClient │───────────────────────────▶│  Postgres    │
 │ (deregister/ │                            │  actor_config│
 │  list/get/   │                            │  queues      │
 │  set_capacity)│                           │  jobs        │
 └──────────────┘                            │  cron_schedules│
        │                                    └──────────────┘
        ▼
 ┌────────────────┐
 │ actor_config_ops│  (list/get/set_capacity
 │ + deregister   │   + deregister_actor)
 └────────────────┘

 CLI: taskq actor-config list/get/set/diff/deregister
 Admin UI: /actors page with deregister button
```

### File structure

| Action | Path | Responsibility |
|--------|------|----------------|
| **Modify** | `src/taskq/worker/actor_config_ops.py` | Add `deregister_actor()` + `DeregisterResult` dataclass + SQL templates |
| **Create** | `src/taskq/client/_actors.py` | `ActorsClient` class — pool-wrapping facade over `actor_config_ops` |
| **Modify** | `src/taskq/client/_taskq.py` | Add `TaskQ.actors` property returning `ActorsClient` |
| **Modify** | `src/taskq/client/__init__.py` | Export `ActorsClient` |
| **Modify** | `src/taskq/exceptions.py` | Add `ActorDeregistrationError`, `ActorHasActiveJobsError`, `ActorHasEnabledSchedulesError` |
| **Modify** | `src/taskq/cli.py` | Add `taskq actor-config deregister` command |
| **Create** | `src/taskq/web/admin/actors.py` | Admin UI actors page + deregister POST route |
| **Create** | `src/taskq/web/templates/actors.html` | Jinja2 template for actors list + deregister button |
| **Modify** | `src/taskq/web/templates/_base.html` | Add "Actors" nav link |
| **Modify** | `src/taskq/__init__.py` | Export `ActorsClient` from public API |
| **Create** | `tests/test_actor_deregistration.py` | Integration tests for `deregister_actor` |
| **Create** | `tests/test_cli_actor_deregister.py` | CLI tests for `taskq actor-config deregister` |
| **Create** | `tests/test_actors_client.py` | Tests for `ActorsClient` |
| **Create** | `tests/test_web_admin_actors.py` | Admin UI actor page + deregister route tests |
| **Create** | `tests/e2e/test_actor_deregistration.py` | E2E: real worker, enqueue jobs, deregister, verify cleanup |
| **Modify** | `docs/guides/actors.md` | Document deregistration API + semantics |
| **Modify** | `docs/guides/cli.md` | Document `taskq actor-config deregister` command |
| **Modify** | `docs/guides/admin-ui.md` | Document actors page + deregister button |

---

## API Surface

### Exceptions (`src/taskq/exceptions.py`)

```python
class ActorDeregistrationError(TaskQError):
    """Base for actor deregistration refusals."""

    def __init__(self, actor: str, detail: str) -> None:
        self.actor = actor
        super().__init__(f"Cannot deregister actor {actor!r}: {detail}")


class ActorHasActiveJobsError(ActorDeregistrationError):
    """Non-terminal jobs reference the actor.

    Carries the count and statuses of the blocking jobs so the caller can
    decide whether to cancel them first or use force=True.
    """

    def __init__(
        self,
        actor: str,
        active_count: int,
        status_counts: dict[str, int],
    ) -> None:
        self.active_count = active_count
        self.status_counts = status_counts
        detail = (
            f"{active_count} non-terminal job(s) still reference this actor"
            f" (breakdown: {status_counts}). Cancel them first or pass"
            f" force=True to cancel pending/scheduled jobs automatically."
        )
        super().__init__(actor, detail)


class ActorHasEnabledSchedulesError(ActorDeregistrationError):
    """Enabled cron schedules reference the actor.

    Carries the schedule IDs so the caller can disable or delete them first.
    """

    def __init__(
        self,
        actor: str,
        schedule_ids: list[str],
    ) -> None:
        self.schedule_ids = schedule_ids
        detail = (
            f"{len(schedule_ids)} enabled cron schedule(s) reference this actor"
            f" (ids: {schedule_ids}). Disable or delete them first or pass"
            f" force=True to disable them automatically."
        )
        super().__init__(actor, detail)


class ActorNotFoundError(ActorDeregistrationError):
    """The actor_config row does not exist — nothing to deregister."""

    def __init__(self, actor: str) -> None:
        super().__init__(actor, "no stored actor_config row for this actor")
```

### Result dataclass (`src/taskq/worker/actor_config_ops.py`)

```python
@dataclass(frozen=True, slots=True)
class DeregisterResult:
    """Outcome of a deregister_actor call.

    All counts are non-negative integers. ``queue_purged`` is True only
    when the orphaned queue row was deleted (requires purge_queue=True
    AND no other actor_config row references the same queue).
    """

    actor: str
    queue: str
    actor_config_deleted: bool
    schedules_disabled: int
    jobs_cancelled: int
    terminal_jobs_remaining: int
    queue_purged: bool
```

### Ops-layer function (`src/taskq/worker/actor_config_ops.py`)

```python
_NON_TERMINAL_STATUSES: tuple[str, ...] = (
    "pending", "scheduled", "running",
)

_RUNNING_STATUS: str = "running"

_DEREGISTER_CHECK_ACTIVE_JOBS_SQL = """
SELECT status, count(*) AS cnt
  FROM "{schema}".jobs
 WHERE actor = $1 AND status = ANY($2::"{schema}".job_status[])
 GROUP BY status
""".strip()

_DEREGISTER_CHECK_SCHEDULES_SQL = """
SELECT id::text FROM "{schema}".cron_schedules
 WHERE actor = $1 AND enabled = true
""".strip()

_DEREGISTER_CANCEL_PENDING_SQL = """
UPDATE "{schema}".jobs
   SET status = 'cancelled',
       finished_at = now(),
       error_class = 'ActorDeregistered',
       error_message = 'Job cancelled by actor deregistration (force=True)'
 WHERE actor = $1
   AND status IN ('pending', 'scheduled')
""".strip()

_DEREGISTER_DISABLE_SCHEDULES_SQL = """
UPDATE "{schema}".cron_schedules
   SET enabled = false
 WHERE actor = $1 AND enabled = true
""".strip()

_DEREGISTER_DELETE_ACTOR_CONFIG_SQL = """
DELETE FROM "{schema}".actor_config WHERE actor = $1
RETURNING queue
""".strip()

_DEREGISTER_PURGE_QUEUE_SQL = """
DELETE FROM "{schema}".queues
 WHERE name = $1
   AND NOT EXISTS (
       SELECT 1 FROM "{schema}".actor_config WHERE queue = $1
   )
""".strip()

_DEREGISTER_COUNT_TERMINAL_SQL = """
SELECT count(*) FROM "{schema}".jobs
 WHERE actor = $1 AND status NOT IN ('pending', 'scheduled', 'running')
""".strip()


async def deregister_actor(
    conn: ConnLike,
    actor: str,
    *,
    force: bool = False,
    purge_queue: bool = False,
    schema: str = "taskq",
) -> DeregisterResult:
    """Deregister an actor: delete its ``actor_config`` row with safety checks.

    **Default (force=False):**
      1. Refuse if any non-terminal jobs (pending/scheduled/running) reference
         the actor — raises :class:`ActorHasActiveJobsError`.
      2. Refuse if any enabled cron schedules reference the actor — raises
         :class:`ActorHasEnabledSchedulesError`.
      3. Delete the ``actor_config`` row.
      4. Optionally purge the orphaned queue (if ``purge_queue=True`` and no
         other ``actor_config`` row references the same queue).

    **force=True:**
      1. Refuse if any *running* jobs reference the actor — raises
         :class:`ActorHasActiveJobsError` (running jobs are actively
         executing; their terminal-write path reads ``actor_config`` for
         ``result_ttl``, and deleting the row mid-execution loses the
         stored override — the ``COALESCE`` in the terminal-write SQL
         falls back to the ``@actor(...)`` literal TTL, which is a silent
         semantic change. More importantly, the dispatch query inner-joins
         ``actor_config``, so a running job that retries would be
         stranded).
      2. Cancel pending/scheduled jobs for this actor (mark as ``cancelled``
         with ``error_class='ActorDeregistered'``). They would be stranded
         anyway: the dispatch query inner-joins ``actor_config``, so without
         a row they would never be dispatched.
      3. Disable enabled cron schedules for this actor (set ``enabled=false``,
         not delete — the operator may want to re-enable if the actor is
         re-registered).
      4. Delete the ``actor_config`` row.
      5. Optionally purge the orphaned queue.

    **Terminal job history** (succeeded/failed/cancelled/crashed/abandoned
    jobs) is *never* deleted or modified. The ``jobs.actor`` column is plain
    ``text``, not a foreign key — terminal rows remain queryable by actor
    name after deregistration. The ``DeregisterResult.terminal_jobs_remaining``
    count tells the caller how many such rows exist.

    **Queue purge** only deletes the ``queues`` row when *no* remaining
    ``actor_config`` row references the same queue name. A shared queue
    (one used by multiple actors) is never purged. The queue row is
    metadata only (``mode``, ``max_concurrent``); deleting it does not
    affect already-queued jobs.

    The entire operation runs inside a single ``conn.transaction()`` block.
    If the actor has no stored ``actor_config`` row, raises
    :class:`ActorNotFoundError`.

    .. warning::

       **Concurrent enqueue / dispatch race (TOCTOU).** The transaction
       uses READ COMMITTED isolation. The safety checks (active-jobs,
       enabled-schedules) and the DELETE are separate statements within
       the same transaction. A job enqueued by a *concurrent* transaction
       that commits *after* the active-jobs check but *before* the DELETE
       will be stranded: the ``jobs`` INSERT does not require an
       ``actor_config`` row (no FK), and the dispatch query inner-joins
       ``actor_config``, so the job will never be dispatched. The same
       applies to cron-fired jobs and dispatch transitions.

       **Deregistration is best-effort against concurrent enqueue /
       dispatch.** Callers must **quiesce the actor first** — stop
       enqueuing, disable cron schedules, and wait for running jobs to
       reach a terminal state — *before* calling ``deregister``. This is
       the same operational discipline required for any shutdown
       sequence.

       After deregistration, any client can still ``enqueue()`` the dead
       actor name — the INSERT succeeds (no FK), the job sits ``pending``
       forever, invisible to dispatch. See "Enqueue after deregistration"
       in the docs guide.

    Parameters
    ----------
    conn:
        An asyncpg connection (or ConnLike). The caller is responsible for
        transaction boundaries if composing with other operations; however,
        this function wraps its work in ``conn.transaction()`` for
        self-contained use.
    actor:
        The actor name (primary key of ``actor_config``).
    force:
        If True, cancel pending/scheduled jobs and disable schedules instead
        of refusing. Still refuses if running jobs exist.
    purge_queue:
        If True, delete the orphaned ``queues`` row when no other
        ``actor_config`` references the same queue.
    schema:
        TaskQ schema name. Defaults to ``"taskq"``.
    """
```

### Client surface (`src/taskq/client/_actors.py`)

```python
class ActorsClient:
    """Pool-wrapping facade for actor configuration operations.

    Acquires a connection from the injected pool for each call, delegates
    to ``taskq.worker.actor_config_ops``, and returns the result. The
    caller must have opened the pool; this class does not manage its
    lifecycle.

    Parameters
    ----------
    pool:
        An open ``asyncpg.Pool``. The caller retains ownership.
    schema:
        TaskQ schema name. Defaults to ``"taskq"``.
    """

    def __init__(self, pool: "asyncpg.Pool", *, schema: str = "taskq") -> None: ...

    async def list(self) -> list[ActorConfigRow]:
        """List all stored actor_config rows. Delegates to list_actor_configs."""

    async def get(self, actor: str) -> ActorConfigRow | None:
        """Get one actor_config row. Delegates to get_actor_config."""

    async def set_capacity(
        self,
        actor: str,
        *,
        max_concurrent: int | None | Unset = UNSET,
        max_pending: int | None | Unset = UNSET,
        result_ttl: float | None | Unset = UNSET,
    ) -> ActorConfigRow | None:
        """Update capacity fields. Delegates to set_actor_config_capacity."""

    async def deregister(
        self,
        actor: str,
        *,
        force: bool = False,
        purge_queue: bool = False,
    ) -> DeregisterResult:
        """Deregister an actor. Delegates to deregister_actor.

        Raises ActorNotFoundError if the actor has no stored row.
        Raises ActorHasActiveJobsError if non-terminal jobs block (force=False)
        or running jobs block (force=True).
        Raises ActorHasEnabledSchedulesError if enabled schedules block
        (force=False only).

        For idempotent cleanup loops, wrap in try/except:

        .. code-block:: python

            try:
                await tq.actors.deregister(actor_name, force=True)
            except ActorNotFoundError:
                pass  # already deregistered
        """
```

### TaskQ client property (`src/taskq/client/_taskq.py`)

```python
class TaskQ:
    # ... existing code ...

    @property
    def actors(self) -> ActorsClient:
        """Actor configuration client — list, get, set capacity, deregister.

        Raises RuntimeError if called before ``open()`` or outside an
        ``async with`` block.
        """
        if self._actors_client is None:
            raise RuntimeError(
                "TaskQ is not open. Call 'await tq.open()' or use "
                "'async with TaskQ(...) as tq:'"
            )
        return self._actors_client
```

### CLI (`src/taskq/cli.py`)

```
taskq actor-config deregister <ACTOR> [--force] [--purge-queue]
```

- `<ACTOR>` — actor name (positional argument)
- `--force` — cancel pending/scheduled jobs, disable schedules, proceed despite non-terminal jobs (running jobs still block)
- `--purge-queue` — also delete the orphaned queues row if no other actor references it
- Exit code 0 on success, 1 on refusal (with error message), 1 on not found

On success, the CLI prints a summary line and a warning:

```
Deregistered actor 'my-actor.run-123': actor_config_deleted=True schedules_disabled=0 jobs_cancelled=0 terminal_jobs_remaining=3 queue_purged=False
WARNING: Actor 'my-actor.run-123' is now unregistered. Any future enqueue() to this actor name will create a stranded pending job that will never be dispatched. Stop enqueuing before deregistering.
```

### Admin UI (`src/taskq/web/admin/actors.py`)

```
GET  /admin/actors              — list all actor_config rows with job counts
POST /admin/actors/{actor}/deregister  — deregister with force + purge_queue params
```

The POST route requires `admin_actions_enabled=True` (same gate as schedule
run, job retry). CSRF-protected via `validate_csrf`. Form params:
- `force` — checkbox
- `purge_queue` — checkbox

---

## Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add actor deregistration with safety checks to the ops layer, CLI, client surface, and admin UI.

**Architecture:** Pure application logic (no migration) — a transactional function that checks for non-terminal jobs and enabled schedules, then deletes the actor_config row, optionally cancels pending jobs, disables schedules, and purges orphaned queues. Exposed via ActorsClient (pool wrapper), CLI, and admin UI.

**Tech Stack:** Python 3.12+, asyncpg, typer, FastAPI, Jinja2, pytest

---

### Task 1: Exceptions for deregistration refusals

**Files:**
- Modify: `src/taskq/exceptions.py` (add after `ActorConfigDriftList`, ~line 344)
- Test: `tests/test_exceptions.py` (add new test class)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_exceptions.py — add at end of file

class TestActorDeregistrationErrors:
    """Tests for the deregistration refusal exception hierarchy."""

    def test_actor_has_active_jobs_error_carries_counts(self) -> None:
        from taskq.exceptions import ActorHasActiveJobsError

        err = ActorHasActiveJobsError(
            actor="my-actor.run-123",
            active_count=3,
            status_counts={"pending": 2, "running": 1},
        )
        assert err.actor == "my-actor.run-123"
        assert err.active_count == 3
        assert err.status_counts == {"pending": 2, "running": 1}
        assert "3 non-terminal" in str(err)
        assert "force=True" in str(err)

    def test_actor_has_enabled_schedules_error_carries_ids(self) -> None:
        from taskq.exceptions import ActorHasEnabledSchedulesError

        err = ActorHasEnabledSchedulesError(
            actor="my-actor.run-123",
            schedule_ids=["sched-1", "sched-2"],
        )
        assert err.actor == "my-actor.run-123"
        assert err.schedule_ids == ["sched-1", "sched-2"]
        assert "2 enabled cron schedule" in str(err)
        assert "force=True" in str(err)

    def test_actor_not_found_error(self) -> None:
        from taskq.exceptions import ActorNotFoundError

        err = ActorNotFoundError("ghost-actor")
        assert err.actor == "ghost-actor"
        assert "no stored actor_config row" in str(err)

    def test_deregistration_errors_inherit_taskq_error(self) -> None:
        from taskq.exceptions import (
            ActorDeregistrationError,
            ActorHasActiveJobsError,
            ActorHasEnabledSchedulesError,
            ActorNotFoundError,
            TaskQError,
        )

        for cls in (
            ActorDeregistrationError,
            ActorHasActiveJobsError,
            ActorHasEnabledSchedulesError,
            ActorNotFoundError,
        ):
            assert issubclass(cls, TaskQError)

    def test_specific_errors_inherit_deregistration_error(self) -> None:
        from taskq.exceptions import (
            ActorDeregistrationError,
            ActorHasActiveJobsError,
            ActorHasEnabledSchedulesError,
            ActorNotFoundError,
        )

        for cls in (
            ActorHasActiveJobsError,
            ActorHasEnabledSchedulesError,
            ActorNotFoundError,
        ):
            assert issubclass(cls, ActorDeregistrationError)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_exceptions.py::TestActorDeregistrationErrors -v`
Expected: FAIL with `ImportError: cannot import name 'ActorDeregistrationError'`

- [ ] **Step 3: Implement the exceptions**

Add to `src/taskq/exceptions.py` after the `ActorConfigDriftList` class (after line 344):

```python
class ActorDeregistrationError(TaskQError):
    """Base for actor deregistration refusals."""

    def __init__(self, actor: str, detail: str) -> None:
        self.actor = actor
        super().__init__(f"Cannot deregister actor {actor!r}: {detail}")


class ActorHasActiveJobsError(ActorDeregistrationError):
    """Non-terminal jobs reference the actor.

    Carries the count and per-status breakdown of the blocking jobs so the
    caller can decide whether to cancel them first or use ``force=True``.
    """

    def __init__(
        self,
        actor: str,
        active_count: int,
        status_counts: dict[str, int],
    ) -> None:
        self.active_count = active_count
        self.status_counts = status_counts
        detail = (
            f"{active_count} non-terminal job(s) still reference this actor"
            f" (breakdown: {status_counts}). Cancel them first or pass"
            f" force=True to cancel pending/scheduled jobs automatically."
        )
        super().__init__(actor, detail)


class ActorHasEnabledSchedulesError(ActorDeregistrationError):
    """Enabled cron schedules reference the actor.

    Carries the schedule IDs so the caller can disable or delete them first.
    """

    def __init__(
        self,
        actor: str,
        schedule_ids: list[str],
    ) -> None:
        self.schedule_ids = schedule_ids
        detail = (
            f"{len(schedule_ids)} enabled cron schedule(s) reference this actor"
            f" (ids: {schedule_ids}). Disable or delete them first or pass"
            f" force=True to disable them automatically."
        )
        super().__init__(actor, detail)


class ActorNotFoundError(ActorDeregistrationError):
    """The actor_config row does not exist — nothing to deregister."""

    def __init__(self, actor: str) -> None:
        super().__init__(actor, "no stored actor_config row for this actor")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_exceptions.py::TestActorDeregistrationErrors -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/taskq/exceptions.py tests/test_exceptions.py
git commit -m "feat: add actor deregistration exception hierarchy"
```

---

### Task 2: `deregister_actor` ops-layer function — safety checks (force=False path)

**Files:**
- Modify: `src/taskq/worker/actor_config_ops.py` (add `DeregisterResult`, SQL, `deregister_actor`)
- Test: `tests/test_actor_deregistration.py` (new file, integration-tier)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_actor_deregistration.py

"""Tests for ``deregister_actor``: the operator surface for removing
actor_config rows with defined safety semantics.

Integration-tier (real Postgres) because the safety checks are set-based
SQL that must execute correctly against the real schema — a fake connection
would only prove the query string looks right.
"""

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.exceptions import (
    ActorHasActiveJobsError,
    ActorHasEnabledSchedulesError,
    ActorNotFoundError,
)
from taskq.worker.actor_config import ActorConfig
from taskq.worker.actor_config_ops import (
    DeregisterResult,
    deregister_actor,
    get_actor_config,
)
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _ensure_schema(conn: asyncpg.Connection, schema: str) -> None:
    """Apply real migrations to create the full TaskQ schema.

    Uses ``taskq.migrate.apply_pending`` — the same pattern as
    ``tests/test_taskq_client.py`` and ``taskq.testing.fixtures`` — so the
    test schema is structurally identical to production. This prevents
    schema-drift bugs (e.g. enum vs text column types, missing columns)
    that a hand-rolled minimal schema would mask.
    """
    from taskq.migrate import apply_pending

    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)

```

- [ ] **Step 2: Write the force=False tests (refusal paths)**

Add to the same test file:

```python
from uuid import uuid4


async def _insert_job(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    status: str = "pending",
) -> str:
    """Insert a minimal job row with the given status.

    Uses the real migrated schema (via ``apply_pending``), so all NOT NULL
    columns without defaults must be specified — ``max_attempts`` and
    ``retry_kind`` have no defaults in the real schema.
    """
    job_id = uuid4()
    await conn.execute(
        f"""
        INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, max_attempts, retry_kind)
        VALUES ($1, $2, 'default', '{{}}'::jsonb, $3::"{schema}".job_status, 3, 'transient')
        """,
        job_id,
        actor,
        status,
    )
    return str(job_id)


async def _insert_schedule(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    enabled: bool = True,
) -> str:
    sched_id = uuid4()
    await conn.execute(
        f"""
        INSERT INTO "{schema}".cron_schedules (id, actor, cron_expr, enabled, next_fire_at)
        VALUES ($1, $2, '0 * * * *', $3, now())
        """,
        sched_id,
        actor,
        enabled,
    )
    return str(sched_id)


async def test_deregister_raises_not_found_for_unknown_actor(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    with pytest.raises(ActorNotFoundError, match="no stored actor_config row"):
        await deregister_actor(pg_conn, "ghost", schema=schema)


async def test_deregister_succeeds_when_no_jobs_or_schedules(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="clean-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )

    result = await deregister_actor(pg_conn, "clean-actor", schema=schema)

    assert isinstance(result, DeregisterResult)
    assert result.actor == "clean-actor"
    assert result.actor_config_deleted is True
    assert result.schedules_disabled == 0
    assert result.jobs_cancelled == 0
    assert result.terminal_jobs_remaining == 0
    assert result.queue_purged is False

    # Row is gone
    assert await get_actor_config(pg_conn, "clean-actor", schema=schema) is None


async def test_deregister_refuses_with_pending_jobs(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="busy-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, "busy-actor", "pending")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "busy-actor", schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"pending": 1}

    # Row is still there
    assert await get_actor_config(pg_conn, "busy-actor", schema=schema) is not None


async def test_deregister_refuses_with_running_jobs(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="running-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, "running-actor", "running")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "running-actor", schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"running": 1}


async def test_deregister_refuses_with_enabled_schedules(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="scheduled-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    sched_id = await _insert_schedule(pg_conn, schema, "scheduled-actor", enabled=True)

    with pytest.raises(ActorHasEnabledSchedulesError) as exc_info:
        await deregister_actor(pg_conn, "scheduled-actor", schema=schema)

    assert sched_id in exc_info.value.schedule_ids

    # Row is still there
    assert await get_actor_config(pg_conn, "scheduled-actor", schema=schema) is not None


async def test_deregister_succeeds_with_disabled_schedules(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="disabled-sched-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_schedule(pg_conn, schema, "disabled-sched-actor", enabled=False)

    result = await deregister_actor(pg_conn, "disabled-sched-actor", schema=schema)

    assert result.actor_config_deleted is True
    assert result.schedules_disabled == 0


async def test_deregister_succeeds_with_terminal_jobs(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="done-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, "done-actor", "succeeded")
    await _insert_job(pg_conn, schema, "done-actor", "failed")

    result = await deregister_actor(pg_conn, "done-actor", schema=schema)

    assert result.actor_config_deleted is True
    assert result.terminal_jobs_remaining == 2
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_actor_deregistration.py -v`
Expected: FAIL with `ImportError: cannot import name 'deregister_actor'`

- [ ] **Step 4: Implement `deregister_actor` (force=False path)**

Add to `src/taskq/worker/actor_config_ops.py`:

```python
# Add to imports section:
from taskq.exceptions import (
    ActorHasActiveJobsError,
    ActorHasEnabledSchedulesError,
    ActorNotFoundError,
)

# Add DeregisterResult to __all__:
__all__ = [
    "UNSET",
    "ActorConfigRow",
    "DeregisterResult",
    "Unset",
    "deregister_actor",
    "get_actor_config",
    "list_actor_configs",
    "set_actor_config_capacity",
]

# Add after the ActorConfigRow dataclass:

@dataclass(frozen=True, slots=True)
class DeregisterResult:
    """Outcome of a deregister_actor call."""

    actor: str
    queue: str
    actor_config_deleted: bool
    schedules_disabled: int
    jobs_cancelled: int
    terminal_jobs_remaining: int
    queue_purged: bool


# SQL templates (as defined in the API surface section above)

_NON_TERMINAL_STATUSES: tuple[str, ...] = ("pending", "scheduled", "running")

_DEREGISTER_CHECK_ACTIVE_JOBS_SQL = """
SELECT status, count(*) AS cnt
  FROM "{schema}".jobs
 WHERE actor = $1 AND status = ANY($2::"{schema}".job_status[])
 GROUP BY status
""".strip()

_DEREGISTER_CHECK_SCHEDULES_SQL = """
SELECT id::text FROM "{schema}".cron_schedules
 WHERE actor = $1 AND enabled = true
""".strip()

_DEREGISTER_DELETE_ACTOR_CONFIG_SQL = """
DELETE FROM "{schema}".actor_config WHERE actor = $1
RETURNING queue
""".strip()

_DEREGISTER_COUNT_TERMINAL_SQL = """
SELECT count(*) FROM "{schema}".jobs
 WHERE actor = $1 AND status NOT IN ('pending', 'scheduled', 'running')
""".strip()


async def deregister_actor(
    conn: ConnLike,
    actor: str,
    *,
    force: bool = False,
    purge_queue: bool = False,
    schema: str = "taskq",
) -> DeregisterResult:
    """Deregister an actor: delete its actor_config row with safety checks."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    # force=False path only — force=True path added in Task 3
    async with conn.transaction():
        # 1. Check for non-terminal jobs
        active_rows = await conn.fetch(
            _DEREGISTER_CHECK_ACTIVE_JOBS_SQL.format(schema=schema),
            actor,
            list(_NON_TERMINAL_STATUSES),
        )
        if active_rows:
            status_counts = {row["status"]: row["cnt"] for row in active_rows}
            active_count = sum(status_counts.values())
            raise ActorHasActiveJobsError(actor, active_count, status_counts)

        # 2. Check for enabled schedules
        schedule_rows = await conn.fetch(
            _DEREGISTER_CHECK_SCHEDULES_SQL.format(schema=schema),
            actor,
        )
        if schedule_rows:
            schedule_ids = [row["id"] for row in schedule_rows]
            raise ActorHasEnabledSchedulesError(actor, schedule_ids)

        # 3. Delete the actor_config row
        deleted_rows = await conn.fetch(
            _DEREGISTER_DELETE_ACTOR_CONFIG_SQL.format(schema=schema),
            actor,
        )
        if not deleted_rows:
            raise ActorNotFoundError(actor)

        queue_name = deleted_rows[0]["queue"]

        # 4. Count terminal jobs remaining
        terminal_count = await conn.fetchval(
            _DEREGISTER_COUNT_TERMINAL_SQL.format(schema=schema),
            actor,
        )

        # 5. Optionally purge queue (implemented in Task 3 Step 3)
        queue_purged = False

    return DeregisterResult(
        actor=actor,
        queue=queue_name,
        actor_config_deleted=True,
        schedules_disabled=0,
        jobs_cancelled=0,
        terminal_jobs_remaining=terminal_count or 0,
        queue_purged=queue_purged,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_actor_deregistration.py -v`
Expected: PASS (7 tests)

- [ ] **Step 6: Commit**

```bash
git add src/taskq/worker/actor_config_ops.py tests/test_actor_deregistration.py
git commit -m "feat: add deregister_actor with force=False safety checks"
```

---

### Task 3: `deregister_actor` force=True path

**Files:**
- Modify: `src/taskq/worker/actor_config_ops.py` (extend `deregister_actor`)
- Test: `tests/test_actor_deregistration.py` (add force=True tests)

- [ ] **Step 1: Write the failing tests for force=True**

Add to `tests/test_actor_deregistration.py`:

```python
async def test_deregister_force_cancels_pending_and_disables_schedules(
    pg_conn: asyncpg.Connection,
) -> None:
    """force=True cancels pending/scheduled jobs and disables schedules."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="force-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, "force-actor", "pending")
    await _insert_job(pg_conn, schema, "force-actor", "scheduled")
    await _insert_schedule(pg_conn, schema, "force-actor", enabled=True)

    result = await deregister_actor(pg_conn, "force-actor", force=True, schema=schema)

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 2
    assert result.schedules_disabled == 1

    # Verify jobs are cancelled
    cancelled = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = 'force-actor' AND status = 'cancelled'"
    )
    assert cancelled == 2

    # Verify schedule is disabled
    enabled = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".cron_schedules WHERE actor = 'force-actor' AND enabled = true"
    )
    assert enabled == 0

    # Row is gone
    assert await get_actor_config(pg_conn, "force-actor", schema=schema) is None


async def test_deregister_force_refuses_with_running_jobs(
    pg_conn: asyncpg.Connection,
) -> None:
    """force=True still refuses if running jobs exist."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="running-force-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, "running-force-actor", "running")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "running-force-actor", force=True, schema=schema)

    # Only running jobs in the breakdown
    assert exc_info.value.status_counts == {"running": 1}

    # Row is still there
    assert await get_actor_config(pg_conn, "running-force-actor", schema=schema) is not None


async def test_deregister_force_with_running_and_pending_only_reports_running(
    pg_conn: asyncpg.Connection,
) -> None:
    """force=True: pending jobs are OK, only running blocks. But the check
    should only report running in the error (pending would be cancelled)."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="mixed-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, "mixed-actor", "pending")
    await _insert_job(pg_conn, schema, "mixed-actor", "running")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "mixed-actor", force=True, schema=schema)

    # Only running is reported (pending would be auto-cancelled)
    assert "running" in exc_info.value.status_counts
    assert "pending" not in exc_info.value.status_counts


async def test_deregister_force_keeps_terminal_history(
    pg_conn: asyncpg.Connection,
) -> None:
    """force=True does not delete or modify terminal jobs."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="hist-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, "hist-actor", "succeeded")
    await _insert_job(pg_conn, schema, "hist-actor", "failed")
    await _insert_job(pg_conn, schema, "hist-actor", "pending")

    result = await deregister_actor(pg_conn, "hist-actor", force=True, schema=schema)

    assert result.terminal_jobs_remaining == 2
    assert result.jobs_cancelled == 1  # only the pending one

    # Terminal jobs are still there
    succeeded = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = 'hist-actor' AND status = 'succeeded'"
    )
    assert succeeded == 1
    failed = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = 'hist-actor' AND status = 'failed'"
    )
    assert failed == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_actor_deregistration.py -k force -v`
Expected: FAIL (force=True not implemented yet — pending jobs not cancelled)

- [ ] **Step 3: Implement the force=True path**

Replace the `deregister_actor` function in `src/taskq/worker/actor_config_ops.py` with the full implementation:

```python
_RUNNING_STATUS: str = "running"

_DEREGISTER_CANCEL_PENDING_SQL = """
UPDATE "{schema}".jobs
   SET status = 'cancelled',
       finished_at = now(),
       error_class = 'ActorDeregistered',
       error_message = 'Job cancelled by actor deregistration (force=True)'
 WHERE actor = $1
   AND status IN ('pending', 'scheduled')
""".strip()

_DEREGISTER_DISABLE_SCHEDULES_SQL = """
UPDATE "{schema}".cron_schedules
   SET enabled = false
 WHERE actor = $1 AND enabled = true
""".strip()


async def deregister_actor(
    conn: ConnLike,
    actor: str,
    *,
    force: bool = False,
    purge_queue: bool = False,
    schema: str = "taskq",
) -> DeregisterResult:
    """Deregister an actor: delete its actor_config row with safety checks.

    See the API surface section in docs/specs/2026-07-29-actor-deregistration.md
    for the full semantics documentation.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    async with conn.transaction():
        if not force:
            # force=False: refuse if ANY non-terminal jobs exist
            active_rows = await conn.fetch(
                _DEREGISTER_CHECK_ACTIVE_JOBS_SQL.format(schema=schema),
                actor,
                list(_NON_TERMINAL_STATUSES),
            )
            if active_rows:
                status_counts = {row["status"]: row["cnt"] for row in active_rows}
                active_count = sum(status_counts.values())
                raise ActorHasActiveJobsError(actor, active_count, status_counts)

            # Refuse if enabled schedules exist
            schedule_rows = await conn.fetch(
                _DEREGISTER_CHECK_SCHEDULES_SQL.format(schema=schema),
                actor,
            )
            if schedule_rows:
                schedule_ids = [row["id"] for row in schedule_rows]
                raise ActorHasEnabledSchedulesError(actor, schedule_ids)

            jobs_cancelled = 0
            schedules_disabled = 0
        else:
            # force=True: refuse only if RUNNING jobs exist
            running_rows = await conn.fetch(
                _DEREGISTER_CHECK_ACTIVE_JOBS_SQL.format(schema=schema),
                actor,
                [_RUNNING_STATUS],
            )
            if running_rows:
                status_counts = {row["status"]: row["cnt"] for row in running_rows}
                active_count = sum(status_counts.values())
                raise ActorHasActiveJobsError(actor, active_count, status_counts)

            # Cancel pending/scheduled jobs
            cancel_result = await conn.execute(
                _DEREGISTER_CANCEL_PENDING_SQL.format(schema=schema),
                actor,
            )
            # asyncpg returns "UPDATE N" — parse the count
            jobs_cancelled = int(cancel_result.split()[-1]) if cancel_result else 0

            # Disable enabled schedules
            disable_result = await conn.execute(
                _DEREGISTER_DISABLE_SCHEDULES_SQL.format(schema=schema),
                actor,
            )
            schedules_disabled = int(disable_result.split()[-1]) if disable_result else 0

        # Delete the actor_config row
        deleted_rows = await conn.fetch(
            _DEREGISTER_DELETE_ACTOR_CONFIG_SQL.format(schema=schema),
            actor,
        )
        if not deleted_rows:
            raise ActorNotFoundError(actor)

        queue_name = deleted_rows[0]["queue"]

        # Count terminal jobs remaining
        terminal_count = await conn.fetchval(
            _DEREGISTER_COUNT_TERMINAL_SQL.format(schema=schema),
            actor,
        )

        # Optionally purge queue
        queue_purged = False
        if purge_queue:
            purge_result = await conn.execute(
                _DEREGISTER_PURGE_QUEUE_SQL.format(schema=schema),
                queue_name,
            )
            queue_purged = purge_result == "DELETE 1"

    return DeregisterResult(
        actor=actor,
        queue=queue_name,
        actor_config_deleted=True,
        schedules_disabled=schedules_disabled,
        jobs_cancelled=jobs_cancelled,
        terminal_jobs_remaining=terminal_count or 0,
        queue_purged=queue_purged,
    )
```

Also add the `_DEREGISTER_PURGE_QUEUE_SQL` constant:

```python
_DEREGISTER_PURGE_QUEUE_SQL = """
DELETE FROM "{schema}".queues
 WHERE name = $1
   AND NOT EXISTS (
       SELECT 1 FROM "{schema}".actor_config WHERE queue = $1
   )
""".strip()
```

- [ ] **Step 4: Run all deregistration tests**

Run: `uv run pytest tests/test_actor_deregistration.py -v`
Expected: PASS (all tests including force=True path)

- [ ] **Step 5: Commit**

```bash
git add src/taskq/worker/actor_config_ops.py tests/test_actor_deregistration.py
git commit -m "feat: add force=True path to deregister_actor"
```

---

### Task 4: Queue purge tests

**Files:**
- Modify: `tests/test_actor_deregistration.py` (add purge_queue tests)

- [ ] **Step 1: Write the failing tests for purge_queue**

Add to `tests/test_actor_deregistration.py`:

```python
async def test_deregister_purge_queue_deletes_orphaned_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    """purge_queue=True deletes the queue row when no other actor uses it."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="solo-actor", max_concurrent=1, queue="solo-queue")],
        schema=schema,
    )
    # Create the queue row
    await pg_conn.execute(
        f"INSERT INTO \"{schema}\".queues (name) VALUES ('solo-queue') ON CONFLICT DO NOTHING"
    )

    result = await deregister_actor(
        pg_conn, "solo-actor", purge_queue=True, schema=schema
    )

    assert result.queue_purged is True
    assert result.queue == "solo-queue"

    # Queue row is gone
    queue_count = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".queues WHERE name = 'solo-queue'"
    )
    assert queue_count == 0


async def test_deregister_purge_queue_keeps_shared_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    """purge_queue=True does NOT delete the queue if another actor still uses it."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [
            ActorConfig(actor="actor-a", max_concurrent=1, queue="shared-queue"),
            ActorConfig(actor="actor-b", max_concurrent=1, queue="shared-queue"),
        ],
        schema=schema,
    )
    await pg_conn.execute(
        f"INSERT INTO \"{schema}\".queues (name) VALUES ('shared-queue') ON CONFLICT DO NOTHING"
    )

    result = await deregister_actor(
        pg_conn, "actor-a", purge_queue=True, schema=schema
    )

    assert result.queue_purged is False  # actor-b still uses it

    # Queue row is still there
    queue_count = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".queues WHERE name = 'shared-queue'"
    )
    assert queue_count == 1


async def test_deregister_without_purge_queue_keeps_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    """Default (purge_queue=False) does not touch the queue row."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="keep-queue-actor", max_concurrent=1, queue="kept-queue")],
        schema=schema,
    )
    await pg_conn.execute(
        f"INSERT INTO \"{schema}\".queues (name) VALUES ('kept-queue') ON CONFLICT DO NOTHING"
    )

    result = await deregister_actor(pg_conn, "keep-queue-actor", schema=schema)

    assert result.queue_purged is False

    # Queue row is still there
    queue_count = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".queues WHERE name = 'kept-queue'"
    )
    assert queue_count == 1
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_actor_deregistration.py -k purge -v`
Expected: PASS (the purge logic was already implemented in Task 3's step 3)

- [ ] **Step 3: Commit**

```bash
git add tests/test_actor_deregistration.py
git commit -m "test: add queue purge tests for deregister_actor"
```

---

### Task 5: ActorsClient — pool-wrapping facade

**Files:**
- Create: `src/taskq/client/_actors.py`
- Test: `tests/test_actors_client.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_actors_client.py

"""Tests for ActorsClient — the pool-wrapping facade over actor_config_ops.

These tests use a fake pool to verify the delegation wiring without
requiring real Postgres (the ops functions themselves are integration-tested
in test_actor_deregistration.py and test_actor_config_ops.py).
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from taskq.worker.actor_config_ops import (
    ActorConfigRow,
    DeregisterResult,
)

pytestmark = [pytest.mark.asyncio]


class _FakePool:
    """Minimal pool that yields a fake connection via async context manager."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def acquire(self) -> Any:
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=self._conn)
        cm.__aexit__ = AsyncMock(return_value=None)
        return cm


class _FakeConn:
    """Fake connection — just needs to be passable to the ops functions."""

    async def close(self) -> None: ...


async def test_actors_client_list_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.client._actors import ActorsClient

    conn = _FakeConn()
    pool = _FakePool(conn)
    client = ActorsClient(pool, schema="test_schema")

    # Patch list_actor_configs to verify delegation
    import taskq.client._actors as actors_mod

    mock_result = [
        ActorConfigRow(
            actor="a", max_concurrent=1, max_pending=None, queue="q",
            result_ttl=None, metadata={}, updated_at="2026-01-01"
        )
    ]
    monkeypatch.setattr(actors_mod, "list_actor_configs", AsyncMock(return_value=mock_result))
    result = await client.list()
    assert result == mock_result
    actors_mod.list_actor_configs.assert_called_once_with(
        conn, schema="test_schema"
    )


async def test_actors_client_deregister_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.client._actors import ActorsClient

    conn = _FakeConn()
    pool = _FakePool(conn)
    client = ActorsClient(pool, schema="test_schema")

    expected = DeregisterResult(
        actor="test-actor", queue="q", actor_config_deleted=True,
        schedules_disabled=0, jobs_cancelled=0,
        terminal_jobs_remaining=0, queue_purged=False,
    )

    import taskq.client._actors as actors_mod

    monkeypatch.setattr(actors_mod, "deregister_actor", AsyncMock(return_value=expected))
    result = await client.deregister("test-actor", force=True, purge_queue=True)
    assert result == expected
    actors_mod.deregister_actor.assert_called_once_with(
        conn, "test-actor", force=True, purge_queue=True, schema="test_schema"
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_actors_client.py -v`
Expected: FAIL with `ImportError: cannot import name 'ActorsClient'`

- [ ] **Step 3: Implement ActorsClient**

Create `src/taskq/client/_actors.py`:

```python
"""ActorsClient — pool-wrapping facade for actor configuration operations.

Provides a typed surface for listing, inspecting, tuning, and deregistering
stored ``actor_config`` rows. Each method acquires a connection from the
injected pool, delegates to ``taskq.worker.actor_config_ops``, and returns
the result.
"""

from typing import TYPE_CHECKING

import structlog

from taskq.worker.actor_config_ops import (
    UNSET,
    ActorConfigRow,
    DeregisterResult,
    Unset,
    deregister_actor,
    get_actor_config,
    list_actor_configs,
    set_actor_config_capacity,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = ["ActorsClient"]

logger = structlog.get_logger("taskq.client._actors")


class ActorsClient:
    """Pool-wrapping facade for actor configuration operations.

    Acquires a connection from the injected pool for each call, delegates
    to ``taskq.worker.actor_config_ops``, and returns the result. The
    caller must have opened the pool; this class does not manage its
    lifecycle.

    Parameters
    ----------
    pool:
        An open ``asyncpg.Pool``. The caller retains ownership.
    schema:
        TaskQ schema name. Defaults to ``"taskq"``.
    """

    def __init__(self, pool: "asyncpg.Pool", *, schema: str = "taskq") -> None:
        self._pool = pool
        self._schema = schema

    async def list(self) -> list[ActorConfigRow]:
        """List all stored actor_config rows, ordered by actor name."""
        async with self._pool.acquire() as conn:
            return await list_actor_configs(conn, schema=self._schema)

    async def get(self, actor: str) -> ActorConfigRow | None:
        """Get one actor_config row, or ``None`` if not found."""
        async with self._pool.acquire() as conn:
            return await get_actor_config(conn, actor, schema=self._schema)

    async def set_capacity(
        self,
        actor: str,
        *,
        max_concurrent: int | None | Unset = UNSET,
        max_pending: int | None | Unset = UNSET,
        result_ttl: float | None | Unset = UNSET,
    ) -> ActorConfigRow | None:
        """Update capacity fields on an existing actor_config row."""
        async with self._pool.acquire() as conn:
            return await set_actor_config_capacity(
                conn,
                actor,
                max_concurrent=max_concurrent,
                max_pending=max_pending,
                result_ttl=result_ttl,
                schema=self._schema,
            )

    async def deregister(
        self,
        actor: str,
        *,
        force: bool = False,
        purge_queue: bool = False,
    ) -> DeregisterResult:
        """Deregister an actor with safety checks.

        See :func:`taskq.worker.actor_config_ops.deregister_actor` for
        the full semantics.
        """
        async with self._pool.acquire() as conn:
            return await deregister_actor(
                conn,
                actor,
                force=force,
                purge_queue=purge_queue,
                schema=self._schema,
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_actors_client.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/taskq/client/_actors.py tests/test_actors_client.py
git commit -m "feat: add ActorsClient pool-wrapping facade"
```

---

### Task 6: TaskQ.actors property

**Files:**
- Modify: `src/taskq/client/_taskq.py` (add `actors` property, create `_actors_client` in `open()`)
- Modify: `src/taskq/client/__init__.py` (export `ActorsClient`)
- Test: `tests/test_taskq_client.py` (add test)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_taskq_client.py`:

```python
async def test_taskq_actors_property_returns_actors_client(
    module_pg_schema: "ModulePgSchema",
) -> None:
    """TaskQ.actors returns an ActorsClient bound to the same pool and schema.

    Uses the module-scoped ``module_pg_schema`` fixture (already migrated
    via ``apply_pending``). ``ModulePgSchema`` is a NamedTuple with
    ``.schema_name`` and ``.pg_dsn`` fields.
    """
    from taskq import TaskQ
    from taskq.client._actors import ActorsClient
    from taskq.testing.fixtures import ModulePgSchema  # noqa: F401

    async with TaskQ(
        dsn=module_pg_schema.pg_dsn,
        schema=module_pg_schema.schema_name,
    ) as tq:
        client = tq.actors
        assert isinstance(client, ActorsClient)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_taskq_client.py::test_taskq_actors_property_returns_actors_client -v`
Expected: FAIL with `AttributeError: 'TaskQ' object has no attribute 'actors'`

- [ ] **Step 3: Implement the `actors` property**

In `src/taskq/client/_taskq.py`, add import at the top:

```python
from taskq.client._actors import ActorsClient
```

Add to `__all__`:

```python
__all__ = ["EventRow", "JobEvent", "TaskQ", "ActorsClient"]
```

In `TaskQ.__init__`, add:

```python
self._actors_client: ActorsClient | None = None
```

In `TaskQ.open()`, after the `self._client = JobsClient(...)` line, add:

```python
self._actors_client = ActorsClient(pool, schema=self._schema)
```

In `TaskQ.close()`, add after `self._client = None`:

```python
self._actors_client = None
```

Add the property after `_require_open`:

```python
@property
def actors(self) -> ActorsClient:
    """Actor configuration client — list, get, set capacity, deregister.

    Raises RuntimeError if called before ``open()`` or outside an
    ``async with`` block.
    """
    if self._actors_client is None:
        raise RuntimeError(
            "TaskQ is not open. Call 'await tq.open()' or use "
            "'async with TaskQ(...) as tq:'"
        )
    return self._actors_client
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_taskq_client.py::test_taskq_actors_property_returns_actors_client -v`
Expected: PASS

- [ ] **Step 5: Update client __init__.py exports**

In `src/taskq/client/__init__.py`, add `ActorsClient` to the exports:

```python
from taskq.client._actors import ActorsClient
```

Add `"ActorsClient"` to `__all__`.

- [ ] **Step 6: Commit**

```bash
git add src/taskq/client/_taskq.py src/taskq/client/__init__.py tests/test_taskq_client.py
git commit -m "feat: add TaskQ.actors property returning ActorsClient"
```

---

### Task 7: CLI `taskq actor-config deregister` command

**Files:**
- Modify: `src/taskq/cli.py` (add `actor_config_deregister` command)
- Test: `tests/test_cli_actor_deregister.py` (new file)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_cli_actor_deregister.py

"""Tests for `taskq actor-config deregister` CLI command.

Monkeypatches the ops function and asyncpg.connect to pin the CLI's
argument parsing, error handling, and output shape without requiring
real Postgres (integration coverage is in test_actor_deregistration.py).
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

from taskq.cli import app
from taskq.worker.actor_config_ops import DeregisterResult

runner = CliRunner()


def _patch_deregister(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: DeregisterResult | None = None,
    raises: Exception | None = None,
) -> dict[str, Any]:
    """Fake asyncpg.connect + deregister_actor; return captured call kwargs."""
    captured: dict[str, Any] = {}

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    async def fake_deregister(conn: Any, actor: str, **kwargs: Any) -> Any:
        captured["actor"] = actor
        captured["kwargs"] = kwargs
        if raises is not None:
            raise raises
        return result or DeregisterResult(
            actor=actor, queue="default", actor_config_deleted=True,
            schedules_disabled=0, jobs_cancelled=0,
            terminal_jobs_remaining=0, queue_purged=False,
        )

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.deregister_actor", fake_deregister)
    return captured


def test_deregister_default_no_force_no_purge(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_deregister(monkeypatch)
    result = runner.invoke(app, ["actor-config", "deregister", "my-actor.run-123"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["actor"] == "my-actor.run-123"
    assert captured["kwargs"]["force"] is False
    assert captured["kwargs"]["purge_queue"] is False


def test_deregister_force_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_deregister(monkeypatch)
    result = runner.invoke(app, ["actor-config", "deregister", "my-actor", "--force"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["kwargs"]["force"] is True


def test_deregister_purge_queue_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_deregister(monkeypatch)
    result = runner.invoke(
        app, ["actor-config", "deregister", "my-actor", "--purge-queue"]
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["kwargs"]["purge_queue"] is True


def test_deregister_not_found_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.exceptions import ActorNotFoundError

    _patch_deregister(monkeypatch, raises=ActorNotFoundError("ghost"))
    result = runner.invoke(app, ["actor-config", "deregister", "ghost"])
    assert result.exit_code == 1
    assert "no stored actor_config row" in result.stderr


def test_deregister_active_jobs_error_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.exceptions import ActorHasActiveJobsError

    _patch_deregister(
        monkeypatch,
        raises=ActorHasActiveJobsError(
            "busy", active_count=3, status_counts={"pending": 2, "running": 1}
        ),
    )
    result = runner.invoke(app, ["actor-config", "deregister", "busy"])
    assert result.exit_code == 1
    assert "non-terminal" in result.stderr
    assert "force=True" in result.stderr


def test_deregister_schedules_error_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.exceptions import ActorHasEnabledSchedulesError

    _patch_deregister(
        monkeypatch,
        raises=ActorHasEnabledSchedulesError("sched-actor", ["s1", "s2"]),
    )
    result = runner.invoke(app, ["actor-config", "deregister", "sched-actor"])
    assert result.exit_code == 1
    assert "enabled cron schedule" in result.stderr


def test_deregister_output_shows_result(monkeypatch: pytest.MonkeyPatch) -> None:
    result = DeregisterResult(
        actor="my-actor", queue="my-queue", actor_config_deleted=True,
        schedules_disabled=2, jobs_cancelled=5,
        terminal_jobs_remaining=10, queue_purged=True,
    )
    _patch_deregister(monkeypatch, result=result)
    output = runner.invoke(app, ["actor-config", "deregister", "my-actor", "--force", "--purge-queue"])
    assert output.exit_code == 0
    assert "deregistered" in output.output.lower()
    assert "schedules_disabled=2" in output.output
    assert "jobs_cancelled=5" in output.output
    assert "terminal_jobs_remaining=10" in output.output
    assert "queue_purged=true" in output.output.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cli_actor_deregister.py -v`
Expected: FAIL (command doesn't exist)

- [ ] **Step 3: Implement the CLI command**

In `src/taskq/cli.py`, add `deregister_actor` to the import from `actor_config_ops`:

```python
from taskq.worker.actor_config_ops import (
    UNSET,
    ActorConfigRow,
    DeregisterResult,
    Unset,
    deregister_actor,
    get_actor_config,
    list_actor_configs,
    set_actor_config_capacity,
)
```

Also add the exception imports:

```python
from taskq.exceptions import (
    ActorConfigDriftList,
    ActorDeregistrationError,
)
```

Add the command after the `actor_config_set` / `_actor_config_set` functions (around line 518):

```python
@actor_config_app.command("deregister")
def actor_config_deregister(
    actor: Annotated[str, typer.Argument(help="Actor name to deregister.")],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Cancel pending/scheduled jobs and disable enabled cron schedules"
            " instead of refusing. Running jobs still block deregistration.",
        ),
    ] = False,
    purge_queue: Annotated[
        bool,
        typer.Option(
            "--purge-queue",
            help="Also delete the orphaned queues row if no other actor_config"
            " references the same queue.",
        ),
    ] = False,
) -> None:
    """Deregister an actor: delete its actor_config row with safety checks.

    By default refuses if non-terminal jobs or enabled cron schedules
    reference the actor. Use --force to cancel pending/scheduled jobs and
    disable schedules. Running jobs always block (force or not). Use
    --purge-queue to also delete the queues row if no other actor uses it.
    """
    settings = TaskQSettings.load()
    asyncio.run(_actor_config_deregister(settings, actor, force, purge_queue))


async def _actor_config_deregister(
    settings: TaskQSettings,
    actor: str,
    force: bool,
    purge_queue: bool,
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        result = await deregister_actor(
            conn,
            actor,
            force=force,
            purge_queue=purge_queue,
            schema=settings.schema_name,
        )
    except (ActorDeregistrationError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    finally:
        await conn.close()

    typer.echo(
        f"Deregistered actor {result.actor!r}:"
        f" actor_config_deleted={result.actor_config_deleted}"
        f" schedules_disabled={result.schedules_disabled}"
        f" jobs_cancelled={result.jobs_cancelled}"
        f" terminal_jobs_remaining={result.terminal_jobs_remaining}"
        f" queue_purged={result.queue_purged}"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cli_actor_deregister.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add src/taskq/cli.py tests/test_cli_actor_deregister.py
git commit -m "feat: add 'taskq actor-config deregister' CLI command"
```

---

### Task 8: Admin UI actors page

**Files:**
- Create: `src/taskq/web/admin/actors.py`
- Create: `src/taskq/web/templates/actors.html`
- Modify: `src/taskq/web/templates/_base.html` (add nav link)
- Test: `tests/test_web_admin_actors.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_web_admin_actors.py

"""Tests for the admin UI actors page and deregister route.

Follows the exact pattern of ``tests/test_web_admin_integration.py``:
a per-test asyncpg pool on the module's migrated schema, a FastAPI app
built via ``create_router`` + ``setup_admin_state`` + ``include_router``,
and ``httpx.AsyncClient`` with ``ASGITransport`` — NOT ``TestClient``,
which runs the app on its own event loop that the per-test asyncpg pool
is not bound to.

CSRF is the synchronizer-token pattern: ``_CsrfRoute`` sets the
``taskq_csrf_token`` cookie on every GET; ``validate_csrf`` compares the
cookie against the ``csrf_token`` form field on POST. There is no test
bypass — every POST test must GET first so the cookie is set, then pass
the cookie value as the form field (the same flow as ``_post_cancel``
in the integration file). ``httpx.AsyncClient`` persists cookies across
requests on the same client.
"""

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.worker.actor_config import ActorConfig
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest_asyncio.fixture
async def admin_pool(module_pg_schema: ModulePgSchema) -> AsyncIterator[asyncpg.Pool]:
    """Per-test pool on the module's (already migrated) schema.

    Created inside the test's event loop so the ASGI app can use it —
    same rationale as the ``pool`` fixture in test_web_admin_integration.py.
    """
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        yield pool
    finally:
        await pool.close()


def _make_admin_app(
    pool: asyncpg.Pool,
    schema: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    admin_actions_enabled: bool,
) -> FastAPI:
    """Build the admin app. Env must be set BEFORE ``create_router`` —
    it calls ``TaskQSettings.load()`` internally and captures
    ``admin_actions_enabled`` at construction time.

    TASKQ_ENVIRONMENT=dev bypasses create_router's fail-closed
    admin_ui_require_auth default (these tests exercise the page and the
    admin-actions gate, not auth — see test_admin_security_fixes.py for
    the auth gates).
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv(
        "TASKQ_ADMIN_ACTIONS_ENABLED", "true" if admin_actions_enabled else "false"
    )
    bundle = create_router(pool, schema=schema, base_path="/admin")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    return app


async def _seed_actor_config(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    queue: str = "default",
) -> None:
    await sync_actor_config(
        conn,
        [ActorConfig(actor=actor, max_concurrent=1, queue=queue)],
        schema=schema,
    )


async def _get_csrf_then_post(
    app: FastAPI,
    get_url: str,
    post_url: str,
    data: dict[str, str] | None = None,
) -> httpx.Response:
    """GET (to obtain the CSRF cookie) then POST with the matching form field."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=False,
    ) as client:
        get_resp = await client.get(get_url)
        assert get_resp.status_code == 200
        csrf_token = get_resp.cookies.get("taskq_csrf_token", "")
        assert csrf_token, "GET must set the taskq_csrf_token cookie"
        return await client.post(
            post_url, data={"csrf_token": csrf_token, **(data or {})}
        )


async def test_actors_page_lists_actor_config_rows(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /admin/actors shows actor_config rows with queue and capacity."""
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "test-actor-1", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=True)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/admin/actors")

    assert resp.status_code == 200
    assert "test-actor-1" in resp.text
    assert "default" in resp.text


async def test_actors_page_shows_deregister_button(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each actor row has a deregister form/button."""
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "button-actor", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=True)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/admin/actors")

    assert resp.status_code == 200
    assert "Deregister" in resp.text
    assert "/deregister" in resp.text


async def test_deregister_route_returns_403_when_admin_actions_disabled(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST with VALID CSRF still returns 403 when admin_actions_enabled=False —
    proving the 403 comes from the admin-actions gate, not a CSRF failure."""
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "disabled-actor", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=False)

    resp = await _get_csrf_then_post(
        app, "/admin/actors", "/admin/actors/disabled-actor/deregister"
    )

    assert resp.status_code == 403
    # Row must still be there — the gate fires before any DB mutation
    count = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        "disabled-actor",
    )
    assert count == 1


async def test_deregister_route_succeeds_for_clean_actor(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST deregister with force=False deletes the actor_config row."""
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "clean-deregister-actor", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=True)

    resp = await _get_csrf_then_post(
        app, "/admin/actors", "/admin/actors/clean-deregister-actor/deregister"
    )

    assert resp.status_code == 303
    assert "/actors" in resp.headers["location"]

    # Verify the actor_config row is gone
    count = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        "clean-deregister-actor",
    )
    assert count == 0
```

Note: every fixture above exists and is visible to test modules —
``module_pg_schema`` / ``clean_pg_conn`` come from ``taskq.testing.fixtures``
(re-exported through ``tests/conftest.py``); ``admin_pool`` is defined in the
file. The 403 test deliberately passes a VALID CSRF token: ``validate_csrf``
runs before the route body, so a missing/invalid token would also 403 — for
the wrong reason. The GET-first CSRF flow is required; there is no dev-mode
CSRF bypass (``TASKQ_ENVIRONMENT=dev`` only relaxes the auth dependency, not
``validate_csrf``).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_web_admin_actors.py -v`
Expected: FAIL (module doesn't exist)

- [ ] **Step 3: Implement the actors admin page**

Create `src/taskq/web/admin/actors.py`:

```python
"""Actors overview and deregister admin pages."""

from urllib.parse import quote_plus

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment

from taskq.exceptions import ActorDeregistrationError
from taskq.settings import TaskQSettings
from taskq.web.admin._factory import (
    get_base_path,
    get_csrf_token,
    get_pg_pool,
    get_realtime_ctx,
    get_schema,
    get_settings,
    get_templates,
    validate_csrf,
)
from taskq.worker.actor_config_ops import deregister_actor

logger = structlog.get_logger("taskq.web.admin.actors")

_ACTORS_SQL = """
SELECT ac.actor, ac.max_concurrent, ac.max_pending, ac.queue,
       ac.result_ttl, ac.metadata::text AS metadata, ac.updated_at::text AS updated_at,
       (SELECT count(*) FROM "{schema}".jobs j
        WHERE j.actor = ac.actor
        AND j.status IN ('pending', 'scheduled', 'running')) AS active_job_count,
       (SELECT count(*) FROM "{schema}".cron_schedules cs
        WHERE cs.actor = ac.actor AND cs.enabled = true) AS enabled_schedule_count
  FROM "{schema}".actor_config ac
 ORDER BY ac.actor
""".strip()


def register(router: APIRouter) -> None:
    """Attach actors overview and deregister routes to *router*."""

    @router.get("/actors", response_class=HTMLResponse)
    async def actors_overview(
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        csrf_token: str = Depends(get_csrf_token),
    ) -> HTMLResponse:
        actors_sql = _ACTORS_SQL.format(schema=schema)
        rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            rows = await conn.fetch(actors_sql)
        actors = [dict(r) for r in rows]
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("actors.html").render(
            actors=actors,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
            csrf_token=csrf_token,
            active_page="actors",
        )
        return HTMLResponse(content=html)

    @router.post("/actors/{actor}/deregister")
    async def actor_deregister(
        actor: str,
        request: Request,
        _csrf: None = Depends(validate_csrf),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")

        # Read form fields after CSRF validation. Starlette caches
        # request.form() so the CSRF dependency's read and this read
        # share the same parsed body.
        form = await request.form()
        force = form.get("force") == "true"
        purge_queue = form.get("purge_queue") == "true"

        async with pool.acquire() as conn:
            try:
                result = await deregister_actor(
                    conn, actor, force=force, purge_queue=purge_queue, schema=schema
                )
            except ActorDeregistrationError as exc:
                raise HTTPException(
                    status_code=409, detail=str(exc)
                ) from None

        return RedirectResponse(
            url=f"{base_path}/actors?notice=deregistered+{quote_plus(actor)}",
            status_code=303,
        )
```

Create `src/taskq/web/templates/actors.html` — a Jinja2 template following the existing pattern (see `workers.html` and `schedules.html` for structure):

```html
{% extends "_base.html" %}
{% block title %}Actors — TaskQ Admin{% endblock %}
{% block content %}
<div class="space-y-4">
  <h2 class="text-xl font-semibold text-slate-800 dark:text-slate-200">Actors</h2>
  {% if actors %}
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
      <thead class="bg-slate-50 dark:bg-slate-800">
        <tr>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Actor</th>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Queue</th>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Max Concurrent</th>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Max Pending</th>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Active Jobs</th>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Schedules</th>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Updated</th>
          <th class="px-4 py-2 text-left text-xs font-medium text-slate-500">Actions</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-slate-200 dark:divide-slate-700">
        {% for a in actors %}
        <tr class="hover:bg-slate-50 dark:hover:bg-slate-800">
          <td class="px-4 py-2 text-sm font-mono">{{ a.actor }}</td>
          <td class="px-4 py-2 text-sm">{{ a.queue }}</td>
          <td class="px-4 py-2 text-sm">{{ a.max_concurrent or '∞' }}</td>
          <td class="px-4 py-2 text-sm">{{ a.max_pending or '—' }}</td>
          <td class="px-4 py-2 text-sm">
            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs
              {% if a.active_job_count > 0 %}
              bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200
              {% else %}
              bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200
              {% endif %}">
              {{ a.active_job_count }}
            </span>
          </td>
          <td class="px-4 py-2 text-sm">{{ a.enabled_schedule_count }}</td>
          <td class="px-4 py-2 text-sm">{{ a.updated_at | time_ago }}</td>
          <td class="px-4 py-2 text-sm">
            <form method="POST" action="{{ base_path }}/actors/{{ a.actor | urlencode }}/deregister"
                  onsubmit="return confirm('Deregister {{ a.actor }}?');">
              <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
              <label class="text-xs"><input type="checkbox" name="force" value="true"> force</label>
              <label class="text-xs"><input type="checkbox" name="purge_queue" value="true"> purge queue</label>
              <button type="submit"
                      class="ml-2 px-2 py-1 text-xs font-medium text-red-600 hover:text-red-800
                      dark:text-red-400 dark:hover:text-red-300 border border-red-300
                      dark:border-red-700 rounded">
                Deregister
              </button>
            </form>
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
  {% else %}
  <p class="text-sm text-slate-500 dark:text-slate-400">No actor_config rows.</p>
  {% endif %}
</div>
{% endblock %}
```

Note on actor-name encoding: the form action pipes the name through
`urlencode` because actor names are unvalidated free text. Starlette's
default `{actor}` path converter matches a single segment, so a name
containing an embedded `/` will not match the deregister route regardless
of encoding (it 404s) — an accepted limitation to document in the admin UI
guide (Task 11); such actors remain deregisterable via the client API and
CLI.

Add the nav link to `src/taskq/web/templates/_base.html` — after the "Workers" link (around line 53), add:

```html
<a href="{{ base_path }}/actors"
   class="px-4 py-2.5 text-sm font-medium whitespace-nowrap border-b-2 transition-colors
          {% if active_page == 'actors' %}text-white border-blue-400{% else %}text-slate-400 border-transparent hover:text-slate-200 hover:border-slate-500{% endif %}">Actors</a>
```

- [ ] **Step 4: Run tests to verify they pass**

The deregister POST route reads form fields via `request.form()` (cached by Starlette) after the CSRF dependency has validated. The `force` and `purge_queue` checkboxes send `"true"` when checked; the route checks `form.get("force") == "true"`.

Run: `uv run pytest tests/test_web_admin_actors.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/taskq/web/admin/actors.py src/taskq/web/templates/actors.html src/taskq/web/templates/_base.html tests/test_web_admin_actors.py
git commit -m "feat: add admin UI actors page with deregister button"
```

---

### Task 9: Export `ActorsClient` from public API

**Files:**
- Modify: `src/taskq/__init__.py`
- Modify: `src/taskq/worker/actor_config_ops.py` (ensure `__all__` is complete)

- [ ] **Step 1: Write the failing test**

```python
# Add to tests/test_taskq_client.py or a new test file

def test_actors_client_importable_from_taskq() -> None:
    from taskq import ActorsClient
    assert ActorsClient is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_taskq_client.py::test_actors_client_importable_from_taskq -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the export**

In `src/taskq/__init__.py`, add:

```python
from taskq.client._actors import ActorsClient
```

Add `"ActorsClient"` to the `__all__` list.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_taskq_client.py::test_actors_client_importable_from_taskq -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/taskq/__init__.py tests/test_taskq_client.py
git commit -m "feat: export ActorsClient from public API"
```

---

### Task 10: E2E test — full deregistration lifecycle

**Files:**
- Create: `tests/e2e/test_actor_deregistration.py`

- [ ] **Step 1: Write the e2e test**

```python
# tests/e2e/test_actor_deregistration.py

"""E2E: actor deregistration lifecycle with a real worker.

Each test uses a **different actor** to avoid cross-test interference:
``e2e_worker`` is module-scoped and ``sync_actor_config`` runs only at
bootstrap, so once a test deregisters an actor's ``actor_config`` row,
later tests cannot enqueue to that same actor (the dispatch query
inner-joins ``actor_config`` — jobs would never be dispatched).

Actors used (all defined in ``tests/e2e/actors.py``):
- ``quick_result`` — 0.05 s sleep, simple payload/result. Used for the
  clean-deregister-after-completion test.
- ``long_running_job`` — 30 s sleep. Used for the refusal-with-active-jobs
  test (guaranteed to be ``running`` when we deregister).
- ``short_lived_job`` — 0.5 s sleep. Used for the force-deregister test.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ._assertions import poll_until, wait_for_handle_status
from .actors import (
    LongRunningPayload,
    QuickResultPayload,
    ShortJobPayload,
    long_running_job,
    quick_result,
    short_lived_job,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_deregister_after_jobs_complete(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Deregister an actor after all its jobs are terminal."""
    schema = e2e_schema.schema_name
    actor_name = quick_result.name

    # 1. Enqueue a job and wait for it to complete
    handle = await e2e_client.enqueue(
        quick_result, QuickResultPayload(run_id=run_id, value="test")
    )
    await handle.wait(timeout=60)

    # 2. Verify the actor_config row exists (seeded by worker startup)
    ac_count = await e2e_pg_pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".actor_config WHERE actor = $1",
        actor_name,
    )
    assert ac_count == 1, f"actor_config row for {actor_name} should exist"

    # 3. Deregister the actor (force=False — all jobs are terminal)
    result = await e2e_client.actors.deregister(actor_name)

    assert result.actor_config_deleted is True
    assert result.terminal_jobs_remaining >= 1  # our completed job

    # 4. Verify the actor_config row is gone
    ac_count = await e2e_pg_pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".actor_config WHERE actor = $1",
        actor_name,
    )
    assert ac_count == 0

    # 5. Verify terminal job history is still queryable
    job_count = await e2e_pg_pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status = 'succeeded'",
        actor_name,
    )
    assert job_count >= 1, "terminal job history should remain after deregistration"


async def test_deregister_refuses_with_active_jobs(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Deregistration refuses when a job is running.

    Uses ``long_running_job`` (30 s sleep) so the job is guaranteed to be
    ``running`` when we attempt deregistration.
    """
    from taskq.exceptions import ActorHasActiveJobsError

    schema = e2e_schema.schema_name
    actor_name = long_running_job.name

    # 1. Enqueue a long-running job
    handle = await e2e_client.enqueue(
        long_running_job, LongRunningPayload(run_id=run_id)
    )

    # 2. Wait until the job is running (poll the DB)
    async def _is_running() -> bool:
        status = await e2e_pg_pool.fetchval(
            f"SELECT status FROM \"{schema}\".jobs WHERE id = $1",
            handle.job_id,
        )
        return status == "running"

    await poll_until(_is_running, timeout=30.0, interval=0.5)

    # 3. Try to deregister — must refuse with ActorHasActiveJobsError
    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await e2e_client.actors.deregister(actor_name)

    assert exc_info.value.actor == actor_name
    assert exc_info.value.active_count >= 1
    assert "running" in exc_info.value.status_counts

    # 4. Verify the actor_config row is still there (refusal did not delete)
    ac_count = await e2e_pg_pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".actor_config WHERE actor = $1",
        actor_name,
    )
    assert ac_count == 1

    # 5. Clean up: cancel the job, wait for terminal, then force-deregister.
    #    Do NOT use handle.wait() here — it raises JobFailed for any
    #    non-success terminal status (cancelled included). Poll the status
    #    instead (the test_cancellation.py idiom). long_running_job never
    #    calls ctx.check_cancelled(), so the cancel lands only after the
    #    30 s sleep finishes and the consumer routes the completion to
    #    mark_cancelled (cancel_phase >= COOPERATIVE is checked post-run) —
    #    budget the full 30 s plus margin.
    await handle.cancel()
    await wait_for_handle_status(handle, "cancelled", timeout=60)

    result = await e2e_client.actors.deregister(actor_name, force=True)
    assert result.actor_config_deleted is True

    # 6. Verify cleanup
    ac_count = await e2e_pg_pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".actor_config WHERE actor = $1",
        actor_name,
    )
    assert ac_count == 0


async def test_deregister_force_after_completion(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """force=True deregister succeeds when all jobs are terminal.

    Uses ``short_lived_job`` (0.5 s sleep). Enqueues a job, waits for
    completion, then force-deregisters. The force path cancels 0 jobs
    (none are pending) and succeeds.
    """
    schema = e2e_schema.schema_name
    actor_name = short_lived_job.name

    # 1. Enqueue a job and wait for completion
    handle = await e2e_client.enqueue(
        short_lived_job, ShortJobPayload(run_id=run_id, label="force-test")
    )
    await handle.wait(timeout=60)

    # 2. Force-deregister (no active jobs, so force has nothing to cancel)
    result = await e2e_client.actors.deregister(actor_name, force=True)

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 0  # no pending jobs to cancel
    assert result.terminal_jobs_remaining >= 1

    # 3. Verify cleanup
    ac_count = await e2e_pg_pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".actor_config WHERE actor = $1",
        actor_name,
    )
    assert ac_count == 0
```

Note: Each test uses a **different actor** (``quick_result``, ``long_running_job``, ``short_lived_job``) to avoid the module-scoped worker issue where deregistering one actor's ``actor_config`` row prevents later tests from dispatching jobs to that actor. All three actors are already defined in ``tests/e2e/actors.py``.

- [ ] **Step 2: Run the e2e test**

Run: `uv run pytest tests/e2e/test_actor_deregistration.py -v --tb=short`
Expected: PASS (3 tests) — requires Docker for the worker container. Each test uses a different actor to avoid cross-test interference.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_actor_deregistration.py
git commit -m "test: add e2e test for actor deregistration lifecycle"
```

---

### Task 11: Documentation updates

**Files:**
- Modify: `docs/guides/actors.md` (add deregistration section)
- Modify: `docs/guides/cli.md` (add `deregister` command docs)
- Modify: `docs/guides/admin-ui.md` (add actors page docs)

- [ ] **Step 1: Add deregistration section to actors.md**

Add a new section at the end of the file (after "Full worked example"):

```markdown
## Actor deregistration

Actors registered by worker startup create `actor_config` rows that persist
until explicitly removed. For long-lived deployments this is intentional —
the row is the source of truth for capacity and routing. For ephemeral,
per-run deployments (e.g. `my-actor.<run-id>`), each run leaves a row behind.

### `client.actors.deregister()`

```python
async with TaskQ(dsn=...) as tq:
    result = await tq.actors.deregister("my-actor.run-123")
    # force=False: refuses if non-terminal jobs or enabled schedules exist
```

**Safety checks (force=False):**
- Refuses if any non-terminal jobs (pending/scheduled/running) reference the
  actor.
- Refuses if any enabled cron schedules reference the actor.

**force=True:**
- Still refuses if **running** jobs exist (they are actively executing).
- Cancels pending/scheduled jobs (marks as `cancelled` with
  `error_class='ActorDeregistered'`).
- Disables enabled cron schedules (sets `enabled=false`).

**Terminal job history** is never deleted. The `jobs.actor` column is plain
text, not a foreign key — terminal rows remain queryable by actor name after
deregistration.

**Queue cleanup** (`purge_queue=True`): deletes the `queues` row if no other
`actor_config` references the same queue. A shared queue is never purged.

### Enqueue after deregistration

After deregistration, any client can still `enqueue()` the dead actor name —
the `INSERT` succeeds (there is no foreign key from `jobs.actor` to
`actor_config.actor`), and the job sits in `pending` status forever. Because
the dispatch query inner-joins `actor_config`, the job will **never be
dispatched** and no background sweep will reap it (deregistration is an
explicit operator action, not a background GC — see Non-goal #2).

**Operational discipline:** stop enqueuing to an actor *before* deregistering
it. Deregistration is best-effort against concurrent enqueue/dispatch;
callers must quiesce the actor first (stop enqueuing, disable cron
schedules, wait for running jobs to reach a terminal state).

A follow-up issue may explore an opt-in "strict mode" that rejects enqueues
to actors with no `actor_config` row; this is explicitly out of scope for
this spec.

### Idempotent deregistration

A second `deregister` call on an already-deregistered actor raises
`ActorNotFoundError`. For cleanup-automation loops (e.g. iterating over
stage actors after a run completes), use the try/except idiom:

```python
from taskq.exceptions import ActorNotFoundError

try:
    await tq.actors.deregister(actor_name, force=True, purge_queue=True)
except ActorNotFoundError:
    pass  # already deregistered — idempotent
```

### `taskq actor-config deregister`

```bash
taskq actor-config deregister my-actor.run-123
taskq actor-config deregister my-actor.run-123 --force --purge-queue
```

### Admin UI

The `/admin/actors` page lists all `actor_config` rows with active job counts
and schedule counts. Each row has a deregister form with `force` and
`purge_queue` checkboxes (requires `TASKQ_ADMIN_ACTIONS_ENABLED=true`).
```

- [ ] **Step 2: Add CLI docs to cli.md**

- [ ] **Step 3: Add admin UI docs to admin-ui.md**

- [ ] **Step 4: Commit**

```bash
git add docs/guides/actors.md docs/guides/cli.md docs/guides/admin-ui.md
git commit -m "docs: add actor deregistration documentation"
```

---

### Task 12: Run full verification

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest tests/test_actor_deregistration.py tests/test_cli_actor_deregister.py tests/test_actors_client.py tests/test_web_admin_actors.py tests/test_exceptions.py -v
```

- [ ] **Step 2: Run type checking**

```bash
uv run pyright src/taskq/worker/actor_config_ops.py src/taskq/client/_actors.py src/taskq/cli.py src/taskq/exceptions.py
```

- [ ] **Step 3: Run linting**

```bash
uv run ruff check src/taskq/worker/actor_config_ops.py src/taskq/client/_actors.py src/taskq/cli.py src/taskq/exceptions.py
```

Also add Ruff S608 per-file-ignore entries to `pyproject.toml` for the new
test files that use f-string SQL (schema name is validated against
`_IDENT_RE` before interpolation — same rationale as existing entries):

```toml
[tool.ruff.lint.per-file-ignores]
# ... existing entries ...
"tests/test_actor_deregistration.py" = ["S608"]
"tests/test_web_admin_actors.py" = ["S608"]
"tests/test_cli_actor_deregister.py" = ["S608"]
```

Then run:

```bash
uv run ruff check tests/test_actor_deregistration.py tests/test_web_admin_actors.py tests/test_cli_actor_deregister.py
```

- [ ] **Step 4: Run e2e tests (if Docker is available)**

```bash
uv run pytest tests/e2e/test_actor_deregistration.py -v
```

- [ ] **Step 5: Commit any remaining fixes**

```bash
git add -A
git commit -m "chore: verification fixes"
```

---

## Test Coverage Requirements

| Layer | Test file | Coverage target |
|-------|-----------|-----------------|
| Exceptions | `tests/test_exceptions.py` | All 4 new exception classes: construction, attributes, inheritance |
| Ops function | `tests/test_actor_deregistration.py` | force=False: not found, clean delete, pending jobs refuse, running jobs refuse, enabled schedules refuse, disabled schedules OK, terminal jobs OK |
| Ops function | `tests/test_actor_deregistration.py` | force=True: cancel pending + disable schedules, refuse running, mixed (running + pending), terminal history preserved |
| Ops function | `tests/test_actor_deregistration.py` | purge_queue: orphan queue deleted, shared queue kept, default (no purge) keeps queue |
| ActorsClient | `tests/test_actors_client.py` | list/get/set_capacity/deregister delegation, pool acquire, schema forwarding |
| TaskQ.actors | `tests/test_taskq_client.py` | Property returns ActorsClient, raises before open() |
| CLI | `tests/test_cli_actor_deregister.py` | Default, --force, --purge-queue, not found, active jobs error, schedules error, output shape |
| Admin UI | `tests/test_web_admin_actors.py` | Page renders, deregister button, 403 when admin_actions disabled, successful deregister |
| E2E | `tests/e2e/test_actor_deregistration.py` | Full lifecycle: enqueue → complete → deregister → verify; refuse with running jobs (real assertions); force deregister after completion |

---

## Backward Compatibility Analysis

1. **No schema migration required.** The existing schema (tables, columns, constraints, indexes) is unchanged. Deregistration works on any schema that has the current migrations applied.

2. **No new dependencies.** The implementation uses only existing modules (`asyncpg`, `structlog`, `typer`, `fastapi`, `jinja2`).

3. **No breaking API changes.** All new code is additive:
   - `ActorsClient` is a new class — no existing code references it.
   - `TaskQ.actors` is a new property — no existing code calls it.
   - `deregister_actor` is a new function in `actor_config_ops.py` — no existing code imports it.
   - `taskq actor-config deregister` is a new CLI command — no existing scripts call it.
   - The admin UI actors page is a new route — auto-discovered by `_discover_and_register`.
   - New exceptions inherit from `ActorDeregistrationError` → `TaskQError` — no existing exception handling is affected.

4. **`__all__` additions are additive.** New names added to `__all__` in `actor_config_ops.py`, `client/__init__.py`, and `taskq/__init__.py` do not remove any existing names.

5. **Admin UI nav link is additive.** The new "Actors" link in `_base.html` does not alter existing navigation.

6. **No changes to drift-check behavior.** `_STRUCTURAL_FIELDS`, `ActorConfigDriftList`, and `sync_actor_config` are unchanged.

7. **Downstream consumer migration.** Consumers currently using hand-rolled SQL:
   ```python
   await conn.execute(f'DELETE FROM "{schema}".actor_config WHERE actor = $1', actor_name)
   ```
   can replace with:
   ```python
   await client.actors.deregister(actor_name, force=True, purge_queue=True)
   ```
   The `force=True` is needed because the hand-rolled SQL doesn't check for non-terminal jobs. Consumers should audit their cleanup paths and decide whether `force=True` or the safer default is appropriate.

---

## Downstream Consumer Impact Analysis

### warden (`~/src/warden`)

**Current pattern:** Static, module-level actor names. Warden's five actors
(`transcription_backend_call`, `diarization_backend_call`,
`ocr_backend_call`, `provision_backend_call`, `autoscale_cron_tick`) are
declared with `@actor(name=...)` at module scope
(`~/src/warden/src/warden/jobs.py:382,532,728,869,1002`). No dynamic or
per-deployment actor naming exists. Tests use `InMemoryBackend` with
`register_actor_config` (`~/src/warden/tests/test_transcription_jobs.py:85-87`).

**Impact:** **None today.** Warden's actors are fixed names that persist for
the lifetime of the deployment — they never accumulate `actor_config` rows
and do not need deregistration. If warden ever adopts ephemeral per-run
actors (e.g. for transient model deployments), the `deregister` API is
available, but no migration is needed now.

### cennan (`~/src/cennan`)

**Current pattern:** Fixed set of actors, one per pipeline stage
(`sync_binding`, `list_page`, `fetch_document`, `extract_document`,
`chunk_document`, `rechunk_binding`, `embed_batch`, `store_batch`,
`reproject_document_metadata`
— `~/src/cennan/src/cennan/pipeline/actors.py:154-241`; architecture doc:
"TaskQ actors, one per stage"). Binding identity travels in job payloads
(`binding:{id}`), not in actor names. Actors are registered at worker
startup and persist for the deployment lifetime.

**Impact:** **None today.** Cennan's actors are fixed stage names, not
per-KB or per-binding — they do not accumulate rows and do not need
deregistration. If cennan ever adopts per-binding actor naming, the
`deregister` API is available, but no migration is needed now.

### aacrtool (`~/src/aacrtool`)

**Current pattern:** Per-review-run actors. The aacrtool spec rev3 plan
explicitly identifies this as gap #11: "Ephemeral actor_name accumulation
(8 rows/scan)" → "No actor deregistration/GC in HEAD" → "upstream
candidate: actor deregistration on worker shutdown / actor_config GC."

**Impact:** **High — this is the consumer that explicitly identified the
gap.** After a scan completes:

```python
# After a scan run is finalized:
for stage in ["s1-crawl", "s2-fetch", "s3-parse", "s4-analyze", "s5-embed", ...]:
    await tq.actors.deregister(f"{stage}.{scan_slug}", force=True, purge_queue=True)
```

**Migration path:** aacrtool's scan finalization handler should deregister
all per-scan actors after the scan reaches a terminal state (`complete` or
`partial`). The `force=True` flag is needed because some jobs may still be
pending when the scan is finalized. `purge_queue=True` cleans up the
per-scan queue. For idempotent cleanup loops, wrap in `try/except
ActorNotFoundError: pass` (see Design Decision §10).

**Design note from aacrtool spec:** "Do NOT delete rows from taskq schema
AACRTool-side." This spec provides the upstream API so aacrtool can stop
deferring the cleanup and use the official `deregister` path.

---

## Key Design Decisions

### 1. Pure application logic, no migration

**Decision:** Deregistration is a transactional set of checks + DELETEs, not a schema change.

**Rationale:** The existing schema has no FKs from `jobs.actor` or `cron_schedules.actor` to `actor_config.actor`. Adding FKs with `ON DELETE` actions would require a migration and risk lock contention on the hot `jobs` table (an `ADD FOREIGN KEY` scan blocks reads and writes). The application logic approach works on any already-migrated schema and is simpler to reason about.

**Tradeoff:** If someone manually deletes an `actor_config` row (bypassing `deregister_actor`), pending jobs for that actor become stranded. The `deregister_actor` function's safety checks prevent this, but the schema doesn't enforce it. This is the same tradeoff the existing design already makes — the drift check is application-level, not schema-level.

### 2. force=True still refuses running jobs

**Decision:** `force=True` cancels pending/scheduled jobs and disables schedules, but still refuses if running jobs exist.

**Rationale:** Running jobs are actively executing — their terminal-write path reads `actor_config.result_ttl` to compute `result_expires_at`. Deleting the row mid-execution loses the stored `result_ttl` override; the `COALESCE` in the terminal-write SQL falls back to the `@actor(...)` literal TTL (or preserves the existing `result_expires_at`), which is a silent semantic change. More importantly, the dispatch query inner-joins `actor_config`, so a running job that retries would be stranded. Refusing is the safe default; the operator can wait for running jobs to complete or cancel them first.

### 3. Schedules are disabled, not deleted

**Decision:** `force=True` sets `enabled=false` on cron schedules, not `DELETE`.

**Rationale:** The schedule row carries configuration (cron expression, timezone, payload factory) that the operator may want to re-enable if the actor is re-registered. Deleting the schedule would lose this configuration. Disabling is reversible; deleting is not.

### 4. Queue purge is opt-in

**Decision:** `purge_queue` defaults to `False`. The caller must explicitly request it.

**Rationale:** Queue rows are metadata (mode, max_concurrent) that might be shared between actors or manually managed by the operator. Deleting a queue row doesn't affect already-queued jobs (there's no FK), but it does remove the configuration. Making it opt-in prevents accidental loss of queue-level settings.

### 5. Terminal job history is never deleted

**Decision:** Terminal jobs (succeeded/failed/cancelled/crashed/abandoned) remain in the `jobs` table after deregistration.

**Rationale:** `jobs.actor` is a plain `text` column, not a foreign key — terminal rows remain queryable by actor name. Deleting them would lose audit history and result data. The `DeregisterResult.terminal_jobs_remaining` count informs the caller how many such rows exist. The existing archive sweep will eventually move them to `jobs_archive` and then hard-delete them per the retention policy — that's the correct GC path, not deregistration.

### 6. ActorsClient as a separate class, not methods on TaskQ

**Decision:** Create `ActorsClient` as a separate class, exposed via `TaskQ.actors` property.

**Rationale:** The issue explicitly asks for `client.actors.deregister(...)`. Separating actor operations from job operations keeps `TaskQ` focused as a job client and provides a clean namespace for future actor management operations. The pool-wrapping pattern mirrors how `JobsClient` wraps the `Backend`.

### 7. Admin UI page is auto-discovered

**Decision:** The actors page follows the existing `_discover_and_register` pattern in `_factory.py`.

**Rationale:** No changes to `_factory.py` are needed — the `register()` function in `actors.py` is automatically discovered and called. This follows the "decompose by composition, not accumulation" principle documented in the codebase.

### 8. Accepted TOCTOU race — deregistration is best-effort against concurrent enqueue/dispatch

**Decision:** Deregistration does NOT serialize against concurrent enqueue, cron-fire, or dispatch. Callers must quiesce the actor first.

**Rationale:** `deregister_actor` runs in a READ COMMITTED transaction. The safety checks and DELETE are separate statements; a job enqueued by a concurrent transaction that commits after the check but before the DELETE is invisible to the check and will be stranded (the `jobs` INSERT has no FK to `actor_config`, and dispatch inner-joins `actor_config` so the job is never dispatched). Three remediation options were evaluated:

- **(a) Document accepted semantics** — "callers must quiesce first." This is the same operational discipline as any shutdown sequence. **Chosen.**
- **(b) `pg_advisory_xact_lock(hashtext(actor))`** in `deregister_actor` and on the enqueue/dispatch paths — would serialize the hot enqueue path against a rare administrative operation. The cost is unjustified for the problem size.
- **(c) A narrow reaper** (leader sweep cancels pending jobs whose actor has no `actor_config` row) — conflicts with Non-goal #2 (no GC sweep), would require amending the non-goal and adding leader-loop complexity.

The accepted-semantics approach is consistent with Non-goal #2 and the existing design philosophy: deregistration is an explicit operator action, not a background automation. The operator's runbook is: stop enqueuing → wait for terminal → deregister.

### 9. Enqueue-after-deregistration is unguarded

**Decision:** After deregistration, any client can still `enqueue()` the dead actor name. The INSERT succeeds (no FK), the job sits `pending` forever, invisible to dispatch.

**Rationale:** Enqueue-side rejection of unknown actors would require a check against `actor_config` on every enqueue — a hot-path cost for a rare operational mistake. The hazard is documented in the API surface, CLI output, and `docs/guides/actors.md`. A follow-up issue may explore an opt-in "strict mode" that rejects enqueues to actors with no `actor_config` row; this is explicitly out of scope for this spec.

### 10. Idempotency — second deregister raises ActorNotFoundError

**Decision:** A second `deregister` call on an already-deregistered actor raises `ActorNotFoundError`. There is no `if_missing` parameter.

**Rationale:** Adding `if_missing: Literal["raise", "ok"]` would complicate the API for a marginal convenience. Cleanup-automation callers (e.g. aacrtool loops) should use the try/except idiom:

```python
from taskq.exceptions import ActorNotFoundError

try:
    await tq.actors.deregister(actor_name, force=True, purge_queue=True)
except ActorNotFoundError:
    pass  # already deregistered — idempotent
```

This is documented in the guide.

---

## Revision log

### 2026-07-29 — Post-review revision (verdict: NEEDS REWORK → resolved)

Revised against `.review/spec-review.md` (1 Critical / 3 High / 4 Medium /
9 Low). The review confirmed the architecture and semantics for issue #56
are sound; the rework was execution fidelity. Standing directive applied:
TaskQ 1.0.0 is a breaking release — no gratuitous churn, but no hacks,
legacy paths, shims, or dual-path compat either; documented downstream
needs are the contract, current downstream usage is not a constraint.

Resolved:

- **C1 (Critical):** `_DEREGISTER_CHECK_ACTIVE_JOBS_SQL` now casts to the
  real enum array type (`$2::"{schema}".job_status[]`, matching the
  `_sql_templates.py:451` precedent) instead of `$2::text[]`, which raised
  PG 42883 against the real `job_status` enum column. The hand-rolled
  minimal test schema (text `status`) was replaced with real migrations
  (`taskq.migrate.apply_pending`), so this class of type drift is
  structurally impossible in tests.
- **H1:** Resolved by the same `apply_pending` fixture change — the real
  migrated `jobs` table carries `finished_at` / `error_class` /
  `error_message`, so Task 3's force=True cancel SQL executes against the
  same columns it will see in production. `_insert_job` supplies the
  NOT-NULL-without-default columns (`max_attempts`, `retry_kind`) and
  casts the status parameter to `job_status`.
- **H2:** TOCTOU race now explicitly acknowledged with chosen option (a) —
  documented best-effort semantics ("quiesce the actor first") in the ops
  docstring warning, Design Decision §8 (options b/c evaluated and
  rejected with cost rationale), the CLI success warning, and the docs
  guide. No serialization added: advisory locks would tax the hot enqueue
  path for a rare admin operation; a reaper conflicts with Non-goal #2.
- **H3:** e2e plan rewritten — uses the real actors `quick_result`,
  `long_running_job`, `short_lived_job` from `tests/e2e/actors.py`
  (verified present and registered in `worker_entry.py`), one actor per
  test to respect the module-scoped worker's bootstrap-only
  `sync_actor_config`, and real refusal assertions. Additionally fixed a
  residual the review did not catch: the refusal test's cleanup used
  `handle.wait()` after cancel, which raises `JobFailed` on the
  `cancelled` terminal status; it now polls with
  `wait_for_handle_status(handle, "cancelled", timeout=60)` and documents
  why the cancel takes the full ~30 s (the actor never calls
  `ctx.check_cancelled()`, so cancellation lands at completion via the
  consumer's post-run `cancel_phase` check).
- **M1:** Task 6 test uses `module_pg_schema.pg_dsn` (not `str()` of the
  NamedTuple); unused `pg_conn` param removed.
- **M2:** Task 8 fully specified — tests now self-contained (per-test
  `admin_pool` on the module schema, `_make_admin_app` helper mirroring
  `test_web_admin_integration.py`, `httpx.AsyncClient` + `ASGITransport`
  instead of `TestClient`, GET-first synchronizer-token CSRF flow instead
  of a hardcoded token that `validate_csrf` would reject; the 403 test
  passes valid CSRF so the gate — not CSRF — is what fails). The POST
  route is exact: `request.form()` after `validate_csrf` (Starlette caches
  the parsed body), `TaskQSettings`-typed settings dependency.
- **M3:** Downstream section rewritten per the directive — aacrtool quote
  re-verified verbatim; warden/cennan false claims removed and replaced
  with the verified reality (fixed-name actors, no deregistration need
  today, API available if they adopt per-run naming). Warden actor names
  and cennan actor list/line-range corrected from the repos.
- **M4:** Enqueue-after-deregistration semantics documented (Design
  Decision §9, docs guide "Enqueue after deregistration", CLI warning,
  ops docstring); strict-mode enqueue rejection named as explicit
  follow-up, out of scope.
- **L1–L9:** broken duplicate `_insert_job` removed; Ruff S608
  per-file-ignore entries added to Task 12; Task 5 uses `monkeypatch`;
  CLI catches `ValueError` alongside `ActorDeregistrationError`; template
  uses `urlencode` (with the single-segment path-converter limitation
  noted); idempotency try/except idiom documented (§10); docs anchor
  corrected to "Full worked example"; Task 2 comment renumbered (purge
  lands in Task 3); scope expansion beyond issue #56 flagged at the top
  for the issue author.

Design changes: none to the core semantics (refusal rules, force=True
behavior, disable-not-delete schedules, opt-in queue purge, terminal
history retention are unchanged and were judged sound). No breaking
changes introduced by this feature — all surface is additive, consistent
with the directive's "no gratuitous churn" clause. Intentionally
deferred: enqueue-side rejection of unknown actors (named follow-up;
hot-path cost), `if_missing` idempotency parameter (YAGNI — the
try/except idiom covers the cleanup-loop case).
