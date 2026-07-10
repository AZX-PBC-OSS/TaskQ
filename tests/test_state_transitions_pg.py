"""Integration tests for state transitions on PostgresBackend.

Each test exercises full lifecycles, concurrency, deadline sweep, the
polling retry pattern, and the heartbeat isolate-self bypass against
real Postgres 18 via testcontainers.
"""

# ruff: noqa: S608 Why: schema name validated by WorkerSettings._post_load against _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches existing integration test pattern

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, RetryKind
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_pending_job, create_running_job, create_worker, parse_detail
from taskq.worker.heartbeat import isolate_self

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    from taskq.backend.postgres import PostgresBackend

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=60)
_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)


# ── Helpers ────────────────────────────────────────────────────────────


async def _enqueue_pg(
    backend: "PostgresBackend",
    *,
    actor: str = "test_actor",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    scheduled_at: datetime | None = None,
    schedule_to_close: datetime | None = None,
) -> JobId:
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={"key": "value"},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        scheduled_at=scheduled_at or datetime.now(UTC),
        schedule_to_close=schedule_to_close,
    )
    row = await backend.enqueue(args)
    return row.id


# ── Full lifecycle on PG ──────────────────────────────────────


class TestFullLifecycle:
    """Full lifecycle on PG: enqueue → dispatch → snooze → wake → dispatch → succeed."""

    async def test_full_lifecycle(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)

        job_id = await _enqueue_pg(backend)

        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1
        assert dispatched[0].status == "running"
        assert dispatched[0].attempt == 1

        result = await backend.mark_snoozed(job_id, worker_id, delay=timedelta(seconds=30))
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert row is not None
        assert row["status"] == "scheduled"
        assert row["attempt"] == 1

        async with deps.worker_pool.acquire() as conn:
            # 5s margin, not 1s — see TestPollingLifecycle's identical fix
            # above for why (PG-server-clock vs. Python-client-clock skew).
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET scheduled_at = now() - interval '5 seconds' WHERE id = $1",
                job_id,
            )
        count = await backend.scheduled_to_pending(datetime.now(UTC))
        assert count >= 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
        assert row is not None
        assert row["status"] == "pending"

        dispatched2 = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched2) == 1
        assert dispatched2[0].status == "running"
        assert dispatched2[0].attempt == 2

        ok = await backend.mark_succeeded(job_id, worker_id, result={"ok": True})
        assert ok is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )
        assert row is not None
        assert row["status"] == "succeeded"
        assert row["finished_at"] is not None

        state_changes = [e for e in events if e["kind"] == "state_change"]
        assert len(state_changes) == 5

        transitions: list[tuple[str | None, str | None]] = []
        for e in state_changes:
            detail = parse_detail(e["detail"])
            transitions.append((detail.get("from_state"), detail.get("to_state")))
        expected_sequence = [
            ("pending", "running"),
            ("running", "scheduled"),
            ("scheduled", "pending"),
            ("pending", "running"),
            ("running", "succeeded"),
        ]
        assert transitions == expected_sequence


# ── Concurrent state transitions ──────────────────────────────


class TestConcurrentTransitions:
    """Two operations on the same job simultaneously (cancel + snooze).

    One wins (idempotent WHERE guard); no split state; no duplicate job_events.
    """

    async def test_concurrent_cancel_and_snooze(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)

        job_id = await _enqueue_pg(backend)

        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1

        cancel_task = asyncio.create_task(
            backend.write_cancel_request(job_id, reason="user"),
            name="concurrent-cancel",
        )
        snooze_task = asyncio.create_task(
            backend.mark_snoozed(job_id, worker_id, delay=timedelta(seconds=30)),
            name="concurrent-snooze",
        )

        cancel_result, snooze_result = await asyncio.gather(
            cancel_task, snooze_task, return_exceptions=True
        )

        assert isinstance(cancel_result, (bool, Exception))
        assert isinstance(snooze_result, (str, Exception))

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
            events = await conn.fetch(
                f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1',
                job_id,
            )
        assert row is not None

        assert row["status"] in ("scheduled", "running")

        state_changes = [e for e in events if e["kind"] == "state_change"]
        seen_keys: set[tuple[object, object]] = set()
        for e in state_changes:
            detail = parse_detail(e["detail"])
            key = (detail.get("from_state"), detail.get("to_state"))
            seen_keys.add(key)
        assert len(state_changes) == len(seen_keys)


# ── State-transition equivalence across backends ────────────


class TestStateTransitionEquivalence:
    """State-transition equivalence across backends for terminal writes.

    Run the full lifecycle on both backends; assert that for every
    state_change recorded in the PG job_events table, the (from_state, to_state)
    tuple is also present in the InMemory event store. In-memory may emit
    additional transitions (enqueue/dispatch); only the PG-emitted subset
    is compared.
    """

    async def test_lifecycle_transitions_match(
        self, clean_jobs_app: JobsApp, memory_jobs: object
    ) -> None:
        from taskq.testing.in_memory import InMemoryBackend

        deps = clean_jobs_app.deps
        pg_backend = clean_jobs_app.backend
        mem_backend: InMemoryBackend = memory_jobs  # type: ignore[assignment] # Why: fixture yields InMemoryBackend typed as object to avoid import; runtime type is InMemoryBackend
        schema = deps.settings.schema_name

        pg_worker_id = new_uuid()
        async with deps.dispatcher_pool.acquire() as conn:
            await create_worker(conn, schema, pg_worker_id)

        mem_worker_id = mem_backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access

        # ── Run lifecycle on InMemory backend ─────────────────────────
        mem_job_id = new_job_id()
        mem_scheduled_at = mem_backend._clock.now()  # type: ignore[reportPrivateUsage,union-attr] # Why: test-only private access for FakeClock
        mem_args = EnqueueArgs(
            id=mem_job_id,
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=mem_scheduled_at,
        )
        await mem_backend.enqueue(mem_args)
        dispatched = await mem_backend.dispatch_batch(
            mem_worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1
        ok = await mem_backend.mark_succeeded(mem_job_id, mem_worker_id, result={"ok": True})
        assert ok is True

        mem_transitions: list[tuple[str, str]] = []
        for e in await mem_backend.get_events(mem_job_id):
            if e.kind == "state_change":
                from_s = e.detail.get("from_state")
                to_s = e.detail.get("to_state")
                if isinstance(from_s, str) and isinstance(to_s, str):
                    mem_transitions.append((from_s, to_s))

        # ── Run lifecycle on PG backend ──────────────────────────────
        pg_job_id = await _enqueue_pg(pg_backend)
        dispatched = await pg_backend.dispatch_batch(
            pg_worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1
        ok = await pg_backend.mark_succeeded(pg_job_id, pg_worker_id, result={"ok": True})
        assert ok is True

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                pg_job_id,
            )

        pg_transitions: list[tuple[str, str]] = []
        for e in events:
            if e["kind"] == "state_change":
                detail = parse_detail(e["detail"])
                from_s = detail.get("from_state")
                to_s = detail.get("to_state")
                if isinstance(from_s, str) and isinstance(to_s, str):
                    pg_transitions.append((from_s, to_s))

        for pg_tuple in pg_transitions:
            assert pg_tuple in mem_transitions, (
                f"PG transition {pg_tuple} not found in mem transitions {mem_transitions}"
            )


# ── Deadline sweep coverage ───────────────────────────────────


class TestDeadlineSweep:
    """Deadline sweep: pending/scheduled past schedule_to_close → failed with DeadlineExceeded.

    evidence: AttemptRow written with started_at IS NOT NULL.
    """

    async def test_pending_past_deadline(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        past = datetime.now(UTC) - timedelta(seconds=10)
        job_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_pending_job(conn, schema, job_id, schedule_to_close=past)

        count = await backend.deadline_sweep(datetime.now(UTC))
        assert count >= 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT outcome, error_class, started_at FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"
        assert row["finished_at"] is not None
        assert len(attempts) >= 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "DeadlineExceeded"
        assert attempts[0]["started_at"] is not None

    async def test_scheduled_past_deadline(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        past = datetime.now(UTC) - timedelta(seconds=10)
        job_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_pending_job(
                conn,
                schema,
                job_id,
                status="scheduled",
                schedule_to_close=past,
            )

        count = await backend.deadline_sweep(datetime.now(UTC))
        assert count >= 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"


# ── polling lifecycle on PG ─────────────────────────────────


class TestPollingLifecycle:
    """Polling lifecycle: enqueue → dispatch → snooze → wake → dispatch → succeed.

    Oracle: all three transitions produce state_change events in
    job_events with correct from_state/to_state.
    """

    async def test_polling_lifecycle(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)

        job_id = await _enqueue_pg(backend)

        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1

        result = await backend.mark_snoozed(job_id, worker_id, delay=timedelta(seconds=30))
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            # 5s margin, not 1s: scheduled_at is set from the PG server clock
            # while scheduled_to_pending's threshold below comes from the
            # Python client clock — independently reproduced (see
            # test_heartbeat_integration.py's _CLOCK_JITTER_TOLERANCE) that
            # these can differ by several hundred ms under this environment's
            # connection-pool/scheduling characteristics, which a 1s margin
            # doesn't reliably absorb.
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET scheduled_at = now() - interval '5 seconds' WHERE id = $1",
                job_id,
            )
        await backend.scheduled_to_pending(datetime.now(UTC))

        dispatched2 = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched2) == 1

        ok = await backend.mark_succeeded(job_id, worker_id, result={"ok": True})
        assert ok is True

        expected_transitions = [
            ("running", "scheduled"),
            ("scheduled", "pending"),
            ("running", "succeeded"),
        ]

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )
        all_state_changes = [e for e in events if e["kind"] == "state_change"]
        for from_s, to_s in expected_transitions:
            found = False
            for e in all_state_changes:
                detail = parse_detail(e["detail"])
                if detail.get("from_state") == from_s and detail.get("to_state") == to_s:
                    found = True
                    break
            assert found, f"missing polling lifecycle event {from_s} → {to_s}"


# ── running → pending via isolate_self (retries remain) ──────


class TestIsolateSelfPending:
    """running → pending via isolate_self when retries remain.

    Oracle: status='pending', lock fields cleared, AttemptRow with
    outcome='crashed' and error_class='HeartbeatLost'.
    """

    async def test_isolate_self_pending(self, pg_dsn: str) -> None:
        from contextlib import AsyncExitStack

        from taskq.migrate import apply_pending
        from taskq.settings import WorkerSettings
        from taskq.worker.deps import WorkerDeps, open_worker_deps

        settings = WorkerSettings.load_from_dict(
            {
                "pg_dsn": pg_dsn,
                "schema_name": f"tsp_{new_base62()}".lower(),
            }
        )
        schema = settings.schema_name

        conn = await asyncpg.connect(str(settings.pg_dsn))
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await apply_pending(conn, schema=schema)
        finally:
            await conn.close()

        assert settings.pg_dsn_direct is not None

        stack = AsyncExitStack()
        deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
        try:
            worker_id = new_uuid()
            async with deps.worker_pool.acquire() as conn:
                await create_worker(conn, schema, worker_id)
                job_id = await create_running_job(
                    conn, schema, worker_id, max_attempts=3, attempt=1
                )

            shutdown = asyncio.Event()
            await isolate_self(deps, worker_id, shutdown)
            assert shutdown.is_set()

            async with deps.worker_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT status, locked_by_worker, lock_expires_at FROM "{schema}".jobs WHERE id = $1',
                    job_id,
                )
                attempts = await conn.fetch(
                    f'SELECT outcome, error_class FROM "{schema}".job_attempts WHERE job_id = $1',
                    job_id,
                )
            assert row is not None
            assert row["status"] == "pending"
            assert row["locked_by_worker"] is None
            assert row["lock_expires_at"] is None

            assert len(attempts) >= 1
            assert attempts[0]["outcome"] == "crashed"
            assert attempts[0]["error_class"] == "HeartbeatLost"
        finally:
            await stack.aclose()


# ── running → crashed via isolate_self (retries exhausted) ────


class TestIsolateSelfCrashed:
    """running → crashed via isolate_self when retries exhausted.

    Oracle: status='crashed', error_class='HeartbeatLost' (NOT
    'WorkerCrashed' — forensic distinction with Sweep 1 is intentional).
    """

    async def test_isolate_self_crashed(self, pg_dsn: str) -> None:
        from contextlib import AsyncExitStack

        from taskq.migrate import apply_pending
        from taskq.settings import WorkerSettings
        from taskq.worker.deps import WorkerDeps, open_worker_deps

        settings = WorkerSettings.load_from_dict(
            {
                "pg_dsn": pg_dsn,
                "schema_name": f"tsp_{new_base62()}".lower(),
            }
        )
        schema = settings.schema_name

        conn = await asyncpg.connect(str(settings.pg_dsn))
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await apply_pending(conn, schema=schema)
        finally:
            await conn.close()

        assert settings.pg_dsn_direct is not None

        stack = AsyncExitStack()
        deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
        try:
            worker_id = new_uuid()
            async with deps.worker_pool.acquire() as conn:
                await create_worker(conn, schema, worker_id)
                job_id = await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    max_attempts=1,
                    attempt=1,
                )

            shutdown = asyncio.Event()
            await isolate_self(deps, worker_id, shutdown)
            assert shutdown.is_set()

            async with deps.worker_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT status, error_class, finished_at FROM "{schema}".jobs WHERE id = $1',
                    job_id,
                )
                attempts = await conn.fetch(
                    f'SELECT outcome, error_class FROM "{schema}".job_attempts WHERE job_id = $1',
                    job_id,
                )
            assert row is not None
            assert row["status"] == "crashed"
            assert row["error_class"] is None
            assert row["finished_at"] is not None
            assert len(attempts) >= 1
            assert attempts[-1]["outcome"] == "crashed"
            assert attempts[-1]["error_class"] == "HeartbeatLost"
        finally:
            await stack.aclose()
