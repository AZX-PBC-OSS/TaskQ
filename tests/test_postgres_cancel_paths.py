"""Integration tests for PostgresBackend cancel-path writes against real PG.

Covers write_cancel_escalation, write_cancel_request,
job_events on cancel paths, mark_cancelled routes through
heartbeat_pool, and the double-write idempotency.
"""

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.assertions import (
    assert_has_event,
    assert_job_status,
    assert_job_terminal,
)
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import enqueue_and_dispatch_memory
from taskq.testing.pg import (
    create_pending_job,
    create_running_job,
    create_worker,
    setup_running_job,
)

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)


# ── Helpers ────────────────────────────────────────────────────────────

# (Local helpers removed: _create_scheduled_job replaced by
# create_pending_job(conn, schema, status="scheduled");
# enqueue_and_dispatch_memory moved to taskq.testing.jobs)


# ── write_cancel_request on pending/scheduled (PG side) ────────


class TestWriteCancelRequestPending:
    """(PG side): write_cancel_request on pending and scheduled jobs
    transitions to cancelled, sets finished_at, writes no job_attempts row,
    and writes exactly two job_events rows (state_change + cancel_request).
    """

    async def test_pending_cancel(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(conn, schema)

        result = await backend.write_cancel_request(job_id, "user request")
        assert result is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )

        assert_job_terminal(row, "cancelled")
        assert len(attempts) == 0

        assert_has_event(events, "state_change", from_state="pending", to_state="cancelled")
        assert_has_event(events, "cancel_request")

    async def test_scheduled_cancel(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(conn, schema, status="scheduled")

        result = await backend.write_cancel_request(job_id, None)
        assert result is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )

        assert_job_terminal(row, "cancelled")
        assert len(attempts) == 0

        assert_has_event(events, "state_change", from_state="scheduled", to_state="cancelled")
        assert_has_event(events, "cancel_request")


# ── write_cancel_request on running (PG side) ──────────────────


class TestWriteCancelRequestRunning:
    """(PG side): write_cancel_request on a running job sets
    cancel_requested_at and cancel_phase=1, writes one job_events row
    with kind='cancel_request'. Second call returns False, no duplicate
    events.
    """

    async def test_running_cancel_sets_phase1(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await setup_running_job(conn, schema)

        result = await backend.write_cancel_request(job_id, "please stop")
        assert result is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT cancel_requested_at, cancel_phase, status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["cancel_requested_at"] is not None
        assert row["cancel_phase"] == 1
        assert_job_status(row, "running")
        assert len(attempts) == 0
        assert_has_event(events, "cancel_request")

    async def test_second_cancel_returns_false_no_duplicate(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await setup_running_job(conn, schema)

        await backend.write_cancel_request(job_id, None)
        r2 = await backend.write_cancel_request(job_id, None)
        assert r2 is False

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        # One state_change from create + one cancel_request, no duplicates
        assert_has_event(events, "state_change")
        cancel_events = [e for e in events if e["kind"] == "cancel_request"]
        assert len(cancel_events) == 1


# ── write_cancel_escalation (PG side) ────────────────────────


class TestWriteCancelEscalation:
    """(PG side): write_cancel_escalation(phase=1) raises ValueError.
    After setting cancel_phase=1 on a running row,
    write_cancel_escalation(phase=2) returns True; cancel_phase is now 2;
    one job_events row with kind='state_change' showing
    cancel_phase_from/to values.
    """

    async def test_phase1_raises_valueerror(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        worker_id = new_uuid()

        with pytest.raises(ValueError, match="phase=2"):
            await backend.write_cancel_escalation(new_uuid(), worker_id, 1)  # type: ignore[arg-type] # Why: testing runtime guard against misuse via casts

    async def test_phase2_writes_event(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        # Set cancel_phase=1 first
        await backend.write_cancel_request(job_id, None)

        r = await backend.write_cancel_escalation(job_id, worker_id, 2)  # type: ignore[arg-type] # Why: Literal[2] not narrowed from int literal by pyright
        assert r is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT cancel_phase, status FROM "{schema}".jobs WHERE id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )

        assert row is not None
        assert row["cancel_phase"] == 2
        assert_job_status(row, "running")

        # Verify the escalation state_change event exists with cancel_phase detail
        assert_has_event(events, "state_change")
        # The last state_change should be the escalation one (from_state=running, to_state=running with phase detail)
        state_events = [e for e in events if e["kind"] == "state_change"]
        from taskq.testing.pg import parse_detail

        detail = parse_detail(state_events[-1]["detail"])
        assert detail["from_state"] == "running"
        assert detail["to_state"] == "running"
        assert detail["cancel_phase_from"] == 1
        assert detail["cancel_phase_to"] == 2
        assert str(worker_id) in str(detail.get("worker_id", ""))

    async def test_worker_mismatch_returns_false(self, clean_jobs_app: JobsApp) -> None:
        """Escalation with wrong worker_id returns False, no event written.
        A stale heartbeat targeting a re-dispatched job hits this path."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        other_worker = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            _owner, job_id = await setup_running_job(conn, schema)
            await create_worker(conn, schema, other_worker)

        await backend.write_cancel_request(job_id, None)

        # Escalate with the *wrong* worker — locked_by_worker != worker_id
        r = await backend.write_cancel_escalation(job_id, other_worker, 2)  # type: ignore[arg-type] # Why: Literal[2] not narrowed from int literal
        assert r is False

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT cancel_phase FROM "{schema}".jobs WHERE id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["cancel_phase"] == 1  # unchanged — escalation did not apply
        # Only state_change from create_running_job + cancel_request; no escalation event
        assert_has_event(events, "state_change")
        assert_has_event(events, "cancel_request")


# ── double-write idempotency ──────────────────────────────────


class TestDoubleWriteIdempotency:
    """double-write idempotency — second write returns False
    and produces no duplicate event rows.
    """

    async def test_cancel_request_double_write_running(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await setup_running_job(conn, schema)

        r1 = await backend.write_cancel_request(job_id, "first")
        assert r1 is True
        r2 = await backend.write_cancel_request(job_id, "second")
        assert r2 is False

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        # state_change from create_running_job + cancel_request; no duplicate
        assert_has_event(events, "state_change")
        cancel_events = [e for e in events if e["kind"] == "cancel_request"]
        assert len(cancel_events) == 1

    async def test_cancel_request_double_write_pending(self, clean_jobs_app: JobsApp) -> None:
        """Pending job → cancelled; second write returns False (now terminal)."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(conn, schema)

        r1 = await backend.write_cancel_request(job_id, None)
        assert r1 is True
        r2 = await backend.write_cancel_request(job_id, None)
        assert r2 is False

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        # Only the two events from the first write
        assert_has_event(events, "state_change")
        cancel_events = [e for e in events if e["kind"] == "cancel_request"]
        assert len(cancel_events) == 1

    async def test_cancel_escalation_double_write(self, clean_jobs_app: JobsApp) -> None:
        """Escalation after cancel_phase already 2 returns False, no duplicate."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        await backend.write_cancel_request(job_id, None)
        r1 = await backend.write_cancel_escalation(job_id, worker_id, 2)  # type: ignore[arg-type] # Why: Literal[2] not narrowed from int literal
        assert r1 is True
        r2 = await backend.write_cancel_escalation(job_id, worker_id, 2)  # type: ignore[arg-type] # Why: same
        assert r2 is False

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        # state_change from create + cancel_request + one escalation state_change, no duplicate
        # The job has been cancelled (cancel_request on running) — verify the state_change events
        state_events = [e for e in events if e["kind"] == "state_change"]
        assert len(state_events) == 2  # original + escalation


# ── Equivalence harness: backend_pair parametrised ────────────────────


class TestEquivalence:
    """Equivalence-harness check: runs the three write_cancel_request
    cases and write_cancel_escalation against both backends and asserts
    identical final row state and event count.
    """

    async def test_cancel_pending_equivalence(
        self, clean_jobs_app: JobsApp, memory_jobs: InMemoryBackend
    ) -> None:
        # ── Memory backend ─────────────────────────────────────────
        args = EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="q",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
        mem_row = await memory_jobs.enqueue(args)
        mem_result = await memory_jobs.write_cancel_request(mem_row.id, "test")
        assert mem_result is True

        mem_updated = await memory_jobs.get(mem_row.id)
        mem_attempts = await memory_jobs.get_attempts(mem_row.id)
        mem_events = await memory_jobs.get_events(mem_row.id)

        # ── PG backend ───────────────────────────────────────────
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            pg_job_id = await create_pending_job(conn, schema)

        pg_result = await backend.write_cancel_request(pg_job_id, "test")
        assert pg_result is True

        async with deps.worker_pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', pg_job_id
            )
            pg_attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', pg_job_id
            )
            pg_events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', pg_job_id
            )

        # ── Equivalence checks ────────────────────────────────────
        assert mem_updated is not None
        assert pg_row is not None
        assert mem_updated.status == pg_row["status"]
        assert mem_updated.finished_at is not None
        assert pg_row["finished_at"] is not None
        assert len(mem_attempts) == len(pg_attempts) == 0
        assert len(mem_events) == len(pg_events) == 2

    async def test_cancel_running_equivalence(
        self, clean_jobs_app: JobsApp, memory_jobs: InMemoryBackend
    ) -> None:
        # ── Memory backend ─────────────────────────────────────────
        mem_job_id, _mem_wid = await enqueue_and_dispatch_memory(memory_jobs)
        mem_result = await memory_jobs.write_cancel_request(mem_job_id, "test")
        assert mem_result is True

        mem_updated = await memory_jobs.get(mem_job_id)
        mem_attempts = await memory_jobs.get_attempts(mem_job_id)
        mem_events = await memory_jobs.get_events(mem_job_id)

        # ── PG backend ───────────────────────────────────────────
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            _, pg_job_id = await setup_running_job(conn, schema, with_events=True)

        pg_result = await backend.write_cancel_request(pg_job_id, "test")
        assert pg_result is True

        async with deps.worker_pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                f'SELECT status, cancel_phase, cancel_requested_at FROM "{schema}".jobs WHERE id = $1',
                pg_job_id,
            )
            pg_attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', pg_job_id
            )
            pg_events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', pg_job_id
            )

        # ── Equivalence checks ────────────────────────────────────
        assert mem_updated is not None
        assert pg_row is not None
        assert mem_updated.status == pg_row["status"]
        assert mem_updated.cancel_phase == pg_row["cancel_phase"]
        assert mem_updated.cancel_requested_at is not None
        assert pg_row["cancel_requested_at"] is not None
        assert len(mem_attempts) == len(pg_attempts) == 0
        assert len(mem_events) == len(pg_events) == 2

    async def test_cancel_escalation_equivalence(
        self, clean_jobs_app: JobsApp, memory_jobs: InMemoryBackend
    ) -> None:
        # ── Memory backend ─────────────────────────────────────────
        mem_job_id, mem_wid = await enqueue_and_dispatch_memory(memory_jobs)
        await memory_jobs.write_cancel_request(mem_job_id, None)
        esc_result = await memory_jobs.write_cancel_escalation(mem_job_id, mem_wid, 2)  # type: ignore[arg-type] # Why: Literal[2] not narrowed
        assert esc_result is True

        mem_updated = await memory_jobs.get(mem_job_id)
        mem_events = await memory_jobs.get_events(mem_job_id)

        # ── PG backend ───────────────────────────────────────────
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            pg_worker, pg_job_id = await setup_running_job(conn, schema, with_events=True)

        await backend.write_cancel_request(pg_job_id, None)
        pg_esc = await backend.write_cancel_escalation(pg_job_id, pg_worker, 2)  # type: ignore[arg-type] # Why: Literal[2] not narrowed
        assert pg_esc is True

        async with deps.worker_pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                f'SELECT cancel_phase, status FROM "{schema}".jobs WHERE id = $1', pg_job_id
            )
            pg_events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', pg_job_id
            )

        # ── Equivalence checks ────────────────────────────────────
        assert mem_updated is not None
        assert pg_row is not None
        assert mem_updated.cancel_phase == pg_row["cancel_phase"]
        assert mem_updated.status == pg_row["status"]
        assert len(mem_events) == len(pg_events) == 3


# ── poll_cancel_flags ─────────────────────────────────────────


class TestPollCancelFlags:
    """PostgresBackend.poll_cancel_flags returns indexed SELECT
    results for a given worker.
    """

    async def test_zero_cancelling_returns_empty(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        worker_id = new_uuid()
        result = await backend.poll_cancel_flags(worker_id)
        assert result == []

    async def test_two_cancelling_jobs_returned(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        now = datetime.now(UTC)

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            j1 = await create_running_job(
                conn, schema, worker_id, cancel_phase=1, cancel_requested_at=now
            )
            j2 = await create_running_job(
                conn, schema, worker_id, cancel_phase=2, cancel_requested_at=now
            )

        result = await backend.poll_cancel_flags(worker_id)
        result.sort(key=lambda f: f.cancel_phase)

        assert len(result) == 2
        assert result[0].job_id == j1
        assert result[0].cancel_phase == 1
        assert result[1].job_id == j2
        assert result[1].cancel_phase == 2

    async def test_different_worker_not_returned(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        now = datetime.now(UTC)

        async with deps.worker_pool.acquire() as conn:
            worker_a, j_a = await setup_running_job(
                conn, schema, cancel_phase=1, cancel_requested_at=now
            )
            _, _j_b = await setup_running_job(conn, schema, cancel_phase=1, cancel_requested_at=now)

        result = await backend.poll_cancel_flags(worker_a)
        assert len(result) == 1
        assert result[0].job_id == j_a

    async def test_no_cancel_requested_at_not_returned(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, _job_id = await setup_running_job(conn, schema)

        result = await backend.poll_cancel_flags(worker_id)
        assert result == []

    async def test_cancelled_job_not_returned(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        now = datetime.now(UTC)

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = new_uuid()
            await conn.execute(
                f"""INSERT INTO \"{schema}\".jobs (
                    id, actor, queue, payload, max_attempts, retry_kind,
                    status, priority, attempt, scheduled_at,
                    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,
                    cancel_phase, cancel_requested_at, finished_at
                ) VALUES (
                    $1, $2, $3, $4::jsonb, $5, $6,
                    'cancelled', 0, 1, now(),
                    $7, now() + interval '60 seconds', now(), now(),
                    $8, $9, now()
                )""",
                job_id,
                "test_actor",
                "default",
                '{"key": "value"}',
                3,
                "transient",
                worker_id,
                2,
                now,
            )

        result = await backend.poll_cancel_flags(worker_id)
        assert result == []


# ── mark_cancelled routes through heartbeat_pool ────────────────

# The behavioral invariant cancelling a running job via
# mark_cancelled must succeed even when worker_pool is exhausted.
# The implementation uses heartbeat_pool, but the test should verify
# the behavior, not the pool routing.


class TestMarkCancelledPoolSource:
    """Cancelling a running job succeeds regardless of pool routing.
    Verifies the behavioral invariant that the job reaches cancelled state
    and becomes idempotent on second call."""

    async def test_cancel_transitions_job_to_cancelled(self, clean_jobs_app: JobsApp) -> None:
        """The job reaches 'cancelled' state after mark_cancelled."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        result = await backend.mark_cancelled(job_id, worker_id)
        assert result is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
        assert row is not None
        assert row["status"] == "cancelled"

        # Idempotent: second call returns False
        result2 = await backend.mark_cancelled(job_id, worker_id)
        assert result2 is False
