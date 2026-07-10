"""Integration tests for PostgresBackend sweep static methods.

Covers Sweep 1 (sweep_expired_locks), Sweep 2
(sweep_deadline_exceeded), Sweep 3 (sweep_scheduled_to_pending),
and Sweep 4 (sweep_leaked_reservation_slots).
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_pending_job, create_running_job, create_worker

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration

# ── Helpers ────────────────────────────────────────────────────────────

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)


async def _create_reservation_slot(
    conn: _Conn,
    schema: str,
    bucket_name: str = "test_bucket",
    slot_index: int = 0,
    job_id: UUID | None = None,
    held_by_worker_id: UUID | None = None,
    lease_expires_at: datetime | None = None,
) -> None:
    """Insert a reservation_slots row."""
    await conn.execute(
        f"""INSERT INTO \"{schema}\".reservation_slots
            (bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at)
        VALUES ($1, $2, $3, $4, $5, $6)""",
        bucket_name,
        slot_index,
        job_id,
        held_by_worker_id,
        datetime.now(UTC) if job_id else None,
        lease_expires_at,
    )


# ── sweep_expired_locks ────────────────────────────────────────


class TestSweepExpiredLocks:
    """sweep_expired_locks (Sweep 1)."""

    async def test_pending_branch_attempts_available(self, clean_jobs_app: JobsApp) -> None:
        """Running job with expired lock, attempts remaining → pending
        with scheduled_at advanced by ~5 seconds."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            # Lock expired in the past, cancel_phase=0
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=3,
                retry_kind="transient",
            )

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                datetime.now(UTC),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker, lock_expires_at, scheduled_at, now() AS pg_now FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "pending"
        assert row["locked_by_worker"] is None
        assert row["lock_expires_at"] is None
        # scheduled_at should be advanced to ~now() + 5s (the re-queue
        # backoff). Allow 2-second tolerance for test execution latency.
        pg_now: datetime = row["pg_now"]
        scheduled_at: datetime = row["scheduled_at"]
        expected_min = pg_now + timedelta(seconds=3)
        expected_max = pg_now + timedelta(seconds=7)
        assert expected_min <= scheduled_at <= expected_max, (
            f"scheduled_at {scheduled_at} not in expected range [{expected_min}, {expected_max}]"
        )
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "crashed"
        assert attempts[0]["error_class"] == "WorkerCrashed"
        assert attempts[0]["worker_id"] == worker_id
        assert len(events) == 2
        assert events[0]["kind"] == "state_change"

    async def test_crashed_branch_attempts_exhausted(self, clean_jobs_app: JobsApp) -> None:
        """Running job with expired lock, no retries left → crashed."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=1,
                retry_kind="transient",
                attempt=1,
            )

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                datetime.now(UTC),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker, lock_expires_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "crashed"
        assert row["locked_by_worker"] is None
        assert row["lock_expires_at"] is None
        assert row["finished_at"] is not None
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "crashed"
        assert attempts[0]["worker_id"] == worker_id
        assert len(events) == 2
        assert events[0]["kind"] == "state_change"

    async def test_crashed_branch_non_retryable(self, clean_jobs_app: JobsApp) -> None:
        """Running job with expired lock, non_retryable → crashed with
        finished_at set and scheduled_at unchanged."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=3,
                retry_kind="non_retryable",
            )

            # Capture pre-sweep scheduled_at to verify it is not advanced
            pre_row = await conn.fetchrow(
                f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            pre_scheduled_at = pre_row["scheduled_at"] if pre_row else None

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                datetime.now(UTC),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at, scheduled_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "crashed"
        # Terminal job must have finished_at set
        assert row["finished_at"] is not None
        # scheduled_at must NOT be advanced to now()+5s — the job is
        # not being re-queued
        assert row["scheduled_at"] == pre_scheduled_at
        # Both job_attempts and job_events rows must exist
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "crashed"
        assert attempts[0]["worker_id"] == worker_id
        assert len(events) == 2
        assert events[0]["kind"] == "state_change"

    async def test_cancel_phase_carve_out_not_touched(self, clean_jobs_app: JobsApp) -> None:
        """Running job with cancel_phase=1 and lock slightly past now
        should NOT be swept — the cancel grace extension applies."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            # cancel_phase=1, lock just expired but within grace window
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=5),
                cancel_phase=1,
            )

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                datetime.now(UTC),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 0

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)

        assert row is not None
        assert row["status"] == "running"

    async def test_cancel_phase_carve_out_deeply_expired(self, clean_jobs_app: JobsApp) -> None:
        """Running job with cancel_phase=1 and lock expired past the
        cancel_grace + cleanup_grace + 60s threshold SHOULD be swept."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        # The threshold is: now() - cancel_grace - cleanup_grace - 60s
        # So the lock must have expired more than 30 + 30 + 60 = 120s ago
        deep_past = datetime.now(UTC) - timedelta(seconds=180)

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=deep_past,
                cancel_phase=1,
            )

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                datetime.now(UTC),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)

        assert row is not None
        assert row["status"] in ("pending", "crashed")

    async def test_event_detail_contains_reason(self, clean_jobs_app: JobsApp) -> None:
        """job_events detail should include reason='lock_expired'."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
            )

            await PostgresBackend.sweep_expired_locks(
                conn,
                datetime.now(UTC),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert len(events) == 2
        # Two events: pending->running (from create_running_job) + running->pending (from sweep).
        # The sweep event is the one with from_state='running'.
        sweep_events = [e for e in events if e["kind"] == "state_change"]
        sweep_event = sweep_events[-1]  # the last state_change event (from sweep)
        detail = sweep_event["detail"]
        if isinstance(detail, str):
            from taskq._json import loads

            detail = loads(detail)
        assert detail["from_state"] == "running"
        assert detail["to_state"] == "pending"
        assert detail["reason"] == "lock_expired"

    async def test_no_rows_affected_returns_zero(self, clean_jobs_app: JobsApp) -> None:
        """No expired locks → returns 0."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            count = await PostgresBackend.sweep_expired_locks(
                conn,
                datetime.now(UTC),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 0


# ── sweep_deadline_exceeded ────────────────────────────────────


class TestSweepDeadlineExceeded:
    """sweep_deadline_exceeded (Sweep 2)."""

    async def test_pending_job_deadline_exceeded(self, clean_jobs_app: JobsApp) -> None:
        """Pending job with schedule_to_close in the past → failed."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                status="pending",
            )

            count = await PostgresBackend.sweep_deadline_exceeded(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )

        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"
        assert row["finished_at"] is not None

        # Exactly one job_attempts row
        # started_at uses COALESCE($3, now()) so never-dispatched jobs satisfy
        # the NOT NULL constraint.
        assert len(attempts) == 1
        attempt = attempts[0]
        assert attempt["outcome"] == "failed"
        assert attempt["error_class"] == "DeadlineExceeded"
        # never-dispatched job: started_at was NULL on jobs row, COALESCE'd
        # to now() — the row exists with a non-null started_at value.
        assert attempt["started_at"] is not None
        assert attempt["worker_id"] is None  # never dispatched, no owner

        # Exactly one job_events row
        assert len(events) == 1
        assert events[0]["kind"] == "state_change"
        detail = events[0]["detail"]
        if isinstance(detail, str):
            from taskq._json import loads

            detail = loads(detail)
        assert detail["from_state"] == "pending"
        assert detail["to_state"] == "failed"
        assert detail["error_class"] == "DeadlineExceeded"

    async def test_scheduled_job_deadline_exceeded(self, clean_jobs_app: JobsApp) -> None:
        """Scheduled job with schedule_to_close in the past → failed,
        event detail has from_state='scheduled'."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                status="scheduled",
            )

            count = await PostgresBackend.sweep_deadline_exceeded(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"
        # Exactly one job_attempts row
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "DeadlineExceeded"
        assert attempts[0]["started_at"] is not None  # COALESCE'd to now()
        assert attempts[0]["worker_id"] is None
        assert len(events) == 1
        detail = events[0]["detail"]
        if isinstance(detail, str):
            from taskq._json import loads

            detail = loads(detail)
        assert detail["from_state"] == "scheduled"
        assert detail["to_state"] == "failed"

    async def test_no_rows_affected_returns_zero(self, clean_jobs_app: JobsApp) -> None:
        """No deadline-exceeded jobs → returns 0."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            count = await PostgresBackend.sweep_deadline_exceeded(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 0

    async def test_running_job_not_touched(self, clean_jobs_app: JobsApp) -> None:
        """Running jobs with schedule_to_close in the past are NOT
        swept — Sweep 2 only targets pending/scheduled."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            # Running job with expired schedule_to_close
            await conn.execute(
                f"""INSERT INTO \"{schema}\".jobs (
                    id, actor, queue, payload, max_attempts, retry_kind,
                    status, priority, scheduled_at, schedule_to_close,
                    locked_by_worker, lock_expires_at, started_at
                ) VALUES (
                    $1, $2, $3, $4::jsonb, $5, $6,
                    'running', 0, now(), $7,
                    $8, now() + interval '60 seconds', now()
                )""",
                new_uuid(),
                "test_actor",
                "default",
                '{"key": "value"}',
                3,
                "transient",
                datetime.now(UTC) - timedelta(seconds=10),
                worker_id,
            )

            count = await PostgresBackend.sweep_deadline_exceeded(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 0


# ── Sweep 4: sweep_leaked_reservation_slots ──────────────────────────


class TestSweep4:
    """Sweep 4: sweep_leaked_reservation_slots."""

    async def test_leaked_slot_released(self, clean_jobs_app: JobsApp) -> None:
        """Reservation slot with expired lease → job_id, held_by_worker_id,
        acquired_at, lease_expires_at cleared."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        slot_job_id = new_uuid()
        slot_worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_pending_job(conn, schema, job_id=slot_job_id)
            await _create_reservation_slot(
                conn,
                schema,
                lease_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                job_id=slot_job_id,
                held_by_worker_id=slot_worker_id,
            )

            count = await PostgresBackend.sweep_leaked_reservation_slots(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT job_id, held_by_worker_id, acquired_at, lease_expires_at FROM "{schema}".reservation_slots'
            )

        assert row is not None
        assert row["job_id"] is None
        assert row["held_by_worker_id"] is None
        assert row["acquired_at"] is None
        assert row["lease_expires_at"] is None

    async def test_active_slot_not_touched(self, clean_jobs_app: JobsApp) -> None:
        """Reservation slot with valid lease should NOT be swept."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        slot_job_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            # Create a job row first (FK requirement)
            await create_pending_job(conn, schema, job_id=slot_job_id)
            await _create_reservation_slot(
                conn,
                schema,
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
                job_id=slot_job_id,
            )

            count = await PostgresBackend.sweep_leaked_reservation_slots(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 0

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT job_id FROM "{schema}".reservation_slots')

        assert row is not None
        assert row["job_id"] == slot_job_id

    async def test_no_rows_affected_returns_zero(self, clean_jobs_app: JobsApp) -> None:
        """No expired slots → returns 0."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            count = await PostgresBackend.sweep_leaked_reservation_slots(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 0

    async def test_multiple_leaked_slots(self, clean_jobs_app: JobsApp) -> None:
        """Multiple leaked slots are all released in one call."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        job_id_0 = new_uuid()
        job_id_1 = new_uuid()
        job_id_2 = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            # Create job rows first (FK requirement)
            await create_pending_job(conn, schema, job_id=job_id_0)
            await create_pending_job(conn, schema, job_id=job_id_1)
            await create_pending_job(conn, schema, job_id=job_id_2)

            await _create_reservation_slot(
                conn,
                schema,
                bucket_name="b1",
                slot_index=0,
                lease_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                job_id=job_id_0,
            )
            await _create_reservation_slot(
                conn,
                schema,
                bucket_name="b1",
                slot_index=1,
                lease_expires_at=datetime.now(UTC) - timedelta(seconds=20),
                job_id=job_id_1,
            )
            # Active slot should not be touched
            await _create_reservation_slot(
                conn,
                schema,
                bucket_name="b1",
                slot_index=2,
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
                job_id=job_id_2,
            )

            count = await PostgresBackend.sweep_leaked_reservation_slots(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 2


# ── consumer-side vs leader-side attempt-row shape ─────────────


class TestConsumerVsLeaderAttemptRowShape:
    """consumer-side vs leader-side DeadlineExceeded attempt-row shape.

    Two independent paths produce DeadlineExceeded rows with different
    ``job_attempts`` shapes. This test asserts both side-by-side so the
    boundary is visible from a single read.
    """

    async def test_consumer_side_snooze_past_deadline_attempt_shape(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Consumer-side: mark_snoozed past schedule_to_close → attempt row
        has real started_at and non-NULL worker_id."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        job_id = new_uuid()
        deadline = datetime.now(UTC) + timedelta(seconds=5)

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                job_id=job_id,
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs SET schedule_to_close = $1 WHERE id = $2',
                deadline,
                job_id,
            )
            dispatched_row = await conn.fetchrow(
                f'SELECT started_at FROM "{schema}".jobs WHERE id = $1', job_id
            )

        dispatched_started_at = dispatched_row["started_at"] if dispatched_row else None
        assert dispatched_started_at is not None

        result = await backend.mark_snoozed(JobId(job_id), worker_id, timedelta(seconds=30))
        assert result == "failed"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "DeadlineExceeded"
        assert attempts[0]["started_at"] == dispatched_started_at
        assert attempts[0]["worker_id"] is not None
        assert attempts[0]["worker_id"] == worker_id

    async def test_leader_side_sweep_deadline_attempt_shape(self, clean_jobs_app: JobsApp) -> None:
        """Leader-side: sweep_deadline_exceeded on never-dispatched job →
        attempt row has COALESCE'd started_at (~now()) and NULL worker_id."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                status="pending",
            )

            sweep_before = datetime.now(UTC)
            count = await PostgresBackend.sweep_deadline_exceeded(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "DeadlineExceeded"
        assert attempts[0]["started_at"] is not None
        assert attempts[0]["worker_id"] is None
        started_at: datetime = attempts[0]["started_at"]
        assert abs((started_at - sweep_before).total_seconds()) < 5


# ── sweep_scheduled_to_pending (Sweep 3) ──────────────────────


class TestSweepScheduledToPending:
    """sweep_scheduled_to_pending (Sweep 3)."""

    async def test_scheduled_job_past_scheduled_at_promoted(self, clean_jobs_app: JobsApp) -> None:
        """Scheduled job with scheduled_at in the past → promoted to
        pending, one state_change event row written."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                status="scheduled",
            )
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET scheduled_at = now() - interval '10 seconds' WHERE id = $1",
                job_id,
            )

            count = await PostgresBackend.sweep_scheduled_to_pending(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1',
                job_id,
            )

        assert row is not None
        assert row["status"] == "pending"
        assert len(events) == 1
        assert events[0]["kind"] == "state_change"
        detail = events[0]["detail"]
        if isinstance(detail, str):
            from taskq._json import loads

            detail = loads(detail)
        assert detail["from_state"] == "scheduled"
        assert detail["to_state"] == "pending"

    async def test_scheduled_job_future_scheduled_at_not_promoted(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Scheduled job with scheduled_at in the future → not promoted."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                status="scheduled",
            )
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET scheduled_at = now() + interval '1 hour' WHERE id = $1",
                job_id,
            )

            count = await PostgresBackend.sweep_scheduled_to_pending(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 0

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1',
                job_id,
            )

        assert row is not None
        assert row["status"] == "scheduled"
        assert len(events) == 0

    async def test_pending_job_not_affected(self, clean_jobs_app: JobsApp) -> None:
        """Pending jobs are not touched by the scheduled→pending sweep."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                status="pending",
            )

            count = await PostgresBackend.sweep_scheduled_to_pending(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 0

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )

        assert row is not None
        assert row["status"] == "pending"

    async def test_event_detail_per_promoted_row(self, clean_jobs_app: JobsApp) -> None:
        """Each promoted row produces one kind='state_change' event with
        from_state='scheduled' and to_state='pending'."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id_1 = await create_pending_job(conn, schema, status="scheduled")
            job_id_2 = await create_pending_job(conn, schema, status="scheduled")
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET scheduled_at = now() - interval '5 seconds' WHERE id IN ($1, $2)",
                job_id_1,
                job_id_2,
            )

            count = await PostgresBackend.sweep_scheduled_to_pending(
                conn,
                datetime.now(UTC),
                schema=schema,
            )

        assert count == 2

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT job_id, kind, detail FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[]) ORDER BY job_id',
                [job_id_1, job_id_2],
            )

        assert len(events) == 2
        for ev in events:
            assert ev["kind"] == "state_change"
            detail = ev["detail"]
            if isinstance(detail, str):
                from taskq._json import loads

                detail = loads(detail)
            assert detail["from_state"] == "scheduled"
            assert detail["to_state"] == "pending"


# ── reclaim_expired_locks instance method ──────────────────────────────


class TestReclaimExpiredLocksInstance:
    """Integration tests for PostgresBackend.reclaim_expired_locks instance
    method — the delegation surface the leader's _sweep_loop calls.

    These exercises go through the instance method (which acquires a
    connection from _notify_pool) rather than calling the static
    sweep_expired_locks directly, ensuring the delegation path works
    end-to-end against real PG.
    """

    async def test_expired_lock_with_retries_moves_to_pending(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Running job with expired lock and retries remaining → pending
        with backoff applied (scheduled_at ≈ now() + 5s)."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=3,
                retry_kind="transient",
            )

        count = await backend.reclaim_expired_locks(
            datetime.now(UTC),
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
        )
        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker, lock_expires_at, scheduled_at, now() AS pg_now FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )

        assert row is not None
        assert row["status"] == "pending"
        assert row["locked_by_worker"] is None
        assert row["lock_expires_at"] is None
        pg_now: datetime = row["pg_now"]
        scheduled_at: datetime = row["scheduled_at"]
        expected_min = pg_now + timedelta(seconds=3)
        expected_max = pg_now + timedelta(seconds=7)
        assert expected_min <= scheduled_at <= expected_max, (
            f"scheduled_at {scheduled_at} not in expected range [{expected_min}, {expected_max}]"
        )

    async def test_expired_lock_no_retries_moves_to_crashed(self, clean_jobs_app: JobsApp) -> None:
        """Running job with expired lock and retries exhausted → crashed
        with error_class='WorkerCrashed'."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=1,
                retry_kind="transient",
                attempt=1,
            )

        count = await backend.reclaim_expired_locks(
            datetime.now(UTC),
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
        )
        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker, lock_expires_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "crashed"
        assert row["locked_by_worker"] is None
        assert row["lock_expires_at"] is None
        assert row["finished_at"] is not None
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "crashed"
        assert attempts[0]["error_class"] == "WorkerCrashed"

    async def test_valid_lock_not_affected(self, clean_jobs_app: JobsApp) -> None:
        """Running job with lock_expires_at in the future is not affected."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=60),
            )

        count = await backend.reclaim_expired_locks(
            datetime.now(UTC),
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
        )
        assert count == 0

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )

        assert row is not None
        assert row["status"] == "running"
        assert row["locked_by_worker"] == worker_id
