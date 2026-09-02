"""Integration tests for PostgresBackend terminal writes against real PG.

Covers mark_succeeded, mark_failed_or_retry,
mark_cancelled, mark_abandoned, mark_snoozed,
job_attempts, job_events,
WorkerOwnershipMismatch, and PayloadValidationError write surface.
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.assertions import (
    assert_has_event,
    assert_job_status,
    assert_job_terminal,
)
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import enqueue_and_dispatch_memory
from taskq.testing.pg import create_worker, setup_running_job

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration

# ── Helpers ────────────────────────────────────────────────────────────

_GRACE = timedelta(seconds=30)

# (Local enqueue_and_dispatch_memory removed — now in taskq.testing.jobs)


# ── terminal writes actually update the row in PG ───────────────


class TestTerminalWritesUpdateRow:
    """each terminal write actually updates the row in PG."""

    async def test_mark_succeeded(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        result = await backend.mark_succeeded(job_id, worker_id, {"ok": True})
        assert result is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert_job_terminal(row, "succeeded")

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "succeeded"
        assert_has_event(events, "state_change")

    async def test_mark_cancelled(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        result = await backend.mark_cancelled(job_id, worker_id)
        assert result is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert_job_terminal(row, "cancelled")

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "cancelled"
        assert_has_event(events, "state_change")

    async def test_mark_abandoned(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await setup_running_job(conn, schema, cancel_phase=2)

        result = await backend.mark_abandoned(job_id)
        assert result is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        assert_job_terminal(row, "abandoned")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "cancelled"
        assert_has_event(events, "state_change")

    async def test_mark_failed_terminal(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, max_attempts=1)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        row = await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)
        assert row.status == "failed"
        assert row.finished_at is not None

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert_has_event(events, "state_change")

    async def test_mark_failed_retry(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, max_attempts=3)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="transient",
            error_traceback=None,
        )
        # The decision is a delay — the server derives scheduled_at from it.
        row = await backend.mark_failed_or_retry(
            job_id, worker_id, error_info, timedelta(seconds=10)
        )
        assert row.status == "scheduled"
        assert row.locked_by_worker is None
        assert row.lock_expires_at is None

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert_has_event(events, "state_change")

    async def test_mark_snoozed(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        row = assert_job_status(row, "scheduled")
        assert row["locked_by_worker"] is None
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "snoozed"
        assert_has_event(events, "state_change")


# ── CHECK constraints fire ─────────────────────────────────────


class TestCheckConstraints:
    """CHECK constraints fire on invalid data."""

    async def test_cancel_phase_3_raises(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    f'INSERT INTO "{schema}".jobs '
                    "(id, actor, queue, payload, max_attempts, retry_kind, "
                    "status, scheduled_at, cancel_phase) "
                    "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'pending', now(), 3)",
                    new_uuid(),
                    "a",
                    "q",
                    "{}",
                    3,
                    "transient",
                )

    async def test_retry_kind_bogus_raises(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    f'INSERT INTO "{schema}".jobs '
                    "(id, actor, queue, payload, max_attempts, retry_kind, "
                    "status, scheduled_at) "
                    "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'pending', now())",
                    new_uuid(),
                    "a",
                    "q",
                    "{}",
                    3,
                    "bogus",
                )


# ── job_events rows match transitions ──────────────────────────


class TestJobEventsMatchTransitions:
    """job_events rows match state transitions."""

    async def test_mark_succeeded_event(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        await backend.mark_succeeded(job_id, worker_id, {"ok": True})

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )
        ev = assert_has_event(events, "state_change", from_state="running", to_state="succeeded")
        assert str(worker_id) in str(ev["detail"])


# ── job_attempts cascade on parent delete ──────────────────────


class TestJobAttemptsCascadeDelete:
    """job_attempts cascade on parent delete (ON DELETE CASCADE)."""

    async def test_cascade_delete(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        await backend.mark_succeeded(job_id, worker_id, {"ok": True})

        async with deps.worker_pool.acquire() as conn:
            # Verify attempt and event rows exist
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        assert len(attempts) == 1
        assert_has_event(events, "state_change")

        async with deps.worker_pool.acquire() as conn:
            # Delete the jobs row directly
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', job_id)
            # Verify cascade
            attempts_after = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events_after = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
        assert len(attempts_after) == 0
        assert len(events_after) == 0


# ── (PG side): wrong worker_id handling ─────────────────────────


class TestWrongWorkerIdPG:
    """(PG): wrong worker_id on bool-returning methods returns False;
    mark_failed_or_retry raises WorkerOwnershipMismatch."""

    async def test_mark_succeeded_wrong_worker_false(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        wrong_worker = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await setup_running_job(conn, schema)

        result = await backend.mark_succeeded(job_id, wrong_worker, None)
        assert result is False

    async def test_mark_cancelled_wrong_worker_false(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        wrong_worker = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await setup_running_job(conn, schema)

        result = await backend.mark_cancelled(job_id, wrong_worker)
        assert result is False

    async def test_mark_failed_or_retry_wrong_worker_raises(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        wrong_worker = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch) as exc_info:
            await backend.mark_failed_or_retry(job_id, wrong_worker, error_info, None)
        assert exc_info.value.job_id == job_id
        assert exc_info.value.expected == wrong_worker
        assert exc_info.value.actual == worker_id

    @pytest.mark.parametrize(
        ("arm", "schedule_to_close"),
        [
            ("retried", None),
            ("deadline_failed", datetime(2020, 1, 1, tzinfo=UTC)),
        ],
    )
    async def test_mark_retry_wrong_worker_raises_on_both_arms(
        self,
        clean_jobs_app: JobsApp,
        arm: str,
        schedule_to_close: datetime | None,
    ) -> None:
        """Both ``mark_retry`` arms are fenced on ``locked_by_worker``.

        Why this needs its own test: ``mark_failed_or_retry`` routes on
        ``retry_delay is None`` alone, so the sibling wrong-worker test above
        — which passes ``None`` — only ever reaches ``mark_failed``.  Neither
        arm of the ``mark_retry`` CTE was covered by any wrong-worker test.

        The exposure is a double-execution, not a bookkeeping slip: a worker
        whose lease expired has had its job reclaimed by sweep 1 and
        re-dispatched to a *different* worker.  If its late
        ``mark_failed_or_retry`` were unfenced it would push that live
        attempt back to pending/scheduled, and the actor would run twice
        concurrently.  ``retry_delay`` past ``schedule_to_close`` takes the
        ``deadline_failed`` arm instead, which would clobber the live attempt
        into a terminal ``failed`` — so both arms are pinned here.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        wrong_worker = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            # A registered worker, not a bare UUID: the scenario is a job the
            # sweep re-dispatched to a real second worker, and only a real
            # workers row lets an unfenced write land instead of tripping the
            # job_attempts FK — which would mask the fence failure as an
            # unrelated database error.
            await create_worker(conn, schema, wrong_worker)
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                attempt=1,
                max_attempts=3,
                retry_kind="transient",
                schedule_to_close=schedule_to_close,
            )
            before = await conn.fetchrow(
                "SELECT status, attempt, scheduled_at, locked_by_worker "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch) as exc_info:
            await backend.mark_failed_or_retry(
                job_id, wrong_worker, error_info, timedelta(seconds=30)
            )
        assert exc_info.value.actual == worker_id

        async with deps.worker_pool.acquire() as conn:
            after = await conn.fetchrow(
                "SELECT status, attempt, scheduled_at, locked_by_worker "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )

        assert before is not None and after is not None
        assert dict(after) == dict(before), (
            f"{arm} arm: a non-owning worker mutated the row it does not hold"
        )

    async def test_mark_failed_or_retry_missing_job_raises_with_none(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """When the job row doesn't exist at all, actual should be None."""
        backend = clean_jobs_app.backend
        worker_id = new_uuid()
        missing_job = new_job_id()

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch) as exc_info:
            await backend.mark_failed_or_retry(missing_job, worker_id, error_info, None)
        assert exc_info.value.actual is None


# ── (PG side): PayloadValidationError write surface ────


class TestPayloadValidationErrorPG:
    """(PG side): PayloadValidationError path through
    mark_failed_or_retry. Verifies the write surface — the
    non-retryable classifier is enforced by the dispatch layer,
    not this write surface."""

    async def test_payload_validation_error_terminal_failure(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, max_attempts=1)

        raw_payload = {"bad": "data"}
        error_info = ErrorInfo(
            error_class="PayloadValidationError",
            error_message=str(raw_payload),
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)

        assert result.status == "failed"
        assert result.error_class == "PayloadValidationError"
        assert str(raw_payload) in (result.error_message or "")

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
                job_id,
            )
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "PayloadValidationError"

        assert_has_event(events, "state_change", to_state="failed")


# ── Equivalence harness check ──────────────────────────────────


class TestEquivalence:
    """exercise both backends in the same test and assert final
    row state is identical (status, finished_at presence, attempt count,
    event count) for mark_succeeded and mark_failed_or_retry."""

    async def test_mark_succeeded_equivalence(
        self, clean_jobs_app: JobsApp, memory_jobs: InMemoryBackend
    ) -> None:
        # ── Memory backend ─────────────────────────────────────────
        mem_job_id, mem_worker = await enqueue_and_dispatch_memory(memory_jobs)
        mem_result = await memory_jobs.mark_succeeded(mem_job_id, mem_worker, {"ok": True})
        assert mem_result is True
        mem_row = await memory_jobs.get(mem_job_id)
        mem_attempts = await memory_jobs.get_attempts(mem_job_id)
        mem_events = await memory_jobs.get_events(mem_job_id)

        # ── PG backend ───────────────────────────────────────────
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            pg_worker, pg_job_id = await setup_running_job(conn, schema, with_events=True)

        pg_result = await backend.mark_succeeded(pg_job_id, pg_worker, {"ok": True})
        assert pg_result is True

        async with deps.worker_pool.acquire() as conn:
            pg_row = await conn.fetchrow(f'SELECT * FROM "{schema}".jobs WHERE id = $1', pg_job_id)
            pg_attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', pg_job_id
            )
            pg_events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', pg_job_id
            )

        # ── Equivalence checks ────────────────────────────────────
        assert mem_row is not None
        assert pg_row is not None
        assert mem_row.status == pg_row["status"]
        assert mem_row.finished_at is not None
        assert pg_row["finished_at"] is not None
        assert len(mem_attempts) == len(pg_attempts)
        assert len(mem_events) == len(pg_events)

    async def test_mark_failed_or_retry_equivalence(
        self, clean_jobs_app: JobsApp, memory_jobs: InMemoryBackend
    ) -> None:
        # ── Memory backend ─────────────────────────────────────────
        mem_job_id, mem_worker = await enqueue_and_dispatch_memory(memory_jobs, max_attempts=1)
        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )
        mem_row = await memory_jobs.mark_failed_or_retry(mem_job_id, mem_worker, error_info, None)
        assert mem_row.status == "failed"
        mem_attempts = await memory_jobs.get_attempts(mem_job_id)
        mem_events = await memory_jobs.get_events(mem_job_id)

        # ── PG backend ───────────────────────────────────────────
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            pg_worker, pg_job_id = await setup_running_job(
                conn, schema, max_attempts=1, with_events=True
            )

        pg_row = await backend.mark_failed_or_retry(pg_job_id, pg_worker, error_info, None)
        assert pg_row.status == "failed"

        async with deps.worker_pool.acquire() as conn:
            pg_attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', pg_job_id
            )
            pg_events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', pg_job_id
            )

        # ── Equivalence checks ────────────────────────────────────
        assert mem_row.status == pg_row.status
        assert mem_row.finished_at is not None
        assert pg_row.finished_at is not None
        assert len(mem_attempts) == len(pg_attempts)
        assert len(mem_events) == len(pg_events)


# ── mark_snoozed preserves attempt ──────────────────────


class TestMarkSnoozedPreservesAttempt:
    """snooze preserves attempt — no budget consumption."""

    async def test_mark_snoozed_preserves_attempt(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, attempt=1)

        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT attempt, status FROM "{schema}".jobs WHERE id = $1', job_id
            )
        row = assert_job_status(row, "scheduled")
        assert row["attempt"] == 1

    async def test_mark_snoozed_attempt_record_preserves_attempt(
        self, clean_jobs_app: JobsApp
    ) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, attempt=1)

        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT attempt, outcome FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
        assert len(attempts) == 1
        assert attempts[0]["attempt"] == 1
        assert attempts[0]["outcome"] == "snoozed"


# ── mark_snoozed clears last_heartbeat_at ────────────────────────


class TestMarkSnoozedHeartbeat:
    """running→scheduled transition clears last_heartbeat_at."""

    async def test_mark_snoozed_clears_last_heartbeat_at(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

            pre = await conn.fetchrow(
                f'SELECT last_heartbeat_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert pre is not None
        assert pre["last_heartbeat_at"] is not None

        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT last_heartbeat_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert row is not None
        assert row["last_heartbeat_at"] is None


# ── snooze-past-deadline guard ───────────────────────────────────


class TestMarkSnoozedDeadline:
    """snooze past schedule_to_close transitions to failed."""

    async def test_mark_snoozed_past_deadline_fails(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        close_at = datetime.now(UTC) + timedelta(seconds=5)

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, schedule_to_close=close_at)

        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        assert result == "failed"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, error_class, error_message, finished_at, last_heartbeat_at "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
        row = assert_job_terminal(row, "failed", error_class="DeadlineExceeded")
        assert row["error_message"] == "schedule_to_close reached before next dispatch"
        assert row["last_heartbeat_at"] is None
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "DeadlineExceeded"

    async def test_mark_snoozed_within_deadline_succeeds(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        close_at = datetime.now(UTC) + timedelta(seconds=30)

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, schedule_to_close=close_at)

        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=5))
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1', job_id
            )
        row = assert_job_status(row, "scheduled")
        assert row["error_class"] is None


# ── outcome='reservation_denied' parameter ────────────────────────


class TestMarkSnoozedReservationDenied:
    """mark_snoozed with outcome='reservation_denied' writes the
    correct attempt outcome and metadata annotation."""

    async def test_mark_snoozed_outcome_reservation_denied(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        result = await backend.mark_snoozed(
            job_id,
            worker_id,
            timedelta(seconds=30),
            metadata_update={"awaiting": "reservation:gpu_pool"},
            outcome="reservation_denied",
        )
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            row = await conn.fetchrow(f'SELECT metadata FROM "{schema}".jobs WHERE id = $1', job_id)
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "reservation_denied"
        assert row is not None

        metadata: object = row["metadata"]
        if isinstance(metadata, str):
            from taskq._json import loads

            metadata = loads(metadata)
        assert isinstance(metadata, dict) and metadata.get("awaiting") == "reservation:gpu_pool"


# ── Idempotent noop: second call returns "noop" ────────────────────────


class TestMarkSnoozedIdempotent:
    """Second mark_snoozed call on an already-moved row returns 'noop'."""

    async def test_mark_snoozed_idempotent_returns_noop(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        result1 = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        assert result1 == "scheduled"

        result2 = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        assert result2 == "noop"

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
        assert len(attempts) == 1


# ── mark_retry_after consume_budget=True increments attempt ──────


class TestMarkRetryAfterConsumeTrue:
    """mark_retry_after with consume_budget=True preserves attempt
    (dispatch CTE is the sole increment point), transitions to scheduled,
    and writes RetryAfter attempt row."""

    async def test_mark_retry_after_consume_budget_true_increments_attempt(
        self, clean_jobs_app: JobsApp
    ) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, attempt=1)

        result = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=True
        )
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT attempt, status FROM "{schema}".jobs WHERE id = $1', job_id
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
        row = assert_job_status(row, "scheduled")
        assert row["attempt"] == 1
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "snoozed"
        assert attempts[0]["error_class"] == "RetryAfter"


# ── mark_retry_after consume_budget=False preserves attempt ──────


class TestMarkRetryAfterConsumeFalse:
    """mark_retry_after with consume_budget=False preserves attempt."""

    async def test_mark_retry_after_consume_budget_false_preserves_attempt(
        self, clean_jobs_app: JobsApp
    ) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, attempt=1)

        result = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=False
        )
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT attempt, status FROM "{schema}".jobs WHERE id = $1', job_id
            )
        row = assert_job_status(row, "scheduled")
        assert row["attempt"] == 1


# ── mark_retry_after max-attempts exceeded ──────────────────────


class TestMarkRetryAfterMaxAttempts:
    """mark_retry_after with exhausted budget transitions to failed."""

    async def test_mark_retry_after_max_attempts_exceeded_transitions_to_failed(
        self, clean_jobs_app: JobsApp
    ) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn, schema, max_attempts=3, retry_kind="transient", attempt=3
            )

        result = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=True
        )
        assert result == "failed:MaxAttemptsExceeded"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, error_class, error_message, finished_at "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
        assert_job_terminal(row, "failed", error_class="MaxAttemptsExceeded")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "MaxAttemptsExceeded"

    async def test_mark_retry_after_indefinite_tier_ignores_max_attempts(
        self, clean_jobs_app: JobsApp
    ) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn, schema, max_attempts=2, retry_kind="indefinite", attempt=5
            )

        result = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=True
        )
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
            )
        row = assert_job_status(row, "scheduled")
        assert row["attempt"] == 5


# ── mark_retry_after past deadline ──────────────────────────────


class TestMarkRetryAfterDeadline:
    """mark_retry_after past schedule_to_close transitions to failed."""

    async def test_mark_retry_after_past_deadline_fails(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        close_at = datetime.now(UTC) + timedelta(seconds=5)

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, schedule_to_close=close_at)

        result = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=True
        )
        assert result == "failed:DeadlineExceeded"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, error_class, error_message, finished_at, last_heartbeat_at, attempt "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
        row = assert_job_terminal(row, "failed", error_class="DeadlineExceeded")
        assert row["error_message"] == "schedule_to_close reached before next dispatch"
        assert row["last_heartbeat_at"] is None
        assert row["attempt"] == 1
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["error_class"] == "DeadlineExceeded"


# ── Idempotent noop: second mark_retry_after returns "noop" ──────────────


class TestMarkRetryAfterIdempotent:
    """Second mark_retry_after call on an already-moved row returns 'noop'."""

    async def test_mark_retry_after_idempotent_returns_noop(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        result1 = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=True
        )
        assert result1 == "scheduled"

        result2 = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=True
        )
        assert result2 == "noop"

        async with deps.worker_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
        assert len(attempts) == 1


# ── mark_retry_after clears last_heartbeat_at ───────────────────────────


class TestMarkRetryAfterHeartbeat:
    """mark_retry_after clears last_heartbeat_at on running→scheduled."""

    async def test_mark_retry_after_clears_last_heartbeat_at(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

            pre = await conn.fetchrow(
                f'SELECT last_heartbeat_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert pre is not None
        assert pre["last_heartbeat_at"] is not None

        result = await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=30), consume_budget=True
        )
        assert result == "scheduled"

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT last_heartbeat_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert row is not None
        assert row["last_heartbeat_at"] is None


class TestMarkSucceededResultExpiryFallback:
    """result_expires_at resolution at completion against real PG:
    stored override → fallback literal ($7) → enqueue-time value.

    Regression tier for the cleared-result_ttl bug: with the stored
    override cleared to NULL, the terminal write used to keep the
    enqueue-pinned expiry, so a job that sat in the queue longer than
    its TTL completed already expired and the sweep ate its result
    milliseconds later.
    """

    async def _seed_cleared_ttl_and_aged_job(
        self,
        jobs_app: JobsApp,
        conn: "_Conn",
        *,
        ttl_seconds: float = 5.0,
        queue_wait_seconds: float = 45.0,
    ) -> tuple[object, object]:
        """Run the reported repro's setup: an operator sets then CLEARS the
        stored result_ttl override, and a running job whose
        result_expires_at is pinned in the past — the state the enqueue
        path leaves behind (enqueue_now + literal) after the job sat in
        the queue longer than its TTL."""
        from taskq.actor_config_ops import set_actor_config_capacity

        deps = jobs_app.deps
        schema = deps.settings.schema_name
        await set_actor_config_capacity(conn, "test_actor", result_ttl=ttl_seconds, schema=schema)
        row = await set_actor_config_capacity(conn, "test_actor", result_ttl=None, schema=schema)
        assert row is not None and row.result_ttl is None

        worker_id, job_id = await setup_running_job(conn, schema)
        await conn.execute(
            f'UPDATE "{schema}"."jobs" SET result_expires_at = now() + make_interval(secs => $1) WHERE id = $2',
            ttl_seconds - queue_wait_seconds,  # negative → pinned in the past
            job_id,
        )
        return worker_id, job_id

    async def test_cleared_result_ttl_falls_back_to_literal_at_completion(
        self, clean_jobs_app: JobsApp
    ) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await self._seed_cleared_ttl_and_aged_job(clean_jobs_app, conn)

        ttl = timedelta(seconds=5)
        ok = await backend.mark_succeeded(job_id, worker_id, {"ok": True}, fallback_result_ttl=ttl)
        assert ok is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT result, result_expires_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["result"] is not None
            # Expiry recomputed from completion (clock_timestamp() at the
            # write — it may lead finished_at = now() by statement
            # execution time): ≈ completion+5s, never 40s in the past.
            skew = row["result_expires_at"] - row["finished_at"]
            assert timedelta(seconds=4) < skew < timedelta(seconds=6)

            # The sweep must leave the result alone.
            swept = await backend.sweep_expired_results(conn, schema=schema)
            assert swept == 0
            row2 = await conn.fetchrow(f'SELECT result FROM "{schema}".jobs WHERE id = $1', job_id)
            assert row2 is not None and row2["result"] is not None

    async def test_cleared_result_ttl_without_fallback_is_reaped(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Control: no fallback (a completing worker that has no literal)
        keeps the enqueue-pinned expiry — the old bug path — and the
        sweep reaps the result. Pins both what the fallback fixes and
        that the fix, not something else, is what saves the result."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await self._seed_cleared_ttl_and_aged_job(clean_jobs_app, conn)

        ok = await backend.mark_succeeded(job_id, worker_id, {"ok": True})
        assert ok is True

        async with deps.worker_pool.acquire() as conn:
            swept = await backend.sweep_expired_results(conn, schema=schema)
            assert swept == 1
            row = await conn.fetchrow(f'SELECT result FROM "{schema}".jobs WHERE id = $1', job_id)
            assert row is not None and row["result"] is None

    async def test_stored_result_ttl_wins_over_fallback_at_completion(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Operator override (stored 300s) beats the worker's fallback
        literal (5s) for jobs completing after the override."""
        from taskq.actor_config_ops import set_actor_config_capacity

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            await set_actor_config_capacity(conn, "test_actor", result_ttl=300.0, schema=schema)
            worker_id, job_id = await setup_running_job(conn, schema)
            await conn.execute(
                f'UPDATE "{schema}"."jobs" SET result_expires_at = now() - interval \'40 seconds\' WHERE id = $1',
                job_id,
            )

        ok = await backend.mark_succeeded(
            job_id, worker_id, {"ok": True}, fallback_result_ttl=timedelta(seconds=5)
        )
        assert ok is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT result_expires_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            skew = row["result_expires_at"] - row["finished_at"]
            assert timedelta(seconds=299) < skew < timedelta(seconds=301)

    async def test_clock_timestamp_used_in_transactional_path(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """``clock_timestamp()`` not ``now()`` in the LOOP-scope
        transactional path: ``now()`` is frozen at transaction start, so
        an actor whose runtime exceeds its TTL would complete
        already-expired. ``clock_timestamp()`` is the wall-clock time the
        write executes.

        This test opens a transaction, sleeps past the TTL, then calls
        ``mark_succeeded_with_conn`` on the same connection — the
        LOOP-scope pattern. If the SQL used ``now()`` the expiry would
        be pinned at txn start (before the sleep) and the result would
        be immediately expired. With ``clock_timestamp()`` the expiry is
        computed at completion.

        Also asserts ``finished_at`` uses ``clock_timestamp()`` too — a
        long-running actor in the transactional path should record when
        it actually finished, not when the transaction started, so the
        two timestamp columns don't disagree by the full runtime.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        ttl_seconds = 2.0
        sleep_seconds = 3.0  # > ttl so now()-based expiry would be in the past

        async with (
            deps.worker_pool.acquire() as conn,
            conn.transaction(),
        ):
            worker_id, job_id = await setup_running_job(conn, schema)

            # Record the transaction start time as PG sees it.
            txn_start = await conn.fetchval("SELECT now()")

            # Sleep past the TTL — simulates a long-running actor.
            # now() stays frozen at txn_start; clock_timestamp() advances.
            await conn.execute(f"SELECT pg_sleep({sleep_seconds})")

            ok = await backend.mark_succeeded_with_conn(
                conn,
                job_id,
                worker_id,
                {"ok": True},
                fallback_result_ttl=timedelta(seconds=ttl_seconds),
            )
            assert ok is True

            row = await conn.fetchrow(
                f'SELECT result_expires_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None

            finished_at: datetime = row["finished_at"]
            result_expires_at: datetime = row["result_expires_at"]

            # finished_at must be AFTER the sleep, not frozen at txn
            # start. If it used now() it would equal txn_start.
            assert finished_at > txn_start + timedelta(seconds=sleep_seconds - 0.5)

            # result_expires_at must be ≈ finished_at + ttl, not
            # txn_start + ttl (which would be in the past).
            skew = result_expires_at - finished_at
            assert (
                timedelta(seconds=ttl_seconds - 0.5) < skew < timedelta(seconds=ttl_seconds + 0.5)
            )

            # The result must NOT be already expired.
            clock_now = await conn.fetchval("SELECT clock_timestamp()")
            assert result_expires_at > clock_now

    async def test_mark_failed_finished_at_uses_clock_timestamp_in_tx(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """``finished_at = now()`` in ``mark_failed`` is a latent bug: in a
        LOOP-scope transaction ``now()`` is frozen at txn start, so a
        long-running actor that fails would record the wrong completion
        time. All terminal-write SET clauses must use
        ``clock_timestamp()`` for the same reason ``mark_succeeded``
        does.

        This test runs the ``mark_failed`` SQL directly inside a
        long-lived transaction to prove the ``now()`` is wrong.
        """

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        sql = backend._sql  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access

        async with deps.worker_pool.acquire() as conn, conn.transaction():
            worker_id, job_id = await setup_running_job(conn, schema)
            txn_start = await conn.fetchval("SELECT now()")
            await conn.execute("SELECT pg_sleep(3)")

            # Execute the mark_failed SQL directly on the txn conn.
            rec = await conn.fetchrow(
                sql.mark_failed,
                job_id,
                worker_id,
                "TestError",
                "test",
                None,
                0,
                None,
            )
            assert rec is not None
            finished_at: datetime = rec["finished_at"]
            # If finished_at used now() it would be frozen at txn_start.
            assert finished_at > txn_start + timedelta(seconds=2)

    async def test_mark_snoozed_scheduled_at_uses_clock_timestamp_in_tx(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """``scheduled_at = now() + delay`` in ``mark_snoozed`` is
        inconsistent with the WHERE clause which already uses
        ``clock_timestamp() + delay`` to check the deadline. In a
        LOOP-scope transaction these would disagree: the deadline check
        uses wall-clock time but the scheduled_at is computed from
        txn-start time, so the job could be scheduled in the past
        relative to the deadline that was just checked.

        This test runs the snooze SQL directly inside a long-lived
        transaction to prove the ``now()`` in the SET clause is wrong.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        sql = backend._sql  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access
        delay = timedelta(seconds=10)

        async with deps.worker_pool.acquire() as conn, conn.transaction():
            worker_id, job_id = await setup_running_job(conn, schema)
            txn_start = await conn.fetchval("SELECT now()")
            await conn.execute("SELECT pg_sleep(3)")

            rec = await conn.fetchrow(
                sql.mark_snoozed,
                job_id,
                worker_id,
                delay,
                None,
                0,
                None,
            )
            assert rec is not None
            assert rec["outcome_branch"] == "snoozed"
            scheduled_at: datetime = rec["scheduled_at"]
            # If scheduled_at used now() it would be txn_start + delay,
            # but the deadline check used clock_timestamp() + delay.
            # The scheduled_at must be after txn_start + sleep + delay - 1s tolerance.
            assert scheduled_at > txn_start + timedelta(seconds=10)
