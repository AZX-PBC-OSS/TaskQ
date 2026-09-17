"""Integration tests for PostgresBackend sweep static methods.

Covers Sweep 1 (sweep_expired_locks), Sweep 2
(sweep_deadline_exceeded), Sweep 3 (sweep_scheduled_to_pending),
and Sweep 4 (sweep_leaked_reservation_slots).
"""

import asyncio
import contextlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo, JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import RECLAIM_EVENT_VISIBILITY_DELAY, wake_channel
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


async def _seed_job_attempts_row(conn: _Conn, schema: str, job_id: UUID, attempt: int) -> None:
    """Insert the job_attempts row a prior retry, crash reclaim, or admin
    retry would already have written for this job's current attempt number."""
    await conn.execute(
        f"""INSERT INTO \"{schema}\".job_attempts
            (job_id, attempt, started_at, finished_at, outcome,
             error_class, error_message, error_traceback, duration_ms,
             worker_id, metadata)
            VALUES ($1, $2, clock_timestamp(), clock_timestamp(), 'failed',
                    'SomeTransientError', 'boom', NULL, 5, NULL, '{{}}'::jsonb)""",
        job_id,
        attempt,
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

    async def test_indefinite_job_is_re_pended_past_its_max_attempts(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """A crash reclaim must not terminalise an ``indefinite`` job that
        has run more attempts than ``max_attempts``.

        For ``retry_kind='indefinite'`` the retry budget is the
        ``schedule_to_close`` deadline; ``max_attempts`` is documented as
        ignored for the retry decision, and the consumer's own failure
        path honours that — it reschedules an indefinite job whatever its
        attempt number. A worker crash is a less conclusive event than a
        failed execution, so it must not be the one thing that ends such a
        job early.

        The operator symptom when it does: a job configured to wait out a
        downstream outage lands in ``crashed`` with hours of deadline
        budget left, and the row carries no ``error_class`` at all, so
        nothing on it explains why the work stopped.
        """
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        deadline = datetime.now(UTC) + timedelta(hours=6)

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                attempt=3,
                max_attempts=3,
                retry_kind="indefinite",
                schedule_to_close=deadline,
            )

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, finished_at, error_class, schedule_to_close "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )

        assert row is not None
        assert row["schedule_to_close"] is not None, (
            "the scenario requires a job whose deadline budget is still open"
        )
        assert row["schedule_to_close"] > datetime.now(UTC), (
            "the scenario requires a deadline that has not yet passed"
        )
        assert row["status"] == "pending", (
            f"an indefinite job with an open schedule_to_close was reclaimed to "
            f"{row['status']!r} instead of being handed back for another attempt; "
            f"max_attempts does not bound an indefinite job's retries, and the row "
            f"carries error_class={row['error_class']!r}, so an operator has "
            f"nothing on the job explaining why it stopped"
        )
        assert row["finished_at"] is None, (
            "a job handed back for another attempt must not carry a finished_at"
        )

    async def test_reclaim_after_worker_row_cleanup(self, clean_jobs_app: JobsApp) -> None:
        """Reclaiming a job whose lock-holding worker's row is already
        deleted must not raise: the attempt records a NULL worker_id.

        cleanup_stale_workers (same sweep loop, earlier tick) deletes a
        crashed worker's row once its heartbeat is stale
        ``heartbeat_interval * (max_heartbeat_failures + 3)`` — with a
        lease longer than that (any worker killed shortly after a
        heartbeat), the row is gone before the reclaim tick first sees the
        expired lease. The job_attempts INSERT must then not FK-violate on
        the dangling worker_id; NULL mirrors the column's ON DELETE SET
        NULL semantics for rows written after the parent's death."""
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
                retry_kind="transient",
            )
            # The stale-worker cleanup beat the reclaim to it.
            await conn.execute(f'DELETE FROM "{schema}".workers WHERE id = $1', worker_id)

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )

        assert row is not None
        assert row["status"] == "pending"
        assert row["locked_by_worker"] is None
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "crashed"
        assert attempts[0]["worker_id"] is None

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
        cancel_grace + cleanup_grace + 60s threshold SHOULD be swept —
        and with operator intent outranking the retry budget (#238),
        the reclaim terminalises it 'cancelled' whatever its budget,
        never re-pends it with a wiped cancel."""
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
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)

        assert row is not None
        assert row["status"] == "cancelled", (
            "a deeply expired row with a cancel in flight must land "
            "'cancelled' even with retries remaining — the operator's "
            "request is the honest terminal label, and the lock-holding "
            "worker this cancel was addressed to is provably gone"
        )

    async def test_deeply_expired_cancel_in_flight_retryable_lands_cancelled(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """A deeply expired lock with an in-flight cancel request on a
        RETRYABLE job terminalises 'cancelled' and keeps its cancel
        columns as the audit trail of the honored request (#238).

        The pre-fix statement evaluated the retry budget before
        ``cancel_phase``, so this row went back 'pending' with
        ``cancel_phase``/``cancel_requested_at`` wiped — the operator's
        cancel silently lost, addressed to a worker the reclaim itself
        had just declared dead. The cancel-first ordering cannot
        resurrect the re-cancel loop the old reset-on-re-pend spelling
        existed to prevent: that loop required a RE-PENDED row still
        carrying cancel columns, and the cancel arm never re-pends.
        The second half pins exactly that loop-guard from the other
        side: a phase-0 reclaim's re-pended row is claimable by a new
        worker whose cancel-poll stays quiet — no inherited phase.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        deep_past = datetime.now(UTC) - timedelta(seconds=180)
        requested_at = datetime.now(UTC)

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=deep_past,
                cancel_phase=1,
                cancel_requested_at=requested_at,
            )

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, cancel_phase, cancel_requested_at, finished_at "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempt_row = await conn.fetchrow(
                f'SELECT outcome, error_class FROM "{schema}".job_attempts '
                f"WHERE job_id = $1 ORDER BY started_at DESC LIMIT 1",
                job_id,
            )

        assert row is not None
        assert row["status"] == "cancelled", (
            "a retryable row with a cancel in flight must terminalise "
            "'cancelled' — the operator's request outranks the retry budget"
        )
        # The audit trail of the honored request survives the terminal
        # write, exactly as mark_cancelled keeps both columns.
        assert row["cancel_phase"] == 1
        assert row["cancel_requested_at"] is not None
        assert row["finished_at"] is not None

        assert attempt_row is not None
        assert attempt_row["outcome"] == "crashed"
        assert attempt_row["error_class"] == "WorkerCrashed"

        # The loop-guard half, kept from the test this replaces: a
        # PHASE-0 reclaim re-pends, and the re-pended row must carry a
        # clean cancellation slate: the next worker's cancel-poll must
        # not return it, or the fleet re-cancels and re-reclaims
        # forever. On a re-pend that left the columns set (the shape
        # the old reset-on-re-pend spelling guarded against) the
        # cancel-poll returns the job and the worker re-cancels it:
        # the infinite cancel/reclaim/retry loop.
        second_worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            phase_zero_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=180),
            )
            reclaim_count = await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )
            assert reclaim_count == 1
            repended = await conn.fetchrow(
                f"SELECT status, cancel_phase, cancel_requested_at "
                f'FROM "{schema}".jobs WHERE id = $1',
                phase_zero_id,
            )
            assert repended is not None
            assert repended["status"] == "pending"
            assert repended["cancel_phase"] == 0
            assert repended["cancel_requested_at"] is None

            # Simulate the claim the dispatch CTE performs: the re-pended
            # row is taken by a new worker.
            await create_worker(conn, schema, second_worker_id)
            await conn.execute(
                f'UPDATE "{schema}".jobs SET '
                "status = 'running', locked_by_worker = $2, "
                "lock_expires_at = $3, started_at = $3, last_heartbeat_at = $3 "
                "WHERE id = $1",
                phase_zero_id,
                second_worker_id,
                datetime.now(UTC) + timedelta(seconds=30),
            )

        flags = await backend.poll_cancel_flags(second_worker_id)
        assert flags == [], (
            "a re-pended row must never carry an inherited cancel phase — "
            "any regression that re-pends a phase-carrying row re-enters "
            "the cancel/reclaim/retry loop this pins shut"
        )

    async def test_deeply_expired_cancel_exhausted_labelled_cancelled(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """A deeply-expired lock with an in-flight cancel request and no
        retries remaining terminates as 'cancelled' (the caller's explicit
        request), not 'crashed' — reconciling terminal states must show
        the cancel was honored.  The job_events outbox row carries
        to_state='cancelled' so watch_reclaims consumers see the honest
        label, while job_attempts still records outcome='crashed'
        (WorkerCrashed): that IS what happened to the attempt."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        deep_past = datetime.now(UTC) - timedelta(seconds=180)

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=deep_past,
                cancel_phase=1,
                cancel_requested_at=datetime.now(UTC),
                max_attempts=1,
                attempt=1,
            )

            count = await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, cancel_phase, cancel_requested_at, finished_at "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            event_rows = await conn.fetch(
                f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1 ORDER BY id',
                job_id,
            )
            attempt_row = await conn.fetchrow(
                f'SELECT outcome, error_class FROM "{schema}".job_attempts '
                f"WHERE job_id = $1 ORDER BY started_at DESC LIMIT 1",
                job_id,
            )

        assert row is not None
        assert row["status"] == "cancelled"
        # The audit trail of the honored request survives the terminal
        # write (#238): the sweep's cancelled arm keeps both cancel
        # columns, the same doctrine mark_cancelled carries: the
        # pre-fix statement wiped them here too.
        assert row["cancel_phase"] == 1
        assert row["cancel_requested_at"] is not None
        assert row["finished_at"] is not None

        reclaim_events = []
        for e in event_rows:
            detail = e["detail"]
            if isinstance(detail, str):
                detail = json.loads(detail)
            if detail.get("reason") == "lock_expired":
                reclaim_events.append(detail)
        assert len(reclaim_events) == 1
        assert reclaim_events[0]["to_state"] == "cancelled"

        assert attempt_row is not None
        assert attempt_row["outcome"] == "crashed"
        assert attempt_row["error_class"] == "WorkerCrashed"

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
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        assert count == 0


# ── poll_reclaim_events ─────────────────────────────────────────


class _FailingEventConn:
    """Wraps a pool connection to make the event INSERT fail, proving
    the outbox write and state change share transactional fate."""

    def __init__(self, conn: _Conn) -> None:
        self._conn: Any = conn  # Why: asyncpg Connection is TYPE_CHECKING-only; wrapper needs runtime method access
        self._execute_count = 0

    async def fetch(
        self, sql: str, *args: object
    ) -> Any:  # Why: asyncpg returns list[Record]; not statically exported
        return await self._conn.fetch(sql, *args)

    async def execute(self, sql: str, *args: object) -> str:
        self._execute_count += 1
        if self._execute_count == 2:
            raise RuntimeError("simulated event INSERT failure")
        result: str = await self._conn.execute(sql, *args)
        return result

    def transaction(
        self,
    ) -> Any:  # Why: asyncpg transaction context manager type is not statically exported
        return self._conn.transaction()


class TestPollReclaimEvents:
    """poll_reclaim_events — fleet-wide cursor-based reclaim event polling.

    A consumer tracking completion of a fan-out of N jobs via an
    outstanding-work counter can observe crash-reclaimed jobs without
    enumerating every job_id.
    """

    async def test_terminal_branch_emits_observable_event(self, clean_jobs_app: JobsApp) -> None:
        """Crash-reclaim with retries exhausted → poll_reclaim_events
        returns the row with to_state='crashed', reason='lock_expired'."""
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
            await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == 1
        evt = events[0]
        assert evt.detail["to_state"] == "crashed"
        assert evt.detail["reason"] == "lock_expired"
        assert evt.detail["from_state"] == "running"
        assert evt.kind == "state_change"
        assert evt.job_id == JobId(job_id)

    async def test_retry_branch_emits_observable_event(self, clean_jobs_app: JobsApp) -> None:
        """Crash-reclaim with retries remaining → poll_reclaim_events
        returns the row with to_state='pending', reason='lock_expired'."""
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
            await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == 1
        evt = events[0]
        assert evt.detail["to_state"] == "pending"
        assert evt.detail["reason"] == "lock_expired"
        assert evt.detail["from_state"] == "running"
        assert evt.job_id == JobId(job_id)

    async def test_cursor_semantics(self, clean_jobs_app: JobsApp) -> None:
        """after_id filtering excludes already-seen events; limit is
        respected; results are ordered ascending by event_id."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            for _ in range(2):
                await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                    max_attempts=1,
                    retry_kind="transient",
                    attempt=1,
                )
            await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        # limit=1 → only 1 event
        first_batch = await backend.poll_reclaim_events(0, limit=1, visibility_delay=timedelta(0))
        assert len(first_batch) == 1

        # after_id filtering → excludes already-seen event
        first_id = first_batch[0].event_id
        second_batch = await backend.poll_reclaim_events(
            first_id, limit=100, visibility_delay=timedelta(0)
        )
        assert len(second_batch) == 1
        assert second_batch[0].event_id > first_id

        # full query → ascending order
        all_events = await backend.poll_reclaim_events(0, limit=100, visibility_delay=timedelta(0))
        assert len(all_events) == 2
        assert all_events[0].event_id < all_events[1].event_id

    async def test_atomicity_event_insert_failure_rolls_back_state(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """If the event INSERT fails inside the transaction, both the
        state change and the event row are rolled back — no partial
        observation."""
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

        # Wrap a fresh connection to make the event INSERT fail
        async with deps.worker_pool.acquire() as conn:
            wrapped = _FailingEventConn(conn)
            with pytest.raises(RuntimeError, match="simulated event INSERT failure"):
                await PostgresBackend.sweep_expired_locks(
                    wrapped,
                    _CANCEL_GRACE,
                    _CLEANUP_GRACE,
                    schema=schema,
                )

        # Job status should still be 'running' (UPDATE rolled back)
        row = await backend.get(JobId(job_id))
        assert row is not None
        assert row.status == "running"

        # No reclaim events should be observable (INSERT rolled back)
        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == 0

    async def test_no_duplicate_across_repeated_sweeps(self, clean_jobs_app: JobsApp) -> None:
        """Running sweep twice on the same job → second run affects 0
        rows and poll_reclaim_events returns exactly one event."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=1,
                retry_kind="transient",
                attempt=1,
            )

            count1 = await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )
            assert count1 == 1

            count2 = await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )
            assert count2 == 0

        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == 1

    async def test_no_duplicate_across_concurrent_sweeps(self, clean_jobs_app: JobsApp) -> None:
        """Two concurrent sweeps over shared expired-lock jobs → total
        event count equals jobs reclaimed exactly once each
        (FOR UPDATE SKIP LOCKED guarantees disjoint ownership)."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        num_jobs = 4

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            for _ in range(num_jobs):
                await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                    max_attempts=1,
                    retry_kind="transient",
                    attempt=1,
                )

        async def _sweep() -> int:
            async with deps.worker_pool.acquire() as conn:
                return await PostgresBackend.sweep_expired_locks(
                    conn,
                    _CANCEL_GRACE,
                    _CLEANUP_GRACE,
                    schema=schema,
                )

        counts = await asyncio.gather(_sweep(), _sweep())
        total_swept = sum(counts)
        assert total_swept == num_jobs

        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == num_jobs

    async def test_out_of_order_commit_does_not_lose_event(self, clean_jobs_app: JobsApp) -> None:
        """``job_events.id`` (bigserial) is allocated at INSERT time, but
        transactions can commit out of order under real contention (lock
        waits, a slower sweep pass, GC pauses): a transaction holding a
        LOWER id can commit AFTER one holding a HIGHER id.

        With ``visibility_delay=0`` (no protection — reproduces the bug
        directly against the real query, not a reverted copy of it),
        polling right after the higher-id transaction commits returns it
        immediately; a cursor advanced to that id would then permanently
        exclude the lower-id transaction's event once it finally commits.

        The production default (``RECLAIM_EVENT_VISIBILITY_DELAY``) holds
        back freshly-committed rows until enough wall-clock time has
        passed that any transaction which could hold a lower id is
        guaranteed to have already committed or aborted — ``id`` and
        ``occurred_at`` are stamped at the same INSERT instant, so they
        are co-monotonic, and this is why waiting on ``occurred_at``
        alone is sufficient once it clears the margin.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        dsn = str(deps.settings.pg_dsn)
        worker_id = new_uuid()

        detail_json = json.dumps(
            {"reason": "lock_expired", "to_state": "crashed", "from_state": "running"}
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_a = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=1,
                retry_kind="transient",
                attempt=1,
            )
            job_b = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=1,
                retry_kind="transient",
                attempt=1,
            )

        conn_a = await asyncpg.connect(dsn)
        conn_b = await asyncpg.connect(dsn)
        tx_a = conn_a.transaction()
        tx_b = conn_b.transaction()
        try:
            # A inserts first, grabbing the LOWER event_id, then stays open.
            await tx_a.start()
            await conn_a.execute(
                f'INSERT INTO "{schema}".'
                "job_events (job_id, kind, detail) "
                "VALUES ($1, 'state_change', $2::jsonb)",
                job_a,
                detail_json,
            )

            # B inserts second, grabbing the HIGHER event_id, and commits
            # immediately — out of order relative to A.
            await tx_b.start()
            await conn_b.execute(
                f'INSERT INTO "{schema}".'
                "job_events (job_id, kind, detail) "
                "VALUES ($1, 'state_change', $2::jsonb)",
                job_b,
                detail_json,
            )
            await tx_b.commit()

            # Reproduces the bug directly: with no visibility delay, B's
            # event is returned right away and a cursor advanced to it
            # would permanently exclude A once A finally commits.
            unsafe = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
            assert len(unsafe) == 1
            assert unsafe[0].job_id == JobId(job_b)

            # The fix: the production default margin holds B back.
            protected = await backend.poll_reclaim_events(0)
            assert protected == [], (
                "visibility_delay should hold back a freshly-committed row "
                "while an earlier-id transaction may still be uncommitted"
            )

            # A finally commits, comfortably within the margin.
            await tx_a.commit()
            await asyncio.sleep(RECLAIM_EVENT_VISIBILITY_DELAY.total_seconds() + 0.3)

            settled = await backend.poll_reclaim_events(0)
            assert len(settled) == 2
            assert settled[0].event_id < settled[1].event_id
            assert settled[0].job_id == JobId(job_a)
            assert settled[1].job_id == JobId(job_b)
        finally:
            with contextlib.suppress(Exception):
                await tx_a.rollback()
            with contextlib.suppress(Exception):
                await tx_b.rollback()
            with contextlib.suppress(Exception):
                await conn_a.close()
            with contextlib.suppress(Exception):
                await conn_b.close()

    async def test_check_reclaim_visibility_delay_risk_detects_long_open_writer(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """check_reclaim_visibility_delay_risk reports a transaction that
        has held job_events open longer than a (deliberately tiny) margin,
        and reports nothing once that transaction commits."""
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        dsn = str(deps.settings.pg_dsn)

        detail_json = json.dumps(
            {"reason": "lock_expired", "to_state": "crashed", "from_state": "running"}
        )

        async with deps.worker_pool.acquire() as conn:
            worker_id = new_uuid()
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

        conn_a = await asyncpg.connect(dsn)
        tx_a = conn_a.transaction()
        try:
            await tx_a.start()
            await conn_a.execute(
                f'INSERT INTO "{schema}".'
                "job_events (job_id, kind, detail) "
                "VALUES ($1, 'state_change', $2::jsonb)",
                job_id,
                detail_json,
            )

            tiny_margin = timedelta(milliseconds=50)
            await asyncio.sleep(0.15)

            risky = await backend.check_reclaim_visibility_delay_risk(visibility_delay=tiny_margin)
            assert len(risky) == 1
            assert risky[0].xact_age_seconds >= tiny_margin.total_seconds()
            # EXTRACT(EPOCH ...) returns numeric → asyncpg Decimal without
            # the ::float8 cast; Decimal >= float compares fine (so the
            # assertion above can't catch it) but json.dumps(Decimal)
            # raises TypeError in the monitoring loop this diagnostic feeds.
            assert isinstance(risky[0].xact_age_seconds, float)

            await tx_a.commit()

            settled = await backend.check_reclaim_visibility_delay_risk(
                visibility_delay=tiny_margin
            )
            assert settled == []
        finally:
            with contextlib.suppress(Exception):
                await tx_a.rollback()
            with contextlib.suppress(Exception):
                await conn_a.close()

    async def test_settings_configured_visibility_delay_is_the_default(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """A PostgresBackend constructed with a custom
        reclaim_event_visibility_delay applies that margin as
        poll_reclaim_events' default when no per-call override is given —
        the end-to-end path from WorkerSettings.reclaim_event_visibility_delay
        (via PostgresBackend.__init__) down to the SQL, not just the
        per-call visibility_delay= parameter exercised elsewhere."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        custom_delay = timedelta(milliseconds=300)

        configured_backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=_CANCEL_GRACE,
            cleanup_grace_period=_CLEANUP_GRACE,
            reclaim_event_visibility_delay=custom_delay,
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=1,
                retry_kind="transient",
                attempt=1,
            )
            await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        # No visibility_delay= override: relies entirely on the
        # constructor-configured default.
        immediate = await configured_backend.poll_reclaim_events(0)
        assert immediate == [], (
            "expected the configured 300ms margin to hold the event back "
            "immediately after the sweep, not the library default (2s) or "
            "no margin at all"
        )

        await asyncio.sleep(custom_delay.total_seconds() + 0.2)

        settled = await configured_backend.poll_reclaim_events(0)
        assert len(settled) == 1

    async def test_cursor_on_empty_table(self, clean_jobs_app: JobsApp) -> None:
        """Cursor semantics on an empty table: polling returns nothing at
        cursor 0, and a cursor arbitrarily far ahead of the table's max id
        (e.g. persisted before a schema reset, or mis-restored from a
        backup) also returns nothing rather than erroring — the cursor is
        a watermark compared with `>`, never dereferenced."""
        backend = clean_jobs_app.backend

        assert await backend.poll_reclaim_events(0, visibility_delay=timedelta(0)) == []
        assert await backend.poll_reclaim_events(2**40, visibility_delay=timedelta(0)) == []

    async def test_cursor_survives_pruning_of_seen_rows(self, clean_jobs_app: JobsApp) -> None:
        """Interaction with job_events pruning: a consumer's cursor may
        point at a row an operator later prunes.  Because the cursor is a
        watermark (`id > cursor`), deleting rows at or below it changes
        nothing — the next poll still returns exactly the un-pruned rows
        ahead of it, with no error and no replay.  (The dangerous
        direction — pruning rows AHEAD of a slow consumer's cursor —
        permanently loses those events for that consumer; that is an
        operational policy constraint, not something the SQL can guard
        against, and is documented in watch_reclaims.)"""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            for _ in range(3):
                await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                    max_attempts=1,
                    retry_kind="transient",
                    attempt=1,
                )
            await PostgresBackend.sweep_expired_locks(
                conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )

        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == 3
        cursor = events[0].event_id

        # Operator prunes the two oldest rows — one AT the consumer's
        # cursor, one AHEAD of it (simulating a too-aggressive prune).
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'DELETE FROM "{schema}".job_events WHERE id = ANY($1::bigint[])',
                [events[0].event_id, events[1].event_id],
            )

        # The pruned cursor row neither errors nor replays; the consumer
        # simply never sees the second (pruned-ahead) event, and resumes
        # cleanly at the first surviving row past its cursor.
        remaining = await backend.poll_reclaim_events(cursor, visibility_delay=timedelta(0))
        assert [e.event_id for e in remaining] == [events[2].event_id]

    async def test_single_pg_notify_per_sweep_with_rows(self, clean_jobs_app: JobsApp) -> None:
        """The sweep fires exactly ONE pg_notify per call that reclaims at
        least one row (not one per row), and none when nothing is swept —
        wake storms on a crash-reclaim of N jobs would punish every
        subscriber for a single sweep pass."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        dsn = str(deps.settings.pg_dsn)
        worker_id = new_uuid()

        listen_conn = await asyncpg.connect(dsn)
        notifications: list[str] = []

        def _on_notify(conn: object, pid: int, channel: str, payload: str) -> None:
            notifications.append(channel)

        try:
            await listen_conn.add_listener(
                wake_channel(schema),
                _on_notify,  # type: ignore[arg-type]  # Why: asyncpg stubs over-narrow the callback type — same suppression as the transports under test
            )

            async with deps.worker_pool.acquire() as conn:
                await create_worker(conn, schema, worker_id)
                for _ in range(3):
                    await create_running_job(
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
                    _CANCEL_GRACE,
                    _CLEANUP_GRACE,
                    schema=schema,
                )
            assert count == 3

            # NOTIFY delivery is asynchronous even after the sweep's
            # commit: a bounded poll on the received list waits exactly as
            # long as delivery needs — a fixed 0.3s sleep races it
            # under load (too short → the notification has not arrived
            # yet → false failure).
            deadline = asyncio.get_running_loop().time() + 5.0
            while len(notifications) < 1:
                assert asyncio.get_running_loop().time() < deadline, (
                    "the sweep's wake notification was never delivered within 5s"
                )
                await asyncio.sleep(0.01)
            assert len(notifications) == 1, (
                f"expected exactly one wake notification for a 3-row sweep, "
                f"got {len(notifications)}"
            )

            # A no-op sweep fires nothing. Negative assert: kept as a
            # fixed settled window on purpose — absence cannot be polled
            # for, and 0.3s (several delivery beats after the commit
            # above) is the window in which a stray NOTIFY would have
            # arrived.
            notifications.clear()
            async with deps.worker_pool.acquire() as conn:
                count = await PostgresBackend.sweep_expired_locks(
                    conn,
                    _CANCEL_GRACE,
                    _CLEANUP_GRACE,
                    schema=schema,
                )
            assert count == 0
            await asyncio.sleep(0.3)
            assert notifications == []
        finally:
            await listen_conn.close()


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
                schema=schema,
            )

        assert count == 0

    async def test_exhausted_budget_retry_past_deadline_is_made_dispatchable(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """An admin-retried job that has already spent its whole retry
        budget and whose deadline has passed must come back budget-eligible
        and dispatchable — not stranded pending past its own deadline.

        This pin's former premise — ``retry_job`` never rebases
        ``schedule_to_close``, so a past-deadline retried row sits
        dispatch-excluded and Sweep 2 is the only thing that can ever touch
        it — was replaced by the merged deliberate contract
        (``tests/test_retry_job_stale_deadline_operator_footgun.py``, and
        the ``retry_job`` template's own comment in
        ``src/taskq/backend/_sql_templates.py``): an already-elapsed
        ``schedule_to_close`` is a spent epoch's artifact, the same class as
        ``finished_at``/``result``, and is CLEARED for the fresh run the
        retry grants; a still-future deadline survives. With the stale
        deadline gone the row is dispatchable again, so the resolution path
        for this shape is dispatch, not the sweep — the sweep-to-failed
        behaviour for rows still past deadline (staged directly, no admin
        retry involved) remains pinned by
        ``test_overdue_job_with_existing_attempt_row_is_still_failed`` and
        ``test_transient_retry_past_deadline_is_swept_to_failed`` below, and
        the no-wedge guarantee for a preserved deadline that later elapses
        by ``tests/test_retry_job_past_deadline_sweep_collision.py``.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        error = ErrorInfo(
            error_class="TransientError",
            error_message="boom",
            error_traceback=None,
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            # A job whose deadline already passed while it was running.
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                max_attempts=1,
                attempt=1,
                lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                with_events=False,
            )

            # Original failure: writes the (job_id, attempt=1) row in
            # job_attempts.
            assert await backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=1)
            row = await conn.fetchrow(
                f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None and row["status"] == "failed" and row["attempt"] == 1

            # Admin retry: attempt stays at 1 (monotonic), max_attempts is
            # raised past the spent attempt, and the already-elapsed
            # schedule_to_close is cleared for the fresh run.
            assert await backend.retry_job(job_id)
            retried = await conn.fetchrow(
                f"SELECT status, attempt, max_attempts, schedule_to_close "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert retried is not None
            assert retried["status"] == "pending"
            assert retried["attempt"] == 1
            assert retried["max_attempts"] == 2, (
                "the retry must raise the ceiling past the spent attempt "
                "(LEAST(GREATEST(max_attempts, attempt + 1), 32767)), or the "
                "exhausted-budget row comes back budget-ineligible"
            )
            assert retried["schedule_to_close"] is None, (
                "an already-elapsed schedule_to_close must be cleared by the "
                "retry: dispatch refuses any row whose deadline has passed, "
                "so re-pending with the stale deadline intact hands back a "
                "row no worker can ever claim"
            )

            # The row left the deadline sweep's predicate: the sweep runs
            # cleanly over the schema and touches nothing — no collision is
            # even reachable on this path, and the row is not re-failed
            # behind the operator's back.
            swept_count = await PostgresBackend.sweep_deadline_exceeded(
                conn,
                schema=schema,
            )
            assert swept_count == 0
            after_sweep = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert after_sweep is not None and after_sweep["status"] == "pending"

        # The no-leak close-out: the only resolver this shape has now is
        # dispatch, and dispatch resolves it — the spent attempt number does
        # not block the claim, because the retry raised the ceiling above it.
        claimed = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=timedelta(minutes=5)
        )
        assert [j.id for j in claimed] == [job_id]

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
                schema=schema,
            )

        assert count == 0

    async def test_overdue_job_with_existing_attempt_row_is_still_failed(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """A pending job can legitimately sit at a nonzero ``attempt`` that
        already owns a ``job_attempts`` row: a transient retry, a crash
        reclaim that re-pends the job, and an admin ``retry_job`` all keep
        ``attempt`` unchanged and leave an attempt row behind for it. If the
        deadline then passes before the job is redispatched, Sweep 2 is the
        only thing that will ever resolve the row, because dispatch skips
        rows past ``schedule_to_close``. It must fail the job cleanly rather
        than colliding on ``job_attempts_pkey``.
        """
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                status="pending",
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs SET attempt = 1 WHERE id = $1',
                job_id,
            )
            await _seed_job_attempts_row(conn, schema, job_id, 1)

            try:
                count = await PostgresBackend.sweep_deadline_exceeded(
                    conn,
                    schema=schema,
                )
            except asyncpg.UniqueViolationError as exc:
                pytest.fail(
                    "sweep_deadline_exceeded raised UniqueViolationError on "
                    f"job_attempts_pkey for a job with an existing attempt row: {exc!r}. "
                    "The sweep must handle a pre-existing job_attempts row for the "
                    "job's current attempt instead of colliding."
                )

        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"
        assert row["finished_at"] is not None

    async def test_bystander_job_swept_despite_colliding_sibling_in_same_batch(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Sweep 2 writes its ``job_attempts`` rows as one batched INSERT
        inside a single transaction, so a primary-key collision raised by
        one job rolls back every job swept in that call. An ordinary
        never-dispatched overdue job must still be failed even when another
        job in the same batch has an attempt row at its current attempt
        number. Otherwise a single wedged job strands every other overdue
        job in pending/scheduled indefinitely, since dispatch also skips
        rows past ``schedule_to_close`` and every subsequent tick hits the
        same collision.
        """
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            # Already ran attempt 1, owns a job_attempts row for it, deadline past.
            colliding_job_id = await create_pending_job(
                conn,
                schema,
                job_id=new_uuid(),
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                status="pending",
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs SET attempt = 1 WHERE id = $1',
                colliding_job_id,
            )
            await _seed_job_attempts_row(conn, schema, colliding_job_id, 1)

            # Ordinary never-dispatched overdue job: attempt 0, no attempt row.
            bystander_job_id = await create_pending_job(
                conn,
                schema,
                job_id=new_uuid(),
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                status="pending",
            )

            try:
                await PostgresBackend.sweep_deadline_exceeded(
                    conn,
                    schema=schema,
                )
            except asyncpg.UniqueViolationError as exc:
                pytest.fail(
                    "sweep_deadline_exceeded's batch transaction rolled back "
                    f"entirely because of one colliding job: {exc!r}. This strands "
                    "every other overdue job in pending/scheduled, since dispatch "
                    "also skips schedule_to_close-expired rows."
                )

        async with deps.worker_pool.acquire() as conn:
            bystander_row = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
                bystander_job_id,
            )
        assert bystander_row is not None
        assert bystander_row["status"] == "failed", (
            "bystander job was collaterally rolled back and left stuck in "
            f"'{bystander_row['status']}' by a colliding sibling's failure in the "
            "same batch transaction"
        )
        assert bystander_row["error_class"] == "DeadlineExceeded"

    async def test_admin_retry_of_past_deadline_job_is_not_swept_back(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """An admin ``retry_job`` of a job whose ``schedule_to_close`` has
        already passed must hand back a genuinely dispatchable row — the
        operator's Retry means "run this again", not "sweep me back to
        failed on the next tick".

        This pin's former premise — ``retry_job`` leaves the stale deadline
        in place, stranding the row past its own deadline until Sweep 2
        re-fails it — was replaced by the merged deliberate contract
        (``tests/test_retry_job_stale_deadline_operator_footgun.py``): a
        ``schedule_to_close`` that has already elapsed is cleared at retry
        time, exactly like the other prior-run terminal-state artifacts the
        retry clears. The row leaves the deadline sweep's predicate, so the
        sweep must neither touch it nor wedge on its spent attempt key; the
        no-wedge guarantee for rows still past deadline is pinned by
        ``test_overdue_job_with_existing_attempt_row_is_still_failed`` and
        ``test_bystander_job_swept_despite_colliding_sibling_in_same_batch``
        below, and for a preserved-future deadline that later elapses by
        ``tests/test_retry_job_past_deadline_sweep_collision.py``.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        error = ErrorInfo(
            error_class="TransientError",
            error_message="boom",
            error_traceback=None,
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                max_attempts=1,
                attempt=1,
                lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                with_events=False,
            )

            # The real dispatched run fails, writing job_attempts(job_id, 1).
            assert await backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=1)
            attempts_before = await conn.fetch(
                f'SELECT attempt FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
            assert {a["attempt"] for a in attempts_before} == {1}

            # The operator re-runs it. The row goes back to pending at the
            # same attempt, with the elapsed deadline cleared — the retry
            # writes no attempt row of its own.
            assert await backend.retry_job(job_id)
            retried = await conn.fetchrow(
                f'SELECT status, attempt, schedule_to_close FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert retried is not None
            assert retried["status"] == "pending"
            assert retried["attempt"] == 1
            assert retried["schedule_to_close"] is None, (
                "the already-elapsed deadline must be cleared for the fresh "
                "run: left intact, dispatch refuses the row and the next "
                "sweep tick silently re-fails it"
            )
            attempts_after = await conn.fetch(
                f'SELECT attempt FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
            assert {a["attempt"] for a in attempts_after} == {1}

            # The sweep runs cleanly over the schema — the retried row is no
            # longer sweep-eligible, so nothing is swept and no
            # job_attempts_pkey collision is even reachable on this path.
            try:
                swept_count = await PostgresBackend.sweep_deadline_exceeded(
                    conn,
                    schema=schema,
                )
            except asyncpg.UniqueViolationError as exc:
                pytest.fail(
                    "sweep_deadline_exceeded raised UniqueViolationError on "
                    f"job_attempts_pkey sweeping an admin-retried job: {exc!r}. "
                    "The retried row must leave the sweep's predicate (the "
                    "elapsed deadline is cleared) instead of being claimed at "
                    "a spent attempt number."
                )

        assert swept_count == 0, (
            "the retried row must not be swept back to failed behind the "
            "operator's back: with the elapsed deadline cleared it is "
            "dispatchable, and the sweep has no business with it"
        )

        async with deps.worker_pool.acquire() as conn:
            final = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        assert final is not None
        assert final["status"] == "pending"

    async def test_transient_retry_past_deadline_is_swept_to_failed(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """A job re-pended by an ordinary transient retry must still be
        sweepable once its ``schedule_to_close`` passes.

        The retried arm of ``mark_failed_or_retry`` writes a
        ``job_attempts`` row for the attempt it just spent and leaves the
        job ``scheduled`` at that same attempt number. No operator action
        is needed to reach this state, so it is the most common way a
        pending/scheduled row comes to own an attempt row for its current
        attempt. If the deadline passes before the backoff elapses, Sweep 2
        must fail the job rather than colliding on ``job_attempts_pkey``.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        error = ErrorInfo(
            error_class="TransientError",
            error_message="boom",
            error_traceback=None,
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            # Budget remains, so the terminal write takes the retried arm.
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                max_attempts=3,
                attempt=1,
                lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                with_events=False,
            )

            assert await backend.mark_failed_or_retry(
                job_id, worker_id, error, timedelta(minutes=5), attempt=1
            )
            retried = await conn.fetchrow(
                f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert retried is not None
            assert retried["status"] == "scheduled"
            assert retried["attempt"] == 1
            attempts_before = await conn.fetch(
                f'SELECT attempt FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
            assert {a["attempt"] for a in attempts_before} == {1}

            # The deadline lapses while the row waits out its backoff.
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET schedule_to_close = now() - interval '10 seconds' WHERE id = $1",
                job_id,
            )

            try:
                swept_count = await PostgresBackend.sweep_deadline_exceeded(
                    conn,
                    schema=schema,
                )
            except asyncpg.UniqueViolationError as exc:
                pytest.fail(
                    "sweep_deadline_exceeded raised UniqueViolationError sweeping a "
                    f"transiently retried job past its deadline: {exc!r}. The retried "
                    "arm's own job_attempts row must not wedge the deadline sweep."
                )

        assert swept_count >= 1

        async with deps.worker_pool.acquire() as conn:
            final = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        assert final is not None
        assert final["status"] == "failed"
        assert final["error_class"] == "DeadlineExceeded"

    async def test_repeated_ticks_do_not_wedge_on_a_previously_attempted_job(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """The deadline sweep converges: a job that already ran an attempt
        is resolved on the first tick, and every later tick over the same
        table is a clean no-op.

        Tolerating an existing ``job_attempts`` row once is not enough. The
        sweep runs on a fixed leader cadence over the whole table, so a row
        it cannot resolve is a row it re-selects on every tick forever —
        the pathology is not one raised error but an unbounded series of
        them, each one aborting that tick's transaction and taking every
        overdue sibling down with it while the leader's error rate climbs
        with no job ever reaching a terminal state.

        Convergence is therefore the property worth pinning, and it is
        observable without reading any internals: the job is terminal after
        one tick, subsequent ticks sweep zero rows, and none of them raises.
        """
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(
                conn,
                schema,
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                status="pending",
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs SET attempt = 1 WHERE id = $1',
                job_id,
            )
            await _seed_job_attempts_row(conn, schema, job_id, 1)

            counts: list[int] = []
            for tick in range(3):
                try:
                    counts.append(
                        await PostgresBackend.sweep_deadline_exceeded(conn, schema=schema)
                    )
                except asyncpg.PostgresError as exc:
                    pytest.fail(
                        f"sweep_deadline_exceeded raised on tick {tick + 1} over a job "
                        f"that already owns a job_attempts row for its attempt: {exc!r}. "
                        "A row the sweep cannot resolve is re-selected every tick, so "
                        "this failure repeats forever and aborts each tick's whole batch."
                    )

        assert counts[0] == 1, (
            f"first tick swept {counts[0]} rows, expected the one overdue job. A job "
            "that already ran an attempt must still be resolvable by the deadline "
            "sweep — nothing else will ever terminate it, since dispatch skips rows "
            "past schedule_to_close."
        )
        assert counts[1:] == [0, 0], (
            f"the deadline sweep kept selecting the same job across ticks: {counts}. "
            "Once resolved the row must fall out of the sweep's candidate set, or the "
            "leader burns a batch on it on every tick indefinitely."
        )

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"

    async def test_concurrent_sweeps_do_not_double_write_or_deadlock(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Two sweep calls racing on overlapping overdue rows (e.g. two
        leader ticks overlapping across a slow batch, or a leader handoff
        mid-tick) must not deadlock, double-insert a ``job_attempts`` row,
        or double-emit a state-change event for the same job. ``SKIP
        LOCKED`` should hand each row to exactly one of the two calls; the
        ``ON CONFLICT DO NOTHING`` fix must not paper over a genuine
        double-write by silently absorbing it.
        """
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        job_ids: list[UUID] = []
        async with deps.worker_pool.acquire() as conn:
            for _ in range(20):
                job_id = await create_pending_job(
                    conn,
                    schema,
                    schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
                    status="pending",
                )
                job_ids.append(job_id)

        async def run_sweep() -> int:
            async with deps.worker_pool.acquire() as conn:
                return await PostgresBackend.sweep_deadline_exceeded(
                    conn, schema=schema, batch_size=20
                )

        counts = await asyncio.gather(run_sweep(), run_sweep())

        assert sum(counts) == 20, (
            f"two concurrent sweeps over 20 disjoint-overdue jobs returned counts {counts} "
            f"(sum {sum(counts)}); SKIP LOCKED should partition the batch across the two "
            "calls with no row missed and none double-counted."
        )

        async with deps.worker_pool.acquire() as conn:
            rows = await conn.fetch(
                f'SELECT id, status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
                job_ids,
            )
            assert all(r["status"] == "failed" for r in rows), (
                "every job must reach failed after the two concurrent sweeps: "
                f"{[(r['id'], r['status']) for r in rows if r['status'] != 'failed']}"
            )

            attempt_counts = await conn.fetch(
                f"""SELECT job_id, count(*) AS n FROM "{schema}".job_attempts
                    WHERE job_id = ANY($1::uuid[]) GROUP BY job_id HAVING count(*) > 1""",
                job_ids,
            )
            assert attempt_counts == [], (
                "no job may end up with more than one job_attempts row from the two "
                f"concurrent sweeps: {attempt_counts}"
            )

            event_counts = await conn.fetch(
                f"""SELECT job_id, count(*) AS n FROM "{schema}".job_events
                    WHERE job_id = ANY($1::uuid[]) AND kind = 'state_change'
                    GROUP BY job_id HAVING count(*) > 1""",
                job_ids,
            )
            assert event_counts == [], (
                "no job may get more than one state-change event from the two "
                f"concurrent sweeps: {event_counts}"
            )


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

        result = await backend.mark_snoozed(
            JobId(job_id), worker_id, timedelta(seconds=30), attempt=1
        )
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
        pending, writing no event row (promotion is scheduler
        bookkeeping; the transitions of record are the terminal writes
        and the sweep/cancel audit entries)."""
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
        assert len(events) == 0

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

    async def test_promotion_writes_no_event_rows(self, clean_jobs_app: JobsApp) -> None:
        """A promoted batch produces no ``job_events`` rows at all — a row
        per promotion is the unbounded-growth vector under sustained
        admission denial (claim + promote are the cycle's two acts), so
        the aggregated denial counters on the job row carry contention
        instead."""
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
                schema=schema,
            )

        assert count == 2

        async with deps.worker_pool.acquire() as conn:
            events = await conn.fetch(
                f'SELECT job_id, kind, detail FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[]) ORDER BY job_id',
                [job_id_1, job_id_2],
            )
            rows = await conn.fetch(
                f'SELECT id, status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
                [job_id_1, job_id_2],
            )

        assert {r["id"] for r in rows} == {job_id_1, job_id_2}
        assert all(r["status"] == "pending" for r in rows)
        assert len(events) == 0


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
