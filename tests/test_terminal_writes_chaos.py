"""Chaos tests: concurrent terminal writes, transaction rollback, cancel race.

concurrent terminal writes from two workers — only one wins.
Covers all five terminal writes (mark_succeeded, mark_failed_or_retry,
mark_cancelled, mark_abandoned, mark_snoozed) plus write_cancel_request
so the "from two concurrent workers against the
same job" clause is satisfied for every method.

PG fails mid-way through a terminal write — nothing lands. The mark_*
terminal writes are ONE fused statement (jobs UPDATE + job_attempts
INSERT + job_events INSERT in a single data-modifying-CTE statement —
see _terminal.py's module docstring), so the mid-flight seam is inside
the statement itself; this test is the executable proof that "Every
running-state terminal transition atomically emits one job_events row
and one job_attempts row in the same statement as the parent UPDATE."

request_cancel racing against worker dispatch. Run ~50
iterations with random small delays; assert all end in a consistent
state.
"""

import asyncio
import json
import random
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_pending_job, create_running_job, create_worker

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = [pytest.mark.integration, pytest.mark.slow]


# ── Helpers ────────────────────────────────────────────────────────────


# ── concurrent terminal writes from two workers ─────────────────


class TestConcurrentTerminalWrites:
    """concurrent terminal writes from two workers — only one wins.

    Parametrized over the six methods: mark_succeeded,
    mark_failed_or_retry, mark_cancelled, mark_abandoned, mark_snoozed,
    write_cancel_request. For each: enqueue and dispatch one job;
    worker A holds the lock. Simultaneously call the method from
    worker A and worker B via ``asyncio.gather``.
    """

    @pytest.mark.parametrize(
        "method",
        [
            "mark_succeeded",
            "mark_failed_or_retry",
            "mark_cancelled",
            "mark_abandoned",
            "mark_snoozed",
            "mark_retry_after",
            "write_cancel_request",
        ],
    )
    async def test_concurrent_terminal_write_only_one_wins(
        self,
        method: str,
        jobs_app: JobsApp,
        pg_dsn: str,
    ) -> None:
        """Two concurrent workers call the same terminal write on one job.

        mark_succeeded / mark_cancelled / mark_snoozed: both pass
        running-state args with their own worker_id; exactly one True,
        one False.

        mark_failed_or_retry: worker A returns JobRow; worker B raises
        WorkerOwnershipMismatch. This asymmetry exists because
        mark_failed_or_retry returns JobRow on success but raises on
        predicate miss, unlike the bool-returning methods which
        silently return False.

        mark_abandoned: pre-set cancel_phase=2; both callers invoke
        mark_abandoned (no worker_id arg); the SQL ``status='running'
        AND cancel_phase=2`` gate serializes; exactly one True, one
        False.

        write_cancel_request (running + cancel_phase=0 case): both
        pass reason='race'; the SQL ``cancel_phase=0`` gate serializes;
        exactly one True, one False; exactly one cancel_request event
        row, no duplicates.
        """

        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name

        worker_a = new_uuid()
        worker_b = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_a)
            await create_worker(conn, schema, worker_b)
            cancel_phase = 2 if method == "mark_abandoned" else 0
            job_id = await create_running_job(conn, schema, worker_a, cancel_phase=cancel_phase)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
        )

        # Race the two callers
        if method == "mark_succeeded":
            results = await asyncio.gather(
                backend.mark_succeeded(job_id, worker_a, {"ok": True}),
                backend.mark_succeeded(job_id, worker_b, {"ok": True}),
                return_exceptions=True,
            )
            assert results == [True, False]

        elif method == "mark_failed_or_retry":
            results = await asyncio.gather(
                backend.mark_failed_or_retry(job_id, worker_a, error_info, retry_delay=None),
                backend.mark_failed_or_retry(job_id, worker_b, error_info, retry_delay=None),
                return_exceptions=True,
            )
            row_a = results[0]
            exc_b = results[1]
            assert not isinstance(row_a, Exception)
            assert isinstance(exc_b, WorkerOwnershipMismatch)

        elif method == "mark_cancelled":
            results = await asyncio.gather(
                backend.mark_cancelled(job_id, worker_a),
                backend.mark_cancelled(job_id, worker_b),
                return_exceptions=True,
            )
            assert results == [True, False]

        elif method == "mark_abandoned":
            results = await asyncio.gather(
                backend.mark_abandoned(job_id),
                backend.mark_abandoned(job_id),
                return_exceptions=True,
            )
            wins = [r for r in results if r is True]
            no_ops = [r for r in results if r is False]
            assert len(wins) == 1
            assert len(no_ops) == 1

        elif method == "mark_snoozed":
            results = await asyncio.gather(
                backend.mark_snoozed(job_id, worker_a, timedelta(seconds=30)),
                backend.mark_snoozed(job_id, worker_b, timedelta(seconds=30)),
                return_exceptions=True,
            )
            assert results == ["scheduled", "noop"]

        elif method == "mark_retry_after":
            results = await asyncio.gather(
                backend.mark_retry_after(
                    job_id, worker_a, timedelta(seconds=30), consume_budget=True
                ),
                backend.mark_retry_after(
                    job_id, worker_b, timedelta(seconds=30), consume_budget=True
                ),
                return_exceptions=True,
            )
            wins = [r for r in results if r == "scheduled"]
            no_ops = [r for r in results if r == "noop"]
            assert len(wins) == 1
            assert len(no_ops) == 1

        elif method == "write_cancel_request":
            results = await asyncio.gather(
                backend.write_cancel_request(job_id, reason="race"),
                backend.write_cancel_request(job_id, reason="race"),
                return_exceptions=True,
            )
            wins = [r for r in results if r is True]
            no_ops = [r for r in results if r is False]
            assert len(wins) == 1
            assert len(no_ops) == 1

        # Verify final row state consistency
        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, cancel_phase, cancel_requested_at, locked_by_worker "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempts = await conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            events = await conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )

        assert row is not None

        if method == "mark_succeeded":
            assert row["status"] == "succeeded"
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "succeeded"
            assert len(events) == 2
            assert events[0]["kind"] == "state_change"

        elif method == "mark_failed_or_retry":
            assert row["status"] == "failed"
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "failed"
            assert len(events) == 2
            assert events[0]["kind"] == "state_change"

        elif method == "mark_cancelled":
            assert row["status"] == "cancelled"
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "cancelled"
            assert len(events) == 2
            assert events[0]["kind"] == "state_change"

        elif method == "mark_abandoned":
            assert row["status"] == "abandoned"
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "cancelled"
            assert len(events) == 2
            assert events[0]["kind"] == "state_change"

        elif method in ("mark_snoozed", "mark_retry_after"):
            assert row["status"] == "scheduled"
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "snoozed"
            assert len(events) == 2
            assert events[0]["kind"] == "state_change"

        elif method == "write_cancel_request":
            assert row["cancel_phase"] == 1
            assert row["cancel_requested_at"] is not None
            assert len(attempts) == 0
            cancel_events = [e for e in events if e["kind"] == "cancel_request"]
            assert len(cancel_events) == 1


# ── transaction rollback on mid-flight failure ──────────────────


class TestTransactionRollbackOnMidFlightFailure:
    """PG fails mid-way through the terminal write — nothing lands.

    The mark_* terminal writes are ONE fused statement (jobs UPDATE +
    job_attempts INSERT + job_events INSERT in a single data-modifying-CTE
    statement — see _terminal.py's module docstring), so there is no
    Python seam between the UPDATE and the INSERTs to fail on: the only
    genuine "mid-flight" failure left is SERVER-side, partway through the
    statement itself.  This test injects one: a pre-seeded job_attempts
    row collides with the att CTE's PK (job_id, attempt) after the jobs
    UPDATE has matched and locked the row, forcing a UniqueViolationError
    from inside the statement.  Asserts the exception propagates, and the
    statement's atomicity leaves the job row still ``running`` with no
    new job_attempts or job_events rows — the executable proof that the
    fused UPDATE cannot commit without its attempt/event INSERTs.
    """

    async def test_transaction_rollback_on_mid_flight_failure(
        self,
        jobs_app: JobsApp,
        pg_dsn: str,
    ) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(conn, schema, worker_id)
            # The poisoned row: same (job_id, attempt) key the fused att CTE
            # will try to INSERT.  The jobs UPDATE matches and locks the row
            # first; the att INSERT then dies on job_attempts_pkey.
            await conn.execute(
                f"""INSERT INTO "{schema}".job_attempts
                    (job_id, attempt, started_at, outcome, metadata)
                    VALUES ($1, 1, clock_timestamp(), 'bogus', '{{}}'::jsonb)""",
                job_id,
            )

        error_info = ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None)
        with pytest.raises(asyncpg.PostgresError):
            await backend.mark_failed_or_retry(job_id, worker_id, error_info, retry_delay=None)

        # Reconnect with a fresh connection and verify nothing landed.
        verify_conn = await asyncpg.connect(pg_dsn)
        try:
            row = await verify_conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None
            assert row["status"] == "running", (
                "failed terminal write must leave the job 'running' — the fused "
                "statement aborted whole (at-least-once reclaim contract)"
            )

            attempts = await verify_conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            assert len(attempts) == 1, "only the pre-seeded attempt row should exist"
            assert attempts[0]["outcome"] == "bogus"

            events = await verify_conn.fetch(
                f'SELECT * FROM "{schema}".job_events WHERE job_id = $1', job_id
            )
            assert len(events) == 1, (
                "job_events row should still contain the pending->running event from create_running_job (which ran before the injected failure)"
            )
            kinds = {event["kind"] for event in events}
            assert "state_change" in kinds
            details = [event["detail"] for event in events]
            to_states = [
                (d["to_state"] if isinstance(d, dict) else json.loads(d)["to_state"])
                for d in details
            ]
            assert to_states == ["running"], (
                "no terminal state_change event may persist from the aborted statement"
            )
        finally:
            await verify_conn.close()


# ── request_cancel racing against worker dispatch ──────────────


class TestCancelRequestRaceAgainstDispatch:
    """``write_cancel_request`` racing against worker dispatch.

    Enqueue a ``pending`` job. Use ``asyncio.gather`` to race:
    (a) the dispatch path that moves the job to ``running``, and
    (b) ``write_cancel_request`` that cancels the pending job.

    Both tasks use their own connection so the SQL ``WHERE`` clause is
    the only serialization point. Assert the final state is consistent:
    either ``running`` (dispatch won) or ``cancelled`` (cancel won).
    Never both. Run ~50 iterations with random small delays.
    """

    async def test_cancel_racing_dispatch(
        self,
        jobs_app: JobsApp,
        pg_dsn: str,
    ) -> None:

        deps = jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)

        dispatch_sql = f"""\
UPDATE \"{schema}\".jobs
SET status = 'running',
    locked_by_worker = $2,
    lock_expires_at = now() + $3::interval,
    started_at = now(),
    last_heartbeat_at = now(),
    attempt = attempt + 1
WHERE id = $1 AND status = 'pending'"""

        cancel_sql = f"""\
UPDATE \"{schema}\".jobs
SET status = 'cancelled', finished_at = now()
WHERE id = $1 AND status IN ('pending', 'scheduled')"""

        iterations = 50
        for i in range(iterations):
            # Create a fresh pending job for each iteration
            async with deps.worker_pool.acquire() as conn:
                job_id = await create_pending_job(conn, schema)

            # Open two independent connections for the race
            dispatch_conn = await asyncpg.connect(pg_dsn)
            cancel_conn = await asyncpg.connect(pg_dsn)
            try:
                delay_dispatch = random.random() * 0.002
                delay_cancel = random.random() * 0.002

                async def _dispatch(
                    _delay: float = delay_dispatch,
                    _conn: asyncpg.Connection = dispatch_conn,
                    _job_id: UUID = job_id,
                ) -> str:
                    await asyncio.sleep(_delay)
                    return await _conn.execute(
                        dispatch_sql,
                        _job_id,
                        worker_id,
                        timedelta(seconds=60),
                    )

                async def _cancel(
                    _delay: float = delay_cancel,
                    _conn: asyncpg.Connection = cancel_conn,
                    _job_id: UUID = job_id,
                ) -> str:
                    await asyncio.sleep(_delay)
                    return await _conn.execute(cancel_sql, _job_id)

                _, _ = await asyncio.gather(_dispatch(), _cancel())

            finally:
                await dispatch_conn.close()
                await cancel_conn.close()

            # Verify consistent final state
            async with deps.worker_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT status, cancel_phase, cancel_requested_at, "
                    f'locked_by_worker FROM "{schema}".jobs WHERE id = $1',
                    job_id,
                )

            assert row is not None
            final_status: str = row["status"]

            if final_status == "running":
                assert row["cancel_phase"] == 0, (
                    f"iteration {i}: running job should have cancel_phase=0"
                )
                assert row["cancel_requested_at"] is None, (
                    f"iteration {i}: running job should have no cancel_requested_at"
                )
            elif final_status == "cancelled":
                assert row["locked_by_worker"] is None, (
                    f"iteration {i}: cancelled job should have no locked_by_worker"
                )
            else:
                pytest.fail(
                    f"iteration {i}: unexpected status {final_status!r}, "
                    "expected 'running' or 'cancelled'"
                )
