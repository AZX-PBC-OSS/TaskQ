"""Chaos tests: concurrent terminal writes, transaction rollback, cancel race.

concurrent terminal writes from two workers - only one wins.
Covers all five terminal writes (mark_succeeded, mark_failed_or_retry,
mark_cancelled, mark_abandoned, mark_snoozed) plus write_cancel_request
so the "from two concurrent workers against the
same job" clause is satisfied for every method.

PG fails mid-way through a terminal write - nothing lands. The mark_*
terminal writes are ONE fused statement (jobs UPDATE + job_attempts
INSERT + job_events INSERT in a single data-modifying-CTE statement -
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
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_pending_job, create_running_job, create_worker

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback - asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = [pytest.mark.integration, pytest.mark.slow]


# ── Helpers ────────────────────────────────────────────────────────────


# ── concurrent terminal writes from two workers ─────────────────


class TestConcurrentTerminalWrites:
    """concurrent terminal writes from two workers - only one wins.

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
                backend.mark_succeeded(job_id, worker_a, {"ok": True}, attempt=1, claim_epoch=1),
                backend.mark_succeeded(job_id, worker_b, {"ok": True}, attempt=1, claim_epoch=1),
                return_exceptions=True,
            )
            assert results == [True, False]

        elif method == "mark_failed_or_retry":
            results = await asyncio.gather(
                backend.mark_failed_or_retry(
                    job_id, worker_a, error_info, retry_delay=None, attempt=1, claim_epoch=1
                ),
                backend.mark_failed_or_retry(
                    job_id, worker_b, error_info, retry_delay=None, attempt=1, claim_epoch=1
                ),
                return_exceptions=True,
            )
            row_a = results[0]
            exc_b = results[1]
            assert not isinstance(row_a, Exception)
            assert isinstance(exc_b, WorkerOwnershipMismatch)

        elif method == "mark_cancelled":
            results = await asyncio.gather(
                backend.mark_cancelled(job_id, worker_a, attempt=1, claim_epoch=1),
                backend.mark_cancelled(job_id, worker_b, attempt=1, claim_epoch=1),
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
                backend.mark_snoozed(
                    job_id, worker_a, timedelta(seconds=30), attempt=1, claim_epoch=1
                ),
                backend.mark_snoozed(
                    job_id, worker_b, timedelta(seconds=30), attempt=1, claim_epoch=1
                ),
                return_exceptions=True,
            )
            assert results == ["scheduled", "noop"]

        elif method == "mark_retry_after":
            results = await asyncio.gather(
                backend.mark_retry_after(
                    job_id,
                    worker_a,
                    timedelta(seconds=30),
                    consume_budget=True,
                    attempt=1,
                    claim_epoch=1,
                ),
                backend.mark_retry_after(
                    job_id,
                    worker_b,
                    timedelta(seconds=30),
                    consume_budget=True,
                    attempt=1,
                    claim_epoch=1,
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

        elif method == "mark_snoozed":
            # Exactly one snooze landed (status scheduled); it wrote no
            # rows - a deferral is not an execution - so the loser's
            # predicate miss and the winner's write agree on zero attempt
            # rows, and the only event is the dispatch seed.
            assert row["status"] == "scheduled"
            assert len(attempts) == 0
            assert len(events) == 1
            assert events[0]["kind"] == "state_change"

        elif method == "mark_retry_after":
            # consume_budget=True: the winner's deferral IS a real
            # execution - one attempt row, one event of its own.
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
    """PG fails mid-way through the terminal write - nothing lands.

    Failure-model note (the re-scope): the previous injector poisoned the
    att CTE's (job_id, attempt) PK with a pre-seeded job_attempts row and
    relied on UniqueViolationError aborting the fused statement. That
    premise no longer exists - every job_attempts insert now carries
    ``ON CONFLICT (job_id, attempt) DO NOTHING`` (backend/_sql_templates.py):
    a claim-clamped attempt number repeats at the smallint ceiling, and the
    deliberate doctrine is to keep the first record of the number rather
    than roll the terminal transition back on a collision. A poisoned row
    is therefore absorbed, not fatal - by design.

    What still must hold, and what this test pins: the mark_* terminal
    writes are ONE fused statement (jobs UPDATE + job_attempts INSERT +
    job_events INSERT in a single data-modifying-CTE statement - see
    _terminal.py's module docstring), so a failure anywhere in it commits
    nothing. The surviving genuine mid-flight failure is the death of the
    connection itself while the statement executes. This test injects
    exactly that: a second session holds a FOR UPDATE row lock on the jobs
    row, the fused statement blocks on that lock mid-execution (observed
    lock-waiting in pg_stat_activity - the statement has started, taken
    its snapshot, and reached the row), and an admin session then
    pg_terminate_backend()s the blocked backend. Asserts the caller sees
    the connection die, and the statement's atomicity leaves the job row
    still ``running`` with no job_attempts or terminal job_events rows -
    the executable proof that the fused UPDATE cannot commit without its
    attempt/event INSERTs. Single-statement atomicity itself is a
    PostgreSQL engine guarantee with no unfenced application seam to run
    red against; the red discriminators are the injection self-checks
    below (the victim must be observed lock-waiting, the terminate must
    report a backend was killed, and the death must be connection-level,
    not a statement/lock timeout) plus the nothing-landed assertions,
    which fail the moment the terminal write is ever de-fused into
    separate statements. The worker-level halves of the same property -
    the actor's side effects never commit, the job is never reported
    succeeded, batch hooks never fire over a dead attempt - are pinned by
    the runner/consumer families (test_runner_escape_batch_hook_parity.py,
    test_runner_force_cancel_absorption.py).
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

        error_info = ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None)

        # Blocker: holds the jobs row's lock in an open transaction, so the
        # fused terminal statement blocks mid-execution when its upd CTE
        # goes to lock the same row.
        blocker = await asyncpg.connect(pg_dsn)
        admin = await asyncpg.connect(pg_dsn)
        victim_task: asyncio.Task[object] | None = None
        try:
            await blocker.execute("BEGIN")
            locked = await blocker.fetchval(
                f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE', job_id
            )
            assert locked == job_id

            victim_task = asyncio.create_task(
                backend.mark_failed_or_retry(
                    job_id, worker_id, error_info, retry_delay=None, attempt=1, claim_epoch=1
                )
            )

            # Deadline-based wait for the victim statement to be observed
            # blocked on the row lock - proof the failure lands
            # mid-statement, not before it starts. Scoped to the current
            # database per the pg_stat_activity hygiene rule. If the victim
            # never reaches the lock wait the injection silently did
            # nothing, and this wait fails the test instead.
            victim_pid: int | None = None

            async def _victim_blocked() -> bool:
                nonlocal victim_pid
                victim_pid = await admin.fetchval(
                    """
                    SELECT pid FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND wait_event_type = 'Lock'
                      AND query LIKE '%' || $1 || '%'
                      AND query LIKE '%job_attempts%'
                    LIMIT 1
                    """,
                    schema,
                )
                return victim_pid is not None

            await wait_for_condition(
                _victim_blocked,
                description="fused terminal statement blocked on the poisoned row lock",
                timeout=5.0,
            )
            assert victim_pid is not None, (
                "the admin session never observed the terminal write "
                "lock-waiting - the mid-statement injection did not "
                "happen, so a pass would prove nothing"
            )

            terminated = await admin.fetchval("SELECT pg_terminate_backend($1)", victim_pid)
            assert terminated is True, (
                "pg_terminate_backend found no live backend for the blocked "
                "statement - the injection did not happen"
            )

            # The raises tuple deliberately excludes builtin OSError:
            # builtin TimeoutError IS an OSError, so catching OSError here
            # would let a wait_for timeout (the injection never landing)
            # masquerade as the victim's connection death. The observed
            # death shape is asyncpg's ConnectionDoesNotExistError - a
            # PostgresError subclass - so the two asyncpg families suffice.
            with pytest.raises((asyncpg.PostgresError, asyncpg.InterfaceError)) as exc_info:
                await asyncio.wait_for(victim_task, timeout=5.0)
            # The death must be the terminated connection, not a statement
            # or lock timeout beating the injector to it - those abort the
            # statement just as atomically, but they are a different
            # failure mode than the one this test exists to pin.
            assert not isinstance(
                exc_info.value,
                (asyncpg.QueryCanceledError, asyncpg.LockNotAvailableError),
            ), (
                f"expected connection death mid-statement, got "
                f"{type(exc_info.value).__name__} - the timeout path fired "
                "instead of pg_terminate_backend"
            )
        finally:
            if victim_task is not None and not victim_task.done():
                victim_task.cancel()
            await blocker.close()  # rolls the lock-holding transaction back
            await admin.close()

        # Reconnect with a fresh connection and verify nothing landed.
        verify_conn = await asyncpg.connect(pg_dsn)
        try:
            row = await verify_conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None
            assert row["status"] == "running", (
                "failed terminal write must leave the job 'running' - the fused "
                "statement aborted whole (at-least-once reclaim contract)"
            )

            attempts = await verify_conn.fetch(
                f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            assert len(attempts) == 0, (
                "no attempt row may persist from the killed statement - the "
                "jobs UPDATE, the attempt INSERT, and the event INSERT are "
                "one statement and roll back together"
            )

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
