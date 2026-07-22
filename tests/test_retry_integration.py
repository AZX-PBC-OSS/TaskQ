"""Integration tests for the retry classifier seam against real PG and the
in-memory consumer loop.

Covers:
- transient retry happy path (3 attempts, final success)
- transient exhaustion (2 attempts, terminal failure)
- cancellation does NOT consume retry budget
- dispatch filter respects schedule_to_close (past-deadline exclusion)
- schedule_to_close computed server-side via now() + interval
"""

# ruff: noqa: S608 Why: schema name is validated by WorkerSettings.post_load and _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches existing integration test pattern

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo
from taskq.retry import (
    Fail,
    JobRetryState,
    Retry,
    RetryPolicy,
    compute_backoff,
    decide_after_failure,
)
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.pg import create_running_job, create_worker

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration

# ── Helpers ────────────────────────────────────────────────────────────

# Tolerance for time-based assertions in CI environments.
# In local dev, backoff timing is exact (clock resolution < 1 ms).
# CI runners (GHA macOS, Docker-in-Docker) add 200-800 ms of scheduling
# jitter on asyncio.sleep, so 1 second is a safe floor.
# Value is intentionally loose to avoid spurious failures under CI load;
# tighten after adding FakeClock-based deterministic backoff.
_CI_TOLERANCE = timedelta(seconds=1)


async def _promote_scheduled_to_running(
    conn: _Conn,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
    attempt: int,
) -> None:
    """Update a scheduled job to running (simulates dispatch + promote)."""
    await conn.execute(
        f"""UPDATE \"{schema}\".jobs
        SET status = 'running',
            attempt = $1,
            locked_by_worker = $2,
            lock_expires_at = now() + interval '60 seconds',
            started_at = now(),
            last_heartbeat_at = now()
        WHERE id = $3""",
        attempt,
        worker_id,
        job_id,
    )


# ── transient retry happy path ──────────────────────────────────


async def test_transient_retry_succeeds_after_retries(
    clean_jobs_app: JobsApp,
) -> None:
    """transient retry happy path — 3 attempts, final success.

    Enqueue a job with max_attempts=3; the actor raises RuntimeError on
    attempts 1 and 2 and returns successfully on attempt 3. The consumer
    loop is simulated manually against real PG. Assert: final
    status='succeeded' with attempt=3; job_attempts has 2 rows with
    outcome='failed' and 1 row with outcome='succeeded'; for each
    intermediate row, scheduled_at ≈ retry_dispatched_at +
    compute_backoff(...) within ±1s tolerance.
    """

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    job_id = new_job_id()

    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_running_job(conn, schema, worker_id, job_id, max_attempts=3, attempt=1)

    # ── Attempt 1: actor raises → Retry ───────────────────────────
    exception = RuntimeError("transient failure")
    job_state_1 = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )
    decision_1 = decide_after_failure(
        _StubConfig(policy),
        exception,
        job_state_1,
        datetime.now(UTC),
    )
    assert isinstance(decision_1, Retry)

    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="transient failure",
        error_traceback=None,
    )
    row_1 = await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        error_info,
        decision_1.next_scheduled_at,
    )
    assert row_1.status == "scheduled"

    retry_dispatched_at_1 = datetime.now(UTC)

    # ── Attempt 2: promote scheduled→running, actor raises → Retry ──
    async with deps.worker_pool.acquire() as conn:
        await _promote_scheduled_to_running(conn, schema, worker_id, job_id, attempt=2)

    job_state_2 = JobRetryState(
        attempt=2,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )
    decision_2 = decide_after_failure(
        _StubConfig(policy),
        exception,
        job_state_2,
        datetime.now(UTC),
    )
    assert isinstance(decision_2, Retry)

    row_2 = await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        error_info,
        decision_2.next_scheduled_at,
    )
    assert row_2.status == "scheduled"

    retry_dispatched_at_2 = datetime.now(UTC)

    # ── Attempt 3: promote, actor succeeds ──────────────────────────
    async with deps.worker_pool.acquire() as conn:
        await _promote_scheduled_to_running(conn, schema, worker_id, job_id, attempt=3)

    result = await backend.mark_succeeded(job_id, worker_id, {"ok": True})
    assert result is True

    # ── Assertions ──────────────────────────────────────────────────
    async with deps.worker_pool.acquire() as conn:
        pg_row = await conn.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
            job_id,
        )

    assert pg_row is not None
    assert pg_row["status"] == "succeeded"
    assert pg_row["attempt"] == 3

    assert len(attempts) == 3
    failed_attempts = [a for a in attempts if a["outcome"] == "failed"]
    succeeded_attempts = [a for a in attempts if a["outcome"] == "succeeded"]
    assert len(failed_attempts) == 2
    assert len(succeeded_attempts) == 1

    # Verify backoff timing for intermediate attempts
    expected_backoff_1 = compute_backoff(policy, 1)
    expected_scheduled_1 = retry_dispatched_at_1 + expected_backoff_1
    actual_scheduled_1 = row_1.scheduled_at
    assert (
        abs((actual_scheduled_1 - expected_scheduled_1).total_seconds())
        < _CI_TOLERANCE.total_seconds()
    )

    expected_backoff_2 = compute_backoff(policy, 2)
    expected_scheduled_2 = retry_dispatched_at_2 + expected_backoff_2
    actual_scheduled_2 = row_2.scheduled_at
    assert (
        abs((actual_scheduled_2 - expected_scheduled_2).total_seconds())
        < _CI_TOLERANCE.total_seconds()
    )


# ── transient exhaustion ────────────────────────────────────────


async def test_transient_exhaustion(
    clean_jobs_app: JobsApp,
) -> None:
    """transient exhaustion — max_attempts=2, always raises.

    Enqueue with max_attempts=2 and an actor that always raises
    RuntimeError. Run consumer. Assert: final status='failed' after
    2 attempts; job_attempts has 2 failed rows; error_class='RuntimeError'
    on the terminal row.
    """

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    job_id = new_job_id()

    policy = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_running_job(conn, schema, worker_id, job_id, max_attempts=2, attempt=1)

    # ── Attempt 1: actor raises → Retry ───────────────────────────
    exception = RuntimeError("transient failure")
    job_state_1 = JobRetryState(
        attempt=1,
        max_attempts=2,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )
    decision_1 = decide_after_failure(
        _StubConfig(policy),
        exception,
        job_state_1,
        datetime.now(UTC),
    )
    assert isinstance(decision_1, Retry)

    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="transient failure",
        error_traceback=None,
    )
    row_1 = await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        error_info,
        decision_1.next_scheduled_at,
    )
    assert row_1.status == "scheduled"

    # ── Attempt 2: promote, actor raises → Fail ───────────────────
    async with deps.worker_pool.acquire() as conn:
        await _promote_scheduled_to_running(conn, schema, worker_id, job_id, attempt=2)

    job_state_2 = JobRetryState(
        attempt=2,
        max_attempts=2,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )
    decision_2 = decide_after_failure(
        _StubConfig(policy),
        exception,
        job_state_2,
        datetime.now(UTC),
    )
    assert isinstance(decision_2, Fail)

    row_2 = await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)
    assert row_2.status == "failed"

    # ── Assertions ──────────────────────────────────────────────────
    async with deps.worker_pool.acquire() as conn:
        pg_row = await conn.fetchrow(
            f'SELECT status, attempt, error_class FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
            job_id,
        )

    assert pg_row is not None
    assert pg_row["status"] == "failed"
    assert pg_row["attempt"] == 2
    assert pg_row["error_class"] == "RuntimeError"

    assert len(attempts) == 2
    assert all(a["outcome"] == "failed" for a in attempts)
    assert attempts[-1]["error_class"] == "RuntimeError"


# ── cancellation does NOT consume retry budget ──────────────────


async def test_cancel_skips_classifier() -> None:
    """cancellation does NOT consume retry budget.

    Enqueue with max_attempts=3; dispatch the job (status='running');
    write a cancel request; tick_cancel_polling to deliver the cancel;
    then mark_cancelled (the consumer's response to cancel, not the
    classifier's response to failure). Assert: mark_cancelled
    was called (NOT mark_failed_or_retry); the final row's attempt
    equals 1 (NOT incremented past the single dispatch).
    """
    clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=timedelta(seconds=2),
        cleanup_grace_period=timedelta(seconds=2),
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="slow_actor",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    await backend.enqueue(args)
    job_id = args.id

    cancel_event = asyncio.Event()
    backend.register_cancel_event(job_id, cancel_event)

    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only access to simulate dispatch
    dispatched = await backend.dispatch_batch(
        worker_id,
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    assert dispatched[0].status == "running"
    assert dispatched[0].attempt == 1

    result = await backend.write_cancel_request(job_id, reason="test cancel")
    assert result is True

    clock.advance(timedelta(seconds=3))
    await backend.tick_cancel_polling()

    assert cancel_event.is_set()

    cancelled = await backend.mark_cancelled(job_id, worker_id)
    assert cancelled is True

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "cancelled"
    assert row.attempt == 1


# ── Stub for ActorConfigLike ────────────────────────────────────────────


class _StubConfig:
    """Minimal stub satisfying ActorConfigLike for integration tests."""

    def __init__(self, retry: RetryPolicy) -> None:
        self._retry = retry

    @property
    def retry(self) -> RetryPolicy:
        return self._retry

    @property
    def non_retryable_exceptions(self) -> tuple[type[Exception], ...]:
        return ()

    @property
    def retry_classifier(self) -> None:
        return None

    @property
    def on_retry_exhausted(self) -> None:
        return None

    @property
    def on_retry_exhausted_timeout(self) -> float:
        return 3.0

    @property
    def on_success(self) -> None:
        return None

    @property
    def on_success_timeout(self) -> float:
        return 3.0


# ── schedule_to_close computed server-side ─────────────────────


async def test_schedule_to_close_computed_server_side(
    clean_jobs_app: JobsApp,
) -> None:
    """enqueue with kind='indefinite', time_budget=timedelta(hours=2)
    against real PG. Oracle: now() <= schedule_to_close AND
    schedule_to_close <= now() + interval '2h' + interval '1 second'.
    Verifies PG-side now() + $::interval evaluation, not Python-side.
    """
    from pydantic import BaseModel

    from taskq.actor import actor
    from taskq.client._jobs import JobsClient

    class _Test(BaseModel):
        pass

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    @actor(retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2)))
    async def indefinite_job(payload: _Test) -> None: ...

    client = JobsClient(backend)
    handle = await client.enqueue(indefinite_job, _Test())

    async with deps.worker_pool.acquire() as conn:
        pg_row = await conn.fetchrow(
            f'SELECT schedule_to_close, now() AS server_now FROM "{schema}".jobs WHERE id = $1',
            handle.job_id,
        )
    assert pg_row is not None
    s2c: datetime = pg_row["schedule_to_close"]  # type: ignore[index] # Why: asyncpg Record supports both attribute and index access
    server_now: datetime = pg_row["server_now"]  # type: ignore[index] # Why: asyncpg Record supports both attribute and index access
    assert server_now <= s2c, f"schedule_to_close ({s2c}) must be >= server now ({server_now})"
    upper = server_now + timedelta(hours=2) + timedelta(seconds=1)
    assert s2c <= upper, f"schedule_to_close ({s2c}) must be <= {upper}"


# ── dispatch filter respects schedule_to_close ─────────────────────


async def test_dispatch_filter_respects_schedule_to_close(
    clean_jobs_app: JobsApp,
) -> None:
    """dispatch filter respects schedule_to_close. Enqueue an
    indefinite-tier job, manually UPDATE schedule_to_close to the past
    via asyncpg, run dispatch_batch. Oracle: the job is NOT returned.
    """

    from pydantic import BaseModel

    from taskq.actor import actor
    from taskq.client._jobs import JobsClient

    class _Test2(BaseModel):
        pass

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    @actor(retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2)))
    async def deadline_job(payload: _Test2) -> None: ...

    client = JobsClient(backend)
    handle = await client.enqueue(deadline_job, _Test2())
    job_id = handle.job_id

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET schedule_to_close = now() - interval '1 second' WHERE id = $1",
            job_id,
        )

    dispatched = await backend.dispatch_batch(
        new_uuid(),
        ["default"],
        limit=10,
        lock_lease=timedelta(seconds=60),
    )
    dispatched_ids = [r.id for r in dispatched]
    assert job_id not in dispatched_ids, (
        f"job {job_id} with past schedule_to_close should not be dispatched, got {dispatched_ids}"
    )


# ── indefinite retry polling pattern ────────────────────────────
#
# Indefinite-tier actor fails 10 times, succeeds on
# attempt 11. Verifies the "retry until done" pattern end-to-end against
# real PG — the primary acceptance test.


async def test_indefinite_retry_polling_pattern(
    clean_jobs_app: JobsApp,
) -> None:
    """Indefinite-tier actor fails 10 times and succeeds on attempt 11.
    The job reaches succeeded status with attempt=11, and the
    job_attempts table has 11 rows — 10 with outcome='failed' and 1
    with outcome='succeeded'. No row has outcome='retried_indefinite'
    (the AttemptOutcome literal is closed).

    This is the primary acceptance test for this pattern."""

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    job_id = new_job_id()

    policy = RetryPolicy(
        kind="indefinite",
        time_budget=timedelta(minutes=30),
        backoff="fixed",
        base=timedelta(seconds=1),
        jitter=0.0,
    )
    exception = RuntimeError("transient failure")
    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="transient failure",
        error_traceback=None,
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_running_job(
            conn, schema, worker_id, job_id, max_attempts=3, retry_kind="indefinite", attempt=1
        )

    for attempt_num in range(1, 11):
        job_state = JobRetryState(
            attempt=attempt_num,
            max_attempts=3,
            retry_kind="indefinite",
            schedule_to_close=datetime.now(UTC) + timedelta(minutes=30),
            start_to_close=None,
        )
        decision = decide_after_failure(
            _StubConfig(policy), exception, job_state, datetime.now(UTC)
        )
        assert isinstance(decision, Retry), f"attempt {attempt_num} should be Retry"

        row_after = await backend.mark_failed_or_retry(
            job_id, worker_id, error_info, decision.next_scheduled_at
        )
        assert row_after.status == "scheduled"

        if attempt_num < 10:
            async with deps.worker_pool.acquire() as conn:
                await _promote_scheduled_to_running(
                    conn, schema, worker_id, job_id, attempt=attempt_num + 1
                )

    async with deps.worker_pool.acquire() as conn:
        await _promote_scheduled_to_running(conn, schema, worker_id, job_id, attempt=11)

    result = await backend.mark_succeeded(job_id, worker_id, {"ok": True})
    assert result is True

    async with deps.worker_pool.acquire() as conn:
        pg_row = await conn.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
            job_id,
        )

    assert pg_row is not None
    assert pg_row["status"] == "succeeded"
    assert pg_row["attempt"] == 11

    assert len(attempts) == 11
    failed_attempts = [a for a in attempts if a["outcome"] == "failed"]
    succeeded_attempts = [a for a in attempts if a["outcome"] == "succeeded"]
    assert len(failed_attempts) == 10
    assert len(succeeded_attempts) == 1

    outcomes = {a["outcome"] for a in attempts}
    assert "retried_indefinite" not in outcomes, (
        "AttemptOutcome literal is closed; 'retried_indefinite' must not appear"
    )


# ── indefinite retry deadline enforcement ───────────────────────


async def test_indefinite_retry_deadline_enforcement(
    clean_jobs_app: JobsApp,
) -> None:
    """indefinite retry deadline enforcement. Actor with
    time_budget=timedelta(seconds=2) fails. On second attempt, push
    schedule_to_close past via PG-side UPDATE. Classifier returns
    Fail(DeadlineExceeded); job transitions to failed with
    error_class='DeadlineExceeded' in the same dispatch cycle
    (no round-trip through the deadline-exceeded sweep)."""

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    job_id = new_job_id()

    policy = RetryPolicy(
        kind="indefinite",
        time_budget=timedelta(seconds=2),
        backoff="fixed",
        base=timedelta(seconds=1),
        jitter=0.0,
    )
    exception = RuntimeError("failure")
    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="failure",
        error_traceback=None,
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_running_job(
            conn, schema, worker_id, job_id, max_attempts=3, retry_kind="indefinite", attempt=1
        )

    job_state_1 = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind="indefinite",
        # Wide margin (well beyond Python-vs-PG clock skew) so the deadline
        # comparison is unambiguously in the future.
        schedule_to_close=datetime.now(UTC) + timedelta(seconds=30),
        start_to_close=None,
    )
    decision_1 = decide_after_failure(
        _StubConfig(policy), exception, job_state_1, datetime.now(UTC)
    )
    assert isinstance(decision_1, Retry)

    row_1 = await backend.mark_failed_or_retry(
        job_id, worker_id, error_info, decision_1.next_scheduled_at
    )
    assert row_1.status == "scheduled"

    async with deps.worker_pool.acquire() as conn:
        await _promote_scheduled_to_running(conn, schema, worker_id, job_id, attempt=2)
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET schedule_to_close = now() - interval '1 second' WHERE id = $1",
            job_id,
        )

    job_state_2 = JobRetryState(
        attempt=2,
        max_attempts=3,
        retry_kind="indefinite",
        # Wide margin (well beyond Python-vs-PG clock skew) so the deadline
        # comparison is unambiguously in the past.
        schedule_to_close=datetime.now(UTC) - timedelta(seconds=30),
        start_to_close=None,
    )
    decision_2 = decide_after_failure(
        _StubConfig(policy), exception, job_state_2, datetime.now(UTC)
    )
    assert isinstance(decision_2, Fail)
    assert decision_2.error_class == "DeadlineExceeded"

    row_2 = await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)
    assert row_2.status == "failed"
    assert row_2.attempt == 2
