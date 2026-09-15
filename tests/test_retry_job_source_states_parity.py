"""An operator can re-run a job that already finished.

``retry_job`` is the "run this again" action behind the admin UI's retry button
and the operator's replay path. It accepts ``failed``, ``crashed`` and
``cancelled`` and refuses ``succeeded`` and ``abandoned``.

Those two refusals leave an operator with no supported path through the exact
situations replay exists for:

**A job that succeeded at doing the wrong thing.** A bug ships, a batch of jobs
runs to completion against it, the bug is fixed. Every one of those jobs is
``succeeded``, and every one of them needs to run again. The status records that
the actor returned without raising — it is not a claim that the work was
correct, and it is not a reason to refuse to repeat it.

**A job abandoned by a worker restart.** ``abandoned`` is written by the
shutdown path when a job outlives the grace periods; the job did not fail, it
was interrupted by a deploy. It is the state most likely to need a manual
re-run and the one an operator is most likely to reach for the retry button on.

Today both return ``False``, and the admin UI turns that into a 409 telling the
operator the job "is not in a retryable state" — with nothing in the code or the
docs explaining why these two states are different from the three that are
allowed.

The one state that genuinely cannot be a retry source is the one a worker is
executing right now: re-pending that row races the live attempt's terminal
write and the job can run twice concurrently. That exclusion is a correctness
constraint and is pinned here alongside the widening, so satisfying one cannot
quietly give up the other.

Pinned on both backends: an admin action that worked on one and refused on the
other would be worse than one that refuses consistently.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import ErrorInfo, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
_ERROR = ErrorInfo(
    error_class="TransientError",
    error_message="boom",
    error_traceback=None,
)


async def _worker_of(backend: Backend) -> UUID:
    """A worker id that exists in the backend's ``workers`` table."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; mirrors tests/test_reclaim_retry_budget_parity.py
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_reclaim_retry_budget_parity.py
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    worker_id = new_uuid()
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; every value is $N-bound
            worker_id,
            "test-host",
            12345,
            ["default"],
        )
    return worker_id


async def _enqueue(backend: Backend, *, max_attempts: int = 3) -> JobId:
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"k": "v"},
            max_attempts=max_attempts,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    return job_id


async def _claim(backend: Backend, job_id: JobId, worker_id: UUID) -> int:
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LOCK_LEASE,
    )
    assert job_id in {row.id for row in dispatched}, (
        "the scenario requires the job to have been claimed for an attempt"
    )
    row = await backend.get(job_id)
    assert row is not None
    return row.attempt


async def _run_to_succeeded(backend: Backend, job_id: JobId) -> None:
    """Drive the job to ``succeeded`` through the real claim-and-complete path."""
    worker_id = await _worker_of(backend)
    attempt = await _claim(backend, job_id, worker_id)
    await backend.mark_succeeded(job_id, worker_id, {"ok": True}, attempt=attempt)
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded", (
        f"the scenario requires a genuinely succeeded job, got {row.status!r}"
    )


async def _run_to_abandoned(backend: Backend, job_id: JobId) -> None:
    """Drive the job to ``abandoned`` the way a worker restart does.

    A shutdown signals the running job, escalates when the cancellation grace
    period expires, and abandons it when the cleanup grace period expires too.
    Driving all three steps rather than forcing the status keeps the source
    state one production actually produces.
    """
    worker_id = await _worker_of(backend)
    await _claim(backend, job_id, worker_id)
    assert await backend.write_cancel_request(job_id, "worker shutting down"), (
        "the scenario requires the shutdown's cancel request to reach the job"
    )
    assert await backend.write_cancel_escalation(job_id, worker_id, phase=2), (
        "the scenario requires the shutdown to escalate past the cancellation grace period"
    )
    await backend.mark_abandoned(job_id)
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "abandoned", (
        f"the scenario requires a genuinely abandoned job, got {row.status!r}"
    )


async def test_retry_job_re_runs_a_succeeded_job(backend_pair: Backend) -> None:
    """A job that completed can be replayed by an operator.

    The replay case: a bug shipped, the jobs ran to completion against it, the
    bug is fixed, and the work has to happen again. ``succeeded`` says the
    actor returned without raising — it does not say the result was right, and
    refusing to repeat it leaves the operator with no supported path.
    """
    job_id = await _enqueue(backend_pair)
    await _run_to_succeeded(backend_pair, job_id)

    retried = await backend_pair.retry_job(job_id)
    assert retried is True, (
        "an operator asking to re-run a completed job was refused. This is the "
        "replay path after a bad deploy — the jobs ran, the code was wrong, the "
        "work must happen again — and refusing it leaves no supported way to do "
        "that; the admin UI turns this into a 409 saying the job 'is not in a "
        "retryable state'"
    )

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status in ("pending", "scheduled"), (
        f"a re-run job must be claimable again, got status {row.status!r}"
    )
    assert row.finished_at is None, (
        "a job re-pended for another run must not still carry the previous run's finished_at"
    )
    assert row.result is None, (
        "the previous run's result must be cleared when the job is re-pended, "
        "so a caller reading the handle cannot mistake the stale result for "
        "the new run's answer"
    )


async def test_retry_job_re_runs_an_abandoned_job(backend_pair: Backend) -> None:
    """A job abandoned by a worker restart can be replayed by an operator.

    ``abandoned`` means a deploy interrupted the job, not that the job did
    anything wrong. It is the state most likely to need a manual re-run, and
    the retry button is where an operator will look for one.
    """
    job_id = await _enqueue(backend_pair)
    await _run_to_abandoned(backend_pair, job_id)

    retried = await backend_pair.retry_job(job_id)
    assert retried is True, (
        "an operator asking to re-run a job abandoned by a worker restart was "
        "refused. The job did not fail — a deploy interrupted it — so this is "
        "precisely the work an operator needs to put back, and there is no "
        "supported path to do it"
    )

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status in ("pending", "scheduled"), (
        f"a re-run job must be claimable again, got status {row.status!r}"
    )
    assert row.finished_at is None, (
        "a job re-pended for another run must not still carry the previous run's finished_at"
    )


async def test_retry_job_refuses_a_running_job(backend_pair: Backend) -> None:
    """The complement: a job a worker is executing right now is not retryable.

    This exclusion is a correctness constraint rather than a policy choice:
    re-pending a row while an attempt is live races that attempt's terminal
    write, and the job could run twice concurrently. Widening the accepted
    source states must not widen this one.
    """
    job_id = await _enqueue(backend_pair)
    worker_id = await _worker_of(backend_pair)
    await _claim(backend_pair, job_id, worker_id)

    before = await backend_pair.get(job_id)
    assert before is not None
    assert before.status == "running", (
        f"the scenario requires a live attempt, got {before.status!r}"
    )

    retried = await backend_pair.retry_job(job_id)
    assert retried is False, (
        "a job with a live attempt was re-pended; the running attempt's "
        "terminal write now races the re-run, and the job can execute twice "
        "concurrently"
    )

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "running", f"the live attempt's row must be left alone, got {row.status!r}"


async def test_retry_job_still_re_runs_a_failed_job(backend_pair: Backend) -> None:
    """The already-supported source state keeps working.

    Widening the predicate must not disturb the states operators use today.
    """
    job_id = await _enqueue(backend_pair, max_attempts=1)
    worker_id = await _worker_of(backend_pair)
    attempt = await _claim(backend_pair, job_id, worker_id)
    await backend_pair.mark_failed_or_retry(job_id, worker_id, _ERROR, None, attempt=attempt)

    before = await backend_pair.get(job_id)
    assert before is not None
    assert before.status == "failed", (
        f"the scenario requires a terminally failed job, got {before.status!r}"
    )

    assert await backend_pair.retry_job(job_id) is True, (
        "re-running a failed job is the already-supported operator path and must keep working"
    )
    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status in ("pending", "scheduled")
    assert row.error_class is None, (
        "the previous run's error must be cleared when the job is re-pended, "
        "so the row does not carry a failure that no longer describes it"
    )
