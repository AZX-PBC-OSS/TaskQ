"""A ``unique_for`` window suppresses a duplicate whose first job succeeded.

``unique_for`` is a time window: "there is at most one job for this identity in
this period". An operator writing ``@actor(unique_for=timedelta(hours=1))`` on a
daily-report or process-this-webhook actor is asking for the work to happen once
in the hour, and is relying on that to make a re-delivered webhook, a retried
API call, or a double-clicked button harmless.

The default ``unique_states`` is ``("pending", "scheduled", "running",
"succeeded")``, so the window keeps covering a job after it succeeds: the first
job completing is the state that says the work already happened - the precise
condition the window exists to detect. Before this default, the identity was
freed the instant the first job succeeded, and the faster the work completed,
the wider the unguarded remainder of the operator's window.

The failure states stay out of the default set, for the mirror-image reason:
``failed``, ``cancelled``, ``crashed``, ``abandoned`` mean the work did *not*
happen, so matching them would let one transient failure suppress every later
attempt for the rest of the window.

Both halves are pinned here so the guarantee cannot be widened or narrowed by
accident: a duplicate inside the window dedups onto the succeeded job, and a
failed job leaves the identity free.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import ErrorInfo, IdentityKey, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
#: Wide enough that nothing in a test run approaches its edge, so a second
#: enqueue is unambiguously inside the window the operator configured.
_WINDOW = timedelta(hours=1)
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


def _args(identity: str) -> EnqueueArgs:
    """An enqueue carrying the shipped ``unique_for`` defaults.

    ``unique_states`` is deliberately left unset: the defaults are what this
    file is about, and spelling them here would pin the test's own choice
    instead of the one operators get.
    """
    return EnqueueArgs(
        id=new_job_id(),
        actor="actor_a",
        queue="default",
        payload={"k": "v"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        identity_key=IdentityKey(identity),
        unique_for=_WINDOW,
    )


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


async def test_second_enqueue_inside_the_window_dedups_onto_a_succeeded_job(
    backend_pair: Backend,
) -> None:
    """A duplicate arriving after the first job succeeded is suppressed.

    This is the whole point of the window: the work happened, the window is
    still open, so the second request must not produce a second execution. The
    caller gets the existing job back, as it would for any other duplicate.
    """
    first = _args("daily-report:2025-01-01")
    first_row = await backend_pair.enqueue(first)

    worker_id = await _worker_of(backend_pair)
    attempt = await _claim(backend_pair, first.id, worker_id)
    await backend_pair.mark_succeeded(first.id, worker_id, {"sent": True}, attempt=attempt)

    done = await backend_pair.get(first.id)
    assert done is not None
    assert done.status == "succeeded", (
        f"the scenario requires the first job to have genuinely completed, got {done.status!r}"
    )

    second = _args("daily-report:2025-01-01")
    second_row = await backend_pair.enqueue(second)

    assert second_row.id == first_row.id, (
        "a second enqueue for the same identity, inside a still-open "
        f"unique_for window of {_WINDOW}, created a new job "
        f"({second_row.id}) instead of deduplicating onto the completed one "
        f"({first_row.id}). The work will run a second time - the report is "
        "sent twice, the webhook processed twice - even though the caller "
        "asked for it to happen once in the window. The window only covers "
        "unfinished jobs today, so it stops protecting the identity the "
        "instant the work succeeds, which is exactly when it has happened"
    )

    row = await backend_pair.get(second_row.id)
    assert row is not None
    assert row.status == "succeeded", (
        f"the deduplicated enqueue must hand back the completed job itself, "
        f"got a row in status {row.status!r}"
    )


async def test_a_failed_job_does_not_block_the_identity_for_the_window(
    backend_pair: Backend,
) -> None:
    """The complement: failure must leave the identity free.

    A job that failed did not do the work, so suppressing the next request for
    the rest of the window would turn one transient failure into an hour of
    silently dropped work. Covering ``succeeded`` must not be implemented by
    covering every terminal state.
    """
    first = _args("webhook:evt-1")
    first_row = await backend_pair.enqueue(first)

    worker_id = await _worker_of(backend_pair)
    attempt = await _claim(backend_pair, first.id, worker_id)
    await backend_pair.mark_failed_or_retry(first.id, worker_id, _ERROR, None, attempt=attempt)

    failed = await backend_pair.get(first.id)
    assert failed is not None
    assert failed.status == "failed", (
        f"the scenario requires a terminally failed first job, got {failed.status!r}"
    )

    second = _args("webhook:evt-1")
    second_row = await backend_pair.enqueue(second)

    assert second_row.id != first_row.id, (
        "a fresh enqueue for an identity whose only job FAILED was "
        "deduplicated onto that failure. The work never happened, so the "
        f"caller's request has been silently dropped for the rest of the "
        f"{_WINDOW} window - a single transient failure suppressing an hour of "
        "real work"
    )
    row = await backend_pair.get(second_row.id)
    assert row is not None
    assert row.status in ("pending", "scheduled"), (
        f"the new job must be claimable, got {row.status!r}"
    )


async def test_second_enqueue_dedups_onto_a_still_running_job(
    backend_pair: Backend,
) -> None:
    """The already-working half of the window keeps working.

    An identity with a job in flight is the case the current default does
    cover, and widening the window must not disturb it.
    """
    first = _args("webhook:evt-2")
    first_row = await backend_pair.enqueue(first)

    worker_id = await _worker_of(backend_pair)
    await _claim(backend_pair, first.id, worker_id)

    running = await backend_pair.get(first.id)
    assert running is not None
    assert running.status == "running", (
        f"the scenario requires an in-flight first job, got {running.status!r}"
    )

    second_row = await backend_pair.enqueue(_args("webhook:evt-2"))
    assert second_row.id == first_row.id, (
        "a duplicate arriving while the first job is still running must "
        "deduplicate onto it rather than starting a concurrent second run"
    )
