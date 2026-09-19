"""The ``mark_snoozed`` boundary: outcome admission and the deferral
delay floor, enforced identically on both backends.

Two seams of the snooze statement were verified live to disagree with
their declared contract:

* **Outcome admission.** The ``outcome`` parameter was typed
  ``AttemptOutcome`` - eight values - while the SQL arms key on exactly
  three. With ``outcome='succeeded'`` on a running job, PG fired NO arm
  (the row stayed ``running``, stranded until the lease sweep, and the
  call returned ``"noop"``) while the in-memory twin silently
  rescheduled the job with no counter increment: an uncounted snooze.
  The twin certifying code that would corrupt on PG is exactly the
  false-confidence failure the in-memory backend exists to prevent.

  The fix narrows the parameter to :data:`SnoozeOutcome` (unrepresentable
  at the type level) and adds a runtime guard at the Python boundary -
  PG cannot reject an unknown bind value inside the statement, so the
  boundary owns the check and the twin carries the identical guard.
  Validation precedes the ownership fence on both backends: an illegal
  outcome raises ``ValueError`` naming the legal set whatever the job's
  state, and the row is left untouched.

* **Delay floor.** A zero-delay non-consuming deferral (``Snooze(0)``,
  ``RetryAfter(0, consume_budget=False)``, a denial with
  ``retry_after=0``) used to land the job ``pending`` at
  ``clock_timestamp()`` - first in every dispatch round, instantly
  re-claimable: a claim/refund hot loop monopolising a worker slot. The
  effective delay is now floored at
  :data:`taskq.constants.MIN_DEFERRAL_INTERVAL` in both non-consuming
  arms. The twin's floor is pinned in ``tests/test_denial_observability.py``;
  the pin here is the PG side of the same contract.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import JobId
from taskq.constants import MIN_DEFERRAL_INTERVAL
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)

#: The five ``AttemptOutcome`` execution outcomes - every value the
#: snooze arms do NOT key on.
_ILLEGAL_OUTCOMES = ("succeeded", "failed", "cancelled", "crashed", "scheduled")

#: The legal set, mirrored from the alias for the message assertion.
_LEGAL_OUTCOMES = ("snoozed", "reservation_denied", "rate_limit_denied")


async def _running_job(backend: Backend) -> tuple[JobId, UUID]:
    """A running, worker-owned job on whichever backend the pair yields."""
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={},
            max_attempts=10,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    worker_id = new_uuid()
    dispatched = await backend.dispatch_batch(worker_id, ["default"], 1, _LOCK_LEASE)
    assert [row.id for row in dispatched] == [job_id]
    return job_id, worker_id


# ── the outcome guard: loud, identical, fence-first ──────────────────────


@pytest.mark.parametrize("outcome", _ILLEGAL_OUTCOMES)
async def test_mark_snoozed_rejects_execution_outcome_loudly(
    backend_pair: Backend, outcome: str
) -> None:
    """An execution outcome raises ``ValueError`` naming the legal set on
    BOTH backends - before the ownership fence, leaving the row
    untouched.

    Pre-fix this was the silent disagreement: PG returned ``"noop"`` and
    stranded the running job; the twin rescheduled it uncounted.
    """
    job_id, worker_id = await _running_job(backend_pair)

    with pytest.raises(ValueError) as exc_info:
        await backend_pair.mark_snoozed(
            job_id,
            worker_id,
            timedelta(seconds=30),
            outcome=outcome,  # type: ignore[arg-type] # Why: deliberately outside the static contract - the runtime guard under test is what catches it; production callers cannot reach this line under pyright.
        )

    message = str(exc_info.value)
    for legal in _LEGAL_OUTCOMES:
        assert legal in message, (
            f"the rejection must name the legal set; {legal!r} missing from {message!r}"
        )
    assert outcome in message, f"the rejection must name the rejected input: {message!r}"

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "running", "the guard must fire before any state is touched"
    assert row.locked_by_worker == worker_id


# ── the delay floor: the PG side of the twin's pin ──────────────────────


async def test_pg_zero_delay_snooze_is_floored_at_min_deferral_interval(
    clean_jobs_app: JobsApp,
) -> None:
    """A zero-delay ``Snooze`` against real PG reschedules at least
    ``MIN_DEFERRAL_INTERVAL`` out as ``scheduled`` - never ``pending``
    at the head of the dispatch order. The bounds are read from PG's own
    clock (the write's arbiter), so app↔DB skew cannot false-pass the
    pin.
    """
    app = clean_jobs_app
    deps = app.deps
    schema: str = deps.settings.schema_name
    worker_id = new_uuid()
    job_id = new_job_id()
    async with deps.worker_pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType] # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await create_worker(conn, schema, worker_id)
        await create_running_job(
            conn,
            schema,
            worker_id,
            job_id=job_id,
            max_attempts=10,
            retry_kind="transient",
            lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            with_events=False,
        )
        before: datetime = await conn.fetchval("SELECT clock_timestamp()")

    result = await app.backend.mark_snoozed(JobId(job_id), worker_id, timedelta(0), attempt=1)
    assert result == "scheduled"

    async with deps.worker_pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType] # Why: same
        rec = await conn.fetchrow(
            f'SELECT status, scheduled_at FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; the job id is $1-bound
            job_id,
        )
    assert rec is not None
    assert rec["status"] == "scheduled"
    assert rec["scheduled_at"] >= before + MIN_DEFERRAL_INTERVAL, (
        f"a zero-delay deferral must reschedule at least {MIN_DEFERRAL_INTERVAL} "
        f"out; got scheduled_at={rec['scheduled_at']!r} against a write window "
        f"starting at {before!r}"
    )
