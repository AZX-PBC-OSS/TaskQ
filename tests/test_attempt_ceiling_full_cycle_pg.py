"""The full life of a job whose attempt counter sits at the smallint ceiling.

Dispatch's claim stamps ``attempt = LEAST(attempt + 1, 32767)`` so one row
parked at the column ceiling cannot turn a whole claim round into a
smallint-out-of-range driver error (the backlog-drain pin lives in
tests/test_dispatch_pg.py::
test_backlog_still_dispatches_with_a_job_at_the_attempt_ceiling). What that
pin does not cover is what happens to the clamped job itself AFTER the claim
- the chain this file exercises end to end:

* the clamped row is claimed at attempt 32767 (not 32768, no driver error)
  and runs;
* its terminal write lands through the ordinary budget arms - a transient
  row at the ceiling is already past its ``max_attempts`` and terminalises
  ``failed``; an ``indefinite`` row retries on - and the repeated attempt
  number makes every terminal path's ``job_attempts`` insert land on the
  same ``(job_id, attempt)`` key twice, where the ``ON CONFLICT DO NOTHING``
  guard must keep the first record without rolling the transition back;
* an operator re-run of the terminalised row is REFUSED at the ceiling:
  ``retry_job``'s raised-ceiling gate
  (``LEAST(GREATEST(max_attempts, attempt + 1), 32767) > attempt``) cannot
  open, so the row stays terminal instead of re-pending a row whose next
  claim would need a number the column cannot hold.

The indefinite double cycle is the shape that earns the pin: claim, fail,
reschedule, claim again at the same clamped number, fail again - the second
cycle's attempt-row insert collides with the first's, and the row must still
reschedule (never stranded, never a driver error, exactly one attempt row
kept: the first cycle's record).
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this test's own fixture-validated schema identifier; every value is $-bound.

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import ErrorInfo, JobId
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type _PGConn = asyncpg.Connection[asyncpg.Record] | PoolConnectionProxy[asyncpg.Record]
else:
    type _PGConn = object  # pyright: ignore[reportInvalidTypeForm] # Why: asyncpg classes are not subscriptable at runtime

pytestmark = pytest.mark.integration

#: The domain ceiling of the ``jobs.attempt`` smallint column
#: (``constants.SMALLINT_MAX``). A row parked here has no headroom for the
#: claim's ``attempt + 1``, so the claim's LEAST clamp engages.
_CEILING = 32767
_LEASE = timedelta(seconds=30)


async def _park_at_ceiling(
    conn: _PGConn,
    schema: str,
    actor: str,
    *,
    retry_kind: str,
) -> UUID:
    """Seed one claimable pending job whose ``attempt`` is already 32767.

    ``retry_kind='indefinite'`` is the kind that reaches this state in
    production: its budget is the ``schedule_to_close`` deadline, not
    ``max_attempts``, so dispatch keeps incrementing the counter for as long
    as the job keeps failing. The transient variant is the same row one
    terminal write earlier in its life.
    """
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, scheduled_at, attempt, "
        " max_attempts, retry_kind) "
        "VALUES ($1, $2, 'default', '{}'::jsonb, 'pending', "
        "        clock_timestamp() - interval '5 minutes', $3, 3, $4)",
        job_id,
        actor,
        _CEILING,
        retry_kind,
    )
    return job_id


async def _register_actor_and_worker(
    conn: _PGConn, schema: str, actor: str, worker_id: UUID
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
        "ON CONFLICT (actor) DO NOTHING",
        actor,
        "default",
    )
    await create_worker(conn, schema, worker_id)


async def test_clamped_transient_job_terminalises_failed_through_the_budget_arm(
    jobs_app: JobsApp,
) -> None:
    """A transient job claimed at the ceiling terminalises ``failed`` on its
    first failure after the clamp - the RetryAfter(consume_budget=True)
    budget arm, which sees ``attempt >= max_attempts`` and owns the exit -
    keeping exactly one attempt row, and refusing the operator re-run whose
    re-pend the next claim could not number.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    actor = f"ceiling_tx_{new_base62(6)}".lower()
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await _register_actor_and_worker(conn, schema, actor, worker_id)
        job_id = await _park_at_ceiling(conn, schema, actor, retry_kind="transient")

    claimed = {
        row.id: row
        for row in await backend.dispatch_batch(
            worker_id=worker_id, queues=["default"], limit=10, lock_lease=_LEASE
        )
    }
    assert job_id in claimed, "the clamped row must still be claimable"
    assert claimed[job_id].attempt == _CEILING, (
        f"the claim must clamp at the column ceiling, not overflow it; "
        f"stamped attempt={claimed[job_id].attempt}"
    )

    outcome = await backend.mark_retry_after(
        JobId(job_id),
        worker_id,
        timedelta(seconds=30),
        consume_budget=True,
        attempt=_CEILING,
        claim_epoch=1,
    )
    assert outcome == "failed:MaxAttemptsExceeded", (
        "a transient row at the ceiling is already past its max_attempts, so "
        "the RetryAfter budget arm must terminalise it, not reschedule it; "
        f"got {outcome!r}"
    )

    row = await backend.get(JobId(job_id))
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "MaxAttemptsExceeded"
    assert row.attempt == _CEILING

    attempts = await backend.get_attempts(JobId(job_id))
    assert len(attempts) == 1, (
        f"exactly one attempt row may exist at the clamped number; found "
        f"{[(a.attempt, a.outcome) for a in attempts]}"
    )
    assert attempts[0].attempt == _CEILING
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_class == "MaxAttemptsExceeded"

    events = await backend.get_events(JobId(job_id))
    failed_events = [
        e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "failed"
    ]
    assert len(failed_events) == 1
    assert failed_events[0].detail.get("error_class") == "MaxAttemptsExceeded"

    retried = await backend.retry_job(JobId(job_id))
    assert retried is False, (
        "the operator re-run must be refused at the ceiling: the raised "
        "ceiling cannot exceed the spent attempt, so the gate stays shut "
        "rather than re-pending a row whose next claim cannot be numbered"
    )
    row_after = await backend.get(JobId(job_id))
    assert row_after is not None
    assert row_after.status == "failed"
    assert row_after.max_attempts == 3, "a refused retry leaves the ceiling untouched"


async def test_clamped_indefinite_job_survives_two_full_claim_fail_cycles(
    jobs_app: JobsApp,
) -> None:
    """An ``indefinite`` job clamped at the ceiling runs, fails, and
    reschedules TWICE: both cycles claim at attempt 32767, both terminal
    writes land, the second cycle's attempt-row insert collides with the
    first's and is absorbed - the audit trail keeps the FIRST record of the
    number, and the row is still scheduled (never stranded, never terminal,
    no driver error from any statement in either cycle).
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    actor = f"ceiling_inf_{new_base62(6)}".lower()
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await _register_actor_and_worker(conn, schema, actor, worker_id)
        job_id = JobId(await _park_at_ceiling(conn, schema, actor, retry_kind="indefinite"))

    for cycle_message in ("cycle one boom", "cycle two boom"):
        claimed = {
            row.id: row
            for row in await backend.dispatch_batch(
                worker_id=worker_id, queues=["default"], limit=10, lock_lease=_LEASE
            )
        }
        assert job_id in claimed, (
            f"the clamped row must be re-claimable on every cycle; missing "
            f"on the cycle failing with {cycle_message!r}"
        )
        assert claimed[job_id].attempt == _CEILING

        retried_row = await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            ErrorInfo(
                error_class="ValueError",
                error_message=cycle_message,
                error_traceback=None,
            ),
            retry_delay=timedelta(seconds=5),
            attempt=_CEILING,
            claim_epoch=claimed[job_id].claim_epoch,
        )
        assert retried_row.status == "scheduled", (
            "an indefinite row with no schedule_to_close retries on its "
            f"deadline-free budget arm even at the ceiling; the cycle failing "
            f"with {cycle_message!r} landed {retried_row.status!r}"
        )

        # Make the rescheduled row due again without sleeping out the
        # delay, then promote it through the production scheduled→pending
        # sweep - the retry arm lands 'scheduled', and only the sweep's
        # promotion makes a due row claimable again.
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs '
                "SET scheduled_at = clock_timestamp() - interval '1 minute' "
                "WHERE id = $1",
                job_id,
            )
        promoted = await backend.scheduled_to_pending()
        assert promoted >= 1, (
            "the rescheduled clamped row must promote back to pending on the "
            "ordinary sweep once due"
        )

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "pending", (
        f"after two full cycles the clamped row must be back in the "
        f"claimable pool (each cycle above re-scheduled and re-promoted it), "
        f"not stranded or terminal; got {row.status!r}"
    )
    assert row.scheduled_at <= datetime.now(UTC), (
        "the promoted row is due - a third claim round would pick it up"
    )
    assert row.attempt == _CEILING

    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 1, (
        "two cycles at the clamped number must keep exactly one attempt row - "
        "the ON CONFLICT guard absorbs the repeat rather than raising the PK "
        f"collision back through the terminal write; found "
        f"{[(a.attempt, a.outcome, a.error_message) for a in attempts]}"
    )
    assert attempts[0].attempt == _CEILING
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_message == "cycle one boom", (
        "the audit trail keeps the FIRST record of the repeated attempt "
        "number; the second cycle's insert must not overwrite it"
    )

    events = await backend.get_events(job_id)
    reschedules = [
        e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "scheduled"
    ]
    assert len(reschedules) == 2, (
        "each cycle's retry transition is its own event (attempt rows are "
        "deduplicated at the repeated key; transitions are not - both "
        f"happened); found {len(reschedules)}"
    )
