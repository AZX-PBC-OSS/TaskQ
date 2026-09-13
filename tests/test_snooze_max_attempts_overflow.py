"""Red-team pins for the unbounded ``jobs.max_attempts`` snooze increment.

``jobs.max_attempts`` is ``smallint`` (migrations/01.00.00_01_pre_initial.sql
:82).  Both snooze arms widen the budget without any ceiling check —
``mark_snoozed`` (_sql_templates.py:487) and the
``RetryAfter(consume_budget=False)`` arm ``mark_retry_after_consume_false``
(:713) each do ``max_attempts = j.max_attempts + 1``.  A job that reaches
32767 therefore cannot be snoozed at all: PG raises ``22003 smallint out of
range`` and the whole statement aborts, so a reservation/rate-limit denial or
a server-provided ``Retry-After`` turns into an unhandled ``DataError`` out
of the consumer loop instead of a reschedule.

Nothing upstream prevents a job from being enqueued at that ceiling:
``RetryPolicy.max_attempts`` is typed ``int`` and validated only ``>= 1``
(retry.py:63-68), unlike ``priority`` — the other smallint the client accepts
— which IS range-guarded in two places (client/_args.py:277, actor.py:605).

These tests assert the DESIRABLE behaviour, so they go green once either the
enqueue-time guard or a saturating/guarded SQL increment lands.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
from pydantic import ValidationError

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.retry import RetryPolicy
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_SMALLINT_MAX = 32767

_DELAY = timedelta(seconds=30)


# ── Enqueue-time validation: mirror the priority smallint guard ──────────


def test_retry_policy_rejects_out_of_smallint_range_max_attempts() -> None:
    """``RetryPolicy(max_attempts=32768)`` must be rejected the way an
    out-of-range ``priority`` is.

    ``priority`` is guarded at both entry points with an explicit
    "must fit smallint range" ValueError; ``max_attempts`` lands in the same
    smallint column and has no ceiling at all.  Without this guard an
    operator can persist a value that makes every snooze of that job a hard
    PG error.
    """
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        RetryPolicy(max_attempts=_SMALLINT_MAX + 1)

    assert "smallint" in str(exc_info.value).lower(), (
        "RetryPolicy accepted max_attempts=32768, which does not fit the "
        "smallint jobs.max_attempts column. priority (the other smallint the "
        "client accepts) raises 'must fit smallint range (-32768..32767)' in "
        "client/_args.py:277 and actor.py:605; max_attempts is validated only "
        "'>= 1' (retry.py:63-68). The snooze arms then do "
        "'max_attempts = j.max_attempts + 1' with no ceiling, so such a job "
        "can never be snoozed."
    )


def test_retry_policy_rejects_max_attempts_that_cannot_absorb_one_snooze() -> None:
    """A job enqueued at exactly 32767 is already un-snoozable.

    The snooze arms add 1 unconditionally, so ``max_attempts == 32767`` is
    the first value at which ``mark_snoozed`` / ``mark_retry_after(
    consume_budget=False)`` raise ``22003 smallint out of range``. Either the
    policy must refuse the value or the SQL must saturate; today neither
    happens.
    """
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        RetryPolicy(max_attempts=_SMALLINT_MAX)

    assert "smallint" in str(exc_info.value).lower(), (
        "RetryPolicy accepted max_attempts=32767. Every snooze of such a job "
        "executes 'max_attempts = j.max_attempts + 1' against a smallint "
        "column and aborts with PG 22003, so a reservation denial, a rate "
        "limit denial, or a RetryAfter(consume_budget=False) becomes an "
        "unhandled DataError instead of a reschedule."
    )


# ── Both SQL increment sites against real Postgres ──────────────────────


async def _seed(app: JobsApp, *, max_attempts: int) -> tuple[str, JobId, UUID]:
    """Insert a running job owned by a fresh worker at *max_attempts*."""
    deps = app.deps
    schema: str = deps.settings.schema_name
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            max_attempts=max_attempts,
            retry_kind="transient",
            lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
    return schema, JobId(job_id), worker_id


async def _read_row(app: JobsApp, schema: str, job_id: JobId) -> asyncpg.Record:
    async with app.deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, max_attempts FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier from module_pg_schema; the job id goes through $1 parameter binding.
            job_id,
        )
    assert row is not None
    return row


async def test_mark_snoozed_at_smallint_ceiling_does_not_overflow(
    clean_jobs_app: JobsApp,
) -> None:
    """Increment site 1 — ``mark_snoozed`` (reservation / rate-limit denial).

    A reservation-denied snooze on a job already at ``max_attempts = 32767``
    must reschedule the job (saturating the budget rather than overflowing
    it), not abort the statement.
    """
    schema, job_id, worker_id = await _seed(clean_jobs_app, max_attempts=_SMALLINT_MAX)

    try:
        outcome = await clean_jobs_app.backend.mark_snoozed(
            job_id,
            worker_id,
            _DELAY,
            outcome="reservation_denied",
        )
    except asyncpg.DataError as exc:  # pragma: no cover - the defect path
        pytest.fail(
            "mark_snoozed raised a raw PG error on a job at the smallint "
            f"ceiling instead of rescheduling it: {exc!r}. "
            "_sql_templates.py:487 does 'max_attempts = j.max_attempts + 1' "
            "against the smallint jobs.max_attempts column with no ceiling "
            "guard, so PG 22003 aborts the whole statement and the "
            "reservation/rate-limit denial escapes the consumer loop as an "
            "unhandled DataError. The job stays 'running' with its lock held "
            "until the lock lease expires."
        )

    assert outcome == "scheduled", (
        f"mark_snoozed returned {outcome!r} for a job at max_attempts=32767; "
        "the snooze must still reschedule the job."
    )

    row = await _read_row(clean_jobs_app, schema, job_id)
    assert row["max_attempts"] <= _SMALLINT_MAX, (
        f"max_attempts overflowed to {row['max_attempts']}; it must saturate "
        "at the smallint ceiling."
    )
    assert row["status"] == "scheduled", (
        f"job left in status {row['status']!r} instead of 'scheduled' — the "
        "snooze did not take effect."
    )


async def test_mark_retry_after_consume_false_at_ceiling_does_not_overflow(
    clean_jobs_app: JobsApp,
) -> None:
    """Increment site 2 — ``RetryAfter(consume_budget=False)``.

    ``mark_retry_after(consume_budget=False)`` routes to
    ``mark_retry_after_consume_false`` (_sql_templates.py:713), which carries
    the same unguarded ``max_attempts = j.max_attempts + 1``. An actor that
    honours a server-provided ``Retry-After`` on a job at the ceiling must
    reschedule, not blow up.
    """
    schema, job_id, worker_id = await _seed(clean_jobs_app, max_attempts=_SMALLINT_MAX)

    try:
        outcome = await clean_jobs_app.backend.mark_retry_after(
            job_id,
            worker_id,
            _DELAY,
            consume_budget=False,
        )
    except asyncpg.DataError as exc:  # pragma: no cover - the defect path
        pytest.fail(
            "mark_retry_after(consume_budget=False) raised a raw PG error on "
            f"a job at the smallint ceiling: {exc!r}. "
            "_sql_templates.py:713 does 'max_attempts = j.max_attempts + 1' "
            "against the smallint jobs.max_attempts column with no ceiling "
            "guard, so PG 22003 aborts the statement and the RetryAfter "
            "escapes the consumer loop as an unhandled DataError."
        )

    assert outcome == "scheduled", (
        f"mark_retry_after returned {outcome!r} for a job at "
        "max_attempts=32767; the non-consuming retry must still reschedule."
    )

    row = await _read_row(clean_jobs_app, schema, job_id)
    assert row["max_attempts"] <= _SMALLINT_MAX, (
        f"max_attempts overflowed to {row['max_attempts']}; it must saturate "
        "at the smallint ceiling."
    )
    assert row["status"] == "scheduled", (
        f"job left in status {row['status']!r} instead of 'scheduled' — the "
        "non-consuming retry did not take effect."
    )


async def test_snooze_below_ceiling_still_widens_budget(
    clean_jobs_app: JobsApp,
) -> None:
    """Control: well below the ceiling, the snooze increment works.

    Pins that the two failures above are about the smallint boundary, not
    about the fixture or the snooze contract itself.
    """
    schema, job_id, worker_id = await _seed(clean_jobs_app, max_attempts=3)

    outcome = await clean_jobs_app.backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
    )
    assert outcome == "scheduled"

    row = await _read_row(clean_jobs_app, schema, job_id)
    assert row["max_attempts"] == 4
    assert row["status"] == "scheduled"
