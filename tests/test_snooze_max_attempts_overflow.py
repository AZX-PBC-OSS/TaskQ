"""Pins for the ``jobs.max_attempts`` smallint domain and the fixed
snooze ceiling.

``jobs.max_attempts`` is ``smallint`` (migrations/01.00.00_01_pre_initial.sql
:82).  ``RetryPolicy.max_attempts`` is typed ``int``, so without an
explicit guard an operator can persist a value the column cannot hold —
unlike ``priority``, the other smallint the client accepts, which IS
range-guarded in two places (client/_args.py:277, actor.py:605).

The enqueue-time guard pins live at the top.  The snooze-arm pins below
them pin the FIXED-CEILING contract: a snooze/denial at any
``max_attempts <= 32767`` reschedules the job and leaves ``max_attempts``
exactly where it was (the ceiling is a bound, not a counter — nothing in
the non-consuming paths raises it, and the deferral is counted on the
row's snooze/denial counters instead).  A job already at the ceiling
still reschedules: with no increment at all there is no overflow to
guard against, so the crash-free property holds trivially.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
from pydantic import ValidationError

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.retry import RetryPolicy
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_SMALLINT_MAX = 32767

_DELAY = timedelta(seconds=30)

# The in-memory mirror's clock: any instant strictly after the enqueued
# scheduled_at makes the job dispatch-eligible without a clock dance.
_MEM_NOW = datetime(2026, 1, 1, tzinfo=UTC)


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
        "client/_args.py:277 and actor.py:605; max_attempts must get the "
        "same boundary guard."
    )


def test_enqueue_args_rejects_out_of_smallint_range_max_attempts() -> None:
    """``EnqueueArgs`` itself must refuse a ``max_attempts`` that cannot
    fit the ``jobs.max_attempts smallint NOT NULL`` column
    (migrations/01.00.00_01_pre_initial.sql:82).

    ``RetryPolicy`` guards the client-facing construction path, but
    ``EnqueueArgs.__post_init__`` (backend/_protocol.py) is the actual
    boundary every enqueue path funnels through — including callers that
    build ``EnqueueArgs`` directly from a raw DB column instead of through
    ``RetryPolicy`` (e.g. ``cron_loop.py`` reading ``actor_config.max_attempts``,
    ``web/admin/ops.py``). Today ``__post_init__`` only checks the
    schedule_to_close mutual-exclusion and NUL-byte text fields; it has no
    max_attempts bound at all, so this out-of-range construction succeeds
    silently instead of raising — a defect this test pins as failing until
    fixed.
    """
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        EnqueueArgs(
            id=new_job_id(),
            actor="foo",
            queue="default",
            payload={},
            max_attempts=_SMALLINT_MAX + 1,
            retry_kind="fixed",
            scheduled_at=None,
        )

    assert "smallint" in str(exc_info.value).lower(), (
        "EnqueueArgs accepted max_attempts=32768, which does not fit the "
        "smallint jobs.max_attempts column. Any raw EnqueueArgs construction "
        "(direct backend use, cron re-enqueue of an actor_config row, "
        "web/admin/ops.py) bypasses RetryPolicy's guard entirely and must be "
        "bounded at the protocol layer itself."
    )


def test_enqueue_args_rejects_negative_max_attempts() -> None:
    """``EnqueueArgs(max_attempts=-5, ...)`` must also be rejected.

    A negative max_attempts is nonsensical domain-wise (a job cannot have
    fewer than zero attempts) even though it technically fits inside the
    smallint's signed range; it should never reach the jobs table.
    """
    with pytest.raises((ValueError, ValidationError)):
        EnqueueArgs(
            id=new_job_id(),
            actor="foo",
            queue="default",
            payload={},
            max_attempts=-5,
            retry_kind="fixed",
            scheduled_at=None,
        )


def test_enqueue_args_rejects_non_integer_max_attempts() -> None:
    """``EnqueueArgs(max_attempts=3.5, ...)`` must be rejected, not stored.

    ``check_max_attempts_domain`` (constants.py) only compares ``<`` and
    ``>`` against the bound; it never checks ``isinstance(value, int)``.
    A ``bool`` or ``float`` value compares fine against the smallint
    bound and sails through silently, so ``EnqueueArgs`` accepts a
    fractional attempt count. ``RetryPolicy`` — the client-facing
    construction path — already rejects the same value via pydantic's
    strict int coercion (``RetryPolicy(max_attempts=3.5)`` raises
    ``ValidationError``), so this is a parity gap between the two
    layers issue #164 was meant to close: ``EnqueueArgs`` is supposed to
    be the one common boundary every enqueue path funnels through, and
    it is currently laxer than the policy layer that feeds it.

    "3.5 attempts" is nonsensical domain-wise the same way a negative
    count is; Postgres will coerce or reject it in a way the in-memory
    twin (which stores whatever Python object it is handed) will not
    replicate, breaking backend parity for any downstream equality or
    arithmetic against ``max_attempts``.
    """
    with pytest.raises((ValueError, ValidationError, TypeError)) as exc_info:
        EnqueueArgs(
            id=new_job_id(),
            actor="foo",
            queue="default",
            payload={},
            max_attempts=3.5,  # type: ignore[arg-type]
            retry_kind="fixed",
            scheduled_at=None,
        )

    assert not isinstance(exc_info.value, TypeError), (
        "EnqueueArgs accepted max_attempts=3.5 outright (no exception at all "
        "if this assertion is reached, the pytest.raises above would already "
        "have failed) — a fractional attempt count must be refused with a "
        "typed ValueError identifying the bad field, not silently stored."
    )


def test_enqueue_args_rejects_none_max_attempts_with_typed_error() -> None:
    """``EnqueueArgs(max_attempts=None, ...)`` must raise a typed refusal,
    not an untyped ``TypeError`` from the comparison inside the guard.

    ``check_max_attempts_domain`` does ``if value < 1`` with no type
    check first; handed ``None`` this raises
    ``TypeError: '<' not supported between instances of 'NoneType' and
    'int'`` — an implementation-detail exception a caller has no reason
    to catch, not the "max_attempts must be >= 1" ``ValueError`` every
    other bad value gets. A bare ``TypeError`` escaping the enqueue
    boundary instead of a typed domain refusal is exactly the class of
    failure issue #164 was about: an untyped exception a caller cannot
    usefully handle, escaping in place of a deliberate refusal.
    """
    with pytest.raises(ValueError) as exc_info:
        EnqueueArgs(
            id=new_job_id(),
            actor="foo",
            queue="default",
            payload={},
            max_attempts=None,  # type: ignore[arg-type]
            retry_kind="fixed",
            scheduled_at=None,
        )

    assert not isinstance(exc_info.value, TypeError), (
        "EnqueueArgs(max_attempts=None) raised a bare TypeError from the "
        "unguarded '<' comparison inside check_max_attempts_domain instead "
        "of a typed ValueError naming max_attempts as the bad field."
    )


def test_retry_policy_rejects_max_attempts_that_cannot_absorb_one_snooze() -> None:
    """A job enqueued at exactly 32767 has no defensive headroom left.

    The policy refuses the top-of-domain value the way it refuses values
    past the column entirely: a row parked at exactly the ceiling leaves
    no margin for any future statement that adds one to a
    max_attempts-derived value (see MAX_ENQUEUABLE_MAX_ATTEMPTS).
    """
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        RetryPolicy(max_attempts=_SMALLINT_MAX)

    assert "smallint" in str(exc_info.value).lower(), (
        "RetryPolicy accepted max_attempts=32767. A row at the exact "
        "smallint ceiling has no headroom for any future +1 against a "
        "max_attempts-derived value; the policy guard must refuse the "
        "top-of-domain value, keeping one of margin."
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
    """Increment-free arm 1 — ``mark_snoozed`` (reservation / rate-limit
    denial).

    A reservation-denied snooze on a job already at ``max_attempts = 32767``
    must reschedule the job and leave the ceiling exactly where it was —
    there is no increment to overflow, and the ceiling is a bound, not a
    counter.
    """
    schema, job_id, worker_id = await _seed(clean_jobs_app, max_attempts=_SMALLINT_MAX)

    try:
        outcome = await clean_jobs_app.backend.mark_snoozed(
            job_id,
            worker_id,
            _DELAY,
            outcome="reservation_denied",
            attempt=1,
        )
    except asyncpg.DataError as exc:  # pragma: no cover - the defect path
        pytest.fail(
            "mark_snoozed raised a raw PG error on a job at the smallint "
            f"ceiling instead of rescheduling it: {exc!r}. "
            "The job must reschedule with max_attempts untouched."
        )

    assert outcome == "scheduled", (
        f"mark_snoozed returned {outcome!r} for a job at max_attempts=32767; "
        "the snooze must still reschedule the job."
    )

    row = await _read_row(clean_jobs_app, schema, job_id)
    assert row["max_attempts"] == _SMALLINT_MAX, (
        f"max_attempts moved to {row['max_attempts']}; the snooze arms must "
        "leave the configured ceiling untouched — it is a bound, not a "
        "counter."
    )
    assert row["status"] == "scheduled", (
        f"job left in status {row['status']!r} instead of 'scheduled' — the "
        "snooze did not take effect."
    )


async def test_mark_retry_after_consume_false_at_ceiling_does_not_overflow(
    clean_jobs_app: JobsApp,
) -> None:
    """Increment-free arm 2 — ``RetryAfter(consume_budget=False)``.

    ``mark_retry_after(consume_budget=False)`` routes to
    ``mark_retry_after_consume_false``, which carries the same fixed
    ceiling. An actor that honours a server-provided ``Retry-After`` on a
    job at the ceiling must reschedule with ``max_attempts`` untouched.
    """
    schema, job_id, worker_id = await _seed(clean_jobs_app, max_attempts=_SMALLINT_MAX)

    try:
        outcome = await clean_jobs_app.backend.mark_retry_after(
            job_id,
            worker_id,
            _DELAY,
            consume_budget=False,
            attempt=1,
        )
    except asyncpg.DataError as exc:  # pragma: no cover - the defect path
        pytest.fail(
            "mark_retry_after(consume_budget=False) raised a raw PG error on "
            f"a job at the smallint ceiling: {exc!r}. "
            "The job must reschedule with max_attempts untouched."
        )

    assert outcome == "scheduled", (
        f"mark_retry_after returned {outcome!r} for a job at "
        "max_attempts=32767; the non-consuming retry must still reschedule."
    )

    row = await _read_row(clean_jobs_app, schema, job_id)
    assert row["max_attempts"] == _SMALLINT_MAX, (
        f"max_attempts moved to {row['max_attempts']}; the non-consuming "
        "retry must leave the configured ceiling untouched."
    )
    assert row["status"] == "scheduled", (
        f"job left in status {row['status']!r} instead of 'scheduled' — the "
        "non-consuming retry did not take effect."
    )


async def test_snooze_below_ceiling_keeps_ceiling_fixed(
    clean_jobs_app: JobsApp,
) -> None:
    """Control: well below the ceiling, the snooze still reschedules.

    Pins that the ceiling tests above are about the smallint boundary and
    the reschedule contract, not about the fixture: the snooze lands
    ``scheduled`` and ``max_attempts`` stays at its configured value —
    the ceiling is a bound, not a counter, and the deferral is counted on
    the row's denial counter instead.
    """
    schema, job_id, worker_id = await _seed(clean_jobs_app, max_attempts=3)

    outcome = await clean_jobs_app.backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
        attempt=1,
    )
    assert outcome == "scheduled"

    row = await _read_row(clean_jobs_app, schema, job_id)
    assert row["max_attempts"] == 3
    assert row["status"] == "scheduled"


# ── In-memory mirror parity: both arms keep the ceiling fixed ──────────


async def _in_memory_running_job_at_ceiling(
    max_attempts: int,
) -> tuple[InMemoryBackend, JobId, UUID]:
    """Enqueue + dispatch one running job at *max_attempts* on the
    in-memory mirror — the same row shape ``_seed`` builds for PG.

    ``EnqueueArgs`` refuses the top-of-domain value (one of defensive
    headroom — see ``MAX_ENQUEUABLE_MAX_ATTEMPTS``), so the seed enqueues
    inside the enqueuable bound and then writes the ceiling onto the
    stored row directly, exactly as the PG side's ``create_running_job``
    bypasses the enqueue boundary with a direct INSERT.
    """
    from dataclasses import replace

    backend = InMemoryBackend(clock=FakeClock(_MEM_NOW))
    args = EnqueueArgs(
        id=new_job_id(),
        actor="mem_ceiling_actor",
        queue="default",
        payload={},
        max_attempts=min(max_attempts, 32766),
        retry_kind="transient",
        scheduled_at=_MEM_NOW - timedelta(seconds=1),
    )
    await backend.enqueue(args)
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="mem_ceiling_actor")
    worker_id = new_uuid()
    dispatched = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(dispatched) == 1
    if max_attempts > 32766:
        job_id = dispatched[0].id
        backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]  # Why: test-only private access — parks the stored row at the column ceiling the enqueue boundary now refuses, mirroring the PG side's direct-INSERT seed.
            backend._jobs[job_id],  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            max_attempts=max_attempts,
        )
    return backend, dispatched[0].id, worker_id


async def test_in_memory_mark_snoozed_at_ceiling_keeps_ceiling_fixed() -> None:
    """Mirror parity for arm 1: the in-memory ``mark_snoozed`` leaves the
    ceiling exactly where PG's arm does.

    Python ints do not overflow, so the mirror's failure mode would be
    quieter than PG's: an unbounded ``row.max_attempts + 1`` silently
    parks the row OUTSIDE the column domain (32768+), where every later
    comparison against real-PG behaviour disagrees. Parity doctrine: the
    mirror keeps the ceiling fixed exactly where production keeps it
    fixed.
    """
    backend, job_id, worker_id = await _in_memory_running_job_at_ceiling(_SMALLINT_MAX)

    outcome = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
        attempt=1,
    )

    assert outcome == "scheduled"
    row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access; the mirror's rows are the assertion surface.
    assert row.max_attempts == _SMALLINT_MAX, (
        f"the mirror moved max_attempts to {row.max_attempts}; it must stay "
        "at the configured ceiling, exactly as PG's arm does."
    )
    assert row.status == "scheduled"


async def test_in_memory_mark_retry_after_consume_false_at_ceiling_keeps_ceiling_fixed() -> None:
    """Mirror parity for arm 2: the in-memory
    ``mark_retry_after(consume_budget=False)`` keeps the ceiling fixed."""
    backend, job_id, worker_id = await _in_memory_running_job_at_ceiling(_SMALLINT_MAX)

    outcome = await backend.mark_retry_after(
        job_id,
        worker_id,
        _DELAY,
        consume_budget=False,
        attempt=1,
    )

    assert outcome == "scheduled"
    row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access; the mirror's rows are the assertion surface.
    assert row.max_attempts == _SMALLINT_MAX, (
        f"the mirror moved max_attempts to {row.max_attempts}; it must stay "
        "at the configured ceiling, exactly as PG's arm does."
    )
    assert row.status == "scheduled"


async def test_in_memory_snooze_below_ceiling_keeps_ceiling_fixed() -> None:
    """Mirror control, the twin of the PG control above: below the
    ceiling the snooze reschedules and leaves the ceiling at its
    configured value."""
    backend, job_id, worker_id = await _in_memory_running_job_at_ceiling(3)

    outcome = await backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
        attempt=1,
    )

    assert outcome == "scheduled"
    row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access; the mirror's rows are the assertion surface.
    assert row.max_attempts == 3
    assert row.status == "scheduled"
