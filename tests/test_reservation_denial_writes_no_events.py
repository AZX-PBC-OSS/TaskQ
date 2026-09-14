# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Red-team pins for the reservation-denial write trail.

A reservation/rate-limit denial is admission control, not an execution —
yet every denial currently mints durable rows as if the job had run and
failed:

* ``mark_snoozed`` (``backend/_sql_templates.py:468-560``, reached from
  ``worker/_handlers.py:516-540`` with ``outcome="reservation_denied"``)
  writes one ``job_attempts`` row and one ``job_events`` row per denial,
  raises ``max_attempts`` by 1 (``:487``), and re-nulls ``finished_at``
  (``:481``) so the terminality-keyed prune can never reclaim the job.
* The ``RetryAfter(consume_budget=False)`` arm (``:698-730``) is the same
  shape — the sibling path an actor honouring a server-provided
  ``Retry-After`` takes.

Because a denial also carries no terminal exit unless the job sets
``schedule_to_close``, a denied job loops dispatch → denial → snooze
forever, accruing two rows per cycle (measured in production at a
12.3:1 denial:success ratio — 6.5M ``job_events`` rows).

The settled contract these tests pin:

1. A denial must NOT persist per-occurrence rows — it is counted on the
   job row and emitted to OTEL; history belongs to collectors.
2. A denial must NOT raise ``max_attempts`` — the ceiling is a bound,
   not a counter.
3. A denial loop must be BOUNDED — a job denied forever must reach a
   terminal outcome within its retry budget, not snooze forever.

These tests assert the desirable behaviour, so they go green under the
fix; today they fail on the rows written, the raised ceiling, and the
loop that never terminates.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_DELAY = timedelta(seconds=30)
_LEASE = timedelta(seconds=30)

_TERMINAL_STATUSES = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


async def _seed_running(app: JobsApp, *, max_attempts: int = 3) -> tuple[str, JobId, UUID]:
    """A running job, locked by a fresh worker, with no pre-seeded events."""
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
            with_events=False,
        )
    return schema, JobId(job_id), worker_id


async def _trail_counts(app: JobsApp, schema: str, job_id: JobId) -> tuple[int, int, int, str]:
    """(job_events rows, job_attempts rows, max_attempts, status) for the job."""
    async with app.deps.worker_pool.acquire() as conn:
        events: int = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',
            job_id,
        )
        attempts: int = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        row = await conn.fetchrow(
            f'SELECT max_attempts, status FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None
    return events, attempts, row["max_attempts"], row["status"]


async def _counter_pair(app: JobsApp, schema: str, job_id: JobId) -> tuple[int, int]:
    """(snooze_count, rate_limit_blocked_count) on the job row."""
    async with app.deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT snooze_count, rate_limit_blocked_count FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None
    return row["snooze_count"], row["rate_limit_blocked_count"]


async def test_reservation_denial_writes_no_event_or_attempt_rows(
    clean_jobs_app: JobsApp,
) -> None:
    """A reservation denial is backpressure, not an execution: it must not
    mint a ``job_attempts``/``job_events`` row, and must not raise the
    ``max_attempts`` ceiling."""
    schema, job_id, worker_id = await _seed_running(clean_jobs_app, max_attempts=3)

    outcome = await clean_jobs_app.backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
    )
    assert outcome == "scheduled"

    events, attempts, max_attempts, status = await _trail_counts(clean_jobs_app, schema, job_id)
    assert events == 0, (
        f"a reservation denial wrote {events} job_events row(s). A denial is "
        "admission control, not a state transition anyone reads back: no "
        "machine consumer ever matches it (the reclaim outbox reads only "
        "kind='state_change' AND reason='lock_expired') and the only human "
        "reader is the admin job-detail page. The denial must be counted on "
        "the job row and emitted to OTEL, never persisted per occurrence — "
        "at the reporter's denial rate this trail is what grew job_events "
        "to 6.5M rows / 1.6GB."
    )
    assert attempts == 0, (
        f"a reservation denial wrote {attempts} job_attempts row(s). The "
        "attempts table records failed EXECUTIONS; a denied job never "
        "executed. The row only fits because dispatch had already bumped "
        "attempt — a conceptual error underneath the storage one."
    )
    assert max_attempts == 3, (
        f"a reservation denial raised max_attempts from 3 to {max_attempts}. "
        "The ceiling is a bound, not a counter: raising it every cycle keeps "
        "the failure gate attempt >= max_attempts permanently unreachable "
        "and walks the smallint column toward overflow."
    )
    assert status == "scheduled"

    snoozed, blocked = await _counter_pair(clean_jobs_app, schema, job_id)
    assert blocked == 1, (
        f"a reservation denial left rate_limit_blocked_count at {blocked}; the "
        "denial must be counted on the job row — the coalesced, O(1)-in-denials "
        "record of how often admission was refused."
    )
    assert snoozed == 0, (
        f"a reservation denial bumped snooze_count to {snoozed}; the counters "
        "are keyed by outcome — admission denials and plain snoozes are "
        "materially different signals."
    )


async def test_retry_after_without_budget_writes_no_event_or_attempt_rows(
    clean_jobs_app: JobsApp,
) -> None:
    """The sibling arm: ``RetryAfter(consume_budget=False)`` is the same
    non-execution deferral and must follow the same rule — no rows, no
    ceiling raise."""
    schema, job_id, worker_id = await _seed_running(clean_jobs_app, max_attempts=3)

    outcome = await clean_jobs_app.backend.mark_retry_after(
        job_id,
        worker_id,
        _DELAY,
        consume_budget=False,
    )
    assert outcome == "scheduled"

    events, attempts, max_attempts, status = await _trail_counts(clean_jobs_app, schema, job_id)
    assert events == 0, (
        f"mark_retry_after(consume_budget=False) wrote {events} job_events "
        "row(s) — same unbounded per-occurrence trail as the reservation "
        "denial path, one statement over."
    )
    assert attempts == 0, (
        f"mark_retry_after(consume_budget=False) wrote {attempts} "
        "job_attempts row(s) for a job that never executed."
    )
    assert max_attempts == 3, (
        f"mark_retry_after(consume_budget=False) raised max_attempts from 3 "
        f"to {max_attempts}; the ceiling must not be used as a counter."
    )
    assert status == "scheduled"

    snoozed, blocked = await _counter_pair(clean_jobs_app, schema, job_id)
    assert snoozed == 1, (
        f"a non-consuming RetryAfter left snooze_count at {snoozed}; the "
        "deferral must be counted on the job row."
    )
    assert blocked == 0, (
        f"a plain non-consuming RetryAfter bumped rate_limit_blocked_count "
        f"to {blocked}; that counter is keyed to admission denials."
    )


async def test_denial_loop_terminates_within_retry_budget(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A job denied admission on every cycle must reach a terminal outcome
    within its retry budget — there must be no denial loop without a
    terminal exit.

    Drives the real cycle — dispatch (which increments ``attempt``) →
    reservation denial (``mark_snoozed``) → re-dispatch — on a job with
    ``max_attempts=3`` and no ``schedule_to_close`` (the default). With
    the ceiling held fixed, the job must fail out terminally once the
    budget is spent; today the snooze raises the ceiling every cycle, so
    ``attempt < max_attempts`` is invariant and the job can never exit.
    """
    schema = module_pg_schema.schema_name
    backend = clean_jobs_app.backend
    worker_id = new_uuid()
    job_id = new_job_id()

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
            "ON CONFLICT (name) DO UPDATE SET mode = $2",
            "default",
            "strict_fifo",
        )
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, NULL, $2, $3::jsonb) ON CONFLICT (actor) DO NOTHING",
            "denial_loop_actor",
            "default",
            "{}",
        )

    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="denial_loop_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        )
    )

    # Generous bound: with the ceiling fixed at 3, the budget is spent by
    # the third cycle. Ten cycles leaves the fix room to choose its own
    # terminal point (e.g. a stall threshold) while still proving the loop
    # is bounded.
    for cycle in range(10):
        rows = await backend.dispatch_batch(worker_id, ["default"], 1, _LEASE)
        claimed = [r.id for r in rows]
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
            status: str = await conn.fetchval(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        if status in _TERMINAL_STATUSES:
            return  # the loop terminated — the contract holds
        assert claimed == [job_id], (
            f"cycle {cycle}: the denied job is no longer dispatchable but is "
            f"in non-terminal status {status!r} — stranded, not terminated."
        )
        await backend.mark_snoozed(
            JobId(job_id),
            worker_id,
            timedelta(0),
            outcome="reservation_denied",
        )
        # A terminal outcome from the denial path is what the fix may add;
        # the row's status is the source of truth, checked at the top of
        # the next iteration — so make the job immediately re-eligible and
        # loop. The zero-delay denial snooze is floored at
        # MIN_DEFERRAL_INTERVAL, which leaves the job 'scheduled' 1 s out;
        # forcing scheduled_at into the past stands in for that second
        # passing, and the scheduled_to_pending sweep is the promotion a
        # real leader runs between dispatch rounds — the loop must go
        # through it, because dispatch only claims 'pending' rows.
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - interval '
                "'1 second' WHERE id = $1",
                job_id,
            )
        await backend.scheduled_to_pending()

    pytest.fail(
        "10 dispatch→denial→snooze cycles on a job with max_attempts=3 and "
        "no terminal outcome. Each snooze does max_attempts = max_attempts + 1 "
        "(_sql_templates.py:487) while dispatch does attempt = attempt + 1, "
        "so the gap is invariant, the failure gate attempt >= max_attempts "
        "is unreachable, and the only terminal arm (deadline_failed) is "
        "gated on schedule_to_close, which this job — like most — does not "
        "set. A denial loop must be bounded independently of "
        "schedule_to_close."
    )


async def test_denial_on_non_retryable_at_budget_fails_max_attempts(
    clean_jobs_app: JobsApp,
) -> None:
    """A ``non_retryable`` job at ``attempt >= max_attempts`` with no
    ``schedule_to_close`` must reach a terminal exit through the denial
    path itself: the budget guard on the non-consuming snooze arm is the
    loop's only bounded exit, so the denial fails the job with
    ``MaxAttemptsExceeded`` (and writes the terminal attempt+event rows a
    terminal transition always writes) instead of rescheduling forever.
    The row counters are the SNOOZE arm's record — the terminal exit's
    record is its attempt/event rows and error_class, and OTEL counted
    the denial itself.
    """
    schema, job_id, worker_id = await _seed_running(clean_jobs_app, max_attempts=1)
    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET retry_kind = 'non_retryable', attempt = 1 WHERE id = $1",
            job_id,
        )

    outcome = await clean_jobs_app.backend.mark_snoozed(
        job_id,
        worker_id,
        _DELAY,
        outcome="reservation_denied",
    )
    assert outcome == "failed:MaxAttemptsExceeded"

    events, attempts, max_attempts, status = await _trail_counts(clean_jobs_app, schema, job_id)
    assert status == "failed"
    assert max_attempts == 1
    # Terminal transitions write the machine-readable failure record: one
    # attempt row and one state_change event — exactly once, at the exit.
    assert attempts == 1
    assert events == 1

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        row = await conn.fetchrow(
            f"SELECT error_class, error_message, snooze_count, rate_limit_blocked_count "
            f'FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None
    assert row["error_class"] == "MaxAttemptsExceeded"
    assert row["error_message"] == "retry budget exhausted"
    # The counters are the snooze arm's record; the terminal arm carries
    # the failure through the ordinary terminal-row channel instead.
    assert row["rate_limit_blocked_count"] == 0
    assert row["snooze_count"] == 0
