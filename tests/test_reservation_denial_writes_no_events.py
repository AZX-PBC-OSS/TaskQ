# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Contracts for the admission-denial write trail and budget semantics.

A reservation/rate-limit denial is admission control, not an execution.
Two paths express it: ``mark_snoozed`` with an
``outcome="reservation_denied"``, and the sibling
``RetryAfter(consume_budget=False)`` arm an actor takes when honouring a
server-provided ``Retry-After``. Both must behave identically.

A denial carries HTTP-429 semantics - "come back later, with a
Retry-After" - and nothing more. It says the fleet had no slot, which
is a statement about capacity, not about the job. So it must never
touch the job's retry budget and must never, by itself, terminally fail
the job. A queue or rate-limit misconfiguration must not be able to
kill work that simply never got a slot: the denied job is rescheduled
indefinitely with backoff until capacity frees, and the only thing that
ends it is its own ``schedule_to_close`` deadline expiring through the
ordinary deadline path.

The settled contract these tests pin:

1. A denial must NOT persist per-occurrence rows - it is counted on the
   job row and emitted to OTEL; history belongs to collectors.
2. A denial must NOT raise ``max_attempts``, and must NOT consume the
   budget - the ceiling is a bound on executions, and a denial is not
   an execution.
3. A denial must NOT terminalise a job. A job denied on every cycle
   stays retryable forever; its only terminal exit is the deadline
   path, which requires an explicit ``schedule_to_close``.
4. Contention stays observable through the aggregated denial counter on
   the job row, since the per-denial rows are gone.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.backend.postgres import PostgresBackend
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
        attempt=1,
        claim_epoch=1,
    )
    assert outcome == "scheduled"

    events, attempts, max_attempts, status = await _trail_counts(clean_jobs_app, schema, job_id)
    assert events == 0, (
        f"a reservation denial wrote {events} job_events row(s). A denial is "
        "admission control, not a state transition anyone reads back: no "
        "machine consumer ever matches it (the reclaim outbox reads only "
        "kind='state_change' AND reason='lock_expired') and the only human "
        "reader is the admin job-detail page. The denial must be counted on "
        "the job row and emitted to OTEL, never persisted per occurrence - "
        "a per-denial row is an unbounded-growth vector on a surface nothing "
        "reads back."
    )
    assert attempts == 0, (
        f"a reservation denial wrote {attempts} job_attempts row(s). The "
        "attempts table records failed EXECUTIONS; a denied job never "
        "executed. The row only fits because dispatch had already bumped "
        "attempt - a conceptual error underneath the storage one."
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
        "denial must be counted on the job row - the coalesced, O(1)-in-denials "
        "record of how often admission was refused."
    )
    assert snoozed == 0, (
        f"a reservation denial bumped snooze_count to {snoozed}; the counters "
        "are keyed by outcome - admission denials and plain snoozes are "
        "materially different signals."
    )


async def test_retry_after_without_budget_writes_no_event_or_attempt_rows(
    clean_jobs_app: JobsApp,
) -> None:
    """The sibling arm: ``RetryAfter(consume_budget=False)`` is the same
    non-execution deferral and must follow the same rule - no rows, no
    ceiling raise."""
    schema, job_id, worker_id = await _seed_running(clean_jobs_app, max_attempts=3)

    outcome = await clean_jobs_app.backend.mark_retry_after(
        job_id,
        worker_id,
        _DELAY,
        consume_budget=False,
        attempt=1,
        claim_epoch=1,
    )
    assert outcome == "scheduled"

    events, attempts, max_attempts, status = await _trail_counts(clean_jobs_app, schema, job_id)
    assert events == 0, (
        f"mark_retry_after(consume_budget=False) wrote {events} job_events "
        "row(s) - same unbounded per-occurrence trail as the reservation "
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


async def test_denial_loop_never_terminalises_and_never_spends_budget(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A job denied admission on every cycle stays retryable forever.

    A denial is capacity backpressure, not a failed execution, so it
    carries HTTP-429 semantics: come back later. A job that never gets a
    slot must be rescheduled indefinitely - never terminally failed, and
    never charged for the denial - so that a queue or rate-limit
    misconfiguration cannot kill work that did nothing wrong. The only
    exit for such a job is its own ``schedule_to_close`` deadline, which
    this job (like most) does not set.

    Drives the real cycle - dispatch (which increments ``attempt``) →
    reservation denial (``mark_snoozed``) → re-dispatch - on a job with
    ``max_attempts=3`` and no ``schedule_to_close``. Well past the
    nominal budget the job must still be claimable and non-terminal,
    its ceiling untouched and its denial count the only thing rising.
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

    # Ten cycles is well past the nominal budget of 3: if a denial could
    # consume budget or terminalise, the job would be gone by cycle 3.
    cycles = 10
    for cycle in range(cycles):
        rows = await backend.dispatch_batch(worker_id, ["default"], 1, _LEASE)
        claimed = [r.id for r in rows]
        assert claimed == [job_id], (
            f"cycle {cycle}: the denied job was not claimable. A denial is "
            "capacity backpressure with 429 semantics - the job must be "
            "rescheduled and re-dispatchable indefinitely until capacity "
            "frees, not dropped out of the dispatch set."
        )
        await backend.mark_snoozed(
            JobId(job_id),
            worker_id,
            timedelta(0),
            outcome="reservation_denied",
            # The attempt-epoch fence: each cycle's dispatch stamped
            # attempt = attempt + 1, so the write carries the dispatched
            # row's current epoch (rows[0].attempt).
            attempt=rows[0].attempt,
            claim_epoch=rows[0].claim_epoch,
        )
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
            row = await conn.fetchrow(
                f'SELECT status, max_attempts, error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        assert row is not None
        assert row["status"] not in _TERMINAL_STATUSES, (
            f"cycle {cycle}: a reservation denial terminalised the job "
            f"(status {row['status']!r}, error_class {row['error_class']!r}). "
            "A denial must never by itself fail a job: the fleet having no "
            "slot says nothing about the work, and a queue or rate-limit "
            "misconfiguration must not be able to kill a job that merely "
            "never got admitted. The only terminal exit is the job's own "
            "schedule_to_close deadline, through the ordinary deadline path."
        )
        assert row["max_attempts"] == 3, (
            f"cycle {cycle}: max_attempts moved to {row['max_attempts']}. "
            "The ceiling is a fixed bound on executions; a denial neither "
            "spends it nor inflates it to stay under it."
        )
        # The zero-delay denial snooze is floored at MIN_DEFERRAL_INTERVAL,
        # which leaves the job 'scheduled' 1 s out; forcing scheduled_at
        # into the past stands in for that second passing, and the
        # scheduled_to_pending sweep is the promotion a real leader runs
        # between dispatch rounds - the loop must go through it, because
        # dispatch only claims 'pending' rows.
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - interval '
                "'1 second' WHERE id = $1",
                job_id,
            )
        await backend.scheduled_to_pending()

    # Contention stayed visible the whole time: the per-denial rows are
    # gone, so the aggregated counter on the row is the only signal an
    # operator has that this job is starving for capacity.
    events, attempts, max_attempts, status = await _trail_counts(clean_jobs_app, schema, job_id)
    assert status not in _TERMINAL_STATUSES
    assert max_attempts == 3
    assert events == 0, f"{cycles} denials wrote {events} job_events row(s)."
    assert attempts == 0, f"{cycles} denials wrote {attempts} job_attempts row(s)."

    snoozed, blocked = await _counter_pair(clean_jobs_app, schema, job_id)
    assert blocked == cycles, (
        f"{cycles} reservation denials left rate_limit_blocked_count at "
        f"{blocked}. With per-denial rows removed, this aggregated counter is "
        "the only way an operator can see that a job is being starved of "
        "admission rather than progressing - it must count every denial."
    )
    assert snoozed == 0, (
        f"admission denials bumped snooze_count to {snoozed}; the counters "
        "are keyed by outcome, and a denial is not an actor-requested snooze."
    )


async def test_denial_at_exhausted_budget_reschedules_rather_than_failing(
    clean_jobs_app: JobsApp,
) -> None:
    """Even a ``non_retryable`` job whose budget is already spent is only
    rescheduled by a denial, never failed by it.

    The retry budget bounds how many times an actor may RUN and fail. A
    denial is not a run: the fleet had no slot, which is a fact about
    capacity, not about the job. So no combination of ``retry_kind`` and
    ``attempt``/``max_attempts`` may turn an admission denial into a
    terminal failure - in particular never ``MaxAttemptsExceeded``, which
    asserts the actor ran and failed that many times and would be a lie
    about a job that never executed. Terminating here would let a
    misconfigured queue kill work with a retry count of 3.

    The denial is therefore a plain reschedule that writes no rows, leaves
    the ceiling alone, and shows up only on the aggregated denial counter.
    Terminating such a job remains the job's own ``schedule_to_close``
    deadline's business, via the deadline path.
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
        attempt=1,
        claim_epoch=1,
    )
    assert outcome == "scheduled", (
        f"a reservation denial on a budget-exhausted job returned {outcome!r}. "
        "A denial has 429 semantics - come back later - and must reschedule, "
        "never terminalise: the budget bounds executions, and a denied job "
        "never executed."
    )

    events, attempts, max_attempts, status = await _trail_counts(clean_jobs_app, schema, job_id)
    assert status == "scheduled", (
        f"a reservation denial left the job in status {status!r}; a job that "
        "never got a slot must remain live until capacity frees or its own "
        "schedule_to_close deadline expires."
    )
    assert max_attempts == 1, (
        f"a reservation denial moved max_attempts to {max_attempts}; the "
        "ceiling is a fixed bound, neither spent nor inflated by a denial."
    )
    assert attempts == 0, (
        f"a reservation denial wrote {attempts} job_attempts row(s) for a job that never executed."
    )
    assert events == 0, f"a reservation denial wrote {events} job_events row(s)."

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        row = await conn.fetchrow(
            f"SELECT error_class, error_message, snooze_count, rate_limit_blocked_count "
            f'FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None
    assert row["error_class"] is None, (
        f"a reservation denial stamped error_class {row['error_class']!r} on a "
        "job that is still live - in particular MaxAttemptsExceeded would "
        "claim the actor ran and failed, which never happened."
    )
    assert row["error_message"] is None
    # Per-denial rows are gone, so the aggregated counter is the operator's
    # only view of admission contention on this job.
    assert row["rate_limit_blocked_count"] == 1, (
        f"the denial left rate_limit_blocked_count at "
        f"{row['rate_limit_blocked_count']}; contention must stay visible "
        "through the aggregated counter on the job row."
    )
    assert row["snooze_count"] == 0


async def _seed_queue_and_actor(app: JobsApp, schema: str, actor: str) -> None:
    """A strict-FIFO default queue and an uncapped actor config for it."""
    async with app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
            "ON CONFLICT (name) DO UPDATE SET mode = $2",
            "default",
            "strict_fifo",
        )
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, NULL, $2, $3::jsonb) ON CONFLICT (actor) DO NOTHING",
            actor,
            "default",
            "{}",
        )


async def _promote_due_now(app: JobsApp, schema: str, job_id: JobId) -> None:
    """Advance the job past its deferral floor and run the promotion sweep.

    A zero-delay denial snooze is floored at ``MIN_DEFERRAL_INTERVAL``, so
    the row sits ``scheduled`` a second out.  Pulling ``scheduled_at`` into
    the past stands in for that second elapsing; the promotion sweep is
    what a real leader runs between dispatch rounds, and dispatch only
    claims ``pending`` rows.
    """
    async with app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - interval '
            "'1 second' WHERE id = $1",
            job_id,
        )
    await app.backend.scheduled_to_pending()  # type: ignore[union-attr]  # Why: backend is object-typed in the shim; PostgresBackend has scheduled_to_pending at runtime.


async def test_denied_job_runs_to_success_once_capacity_frees(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A job denied admission repeatedly still executes and succeeds when
    a slot finally opens - the denials cost it nothing.

    This is the operator-visible half of 429 semantics.  "Never terminally
    failed" alone is not reliability: a job could satisfy that and still be
    permanently unrunnable, having been pushed out of the dispatch set, had
    its fence epoch corrupted, or had its budget quietly spent so the first
    real execution is refused.  What an operator needs is convergence - the
    work eventually runs, exactly once, and reports success.

    The adverse condition is a long denial streak well past the nominal
    retry budget, driven through the real dispatch → deny → promote cycle.
    Capacity then frees and the job is dispatched and completed through the
    ordinary terminal write.
    """
    schema = module_pg_schema.schema_name
    backend = clean_jobs_app.backend
    worker_id = new_uuid()
    job_id = JobId(new_job_id())
    actor = "denial_convergence_actor"

    await _seed_queue_and_actor(clean_jobs_app, schema, actor)
    await backend.enqueue(  # type: ignore[union-attr]  # Why: backend is object-typed in the shim; PostgresBackend has enqueue at runtime.
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        )
    )
    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        await create_worker(conn, schema, worker_id)

    # Capacity is unavailable: every cycle claims the job and denies it.
    denial_cycles = 8
    for cycle in range(denial_cycles):
        rows = await backend.dispatch_batch(worker_id, ["default"], 1, _LEASE)  # type: ignore[union-attr]  # Why: as above.
        assert [r.id for r in rows] == [job_id], (
            f"cycle {cycle}: a denied job fell out of the dispatch set. Work "
            "that never got a slot must stay reachable - a job nobody can "
            "claim any more is lost, whatever its row says."
        )
        denial_outcome = await backend.mark_snoozed(  # type: ignore[union-attr]  # Why: as above.
            job_id,
            worker_id,
            timedelta(0),
            outcome="reservation_denied",
            attempt=rows[0].attempt,
            claim_epoch=rows[0].claim_epoch,
        )
        assert denial_outcome == "scheduled", (
            f"cycle {cycle}: the denial returned {denial_outcome!r}. Denials "
            "carry 429 semantics and must only ever reschedule - a denial "
            "that terminalises the job destroys work that never ran, which "
            "is exactly the outcome a capacity shortage must not produce."
        )
        await _promote_due_now(clean_jobs_app, schema, job_id)

    # Capacity frees: the job is claimed and run to completion.
    rows = await backend.dispatch_batch(worker_id, ["default"], 1, _LEASE)  # type: ignore[union-attr]  # Why: as above.
    assert [r.id for r in rows] == [job_id], (
        f"after {denial_cycles} denials the job was no longer dispatchable, so "
        "it can never run: admission backpressure destroyed the work instead "
        "of deferring it."
    )
    succeeded = await backend.mark_succeeded(  # type: ignore[union-attr]  # Why: as above.
        job_id,
        worker_id,
        {"ok": True},
        attempt=rows[0].attempt,
        claim_epoch=rows[0].claim_epoch,
    )
    assert succeeded, (
        "the terminal success write matched no row after a denial streak. The "
        "attempt-epoch fence must still admit the one real execution - a "
        "denial that desynchronises the fence makes a job permanently "
        "unfinishable while it still looks healthy."
    )

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        row = await conn.fetchrow(
            f"SELECT status, error_class, rate_limit_blocked_count, max_attempts "
            f'FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempt_rows = await conn.fetch(
            f'SELECT outcome FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
    assert row is not None
    assert row["status"] == "succeeded", (
        f"the job ended in status {row['status']!r} rather than succeeded "
        "after capacity freed and its single execution completed."
    )
    assert row["error_class"] is None
    assert row["max_attempts"] == 3, (
        f"max_attempts drifted to {row['max_attempts']} across the denial "
        "streak; the ceiling is a bound on executions, and no execution "
        "happened until the last cycle."
    )
    assert row["rate_limit_blocked_count"] == denial_cycles, (
        f"rate_limit_blocked_count is {row['rate_limit_blocked_count']} after "
        f"{denial_cycles} denials. With no per-denial rows, this aggregated "
        "counter is the only evidence an operator has that the job spent time "
        "starved of admission rather than merely sitting idle."
    )
    assert len(attempt_rows) == 1, (
        f"the job accrued {len(attempt_rows)} job_attempts rows for one real "
        "execution; denials must leave no execution trail, and the single "
        "execution must leave exactly one."
    )
    assert attempt_rows[0]["outcome"] == "succeeded"


async def test_denied_job_past_its_close_deadline_fails_visibly_not_silently(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A job denied until its ``schedule_to_close`` passes reaches an
    explicit terminal state an operator can see - it never goes quiet.

    Denials are deliberately invisible in the durable trail, which creates
    the opposite reliability hazard: work that is never admitted and never
    terminated becomes a row nobody watches, pending forever with no
    signal.  The bound is the job's own close deadline, and crossing it
    must produce the ordinary terminal failure - status, error class,
    finish timestamp, an attempt row and a state-change event - through the
    deadline sweep, not a silent disappearance from the dispatch set.
    """
    schema = module_pg_schema.schema_name
    backend = clean_jobs_app.backend
    worker_id = new_uuid()
    job_id = JobId(new_job_id())
    actor = "denial_deadline_actor"

    await _seed_queue_and_actor(clean_jobs_app, schema, actor)
    await backend.enqueue(  # type: ignore[union-attr]  # Why: backend is object-typed in the shim; PostgresBackend has enqueue at runtime.
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
            schedule_to_close=datetime.now(UTC) + timedelta(hours=1),
        )
    )
    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        await create_worker(conn, schema, worker_id)

    for cycle in range(4):
        rows = await backend.dispatch_batch(worker_id, ["default"], 1, _LEASE)  # type: ignore[union-attr]  # Why: as above.
        assert [r.id for r in rows] == [job_id], (
            f"cycle {cycle}: the denied job was not claimable before its close "
            "deadline; until that deadline it must stay live and reachable."
        )
        assert (
            await backend.mark_snoozed(  # type: ignore[union-attr]  # Why: as above.
                job_id,
                worker_id,
                timedelta(0),
                outcome="reservation_denied",
                attempt=rows[0].attempt,
                claim_epoch=rows[0].claim_epoch,
            )
            == "scheduled"
        )
        await _promote_due_now(clean_jobs_app, schema, job_id)

    # The close deadline passes while the job is still waiting for a slot.
    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        await conn.execute(
            f'UPDATE "{schema}".jobs SET schedule_to_close = clock_timestamp() - '
            "interval '1 second' WHERE id = $1",
            job_id,
        )
        swept = await PostgresBackend.sweep_deadline_exceeded(conn, schema=schema)
    assert swept == 1, (
        f"the deadline sweep reclaimed {swept} rows; a job that ran out its "
        "close deadline while being denied admission must be terminated by "
        "the ordinary deadline path, not left waiting indefinitely."
    )

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: as above.
        row = await conn.fetchrow(
            f'SELECT status, error_class, finished_at FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempt_rows = await conn.fetch(
            f'SELECT outcome, error_class FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        # The denial transition is running → scheduled.  The scheduled →
        # pending promotion the sweep writes is an ordinary transition every
        # deferred job makes, so it is counted separately rather than being
        # charged to the denial.
        denial_events: int = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1 '
            "AND detail->>'from_state' = 'running' AND detail->>'to_state' = 'scheduled'",
            job_id,
        )
        terminal_events: int = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1 '
            "AND detail->>'to_state' = 'failed'",
            job_id,
        )
    assert row is not None
    assert row["status"] == "failed", (
        f"the job is in status {row['status']!r} after its close deadline "
        "passed. A denied job whose deadline expires must reach an explicit "
        "terminal state; anything else is work that quietly stops moving with "
        "nothing for an operator to alert on."
    )
    assert row["error_class"] == "DeadlineExceeded", (
        f"the terminal failure names error_class {row['error_class']!r}. The "
        "cause must say the deadline expired - not MaxAttemptsExceeded, which "
        "would claim executions that never happened."
    )
    assert row["finished_at"] is not None, (
        "the terminal row carries no finished_at, so the job's end is not "
        "visible to anything reading completion times."
    )
    assert len(attempt_rows) == 1 and attempt_rows[0]["outcome"] == "failed", (
        f"the deadline termination left {len(attempt_rows)} attempt row(s); "
        "the terminal outcome must be auditable exactly once, and the "
        "preceding denials must have contributed none."
    )
    assert attempt_rows[0]["error_class"] == "DeadlineExceeded"
    assert denial_events == 0, (
        f"the denial cycles left {denial_events} running→scheduled event "
        "row(s). Admission denials must write no per-denial rows: a job "
        "starved of capacity for hours would otherwise accrue durable rows "
        "linear in the denial count, on a surface nothing reads back."
    )
    assert terminal_events == 1, (
        f"the job's timeline holds {terminal_events} terminal transition(s); "
        "a job that reached a terminal state must leave exactly one, so its "
        "end is visible to anyone reading the timeline."
    )
