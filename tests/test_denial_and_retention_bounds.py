"""Bounds on durable rows written by reservation/rate-limit denials, and
age-based retention for ``job_events``.

An admission denial - a rate-limit or reservation refusal - carries HTTP-429
semantics: "come back later".  No handler ran and nothing failed, so a denial
must never consume the job's retry budget and must never by itself terminally
fail a job.  A denied job is rescheduled for as long as it takes, until
capacity frees or its own ``schedule_to_close`` deadline expires and ends it
through the ordinary deadline path.  A queue or rate-limit misconfiguration is
an operator problem; it must not be able to kill work that simply never got a
slot.

Three contracts are pinned here, all verified against real Postgres:

1. Denials write no per-denial ``job_attempts`` or ``job_events`` rows.  A job
   denied forever must not accrue durable rows linear in the number of
   denials from a single logical unit of work.  Contention stays observable
   through the aggregated denial counter on the job row.

2. Neither side of the retry budget moves across a denial loop:
   ``max_attempts`` stays at its configured ceiling and ``attempt`` returns to
   where it started, so the job stays reschedulable.  The rows of a job that
   does reach a terminal state - via its close deadline - are reclaimable by
   the terminal prune.

3. ``job_events`` needs its own age-based retention, independent of parent
   terminality: a long-lived non-terminal job otherwise accumulates event rows
   with no upper bound and no mechanism that can ever remove them.  There is
   also no index on ``occurred_at`` alone to support such a sweep.

The tests below assert the DESIRABLE behaviour so they go green when the
implementation matches it.

CRITICAL - crash-reclamation outbox.  ``job_events`` rows with
``kind = 'state_change' AND detail->>'reason' = 'lock_expired'`` are the
outbox consumed by ``poll_reclaim_events`` driving ``TaskQ.watch_reclaims()``.
Any age-based retention MUST exempt that slice or crash reclamation silently
stops.  ``test_lock_expired_reclaim_outbox_is_exempt_from_retention`` pins
that exemption and is as critical as the red tests.
"""

from datetime import UTC, datetime, timedelta
from typing import Final, Protocol, cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq._json import dumps_str
from taskq.backend._protocol import JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.testing.pg import create_running_job, create_worker, seed_actors
from taskq.worker._leader_shared import prune_terminal_jobs
from taskq.worker.deps import WorkerDeps, open_worker_deps

pytestmark = pytest.mark.integration

_ZERO = timedelta(seconds=0)

# Number of deny/redispatch cycles a perpetually denied job is driven through.
_DENIAL_CYCLES = 12

# The per-job durable-row budget a denial loop must respect.  A correct
# implementation may keep a handful of rows (e.g. a first-denial event plus a
# coalesced counter), but it must NOT grow linearly with the number of
# denials.  Anything at or below the cycle count proves coalescing; we allow a
# generous constant so a reasonable fix is not over-constrained.
_BOUNDED_ROW_BUDGET = 4


# ── Fixtures / helpers ────────────────────────────────────────────────


def _build_settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema.lower(),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )


async def _relock_for_next_dispatch(
    conn: asyncpg.Connection, schema: str, job_id: UUID, worker_id: UUID
) -> None:
    """Re-run the parts of dispatch that matter for a denial loop.

    ``mark_snoozed`` clears the lock and moves the job to scheduled/pending.
    The real dispatcher then re-locks it and does ``attempt = j.attempt + 1``
    (``backend/_dispatch_sql.py``).  Reproducing exactly that here keeps the
    loop faithful without spinning a whole worker.
    """
    await conn.execute(
        f'UPDATE "{schema}".jobs '  # noqa: S608
        "SET status = 'running', "
        "    locked_by_worker = $2, "
        "    lock_expires_at = clock_timestamp() + interval '60 seconds', "
        "    started_at = clock_timestamp(), "
        "    last_heartbeat_at = clock_timestamp(), "
        "    attempt = attempt + 1, "
        "    claim_epoch = claim_epoch + 1 "
        "WHERE id = $1",
        job_id,
        worker_id,
    )


async def _current_attempt(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    """The row's current attempt number - the denial-cycle writes' fence bind."""
    attempt: int | None = await conn.fetchval(
        f'SELECT attempt FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated against _IDENT_RE upstream; job_id is $-bound.
        job_id,
    )
    assert attempt is not None
    return attempt


async def _current_claim_epoch(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    """The row's current claim epoch - the denial-cycle writes' second fence bind.

    The relock below bumps it beside the attempt (a real claim advances
    both), so the epoch is read fresh each cycle too.
    """
    epoch: int | None = await conn.fetchval(
        f'SELECT claim_epoch FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
        job_id,
    )
    assert epoch is not None
    return epoch


async def _count(conn: asyncpg.Connection, schema: str, table: str, job_id: UUID) -> int:
    count: int | None = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".{table} WHERE job_id = $1',  # noqa: S608
        job_id,
    )
    return count or 0


async def _drive_denial_loop(
    backend: PostgresBackend,
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    worker_id: UUID,
    *,
    cycles: int = _DENIAL_CYCLES,
) -> None:
    """Drive ``cycles`` real reservation denials through the real backend.

    Each iteration is exactly what the worker does when an actor raises
    ``ReservationUnavailable``: ``_handle_reservation_class_denied`` calls
    ``backend.mark_snoozed(..., outcome="reservation_denied")``, then the
    dispatcher picks the job back up.
    """
    for _ in range(cycles):
        outcome = await backend.mark_snoozed(
            JobId(job_id),
            worker_id,
            _ZERO,
            metadata_update={"awaiting": "reservation:test_bucket"},
            outcome="reservation_denied",
            # The attempt-epoch fence: _relock_for_next_dispatch replays
            # dispatch's attempt increment, so the write carries the row's
            # CURRENT epoch, read fresh each cycle.
            attempt=await _current_attempt(conn, schema, job_id),
            claim_epoch=await _current_claim_epoch(conn, schema, job_id),
        )
        assert outcome == "scheduled", f"denial cycle did not snooze: {outcome!r}"
        await _relock_for_next_dispatch(conn, schema, job_id, worker_id)


_SEED_CARRIER_DEADLINE: Final[object] = object()
"""Sentinel for ``_seed_denied_job``: seed with the +1-day close deadline -
the shape whose denial loop keeps rescheduling until the deadline. Pass
``None`` for the no-deadline shape, which has no terminal exit at all under
denial pressure: it stays reschedulable indefinitely."""


async def _seed_denied_job(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    *,
    schedule_to_close: object | datetime | None = _SEED_CARRIER_DEADLINE,
) -> UUID:
    job_id = new_job_id()
    await seed_actors(conn, schema)
    await create_worker(conn, schema, worker_id)
    await create_running_job(
        conn,
        schema,
        worker_id,
        job_id=job_id,
        max_attempts=3,
        attempt=1,
        schedule_to_close=cast(
            "datetime | None",
            datetime.now(UTC) + timedelta(days=1)
            if schedule_to_close is _SEED_CARRIER_DEADLINE
            else schedule_to_close,
        ),
    )
    return job_id


# ── Denials must not accrue durable rows per denial ───────────────────


async def test_reservation_denial_does_not_accrue_unbounded_durable_rows(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """CONTRACT: a job denied a reservation N times must not write O(N)
    durable rows.

    A denial is not work that happened - no handler ran, no attempt was
    consumed, no budget was spent.  It is backpressure.  Recording it as a
    full ``job_attempts`` row plus a full ``job_events`` row per poll turns a
    single logical unit of work into unbounded storage: a job denied by a
    saturated bucket for an hour at a one-second retry_after writes 7200 rows
    that describe nothing that happened.

    The desirable behaviour is that repeated denials COALESCE - a counter on
    the job row, or a single updated event - so the durable footprint of a
    denied job is O(1) in the number of denials.

    The durable footprint of a denied job must therefore be O(1) in the
    number of denials, with contention carried instead by the aggregated
    denial counter on the job row.
    """
    schema = settings.schema_name
    worker_settings = _build_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(worker_settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        worker_id = new_uuid()
        job_id = await _seed_denied_job(conn, schema, worker_id)

        events_before = await _count(conn, schema, "job_events", job_id)
        attempts_before = await _count(conn, schema, "job_attempts", job_id)

        async with open_worker_deps(worker_settings) as deps:
            backend = _make_backend(deps)
            await _drive_denial_loop(backend, conn, schema, job_id, worker_id)

        events_added = await _count(conn, schema, "job_events", job_id) - events_before
        attempts_added = await _count(conn, schema, "job_attempts", job_id) - attempts_before
    finally:
        await conn.close()

    assert attempts_added <= _BOUNDED_ROW_BUDGET, (
        f"{_DENIAL_CYCLES} reservation denials wrote {attempts_added} job_attempts rows; "
        f"denials must coalesce to at most {_BOUNDED_ROW_BUDGET} durable rows, not grow "
        "one-per-poll - a denial records that nothing happened."
    )
    assert events_added <= _BOUNDED_ROW_BUDGET, (
        f"{_DENIAL_CYCLES} reservation denials wrote {events_added} job_events rows; "
        f"denials must coalesce to at most {_BOUNDED_ROW_BUDGET} durable rows, not grow "
        "one-per-poll - a denial records that nothing happened."
    )


def _make_backend(deps: WorkerDeps) -> PostgresBackend:
    return PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=_ZERO,
        cleanup_grace_period=_ZERO,
    )


# ── Denials must not consume retry budget ─────────────────────────────


async def test_denial_consumes_no_retry_budget(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """CONTRACT: an admission denial never spends the job's retry budget.

    A denial is HTTP-429 semantics - "come back later" - not a failed
    execution.  No handler ran, nothing failed, so nothing may be charged
    against the budget that exists to bound *failures*.  Concretely, across
    any number of deny/redispatch cycles both sides of the budget must be
    untouched: ``max_attempts`` stays at its configured ceiling (it is a
    ceiling, never a tally that inflates to "pay back" a spend), and
    ``attempt`` returns to where it started (the dispatcher's claim-time
    increment is refunded, because that claim did no work).

    This matters because the alternative lets a capacity misconfiguration
    kill work.  A saturated bucket or a mis-sized rate limit is an operator
    problem; if each denial nibbles the budget, a job with ``max_attempts=3``
    dies after three polls against a full bucket having never once run its
    handler.  Backpressure must delay work, never destroy it - a denied job
    is rescheduled indefinitely until capacity frees or its
    schedule-to-close deadline ends it through the normal deadline path.

    Contention stays visible instead through the aggregated denial counter
    on the job row, which must rise once per denial.
    """
    schema = settings.schema_name
    worker_settings = _build_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(worker_settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        worker_id = new_uuid()
        # No schedule_to_close: nothing but the budget could end this job, so
        # if the budget is spendable by denials the loop terminalises and the
        # per-cycle "scheduled" assertion fails.
        job_id = await _seed_denied_job(conn, schema, worker_id, schedule_to_close=None)

        row_before = await conn.fetchrow(
            f"SELECT attempt, max_attempts, rate_limit_blocked_count "  # noqa: S608
            f'FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert row_before is not None
        max_attempts_before: int = row_before["max_attempts"]
        attempt_before: int = row_before["attempt"]
        denials_before: int = row_before["rate_limit_blocked_count"]

        async with open_worker_deps(worker_settings) as deps:
            backend = _make_backend(deps)
            await _drive_denial_loop(backend, conn, schema, job_id, worker_id)

        row_after = await conn.fetchrow(
            f"SELECT attempt, max_attempts, status::text AS status, "  # noqa: S608
            f'rate_limit_blocked_count FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert row_after is not None
        max_attempts_after: int = row_after["max_attempts"]
        attempt_after: int = row_after["attempt"]
        status_after: str = row_after["status"]
        denials_after: int = row_after["rate_limit_blocked_count"]
    finally:
        await conn.close()

    assert max_attempts_after == max_attempts_before, (
        f"max_attempts drifted {max_attempts_before} -> {max_attempts_after} across "
        f"{_DENIAL_CYCLES} denials; the configured ceiling must be immutable - a denial "
        "is not an execution, so neither side of the budget may move."
    )
    assert attempt_after == attempt_before, (
        f"attempt drifted {attempt_before} -> {attempt_after} across {_DENIAL_CYCLES} "
        "denials; the claim that ended in a denial did no work, so its increment must be "
        "refunded - otherwise a saturated bucket spends a budget that exists to bound "
        "failures, and a job dies without its handler ever running."
    )
    assert status_after not in {"failed", "cancelled", "crashed", "abandoned"}, (
        f"{_DENIAL_CYCLES} denials drove the job to {status_after!r}; backpressure must "
        "never by itself terminally fail a job. A denied job stays reschedulable until "
        "capacity frees or its schedule-to-close deadline expires."
    )
    assert denials_after == denials_before + _DENIAL_CYCLES, (
        f"the aggregated denial counter moved {denials_before} -> {denials_after} across "
        f"{_DENIAL_CYCLES} denials; with no per-denial job_events or job_attempts rows "
        "this counter is the only way contention stays visible to an operator."
    )


# ── A deadline-exited denied job's rows are reclaimable ───────────────


async def test_denial_rows_are_reclaimable_by_retention(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """CONTRACT: rows written by a denial loop must be reclaimable.

    ``prune_terminal_jobs`` keys on ``status IN (terminal) AND finished_at
    < cutoff`` - a standard reclaim shape (delete whole job rows keyed on
    a completion timestamp with a configurable retention window). A zero
    retention is the documented prune-terminal-now point, archiving all
    terminal jobs immediately.

    A denied job must never be terminalised by the denials themselves -
    backpressure delays work, it does not destroy it.  The one exit a
    perpetually denied job has is its own ``schedule_to_close`` deadline:
    when the next reschedule point would fall past it, the job fails
    terminally as ``DeadlineExceeded`` through the ordinary deadline path,
    exactly as it would have had it been waiting for any other reason.
    That is the moment its rows become reachable, and this test pins the
    whole chain: denial pressure against a close deadline, the deadline
    exit, and the prune reclaiming the aged rows via the parent cascade.

    Anchoring reclaimability on the deadline rather than on retry
    exhaustion is what keeps the two halves consistent - if a denial could
    exhaust the budget, this test would be green for the wrong reason and
    would quietly re-license killing work that never got a slot.
    """
    schema = settings.schema_name
    worker_settings = _build_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(worker_settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        worker_id = new_uuid()
        # A close deadline already in the past: the very next reschedule
        # point falls beyond it, so the deadline arm - the only terminal
        # exit a denied job has - fires on the first denial.
        job_id = await _seed_denied_job(
            conn,
            schema,
            worker_id,
            schedule_to_close=datetime.now(UTC) - timedelta(seconds=1),
        )

        async with open_worker_deps(worker_settings) as deps:
            backend = _make_backend(deps)
            # Drive the real denial cycle - mark_snoozed, then the
            # dispatcher's re-claim - until the deadline exit fires.
            terminal_outcome: str | None = None
            for _ in range(_DENIAL_CYCLES):
                outcome = await backend.mark_snoozed(
                    JobId(job_id),
                    worker_id,
                    _ZERO,
                    metadata_update={"awaiting": "reservation:test_bucket"},
                    outcome="reservation_denied",
                    # The attempt-epoch fence: the row's CURRENT epoch,
                    # read fresh each cycle (the relock below increments it).
                    attempt=await _current_attempt(conn, schema, job_id),
                    claim_epoch=await _current_claim_epoch(conn, schema, job_id),
                )
                if outcome != "scheduled":
                    terminal_outcome = outcome
                    break
                await _relock_for_next_dispatch(conn, schema, job_id, worker_id)
        assert terminal_outcome == "failed", (
            "a denied job past its schedule_to_close must fail terminally through the "
            f"deadline path, not the retry budget; last outcome {terminal_outcome!r}"
        )
        error_class: str | None = await conn.fetchval(
            f'SELECT error_class FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
        assert error_class == "DeadlineExceeded", (
            "the denied job's terminal exit must be attributed to its schedule_to_close "
            f"deadline, not to retry exhaustion; got error_class {error_class!r}. A denial "
            "never consumes retry budget, so MaxAttemptsExceeded is unreachable here."
        )

        row = await conn.fetchrow(
            f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
        assert row is not None and row["status"] == "failed"
        assert row["finished_at"] is not None

        # Age every row far past any plausible retention window.
        await conn.execute(
            f'UPDATE "{schema}".job_events SET occurred_at = occurred_at '  # noqa: S608
            "- interval '400 days' WHERE job_id = $1",
            job_id,
        )
        await conn.execute(
            f'UPDATE "{schema}".job_attempts SET started_at = started_at '  # noqa: S608
            "- interval '400 days', finished_at = finished_at - interval '400 days' "
            "WHERE job_id = $1",
            job_id,
        )

        events_before = await _count(conn, schema, "job_events", job_id)
        assert events_before > 0, "the terminal exit wrote no events - test setup is wrong"

        # Retention zero is the prune family's documented immediate-archive
        # point (settings.py: "timedelta(0) means archive all terminal jobs
        # immediately (valid)") - the corpus's prune-terminal-now form.
        _zero = timedelta(seconds=0)
        await prune_terminal_jobs(
            conn,
            retention_per_status={
                "succeeded": _zero,
                "failed": _zero,
                "cancelled": _zero,
                "crashed": _zero,
                "abandoned": _zero,
            },
            archive_retention=timedelta(days=365),
            batch_size=10_000,
            schema=schema,
        )

        events_after = await _count(conn, schema, "job_events", job_id)
    finally:
        await conn.close()

    assert events_after < events_before, (
        f"{events_before} job_events rows aged 400 days survived an immediate-retention "
        "prune of their terminal parent: prune_terminal_jobs' "
        "`status IN (terminal) AND finished_at < cutoff` predicate matched the "
        f"deadline-exited job but its event rows survived ({events_after} remain). A "
        "terminal job's rows must be reclaimable with it."
    )


# ── job_events needs age-based retention ──────────────────────────────


async def test_job_events_have_age_based_retention_for_nonterminal_parents(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """CONTRACT: ``job_events`` needs its own age-based retention sweep.

    ``job_events`` is append-only in the strictest sense: 16 INSERT sites in
    ``src/taskq`` and ZERO ``DELETE FROM`` sites.  Its only exit is the FK
    cascade when the parent job is pruned, which requires the parent to reach
    a terminal status.  A long-lived non-terminal job (a snoozing cron actor,
    a perpetually denied job, a job parked in ``scheduled``) accumulates event
    rows with no upper bound and no mechanism that can ever remove them.

    There must be a sweep that reclaims sufficiently old ``job_events`` rows
    independent of parent terminality - while EXEMPTING the crash-reclaim
    outbox slice (see the companion pinning test below).

    Because a denial never fails a job and a worker never refuses to start
    over a capacity condition, unbounded event growth has no loud failure to
    announce it - a bounded sweep is the only thing keeping the table from
    being a silent growth vector.
    """
    schema = settings.schema_name
    worker_settings = _build_settings(pg_dsn, schema)

    sweep = _find_job_events_retention_sweep()

    conn = await asyncpg.connect(str(worker_settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        worker_id = new_uuid()
        job_id = await _seed_denied_job(conn, schema, worker_id)

        await _insert_event(
            conn,
            schema,
            job_id,
            kind="progress",
            detail={"pct": 10},
            age=timedelta(days=400),
        )

        status: str | None = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
        assert status not in {"succeeded", "failed", "cancelled", "crashed", "abandoned"}, (
            "parent must be NON-terminal for this test to exercise the gap"
        )

        before = await _count(conn, schema, "job_events", job_id)
        assert before > 0

        assert sweep is not None, (
            "No age-based retention exists for job_events: the table has 16 INSERT sites "
            "and ZERO `DELETE FROM` sites in src/taskq. Its only exit is the FK cascade "
            "from a pruned parent job, so events under a non-terminal parent are "
            "unreclaimable forever. Add a sweep that deletes job_events older than a "
            "retention window regardless of parent status, EXEMPTING "
            "kind='state_change' AND detail->>'reason'='lock_expired' (the crash-reclaim "
            "outbox consumed by poll_reclaim_events). Note there is also no index on "
            "occurred_at alone - only (job_id, occurred_at) and the partial reclaim "
            "index - so such a sweep needs a supporting index."
        )

        await sweep(conn, schema=schema, retention=timedelta(days=30))
        after = await _count(conn, schema, "job_events", job_id)
    finally:
        await conn.close()

    assert after < before, (
        "an age-based job_events sweep exists but reclaimed nothing for a 400-day-old "
        "event under a non-terminal parent"
    )


async def test_lock_expired_reclaim_outbox_is_exempt_from_retention(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """PIN: the crash-reclaim outbox slice must NEVER be aged out.

    ``job_events`` rows with ``kind = 'state_change'`` and
    ``detail->>'reason' = 'lock_expired'`` are not history - they are the
    outbox ``poll_reclaim_events`` reads to drive ``TaskQ.watch_reclaims()``.
    ``poll_reclaim_events`` advances a trailing watermark over ``id``; if a
    retention sweep deletes an un-consumed row in that slice, the reclaim
    event is lost silently and permanently, and a crashed worker's job is
    never surfaced to any watcher.

    This test is the guard rail on the retention sweep: whatever shape that
    sweep takes, an aged ``lock_expired`` row must survive it and must still
    be visible to ``poll_reclaim_events``.
    """
    schema = settings.schema_name
    worker_settings = _build_settings(pg_dsn, schema)

    sweep = _find_job_events_retention_sweep()

    conn = await asyncpg.connect(str(worker_settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        worker_id = new_uuid()
        job_id = await _seed_denied_job(conn, schema, worker_id)

        await _insert_event(
            conn,
            schema,
            job_id,
            kind="state_change",
            detail={
                "from_state": "running",
                "to_state": "pending",
                "reason": "lock_expired",
                "worker_id": str(worker_id),
            },
            age=timedelta(days=400),
        )
        await _insert_event(
            conn,
            schema,
            job_id,
            kind="progress",
            detail={"pct": 50},
            age=timedelta(days=400),
        )

        if sweep is not None:
            await sweep(conn, schema=schema, retention=timedelta(days=30))

        surviving: int | None = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events '  # noqa: S608
            "WHERE job_id = $1 AND kind = 'state_change' "
            "AND detail->>'reason' = 'lock_expired'",
            job_id,
        )

        # The outbox must still be readable through the real poller query.
        async with open_worker_deps(worker_settings) as deps:
            backend = _make_backend(deps)
            reclaim_events = await backend.poll_reclaim_events(0, 100)
    finally:
        await conn.close()

    assert surviving == 1, (
        "the crash-reclaim outbox row (kind='state_change', "
        "detail->>'reason'='lock_expired') was deleted by age-based retention. "
        "That slice is an OUTBOX, not history: poll_reclaim_events drives "
        "TaskQ.watch_reclaims() from it, so deleting an un-consumed row silently loses a "
        "crashed worker's reclaim forever. Any job_events retention sweep MUST exempt it."
    )
    assert any(ev.job_id == job_id for ev in reclaim_events), (
        "the aged lock_expired row is no longer visible to poll_reclaim_events - crash "
        "reclamation is broken for this job."
    )


# ── Helpers for the retention tests ───────────────────────────────────


async def _insert_event(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    *,
    kind: str,
    detail: dict[str, object],
    age: timedelta,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail) '  # noqa: S608
        "VALUES ($1, clock_timestamp() - $2::interval, $3, $4::jsonb)",
        job_id,
        age,
        kind,
        dumps_str(detail),
    )


class _EventsSweep(Protocol):
    """The shape an age-based ``job_events`` retention sweep is expected to have."""

    async def __call__(
        self, conn: asyncpg.Connection, *, schema: str, retention: timedelta
    ) -> object: ...


def _find_job_events_retention_sweep() -> _EventsSweep | None:
    """Locate an age-based ``job_events`` retention sweep, if one exists.

    Deliberately tolerant about the shape a fix might take: any callable
    exported under a plausible name from the leader/sweep modules counts.
    Returns ``None`` when no such mechanism is present, which is the state
    this file pins today.
    """
    import importlib

    candidates = (
        ("taskq.worker._leader_shared", "prune_job_events"),
        ("taskq.worker._leader_shared", "prune_old_job_events"),
        ("taskq.worker._leader_shared", "prune_events"),
        ("taskq.worker.leader", "prune_job_events"),
        ("taskq.worker.leader", "prune_old_job_events"),
        ("taskq.backend._sweeps", "prune_job_events"),
        ("taskq.backend._sweeps", "sweep_old_job_events"),
    )
    for module_name, attr in candidates:
        try:
            module = importlib.import_module(module_name)
        except ImportError:  # pragma: no cover - module always present today
            continue
        fn = getattr(module, attr, None)
        if callable(fn):
            return cast("_EventsSweep", fn)
    return None
