"""Bounds on durable rows written by reservation/rate-limit denials, and
age-based retention for ``job_events``.

Three defects are pinned here, all verified against real Postgres:

1. A reservation denial is dispatched to ``_handle_reservation_class_denied``
   (``src/taskq/worker/_handlers.py``) with ``outcome="reservation_denied"``,
   which calls ``mark_snoozed``.  Per denial cycle that template writes one
   ``job_attempts`` row and one ``job_events`` row, so a job that is denied
   forever accrues unbounded durable rows from a single logical unit of work.

2. ``mark_snoozed`` also does ``max_attempts = j.max_attempts + 1`` while
   never assigning ``attempt``.  Dispatch increments ``attempt`` by one each
   cycle, so the gap ``max_attempts - attempt`` is INVARIANT across denials
   and the retry-exhaustion gate is unreachable.  Combined with
   ``finished_at = NULL`` on every snooze, the job never reaches a terminal
   status, so ``prune_terminal_jobs`` (which keys on
   ``status IN (terminal) AND finished_at < cutoff``) can never reclaim any
   of those rows at ANY retention period.

3. ``job_events`` has no age-based retention at all: 16 INSERT sites and zero
   DELETE sites in ``src/taskq``.  Its only exit is the FK cascade when the
   parent job is pruned, which by (2) never happens for a perpetually denied
   job.  There is also no index on ``occurred_at`` alone.

The tests below assert the DESIRABLE behaviour so they go green when the
defects are fixed.

CRITICAL — crash-reclamation outbox.  ``job_events`` rows with
``kind = 'state_change' AND detail->>'reason' = 'lock_expired'`` are the
outbox consumed by ``poll_reclaim_events`` driving ``TaskQ.watch_reclaims()``.
Any age-based retention MUST exempt that slice or crash reclamation silently
stops.  ``test_lock_expired_reclaim_outbox_is_exempt_from_retention`` pins
that exemption and is as load-bearing as the red tests.
"""

from datetime import UTC, datetime, timedelta
from typing import Protocol, cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq._json import dumps_str
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
        "    attempt = attempt + 1 "
        "WHERE id = $1",
        job_id,
        worker_id,
    )


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
            job_id,
            worker_id,
            _ZERO,
            metadata_update={"awaiting": "reservation:test_bucket"},
            outcome="reservation_denied",
        )
        assert outcome == "scheduled", f"denial cycle did not snooze: {outcome!r}"
        await _relock_for_next_dispatch(conn, schema, job_id, worker_id)


async def _seed_denied_job(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> UUID:
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
        schedule_to_close=datetime.now(UTC) + timedelta(days=1),
    )
    return job_id


# ── DEFECT 1 — denials accrue unbounded durable rows ──────────────────


async def test_reservation_denial_does_not_accrue_unbounded_durable_rows(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """CONTRACT: a job denied a reservation N times must not write O(N)
    durable rows.

    A denial is not work that happened — no handler ran, no attempt was
    consumed, no budget was spent.  It is backpressure.  Recording it as a
    full ``job_attempts`` row plus a full ``job_events`` row per poll turns a
    single logical unit of work into unbounded storage: a job denied by a
    saturated bucket for an hour at a one-second retry_after writes 7200 rows
    that describe nothing that happened.

    The desirable behaviour is that repeated denials COALESCE — a counter on
    the job row, or a single updated event — so the durable footprint of a
    denied job is O(1) in the number of denials.

    RED today: ``mark_snoozed`` writes one ``job_attempts`` row and one
    ``job_events`` row per cycle unconditionally.
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
        "one-per-poll — a denial records that nothing happened."
    )
    assert events_added <= _BOUNDED_ROW_BUDGET, (
        f"{_DENIAL_CYCLES} reservation denials wrote {events_added} job_events rows; "
        f"denials must coalesce to at most {_BOUNDED_ROW_BUDGET} durable rows, not grow "
        "one-per-poll — a denial records that nothing happened."
    )


def _make_backend(deps: WorkerDeps) -> PostgresBackend:
    return PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=_ZERO,
        cleanup_grace_period=_ZERO,
    )


# ── DEFECT 2 — the retry gap never closes ─────────────────────────────


async def test_denial_does_not_inflate_max_attempts_so_the_gap_closes(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """CONTRACT: ``max_attempts`` is a CEILING, not a tally.

    A denial legitimately should not consume retry budget — that is why
    ``mark_snoozed`` leaves ``attempt`` alone.  But it compensates with
    ``max_attempts = j.max_attempts + 1``, and dispatch DOES increment
    ``attempt`` on every re-pickup.  The two increments cancel, so the gap
    ``max_attempts - attempt`` is invariant and the job can never fail out:
    a permanently saturated bucket produces a job that retries forever while
    its ``max_attempts`` climbs without bound.

    The right shape is to leave ``max_attempts`` fixed at its configured
    ceiling and not count a denial as an attempt — then the gap closes as
    real attempts are consumed and retry exhaustion stays reachable.

    RED today: ``max_attempts`` grows by one per denial and the gap is frozen.
    """
    schema = settings.schema_name
    worker_settings = _build_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(worker_settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        worker_id = new_uuid()
        job_id = await _seed_denied_job(conn, schema, worker_id)

        row_before = await conn.fetchrow(
            f'SELECT attempt, max_attempts FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
        assert row_before is not None
        max_attempts_before: int = row_before["max_attempts"]
        gap_before: int = row_before["max_attempts"] - row_before["attempt"]

        async with open_worker_deps(worker_settings) as deps:
            backend = _make_backend(deps)
            await _drive_denial_loop(backend, conn, schema, job_id, worker_id)

        row_after = await conn.fetchrow(
            f'SELECT attempt, max_attempts FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
        assert row_after is not None
        max_attempts_after: int = row_after["max_attempts"]
        attempt_after: int = row_after["attempt"]
    finally:
        await conn.close()

    gap_after = max_attempts_after - attempt_after

    assert max_attempts_after == max_attempts_before, (
        f"max_attempts drifted {max_attempts_before} -> {max_attempts_after} across "
        f"{_DENIAL_CYCLES} denials; the configured ceiling must be immutable — inflating "
        "it to 'pay back' the attempt dispatch consumed makes the ceiling unreachable."
    )
    assert gap_after < gap_before, (
        f"retry gap (max_attempts - attempt) is invariant at {gap_before} across "
        f"{_DENIAL_CYCLES} denials; the failure gate is unreachable and the job retries "
        "forever against a saturated bucket."
    )


# ── DEFECT 2b — the rows are unreclaimable by prune ───────────────────


async def test_denial_rows_are_reclaimable_by_retention(
    pg_dsn: str,
    settings: TaskQSettings,
) -> None:
    """CONTRACT: rows written by a denial loop must be reclaimable.

    ``prune_terminal_jobs`` keys on ``status IN (terminal) AND finished_at <
    cutoff``.  Every snooze sets ``finished_at = NULL`` and the job never
    terminates (defect 2), so the prune sweep matches zero rows at ANY
    retention period — the accrued rows from defect 1 are permanently
    unreclaimable.  Either the rows must not accrue, or some sweep must be
    able to reach them.

    RED today: prune archives nothing and every accrued row survives.
    """
    schema = settings.schema_name
    worker_settings = _build_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(worker_settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        worker_id = new_uuid()
        job_id = await _seed_denied_job(conn, schema, worker_id)

        async with open_worker_deps(worker_settings) as deps:
            backend = _make_backend(deps)
            await _drive_denial_loop(backend, conn, schema, job_id, worker_id)

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
        assert events_before > 0, "denial loop wrote no events — test setup is wrong"

        await prune_terminal_jobs(
            conn,
            retention_per_status={
                "succeeded": timedelta(seconds=1),
                "failed": timedelta(seconds=1),
                "cancelled": timedelta(seconds=1),
                "crashed": timedelta(seconds=1),
                "abandoned": timedelta(seconds=1),
            },
            archive_retention=timedelta(days=365),
            batch_size=10_000,
            schema=schema,
        )

        events_after = await _count(conn, schema, "job_events", job_id)
    finally:
        await conn.close()

    assert events_after < events_before, (
        f"{events_before} job_events rows aged 400 days survived a 1-second retention "
        "prune untouched: the denial loop re-nulls finished_at and the job never reaches a "
        "terminal status, so prune_terminal_jobs' "
        "`status IN (terminal) AND finished_at < cutoff` predicate matches zero rows at "
        "ANY retention period. These rows are permanently unreclaimable."
    )


# ── DEFECT 3 — no age-based retention for job_events ──────────────────


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
    independent of parent terminality — while EXEMPTING the crash-reclaim
    outbox slice (see the companion pinning test below).

    RED today: no such mechanism exists anywhere in ``src/taskq``.  This test
    is expected to fail until one is added.
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
            "occurred_at alone — only (job_id, occurred_at) and the partial reclaim "
            "index — so such a sweep needs a supporting index."
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
    ``detail->>'reason' = 'lock_expired'`` are not history — they are the
    outbox ``poll_reclaim_events`` reads to drive ``TaskQ.watch_reclaims()``.
    ``poll_reclaim_events`` advances a trailing watermark over ``id``; if a
    retention sweep deletes an un-consumed row in that slice, the reclaim
    event is lost silently and permanently, and a crashed worker's job is
    never surfaced to any watcher.

    This test is the guard rail on the fix for defect 3: whatever sweep is
    added, an aged ``lock_expired`` row must survive it and must still be
    visible to ``poll_reclaim_events``.  It is GREEN today (no sweep deletes
    anything) and must stay green after the fix.
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
        "the aged lock_expired row is no longer visible to poll_reclaim_events — crash "
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
