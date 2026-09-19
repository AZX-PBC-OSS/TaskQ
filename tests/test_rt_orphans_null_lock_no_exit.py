# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team attacks on the no-exit cell: ``running`` x ``lock_expires_at IS NULL`` x dead holder.

Every maintenance path that can exit a ``running`` row is gated on something
this cell cannot satisfy:

* Sweep 1 (``_SWEEP_1_SQL``, src/taskq/backend/_sweeps.py:234-237) selects
  ``status = 'running' AND lock_expires_at < statement_timestamp()`` - a NULL
  lock makes the comparison NULL-false at every age, so the reclaim sweep is
  blind to the row forever. The in-memory twin shares the guard
  (src/taskq/testing/_sweeps.py:162-166, ``row.lock_expires_at is not None``).
* Phase-2 escalation is owner-worker-scoped (``locked_by_worker = $2 AND
  cancel_phase = 1`` - only the live holder can write phase 2; a dead holder
  never does), and ``mark_abandoned`` is phase-2-scoped
  (src/taskq/backend/_sql_templates.py:460), so the cancel protocol can land
  phase 1 and then stall forever.
* Every terminal write (``mark_succeeded`` / ``mark_failed_or_retry`` /
  ``mark_cancelled``) is owner-worker-scoped; ``cancel_pending_scheduled``
  requires pending/scheduled; ``retry_job`` requires a terminal status;
  ``isolate_self`` is owner-scoped.

Only direct SQL can plant the shape today (nothing detects or repairs it),
which makes it a defense-in-depth gap: the contract is that every non-terminal
row must have a reachable exit (or a detector that alarms on the shape).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobFilter, JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_running_job

_START = datetime(2025, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)


class _StubBackendDeps:
    """Minimal duck-typed BackendDeps: pools are set per-test, nothing else is read."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None


def _pool_backend(schema: str, pool: asyncpg.Pool) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema,
        },
        validate=False,
    )
    deps = _StubBackendDeps(settings)
    deps.worker_pool = pool
    deps.heartbeat_pool = pool
    deps.dispatcher_pool = pool
    return PostgresBackend(
        deps,  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; only settings + pools are read on the paths under test.
        clock=SystemClock(),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


# ── In-memory tier: the twin shares the NULL-blind guard ────────────────


async def test_in_memory_null_lock_running_row_has_no_reachable_exit() -> None:
    """A running row with a NULL lock and a dead holder must have a reachable
    exit (or an alarm). Today every exit the backend offers leaves it running.

    The row is planted exactly in the no-exit cell: status ``running``,
    ``locked_by_worker`` a UUID no live worker holds, ``lock_expires_at``
    None. Every exit the system offers runs, the clock advances a decade
    past every grace, and the row is still ``running``.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    dead_holder = new_uuid()
    row = replace(
        make_job_row(status="running"),
        locked_by_worker=dead_holder,
        lock_expires_at=None,
        schedule_to_close=None,
    )
    backend._jobs[row.id] = row
    job_id = row.id
    live_worker = new_uuid()

    exits: list[str] = []

    n = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    exits.append(f"reclaim_expired_locks={n}")
    n = await backend.deadline_sweep()
    exits.append(f"deadline_sweep={n}")
    n = await backend.scheduled_to_pending()
    exits.append(f"scheduled_to_pending={n}")

    ok = await backend.write_cancel_request(job_id, "orphan-probe")
    exits.append(f"write_cancel_request={ok}")
    bulk = await backend.cancel_where(JobFilter(status=["running"]), "orphan-probe")
    exits.append(
        f"cancel_where=(directly={bulk.cancelled_directly}, requested={bulk.cancel_requested})"
    )
    ok = await backend.write_cancel_escalation(job_id, live_worker, 2)
    exits.append(f"write_cancel_escalation(live worker)={ok}")
    ok = await backend.mark_abandoned(job_id)
    exits.append(f"mark_abandoned={ok}")
    ok = await backend.mark_cancelled(job_id, live_worker, attempt=1)
    exits.append(f"mark_cancelled(live worker)={ok}")
    ok = await backend.retry_job(job_id)
    exits.append(f"retry_job={ok}")

    # Advance the clock arbitrarily - a decade past every grace and margin.
    clock.advance(timedelta(days=3650))
    n = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    exits.append(f"reclaim_expired_locks(+10y)={n}")
    n = await backend.deadline_sweep()
    exits.append(f"deadline_sweep(+10y)={n}")
    n = await backend.scheduled_to_pending()
    exits.append(f"scheduled_to_pending(+10y)={n}")

    final = await backend.get(job_id)
    assert final is not None
    assert final.status != "running", (
        "Contract: every non-terminal job row must have a reachable exit (or a detector that "
        "alarms on the no-exit shape) - a running row whose lock never expires is unreclaimable "
        "forever. Current behavior violates it: after every exit the system offers "
        f"({'; '.join(exits)}) and a 10-year clock advance, the row is still "
        f"status={final.status!r} with dead holder locked_by_worker={final.locked_by_worker!r} "
        f"and lock_expires_at={final.lock_expires_at!r} - the twin's NULL guard "
        "(src/taskq/testing/_sweeps.py:164, `row.lock_expires_at is not None`) makes the reclaim "
        "sweep blind to it, and every other exit is owner-worker- or phase-2-scoped."
    )


# ── PG tier: the real sweep's NULL-false predicate ──────────────────────


@pytest.mark.integration
async def test_pg_null_lock_running_row_survives_every_sweep(pg_dsn: str) -> None:
    """On real Postgres, a running row whose lock is NULLed survives every
    sweep and every cancel entry point - while an identical expired-lock row
    is reclaimed, proving the sweep ran and the NULL comparison is the blind
    spot (``lock_expires_at < statement_timestamp()`` is NULL-false,
    src/taskq/backend/_sweeps.py:235).
    """
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        dead_holder = new_uuid()
        past = datetime.now(UTC) - timedelta(hours=1)
        # Control: same dead holder, expired NON-NULL lock - sweep 1 reclaims it.
        control_id: UUID = await create_running_job(
            conn,
            schema,
            dead_holder,
            lock_expires_at=past,
            max_attempts=1,
            retry_kind="transient",
            attempt=1,
            with_events=False,
        )
        # Victim: identical shape, then the lock column is NULLed (direct SQL -
        # the only writer that can plant the cell today).
        victim_id: UUID = await create_running_job(
            conn,
            schema,
            dead_holder,
            lock_expires_at=past,
            max_attempts=1,
            retry_kind="transient",
            attempt=1,
            with_events=False,
        )
        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = NULL WHERE id = $1', victim_id
        )

        reclaimed = await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)
        control_status = await conn.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1', control_id
        )
        assert reclaimed == 1, f"control row must prove sweep 1 ran; reclaimed={reclaimed}"
        assert control_status == "crashed"

        exits: list[str] = [
            f"sweep_expired_locks={reclaimed} (control only)",
            f"sweep_deadline_exceeded={await PostgresBackend.sweep_deadline_exceeded(conn, schema=schema)}",
            f"sweep_scheduled_to_pending={await PostgresBackend.sweep_scheduled_to_pending(conn, schema=schema)}",
        ]

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        backend = _pool_backend(schema, pool)
        ok = await backend.write_cancel_request(JobId(victim_id), "orphan-probe")
        exits.append(f"write_cancel_request={ok}")
        bulk = await backend.cancel_where(JobFilter(status=["running"]), "orphan-probe")
        exits.append(
            f"cancel_where=(directly={bulk.cancelled_directly}, requested={bulk.cancel_requested})"
        )
        ok = await backend.write_cancel_escalation(JobId(victim_id), new_uuid(), 2)
        exits.append(f"write_cancel_escalation(live worker)={ok}")
        ok = await backend.mark_abandoned(JobId(victim_id))
        exits.append(f"mark_abandoned={ok}")
        ok = await backend.retry_job(JobId(victim_id))
        exits.append(f"retry_job={ok}")

        # No clock advance needed: the server-side predicates use their own
        # clock; a NULL lock is NULL-false at every age.
        victim = await conn.fetchrow(
            f"SELECT status, locked_by_worker, lock_expires_at, cancel_phase "
            f'FROM "{schema}".jobs WHERE id = $1',
            victim_id,
        )
        assert victim is not None
        assert victim["status"] != "running", (
            "Contract: every non-terminal job row must have a reachable exit (or a detector "
            "that alarms on the no-exit shape). Current behavior violates it: the control row "
            f"with an expired NON-NULL lock was reclaimed ({control_status!r}) while the "
            "identical row with lock_expires_at = NULL survived every exit "
            f"({'; '.join(exits)}) - `lock_expires_at < statement_timestamp()` "
            "(src/taskq/backend/_sweeps.py:235) is NULL-false, so sweep 1 can never select it, "
            f"and the row is still status={victim['status']!r}, "
            f"locked_by_worker={victim['locked_by_worker']!r} (dead holder), "
            f"lock_expires_at={victim['lock_expires_at']!r}, "
            f"cancel_phase={victim['cancel_phase']!r}."
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
