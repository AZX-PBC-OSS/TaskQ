# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""The shutdown window's operator-verdict contract, proven on real PG (#596).

The ownership contract: an operator's cancel verdict is ``cancelled``
(forced) - every writer that can terminalise a running, cancel-flagged
row DURING the shutdown orchestration writes the SAME attempt-fenced
verdict, so a race between them dissolves instead of letting the
host's weather pick the row's audit trail. The writers, named:

- the unwinding consumer's ``mark_cancelled`` (worker+attempt+epoch fence),
- the heartbeat ladder's phase-3 drain for a HELD entry (``mark_cancelled``
  while the orchestration is active - the #596 swap),
- RELEASING's OPERATOR arm and its noop-fence arm (both ``mark_cancelled``),
- the drain's UNHELD orphan class (``mark_abandoned`` - no entry, no
  consumer, the ladder is the row's only terminal writer).

The stub-level pins (``test_rt_cancelwatch_abandon_drain.py``,
``test_shutdown_orchestrator.py``) prove WHICH write the drain issues;
the system tier proves the whole scenario. This file proves the fence
arithmetic at the SQL boundary on real Postgres: both race orders land
``cancelled``/``CancelledForced`` with exactly one attempt row and no
verdict relabelling, the UNHELD class is the shutdown-window path that
legitimately produces ``abandoned``, and the non-shutdown holder-ignored
expiry keeps the ladder's own ``mark_abandoned`` terminal.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_running_job, create_worker, seed_actors
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import ShutdownPhase
from tests._cancel_ctx import make_ctx
from tests.conftest import _FakePool

pytestmark = pytest.mark.integration

_CANCEL_GRACE = 0.0
_CLEANUP_GRACE = 0.0


class _BackendDepsShim:
    """The pools PostgresBackend accesses, on the module's own DSN."""

    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool


def _deps_for(dsn: str, schema: str) -> WorkerDeps:
    return WorkerDeps(  # type: ignore[call-arg]
        settings=WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_LOCK_LEASE": "360",
                "TASKQ_TERMINATION_GRACE_PERIOD": "360",
                "TASKQ_CANCELLATION_GRACE_PERIOD": "0",
                "TASKQ_CLEANUP_GRACE_PERIOD": "0",
            }
        ),
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


def _sleeper() -> asyncio.Task[object]:
    return asyncio.get_running_loop().create_task(asyncio.sleep(3600))


async def _tick(deps: WorkerDeps, backend: PostgresBackend, dsn: str, worker_id: UUID) -> None:
    controller = make_cancel_controller(deps, worker_id, backend)
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            await controller.run_in_tx(conn)  # type: ignore[arg-type]
        await controller.run_post_tx()
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module")
async def rt_schema(pg_dsn: str) -> AsyncIterator[tuple[str, str]]:
    """A random migrated schema on this module's database: ``(schema, dsn)``."""
    schema = f"tcv_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    yield schema, pg_dsn
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def _seed_phase2_row(
    pool: asyncpg.Pool,
    dsn: str,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
) -> PostgresBackend:
    """Seed actors + worker + a running row escalated to cancel_phase 2."""
    backend = PostgresBackend(
        _BackendDepsShim(_deps_for(dsn, schema).settings, pool),  # type: ignore[arg-type]
        SystemClock(),
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
    )
    async with pool.acquire() as conn:
        await seed_actors(conn, schema)
        await create_worker(conn, schema, worker_id)
        await create_running_job(
            conn,
            schema,
            worker_id,
            job_id,
            cancel_phase=1,
            cancel_requested_at=datetime.now(UTC),
        )
        escalated = await backend.write_cancel_escalation(job_id, worker_id, phase=2)
        assert escalated is True, "fixture broken: the phase-2 escalation did not land"
    return backend


async def _row(pool: asyncpg.Pool, schema: str, job_id: UUID) -> dict[str, object]:
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            f"SELECT status::text AS status, error_class::text AS error_class, "
            f"locked_by_worker, cancel_phase, cancel_requested_at "
            f'FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert rec is not None
    return dict(rec)


async def _attempt_rows(pool: asyncpg.Pool, schema: str, job_id: UUID) -> list[dict[str, object]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT attempt, outcome, error_class::text AS error_class "
            f'FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
            job_id,
        )
    return [dict(r) for r in rows]


async def _reap(task: asyncio.Task[object]) -> None:
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_drain_held_abandon_under_shutdown_lands_the_operators_verdict(
    rt_schema: tuple[str, str],
) -> None:
    """Drain-first race order: the shutdown-active drain's HELD abandon must
    land ``cancelled`` (forced), and the unwinding consumer's racing write
    must lose to the fence without relabelling the row."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    task = _sleeper()
    try:
        backend = await _seed_phase2_row(pool, dsn, schema, worker_id, job_id)
        deps = _deps_for(dsn, schema)
        deps.shutdown_phase = ShutdownPhase.RELEASING
        await deps.active_jobs.register(
            job_id, task, make_ctx(job_id, worker_id, attempt=1, claim_epoch=1)
        )
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        # The escalation was observed on an earlier tick (local FORCED, the
        # cancellation delivered); the abandon comes due now, while the row
        # is still running - the consumer's unwind has not committed yet.
        entry.cancel_phase = CancelPhase.FORCED
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 1.0

        await _tick(deps, backend, dsn, worker_id)

        row = await _row(pool, schema, job_id)
        assert row["status"] == "cancelled", (
            "Contract: the drain's held abandon under the orchestration lands "
            "the operator's OWN verdict (mark_cancelled), never 'abandoned'"
        )
        assert row["error_class"] == "CancelledForced", (
            "The verdict is FORCED: the row carried cancel_phase=2, the "
            "mark_cancelled stamp must read the escalation, not a "
            "cooperative yield"
        )
        assert row["locked_by_worker"] is None

        # The consumer's racing write (same fence) loses gracefully: the row
        # is already terminal, the write no-ops, nothing is relabelled and
        # no second attempt row appears.
        consumer_landed = await backend.mark_cancelled(job_id, worker_id, attempt=1, claim_epoch=1)
        assert consumer_landed is False
        row = await _row(pool, schema, job_id)
        assert row["status"] == "cancelled"
        assert row["error_class"] == "CancelledForced"

        attempts = await _attempt_rows(pool, schema, job_id)
        assert len(attempts) == 1, (
            "Exactly one attempt row: the loser's fenced write must not "
            "mint a duplicate attempt record"
        )
        assert attempts[0]["outcome"] == "cancelled"
        assert attempts[0]["error_class"] == "CancelledForced"
        assert deps.active_jobs.get(job_id) is None, (
            "The applied verdict's delivery deregisters the held entry"
        )
    finally:
        await _reap(task)
        await pool.close()


async def test_consumer_verdict_first_drain_noops_and_still_delivers(
    rt_schema: tuple[str, str],
) -> None:
    """Consumer-first race order: the consumer's own mark_cancelled commits
    while the drain's write is in flight, the drain's write loses to the
    fence, and the drain still completes the cancellation delivery +
    deregistration via the not-applied re-read."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    task = _sleeper()
    try:
        backend = await _seed_phase2_row(pool, dsn, schema, worker_id, job_id)

        class _ConsumerCommitsFirst(PostgresBackend):
            """Realises the interleaving: the consumer's fenced write commits
            in front of the drain's, inside the drain's own write call."""

            def __init__(self) -> None:
                self._consumer_landed = False

            async def mark_cancelled(  # type: ignore[override]
                self, job_id: object, worker_id: object, **kwargs: object
            ) -> bool:
                if not self._consumer_landed:
                    self._consumer_landed = True
                    # The unwinding consumer's write: same fence, commits FIRST.
                    await super().mark_cancelled(job_id, worker_id, attempt=1, claim_epoch=1)
                return await super().mark_cancelled(job_id, worker_id, **kwargs)  # type: ignore[arg-type]

        racing = _ConsumerCommitsFirst()
        racing.__dict__.update(backend.__dict__)
        racing._consumer_landed = False

        deps = _deps_for(dsn, schema)
        deps.shutdown_phase = ShutdownPhase.RELEASING
        await deps.active_jobs.register(
            job_id, task, make_ctx(job_id, worker_id, attempt=1, claim_epoch=1)
        )
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.FORCED
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 1.0

        await _tick(deps, racing, dsn, worker_id)

        row = await _row(pool, schema, job_id)
        assert row["status"] == "cancelled", (
            "The consumer's verdict owns the row; the drain's lost write must not relabel it"
        )
        assert row["error_class"] == "CancelledForced"
        attempts = await _attempt_rows(pool, schema, job_id)
        assert len(attempts) == 1, "The drain's fenced-out write must not mint a second attempt row"
        assert deps.active_jobs.get(job_id) is None, (
            "The not-applied arm's re-read sees the durable verdict and "
            "completes the delivery the consumer's exit did not: the entry "
            "must be deregistered, not stranded"
        )
    finally:
        await _reap(task)
        await pool.close()


async def test_unheld_orphan_under_shutdown_keeps_the_ladders_terminal(
    rt_schema: tuple[str, str],
) -> None:
    """The UNHELD orphan class is the shutdown-window path that legitimately
    produces ``abandoned``: no registry entry, no consumer, no other writer -
    the ladder is the row's only terminal writer."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        backend = await _seed_phase2_row(pool, dsn, schema, worker_id, job_id)
        deps = _deps_for(dsn, schema)
        deps.shutdown_phase = ShutdownPhase.RELEASING
        # NO registry entry: the row is the unheld walk's orphan.

        await _tick(deps, backend, dsn, worker_id)

        row = await _row(pool, schema, job_id)
        assert row["status"] == "abandoned", (
            "Contract: the UNHELD orphan keeps mark_abandoned even under the "
            "orchestration - there is no consumer unwinding toward a "
            "different verdict, so no race exists to dissolve"
        )
        assert row["error_class"] == "CancelAbandoned"
    finally:
        await pool.close()


async def test_non_shutdown_held_expiry_keeps_mark_abandoned(
    rt_schema: tuple[str, str],
) -> None:
    """Away from shutdown, the HELD abandon keeps the ladder's own
    ``mark_abandoned`` terminal: the holder-ignored-the-cancel expiry."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    task = _sleeper()
    try:
        backend = await _seed_phase2_row(pool, dsn, schema, worker_id, job_id)
        deps = _deps_for(dsn, schema)
        assert deps.shutdown_phase is ShutdownPhase.NONE
        await deps.active_jobs.register(
            job_id, task, make_ctx(job_id, worker_id, attempt=1, claim_epoch=1)
        )
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        # The escalation was observed on an earlier tick; the holder ignored
        # both the cooperative signal and the forced cancellation, and the
        # abandon now comes due while the worker runs normally.
        entry.cancel_phase = CancelPhase.FORCED
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 1.0

        await _tick(deps, backend, dsn, worker_id)

        row = await _row(pool, schema, job_id)
        assert row["status"] == "abandoned", (
            "Contract: the holder-ignored expiry stays the ladder's own "
            "mark_abandoned terminal while the worker runs normally"
        )
        assert deps.active_jobs.get(job_id) is None
    finally:
        await _reap(task)
        await pool.close()


async def test_first_sight_phase2_applies_phase1_effects(
    rt_schema: tuple[str, str],
) -> None:
    """Escalation correctness: a row first observed at PG phase 2 applies
    phase-1's effects (cancel_event, origin stamp, observation stamp) in the
    SAME tick it fast-advances the local phase to FORCED - phase 2 never
    skips phase 1."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    task = _sleeper()
    try:
        backend = await _seed_phase2_row(pool, dsn, schema, worker_id, job_id)
        deps = _deps_for(dsn, schema)
        await deps.active_jobs.register(
            job_id, task, make_ctx(job_id, worker_id, attempt=1, claim_epoch=1)
        )
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        assert entry.cancel_phase is CancelPhase.NONE
        assert entry.cancel_observed_at is None

        await _tick(deps, backend, dsn, worker_id)

        assert entry.cancel_phase is CancelPhase.FORCED, (
            "The fast-advance arm moved the local phase to FORCED"
        )
        assert entry.cancel_observed_at is not None, (
            "Phase-1's observation stamp was applied by the phase-1 arm in "
            "the same tick - without it the phase-3 graces never start"
        )
        assert entry.ctx.cancel_event.is_set(), (
            "Phase-1's cooperative signal was applied: phase 2 must never skip phase 1's effects"
        )
        assert entry.cancel_origin.name == "OPERATOR"
    finally:
        await _reap(task)
        await pool.close()
