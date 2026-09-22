# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""Red-team: a same-tick abandon whose heartbeat transaction rolls back.

When both cancel graces are already satisfied on the same tick, the
phase-2 arm runs inside the caller's still-open heartbeat transaction:
it writes ``cancel_phase = 2`` with its event, sets the entry to
``ABANDON_PENDING``, queues the abandon, and (before the fix) called
``task.cancel()``. The cancelled consumer re-raises without a terminal
write (its ``ABANDON_PENDING`` guard) and its unconditional ``finally``
deregisters the job. If that tick's transaction then fails - another
job's statement in the same tick, or the COMMIT itself - Postgres still
reads ``cancel_phase = 1``, ``run_post_tx``'s ``mark_abandoned`` cannot
match (its guard is ``cancel_phase = 2 OR lock_expires_at IS NULL``),
and the not-applied False arm's re-arm found no entry to re-arm: the
consumer had already popped it. Every later tick's ladder iterates a
registry that no longer holds the job, the heartbeat keeps renewing the
lease (only ``disowned_jobs`` is excluded, and no terminal write failed),
and the row is stuck ``running`` at phase 1, unreachable by any cancel
or abandon path, for as long as the holding worker lives.

The contract the module's own comments state (the re-issue arm is the
recovery for a rolled-back phase-2 write; a not-applied abandon "leaves
the job registered and its phase back at FORCED, so a later tick can
re-issue the escalation") must be the contract the code enforces. The
fix defers the cancellation to ``run_post_tx``, delivered only after
``mark_abandoned`` has made the abandon durable: a rollback now leaves
the entry registered, the handler still running, and the False arm
re-arms it at FORCED for a later tick's re-issue.

The consumer here is a faithful model of the production unwinding
(_consumer.py): on CancelledError, an entry at ``ABANDON_PENDING`` or
beyond re-raises without a terminal write, and the unconditional
``finally`` deregisters.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
import structlog
from pydantic import BaseModel

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.constants import CANCEL_ORIGIN_ABANDONED
from taskq.context import JobContext
from taskq.migrate import apply_pending
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_running_job, create_worker, seed_actors
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool

pytestmark = pytest.mark.integration

# Both graces at zero make the same-tick fast path fire on the very tick
# the cancel is observed: the configuration the fast path's own comment
# calls anticipated.
_CANCEL_GRACE = timedelta(seconds=0)
_CLEANUP_GRACE = timedelta(seconds=0)
# Bounds every wait in this file so a wedged interleaving fails, never hangs.
_WAIT = 15.0


class _SimulatedSiblingError(Exception):
    """Another job's statement failed inside the same heartbeat tick."""


class _StubPayload(BaseModel):
    """Minimal payload for a cancel-path JobContext."""


class _BackendDepsShim:
    """The pools PostgresBackend accesses, on the module's own DSN."""

    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool


@pytest_asyncio.fixture(scope="module")
async def rt_schema(pg_dsn: str) -> AsyncIterator[tuple[str, str]]:
    """A random migrated schema on this module's database: ``(schema, dsn)``."""
    schema = f"tsa_{new_base62()}".lower()
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


def _make_ctx(job_id: UUID, worker_id: UUID) -> JobContext[BaseModel]:
    return JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        claim_epoch=0,
        worker_id=worker_id,
        payload=_StubPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


async def _row(conn: asyncpg.Connection, schema: str, job_id: UUID) -> dict[str, object]:
    rows = await conn.fetch(
        f"SELECT status::text AS status, cancel_phase, locked_by_worker "
        f'FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert len(rows) == 1
    return dict(rows[0])


async def test_same_tick_abandon_rollback_leaves_job_recoverable(
    rt_schema: tuple[str, str],
) -> None:
    """The rollback of a same-tick abandon's tick must leave the job
    registered and re-armed, so a later tick can re-issue the escalation
    and the abandon lands. The old behavior stranded the row running at
    cancel_phase 1 with the entry gone from the registry: no later tick
    could reach it."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    deps = _deps_for(dsn, schema)
    backend = PostgresBackend(
        _BackendDepsShim(deps.settings, pool),  # type: ignore[arg-type]
        SystemClock(),
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
    )
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]

    stopped = asyncio.Event()

    async def _consumer_body() -> None:
        """The production unwinding's shape (_consumer.py): on
        CancelledError, an entry at ABANDON_PENDING or beyond re-raises
        without a terminal write, and the unconditional finally
        deregisters."""
        try:
            await stopped.wait()
        except asyncio.CancelledError:
            entry = deps.active_jobs.get(job_id)
            if entry is not None and entry.cancel_phase >= CancelPhase.ABANDON_PENDING:
                await deps.active_jobs.deregister(job_id)
            raise

    consumer_task = asyncio.get_running_loop().create_task(_consumer_body())

    conn = await asyncpg.connect(dsn)
    try:
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
    finally:
        await conn.close()

    try:
        await deps.active_jobs.register(job_id, consumer_task, _make_ctx(job_id, worker_id))

        # ── Tick 1: the same-tick abandon fires, then the tx rolls back. ──
        conn1 = await asyncpg.connect(dsn)
        tx = conn1.transaction()
        try:
            await tx.start()
            await controller.run_in_tx(conn1)  # type: ignore[arg-type]
            # The consumer's unwind overlaps the rest of the tick in
            # production; give the cancelled task its scheduling slices.
            await asyncio.wait([consumer_task], timeout=0.5)
            # A sibling job's statement fails inside the same tick: the
            # transaction dies and every write in it rolls back.
            raise _SimulatedSiblingError
        except _SimulatedSiblingError:
            await tx.rollback()
        finally:
            # The heartbeat's finally: run_post_tx follows run_in_tx on
            # every tick, rollback included.
            await controller.run_post_tx()
            await conn1.close()

        entry = deps.active_jobs.get(job_id)
        assert entry is not None, (
            "Contract (cancel.py, run_post_tx): an abandon that did NOT "
            "apply leaves the job registered so a later tick can re-issue "
            "the escalation. The consumer's unconditional finally had "
            "already popped the entry the re-issue arm needs: every later "
            "tick's ladder iterated a registry that no longer held the "
            "job, and the row was unreachable by any cancel or abandon "
            "path."
        )
        assert entry.cancel_phase == CancelPhase.FORCED, (
            "Contract: the not-applied abandon re-arms the entry at FORCED "
            "for a later tick's re-issue."
        )
        conn = await asyncpg.connect(dsn)
        try:
            row = await _row(conn, schema, job_id)
        finally:
            await conn.close()
        assert row["status"] == "running"
        assert row["cancel_phase"] == 1, (
            "The escalation write rolled back with its transaction: the "
            "row must still read phase 1, the state the re-issue arm "
            "matches."
        )
        assert row["locked_by_worker"] == worker_id
        # The heartbeat keeps renewing this lease: nothing failed a
        # terminal write, so nothing disowned the row. That is exactly
        # why a stranded row is invisible to the reclaim sweep.
        assert job_id not in deps.disowned_jobs

        # ── Tick 2: a healthy, committing tick re-issues and lands. ──
        conn2 = await asyncpg.connect(dsn)
        try:
            async with conn2.transaction():
                await controller.run_in_tx(conn2)  # type: ignore[arg-type]
            await controller.run_post_tx()
        finally:
            await conn2.close()

        conn = await asyncpg.connect(dsn)
        try:
            row = await _row(conn, schema, job_id)
            attempts = await conn.fetch(
                f'SELECT outcome, error_class FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
        finally:
            await conn.close()
        assert row["status"] == "abandoned", (
            f"The re-issued abandon must land: the row reads "
            f"{row['status']!r} at cancel_phase {row['cancel_phase']}, the "
            f"old behavior never re-issued and the job stayed running, "
            f"renewed, unreachable, for as long as the worker lived."
        )
        assert row["cancel_phase"] == 2
        assert len(attempts) == 1
        assert attempts[0]["error_class"] == CANCEL_ORIGIN_ABANDONED
        assert deps.active_jobs.get(job_id) is None
        # The drain delivered the deferred cancellation: the consumer
        # unwound through its ABANDON_PENDING guard (no terminal write)
        # and is done.
        await asyncio.wait([consumer_task], timeout=_WAIT)
        assert consumer_task.done()
    finally:
        stopped.set()
        if not consumer_task.done():
            consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        await pool.close()
