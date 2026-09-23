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
from taskq.backend._protocol import CancelPhase, JobId
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


async def _attempt_rows(
    conn: asyncpg.Connection, schema: str, job_id: UUID
) -> list[asyncpg.Record]:
    return await conn.fetch(  # pyright: ignore[reportUnknownVariableType]  # Why: conn type is the concrete asyncpg connection here.
        f'SELECT attempt, outcome, error_class FROM "{schema}".job_attempts WHERE job_id = $1 '
        "ORDER BY attempt",
        job_id,
    )


async def _wait_row_status(
    conn: asyncpg.Connection, schema: str, job_id: UUID, status: str
) -> dict[str, object]:
    """Poll the row until it reads ``status``; fails on the _WAIT bound, never hangs."""
    deadline = asyncio.get_running_loop().time() + _WAIT
    while True:
        row = await _row(conn, schema, job_id)
        if row["status"] == status:
            return row
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"row never reached {status!r}: last {row!r}")
        await asyncio.sleep(0.01)


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
                await deps.active_jobs.deregister(job_id, entry)
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


async def test_job_succeeding_in_delivery_window_abandon_noops(
    rt_schema: tuple[str, str],
) -> None:
    """The delivery window attack: the abandon's cancellation is deferred
    to ``run_post_tx``, so a job whose consumer finishes NATURALLY between
    the queueing and the drain must not be abandoned and must not take a
    cancellation. ``mark_abandoned``'s guard (``status = 'running'``)
    cannot match a succeeded row - the abandon no-ops cleanly, the row
    keeps its honest terminal state, and the ledger carries the success,
    not an ``abandoned`` stamp over a completed body."""
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

    release = asyncio.Event()
    tick_done = asyncio.Event()

    async def _consumer_body() -> None:
        # The natural-completion tail, modelled on _consumer.py: the
        # body's mark_succeeded commits INSIDE the delivery window (after
        # the same-tick arm queued the abandon, before the drain runs),
        # then the consumer still owes its publish/deregister unwinding
        # (it has NOT finished: the task is live and cancellable while
        # the drain runs).
        await tick_done.wait()
        landed = await backend.mark_succeeded(job_id, worker_id, None, attempt=1, claim_epoch=1)
        assert landed, "fixture broken: the success write must land on the seeded running row"
        await release.wait()

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

        # The same-tick abandon arm fires and the tick COMMITS: the row
        # reads cancel_phase 2, the abandon is queued, the cancellation is
        # NOT delivered (the fix defers it to the drain). The consumer is
        # parked on tick_done for the arm's whole run.
        conn1 = await asyncpg.connect(dsn)
        try:
            async with conn1.transaction():
                await controller.run_in_tx(conn1)  # type: ignore[arg-type]
        finally:
            await conn1.close()
        entry_after_arm = deps.active_jobs.get(job_id)
        assert entry_after_arm is not None and (
            entry_after_arm.cancel_phase == CancelPhase.ABANDON_PENDING
        ), "fixture broken: the same-tick arm must have queued the abandon"
        tick_done.set()

        # THE DELIVERY WINDOW: the consumer finishes naturally before the
        # drain runs. Wait for the success write to be durable.
        conn = await asyncpg.connect(dsn)
        try:
            await _wait_row_status(conn, schema, job_id, "succeeded")
        finally:
            await conn.close()

        await controller.run_post_tx()

        conn = await asyncpg.connect(dsn)
        try:
            row = await _row(conn, schema, job_id)
            attempts = await _attempt_rows(conn, schema, job_id)
        finally:
            await conn.close()
        assert row["status"] == "succeeded", (
            f"mark_abandoned's guard is status = 'running': a job that "
            f"terminalised first must keep its succeeded state, the drain "
            f"must not stamp 'abandoned' over it (got {row['status']!r})"
        )
        assert len(attempts) == 1, (
            f"exactly the success attempt row may exist, got "
            f"{[dict(r) for r in attempts]}: a second (abandoned) row "
            f"means the abandon applied over a succeeded job"
        )
        assert attempts[0]["outcome"] == "succeeded"
        assert consumer_task.cancelling() == 0, (
            "the drain must not deliver a cancellation on the not-applied "
            "arm: the job finished naturally, a cancel here is a stray "
            "delivery the delivery oracle forbids"
        )
        entry = deps.active_jobs.get(job_id)
        assert entry is not None, (
            "the not-applied arm leaves the entry registered (re-armed at "
            "FORCED); the consumer's own finally owns the deregister"
        )
        assert entry is not None and entry.cancel_phase == CancelPhase.FORCED
    finally:
        release.set()
        if not consumer_task.done():
            consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        await pool.close()


async def test_drain_delivers_no_second_cancel_to_already_cancelling_task(
    rt_schema: tuple[str, str],
) -> None:
    """The first-delivery-only pin: the staggered path's phase-2 arm
    cancelled the task a tick before the drain (``cancelling() == 1``);
    the drain's applied abandon must NOT deliver a second cancellation -
    the re-delivered CancelledError would interrupt the consumer's own
    unwinding mid-flight."""
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
    unwind_park = asyncio.Event()

    async def _consumer_body() -> None:
        try:
            await stopped.wait()
        except asyncio.CancelledError:
            # Mid-unwind park: the consumer caught the phase-2 arm's
            # cancellation and is unwinding - exactly the state the
            # drain's first-delivery-only guard exists for.
            await unwind_park.wait()
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
            cancel_phase=2,
            cancel_requested_at=datetime.now(UTC),
        )
    finally:
        await conn.close()

    try:
        await deps.active_jobs.register(job_id, consumer_task, _make_ctx(job_id, worker_id))
        # The staggered path's phase-2 arm: FORCED locally, the task
        # cancelled a tick before the drain, the observation timestamped
        # then too (the phase-3 arm's elapsed source).
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.FORCED
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 1000.0
        consumer_task.cancel()
        for _ in range(5):
            await asyncio.sleep(0)
        assert consumer_task.cancelling() == 1, (
            "fixture broken: the task must be mid-unwind (cancelling once) when the drain runs"
        )

        # The phase-3 arm (the staggered path's own queueing arm: local
        # FORCED, db FORCED, both graces elapsed) queues the abandon; the
        # commit is what makes the row read phase 2 for the drain.
        conn1 = await asyncpg.connect(dsn)
        try:
            async with conn1.transaction():
                await controller.run_in_tx(conn1)  # type: ignore[arg-type]
        finally:
            await conn1.close()

        # The abandon applies, the guard must take no second delivery.
        await controller.run_post_tx()

        assert consumer_task.cancelling() == 1, (
            f"the drain re-cancelled a task that was already cancelling "
            f"(cancelling() == {consumer_task.cancelling()}): the "
            f"first-delivery-only guard is load-bearing, a re-delivered "
            f"CancelledError interrupts the consumer's own unwinding"
        )
        conn = await asyncpg.connect(dsn)
        try:
            row = await _row(conn, schema, job_id)
            attempts = await _attempt_rows(conn, schema, job_id)
        finally:
            await conn.close()
        assert row["status"] == "abandoned"
        assert len(attempts) == 1, (
            f"one applied abandon writes exactly one attempt row, got {[dict(r) for r in attempts]}"
        )
        assert attempts[0]["error_class"] == CANCEL_ORIGIN_ABANDONED
        assert deps.active_jobs.get(job_id) is None, (
            "an applied abandon deregisters the entry: the drain owns the "
            "deregister once the abandon write is durable"
        )
    finally:
        stopped.set()
        unwind_park.set()
        if not consumer_task.done():
            consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        await pool.close()


async def test_abandon_raise_requeue_then_landing_is_idempotent(
    rt_schema: tuple[str, str],
) -> None:
    """The reachable double-delivery attack: a drain whose
    ``mark_abandoned`` write RAISES re-queues the entry at the head and
    propagates; the next tick's drain re-issues it. The re-issued abandon
    must land exactly once - one attempt row, one cancellation delivery -
    and a late duplicate of the applied abandon (the detached shield's
    write racing the re-issue) is absorbed by mark_abandoned's
    ``status = 'running'`` guard, not written twice."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    deps = _deps_for(dsn, schema)

    class _RaiseOnceBackend(PostgresBackend):
        """The pool death the re-queue arm exists for: the first
        mark_abandoned never lands, the second does."""

        def __init__(self) -> None:
            super().__init__(
                _BackendDepsShim(deps.settings, pool),  # type: ignore[arg-type]
                SystemClock(),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
            )
            self.raised = False

        async def mark_abandoned(  # type: ignore[override]  # Why: signature narrowing is not introduced; only the first call's outcome changes.
            self,
            job_id: JobId,
            progress_seq: int = 0,
            progress_state: dict[str, object] | None = None,
        ) -> bool:
            if not self.raised:
                self.raised = True
                raise _SimulatedSiblingError
            return await super().mark_abandoned(job_id, progress_seq, progress_state)

    backend = _RaiseOnceBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]

    stopped = asyncio.Event()

    async def _consumer_body() -> None:
        try:
            await stopped.wait()
        except asyncio.CancelledError:
            entry = deps.active_jobs.get(job_id)
            if entry is not None and entry.cancel_phase >= CancelPhase.ABANDON_PENDING:
                await deps.active_jobs.deregister(job_id, entry)
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
            cancel_phase=2,
            cancel_requested_at=datetime.now(UTC),
        )
    finally:
        await conn.close()

    try:
        await deps.active_jobs.register(job_id, consumer_task, _make_ctx(job_id, worker_id))
        # The staggered path's shape: the phase-2 arm is done (the row
        # reads phase 2), the entry is FORCED with its observation
        # timestamped; the phase-3 arm below is what queues the abandon.
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.FORCED
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 1000.0

        # Tick 1: the phase-3 arm queues the abandon, the tick commits,
        # and the drain's abandon write dies (pool death): the entry is
        # re-queued at the head and the exception propagates to the
        # heartbeat loop's failure accounting.
        conn1 = await asyncpg.connect(dsn)
        try:
            async with conn1.transaction():
                await controller.run_in_tx(conn1)  # type: ignore[arg-type]
        finally:
            await conn1.close()
        with contextlib.suppress(_SimulatedSiblingError):
            await controller.run_post_tx()
        assert backend.raised
        entry_after_raise = deps.active_jobs.get(job_id)
        assert entry_after_raise is not None, (
            "the raise path must leave the entry registered: the re-queue "
            "hands the abandon to the next tick's drain"
        )
        assert consumer_task.cancelling() == 0, (
            "no cancellation may be delivered for an abandon whose write did not land"
        )

        # Tick 2: the re-issued abandon lands, once.
        await controller.run_post_tx()

        conn = await asyncpg.connect(dsn)
        try:
            row = await _row(conn, schema, job_id)
            attempts = await _attempt_rows(conn, schema, job_id)
        finally:
            await conn.close()
        assert row["status"] == "abandoned"
        assert row["cancel_phase"] == 2
        assert len(attempts) == 1, (
            f"the re-issued abandon must land exactly once, got "
            f"{len(attempts)} attempt rows: {[dict(r) for r in attempts]}"
        )
        assert attempts[0]["error_class"] == CANCEL_ORIGIN_ABANDONED
        assert consumer_task.cancelling() == 1, (
            f"exactly one cancellation delivery across the raise and the "
            f"re-issue, got cancelling() == {consumer_task.cancelling()}"
        )
        assert deps.active_jobs.get(job_id) is None

        # A late duplicate of the applied abandon (the detached shield's
        # write landing after the budget cut that re-queued the entry) is
        # absorbed by the guard: no second attempt row, no state change.
        applied_again = await backend.mark_abandoned(job_id)
        assert applied_again is False
        conn = await asyncpg.connect(dsn)
        try:
            attempts_after = await _attempt_rows(conn, schema, job_id)
        finally:
            await conn.close()
        assert len(attempts_after) == 1
    finally:
        stopped.set()
        if not consumer_task.done():
            consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        await pool.close()


async def test_budget_cut_drain_whose_detached_write_lands_still_delivers(
    rt_schema: tuple[str, str],
) -> None:
    """A tick's leftover command budget cutting the drain mid-write must
    not leave the cancellation undelivered.

    ``run_post_tx`` awaits ``mark_abandoned`` under
    ``shield_with_retrieval``, so the heartbeat's ``asyncio.timeout`` cut
    detaches the inner write instead of killing it: the write lands
    anyway, while the drain's except arm re-queues the entry on the
    assumption the write did not land. The next tick's drain then reads
    ``False`` (the row is no longer ``running``), and the not-applied arm
    re-arms at FORCED without delivering the cancellation: every later
    tick's poll filters ``status = 'running'``, so ``db_phase`` reads
    NONE forever and no ladder arm can match again. The handler keeps
    running and its slot is gone until the worker isolates itself or
    exits.

    The contract: a cut drain's False must be disambiguated against the
    row. An abandon that is already durable owns the terminal state, so
    the drain completes the delivery (the same first-delivery-only shape
    as the applied arm) instead of leaving the cancellation silently
    undelivered; every other False cause keeps the re-arm semantics."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    deps = _deps_for(dsn, schema)

    class _ParkedWriteBackend(PostgresBackend):
        """The budget cut racing the abandon write: the drain's outer
        await is cut while ``mark_abandoned`` is parked mid-flight, and
        the shield detaches the write, which then commits."""

        def __init__(self) -> None:
            super().__init__(
                _BackendDepsShim(deps.settings, pool),  # type: ignore[arg-type]
                SystemClock(),
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
            )
            self.write_started = asyncio.Event()
            self.release_write = asyncio.Event()

        async def mark_abandoned(  # type: ignore[override]  # Why: signature narrowing is not introduced; only the first call's timing changes.
            self,
            job_id: JobId,
            progress_seq: int = 0,
            progress_state: dict[str, object] | None = None,
        ) -> bool:
            self.write_started.set()
            await self.release_write.wait()
            return await super().mark_abandoned(job_id, progress_seq, progress_state)

    backend = _ParkedWriteBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]

    stopped = asyncio.Event()

    async def _consumer_body() -> None:
        try:
            await stopped.wait()
        except asyncio.CancelledError:
            entry = deps.active_jobs.get(job_id)
            if entry is not None and entry.cancel_phase >= CancelPhase.ABANDON_PENDING:
                await deps.active_jobs.deregister(job_id, registered_entry)
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
        registered_entry = await deps.active_jobs.register(
            job_id, consumer_task, _make_ctx(job_id, worker_id)
        )

        # Tick 1: the same-tick arm escalates the row to phase 2, queues
        # the abandon WITHOUT cancelling (the deferred delivery), and the
        # tick commits. The drain is then cut by the tick's leftover
        # budget while the shielded write is parked mid-flight.
        conn1 = await asyncpg.connect(dsn)
        try:
            async with conn1.transaction():
                await controller.run_in_tx(conn1)  # type: ignore[arg-type]
        finally:
            await conn1.close()

        cut_finished = asyncio.Event()

        async def _cut_tick() -> None:
            # The heartbeat's post-tx shape: the drain runs under the
            # tick budget's leftover asyncio.timeout, and an expiry
            # surfaces as TimeoutError at the async-with.
            try:
                async with asyncio.timeout(0.2):
                    await controller.run_post_tx()
            except TimeoutError:
                pass
            finally:
                cut_finished.set()

        asyncio.get_running_loop().create_task(_cut_tick())
        await asyncio.wait_for(backend.write_started.wait(), _WAIT)
        await cut_finished.wait()
        # The cut re-queued the entry on the assumption the write did not
        # land; the shield's detached write is still parked. Release it
        # and watch the abandon commit anyway.
        backend.release_write.set()
        conn = await asyncpg.connect(dsn)
        try:
            row = await _wait_row_status(conn, schema, job_id, "abandoned")
        finally:
            await conn.close()
        assert row["cancel_phase"] == 2
        entry_after_cut = deps.active_jobs.get(job_id)
        assert entry_after_cut is not None, (
            "fixture broken: the cut must leave the entry registered "
            "(the except arm re-queues it for the next tick's drain)"
        )
        assert consumer_task.cancelling() == 0, (
            "fixture broken: the cut itself delivers no cancellation"
        )

        # Tick 2: the poll no longer returns the row (status = 'running'
        # is gone), so no ladder arm fires; the drain re-issues the
        # re-queued abandon, mark_abandoned reads False, and the
        # not-applied arm runs.
        conn2 = await asyncpg.connect(dsn)
        try:
            async with conn2.transaction():
                await controller.run_in_tx(conn2)  # type: ignore[arg-type]
        finally:
            await conn2.close()
        await controller.run_post_tx()

        conn = await asyncpg.connect(dsn)
        try:
            row = await _row(conn, schema, job_id)
            attempts = await _attempt_rows(conn, schema, job_id)
        finally:
            await conn.close()
        assert row["status"] == "abandoned"
        assert row["cancel_phase"] == 2
        assert len(attempts) == 1, (
            f"completing the delivery must not write a second abandon, got "
            f"{len(attempts)} attempt rows: {[dict(r) for r in attempts]}"
        )
        assert attempts[0]["error_class"] == CANCEL_ORIGIN_ABANDONED
        assert consumer_task.cancelling() == 1, (
            f"the cut drain's detached write landed, so its cancellation "
            f"must be delivered on the next drain, got cancelling() == "
            f"{consumer_task.cancelling()}"
        )
        deadline = asyncio.get_running_loop().time() + _WAIT
        while not consumer_task.done():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(
                    "the cancellation was never delivered: the handler is "
                    "still running with its row already abandoned"
                )
            await asyncio.sleep(0.01)
        assert consumer_task.cancelled()
        assert deps.active_jobs.get(job_id) is None
    finally:
        stopped.set()
        if not consumer_task.done():
            consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        await pool.close()


async def test_ladder_survives_repeated_rollbacks_and_lands_on_the_good_tick(
    rt_schema: tuple[str, str],
) -> None:
    """A PERSISTENTLY failing tick (the rollback condition recurs): the
    honest state is the job still running, its entry registered and
    re-armed at FORCED each time, the handler live and uncancellable-by-
    surprise (the same-tick arm never delivers in-transaction) - and the
    FIRST committing tick lands the abandon, once."""
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
        try:
            await stopped.wait()
        except asyncio.CancelledError:
            entry = deps.active_jobs.get(job_id)
            if entry is not None and entry.cancel_phase >= CancelPhase.ABANDON_PENDING:
                await deps.active_jobs.deregister(job_id, entry)
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

        for rollback_tick in range(2):
            conn_tx = await asyncpg.connect(dsn)
            tx = conn_tx.transaction()
            try:
                await tx.start()
                await controller.run_in_tx(conn_tx)  # type: ignore[arg-type]
                raise _SimulatedSiblingError
            except _SimulatedSiblingError:
                await tx.rollback()
            finally:
                await controller.run_post_tx()
                await conn_tx.close()

            entry = deps.active_jobs.get(job_id)
            assert entry is not None, (
                f"rollback {rollback_tick + 1}: the entry must survive "
                f"registered - the re-issue arm iterates active_jobs.all()"
            )
            assert entry.cancel_phase == CancelPhase.FORCED, (
                f"rollback {rollback_tick + 1}: the not-applied abandon "
                f"re-arms the entry at FORCED for the next tick"
            )
            assert consumer_task.cancelling() == 0, (
                f"rollback {rollback_tick + 1}: no cancellation may be "
                f"delivered for an abandon that did not apply"
            )
            conn = await asyncpg.connect(dsn)
            try:
                row = await _row(conn, schema, job_id)
            finally:
                await conn.close()
            assert row["status"] == "running"
            assert row["cancel_phase"] == 1

        # The first committing tick lands the whole ladder, once.
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
            attempts = await _attempt_rows(conn, schema, job_id)
        finally:
            await conn.close()
        assert row["status"] == "abandoned"
        assert len(attempts) == 1, (
            f"N rollbacks and one good tick write exactly one abandon "
            f"attempt row, got {[dict(r) for r in attempts]}"
        )
        assert attempts[0]["error_class"] == CANCEL_ORIGIN_ABANDONED
        assert consumer_task.cancelling() == 1
        await asyncio.wait([consumer_task], timeout=_WAIT)
        assert consumer_task.done()
        assert deps.active_jobs.get(job_id) is None
    finally:
        stopped.set()
        if not consumer_task.done():
            consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        await pool.close()
