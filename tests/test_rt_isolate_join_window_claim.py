# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""Red-team: a claim landing inside isolate_self's join window.

``isolate_self`` used to capture the re-pend's exclusion set once, before
any await, then cancel its active entries and join them with a bound of
``cancellation_grace_period + cleanup_grace_period + CLOSE_TIMEOUT_SECS``
(45s at defaults). The producer and the consumer loops kept running for
that whole window - ``shutdown`` is only set in the outermost ``finally``
- so a row claimed after the snapshot arrived ``running`` and locked to
this worker, was missing from the ``id <> ALL($2::uuid[])`` exclusion,
and the guarded UPDATE matched it while its local handler was live: a
peer claims the re-pended row the moment its ``scheduled_at`` arrives
and runs it concurrently with the local body, a double run, and the
local terminal write then loses the attempt-epoch fence against a
ledger row that already says ``crashed``.

Two exposure shapes, both pinned here through the same deterministic
park-point choreography (a claim intent registered mid-window, forced
by entry A's delivered cancellation, which can only fire after the
pre-join snapshot the old code took):

* B models the primary race: its claim intent and registry entry land
  INSIDE the join window, after the old snapshot.
* C models the queue-resident shape: claimed, marked enqueued, parked in
  local_queue - in neither ``held_ids`` map, about to be taken and
  executed by a consumer the moment the loop schedules the take.

Contract under test: ``isolate_self``'s re-pend excludes every row this
process may still execute locally - registered, intent, and queued -
captured as late as the statement boundary allows. A row whose handler
is live in this process stays ``running`` and locked, with no attempt
ledger row, whatever the moment its claim landed relative to the join
window. Lock-lease expiry remains the backstop for rows whose
unwinding never finishes.

The exclusion alone cannot close the one-scheduler-step residual (a
claim committed to the jobs table but not yet marked in either map), so
the fix also stops the claim path: ``isolate_self`` sets
``producer_stop_event`` at entry - the producer starts no new rounds,
its exit pass hands back what it claimed unmarked, and a row a consumer
has already taken is never dispatched (run.py's stop guard).
"""

import asyncio
from collections.abc import AsyncIterator
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
import structlog
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.migrate import apply_pending
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_running_job, create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import isolate_self
from tests.conftest import _FakePool

pytestmark = pytest.mark.integration

# The join window bound is cancellation_grace + cleanup_grace +
# CLOSE_TIMEOUT_SECS (5.0): graces near zero give a ~5s window, wide
# enough for the mid-window claim, far shorter than the stock 45s.
_CANCEL_GRACE = 0.05
_CLEANUP_GRACE = 0.05
# Bounds every wait in this file so a wedged interleaving fails, never hangs.
_WAIT = 15.0


class _StubPayload(BaseModel):
    """Minimal payload for a JobContext."""


@pytest_asyncio.fixture(scope="module")
async def rt_schema(pg_dsn: str) -> AsyncIterator[tuple[str, str]]:
    """A random migrated schema on this module's database: ``(schema, dsn)``."""
    schema = f"tij_{new_base62()}".lower()
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


def _isolate_deps(dsn: str, schema: str) -> WorkerDeps:
    return WorkerDeps(  # type: ignore[call-arg]
        settings=WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_LOCK_LEASE": "360",
                "TASKQ_TERMINATION_GRACE_PERIOD": "360",
                "TASKQ_CANCELLATION_GRACE_PERIOD": str(_CANCEL_GRACE),
                "TASKQ_CLEANUP_GRACE_PERIOD": str(_CLEANUP_GRACE),
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


async def _row_states(
    conn: asyncpg.Connection, schema: str, ids: list[UUID]
) -> dict[UUID, tuple[str, UUID | None]]:
    rows = await conn.fetch(
        f'SELECT id, status::text AS status, locked_by_worker FROM "{schema}".jobs '
        "WHERE id = ANY($1::uuid[])",
        ids,
    )
    return {r["id"]: (str(r["status"]), r["locked_by_worker"]) for r in rows}


async def test_claim_landing_in_join_window_is_not_repended(
    rt_schema: tuple[str, str],
) -> None:
    """A row claimed before or during the join window keeps its running
    row and its clean ledger: the re-pend must not touch what this
    process may still execute."""
    schema, dsn = rt_schema
    worker_id = new_uuid()
    conn = await asyncpg.connect(dsn)
    try:
        await create_worker(conn, schema, worker_id)
        a_id = await create_running_job(conn, schema, worker_id)
        b_id = await create_running_job(conn, schema, worker_id)
        c_id = await create_running_job(conn, schema, worker_id)
    finally:
        await conn.close()

    deps = _isolate_deps(dsn, schema)

    # Entry A: registered BEFORE the call, so the pre-join snapshot the
    # old code took covers it (the control). Its body parks; the
    # cancellation isolate delivers is the park point that proves the
    # snapshot has already run.
    cancel_delivered = asyncio.Event()
    release = asyncio.Event()

    async def _parked_handler(sets_park_point: bool) -> None:
        try:
            await asyncio.Event().wait()  # the live "handler"
        except asyncio.CancelledError:
            if sets_park_point:
                cancel_delivered.set()
            await release.wait()
            raise

    async def _claim_during_window() -> None:
        """B's claim sequence, parked until A's cancellation proves the
        pre-join snapshot has run: mark_claimed then register, the same
        take-to-register chain run.py's consumer walks."""
        await cancel_delivered.wait()
        deps.active_jobs.mark_claimed(b_id)
        await deps.active_jobs.register(b_id, b_task, _make_ctx(b_id, worker_id))

    loop = asyncio.get_running_loop()
    a_task = loop.create_task(_parked_handler(sets_park_point=True))
    b_task = loop.create_task(_parked_handler(sets_park_point=False))
    registrar = loop.create_task(_claim_during_window())
    for _ in range(5):
        await asyncio.sleep(0)
    await deps.active_jobs.register(a_id, a_task, _make_ctx(a_id, worker_id))
    # C: claimed, marked enqueued, parked in local_queue - in neither
    # held_ids map, still to be taken and executed by a consumer.
    deps.active_jobs.mark_enqueued(c_id)
    # Fixture integrity: B must still be unclaimed here - its claim only
    # fires once isolate_self is inside the join window.
    assert not registrar.done()

    shutdown = asyncio.Event()
    try:
        await isolate_self(deps, worker_id, shutdown)
        assert shutdown.is_set()
        # B's claim landed inside the join window, before the re-pend's
        # SELECT: the interleaving under test really happened.
        assert registrar.done()
        assert deps.active_jobs.get(b_id) is not None, (
            "fixture broken: B's registry entry must still be live when the re-pend ran"
        )

        conn = await asyncpg.connect(dsn)
        try:
            states = await _row_states(conn, schema, [a_id, b_id, c_id])
            attempts = await conn.fetch(
                f'SELECT job_id FROM "{schema}".job_attempts WHERE job_id = ANY($1::uuid[])',
                [a_id, b_id, c_id],
            )
        finally:
            await conn.close()
    finally:
        # Reap EVERY minted task before the loop's own teardown: a parked
        # task left pending hangs the loop shutdown (the suite's doctrine:
        # a task minted on the module loop is the minting test's to
        # retrieve).
        release.set()
        for task in (a_task, b_task, registrar):
            if not task.done():
                task.cancel()
        # return_exceptions: A re-raises the cancellation isolate
        # delivered, B and the registrar return normally; retrieval is
        # what matters, the values are not asserted.
        await asyncio.gather(a_task, b_task, registrar, return_exceptions=True)
        for task in (a_task, b_task, registrar):
            assert task.done(), "fixture broken: a parked task outlived the reap"

    # The contract: every row this process may still execute locally
    # stays running and locked, with a clean attempt ledger. The old
    # code re-pended B and C here (pending, unlocked) while their local
    # handlers were live, and wrote (job_id, attempt) = 'crashed' /
    # 'HeartbeatLost' rows that fence the live handlers' own terminal
    # writes out.
    for job_id in (a_id, b_id, c_id):
        status, holder = states[job_id]
        assert (status, holder) == ("running", worker_id), (
            f"job {job_id} was re-pended (status={status!r}, "
            f"locked_by_worker={holder!r}) while its local handler was "
            f"live: a peer claiming the re-pended row double-runs it and "
            f"the local terminal write loses the attempt-epoch fence"
        )
    assert attempts == [], (
        f"isolate_self wrote crashed attempt rows {[dict(r) for r in attempts]} "
        f"for jobs whose local handlers were live: the ledger says 'crashed' "
        f"while the handler runs, fencing out its own terminal write"
    )
    # The claim path is stopped: a re-pend of a claim that committed but
    # is not yet marked cannot double-run, because a taken row is never
    # dispatched after this event is set.
    assert deps.producer_stop_event.is_set()
