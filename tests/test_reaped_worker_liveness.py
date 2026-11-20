"""A live worker whose ``workers`` row the stale sweep reaped must survive.

``stale_workers_grace`` is ``heartbeat_interval * (max_heartbeat_failures + 3)``
(:mod:`taskq.worker._leader_sweeps`). At the system-e2e tier's cadence
(0.5 s heartbeat, 3-strike isolation) that is 3 s, while a single stalled
tick can cost the tier's 0.5 s command budget plus its own acquire. A
transient database stall therefore lets a peer leader's
``cleanup_stale_workers`` delete the row of a worker whose process is
still alive and still ticking.

The beat that follows cannot notice: ``UPDATE_WORKER_LIVENESS_SQL_TEMPLATE``
is a bare ``UPDATE ... WHERE id = $1``, so a reaped row yields a zero-row
update that reports success, and the successful tick resets the
consecutive-failure counter. The worker stays up, silently unregistered.
The first statement that then stamps the worker's identity into a
``workers``-referencing foreign key is the leader election's
``maintenance_leader`` upsert, whose ``ForeignKeyViolationError`` arm is
fail-closed: it sets the process-wide ``shutdown`` and returns. One
bookkeeping loss becomes a whole-process liveness loss, and when every
remaining member crosses it the fleet is empty with nothing left to
re-elect.

These two pins are the two halves of that invariant. Neither deletes a
row directly: the sweep that reaps it is the real
``cleanup_stale_workers`` at the tier's own grace derivation.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.pg import create_worker
from taskq.worker._leader_shared import cleanup_stale_workers
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.leader import MaintenanceLeader

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL_S = 0.5
_MAX_HEARTBEAT_FAILURES = 3
_LOCK_LEASE_S = 8.0
#: The tier's own derivation (tests/system_e2e/_harness.py
#: stale_worker_reap_window_s): 0.5 * (3 + 3) = 3 s.
_GRACE = timedelta(seconds=_HEARTBEAT_INTERVAL_S * (_MAX_HEARTBEAT_FAILURES + 3))


def _tier_shaped_settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema.lower(),
            "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL_S),
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.5",
            "TASKQ_LOCK_LEASE": str(_LOCK_LEASE_S),
            "TASKQ_MAX_HEARTBEAT_FAILURES": str(_MAX_HEARTBEAT_FAILURES),
            "TASKQ_WATCHDOG_ENABLED": "false",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )


class _LiveWorker:
    """One registered, live-ticking worker plus the peer that reaps it."""

    def __init__(
        self,
        *,
        schema: str,
        stack: AsyncExitStack,
        deps: WorkerDeps,
        backend: PostgresBackend,
        worker_id: UUID,
        peer_id: UUID,
        obs_conn: asyncpg.Connection,
    ) -> None:
        self.schema = schema
        self.stack = stack
        self.deps = deps
        self.backend = backend
        self.worker_id = worker_id
        self.peer_id = peer_id
        self.obs_conn = obs_conn


async def _open_live_worker(pg_dsn: str) -> _LiveWorker:
    schema = f"test_reaped_{new_base62()}".lower()
    settings = _tier_shaped_settings(pg_dsn, schema)

    admin = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(admin, schema=schema)
    finally:
        await admin.close()

    assert settings.pg_dsn is not None
    assert settings.pg_dsn_direct is not None

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(0),
            cleanup_grace_period=timedelta(0),
        )
    except BaseException:
        await stack.aclose()
        raise

    worker_id, peer_id = new_uuid(), new_uuid()
    obs_conn = await asyncpg.connect(str(settings.pg_dsn_direct))
    stack.push_async_callback(obs_conn.close)
    async with deps.dispatcher_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    await create_worker(obs_conn, schema, peer_id)

    return _LiveWorker(
        schema=schema,
        stack=stack,
        deps=deps,
        backend=backend,
        worker_id=worker_id,
        peer_id=peer_id,
        obs_conn=obs_conn,
    )


async def _reap_the_live_workers_row(live: _LiveWorker) -> int:
    """Age the row past the tier's grace and run the real stale sweep.

    The UPDATE and the DELETE share one transaction so the live worker's
    next beat cannot refresh ``last_seen_at`` between them: the race the
    sweep runs in production, made deterministic. Holding the row lock
    briefly is also the stall shape, without waiting the grace out.
    """
    async with live.obs_conn.transaction():
        await live.obs_conn.execute(
            f'UPDATE "{live.schema}".workers '  # noqa: S608  # Why: schema is identifier-validated by the migration/settings layer; values are bound.
            "SET last_seen_at = clock_timestamp() - $1::interval WHERE id = $2",
            _GRACE + timedelta(seconds=1),
            live.worker_id,
        )
        return await cleanup_stale_workers(
            live.obs_conn,
            worker_id=live.peer_id,
            staleness=_GRACE,
            schema=live.schema,
        )


async def _registered(live: _LiveWorker) -> bool:
    found = await live.obs_conn.fetchval(
        f'SELECT count(*) FROM "{live.schema}".workers WHERE id = $1',  # noqa: S608  # Why: see above.
        live.worker_id,
    )
    return int(found) == 1


async def test_a_live_workers_reaped_registration_is_not_silently_a_no_op(
    pg_dsn: str,
) -> None:
    """The liveness write must not report success against a deleted row.

    A reaped worker is alive: its process is running, its heartbeat loop is
    running, and only its membership row is gone. If the beat accepts a
    zero-row update as a healthy tick, nothing in the process ever learns
    the row is missing, the failure counter is reset, and the worker runs on
    unregistered until an unrelated write hits the foreign key. The
    liveness write is the seam that owns the fact, so the fact has to hold
    there: after the beats that follow the reap, the worker is registered
    again. Merely logging the zero-row result is not enough - the worker
    would still be unregistered, and the next foreign-key write would still
    shut it down.
    """
    live = await _open_live_worker(pg_dsn)
    shutdown = asyncio.Event()
    beat: asyncio.Task[None] | None = None
    try:
        beat = asyncio.create_task(
            heartbeat_loop(live.deps, live.worker_id, shutdown), name="heartbeat-reaped"
        )
        await wait_for_condition(
            lambda: _registered(live),
            description="the live worker's first beat landed",
            timeout=30.0,
        )

        removed = await _reap_the_live_workers_row(live)
        assert removed == 1, (
            "the trigger did not reproduce: the stale sweep removed "
            f"{removed} rows, expected the one live worker whose last_seen_at "
            "crossed the tier's grace"
        )
        assert not await _registered(live)

        # The beats after the reap. Every one of them returns without an
        # exception today: the zero-row UPDATE is a success, so the counter
        # resets and the worker never isolates itself. Wait on the row's
        # return rather than sleeping a fixed number of intervals - the
        # control must red on the mechanism, never on a loaded lane.
        await wait_for_condition(
            lambda: _registered(live),
            description=(
                "the live worker's reaped membership to be restored by its "
                "heartbeat: the zero-row liveness UPDATE reported success (no "
                "exception, so the counter never moved) and the row was never "
                "noticed or restored, so any later write that stamps this "
                "worker's id into a workers-referencing foreign key (the "
                "leader election's maintenance_leader upsert) fails closed "
                "and shuts the live process down"
            ),
            timeout=10.0,
        )

        assert live.deps.heartbeat_failures == 0, (
            "the beats after the reap accumulated failures; this pin needs the "
            "silent-success shape (a zero-row liveness UPDATE that raises "
            f"nothing) to be the state under test, saw "
            f"{live.deps.heartbeat_failures} consecutive failures"
        )
        assert await _registered(live)
    finally:
        shutdown.set()
        if beat is not None:
            beat.cancel()
            with suppress(asyncio.CancelledError, ExceptionGroup):
                await beat
        await live.stack.aclose()


async def test_a_live_worker_whose_row_was_reaped_does_not_shut_itself_down(
    pg_dsn: str,
) -> None:
    """End-to-end: a reaped live worker recovers its row and wins again.

    This is the fleet-emptying half of the incident, run as the tier runs
    it: the beat and the campaign share the process. The survivor
    campaigns, the ``maintenance_leader`` upsert violates
    ``maintenance_leader_worker_id_fkey`` because the worker's row is gone,
    and the pre-fix fail-closed arm set the process-wide ``shutdown``. A
    live worker in this state must end up registered and leading - whether
    the beat's repair landed first or the election's did.

    It is deliberately an end-to-end pin, not the election seam's pin: the
    beat can restore the row before the campaign reaches the FK, so this
    test stays green under either single seam's deletion. The isolated
    election-seam coverage lives in
    ``tests/test_atk_reaped_registration_restore.py`` and the FK-arm unit
    block in ``tests/test_leader.py``; what this asserts is the outcome the
    ticket reports (a live process never converts a reaped membership into
    a process death).
    """
    live = await _open_live_worker(pg_dsn)
    shutdown = asyncio.Event()
    beat: asyncio.Task[None] | None = None
    election: asyncio.Task[None] | None = None
    try:
        beat = asyncio.create_task(
            heartbeat_loop(live.deps, live.worker_id, shutdown), name="heartbeat-reaped"
        )
        await wait_for_condition(
            lambda: _registered(live),
            description="the live worker's first beat landed",
            timeout=30.0,
        )

        removed = await _reap_the_live_workers_row(live)
        assert removed == 1, (
            "the trigger did not reproduce: the stale sweep removed "
            f"{removed} rows, expected the one live worker whose last_seen_at "
            "crossed the tier's grace"
        )

        # The campaign runs immediately, the shape the tier's survivor is in:
        # the row is gone and the next elect references it.
        leader = MaintenanceLeader(live.deps, live.worker_id, live.backend, clock=SystemClock())
        election = asyncio.create_task(leader.run(shutdown), name="leader-reaped")

        with structlog.testing.capture_logs() as captured:

            def recovered() -> bool:
                if not shutdown.is_set():
                    return live.deps.is_leader.is_set()
                kinds = {event.get("kind") for event in captured}
                if "leader_upsert_fk_violation" not in kinds:
                    raise AssertionError(
                        "the live worker shut itself down for a reason other than "
                        f"the leader upsert's foreign key: kinds={sorted(k for k in kinds if k)}, "
                        f"heartbeat_failures={live.deps.heartbeat_failures}"
                    )
                raise AssertionError(
                    "the live worker's leader election raised "
                    "ForeignKeyViolationError on the maintenance_leader upsert "
                    "(maintenance_leader_worker_id_fkey) because the stale sweep "
                    "had reaped its workers row, and the fail-closed "
                    "leader-upsert-fk-violation arm set the process-wide shutdown "
                    "instead of restoring the registration and retrying the "
                    "campaign. A live worker's bookkeeping loss became a "
                    "whole-process liveness loss; when every member crosses this "
                    "the fleet is empty and nothing is left to re-elect."
                )

            await wait_for_condition(
                recovered,
                description="the live worker recovered its registration and won the "
                "election instead of shutting down",
                timeout=10.0,
            )
        assert await _registered(live), (
            "the worker won the election but its workers row is still missing; "
            "the maintenance_leader row cannot legitimately reference it"
        )
    finally:
        shutdown.set()
        for task in (election, beat):
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError, ExceptionGroup):
                    await task
        await live.stack.aclose()
