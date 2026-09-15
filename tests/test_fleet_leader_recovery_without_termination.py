"""A silent leader must not take the maintenance plane down with it.

Every piece of scheduled work in the fleet is leader-gated: the reclaim sweeps,
the cron ticks, the scheduled-to-pending promotion, the pruning. Exactly one pod
does them, which is what stops a cron schedule firing once per pod. The cost of
that design is that losing the leader has to be recoverable without help.

The role is held by a session-scoped advisory lock, which is released when the
holder's session ends. That covers a clean exit and a dropped connection, but
not a holder that stops working while its session stays open: a process stopped
mid-flight, a host frozen, a network partition that black-holes traffic without
resetting it. In those cases the lock stays held by a backend that will never
renew its lease, and the only recovery the fleet has is to terminate that
backend.

Terminating another session's backend is a privilege, and on managed Postgres it
is commonly reserved. Where it is unavailable the recovery path cannot run and
the lock survives until the server's own keepalive reaping eventually closes the
session — a horizon measured in hours at stock settings. For that whole window
no pod holds the role and none can take it: nothing sweeps, nothing promotes
scheduled jobs, no cron fires, nothing prunes.

The failure is invisible from the inside. Dispatch is not leader-gated, so
workers keep claiming and running whatever is already pending and every pod
reports itself healthy. What stops is everything time-based — and the first
symptom reaches an operator hours later as work that silently never ran.

The contract pinned here is that recovery does not depend on that privilege. A
leader that has gone silent past its lease must be displaceable by a surviving
pod using only the rights an ordinary application role has over its own tables,
because the whole maintenance plane is what waits on it.

The restriction is applied at the database, not by substituting anything inside
the worker: the deployment's own ``search_path`` resolves
``pg_terminate_backend`` to a function that refuses, which is what a reserved
privilege looks like to the code that calls it. The surviving pod is started
after that is in place, as a pod scheduled into such a deployment is, so every
connection it opens — including the one its election loop opens for itself —
sees a database that will not let it terminate a peer.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.constants import schema_lock_name
from taskq.testing.assertions import wait_for_condition
from taskq.worker.leader import MaintenanceLeader
from tests._fleet import Fleet, Pod, open_fleet

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_QUEUE = "fleet_leadrec_q"
_ACTOR = "fleet_leadrec_actor"

#: A short heartbeat so the staleness horizon — a small multiple of it — is
#: reached inside the test rather than at production timings.
_LEADER_SETTINGS = {"heartbeat_interval": "0.5", "lock_lease": "4.0"}


def _start_leader(pod: Pod) -> tuple[asyncio.Event, asyncio.Task[None]]:
    """Run the production election loop for this pod."""
    leader = MaintenanceLeader(pod.deps, pod.worker_id, pod.backend, clock=SystemClock())
    stop = asyncio.Event()
    return stop, asyncio.create_task(leader.run(stop))


async def _leader_row_worker(fleet: Fleet) -> object | None:
    rows = await fleet.fetch(
        'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
    )
    return rows[0]["worker_id"] if rows else None


async def _restrict_backend_termination(dsn: str, schema: str, database: str) -> None:
    """Make ``pg_terminate_backend`` refuse, as a reserved privilege does.

    The refusing function is installed in the deployment's schema and the
    database's ``search_path`` is set to resolve that schema first, so every
    connection opened afterwards — including the ones the election loop opens
    for itself — sees the restricted deployment.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f'CREATE FUNCTION "{schema}".pg_terminate_backend(integer) RETURNS boolean '
            "LANGUAGE plpgsql AS $$ BEGIN "
            "RAISE EXCEPTION 'permission denied for function pg_terminate_backend' "
            "USING ERRCODE = '42501'; "
            "END $$"
        )
        await conn.execute(
            f'ALTER DATABASE "{database}" SET search_path = "{schema}", pg_catalog, public'
        )
    finally:
        await conn.close()


async def _lift_restriction(dsn: str, database: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'ALTER DATABASE "{database}" RESET search_path')
    finally:
        await conn.close()


async def test_a_silent_leader_is_displaced_without_terminating_its_backend(
    pg_dsn: str,
) -> None:
    """A surviving pod takes the role from a frozen leader using ordinary rights.

    The frozen leader is simulated exactly as the failure occurs in production:
    a live session holds the election lock and its recorded lease goes stale
    because nothing renews it. The session is never closed — that is the point,
    since a closed session would release the lock and there would be nothing to
    recover from.
    """
    schema = f"fleet_leadrec_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=(),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_LEADER_SETTINGS,
    ) as fleet:
        dsn = str(fleet.settings.pg_dsn)

        probe = await asyncpg.connect(dsn)
        try:
            database = await probe.fetchval("SELECT current_database()")
        finally:
            await probe.close()
        assert isinstance(database, str)

        # The frozen incumbent: its own live session, holding the election
        # lock, with a stale lease recorded against a worker that is not the
        # successor. Nothing renews it and nothing closes it.
        frozen = await asyncpg.connect(dsn)
        try:
            lock_name = schema_lock_name("maintenance_leader", schema)
            held = await frozen.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
            )
            assert held is True, (
                "the scenario requires the frozen incumbent to be holding the election lock"
            )

            ghost = new_uuid()
            await frozen.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is this test's own generated identifier; values are $-bound
                "VALUES ($1, $2, $3, $4)",
                ghost,
                "frozen-host",
                4242,
                [_QUEUE],
            )
            await frozen.execute(
                f'INSERT INTO "{schema}".maintenance_leader '  # noqa: S608  # Why: same
                "(singleton, worker_id, elected_at, last_seen_at) "
                "VALUES (true, $1, clock_timestamp() - interval '1 hour', "
                "clock_timestamp() - interval '1 hour') "
                "ON CONFLICT (singleton) DO UPDATE SET worker_id = EXCLUDED.worker_id, "
                "last_seen_at = EXCLUDED.last_seen_at",
                ghost,
            )

            stale = await frozen.fetchval(
                f"SELECT last_seen_at < clock_timestamp() - interval '10 seconds' "  # noqa: S608  # Why: same
                f'FROM "{schema}".maintenance_leader WHERE singleton = true'
            )
            assert stale is True, "the scenario requires the incumbent's lease to have gone silent"

            # The restriction goes in before the surviving pod exists, so the
            # pod starts into an already-restricted deployment and every
            # connection it opens carries the restriction — including the one
            # its election loop opens for itself.
            await _restrict_backend_termination(dsn, schema, database)
            took_over = False
            held_by: object | None = None
            election_crash: BaseException | None = None
            try:
                successor = await fleet.start_pod("successor")
                stop, task = _start_leader(successor)
                try:
                    # The role is observed while the loop is still running:
                    # stopping it stands the pod down, so a check afterwards
                    # would say nothing about whether it ever took over.
                    try:
                        await wait_for_condition(
                            successor.deps.is_leader.is_set,
                            description=(
                                "a surviving pod took the maintenance role from "
                                "a leader that went silent while holding its "
                                "session open, on a deployment that does not "
                                "permit terminating another backend"
                            ),
                            timeout=25.0,
                        )
                        took_over = True
                        held_by = await _leader_row_worker(fleet)
                    except AssertionError:
                        took_over = False
                finally:
                    stop.set()
                    try:
                        await task
                    except BaseException as exc:  # Why: the election loop's own failure is an observable outcome of this scenario, reported by the assertions below rather than raised as a test error
                        election_crash = exc
            finally:
                await _lift_restriction(dsn, database)

            assert election_crash is None, (
                "the election loop did not survive a deployment that refuses "
                "backend termination: it raised "
                f"{type(election_crash).__name__} and stopped. A pod that cannot "
                "displace a silent leader should remain a healthy follower and "
                "keep trying, not fail its maintenance task — the fleet now has "
                "neither a leader nor a candidate, and every loop the role owns "
                "is gone until the pod is restarted"
            )

            assert took_over, (
                "no pod took the maintenance role. The previous leader went "
                "silent without closing its session, so the session-scoped "
                "election lock is still held by a backend that will never renew "
                "its lease, and the only recovery path available is terminating "
                "that backend — a privilege managed Postgres commonly reserves. "
                "Until the server's own keepalive reaping closes the session, "
                "hours later, nothing sweeps, nothing promotes scheduled jobs, "
                "no cron fires and nothing prunes. Dispatch is not leader-gated, "
                "so every pod keeps running pending work and reports itself "
                "healthy while all time-based work silently stops"
            )
            assert held_by == successor.worker_id, (
                "the maintenance leader row must name the pod that now holds "
                "the role, so an operator reading it is told who owns "
                f"maintenance; it names {held_by!r}"
            )
        finally:
            # The frozen session may or may not still exist depending on how
            # recovery was achieved; closing it is cleanup, not an assertion.
            await frozen.close()
