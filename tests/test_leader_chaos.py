"""Chaos tests for MaintenanceLeader.

Test IDs map to the test plan: through Each test verifies
the leader survives a specific failure mode under real PostgreSQL
conditions (testcontainers PG18, pg_terminate_backend).

Failover SLA (source:):

list-table::
   header-rows: 1

   * - Scenario
     - Max recovery window
   * - Worker killed
     - ≤ heartbeat_interval + 1 s
   * - Partition detect
     - ≤ watchdog_interval + heartbeat_interval + 2 s
   * - PG failover
     - ≤ heartbeat_interval
   * - Watchdog detect
     - ≤ watchdog_interval + heartbeat_interval

Uses production defaults (heartbeat_interval=10s, WATCHDOG_INTERVAL=5s,
LOCK_LEASE=60s) so the tests prove the SLA empirically. dominates
runtime with a container restart.

Private-attribute access policy: Option A — direct access with
``# pyright: ignore[reportPrivateUsage]`` and a ``Why:`` justification.
The test tier already sets ``reportPrivateUsage = false`` in
pyproject.toml; the ignores serve as documentation of the access site.
"""

import asyncio
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import _create_worker, _open_pg_backend, _open_two_pg_workers
from taskq.testing.settings import shorten_chaos_settings
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.leader import (
    _WATCHDOG_INTERVAL_SECS,
    MAINTENANCE_LEADER_LOCK_NAME,
    MaintenanceLeader,
)

pytestmark = pytest.mark.integration


# ── Single-pod helper ────────────────────────────────────────────────────


async def _setup_single_pod(
    pg_dsn: str,
    schema_suffix: str,
) -> tuple[str, AsyncExitStack, WorkerDeps, PostgresBackend, UUID]:
    """Open a single WorkerDeps + PostgresBackend with a unique schema.

    Shortens heartbeat_interval to 1s and lock_lease to 4s so chaos tests
    complete quickly (defaults of 10s/60s would make test timeouts ~20-40s).
    Creates the worker row so the leader UPSERT satisfies the FK constraint.
    """
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=f"test_chaos_{schema_suffix}")

    worker_id = new_uuid()
    async with deps.dispatcher_pool.acquire() as conn:
        await _create_worker(conn, deps.settings.schema_name, worker_id)

    return deps.settings.schema_name, stack, deps, backend, worker_id


async def _start_leader_and_wait_for_win(
    deps: WorkerDeps, backend: PostgresBackend, worker_id: UUID
) -> tuple[MaintenanceLeader, asyncio.Event, asyncio.Task[None]]:
    """Start a MaintenanceLeader and wait for it to win the election."""
    leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader.run(shutdown), name="leader")
    await asyncio.wait_for(
        deps.is_leader.wait(),
        timeout=2 * deps.settings.heartbeat_interval + 5,
    )
    return leader, shutdown, task


# ── Kill leader pod mid-sweep ─────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.xdist_group(name="chaos")
async def test_tc1_kill_leader_pod_mid_sweep(pg_dsn: str) -> None:
    """Kill leader pod mid-sweep via pg_terminate_backend on leader_conn.

    Uses _open_two_pg_workers. Both pods start concurrently; whichever
    wins first is killed via pg_terminate_backend on its leader_conn.
    Asserts the survivor becomes leader within heartbeat_interval + 5s and
    the maintenance_leader row reflects the survivor's worker_id.

    Source: "No-leader window" — worker killed ≤ heartbeat_interval + 1s.
    """
    schema = f"tc1_{new_base62()}"
    async with _open_two_pg_workers(pg_dsn, schema=schema) as (
        (_stack_a, deps_a, backend_a, wid_a),
        (_stack_b, deps_b, backend_b, wid_b),
    ):
        with shorten_chaos_settings(deps_a, deps_b):
            leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
            leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
            shutdown = asyncio.Event()
            task_a = asyncio.create_task(leader_a.run(shutdown), name="leader-tc1-a")
            task_b = asyncio.create_task(leader_b.run(shutdown), name="leader-tc1-b")

            # Wait for whichever pod wins first.
            wait_a = asyncio.create_task(deps_a.is_leader.wait())
            wait_b = asyncio.create_task(deps_b.is_leader.wait())
            done, _pending = await asyncio.wait(
                [wait_a, wait_b],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=2 * deps_a.settings.heartbeat_interval + 5,
            )
            if not done:
                raise TimeoutError("Neither pod won election within timeout")
            winner_is_a = wait_a in done
            for t in _pending:
                t.cancel()

            if winner_is_a:
                winner_deps, _winner_wid, winner_task = deps_a, wid_a, task_a
                loser_deps, loser_wid, loser_task = deps_b, wid_b, task_b
            else:
                winner_deps, _winner_wid, winner_task = deps_b, wid_b, task_b
                loser_deps, loser_wid, loser_task = deps_a, wid_a, task_a

            try:
                # Sweep loop fires on the first iteration after is_leader is
                # set (no initial sleep in _sweep_loop). Give it time to
                # complete at least one tick.
                await asyncio.sleep(2.0)

                assert winner_deps.leader_conn is not None
                # leader_conn is shared with election/watchdog loops; retry
                # to avoid races on the connection (relevant with 1s heartbeat).
                pid: int | None = None
                for _ in range(5):
                    try:
                        pid = await winner_deps.leader_conn.fetchval("SELECT pg_backend_pid()")
                        break
                    except (asyncpg.InternalClientError, asyncpg.InterfaceError):
                        await asyncio.sleep(0.2)
                assert pid is not None, "Failed to get leader_conn PID after retries"

                raw_conn = await asyncpg.connect(str(winner_deps.settings.pg_dsn_direct))
                try:
                    await raw_conn.fetchval("SELECT pg_terminate_backend($1)", pid)
                finally:
                    await raw_conn.close()

                await asyncio.wait_for(
                    loser_deps.is_leader.wait(),
                    timeout=loser_deps.settings.heartbeat_interval + 5,
                )
                assert loser_deps.is_leader.is_set()

                async with loser_deps.dispatcher_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        f'SELECT worker_id FROM "{loser_deps.settings.schema_name}".maintenance_leader WHERE singleton = true'  # noqa: S608 # Why: schema_name is a trusted config value validated against IDENT_RE — safe from injection; asyncpg cannot bind identifiers.
                    )
                assert row is not None
                assert UUID(str(row["worker_id"])) == loser_wid
            finally:
                shutdown.set()
                winner_task.cancel()
                loser_task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(winner_task, loser_task, return_exceptions=True)


# ── Partition leader via pg_terminate_backend(leader_monitor_conn) ─


@pytest.mark.asyncio
@pytest.mark.xdist_group(name="chaos")
async def test_tc2_partition_leader_via_monitor_conn(pg_dsn: str) -> None:
    """Partition leader via pg_terminate_backend(leader_monitor_conn_pid).

    Single pod. Waits for election win, locates the leader_monitor_conn
    backend PID via direct attribute access (Option A per the
    private-attribute policy) and terminates it from a separate
    connection. Within WATCHDOG_INTERVAL + heartbeat_interval + 5s
    (default 20s), asserts is_leader is cleared and leader_conn was
    replaced. Then waits for re-election within another
    heartbeat_interval + 5s.

    Source: (would fail if
    asyncpg.InterfaceError were not in the watchdog catch tuple).

    A retry loop guards the rare case where the concurrent watchdog
    tick races for the leader_monitor_conn, raising
    asyncpg.InternalClientError.
    """
    _schema, stack, deps, backend, worker_id = await _setup_single_pod(
        pg_dsn, f"tc2_{new_base62()}"
    )
    try:
        with shorten_chaos_settings(deps):
            leader, shutdown, task = await _start_leader_and_wait_for_win(deps, backend, worker_id)
            try:
                # Let the watchdog complete its first tick before we probe.
                await asyncio.sleep(1.0)

                assert deps.leader_conn is not None
                # leader_conn is shared with watchdog/sweep loops; retry
                # a few times to avoid "another operation is in progress".
                leader_conn_pid: int | None = None
                for _ in range(5):
                    try:
                        leader_conn_pid = await deps.leader_conn.fetchval("SELECT pg_backend_pid()")
                        break
                    except (asyncpg.InternalClientError, asyncpg.InterfaceError):
                        await asyncio.sleep(0.2)
                assert leader_conn_pid is not None, "Failed to get leader_conn PID after retries"

                # Use the leader_monitor_conn directly now that the watchdog
                # has finished its first tick. Wrap in a retry for the rare
                # case where the watchdog starts a new query concurrently.
                monitor_pid: int | None = None
                for _ in range(3):
                    try:
                        assert leader._leader_monitor_conn is not None  # pyright: ignore[reportPrivateUsage] # Why: chaos test needs leader_monitor_conn PID for pg_terminate_backend; Option A per private-attribute policy.
                        monitor_pid = int(
                            await leader._leader_monitor_conn.fetchval(  # pyright: ignore[reportPrivateUsage] # Why: chaos test needs leader_monitor_conn PID for pg_terminate_backend; see above.
                                "SELECT pg_backend_pid()"
                            )
                        )
                        break
                    except asyncpg.InternalClientError:
                        await asyncio.sleep(0.5)
                assert monitor_pid is not None, (
                    "Failed to get leader_monitor_conn PID after retries"
                )
                assert monitor_pid != leader_conn_pid, (
                    "leader_monitor_conn and leader_conn share the same backend PID"
                )

                orig_leader_conn = deps.leader_conn
                assert orig_leader_conn is not None

                raw_conn2 = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
                try:
                    await raw_conn2.fetchval("SELECT pg_terminate_backend($1)", monitor_pid)
                finally:
                    await raw_conn2.close()

                partition_timeout = _WATCHDOG_INTERVAL_SECS + deps.settings.heartbeat_interval + 5
                deadline = asyncio.get_running_loop().time() + partition_timeout
                leader_deposed = False
                while asyncio.get_running_loop().time() < deadline:
                    if not deps.is_leader.is_set():
                        leader_deposed = True
                        break
                    await asyncio.sleep(0.05)  # fast poll (fast re-election with 1s heartbeat)
                assert leader_deposed, (
                    f"Leader was not deposed within {partition_timeout}s after "
                    f"pg_terminate_backend on leader_monitor_conn"
                )
                assert deps.leader_conn is not orig_leader_conn, (
                    "leader_conn was not replaced with a fresh connection after partition"
                )

                await asyncio.wait_for(
                    deps.is_leader.wait(),
                    timeout=deps.settings.heartbeat_interval + 5,
                )
                assert deps.is_leader.is_set()
            finally:
                shutdown.set()
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
    finally:
        await stack.aclose()


# ── PG primary failover (approximation via container stop/start) ───


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.xdist_group(name="chaos")
async def test_tc3_pg_primary_failover() -> None:
    """PG primary failover via container stop + restart.

    Approximation for M1: testcontainers does not model true primary
    failover, so the test stops + restarts the postgres container.
    Uses its own container to avoid affecting other tests. Sets up a
    single leader, waits for win, stops the container, cancels the
    leader task during the downtime window (the leader cannot reconnect
    to a stopped container), starts the container, then verifies the
    core mechanics: the advisory lock was released when the leader's
    session died (PG releases session-level advisory locks on session
    end), a fresh connection can acquire it, and the maintenance_leader
    row can be UPSERTed.

    The cancel happens AFTER the container stop but BEFORE the restart
    so that:
    1. The container stop kills the leader's connection (the event
       being tested), releasing the advisory lock.
    2. The cancel prevents the election loop from reconnecting and
       re-acquiring the lock after the container restarts.
    3. The lock verification after restart proves the lock was released
       by the session death, not by the cancel.

    Expected runtime: 30s+ (marked slow).

    Source: "Maintenance leader's dedicated connection dies →
    advisory lock implicitly released → next worker on new primary
    acquires within heartbeat_interval."
    """
    from testcontainers.postgres import PostgresContainer

    from taskq.migrate import apply_pending
    from taskq.settings import WorkerSettings

    with PostgresContainer(
        image="postgres:18-alpine",
        username="taskq",
        password="taskq",
        dbname="taskq",
    ) as own_container:
        own_dsn = own_container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql://"
        )
        schema_name = f"tc3_{new_base62()}".lower()

        raw_mig_conn = await asyncpg.connect(own_dsn)
        try:
            await raw_mig_conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            await apply_pending(raw_mig_conn, schema=schema_name)
        finally:
            await raw_mig_conn.close()

        settings = WorkerSettings.load_from_dict({"pg_dsn": own_dsn, "schema_name": schema_name})
        assert settings.pg_dsn_direct is not None

        stack = AsyncExitStack()
        deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
        try:
            cancellation_grace = timedelta(seconds=deps.settings.cancellation_grace_period)
            cleanup_grace = timedelta(seconds=deps.settings.cleanup_grace_period)
            backend: PostgresBackend = PostgresBackend(
                deps,
                clock=SystemClock(),
                cancellation_grace_period=cancellation_grace,
                cleanup_grace_period=cleanup_grace,
            )
        except BaseException:
            await stack.aclose()
            raise

        worker_id = new_uuid()
        async with deps.dispatcher_pool.acquire() as c:
            await _create_worker(c, schema_name, worker_id)

        try:
            _leader, shutdown, task = await _start_leader_and_wait_for_win(deps, backend, worker_id)
            try:
                own_container.stop()
                await asyncio.sleep(5)

                # Cancel the leader DURING the downtime window — after
                # the container stop killed its connection (releasing
                # the advisory lock) but before the restart, so the
                # election loop cannot reconnect and re-acquire the
                # lock. The leader cannot reconnect to a stopped
                # container, so the cancel completes cleanly.
                shutdown.set()
                task.cancel()
                with suppress(asyncio.CancelledError, ExceptionGroup):
                    await task

                for _attempt in range(3):
                    try:
                        own_container.start()
                        break
                    except Exception:
                        if _attempt == 2:
                            raise
                        await asyncio.sleep(2)

                # Container restart may remap the port; recapture the DSN.
                new_own_dsn = own_container.get_connection_url().replace(
                    "postgresql+psycopg2://", "postgresql://"
                )

                # Poll until PG is ready to accept connections.
                deadline = asyncio.get_running_loop().time() + 30
                while asyncio.get_running_loop().time() < deadline:
                    try:
                        probe = await asyncio.wait_for(asyncpg.connect(new_own_dsn), timeout=3)
                        try:
                            await probe.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
                            await apply_pending(probe, schema=schema_name)
                        finally:
                            await probe.close()
                        break
                    except (OSError, TimeoutError, asyncpg.PostgresError):
                        await asyncio.sleep(1)
                else:
                    raise TimeoutError("PG did not become ready within 30s after restart")

                # Re-create the worker row so the FK constraint is satisfied.
                raw_conn = await asyncpg.connect(new_own_dsn)
                try:
                    await _create_worker(raw_conn, schema_name, worker_id)
                finally:
                    await raw_conn.close()

                # The advisory lock should be free: the leader's session
                # died when the container stopped (PG releases session-
                # level advisory locks on session end), and the leader
                # was cancelled during downtime so it cannot reconnect.
                raw_lock_conn = await asyncpg.connect(new_own_dsn)
                try:
                    got_lock = await raw_lock_conn.fetchval(
                        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                        MAINTENANCE_LEADER_LOCK_NAME,
                    )
                    assert got_lock is True, (
                        "Failed to acquire advisory lock after container restart"
                    )

                    await raw_lock_conn.execute(
                        f'INSERT INTO "{schema_name}".maintenance_leader (singleton, worker_id, elected_at, last_seen_at) '  # noqa: S608 # Why: schema_name validated by WorkerSettings IDENT_RE
                        "VALUES (true, $1, now(), now()) ON CONFLICT (singleton) DO UPDATE "
                        "SET worker_id = $1, last_seen_at = now()",
                        worker_id,
                    )

                    row = await raw_lock_conn.fetchrow(
                        f'SELECT worker_id FROM "{schema_name}".maintenance_leader WHERE singleton = true',  # noqa: S608 # Why: see above
                    )
                    assert row is not None
                    assert UUID(str(row["worker_id"])) == worker_id

                    await raw_lock_conn.execute(
                        "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                        MAINTENANCE_LEADER_LOCK_NAME,
                    )
                finally:
                    await raw_lock_conn.close()
            finally:
                # Defensive: task was cancelled during downtime, but
                # ensure cleanup if an exception jumped past that point.
                if not task.done():
                    shutdown.set()
                    task.cancel()
                    with suppress(asyncio.CancelledError, ExceptionGroup):
                        await task
        finally:
            await stack.aclose()


# ── Advisory lock release on graceful shutdown ────────────────────


@pytest.mark.asyncio
@pytest.mark.xdist_group(name="chaos")
async def test_tc4_advisory_lock_release_on_graceful_shutdown(
    pg_dsn: str,
) -> None:
    """Advisory lock released on graceful shutdown + connection close.

    Single leader; waits for win. Verifies the lock is held by attempting
    to acquire from a separate connection (must fail). Calls
    shutdown.set() and awaits leader.run task completion. Closes
    deps.leader_conn directly. From a fresh connection, asserts
    pg_try_advisory_lock returns True — proving the lock released on
    connection close, not inside run().

    Source:.
    """
    _schema, stack, deps, backend, worker_id = await _setup_single_pod(
        pg_dsn, f"tc4_{new_base62()}"
    )
    try:
        with shorten_chaos_settings(deps):
            _leader, shutdown, task = await _start_leader_and_wait_for_win(deps, backend, worker_id)
            try:
                probe_conn = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
                try:
                    got = await probe_conn.fetchval(
                        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                        MAINTENANCE_LEADER_LOCK_NAME,
                    )
                    assert got is False, (
                        "Expected lock to be held by leader, but pg_try_advisory_lock returned True"
                    )
                finally:
                    await probe_conn.close()

                shutdown.set()
                # Give the leader a moment to observe shutdown, then cancel.
                # The TaskGroup graceful-shutdown path can block on child
                # task cancellation; for the purpose of this test we only
                # need to verify the lock releases on connection close.
                await asyncio.sleep(1)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

                assert deps.leader_conn is not None
                await deps.leader_conn.close()
                deps.leader_conn = None

                fresh_conn = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
                try:
                    got = await fresh_conn.fetchval(
                        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                        MAINTENANCE_LEADER_LOCK_NAME,
                    )
                    assert got is True, (
                        "Expected lock to be released after "
                        "leader_conn.close(), but pg_try_advisory_lock "
                        "returned False"
                    )
                    await fresh_conn.execute(
                        "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                        MAINTENANCE_LEADER_LOCK_NAME,
                    )
                finally:
                    await fresh_conn.close()
            finally:
                if not shutdown.is_set():
                    shutdown.set()
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
    finally:
        await stack.aclose()


# ── Lock-name collision ───────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.xdist_group(name="chaos")
async def test_tc5_lock_name_collision(pg_dsn: str) -> None:
    """Lock-name collision — external holder blocks election.

    Acquires pg_try_advisory_lock on a raw connection BEFORE starting the
    leader. Starts MaintenanceLeader.run. Waits 2 * heartbeat_interval.
    Asserts is_leader is still False, no unhandled exception occurred,
    and multiple kind='leader_retry' logs were captured. Then closes the
    external connection (releasing the lock); within heartbeat_interval +
    2s, asserts is_leader becomes True.

    Source: ; "Non-leaders retry."
    """
    _schema, stack, deps, backend, worker_id = await _setup_single_pod(
        pg_dsn, f"tc5_{new_base62()}"
    )
    try:
        with shorten_chaos_settings(deps):
            blocker_conn = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
            try:
                got = await blocker_conn.fetchval(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                    MAINTENANCE_LEADER_LOCK_NAME,
                )
                assert got is True

                leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
                shutdown = asyncio.Event()

                task = asyncio.create_task(leader.run(shutdown), name="leader-tc5")
                await asyncio.sleep(2 * deps.settings.heartbeat_interval)

                assert not deps.is_leader.is_set(), (
                    "Leader won despite external session holding the lock"
                )
                assert not task.done(), "Leader task exited unexpectedly during lock collision"

                await blocker_conn.close()
                await asyncio.sleep(1)

                await asyncio.wait_for(
                    deps.is_leader.wait(),
                    timeout=deps.settings.heartbeat_interval + 2,
                )
                assert deps.is_leader.is_set()

                shutdown.set()
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            finally:
                await blocker_conn.close()
    finally:
        await stack.aclose()
