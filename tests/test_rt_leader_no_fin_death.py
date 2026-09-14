# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team pin: leader death WITHOUT a FIN must not down the maintenance plane for hours.

The election lock is session-scoped — ``pg_try_advisory_lock(hashtextextended($1, 0))``
on ``deps.leader_conn`` (``src/taskq/worker/leader.py``, the election lock
acquisition) — with no matching unlock anywhere in the tree (only the prune,
archive-expiry and migration locks unlock theirs) and no TTL/lease behind it.  A
holder whose TCP session dies without a FIN (power loss, SIGSTOP, black-holed
partition) keeps its backend — and with it the lock — until the server's
``tcp_keepalives_*`` reap the session (stock default ≈ 2 h 11 m); a session that is
merely idle holds the lock *indefinitely*.  From every other pod's point of view an
idle open holder and a dead-without-FIN holder are the same shape:
``pg_try_advisory_lock`` returns false on every cycle, no pod promotes, and the whole
maintenance plane (sweeps, cron, prune, stale-batch completion) is down for the
reaping horizon.

The codebase's own assumption says this stall class is "minutes, not forever"
(``cron_loop.py``'s contention branch — for the transaction-scoped CRON lock); this
file holds the ELECTION lock to at least that recoverability bar.

Contract under test (RED until fixed): once the holder has gone silent past the
heartbeat/watchdog horizon, a healthy pod's election must promote within a bounded,
configured window — e.g. by detecting the stale holder
(``maintenance_leader.last_seen_at``) and reclaiming its backend, or any equivalent
promote-or-fail-loudly mechanism — instead of silently retrying ``false`` forever.

The recovery leg (GREEN, mechanism proof for the fix): ``pg_terminate_backend`` of
the silent session frees the session-scoped lock and the waiting pod promotes within
a few heartbeats.
"""

from __future__ import annotations

import asyncio
import contextlib
from uuid import UUID

import asyncpg
import pytest
import structlog

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import _open_two_pg_workers
from taskq.testing.settings import shorten_chaos_settings
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import MaintenanceLeader

pytestmark = pytest.mark.integration

#: The bounded, configured recovery window the contract demands: with
#: heartbeat_interval=1 s (shorten_chaos_settings) this is 12 election cycles and
#: more than 2x the watchdog interval (5 s) — far past any healthy-leader
#: detection horizon, yet nothing like the ~2 h 11 m tcp_keepalives reaping
#: horizon (or indefinite, for an idle session) the lock's occupancy allows.
_RECOVERY_WINDOW_SECS: float = 12.0


async def _election_only(
    deps: WorkerDeps, backend: PostgresBackend, worker_id: UUID
) -> tuple[MaintenanceLeader, asyncio.Event, asyncio.Task[None]]:
    """Start ONLY the leader's election loop — no ``run()`` TaskGroup, no watchdog.

    The two-pod harness constructs MaintenanceLeader and leaves loops unstarted
    by design; starting the election loop alone lets this file freeze a REAL
    elected leader in place (stop the loop task, keep every connection open)
    — the no-FIN holder shape — without tearing its session down.
    """
    leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        leader._election_loop(shutdown),  # pyright: ignore[reportPrivateUsage]  # Why: the harness contract starts individual leader loops by hand; the election loop alone is the seam under test.
        name=f"rt-election-{worker_id}",
    )
    return leader, shutdown, task


async def _stop_election_task(
    task: asyncio.Task[None], shutdown: asyncio.Event, heartbeat: float
) -> None:
    """Stop an election-loop task: shutdown wakes it within one heartbeat."""
    shutdown.set()
    try:
        await asyncio.wait_for(task, timeout=heartbeat + 5.0)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _release_leader_conns(leader: MaintenanceLeader) -> None:
    """Close the leader-owned conns (cron/monitor/leader) the election-only
    startup path leaves open — ``run()``'s finally normally does this."""
    await leader._close_leader_owned_conns(mid_run=False)  # pyright: ignore[reportPrivateUsage]  # Why: election-only startup skips run()'s teardown; this is its manual equivalent.
    await leader._drop_leader_conn(reason="rt teardown")  # pyright: ignore[reportPrivateUsage]  # Why: same — release the session-scoped election lock for teardown.


async def test_no_fin_leader_death_leadership_must_be_recoverable_within_bounded_window(
    pg_dsn: str,
) -> None:
    schema = f"tlrt_{new_base62()}".lower()
    async with _open_two_pg_workers(pg_dsn, schema=schema) as (
        (_stack_a, deps_a, backend_a, wid_a),
        (_stack_b, deps_b, backend_b, wid_b),
    ):
        with shorten_chaos_settings(deps_a, deps_b):
            heartbeat = deps_a.settings.heartbeat_interval

            leader_a, shutdown_a, task_a = await _election_only(deps_a, backend_a, wid_a)
            leader_b: MaintenanceLeader | None = None
            shutdown_b: asyncio.Event | None = None
            task_b: asyncio.Task[None] | None = None
            try:
                # A real leader wins the election and holds the session lock.
                await asyncio.wait_for(deps_a.is_leader.wait(), timeout=2 * heartbeat + 5.0)
                assert deps_a.leader_conn is not None and not deps_a.leader_conn.is_closed()

                # -- A dies WITHOUT a FIN ------------------------------------------
                # Stopping ONLY the election-loop task exits the loop with no
                # cleanup: deps_a.leader_conn (and the cron/monitor conns) stay
                # OPEN and idle and is_leader stays SET — a holder gone silent
                # with the lock still occupied. Nothing sends a FIN; from pod
                # B's side this is exactly a partitioned/black-holed leader.
                shutdown_a.set()
                await asyncio.wait_for(task_a, timeout=heartbeat + 5.0)
                assert deps_a.is_leader.is_set()
                assert deps_a.leader_conn is not None and not deps_a.leader_conn.is_closed()

                # -- B must be able to take over within the bounded window -------
                leader_b, shutdown_b, task_b = await _election_only(deps_b, backend_b, wid_b)
                with structlog.testing.capture_logs() as captured:
                    promoted = False
                    try:
                        await asyncio.wait_for(
                            deps_b.is_leader.wait(), timeout=_RECOVERY_WINDOW_SECS
                        )
                        promoted = True
                    except TimeoutError:
                        promoted = False
                lost_attempts = [e for e in captured if e.get("event") == "leader-retry"]
                assert promoted, (
                    "CONTRACT: after a leader's session goes silent without a FIN, "
                    "leadership must be recoverable within a bounded, configured window "
                    "(promote — e.g. detect the stale holder via maintenance_leader and "
                    "reclaim its backend — or fail loudly): the maintenance plane "
                    "(sweeps, cron, prune, stale-batch completion) cannot stay down for "
                    "the server's tcp_keepalives reaping horizon. TODAY: over "
                    f"{_RECOVERY_WINDOW_SECS}s — {len(lost_attempts)} full election "
                    f"cycles at heartbeat_interval={heartbeat}s, watchdog=5s — every "
                    "pg_try_advisory_lock on the maintenance_leader key returned false. "
                    "The election lock is session-scoped "
                    "(pg_try_advisory_lock(hashtextextended($1, 0)) on deps.leader_conn, "
                    "leader.py election lock acquisition) with NO unlock and NO TTL, so "
                    "the silent holder's backend keeps the lock until the server reaps "
                    "it (stock tcp_keepalives default ~2h11m; an idle session holds it "
                    "indefinitely), while the codebase's own assumption for the sibling "
                    "cron lock (cron_loop.py contention branch) is that this stall class "
                    "is 'minutes, not forever'. Every pod's election silently retries "
                    "(leader-retry info logs only) and the maintenance plane is down."
                )

                # -- Recovery leg (GREEN — the mechanism the fix needs) ----------
                # Terminating the silent holder's backend server-side frees the
                # session-scoped lock; the waiting pod must promote within a few
                # heartbeats.
                pid: int | None = None
                with contextlib.suppress(
                    asyncpg.PostgresConnectionError,
                    asyncpg.InterfaceError,
                    OSError,
                    TimeoutError,
                ):
                    # The fix under test may already have terminated this backend
                    # (that is what 'promoted' above means) — then the pid probe
                    # fails and the promote assertions below are already satisfied.
                    # (leader_conn non-None is pinned by the zombie asserts above.)
                    if not deps_a.leader_conn.is_closed():
                        pid = await deps_a.leader_conn.fetchval("SELECT pg_backend_pid()")
                if pid is not None:
                    admin = await asyncpg.connect(str(deps_a.settings.pg_dsn_direct))
                    try:
                        await admin.fetchval("SELECT pg_terminate_backend($1)", pid)
                    finally:
                        await admin.close()
                await asyncio.wait_for(deps_b.is_leader.wait(), timeout=4 * heartbeat + 2.0)
                async with deps_b.dispatcher_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        f'SELECT worker_id FROM "{schema}".maintenance_leader '
                        f"WHERE singleton = true"
                    )
                assert row is not None, "promotion must record a maintenance_leader row"
                assert UUID(str(row["worker_id"])) == wid_b, (
                    "the maintenance_leader row must reflect the promoted pod's "
                    f"worker_id, got {row['worker_id']}"
                )
            finally:
                if task_b is not None and shutdown_b is not None:
                    await _stop_election_task(task_b, shutdown_b, heartbeat)
                if leader_b is not None:
                    await _release_leader_conns(leader_b)
                await _release_leader_conns(leader_a)
