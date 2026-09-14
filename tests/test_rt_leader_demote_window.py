# Why: schema is a fixed test identifier, not user input; every value is $-bound.
# (No file-level S608 noqa: none of this file's statements match the S608 SELECT-interpolation pattern.)
"""Green pin: the dual-leader overlap after a leader's session dies is bounded.

``tests/test_leader_chaos.py`` TC1 pins kill+promote — the terminated leader's pod
dies and the survivor takes over — but nothing pins WHEN the dead leader stops
acting as one.  The overlap window in which two pods both believe they are the
leader opens the moment the dead holder's backend (and with it the session-scoped
election lock) goes away and a contender can promote, and it closes when the dead
leader's own probe loop notices its connection is gone and demotes.  The bound on
that window is therefore the demote latency: heartbeat_interval (the probe
cadence) + dispatcher_command_timeout (the probe bound), per the failover SLA in
``leader.py``'s docstring ("PG failover ≤ heartbeat_interval", "Worker killed
≤ heartbeat_interval + 1 s").

Contract under test (expected GREEN): after ``pg_terminate_backend`` of the
leader's election connection, ``is_leader`` must clear within
heartbeat_interval + dispatcher_command_timeout (+ a small scheduling margin) —
and the freed lock must let the pod re-establish leadership, so the demote is a
demote, not a crash.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.testing.fixtures import _create_worker, _open_pg_backend
from taskq.worker.leader import MaintenanceLeader

pytestmark = pytest.mark.integration

#: Scheduling margin over the SLA bound (heartbeat + command_timeout): the demote
#: itself is is_leader.clear() at the head of the conn-loss cleanup, before any
#: bounded close, so the margin only covers event-loop jitter.
_DEMOTE_BOUND_SLACK_SECS: float = 1.0


async def test_dead_leader_demotes_within_heartbeat_plus_command_timeout(
    pg_dsn: str,
) -> None:
    schema = f"tlrt_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    # _open_pg_backend hands back blitz settings: heartbeat 0.5 s.
    heartbeat = deps.settings.heartbeat_interval
    command_timeout = deps.settings.dispatcher_command_timeout
    demote_bound = heartbeat + command_timeout + _DEMOTE_BOUND_SLACK_SECS
    shutdown = asyncio.Event()
    leader: MaintenanceLeader | None = None
    task: asyncio.Task[None] | None = None
    try:
        worker_id = new_uuid()
        async with deps.dispatcher_pool.acquire() as conn:
            await _create_worker(conn, schema, worker_id)

        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        task = asyncio.create_task(
            leader._election_loop(shutdown),  # pyright: ignore[reportPrivateUsage]  # Why: election loop alone is the seam under test — run()'s TaskGroup is not needed to observe the demote-on-probe-failure path.
            name="rt-demote-window",
        )
        await asyncio.wait_for(deps.is_leader.wait(), timeout=2 * heartbeat + 5.0)
        assert deps.is_leader.is_set()

        # leader_conn is shared with the running election loop (it probes every
        # heartbeat); retry the pid probe through the interleaving, like TC1.
        pid: int | None = None
        for _ in range(10):
            try:
                assert deps.leader_conn is not None
                pid = await deps.leader_conn.fetchval("SELECT pg_backend_pid()")
                break
            except (asyncpg.InternalClientError, asyncpg.InterfaceError):
                await asyncio.sleep(0.2)
        assert pid is not None, "could not read the leader_conn backend pid"

        admin = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
        try:
            await admin.fetchval("SELECT pg_terminate_backend($1)", pid)
        finally:
            await admin.close()

        t_kill = time.monotonic()
        cleared_at: float | None = None
        while time.monotonic() - t_kill < demote_bound + 1.0:
            if not deps.is_leader.is_set():
                cleared_at = time.monotonic()
                break
            await asyncio.sleep(0.02)
        assert cleared_at is not None, (
            "CONTRACT: after the leader's election connection dies, the pod must "
            "demote (clear is_leader) within heartbeat_interval + "
            f"dispatcher_command_timeout = {heartbeat}s + {command_timeout}s — that is "
            "the bound on the dual-leader overlap window (the contender can only "
            "promote after the lock-holding backend died, so the overlap lasts "
            "exactly as long as the dead holder still acts as leader); the failover "
            "SLA in leader.py's docstring promises 'PG failover ≤ heartbeat_interval'"
        )
        assert cleared_at - t_kill <= demote_bound, (
            f"demote took {cleared_at - t_kill:.2f}s, bound is {demote_bound:.2f}s — "
            "the dual-leader overlap window after a leader's session death is not "
            "bounded by heartbeat + command_timeout"
        )

        # The demote must be a demote, not a crash: with the lock freed the same
        # pod re-establishes leadership on its next election cycle.
        await asyncio.wait_for(deps.is_leader.wait(), timeout=2 * heartbeat + 2.0)
    finally:
        if task is not None:
            shutdown.set()
            try:
                await asyncio.wait_for(task, timeout=heartbeat + 5.0)
            except TimeoutError:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        if leader is not None:
            await leader._close_leader_owned_conns(mid_run=False)  # pyright: ignore[reportPrivateUsage]  # Why: election-only startup skips run()'s teardown; this is its manual equivalent.
            await leader._drop_leader_conn(reason="rt teardown")  # pyright: ignore[reportPrivateUsage]  # Why: same — release the session-scoped election lock for teardown.
        await stack.aclose()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
