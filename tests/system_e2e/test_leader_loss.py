"""Lifecycle 2: leader loss + re-election mid-flight.

The maintenance leader is SIGKILLed with jobs in flight. Its leases
lapse, the surviving worker wins re-election, its Sweep 1 reclaims the
disowned rows, and every job that belonged to the dead process terminates
on the survivor.

System invariants: the killed process's death is visible as an
attempt-ledger fact (never a silently vanished claim), the reclaim fires
(lock_expired evidence in the event trail), no attempt's body ran twice,
and the conservation counter balances across the whole population with
every job ``succeeded``.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import pytest

from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import (
    assert_balanced,
    assert_effects_balance,
    delete_tagged,
)
from tests.system_e2e.actors import SysPayload, sys_slow

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s2"


async def _pid_to_worker_id(conn: asyncpg.Connection, schema: str) -> dict[int, str]:
    rows = await conn.fetch(f'SELECT pid, id::text AS id FROM "{schema}".workers')
    return {int(r["pid"]): str(r["id"]) for r in rows}


@pytest.mark.timeout(300)
async def test_sigkill_the_leader_mid_flight_the_fleet_re_elects_and_conserves(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    schema = module_pg_schema.schema_name
    conn = sys_ledger

    worker_a: WorkerProc | None = None
    worker_b: WorkerProc | None = None
    try:
        worker_a = spawn_worker(pg_dsn, schema, tag="s2-a")
        wait_worker_ready(worker_a)
        worker_b = spawn_worker(pg_dsn, schema, tag="s2-b")
        wait_worker_ready(worker_b)

        # The jobs: long enough to be mid-flight when the leader dies, and
        # effect-on-completion so a killed attempt writes nothing.
        for _ in range(6):
            await sys_client.enqueue(sys_slow, SysPayload(sleep=6.0), tags=[_TAG])

        leader_row: asyncpg.Record | None = None
        running: int | None = None
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            running = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".jobs '
                "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
                _TAG,
            )
            leader_row = await conn.fetchrow(
                f'SELECT worker_id::text AS wid FROM "{schema}".maintenance_leader'
            )
            if running and running >= 2 and leader_row is not None:
                break
            await asyncio.sleep(0.1)
        assert leader_row is not None, "no maintenance leader was ever elected"
        assert running is not None and running >= 2, (
            "the fleet never claimed the enqueue: nothing was in flight to lose"
        )

        pid_map = await _pid_to_worker_id(conn, schema)
        leader_wid = leader_row["wid"]
        leader_pid = {v: k for k, v in pid_map.items()}.get(leader_wid)
        assert leader_pid is not None, (
            f"the elected leader {leader_wid} is not one of this scenario's "
            f"subprocesses (known pids {sorted(pid_map)})"
        )

        # SIGKILL the leader mid-flight. The survivor must win
        # re-election, reclaim the lapsed leases after the lease window,
        # and drive every disowned job to a terminal outcome.
        leader_proc = worker_a if worker_a.proc.pid == leader_pid else worker_b
        survivor = worker_b if leader_proc is worker_a else worker_a
        leader_proc.proc.kill()
        leader_rc = leader_proc.proc.wait(timeout=10)
        assert leader_rc == -9, f"the leader exited {leader_rc}, not by SIGKILL"
        if leader_proc is worker_a:
            worker_a = None
        else:
            worker_b = None

        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("succeeded", 0) == 6, (
            f"the post-failover fleet did not complete every job: {counts}"
        )
        assert set(counts) <= {"succeeded"}, (
            f"a leader loss must not manufacture other outcomes: {counts}"
        )

        # The reclaim fired: the event trail carries the lock_expired
        # verdict the re-election sweep writes.
        reclaim_events = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_events e '
            f'JOIN "{schema}".jobs j ON j.id = e.job_id '
            "WHERE j.tags @> ARRAY[$1::text] AND e.kind = 'state_change' "
            "AND e.detail->>'reason' = 'lock_expired'",
            _TAG,
        )
        assert reclaim_events >= 1, (
            "the survivor never wrote a lock_expired reclaim event: the "
            "disowned rows were not reclaimed by a sweep"
        )

        # Exactly-once effects: no attempt of a no-retry actor ran twice,
        # and every effect has a claim row behind it.
        await assert_effects_balance(conn, schema, _TAG)

        # The survivor is still alive at the end of the scenario.
        assert survivor is not None and survivor.proc.poll() is None, (
            "the surviving worker did not outlive the leader's death"
        )
    finally:
        if worker_a is not None:
            reap(worker_a)
        if worker_b is not None:
            reap(worker_b)
        await delete_tagged(conn, schema, _TAG)
