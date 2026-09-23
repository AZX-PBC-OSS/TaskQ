"""Lifecycle 2: leader loss + re-election mid-flight.

The maintenance leader is SIGKILLed with jobs it holds in flight. Its
leases lapse, the surviving worker wins re-election, its Sweep 1
reclaims the disowned rows, and every job that belonged to the dead
process terminates on the survivor.

The choreography (the rolling-deploy scenario's sequencing, reasons
below): worker A boots ALONE and fills its slots, worker B boots into
the live fleet, then A is SIGKILLed. Booting B only after A holds work
is what makes the kill's premise deterministic: while A is the fleet's
only claimant, the locks the gate observes are provably A's, and B -
whose prefetch can otherwise win the whole backlog in the first claim
rounds (a worker may transiently hold up to its consumer count beyond
its free slots in the get-to-register window, so a lone fast claimant
can lock every row while the peer's producer never wins one) - is
guaranteed a live peer that outlives the kill.

System invariants: the killed process's death is visible as an
attempt-ledger fact (never a silently vanished claim), the reclaim fires
(lock_expired evidence in the event trail), no attempt's body ran twice,
and the conservation counter balances across the whole population with
every job ``succeeded``.
"""

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

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

_TAG = "sys-s2"


async def _pid_to_worker_id(conn: asyncpg.Connection, schema: str) -> dict[int, str]:
    rows = await conn.fetch(f'SELECT pid, id::text AS id FROM "{schema}".workers')
    return {int(r["pid"]): str(r["id"]) for r in rows}


async def _wait_leader_holds_running(
    conn: asyncpg.Connection,
    schema: str,
    tag: str,
    *,
    want: int,
    cap_secs: float,
) -> list[str]:
    """Block until the maintenance leader holds the locks of ``want``
    running tagged rows; return the leader's in-flight job ids.

    The kill's premise is this ONE consistent observation (leader row and
    lock ownership read together): the elected leader is the lock holder
    of in-flight work. A bare "rows are running" check does not bind the
    two facts - the leader's dispatch loop can lose every claim round to
    its peer (see the module docstring), and killing a leader that holds
    no locks orphans nothing, so no lease can ever lapse and no sweep can
    ever fire. The wait bounds are derived in the scenario body.
    """
    deadline = time.monotonic() + cap_secs
    held: list[str] = []
    while time.monotonic() < deadline:
        rows = await conn.fetch(
            f"SELECT j.id::text AS id, j.locked_by_worker::text AS wid "
            f'FROM "{schema}".jobs j '
            "WHERE j.tags @> ARRAY[$1::text] AND j.status = 'running'",
            tag,
        )
        leader_row = await conn.fetchrow(
            f'SELECT worker_id::text AS wid FROM "{schema}".maintenance_leader'
        )
        held = [
            str(r["id"]) for r in rows if leader_row is not None and r["wid"] == leader_row["wid"]
        ]
        if len(held) >= want:
            return held
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"the maintenance leader never held {want} in-flight lock(s) within "
        f"{cap_secs}s - the SIGKILL would orphan nothing and the reclaim "
        "premise is untestable"
    )


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
        # Generation A boots alone: it elects itself the maintenance
        # leader (the fleet's only elector) and is the only claimant.
        worker_a = spawn_worker(pg_dsn, schema, tag="s2-a")
        wait_worker_ready(worker_a)

        # The jobs: long enough to be mid-flight when the leader dies, and
        # effect-on-completion so a killed attempt writes nothing.
        for _ in range(6):
            await sys_client.enqueue(sys_slow, SysPayload(sleep=6.0), tags=[_TAG])

        # A fills its slots: the leader now holds in-flight locks that the
        # SIGKILL will orphan. 30s of cap for the boot-plus-claim cascade
        # (one subprocess boot, python import + bootstrap, then claims at
        # TASKQ_POLL_INTERVAL=0.05) under CI co-tenant load.
        await _wait_leader_holds_running(conn, schema, _TAG, want=2, cap_secs=30.0)

        # Generation B boots into the live fleet: the survivor, present
        # and idle (A holds every row), before the kill.
        worker_b = spawn_worker(pg_dsn, schema, tag="s2-b")
        wait_worker_ready(worker_b)

        # Re-verify at kill time: A's first wave could have drained only
        # if B's boot outlasted the whole 6s body, and A re-claims its own
        # freed slots the moment a body ends (the deregister-side wake),
        # so the leader's holding is re-observed, not assumed.
        await _wait_leader_holds_running(conn, schema, _TAG, want=1, cap_secs=30.0)

        pid_map = await _pid_to_worker_id(conn, schema)
        leader_wid = await conn.fetchval(
            f'SELECT worker_id::text FROM "{schema}".maintenance_leader'
        )
        leader_pid = {v: k for k, v in pid_map.items()}.get(leader_wid)
        assert leader_pid is not None, (
            f"the elected leader {leader_wid} is not one of this scenario's "
            f"subprocesses (known pids {sorted(pid_map)})"
        )

        # SIGKILL the leader mid-flight. The gates above made the kill's
        # premise true by construction: the leader holds at least one
        # in-flight lock, so the SIGKILL orphans it, and the survivor must
        # win re-election, reclaim the lapsed leases, and drive every
        # disowned job to a terminal outcome.
        #
        # The wait bounds, derived from the fleet settings the harness
        # pins (_harness._BASE_ENV), not guessed:
        # * jobs' lock lease, TASKQ_LOCK_LEASE=8.0: a disowned row's lock
        #   lapses at most 8s after the holder's last heartbeat beat
        #   (TASKQ_HEARTBEAT_INTERVAL=0.5).
        # * takeover, <= leader_lease + heartbeat_interval + one round
        #   trip (the failover bound worker/leader.py documents): the
        #   default resolved_leader_lease is 40.0, and the four-beat
        #   floor (4 * TASKQ_HEARTBEAT_INTERVAL = 2.0) never raises it,
        #   so the survivor leads by ~40.5s at the latest, and its
        #   first TASKQ_SWEEP_INTERVAL=1.0 tick after that reclaims
        #   every lapsed row.
        # * re-run: the re-pend rides the actor's retry curve
        #   (_FAST_RETRY, base 1s, no jitter) plus at most two 4-slot
        #   waves of the 6s body (12s).
        # The whole cascade is ~62s worst case from the kill, inside the
        # 120s settle cap assert_balanced applies and the 300s test
        # timeout.
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
