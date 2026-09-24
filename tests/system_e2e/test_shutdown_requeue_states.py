"""Lifecycle 2: the SIGTERM requeue-state matrix over real worker processes.

The in-process pins (``tests/test_rt_requeue_states_pg.py``) construct each
in-flight state deterministically and pin the row-level contracts. This
module runs the states a deploy can actually produce from the outside
through REAL worker processes: a pod SIGTERMed mid-body, a successor pod
picking the work up, and the balancing counter over the whole population.

The states that live only inside the worker's memory (a row parked in the
local queue, the take-to-register claim intent, a terminal write held
mid-flight) are not reachable from outside a process and stay with the
in-process pins; what is provable here is the end-to-end shape an operator's
rolling deploy sees: the interrupted attempt comes back to the fleet at a
NEW attempt, the successor completes it, the counter balances, and no
population member ends in anything the deploy did not choose.

Scenario 1 (state 3, RUNNING pre-terminal): every slot of generation A is
mid-body at the SIGTERM; the drain interrupts them (the spent attempt
stands, the row re-pends), generation B picks the rows up within the derived
bound (the interrupted rows are claimable immediately - the async actors
provably unwound - so the bound is B's own boot plus its poll floor), and
every job ends ``succeeded`` having run its body exactly once.

Scenario 2 (state 5, CANCELLED during the shutdown): the operator's cancel
lands on generation A's running rows and the SIGTERM lands while the
ladder's poll may not have observed them. The cancel ladder owns the exit:
the rows terminalise ``cancelled``, never re-pended, never re-run, and the
balancing counter reconciles over the population.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import pytest

from taskq.backend._protocol import JobFilter
from tests.system_e2e._harness import (
    WorkerProc,
    graceful_stop,
    reap,
    spawn_worker,
    wait_worker_ready,
)
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

_TAG = "sys-req"


async def _wait_running(
    conn: asyncpg.Connection, schema: str, tag: str, want: int, cap_secs: float
) -> set[str]:
    """Block until ``want`` tagged jobs are running; return their ids."""
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        rows = await conn.fetch(
            f'SELECT id::text FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
            tag,
        )
        if len(rows) >= want:
            return {str(r["id"]) for r in rows}
        await asyncio.sleep(0.05)
    raise AssertionError(f"fewer than {want} jobs claimed within {cap_secs}s")


@pytest.mark.timeout(240)
async def test_sigterm_mid_body_interrupts_and_the_successor_completes(
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
        worker_a = spawn_worker(pg_dsn, schema, tag="req-a")
        wait_worker_ready(worker_a)

        handles = [
            await sys_client.enqueue(sys_slow, SysPayload(sleep=8.0), tags=[_TAG]) for _ in range(3)
        ]
        in_flight = await _wait_running(conn, schema, _TAG, want=3, cap_secs=30.0)

        # The SIGTERM with every slot mid-body: the drain's CANCELLING
        # signal reaches the async bodies, the FORCING cancel lands, and
        # the consumer's interruption release re-pends each row (the spent
        # attempt standing) before the process exits cleanly.
        rc = graceful_stop(worker_a, timeout=60.0)
        worker_a = None
        assert rc == 0, f"generation A exited rc={rc} - the drain was not graceful"

        # The successor picks the work up: generation B claims and
        # completes everything (the bound is B's boot plus its poll floor,
        # covered by the settle cap inside assert_balanced).
        worker_b = spawn_worker(pg_dsn, schema, tag="req-b")
        wait_worker_ready(worker_b)

        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("succeeded", 0) == len(handles), (
            f"the successor must complete every interrupted job: {counts}"
        )
        for job_id in in_flight:
            row = await conn.fetchrow(
                f"SELECT status::text AS status, attempt, interrupt_count "
                f'FROM "{schema}".jobs WHERE id = $1::uuid',
                job_id,
            )
            archived = row is None
            if archived:
                row = await conn.fetchrow(
                    f"SELECT status::text AS status, attempt, interrupt_count "
                    f'FROM "{schema}".jobs_archive WHERE id = $1::uuid',
                    job_id,
                )
            assert row is not None, f"job {job_id} vanished from both tables"
            assert row["status"] == "succeeded", (
                f"an interrupted job must be completed by the fleet, ended "
                f"{row['status']!r} (archived={archived})"
            )
            # The state the job was in at the SIGTERM decides the shape,
            # and both requeue whole: TAKEN-and-running jobs are
            # interrupted (interrupt_count 1, the charged attempt stands,
            # the successor's run is a NEW attempt); jobs still parked in
            # the departing worker's local queue (claimed, untaken - the
            # claim beats the take by milliseconds) are handed back with
            # the attempt refunded, and the successor's run IS the first
            # attempt. The biconditional is the pin: an interrupted job
            # with no re-run, or a refunded claim that still spent an
            # attempt, is a broken requeue either way.
            if row["interrupt_count"] == 1:
                assert row["attempt"] >= 2, (
                    f"the interrupted attempt stands charged and the "
                    f"successor's run is a NEW attempt, got {row}"
                )
            else:
                assert row["interrupt_count"] == 0 and row["attempt"] == 1, (
                    f"a job never interrupted was handed back with its claim "
                    f"refunded, so the successor's run is the first attempt, got {row}"
                )
        assert set(counts) <= {"succeeded"}, (
            f"a deploy must not manufacture other outcomes: {counts}"
        )
        # Exactly-once effects: one body run per job, at the attempt that
        # completed it; the interrupted attempts wrote no effect and no
        # ledger row (an interruption is not an execution outcome).
        dones = await conn.fetch(
            f'SELECT job_id::text, attempt FROM "{schema}".sys_effects '
            f'WHERE job_id IN (SELECT id FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] '
            f'UNION ALL SELECT id FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text])',
            _TAG,
        )
        assert len(dones) == len(handles), (
            f"each job's body must have run exactly once, got {len(dones)} effect rows"
        )
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        if worker_a is not None:
            reap(worker_a)
        if worker_b is not None:
            reap(worker_b)
        await delete_tagged(conn, schema, _TAG)


@pytest.mark.timeout(240)
async def test_operator_cancel_racing_the_sigterm_owns_the_exit(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    schema = module_pg_schema.schema_name
    conn = sys_ledger

    worker_a: WorkerProc | None = None
    try:
        worker_a = spawn_worker(pg_dsn, schema, tag="req-c")
        wait_worker_ready(worker_a)

        handles = [
            await sys_client.enqueue(sys_slow, SysPayload(sleep=8.0), tags=[_TAG]) for _ in range(2)
        ]
        in_flight = await _wait_running(conn, schema, _TAG, want=2, cap_secs=30.0)

        # The operator's cancel lands on the running rows; the SIGTERM
        # lands while the ladder's poll may not have observed them yet.
        result = await sys_client.cancel_where(JobFilter(tags=(_TAG,)), reason="offboard")
        assert result.cancel_requested == 2, (
            f"the premise: both running rows carry the operator's request, got {result}"
        )
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            phases = await conn.fetch(
                f'SELECT cancel_phase FROM "{schema}".jobs WHERE id::text = ANY($1::text[])',
                sorted(in_flight),
            )
            if len(phases) == len(in_flight) and all(int(r["cancel_phase"]) >= 1 for r in phases):
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("the operator's cancel never reached the rows")

        rc = graceful_stop(worker_a, timeout=60.0)
        worker_a = None
        assert rc == 0, f"generation A exited rc={rc} - the drain was not graceful"

        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("cancelled", 0) == len(handles), (
            f"the cancel ladder owns the exit: every racing row terminalises "
            f"cancelled, got {counts}"
        )
        # No requeue: a cancelled row must not re-enter the fleet for a
        # successor to run (one writer owns the exit).
        requeued = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs j '
            "WHERE j.tags @> ARRAY[$1::text] AND j.status::text NOT IN "
            "('cancelled', 'failed', 'crashed', 'abandoned', 'succeeded')",
            _TAG,
        )
        assert requeued == 0, f"{requeued} row(s) re-entered the fleet after the cancel"
        dones = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects '
            f'WHERE job_id IN (SELECT id FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] '
            f'UNION ALL SELECT id FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text])',
            _TAG,
        )
        assert dones == 0, (
            f"{dones} body run(s) recorded for cancelled jobs - the bodies "
            "never completed, and a completed run here would be a re-run"
        )
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        if worker_a is not None:
            reap(worker_a)
        await delete_tagged(conn, schema, _TAG)
