"""Lifecycle 5: a mixed fleet - sync and async actors misbehaving together.

One worker serves a fleet where bodies fail in every failure mode at
once: a panic (an ordinary exception), a hang past its start_to_close
deadline, a sync body that raises SystemExit (a BaseException crossing
the executor-thread task boundary), and healthy sync + async neighbours.
The worker must survive all of it (a body's failure mode is a job
outcome, never a process outcome), serve work AFTER the last
misbehaviour landed, and the ledger must balance.

Two tests, split by defect ownership:

- the survivable fleet (panic + hang + healthy neighbours) is the
  lifecycle as it should hold today;
- the SystemExit member of the fleet is its own test: on main it KILLS
  the worker process (rc = the actor's exit code, claimed rows
  stranded) - the defect class ``fix/459-sync-actor-systemexit`` owns.
  The test documents the contract that must hold (the worker survives,
  the actor's own SystemExit lands as the attempt's recorded error) and
  reds, naming the owner, until that branch lands.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, assert_effects_balance, delete_tagged
from tests.system_e2e.actors import (
    SysPayload,
    sys_fast,
    sys_hang,
    sys_panic,
    sys_sync_ok,
    sys_sysexit,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s5"


@pytest.mark.timeout(300)
async def test_mixed_fleet_survives_hang_and_panic_ledger_balances(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """The survivable mixed fleet: a panic, a hung body past its deadline,
    and healthy sync + async neighbours on ONE worker. The worker must
    outlive every failure mode, finish the fleet, and serve again."""
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    worker: WorkerProc | None = None
    try:
        worker = spawn_worker(pg_dsn, schema, tag="s5")
        wait_worker_ready(worker)

        await sys_client.enqueue(sys_sync_ok, SysPayload(), tags=[_TAG])
        await sys_client.enqueue(sys_sync_ok, SysPayload(), tags=[_TAG])
        # The deadline workload: hung past a 4s start_to_close, no retry
        # budget - the reaper owns the row and exhaustion is terminal.
        hang_handle = await sys_client.enqueue(
            sys_hang, SysPayload(), tags=[_TAG], start_to_close=timedelta(seconds=4)
        )
        panic_handles = [
            await sys_client.enqueue(sys_panic, SysPayload(), tags=[_TAG]) for _ in range(2)
        ]
        fast_handles = [
            await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG]) for _ in range(2)
        ]

        counts = await assert_balanced(conn, schema, _TAG)
        await assert_effects_balance(conn, schema, _TAG)

        # The ledger balances per failure mode: the panic is 'failed' with
        # its error recorded truthfully; the hung job was reaped (terminal
        # with finished_at stamped - the conservation counter's half-state
        # clause would red otherwise).
        for handle in panic_handles:
            row = await conn.fetchrow(
                f"SELECT status::text AS status, error_class AS error_class "
                f'FROM "{schema}".jobs WHERE id = $1',
                handle.job_id,
            )
            assert row is not None and row["status"] == "failed", (
                f"the panic did not land as a failed job: {row}"
            )
            assert row["error_class"] == "RuntimeError", (
                f"the panic's error class was not recorded truthfully: {row}"
            )
        hang_status = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1 AND finished_at IS NOT NULL',
            hang_handle.job_id,
        )
        assert hang_status is not None, f"the hung job was never reaped: {hang_handle.job_id}"

        # The worker survived: AFTER every misbehaviour landed, the same
        # process still takes and completes new work.
        assert worker.proc.poll() is None, "the worker died serving the mixed fleet"
        post = await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])
        status = None
        for _ in range(120):
            status = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', post.job_id
            )
            if status == "succeeded":
                break
            await asyncio.sleep(0.25)
        assert status == "succeeded", (
            f"the worker never served work again after the misbehaviours: {status}"
        )
        assert counts.get("succeeded", 0) >= len(fast_handles) + 2, (
            f"the healthy fleet did not complete: {counts}"
        )
    finally:
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)


@pytest.mark.timeout(300)
async def test_sync_systemexit_is_an_attempt_outcome_not_worker_death(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """The SystemExit member of the mixed fleet.

    The contract: a sync body's SystemExit is an ordinary attempt outcome
    (the actor's own exception recorded as the attempt's error, the
    ordinary retry classification applying to it); the worker survives
    it, its co-resident jobs still terminate, and the process serves work
    afterward. RED ON MAIN: the SystemExit crosses the executor-thread
    task boundary bare, the event loop dies with the actor's exit code,
    and the worker exits rc=3 with its claimed rows stranded - the defect
    class fix/459-sync-actor-systemexit owns. When that branch lands this
    scenario must go green unchanged.
    """
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    worker: WorkerProc | None = None
    try:
        worker = spawn_worker(pg_dsn, schema, tag="s5-exit")
        wait_worker_ready(worker)

        sysexit_handles = [
            await sys_client.enqueue(sys_sysexit, SysPayload(), tags=[_TAG]) for _ in range(2)
        ]
        # Co-resident neighbours: the healthy work the worker must not
        # take down with the SystemExit (the TaskGroup sibling survival
        # is part of the contract).
        fast_handles = [
            await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG]) for _ in range(2)
        ]

        # Probe the defect directly before settling: a worker that died
        # serving the SystemExit strands its claimed rows forever, and
        # the settle backstop would report that only as generic limbo.
        # The named failure below is the red this scenario owes on main;
        # once fix/459-sync-actor-systemexit lands, poll() is None and
        # the full contract runs.
        for _ in range(40):
            if worker.proc.poll() is not None:
                break
            await asyncio.sleep(0.25)
        assert worker.proc.poll() is None, (
            f"the worker died serving a sync actor's SystemExit "
            f"(rc={worker.proc.returncode}): a body's failure mode became a "
            "process outcome - the defect class fix/459-sync-actor-systemexit owns"
        )

        counts = await assert_balanced(conn, schema, _TAG)
        await assert_effects_balance(conn, schema, _TAG)

        assert worker.proc.poll() is None, (
            f"the worker died serving a sync actor's SystemExit "
            f"(rc={worker.proc.returncode}): a body's failure mode became a "
            "process outcome - the defect class fix/459-sync-actor-systemexit owns"
        )
        for handle in sysexit_handles:
            row = await conn.fetchrow(
                f"SELECT status::text AS status, error_class AS error_class "
                f'FROM "{schema}".jobs WHERE id = $1',
                handle.job_id,
            )
            assert row is not None and row["status"] == "failed", (
                f"the SystemExit did not land as a failed job: {row}"
            )
            assert row["error_class"] == "SystemExit", (
                f"the actor's own exception was not recorded as the attempt's error: {row}"
            )
        assert counts.get("succeeded", 0) >= len(fast_handles), (
            f"the co-resident jobs did not survive the SystemExit: {counts}"
        )
    finally:
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)
