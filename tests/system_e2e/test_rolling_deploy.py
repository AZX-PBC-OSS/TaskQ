"""Lifecycle 1: a rolling deploy.

Generation A runs jobs while generation B boots; A drains and exits; B
takes ownership. Jobs in flight across the boundary must all terminate -
the deploy may change what serves the queue, never what happens to a job
already admitted.

The scenario: A boots and fills its slots with long jobs; B boots into
the same fleet (the new generation is live while the old one still holds
work - the overlap window every real deploy has); A is SIGTERMed and
drains; MORE jobs are enqueued during A's drain window, the shape a
deploy cannot pause; B must be the one that serves them.

System invariants: A exits cleanly (graceful drain, not a crash), every
job in flight at the SIGTERM reaches ``succeeded`` (drained to
completion, never abandoned, never re-run), the post-drain enqueues
terminate, and the conservation counter balances across the whole
population.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import pytest

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
from tests.system_e2e.actors import SysPayload, sys_fast, sys_slow

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s1"


async def _wait_running(
    conn: asyncpg.Connection, schema: str, tag: str, want: int, cap_secs: float
) -> set[str]:
    """Block until ``want`` tagged jobs are running; return their ids."""
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        rows = await conn.fetch(
            f'SELECT id FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
            tag,
        )
        if len(rows) >= want:
            return {str(r["id"]) for r in rows}
        await asyncio.sleep(0.05)
    raise AssertionError(f"fewer than {want} jobs claimed within {cap_secs}s")


@pytest.mark.timeout(240)
async def test_rolling_deploy_jobs_in_flight_across_the_boundary_all_terminate(
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
        # Generation A boots and fills its slots with long jobs.
        worker_a = spawn_worker(pg_dsn, schema, tag="s1-a")
        wait_worker_ready(worker_a)

        slow_handles = [
            await sys_client.enqueue(sys_slow, SysPayload(sleep=6.0), tags=[_TAG]) for _ in range(4)
        ]
        in_flight = await _wait_running(conn, schema, _TAG, want=4, cap_secs=30.0)

        # Generation B boots into the live fleet: the deploy's overlap
        # window, both generations serving one queue.
        worker_b = spawn_worker(pg_dsn, schema, tag="s1-b")
        wait_worker_ready(worker_b)

        # A is SIGTERMed with all four slots full: the drain begins, and
        # the deploy keeps enqueueing while it runs.
        rc = graceful_stop(worker_a, timeout=60.0)
        worker_a = None  # exited cleanly; nothing left to reap
        assert rc == 0, (
            f"generation A exited rc={rc} - the rolling deploy's drain was "
            "not graceful (a crashed or force-killed drain)"
        )

        for _ in range(3):
            await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])

        # The in-flight set must drain to completion, never abandoned.
        counts = await assert_balanced(conn, schema, _TAG)
        for job_id in in_flight:
            status = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
            )
            if status is None:
                status = await conn.fetchval(
                    f'SELECT status::text FROM "{schema}".jobs_archive WHERE id = $1',
                    job_id,
                )
            assert status == "succeeded", (
                f"in-flight job {job_id} ended {status!r} across the deploy "
                "boundary - the drain did not carry it to completion"
            )
        assert counts.get("succeeded", 0) == len(slow_handles) + 3, (
            f"the fleet did not complete every job it admitted: {counts}"
        )
        assert set(counts) <= {"succeeded"}, (
            f"a rolling deploy must not manufacture other outcomes: {counts}"
        )
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        if worker_a is not None:
            reap(worker_a)
        if worker_b is not None:
            reap(worker_b)
        await delete_tagged(conn, schema, _TAG)
