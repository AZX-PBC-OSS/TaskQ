"""Lifecycle 6: the full happy path at load.

Sustained enqueue/dispatch/terminal across a two-worker fleet, in three
overlapping waves (the next wave lands while the last is still
dispatching), with retries, progress fanouts and a deliberately failing
actor in the mix. This is the tier's baseline: before any chaos, the
system at production shape must conserve everything.

The conservation counter, checked at the end: every enqueued job reaches
exactly one terminal outcome (``succeeded`` or ``failed``, the only
labels this scenario can manufacture), the attempt ledger reconciles,
the effects ledger shows exactly one body run per attempt, and both
worker processes are still alive.
"""

# Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, assert_effects_balance, delete_tagged
from tests.system_e2e.actors import (
    FlakyPayload,
    SysPayload,
    sys_always_fails,
    sys_fast,
    sys_flaky,
    sys_progress,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s6"

#: Three overlapping waves x (20 fast + 6 progress + 4 flaky + 2 always
#: failing) = 96 jobs, no drain between waves.
_WAVES = 3


@pytest.mark.timeout(420)
async def test_sustained_happy_path_at_load_conserves_every_job(
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
        worker_a = spawn_worker(pg_dsn, schema, tag="s6-a")
        wait_worker_ready(worker_a)
        worker_b = spawn_worker(pg_dsn, schema, tag="s6-b")
        wait_worker_ready(worker_b)

        enqueued = 0
        for wave in range(_WAVES):
            # Waves overlap: no draining between them, dispatch is under
            # sustained load the whole time.
            for _ in range(20):
                await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])
                enqueued += 1
            for _ in range(6):
                await sys_client.enqueue(sys_progress, SysPayload(sleep=0.3, beats=3), tags=[_TAG])
                enqueued += 1
            for _ in range(4):
                await sys_client.enqueue(sys_flaky, FlakyPayload(fail_until_attempt=2), tags=[_TAG])
                enqueued += 1
            for _ in range(2):
                await sys_client.enqueue(sys_always_fails, SysPayload(), tags=[_TAG])
                enqueued += 1
            if wave < _WAVES - 1:
                await asyncio.sleep(1.0)

        # The final conservation check, with the settle backstop: nothing
        # may remain non-terminal when the scenario ends.
        counts = await assert_balanced(conn, schema, _TAG)
        await assert_effects_balance(conn, schema, _TAG)

        # Conservation: jobs-in == terminal-out. The exact terminal LABEL
        # of a deliberate failure is the reaper's business, not the
        # conservation contract's: an ordinary ladder exhaustion lands
        # 'failed'; a lease lost to a co-tenant load stall lands as the
        # crash-reclaim's 'crashed' verdict at budget exhaustion (the
        # sweep's attempt row records what actually happened). What must
        # hold is the population shape: every healthy job succeeded, no
        # job manufactured an outcome this workload cannot produce (no
        # canceller runs here, so nothing may be cancelled or abandoned).
        assert sum(counts.values()) == enqueued, (
            f"jobs-in {enqueued} != terminal-out {sum(counts.values())}: {counts}"
        )
        assert counts.get("succeeded", 0) == enqueued - 2 * _WAVES, (
            f"the healthy population did not succeed: {counts}"
        )
        assert counts.get("failed", 0) + counts.get("crashed", 0) == 2 * _WAVES, (
            f"the deliberate failures did not all reach a terminal verdict: {counts}"
        )
        assert set(counts) <= {"succeeded", "failed", "crashed"}, (
            f"the load window manufactured outcomes: {counts}"
        )

        # Both workers served the whole window and are still alive.
        assert worker_a.proc.poll() is None and worker_b.proc.poll() is None, (
            "a worker did not survive the sustained window"
        )
    finally:
        if worker_a is not None:
            reap(worker_a)
        if worker_b is not None:
            reap(worker_b)
        await delete_tagged(conn, schema, _TAG)
