"""Two-process execution-policy controls on the real workflow dispatch fleet.

This is a short focused system scenario, not the separate ten-minute saturation
or 200k-record acceptance artifacts.
"""

# ruff: noqa: S608 — all schema identifiers are validated fixture values; data is bound.

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import FlowRunner
from tests.system_e2e._harness import WorkerProc, reap
from tests.system_e2e._wf_app import MARCH_FLOWS
from tests.system_e2e._wf_harness import spawn_wf_fleet

pytestmark = [pytest.mark.system, pytest.mark.integration]


async def _run(pool: asyncpg.Pool, schema: str, name: str) -> str:
    runner = FlowRunner(MARCH_FLOWS[name], pool, schema)
    return str((await runner.create_flow()).flow_id)


async def _wait_policy_root(
    conn: asyncpg.Connection, schema: str, flow_id: str, *, bound_s: float
) -> tuple[str, float]:
    """Read the maintained root, not the shim's key-collapsed chain projection."""
    async with asyncio.timeout(bound_s):
        start = asyncio.get_running_loop().time()
        while True:
            status = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid', flow_id
            )
            if status in ("succeeded", "failed", "cancelled", "crashed", "abandoned"):
                return str(status), asyncio.get_running_loop().time() - start
            await asyncio.sleep(0.05)
        # asyncio.timeout exposes the deadline only for cancellation handling;
        # the timeout's failure text below is provided by the caller.
    raise AssertionError("unreachable root wait")


def _max_overlap(rows: list[dict[str, Any]]) -> int:
    transitions: list[tuple[datetime, int]] = []
    for row in rows:
        assert isinstance(row["started_at"], datetime)
        assert isinstance(row["finished_at"], datetime)
        transitions.append((row["started_at"], 1))
        # At an equal timestamp, finishing releases before the next entry begins.
        transitions.append((row["finished_at"], -1))
    current = 0
    maximum = 0
    for _at, delta in sorted(transitions, key=lambda item: (item[0], item[1])):
        current += delta
        maximum = max(maximum, current)
    return maximum


async def _attempt_rows(
    conn: asyncpg.Connection, schema: str, flow_id: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f"""SELECT a.job_id, a.attempt, a.started_at, a.finished_at, a.outcome,
                   a.worker_id::text AS worker_id, j.map_index, j.created_at
            FROM "{schema}".job_attempts AS a
            JOIN "{schema}".jobs AS j ON j.id = a.job_id
            WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'
            ORDER BY a.started_at, j.map_index""",
        flow_id,
    )
    return [dict(row) for row in rows]


async def _wait_for_high_priority_row(
    conn: asyncpg.Connection, schema: str, flow_id: str, *, bound_s: float
) -> None:
    async with asyncio.timeout(bound_s):
        while True:
            terminal = await conn.fetchval(
                f"""SELECT count(*) FROM "{schema}".jobs
                    WHERE metadata->>'flow_id' = $1 AND step_key = 'work'
                      AND status = 'succeeded' AND finished_at IS NOT NULL""",
                flow_id,
            )
            if terminal == 1:
                return
            await asyncio.sleep(0.05)


@pytest.mark.timeout(300)
async def test_two_process_chain_policy_capacity_and_late_priority(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    schema = module_pg_schema.schema_name
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    conn = await asyncpg.connect(pg_dsn)
    try:
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["policy-a", "policy-b"])

        # Eight real chain records share one strict slot across two processes.
        reserved_flow = await _run(pool, schema, "policy_reserved_fleet")
        verdict, measured = await _wait_policy_root(conn, schema, reserved_flow, bound_s=90)
        print(f"[policy-reserved] terminal measured={measured:.3f}s bound=90s")
        assert verdict == "succeeded"
        attempts = await _attempt_rows(conn, schema, reserved_flow)
        if len(attempts) != 8:
            every_row = await conn.fetch(
                f"""SELECT id, parent_id, step_key, map_index, attempt, status::text AS status,
                           actor, locked_by_worker, error_class, error_message, metadata
                    FROM "{schema}".jobs
                    WHERE id = $1::uuid OR (metadata->>'flow_id')::uuid = $1::uuid
                    ORDER BY id""",
                reserved_flow,
            )
            raise AssertionError(
                f"reserved run did not have all eight attempts: attempts={attempts!r}; "
                f"all rows={[dict(row) for row in every_row]!r}"
            )
        assert all(row["outcome"] == "succeeded" for row in attempts)
        windows = await conn.fetch(
            f"""SELECT created_at AS started_at, updated_at AS finished_at
                FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1::uuid AND step_key = 'policy-reserved-window'
                ORDER BY created_at""",
            reserved_flow,
        )
        typed_windows = [dict(row) for row in windows]
        assert len(typed_windows) == 8
        observed_overlap = _max_overlap(typed_windows)
        if observed_overlap != 1:
            diagnostic = await conn.fetch(
                f"""SELECT j.id, j.map_index, j.actor, j.metadata, j.locked_by_worker,
                           j.status::text AS status, a.started_at, a.finished_at,
                           a.worker_id::text AS worker_id
                    FROM "{schema}".job_attempts AS a
                    JOIN "{schema}".jobs AS j ON j.id = a.job_id
                    WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'
                    ORDER BY a.started_at""",
                reserved_flow,
            )
            slots = await conn.fetch(
                f"""SELECT * FROM "{schema}".reservation_slots
                    WHERE bucket_name = 'wf-policy-reservation' ORDER BY slot_index"""
            )
            raise AssertionError(
                f"reserved overlap={observed_overlap}; attempts={[dict(r) for r in attempts]!r}; "
                f"rows={[dict(r) for r in diagnostic]!r}; slots={[dict(r) for r in slots]!r}"
            )
        workers = {row["worker_id"] for row in attempts}
        assert len(workers) == 2, workers
        held = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".reservation_slots
                WHERE bucket_name = 'wf-policy-reservation'
                  AND (job_id IS NOT NULL OR held_by_worker_id IS NOT NULL)"""
        )
        assert held == 0
        policy = await conn.fetchrow(
            f"""SELECT max_concurrent, result_ttl FROM "{schema}".actor_config
                WHERE actor = 'wf-policy-reserved'"""
        )
        assert policy is not None  # the chain cohort was projected at real boot
        assert policy["max_concurrent"] == 1  # the capped handle survived resolution/boot
        node = await conn.fetchrow(
            f"""SELECT start_to_close, priority, max_attempts FROM "{schema}".jobs
                WHERE metadata->>'flow_id' = $1 AND step_key = 'work' LIMIT 1""",
            reserved_flow,
        )
        assert node is not None
        assert node["start_to_close"] == timedelta(seconds=5)
        assert node["priority"] == 4 and node["max_attempts"] == 1

        # Active workers must stop before the priority fixture is born, so its
        # deeper backlog remains pending until both new processes start.
        for pod in fleet.values():
            reap(pod)
        fleet = {}

        priority_flow = await _run(pool, schema, "policy_priority_fleet")
        await conn.executemany(
            f"""INSERT INTO "{schema}".jobs
                (id, actor, queue, status, payload, max_attempts, retry_kind,
                 scheduled_at, idempotency_scope, idempotency_key, priority)
                VALUES ($1, 'wf_policy_backlog', 'default', 'pending',
                        '{{}}', 3, 'transient', clock_timestamp(), $2, $3, 0)""",
            [
                (
                    new_uuid(),
                    f"policy-priority-backlog:{schema}",
                    f"backlog-{index}",
                )
                for index in range(12)
            ],
        )
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["policy-pa", "policy-pb"])
        await _wait_for_high_priority_row(conn, schema, priority_flow, bound_s=45)
        high = await _attempt_rows(conn, schema, priority_flow)
        assert len(high) == 1
        assert all(row["outcome"] == "succeeded" for row in high)
        root_created = await conn.fetchval(
            f'SELECT created_at FROM "{schema}".jobs WHERE id = $1::uuid', priority_flow
        )
        assert all(row["created_at"] > root_created for row in high), (
            "all these policy rows must be source-materialized after create_flow"
        )

        low = await conn.fetch(
            f"""SELECT j.id, j.started_at FROM "{schema}".jobs AS j
                WHERE j.actor = 'wf_policy_backlog' AND j.started_at IS NOT NULL
                ORDER BY j.started_at, j.id"""
        )
        high_claims = sorted(row["started_at"] for row in high)
        async with asyncio.timeout(20):
            while not low:
                await asyncio.sleep(0.05)
                low = await conn.fetch(
                    f"""SELECT j.id, j.started_at FROM "{schema}".jobs AS j
                        WHERE j.actor = 'wf_policy_backlog' AND j.started_at IS NOT NULL
                        ORDER BY j.started_at, j.id"""
                )
        # A backlog claim that began before the source materialized its first
        # high row is legal. Every claim made after that row exists must respect
        # its priority; there must still be an actual later low claim to read.
        first_materialized = min(row["created_at"] for row in high)
        later_low = [row for row in low if row["started_at"] >= first_materialized]
        assert later_low, (first_materialized, low)
        assert max(high_claims) <= later_low[0]["started_at"], (
            f"high claims={high_claims!r}; materialized={first_materialized!r}; "
            f"later backlog={later_low[0]['started_at']!r}; earlier legal backlog may exist"
        )
        observed = await conn.fetchrow(
            f"""SELECT min(j.started_at) AS first, max(j.started_at) AS last
                FROM "{schema}".jobs AS j
                WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'""",
            priority_flow,
        )
        assert observed is not None and observed["first"] is not None
        actual_order = await conn.fetch(
            f"""SELECT j.actor, j.map_index, a.started_at
                FROM "{schema}".job_attempts AS a
                JOIN "{schema}".jobs AS j ON j.id = a.job_id
                WHERE (j.metadata->>'flow_id' = $1 AND j.step_key = 'work')
                   OR (j.actor = 'wf_policy_backlog' AND a.started_at >= $2)
                ORDER BY a.started_at, j.actor, j.map_index""",
            priority_flow,
            first_materialized,
        )
        high_positions = [
            index for index, row in enumerate(actual_order) if row["actor"] == "wf-policy-priority"
        ]
        assert high_positions and all(
            row["map_index"] in (0, 1, 2, 3) for row in actual_order[: max(high_positions) + 1]
        )

        for pod in fleet.values():
            reap(pod)
        fleet = {}
        # The release observation is read after both healthy processes stop;
        # enforcement itself was proved above, before cleanup.
        released = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".reservation_slots
                WHERE bucket_name = 'wf-policy-reservation'
                  AND (job_id IS NOT NULL OR held_by_worker_id IS NOT NULL)"""
        )
        assert released == 0
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()
