"""Real two-process durable terminal-effect scenarios on the workflow fleet.

Wait bounds describe responsive async bodies only. No claim is made for blocking
or non-yielding application code, and root terminal is not treated as body exit.
"""

# ruff: noqa: S608  # migrated fixture schema identifiers; every value binds

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import FlowRunner
from tests.system_e2e._harness import WorkerProc, reap
from tests.system_e2e._p5_cancel_app import P5_FLOWS
from tests.system_e2e._wf_harness import spawn_wf_fleet

pytestmark = [pytest.mark.system, pytest.mark.integration]

_OBSERVE_BOUND_S = 120.0
_TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


async def _ddl(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS "{schema}".p5_effect_results (
            flow_id uuid NOT NULL,
            node_key text NOT NULL,
            run_epoch bigint NOT NULL,
            record_id int NOT NULL,
            error_class text NOT NULL,
            invocations int NOT NULL,
            PRIMARY KEY (flow_id,node_key,run_epoch)
        )"""
    )
    await conn.execute(f'TRUNCATE "{schema}".p5_effect_results')


async def _create(pool: asyncpg.Pool, schema: str, kind: str, mode: str) -> str:
    runner = FlowRunner(P5_FLOWS[kind], pool, schema)
    claim = await runner.create_flow(input=mode, run_key=f"p5:{kind}:{new_uuid()}")
    return str(claim.flow_id)


async def _root(conn: asyncpg.Connection, schema: str, flow_id: str) -> dict[str, Any]:
    row = await conn.fetchrow(
        f"SELECT id,status::text AS status,run_epoch,required_worker_schema "
        f'FROM "{schema}".jobs WHERE id=$1::uuid',
        flow_id,
    )
    assert row is not None
    return dict(row)


async def _wait_root(
    conn: asyncpg.Connection, schema: str, flow_id: str, expected: str | None = None
) -> str:
    async with asyncio.timeout(_OBSERVE_BOUND_S):
        while True:
            root = await _root(conn, schema, flow_id)
            status = str(root["status"])
            if status in _TERMINAL and (expected is None or status == expected):
                return status
            await asyncio.sleep(0.05)


async def _settled_nodes(
    conn: asyncpg.Connection, schema: str, flow_id: str
) -> list[dict[str, Any]]:
    async with asyncio.timeout(_OBSERVE_BOUND_S):
        while True:
            rows = await conn.fetch(
                f"""SELECT id::text,step_key,map_index,status::text AS status,attempt,
                           locked_by_worker::text,error_class,metadata
                    FROM "{schema}".jobs
                    WHERE metadata->>'flow_id'=$1 ORDER BY id""",
                flow_id,
            )
            if rows and all(row["status"] in _TERMINAL for row in rows):
                return [dict(row) for row in rows]
            await asyncio.sleep(0.02)


def _doc(raw: object) -> dict[str, object]:
    import json

    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


@pytest.mark.timeout(600)
async def test_p5_operator_all_five_kinds_effect_and_parked_terminal_fence(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    schema = module_pg_schema.schema_name
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=6)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _ddl(conn, schema)
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["p5-a", "p5-b"])

        # The effect queue job is created from its durable intent after a
        # genuinely failed policy terminal and executes on the surviving fleet.
        effect_id = await _create(pool, schema, "p5_effect", "operator")
        effect_result: asyncpg.Record | None = None
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while effect_result is None:
                effect_result = await conn.fetchrow(
                    f'SELECT * FROM "{schema}".p5_effect_results WHERE flow_id=$1::uuid',
                    effect_id,
                )
                if effect_result is None:
                    await asyncio.sleep(0.02)
        assert effect_result["invocations"] == 1
        assert effect_result["run_epoch"] == 1
        assert effect_result["error_class"] == "RuntimeError"
        effect_nodes = await _settled_nodes(conn, schema, effect_id)
        effect_row = next(row for row in effect_nodes if row["step_key"] == "work.__on_exhaustion")
        assert effect_row["status"] == "succeeded"
        assert _doc(effect_row["metadata"])["wf_after_terminal_effect"] is True
        assert await _wait_root(conn, schema, effect_id, "failed") == "failed"

        deadline_id = await _create(pool, schema, "p5_effect_deadline", "operator")
        deadline_result: asyncpg.Record | None = None
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while deadline_result is None:
                deadline_result = await conn.fetchrow(
                    f'SELECT * FROM "{schema}".p5_effect_results WHERE flow_id=$1::uuid',
                    deadline_id,
                )
                if deadline_result is None:
                    await asyncio.sleep(0.05)
        deadline_nodes = await _settled_nodes(conn, schema, deadline_id)
        deadline_row = next(
            row for row in deadline_nodes if row["step_key"] == "work.__on_exhaustion"
        )
        assert deadline_row["status"] == "succeeded" and deadline_row["attempt"] == 2
        assert deadline_result["invocations"] == 1
        deadline_attempts = await conn.fetch(
            f"""SELECT attempt,status,error_class,error_message
                FROM "{schema}".wf_step_ledger
                WHERE flow_id=$1::uuid AND step_key='work.__on_exhaustion'
                ORDER BY attempt""",
            deadline_id,
        )
        assert len(deadline_attempts) == 2
        assert deadline_attempts[0]["status"] == "failed"
        assert deadline_attempts[0]["error_class"] == "TimeoutError"
        assert deadline_attempts[0]["error_message"] == "start_to_close"
        assert deadline_attempts[1]["status"] == "succeeded"

        kill_id = await _create(pool, schema, "p5_effect", "kill-after-domain")
        killed_before = {pod.proc.pid: pod.returncode for pod in fleet.values()}
        # One worker kills itself only after its guarded application commit.
        # The surviving worker re-claims the exact effect job after lease loss;
        # there is one result row but two actual effect invocations.
        kill_result: asyncpg.Record | None = None
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while kill_result is None or kill_result["invocations"] < 2:
                kill_result = await conn.fetchrow(
                    f'SELECT * FROM "{schema}".p5_effect_results WHERE flow_id=$1::uuid',
                    kill_id,
                )
                if kill_result is None or kill_result["invocations"] < 2:
                    await asyncio.sleep(0.05)
        kill_nodes = await _settled_nodes(conn, schema, kill_id)
        kill_effect = next(row for row in kill_nodes if row["step_key"] == "work.__on_exhaustion")
        assert kill_effect["status"] == "succeeded" and kill_effect["attempt"] == 2
        assert (
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".wf_outbox WHERE flow_id=$1::uuid', kill_id
            )
            == 1
        )
        actual_kills = [
            pod
            for pod in fleet.values()
            if (returncode := pod.poll()) is not None and returncode < 0
        ]
        assert len(actual_kills) == 1, (
            f"no actual effect-holder process was killed: before={killed_before!r}; "
            f"after={[(pod.proc.pid, pod.returncode) for pod in fleet.values()]!r}"
        )
        assert actual_kills[0].poll() == -9
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()
