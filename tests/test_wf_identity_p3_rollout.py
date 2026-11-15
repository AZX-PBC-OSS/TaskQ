"""#717 staged roots, terminal replay and explicit post-POST retry."""
# ruff: noqa: S608  # Why: only the generated test schema identifier is interpolated; every value is bound.

from __future__ import annotations

import enum
import json

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending
from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
    retry_workflow_node,
)
from taskq.workflows.chain import DONE, Route

pytestmark = pytest.mark.integration


async def test_preexisting_failed_and_live_roots_remain_distinct_across_post(
    pg_dsn: str,
) -> None:
    schema = "idp3roots_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await apply_pending(conn, schema=schema, phase="pre")
        # A rollout starts from the previous completed POST close-outs too;
        # only 01.00.40 identity widening remains pending.
        await apply_pending(conn, schema=schema, phase="post", target="01.00.37_01")
        pool = await asyncpg.create_pool(pg_dsn)

        class Outcome(enum.Enum):
            OK = "ok"

        async def record(ctx: StepContext, item: object) -> Outcome:
            assert type(ctx.map_index) is int
            assert isinstance(item, dict)
            return Outcome.OK

        chain = Chain(
            name="identity-rollout",
            start="record",
            steps={"record": Step(body=record, outcomes=Outcome, route=Route({Outcome.OK: DONE}))},
        )

        async def source(ctx: StepContext) -> None:
            assert ctx.attempt == 1
            cursor = await ctx.cursor()
            previous = cursor.get("page", -1)
            assert type(previous) is int
            for page, indexes in enumerate(((0,), (32768,))):
                if page <= previous:
                    continue
                await ctx.emit_batch(
                    [
                        chain_start(
                            chain,
                            {"record_id": index},
                            map_index=index,
                            trace_id=f"record-{index}",
                        )
                        for index in indexes
                    ],
                    cursor={"page": page},
                )

        app = WorkflowApp()

        @app.workflow("identity_rollout")
        def workflow() -> Promise[object]:
            return build(chain_source(chain, source, key="source"))

        runner = FlowRunner(app.get("identity_rollout"), pool, schema)
        a = (await runner.create_flow(run_key="rollout-A")).flow_id
        b = (await runner.create_flow(run_key="rollout-B")).flow_id
        assert a != b
        assert await runner.drive(a, max_ticks=100) == "terminal"
        a_root = await conn.fetchrow(
            f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', a
        )
        assert a_root is not None and a_root["status"] == "failed"
        a_events_before = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events '
            "WHERE job_id = $1 AND kind = 'state_change' AND detail->>'to_state' = 'failed'",
            a,
        )
        assert a_events_before == 1
        assert (
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND map_index = 32768",
                a,
            )
            == 0
        )
        b_before = await conn.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', b)
        assert b_before == "running"

        await apply_pending(conn, schema=schema, phase="post")
        replay = await runner.create_flow(run_key="rollout-A")
        assert replay.kind == "existing-terminal"
        assert replay.flow_id == a
        assert (
            await conn.fetchval(f'SELECT finished_at FROM "{schema}".jobs WHERE id = $1', a)
            == (a_root["finished_at"])
        )

        assert await runner.drive(b, max_ticks=100) == "terminal"
        assert await conn.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', b) == (
            "succeeded"
        )

        # A terminal run never self-revives. The explicit operator retry reopens
        # only the failed source at its not-committed high page; low history and
        # its terminal event are not rewritten or duplicated.
        reopened = await retry_workflow_node(
            pool, schema=schema, flow_id=a, node_key="source", reason="identity POST completed"
        )
        assert reopened == 1
        assert await runner.drive(a, max_ticks=100) == "terminal"
        high = await conn.fetch(
            f"SELECT id, status, idempotency_key, "
            "(metadata->>'flow_id')::uuid AS flow FROM "
            f'"{schema}".jobs WHERE map_index = 32768 ORDER BY idempotency_key'
        )
        assert len(high) == 2
        assert {row["flow"] for row in high} == {a, b}
        assert {row["status"] for row in high} == {"succeeded"}
        assert {row["idempotency_key"] for row in high} == {
            f"wf:{a}:emit:32768:record",
            f"wf:{b}:emit:32768:record",
        }
        claims = await conn.fetch(
            f'SELECT flow_id, job_id, status, attempt FROM "{schema}".wf_step_ledger '
            "WHERE map_index = 32768 AND step_key = 'record'"
        )
        assert len(claims) == 2
        assert {row["flow_id"] for row in claims} == {a, b}
        assert {row["job_id"] for row in claims} == {row["id"] for row in high}
        assert all(row["status"] == "succeeded" for row in claims)
        source_a = await conn.fetchrow(
            f'SELECT metadata, status FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
            a,
        )
        assert source_a is not None and source_a["status"] == "succeeded"
        metadata = source_a["metadata"]
        document = json.loads(metadata) if isinstance(metadata, str) else metadata
        assert document["emit_cursor"] == {"page": 1}
        assert (
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_events '
                "WHERE job_id = $1 AND kind = 'state_change' AND detail->>'to_state' = 'failed'",
                a,
            )
            == a_events_before
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
