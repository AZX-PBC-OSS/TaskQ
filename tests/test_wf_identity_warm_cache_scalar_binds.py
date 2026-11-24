"""The widened identity's scalar binds on a pool warmed BEFORE the POST phase.

The prepared-statement cache is the defect's carrier: asyncpg takes a
parameter's type from the server's Parse response and caches the encoder with
the statement. A statement parsed while ``map_index`` was ``smallint`` keeps an
``int2`` encoder, so a high identity is refused during Bind, before the server
sees it::

    DataError: invalid input for query argument $5: 32768 (value out of int16 range)

The documented rollout order applies the POST only after the whole fleet runs
the new release, so every pool is warm against ``smallint`` when the widening
lands. An uncast ``$n`` identity bind therefore defeats the widening on every
door; ``$n::bigint`` serves both the pre-POST and post-POST domains on the same
warm connection.

These pins drive the real API doors — not copied SQL — so the correct fix (the
cast on the production statement) is what makes them pass. The emit and fork
array binds are the immune control: they already carry ``::bigint[]`` and this
module proves the contrast rather than asserting it.
"""

# ruff: noqa: S608  # Why: the f-strings interpolate only throwaway schema identifiers from the fixtures; every value is $-bound.

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.migrate import apply_pending
from taskq.workflows._fork import insert_fork
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import ChildSpec, ForkSpec, NodeSpec, resolve_map_index_domain
from taskq.workflows.api._hitl import register_hold
from taskq.workflows.engine import insert_node, render_workflow_sql
from taskq.workflows.ledger import claim_step_ledger
from tests._wf_fixtures import seed_flow, seed_running_node

pytestmark = pytest.mark.integration

HIGH = 32768
LOW = 0


async def _probe(pg_dsn: str) -> tuple[asyncpg.Connection, str, WorkflowSql]:
    control = await asyncpg.connect(pg_dsn)
    schema = "warm_scalar_" + new_base62(8, precision="random").lower()
    await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(control, schema=schema, phase="pre")
    return control, schema, render_workflow_sql(schema)


@pytest_asyncio.fixture
async def warm_pool(
    pg_dsn: str,
) -> AsyncIterator[tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool]]:
    """A pre-POST schema and a pool whose prepared statements are warmed at LOW.

    The pool deliberately keeps multiple connections' worth of cache lifecycle
    options at the shipped defaults so the warm statement survives the POST.
    """
    control, schema, wsql = await _probe(pg_dsn)
    pool = await asyncpg.create_pool(
        pg_dsn,
        min_size=1,
        max_size=1,
        statement_cache_size=512,
        max_cached_statement_lifetime=3600.0,
    )  # pyright: ignore[reportCallIssue]  # Why: asyncpg accepts the shipped cache options at runtime.
    try:
        yield control, schema, wsql, pool
    finally:
        await pool.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()


async def _seed(control: asyncpg.Connection, schema: str) -> tuple[JobId, JobId]:
    flow = await seed_flow(control, schema)
    node = await seed_running_node(control, schema, flow, step_key="source")
    return flow, node


async def _post(control: asyncpg.Connection, schema: str) -> None:
    await apply_pending(control, schema=schema, phase="post")


async def test_warm_ledger_claim_bind_serves_high_identity(
    warm_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    control, schema, wsql, pool = warm_pool
    flow, node = await _seed(control, schema)
    async with pool.acquire() as conn:
        await claim_step_ledger(
            conn,
            wsql,
            flow_id=flow,
            job_id=node,
            step_key="source",
            map_index=LOW,
            attempt=1,
            claim_epoch=0,
        )
    await _post(control, schema)
    async with pool.acquire() as conn:
        claim = await claim_step_ledger(
            conn,
            wsql,
            flow_id=flow,
            job_id=node,
            step_key="source",
            map_index=HIGH,
            attempt=1,
            claim_epoch=0,
        )
    assert claim.map_index == HIGH
    assert (
        await control.fetchval(
            f'SELECT map_index FROM "{schema}".wf_step_ledger WHERE id = $1', claim.ledger_id
        )
        == HIGH
    )


async def test_warm_outbox_insert_bind_serves_high_identity(
    warm_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    control, schema, wsql, pool = warm_pool
    flow, node = await _seed(control, schema)
    async with pool.acquire() as conn:
        await conn.execute(
            wsql.outbox_insert,
            new_uuid(),
            node,
            flow,
            "consumer",
            LOW,
            '{"actor":"wf","queue":"default","payload":{},"trace_id":"low"}',
        )
    await _post(control, schema)
    outbox_id = new_uuid()
    async with pool.acquire() as conn:
        await conn.execute(
            wsql.outbox_insert,
            outbox_id,
            node,
            flow,
            "consumer",
            HIGH,
            '{"actor":"wf","queue":"default","payload":{},"trace_id":"high"}',
        )
    assert (
        await control.fetchval(
            f'SELECT map_index FROM "{schema}".wf_outbox WHERE id = $1', outbox_id
        )
        == HIGH
    )


async def test_warm_node_insert_bind_serves_high_identity(
    warm_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    control, schema, wsql, pool = warm_pool
    flow, _node = await _seed(control, schema)
    async with pool.acquire() as conn:
        await insert_node(
            conn,
            wsql,
            NodeSpec(flow_id=flow, step_key="n-low", actor="wf", queue="default", map_index=LOW),
        )
    await _post(control, schema)
    async with pool.acquire() as conn:
        # The high identity probes the live domain on this same connection; the
        # bind that follows is the warm statement's own int2 face.
        assert await resolve_map_index_domain(conn, schema) > HIGH
        high_id = await insert_node(
            conn,
            wsql,
            NodeSpec(
                flow_id=flow,
                step_key="n-high",
                actor="wf",
                queue="default",
                map_index=HIGH,
            ),
        )
    assert (
        await control.fetchval(f'SELECT map_index FROM "{schema}".jobs WHERE id = $1', high_id)
        == HIGH
    )


async def test_warm_hold_insert_bind_serves_high_identity(
    warm_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    control, schema, _wsql, pool = warm_pool
    flow, node = await _seed(control, schema)
    payload_schema: dict[str, object] = {"Approval": {"type": "object", "properties": {}}}
    async with pool.acquire() as conn:
        await register_hold(
            conn,
            schema=schema,
            workflow_id=flow,
            node_id=node,
            node_key="review",
            signal_name="Approval",
            hold_epoch=0,
            call_id="call:low",
            payload_schema=payload_schema,
            timeout_s=120.0,
            map_index=LOW,
        )
    await _post(control, schema)
    async with pool.acquire() as conn:
        hold_id = await register_hold(
            conn,
            schema=schema,
            workflow_id=flow,
            node_id=node,
            node_key="review",
            signal_name="Approval",
            hold_epoch=1,
            call_id="call:high",
            payload_schema=payload_schema,
            timeout_s=120.0,
            map_index=HIGH,
        )
    assert (
        await control.fetchval(
            f'SELECT map_index FROM "{schema}".wf_signals WHERE id = $1', hold_id
        )
        == HIGH
    )


async def test_warm_admin_node_panel_bind_serves_high_identity(
    warm_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
    from fastapi import FastAPI, HTTPException

    from taskq.web.admin import create_router, setup_admin_state

    control, schema, _wsql, pool = warm_pool
    await _seed(control, schema)

    async def _allow(_request: Any) -> None:
        return None

    app = FastAPI()
    bundle = create_router(pool, schema=schema, base_path="", auth_dependency=_allow)
    setup_admin_state(app, bundle)
    endpoint = next(
        route.endpoint  # pyright: ignore[reportAttributeAccessIssue]  # Why: the matched route is an APIRoute (which carries ``endpoint``).
        for route in bundle.router.routes
        if getattr(route, "name", "") == "node_panel"
    )
    run_id = new_uuid()

    # Warm the child-addressing statement pre-POST. 404 is the cold row's
    # outcome; the statement itself is parsed and cached by this call.
    with pytest.raises(HTTPException) as cold:
        await endpoint(run_id, "review", pool, schema, LOW)
    assert cold.value.status_code == 404

    await _post(control, schema)
    # Seed the addressed child on the un-warmed control connection: the pool's
    # cache is the subject, not this row's writer.
    await insert_node(
        control,
        _wsql,
        NodeSpec(flow_id=run_id, step_key="review", actor="wf", queue="default", map_index=HIGH),
    )
    panel = await endpoint(run_id, "review", pool, schema, HIGH)
    assert panel["map_index"] == HIGH
    assert panel["key"] == "review"


async def test_warm_fork_array_bind_is_the_immune_control(
    warm_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    """The parallel-array door already carries ``::bigint[]``: warm or not, it
    serves the high identity. This is why the shipped evidence missed S1 — the
    one face it drove was structurally immune."""
    control, schema, wsql, pool = warm_pool
    flow, node = await _seed(control, schema)
    async with pool.acquire() as conn:
        await insert_fork(
            conn,
            wsql,
            flow_id=flow,
            parent_id=node,
            parent_step_key="source",
            fork=ForkSpec(
                children=(ChildSpec(step_key="c", actor="wf", queue="default", map_index=LOW),)
            ),
        )
    await _post(control, schema)
    async with pool.acquire() as conn:
        high_ids, high_join = await insert_fork(
            conn,
            wsql,
            flow_id=flow,
            parent_id=node,
            parent_step_key="source",
            fork=ForkSpec(
                children=(ChildSpec(step_key="c", actor="wf", queue="default", map_index=HIGH),)
            ),
        )
    assert high_join is None and len(high_ids) == 1
    assert (
        await control.fetchval(f'SELECT map_index FROM "{schema}".jobs WHERE id = $1', high_ids[0])
        == HIGH
    )
