"""The wait/timer doors' identity binds on a pool warmed BEFORE the POST.

``tests/test_wf_identity_warm_cache_scalar_binds.py`` pins the five named
statement constants. The wait machinery's eight ``map_index`` binds are inline
``render_sql(...)`` literals inside :class:`~taskq.workflows.api._ctx_wait.
CtxWaitOps` / :class:`~taskq.workflows.api._ctx_timer.CtxTimerOps`; nothing
connects a warm pre-POST pool to a real ``StepContext``, so a slipped
``$3``/``$4`` position or a dropped ``::bigint`` in those literals would pass
every scalar pin.

This module drives the REAL doors (``build_step_context`` →
``ctx.wait_signal`` / ``ctx.wait_until``) across the widening: the same pool
parses the four read statements and the hold mint while ``map_index`` is
``smallint``, the POST widens all five carriers, and the same warm statements
then serve a sibling identity at 32768. No copied SQL: the assertion is on the
row the door writes.

A map source whose children wait on a human or on the clock is the reachable
shape: the documented rollout applies the POST only after the fleet runs the
new release, so every pool is warm against ``smallint`` when the widening
lands, and a map sibling past 32767 then takes this exact path.
"""

# ruff: noqa: S608  # Why: the f-strings interpolate only throwaway schema identifiers; every value is $-bound.

from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.migrate import apply_pending
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.api._ctx import StepContext, build_step_context
from taskq.workflows.api._ctx_wait import NodeHeldError, SignalUnavailableError
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import seed_flow, seed_running_node

pytestmark = pytest.mark.integration

LOW = 0
HIGH = 32768


class Decision(BaseModel):
    """The wait site's declared payload contract (a hold never mints without one)."""

    approved: bool = True


@pytest_asyncio.fixture
async def warm_wait_pool(
    pg_dsn: str,
) -> AsyncIterator[tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool]]:
    """A pre-POST schema and a pool whose statement cache is warmed at LOW."""
    control = await asyncpg.connect(pg_dsn)
    schema = "warm_ctx_" + new_base62(8, precision="random").lower()
    await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(control, schema=schema, phase="pre")
    pool = await asyncpg.create_pool(
        pg_dsn,
        min_size=1,
        max_size=1,
        statement_cache_size=512,
        max_cached_statement_lifetime=3600.0,
    )  # pyright: ignore[reportCallIssue]  # Why: asyncpg accepts the shipped cache options at runtime.
    try:
        yield control, schema, render_workflow_sql(schema), pool
    finally:
        await pool.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()


def _ctx(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    flow_id: JobId,
    node_id: JobId,
    *,
    map_index: int | None,
    workflow_name: str,
) -> StepContext:
    return build_step_context(
        flow_id=flow_id,
        job_id=node_id,
        node_key="source",
        attempt=1,
        # the granted epoch: the wait doors' hold park fences on it, so each
        # wait case seeds its own running row claimed at this epoch.
        claim_epoch=1,
        pool=pool,
        wsql=wsql,
        input=None,
        map_index=map_index,
        workflow_name=workflow_name,
    )


async def test_warm_signal_wait_doors_serve_a_high_sibling(
    warm_wait_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    control, schema, wsql, pool = warm_wait_pool
    flow = await seed_flow(control, schema)
    low_node = await seed_running_node(control, schema, flow, step_key="source")
    high_node = await seed_running_node(control, schema, flow, step_key="source")
    # one running row per sibling: each sibling's mint parks its own row, and
    # the park fences on the claim's epoch (``seed_running_node`` writes 0).
    await control.execute(
        f'UPDATE "{schema}".jobs SET claim_epoch = 1 WHERE id = ANY($1::uuid[])',
        [low_node, high_node],
    )

    # Warm the four read statements and the hold mint at LOW, pre-POST.
    with pytest.raises(NodeHeldError):
        await _ctx(
            pool, wsql, flow, low_node, map_index=LOW, workflow_name="warm-ctx-signal"
        ).wait_signal((Decision,), timeout_s=120.0)

    await apply_pending(control, schema=schema, phase="post")

    # The same warm statements, now widened, at a sibling identity past 32767.
    with pytest.raises(NodeHeldError) as held:
        await _ctx(
            pool, wsql, flow, high_node, map_index=HIGH, workflow_name="warm-ctx-signal"
        ).wait_signal((Decision,), timeout_s=120.0)

    row = await control.fetchrow(
        f'SELECT map_index, status FROM "{schema}".wf_signals WHERE id = $1',
        held.value.hold_id,
    )
    assert row is not None and row["map_index"] == HIGH and row["status"] == "held"
    # The low sibling's own hold survives untouched: the read filters per sibling.
    assert (
        await control.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_signals WHERE map_index = $1', LOW
        )
        == 1
    )


async def test_warm_signal_read_serves_a_high_sibling(
    warm_wait_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    """``ctx.signal()`` — the resume's read of ONE delivered payload.

    Its own statement, its own early return (no row → the typed refusal), and
    its own ``$4`` bind: a wait pin that stops at ``wait_signal`` would let a
    dropped cast (or a shifted position) in this read pass every gate while a
    map sibling's resume read overflows on the warm cache.
    """
    control, schema, wsql, pool = warm_wait_pool
    flow = await seed_flow(control, schema)
    node = await seed_running_node(control, schema, flow, step_key="source")

    # Warm the read statement pre-POST (no delivered row yet: the typed refusal
    # is the cold outcome; the Parse is what the cache keeps).
    with pytest.raises(SignalUnavailableError):
        await _ctx(pool, wsql, flow, node, map_index=LOW, workflow_name="warm-ctx-read").signal(
            "Approval"
        )

    await apply_pending(control, schema=schema, phase="post")

    # The delivered answer lands on the control connection: the pool's warm
    # statement is the subject, not this row's writer.
    await control.execute(
        f'INSERT INTO "{schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, map_index, call_id, "
        "payload, status) "
        "VALUES ($1, $2, 'source', 'Approval', 0, $3, 'call:1:0', $4::jsonb, 'delivered')",
        new_uuid(),
        flow,
        HIGH,
        '{"approved": true}',
    )

    payload = await _ctx(
        pool, wsql, flow, node, map_index=HIGH, workflow_name="warm-ctx-read"
    ).signal("Approval")
    assert payload == {"approved": True}

    # The sibling filter still holds on the same warm statement: the low
    # context may not read the high sibling's answer.
    with pytest.raises(SignalUnavailableError):
        await _ctx(pool, wsql, flow, node, map_index=LOW, workflow_name="warm-ctx-read").signal(
            "Approval"
        )


async def test_warm_timer_wait_doors_serve_a_high_sibling(
    warm_wait_pool: tuple[asyncpg.Connection, str, WorkflowSql, asyncpg.Pool],
) -> None:
    control, schema, wsql, pool = warm_wait_pool
    flow = await seed_flow(control, schema)
    low_node = await seed_running_node(control, schema, flow, step_key="source")
    high_node = await seed_running_node(control, schema, flow, step_key="source")
    await control.execute(
        f'UPDATE "{schema}".jobs SET claim_epoch = 1 WHERE id = ANY($1::uuid[])',
        [low_node, high_node],
    )

    with pytest.raises(NodeHeldError):
        await _ctx(
            pool, wsql, flow, low_node, map_index=LOW, workflow_name="warm-ctx-timer"
        ).wait_until(120.0, key="remind")

    await apply_pending(control, schema=schema, phase="post")

    with pytest.raises(NodeHeldError) as held:
        await _ctx(
            pool, wsql, flow, high_node, map_index=HIGH, workflow_name="warm-ctx-timer"
        ).wait_until(120.0, key="remind")

    row = await control.fetchrow(
        f'SELECT map_index, status, signal_name FROM "{schema}".wf_signals WHERE id = $1',
        held.value.hold_id,
    )
    assert row is not None and row["map_index"] == HIGH and row["status"] == "held"
    assert row["signal_name"] == "__timer__remind"
