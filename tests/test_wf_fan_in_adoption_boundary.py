"""The adoption guard's own boundary: SMALLINT_MAX is allowed, +1 refuses.

``_fork.insert_fork``'s adoption branch reads the pre-created join's
``deps_pending`` and refuses typed when ``existing + len(children)`` passes the
counter's smallint domain. H1 was an off-by-one; the guard that fixes it can
introduce the mirror-image one — refusing a fork that still fits, which
regresses a legal adoption that succeeded before the guard existed.

The two cases drive the real adoption path on one seeded join counter, one
child apart, so a ``>=`` (over-refusal) or a ``> SMALLINT_MAX + 1`` (a refusal
that never fires) reds here. The counter starts near the bound rather than
building 32767 static parents: the row's value is the guard's input, and the
declarative fan-in bound (MAX_FAN_IN_PER_JOIN) forbids declaring that many
parents — an adopted child-driven join is how the counter legitimately grows.
"""

# ruff: noqa: S608  # Why: the f-string interpolates only a throwaway schema identifier; every value is $-bound.

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.constants import SMALLINT_MAX
from taskq.migrate import apply_pending
from taskq.workflows._fork import insert_fork
from taskq.workflows._types import ChildSpec, ForkSpec, JoinSpec
from taskq.workflows.definitions import FanInBoundExceededError
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import seed_flow, seed_join

pytestmark = [pytest.mark.integration, pytest.mark.slow]

EXISTING = SMALLINT_MAX - 6  # 32761


def _fork(width: int) -> ForkSpec:
    return ForkSpec(
        children=tuple(
            ChildSpec(step_key="c", actor="wf", queue="default", map_index=index)
            for index in range(width)
        ),
        join=JoinSpec(step_key="j", actor="wf", queue="default", child_driven=True),
    )


async def test_adoption_at_the_counter_bound_is_allowed(pg_dsn: str) -> None:
    schema = "adopt_edge_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema, phase="pre")
        wsql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema)
        join_id = await seed_join(conn, schema, flow, step_key="j", deps=EXISTING)
        width = SMALLINT_MAX - EXISTING  # existing + width == SMALLINT_MAX exactly

        async with conn.transaction():
            children, adopted = await insert_fork(
                conn,
                wsql,
                flow_id=flow,
                parent_id=flow,
                parent_step_key="src",
                fork=_fork(width),
            )

        assert adopted == join_id and len(children) == width
        assert (
            await conn.fetchval(f'SELECT deps_pending FROM "{schema}".jobs WHERE id = $1', join_id)
            == SMALLINT_MAX
        ), "the adoption that lands exactly on the counter's bound must still fit"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_adoption_one_past_the_counter_bound_refuses_typed(pg_dsn: str) -> None:
    schema = "adopt_edge2_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema, phase="pre")
        wsql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema)
        join_id = await seed_join(conn, schema, flow, step_key="j", deps=EXISTING)
        width = SMALLINT_MAX - EXISTING + 1  # existing + width == SMALLINT_MAX + 1

        with pytest.raises(FanInBoundExceededError):
            async with conn.transaction():
                await insert_fork(
                    conn,
                    wsql,
                    flow_id=flow,
                    parent_id=flow,
                    parent_step_key="src",
                    fork=_fork(width),
                )

        assert (
            await conn.fetchval(f'SELECT deps_pending FROM "{schema}".jobs WHERE id = $1', join_id)
            == EXISTING
        ), "the refused adoption changed the pre-created join's counter"
        assert (
            await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE step_key = $1', "c")
            == 0
        ), "the refused adoption wrote child rows"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
