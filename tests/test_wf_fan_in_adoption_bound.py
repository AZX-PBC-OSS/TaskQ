"""The pre-created join's adoption path must account for the deps it already has.

``jobs.deps_pending`` is a ``smallint`` COUNT of unterminal parents, deliberately
separate from the record identity's widened domain. ``validate_fork`` permits a
child-driven fork of ``SMALLINT_MAX`` children, but the API pre-creates the join
with ``deps_pending = 1`` (its source edge) and the fork ADOPTS that row by
adding the children's weight: ``1 + 32767 = 32768`` overflows ``smallint`` at
the server with a raw ``NumericValueOutOfRangeError`` (22003), after the
children are already inserted. That error is in neither ``is_infra_fault`` nor
the deterministic classes, so the finalize path can reclaim-loop instead of
terminalizing — the refusal must be the typed ``FanInBoundExceededError``,
raised before the counter UPDATE runs.

Nothing persists either way: the guard reads the join row the adoption already
looked up and refuses inside the CALLER's transaction (production: the
finalize's tx1), so the children the fork inserted before that lookup unwind
with it. The assertions below therefore pin the observable outcome — a typed
refusal, an unchanged counter, no child row left behind — not the statement
order inside the fork.
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


def _legal_max_child_driven_fork() -> ForkSpec:
    children = tuple(
        ChildSpec(step_key="c", actor="wf", queue="default", map_index=index)
        for index in range(SMALLINT_MAX)
    )
    return ForkSpec(
        children=children,
        join=JoinSpec(
            step_key="j",
            actor="wf",
            queue="default",
            # The documented escape past MAX_FAN_IN_PER_JOIN; the counter's own
            # smallint bound is the separate guard this case meets.
            child_driven=True,
        ),
    )


async def test_adoption_path_refuses_typed_before_writing_at_the_counter_bound(
    pg_dsn: str,
) -> None:
    schema = "adopt_bound_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema, phase="pre")
        wsql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema)
        # The API's pre-created join: born with its source edge's weight.
        join_id = await seed_join(conn, schema, flow, step_key="j", deps=1)

        with pytest.raises(FanInBoundExceededError):
            async with conn.transaction():
                await insert_fork(
                    conn,
                    wsql,
                    flow_id=flow,
                    parent_id=flow,
                    parent_step_key="src",
                    fork=_legal_max_child_driven_fork(),
                )

        assert (
            await conn.fetchval(f'SELECT deps_pending FROM "{schema}".jobs WHERE id = $1', join_id)
            == 1
        ), "the refused adoption changed the pre-created join's counter"
        assert (
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE parent_id = $1 AND step_key = $2',
                flow,
                "c",
            )
            == 0
        ), "the refused adoption wrote child rows"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
