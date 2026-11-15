"""#717 P3: identity width is not the join child-counter domain."""
# ruff: noqa: S608  # Why: only this test's generated schema identifier is interpolated; all values are bound.

from __future__ import annotations

import asyncpg
import pytest

from taskq.constants import BIGINT_MAX, SMALLINT_MAX
from taskq.workflows._emit import MapIndexExhaustedError
from taskq.workflows._fork import ForkCounterExhaustedError, insert_fork
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import (
    ChildSpec,
    ForkSpec,
    JoinSpec,
    MapIndexSchemaError,
    NodeSpec,
)
from taskq.workflows.api._runner_ladder import is_deterministic_authoring_failure
from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN, FanInBoundExceededError
from taskq.workflows.engine import insert_node
from tests._wf_fixtures import seed_flow, seed_running_node

pytestmark = pytest.mark.integration


def _children(count: int) -> tuple[ChildSpec, ...]:
    return tuple(
        ChildSpec(step_key="item", actor="wf", queue="default", map_index=index)
        for index in range(count)
    )


def _child_driven(count: int) -> ForkSpec:
    return ForkSpec(
        children=_children(count),
        join=JoinSpec(step_key="join", actor="wf", queue="default", child_driven=True),
    )


async def test_32768_child_driven_fork_refuses_before_any_rows_on_bigint_schema(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    assert SMALLINT_MAX == 32767
    kinds = await wf_conn.fetch(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = 'jobs' AND column_name = 'map_index'",
        wf_schema,
    )
    assert [row["data_type"] for row in kinds] == ["bigint"]
    flow = await seed_flow(wf_conn, wf_schema)
    parent = await seed_running_node(wf_conn, wf_schema, flow, step_key="parent")
    before = int(
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs',
        )
        or 0
    )
    with pytest.raises(ForkCounterExhaustedError, match="deps_pending") as raised:
        await insert_fork(
            wf_conn,
            wf_sql,
            flow_id=flow,
            parent_id=parent,
            parent_step_key="parent",
            fork=_child_driven(SMALLINT_MAX + 1),
        )
    assert type(raised.value) is ForkCounterExhaustedError
    assert str(32768) in str(raised.value)
    assert str(SMALLINT_MAX) in str(raised.value)
    assert is_deterministic_authoring_failure(raised.value)
    after = int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs') or 0)
    assert after == before
    assert int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_edge') or 0) == 0
    ledger_type = await wf_conn.fetchval(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = 'jobs' AND column_name = 'deps_pending'",
        wf_schema,
    )
    assert ledger_type == "smallint"


async def test_1001_ordinary_join_keeps_its_original_1000_contract(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    parent = await seed_running_node(wf_conn, wf_schema, flow, step_key="parent")
    before = int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs') or 0)
    fork = ForkSpec(
        children=_children(MAX_FAN_IN_PER_JOIN + 1),
        join=JoinSpec(step_key="join", actor="wf", queue="default"),
    )
    with pytest.raises(FanInBoundExceededError, match="1000") as raised:
        await insert_fork(
            wf_conn,
            wf_sql,
            flow_id=flow,
            parent_id=parent,
            parent_step_key="parent",
            fork=fork,
        )
    assert type(raised.value) is FanInBoundExceededError
    assert int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs') or 0) == before
    assert int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_edge') or 0) == 0


@pytest.mark.timeout(300)
async def test_32767_is_the_largest_legal_join_counter(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """Non-vacuous positive boundary: the real arrays/edges/counter all land."""
    flow = await seed_flow(wf_conn, wf_schema)
    parent = await seed_running_node(wf_conn, wf_schema, flow, step_key="parent")
    ids, join = await insert_fork(
        wf_conn,
        wf_sql,
        flow_id=flow,
        parent_id=parent,
        parent_step_key="parent",
        fork=_child_driven(SMALLINT_MAX),
    )
    assert len(ids) == SMALLINT_MAX
    assert join is not None
    row = await wf_conn.fetchrow(
        f'SELECT deps_pending, status FROM "{wf_schema}".jobs WHERE id = $1', join
    )
    assert row is not None
    assert row["deps_pending"] == SMALLINT_MAX
    assert row["status"] == "pending"
    assert int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_edge') or 0) == (
        2 * SMALLINT_MAX
    )


async def test_bigint_max_works_at_node_and_fork_doors_and_bad_types_stay_exact(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    node = await insert_node(
        wf_conn,
        wf_sql,
        NodeSpec(
            flow_id=flow,
            step_key="high-node",
            actor="wf",
            queue="default",
            map_index=BIGINT_MAX,
        ),
    )
    parent = await seed_running_node(wf_conn, wf_schema, flow, step_key="parent")
    ids, join = await insert_fork(
        wf_conn,
        wf_sql,
        flow_id=flow,
        parent_id=parent,
        parent_step_key="parent",
        fork=ForkSpec(
            children=(
                ChildSpec(
                    step_key="high-child",
                    actor="wf",
                    queue="default",
                    map_index=BIGINT_MAX,
                ),
            )
        ),
    )
    assert join is None
    rows = await wf_conn.fetch(
        f'SELECT id, map_index, idempotency_key FROM "{wf_schema}".jobs '
        "WHERE id = ANY($1::uuid[]) ORDER BY step_key",
        [node, ids[0]],
    )
    assert [row["map_index"] for row in rows] == [BIGINT_MAX, BIGINT_MAX]
    fork_row = next(row for row in rows if row["id"] == ids[0])
    assert fork_row["idempotency_key"] == f"wf:{flow}:parent:high-child:{BIGINT_MAX}"

    corrupt = NodeSpec(flow_id=flow, step_key="bad", actor="wf", queue="default")
    object.__setattr__(corrupt, "map_index", True)
    with pytest.raises(ValueError, match="exact int"):
        await insert_node(wf_conn, wf_sql, corrupt)
    before = int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs') or 0)
    with pytest.raises(ForkCounterExhaustedError, match="deps_pending"):
        await insert_node(
            wf_conn,
            wf_sql,
            NodeSpec(
                flow_id=flow,
                step_key="oversized-counter",
                actor="wf",
                queue="default",
                deps_pending=SMALLINT_MAX + 1,
            ),
        )
    assert int(await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs') or 0) == before
    assert is_deterministic_authoring_failure(MapIndexSchemaError("broken deployment"))
    assert is_deterministic_authoring_failure(MapIndexExhaustedError("bad identity"))
