"""The T07 fan-in bound pins (the fast tier): the DECLARED MAXIMUM FAN-IN
PER JOIN refused at validate with the child-driven alternative named, the
escape recorded on the joined node, and the exactly-once pins on the
child-driven shape (50 duplicate finalizes → 1 decrement; the PK rejects
the double-fire — the shipped drills re-run on the child-driven join).

THE BOUND'S HONEST DERIVATION (F7, restated — the docs carry the same
text; the pin asserts the derivation's PROCEDURE, never a false line):

P1's three measured points — 200 → 14.9 ms, 1000 → 23.8 ms, 5000 →
39.3 ms — are SUB-LINEAR-IN-LOG but NOT one line: an endpoint fit
(~5.1 µs/edge + ~14 ms base) predicts 19.1 ms at 1000 against the
measured 23.8 — the 1000 point is the OUTLIER, named as such
(least-squares gives ≈4.7 µs + 16.3 ms and still misses the endpoints).
The governing budget, stated honestly: at the ~14-16 ms BASE term no
fan-in meets a 5 ms-class budget; the declared-edge cost is dominated by
the BASE (paid once per sweep pass regardless of fan-in), not the edges —
the marginal edge cost is ~5 µs. The bound's candidate ≤ 1000 declared
parents per join therefore stands on the BASE-COST argument (what the
bound bounds is the PER-JOIN marginal work inside one pass), NOT on the
5 ms-class comparison — that comparison was false and is struck.

THE REFIT PROCEDURE (what the scale pin does on the landed
implementation): re-fit the curve on the shipped code (the endpoint fit
over the boundary points), re-derive the marginal edge cost from the fit,
and re-state the base-dominated argument against the refit numbers. The
bound moves only by that argument in review — never by editing the
constant to make a test pass.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq.backend._protocol import JobId
from taskq.workflows import finalize_node
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import ChildSpec, ForkSpec, JoinSpec
from taskq.workflows.definitions import (
    MAX_FAN_IN_PER_JOIN,
    validate_fork,
    validate_join_spec,
)


def new_id() -> object:
    """A seam-shaped id for the validator's door (the validators check
    COUNTS, not id content; the pins pass opaque handles)."""
    from taskq._ids import new_uuid

    return new_uuid()


# ── Red-first 1: the UNBOUNDED FAN-IN is refused at validate ────────────


def test_t07_pin_bound_refuses_above_the_maximum_naming_the_alternative() -> None:
    """A join declaring fan-in above the bound on the declared-edge shape
    is REJECTED at validate — the error names the bound AND the
    child-driven alternative. Red until the bound check exists: the
    unbounded variant accepted the declaration silently and the re-derive
    paid the edge-count cost with no gate."""
    n = MAX_FAN_IN_PER_JOIN + 1
    with pytest.raises(ValueError, match="child-driven") as exc_info:
        validate_join_spec("big_join", tuple(JobId(new_id()) for _ in range(n)), n)
    assert str(MAX_FAN_IN_PER_JOIN) in str(exc_info.value), "the error must name the bound"


def test_t07_pin_bound_refuses_the_forks_join_above_the_maximum() -> None:
    """The fork's door: a join over more children than the bound is
    refused unless the child-driven shape is declared explicitly."""
    children = tuple(
        ChildSpec(step_key=f"c{i}", actor="a", queue="q") for i in range(MAX_FAN_IN_PER_JOIN + 1)
    )
    fork = ForkSpec(
        children=children,
        join=JoinSpec(step_key="j", actor="a", queue="q"),
    )
    with pytest.raises(ValueError, match="child_driven=True"):
        validate_fork(fork)

    # THE ESCAPE: the explicit opt-in validates (the choice is recorded on
    # the joined node's metadata — the fork pin drives the row).
    escaped = ForkSpec(
        children=children,
        join=JoinSpec(step_key="j", actor="a", queue="q", child_driven=True),
    )
    validate_fork(escaped)


def test_t07_pin_bound_at_the_maximum_is_accepted() -> None:
    """The bound is a ceiling, not a haircut: exactly the bound is legal
    (over-rejection is the worse asymmetry — the doctrine)."""
    parents = tuple(JobId(new_id()) for _ in range(MAX_FAN_IN_PER_JOIN))
    validate_join_spec("edge_join", parents, len(parents))


# ── Red-first 2: the CHILD-DRIVEN shape fires exactly once ──────────────


@pytest.mark.integration
async def test_t07_pin_child_driven_join_fires_exactly_once(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """Past the bound (the child-driven escape): the join must STILL fire
    exactly once — the shipped duplicate-finalize drill re-run on the
    child-driven join: 50 duplicate finalizes → 1 decrement; the
    wf_join_fire PK rejects the double-fire. The convicted variant (a
    child-driven join whose count double-fires) reds on the PK."""
    from taskq.workflows import insert_node
    from taskq.workflows._types import NodeSpec
    from tests._wf_fixtures import claim_view, fire_count, node_state, seed_flow, seed_running_node

    flow_id = await seed_flow(wf_conn, wf_schema)
    n = 3
    children = [
        await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"c{i}") for i in range(n)
    ]
    # The child-driven escape, driven through the ENGINE (the recorded
    # choice rides the join row's metadata.join_shape).
    join_id = await insert_node(
        wf_conn,
        wf_sql,
        NodeSpec(
            flow_id=flow_id,
            step_key="child_driven_join",
            actor="wf",
            queue="default",
            parents=tuple(children),
            deps_pending=n,
            child_driven=True,
        ),
    )
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["metadata"].get("join_shape") == "child_driven", (
        "the shape choice must be recorded on the joined node"
    )

    # The children terminalize — the last decrement fires the join.
    for i, child in enumerate(children):
        result = await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=child,
            step_key=f"c{i}",
            worker_id=(await claim_view(wf_conn, wf_schema, child))[0],
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
            result={"i": i},
        )
        assert result.applied

    assert await fire_count(wf_conn, wf_schema, join_id) == 1

    # THE DUPLICATE-FINALIZE DRILL: 50 more terminal writes on an already-
    # terminal child — every one fenced (the rowcount gate); no second
    # decrement, no second fire.
    for _ in range(50):
        dup = await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=children[0],
            step_key="c0",
            worker_id=(await claim_view(wf_conn, wf_schema, children[0]))[0],
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
        )
        assert not dup.applied, "the duplicate finalize must fence out"
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, "the PK rejects the double-fire"
