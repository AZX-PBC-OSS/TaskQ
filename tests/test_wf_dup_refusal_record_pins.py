# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""The duplicate-refusal's RECORD pins (area: duplicate-refusal-routing).

``DuplicateSubstepError`` exists so an operator can read the failure off the
rows. The claim is stated three times in the change:

- the class docstring: "uncaught, the node fails on its first attempt with
  this class on the record (a loop body exhausts the loop as a body failure,
  the class named in its message)" — ``api/_runner_errors.py``;
- ``docs/guides/workflows.md``: "in a loop body the loop exhausts as a body
  failure whose message names it";
- ``docs/guides/upgrading.md``: "in a loop body the loop exhausts as a body
  failure whose message names it".

A refusal that travels inside an ``ExceptionGroup`` (``asyncio.TaskGroup``,
the shape the ladder's own deterministic docstring names as expected) is
classified by its LEAF — ``_deterministic_error_class`` walks the group — but
both terminal paths build the persisted MESSAGE from the RAISED object, not
the leaf. These two pins hold the record to the documented promise: the class
is right on the node path (already pinned), and the message must still let an
operator see which name collided.
"""

from __future__ import annotations

import asyncio

import asyncpg

from taskq.backend._protocol import JobId
from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    loop,
    step,
)


async def _node_row(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, step_key: str
) -> asyncpg.Record:
    row = await conn.fetchrow(
        f'SELECT status, attempt, error_class, error_message FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        step_key,
    )
    assert row is not None, f"no node row for {step_key!r}"
    return row


def _duplicate_pair(in_fn: asyncio.Event) -> tuple[object, object]:
    """The two same-name substep fns: the first parks inside its fn (so its
    scoped key is registered and its claim is made), the second is trivial."""

    async def slow() -> str:
        in_fn.set()
        try:
            await asyncio.sleep(30)
        finally:
            pass
        return "slow"

    async def fast() -> str:
        return "fast"

    return slow, fast


async def test_a_taskgroup_wrapped_refusal_keeps_its_name_on_the_node_record(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A body that fans out inside ``asyncio.TaskGroup`` re-raises the
    refusal as ``ExceptionGroup[DuplicateSubstepError]``. The terminal's
    CLASS walks to the leaf; the terminal's MESSAGE does not, so the record
    loses ``enrich`` — the one fact the operator needs (which name collided)
    — and reads as the group's own wrapper text instead."""
    in_fn = asyncio.Event()
    slow, fast = _duplicate_pair(in_fn)

    async def body(ctx: StepContext) -> list[str]:
        async with asyncio.TaskGroup() as tg:

            async def second() -> str:
                await asyncio.wait_for(in_fn.wait(), timeout=10)
                return await ctx.substep("enrich", fast)

            tg.create_task(ctx.substep("enrich", slow))
            tg.create_task(second())
        return []

    app = WorkflowApp()

    @app.workflow("substep_scope_taskgroup_node_record")
    def wf() -> Promise[object]:
        return build(step(body, key="dup", max_attempts=3))

    runner = FlowRunner(app.get("substep_scope_taskgroup_node_record"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    node = await _node_row(wf_conn, wf_schema, flow_id, "dup")
    # The good part, and it holds: the leaf's class wins the record and the
    # ladder is not spent (the group is deterministic through its leaf).
    assert (node["status"], node["attempt"], node["error_class"]) == (
        "failed",
        1,
        "DuplicateSubstepError",
    ), dict(node)
    # The record's own truth: an operator reading `jobs.error_message` must
    # learn which name collided, exactly as the docstring promises.
    assert "enrich" in (node["error_message"] or ""), dict(node)


async def test_a_taskgroup_wrapped_refusal_still_names_the_class_in_the_loop_record(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The loop path's promise is stronger and its machinery weaker: the loop
    driver does not consult the deterministic classifier at all, so the class
    survives only as ``f"the loop body failed: {type(exc).__name__}: ..."``.
    For a grouped refusal ``type(exc).__name__`` is ``ExceptionGroup``, so the
    exhaust records neither ``DuplicateSubstepError`` nor the name — and the
    class docstring's "the class named in its message" is false on this
    shape."""
    in_fn = asyncio.Event()
    slow, fast = _duplicate_pair(in_fn)
    fns: list[str] = []

    async def loop_body(ctx: StepContext, carry: object) -> object:
        fns.append("iteration")

        async def second() -> str:
            await asyncio.wait_for(in_fn.wait(), timeout=10)
            return await ctx.substep("enrich", fast)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(ctx.substep("enrich", slow))
            tg.create_task(second())
        return Done(None)

    app = WorkflowApp()

    @app.workflow("substep_scope_taskgroup_loop_record")
    def wf() -> Promise[object]:
        return build(loop("dupl", loop_body, max_iterations=3))

    runner = FlowRunner(app.get("substep_scope_taskgroup_loop_record"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    node = await _node_row(wf_conn, wf_schema, flow_id, "dupl")
    assert (node["status"], node["attempt"], node["error_class"]) == (
        "failed",
        1,
        "LoopBodyFailure",
    ), dict(node)
    # The documented promise: "the class named in its message".
    assert "DuplicateSubstepError" in (node["error_message"] or ""), dict(node)
