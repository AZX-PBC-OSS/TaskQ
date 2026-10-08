# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""THE CONTEXT CONTRACT PIN (the maintainer's addendum — the runtime
info on the context, surface-driven): a BODY asserting on ctx sees ALL
of it — the attempt ordinal, the node key, the map index, the run id,
the flow name, the queue, the claim timestamp, the remaining loop
budget, the consumed hold's epoch. The assertions run INSIDE the body
on a REAL run (no mocks — a mock of the context would pin the mock)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pytest
from pydantic import BaseModel

from taskq.workflows import FlowRunner, WorkflowApp, build, loop, map_source, step


class Ingest(BaseModel):
    doc_id: str


SEEN: dict[str, Any] = {}


async def test_the_context_contract_every_field_a_body_asserts_on(
    wf_pool: Any, wf_schema: str, wf_conn: Any
) -> None:
    app = WorkflowApp()

    async def asserting_body(ctx: Any, params: Ingest) -> list[str]:
        # THE BODY IS THE PIN: every field the contract names, asserted
        # on the LIVE surface. The failures are RECORDED (the body's
        # exception feeds the ladder; the record is what the test
        # asserts on — the body's own verdict is the pin).
        fields = {
            "flow_id": str(ctx.flow_id),
            "node_key": ctx.node_key,
            "attempt": ctx.attempt,
            "input": ctx.input,
            "flow_name": ctx.flow_name,
            "queue": ctx.queue,
            "claimed_at": str(ctx.claimed_at),
        }
        SEEN["fields"] = fields
        assert ctx.node_key == "asserting", fields
        assert ctx.attempt >= 1, fields
        assert ctx.input == {"doc_id": "d1"}, fields  # the FLOW input (create_flow's)
        assert ctx.flow_name == "context_contract", fields
        assert ctx.queue == "contract-q", fields
        assert isinstance(ctx.claimed_at, datetime), fields
        SEEN["step"] = True
        return ["a", "b", "c"]  # the map's source returns the LIST (the fork's items)

    async def map_item(ctx: Any, doc_id: str) -> str:
        assert ctx.map_index is not None, "the map item's index"
        # THE CHILD'S OWN KEY: the fork's children run under
        # ``<source>.item`` (the per-item ledger identity), not the
        # wiring node's key.
        if ctx.node_key != "asserting.item":
            SEEN.setdefault("child_keys", []).append(ctx.node_key)
            SEEN["child_failed"] = ctx.node_key
        else:
            SEEN.setdefault("map_indexes", []).append(ctx.map_index)
        return doc_id
    # (the asserting body returns the LIST the map fans over)

    app_obj = app

    @app_obj.workflow("context_contract")
    def context_contract() -> object:
        first = step(asserting_body, Ingest(doc_id="d1"), key="asserting", queue="contract-q")
        async def tail(ctx: Any, items: list[str]) -> int:
            return len(items)

        mapped = map_source(first, map_item, key="enrich")
        return build(step(tail, mapped, key="tail"))

    runner = FlowRunner(app_obj.get("context_contract"), wf_pool, wf_schema)
    flow_id = await runner.create_flow(input=Ingest(doc_id="d1"))
    await runner.drive(flow_id)
    root = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    if root != "succeeded":
        errors = await wf_conn.fetch(
            f'SELECT step_key, error_class, error_message FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND error_message IS NOT NULL",
            flow_id,
        )
        pytest.fail(
            f"the run failed: {root} — "
            + json.dumps([dict(r) for r in errors], default=str)
        )
    assert SEEN.get("step") is True, json.dumps(SEEN, default=str)
    assert sorted(SEEN.get("map_indexes", [])) == [0, 1, 2], json.dumps(SEEN, default=str)
    assert SEEN.get("step") is True, json.dumps(SEEN, default=str)
    assert sorted(SEEN.get("map_indexes", [])) == [0, 1, 2], json.dumps(SEEN, default=str)


async def test_the_loop_ctx_carries_the_budget_wall(
    wf_pool: Any, wf_schema: str
) -> None:
    """The loop ctx's budget_remaining_ms: the wall's on-wake read (a
    number, not None — the wall EXISTS when budget_s is set)."""
    from taskq.workflows import Done

    seen: dict[str, Any] = {}

    app = WorkflowApp()

    async def loop_body(ctx: Any, carry: int) -> Done[str]:
        seen["budget_ms"] = ctx.budget_remaining_ms
        seen["flow_name"] = ctx.flow_name
        return Done("finished")

    @app.workflow("ctx_budget")
    def ctx_budget() -> object:
        return build(loop("the_loop", loop_body, carry=0, budget_s=3600.0))

    runner = FlowRunner(app.get("ctx_budget"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()  # no input — ctx.input is None (the honest zero)
    assert await runner.drive(flow_id) == "terminal"
    assert seen["flow_name"] == "ctx_budget"
    assert seen["budget_ms"] is not None
    assert 0 < seen["budget_ms"] <= 3600 * 1000
