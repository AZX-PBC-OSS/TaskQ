# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""The #745 section of ``docs/guides/upgrading.md`` as an executable contract.

The section "A map on another fan-out's join: the nested join has one row"
is the change's only instrument for finding — and its only instruction for
repairing — pre-fix persisted state: three read-only queries plus one
guarded removal. Nothing else in the suite executes those statements, so a
key-shape change (the drain's bare ``wf:<step>`` vs the fork's
``wf:<flow>:<parent>:<join>``), a status leg added to the adoption, or a
relaxed dotted-key refusal would silently turn a detector into a false
all-clear, or the removal into a row-eater.

So the statements are extracted from the document (not retyped) and driven
against a state the real runner can leave behind: an in-flight run whose
outer fork ran before #745 (the old binding stamped onto the outer join, the
same forge the keyed-admission pin uses) with the leader's outbox drain
landing after the run terminalised. Pinned:

* detector 1 selects exactly the surplus row, and detectors 2 and 3 do not
  also claim it;
* the documented removal deletes exactly that one row and the run's rows
  then derive ``complete`` under its ``succeeded`` root — the estate's
  status-truth invariant (G7), which the alternative ``taskq job cancel``
  cannot restore (a ``cancelled`` row forever defeats rule 4 of
  ``derive_workflow_status``, and the maintenance leg never re-derives a
  terminal root);
* no detector and no removal touches a healthy post-fix run.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

import taskq.workflows._sweep as sweep_module
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)
from taskq.workflows._cli import (
    FLOW_ROOT_STEP_KEY,
    FlowListRow,
    FlowNodeRow,
    derive_flow_status,
    format_flow_list,
)
from taskq.workflows._status import reconstruct_workflow_status

pytestmark = pytest.mark.integration

GUIDE = Path(__file__).resolve().parents[1] / "docs" / "guides" / "upgrading.md"
SECTION = "### A map on another fan-out's join: the nested join has one row"


class Img(BaseModel):
    n: int


async def seeds(ctx: StepContext) -> list[dict[str, int]]:
    return [{"seed": s} for s in range(10)]


async def expand(ctx: StepContext, seed: dict[str, int]) -> list[str]:
    return [f"s{seed['seed']}-{k}" for k in range(5)]


async def ident(ctx: StepContext, x: object) -> object:
    return x


async def count(ctx: StepContext, xs: list[object]) -> int:
    return len(xs)


def _wire(app: WorkflowApp) -> str:
    """The issue's shape: a map on a map's join, consumed by ``final``."""

    @app.workflow("upgrading_map_on_map_join")
    def upgrading_map_on_map_join() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand)
        return build(step(count, map_source(first, ident), key="final"))

    return "upgrading_map_on_map_join"


def _statements() -> tuple[list[str], str]:
    """The section's SQL fences, verbatim: the three detectors and the guarded
    removal, in document order."""
    text = GUIDE.read_text(encoding="utf-8")
    start = text.index(SECTION)
    rest = text[start + len(SECTION) :]
    nxt = re.search(r"^#{2,3} ", rest, flags=re.MULTILINE)
    body = rest[: nxt.start()] if nxt else rest
    fences = [f.strip() for f in re.findall(r"```sql\n(.*?)```", body, flags=re.DOTALL)]
    detectors = [fence for fence in fences if fence.startswith("SELECT")]
    removals = [fence for fence in fences if fence.startswith("DELETE")]
    assert len(fences) == 4, f"the section carries three detectors and one removal; got {fences}"
    assert len(detectors) == 3, detectors
    assert len(removals) == 1, removals
    return detectors, removals[0]


async def _rows(conn: asyncpg.Connection, schema: str, flow_id: Any) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"SELECT id, step_key, status, idempotency_key, deps_pending, "
        f"metadata->>'blocking_reason' AS blocking_reason "
        f"FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 AND id <> $1 "
        "ORDER BY id",
        flow_id,
    )


def _listed(rows: list[asyncpg.Record], root_status: str) -> str:
    """The ``flows list`` line for this run, through the CLI's own render."""
    nodes = [
        FlowNodeRow(
            step_key=r["step_key"],
            status=r["status"],
            deps_pending=r["deps_pending"],
            blocking_reason=r["blocking_reason"],
        )
        for r in rows
    ]
    line = format_flow_list(
        [
            FlowListRow(
                run_id="00000000-0000-0000-0000-000000000000",
                workflow="upgrading_map_on_map_join",
                root_status=root_status,
                derived=derive_flow_status(nodes),
                nodes_total=len(nodes),
                created_at=None,
            )
        ]
    )
    return line[1].replace("00000000-0000-0000-0000-000000000000", "")


async def _root_status(conn: asyncpg.Connection, schema: str, flow_id: Any) -> str:
    return str(
        await conn.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1 AND step_key = $2',
            flow_id,
            FLOW_ROOT_STEP_KEY,
        )
    )


async def _forge_old_binding(
    runner: FlowRunner, conn: asyncpg.Connection, schema: str, flow_id: Any
) -> None:
    """Stamp the nested map's join onto the outer join's consumers — the
    binding a fork that ran before #745 wrote."""
    from taskq.workflows._types import ConsumerBinding, _consumer_binding_doc
    from taskq.workflows.api._graph import rate_limit_names, reservation_names
    from taskq.workflows.api._runner import _node_retry_spec

    decl = runner.compiled.nodes["seeds.join.join"]
    old_binding = _consumer_binding_doc(
        ConsumerBinding(
            step_key="seeds.join.join",
            actor=decl.actor,
            queue=decl.queue,
            payload={},
            failure_policy=decl.on_failure,
            start_to_close=decl.start_to_close,
            rate_limits=rate_limit_names(decl.rate_limits, what="the old binding"),
            reservations=reservation_names(decl.reservations, what="the old binding"),
            priority=decl.priority,
            retry=_node_retry_spec(decl),
        )
    )
    stamped = await conn.execute(
        f"UPDATE \"{schema}\".jobs SET metadata = jsonb_set(metadata, '{{consumers}}', $2::jsonb, true) "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'seeds.join'",
        flow_id,
        json.dumps([old_binding]),
    )
    assert stamped == "UPDATE 1", stamped


async def _late_drain_state(
    wf_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[FlowRunner, Any, dict[str, Any]]:
    """A terminal run in the guide's query-1 state: an in-flight run whose
    outer fork carried the pre-#745 binding, with the leader's outbox drain
    landing after the run finished. Returns (runner, flow_id, surplus row)."""
    app = WorkflowApp()
    runner = FlowRunner(app.get(_wire(app)), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # tick once (the source runs), then stamp the pre-#745 binding before the
    # outer join fires: the fire copies metadata->'consumers' into the outbox.
    await runner.tick(flow_id)
    await _forge_old_binding(runner, wf_conn, wf_schema, flow_id)

    # the leader's drain lags the dispatch (the sweep cadence): muted through
    # the run, so the outbox row is still undelivered when the run terminalises.
    real_drain = sweep_module.drain_outbox
    muted_calls = 0

    async def _muted_drain(*_a: object, **_k: object) -> int:
        nonlocal muted_calls
        muted_calls += 1
        return 0

    monkeypatch.setattr(sweep_module, "drain_outbox", _muted_drain)
    assert await runner.drive(flow_id) == "terminal"
    monkeypatch.setattr(sweep_module, "drain_outbox", real_drain)
    assert muted_calls > 0, "tick() never called the muted drain"
    assert await runner.result(flow_id) == 10

    # the leader's late tick: the drain mints the surplus nested-join row.
    await real_drain(wf_pool, runner.wsql)
    rows = await _rows(wf_conn, wf_schema, flow_id)
    surplus = [dict(r) for r in rows if r["step_key"] == "seeds.join.join"]
    assert len(surplus) == 2, surplus
    drain_row = next(r for r in surplus if r["idempotency_key"] == "wf:seeds.join.join")
    assert drain_row["status"] == "pending", drain_row
    return runner, flow_id, drain_row


async def test_the_detector_selects_the_surplus_row_and_the_guarded_removal_restores_truth(
    wf_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await wf_conn.execute(f'SET search_path TO "{wf_schema}"')
    detectors, removal = _statements()

    runner, flow_id, drain_row = await _late_drain_state(wf_pool, wf_conn, wf_schema, monkeypatch)

    before = await _rows(wf_conn, wf_schema, flow_id)
    root_before = await _root_status(wf_conn, wf_schema, flow_id)
    assert root_before == "succeeded", root_before

    # ── detector 1 selects exactly the surplus row; 2 and 3 do not ────────
    found = await wf_conn.fetch(detectors[0])
    assert [(str(r["id"]), r["step_key"], r["root_status"]) for r in found] == [
        (str(drain_row["id"]), "seeds.join.join", "succeeded")
    ], [dict(r) for r in found]
    assert [dict(r) for r in await wf_conn.fetch(detectors[1])] == []
    assert [dict(r) for r in await wf_conn.fetch(detectors[2])] == []

    # the guide's before-line: the run reads pending under a succeeded root.
    assert "pending" in _listed(before, root_before), _listed(before, root_before)
    assert "root=succeeded" in _listed(before, root_before)

    # ── the removal is a pure-orphan delete: exactly one row ──────────────
    deleted = await wf_conn.execute(removal)
    assert deleted == "DELETE 1", deleted
    await wf_conn.execute(f'SET search_path TO "{wf_schema}"')

    after = await _rows(wf_conn, wf_schema, flow_id)
    assert [r["id"] for r in after if str(r["id"]) == str(drain_row["id"])] == [], (
        f"the guard refused the surplus row or deleted more than it: {[dict(r) for r in after]}"
    )
    root_after = await _root_status(wf_conn, wf_schema, flow_id)
    assert root_after == "succeeded", root_after
    # the load-bearing promise: the root keeps its status, the result intact.
    assert await runner.result(flow_id) == 10

    # ── the estate's status truth, which the cancel remedy cannot restore ─
    assert await reconstruct_workflow_status(wf_conn, runner.wsql, flow_id) == "complete"
    after_line = _listed(after, root_after)
    assert "complete" in after_line, after_line
    assert "root=succeeded" in after_line, after_line
    # detector 1 no longer selects the run (the guide's confirmation step).
    assert [dict(r) for r in await wf_conn.fetch(detectors[0])] == []


async def test_the_detectors_and_the_removal_select_nothing_on_a_healthy_run(
    wf_pool: asyncpg.Pool, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The converse every remedy depends on: no healthy post-fix run matches a
    detector, and the removal has nothing to delete (an operator must never be
    sent after one)."""
    await wf_conn.execute(f'SET search_path TO "{wf_schema}"')
    detectors, removal = _statements()

    app = WorkflowApp()
    runner = FlowRunner(app.get(_wire(app)), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == 10
    await sweep_module.drain_outbox(wf_pool, runner.wsql)

    for i, statement in enumerate(detectors, start=1):
        rows = await wf_conn.fetch(statement)
        assert [dict(r) for r in rows] == [], f"detector {i} selected a healthy run: {rows}"
    assert await wf_conn.execute(removal) == "DELETE 0"
    await wf_conn.execute(f'SET search_path TO "{wf_schema}"')
    assert await reconstruct_workflow_status(wf_conn, runner.wsql, flow_id) == "complete"
