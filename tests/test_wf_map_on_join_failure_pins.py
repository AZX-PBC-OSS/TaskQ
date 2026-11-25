# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""The map-on-another-fan-out's-join family's FAILURE face.

``tests/test_wf_map_on_join_pins.py`` pins the success path of the #745 fix
(one writer per nested join, nothing pending). Nothing there makes a step
fail, and the failure path is where the topology's one novel row shape
lives: the post-map step (``final``) is a STATIC row whose dep is *reserved*
without an edge (``_runner.py::_insert_static_nodes``'s ``parent_node.kind
== "map_join"`` branch), and the edge that releases it is written only when
the NESTED fork runs (``_fork.py::insert_fork``'s ``wsql.fork_consumer_edges``).

So between the consumer's birth and the nested fork's claim, ``final`` is
``pending`` with ``deps_pending = 1``, the bare ``join`` marker and ZERO edge
rows — which is also the exact shape the re-derive sweep's ``blocked`` arm
reads (``_sql_sweep.py::REDERIVE_SWEEP_SQL``: ``c.missing_parents > 0 OR
c.edge_count = 0``). Before the repair, that arm stamped it
``orphan_parent``, its ``jsonb_set`` OVERWROTE ``blocking_reason``, and both
the ``blocked`` guard (``NOT metadata @> '{"blocking_reason":
"orphan_parent"}'``) and the ``flow_fenced`` arm's own predicate
(``metadata @> '{"blocking_reason": "join"}'``) then locked the row out of
every later pass — so the truthful ``flow_dead``/``failed_parent``
resolution, which the same statement has an arm for, was never reachable
again.

The pinned law, in both directions:

* a run that SUCCEEDS must leave no ``orphan_parent`` record on the consumer
  that ran (the pass defers an edge-less row while another live row can
  still fork and write its edge);
* a DEAD flow's consumer must carry the fence's record (``flow_dead``) or
  the named dead parent (``failed_parent``), never the wiring-fault stamp
  (a terminal flow's resolution belongs to the ``flow_fenced`` arm).

The stamps are not interchangeable. ``docs/guides/runbooks.md`` documents
``'orphan_parent'`` as "an edge points at a parent row that does not exist (a
misnamed child): the join can never fire; fix the definition and re-run" —
a wiring fault with a wiring remedy — while ``'flow_dead'`` is "the fence's
record: the flow died and this join can never fire" with the retry-or-cancel
remedy ``_cli.py::stuck_lines`` derives from it.

``test_a_route_cannot_sit_on_a_join`` pins the build refusal that keeps a
``map_join`` node out of every ROUTE's consumer set — the reason the #745
predicate added to ``_route_fork`` cannot fire on any shape the DSL accepts.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    gather,
    map_source,
    route,
    step,
)
from taskq.workflows._status import reconstruct_workflow_status

#: the reasons a consumer row on a DEAD flow may honestly carry: the fence's
#: record (``'flow_dead'``) or the named dead parent (``'failed_parent'``).
TRUTHFUL_DEAD_REASONS = ("failed_parent", "flow_dead")


class Img(BaseModel):
    n: int


async def seeds(ctx: StepContext) -> list[dict[str, int]]:
    return [{"seed": s} for s in range(3)]


async def expand(ctx: StepContext, seed: dict[str, int]) -> list[str]:
    if seed["seed"] == 1:
        raise RuntimeError("the corpus reader failed")
    return [f"s{seed['seed']}-{k}" for k in range(2)]


async def expand_clean(ctx: StepContext, seed: dict[str, int]) -> list[str]:
    return [f"s{seed['seed']}-{k}" for k in range(2)]


async def on_img(ctx: StepContext, item: Img) -> str:
    return f"img{item.n}"


async def ident(ctx: StepContext, x: object) -> object:
    return x


async def count(ctx: StepContext, xs: list[object]) -> int:
    return len(xs)


def _map_on_join(app: WorkflowApp, name: str, item_body: Any) -> None:
    """Register the issue's topology: a map, then a map placed on that map's
    join, then the static step that consumes the NESTED join."""
    body = item_body

    @app.workflow(name)
    def wired() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), body)
        return build(step(count, map_source(first, ident), key="final"))


async def _final_row(conn: asyncpg.Connection, schema: str, flow_id: Any) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f'SELECT step_key, status, deps_pending, metadata FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'final'",
        flow_id,
    )


def _reason(row: asyncpg.Record | None) -> str | None:
    assert row is not None
    # jsonb comes back as a str on the fixture's codec; tolerate both.
    meta = row["metadata"]
    if isinstance(meta, str):
        meta = json.loads(meta)
    return meta.get("blocking_reason") if isinstance(meta, dict) else None


@pytest.mark.integration
async def test_a_dead_flow_resolves_the_post_map_consumer_with_a_truthful_reason(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """One outer item fails (``fail_closed``): the outer join can never fire,
    so the nested fork never runs and ``final``'s reserved dep is never
    released. The run itself reaches a truthful terminal (``failed``); the
    ROW must say why it can never run. ``orphan_parent`` says the child edge
    is misnamed and sends the operator to fix a definition (runbooks.md) —
    on a run whose real remedy is the failed step's retry."""
    app = WorkflowApp()
    _map_on_join(app, "map_on_join_item_failure", expand)
    runner = FlowRunner(app.get("map_on_join_item_failure"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    assert await runner.drive(flow_id) == "terminal"
    # the run's own verdict is legible: the failed child fails the root.
    assert await reconstruct_workflow_status(wf_conn, runner.wsql, flow_id) == "failed"

    final = await _final_row(wf_conn, wf_schema, flow_id)
    assert final is not None, "the post-map consumer is a static row — it always exists"
    assert final["status"] == "pending", f"the nested fork never ran; got {dict(final)}"
    reason = _reason(final)
    assert reason != "orphan_parent", (
        f"THE FALSE-WIRING RECORD: the post-map consumer of a nested join rests "
        f"{dict(final)} — the re-derive sweep stamped 'orphan_parent' (its blocked "
        "arm's c.edge_count = 0 leg) on a row whose edge was never written because "
        "the nested fork never ran, not because the edge is misnamed. The stamp "
        "overwrote 'join', so the sweep's flow_fenced arm can never re-stamp it and "
        "no later pass touches the row again. runbooks.md reads 'orphan_parent' as "
        "'a misnamed child: fix the definition and re-run' — the operator is sent "
        "after a wiring bug that does not exist."
    )
    assert reason in TRUTHFUL_DEAD_REASONS, (
        f"the consumer of a nested join on a dead flow must carry the fence's record "
        f"('flow_dead') or the named dead parent ('failed_parent'); got {reason!r} "
        f"({dict(final)})"
    )


@pytest.mark.integration
async def test_a_successful_nested_map_leaves_no_orphan_parent_stamp(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """The same stamp lands on a run that SUCCEEDS: the window between the
    consumer's birth and the nested fork's consumer-edge write spans the whole
    outer map, so the in-process drive's first sweep tick stamps it before any
    child has finished. A completed run must not carry the misnamed-child
    record on a node that ran."""
    app = WorkflowApp()
    _map_on_join(app, "map_on_join_clean", expand_clean)
    runner = FlowRunner(app.get("map_on_join_clean"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    assert await runner.drive(flow_id) == "terminal"
    assert await reconstruct_workflow_status(wf_conn, runner.wsql, flow_id) == "complete"
    assert await runner.result(flow_id) == 3

    final = await _final_row(wf_conn, wf_schema, flow_id)
    assert final is not None
    assert final["status"] == "succeeded", dict(final)
    assert _reason(final) != "orphan_parent", (
        f"A CLEAN RUN CARRIES THE WIRING-FAULT RECORD: {dict(final)} — the consumer "
        "ran and the run completed, yet the row still says its child edge is "
        "misnamed. Any detector keyed on metadata.blocking_reason = "
        "'orphan_parent' reads this healthy run as a broken definition."
    )


def test_a_route_cannot_sit_on_a_join() -> None:
    """The refusal that makes ``_route_fork``'s #745 predicate unreachable:
    a join node declares no body, so ``_attach_route`` refuses it. Only a
    ``map_source`` can be placed on a join, so only ``_map_fork``'s consumer
    comprehension can ever name a ``map_join`` node."""
    app = WorkflowApp()

    @app.workflow("route_on_join_refused")
    def wired() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand_clean)
        return build(route(first, {Img: RouteArm(body=on_img)}))

    with pytest.raises(WorkflowBuildError, match="declares no body"):
        app.get("route_on_join_refused")


@pytest.mark.integration
async def test_an_empty_corpus_under_a_nested_map_still_terminalises(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """The composition this topology newly creates: a ZERO-consumer join over
    a ZERO-item corpus. The outer map's body returns ``[]``, so the outer join
    is born ``deps_pending = 0``, fires at birth, and (as a map source itself)
    forks the nested map over the empty list — whose join is again born
    ``deps_pending = 0`` but now HAS the static consumer. ``insert_fork``
    writes the consumer edge before the born-zero fire, so the born-zero nested
    join is claimable, runs, and releases the consumer's reserved dep. The run
    must terminalise SUCCESS with ``result() == 0``, never wedge ``running``
    with an unreleasable reserved dep."""

    async def nothing(ctx: StepContext) -> list[dict[str, int]]:
        return []

    app = WorkflowApp()

    @app.workflow("map_on_join_empty")
    def wired() -> Promise[object]:
        first = map_source(step(nothing, key="seeds"), expand_clean)
        return build(step(count, map_source(first, ident), key="final"))

    runner = FlowRunner(app.get("map_on_join_empty"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    assert await runner.drive(flow_id) == "terminal"
    assert await reconstruct_workflow_status(wf_conn, runner.wsql, flow_id) == "complete"
    assert await runner.result(flow_id) == 0

    nested = await wf_conn.fetch(
        f'SELECT step_key, status, deps_pending FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'seeds.join%' "
        "ORDER BY step_key",
        flow_id,
    )
    assert [(r["step_key"], r["status"], r["deps_pending"]) for r in nested] == [
        ("seeds.join", "succeeded", 0),
        ("seeds.join.join", "succeeded", 0),
    ], [dict(r) for r in nested]


@pytest.mark.integration
async def test_a_gather_on_a_map_join_is_not_stamped_in_the_in_process_drive(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """The SCOPE of the mis-stamp above, pinned so the repair does not
    overshoot: the in-process gate holds for a consumer whose producer join
    already exists when the pass runs. ``FlowRunner.tick`` claims and runs
    BEFORE it sweeps, so a ``gather`` reserved on a map's join gets its edge
    in the same tick the map source forks, and the sweep never sees the gap.
    Only a consumer of a *fork-spawned* join (``<x>.join.join`` — created a
    whole map-phase later) stays edge-less across passes.

    This is why the finding names the nested topology rather than the whole
    reserved-dep path; if this test ever goes red, the stamp is general and
    the repair's blast radius is wider than stated."""

    async def expand_fan(ctx: StepContext, seed: dict[str, int]) -> list[str]:
        return [f"s{seed['seed']}-{k}" for k in range(2)]

    async def total(ctx: StepContext, xs: list[object]) -> int:
        return sum(len(x) if isinstance(x, list) else 1 for x in xs)

    app = WorkflowApp()

    @app.workflow("gather_on_map_join")
    def wired() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand_fan)
        return build(step(total, gather([first]), key="final"))

    runner = FlowRunner(app.get("gather_on_map_join"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    assert await runner.drive(flow_id) == "terminal"
    assert await reconstruct_workflow_status(wf_conn, runner.wsql, flow_id) == "complete"

    rows = await wf_conn.fetch(
        f'SELECT step_key, status, metadata FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gather'",
        flow_id,
    )
    assert len(rows) == 1, [dict(r) for r in rows]
    assert _reason(rows[0]) != "orphan_parent", (
        f"the reserved-dep window is no longer closed inside a tick — the "
        f"mis-stamp is not specific to a fork-spawned join: {dict(rows[0])}"
    )
