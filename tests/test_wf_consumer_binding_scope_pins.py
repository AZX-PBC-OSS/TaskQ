# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""The *other* fan-in shapes the fork's ``is_fork_spawned_join`` exclusion decides.

``tests/test_wf_map_on_join_pins.py`` pins the exclusion (a nested map's join
is not a consumer) and one retained consumer of a MAP join (``mixed``: ``sib``).
These pins walk the shapes that file never combines, because the predicate's
correctness is a two-sided claim: it must exclude the fork-spawned join row and
keep every statically inserted child — the child whose ``deps_pending``
``_insert_static_nodes`` reserved against the join and whose only release is the
consumer binding the fork stamps.

* a static sibling of a ROUTE's join (the ``mixed`` shape, route side);
* a ``gather`` over a map's join (``kind="gather"`` — a static row);
* a ``gather`` that is *also* a map source (``kind="gather"`` on both ends of
  the predicate: a reserved consumer of one join and the source of another);
* the compile's kind domain itself, and the naming law the operator-facing
  upgrade detectors rely on (every fork-spawned join key ends in ``.join``).
"""

from __future__ import annotations

import enum
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    RouteArm,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    gather,
    loop,
    map_source,
    route,
    step,
)
from taskq.workflows.chain import DONE, Route

pytestmark = pytest.mark.integration


class Img(BaseModel):
    n: int


class Aud(BaseModel):
    n: int


class Outcome(enum.Enum):
    OK = "ok"


async def seeds(ctx: StepContext) -> list[dict[str, int]]:
    return [{"seed": s} for s in range(10)]


async def expand(ctx: StepContext, seed: dict[str, int]) -> list[str]:
    return [f"s{seed['seed']}-{k}" for k in range(5)]


async def media(ctx: StepContext) -> list[Img | Aud]:
    return [Img(n=i) if i % 2 else Aud(n=i) for i in range(6)]


async def on_img(ctx: StepContext, x: Img) -> str:
    return f"img{x.n}"


async def on_aud(ctx: StepContext, x: Aud) -> str:
    return f"aud{x.n}"


async def ident(ctx: StepContext, x: object) -> object:
    return x


async def count(ctx: StepContext, xs: list[object]) -> int:
    return len(xs)


async def add(ctx: StepContext, xs: list[object], n: int) -> int:
    return len(xs) + n


async def one(ctx: StepContext) -> int:
    return 1


async def _flow_rows(conn: asyncpg.Connection, schema: str, flow_id: Any) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT step_key, status, idempotency_key, deps_pending FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND id <> $1 ORDER BY id",
        flow_id,
    )


async def _rows_for(
    conn: asyncpg.Connection, schema: str, flow_id: Any, step_key: str
) -> list[dict[str, Any]]:
    return [dict(r) for r in await _flow_rows(conn, schema, flow_id) if r["step_key"] == step_key]


async def _consumers_of(
    conn: asyncpg.Connection, schema: str, flow_id: Any, join_key: str
) -> list[str]:
    rows = await conn.fetch(
        f"SELECT c->>'step_key' AS consumer FROM \"{schema}\".jobs j, "
        "jsonb_array_elements(j.metadata->'consumers') c "
        "WHERE (j.metadata->>'flow_id')::uuid = $1 AND j.step_key = $2 ORDER BY 1",
        flow_id,
        join_key,
    )
    return [r["consumer"] for r in rows]


def _assert_no_pending(rows: list[asyncpg.Record]) -> None:
    pending = [dict(r) for r in rows if r["status"] == "pending"]
    assert pending == [], f"a terminal run holds no pending row; got {pending}"


async def test_a_route_join_keeps_its_static_sibling(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The ``mixed`` shape on the ROUTE side.

    ``media.join``'s consumers are exactly ``["sib"]``: the map on the join is
    excluded, the static sibling is not (its reserved dep is released only by
    the binding the route's fork stamps)."""
    app = WorkflowApp()

    @app.workflow("route_join_static_sibling")
    def route_join_static_sibling() -> Promise[object]:
        routed = route(step(media, key="media"), {Img: RouteArm(body=on_img), Aud: on_aud})
        sib = step(count, routed, key="sib")
        return build(step(add, map_source(routed, ident), sib, key="final"))

    runner = FlowRunner(app.get("route_join_static_sibling"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    # the outer value is the 6 arm returns; sib counts the same 6.
    assert await runner.result(flow_id) == 12

    rows = await _flow_rows(wf_conn, wf_schema, flow_id)
    _assert_no_pending(rows)
    nested = await _rows_for(wf_conn, wf_schema, flow_id, "media.join.join")
    assert len(nested) == 1, nested
    assert nested[0]["status"] == "succeeded", nested
    sib = await _rows_for(wf_conn, wf_schema, flow_id, "sib")
    assert [r["idempotency_key"] for r in sib] == ["wf:sib"], sib
    assert await _consumers_of(wf_conn, wf_schema, flow_id, "media.join") == ["sib"]


async def test_a_gather_over_a_map_join_stays_a_consumer(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """``kind="gather"`` must stay a consumer: the gather is a static row whose
    dep ``_insert_static_nodes`` reserves against the join."""
    app = WorkflowApp()

    @app.workflow("gather_over_map_join")
    def gather_over_map_join() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand)
        other = step(one, key="other")
        joined = gather([first, other])
        return build(step(count, joined, key="final"))

    compiled = app.get("gather_over_map_join")
    gather_keys = [k for k, n in compiled.nodes.items() if n.kind == "gather"]
    assert len(gather_keys) == 1, compiled.nodes
    gather_key = gather_keys[0]

    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    # the gather packs its two parents: the map's 10-item list and `one`'s 1.
    assert await runner.result(flow_id) == 2

    rows = await _flow_rows(wf_conn, wf_schema, flow_id)
    _assert_no_pending(rows)
    gathered = await _rows_for(wf_conn, wf_schema, flow_id, gather_key)
    assert len(gathered) == 1, gathered
    assert gathered[0]["status"] == "succeeded", gathered
    assert await _consumers_of(wf_conn, wf_schema, flow_id, "seeds.join") == [gather_key]


async def test_a_map_on_a_gather_keeps_the_gather_a_consumer(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The same node is a reserved consumer of one join and the source of
    another (``kind="gather"`` on both ends of the predicate)."""
    app = WorkflowApp()

    @app.workflow("map_on_gather")
    def map_on_gather() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand)
        other = step(one, key="other")
        joined = gather([first, other])
        return build(step(count, map_source(joined, ident), key="final"))

    compiled = app.get("map_on_gather")
    gather_key = next(k for k, n in compiled.nodes.items() if n.kind == "gather")

    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == 2

    rows = await _flow_rows(wf_conn, wf_schema, flow_id)
    _assert_no_pending(rows)
    nested = await _rows_for(wf_conn, wf_schema, flow_id, f"{gather_key}.join")
    assert len(nested) == 1, nested
    assert nested[0]["status"] == "succeeded", nested
    assert await _consumers_of(wf_conn, wf_schema, flow_id, "seeds.join") == [gather_key]


async def _source_body(ctx: StepContext, upstream: object) -> None:
    del ctx, upstream


async def _screen(ctx: StepContext, item: object) -> Outcome:
    del ctx, item
    return Outcome.OK


def _chain() -> Chain:
    return Chain(
        name="kind-domain-chain",
        start="screen",
        queue="default",
        steps={
            "screen": Step(
                body=_screen,
                outcomes=Outcome,
                route=Route({Outcome.OK: DONE}),
            )
        },
    )


async def _loop_body(ctx: StepContext, carry: object) -> object:
    del ctx
    return carry


def test_the_compiled_kind_domain_is_enumerated() -> None:
    """The domain the fork predicate keys on has no ``Literal``/enum; this
    canary names it, so a new ``kind`` cannot reach production without a
    decision about ``_insert_static_nodes``' skip and the fork builders'
    consumer filters.

    ``NodeDecl.kind`` declares five values and the compile assigns all five
    here. The second half is the naming law the operator-facing upgrade
    detectors read instead of a ``kind`` column: every fork-spawned join's
    step key ends in ``.join`` (and every ``.join`` node is fork-spawned), so
    ``n.step_key LIKE '%.join.join'`` selects exactly the nested joins."""
    app = WorkflowApp()

    @app.workflow("kind_domain")
    def kind_domain() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand)
        other = step(one, key="other")
        joined = gather([first, other])
        routed = route(step(media, key="media"), {Img: RouteArm(body=on_img), Aud: on_aud})
        chained = chain_source(_chain(), _source_body, other, key="source")
        counted = loop("counter", _loop_body, max_iterations=2)
        return build(chained, joined, routed, first, counted)

    compiled = app.get("kind_domain")
    observed = {n.kind for n in compiled.nodes.values()}
    assert observed == {"step", "gather", "map_join", "chain_source", "loop"}, observed

    fork_spawned = {k for k, n in compiled.nodes.items() if n.is_fork_spawned_join}
    dotted = {k for k in compiled.nodes if k.endswith(".join")}
    assert fork_spawned == dotted, (fork_spawned, dotted)
    assert fork_spawned == {"seeds.join", "media.join"}, fork_spawned
