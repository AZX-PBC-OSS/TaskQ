# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""A map placed directly on another fan-out's join (a map's or a route's):
the nested join row ``<outer>.join.join`` has exactly ONE writer — its own
source's fork — so a run ends with exactly one such row and nothing pending.
(A route cannot sit on a join: a join declares no body, so the build refuses
it.)

The defect (#745): the outer fork listed the nested map's derived join as
one of its join's consumers, so the outer join's fire put a binding for it
on the outbox. On the fleet the leader's drain runs on the sweep cadence
(``sweep_interval``, 30 s by default) while the dispatch claims the outer
join and forks the second map at once: the fork inserts the nested join
under ``wf:{flow}:<outer>.join:<outer>.join.join``, then the drain inserts a
second row for the same step under ``wf:<outer>.join.join``. The keys differ,
the arbiter never fires, and the second row sits ``pending`` forever beside
the real one (the run reads ``pending root=succeeded``).

The pins place the drain the way the fleet's cadence can: muted until the
run is terminal (the issue's run), muted until the nested fork landed (the
drain arrives while the nested children run), or at the end of every tick
(it lands before the outer join is claimed — no nested row may exist yet).
The last pin states the invariant itself, whatever the drain's timing: no
join row's consumers name a ``map_join`` node, so the outbox never carries
one.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

import taskq.workflows._sweep as sweep_module
from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    route,
    step,
)

pytestmark = pytest.mark.integration

# map: a map on a map's join (the issue's run); route: a map on a route's
# join; mixed: the outer join also feeds a static step; depth3: a map on a
# map on a map's join (two nested joins).
SHAPES = ("map", "route", "mixed", "depth3")


class Img(BaseModel):
    n: int


class Aud(BaseModel):
    n: int


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


def _wire(app: WorkflowApp, shape: str) -> tuple[list[str], int]:
    """Register ``map_on_{shape}_join``; returns the nested join step keys
    (outermost first) and the expected final value."""
    if shape == "map":

        @app.workflow("map_on_map_join")
        def map_on_map_join() -> Promise[object]:
            first = map_source(step(seeds, key="seeds"), expand)
            return build(step(count, map_source(first, ident), key="final"))

        return ["seeds.join.join"], 10

    if shape == "route":

        @app.workflow("map_on_route_join")
        def map_on_route_join() -> Promise[object]:
            routed = route(step(media, key="media"), {Img: RouteArm(body=on_img), Aud: on_aud})
            return build(step(count, map_source(routed, ident), key="final"))

        return ["media.join.join"], 6

    if shape == "mixed":

        @app.workflow("map_on_mixed_join")
        def map_on_mixed_join() -> Promise[object]:
            first = map_source(step(seeds, key="seeds"), expand)
            sib = step(count, first, key="sib")
            return build(step(add, map_source(first, ident), sib, key="final"))

        return ["seeds.join.join"], 20

    @app.workflow("map_on_depth3_join")
    def map_on_depth3_join() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand)
        return build(step(count, map_source(map_source(first, ident), ident), key="final"))

    return ["seeds.join.join", "seeds.join.join.join"], 10


async def _flow_rows(conn: asyncpg.Connection, schema: str, flow_id: Any) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT step_key, status, idempotency_key FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND id <> $1 ORDER BY id",
        flow_id,
    )


async def _nested_count(
    conn: asyncpg.Connection, schema: str, flow_id: Any, step_key: str, *, waiting: bool = False
) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2"
            + (" AND status = 'pending' AND deps_pending > 0" if waiting else ""),
            flow_id,
            step_key,
        )
        or 0
    )


def _assert_one_writer(rows: list[asyncpg.Record], nested: list[str]) -> None:
    for key in nested:
        found = [dict(r) for r in rows if r["step_key"] == key]
        assert len(found) == 1, f"the nested join {key!r} has one writer; got rows {found}"
        assert found[0]["status"] == "succeeded", found
    pending = [dict(r) for r in rows if r["status"] == "pending"]
    assert pending == [], f"a terminal run holds no pending row; got {pending}"


@pytest.mark.parametrize(
    ("shape", "drain_lands"),
    [(s, "after_terminal") for s in SHAPES] + [(s, "while_live") for s in ("map", "route")],
)
async def test_a_late_drain_never_mints_a_second_nested_join_row(
    shape: str,
    drain_lands: str,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fleet's usual ordering: the second fork lands before the
    leader's drain. The drain lands either after the run is terminal (the
    issue's run) or while the nested children still run (children that
    outlive the sweep interval). Either way each nested join has exactly
    one row and the terminal run has no pending row."""
    app = WorkflowApp()
    nested, expected = _wire(app, shape)
    runner = FlowRunner(app.get(f"map_on_{shape}_join"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    real_drain = sweep_module.drain_outbox
    muted_calls = 0

    async def _muted_drain(*_a: object, **_k: object) -> int:
        nonlocal muted_calls
        muted_calls += 1
        return 0

    # the leader's drain lags the dispatch (the sweep cadence): mute it — the
    # run completes without it (the consumers' deps are released by the fork's
    # edges; the drain is the belt).
    monkeypatch.setattr(sweep_module, "drain_outbox", _muted_drain)
    if drain_lands == "while_live":
        for _ in range(50):
            await runner.tick(flow_id)
            if await _nested_count(wf_conn, wf_schema, flow_id, nested[0], waiting=True):
                break
        assert await _nested_count(wf_conn, wf_schema, flow_id, nested[0], waiting=True) == 1
        # the leader's tick lands while the nested children run; every
        # later tick drains too.
        monkeypatch.setattr(sweep_module, "drain_outbox", real_drain)
    assert await runner.drive(flow_id) == "terminal"
    monkeypatch.setattr(sweep_module, "drain_outbox", real_drain)
    # the mute works only while tick() reads drain_outbox off the module at
    # call time; a hoisted import would leave it inert and these arms would
    # silently run the in-process order.
    assert muted_calls > 0, "tick() never called the muted drain"
    assert await runner.result(flow_id) == expected
    await real_drain(wf_pool, runner.wsql)  # the leader's late tick

    _assert_one_writer(await _flow_rows(wf_conn, wf_schema, flow_id), nested)


@pytest.mark.parametrize("shape", SHAPES)
async def test_the_in_process_drive_keeps_one_nested_join_row(
    shape: str, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The in-process ordering (the drain runs at the end of every tick,
    before the outer join is claimed): the same law — one row per nested
    join, nothing pending, the right answer."""
    app = WorkflowApp()
    nested, expected = _wire(app, shape)
    runner = FlowRunner(app.get(f"map_on_{shape}_join"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == expected
    await sweep_module.drain_outbox(wf_pool, runner.wsql)

    _assert_one_writer(await _flow_rows(wf_conn, wf_schema, flow_id), nested)


@pytest.mark.parametrize("shape", ["map", "route"])
async def test_an_early_drain_never_creates_the_nested_join_before_its_fork(
    shape: str, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The other ordering: the drain lands between the outer join's fire
    and its claim. No row for the nested join may exist yet — its source
    (the outer join) has not forked. A drain-made one is pending with
    deps_pending 0 and no join-wait marker, so any worker can claim it
    before the fork; it then succeeds empty, the fork adopts the terminal
    row, and ``final``'s reserved dep is never released (the run wedges
    ``running``, or ``final`` runs on the empty join)."""
    app = WorkflowApp()
    nested, expected = _wire(app, shape)
    outer_join = nested[0].removesuffix(".join")
    runner = FlowRunner(app.get(f"map_on_{shape}_join"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    fired = False
    for _ in range(50):
        await runner.tick(flow_id)  # the tick ends with the drain
        fired = bool(
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2 "
                "AND status = 'pending' AND deps_pending = 0",
                flow_id,
                outer_join,
            )
        )
        if fired:
            break
    assert fired, f"the outer join {outer_join!r} never fired"
    premature = await _flow_rows(wf_conn, wf_schema, flow_id)
    assert [dict(r) for r in premature if r["step_key"] == nested[0]] == [], (
        f"the nested join {nested[0]!r} exists before its source {outer_join!r} forked"
    )

    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == expected
    _assert_one_writer(await _flow_rows(wf_conn, wf_schema, flow_id), nested)


@pytest.mark.parametrize("shape", SHAPES)
async def test_no_binding_names_a_nested_join(
    shape: str, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The invariant, not its symptoms: a join row's only writer is its
    own source's fork, so no join row's consumers name a ``map_join``
    node and the outbox never carries one — with no outbox row there is
    no drained row to race the fork, in any order. The outbox carries
    exactly the static consumers of the joins (``mixed``: the outer
    join still feeds ``sib``, through its one static row)."""
    app = WorkflowApp()
    _nested, expected = _wire(app, shape)
    runner = FlowRunner(app.get(f"map_on_{shape}_join"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == expected

    nodes = runner.compiled.nodes
    map_joins = {k for k, n in nodes.items() if n.kind == "map_join"}
    static_consumers = sorted(
        k for k, n in nodes.items() if n.kind != "map_join" and set(n.parents) & map_joins
    )
    # the binding doc carries no kind: key the check on step keys.
    named = await wf_conn.fetch(
        f"SELECT j.step_key AS join_key, c->>'step_key' AS consumer "
        f"FROM \"{wf_schema}\".jobs j, jsonb_array_elements(j.metadata->'consumers') c "
        "WHERE (j.metadata->>'flow_id')::uuid = $1 AND j.metadata ? 'consumers' "
        "ORDER BY 1, 2",
        flow_id,
    )
    bindings = [(r["join_key"], r["consumer"]) for r in named]
    assert [b for b in bindings if b[1] in map_joins] == [], (
        f"a join row's consumers name a map_join node: {bindings}"
    )
    outbox = [
        r["consumer_step_key"]
        for r in await wf_conn.fetch(
            f'SELECT consumer_step_key FROM "{wf_schema}".wf_outbox '
            "WHERE flow_id = $1 ORDER BY consumer_step_key",
            flow_id,
        )
    ]
    assert outbox == static_consumers, (
        f"the outbox carries exactly the joins' static consumers; got {outbox}"
    )
    if shape == "mixed":
        assert [c for j, c in bindings if j == "seeds.join"] == ["sib"], bindings
        sib = await wf_conn.fetch(
            f'SELECT idempotency_key FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'sib'",
            flow_id,
        )
        assert [r["idempotency_key"] for r in sib] == ["wf:sib"], sib
