# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""The substep-scope pins (issue #743): a ``ctx.substep`` record belongs to
the node execution that made it, never to the run.

The invariant: a substep's memo and claim resolve only rows written by the
SAME owner (the calling node's job, its map index, its loop iteration) under
the SAME substep name. Two nodes, two maps, a node key and a substep name
that happen to share a string never share a ledger row, and the replay never
returns another owner's result.

The rejected shape (HEAD aef9e8c1): ``WorkflowSteps.step`` passes the bare
name as the ledger's ``step_key``, so the memo read
``(flow_id, step_key, map_index)`` and the claim arbiter
``(flow_id, step_key, COALESCE(map_index, -1), attempt, claim_epoch)`` are
run-scoped: the second node's substep replays the first node's value, a
substep named after a node returns that node's envelope, and two concurrent
nodes write one row.

Every pin drives the real FlowRunner (the in-process driver); the fleet door
runs the same ``_execute_claimed`` tail and the same ``StepContext.substep``,
so the identity under test is driver-agnostic. One pin runs attempt 2 through
the fleet door itself, to pin that both doors hand the context one job id.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    Done,
    FlowRunner,
    GateDecl,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    map_source,
    step,
)
from taskq.workflows.api._hitl import HitlClient
from taskq.workflows.engine import render_workflow_sql
from taskq.workflows.ledger import memoized_substep_result
from tests._wf_fixtures import seed_flow, seed_running_node


class Item(BaseModel):
    n: int


async def _ledger_rows(
    conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        "SELECT l.step_key, l.map_index, l.status, l.result, j.step_key AS owner "
        f'FROM "{schema}".wf_step_ledger AS l JOIN "{schema}".jobs AS j ON j.id = l.job_id '
        "WHERE l.flow_id = $1 ORDER BY l.created_at, l.id",
        flow_id,
    )
    return [dict(r) for r in rows]


async def test_two_nodes_sharing_a_substep_name_each_run_their_own(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The issue's sequential repro: ``sa`` then ``sb``, both
    ``ctx.substep("x", ...)``. Each node's substep runs its own fn and
    returns its own value; the ledger holds one substep record per node."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def sa(ctx: StepContext) -> str:
        return await ctx.substep("x", ret, "A")

    async def sb(ctx: StepContext, _a: str) -> str:
        return await ctx.substep("x", ret, "B")

    async def pair(ctx: StepContext, a: str, b: str) -> dict[str, str]:
        return {"a": a, "b": b}

    app = WorkflowApp()

    @app.workflow("substep_scope_seq")
    def seq() -> Promise[object]:
        a = step(sa, key="sa")
        return build(step(pair, a, step(sb, a, key="sb"), key="out"))

    runner = FlowRunner(app.get("substep_scope_seq"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await _ledger_rows(wf_conn, wf_schema, flow_id)
    substep_rows = [r for r in rows if r["step_key"] not in ("sa", "sb", "out")]
    assert await runner.result(flow_id) == {"a": "A", "b": "B"}, (calls, substep_rows)
    assert calls == ["A", "B"], f"sb's substep never ran its own fn: {calls}"
    owners = sorted((r["owner"], r["result"]) for r in substep_rows)
    assert owners == [("sa", '"A"'), ("sb", '"B"')], substep_rows


async def test_a_substep_named_after_an_upstream_node_runs_its_own_fn(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A substep whose name equals another node's key must not replay that
    node's stored envelope."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def first(ctx: StepContext) -> str:
        return "node-first-result"

    async def second(ctx: StepContext, _f: str) -> object:
        return await ctx.substep("first", ret, "substep-value")

    app = WorkflowApp()

    @app.workflow("substep_scope_nodekey")
    def wf() -> Promise[object]:
        return build(step(second, step(first, key="first"), key="second"))

    runner = FlowRunner(app.get("substep_scope_nodekey"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    got = await runner.result(flow_id)
    assert got == "substep-value", f"the substep replayed node 'first's record: {got!r}"
    assert calls == ["substep-value"], calls


async def test_a_substep_named_after_its_own_node_keeps_the_node_record(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A substep named after its OWN node must not land on the node's own
    claim row: the node's record keeps the node's envelope, the substep keeps
    its own row."""

    async def ret(v: str) -> str:
        return v

    async def selfie(ctx: StepContext) -> dict[str, str]:
        return {"inner": await ctx.substep("selfie", ret, "raw-substep-value")}

    app = WorkflowApp()

    @app.workflow("substep_scope_self")
    def wf() -> Promise[object]:
        return build(step(selfie, key="selfie"))

    runner = FlowRunner(app.get("substep_scope_self"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"inner": "raw-substep-value"}

    rows = await _ledger_rows(wf_conn, wf_schema, flow_id)
    node_rows = [r for r in rows if r["step_key"] == "selfie"]
    assert len(rows) == 2, f"the substep collapsed onto the node's own claim row: {rows}"
    assert len(node_rows) == 1, rows
    assert node_rows[0]["status"] == "succeeded", node_rows
    assert "raw-substep-value" in str(node_rows[0]["result"]) and node_rows[0]["result"] != (
        '"raw-substep-value"'
    ), f"the node's record holds the raw substep value, not its envelope: {node_rows}"


async def test_two_maps_sharing_a_substep_name_keep_their_own_values(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Two maps over [0, 1] whose children both call ``ctx.substep("item")``:
    the map index is the same, the owner is not. The second map's children
    must not replay the first map's values."""

    async def ret(v: str) -> str:
        return v

    async def items(ctx: StepContext) -> list[Item]:
        return [Item(n=0), Item(n=1)]

    async def child_a(ctx: StepContext, item: Item) -> str:
        return await ctx.substep("item", ret, f"a{item.n}")

    async def child_b(ctx: StepContext, item: Item) -> str:
        return await ctx.substep("item", ret, f"b{item.n}")

    async def both(ctx: StepContext, a: list[str], b: list[str]) -> dict[str, list[str]]:
        return {"a": a, "b": b}

    app = WorkflowApp()

    @app.workflow("substep_scope_maps")
    def wf() -> Promise[object]:
        ma = map_source(step(items, key="srca"), child_a)
        mb = map_source(step(items, key="srcb"), child_b)
        return build(step(both, ma, mb, key="out"))

    runner = FlowRunner(app.get("substep_scope_maps"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"a": ["a0", "a1"], "b": ["b0", "b1"]}


async def test_concurrent_nodes_sharing_a_substep_name_get_separate_records(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The concurrent variant: two independent nodes run their ``x`` substeps
    overlapped (sb claims while sa's claim is still ``running``). Each must
    record its own row with its own value — the second claim must never
    land on the first node's live row (on HEAD it does: context.py returns
    early only for ``succeeded``, so both bodies run and sb's terminal
    overwrites sa's record)."""
    sa_in_fn = asyncio.Event()
    sb_done = asyncio.Event()

    async def ret_a() -> str:
        sa_in_fn.set()
        await asyncio.wait_for(sb_done.wait(), timeout=10)
        return "A"

    async def ret_b() -> str:
        return "B"

    async def sa(ctx: StepContext) -> str:
        return await ctx.substep("x", ret_a)

    async def sb(ctx: StepContext) -> str:
        await asyncio.wait_for(sa_in_fn.wait(), timeout=10)
        try:
            return await ctx.substep("x", ret_b)
        finally:
            sb_done.set()

    async def pair(ctx: StepContext, a: str, b: str) -> dict[str, str]:
        return {"a": a, "b": b}

    app = WorkflowApp()

    @app.workflow("substep_scope_concurrent")
    def wf() -> Promise[object]:
        return build(step(pair, step(sa, key="sa"), step(sb, key="sb"), key="out"))

    runner = FlowRunner(app.get("substep_scope_concurrent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    claimable = await runner._claimable(flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the overlap needs the two roots run concurrently; drive() runs them in sequence.
    by_key = {r["step_key"]: r for r in claimable}
    assert set(by_key) == {"sa", "sb"}, by_key
    await asyncio.gather(
        runner._run_node(flow_id, by_key["sa"]),  # pyright: ignore[reportPrivateUsage]  # Why: see above.
        runner._run_node(flow_id, by_key["sb"]),  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    )
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"a": "A", "b": "B"}

    rows = await _ledger_rows(wf_conn, wf_schema, flow_id)
    substep_rows = [r for r in rows if r["step_key"] not in ("sa", "sb", "out")]
    owners = sorted((r["owner"], r["status"], r["result"]) for r in substep_rows)
    assert owners == [("sa", "succeeded", '"A"'), ("sb", "succeeded", '"B"')], (
        f"the two nodes share one substep record: {substep_rows}"
    )


async def test_concurrent_same_name_substeps_in_one_body_refuse_the_second(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The one shape that can still land a claim on a live row once records
    are owner-scoped: ONE body attempt issuing the same name concurrently.
    The second call must refuse loudly before touching the ledger (its
    ``ON CONFLICT DO UPDATE`` would re-stamp the first claim's row and drop
    the first terminal), never run its fn, and leave the first call's record
    intact. On HEAD both fns run and the row ends with the second value.

    The guard releases the name when the first call finishes (a sequential
    re-call replays ``"first"``), and ``idempotent=False`` calls write no
    row, so two overlapping ones with one name both run."""
    calls: list[str] = []
    first_in_fn = asyncio.Event()
    loose_in_fn = asyncio.Event()

    async def slow() -> str:
        calls.append("first")
        first_in_fn.set()
        await asyncio.sleep(0.2)
        return "first"

    async def fast() -> str:
        calls.append("second")
        return "second"

    async def loose(tag: str) -> str:
        calls.append(tag)
        if tag == "loose-1":
            loose_in_fn.set()
            await asyncio.sleep(0.1)
        return tag

    async def body(ctx: StepContext) -> dict[str, object]:
        async def second_call() -> str:
            await asyncio.wait_for(first_in_fn.wait(), timeout=10)
            return await ctx.substep("x", fast)

        got = await asyncio.gather(ctx.substep("x", slow), second_call(), return_exceptions=True)
        replay = await ctx.substep("x", fast)

        async def second_loose() -> str:
            await asyncio.wait_for(loose_in_fn.wait(), timeout=10)
            return await ctx.substep("y", loose, "loose-2", idempotent=False)

        both = await asyncio.gather(
            ctx.substep("y", loose, "loose-1", idempotent=False), second_loose()
        )
        return {
            "gather": [type(v).__name__ if isinstance(v, BaseException) else str(v) for v in got],
            "replay": replay,
            "loose": list(both),
        }

    app = WorkflowApp()

    @app.workflow("substep_scope_same_body_concurrent")
    def wf() -> Promise[object]:
        return build(step(body, key="solo"))

    runner = FlowRunner(app.get("substep_scope_same_body_concurrent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {
        "gather": ["first", "DuplicateSubstepError"],
        "replay": "first",
        "loose": ["loose-1", "loose-2"],
    }, calls
    assert calls == ["first", "loose-1", "loose-2"], (
        f"the duplicate concurrent call (or the sequential re-call) ran its fn: {calls}"
    )
    rows = await _ledger_rows(wf_conn, wf_schema, flow_id)
    substep_rows = [r for r in rows if r["step_key"] != "solo"]
    assert [(r["status"], r["result"]) for r in substep_rows] == [("succeeded", '"first"')], (
        substep_rows
    )


async def test_loop_iterations_sharing_a_substep_name_run_per_iteration(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A loop iteration is its own execution unit (``_loop.py``: the
    iteration-scoped step keys ``(workflow, loop_key, iteration, step)``).
    Iteration 1's ``ctx.substep("tick")`` must run its own fn, never replay
    iteration 0's record."""
    calls: list[int] = []

    async def tick(i: int) -> int:
        calls.append(i)
        return i

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        got = await ctx.substep("tick", tick, i)
        if i >= 2:
            return Done({"last": got})
        return Refine(got + 1)

    app = WorkflowApp()

    @app.workflow("substep_scope_loop")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5))

    runner = FlowRunner(app.get("substep_scope_loop"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert calls == [0, 1, 2], f"a later iteration replayed an earlier iteration's substep: {calls}"
    assert await runner.result(flow_id) == {"last": 2}


async def test_separator_bearing_node_keys_and_names_never_collide(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The scoped identity is injective and disjoint from every node key,
    whatever characters keys and names carry. ``step(key=)`` refuses ``/``
    but ``loop(name)`` does not, and substep names have no grammar, so an
    encoding like ``f"{node_key}/{name}"`` would make step ``sa``'s substep
    ``x`` equal the loop key ``sa/x`` (its own claim rows), and step ``a``'s
    substep ``b/c`` equal loop ``a/b``'s substep ``c``. Green on HEAD (the
    bare names differ there); red under a node-key-prefix encoding."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def loop_done(ctx: StepContext, carry: object) -> object:
        return Done("loop-sa/x-result")

    async def loop_ab(ctx: StepContext, carry: object) -> object:
        return Done(await ctx.substep("c", ret, "a/b:c"))

    async def sa(ctx: StepContext, _u: object) -> str:
        return await ctx.substep("x", ret, "sa:x")

    async def a(ctx: StepContext, _ab: object) -> str:
        return await ctx.substep("b/c", ret, "a:b/c")

    async def pair(ctx: StepContext, x: str, y: str) -> dict[str, str]:
        return {"sa": x, "a": y}

    app = WorkflowApp()

    @app.workflow("substep_scope_separator")
    def wf() -> Promise[object]:
        left = step(sa, loop("sa/x", loop_done, max_iterations=2), key="sa")
        right = step(a, loop("a/b", loop_ab, max_iterations=2), key="a")
        return build(step(pair, left, right, key="out"))

    runner = FlowRunner(app.get("substep_scope_separator"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"sa": "sa:x", "a": "a:b/c"}
    assert sorted(calls) == ["a/b:c", "a:b/c", "sa:x"], calls


async def test_a_self_named_substep_replays_across_node_retries(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A substep named after its own node, in a node that fails twice after
    the substep and then succeeds: the substep ran once, every retry replays
    it. On HEAD the substep's claim lands ON the node's own claim row and
    re-stamps it, so the node's failure disposition is refused as stale
    (``workflow-attempt-stale-noop``) and the node never retries. The same pin
    guards the fix's legacy read: treating the node's own claim rows (same
    job, ``step_key`` == the node key) as a legacy substep record would make
    attempt 3's memo read attempt 2's FAILED node record and re-run the side
    effect."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def selfie(ctx: StepContext) -> str:
        got = await ctx.substep("selfie", ret, "v")
        if ctx.attempt < 3:
            raise RuntimeError("the transient after the substep")
        return got

    app = WorkflowApp()

    @app.workflow("substep_scope_self_retry")
    def wf() -> Promise[object]:
        return build(step(selfie, key="selfie", max_attempts=3))

    runner = FlowRunner(app.get("substep_scope_self_retry"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # bounded: on HEAD the substep's claim re-stamps the node's own claim row,
    # the node's failure disposition is refused as stale and the node wedges.
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert calls == ["v"], f"a retry re-ran the self-named substep: {calls}"
    assert await runner.result(flow_id) == "v"


# ── upgrade compatibility: rows the pre-fix code wrote under the bare name ──


async def _seed_bare_substep_row(
    conn: asyncpg.Connection,
    schema: str,
    *,
    flow_id: JobId,
    owner_key: str,
    name: str,
    value: str,
) -> None:
    """A row exactly as the pre-fix ``WorkflowSteps.step`` left it: the bare
    name as ``step_key``, the owning node's ``job_id``, a terminal
    ``succeeded`` raw result, attempt 1 / epoch 1 (an earlier claim)."""
    job_id = await conn.fetchval(
        f'SELECT id FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        owner_key,
    )
    assert job_id is not None, owner_key
    await conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status, result) "
        "VALUES ($1, $2, $3, $4, NULL, 1, 1, 'succeeded', to_jsonb($5::text))",
        new_uuid(),
        flow_id,
        job_id,
        name,
        value,
    )


@pytest.mark.parametrize("name", ["x", ""], ids=["named", "empty"])
async def test_a_legacy_bare_row_owned_by_this_node_replays_without_rerunning(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, name: str
) -> None:
    """In-flight upgrade: the pre-fix code already ran this node's substep
    and recorded it under the bare name (then the attempt died before the
    node's terminal). The post-fix retry must replay that record — never
    re-run the side effect — and say so: the memo names the bare row it
    read, and the replay logs ``workflow-substep-legacy-replay``. The empty
    name is a name too."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def solo(ctx: StepContext) -> str:
        return await ctx.substep(name, ret, "fresh")

    app = WorkflowApp()

    wf_name = f"substep_scope_legacy_own_{len(name)}"  # the registry is process-global

    @app.workflow(wf_name)
    def wf() -> Promise[object]:
        return build(step(solo, key="solo"))

    runner = FlowRunner(app.get(wf_name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await _seed_bare_substep_row(
        wf_conn, wf_schema, flow_id=flow_id, owner_key="solo", name=name, value="recorded"
    )
    job_id = (await _node_row(wf_conn, wf_schema, flow_id, "solo"))["id"]
    memo = await memoized_substep_result(
        wf_conn,
        runner.wsql,
        flow_id=flow_id,
        job_id=job_id,
        step_keys=[f"{job_id}/{name}", name],
        map_index=None,
    )
    assert memo is not None and memo.step_key == name, memo
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id) == "terminal"
    assert calls == [], f"the retry re-ran a substep the old code had recorded: {calls}"
    assert await runner.result(flow_id) == "recorded"
    replays = [e for e in logs if e["event"] == "workflow-substep-legacy-replay"]
    assert [(e["step_key"], e["job_id"]) for e in replays] == [(name, str(job_id))], logs


async def test_a_legacy_bare_row_owned_by_another_node_is_never_adopted(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The legacy read is owner-scoped too: a bare-name row another node's
    job wrote before the upgrade is that node's record, never this one's."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def other(ctx: StepContext) -> str:
        return "other-done"

    async def solo(ctx: StepContext, _o: str) -> str:
        return await ctx.substep("x", ret, "mine")

    app = WorkflowApp()

    @app.workflow("substep_scope_legacy_foreign")
    def wf() -> Promise[object]:
        return build(step(solo, step(other, key="other"), key="solo"))

    runner = FlowRunner(app.get("substep_scope_legacy_foreign"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await _seed_bare_substep_row(
        wf_conn, wf_schema, flow_id=flow_id, owner_key="other", name="x", value="FOREIGN"
    )
    assert await runner.drive(flow_id) == "terminal"
    got = await runner.result(flow_id)
    assert got == "mine", f"the substep adopted another node's legacy record: {got!r}"
    assert calls == ["mine"], calls


# ── the review pins: replay across drivers, holds and loop resumes; the
# deterministic duplicate; the ``<loop>.iter<n>`` name; the in-flight loop
# row; a real scoped row beside a node key ──


class Approval(BaseModel):
    verdict: str


_GATES = (GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),)


async def _node_row(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, key: str
) -> asyncpg.Record:
    row = await conn.fetchrow(
        f'SELECT id, status, attempt, claim_epoch, error_class, scheduled_at, actor, queue FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        key,
    )
    assert row is not None, key
    return row


async def _resolve_all_holds(client: HitlClient, flow_id: JobId) -> None:
    for hold in await client.list(run=flow_id):
        await client.resolve(hold.hold_id, {"verdict": "ok"})


async def test_a_substep_recorded_on_the_direct_driver_replays_on_the_fleet_door(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Attempt 1 runs on the in-process driver (``_run_node``), records the
    substep, then fails. The row is dispatch-claimed (``dispatch_batch``)
    and attempt 2 runs through the fleet door (``run_fleet_claimed_step``).
    Both doors must hand the context the same job id, so attempt 2 replays
    the record instead of re-running the side effect."""
    from datetime import timedelta

    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
    from taskq.backend._sweeps import sweep_scheduled_to_pending

    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def body(ctx: StepContext) -> str:
        got = await ctx.substep("x", ret, "v")
        if ctx.attempt < 2:
            raise RuntimeError("the transient after the substep")
        return got

    app = WorkflowApp()

    @app.workflow("substep_scope_cross_driver")
    def wf() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=3))

    direct = FlowRunner(app.get("substep_scope_cross_driver"), wf_pool, wf_schema)
    flow_id = (await direct.create_flow()).flow_id
    [row] = await direct._claimable(flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: one attempt on the direct door; drive() would run attempt 2 there too.
    await direct._run_node(flow_id, row)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert calls == ["v"], calls
    node = await _node_row(wf_conn, wf_schema, flow_id, "solo")
    assert (node["attempt"], node["status"]) == (1, "scheduled"), dict(node)
    # the ladder's backoff (50 ms on attempt 1) is due; the scheduler's own
    # promotion makes the row dispatchable.
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET scheduled_at = now() WHERE id = $1', node["id"]
    )
    assert await sweep_scheduled_to_pending(wf_conn, schema=wf_schema) == 1

    worker_id = JobId(new_uuid())
    fleet = FlowRunner(
        app.get("substep_scope_cross_driver"), wf_pool, wf_schema, worker_id=worker_id
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        node["actor"],
        node["queue"],
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'i743-cross-driver-pin', 1, $2::text[], $3::jsonb)",
        worker_id,
        [node["queue"]],
        '{"workflow_execution": true}',
    )
    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=[node["queue"]],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    [job] = [r for r in dispatched if str(r["id"]) == str(node["id"])]
    outcome = await fleet.run_fleet_claimed_step(
        flow_id,
        {
            "id": job["id"],
            "step_key": "solo",
            "map_index": None,
            "payload": job["payload"],
            "trace_id": job["trace_id"],
        },
        attempt=int(job["attempt"]),
        claim_epoch=int(job["claim_epoch"]),
    )
    assert outcome == "succeeded", outcome
    assert calls == ["v"], f"the fleet door re-ran the direct driver's substep: {calls}"
    assert await direct.drive(flow_id) == "terminal"
    assert await direct.result(flow_id) == "v"


async def test_a_substep_named_after_a_loop_iteration_key_runs_its_own_fn(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A loop job owns ``<loop>.iter<n>`` claim rows for every iteration so
    far. A body at iteration 1 calling ``ctx.substep("ticker.iter0")`` must
    run its own fn, never adopt iteration 0's claim row
    (``{"done": False, "feedback": 1}``) as a legacy substep record."""
    calls: list[int] = []

    async def note(i: int) -> str:
        calls.append(i)
        return f"note{i}"

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        if i >= 1:
            return Done({"got": await ctx.substep("ticker.iter0", note, i)})
        return Refine(i + 1)

    app = WorkflowApp()

    @app.workflow("substep_scope_iter_named")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5))

    runner = FlowRunner(app.get("substep_scope_iter_named"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert calls == [1], f"the substep adopted the loop's own iteration claim row: {calls}"
    assert await runner.result(flow_id) == {"got": "note1"}


async def test_a_substep_named_after_its_own_iteration_key_never_adopts_a_lapsed_claim(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The current iteration's own claim key: a lease-lapsed earlier claim
    of iteration 1 (the zombie beside its reclaimed winner) terminalizes
    its own ``ticker.iter1`` row while the winner's body runs. That row is
    no older than the iteration's first claim, so only the self-name skip
    keeps the winner's ``ctx.substep("ticker.iter1")`` from adopting the
    zombie's iteration record as a legacy substep record."""
    calls: list[int] = []

    async def note(i: int) -> str:
        calls.append(i)
        return f"note{i}"

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        if i == 0:
            return Refine(1)
        # the zombie's terminal on its own claim row of this iteration.
        await wf_pool.execute(
            f'INSERT INTO "{wf_schema}".wf_step_ledger '
            "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status, result) "
            "VALUES ($1, $2, $3, 'ticker.iter1', NULL, 1, 0, 'succeeded', "
            """'{"done": false, "feedback": 99}'::jsonb)""",
            new_uuid(),
            ctx.flow_id,
            ctx.job_id,
        )
        return Done({"got": await ctx.substep("ticker.iter1", note, i)})

    app = WorkflowApp()

    @app.workflow("substep_scope_own_iter_named")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5))

    runner = FlowRunner(app.get("substep_scope_own_iter_named"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert calls == [1], f"the substep adopted the iteration's own lapsed claim row: {calls}"
    assert await runner.result(flow_id) == {"got": "note1"}


async def test_a_substep_named_after_a_later_iteration_key_never_adopts_that_iterations_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A later iteration's key: a lease-lapsed driver still in iteration 1's
    body sees its reclaimed winner's terminal ``ticker.iter2`` row. That row
    is no older than iteration 1's first claim, so only the self-name skip
    (every iteration number, not just the current one) keeps
    ``ctx.substep("ticker.iter2")`` from adopting that iteration record."""
    calls: list[int] = []

    async def note(i: int) -> str:
        calls.append(i)
        return f"note{i}"

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        if i == 0:
            return Refine(1)
        # the reclaimed winner's terminal on iteration 2's claim row.
        await wf_pool.execute(
            f'INSERT INTO "{wf_schema}".wf_step_ledger '
            "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status, result) "
            "VALUES ($1, $2, $3, 'ticker.iter2', NULL, 1, 0, 'succeeded', "
            """'{"done": false, "feedback": 99}'::jsonb)""",
            new_uuid(),
            ctx.flow_id,
            ctx.job_id,
        )
        return Done({"got": await ctx.substep("ticker.iter2", note, i)})

    app = WorkflowApp()

    @app.workflow("substep_scope_later_iter_named")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5))

    runner = FlowRunner(app.get("substep_scope_later_iter_named"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert calls == [1], f"the substep adopted a later iteration's claim row: {calls}"
    assert await runner.result(flow_id) == {"got": "note1"}


@pytest.mark.parametrize(
    "name",
    ["ticker.iter²", "ticker.iter٣", "ticker.iter05"],
    ids=["superscript-digit", "arabic-indic-digit", "leading-zero"],
)
async def test_a_legacy_row_spelled_unlike_an_iteration_key_replays_in_its_iteration(
    name: str, wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The self-name skip matches only the driver's own spelling
    ``<loop>.iter<n>`` (ASCII digits, no leading zero). A pre-upgrade bare
    row whose name merely looks like one (``str.isdigit`` is true for
    ``²`` and ``٣``; the driver writes ``iter5``, never ``iter05``) is a
    substep record: the iteration that wrote it replays it across the
    upgrade and never re-runs fn."""
    calls: list[int] = []

    async def tick(i: int) -> int:
        calls.append(i)
        return i

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        got = await ctx.substep(name, tick, i)
        await ctx.wait_signal(Approval, timeout_s=120.0)
        return Done({"got": got})

    app = WorkflowApp()

    @app.workflow("substep_scope_iter_lookalike")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5, budget_s=600.0, gates=_GATES))

    runner = FlowRunner(app.get("substep_scope_iter_lookalike"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    client = HitlClient(wf_pool, schema=wf_schema)
    assert await runner.drive(flow_id, until="held") == "held"
    # the pre-fix shape of iteration 0's record: the bare name.
    renamed = await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_step_ledger SET step_key = $2 '
        "WHERE flow_id = $1 AND step_key = job_id::text || '.iter0/' || $2",
        flow_id,
        name,
    )
    assert renamed == "UPDATE 1", renamed
    await _resolve_all_holds(client, flow_id)
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert calls == [0], f"the pre-upgrade bare row {name!r} was not replayed: {calls}"
    assert await runner.result(flow_id) == {"got": 0}


async def test_concurrent_same_name_substeps_fail_the_node_on_its_first_attempt(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A plain ``gather`` of two same-name substeps (no
    ``return_exceptions``) is an authoring defect that every retry repeats:
    the node fails on attempt 1 with ``DuplicateSubstepError`` on the
    record, the ladder is not spent (the body's un-ledgered prefix ran
    once), and the duplicate's fn never ran. The first call is not
    cancelled by the gather; its record still lands."""
    prefix: list[int] = []
    fns: list[str] = []

    async def slow() -> str:
        fns.append("slow")
        await asyncio.sleep(0.05)
        return "slow"

    async def fast() -> str:
        fns.append("fast")
        return "fast"

    async def body(ctx: StepContext) -> list[str]:
        prefix.append(ctx.attempt)
        return list(await asyncio.gather(ctx.substep("x", slow), ctx.substep("x", fast)))

    app = WorkflowApp()

    @app.workflow("substep_scope_duplicate_ladder")
    def wf() -> Promise[object]:
        return build(step(body, key="dup", max_attempts=3))

    runner = FlowRunner(app.get("substep_scope_duplicate_ladder"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    # the orphaned first call finishes in the background; wait for its record.
    substep_rows: list[dict[str, Any]] = []
    for _ in range(100):
        substep_rows = [
            r
            for r in await _ledger_rows(wf_conn, wf_schema, flow_id)
            if r["step_key"] != "dup" and r["status"] == "succeeded"
        ]
        if substep_rows:
            break
        await asyncio.sleep(0.05)
    node = await _node_row(wf_conn, wf_schema, flow_id, "dup")
    assert (node["status"], node["attempt"], node["error_class"]) == (
        "failed",
        1,
        "DuplicateSubstepError",
    ), dict(node)
    assert prefix == [1], f"the duplicate spent the retry ladder: {prefix}"
    assert fns == ["slow"], fns
    assert [r["result"] for r in substep_rows] == ['"slow"'], substep_rows


async def test_a_duplicate_substep_in_a_loop_body_exhausts_the_loop_naming_the_class(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Inside a loop body the duplicate refuses the same way, but the loop
    driver records every body exception as its own body failure: the loop
    exhausts on attempt 1 with ``LoopBodyFailure``, and the message names
    ``DuplicateSubstepError``. The duplicate's fn never ran."""
    fns: list[str] = []

    async def slow() -> str:
        fns.append("slow")
        await asyncio.sleep(0.05)
        return "slow"

    async def fast() -> str:
        fns.append("fast")
        return "fast"

    orphans: list[asyncio.Task[Any]] = []

    async def body(ctx: StepContext, carry: object) -> object:
        # the gather leaves the first call running; keep its task so the test can await it
        first = asyncio.create_task(ctx.substep("x", slow))
        orphans.append(first)
        return Done(list(await asyncio.gather(first, ctx.substep("x", fast))))

    app = WorkflowApp()

    @app.workflow("substep_scope_loop_duplicate")
    def wf() -> Promise[object]:
        return build(loop("dupl", body, max_iterations=3))

    runner = FlowRunner(app.get("substep_scope_loop_duplicate"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    node = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'dupl'",
        flow_id,
    )
    assert node is not None
    assert (node["status"], node["attempt"], node["error_class"]) == (
        "failed",
        1,
        "LoopBodyFailure",
    ), dict(node)
    assert "DuplicateSubstepError" in node["error_message"], dict(node)
    # the orphaned first call finishes in the background; await the task itself
    # (a row-status poll can break before its pool release ends), then assert
    # its record, so a lost terminal write fails here (#743 W-10).
    (orphan,) = orphans
    await asyncio.wait_for(orphan, 5)
    substep_rows = [
        r
        for r in await _ledger_rows(wf_conn, wf_schema, flow_id)
        if r["step_key"].endswith(".iter0/x")
    ]
    assert fns == ["slow"], fns
    assert [(r["status"], r["result"]) for r in substep_rows] == [("succeeded", '"slow"')], (
        substep_rows
    )


async def test_a_substep_replays_across_a_hold_resume(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A step records a substep, then waits on a hold. The resume re-runs
    the body under a new claim epoch; the substep replays."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def body(ctx: StepContext) -> str:
        got = await ctx.substep("x", ret, "v")
        await ctx.wait_signal(Approval, timeout_s=120.0)
        return got

    app = WorkflowApp()

    @app.workflow("substep_scope_hold_resume")
    def wf() -> Promise[object]:
        return build(step(body, key="held", gates=_GATES))

    runner = FlowRunner(app.get("substep_scope_hold_resume"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held") == "held"
    await _resolve_all_holds(HitlClient(wf_pool, schema=wf_schema), flow_id)
    assert await runner.drive(flow_id) == "terminal"
    assert calls == ["v"], f"the hold resume re-ran the substep: {calls}"
    assert await runner.result(flow_id) == "v"


async def _drive_loop_through_holds(
    runner: FlowRunner, client: HitlClient, flow_id: JobId, *, rounds: int
) -> list[str]:
    verdicts: list[str] = []
    for _ in range(rounds):
        verdict = await runner.drive(flow_id, until="held", max_ticks=500)
        verdicts.append(verdict)
        if verdict != "held":
            break
        await _resolve_all_holds(client, flow_id)
    return verdicts


async def test_a_loop_iteration_replays_its_substep_across_a_hold_resume(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Each iteration records ``ctx.substep("tick", i)`` and waits once
    (E13). The resume of iteration n replays iteration n's record, and
    every iteration runs its own ``tick`` exactly once."""
    calls: list[int] = []

    async def tick(i: int) -> int:
        calls.append(i)
        return i

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        got = await ctx.substep("tick", tick, i)
        await ctx.wait_signal(Approval, timeout_s=120.0)
        if i >= 2:
            return Done({"last": got})
        return Refine(got + 1)

    app = WorkflowApp()

    @app.workflow("substep_scope_loop_hold")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5, budget_s=600.0, gates=_GATES))

    runner = FlowRunner(app.get("substep_scope_loop_hold"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    client = HitlClient(wf_pool, schema=wf_schema)
    verdicts = await _drive_loop_through_holds(runner, client, flow_id, rounds=6)
    assert calls == [0, 1, 2], f"an iteration's resume re-ran (or replayed) the wrong tick: {calls}"
    assert verdicts == ["held", "held", "held", "terminal"], verdicts
    assert await runner.result(flow_id) == {"last": 2}


async def test_a_loop_held_mid_iteration_across_the_upgrade_replays_only_that_iteration(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """In-flight upgrade, loop shape: iteration 0 recorded ``tick`` under
    the pre-fix bare name, then held. The resume replays that record for
    iteration 0 (no double side effect); iterations 1 and 2 never adopt it
    and run their own ``tick``, so the loop advances and completes."""
    calls: list[int] = []

    async def tick(i: int) -> int:
        calls.append(i)
        return i

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        got = await ctx.substep("tick", tick, i)
        await ctx.wait_signal(Approval, timeout_s=120.0)
        if got >= 2:
            return Done({"last": got})
        return Refine(got + 1)

    app = WorkflowApp()

    @app.workflow("substep_scope_loop_legacy")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5, budget_s=600.0, gates=_GATES))

    runner = FlowRunner(app.get("substep_scope_loop_legacy"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    client = HitlClient(wf_pool, schema=wf_schema)
    assert await runner.drive(flow_id, until="held") == "held"
    # the pre-fix shape of iteration 0's record: the bare name.
    renamed = await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_step_ledger SET step_key = $2 '
        "WHERE flow_id = $1 AND step_key LIKE '%/tick'",
        flow_id,
        "tick",
    )
    assert renamed == "UPDATE 1", renamed
    await _resolve_all_holds(client, flow_id)
    verdicts = await _drive_loop_through_holds(runner, client, flow_id, rounds=6)
    assert verdicts[-1] == "terminal", verdicts
    assert calls == [0, 1, 2], f"the legacy row was re-run or adopted by a later iteration: {calls}"
    assert await runner.result(flow_id) == {"last": 2}


async def test_a_failing_self_named_substep_never_counts_against_the_node_ladder(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A real scoped substep row beside its node's key: node ``n``
    (``max_attempts=3``) catches a failing ``ctx.substep("n", boom)`` on
    every attempt, fails attempts 1 and 2 itself, and succeeds on 3. The
    ladder counts the node's own failed rows only (``_FAILED_COUNT_SQL``
    selects by the node key), so the substep's failed rows never spend the
    node's budget."""
    booms: list[int] = []

    async def boom(attempt: int) -> str:
        booms.append(attempt)
        raise ValueError("the substep's own failure")

    async def n(ctx: StepContext) -> str:
        with contextlib.suppress(ValueError):
            await ctx.substep("n", boom, ctx.attempt)
        if ctx.attempt < 3:
            raise RuntimeError("the node's own transient")
        return "done"

    app = WorkflowApp()

    @app.workflow("substep_scope_ladder_count")
    def wf() -> Promise[object]:
        return build(step(n, key="n", max_attempts=3))

    runner = FlowRunner(app.get("substep_scope_ladder_count"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    node = await _node_row(wf_conn, wf_schema, flow_id, "n")
    assert (node["status"], node["attempt"]) == ("succeeded", 3), dict(node)
    assert booms == [1, 2, 3], booms
    assert await runner.result(flow_id) == "done"
    rows = await _ledger_rows(wf_conn, wf_schema, flow_id)
    assert sorted(r["status"] for r in rows if r["step_key"] == "n") == [
        "failed",
        "failed",
        "succeeded",
    ], rows


async def test_a_map_child_past_smallint_reads_its_substep_memo(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """#717 widened ``map_index`` to bigint. The substep memo binds it as
    bigint too, so a map child at 32768 reads its own record instead of
    failing at bind (``value out of int16 range``)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    job_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="m")
    key = f"{job_id}/x"
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status, result) "
        "VALUES ($1, $2, $3, $4, 32768, 1, 1, 'succeeded', to_jsonb('v'::text))",
        new_uuid(),
        flow_id,
        job_id,
        key,
    )
    memo = await memoized_substep_result(
        wf_conn,
        render_workflow_sql(wf_schema),
        flow_id=flow_id,
        job_id=job_id,
        step_keys=[key, "x"],
        map_index=32768,
    )
    assert memo is not None and (memo.step_key, memo.result) == (key, "v"), memo
