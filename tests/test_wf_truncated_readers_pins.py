"""Integration pins for readers of truncated bounded-collect workflow joins.

When a ``map_join`` or ``gather`` node collects more than the 64 KiB
``FANIN_FAILURES_BYTE_CAP``, ``_bounded_collect`` writes a bounded envelope
(``value`` prefix + ``__truncated__`` count + ``detail_home``) while the full
elements remain on the parent rows. Every join-row reader must detect
``result_truncation`` and rebuild the complete list from parent rows (flattening
for ``gather`` and failing closed if parent detail rows are missing), rather
than silently handing callers the 64 KiB prefix.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel, RootModel

from taskq.backend._protocol import JobId
from taskq.backend.postgres import PostgresBackend
from taskq.workflows import (
    ChildCancelled,
    ChildFailed,
    ChildHandle,
    ChildOk,
    Expired,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    gather,
    map_source,
    step,
)
from taskq.workflows._childflow import JoinSite
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.api._runner_errors import WorkflowResultDetailUnavailableError
from taskq.workflows.api._sql_runner import render_sql

# Each serialized item is ~530 bytes: 100 items (~53 KiB) fit inside a single
# parent step row (< 64 KiB), while 200 items (~106 KiB) exceed the 64 KiB
# bounded-collect cap so ``_bounded_collect`` writes a truncated prefix.
_ITEM_PAD = "p" * 500
_TOTAL_ITEMS = 200
_HALF_ITEMS = 100


class _RangeIn(BaseModel):
    start: int = 0
    count: int = _TOTAL_ITEMS


class _FatItem(BaseModel):
    idx: int
    pad: str


class _FatItemList(RootModel[list[_FatItem]]):
    """Typed contract for a child workflow whose terminal node is a list join."""


class _FatBatches(RootModel[list[list[_FatItem]]]):
    """Typed contract for a ``map_join`` over list-returning items — the
    join's value is ``list[list[T]]`` and must NEVER be flattened."""


class _MixedGatherResult(RootModel[list[object]]):
    """A mixed-shape gather keeps its list parent as one nested value."""


class _ParentSummary(BaseModel):
    count: int
    indices: list[int]
    verdict: str


class _BatchSummary(BaseModel):
    """The parent's view of a ``list[list[T]]`` child join: the number of
    BATCH values returned and each batch's own length."""

    batches: int
    sizes: list[int]
    verdict: str


class _ConsumeOut(BaseModel):
    count: int
    indices: list[int]


async def _emit_indices(ctx: StepContext, params: _RangeIn) -> list[int]:
    return list(range(params.start, params.start + params.count))


async def _expand_item(ctx: StepContext, idx: int) -> _FatItem:
    return _FatItem(idx=idx, pad=_ITEM_PAD)


async def _expand_pair(ctx: StepContext, idx: int) -> list[_FatItem]:
    """One map item returning a LIST — a ``map_join`` over this body packs
    ``list[list[_FatItem]]`` (400 items inside 200 lists)."""
    return [_FatItem(idx=idx * 2, pad=_ITEM_PAD), _FatItem(idx=idx * 2 + 1, pad=_ITEM_PAD)]


async def _expand_item_or_fail(ctx: StepContext, idx: int) -> _FatItem:
    if idx == _TOTAL_ITEMS - 1:
        raise RuntimeError("absorbed map-item failure")
    return await _expand_item(ctx, idx)


async def _emit_half(ctx: StepContext, params: _RangeIn) -> list[_FatItem]:
    return [
        _FatItem(idx=i, pad=_ITEM_PAD) for i in range(params.start, params.start + params.count)
    ]


async def _consume_items(ctx: StepContext, items: list[_FatItem]) -> _ConsumeOut:
    return _ConsumeOut(count=len(items), indices=[item.idx for item in items])


async def _assert_row_truncated(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    flow_id: JobId,
    step_key: str,
    total_expected: int,
) -> int:
    """Confirm the join row on disk actually triggered bounded-collect truncation."""
    sql = render_sql(
        "SELECT result FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        wf_schema,
    )
    async with wf_pool.acquire() as conn:
        raw = await conn.fetchval(sql, flow_id, step_key)
    assert raw is not None, f"join row {step_key!r} has no result"
    doc: Any = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(doc, dict) and "__truncated__" in doc, (
        f"test precondition failed: {step_key!r} was not truncated ({type(doc)})"
    )
    kept = len(doc["value"])
    truncated = int(doc["__truncated__"])
    assert truncated > 0 and kept + truncated == total_expected, (
        f"unexpected truncation accounting for {step_key!r}: kept={kept}, truncated={truncated}"
    )
    return kept


async def _step_row(
    wf_pool: asyncpg.Pool, wf_schema: str, flow_id: JobId, step_key: str
) -> dict[str, Any] | None:
    """One node row's status/attempt/error face (the terminal-failure record)."""
    sql = render_sql(
        "SELECT status, attempt, max_attempts, error_class FROM {schema}.jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        wf_schema,
    )
    async with wf_pool.acquire() as conn:
        row = await conn.fetchrow(sql, flow_id, step_key)
    return None if row is None else dict(row)


async def _delete_item_rows(
    wf_pool: asyncpg.Pool, wf_schema: str, flow_id: JobId, source_key: str
) -> int:
    """Purge a map source's child rows — the ONLY home of a truncated join's
    full collect (the ``detail_home`` contract), as retention would."""
    sql = render_sql(
        "DELETE FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND (step_key = $2 OR step_key LIKE $3)",
        wf_schema,
    )
    async with wf_pool.acquire() as conn:
        status = await conn.execute(sql, flow_id, f"{source_key}.item", f"{source_key}.item:%")
    return int(status.split()[-1])


@pytest.mark.integration
async def test_child_ok_result_rebuilds_truncated_map_join(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """ChildHandle._ok_result must rebuild all items of a truncated map_join terminal."""
    child_app = WorkflowApp()

    @child_app.workflow("tr_child_map_join")
    def child_wf() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item))

    child_compiled = child_app.get("tr_child_map_join")

    parent_app = WorkflowApp()
    child_flow_ids: list[JobId] = []

    @parent_app.actor(queue="default", max_attempts=1)
    async def spawn_and_join(ctx: StepContext) -> _ParentSummary:
        handle = await ctx.spawn_flow("tr_child_map_join", result=_FatItemList)
        child_flow_ids.append(handle.flow_id)
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=items):
                return _ParentSummary(
                    count=len(items.root),
                    indices=[item.idx for item in items.root],
                    verdict="ok",
                )
            case ChildFailed():
                return _ParentSummary(count=0, indices=[], verdict="failed")
            case ChildCancelled():
                return _ParentSummary(count=0, indices=[], verdict="cancelled")
            case Expired():
                return _ParentSummary(count=0, indices=[], verdict="expired")
        raise AssertionError("unreachable")  # pragma: no cover

    @parent_app.workflow("tr_parent_map_join")
    def parent_wf() -> Promise[object]:
        return build(step(spawn_and_join, key="spawn_join"))

    runner = FlowRunner(parent_app.get("tr_parent_map_join"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal", outcome
    parent_res = await runner.result(claim.flow_id)
    assert child_flow_ids, f"parent never spawned child flow (result={parent_res!r})"
    child_flow_id = child_flow_ids[-1]

    kept = await _assert_row_truncated(wf_pool, wf_schema, child_flow_id, "src.join", _TOTAL_ITEMS)

    # Child FlowRunner.result() already rebuilds the full list.
    child_runner = FlowRunner(child_compiled, wf_pool, wf_schema)
    child_full = await child_runner.result(child_flow_id)
    assert isinstance(child_full, list) and len(child_full) == _TOTAL_ITEMS

    # Parent's ChildOk.result must match the full collect, not the 64 KiB prefix.
    assert isinstance(parent_res, dict)
    assert parent_res["verdict"] == "ok", parent_res
    assert parent_res["count"] == _TOTAL_ITEMS, (
        f"ChildOk.result saw only {parent_res['count']} items (prefix={kept}) "
        f"instead of all {_TOTAL_ITEMS}"
    )
    assert parent_res["indices"] == list(range(_TOTAL_ITEMS))


@pytest.mark.integration
async def test_child_ok_result_rebuilds_truncated_gather(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """ChildHandle._ok_result must rebuild and flatten a truncated gather terminal."""
    child_app = WorkflowApp()

    @child_app.workflow("tr_child_gather")
    def child_wf() -> Promise[object]:
        half_a = step(_emit_half, _RangeIn(start=0, count=_HALF_ITEMS), key="half_a")
        half_b = step(_emit_half, _RangeIn(start=_HALF_ITEMS, count=_HALF_ITEMS), key="half_b")
        return build(gather([half_a, half_b]))

    child_compiled = child_app.get("tr_child_gather")
    assert child_compiled.terminal == "gather"

    parent_app = WorkflowApp()
    child_flow_ids: list[JobId] = []

    @parent_app.actor(queue="default", max_attempts=1)
    async def spawn_and_join(ctx: StepContext) -> _ParentSummary:
        handle = await ctx.spawn_flow("tr_child_gather", result=_FatItemList)
        child_flow_ids.append(handle.flow_id)
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=items):
                return _ParentSummary(
                    count=len(items.root),
                    indices=[item.idx for item in items.root],
                    verdict="ok",
                )
            case ChildFailed():
                return _ParentSummary(count=0, indices=[], verdict="failed")
            case ChildCancelled():
                return _ParentSummary(count=0, indices=[], verdict="cancelled")
            case Expired():
                return _ParentSummary(count=0, indices=[], verdict="expired")
        raise AssertionError("unreachable")  # pragma: no cover

    @parent_app.workflow("tr_parent_gather")
    def parent_wf() -> Promise[object]:
        return build(step(spawn_and_join, key="spawn_join"))

    runner = FlowRunner(parent_app.get("tr_parent_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal", outcome
    parent_res = await runner.result(claim.flow_id)
    assert child_flow_ids, f"parent never spawned child flow (result={parent_res!r})"
    child_flow_id = child_flow_ids[-1]

    kept = await _assert_row_truncated(wf_pool, wf_schema, child_flow_id, "gather", _TOTAL_ITEMS)

    assert isinstance(parent_res, dict)
    assert parent_res["verdict"] == "ok", parent_res
    assert parent_res["count"] == _TOTAL_ITEMS, (
        f"ChildOk.result for truncated gather saw only {parent_res['count']} items "
        f"(prefix={kept}) instead of all {_TOTAL_ITEMS}"
    )
    assert parent_res["indices"] == list(range(_TOTAL_ITEMS))

    # Also verify standalone ChildHandle outside any FlowRunner resolves gather flattening.
    standalone = ChildHandle[_FatItemList](
        site=JoinSite(
            pool=wf_pool,
            wsql=WorkflowSql.build(wf_schema),
            flow_id=claim.flow_id,
            job_id=claim.flow_id,
            node_key="standalone",
            attempt=1,
            ledger_id=None,
        ),
        flow_id=child_flow_id,
        result_model=_FatItemList,
        terminal_key="gather",
    )
    standalone_outcome = await standalone.wait()
    assert isinstance(standalone_outcome, ChildOk)
    assert [item.idx for item in standalone_outcome.result.root] == list(range(_TOTAL_ITEMS))


@pytest.mark.integration
async def test_skip_predicate_flow_state_rebuilds_truncated_map_join(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """FlowRunner._flow_state must expose full items of a truncated map_join to skip= predicates."""
    app = WorkflowApp()
    seen_in_skip: list[list[dict[str, Any]]] = []

    def skip_if_incomplete(state: dict[str, object]) -> bool:
        results = state["results"]
        assert isinstance(results, dict)
        joined = results["src.join"]
        assert isinstance(joined, list)
        seen_in_skip.append(list(joined))
        # Skip if fewer than all expected items are visible (triggers when truncated).
        return len(joined) < _TOTAL_ITEMS

    @app.workflow("tr_skip_map_join")
    def wf_def() -> Promise[object]:
        items = map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item)
        return build(step(_consume_items, items, key="consume", skip=skip_if_incomplete))

    runner = FlowRunner(app.get("tr_skip_map_join"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal", outcome

    kept = await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)

    assert len(seen_in_skip) == 1
    assert len(seen_in_skip[0]) == _TOTAL_ITEMS, (
        f"skip predicate saw only {len(seen_in_skip[0])} items (prefix={kept}) "
        f"instead of all {_TOTAL_ITEMS}"
    )
    assert [item["idx"] for item in seen_in_skip[0]] == list(range(_TOTAL_ITEMS))

    res = await runner.result(claim.flow_id)
    assert isinstance(res, dict) and res.get("skipped") is not True, (
        f"consume step was wrongly skipped because _flow_state saw truncated prefix: {res!r}"
    )
    assert res["count"] == _TOTAL_ITEMS
    assert res["indices"] == list(range(_TOTAL_ITEMS))


@pytest.mark.integration
async def test_skip_predicate_flow_state_rebuilds_truncated_gather(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """FlowRunner._flow_state must expose flattened full items of a truncated gather to skip=."""
    app = WorkflowApp()
    seen_in_skip: list[list[dict[str, Any]]] = []

    def skip_if_incomplete(state: dict[str, object]) -> bool:
        results = state["results"]
        assert isinstance(results, dict)
        gathered = results["gather"]
        assert isinstance(gathered, list)
        seen_in_skip.append(list(gathered))
        return len(gathered) < _TOTAL_ITEMS

    @app.workflow("tr_skip_gather")
    def wf_def() -> Promise[object]:
        half_a = step(_emit_half, _RangeIn(start=0, count=_HALF_ITEMS), key="half_a")
        half_b = step(_emit_half, _RangeIn(start=_HALF_ITEMS, count=_HALF_ITEMS), key="half_b")
        g = gather([half_a, half_b])
        return build(step(_consume_items, g, key="consume", skip=skip_if_incomplete))

    runner = FlowRunner(app.get("tr_skip_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal", outcome

    kept = await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "gather", _TOTAL_ITEMS)

    assert len(seen_in_skip) == 1
    assert len(seen_in_skip[0]) == _TOTAL_ITEMS, (
        f"skip predicate saw only {len(seen_in_skip[0])} items (prefix={kept}) "
        f"instead of all {_TOTAL_ITEMS}"
    )
    assert [item["idx"] for item in seen_in_skip[0]] == list(range(_TOTAL_ITEMS))

    res = await runner.result(claim.flow_id)
    assert isinstance(res, dict) and res.get("skipped") is not True
    assert res["count"] == _TOTAL_ITEMS
    assert res["indices"] == list(range(_TOTAL_ITEMS))


@pytest.mark.integration
async def test_truncated_join_missing_detail_rows_fails_closed(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """Both ChildHandle._ok_result and FlowRunner._flow_state/result must fail closed
    when parent detail rows of a truncated join are missing rather than silently
    returning a prefix or empty list."""
    app = WorkflowApp()

    @app.workflow("tr_missing_detail_map")
    def wf_def() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item))

    runner = FlowRunner(app.get("tr_missing_detail_map"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal", outcome

    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)

    # A torn purge must not return the surviving 199 rows as if they were the
    # declared 200-item collect. The all-rows-gone case below must not be the
    # only shape this fail-closed contract pins.
    async with wf_pool.acquire() as conn:
        deleted = await conn.execute(
            render_sql(
                "DELETE FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.item' AND map_index = 0",
                wf_schema,
            ),
            claim.flow_id,
        )
    assert deleted == "DELETE 1"
    with pytest.raises(
        WorkflowResultDetailUnavailableError,
        match=r"node 'src\.join'.*expected 200 items, rebuilt 199",
    ):
        await runner.result(claim.flow_id)

    # Simulate purged/missing parent detail rows (remove parent .item jobs & edges).
    delete_edges_sql = render_sql(
        "DELETE FROM {schema}.wf_edge WHERE flow_id = $1",
        wf_schema,
    )
    delete_items_sql = render_sql(
        "DELETE FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'src.item'",
        wf_schema,
    )
    async with wf_pool.acquire() as conn:
        await conn.execute(delete_edges_sql, claim.flow_id)
        await conn.execute(delete_items_sql, claim.flow_id)

    # 1) ChildHandle._ok_result must fail loudly, never return a truncated ChildOk.
    handle = ChildHandle[_FatItemList](
        site=JoinSite(
            pool=wf_pool,
            wsql=WorkflowSql.build(wf_schema),
            flow_id=claim.flow_id,
            job_id=claim.flow_id,
            node_key="parent_node",
            attempt=1,
            ledger_id=None,
        ),
        flow_id=claim.flow_id,
        result_model=_FatItemList,
        terminal_key="src.join",
    )
    with pytest.raises(
        WorkflowResultDetailUnavailableError,
        match="expected 200 items, rebuilt 0",
    ):
        await handle.wait()

    # 2) FlowRunner._flow_state must fail loudly, never return a truncated prefix.
    with pytest.raises(
        WorkflowResultDetailUnavailableError,
        match=r"node 'src\.join'.*expected 200 items, rebuilt 0",
    ):
        await runner._flow_state(claim.flow_id)

    # 3) FlowRunner.result must also fail loudly, never return [].
    with pytest.raises(
        WorkflowResultDetailUnavailableError,
        match="handing back a truncated prefix",
    ):
        await runner.result(claim.flow_id)

    # The root still says succeeded after the terminal row itself is purged:
    # ChildHandle must refuse that absent source rather than validating None.
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "DELETE FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.join'",
                wf_schema,
            ),
            claim.flow_id,
        )
    with pytest.raises(
        WorkflowResultDetailUnavailableError,
        match="terminal row is missing",
    ):
        await handle.wait()


@pytest.mark.integration
async def test_nested_truncated_gather_over_truncated_map_join_rebuilds_all_items(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """When a truncated gather wraps a truncated map_join, detail re-read must recurse
    into the truncated parent join rather than decoding only its 64 KiB prefix."""
    app = WorkflowApp()

    @app.workflow("tr_nested_gather_map")
    def wf_def() -> Promise[object]:
        big_map = map_source(
            step(_emit_indices, _RangeIn(start=0, count=_TOTAL_ITEMS), key="src_big"),
            _expand_item,
        )
        tail = step(_emit_half, _RangeIn(start=_TOTAL_ITEMS, count=20), key="tail")
        return build(gather([big_map, tail]))

    runner = FlowRunner(app.get("tr_nested_gather_map"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal", outcome

    expected_total = _TOTAL_ITEMS + 20
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src_big.join", _TOTAL_ITEMS)
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "gather", expected_total)

    full = await runner.result(claim.flow_id)
    assert isinstance(full, list)
    assert len(full) == expected_total, (
        f"nested truncated gather rebuilt only {len(full)} items instead of {expected_total}"
    )
    # Static node `tail` is created before fork-spawned `src_big.join`, so the
    # ordered parent read puts its 20 items first. Rebuilding must preserve
    # that order at both recursion depths, not merely preserve membership.
    expected_indices = [
        *range(_TOTAL_ITEMS, expected_total),
        *range(_TOTAL_ITEMS),
    ]
    assert [item["idx"] for item in full] == expected_indices

    # The runner-less face has no compiled-graph kind fallback: recursive
    # metadata must be sufficient at both the gather and map-join depths.
    handle = ChildHandle[_FatItemList](
        site=JoinSite(
            pool=wf_pool,
            wsql=WorkflowSql.build(wf_schema),
            flow_id=claim.flow_id,
            job_id=claim.flow_id,
            node_key="standalone",
            attempt=1,
            ledger_id=None,
        ),
        flow_id=claim.flow_id,
        result_model=_FatItemList,
        terminal_key="gather",
    )
    outcome = await handle.wait()
    assert isinstance(outcome, ChildOk)
    assert [item.idx for item in outcome.result.root] == expected_indices


@pytest.mark.integration
async def test_truncated_map_join_over_list_items_is_never_flattened(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A ``map_join`` over list-returning items packs ``list[list[T]]``: the
    detail re-read must NOT flatten it (the gather-only rule), at any reader —
    ``ChildHandle._ok_result``, ``FlowRunner.result()``, ``_flow_state()``.

    This is what makes the durable ``wf_node_kind`` stamp load-bearing: a
    runner-less reader has no ``CompiledWorkflow`` to ask, and a naive
    ``all(isinstance(v, list) for v in parents)`` heuristic would flatten here
    and hand back 400 items where the declared contract is 200 batches.
    """
    child_app = WorkflowApp()

    @child_app.workflow("tr_child_map_join_of_lists")
    def child_wf() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_pair))

    child_compiled = child_app.get("tr_child_map_join_of_lists")
    assert child_compiled.terminal == "src.join"

    parent_app = WorkflowApp()
    child_flow_ids: list[JobId] = []

    @parent_app.actor(queue="default", max_attempts=1)
    async def spawn_and_join(ctx: StepContext) -> _BatchSummary:
        handle = await ctx.spawn_flow("tr_child_map_join_of_lists", result=_FatBatches)
        child_flow_ids.append(handle.flow_id)
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=batches):
                return _BatchSummary(
                    batches=len(batches.root),
                    sizes=[len(batch) for batch in batches.root],
                    verdict="ok",
                )
            case ChildFailed():
                return _BatchSummary(batches=0, sizes=[], verdict="failed")
            case ChildCancelled():
                return _BatchSummary(batches=0, sizes=[], verdict="cancelled")
            case Expired():
                return _BatchSummary(batches=0, sizes=[], verdict="expired")
        raise AssertionError("unreachable")  # pragma: no cover

    @parent_app.workflow("tr_parent_map_join_of_lists")
    def parent_wf() -> Promise[object]:
        return build(step(spawn_and_join, key="spawn_join"))

    runner = FlowRunner(parent_app.get("tr_parent_map_join_of_lists"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal", outcome
    parent_res = await runner.result(claim.flow_id)
    assert child_flow_ids, f"parent never spawned child flow (result={parent_res!r})"
    child_flow_id = child_flow_ids[-1]

    kept = await _assert_row_truncated(wf_pool, wf_schema, child_flow_id, "src.join", _TOTAL_ITEMS)

    # 1) the runner-less ChildHandle read: 200 batches of 2, never 400 items.
    assert isinstance(parent_res, dict) and parent_res["verdict"] == "ok", parent_res
    assert parent_res["batches"] == _TOTAL_ITEMS, (
        f"ChildOk.result saw {parent_res['batches']} values (prefix={kept}) where the "
        f"map_join packs {_TOTAL_ITEMS} batches — a list join was flattened"
    )
    assert parent_res["sizes"] == [2] * _TOTAL_ITEMS

    # 2) FlowRunner.result() on the child itself.
    child_runner = FlowRunner(child_compiled, wf_pool, wf_schema)
    child_full = await child_runner.result(child_flow_id)
    assert isinstance(child_full, list)
    assert len(child_full) == _TOTAL_ITEMS, (
        f"result() rebuilt {len(child_full)} values instead of {_TOTAL_ITEMS} batches"
    )
    assert all(isinstance(batch, list) and len(batch) == 2 for batch in child_full)

    # 3) the skip-predicate's view of the same row.
    state = await child_runner._flow_state(child_flow_id)
    results = state["results"]
    assert isinstance(results, dict)
    entry = results["src.join"]
    assert isinstance(entry, list) and len(entry) == _TOTAL_ITEMS, (
        f"_flow_state rebuilt {len(entry) if isinstance(entry, list) else entry!r} "
        f"values instead of {_TOTAL_ITEMS} batches"
    )
    assert all(isinstance(batch, list) and len(batch) == 2 for batch in entry)


@pytest.mark.integration
async def test_skip_gate_missing_detail_rows_fails_deterministically(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A ``skip=`` predicate whose flow state holds an unrebuildable truncated
    join terminal-fails the node deterministically on attempt 1 — it does not
    strand the claimed node ``running``, burn the retry budget, or crash
    ``drive()``."""
    app = WorkflowApp()

    def skip_if_incomplete(state: dict[str, object]) -> bool:
        results = state["results"]
        assert isinstance(results, dict)
        joined = results["src.join"]
        assert isinstance(joined, list)
        return len(joined) < _TOTAL_ITEMS

    @app.workflow("tr_skip_missing_detail")
    def wf_def() -> Promise[object]:
        items = map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item)
        return build(step(_consume_items, items, key="consume", skip=skip_if_incomplete))

    runner = FlowRunner(app.get("tr_skip_missing_detail"), wf_pool, wf_schema)
    claim = await runner.create_flow()

    # Advance tick by tick (each tick claims from a snapshot taken before it
    # runs) until the join has collected — the downstream ``consume`` node is
    # claimable but not yet claimed.
    for _ in range(300):
        join_row = await _step_row(wf_pool, wf_schema, claim.flow_id, "src.join")
        if join_row is not None and join_row["status"] == "succeeded":
            break
        await runner.tick(claim.flow_id)
    else:  # pragma: no cover - a broken fork/join fires here
        pytest.fail("the map join never succeeded within 300 ticks")
    assert join_row is not None
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)
    consume_row = await _step_row(wf_pool, wf_schema, claim.flow_id, "consume")
    assert consume_row is not None and consume_row["status"] == "pending", (
        f"the consume node was already claimed before the detail rows were purged: {consume_row}"
    )

    # The detail rows are gone: the collect's only home was purged.
    purged = await _delete_item_rows(wf_pool, wf_schema, claim.flow_id, "src")
    assert purged == _TOTAL_ITEMS, purged

    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=200)
    assert outcome == "terminal", outcome

    failed = await _step_row(wf_pool, wf_schema, claim.flow_id, "consume")
    assert failed is not None, "the consume row vanished"
    assert failed["status"] == "failed", (
        f"the skip gate left the node {failed['status']!r} instead of terminal-failing it"
    )
    assert failed["error_class"] == "WorkflowResultDetailUnavailableError", (
        f"the deterministic refusal was recorded as {failed['error_class']!r}"
    )
    # Deterministic by construction: the FIRST attempt, with a retry budget the
    # ladder deliberately did not spend.
    assert int(failed["attempt"]) == 1, failed
    assert int(failed["max_attempts"]) > 1, failed


@pytest.mark.integration
async def test_truncated_collect_with_absorbed_failure_rebuilds_only_successes(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The detail read must apply the same absorbed-parent filter as the writer.

    The failed item contributes no value and no slot to either the bounded
    prefix or the rebuilt collect; counting it would make an intact row look
    corrupt, while including its null result would fabricate an extra item.
    """
    app = WorkflowApp()

    @app.workflow("tr_truncated_absorbed_failure")
    def wf_def() -> Promise[object]:
        return build(
            map_source(
                step(_emit_indices, _RangeIn(), key="src"),
                _expand_item_or_fail,
                on_failure="collect",
                max_attempts=1,
            )
        )

    runner = FlowRunner(app.get("tr_truncated_absorbed_failure"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    expected_indices = list(range(_TOTAL_ITEMS - 1))
    await _assert_row_truncated(
        wf_pool, wf_schema, claim.flow_id, "src.join", len(expected_indices)
    )

    full = await runner.result(claim.flow_id)
    assert isinstance(full, list)
    assert [item["idx"] for item in full] == expected_indices

    handle = ChildHandle[_FatItemList](
        site=JoinSite(
            pool=wf_pool,
            wsql=WorkflowSql.build(wf_schema),
            flow_id=claim.flow_id,
            job_id=claim.flow_id,
            node_key="standalone",
            attempt=1,
            ledger_id=None,
        ),
        flow_id=claim.flow_id,
        result_model=_FatItemList,
        terminal_key="src.join",
    )
    outcome = await handle.wait()
    assert isinstance(outcome, ChildOk)
    assert [item.idx for item in outcome.result.root] == expected_indices


@pytest.mark.integration
async def test_truncated_mixed_gather_preserves_nested_list_and_empty_prefix(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A gather flattens only when EVERY parent returns a list.

    The oversized first parent also forces an empty kept prefix, proving the
    rebuild does not depend on seeing even one summary item to infer shape.
    """
    app = WorkflowApp()

    @app.workflow("tr_truncated_mixed_gather")
    def wf_def() -> Promise[object]:
        many = step(_emit_half, _RangeIn(), key="many")
        scalar = step(_expand_item, _TOTAL_ITEMS, key="scalar")
        return build(gather([many, scalar]))

    runner = FlowRunner(app.get("tr_truncated_mixed_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    kept = await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "gather", 2)
    assert kept == 0

    full = await runner.result(claim.flow_id)
    assert isinstance(full, list) and len(full) == 2
    nested, scalar = full
    assert isinstance(nested, list)
    assert [item["idx"] for item in nested] == list(range(_TOTAL_ITEMS))
    assert isinstance(scalar, dict) and scalar["idx"] == _TOTAL_ITEMS

    handle = ChildHandle[_MixedGatherResult](
        site=JoinSite(
            pool=wf_pool,
            wsql=WorkflowSql.build(wf_schema),
            flow_id=claim.flow_id,
            job_id=claim.flow_id,
            node_key="standalone",
            attempt=1,
            ledger_id=None,
        ),
        flow_id=claim.flow_id,
        result_model=_MixedGatherResult,
        terminal_key="gather",
    )
    outcome = await handle.wait()
    assert isinstance(outcome, ChildOk)
    assert outcome.result.root == full


@pytest.mark.integration
async def test_truncated_join_with_expired_parent_result_fails_closed(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A null parent result left by the TTL sweep is missing detail, not a
    collected ``None``: the row and edge surviving must not let the count
    check bless a collect with a hole in it."""
    app = WorkflowApp()

    @app.workflow("tr_expired_parent_detail")
    def wf_def() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item))

    runner = FlowRunner(app.get("tr_expired_parent_detail"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)

    # Once the join has fired, the ordinary result-retention sweep may expire
    # its parents. Keep the row and edge, exactly as the sweep does.
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET result_expires_at = now() - interval '1 second' "
                "WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.item' AND map_index = 0",
                wf_schema,
            ),
            claim.flow_id,
        )
    assert await PostgresBackend.sweep_expired_results(wf_pool, schema=wf_schema) == 1

    with pytest.raises(
        WorkflowResultDetailUnavailableError,
        match=r"node 'src\.item'.*result TTL has expired",
    ):
        await runner.result(claim.flow_id)


@pytest.mark.integration
async def test_prestamp_truncated_gather_uses_compiled_kind_fallback(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """Runner reads of in-flight rows created before the metadata stamp must
    still flatten through the compiled graph, rather than failing a healthy
    historical row as incomplete."""
    app = WorkflowApp()

    @app.workflow("tr_prestamp_gather")
    def wf_def() -> Promise[object]:
        half_a = step(_emit_half, _RangeIn(start=0, count=_HALF_ITEMS), key="half_a")
        half_b = step(_emit_half, _RangeIn(start=_HALF_ITEMS, count=_HALF_ITEMS), key="half_b")
        return build(gather([half_a, half_b]))

    runner = FlowRunner(app.get("tr_prestamp_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "gather", _TOTAL_ITEMS)
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET metadata = metadata - 'wf_node_kind' "
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gather'",
                wf_schema,
            ),
            claim.flow_id,
        )

    full = await runner.result(claim.flow_id)
    assert isinstance(full, list)
    assert [item["idx"] for item in full] == list(range(_TOTAL_ITEMS))

    state = await runner._flow_state(claim.flow_id)
    results = state["results"]
    assert isinstance(results, dict)
    gathered = results["gather"]
    assert isinstance(gathered, list)
    assert [item["idx"] for item in gathered] == list(range(_TOTAL_ITEMS))
