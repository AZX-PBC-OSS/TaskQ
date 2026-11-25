"""Integration pins for readers of truncated bounded-collect workflow joins.

When a ``map_join`` or ``gather`` node collects more than the 64 KiB
``FANIN_FAILURES_BYTE_CAP``, ``_bounded_collect`` writes a bounded envelope
(``value`` prefix + ``__truncated__`` count + ``detail_home``) while the full
elements remain on the parent rows. Every join-row reader must detect
``result_truncation`` and rebuild the complete list from parent rows (flattening
for ``gather`` and failing closed if parent detail rows are missing), rather
than silently handing callers the 64 KiB prefix.

The same fail-closed contract covers rows with no durable value at all — a
result the retention sweep nulled, or one an operator retry re-pended — the
codec's malformed-row doors (a non-list prefix, a marker that is not a positive
int, a non-object result, an over-count), the one-level flatten's empty-list
boundary, and the per-read memo that keeps a shared truncated subtree to one
fetch per node.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel, RootModel

from taskq.backend._protocol import JobId
from taskq.backend._sql_templates import render as render_backend_sql
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
from taskq.workflows.api._runner_codec import decode_result_with_detail
from taskq.workflows.api._runner_errors import WorkflowResultDetailUnavailableError
from taskq.workflows.api._sql_runner import PARENT_RESULTS_BY_KEY_SQL_TEMPLATE, render_sql

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
            claim_epoch=1,
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
            claim_epoch=1,
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
            claim_epoch=1,
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

    The runner seats resolve the kind from the compiled graph, so the shape is
    stamp-independent there. At the runner-less seat a ``map_join`` row carries
    no ``wf_node_kind`` stamp either, and the count gate is what keeps it
    correct: the un-flattened parent count already equals the recorded count,
    so the pre-stamp count-verified reconciliation never fires. A naive
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
            claim_epoch=1,
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
            claim_epoch=1,
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


# ── the result-expiry contract: a row with no durable value refuses ──────


async def _row_full(
    wf_pool: asyncpg.Pool, wf_schema: str, flow_id: JobId, step_key: str
) -> dict[str, Any] | None:
    """One row's value face: status + the result/TTL pair + the error record."""
    sql = render_sql(
        "SELECT id, status, result, result_expires_at, error_class, attempt "
        "FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        wf_schema,
    )
    async with wf_pool.acquire() as conn:
        row = await conn.fetchrow(sql, flow_id, step_key)
    return None if row is None else dict(row)


async def _child_handle(
    wf_pool: asyncpg.Pool, wf_schema: str, flow_id: JobId, terminal_key: str
) -> ChildHandle[_FatItemList]:
    return ChildHandle[_FatItemList](
        site=JoinSite(
            pool=wf_pool,
            wsql=WorkflowSql.build(wf_schema),
            flow_id=flow_id,
            job_id=flow_id,
            node_key="standalone",
            attempt=1,
            claim_epoch=1,
            ledger_id=None,
        ),
        flow_id=flow_id,
        result_model=_FatItemList,
        terminal_key=terminal_key,
    )


async def _re_pend_first_item(wf_pool: asyncpg.Pool, wf_schema: str, flow_id: JobId) -> None:
    """The row shape ``Backend.retry_job`` writes for an operator re-run:
    ``status='pending'``, ``result=NULL``, ``result_expires_at=NULL``."""
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET status = 'pending', result = NULL, "
                "result_expires_at = NULL, finished_at = NULL, error_class = NULL, "
                "error_message = NULL, error_traceback = NULL, "
                "locked_by_worker = NULL, lock_expires_at = NULL "
                "WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.item' AND map_index = 0",
                wf_schema,
            ),
            flow_id,
        )


async def _un_re_pend_first_item(wf_pool: asyncpg.Pool, wf_schema: str, flow_id: JobId) -> None:
    """Restore the pin's own out-of-band row so the module teardown's
    status-truth check reads the row set the engine actually left. The
    parent's ``result`` stays NULL: the value is what the pin is about."""
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET status = 'succeeded' "
                "WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.item' AND map_index = 0",
                wf_schema,
            ),
            flow_id,
        )


async def _truncated_map_join_run(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> tuple[FlowRunner, JobId]:
    """A terminal flow whose ``map_join`` row really is a bounded summary."""
    app = WorkflowApp()

    @app.workflow("pin_expiry_map_join")
    def wf_def() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item))

    runner = FlowRunner(app.get("pin_expiry_map_join"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)
    return runner, claim.flow_id


@pytest.mark.integration
async def test_re_pended_parent_row_does_not_decode_as_a_collected_none(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A re-pended parent row has no durable value left: the truncated join's
    rebuild must refuse, never fabricate a ``None`` item and pass its count
    check on row presence alone.

    ``result_expired`` is the statement's two-valued ``p.result IS NULL``. The
    TTL comparison alone would be SQL NULL for a re-pended row (no expiry
    stamp at all), read as "not expired", and the row's lost value would be
    counted as a collected item.
    """
    runner, flow_id = await _truncated_map_join_run(wf_pool, wf_schema)
    await _re_pend_first_item(wf_pool, wf_schema, flow_id)
    try:
        with pytest.raises(WorkflowResultDetailUnavailableError, match="result TTL has expired"):
            await runner.result(flow_id)
    finally:
        await _un_re_pend_first_item(wf_pool, wf_schema, flow_id)


@pytest.mark.integration
async def test_parent_result_expired_flag_is_never_null(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """``result_expired`` is read through ``bool(...)``, so the statement must
    return a two-valued boolean for every parent shape."""
    _runner, flow_id = await _truncated_map_join_run(wf_pool, wf_schema)
    await _re_pend_first_item(wf_pool, wf_schema, flow_id)
    try:
        async with wf_pool.acquire() as conn:
            join_id = await conn.fetchval(
                render_sql(
                    "SELECT id FROM {schema}.jobs "
                    "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'src.join'",
                    wf_schema,
                ),
                flow_id,
            )
            rows = await conn.fetch(
                render_sql(PARENT_RESULTS_BY_KEY_SQL_TEMPLATE, wf_schema), join_id
            )
        assert rows, "test precondition failed: the join row has no parent edges"
        for row in rows:
            assert row["result_expired"] is not None, (
                f"parent {row['step_key']!r} has result NULL and result_expires_at NULL, "
                f"and the statement returned a three-valued {row['result_expired']!r} — "
                "bool() then reads it as 'not expired'"
            )
    finally:
        await _un_re_pend_first_item(wf_pool, wf_schema, flow_id)


@pytest.mark.integration
async def test_re_pended_parent_through_the_retry_verb_fails_closed(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The same shape through the production writer: ``Backend.retry_job``.

    The retry verb is a documented operator action with no workflow-row
    exclusion, so its row shape must fail closed at the read rather than
    fabricate an item in a declared ``list[...]``.
    """
    app = WorkflowApp()

    @app.workflow("pin_retry_verb_map_join")
    def wf_def() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item))

    runner = FlowRunner(app.get("pin_retry_verb_map_join"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)

    item_sql = render_sql(
        "SELECT id FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'src.item' AND map_index = 0",
        wf_schema,
    )
    async with wf_pool.acquire() as conn:
        item_id = await conn.fetchval(item_sql, claim.flow_id)
    assert item_id is not None, "the map item row is missing"

    async with wf_pool.acquire() as conn:
        retried = await conn.fetchrow(render_backend_sql(wf_schema).retry_job, item_id)
    assert retried is not None, "retry_job refused the terminal item row"
    try:
        async with wf_pool.acquire() as conn:
            repended = await conn.fetchrow(
                render_sql(
                    "SELECT id, status, result, result_expires_at FROM {schema}.jobs WHERE id = $1",
                    wf_schema,
                ),
                item_id,
            )
        assert repended is not None
        assert repended["status"] == "pending", dict(repended)
        assert repended["result"] is None and repended["result_expires_at"] is None, dict(repended)
        with pytest.raises(WorkflowResultDetailUnavailableError, match="result TTL has expired"):
            await runner.result(claim.flow_id)
    finally:
        # The re-pended row is this pin's own staged state; the shared
        # fixture's status-truth teardown owns the invariant that a flow
        # reporting 'succeeded' holds no live row.
        async with wf_pool.acquire() as conn:
            await conn.execute(
                render_sql("DELETE FROM {schema}.wf_edge WHERE parent_id = $1", wf_schema),
                item_id,
            )
            await conn.execute(
                render_sql("DELETE FROM {schema}.jobs WHERE id = $1", wf_schema), item_id
            )


@pytest.mark.integration
async def test_swept_terminal_row_expiry_refuses_at_both_seats(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The terminal row's OWN result TTL is consulted at both seats.

    ``TERMINAL_RESULT_SQL_TEMPLATE`` and ``_CHILD_TERMINAL_RESULT_SQL`` select
    the row's own ``result_expired``, so a swept terminal refuses through
    ``FlowRunner.result()`` and through ``ChildHandle.wait()`` alike.
    """
    runner, flow_id = await _truncated_map_join_run(wf_pool, wf_schema)
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET result_expires_at = now() - interval '1 second' "
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'src.join'",
                wf_schema,
            ),
            flow_id,
        )
    assert await PostgresBackend.sweep_expired_results(wf_pool, schema=wf_schema) == 1
    expired = await _row_full(wf_pool, wf_schema, flow_id, "src.join")
    assert expired is not None and expired["result"] is None
    assert expired["status"] == "succeeded"

    with pytest.raises(WorkflowResultDetailUnavailableError):
        await runner.result(flow_id)
    handle = await _child_handle(wf_pool, wf_schema, flow_id, "src.join")
    with pytest.raises(WorkflowResultDetailUnavailableError):
        await handle.wait()


@pytest.mark.integration
async def test_skip_gate_flow_state_refuses_a_swept_parent(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The ``skip=`` gate's state read must not decide against a truncated
    collect whose parent result the retention sweep nulled: ``_flow_state``
    raises the typed refusal, and the gate terminal-fails the just-claimed node
    deterministically instead of stranding it."""
    app = WorkflowApp()
    seen_in_skip: list[list[Any]] = []

    def skip_if_incomplete(state: dict[str, object]) -> bool:
        results = state["results"]
        assert isinstance(results, dict)
        joined = results["src.join"]
        assert isinstance(joined, list)
        seen_in_skip.append(list(joined))
        return len(joined) < _TOTAL_ITEMS

    @app.workflow("pin_skip_swept_parent")
    def wf_def() -> Promise[object]:
        items = map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_item)
        return build(step(_consume_items, items, key="consume", skip=skip_if_incomplete))

    runner = FlowRunner(app.get("pin_skip_swept_parent"), wf_pool, wf_schema)
    claim = await runner.create_flow()

    # Advance until the join has collected; the downstream node is then
    # claimable but not yet claimed.
    for _ in range(300):
        join = await _row_full(wf_pool, wf_schema, claim.flow_id, "src.join")
        if join is not None and join["status"] == "succeeded":
            break
        await runner.tick(claim.flow_id)
    else:  # pragma: no cover - a broken fork/join fires here
        pytest.fail("the map join never succeeded within 300 ticks")
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)

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

    with pytest.raises(WorkflowResultDetailUnavailableError, match="result TTL has expired"):
        await runner._flow_state(claim.flow_id)

    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=200) == "terminal"
    assert not seen_in_skip, "the skip predicate ran over the hole instead of refusing"
    consume = await _step_row(wf_pool, wf_schema, claim.flow_id, "consume")
    assert consume is not None, "the consume row vanished"
    assert consume["status"] == "failed", consume
    assert consume["error_class"] == "WorkflowResultDetailUnavailableError", consume


# ── a raising skip predicate gets a disposition ─────────────────────────


@pytest.mark.integration
async def test_raising_skip_predicate_terminal_fails_the_node(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A ``skip=`` predicate that raises must not strand the claimed node.

    The gate's state read sits above the body-failure boundary, so every raise
    from the predicate is routed through the ladder by the gate itself: the
    node gets a disposition (here, the retry budget is spent and the terminal
    failure records the raised class) instead of being left ``running`` until
    its lease expires.
    """

    def bad_predicate(state: dict[str, object]) -> bool:
        results = state["results"]
        assert isinstance(results, dict)
        # A plain authoring bug: the key a predicate guessed wrongly.
        return bool(results["src.items"])

    app = WorkflowApp()

    @app.workflow("pin_skip_predicate_raises")
    def wf_def() -> Promise[object]:
        src = step(_emit_indices, _RangeIn(), key="src")
        return build(step(_consume_items, src, key="consume", skip=bad_predicate))

    runner = FlowRunner(app.get("pin_skip_predicate_raises"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=200) == "terminal"

    consume = await _step_row(wf_pool, wf_schema, claim.flow_id, "consume")
    assert consume is not None, "the consume row vanished"
    assert consume["status"] == "failed", (
        f"the skip predicate's raise left the claimed node {consume['status']!r} "
        f"(attempt {consume['attempt']}/{consume['max_attempts']}, "
        f"error_class={consume['error_class']!r}) — a raise at the gate has no disposition"
    )
    assert consume["error_class"] == "KeyError", consume


# ── the count gate's honesty and direction ──────────────────────────────


@pytest.mark.integration
async def test_count_mismatch_reason_does_not_blame_purged_rows(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """When the parent rows are present but their values do not reconcile with
    the summary, the refusal must say so — the purge diagnosis is for the
    case where the rows are actually gone."""
    runner, flow_id = await _truncated_map_join_run(wf_pool, wf_schema)
    async with wf_pool.acquire() as conn:
        deleted = await conn.execute(
            render_sql(
                "DELETE FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.item' AND map_index = 0",
                wf_schema,
            ),
            flow_id,
        )
    assert deleted == "DELETE 1"
    with pytest.raises(WorkflowResultDetailUnavailableError) as refused:
        await runner.result(flow_id)
    message = str(refused.value)
    assert "does not reconcile" in message, message
    assert "purged" not in message, message
    assert refused.value.expected == _TOTAL_ITEMS, refused.value.expected
    assert refused.value.rebuilt == _TOTAL_ITEMS - 1, refused.value.rebuilt


# ── the pre-stamp gather (no durable node-kind stamp) ───────────────────


@pytest.mark.integration
async def test_prestamp_truncated_gather_reads_through_the_runnerless_seat(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A truncated ``gather`` row written before the ``wf_node_kind`` stamp
    existed (the rolling-upgrade shape) must still read through the
    runner-less ``ChildHandle`` seat.

    That seat has no compiled graph, so the kind is unknown. The re-read
    reconciles the shape by count: the un-flattened parent values are two
    lists, the recorded count is their 200 items, so the one-level gather
    flatten is accepted.
    """
    app = WorkflowApp()

    @app.workflow("pin_prestamp_gather")
    def wf_def() -> Promise[object]:
        half_a = step(_emit_half, _RangeIn(start=0, count=_HALF_ITEMS), key="half_a")
        half_b = step(_emit_half, _RangeIn(start=_HALF_ITEMS, count=_HALF_ITEMS), key="half_b")
        return build(gather([half_a, half_b]))

    runner = FlowRunner(app.get("pin_prestamp_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "gather", _TOTAL_ITEMS)

    # The pre-stamp row: the previous deploy's create path wrote no stamp.
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET metadata = metadata - 'wf_node_kind' "
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gather'",
                wf_schema,
            ),
            claim.flow_id,
        )

    handle = await _child_handle(wf_pool, wf_schema, claim.flow_id, "gather")
    outcome = await handle.wait()
    assert isinstance(outcome, ChildOk)
    assert [item.idx for item in outcome.result.root] == list(range(_TOTAL_ITEMS))


# ── a forged node-kind stamp cannot change the runner's shape ───────────


async def _expand_singleton(ctx: StepContext, idx: int) -> list[_FatItem]:
    """One item returning a ONE-element list: 200 batches exceed the cap, and
    flattening them preserves the item count."""
    return [_FatItem(idx=idx, pad=_ITEM_PAD)]


@pytest.mark.integration
async def test_forged_gather_stamp_cannot_change_a_map_joins_shape(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A metadata write claiming ``wf_node_kind: "gather"`` on a truncated
    ``map_join`` row must not flatten its rebuild at a runner seat: the
    compiled graph is the write path's own authority, so it outranks the
    stamp."""
    app = WorkflowApp()

    @app.workflow("pin_singleton_batches")
    def wf_def() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_singleton))

    runner = FlowRunner(app.get("pin_singleton_batches"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)

    async with wf_pool.acquire() as conn:
        row = await conn.fetchrow(
            render_sql(
                "SELECT id FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.join'",
                wf_schema,
            ),
            claim.flow_id,
        )
    assert row is not None
    assert runner._compiled_node_kind("src.join") == "map_join"
    honest = await runner.result(claim.flow_id)
    assert isinstance(honest, list) and all(isinstance(batch, list) for batch in honest)

    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET metadata = metadata || "
                '\'{"wf_node_kind": "gather"}\'::jsonb WHERE id = $1',
                wf_schema,
            ),
            row["id"],
        )
    forged = await runner.result(claim.flow_id)
    assert forged == honest, (
        "a metadata write claiming wf_node_kind='gather' flattened a truncated "
        f"map_join: read back {type(forged[0]) if isinstance(forged, list) and forged else forged!r}"
    )


# ── a retried absorbed parent must not skew a packed join ───────────────


@pytest.mark.integration
async def test_packed_join_keeps_its_answer_after_an_absorbed_parent_is_retried(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The rebuild's absorbed-parent filter must match the pack's succeeded
    set, not a parent's LIVE status.

    A failed-and-absorbed parent was excluded when the join packed its 199
    items. The per-node retry then re-pends that row; the filter excludes any
    non-succeeded parent on an absorbing edge, so the packed collect stays
    readable with its original 199 items.
    """
    app = WorkflowApp()

    @app.workflow("pin_absorbed_retry")
    def wf_def() -> Promise[object]:
        return build(
            map_source(
                step(_emit_indices, _RangeIn(), key="src"),
                _expand_item_or_fail,
                on_failure="collect",
                max_attempts=1,
            )
        )

    runner = FlowRunner(app.get("pin_absorbed_retry"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    expected_indices = list(range(_TOTAL_ITEMS - 1))

    async with wf_pool.acquire() as conn:
        absorbed = await conn.fetchrow(
            render_sql(
                "SELECT id, status, error_class, error_message FROM {schema}.jobs "
                "WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.item' AND status = 'failed'",
                wf_schema,
            ),
            claim.flow_id,
        )
    assert absorbed is not None, "test precondition failed: no absorbed failed parent"

    restore_sql = render_sql(
        "UPDATE {schema}.jobs SET status = $2, error_class = $3, error_message = $4 WHERE id = $1",
        wf_schema,
    )
    async with wf_pool.acquire() as conn:
        await conn.execute(
            render_sql(
                "UPDATE {schema}.jobs SET status = 'pending', error_class = NULL, "
                "error_message = NULL WHERE id = $1",
                wf_schema,
            ),
            absorbed["id"],
        )
    try:
        full = await runner.result(claim.flow_id)
        assert isinstance(full, list)
        assert [item["idx"] for item in full] == expected_indices, (
            f"the terminal read changed its answer after the retry: {len(full)} items"
        )
    finally:
        async with wf_pool.acquire() as conn:
            await conn.execute(
                restore_sql,
                absorbed["id"],
                absorbed["status"],
                absorbed["error_class"],
                absorbed["error_message"],
            )


# ── the codec's own shape doors (contrived rows, no database) ────────────

_TRUNCATED_RAW = json.dumps({"value": [1], "__truncated__": 1, "detail_home": "pin"})
_LEAF_RAW = json.dumps({"value": 2})


class _ExplodingConn:
    """A ``ConnLike`` stand-in that fails if the re-read reaches the parent query.

    The malformed-row refusals all fire before any fetch, so reaching the
    database would mean the door let the bad shape through.
    """

    async def fetch(self, sql: str, node_id: object) -> list[dict[str, object]]:
        raise AssertionError(f"the re-read fetched parents for a malformed row ({node_id!r})")


class _FixedParentsConn:
    """A ``ConnLike`` stand-in over a fixed parent set, counting its reads."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.fetches = 0

    async def fetch(self, sql: str, node_id: object) -> list[dict[str, object]]:
        self.fetches += 1
        return self.rows


def _leaf_row(parent: object) -> dict[str, object]:
    return {
        "id": parent,
        "step_key": f"n{parent}",
        "result": _LEAF_RAW,
        "metadata": None,
        "result_expired": False,
    }


@pytest.mark.parametrize("marker", ["200", 0, -1, True, None])
async def test_truncated_marker_that_is_not_a_positive_int_refuses(marker: object) -> None:
    """The unwrap door keys on the marker's PRESENCE, so the truncation door
    must not accept a marker it cannot read: a string, a bool, ``None``, zero
    or a negative count is corruption, not "not truncated"."""
    raw = json.dumps({"value": [1], "__truncated__": marker, "detail_home": "pin"})
    with pytest.raises(WorkflowResultDetailUnavailableError, match="not a positive integer"):
        await decode_result_with_detail(
            _ExplodingConn(),  # pyright: ignore[reportArgumentType]  # Why: the stand-in exposes only the fetch the re-read uses.
            "pin",
            node_id=1,
            raw=raw,
            metadata=None,
            step_key="n1",
        )


async def test_truncated_envelope_with_a_non_list_prefix_refuses() -> None:
    raw = json.dumps({"value": {"not": "a list"}, "__truncated__": 2, "detail_home": "pin"})
    with pytest.raises(WorkflowResultDetailUnavailableError, match="prefix is not a list"):
        await decode_result_with_detail(
            _ExplodingConn(),  # pyright: ignore[reportArgumentType]  # Why: the stand-in exposes only the fetch the re-read uses.
            "pin",
            node_id=1,
            raw=raw,
            metadata=None,
            step_key="n1",
        )


async def test_non_dict_result_refuses_with_the_typed_class() -> None:
    """A row whose ``result`` is not a JSON object has no envelope: the typed
    class, never a bare ``AssertionError`` (which ``python -O`` strips)."""
    with pytest.raises(WorkflowResultDetailUnavailableError, match="not a JSON object"):
        await decode_result_with_detail(
            _ExplodingConn(),  # pyright: ignore[reportArgumentType]  # Why: the stand-in exposes only the fetch the re-read uses.
            "pin",
            node_id=1,
            raw="[1, 2]",
            metadata=None,
            step_key="n1",
        )


async def test_count_gate_refuses_an_over_count() -> None:
    """The gate compares counts for EQUALITY: more rebuilt values than the
    summary recorded is as wrong as fewer, and must refuse."""
    conn = _FixedParentsConn([_leaf_row(11), _leaf_row(12), _leaf_row(13)])
    with pytest.raises(WorkflowResultDetailUnavailableError) as refused:
        await decode_result_with_detail(
            conn,  # pyright: ignore[reportArgumentType]  # Why: the stand-in exposes only the fetch the re-read uses.
            "pin",
            node_id=1,
            raw=_TRUNCATED_RAW,
            metadata=None,
            step_key="n1",
        )
    assert refused.value.expected == 2
    assert refused.value.rebuilt == 3
    assert conn.fetches == 1


# ── a shared truncated subtree is rebuilt once per read ─────────────────


class _CountingConn:
    """A ``ConnLike`` over a fixed parent graph: ``node_id -> [parent ids]``."""

    def __init__(self, graph: dict[int, list[int]], leaf_ids: set[int]) -> None:
        self.graph = graph
        self.leaf_ids = leaf_ids
        self.reads: dict[int, int] = {}

    async def fetch(self, sql: str, node_id: object) -> list[dict[str, object]]:
        n = int(str(node_id))
        self.reads[n] = self.reads.get(n, 0) + 1
        return [
            {
                "id": parent,
                "step_key": f"n{parent}",
                "result": _LEAF_RAW if parent in self.leaf_ids else _TRUNCATED_RAW,
                "metadata": None,
                "result_expired": False,
            }
            for parent in self.graph.get(n, [])
        ]


def _diamond(layers: int) -> tuple[dict[int, list[int]], set[int]]:
    """A shared-subtree DAG: two truncated joins over the SAME truncated parents."""
    graph: dict[int, list[int]] = {}
    leaf_ids: set[int] = set()
    node = 1
    layer_nodes: list[int] = [1]
    for _ in range(layers):
        below = [node + 1, node + 2]
        node += 2
        for parent in layer_nodes:
            graph[parent] = list(below)
        layer_nodes = below
    for leaf in layer_nodes:
        graph[leaf] = [-(leaf), -(leaf + 100)]
        leaf_ids.update(graph[leaf])
    return graph, leaf_ids


async def test_shared_truncated_subtree_is_rebuilt_once_per_read() -> None:
    """A truncated subtree shared by two sibling joins must be read once.

    The parent set is immutable inside one read, so the per-read memo makes
    the repeat visits free: one fetch per distinct node, not one per path.
    """
    graph, leaf_ids = _diamond(layers=5)
    conn = _CountingConn(graph, leaf_ids)
    out = await decode_result_with_detail(
        conn,  # pyright: ignore[reportArgumentType]  # Why: the stand-in exposes only the fetch the re-read uses.
        "pin",
        node_id=1,
        raw=_TRUNCATED_RAW,
        metadata=None,
        step_key="n1",
    )
    assert isinstance(out, list)
    assert max(conn.reads.values()) == 1, (
        f"a node in the shared subtree was read {max(conn.reads.values())} times "
        f"(total fetches {sum(conn.reads.values())} for {len(conn.reads)} distinct nodes)"
    )


# ── the recursive metadata read and the empty-list flatten boundary ─────


async def _emit_pin_items(ctx: StepContext, start: int) -> list[dict[str, Any]]:
    return [{"idx": i, "pad": _ITEM_PAD} for i in range(start, start + _HALF_ITEMS)]


@pytest.mark.integration
async def test_nested_truncated_gather_over_gather_rebuilds_at_both_depths(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """``gather([gather([a, b]), c])`` — a bodyless ``gather`` as another
    ``gather``'s parent — must rebuild all items when BOTH join rows are
    truncated.

    The recursion threads each parent row's own ``metadata`` back into the
    read, so the inner gather's stamp is what flattens its two list parents at
    depth; the runner-less seat has no compiled graph and depends on it.
    """
    app = WorkflowApp()

    @app.workflow("pin_nested_gather_over_gather")
    def wf_def() -> Promise[object]:
        half_a = step(_emit_pin_items, 0, key="half_a")
        half_b = step(_emit_pin_items, _HALF_ITEMS, key="half_b")
        inner = gather([half_a, half_b])
        tail = step(_emit_pin_items, 2 * _HALF_ITEMS, key="tail")
        return build(gather([inner, tail]))

    runner = FlowRunner(app.get("pin_nested_gather_over_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"

    # The graph's two bodyless gathers take derived keys (``gather`` and
    # ``gather:1``); the terminal is the outer one.
    outer_key = runner.compiled.terminal
    assert outer_key is not None
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "gather", 2 * _HALF_ITEMS)
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, outer_key, 3 * _HALF_ITEMS)
    expected = list(range(3 * _HALF_ITEMS))

    full = await runner.result(claim.flow_id)
    assert isinstance(full, list)
    assert sorted(item["idx"] for item in full) == expected

    handle = await _child_handle(wf_pool, wf_schema, claim.flow_id, outer_key)
    outcome = await handle.wait()
    assert isinstance(outcome, ChildOk)
    assert sorted(item.idx for item in outcome.result.root) == expected


@pytest.mark.integration
async def test_cycle_guard_refuses_a_self_referencing_truncated_row(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A ``wf_edge`` cycle (staged here as a self-edge, which no production
    writer creates) must be refused with the typed class, not recurse."""
    app = WorkflowApp()

    @app.workflow("pin_cycle_gather")
    def wf_def() -> Promise[object]:
        half_a = step(_emit_half, _RangeIn(start=0, count=_HALF_ITEMS), key="half_a")
        half_b = step(_emit_half, _RangeIn(start=_HALF_ITEMS, count=_HALF_ITEMS), key="half_b")
        return build(gather([half_a, half_b]))

    runner = FlowRunner(app.get("pin_cycle_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"

    async with wf_pool.acquire() as conn:
        join_id = await conn.fetchval(
            render_sql(
                "SELECT id FROM {schema}.jobs WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'gather'",
                wf_schema,
            ),
            claim.flow_id,
        )
        assert join_id is not None
        await conn.execute(
            render_sql(
                "INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy) "
                "VALUES ($1, $1, $2, 'fail_closed')",
                wf_schema,
            ),
            join_id,
            claim.flow_id,
        )
        with pytest.raises(WorkflowResultDetailUnavailableError, match="form a cycle"):
            await decode_result_with_detail(
                conn,
                wf_schema,
                node_id=join_id,
                raw=await conn.fetchval(
                    render_sql("SELECT result FROM {schema}.jobs WHERE id = $1", wf_schema),
                    join_id,
                ),
                metadata=await conn.fetchval(
                    render_sql("SELECT metadata FROM {schema}.jobs WHERE id = $1", wf_schema),
                    join_id,
                ),
                step_key="gather",
            )
        await conn.execute(
            render_sql("DELETE FROM {schema}.wf_edge WHERE parent_id = $1", wf_schema), join_id
        )


async def _empty_list(ctx: StepContext) -> list[_FatItem]:
    return []


async def _big_list(ctx: StepContext) -> list[_FatItem]:
    return [_FatItem(idx=i, pad=_ITEM_PAD) for i in range(_TOTAL_ITEMS)]


@pytest.mark.integration
async def test_truncated_gather_with_empty_list_parent_rebuilds_flattened(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """An empty list parent is a list: it flattens away, not into an empty slot.

    This is the boundary between "all parents are lists" (flatten) and "a
    parent is not a list" (do not): the writer flattens ``[[], [200 items]]``
    to 200, and a reader that disagrees shows up as a count mismatch.
    """
    app = WorkflowApp()

    @app.workflow("pin_empty_parent_gather")
    def wf_def() -> Promise[object]:
        empty = step(_empty_list, key="empty")
        big = step(_big_list, key="big")
        return build(gather([empty, big]))

    runner = FlowRunner(app.get("pin_empty_parent_gather"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"
    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "gather", _TOTAL_ITEMS)

    full = await runner.result(claim.flow_id)
    assert isinstance(full, list)
    assert [item["idx"] for item in full] == list(range(_TOTAL_ITEMS))

    handle = await _child_handle(wf_pool, wf_schema, claim.flow_id, "gather")
    outcome = await handle.wait()
    assert isinstance(outcome, ChildOk)
    assert [item.idx for item in outcome.result.root] == list(range(_TOTAL_ITEMS))


async def _expand_none_item(ctx: StepContext, idx: int) -> object:
    """One map item that legitimately returns ``None``."""
    if idx == 0:
        return None
    return await _expand_item(ctx, idx)


@pytest.mark.integration
async def test_none_returning_parent_is_stored_as_a_document_not_a_null_result(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A ``None``-returning step is NOT ``result IS NULL`` on the row.

    The write path binds the empty document for a ``None`` payload, which is
    what makes ``result IS NULL`` an unambiguous "this item's value is gone"
    signal for the rebuild — and what keeps a real ``None`` item readable.
    """
    app = WorkflowApp()

    @app.workflow("pin_none_item")
    def wf_def() -> Promise[object]:
        return build(map_source(step(_emit_indices, _RangeIn(), key="src"), _expand_none_item))

    runner = FlowRunner(app.get("pin_none_item"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, until="terminal", max_ticks=4000) == "terminal"

    async with wf_pool.acquire() as conn:
        none_row = await conn.fetchrow(
            render_sql(
                "SELECT result FROM {schema}.jobs "
                "WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND step_key = 'src.item' AND map_index = 0",
                wf_schema,
            ),
            claim.flow_id,
        )
    assert none_row is not None, "test precondition failed: no None-returning item row"
    stored: Any = none_row["result"]
    assert stored is not None, (
        "a None-returning step landed SQL NULL — then `result IS NULL` is not a "
        "value-gone signal and a real None item would be refused"
    )
    stored_doc: Any = json.loads(stored) if isinstance(stored, str) else stored
    assert stored_doc == {}, f"a None-returning step must land the empty document ({stored!r})"

    await _assert_row_truncated(wf_pool, wf_schema, claim.flow_id, "src.join", _TOTAL_ITEMS)
    full = await runner.result(claim.flow_id)
    assert isinstance(full, list) and len(full) == _TOTAL_ITEMS
    assert full[0] == {}, f"the None item did not round-trip as its stored document: {full[0]!r}"


# ── the refusal's diagnosis survives the CLI's bounded line ─────────────


@pytest.mark.parametrize(
    ("reason", "expected", "rebuilt", "diagnosis"),
    [
        (
            "result TTL has expired (the row's result is NULL — swept, or re-pended "
            "with no expiry stamp; the surviving row and edge cannot supply a durable "
            "collect item)",
            None,
            None,
            "TTL has expired",
        ),
        (
            "purged or missing parent detail rows (the full collect's only home is "
            "gone: a deleted row, a missing wf_edge row, or a partially deleted fork)",
            200,
            0,
            "purged",
        ),
        (
            "the count does not reconcile (every surviving parent row is counted, "
            "but the parent values do not reach the summary's length)",
            200,
            199,
            "does not reconcile",
        ),
    ],
)
def test_refusal_reason_survives_the_cli_failed_node_line(
    reason: str, expected: int | None, rebuilt: int | None, diagnosis: str
) -> None:
    """``taskq flows status`` must be able to say WHY a refusal happened.

    ``_cli.stuck_lines`` renders a failed node through
    ``_bounded_line(text, 120)``, which keeps about the first 100 characters.
    The message leads with the node key, the counts and the diagnosis, and the
    36-character row id trails them, so the reason and the full counts survive
    the clamp.
    """
    from taskq.workflows._cli import _bounded_line

    message = str(
        WorkflowResultDetailUnavailableError(
            node_id="01a21423-6009-7730-9e41-fc62bbf4a858",
            step_key="src.join",
            reason=reason,
            expected=expected,
            rebuilt=rebuilt,
        )
    )
    rendered = _bounded_line(message)
    assert diagnosis in rendered, (
        f"the CLI's failed-node line drops the refusal's diagnosis: {rendered!r} "
        f"(the full message is {len(message)} chars against the 120-char cap)"
    )
    if expected is not None and rebuilt is not None:
        assert f"expected {expected} items, rebuilt {rebuilt}" in rendered, rendered


def test_refusal_message_omits_a_fabricated_rebuilt_count() -> None:
    """A caller that supplies ``expected`` alone must not be answered with a
    fabricated ``rebuilt 0``."""
    message = str(
        WorkflowResultDetailUnavailableError(
            node_id="01a21423-6009-7730-9e41-fc62bbf4a858",
            step_key="src.join",
            reason="a reason",
            expected=200,
        )
    )
    assert "rebuilt" not in message, message
    assert "expected 200 items" in message, message
