"""Regression pins for a ``skip=``-carrying step wired as a ``map_source`` source.

When a step declared with ``skip=pred`` is wired as the source of a
``map_source(...)`` and ``pred`` returns ``True`` at runtime:

1. ``FlowRunner._execute_claimed_tail`` routes to ``_finalize_skipped`` before
   executing the source body.
2. ``_finalize_skipped`` calls ``finalize_node(..., outcome="succeeded",
   result={"skipped": True})`` with NO ``fork=`` argument, bypassing
   ``_finalize_success`` and ``_map_fork``.
3. The derived ``<source>.join`` row is never spawned, downstream consumers stay
   ``status='pending'`` with ``deps_pending=1`` and zero incoming edges (stamped
   ``orphan_parent`` by ``wf_join_rederive``), and ``__flow__`` remains
   ``status='running'`` permanently with zero error rows anywhere.

These pins cover all three doors:
- wiring refusal at ``map_source(...)`` (both single-parent ``kind="step"`` and
  multi-parent ``kind="gather"`` sources);
- checker-independent validation re-proof on a mutated ``CompiledWorkflow``
  (both ``validate_compiled`` and ``FlowRunner.__init__``);
- runtime fail-closed defense against real Postgres so a fan-out source
  reaching ``skip=True`` can never silently wedge ``__flow__`` in ``running``.

The same three doors are pinned on the OTHER attachment that lowers into the
same fork (``route(...)`` / ``map_source``'s dict form — ``_attach_route``),
plus the two zero-false-positive faces the refusal must not swallow: a plain
``step(..., skip=...)`` CONSUMING a map's join is still legal, and a fan-out
source whose predicate would have returned ``False`` still fails closed at
the runtime door (the guard rejects the state, never the predicate's verdict).
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    WorkflowRunError,
    build,
    map_source,
    route,
    step,
)
from taskq.workflows.api._validate import WorkflowValidationError, validate_compiled

if TYPE_CHECKING:
    import asyncpg


async def produce_items(ctx: StepContext) -> list[int]:
    return [1, 2, 3]


async def seed_left(ctx: StepContext) -> list[int]:
    return [1, 2]


async def seed_right(ctx: StepContext) -> list[int]:
    return [3]


async def merge_items(ctx: StepContext, left: list[int], right: list[int]) -> list[int]:
    return [*left, *right]


async def produce_from_seed(ctx: StepContext, seed: list[int]) -> list[int]:
    return seed


async def double_item(ctx: StepContext, item: int) -> int:
    return item * 2


async def consume_items(ctx: StepContext, items: list[int]) -> int:
    return sum(items)


def _always_skip(_state: dict[str, object]) -> bool:
    return True


class Doc(BaseModel):
    doc_id: str


class DocResult(BaseModel):
    doc_id: str


async def produce_docs(ctx: StepContext) -> list[Doc]:
    return [Doc(doc_id="a")]


async def process_doc(ctx: StepContext, doc: Doc) -> DocResult:
    return DocResult(doc_id=doc.doc_id)


def test_map_source_refuses_skip_source_at_wiring() -> None:
    """Wiring ``map_source`` onto a ``step(..., skip=...)`` must raise
    ``WorkflowBuildError`` rather than accepting a graph that wedges at runtime."""
    app = WorkflowApp()

    @app.workflow("skipped_map_source")
    def flow() -> Promise[object]:
        items = step(produce_items, key="items", skip=_always_skip)
        mapped = map_source(items, double_item)
        return build(step(consume_items, mapped, key="consume"))

    with pytest.raises(WorkflowBuildError) as exc_info:
        app.get("skipped_map_source")
    message = str(exc_info.value)
    assert "skip" in message.lower(), f"refusal must mention skip=: {message!r}"
    assert "items" in message, f"refusal must name the source node: {message!r}"


def test_map_source_refuses_multi_parent_skip_source_at_wiring() -> None:
    """A multi-parent ``step(..., skip=...)`` has ``kind='gather'``; wiring
    ``map_source`` onto it hits the same ``_finalize_skipped`` wedge and must
    be refused at the same wiring door."""
    app = WorkflowApp()

    @app.workflow("skipped_gather_map_source")
    def flow() -> Promise[object]:
        left = step(seed_left, key="left")
        right = step(seed_right, key="right")
        items = step(merge_items, left, right, key="items", skip=_always_skip)
        mapped = map_source(items, double_item)
        return build(step(consume_items, mapped, key="consume"))

    with pytest.raises(WorkflowBuildError) as exc_info:
        app.get("skipped_gather_map_source")
    message = str(exc_info.value)
    assert "skip" in message.lower(), f"refusal must mention skip=: {message!r}"
    assert "items" in message, f"refusal must name the source node: {message!r}"


def test_validate_refuses_skip_on_map_source() -> None:
    """``CompiledWorkflow`` is public mutable data; ``validate_compiled`` must
    re-prove the refusal when ``skip`` is set on a ``map_source`` node."""
    app = WorkflowApp()

    @app.workflow("mutated_skip_map_source")
    def flow() -> Promise[object]:
        items = step(produce_items, key="items")
        mapped = map_source(items, double_item)
        return build(step(consume_items, mapped, key="consume"))

    compiled = app.get("mutated_skip_map_source")
    compiled.nodes["items"].skip = _always_skip
    with pytest.raises(WorkflowValidationError) as exc_info:
        validate_compiled(compiled)
    message = str(exc_info.value)
    assert "skip" in message.lower(), f"validation error must mention skip=: {message!r}"
    assert "items" in message, f"validation error must name the source node: {message!r}"


@pytest.mark.integration
async def test_flow_runner_refuses_mutated_skip_map_source_before_postgres_wedge(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """``FlowRunner.__init__`` validates the compiled graph and must refuse a
    mutated ``skip=`` on a ``map_source`` source before any row is created."""
    app = WorkflowApp()

    @app.workflow("runner_mutated_skip_map_source")
    def flow() -> Promise[object]:
        items = step(produce_items, key="items")
        mapped = map_source(items, double_item)
        return build(step(consume_items, mapped, key="consume"))

    compiled = app.get("runner_mutated_skip_map_source")
    compiled.nodes["items"].skip = _always_skip
    with pytest.raises(WorkflowValidationError) as exc_info:
        FlowRunner(compiled, wf_pool, wf_schema)
    assert "skip" in str(exc_info.value).lower()


@pytest.mark.integration
async def test_runtime_skip_on_map_source_never_wedges_workflow_in_postgres(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """Runtime defense against real Postgres: even if a ``map_source`` node
    reaches execution with a truthy ``skip`` predicate (post-init mutation or
    bypassed validation), the workflow must reach a terminal state and never
    wedge ``__flow__`` in ``running`` with an unspawned ``<source>.join``."""
    app = WorkflowApp()

    @app.workflow("runtime_skip_map_source_wedge")
    def flow() -> Promise[object]:
        items = step(produce_items, key="items")
        mapped = map_source(items, double_item)
        return build(step(consume_items, mapped, key="consume"))

    compiled = app.get("runtime_skip_map_source_wedge")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Simulate a runtime path that bypasses build/init validation.
    runner.compiled.nodes["items"].skip = _always_skip

    verdict = await runner.drive(flow_id, max_ticks=25, tick=0.01)

    rows = await wf_conn.fetch(
        f"SELECT step_key, status, attempt, deps_pending, result, error_class, "
        f"metadata->>'blocking_reason' AS blocking_reason "
        f'FROM "{wf_schema}".jobs '
        "WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1 "
        "ORDER BY step_key",
        flow_id,
    )
    by_key = {str(r["step_key"]): dict(r) for r in rows}

    assert verdict == "terminal", f"workflow wedged (drive returned {verdict!r}); rows={by_key}"
    assert by_key["__flow__"]["status"] == "failed", by_key
    assert by_key["items"]["status"] == "failed", by_key
    assert by_key["items"]["error_class"] == "SkippedFanOutSourceError", by_key["items"]
    assert by_key["items"]["attempt"] == 1, (
        f"the deterministic refusal must not spend the retry ladder: {by_key['items']}"
    )
    assert "items.join" not in by_key, f"the refused source must not fork: {by_key}"
    assert by_key["consume"]["status"] == "pending", by_key["consume"]
    assert by_key["consume"]["deps_pending"] == 1, by_key["consume"]
    assert by_key["consume"]["blocking_reason"] == "flow_dead", (
        f"the reserved consumer must name the dead flow, not an orphan: {by_key['consume']}"
    )


@pytest.mark.integration
async def test_runtime_skip_on_terminal_map_source_never_succeeds_without_join_in_postgres(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """When ``map_source`` is the workflow terminal (``build(mapped)``) with no
    downstream consumer, skipping the source without spawning ``<source>.join``
    falsely terminalizes ``__flow__`` as ``succeeded`` while ``runner.result``
    returns ``None`` because the terminal join row never existed."""
    app = WorkflowApp()

    @app.workflow("runtime_skip_terminal_map_source")
    def flow() -> Promise[object]:
        items = step(produce_items, key="items")
        mapped = map_source(items, double_item)
        return build(mapped)

    compiled = app.get("runtime_skip_terminal_map_source")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Simulate a runtime path that bypasses build/init validation.
    runner.compiled.nodes["items"].skip = _always_skip

    verdict = await runner.drive(flow_id, max_ticks=25, tick=0.01)

    rows = await wf_conn.fetch(
        f"SELECT step_key, status, attempt, deps_pending, result, error_class "
        f'FROM "{wf_schema}".jobs '
        "WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1 "
        "ORDER BY step_key",
        flow_id,
    )
    by_key = {str(r["step_key"]): dict(r) for r in rows}

    assert verdict == "terminal", f"workflow wedged (drive returned {verdict!r}); rows={by_key}"
    assert by_key["__flow__"]["status"] == "failed", by_key
    assert by_key["items"]["status"] == "failed", by_key
    assert by_key["items"]["error_class"] == "SkippedFanOutSourceError", by_key["items"]
    assert by_key["items"]["attempt"] == 1, (
        f"the deterministic refusal must not spend the retry ladder: {by_key['items']}"
    )
    assert "items.join" not in by_key, f"the refused terminal source must not fork: {by_key}"
    with pytest.raises(WorkflowRunError, match="SkippedFanOutSourceError") as exc_info:
        await runner.result(flow_id)
    assert "terminal map" in str(exc_info.value)


@pytest.mark.integration
async def test_runtime_skip_refusal_precedes_flow_state_read(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """The invalid fan-out state must win over an unreadable sibling result.

    If the guard performed ``_flow_state`` first, the expired ``seed`` row
    would record ``WorkflowResultDetailUnavailableError`` instead of the
    fan-out refusal. Real retention state, rather than a runner mock, pins
    the guard's ordering.
    """
    app = WorkflowApp()

    @app.workflow("runtime_skip_before_state_read")
    def flow() -> Promise[object]:
        seed = step(seed_left, key="seed")
        items = step(produce_from_seed, seed, key="items")
        return build(map_source(items, double_item))

    compiled = app.get("runtime_skip_before_state_read")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    runner.compiled.nodes["items"].skip = _always_skip

    # The first pass completes only the upstream seed; the map source becomes
    # claimable on the next pass. Simulate result retention between them.
    await runner.tick(flow_id)
    expired = await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET result = NULL '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'seed' AND status = 'succeeded'",
        flow_id,
    )
    assert expired == "UPDATE 1", "the state-read precondition did not land"

    verdict = await runner.drive(flow_id, max_ticks=25, tick=0.01)

    items = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'items'",
        flow_id,
    )
    assert verdict == "terminal"
    assert items is not None
    assert items["status"] == "failed", dict(items)
    assert items["error_class"] == "SkippedFanOutSourceError", dict(items)
    assert items["attempt"] == 1, dict(items)


# ── the other attachment: ``_attach_route`` (``route`` / dict-form map) ──


def test_route_refuses_skip_source_at_wiring() -> None:
    """``route(...)`` lowers into the same fork-spawning attachment, so a
    ``skip=`` predicate on its source is the same wedge and is refused at
    the same wiring door."""
    app = WorkflowApp()

    @app.workflow("skipped_route_source")
    def flow() -> Promise[object]:
        docs = step(produce_docs, key="docs", skip=_always_skip)
        routed = route(docs, {Doc: RouteArm(body=process_doc)})
        return build(routed)

    with pytest.raises(WorkflowBuildError) as exc_info:
        app.get("skipped_route_source")
    message = str(exc_info.value)
    assert "skip" in message.lower(), f"refusal must mention skip=: {message!r}"
    assert "route 'docs'" in message, f"refusal must name the attachment and source: {message!r}"


def test_map_source_dict_form_refuses_skip_source_at_wiring() -> None:
    """``map_source(src, {T: arm})`` is the typed route's dict form — one
    lowering, one refusal. Fixing only the plain-callable branch would
    leave this spelling accepted at build and refused only later."""
    app = WorkflowApp()

    @app.workflow("skipped_dict_map_source")
    def flow() -> Promise[object]:
        docs = step(produce_docs, key="docs", skip=_always_skip)
        mapped = map_source(docs, {Doc: process_doc})
        return build(mapped)

    with pytest.raises(WorkflowBuildError) as exc_info:
        app.get("skipped_dict_map_source")
    message = str(exc_info.value)
    assert "skip" in message.lower(), f"refusal must mention skip=: {message!r}"
    assert "map_source 'docs'" in message, (
        f"refusal must name the attachment and source: {message!r}"
    )


def test_validate_refuses_skip_on_route_source() -> None:
    """The validator's re-proof covers ``map_arms`` (the route face), not
    just ``map_item`` — a mutated route source is refused identically."""
    app = WorkflowApp()

    @app.workflow("mutated_skip_route_source")
    def flow() -> Promise[object]:
        docs = step(produce_docs, key="docs")
        routed = route(docs, {Doc: RouteArm(body=process_doc)})
        return build(routed)

    compiled = app.get("mutated_skip_route_source")
    compiled.nodes["docs"].skip = _always_skip
    with pytest.raises(WorkflowValidationError) as exc_info:
        validate_compiled(compiled)
    message = str(exc_info.value)
    assert "E22-skip-fan-out-source" in message, message
    assert "docs" in message, f"validation error must name the source node: {message!r}"


def test_a_skip_predicate_downstream_of_a_map_join_is_still_legal() -> None:
    """The refusal's ZERO-FALSE-POSITIVE face: ``skip=`` on a step that
    CONSUMES a map's join (it has parents, and the join is fork-spawned)
    is the documented sibling-conditional shape and must keep compiling —
    the refusal is about the node that OWNS the fork, never about any node
    downstream of one."""
    app = WorkflowApp()

    @app.workflow("skip_consumer_of_map")
    def flow() -> Promise[object]:
        items = step(produce_items, key="items")
        mapped = map_source(items, double_item)
        return build(step(consume_items, mapped, key="consume", skip=_always_skip))

    compiled = app.get("skip_consumer_of_map")
    diagnostics = validate_compiled(compiled)
    errors = [d for d in diagnostics if d.severity == "error"]
    assert not errors, f"a skip predicate on the map's CONSUMER must stay legal: {errors}"


@pytest.mark.integration
async def test_runtime_skip_on_route_source_fails_closed_even_when_predicate_is_false(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """The runtime door rejects the STATE, never the predicate's verdict: a
    fan-out source carrying a ``skip=`` predicate whose call would return
    ``False`` still fails closed — the guard runs before the state read and
    the predicate, so it cannot be talked out of the refusal."""
    predicate_calls: list[dict[str, object]] = []

    def never_skip(state: dict[str, object]) -> bool:
        predicate_calls.append(state)
        return False

    app = WorkflowApp()

    @app.workflow("runtime_never_skip_route_source")
    def flow() -> Promise[object]:
        docs = step(produce_docs, key="docs")
        routed = route(docs, {Doc: RouteArm(body=process_doc)})
        return build(routed)

    compiled = app.get("runtime_never_skip_route_source")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Simulate a runtime path that bypasses build/init validation, with a
    # predicate that would have said "do not skip".
    runner.compiled.nodes["docs"].skip = never_skip

    verdict = await runner.drive(flow_id, max_ticks=25, tick=0.01)

    rows = await wf_conn.fetch(
        f"SELECT step_key, status, attempt, error_class "
        f'FROM "{wf_schema}".jobs '
        "WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1 "
        "ORDER BY step_key",
        flow_id,
    )
    by_key = {str(r["step_key"]): dict(r) for r in rows}

    assert verdict == "terminal", f"workflow wedged (drive returned {verdict!r}); rows={by_key}"
    assert by_key["__flow__"]["status"] == "failed", (
        f"a skipped fan-out source must fail the run, not succeed it: {by_key}"
    )
    assert by_key["docs"]["status"] == "failed", (
        f"the fan-out source must fail on its first attempt: {by_key}"
    )
    assert by_key["docs"]["error_class"] == "SkippedFanOutSourceError", (
        f"the record must name the refusal class: {by_key['docs']}"
    )
    assert by_key["docs"]["attempt"] == 1, (
        f"the deterministic refusal must not spend the retry ladder: {by_key['docs']}"
    )
    assert predicate_calls == [], "the refused fan-out source must not evaluate its predicate"
