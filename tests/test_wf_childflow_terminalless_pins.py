"""Terminal-less child workflow pins for ``ctx.spawn_flow``.

``ctx.spawn_flow(name, ..., result=G)`` promises a :class:`ChildHandle` whose
``wait()`` decodes the child workflow's terminal node into ``G``. A compiled
child workflow with ``terminal is None`` (for example, one whose steps are
explicitly sunk via ``sink(...)`` and returns ``None``) can never produce a
terminal result, so ``spawn_flow`` refuses it before any child row or spawn
cursor is written.

Because no retry can give a terminal-less workflow a terminal node, this
refusal is a deterministic authoring failure: the parent step must fail on
attempt 1 with exactly one ``failed`` ledger row, zero child flows minted,
and no retry backoff burned.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import asyncio
import json
from typing import Any, NoReturn, cast

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.workflows import (
    ChildWorkflowNoTerminalError,
    ChildWorkflowUnknownError,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    sink,
    step,
)
from taskq.workflows._childflow import CtxSpawnOps
from taskq.workflows._progress import MESSAGE_TERMINAL_MAX
from taskq.workflows.api._runner_ladder import is_deterministic_authoring_failure


class _LeafOut(BaseModel):
    n: int


class _ParentOut(BaseModel):
    ok: bool


class _InertSentinel:
    """Stand-in for host attributes: reads succeed, any call fails loudly."""

    def __getattr__(self, name: str) -> _InertSentinel:
        return self

    def __call__(self, *args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(
            "spawn_flow touched a host field before refusing a terminal-less child"
        )


class _StubSpawnHost:
    """Inert ``_SpawnHost`` proving the terminal-less refusal runs before any DB use."""

    __slots__ = (
        "_ledger_id",
        "_loop_iteration",
        "_map_index",
        "_pool",
        "_wsql",
        "attempt",
        "claim_epoch",
        "flow_id",
        "job_id",
        "node_key",
    )

    def __init__(self) -> None:
        sentinel = _InertSentinel()
        self.flow_id = sentinel
        self.job_id = sentinel
        self.node_key = sentinel
        self.attempt = sentinel
        self.claim_epoch = sentinel
        self._pool = sentinel
        self._wsql = sentinel
        self._ledger_id = sentinel
        self._loop_iteration = None
        self._map_index = None


def _register_terminalless_child(app: WorkflowApp, name: str) -> None:
    @app.actor(queue="cpu", max_attempts=1)
    async def sunk_leaf(ctx: StepContext) -> _LeafOut:
        return _LeafOut(n=1)

    workflow_factory = app.workflow(name)

    @workflow_factory
    def child() -> None:
        sink(step(sunk_leaf, key="leaf"))

    compiled = app.get(name)
    assert compiled.terminal is None, compiled.terminal


def _spawn_refusal(host: Any, name: str) -> ChildWorkflowUnknownError:
    """Invoke the spawn on ``host`` and return the refusal it raised."""
    with pytest.raises(ChildWorkflowUnknownError) as exc_info:
        asyncio.run(CtxSpawnOps.spawn_flow(host, name, result=_LeafOut))
    return exc_info.value


def test_terminalless_child_refusal_is_classified_as_deterministic() -> None:
    """Refusing a terminal-less child workflow must route deterministically
    (both directly and when embedded in an ``ExceptionGroup``) while keeping
    ``ChildWorkflowUnknownError`` catch compatibility."""
    child_name = f"terminalless_unit_child_{new_uuid().hex[:12]}"
    child_app = WorkflowApp()
    _register_terminalless_child(child_app, child_name)

    exc = _spawn_refusal(cast("Any", _StubSpawnHost()), child_name)

    assert isinstance(exc, ChildWorkflowNoTerminalError), (
        f"a registered-but-terminal-less child is reported as {type(exc).__name__} — "
        "the record must name the terminal-less refusal, not an unknown workflow"
    )
    assert "terminal" in str(exc).lower(), str(exc)
    assert is_deterministic_authoring_failure(exc), (
        f"terminal-less child refusal ({type(exc).__name__}) is not classified as a "
        "deterministic authoring failure — the retry ladder will burn all attempts"
    )
    grouped = ExceptionGroup("spawn fanout", [exc])
    assert is_deterministic_authoring_failure(grouped), (
        f"grouped terminal-less child refusal ({type(exc).__name__}) is not classified "
        "as a deterministic authoring failure"
    )


def test_terminalless_child_refusal_keeps_its_diagnosis_for_any_name_length() -> None:
    """The refusal's persisted form is truncated to ``MESSAGE_TERMINAL_MAX``,
    so the terminal diagnosis must survive that slice for a registry key of any
    length (nothing else bounds a workflow name). The name here deliberately
    does NOT contain the word "terminal": an unbounded ``{name!r}`` would push
    the diagnosis past the window and this pin would fail."""
    child_name = "longname_" + "x" * MESSAGE_TERMINAL_MAX
    child_app = WorkflowApp()
    _register_terminalless_child(child_app, child_name)
    assert len(child_name) > MESSAGE_TERMINAL_MAX

    exc = _spawn_refusal(cast("Any", _StubSpawnHost()), child_name)

    persisted = str(exc)[:MESSAGE_TERMINAL_MAX].lower()
    assert "terminal" in persisted, (
        f"the terminal diagnosis is truncated away for a {len(child_name)}-character "
        f"workflow name (the name must be bounded in the message): {persisted!r}"
    )


@pytest.mark.integration
class TestSpawnTerminalLessChild:
    async def test_spawn_flow_on_terminalless_child_fails_deterministically_on_attempt_one(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """A parent step with ``max_attempts=3`` spawning a registered
        terminal-less child must fail on attempt 1 with exactly 1 failed
        ledger row, 1 body invocation, 0 child flows created, and no spawn
        cursor or receipt written."""
        suffix = new_uuid().hex[:12]
        child_name = f"terminalless_child_{suffix}"
        parent_name = f"terminalless_parent_{suffix}"

        child_app = WorkflowApp()
        _register_terminalless_child(child_app, child_name)

        calls = 0
        parent_app = WorkflowApp()

        @parent_app.actor(queue="cpu", max_attempts=3)
        async def spawner(ctx: StepContext) -> _ParentOut:
            nonlocal calls
            calls += 1
            handle = await ctx.spawn_flow(child_name, result=_LeafOut)
            await handle.wait()
            return _ParentOut(ok=True)

        @parent_app.workflow(parent_name)
        def parent() -> Promise[object]:
            return build(step(spawner, key="wrap"))

        runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        children = await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        assert children == 0, f"spawn_flow minted {children} child flow(s) for terminal-less child"

        node = await wf_conn.fetchrow(
            f'SELECT status, attempt, error_class, error_message, metadata FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["status"] == "failed", node["status"]
        assert node["error_class"] == "ChildWorkflowNoTerminalError", (
            f"the parent row records {node['error_class']!r} for a registered child — "
            "an operator would read that as an unregistered workflow name"
        )
        assert "terminal" in str(node["error_message"]).lower(), str(node["error_message"])

        raw_meta = node["metadata"]
        meta = cast(
            "dict[str, Any]", json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
        )
        touched = sorted(
            k for k in meta if k.startswith(("spawn_slot_", "spawn_cursor_")) or k == "child_flows"
        )
        assert touched == [], f"spawn_flow wrote {touched} before refusing terminal-less child"

        failed = await wf_conn.fetch(
            f'SELECT error_class, error_message FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )
        assert len(failed) == 1, (
            f"terminal-less child refusal burned {len(failed)} attempts "
            f"({[r['error_class'] for r in failed]}) — expected deterministic failure on attempt 1"
        )
        assert failed[0]["error_class"] == "ChildWorkflowNoTerminalError", failed[0]["error_class"]
        assert node["attempt"] == 1, f"expected attempt=1, got {node['attempt']}"
        assert calls == 1, f"expected parent body to run once, ran {calls} times"
