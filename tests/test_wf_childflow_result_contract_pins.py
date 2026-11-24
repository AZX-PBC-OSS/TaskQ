"""The spawn's result-contract pin (#749): ``ctx.spawn_flow(result=…)``
must be a pydantic model class, and the spawn refuses anything else
BEFORE any side effect — no child flow minted, no spawn slot claimed —
as a deterministic authoring failure (the ladder unburned).

The red face on the unrefused tree: ``result=int`` passes the spawn,
the child is created and runs, then ``wait()`` dies on the first use of
the model (``model_json_schema`` for the hold, ``model_validate`` once
the child has succeeded) with an ``AttributeError`` that names neither
``spawn_flow`` nor ``result=``; the retry re-adopts the same child and
burns the whole budget on an error that can never succeed.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import json
from typing import Any, cast

import asyncpg
import pytest
from pydantic import BaseModel, RootModel

from taskq.workflows import (
    ChildWorkflowUnknownError,
    FlowRunner,
    Promise,
    SpawnResultModelError,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows._childflow import CtxSpawnOps
from taskq.workflows.api._runner_ladder import is_deterministic_authoring_failure


class LeafOut(BaseModel):
    n: int


class WrapOut(BaseModel):
    ok: bool


class SubReport(BaseModel):
    findings: list[str]


class Page[T](BaseModel):
    items: list[T]


class TestSpawnResultContract:
    async def test_a_non_model_result_is_refused_before_the_child_exists_and_never_retried(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The issue's own shape (``result=int``, ``max_attempts=2``):
        the spawn must refuse before the child exists, the node must
        terminal-fail on its FIRST attempt (one failed ledger row — a
        deterministic death never rides the ladder), and the record must
        name the door (``spawn_flow``) and the parameter (``result``)."""
        child_app = WorkflowApp()

        @child_app.actor(queue="cpu", max_attempts=1)
        async def i749_leaf(ctx: StepContext) -> LeafOut:
            return LeafOut(n=1)

        @child_app.workflow("i749_child")
        def child() -> Promise[object]:
            return build(step(i749_leaf, key="leaf"))

        child_app.get("i749_child")  # registers the child (the D1 registry)

        parent_app = WorkflowApp()

        @parent_app.actor(queue="cpu", max_attempts=2)
        async def i749_spawner(ctx: StepContext) -> WrapOut:
            handle = await ctx.spawn_flow(
                "i749_child",
                result=int,  # pyright: ignore[reportArgumentType]  # Why: the wrong-typed contract IS the input under test (#749).
            )
            await handle.wait()
            return WrapOut(ok=True)

        @parent_app.workflow("i749_parent")
        def parent() -> Promise[object]:
            return build(step(i749_spawner, key="wrap"))

        runner = FlowRunner(parent_app.get("i749_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        # no side effect before the refusal: no child flow was ever minted.
        children = await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        assert children == 0, (
            f"the spawn minted {children} child flow(s) before refusing result=int"
        )

        node = await wf_conn.fetchrow(
            f'SELECT status, error_class, error_message, metadata FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["status"] == "failed", node["status"]
        assert node["error_class"] == "SpawnResultModelError", (
            node["error_class"],
            node["error_message"],
        )

        # nor any write on the spawning node: no slot claimed, no receipt.
        raw_meta = node["metadata"]
        meta = cast(
            "dict[str, Any]", json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
        )
        touched = sorted(k for k in meta if k.startswith("spawn_cursor_") or k == "child_flows")
        assert touched == [], f"the spawn wrote {touched} before refusing result=int"

        # the deterministic route: one attempt, the ladder unburned.
        failed = await wf_conn.fetch(
            f'SELECT error_class FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )
        assert len(failed) == 1, (
            f"the refusal burned {len(failed)} attempts — a deterministic authoring "
            f"failure must not retry (last error: {node['error_class']}: "
            f"{node['error_message']})"
        )
        # the ledger row names the refusal too (the one-row count above is
        # what pins the deterministic route: a direct raise records its own
        # class on either route).
        assert failed[0]["error_class"] == "SpawnResultModelError", failed[0]["error_class"]

        # the record names the door and the parameter.
        message = str(node["error_message"])
        assert "spawn_flow" in message, message
        assert "result" in message, message


#: the stub host has no attributes at all, so any read of ``self`` (the pool,
#: the schema, the node) or any I/O raises something other than the refusal.
_UNREGISTERED = "i749_never_registered"


class TestSpawnResultGuardIsFirst:
    """The refusal is the spawn's FIRST act: before the registry lookup,
    any read and any write, on both slot paths (the default cursor and
    ``key=``). An unregistered name proves the order — every later guard
    placement answers ``ChildWorkflowUnknownError`` instead."""

    @pytest.mark.parametrize("key", [None, "k"])
    @pytest.mark.parametrize(
        "result",
        [int, BaseModel, SubReport | None, list[SubReport], "SubReport", SubReport(findings=[])],
        ids=["int", "bare-BaseModel", "optional-model", "list-of-model", "string", "instance"],
    )
    async def test_a_non_model_result_is_refused_before_any_read(
        self, result: object, key: str | None
    ) -> None:
        with pytest.raises(SpawnResultModelError, match=r"spawn_flow.*result="):
            await CtxSpawnOps.spawn_flow(
                cast("CtxSpawnOps", object()), _UNREGISTERED, result=cast("Any", result), key=key
            )

    @pytest.mark.parametrize("key", [None, "k"])
    @pytest.mark.parametrize(
        "result",
        [SubReport, Page[int], RootModel[int], Page],
        ids=["model", "parametrised-generic", "root-model", "unparametrised-generic"],
    )
    async def test_a_model_class_passes_the_guard(self, result: object, key: str | None) -> None:
        with pytest.raises(ChildWorkflowUnknownError):
            await CtxSpawnOps.spawn_flow(
                cast("CtxSpawnOps", object()), _UNREGISTERED, result=cast("Any", result), key=key
            )

    @pytest.mark.parametrize(
        ("result", "shown", "hidden"),
        [
            (SubReport(findings=["i749-canary"]), "an instance of SubReport", "i749-canary"),
            (list[SubReport], "list[", None),
            (int, "<class 'int'>", None),
        ],
        ids=["instance", "alias", "plain-type"],
    )
    async def test_the_message_names_an_instance_by_class_never_by_its_values(
        self, result: object, shown: str, hidden: str | None
    ) -> None:
        """The message lands verbatim in the durable ``error_message`` (job
        row and ledger), outside the capture's redact chain: an instance is
        named by its class, never by its field values; types and aliases
        keep their repr (the useful half of the message)."""
        with pytest.raises(SpawnResultModelError) as exc_info:
            await CtxSpawnOps.spawn_flow(
                cast("CtxSpawnOps", object()), _UNREGISTERED, result=cast("Any", result)
            )
        message = str(exc_info.value)
        assert shown in message, message
        assert hidden is None or hidden not in message, message
        # the remedy survives the loop boundary's 200-char cut (_runner_loop.py).
        assert "pass that class" in message[:200], message


def test_the_refusal_is_a_deterministic_type_error() -> None:
    """The ladder routes the refusal deterministic (one tuple member);
    it stays catchable as ``TypeError``, while plain ``TypeError`` never
    joins the tuple (that would make every body ``TypeError`` terminal)."""
    assert issubclass(SpawnResultModelError, TypeError)
    assert is_deterministic_authoring_failure(SpawnResultModelError("x"))
    assert not is_deterministic_authoring_failure(TypeError("x"))
