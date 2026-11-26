"""The spawn's result-contract pins: ``ctx.spawn_flow(result=…)`` must be
a pydantic model class the join's hold can build a JSON schema from, and
the spawn refuses anything else BEFORE any side effect — no child flow
minted, no spawn slot claimed — as a deterministic authoring failure (the
ladder unburned).

The red face on the unrefused tree: ``result=int`` passes the spawn, the
child is created and runs, then ``wait()`` dies on the first use of the
model (``model_json_schema`` for the hold, ``model_validate`` once the
child has succeeded) with an ``AttributeError`` that names neither
``spawn_flow`` nor ``result=``; the retry re-adopts the same child and
burns the whole budget on an error that can never succeed.

The accepted half is proved through the real doors too: a model class
pydantic can schema is spawned, waited on, and decoded (``ChildOk``).
The two failure shapes one step away from ``result=int`` are pinned here:

* a model pydantic validates and serialises but cannot build a JSON
  schema for (an ``arbitrary_types_allowed`` field) — refused up front;
* a model that is not fully defined (an unresolved forward reference) —
  refused up front, the same class of failure one annotation typo away.

The join's decode door is the third: a valid model that cannot decode the
child's terminal result re-raises as ``ChildResultContractError`` so the
deterministic route holds (one failed attempt, never a burned ladder)
instead of spending the budget on pydantic's ``ValidationError``.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, NoReturn, cast

import asyncpg
import pytest
from pydantic import BaseModel, ConfigDict, RootModel, field_serializer, field_validator

from taskq.backend._protocol import JobId
from taskq.workflows import (
    ChildOk,
    ChildResultContractError,
    ChildWorkflowUnknownError,
    Done,
    FlowRunner,
    Promise,
    SpawnResultModelError,
    StepContext,
    WorkflowApp,
    build,
    loop,
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


class OtherOut(BaseModel):
    """A well-formed result contract — for a DIFFERENT child's result."""

    nope: str


class Page[T](BaseModel):
    items: list[T]


class DomainCursor:
    """A domain object a result model embeds (not a JSON-native type)."""

    def __init__(self, v: str) -> None:
        self.v = v


class EmbeddedDomainObjectOut(BaseModel):
    """A result contract pydantic can validate and serialise, but whose JSON
    *schema* pydantic refuses to generate: the field's core schema is an
    ``IsInstanceSchema`` (``arbitrary_types_allowed``), which the schema
    generator cannot describe."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    findings: list[str]
    cursor: DomainCursor

    @field_serializer("cursor")
    def _serialise(self, v: DomainCursor) -> str:
        return v.v

    @field_validator("cursor", mode="before")
    @classmethod
    def _parse(cls, v: Any) -> DomainCursor:
        return v if isinstance(v, DomainCursor) else DomainCursor(str(v))


class PartiallyDefined(BaseModel):
    """A real model class pydantic cannot build a JSON schema for — the
    unresolved forward reference is a mistyped/later-defined annotation, not
    a synthetic input."""

    payload: MissingSymbol  # noqa: F821  # pyright: ignore[reportUndefinedVariable]  # Why: the unresolved name IS the input under test.


@dataclass
class LeakyConfig:
    """A plausible authoring slip: the config object the author meant to name
    a model from. Its repr carries every field value."""

    endpoint: str
    api_key: str


_CANARY = "i749-dur-canary"

#: The guard's first-act witness: the host is inert, so any read before the
#: refusal is harmless and any use (a method call, a query) fails.
_UNREGISTERED = "i749_never_registered"


class _InertSentinel:
    """A stand-in for one of the host's declared fields: attribute reads
    succeed (and return the sentinel), any invocation fails loudly."""

    def __getattr__(self, name: str) -> _InertSentinel:
        return self

    def __call__(self, *args: object, **kwargs: object) -> NoReturn:
        raise AssertionError("the spawn used a host field before refusing the result contract")


class _StubSpawnHost:
    """``_SpawnHost``'s seven declared fields as inert sentinels. A harmless
    ``self.*`` read before the guard costs nothing (a behaviour-preserving
    hoist must not red the ordering pin); the first real use raises."""

    __slots__ = ("_ledger_id", "_pool", "_wsql", "attempt", "flow_id", "job_id", "node_key")

    def __init__(self) -> None:
        sentinel = _InertSentinel()
        self.flow_id = sentinel
        self.job_id = sentinel
        self.node_key = sentinel
        self.attempt = sentinel
        self._pool = sentinel
        self._wsql = sentinel
        self._ledger_id = sentinel


def _refusal_message(name: str, result: object) -> str:
    """The refusal text for *result*, taken from the guard itself."""
    with pytest.raises(SpawnResultModelError) as exc_info:
        asyncio.run(CtxSpawnOps.spawn_flow(_StubSpawnHost(), name, result=result))
    return str(exc_info.value)


def _register_child(app: WorkflowApp, name: str) -> None:
    @app.actor(queue="cpu", max_attempts=1)
    async def leaf(ctx: StepContext) -> LeafOut:
        return LeafOut(n=1)

    workflow_factory = app.workflow(name)

    @workflow_factory
    def child() -> Promise[object]:
        return build(step(leaf, key="leaf"))

    app.get(name)


async def _run_wrong_contract(
    result: object, *, child_name: str, wf_pool: asyncpg.Pool, wf_schema: str
) -> tuple[JobId, str]:
    app = WorkflowApp()
    _register_child(app, child_name)

    @app.actor(queue="cpu", max_attempts=2)
    async def spawner(ctx: StepContext) -> LeafOut:
        handle = await ctx.spawn_flow(child_name, result=result)
        await handle.wait()
        return LeafOut(n=2)

    @app.workflow(f"{child_name}_parent")
    def parent() -> Promise[object]:
        return build(step(spawner, key="wrap"))

    runner = FlowRunner(app.get(f"{child_name}_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
    return flow_id, outcome


@pytest.mark.integration
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
                result=int,  # Why: the wrong-typed contract IS the input under test.
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

    async def test_an_accepted_model_is_usable_at_both_doors(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The accepted half, through the real doors rather than a stub: a
        model class pydantic can schema is spawned, joined by ``wait()``,
        and decoded into ``ChildOk``. A guard that refused a usable model
        would red here; a hold that could not schema it would die at the
        join."""
        child_app = WorkflowApp()

        @child_app.actor(queue="cpu", max_attempts=1)
        async def i749_ok_leaf(ctx: StepContext) -> Page[int]:
            return Page[int](items=[1, 2])

        @child_app.workflow("i749_ok_child")
        def child() -> Promise[object]:
            return build(step(i749_ok_leaf, key="leaf"))

        child_app.get("i749_ok_child")

        parent_app = WorkflowApp()

        @parent_app.actor(queue="cpu", max_attempts=1)
        async def i749_ok_spawner(ctx: StepContext) -> WrapOut:
            handle = await ctx.spawn_flow("i749_ok_child", result=Page[int])
            outcome = await handle.wait()
            assert isinstance(outcome, ChildOk), outcome
            assert outcome.result == Page[int](items=[1, 2]), outcome.result
            return WrapOut(ok=True)

        @parent_app.workflow("i749_ok_parent")
        def parent() -> Promise[object]:
            return build(step(i749_ok_spawner, key="wrap"))

        runner = FlowRunner(parent_app.get("i749_ok_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        node = await wf_conn.fetchrow(
            f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["status"] == "succeeded", (node["status"], node["error_class"])


class TestSpawnResultGuardIsFirst:
    """The refusal is the spawn's FIRST act: before the registry lookup,
    any use of the host and any write, on both slot paths (the default
    cursor and ``key=``). An unregistered name proves the order — every
    later guard placement answers ``ChildWorkflowUnknownError`` instead.

    The unit cases here are sync (``asyncio.run``) so they stay in the
    unit lane: the collection hook's G7 check requests the PG fixtures, and
    it only assigns them to async items."""

    @pytest.mark.parametrize("key", [None, "k"])
    @pytest.mark.parametrize(
        "result",
        [int, BaseModel, SubReport | None, list[SubReport], "SubReport", SubReport(findings=[])],
        ids=["int", "bare-BaseModel", "optional-model", "list-of-model", "string", "instance"],
    )
    def test_a_non_model_result_is_refused_before_any_read(
        self, result: object, key: str | None
    ) -> None:
        with pytest.raises(SpawnResultModelError, match=r"spawn_flow.*result="):
            asyncio.run(
                CtxSpawnOps.spawn_flow(_StubSpawnHost(), _UNREGISTERED, result=result, key=key)
            )

    @pytest.mark.parametrize("key", [None, "k"])
    @pytest.mark.parametrize(
        "result",
        [SubReport, Page[int], RootModel[int], Page],
        ids=["model", "parametrised-generic", "root-model", "unparametrised-generic"],
    )
    def test_a_model_class_passes_the_guard(self, result: object, key: str | None) -> None:
        with pytest.raises(ChildWorkflowUnknownError):
            asyncio.run(
                CtxSpawnOps.spawn_flow(_StubSpawnHost(), _UNREGISTERED, result=result, key=key)
            )

    @pytest.mark.parametrize(
        ("result", "shown", "hidden"),
        [
            (SubReport(findings=["i749-canary"]), "an instance of SubReport", "i749-canary"),
            (
                LeakyConfig(endpoint="https://api.example", api_key=_CANARY),
                "an instance of LeakyConfig",
                _CANARY,
            ),
            ({"api_key": _CANARY}, "an instance of dict", _CANARY),
            (_CANARY, "an instance of str", _CANARY),
            (list[SubReport], "list[", None),
            (int, "<class 'int'>", None),
        ],
        ids=["instance", "dataclass", "dict", "plain-string", "alias", "plain-type"],
    )
    def test_the_message_names_a_value_by_class_never_by_its_values(
        self, result: object, shown: str, hidden: str | None
    ) -> None:
        """The message lands verbatim in the durable ``error_message`` (job
        row and ledger), outside the capture's redact chain: every
        non-type value is named by its class, never by its field values;
        types and aliases keep their repr (the useful half of the message).
        The remedy survives the loop boundary's 200-char cut."""
        message = _refusal_message(_UNREGISTERED, result)
        assert shown in message, message
        assert hidden is None or hidden not in message, message
        assert "pass that class" in message[:200], message

    def test_the_remedy_lands_inside_the_loop_window_for_a_long_name(self) -> None:
        """The remedy's offset moves with the workflow name, and nothing
        bounds a registry key: a 54-character dotted name (the shape a real
        registry carries) must still keep ``pass that class`` inside the
        loop driver's 200-char window."""
        message = _refusal_message(
            "doc_ingest.sub_analysis.children_of_large_documents.v2",
            LeakyConfig(endpoint="https://api.example", api_key=_CANARY),
        )
        assert "pass that class" in message[:200], message

    def test_the_schema_refusal_fits_the_loop_window(self) -> None:
        """The schema guard's message names the class and the exception even
        for a bounded 40-character workflow name: it is read through the same
        200-char loop window as the first guard's refusal."""
        message = _refusal_message(
            "doc_ingest.sub_analysis.children_of_large_documents.v2",
            EmbeddedDomainObjectOut,
        )
        window = message[:200]
        assert "EmbeddedDomainObjectOut" in window, message
        assert "PydanticInvalidForJsonSchema" in window, message
        assert "pass that class" in window, message

    def test_the_offending_value_survives_the_loop_window(self) -> None:
        """The value that tells the author what they passed is not cut
        mid-token: a module-qualified parametrised alias keeps its repr
        inside the same 200-char window."""
        got = repr(list[SubReport])
        message = _refusal_message(_UNREGISTERED, list[SubReport])
        assert got in message[:200], (got, message)


def test_the_refusal_is_a_deterministic_type_error() -> None:
    """The ladder routes the refusal deterministic (one tuple member);
    it stays catchable as ``TypeError``, while plain ``TypeError`` never
    joins the tuple (that would make every body ``TypeError`` terminal)."""
    assert issubclass(SpawnResultModelError, TypeError)
    assert is_deterministic_authoring_failure(SpawnResultModelError("x"))
    assert not is_deterministic_authoring_failure(TypeError("x"))


def test_the_decode_refusal_is_a_deterministic_value_error() -> None:
    """The join's decode refusal re-raises pydantic's ``ValidationError``
    (a ``ValueError``) as a class in the deterministic tuple, so an author
    who declares the wrong result model never burns the retry ladder."""
    assert issubclass(ChildResultContractError, ValueError)
    assert is_deterministic_authoring_failure(ChildResultContractError("x"))
    assert not is_deterministic_authoring_failure(ValueError("x"))


@pytest.mark.integration
class TestSpawnResultContractDoors:
    """The model shapes one step away from ``result=int``: classes the
    predicate accepts but a door cannot use. Each proves the refusal is
    still the spawn's first act (no child, no cursor, one attempt), and the
    decode door's deterministic re-raise."""

    async def test_a_model_the_hold_cannot_schema_is_refused_before_the_child_exists(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """``model_json_schema()`` is the hold's own contract (``wait``
        builds ``payload_schema`` from it, and the admin renders that row),
        so a class pydantic cannot schema is not a result contract: the
        spawn refuses it before the child is minted."""
        flow_id, outcome = await _run_wrong_contract(
            EmbeddedDomainObjectOut,
            child_name="i749c_child",
            wf_pool=wf_pool,
            wf_schema=wf_schema,
        )
        assert outcome == "terminal", outcome

        children = await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        node = await wf_conn.fetchrow(
            f'SELECT status, error_class, error_message, metadata FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        raw_meta = node["metadata"]
        meta = cast(
            "dict[str, Any]", json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
        )
        touched = sorted(k for k in meta if k.startswith("spawn_cursor_") or k == "child_flows")
        failed = await wf_conn.fetch(
            f'SELECT error_class FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )

        assert children == 0, (
            f"the spawn minted {children} child flow(s) — and ran their side effects — "
            "before refusing a result model the hold cannot schema; the node died later "
            f"with {node['error_class']}"
        )
        assert touched == [], f"the spawn wrote {touched} before refusing the model"
        assert len(failed) == 1, (
            f"the refusal burned {len(failed)} attempts "
            f"({[r['error_class'] for r in failed]}) — a result contract the join can "
            "never use is a deterministic authoring failure"
        )
        assert node["error_class"] == "SpawnResultModelError", (
            "the guard accepted a model whose JSON schema the hold cannot build, so the "
            f"record names neither the door nor the parameter: {node['error_class']}: "
            f"{node['error_message']}"
        )
        message = str(node["error_message"])
        assert "EmbeddedDomainObjectOut" in message, message
        assert "PydanticInvalidForJsonSchema" in message, message

    async def test_an_accepted_model_that_cannot_build_a_schema_is_still_refused_up_front(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The unresolved-forward-reference shape (a mistyped annotation, a
        later-defined name, a ``TYPE_CHECKING``-only import): the predicate
        accepts the class, its JSON schema raises ``PydanticUserError``, and
        the spawn must refuse before minting the child."""
        # the precondition this pin rests on: the predicate alone accepts it.
        assert isinstance(PartiallyDefined, type)
        assert issubclass(PartiallyDefined, BaseModel)

        flow_id, outcome = await _run_wrong_contract(
            PartiallyDefined,
            child_name="i749u_child",
            wf_pool=wf_pool,
            wf_schema=wf_schema,
        )
        assert outcome == "terminal", outcome

        children = await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        failed = await wf_conn.fetch(
            f'SELECT error_class FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )
        assert children == 0, (
            "the spawn minted a child flow for a result model that cannot build a "
            f"schema — the shape the guard exists to prevent (minted {children} child flow(s))"
        )
        assert len(failed) == 1, (
            f"the refusal burned {len(failed)} attempts — a schema-less result model "
            f"is the same authoring failure as result=int (classes: "
            f"{[r['error_class'] for r in failed]})"
        )
        assert failed[0]["error_class"] == "SpawnResultModelError", failed[0]["error_class"]

    async def test_a_declared_model_the_child_cannot_satisfy_does_not_burn_the_ladder(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The property the refusal exists to buy — the ladder is not spent
        on a failure that re-running cannot fix — held against the decode
        door. No predicate over the class can close this one: only the join
        knows the child's real result."""
        flow_id, outcome = await _run_wrong_contract(
            OtherOut, child_name="i749d_child", wf_pool=wf_pool, wf_schema=wf_schema
        )
        assert outcome == "terminal", outcome

        children = await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        failed = await wf_conn.fetch(
            f'SELECT error_class, error_message FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )
        assert children == 1, children
        assert len(failed) == 1, (
            f"the join's decode failure burned {len(failed)} attempts "
            f"({[r['error_class'] for r in failed]}) on a failure that re-running cannot "
            "fix — a declared result model the child cannot satisfy repeats verbatim"
        )
        assert failed[0]["error_class"] == "ChildResultContractError", failed[0]["error_class"]
        message = str(failed[0]["error_message"])
        assert "OtherOut" in message, message
        assert "nope" in message, message

    async def test_the_refusal_row_carries_no_secret_from_the_offending_value(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The composition is not academic: the ladder binds the refusal
        text raw into ``jobs.error_message`` and the ledger's terminal row —
        the rows the CLI, the run explorer and the admin job view read."""
        child_app = WorkflowApp()

        @child_app.actor(queue="cpu", max_attempts=1)
        async def dur_leaf(ctx: StepContext) -> LeafOut:
            return LeafOut(n=1)

        @child_app.workflow("dur_child")
        def child() -> Promise[object]:
            return build(step(dur_leaf, key="leaf"))

        child_app.get("dur_child")

        parent_app = WorkflowApp()

        @parent_app.actor(queue="cpu", max_attempts=1)
        async def dur_spawner(ctx: StepContext) -> WrapOut:
            # the author means "the model this config builds" and passes the
            # config — the guard fires before the registry read.
            await ctx.spawn_flow(
                "dur_child",
                result=LeakyConfig(endpoint="https://api.example", api_key=_CANARY),
            )
            return WrapOut(ok=True)

        @parent_app.workflow("dur_parent")
        def parent() -> Promise[object]:
            return build(step(dur_spawner, key="wrap"))

        runner = FlowRunner(parent_app.get("dur_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        node = await wf_conn.fetchrow(
            f'SELECT error_class, error_message FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["error_class"] == "SpawnResultModelError", node["error_class"]
        message = str(node["error_message"])
        # the durable row is the refusal's own text (the premise below): it
        # names the door and the parameter...
        assert "spawn_flow" in message, message
        assert "result" in message, message
        # ...and must carry no field value of the offending object.
        assert _CANARY not in message, message

        ledger = await wf_conn.fetch(
            f'SELECT error_message FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )
        assert len(ledger) == 1, [dict(r) for r in ledger]
        assert _CANARY not in str(ledger[0]["error_message"]), str(ledger[0]["error_message"])

    async def test_the_loop_rows_durable_message_keeps_the_remedy(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The loop body's 200-char slice is real: a 54-character workflow
        name must not push the remedy out of the durable row an operator
        opens."""
        app = WorkflowApp()

        async def dur_loop_body(ctx: StepContext, carry: object) -> object:
            name = "doc_ingest.sub_analysis.children_of_large_documents.v2"
            await ctx.spawn_flow(name, result=int)
            return Done({"unreachable": True})

        @app.workflow("dur_loop_flow")
        def flow() -> Promise[object]:
            return build(loop("spawnloop", dur_loop_body, max_iterations=2))

        runner = FlowRunner(app.get("dur_loop_flow"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        row = await wf_conn.fetchrow(
            f'SELECT error_class, error_message FROM "{wf_schema}".jobs '
            "WHERE step_key = 'spawnloop' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert row is not None
        assert row["error_class"] == "LoopBodyFailure", row["error_class"]
        message = str(row["error_message"])
        # the class half survives the loop's carve-out...
        assert "SpawnResultModelError" in message, message
        # ...and the remedy must too.
        assert "pass that class" in message, message

    async def test_a_loop_bodied_refusal_exhausts_as_loop_body_failure_naming_the_class(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The guide's §3b loop sentence, driven: a spawn inside a loop body
        exhausts the loop as ``LoopBodyFailure`` (the named state
        ``loop_body_failed``) with the refusal's class in the message."""

        async def spawning_body(ctx: StepContext, carry: object) -> object:
            await ctx.spawn_flow(
                "i749_never_registered",
                result=int,  # Why: the wrong-typed contract IS the input under test.
            )
            return Done({"ok": True})  # unreachable: the spawn refuses

        app = WorkflowApp()

        @app.workflow("i749_loop_flow")
        def flow() -> Promise[object]:
            return build(loop("spin", spawning_body, max_iterations=2, initial={"ok": False}))

        runner = FlowRunner(app.get("i749_loop_flow"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id, until="terminal", max_ticks=400) == "terminal"

        row = await wf_conn.fetchrow(
            f'SELECT status, error_class, error_message, metadata FROM "{wf_schema}".jobs '
            "WHERE step_key = 'spin' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert row is not None
        assert row["status"] == "failed", (row["status"], row["error_class"], row["error_message"])
        # the loop's own vocabulary on the record (not the refusal's class) ...
        assert row["error_class"] == "LoopBodyFailure", (
            row["error_class"],
            row["error_message"],
        )
        meta: Any = row["metadata"]
        assert '"loop_body_failed"' in str(meta), meta
        # ... and the class name inside the message, which is what the guide says.
        message = str(row["error_message"])
        assert "SpawnResultModelError" in message, message
