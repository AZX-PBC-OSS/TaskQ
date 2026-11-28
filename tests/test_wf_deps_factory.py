"""The deps FACTORY arm's pins (#719): ``deps_factory=`` at every door.

A static ``deps=`` instance is bound before the runtime loop exists; a
factory is invoked LAZILY, on the running loop, at the first body execution,
and every body of the run receives that one instance. These pins drive the
real ``FlowRunner`` against the pg fixtures (the deps seam's test face — the
in-memory backend has no workflow execution surface) and prove:

* the declaration never invokes the factory (``WorkflowApp``/``app.get`` and
  ``FlowRunner(...)`` are side-effect free), and ``deps=`` + ``deps_factory=``
  together are refused at every door;
* the resolved instance is loop-bound and identical across the step, map,
  route, chain, loop, and escalation body families, with concurrent first
  claims resolving it exactly once;
* every supported factory/teardown shape sets up before the first body and
  tears down exactly once — via ``async with``, ``await runner.aclose()``, or
  the packaged ``run(...)``, on the success and the failure path alike — under
  the single-owner precedence (a context-manager factory's exit closes what it
  yielded; an explicit callback outranks ``aclose()``; the two together are
  refused);
* a failing factory propagates its own cause and is never cached, so the next
  claim retries it;
* E12 reads the factory's PRODUCT TYPE at build: a mismatch names both types
  for every factory shape, a satisfying product (or subtype) passes, and an
  unverifiable annotation skips; E10's arity and the static ``deps=`` arm are
  untouched.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import functools
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol, Self, cast

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.retry import RetryPolicy
from taskq.workflows import (
    ChildOk,
    CompiledWorkflow,
    Done,
    ExecutionPolicy,
    FlowRunner,
    GateDecl,
    Promise,
    Refine,
    RetrySpec,
    StepContext,
    TerminalEffect,
    WorkflowApp,
    WorkflowRunError,
    build,
    chain_source,
    loop,
    map_source,
    route,
    run,
    sink,
    step,
)
from taskq.workflows.api._validate import WorkflowValidationError
from taskq.workflows.chain import DONE, Chain, Route, chain_start
from taskq.workflows.chain import Step as ChainStep

# ── the probe resource and its factories ─────────────────────────────────

_EVENTS: list[str] = []
"""Setup / body / teardown markers, in the order they happened."""

_INSTANCES: list[object] = []
"""The deps instances the bodies observed, in execution order."""

_PRODUCED: list[int] = []
"""Factory invocation count (the laziness and single-init witness)."""

_FAILURES: list[int] = []
"""The flaky factory's remaining forced failures."""


def _reset() -> None:
    _EVENTS.clear()
    _INSTANCES.clear()
    _PRODUCED.clear()
    _PRODUCED.append(0)
    _FAILURES.clear()


class _Probe:
    """The factory-created deps instance: it records the loop it was created
    on and releases through ``aclose()`` (the shape a pool or client has)."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.marker = "probe"

    async def aclose(self) -> None:
        _EVENTS.append("aclose")


class _PlainProbe:
    """A resource with NO teardown surface (the no-op arm)."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.marker = "plain"


class _Other:
    """A DIFFERENT deps type (the E12 mismatch's subject)."""


class _DepsProtocol(Protocol):
    """A non-runtime-checkable protocol (an unverifiable declaration)."""

    async def fetch(self) -> object: ...


def _make_probe() -> _Probe:
    _PRODUCED[0] += 1
    _EVENTS.append("setup")
    return _Probe(asyncio.get_running_loop())


async def _make_probe_async() -> _Probe:
    _PRODUCED[0] += 1
    _EVENTS.append("setup")
    return _Probe(asyncio.get_running_loop())


@contextlib.asynccontextmanager
async def _make_probe_cm() -> AsyncGenerator[_Probe, None]:
    _PRODUCED[0] += 1
    _EVENTS.append("setup")
    try:
        yield _Probe(asyncio.get_running_loop())
    finally:
        _EVENTS.append("exit")


async def _make_probe_agen() -> AsyncGenerator[_Probe, None]:
    _PRODUCED[0] += 1
    _EVENTS.append("setup")
    yield _Probe(asyncio.get_running_loop())
    _EVENTS.append("exit")


def _make_plain() -> _PlainProbe:
    _PRODUCED[0] += 1
    _EVENTS.append("setup")
    return _PlainProbe(asyncio.get_running_loop())


async def _teardown_probe(instance: object) -> None:
    del instance
    _EVENTS.append("teardown")


def _teardown_probe_sync(instance: object) -> None:
    del instance
    _EVENTS.append("teardown")


async def _make_probe_slowly() -> _Probe:
    """Widens the first-claim window: the factory yields the loop several
    times, so a second concurrent body would enter the resolution too."""
    _PRODUCED[0] += 1
    for _ in range(5):
        await asyncio.sleep(0)
    return _Probe(asyncio.get_running_loop())


async def _make_probe_flaky() -> _Probe:
    _PRODUCED[0] += 1
    if _FAILURES[0] > 0:
        _FAILURES[0] -= 1
        raise RuntimeError("deps factory exploded")
    return _Probe(asyncio.get_running_loop())


async def _make_other() -> _Other:
    return _Other()


def _make_probe_awaitable() -> Awaitable[_Probe]:
    """The literal ``() -> Awaitable[T]`` shape."""
    return _make_probe_async()


def _make_probe_cm_annotated() -> AbstractAsyncContextManager[_Probe]:
    """The literal ``() -> AsyncContextManager[T]`` shape (a sync callable
    returning the context manager object)."""
    return _make_probe_cm()


def _make_probe_cm_sync() -> Any:
    """A SYNCHRONOUS factory returning an async context manager (the
    ``() -> AsyncContextManager[T]`` shape)."""
    return _make_probe_cm()


class _ProbeContextManager:
    """A concrete CM annotation: the factory product is what enter yields."""

    async def __aenter__(self) -> _Probe:
        return _Probe(asyncio.get_running_loop())

    async def __aexit__(self, *exc_info: object) -> None:
        del exc_info


def _make_probe_concrete_cm() -> _ProbeContextManager:
    return _ProbeContextManager()


class _OtherContextManager:
    async def __aenter__(self) -> _Other:
        return _Other()

    async def __aexit__(self, *exc_info: object) -> None:
        del exc_info


class _SelfContextManager:
    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        del exc_info


# ── the bodies (one per family) ──────────────────────────────────────────


class _Doc(BaseModel):
    doc_id: str


class _Report(BaseModel):
    ref: str


class _Carry(BaseModel):
    n: int = 0


class _ArmOne(BaseModel):
    tag: str


class _ArmTwo(BaseModel):
    tag: str


class _ChainOutcome(enum.Enum):
    OK = "ok"


async def _probe_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("body")
    _INSTANCES.append(deps)
    return _Report(ref=f"{doc.doc_id}:{deps.marker}")


async def _plain_body(ctx: StepContext, doc: _Doc, deps: _PlainProbe) -> _Report:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("body")
    _INSTANCES.append(deps)
    return _Report(ref=f"{doc.doc_id}:{deps.marker}")


async def _failing_probe_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
    del ctx, doc, deps
    _EVENTS.append("body")
    raise RuntimeError("body exploded")


async def _failing_plain_body(ctx: StepContext, doc: _Doc, deps: _PlainProbe) -> _Report:
    del ctx, doc, deps
    _EVENTS.append("body")
    raise RuntimeError("body exploded")


async def _other_deps_body(ctx: StepContext, doc: _Doc, deps: _Other) -> _Report:
    del ctx, deps
    return _Report(ref=doc.doc_id)  # pragma: no cover - E12 refuses this body


async def _protocol_deps_body(ctx: StepContext, doc: _Doc, deps: _DepsProtocol) -> _Report:
    del ctx, deps
    return _Report(ref=doc.doc_id)  # pragma: no cover - the declaration is uncheckable


async def _items(ctx: StepContext, doc: _Doc) -> list[str]:
    del ctx, doc
    return ["a", "b"]


async def _item_body(ctx: StepContext, item: str, deps: _Probe) -> _Report:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("map")
    _INSTANCES.append(deps)
    return _Report(ref=f"map:{item}")


async def _route_source(ctx: StepContext, doc: _Doc) -> list[_ArmOne | _ArmTwo]:
    del ctx, doc
    return [_ArmOne(tag="one"), _ArmTwo(tag="two")]


async def _arm_one(ctx: StepContext, item: _ArmOne, deps: _Probe) -> _Report:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("route")
    _INSTANCES.append(deps)
    return _Report(ref=f"arm:{item.tag}")


async def _arm_two(ctx: StepContext, item: _ArmTwo, deps: _Probe) -> _Report:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("route")
    _INSTANCES.append(deps)
    return _Report(ref=f"arm:{item.tag}")


async def _chain_leaf(ctx: StepContext, item: dict[str, object], deps: _Probe) -> _ChainOutcome:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("chain")
    _INSTANCES.append(deps)
    del item
    return _ChainOutcome.OK


_CHAIN = Chain(
    name="deps-factory-chain",
    start="chain_leaf",
    steps={
        "chain_leaf": ChainStep(
            body=_chain_leaf,
            outcomes=_ChainOutcome,
            route=Route({_ChainOutcome.OK: DONE}),
        )
    },
)


async def _chain_emit_body(ctx: StepContext, doc: _Doc) -> None:
    """The chain source's body: emit one record, which the chain's leaf step
    executes as its own row (a registered body, no compiled node)."""
    await ctx.emit_batch(
        [chain_start(_CHAIN, {"doc_id": doc.doc_id}, map_index=0, trace_id="chain-0")],
        cursor={"emitted": True},
    )


async def _loop_body(
    ctx: StepContext, carry: _Carry, deps: _Probe
) -> Done[_Report] | Refine[_Carry]:
    del ctx
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("loop")
    _INSTANCES.append(deps)
    return Refine(_Carry(n=carry.n + 1))


async def _escalation_body(
    ctx: StepContext, escalation: dict[str, object], deps: _Probe
) -> _Report:
    del ctx, escalation
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("escalation")
    _INSTANCES.append(deps)
    return _Report(ref="escalated")


# ── the shape matrix ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Shape:
    factory: Callable[..., object]
    teardown: Callable[[object], object] | None
    body: Callable[..., Awaitable[object]]
    failing: Callable[..., Awaitable[object]]
    marker: str
    #: The release marker the shape's teardown emits (``None`` = no surface).
    release: str | None


_SHAPES: dict[str, _Shape] = {
    "sync": _Shape(_make_probe, None, _probe_body, _failing_probe_body, "probe", "aclose"),
    "async": _Shape(_make_probe_async, None, _probe_body, _failing_probe_body, "probe", "aclose"),
    "context-manager": _Shape(
        _make_probe_cm, None, _probe_body, _failing_probe_body, "probe", "exit"
    ),
    "async-generator": _Shape(
        _make_probe_agen, None, _probe_body, _failing_probe_body, "probe", "exit"
    ),
    "explicit-teardown": _Shape(
        _make_probe, _teardown_probe, _probe_body, _failing_probe_body, "probe", "teardown"
    ),
    "sync-teardown": _Shape(
        _make_probe, _teardown_probe_sync, _probe_body, _failing_probe_body, "probe", "teardown"
    ),
    "no-teardown-surface": _Shape(
        _make_plain, None, _plain_body, _failing_plain_body, "plain", None
    ),
}


def _shape_workflow(app: WorkflowApp, shape: str) -> str:
    """Declare the shape's one-step workflow on *app* and return its name."""
    spec = _SHAPES[shape]
    name = f"deps_factory_shape_{shape.replace('-', '_')}"

    @app.workflow(name)
    def wf() -> Promise[object]:
        return build(step(spec.body, _Doc(doc_id="d1"), key="step"))

    return name


def _expected_events(shape: str) -> list[str]:
    release = _SHAPES[shape].release
    return ["setup", "body"] + ([release] if release is not None else [])


# ── the declaration: lazy, mutually exclusive, one teardown owner ────────


def test_compiled_workflow_preserves_existing_positional_fields() -> None:
    """The additive factory binding does not shift the public dataclass's
    existing positional ``input_type=`` field."""
    input_type = object()

    compiled = CompiledWorkflow(
        "positional", {}, (), None, WorkflowApp().channel(), None, input_type
    )

    assert compiled.input_type is input_type
    assert compiled.deps_factory is None


def test_declaration_never_invokes_the_factory() -> None:
    """``WorkflowApp(deps_factory=…)`` and ``app.get(...)`` (compile +
    validate + register) invoke NOTHING — the factory belongs to the running
    loop, not to import time."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe)
    name = _shape_workflow(app, "sync")

    compiled = app.get(name)

    assert _PRODUCED[0] == 0
    assert compiled.deps is None
    assert compiled.deps_factory is not None
    assert _PRODUCED[0] == 0


def test_runner_construction_outside_a_loop_resolves_nothing() -> None:
    """``FlowRunner(...)`` may be built before any event loop exists: the
    declaration records the factory, validation reads only annotations, and
    nothing is invoked (the pool is not touched either until a claim)."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe)
    compiled = app.get(_shape_workflow(app, "sync"))

    # the stub pool stands for a boundary this pin proves is untouched: the
    # constructor reads no binding and opens no connection.
    FlowRunner(compiled, cast("asyncpg.Pool", None), "unmigrated")
    FlowRunner(
        compiled,
        cast("asyncpg.Pool", None),
        "unmigrated",
        deps_factory=_make_probe,
    )

    assert _PRODUCED[0] == 0


def test_declaration_refuses_non_callable_shapes() -> None:
    """The ergonomic refusals at the declaration site: a factory that cannot be
    called and a teardown that cannot release anything."""
    with pytest.raises(TypeError, match="deps_factory= must be callable"):
        WorkflowApp(deps_factory=cast("Callable[..., object]", 42))
    with pytest.raises(TypeError, match="deps_teardown= must be callable"):
        WorkflowApp(deps_factory=_make_probe, deps_teardown=cast("Callable[[object], object]", 7))


def test_teardown_without_a_factory_is_refused() -> None:
    """A static instance is not owned by the seam — ``deps_teardown=`` beside
    ``deps=`` alone would tear down something the caller created."""
    with pytest.raises(ValueError, match="requires deps_factory"):
        WorkflowApp(deps=object(), deps_teardown=_teardown_probe)


def test_context_manager_factory_with_an_explicit_teardown_is_refused() -> None:
    """The async CM / async generator factory owns its own teardown; pairing
    it with a callback is the contradictory dual ownership, refused at the
    declaration."""
    with pytest.raises(ValueError, match="ONE teardown owner"):
        WorkflowApp(deps_factory=_make_probe_cm, deps_teardown=_teardown_probe)
    with pytest.raises(ValueError, match="ONE teardown owner"):
        WorkflowApp(deps_factory=_make_probe_agen, deps_teardown=_teardown_probe)


async def test_deps_and_deps_factory_are_refused_at_every_door(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A door binds ONE deps instance: the static instance and the factory
    together are the declaration-site refusal — on the app, on the runner, and
    through the packaged ``run``."""
    _reset()
    static = _Probe(asyncio.get_running_loop())
    with pytest.raises(ValueError, match="both given"):
        WorkflowApp(deps=static, deps_factory=_make_probe)

    app = WorkflowApp(deps_factory=_make_probe)
    compiled = app.get(_shape_workflow(app, "sync"))
    with pytest.raises(ValueError, match="both given"):
        FlowRunner(compiled, wf_pool, wf_schema, deps=static, deps_factory=_make_probe)
    with pytest.raises(ValueError, match="both given"):
        await run(compiled, wf_pool, wf_schema, deps=static, deps_factory=_make_probe)
    assert _PRODUCED[0] == 0


async def test_plain_factory_producing_a_context_manager_with_teardown_is_refused(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A SYNCHRONOUS factory may return an async context manager; pairing it
    with ``deps_teardown=`` is the same dual ownership — the resolve refuses
    before ``__aenter__`` is reached, so nothing is entered and nothing leaks."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe_cm_sync, deps_teardown=_teardown_probe)
    name = "deps_factory_sync_cm_teardown"

    @app.workflow(name)
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    with pytest.raises(WorkflowRunError, match="ONE teardown owner"):
        await runner.result(flow_id)
    # the factory produced the CM object but ``__aenter__`` never ran — the
    # refusal precedes the enter, so no setup happened and nothing leaks.
    assert _EVENTS == [], _EVENTS


# ── the lifecycle shapes ─────────────────────────────────────────────────


@pytest.mark.parametrize("shape", list(_SHAPES), ids=list(_SHAPES))
async def test_each_shape_sets_up_before_the_body_and_tears_down_once(
    shape: str, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``async with FlowRunner(...)`` owns the lifecycle: setup precedes the
    first body, and the SAME instance is released exactly once on exit —
    through the shape's single teardown owner, never twice."""
    _reset()
    spec = _SHAPES[shape]
    app = WorkflowApp(deps_factory=spec.factory, deps_teardown=spec.teardown)
    runner = FlowRunner(app.get(_shape_workflow(app, shape)), wf_pool, wf_schema)

    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
        assert await runner.result(flow_id) == {"ref": f"d1:{spec.marker}"}
        assert _EVENTS == ["setup", "body"], f"teardown ran before the scope exit: {_EVENTS}"

    assert _PRODUCED[0] == 1
    assert _expected_events(shape) == _EVENTS, _EVENTS
    # idempotent: a repeated close attempts nothing further.
    await runner.aclose()
    assert _expected_events(shape) == _EVENTS, _EVENTS


async def test_a_decorated_async_generator_factory_enters_and_finalizes(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A decorator using ``functools.wraps`` preserves the async-generator
    declaration E12 reads. Resolution must still enter that generator and
    inject its yielded resource rather than injecting the generator itself."""
    _reset()

    @functools.wraps(_make_probe_agen)
    def decorated_factory() -> AsyncGenerator[_Probe, None]:
        return _make_probe_agen()

    app = WorkflowApp(deps_factory=decorated_factory)
    runner = FlowRunner(app.get(_shape_workflow(app, "async-generator")), wf_pool, wf_schema)

    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
        assert await runner.result(flow_id) == {"ref": "d1:probe"}

    assert _EVENTS == ["setup", "body", "exit"]
    assert len(_INSTANCES) == 1 and isinstance(_INSTANCES[0], _Probe)


async def test_aclose_disposes_without_the_context_manager(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``await runner.aclose()`` is the explicit door for callers that do not
    use ``async with`` — the same single teardown attempt."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe_cm)
    runner = FlowRunner(app.get(_shape_workflow(app, "context-manager")), wf_pool, wf_schema)

    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert _EVENTS == ["setup", "body"]

    await runner.aclose()
    await runner.aclose()
    assert _EVENTS == ["setup", "body", "exit"]

    # the torn-down owner never hands the released instance out again: a later
    # run fails loudly instead of resurrecting the factory behind the caller.
    later = (await runner.create_flow()).flow_id
    assert await runner.drive(later) == "terminal"
    with pytest.raises(WorkflowRunError, match="deps owner is closed"):
        await runner.result(later)


@pytest.mark.parametrize("shape", list(_SHAPES), ids=list(_SHAPES))
async def test_teardown_runs_once_when_the_body_fails(
    shape: str, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The failure path releases exactly as the success path does — the
    factory's resource is not stranded by a failed run."""
    _reset()
    spec = _SHAPES[shape]
    app = WorkflowApp(deps_factory=spec.factory, deps_teardown=spec.teardown)
    name = f"deps_factory_failing_{shape.replace('-', '_')}"

    @app.workflow(name)
    def wf() -> Promise[object]:
        return build(step(spec.failing, _Doc(doc_id="d1"), key="step", max_attempts=1))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
        with pytest.raises(WorkflowRunError, match="body exploded"):
            await runner.result(flow_id)

    assert _expected_events(shape) == _EVENTS, _EVENTS


@pytest.mark.parametrize("shape", list(_SHAPES), ids=list(_SHAPES))
async def test_run_door_tears_down_on_success_and_failure(
    shape: str, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The packaged ``run(...)`` door manages its own runner: the resource is
    released when the call returns, success or failure."""
    _reset()
    spec = _SHAPES[shape]
    app = WorkflowApp(deps_factory=spec.factory, deps_teardown=spec.teardown)
    name = _shape_workflow(app, shape)

    result = await run(app.get(name), wf_pool, wf_schema)

    assert result.outcome == "terminal"
    assert result.result == {"ref": f"d1:{spec.marker}"}
    assert _PRODUCED[0] == 1
    assert _expected_events(shape) == _EVENTS, _EVENTS

    # the failure path: run() closes the runner too.
    _reset()
    failing_name = f"deps_factory_run_failing_{shape.replace('-', '_')}"

    @app.workflow(failing_name)
    def failing() -> Promise[object]:
        return build(step(spec.failing, _Doc(doc_id="d1"), key="step", max_attempts=1))

    with pytest.raises(WorkflowRunError, match="body exploded"):
        await run(app.get(failing_name), wf_pool, wf_schema)
    assert _expected_events(shape) == _EVENTS, _EVENTS


# ── one instance, every body family ──────────────────────────────────────


async def test_one_lazy_instance_reaches_step_map_route_and_chain(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A single run resolves the factory once and hands that same instance to
    the step body, the map's item children, both typed-route arms, and the
    chain step (which has no compiled node — its body resolves from the
    registry)."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe)

    @app.workflow("deps_factory_families")
    def families() -> Promise[object]:
        sink(step(_probe_body, _Doc(doc_id="d1"), key="head"))
        items = step(_items, _Doc(doc_id="d1"), key="items_src")
        sink(map_source(items, _item_body))
        routed = route(
            step(_route_source, _Doc(doc_id="d1"), key="route_src"),
            {_ArmOne: _arm_one, _ArmTwo: _arm_two},
        )
        sink(routed)
        return build(chain_source(_CHAIN, _chain_emit_body, _Doc(doc_id="d1"), key="chain_src"))

    runner = FlowRunner(app.get("deps_factory_families"), wf_pool, wf_schema)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id, max_ticks=4000) == "terminal"

    assert _PRODUCED[0] == 1, f"the factory ran {_PRODUCED[0]} times, not once"
    assert {"body", "map", "route", "chain"} <= set(_EVENTS), _EVENTS
    assert len(_INSTANCES) == 1 + 2 + 2 + 1, _EVENTS  # step + map items + route arms + chain
    assert all(instance is _INSTANCES[0] for instance in _INSTANCES)
    assert _EVENTS.count("aclose") == 1


async def test_loop_and_escalation_bodies_share_the_instance(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The loop driver hands the same instance to every iteration, and the
    outbox-drained escalation body receives it too (its row is a registered
    step, not a compiled node)."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe)

    @app.workflow("deps_factory_loop_escalation")
    def wf() -> Promise[object]:
        return build(
            loop(
                "iter",
                _loop_body,
                initial=_Carry(n=0),
                max_iterations=2,
                on_exhausted="escalate",
                escalates_to=_escalation_body,
            )
        )

    runner = FlowRunner(app.get("deps_factory_loop_escalation"), wf_pool, wf_schema)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id, max_ticks=4000) == "terminal"
        # the exhaust's outbox row drains into the escalation consumer's own
        # job; the driver's next claim pass runs it.
        await runner.tick(flow_id)

    assert _PRODUCED[0] == 1, f"the factory ran {_PRODUCED[0]} times, not once"
    assert _EVENTS.count("loop") == 2, _EVENTS
    assert _EVENTS.count("escalation") == 1, _EVENTS
    assert all(instance is _INSTANCES[0] for instance in _INSTANCES)
    assert _EVENTS.count("aclose") == 1, _EVENTS


async def test_hold_wake_reuses_the_lazy_instance(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """A hold wake re-executes with the original lazy instance, not a new resource."""

    class Approval(BaseModel):
        verdict: str

    from taskq.workflows.api._hitl import HitlClient

    _reset()

    async def review(ctx: StepContext, doc: _Doc, deps: _Probe) -> object:
        del doc
        _INSTANCES.append(deps)
        if len(_INSTANCES) == 1:
            return await ctx.wait_signal(Approval, timeout_s=120.0)
        return _Report(ref=f"woke:{deps.marker}")

    app = WorkflowApp(deps_factory=_make_probe)

    @app.workflow("deps_factory_hold_wake")
    def wf() -> Promise[object]:
        return build(
            step(
                review,
                _Doc(doc_id="d1"),
                key="review",
                gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),),
            )
        )

    runner = FlowRunner(app.get("deps_factory_hold_wake"), wf_pool, wf_schema)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id, until="held") == "held"
        holds = await HitlClient(wf_pool, schema=wf_schema).list(flow_id)
        assert len(holds) == 1
        resolved = await HitlClient(wf_pool, schema=wf_schema).resolve(
            holds[0].hold_id, {"verdict": "approve"}
        )
        assert resolved.status == "delivered"
        assert await runner.drive(flow_id) == "terminal"
        assert await runner.result(flow_id) == {"ref": "woke:probe"}

    assert _PRODUCED[0] == 1
    assert len(_INSTANCES) == 2 and _INSTANCES[0] is _INSTANCES[1]
    assert _EVENTS.count("aclose") == 1


async def test_concurrent_first_claims_resolve_once(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """Two runs driven concurrently on the same runner race their first body
    claim; the lock serialises them so the factory runs once and both bodies
    see the same instance."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe_slowly)

    @app.workflow("deps_factory_concurrent")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    runner = FlowRunner(app.get("deps_factory_concurrent"), wf_pool, wf_schema)
    async with runner:
        first = (await runner.create_flow()).flow_id
        second = (await runner.create_flow()).flow_id
        outcomes = await asyncio.gather(runner.drive(first), runner.drive(second))

    assert outcomes == ["terminal", "terminal"]
    assert _PRODUCED[0] == 1, f"concurrent first claims minted {_PRODUCED[0]} instances"
    assert len(_INSTANCES) == 2 and _INSTANCES[0] is _INSTANCES[1]
    assert _EVENTS.count("aclose") == 1


def _declare_child_flows(app: WorkflowApp, *, suffix: str) -> str:
    """A parent whose drive resolves and closes a distinct child runner."""
    child_name = f"deps_factory_child_{suffix}"
    parent_name = f"deps_factory_parent_{suffix}"

    async def child_step(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
        del ctx
        assert deps.loop is asyncio.get_running_loop()
        _EVENTS.append("child")
        _INSTANCES.append(deps)
        return _Report(ref=f"child:{doc.doc_id}")

    @app.workflow(child_name)
    def child() -> Promise[object]:
        return build(step(child_step, _Doc(doc_id="d1"), key="child_step"))

    async def parent_step(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
        assert deps.loop is asyncio.get_running_loop()
        _EVENTS.append("parent")
        _INSTANCES.append(deps)
        handle = await ctx.spawn_flow(child_name, input=doc, result=_Report)
        outcome = await handle.wait()
        assert isinstance(outcome, ChildOk), outcome
        return _Report(ref=f"parent:{doc.doc_id}")

    @app.workflow(parent_name)
    def parent() -> Promise[object]:
        return build(step(parent_step, _Doc(doc_id="d1"), key="parent_step"))

    app.get(child_name)  # the child's compile + registry entry
    return parent_name


async def test_child_flow_runner_owns_and_disposes_its_own_instance(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A spawned child flow executes through its own runner: it resolves its
    own instance (never inherits the parent's), and the parent's ``aclose()``
    disposes the child runner it created during the drive."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe)
    parent_name = _declare_child_flows(app, suffix="shared")
    runner = FlowRunner(app.get(parent_name), wf_pool, wf_schema)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id, max_ticks=4000) == "terminal"

    assert _PRODUCED[0] == 2, f"parent+child runners minted {_PRODUCED[0]} instances"
    # the parent's body re-executes on the child join's wake — with the SAME
    # parent instance; the child's runner resolved its own, distinct one.
    assert len(_INSTANCES) == 3, _EVENTS
    assert _INSTANCES[0] is _INSTANCES[2]
    assert _INSTANCES[0] is not _INSTANCES[1]
    assert _EVENTS.count("aclose") == 2, _EVENTS


async def test_child_teardown_failure_still_closes_the_parent_once(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A child's failing release must not strand the parent's resolved resource."""
    _reset()

    async def failing_teardown(instance: object) -> None:
        del instance
        _EVENTS.append("teardown")
        raise RuntimeError("teardown exploded")

    app = WorkflowApp(deps_factory=_make_probe, deps_teardown=failing_teardown)
    parent_name = _declare_child_flows(app, suffix="failing_teardown")
    runner = FlowRunner(app.get(parent_name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=4000) == "terminal"

    with pytest.raises(RuntimeError, match="teardown exploded"):
        await runner.aclose()

    assert _EVENTS.count("teardown") == 2, _EVENTS
    await runner.aclose()
    assert _EVENTS.count("teardown") == 2, _EVENTS


# ── failure: cause intact, nothing cached ────────────────────────────────


async def test_failing_factory_propagates_its_cause_and_is_retried(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A factory that raises fails the attempt with its own error (the cause
    reaches the run's verdict), caches nothing, and the next claim invokes it
    again — the lock is not wedged and no half-resolved state survives."""
    _reset()
    _FAILURES.append(1)
    app = WorkflowApp(deps_factory=_make_probe_flaky)

    @app.workflow("deps_factory_flaky")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step", max_attempts=1))

    runner = FlowRunner(app.get("deps_factory_flaky"), wf_pool, wf_schema)
    async with runner:
        first = (await runner.create_flow()).flow_id
        assert await runner.drive(first) == "terminal"
        with pytest.raises(WorkflowRunError, match="deps factory exploded"):
            await runner.result(first)

        second = (await runner.create_flow()).flow_id
        assert await runner.drive(second) == "terminal"
        assert await runner.result(second) == {"ref": "d1:probe"}

    assert _PRODUCED[0] == 2, "the failed resolution was cached instead of retried"
    assert len(_INSTANCES) == 1


async def test_failed_context_manager_enter_is_not_exited_and_retries(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A failed enter owns no exit; the next resolution starts afresh and closes once."""
    _reset()
    enters = 0
    exits = 0

    class FlakyContextManager:
        async def __aenter__(self) -> _Probe:
            nonlocal enters
            enters += 1
            if enters == 1:
                raise RuntimeError("enter exploded")
            _EVENTS.append("setup")
            return _Probe(asyncio.get_running_loop())

        async def __aexit__(self, *exc_info: object) -> None:
            nonlocal exits
            del exc_info
            exits += 1
            _EVENTS.append("exit")

    def factory() -> AbstractAsyncContextManager[_Probe]:
        _PRODUCED[0] += 1
        return FlakyContextManager()

    app = WorkflowApp(deps_factory=factory)

    @app.workflow("deps_factory_flaky_enter")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step", max_attempts=1))

    runner = FlowRunner(app.get("deps_factory_flaky_enter"), wf_pool, wf_schema)
    async with runner:
        first = (await runner.create_flow()).flow_id
        assert await runner.drive(first) == "terminal"
        with pytest.raises(WorkflowRunError, match="enter exploded"):
            await runner.result(first)
        assert exits == 0

        second = (await runner.create_flow()).flow_id
        assert await runner.drive(second) == "terminal"
        assert await runner.result(second) == {"ref": "d1:probe"}

    assert _PRODUCED[0] == 2
    assert enters == 2
    assert exits == 1


class _ClassFactory:
    """A class used directly as the factory: calling it produces the deps."""

    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.marker = "class"

    async def aclose(self) -> None:
        _EVENTS.append("aclose")


async def _class_body(ctx: StepContext, doc: _Doc, deps: _ClassFactory) -> _Report:
    del ctx
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EVENTS.append("body")
    _INSTANCES.append(deps)
    return _Report(ref=f"{doc.doc_id}:{deps.marker}")


async def test_a_class_factory_is_a_supported_sync_factory(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A class is a sync callable whose product type is itself — declared
    directly as ``deps_factory=``, resolved lazily and closed once."""
    _reset()
    app = WorkflowApp(deps_factory=_ClassFactory)

    @app.workflow("deps_factory_class")
    def wf() -> Promise[object]:
        return build(step(_class_body, _Doc(doc_id="d1"), key="step"))

    runner = FlowRunner(app.get("deps_factory_class"), wf_pool, wf_schema)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
        assert await runner.result(flow_id) == {"ref": "d1:class"}

    assert len(_INSTANCES) == 1 and isinstance(_INSTANCES[0], _ClassFactory)
    assert _EVENTS == ["body", "aclose"], _EVENTS


# ── E12: the factory's product type at build ─────────────────────────────


@pytest.mark.parametrize("shape", list(_SHAPES), ids=list(_SHAPES))
def test_e12_refuses_a_mismatched_factory_product(shape: str) -> None:
    """The body's declared deps type against the factory's PRODUCT type —
    refused at build, with both type names, for every factory shape — and the
    factory is never invoked to find out."""
    _reset()
    spec = _SHAPES[shape]
    app = WorkflowApp(deps_factory=spec.factory, deps_teardown=spec.teardown)
    name = f"deps_factory_e12_{shape.replace('-', '_')}"

    @app.workflow(name)
    def wf() -> Promise[object]:
        return build(step(_other_deps_body, _Doc(doc_id="d1"), key="step"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get(name)

    message = str(excinfo.value)
    assert "_Other" in message, message
    assert "_Probe" in message or "_PlainProbe" in message, message
    assert _PRODUCED[0] == 0, "E12 invoked the factory instead of reading its annotation"


class _SubProbe(_Probe):
    """A subtype of the declared deps type (the satisfying product)."""


async def _make_sub_probe() -> _SubProbe:
    _PRODUCED[0] += 1
    return _SubProbe(asyncio.get_running_loop())


def test_e12_accepts_a_factory_product_that_satisfies_the_declaration() -> None:
    """A product type that IS (or subtypes) the declared deps type passes at
    build — no instance exists yet, so the check reads the annotation only."""
    _reset()
    app = WorkflowApp(deps_factory=_make_sub_probe)

    compiled = app.get(_shape_workflow(app, "sync"))

    assert compiled.deps_factory is not None
    assert _PRODUCED[0] == 0


def _unannotated_factory() -> Any:
    _PRODUCED[0] += 1
    return _Probe(asyncio.get_running_loop())


def _any_factory() -> Any:
    return _Probe(asyncio.get_running_loop())


def _object_factory() -> object:
    return _Probe(asyncio.get_running_loop())


def _protocol_product_factory() -> _DepsProtocol:
    return cast("_DepsProtocol", _Probe(asyncio.get_running_loop()))


def test_e12_skips_an_unverifiable_body_declaration() -> None:
    """A body declaring a non-runtime-checkable protocol is UNVERIFIABLE —
    the zero-false-positive doctrine skips it rather than rejecting a guess."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe)

    @app.workflow("deps_factory_e12_protocol")
    def wf() -> Promise[object]:
        return build(step(_protocol_deps_body, _Doc(doc_id="d1"), key="step"))

    app.get("deps_factory_e12_protocol")
    assert _PRODUCED[0] == 0


@pytest.mark.parametrize(
    "factory",
    [_unannotated_factory, _any_factory, _object_factory, _protocol_product_factory],
    ids=["unannotated", "any", "object", "non-runtime-checkable-protocol"],
)
def test_e12_skips_an_unverifiable_factory_product(factory: Callable[..., object]) -> None:
    """An unannotated / ``Any`` / ``object`` / non-runtime-checkable protocol
    product cannot prove anything — the build passes and execution decides."""
    _reset()
    app = WorkflowApp(deps_factory=factory)

    app.get(_shape_workflow(app, "sync"))

    assert _PRODUCED[0] == 0


@pytest.mark.parametrize(
    "factory",
    [_make_probe_awaitable, _make_probe_cm_annotated],
    ids=["awaitable", "context-manager"],
)
def test_e12_unwraps_the_documented_product_annotations(
    factory: Callable[..., object],
) -> None:
    """``() -> Awaitable[T]`` and ``() -> AsyncContextManager[T]`` both read as
    ``T`` — a mismatch names the produced instance type, not the wrapper."""
    _reset()
    app = WorkflowApp(deps_factory=factory)

    @app.workflow(f"deps_factory_e12_annotation_{factory.__name__}")
    def wf() -> Promise[object]:
        return build(step(_other_deps_body, _Doc(doc_id="d1"), key="step"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get(f"deps_factory_e12_annotation_{factory.__name__}")
    assert "_Probe" in str(excinfo.value), str(excinfo.value)


@pytest.mark.parametrize(
    "factory",
    [_ProbeContextManager, _make_probe_concrete_cm],
    ids=["class-factory", "annotated-factory-function"],
)
def test_e12_reads_a_concrete_context_managers_enter_product(
    factory: Callable[..., object],
) -> None:
    """A concrete CM class is not the injected deps: its ``__aenter__`` product is."""
    _reset()
    app = WorkflowApp(deps_factory=factory)

    app.get(_shape_workflow(app, "sync"))

    assert _PRODUCED[0] == 0


def test_e12_refuses_a_concrete_context_managers_mismatched_enter_product() -> None:
    """Reading a concrete CM's enter product retains the mismatch refusal."""
    app = WorkflowApp(deps_factory=_OtherContextManager)

    @app.workflow("deps_factory_e12_concrete_cm_mismatch")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get("deps_factory_e12_concrete_cm_mismatch")
    assert "produces '_Other'" in str(excinfo.value), str(excinfo.value)


def test_e12_reads_a_concrete_context_managers_self_product() -> None:
    """``Self`` is a provable enter product, not an unverifiable annotation."""
    app = WorkflowApp(deps_factory=_SelfContextManager)

    @app.workflow("deps_factory_e12_concrete_cm_self")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get("deps_factory_e12_concrete_cm_self")
    assert "produces '_SelfContextManager'" in str(excinfo.value), str(excinfo.value)


def test_e12_reads_a_closure_declared_product_type() -> None:
    """The product type resolves through the declaring scope's CLOSURE too (a
    resource class declared inside the declaring function, under postponed
    annotations)."""
    _reset()

    class LocalDeps:
        def __init__(self) -> None:
            self.marker = "local"

    def make() -> LocalDeps:
        return LocalDeps()

    app = WorkflowApp(deps_factory=make)

    @app.workflow("deps_factory_e12_closure")
    def wf() -> Promise[object]:
        return build(step(_other_deps_body, _Doc(doc_id="d1"), key="step"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get("deps_factory_e12_closure")
    assert "LocalDeps" in str(excinfo.value)


def test_e10_still_owns_the_true_arity_mismatch_beside_a_factory() -> None:
    """Two params beyond the wiring is not the deps shape — E10's refusal is
    untouched by the factory binding."""

    async def too_many(ctx: StepContext, doc: _Doc, a: str, b: str) -> _Report:
        del ctx, a, b
        return _Report(ref=doc.doc_id)  # pragma: no cover - the build refuses

    app = WorkflowApp(deps_factory=_make_probe)

    @app.workflow("deps_factory_e10")
    def wf() -> Promise[object]:
        return build(step(too_many, _Doc(doc_id="d1"), key="step"))

    with pytest.raises(WorkflowValidationError, match="E10-arity"):
        app.get("deps_factory_e10")


async def test_static_deps_arm_is_unchanged() -> None:
    """The static binding keeps its build-time check: a mismatched instance is
    still refused, and a matching one compiles with that same instance bound."""
    orphan = WorkflowApp()

    @orphan.workflow("deps_factory_static_orphan")
    def orphan_wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        orphan.get("deps_factory_static_orphan")

    static = _Probe(asyncio.get_running_loop())
    bound = WorkflowApp(deps=static)

    @bound.workflow("deps_factory_static_bound")
    def bound_wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    compiled = bound.get("deps_factory_static_bound")
    assert compiled.deps is static and compiled.deps_factory is None
    del static


# ── the direct door's override: effective binding, no false exclusion ────


async def test_runner_override_validates_against_the_effective_binding(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``FlowRunner(..., deps_factory=…)`` validates E12 against what the
    bodies will ACTUALLY receive: an override UPGRADES a compile that binds
    nothing (which the door itself would refuse), and a mismatched override is
    refused even when the compile's own binding was fine."""
    _reset()
    app = WorkflowApp()

    @app.workflow("deps_factory_runner_upgrade")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    # the probe seam: compile without the door's E12 raise — this graph binds
    # no deps, and the runner's factory is the effective binding.
    compiled = app._compile("deps_factory_runner_upgrade")  # pyright: ignore[reportPrivateUsage]  # Why: the validator/door's probe seam; this pin supplies the binding the door would have demanded.
    runner = FlowRunner(compiled, wf_pool, wf_schema, deps_factory=_make_probe)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
        assert await runner.result(flow_id) == {"ref": "d1:probe"}
    assert _PRODUCED[0] == 1
    assert _EVENTS.count("aclose") == 1

    # the app's factory satisfies the body; the runner's mismatching factory is
    # the effective binding and is refused.
    bound = WorkflowApp(deps_factory=_make_probe)
    bound_name = _shape_workflow(bound, "sync")
    bound_compiled = bound.get(bound_name)
    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        FlowRunner(bound_compiled, wf_pool, wf_schema, deps_factory=_make_other)
    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        FlowRunner(bound_compiled, wf_pool, wf_schema, deps=_Other())

    # an explicit static instance supersedes a compiled factory cleanly (no
    # false mutual exclusion), and the factory is never invoked.
    _reset()
    static = _Probe(asyncio.get_running_loop())
    runner = FlowRunner(bound_compiled, wf_pool, wf_schema, deps=static)
    async with runner:
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
    assert _PRODUCED[0] == 0
    assert _INSTANCES and _INSTANCES[-1] is static


async def test_run_factory_override_validates_and_supersedes_static_deps(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The packaged door validates and uses its factory, not the compiled static arm."""
    _reset()
    static = _Probe(asyncio.get_running_loop())
    static.marker = "compiled-static"
    app = WorkflowApp(deps=static)

    @app.workflow("deps_factory_run_factory_override")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    compiled = app.get("deps_factory_run_factory_override")
    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        await run(compiled, wf_pool, wf_schema, deps_factory=_make_other)
    assert _PRODUCED[0] == 0

    result = await run(compiled, wf_pool, wf_schema, deps_factory=_make_probe)

    assert result.result == {"ref": "d1:probe"}
    assert _PRODUCED[0] == 1
    assert len(_INSTANCES) == 1 and _INSTANCES[0] is not static
    assert _EVENTS == ["setup", "body", "aclose"]


async def test_run_static_override_supersedes_compiled_factory_without_resolving_it(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A static run override neither resolves nor closes the compiled factory."""
    _reset()
    app = WorkflowApp(deps_factory=_make_probe)

    @app.workflow("deps_factory_run_static_override")
    def wf() -> Promise[object]:
        return build(step(_probe_body, _Doc(doc_id="d1"), key="step"))

    static = _Probe(asyncio.get_running_loop())
    static.marker = "run-static"
    result = await run(app.get("deps_factory_run_static_override"), wf_pool, wf_schema, deps=static)

    assert result.result == {"ref": "d1:run-static"}
    assert _PRODUCED[0] == 0
    assert [static] == _INSTANCES
    assert _EVENTS == ["body"]


# ── the terminal-effect handler's build-time check ───────────────────────


async def _effect_leaf(ctx: StepContext, item: dict[str, object]) -> _ChainOutcome:
    del ctx, item
    return _ChainOutcome.OK


def _effect_chain(effect_body: Callable[..., Awaitable[object]]) -> Chain:
    """The chain whose exhausted step declares a terminal effect — the
    handler is compiled into the workflow under its own step key."""
    return Chain(
        name="deps-factory-effect-chain",
        start="effect_leaf",
        steps={
            "effect_leaf": ChainStep(
                body=_effect_leaf,
                outcomes=_ChainOutcome,
                route=Route({_ChainOutcome.OK: DONE}),
                policy=ExecutionPolicy(
                    retry=RetrySpec(
                        policy=RetryPolicy(max_attempts=1),
                        on_exhaustion=TerminalEffect(
                            body=effect_body,
                            policy=ExecutionPolicy(actor="wf", queue="default"),
                        ),
                    )
                ),
            )
        },
    )


def test_e12_walks_the_terminal_effect_handler() -> None:
    """The terminal-effect handler rides the same E12 walk as every other
    body: a mismatched declaration against the factory's product is refused at
    build. (The handler's RUNTIME row needs the fleet door — the direct door
    refuses guarded runs — but the registration and the check are compile-time
    and shared with the chain-step row path.)"""
    _reset()
    chain = _effect_chain(_other_deps_body)
    app = WorkflowApp(deps_factory=_make_probe)

    @app.workflow("deps_factory_effect_e12")
    def wf() -> Promise[object]:
        return build(chain_source(chain, _chain_emit_body, _Doc(doc_id="d1"), key="effect_src"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get("deps_factory_effect_e12")

    message = str(excinfo.value)
    assert "__on_exhaustion" in message, message
    assert "_Other" in message and "_Probe" in message, message
    assert _PRODUCED[0] == 0
