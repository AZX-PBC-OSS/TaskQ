"""Worker/loop-scoped workflow dependency authoring and lifecycle pins."""

from __future__ import annotations

import asyncio
import functools
import inspect
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import timedelta
from typing import Protocol, cast, runtime_checkable

import asyncpg
import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import (
    LoopScope,
    ProcessScope,
    _ProviderSystemExitError,
    make_resolver,
)
from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId, JobRow
from taskq.backend._records import _job_row_from_record
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    WorkflowDepsContext,
    WorkflowDepsFactory,
    WorkflowDepsLifecycleError,
    WorkflowDepsProductTypeError,
    WorkflowProviderBridge,
    build,
    chain_source,
    loop,
    map_source,
    step,
)
from taskq.workflows import _worker_execution as seam
from taskq.workflows.api._validate import WorkflowValidationError
from taskq.workflows.chain import Chain, Step, chain_start


class FactoryDeps:
    """A real close-counting loop resource; unexpected repeat close is an error."""

    def __init__(
        self, marker: str = "factory", *, loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        self.marker = marker
        self.loop = loop
        self.worker_id: JobId | None = None
        self.generation = 0
        self.closes = 0
        self.close_loop: asyncio.AbstractEventLoop | None = None

    async def aclose(self) -> None:
        if self.closes:
            raise RuntimeError("FactoryDeps closed twice")
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("FactoryDeps closed from a different loop")
        self.closes += 1
        self.close_loop = asyncio.get_running_loop()


class ConstructedDeps(FactoryDeps):
    def __init__(self, context: WorkflowDepsContext) -> None:
        super().__init__(f"worker:{context.worker_id}", loop=context.loop)


class OtherDeps:
    pass


class WrongProduct:
    def __init__(self) -> None:
        self.closes = 0

    async def aclose(self) -> None:
        self.closes += 1


async def _factory_body(ctx: StepContext, deps: FactoryDeps) -> dict[str, str]:
    return {"marker": deps.marker}


async def _plain_body(ctx: StepContext) -> dict[str, str]:
    return {"marker": "plain"}


class _LoopCarry(BaseModel):
    n: int = 0


_KIND_SEEN: list[FactoryDeps] = []


async def _kind_items(ctx: StepContext) -> list[str]:
    return ["left", "right"]


async def _kind_map_item(ctx: StepContext, item: str, deps: FactoryDeps) -> dict[str, str]:
    _KIND_SEEN.append(deps)
    return {"item": item, "marker": deps.marker}


async def _kind_loop_body(
    ctx: StepContext, carry: _LoopCarry, deps: FactoryDeps
) -> Done[dict[str, str]] | Refine[_LoopCarry]:
    _KIND_SEEN.append(deps)
    if carry.n:
        return Done({"marker": deps.marker})
    return Refine(_LoopCarry(n=1))


async def _kind_chain_body(ctx: StepContext, item: dict[str, object], deps: FactoryDeps) -> object:
    _KIND_SEEN.append(deps)
    return item


_KIND_CHAIN = Chain(
    name="p6-kind-chain",
    start="record",
    steps={"record": Step(body=_kind_chain_body, outcomes=object)},
)


async def _kind_chain_source(ctx: StepContext, deps: FactoryDeps) -> None:
    _KIND_SEEN.append(deps)
    await ctx.emit_batch(
        [chain_start(_KIND_CHAIN, {"marker": deps.marker}, map_index=0, trace_id="p6-kind")],
        cursor={"page": 0},
    )


def _declare(name: str, factory: object, body: object = _factory_body) -> WorkflowApp:
    app = WorkflowApp(deps_factory=cast("WorkflowDepsFactory[object]", factory))

    decorator = app.workflow(name)

    def declaration() -> Promise[object]:
        return build(step(cast("CallableBody", body), key="solo"))

    decorator(declaration)
    return app


# A structural alias used only by tests' heterogeneous body registration helper.
type CallableBody = Callable[..., Awaitable[object]]


def test_deps_faces_are_mutually_exclusive() -> None:
    def make(context: WorkflowDepsContext) -> FactoryDeps:
        return FactoryDeps(loop=context.loop)

    with pytest.raises(ValueError, match=r"deps=.*deps_factory=.*mutually exclusive"):
        WorkflowApp(deps=FactoryDeps(), deps_factory=make)


def test_factory_mismatch_is_refused_without_construction() -> None:
    calls = 0

    async def make_deps(context: WorkflowDepsContext) -> FactoryDeps:
        nonlocal calls
        calls += 1
        return FactoryDeps(loop=context.loop)

    async def wrong_body(ctx: StepContext, deps: OtherDeps) -> dict[str, str]:
        return {"marker": type(deps).__name__}

    app = _declare("p6_factory_mismatch", make_deps, wrong_body)
    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as caught:
        app.get("p6_factory_mismatch")
    assert "OtherDeps" in str(caught.value) and "FactoryDeps" in str(caught.value)
    assert calls == 0


@pytest.mark.parametrize(
    "shape",
    [
        "async",
        "awaitable",
        "acm",
        "decorated",
        "asyncgen",
        "partial",
        "callable",
        "async-callable",
        "class",
    ],
)
def test_factory_shapes_validate_statically_without_construction(shape: str) -> None:
    """Functions, callable objects, constructors and lifecycle annotations share E12."""
    calls = 0

    async def value(context: WorkflowDepsContext) -> FactoryDeps:
        nonlocal calls
        calls += 1
        return FactoryDeps(loop=context.loop)

    def make_awaitable(context: WorkflowDepsContext) -> Awaitable[FactoryDeps]:
        return value(context)

    class ProductContext:
        def __init__(self, context: WorkflowDepsContext) -> None:
            self.context = context

        async def __aenter__(self) -> FactoryDeps:
            return FactoryDeps(loop=self.context.loop)

        async def __aexit__(self, *args: object) -> None:
            return None

    def make_acm(context: WorkflowDepsContext) -> AbstractAsyncContextManager[FactoryDeps]:
        return ProductContext(context)

    @asynccontextmanager  # pyright: ignore[reportDeprecated]  # existing annotation spelling is part of the compatibility test
    async def decorated(context: WorkflowDepsContext) -> AsyncIterator[FactoryDeps]:
        yield FactoryDeps(loop=context.loop)

    async def generator(context: WorkflowDepsContext) -> AsyncGenerator[FactoryDeps, None]:
        yield FactoryDeps(loop=context.loop)

    def plain(context: WorkflowDepsContext, *, marker: str) -> FactoryDeps:
        return FactoryDeps(marker, loop=context.loop)

    class CallableFactory:
        def __call__(self, context: WorkflowDepsContext) -> FactoryDeps:
            return FactoryDeps(loop=context.loop)

    class AsyncCallableFactory:
        async def __call__(self, context: WorkflowDepsContext) -> FactoryDeps:
            return FactoryDeps(loop=context.loop)

    candidates: dict[str, object] = {
        "async": value,
        "awaitable": make_awaitable,
        "acm": make_acm,
        "decorated": decorated,
        "asyncgen": generator,
        "partial": functools.partial(plain, marker="partial"),
        "callable": CallableFactory(),
        "async-callable": AsyncCallableFactory(),
        "class": ConstructedDeps,
    }
    app = _declare(f"p6_shape_{shape}", candidates[shape])
    app.get(f"p6_shape_{shape}")
    assert calls == 0


async def test_sync_async_generator_wrapper_warns_at_build_and_yields_real_product(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    constructions = 0
    products: list[FactoryDeps] = []

    async def actual_generator(context: WorkflowDepsContext) -> AsyncGenerator[FactoryDeps, None]:
        product = FactoryDeps("sync-wrapper", loop=context.loop)
        products.append(product)
        try:
            yield product
        finally:
            await product.aclose()

    def wrapper(context: WorkflowDepsContext) -> AsyncGenerator[FactoryDeps, None]:
        nonlocal constructions
        constructions += 1
        assert not inspect.isasyncgenfunction(wrapper)
        return actual_generator(context)

    app = _declare("p6_sync_asyncgen_wrapper", wrapper)
    compiled = app.get("p6_sync_asyncgen_wrapper")
    assert constructions == 0, "E12 must not construct the factory"
    diagnostics = compiled.validate()
    assert [diagnostic.rule for diagnostic in diagnostics] == ["E12-deps-contract-unverifiable"], (
        diagnostics
    )
    assert all(diagnostic.severity == "warning" for diagnostic in diagnostics)
    assert "sync/wrapper async-generator" in diagnostics[0].message

    lease = await seam._acquire_runner(
        "p6_sync_asyncgen_wrapper",
        wf_pool,
        wf_schema,
        JobId(new_uuid()),
        factory_timeout=1,
    )
    try:
        assert lease.owner is not None
        product = lease.owner.product
        assert isinstance(product, FactoryDeps)
        assert product is products[0]
        assert product.marker == "sync-wrapper"
        assert lease.runner._deps is product
    finally:
        lease.release()
        await seam.aclose_workflow_runners_for_tests()
    assert constructions == 1 and products[0].closes == 1


def test_chain_steps_use_the_same_e12_walk_without_construction() -> None:
    calls = 0

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        nonlocal calls
        calls += 1
        return FactoryDeps(loop=context.loop)

    async def chain_body(ctx: StepContext, item: object, deps: OtherDeps) -> object:
        return item

    chain = Chain(
        name="p6-chain",
        start="chain-step",
        steps={"chain-step": Step(body=chain_body, outcomes=object)},
    )
    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6_chain_e12")
    def declaration() -> Promise[object]:
        return build(chain_source(chain, _plain_body, key="chain-source"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as caught:
        app.get("p6_chain_e12")
    assert "chain-step" in str(caught.value)
    assert calls == 0


def test_unresolvable_and_non_runtime_annotations_have_discrete_warnings() -> None:
    async def unresolved(context: WorkflowDepsContext) -> "MissingAnnotation":  # noqa: UP037,F821  # pyright: ignore[reportUndefinedVariable, reportUnknownParameterType]  # deliberately unresolvable forward reference
        return FactoryDeps(loop=context.loop)  # type: ignore[return-value]

    app = _declare("p6_unresolved_annotation", unresolved)
    compiled = app.get("p6_unresolved_annotation")
    diagnostics = compiled.validate()
    assert [d.rule for d in diagnostics] == ["E12-deps-contract-unverifiable"]
    assert "unresolvable" in diagnostics[0].message or "resolvable" in diagnostics[0].message

    @runtime_checkable
    class Structural(Protocol):
        marker: str

    async def structural_body(ctx: StepContext, deps: Structural) -> dict[str, str]:
        assert isinstance(deps, Structural)  # makes the local annotation namespace available
        return {"marker": deps.marker}

    async def make(context: WorkflowDepsContext) -> FactoryDeps:
        return FactoryDeps(loop=context.loop)

    app2 = _declare("p6_protocol_annotation", make, structural_body)
    diagnostics2 = app2.get("p6_protocol_annotation").validate()
    assert any(d.rule == "E12-deps-contract-unverifiable" for d in diagnostics2)


def _registered_app(name: str, factory: object) -> WorkflowApp:
    return _declare(name, factory)


async def test_factory_is_inert_when_no_registered_body_takes_deps(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls = 0

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        nonlocal calls
        calls += 1
        return FactoryDeps(loop=context.loop)

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6_no_deps_body")
    def declaration() -> Promise[object]:
        return build(step(_plain_body, key="solo"))

    app.get("p6_no_deps_body")
    worker = JobId(new_uuid())
    try:
        first = await seam._acquire_runner(
            "p6_no_deps_body", wf_pool, wf_schema, worker, factory_timeout=1
        )
        second = await seam._acquire_runner(
            "p6_no_deps_body", wf_pool, wf_schema, worker, factory_timeout=1
        )
        assert first.runner is second.runner and first.owner is second.owner is None
        first.release()
        second.release()
    finally:
        await seam.aclose_workflow_runners_for_tests()
    assert calls == 0


async def test_same_loop_creation_is_shielded_single_flight(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    entered = asyncio.Event()
    release_factory = asyncio.Event()
    products: list[FactoryDeps] = []

    async def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(loop=context.loop)
        products.append(product)
        entered.set()
        await release_factory.wait()
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_single_flight", make).get("p6_single_flight")
    try:
        first = asyncio.create_task(
            seam._acquire_runner("p6_single_flight", wf_pool, wf_schema, worker, factory_timeout=1)
        )
        second = asyncio.create_task(
            seam._acquire_runner("p6_single_flight", wf_pool, wf_schema, worker, factory_timeout=1)
        )
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not second.done(), "one waiter's cancellation infected the shared factory"
        release_factory.set()
        lease = await second
        assert len(products) == 1
        same = await seam._acquire_runner(
            "p6_single_flight", wf_pool, wf_schema, worker, factory_timeout=1
        )
        assert same.owner is lease.owner and lease.owner is not None
        assert lease.owner.product is products[0]
        same.release()
        lease.release()
    finally:
        await seam.aclose_workflow_runners_for_tests()
    assert len(products) == 1 and products[0].closes == 1


async def test_cancelled_all_waiters_late_product_remains_owned(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    entered = asyncio.Event()
    release_factory = asyncio.Event()
    produced = asyncio.Event()
    products: list[FactoryDeps] = []

    async def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(loop=context.loop)
        products.append(product)
        entered.set()
        await release_factory.wait()
        produced.set()
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_late_product", make).get("p6_late_product")
    try:
        waiter = asyncio.create_task(
            seam._acquire_runner("p6_late_product", wf_pool, wf_schema, worker, factory_timeout=1)
        )
        await entered.wait()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        release_factory.set()
        await produced.wait()
        lease = await seam._acquire_runner(
            "p6_late_product", wf_pool, wf_schema, worker, factory_timeout=1
        )
        assert len(products) == 1 and lease.owner is not None
        assert lease.owner.product is products[0]
        lease.release()
    finally:
        await seam.aclose_workflow_runners_for_tests()
    assert products[0].closes == 1


async def test_factory_failure_clears_flight_and_next_claim_retries(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls = 0
    expected = LookupError("transient factory failure")
    products: list[FactoryDeps] = []

    async def make(context: WorkflowDepsContext) -> FactoryDeps:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise expected
        product = FactoryDeps(loop=context.loop)
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_factory_retry", make).get("p6_factory_retry")
    try:
        with pytest.raises(LookupError) as caught:
            await seam._acquire_runner(
                "p6_factory_retry", wf_pool, wf_schema, worker, factory_timeout=1
            )
        assert caught.value is expected
        lease = await seam._acquire_runner(
            "p6_factory_retry", wf_pool, wf_schema, worker, factory_timeout=1
        )
        assert calls == 2
        lease.release()
    finally:
        await seam.aclose_workflow_runners_for_tests()
    assert calls == 2 and products[0].closes == 1


async def test_factory_system_exit_uses_existing_attempt_carrier(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    def make(context: WorkflowDepsContext) -> FactoryDeps:
        raise SystemExit(17)

    worker = JobId(new_uuid())
    _registered_app("p6_system_exit", make).get("p6_system_exit")
    try:
        with pytest.raises(_ProviderSystemExitError) as caught:
            await seam._acquire_runner(
                "p6_system_exit", wf_pool, wf_schema, worker, factory_timeout=1
            )
        assert caught.value.original.code == 17
        # The running loop remains available for ordinary unrelated work.
        await asyncio.sleep(0)
    finally:
        await seam.aclose_workflow_runners_for_tests()


async def test_runtime_mismatch_closes_entered_product_then_fails_loudly(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[WrongProduct] = []

    async def lie(context: WorkflowDepsContext) -> object:
        product = WrongProduct()
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_runtime_mismatch", lie).get("p6_runtime_mismatch")
    try:
        with pytest.raises(WorkflowDepsProductTypeError, match=r"WrongProduct.*FactoryDeps"):
            await seam._acquire_runner(
                "p6_runtime_mismatch", wf_pool, wf_schema, worker, factory_timeout=1
            )
    finally:
        await seam.aclose_workflow_runners_for_tests()
    assert len(products) == 1 and products[0].closes == 1


async def test_runtime_awaitable_acm_and_async_generator_products(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []
    exited = 0

    async def awaitable_value(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps("awaitable", loop=context.loop)
        products.append(product)
        return product

    def awaitable_factory(context: WorkflowDepsContext) -> Awaitable[FactoryDeps]:
        return awaitable_value(context)

    class ProductContext:
        def __init__(self, context: WorkflowDepsContext) -> None:
            self.context = context

        async def __aenter__(self) -> FactoryDeps:
            self.product = FactoryDeps("acm", loop=self.context.loop)
            products.append(self.product)
            return self.product

        async def __aexit__(self, *args: object) -> None:
            nonlocal exited
            exited += 1
            await self.product.aclose()

    def acm_factory(context: WorkflowDepsContext) -> AbstractAsyncContextManager[FactoryDeps]:
        return ProductContext(context)

    async def generator_factory(context: WorkflowDepsContext) -> AsyncGenerator[FactoryDeps, None]:
        nonlocal exited
        product = FactoryDeps("generator", loop=context.loop)
        products.append(product)
        try:
            yield product
        finally:
            exited += 1
            await product.aclose()

    worker = JobId(new_uuid())
    factories = (awaitable_factory, acm_factory, generator_factory)
    try:
        for index, factory in enumerate(factories):
            name = f"p6_runtime_shape_{index}"
            _registered_app(name, factory).get(name)
            lease = await seam._acquire_runner(name, wf_pool, wf_schema, worker, factory_timeout=1)
            assert lease.owner is not None and lease.owner.product in products
            lease.release()
    finally:
        await seam.aclose_workflow_runners_for_tests()
    assert [product.closes for product in products] == [1, 1, 1]
    # ACM's exit occurred once; the async-generator product's own close plus its
    # enclosing generator finalization are one scoped exit, not two owners.
    assert exited == 2


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["map", "loop", "chain"])
async def test_fleet_created_static_projection_injects_loop_map_and_chain_bodies(
    kind: str, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """P6A only: this is a runner CREATED by the fleet adapter, not P6B direct lazy injection."""
    products: list[FactoryDeps] = []

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(f"kind-{kind}", loop=context.loop)
        products.append(product)
        return product

    app = WorkflowApp(deps_factory=cast("WorkflowDepsFactory[object]", make))
    name = f"p6_kind_{kind}_runtime"

    if kind == "map":

        @app.workflow(name)
        def declaration() -> Promise[object]:
            source = step(_kind_items, key="items")
            map_source(source, _kind_map_item)
            return build(source)
    elif kind == "loop":

        @app.workflow(name)
        def declaration() -> Promise[object]:
            return build(loop("it", _kind_loop_body, initial=_LoopCarry(), max_iterations=3))
    else:

        @app.workflow(name)
        def declaration() -> Promise[object]:
            return build(chain_source(_KIND_CHAIN, _kind_chain_source, key="source"))

    app.get(name)
    _KIND_SEEN.clear()
    worker = JobId(new_uuid())
    lease = await seam._acquire_runner(name, wf_pool, wf_schema, worker, factory_timeout=1)
    try:
        assert lease.owner is not None and lease.owner.product is products[0]
        assert lease.runner._deps is products[0]
        flow_id = (await lease.runner.create_flow()).flow_id
        assert await lease.runner.drive(flow_id) == "terminal"
        assert len(_KIND_SEEN) >= 2, (kind, _KIND_SEEN)
        assert all(product is products[0] for product in _KIND_SEEN)
    finally:
        lease.release()
        await seam.aclose_workflow_runners_for_tests()
        _KIND_SEEN.clear()
    assert products[0].closes == 1


async def test_two_fresh_loops_same_worker_do_not_evict_or_close_peers(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []
    barrier = threading.Barrier(2)
    worker = JobId(new_uuid())

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(loop=context.loop)
        products.append(product)
        return product

    _registered_app("p6_two_loops", make).get("p6_two_loops")
    errors: list[BaseException] = []

    def one_loop() -> None:
        async def run() -> None:
            lease = await seam._acquire_runner(
                "p6_two_loops", wf_pool, wf_schema, worker, factory_timeout=1
            )
            try:
                barrier.wait(timeout=5)
            finally:
                lease.release()
                await seam.aclose_workflow_runners_for_tests()

        try:
            asyncio.run(run())
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=one_loop) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    try:
        assert not errors
        assert all(not thread.is_alive() for thread in threads)
        assert len(products) == 2
        assert products[0] is not products[1]
        assert products[0].loop is not products[1].loop
        assert [product.closes for product in products] == [1, 1]
        assert [product.close_loop for product in products] == [
            product.loop for product in products
        ]
    finally:
        # Normally the loop-local cleanup above has already removed both cells.
        if not errors and any(thread.is_alive() for thread in threads):
            pytest.fail("a loop is still alive; refusing to close its product from the test loop")


async def test_generation_retirement_waits_for_old_claim_pin(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []
    old_closed = asyncio.Event()

    async def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(f"g{context.generation}", loop=context.loop)
        original_close = product.aclose

        async def observed_close() -> None:
            await original_close()
            if context.generation == 0:
                old_closed.set()

        product.aclose = observed_close  # type: ignore[method-assign]
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_generation_pin", make).get("p6_generation_pin")
    old = await seam._acquire_runner(
        "p6_generation_pin", wf_pool, wf_schema, worker, generation=0, factory_timeout=1
    )
    new = await seam._acquire_runner(
        "p6_generation_pin", wf_pool, wf_schema, worker, generation=1, factory_timeout=1
    )
    try:
        assert old.owner is not None and new.owner is not None and old.owner is not new.owner
        assert products[0].closes == 0, "old product closed underneath its claimed body"
        new.release()
        assert products[0].closes == 0
        old.release()
        await asyncio.wait_for(old_closed.wait(), timeout=1)
        assert products[0].closes == 1 and products[1].closes == 0
    finally:
        if not old.released:
            old.release()
        if not new.released:
            new.release()
        await seam.aclose_workflow_runners_for_tests()
    assert [product.closes for product in products] == [1, 1]


async def test_sixteen_pinned_retirees_refuse_before_retirement_then_drain_and_admit(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []
    first_closed = asyncio.Event()

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(f"g{context.generation}", loop=context.loop)
        if context.generation == 0:
            original_close = product.aclose

            async def observed_close() -> None:
                await original_close()
                first_closed.set()

            product.aclose = observed_close  # type: ignore[method-assign]
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_retiree_cap", make).get("p6_retiree_cap")
    leases: list[seam._AcquiredRunner] = []
    try:
        # Keep every old claim pinned. After gen16 is current, exactly gen0..15
        # are draining, no seventeenth retiree has been admitted.
        for generation in range(17):
            leases.append(
                await seam._acquire_runner(
                    "p6_retiree_cap",
                    wf_pool,
                    wf_schema,
                    worker,
                    generation=generation,
                    factory_timeout=1,
                )
            )
        current = leases[-1].cell.current
        assert current is not None and current.owner is leases[-1].owner
        assert (
            len([entry for entry in leases[-1].cell.retirees if not entry.close_task.done()]) == 16
        )
        with pytest.raises(WorkflowDepsLifecycleError, match=r"16 live retired"):
            await seam._acquire_runner(
                "p6_retiree_cap",
                wf_pool,
                wf_schema,
                worker,
                generation=17,
                factory_timeout=1,
            )
        assert leases[-1].cell.current is current
        assert current.owner is not None and not current.owner.retired
        assert leases[-1].cell.key[4] is asyncio.get_running_loop()

        # Real final release of one pinned old generation drains its resource;
        # admission of gen17 then retires current gen16 without exceeding 16.
        leases[0].release()
        await asyncio.wait_for(first_closed.wait(), timeout=1)
        admitted = await seam._acquire_runner(
            "p6_retiree_cap",
            wf_pool,
            wf_schema,
            worker,
            generation=17,
            factory_timeout=1,
        )
        leases.append(admitted)
        assert admitted.owner is not None and admitted.owner.context.generation == 17
        assert len(products) == 18
    finally:
        for lease in leases:
            if not lease.released:
                lease.release()
        await seam.aclose_workflow_runners_for_tests()
    assert products and [product.closes for product in products] == [1] * len(products)


async def test_timeout_retirees_stay_counted_until_real_release_and_close(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []
    first_closed = asyncio.Event()

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(f"timeout-g{context.generation}", loop=context.loop)
        if context.generation == 0:
            original_close = product.aclose

            async def observed_close() -> None:
                await original_close()
                first_closed.set()

            product.aclose = observed_close  # type: ignore[method-assign]
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_retiree_timeout_cap", make).get("p6_retiree_timeout_cap")
    leases: list[seam._AcquiredRunner] = []
    with capture_logs() as logs:
        try:
            for generation in range(17):
                leases.append(
                    await seam._acquire_runner(
                        "p6_retiree_timeout_cap",
                        wf_pool,
                        wf_schema,
                        worker,
                        generation=generation,
                        factory_timeout=0.001,
                    )
                )
            cell = leases[-1].cell
            current = cell.current
            retired = list(cell.retirees)
            assert len(retired) == 16
            # Fully complete each real bounded drain wait. Its owner is still
            # genuinely pinned, so timeout/done/cancelled is not retirement.
            await asyncio.gather(*(asyncio.shield(entry.close_task) for entry in retired))
            for entry in retired:
                assert entry.close_task.done()
                assert entry.owner.retired and entry.owner.pins == 1
                assert not cell._owner_close_complete(entry.owner)
            assert len(cell.retirees) == 16
            assert current is not None and current.owner is leases[-1].owner
            with pytest.raises(WorkflowDepsLifecycleError, match=r"16 live retired"):
                await seam._acquire_runner(
                    "p6_retiree_timeout_cap",
                    wf_pool,
                    wf_schema,
                    worker,
                    generation=17,
                    factory_timeout=0.001,
                )
            assert cell.current is current and len(products) == 17
            assert current.owner is not None and not current.owner.retired
            timeouts = [
                event
                for event in logs
                if event["event"] == "workflow-deps-retirement-drain-timeout"
            ]
            assert len(timeouts) == 16, timeouts

            # The release path replaces the retained old done wait entry with
            # one actual close. Only then may gen17 consume the freed slot.
            leases[0].release()
            await asyncio.wait_for(first_closed.wait(), timeout=1)
            admitted = await seam._acquire_runner(
                "p6_retiree_timeout_cap",
                wf_pool,
                wf_schema,
                worker,
                generation=17,
                factory_timeout=1,
            )
            leases.append(admitted)
            assert admitted.owner is not None and admitted.owner.context.generation == 17
            assert len(products) == 18
        finally:
            for lease in leases:
                if not lease.released:
                    lease.release()
            await seam.aclose_workflow_runners_for_tests()
    assert products and [product.closes for product in products] == [1] * len(products)


async def test_forty_unpinned_generations_leave_no_retired_or_task_history(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(f"churn-g{context.generation}", loop=context.loop)
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_close_task_history", make).get("p6_close_task_history")
    cell: seam._RunnerCell | None = None
    try:
        for generation in range(40):
            lease = await seam._acquire_runner(
                "p6_close_task_history",
                wf_pool,
                wf_schema,
                worker,
                generation=generation,
                factory_timeout=1,
            )
            cell = lease.cell
            lease.release()
            if generation:
                # These are event-loop callback turns, not a wall-clock wait
                # substituting for resource evidence.
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                assert products[generation - 1].closes == 1
        assert cell is not None
        assert cell.retirees == []
        assert cell.close_tasks == {}
    finally:
        await seam.aclose_workflow_runners_for_tests()
    assert len(products) == 40
    assert [product.closes for product in products] == [1] * 40


@pytest.mark.parametrize("wait_outcome", ["timeout", "cancel"])
async def test_late_release_after_drain_timeout_replaces_only_stale_wait(
    wait_outcome: str, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []
    old_closed = asyncio.Event()

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(f"late-g{context.generation}", loop=context.loop)
        if context.generation == 0:
            original_close = product.aclose

            async def observed_close() -> None:
                await original_close()
                old_closed.set()

            product.aclose = observed_close  # type: ignore[method-assign]
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_late_timeout_release", make).get("p6_late_timeout_release")
    old = await seam._acquire_runner(
        "p6_late_timeout_release",
        wf_pool,
        wf_schema,
        worker,
        generation=0,
        factory_timeout=0.001,
    )
    new = await seam._acquire_runner(
        "p6_late_timeout_release",
        wf_pool,
        wf_schema,
        worker,
        generation=1,
        factory_timeout=1,
    )
    cell = old.cell
    owner = old.owner
    assert owner is not None
    first_wait = cell.close_tasks[id(owner)]
    try:
        if wait_outcome == "timeout":
            # Wait for the real timeout while the old pin remains held.
            await asyncio.shield(first_wait)
        else:
            # Cancelling only the cell's bookkeeping waiter must not complete
            # or detach a genuine owner close.
            first_wait.cancel()
            results = await asyncio.gather(first_wait, return_exceptions=True)
            assert isinstance(results[0], asyncio.CancelledError)
        assert first_wait.done() and owner.retired and owner.pins == 1
        assert not cell._owner_close_complete(owner)
        retained = [entry for entry in cell.retirees if entry.owner is owner]
        assert len(retained) == 1 and retained[0].close_task is first_wait

        old.release()
        retry_wait = cell.close_tasks[id(owner)]
        assert retry_wait is not first_wait
        retained_after = [entry for entry in cell.retirees if entry.owner is owner]
        assert len(retained_after) == 1 and retained_after[0].close_task is retry_wait
        await asyncio.shield(retry_wait)
        await asyncio.wait_for(old_closed.wait(), timeout=1)
        # Run actual callbacks, including any stale callback from first_wait.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert products[0].closes == 1
        assert cell._owner_close_complete(owner)
        assert id(owner) not in cell.close_tasks
        assert not [entry for entry in cell.retirees if entry.owner is owner]
    finally:
        if not old.released:
            old.release()
        if not new.released:
            new.release()
        await seam.aclose_workflow_runners_for_tests()
    assert [product.closes for product in products] == [1, 1]


async def test_sync_reset_logs_a_real_leak_without_fake_close(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[FactoryDeps] = []

    def make(context: WorkflowDepsContext) -> FactoryDeps:
        product = FactoryDeps(loop=context.loop)
        products.append(product)
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_sync_reset", make).get("p6_sync_reset")
    lease = await seam._acquire_runner(
        "p6_sync_reset", wf_pool, wf_schema, worker, factory_timeout=1
    )
    lease.release()
    with capture_logs() as logs:
        seam.reset_app_registry_for_tests()
    assert products[0].closes == 0
    assert any(log["event"] == "workflow-deps-test-reset-leak" for log in logs)
    assert lease.owner is not None
    await lease.owner.aclose()
    assert products[0].closes == 1


class RegisteredResource:
    def __init__(self) -> None:
        self.open = True
        self.closes = 0

    async def aclose(self) -> None:
        if self.closes:
            raise RuntimeError("registered provider was double-closed")
        self.closes += 1
        self.open = False


async def _registered_resource_body(ctx: StepContext, deps: RegisteredResource) -> dict[str, bool]:
    return {"open": deps.open}


async def test_actual_provider_scope_outlives_workflow_product_close(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    events: list[str] = []
    provider = RegisteredResource()
    registry = ProviderRegistry()

    async def registered_factory() -> AsyncGenerator[RegisteredResource, None]:
        events.append("provider-open")
        try:
            yield provider
        finally:
            await provider.aclose()
            events.append("provider-close")

    registry.register_factory(RegisteredResource, Scope.LOOP, registered_factory)
    scopes: dict[Scope, object] = {}
    resolver = make_resolver(registry, scopes)  # type: ignore[arg-type]
    process_scope = ProcessScope(resolver=resolver, factory_timeout=1)
    loop_scope = LoopScope(resolver=resolver, factory_timeout=1)
    scopes[Scope.PROCESS] = process_scope
    scopes[Scope.LOOP] = loop_scope
    await loop_scope.bootstrap(registry, process_scope)
    bridge = WorkflowProviderBridge(
        registry,
        loop_scope=loop_scope,
        process_scope=process_scope,
        thread_scope=None,
    )
    product = FactoryDeps(loop=asyncio.get_running_loop())

    async def make(context: WorkflowDepsContext) -> FactoryDeps:
        borrowed = await context.provider(RegisteredResource)
        assert borrowed is provider and borrowed.open
        original_close = product.aclose

        async def observed_close() -> None:
            assert provider.open, "registered provider closed before its workflow dependent"
            await original_close()
            events.append("workflow-close")

        product.aclose = observed_close  # type: ignore[method-assign]
        return product

    worker = JobId(new_uuid())
    _registered_app("p6_provider_order", make).get("p6_provider_order")
    lease = await seam._acquire_runner(
        "p6_provider_order",
        wf_pool,
        wf_schema,
        worker,
        provider_bridge=bridge,
        factory_timeout=1,
    )
    lease.release()
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as stack:
        stack.push_async_callback(loop_scope.shutdown)
        stack.push_async_callback(seam.close_workflow_runners_bounded)
    assert events == ["provider-open", "workflow-close", "provider-close"], events
    assert product.closes == 1 and provider.closes == 1


async def test_returned_parent_provider_stays_parent_owned(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    provider = RegisteredResource()
    registry = ProviderRegistry()

    async def registered_factory() -> AsyncGenerator[RegisteredResource, None]:
        yield provider
        await provider.aclose()

    registry.register_factory(RegisteredResource, Scope.LOOP, registered_factory)
    scopes: dict[Scope, object] = {}
    resolver = make_resolver(registry, scopes)  # type: ignore[arg-type]
    process_scope = ProcessScope(resolver=resolver, factory_timeout=1)
    loop_scope = LoopScope(resolver=resolver, factory_timeout=1)
    scopes[Scope.PROCESS] = process_scope
    scopes[Scope.LOOP] = loop_scope
    await loop_scope.bootstrap(registry, process_scope)

    async def make(context: WorkflowDepsContext) -> RegisteredResource:
        return await context.provider(RegisteredResource)

    app = _declare("p6_parent_provider", make, _registered_resource_body)
    app.get("p6_parent_provider")
    lease = await seam._acquire_runner(
        "p6_parent_provider",
        wf_pool,
        wf_schema,
        JobId(new_uuid()),
        provider_bridge=WorkflowProviderBridge(
            registry,
            loop_scope=loop_scope,
            process_scope=process_scope,
            thread_scope=None,
        ),
        factory_timeout=1,
    )
    lease.release()
    await seam.aclose_workflow_runners_for_tests()
    assert provider.closes == 0 and provider.open
    await loop_scope.shutdown()
    assert provider.closes == 1 and not provider.open


class _PoolWitness:
    """A fail-closed adapter over a real asyncpg pool, recording ownership."""

    def __init__(self, actual: asyncpg.Pool, label: str) -> None:
        self.actual = actual
        self.label = label
        self.acquire_count = 0
        self.fetch_count = 0

    def acquire(self) -> object:
        # asyncpg acquire() is synchronous and returns its real async CM.
        self.acquire_count += 1
        return self.actual.acquire()

    async def fetchval(self, query: str, *args: object) -> object:
        self.fetch_count += 1
        return await self.actual.fetchval(query, *args)


class _RaceDeps(FactoryDeps):
    def __init__(self, context: WorkflowDepsContext) -> None:
        super().__init__(f"gen{context.generation}", loop=context.loop)
        self.generation = context.generation
        self.pool = context.pool


async def _race_body(ctx: StepContext, deps: _RaceDeps) -> dict[str, object]:
    # This is an actual PostgreSQL round trip through the factory-owned pool,
    # not just a recording of an identity string.
    assert deps.closes == 0, "old dependency closed before its body used it"
    observed = await deps.pool.fetchval("SELECT $1::text", f"body-gen{deps.generation}")
    assert deps.closes == 0, "old dependency closed before its body round trip returned"
    return {"generation": deps.generation, "observed": observed}


@pytest.mark.integration
async def test_midflight_pool_generation_change_never_pins_stale_owner(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    from tests.test_worker_execution_seam import _project_cohorts_into

    gen0_entered = asyncio.Event()
    release_gen0 = asyncio.Event()
    products: list[_RaceDeps] = []
    pool_a = _PoolWitness(wf_pool, "pool-a")
    pool_b_actual = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    pool_b = _PoolWitness(pool_b_actual, "pool-b")

    async def make(context: WorkflowDepsContext) -> _RaceDeps:
        product = _RaceDeps(context)
        products.append(product)
        if context.generation == 0:
            gen0_entered.set()
            await release_gen0.wait()
        return product

    app = WorkflowApp(deps_factory=cast("WorkflowDepsFactory[object]", make))

    @app.workflow("p6_midflight_pool_generation")
    def declaration() -> Promise[object]:
        return build(step(_race_body, key="solo"))

    compiled = app.get("p6_midflight_pool_generation")
    await _project_cohorts_into(wf_conn, wf_schema)
    worker = JobId(new_uuid())
    registered = False
    jobs: list[JobRow] = []
    try:
        flows = []
        for pool in (pool_a, pool_b):
            creator = FlowRunner(compiled, cast("asyncpg.Pool", pool), wf_schema)
            flows.append((await creator.create_flow()).flow_id)
        for _flow_id in flows:
            jobs.append(await _claim_one(wf_conn, wf_schema, worker, register=not registered))
            registered = True

        first = asyncio.create_task(
            seam.execute_flow_job(
                pool=cast("asyncpg.Pool", pool_a),
                schema=wf_schema,
                worker_id=worker,
                job=jobs[0],
                generation=0,
                factory_timeout=1,
            )
        )
        await gen0_entered.wait()
        second = asyncio.create_task(
            seam.execute_flow_job(
                pool=cast("asyncpg.Pool", pool_b),
                schema=wf_schema,
                worker_id=worker,
                job=jobs[1],
                generation=1,
                factory_timeout=1,
            )
        )
        # One scheduler turn lets B complete its bounded node read and join A's
        # still-open flight; the release below is an event, not a timing sleep.
        await asyncio.sleep(0)
        assert not second.done(), "B completed before the gated old construction opened"
        release_gen0.set()
        outcome_a, outcome_b = await asyncio.gather(first, second)
        assert outcome_a.outcome == outcome_b.outcome == "succeeded"
        statuses = [
            await wf_conn.fetchval(
                f'SELECT status FROM "{wf_schema}".jobs WHERE id=$1',  # noqa: S608  # migration/fixture-owned schema
                job.id,
            )
            for job in jobs
        ]
        assert statuses == ["succeeded", "succeeded"]
        assert len(products) == 2
        assert [product.generation for product in products] == [0, 1]
        assert products[0].pool is cast("asyncpg.Pool", pool_a)
        assert products[1].pool is cast("asyncpg.Pool", pool_b)
        assert pool_a.fetch_count == 1 and pool_b.fetch_count == 1
        assert pool_a.acquire_count > 0 and pool_b.acquire_count > 0
        assert not wf_pool.is_closing() and not pool_b_actual.is_closing()
    finally:
        # Products close before either real pool, including the old generation.
        await seam.aclose_workflow_runners_for_tests()
        await pool_b_actual.close()
    assert [product.closes for product in products] == [1, 1]


_REAL_OBSERVATIONS: list[tuple[object, str, int, int]] = []
_REAL_PRODUCTS: list[FactoryDeps] = []


async def _real_fleet_body(ctx: StepContext, deps: FactoryDeps) -> dict[str, str]:
    assert deps.loop is asyncio.get_running_loop()
    _REAL_OBSERVATIONS.append((deps, str(deps.worker_id), deps.generation, id(deps.loop)))
    return {"marker": deps.marker}


async def _claim_one(
    conn: asyncpg.Connection, schema: str, worker_id: JobId, *, register: bool
) -> JobRow:
    from tests.test_worker_execution_seam import _capable_worker_row

    if register:
        await _capable_worker_row(conn, schema, worker_id)
    rows = await dispatch_batch(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=["default"],
        limit_n=1,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert len(rows) == 1, rows
    return _job_row_from_record(rows[0])


@pytest.mark.integration
async def test_real_pg_two_worker_bodies_reuse_only_their_owner_generation(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """Actual claimed rows execute through real workers' execution door."""
    from tests.test_worker_execution_seam import _project_cohorts_into

    class ObservedDeps(FactoryDeps):
        def __init__(self, context: WorkflowDepsContext) -> None:
            super().__init__(f"{context.worker_id}:g{context.generation}", loop=context.loop)
            self.worker_id = context.worker_id
            self.generation = context.generation

    def make(context: WorkflowDepsContext) -> ObservedDeps:
        assert context.pool is wf_pool and context.schema == wf_schema
        product = ObservedDeps(context)
        _REAL_PRODUCTS.append(product)
        return product

    _REAL_OBSERVATIONS.clear()
    _REAL_PRODUCTS.clear()
    app = WorkflowApp(deps_factory=cast("WorkflowDepsFactory[object]", make))

    @app.workflow("p6_real_worker_generation")
    def declaration() -> Promise[object]:
        return build(step(_real_fleet_body, key="solo"))

    compiled = app.get("p6_real_worker_generation")
    direct = FlowRunner(compiled, wf_pool, wf_schema)  # create-only: no direct factory body drive
    await _project_cohorts_into(wf_conn, wf_schema)
    worker_a = JobId(new_uuid())
    worker_b = JobId(new_uuid())
    registered_workers: set[JobId] = set()
    server_version = await wf_conn.fetchval("SHOW server_version")
    print(f"P6_REAL_POSTGRES_VERSION={server_version}")
    try:
        for worker, generation in (
            (worker_a, 0),
            (worker_a, 0),  # one product across distinct claims/runs
            (worker_a, 1),  # applied/explicit invalidation creates the next
            (worker_b, 0),  # real peer worker never inherits the product
        ):
            flow_id = (await direct.create_flow()).flow_id
            job = await _claim_one(
                wf_conn, wf_schema, worker, register=worker not in registered_workers
            )
            registered_workers.add(worker)
            outcome = await seam.execute_flow_job(
                pool=wf_pool,
                schema=wf_schema,
                worker_id=worker,
                job=job,
                generation=generation,
                factory_timeout=1,
            )
            assert outcome.outcome == "succeeded"
            status = await wf_conn.fetchval(
                f'SELECT status FROM "{wf_schema}".jobs WHERE id=$1',  # noqa: S608  # migration-owned fixture schema, never external input
                job.id,
            )
            assert status == "succeeded"
            assert flow_id != job.id
        assert len(_REAL_OBSERVATIONS) == 4
        assert _REAL_OBSERVATIONS[0][0] is _REAL_OBSERVATIONS[1][0]
        assert _REAL_OBSERVATIONS[0][0] is not _REAL_OBSERVATIONS[2][0]
        assert _REAL_OBSERVATIONS[0][0] is not _REAL_OBSERVATIONS[3][0]
        assert [entry[2] for entry in _REAL_OBSERVATIONS] == [0, 0, 1, 0]
        assert [entry[1] for entry in _REAL_OBSERVATIONS] == [
            str(worker_a),
            str(worker_a),
            str(worker_a),
            str(worker_b),
        ]
    finally:
        await seam.aclose_workflow_runners_for_tests()
        _REAL_OBSERVATIONS.clear()
    assert len(_REAL_PRODUCTS) == 3
    assert len({id(product) for product in _REAL_PRODUCTS}) == 3
    assert [product.closes for product in _REAL_PRODUCTS] == [1, 1, 1]
