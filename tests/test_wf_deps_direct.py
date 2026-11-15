"""Direct FlowRunner and packaged-run dependency factory integration pins."""

from __future__ import annotations

import asyncio
import enum
import json
import threading
from collections.abc import AsyncGenerator, Awaitable
from contextlib import asynccontextmanager

import asyncpg
import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import (
    ChildOk,
    Done,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    WorkflowDepsContext,
    WorkflowDepsLifecycleError,
    WorkflowRunError,
    build,
    chain_source,
    gather,
    loop,
    map_source,
    run,
    step,
)
from taskq.workflows._progress import ProgressEmitter
from taskq.workflows._types import FinalizeResult, ForkSpec
from taskq.workflows.chain import Chain, Step, chain_start


class DirectDeps:
    """A real direct-owned resource with strict close accounting."""

    def __init__(self, context: WorkflowDepsContext) -> None:
        self.loop = context.loop
        self.pool = context.pool
        self.schema = context.schema
        self.worker_id = context.worker_id
        self.generation = context.generation
        self.closes = 0

    async def aclose(self) -> None:
        if self.closes:
            raise RuntimeError("DirectDeps closed twice")
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("DirectDeps closed on a peer loop")
        self.closes += 1


async def test_direct_parent_app_factory_executes_body_and_persists_success(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[DirectDeps] = []
    body_products: list[DirectDeps] = []

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    observations: list[tuple[int, int]] = []

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, object]:
        body_products.append(deps)
        observations.append((ctx.attempt, deps.generation))
        observed = await deps.pool.fetchval("SELECT $1::text", deps.schema)
        assert observed == wf_schema
        return {"loop_bound": deps.loop is asyncio.get_running_loop()}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_parent_static")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=1))

    runner = FlowRunner(app.get("p6b_direct_parent_static"), wf_pool, wf_schema)
    flow_ids = []
    try:
        for _ in range(2):
            claim = await runner.create_flow()
            flow_ids.append(claim.flow_id)
            outcome = await runner.drive(claim.flow_id)
            assert outcome == "terminal"
            assert await runner.result(claim.flow_id) == {"loop_bound": True}
        statuses = [
            await wf_pool.fetchval(
                f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608  # fixture-owned schema
                flow_id,
            )
            for flow_id in flow_ids
        ]
        assert statuses == ["succeeded", "succeeded"]
        assert len(products) == 1
        assert body_products == [products[0], products[0]]
        assert observations == [(1, 0), (1, 0)]
        assert products[0].pool is wf_pool and products[0].schema == wf_schema
        assert products[0].worker_id == runner._worker_id
        assert products[0].loop is asyncio.get_running_loop()
        assert products[0].closes == 0
        await asyncio.gather(runner.aclose(), runner.aclose())
    finally:
        await runner.aclose()
    assert products[0].closes == 1
    assert not wf_pool.is_closing()

    for call in (
        lambda: runner.create_flow(),
        lambda: runner.drive(flow_ids[0]),
        lambda: runner.tick(flow_ids[0]),
        lambda: runner.result(flow_ids[0]),
        lambda: runner.cancel_workflow(flow_ids[0]),
    ):
        with pytest.raises(WorkflowDepsLifecycleError, match=r"closed|belongs"):
            await call()


async def test_direct_map_and_loop_bodies_receive_the_same_lazy_product(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    class Carry(BaseModel):
        n: int = 0

    products: list[DirectDeps] = []
    seen: list[tuple[str, object, DirectDeps]] = []

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def items(ctx: StepContext) -> list[str]:
        return ["a", "b"]

    async def item(ctx: StepContext, value: str, deps: DirectDeps) -> dict[str, str]:
        seen.append(("map", ctx.map_index, deps))
        return {"item": value}

    async def iterate(
        ctx: StepContext, carry: Carry, deps: DirectDeps
    ) -> Done[dict[str, object]] | Refine[Carry]:
        seen.append(("loop", carry.n, deps))
        if carry.n:
            return Done({"iterations": carry.n + 1})
        return Refine(Carry(n=1))

    for kind in ("map", "loop"):
        seen.clear()
        app = WorkflowApp(deps_factory=make)
        name = f"p6b_direct_{kind}_body"
        if kind == "map":

            @app.workflow(name)
            def declaration() -> Promise[object]:
                source = step(items, key="items")
                map_source(source, item)
                return build(source)
        else:

            @app.workflow(name)
            def declaration() -> Promise[object]:
                return build(loop("iterate", iterate, initial=Carry(), max_iterations=3))

        runner = FlowRunner(app.get(name), wf_pool, wf_schema)
        try:
            flow_id = (await runner.create_flow()).flow_id
            assert await runner.drive(flow_id) == "terminal"
            assert (
                await wf_pool.fetchval(
                    f'SELECT status FROM "{wf_schema}".jobs WHERE id=$1',  # noqa: S608  # fixture-owned schema
                    flow_id,
                )
                == "succeeded"
            )
            expected_calls = 2
            assert [entry[0] for entry in seen] == [kind] * expected_calls
            assert all(entry[2] is products[-1] for entry in seen)
            if kind == "loop":
                assert (await runner.result(flow_id))["iterations"] == 2  # type: ignore[index]
        finally:
            await runner.aclose()
    assert len(products) == 2
    assert [product.closes for product in products] == [1, 1]


class _Outcome(enum.Enum):
    OK = "ok"


async def test_direct_chain_source_and_step_retry_keep_product_and_records(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[DirectDeps] = []
    source_observations: list[tuple[int, DirectDeps]] = []
    step_products: list[DirectDeps] = []

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def chain_step_body(
        ctx: StepContext, item: dict[str, object], deps: DirectDeps
    ) -> dict[str, object]:
        step_products.append(deps)
        return item

    chain = Chain(
        name="p6b-direct-retry-chain",
        start="record",
        steps={"record": Step(body=chain_step_body, outcomes=object)},
    )

    async def source(ctx: StepContext, deps: DirectDeps) -> None:
        source_observations.append((ctx.attempt, deps))
        if len(source_observations) == 1:
            raise RuntimeError("first source attempt is a genuine failure")
        await ctx.emit_batch(
            [
                chain_start(
                    chain,
                    {"record": 7},
                    map_index=7,
                    trace_id="p6b-direct-retry",
                )
            ],
            cursor={"page": 0},
        )

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_chain_retry")
    def declaration() -> Promise[object]:
        return build(chain_source(chain, source, key="source", max_attempts=2))

    runner = FlowRunner(app.get("p6b_direct_chain_retry"), wf_pool, wf_schema)
    try:
        flow_id = (await runner.create_flow()).flow_id
        await runner.tick(flow_id)
        first = await wf_conn.fetchrow(
            f'SELECT status, attempt, metadata FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='source'",
            flow_id,
        )
        assert first is not None
        assert (first["status"], first["attempt"]) == ("scheduled", 1)
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET scheduled_at=now() '  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='source'",
            flow_id,
        )
        assert await runner.drive(flow_id) == "terminal"
        children = await wf_conn.fetch(
            f'SELECT map_index, status, idempotency_key FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
            "WHERE parent_id = (SELECT id FROM "  # parent source
            f"\"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid=$1 "
            "AND step_key='source') ORDER BY map_index",
            flow_id,
        )
        assert [(row["map_index"], row["status"]) for row in children] == [(7, "succeeded")]
        assert len({row["idempotency_key"] for row in children}) == 1
        assert [attempt for attempt, _product in source_observations] == [1, 2]
        assert all(product is products[0] for _attempt, product in source_observations)
        assert step_products == [products[0]]
        metadata_raw: object = first["metadata"]
        metadata = json.loads(metadata_raw) if isinstance(metadata_raw, str) else metadata_raw
        assert isinstance(metadata, dict) and metadata["wf_source_logical_attempt"] == 2
    finally:
        await runner.aclose()
    assert len(products) == 1 and products[0].closes == 1


@pytest.mark.parametrize("shape", ["raw", "awaitable", "acm", "asyncgen", "wrapped"])
async def test_direct_factory_shapes_execute_and_close_once(
    shape: str, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[DirectDeps] = []
    exits = 0

    def product_for(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def awaitable_value(context: WorkflowDepsContext) -> DirectDeps:
        return product_for(context)

    def awaitable_factory(context: WorkflowDepsContext) -> Awaitable[DirectDeps]:
        return awaitable_value(context)

    @asynccontextmanager
    async def acm_factory(context: WorkflowDepsContext) -> AsyncGenerator[DirectDeps, None]:
        nonlocal exits
        product = product_for(context)
        try:
            yield product
        finally:
            exits += 1
            await product.aclose()

    async def generator_factory(context: WorkflowDepsContext) -> AsyncGenerator[DirectDeps, None]:
        nonlocal exits
        product = product_for(context)
        try:
            yield product
        finally:
            exits += 1
            await product.aclose()

    def wrapped_factory(context: WorkflowDepsContext) -> AsyncGenerator[DirectDeps, None]:
        return generator_factory(context)

    def raw_factory(context: WorkflowDepsContext) -> DirectDeps:
        return product_for(context)

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, int]:
        return {"generation": deps.generation}

    factories = {
        "raw": raw_factory,
        "awaitable": awaitable_factory,
        "acm": acm_factory,
        "asyncgen": generator_factory,
        "wrapped": wrapped_factory,
    }
    app = WorkflowApp(deps_factory=factories[shape])
    name = f"p6b_direct_shape_{shape}"

    @app.workflow(name)
    def declaration() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=1))

    result = await run(app.get(name), wf_pool, wf_schema)
    assert result.outcome == "terminal" and result.result == {"generation": 0}
    assert len(products) == 1 and products[0].closes == 1
    assert exits == (1 if shape in {"acm", "asyncgen", "wrapped"} else 0)


async def test_direct_failure_retry_and_exhaustion_record_factory_error_class(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls = 0
    products: list[DirectDeps] = []
    bodies = 0

    class FactoryDeclarationError(RuntimeError):
        pass

    def make(context: WorkflowDepsContext) -> DirectDeps:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FactoryDeclarationError("factory declaration failed before its product")
        product = DirectDeps(context)
        products.append(product)
        return product

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, int]:
        nonlocal bodies
        bodies += 1
        return {"calls": len(products)}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_factory_retry")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=2))

    runner = FlowRunner(app.get("p6b_direct_factory_retry"), wf_pool, wf_schema)
    try:
        flow_id = (await runner.create_flow()).flow_id
        await runner.tick(flow_id)
        failed = await wf_conn.fetchrow(
            f'SELECT status, attempt FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='solo'",
            flow_id,
        )
        failed_ledger = await wf_conn.fetchrow(
            f'SELECT status, error_class, error_message FROM "{wf_schema}".wf_step_ledger '  # noqa: S608  # fixture-owned schema
            "WHERE flow_id=$1 AND step_key='solo' AND attempt=1",
            flow_id,
        )
        assert failed is not None
        assert failed["status"] == "scheduled" and failed["attempt"] == 1
        assert failed_ledger is not None and failed_ledger["status"] == "failed"
        assert failed_ledger["error_class"] == "FactoryDeclarationError"
        assert "factory declaration failed" in failed_ledger["error_message"]
        assert bodies == 0 and not products
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET scheduled_at=now() '  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='solo'",
            flow_id,
        )
        await runner.tick(flow_id)
        assert await runner.drive(flow_id) == "terminal"
        assert await runner.result(flow_id) == {"calls": 1}
        assert calls == 2 and bodies == 1
    finally:
        await runner.aclose()
    assert len(products) == 1 and products[0].closes == 1

    # Force the already-used factory closed semantics away from this separate
    # declaration: its only call fails explicitly, not via a swallowed body key.
    def always_fail(context: WorkflowDepsContext) -> DirectDeps:
        raise FactoryDeclarationError("exhausted factory")

    exhausted_app = WorkflowApp(deps_factory=always_fail)

    @exhausted_app.workflow("p6b_direct_factory_exhaustion_real")
    def real_exhausted_declaration() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=1))

    exhausted_runner = FlowRunner(
        exhausted_app.get("p6b_direct_factory_exhaustion_real"), wf_pool, wf_schema
    )
    try:
        exhausted = await exhausted_runner.create_flow()
        assert await exhausted_runner.drive(exhausted.flow_id) == "terminal"
        exhausted_row = await wf_conn.fetchrow(
            f'SELECT error_class, attempt FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='solo'",
            exhausted.flow_id,
        )
        assert exhausted_row is not None
        assert exhausted_row["error_class"] == "FactoryDeclarationError"
        assert exhausted_row["attempt"] == 1
    finally:
        await exhausted_runner.aclose()


async def test_direct_provider_requirement_is_typed_and_static_override_wins(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    body_calls = 0
    factory_calls = 0

    def requiring_worker(context: WorkflowDepsContext) -> DirectDeps:
        context.require_providers()
        raise AssertionError("require_providers returned without a direct registry")

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, bool]:
        nonlocal body_calls
        body_calls += 1
        return {"same_loop": deps.loop is asyncio.get_running_loop()}

    app = WorkflowApp(deps_factory=requiring_worker)

    @app.workflow("p6b_direct_provider_refusal")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=1))

    provider_runner = FlowRunner(app.get("p6b_direct_provider_refusal"), wf_pool, wf_schema)
    try:
        claim = await provider_runner.create_flow()
        assert await provider_runner.drive(claim.flow_id) == "terminal"
        row = await wf_conn.fetchrow(
            f'SELECT error_class, error_message FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='solo'",
            claim.flow_id,
        )
        assert row is not None
        assert row["error_class"] == "WorkflowDepsLifecycleError"
        assert "ProviderRegistry" in row["error_message"]
        assert "direct FlowRunner" in row["error_message"]
        assert body_calls == 0
    finally:
        await provider_runner.aclose()

    static = DirectDeps(
        WorkflowDepsContext(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=JobId(new_uuid()),
            generation=9,
            loop=asyncio.get_running_loop(),
        )
    )

    def never_call_factory(context: WorkflowDepsContext) -> DirectDeps:
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("explicit deps must clear the compiled factory before validation")

    factory_app = WorkflowApp(deps_factory=never_call_factory)

    @factory_app.workflow("p6b_direct_static_override")
    def override_declaration() -> Promise[object]:
        return build(step(body, key="solo"))

    compiled = factory_app.get("p6b_direct_static_override")
    runner = FlowRunner(compiled, wf_pool, wf_schema, deps=static)
    try:
        assert runner.compiled.deps is static and runner.compiled.deps_factory is None
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
        assert body_calls == 1 and factory_calls == 0
        assert static.closes == 0, "the unchanged static face remains caller-owned"
    finally:
        await runner.aclose()
    assert static.closes == 0


async def test_direct_close_waits_for_body_and_cancellation_preserves_claim_state(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[DirectDeps] = []
    body_started = asyncio.Event()
    release_body = asyncio.Event()

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, int]:
        body_started.set()
        await release_body.wait()
        assert deps.closes == 0, "root runner close cut through the active body pin"
        return {"closed": deps.closes}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_close_body")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo"))

    runner = FlowRunner(app.get("p6b_direct_close_body"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    drive_task = asyncio.create_task(runner.drive(flow_id))
    await asyncio.wait_for(body_started.wait(), 2)
    close_task = asyncio.create_task(runner.aclose())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not close_task.done()
    assert products[0].closes == 0
    release_body.set()
    assert await asyncio.wait_for(drive_task, 2) == "terminal"
    await asyncio.wait_for(close_task, 2)
    assert products[0].closes == 1
    assert (
        await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id=$1',  # noqa: S608  # fixture-owned schema
            flow_id,
        )
        == "succeeded"
    )

    second_started = asyncio.Event()
    never_finish = asyncio.Event()

    async def cancelling_body(ctx: StepContext, deps: DirectDeps) -> dict[str, int]:
        second_started.set()
        await never_finish.wait()
        raise AssertionError("cancelled body resumed without its release")

    cancelling_app = WorkflowApp(deps_factory=make)

    @cancelling_app.workflow("p6b_direct_body_cancel")
    def cancelling_declaration() -> Promise[object]:
        return build(step(cancelling_body, key="solo"))

    cancelling_runner = FlowRunner(cancelling_app.get("p6b_direct_body_cancel"), wf_pool, wf_schema)
    cancelling_flow = (await cancelling_runner.create_flow()).flow_id
    tick = asyncio.create_task(cancelling_runner.tick(cancelling_flow))
    await asyncio.wait_for(second_started.wait(), 2)
    tick.cancel()
    with pytest.raises(asyncio.CancelledError):
        await tick
    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='solo'",
        cancelling_flow,
    )
    assert row is not None and (row["status"], row["attempt"]) == ("running", 1)
    assert row["error_class"] is None
    assert products[1].closes == 0, "cancellation must not close under the old body frame"
    await cancelling_runner.aclose()
    assert products[1].closes == 1


async def test_packaged_setup_and_close_cancellation_close_late_product_once(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    class ClosingDeps(DirectDeps):
        def __init__(self, context: WorkflowDepsContext) -> None:
            super().__init__(context)
            self.close_started = asyncio.Event()
            self.allow_close = asyncio.Event()

        async def aclose(self) -> None:
            self.close_started.set()
            await self.allow_close.wait()
            await super().aclose()

    products: list[ClosingDeps] = []
    setup_started = asyncio.Event()
    setup_release = asyncio.Event()
    body_calls = 0

    async def make(context: WorkflowDepsContext) -> ClosingDeps:
        product = ClosingDeps(context)
        products.append(product)
        setup_started.set()
        await setup_release.wait()
        return product

    async def body(ctx: StepContext, deps: ClosingDeps) -> dict[str, int]:
        nonlocal body_calls
        body_calls += 1
        return {"closes": deps.closes}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_setup_cancel")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo"))

    task = asyncio.create_task(run(app.get("p6b_direct_setup_cancel"), wf_pool, wf_schema))
    await asyncio.wait_for(setup_started.wait(), 2)
    task.cancel()
    setup_release.set()
    products[0].allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert body_calls == 0
    assert len(products) == 1 and products[0].closes == 1
    assert [
        task.done() for task in asyncio.all_tasks() if task.get_name() == "workflow-deps-close"
    ] == []

    # A completed entry now exercises cancellation of the close waiter itself.
    async def immediate(context: WorkflowDepsContext) -> ClosingDeps:
        product = ClosingDeps(context)
        products.append(product)
        return product

    closing_app = WorkflowApp(deps_factory=immediate)

    @closing_app.workflow("p6b_direct_close_cancel")
    def closing_declaration() -> Promise[object]:
        return build(step(body, key="solo"))

    runner = FlowRunner(closing_app.get("p6b_direct_close_cancel"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    close = asyncio.create_task(runner.aclose())
    await asyncio.wait_for(products[1].close_started.wait(), 2)
    close.cancel()
    products[1].allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await close
    await runner.aclose()
    assert products[1].closes == 1


async def test_two_direct_runners_and_cross_loop_calls_never_share_or_close_peers(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    worker = JobId(new_uuid())
    products: list[DirectDeps] = []

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, str]:
        return {"worker": str(deps.worker_id)}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_runner_isolation")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo"))

    compiled = app.get("p6b_direct_runner_isolation")
    first = FlowRunner(compiled, wf_pool, wf_schema, worker_id=worker)
    second = FlowRunner(compiled, wf_pool, wf_schema, worker_id=worker)
    try:
        for runner in (first, second):
            flow_id = (await runner.create_flow()).flow_id
            assert await runner.drive(flow_id) == "terminal"
        assert len(products) == 2 and products[0] is not products[1]
        await first.aclose()
        assert [product.closes for product in products] == [1, 0]
        third_flow = (await second.create_flow()).flow_id
        assert await second.drive(third_flow) == "terminal"
        assert len(products) == 2 and products[1].closes == 0
    finally:
        await first.aclose()
        await second.aclose()
    assert [product.closes for product in products] == [1, 1]

    cross_loop_errors: list[BaseException] = []

    def peer_loop() -> None:
        async def reject() -> None:
            try:
                await second.create_flow()
            except BaseException as exc:
                cross_loop_errors.append(exc)
            else:
                raise AssertionError("a bound direct runner accepted a peer-loop create")
            try:
                await second.aclose()
            except WorkflowDepsLifecycleError as exc:
                cross_loop_errors.append(exc)
            else:
                raise AssertionError("peer-loop close was accepted")

        asyncio.run(reject())

    await asyncio.to_thread(peer_loop)
    assert len(cross_loop_errors) == 2
    assert all(isinstance(exc, WorkflowDepsLifecycleError) for exc in cross_loop_errors)
    assert all("loop" in str(exc) for exc in cross_loop_errors)


async def test_same_worker_on_two_real_loops_owns_loop_local_products(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    products: list[DirectDeps] = []
    barrier = threading.Barrier(2)
    worker = JobId(new_uuid())
    server_version = await wf_conn.fetchval("SHOW server_version")
    print(f"P6B_REAL_POSTGRES_VERSION={server_version}")

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, object]:
        return {"loop": id(deps.loop), "generation": deps.generation}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_two_real_loops")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo"))

    compiled = app.get("p6b_direct_two_real_loops")

    def one_loop() -> None:
        async def execute() -> None:
            pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=0)
            runner = FlowRunner(compiled, pool, wf_schema, worker_id=worker)
            try:
                flow_id = (await runner.create_flow()).flow_id
                assert await runner.drive(flow_id) == "terminal"
                barrier.wait(timeout=10)
                owned = [
                    product for product in products if product.loop is asyncio.get_running_loop()
                ]
                assert len(owned) == 1 and owned[0].closes == 0
                status = await pool.fetchval(
                    f'SELECT status FROM "{wf_schema}".jobs WHERE id=$1',  # noqa: S608  # fixture-owned schema
                    flow_id,
                )
                assert status == "succeeded"
            finally:
                await runner.aclose()
                await pool.close()

        asyncio.run(execute())
        barrier.wait(timeout=10)

    await asyncio.gather(asyncio.to_thread(one_loop), asyncio.to_thread(one_loop))
    assert len(products) == 2
    assert products[0] is not products[1]
    assert products[0].loop is not products[1].loop
    assert [product.worker_id for product in products] == [worker, worker]
    assert [product.generation for product in products] == [0, 0]
    assert [product.closes for product in products] == [1, 1]


class _ChildReport(BaseModel):
    value: int


async def test_direct_child_shares_family_and_child_close_preserves_parent(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[DirectDeps] = []
    observations: list[tuple[str, DirectDeps]] = []

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    app = WorkflowApp(deps_factory=make)

    async def child_body(ctx: StepContext, deps: DirectDeps) -> _ChildReport:
        observations.append(("child", deps))
        return _ChildReport(value=7)

    @app.workflow("p6b_direct_child")
    def child_declaration() -> Promise[_ChildReport]:
        return build(step(child_body, key="child"))

    async def parent_body(ctx: StepContext, deps: DirectDeps) -> dict[str, int]:
        observations.append(("parent", deps))
        handle = await ctx.spawn_flow("p6b_direct_child", input=None, result=_ChildReport)
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=report):
                return {"child": report.value}
            case _:
                pass
        raise AssertionError(f"direct child did not succeed: {outcome!r}")

    @app.workflow("p6b_direct_parent")
    def parent_declaration() -> Promise[object]:
        return build(step(parent_body, key="parent", max_attempts=3))

    app.get("p6b_direct_child")
    runner = FlowRunner(app.get("p6b_direct_parent"), wf_pool, wf_schema)
    try:
        first_flow = (await runner.create_flow()).flow_id
        assert await runner.drive(first_flow) == "terminal"
        assert await runner.result(first_flow) == {"child": 7}
        assert len(runner._child_runners) == 1
        child = next(iter(runner._child_runners.values()))
        await child.aclose()
        assert products[0].closes == 0
        with pytest.raises(WorkflowDepsLifecycleError, match="closed"):
            await child.tick(first_flow)
        # Closing that child did not make the active root runner unusable.
        assert await runner.result(first_flow) == {"child": 7}
        second_flow = (await runner.create_flow()).flow_id
        assert await runner.drive(second_flow) == "terminal"
        assert await runner.result(second_flow) == {"child": 7}
        assert len(products) == 1
        assert observations
        assert all(product is products[0] for _owner, product in observations)
    finally:
        await runner.aclose()
    assert len(products) == 1 and products[0].closes == 1

    # Construction and explicit close alone remain declaration-only.
    unused = FlowRunner(runner.compiled, wf_pool, wf_schema)
    await unused.aclose()
    assert len(products) == 1


async def test_direct_source_park_resume_reuses_open_owner_and_exact_records(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    from tests.test_wf_emit_parking_controls import (
        _await_park_or_settled,
        _await_source_status,
        _chain,
        _children,
        _park_is_live,
        _source_body,
        _source_state,
    )

    products: list[DirectDeps] = []
    source_attempts: list[tuple[int, int]] = []
    source_products: list[DirectDeps] = []
    child_records: list[int | None] = []
    gate = asyncio.Event()
    child_zero_started = asyncio.Event()
    child_one_started = asyncio.Event()
    page_zero_emitted = asyncio.Event()

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def screen(ctx: StepContext, item: object) -> _Outcome:
        child_records.append(ctx.map_index)
        if ctx.map_index == 0:
            child_zero_started.set()
        elif ctx.map_index == 1:
            child_one_started.set()
        await gate.wait()
        return _Outcome.OK

    chain = _chain("p6b-parking-chain", screen)
    inner_source = _source_body(
        chain,
        ((0, 1), (2, 3)),
        source_attempts,
        page_zero_emitted=page_zero_emitted,
    )

    async def source(ctx: StepContext, deps: DirectDeps) -> None:
        source_products.append(deps)
        assert deps.closes == 0
        await inner_source(ctx)

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_parking", max_in_flight=2)
    def declaration() -> Promise[object]:
        return build(chain_source(chain, source, key="source", max_attempts=4))

    compiled = app.get("p6b_direct_parking")
    source_runner = FlowRunner(compiled, wf_pool, wf_schema)
    child_zero = FlowRunner(compiled, wf_pool, wf_schema)
    child_one = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await source_runner.create_flow()).flow_id
    source_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='source'",
        flow_id,
    )
    assert source_id is not None
    source_task = asyncio.create_task(source_runner.tick(flow_id))
    child_tasks: list[asyncio.Task[bool]] = []
    try:
        await asyncio.wait_for(page_zero_emitted.wait(), 5)
        child_tasks.append(asyncio.create_task(child_zero.tick(flow_id)))
        await asyncio.wait_for(child_zero_started.wait(), 5)
        child_tasks.append(asyncio.create_task(child_one.tick(flow_id)))
        await asyncio.wait_for(child_one_started.wait(), 5)
        parked = await _await_park_or_settled(wf_conn, wf_schema, flow_id, source_id, "source")
        assert _park_is_live(parked), parked
        await asyncio.wait_for(source_task, 5)
        assert products[0].closes == 0
        snapshot_attempts = list(source_attempts)
        assert await source_runner.tick(flow_id) is False
        assert list(source_attempts) == snapshot_attempts
        gate.set()
        await asyncio.wait_for(asyncio.gather(*child_tasks), 5)
        woken = await _await_source_status(
            wf_conn,
            wf_schema,
            flow_id,
            source_id,
            "source",
            lambda state: state["status"] == "pending",
            deadline_s=5,
        )
        assert woken["blocking_reason"] is None
        assert await source_runner.drive(flow_id, max_ticks=250) == "terminal"
        final_source = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
        children = await _children(wf_conn, wf_schema, source_id, "screen")
        assert source_attempts == [(0, 1), (1, 1), (1, 1)]
        assert final_source["status"] == "succeeded"
        assert final_source["attempt"] == 1
        assert final_source["failed_ledger"] == 0
        assert [row["map_index"] for row in children] == [0, 1, 2, 3]
        assert all(row["status"] == "succeeded" for row in children)
        assert len({row["idempotency_key"] for row in children}) == 4
        assert sorted(child_records) == [0, 1, 2, 3]
        assert len(products) == 1
        assert source_products and all(product is products[0] for product in source_products)
    finally:
        gate.set()
        for task in (source_task, *child_tasks):
            if not task.done():
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        await source_runner.aclose()
        await child_zero.aclose()
        await child_one.aclose()
    assert len(products) == 1 and products[0].closes == 1


async def test_direct_concurrent_claims_single_flight_and_mismatch_close_once(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[DirectDeps] = []
    setup_started = asyncio.Event()
    setup_release = asyncio.Event()
    setup_calls = 0
    body_products: list[tuple[str, DirectDeps]] = []

    async def make(context: WorkflowDepsContext) -> DirectDeps:
        nonlocal setup_calls
        setup_calls += 1
        product = DirectDeps(context)
        products.append(product)
        setup_started.set()
        await setup_release.wait()
        return product

    async def left(ctx: StepContext, deps: DirectDeps) -> dict[str, str]:
        body_products.append(("left", deps))
        return {"side": "left"}

    async def right(ctx: StepContext, deps: DirectDeps) -> dict[str, str]:
        body_products.append(("right", deps))
        return {"side": "right"}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_concurrent")
    def declaration() -> Promise[object]:
        return build(
            gather(
                [
                    step(left, key="left"),
                    step(right, key="right"),
                ]
            )
        )

    runner = FlowRunner(app.get("p6b_direct_concurrent"), wf_pool, wf_schema)
    try:
        flow_id = (await runner.create_flow()).flow_id
        attempts = [
            asyncio.create_task(runner.tick(flow_id)),
            asyncio.create_task(runner.tick(flow_id)),
        ]
        await asyncio.wait_for(setup_started.wait(), 2)
        await asyncio.sleep(0)
        assert setup_calls == 1 and len(products) == 1
        setup_release.set()
        await asyncio.gather(*attempts)
        assert await runner.drive(flow_id) == "terminal"
        assert {name for name, _product in body_products} == {"left", "right"}
        assert all(product is products[0] for _name, product in body_products)
        assert setup_calls == 1
        statuses = await wf_conn.fetch(
            f'SELECT step_key, status FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key IN ('left','right')",
            flow_id,
        )
        assert sorted((row["step_key"], row["status"]) for row in statuses) == [
            ("left", "succeeded"),
            ("right", "succeeded"),
        ]
    finally:
        await runner.aclose()
    assert products[0].closes == 1

    class WrongProduct:
        def __init__(self) -> None:
            self.closes = 0

        async def aclose(self) -> None:
            if self.closes:
                raise RuntimeError("wrong product closed twice")
            self.closes += 1

    wrong_products: list[WrongProduct] = []

    async def wrong_generator(
        context: WorkflowDepsContext,
    ) -> AsyncGenerator[WrongProduct, None]:
        product = WrongProduct()
        wrong_products.append(product)
        try:
            yield product
        finally:
            await product.aclose()

    # The concrete product is checked at runtime without invoking a second
    # factory. Packaged run() then performs its own finally-close without
    # closing this already-entered-and-rejected resource again.
    def wrong_factory(context: WorkflowDepsContext) -> AsyncGenerator[WrongProduct, None]:
        return wrong_generator(context)

    wrong_app = WorkflowApp(deps_factory=wrong_factory)

    @wrong_app.workflow("p6b_direct_wrong_product")
    def wrong_declaration() -> Promise[object]:
        return build(step(left, key="left", max_attempts=1))

    with pytest.raises(WorkflowRunError, match="WorkflowDepsProductTypeError"):
        await run(wrong_app.get("p6b_direct_wrong_product"), wf_pool, wf_schema)
    assert len(wrong_products) == 1 and wrong_products[0].closes == 1


async def test_direct_cached_replay_does_not_reconstruct_or_recall_body(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    products: list[DirectDeps] = []
    body_calls = 0

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def cached(ctx: StepContext, value: int, deps: DirectDeps) -> dict[str, int]:
        nonlocal body_calls
        body_calls += 1
        return {"value": value, "generation": deps.generation}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_cache_replay")
    def declaration() -> Promise[object]:
        return build(step(cached, 11, key="cached", cache=True))

    runner = FlowRunner(app.get("p6b_direct_cache_replay"), wf_pool, wf_schema)
    try:
        first = (await runner.create_flow()).flow_id
        assert await runner.drive(first) == "terminal"
        assert await runner.result(first) == {"value": 11, "generation": 0}
        second = (await runner.create_flow()).flow_id
        assert await runner.drive(second) == "terminal"
        assert await runner.result(second) == {"value": 11, "generation": 0}
        receipt_raw: object = await wf_conn.fetchval(
            f"SELECT metadata->'wf_cache_hit' FROM \"{wf_schema}\".jobs "  # noqa: S608  # fixture-owned schema
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='cached'",
            second,
        )
        receipt = json.loads(receipt_raw) if isinstance(receipt_raw, str) else receipt_raw
        assert body_calls == 1 and len(products) == 1
        assert isinstance(receipt, dict) and receipt["run_id"] == str(first)
    finally:
        await runner.aclose()
    assert products[0].closes == 1


async def test_direct_finalizer_close_order_and_cancel_unwind_keep_owner_safe(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.workflows.api._runner import FlowRunner as Runner

    products: list[DirectDeps] = []
    finalizer_started = asyncio.Event()
    allow_finalizer = asyncio.Event()
    finalizer_committed = 0

    def make(context: WorkflowDepsContext) -> DirectDeps:
        product = DirectDeps(context)
        products.append(product)
        return product

    async def body(ctx: StepContext, deps: DirectDeps) -> dict[str, int]:
        return {"closes": deps.closes}

    app = WorkflowApp(deps_factory=make)

    @app.workflow("p6b_direct_finalizer_close")
    def declaration() -> Promise[object]:
        return build(step(body, key="solo"))

    original = Runner._finalize_success

    async def gated_finalize(
        self: Runner,
        flow_id: JobId,
        row: dict[str, object],
        attempt: int,
        node: object,
        result: dict[str, object] | None,
        emitter: ProgressEmitter | None = None,
        *,
        claim_epoch: int = 0,
        prebuilt_route_fork: ForkSpec | None = None,
        run_priority: int | None = None,
        ledger_id: JobId | None = None,
        ledger_claimed_at: object = None,
    ) -> FinalizeResult:
        nonlocal finalizer_committed
        finalizer_started.set()
        await allow_finalizer.wait()
        finalized = await original(
            self,
            flow_id,
            row,
            attempt,
            node,
            result,
            emitter=emitter,
            claim_epoch=claim_epoch,
            prebuilt_route_fork=prebuilt_route_fork,
            run_priority=run_priority,
            ledger_id=ledger_id,
            ledger_claimed_at=ledger_claimed_at,
        )
        finalizer_committed += 1
        return finalized

    monkeypatch.setattr(Runner, "_finalize_success", gated_finalize)
    runner = FlowRunner(app.get("p6b_direct_finalizer_close"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    drive_task = asyncio.create_task(runner.drive(flow_id))
    await asyncio.wait_for(finalizer_started.wait(), 2)
    close_task = asyncio.create_task(runner.aclose())
    await asyncio.sleep(0)
    assert products[0].closes == 0 and not close_task.done()
    allow_finalizer.set()
    assert await asyncio.wait_for(drive_task, 2) == "terminal"
    await asyncio.wait_for(close_task, 2)
    assert finalizer_committed == 1
    assert products[0].closes == 1
    assert (
        await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id=$1',  # noqa: S608  # fixture-owned schema
            flow_id,
        )
        == "succeeded"
    )

    # A second real claim is cancelled inside its finalizer. The transaction
    # unwinds claim-owned rather than fabricating failure/terminal success, and
    # the direct product stays open until explicit owner close.
    finalizer_started.clear()
    allow_finalizer.clear()
    cancelling_runner = FlowRunner(runner.compiled, wf_pool, wf_schema)
    cancelling_flow = (await cancelling_runner.create_flow()).flow_id
    cancelling_drive = asyncio.create_task(cancelling_runner.drive(cancelling_flow))
    await asyncio.wait_for(finalizer_started.wait(), 2)
    cancelling_drive.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelling_drive
    cancelling_row = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '  # noqa: S608  # fixture-owned schema
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='solo'",
        cancelling_flow,
    )
    assert cancelling_row is not None
    assert (cancelling_row["status"], cancelling_row["attempt"]) == ("running", 1)
    assert cancelling_row["error_class"] is None
    assert finalizer_committed == 1 and products[1].closes == 0
    await cancelling_runner.aclose()
    assert products[1].closes == 1


async def test_direct_close_failure_is_logged_and_other_child_still_closes(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    trace: list[str] = []

    class BadClose(DirectDeps):
        def __init__(self, context: WorkflowDepsContext) -> None:
            super().__init__(context)
            self.close_attempts = 0

        async def aclose(self) -> None:
            self.close_attempts += 1
            trace.append("parent-close-attempt")
            raise RuntimeError("synthetic close failure remains observable")

    def bad_factory(context: WorkflowDepsContext) -> BadClose:
        return BadClose(context)

    def good_factory(context: WorkflowDepsContext) -> DirectDeps:
        return DirectDeps(context)

    child_app = WorkflowApp(deps_factory=good_factory)

    async def child_body(ctx: StepContext, deps: DirectDeps) -> _ChildReport:
        return _ChildReport(value=deps.generation)

    @child_app.workflow("p6b_direct_close_child")
    def child_declaration() -> Promise[_ChildReport]:
        return build(step(child_body, key="child"))

    parent_app = WorkflowApp(deps_factory=bad_factory)

    async def parent_body(ctx: StepContext, deps: BadClose) -> dict[str, int]:
        handle = await ctx.spawn_flow("p6b_direct_close_child", input=None, result=_ChildReport)
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=report):
                return {"generation": report.value + deps.generation}
            case _:
                pass
        raise AssertionError(f"child close fixture did not succeed: {outcome!r}")

    @parent_app.workflow("p6b_direct_close_parent")
    def parent_declaration() -> Promise[object]:
        return build(step(parent_body, key="parent"))

    child_app.get("p6b_direct_close_child")
    runner = FlowRunner(parent_app.get("p6b_direct_close_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"generation": 0}
    parent_product = runner._deps
    assert isinstance(parent_product, BadClose)
    with capture_logs() as logs:
        await runner.aclose()
    direct = runner._deps_direct
    assert direct is not None
    child_products = [
        product
        for owner in direct.family.owners.values()
        for product in (owner.product,)
        if type(product) is DirectDeps
    ]
    assert len(child_products) == 1 and child_products[0].closes == 1
    assert parent_product.close_attempts == 1
    assert set(trace) == {"parent-close-attempt"}
    failures = [entry for entry in logs if entry.get("event") == "workflow-deps-close-failed"]
    assert len(failures) == 1
    assert failures[0]["error_class"] == "RuntimeError"
    assert "synthetic close failure" in failures[0]["error"]
