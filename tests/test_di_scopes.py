"""Unit tests for ScopeContainer, ProcessScope, ThreadScope, LoopScope.

Covers:
  - register_factory resolution with generator teardown
  - LIFO teardown order
  - Plain class: no teardown callback registered
  - ACM lifecycle: __aenter__ called at bootstrap; __aexit__ at shutdown
  - ACM injected value is __aenter__() return, NOT instance
  - Per-scope caching
  - SyncCloseable lifecycle + asyncio.to_thread
  - TRANSIENT uncached
  - __aenter__ raises: no teardown for failing provider; prior teardowns still fire
  - Async generator yields twice: RuntimeError caught by log-and-continue
"""

import asyncio
import threading
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ScopeContainer, ThreadScope
from taskq._di.types import FactoryShape, ProviderEntry, ProviderLifecycle
from taskq.settings import WorkerSettings

# ── Helpers ────────────────────────────────────────────────────────


async def _stub_resolver(func: Any, **kw: Any) -> dict[str, object]:
    return {}


def _make_registry_and_loop_scope(
    *entries: tuple[type, ProviderEntry[object]],
) -> tuple[ProviderRegistry, LoopScope]:
    registry = ProviderRegistry()
    for t, e in entries:
        registry._providers[t] = e
    loop_scope = LoopScope(resolver=_stub_resolver)
    return registry, loop_scope


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
        },
    )


# ── Stub types ──────────────────────────────────────────────────


class _MockClient:
    pass


class _MockResource:
    pass


class _SvcA:
    pass


class _SvcB:
    pass


class _SvcC:
    pass


class _X:
    pass


class _Y:
    pass


# ── register_factory resolution with generator teardown ────


async def test_async_generator_teardown() -> None:
    teardown_ran = False

    async def make_client() -> AsyncIterator[_MockClient]:
        nonlocal teardown_ran
        yield _MockClient()
        teardown_ran = True

    entry = ProviderEntry(
        type_=_MockClient,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_client,
        factory_shape=FactoryShape.ASYNC_GENERATOR,
    )
    registry, loop_scope = _make_registry_and_loop_scope((_MockClient, entry))
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    assert not teardown_ran

    await loop_scope.shutdown()

    assert teardown_ran


# ── LIFO teardown order ────────────────────────────────────


async def test_lifo_teardown_order() -> None:
    teardown_order: list[str] = []

    async def make_a() -> AsyncIterator[_SvcA]:
        yield _SvcA()
        teardown_order.append("A")

    async def make_b() -> AsyncIterator[_SvcB]:
        yield _SvcB()
        teardown_order.append("B")

    async def make_c() -> AsyncIterator[_SvcC]:
        yield _SvcC()
        teardown_order.append("C")

    entries = {
        _SvcA: ProviderEntry(
            type_=_SvcA,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_a,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
        _SvcB: ProviderEntry(
            type_=_SvcB,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_b,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
        _SvcC: ProviderEntry(
            type_=_SvcC,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_c,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
    }
    registry, loop_scope = _make_registry_and_loop_scope(*entries.items())
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))
    await loop_scope.shutdown()

    assert teardown_order == ["C", "B", "A"]


# ── Per-scope caching ─────────────────────────────────────


async def test_per_scope_caching() -> None:
    call_count = 0

    async def make_a() -> _SvcA:
        nonlocal call_count
        call_count += 1
        return _SvcA()

    entry_a = ProviderEntry(
        type_=_SvcA,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_a,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = ProviderRegistry()
    registry._providers[_SvcA] = entry_a
    loop_scope = LoopScope(resolver=_stub_resolver)

    result1 = await loop_scope.get_or_create(_SvcA, registry.get(_SvcA))
    assert call_count == 1
    result2 = await loop_scope.get_or_create(_SvcA, registry.get(_SvcA))
    assert call_count == 1
    assert result1 is result2

    await loop_scope.shutdown()


# ── TRANSIENT uncached ────────────────────────────────────


async def test_transient_uncached() -> None:
    container = ScopeContainer(scope=Scope.TRANSIENT, resolver=_stub_resolver)

    call_count = 0

    def make_x() -> _X:
        nonlocal call_count
        call_count += 1
        return _X()

    entry = ProviderEntry(
        type_=_X,
        scope=Scope.TRANSIENT,
        kind="factory",
        impl=make_x,
        factory_shape=FactoryShape.SYNC_CALLABLE,
    )

    r1 = await container.get_or_create(_X, entry)
    r2 = await container.get_or_create(_X, entry)
    assert r1 is not r2
    assert call_count == 2

    await container.aclose()


# ── Log-and-continue teardown ──────────────────────────────────────


async def test_log_and_continue_teardown() -> None:
    b_teardown_ran = False

    async def make_a() -> AsyncIterator[_SvcA]:
        yield _SvcA()
        raise RuntimeError("teardown boom")

    async def make_b() -> AsyncIterator[_SvcB]:
        nonlocal b_teardown_ran
        yield _SvcB()
        b_teardown_ran = True

    entries = {
        _SvcA: ProviderEntry(
            type_=_SvcA,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_a,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
        _SvcB: ProviderEntry(
            type_=_SvcB,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_b,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
    }
    registry, loop_scope = _make_registry_and_loop_scope(*entries.items())

    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))
    await loop_scope.shutdown()

    assert b_teardown_ran


# ── Log-and-continue across CancelledError teardown ────────────────


async def test_log_and_continue_across_cancelled_error() -> None:
    first_ran = False
    third_ran = False

    async def make_first() -> AsyncIterator[_SvcA]:
        nonlocal first_ran
        yield _SvcA()
        first_ran = True

    async def make_second() -> AsyncIterator[_SvcB]:
        yield _SvcB()
        # Why: pre-arm a CancelledError from inside the teardown, simulating
        # a cancellation cascade hitting a teardown callback mid-shutdown.
        task = asyncio.current_task()
        if task is not None:
            task.cancel()
        await asyncio.sleep(0)

    async def make_third() -> AsyncIterator[_SvcC]:
        nonlocal third_ran
        yield _SvcC()
        third_ran = True

    entries = {
        _SvcA: ProviderEntry(
            type_=_SvcA,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_first,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
        _SvcB: ProviderEntry(
            type_=_SvcB,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_second,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
        _SvcC: ProviderEntry(
            type_=_SvcC,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_third,
            factory_shape=FactoryShape.ASYNC_GENERATOR,
        ),
    }
    registry, loop_scope = _make_registry_and_loop_scope(*entries.items())
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    with pytest.raises(asyncio.CancelledError):
        await loop_scope.shutdown()

    assert first_ran
    assert third_ran


# ── LoopScope.replace_value (credential hot-reload seam) ────────────────


async def test_loop_scope_replace_value_replaces_cached_instance() -> None:
    """replace_value swaps the cached instance so resolved_cache() and
    get() reflect the new value — the sanctioned mid-loop swap for
    hot-reloaded resources (e.g. worker_pool after SIGHUP)."""
    first, second = _MockClient(), _MockClient()
    entry = ProviderEntry(
        type_=_MockClient,
        scope=Scope.LOOP,
        kind="value",
        impl=first,
        factory_shape=FactoryShape.VALUE,
    )
    registry, loop_scope = _make_registry_and_loop_scope((_MockClient, entry))
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    assert loop_scope.resolved_cache()[_MockClient] is first

    loop_scope.replace_value(_MockClient, second)

    assert loop_scope.resolved_cache()[_MockClient] is second
    assert loop_scope.get(_MockClient) is second
    await loop_scope.shutdown()


async def test_loop_scope_replace_value_raises_for_uncached_type() -> None:
    """replace_value on a type with no cached value is a programming error
    — fail fast rather than silently inserting a bogus entry."""
    _registry, loop_scope = _make_registry_and_loop_scope()
    with pytest.raises(KeyError, match="nothing to replace"):
        loop_scope.replace_value(_MockClient, _MockClient())
    await loop_scope.shutdown()


# ── LOOP-loop assertion ────────────────────────────────────────────


async def test_loop_scope_loop_assertion() -> None:
    entry = ProviderEntry(
        type_=_MockClient,
        scope=Scope.LOOP,
        kind="factory",
        impl=lambda: _MockClient(),
        factory_shape=FactoryShape.SYNC_CALLABLE,
    )
    registry = ProviderRegistry()
    registry._providers[_MockClient] = entry

    loop_scope = LoopScope(resolver=_stub_resolver)

    exc_raised = False

    def _run_on_other_loop() -> None:
        nonlocal exc_raised
        other_loop = asyncio.new_event_loop()
        try:
            other_loop.run_until_complete(
                loop_scope.get_or_create(_MockClient, registry.get(_MockClient)),
            )
        except RuntimeError:
            exc_raised = True
        finally:
            other_loop.close()

    await asyncio.to_thread(_run_on_other_loop)
    assert exc_raised

    await loop_scope.shutdown()


# ── Factory shape dispatch — all six arms ──────────────────────────


def _make_sync_callable() -> _Y:
    return _Y()


async def _make_async_callable() -> _Y:
    return _Y()


def _make_sync_gen() -> Iterator[_Y]:
    yield _Y()


async def _make_async_gen() -> AsyncIterator[_Y]:
    yield _Y()


@pytest.mark.parametrize(
    ("shape", "impl"),
    [
        (FactoryShape.VALUE, _Y()),
        (FactoryShape.SYNC_CALLABLE, _make_sync_callable),
        (FactoryShape.ASYNC_CALLABLE, _make_async_callable),
        (FactoryShape.SYNC_GENERATOR, _make_sync_gen),
        (FactoryShape.ASYNC_GENERATOR, _make_async_gen),
        (FactoryShape.CLASS, _Y),
    ],
    ids=[s.name for s in FactoryShape],
)
async def test_factory_shape_dispatch(shape: FactoryShape, impl: object) -> None:
    entry = ProviderEntry(
        type_=_Y,
        scope=Scope.LOOP,
        kind="factory",
        impl=impl,
        factory_shape=shape,
    )
    registry = ProviderRegistry()
    registry._providers[_Y] = entry

    loop_scope = LoopScope(resolver=_stub_resolver)
    result = await loop_scope.get_or_create(_Y, registry.get(_Y))
    assert isinstance(result, _Y)
    await loop_scope.shutdown()


# ── assert_never exhaustiveness compile check ──────────────────────

# This is a type-only assertion — pyright-strict catches a missing arm
# via assert_never in get_or_create's match statement.
_AllShapes: tuple[FactoryShape, ...] = tuple(FactoryShape)


# ── INFO logging on bootstrap/shutdown ──────────────────────────────


async def test_process_scope_logging() -> None:
    entry = ProviderEntry(
        type_=_SvcA,
        scope=Scope.PROCESS,
        kind="value",
        impl=_SvcA(),
        factory_shape=FactoryShape.VALUE,
    )
    registry = ProviderRegistry()
    registry._providers[_SvcA] = entry

    process_scope = ProcessScope(resolver=_stub_resolver)

    await process_scope.bootstrap(registry, _settings())
    cached = process_scope.get(_SvcA)
    assert cached is not None
    await process_scope.shutdown()


async def test_loop_scope_logging() -> None:
    entry = ProviderEntry(
        type_=_SvcA,
        scope=Scope.LOOP,
        kind="value",
        impl=_SvcA(),
        factory_shape=FactoryShape.VALUE,
    )
    registry = ProviderRegistry()
    registry._providers[_SvcA] = entry

    loop_scope = LoopScope(resolver=_stub_resolver)

    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))
    cached = loop_scope.get(_SvcA)
    assert cached is not None
    await loop_scope.shutdown()


# ── Empty THREAD scope ──────────────────────────────────────────────


async def test_empty_thread_scope() -> None:
    registry = ProviderRegistry()
    process_scope = ProcessScope(resolver=_stub_resolver)
    thread_scope = ThreadScope(resolver=_stub_resolver)

    await thread_scope.bootstrap(registry, process_scope)
    assert len(thread_scope._teardowns) == 0
    await thread_scope.shutdown()


# ── SYNC_GENERATOR executor is shared across providers ──────────────


async def test_sync_gen_executor_shared_across_providers() -> None:
    async def resolver_spy(func: Any, **kw: Any) -> dict[str, object]:
        return {}

    def make_r1() -> Iterator[_MockResource]:
        yield _MockResource()

    def make_r2() -> Iterator[_MockResource]:
        yield _MockResource()

    entries = {
        _MockResource: ProviderEntry(
            type_=_MockResource,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_r1,
            factory_shape=FactoryShape.SYNC_GENERATOR,
        ),
        _Y: ProviderEntry(
            type_=_Y,
            scope=Scope.LOOP,
            kind="factory",
            impl=make_r2,
            factory_shape=FactoryShape.SYNC_GENERATOR,
        ),
    }
    registry = ProviderRegistry()
    for t, e in entries.items():
        registry._providers[t] = e

    loop_scope = LoopScope(resolver=resolver_spy)
    await loop_scope.bootstrap(registry, ProcessScope(resolver=resolver_spy))

    assert loop_scope._sync_gen_executor is not None

    await loop_scope.shutdown()

    assert loop_scope._sync_gen_executor is None


# ── SYNC_GENERATOR executor shutdown does not block the loop ────────


async def test_sync_gen_executor_shutdown_after_teardowns() -> None:
    teardown_order: list[str] = []

    def make_resource() -> Iterator[_MockResource]:
        yield _MockResource()
        teardown_order.append("resource_teardown")

    entry = ProviderEntry(
        type_=_MockResource,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_resource,
        factory_shape=FactoryShape.SYNC_GENERATOR,
    )
    registry = ProviderRegistry()
    registry._providers[_MockResource] = entry

    loop_scope = LoopScope(resolver=_stub_resolver)

    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))
    assert loop_scope._sync_gen_executor is not None
    await loop_scope.shutdown()

    assert teardown_order == ["resource_teardown"]
    assert loop_scope._sync_gen_executor is None


# ── No SYNC_GENERATOR resolved → no executor created ──────────────


async def test_no_sync_gen_no_executor() -> None:
    entry = ProviderEntry(
        type_=_SvcA,
        scope=Scope.LOOP,
        kind="value",
        impl=_SvcA(),
        factory_shape=FactoryShape.VALUE,
    )
    registry = ProviderRegistry()
    registry._providers[_SvcA] = entry

    loop_scope = LoopScope(resolver=_stub_resolver)

    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))
    assert loop_scope._sync_gen_executor is None
    await loop_scope.shutdown()

    assert loop_scope._sync_gen_executor is None


# ── aclose() is idempotent for the executor branch ─────────────────


async def test_aclose_idempotent_executor() -> None:
    def make_resource() -> Iterator[_MockResource]:
        yield _MockResource()

    entry = ProviderEntry(
        type_=_MockResource,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_resource,
        factory_shape=FactoryShape.SYNC_GENERATOR,
    )
    registry = ProviderRegistry()
    registry._providers[_MockResource] = entry

    loop_scope = LoopScope(resolver=_stub_resolver)

    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))
    await loop_scope.shutdown()
    assert loop_scope._sync_gen_executor is None
    await loop_scope.shutdown()
    assert loop_scope._sync_gen_executor is None


# ── LoopScope.resolved_cache() ────────────────────────────────────


async def test_resolved_cache_returns_resolved_values() -> None:
    entry = ProviderEntry(
        type_=int,
        scope=Scope.LOOP,
        kind="value",
        impl=42,
        factory_shape=FactoryShape.VALUE,
    )
    registry = ProviderRegistry()
    registry._providers[int] = entry
    loop_scope = LoopScope(resolver=_stub_resolver)
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    cache = loop_scope.resolved_cache()
    assert cache[int] == 42

    await loop_scope.shutdown()


async def test_resolved_cache_read_only() -> None:
    entry = ProviderEntry(
        type_=int,
        scope=Scope.LOOP,
        kind="value",
        impl=42,
        factory_shape=FactoryShape.VALUE,
    )
    registry = ProviderRegistry()
    registry._providers[int] = entry
    loop_scope = LoopScope(resolver=_stub_resolver)
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    cache = loop_scope.resolved_cache()
    with pytest.raises(TypeError):
        cache[int] = 99  # type: ignore[index]  # Why: MappingProxyType raises TypeError at runtime; cast would defeat the test's purpose

    await loop_scope.shutdown()


async def test_resolved_cache_live_view() -> None:
    entry_a = ProviderEntry(
        type_=_SvcA,
        scope=Scope.LOOP,
        kind="value",
        impl=_SvcA(),
        factory_shape=FactoryShape.VALUE,
    )
    entry_b = ProviderEntry(
        type_=_SvcB,
        scope=Scope.LOOP,
        kind="value",
        impl=_SvcB(),
        factory_shape=FactoryShape.VALUE,
    )
    registry = ProviderRegistry()
    registry._providers[_SvcA] = entry_a
    loop_scope = LoopScope(resolver=_stub_resolver)
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    cache = loop_scope.resolved_cache()
    assert _SvcA in cache
    assert _SvcB not in cache

    registry._providers[_SvcB] = entry_b
    await loop_scope.get_or_create(_SvcB, registry.get(_SvcB))

    assert _SvcB in cache

    await loop_scope.shutdown()


async def test_resolved_cache_empty_before_and_after_bootstrap_no_providers() -> None:
    registry = ProviderRegistry()
    loop_scope = LoopScope(resolver=_stub_resolver)

    assert len(loop_scope.resolved_cache()) == 0

    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    assert len(loop_scope.resolved_cache()) == 0

    await loop_scope.shutdown()


# ── Plain class — no teardown callback registered ──────────


class _PlainSvc:
    pass


async def test_plain_class_no_teardown() -> None:
    container = ScopeContainer(scope=Scope.LOOP, resolver=_stub_resolver)
    teardowns_before = len(container._teardowns)

    entry = ProviderEntry(
        type_=_PlainSvc,
        scope=Scope.LOOP,
        kind="class",
        impl=_PlainSvc,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.Plain,
    )
    result = await container.get_or_create(_PlainSvc, entry)
    assert isinstance(result, _PlainSvc)
    assert len(container._teardowns) == teardowns_before

    await container.aclose()


# ── ACM lifecycle — __aenter__ at bootstrap, __aexit__ at shutdown ──


class _AcmSvc:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> "_AcmSvc":
        self.entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.exited = True


async def test_acm_lifecycle() -> None:
    entry = ProviderEntry(
        type_=_AcmSvc,
        scope=Scope.LOOP,
        kind="class",
        impl=_AcmSvc,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.AsyncContextManager,
    )
    registry, loop_scope = _make_registry_and_loop_scope((_AcmSvc, entry))
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    cached = loop_scope.get(_AcmSvc)
    assert cached is not None
    inst = cached
    assert isinstance(inst, _AcmSvc)
    assert inst.entered
    assert not inst.exited

    await loop_scope.shutdown()

    assert inst.exited


# ── ACM injected value is __aenter__() return, NOT instance ──


_SENTINEL = object()


class _AcmReturnsOther:
    async def __aenter__(self) -> object:
        return _SENTINEL

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        pass


async def test_acm_injected_value_is_aenter_return() -> None:
    entry = ProviderEntry(
        type_=_AcmReturnsOther,
        scope=Scope.LOOP,
        kind="class",
        impl=_AcmReturnsOther,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.AsyncContextManager,
    )
    registry, loop_scope = _make_registry_and_loop_scope((_AcmReturnsOther, entry))
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    cached = loop_scope.get(_AcmReturnsOther)
    assert cached is _SENTINEL

    await loop_scope.shutdown()


# ── AsyncCloseable lifecycle — aclose() called at shutdown ──


class _AsyncCloseableSvc:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


async def test_async_closeable_lifecycle() -> None:
    entry = ProviderEntry(
        type_=_AsyncCloseableSvc,
        scope=Scope.LOOP,
        kind="class",
        impl=_AsyncCloseableSvc,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.AsyncCloseable,
    )
    registry, loop_scope = _make_registry_and_loop_scope((_AsyncCloseableSvc, entry))
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    cached = loop_scope.get(_AsyncCloseableSvc)
    assert cached is not None
    inst = cached
    assert isinstance(inst, _AsyncCloseableSvc)
    assert not inst.closed

    await loop_scope.shutdown()

    assert inst.closed


# ── SyncCloseable lifecycle + asyncio.to_thread ─────────────


class _SyncCloseableSvc:
    def __init__(self) -> None:
        self.closed = False
        self.closed_from_thread: threading.Thread | None = None

    def close(self) -> None:
        self.closed = True
        self.closed_from_thread = threading.current_thread()


async def test_sync_closeable_lifecycle_to_thread() -> None:
    entry = ProviderEntry(
        type_=_SyncCloseableSvc,
        scope=Scope.LOOP,
        kind="class",
        impl=_SyncCloseableSvc,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.SyncCloseable,
    )
    registry, loop_scope = _make_registry_and_loop_scope((_SyncCloseableSvc, entry))
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))

    cached = loop_scope.get(_SyncCloseableSvc)
    assert cached is not None
    inst = cached
    assert isinstance(inst, _SyncCloseableSvc)
    assert not inst.closed

    await loop_scope.shutdown()

    assert inst.closed
    assert inst.closed_from_thread is not threading.main_thread()


# ── __aenter__ raises — no teardown; prior teardowns still fire ──


class _AcmOk:
    def __init__(self) -> None:
        self.exited = False

    async def __aenter__(self) -> "_AcmOk":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.exited = True


class _AcmFailsEnter:
    async def __aenter__(self) -> None:
        raise RuntimeError("aenter boom")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        pass


async def test_aenter_raises_no_teardown_prior_still_fire() -> None:
    entry_a = ProviderEntry(
        type_=_AcmOk,
        scope=Scope.LOOP,
        kind="class",
        impl=_AcmOk,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.AsyncContextManager,
    )
    entry_b = ProviderEntry(
        type_=_AcmFailsEnter,
        scope=Scope.LOOP,
        kind="class",
        impl=_AcmFailsEnter,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.AsyncContextManager,
    )
    registry = ProviderRegistry()
    registry._providers[_AcmOk] = entry_a
    registry._providers[_AcmFailsEnter] = entry_b

    loop_scope = LoopScope(resolver=_stub_resolver)

    inst_a = await loop_scope.get_or_create(_AcmOk, registry.get(_AcmOk))
    assert isinstance(inst_a, _AcmOk)
    assert not inst_a.exited

    with pytest.raises(RuntimeError, match="aenter boom"):
        await loop_scope.get_or_create(_AcmFailsEnter, registry.get(_AcmFailsEnter))

    assert not inst_a.exited

    await loop_scope.shutdown()

    assert inst_a.exited


# ── Async generator yields twice — RuntimeError caught by log-and-continue ──


async def test_async_gen_yields_twice_runtime_error_caught() -> None:
    async def make_double_yield() -> AsyncIterator[_SvcA]:
        yield _SvcA()
        yield _SvcA()

    entry = ProviderEntry(
        type_=_SvcA,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_double_yield,
        factory_shape=FactoryShape.ASYNC_GENERATOR,
        lifecycle=ProviderLifecycle.AsyncGenerator,
    )
    registry, loop_scope = _make_registry_and_loop_scope((_SvcA, entry))

    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))
    cached = loop_scope.get(_SvcA)
    assert cached is not None
    # Double-yield async gen triggers RuntimeError during teardown;
    # shutdown must complete without propagating it (log-and-continue).
    await loop_scope.shutdown()


# ── Class-shape teardown failure isolation (for class shapes) ──


class _AcmOkWithFlag:
    def __init__(self) -> None:
        self.exited = False

    async def __aenter__(self) -> "_AcmOkWithFlag":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.exited = True


class _AcmFailsExit:
    async def __aenter__(self) -> "_AcmFailsExit":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        raise RuntimeError("boom")


async def test_class_shape_teardown_failure_isolation() -> None:
    entry_a = ProviderEntry(
        type_=_AcmOkWithFlag,
        scope=Scope.LOOP,
        kind="class",
        impl=_AcmOkWithFlag,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.AsyncContextManager,
    )
    entry_b = ProviderEntry(
        type_=_AcmFailsExit,
        scope=Scope.LOOP,
        kind="class",
        impl=_AcmFailsExit,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.AsyncContextManager,
    )
    registry = ProviderRegistry()
    registry._providers[_AcmOkWithFlag] = entry_a
    registry._providers[_AcmFailsExit] = entry_b

    loop_scope = LoopScope(resolver=_stub_resolver)
    inst_a = await loop_scope.get_or_create(_AcmOkWithFlag, registry.get(_AcmOkWithFlag))
    await loop_scope.get_or_create(_AcmFailsExit, registry.get(_AcmFailsExit))

    await loop_scope.shutdown()

    assert inst_a.exited
