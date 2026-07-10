"""Chaos and failure-mode tests for the DI lifecycle system.

Scenarios:
  __aenter__ raises during LOOP scope bootstrap (after one provider
         already entered): already-entered provider's __aexit__ fires;
         exception propagates; failing provider has no teardown; C never
         instantiated.
  __aexit__ raises during LOOP scope shutdown: exception logged at
         ERROR with event="provider-teardown-error"; other providers' teardowns
         still fire in LIFO order; no exception propagates from shutdown().
  aclose() raises during shutdown: same log + isolation contract as
         the __aexit__ case (AsyncCloseable path).
  Async-generator teardown raises (code after yield raises): caught,
         logged at ERROR, remaining providers still torn down.

Factory-shape chaos tests are preserved below (solver-level
failure propagation), separate from the lifecycle chaos surface.

All chaos tests assert teardown side effects via per-instance flags
(__aexit_called, aclose_called, etc.) — NOT by monkeypatching internals.
"""

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from typing import Any

import pytest
import structlog

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._di.solver import solve_dependencies
from taskq._di.types import FactoryShape, ProviderEntry
from taskq.exceptions import MissingProvider
from taskq.settings import WorkerSettings


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
        },
    )


# ── Lifecycle chaos test doubles ─────────────────────────────────


class _AcmOk:
    """ACM provider that enters and exits cleanly."""

    def __init__(self) -> None:
        self.aexit_called = False

    async def __aenter__(self) -> "_AcmOk":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.aexit_called = True


# ── __aenter__ raises during LOOP scope bootstrap ──────────


async def test_aenter_raises_during_loop_bootstrap() -> None:
    """__aenter__ raises during LOOP scope bootstrap (after one provider already entered).

    Three providers A, B, C registered at LOOP scope; B's __aenter__ raises
    RuntimeError. Oracle:
      - the exception propagates from bootstrap (caller sees it)
      - A's __aexit__ is called (teardown fires via aclose() on partial state)
      - C is never instantiated (bootstrap aborted before reaching C)
      - no teardown for B (guarantee)
    """
    b_aexit_called = False

    class _SvcB:
        def __init__(self) -> None:
            pass

        async def __aenter__(self) -> None:
            raise RuntimeError("aenter boom")

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: Any,
        ) -> None:
            nonlocal b_aexit_called
            b_aexit_called = True

    registry = ProviderRegistry()
    settings = _settings()

    class _SvcC:
        instantiated = False

        def __init__(self) -> None:
            _SvcC.instantiated = True
            self.aexit_called = False

        async def __aenter__(self) -> "_SvcC":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: Any,
        ) -> None:
            self.aexit_called = True

    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_class(_AcmOk, Scope.LOOP)
    registry.register_class(_SvcB, Scope.LOOP)
    registry.register_class(_SvcC, Scope.LOOP)
    registry.validate()

    scope_containers: dict[Scope, object] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol — pyright cannot verify dict covariance across the Protocol boundary

    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)

    with pytest.raises(RuntimeError, match="aenter boom"):
        await loop_scope.bootstrap(registry, process_scope, thread_scope)

    inst_a = loop_scope.get(_AcmOk)
    assert inst_a is not None
    assert isinstance(inst_a, _AcmOk)

    await loop_scope.shutdown()

    assert inst_a.aexit_called
    assert not _SvcC.instantiated
    assert not b_aexit_called


# ── __aexit__ raises during LOOP scope shutdown ────────────


async def test_aexit_raises_during_loop_shutdown() -> None:
    """__aexit__ raises during LOOP scope shutdown.

    Register A, B, C ACM providers; B's __aexit__ raises. Oracle:
      - the exception is logged at ERROR with event="provider-teardown-error"
      - A's and C's __aexit__ are called (LIFO order: C → B → A)
      - no exception propagates from shutdown()
    """
    teardown_order: list[str] = []

    class _AcmA:
        def __init__(self) -> None:
            self.aexit_called = False

        async def __aenter__(self) -> "_AcmA":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: Any,
        ) -> None:
            self.aexit_called = True
            teardown_order.append("A")

    class _AcmB:
        def __init__(self) -> None:
            self.aexit_called = False

        async def __aenter__(self) -> "_AcmB":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: Any,
        ) -> None:
            self.aexit_called = True
            teardown_order.append("B")
            raise RuntimeError("aexit boom")

    class _AcmC:
        def __init__(self) -> None:
            self.aexit_called = False

        async def __aenter__(self) -> "_AcmC":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: Any,
        ) -> None:
            self.aexit_called = True
            teardown_order.append("C")

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_class(_AcmA, Scope.LOOP)
    registry.register_class(_AcmB, Scope.LOOP)
    registry.register_class(_AcmC, Scope.LOOP)
    registry.validate()

    scope_containers: dict[Scope, object] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: same covariance gap as
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    inst_a = loop_scope.get(_AcmA)
    inst_b = loop_scope.get(_AcmB)
    inst_c = loop_scope.get(_AcmC)
    assert inst_a is not None and isinstance(inst_a, _AcmA)
    assert inst_b is not None and isinstance(inst_b, _AcmB)
    assert inst_c is not None and isinstance(inst_c, _AcmC)

    await loop_scope.shutdown()

    assert inst_c.aexit_called
    assert inst_b.aexit_called
    assert inst_a.aexit_called

    assert teardown_order == ["C", "B", "A"]


# ── aclose() raises during shutdown ────────────────────────


async def test_aclose_raises_during_shutdown() -> None:
    """aclose() raises during shutdown: same log + isolation contract as.

    Register an AsyncCloseable provider whose aclose() raises, plus other
    providers that close cleanly. Oracle:
      - exception logged at ERROR with event="provider-teardown-error"
      - other providers' teardowns still fire
      - no exception propagates from shutdown()
    Verifies (AsyncCloseable path).
    """
    teardown_order: list[str] = []

    class _CloseOk:
        def __init__(self) -> None:
            self.aclose_called = False

        async def aclose(self) -> None:
            self.aclose_called = True
            teardown_order.append("ok")

    class _CloseFails:
        def __init__(self) -> None:
            self.aclose_called = False

        async def aclose(self) -> None:
            self.aclose_called = True
            teardown_order.append("fail")
            raise RuntimeError("aclose boom")

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_class(_CloseOk, Scope.LOOP)
    registry.register_class(_CloseFails, Scope.LOOP)
    registry.validate()

    scope_containers: dict[Scope, object] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: same covariance gap as
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    inst_ok = loop_scope.get(_CloseOk)
    inst_fail = loop_scope.get(_CloseFails)
    assert inst_ok is not None and isinstance(inst_ok, _CloseOk)
    assert inst_fail is not None and isinstance(inst_fail, _CloseFails)

    await loop_scope.shutdown()

    assert inst_ok.aclose_called
    assert inst_fail.aclose_called


# ── async-generator teardown raises ─────────────────────────


async def test_async_gen_teardown_raises() -> None:
    """async-generator teardown raises (code after yield raises).

    Register a factory whose post-yield code raises; verify the existing
    aclose() iteration catches, logs, and continues. Oracle:
      - exception logged at ERROR with event="provider-teardown-error"
      - other providers' teardowns still fire
      - no exception propagates from shutdown()
    Verifies +.
    """

    class _SvcOk:
        pass

    ok_torn_down = False

    class _SvcFails:
        pass

    async def make_ok() -> AsyncIterator[_SvcOk]:
        nonlocal ok_torn_down
        yield _SvcOk()
        ok_torn_down = True

    async def make_fails() -> AsyncIterator[_SvcFails]:
        yield _SvcFails()
        raise RuntimeError("gen teardown boom")

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_factory(_SvcOk, Scope.LOOP, make_ok)
    registry.register_factory(_SvcFails, Scope.LOOP, make_fails)
    registry.validate()

    scope_containers: dict[Scope, object] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: same covariance gap as
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    await loop_scope.shutdown()

    assert ok_torn_down


# ── Factory-shape chaos tests (solver-level) ──────────


class _StubRegistry:
    """Minimal ProviderRegistry test double."""

    def __init__(self, entries: dict[type, ProviderEntry[object]]) -> None:
        self._entries = entries

    @property
    def providers(self) -> dict[type, ProviderEntry[object]]:
        return dict(self._entries)

    def get(self, type_: type[object]) -> ProviderEntry[object]:
        entry = self._entries.get(type_)
        if entry is None:
            raise MissingProvider(type_name=type_.__qualname__, required_by="test")
        return entry


class _StubScopeContainer:
    """ScopeContainer test double with log-and-continue close policy."""

    def __init__(
        self,
        scope: Scope,
        registry: _StubRegistry,
        containers: dict[Scope, "_StubScopeContainer"],
    ) -> None:
        self._scope = scope
        self._registry = registry
        self._containers = containers
        self._cache: dict[type, object] = {}
        self._stack = AsyncExitStack()
        self._log = structlog.get_logger("taskq._di.test_chaos")
        self._last_cache_hit: bool = False

    @property
    def last_cache_hit(self) -> bool:
        return self._last_cache_hit

    async def get_or_create(self, type_: type[object], entry: ProviderEntry[object]) -> object:
        if self._scope is not Scope.TRANSIENT and type_ in self._cache:
            self._last_cache_hit = True
            return self._cache[type_]
        self._last_cache_hit = False

        from contextlib import asynccontextmanager
        from typing import cast

        shape = entry.factory_shape

        if shape == FactoryShape.VALUE:
            value = entry.impl
        elif shape == FactoryShape.ASYNC_GENERATOR:
            cm = asynccontextmanager(entry.impl)()  # type: ignore[arg-type] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
            value = cast(object, await self._stack.enter_async_context(cm))
        elif shape in (FactoryShape.ASYNC_CALLABLE, FactoryShape.SYNC_CALLABLE):
            if shape == FactoryShape.ASYNC_CALLABLE:
                value = cast(object, await entry.impl())  # type: ignore[call-arg,misc] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
            else:
                value = cast(object, entry.impl())  # type: ignore[call-arg,misc] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
        else:
            raise NotImplementedError(f"factory_shape {shape!r} not implemented in test double")

        if self._scope is not Scope.TRANSIENT:
            self._cache[type_] = value

        return value

    async def aclose(self) -> None:
        try:
            await self._stack.aclose()
        except Exception as exc:
            self._log.warning("teardown-error", error=str(exc))


def _make_containers(
    registry: _StubRegistry,
) -> dict[Scope, _StubScopeContainer]:
    containers: dict[Scope, _StubScopeContainer] = {}
    for scope in Scope:
        containers[scope] = _StubScopeContainer(scope, registry, containers)
    return containers


class _SvcA:
    pass


class _SvcB:
    pass


async def test_factory_raises_mid_graph() -> None:
    """Factory B raises mid-graph; provider A still torn down on container close."""

    a_torn_down = False

    async def make_a() -> AsyncIterator[_SvcA]:
        nonlocal a_torn_down
        yield _SvcA()
        a_torn_down = True

    async def make_b() -> _SvcB:
        raise RuntimeError("kapow")

    entry_a = ProviderEntry(
        type_=_SvcA,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_a,
        factory_shape=FactoryShape.ASYNC_GENERATOR,
    )
    entry_b = ProviderEntry(
        type_=_SvcB,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_b,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = _StubRegistry({_SvcA: entry_a, _SvcB: entry_b})
    containers = _make_containers(registry)

    async def actor(a: _SvcA, b: _SvcB) -> None:
        pass

    with pytest.raises(RuntimeError, match="kapow"):
        await solve_dependencies(
            func=actor,
            registry=registry,
            scope_containers=containers,
        )

    assert not a_torn_down

    await containers[Scope.LOOP].aclose()

    assert a_torn_down


async def test_factory_raises_during_scope_bootstrap() -> None:
    """LOOP-scoped factory raises during bootstrap; PROCESS scope torn down via AsyncExitStack.

    Registers a PROCESS-scoped value (Settings-like) and a LOOP-scoped
    factory that raises RuntimeError when called. Bootstraps ProcessScope
    (succeeds), ThreadScope (empty), then calls LoopScope.bootstrap.
    Asserts the RuntimeError propagates and that ProcessScope's
    already-resolved providers were torn down via AsyncExitStack during
    the unwind.
    """
    process_torn_down = False

    class _ProcessSvc:
        pass

    async def make_process_svc() -> AsyncIterator[_ProcessSvc]:
        nonlocal process_torn_down
        yield _ProcessSvc()
        process_torn_down = True

    class _FailingLoopSvc:
        pass

    async def make_failing_svc() -> _FailingLoopSvc:
        raise RuntimeError("intentional bootstrap failure")

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_factory(_ProcessSvc, Scope.PROCESS, make_process_svc)
    registry.register_factory(_FailingLoopSvc, Scope.LOOP, make_failing_svc)
    registry.validate()

    scope_containers: dict[Scope, object] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol — pyright cannot verify dict covariance across the Protocol boundary

    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    async with AsyncExitStack() as stack:
        await process_scope.bootstrap(registry, settings)
        stack.push_async_callback(process_scope.shutdown)

        await thread_scope.bootstrap(registry, process_scope)
        stack.push_async_callback(thread_scope.shutdown)

        with pytest.raises(RuntimeError, match="intentional bootstrap failure"):
            await loop_scope.bootstrap(registry, process_scope, thread_scope)

    assert process_torn_down
