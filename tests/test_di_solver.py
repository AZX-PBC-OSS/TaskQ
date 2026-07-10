"""Tests for the DI solver engine.

Scenarios:
  Plain-annotation resolution
  Generator-based provider with LIFO teardown
  Nested dependency resolution
  LOOP cache hit
  TRANSIENT distinct per injection point
  passthrough_kwargs merge
  Multiple Scope markers → DIError
  No __future__ annotations import
  Registration-time default scope
  Call-site override narrower than default
  Missing provider propagates
  Unresolvable forward reference → DIError wrapping
  Non-type annotation defensive guard
"""

import types
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Annotated, Final, cast, get_type_hints

import pytest

from taskq._di.scope import Scope
from taskq._di.solver import solve_dependencies
from taskq._di.types import FactoryShape, ProviderEntry, ProviderRegistry
from taskq.exceptions import DIError, MissingProvider

_RETURN_KEY = "return"


# ── Test doubles ──────────────────────────────────────────────────


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
    """Minimal ScopeContainer test double that mirrors the contract.

    Caches for PROCESS / THREAD / LOOP; no cache for TRANSIENT.
    Dispatches on factory_shape for VALUE / SYNC_CALLABLE / ASYNC_CALLABLE /
    ASYNC_GENERATOR. Recursively resolves factory parameters via
    solve_dependencies only when the factory has resolvable DI parameters.
    """

    def __init__(
        self,
        scope: Scope,
        registry: ProviderRegistry,
        containers: dict[Scope, "_StubScopeContainer"],
    ) -> None:
        self._scope = scope
        self._registry = registry
        self._containers = containers
        self._cache: dict[type[object], object] = {}
        self._stack = AsyncExitStack()
        self._last_cache_hit: bool = False

    @property
    def last_cache_hit(self) -> bool:
        return self._last_cache_hit

    async def get_or_create(self, type_: type[object], entry: ProviderEntry[object]) -> object:
        if self._scope is not Scope.TRANSIENT and type_ in self._cache:
            self._last_cache_hit = True
            return self._cache[type_]
        self._last_cache_hit = False

        shape = entry.factory_shape

        if shape == FactoryShape.VALUE:
            value = entry.impl
        elif shape in (FactoryShape.ASYNC_CALLABLE, FactoryShape.SYNC_CALLABLE):
            factory_kwargs = await _resolve_factory_params(
                entry.impl,
                self._registry,
                self._containers,
            )
            if shape == FactoryShape.ASYNC_CALLABLE:
                value = cast(object, await entry.impl(**factory_kwargs))  # type: ignore[call-arg] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
            else:
                value = cast(object, entry.impl(**factory_kwargs))  # type: ignore[call-arg] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
        elif shape == FactoryShape.ASYNC_GENERATOR:
            factory_kwargs = await _resolve_factory_params(
                entry.impl,
                self._registry,
                self._containers,
            )
            cm = asynccontextmanager(entry.impl)(**factory_kwargs)  # type: ignore[arg-type] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
            value = cast(object, await self._stack.enter_async_context(cm))
        else:
            raise NotImplementedError(f"factory_shape {shape!r} not implemented in test double")

        if self._scope is not Scope.TRANSIENT:
            self._cache[type_] = value

        return value

    async def aclose(self) -> None:
        await self._stack.aclose()


async def _resolve_factory_params(
    factory: object,
    registry: ProviderRegistry,
    containers: dict[Scope, _StubScopeContainer],
) -> dict[str, object]:
    """Resolve a factory's own DI parameters, if any."""
    if not callable(factory):
        return {}
    try:
        hints = get_type_hints(factory, include_extras=True)
    except Exception:
        return {}
    di_params = {k: v for k, v in hints.items() if k != _RETURN_KEY}
    if not di_params:
        return {}
    return await solve_dependencies(
        func=factory,
        registry=registry,
        scope_containers=containers,
    )


def _make_containers(
    registry: ProviderRegistry,
) -> dict[Scope, _StubScopeContainer]:
    containers: dict[Scope, _StubScopeContainer] = {}
    for scope in Scope:
        containers[scope] = _StubScopeContainer(scope, registry, containers)
    return containers


# ── Stub types for tests ──────────────────────────────────────────


class _DBConn:
    pass


class _Settings:
    pass


class _AsyncGraphClient:
    def __init__(self, settings: _Settings) -> None:
        self.settings = settings


class _PortfolioClient:
    def __init__(self, graph: _AsyncGraphClient, settings: _Settings) -> None:
        self.graph = graph
        self.settings = settings


# ── Plain-annotation resolution ────────────────────────────


async def test_plain_annotation_resolution() -> None:
    """Plain annotation resolves through registry + container."""

    async def make_conn() -> _DBConn:
        return _DBConn()

    entry = ProviderEntry(
        type_=_DBConn,
        scope=Scope.TRANSIENT,
        kind="factory",
        impl=make_conn,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = _StubRegistry({_DBConn: entry})
    containers = _make_containers(registry)

    async def actor(db: _DBConn) -> None:
        pass

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    assert "db" in kwargs
    assert isinstance(kwargs["db"], _DBConn)


# ── Generator-based provider with LIFO teardown ──────────


async def test_generator_lifo_teardown() -> None:
    """Async generator teardown runs on container aclose()."""

    teardown_ran = False

    async def make_conn() -> AsyncIterator[_DBConn]:
        nonlocal teardown_ran
        yield _DBConn()
        teardown_ran = True

    entry = ProviderEntry(
        type_=_DBConn,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_conn,
        factory_shape=FactoryShape.ASYNC_GENERATOR,
    )
    registry = _StubRegistry({_DBConn: entry})
    containers = _make_containers(registry)

    async def actor(db: _DBConn) -> None:
        pass

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    assert isinstance(kwargs["db"], _DBConn)
    assert not teardown_ran

    await containers[Scope.LOOP].aclose()
    assert teardown_ran


# ── Nested dependency resolution ──────────────────────────


async def test_nested_resolution() -> None:
    """Factory parameters are recursively resolved."""

    settings_obj = _Settings()

    settings_entry = ProviderEntry(
        type_=_Settings,
        scope=Scope.PROCESS,
        kind="value",
        impl=settings_obj,
        factory_shape=FactoryShape.VALUE,
    )

    async def make_graph(settings: _Settings) -> _AsyncGraphClient:
        return _AsyncGraphClient(settings)

    graph_entry = ProviderEntry(
        type_=_AsyncGraphClient,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_graph,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )

    def make_portfolio(graph: _AsyncGraphClient, settings: _Settings) -> _PortfolioClient:
        return _PortfolioClient(graph, settings)

    portfolio_entry = ProviderEntry(
        type_=_PortfolioClient,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_portfolio,
        factory_shape=FactoryShape.SYNC_CALLABLE,
    )

    registry = _StubRegistry(
        {
            _Settings: settings_entry,
            _AsyncGraphClient: graph_entry,
            _PortfolioClient: portfolio_entry,
        }
    )
    containers = _make_containers(registry)

    async def actor(client: _PortfolioClient) -> None:
        pass

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    client = kwargs["client"]
    assert isinstance(client, _PortfolioClient)
    assert isinstance(client.graph, _AsyncGraphClient)
    assert client.settings is settings_obj
    assert client.graph.settings is settings_obj


# ── LOOP cache hit ────────────────────────────────────────


async def test_loop_cache_hit() -> None:
    """LOOP-scoped factory called once; second resolve returns same object."""

    call_count = 0

    async def make_settings() -> _Settings:
        nonlocal call_count
        call_count += 1
        return _Settings()

    entry = ProviderEntry(
        type_=_Settings,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_settings,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = _StubRegistry({_Settings: entry})
    containers = _make_containers(registry)

    async def actor_a(s: _Settings) -> None:
        pass

    async def actor_b(s: _Settings) -> None:
        pass

    kwargs_a = await solve_dependencies(
        func=actor_a,
        registry=registry,
        scope_containers=containers,
    )
    kwargs_b = await solve_dependencies(
        func=actor_b,
        registry=registry,
        scope_containers=containers,
    )

    assert call_count == 1
    assert kwargs_a["s"] is kwargs_b["s"]


# ── TRANSIENT distinct per injection point ────────────────


async def test_transient_distinct_instances() -> None:
    """TRANSIENT-scoped factory produces distinct instances."""

    call_count = 0

    async def make_settings() -> _Settings:
        nonlocal call_count
        call_count += 1
        return _Settings()

    entry = ProviderEntry(
        type_=_Settings,
        scope=Scope.TRANSIENT,
        kind="factory",
        impl=make_settings,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = _StubRegistry({_Settings: entry})
    containers = _make_containers(registry)

    async def actor(s1: _Settings, s2: _Settings) -> None:
        pass

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    assert call_count == 2
    assert kwargs["s1"] is not kwargs["s2"]


async def test_transient_annotated_explicit() -> None:
    """sub-test: explicit Annotated[T, Scope.TRANSIENT] also distinct."""

    call_count = 0

    async def make_settings() -> _Settings:
        nonlocal call_count
        call_count += 1
        return _Settings()

    entry = ProviderEntry(
        type_=_Settings,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_settings,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = _StubRegistry({_Settings: entry})
    containers = _make_containers(registry)

    async def actor(
        s1: Annotated[_Settings, Scope.TRANSIENT],
        s2: Annotated[_Settings, Scope.TRANSIENT],
    ) -> None:
        pass

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    assert call_count == 2
    assert kwargs["s1"] is not kwargs["s2"]


# ── passthrough_kwargs merge ──────────────────────────────


async def test_passthrough_kwargs() -> None:
    """Unregistered parameter bound from passthrough_kwargs."""

    sentinel = object()

    def actor(ctx: object) -> None:
        pass

    registry = _StubRegistry({})
    containers = _make_containers(registry)

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
        passthrough_kwargs={"ctx": sentinel},
    )

    assert kwargs["ctx"] is sentinel


# ── Multiple Scope markers ────────────────────────────────


async def test_multiple_scope_markers() -> None:
    """Annotated[T, Scope.X, Scope.Y] raises DIError."""

    async def actor(
        t: Annotated[_Settings, Scope.PROCESS, Scope.LOOP],
    ) -> None:
        pass

    registry = _StubRegistry({})
    containers = _make_containers(registry)

    with pytest.raises(DIError, match="t"):
        await solve_dependencies(
            func=actor,
            registry=registry,
            scope_containers=containers,
        )


# ── No __future__ annotations import ─────────────────────


def test_no_future_annotations() -> None:
    """solver.py does not import from __future__."""
    source = Path("src/taskq/_di/solver.py").read_text()
    assert "from __future__ import annotations" not in source


# ── Registration-time default scope ──────────────────────


async def test_registration_default_scope() -> None:
    """Plain annotation uses the registered default scope."""

    call_count = 0

    async def make_settings() -> _Settings:
        nonlocal call_count
        call_count += 1
        return _Settings()

    entry = ProviderEntry(
        type_=_Settings,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_settings,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = _StubRegistry({_Settings: entry})
    containers = _make_containers(registry)

    async def actor(s: _Settings) -> None:
        pass

    kwargs_a = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )
    kwargs_b = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    assert call_count == 1
    assert kwargs_a["s"] is kwargs_b["s"]


# ── Call-site override narrower than default ──────────────


async def test_call_site_override_narrower() -> None:
    """Annotated[T, Scope.TRANSIENT] overrides LOOP default."""

    call_count = 0

    async def make_settings() -> _Settings:
        nonlocal call_count
        call_count += 1
        return _Settings()

    entry = ProviderEntry(
        type_=_Settings,
        scope=Scope.LOOP,
        kind="factory",
        impl=make_settings,
        factory_shape=FactoryShape.ASYNC_CALLABLE,
    )
    registry = _StubRegistry({_Settings: entry})
    containers = _make_containers(registry)

    async def actor(
        s1: Annotated[_Settings, Scope.TRANSIENT],
        s2: Annotated[_Settings, Scope.TRANSIENT],
    ) -> None:
        pass

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    assert call_count == 2
    assert kwargs["s1"] is not kwargs["s2"]


# ── Missing provider propagates ────────────────────────────


async def test_missing_provider_propagates() -> None:
    """Unregistered, non-passthrough parameter raises MissingProvider."""

    async def actor(s: _Settings) -> None:
        pass

    registry = _StubRegistry({})
    containers = _make_containers(registry)

    with pytest.raises(MissingProvider, match=_Settings.__qualname__):
        await solve_dependencies(
            func=actor,
            registry=registry,
            scope_containers=containers,
        )


# ── Unresolvable forward reference → DIError wrapping ─────


async def test_unresolvable_forward_ref() -> None:
    """NameError from get_type_hints wrapped in DIError with chain."""

    mod = types.ModuleType("_test_unresolvable_module")

    def _no_op() -> None:
        pass

    actor_fn = types.FunctionType(
        _no_op.__code__,
        mod.__dict__,
        name="actor",
        argdefs=(),
        closure=None,
    )
    actor_fn.__annotations__ = {"x": "NonexistentType", "return": type(None)}
    actor_fn.__module__ = "_test_unresolvable_module"
    actor_fn.__qualname__ = "actor"

    import sys

    sys.modules["_test_unresolvable_module"] = mod
    try:
        registry = _StubRegistry({})
        containers = _make_containers(registry)

        with pytest.raises(DIError) as exc_info:
            await solve_dependencies(
                func=actor_fn,
                registry=registry,
                scope_containers=containers,
            )

        exc = exc_info.value
        msg = str(exc)
        assert msg.startswith("unresolvable annotation in _test_unresolvable_module.actor:")
        assert "NonexistentType" in msg
        assert isinstance(exc.__cause__, NameError)
    finally:
        del sys.modules["_test_unresolvable_module"]


# ── Non-type annotation defensive guard ────────────────────


async def test_non_type_annotation_guard() -> None:
    """Non-type annotation (e.g. Final[int]) raises DIError."""

    async def actor(x: Final[int]) -> None:  # type: ignore[type-arg] # Why: Final[int] is intentionally invalid as a parameter annotation to produce a non-type result from get_type_hints.
        pass

    registry = _StubRegistry({})
    containers = _make_containers(registry)

    with pytest.raises(DIError, match="x"):
        await solve_dependencies(
            func=actor,
            registry=registry,
            scope_containers=containers,
        )
