"""Unit tests for ProviderRegistry registration API."""

import warnings
from collections.abc import AsyncIterator, Iterator
from typing import Annotated

import pytest
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import LifecycleDetectionWarning, Scope
from taskq._di.types import FactoryShape, ProviderLifecycle
from taskq.backend.clock import Clock, SystemClock
from taskq.context import JobContext
from taskq.exceptions import DIError, MissingProvider


class _Settings:
    pass


class _AsyncGraphClient:
    pass


class _PortfolioClient:
    def __init__(self, settings: _Settings, client: _AsyncGraphClient) -> None:
        self.settings = settings
        self.client = client


class _Payload(BaseModel):
    x: int


# ── register_value resolution ──────────────────────────────────────────


def test_register_value_resolution() -> None:
    registry = ProviderRegistry()
    value = _Settings()
    registry.register_value(_Settings, Scope.PROCESS, value)

    entry = registry.get(_Settings)
    assert entry.impl is value
    assert entry.scope == Scope.PROCESS
    assert entry.kind == "value"
    assert entry.factory_shape == FactoryShape.VALUE


# ── PROCESS-scope register_value(Clock,...) preserves object identity ──


def test_clock_process_scope_object_identity() -> None:
    """PROCESS-scope register_value(Clock,...) preserves object identity."""
    registry = ProviderRegistry()
    s = SystemClock()
    registry.register_value(Clock, Scope.PROCESS, s)

    entry = registry.get(Clock)
    assert entry.impl is s
    assert entry.scope == Scope.PROCESS
    assert entry.kind == "value"


# ── register_class resolution ──────────────────────────────────────────


def test_register_class_resolution() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    registry.register_value(_AsyncGraphClient, Scope.LOOP, _AsyncGraphClient())
    registry.register_class(_PortfolioClient, Scope.LOOP)

    entry = registry.get(_PortfolioClient)
    assert entry.factory_shape == FactoryShape.CLASS
    assert entry.kind == "class"
    assert entry.impl is _PortfolioClient
    assert entry.lifecycle == ProviderLifecycle.Plain

    assert (_PortfolioClient, _Settings, None) in registry._dep_edges
    assert (_PortfolioClient, _AsyncGraphClient, None) in registry._dep_edges


# ── duplicate registration raises ValueError ──────────────────────────


def test_register_value_duplicate_raises_value_error() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    with pytest.raises(ValueError, match="already registered"):
        registry.register_value(_Settings, Scope.PROCESS, _Settings())


def test_register_factory_duplicate_raises_value_error() -> None:
    registry = ProviderRegistry()

    def factory() -> _Settings:
        return _Settings()

    registry.register_factory(_Settings, Scope.PROCESS, factory)

    def factory2() -> _Settings:
        return _Settings()

    with pytest.raises(ValueError, match="already registered"):
        registry.register_factory(_Settings, Scope.PROCESS, factory2)


def test_register_class_duplicate_raises_value_error() -> None:
    registry = ProviderRegistry()
    registry.register_class(_Settings, Scope.PROCESS)
    with pytest.raises(ValueError, match="already registered"):
        registry.register_class(_Settings, Scope.PROCESS)


# ── registry sealed after validate() ──────────────────────────────────


def test_sealed_registry_rejects_registration() -> None:
    registry = ProviderRegistry()
    registry._sealed = True
    with pytest.raises(RuntimeError, match="sealed"):
        registry.register_value(_Settings, Scope.PROCESS, _Settings())


# ── has_provider ──────────────────────────────────────────────────────


def test_has_provider() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    assert registry.has_provider(_Settings) is True
    assert registry.has_provider(_AsyncGraphClient) is False


# ── ACM class registration: no warning, lifecycle set ─────────────────────────


class _ContextManagerClass:
    async def __aenter__(self) -> "_ContextManagerClass":
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass


def test_register_class_aenter_no_warning_lifecycle_set() -> None:
    registry = ProviderRegistry()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        registry.register_class(_ContextManagerClass, Scope.LOOP)

    entry = registry.get(_ContextManagerClass)
    assert entry.lifecycle == ProviderLifecycle.AsyncContextManager


# ── SyncCloseable WARNING fires at registration ────────────────────────


class _SyncCloseableClass:
    def close(self) -> None:
        pass


def test_sync_closeable_warning_at_registration() -> None:
    """SyncCloseable registration emits dual-signal warning exactly once."""
    registry = ProviderRegistry()
    with pytest.warns(LifecycleDetectionWarning, match="close") as record:
        registry.register_class(_SyncCloseableClass, Scope.LOOP)

    assert len(record) == 1
    entry = registry.get(_SyncCloseableClass)
    assert entry.lifecycle == ProviderLifecycle.SyncCloseable


# ── sync-ACM WARNING fires at registration ────────────────────


class _SyncContextManagerClass:
    def __enter__(self) -> "_SyncContextManagerClass":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass


def test_sync_acm_warning_at_registration() -> None:
    """sync-only context manager → Plain + dual-signal warning."""
    registry = ProviderRegistry()
    with pytest.warns(LifecycleDetectionWarning, match="__enter__") as record:
        registry.register_class(_SyncContextManagerClass, Scope.LOOP)

    assert len(record) == 1
    entry = registry.get(_SyncContextManagerClass)
    assert entry.lifecycle == ProviderLifecycle.Plain


# ── explicit lifecycle= override suppresses auto-detection ────────────


class _HybridAcmAndEnter:
    async def __aenter__(self) -> "_HybridAcmAndEnter":
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass

    def __enter__(self) -> "_HybridAcmAndEnter":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass


def test_explicit_lifecycle_override_suppresses_detection() -> None:
    """explicit lifecycle=Plain suppresses auto-detection and warnings."""
    registry = ProviderRegistry()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        registry.register_class(_HybridAcmAndEnter, Scope.LOOP, lifecycle=ProviderLifecycle.Plain)

    entry = registry.get(_HybridAcmAndEnter)
    assert entry.lifecycle == ProviderLifecycle.Plain


# ── Factory shape detection ───────────────────────────────────────────────────


def _sync_factory() -> _Settings:
    return _Settings()


async def _async_factory() -> _Settings:
    return _Settings()


def _sync_gen_factory() -> Iterator[_Settings]:
    yield _Settings()


async def _async_gen_factory() -> AsyncIterator[_Settings]:
    yield _Settings()


@pytest.mark.parametrize(
    ("factory", "expected_shape"),
    [
        (_sync_factory, FactoryShape.SYNC_CALLABLE),
        (_async_factory, FactoryShape.ASYNC_CALLABLE),
        (_sync_gen_factory, FactoryShape.SYNC_GENERATOR),
        (_async_gen_factory, FactoryShape.ASYNC_GENERATOR),
    ],
)
def test_factory_shape_detection(factory: object, expected_shape: FactoryShape) -> None:
    registry = ProviderRegistry()
    unique_type = type(f"_{expected_shape.name}", (), {})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", LifecycleDetectionWarning)
        registry.register_factory(unique_type, Scope.LOOP, factory)
    assert registry.get(unique_type).factory_shape == expected_shape


# ── sync-generator factory WARNING fires at registration ─────────────────


def test_sync_generator_warning_at_registration() -> None:
    """sync-generator factory registration emits dual-signal warning exactly once."""
    registry = ProviderRegistry()
    with pytest.warns(LifecycleDetectionWarning, match="sync generator") as record:
        registry.register_factory(_Settings, Scope.LOOP, _sync_gen_factory)

    assert len(record) == 1
    entry = registry.get(_Settings)
    assert entry.lifecycle == ProviderLifecycle.SyncGenerator


# ── Factory entry.lifecycle values ──────────────────────────────────────────────


def test_factory_lifecycle_async_generator() -> None:
    registry = ProviderRegistry()
    registry.register_factory(_Settings, Scope.LOOP, _async_gen_factory)
    assert registry.get(_Settings).lifecycle == ProviderLifecycle.AsyncGenerator


def test_factory_lifecycle_sync_generator() -> None:
    registry = ProviderRegistry()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", LifecycleDetectionWarning)
        registry.register_factory(_Settings, Scope.LOOP, _sync_gen_factory)
    assert registry.get(_Settings).lifecycle == ProviderLifecycle.SyncGenerator


def test_factory_lifecycle_plain_factory() -> None:
    registry = ProviderRegistry()
    registry.register_factory(_Settings, Scope.LOOP, _sync_factory)
    assert registry.get(_Settings).lifecycle == ProviderLifecycle.PlainFactory


# ── Annotated scope override capture ──────────────────────────────────────────


def _factory_with_annotated(
    client: Annotated[_AsyncGraphClient, Scope.PROCESS],
) -> _Settings:
    return _Settings()


def test_annotated_scope_override_capture() -> None:
    registry = ProviderRegistry()
    registry.register_factory(_Settings, Scope.LOOP, _factory_with_annotated)
    assert (_Settings, _AsyncGraphClient, Scope.PROCESS) in registry._dep_edges


# ── Multiple Scope markers raise DIError ──────────────────────────────────────


def _factory_with_multiple_scopes(
    client: Annotated[_AsyncGraphClient, Scope.LOOP, Scope.PROCESS],
) -> _Settings:
    return _Settings()


def test_multiple_scope_markers_raises_di_error() -> None:
    registry = ProviderRegistry()
    with pytest.raises(DIError, match="multiple Scope markers"):
        registry.register_factory(_Settings, Scope.LOOP, _factory_with_multiple_scopes)


# ── Passthrough parameter exclusion ────────────────────────────────────────────


def _factory_with_ctx(ctx: JobContext[_Payload]) -> _Settings:
    return _Settings()


def test_passthrough_parameter_exclusion() -> None:
    registry = ProviderRegistry()
    registry.register_factory(_Settings, Scope.LOOP, _factory_with_ctx)
    for owner, dep, _ in registry._dep_edges:
        if owner is _Settings:
            assert dep is not JobContext


def test_payload_name_passthrough_exclusion() -> None:
    def factory(payload: _Payload) -> _Settings:
        return _Settings()

    registry = ProviderRegistry()
    registry.register_factory(_Settings, Scope.LOOP, factory)
    for owner, dep, _ in registry._dep_edges:
        if owner is _Settings:
            assert dep is not _Payload


def test_variadic_params_excluded_by_kind_not_name() -> None:
    def factory(*deps: int, **options: str) -> _Settings:
        return _Settings()

    registry = ProviderRegistry()
    registry.register_factory(_Settings, Scope.LOOP, factory)
    for _owner, dep, _ in registry._dep_edges:
        assert dep is not int
        assert dep is not str


# ── get of an unregistered type raises MissingProvider ─────────────────────────


def test_get_unregistered_raises_missing_provider() -> None:
    registry = ProviderRegistry()
    with pytest.raises(MissingProvider, match="Settings"):
        registry.get(_Settings)


# ── providers property returns a copy ──────────────────────────────────────────


def test_providers_property_returns_copy() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    copy = registry.providers
    copy.clear()
    assert _Settings in registry._providers


# ── Non-callable factory raises TypeError ─────────────────────────────────────


def test_non_callable_factory_raises_type_error() -> None:
    registry = ProviderRegistry()
    with pytest.raises(TypeError, match="callable"):
        registry.register_factory(_Settings, Scope.LOOP, "not_a_callable")


# ── validate() stub raises NotImplementedError ─────────────────────────────────


def test_validate_succeeds_on_valid_registry() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    registry.validate()
    assert registry._validated is True
    assert registry._sealed is True
