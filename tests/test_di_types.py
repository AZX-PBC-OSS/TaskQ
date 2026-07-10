"""Unit tests for DI type vocabulary (Factory, FactoryShape, ProviderEntry, Protocols)."""

from collections.abc import AsyncIterator, Iterator
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from taskq._di.scope import Scope
from taskq._di.types import (
    Factory,
    FactoryShape,
    ProviderEntry,
    ProviderLifecycle,
    ProviderRegistry,
    ScopeContainer,
)

_REPO_ROOT = Path(__file__).parent.parent
_TYPES_PY = _REPO_ROOT / "src/taskq/_di/types.py"
_INIT_PY = _REPO_ROOT / "src/taskq/_di/__init__.py"

_FORBIDDEN_IMPORT = "from __future__ import annotations"


# ── ProviderLifecycle enum ──────────────────────────────────────────────────


def test_provider_lifecycle_has_seven_members() -> None:
    assert list(ProviderLifecycle) == [
        ProviderLifecycle.Plain,
        ProviderLifecycle.AsyncContextManager,
        ProviderLifecycle.AsyncCloseable,
        ProviderLifecycle.SyncCloseable,
        ProviderLifecycle.AsyncGenerator,
        ProviderLifecycle.SyncGenerator,
        ProviderLifecycle.PlainFactory,
    ]


def test_provider_lifecycle_string_values() -> None:
    assert ProviderLifecycle.Plain.value == "plain"
    assert ProviderLifecycle.AsyncContextManager.value == "acm"
    assert ProviderLifecycle.AsyncCloseable.value == "acloseable"
    assert ProviderLifecycle.SyncCloseable.value == "scloseable"
    assert ProviderLifecycle.AsyncGenerator.value == "asyncgen"
    assert ProviderLifecycle.SyncGenerator.value == "syncgen"
    assert ProviderLifecycle.PlainFactory.value == "plainfactory"


def test_provider_entry_lifecycle_defaults_none() -> None:
    entry = ProviderEntry(
        type_=int,
        scope=Scope.PROCESS,
        kind="value",
        impl=42,
        factory_shape=FactoryShape.VALUE,
    )
    assert entry.lifecycle is None


def test_provider_entry_lifecycle_explicit() -> None:
    entry = ProviderEntry(
        type_=int,
        scope=Scope.PROCESS,
        kind="class",
        impl=int,
        factory_shape=FactoryShape.CLASS,
        lifecycle=ProviderLifecycle.Plain,
    )
    assert entry.lifecycle is ProviderLifecycle.Plain


# ── FactoryShape enum ──────────────────────────────────────────────────────


def test_factory_shape_has_six_members() -> None:
    assert list(FactoryShape) == [
        FactoryShape.SYNC_CALLABLE,
        FactoryShape.ASYNC_CALLABLE,
        FactoryShape.ASYNC_GENERATOR,
        FactoryShape.SYNC_GENERATOR,
        FactoryShape.VALUE,
        FactoryShape.CLASS,
    ]


def test_factory_shape_integer_values() -> None:
    assert [m.value for m in FactoryShape] == [0, 1, 2, 3, 4, 5]


# ── ProviderEntry dataclass ────────────────────────────────────────────────


def test_provider_entry_construction() -> None:
    entry = ProviderEntry(
        type_=int,
        scope=Scope.PROCESS,
        kind="value",
        impl=42,
        factory_shape=FactoryShape.VALUE,
    )
    assert entry.type_ is int
    assert entry.scope is Scope.PROCESS
    assert entry.kind == "value"
    assert entry.impl == 42
    assert entry.factory_shape is FactoryShape.VALUE


def test_provider_entry_frozen() -> None:
    entry = ProviderEntry(
        type_=int,
        scope=Scope.PROCESS,
        kind="value",
        impl=42,
        factory_shape=FactoryShape.VALUE,
    )
    with pytest.raises(FrozenInstanceError):
        entry.impl = 99  # type: ignore[misc] # Why: intentionally assigning to a frozen dataclass field to prove FrozenInstanceError is raised; pyright flags the write as misc error on the frozen dataclass


_KIND_SHAPE_PAIRS: list[tuple[str, FactoryShape]] = [
    ("value", FactoryShape.VALUE),
    ("factory", FactoryShape.SYNC_CALLABLE),
    ("factory", FactoryShape.ASYNC_CALLABLE),
    ("factory", FactoryShape.ASYNC_GENERATOR),
    ("factory", FactoryShape.SYNC_GENERATOR),
    ("class", FactoryShape.CLASS),
]


@pytest.mark.parametrize("kind, shape", _KIND_SHAPE_PAIRS)
def test_provider_entry_kind_shape_mapping(kind: str, shape: FactoryShape) -> None:
    ProviderEntry(
        type_=int,
        scope=Scope.PROCESS,
        kind=kind,
        impl=object(),
        factory_shape=shape,
    )


# ── ProviderRegistry Protocol ──────────────────────────────────────────────


class _StubRegistry:
    """Minimal implementation of ProviderRegistry for isinstance check."""

    def __init__(self, providers: dict[type, ProviderEntry[object]] | None = None) -> None:
        self._providers = providers or {}

    @property
    def providers(self) -> dict[type, ProviderEntry[object]]:
        return self._providers

    def get(self, type_: type[object]) -> ProviderEntry[object]:
        if type_ in self._providers:
            return self._providers[type_]
        from taskq.exceptions import MissingProvider

        raise MissingProvider(type_name=str(type_), required_by="test")


def test_provider_registry_runtime_checkable() -> None:
    assert isinstance(_StubRegistry(), ProviderRegistry)


# ── ScopeContainer Protocol ────────────────────────────────────────────────


class _StubContainer:
    """Minimal implementation of ScopeContainer for isinstance check."""

    @property
    def last_cache_hit(self) -> bool:
        return False

    async def get_or_create(
        self,
        type_: type[object],
        entry: ProviderEntry[object],
    ) -> object:
        return entry.impl

    async def aclose(self) -> None:
        return None


async def test_scope_container_runtime_checkable() -> None:
    assert isinstance(_StubContainer(), ScopeContainer)


# ── Factory[T] type alias compile-time assertions ──────────────────────────


def _sync_int() -> int:
    return 1


async def _async_int() -> int:
    return 1


async def _async_gen_int() -> AsyncIterator[int]:
    yield 1


def _sync_gen_int() -> Iterator[int]:
    yield 1


def test_factory_shapes_assignable() -> None:
    """Compile-time assertion: each factory shape is assignable to Factory[int].

    Pyright validates these four assignments type-check against Factory[int].
    The runtime assert uses the callable to satisfy ruff F841.
    """
    sync_fn: Factory[int] = _sync_int
    async_fn: Factory[int] = _async_int
    async_gen_fn: Factory[int] = _async_gen_int
    sync_gen_fn: Factory[int] = _sync_gen_int

    assert sync_fn() == 1
    assert async_fn is _async_int
    assert async_gen_fn is _async_gen_int
    assert sync_gen_fn is _sync_gen_int


# ── No from __future__ import annotations ───────────────────────────


@pytest.mark.parametrize("path", [_TYPES_PY, _INIT_PY])
def test_no_future_annotations(path: Path) -> None:
    assert _FORBIDDEN_IMPORT not in path.read_text()


# ── Re-export wiring ───────────────────────────────────────────────────────


def test_init_re_exports() -> None:
    from taskq._di import (
        Factory,
        FactoryShape,
        ProviderEntry,
        ProviderLifecycle,
        ProviderRegistry,
        ScopeContainer,
    )

    assert Factory is not None
    assert FactoryShape is not None
    assert ProviderEntry is not None
    assert ProviderLifecycle is not None
    assert ProviderRegistry is not None
    assert ScopeContainer is not None


def test_di_public_surface_re_exports_provider_lifecycle() -> None:
    from taskq.di import ProviderLifecycle as PublicLifecycle

    assert PublicLifecycle is ProviderLifecycle
