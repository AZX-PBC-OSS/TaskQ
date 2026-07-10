"""Hypothesis property tests for the DI lifecycle system.

Scenarios:
  LIFO teardown across all five lifecycle shapes (Plain, ACM,
         AsyncCloseable, SyncCloseable, AsyncGenerator). Hypothesis
         generates combinations of 1-5 providers; each records its
         teardown index at the moment teardown is *initiated*; oracle:
         the recorded order is reverse of construction order for every
         generated combination.

DAG resolution and cycle-detection property tests are
preserved below (DAG cycles).

SyncCloseable note (decompose-review-1 W-7):
  For the SyncCloseable shape, the teardown wraps instance.close() via
  asyncio.to_thread, so the synchronous body runs on the loop's default
  thread pool. Recording the teardown index from inside the synchronous
  close() body would race against thread-pool scheduling and is
  non-deterministic. To keep the property test deterministic, the
  SyncCloseable test class records its index in a wrapper teardown
  closure that runs on the event loop BEFORE delegating to
  asyncio.to_thread(self.close) - i.e. record at *initiation*, not at
  *completion*. The wrapper closure MUST await asyncio.to_thread(
  self.close) so the thread-pool dispatch still occurs (verifying the
  asyncio.to_thread path remains present); the index recording is the
  observable oracle. Do NOT "fix" this to record inside the sync body.
"""

import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import ScopeContainer
from taskq._di.solver import solve_dependencies
from taskq._di.types import FactoryShape, ProviderEntry, ProviderLifecycle
from taskq.exceptions import DependencyCycle, MissingProvider


async def _stub_resolver(func: Any, **kw: Any) -> dict[str, object]:
    return {}


# ── Shape strategy ─────────────────────────────────────────────────

_LIFECYCLE_SHAPES = [
    ProviderLifecycle.Plain,
    ProviderLifecycle.AsyncContextManager,
    ProviderLifecycle.AsyncCloseable,
    ProviderLifecycle.SyncCloseable,
    ProviderLifecycle.AsyncGenerator,
]

_SHAPE_NAMES: dict[ProviderLifecycle, str] = {
    ProviderLifecycle.Plain: "plain",
    ProviderLifecycle.AsyncContextManager: "acm",
    ProviderLifecycle.AsyncCloseable: "acloseable",
    ProviderLifecycle.SyncCloseable: "scloseable",
    ProviderLifecycle.AsyncGenerator: "asyncgen",
}


@st.composite
def _shape_combo_strategy(
    draw: st.DrawFn,
) -> list[tuple[ProviderLifecycle, str]]:
    """Generate a list of (shape, label) tuples, size 1-5."""
    n = draw(st.integers(min_value=1, max_value=5))
    shapes = draw(
        st.lists(
            st.sampled_from(_LIFECYCLE_SHAPES),
            min_size=n,
            max_size=n,
        )
    )
    return [(shape, f"{_SHAPE_NAMES[shape]}_{i}") for i, shape in enumerate(shapes)]


# ── Test doubles that record teardown index at initiation ───────────

_teardown_order: list[str] = []


def _make_acm_type(label: str) -> type:
    """Create a unique ACM class that records its label on __aexit__."""

    class _AcmSpy:
        async def __aenter__(self) -> "_AcmSpy":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: Any,
        ) -> None:
            _teardown_order.append(label)

    _AcmSpy.__qualname__ = label
    return _AcmSpy


def _make_async_closeable_type(label: str) -> type:
    """Create a unique AsyncCloseable class that records its label on aclose()."""

    class _AsyncCloseableSpy:
        async def aclose(self) -> None:
            _teardown_order.append(label)

    _AsyncCloseableSpy.__qualname__ = label
    return _AsyncCloseableSpy


class _SyncCloseableSpyBase:
    """Base for SyncCloseable spys - has close() for detection."""

    close_called = False
    close_from_thread: threading.Thread | None = None

    def close(self) -> None:
        self.close_called = True
        self.close_from_thread = threading.current_thread()


def _make_sync_closeable_wrapper_teardown(inst: _SyncCloseableSpyBase, label: str) -> Any:
    """Wrapper teardown: records label at initiation, then delegates to asyncio.to_thread(close).

    Per the module docstring: the index is recorded at *initiation*
    (on the event loop), not at *completion* (inside the sync body on
    the thread pool), to avoid thread-pool scheduling non-determinism.
    The close() method still runs via asyncio.to_thread to verify that
    code path remains present.
    """

    async def _teardown() -> None:
        _teardown_order.append(label)
        await asyncio.to_thread(inst.close)

    return _teardown


def _make_async_gen_spy_factory(label: str) -> Any:
    """Create an async generator factory that records teardown index at cleanup."""

    async def _factory() -> AsyncIterator[str]:
        yield label
        _teardown_order.append(label)

    return _factory


# ── LIFO across all shapes ─────────────────────────────────


@given(combo=_shape_combo_strategy())
async def test_lifo_teardown_across_all_shapes(
    combo: list[tuple[ProviderLifecycle, str]],
) -> None:
    """LIFO teardown holds for any combination of lifecycle shapes.

    For each generated combination, create providers at LOOP scope,
    resolve them via ScopeContainer.get_or_create, then close. Each
    provider records its label in _teardown_order at the moment its
    teardown is initiated. Oracle: the recorded order is the reverse
    of construction order (LIFO) for every generated combination.
    Plain providers have no teardown and should NOT appear in the
    teardown order.
    """
    global _teardown_order
    _teardown_order = []

    container = ScopeContainer(scope=Scope.LOOP, resolver=_stub_resolver)
    construction_order: list[str] = []
    sync_closeable_teardown_indices: dict[int, tuple[_SyncCloseableSpyBase, str]] = {}
    entry: ProviderEntry[object]  # pyright: ignore[reportUnknownVariableType] # Why: unique_type is dynamically created; T erased per section 9

    for lifecycle, label in combo:
        if lifecycle == ProviderLifecycle.Plain:

            class _PlainType:
                pass

            _PlainType.__qualname__ = label
            entry = ProviderEntry(
                type_=_PlainType,
                scope=Scope.LOOP,
                kind="class",
                impl=_PlainType,
                factory_shape=FactoryShape.CLASS,
                lifecycle=ProviderLifecycle.Plain,
            )
            await container.get_or_create(_PlainType, entry)
        elif lifecycle == ProviderLifecycle.AsyncContextManager:
            acm_type = _make_acm_type(label)
            entry = ProviderEntry(  # pyright: ignore[reportUnknownVariableType] # Why: dynamically created type; T erased per section 9
                type_=acm_type,
                scope=Scope.LOOP,
                kind="class",
                impl=acm_type,
                factory_shape=FactoryShape.CLASS,
                lifecycle=ProviderLifecycle.AsyncContextManager,
            )
            await container.get_or_create(acm_type, entry)
        elif lifecycle == ProviderLifecycle.AsyncCloseable:
            acloseable_type = _make_async_closeable_type(label)
            entry = ProviderEntry(  # pyright: ignore[reportUnknownVariableType] # Why: same as above
                type_=acloseable_type,
                scope=Scope.LOOP,
                kind="class",
                impl=acloseable_type,
                factory_shape=FactoryShape.CLASS,
                lifecycle=ProviderLifecycle.AsyncCloseable,
            )
            await container.get_or_create(acloseable_type, entry)
        elif lifecycle == ProviderLifecycle.SyncCloseable:

            class _SyncCloseType(_SyncCloseableSpyBase):
                pass

            _SyncCloseType.__qualname__ = label
            entry = ProviderEntry(
                type_=_SyncCloseType,
                scope=Scope.LOOP,
                kind="class",
                impl=_SyncCloseType,
                factory_shape=FactoryShape.CLASS,
                lifecycle=ProviderLifecycle.SyncCloseable,
            )
            await container.get_or_create(_SyncCloseType, entry)
            sync_closeable_teardown_indices[len(container._teardowns) - 1] = (
                _SyncCloseType(),
                label,
            )  # pyright: ignore[reportPrivateUsage] # Why: must capture the SyncCloseable spy instance for wrapper teardown replacement; _teardowns is the only seam available for the per-initiation recording pattern
        elif lifecycle == ProviderLifecycle.AsyncGenerator:
            factory = _make_async_gen_spy_factory(label)

            class _AsyncGenType:
                pass

            _AsyncGenType.__qualname__ = label
            entry = ProviderEntry(
                type_=_AsyncGenType,
                scope=Scope.LOOP,
                kind="factory",
                impl=factory,
                factory_shape=FactoryShape.ASYNC_GENERATOR,
                lifecycle=ProviderLifecycle.AsyncGenerator,
            )
            await container.get_or_create(_AsyncGenType, entry)

        construction_order.append(label)

    for tdi, (spy, label) in sync_closeable_teardown_indices.items():
        wrapper = _make_sync_closeable_wrapper_teardown(spy, label)
        container._teardowns[tdi] = wrapper  # pyright: ignore[reportPrivateUsage] # Why: property test must replace the bare-lambda SyncCloseable teardown with the wrapper that records at initiation per the module docstring; the _teardowns list is the only seam available

    await container.aclose()

    expected = [lbl for lbl in reversed(construction_order) if not lbl.startswith("plain_")]
    assert expected == _teardown_order, (
        f"teardown order {_teardown_order} != expected LIFO {expected}"
    )

    for spy, _label in sync_closeable_teardown_indices.values():
        assert spy.close_called


# ── Property tests (DAG resolution, cycle detection) ──


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
    """ScopeContainer test double with scope-aware caching and LIFO teardown."""

    def __init__(self, scope: Scope) -> None:
        self._scope = scope
        self._cache: dict[type, object] = {}
        self._stack = AsyncExitStack()
        self._resolution_order: list[type] = []
        self._teardown_order: list[type] = []
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
        elif shape == FactoryShape.ASYNC_GENERATOR:
            cm = asynccontextmanager(entry.impl)()  # type: ignore[arg-type] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
            value = cast(object, await self._stack.enter_async_context(cm))
        elif shape == FactoryShape.ASYNC_CALLABLE:
            value = cast(object, await entry.impl())  # type: ignore[call-arg,misc] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
        elif shape == FactoryShape.SYNC_CALLABLE:
            value = cast(object, entry.impl())  # type: ignore[call-arg,misc] # Why: entry.impl is object at the erasure boundary; runtime guarantee from FactoryShape dispatch
        else:
            raise NotImplementedError(f"factory_shape {shape!r} not implemented in test double")

        if self._scope is not Scope.TRANSIENT:
            self._cache[type_] = value

        self._resolution_order.append(type_)

        async def _record_teardown(t: type = type_) -> None:
            self._teardown_order.append(t)

        self._stack.push_async_callback(_record_teardown)
        return value

    async def aclose(self) -> None:
        try:  # noqa: SIM105 — Why: log-and-continue teardown per section 7.4; contextlib.suppress does not support await; errors are intentionally swallowed to avoid masking the primary failure.
            await self._stack.aclose()
        except Exception:  # noqa: S110 — Why: intentional log-and-continue teardown per section 7.4; swallowing here so subsequent teardowns run.
            pass


def _make_containers() -> dict[Scope, _StubScopeContainer]:
    return {scope: _StubScopeContainer(scope) for scope in Scope}


class _ProviderSpec:
    def __init__(self, type_: type, scope: Scope, deps: list[type]) -> None:
        self.type_ = type_
        self.scope = scope
        self.deps = deps


class _P0:
    pass


class _P1:
    pass


class _P2:
    pass


class _P3:
    pass


class _P4:
    pass


_STUB_TYPES: list[type] = [_P0, _P1, _P2, _P3, _P4]


@st.composite
def _dag_strategy(draw: st.DrawFn) -> list[_ProviderSpec]:
    n = draw(st.integers(min_value=1, max_value=5))
    specs: list[_ProviderSpec] = []

    for i in range(n):
        type_ = _STUB_TYPES[i]

        if i == 0:
            scope = draw(st.sampled_from(list(Scope)))
            specs.append(_ProviderSpec(type_=type_, scope=scope, deps=[]))
            continue

        dep_subset = draw(
            st.lists(
                st.sampled_from([s.type_ for s in specs]),
                unique=True,
                max_size=len(specs),
            )
        )

        if dep_subset:
            max_dep_scope_value = max(
                next(s.scope for s in specs if s.type_ is dt) for dt in dep_subset
            )
            min_scope = Scope(max_dep_scope_value)
            allowed_scopes = [s for s in Scope if s >= min_scope]
        else:
            allowed_scopes = list(Scope)

        scope = draw(st.sampled_from(allowed_scopes))

        if scope is Scope.TRANSIENT:
            dep_subset = []

        specs.append(_ProviderSpec(type_=type_, scope=scope, deps=dep_subset))

    return specs


def _build_registry_and_actor(
    specs: list[_ProviderSpec],
) -> tuple[_StubRegistry, object, dict[type, int]]:
    call_counts: dict[type, int] = {spec.type_: 0 for spec in specs}
    entries: dict[type, ProviderEntry[object]] = {}

    for spec in specs:
        t = spec.type_

        if spec.deps:
            if spec.scope is Scope.TRANSIENT:

                async def _factory_transient(
                    _t: type = t,
                    _deps: list[type] = spec.deps,
                    _counts: dict[type, int] = call_counts,
                ) -> object:
                    _counts[_t] += 1
                    return _t()

            else:

                async def _factory_with_deps(
                    _t: type = t,
                    _deps: list[type] = spec.deps,
                    _counts: dict[type, int] = call_counts,
                ) -> AsyncIterator[object]:
                    _counts[_t] += 1
                    yield _t()

            impl = _factory_transient if spec.scope is Scope.TRANSIENT else _factory_with_deps  # pyright: ignore[reportPossiblyUnboundVariable] # Why: both names are defined in the correlated if/else branches above; pyright does not track the branch correlation
            shape = (
                FactoryShape.ASYNC_CALLABLE
                if spec.scope is Scope.TRANSIENT
                else FactoryShape.ASYNC_GENERATOR
            )
        else:
            if spec.scope is Scope.TRANSIENT:

                async def _leaf_transient(
                    _t: type = t,
                    _counts: dict[type, int] = call_counts,
                ) -> object:
                    _counts[_t] += 1
                    return _t()

                impl = _leaf_transient
                shape = FactoryShape.ASYNC_CALLABLE
            else:

                async def _leaf_generator(
                    _t: type = t,
                    _counts: dict[type, int] = call_counts,
                ) -> AsyncIterator[object]:
                    _counts[_t] += 1
                    yield _t()

                impl = _leaf_generator
                shape = FactoryShape.ASYNC_GENERATOR

        entries[t] = ProviderEntry(
            type_=t,
            scope=spec.scope,
            kind="factory",
            impl=impl,
            factory_shape=shape,
        )

    annotations: dict[str, type] = {}
    for i, spec in enumerate(specs):
        annotations[f"p{i}"] = spec.type_

    def _make_actor(anns: dict[str, type]) -> object:
        if not anns:
            return lambda: None

        lines = ["async def _generated_actor("]
        for name, tp in anns.items():
            lines.append(f"    {name}: {tp.__name__},")
        lines.append(") -> None: pass")

        ns: dict[str, object] = {tp.__name__: tp for tp in _STUB_TYPES}
        exec("\n".join(lines), ns)  # noqa: S102 — Why: dynamically constructing actor functions with correct __annotations__ from generated DAG specs; no user input flows into the exec string.
        return ns["_generated_actor"]

    actor = _make_actor(annotations)
    return _StubRegistry(entries), actor, call_counts


@given(specs=_dag_strategy())
async def test_dag_resolution_and_teardown(specs: list[_ProviderSpec]) -> None:
    """DAG variant: Any valid DAG of up to 5 providers resolves; teardown is LIFO."""
    registry, actor, call_counts = _build_registry_and_actor(specs)
    containers = _make_containers()

    kwargs = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    for i, spec in enumerate(specs):
        param_name = f"p{i}"
        assert param_name in kwargs, f"parameter {param_name} not resolved"
        value = kwargs[param_name]
        assert isinstance(value, spec.type_), f"{param_name} is not {spec.type_.__name__}"

    for spec in specs:
        if spec.scope is not Scope.TRANSIENT:
            assert call_counts[spec.type_] == 1, (
                f"{spec.type_.__name__} at {spec.scope.name} called {call_counts[spec.type_]} times, expected 1"
            )

    kwargs2 = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
    )

    for i, spec in enumerate(specs):
        param_name = f"p{i}"
        if spec.scope is Scope.TRANSIENT:
            assert kwargs[param_name] is not kwargs2[param_name], (
                f"{param_name} at TRANSIENT should be distinct on second resolve"
            )
        else:
            assert kwargs[param_name] is kwargs2[param_name], (
                f"{param_name} at {spec.scope.name} should be same on second resolve"
            )

    for scope in Scope:
        await containers[scope].aclose()

    for scope in Scope:
        container = containers[scope]
        if len(container._resolution_order) > 1:
            assert container._teardown_order == list(reversed(container._resolution_order)), (
                f"LIFO teardown order not preserved for {scope.name}"
            )


# ── Cycle detection via Hypothesis ────────────────────────


class _N0:
    pass


class _N1:
    pass


class _N2:
    pass


class _N3:
    pass


class _N4:
    pass


class _N5:
    pass


class _N6:
    pass


class _N7:
    pass


class _N8:
    pass


class _N9:
    pass


_GRAPH_NODES: list[type] = [_N0, _N1, _N2, _N3, _N4, _N5, _N6, _N7, _N8, _N9]


def _has_cycle_dfs(adj: dict[int, list[int]]) -> bool:
    visited: set[int] = set()
    for start in adj:
        if start in visited:
            continue
        stack: list[int] = [start]
        path_set: set[int] = {start}
        children_idx: dict[int, int] = {}
        children_list: dict[int, list[int]] = {}

        while stack:
            current = stack[-1]
            if current not in children_list:
                children_list[current] = adj.get(current, [])
                children_idx[current] = 0

            idx = children_idx[current]
            neighbors = children_list[current]

            if idx < len(neighbors):
                neighbor = neighbors[idx]
                children_idx[current] = idx + 1
                if neighbor in path_set:
                    return True
                if neighbor not in visited:
                    stack.append(neighbor)
                    path_set.add(neighbor)
            else:
                stack.pop()
                path_set.discard(current)
                visited.add(current)

    return False


@st.composite
def _random_digraph_strategy(
    draw: st.DrawFn,
) -> tuple[int, dict[int, list[int]]]:
    n = draw(st.integers(min_value=2, max_value=10))
    adj: dict[int, list[int]] = {i: [] for i in range(n)}

    for src in range(n):
        possible_targets = [t for t in range(n) if t != src]
        n_edges = draw(st.integers(min_value=0, max_value=min(len(possible_targets), n)))
        targets = draw(
            st.lists(
                st.sampled_from(possible_targets),
                unique=True,
                min_size=n_edges,
                max_size=n_edges,
            )
        )
        adj[src] = targets

    return n, adj


def _register_factories_from_graph(
    registry: ProviderRegistry,
    n: int,
    adj: dict[int, list[int]],
) -> None:
    for src in range(n):
        src_type = _GRAPH_NODES[src]
        dep_types = [_GRAPH_NODES[t] for t in adj.get(src, [])]

        if not dep_types:
            registry.register_factory(
                src_type,
                Scope.TRANSIENT,
                lambda _t=src_type: _t(),
            )
        else:
            params = ", ".join(f"p{i}: {_dep_t.__name__}" for i, _dep_t in enumerate(dep_types))
            lines = [
                "async def _factory(" + params + ") -> object:",
                "    return _SRC_TYPE()",
            ]
            ns: dict[str, object] = {dt.__name__: dt for dt in dep_types}
            ns["_SRC_TYPE"] = src_type
            exec("\n".join(lines), ns)  # noqa: S102 — Why: dynamically constructing factory functions from Hypothesis-generated graph specs; no user input flows into the exec string.
            factory = ns["_factory"]
            registry.register_factory(
                src_type,
                Scope.TRANSIENT,
                factory,
            )


@given(params=_random_digraph_strategy())
async def test_cycle_detection_via_hypothesis(
    params: tuple[int, dict[int, list[int]]],
) -> None:
    """validate() raises DependencyCycle iff the oracle DFS detects a cycle."""
    n, adj = params
    oracle_has_cycle = _has_cycle_dfs(adj)

    registry = ProviderRegistry()
    _register_factories_from_graph(registry, n, adj)

    if oracle_has_cycle:
        with pytest.raises(DependencyCycle):
            registry.validate()
    else:
        registry.validate()
