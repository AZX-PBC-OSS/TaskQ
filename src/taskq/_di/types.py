"""DI type vocabulary for the solver engine."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Literal, Protocol, runtime_checkable

from taskq._di.scope import Scope

type Factory[T] = (
    Callable[..., T]
    | Callable[..., Awaitable[T]]
    | Callable[..., AsyncIterator[T]]
    | Callable[..., Iterator[T]]
)


class FactoryShape(IntEnum):
    """Classification of factory shapes for resolution-time dispatch."""

    SYNC_CALLABLE = 0
    ASYNC_CALLABLE = 1
    ASYNC_GENERATOR = 2
    SYNC_GENERATOR = 3
    VALUE = 4
    CLASS = 5


class ProviderLifecycle(Enum):
    """Provider lifecycle shapes for resolution-time dispatch."""

    Plain = "plain"
    AsyncContextManager = "acm"
    AsyncCloseable = "acloseable"
    SyncCloseable = "scloseable"
    AsyncGenerator = "asyncgen"
    SyncGenerator = "syncgen"
    PlainFactory = "plainfactory"


@dataclass(frozen=True, slots=True)
class ProviderEntry[T]:
    """One registered provider for type T.

    Field-to-shape mapping (canonical):
      kind="value"   → factory_shape == FactoryShape.VALUE
      kind="factory" → factory_shape ∈ {SYNC_CALLABLE, ASYNC_CALLABLE,
                                       ASYNC_GENERATOR, SYNC_GENERATOR}
      kind="class"   → factory_shape == FactoryShape.CLASS
    ``register_value/factory/class`` methods are the single point that
    constructs ``ProviderEntry`` and MUST honour this table.
    """

    type_: type[T]
    scope: Scope
    kind: Literal["value", "factory", "class"]
    impl: object  # Why: heterogeneous (Factory[T] | type[T] | T) — erasure documented
    factory_shape: FactoryShape
    lifecycle: ProviderLifecycle | None = field(default=None)


@runtime_checkable
class ProviderRegistry(Protocol):
    """Read-only registry surface consumed by the solver engine.

    The full implementation (register_value / register_factory /
    register_class / validate / has_provider) is owned by the container
    implementation. The engine only needs the lookup surface defined here.
    """

    @property
    def providers(self) -> dict[type, ProviderEntry[object]]: ...

    # Why: runtime_checkable Protocol cannot express per-method type
    # parameters (PEP 695 + runtime_checkable limitation under pyright).
    # The signature is erased from the published shape
    # ``get[T](type_: type[T]) -> ProviderEntry[T]`` to ``type[object] /
    # ProviderEntry[object]``. This is the sanctioned DI erasure boundary.
    def get(self, type_: type[object]) -> ProviderEntry[object]:
        """Return the registered ProviderEntry for type_.

        Raises:
            MissingProvider: if type_ has no registered provider.

        The engine relies on this contract: when a parameter has no
        registered provider and is not in passthrough_kwargs, the
        MissingProvider raised by this lookup is allowed to propagate.
        Any test double or alternative implementation MUST match this
        raises-contract — returning None or a sentinel breaks the solver.
        """
        ...


@runtime_checkable
class ScopeContainer(Protocol):
    """One scope-lifetime container; owns its own AsyncExitStack.

    The container is responsible for ALL factory invocation, caching,
    and teardown registration. The solver engine NEVER calls a factory
    directly and NEVER touches an AsyncExitStack — every resolution
    goes through ``get_or_create``.
    """

    async def get_or_create(
        self,
        type_: type[object],
        entry: ProviderEntry[object],
    ) -> object:
        """Return a resolved instance for type_, creating it if needed."""
        ...

    @property
    def last_cache_hit(self) -> bool:
        """Whether the most recent ``get_or_create`` served a cached value."""
        ...

    async def aclose(self) -> None:
        """Close the container's internal AsyncExitStack with the
        log-and-continue policy.
        """
        ...
