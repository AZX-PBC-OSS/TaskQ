"""Concrete ProviderRegistry — registration API and seal mechanics.

The public surface (register_value / register_factory / register_class,
has_provider, get[T], providers, validate) matches the
published block. The validate() method delegates the five-phase startup
validation algorithm to ``_di._validate.run_validation``.
"""

import inspect
import warnings
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import structlog

from taskq._di._utils import (
    _origin_is_job_context,  # pyright: ignore[reportPrivateUsage] — internal helper shared within _di package; the _di prefix itself signals package-level privacy
)
from taskq._di._validate import (
    _emit_redundant_override_warnings,  # pyright: ignore[reportPrivateUsage] — internal helper shared within _di package; the _di prefix itself signals package-level privacy
    run_validation,
)
from taskq._di.lifecycle import detect_factory_lifecycle, detect_lifecycle
from taskq._di.scope import LifecycleDetectionWarning, Scope
from taskq._di.types import Factory, FactoryShape, ProviderEntry, ProviderLifecycle
from taskq.exceptions import DIError, MissingProvider

if TYPE_CHECKING:
    from taskq.actor import ActorRef
    from taskq.ratelimit.registry import RateLimitRegistry

logger = structlog.get_logger("taskq._di.registry")


class ProviderRegistry:
    """Mutable provider registry with seal guard and edge capture.

    Structurally satisfies the ``taskq._di.types.ProviderRegistry``
    Protocol without explicit inheritance — the Protocol is
    ``runtime_checkable`` for isinstance checks, but the concrete class
    does not inherit from it to avoid forcing runtime_checkable
    constraints on the implementation.
    """

    def __init__(self) -> None:
        self._providers: dict[type, ProviderEntry[object]] = {}
        self._dep_edges: list[tuple[type, type, Scope | None]] = []
        self._validated: bool = False
        self._sealed: bool = False
        self._plan_cache: dict[tuple[str, Scope], list[type]] = {}
        self._validating: bool = False

    def register_value[T](self, type_: type[T], scope: Scope, value: T) -> None:
        if self._sealed:
            raise RuntimeError("registry is sealed after validate()")
        if type_ in self._providers:
            raise ValueError(f"{type_!r} is already registered")
        entry = ProviderEntry(
            type_=type_,
            scope=scope,
            kind="value",
            impl=value,
            factory_shape=FactoryShape.VALUE,
        )
        self._providers[type_] = entry
        logger.debug(
            "provider-registered",
            type_name=type_.__qualname__,
            scope=scope.name,
            kind="value",
        )

    def register_factory[T](self, type_: type[T], scope: Scope, factory: Factory[T]) -> None:
        if self._sealed:
            raise RuntimeError("registry is sealed after validate()")
        if type_ in self._providers:
            raise ValueError(f"{type_!r} is already registered")
        if not callable(factory):
            raise TypeError(f"factory must be callable, got {type(factory).__qualname__}")
        if inspect.isasyncgenfunction(factory):
            shape = FactoryShape.ASYNC_GENERATOR
        elif inspect.isgeneratorfunction(factory):
            shape = FactoryShape.SYNC_GENERATOR
        elif inspect.iscoroutinefunction(factory):
            shape = FactoryShape.ASYNC_CALLABLE
        else:
            shape = FactoryShape.SYNC_CALLABLE

        lifecycle = detect_factory_lifecycle(factory)

        if lifecycle is ProviderLifecycle.SyncGenerator:
            warnings.warn(
                LifecycleDetectionWarning(
                    f"{getattr(factory, '__qualname__', repr(factory))} is a sync "
                    f"generator factory; its cleanup will run synchronously and "
                    f"cannot be awaited by the DI engine."
                ),
                stacklevel=2,
            )
            logger.warning(
                "sync_generator_registered",
                factory_name=getattr(factory, "__qualname__", repr(factory)),
                scope=scope.name,
            )

        edges = _collect_dep_edges(factory, type_)
        self._dep_edges.extend(edges)

        entry = ProviderEntry(
            type_=type_,
            scope=scope,
            kind="factory",
            impl=factory,
            factory_shape=shape,
            lifecycle=lifecycle,
        )
        self._providers[type_] = entry
        logger.debug(
            "provider-registered",
            type_name=type_.__qualname__,
            scope=scope.name,
            kind="factory",
            factory_shape=shape.name,
        )

    def register_class[T](
        self,
        type_: type[T],
        scope: Scope,
        *,
        lifecycle: ProviderLifecycle | None = None,
    ) -> None:
        if self._sealed:
            raise RuntimeError("registry is sealed after validate()")
        if type_ in self._providers:
            raise ValueError(f"{type_!r} is already registered")

        resolved_lifecycle = lifecycle if lifecycle is not None else detect_lifecycle(type_)

        if lifecycle is None:
            if resolved_lifecycle is ProviderLifecycle.SyncCloseable:
                warnings.warn(
                    LifecycleDetectionWarning(
                        f"{type_.__qualname__} has close() but no aclose(); "
                        f"the DI engine will call close() via asyncio.to_thread "
                        f"at teardown. Prefer an async close (aclose) for "
                        f"async-native code."
                    ),
                    stacklevel=2,
                )
                logger.warning(
                    "sync_close_registered",
                    class_name=type_.__qualname__,
                    scope=scope.name,
                )
            elif (
                resolved_lifecycle is ProviderLifecycle.Plain
                and hasattr(type_, "__enter__")
                and not hasattr(type_, "__aenter__")
            ):
                warnings.warn(
                    LifecycleDetectionWarning(
                        f"{type_.__qualname__} has __enter__/__exit__ but no "
                        f"__aenter__/__aexit__; sync context managers are not "
                        f"supported by the async DI engine. The class will be "
                        f"managed as Plain (no teardown)."
                    ),
                    stacklevel=2,
                )
                logger.warning(
                    "sync_context_manager_not_supported",
                    class_name=type_.__qualname__,
                    scope=scope.name,
                )

        edges = _collect_dep_edges(type_.__init__, type_)
        self._dep_edges.extend(edges)

        entry = ProviderEntry(
            type_=type_,
            scope=scope,
            kind="class",
            impl=type_,
            factory_shape=FactoryShape.CLASS,
            lifecycle=resolved_lifecycle,
        )
        self._providers[type_] = entry
        logger.debug(
            "provider-registered",
            type_name=type_.__qualname__,
            scope=scope.name,
            kind="class",
        )

    def has_provider(self, type_: type) -> bool:
        return type_ in self._providers

    def get[T](self, type_: type[T]) -> ProviderEntry[T]:
        entry = self._providers.get(type_)
        if entry is None:
            raise MissingProvider(
                type_name=type_.__qualname__,
                required_by="<unknown>",
            )
        return cast(
            ProviderEntry[T], entry
        )  # Why: internal map is dict[type, ProviderEntry[object]] (erasure boundary); get[T] recovers the type parameter

    @property
    def providers(self) -> dict[type, ProviderEntry[object]]:
        """Shallow copy preventing external mutation.

        Why: the internal map is dict[type, ProviderEntry[object]] per
        the erasure boundary.
        Returning a copy prevents callers from mutating registry state.
        """
        return dict(self._providers)

    def validate(
        self,
        actors: list["ActorRef[Any, Any]"] | None = None,
        rate_limit_registry: "RateLimitRegistry | None" = None,
    ) -> None:
        """Walk all providers and actors; raise on first error; seal on success.

        Pure graph-walk over registration-time metadata. Never invokes a
        factory, calls a resolver, or performs await — the entire algorithm
        is synchronous introspection on ``_providers`` and ``_dep_edges``.

        actors: explicit list of ActorRef instances whose DI parameter
        annotations are walked for MissingProvider checks and plan-cache
        population. Defaults to None (no actor walk — only
        provider→provider edges are validated).

        rate_limit_registry: when provided, each actor's ``rate_limits``
        and ``reservations`` lists are checked against the registry's
        dicts. Unknown names raise ``MissingProvider`` at startup.
        When ``None`` (default), the name-check phase is
        skipped entirely.

        # Why: ActorRef[Any, Any] is the sanctioned erasure
        # boundary for the heterogeneous actor registry — each ActorRef has
        # different P and R type parameters, and validate() does not need
        # per-actor narrowing. The same erasure is used at the existing API
        # boundary in worker/run.py:283 and cli.py:98.
        """
        if self._validated:
            return
        if self._validating:
            raise RuntimeError("validate() called recursively or concurrently")
        self._validating = True
        try:
            self._plan_cache = run_validation(
                self._providers,
                self._dep_edges,
                actors,
                rate_limit_registry,
            )
            if actors is not None:
                _emit_redundant_override_warnings(actors, self._providers)
            self._validated = True
            self._sealed = True
            logger.info(
                "registry-validated",
                provider_count=len(self._providers),
                actor_count=len(actors) if actors else 0,
            )
        finally:
            self._validating = False


def _collect_dep_edges(
    callable_: object,
    owner_type: type,
) -> list[tuple[type, type, Scope | None]]:
    """Walk ``callable_``'s parameter annotations and capture dependency edges.

    Returns ``(owner_type, dep_type, override_scope_or_None)`` tuples.
    ``owner_type`` is the provider type being registered; ``dep_type`` is
    the annotated dependency; ``override_scope_or_None`` is ``None`` for
    late binding or the explicit ``Scope`` from ``Annotated``.
    """
    if not callable(callable_):
        return []

    try:
        hints = get_type_hints(callable_, include_extras=True)
    except NameError as err:
        qualname = getattr(callable_, "__qualname__", repr(callable_))
        raise DIError(f"unresolvable annotation in {qualname}: {err}") from err

    sig = inspect.signature(callable_)
    edges: list[tuple[type, type, Scope | None]] = []

    for param_name, param in sig.parameters.items():
        if param_name == "self":
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue

        annotation = hints.get(param_name)
        if annotation is None:
            continue

        if _origin_is_job_context(annotation) or param_name == "payload":
            continue

        unwrapped_type: type | None = None
        override_scope: Scope | None = None
        origin = get_origin(annotation)

        if origin is Annotated:
            args = get_args(annotation)
            if args and isinstance(args[0], type):
                unwrapped_type = args[0]
            scopes: list[Scope] = []
            for meta in args[1:]:
                if isinstance(meta, Scope):
                    scopes.append(meta)
            if len(scopes) > 1:
                raise DIError(
                    f"parameter '{param_name}' has multiple Scope markers: "
                    f"{', '.join(s.name for s in scopes)}"
                )
            if scopes:
                override_scope = scopes[0]
        elif isinstance(annotation, type):
            unwrapped_type = annotation

        if unwrapped_type is None:
            continue

        edges.append((owner_type, unwrapped_type, override_scope))

    return edges
