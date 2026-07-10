"""DI dependency-resolution engine.

Inspects a callable's signature, resolves each parameter through the
provider registry and scope containers, and returns a kwargs dict
suitable for ``**kwargs`` injection. The engine never calls factories
directly and never touches an AsyncExitStack — that is the container's
responsibility.
"""

import inspect
import sys
from typing import Annotated, get_args, get_origin, get_type_hints

import structlog

from taskq._di.scope import Scope
from taskq._di.types import ProviderRegistry, ScopeContainer
from taskq.exceptions import DIError

logger = structlog.get_logger("taskq._di.solver")


def _unwrap_scope_override(
    param_name: str,
    annotation: object,
) -> tuple[type | None, Scope | None]:
    """Return ``(unwrapped_type, scope_override)`` for an annotation.

    - ``(None, None)`` — annotation is not ``Annotated[...]``; caller uses
      ``annotation`` directly as the registry lookup key.
    - ``(T, None)`` — ``Annotated[T, ...]`` with no ``Scope`` in metadata;
      caller uses ``T`` as lookup key and the registered default scope.
    - ``(T, scope)`` — ``Annotated[T, ...]`` with exactly one ``Scope``;
      caller uses ``T`` as lookup key and ``scope`` as call-site override.
    - Raises ``DIError`` if multiple ``Scope`` members appear.
    """
    origin = get_origin(annotation)
    if origin is not Annotated:
        return (None, None)

    args = get_args(annotation)
    if not args:
        return (None, None)

    unwrapped: type | None = args[0] if isinstance(args[0], type) else None
    scopes: list[Scope] = []
    for meta in args[1:]:
        if isinstance(meta, Scope):
            scopes.append(meta)

    if len(scopes) > 1:
        raise DIError(
            f"parameter '{param_name}' has multiple Scope markers: "
            f"{', '.join(s.name for s in scopes)}"
        )

    override_scope: Scope | None = scopes[0] if scopes else None
    return (unwrapped, override_scope)


async def solve_dependencies(
    *,
    func: object,
    registry: ProviderRegistry,
    scope_containers: dict[Scope, ScopeContainer],
    passthrough_kwargs: dict[str, object] | None = None,
) -> dict[str, object]:
    """Resolve all DI parameters for *func* and return a kwargs dict.

    Parameters
    ----------
    func:
        The callable whose parameters will be resolved.
    registry:
        Read-only provider registry for type→entry lookups.
    scope_containers:
        One container per scope; the engine selects by effective scope.
    passthrough_kwargs:
        Optional concrete values injected before registry lookup.

    Raises
    ------
    DIError:
        Malformed annotation (multiple Scope markers, non-type
        annotation, unresolvable forward reference).
    MissingProvider:
        Propagated from ``registry.get()`` when a type has no provider
        and is not in ``passthrough_kwargs``.
    """
    if not callable(func):
        raise DIError(f"solve_dependencies requires a callable, got {type(func)!r}")

    try:
        module = sys.modules.get(func.__module__)
        globalns = vars(module) if module is not None else {}
        hints = get_type_hints(
            func,
            include_extras=True,
            globalns=globalns,
        )
    except NameError as name_error:
        raise DIError(
            f"unresolvable annotation in {func.__module__}.{func.__qualname__}: {name_error}"
        ) from name_error

    passthrough = passthrough_kwargs or {}
    kwargs: dict[str, object] = {}

    for param_name, annotation in hints.items():
        if param_name == "return":
            continue
        if param_name in passthrough:
            kwargs[param_name] = passthrough[param_name]
            continue

        unwrapped, override_scope = _unwrap_scope_override(param_name, annotation)

        lookup_type: object = unwrapped if unwrapped is not None else annotation

        if not isinstance(lookup_type, type):
            raise DIError(
                f"parameter '{param_name}' has a non-type annotation "
                f"{lookup_type!r}; registry lookup requires a concrete type"
            )

        entry = registry.get(lookup_type)

        effective_scope = override_scope if override_scope is not None else entry.scope

        container = scope_containers[effective_scope]
        value = await container.get_or_create(lookup_type, entry)

        kwargs[param_name] = value

        logger.debug(
            "dep-resolved",
            param_name=param_name,
            dep_type=lookup_type.__qualname__,
            scope=effective_scope,
            cache_hit=container.last_cache_hit,
        )

    sig = inspect.signature(func)
    for pname, param in sig.parameters.items():
        if pname == "self":
            continue
        if pname in kwargs or pname in passthrough:
            continue
        if param.default is inspect.Parameter.empty and param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise DIError(
                f"parameter '{pname}' of {func.__module__}.{func.__qualname__} "
                f"has no type annotation and no default value; "
                f"DI cannot resolve unannotated required parameters"
            )

    return kwargs
