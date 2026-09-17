"""Static walk of the provider graph for the types derived from a shadowed one."""

from collections.abc import Mapping
from typing import Any, cast

from taskq._di.solver import (
    _cached_introspection,  # pyright: ignore[reportPrivateUsage]  # Why: the shadow-derivation walk mirrors the solver's own parameter introspection; reusing its memoized introspection keeps the walk at cached-tuple access instead of re-running get_type_hints.
    _unwrap_scope_override,  # pyright: ignore[reportPrivateUsage]  # Why: same — the walk must unwrap Annotated[...] exactly as the solver does, or a Scope-marked shadowed parameter would be misread.
)
from taskq._di.types import FactoryShape, ProviderEntry

__all__ = ["shadow_derived_providers"]


def shadow_derived_providers(
    providers: Mapping[type, ProviderEntry[object]],
    shadow_types: frozenset[type],
) -> frozenset[type]:
    """The provider types whose dependency closure reaches a shadowed type.

    Walks the provider graph statically — the same parameter
    introspection the solver performs at resolution time (memoized by
    ``_cached_introspection``), followed recursively through
    provider→provider edges — and returns every NON-value provider whose
    own parameters, or any transitively injected provider's parameters,
    name a type in *shadow_types*. Those are the LOOP-scoped factories
    that bake a LOOP-registered connection (or anything derived from
    one) into the singleton the scope's bootstrap resolution created;
    the per-slot view re-resolves them per actor invocation instead.

    VALUE providers are never returned: a value carries no dependency
    graph, so it cannot derive from anything. Unregistered parameter
    types contribute nothing (a validated registry has none left).
    """
    verdict: dict[type, bool] = {}

    def _entry_callable(entry: ProviderEntry[object]) -> object | None:
        # The callable whose parameters name this provider's
        # dependencies: the factory itself, or the class's __init__ —
        # the same pair the solver resolves through.
        if entry.factory_shape is FactoryShape.VALUE:
            return None
        if entry.factory_shape is FactoryShape.CLASS:
            return cast("type[Any]", entry.impl).__init__
        return entry.impl

    def _reaches(t: type, seen: frozenset[type]) -> bool:
        if t in verdict:
            return verdict[t]
        if t in seen:
            # A cycle's back-edge cannot be the path that makes either
            # member shadow-derived; the verdict is decided by the rest
            # of each member's dependencies.
            return False
        entry = providers.get(t)
        if entry is None or entry.factory_shape is FactoryShape.VALUE:
            verdict[t] = t in shadow_types
            return verdict[t]
        callable_ = _entry_callable(entry)
        hit = False
        if callable_ is not None:
            hints, _sig_params = _cached_introspection(callable_)
            for param_name, annotation in hints.items():
                if param_name == "return":
                    continue
                unwrapped, _override = _unwrap_scope_override(param_name, annotation)
                lookup_type = unwrapped if unwrapped is not None else annotation
                if not isinstance(lookup_type, type):
                    continue
                if lookup_type in shadow_types or _reaches(lookup_type, seen | {t}):
                    hit = True
                    break
        verdict[t] = hit
        return hit

    return frozenset(
        t
        for t, entry in providers.items()
        if entry.factory_shape is not FactoryShape.VALUE and _reaches(t, frozenset())
    )
