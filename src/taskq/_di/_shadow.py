"""Static walk of the provider graph for the types derived from a shadowed one."""

from collections.abc import Mapping
from typing import Any, cast

from taskq._di.solver import (
    _cached_introspection,  # pyright: ignore[reportPrivateUsage]  # Why: the shadow-derivation walk mirrors the solver's own parameter introspection; reusing its memoized introspection keeps the walk at cached-tuple access instead of re-running get_type_hints.
    _unwrap_scope_override,  # pyright: ignore[reportPrivateUsage]  # Why: same, the walk must unwrap Annotated[...] exactly as the solver does, or a Scope-marked shadowed parameter would be misread.
)
from taskq._di.types import FactoryShape, ProviderEntry

__all__ = ["shadow_derived_providers"]


def shadow_derived_providers(
    providers: Mapping[type, ProviderEntry[object]],
    shadow_types: frozenset[type],
) -> frozenset[type]:
    """The provider types whose dependency closure reaches a shadowed type.

    Walks the provider graph statically, the same parameter
    introspection the solver performs at resolution time (memoized by
    ``_cached_introspection``), followed recursively through
    provider→provider edges, and returns every NON-value provider whose
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
        # dependencies: the factory itself, or the class's __init__ ,
        # the same pair the solver resolves through.
        if entry.factory_shape is FactoryShape.VALUE:
            return None
        if entry.factory_shape is FactoryShape.CLASS:
            return cast("type[Any]", entry.impl).__init__
        return entry.impl

    def _reaches(t: type, seen: frozenset[type]) -> bool:
        verdict_, _tainted = _reaches_tracked(t, seen)
        return verdict_

    def _reaches_tracked(t: type, seen: frozenset[type]) -> tuple[bool, bool]:
        """``(reaches, clean)``: clean is False when a cycle back-edge
        truncated the walk, so a False verdict is an artifact of the
        truncation rather than the type's answer from a clean entry point.
        """
        if t in verdict:
            return verdict[t], True
        if t in seen:
            # A cycle's back-edge cannot be the path that makes either
            # member shadow-derived; the verdict is decided by the rest
            # of each member's dependencies. The False is provisional:
            # the member is left uncached so an entry elsewhere on the
            # real graph (where the cycle's own shadow dependency is
            # visible) computes the true verdict.
            return False, False
        entry = providers.get(t)
        if entry is None or entry.factory_shape is FactoryShape.VALUE:
            verdict[t] = t in shadow_types
            return verdict[t], True
        callable_ = _entry_callable(entry)
        hit = False
        tainted = False
        if callable_ is not None:
            hints, _sig_params = _cached_introspection(callable_)
            for param_name, annotation in hints.items():
                if param_name == "return":
                    continue
                unwrapped, _override = _unwrap_scope_override(param_name, annotation)
                lookup_type = unwrapped if unwrapped is not None else annotation
                if not isinstance(lookup_type, type):
                    continue
                sub, sub_clean = _reaches_tracked(lookup_type, seen | {t})
                tainted = tainted or not sub_clean
                if lookup_type in shadow_types or sub:
                    hit = True
                    break
        if tainted:
            # A truncation fired somewhere below: this False is what the
            # truncated view sees, not what the type answers from a clean
            # root. Caching it would pin the artifact; cache clean walks
            # only (a True result is always real, but skipping the cache
            # for tainted walks either way keeps the rule one-line).
            return hit, False
        verdict[t] = hit
        return hit, True

    return frozenset(
        t
        for t, entry in providers.items()
        if entry.factory_shape is not FactoryShape.VALUE and _reaches(t, frozenset())
    )
