"""Five-phase startup-validation algorithm.

Pure function — does not mutate any input. Raises ``MissingProvider`` /
``DependencyCycle`` / ``ScopeViolation`` .
"""

import inspect
import warnings
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    get_args,
    get_origin,
    get_type_hints,
)

import structlog

from taskq._di._utils import (
    _origin_is_job_context,  # pyright: ignore[reportPrivateUsage] — internal helper shared within _di package; the _di prefix itself signals package-level privacy
)
from taskq._di.scope import LifecycleDetectionWarning, Scope
from taskq._di.types import ProviderEntry
from taskq.exceptions import (
    DependencyCycle,
    DIError,
    MissingProvider,
    ScopeViolation,
)

if TYPE_CHECKING:
    from taskq.actor import ActorRef
    from taskq.ratelimit.registry import RateLimitRegistry

_log = structlog.get_logger("taskq._di.validate")


def _qual(t: type) -> str:
    """Return the fully-qualified name of ``t`` for diagnostic purposes."""
    return f"{t.__module__}.{t.__qualname__}"


def _collect_actor_edges(
    actors: list["ActorRef[Any, Any]"],
) -> list[tuple[str, type, Scope | None]]:
    """Walk each actor's DI parameter annotations and return edges.

    Returns ``(actor_name, dep_type, override_or_None)`` tuples.
    ``actor_name`` comes from ``ActorRef.name``; ``dep_type`` and
    ``override_or_None`` are extracted from the actor handler's
    parameter annotations.
    """
    edges: list[tuple[str, type, Scope | None]] = []
    for actor in actors:
        fn = actor.fn
        try:
            hints = get_type_hints(fn, include_extras=True)
        except NameError as err:
            raise DIError(f"unresolvable annotation in {fn.__qualname__}: {err}") from err

        sig = inspect.signature(fn)
        for param_name, param in sig.parameters.items():
            if param_name == "payload":
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue

            annotation = hints.get(param_name)
            if annotation is None:
                continue

            if _origin_is_job_context(annotation) or param_name == "ctx":
                continue

            unwrapped_type: type | None = None
            override_scope: Scope | None = None
            origin = get_origin(annotation)

            if origin is Annotated:
                args = get_args(annotation)
                if args and isinstance(args[0], type):
                    unwrapped_type = args[0]
                scopes_found: list[Scope] = []
                for meta in args[1:]:
                    if isinstance(meta, Scope):
                        scopes_found.append(meta)
                if len(scopes_found) > 1:
                    raise DIError(
                        f"parameter '{param_name}' has multiple Scope markers: "
                        f"{', '.join(s.name for s in scopes_found)}"
                    )
                if scopes_found:
                    override_scope = scopes_found[0]
            elif isinstance(annotation, type):
                unwrapped_type = annotation

            if unwrapped_type is None:
                continue

            edges.append((actor.name, unwrapped_type, override_scope))
    return edges


def _emit_redundant_override_warnings(  # pyright: ignore[reportUnusedFunction] — used by registry.py via import; the _di prefix signals package-level privacy
    actors: list["ActorRef[Any, Any]"],
    providers: dict[type, ProviderEntry[object]],
) -> None:
    """Walk each actor's parameters and emit dual-signal for redundant Scope overrides.

    Runs after phases 1-4 so it never preempts error reporting. A redundant
    override is one where ``Annotated[T, Scope.X]`` matches the registered
    default scope for ``T`` — the override has no effect.
    """
    for actor in actors:
        fn = actor.fn
        try:
            hints = get_type_hints(fn, include_extras=True)
        except NameError as err:
            raise DIError(f"unresolvable annotation in {fn.__qualname__}: {err}") from err

        sig = inspect.signature(fn)
        for param_name, param in sig.parameters.items():
            if param_name == "payload":
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue

            annotation = hints.get(param_name)
            if annotation is None:
                continue

            if _origin_is_job_context(annotation) or param_name == "ctx":
                continue

            origin = get_origin(annotation)
            if origin is not Annotated:
                continue

            args = get_args(annotation)
            if not args:
                continue

            unwrapped: type | None = args[0] if isinstance(args[0], type) else None
            scope_override: Scope | None = None
            for meta in args[1:]:
                if isinstance(meta, Scope):
                    scope_override = meta
                    break

            if scope_override is None:
                continue

            if unwrapped is None or unwrapped not in providers:
                continue

            registered_default = providers[unwrapped].scope
            if scope_override != registered_default:
                continue

            t = unwrapped
            scope = scope_override
            warnings.warn(
                LifecycleDetectionWarning(
                    f"redundant Scope override on {actor.name}.{param_name}: "
                    f"Annotated[..., Scope.{scope.name}] matches the registered "
                    f"default for {_qual(t)}; the override has no effect."
                ),
                stacklevel=3,  # Why: three frames to skip — warn → _emit_redundant_override_warnings → validate() — so the warning points at validate()'s caller
            )
            _log.warning(
                "redundant_scope_override",
                actor=actor.name,
                param=param_name,
                type_name=_qual(t),
                scope=scope.name,
            )


def _build_adjacency(
    edges: list[tuple[type, type, Scope | None]],
) -> dict[type, list[type]]:
    """Build adjacency list from dependency edges keyed by provider type."""
    adj: dict[type, list[type]] = {}
    for provider_type, dep_type, _ in edges:
        adj.setdefault(provider_type, []).append(dep_type)
        if dep_type not in adj:
            adj[dep_type] = []
    return adj


def _detect_cycles(adjacency: dict[type, list[type]]) -> None:
    """Iterative DFS cycle detection over the provider graph.

    Uses a list as the recursion stack so the cycle path can be
    reconstructed by index. When DFS visits a node whose outgoing
    edge points to a node already in the recursion stack, slice
    the stack from that node's index and append it again to form
    the cycle path.
    """
    visited: set[type] = set()

    for start in adjacency:
        if start in visited:
            continue
        stack: list[type] = [start]
        path_set: set[type] = {start}
        children_idx: dict[type, int] = {}
        children_list: dict[type, list[type]] = {}

        while stack:
            current = stack[-1]
            if current not in children_list:
                children_list[current] = adjacency.get(current, [])
                children_idx[current] = 0

            idx = children_idx[current]
            neighbors = children_list[current]

            if idx < len(neighbors):
                neighbor = neighbors[idx]
                children_idx[current] = idx + 1
                if neighbor in path_set:
                    cycle_start = stack.index(neighbor)
                    cycle_path = [_qual(t) for t in stack[cycle_start:]] + [_qual(neighbor)]
                    raise DependencyCycle(cycle_path=cycle_path)
                if neighbor not in visited:
                    stack.append(neighbor)
                    path_set.add(neighbor)
            else:
                stack.pop()
                path_set.discard(current)
                visited.add(current)


def _post_order_dfs(
    node: type,
    adjacency: dict[type, list[type]],
    visited: set[type],
    post_order: list[type],
) -> None:
    """Post-order DFS traversal for topological sort."""
    if node in visited:
        return
    visited.add(node)
    for neighbor in adjacency.get(node, []):
        _post_order_dfs(neighbor, adjacency, visited, post_order)
    post_order.append(node)


def _actor_deps_at_scope(
    actor: "ActorRef[Any, Any]",
    scope: Scope,
    providers: dict[type, ProviderEntry[object]],
) -> list[type]:
    """Return the actor's DI parameter types whose effective scope matches ``scope``."""
    fn = actor.fn
    try:
        hints = get_type_hints(fn, include_extras=True)
    except NameError as err:
        raise DIError(f"unresolvable annotation in {fn.__qualname__}: {err}") from err

    sig = inspect.signature(fn)
    result: list[type] = []

    for param_name, param in sig.parameters.items():
        if param_name == "payload":
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue

        annotation = hints.get(param_name)
        if annotation is None:
            continue

        if _origin_is_job_context(annotation) or param_name == "ctx":
            continue

        unwrapped_type: type | None = None
        override_scope: Scope | None = None
        origin = get_origin(annotation)

        if origin is Annotated:
            args = get_args(annotation)
            if args and isinstance(args[0], type):
                unwrapped_type = args[0]
            for meta in args[1:]:
                if isinstance(meta, Scope):
                    override_scope = meta
                    break
        elif isinstance(annotation, type):
            unwrapped_type = annotation

        if unwrapped_type is None or unwrapped_type not in providers:
            continue

        effective_scope = override_scope or providers[unwrapped_type].scope
        if effective_scope == scope:
            result.append(unwrapped_type)

    return result


def _warn_sync_actor_loop_deps(
    actors: list["ActorRef[Any, Any]"],
    providers: dict[type, ProviderEntry[object]],
) -> None:
    """Warn when a sync actor declares a LOOP-scoped dependency.

    LOOP-scoped providers (e.g. an ``asyncpg.Connection``) are not
    thread-safe. Sync actors run via ``asyncio.to_thread()``, so using a
    LOOP-scoped dependency from one is a latent thread-safety bug. This
    is advisory only (does not raise) — see docs/guides/actors.md#sync-actors.
    """
    for actor in actors:
        if not actor.is_sync:
            continue
        loop_deps = _actor_deps_at_scope(actor, Scope.LOOP, providers)
        for dep_type in loop_deps:
            _log.warning(
                "sync_actor_loop_scoped_dependency",
                actor=actor.name,
                type_name=_qual(dep_type),
            )


def _topo_sort_for_actor(
    actor: "ActorRef[Any, Any]",
    scope: Scope,
    edges: list[tuple[type, type, Scope | None]],
    providers: dict[type, ProviderEntry[object]],
) -> list[type]:
    """Compute the dependency-first resolution plan for ``actor`` at ``scope``.

    Walks the actor's transitive dep closure rooted at parameters whose
    effective scope is ``scope``, using post-order DFS on depends-on
    edges, which directly produces dependency-first order (leaf deps
    first, dependents last) without reversal.
    """
    root_deps = _actor_deps_at_scope(actor, scope, providers)
    if not root_deps:
        return []

    adjacency: dict[type, list[type]] = {}
    for provider_type, dep_type, _ in edges:
        if provider_type in providers:
            adjacency.setdefault(provider_type, []).append(dep_type)

    visited: set[type] = set()
    post_order: list[type] = []

    for root in root_deps:
        _post_order_dfs(root, adjacency, visited, post_order)

    # Why: post-order DFS on "depends-on" edges already produces
    # dependency-first order (leaf deps first, dependents last);
    # no reversal needed — the "reverse" instruction
    # assumes edges point from deps to dependents, but our adjacency
    # has edges from dependents to dependencies.
    return post_order


def run_validation(
    providers: dict[type, ProviderEntry[object]],
    dep_edges: list[tuple[type, type, Scope | None]],
    actors: list["ActorRef[Any, Any]"] | None,
    rate_limit_registry: "RateLimitRegistry | None" = None,
) -> dict[tuple[str, Scope], list[type]]:
    """Run the five-phase algorithm. Return the plan cache.

    Raises MissingProvider / DependencyCycle / ScopeViolation.
    Pure function — does not mutate any input.
    Never invokes a factory function, calls a resolver, or performs
    await — the entire algorithm is synchronous introspection on
    registration metadata only.

    When ``rate_limit_registry`` is provided, an additional phase
    after the DI-edge walk checks each actor's ``rate_limits`` and
    ``reservations`` string lists against the registry's dicts.
    Unknown names raise ``MissingProvider`` at startup .
    """
    actor_edges: list[tuple[str, type, Scope | None]] = []
    if actors is not None:
        actor_edges = _collect_actor_edges(actors)

    # Phase 2 — MissingProvider check
    for provider_type, dep_type, _override in dep_edges:
        if dep_type not in providers:
            raise MissingProvider(
                type_name=_qual(dep_type),
                required_by=provider_type.__qualname__,
            )
    for actor_name, dep_type, _override in actor_edges:
        if dep_type not in providers:
            raise MissingProvider(
                type_name=_qual(dep_type),
                required_by=actor_name,
            )

    # Phase 2b — Rate-limit / reservation name check
    if rate_limit_registry is not None and actors is not None:
        rl_names = rate_limit_registry.rate_limits
        res_names = rate_limit_registry.reservations
        for actor in actors:
            for rl_name in actor.rate_limits:
                if rl_name not in rl_names:
                    raise MissingProvider(
                        type_name="RateLimit",
                        required_by=f"actor:{actor.name}:rate_limits:{rl_name}",
                    )
            for res_name in actor.reservations:
                if res_name not in res_names:
                    raise MissingProvider(
                        type_name="ConcurrencyReservation",
                        required_by=f"actor:{actor.name}:reservations:{res_name}",
                    )

    # Phase 3 — DependencyCycle detection
    adjacency = _build_adjacency(dep_edges)
    _detect_cycles(adjacency)

    # Phase 4 — ScopeViolation direction check
    for provider_type, dep_type, override in dep_edges:
        provider_scope = providers[provider_type].scope
        effective_dep_scope = override or providers[dep_type].scope
        # Why: direction rule — effective_dep_scope.value <=
        # provider_scope.value is valid (narrower dep is safe)
        if effective_dep_scope.value > provider_scope.value:
            raise ScopeViolation(
                from_scope=provider_scope,
                to_scope=effective_dep_scope,
                type_name=_qual(dep_type),
                dependent=_qual(provider_type),
            )

    # Why: actors run inside build_actor_scope which creates a
    # per-invocation TRANSIENT container — the actor body is
    # TRANSIENT-scoped by construction. Direction rule
    # applied uniformly: no actor→provider edge can ever raise
    # ScopeViolation because TRANSIENT (value 3) is the narrowest
    # scope (DoD item 5).

    # Phase 4b — sync actor + LOOP-scoped dependency advisory warning
    if actors is not None:
        _warn_sync_actor_loop_deps(actors, providers)

    # Phase 5 — topological sort and plan cache
    plan_cache: dict[tuple[str, Scope], list[type]] = {}
    if actors is not None:
        for actor in actors:
            for scope in (Scope.PROCESS, Scope.THREAD, Scope.LOOP, Scope.TRANSIENT):
                plan = _topo_sort_for_actor(actor, scope, dep_edges, providers)
                if plan:
                    plan_cache[(actor.name, scope)] = plan

    return plan_cache
