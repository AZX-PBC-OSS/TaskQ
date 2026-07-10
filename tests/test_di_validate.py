"""Unit tests for ProviderRegistry five-phase validate() algorithm.


(idempotency), (seal enforcement), (plan cache),
G13 (empty plans omitted), G11 (deterministic ordering), G5 (re-entrant guard).
"""

from typing import Annotated, Any

import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq.actor import ActorRef
from taskq.context import JobContext
from taskq.exceptions import DependencyCycle, DIError, MissingProvider, ScopeViolation
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import RetryPolicy


class _Settings:
    pass


class _AsyncGraphClient:
    pass


class _DbPool:
    pass


class _HttpClient:
    pass


class _Payload(BaseModel):
    x: int


class _Result(BaseModel):
    y: int


def _make_actor_ref(
    name: str,
    fn: Any,
) -> ActorRef[Any, Any]:
    """Construct an ActorRef test double with minimal wiring."""
    return ActorRef(
        name=name,
        queue="default",
        fn=fn,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
    )


def _make_actor_ref_with_ctx(
    name: str,
    fn: Any,
) -> ActorRef[Any, Any]:
    """Construct an ActorRef test double that wants ctx."""
    return ActorRef(
        name=name,
        queue="default",
        fn=fn,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
    )


# ── MissingProvider on undefined dependency ─────────────────────────────


def test_missing_provider_on_undefined_dep() -> None:
    registry = ProviderRegistry()

    def factory(client: _AsyncGraphClient) -> _Settings:
        return _Settings()

    registry.register_factory(_Settings, Scope.TRANSIENT, factory)
    with pytest.raises(MissingProvider) as exc_info:
        registry.validate()
    assert "AsyncGraphClient" in exc_info.value.type_name
    assert exc_info.value.required_by == _Settings.__qualname__


# ── DependencyCycle on circular dependency ──────────────────────────────


def test_dependency_cycle_on_circular_dep() -> None:
    registry = ProviderRegistry()

    def factory_a(b: _AsyncGraphClient) -> _Settings:
        return _Settings()

    def factory_b(a: _Settings) -> _AsyncGraphClient:
        return _AsyncGraphClient()

    registry.register_factory(_Settings, Scope.LOOP, factory_a)
    registry.register_factory(_AsyncGraphClient, Scope.LOOP, factory_b)

    with pytest.raises(DependencyCycle) as exc_info:
        registry.validate()
    assert len(exc_info.value.cycle_path) == 3


# ── ScopeViolation on LOOP→TRANSIENT reverse edge ───────────────────────


def test_scope_violation_loop_to_transient() -> None:
    registry = ProviderRegistry()

    def factory(client: Annotated[_DbPool, Scope.TRANSIENT]) -> _Settings:
        return _Settings()

    registry.register_factory(_Settings, Scope.LOOP, factory)
    registry.register_value(_DbPool, Scope.TRANSIENT, _DbPool())

    with pytest.raises(ScopeViolation) as exc_info:
        registry.validate()
    assert exc_info.value.from_scope == Scope.LOOP
    assert exc_info.value.to_scope == Scope.TRANSIENT


# ── ScopeViolation message quality ──────────────────────────────────────


def test_scope_violation_message_quality() -> None:
    registry = ProviderRegistry()

    def factory(client: Annotated[_DbPool, Scope.TRANSIENT]) -> _Settings:
        return _Settings()

    registry.register_factory(_Settings, Scope.LOOP, factory)
    registry.register_value(_DbPool, Scope.TRANSIENT, _DbPool())

    with pytest.raises(ScopeViolation) as exc_info:
        registry.validate()
    msg = str(exc_info.value)
    assert "LOOP" in msg
    assert "TRANSIENT" in msg
    assert "DbPool" in exc_info.value.type_name
    assert "Settings" in exc_info.value.dependent


# ── Valid scope direction ───────────────────────────────────────────────


def test_valid_scope_direction() -> None:
    registry = ProviderRegistry()

    def factory_thread(settings: _Settings) -> _AsyncGraphClient:
        return _AsyncGraphClient()

    def factory_loop(client: _AsyncGraphClient) -> _DbPool:
        return _DbPool()

    def factory_transient(pool: _DbPool) -> _HttpClient:
        return _HttpClient()

    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    registry.register_factory(_AsyncGraphClient, Scope.THREAD, factory_thread)
    registry.register_factory(_DbPool, Scope.LOOP, factory_loop)
    registry.register_factory(_HttpClient, Scope.TRANSIENT, factory_transient)

    registry.validate()
    assert registry._validated is True
    assert registry._sealed is True


# ── PROCESS cannot depend on LOOP ──────────────────────────────────────


def test_process_cannot_depend_on_loop() -> None:
    registry = ProviderRegistry()

    def factory(pool: Annotated[_DbPool, Scope.LOOP]) -> _Settings:
        return _Settings()

    registry.register_factory(_Settings, Scope.PROCESS, factory)
    registry.register_value(_DbPool, Scope.LOOP, _DbPool())

    with pytest.raises(ScopeViolation) as exc_info:
        registry.validate()
    assert exc_info.value.from_scope == Scope.PROCESS
    assert exc_info.value.to_scope == Scope.LOOP


# ── Validation timing — validate-only ────────────────────────────────


def test_validate_only_timing() -> None:
    registry = ProviderRegistry()

    async def my_actor(payload: _Payload, dep: _AsyncGraphClient) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref("my_actor", my_actor)
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    registry.validate()
    assert registry._validated is True

    registry2 = ProviderRegistry()
    registry2.register_value(_Settings, Scope.PROCESS, _Settings())
    with pytest.raises(MissingProvider) as exc_info:
        registry2.validate(actors=[actor])
    assert "AsyncGraphClient" in exc_info.value.type_name
    assert exc_info.value.required_by == "my_actor"


# ── Self-dependency cycle ───────────────────────────────────────────────


def test_self_dependency_cycle() -> None:
    registry = ProviderRegistry()

    class _Self:
        pass

    def factory(self_dep: _Self) -> _Self:
        return _Self()

    registry.register_factory(_Self, Scope.LOOP, factory)
    with pytest.raises(DependencyCycle) as exc_info:
        registry.validate()
    assert len(exc_info.value.cycle_path) == 2
    assert exc_info.value.cycle_path[0] == exc_info.value.cycle_path[1]


# ── Idempotency ───────────────────────────────────────────────────────


def test_validate_idempotent() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    registry.validate()
    registry.validate()

    assert registry._validated is True


# ── Seal enforcement ─────────────────────────────────────────────────────


def test_seal_enforcement_after_validate() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    registry.validate()

    with pytest.raises(RuntimeError, match="sealed"):
        registry.register_value(_AsyncGraphClient, Scope.LOOP, _AsyncGraphClient())


# ── Plan cache populated ────────────────────────────────────────────────


def test_plan_cache_populated() -> None:
    registry = ProviderRegistry()

    class A:
        pass

    class B:
        def __init__(self, a: A) -> None:
            self.a = a

    class C:
        def __init__(self, b: B) -> None:
            self.b = b

    registry.register_class(A, Scope.LOOP)
    registry.register_class(B, Scope.LOOP)
    registry.register_class(C, Scope.LOOP)

    async def my_actor(payload: _Payload, dep: C) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref("my_actor", my_actor)
    registry.validate(actors=[actor])

    plan = registry._plan_cache[("my_actor", Scope.LOOP)]
    assert plan == [A, B, C]


# ── Empty plans omitted (G13) ──────────────────────────────────────────────────


def test_empty_plans_omitted() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def no_dep_actor(payload: _Payload) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref("no_dep_actor", no_dep_actor)
    registry.validate(actors=[actor])

    assert ("no_dep_actor", Scope.PROCESS) not in registry._plan_cache
    assert ("no_dep_actor", Scope.THREAD) not in registry._plan_cache
    assert ("no_dep_actor", Scope.LOOP) not in registry._plan_cache
    assert ("no_dep_actor", Scope.TRANSIENT) not in registry._plan_cache


# ── Plan cache deterministic ordering (G11) ─────────────────────────────────────


def test_plan_cache_deterministic_ordering() -> None:
    class A1:
        pass

    class B1:
        pass

    class C1:
        def __init__(self, a: A1, b: B1) -> None:
            pass

    def build_registry() -> ProviderRegistry:
        registry = ProviderRegistry()
        registry.register_class(A1, Scope.LOOP)
        registry.register_class(B1, Scope.LOOP)
        registry.register_class(C1, Scope.LOOP)

        async def actor1(payload: _Payload, dep: C1) -> _Result:
            return _Result(y=1)

        actor = _make_actor_ref("actor1", actor1)
        registry.validate(actors=[actor])
        return registry

    r1 = build_registry()
    r2 = build_registry()
    assert r1._plan_cache[("actor1", Scope.LOOP)] == r2._plan_cache[("actor1", Scope.LOOP)]


# ── Multi-actor plan cache ─────────────────────────────────────────────────────


def test_multi_actor_plan_cache() -> None:
    registry = ProviderRegistry()

    class A:
        pass

    class B:
        def __init__(self, a: A) -> None:
            pass

    registry.register_class(A, Scope.LOOP)
    registry.register_class(B, Scope.LOOP)

    async def actor_1(payload: _Payload, dep: A) -> _Result:
        return _Result(y=1)

    async def actor_2(payload: _Payload, dep: B) -> _Result:
        return _Result(y=1)

    ref1 = _make_actor_ref("actor_1", actor_1)
    ref2 = _make_actor_ref("actor_2", actor_2)
    registry.validate(actors=[ref1, ref2])

    plan_1 = registry._plan_cache[("actor_1", Scope.LOOP)]
    plan_2 = registry._plan_cache[("actor_2", Scope.LOOP)]
    assert A in plan_1
    assert A in plan_2
    assert B not in plan_1
    assert B in plan_2


# ── Validation phases run in prescribed order ───────────────────────────────────


def test_missing_provider_raised_before_cycle() -> None:
    registry = ProviderRegistry()

    class _Missing:
        pass

    class _A2:
        pass

    class _B2:
        pass

    def factory_a(missing: _Missing, b: _B2) -> _A2:
        return _A2()

    def factory_b(a: _A2) -> _B2:
        return _B2()

    registry.register_factory(_A2, Scope.LOOP, factory_a)
    registry.register_factory(_B2, Scope.LOOP, factory_b)

    with pytest.raises(MissingProvider):
        registry.validate()


def test_cycle_raised_before_scope_violation() -> None:
    registry = ProviderRegistry()

    class _X2:
        pass

    class _Y2:
        pass

    def factory_x(y: _Y2) -> _X2:
        return _X2()

    def factory_y(x: _X2) -> _Y2:
        return _Y2()

    registry.register_factory(_X2, Scope.PROCESS, factory_x)
    registry.register_factory(_Y2, Scope.TRANSIENT, factory_y)

    with pytest.raises(DependencyCycle):
        registry.validate()


# ── validate() seals and caches ────────────────────────────────────────────


def test_validate_seals_and_caches_plan() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    registry.validate()

    assert registry._validated is True
    assert registry._sealed is True
    assert registry._plan_cache == {}


def test_validate_with_actors_seals_registry() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def my_actor(payload: _Payload, dep: _Settings) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref("my_actor", my_actor)

    registry.validate(actors=[actor])

    assert registry._validated is True
    assert registry._sealed is True


# ── Defensive invariant (G5) — re-entrant validate() ───────────────────────────


def test_reentrant_validate_raises() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())
    registry._validating = True
    try:
        with pytest.raises(RuntimeError, match="recursively or concurrently"):
            registry.validate()
    finally:
        registry._validating = False


# ── Actor with ctx parameter — ctx excluded from edges ──────────────────────────


def test_actor_ctx_excluded_from_edges() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload], dep: _Settings) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref_with_ctx("my_actor", my_actor)
    registry.validate(actors=[actor])

    plan = registry._plan_cache.get(("my_actor", Scope.PROCESS))
    assert plan is not None
    assert _Settings in plan


# ── Actor with Annotated scope override ─────────────────────────────────────────


def test_actor_annotated_scope_override_in_plan() -> None:
    registry = ProviderRegistry()

    class A:
        pass

    registry.register_value(A, Scope.LOOP, A())

    async def my_actor(payload: _Payload, dep: Annotated[A, Scope.LOOP]) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref("my_actor", my_actor)
    registry.validate(actors=[actor])

    plan = registry._plan_cache.get(("my_actor", Scope.LOOP))
    assert plan is not None
    assert A in plan


# ── Regression: _actor_deps_at_scope raises DIError on unresolvable annotation ────


def test_actor_deps_at_scope_raises_on_unresolvable_annotation() -> None:
    from taskq._di._validate import _actor_deps_at_scope

    registry = ProviderRegistry()

    async def bad_actor(
        payload: _Payload,
        dep: "NonexistentType",  # noqa: F821 # Why: intentionally unresolvable forward ref to exercise NameError path in _actor_deps_at_scope # type: ignore[name-defined]
    ) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref("bad_actor", bad_actor)
    with pytest.raises(DIError, match="unresolvable annotation"):
        _actor_deps_at_scope(actor, Scope.LOOP, registry._providers)


def _make_actor_ref_with_rl(
    name: str,
    fn: Any,
    rate_limits: list[str] | None = None,
    reservations: list[str] | None = None,
) -> ActorRef[Any, Any]:
    """Construct an ActorRef test double with rate_limits/reservations."""
    return ActorRef(
        name=name,
        queue="default",
        fn=fn,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        rate_limits=rate_limits,
        reservations=reservations,
    )


# ── MissingProvider on unknown rate_limits name ──────────────────────────


def test_missing_provider_on_unknown_rate_limit_name() -> None:
    rl_registry = RateLimitRegistry()

    async def my_actor(payload: _Payload) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref_with_rl("my_actor", my_actor, rate_limits=["missing"])
    di_registry = ProviderRegistry()
    di_registry.register_value(_Settings, Scope.PROCESS, _Settings())

    with pytest.raises(MissingProvider) as exc_info:
        di_registry.validate(actors=[actor], rate_limit_registry=rl_registry)
    assert exc_info.value.type_name == "RateLimit"
    assert "my_actor" in exc_info.value.required_by
    assert "missing" in exc_info.value.required_by


# ── MissingProvider on unknown reservations name ─────────────────────────


def test_missing_provider_on_unknown_reservation_name() -> None:
    rl_registry = RateLimitRegistry()

    async def my_actor(payload: _Payload) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref_with_rl("my_actor", my_actor, reservations=["missing"])
    di_registry = ProviderRegistry()
    di_registry.register_value(_Settings, Scope.PROCESS, _Settings())

    with pytest.raises(MissingProvider) as exc_info:
        di_registry.validate(actors=[actor], rate_limit_registry=rl_registry)
    assert exc_info.value.type_name == "ConcurrencyReservation"
    assert "my_actor" in exc_info.value.required_by
    assert "missing" in exc_info.value.required_by


# ── Valid rate-limit and reservation names pass validation ───────────────────────


def test_valid_rate_limit_names_pass_validation() -> None:
    rl_registry = RateLimitRegistry()
    rl_registry.register(
        TokenBucket(name="openai", capacity=10, refill_per_second=1, backend="memory")
    )
    rl_registry.register(ConcurrencyReservation(name="gpu_pool", slots=4, lease=30))

    async def my_actor(payload: _Payload) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref_with_rl(
        "my_actor",
        my_actor,
        rate_limits=["openai"],
        reservations=["gpu_pool"],
    )
    di_registry = ProviderRegistry()
    di_registry.register_value(_Settings, Scope.PROCESS, _Settings())

    di_registry.validate(actors=[actor], rate_limit_registry=rl_registry)
    assert di_registry._validated is True


# ── validate() with rate_limit_registry=None skips name-check (backward compat) ─


def test_validate_without_rate_limit_registry_skips_name_check() -> None:
    async def my_actor(payload: _Payload) -> _Result:
        return _Result(y=1)

    actor = _make_actor_ref_with_rl("my_actor", my_actor, rate_limits=["missing"])
    di_registry = ProviderRegistry()
    di_registry.register_value(_Settings, Scope.PROCESS, _Settings())

    di_registry.validate(actors=[actor])
    assert di_registry._validated is True
