"""Unit tests for the worker bootstrap RateLimitRegistry resolution order.

1. Explicit ``rate_limit_registry=`` argument wins.
2. A DI *value* provider wins over the singleton.
3. A DI factory/class provider raises TypeError (fail fast — a factory
   would split-brain bootstrap vs. LOOP-scope dispatch resolution); a value
   provider at a non-LOOP scope raises TypeError for the same reason
   (dispatch resolves from the LOOP-scope cache only).
4. Default: the module singleton.

Plus the actor-declaration collection pass in ``_main``: a same-name
different-config conflict raises ValueError at bootstrap (fail fast),
redeclaring the SAME instance on two actors is an idempotent no-op that
still counts 2 declarations in the startup log, and a reservation name
with the reserved queue-cap prefix is rejected.
"""

from datetime import timedelta

import pytest
import structlog
from pydantic import BaseModel

from taskq._di import ProviderRegistry, Scope
from taskq.actor import actor
from taskq.ratelimit.registry import QUEUE_CONCURRENCY_PREFIX, RateLimitRegistry
from taskq.ratelimit.registry import registry as singleton
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.worker._bootstrap import (
    _resolve_rl_registry,  # pyright: ignore[reportPrivateUsage]  # Why: unit-testing the documented resolution-order helper directly
)


class _Payload(BaseModel):
    x: int = 1


def test_explicit_argument_wins_over_singleton() -> None:
    explicit = RateLimitRegistry()

    assert _resolve_rl_registry(explicit, ProviderRegistry()) is explicit


def test_explicit_arg_plus_di_provider_raises_typeerror() -> None:
    """Co-presence is ambiguous: bootstrap would use the explicit instance
    while LOOP-scope dispatch resolved the DI one — fail fast."""
    explicit = RateLimitRegistry()
    di = ProviderRegistry()
    di.register_value(RateLimitRegistry, Scope.LOOP, RateLimitRegistry())

    with pytest.raises(TypeError, match="ambiguous"):
        _resolve_rl_registry(explicit, di)


def test_di_value_provider_wins_over_singleton() -> None:
    own = RateLimitRegistry()
    di = ProviderRegistry()
    di.register_value(RateLimitRegistry, Scope.LOOP, own)

    assert _resolve_rl_registry(None, di) is own


def test_di_factory_provider_raises_typeerror() -> None:
    di = ProviderRegistry()

    def _factory() -> RateLimitRegistry:
        return RateLimitRegistry()

    di.register_factory(RateLimitRegistry, Scope.LOOP, _factory)

    with pytest.raises(TypeError, match="value provider"):
        _resolve_rl_registry(None, di)


def test_di_class_provider_raises_typeerror() -> None:
    di = ProviderRegistry()
    di.register_class(RateLimitRegistry, Scope.LOOP)

    with pytest.raises(TypeError, match="value provider"):
        _resolve_rl_registry(None, di)


@pytest.mark.parametrize("scope", [Scope.PROCESS, Scope.THREAD, Scope.TRANSIENT])
def test_di_value_provider_non_loop_scope_raises_typeerror(scope: Scope) -> None:
    """Scope has the same split-brain failure mode as kind: dispatch reads
    the LOOP-scope cache only (dispatch.py), so a non-LOOP value provider
    bootstraps against one instance while dispatch finds none — silently
    disabling rate-limit acquisition. Fail fast, same as the kind guard."""
    di = ProviderRegistry()
    di.register_value(RateLimitRegistry, scope, RateLimitRegistry())

    with pytest.raises(TypeError, match=r"Scope\.LOOP"):
        _resolve_rl_registry(None, di)


def test_default_is_module_singleton() -> None:
    assert _resolve_rl_registry(None, ProviderRegistry()) is singleton


async def test_main_raises_typeerror_for_factory_provider_before_opening_pools() -> None:
    """The TypeError fails fast in _main BEFORE open_worker_deps (no PG needed)."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import _main

    di = ProviderRegistry()

    def _factory() -> RateLimitRegistry:
        return RateLimitRegistry()

    di.register_factory(RateLimitRegistry, Scope.LOOP, _factory)
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://x:x@localhost/x"}, validate=False
    )

    with pytest.raises(TypeError, match="value provider"):
        await _main(settings, _registry=di)


async def test_main_raises_typeerror_for_process_scope_provider_before_opening_pools() -> None:
    """The scope guard fails fast in _main BEFORE open_worker_deps (no PG needed)."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import _main

    di = ProviderRegistry()
    di.register_value(RateLimitRegistry, Scope.PROCESS, RateLimitRegistry())
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://x:x@localhost/x"}, validate=False
    )

    with pytest.raises(TypeError, match=r"Scope\.LOOP"):
        await _main(settings, _registry=di)


async def test_two_actors_same_name_different_config_raises_valueerror() -> None:
    """Two actors declaring same-named TokenBuckets with different configs:
    the collection pass's register() raises ValueError at bootstrap (fail
    fast, before open_worker_deps — no PG needed)."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import _main

    bucket_a = TokenBucket(name="api", capacity=100, refill_per_second=1.0, backend="memory")
    bucket_b = TokenBucket(name="api", capacity=200, refill_per_second=1.0, backend="memory")

    @actor(name="actor_a", queue="default", rate_limits=[bucket_a])
    async def actor_a(payload: _Payload) -> None:
        pass

    @actor(name="actor_b", queue="default", rate_limits=[bucket_b])
    async def actor_b(payload: _Payload) -> None:
        pass

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://x:x@localhost/x"}, validate=False
    )
    with pytest.raises(ValueError, match="api"):
        await _main(
            settings,
            actor_registry={"actor_a": actor_a, "actor_b": actor_b},
            rate_limit_registry=RateLimitRegistry(),
        )


async def test_same_instance_on_two_actors_idempotent_and_counts_declarations() -> None:
    """The SAME TokenBucket instance declared on two actors: the collection
    pass completes WITHOUT ValueError (the second declaration is an
    idempotent no-op), the instance is registered exactly once in the owned
    registry, and the startup log counts 2 DECLARATIONS (not distinct
    registrations).

    _main proceeds past the collection pass into open_worker_deps, which
    fails on the unroutable DSN — that later failure must not surface as a
    ValueError (the pass itself succeeded).
    """
    from taskq.settings import WorkerSettings
    from taskq.worker.run import _main

    bucket = TokenBucket(name="shared", capacity=10, refill_per_second=1.0, backend="memory")

    @actor(name="actor_a", queue="default", rate_limits=[bucket])
    async def actor_a(payload: _Payload) -> None:
        pass

    @actor(name="actor_b", queue="default", rate_limits=[bucket])
    async def actor_b(payload: _Payload) -> None:
        pass

    own = RateLimitRegistry()
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://x:x@localhost/x"}, validate=False
    )
    with structlog.testing.capture_logs() as captured:
        pool_error: BaseException | None = None
        try:
            await _main(
                settings,
                actor_registry={"actor_a": actor_a, "actor_b": actor_b},
                rate_limit_registry=own,
            )
        except Exception as exc:
            pool_error = exc

    # (1) The collection pass did NOT raise ValueError — _main failed
    # later, at open_worker_deps on the unroutable DSN.
    assert pool_error is not None
    assert not isinstance(pool_error, ValueError)

    # Registered exactly once, and it IS the declared instance.
    assert len(own.rate_limits) == 1
    assert own.rate_limits["shared"] is bucket

    # The startup log counts DECLARATIONS: two actors declared the instance,
    # so rate_limit_count=2 even though register() stored it once.
    events = [e for e in captured if e.get("event") == "ratelimit-actor-primitives-registered"]
    assert len(events) == 1
    assert events[0]["rate_limit_count"] == 2
    assert events[0]["rate_limit_names"] == ["shared", "shared"]


async def test_actor_declared_queue_cap_prefix_name_raises_valueerror() -> None:
    """An actor declaring a ConcurrencyReservation whose name starts with
    the reserved queue-cap prefix: the collection pass's register() raises
    ValueError at bootstrap — reserved names must go through
    register_queue_cap_reservation(), never actor declarations."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import _main

    cap = ConcurrencyReservation(
        name=f"{QUEUE_CONCURRENCY_PREFIX}foo", slots=1, lease=timedelta(seconds=30)
    )

    @actor(name="cap_actor", queue="default", reservations=[cap])
    async def cap_actor(payload: _Payload) -> None:
        pass

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://x:x@localhost/x"}, validate=False
    )
    with pytest.raises(ValueError, match="reserved prefix"):
        await _main(
            settings,
            actor_registry={"cap_actor": cap_actor},
            rate_limit_registry=RateLimitRegistry(),
        )
