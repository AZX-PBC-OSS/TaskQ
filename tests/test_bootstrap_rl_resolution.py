"""Unit tests for the worker bootstrap RateLimitRegistry resolution order.

1. Explicit ``rate_limit_registry=`` argument wins.
2. A DI *value* provider wins over the singleton.
3. A DI factory/class provider raises TypeError (fail fast — a factory
   would split-brain bootstrap vs. LOOP-scope dispatch resolution).
4. Default: the module singleton.
"""

import pytest

from taskq._di import ProviderRegistry, Scope
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.registry import registry as singleton
from taskq.worker._bootstrap import (
    _resolve_rl_registry,  # pyright: ignore[reportPrivateUsage]  # Why: unit-testing the documented resolution-order helper directly
)


def test_explicit_argument_wins_over_di_and_singleton() -> None:
    explicit = RateLimitRegistry()
    di_valued = RateLimitRegistry()
    di = ProviderRegistry()
    di.register_value(RateLimitRegistry, Scope.LOOP, di_valued)

    assert _resolve_rl_registry(explicit, di) is explicit


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
