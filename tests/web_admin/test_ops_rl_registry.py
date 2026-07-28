"""Admin rate-limit handlers resolve the registry via get_rl_registry.

A bundle-provided instance wins; when app.state never had the key set
(hand-assembled state), the module singleton is the fallback.
"""

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.registry import registry as singleton
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.web.admin._factory import get_rl_registry


def _bucket(name: str) -> TokenBucket:
    return TokenBucket(name=name, capacity=5, refill_per_second=1.0, backend="memory")


def test_get_rl_registry_prefers_app_state() -> None:
    own = RateLimitRegistry()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(rate_limit_registry=own)))

    assert get_rl_registry(request) is own  # type: ignore[arg-type]  # Why: duck-typed Request double


def test_get_rl_registry_falls_back_to_singleton_when_key_missing() -> None:
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    assert get_rl_registry(request) is singleton  # type: ignore[arg-type]  # Why: duck-typed Request double


def test_rate_limits_page_uses_bundle_provided_registry(
    make_app: Callable[..., Any],
) -> None:
    own = RateLimitRegistry()
    own.register(_bucket("owned_bucket"))
    client = make_app(rate_limit_registry=own)

    response = client.get("/rate-limits")

    assert response.status_code == 200
    assert "owned_bucket" in response.text


def test_rate_limits_page_falls_back_to_singleton(
    make_app: Callable[..., Any],
) -> None:
    singleton.register(
        _bucket("singleton_bucket")
    )  # conftest clears the singleton before each test
    client = make_app()

    response = client.get("/rate-limits")

    assert response.status_code == 200
    assert "singleton_bucket" in response.text


def test_reservations_page_uses_bundle_provided_registry(
    make_app: Callable[..., Any],
) -> None:
    from datetime import timedelta

    from taskq.ratelimit.reservation import ConcurrencyReservation

    own = RateLimitRegistry()
    own.register(ConcurrencyReservation(name="owned_res", slots=2, lease=timedelta(seconds=30)))
    client = make_app(rate_limit_registry=own)

    response = client.get("/reservations")

    assert response.status_code == 200
    assert "owned_res" in response.text
