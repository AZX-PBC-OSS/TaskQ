"""Admin rate-limit handlers resolve the registry via get_rl_registry.

A bundle-provided instance wins; when app.state never had the key set
(hand-assembled state), the module singleton is the fallback.
"""

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from taskq.backend.clock import SystemClock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.registry import registry as singleton
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.web.admin._factory import get_rl_registry


def _bucket(name: str) -> TokenBucket:
    return TokenBucket(name=name, capacity=5, refill_per_second=1.0, backend="memory")


def _get_csrf_token(client: Any) -> str:
    """GET /queues to set the CSRF cookie, then return the token."""
    client.get("/queues")
    return client.cookies.get("taskq_csrf_token", "")


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


# ── Rate-limit reset (POST mutation) ─────────────────────────────────────


async def test_rate_limit_reset_uses_bundle_provided_registry(
    make_app: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /rate-limits/{name}/reset resets the OWNED registry's bucket.

    The singleton holds a same-named bucket in the same drained state and
    must remain untouched — proving the reset hit the owned registry, not
    the module singleton. Fixed-quota (refill=0) buckets keep token counts
    exact regardless of wall-clock elapsed time.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")

    clock = SystemClock()
    own = RateLimitRegistry()
    own.register(TokenBucket(name="api:owned", capacity=3, refill_per_second=0.0, backend="memory"))
    singleton.register(
        TokenBucket(name="api:owned", capacity=3, refill_per_second=0.0, backend="memory")
    )  # conftest clears the singleton before each test

    async with own.acquire("api:owned", clock=clock):
        pass
    async with singleton.acquire("api:owned", clock=clock):
        pass
    assert (await own.peek("api:owned", clock=clock)).tokens_remaining == 2.0
    assert (await singleton.peek("api:owned", clock=clock)).tokens_remaining == 2.0

    client = make_app(rate_limit_registry=own)
    token = _get_csrf_token(client)
    response = client.post(
        "/rate-limits/api:owned/reset",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"].endswith("/rate-limits")
    assert (await own.peek("api:owned", clock=clock)).tokens_remaining == 3.0
    assert (await singleton.peek("api:owned", clock=clock)).tokens_remaining == 2.0
