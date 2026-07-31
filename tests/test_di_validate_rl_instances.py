"""Phase 2b: primitive instances declared on actors are validated by .name.

Post-registration (the worker bootstrap collection pass) instances always
resolve; an unregistered instance fails with MissingProvider carrying the
remediation text.
"""

from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq.actor import ActorRef
from taskq.exceptions import MissingProvider
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import RetryPolicy


class _Settings:
    pass


class _Payload(BaseModel):
    x: int


class _Result(BaseModel):
    y: int


def _make_actor(
    name: str,
    rate_limits: list[Any] | None = None,
    reservations: list[Any] | None = None,
) -> ActorRef[Any, Any]:
    async def fn(payload: _Payload) -> _Result:
        return _Result(y=payload.x)

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


def _di_registry() -> ProviderRegistry:
    di = ProviderRegistry()
    di.register_value(_Settings, Scope.PROCESS, _Settings())
    return di


def test_registered_rate_limit_instance_passes_validation() -> None:
    rl = RateLimitRegistry()
    tb = TokenBucket(name="api", capacity=10, refill_per_second=1.0, backend="memory")
    rl.register(tb)
    actor = _make_actor("a", rate_limits=[tb])

    _di_registry().validate(actors=[actor], rate_limit_registry=rl)


def test_unregistered_rate_limit_instance_fails_with_remediation() -> None:
    rl = RateLimitRegistry()
    tb = TokenBucket(name="ghost", capacity=10, refill_per_second=1.0, backend="memory")
    actor = _make_actor("a", rate_limits=[tb])

    with pytest.raises(MissingProvider) as exc_info:
        _di_registry().validate(actors=[actor], rate_limit_registry=rl)

    assert exc_info.value.type_name == "RateLimit"
    assert "a" in exc_info.value.required_by
    assert "ghost" in exc_info.value.required_by
    assert "declare the primitive on the actor" in exc_info.value.required_by


def test_unregistered_reservation_instance_fails_with_remediation() -> None:
    rl = RateLimitRegistry()
    res = ConcurrencyReservation(name="ghost_res", slots=2, lease=timedelta(seconds=30))
    actor = _make_actor("a", reservations=[res])

    with pytest.raises(MissingProvider) as exc_info:
        _di_registry().validate(actors=[actor], rate_limit_registry=rl)

    assert exc_info.value.type_name == "ConcurrencyReservation"
    assert "ghost_res" in exc_info.value.required_by
    assert "register it on the worker's rate-limit registry" in exc_info.value.required_by


def test_registered_reservation_instance_passes_validation() -> None:
    rl = RateLimitRegistry()
    res = ConcurrencyReservation(name="db_seats", slots=2, lease=timedelta(seconds=30))
    rl.register(res)
    actor = _make_actor("a", reservations=[res])

    _di_registry().validate(actors=[actor], rate_limit_registry=rl)


def test_registered_sliding_window_instance_passes_validation() -> None:
    rl = RateLimitRegistry()
    sw = SlidingWindow(
        name="api_sw",
        limit=5,
        window=timedelta(seconds=10),
        backend="memory",
        style="log",
    )
    rl.register(sw)
    actor = _make_actor("a", rate_limits=[sw])

    _di_registry().validate(actors=[actor], rate_limit_registry=rl)
