"""Unit tests for RateLimitRef and ReservationRef ().

Tests pydantic model construction, field defaults, and the ratelimit
re-exports for the ref types.
"""

import pytest
from pydantic import ValidationError

from taskq.ratelimit import RateLimitRef, ReservationRef
from taskq.ratelimit.refs import RateLimitRef as RateLimitRefDirect
from taskq.ratelimit.refs import ReservationRef as ReservationRefDirect


class TestRateLimitRef:
    def test_construction_with_defaults(self) -> None:
        r = RateLimitRef(name="openai")
        assert r.name == "openai"
        assert r.count == 1.0

    def test_construction_with_count(self) -> None:
        r = RateLimitRef(name="openai", count=5.0)
        assert r.count == 5.0

    def test_mutable_by_default(self) -> None:
        r = RateLimitRef(name="openai")
        r.name = "other"
        assert r.name == "other"

    def test_rejects_missing_name(self) -> None:
        with pytest.raises(ValidationError):
            RateLimitRef()  # type: ignore[call-arg]

    def test_rejects_non_string_name(self) -> None:
        with pytest.raises(ValidationError):
            RateLimitRef(name=42)  # type: ignore[arg-type]


class TestReservationRef:
    def test_construction(self) -> None:
        r = ReservationRef(name="gpu_pool")
        assert r.name == "gpu_pool"

    def test_mutable_by_default(self) -> None:
        r = ReservationRef(name="gpu_pool")
        r.name = "other"
        assert r.name == "other"

    def test_rejects_missing_name(self) -> None:
        with pytest.raises(ValidationError):
            ReservationRef()  # type: ignore[call-arg]


class TestRefReExports:
    def test_rate_limit_ref_from_ratelimit(self) -> None:
        import taskq.ratelimit as rl

        assert rl.RateLimitRef is RateLimitRefDirect

    def test_reservation_ref_from_ratelimit(self) -> None:
        import taskq.ratelimit as rl

        assert rl.ReservationRef is ReservationRefDirect
