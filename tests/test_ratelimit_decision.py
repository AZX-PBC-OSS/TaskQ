"""Unit tests for RateLimitDecision dataclass and ratelimit re-exports."""

from dataclasses import FrozenInstanceError
from datetime import timedelta

import pytest

from taskq.ratelimit import RateLimitBackend, RateLimitDecision


class TestRateLimitDecisionConstruction:
    def test_allowed_round_trip(self) -> None:
        d = RateLimitDecision(
            allowed=True,
            remaining=10.0,
            retry_after=timedelta(0),
            bucket_name="x",
            backend="memory",
        )
        assert d.allowed is True
        assert d.remaining == 10.0
        assert d.retry_after == timedelta(0)
        assert d.bucket_name == "x"
        assert d.backend == "memory"

    def test_denied_with_none_retry_after(self) -> None:
        d = RateLimitDecision(
            allowed=False,
            remaining=0.0,
            retry_after=None,
            bucket_name="fixed",
            backend="redis",
        )
        assert d.allowed is False
        assert d.retry_after is None
        assert d.backend == "redis"


class TestRateLimitDecisionFrozen:
    def test_mutation_raises_frozen_instance_error(self) -> None:
        d = RateLimitDecision(
            allowed=True,
            remaining=5.0,
            retry_after=timedelta(0),
            bucket_name="y",
            backend="postgres",
        )
        with pytest.raises(FrozenInstanceError):
            d.allowed = False  # type: ignore[misc]

    def test_no_instance_dict(self) -> None:
        d = RateLimitDecision(
            allowed=True,
            remaining=1.0,
            retry_after=timedelta(0),
            bucket_name="z",
            backend="memory",
        )
        assert not hasattr(d, "__dict__")


class TestReExports:
    def test_from_ratelimit_import_decision(self) -> None:
        import taskq.ratelimit as rl

        assert rl.RateLimitDecision is RateLimitDecision

    def test_from_ratelimit_import_backend(self) -> None:
        assert RateLimitBackend is not None

    def test_backend_literal_values(self) -> None:
        for val in ("redis", "postgres", "memory"):
            d = RateLimitDecision(
                allowed=True,
                remaining=1.0,
                retry_after=timedelta(0),
                bucket_name="t",
                backend=val,  # type: ignore[arg-type] # Why: val is str (loop variable), not narrowed to Literal["redis","postgres","memory"] by pyright
            )
            assert d.backend == val
