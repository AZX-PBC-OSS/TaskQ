"""Hypothesis property test for token-bucket invariants.

Drives the in-memory TokenBucket with random sequences of
``acquire(count)`` interleaved with FakeClock advances and asserts
four invariants across the entire run.

Runs only under ``-m slow`` so it does not slow the default unit tier.
"""

import math
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.ratelimit import TokenBucket
from taskq.testing.clock import FakeClock

pytestmark = pytest.mark.slow

_START = datetime(2025, 1, 1, tzinfo=UTC)

_OPERATION_STRATEGY = st.lists(
    st.tuples(
        st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
        st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
    ),
    max_size=200,
)

_CAPACITY_STRATEGY = st.floats(
    min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False
)
_REFILL_STRATEGY = st.one_of(
    st.just(0.0),
    st.floats(min_value=0.1, max_value=50.0, allow_nan=False, allow_infinity=False),
)


def _clamp_count(count: float, capacity: float) -> float:
    return max(0.1, min(count, capacity + 1.0))


@given(
    capacity=_CAPACITY_STRATEGY,
    refill_per_second=_REFILL_STRATEGY,
    operations=_OPERATION_STRATEGY,
)
@settings(max_examples=200, deadline=None)
async def test_token_bucket_invariants(
    capacity: float,
    refill_per_second: float,
    operations: list[tuple[float, float]],
) -> None:
    """token-bucket invariants hold over random acquire/clock-advance sequences."""
    tb = TokenBucket(
        name="prop",
        capacity=capacity,
        refill_per_second=refill_per_second,
        backend="memory",
    )
    clock = FakeClock(_START)

    total_consumed: float = 0.0
    total_elapsed: float = 0.0
    prev_remaining: float = capacity

    for count_raw, advance_seconds in operations:
        count = _clamp_count(count_raw, capacity)
        clock.advance(timedelta(seconds=advance_seconds))
        total_elapsed += advance_seconds

        result = await tb.acquire(count=count, clock=clock)

        # Invariant 1: Conservation — total tokens consumed ≤ capacity + refill * elapsed
        assert total_consumed <= capacity + refill_per_second * total_elapsed + 1e-9, (
            f"conservation violated: consumed={total_consumed} "
            f"capacity={capacity} refill={refill_per_second} "
            f"elapsed={total_elapsed}"
        )

        # Invariant 2: Non-negative retry_after
        if result.retry_after is not None:
            assert result.retry_after >= timedelta(0), f"negative retry_after: {result.retry_after}"

        # Invariant 3: remaining bounds
        assert 0 <= result.remaining <= capacity + 1e-9, (
            f"remaining out of bounds: {result.remaining} capacity={capacity}"
        )

        # Invariant 4: Allowed-implies-deduction
        if result.allowed:
            expected_upper = prev_remaining + advance_seconds * refill_per_second - count
            assert result.remaining <= expected_upper or math.isclose(
                result.remaining, expected_upper, rel_tol=1e-9, abs_tol=1e-4
            ), (
                f"allowed=True did not deduct: prev={prev_remaining} "
                f"advance={advance_seconds} refill={refill_per_second} "
                f"count={count} got={result.remaining}"
            )
            total_consumed += count

        prev_remaining = result.remaining
