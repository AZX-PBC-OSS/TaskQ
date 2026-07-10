"""Hypothesis property tests for sliding-window invariants.

Drives the in-memory SlidingWindow with random sequences of ``acquire()``
interleaved with FakeClock advances and asserts algorithmic invariants for
both ``style="log"`` and ``style="gcra"``.

Two coverage tiers:

- **Default (every unit run):** ``@settings(max_examples=50, deadline=None)``
  so property tests are fast (< 5 s combined) and run on every
  ``pytest -m 'not integration'`` invocation.
- **Extended (``@pytest.mark.slow``):** ``@settings(max_examples=200,
  deadline=None)`` for the high-volume CI run. Activated via
  ``pytest -m slow``; excluded from the default unit tier.
"""

from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_base62
from taskq.ratelimit import SlidingWindow
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)

LIMIT = 60
WINDOW = timedelta(seconds=60)
WINDOW_MS = int(WINDOW.total_seconds() * 1000)

_LOG_STRATEGY = st.lists(
    st.tuples(
        st.integers(min_value=1, max_value=1000),
        st.floats(min_value=0.0, max_value=120.0, allow_nan=False, allow_infinity=False),
    ),
    max_size=200,
)

_GCRA_STRATEGY = st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=1000),
        st.floats(min_value=0.0, max_value=120.0, allow_nan=False, allow_infinity=False),
    ),
    max_size=200,
)


async def _check_log_invariants(operations: list[tuple[int, float]]) -> None:
    bucket = f"prop_{new_base62()}"
    sw = SlidingWindow(name=bucket, limit=LIMIT, window=WINDOW, backend="memory", style="log")
    clock = FakeClock(_START)

    allowed_timestamps: list[int] = []

    for advance_ms, _ in operations:
        clock.advance(timedelta(milliseconds=advance_ms))
        now_ms = int(clock.now().timestamp() * 1000)

        result = await sw.acquire(clock=clock)

        if result.allowed:
            allowed_timestamps.append(now_ms)

        now_ms_actual = int(clock.now().timestamp() * 1000)
        cutoff = now_ms_actual - WINDOW_MS
        in_window = [t for t in allowed_timestamps if cutoff < t <= now_ms_actual]

        assert len(in_window) <= LIMIT, (
            f"bounded count violated: {len(in_window)} > {LIMIT} at now_ms={now_ms_actual}"
        )

        assert result.retry_after is not None
        assert result.retry_after >= timedelta(0), f"negative retry_after: {result.retry_after}"

        assert 0.0 <= result.remaining <= float(LIMIT), (
            f"remaining out of bounds: {result.remaining}"
        )

        if result.allowed:
            pre_count = len([t for t in allowed_timestamps[:-1] if cutoff < t <= now_ms_actual])
            assert pre_count < LIMIT, (
                f"allowed-implies-fits violated: pre_count={pre_count} >= limit={LIMIT}"
            )


async def _check_gcra_invariants(operations: list[tuple[int, float]]) -> None:
    bucket = f"prop_{new_base62()}"
    sw = SlidingWindow(name=bucket, limit=LIMIT, window=WINDOW, backend="memory", style="gcra")
    clock = FakeClock(_START)

    allowed_timestamps: list[int] = []
    last_denial_at_same_now: timedelta | None = None
    last_now_ms: int | None = None

    for advance_ms, _ in operations:
        clock.advance(timedelta(milliseconds=advance_ms))
        now_ms = int(clock.now().timestamp() * 1000)

        if now_ms != last_now_ms:
            last_denial_at_same_now = None

        result = await sw.acquire(clock=clock)

        assert result.retry_after is not None
        assert result.retry_after >= timedelta(0), f"negative retry_after: {result.retry_after}"

        assert 0.0 <= result.remaining <= float(LIMIT), (
            f"remaining out of bounds: {result.remaining}"
        )

        if not result.allowed:
            if last_denial_at_same_now is not None:
                assert result.retry_after <= last_denial_at_same_now, (
                    f"monotonic-emission violated: retry_after={result.retry_after} "
                    f"> previous={last_denial_at_same_now} at same now_ms={now_ms}"
                )
            last_denial_at_same_now = result.retry_after
        else:
            last_denial_at_same_now = None
            allowed_timestamps.append(now_ms)

        last_now_ms = now_ms

        cutoff = now_ms - WINDOW_MS
        in_window = [t for t in allowed_timestamps if cutoff < t <= now_ms]
        assert len(in_window) <= LIMIT, (
            f"bounded-burst violated: {len(in_window)} > {LIMIT} at now_ms={now_ms}"
        )


@given(operations=_LOG_STRATEGY)
@settings(max_examples=50, deadline=None)
async def test_log_invariants_default(operations: list[tuple[int, float]]) -> None:
    """(log, default tier): log-style invariants hold over random sequences."""
    await _check_log_invariants(operations)


@given(operations=_LOG_STRATEGY)
@settings(max_examples=200, deadline=None)
@pytest.mark.slow
async def test_log_invariants_slow(operations: list[tuple[int, float]]) -> None:
    """(log, slow tier): log-style invariants at high volume."""
    await _check_log_invariants(operations)


@given(operations=_GCRA_STRATEGY)
@settings(max_examples=50, deadline=None)
async def test_gcra_invariants_default(operations: list[tuple[int, float]]) -> None:
    """(gcra, default tier): GCRA invariants hold over random sequences."""
    await _check_gcra_invariants(operations)


@given(operations=_GCRA_STRATEGY)
@settings(max_examples=200, deadline=None)
@pytest.mark.slow
async def test_gcra_invariants_slow(operations: list[tuple[int, float]]) -> None:
    """(gcra, slow tier): GCRA invariants at high volume."""
    await _check_gcra_invariants(operations)
