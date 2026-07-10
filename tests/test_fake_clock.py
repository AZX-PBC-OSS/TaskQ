"""Tests for FakeClock.

Covers the Definition of Done items for ``src/taskq/testing/clock.py``:
- ``move_to`` and ``advance`` mutate ``now()`` correctly
- ``monotonic()`` is consistent: same wall-clock returns same float; never
  decreases within a test
- ``FakeClock`` is structurally compatible with ``Clock``
- Two ``FakeClock`` instances are isolated (partial)
- FakeClock(start=...).now() returns the start time
- advance() updates now(); return value is None
- move_to() updates now(); return value is None
- monotonic() increases by exactly the advanced delta
- negative advance is allowed
- negative advance with defined start at noon UTC
- monotonic consistency after advance
- advance is sum-consistent under arbitrary delta sequences
"""

from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis.strategies import lists, timedeltas

from taskq.backend.clock import Clock
from taskq.testing.clock import FakeClock

# ── Default start time ────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)


# ── FakeClock.now() returns start time ────────────────


class TestStartValue:
    def test_now_returns_start(self) -> None:
        """FakeClock(start=...).now() returns the start time."""
        clock = FakeClock(_START)
        assert clock.now() == _START

    def test_now_returns_custom_start(self) -> None:
        """FakeClock with arbitrary start returns that start."""
        custom = datetime(2024, 6, 15, 12, 30, tzinfo=UTC)
        clock = FakeClock(custom)
        assert clock.now() == custom


# ── move_to / advance ─────────────────────────────────────────────────


class TestMoveToAndAdvance:
    def test_move_to_sets_now(self) -> None:
        """move_to(datetime) updates now(); return value is None."""
        clock = FakeClock(_START)
        target = datetime(2025, 6, 15, tzinfo=UTC)
        result = clock.move_to(target)
        assert result is None
        assert clock.now() == target

    def test_advance_adds_delta(self) -> None:
        """advance(timedelta) updates now(); return value is None."""
        clock = FakeClock(_START)
        result = clock.advance(timedelta(hours=2))
        assert result is None
        expected = _START + timedelta(hours=2)
        assert clock.now() == expected

    def test_advance_zero_is_noop(self) -> None:
        clock = FakeClock(_START)
        clock.advance(timedelta(0))
        assert clock.now() == _START

    def test_move_then_advance(self) -> None:
        clock = FakeClock(_START)
        target = datetime(2025, 3, 1, tzinfo=UTC)
        clock.move_to(target)
        clock.advance(timedelta(minutes=30))
        assert clock.now() == target + timedelta(minutes=30)


# ── monotonic() ───────────────────────────────────────────────────────


class TestMonotonic:
    def test_returns_float(self) -> None:
        clock = FakeClock(_START)
        assert isinstance(clock.monotonic(), float)

    def test_nonzero_at_fixture_start(self) -> None:
        """monotonic returns ~157,766,400.0 at the 2025-01-01 fixture start."""
        clock = FakeClock(_START)
        mono = clock.monotonic()
        assert mono > 0.0
        # Roughly 5 years from 2020-01-01 epoch
        assert 150_000_000 < mono < 200_000_000

    def test_same_wallclock_same_float(self) -> None:
        """Same wall-clock position always returns the same float."""
        a = FakeClock(_START)
        b = FakeClock(_START)
        assert a.monotonic() == b.monotonic()

    def test_never_decreases(self) -> None:
        """monotonic never decreases within a test."""
        clock = FakeClock(_START)
        t1 = clock.monotonic()
        clock.advance(timedelta(seconds=1))
        t2 = clock.monotonic()
        assert t2 > t1

    def test_advance_by_one_second(self) -> None:
        """advance(timedelta(seconds=10)) increases monotonic by exactly 10.0."""
        clock = FakeClock(_START)
        before = clock.monotonic()
        clock.advance(timedelta(seconds=10))
        after = clock.monotonic()
        assert after - before == pytest.approx(10.0)

    def test_move_to_future_increases(self) -> None:
        clock = FakeClock(_START)
        before = clock.monotonic()
        clock.move_to(_START + timedelta(hours=1))
        after = clock.monotonic()
        assert after > before


# ── Structural compatibility with Clock ────────────────────────────────


class TestClockCompatibility:
    def test_annotated_function_accepts_fake_clock(self) -> None:
        """Demonstrates that FakeClock satisfies the Clock protocol
        at the static-type level — pyright verifies this call site."""

        def accept_clock(c: Clock) -> float:
            return c.monotonic()

        result = accept_clock(FakeClock(_START))
        assert isinstance(result, float)

    def test_isinstance_clock(self) -> None:
        """Clock is @runtime_checkable; FakeClock should pass isinstance."""
        assert isinstance(FakeClock(_START), Clock)


# ── Instance isolation (partial) ────────────────────────────────


class TestInstanceIsolation:
    def test_two_clocks_independent(self) -> None:
        """Two FakeClock instances are fully isolated."""
        a = FakeClock(_START)
        b = FakeClock(_START)
        a.advance(timedelta(hours=1))
        assert a.now() == _START + timedelta(hours=1)
        assert b.now() == _START

    def test_move_to_does_not_affect_other(self) -> None:
        a = FakeClock(_START)
        b = FakeClock(_START)
        a.move_to(datetime(2026, 1, 1, tzinfo=UTC))
        assert b.now() == _START


# ── negative advance is allowed ──────────────────────


class TestNegativeAdvance:
    def test_advance_negative_no_exception(self) -> None:
        """FakeClock.advance(timedelta(seconds=-30)) is allowed; no exception."""
        clock = FakeClock(_START)
        clock.advance(timedelta(seconds=-30))
        assert clock.now() == _START + timedelta(seconds=-30)

    def test_advance_negative_returns_none(self) -> None:
        """negative advance returns None (not datetime)."""
        clock = FakeClock(_START)
        result = clock.advance(timedelta(seconds=-30))
        assert result is None


# ── negative advance with defined start ───────────────


class TestNegativeAdvanceWithStart:
    def test_negative_advance_from_noon(self) -> None:
        """advance(seconds=-30) from noon UTC → 11:59:30 UTC."""
        noon = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        clock = FakeClock(noon)
        clock.advance(timedelta(seconds=-30))
        assert clock.now() == datetime(2025, 1, 1, 11, 59, 30, tzinfo=UTC)


# ── monotonic consistency after advance ───────────────


class TestMonotonicConsistencyAfterAdvance:
    def test_monotonic_increases_by_exactly_60(self) -> None:
        """after advance(timedelta(seconds=60)), monotonic() increases by 60.0."""
        clock = FakeClock(_START)
        before = clock.monotonic()
        clock.advance(timedelta(seconds=60))
        after = clock.monotonic()
        assert after - before == pytest.approx(60.0)


# ── advance is sum-consistent under arbitrary delta sequences ──


_BOUNDED_DELTAS = timedeltas(
    min_value=timedelta(days=-365),
    max_value=timedelta(days=365),
)


@given(deltas=lists(_BOUNDED_DELTAS, max_size=100))
@settings(max_examples=200)
def test_advance_sum_consistency(deltas: list[timedelta]) -> None:
    """FakeClock.advance is sum-consistent under arbitrary delta sequences."""
    clock = FakeClock(_START)
    for delta in deltas:
        clock.advance(delta)
    assert clock.now() == _START + sum(deltas, timedelta())
