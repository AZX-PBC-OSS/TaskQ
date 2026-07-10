"""Tests for the Clock protocol and SystemClock implementation.

Covers the Definition of Done items:
- SystemClock.now() returns timezone-aware UTC within 1s of datetime.now(UTC)
- SystemClock.monotonic() is non-decreasing across two calls
- isinstance(SystemClock(), Clock) and isinstance(FakeClock(...), Clock) both return True
- No forbidden imports (no ``from __future__ import annotations``, no ``Any``)
- Only stdlib imports in the clock module
"""

from datetime import UTC, datetime

import pytest

from taskq.backend.clock import Clock, SystemClock

# ── SystemClock.now() ───────────────────────────────────────────────────


class TestSystemClockNow:
    def test_returns_datetime(self) -> None:
        result = SystemClock().now()
        assert isinstance(result, datetime)

    def test_timezone_is_utc(self) -> None:
        result = SystemClock().now()
        assert result.tzinfo is UTC

    def test_returns_reasonable_time(self) -> None:
        """SystemClock.now() returns UTC time within 1s of datetime.now(UTC)."""
        before = datetime.now(UTC)
        result = SystemClock().now()
        after = datetime.now(UTC)
        assert before <= result <= after
        assert result.tzinfo is not None


# ── SystemClock.monotonic() ────────────────────────────────────────────


class TestSystemClockMonotonic:
    def test_returns_float(self) -> None:
        result = SystemClock().monotonic()
        assert isinstance(result, float)

    def test_non_decreasing(self) -> None:
        """SystemClock.monotonic() is non-decreasing across two calls."""
        a = SystemClock().monotonic()
        b = SystemClock().monotonic()
        assert b >= a

    def test_positive(self) -> None:
        """Sanity: monotonic clock should return a positive value."""
        result = SystemClock().monotonic()
        assert result > 0.0


# ── Runtime checkability ───────────────────────────────────────────────


class TestRuntimeCheckable:
    def test_system_clock_is_clock(self) -> None:
        """isinstance(SystemClock(), Clock) returns True."""
        assert isinstance(SystemClock(), Clock)

    def test_fake_clock_is_clock(self) -> None:
        """isinstance(FakeClock(start=...), Clock) returns True."""
        from taskq.testing.clock import FakeClock

        assert isinstance(FakeClock(datetime(2025, 1, 1, tzinfo=UTC)), Clock)

    def test_plain_object_is_not_clock(self) -> None:
        assert not isinstance(object(), Clock)


# ── Static type compatibility ──────────────────────────────────────────


class TestStaticTypeCompatibility:
    def test_annotated_function_accepts_system_clock(self) -> None:
        """Demonstrates that SystemClock satisfies the Clock protocol
        at the static-type level — pyright verifies this call site."""

        def accept_clock(c: Clock) -> float:
            return c.monotonic()

        result = accept_clock(SystemClock())
        assert isinstance(result, float)


# ── No forbidden imports ───────────────────────────────────────────────


class TestNoForbiddenImports:
    def test_no_future_annotations(self) -> None:
        """no ``from __future__ import annotations``."""
        import taskq.backend.clock as clock_mod

        source_file = clock_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            for line in f:
                assert "from __future__ import annotations" not in line

    def test_no_any_import(self) -> None:
        """no ``Any`` in the clock module."""
        import taskq.backend.clock as clock_mod

        source_file = clock_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            for line in f:
                assert "Any" not in line

    def test_only_stdlib_imports(self) -> None:
        """Clock module imports only from the standard library.

        Only checks names in ``__all__`` to avoid dunder attributes
        that carry import-machinery ``__module__`` values.
        """
        import taskq.backend.clock as clock_mod

        allowed_prefixes = ("dataclasses", "datetime", "time", "typing", "collections", "abc")
        for name in clock_mod.__all__:
            obj = getattr(clock_mod, name)
            mod = getattr(obj, "__module__", None)
            if isinstance(mod, str) and mod not in ("taskq.backend.clock", "__main__"):
                assert mod.startswith(allowed_prefixes), (
                    f"Non-stdlib import detected: {name} from {mod}"
                )


# ── SystemClock is frozen dataclass with no state ──────────────────────


class TestSystemClockIsStateless:
    def test_frozen(self) -> None:
        """SystemClock is frozen — cannot mutate attributes.

        On CPython, frozen+slots dataclasses raise ``TypeError`` (not
        ``AttributeError``) when setting an undeclared slot on a zero-field
        class. Either exception proves the instance is immutable.
        """
        clock = SystemClock()
        with pytest.raises((AttributeError, TypeError)):
            clock._arbitrary = 42  # type: ignore[attr-defined] # Why: intentionally assigning to a non-existent attribute to prove the frozen+slots dataclass rejects it

    def test_slots(self) -> None:
        """SystemClock uses slots — no __dict__."""
        clock = SystemClock()
        assert not hasattr(clock, "__dict__")

    def test_has_no_init_params(self) -> None:
        """SystemClock requires no constructor arguments."""
        from dataclasses import fields

        assert len(fields(SystemClock)) == 0
