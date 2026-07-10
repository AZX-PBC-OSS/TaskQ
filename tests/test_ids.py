"""Unit tests for taskq._ids — UUIDv7 generation and base62 identifiers."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.testing.clock import FakeClock

_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE62_LEN = len(_BASE62)

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _dt_to_unix(dt: datetime) -> float:
    return dt.timestamp()


def _decode_base62(s: str) -> int:
    result = 0
    for ch in s:
        result = result * _BASE62_LEN + _BASE62.index(ch)
    return result


# ── new_uuid ────────────────────────────────────────────────────────────


class TestNewUuid:
    def test_returns_uuidv7(self) -> None:
        u = new_uuid()
        assert isinstance(u, UUID)
        assert u.version == 7

    def test_monotonic_across_calls(self) -> None:
        a = new_uuid()
        b = new_uuid()
        assert b >= a

    def test_unique(self) -> None:
        ids = {new_uuid() for _ in range(1000)}
        assert len(ids) == 1000


class TestNewJobId:
    def test_wraps_uuid(self) -> None:
        jid = new_job_id()
        assert isinstance(jid, UUID)
        assert jid.version == 7

    def test_unique(self) -> None:
        ids = {new_job_id() for _ in range(1000)}
        assert len(ids) == 1000


# ── new_base62 — charset and format ─────────────────────────────────────


class TestBase62Charset:
    @given(length=st.integers(min_value=1, max_value=32))
    @settings(max_examples=50, deadline=None)
    def test_random_mode_valid_charset(self, length: int) -> None:
        s = new_base62(length=length, precision="random")
        assert len(s) == length
        assert set(s) <= set(_BASE62)

    @given(length=st.integers(min_value=7, max_value=32))
    @settings(max_examples=50, deadline=None)
    def test_second_mode_valid_charset(self, length: int) -> None:
        s = new_base62(length=length, precision="second")
        assert len(s) == length
        assert set(s) <= set(_BASE62)

    @given(length=st.integers(min_value=9, max_value=32))
    @settings(max_examples=50, deadline=None)
    def test_millisecond_mode_valid_charset(self, length: int) -> None:
        s = new_base62(length=length, precision="millisecond")
        assert len(s) == length
        assert set(s) <= set(_BASE62)


class TestBase62Uniqueness:
    def test_random_8_unique_1000(self) -> None:
        ids = {new_base62(8, precision="random") for _ in range(1000)}
        assert len(ids) == 1000

    def test_random_16_unique_1000(self) -> None:
        ids = {new_base62(16, precision="random") for _ in range(1000)}
        assert len(ids) == 1000

    def test_second_14_unique_1000(self) -> None:
        # 14 chars = 6 timestamp + 8 random (62^8 ≈ 2.2e14 suffix space) —
        # the length production call sites use. Shorter lengths (e.g. 10 =
        # only 62^4 ≈ 14.7M suffixes) have a ~3% birthday-collision chance
        # at 1000 IDs/second and CANNOT guarantee uniqueness at this density.
        ids = {new_base62(14, precision="second") for _ in range(1000)}
        assert len(ids) == 1000

    def test_millisecond_16_unique_1000(self) -> None:
        # 16 chars = 8 timestamp + 8 random, matching production usage;
        # see test_second_14_unique_1000 for the density rationale.
        ids = {new_base62(16, precision="millisecond") for _ in range(1000)}
        assert len(ids) == 1000


class TestBase62RandomSuffixEntropy:
    def test_random_suffix_chars_are_diverse(self) -> None:
        samples = [new_base62(8, precision="random") for _ in range(100)]
        all_chars = "".join(samples)
        assert len(set(all_chars)) >= 40

    def test_second_random_suffix_chars_are_diverse(self) -> None:
        now = _dt_to_unix(_START)
        samples = [new_base62(10, precision="second", _now=now) for _ in range(100)]
        suffixes = [s[6:] for s in samples]
        distinct = set("".join(suffixes))
        assert len(distinct) >= 30

    def test_millisecond_random_suffix_chars_are_diverse(self) -> None:
        now = _dt_to_unix(_START)
        samples = [new_base62(12, precision="millisecond", _now=now) for _ in range(100)]
        suffixes = [s[8:] for s in samples]
        distinct = set("".join(suffixes))
        assert len(distinct) >= 30


class TestBase62Sortability:
    def test_second_mode_ids_sort_across_seconds(self) -> None:
        clock = FakeClock(_START)
        a = new_base62(8, precision="second", _now=_dt_to_unix(clock.now()))
        clock.advance(timedelta(seconds=1))
        b = new_base62(8, precision="second", _now=_dt_to_unix(clock.now()))
        assert a < b

    def test_millisecond_mode_ids_sort_across_milliseconds(self) -> None:
        clock = FakeClock(_START)
        a = new_base62(10, precision="millisecond", _now=_dt_to_unix(clock.now()))
        clock.advance(timedelta(milliseconds=1))
        b = new_base62(10, precision="millisecond", _now=_dt_to_unix(clock.now()))
        assert a < b

    def test_second_mode_same_second_shares_prefix(self) -> None:
        now = _dt_to_unix(_START)
        ids = [new_base62(10, precision="second", _now=now) for _ in range(10)]
        prefixes = {s[:6] for s in ids}
        assert len(prefixes) == 1

    def test_millisecond_mode_same_millisecond_shares_prefix(self) -> None:
        now = _dt_to_unix(_START)
        ids = [new_base62(12, precision="millisecond", _now=now) for _ in range(10)]
        prefixes = {s[:8] for s in ids}
        assert len(prefixes) == 1

    def test_second_mode_different_seconds_different_prefixes(self) -> None:
        clock = FakeClock(_START)
        prefixes = set()
        for _ in range(3):
            s = new_base62(8, precision="second", _now=_dt_to_unix(clock.now()))
            prefixes.add(s[:6])
            clock.advance(timedelta(seconds=1))
        assert len(prefixes) == 3

    def test_millisecond_mode_different_ms_different_prefixes(self) -> None:
        clock = FakeClock(_START)
        prefixes = set()
        for _ in range(3):
            s = new_base62(10, precision="millisecond", _now=_dt_to_unix(clock.now()))
            prefixes.add(s[:8])
            clock.advance(timedelta(milliseconds=1))
        assert len(prefixes) == 3

    def test_random_mode_not_guaranteed_sorted(self) -> None:
        ids = [new_base62(8, precision="random") for _ in range(100)]
        assert ids != sorted(ids)


class TestBase62LengthValidation:
    def test_random_min_length_1(self) -> None:
        new_base62(1, precision="random")

    def test_random_length_0_raises(self) -> None:
        with pytest.raises(ValueError, match="length must be >= 1"):
            new_base62(0, precision="random")

    def test_second_min_length_7(self) -> None:
        new_base62(7, precision="second")

    def test_second_length_6_raises(self) -> None:
        with pytest.raises(ValueError, match="length must be >= 7"):
            new_base62(6, precision="second")

    def test_millisecond_min_length_9(self) -> None:
        new_base62(9, precision="millisecond")

    def test_millisecond_length_8_raises(self) -> None:
        with pytest.raises(ValueError, match="length must be >= 9"):
            new_base62(8, precision="millisecond")


class TestBase62PrecisionValidation:
    def test_invalid_precision_raises(self) -> None:
        with pytest.raises(ValueError):
            new_base62(precision="hour")


class TestBase62TimestampEncoding:
    def test_second_timestamp_round_trips(self) -> None:
        now = _dt_to_unix(_START)
        s = new_base62(7, precision="second", _now=now)
        decoded = _decode_base62(s[:6])
        assert decoded == int(now)

    def test_millisecond_timestamp_round_trips(self) -> None:
        now = _dt_to_unix(_START)
        s = new_base62(10, precision="millisecond", _now=now)
        decoded = _decode_base62(s[:8])
        assert decoded == int(now * 1000)

    def test_second_timestamp_advances_with_clock(self) -> None:
        clock = FakeClock(_START)
        a = new_base62(7, precision="second", _now=_dt_to_unix(clock.now()))
        clock.advance(timedelta(seconds=1))
        b = new_base62(7, precision="second", _now=_dt_to_unix(clock.now()))
        ts_a = _decode_base62(a[:6])
        ts_b = _decode_base62(b[:6])
        assert ts_b == ts_a + 1

    def test_millisecond_timestamp_advances_with_clock(self) -> None:
        clock = FakeClock(_START)
        a = new_base62(10, precision="millisecond", _now=_dt_to_unix(clock.now()))
        clock.advance(timedelta(milliseconds=5))
        b = new_base62(10, precision="millisecond", _now=_dt_to_unix(clock.now()))
        ts_a = _decode_base62(a[:8])
        ts_b = _decode_base62(b[:8])
        assert ts_b == ts_a + 5

    def test_second_6char_capacity_covers_until_2106(self) -> None:
        year_2106 = 2**31 - 1
        assert year_2106 < _BASE62_LEN**6

    def test_millisecond_8char_capacity_covers_until_2106(self) -> None:
        year_2106_ms = (2**31 - 1) * 1000
        assert year_2106_ms < _BASE62_LEN**8


class TestBase62StructuralProperties:
    def test_second_mode_structure_is_timestamp_then_random(self) -> None:
        now = _dt_to_unix(_START)
        s1 = new_base62(10, precision="second", _now=now)
        s2 = new_base62(10, precision="second", _now=now)
        assert s1[:6] == s2[:6]
        assert s1[6:] != s2[6:]

    def test_millisecond_mode_structure_is_timestamp_then_random(self) -> None:
        now = _dt_to_unix(_START)
        s1 = new_base62(12, precision="millisecond", _now=now)
        s2 = new_base62(12, precision="millisecond", _now=now)
        assert s1[:8] == s2[:8]
        assert s1[8:] != s2[8:]

    def test_random_mode_all_chars_vary(self) -> None:
        s1 = new_base62(16, precision="random")
        s2 = new_base62(16, precision="random")
        assert s1 != s2

    def test_second_same_second_ids_differ_only_in_suffix(self) -> None:
        now = _dt_to_unix(_START)
        ids = [new_base62(14, precision="second", _now=now) for _ in range(10)]
        prefixes = {s[:6] for s in ids}
        suffixes = {s[6:] for s in ids}
        assert len(prefixes) == 1
        assert len(suffixes) == 10

    def test_millisecond_same_ms_ids_differ_only_in_suffix(self) -> None:
        now = _dt_to_unix(_START)
        ids = [new_base62(16, precision="millisecond", _now=now) for _ in range(10)]
        prefixes = {s[:8] for s in ids}
        suffixes = {s[8:] for s in ids}
        assert len(prefixes) == 1
        assert len(suffixes) == 10
