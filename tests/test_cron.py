"""Tests for :mod:`taskq.cron` and :mod:`taskq.scheduler`.

compute_next_fire_after with UTC.
compute_next_fire_after at minute boundary.
_resolve_factory succeeds and caches.
_resolve_factory failure - ModuleNotFoundError.
_resolve_factory failure - AttributeError.
cron() with both payload_factory and static_payload raises ValueError.
property test - compute_next_fire_after always returns a datetime
       strictly after the `after` argument for valid expressions.
"""

import asyncio
import contextlib
import threading
import time
from collections.abc import Iterator
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from croniter import croniter
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from pydantic import BaseModel, ValidationError

from taskq.backend._protocol import ScheduleRecord
from taskq.cron import (
    DST_STRATEGIES,
    CronScheduleSpec,
    ScheduleHandle,
    _check_gap,
    _factory_cache,
    _fold_to_utc,
    _is_ambiguous_time,
    _resolve_factory,
    compute_next_fire_after,
    cron,
    resolve_payload,
)
from taskq.scheduler import _CRON_REGISTRY, get_registered_crons, register_cron


def test_dst_strategies_matches_the_literal() -> None:
    """One source of truth for the DST strategy set.

    The set was hand-rolled three times (backend validation, cron loop
    coercion, admin ops coercion) before this export; a fourth hand-rolled
    copy would drift against the ``DstStrategy`` Literal the type checker
    enforces. Derived via ``get_args`` so adding a strategy to the Literal
    updates the set - and every coercion site - with it.
    """
    assert frozenset({"skip", "firstof", "allof"}) == DST_STRATEGIES


def _sample_factory() -> dict[str, int]:
    return {"x": 1}


class _SamplePayload(BaseModel):
    name: str
    count: int


def _sample_model_factory() -> _SamplePayload:
    return _SamplePayload(name="test", count=3)


async def _async_factory() -> "dict[str, str]":
    return {"async": "yes"}


def _bad_factory() -> int:
    return 42


# ── Autouse fixture: restore _CRON_REGISTRY and _factory_cache ──────


@pytest.fixture(autouse=True)
def _restore_module_globals(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Snapshot and restore _CRON_REGISTRY and _factory_cache.

    Tests that use ``register_cron`` or ``_resolve_factory`` mutate
    module-level state; this fixture ensures every test starts clean.
    File-scope autouse is justified because the majority of tests in
    this file touch the registry or cache.
    """
    original_registry = list(_CRON_REGISTRY)
    original_cache = dict(_factory_cache)
    try:
        yield
    finally:
        _CRON_REGISTRY.clear()
        _CRON_REGISTRY.extend(original_registry)
        _factory_cache.clear()
        _factory_cache.update(original_cache)


# ── compute_next_fire_after with UTC ────────────────────────


def testcompute_next_fire_after_utc() -> None:
    """'0 3 * * *' at 10:00 UTC → next 03:00 UTC."""
    after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("0 3 * * *", "UTC", after)
    assert result == [datetime(2025, 1, 2, 3, 0, 0, tzinfo=UTC)]


# ── compute_next_fire_after at minute boundary ───────────────


def testcompute_next_fire_after_minute_boundary() -> None:
    """'*/5 * * * *' at 10:00 UTC → 10:05 UTC."""
    after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("*/5 * * * *", "UTC", after)
    assert result == [datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)]


# ── compute_next_fire_after with a 6-field (seconds) expression ──────


def testcompute_next_fire_after_seconds_precision() -> None:
    """'*/5 * * * * *' (6-field, seconds precision) at 10:00:00 UTC → 10:00:01 UTC."""
    after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("*/5 * * * * *", "UTC", after)
    assert result == [datetime(2025, 1, 1, 10, 0, 1, tzinfo=ZoneInfo("UTC"))]


# ── compute_next_fire_after: candidate.tzinfo is None branch ─────────


def testcompute_next_fire_after_naive_candidate_gets_localized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When croniter.get_next returns a naive datetime, compute_next_fire_after
    localizes it to the schedule's timezone (line: `if candidate.tzinfo is None`).

    Real croniter versions always return a tz-aware candidate when given a
    tz-aware `after_local`; this test forces the naive branch by monkeypatching
    croniter.get_next, since it is otherwise unreachable through real croniter
    behavior with the installed version.
    """

    def fake_get_next(
        self: croniter, ret_type: object = None, *args: object, **kwargs: object
    ) -> datetime:
        return datetime(2025, 6, 15, 10, 30, 0)

    monkeypatch.setattr(croniter, "get_next", fake_get_next)
    after = datetime(2025, 6, 15, 8, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after(
        "30 10 * * *", "America/New_York", after, dst_strategy="firstof"
    )
    tz = ZoneInfo("America/New_York")
    assert result == [datetime(2025, 6, 15, 10, 30, 0, tzinfo=tz)]


# ── compute_next_fire_after: DST gap (spring-forward) ────────────────


def testcompute_next_fire_after_gap_day_fires_at_gap_end_not_next_day() -> None:
    """Unforced vectors for the REAL gap behavior on spring-forward day.

    On 2025-03-30 Europe/Berlin the local wall 02:00→03:00 never exists
    (the gap is [02:00, 03:00)).  croniter's aware-seed resolution, the
    only path production seeds reach, answers a match that falls INSIDE
    the gap with the gap's END instant - the fire still happens that day,
    delayed by the gap's length.  It does NOT skip to the next day's
    match, and it does not drop the first valid post-gap match:

    * a daily 02:30 fires that day at 03:00 (a shifted instant, not a
      cron match - 'next valid cron match' would be 2025-03-31 02:30);
    * an hourly schedule's 02:00 match shifts to 03:00 and the 03:00
      match is that same instant, so nothing between the seed and the
      answer is dropped;
    * a minutely schedule's 60 gap minutes collapse to the single 03:00
      fire (the first post-gap match, at its normal wall time).

    The monkeypatched tests below pin the defensive branch that governs
    if a future croniter ever returns a nonexistent candidate; these pin
    what operators actually observe today.  All three strategies share
    the gap behavior, and 'allof' returns ONE element on a gap day (a
    gap has no fold pair to double).
    """
    tz_name = "Europe/Berlin"
    tz = ZoneInfo(tz_name)
    gap_end = datetime(2025, 3, 30, 3, 0, tzinfo=tz)

    seed = datetime(2025, 3, 30, 1, 30, tzinfo=tz)  # 01:30 CET, exists
    out = compute_next_fire_after("30 2 * * *", tz_name, seed, dst_strategy="skip")
    assert out == [gap_end], (
        "the transition-day fire must happen at the gap's end, not skip to "
        f"the next day's match, got {[d.isoformat() for d in out]}"
    )

    out = compute_next_fire_after("0 * * * *", tz_name, seed, dst_strategy="skip")
    assert out == [gap_end], (
        "the hourly 02:00 match shifts to the gap end and IS the 03:00 "
        f"match; nothing in between may be dropped, got {[d.isoformat() for d in out]}"
    )

    seed = datetime(2025, 3, 30, 1, 59, tzinfo=tz)
    out = compute_next_fire_after("* * * * *", tz_name, seed, dst_strategy="skip")
    assert out == [gap_end], (
        "the gap's minutes are skipped and the first post-gap match fires "
        f"at its normal wall time, got {[d.isoformat() for d in out]}"
    )

    for strategy in ("firstof", "allof"):
        seed = datetime(2025, 3, 30, 1, 30, tzinfo=tz)
        out = compute_next_fire_after("30 2 * * *", tz_name, seed, dst_strategy=strategy)
        assert out == [gap_end], (
            f"{strategy} shares the gap behavior and answers one instant on "
            f"a gap day, got {[d.isoformat() for d in out]}"
        )


def testcompute_next_fire_after_gap_advances_to_next_valid_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On 2025-03-09 America/New_York, 02:00→03:00 (02:30 does not exist).
    Forcing croniter to return the nonexistent 02:30 candidate exercises
    the gap-detection and _check_gap advancement; the result is the next
    valid cron match (2025-03-10 02:30), not the same-day gap time.

    Real croniter already resolves this internally for the installed
    version (it never returns 02:30 for '30 2 * * *' on this date), so the
    gap branch is forced via monkeypatching the first get_next call only;
    the second internal call (cr2.get_next inside compute_next_fire_after)
    falls through to the real implementation.
    """
    tz = ZoneInfo("America/New_York")
    calls = {"n": 0}
    original_get_next = cast(Any, croniter.get_next)

    def fake_get_next(
        self: croniter, ret_type: object = None, *args: object, **kwargs: object
    ) -> datetime:
        calls["n"] += 1
        if calls["n"] == 1:
            return datetime(2025, 3, 9, 2, 30, 0, tzinfo=tz)
        return cast(datetime, original_get_next(self, ret_type, *args, **kwargs))

    monkeypatch.setattr(croniter, "get_next", fake_get_next)
    after = datetime(2025, 3, 8, 20, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("30 2 * * *", "America/New_York", after, dst_strategy="skip")
    assert result == [datetime(2025, 3, 10, 2, 30, 0, tzinfo=tz)]


def testcompute_next_fire_after_gap_naive_next_valid_gets_localized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the second (post-gap) croniter.get_next call also returns a naive
    datetime, compute_next_fire_after localizes it too before delegating to
    _check_gap (line: `if next_valid.tzinfo is None`)."""
    tz = ZoneInfo("America/New_York")
    calls = {"n": 0}

    def fake_get_next(
        self: croniter, ret_type: object = None, *args: object, **kwargs: object
    ) -> datetime:
        calls["n"] += 1
        if calls["n"] == 1:
            return datetime(2025, 3, 9, 2, 30, 0)
        if calls["n"] == 2:
            return datetime(2025, 3, 10, 2, 30, 0)
        raise AssertionError("unexpected third croniter.get_next call")

    monkeypatch.setattr(croniter, "get_next", fake_get_next)
    after = datetime(2025, 3, 8, 20, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("30 2 * * *", "America/New_York", after, dst_strategy="skip")
    assert result == [datetime(2025, 3, 10, 2, 30, 0, tzinfo=tz)]


# ── compute_next_fire_after: DST overlap (fall-back) ──────────────────


def testcompute_next_fire_after_overlap_allof_returns_both_occurrences() -> None:
    """On 2025-11-02 America/New_York, 02:00→01:00 (01:30 occurs twice).
    dst_strategy='allof' returns both UTC instants, 1 hour apart."""
    tz = ZoneInfo("America/New_York")
    after = datetime(2025, 11, 1, 20, 0, 0, tzinfo=tz).astimezone(UTC)
    result = compute_next_fire_after("30 1 * * *", "America/New_York", after, dst_strategy="allof")
    assert len(result) == 2
    assert result[0].astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)
    assert result[1].astimezone(UTC) == datetime(2025, 11, 2, 6, 30, 0, tzinfo=UTC)


@pytest.mark.parametrize("dst_strategy", ["skip", "firstof"])
def testcompute_next_fire_after_overlap_skip_and_firstof_use_earlier_occurrence(
    dst_strategy: str,
) -> None:
    """For the same DST overlap, 'skip' and 'firstof' both return only the
    earlier (fold=0) occurrence."""
    tz = ZoneInfo("America/New_York")
    after = datetime(2025, 11, 1, 20, 0, 0, tzinfo=tz).astimezone(UTC)
    result = compute_next_fire_after(
        "30 1 * * *",
        "America/New_York",
        after,
        dst_strategy=cast(Any, dst_strategy),
    )
    assert len(result) == 1
    assert result[0].astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)


def testcompute_next_fire_after_overlap_seed_on_spent_earlier_occurrence_advances_beyond_range() -> (
    None
):
    """A fold-0 seed SITTING ON the overlap's earlier occurrence - the slot
    that just fired - must be answered with a fire strictly AFTER the seed
    under ``skip`` and ``firstof``.

    WHY IT MATTERS: the seed (``server_now`` in the tick, ``next_fire_at``'s
    re-anchor) at wall 01:30 fold=0 IS the earlier occurrence, already
    spent. croniter answers the fold-1 replay (strictly after, correct for
    its own wall semantics); the overlap branch then re-interpreted that
    candidate at fold=0 for ``skip``/``firstof`` - the SAME instant as the
    seed. The tick's advance wrote a ``next_fire_at`` at or before the
    instant that had just fired: the schedule stayed due, and re-fired
    every tick through the rest of the repeated hour. The function's own
    contract says it may never answer at or before its seed; for a spent
    slot the replay is owed nothing, the next fire is the first match
    beyond the repeated range - the same answer a fold-1 seed inside the
    range already gets.
    """
    tz = ZoneInfo("America/New_York")
    # 2025-11-02 01:30 fold=0 EDT == 05:30 UTC: the earlier occurrence of
    # the day's only 01:30 slot, the instant a tick at that slot fires.
    spent = datetime(2025, 11, 2, 1, 30, 0, tzinfo=tz)
    assert spent.astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)
    assert spent.fold == 0
    for dst_strategy in ("skip", "firstof"):
        result = compute_next_fire_after(
            "30 1 * * *",
            "America/New_York",
            spent,
            dst_strategy=cast(Any, dst_strategy),
        )
        assert len(result) == 1
        assert result[0].astimezone(UTC) > spent.astimezone(UTC), (
            f"{dst_strategy}: a seed on the spent earlier occurrence must "
            "be answered strictly after itself"
        )
        # The first match BEYOND the repeated range: the next day's 01:30
        # (unambiguous, EST), not the fold-1 replay of today's slot.
        assert result[0].astimezone(UTC) == datetime(2025, 11, 3, 6, 30, 0, tzinfo=UTC)


def testcompute_next_fire_after_overlap_seed_just_before_slot_still_fires_the_slot() -> None:
    """The spent-slot advance must not swallow the slot itself: a seed just
    BEFORE the overlap slot still answers that slot's earlier occurrence."""
    tz = ZoneInfo("America/New_York")
    before = datetime(2025, 11, 2, 1, 29, 0, tzinfo=tz)
    result = compute_next_fire_after("30 1 * * *", "America/New_York", before, dst_strategy="skip")
    assert len(result) == 1
    assert result[0].astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)


def testcompute_next_fire_after_overlap_seed_on_spent_slot_allof_answers_the_replay() -> None:
    """Under ``allof`` the same spent fold-0 seed answers the fold-1
    replay: both occurrences fire, the fold-0 one just did."""
    tz = ZoneInfo("America/New_York")
    spent = datetime(2025, 11, 2, 1, 30, 0, tzinfo=tz)
    result = compute_next_fire_after("30 1 * * *", "America/New_York", spent, dst_strategy="allof")
    assert len(result) == 1
    assert result[0].astimezone(UTC) == datetime(2025, 11, 2, 6, 30, 0, tzinfo=UTC)


def testcompute_next_fire_after_chain_never_stalls_in_a_repeated_range() -> None:
    """Chained fires across a fall-back day strictly increase in absolute
    time for every strategy: the walk has no zero-interval fixed point.

    Seeds cover every minute of the repeated hour (fold 0 and fold 1),
    the zone where the fixed point lived. This is the property the tick
    depends on: ``next_fire_at`` advancing past the instant it was seeded
    from is what makes a due schedule eventually not-due.
    """
    tz_name = "America/New_York"
    tz = ZoneInfo(tz_name)
    for dst_strategy in ("skip", "firstof", "allof"):
        for minute in (0, 30, 45, 59):
            for fold in (0, 1):
                seed = datetime(2025, 11, 2, 1, minute, tzinfo=tz).replace(fold=fold)
                cursor = seed
                for _ in range(6):
                    fires = compute_next_fire_after(
                        "30 1 * * *",
                        tz_name,
                        cursor,
                        dst_strategy=cast(Any, dst_strategy),
                    )
                    assert all(f.astimezone(UTC) > cursor.astimezone(UTC) for f in fires), (
                        f"{dst_strategy} fold={fold} minute={minute}: chain broke at "
                        f"{cursor} -> {fires}"
                    )
                    cursor = fires[-1]


# ── _is_ambiguous_time converts when tzinfo is not the target tz object ──


def test_is_ambiguous_time_converts_dt_with_different_tzinfo() -> None:
    """_is_ambiguous_time converts *dt* to *tz* first when dt.tzinfo is not
    the same object as *tz* (exercises the `dt = dt.astimezone(tz)` branch)."""
    tz = ZoneInfo("America/New_York")
    ambiguous_instant = datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)
    assert _is_ambiguous_time(ambiguous_instant, tz) is True

    unambiguous_instant = datetime(2025, 6, 15, 14, 30, 0, tzinfo=UTC)
    assert _is_ambiguous_time(unambiguous_instant, tz) is False


# ── _fold_to_utc resolves ambiguous local time by fold value ──────────


def test_fold_to_utc_resolves_by_fold_value() -> None:
    """_fold_to_utc(dt, tz, fold=0) and fold=1 resolve to UTC instants
    1 hour apart for an ambiguous local time."""
    tz = ZoneInfo("America/New_York")
    ambiguous_local = datetime(2025, 11, 2, 1, 30, 0, tzinfo=tz)
    earlier = _fold_to_utc(ambiguous_local, tz, fold=0)
    later = _fold_to_utc(ambiguous_local, tz, fold=1)
    assert earlier.astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)
    assert later.astimezone(UTC) == datetime(2025, 11, 2, 6, 30, 0, tzinfo=UTC)


# ── _check_gap advances past a DST gap, localizes naive input ─────────


def test_check_gap_advances_naive_gap_datetime_to_valid_time() -> None:
    """_check_gap localizes a naive datetime and advances minute-by-minute
    out of a DST gap until the wall-clock time round-trips through UTC."""
    tz = ZoneInfo("America/New_York")
    gap_dt = datetime(2025, 3, 9, 2, 15, 0)
    result = _check_gap(gap_dt, tz)
    assert result == datetime(2025, 3, 9, 3, 0, 0, tzinfo=tz)


def test_check_gap_returns_unchanged_when_already_valid() -> None:
    """_check_gap returns *dt* unchanged when it is not in a DST gap."""
    tz = ZoneInfo("America/New_York")
    valid_dt = datetime(2025, 6, 15, 10, 0, 0)
    result = _check_gap(valid_dt, tz)
    assert result == datetime(2025, 6, 15, 10, 0, 0, tzinfo=tz)


# ── cron() with valid expression returns CronScheduleSpec ────────────


def test_cron_valid_expression() -> None:
    """cron() with valid expression returns CronScheduleSpec with correct fields."""
    spec = cron("*/5 * * * *", "my_actor", timezone="UTC", enabled=True)
    assert isinstance(spec, CronScheduleSpec)
    assert spec.actor == "my_actor"
    assert spec.cron_expr == "*/5 * * * *"
    assert spec.timezone == "UTC"
    assert spec.enabled is True
    assert spec.payload_factory is None
    assert spec.static_payload is None
    assert spec.name == ""


# ── CronScheduleSpec name defaults to "" and identity_key to None ────


def test_cron_schedule_spec_name_defaults_empty_string() -> None:
    """CronScheduleSpec.name defaults to '' so existing (actor-only)
    schedules map to the (actor, '') uniqueness key after migration."""
    spec = CronScheduleSpec(actor="a", cron_expr="0 * * * *")
    assert spec.name == ""


def test_cron_schedule_spec_identity_key_defaults_none() -> None:
    """CronScheduleSpec.identity_key defaults to None."""
    spec = CronScheduleSpec(actor="a", cron_expr="0 * * * *")
    assert spec.identity_key is None


def test_cron_accepts_name_and_identity_key() -> None:
    """cron() accepts name and identity_key and forwards them to CronScheduleSpec."""
    from taskq.backend._protocol import IdentityKey

    spec = cron(
        "0 * * * *",
        "per_property_actor",
        name="prop-123",
        identity_key=IdentityKey("sync:entity:123"),
    )
    assert spec.name == "prop-123"
    assert spec.identity_key is not None
    assert str(spec.identity_key) == "sync:entity:123"


# ── cron() with invalid expression raises ValueError ─────────────────


def test_cron_invalid_expression() -> None:
    """cron() with invalid expression raises ValueError."""
    with pytest.raises(ValueError, match="Invalid cron expression"):
        cron("not valid", "my_actor")


# ── cron() with both payload_factory and static_payload raises ValueError ──


def test_cron_mutually_exclusive_fields() -> None:
    """cron() with both payload_factory and static_payload raises ValueError."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        cron(
            "*/5 * * * *",
            "my_actor",
            payload_factory="some.module.fn",
            static_payload={"key": "value"},
        )


# ── cron() auto-registers via register_cron ─────────────────────────


def test_cron_auto_registers() -> None:
    """cron() automatically calls register_cron."""
    spec = cron("0 * * * *", "auto_actor")
    registered = get_registered_crons()
    assert spec in registered


# ── register_cron adds to registry; get_registered_crons returns it ──


def test_register_cron_adds_to_registry() -> None:
    """register_cron() adds to registry; get_registered_crons() returns it."""
    spec = CronScheduleSpec(
        actor="test_actor",
        cron_expr="0 * * * *",
    )
    register_cron(spec)
    registered = get_registered_crons()
    assert spec in registered


def test_register_cron_duplicate_adds_again() -> None:
    """Duplicate register_cron calls add again (registry is a list)."""
    spec = CronScheduleSpec(
        actor="dup_actor",
        cron_expr="0 * * * *",
    )
    register_cron(spec)
    register_cron(spec)
    registered = get_registered_crons()
    assert registered.count(spec) == 2


def test_register_cron_invalid_expression_raises() -> None:
    """register_cron() with invalid expression raises ValueError."""
    spec = CronScheduleSpec(actor="bad", cron_expr="not valid")
    with pytest.raises(ValueError, match="Invalid cron expression"):
        register_cron(spec)


def test_register_cron_mutually_exclusive_raises() -> None:
    """register_cron() with both payload_factory and static_payload raises ValueError."""
    spec = CronScheduleSpec(
        actor="bad",
        cron_expr="0 * * * *",
        payload_factory="a.b",
        static_payload={"k": 1},
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        register_cron(spec)


# ── get_registered_crons returns snapshot copy ──────────────────────


def test_get_registered_crons_returns_copy() -> None:
    """get_registered_crons() returns a snapshot; mutating it does not affect registry."""
    spec = CronScheduleSpec(actor="snap_actor", cron_expr="0 * * * *")
    register_cron(spec)
    snapshot = get_registered_crons()
    snapshot.clear()
    assert len(get_registered_crons()) >= 1


# ── _resolve_factory succeeds and caches ─────────────────────


def test_resolve_factory_succeeds_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """_resolve_factory resolves a dotted path, returns callable, caches."""
    dotted = f"{_sample_factory.__module__}.{_sample_factory.__qualname__}"
    factory = _resolve_factory(dotted)
    assert factory is _sample_factory
    assert dotted in _factory_cache
    assert _factory_cache[dotted] is _sample_factory
    cached = _resolve_factory(dotted)
    assert cached is _sample_factory


# ── _resolve_factory failure - ModuleNotFoundError ────────────


def test_resolve_factory_module_not_found() -> None:
    """_resolve_factory raises ModuleNotFoundError for nonexistent module."""
    with pytest.raises(ModuleNotFoundError):
        _resolve_factory("nonexistent.module.fn")


# ── _resolve_factory failure - AttributeError ─────────────────


def test_resolve_factory_attribute_error() -> None:
    """_resolve_factory raises AttributeError for missing attribute."""
    with pytest.raises(AttributeError):
        _resolve_factory(f"{_sample_factory.__module__}.nonexistent_attr")


# ── _resolve_factory invalid dotted path ─────────────────────────────


def test_resolve_factory_invalid_dotted_path() -> None:
    """_resolve_factory raises ImportError for a path without a dot."""
    with pytest.raises(ImportError, match="Invalid dotted path"):
        _resolve_factory("no_dots_here")


# ── ScheduleRecord roundtrips through model_validate ─────────────────


def test_schedule_record_roundtrip() -> None:
    """ScheduleRecord roundtrips through model_validate correctly."""
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
    from taskq._ids import new_uuid

    uid = new_uuid()
    data = {
        "id": uid,
        "actor": "my_actor",
        "cron_expr": "0 3 * * *",
        "timezone": "UTC",
        "payload_factory": None,
        "enabled": True,
        "last_fired_at": None,
        "last_fire_error": None,
        "consecutive_failures": 0,
        "next_fire_at": now,
        "metadata": {},
    }
    record = ScheduleRecord.model_validate(data)
    assert record.id == uid
    assert record.actor == "my_actor"
    assert record.consecutive_failures == 0
    assert record.next_fire_at == now
    assert record.enabled is True
    roundtripped = ScheduleRecord.model_validate(record.model_dump())
    assert roundtripped == record


# ── ScheduleRecord frozen config ─────────────────────────────────────


def test_schedule_record_frozen() -> None:
    """ScheduleRecord is frozen (ConfigDict(frozen=True))."""
    from taskq._ids import new_uuid

    record = ScheduleRecord(
        id=new_uuid(),
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        payload_factory=None,
        enabled=True,
        last_fired_at=None,
        last_fire_error=None,
        consecutive_failures=0,
        next_fire_at=datetime.now(tz=UTC),
        metadata={},
    )
    with pytest.raises(ValidationError):
        record.actor = "changed"  # type: ignore[misc] # Why: deliberate mutation to verify frozen Pydantic model guard.


# ── CronScheduleSpec frozen ──────────────────────────────────────────


def test_cron_schedule_spec_frozen() -> None:
    """CronScheduleSpec is a frozen dataclass."""
    spec = CronScheduleSpec(actor="a", cron_expr="0 * * * *")
    with pytest.raises(FrozenInstanceError):
        spec.actor = "changed"  # type: ignore[misc] # Why: deliberate mutation to verify frozen dataclass guard.


# ── ScheduleHandle methods ───────────────────────────────────────────


class _StubBackend:
    """Minimal stub for ScheduleHandle method tests.

    Matches the Backend protocol signature: ``update_schedule(schedule_id, args)``.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, UUID, object]] = []

    async def update_schedule(
        self,
        schedule_id: UUID,
        args: object,
    ) -> None:
        self.calls.append(("update", schedule_id, args))

    async def delete_schedule(self, schedule_id: UUID) -> None:
        self.calls.append(("delete", schedule_id, None))


@pytest.mark.asyncio
async def test_schedule_handle_disable() -> None:
    """ScheduleHandle.disable() delegates to backend.update_schedule with ScheduleUpdateArgs(enabled=False)."""
    from taskq._ids import new_uuid
    from taskq.backend._protocol import ScheduleUpdateArgs

    stub = _StubBackend()
    uid = new_uuid()
    handle = ScheduleHandle(
        schedule_id=uid,
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=True,
        next_fire_at=datetime.now(tz=UTC),
        _backend=stub,
    )
    await handle.disable()
    assert len(stub.calls) == 1
    method, sid, args = stub.calls[0]
    assert method == "update"
    assert sid == uid
    assert isinstance(args, ScheduleUpdateArgs)
    assert args.enabled is False


@pytest.mark.asyncio
async def test_schedule_handle_enable() -> None:
    """ScheduleHandle.enable() delegates to backend.update_schedule with ScheduleUpdateArgs(enabled=True).

    The backend resets consecutive_failures and last_fire_error when enabled=True;
    the handle does not need to pass those fields explicitly.
    """
    from taskq._ids import new_uuid
    from taskq.backend._protocol import ScheduleUpdateArgs

    stub = _StubBackend()
    uid = new_uuid()
    handle = ScheduleHandle(
        schedule_id=uid,
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=False,
        next_fire_at=datetime.now(tz=UTC),
        _backend=stub,
    )
    await handle.enable()
    assert len(stub.calls) == 1
    method, sid, args = stub.calls[0]
    assert method == "update"
    assert sid == uid
    assert isinstance(args, ScheduleUpdateArgs)
    assert args.enabled is True


@pytest.mark.asyncio
async def test_schedule_handle_delete() -> None:
    """ScheduleHandle.delete() delegates to backend.delete_schedule with schedule_id."""
    from taskq._ids import new_uuid

    stub = _StubBackend()
    uid = new_uuid()
    handle = ScheduleHandle(
        schedule_id=uid,
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=True,
        next_fire_at=datetime.now(tz=UTC),
        _backend=stub,
    )
    await handle.delete()
    assert stub.calls == [("delete", uid, None)]


# ── ScheduleHandle frozen ────────────────────────────────────────────


def test_schedule_handle_frozen() -> None:
    """ScheduleHandle is frozen (frozen=True, slots=True)."""
    from taskq._ids import new_uuid

    handle = ScheduleHandle(
        schedule_id=new_uuid(),
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=True,
        next_fire_at=datetime.now(tz=UTC),
        _backend=_StubBackend(),
    )
    with pytest.raises(FrozenInstanceError):
        handle.enabled = False  # type: ignore[misc] # Why: deliberate mutation to verify frozen dataclass guard.


# ── compute_next_fire_after always returns datetime after `after` ──


_KNOWN_EXPRESSIONS = st.sampled_from(
    [
        "*/5 * * * *",
        "0 3 * * *",
        "0 12 * * 1-5",
        "30 8 1 * *",
        "*/10 */2 * * *",
        "0 0 * * *",
        "15 14 * * 1",
        "0 22 * * 1-5",
        "* * * * *",
        "0 */6 * * *",
    ]
)

_REGEX_EXPRESSIONS = st.from_regex(r"[1-5][0-9] [0-9] \* \* \*", fullmatch=True)

_CRON_STRATEGY = _KNOWN_EXPRESSIONS | _REGEX_EXPRESSIONS


@given(expr=_CRON_STRATEGY)
@settings(max_examples=50, deadline=None)
def testcompute_next_fire_after_always_after_now(expr: str) -> None:
    """compute_next_fire_after returns datetimes strictly after `after`
    for all valid 5-field cron expressions, without raising."""
    assume(croniter.is_valid(expr))
    after = datetime.now(UTC)
    result = compute_next_fire_after(expr, "UTC", after)
    assert all(r > after for r in result)


# ── resolve_payload: static payload from metadata ─────────────────────


@pytest.mark.asyncio
async def testresolve_payload_static_from_metadata() -> None:
    """resolve_payload returns static_payload from metadata when no factory."""
    result = await resolve_payload(None, {"static_payload": {"key": "val"}})
    assert result == {"key": "val"}


@pytest.mark.asyncio
async def testresolve_payload_static_from_json_string_metadata() -> None:
    """resolve_payload parses raw_metadata when it is a truthy non-dict (JSON string)."""
    result = await resolve_payload(None, '{"static_payload": {"key": "val"}}')
    assert result == {"key": "val"}


@pytest.mark.asyncio
async def testresolve_payload_empty_dict_without_factory_or_static() -> None:
    """resolve_payload returns {} when no factory and no static_payload."""
    result = await resolve_payload(None, {})
    assert result == {}


@pytest.mark.asyncio
async def testresolve_payload_none_metadata() -> None:
    """resolve_payload returns {} when metadata is None."""
    result = await resolve_payload(None, None)
    assert result == {}


@pytest.mark.asyncio
async def testresolve_payload_factory_returns_dict() -> None:
    """resolve_payload resolves a factory that returns a dict."""
    dotted = f"{_sample_factory.__module__}.{_sample_factory.__qualname__}"
    result = await resolve_payload(dotted, {})
    assert result == {"x": 1}


@pytest.mark.asyncio
async def testresolve_payload_factory_returns_basemodel() -> None:
    """resolve_payload resolves a factory that returns a BaseModel and converts via model_dump."""
    dotted = f"{_sample_model_factory.__module__}.{_sample_model_factory.__qualname__}"
    result = await resolve_payload(dotted, {})
    assert result == {"name": "test", "count": 3}


@pytest.mark.asyncio
async def testresolve_payload_factory_returns_async() -> None:
    """resolve_payload awaits async factories."""
    dotted = f"{_async_factory.__module__}.{_async_factory.__qualname__}"
    result = await resolve_payload(dotted, {})
    assert result == {"async": "yes"}


@pytest.mark.asyncio
async def testresolve_payload_factory_unexpected_type_raises_typeerror() -> None:
    """resolve_payload raises TypeError when factory returns unexpected type."""
    dotted = f"{_bad_factory.__module__}.{_bad_factory.__qualname__}"
    with pytest.raises(TypeError, match="cron factory"):
        await resolve_payload(dotted, {})


@pytest.mark.asyncio
async def testresolve_payload_factory_import_error_propagates() -> None:
    """resolve_payload propagates ImportError from _resolve_factory."""
    with pytest.raises(ImportError):
        await resolve_payload("nonexistent.module.fn", {})


# ── Cron payload factories are isolated from the actor thread pool ─────
#
# Cron payload resolution must not depend on actor-pool availability to
# make progress. A sync factory runs off the event loop so a blocking
# factory cannot stall the loop, but the executor it runs on must not be
# the loop's default ThreadPoolExecutor - that is the same pool sync
# actor bodies check out of. Sharing it means a fleet of busy sync actors
# holding every thread stalls schedule ticks: even an instantaneous
# `def f(): return {}` cannot start until a slot frees, and the bounded
# resolution can time out while the tick holds the cron advisory lock.
# Isolation also bounds leaked threads when a factory hangs.


def _trivial_sync_factory() -> dict[str, object]:
    """Instantaneous sync factory - the case that must never wait on
    actor-pool contention at all."""
    return {}


@pytest.mark.asyncio
async def testresolve_payload_trivial_sync_factory_survives_a_saturated_actor_pool() -> None:
    """A trivial sync payload factory must resolve promptly even when every
    slot of the loop's default thread pool is held by long-running sync
    actor work.

    Cron payload resolution runs a sync factory off the event loop to keep
    a blocking factory from stalling the loop, but it must do so on an
    executor of its own. Submitting to the default pool - the one sync
    actor bodies use - makes a schedule tick's progress depend on actor
    load: an instantaneous factory queues behind saturating work and its
    bounded resolution times out while the tick holds the cron lock."""
    import concurrent.futures
    import threading

    loop = asyncio.get_running_loop()
    # Discover (and pin) the loop's default executor exactly as
    # asyncio.to_thread would create/reuse it, so max_workers is the real
    # configured size, not a guess.
    probe_started = threading.Event()
    probe_release = threading.Event()

    def _probe() -> None:
        probe_started.set()
        probe_release.wait(5.0)

    probe_future = loop.run_in_executor(None, _probe)
    assert probe_started.wait(5.0), "the probe task never started on the default executor"
    executor = loop._default_executor  # type: ignore[attr-defined]  # Why: no public accessor; this is exactly what to_thread submits to.
    assert isinstance(executor, concurrent.futures.ThreadPoolExecutor)
    max_workers = executor._max_workers  # type: ignore[attr-defined]  # Why: no public accessor for the configured pool size.
    probe_release.set()
    await asyncio.wrap_future(probe_future)

    # Saturate every worker slot with long-blocking work - standing in for
    # sync actor bodies holding the pool.
    saturators_started = [threading.Event() for _ in range(max_workers)]
    saturators_release = threading.Event()

    def _saturate(started: threading.Event) -> None:
        started.set()
        saturators_release.wait(10.0)

    saturator_futures = [
        loop.run_in_executor(None, _saturate, started) for started in saturators_started
    ]
    try:
        for started in saturators_started:
            assert started.wait(5.0), (
                "not every saturating task reached the pool - miscounted max_workers"
            )

        # Every slot is now held by simulated actor work. A cron system
        # that does not share the actor pool (or that calls a plain sync
        # factory inline) resolves a trivial factory immediately, well
        # inside a generous 3s timeout, regardless of pool saturation.
        result = await resolve_payload(
            f"{_trivial_sync_factory.__module__}.{_trivial_sync_factory.__qualname__}",
            {},
            timeout_s=3.0,
        )
        assert result == {}, (
            "resolve_payload did not return the trivial factory's payload - "
            "unexpected failure shape, not the starvation this test targets"
        )
    finally:
        saturators_release.set()
        await asyncio.gather(
            *(asyncio.wrap_future(f) for f in saturator_futures), return_exceptions=True
        )


# ── compute_next_fire_after: a seed inside a repeated (fall-back) hour ──
#
# croniter walks naive wall-clock time, so a seed sitting exactly ON a
# match inside a DST overlap is invisible to the strictly-greater walk:
# the same wall-clock's later occurrence is a real next match, but naive
# comparison cannot distinguish the two and the walk jumps a year.  The
# strategies split on what that means: ``skip``/``firstof`` fire a
# repeated hour once, at the earlier occurrence - which the seed already
# is - so the next slot is genuinely next year; ``allof`` owes the later
# occurrence, so the fold-1 twin is the next fire.  Reachable in
# production whenever a leader outage or manual edit leaves
# ``next_fire_at`` ON the fold-0 occurrence: the post-outage tick fires
# the earlier occurrence and, without this, the later one is silently
# lost for a year.

_OVERLAP_TZ = "America/New_York"
_OVERLAP_EXPR = "30 1 1 11 *"  # 01:30 local every Nov 1; 2026-11-01 falls back
_FOLD0_UTC = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)  # 01:30 EDT (first 01:30)
_FOLD1_UTC = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)  # 01:30 EST (second 01:30)
_NEXT_SLOT_UTC = datetime(2027, 11, 1, 5, 30, tzinfo=UTC)


def test_allof_seed_on_first_occurrence_returns_the_second() -> None:
    """The fold-0 occurrence just fired (it is the seed); ``allof`` still
    owes the fold-1 twin - a strictly later instant - so the next fire
    is the twin, not next year.

    Compared as normalized instants: the function's contract preserves
    the schedule's timezone, and a zone-aware datetime does not compare
    equal to a UTC one at the same instant in this runtime."""
    out = compute_next_fire_after(_OVERLAP_EXPR, _OVERLAP_TZ, _FOLD0_UTC, dst_strategy="allof")
    assert [d.astimezone(UTC) for d in out] == [_FOLD1_UTC], (
        f"a seed on the first occurrence of a repeated hour must return the "
        f"second occurrence ({_FOLD1_UTC}) under allof, got {out}"
    )


def test_allof_seed_on_second_occurrence_advances_to_next_slot() -> None:
    """From the fold-1 occurrence both members of the pair are consumed or
    past; the next fire is the next slot, which croniter finds correctly."""
    out = compute_next_fire_after(_OVERLAP_EXPR, _OVERLAP_TZ, _FOLD1_UTC, dst_strategy="allof")
    assert [d.astimezone(UTC) for d in out] == [_NEXT_SLOT_UTC]


@pytest.mark.parametrize("strategy", ["skip", "firstof"])
def test_single_fire_strategies_advance_one_slot_from_inside_the_overlap(
    strategy: str,
) -> None:
    """``skip``/``firstof`` fire a repeated hour once, at the earlier
    occurrence - the seed - so nothing is owed and the next slot is next
    year, from either member of the pair."""
    for seed in (_FOLD0_UTC, _FOLD1_UTC):
        out = compute_next_fire_after(_OVERLAP_EXPR, _OVERLAP_TZ, seed, dst_strategy=strategy)
        assert [d.astimezone(UTC) for d in out] == [_NEXT_SLOT_UTC], (
            f"{strategy} from {seed}: got {out}"
        )


def test_allof_seed_inside_overlap_on_a_non_matching_wall_is_untouched() -> None:
    """A seed inside the repeated hour whose wall-clock is NOT a match
    takes the normal walk - the fold handling only owns the seed's own
    matched wall-clock."""
    seed = datetime(2026, 11, 1, 5, 10, tzinfo=UTC)  # 01:10 EDT, inside the hour
    out = compute_next_fire_after(_OVERLAP_EXPR, _OVERLAP_TZ, seed, dst_strategy="allof")
    assert [d.astimezone(UTC) for d in out] == [_FOLD0_UTC, _FOLD1_UTC], (
        "a non-matching seed inside the fold must still see both occurrences"
    )


async def _hung_factory() -> dict[str, object]:
    """Outlives the payload-factory timeout: the wait_for cancels the
    sleep long before the return is reached."""
    await asyncio.sleep(30)
    return {}


@pytest.mark.asyncio
async def testresolve_payload_timeout_names_the_factory() -> None:
    """A hung async factory's TimeoutError must name the dotted factory
    path - the schedule's error text is the only place an operator sees
    WHICH factory hung."""
    dotted = f"{_hung_factory.__module__}.{_hung_factory.__qualname__}"
    with pytest.raises(TimeoutError, match="timed out after 5s") as exc_info:
        await resolve_payload(dotted, {})
    assert dotted in str(exc_info.value)


# ── resolve_payload: a coroutine-function factory needs only the loop ──
#
# Running a factory off the event loop exists to protect the loop from a
# SYNC factory that blocks. A coroutine function needs no such protection:
# calling it does not run the body, it only constructs a coroutine object,
# and that construction never blocks. Routing that call through the
# default thread pool buys nothing and couples cron payload resolution to
# a pool sync actor bodies also check out of, so a busy fleet of sync
# actors can stall a cron tick that has no blocking work at all.


async def _immediate_async_factory() -> dict[str, object]:
    """A coroutine-function factory that awaits nothing and returns at once."""
    return {"ok": True}


@pytest.mark.asyncio
async def testresolve_payload_async_factory_resolves_off_the_shared_thread_pool() -> None:
    """A coroutine-function payload factory must resolve promptly even when
    every worker of the loop's default thread pool is checked out.

    Calling a coroutine function only constructs a coroutine object, so
    resolution needs the event loop and nothing else. Submitting that call
    to the default executor makes it queue behind whatever holds the pool
    -- sync actor bodies run there too -- and a trivially fast cron
    payload can then miss its per-factory deadline while the tick holds
    the cron advisory lock.
    """
    import time

    loop = asyncio.get_running_loop()
    # The default executor is created lazily on first use; touch it, then
    # read its real configured size. Saturating EXACTLY that many workers
    # matters: extra submissions would queue behind the blockers and
    # deadlock this test's own setup, since none of them release early.
    probe = await loop.run_in_executor(None, lambda: None)
    del probe
    executor = loop._default_executor  # type: ignore[attr-defined]  # Why: no public accessor; this is exactly what to_thread submits to.
    n_blockers = executor._max_workers  # type: ignore[attr-defined]  # Why: no public accessor for the configured pool size.

    release = asyncio.Event()
    started = asyncio.Event()
    starts_remaining = n_blockers

    def _block_until_released() -> None:
        nonlocal starts_remaining
        starts_remaining -= 1
        if starts_remaining == 0:
            loop.call_soon_threadsafe(started.set)
        while not release.is_set():
            time.sleep(0.01)

    blocker_futures = [loop.run_in_executor(None, _block_until_released) for _ in range(n_blockers)]
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)

        dotted = f"{_immediate_async_factory.__module__}.{_immediate_async_factory.__qualname__}"
        t0 = time.monotonic()
        result = await asyncio.wait_for(resolve_payload(dotted, {}), timeout=2.0)
        elapsed = time.monotonic() - t0

        assert result == {"ok": True}
        assert elapsed < 1.0, (
            f"resolving a coroutine-function factory took {elapsed:.2f}s with the "
            "default thread pool saturated -- the factory call is being submitted "
            "to that pool instead of being made directly on the event loop"
        )
    finally:
        release.set()
        for fut in blocker_futures:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(fut, timeout=5.0)


# ── A hung factory strands threads outside the actor pool ──────────────
#
# A sync factory that never returns is cut off at the per-factory
# deadline, but the thread already running it cannot be interrupted: it
# stays parked inside the factory. A schedule whose factory hangs every
# fire strands one such thread per tick, and cron ticks forever. Those
# stranded threads must come from cron's own bounded pool, never from the
# pool sync actor bodies check out of - otherwise one permanently hung
# schedule silently eats the fleet's actor execution capacity, a
# slow-motion outage no cron metric reports.


_HUNG_FACTORY_RELEASE = threading.Event()
"""Release flag for :func:`_hung_sync_factory` - module scope because the
factory is resolved by dotted path, so the test can only reach it here."""


def _hung_sync_factory() -> dict[str, object]:
    """Sync factory that blocks until the test releases it, recording the
    thread it was parked on so the test can identify that thread's pool."""
    _HUNG_PARKED_THREADS.append(threading.current_thread())
    _HUNG_FACTORY_RELEASE.wait(60.0)
    return {}


_HUNG_PARKED_THREADS: list[threading.Thread] = []
"""Threads :func:`_hung_sync_factory` was parked on, in call order."""


@pytest.mark.asyncio
async def testresolve_payload_hung_factories_never_strand_actor_pool_threads() -> None:
    """Threads stranded by hung payload factories must not be the loop's
    default-executor threads.

    The per-factory deadline bounds how long a tick waits, not how long the
    factory runs: each cut-off call leaves its thread parked inside the
    factory until the factory itself returns, which a truly hung one never
    does. Cron ticks indefinitely, so one permanently hung schedule strands
    a thread per tick. If those calls run on the loop's default thread pool
    - the one sync actor bodies run on - the hung schedule steadily
    consumes the worker's own execution capacity until sync actors have no
    thread left, with nothing in the cron telemetry to explain it. Cron
    must strand only threads from a pool of its own.
    """
    _HUNG_FACTORY_RELEASE.clear()
    _HUNG_PARKED_THREADS.clear()
    loop = asyncio.get_running_loop()

    # Materialize the loop's default executor and note its threads, so a
    # parked factory thread can be attributed to that pool or not. It is
    # created lazily, exactly as asyncio.to_thread would create it.
    await loop.run_in_executor(None, lambda: None)
    default_executor = loop._default_executor  # type: ignore[attr-defined]  # Why: no public accessor; this is exactly what to_thread submits to.

    dotted = f"{_hung_sync_factory.__module__}.{_hung_sync_factory.__qualname__}"
    try:
        for _ in range(3):
            with pytest.raises(TimeoutError):
                await resolve_payload(dotted, {}, timeout_s=0.05)

        assert len(_HUNG_PARKED_THREADS) == 3, (
            "vacuous run: the factory never reached a thread, so nothing here "
            f"says which pool it stranded; saw {_HUNG_PARKED_THREADS}"
        )
        default_threads = set(default_executor._threads)  # type: ignore[attr-defined]  # Why: no public accessor for a pool's worker threads.
        stranded_from_actor_pool = [t for t in _HUNG_PARKED_THREADS if t in default_threads]
        assert stranded_from_actor_pool == [], (
            f"{len(stranded_from_actor_pool)} of 3 hung payload factories are "
            "parked on the loop's default executor - the same pool sync actor "
            "bodies run on. Each hung fire permanently removes one thread from "
            "actor execution capacity, so a single stuck schedule degrades "
            "unrelated work until the pool is exhausted"
        )
    finally:
        _HUNG_FACTORY_RELEASE.set()


@pytest.mark.asyncio
async def testresolve_payload_healthy_factory_survives_a_saturated_cron_pool() -> None:
    """A pool whose every thread is stranded on abandoned work must be
    retired, not queue new work behind it forever.

    The cron-dedicated factory pool is finite (_FACTORY_POOL_SIZE) and a
    stdlib executor cannot recall a parked thread, so that many hung sync
    factories strand the whole pool. Without recovery a healthy factory
    then queues behind the residue and misses every deadline for the rest
    of the process's life - cron's OWN contention against itself, one
    level in from where the actor-pool isolation fixed it. The contract:
    the submit path detects the fully-stranded pool, retires it loudly
    (one WARN, naming the residue), and the healthy factory resolves on a
    fresh pool. The parked threads are NOT reclaimed - they finish when
    their factories return, which the test's release forces so nothing
    outlives it.
    """
    import structlog.testing

    from taskq.cron import (
        _FACTORY_POOL_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: the saturation level under test IS the pool's own cap.
    )

    _HUNG_FACTORY_RELEASE.clear()
    _HUNG_PARKED_THREADS.clear()
    dotted = f"{_hung_sync_factory.__module__}.{_hung_sync_factory.__qualname__}"
    try:
        # Saturate the dedicated cron factory pool with _FACTORY_POOL_SIZE
        # hung calls -- each one permanently strands its worker thread.
        pool_size = _FACTORY_POOL_SIZE
        for _ in range(pool_size):
            with pytest.raises(TimeoutError):
                await resolve_payload(dotted, {}, timeout_s=0.05)
        assert len(_HUNG_PARKED_THREADS) == pool_size

        # A completely healthy, instantaneous factory must now resolve -
        # the saturated pool was retired instead of queueing this call
        # behind residue it could never pass.
        with structlog.testing.capture_logs() as captured:
            result = await asyncio.wait_for(
                resolve_payload(
                    f"{_trivial_sync_factory.__module__}.{_trivial_sync_factory.__qualname__}",
                    {},
                    timeout_s=5.0,
                ),
                timeout=5.0,
            )
        assert result == {}

        retired = [e for e in captured if e["event"] == "cron-factory-pool-saturated-retired"]
        assert len(retired) == 1, (
            f"expected exactly one pool-retirement WARN, got {len(retired)} - "
            "retirement must be loud (it is the only place an operator learns "
            "hung schedules stranded a pool) and must not spam per submit"
        )
        assert retired[0]["log_level"] == "warning"
        assert retired[0]["abandoned"] == pool_size
    finally:
        _HUNG_FACTORY_RELEASE.set()


def _slow_sync_factory() -> dict[str, object]:
    """Slow-but-alive sync factory: occupies a pool thread briefly, then
    returns - the live-work contrast to :func:`_hung_sync_factory`."""
    time.sleep(0.3)
    return {"slow": True}


@pytest.mark.asyncio
async def testresolve_payload_busy_but_alive_cron_pool_is_not_retired() -> None:
    """Retirement is for ABANDONED residue, not busyness: a pool whose
    threads are all occupied by slow-but-alive factories their waiters
    still await must serve a queued call as soon as a thread frees, and
    must not be retired out from under live work.
    """
    import structlog.testing

    from taskq.cron import (
        _FACTORY_POOL_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: the occupancy level under test IS the pool's own cap.
    )

    pool_size = _FACTORY_POOL_SIZE
    dotted_slow = f"{_slow_sync_factory.__module__}.{_slow_sync_factory.__qualname__}"
    dotted_trivial = f"{_trivial_sync_factory.__module__}.{_trivial_sync_factory.__qualname__}"
    with structlog.testing.capture_logs() as captured:
        # Occupy every thread with live, waited-on work...
        slow = [
            asyncio.create_task(resolve_payload(dotted_slow, {}, timeout_s=5.0))
            for _ in range(pool_size)
        ]
        # ...then queue a healthy call behind them: it resolves as soon as
        # the first slow call frees its thread.
        healthy = asyncio.create_task(resolve_payload(dotted_trivial, {}, timeout_s=5.0))
        results = await asyncio.wait_for(asyncio.gather(*slow, healthy), timeout=10.0)
    assert results[:pool_size] == [{"slow": True}] * pool_size
    assert results[pool_size] == {}
    assert not [e for e in captured if e["event"] == "cron-factory-pool-saturated-retired"], (
        "a busy-but-alive pool was retired - retirement is only for pools "
        "whose every thread is stranded on work nobody waits for"
    )
