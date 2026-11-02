"""Adversarial review of the DST fall-back discovery the parity drives date
themselves from (``test_rt_cron_harness.fallback_transitions`` /
``FallbackTransition``).

The discovery exists so a drive's premise ("this zone falls back at local
midnight, its range crosses the date boundary, its repeat is half an hour")
is checked against the *installed* zone database instead of a hardcoded
calendar date.  Its output is the drives' seed, tick and bound arithmetic, so
an output that is wrong-but-plausible is worse than a hardcoded date: the
drive stays green on a shape the zone does not have.

Two contract defects are pinned red here:

1.  ``FallbackTransition.range_start`` is documented as a "naive dated wall",
    but it is returned carrying ``fold=1`` (it is the transition instant's own
    wall, and ``repeated_range_bounds`` copies ``after_local``'s fold into it).
    ``range_end`` carries ``fold=0``.  ``str()`` renders both identically and
    ``datetime`` equality ignores ``fold``, so the difference is invisible; the
    natural conversion of a naive wall - ``range_start.replace(tzinfo=tz)`` -
    silently resolves to the *fold-1* (later, post-transition) instant.  A
    drive that seeds a schedule from that wall starts on the wrong pass of the
    fold.  ``fold_instant`` exists, but the exported wall must not smuggle a
    second meaning the documented "naive wall" reading silently resolves to.

2.  ``fallback_transitions`` is ``functools.cache``d and returns the cached
    *list object*, so every caller shares one mutable value.  A consumer that
    reorders, trims or extends the result (``.sort()``/``.reverse()``/
    ``.extend()`` for a property drive is the ordinary way to select a
    transition) silently changes what every later caller - including the
    parity drives - dates itself from.  The drives then stay green on a
    different transition than the one they were written for.

Every other hypothesis area is searched by the green pins below: the range
characterization against an independent ambiguity oracle (half-hour repeat,
45-minute offset, midnight-starting, midnight-ending, month/year-crossing,
leap-day, two-hour repeat), the half-open discovery window at its edges, the
loud failure for a zone with no fall-back, TZ independence, and a fold-1 seed
at an interior slot (the added midnight pin seeds only the range's first
fold-1 slot).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import asyncpg
import pytest

from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import ActorFirePolicy

from .test_rt_cron_harness import (
    FallbackTransition,
    count_jobs,
    cron_settings,
    fallback_transitions,
    schedule_row,
    seed_actor_config,
    seed_schedule,
)
from .test_rt_cron_parity_dst import (
    _MINUTELY,
    _OPEN_ACTOR,
    TestFoldParityBeyondNewYork,
    _first_midnight_ending_fallback,
    _first_midnight_starting_fallback,
    _first_non_hour_fallback,
    _first_same_day_fallback,
    _seed_scheduled_twin,
    _tick,
)

_HALF_HOUR_ZONE = "Australia/Lord_Howe"
_QUARTER_OFFSET_ZONE = "Pacific/Chatham"
_TWO_HOUR_ZONE = "Antarctica/Troll"
_MONTH_CROSSING_ZONE = "Africa/Cairo"
_LEAP_DAY_ZONE = "Asia/Almaty"


def _ambiguous(wall: datetime, tz: ZoneInfo) -> bool:
    """Whether *wall* reads twice in *tz* - the independent PEP 495 oracle,
    deliberately not ``taskq.cron``'s own check."""
    return wall.replace(tzinfo=tz, fold=0).astimezone(UTC) != wall.replace(
        tzinfo=tz, fold=1
    ).astimezone(UTC)


def _offset_at(instant: datetime, tz: ZoneInfo) -> timedelta:
    offset = instant.astimezone(tz).utcoffset()
    assert offset is not None
    return offset


def _first_transition_matching(
    zone: str, predicate: Callable[[FallbackTransition], bool]
) -> FallbackTransition:
    matches = [
        transition for transition in fallback_transitions(zone, 2018, 2038) if predicate(transition)
    ]
    assert matches, f"{zone}: no fall-back in [2018, 2038) matches the shape"
    return matches[0]


def _first_two_hour_fallback(zone: str) -> FallbackTransition:
    return _first_transition_matching(zone, lambda transition: transition.width_minutes >= 120)


def _first_month_crossing_fallback(zone: str) -> FallbackTransition:
    return _first_transition_matching(
        zone,
        lambda transition: (
            (transition.range_start.year, transition.range_start.month)
            != (transition.range_end.year, transition.range_end.month)
        ),
    )


def _first_leap_day_fallback(zone: str) -> FallbackTransition:
    return _first_transition_matching(
        zone,
        lambda transition: transition.range_start.month == 2 and transition.range_start.day == 29,
    )


def test_a_discovered_wall_is_a_plain_wall_not_a_latent_fold() -> None:
    """``range_start`` is documented as a naive dated wall, so the obvious
    ``replace(tzinfo=zone)`` must resolve to the pre-transition (fold-0)
    occurrence.  Today it carries ``fold=1`` and silently resolves to the
    later occurrence; ``range_end`` carries ``fold=0``."""
    fold = _first_midnight_starting_fallback("America/Havana")
    tz = ZoneInfo(fold.zone)
    assert fold.range_start.fold == 0, (
        f"{fold.zone}: range_start is documented as a naive wall but carries "
        f"fold={fold.range_start.fold}; the same wall text resolves to "
        f"{fold.range_start.replace(tzinfo=tz).astimezone(UTC).isoformat()} "
        "under the obvious conversion instead of the fold-0 "
        f"{fold.range_start_fold0_utc.isoformat()}"
    )
    assert fold.range_end.fold == 0, f"{fold.zone}: range_end carries fold={fold.range_end.fold}"
    assert fold.range_start.replace(tzinfo=tz) == fold.range_start_fold0_utc, (
        f"{fold.zone}: wall {fold.range_start!s} must resolve to the "
        "pre-transition occurrence the dataclass documents"
    )


def test_the_discovery_result_is_not_shared_mutable_state_across_callers() -> None:
    """The cached discovery must not hand every caller one mutable list: a
    consumer ordering or trimming the result for its own drive silently
    re-dates every later caller (including the parity drives)."""
    cached = fallback_transitions("America/Havana", 2018, 2038)
    earliest = cached[0]
    # An immutable result has no in-place ordering to leak and a copy has none
    # either; only the shared cached list can be re-dated through one caller's
    # reference.  Resolved through getattr so the pin stays honest after either
    # cure (tuple return or per-call copy).
    sort_in_place = getattr(cached, "sort", None)
    if sort_in_place is not None:
        sort_in_place(key=lambda transition: transition.transition_utc, reverse=True)
    try:
        later = fallback_transitions("America/Havana", 2018, 2038)
        assert later[0] == earliest, (
            "a caller that reordered the returned result changed the earliest "
            f"transition every later caller sees: {earliest.range_start.date()} "
            f"-> {later[0].range_start.date()}"
        )
    finally:
        if sort_in_place is not None:
            sort_in_place(key=lambda transition: transition.transition_utc)


@pytest.mark.parametrize(
    "zone",
    [
        "America/Havana",
        "America/Santiago",
        "America/New_York",
        _HALF_HOUR_ZONE,
        _QUARTER_OFFSET_ZONE,
        _TWO_HOUR_ZONE,
        _MONTH_CROSSING_ZONE,
        _LEAP_DAY_ZONE,
    ],
)
def test_every_discovered_range_agrees_with_the_zone_ambiguity_oracle(zone: str) -> None:
    """Each characterized range must be exactly the ambiguous wall run: the
    start ambiguous, the minute before it not, the last minute ambiguous and
    the end not - whatever the repeat width or the boundary it crosses."""
    tz = ZoneInfo(zone)
    transitions = fallback_transitions(zone, 2018, 2038)
    assert transitions, f"{zone}: expected at least one fall-back in the window"
    for transition in transitions:
        assert _ambiguous(transition.range_start, tz), (
            f"{zone}: {transition.range_start} is not ambiguous though it is "
            "the characterized range start"
        )
        assert not _ambiguous(transition.range_start - timedelta(minutes=1), tz), (
            f"{zone}: the minute before {transition.range_start} is ambiguous - "
            "the walk-back stopped one minute late"
        )
        assert _ambiguous(transition.range_end - timedelta(minutes=1), tz), (
            f"{zone}: the last range wall {transition.range_end} - 1min is not "
            "ambiguous - the walk-forward stopped one minute early"
        )
        assert not _ambiguous(transition.range_end, tz), (
            f"{zone}: the range end {transition.range_end} is ambiguous - the "
            "characterization absorbed a wall past the repeated run"
        )
        assert transition.range_end > transition.range_start
        assert transition.range_start_fold1_utc == transition.transition_utc, (
            f"{zone}: the fold-1 start must be the transition instant itself"
        )
        assert transition.range_end_utc - transition.range_start_fold1_utc == timedelta(
            minutes=transition.width_minutes
        ), f"{zone}: the range end must sit one repeat-width past the fold-1 start"
        offset_before = _offset_at(transition.transition_utc - timedelta(minutes=1), tz)
        offset_after = _offset_at(transition.transition_utc, tz)
        assert offset_before - offset_after == timedelta(minutes=transition.width_minutes), (
            f"{zone}: the characterized width {transition.width_minutes}m differs "
            f"from the transition's offset drop {offset_before - offset_after}"
        )


def test_the_discovery_window_is_half_open_at_both_ends() -> None:
    """A transition on the window's last year must be found; the year after
    must not, and an empty window must answer empty rather than scan past it."""
    last_of_window = fallback_transitions("America/New_York", 2018, 2019)
    assert [transition.range_start.year for transition in last_of_window] == [2018], (
        "the transition inside [2018, 2019) must be discovered"
    )
    assert fallback_transitions("America/New_York", 2018, 2018) == [], (
        "an empty half-open window must not discover anything"
    )
    assert fallback_transitions("America/New_York", 2019, 2019) == []


def test_a_zone_without_a_fall_back_fails_the_drive_premise_loudly() -> None:
    """A zone (or window) with no fall-back must not look like a green drive:
    the discovery answers empty and the drive's selector refuses to invent a
    premise."""
    assert fallback_transitions("Asia/Kolkata", 2018, 2038) == []
    assert fallback_transitions("UTC", 2018, 2038) == []
    for selector in (
        _first_midnight_starting_fallback,
        _first_midnight_ending_fallback,
        _first_same_day_fallback,
        _first_non_hour_fallback,
    ):
        with pytest.raises(AssertionError, match="premise is false"):
            selector("Asia/Kolkata")


def test_discovery_does_not_read_the_runners_tz_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same zone and window must answer the same instants whatever ``TZ``
    the runner carries.  The second call uses a different window so the
    process cache cannot hide a TZ-dependent read."""
    baseline = [
        transition.transition_utc
        for transition in fallback_transitions("America/Havana", 2018, 2038)
    ]
    monkeypatch.setenv("TZ", "Pacific/Kiritimati")
    time.tzset()
    try:
        shifted = [
            transition.transition_utc
            for transition in fallback_transitions("America/Havana", 2017, 2038)
            if transition.range_start.year >= 2018
        ]
    finally:
        monkeypatch.undo()
        time.tzset()
    assert shifted == baseline, (
        "the discovered instants changed with TZ set - the drives would date "
        "themselves from the runner's environment"
    )


@pytest.mark.integration
async def test_fold1_seed_at_an_interior_slot_advances_through_the_remainder(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The added midnight pin seeds the range's *first* fold-1 slot; an
    operator edit or an outage recompute can leave ``next_fire_at`` at an
    interior fold-1 slot.  The uncovered remainder must fire once each and the
    twin-covered tail must not re-fire."""
    fold = _first_midnight_starting_fallback("America/Havana")
    seed_offset = fold.width_minutes // 2
    covered_from = seed_offset + 5
    seed_utc = fold.range_start_fold1_utc + timedelta(minutes=seed_offset)
    schema = module_pg_schema.schema_name
    settings = cron_settings(schema)
    await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
    schedule_id = await seed_schedule(
        clean_pg_conn,
        schema,
        actor=_OPEN_ACTOR,
        name="attack-havana-interior-fold1-seed",
        cron_expr=_MINUTELY,
        timezone=fold.zone,
        dst_strategy="allof",
        next_fire_at=seed_utc,
        identity_key="attack-havana-interior-fold1-seed",
    )
    for offset in range(covered_from, fold.width_minutes):
        await _seed_scheduled_twin(
            clean_pg_conn,
            schema,
            _OPEN_ACTOR,
            fold.range_start_fold1_utc + timedelta(minutes=offset),
            schedule_id,
        )
    no_policies: Mapping[str, ActorFirePolicy] = {}
    fired_total = 0
    for offset in range(seed_offset, fold.width_minutes):
        due = fold.range_start_fold1_utc + timedelta(minutes=offset)
        fired = await _tick(clean_pg_conn, settings, schema, no_policies, due_as_of=due)
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] > due, (
            f"{fold.zone}: after the {due.isoformat()} tick the schedule sits "
            f"due in the past (next_fire_at={row['next_fire_at']})"
        )
        if offset < covered_from:
            assert fired == 1, (
                f"{fold.zone}: the interior uncovered fold-1 slot {due.isoformat()} "
                "was silently skipped"
            )
            fired_total += 1
        else:
            assert fired == 0, (
                f"{fold.zone}: the twin-covered fold-1 slot {due.isoformat()} re-fired"
            )
    row = await schedule_row(clean_pg_conn, schema, schedule_id)
    assert row["next_fire_at"] == fold.range_end_utc, (
        f"{fold.zone}: the interior seed's remainder is spent at the range end "
        f"({fold.range_end_utc.isoformat()}), got {row['next_fire_at'].isoformat()}"
    )
    assert fired_total == covered_from - seed_offset
    assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == (
        fold.width_minutes - seed_offset
    ), "every fold-1 occurrence from the interior seed gets exactly one job"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("zone", "selector"),
    [
        ("America/Havana", _first_midnight_starting_fallback),
        ("America/Santiago", _first_midnight_ending_fallback),
        ("Australia/Sydney", _first_same_day_fallback),
        ("Australia/Lord_Howe", _first_non_hour_fallback),
        ("America/New_York", _first_same_day_fallback),
        ("Antarctica/Troll", _first_two_hour_fallback),
        ("Africa/Cairo", _first_month_crossing_fallback),
        ("Asia/Almaty", _first_leap_day_fallback),
    ],
)
async def test_the_steady_state_drive_delivers_every_fold0_slot(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    zone: str,
    selector: Callable[[str], FallbackTransition],
) -> None:
    """The drive's own job count is ``fired_total + fold1_slots``, so a fold-0
    slot the walk silently skipped lowers both sides together and the drive
    stays green.  The absolute delivery count is not self-referential: the
    two pre-fold slots, every fold-0 slot, and every fold-1 twin."""
    fold = selector(zone)
    await TestFoldParityBeyondNewYork()._drive_steady_state_fold(
        clean_pg_conn,
        module_pg_schema,
        zone=fold.zone,
        identity=f"attack-steady-{zone}",
        seed_utc=fold.range_start_fold0_utc - timedelta(minutes=2),
        last_fold0_tick_utc=fold.last_fold0_slot_utc,
        fold1_first_utc=fold.range_start_fold1_utc,
        fold1_slots=fold.width_minutes,
        after_range_utc=fold.range_end_utc,
    )
    schema = module_pg_schema.schema_name
    delivered = await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR)
    assert delivered == 2 * fold.width_minutes + 2, (
        f"{fold.zone}: {fold.width_minutes + 2} fold-0 fires + "
        f"{fold.width_minutes} fold-1 twins expected, got {delivered} - a "
        "skipped fold-0 slot is invisible to the drive's self-referential count"
    )
