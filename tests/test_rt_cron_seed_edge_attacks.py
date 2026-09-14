"""Red-team attacks on the two newest `taskq.cron` branches.

Both target code added after the bounded-sweep fix, and both were
verified red by direct run before being written here — neither is
covered by the existing pins (`tests/test_cron.py`'s DST section pins
exact-minute seeds and the empty-message hang; `test_rt_cron_parity_dst.py`
pins the overlap walk, not these edges):

1.  A seed *within* a matching ambiguous minute — but not exactly on
    the match — under `dst_strategy="allof"`. The seed check then in
    `cron.py` (`_seed_is_first_occurrence_of_match`, since reworked
    into `_next_fold1_fire`) truncated the
    seed to the minute before comparing, so "01:30:45 inside the
    repeated hour" reads as "sitting exactly ON the 01:30 match" and
    the tick returns the fold-1 twin (~an hour out) instead of the
    imminent fold-0 next-minute fire the ordinary walk finds. Measured:
    seed 2025-11-02 01:30:45 fold-0 America/New_York, every-minute
    expression — `allof` answers `[01:30:45 fold-1]` while `skip`
    answers `[01:31 fold-0]`: the superset strategy fires an hour
    *later* than the subset one, skipping ~29 owed per-minute fires.
    Reachable whenever the seed carries seconds — beyond-window
    recomputes anchor on the PG server clock, client seeds anchor on
    it too, and cron_expr-change recomputes seed on now — not only the
    exact-minute `next_fire_at` values the branch comment assumes.

2.  A payload factory that raises its *own* non-empty `TimeoutError`.
    The new rewrite (`resolve_payload`) replaces the message with
    `"cron payload factory ... timed out after 5s"` unconditionally —
    correct for a genuine `wait_for` hang (empty message, pinned at
    `test_cron.py::testresolve_payload_timeout_names_the_factory`),
    but a factory failing with its own reason (`TimeoutError("db pool
    exhausted")`) gets that reason destroyed: it survives only in
    `__cause__`, never in the operator-visible `error_text` /
    `last_fire_error`. The change's own stated intent is to *add*
    context (which factory), not to delete the reason — and the
    operator reading the schedule row during an outage is told "hung"
    when the truth is "pool exhausted".
"""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from taskq.cron import _factory_cache, compute_next_fire_after, resolve_payload

_TZ = "America/New_York"
_NY = ZoneInfo(_TZ)
# Fall-back morning: 2025-11-02 02:00 EDT -> 01:00 EST; 01:00-01:59:59
# occurs twice. fold=0 is EDT (UTC-4), fold=1 is EST (UTC-5).
_FALLBACK_DAY = (2025, 11, 2)


@pytest.mark.parametrize(
    ("second", "microsecond"),
    [
        (45, 0),  # mid-minute server-clock seed
        (0, 123456),  # on-the-minute seed with leftover microseconds
    ],
)
def test_allof_seed_inside_matching_minute_fires_imminent_fold0_next(
    second: int, microsecond: int
) -> None:
    """A non-exact seed in a matching ambiguous minute must not jump the fold.

    The seed is 45 s (resp. 123 µs) *past* the 01:30 fold-0 match, so the
    next owed fire is the 01:31 fold-0/fold-1 pair — the same answer the
    ordinary walk gives `skip`. Returning the fold-1 twin of the seed's
    own minute skips every fire between the seed and that twin.
    """
    year, month, day = _FALLBACK_DAY
    seed = datetime(year, month, day, 1, 30, second, microsecond, tzinfo=_NY, fold=0)
    out = compute_next_fire_after("* * * * *", _TZ, seed, dst_strategy="allof")
    assert out == [
        datetime(year, month, day, 1, 31, tzinfo=_NY, fold=0),
        datetime(year, month, day, 1, 31, tzinfo=_NY, fold=1),
    ], (
        f"seed {seed.isoformat()} sits past the 01:30 fold-0 match; the next "
        f"owed fires are the 01:31 pair, got {[d.isoformat() for d in out]}"
    )


async def test_factory_own_timeout_reason_survives_the_rewrite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A factory's own `TimeoutError` message must reach the operator intact.

    The rewrite exists so a *hung* factory (empty `wait_for` message)
    names itself. When the factory fails with its own reason, replacing
    that reason with a hang claim misdiagnoses the schedule row: the
    test pins the reason surviving in `str(exc)`, which is what
    `_record_fire_failure` stamps into `error_text` / `last_fire_error`.
    """

    async def _exhausted_factory() -> dict[str, object]:
        raise TimeoutError("db pool exhausted")

    dotted = "redteam.factories.exhausted"
    monkeypatch.setitem(_factory_cache, dotted, _exhausted_factory)
    with pytest.raises(TimeoutError) as exc_info:
        await resolve_payload(dotted, {})
    assert "db pool exhausted" in str(exc_info.value), (
        "the factory's own failure reason must survive the timeout rewrite; "
        f"got {str(exc_info.value)!r}"
    )


def test_allof_exact_minute_seed_with_later_inhour_matches_owes_them_first() -> None:
    """FINDING (coverage round 3, fixed): the seed branch fired too broadly.

    The seed check of the time (``_seed_is_first_occurrence_of_match``,
    since reworked into ``_next_fold1_fire``) answered True for ANY
    exact-minute seed on a match inside the ambiguous hour, and the
    branch then returned the fold-1 twin unconditionally.  That is only
    right when the seed's minute is the LAST owed match of the hour
    (the yearly ``30 1 1 11 *`` case the branch was built for).  Under
    a minutely expression, an exact seed at 01:30 fold-0 owes 01:31
    fold-0 ONE MINUTE later — the walk's answer, and the same answer
    this file pins for the sub-minute seeds above.  Measured while red:
    the branch returned the 01:30 fold-1 twin (06:30 UTC), silently
    skipping every fire from 01:31 through the twin.

    This contradicted ``test_allof_exact_minute_seed_still_returns_the_fold1_twin``
    below, which pinned the twin for ``* * * * *`` — that guard was
    written against the intended single-match case but pinned the
    defect for multi-match hours; the coverage triage re-scoped it to
    the single-match premise.  The reconciliation: the fold-1 pass is
    owed only once no fold-0 match remains after the seed."""
    year, month, day = _FALLBACK_DAY
    seed = datetime(year, month, day, 1, 30, tzinfo=_NY, fold=0)  # exact fire instant
    out = compute_next_fire_after("* * * * *", _TZ, seed, dst_strategy="allof")
    assert out == [
        datetime(year, month, day, 1, 31, tzinfo=_NY, fold=0),
        datetime(year, month, day, 1, 31, tzinfo=_NY, fold=1),
    ], (
        "an exact-minute seed on a minutely expression owes the next minute's "
        "pair first; the fold-1 twin is owed only once no in-hour match "
        f"remains. Got {[d.isoformat() for d in out]}"
    )


@pytest.mark.parametrize(
    ("cron_expr", "seed", "expected_utc"),
    [
        # seconds-precision expression: its fire instants carry seconds,
        # so an exactness-shaped guard year-skips them; the owed fire is
        # the match's fold-1 twin, seconds included
        (
            "30 1 1 11 * 30",
            datetime(2026, 11, 1, 1, 30, 30, tzinfo=_NY, fold=0),
            datetime(2026, 11, 1, 6, 30, 30, tzinfo=UTC),
        ),
        # seed past the range's only match: the MATCH's fold-1 twin is
        # owed — not the seed's (01:30:45 fold-1 is not a fire instant)
        (
            "30 1 1 11 *",
            datetime(2026, 11, 1, 1, 30, 45, tzinfo=_NY, fold=0),
            datetime(2026, 11, 1, 6, 30, tzinfo=UTC),
        ),
        # late in a minutely range: the fold-1 pass re-plays from 01:00 —
        # the seed's own twin (01:59 fold-1) is owed LAST, not first
        (
            "* * * * *",
            datetime(2026, 11, 1, 1, 59, 0, tzinfo=_NY, fold=0),
            datetime(2026, 11, 1, 6, 0, tzinfo=UTC),
        ),
        # the same from a seconds-carrying server-clock seed — the walk
        # alone answers 02:00 (or a pair with a PAST 01:00 fold-0 member),
        # skipping the fold-1 pass entirely
        (
            "* * * * *",
            datetime(2026, 11, 1, 1, 59, 45, tzinfo=_NY, fold=0),
            datetime(2026, 11, 1, 6, 0, tzinfo=UTC),
        ),
        # seed on a non-matching wall: the range's earlier match still
        # owes its fold-1 occurrence — a single fire, no past fold-0 member
        (
            "0 1 * * *",
            datetime(2026, 11, 1, 1, 30, 0, tzinfo=_NY, fold=0),
            datetime(2026, 11, 1, 6, 0, tzinfo=UTC),
        ),
    ],
)
def test_fold0_seed_routes_to_the_fold1_pass_once_fold0_matches_are_spent(
    cron_expr: str, seed: datetime, expected_utc: datetime
) -> None:
    """Class closure: every fold-0 seed inside a repeated range whose
    fold-0 pass is spent owes the fold-1 pass's FIRST match.

    The instances above were all live before `_next_fold1_fire` (year
    skips, the seed's phantom twin, a pair containing an instant before
    the seed); they are the same class the coverage finding routed —
    the owed fire is computed from the range, not from the seed's shape.
    """
    out = compute_next_fire_after(cron_expr, _TZ, seed, dst_strategy="allof")
    assert [d.astimezone(UTC) for d in out] == [expected_utc], (
        f"seed {seed.isoformat()} on {cron_expr!r} must route to the "
        f"fold-1 pass's first match ({expected_utc}), got "
        f"{[d.isoformat() for d in out]}"
    )


def test_half_hour_repeat_zone_routes_the_same_way() -> None:
    """Lord Howe repeats 30 minutes (02:00→01:30), not an hour: the
    fold-1 pass is the [01:30, 02:00) wall range, and the range bounds
    must be walked from the wall, not assumed hour-aligned.

    A seed past the range's only match owes that match's twin; a seed
    with fold-0 matches remaining still takes the walk's pair; a seed
    in a range the expression never matches takes the walk's normal
    answer.
    """
    tz_name = "Australia/Lord_Howe"
    lh = ZoneInfo(tz_name)
    out = compute_next_fire_after(
        "40 1 * * *",
        tz_name,
        datetime(2026, 4, 5, 1, 40, 30, tzinfo=lh, fold=0),
        dst_strategy="allof",
    )
    assert [d.astimezone(UTC) for d in out] == [datetime(2026, 4, 4, 15, 10, tzinfo=UTC)], (
        "a seed past a half-hour range's only match owes the match's "
        f"fold-1 twin, got {[d.isoformat() for d in out]}"
    )
    out = compute_next_fire_after(
        "*/5 * * * *", tz_name, datetime(2026, 4, 5, 1, 40, tzinfo=lh, fold=0), dst_strategy="allof"
    )
    assert [d.astimezone(UTC) for d in out] == [
        datetime(2026, 4, 4, 14, 45, tzinfo=UTC),
        datetime(2026, 4, 4, 15, 15, tzinfo=UTC),
    ], (
        "fold-0 matches remaining in a half-hour range take the walk's "
        f"pair, got {[d.isoformat() for d in out]}"
    )
    out = compute_next_fire_after(
        "0 2 * * *", tz_name, datetime(2026, 4, 5, 1, 40, tzinfo=lh, fold=0), dst_strategy="allof"
    )
    assert [d.astimezone(UTC) for d in out] == [datetime(2026, 4, 4, 15, 30, tzinfo=UTC)], (
        "no match in the repeated range: the walk's normal answer stands, "
        f"got {[d.isoformat() for d in out]}"
    )


def test_allof_exact_minute_seed_still_returns_the_fold1_twin() -> None:
    """The branch's founding case keeps working — a guard against overfix.

    A seed exactly on the earlier occurrence (seconds and microseconds
    zero) of a repeated hour with NO later in-hour match genuinely owes
    the fold-1 twin under `allof`; a fix for the findings above must not
    delete that.  Re-scoped by the coverage triage from the every-minute
    expression it was first written with to the single-match premise it
    was written FOR: on a multi-match hour the twin is owed only after
    the in-hour matches, which is the finding test above — the two had
    pinned contradictory outputs for the same input.
    """
    year, month, day = (2026, 11, 1)  # Nov 1 2026 is NY's fall-back Sunday
    seed = datetime(year, month, day, 1, 30, tzinfo=_NY, fold=0)
    out = compute_next_fire_after("30 1 1 11 *", _TZ, seed, dst_strategy="allof")
    assert [d.astimezone(UTC) for d in out] == [datetime(year, month, day, 6, 30, tzinfo=UTC)], (
        "an exact-minute seed on the hour's last owed match still owes the "
        f"later occurrence, got {[d.isoformat() for d in out]}"
    )


@pytest.mark.parametrize(
    ("strategy", "cron_expr", "tz_name", "seed", "expected_utc"),
    [
        # skip/firstof from a fold-1 seed with MULTIPLE in-range matches
        # still ahead of the seed's wall: the range's slots each fire
        # once, at the earlier occurrence — all spent before a fold-1
        # seed — so the walk-past-the-range loop owes the first match
        # beyond it (02:00 = 07:00 UTC), not an in-range wall whose
        # fold-0 instant precedes the seed
        (
            "skip",
            "*/20 * * * *",
            _TZ,
            datetime(2026, 11, 1, 1, 0, tzinfo=_NY, fold=1),
            datetime(2026, 11, 1, 7, 0, tzinfo=UTC),
        ),
        # firstof shares the branch with skip
        (
            "firstof",
            "*/20 * * * *",
            _TZ,
            datetime(2026, 11, 1, 1, 0, tzinfo=_NY, fold=1),
            datetime(2026, 11, 1, 7, 0, tzinfo=UTC),
        ),
        # skip with a SINGLE remaining in-range candidate: one loop
        # iteration, and the first match beyond the range is tomorrow's
        (
            "skip",
            "30 1 * * *",
            _TZ,
            datetime(2026, 11, 1, 1, 0, tzinfo=_NY, fold=1),
            datetime(2026, 11, 2, 6, 30, tzinfo=UTC),
        ),
        # allof from the LAST in-range match's fold-1 occurrence: the
        # fold-1 pass is spent too, so the walk's beyond-range answer
        # stands (the fold-1 branch falls through)
        (
            "allof",
            "* * * * *",
            _TZ,
            datetime(2026, 11, 1, 1, 59, tzinfo=_NY, fold=1),
            datetime(2026, 11, 1, 7, 0, tzinfo=UTC),
        ),
        # Lord Howe repeats 30 minutes, not an hour: the in-range
        # membership check walks the same wall-derived bounds — allof
        # owes the next in-range match's fold-1 occurrence (01:40 fold-1
        # = 15:10 UTC), skip walks past the range to 02:00 (= 15:30 UTC)
        (
            "allof",
            "*/5 * * * *",
            "Australia/Lord_Howe",
            datetime(2026, 4, 5, 1, 35, tzinfo=ZoneInfo("Australia/Lord_Howe"), fold=1),
            datetime(2026, 4, 4, 15, 10, tzinfo=UTC),
        ),
        (
            "skip",
            "*/5 * * * *",
            "Australia/Lord_Howe",
            datetime(2026, 4, 5, 1, 35, tzinfo=ZoneInfo("Australia/Lord_Howe"), fold=1),
            datetime(2026, 4, 4, 15, 30, tzinfo=UTC),
        ),
    ],
)
def test_fold1_seed_routes_within_or_beyond_the_range_by_strategy(
    strategy: str, cron_expr: str, tz_name: str, seed: datetime, expected_utc: datetime
) -> None:
    """Class closure for the fold-1-seed mirror branch: every sub-case
    the finding below does not already pin.

    A fold-1 seed sits past every fold-0 instant of its range, so under
    ``skip``/``firstof`` the range owes nothing more and the answer is
    the first match BEYOND it — the naive walk alone would offer an
    in-range wall whose earlier occurrence precedes the seed.  Under
    ``allof`` the owed fire is the next in-range match's fold-1
    occurrence (the finding below), or the walk's beyond-range answer
    once no in-range match remains.  Half-hour repeat zones answer the
    same way — the bounds are walked from the wall, not assumed
    hour-aligned.
    """
    out = compute_next_fire_after(cron_expr, tz_name, seed, dst_strategy=strategy)
    assert [d.astimezone(UTC) for d in out] == [expected_utc], (
        f"{strategy} seed {seed.isoformat()} on {cron_expr!r} in {tz_name} must "
        f"route to {expected_utc}, got {[d.isoformat() for d in out]}"
    )


def test_allof_fold1_seed_owes_the_next_fold1_match_not_the_spent_fold0() -> None:
    """FINDING (coverage round 4, fixed): from a fold-1 seed the walk answered a
    pair whose first member was in the PAST.

    In a fall-back repeated hour every wall time's fold-0 occurrence
    precedes its fold-1 occurrence in UTC, so from a seed already inside
    the fold-1 pass the next minute's fold-0 member is spent by
    construction — yet the walk returns it first: a function named
    ``compute_next_fire_after`` answering an instant BEFORE its seed.
    The tick stores the first member as ``next_fire_at``; the schedule
    then sits due in the past and re-fires already-played slots (the
    over-delivery half of the regression the parity file pins at tick
    level).  For singletons — whose delivery is schedule-driven, with no
    twin chain — the same answer would send the fold-1 pass back through
    the fold-0 hour instead of advancing within it.  The owed answer
    from a fold-1 seed is the remaining fold-1 matches.
    """
    seed = datetime(2026, 11, 1, 6, 0, tzinfo=UTC)  # 01:00 fold-1; fold-0 pass is spent
    out = compute_next_fire_after("* * * * *", _TZ, seed, dst_strategy="allof")
    assert [d.astimezone(UTC) for d in out] == [datetime(2026, 11, 1, 6, 1, tzinfo=UTC)], (
        "from a fold-1 seed the next minute's fold-0 member is spent (it precedes "
        "the seed); the owed fire is the fold-1 occurrence alone, got "
        f"{[d.isoformat() for d in out]}"
    )
