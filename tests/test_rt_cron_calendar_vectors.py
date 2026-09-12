"""Red-team calendar-correctness vectors for the cron parser.

Target: ``taskq.cron.compute_next_fire_after`` (croniter-backed, plus custom
DST gap/overlap handling). ``tests/test_cron.py`` has no Feb-29 / leap-year,
day-of-month-vs-day-of-week, ``L``, or short-month vectors, and neither
``src/taskq/cron.py`` nor ``docs/guides/cron.md`` documents the DOM-vs-DOW
combining rule or ``L`` support — both are inherited silently from croniter.

Oracle: where TaskQ documents semantics, those govern; where semantics are
undocumented (DOM-vs-DOW combining, ``L``), standard-cron behavior is the
oracle, stated per test. All seeds use ``UTC`` so DST handling cannot move
the answer. Each expected instant was computed by hand from the calendar
(day-of-week verified against ``datetime.strftime('%A')``) and confirmed
against a direct run before being pinned here.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from taskq.cron import _factory_cache, compute_next_fire_after, cron
from taskq.scheduler import _CRON_REGISTRY

_UTC = "UTC"
_TZ = ZoneInfo(_UTC)


@pytest.fixture(autouse=True)
def _restore_module_globals() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Snapshot and restore _CRON_REGISTRY around each test.

    The ``L`` test below calls ``cron()``, which appends to the
    module-level registry; without this fixture that entry would leak
    into other test modules. Mirrors the autouse fixture in
    ``tests/test_cron.py`` (registry only — this file never touches
    the factory cache).
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


def test_feb29_found_across_non_leap_years() -> None:
    """``0 0 29 2 *`` seeded before a leap day must land on the leap day.

    Seed: 2027-02-27 12:00 UTC (2027 and 2029 have no Feb 29; 2028 does).
    Hand-computed oracle: 2028 is divisible by 4 and not a century year, so
    2028-02-29 exists and is the first Feb-29 strictly after the seed. A
    Feb-28 or Mar-01 answer would be calendar drift; an exception would be
    a leap-year outage (pg_cron #365 class).
    """
    seed = datetime(2027, 2, 27, 12, 0, tzinfo=_TZ)
    out = compute_next_fire_after("0 0 29 2 *", _UTC, seed)
    assert out == [datetime(2028, 2, 29, 0, 0, tzinfo=_TZ)], (
        f"2028-02-29 is the first Feb-29 after 2027-02-27; got {[d.isoformat() for d in out]}"
    )


def test_feb29_after_leap_day_skips_to_next_leap_cycle() -> None:
    """``0 0 29 2 *`` seeded just after a leap day must skip to 2032.

    Seed: 2028-03-01 00:00 UTC (exactly at midnight after the 2028 leap
    day fired). Hand-computed oracle: 2029/2030/2031 have no Feb 29, and
    2032 is the next leap year (divisible by 4, not a century year), so the
    next fire is 2032-02-29 00:00 UTC. A Feb-28/Mar-01 answer in an
    intervening year would be drift; an exception would be a leap-cycle
    outage.
    """
    seed = datetime(2028, 3, 1, 0, 0, tzinfo=_TZ)
    out = compute_next_fire_after("0 0 29 2 *", _UTC, seed)
    assert out == [datetime(2032, 2, 29, 0, 0, tzinfo=_TZ)], (
        "no Feb-29 exists in 2029-2031, so the next fire after the 2028 "
        f"leap day is 2032-02-29; got {[d.isoformat() for d in out]}"
    )


def test_dom_vs_dow_uses_standard_cron_or_rule() -> None:
    """``0 0 1 * 1`` must fire on the 1st-of-month OR Monday (standard cron).

    FINDING (ambiguity): neither ``src/taskq/cron.py`` nor
    ``docs/guides/cron.md`` documents the DOM-vs-DOW combining rule — the
    docs say only "standard 5-field cron expression ... validated via
    ``croniter.is_valid()``". The rule is inherited silently from croniter,
    so an operator reading TaskQ's docs cannot predict this schedule. This
    test pins the de-facto rule so any future change is a visible break.

    Hand-computed oracle under the standard OR rule: 2024-06-15 is a
    Saturday, so the next fire is Monday 2024-06-17 (DOW matches, DOM does
    not). Under an AND rule the answer would instead be 2024-07-01 (the
    next date that is both the 1st and a Monday — verified: 2024-07-01 is
    a Monday). pg_cron #421 class.
    """
    seed = datetime(2024, 6, 15, 12, 0, tzinfo=_TZ)
    out = compute_next_fire_after("0 0 1 * 1", _UTC, seed)
    assert out == [datetime(2024, 6, 17, 0, 0, tzinfo=_TZ)], (
        "standard cron OR rule: Monday 2024-06-17 matches DOW even though "
        f"it is not the 1st (AND rule would give 2024-07-01); "
        f"got {[d.isoformat() for d in out]}"
    )


def test_dom_vs_dow_second_step_confirms_or_not_and() -> None:
    """A second step distinguishes OR from AND even when both rules agree once.

    Seed: exactly at the 2024-06-17 Monday fire. Under OR the next fire is
    the next Monday, 2024-06-24 (DOW matches, DOM does not). Under AND the
    next fire would be 2024-07-01 (next 1st-that-is-a-Monday). This closes
    the loophole where 2024-07-01 satisfies both rules at once.
    """
    seed = datetime(2024, 6, 17, 0, 0, tzinfo=_TZ)
    out = compute_next_fire_after("0 0 1 * 1", _UTC, seed)
    assert out == [datetime(2024, 6, 24, 0, 0, tzinfo=_TZ)], (
        "standard cron OR rule: the Monday after 2024-06-17 is 2024-06-24 "
        f"(AND rule would skip to 2024-07-01); got {[d.isoformat() for d in out]}"
    )


def test_last_day_of_month_accepted_and_calendar_correct() -> None:
    """``L`` (last-day-of-month) is accepted and lands on month ends.

    ``croniter.is_valid('0 0 L * *')`` is True and ``cron()`` accepts the
    expression at schedule-creation depth (clean ``ValueError``-or-accept
    contract — no traceback from deep inside a tick). Hand-computed oracle:
    Jan 2024 has 31 days, Feb 2024 (leap) has 29, Feb 2023 has 28, Apr 2024
    has 30 — the fires must be those exact last days at 00:00 UTC.
    """
    assert cron("0 0 L * *", actor="rt_probe_last_day").cron_expr == "0 0 L * *"
    cases: list[tuple[datetime, datetime]] = [
        (datetime(2024, 1, 15, 12, 0, tzinfo=_TZ), datetime(2024, 1, 31, 0, 0, tzinfo=_TZ)),
        (datetime(2024, 2, 15, 12, 0, tzinfo=_TZ), datetime(2024, 2, 29, 0, 0, tzinfo=_TZ)),
        (datetime(2023, 2, 15, 12, 0, tzinfo=_TZ), datetime(2023, 2, 28, 0, 0, tzinfo=_TZ)),
        (datetime(2024, 4, 15, 12, 0, tzinfo=_TZ), datetime(2024, 4, 30, 0, 0, tzinfo=_TZ)),
    ]
    for seed, expected in cases:
        out = compute_next_fire_after("0 0 L * *", _UTC, seed)
        assert out == [expected], (
            f"last day of {seed.strftime('%B %Y')} is {expected.date()}; "
            f"got {[d.isoformat() for d in out]}"
        )


def test_31st_skips_short_months_without_drift() -> None:
    """``0 0 31 * *`` across April (30 days) must skip to May 31.

    Hand-computed oracle: April has 30 days, so there is no April-31 fire;
    the next 31st strictly after 2024-04-15 (and after 2024-04-30, past
    April's end) is 2024-05-31 00:00 UTC. Firing on April 30 would be the
    pg_cron #292 off-by-one class (clamping to month end instead of
    skipping).
    """
    for seed in (
        datetime(2024, 4, 15, 12, 0, tzinfo=_TZ),
        datetime(2024, 4, 30, 12, 0, tzinfo=_TZ),
    ):
        out = compute_next_fire_after("0 0 31 * *", _UTC, seed)
        assert out == [datetime(2024, 5, 31, 0, 0, tzinfo=_TZ)], (
            f"April has no 31st, so the next fire after {seed.isoformat()} "
            f"is 2024-05-31; got {[d.isoformat() for d in out]}"
        )
