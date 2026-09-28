"""Differential audit: the pinned humantime contracts and the admin status sets.

Two refactor claims of b56a0f39, executed against the OLD sources rather
than re-asserted:

1. ``taskq._humantime``'s formatters reproduce the pre-refactor
   functions' outputs EXACTLY (the vendored old sources in
   ``tests/_audit_vendor/_old_formatters.py`` were extracted verbatim
   from ``origin/main`` by AST, no transcription) — across the boundary
   inputs: 0, fractional seconds, unit boundaries, day boundaries,
   negatives, ``None``, and NaN.
2. ``web/admin/_constants``' derived status sets are not just derived
   from the state machine (pinned by identity in
   ``tests/test_humantime.py``) but EQUAL to the hand-copied literals
   the derivation replaced — so a status added to the machine REDS here
   and forces the admin consumers' notification, instead of silently
   widening the pages' filters.
"""

import importlib.util
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from taskq._humantime import humanize_age, humanize_wait, split_seconds
from taskq.backend._protocol import JOB_STATUS_VALUES
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES
from taskq.timescale import _format_interval  # pyright: ignore[reportPrivateUsage]

_VENDOR = Path(__file__).parent / "_audit_vendor" / "_old_formatters.py"

# The OLD hand-copied sets (origin/main's web/admin/_constants.py), the
# boundary the derivation must reproduce exactly.
_OLD_HAND_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})
_OLD_HAND_ACTIVE = frozenset({"pending", "scheduled", "running"})

# The attack grid: 0, fractional, sub-minute, the minute boundary,
# fractional minutes (banker's rounding), the hour/day boundaries, the
# multi-day shape, negatives, None, NaN.
_WAIT_GRID = [0, 0.5, 59.5, 60, 61, 3600, 86399, 86400, 90000, -5, None]
_AGE_GRID = [0, 0.5, 59.5, 60, 61, 3600, 86399, 86400, 90000, -5, -90000]
_INTERVALS = [
    timedelta(0),
    timedelta(seconds=1),
    timedelta(seconds=59),
    timedelta(minutes=90),
    timedelta(hours=25),
    timedelta(days=1),
    timedelta(days=7),
    timedelta(days=30),
    timedelta(days=30, seconds=1),
    timedelta(seconds=-90),
]


@pytest.fixture(scope="module")
def old_formatters() -> Any:
    spec = importlib.util.spec_from_file_location("taskq_audit_old_formatters", _VENDOR)
    assert spec is not None and spec.loader is not None
    mod: Any = importlib.util.module_from_spec(spec)
    # The vendored defs evaluate their annotations at def time; the old
    # ``_format_interval`` annotates ``td: timedelta``.
    mod.__dict__["timedelta"] = timedelta
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("seconds", _WAIT_GRID)
def test_audit_humanize_wait_matches_the_old_insights_formatter(
    old_formatters: Any, seconds: float | None
) -> None:
    assert humanize_wait(seconds) == old_formatters._humanize_seconds(seconds)


@pytest.mark.parametrize("seconds", _AGE_GRID)
def test_audit_humanize_age_matches_the_old_cli_formatter(
    old_formatters: Any, seconds: float
) -> None:
    assert humanize_age(seconds) == old_formatters._format_age(seconds)


@pytest.mark.parametrize("td", _INTERVALS)
def test_audit_format_interval_matches_the_old_timescale_formatter(
    old_formatters: Any, td: timedelta
) -> None:
    assert _format_interval(td) == old_formatters._format_interval(td)


def test_audit_nan_is_a_shared_edge_not_a_new_one(old_formatters: Any) -> None:
    """NaN's old behavior (a ValueError from ``round``/``int``) is exact."""
    nan = float("nan")
    with pytest.raises(ValueError):
        humanize_wait(nan)
    with pytest.raises(ValueError):
        old_formatters._humanize_seconds(nan)
    with pytest.raises(ValueError):
        humanize_age(nan)
    with pytest.raises(ValueError):
        old_formatters._format_age(nan)


def test_audit_admin_terminal_set_equals_the_old_hand_copy() -> None:
    """The derived TERMINAL set IS the old hand-copied literal set.

    A status added to the state machine's terminal literal changes this
    set — the red forces the admin consumers' explicit notification.
    """
    assert TERMINAL_STATUSES == _OLD_HAND_TERMINAL


def test_audit_admin_active_set_equals_the_old_hand_copy() -> None:
    assert ACTIVE_STATUSES == _OLD_HAND_ACTIVE


def test_audit_admin_union_covers_every_status_exactly_once() -> None:
    """Terminal + Active = the JobStatus literal set, disjointly."""
    assert TERMINAL_STATUSES.isdisjoint(ACTIVE_STATUSES)
    assert frozenset(JOB_STATUS_VALUES) == TERMINAL_STATUSES | ACTIVE_STATUSES


def test_audit_boundary_status_memberships() -> None:
    """The boundary statuses sit on the side the old hand-copies put them."""
    for status in ("succeeded", "failed", "cancelled", "crashed", "abandoned"):
        assert status in TERMINAL_STATUSES
        assert status not in ACTIVE_STATUSES
    for status in ("pending", "scheduled", "running"):
        assert status in ACTIVE_STATUSES
        assert status not in TERMINAL_STATUSES


def test_audit_split_seconds_cascade_matches_the_old_divmods() -> None:
    """The shared split reproduces the old inline cascades' components."""
    for total in (0, 61, 3600, 86399, 86400, 90061, 194400):
        days, rem = divmod(total, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)
        assert split_seconds(total) == (days, hours, minutes, secs)


def test_audit_negative_seconds_shared_rounding(old_formatters: Any) -> None:
    """Negative inputs: the old functions' shapes are pinned, not changed."""
    # old _humanize_seconds(-5): round(-5) = -5 < 1 → '<1s'
    assert humanize_wait(-5) == old_formatters._humanize_seconds(-5) == "<1s"
    # old _format_age(-90000): int(-90000); divmod floors, so the old
    # cascade yields (-2, 82800) → 23h0m — reproduced, not 'improved'.
    assert humanize_age(-90000) == old_formatters._format_age(-90000) == "23h0m"
