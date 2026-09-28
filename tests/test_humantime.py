"""The human-duration module's presentation contracts, pinned.

Three operator surfaces render elapsed time; the unit cascade lives once
in :mod:`taskq._humantime` and each surface's output shape is a pinned
contract (the admin pages' parity tests and the CLI's table format
assert exact strings against them).  These tests pin both layers:

* each formatter's EXACT output strings — a change to a surface's
  rendering is a visible-contract change and must edit a pin here, not
  silently rewrite the shared split;
* the admin constants' status sets are DERIVED from the state machine,
  not hand-copied — the drift the derivation removes (the admin pages'
  filters tracking a new status without a second literal) is the point,
  so the pin asserts identity with the statemachine's frozensets.
"""

import pytest

from taskq._humantime import humanize_age, humanize_wait, split_seconds
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (None, None),
        (0.4, "<1s"),
        (0.9, "1s"),  # rounds first: 0.9 → 1s, the <1s floor is post-round
        (1, "1s"),
        (45, "45s"),
        (59, "59s"),
        (60, "1m"),
        (799, "13m"),
        (3600, "1h"),
        (3661, "1h 1m"),
        (90000, "1d 1h"),
        (86400, "1d"),
        (172800, "2d"),
        (90061, "1d 1h"),
        (-5, "<1s"),  # negative floors at the same <1s sentinel
    ],
)
def test_humanize_wait_pinned(seconds: float | None, expected: str | None) -> None:
    """The dashboard's wait columns render exactly these strings."""
    assert humanize_wait(seconds) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "0s"),
        (45, "45s"),
        (59, "59s"),
        (60, "1m0s"),
        (330, "5m30s"),
        (3600, "1h0m"),
        (12900, "3h35m"),
        (86400, "1d0h"),
        (194400, "2d6h"),
        (90061, "1d1h"),  # truncated: the CLI column never rounds up
    ],
)
def test_humanize_age_pinned(seconds: float, expected: str) -> None:
    """The CLI depth table's age column renders exactly these strings."""
    assert humanize_age(seconds) == expected


def test_split_seconds_cascade() -> None:
    """The one split every formatter composes: days/hours/minutes/seconds."""
    assert split_seconds(90061) == (1, 1, 1, 1)
    assert split_seconds(0) == (0, 0, 0, 0)
    assert split_seconds(86399) == (0, 23, 59, 59)


def test_admin_status_sets_derive_from_the_state_machine() -> None:
    """The admin constants' status closed sets ARE the statemachine's.

    The pre-derivation shape hand-copied both string sets here; the pin
    asserts object identity so a future literal re-introduced next to
    this module's import fails loudly instead of drifting silently.
    """
    from taskq.web.admin import _constants

    assert _constants._TERMINAL_STATUSES is TERMINAL_STATUSES
    assert _constants._ACTIVE_STATUSES is ACTIVE_STATUSES
    assert _constants._ALL_STATUSES == TERMINAL_STATUSES | ACTIVE_STATUSES
