"""Human-duration rendering for the operator surfaces.

Three surfaces render elapsed time to an operator — the CLI's depth
table (``taskq queues-depth``), the dashboard's ``/insights`` page, and
the TimescaleDB report — and each used to carry its own ``divmod``
cascade over the same three unit sizes (86 400 / 3 600 / 60).  Three
hand-maintained copies of one cascade is the drift pattern this module
exists to close: the unit sizes and the day/hour/minute/second split
live here ONCE, and each surface's own presentation contract (spacing,
rounding, sub-minute floor, pluralisation) stays a small formatter on
top of the shared split.

Not one formatter with mode flags on purpose: the three surfaces'
outputs are pinned presentation contracts (tests assert the exact
strings), so the variants stay separate named functions over one
split, and a change to one surface's contract cannot silently move
another's.
"""

from __future__ import annotations

from typing import Final

__all__ = [
    "humanize_age",
    "humanize_wait",
    "split_seconds",
]

_SECS_PER_DAY: Final[int] = 86400
_SECS_PER_HOUR: Final[int] = 3600
_SECS_PER_MINUTE: Final[int] = 60


def split_seconds(total: int) -> tuple[int, int, int, int]:
    """Split a whole number of seconds into (days, hours, minutes, seconds).

    The single home of the cascade every operator surface needs: a
    formatter renders from these components, never re-derives them.
    """
    days, rem = divmod(total, _SECS_PER_DAY)
    hours, rem = divmod(rem, _SECS_PER_HOUR)
    minutes, secs = divmod(rem, _SECS_PER_MINUTE)
    return days, hours, minutes, secs


def humanize_wait(seconds: float | None) -> str | None:
    """A human duration for the insights page's wait columns.

    ``800`` → ``13m``, ``90000`` → ``1d 1h``; ``None`` passes through so
    the template can render its dash.  Sub-minute waits floor to
    ``<1s``; a minute and up never shows seconds (a wait column's
    resolution is minutes — seconds there read as precision the
    aggregate does not have).  The leading unit never hides the next
    one's remainder below a minute, so ``3661`` renders ``1h 1m``, not
    ``1h``.
    """
    if seconds is None:
        return None
    s = round(seconds)
    if s < 1:
        return "<1s"
    if s < _SECS_PER_MINUTE:
        return f"{s}s"
    days, hours, minutes, _ = split_seconds(s)
    if days:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    return f"{minutes}m"


def humanize_age(seconds: float) -> str:
    """Humanize a server-computed age for the CLI's depth table.

    Truncates to whole seconds and always carries the next unit's
    remainder in a compact, no-space shape (``2d3h``, ``5m30s``) — a
    terminal column, where the spaces cost alignment.
    """
    total = int(seconds)
    days, hours, minutes, secs = split_seconds(total)
    if days > 0:
        return f"{days}d{hours}h"
    if hours > 0:
        return f"{hours}h{minutes}m"
    if minutes > 0:
        return f"{minutes}m{secs}s"
    return f"{secs}s"
