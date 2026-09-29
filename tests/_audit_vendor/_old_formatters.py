"""VERBATIM old formatter sources, extracted from origin/main by AST

(git show origin/main:<path> -> ast.get_source_segment). No hand
transcription: this is the exact pre-refactor text, vendored so the
differential audit tests can execute it against the new module.
"""

# Harness shim, not old source: the vendored defs evaluate their
# annotations at def time and _format_interval annotates ``timedelta``.
from datetime import timedelta


def _format_age(seconds: float) -> str:  # pyright: ignore[reportUnusedFunction]  # Why: vendored old source — referenced dynamically by the differential audit tests.
    """Humanize a server-computed age for the depth table."""
    total = int(seconds)
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days > 0:
        return f"{days}d{hours}h"
    if hours > 0:
        return f"{hours}h{minutes}m"
    if minutes > 0:
        return f"{minutes}m{secs}s"
    return f"{secs}s"


def _humanize_seconds(seconds: float | None) -> str | None:  # pyright: ignore[reportUnusedFunction]  # Why: vendored old source — referenced dynamically by the differential audit tests.
    """A human duration for a seconds figure (``800`` → ``13m``); ``None``
    passes through so the template can render its dash."""
    if seconds is None:
        return None
    s = round(seconds)
    if s < 1:
        return "<1s"
    if s < 60:
        return f"{s}s"
    days, rem = divmod(s, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    return f"{minutes}m"


def _format_interval(td: timedelta) -> str:  # pyright: ignore[reportUnusedFunction]  # Why: vendored old source — referenced dynamically by the differential audit tests.
    """Human form for the report only ('30 days', '7 days'), never SQL."""
    seconds = int(td.total_seconds())
    days, rem = divmod(seconds, 86400)
    if days and not rem:
        return f"{days} days"
    hours, rem = divmod(rem, 3600)
    if hours and not rem:
        return f"{hours} hours"
    return f"{seconds} seconds"
