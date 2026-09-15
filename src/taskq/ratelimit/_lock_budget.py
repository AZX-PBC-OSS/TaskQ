"""Operator-configured bounded-wait budgets for the Postgres admission locks.

Every admission lock the PG paths take — the token bucket's and the GCRA
style's ``rate_limit_buckets`` row lock, the log style's per-bucket
advisory lock — waits for a bounded budget so contention surfaces as a
denial with a truthful retry hint instead of an indefinite block. This
module holds the one place those budgets are read off the settings
object, so the acquire and refund arms of both limiter shapes cannot
drift onto different values.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskq.settings import WorkerSettings

__all__ = [
    "resolve_sliding_window_lock_timeout_ms",
    "resolve_token_bucket_lock_timeout_ms",
]


def _resolve(
    override: float | None,
    settings: "WorkerSettings | None",
    field: str,
    default: float,
) -> float:
    """Pick the budget an admission lock waits for.

    An explicit *override* wins so a caller can bound a single acquire
    more tightly than the deployment-wide knob. Otherwise the operator's
    settings value governs, read WITHOUT a fallback: the field is declared
    on WorkerSettings and that declaration is pinned by the settings
    contract tests, so a settings object lacking it is a wiring bug. The
    resulting AttributeError surfaces that bug at the acquire — a silent
    fallback to the shipped constant would ignore the operator's setting
    while looking like success.
    """
    if override is not None:
        return override
    if settings is None:
        return default
    return float(getattr(settings, field))


def resolve_token_bucket_lock_timeout_ms(
    override: float | None,
    settings: "WorkerSettings | None",
    default: float,
) -> float:
    """Budget for the token bucket's ``rate_limit_buckets`` row lock."""
    return _resolve(override, settings, "token_bucket_lock_timeout_ms", default)


def resolve_sliding_window_lock_timeout_ms(
    override: float | None,
    settings: "WorkerSettings | None",
    default: float,
) -> float:
    """Budget for the sliding window's advisory (log) or row (GCRA) lock."""
    return _resolve(override, settings, "sliding_window_lock_timeout_ms", default)
