"""Shared constants and helpers for admin list pages with keyset pagination."""

from fastapi import HTTPException

_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
)
_ACTIVE_STATUSES: frozenset[str] = frozenset({"pending", "scheduled", "running"})
_ALL_STATUSES: frozenset[str] = _TERMINAL_STATUSES | _ACTIVE_STATUSES
_PAGE_SIZE: int = 50
_FETCH_SIZE: int = _PAGE_SIZE + 1


def parse_job_statuses(raw: list[str], *, default: list[str] | None = None) -> list[str]:
    """Validate and return the requested status list; raises HTTPException on bad input.

    When *raw* is empty, returns *default* (or all terminal statuses when
    *default* is ``None``).
    """
    invalid = [s for s in raw if s not in _ALL_STATUSES]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"invalid status value(s): {invalid!r}; allowed: {sorted(_ALL_STATUSES)!r}",
        )
    return raw if raw else (default if default is not None else sorted(_TERMINAL_STATUSES))
