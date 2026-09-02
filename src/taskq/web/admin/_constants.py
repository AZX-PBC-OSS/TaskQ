"""Shared constants and helpers for admin list pages with keyset pagination."""

from fastapi import HTTPException

from taskq.client._args import (
    _MAX_TAG_LENGTH,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the enqueue-side tag length contract rather than redefining a drifting copy of it.
)

_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
)
_ACTIVE_STATUSES: frozenset[str] = frozenset({"pending", "scheduled", "running"})
_ALL_STATUSES: frozenset[str] = _TERMINAL_STATUSES | _ACTIVE_STATUSES
_PAGE_SIZE: int = 50
_FETCH_SIZE: int = _PAGE_SIZE + 1

#: The tags filter is a hand-typed comma list; 16 items is generous for
#: any real overlap query while bounding the text[] bind and the parse.
_MAX_TAG_FILTER_ITEMS: int = 16


def parse_job_statuses(raw: list[str], *, default: list[str] | None = None) -> list[str]:
    """Validate and return the requested status list; raises HTTPException on bad input.

    When *raw* is empty, returns *default* (or all terminal statuses when
    *default* is ``None``). More values than the closed set (8) is
    rejected — no valid request needs them, since duplicates are deduped
    (first-occurrence order) and the set has no more members than that.
    """
    invalid = [s for s in raw if s not in _ALL_STATUSES]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"invalid status value(s): {invalid!r}; allowed: {sorted(_ALL_STATUSES)!r}",
        )
    if len(raw) > len(_ALL_STATUSES):
        raise HTTPException(
            status_code=400,
            detail=(
                f"too many status values: {len(raw)}; at most {len(_ALL_STATUSES)} "
                "(the full closed set) are accepted — duplicates are deduplicated"
            ),
        )
    if not raw:
        return default if default is not None else sorted(_TERMINAL_STATUSES)
    seen: set[str] = set()
    deduped: list[str] = []
    for s in raw:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def parse_job_tags(raw: str | None) -> list[str] | None:
    """Parse the comma-joined ``tags`` filter; raises HTTPException on bad input.

    Returns ``None`` when *raw* is empty/absent (no filter). Items are
    stripped and deduplicated (first-occurrence order), the item count is
    capped, and each item is capped at the enqueue-side ``_MAX_TAG_LENGTH``
    — the stored tags can never exceed it, so a longer filter term can
    never match anything.
    """
    if not raw:
        return None
    items = [t.strip() for t in raw.split(",") if t.strip()]
    if len(items) > _MAX_TAG_FILTER_ITEMS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"too many tag filters: {len(items)}; at most {_MAX_TAG_FILTER_ITEMS} are accepted"
            ),
        )
    for t in items:
        if len(t) > _MAX_TAG_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"tag filter exceeds {_MAX_TAG_LENGTH} characters: {t[:64]!r}…",
            )
    seen: set[str] = set()
    deduped: list[str] = []
    for t in items:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped or None
