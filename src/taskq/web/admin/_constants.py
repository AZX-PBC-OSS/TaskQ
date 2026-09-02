"""Shared constants and helpers for admin list pages with keyset pagination."""

from fastapi import HTTPException

from taskq._json import check_no_nul_str
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


def parse_job_statuses(raw: list[str], *, default: list[str] | None = None) -> list[str]:
    """Validate and return the requested status list; raises HTTPException on bad input.

    When *raw* is empty, returns *default* (or all terminal statuses when
    *default* is ``None``). Values outside the closed set are rejected;
    what survives is deduplicated in first-occurrence order, which alone
    bounds the returned list to the set's 8 members however long *raw* is.
    """
    invalid = [s for s in raw if s not in _ALL_STATUSES]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"invalid status value(s): {invalid!r}; allowed: {sorted(_ALL_STATUSES)!r}",
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


def parse_text_filter(raw: str | None, what: str) -> str | None:
    """Validate a scalar text filter; raises HTTPException on a NUL.

    Every admin list filter is bound as a ``text`` parameter, and
    PostgreSQL rejects a NUL in a ``text`` value with
    ``CharacterNotInRepertoireError`` (SQLSTATE 22021) — an opaque 500
    from deep inside the driver. This is the admin-route counterpart of
    the client path's ``JobFilter``/``EnqueueArgs`` NUL guards
    (:func:`taskq._json.check_no_nul_str`): reject at parse time with a
    clean 400 instead. *raw* is returned unchanged (``None`` included) —
    blank-normalization stays each route's own concern.
    """
    if raw is None:
        return None
    try:
        check_no_nul_str(raw, what=what)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return raw


def parse_job_tags(raw: str | None) -> list[str] | None:
    """Parse the comma-joined ``tags`` filter; raises HTTPException on bad input.

    Returns ``None`` when *raw* is empty/absent (no filter). Items are
    stripped and deduplicated (first-occurrence order) and each item is
    capped at the enqueue-side ``_MAX_TAG_LENGTH`` — the stored tags can
    never exceed it, so a longer filter term can never match anything.
    There is deliberately no cap on the item *count*: the parse is O(n)
    over a query string the URL length already bounds and the ``text[]``
    bind is flat, so a cap would only make a legitimate wide overlap
    query unexpressible. A NUL in an item is rejected here because the
    list is bound as ``text[]`` — the same SQLSTATE-22021 class
    :func:`parse_text_filter` guards for scalar filters.
    """
    if not raw:
        return None
    items = [t.strip() for t in raw.split(",") if t.strip()]
    for t in items:
        if len(t) > _MAX_TAG_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"tag filter exceeds {_MAX_TAG_LENGTH} characters: {t[:64]!r}…",
            )
        try:
            check_no_nul_str(t, what="tag filter item")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    seen: set[str] = set()
    deduped: list[str] = []
    for t in items:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped or None
