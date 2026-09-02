"""Cross-backend cursor encoding for keyset pagination.

Both :class:`PostgresBackend` and :class:`InMemoryBackend` must agree on
cursor encoding and comparison semantics (``JobFilter.cursor`` docstring).
This module is the canonical location for that contract — it lives in
``taskq.backend`` so that production code can import it without depending
on the ``taskq.testing`` package.
"""

from datetime import datetime
from uuid import UUID

__all__ = [
    "decode_batch_cursor",
    "decode_cursor",
    "encode_batch_cursor",
    "encode_cursor",
]


def encode_cursor(priority: int, scheduled_at: datetime, job_id: UUID) -> str:
    """Encode keyset pagination cursor as ``priority|iso|uuid``."""
    return f"{priority}|{scheduled_at.isoformat()}|{job_id}"


def decode_cursor(cursor: str) -> tuple[int, datetime, UUID]:
    """Decode keyset pagination cursor."""
    parts = cursor.split("|", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid cursor format: {cursor!r}")
    priority = int(parts[0])
    scheduled_at = datetime.fromisoformat(parts[1])
    job_id = UUID(parts[2])
    return priority, scheduled_at, job_id


def encode_batch_cursor(created_at: datetime, batch_id: UUID) -> str:
    """Encode a batch keyset cursor as ``iso|uuid``.

    Same ``|``-delimited shape as :func:`encode_cursor`, one field
    shorter: ``list_batches`` orders on ``(created_at, id)`` where
    ``list_jobs`` orders on ``(priority, scheduled_at, id)``.
    """
    return f"{created_at.isoformat()}|{batch_id}"


def decode_batch_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode a batch keyset cursor to the columns' own Python types.

    Returning a ``datetime`` and a ``UUID`` -- not the raw text -- is
    load-bearing, not tidiness: asyncpg infers each placeholder's type
    from its ``::`` cast and refuses a ``str`` for ``timestamptz`` or
    ``uuid``, which is what made every admin job-list page turn raise
    ``DataError`` before 2569da5.
    """
    parts = cursor.split("|", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid batch cursor format: {cursor!r}")
    return datetime.fromisoformat(parts[0]), UUID(parts[1])
