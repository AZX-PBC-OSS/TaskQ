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
    "decode_cursor",
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
