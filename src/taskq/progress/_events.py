"""ProgressEvent wire-format model for Redis pubsub fanout."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

__all__ = ["ProgressEvent"]


class ProgressEvent(BaseModel):
    """Point-in-time progress snapshot published to Redis for SSE/stream fanout.

    Covers both ``kind="progress"`` (incremental update) and
    ``kind="state_change"`` (terminal or status transition) events.  The
    ``exclude_none=True`` flag on :meth:`model_dump_json` suppresses null
    fields so the JSON payload stays compact on the wire.
    """

    model_config = ConfigDict(frozen=True)

    v: int = 1
    kind: Literal["progress", "state_change"]
    job_id: UUID
    actor: str
    ts: datetime
    seq: int
    status: str
    step: int | None = None
    percent: float | None = None
    detail: str | None = None
    data: dict[str, object] | None = None
    terminal: bool = False
