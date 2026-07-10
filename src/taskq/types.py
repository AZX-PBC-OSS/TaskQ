"""Client-facing result and event-detail types.

``CancelResult`` is the structured return value of ``JobsClient.cancel()``
 ``StateChangeEvent`` is the JSON payload stored in
``job_events.detail`` for rows with ``kind='state_change'``.

These types live here — not in ``taskq.backend`` — so the Backend protocol
remains pydantic-free and the layering contract is enforceable by import
inspection.
"""

from dataclasses import dataclass
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from taskq.backend._protocol import JobId, JobStatus

__all__ = ["CancelResult", "StateChangeEvent"]


class CancelResult(BaseModel):
    """Structured outcome of a cancellation request.

    Returned by ``JobsClient.cancel()`` so callers can inspect whether
    the cancellation was initiated and what the status transition was.
    """

    model_config = ConfigDict(frozen=True)

    job_id: JobId
    previous_status: JobStatus
    new_status: JobStatus
    cancellation_initiated: bool


@dataclass(frozen=True, slots=True)
class StateChangeEvent:
    """JSON payload for ``job_events.detail`` when ``kind='state_change'``.

    Serialized via ``taskq._json.dumps`` (orjson with UUID support), not
    pydantic, to keep the event-detail path free of validation overhead.
    """

    from_state: JobStatus
    to_state: JobStatus
    error_class: str | None = None
    worker_id: UUID | None = None
