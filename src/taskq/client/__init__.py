"""TaskQ client — the public surface for enqueuing, querying, and
cancelling jobs.

Re-exports :class:`JobsClient`, :class:`JobHandle`, :class:`TaskQ`, and
:class:`JobEvent`.
Import from ``taskq.client`` (or from ``taskq`` which re-exports these
names).

anchors:  (public API ownership).
"""

from taskq.client._enqueuer import SubJobEnqueuer
from taskq.client._handle import JobHandle
from taskq.client._jobs import JobsClient
from taskq.client._taskq import JobEvent, TaskQ
from taskq.types import CancelResult

__all__ = [
    "CancelResult",
    "JobEvent",
    "JobHandle",
    "JobsClient",
    "SubJobEnqueuer",
    "TaskQ",
]
