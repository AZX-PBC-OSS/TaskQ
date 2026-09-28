"""A cancel-path ``JobContext`` with a minimal payload and a null enqueuer.

Three cancel-watch suites (the post-tx contract test and the two
cross-worker abandon/fencing attacks) grew identical copies of
``_StubPayload`` and ``_make_ctx``; this module is their single source.
The context is shaped for the cancel ladder's seams: a real bound logger
(``bind_job_context``) so job-scoped log assertions see the envelope, and
a ``SubJobEnqueuer`` stubbed to ``None`` on every dependency, because the
cancel path must never actually enqueue a sub-job for these scenarios.
"""

from __future__ import annotations

from uuid import UUID

import structlog
from pydantic import BaseModel

from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context

__all__ = ["StubPayload", "make_ctx"]


class StubPayload(BaseModel):
    """Minimal payload for a cancel-path JobContext."""


def make_ctx(job_id: UUID, worker_id: UUID) -> JobContext[BaseModel]:
    return JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        claim_epoch=0,
        worker_id=worker_id,
        payload=StubPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )
