"""A progress-wired :class:`JobContext`, for tests of the progress path.

``ctx.progress()`` writes into the worker's per-job buffer map and, when a
Redis client and settings are present, publishes through them. Suites
that exercise that path construct the same context - a job's identity,
a no-op sub-enqueuer over an in-memory backend, a bound logger, and the
private progress wiring - so one builder here keeps them on one shape.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import structlog

from taskq._ids import new_job_id
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

__all__ = ["make_progress_context"]


def make_progress_context(
    buffers: dict[UUID, _ProgressBuffer] | None,
    job_id: UUID | None = None,
    *,
    worker_id: UUID | None = None,
    actor: str = "test_actor",
    queue: str = "default",
    attempt: int = 1,
    backend: InMemoryBackend | None = None,
    settings: WorkerSettings | None = None,
    redis_client: Any = None,
    pending_publish_tasks: set[asyncio.Task[None]] | None = None,
) -> JobContext[PassthroughPayload]:
    """A ``JobContext`` whose progress calls land in *buffers*.

    *job_id* defaults to a fresh id; *worker_id* to *backend*'s own (a
    fresh in-memory backend unless one is given). With no *settings* the
    context enforces no data-size cap and publishes nothing - pass
    settings (and a *redis_client*) to exercise publishing; ``None``
    *buffers* is a context with no progress wiring at all.
    """
    if job_id is None:
        job_id = new_job_id()
    if backend is None:
        backend = InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))
    if worker_id is None:
        worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: the backend's own worker id is the one its dispatch stamps; tests reuse it so a context matches its rows.
    return JobContext(
        job_id=job_id,
        actor=actor,
        queue=queue,
        attempt=attempt,
        worker_id=worker_id,
        payload=PassthroughPayload(),
        cancel_event=asyncio.Event(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=job_id,
            actor=actor,
            queue=queue,
            attempt=attempt,
            identity_key=None,
            trace_id="",
        ),
        _progress_buffers=buffers,
        _redis_client=redis_client,
        _worker_settings=settings,
        _pending_publish_tasks=pending_publish_tasks,
    )
