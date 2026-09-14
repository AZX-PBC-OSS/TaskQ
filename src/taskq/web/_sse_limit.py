"""Shared connection cap for Server-Sent Events endpoints.

Every SSE connection pins server resources for as long as the client keeps it
open: an asyncio task, a Postgres LISTEN connection or a Redis pubsub
subscription, and a socket/file descriptor. Without a cap, any principal who
can reach the route can open them until the process runs out -- on the app
hosting the ingestion pipeline, not on some isolated dashboard tier.

`web/admin/sse.py` already capped its own `/sse/{topic}` endpoint this way. Two
other streams did not, so the mechanism is extracted here rather than
duplicated a third time:

* ``/jobs/api/job/{job_id}/progress/stream`` (per-job progress; holds a Redis
  pubsub subscription)
* ``/jobs/sse/live`` (admin live job feed; holds a PG LISTEN connection) --
  which lives in `admin/jobs.py` and bypassed `admin/sse.py` entirely, so its
  `admin_max_sse_connections` cap never applied to it.

The semaphore is keyed by endpoint family and limit, and is process-local.
Multiple worker processes each get their own budget, which is the same
semantics the admin endpoint already had.
"""

import asyncio
from collections.abc import AsyncGenerator

from fastapi import HTTPException

__all__ = ["acquire_sse_slot", "release_after"]

_SEMAPHORES: dict[tuple[str, int], asyncio.Semaphore] = {}

#: Non-blocking acquire. A waiting client would hold the request open while
#: queueing for a slot, which is the resource exhaustion being prevented.
_ACQUIRE_TIMEOUT: float = 0.001


def _semaphore(key: str, limit: int) -> asyncio.Semaphore:
    # The limit is part of the key because one process can mount the same
    # family at several limits (the test suite does; a misconfigured process
    # could), and a later mount must enforce the limit it was given — keying
    # by family alone made the first mount's limit silently govern every
    # later one. Mounts that agree on the limit still share one budget,
    # which is the cross-router sharing the family key exists to provide.
    scoped = (key, limit)
    if scoped not in _SEMAPHORES:
        _SEMAPHORES[scoped] = asyncio.Semaphore(limit)
    return _SEMAPHORES[scoped]


async def acquire_sse_slot(key: str, limit: int) -> asyncio.Semaphore:
    """Take an SSE slot for *key* or raise 429.

    Returns the semaphore so the caller can release it when its stream ends.
    Callers MUST release exactly once, in a ``finally`` inside the streaming
    generator -- releasing in the route handler would free the slot while the
    stream is still open.
    """
    semaphore = _semaphore(key, limit)
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=_ACQUIRE_TIMEOUT)
    except TimeoutError:
        raise HTTPException(
            status_code=429,
            detail=f"too many concurrent SSE connections for {key!r}",
        ) from None
    return semaphore


async def release_after(
    semaphore: asyncio.Semaphore, gen: AsyncGenerator[str, None]
) -> AsyncGenerator[str, None]:
    """Wrap *gen*, releasing *semaphore* when it finishes for any reason.

    Client disconnect surfaces as ``CancelledError`` thrown into the
    generator, so the release has to be in a ``finally`` around the
    iteration rather than after it.
    """
    try:
        async for chunk in gen:
            yield chunk
    finally:
        semaphore.release()
