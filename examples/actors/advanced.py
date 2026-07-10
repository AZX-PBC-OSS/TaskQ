"""Advanced actor options — singleton, max_concurrent, unique_for, result_ttl.

These actors demonstrate less-common but important @actor decorator options:

- ``singleton_job``: fleet-wide at-most-one-active enforcement via
  ``singleton=True``.  Enqueueing a second copy while one is running raises
  :class:`~taskq.SingletonCollisionError`.
- ``capped_job``: soft fleet-wide concurrency cap via ``max_concurrent=2``.
  Up to 2 may run simultaneously; further jobs queue behind them.
- ``deduplicated``: per-identity deduplication via ``unique_for``.  Enqueue
  the same ``identity_key`` twice within 1 minute and the second call returns
  the existing :class:`~taskq.JobHandle` (``handle.was_existing == True``).
- ``summer``: returns a typed :class:`SumResult` stored for 1 hour via
  ``result_ttl``.  Callers can retrieve the value with
  ``await handle.wait()``.
"""

import asyncio
from datetime import timedelta

from pydantic import BaseModel

from taskq import actor


class EmptyPayload(BaseModel):
    pass


class DeduplicatedPayload(BaseModel):
    key: str = "default"


class SumPayload(BaseModel):
    values: str = "1,2,3"


class SumResult(BaseModel):
    total: int


@actor(name="singleton_job", queue="examples", singleton=True)
async def singleton_job(payload: EmptyPayload) -> None:
    """Runs for 5s — only one instance may be active fleet-wide (singleton=True)."""
    await asyncio.sleep(5)


@actor(name="capped_job", queue="examples", max_concurrent=2)
async def capped_job(payload: EmptyPayload) -> None:
    """Runs for 3s — at most 2 may run simultaneously fleet-wide (max_concurrent=2)."""
    await asyncio.sleep(3)


@actor(
    name="deduplicated",
    queue="examples",
    unique_for=timedelta(minutes=1),
)
async def deduplicated(payload: DeduplicatedPayload) -> None:
    """Sleeps 2s — enqueueing the same identity_key within 1 min returns the existing job."""
    await asyncio.sleep(2)


@actor(name="summer", queue="examples", result_ttl=timedelta(hours=1))
async def summer(payload: SumPayload) -> SumResult:
    """Sums a comma-separated list of integers and returns the result (stored for 1h)."""
    total = sum(int(v.strip()) for v in payload.values.split(",") if v.strip())
    return SumResult(total=total)
