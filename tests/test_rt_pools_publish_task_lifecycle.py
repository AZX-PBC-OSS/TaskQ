"""Pins for the ``pending_publish_tasks`` fire-and-forget lifecycle.

``WorkerDeps.pending_publish_tasks`` exists so a progress publish task
started by a short-lived JobContext "outlives the context and cannot be
garbage-collected mid-publish" (deps.py). The lifecycle contract:

* every task ``ctx.progress()`` creates is added to the shared set
  (a strong reference — the whole point of the set);
* the task's done-callback discards it, so a COMPLETED publish leaves
  no reference behind — the set is a window over in-flight publishes,
  not a grow-only ledger;
* a publish that HANGS is abandoned by the publish path's own
  ``_PUBLISH_TIMEOUT_S`` (1 s) bound, so even a black-holed Redis
  cannot keep a task (and its reference, and its coroutine frame) in
  the set indefinitely — and shutdown's
  ``_drain_pending_publishes`` window cannot be held open by it.

Both pins below assert the set drains to empty under bounded waits, so
a regression to an unbounded publish (or a done-callback that stops
firing) fails in seconds instead of hanging the drain forever.

In-memory tier: a fake Redis client whose pipeline round trip is
delayed or hung on demand; a real ``JobContext`` wired the way the
consumer wires it (buffer registered in the shared dict, publish tasks
tracked in the shared set).
"""

from __future__ import annotations

import asyncio
from uuid import UUID

from taskq._ids import new_uuid
from taskq.context import JobContext
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.testing.in_memory import PassthroughPayload
from tests._progress_context import make_progress_context

_DSN = "postgresql://u:p@h:5432/db"


class _FakePipeline:
    """Non-transactional pipeline double: one bounded (or hung) execute."""

    def __init__(self, redis: _FakeRedis) -> None:
        self._redis = redis
        self._channels: list[str] = []

    def publish(self, channel: str, payload: str) -> None:
        self._channels.append(channel)

    async def execute(self) -> list[int]:
        self._redis.published.extend(self._channels)
        await self._redis._round_trip()
        return [1] * len(self._channels)

    async def __aenter__(self) -> _FakePipeline:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeRedis:
    """Redis double on the default publish path (dual-channel pipeline)."""

    def __init__(self, *, delay: float = 0.0, hang: bool = False) -> None:
        self.published: list[str] = []
        self._delay = delay
        self._hang = hang

    async def _round_trip(self) -> None:
        if self._hang:
            await asyncio.sleep(30)
        if self._delay:
            await asyncio.sleep(self._delay)

    def pipeline(self, transaction: bool = True) -> _FakePipeline:
        return _FakePipeline(self)

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append(channel)
        await self._round_trip()
        return 1


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": _DSN, "TASKQ_SCHEMA_NAME": "taskq"},
        validate=False,
    )


def _ctx(
    redis: _FakeRedis,
    settings: WorkerSettings,
    pending: set[asyncio.Task[None]],
    buffers: dict[UUID, _ProgressBuffer],
    job_id: UUID,
) -> JobContext[PassthroughPayload]:
    buffers[job_id] = _ProgressBuffer(job_id=job_id, base_seq=0)
    return make_progress_context(
        buffers,
        job_id,
        actor="progress_actor",
        worker_id=new_uuid(),
        settings=settings,
        redis_client=redis,
        pending_publish_tasks=pending,
    )


async def _wait_set_empty(tasks: set[asyncio.Task[None]], drain_bound_s: float) -> None:
    """Bounded drain: the done-callbacks run on the loop, so polling drains."""
    deadline = asyncio.get_running_loop().time() + drain_bound_s
    while tasks:
        if asyncio.get_running_loop().time() > deadline:
            still = [t for t in tasks if not t.done()]
            raise AssertionError(
                f"pending_publish_tasks did not drain within {drain_bound_s}s: "
                f"{len(still)} task(s) still referenced after completion "
                "window — the done-callback discard is not firing (or the "
                "publish itself is unbounded), so every progress call leaks "
                "one task reference for the life of the worker."
            )
        await asyncio.sleep(0.01)


async def test_completed_publishes_are_discarded_from_the_shared_set() -> None:
    """GREEN pin: the set is a window over in-flight publishes, not a ledger.

    Two progress calls with a 0.25 s pipeline round trip each create a
    tracked task; while in flight the set holds both (the
    anti-GC-reference is the set's whole purpose), and once both round
    trips complete the done-callbacks must have discarded them.
    """
    redis = _FakeRedis(delay=0.25)
    pending: set[asyncio.Task[None]] = set()
    buffers: dict[UUID, _ProgressBuffer] = {}
    ctx = _ctx(redis, _settings(), pending, buffers, new_uuid())

    await ctx.progress(step=1)
    await ctx.progress(step=2)

    assert len(pending) == 2, (
        "each in-flight publish must be strongly referenced by "
        "pending_publish_tasks — that reference is what stops the event "
        "loop garbage-collecting a fire-and-forget task mid-publish."
    )
    await _wait_set_empty(pending, drain_bound_s=5.0)
    assert redis.published, "the fake round trip must actually have run"


async def test_hung_publish_is_abandoned_bounded_and_discarded() -> None:
    """GREEN pin: a black-holed Redis round trip cannot pin a task reference.

    The publish path bounds every round trip at ``_PUBLISH_TIMEOUT_S``
    (1 s); a pipeline that never returns must be abandoned by that
    bound, its task completing (timeout swallowed, failure recorded) and
    its reference discarded — so the set, and shutdown's
    ``_drain_pending_publishes`` window, cannot be held open by a dead
    Redis. A regression to an unbounded publish would leave the task
    referenced past this test's 3 s drain bound.
    """
    redis = _FakeRedis(hang=True)
    pending: set[asyncio.Task[None]] = set()
    buffers: dict[UUID, _ProgressBuffer] = {}
    ctx = _ctx(redis, _settings(), pending, buffers, new_uuid())

    await ctx.progress(step=1)

    assert len(pending) == 1
    await _wait_set_empty(pending, drain_bound_s=3.0)
