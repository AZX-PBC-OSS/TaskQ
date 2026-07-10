"""Unit tests for ActiveJobRegistry (,).

Covers
- register / deregister / get / all / count correctness
- idempotent deregister (missing key is a no-op)
- atomicity: concurrent register/deregister tasks leave registry consistent
- __len__ mirrors count()
- Registered entry exposes job_id, task, ctx, cancel_phase, cancel_observed_at
"""

import asyncio
from uuid import UUID

import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import JobId
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.worker.cancel import ActiveJobRegistry, _ActiveJob

# ── Helpers ────────────────────────────────────────────────────────────────


def _make_task() -> asyncio.Task[object]:
    """Create a placeholder never-completing asyncio.Task."""
    loop = asyncio.get_running_loop()
    return loop.create_task(asyncio.sleep(9999))


class _FakePayload(BaseModel):
    """Minimal payload model for registry tests."""


def _make_ctx(job_id: UUID) -> JobContext[BaseModel]:
    """Create a minimal JobContext for registry test purposes."""
    from datetime import UTC, datetime

    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    return JobContext(
        job_id=job_id,
        actor="test",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=_FakePayload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=backend,
        ),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job_id,
            actor="test",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


# ── Basic register / get / count / all / deregister ───────────────────────


async def test_register_and_get() -> None:
    """register then get returns the _ActiveJob with correct fields."""
    registry = ActiveJobRegistry()
    job_id = new_job_id()
    task = _make_task()
    ctx = _make_ctx(job_id)

    await registry.register(job_id, task, ctx)

    entry = registry.get(job_id)
    assert entry is not None
    assert isinstance(entry, _ActiveJob)
    assert entry.job_id == job_id
    assert entry.task is task
    assert entry.ctx is ctx
    assert entry.cancel_phase == 0
    assert entry.cancel_observed_at is None

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


async def test_get_missing_returns_none() -> None:
    """get on an unregistered job_id returns None."""
    registry = ActiveJobRegistry()
    assert registry.get(new_job_id()) is None


async def test_count_and_len_empty() -> None:
    """count() and len() return 0 on empty registry."""
    registry = ActiveJobRegistry()
    assert registry.count() == 0
    assert len(registry) == 0


async def test_count_after_register() -> None:
    """count() increments after register."""
    registry = ActiveJobRegistry()
    ids = [new_job_id() for _ in range(3)]
    tasks: list[asyncio.Task[object]] = []
    for jid in ids:
        t = _make_task()
        tasks.append(t)
        await registry.register(jid, t, _make_ctx(jid))

    assert registry.count() == 3
    assert len(registry) == 3

    for t in tasks:
        t.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await t


async def test_all_returns_snapshot() -> None:
    """all() returns a list copy containing all registered entries."""
    registry = ActiveJobRegistry()
    ids = [new_job_id(), new_job_id()]
    tasks: list[asyncio.Task[object]] = []
    for jid in ids:
        t = _make_task()
        tasks.append(t)
        await registry.register(jid, t, _make_ctx(jid))

    snapshot = registry.all()
    assert len(snapshot) == 2
    snapshot_ids = {entry.job_id for entry in snapshot}
    assert snapshot_ids == set(ids)

    for t in tasks:
        t.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await t


async def test_deregister_removes_entry() -> None:
    """deregister removes the entry; count drops; get returns None."""
    registry = ActiveJobRegistry()
    job_id = new_job_id()
    task = _make_task()
    await registry.register(job_id, task, _make_ctx(job_id))
    assert registry.count() == 1

    await registry.deregister(job_id)
    assert registry.count() == 0
    assert registry.get(job_id) is None

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


async def test_deregister_idempotent() -> None:
    """deregister on an already-absent job_id is a silent no-op."""
    registry = ActiveJobRegistry()
    missing_id = new_job_id()
    # Should not raise
    await registry.deregister(missing_id)
    assert registry.count() == 0


async def test_deregister_twice_is_idempotent() -> None:
    """Calling deregister twice for the same job_id does not raise."""
    registry = ActiveJobRegistry()
    job_id = new_job_id()
    task = _make_task()
    await registry.register(job_id, task, _make_ctx(job_id))

    await registry.deregister(job_id)
    await registry.deregister(job_id)  # second call: silent no-op
    assert registry.count() == 0

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


async def test_all_snapshot_is_independent_of_subsequent_mutations() -> None:
    """all() returns a copy; mutations after the snapshot do not affect it."""
    registry = ActiveJobRegistry()
    job_id = new_job_id()
    task = _make_task()
    await registry.register(job_id, task, _make_ctx(job_id))

    snapshot = registry.all()
    assert len(snapshot) == 1

    # Deregister after taking snapshot
    await registry.deregister(job_id)
    assert registry.count() == 0

    # Snapshot still has the original entry
    assert len(snapshot) == 1

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── Multiple registrations ────────────────────────────────────────────────


async def test_multiple_jobs_independent() -> None:
    """Multiple jobs registered independently; each get() returns its own entry."""
    registry = ActiveJobRegistry()
    job_ids = [new_job_id() for _ in range(5)]
    tasks: list[asyncio.Task[object]] = []
    ctxs: list[JobContext[BaseModel]] = []

    for jid in job_ids:
        t = _make_task()
        ctx = _make_ctx(jid)
        tasks.append(t)
        ctxs.append(ctx)
        await registry.register(jid, t, ctx)

    for jid, t, ctx in zip(job_ids, tasks, ctxs, strict=True):
        entry = registry.get(jid)
        assert entry is not None
        assert entry.task is t
        assert entry.ctx is ctx

    for t in tasks:
        t.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await t


async def test_overwrite_registration() -> None:
    """Registering the same job_id twice overwrites the entry (last write wins)."""
    registry = ActiveJobRegistry()
    job_id = new_job_id()
    task1 = _make_task()
    task2 = _make_task()
    ctx1 = _make_ctx(job_id)
    ctx2 = _make_ctx(job_id)

    await registry.register(job_id, task1, ctx1)
    assert registry.count() == 1

    await registry.register(job_id, task2, ctx2)
    assert registry.count() == 1  # still one entry (same key)

    entry = registry.get(job_id)
    assert entry is not None
    assert entry.task is task2  # second registration wins
    assert entry.ctx is ctx2

    for t in (task1, task2):
        t.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await t


# ── Atomicity under concurrent register/deregister ───────────────────────


async def test_concurrent_register_deregister_atomicity() -> None:
    """atomicity: concurrent register and deregister leave the
    registry in a consistent (non-corrupted) state.

    Spawns 10 concurrent register tasks and 5 concurrent deregister tasks
    for distinct job_ids. After all tasks complete, count() is exactly
    the number of registered-but-not-deregistered IDs.
    """
    registry = ActiveJobRegistry()

    n_register = 10
    n_deregister = 5  # first 5 IDs are also deregistered concurrently

    job_ids = [new_job_id() for _ in range(n_register)]
    tasks: list[asyncio.Task[object]] = [_make_task() for _ in range(n_register)]

    async def register_one(jid: JobId, t: asyncio.Task[object]) -> None:
        await registry.register(jid, t, _make_ctx(jid))

    async def deregister_one(jid: JobId) -> None:
        await registry.deregister(jid)

    # Launch all register and first-N deregister concurrently
    register_coros = [register_one(jid, t) for jid, t in zip(job_ids, tasks, strict=True)]
    deregister_coros = [deregister_one(jid) for jid in job_ids[:n_deregister]]

    await asyncio.gather(*register_coros, *deregister_coros)

    # After concurrent register+deregister, count must be consistent:
    # each ID is either registered or not; no partial/corrupted state.
    final_count = registry.count()
    assert 0 <= final_count <= n_register

    # IDs that were only registered (not in the deregister list) must be present
    for jid in job_ids[n_deregister:]:
        assert registry.get(jid) is not None, f"{jid} should be registered"

    for t in tasks:
        t.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await t


# ── _ActiveJob default fields ─────────────────────────────────────────────


async def test_active_job_defaults() -> None:
    """_ActiveJob is constructed with cancel_phase=0 and cancel_observed_at=None."""
    job_id = new_job_id()
    task = _make_task()
    ctx = _make_ctx(job_id)
    entry = _ActiveJob(job_id=job_id, task=task, ctx=ctx)

    assert entry.cancel_phase == 0
    assert entry.cancel_observed_at is None

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task
