"""Structural guards on the consumer's terminal-write path.

Three conditions run on every successful attempt and none of them had an
assertion that depended on their *shape*:

* ``if entry is not None and entry.cancel_phase >= COOPERATIVE`` — the
  phase threshold is pinned by existing tests, the ``entry is not None``
  half is not.  A job whose registry entry has already been removed while
  other jobs are still in flight reaches this line with ``entry is None``.
* ``_pbuf.dirty = False`` after a successful terminal write — nothing
  distinguished "we flushed" from "we flushed exactly once".
* ``progress_state=... if _cancel_buf is not None and _cancel_buf.dirty
  else None`` on the cooperative-cancel write — every covering test had a
  clean buffer *and* never looked at ``progress_state``.
"""

import asyncio
from datetime import UTC, datetime
from types import TracebackType
from typing import Self
from unittest.mock import MagicMock
from uuid import UUID

from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import CancelPhase, JobRow
from taskq.backend.clock import Clock
from taskq.context import JobContext
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._flush import _flush_buffer_immediate
from taskq.settings import WorkerSettings
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


# ── Test doubles ─────────────────────────────────────────────────────


class _TxFakeConnection:
    """asyncpg.Connection stand-in for the LOOP-scope transactional path."""

    class _Transaction:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

    def transaction(self) -> "_TxFakeConnection._Transaction":
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _TxFakeBackend(FakeBackend):
    """FakeBackend that advertises transactional simulation support."""

    BACKEND_PROTOCOL_VERSION: int = 1
    supports_transactional_simulation: bool = True

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: UUID,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: object = None,
    ) -> bool:
        return await self.mark_succeeded(job_id, worker_id, result, progress_seq)


class _RecordingPool:
    """asyncpg.Pool stand-in that records every statement a flush issues."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    class _Acquired:
        def __init__(self, pool: "_RecordingPool") -> None:
            self._pool = pool

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        async def fetchrow(self, query: str, *args: object) -> dict[str, object]:
            self._pool.statements.append(query)
            return {"progress_seq": 1}

    def acquire(self) -> "_RecordingPool._Acquired":
        return self._Acquired(self)


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})


# ── cancel guard: a registry holding only OTHER jobs ─────────────────


async def _run_with_foreign_registry_entry(
    *, loop_conn: object | None
) -> tuple[str, _TxFakeBackend]:
    """Run a job whose own registry entry is gone but siblings remain.

    Deregistration racing the terminal write is real: the shutdown and
    cancel paths both remove entries while other jobs stay in flight.
    """
    active_jobs = ActiveJobRegistry()
    backend = _TxFakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    sibling_id = new_uuid()

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        task = asyncio.current_task()
        assert task is not None
        await active_jobs.register(sibling_id, task, ctx)
        await active_jobs.deregister(running.id)
        return {"ok": True}

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        active_jobs=active_jobs,
        loop_conn=loop_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _TxFakeConnection supplies the transaction()/execute() surface the consumer uses.
    )
    assert active_jobs.get(sibling_id) is not None
    assert active_jobs.get(job.id) is None
    return outcome, backend


async def test_autonomous_missing_own_entry_still_succeeds() -> None:
    """A populated registry without this job's entry is not a cancellation."""
    outcome, backend = await _run_with_foreign_registry_entry(loop_conn=None)

    assert outcome == "succeeded"
    assert len(backend.mark_succeeded_calls) == 1
    assert len(backend.mark_cancelled_calls) == 0


async def test_transactional_missing_own_entry_still_succeeds() -> None:
    """Same guard, same shape, on the LOOP-scope transactional path."""
    outcome, backend = await _run_with_foreign_registry_entry(loop_conn=_TxFakeConnection())

    assert outcome == "succeeded"
    assert len(backend.mark_succeeded_calls) == 1
    assert len(backend.mark_cancelled_calls) == 0


# ── terminal write retires the buffer exactly once ───────────────────


async def _succeed_with_dirty_buffer(
    *, loop_conn: object | None
) -> tuple[str, _TxFakeBackend, _ProgressBuffer, UUID]:
    backend = _TxFakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}
    captured: list[_ProgressBuffer] = []

    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = None
    deps.settings = _settings()
    deps.redis_client = None

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        await ctx.progress(step=1, detail="halfway")
        buf = buffers[running.id]
        assert buf.dirty is True
        captured.append(buf)
        return {"ok": True}

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        loop_conn=loop_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _TxFakeConnection supplies the transaction()/execute() surface the consumer uses.
    )
    return outcome, backend, captured[0], job.id


async def _assert_buffer_retired(loop_conn: object | None) -> None:
    """After a success the buffer is clean, so a later flush writes nothing.

    ``buf.dirty is False`` alone says only that the flag was assigned; the
    follow-up flush is what distinguishes "flushed once" from "will flush
    again", which is a duplicate progress write against a finished job.
    """
    outcome, backend, buf, job_id = await _succeed_with_dirty_buffer(loop_conn=loop_conn)

    assert outcome == "succeeded"
    assert len(backend.mark_succeeded_calls) == 1
    assert buf.dirty is False

    pool = _RecordingPool()
    await _flush_buffer_immediate(
        pool,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Pool; _RecordingPool supplies the acquire()/fetchrow() surface the flush uses.
        "taskq_test",
        job_id,
        _WORKER_ID,
        {job_id: buf},
    )
    assert pool.statements == []


async def test_autonomous_terminal_write_retires_the_progress_buffer() -> None:
    await _assert_buffer_retired(None)


async def test_transactional_terminal_write_retires_the_progress_buffer() -> None:
    await _assert_buffer_retired(_TxFakeConnection())


# ── cooperative cancel carries progress only when there is progress ──


async def _cancelled_write(*, report_progress: bool) -> dict[str, object]:
    active_jobs = ActiveJobRegistry()
    backend = FakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = None
    deps.settings = _settings()
    deps.redis_client = None

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        if report_progress:
            await ctx.progress(step=7)
        entry = active_jobs.get(running.id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        return {"ok": True}

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        active_jobs=active_jobs,
    )

    assert len(backend.mark_cancelled_calls) == 1
    return backend.mark_cancelled_calls[0]


async def test_cancel_write_omits_progress_state_for_a_clean_buffer() -> None:
    """No progress reported → the cancel write carries no progress state.

    Not ``{}``: an empty dict is a value the terminal SQL would write over
    the row's existing progress_state.
    """
    call = await _cancelled_write(report_progress=False)

    assert call["progress_state"] is None
    assert call["progress_seq"] == 0


async def test_cancel_write_carries_progress_state_for_a_dirty_buffer() -> None:
    """Progress reported → the cancel write carries it, unflushed."""
    call = await _cancelled_write(report_progress=True)

    assert call["progress_state"] == {"step": 7}
    assert call["progress_seq"] == 1
