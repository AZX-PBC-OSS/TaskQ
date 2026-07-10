"""Hypothesis property test for sub-enqueue visibility invariant.

Uses a _FakeBackend stub (faster than testcontainers; the transactional
guarantee is mirrored by the buffer and exercised on
real PG).

Invariant: for any sequence of N ctx.jobs.enqueue() calls within a
single parent with a LOOP-scope connection registered, when the parent's
outcome is drawn from a fixed set, the visibility invariant holds:

- On "success": all N children are flushed (enqueue_with_conn called).
- On any failure outcome: zero children are flushed (buffer discarded).

Hypothesis settings mirror existing property tests in
tests/test_di_property.py: max_examples=100, deadline=2s.
"""

import asyncio
from contextlib import AbstractAsyncContextManager, suppress
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

import asyncpg
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    AttemptRow,
    CancelFlag,
    JobFilter,
    JobRow,
)
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import RetryAfter, Snooze
from taskq.retry import RetryPolicy
from taskq.testing.actor import FakeBackend, as_backend, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


class _ParentPayload(BaseModel):
    name: str = "parent"


class _ChildPayload(BaseModel):
    name: str = "child"


class _Result(BaseModel):
    ok: bool = True


def _child_ref() -> ActorRef[_ChildPayload, _Result]:
    async def _handler(payload: _ChildPayload) -> _Result:
        return _Result()

    return ActorRef(
        name="child_actor",
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_ChildPayload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


_CHILD_REF = _child_ref()


class _FakeBackend(FakeBackend):
    """FakeBackend subclass that tracks enqueue calls for visibility assertions."""

    supports_transactional_simulation: bool = True

    def __init__(self) -> None:
        super().__init__()
        self.enqueue_calls: list[object] = []
        self.enqueue_with_conn_calls: list[object] = []
        self.mark_succeeded_with_conn_calls: list[object] = []

    async def enqueue(self, args: object) -> JobRow:
        self.enqueue_calls.append(args)
        return make_job_row()

    async def enqueue_with_conn(self, conn: object, args: object) -> JobRow:
        self.enqueue_with_conn_calls.append(args)
        return make_job_row()

    async def mark_succeeded(
        self,
        job_id: UUID,
        worker_id: UUID,
        result: object = None,
        progress_seq: int = 0,
        progress_state: object = None,
    ) -> bool:
        self.mark_succeeded_calls.append(result)
        return True

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: UUID,
        worker_id: UUID,
        result: object = None,
        progress_seq: int = 0,
        progress_state: object = None,
    ) -> bool:
        self.mark_succeeded_with_conn_calls.append(result)
        return True

    async def mark_failed_or_retry(
        self,
        job_id: UUID,
        worker_id: UUID,
        error_info: object = None,
        next_scheduled_at: object = None,
        progress_seq: int = 0,
        progress_state: object = None,
    ) -> JobRow:
        self.mark_failed_or_retry_calls.append(error_info)
        return make_job_row()

    async def mark_cancelled(
        self,
        job_id: UUID,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: object = None,
    ) -> bool:
        self.mark_cancelled_calls.append(job_id)
        return True

    async def mark_snoozed(
        self,
        job_id: UUID,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: object = None,
        progress_seq: int = 0,
        progress_state: object = None,
        outcome: str = "snoozed",
    ) -> Literal["scheduled", "failed", "noop"]:
        self.mark_snoozed_calls.append(job_id)
        return "scheduled"

    async def mark_retry_after(
        self,
        job_id: UUID,
        worker_id: UUID,
        delay: timedelta,
        *,
        consume_budget: bool = True,
        progress_seq: int = 0,
        progress_state: object = None,
    ) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
        self.mark_retry_after_calls.append(job_id)
        return "scheduled"

    async def write_attempt(self, attempt: object) -> None:
        pass

    async def get_attempts(self, job_id: UUID) -> list[AttemptRow]:
        return []

    async def write_cancel_request(self, job_id: UUID, reason: str | None) -> bool:
        return False

    async def poll_cancel_flags(self, worker_id: UUID) -> list[CancelFlag]:
        return []

    async def scheduled_to_pending(self, now: object) -> int:
        return 0

    async def deadline_sweep(self, now: object) -> int:
        return 0

    async def reclaim_expired_locks(
        self, now: object, cancel_grace: timedelta, cleanup_grace: timedelta
    ) -> int:
        return 0

    async def get(self, job_id: UUID) -> JobRow | None:
        return None

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]:
        return []

    def subscribe_wake(self) -> AbstractAsyncContextManager[asyncio.Event]:
        raise NotImplementedError


class _FakeConnection:
    class _Transaction:
        async def __aenter__(self) -> "_FakeConnection._Transaction":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    def transaction(self) -> "_FakeConnection._Transaction":
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


_OUTCOME_STRATEGY = st.sampled_from(
    [
        "success",
        "transient",
        "snooze",
        "retry_after",
        "cancelled",
    ]
)


@given(
    n_children=st.integers(min_value=0, max_value=10),
    outcome=_OUTCOME_STRATEGY,
)
@settings(max_examples=100, deadline=timedelta(seconds=2))
async def test_tp1_visibility_invariant(n_children: int, outcome: str) -> None:
    """For any N sub-enqueues and any parent outcome, the visibility invariant holds."""

    backend = _FakeBackend()
    fake_conn = _FakeConnection()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: fake_conn},
        worker_pool=None,
        backend=as_backend(backend),
        clock=FakeClock(_NOW),
    )

    job = make_job_row()
    worker_id = new_uuid()

    async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        for _ in range(n_children):
            await enqueuer.enqueue(_CHILD_REF, _ChildPayload())
        match outcome:
            case "success":
                return {"ok": True}
            case "transient":
                raise RuntimeError("transient failure")
            case "snooze":
                raise Snooze(timedelta(seconds=60))
            case "retry_after":
                raise RetryAfter(timedelta(seconds=30))
            case "cancelled":
                raise asyncio.CancelledError()
            case _:
                return {"ok": True}

    clk = FakeClock(_NOW)
    with suppress(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            worker_id,
            run_actor=run_actor,
            actor_config=default_actor_config(),
            payload_type=_ParentPayload,
            clock=clk,
            enqueuer=enqueuer,
            loop_conn=fake_conn,
        )

    flushed = len(backend.enqueue_calls)

    if outcome in ("success", "snooze", "retry_after"):
        assert flushed == n_children, (
            f"on {outcome}: expected {n_children} flushed/re-enqueued children, got {flushed}"
        )
    else:
        assert flushed == 0, f"on {outcome}: expected 0 flushed children, got {flushed}"
