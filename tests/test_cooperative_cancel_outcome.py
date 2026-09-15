"""An actor that observes cancellation and returns a value has succeeded.

Cancellation in TaskQ is a request, not a kill: ``ctx.cancellation_requested``
exists so an actor can wind down deliberately — flush what it computed, close
what it opened, and return whatever partial or degraded answer it managed. The
guide teaches exactly that shape (``docs/guides/cancellation.md``: check the
flag, ``await cleanup()``, ``return``).

The terminal state must therefore be decided by what the actor did, not by the
fact that a cancel was requested while it ran. An actor that RETURNS a value
completed its unit of work; an actor that raises ``CancelledError``, or that is
force-cancelled after the grace period, did not. Collapsing both into
``cancelled`` throws away a result the system already holds in hand — and it is
not a race, but a decision taken after the value has returned.

The operator stake: a cancel that arrives one millisecond before an actor's
final ``return`` destroys completed work and, because the job is terminal, it
is never re-run. Anything downstream that reads the result — a batch waiting on
members, a caller polling the handle, a report assembled from partial answers —
sees nothing, and the row records no error explaining why.

These tests drive the real ``consume_one_job`` on both execution paths — the
autonomous path and the transactional (LOOP-scope) path — because the routing
decision is made separately in each, and an actor's contract must not depend on
which one its config selects.
"""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import CancelPhase, EnqueueArgs, JobRow
from taskq.backend.clock import Clock
from taskq.context import JobContext
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

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()

#: What the degrading actor hands back. A distinctive value so the assertion
#: can show the operator the exact payload that was discarded.
_DEGRADED_RESULT: dict[str, object] = {"processed_chunks": 7, "degraded": True}


class _TransactionalBackend(FakeBackend):
    """A backend that selects the consumer's LOOP-scope transactional path.

    ``supports_transactional_simulation`` is the flag ``consume_one_job``
    reads to decide whether the actor runs inside a caller-visible
    transaction; the ``_with_conn`` terminal writes below record what that
    path decided so the same assertions serve both paths.
    """

    BACKEND_PROTOCOL_VERSION: int = 1
    supports_transactional_simulation: bool = True

    def __init__(self) -> None:
        super().__init__()
        self.mark_succeeded_with_conn_calls: list[tuple[UUID, dict[str, object] | None]] = []

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        return make_job_row()

    async def enqueue_with_conn(self, conn: object, args: EnqueueArgs) -> JobRow:
        return await self.enqueue(args)

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: UUID,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: object = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
    ) -> bool:
        self.mark_succeeded_with_conn_calls.append((job_id, result))
        return await self.mark_succeeded(
            job_id,
            worker_id,
            result,
            progress_seq,
            progress_state,
            result_bytes=result_bytes,
            attempt=attempt,
        )


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False


class _FakeConnection:
    """An asyncpg connection stand-in for the transactional path.

    A true I/O boundary: the consumer's transactional path needs a
    connection object to open a transaction on and to issue savepoint
    statements against. Nothing about the cancel-routing decision under
    test lives here.
    """

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, *_args: object, **_kwargs: object) -> str:
        return "OK"

    async def fetchval(self, *_args: object, **_kwargs: object) -> object:
        return None


async def _degrading_actor(
    job: JobRow,
    ctx: JobContext[BaseModel],
    *,
    active_jobs: ActiveJobRegistry,
) -> dict[str, object]:
    """The shape the cancellation guide teaches.

    Observes the request, does its graceful degradation, and returns the
    work it completed. The phase is raised here rather than pre-seeded so
    the cancel lands mid-attempt, exactly as the heartbeat loop's cancel
    poll raises it while the actor body is running.
    """
    entry = active_jobs.get(job.id)
    assert entry is not None, (
        "the scenario requires the job to be registered as active so a "
        "cancel request can reach the running attempt"
    )
    entry.cancel_phase = CancelPhase.COOPERATIVE
    ctx.cancel_event.set()
    assert ctx.cancellation_requested, (
        "the scenario requires the actor to actually observe the cancel request before it degrades"
    )
    return dict(_DEGRADED_RESULT)


async def test_autonomous_actor_returning_after_observing_cancel_succeeds() -> None:
    """An autonomous actor that degrades and returns is recorded succeeded.

    The actor did its job: it noticed the request, wound down, and produced
    a result. Recording that as ``cancelled`` discards a value the worker
    is holding, and the job is terminal so nothing re-computes it.
    """
    active_jobs = ActiveJobRegistry()
    backend = FakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()

    async def actor(j: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return await _degrading_actor(j, ctx, active_jobs=active_jobs)

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        active_jobs=active_jobs,
    )

    assert backend.mark_cancelled_calls == [], (
        "a job whose actor observed the cancel, degraded gracefully and "
        "RETURNED a result was written to the database as cancelled. The "
        "result the actor computed is discarded and the job is terminal, so "
        "nothing will recompute it — a cancel arriving just before the "
        "actor's return silently destroys completed work. Cancellation is a "
        "request; the actor's outcome decides the terminal state, and this "
        "actor's outcome was a value."
    )
    assert len(backend.mark_succeeded_calls) == 1, (
        "an actor that returned a value must be recorded as succeeded; the "
        f"terminal write never reached mark_succeeded "
        f"(cancel writes: {len(backend.mark_cancelled_calls)})"
    )
    _job_id, _worker, result, _result_bytes = backend.mark_succeeded_calls[0]
    assert result == _DEGRADED_RESULT, (
        "the degraded result the actor computed must be the result stored on "
        f"the job, so callers and batches downstream can read it; got {result!r}"
    )
    assert outcome == "succeeded", (
        "the outcome reported to the consumer's caller must match the state "
        f"persisted on the job row, got {outcome!r} — a caller that acts on "
        "the returned outcome and an operator that reads the row must not "
        "see two different answers for the same attempt"
    )


async def test_transactional_actor_returning_after_observing_cancel_succeeds() -> None:
    """The same contract on the LOOP-scope transactional path.

    A transactional actor's writes and its result are one unit: if the
    actor returned, its transaction should commit and the job should be
    succeeded. Forcing the cancel branch after the actor has returned both
    discards the result and rolls back work the actor completed, and which
    of the two paths an actor runs on is a config choice it cannot see.
    """
    active_jobs = ActiveJobRegistry()
    backend = _TransactionalBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()

    async def actor(j: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return await _degrading_actor(j, ctx, active_jobs=active_jobs)

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        active_jobs=active_jobs,
        transaction_conn=_FakeConnection(),
    )

    assert backend.mark_cancelled_calls == [], (
        "on the transactional path, an actor that observed the cancel and "
        "returned a result had its transaction forced into the cancel branch "
        "after the value was already in hand. Both the result and the actor's "
        "transactional writes are discarded, and the job is terminal so "
        "neither is recomputed."
    )
    assert len(backend.mark_succeeded_with_conn_calls) == 1, (
        "the transactional success write must run inside the actor's own "
        "transaction so its writes and its result commit together; it never "
        "ran"
    )
    _job_id, result = backend.mark_succeeded_with_conn_calls[0]
    assert result == _DEGRADED_RESULT, (
        f"the degraded result must be committed with the actor's writes; got {result!r}"
    )
    assert outcome == "succeeded", (
        f"the reported outcome must match the persisted state, got {outcome!r}"
    )


async def test_actor_raising_cancelled_after_observing_cancel_is_cancelled() -> None:
    """The complement: raising, not returning, is what marks a job cancelled.

    This is the half of the contract that must not be lost while fixing the
    other half. An actor that abandons its unit of work signals that by
    raising ``CancelledError``; there is no result to keep and ``cancelled``
    is the honest terminal label. Without this pin, "the actor's outcome
    decides" could be satisfied by never cancelling at all.
    """
    active_jobs = ActiveJobRegistry()
    backend = FakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()

    async def actor(j: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        entry = active_jobs.get(j.id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        ctx.cancel_event.set()
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=active_jobs,
        )

    assert len(backend.mark_cancelled_calls) == 1, (
        "an actor that abandoned its work by raising CancelledError must be "
        "recorded cancelled — that is the signal an actor uses to say it did "
        "not finish"
    )
    assert backend.mark_succeeded_calls == [], (
        "an actor that raised produced no result, so nothing may be recorded as succeeded"
    )
