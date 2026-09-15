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
from contextlib import suppress
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
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    StubActorConfig,
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
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: object = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
    ) -> bool:
        return await self.mark_succeeded(
            job_id,
            worker_id,
            result,
            progress_seq,
            result_bytes=result_bytes,
            attempt=attempt,
        )


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
    *, transaction_conn: object | None
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
        transaction_conn=transaction_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _TxFakeConnection supplies the transaction()/execute() surface the consumer uses.
    )
    assert active_jobs.get(sibling_id) is not None
    assert active_jobs.get(job.id) is None
    return outcome, backend


async def test_autonomous_missing_own_entry_still_succeeds() -> None:
    """A populated registry without this job's entry is not a cancellation."""
    outcome, backend = await _run_with_foreign_registry_entry(transaction_conn=None)

    assert outcome == "succeeded"
    assert len(backend.mark_succeeded_calls) == 1
    assert len(backend.mark_cancelled_calls) == 0


async def test_transactional_missing_own_entry_still_succeeds() -> None:
    """Same guard, same shape, on the LOOP-scope transactional path."""
    outcome, backend = await _run_with_foreign_registry_entry(transaction_conn=_TxFakeConnection())

    assert outcome == "succeeded"
    assert len(backend.mark_succeeded_calls) == 1
    assert len(backend.mark_cancelled_calls) == 0


# ── terminal write retires the buffer exactly once ───────────────────


async def _succeed_with_dirty_buffer(
    *, transaction_conn: object | None
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
        transaction_conn=transaction_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _TxFakeConnection supplies the transaction()/execute() surface the consumer uses.
    )
    return outcome, backend, captured[0], job.id


async def _assert_buffer_retired(transaction_conn: object | None) -> None:
    """After a success the buffer is clean, so a later flush writes nothing.

    ``buf.dirty is False`` alone says only that the flag was assigned; the
    follow-up flush is what distinguishes "flushed once" from "will flush
    again", which is a duplicate progress write against a finished job.
    """
    outcome, backend, buf, job_id = await _succeed_with_dirty_buffer(
        transaction_conn=transaction_conn
    )

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
    """Drive the consumer's cancel-write path (an actor that abandons by
    raising ``CancelledError``) and return the recorded write.

    The actor signals abandonment by raising, never by returning — an actor
    that returns under a cancel request has succeeded and takes the success
    path instead (that contract lives in
    ``tests/test_cooperative_cancel_outcome.py``).
    """
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
        raise asyncio.CancelledError

    with suppress(asyncio.CancelledError):
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


# ── a fenced terminal write must not silently commit ─────────────────


class _RecordingTxConnection(_TxFakeConnection):
    """Like _TxFakeConnection but records every statement on the connection.

    Recording the SQL text is what lets a test distinguish a savepoint
    RELEASE (the actor's writes join the outer commit) from a ROLLBACK,
    and observe whether the actor's own writes on the injected
    LOOP-scope connection went out at all.
    """

    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.statements.append((query, args))
        return ""


class _FencedTxFakeBackend(_TxFakeBackend):
    """Simulates the fencing UPDATE matching zero rows.

    Mirrors ``_mark_succeeded_on_conn``: when the fenced UPDATE finds no
    matching row (lease reclaimed, wrong attempt, wrong worker) the real
    backend returns ``False`` with no exception and no state-change log,
    so the boolean is the only signal the caller gets.
    """

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
        self.mark_succeeded_calls.append((job_id, worker_id, result, result_bytes))
        return False


async def test_fenced_terminal_write_must_not_report_succeeded() -> None:
    """A fenced-out terminal write rolls back the actor and reports no success.

    On the transactional path the actor's own writes and the terminal
    write share one transaction, and the guarantee the worker guide sells
    is that they commit together. ``mark_succeeded_with_conn`` fences its
    UPDATE on the job's id, running status, holding worker and attempt
    number, and returns ``False`` when that clause matches nothing: the
    lease expired, the job was re-pended and re-claimed, and this stale
    handler's write arrived too late.

    That ``False`` must not be treated as a commit. Releasing the
    savepoint anyway lets the actor's side effects join the outer
    transaction's commit even though the job row never transitioned to
    succeeded under this attempt, and reporting ``"succeeded"`` tells the
    caller the job ran exactly once. The row is meanwhile pending or
    running under a later attempt, so the work runs a second time with
    the stale attempt's writes already durable. The rejected write has to
    roll the actor back and surface as something other than success.
    """
    backend = _FencedTxFakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    tx_conn = _RecordingTxConnection()

    actor_side_effect_ran = False

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        nonlocal actor_side_effect_ran
        # The actor's own write on the injected LOOP-scope connection, for
        # instance an INSERT into an application table, issued inside the
        # same transaction as the about-to-be-fenced terminal write.
        await tx_conn.execute("INSERT INTO side_effects (job_id) VALUES ($1)", running.id)
        actor_side_effect_ran = True
        return {"ok": True}

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        transaction_conn=tx_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _RecordingTxConnection supplies the transaction()/execute() surface the consumer uses.
    )

    assert actor_side_effect_ran
    assert len(backend.mark_succeeded_calls) == 1

    # The fenced write reported False (no matching row), so the actor's
    # writes on transaction_conn must not have been allowed to commit via
    # a savepoint release, and the outcome must not be "succeeded".
    released = any("RELEASE SAVEPOINT _tq_actor" in q for q, _ in tx_conn.statements)
    assert not released, (
        "the actor's writes were committed (savepoint released) even though "
        "the fenced terminal write matched no row, so side effects from a "
        "stale attempt become durable for a job that never transitioned to "
        "succeeded under that attempt"
    )
    assert outcome != "succeeded", (
        f"consume_one_job reported {outcome!r} for a fenced (zero-row) terminal "
        "write: the caller cannot distinguish this from a real success, so the "
        "job will be re-run at a later attempt with this attempt's actor side "
        "effects already durable"
    )


# ── the autonomous path owes the same fencing respect ────────────────


class _RecordingRedis:
    """redis.asyncio.Redis stand-in recording every published event.

    ``_publish_state_change_event`` pipelines its two PUBLISH commands and
    swallows any exception, so a stub missing ``pipeline`` would look like
    a silent success. This one supplies both surfaces.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    class _Pipe:
        def __init__(self, owner: "_RecordingRedis") -> None:
            self._owner = owner

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        def publish(self, channel: str, payload: str) -> None:
            self._owner.published.append((channel, payload))

        async def execute(self) -> list[int]:
            return [1, 1]

    def pipeline(self, transaction: bool = True) -> "_RecordingRedis._Pipe":
        return self._Pipe(self)

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append((channel, payload))
        return 1


class _FencedAutonomousBackend(FakeBackend):
    """Autonomous success write fenced out: the UPDATE matched no row.

    The no-transaction dispatch path calls ``mark_succeeded`` rather than
    ``mark_succeeded_with_conn``, and the real backend signals a fenced
    write the same way there: ``False``, no exception.
    """

    async def mark_succeeded(
        self,
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
        self.mark_succeeded_calls.append((job_id, worker_id, result, result_bytes))
        return False


class _FencedCancelBackend(FakeBackend):
    """Cooperative-cancel write fenced out: the UPDATE matched no row."""

    async def mark_cancelled(
        self,
        job_id: UUID,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
    ) -> bool:
        self.mark_cancelled_calls.append(
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "progress_seq": progress_seq,
                "progress_state": progress_state,
            }
        )
        return False


async def test_fenced_autonomous_success_does_not_invoke_on_success() -> None:
    """A fenced-out success write must not fire the on_success hook.

    ``mark_succeeded`` returns ``False`` when its fenced UPDATE (job id,
    running status, holding worker, attempt number) matched no row, which
    happens when the lock lease expired, the sweep re-pended the job, and
    another worker claimed it under a new attempt before this stale
    handler's write landed. The row never transitioned to succeeded under
    this attempt, so this attempt did not succeed.

    ``on_success`` is a user hook that emails a customer, fires a webhook
    or charges a payment. Running it for an attempt that lost the row
    means it runs twice: once here, spuriously, and once more for the
    attempt that actually wins the row.
    """
    backend = _FencedAutonomousBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()

    on_success_calls: list[tuple[JobRow, object]] = []

    def on_success(completed_job: JobRow, result: object) -> None:
        on_success_calls.append((completed_job, result))

    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=on_success,
    )

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=actor_config,
        payload_type=EmptyPayload,
        clock=clock,
        transaction_conn=None,
    )

    assert len(backend.mark_succeeded_calls) == 1, "fixture broken: success write not attempted"
    assert on_success_calls == [], (
        "on_success fired for a fenced-out (False) success write, so a hook "
        "with external side effects runs for an attempt whose row was "
        "reclaimed under a new attempt, and again when that attempt finishes"
    )


async def test_fenced_autonomous_success_is_not_reported_as_succeeded() -> None:
    """The autonomous path must not claim success when its write no-opped.

    The outcome string is what the caller, including the batch completion
    hook, records. Reporting ``"succeeded"`` for a write that matched no
    row hands the caller a result the database never agreed to.
    """
    backend = _FencedAutonomousBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        transaction_conn=None,
    )

    assert outcome != "succeeded", (
        f"outcome was {outcome!r} for a fenced-out (False) success write: the "
        "row was never transitioned to succeeded under this attempt, so the "
        "caller is told a job succeeded that is still pending or running "
        "elsewhere"
    )


async def test_fenced_cancel_write_does_not_publish_a_terminal_event() -> None:
    """A fenced-out cancel write must not announce the job cancelled.

    ``mark_cancelled`` is fenced the same way the success write is and
    returns ``False`` on the same reclaim race. The retry and failure
    handler already gates its publish on the write's own result, refusing
    to announce a move the row never made. The consumer's cancel branch —
    reached when the actor abandons its unit of work by raising
    ``CancelledError`` — owes subscribers the same honesty: a
    ``terminal=True`` ``status="cancelled"`` event for a row that another
    worker is still running tells every progress subscriber, and anything
    downstream of the stream, that the job is finished when it is not.
    """
    active_jobs = ActiveJobRegistry()
    backend = _FencedCancelBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    redis_client = _RecordingRedis()

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        entry = active_jobs.get(running.id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        raise asyncio.CancelledError

    with suppress(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=active_jobs,
            redis_client=redis_client,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed redis.asyncio.Redis; _RecordingRedis supplies the pipeline()/publish() surface the publisher uses.
            settings=_settings(),
            transaction_conn=None,
        )

    assert len(backend.mark_cancelled_calls) == 1, "fixture broken: cancel write not attempted"
    terminal_cancels = [
        payload for _, payload in redis_client.published if '"status":"cancelled"' in payload
    ]
    assert terminal_cancels == [], (
        "a terminal cancelled event was published even though the fenced "
        "cancel write matched no row, so subscribers see a job announced "
        "finished while another worker is still running it under a new attempt"
    )


# ── the transactional path owes the same fencing respect ─────────────


async def test_fenced_transactional_success_does_not_invoke_on_success() -> None:
    """The per-slot transactional path must not fire on_success when fenced.

    The transactional consumer runs the actor and the terminal write
    inside one slot-scoped transaction and then, after the shield
    resolves, invokes ``on_success`` unconditionally. But
    ``mark_succeeded_with_conn`` fences its UPDATE on the job's id,
    running status, holding worker and attempt number and returns
    ``False`` when nothing matched — the lease expired, the row was
    re-pended and re-claimed elsewhere.

    ``on_success`` is user code with external reach: a webhook, a
    customer email, a ledger write. Firing it for an attempt the database
    refused means it fires twice, once here for an attempt that lost the
    row and once for the attempt that wins it. The autonomous path
    already withholds the hook on a fenced write; the transactional path
    is the same class of call site and must withhold it identically.
    """
    backend = _FencedTxFakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    tx_conn = _RecordingTxConnection()

    on_success_calls: list[tuple[JobRow, object]] = []

    def on_success(completed_job: JobRow, result: object) -> None:
        on_success_calls.append((completed_job, result))

    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=on_success,
    )

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=actor_config,
        payload_type=EmptyPayload,
        clock=clock,
        transaction_conn=tx_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _RecordingTxConnection supplies the transaction()/execute() surface the consumer uses.
    )

    assert len(backend.mark_succeeded_calls) == 1, "fixture broken: success write not attempted"
    assert on_success_calls == [], (
        "on_success fired on the transactional path for a fenced-out (False) "
        "success write, so a hook with external side effects runs for an "
        "attempt whose row was reclaimed under a new attempt"
    )


async def test_fenced_transactional_success_does_not_publish_a_terminal_event() -> None:
    """A fenced transactional success must not announce the job finished.

    The ``terminal=True status="succeeded"`` publish is what every
    progress subscriber and anything downstream of the stream treats as
    "this job is done". Emitting it for a write the fence rejected
    reports a completion the row never made, while another attempt is
    still running the same work.
    """
    backend = _FencedTxFakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    tx_conn = _RecordingTxConnection()
    redis_client = _RecordingRedis()

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        redis_client=redis_client,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed redis.asyncio.Redis; _RecordingRedis supplies the pipeline()/publish() surface the publisher uses.
        settings=_settings(),
        transaction_conn=tx_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _RecordingTxConnection supplies the transaction()/execute() surface the consumer uses.
    )

    assert len(backend.mark_succeeded_calls) == 1, "fixture broken: success write not attempted"
    terminal_successes = [
        payload for _, payload in redis_client.published if '"status":"succeeded"' in payload
    ]
    assert terminal_successes == [], (
        "a terminal succeeded event was published even though the fenced "
        "transactional terminal write matched no row, so subscribers are told "
        "the job finished while another attempt is still running it"
    )


class _FencedTxCancelBackend(_TxFakeBackend):
    """Cooperative-cancel write fenced out on the transactional path."""

    async def mark_cancelled(
        self,
        job_id: UUID,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
    ) -> bool:
        self.mark_cancelled_calls.append(
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "progress_seq": progress_seq,
                "progress_state": progress_state,
            }
        )
        return False


async def test_fenced_transactional_cancel_does_not_publish_a_terminal_event() -> None:
    """The transactional cancel branch must respect its fence too.

    When an actor abandons its unit of work on a slot-scoped transaction
    (raises ``CancelledError``), the transaction rolls back and the
    consumer's cancel handler writes ``mark_cancelled``. That write is
    fenced exactly like the success write and returns ``False`` on the
    same reclaim race. The terminal ``cancelled`` publish must be gated
    on the write's own result: announcing a cancellation the row never
    took strands every subscriber on a job that is still running
    elsewhere.
    """
    active_jobs = ActiveJobRegistry()
    backend = _FencedTxCancelBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    tx_conn = _RecordingTxConnection()
    redis_client = _RecordingRedis()

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        entry = active_jobs.get(running.id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        raise asyncio.CancelledError

    with suppress(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=active_jobs,
            redis_client=redis_client,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed redis.asyncio.Redis; _RecordingRedis supplies the pipeline()/publish() surface the publisher uses.
            settings=_settings(),
            transaction_conn=tx_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _RecordingTxConnection supplies the transaction()/execute() surface the consumer uses.
        )

    assert len(backend.mark_cancelled_calls) == 1, "fixture broken: cancel write not attempted"
    terminal_cancels = [
        payload for _, payload in redis_client.published if '"status":"cancelled"' in payload
    ]
    assert terminal_cancels == [], (
        "a terminal cancelled event was published on the transactional path "
        "even though the fenced cancel write matched no row"
    )


# ── the batch counters must not move for a write that matched nothing ─


async def test_fenced_success_reports_the_outcome_batch_policy_ignores() -> None:
    """A fenced-out success reports ``noop`` so no batch counter budges.

    ``apply_batch_terminal_outcome`` is driven purely by the outcome
    string the consumer returns. It already treats ``"noop"`` — "a
    terminal write that matched nothing, the job was never this
    dispatch's to move" — as non-terminal and returns before touching a
    single counter. The fenced success path must produce exactly that
    value.

    Any other value is load-bearing damage: ``"succeeded"`` resets the
    consecutive-failure streak and fires a completion attempt, and
    ``"failed"`` increments toward the abort threshold — both on behalf
    of a job that is still pending or running under a later attempt. The
    batch would complete, and its on-finish hook fire, before its members
    actually finished.
    """
    backend = _FencedAutonomousBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        transaction_conn=None,
    )

    assert outcome == "noop", (
        f"outcome was {outcome!r} for a fenced-out success write; batch policy "
        "keys off this string alone, and every value other than 'noop' moves a "
        "batch counter or fires a completion attempt for a job this dispatch "
        "never terminated"
    )


async def test_fenced_transactional_success_reports_the_outcome_batch_policy_ignores() -> None:
    """The transactional path reports ``noop`` for a fenced success too.

    Per-slot transactions run the same batch hook off the same outcome
    string, so the transactional path owes the same value. Reporting
    anything terminal here would complete a batch whose member never
    committed.
    """
    backend = _FencedTxFakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    tx_conn = _RecordingTxConnection()

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
        transaction_conn=tx_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; _RecordingTxConnection supplies the transaction()/execute() surface the consumer uses.
    )

    assert outcome == "noop", (
        f"outcome was {outcome!r} for a fenced-out transactional success write; "
        "batch policy keys off this string alone and would move a counter for a "
        "job this slot's transaction never committed"
    )


# ── same-worker re-claim at a newer attempt ──────────────────────────


class _AttemptFencedBackend(FakeBackend):
    """Fences the success write on the attempt epoch, not the worker id.

    The same-worker reclaim shape: the sweep re-pended the job and THIS
    worker claimed it again at a later attempt. ``locked_by_worker`` is
    unchanged, so the ownership half of the fence passes; only the
    attempt conjunct rejects the stale handler's write. The backend
    reports that the way the real one does — ``False``, no exception.
    """

    def __init__(self, *, live_attempt: int) -> None:
        super().__init__()
        self._live_attempt = live_attempt

    async def mark_succeeded(
        self,
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
        self.mark_succeeded_calls.append((job_id, worker_id, result, result_bytes))
        return attempt == self._live_attempt


async def test_stale_attempt_on_the_same_worker_is_not_reported_as_succeeded() -> None:
    """A same-worker re-claim at a newer attempt fences the stale handler.

    The worker fence alone cannot separate attempt N's suspended handler
    from attempt N+1's live one when the same worker re-claims the job
    after a lease-expiry sweep — both present the same ``locked_by_worker``.
    Only the attempt conjunct rejects the stale write, and it reports that
    rejection as a plain ``False``.

    This is the path most easily missed, because nothing about it looks
    like a hand-off: one process, one worker id, one job. If the consumer
    reads that ``False`` as success, the older handler's result overwrites
    nothing but is nonetheless reported as a completion, its hooks fire,
    and the live attempt runs the work a second time.
    """
    backend = _AttemptFencedBackend(live_attempt=2)
    clock: Clock = FakeClock(_NOW)
    # The handler is still carrying attempt 1; the row moved on to 2.
    job = make_job_row(attempt=1)

    on_success_calls: list[tuple[JobRow, object]] = []

    def on_success(completed_job: JobRow, result: object) -> None:
        on_success_calls.append((completed_job, result))

    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=on_success,
    )

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=actor_config,
        payload_type=EmptyPayload,
        clock=clock,
        transaction_conn=None,
    )

    assert len(backend.mark_succeeded_calls) == 1, "fixture broken: success write not attempted"
    assert outcome == "noop", (
        f"outcome was {outcome!r} for a write fenced out by the attempt epoch "
        "on the same worker: the row is running under a newer attempt and this "
        "dispatch terminated nothing"
    )
    assert on_success_calls == [], (
        "on_success fired for a stale attempt on the same worker, so the hook "
        "runs once here and again when the live attempt finishes"
    )


# ── the class is closed, not the instance ────────────────────────────


def test_no_consumer_terminal_write_discards_its_fenced_outcome() -> None:
    """Every fenced terminal write in the consumer consumes its result.

    The fencing guarantee is only as strong as its weakest call site: one
    ``await backend.mark_succeeded(...)`` written as a bare statement
    silently throws away the boolean that says whether the row actually
    moved, and restores the at-least-twice execution the fence exists to
    prevent. Behavioural tests can only cover the paths someone thought
    to write; this one closes the class by reading the source.

    A call is considered to consume its outcome when the await is bound
    (assigned to a name, or tested in a condition) rather than issued as
    a standalone expression statement. ``shield_with_retrieval`` wrapping
    is not by itself consumption — it protects the write from external
    cancellation and returns the same boolean, which the caller must
    still read.
    """
    import ast
    import inspect

    import taskq.worker._consumer as consumer_module

    fenced_writes = {
        "mark_succeeded",
        "mark_succeeded_with_conn",
        "mark_cancelled",
        "mark_cancelled_with_conn",
    }

    source = inspect.getsource(consumer_module)
    tree = ast.parse(source)

    def called_name(node: ast.AST) -> str | None:
        """The attribute name of a backend method call inside *node*."""
        for inner in ast.walk(node):
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr in fenced_writes
            ):
                return inner.func.attr
        return None

    discarding: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        # A bare expression statement: the value is computed and dropped.
        if isinstance(node, ast.Expr):
            name = called_name(node)
            if name is not None:
                discarding.append((name, node.lineno))

    assert discarding == [], (
        "these fenced terminal writes in taskq.worker._consumer discard the "
        "boolean that says whether the row actually moved, so a write that "
        "matched no row is indistinguishable from one that landed: "
        + ", ".join(f"{name} at line {lineno}" for name, lineno in discarding)
    )
