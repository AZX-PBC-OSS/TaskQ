"""A shutdown release is held back until the interrupted actor has provably
exited.

``task.cancel()`` on a sync actor cancels the *await* on the executor
thread, never the thread: the consumer's cancellation handler used to write
``mark_interrupted(hold=timedelta(0))`` on the assumption the actor had
unwound with the await, so the row went straight back to ``pending``:
claimable by another worker while the thread was still mid-body. The
transactional path had the same gap: ``tx_task.cancel()`` followed by an
immediate re-raise released the row while the transaction task was still
unwinding its rollback.

This is the unit tier (no Postgres): the tracked sync-actor handle
(``_run_sync_actor_tracked``, the production dispatch helper), the bounded
exit park, and the hold the release carries, all driven through the
production ``consume_one_job`` cancellation arm against the recording
``FakeBackend``. The real-Postgres gate: a second worker's claim while the
first worker's sync thread is provably still running: lives in
``tests/test_shutdown_interrupts_running_work.py``.

The companion fix for the first half is pinned here too: an infra-failed
release write must propagate the *cancellation* out of ``consume_one_job``,
never the infra error: a bare ``raise`` in that except arm replaced the
CancelledError with the InterfaceError and dispatch's generic handler spent
the attempt's budget on a deploy.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable, Iterator
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Literal, Self
from unittest.mock import MagicMock
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import CancelPhase, JobRow
from taskq.backend.clock import Clock
from taskq.context import CancelOrigin, JobContext
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload, FakeBackend, as_backend, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker import (
    _handlers as _handlers_mod,  # pyright: ignore[reportPrivateUsage]  # Why: the retry backoff is monkeypatched to keep the infra-failure test inside the unit lane's time budget.
)
from taskq.worker._consumer import (
    _actor_exit_wait_budget,  # pyright: ignore[reportPrivateUsage]  # Why: the park-budget function under test: the anchored shape had no unit pin (F7).
    _interrupted_actor_hold,  # pyright: ignore[reportPrivateUsage]  # Why: the bare-call arm's fallback hold is a defensive surface a refactor could unbound or zero.
    consume_one_job,
)
from taskq.worker._handlers import (  # pyright: ignore[reportPrivateUsage]  # Why: the release write's own budget: the park's reserve term, pinned against the canonical constant.
    _TERMINAL_WRITE_BUDGET,
)
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import (  # pyright: ignore[reportPrivateUsage]  # Why: the production sync-actor dispatch helper: the tracking under test is exactly what dispatch_one_job calls for a sync actor.
    _run_sync_actor_tracked,
)
from taskq.worker.shutdown import (  # pyright: ignore[reportPrivateUsage]  # Why: the hold math under test is the module's own; the test recomputes the expected hold from the same exit tail.
    _watchdog_exit_tail,
)

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


@pytest.fixture(autouse=True)
def _clean_tracked_actor_handles() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Isolate the process-wide tracked-handle registry per test.

    The registry is module state shared with every other module in the
    session; a handle leaked by a failed assertion here would park a
    later ``await_tracked_actor_reap`` forever (the watchdog is its only
    bound, and unit tests do not arm one).
    """
    from taskq.worker import _watchdog as _watchdog_mod

    saved = set(_watchdog_mod._tracked_actor_handles)  # pyright: ignore[reportPrivateUsage]  # Why: test isolation of the module-level registry under assertion.
    _watchdog_mod._tracked_actor_handles.clear()
    try:
        yield
    finally:
        _watchdog_mod._tracked_actor_handles.clear()
        _watchdog_mod._tracked_actor_handles.update(saved)


def _settings(*, cleanup_grace: float = 0.4, termination_grace: float = 20.0) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": str(cleanup_grace),
            "TASKQ_TERMINATION_GRACE_PERIOD": str(termination_grace),
        }
    )


# ── Test doubles ─────────────────────────────────────────────────────────


class _InfraFailingReleaseBackend(FakeBackend):
    """Every mark_interrupted attempt fails with the pool-lifecycle family.

    The class asyncpg raises from a bounded pool close (worker teardown):
    the exact infra error the arm must swallow so the cancellation,
    not the InterfaceError, escapes the consumer.
    """

    async def mark_interrupted(
        self,
        job_id: UUID,
        worker_id: UUID,
        *,
        attempt: int,
        claim_epoch: int | None = None,
        hold: timedelta,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> Literal["pending", "scheduled", "failed:DeadlineExceeded", "noop"]:
        raise asyncpg.InterfaceError("pool is closing")


class _ParkedTxConnection:
    """asyncpg.Connection stand-in whose transaction unwind can be parked.

    The transactional path's unwind: the ``async with transaction()``
    exit that rolls back: is the thing the release used to race (the
    second mechanism). Parking it lets the test hold the unwind open past
    the cancel and observe the ordering: the release write must land only
    after ``rollback_done``.
    """

    def __init__(self) -> None:
        self.events: list[str] = []
        self.unwind_gate: asyncio.Event = asyncio.Event()

    class _Transaction:
        def __init__(self, outer: _ParkedTxConnection) -> None:
            self._outer = outer

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            self._outer.events.append("rollback_entered")
            await self._outer.unwind_gate.wait()
            self._outer.events.append("rollback_done")

    def transaction(self) -> _ParkedTxConnection._Transaction:
        return self._Transaction(self)

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _EventRecordingReleaseBackend(FakeBackend):
    """FakeBackend that appends to the tx test's ordering log on release."""

    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    async def mark_interrupted(
        self,
        job_id: UUID,
        worker_id: UUID,
        *,
        attempt: int,
        claim_epoch: int | None = None,
        hold: timedelta,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> Literal["pending", "scheduled", "failed:DeadlineExceeded", "noop"]:
        self._events.append("release_write")
        return await super().mark_interrupted(
            job_id,
            worker_id,
            attempt=attempt,
            hold=hold,
            progress_seq=progress_seq,
            progress_state=progress_state,
        )


# ── Harness ──────────────────────────────────────────────────────────────


def _deps_with(settings: WorkerSettings) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.settings = settings
    deps.disowned_jobs = set()
    deps.progress_buffers = {}
    deps.worker_pool = None
    deps.redis_client = None
    deps.shutdown_started_at = None
    return deps


async def _stamp_shutdown_origin(registry: ActiveJobRegistry, job_id: UUID) -> None:
    """Stamp a job's registry entry the way the orchestrator's CANCELLING
    phase does: origin SHUTDOWN, phase FORCED (FORCING's task.cancel() is
    what delivers the cancellation under test)."""
    entry = registry.get(job_id)
    assert entry is not None, "the attempt must be registered before it can be cancelled"
    entry.cancel_origin = CancelOrigin.SHUTDOWN
    entry.ctx._set_cancel_origin(CancelOrigin.SHUTDOWN)  # pyright: ignore[reportPrivateUsage]  # Why: the test stamps the context the same way the orchestrator does alongside cancel_event.set().
    entry.cancel_phase = CancelPhase.FORCED


def _tracked_sync_run_actor(
    body: Callable[..., object],
    captured_ctx: list[JobContext[EmptyPayload]] | None = None,
) -> Callable[[JobRow, JobContext[EmptyPayload]], Awaitable[object]]:
    """Wrap a sync body in the production tracked dispatch helper.

    *captured_ctx* optionally records the per-attempt context so a test can
    reach the tracked thread handle after the attempt task has ended (the
    registry entry is deregistered by then).
    """

    async def run_actor(job_row: JobRow, ctx: JobContext[EmptyPayload]) -> object:
        del job_row
        if captured_ctx is not None:
            captured_ctx.append(ctx)
        return await _run_sync_actor_tracked(body, {"payload": ctx.payload}, ctx)

    return run_actor


# ── The tracked sync-actor handle ────────────────────────────────────────


async def test_a_cancelled_sync_actor_await_detaches_but_the_thread_is_tracked() -> None:
    """Cancelling the await does not stop the thread, and now it is provable.

    The tracked helper keeps ``asyncio.to_thread``'s semantics (default
    executor, context copy, the thread runs to its own outcome) while
    leaving the handle on the ctx: the await ends cancelled the moment the
    consumer is cancelled, the handle ends only when the body returns.
    Before the handle existed the two were indistinguishable from the
    consumer's side, which is exactly what hold=0 assumed away.
    """
    body_started = threading.Event()
    release_body = threading.Event()
    body_returned = threading.Event()

    def body() -> object:
        body_started.set()
        release_body.wait(10.0)
        body_returned.set()
        return {"done": True}

    class _Ctx:
        _sync_actor_task: asyncio.Task[object] | None = None

        def _set_sync_actor_task(self, task: asyncio.Task[object]) -> None:
            self._sync_actor_task = task

    ctx = _Ctx()

    awaitable = _run_sync_actor_tracked(body, {}, ctx)  # type: ignore[arg-type]  # Why: the minimal ctx double carries just the setter contract the helper uses.
    runner = asyncio.ensure_future(awaitable)
    await asyncio.to_thread(body_started.wait, 10.0)

    runner.cancel()
    with suppress(asyncio.CancelledError):
        await runner

    handle = ctx._sync_actor_task
    assert handle is not None, "the dispatch helper must record the thread handle on the ctx"
    assert not handle.done(), (
        "the thread was gated, so its handle must still be live after the "
        "await was cancelled - the actor body has NOT exited"
    )
    assert not body_returned.is_set()

    release_body.set()
    done, _pending = await asyncio.wait({handle}, timeout=10.0)
    assert handle in done and body_returned.is_set(), (
        "the handle completes exactly when the thread's body returns: that "
        "is the provable-exit signal the shutdown release parks on"
    )


# ── The consumer's release hold ──────────────────────────────────────────


async def test_a_live_sync_actor_is_released_behind_a_hold_not_pending() -> None:
    """The sync actor still running at the forced cancel is released HELD.

    This is the regression cell: on the old arm the release carried
    hold=0 on the false assumption the actor unwound with the await, so the
    row landed pending and a second worker could claim it while the thread
    ran on. The hold is now the process's exit window (the unanchored
    defensive shape: the full termination budget plus the watchdog's exit
    tail), and the row stays unclaimable until the process is provably
    gone.
    """
    settings = _settings()
    deps = _deps_with(settings)
    backend = FakeBackend()
    registry = ActiveJobRegistry()
    job = make_job_row()
    clock: Clock = FakeClock(_NOW)

    body_started = threading.Event()
    release_body = threading.Event()

    def body(payload: EmptyPayload) -> object:
        del payload
        body_started.set()
        release_body.wait(30.0)
        return {"done": True}

    captured_ctx: list[JobContext[EmptyPayload]] = []
    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=_tracked_sync_run_actor(body, captured_ctx),
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=registry,
        )
    )
    await asyncio.to_thread(body_started.wait, 10.0)
    await _stamp_shutdown_origin(registry, job.id)

    attempt.cancel()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=10.0)

    assert len(backend.mark_interrupted_calls) == 1, (
        "the shutdown-origin cancellation must release the attempt back to "
        "the fleet - one mark_interrupted, never a mark_cancelled"
    )
    release = backend.mark_interrupted_calls[0]
    expected_hold = timedelta(seconds=20.0 + _watchdog_exit_tail(settings))
    assert release["hold"] == expected_hold, (
        "an actor that has not provably exited must be released behind this "
        "process's exit window (full budget + watchdog exit tail in the "
        f"unanchored shape), not hold=0; got {release['hold']}"
    )
    assert backend.mark_cancelled_calls == []

    # The row's release happened while the thread was provably still alive.
    assert not release_body.is_set()
    release_body.set()
    handle = captured_ctx[0]._sync_actor_task  # pyright: ignore[reportPrivateUsage]  # Why: the test reads the dispatch layer's handle to join the executor thread before the test ends.
    assert handle is not None
    done, _pending = await asyncio.wait({handle}, timeout=10.0)
    assert handle in done, "the gated body returns once released; the thread joins"


async def test_a_sync_actor_exiting_inside_the_window_is_released_pending_hold_zero() -> None:
    """The bounded park earns hold=0 when the actor exits within it.

    Waiting is the point of the park: a sync actor that is merely slow (not
    unbounded) finishes inside the remaining budget, its handle completes,
    and the row is released claimable immediately: the actor is provably
    gone, so no hold is owed. A fix that skipped the wait and always held
    would turn every deploy-interrupted sync actor into ~a-termination-
    budget of dead latency.
    """
    settings = _settings(cleanup_grace=1.0)
    deps = _deps_with(settings)
    backend = FakeBackend()
    registry = ActiveJobRegistry()
    job = make_job_row()
    clock: Clock = FakeClock(_NOW)

    body_started = threading.Event()
    release_body = threading.Event()

    def body(payload: EmptyPayload) -> object:
        del payload
        body_started.set()
        release_body.wait(30.0)
        return {"done": True}

    captured_ctx: list[JobContext[EmptyPayload]] = []
    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=_tracked_sync_run_actor(body, captured_ctx),
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=registry,
        )
    )
    await asyncio.to_thread(body_started.wait, 10.0)
    await _stamp_shutdown_origin(registry, job.id)

    attempt.cancel()
    # The body exits shortly after the cancel: inside the 1.0s park window.
    await asyncio.sleep(0.05)
    release_body.set()

    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=10.0)

    assert len(backend.mark_interrupted_calls) == 1
    release = backend.mark_interrupted_calls[0]
    assert release["hold"] == timedelta(0), (
        "an actor that provably exited inside the bounded park is gone: the "
        "row is genuinely free and the release must land pending "
        f"(hold=0), got {release['hold']}"
    )
    handle = captured_ctx[0]._sync_actor_task  # pyright: ignore[reportPrivateUsage]  # Why: joining the executor thread before the test ends.
    assert handle is not None and handle.done()


async def test_an_async_actor_that_unwound_keeps_the_immediate_release() -> None:
    """The responsive-async fast path is unchanged: hold=0, no park.

    An async actor has unwound by the time the cancellation handler runs:
    the CancelledError propagated through its frames to get there. No
    handles exist, so no park and no hold: the row goes straight back to
    the fleet, exactly as before the fix.
    """
    settings = _settings()
    deps = _deps_with(settings)
    backend = FakeBackend()
    registry = ActiveJobRegistry()
    job = make_job_row()
    clock: Clock = FakeClock(_NOW)

    async def body(running: JobRow, ctx: JobContext[BaseModel]) -> object:
        del running, ctx
        await asyncio.sleep(3600)
        return {"unreachable": True}

    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=registry,
        )
    )
    await asyncio.sleep(0.05)
    await _stamp_shutdown_origin(registry, job.id)

    attempt.cancel()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=10.0)

    assert len(backend.mark_interrupted_calls) == 1
    assert backend.mark_interrupted_calls[0]["hold"] == timedelta(0), (
        "an unwound async actor is provably gone; the immediate pending "
        "release is the behaviour this fix must preserve, not regress"
    )


async def test_a_cancel_landing_on_the_park_still_releases_with_the_hold() -> None:
    """A second cancellation ends the waiting, never the release.

    The TaskGroup teardown after RELEASING cancels the parked consumer; the
    park must treat that as "stop waiting", write the release with the full
    hold, and only then let the cancellation propagate. Swallowing the
    interrupt inside the park is what keeps the row from being stranded
    running behind a dying lease when the group wants to exit.
    """
    settings = _settings(cleanup_grace=1.0)
    deps = _deps_with(settings)
    backend = FakeBackend()
    registry = ActiveJobRegistry()
    job = make_job_row()
    clock: Clock = FakeClock(_NOW)

    body_started = threading.Event()
    release_body = threading.Event()

    def body(payload: EmptyPayload) -> object:
        del payload
        body_started.set()
        release_body.wait(30.0)
        return {"done": True}

    captured_ctx: list[JobContext[EmptyPayload]] = []
    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=_tracked_sync_run_actor(body, captured_ctx),
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=registry,
        )
    )
    await asyncio.to_thread(body_started.wait, 10.0)
    await _stamp_shutdown_origin(registry, job.id)

    attempt.cancel()
    # The forced cancel parked the consumer; a second cancel (the group
    # teardown's shape) lands mid-park.
    await asyncio.sleep(0.05)
    attempt.cancel()

    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=10.0)

    assert len(backend.mark_interrupted_calls) == 1, (
        "a cancellation landing on the exit park must not discard the "
        "release - the row would stay running behind a dying lease"
    )
    release = backend.mark_interrupted_calls[0]
    assert release["hold"] == timedelta(seconds=20.0 + _watchdog_exit_tail(settings)), (
        "the interrupted park must still release behind the process's exit "
        f"window; got {release['hold']}"
    )
    release_body.set()
    handle = captured_ctx[0]._sync_actor_task  # pyright: ignore[reportPrivateUsage]  # Why: joining the executor thread before the test ends.
    assert handle is not None
    done, _pending = await asyncio.wait({handle}, timeout=10.0)
    assert handle in done


# ── The transactional unwind ─────────────────────────────────────────────


async def test_the_transactional_release_lands_only_after_the_unwind() -> None:
    """The tx task's unwind is awaited (bounded) before the row is released.

    The old path cancelled ``tx_task`` and re-raised without waiting, so
    ``mark_interrupted`` raced the rollback. Here the rollback is parked
    open: the release write must observe it finished first, and with the
    unwind completing inside the park window, the release is hold=0 (the
    actor is provably gone with its transaction).
    """
    settings = _settings(cleanup_grace=2.0)
    deps = _deps_with(settings)
    tx_conn = _ParkedTxConnection()
    backend = _EventRecordingReleaseBackend(tx_conn.events)
    registry = ActiveJobRegistry()
    job = make_job_row()
    clock: Clock = FakeClock(_NOW)

    async def body(running: JobRow, ctx: JobContext[BaseModel]) -> object:
        del running, ctx
        await asyncio.sleep(3600)
        return {"unreachable": True}

    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=registry,
            transaction_conn=tx_conn,  # pyright: ignore[reportArgumentType]  # Why: the parameter is typed asyncpg.Connection; the stand-in supplies the transaction()/execute() surface the transactional consumer uses.
        )
    )
    await asyncio.sleep(0.05)
    await _stamp_shutdown_origin(registry, job.id)

    attempt.cancel()
    await asyncio.sleep(0.1)
    # The unwind (the parked rollback) is what the consumer is now waiting
    # on: the release write has not happened yet.
    assert "release_write" not in tx_conn.events, (
        "the release must not land while the transaction task is still "
        "unwinding - the consumer parks on the unwind handle first"
    )
    assert "rollback_entered" in tx_conn.events, (
        "the cancellation must have reached the tx task and entered its "
        "transaction unwind by now - the park is what keeps the release "
        "behind it"
    )

    tx_conn.unwind_gate.set()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=10.0)

    assert tx_conn.events == ["rollback_entered", "rollback_done", "release_write"], (
        "the release write must land only after the transaction task's "
        f"unwind provably finished; observed order: {tx_conn.events!r}"
    )
    assert len(backend.mark_interrupted_calls) == 1
    assert backend.mark_interrupted_calls[0]["hold"] == timedelta(0), (
        "the unwind completed inside the bounded park, so the actor is "
        "provably gone with its transaction: the row is genuinely free"
    )


# ── : an infra-failed release write must not eat the cancellation ────


async def test_infra_failed_release_write_propagates_the_cancellation_not_the_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exhausted-retry InterfaceError is swallowed; CancelledError wins.

    The old arm ended in a bare ``raise`` inside ``except
    _TERMINAL_WRITE_INFRA_EXCEPTIONS``, which re-raised the InterfaceError
    and REPLACED the CancelledError being handled: the interruption escaped
    ``consume_one_job`` as a job exception and dispatch's generic handler
    spent the attempt's budget on a deploy. The fix mirrors the
    ``mark_cancelled`` arm: log, disown, fall to the handler's final
    raise so the cancellation propagates.
    """
    # Zero the retry backoff so the four attempts fit the unit lane; the
    # retry MACHINERY is not under test here, only the arm's routing.
    monkeypatch.setattr(
        _handlers_mod, "_TERMINAL_WRITE_BACKOFF", (timedelta(0), timedelta(0), timedelta(0))
    )
    settings = _settings()
    deps = _deps_with(settings)
    backend = _InfraFailingReleaseBackend()
    registry = ActiveJobRegistry()
    job = make_job_row()
    clock: Clock = FakeClock(_NOW)

    async def body(running: JobRow, ctx: JobContext[BaseModel]) -> object:
        del running, ctx
        await asyncio.sleep(3600)
        return {"unreachable": True}

    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=registry,
        )
    )
    await asyncio.sleep(0.05)
    await _stamp_shutdown_origin(registry, job.id)

    attempt.cancel()
    escaped: BaseException | None = None
    try:
        await asyncio.wait_for(attempt, timeout=10.0)
    except asyncio.CancelledError as exc:
        escaped = exc

    assert isinstance(escaped, asyncio.CancelledError), (
        "the cancellation must be what leaves the consumer: an "
        "InterfaceError escaping here is the deploy-mislabelled-as-failure "
        f"bug; got {escaped!r}"
    )
    assert job.id in deps.disowned_jobs, (
        "the infra-failed release is best-effort: the row stays running and "
        "disowned so lock-lease expiry reclaims it"
    )
    assert backend.mark_cancelled_calls == [], (
        "an infra-failed infrastructure release must NOT fall through to "
        "mark_cancelled - the row carries no operator cancel, and a cancel "
        "write here would terminalise a deploy interruption"
    )


# ── F7: the anchored park budget and the dispatch→registry seam ────────


async def test_the_anchored_park_budget_is_the_remaining_share_capped_by_the_lease() -> None:
    """The anchored park is min(remaining - write budget, lease cap): both
    orders pinned (F7: every unit park shape was unanchored before this;
    only the integration run exercised the anchored branch).

    The remaining-share term is the endorsed bound (park to the budget,
    reserving the release write's own budget). The lease cap is what makes
    the single-RELEASING-write-failure exposure structurally impossible:
    the heartbeat stops at shutdown_event, so a park that outlived the
    lease would hand the row to the reclaim sweep while its actor thread
    still executes. Whichever binds first is the park.
    """
    from taskq.worker._watchdog import (
        live_tracked_actor_handles,  # pyright: ignore[reportPrivateUsage]  # Why: asserting the registry stayed empty: the budget function itself must not touch it.
    )

    loop = asyncio.get_running_loop()
    assert live_tracked_actor_handles() == []

    # A generous lease: the budget bound binds (60 - 10 elapsed - 5 = 45
    # remaining-share vs 100 - 10 - 5 = 85 cap).
    settings = _settings(termination_grace=60.0)
    assert settings.lock_lease == 60.0 and settings.heartbeat_interval == 10.0
    deps = _deps_with(settings)
    deps.shutdown_started_at = loop.time() - 10.0
    budget = _actor_exit_wait_budget(
        deps, settings, loop, reserve=_TERMINAL_WRITE_BUDGET.total_seconds()
    )
    assert budget == pytest.approx(60.0 - 10.0 - 5.0, abs=0.5), (
        "with the lease out of the way the anchored park is the remaining "
        "termination budget minus the release write's own budget"
    )

    # A tight lease: the cap binds (same remaining share, lease cap
    # 2 - 0.5 - 5 < 0 → the park is gone entirely and the release write
    # happens immediately: still ahead of any lease reclaim).
    tight = _settings(termination_grace=60.0, cleanup_grace=0.1)
    tight.lock_lease = 2.0
    tight.heartbeat_interval = 0.5
    tight_deps = _deps_with(tight)
    tight_deps.shutdown_started_at = loop.time() - 10.0
    tight_budget = _actor_exit_wait_budget(
        tight_deps, tight, loop, reserve=_TERMINAL_WRITE_BUDGET.total_seconds()
    )
    assert tight_budget == 0.0, (
        "a lease that cannot cover the park plus the write floors the park "
        "at zero - the consumer releases immediately with the full hold "
        "rather than parking into the reclaim sweep's window"
    )


async def test_the_unanchored_park_shapes_pin_their_degraded_bounds() -> None:
    """The park's two no-anchor bounds: the watchdog-disabled bound is the
    cleanup grace, and a bare call with neither deps nor settings falls
    back to the fixed 60s hold.

    Both branches are defensive surfaces a refactor could silently widen:
    with the watchdog disabled there is no guaranteed exit, so parking
    longer than the orchestrator's own post-cancel patience buys nothing
    (RELEASING releases the row regardless at the cleanup grace), and a
    bare direct ``consume_one_job`` call must still get a bounded hold
    rather than hold=0 or an unbounded park.
    """
    loop = asyncio.get_running_loop()

    degraded = _settings(cleanup_grace=1.0)
    degraded.watchdog_enabled = False
    budget = _actor_exit_wait_budget(
        _deps_with(degraded), degraded, loop, reserve=_TERMINAL_WRITE_BUDGET.total_seconds()
    )
    assert budget == pytest.approx(1.0, abs=0.05), (
        "with the watchdog disabled the park degrades to the cleanup grace: "
        "the orchestrator's own post-cancel patience, past which RELEASING "
        "releases the row regardless"
    )

    class _Ctx:
        _sync_actor_task: asyncio.Task[object] | None = None
        _tx_unwind_task: asyncio.Task[object] | None = None
        _actor_body_task: asyncio.Task[object] | None = None

        def _set_sync_actor_task(self, task: asyncio.Task[object]) -> None:
            self._sync_actor_task = task

    ctx = _Ctx()
    thread_gate = threading.Event()
    handle = asyncio.ensure_future(asyncio.to_thread(thread_gate.wait, 10.0))
    ctx._set_sync_actor_task(handle)
    try:
        fallback_hold = await _interrupted_actor_hold(ctx, deps=None, settings=None)
        from taskq.worker._consumer import (  # pyright: ignore[reportPrivateUsage]  # Why: the fallback constant the bare-call arm returns: pinned so a refactor cannot silently unbound or zero it.
            _BARE_CALL_HOLD_FALLBACK_SECS,
        )

        assert fallback_hold == timedelta(seconds=_BARE_CALL_HOLD_FALLBACK_SECS), (
            "a bare call with neither deps nor settings must carry the fixed "
            "fallback hold - the lease-expiry bound a stranded row already "
            "imposes, never hold=0 with no evidence either way"
        )
    finally:
        thread_gate.set()
        with suppress(asyncio.CancelledError):
            await asyncio.wait({handle}, timeout=10.0)


async def test_a_running_sync_actor_is_registered_until_its_thread_returns() -> None:
    """The dispatch helper's registry twin: live while the body runs, gone
    when it returns.

    The ctx stash serves the consumer's park; the process-wide registry is
    what the shutdown exit gate reads (await_tracked_actor_reap keeps the
    watchdog armed on it). A registration that missed either surface would
    silently unbound one half of the closure.
    """
    from taskq.worker._watchdog import (  # pyright: ignore[reportPrivateUsage]  # Why: the registry is the seam under test.
        live_tracked_actor_handles,
        register_tracked_actor_handle,
    )

    body_started = threading.Event()
    release_body = threading.Event()

    def body() -> object:
        body_started.set()
        release_body.wait(30.0)
        return {"done": True}

    class _Ctx:
        """Minimal ctx double carrying the stash contract the helper uses."""

        _sync_actor_task: asyncio.Task[object] | None = None

        def _set_sync_actor_task(self, task: asyncio.Task[object]) -> None:
            self._sync_actor_task = task

    ctx = _Ctx()
    runner = asyncio.ensure_future(_run_sync_actor_tracked(body, {}, ctx))  # type: ignore[arg-type]  # Why: the minimal double satisfies the setter contract; the body takes no kwargs.
    await asyncio.to_thread(body_started.wait, 10.0)

    handle = ctx._sync_actor_task
    assert handle is not None
    assert live_tracked_actor_handles() == [handle], (
        "a sync actor mid-body must be registered process-wide: this is "
        "what keeps the shutdown watchdog armed past the TaskGroup"
    )

    release_body.set()
    with suppress(asyncio.CancelledError):
        await runner
    assert handle.done()
    assert live_tracked_actor_handles() == [], (
        "a reaped handle must leave the registry's live set: the exit "
        "gate disarms the watchdog exactly when this empties"
    )

    # The registration is idempotent-shaped for the done filter: a done
    # entry lingering in the weak set never blocks a reap.
    register_tracked_actor_handle(handle)
    assert live_tracked_actor_handles() == []
