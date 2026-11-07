"""Body-side time-limit interrupt attacks.

The terminal-write side of the deadline machinery is proven elsewhere (the
poison-pill pins); this module attacks the BODY side: the deadline firing
inside the body's own transaction, inside its own try/finally, inside its
cleanup, and against a body that raises or catches the exception classes
the deadline machinery itself is built from.

Mined from dramatiq #791/#445/#845/#388. Three mechanism contracts are
pinned here, all enforced by ``_enforce_start_to_close``:

1. the machinery's expiry is a distinct marker (``_StartToCloseExceededError``);
   a body-raised ``TimeoutError`` routes as the ordinary failure it is,
   never as a deadline hit (the #791 conflation: the time-limit
   machinery's internal use of ``TimeoutError`` absorbing the body's own);
2. the deadline is not absorbable: a body that catches the expiry's
   cancellation and returns does not succeed past its own time limit;
3. the deadline is not deferrable: a hostile or incompetent ``finally``
   (``await asyncio.shield(cleanup())``) cannot hold the attempt (the
   slot, the row) hostage; the body unwinds detached and tracked, and the
   exit-proof hold fences the re-pend on its handle.

Plus the transactional path's rollback x unwind race (Attack 6): the tx
body unwinds inside ``transaction_conn.transaction()``, whose
``__aexit__`` answers the deadline's marker with a ROLLBACK on the SHARED
connection, so the tx path bound-waits the unwind (the exit-wait budget)
before the marker propagates -- the rollback runs on a quiesced
connection, the row records the truthful ``TimeoutError``, and a hostile
unwind outliving the budget still cannot hold the attempt past it.

Plus the classification matrix: for each exception class a body can raise,
the observed disposition and the attempt ledger's wholeness.
"""

import asyncio
import contextlib
import time
import warnings
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime, timedelta

import asyncpg
import orjson
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.constants import check_max_attempts_domain
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload, FakeBackend, as_backend, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import (  # pyright: ignore[reportPrivateUsage]  # Why: the pins reference the exact budget the tx path bound-waits, so a budget change cannot silently invalidate the timing contract.
    _TX_UNWIND_WAIT_BUDGET,
    consume_one_job,
)
from taskq.worker._watchdog import live_tracked_actor_handles
from taskq.worker.dispatch import (
    _run_sync_actor_tracked,  # pyright: ignore[reportPrivateUsage]  # Why: the sync-carrier pin reaches the executor-thread boundary the worker loop builds for a sync actor (dispatch's is_sync branch); the consumer harness's run_actor wraps it exactly so.
)

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()
_LIMIT = timedelta(milliseconds=150)

# The carrier tests arm a start_to_close only to reach the enforcement's
# task boundary (`if timeout is None` skips it). Their subject is the
# carrier conversion, not the deadline, so the limit is far beyond any
# scheduling delay. The deadline tests keep the tight _LIMIT.
_CARRIER_LIMIT = timedelta(seconds=30)


class _FakeConnection:
    """Minimal asyncpg.Connection stand-in with a transaction() context manager."""

    class _Transaction:
        async def __aenter__(self) -> "_FakeConnection._Transaction":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

    def transaction(self) -> "_FakeConnection._Transaction":
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _AtomicGuardConn:
    """asyncpg.Connection stand-in with the two faces the tx path races on.

    ``execute`` models asyncpg's one-operation-at-a-time connection guard
    (the ``_Atomic`` wrap): a second operation issued while one is still
    in flight raises ``InterfaceError`` ("another operation on this
    connection is in progress"). ``transaction()`` models the asyncpg
    transaction context manager: an exception flowing through
    ``__aexit__`` is met by a ROLLBACK issued on the same connection.
    Together they reproduce, deterministically, the transactional path's
    collision: the deadline's marker reaches the ``__aexit__`` while the
    body unwind's statement is still in flight.
    """

    def __init__(self, execute_delay: float, delayed_queries: "set[str] | None" = None) -> None:
        self._execute_delay = execute_delay
        # None (the default): every statement takes the delay. A set: only
        # those statements take it and the rest are instant -- this
        # shape needs savepoint and ROLLBACK instant while the hostile
        # unwind's own statement stays in flight past the budget.
        self._delayed_queries = delayed_queries
        self._busy = False
        self.log: list[str] = []

    class _Transaction:
        def __init__(self, conn: "_AtomicGuardConn") -> None:
            self._conn = conn

        async def __aenter__(self) -> "_AtomicGuardConn._Transaction":
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            if exc_type is not None:
                # asyncpg's __aexit__ answers an exception flow with a
                # ROLLBACK on the same connection.
                await self._conn.execute("ROLLBACK")

    def transaction(self) -> "_AtomicGuardConn._Transaction":
        return self._Transaction(self)

    async def execute(self, query: str, *args: object) -> str:
        if self._busy:
            raise asyncpg.exceptions.InterfaceError(
                "another operation on this connection is in progress"
            )
        self._busy = True
        try:
            delay = (
                self._execute_delay
                if self._delayed_queries is None or query in self._delayed_queries
                else 0.0
            )
            await asyncio.sleep(delay)
            self.log.append(query)
            return "OK"
        finally:
            self._busy = False


def _job(start_to_close: timedelta | None) -> JobRow:
    return _dc_replace(
        make_job_row(start_to_close=start_to_close),
        locked_by_worker=_WORKER_ID,
    )


def _small_grace_settings() -> "WorkerSettings":
    """Settings whose cleanup grace keeps the exit-proof hold's park fast
    in tests while the park itself stays exercised (the bare-call path
    skips the park entirely, hold=60s fallback, which would prove
    nothing)."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
        }
    )


async def _consume(
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    job: JobRow,
    *,
    transactional: bool = False,
    backend: FakeBackend | None = None,
) -> object:
    backend = backend if backend is not None else FakeBackend()
    outcome: object | None = None
    with suppress(asyncio.CancelledError):
        outcome = await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=run_actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
        )
    return outcome


class _DeadlineFireProbe:
    """A recording proxy around the loop's ``call_later``. Every timer is
    delegated to the real one with all arguments. A timer whose delay is
    the limit and whose callback is the enforcement's ``_fire`` (matched
    by qualname) is counted at fire time, and its handle's due time is
    stored on ``deadline_when``. The count keeps the stall tests honest:
    if a hop shift moves the stall off the deadline, the count stays 0
    and the assert fails instead of the test passing vacuously. The
    shadowed attribute is restored on exit."""

    def __init__(self, loop: asyncio.AbstractEventLoop, limit: timedelta) -> None:
        self.fires = 0
        self.deadline_when: float | None = None
        self._loop = loop
        self._limit = limit
        self._real: Callable[..., asyncio.TimerHandle] = loop.call_later

    def __enter__(self) -> "_DeadlineFireProbe":
        probe = self

        def recording_call_later(
            delay: float, callback: Callable[..., object], *args: object, **kwargs: object
        ) -> object:
            if delay == probe._limit.total_seconds() and getattr(
                callback, "__qualname__", ""
            ).endswith("._fire"):

                def counted() -> object:
                    probe.fires += 1
                    return callback(*args)

                handle = probe._real(delay, counted, *args, **kwargs)
                probe.deadline_when = handle.when()
                return handle
            return probe._real(delay, callback, *args, **kwargs)

        self._loop.call_later = recording_call_later  # pyright: ignore[reportAttributeAccessIssue]  # Why: the recording proxy shadows the loop's bound method for the test's duration and restores it on exit; the loop object carries no __slots__ and the shadow is the count-don't-answer seam.
        return self

    def __exit__(self, *exc: object) -> None:
        self._loop.call_later = self._real  # pyright: ignore[reportAttributeAccessIssue]  # Why: restore the original bound method (see __enter__).


# ── Attack 1: the deadline x the body's own transaction ───────────────


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_deadline_fires_inside_the_bodys_own_transaction(
    transactional: bool,
) -> None:
    """A body mid-flight in its own database transaction (its own pool
    connection, not taskq's) when the time limit fires: the uncertain-
    commit shape, the zombie class from the body side.

    The construction: the body's own ``commit()`` is in flight when the
    deadline's cancellation reaches it, and the server side of that
    commit LANDS anyway (the uncertain-commit window: the client saw the
    cancel, the server committed).

    The obligation: the attempt ledger stays WHOLE (one truthful timeout
    failure recorded, the row re-pended for retry) and the body's late
    work never flips the job row (no success write lands for an attempt
    the deadline killed; the re-run is a new attempt epoch, and the
    body-side exactly-once contract is the actor's own identity_key).
    """
    backend = FakeBackend()
    commit_outcome: list[str] = []

    class _OwnConn:
        """The body's own pool connection, mid-COMMIT at the deadline."""

        async def commit(self) -> None:
            try:
                # The COMMIT statement in flight on the server; the
                # deadline's cancellation reaches it here.
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                # The server raced the CancelRequest and won: the commit
                # is durable, the client still sees the cancellation. The
                # uncertain-commit window, deterministically.
                commit_outcome.append("committed")
                raise

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        own = _OwnConn()
        await asyncio.sleep(0.02)  # the body's own writes, pre-commit
        await own.commit()  # cancelled mid-COMMIT
        return None  # never reached

    outcome = await consume_one_job(
        as_backend(backend),
        _job(_LIMIT),
        _WORKER_ID,
        run_actor=body,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
    )

    assert commit_outcome == ["committed"], "the uncertain commit landed server-side"
    # The ledger is whole: the deadline failure was recorded truthfully
    # and the row re-pended, never succeeded.
    assert outcome == "scheduled", outcome
    assert not backend.mark_succeeded_calls, "a zombie's late commit must not flip the row"
    assert len(backend.mark_failed_or_retry_calls) == 1, "exactly one failure write"
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError"  # pyright: ignore[reportAttributeAccessIssue]
    assert write["retry_delay"] is not None  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_reclaim_fence_stands_against_the_zombie(transactional: bool) -> None:
    """The re-pend the deadline writes is fenced on (attempt, claim_epoch):
    a zombie body that somehow reaches a terminal write for the OLD attempt
    (a stale epoch, the pre-reclaim identity) cannot win the row the live
    attempt now owns."""
    backend = FakeBackend()
    job = _job(_LIMIT)

    async def body(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.Event().wait()
        return "absorbed and returned"

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=body,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
    )
    assert outcome == "scheduled"
    # The failure write carried this attempt's identity: the fence keys
    # the re-pend to the attempt the deadline killed, so a later reclaim
    # (a newer epoch) owns the row alone.
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError"  # pyright: ignore[reportAttributeAccessIssue]


# ── Attack 2: the deadline x the body's try/finally ───────────────────


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
# load_sensitive (pr #695's ruling): this file's elapsed-measuring family.
# The load-bearing teeth are the two tx-unwind window pins below — elapsed
# must land inside [_TX_UNWIND_WAIT_BUDGET, _TX_UNWIND_WAIT_BUDGET + 3.0) —
# and a window that tight measures the packing before it measures the code
# on a starved runner. The family runs in the serial load-sensitive lane;
# the asserts are untouched, so the windows still separate "cut by the
# machinery" from "cut by the test's own net". The FakeClock wedge
# tripwires in the family (this pin's elapsed < 5.0 and the fleet pin's
# elapsed < 10.0 — 50x+ slack by design, "not a measurement" per their own
# comments) ride along: harmless serially, and they drive the same
# scenario.
@pytest.mark.load_sensitive
async def test_hostile_finally_cannot_defer_the_deadline(transactional: bool) -> None:
    """A body whose finally does its own long (shielded) cleanup must not
    defer the hard limit: the soft cancel lands at the deadline, the
    attempt ends on it, and the still-unwinding body is detached tracked,
    never allowed to hold the slot for as long as its cleanup likes."""
    backend = FakeBackend()
    cleanups: list[asyncio.Task[object]] = []

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        try:
            await asyncio.Event().wait()
        finally:
            cleanup: asyncio.Task[object] = asyncio.ensure_future(asyncio.sleep(3600))
            cleanups.append(cleanup)
            await asyncio.shield(cleanup)

    started = asyncio.get_running_loop().time()
    outcome = await consume_one_job(
        as_backend(backend),
        _job(timedelta(milliseconds=100)),
        _WORKER_ID,
        run_actor=body,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
    )
    elapsed = asyncio.get_running_loop().time() - started

    assert outcome == "scheduled", outcome
    # The 3600s shielded cleanup did not hold the attempt: the deadline
    # ended it in bounded loop time.
    # Derived bound: the clock is a FakeClock, so the attempt consumes loop
    # turns, not wall time — 5.0 s of wall covers thousands of turns. It is
    # a wedge tripwire (co-tenancy slack ~50x the observed sub-100ms path),
    # not a measurement.
    assert elapsed < 5.0, elapsed
    # The unwinding zombie is accounted for: the shutdown watchdog can
    # see it and the exit-proof hold parked on it before the re-pend.
    zombies = live_tracked_actor_handles()
    assert any(not z.done() for z in zombies), "a hostile unwind must stay tracked"
    # Teardown: reap the zombies and their shielded cleanups so the test
    # loop closes clean.
    for z in zombies:
        z.cancel()
    for c in cleanups:
        c.cancel()
    await asyncio.gather(*zombies, *cleanups, return_exceptions=True)


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_well_behaved_finally_still_finishes_before_the_write(
    transactional: bool,
) -> None:
    """A body whose finally finishes promptly (the competent case) is
    provably unwound before the row is re-pended: the exit-proof hold
    parked on the body-task handle, saw it done, and the write landed
    with hold=0 (the decision's own delay, unraised)."""
    backend = FakeBackend()
    finally_done = asyncio.Event()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        try:
            await asyncio.sleep(30)
        finally:
            await asyncio.sleep(0.01)
            finally_done.set()

    outcome = await consume_one_job(
        as_backend(backend),
        _job(_LIMIT),
        _WORKER_ID,
        run_actor=body,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        settings=_small_grace_settings(),
        transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
    )
    assert outcome == "scheduled"
    assert finally_done.is_set(), "a prompt finally must finish before the re-pend"


# ── Attack 3: the classification matrix ───────────────────────────────


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_timeout_error_is_not_a_deadline_hit(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A body raising its own ``TimeoutError`` with the limit far away:
    the dramatiq #791 shape. The body's error takes the ordinary failure
    path: its own error_class on the row, the normal retry decision, and
    NONE of the deadline machinery's signals (no ``job_timeout`` log, no
    ``taskq.jobs.timeouts`` increment). The confusion must never loop:
    the body's TimeoutError is a bounded attempt outcome, not the
    machinery's."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError("the body's own timeout, the limit is 30s away")

    with structlog.testing.capture_logs() as logs:
        outcome = await _consume(body, _job(timedelta(seconds=30)), transactional=transactional)

    assert outcome == "scheduled", outcome
    assert not timeout_metric_calls, (
        f"the body's own TimeoutError was misreported as a deadline hit: {timeout_metric_calls}"
    )
    assert not any(entry.get("event") == "job_timeout" for entry in logs), (
        "the body's own TimeoutError must not produce the machinery's job_timeout log"
    )


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_timeout_error_records_truthful_row(
    transactional: bool,
) -> None:
    """The attempt ledger for a body-raised ``TimeoutError``: error_class
    is the body's own class name, the write is the ordinary failure
    write, the budget still applies."""
    backend = FakeBackend()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError("the body's own timeout")

    outcome = await _consume(
        body, _job(timedelta(seconds=30)), transactional=transactional, backend=backend
    )

    assert outcome == "scheduled", outcome
    assert len(backend.mark_failed_or_retry_calls) == 1
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError"  # pyright: ignore[reportAttributeAccessIssue]
    assert write["error_info"].error_message == "the body's own timeout"  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_real_deadline_is_still_a_deadline_hit(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control arm: a body that genuinely runs past its limit lands in
    the timeout handler, with the deadline's log line and metric, and the
    row reads ``TimeoutError`` (the deadline class, not the marker's
    private name)."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(30)

    with structlog.testing.capture_logs() as logs:
        outcome = await consume_one_job(
            as_backend(backend),
            _job(_LIMIT),
            _WORKER_ID,
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
        )

    assert outcome == "scheduled", outcome
    assert any(call["kind"] == "start_to_close" for call in timeout_metric_calls), (
        timeout_metric_calls
    )
    assert any(entry.get("event") == "job_timeout" for entry in logs)
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError"  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_cannot_absorb_the_deadline(transactional: bool) -> None:
    """A body that catches the deadline's cancellation and returns a
    value: since 3.12 ``wait_for`` hands that value back and the attempt
    was marked SUCCEEDED past its own time limit. The deadline wins: the
    value is discarded, the attempt is a timeout."""
    backend = FakeBackend()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(30)
        return "absorbed and returned"

    outcome = await consume_one_job(
        as_backend(backend),
        _job(_LIMIT),
        _WORKER_ID,
        run_actor=body,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
    )

    assert outcome != "succeeded", "the deadline was absorbed and the attempt succeeded"
    assert outcome == "scheduled", outcome
    assert not backend.mark_succeeded_calls


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_that_finished_inside_the_deadline_is_not_a_timeout(
    transactional: bool,
) -> None:
    """A body that finished before the deadline is the attempt's own
    outcome, even when the enforcement's continuation resumes late. The
    body task's done-callback is queued, so the wait's wakeup trails the
    body's completion by two loop hops; a callback stalling the loop in
    between lets the due ``_fire`` latch ``expired`` over a body that had
    already surfaced its result, which the ``if expired:`` branch then
    discarded as a timeout. The rescue reads the body's finish stamp: a
    finish at or before the armed deadline returns the value or re-raises
    its exception as the ordinary failure. A finish after the deadline
    (the absorbing body above) stays a timeout."""
    backend = FakeBackend()
    loop = asyncio.get_running_loop()
    body_result: dict[str, object] | None = None

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        nonlocal body_result
        body_result = {"ok": True}
        return body_result

    # The stall must run inside the body's own loop batch, after the
    # body's step and before the done-callback releases the wait, and
    # hold the loop past the 150ms limit so _fire latches over the
    # finished body. The tx path reaches the enforcement through the
    # shielded tx task, one hop later, so its stall is scheduled one
    # batch later. The ordering is forced by call_soon/call_later, not
    # timed.
    if transactional:
        loop.call_soon(lambda: loop.call_later(0, time.sleep, 0.16))
    else:
        loop.call_later(0, time.sleep, 0.16)

    with _DeadlineFireProbe(loop, _LIMIT) as probe:
        outcome = await consume_one_job(
            as_backend(backend),
            _job(_LIMIT),
            _WORKER_ID,
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
        )

    assert outcome == "succeeded", outcome
    assert len(backend.mark_succeeded_calls) == 1, backend.mark_succeeded_calls
    assert orjson.loads(backend.mark_succeeded_calls[0][3]) == {"ok": True}, (
        backend.mark_succeeded_calls
    )
    assert not backend.mark_failed_or_retry_calls, backend.mark_failed_or_retry_calls
    assert body_result == {"ok": True}
    assert probe.fires == 1, (
        "the stall must cross the start_to_close deadline exactly once (the proxy"
        " must see the enforcement's own _fire run) or this pin no longer reaches"
        " its subject"
    )


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_sync_body_that_finished_inside_the_deadline_is_not_a_timeout(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sync carrier of the same contract. A plain ``def`` body runs
    in the executor thread and finishes inside the deadline, but the
    enforcement's coroutine resumes late: the dispatch's ``await
    asyncio.shield(thread_task)`` resumes only when the loop next runs.
    The thread stamps its real finish itself (``loop.time()`` is safe
    from the thread) and earliest-wins keeps it over the late resume, so
    the rescue sees an in-time finish and the attempt is the thread's
    recorded outcome: succeeded, one success write, no failure write, no
    ``job_timeout`` log or metric. The stall schedules from the ctx hook
    the dispatch calls when it stashes the thread task, which is
    production's hop count; ``run_actor`` is the production shape (a
    direct await, no extra task)."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    loop = asyncio.get_running_loop()
    body_ran = False

    def body() -> object:
        nonlocal body_ran
        body_ran = True
        time.sleep(0.05)
        return {"ok": True}

    # Scheduled right after the dispatch stashes the thread task, the
    # stall holds the loop past the 150ms limit: the thread runs 0.05s
    # (inside the deadline) while the enforcement stays starved until
    # about +0.5s. The ordering is forced by the hop structure, not
    # timed.
    real_set_sync = JobContext._set_sync_actor_task

    def _stall_once_thread_task_is_stashed(
        self: JobContext[BaseModel], task: asyncio.Task[object]
    ) -> None:
        real_set_sync(self, task)
        loop.call_soon(time.sleep, 0.5)

    monkeypatch.setattr(JobContext, "_set_sync_actor_task", _stall_once_thread_task_is_stashed)

    async def run_actor(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        del job
        # Called directly as production calls it (run_actor_with_di).
        return await _run_sync_actor_tracked(body, {}, ctx)

    with (
        structlog.testing.capture_logs() as logs,
        _DeadlineFireProbe(loop, _LIMIT) as probe,
    ):
        outcome = await _consume(
            run_actor, _job(_LIMIT), transactional=transactional, backend=backend
        )

    assert outcome == "succeeded", outcome
    assert len(backend.mark_succeeded_calls) == 1, backend.mark_succeeded_calls
    assert orjson.loads(backend.mark_succeeded_calls[0][3]) == {"ok": True}, (
        backend.mark_succeeded_calls
    )
    assert not backend.mark_failed_or_retry_calls, backend.mark_failed_or_retry_calls
    assert not timeout_metric_calls, timeout_metric_calls
    assert not any(entry.get("event") == "job_timeout" for entry in logs), logs
    assert body_ran
    assert probe.fires == 1, (
        "the stall must cross the start_to_close deadline exactly once (the proxy"
        " must see the enforcement's own _fire run) or this pin no longer reaches"
        " its subject"
    )


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_deadline_cancel_landing_on_a_recorded_in_time_body_is_its_outcome(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rescue's cancelled arm at the inclusive boundary: the ctx holds
    the body's record (outcome first, then the finish stamp, both inside
    the deadline) while _fire's cancel is delivered into the body task.
    The attempt must be the recorded outcome: the value returned, the row
    succeeded, no failure write, no cancel write, no ``job_timeout`` log
    or metric, and no CancelledError escaping the consumer. Production
    reaches this state through a sub-microsecond race between the
    thread's completion callback and _fire, which cannot be induced
    deterministically through the executor handoff, so the record is
    written here exactly as ``_thread_body`` writes it. The thread's own
    recording is covered by the sync test and by ``_thread_body``."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    loop = asyncio.get_running_loop()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        # The record exactly as ``_thread_body`` writes it: outcome first,
        # then the finish stamp, both inside the deadline. The body then
        # blocks past the deadline so _fire's cancel lands on this task.
        ctx._record_actor_body_outcome({"ok": True}, None)
        ctx._record_actor_body_finished_at(loop.time())
        await asyncio.sleep(30)

    with structlog.testing.capture_logs() as logs:
        outcome = await _consume(body, _job(_LIMIT), transactional=transactional, backend=backend)

    assert outcome == "succeeded", outcome
    assert len(backend.mark_succeeded_calls) == 1, backend.mark_succeeded_calls
    assert orjson.loads(backend.mark_succeeded_calls[0][3]) == {"ok": True}, (
        backend.mark_succeeded_calls
    )
    assert not backend.mark_failed_or_retry_calls, backend.mark_failed_or_retry_calls
    assert not backend.mark_cancelled_calls, backend.mark_cancelled_calls
    assert not timeout_metric_calls, timeout_metric_calls
    assert not any(entry.get("event") == "job_timeout" for entry in logs), logs


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_sync_body_systemexit_inside_the_deadline_is_the_actors_outcome(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sync body raising ``SystemExit(3)`` inside the deadline: the
    thread records the carrier and its in-time finish, the stall lets the
    deadline's cancel land on the body task, and the rescue re-raises the
    recorded carrier. The row carries ``error_class SystemExit`` on one
    failure/retry write, with no success write, no cancel write, and no
    ``job_timeout`` log or metric. The carrier exists so the rescue never
    re-raises the raw pair into a task."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    loop = asyncio.get_running_loop()

    def body() -> object:
        raise SystemExit(3)

    real_set_sync = JobContext._set_sync_actor_task

    def _stall_once_thread_task_is_stashed(
        self: JobContext[BaseModel], task: asyncio.Task[object]
    ) -> None:
        real_set_sync(self, task)
        loop.call_soon(time.sleep, 0.5)

    monkeypatch.setattr(JobContext, "_set_sync_actor_task", _stall_once_thread_task_is_stashed)

    async def run_actor(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        del job
        return await _run_sync_actor_tracked(body, {}, ctx)

    with (
        structlog.testing.capture_logs() as logs,
        _DeadlineFireProbe(loop, _LIMIT) as probe,
    ):
        outcome = await _consume(
            run_actor, _job(_LIMIT), transactional=transactional, backend=backend
        )

    assert outcome == "scheduled", outcome
    assert len(backend.mark_failed_or_retry_calls) == 1, backend.mark_failed_or_retry_calls
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "SystemExit"  # pyright: ignore[reportAttributeAccessIssue]
    assert not backend.mark_succeeded_calls, backend.mark_succeeded_calls
    assert not backend.mark_cancelled_calls, backend.mark_cancelled_calls
    assert not timeout_metric_calls, timeout_metric_calls
    assert not any(entry.get("event") == "job_timeout" for entry in logs), logs
    assert probe.fires == 1, (
        "the stall must cross the start_to_close deadline exactly once (the proxy"
        " must see the enforcement's own _fire run) or this pin no longer reaches"
        " its subject"
    )


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_sync_body_own_timeout_inside_the_deadline_is_the_actors_outcome(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sync body raising its OWN ``TimeoutError`` inside the deadline
    (the #791 conflation, through the rescue): the thread records the
    exception and its in-time finish, the stall lets the deadline's
    cancel land on the body task, and the rescue re-raises the recorded
    exception. The row carries the body's own class and message, not the
    ``start_to_close`` marker's, with no ``job_timeout`` log or
    metric."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    loop = asyncio.get_running_loop()

    def body() -> object:
        raise TimeoutError("body's own timeout")

    real_set_sync = JobContext._set_sync_actor_task

    def _stall_once_thread_task_is_stashed(
        self: JobContext[BaseModel], task: asyncio.Task[object]
    ) -> None:
        real_set_sync(self, task)
        loop.call_soon(time.sleep, 0.5)

    monkeypatch.setattr(JobContext, "_set_sync_actor_task", _stall_once_thread_task_is_stashed)

    async def run_actor(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        del job
        return await _run_sync_actor_tracked(body, {}, ctx)

    with (
        structlog.testing.capture_logs() as logs,
        _DeadlineFireProbe(loop, _LIMIT) as probe,
    ):
        outcome = await _consume(
            run_actor, _job(_LIMIT), transactional=transactional, backend=backend
        )

    assert outcome == "scheduled", outcome
    assert len(backend.mark_failed_or_retry_calls) == 1, backend.mark_failed_or_retry_calls
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError"  # pyright: ignore[reportAttributeAccessIssue]
    assert write["error_info"].error_message == "body's own timeout"  # pyright: ignore[reportAttributeAccessIssue]
    assert not backend.mark_succeeded_calls, backend.mark_succeeded_calls
    assert not backend.mark_cancelled_calls, backend.mark_cancelled_calls
    assert not timeout_metric_calls, timeout_metric_calls
    assert not any(entry.get("event") == "job_timeout" for entry in logs), logs
    assert probe.fires == 1, (
        "the stall must cross the start_to_close deadline exactly once (the proxy"
        " must see the enforcement's own _fire run) or this pin no longer reaches"
        " its subject"
    )


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_stamped_exactly_at_the_deadline_is_its_own_outcome(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rescue's line is inclusive (``<=``): a body whose finish stamp
    equals the armed deadline made its deadline. The probe captures the
    ``_fire`` handle's due time; the body stamps exactly that value with
    its outcome on the ctx, then blocks past the deadline so ``_fire``'s
    cancel lands on the body task. The attempt is the recorded outcome:
    succeeded, no failure write, no cancel write, no ``job_timeout`` log
    or metric. A ``<`` regression reds this test."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    loop = asyncio.get_running_loop()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        assert probe.deadline_when is not None, (
            "the enforcement must have armed the deadline before the body ran"
        )
        # The record exactly as ``_thread_body`` writes it, with the
        # finish stamp exactly the armed deadline.
        ctx._record_actor_body_outcome({"ok": True}, None)
        ctx._record_actor_body_finished_at(probe.deadline_when)
        await asyncio.sleep(30)

    with (
        structlog.testing.capture_logs() as logs,
        _DeadlineFireProbe(loop, _LIMIT) as probe,
    ):
        outcome = await _consume(body, _job(_LIMIT), transactional=transactional, backend=backend)

    assert outcome == "succeeded", outcome
    assert len(backend.mark_succeeded_calls) == 1, backend.mark_succeeded_calls
    assert orjson.loads(backend.mark_succeeded_calls[0][3]) == {"ok": True}, (
        backend.mark_succeeded_calls
    )
    assert not backend.mark_failed_or_retry_calls, backend.mark_failed_or_retry_calls
    assert not backend.mark_cancelled_calls, backend.mark_cancelled_calls
    assert not timeout_metric_calls, timeout_metric_calls
    assert not any(entry.get("event") == "job_timeout" for entry in logs), logs
    assert probe.fires == 1, (
        "the stall must cross the start_to_close deadline exactly once (the proxy"
        " must see the enforcement's own _fire run) or this pin no longer reaches"
        " its subject"
    )


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_stamped_in_time_without_a_record_is_a_truthful_timeout(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rescue's defensive arm: an in-time finish stamp with no
    outcome record, while the deadline's cancel lands on the body task.
    The recorder is disabled for the attempt (an async body's own
    self-cancellation writes its record and is the previous test's
    subject; this pin is the state where no record exists at all). The
    attempt is the truthful timeout, the ``_StartToCloseExceededError``
    marker's own disposition: one failure/retry write with ``error_class
    TimeoutError`` and ``error_message start_to_close``, the
    ``job_timeout`` log line and metric present, no success write, no
    cancel write, and no CancelledError escaping the consumer."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    loop = asyncio.get_running_loop()

    # The recording failure, simulated at the face the writers use: the
    # outcome write is dropped for the attempt (monkeypatched on the ctx
    # class, restored by the fixture), so the frame's cancel-delivered
    # write lands on the no-op and the record stays absent.
    def _no_record(self: JobContext[BaseModel], value: object, exc: BaseException | None) -> None:
        return None

    monkeypatch.setattr(JobContext, "_record_actor_body_outcome", _no_record)

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        # The in-time finish stamp ONLY -- no outcome record anywhere.
        ctx._record_actor_body_finished_at(loop.time())
        await asyncio.sleep(30)

    with structlog.testing.capture_logs() as logs:
        outcome = await _consume(body, _job(_LIMIT), transactional=transactional, backend=backend)

    assert outcome == "scheduled", outcome
    assert len(backend.mark_failed_or_retry_calls) == 1, backend.mark_failed_or_retry_calls
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError"  # pyright: ignore[reportAttributeAccessIssue]
    assert write["error_info"].error_message == "start_to_close"  # pyright: ignore[reportAttributeAccessIssue]
    assert not backend.mark_succeeded_calls, backend.mark_succeeded_calls
    assert not backend.mark_cancelled_calls, backend.mark_cancelled_calls
    assert any(call["kind"] == "start_to_close" for call in timeout_metric_calls), (
        timeout_metric_calls
    )
    assert any(entry.get("event") == "job_timeout" for entry in logs), logs


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_self_cancellation_inside_the_deadline_is_the_abandon_write(
    transactional: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An async body raising its own ``asyncio.CancelledError`` (the
    abandon signal) before the deadline, while a stall lets the
    deadline's ``_fire`` win the wakeup race: the frame's record (the raw
    CancelledError, written before its in-time finish stamp) is the
    rescue's answer, so the attempt takes the normal cancellation
    disposition. Without the frame's record this state degraded to the
    defensive arm's start_to_close timeout, re-pending a self-abandoned
    attempt under the timeout policy."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    loop = asyncio.get_running_loop()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    # The same stall shape as the async stall test: the body task
    # completes at once, the wait's wakeup is two loop hops behind, and a
    # stall crossing the deadline lets ``_fire`` latch first.
    if transactional:
        loop.call_soon(lambda: loop.call_later(0, time.sleep, 0.16))
    else:
        loop.call_later(0, time.sleep, 0.16)

    with (
        structlog.testing.capture_logs() as logs,
        _DeadlineFireProbe(loop, _LIMIT) as probe,
    ):
        outcome = await _consume(body, _job(_LIMIT), transactional=transactional, backend=backend)

    assert outcome is None, outcome  # the CancelledError surfaces; _consume suppresses it
    assert len(backend.mark_cancelled_calls) == 1, backend.mark_cancelled_calls
    assert not backend.mark_failed_or_retry_calls, backend.mark_failed_or_retry_calls
    assert not backend.mark_succeeded_calls, backend.mark_succeeded_calls
    assert not timeout_metric_calls, timeout_metric_calls
    assert not any(entry.get("event") == "job_timeout" for entry in logs), logs
    assert probe.fires == 1, (
        "the stall must cross the start_to_close deadline exactly once (the proxy"
        " must see the enforcement's own _fire run) or this pin no longer reaches"
        " its subject"
    )


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_cancelled_error_is_the_abandon_signal(transactional: bool) -> None:
    """A body raising a bare ``asyncio.CancelledError`` with no cancel
    request anywhere: the documented abandon-the-unit-of-work signal. The
    consumer's cancel arm terminalises the row (cancelled, no error
    class), the ledger is whole, and the exception surfaces to the caller
    (the loop absorbs it as the cancelled outcome and keeps serving)."""
    backend = FakeBackend()
    escaped = False

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    with suppress(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            _job(None),
            _WORKER_ID,
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            transaction_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]
        )
    escaped = True

    assert escaped
    assert len(backend.mark_cancelled_calls) == 1, "the abandon signal terminalises the row"
    assert not backend.mark_failed_or_retry_calls, "no failure is recorded for an abandonment"


async def test_external_interrupt_still_reaches_a_limited_body() -> None:
    """The enforcement runs the body in its own task; a shutdown
    interrupt / operator cancel of the consumer task must still be
    forwarded to the body and surface as the consumer's CancelledError
    (the interrupt arm owns the row then, not the deadline machinery)."""
    backend = FakeBackend()
    probe_cancelled = asyncio.Event()
    body_started = asyncio.Event()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        body_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            probe_cancelled.set()
            raise
        return None

    consume_task: asyncio.Task[object] = asyncio.create_task(
        consume_one_job(
            as_backend(backend),
            _job(timedelta(seconds=30)),
            _WORKER_ID,
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
        )
    )
    await body_started.wait()
    consume_task.cancel()
    with suppress(asyncio.CancelledError):
        await consume_task

    assert probe_cancelled.is_set(), "the interrupt must reach the body task"
    assert backend.mark_cancelled_calls, "the interrupted row is terminalised cancelled"


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_systemexit_stays_an_attempt_outcome(transactional: bool) -> None:
    """An async body calling sys.exit() with a start_to_close armed: the
    enforcement adds a task boundary to the autonomous path (the third
    after the sync thread and the tx task); a raw SystemExit there would
    kill the loop (Task.__step re-raises the pair bare). The carrier
    conversion keeps it an attempt outcome: error_class ``SystemExit``."""
    backend = FakeBackend()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        raise SystemExit(3)

    outcome = await _consume(
        body, _job(_CARRIER_LIMIT), transactional=transactional, backend=backend
    )

    assert outcome == "scheduled", outcome
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "SystemExit"  # pyright: ignore[reportAttributeAccessIssue]


class ActorBoom(BaseException):
    pass


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_body_baseexception_subclass_is_an_attempt_outcome(transactional: bool) -> None:
    """A custom ``BaseException`` subclass from a limited body: the
    per-attempt capture contract holds across the new task boundary. The
    default transient budget still applies (an ActorBoom is retried like
    any failure), so the outcome is ``scheduled`` with the truthful
    error_class on the failure write."""
    backend = FakeBackend()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        raise ActorBoom("boom")

    outcome = await _consume(
        body, _job(_CARRIER_LIMIT), transactional=transactional, backend=backend
    )

    assert outcome == "scheduled", outcome
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "ActorBoom"  # pyright: ignore[reportAttributeAccessIssue]


# ── Attack 4: the limits' config edges ────────────────────────────────


def test_zero_backoff_base_is_refused() -> None:
    """dramatiq #388's zero-spin shape: a zero (or negative) backoff base
    degenerates the retry curve to a zero-period loop that monopolises a
    slot. The validation gate refuses it at registration."""
    with pytest.raises(ValueError, match="base must be > 0"):
        RetryPolicy(base=timedelta(0))


def test_zero_max_attempts_is_refused() -> None:
    """max_attempts = 0 (or negative): a job that can never complete. The
    domain gate refuses below one."""
    with pytest.raises(ValueError, match="must be >= 1"):
        check_max_attempts_domain(0)
    with pytest.raises(ValueError, match="must be >= 1"):
        check_max_attempts_domain(-3)


def test_zero_start_to_close_is_refused() -> None:
    """A non-positive start_to_close anchors the deadline in the past:
    refused at the actor layer (the settings and enqueue layers carry the
    same gate, pinned by test_start_to_close_validation)."""
    from taskq.actor import actor as actor_decorator

    async def handler(_payload: EmptyPayload) -> None:
        pass

    with pytest.raises(ValueError, match="start_to_close must be > 0"):
        actor_decorator(name="_attack_stc_zero", start_to_close=timedelta(0))(handler)


async def test_time_limit_smaller_than_the_heartbeat_tick_is_safe() -> None:
    """A start_to_close far below the heartbeat interval: the deadline
    fires between beats, the attempt ends and the row re-pends while the
    lease is still fresh; no wedge, no slot spin. The lease (not the
    deadline) is what protects the row, so no invariant couples the
    two."""
    backend = FakeBackend()

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(30)

    outcome = await consume_one_job(
        as_backend(backend),
        _job(timedelta(milliseconds=20)),
        _WORKER_ID,
        run_actor=body,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
    )
    assert outcome == "scheduled", outcome
    assert len(backend.mark_failed_or_retry_calls) == 1


# ── Attack 5: the fleet shape ─────────────────────────────────────────


@pytest.mark.load_sensitive
async def test_fleet_of_deadline_hits_keeps_capacity_and_slots() -> None:
    """100 bodies all hitting their limits at once, every one of them
    hostile (a shielded 3600s cleanup in its finally): every attempt still
    ends, every slot is returned, the loop stays responsive, every zombie
    is tracked for the shutdown watchdog, and no task outcome is left
    unretrieved."""
    n_jobs = 100
    backend = FakeBackend()
    cleanups: list[asyncio.Task[object]] = []

    async def body(job: JobRow, ctx: JobContext[BaseModel]) -> object:
        try:
            await asyncio.Event().wait()
        finally:
            cleanup: asyncio.Task[object] = asyncio.ensure_future(asyncio.sleep(3600))
            cleanups.append(cleanup)
            await asyncio.shield(cleanup)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        started = asyncio.get_running_loop().time()
        outcomes: list[object] = list(
            await asyncio.gather(
                *(
                    consume_one_job(
                        as_backend(backend),
                        _job(timedelta(milliseconds=50)),
                        _WORKER_ID,
                        run_actor=body,
                        actor_config=default_actor_config(),
                        payload_type=EmptyPayload,
                        clock=FakeClock(_NOW),
                    )
                    for _ in range(n_jobs)
                )
            )
        )
        elapsed = asyncio.get_running_loop().time() - started

    assert all(o == "scheduled" for o in outcomes), outcomes
    # Derived bound: FakeClock again — n_jobs deadline hits cost O(n) loop
    # turns, microseconds each; 10.0 s of wall is the wedge tripwire
    # (co-tenancy slack over the observed sub-second fleet drain), not a
    # latency measurement.
    assert elapsed < 10.0, f"the fleet wedged: {elapsed}s for {n_jobs} deadline hits"
    assert len(backend.mark_failed_or_retry_calls) == n_jobs, "every attempt recorded"
    assert not backend.mark_succeeded_calls, "no zombie's return flipped a row"
    unretrieved = [w for w in caught if "never retrieved" in str(w.message)]
    assert not unretrieved, f"detached zombies leaked task outcomes: {unretrieved}"
    zombies = [t for t in live_tracked_actor_handles() if not t.done()]
    assert len(zombies) >= n_jobs, f"expected {n_jobs} tracked zombies, got {len(zombies)}"
    for z in zombies:
        z.cancel()
    for c in cleanups:
        c.cancel()
    await asyncio.gather(*zombies, *cleanups, return_exceptions=True)


# ── Attack 6: the tx ROLLBACK x the unwind, the shared connection ─────


async def test_tx_rollback_waits_out_the_unwind_before_touching_the_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The instrument, the transactional path's rollback x the unwind
    race. A tx-path body whose ``finally`` awaits ON the
    transaction connection past an 80ms deadline: the deadline's marker
    reached the transaction ``__aexit__`` while the unwind's statement was
    still in flight on the SHARED connection, the ROLLBACK collided with
    it on asyncpg's one-operation-at-a-time guard, and the failing
    ROLLBACK replaced the marker in ``__aexit__`` -- the row recorded the
    collision's ``InterfaceError`` instead of the truthful
    ``TimeoutError``, and the ``job_timeout`` log and the timeouts metric
    were lost with it.

    The obligation: the tx path bound-waits the unwind (the exit-wait
    budget) before the marker propagates into ``__aexit__`` -- the
    rollback runs on a quiesced connection, the unwind's cleanup completes
    untouched, and the row is the truthful deadline hit with its log line
    and its metric."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    tx_conn = _AtomicGuardConn(execute_delay=0.2)

    async def body(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        try:
            await asyncio.Event().wait()
        finally:
            # The unwind's cleanup awaits on the transaction connection,
            # in flight when the marker reaches the __aexit__.
            await tx_conn.execute("CLEANUP")

    with structlog.testing.capture_logs() as logs:
        outcome = await consume_one_job(
            as_backend(backend),
            _job(timedelta(milliseconds=80)),
            _WORKER_ID,
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            transaction_conn=tx_conn,
        )

    assert outcome == "scheduled", outcome
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError", (  # pyright: ignore[reportAttributeAccessIssue]
        write
    )
    assert any(call["kind"] == "start_to_close" for call in timeout_metric_calls), (
        timeout_metric_calls
    )
    assert any(entry.get("event") == "job_timeout" for entry in logs), logs
    assert "CLEANUP" in tx_conn.log, "the unwind's cleanup completed on the conn"
    assert "ROLLBACK" in tx_conn.log, "the tx ROLLBACK completed, never blocked by the guard"


@pytest.mark.load_sensitive
async def test_tx_hostile_finally_past_the_budget_still_ends_at_the_deadline() -> None:
    """The deferrable-deadline win survives the bound unwind wait on the
    transactional path: a hostile ``finally`` that awaits PAST the
    exit-wait budget cannot hold the attempt hostage either. At budget
    expiry the marker proceeds (the unwind detached tracked), so the
    attempt ends bounded -- deadline + budget + slack, never at the
    hostile cleanup's leisure -- and the row stays the truthful
    deadline hit."""
    backend = FakeBackend()
    cleanups: list[asyncio.Task[object]] = []

    async def body(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        try:
            await asyncio.Event().wait()
        finally:
            cleanup: asyncio.Task[object] = asyncio.ensure_future(asyncio.sleep(3600))
            cleanups.append(cleanup)
            await asyncio.shield(cleanup)  # outlives the budget

    started = asyncio.get_running_loop().time()
    outcome = await consume_one_job(
        as_backend(backend),
        _job(timedelta(milliseconds=80)),
        _WORKER_ID,
        run_actor=body,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        transaction_conn=_AtomicGuardConn(execute_delay=0.0),
    )
    elapsed = asyncio.get_running_loop().time() - started

    assert outcome == "scheduled", outcome
    assert elapsed >= _TX_UNWIND_WAIT_BUDGET, (
        f"the budget did not bound the wait: {elapsed}s < {_TX_UNWIND_WAIT_BUDGET}s"
    )
    assert elapsed < _TX_UNWIND_WAIT_BUDGET + 3.0, (
        f"the hostile cleanup held the attempt past the budget: {elapsed}s"
    )
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError"  # pyright: ignore[reportAttributeAccessIssue]
    zombies = live_tracked_actor_handles()
    assert any(not z.done() for z in zombies), "a hostile unwind past the budget stays tracked"
    # Teardown: reap the zombies and their shielded cleanups so the test
    # loop closes clean.
    for z in zombies:
        z.cancel()
    for c in cleanups:
        c.cancel()
    await asyncio.gather(*zombies, *cleanups, return_exceptions=True)


@pytest.mark.load_sensitive
async def test_tx_hostile_finally_holding_the_conn_past_the_budget_records_the_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The residual: the hostile ``finally``
    holds the SHARED transaction connection ITSELF -- a SHIELDED conn
    execute that outlives the deadline and the exit-wait budget. At budget
    expiry the marker proceeds, the transaction ``__aexit__``'s ROLLBACK
    collides with the still-in-flight statement on asyncpg's
    one-operation-at-a-time guard, and the collision's ``InterfaceError``
    replaced the marker -- the row recorded ``InterfaceError`` instead of
    the truthful ``TimeoutError``, the ``job_timeout`` log and the
    timeouts metric lost with it.

    The obligation: on the deadline path the row is always the truthful
    ``TimeoutError`` -- the capture boundary translates our own rollback's
    collision back to the deadline disposition -- the ``job_timeout`` log
    and the timeouts metric land, the attempt stays bounded (deadline +
    budget + slack), and the still-hostile unwind is captured tracked."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    # The hostile statement never finishes inside the test: it is still
    # in flight (the guard's busy flag held) when the budget expires and
    # the __aexit__'s ROLLBACK collides with it. Only the hostile cleanup
    # is delayed; the tx machinery's own savepoint/ROLLBACK are instant.
    tx_conn = _AtomicGuardConn(execute_delay=3600.0, delayed_queries={"CLEANUP"})
    held_ops: list[asyncio.Task[object]] = []

    async def body(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        try:
            await asyncio.Event().wait()
        finally:
            # The hostile cleanup: a shielded statement on the shared
            # transaction connection, outliving deadline + budget.
            op: asyncio.Task[object] = asyncio.ensure_future(tx_conn.execute("CLEANUP"))
            held_ops.append(op)
            await asyncio.shield(op)

    started = asyncio.get_running_loop().time()
    with structlog.testing.capture_logs() as logs:
        outcome = await consume_one_job(
            as_backend(backend),
            _job(timedelta(milliseconds=80)),
            _WORKER_ID,
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            transaction_conn=tx_conn,
        )
    elapsed = asyncio.get_running_loop().time() - started

    assert outcome == "scheduled", outcome
    assert elapsed >= _TX_UNWIND_WAIT_BUDGET, (
        f"the budget did not bound the wait: {elapsed}s < {_TX_UNWIND_WAIT_BUDGET}s"
    )
    assert elapsed < _TX_UNWIND_WAIT_BUDGET + 3.0, (
        f"the hostile conn op held the attempt past the budget: {elapsed}s"
    )
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "TimeoutError", (  # pyright: ignore[reportAttributeAccessIssue]
        write,
    )
    assert any(call["kind"] == "start_to_close" for call in timeout_metric_calls), (
        timeout_metric_calls
    )
    assert any(entry.get("event") == "job_timeout" for entry in logs), logs
    zombies = [t for t in live_tracked_actor_handles() if not t.done()]
    assert zombies, "the still-hostile unwind is not captured tracked"
    # Teardown: reap the zombie and its shielded conn op so the test loop
    # closes clean.
    for z in zombies:
        z.cancel()
    for op in held_ops:
        op.cancel()
    await asyncio.gather(*zombies, *held_ops, return_exceptions=True)


class _BodyOpFailsConn(_AtomicGuardConn):
    """A shared conn whose body-issued statement fails with a genuine
    ``InterfaceError`` (asyncpg's own failure class), well before the
    deadline: the honest case the collision re-label must never launder."""

    async def execute(self, query: str, *args: object) -> str:
        if query == "BODY_OP":
            raise asyncpg.exceptions.InterfaceError("the body's own statement failed")
        return await super().execute(query, *args)


async def test_body_caused_interface_error_outside_the_deadline_window_stays_interfaceerror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The honest-InterfaceError case: a body whose own conn op raises a
    genuine ``InterfaceError`` while the deadline (``_CARRIER_LIMIT``) is
    nowhere near -- armed only to engage the enforcement's task boundary,
    unreachable by construction, since the claim under test is that the
    deadline is not involved.
    The collision re-label keys on the deadline window alone -- it must
    never launder a body-caused ``InterfaceError`` into a deadline hit:
    the row stays ``InterfaceError``, no ``job_timeout`` log, no timeouts
    metric, and the ``__aexit__``'s ROLLBACK still completes (the conn is
    quiesced: no deadline, no hostile unwind)."""
    import structlog

    import taskq.worker._handlers as handlers_mod

    timeout_metric_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        handlers_mod,
        "record_job_timeout",
        lambda actor, *, kind, count=1: timeout_metric_calls.append({"actor": actor, "kind": kind}),
    )
    backend = FakeBackend()
    tx_conn = _BodyOpFailsConn(execute_delay=0.0)

    async def body(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        # The body's own statement on the shared conn fails genuinely,
        # long before the 150ms deadline could fire.
        await tx_conn.execute("BODY_OP")

    with structlog.testing.capture_logs() as logs:
        outcome = await consume_one_job(
            as_backend(backend),
            _job(_CARRIER_LIMIT),
            _WORKER_ID,
            run_actor=body,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            transaction_conn=tx_conn,
        )

    assert outcome == "scheduled", outcome
    write = backend.mark_failed_or_retry_calls[0]
    assert write["error_info"].error_class == "InterfaceError", (  # pyright: ignore[reportAttributeAccessIssue]
        write,
    )
    assert not timeout_metric_calls, timeout_metric_calls
    assert not any(entry.get("event") == "job_timeout" for entry in logs), logs
    assert "ROLLBACK" in tx_conn.log, "the tx ROLLBACK completed on the quiesced conn"


# ── The sentinel's own contract ───────────────────────────────────────


def test_the_deadline_marker_is_a_timeouterror() -> None:
    """The marker subclasses ``TimeoutError``: every handler, hook and
    classifier that catches TimeoutError keeps working when the deadline
    fires, while ``_dispatch_exception`` can still tell WHO raised it."""
    from taskq.worker._handlers import _StartToCloseExceededError

    marker = _StartToCloseExceededError("start_to_close")
    assert isinstance(marker, TimeoutError)
    assert not isinstance(TimeoutError("body's own"), _StartToCloseExceededError)
