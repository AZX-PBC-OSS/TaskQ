"""Tests for InMemoryBackend dispatch loop.

Covers dispatch_batch, run_until_drained, and related dispatch behaviour:
- state-machine reachability (enqueue + run_until_drained → succeeded)
- retry reachability (stub fails twice, succeeds third; attempt == 3)
- determinism (two backends with same clock and order → equal state)
- clock time-travel (scheduled job dispatchable after advance)
- termination on perpetual snooze (no clock advance → returns)
- run_until_drained returns immediately when clock lacks move_to
- no double-dispatch
- dispatch queue filtering
- enqueue initial-status bifurcation
- clock advance during dispatch
- isinstance(InMemoryBackend, Backend) returns True
"""

# pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType, reportPrivateUsage]
# Why: StubFn is Callable[..., object] by design; stub lambdas
# inherently have unknown parameter types.  Private access to _jobs and
# _worker_id is for test-only inspection.

from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_job_id
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import IdentityKey, JobId, RetryKind
from taskq.exceptions import ReservationUnavailable, Snooze
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)


def _make_backend(
    clock: FakeClock | None = None,
) -> InMemoryBackend:
    clk = clock or FakeClock(_START)
    backend = InMemoryBackend(clock=clk)
    backend.register_actor_config(actor="test_actor")
    backend.register_actor_config(actor="async_actor")
    backend.register_actor_config(actor="a1")
    backend.register_actor_config(actor="a2")
    backend.register_actor_config(actor="actor")
    backend.register_actor_config(actor="res_actor")
    backend.register_actor_config(actor="flaky_actor")
    backend.register_actor_config(actor="snoozy")
    return backend


def _enqueue_args(
    actor: str = "test_actor",
    queue: str = "default",
    scheduled_at: datetime | None = None,
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    priority: int = 0,
    schedule_to_close: datetime | None = None,
    identity_key: IdentityKey | None = None,
) -> EnqueueArgs:
    """Build minimal EnqueueArgs for testing."""
    return make_enqueue_args(
        actor=actor,
        queue=queue,
        payload={"key": "value"},
        scheduled_at=scheduled_at or _START,
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        priority=priority,
        schedule_to_close=schedule_to_close,
        identity_key=str(identity_key) if identity_key is not None else None,
    )


# ── state-machine reachability ────────────────────────────────


class TestStateMachineReachability:
    async def test_succeeding_stub(self) -> None:
        """enqueue + run_until_drained with a succeeding stub;
        final status == "succeeded", attempt == 1, one attempt row with
        outcome="succeeded".
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_stub("test_actor", lambda payload, ctx: {"ok": True})

        args = _enqueue_args(actor="test_actor")
        await backend.enqueue(args)
        await backend.run_until_drained()

        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.attempt == 1

        attempts = await backend.get_attempts(args.id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "succeeded"


# ── retry reachability ────────────────────────────────────────


class TestRetryReachability:
    async def test_fails_twice_succeeds_third(self) -> None:
        """stub raises generic Exception twice, succeeds on third
        attempt; max_attempts=3; final status == "succeeded", attempt == 3.

        run_until_drained advances FakeClock to scheduled_at when a
        retry is scheduled in the future, so the loop continues.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        call_count = 0

        def flaky_stub(payload: object, ctx: object) -> object:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RuntimeError("transient failure")
            return {"ok": True}

        backend.register_stub("flaky_actor", flaky_stub)

        args = _enqueue_args(actor="flaky_actor", max_attempts=3)
        await backend.enqueue(args)
        await backend.run_until_drained()

        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.attempt == 3
        assert call_count == 3


# ── determinism ───────────────────────────────────────────────


class TestDeterminism:
    async def test_identical_backends_produce_equal_state(self) -> None:
        """two InMemoryBackend instances with identical FakeClock
        and same enqueue order produce equal final job lists (same
        statuses and attempts per actor).
        """
        results: list[list[tuple[str, str, int]]] = []

        for _ in range(2):
            clock = FakeClock(_START)
            backend = _make_backend(clock)
            backend.register_stub("a1", lambda p, c: {"v": 1})
            backend.register_stub("a2", lambda p, c: {"v": 2})

            # Enqueue same jobs in same order
            args1 = _enqueue_args(actor="a1", priority=5, scheduled_at=_START)
            args2 = _enqueue_args(actor="a2", priority=3, scheduled_at=_START)
            args3 = _enqueue_args(
                actor="a1", priority=5, scheduled_at=_START + timedelta(seconds=10)
            )

            await backend.enqueue(args1)
            await backend.enqueue(args2)
            await backend.enqueue(args3)

            await backend.run_until_drained()

            # Compare by (actor, status, attempt) — UUIDs differ between runs
            state = sorted([(r.actor, r.status, r.attempt) for r in backend._jobs.values()])
            results.append(state)

        assert results[0] == results[1]


# ── clock time-travel ─────────────────────────────────────────


class TestClockTimeTravel:
    async def test_scheduled_job_dispatchable_after_advance(self) -> None:
        """enqueue scheduled_at = now+1h; first dispatch_batch
        returns empty; fake_clock.advance(1h); call scheduled_to_pending;
        second dispatch_batch returns the job.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_stub("test_actor", lambda p, c: None)

        future = _START + timedelta(hours=1)
        args = _enqueue_args(actor="test_actor", scheduled_at=future)
        await backend.enqueue(args)

        # Not yet dispatchable (still scheduled, not pending)
        result = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert result == []

        # Advance clock and promote scheduled→pending
        clock.advance(timedelta(hours=1))
        await backend.scheduled_to_pending(clock.now())

        # Now dispatchable
        result = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(result) == 1
        assert result[0].id == args.id


# ── termination on perpetual snooze ──────────────────────────


class TestPerpetualSnooze:
    async def test_snooze_advances_clock_and_reschedules(self) -> None:
        """stub raises Snooze(timedelta(seconds=10)) every call;
        run_until_drained() advances the FakeClock and re-dispatches.
        The job stays "scheduled" after each snooze, and the clock
        moves forward by the snooze delay each iteration.

        With a perpetual snooze and FakeClock, the loop would run
        forever, so we use a snooze stub that snoozes once then succeeds.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        call_count = 0

        def snoozy_stub(payload: object, ctx: object) -> object:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Snooze(timedelta(seconds=10))
            return {"done": True}

        backend.register_stub("snoozy", snoozy_stub)

        args = _enqueue_args(actor="snoozy")
        await backend.enqueue(args)
        await backend.run_until_drained()

        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "succeeded"
        assert call_count == 3  # snoozed twice, succeeded third
        # Clock should have advanced by at least 2 * 10s
        assert clock.now() >= _START + timedelta(seconds=20)


# ── run_until_drained returns immediately when clock lacks move_to ─


class TestNoClockAdvanceReturns:
    async def test_returns_with_scheduled_job_when_no_move_to(self) -> None:
        """When the clock lacks ``move_to`` (e.g. SystemClock),
        ``run_until_drained`` returns immediately if nothing is
        dispatchable but a future-scheduled job exists.  The job stays
        in ``scheduled`` status.  This tests the "no FakeClock, return"
        branch that was previously untested (review finding 4).
        """
        from dataclasses import replace as _replace

        from taskq.backend.clock import SystemClock

        backend = InMemoryBackend(clock=SystemClock())
        backend.register_actor_config(actor="snoozy")
        backend.register_stub("snoozy", lambda p, c: {"done": True})

        # Enqueue an immediate job, then dispatch + snooze it into
        # the future to create a scheduled job that can't be dispatched
        # without clock advancement.
        args = _enqueue_args(actor="snoozy")
        await backend.enqueue(args)

        # Manually dispatch the job
        wid = backend._worker_id
        dispatched = await backend.dispatch_batch(wid, ["default"], 1, _GRACE)
        assert len(dispatched) == 1
        job = dispatched[0]

        # Snooze: running → scheduled with future scheduled_at
        future = datetime.now(UTC) + timedelta(hours=1)
        backend._jobs[job.id] = _replace(
            job,
            status="scheduled",
            scheduled_at=future,
            locked_by_worker=None,
            lock_expires_at=None,
        )

        # run_until_drained should return immediately (no move_to on clock)
        await backend.run_until_drained()

        # The job should still be scheduled (not dispatched again)
        final_row = await backend.get(args.id)
        assert final_row is not None
        assert final_row.status == "scheduled"

    async def test_returns_on_empty_queue(self) -> None:
        """run_until_drained returns immediately when there are
        no jobs at all (the fully-drained termination condition).
        """
        from taskq.backend.clock import SystemClock

        backend = InMemoryBackend(clock=SystemClock())

        # No jobs — should return immediately
        await backend.run_until_drained()


# ── no double-dispatch ────────────────────────────────────────


class TestNoDoubleDispatch:
    async def test_dispatch_returns_job_once(self) -> None:
        """enqueue 1; dispatch_batch returns it; dispatch_batch
        again returns []. ()
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        args = _enqueue_args()
        await backend.enqueue(args)

        result1 = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(result1) == 1

        result2 = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert result2 == []


# ── dispatch queue filtering ─────────────────────────────────


class TestQueueFiltering:
    async def test_dispatch_filters_by_queue(self) -> None:
        """enqueue two jobs on different queues; dispatch_batch
        with queues=["q1"] returns only "q1" job; then dispatch with
        queues=["q2"] returns the "q2" job.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        args_q1 = _enqueue_args(queue="q1", actor="a1")
        args_q2 = _enqueue_args(queue="q2", actor="a2")
        await backend.enqueue(args_q1)
        await backend.enqueue(args_q2)

        # Dispatch only q1
        result = await backend.dispatch_batch(backend._worker_id, ["q1"], 10, _GRACE)
        assert len(result) == 1
        assert result[0].queue == "q1"

        # q2 is still pending
        row_q2 = await backend.get(args_q2.id)
        assert row_q2 is not None
        assert row_q2.status == "pending"

        # Now dispatch q2
        result = await backend.dispatch_batch(backend._worker_id, ["q2"], 10, _GRACE)
        assert len(result) == 1
        assert result[0].queue == "q2"


# ── enqueue initial-status bifurcation ──────────────────────


class TestInitialStatusBifurcation:
    async def test_immediate_vs_future_status(self) -> None:
        """enqueue two jobs: one with scheduled_at = clock.now()
               (immediate), one with scheduled_at = clock.now() + 1h (future).
               Assert the immediate job has status == "pending" and the future
        job has status == "scheduled". Validates / §3.5.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        immediate_args = _enqueue_args(scheduled_at=_START)
        future_args = _enqueue_args(scheduled_at=_START + timedelta(hours=1))

        row_imm = await backend.enqueue(immediate_args)
        row_fut = await backend.enqueue(future_args)

        assert row_imm.status == "pending"
        assert row_fut.status == "scheduled"


# ── clock advance during dispatch ──────────────────────────────


class TestClockAdvanceDuringDispatch:
    async def test_no_double_dispatch_after_clock_advance(self) -> None:
        """schedule job_A at t+10s, job_B at t+5s; dispatch at t
        → empty; advance to t+7s → only job_B; advance to t+15s →
        job_A available; no double-dispatch of job_B.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        args_b = _enqueue_args(scheduled_at=_START + timedelta(seconds=5))
        args_a = _enqueue_args(scheduled_at=_START + timedelta(seconds=10))
        await backend.enqueue(args_b)
        await backend.enqueue(args_a)

        # At t: nothing dispatchable
        result = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert result == []

        # Advance to t+7s: only job_B
        clock.advance(timedelta(seconds=7))
        await backend.scheduled_to_pending(clock.now())
        result = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(result) == 1
        assert result[0].id == args_b.id

        # job_B again → empty (already running)
        result = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert result == []

        # Advance to t+15s: job_A now dispatchable
        clock.advance(timedelta(seconds=8))
        await backend.scheduled_to_pending(clock.now())
        result = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(result) == 1
        assert result[0].id == args_a.id


# ── isinstance(InMemoryBackend, Backend) ──────────────────────


class TestRuntimeCheckable:
    def test_isinstance_backend_true(self) -> None:
        """isinstance(InMemoryBackend(FakeClock(...)), Backend)
        returns True.
        """
        backend = _make_backend()
        assert isinstance(backend, Backend)

    def test_isinstance_backend_false_for_object(self) -> None:
        """isinstance(object(), Backend) returns False."""
        assert not isinstance(object(), Backend)


# ── Async stub support ────────────────────────────────────────────────


class TestAsyncStub:
    async def test_async_stub_awaited(self) -> None:
        """run_until_drained awaits async stubs correctly."""
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        async def async_stub(payload: object, ctx: object) -> dict[str, object]:
            return {"async": True}

        backend.register_stub("async_actor", async_stub)

        args = _enqueue_args(actor="async_actor")
        await backend.enqueue(args)
        await backend.run_until_drained()

        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.result == {"async": True}


# ── Unregistered actor raises RuntimeError ─────────────────────────────


class TestUnregisteredActor:
    async def test_missing_stub_raises(self) -> None:
        """Unregistered actor stub raises RuntimeError."""
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        # Register actor_config so it dispatches, but no stub registered
        backend.register_actor_config(actor="missing_actor")

        args = _enqueue_args(actor="missing_actor")
        await backend.enqueue(args)

        with pytest.raises(RuntimeError, match="no stub registered for actor: missing_actor"):
            await backend.run_until_drained()


# ── ReservationUnavailable handling via consume_one_job ─────────────────


async def test_run_until_drained_handles_reservation_unavailable() -> None:
    """ReservationUnavailable raised by stub produces a scheduled row
    with metadata['awaiting'] == 'reservation:<bucket>' and an attempt
    row with outcome='reservation_denied'.
    """
    clock = FakeClock(_START)
    backend = _make_backend(clock)

    call_count = 0

    def res_stub(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ReservationUnavailable("gpu_pool", timedelta(seconds=10))
        return {"ok": True}

    backend.register_stub("res_actor", res_stub)

    args = _enqueue_args(actor="res_actor")
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.metadata.get("awaiting") == "reservation:gpu_pool"

    attempts = await backend.get_attempts(args.id)
    reservation_attempts = [a for a in attempts if a.outcome == "reservation_denied"]
    assert len(reservation_attempts) == 1


# ── Single-actor cap ─────────────────────────────────────────────


class TestSingleActorCap:
    async def test_dispatch_respects_max_concurrent(self) -> None:
        """enqueue 5 jobs, register max_concurrent=2, dispatch limit=10.
        Oracle: exactly 2 transition to running; 3 remain pending.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_actor_config(actor="capped_actor", max_concurrent=2)

        args = [_enqueue_args(actor="capped_actor") for _ in range(5)]
        for a in args:
            await backend.enqueue(a)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(dispatched) == 2

        for row in dispatched:
            assert row.status == "running"

        pending = [r for r in backend._jobs.values() if r.status == "pending"]
        assert len(pending) == 3


# ── Per-identity serialization ───────────────────────────────────


class TestIdentitySerialization:
    async def test_identical_identity_key_serializes(self) -> None:
        """enqueue 3 jobs with same (actor, identity_key) and max_concurrent=10.
        Oracle: exactly 1 transitions to running; 2 remain pending.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_actor_config(actor="id_actor", max_concurrent=10)

        key = IdentityKey("shared_key")
        args = [_enqueue_args(actor="id_actor", identity_key=key) for _ in range(3)]
        for a in args:
            await backend.enqueue(a)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(dispatched) == 1

        pending = [r for r in backend._jobs.values() if r.status == "pending"]
        assert len(pending) == 2


# ── Different identities dispatched concurrently ──────────────────


class TestDifferentIdentities:
    async def test_different_identity_keys_all_dispatch(self) -> None:
        """enqueue 3 jobs with different identity_key and max_concurrent=10.
        Oracle: all 3 transition to running.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_actor_config(actor="id_actor", max_concurrent=10)

        args = [
            _enqueue_args(actor="id_actor", identity_key=IdentityKey("a")),
            _enqueue_args(actor="id_actor", identity_key=IdentityKey("b")),
            _enqueue_args(actor="id_actor", identity_key=IdentityKey("c")),
        ]
        for a in args:
            await backend.enqueue(a)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(dispatched) == 3
        assert all(r.status == "running" for r in dispatched)


# ── No identity_key means no serialization ────────────────────────


class TestNoIdentityKey:
    async def test_no_identity_key_dispatches_all(self) -> None:
        """enqueue 2 jobs with identity_key=None and max_concurrent=10.
        Oracle: both transition to running.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_actor_config(actor="actor", max_concurrent=10)

        args = [_enqueue_args(actor="actor", identity_key=None) for _ in range(2)]
        for a in args:
            await backend.enqueue(a)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(dispatched) == 2
        assert all(r.status == "running" for r in dispatched)


# ── schedule_to_close filter ──────────────────────────────────────


class TestScheduleToClose:
    async def test_schedule_to_close_in_past_not_dispatched(self) -> None:
        """enqueue job with schedule_to_close in the past.
        Oracle: job remains pending.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        closed = _START - timedelta(seconds=1)
        args = _enqueue_args(schedule_to_close=closed)
        await backend.enqueue(args)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert len(dispatched) == 0

        row = await backend.get(args.id)
        assert row is not None
        assert row.status == "pending"


# ── Unbounded actor ──────────────────────────────────────────────


class TestUnboundedActor:
    async def test_max_concurrent_none_dispatches_limited_by_limit(self) -> None:
        """register actor with max_concurrent=None, enqueue 100, dispatch limit=50.
        Oracle: 50 dispatch.
        """
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_actor_config(actor="unbounded", max_concurrent=None)

        args = [_enqueue_args(actor="unbounded") for _ in range(100)]
        for a in args:
            await backend.enqueue(a)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 50, _GRACE)
        assert len(dispatched) == 50


# ── No matching queues ──────────────────────────────────────────


class TestNoMatchingQueues:
    async def test_dispatch_nonexistent_queue_returns_empty(self) -> None:
        """dispatch with queues=["nonexistent"]. Oracle: empty result, no error."""
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        args = _enqueue_args(queue="default")
        await backend.enqueue(args)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["nonexistent"], 10, _GRACE)
        assert dispatched == []


# ── All candidates exceed cap ────────────────────────────────────


class TestCapZero:
    async def test_max_concurrent_zero_blocks_all(self) -> None:
        """enqueue 10 jobs, max_concurrent=0. Oracle: empty result, all pending."""
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        backend.register_actor_config(actor="capped", max_concurrent=0)

        args = [_enqueue_args(actor="capped") for _ in range(10)]
        for a in args:
            await backend.enqueue(a)

        dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
        assert dispatched == []

        pending = [r for r in backend._jobs.values() if r.status == "pending"]
        assert len(pending) == 10


# ── Determinism ─────────────────────────────────────────────────────────


class TestDispatchDeterminism:
    async def test_identical_backends_same_dispatch_order(self) -> None:
        """Two InMemoryBackends enqueue the same jobs in the same order;
        dispatched lists are identical in (id, status) order.
        """
        from taskq._ids import new_job_id

        results: list[list[tuple[JobId, str]]] = []
        pre_ids: list[JobId] = [new_job_id() for _ in range(5)]

        for _ in range(2):
            clock = FakeClock(_START)
            backend = _make_backend(clock)
            backend.register_actor_config(actor="actor", max_concurrent=5)

            for i, jid in enumerate(pre_ids):
                await backend.enqueue(
                    EnqueueArgs(
                        id=jid,
                        actor="actor",
                        queue="default",
                        payload={"key": "value"},
                        max_attempts=3,
                        retry_kind="transient",
                        scheduled_at=_START,
                        priority=5 - i,
                    )
                )

            dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
            results.append([(r.id, r.status) for r in dispatched])

        assert results[0] == results[1]


# ── Hypothesis property test ─────────────────────────────────────


_id_actor_names = st.sampled_from(["id_a", "id_b", "id_c"])


def _build_ident(actor: str, ik: str | None) -> tuple[str, IdentityKey | None]:
    return (actor, IdentityKey(ik) if ik is not None else None)


_job_spec = st.builds(
    _build_ident,
    actor=_id_actor_names,
    ik=st.one_of(st.none(), st.text(min_size=1, max_size=10)),
)


@settings(max_examples=200, deadline=None)
@given(jobs=st.lists(_job_spec, min_size=0, max_size=40))
async def test_property_identity_invariant(
    jobs: list[tuple[str, str | None]],
) -> None:
    """after dispatch, at most one job per (actor, identity_key) is running.

    For every (actor, identity_key) pair, the number of running jobs is 0 or 1.
    Identity serialization means at most one member of each equivalence class
    can be dispatched.
    """
    clock = FakeClock(_START)
    backend = _make_backend(clock)

    # Register all actors with generous caps so identity is the binding constraint
    for actor_name in ("id_a", "id_b", "id_c"):
        backend.register_actor_config(actor=actor_name, max_concurrent=100)

    # Enqueue all generated job specs
    for actor_name, ik_raw in jobs:
        ident: IdentityKey | None = IdentityKey(ik_raw) if ik_raw is not None else None
        args = _enqueue_args(actor=actor_name, identity_key=ident)
        await backend.enqueue(args)

    # Dispatch everything in one big batch
    dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 200, _GRACE)

    # Group running jobs by (actor, identity_key)
    running_by_ident: dict[tuple[str, str], int] = {}
    for row in dispatched:
        if row.status == "running" and row.identity_key is not None:
            key = (row.actor, row.identity_key)
            running_by_ident[key] = running_by_ident.get(key, 0) + 1

    # Invariant: at most 1 per (actor, identity_key)
    for key, count in running_by_ident.items():
        assert count <= 1, (
            f"identity violation: (actor={key[0]!r}, identity_key={key[1]!r}) "
            f"has {count} running jobs"
        )

    # Secondary invariant: ALL dispatched rows have status "running"
    for row in dispatched:
        assert row.status == "running"


# ── dispatch_batch: schedule_to_close interval filter mirrors PG ──────


async def test_dispatch_batch_respects_schedule_to_close_interval() -> None:
    """§11.3 mirror of PG dispatch filter: enqueue an indefinite-tier job
    with schedule_to_close_interval=timedelta(seconds=1), advance clock
    past the deadline, call dispatch_batch → empty result.
    """
    clock = FakeClock(_START)
    backend = _make_backend(clock)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="indefinite",
        scheduled_at=_START,
        schedule_to_close_interval=timedelta(seconds=1),
    )
    await backend.enqueue(args)

    clock.advance(timedelta(seconds=2))

    dispatched = await backend.dispatch_batch(backend._worker_id, ["default"], 10, _GRACE)
    assert dispatched == []


# ── SubJobEnqueuer wiring regression test ──────────────────────────────


async def test_consume_one_job_receives_enqueuer() -> None:
    """consume_one_job uses the passed enqueuer instance for ctx.jobs,
    verifying the wiring from _main → di_consumer_loop → dispatch_one_job
    → consume_one_job."""
    from typing import cast

    from pydantic import BaseModel as _BaseModel

    from taskq._ids import new_job_id, new_uuid
    from taskq.backend._protocol import Backend
    from taskq.backend._protocol import JobRow as _JobRow
    from taskq.backend.clock import Clock
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.retry import OnRetryExhausted, RetryPolicy
    from taskq.testing.clock import FakeClock as _FakeClock
    from taskq.worker._consumer import consume_one_job

    _now = _START
    _wid = new_uuid()

    class _P(_BaseModel):
        pass

    captured_enqueuer: SubJobEnqueuer | None = None

    async def actor(_job: object, ctx: JobContext[_BaseModel]) -> object:
        nonlocal captured_enqueuer
        if isinstance(ctx, JobContext):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: isinstance check is runtime validation; type narrowing ensures safe attribute access.
            captured_enqueuer = ctx.jobs
        return None

    class _StubConfig:
        retry: RetryPolicy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
        non_retryable_exceptions: tuple[type[Exception], ...] = ()
        retry_classifier: None = None
        on_retry_exhausted: OnRetryExhausted | None = None
        on_retry_exhausted_timeout: float = 3.0
        on_success: None = None
        on_success_timeout: float = 3.0

    clock = _FakeClock(_START)
    _backend = _make_backend(clock)

    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=None,
        backend=_backend,
    )

    job = _JobRow(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=_now,
        scheduled_at=_now,
        started_at=_now,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=_wid,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=0,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )

    clk: Clock = _FakeClock(_now)

    await consume_one_job(
        cast(Backend, _backend),
        job,
        _wid,
        run_actor=actor,
        actor_config=_StubConfig(),
        payload_type=_P,
        clock=clk,
        enqueuer=enqueuer,
    )

    assert captured_enqueuer is enqueuer
