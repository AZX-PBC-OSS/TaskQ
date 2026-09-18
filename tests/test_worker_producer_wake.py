"""Producer slot-refill wake and poll-jitter pins.

Two dispatch-loop latencies these pins hold:

* Slot refill under saturation — when every consumer slot is busy and the
  local queue is full, the producer waits on the slot-freed event the
  consumer loops set at their ``local_queue.get()`` (the point a queue
  slot actually frees — ``qsize`` drops at get, not at job completion),
  bounded by the fallback interval so a never-set event cannot park the
  producer. Waking on a slot-release event is the mechanism to remove
  latency when slots free: the producer claims the next job the instant
  a slot releases, not after waiting out a poll interval.
* Fallback poll jitter — an idle fleet polling the same interval in
  phase re-synchronizes after any transient event into periodic DB load
  spikes; the empty-dispatch wait is jittered ±10% to spread the fleet's
  polls across the interval rather than ticking in unison, seeded
  per-producer like the retry RNG.

Timing is deliberately NOT the gate: the wake pin asserts the producer
needed no poll sleep at all (a sleep recorder — the event wake is
µs-fast while a poll wake lands anywhere inside the interval, so a
timing race would flake exactly on the distinction under test), and the
jitter pins assert on seeded RNG draws, which are deterministic.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobId, JobRow
from taskq.backend.clock import Clock
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker import run as run_mod
from taskq.worker.run import (
    _POLL_JITTER_FRACTION,
    _SLOT_REFILL_POLL_SECONDS,
    _jittered_poll_interval,
    consumer_loop_stub,
    di_consumer_loop,
    producer_loop,
)

_PROMPT_WAKE_BOUND_S = 1.0
_EPS = 1e-9


class _NoopPool:
    """asyncpg.Pool stand-in for the producer's exit hand-back.

    The producer hands its held rows back on the drain path's way out
    (``drain_local_queue_to_pending``); these tests drive the loop with a
    namespace deps, so the pool answers the one bounded statement with an
    empty UPDATE tag.
    """

    class _Conn:
        async def __aenter__(self) -> _NoopPool._Conn:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def execute(self, *_args: object) -> str:
            return "UPDATE 0"

    def acquire(self, *, timeout: float | None = None) -> _NoopPool._Conn:
        return self._Conn()


def _producer_deps(
    *, poll_interval: float = 5.0, maxsize: int = 1, pooled: bool = False
) -> SimpleNamespace:
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=poll_interval,
        notify_poll_interval=poll_interval,
        max_concurrency=maxsize,
        schema_name="taskq",
        pg_is_pooled=pooled,
    )
    liveness = SimpleNamespace(tick=lambda *args, **kwargs: None, forget=lambda *a, **k: None)
    return SimpleNamespace(
        settings=settings,
        liveness=liveness,
        active_jobs=SimpleNamespace(all=list),
        disowned_jobs=set(),
        dispatcher_pool=_NoopPool(),
    )


class _RecordingBackend:
    """dispatch_batch that records call times and answers from a list."""

    def __init__(self, jobs: list[JobRow] | None = None) -> None:
        self.jobs = jobs if jobs is not None else []
        self.dispatch_calls: list[float] = []

    async def dispatch_batch(
        self,
        *,
        worker_id: object,
        queues: object,
        limit: int,
        lock_lease: object,
    ) -> list[JobRow]:
        self.dispatch_calls.append(time.monotonic())
        if self.jobs and limit > 0:
            return [self.jobs.pop(0)]
        return []


class _RaisingBackend:
    """dispatch_batch that raises a fixed exception every round and
    counts the rounds - the pooler-remap storm's stand-in."""

    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.rounds = 0

    async def dispatch_batch(
        self,
        *,
        worker_id: object,
        queues: object,
        limit: int,
        lock_lease: object,
    ) -> list[JobRow]:
        self.rounds += 1
        raise self.exc


# ── Slot refill: the producer wakes on the consumer's release ───────────


async def test_slot_release_wakes_the_saturated_producer_without_a_poll_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A saturated producer (all slots busy, local queue full) claims the
    next job on the consumer's slot-release event — without any poll
    sleep. The sleep recorder is the gate: an event wake needs no timer,
    while a poll-loop producer must sleep inside the interval to notice
    the freed slot, which is exactly the up-to-100ms claim latency this
    seam removes.

    The fallback bound is patched to 3600 s so a producer that missed
    the set entirely cannot pass by waking on the old 100 ms cadence —
    it would hang and fail the prompt-wait wait, loudly.
    """
    backend = _RecordingBackend(jobs=[make_job_row(status="pending") for _ in range(3)])
    deps = _producer_deps(poll_interval=5.0, maxsize=1)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    await local_queue.put(make_job_row(status="pending"))  # saturated: queue full, no consumer

    slot_freed = asyncio.Event()
    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()

    # (calling task, duration): the patch is global to the asyncio module,
    # so the test's own waits and the wait helper's polls land here too —
    # filtered by task identity, the producer's own waits are the gate.
    # Signature mirrors asyncio.sleep(delay, result) — the signature-drift
    # guard (test_double_signature_drift.py) fails narrower doubles.
    sleeps: list[tuple[object, float]] = []
    real_sleep = asyncio.sleep

    async def _recording_sleep(delay: float, result: object = None) -> object:
        # Record, then yield once without sleeping the delay: the producer
        # keeps cycling so the test coroutine stays scheduled, while the
        # recorder still sees every timer-based wait.
        sleeps.append((asyncio.current_task(), delay))
        await real_sleep(0)
        return result

    monkeypatch.setattr(run_mod, "_SLOT_REFILL_POLL_SECONDS", 3600.0, raising=False)
    monkeypatch.setattr(run_mod.asyncio, "sleep", _recording_sleep)

    task = asyncio.create_task(
        producer_loop(
            deps,  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in for WorkerDeps, the established producer-loop unit pattern (tests/test_watchdog_safety.py).
            local_queue,
            shutdown_event,
            stop_event,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
            slot_freed_event=slot_freed,
        )
    )
    try:
        await asyncio.sleep(0.05)
        assert backend.dispatch_calls == [], "a saturated producer must not dispatch"

        # The consumer-side release: get() drains the slot, then the
        # consumer loop sets the event at exactly that point.
        await local_queue.get()
        released_at = time.monotonic()
        slot_freed.set()

        await wait_for_condition(
            lambda: len(backend.dispatch_calls) >= 1,
            description="dispatch after slot release",
            timeout=2.0,
        )

        assert backend.dispatch_calls[0] - released_at < _PROMPT_WAKE_BOUND_S, (
            "the slot-release event must wake the producer promptly — a "
            "producer still waiting out a poll interval after a slot freed "
            "is the latency this seam exists to remove"
        )
        producer_sleeps = [duration for caller, duration in sleeps if caller is task]
        assert producer_sleeps == [], (
            f"the saturated producer slept {producer_sleeps} — the slot-release "
            "wake is not working; the producer is back to timer-polling while "
            "consumers wait for the next claim"
        )
    finally:
        # The 3600 s bound makes a clean stop-event exit take an hour;
        # cancelling is this test's own cleanup, not production behaviour
        # (the real bound is 100 ms).
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_saturated_producer_still_notices_stop_within_the_fallback_bound() -> None:
    """The saturation wait is bounded, not bare: with no consumer ever
    setting the event, a stop must still be noticed within the fallback
    interval — a bare ``event.wait()`` parks the producer until a
    consumer acts and the drain path stalls."""
    backend = _RecordingBackend()
    deps = _producer_deps(poll_interval=5.0, maxsize=1)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    await local_queue.put(make_job_row(status="pending"))

    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()

    task = asyncio.create_task(
        producer_loop(
            deps,  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in, as above.
            local_queue,
            shutdown_event,
            stop_event,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
        )
    )
    await asyncio.sleep(0.05)
    assert backend.dispatch_calls == []

    stop_event.set()
    await asyncio.wait_for(task, timeout=_SLOT_REFILL_POLL_SECONDS * 5)


# ── Slot refill: the consumer loops signal the release point ────────────


async def test_di_consumer_loop_signals_slot_release_when_it_takes_a_job() -> None:
    """The production consumer loop sets the slot-freed event at its
    get() — driven through the actor-not-found branch, which exercises
    the loop's full race/get/processing shape with no DI scaffolding:
    every iteration passes the release point regardless of how the job
    ends."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    process_scope = SimpleNamespace(get=lambda t: clock if t is Clock else None)
    shutdown_event = asyncio.Event()
    snoozed: list[JobId] = []

    class _SnoozingBackend:
        async def mark_snoozed(
            self,
            job_id: JobId,
            worker_id: object,
            delay: object,
            *,
            metadata_update: dict[str, object] | None = None,
            attempt: int | None = None,
        ) -> str:
            snoozed.append(job_id)
            shutdown_event.set()  # one job is enough — exit after it
            return "scheduled"

    job = make_job_row(status="pending")
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    await local_queue.put(job)

    slot_freed = asyncio.Event()

    await asyncio.wait_for(
        di_consumer_loop(
            # The loop reads the DRAINING signal off deps on every
            # iteration (the consumer stop-pulling gate); the rest of deps
            # is unused on the actor-not-found path.
            SimpleNamespace(producer_stop_event=asyncio.Event()),  # type: ignore[arg-type]  # Why: minimal stand-in carrying the one field the loop's gate reads; the signature still requires the full WorkerDeps.
            local_queue,
            shutdown_event,
            backend=cast(Backend, _SnoozingBackend()),  # type: ignore[arg-type]  # Why: structural stand-in satisfying the mark_snoozed call the loop makes.
            worker_id=new_uuid(),
            registry=cast(Any, SimpleNamespace()),
            process_scope=cast(Any, process_scope),
            thread_scope=cast(Any, SimpleNamespace()),
            loop_scope=cast(Any, SimpleNamespace()),
            actor_registry={},
            enqueuer=cast(Any, Mock()),
            slot_freed_event=slot_freed,
        ),
        timeout=2.0,
    )

    assert snoozed == [job.id], "the loop must have processed the job it took"
    assert slot_freed.is_set(), (
        "the consumer loop did not signal the slot release at its get() — "
        "a saturated producer polls on the fallback cadence instead of "
        "claiming the moment this loop freed a slot"
    )


async def test_consumer_loop_stub_signals_slot_release_when_it_takes_a_job() -> None:
    """The stub consumer sets the event at the same release point, so a
    stub-mode worker's producer wakes too (the bootstrap wires one event
    into whichever consumer loop it spawns)."""
    from taskq.worker.deps import WorkerDeps

    deps = WorkerDeps(  # type: ignore[arg-type]  # Why: real ActiveJobRegistry via the default factory; pools and settings are never touched on the stub path.
        settings=SimpleNamespace(),
        dispatcher_pool=object(),
        heartbeat_pool=object(),
        worker_pool=object(),
        notify_conn=None,
        leader_conn=None,
    )
    shutdown_event = asyncio.Event()

    class _StubBackend:
        async def mark_succeeded(
            self,
            job_id: object,
            worker_id: object,
            result: object,
            fallback_result_ttl: object = None,
            *,
            attempt: int | None = None,
        ) -> bool:
            shutdown_event.set()  # exit after one job
            return True

    job = make_job_row(status="pending")
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    await local_queue.put(job)

    slot_freed = asyncio.Event()

    await asyncio.wait_for(
        consumer_loop_stub(
            deps,
            local_queue,
            shutdown_event,
            backend=cast(Backend, _StubBackend()),  # type: ignore[arg-type]  # Why: structural stand-in satisfying mark_succeeded.
            worker_id=new_uuid(),
            stub_work_timeout=0.01,
            slot_freed_event=slot_freed,
        ),
        timeout=2.0,
    )

    assert slot_freed.is_set()


# ── Fallback poll jitter ────────────────────────────────────────────────


def test_jittered_poll_interval_stays_within_the_jitter_band() -> None:
    """Every draw stays inside ±_POLL_JITTER_FRACTION of the configured
    interval — jitter spreads the fleet, it never lengthens or shortens
    the cadence beyond the band."""
    rng = random.Random(1234)
    interval = 5.0
    lo = interval * (1.0 - _POLL_JITTER_FRACTION) - _EPS
    hi = interval * (1.0 + _POLL_JITTER_FRACTION) + _EPS
    draws = [_jittered_poll_interval(interval, rng) for _ in range(1000)]
    assert all(lo <= d <= hi for d in draws)
    assert len(set(draws)) > 1, "jitter must actually vary, not return the interval"


def test_same_settings_different_producers_draw_different_waits() -> None:
    """Two producers with identical settings draw different wait
    sequences: each carries its own RNG (seeded from the OS entropy pool
    in production), so an idle fleet dephases instead of ticking in
    unison. Seeded RNGs make the pin deterministic — fixed seeds, fixed
    (distinct) sequences."""
    interval = 5.0
    lo = interval * (1.0 - _POLL_JITTER_FRACTION) - _EPS
    hi = interval * (1.0 + _POLL_JITTER_FRACTION) + _EPS

    waits_a = [_jittered_poll_interval(interval, random.Random(1)) for _ in range(20)]
    waits_b = [_jittered_poll_interval(interval, random.Random(2)) for _ in range(20)]

    assert waits_a != waits_b, (
        "two producers with the same settings produced identical wait "
        "sequences — the fleet polls in lockstep and any transient event "
        "re-synchronizes it into periodic DB load spikes"
    )
    assert all(lo <= w <= hi for w in waits_a + waits_b)


async def test_producer_loop_poll_waits_are_jittered_not_fixed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop itself sleeps the jittered interval, not the configured
    constant: every empty-dispatch wait lands somewhere inside the band,
    and no two consecutive waits are the same fixed value."""
    poll_interval = 0.05
    backend = _RecordingBackend()
    deps = _producer_deps(poll_interval=poll_interval, maxsize=4)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()

    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def _recording_sleep(delay: float, result: object = None) -> object:
        # Signature mirrors asyncio.sleep(delay, result) — the
        # signature-drift guard fails narrower doubles. Records, then
        # yields once without sleeping the delay so the producer keeps
        # cycling and this test's await still returns.
        sleeps.append(delay)
        await real_sleep(0)
        if len(sleeps) >= 6:
            stop_event.set()
        return result

    monkeypatch.setattr(run_mod.asyncio, "sleep", _recording_sleep)

    await producer_loop(
        deps,  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in, as above.
        local_queue,
        shutdown_event,
        stop_event,
        backend=cast(Backend, backend),
        worker_id=new_uuid(),
        rng=random.Random(42),
    )

    assert len(sleeps) >= 5, "the producer must have waited between empty polls"
    lo = poll_interval * (1.0 - _POLL_JITTER_FRACTION) - _EPS
    hi = poll_interval * (1.0 + _POLL_JITTER_FRACTION) + _EPS
    assert all(lo <= d <= hi for d in sleeps), (
        f"poll waits {sleeps} fall outside the ±{_POLL_JITTER_FRACTION:.0%} "
        f"band around {poll_interval}s"
    )
    assert len(set(sleeps)) > 1, (
        f"every poll wait was the same value {sleeps[0]} — the fallback poll "
        "lost its jitter and an idle fleet ticks in lockstep"
    )


# ── Pooler-remap dispatch errors: the pooled gate at the dispatch round ──


async def _run_producer_over_raising_backend(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pooled: bool,
) -> list[Any]:
    """Drive producer_loop until a raising backend has failed 3 rounds.

    Returns every captured log record. The real poll sleep is replaced by
    a recorder that raises the stop flag once the round count is met, so
    the test terminates deterministically without timing sleeps.
    """
    backend = _RaisingBackend(
        asyncpg.InvalidSQLStatementNameError("unnamed prepared statement does not exist")
    )
    deps = _producer_deps(poll_interval=0.05, maxsize=1, pooled=pooled)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()

    real_sleep = asyncio.sleep

    async def _round_counting_sleep(delay: float, result: object = None) -> object:
        if backend.rounds >= 3:
            stop_event.set()
        await real_sleep(0)
        return result

    monkeypatch.setattr(run_mod.asyncio, "sleep", _round_counting_sleep)

    with structlog.testing.capture_logs() as captured:
        await producer_loop(
            deps,  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in for WorkerDeps, the established producer-loop unit pattern.
            local_queue,
            shutdown_event,
            stop_event,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
        )
    return captured


async def test_pooler_remap_error_degrades_quietly_under_pooled_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQLSTATE 26000 from a transaction-mode pooler is a warning plus a
    retry next tick, not a loud dispatch-batch-error per round.

    With the pooled topology declared (TASKQ_PG_IS_POOLED), a pooler remap
    that split a prepared statement's Parse from its Bind degrades the
    round quietly: every round stays on the record as
    dispatch-batch-transient and the loud exception-level record never
    fires.
    """
    captured = await _run_producer_over_raising_backend(monkeypatch, pooled=True)

    transient = [e for e in captured if e.get("event") == "dispatch-batch-transient"]
    assert len(transient) >= 3, f"every failed round is on the record: {captured}"
    assert all(e["log_level"] == "warning" for e in transient), (
        f"the degradation must be quiet, not exception-level: {transient}"
    )
    assert all(e.get("error_class") == "InvalidSQLStatementNameError" for e in transient), (
        f"the record names the shape that failed: {transient}"
    )
    assert not [e for e in captured if e.get("event") == "dispatch-batch-error"], (
        f"no loud record under the pooled gate: {captured}"
    )


async def test_remap_error_stays_loud_when_the_pooled_gate_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the pooled declaration the same error keeps the loud record.

    On a direct connection 26000 cannot be a pooling artifact: only an
    asyncpg bug produces it, and a bug must stay visible, so the default
    (pooled=False) must never launder the round into a quiet retry.
    """
    captured = await _run_producer_over_raising_backend(monkeypatch, pooled=False)

    loud = [e for e in captured if e.get("event") == "dispatch-batch-error"]
    assert len(loud) >= 3, f"every failed round is loud on a direct DSN: {captured}"
    assert all(e["log_level"] == "error" for e in loud), f"loud means exception-level: {loud}"
    assert not [e for e in captured if e.get("event") == "dispatch-batch-transient"], (
        f"no quiet degradation with the gate closed: {captured}"
    )
