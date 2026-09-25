"""The burst claim-stall pins: a denied admission must not park a claimed
job for a lease, and the producer must not sleep through frees.

Three mechanisms these pins hold, each measured end to end against real
Postgres before the fix (the 500-job burst repro: 500 root jobs on a
queue capped at 12 by a fleet-wide ``ConcurrencyReservation``, chained
two deep, two real worker subprocesses at ``TASKQ_MAX_CONCURRENCY``
24/8, the reported topology):

* A denied admission used to snooze at the acquire's lease-expiry hint
  (``lock_lease``, 60s shipped): the hint prices the CRASH bound, never
  a live holder's completion, so every over-admitted claim parked in
  ``scheduled`` for a full lease while the slots it lost to freed within
  one body length. The consumer now re-probes the acquisition locally
  (bounded by its own consumer count, woken by local releases) before
  accepting the snooze.
* The producer's empty-round wait did not listen for the consumers'
  slot-release event: a round the cap damper closed (headroom 0 while a
  cohort holds the cap) self-corrects the moment that cohort completes,
  but the producer slept the whole fallback poll (5s shipped) with
  pending work, free slots, and an idle DB - the reported ~0.6 jobs/s
  per-worker ceiling regardless of backlog.
* The producer's availability arithmetic did not count claim intents,
  so the get()-to-register window (DI resolution and the admission
  acquire run before ``register``) was a blind spot a burst fills with
  double claims - the reported "claimed-but-stalled exceeds the queue's
  cap" (io 17 > 12) at twice the worker's ``max_concurrency``.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import timedelta
from types import SimpleNamespace
from typing import cast

import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import Backend, JobRow
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.run import producer_loop
from tests._fleet import fleet_actor_config, open_fleet
from tests.conftest import interpreter_is_traced

pytestmark = pytest.mark.integration

_QUEUE = "claim_stall_burst_q"


# ── Unit: the producer's empty round wakes on a slot release ┹───────────


class _ScriptedBackend:
    """dispatch_batch answering from a script of per-round lists.

    An empty script entry is the cap damper's closed gate: the round
    claims nothing while pending work exists. Later entries (appended by
    the test) are the rounds after the cohort's completion re-opened
    admission.
    """

    def __init__(self, rounds: list[list[JobRow]]) -> None:
        self.rounds = rounds
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
        if self.rounds and limit > 0:
            return self.rounds.pop(0)
        return []


class _NoopPool:
    class _Conn:
        async def __aenter__(self) -> _NoopPool._Conn:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def execute(self, *_args: object) -> str:
            return "UPDATE 0"

    def acquire(self, *, timeout: float | None = None) -> _NoopPool._Conn:
        return self._Conn()


def _producer_deps(poll_interval: float) -> SimpleNamespace:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
            "TASKQ_QUEUES": _QUEUE,
            "TASKQ_POLL_INTERVAL": str(poll_interval),
            "TASKQ_NOTIFY_ENABLED": "false",
            "TASKQ_MAX_CONCURRENCY": "2",
        }
    )
    registry = ActiveJobRegistry()
    return SimpleNamespace(
        settings=settings,
        liveness=SimpleNamespace(tick=lambda *a, **k: None, forget=lambda *a, **k: None),
        active_jobs=SimpleNamespace(
            all=list,
            count=lambda: 0,
            intent_count=lambda: 0,
            mark_enqueued=registry.mark_enqueued,
            queued_ids=registry.queued_ids,
            mark_claimed=registry.mark_claimed,
            held_ids=registry.held_ids,
        ),
        disowned_jobs=set(),
        dispatcher_pool=_NoopPool(),
    )


async def test_empty_round_wakes_on_slot_release_before_the_poll_interval() -> None:
    """An empty round (the cap damper closed while a cohort holds the cap)
    must not park the producer for a poll interval: the cohort's
    completion sets the slot-release event, and that event is in the
    empty-round wait set, so the next claim starts the moment capacity
    frees. With a 30s poll, a producer that sleeps the poll cannot pass
    the 2s bound; under the pre-fix wait set this pin is red.
    """
    from taskq.testing.assertions import wait_for_condition
    from taskq.testing.jobs import make_job_row

    backend = _ScriptedBackend(rounds=[[make_job_row(status="pending")], []])
    deps = _producer_deps(poll_interval=30.0)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
    slot_freed = asyncio.Event()
    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()

    task = asyncio.create_task(
        producer_loop(
            deps,  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in for WorkerDeps, the established producer-loop unit pattern.
            local_queue,
            shutdown_event,
            stop_event,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
            slot_freed_event=slot_freed,
        )
    )
    try:
        # Round 1 claims (the damper's snapshot said free), round 2 is the
        # damper-closed empty round: pending work exists (the script has
        # none left to give, the shape is "admission closed, not drained").
        await wait_for_condition(
            lambda: len(backend.dispatch_calls) >= 2,
            description="the damper-closed empty round",
            timeout=2.0,
        )
        released_at = time.monotonic()
        # The cohort's completion: a holder released its slot. The producer
        # has no wake but this event and the 30s poll.
        slot_freed.set()
        backend.rounds.append([make_job_row(status="pending")])

        await wait_for_condition(
            lambda: len(backend.dispatch_calls) >= 3,
            description="claim after slot release",
            timeout=2.0,
        )
        assert backend.dispatch_calls[2] - released_at < 2.0, (
            "the producer slept through the slot release after an empty round: "
            f"the next claim landed {backend.dispatch_calls[2] - released_at:.2f}s "
            "after the release, the fallback-poll park this pin exists to hold"
        )
    finally:
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_availability_counts_claim_intents() -> None:
    """count() misses the take-to-register window; intent_count() is the
    third term of the producer's slot arithmetic, disjoint from both the
    queued rows and the registered entries, and register absorbs it."""
    registry = ActiveJobRegistry()
    job_id = new_uuid()
    registry.mark_enqueued(job_id)
    assert registry.intent_count() == 0, "a queued row is not yet a claim intent"

    token = registry.mark_claimed(job_id)
    assert registry.count() == 0
    assert registry.intent_count() == 1, (
        "a taken-but-unregistered job must be counted: the producer's "
        "availability subtracts it, or a burst double-claims every slot "
        "whose consumer is mid DI-resolution or admission retry"
    )

    task = asyncio.current_task()
    assert task is not None
    from taskq.context import JobContext

    ctx = JobContext(
        job_id=job_id,
        actor="a",
        queue=_QUEUE,
        attempt=1,
        claim_epoch=0,
        snooze_count=0,
        worker_id=new_uuid(),
        payload=None,
        jobs=None,  # type: ignore[arg-type]
        log=None,  # type: ignore[arg-type]
    )
    entry = await registry.register(job_id, task, ctx)
    assert registry.intent_count() == 0, "register absorbs the intent"
    assert registry.count() == 1

    await registry.deregister(job_id, entry)
    registry.resolve_claim(job_id, token)
    assert registry.count() == 0 and registry.intent_count() == 0


# ── Unit: the denial retry's budget and routing ─────────────────────────


class _FlakyRegistry:
    """acquire_for_actor that denies N times (recording calls) then wins."""

    def __init__(self, denials: int, *, source: str = "reservation") -> None:
        self.denials = denials
        self.source = source
        self.calls = 0
        self.release_event = asyncio.Event()

    def bucket_release_event(self, _name: str) -> asyncio.Event:
        return self.release_event

    async def acquire_for_actor(self, **_kwargs: object) -> list[object]:
        self.calls += 1
        if self.calls <= self.denials:
            raise ReservationUnavailable(
                "bucket",
                timedelta(seconds=60),
                source=self.source,  # type: ignore[arg-type]
            )
        return []


async def test_denial_retry_wins_when_a_local_release_lands() -> None:
    """A denied acquire re-probes on the bucket's release event and wins
    the freed slot without a snooze: the retry is the mechanism that
    keeps a burst's over-admitted claims from parking at the lease hint."""
    from taskq.worker._consumer import _acquire_for_actor_with_denial_retry

    registry = _FlakyRegistry(denials=2)
    # The holders release after ~150ms: the first wake wins the retry.
    loop = asyncio.get_running_loop()

    def _release_soon() -> None:
        loop.call_later(0.15, registry.release_event.set)

    loop.call_soon(_release_soon)

    t0 = time.monotonic()
    acquired = await _acquire_for_actor_with_denial_retry(
        registry,  # type: ignore[arg-type]
        rate_limits=(),
        reservations=(),
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=None,
        redis_client=None,
        pg_pool=None,
        clock=None,  # type: ignore[arg-type]
        settings=None,
        job_log=_quiet_log(),
    )
    elapsed = time.monotonic() - t0
    assert acquired == []
    assert registry.calls == 3, "two denials then the win"
    assert elapsed < 1.0, (
        f"the retry took {elapsed:.2f}s despite the release wake landing at "
        "150ms: the retry must ride the bucket's release event, not a fixed "
        "sleep grid"
    )


async def test_denial_retry_exhausts_to_the_last_hint() -> None:
    """A bucket that never frees exhausts the budget and re-raises the
    LAST denial: the caller's snooze path proceeds with the lease hint
    exactly as before the retry existed (the churn doctrine's regime)."""
    from taskq.worker._consumer import _acquire_for_actor_with_denial_retry

    registry = _FlakyRegistry(denials=10**9)
    with pytest.raises(ReservationUnavailable) as exc_info:
        await _acquire_for_actor_with_denial_retry(
            registry,  # type: ignore[arg-type]
            rate_limits=(),
            reservations=(),
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=None,
            redis_client=None,
            pg_pool=None,
            clock=None,  # type: ignore[arg-type]
            settings=SimpleNamespace(max_concurrency=2),
            job_log=_quiet_log(),
        )
    assert exc_info.value.retry_after == timedelta(seconds=60)
    assert registry.calls == 2 * 2 + 1, (
        "the budget is twice the worker's consumer count (the concurrent-"
        "denier bound) plus the initial attempt"
    )


async def test_rate_limit_denial_is_never_retried() -> None:
    """A rate limit's own denial (source='rate_limit') keeps its routing:
    no local retry, the first denial propagates."""
    from taskq.worker._consumer import _acquire_for_actor_with_denial_retry

    registry = _FlakyRegistry(denials=5, source="rate_limit")
    with pytest.raises(ReservationUnavailable):
        await _acquire_for_actor_with_denial_retry(
            registry,  # type: ignore[arg-type]
            rate_limits=(),
            reservations=(),
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=None,
            redis_client=None,
            pg_pool=None,
            clock=None,  # type: ignore[arg-type]
            settings=SimpleNamespace(max_concurrency=8),
            job_log=_quiet_log(),
        )
    assert registry.calls == 1, "a rate-limit denial routes, never retries"


def _quiet_log() -> object:
    import structlog

    return structlog.get_logger("test_load_claim_stall")


# ── Integration: the burst shape end to end ─────────────────────────────


_BURST_CAP_SLOTS = 2
_BURST_N_JOBS = 30
_BURST_BODY_S = 0.15
_BURST_MAX_CONCURRENCY = 6
# Healthy arithmetic: 30 bodies x 0.15s through a 2-slot cap is ~2.3s of
# work. The pre-fix mechanism parks the over-admitted claims at the lease
# hint (minutes away here), which cannot pass even a heavily rounded
# ceiling.
_BURST_DRAIN_CEILING_S = 20.0
_BURST_RUN_CEILING_S = 60.0


@pytest.mark.load_sensitive
@pytest.mark.slow
async def test_burst_fan_out_does_not_stall_on_a_queue_cap(pg_dsn: str) -> None:
    if interpreter_is_traced():
        pytest.skip("the drain ceiling measures the scheduler, not the code")
    """A burst deeper than a queue's cap drains at execution pace: no job
    parks in ``scheduled`` at the lease hint, and no outcome snoozes.

    Under the pre-fix mechanism this shape stalls for a full lease per
    over-admitted claim (measured: 145s where ~13s is healthy), the
    reported 30-60x drain collapse.
    """
    schema = f"claim_stall_burst_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_QUEUE, _QUEUE),),
        settings_overrides={"max_concurrency": str(_BURST_MAX_CONCURRENCY)},
    ) as fleet:
        pod = fleet.pod("pod-1")
        registry = RateLimitRegistry()
        reservation = ConcurrencyReservation(
            name="claim_stall_burst_cap",
            slots=_BURST_CAP_SLOTS,
            lease=timedelta(minutes=10),
            schema=fleet.schema,
        )
        await reservation.ensure_slots(pod.deps.worker_pool)
        registry.register(reservation)

        await fleet.enqueue(_BURST_N_JOBS, actor=_QUEUE, queue=_QUEUE)

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=_BURST_MAX_CONCURRENCY)
        stop = asyncio.Event()
        outcomes: list[object] = []
        slot_freed = asyncio.Event()

        async def producer() -> None:
            while not stop.is_set():
                available = (
                    local_queue.maxsize
                    - local_queue.qsize()
                    - pod.deps.active_jobs.count()
                    - pod.deps.active_jobs.intent_count()
                )
                if available <= 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(slot_freed.wait(), timeout=0.1)
                    slot_freed.clear()
                    continue
                claimed = await pod.claim([_QUEUE], available)
                for job in claimed:
                    pod.deps.active_jobs.mark_enqueued(job.id)
                    await local_queue.put(job)
                if not claimed:
                    await asyncio.sleep(0.02)

        async def consumer() -> None:
            while not stop.is_set():
                try:
                    job = await asyncio.wait_for(local_queue.get(), timeout=0.05)
                except TimeoutError:
                    continue
                claim = pod.deps.active_jobs.mark_claimed(job.id)
                slot_freed.set()

                async def _run_actor(_row: JobRow, ctx: object) -> None:
                    await asyncio.sleep(_BURST_BODY_S)

                outcome = await pod.run(job, _run_actor, actor_config=fleet_actor_config())
                pod.deps.active_jobs.resolve_claim(job.id, claim)
                outcomes.append(outcome)
                if len(outcomes) >= _BURST_N_JOBS:
                    stop.set()

        t0 = time.monotonic()
        prod_task = asyncio.create_task(producer())
        cons_tasks = [asyncio.create_task(consumer()) for _ in range(_BURST_MAX_CONCURRENCY)]
        deadline = t0 + _BURST_RUN_CEILING_S
        while not stop.is_set() and time.monotonic() < deadline:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=0.05)
        stop.set()
        prod_task.cancel()
        for t in cons_tasks:
            t.cancel()
        await asyncio.gather(prod_task, *cons_tasks, return_exceptions=True)
        drain_s = time.monotonic() - t0

        assert len(outcomes) == _BURST_N_JOBS, (
            f"only {len(outcomes)}/{_BURST_N_JOBS} burst jobs completed within the "
            f"{_BURST_RUN_CEILING_S:.0f}s ceiling (drain={drain_s:.1f}s): the burst "
            "stalled on the queue cap"
        )
        snoozed = [o for o in outcomes if o == "scheduled"]
        assert not snoozed, (
            f"{len(snoozed)} burst jobs snoozed instead of winning a freed slot: "
            "the denied claims parked at the lease hint rather than re-probing "
            "the bucket locally"
        )
        assert drain_s < _BURST_DRAIN_CEILING_S, (
            f"the burst drained in {drain_s:.1f}s (ceiling {_BURST_DRAIN_CEILING_S}s, "
            f"healthy ~{_BURST_N_JOBS * _BURST_BODY_S / _BURST_CAP_SLOTS:.1f}s): "
            "the claim-stall pacing is back"
        )

        states = await fleet.job_states()
        assert all(s == "succeeded" for s in states.values()), (
            "every burst job must reach succeeded: a denied claim that "
            "exhausts its local retry must still recover, never strand"
        )
