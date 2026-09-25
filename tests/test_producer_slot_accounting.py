"""The producer claims by genuinely free slots, not queue emptiness.

Before this change the producer sized every claim by
``local_queue.maxsize - local_queue.qsize()`` alone: a worker whose
consumers were all busy and whose queue had drained looked FULLY free,
so it locked up to ``max_concurrency`` ADDITIONAL rows - up to 2x the
slot count held running at once (double reclaim exposure on a crash,
pending work locked behind long jobs while peer workers idled, gauges
overstating occupancy by up to 2x). The claim now subtracts the
active-jobs count, and the consumers wake the producer at the
DEREGISTER that actually frees the slot - so a finished job's
replacement claim does not wait for the fallback poll.

Pins in this file (pure unit, no PG):

* a fully-busy worker claims nothing, and its claim fires the moment
  accounting settles - without arming the claim cooldown (the cooldown
  only follows a ROUND; a skipped round arms nothing);
* the claim size tracks the active count exactly;
* the stub consumer's completion path wakes the producer AFTER the
  deregister that frees the slot (the second slot-release point);
* a saturated producer drains promptly as slots free one at a time.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from types import SimpleNamespace
from typing import Any, cast

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobRow
from taskq.testing.assertions import wait_for_condition
from taskq.testing.jobs import make_job_row
from taskq.worker.run import (
    _CLAIM_COOLDOWN_SECONDS,
    _POLL_JITTER_FRACTION,
    consumer_loop_stub,
    producer_loop,
)


class _Active:
    """Mutable active-jobs stand-in: ``count()``/``intent_count()`` read
    the live numbers. ``n`` is the registered count, ``intents`` the
    takes not yet absorbed by a registration."""

    def __init__(self, n: int = 0, *, intents: int = 0) -> None:
        self.n = n
        self.intents = intents

    def count(self) -> int:
        return self.n

    def intent_count(self) -> int:
        return self.intents

    def all(self) -> list[object]:
        return []

    def held_ids(self) -> list[object]:
        return []

    def queued_ids(self) -> list[object]:
        return []

    def mark_enqueued(self, job_id: object) -> None:
        pass

    def mark_claimed(self, job_id: object) -> None:
        return None

    def resolve_claim(self, job_id: object, token: object = None) -> None:
        return None


class _RecordingBackend:
    """dispatch_batch that records (monotonic time, limit) per round."""

    def __init__(self, jobs: int = 0) -> None:
        self.rounds: list[tuple[float, int]] = []
        self._jobs = [make_job_row() for _ in range(jobs)]

    async def dispatch_batch(
        self,
        *,
        worker_id: object,
        queues: object,
        limit: int,
        lock_lease: object,
    ) -> list[JobRow]:
        self.rounds.append((time.monotonic(), limit))
        if self._jobs and limit > 0:
            return [self._jobs.pop(0)]
        return []


def _deps(active: _Active, *, maxsize: int, poll_interval: float = 5.0) -> SimpleNamespace:
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=poll_interval,
        notify_poll_interval=poll_interval,
        max_concurrency=maxsize,
        schema_name="taskq",
    )
    return SimpleNamespace(
        settings=settings,
        liveness=SimpleNamespace(tick=lambda *a, **k: None, forget=lambda *a, **k: None),
        active_jobs=active,
        disowned_jobs=set(),
        dispatcher_pool=SimpleNamespace(),
    )


async def _run_producer(
    deps: SimpleNamespace,
    backend: _RecordingBackend,
    local_queue: asyncio.Queue[JobRow],
    slot_freed: asyncio.Event,
) -> asyncio.Task[None]:
    return asyncio.create_task(
        producer_loop(
            cast(WorkerDepsLike, deps),
            local_queue,
            asyncio.Event(),
            asyncio.Event(),
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
            slot_freed_event=slot_freed,
        )
    )


#: The producer_loop signature's deps parameter is WorkerDeps; the
#: namespace fakes in this file carry every field the loop reads.
WorkerDepsLike = Any


async def test_a_fully_busy_worker_claims_nothing_and_never_arms_the_cooldown() -> None:
    """Every consumer slot busy (active = max_concurrency), queue empty:
    the producer must run NO claim round at all - the pre-fix producer
    saw ``maxsize - qsize`` free slots here and locked a full extra
    batch. No round also means no short-round cooldown is armed, so the
    claim that follows a freed slot is immediate, not floored."""
    active = _Active(4)
    backend = _RecordingBackend(jobs=8)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
    slot_freed = asyncio.Event()
    task = await _run_producer(_deps(active, maxsize=4), backend, local_queue, slot_freed)
    try:
        # Long enough for several fallback-poll cycles: a saturated
        # producer re-checks on the 0.1s cadence and must still claim
        # nothing while every slot is busy.
        await asyncio.sleep(0.35)
        assert backend.rounds == [], (
            f"a fully-busy worker ran {len(backend.rounds)} claim rounds "
            f"(limits {[limit for _, limit in backend.rounds]}) - the "
            "availability accounting must subtract active jobs"
        )
        # The slot frees: accounting settles and the consumer-side wake
        # fires. The claim that follows must NOT pay the short-round
        # cooldown floor (no round ever ran to arm it): it lands within
        # a scheduler step, far inside the cooldown's jittered minimum.
        active.n = 0
        slot_freed.set()
        woke_at = time.monotonic()
        await wait_for_condition(
            lambda: len(backend.rounds) >= 1,
            description="claim after the slot freed",
            timeout=2.0,
        )
        claimed_at, limit = backend.rounds[0]
        cooldown_floor = _CLAIM_COOLDOWN_SECONDS * (1.0 - _POLL_JITTER_FRACTION)
        assert limit == 4
        assert claimed_at - woke_at < cooldown_floor, (
            f"the post-saturation claim landed {claimed_at - woke_at:.3f}s after "
            "the slot freed - a cooldown floor was applied to a claim that "
            "followed NO round (only short rounds arm the cooldown)"
        )
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_claim_size_tracks_the_active_count() -> None:
    """With k of maxsize slots busy and the queue empty, the round asks
    for exactly maxsize - k rows - never the full slot count again."""
    for busy in (0, 1, 3):
        active = _Active(busy)
        backend = _RecordingBackend(jobs=8)
        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
        slot_freed = asyncio.Event()
        task = await _run_producer(
            _deps(active, maxsize=4, poll_interval=0.05), backend, local_queue, slot_freed
        )
        try:
            await wait_for_condition(
                lambda backend=backend: len(backend.rounds) >= 1,
                description=f"claim round with {busy} active",
                timeout=2.0,
            )
            assert backend.rounds[0][1] == 4 - busy, (
                f"with {busy} of 4 slots busy the producer asked for "
                f"{backend.rounds[0][1]} rows - the claim must size by the "
                "genuinely free slots (maxsize - qsize - active)"
            )
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


class _StubTerminalBackend:
    """The terminal writes consumer_loop_stub issues."""

    def __init__(self) -> None:
        self.succeeded: list[object] = []

    async def mark_succeeded(
        self,
        job_id: object,
        worker_id: object,
        result: object = None,
        **kwargs: object,
    ) -> bool:
        self.succeeded.append(job_id)
        return True

    async def mark_cancelled(self, job_id: object, worker_id: object, **kwargs: object) -> bool:
        return True


class _RegistrySpy:
    """ActiveJobRegistry stand-in that records registrations and reads."""

    def __init__(self) -> None:
        self.registered: list[object] = []
        self.deregistered: list[object] = []

    async def register(self, job_id: object, task: object, ctx: object) -> None:
        self.registered.append(job_id)
        return None

    async def deregister(self, job_id: object, entry: object) -> None:
        self.deregistered.append(job_id)

    def count(self) -> int:
        return len(self.registered) - len(self.deregistered)

    def all(self) -> list[object]:
        return []

    def held_ids(self) -> list[object]:
        return []

    def mark_claimed(self, job_id: object) -> None:
        self.registered.append(("intent", job_id))
        return None

    def resolve_claim(self, job_id: object, token: object = None) -> None:
        return None


async def test_stub_consumer_wakes_the_producer_after_the_deregister() -> None:
    """The completion-side slot release: the stub consumer sets the
    shared slot-freed event AFTER its deregister (the point the active
    count actually drops), so the producer the event wakes reads settled
    accounting - the claim is not delayed to the fallback poll and not
    answered with a stale count."""
    job = make_job_row()
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    await local_queue.put(job)
    shutdown = asyncio.Event()
    slot_freed = asyncio.Event()
    registry = _RegistrySpy()
    backend = _StubTerminalBackend()
    deps = SimpleNamespace(
        active_jobs=registry,
        producer_stop_event=asyncio.Event(),
        disowned_jobs=set(),
    )

    woke_counts: list[int] = []
    events: list[str] = []
    # Observe the wake order: capture the active count at the moment the
    # event transitions to set, and interleave it with the registry's
    # own transitions so the ORDER is the assertion medium.
    original_set = slot_freed.set

    def _counting_set() -> None:
        woke_counts.append(registry.count())
        events.append("wake")
        original_set()

    slot_freed.set = _counting_set  # type: ignore[method-assign]
    original_register = registry.register

    async def _recording_register(job_id: object, task: object, ctx: object) -> None:
        events.append("register")
        await original_register(job_id, task, ctx)

    registry.register = _recording_register  # type: ignore[method-assign]
    original_deregister = registry.deregister

    async def _recording_deregister(job_id: object, entry: object) -> None:
        await original_deregister(job_id, entry)
        events.append("deregister")

    registry.deregister = _recording_deregister  # type: ignore[method-assign]

    task = asyncio.create_task(
        consumer_loop_stub(
            cast(Any, deps),
            local_queue,
            shutdown,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
            stub_work_timeout=0.01,
            slot_freed_event=slot_freed,
        )
    )
    try:
        # The stub is a loop: wait for the observable (terminal write +
        # deregister + both wakes), not for the task to return.
        await wait_for_condition(
            lambda: (
                backend.succeeded == [job.id]
                and registry.deregistered == [job.id]
                and len(woke_counts) >= 2
            ),
            description="stub consumer completed the job and woke twice",
            timeout=5.0,
        )
    finally:
        shutdown.set()
        deps.producer_stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert backend.succeeded == [job.id]
    # The wake ORDER is the pin: the get()-point wake fires before the
    # job registers (the queue slot freed, the active slot not yet
    # taken - both counts read 0, which is why the order rather than
    # the count is the assertion), and the completion-side wake fires
    # AFTER the deregister that frees the active slot - never while the
    # finished job is still counted.
    assert events == ["wake", "register", "deregister", "wake"], (
        f"consumer event order was {events} (active counts at wakes "
        f"{woke_counts}) - the completion-side wake must follow the "
        "deregister, so the producer it wakes reads settled accounting"
    )
    assert registry.deregistered == [job.id]


async def test_a_saturated_producer_drains_promptly_as_slots_free() -> None:
    """No stuck local_queue: a producer that spent a stretch fully
    saturated claims promptly for each slot that frees - the
    deregister-side wake re-arms it without a poll wait, and every round
    sizes by the slots genuinely free (the queue fills with the claims,
    so the arithmetic must keep subtracting both terms)."""
    maxsize = 4
    active = _Active(maxsize)
    backend = _RecordingBackend(jobs=4 * maxsize)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=maxsize)
    slot_freed = asyncio.Event()
    task = await _run_producer(
        _deps(active, maxsize=maxsize, poll_interval=5.0), backend, local_queue, slot_freed
    )
    try:
        await asyncio.sleep(0.2)
        assert backend.rounds == []
        # Slots free one at a time (jobs completing), each with its
        # deregister-side wake. After the k-th slot frees the worker
        # holds k-1 claimed-but-unconsumed rows (this test runs no
        # consumers), so the genuinely free count is exactly one per
        # freed slot - the promptness is the pin, not the size.
        for freed in range(1, maxsize + 1):
            active.n = maxsize - freed
            slot_freed.set()
            await wait_for_condition(
                lambda freed=freed: len(backend.rounds) >= freed,
                description=f"round after {freed} slot(s) freed",
                timeout=2.0,
            )
        assert [limit for _, limit in backend.rounds] == [1] * maxsize, (
            f"round limits were {[limit for _, limit in backend.rounds]} - "
            "each freed slot must yield one prompt, correctly-sized round"
        )
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ── The take-to-register window: counted, never blind ───────────────────


async def test_a_consumer_in_the_take_register_window_over_admits_nothing() -> None:
    """The window between a consumer's get() and the job's active_jobs
    register is COUNTED: the take installs a claim intent the producer's
    sizing arithmetic subtracts (count + intent_count), so a consumer
    paused in DI resolution or a denied-admission retry is still a held
    slot. With three slots genuinely busy and the fourth consumer sitting
    in the window, the claim is exactly 0 rows - the blind window that
    read one slot too many (the burst's double-claim amplifier) is gone.
    Closing the window changes nothing: all four slots were held all
    along."""
    active = _Active(3, intents=1)  # three registered busy, one take in the window
    backend = _RecordingBackend(jobs=4)
    # The queue is empty because the fourth slot's row was TAKEN: the
    # window is open, its consumer has not registered the row yet.
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
    slot_freed = asyncio.Event()
    task = await _run_producer(
        _deps(active, maxsize=4, poll_interval=0.05), backend, local_queue, slot_freed
    )
    try:
        await asyncio.sleep(0.35)
        assert backend.rounds == [], (
            f"the producer claimed {[limit for _, limit in backend.rounds]} with three "
            "slots busy and one row taken but unregistered - the claim intents "
            "are the third term of the slot arithmetic: the take-register "
            "window is a held slot, never a free one, and the burst shape "
            "(DI + admission-retry pauses in that window) must not "
            "double-claim it"
        )
        # The window closes: the taken row registers, all four slots are
        # genuinely occupied, nothing more to claim.
        active.n = 4
        active.intents = 0
        await asyncio.sleep(0.2)
        assert backend.rounds == []
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_over_admission_scales_with_the_window_never_with_the_slots() -> None:
    """Two consumers caught between get() and register hold two slots the
    arithmetic can see: the claim is 0, never the full slot count. The
    window is bounded by the consumer count and counted row for row."""
    active = _Active(2, intents=2)  # two consumers registered, two rows taken unregistered
    backend = _RecordingBackend(jobs=0)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
    slot_freed = asyncio.Event()
    task = await _run_producer(
        _deps(active, maxsize=4, poll_interval=0.05), backend, local_queue, slot_freed
    )
    try:
        await asyncio.sleep(0.35)
        assert backend.rounds == [], (
            f"first round asked for {[limit for _, limit in backend.rounds]} with two "
            "slots busy and two rows taken but unregistered - the take-register "
            "window is counted (intents), so the claim is 0, never 4 and never "
            "the window's old two-row over-admission"
        )
        # Settle the window: both taken rows register, the worker is
        # genuinely full, the claims stop.
        active.n = 4
        active.intents = 0
        await asyncio.sleep(0.2)
        assert backend.rounds == [], (
            f"round limits {[limit for _, limit in backend.rounds]} on a "
            "genuinely full worker"
        )
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
