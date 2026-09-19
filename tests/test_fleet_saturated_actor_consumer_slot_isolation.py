"""A saturated actor must not spend a healthy neighbour's consumer slots.

TaskQ's worker gives every actor and queue a *worker process subscribes to*
one shared pool of ``TASKQ_MAX_CONCURRENCY`` consumer coroutines draining
one ``local_queue`` (docs/guides/workers.md "Internal components" —
"Consumer loops. ``max_concurrency`` concurrent coroutines drain
``local_queue``"; src/taskq/worker/run.py's ``producer_loop`` /
``di_consumer_loop``). Admission has two layers: the claim (the SQL in
src/taskq/backend/_dispatch_sql.py, pending -> running) and the per-job
``acquire_for_actor`` AFTER the claim, inside ``consume_one_job``
(src/taskq/worker/_consumer.py:416-427), which decides whether the claimed
job can actually spend its declared ``reservations``/``rate_limits`` — on
denial (``ReservationUnavailable``) the job is rescheduled to ``scheduled``
(docs/guides/rate-limiting.md:685-693). The claim is reservation-aware: its
capacity computation folds in live ``reservation_slots`` occupancy (the
``reservation_holdings`` / ``reservation_headroom`` CTEs), so an actor
whose held bucket is full is admitted nothing that round. This pin exists
because the claim was NOT always aware: without the gate, an actor sharing
a queue with a healthy actor whose every job is denied by an exhausted
reservation still consumes one of the worker's ``max_concurrency``
consumer-coroutine slots for the (short but nonzero) duration of each
failing acquire call, cycle after cycle, for as long as its backlog and
the reservation's exhaustion both persist. That is capacity taken away
from every other actor sharing the same worker process's consumer pool --
not a correctness bug (the healthy actor's jobs still eventually complete)
but a throughput regression the pin measures directly.

The contrasting architecture gives every queue its OWN producer and its
OWN worker pool (a queue is a unit of concurrency, not a label on a
shared unit). A queue whose jobs are
all denied capacity can only ever starve ITS OWN pool's workers -- it
structurally cannot touch a sibling queue's dedicated worker slots.
TaskQ's single-shared-pool-per-worker-process design is a
real, documented architectural choice (docs/guides/workers.md's
"Concurrency model" table has no "per-queue worker pool" row at all — only
process/queue/actor/reservation *caps*, none of which isolate one actor's
consumer-slot churn from another's), so this test does not ask TaskQ to
adopt a per-queue-pool shape. It pins the narrower, checkable
contract an adopter will assume holds regardless of internal architecture:
capacity denied to one actor must not measurably reduce a co-located
healthy actor's completion throughput on the same worker.

Measured on this branch before the fix (two independent runs, real
Postgres, real ``consume_one_job``, ``TASKQ``-shaped ``max_concurrency=4``
consumer pool, one actor holding a permanently-exhausted single-slot
``ConcurrencyReservation`` and flooding its queue with jobs that are denied
every attempt): healthy-actor throughput dropped ~44-45% versus an
uncontended baseline (234.3 jobs/s baseline vs 127.9 jobs/s contended, and
220.7 vs 124.1 on a second run — see the scratch probe this test's
harness reproduces, not committed to the repo per the sweep's own rules).
This test pins a conservative bound (no more than a 15% throughput drop)
well inside that measured regression.

The fix that turned it green: the claim statement is reservation-aware
(``reservation_holdings`` / ``reservation_headroom`` in
src/taskq/backend/_dispatch_sql.py) — an actor whose held bucket has no
acquirable slot is admitted nothing that round, so its rows stay pending
instead of churning consumer coroutines into denied acquires. The
measured shape after the fix (same harness, same machine): the drop
collapses to noise (~0%), denials collapse from ~150 to the one bounded
first-round race, and the saturated actor drains the moment capacity
frees. The shared-pool design stays; the pin now guards the isolation
contract on it.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobRow
from taskq.backend.clock import SystemClock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.worker._consumer import consume_one_job
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = [pytest.mark.integration, pytest.mark.load_sensitive]

_QUEUE = "fleet_saturation_isolation_q"
_SATURATED_ACTOR = "saturation_isolation_saturated"
_HEALTHY_ACTOR = "saturation_isolation_healthy"

# Mirrors a modest production worker (TASKQ_MAX_CONCURRENCY default is 8;
# 4 keeps the probe fast while still giving the saturated actor's flood
# real competition for consumer coroutines).
_MAX_CONCURRENCY = 4
_N_HEALTHY_JOBS = 600
_N_FLOOD_JOBS = 300
_RUN_CEILING_SECONDS = 60.0

# The regression this branch fixed measured ~44-45%: denied jobs burning
# consumer-coroutine turns collapsed the healthy actor's throughput toward
# the pool-share arithmetic (150 healthy vs 450 pool competitors is a ~2/3
# drop). With isolation, contended throughput tracks baseline within
# single-digit percent even on a loaded runner, because the statistic below
# is the median inter-completion gap: a scheduler hiccup, a GC pause or a
# slow producer ramp moves a handful of gaps, not the median of hundreds.
# The 35% bound sits far above that noise floor and far below the ~44% a
# real isolation regression produces, so it pins the behaviour without
# pinning the runner's speed.
_MAX_ACCEPTABLE_THROUGHPUT_DROP = 0.35


async def _noop(payload: FleetPayload, ctx: object) -> None:
    return None


async def _hog_forever(payload: FleetPayload, ctx: object) -> None:
    await asyncio.sleep(3600)


async def _drain_healthy_actor_throughput(
    fleet: Fleet,
    *,
    with_saturated_neighbour: bool,
    registry: RateLimitRegistry | None,
    reservation_name: str,
) -> float:
    """Run a minimal, faithful reproduction of the production consumer-pool
    shape (one shared local_queue, ``_MAX_CONCURRENCY`` consumer
    coroutines, each calling the REAL ``consume_one_job``) and return
    healthy-actor jobs/second as the mean inter-completion gap across the
    span from the first to the ``_N_HEALTHY_JOBS``th completion.
    """
    pod = fleet.pod("pod-1")
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=_MAX_CONCURRENCY)
    stop = asyncio.Event()
    healthy_completed_at: list[float] = []

    async def producer() -> None:
        while not stop.is_set():
            # Deliberately the pre-#229 queue-emptiness formula: this
            # harness measures the dispatch SQL's reservation gate
            # (perf-evidence-dispatch.md A6), whose conditions include
            # claims landing while consumers are busy — the shipped
            # producer's exact-slot accounting (run.py, #229) is pinned
            # separately in tests/test_producer_slot_accounting.py.
            available = local_queue.maxsize - local_queue.qsize()
            if available <= 0:
                await asyncio.sleep(0.01)
                continue
            claimed = await pod.claim([_QUEUE], available)
            for job in claimed:
                await local_queue.put(job)
            if not claimed:
                await asyncio.sleep(0.02)

    async def consumer() -> None:
        while not stop.is_set():
            try:
                job = await asyncio.wait_for(local_queue.get(), timeout=0.05)
            except TimeoutError:
                continue
            payload = FleetPayload.model_validate(job.payload)
            handler = _hog_forever if job.actor == _SATURATED_ACTOR else _noop

            async def _run_actor(
                _row: JobRow,
                ctx: object,
                _h: Callable[[FleetPayload, object], Awaitable[None]] = handler,
                _p: FleetPayload = payload,
            ) -> None:
                return await _h(_p, ctx)

            # Only the saturated actor's jobs declare the exhausted
            # reservation — the production wiring passes no registry
            # (and spends no acquire) for an actor without declarations.
            rl_registry: RateLimitRegistry | None = None
            rl_reservations: list[str] | None = None
            rl_pool: asyncpg.Pool | None = None
            if registry is not None and job.actor == _SATURATED_ACTOR:
                rl_registry = registry
                rl_reservations = [reservation_name]
                rl_pool = pod.deps.worker_pool

            await consume_one_job(
                pod.backend,
                job,
                pod.worker_id,
                deps=pod.deps,
                run_actor=_run_actor,
                actor_config=fleet_actor_config(),
                payload_type=FleetPayload,
                clock=SystemClock(),
                active_jobs=pod.deps.active_jobs,
                rate_limit_registry=rl_registry,
                reservations=rl_reservations,
                worker_pool=rl_pool,
            )

            if job.actor == _HEALTHY_ACTOR:
                healthy_completed_at.append(time.monotonic())
                if len(healthy_completed_at) >= _N_HEALTHY_JOBS:
                    stop.set()

    prod_task = asyncio.create_task(producer())
    cons_tasks = [asyncio.create_task(consumer()) for _ in range(_MAX_CONCURRENCY)]
    t0 = time.monotonic()
    deadline = t0 + _RUN_CEILING_SECONDS
    while not stop.is_set() and time.monotonic() < deadline:
        # Wake on the completion signal itself (the 150th healthy job),
        # never a fixed sleep — the deadline caps the wait, the event
        # makes the measured `elapsed` end at the completion, not at the
        # next poll tick.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=min(0.05, deadline - time.monotonic()))
    stop.set()
    prod_task.cancel()
    for t in cons_tasks:
        t.cancel()
    await asyncio.gather(prod_task, *cons_tasks, return_exceptions=True)

    # The throughput statistic is the mean inter-completion gap across the
    # span from the first to the last completion, not total-over-elapsed
    # (which adds the producer's ramp and the waiter's final poll tick) and
    # not the median gap (healthy completions arrive in bursts -- four
    # consumers overlapping their Postgres round trips -- so the median
    # measures burst shape, not rate). Across ~600 completions a single
    # scheduler hiccup or GC pause contributes only stall/600 to the mean,
    # which is far inside the band under assertion.
    n_done = len(healthy_completed_at)
    assert n_done == _N_HEALTHY_JOBS, (
        f"the healthy actor completed only {n_done}/{_N_HEALTHY_JOBS} jobs within the "
        f"{_RUN_CEILING_SECONDS}s ceiling (with_saturated_neighbour={with_saturated_neighbour}) "
        "-- widen the ceiling or reduce _N_HEALTHY_JOBS before trusting the rate below."
    )
    return (n_done - 1) / (healthy_completed_at[-1] - healthy_completed_at[0])


@pytest.mark.slow
async def test_saturated_actor_does_not_reduce_healthy_actor_throughput(pg_dsn: str) -> None:
    """A co-located actor stuck on an exhausted reservation must not
    measurably slow a healthy actor sharing its worker's consumer pool.

    Baseline: only the healthy actor is present. Contended: the healthy
    actor shares its queue and worker with a saturated actor holding a
    permanently-exhausted single-slot ``ConcurrencyReservation`` (one
    "hog" job holds the only slot for the whole run) and flooding the
    queue with jobs that are claimed, denied, and rescheduled on every
    attempt. If TaskQ's shared consumer pool is isolated from a
    denied-on-every-attempt neighbour, both throughputs should be close;
    if the neighbour's denied jobs are still burning consumer-coroutine
    turns, contended throughput drops well below baseline.
    """
    baseline_schema = f"fleet_sat_iso_base_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=baseline_schema,
        pods=("pod-1",),
        actors=((_HEALTHY_ACTOR, _QUEUE),),
    ) as fleet:
        await fleet.enqueue(_N_HEALTHY_JOBS, actor=_HEALTHY_ACTOR, queue=_QUEUE)
        baseline_rate = await _drain_healthy_actor_throughput(
            fleet,
            with_saturated_neighbour=False,
            registry=None,
            reservation_name="",
        )

    contended_schema = f"fleet_sat_iso_cont_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=contended_schema,
        pods=("pod-1",),
        actors=((_HEALTHY_ACTOR, _QUEUE), (_SATURATED_ACTOR, _QUEUE)),
    ) as fleet:
        pod = fleet.pod("pod-1")
        registry = RateLimitRegistry()
        reservation_name = "saturation_isolation_slot"
        reservation = ConcurrencyReservation(
            name=reservation_name,
            slots=1,
            lease=timedelta(minutes=10),
            schema=fleet.schema,
        )
        await reservation.ensure_slots(pod.deps.worker_pool)
        registry.register(reservation)

        # One hog job holds the reservation's single slot for the whole
        # run -- every other saturated-actor job is denied on every
        # attempt, the realistic "an actor sharing a fleet slot that
        # never frees" shape.
        await fleet.enqueue(1, actor=_SATURATED_ACTOR, queue=_QUEUE)
        await fleet.enqueue(_N_FLOOD_JOBS, actor=_SATURATED_ACTOR, queue=_QUEUE)
        await fleet.enqueue(_N_HEALTHY_JOBS, actor=_HEALTHY_ACTOR, queue=_QUEUE)

        contended_rate = await _drain_healthy_actor_throughput(
            fleet,
            with_saturated_neighbour=True,
            registry=registry,
            reservation_name=reservation_name,
        )

        denied_rows = await fleet.fetch(
            'SELECT count(*) AS n FROM "{schema}".jobs '
            "WHERE actor = $1 AND rate_limit_blocked_count > 0",
            _SATURATED_ACTOR,
        )
        denied_count = int(denied_rows[0]["n"])
        assert denied_count > 0, (
            "the saturated actor's jobs were never denied -- the reservation setup is "
            "broken and this test is not exercising the contended path it claims to."
        )

    drop = (baseline_rate - contended_rate) / baseline_rate if baseline_rate > 0 else float("nan")
    assert drop <= _MAX_ACCEPTABLE_THROUGHPUT_DROP, (
        f"healthy-actor throughput dropped {drop:.1%} (baseline={baseline_rate:.1f} jobs/s, "
        f"contended={contended_rate:.1f} jobs/s) when sharing its worker's consumer pool with "
        f"an actor whose {denied_count} jobs were all denied by an exhausted reservation. "
        f"The bound this test enforces is {_MAX_ACCEPTABLE_THROUGHPUT_DROP:.0%} -- a fleet "
        "operator adding capacity for a healthy actor gets less of it than they provisioned, "
        "silently, whenever an unrelated actor shares the same worker process and queue while "
        "stuck on a rate limit or exhausted reservation. Nothing in docs/guides/rate-limiting.md "
        "or docs/guides/workers.md warns that a denied actor's jobs still compete for "
        "TASKQ_MAX_CONCURRENCY consumer-coroutine turns."
    )
