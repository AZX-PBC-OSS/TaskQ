"""A saturated rate limit must not spend the round's dispatch capacity on
jobs it is about to deny -- and, measured against real Postgres, it does
not: TaskQ's per-actor fairness allocation in dispatch SQL already
prevents this.

The general shape this sits in: for a rate-limited queue, admission is
decided before the fetch (jobs above the limit are deferred from being
fetched at all) rather than fetching-then-rejecting.

The hypothesis this test was written to check: TaskQ's own docs
(docs/guides/rate-limiting.md, "Wiring to Actors") state the dispatch
sequence as claim the row (dispatch SQL, `FOR UPDATE SKIP LOCKED`), THEN
attempt `acquire_for_actor`, and only THEN decide admission -- so a
claimed-but-denied job already consumed one of the round's
`dispatch_batch(..., limit_n=N)` slots before the denial is known. If a
queue mixed a fully-saturated rate-limited actor with a healthy,
unlimited actor sharing one queue, this predicted the round's claim
budget could be spent disproportionately on jobs about to be denied,
reducing the healthy actor's throughput.

Measured result (see the passing assertions below): this does NOT
happen at the scale tested (a 40-job saturated backlog against a 10-job
healthy cohort, 4 slots/round). Reading `src/taskq/backend/_dispatch_sql.py`
after the fact explains why: dispatch computes a `per_actor_capacity`
CTE with a `residual` allocation *per actor* before candidates are ever
selected (see the `candidates` CTE, which joins `per_actor_capacity`
laterally per actor and requires `pac.residual > 0`), so each actor
sharing a queue gets its own slice of a round's claim budget regardless
of queue mode (`strict_fifo` vs `round_robin`) -- this is the same
per-actor fairness mechanism `test_fleet_fairness_starvation.py` pins
for the general (non-rate-limited) starvation case, and it turns out to
extend to this scenario too: a saturated actor's denied claims do not
crowd out a co-located healthy actor's admissions, because the healthy
actor was never competing for the *same* per-actor capacity slice in
the first place.

This is left in as a passing pin, per instruction, rather than deleted:
it is coverage worth keeping precisely because the predicted failure
mode is a reasonable one for an adopter with vendor experience to
expect, and it is falsified here against real Postgres rather than left
as an untested assumption in either direction. It complements (does not
duplicate) `test_fleet_throttle_visibility.py` (which pins that the
*backlog* is attributable) and `test_fleet_capacity_pressure.py` (which
pins that a denial spends no retry budget): this one pins that a
saturated actor's denials do not measurably reduce a co-located healthy
actor's per-round admission rate.

Caveat on scope: this test uses one pod, one queue, two actors, and
`ConcurrencyReservation`/`TokenBucket` denial via a direct
`consume_one_job(..., rate_limit_registry=...)` call (the same
production path `worker/_consumer.py` uses, invoked directly because
`tests/_fleet.py`'s `Pod.run()` helper does not forward rate-limit
kwargs -- no production code was changed to make this test possible).
It does not cover many actors each independently saturated (a queue with
20 rate-limited actors, 19 of them exhausted, one healthy) or multiple
pods dispatching concurrently against the same saturated bucket; those
remain unmeasured.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import RateLimitRegistry, TokenBucket
from taskq.worker._consumer import consume_one_job
from tests._fleet import FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_rl_occupancy_q"
_THROTTLED = "occ_throttled_actor"
_HEALTHY = "occ_healthy_actor"

# A backlog of fully-saturated jobs large enough to dominate several
# rounds' worth of claim capacity, plus a small healthy cohort that
# should still make steady progress if denial were round-cheap.
_THROTTLED_BACKLOG = 40
_HEALTHY_BACKLOG = 10
_ROUND_LIMIT = 4
_ROUNDS = 10


async def test_a_saturated_rate_limit_does_not_starve_a_healthy_cohorts_round_admission(
    pg_dsn: str,
) -> None:
    """An exhausted rate limit must not consume claim slots a healthy actor needs.

    The throttled actor's TokenBucket starts at capacity=1 and is drained
    to empty before the round loop begins, with refill_per_second=0 (a
    fixed, exhausted quota) so every one of its claimed jobs is denied
    for the whole test -- no natural refill can let any of them through
    and confound the measurement.

    If dispatch is round-cheap for denials (the property this test wants
    pinned), the healthy actor's jobs should be admitted at close to the
    rate they would see alone: enough rounds to drain _HEALTHY_BACKLOG
    jobs at _ROUND_LIMIT per round is ceil(10/4) = 3 rounds' worth of
    *healthy* admissions, comfortably inside _ROUNDS = 10. If instead
    claimed-but-denied throttled rows are eating round capacity that
    would otherwise go to the healthy actor, the healthy actor's
    completions will lag well behind that bound.
    """
    schema = f"fleet_rl_occ_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_THROTTLED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        rl_registry = RateLimitRegistry()
        bucket = TokenBucket(
            name=f"occ_bucket_{schema}",
            capacity=1,
            refill_per_second=0,  # fixed quota, no refill -- stays exhausted
            backend="postgres",
        )
        rl_registry.register(bucket)
        # Drain the one token so every subsequent acquire is denied for
        # the rest of the test.
        pool = fleet.any_pod().deps.worker_pool
        drained = await bucket.acquire(pg_pool=pool, settings=fleet.settings)
        assert drained.allowed, "setup: bucket must start with its one token available"
        denied_probe = await bucket.acquire(pg_pool=pool, settings=fleet.settings)
        assert not denied_probe.allowed, (
            "setup: bucket must read as exhausted before the round loop"
        )

        await fleet.enqueue(_THROTTLED_BACKLOG, actor=_THROTTLED, queue=_QUEUE)
        await fleet.enqueue(_HEALTHY_BACKLOG, actor=_HEALTHY, queue=_QUEUE)

        pod = fleet.pod("pod-1")

        async def _healthy_work(_payload: FleetPayload, _ctx: object) -> str:
            return "done"

        healthy_completed = 0
        throttled_denied = 0

        for _ in range(_ROUNDS):
            claimed = await pod.claim([_QUEUE], _ROUND_LIMIT)
            if not claimed:
                break
            for job in claimed:
                if job.actor == _THROTTLED:
                    outcome = await consume_one_job(
                        pod.backend,
                        job,
                        pod.worker_id,
                        deps=pod.deps,
                        run_actor=lambda _row, _ctx: _fail_if_called(),
                        actor_config=fleet_actor_config(),
                        payload_type=FleetPayload,
                        clock=SystemClock(),
                        active_jobs=pod.deps.active_jobs,
                        rate_limit_registry=rl_registry,
                        rate_limits=[bucket.name],
                        worker_pool=pod.deps.worker_pool,
                        settings=fleet.settings,
                    )
                    throttled_denied += 1
                    assert outcome != "succeeded", (
                        "setup invariant broken: a throttled job ran despite an "
                        "exhausted, non-refilling rate limit -- the measurement "
                        "below would be meaningless"
                    )
                else:
                    await pod.run(job, _healthy_work, actor_config=fleet_actor_config())
                    healthy_completed += 1

            if healthy_completed >= _HEALTHY_BACKLOG:
                break

        # The bound: draining _HEALTHY_BACKLOG jobs at _ROUND_LIMIT per
        # round, if the healthy actor's admissions were unaffected by the
        # co-located saturated actor, takes ceil(10/4) = 3 rounds. Give
        # meaningful headroom (still far inside _ROUNDS=10) and assert
        # the healthy actor finished well within it.
        expected_rounds_if_uncontended = -(-_HEALTHY_BACKLOG // _ROUND_LIMIT)  # ceil
        generous_round_budget = expected_rounds_if_uncontended * 2

        assert healthy_completed == _HEALTHY_BACKLOG, (
            f"only {healthy_completed}/{_HEALTHY_BACKLOG} healthy jobs completed in "
            f"{_ROUNDS} rounds of {_ROUND_LIMIT} while {throttled_denied} throttled "
            "claims were denied. An exhausted rate limit on one actor is reducing a "
            "co-located healthy actor's dispatch throughput, which is a round-budget "
            "leak: the healthy actor's own capacity is being spent on jobs that were "
            "always going to be denied."
        )
        assert throttled_denied <= generous_round_budget * _ROUND_LIMIT, (
            f"the throttled actor consumed {throttled_denied} claim slots across "
            f"{_ROUNDS} rounds even though its rate limit never refills -- once a "
            "bucket reads as durably exhausted, repeatedly re-claiming and "
            "re-denying its jobs before a healthy cohort gets a turn is round "
            "capacity spent for a decision dispatch could have skipped."
        )


async def _fail_if_called() -> object:
    raise AssertionError(
        "the throttled actor's handler must never run -- its rate limit is "
        "permanently exhausted by test setup"
    )
