"""Leader-only work runs on one pod, and keeps running when that pod dies.

Sweeps, cron ticks and reclaims must happen exactly once across the
fleet: run them everywhere and a cron schedule fires once per pod, run
them nowhere and expired leases are never reclaimed. Election is what
makes "exactly once" possible with every pod running identical code.

Two properties matter to an operator. The first is that at any moment
one pod holds the role - never two, because double-running maintenance
duplicates side effects the queue cannot undo. The second is that the
role survives losing the pod that holds it: a leader is just a pod, and
pods are replaced constantly, so a hand-off that requires intervention
means maintenance silently stops at the next deploy.

Losing an election is also the ordinary state of every follower. It
must be unremarkable - a pod that treats not being leader as a failure
fills the logs of a healthy fleet with alarming output and trains the
operator to ignore exactly the signal that would matter.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.testing.assertions import wait_for_condition
from taskq.worker.leader import MaintenanceLeader
from tests._fleet import Fleet, Pod, open_fleet

# Election turnover is paced by the heartbeat and the sweep loop, so
# these tests wait on real elections rather than on a simulated clock:
# they are the slowest in the fleet suite and carry the ``slow`` mark so
# a routine run can leave them out with ``-m 'not slow'``.
pytestmark = [pytest.mark.integration, pytest.mark.slow]

_QUEUE = "fleet_leader_q"
_ACTOR = "fleet_leader_actor"

# Election turnover is driven by the heartbeat: a leader that stops
# renewing is displaced once its claim goes stale. Shortening both keeps
# the failover observable without the test waiting on production
# timings.
_LEADER_SETTINGS = {"heartbeat_interval": "0.5", "lock_lease": "4.0"}

# The sweep cadence is the maintenance the role exists to perform, so a
# failover test has to wait a full sweep period to see it resume. At the
# shipped default that is half a minute of waiting for an event the
# fleet reaches in one tick; the loop's own interval is the honest knob
# to turn, rather than asserting on a shorter wait and hoping.
_SWEEPING_LEADER_SETTINGS = {**_LEADER_SETTINGS, "sweep_interval": "1.0"}


def _start_leader(pod: Pod) -> tuple[asyncio.Event, asyncio.Task[None]]:
    """Run the production election loop for this pod.

    Returns the stop event and the running task: a caller sets the event
    to take this pod out of the election, exactly as its shutdown does.
    """
    leader = MaintenanceLeader(pod.deps, pod.worker_id, pod.backend, clock=SystemClock())
    stop = asyncio.Event()
    return stop, asyncio.create_task(leader.run(stop))


async def _elected_count(fleet: Fleet) -> int:
    return sum(1 for pod in fleet.pods if pod.deps.is_leader.is_set())


async def _leader_row(fleet: Fleet) -> object | None:
    rows = await fleet.fetch(
        'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
    )
    return rows[0]["worker_id"] if rows else None


async def test_exactly_one_pod_holds_the_leader_role(pg_dsn: str) -> None:
    """Three pods racing for the role produce one leader, not three.

    Every pod runs the same election loop and all three start at once,
    which is what a fleet does when a deployment rolls out. The database
    row is the arbiter, and it must name exactly one holder.

    Two leaders means every sweep, prune and cron tick runs twice: a
    schedule fires twice per interval, and reclaim sweeps race each
    other over the same rows.
    """
    schema = f"fleet_lead_one_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2", "pod-3"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_LEADER_SETTINGS,
    ) as fleet:
        started = [_start_leader(fleet.pod(name)) for name in ("pod-1", "pod-2", "pod-3")]
        try:
            await wait_for_condition(
                lambda: any(pod.deps.is_leader.is_set() for pod in fleet.pods),
                description="a pod won the leader election",
                timeout=10.0,
            )
            # The role is settled once a pod holds it; the contract is
            # that no second pod ever joins it.
            elected = await _elected_count(fleet)
            assert elected == 1, (
                f"{elected} pods hold the leader role at once. Leader-only work - cron "
                "fires, reclaim sweeps, pruning - is running on every one of them, so "
                "scheduled jobs are enqueued once per leader and maintenance races "
                "itself over the same rows."
            )

            rows = await fleet.fetch('SELECT count(*) AS n FROM "{schema}".maintenance_leader')
            assert int(rows[0]["n"]) == 1, (
                f"the leader table holds {rows[0]['n']} rows; the role is a singleton and "
                "a second row means two pods can each believe they own it."
            )
        finally:
            for stop, _task in started:
                stop.set()
            await asyncio.gather(*(task for _stop, task in started))


async def test_the_role_passes_on_when_the_leader_pod_goes_away(
    pg_dsn: str,
) -> None:
    """A leader dying hands the role to a survivor without intervention.

    The leader is stopped the way a deploy or a node loss stops it: its
    loop ends and its pod leaves the fleet, with no hand-over step. A
    surviving pod must then take the role on its own.

    If it does not, maintenance stops at the next deploy and stays
    stopped - and nothing reports it, because every remaining pod is
    healthy and is not the leader. Expired leases go unreclaimed
    and cron stops firing, with the first symptom arriving hours later
    as work that never ran.
    """
    schema = f"fleet_lead_fail_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("incumbent", "successor"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_LEADER_SETTINGS,
    ) as fleet:
        incumbent = fleet.pod("incumbent")
        successor = fleet.pod("successor")

        stop_a, task_a = _start_leader(incumbent)
        await wait_for_condition(
            incumbent.deps.is_leader.is_set,
            description="the incumbent won the initial election",
            timeout=10.0,
        )
        held_by = await _leader_row(fleet)
        assert held_by == incumbent.worker_id

        stop_b, task_b = _start_leader(successor)
        try:
            # The leader goes away. Nothing tells the successor to take
            # over; it has to notice for itself.
            stop_a.set()
            await task_a
            await fleet.stop_pod("incumbent", graceful=False)

            await wait_for_condition(
                successor.deps.is_leader.is_set,
                description="the surviving pod took over the leader role",
                timeout=20.0,
            )
            assert await _leader_row(fleet) == successor.worker_id, (
                "the leader row still names the pod that left the fleet. No live pod "
                "owns maintenance: sweeps, reclaims and cron have stopped, and every "
                "remaining pod reports itself healthy while they stay stopped."
            )
        finally:
            stop_b.set()
            await task_b


async def test_leader_only_maintenance_still_runs_after_a_failover(
    pg_dsn: str,
) -> None:
    """The work the role exists for continues across the hand-off.

    Holding the row is not the point; doing the maintenance is. After
    the incumbent is gone, a job orphaned by a dead pod must still be
    reclaimed - that reclaim is the fleet's only route back for work
    whose owner died, so a failover that hands over the title without
    the duties loses exactly that work.
    """
    schema = f"fleet_lead_work_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("incumbent", "successor"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_SWEEPING_LEADER_SETTINGS,
    ) as fleet:
        incumbent = fleet.pod("incumbent")
        successor = fleet.pod("successor")
        await fleet.enqueue(4, actor=_ACTOR, queue=_QUEUE)

        # Work is claimed and then orphaned: its holder's lease is long
        # past and its process is gone.
        claimed = await incumbent.claim([_QUEUE], 4)
        assert len(claimed) == 4
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET lock_expires_at = $1 '
            "WHERE locked_by_worker = $2 RETURNING id",
            datetime.now(UTC) - timedelta(minutes=5),
            incumbent.worker_id,
        )

        stop_a, task_a = _start_leader(incumbent)
        await wait_for_condition(
            incumbent.deps.is_leader.is_set,
            description="the incumbent won the initial election",
            timeout=10.0,
        )
        stop_a.set()
        await task_a
        await fleet.stop_pod("incumbent", graceful=False)

        stop_b, task_b = _start_leader(successor)
        try:
            await wait_for_condition(
                successor.deps.is_leader.is_set,
                description="the surviving pod took over the leader role",
                timeout=20.0,
            )

            async def _orphans_recovered() -> bool:
                rows = await fleet.fetch(
                    'SELECT count(*) AS n FROM "{schema}".jobs '
                    "WHERE status = 'running' AND locked_by_worker = $1",
                    incumbent.worker_id,
                )
                return int(rows[0]["n"]) == 0

            await wait_for_condition(
                _orphans_recovered,
                description=(
                    "the new leader's sweeps reclaimed the jobs orphaned by the pod that "
                    "died holding them"
                ),
                timeout=30.0,
            )
        finally:
            stop_b.set()
            await task_b


async def test_losing_an_election_is_not_reported_as_a_failure(
    pg_dsn: str,
) -> None:
    """Followers are the normal case and must not look like broken pods.

    In any fleet larger than one, most pods are followers all the time.
    If losing an election surfaced as an error - a raised exception, a
    loop that gives up - then the bigger the fleet the noisier it is
    while perfectly healthy, and the operator learns to discount the
    signal that would tell them something real.

    The observable contract is modest and exact: the follower's election
    loop keeps running, stays ready to take over, and reports itself as
    not-leader rather than as failed.
    """
    schema = f"fleet_lead_follow_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("winner", "follower"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_LEADER_SETTINGS,
    ) as fleet:
        winner = fleet.pod("winner")
        follower = fleet.pod("follower")

        stop_a, task_a = _start_leader(winner)
        await wait_for_condition(
            winner.deps.is_leader.is_set,
            description="the first pod won the election",
            timeout=10.0,
        )

        stop_b, task_b = _start_leader(follower)
        try:
            # Give the follower's loop several election attempts against
            # a healthy incumbent, then require it to be alive and well.
            await wait_for_condition(
                lambda: task_b.done() or follower.deps.is_leader.is_set() is False,
                description="the follower's election loop completed an attempt",
                timeout=10.0,
            )

            assert not task_b.done(), (
                "the follower's election loop exited while another pod held the role. A "
                "pod that stops running its election on losing one can never take over, "
                f"so the fleet has no successor: {task_b.exception()!r}"
            )
            assert not follower.deps.is_leader.is_set(), (
                "both pods believe they are leader, so leader-only maintenance is running twice."
            )

            # The proof that losing was harmless: this pod can still win.
            stop_a.set()
            await task_a
            await fleet.stop_pod("winner", graceful=False)
            await wait_for_condition(
                follower.deps.is_leader.is_set,
                description="the follower took the role once it was free",
                timeout=20.0,
            )
        finally:
            stop_b.set()
            await task_b
