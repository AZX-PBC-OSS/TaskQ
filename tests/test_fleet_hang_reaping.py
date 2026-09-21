"""A job whose worker stops making progress is reaped and tried again.

The hardest failure to recover from is not a crash but a hang: the pod
is up, its lease renews, its health checks pass, and one job inside it
has stopped moving. Nothing about the pod looks wrong, so nothing
replaces it, and the job stays checked out for as long as the process
lives. A queue with no answer to this loses the job silently and reports
it as in progress the entire time.

Two deadlines answer it, and they answer different questions. A
heartbeat timeout catches a job that has stopped reporting progress
while its pod is otherwise alive. A schedule-to-close deadline bounds
the total time a job may occupy, however many attempts or pods it takes.
Both must end in the work being either retried or failed visibly -
never left running for ever, and never quietly discarded.

Recovery also has to be honest about what it costs the job. A reaped
attempt did happen - the actor ran, it did not finish - so it is
right that it is recorded and right that it counts. That is the exact
opposite of a hand-back, where nothing ran, and the two must not be
confused: one is the system telling the truth about a stuck run, the
other is the system inventing a failure that never occurred.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_hang_q"
_ACTOR = "fleet_hang_actor"


async def _job(fleet: Fleet, job_id: JobId) -> dict[str, object]:
    rows = await fleet.fetch(
        'SELECT status, attempt, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    return dict(rows[0])


async def _attempt_outcomes(fleet: Fleet, job_id: JobId) -> list[str]:
    rows = await fleet.fetch(
        'SELECT outcome FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
        job_id,
    )
    return [str(row["outcome"]) for row in rows]


async def test_a_job_that_stops_heartbeating_is_reclaimed(pg_dsn: str) -> None:
    """A stalled job is taken back even though its pod is alive.

    The job declares a heartbeat timeout, is claimed, and then stops
    reporting - the shape of an actor blocked on something that will
    never return, inside a pod that is otherwise healthy and whose lock
    lease keeps renewing.

    The lease alone cannot catch this: the pod is alive and renewing it.
    Only the per-job deadline can, which is why the timeout exists. If
    it does not fire, the job is checked out until the pod is replaced
    for some unrelated reason, and the queue reports it as running the
    whole time.
    """
    schema = f"fleet_hang_hb_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("stalled", "leader"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(
            1, actor=_ACTOR, queue=_QUEUE, heartbeat_timeout=timedelta(seconds=1)
        )

        claimed = await fleet.pod("stalled").claim([_QUEUE], 1)
        assert len(claimed) == 1

        # The pod is alive and its lease is healthy: only the job has
        # stopped. Ageing the job's last beat past its own timeout is
        # that condition exactly, reached without waiting it out.
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET last_heartbeat_at = $1, lock_expires_at = $2 '
            "WHERE id = $3 RETURNING id",
            datetime.now(UTC) - timedelta(minutes=5),
            datetime.now(UTC) + timedelta(minutes=5),
            job_id,
        )

        reclaimed = await fleet.pod("leader").backend.reclaim_expired_locks(
            timedelta(seconds=0), timedelta(seconds=0)
        )
        assert reclaimed >= 1, (
            "a job that declared a heartbeat timeout and then stopped reporting was not "
            "reclaimed, although its holder's lock lease is still valid. Nothing else "
            "can catch this: the pod is healthy, so the job stays checked out until that "
            "pod is replaced for some unrelated reason, and the queue calls it running "
            "for the whole of that time."
        )

        row = await _job(fleet, job_id)
        assert row["status"] in {"pending", "scheduled"}, (
            f"the reclaimed job reads as {row['status']!r}; a job taken back from a "
            "stalled worker must be waiting for the fleet to try again."
        )
        assert row["locked_by_worker"] is None, (
            "the reclaimed job is still locked to the pod it was taken from, so no other "
            "pod can pick it up."
        )


async def test_a_reaped_attempt_is_recorded_and_counted(pg_dsn: str) -> None:
    """Reaping records the attempt that really happened.

    The actor did run - it started and then stalled - so the attempt is
    real and belongs in the history, and it is right that it counts
    against the retry budget. Otherwise a job that hangs every time it
    runs would be retried for ever, occupying a slot in every pod it
    touches.

    This is the deliberate counterpart to the hand-back contract: there
    the actor never ran and the budget must be refunded. Confusing the
    two in either direction is a defect - inventing failures that did not
    happen, or letting a genuinely stuck job retry without limit.
    """
    schema = f"fleet_hang_count_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("stalled", "leader"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(
            1,
            actor=_ACTOR,
            queue=_QUEUE,
            max_attempts=3,
            heartbeat_timeout=timedelta(seconds=1),
        )

        claimed = await fleet.pod("stalled").claim([_QUEUE], 1)
        attempt_when_claimed = claimed[0].attempt

        await fleet.fetch(
            'UPDATE "{schema}".jobs SET last_heartbeat_at = $1, lock_expires_at = $2 '
            "WHERE id = $3 RETURNING id",
            datetime.now(UTC) - timedelta(minutes=5),
            datetime.now(UTC) + timedelta(minutes=5),
            job_id,
        )
        await fleet.pod("leader").backend.reclaim_expired_locks(
            timedelta(seconds=0), timedelta(seconds=0)
        )

        outcomes = await _attempt_outcomes(fleet, job_id)
        assert outcomes, (
            "reaping a stalled job left no record of the attempt. The actor ran and "
            "stalled, and the job's history shows nothing - so an operator seeing the "
            "job retry has no way to learn that a previous run hung."
        )
        row = await _job(fleet, job_id)
        assert int(str(row["attempt"])) >= attempt_when_claimed, (
            f"a reaped attempt left the counter at {row['attempt']}, below the "
            f"{attempt_when_claimed} the run was issued under. A job that hangs on every "
            "attempt would retry for ever, taking a slot on every pod it touches."
        )


async def test_a_reaped_job_is_redispatched_to_a_healthy_pod(pg_dsn: str) -> None:
    """Reaping is only half of recovery; the work has to run.

    A job taken back from a stalled worker must be claimable by a
    healthy one and must then complete. Reclaiming without redispatch
    would move the job from stuck-and-running to stuck-and-
    pending, which is harder to notice, not easier.
    """
    schema = f"fleet_hang_redis_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("stalled", "healthy"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(
            1, actor=_ACTOR, queue=_QUEUE, heartbeat_timeout=timedelta(seconds=1)
        )

        claimed = await fleet.pod("stalled").claim([_QUEUE], 1)
        assert len(claimed) == 1
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET last_heartbeat_at = $1, lock_expires_at = $2 '
            "WHERE id = $3 RETURNING id",
            datetime.now(UTC) - timedelta(minutes=5),
            datetime.now(UTC) + timedelta(minutes=5),
            job_id,
        )
        await fleet.pod("healthy").backend.reclaim_expired_locks(
            timedelta(seconds=0), timedelta(seconds=0)
        )

        # Serve whatever backoff the reclaim scheduled, on the row.
        await fleet.fetch(
            "UPDATE \"{schema}\".jobs SET scheduled_at = $1, status = 'pending' "
            "WHERE id = $2 RETURNING id",
            datetime.now(UTC) - timedelta(seconds=1),
            job_id,
        )

        retaken = await fleet.pod("healthy").claim([_QUEUE], 1)
        assert [JobId(job.id) for job in retaken] == [job_id], (
            "a job reaped from a stalled pod was not claimable by a healthy one. The "
            "reclaim moved it out of running and no further, so it is now stuck pending "
            "- which no stuck-job alert built around running jobs will ever show."
        )

        async def _work(_payload: FleetPayload, _ctx: object) -> str:
            return "recovered"

        await fleet.pod("healthy").run(retaken[0], _work, actor_config=fleet_actor_config())
        row = await _job(fleet, job_id)
        assert row["status"] == "succeeded", (
            f"a reaped and redispatched job reads as {row['status']!r} after running to "
            "completion on a healthy pod. Recovery that does not end in the work being "
            "done is not recovery."
        )


async def test_a_job_past_its_close_deadline_fails_visibly(pg_dsn: str) -> None:
    """Total time is bounded, and running out of it is not silent.

    Schedule-to-close bounds how long a job may take across every
    attempt and every pod. It is the backstop for work that is retried
    indefinitely by transient failures or repeated reaping: without it a
    job can consume slots for ever.

    Hitting it must produce a terminal state an operator can see and
    alert on. A job that stops being retried, with no terminal
    status and no event, is indistinguishable from one still waiting its
    turn.
    """
    schema = f"fleet_hang_close_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET schedule_to_close = $1 WHERE id = $2 RETURNING id',
            datetime.now(UTC) - timedelta(minutes=1),
            job_id,
        )

        swept = await fleet.pod("pod-1").backend.deadline_sweep()
        assert swept >= 1, (
            "a job past its schedule-to-close deadline was not swept. Its total time is "
            "unbounded after all: it keeps being retried and keeps occupying slots, and "
            "the deadline an operator set to stop exactly that did nothing."
        )

        row = await _job(fleet, job_id)
        assert row["status"] in {"failed", "crashed", "cancelled"}, (
            f"a job past its deadline reads as {row['status']!r} rather than a terminal "
            "state. It has stopped being worked on but says nothing about why, so it is "
            "indistinguishable from a job still waiting for a pod."
        )


async def test_a_hanging_job_does_not_block_its_pods_other_work(
    pg_dsn: str,
) -> None:
    """One stuck job must not stop the pod that is running it.

    Pods run several jobs at once. A single actor blocked on something
    that never returns costs one slot; it must not cost the pod's
    ability to claim and finish everything else. A queue where one hung
    job stalls its whole pod turns a single bad job into a fleet-wide
    outage as each pod in turn picks up a copy of it.
    """
    schema = f"fleet_hang_block_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(5, actor=_ACTOR, queue=_QUEUE)
        pod = fleet.pod("pod-1")

        claimed = await pod.claim([_QUEUE], 5)
        assert len(claimed) == 5

        hanging_started = asyncio.Event()
        release = asyncio.Event()

        async def _hang(_payload: FleetPayload, _ctx: object) -> str:
            hanging_started.set()
            # The wait IS the hang under test; it ends when the test
            # releases it, never on a timer.
            await release.wait()
            return "eventually"

        async def _work(_payload: FleetPayload, _ctx: object) -> str:
            return "done"

        hung = asyncio.create_task(pod.run(claimed[0], _hang, actor_config=fleet_actor_config()))
        await asyncio.wait_for(hanging_started.wait(), timeout=5.0)

        # With one slot wedged, the rest of the round must still finish.
        for job in claimed[1:]:
            await pod.run(job, _work, actor_config=fleet_actor_config())

        for job in claimed[1:]:
            row = await _job(fleet, JobId(job.id))
            assert row["status"] == "succeeded", (
                f"a job on a pod that also holds a hung job reads as {row['status']!r}. "
                "One actor blocked on something that never returns has stopped the pod's "
                "other work, so a single bad job becomes a fleet-wide outage as each pod "
                "picks up a copy of it."
            )

        release.set()
        await asyncio.wait_for(hung, timeout=10.0)
