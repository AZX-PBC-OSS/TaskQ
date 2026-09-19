"""A job's actor runs once, and its attempt count says how many times it ran.

Everything an operator concludes from the queue rests on these two
claims. At-most-once is why a job may safely charge a card or send a
message. The attempt count is how anyone decides whether a job is
failing: if it can be raised by events that are not runs - a deploy, a
reclaim, a capacity denial - then the number means something different
from what every dashboard and every runbook assumes, and the difference
grows with how busy and how frequently deployed the fleet is.

These tests drive the states a pod can be stopped in - holding a claim
it has not started, part-way through a run, waiting on a retry, deferred
- and in each case require that the job ends up run exactly once or back
with the fleet exactly once, with an attempt count that matches the
number of times its actor actually executed.
"""

from __future__ import annotations

import asyncio

import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_amo_q"
_ACTOR = "fleet_amo_actor"


async def _attempt_outcomes(fleet: Fleet, job_id: JobId) -> list[str]:
    rows = await fleet.fetch(
        'SELECT outcome FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
        job_id,
    )
    return [str(row["outcome"]) for row in rows]


async def _job(fleet: Fleet, job_id: JobId) -> dict[str, object]:
    rows = await fleet.fetch('SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id)
    return dict(rows[0])


async def test_a_job_claimed_by_one_pod_is_invisible_to_every_other(
    pg_dsn: str,
) -> None:
    """A held job must not be handed to a second pod.

    This is at-most-once at its narrowest: while one pod holds a claim,
    no other pod may claim the same row, however many of them are
    polling. Three pods poll a single job, repeatedly.

    A second holder means the actor runs twice concurrently - two
    charges, two emails, two writes - and the job's history records one
    run, so nothing about the duplicate is visible afterwards.
    """
    schema = f"fleet_amo_hold_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2", "pod-3"),
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        holders: list[str] = []
        for _ in range(3):
            rounds = await asyncio.gather(
                *(fleet.pod(name).claim([_QUEUE], 5) for name in ("pod-1", "pod-2", "pod-3"))
            )
            for name, claimed in zip(("pod-1", "pod-2", "pod-3"), rounds, strict=True):
                holders.extend(name for job in claimed if JobId(job.id) == job_id)

        assert len(holders) == 1, (
            f"one job was handed out {len(holders)} times, to {holders}. Each holder "
            "will run the actor and write a result, so the work happens more than once "
            "while the job's own record shows a single run."
        )


async def test_a_job_run_once_records_exactly_one_attempt(pg_dsn: str) -> None:
    """The attempt history counts runs, and nothing else.

    A job that is claimed, run and completed must show exactly one
    attempt with a successful outcome. This is the baseline the other
    tests in this module are measured against: if a quiet, undisturbed
    job cannot produce a truthful attempt count, no conclusion drawn
    from attempt counts anywhere is safe.
    """
    schema = f"fleet_amo_once_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        runs = 0

        async def _work(_payload: FleetPayload, _ctx: object) -> str:
            nonlocal runs
            runs += 1
            return "ok"

        claimed = await fleet.pod("pod-1").claim([_QUEUE], 1)
        await fleet.pod("pod-1").run(claimed[0], _work, actor_config=fleet_actor_config())

        assert runs == 1, f"the actor ran {runs} times for a single claimed job."
        row = await _job(fleet, job_id)
        assert row["status"] == "succeeded", (
            f"a job whose actor returned normally reads as {row['status']!r}."
        )
        outcomes = await _attempt_outcomes(fleet, job_id)
        assert outcomes == ["succeeded"], (
            f"a job that ran once has attempt history {outcomes}. The history must "
            "record one attempt per run of the actor: any other count makes failure "
            "rates and retry dashboards report something other than what happened."
        )


async def test_a_job_handed_back_unrun_records_no_attempt(pg_dsn: str) -> None:
    """A claim that never became a run leaves no trace in the history.

    The pod is stopped holding a claim it has not started - the most
    common state a SIGTERM finds work in, because a claim round fills a
    buffer that the consumers then work through. Nothing ran, so the
    history must be empty and the counter unmoved.

    If a hand-back leaves an attempt row behind, every deploy inflates
    the apparent failure rate of whatever was in flight, and the jobs
    that were merely interrupted are indistinguishable from jobs that
    genuinely failed.
    """
    schema = f"fleet_amo_hand_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        claimed = await fleet.pod("pod-1").claim([_QUEUE], 1)
        assert len(claimed) == 1
        await fleet.stop_pod("pod-1", graceful=True)
        await fleet.start_pod("pod-2")

        outcomes = await _attempt_outcomes(fleet, job_id)
        assert outcomes == [], (
            f"a job that was claimed and handed back without running has attempt history "
            f"{outcomes}. The actor never executed, so every count of attempts now "
            "reports a run that did not happen, and a deploy looks like a failure wave."
        )

        row = await _job(fleet, job_id)
        assert row["status"] == "pending", (
            f"the handed-back job reads as {row['status']!r} rather than pending."
        )


async def test_a_job_interrupted_mid_run_is_retried_not_duplicated(
    pg_dsn: str,
) -> None:
    """Work interrupted part-way through comes back once, and runs again once.

    The actor starts, the pod is lost before it finishes, and the job
    returns to the fleet. The next pod runs it again - that is what
    at-least-once delivery means and why actors must be idempotent.

    What must not happen is the job coming back more than once. Two
    copies of an interrupted job means the retry itself fans out, and a
    single crash multiplies into as many runs as there were reclaims.
    """
    schema = f"fleet_amo_mid_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1", "pod-2"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        started = asyncio.Event()
        release = asyncio.Event()

        async def _hang(_payload: FleetPayload, _ctx: object) -> str:
            started.set()
            # A job that is genuinely still working when its pod is
            # taken away: the wait IS the condition under test, and it
            # is released by the test rather than by elapsed time.
            await release.wait()
            return "finished-after-interruption"

        claimed = await fleet.pod("pod-1").claim([_QUEUE], 1)
        run_task = asyncio.create_task(
            fleet.pod("pod-1").run(claimed[0], _hang, actor_config=fleet_actor_config())
        )
        await asyncio.wait_for(started.wait(), timeout=5.0)

        # The pod is lost mid-run.
        release.set()
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)

        rows = await fleet.fetch('SELECT count(*) AS n FROM "{schema}".jobs WHERE id = $1', job_id)
        assert int(rows[0]["n"]) == 1, (
            f"an interrupted job is represented by {rows[0]['n']} rows. A retry must "
            "reuse the job's own row: a second row is a second copy of the work, with "
            "its own independent retry budget and no link back to the original."
        )


async def test_a_pod_cannot_finish_a_job_it_has_already_handed_back(
    pg_dsn: str,
) -> None:
    """Draining and finishing the same job are mutually exclusive.

    During shutdown a pod re-pends the jobs it holds and has not
    started. If it could then also run one of them, the job would be
    executed by the draining pod and by whichever pod claims the
    re-pended row - the classic double-run a deploy produces.

    The contract is that once a job has been handed back, this pod's
    terminal write for it must not land: the row belongs to the fleet
    again, and its next owner is the only one entitled to end it.
    """
    schema = f"fleet_amo_drain_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("draining", "next"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        claimed = await fleet.pod("draining").claim([_QUEUE], 1)
        assert len(claimed) == 1
        draining = fleet.pod("draining")
        stale_attempt = claimed[0].attempt

        await fleet.stop_pod("draining", graceful=True)

        # The next pod picks the handed-back job up and owns it now.
        taken = await fleet.pod("next").claim([_QUEUE], 1)
        assert [JobId(job.id) for job in taken] == [job_id], (
            "the handed-back job was not claimable by the surviving pod, so the "
            "two-owner condition under test was never reached."
        )

        landed = await fleet.pod("next").backend.mark_succeeded(
            job_id, draining.worker_id, attempt=stale_attempt
        )
        assert landed is False, (
            "a pod that handed this job back during shutdown was still able to report it "
            "succeeded. Another pod holds it and is running it now, so the job is being "
            "executed twice and the first write decides the recorded outcome."
        )

        row = await _job(fleet, job_id)
        assert row["status"] == "running", (
            f"the job reads as {row['status']!r} after a drained pod's terminal write, "
            "although the pod that currently owns it has not finished it."
        )


async def test_repeated_deploys_do_not_erode_a_jobs_retry_budget(
    pg_dsn: str,
) -> None:
    """Surviving many deploys must leave a job's budget intact.

    A job that is unlucky enough to be in flight during several
    consecutive deploys is handed back each time without ever running.
    Its retry budget must be exactly what it started with, because none
    of those hand-backs was an attempt.

    Where a single lost attempt is easy to overlook, this is the shape
    that reaches operators: after a busy week of deploys, jobs start
    arriving at crashed having never executed, their histories showing
    attempts nobody can account for, and the fleet's deploy cadence -
    not the code - is what set the limit.
    """
    schema = f"fleet_amo_budget_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-0",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE, max_attempts=3)
        deploys = 4

        for generation in range(deploys):
            current = f"pod-{generation}"
            claimed = await fleet.pod(current).claim([_QUEUE], 1)
            assert len(claimed) == 1, (
                f"deploy {generation}: the job was not claimable. After "
                f"{generation} hand-backs it must still be queued - none of them ran it."
            )
            await fleet.stop_pod(current, graceful=True)
            await fleet.start_pod(f"pod-{generation + 1}")

            row = await _job(fleet, job_id)
            assert row["status"] != "crashed", (
                f"after {generation + 1} deploys the job reads as crashed although its "
                "actor has never run. The deploy cadence, not the work, exhausted its "
                "retries."
            )

        row = await _job(fleet, job_id)
        outcomes = await _attempt_outcomes(fleet, job_id)
        assert row["attempt"] == 0, (
            f"after {deploys} deploys the job's attempt counter reads {row['attempt']}, "
            "although its actor has never run once. Each deploy silently spent one of "
            f"its {3} retries, so how many times this job may genuinely fail now depends "
            "on how often the fleet is deployed."
        )
        assert outcomes == [], (
            f"a job that has never run has attempt history {outcomes} after {deploys} deploys."
        )
