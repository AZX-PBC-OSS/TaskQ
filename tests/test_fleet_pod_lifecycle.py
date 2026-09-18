"""A pod leaving the fleet must cost the jobs it held nothing.

Pods stop constantly: a rolling deploy replaces every one of them, an
autoscaler removes them under light load, a node drains, an OOM killer
takes one without warning. The queue's correctness cannot depend on
which of those happened. Every job a departing pod held must end up in
exactly one place — finished once, or back with the fleet once — and
must arrive there with the same retry budget it would have had if the
pod had never been disturbed.

The budget half is the part that hides. A hand-back that quietly spends
an attempt looks like nothing at all on the day it happens; it surfaces
weeks later as jobs reaching ``crashed`` after a busy deploy week, with
nothing in their history naming the deploys as the cause. Deploy
frequency silently becomes the retry budget.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from taskq.testing.assertions import wait_for_condition
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_life_q"
_ACTOR = "fleet_life_actor"

_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})


async def _attempt_of(fleet: Fleet, job_id: JobId) -> int:
    rows = await fleet.fetch('SELECT attempt FROM "{schema}".jobs WHERE id = $1', job_id)
    return int(rows[0]["attempt"])


async def _status_of(fleet: Fleet, job_id: JobId) -> str:
    rows = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(rows[0]["status"])


async def test_rolling_deploy_settles_every_job_exactly_once(pg_dsn: str) -> None:
    """Replacing every pod in turn loses no job and duplicates none.

    The sequence is the one a deployment controller performs: with the
    fleet busy, stop a pod, bring its replacement up, and repeat until
    none of the original pods remain. Jobs are claimed and completed
    throughout, so at every step some rows are held by a pod that is
    about to go away.

    Afterwards each job must be in exactly one place. A job that is
    finished must have finished once; a job that is not finished must be
    claimable by the surviving fleet. A job that is neither — still
    marked running, locked to a pod that no longer exists — is the
    failure this pins: it is invisible to the queue's own accounting,
    no pod will ever finish it, and it stays that way until a lease
    expires long after the deploy is reported complete.
    """
    schema = f"fleet_rolling_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1", "pod-2"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        job_ids = await fleet.enqueue(24, actor=_ACTOR, queue=_QUEUE)
        completed: list[JobId] = []

        async def _finish(_payload: FleetPayload, _ctx: object) -> None:
            return None

        # Roll the fleet: each generation claims and runs some work, then
        # is replaced while still holding the rest of its round.
        for generation, (departing, arriving) in enumerate(
            (("pod-1", "pod-3"), ("pod-2", "pod-4")), start=1
        ):
            claimed = await fleet.pod(departing).claim([_QUEUE], 6)
            assert claimed, (
                f"generation {generation}: the departing pod claimed nothing, so this "
                "roll would not exercise a pod leaving with work in hand."
            )
            # Finish part of the round; the rest is still held when the
            # pod is told to stop, which is the state a SIGTERM finds.
            for job in claimed[:2]:
                await fleet.pod(departing).run(job, _finish, actor_config=fleet_actor_config())
                completed.append(JobId(job.id))

            departing_worker = fleet.pod(departing).worker_id
            exit_code = await fleet.stop_pod(departing, graceful=True)
            assert exit_code == 0, (
                f"generation {generation}: graceful shutdown of {departing} reported exit "
                f"code {exit_code}; a deploy that cannot drain a pod cleanly will be "
                "retried by the controller and the fleet churns."
            )

            stranded = await fleet.rows_locked_by(departing_worker)
            assert stranded == [], (
                f"generation {generation}: {len(stranded)} jobs are still marked running "
                f"and locked to {departing}, which has exited. No pod alive holds them, "
                "no shutdown phase is left to release them, and the queue reports them "
                "as in progress. They resume only when a lock lease expires, long after "
                "the deploy is reported complete."
            )

            await fleet.start_pod(arriving)

        states = await fleet.job_states()
        assert len(states) == len(job_ids), (
            f"{len(job_ids)} jobs were enqueued but {len(states)} rows exist; the roll "
            "lost or duplicated rows outright."
        )

        unfinished_and_locked = [job_id for job_id, status in states.items() if status == "running"]
        assert unfinished_and_locked == [], (
            f"after the roll completed, {len(unfinished_and_locked)} jobs are still "
            "marked running although every pod that claimed them has exited. An operator "
            "watching queue depth sees work in progress that no process is doing."
        )

        for job_id in completed:
            status = await _status_of(fleet, job_id)
            assert status == "succeeded", (
                f"job {job_id} was run to completion before its pod was replaced but "
                f"reads as {status!r}. Work that finished must stay finished across a "
                "deploy, or the deploy silently re-runs side effects."
            )


async def test_handback_during_shutdown_refunds_the_claim_attempt(pg_dsn: str) -> None:
    """A job handed back by a deploy must not have paid an attempt.

    Claiming a job increments its attempt counter, because a claim is
    normally the start of a run. When a graceful shutdown hands the job
    straight back without ever running the actor, that increment has
    bought nothing and must be refunded.

    If it is not, every deploy costs every in-flight job one retry.
    A job with three attempts that is unlucky in three consecutive
    deploys reaches its limit and is written off as crashed without its
    actor having executed even once — and its history shows three
    attempts, so the operator investigating concludes the code is broken
    rather than the deploys.
    """
    schema = f"fleet_refund_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        job_ids = await fleet.enqueue(4, actor=_ACTOR, queue=_QUEUE, max_attempts=3)

        claimed = await fleet.pod("pod-1").claim([_QUEUE], 4)
        assert len(claimed) == 4, (
            f"the pod claimed {len(claimed)} of 4 jobs; the hand-back below only pins "
            "the budget contract for jobs the pod actually holds."
        )
        attempts_when_claimed = {
            JobId(job.id): await _attempt_of(fleet, JobId(job.id)) for job in claimed
        }

        # Nothing runs. The pod is told to stop while every claim is
        # still exactly where the claim round left it.
        await fleet.stop_pod("pod-1", graceful=True)
        await fleet.start_pod("pod-2")

        for job_id in job_ids:
            status = await _status_of(fleet, job_id)
            assert status == "pending", (
                f"job {job_id} reads as {status!r} after a graceful shutdown handed it "
                "back; a job that never ran must return to the queue as pending."
            )
            attempt_now = await _attempt_of(fleet, job_id)
            assert attempt_now == 0, (
                f"job {job_id} was claimed (attempt counter {attempts_when_claimed[job_id]}) "
                f"and handed back without its actor ever running, but its attempt counter "
                f"reads {attempt_now} rather than 0. The deploy spent one of this job's "
                "three retries on a run that never happened: deploy frequency, not code "
                "quality, now sets the effective retry budget, and a job can reach crashed "
                "having never executed."
            )


async def test_abrupt_kill_returns_work_to_the_fleet_via_lease_expiry(
    pg_dsn: str,
) -> None:
    """A pod that dies without draining still gives its work back.

    There is no graceful path here: the pod's pools close under it with
    its claims held, which is what the fleet sees when a node is lost, a
    container is OOM-killed, or a SIGKILL follows an unresponsive
    SIGTERM. The only mechanism left is the lock lease, and the only
    actor is the reclaim sweep run by a surviving pod.

    The contract is that the sweep converges: after the lease is past,
    the killed pod's rows are back in the fleet's reach, and a surviving
    pod can claim and finish them. A queue where this does not converge
    loses the work permanently with no error anywhere.
    """
    schema = f"fleet_kill_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("doomed", "survivor"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(6, actor=_ACTOR, queue=_QUEUE)

        claimed = await fleet.pod("doomed").claim([_QUEUE], 3)
        assert len(claimed) == 3
        doomed_ids = {JobId(job.id) for job in claimed}

        # Expire the lease the killed pod holds, rather than waiting it
        # out: the row's own expiry column is the input the reclaim path
        # reads, so moving it is the same condition a real expiry
        # produces, reached without a timing window.
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET lock_expires_at = $1 '
            "WHERE locked_by_worker = $2 RETURNING id",
            datetime.now(UTC) - timedelta(minutes=5),
            fleet.pod("doomed").worker_id,
        )
        await fleet.stop_pod("doomed", graceful=False)

        reclaimed = await fleet.pod("survivor").backend.reclaim_expired_locks(
            timedelta(seconds=0), timedelta(seconds=0)
        )
        assert reclaimed >= len(doomed_ids), (
            f"the reclaim sweep recovered {reclaimed} rows but the killed pod held "
            f"{len(doomed_ids)} with expired leases. Whatever it left behind is marked "
            "running and owned by a process that no longer exists — work the fleet has "
            "silently stopped doing."
        )

        # Reclaim schedules the recovered attempt behind the retry
        # policy's backoff rather than making it instantly claimable,
        # which is what stops a crash-looping job from spinning. The
        # fleet contract is that the work comes back when that backoff
        # is served, so the wait is served here by moving the row's own
        # due time rather than by sleeping through it.
        for job_id in doomed_ids:
            status = await _status_of(fleet, job_id)
            assert status in {"pending", "scheduled"}, (
                f"job {job_id} reads as {status!r} after its holder was killed and the "
                "sweep ran. A crash-reclaimed job must be waiting for the fleet, not "
                "sitting in a state no pod will look at."
            )
        await fleet.fetch(
            "UPDATE \"{schema}\".jobs SET scheduled_at = $1, status = 'pending' "
            "WHERE id = ANY($2::uuid[]) RETURNING id",
            datetime.now(UTC) - timedelta(seconds=1),
            list(doomed_ids),
        )

        async def _finish(_payload: FleetPayload, _ctx: object) -> None:
            return None

        recovered: set[JobId] = set()
        for _ in range(6):
            batch = await fleet.pod("survivor").claim([_QUEUE], 6)
            if not batch:
                break
            for job in batch:
                await fleet.pod("survivor").run(job, _finish, actor_config=fleet_actor_config())
                recovered.add(JobId(job.id))

        missing = doomed_ids - recovered
        assert missing == set(), (
            f"{len(missing)} jobs held by the killed pod were never claimable again by "
            "the surviving fleet, although their leases expired, the sweep returned them "
            "to the queue and their retry backoff was served. They are lost: no pod owns "
            "them and no sweep returns them."
        )


async def test_a_snoozed_job_survives_the_pod_that_snoozed_it(pg_dsn: str) -> None:
    """A snooze outlives the process that issued it.

    Deferral is durable state on the row, not a timer in a worker's
    memory — that is the whole reason it is written to the database. A
    pod that snoozes a job and is then replaced by a deploy must leave a
    job that the next pod picks up when it comes due.

    If the deferral lived in the departing process, the job would simply
    never run again, and nothing would report it: the row would sit at a
    non-terminal status that no pod is watching, indistinguishable from
    a job legitimately waiting for its time.
    """
    schema = f"fleet_snooze_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        claimed = await fleet.pod("pod-1").claim([_QUEUE], 1)
        assert len(claimed) == 1

        # The terminal writes are fenced on the attempt the claim handed
        # out, so the pod passes the attempt it is actually holding —
        # the same value the consumer passes on the production path.
        outcome = await fleet.pod("pod-1").backend.mark_snoozed(
            job_id,
            fleet.pod("pod-1").worker_id,
            timedelta(hours=1),
            attempt=claimed[0].attempt,
        )
        assert outcome == "scheduled", (
            f"the snooze returned {outcome!r} rather than scheduling the job; the "
            "survival contract below only means something for a job that was deferred."
        )

        await fleet.stop_pod("pod-1", graceful=True)
        await fleet.start_pod("pod-2")

        status = await _status_of(fleet, job_id)
        assert status == "scheduled", (
            f"a job snoozed by a pod that has since been replaced reads as {status!r}. "
            "The deferral did not survive the process that issued it, so the job is "
            "either gone or running early."
        )

        # Due time arrives. The surviving fleet, which never saw the
        # snooze happen, must be the one that wakes it.
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET scheduled_at = $1 WHERE id = $2 RETURNING id',
            datetime.now(UTC) - timedelta(seconds=1),
            job_id,
        )
        promoted = await fleet.pod("pod-2").backend.scheduled_to_pending()
        assert promoted >= 1, (
            f"the scheduled-to-pending sweep promoted {promoted} rows although a snoozed "
            "job is past due. A job whose due time has passed and which no sweep "
            "promotes waits forever, and its status says it is merely scheduled."
        )

        woken = await fleet.pod("pod-2").claim([_QUEUE], 1)
        assert [JobId(job.id) for job in woken] == [job_id], (
            "the pod that replaced the one which snoozed the job could not claim it once "
            "it came due. A deferred job must be claimable by whichever pod is alive "
            "when its time arrives, not only by the pod that deferred it."
        )


async def test_concurrent_pods_cannot_both_finish_the_same_job(pg_dsn: str) -> None:
    """Only the pod that holds a job's current claim may end it.

    This is the fencing contract seen from the fleet's side. A pod whose
    lease was reclaimed while it was working is, from the queue's point
    of view, no longer the owner: another pod may already be running the
    same job. Its terminal write must not land, or the job's recorded
    outcome becomes whichever pod happened to write last, and a failure
    from the stale pod can overwrite a success from the live one.
    """
    schema = f"fleet_fence_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("stale", "live"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        claimed = await fleet.pod("stale").claim([_QUEUE], 1)
        assert len(claimed) == 1

        # The stale pod's lease expires and the row is reclaimed and
        # re-claimed by a live pod, while the stale pod is still working.
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET lock_expires_at = $1 WHERE id = $2 RETURNING id',
            datetime.now(UTC) - timedelta(minutes=5),
            job_id,
        )
        await fleet.pod("live").backend.reclaim_expired_locks(
            timedelta(seconds=0), timedelta(seconds=0)
        )
        # Serve the reclaimed attempt's retry backoff on the row, so the
        # re-claim below happens for the reason under test rather than
        # waiting out a delay.
        await fleet.fetch(
            "UPDATE \"{schema}\".jobs SET scheduled_at = $1, status = 'pending' "
            "WHERE id = $2 RETURNING id",
            datetime.now(UTC) - timedelta(seconds=1),
            job_id,
        )
        retaken = await fleet.pod("live").claim([_QUEUE], 1)
        assert [JobId(job.id) for job in retaken] == [job_id], (
            "the live pod could not re-claim the reclaimed job, so the two-owner "
            "condition this test needs was never reached."
        )

        stale_write_landed = await fleet.pod("stale").backend.mark_succeeded(
            job_id, fleet.pod("stale").worker_id, attempt=claimed[0].attempt
        )
        assert stale_write_landed is False, (
            "a pod whose claim on this job was already reclaimed reported the job "
            "succeeded and the write was accepted. Another pod holds the job and is "
            "running it now: its result will be overwritten, or its success replaced by "
            "a stale pod's failure, with nothing in the job's history showing two owners."
        )

        status = await _status_of(fleet, job_id)
        assert status == "running", (
            f"the job reads as {status!r} after a stale pod's terminal write; the live "
            "pod still holds it and has not finished, so the queue is now reporting an "
            "outcome for work that is still in progress."
        )


@pytest.mark.slow
async def test_rolling_deploy_interrupts_running_jobs_and_the_fleet_finishes_them(
    pg_dsn: str,
) -> None:
    """A pod stopped with actors mid-flight hands every one back, held
       until the pod is gone, and finished by the fleet exactly once. The
       interrupted claims spend their attempt: the interrupt arm does not
    refund it: a refund re-creates the epoch a zombie
       handler holds).

       Eight jobs run on one pod, every actor slower than the grace periods —
       the ordinary shape of a rolling deploy under load. The deploy must not
       terminalise any of them (no ``cancelled``/``abandoned``/``crashed``),
       must not spend their budget (each completes on its first spent
       attempt), and must not let the surviving pod claim a held row before
       the departing pod is provably gone. The interruption is counted on
       each row, so a week of deploys is visible as deploys, not as the
       jobs' own failures.
    """
    schema = f"fleet_interrupt_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2"),
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        job_ids = await fleet.enqueue(8, actor=_ACTOR, queue=_QUEUE, max_attempts=3)

        departing = fleet.pod("pod-1")
        claimed = await departing.claim([_QUEUE], 8)
        assert len(claimed) == 8

        started: dict[str, asyncio.Event] = {
            str(row.payload["marker"]): asyncio.Event() for row in claimed
        }
        release_actors = asyncio.Event()
        first_pod_runs: list[str] = []

        async def slow_work(payload: FleetPayload, _ctx: object) -> object:
            """Longer than both graces; swallows the forced cancel and parks.

            This is not a misbehaving actor — it is the export/report/shape
            of work that simply cannot finish inside a deploy's grace
            window. The zombie's late return after the release must not
            move the released row (the attempt epoch it carries is stale).
            """
            started[payload.marker].set()
            first_pod_runs.append(payload.marker)
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(300)
            await release_actors.wait()
            return {"done": True}

        tasks = [
            departing.start(job, slow_work, actor_config=fleet_actor_config()) for job in claimed
        ]
        try:
            # Every actor is mid-flight before the deploy lands.
            await wait_for_condition(
                lambda: departing.deps.active_jobs.count() == 8,
                description="all 8 jobs registered as in-flight on the departing pod",
                timeout=10.0,
            )

            exit_code = await fleet.stop_pod("pod-1", graceful=True)
            assert exit_code == 0

            assert await fleet.rows_locked_by(departing.worker_id) == [], (
                "rows remain locked to a pod that has exited"
            )

            states = await fleet.job_states()
            assert all(states[jid] == "scheduled" for jid in job_ids), (
                "every interrupted row must be released behind the hold, not "
                f"terminalised or left running: {states}"
            )

            rows = await fleet.fetch(
                'SELECT id, attempt, interrupt_count, scheduled_at FROM "{schema}".jobs'
            )
            by_id = {row["id"]: row for row in rows}
            for jid in job_ids:
                assert by_id[jid]["attempt"] == 1, (
                    "the interrupt arm must not refund the attempt increment: "
                    "the attempt started executing, and a refund re-creates the "
                    "epoch a zombie handler holds, the interrupted "
                    "claim spends the attempt"
                )
                assert by_id[jid]["interrupt_count"] == 1
            assert all(by_id[jid]["scheduled_at"] > datetime.now(UTC) for jid in job_ids), (
                "held rows must not be due while the departing pod may still be alive"
            )

            # The surviving pod cannot claim held rows before the hold
            # elapses — never two runners for one row.
            survivor = fleet.pod("pod-2")
            early = await survivor.claim([_QUEUE], 8)
            assert early == [], "pod-2 claimed a row whose departing pod may still be running it"
        finally:
            # The interrupted actors' late writes race the released rows and
            # lose on the attempt fence: the interrupt arm did not refund,
            # so the re-claimed attempt's epoch is strictly higher. Join
            # them quietly.
            release_actors.set()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

        # The hold elapses (served on the row, as the suite's other deferral
        # choreography does, rather than by sleeping through it) and the
        # leader's promotion tick hands the rows to the fleet.
        await fleet.fetch(
            "UPDATE \"{schema}\".jobs SET scheduled_at = $1 WHERE status = 'scheduled'",
            datetime.now(UTC) - timedelta(seconds=1),
        )
        survivor = fleet.pod("pod-2")
        promoted = await survivor.backend.scheduled_to_pending()
        assert promoted == 8

        completed: list[str] = []

        claimed_back = await survivor.claim([_QUEUE], 8)
        assert {row.id for row in claimed_back} == set(job_ids), (
            "the surviving pod could not re-claim the interrupted jobs once their holds elapsed"
        )
        for row in claimed_back:
            assert row.attempt == 2, (
                "the re-claim must climb past the interrupted attempt's epoch "
                "(the interrupt arm does not refund,: the zombie "
                "handler's late write can then only lose the fence"
            )

        async def finish(payload: FleetPayload, _ctx: object) -> object:
            completed.append(payload.marker)
            return {"done": True}

        for row in claimed_back:
            outcome = await survivor.run(row, finish, actor_config=fleet_actor_config())
            assert outcome == "succeeded"

        states = await fleet.job_states()
        for jid in job_ids:
            assert states[jid] == "succeeded", (
                f"the interrupted job finished {states[jid]!r} — the fleet must "
                "complete what the deploy interrupted"
            )
        assert sorted(completed) == sorted(row.payload["marker"] for row in claimed_back), (
            "each interrupted job ran exactly once after the deploy"
        )
        assert sorted(first_pod_runs) == sorted(row.payload["marker"] for row in claimed), (
            "each interrupted job's first run was on the departed pod (the scenario's premise)"
        )


async def test_pods_stopping_together_leave_no_job_locked(pg_dsn: str) -> None:
    """A whole-fleet restart releases everything it was holding.

    A cluster-wide restart — a config rollout, a node pool replacement,
    a control-plane upgrade — stops every pod at once, each holding a
    full round. There is no surviving peer to reclaim for them during
    the window, so each pod's own shutdown must release its own work.

    What makes this different from stopping one pod is that the shutdowns
    overlap: each is re-pending rows while the others are doing the same.
    If the hand-back is not safe under that overlap, the fleet comes back
    up with rows locked to worker ids that no longer exist.
    """
    schema = f"fleet_restart_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2", "pod-3"),
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        await fleet.enqueue(30, actor=_ACTOR, queue=_QUEUE)

        names = ["pod-1", "pod-2", "pod-3"]
        await asyncio.gather(*(fleet.pod(name).claim([_QUEUE], 5) for name in names))
        departing = {name: fleet.pod(name).worker_id for name in names}

        # Every pod's shutdown runs at the same time, as a cluster-wide
        # restart does — not one after another.
        exit_codes = await asyncio.gather(*(fleet.stop_pod(name, graceful=True) for name in names))
        assert set(exit_codes) == {0}, (
            f"a simultaneous fleet restart produced exit codes {exit_codes}; a pod that "
            "cannot drain while its peers are draining will be force-killed by the "
            "orchestrator, taking its in-flight work with it."
        )

        await fleet.start_pod("pod-4")
        for name, worker_id in departing.items():
            stranded = await fleet.rows_locked_by(worker_id)
            assert stranded == [], (
                f"{len(stranded)} jobs remain locked to {name} after the whole fleet "
                "restarted together. Every pod that could have released them is gone, so "
                "they wait out a lock lease while the restarted fleet sits idle beside "
                "them."
            )

        states = await fleet.job_states()
        running = [job_id for job_id, status in states.items() if status == "running"]
        assert running == [], (
            f"{len(running)} jobs are marked running after every pod that claimed them "
            "has exited and a fresh pod has started. Queue depth under-reports the real "
            "backlog by exactly this many jobs."
        )
        assert not (_TERMINAL & set(states.values())), (
            "no job ran during this restart, so none should have reached a terminal "
            f"state; found {sorted(_TERMINAL & set(states.values()))}."
        )
