"""A pod told to reload keeps its work, and keeps working.

Credential rotation is routine: a short-lived database token expires, a
secret is rotated, an operator sends SIGHUP rather than restarting the
fleet. The whole point of reloading in place is that it is cheaper and
safer than a restart - so it has to be, which means the pod's claims
survive it and the pod goes on claiming afterwards.

A reload that quietly loses in-flight work is worse than a restart,
because nothing about it looks like a disruption: there is no pod
replacement in the deployment history, no exit code, nothing for an
operator to correlate the stuck jobs with. It is the kind of event that
gets ruled out early in an investigation precisely because it was
supposed to be transparent.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from taskq.worker.deps import reload_credentials
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_reload_q"
_ACTOR = "fleet_reload_actor"


async def _status_of(fleet: Fleet, job_id: JobId) -> str:
    rows = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(rows[0]["status"])


async def test_a_reload_does_not_release_the_jobs_a_pod_holds(pg_dsn: str) -> None:
    """Rotating credentials must not hand this pod's work to anyone else.

    The pod holds a full round of claims when the reload happens. A
    reload replaces the pod's connection pools; it does not replace the
    pod, so its identity and its claims are unchanged and the rows stay
    locked to it.

    If a reload released them, the same job would be running on this pod
    and claimable by every other pod at once - a duplicate run triggered
    by nothing more than a routine secret rotation.
    """
    schema = f"fleet_reload_hold_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1", "pod-2"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(10, actor=_ACTOR, queue=_QUEUE)

        pod = fleet.pod("pod-1")
        claimed = await pod.claim([_QUEUE], 5)
        assert len(claimed) == 5
        held = {JobId(job.id) for job in claimed}

        await reload_credentials(pod.deps)

        still_held = set(await fleet.rows_locked_by(pod.worker_id))
        assert still_held == held, (
            f"a credential reload changed which jobs this pod holds: {len(held)} before, "
            f"{len(still_held)} after. The pod is still running and still believes it "
            "owns them, so any row it lost is now claimable by a peer while this pod "
            "works on it."
        )

        peer_round = await fleet.pod("pod-2").claim([_QUEUE], 10)
        double_claimed = held & {JobId(job.id) for job in peer_round}
        assert double_claimed == set(), (
            f"after this pod reloaded, a peer claimed {len(double_claimed)} of the jobs "
            "it still holds. A routine secret rotation has produced a duplicate run, "
            "with nothing in the deployment history to correlate it with."
        )


async def test_a_pod_keeps_claiming_and_completing_after_a_reload(
    pg_dsn: str,
) -> None:
    """A reloaded pod is a working pod, not a surviving one.

    Swapping pools out from under a running worker has to leave it able
    to do everything it did before: claim new work and write results
    through the replacement pools. A pod that survives a reload but can
    no longer claim is worse than one that crashed - it stays in the
    fleet, reports itself healthy, and does nothing.
    """
    schema = f"fleet_reload_work_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(6, actor=_ACTOR, queue=_QUEUE)
        pod = fleet.pod("pod-1")

        await reload_credentials(pod.deps)

        claimed = await pod.claim([_QUEUE], 3)
        assert len(claimed) == 3, (
            f"a reloaded pod claimed {len(claimed)} of 3 jobs from a backlog of 6. It is "
            "still in the fleet and still reporting healthy, but it has stopped taking "
            "work - the quietest way for a fleet to lose a pod's capacity."
        )

        async def _work(_payload: FleetPayload, _ctx: object) -> str:
            return "done"

        for job in claimed:
            await pod.run(job, _work, actor_config=fleet_actor_config())

        for job in claimed:
            status = await _status_of(fleet, JobId(job.id))
            assert status == "succeeded", (
                f"a job run after a credential reload reads as {status!r}. The pod can "
                "claim through its new pools but cannot record outcomes through them, so "
                "its work is repeated by whichever pod reclaims the lease."
            )


async def test_a_reload_request_is_observable_to_the_pod(pg_dsn: str) -> None:
    """The reload signal reaches the pod that must act on it.

    SIGHUP sets an event the worker's reload coordinator consumes; the
    programmatic trigger is the same path, and is what an embedder or a
    platform without SIGHUP uses. A request that does not raise the flag
    is a rotation that silently never happens - and the failure surfaces
    later, as authentication errors from a pod still using a credential
    everyone believes was replaced.
    """
    schema = f"fleet_reload_sig_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        pod = fleet.pod("pod-1")
        assert not pod.deps.reload_event.is_set(), (
            "the pod believes a reload is pending before one was asked for."
        )

        pod.deps.request_reload()

        assert pod.deps.reload_event.is_set(), (
            "asking a pod to reload left it unaware of the request. The rotation never "
            "happens, and the first symptom is authentication failures from a pod still "
            "presenting a credential that was supposed to have been replaced."
        )
