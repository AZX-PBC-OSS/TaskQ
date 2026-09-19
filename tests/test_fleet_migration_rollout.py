"""Both releases keep working while a migration rolls through the fleet.

A schema change reaches a running deployment in stages. The pre-phase
migrations are applied while the old release is still serving every pod;
the new release then rolls out pod by pod, so for the length of the roll
two code versions run against one schema; the post-phase migrations are
applied only once no old pod remains. During that window nothing may
stop: the queue is live, and a rollout that pauses dispatch or breaks
enqueue is an outage regardless of how briefly it lasts.

The window is also where a rollout becomes unrecoverable. If an
un-upgraded pod cannot dispatch against the part-migrated schema, the
fleet's throughput falls to whatever fraction has been upgraded, and
rolling *back* does not help - the schema has already moved. What makes
a rollout safe is that a pod of either release, at any point in the
sequence, can still claim work, finish it, retry it and reclaim it.

These tests hold the schema in the real intermediate state - pre-phase
applied, post-phase deliberately pending - and require the full working
cycle from pods running against it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from taskq.migrate import apply_pending
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_migrate_q"
_ACTOR = "fleet_migrate_actor"


async def _pending_phases(dsn: str, schema: str) -> list[str]:
    """Which migration phases are still unapplied on this schema."""
    conn = await asyncpg.connect(dsn)
    try:
        pending = await apply_pending(conn, schema=schema, max_steps=0)
    finally:
        await conn.close()
    return [migration.phase for migration in pending]


async def _apply_pre_phase_only(dsn: str, schema: str) -> None:
    """Put the schema in the state a rollout actually passes through.

    Pre-phase migrations are applied ahead of the new code; post-phase
    ones wait until the last old pod is gone. Between those two points
    the schema is neither version, and that is the state every pod in a
    rolling deploy is running against.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
    finally:
        await conn.close()


async def _status_of(fleet: Fleet, job_id: JobId) -> str:
    rows = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(rows[0]["status"])


async def test_the_rollout_window_is_a_real_intermediate_schema_state(
    pg_dsn: str,
) -> None:
    """The state the other tests run against is genuinely part-migrated.

    A rollout test asserting behaviour against a fully migrated schema
    would prove nothing while looking convincing, so the intermediate
    state is established as a fact first: pre-phase applied, post-phase
    still pending. If this ever stops being true - because every
    migration became pre-phase, say - the tests below would silently
    become ordinary same-schema tests, and this one says so instead.
    """
    schema = f"fleet_mig_state_{new_base62()}".lower()
    await _apply_pre_phase_only(pg_dsn, schema)
    try:
        pending = await _pending_phases(pg_dsn, schema)
        assert pending, (
            "applying only the pre-phase migrations left nothing pending, so the schema "
            "is fully migrated and the rollout-window tests in this module are no longer "
            "exercising a part-migrated schema at all."
        )
        assert set(pending) == {"post"}, (
            f"after applying the pre phase the pending set is {sorted(set(pending))}; the "
            "rollout window is defined by post-phase work outstanding, and a pending pre "
            "migration means the schema is in a state no correct rollout produces."
        )
    finally:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()


async def test_pods_keep_dispatching_and_completing_mid_rollout(
    pg_dsn: str,
) -> None:
    """The queue stays live while the migration is half applied.

    Two pods claim and complete work against the part-migrated schema.
    This is the plain availability contract of a rolling deploy: work
    submitted during the roll is worked during the roll.

    A failure here is an outage that lasts as long as the rollout and
    cannot be rolled back out of, because the schema has already moved.
    """
    schema = f"fleet_mig_live_{new_base62()}".lower()
    await _apply_pre_phase_only(pg_dsn, schema)
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("old-release", "new-release"),
        actors=((_ACTOR, _QUEUE),),
        migrate=False,
    ) as fleet:
        job_ids = await fleet.enqueue(8, actor=_ACTOR, queue=_QUEUE)

        async def _work(_payload: FleetPayload, _ctx: object) -> str:
            return "done"

        completed = 0
        for name in ("old-release", "new-release"):
            claimed = await fleet.pod(name).claim([_QUEUE], 4)
            assert claimed, (
                f"pod {name!r} claimed nothing from a backlog of 8 during the rollout "
                "window. Its share of the fleet's capacity is doing nothing, and since "
                "the schema has already moved forward, rolling the release back does not "
                "restore it."
            )
            for job in claimed:
                await fleet.pod(name).run(job, _work, actor_config=fleet_actor_config())
                completed += 1

        assert completed == len(job_ids), (
            f"{completed} of {len(job_ids)} jobs completed during the rollout window; "
            "work submitted while a migration rolls through must still be worked."
        )
        for job_id in job_ids:
            status = await _status_of(fleet, job_id)
            assert status == "succeeded", (
                f"job {job_id} reads as {status!r} after running to completion against "
                "the part-migrated schema."
            )


async def test_retry_and_reclaim_still_work_mid_rollout(pg_dsn: str) -> None:
    """Recovery paths must survive the rollout window too.

    Dispatch and completion are the paths a smoke test would exercise;
    the ones that matter during a deploy are retry and reclaim, because
    a rolling deploy is exactly when jobs get interrupted and pods
    disappear. If reclaim is what breaks under the part-migrated schema,
    the symptom is work stranded by the deploy that only the deploy
    could have stranded - and it accumulates silently for the length of
    the roll.
    """
    schema = f"fleet_mig_recover_{new_base62()}".lower()
    await _apply_pre_phase_only(pg_dsn, schema)
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("doomed", "survivor"),
        actors=((_ACTOR, _QUEUE),),
        migrate=False,
    ) as fleet:
        (retry_job,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE, max_attempts=3)
        claimed = await fleet.pod("survivor").claim([_QUEUE], 1)
        assert len(claimed) == 1

        # A failing attempt must still be scheduled for retry.
        async def _fail(_payload: FleetPayload, _ctx: object) -> None:
            raise RuntimeError("actor failed during the rollout window")

        await fleet.pod("survivor").run(claimed[0], _fail, actor_config=fleet_actor_config())
        status = await _status_of(fleet, retry_job)
        assert status in {"pending", "scheduled"}, (
            f"a job that failed mid-rollout reads as {status!r} instead of being queued "
            "for another attempt. Retries have stopped working for the duration of the "
            "deploy, so ordinary transient failures become permanent ones."
        )

        # A pod lost mid-rollout must still have its work reclaimed.
        (orphan,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        await fleet.fetch(
            "UPDATE \"{schema}\".jobs SET scheduled_at = $1, status = 'pending' "
            "WHERE id = $2 RETURNING id",
            datetime.now(UTC) - timedelta(seconds=1),
            orphan,
        )
        held = await fleet.pod("doomed").claim([_QUEUE], 1)
        assert [JobId(job.id) for job in held] == [orphan]
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET lock_expires_at = $1 WHERE id = $2 RETURNING id',
            datetime.now(UTC) - timedelta(minutes=5),
            orphan,
        )
        await fleet.stop_pod("doomed", graceful=False)

        reclaimed = await fleet.pod("survivor").backend.reclaim_expired_locks(
            timedelta(seconds=0), timedelta(seconds=0)
        )
        assert reclaimed >= 1, (
            "a job orphaned by a pod lost during the rollout window was not reclaimed. "
            "Reclaim is the only route back for work whose owner died, and a deploy is "
            "precisely when owners die - so this strands work for the length of the roll "
            "and reports nothing."
        )


async def test_enqueue_keeps_working_for_every_pod_mid_rollout(
    pg_dsn: str,
) -> None:
    """Producers must not be broken by a partly applied schema.

    Enqueue is the path with the widest blast radius: it is called from
    application code across the whole deployment, not just from workers.
    An enqueue that fails against the part-migrated schema takes down
    request handling in every un-upgraded service, which is a far larger
    incident than a slow queue.
    """
    schema = f"fleet_mig_enqueue_{new_base62()}".lower()
    await _apply_pre_phase_only(pg_dsn, schema)
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("old-release", "new-release"),
        actors=((_ACTOR, _QUEUE),),
        migrate=False,
    ) as fleet:
        for name in ("old-release", "new-release"):
            pod = fleet.pod(name)
            before = await fleet.fetch('SELECT count(*) AS n FROM "{schema}".jobs')
            await fleet.enqueue(3, actor=_ACTOR, queue=_QUEUE)
            after = await fleet.fetch('SELECT count(*) AS n FROM "{schema}".jobs')
            assert int(after[0]["n"]) == int(before[0]["n"]) + 3, (
                f"enqueue through pod {name!r} did not persist its jobs during the "
                "rollout window. Every service that submits work is affected, not just "
                "the queue: an enqueue failure here is a request-path outage."
            )
            claimed = await pod.claim([_QUEUE], 3)
            assert claimed, (
                f"jobs enqueued during the rollout window were not claimable by pod "
                f"{name!r}: they persist but never dispatch, which is the worst shape - "
                "the backlog grows and nothing reports an error."
            )
