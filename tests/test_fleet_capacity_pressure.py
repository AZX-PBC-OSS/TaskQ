"""Work denied a slot waits for one; it does not die of waiting.

Capacity limits are shared across the fleet - a rate limit of ten per
second is ten for the deployment, not ten per pod - so a pod that wants
to run a job routinely finds the capacity already spent by a peer. The
same shape arrives from outside: an upstream answering 429 means come
back later, not give up.

The contract that makes this safe is that being denied a slot is not a
failure of the job. A denial must not spend a retry, must not write an
attempt row that reads as a failed run, and must never by itself end a
job. Otherwise a queue that is merely busy consumes its own work: jobs
that would have succeeded the moment capacity freed are written off as
crashed, and the busier the fleet, the more of them. The bound on
waiting is the job's own schedule-to-close deadline, which is the one an
operator set deliberately.

A job deferred this way must also come back on time, from whichever pod
is alive when its moment arrives - the deferral is a row in the
database precisely so it does not belong to the process that issued it.

The saturation denial currently spends a retry and can end a job that
never ran; the tests below state the behaviour the queue is meant to
have, so they fail until it does. They are not describing today's
arithmetic and must not be adjusted to match it - a denial that costs
budget is the defect, not the specification.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_cap_q"
_ACTOR = "fleet_cap_actor"

# One attempt of headroom is all it takes: if a denial spends budget, a
# job with two attempts is terminal after two denials, which a busy
# fleet reaches in seconds.
_MAX_ATTEMPTS = 2


async def _row(fleet: Fleet, job_id: JobId) -> dict[str, object]:
    rows = await fleet.fetch(
        'SELECT status, attempt, scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
    )
    return dict(rows[0])


async def _attempt_rows(fleet: Fleet, job_id: JobId) -> int:
    rows = await fleet.fetch(
        'SELECT count(*) AS n FROM "{schema}".job_attempts WHERE job_id = $1', job_id
    )
    return int(rows[0]["n"])


@pytest.mark.parametrize(
    "outcome",
    ["rate_limit_denied", "reservation_denied"],
)
async def test_a_denied_job_spends_no_retry_budget(pg_dsn: str, outcome: str) -> None:
    """Being refused a slot must cost the job nothing.

    Both admission gates are pinned: a rate limit the fleet shares and a
    keyed reservation another pod holds. In each case the pod claims the
    job, finds no capacity, and puts it back.

    The job's attempt counter must read exactly what it read before the
    claim. If a denial leaves the counter raised, the job's remaining
    retries are being spent on contention rather than on failures, and a
    job that has never run and never failed marches toward crashed at
    the rate the fleet is busy.
    """
    schema = f"fleet_denial_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE, max_attempts=_MAX_ATTEMPTS)
        before = await _row(fleet, job_id)

        claimed = await fleet.pod("pod-1").claim([_QUEUE], 1)
        assert len(claimed) == 1

        result = await fleet.pod("pod-1").backend.mark_snoozed(
            job_id,
            fleet.pod("pod-1").worker_id,
            timedelta(seconds=30),
            outcome=outcome,  # pyright: ignore[reportArgumentType]  # Why: the parametrize ids are exactly the SnoozeOutcome denial literals.
            attempt=claimed[0].attempt,
            claim_epoch=claimed[0].claim_epoch,
        )
        assert result == "scheduled", (
            f"a {outcome} returned {result!r} instead of rescheduling the job. A job "
            "refused capacity must be put back for later, not resolved."
        )

        after = await _row(fleet, job_id)
        assert after["attempt"] == before["attempt"], (
            f"a {outcome} moved the job's attempt counter from {before['attempt']} to "
            f"{after['attempt']}. The job never ran - the fleet was busy - yet it "
            f"has one fewer of its {_MAX_ATTEMPTS} retries left. A queue under load now "
            "consumes its own backlog: jobs that would succeed the moment capacity frees "
            "are written off instead, and the busier the fleet the more of them."
        )
        assert after["status"] == "scheduled", (
            f"a denied job reads as {after['status']!r}; it must be waiting for capacity."
        )


async def test_repeated_denials_never_end_a_job(pg_dsn: str) -> None:
    """A job can be refused more times than it has retries and still run.

    This is the property the word 'indefinite' means operationally. A
    keyed limit held by a busy peer can refuse the same job far more
    often than its retry budget would allow if denials counted - the
    fleet may be saturated for hours. The job must survive all of it and
    then run when a slot appears.

    A fleet that fails this has a queue whose maximum contention is
    capped by ``max_attempts``: exceed it and work is destroyed, with
    the job's history showing attempts that never happened.
    """
    schema = f"fleet_indef_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE, max_attempts=_MAX_ATTEMPTS)

        denials = _MAX_ATTEMPTS * 4
        for round_index in range(denials):
            claimed = await fleet.pod("pod-1").claim([_QUEUE], 1)
            assert len(claimed) == 1, (
                f"denial round {round_index}: the job was not claimable. After "
                f"{round_index} denials it must still be queued and due, because a "
                "denial defers work rather than resolving it."
            )
            result = await fleet.pod("pod-1").backend.mark_snoozed(
                job_id,
                fleet.pod("pod-1").worker_id,
                timedelta(seconds=30),
                outcome="rate_limit_denied",
                attempt=claimed[0].attempt,
                claim_epoch=claimed[0].claim_epoch,
            )
            assert result == "scheduled", (
                f"denial round {round_index} returned {result!r}. After "
                f"{round_index + 1} denials against a budget of {_MAX_ATTEMPTS} "
                "attempts, contention has become fatal to the job."
            )
            # Capacity is still gone when the job comes back; bring its
            # due time forward rather than waiting out the backoff.
            await fleet.fetch(
                "UPDATE \"{schema}\".jobs SET scheduled_at = $1, status = 'pending' "
                "WHERE id = $2 RETURNING id",
                datetime.now(UTC) - timedelta(seconds=1),
                job_id,
            )

        # Capacity frees. The job must run, normally, first attempt.
        final = await fleet.pod("pod-1").claim([_QUEUE], 1)
        assert len(final) == 1

        async def _work(_payload: FleetPayload, _ctx: object) -> str:
            return "done"

        await fleet.pod("pod-1").run(
            final[0], _work, actor_config=fleet_actor_config(max_attempts=_MAX_ATTEMPTS)
        )

        row = await _row(fleet, job_id)
        assert row["status"] == "succeeded", (
            f"after {denials} capacity denials and one real run the job reads as "
            f"{row['status']!r}. Work that was only ever waiting for a slot must "
            "complete once it gets one."
        )


async def test_a_denial_writes_no_failed_attempt_history(pg_dsn: str) -> None:
    """Contention must not read as a failed run in the job's history.

    An attempt row is the record of the actor having executed. A denial
    means it did not. If denials write attempt rows, every dashboard and
    every investigation that counts attempts is wrong in the same
    direction - a busy period looks like a wave of failures, and the
    operator debugging it is looking for a bug in code that never ran.
    """
    schema = f"fleet_denial_hist_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        claimed = await fleet.pod("pod-1").claim([_QUEUE], 1)
        await fleet.pod("pod-1").backend.mark_snoozed(
            job_id,
            fleet.pod("pod-1").worker_id,
            timedelta(seconds=30),
            outcome="rate_limit_denied",
            attempt=claimed[0].attempt,
            claim_epoch=claimed[0].claim_epoch,
        )

        written = await _attempt_rows(fleet, job_id)
        assert written == 0, (
            f"a capacity denial wrote {written} attempt row(s) for a job whose actor "
            "never ran. Every count of attempts - the job's own history, failure rates, "
            "retry dashboards - now reports contention as execution, so a busy fleet is "
            "indistinguishable from a broken one."
        )


async def test_capacity_denied_work_is_picked_up_by_a_different_pod(
    pg_dsn: str,
) -> None:
    """A deferral belongs to the fleet, not to the pod that issued it.

    The pod that hit the limit may well be gone by the time capacity
    frees - that is the normal case during a deploy, and the reason the
    deferral is a row rather than a timer. Whichever pod is alive when
    the job comes due must be able to run it.

    If deferred work could only be resumed by its original pod, every
    deploy would strand whatever was waiting on capacity at that moment,
    silently: the rows look scheduled, which is exactly what a healthy
    waiting job looks like.
    """
    schema = f"fleet_denial_pod_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("busy",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)

        claimed = await fleet.pod("busy").claim([_QUEUE], 1)
        await fleet.pod("busy").backend.mark_snoozed(
            job_id,
            fleet.pod("busy").worker_id,
            timedelta(seconds=30),
            outcome="reservation_denied",
            attempt=claimed[0].attempt,
            claim_epoch=claimed[0].claim_epoch,
        )

        # The pod that was refused capacity is replaced, as a deploy
        # would replace it, while the job is still waiting.
        await fleet.stop_pod("busy", graceful=True)
        fresh = await fleet.start_pod("fresh")

        await fleet.fetch(
            'UPDATE "{schema}".jobs SET scheduled_at = $1 WHERE id = $2 RETURNING id',
            datetime.now(UTC) - timedelta(seconds=1),
            job_id,
        )
        promoted = await fresh.backend.scheduled_to_pending()
        assert promoted >= 1, (
            f"the due-work sweep promoted {promoted} rows although a capacity-deferred "
            "job is past due. Work waiting on capacity that no sweep wakes waits for "
            "ever, and its status says only that it is scheduled."
        )

        picked_up = await fresh.claim([_QUEUE], 1)
        assert [JobId(job.id) for job in picked_up] == [job_id], (
            "a job deferred for capacity by a pod that has since been replaced was not "
            "claimable by the pod that replaced it. Every deploy would strand whatever "
            "the fleet was waiting to retry, with nothing to distinguish those rows from "
            "jobs legitimately waiting their turn."
        )


async def test_future_scheduled_work_is_not_claimable_before_its_time(
    pg_dsn: str,
) -> None:
    """A job scheduled for later must not run early, on any pod.

    Deferral is only trustworthy if it holds against every pod in the
    fleet, not just the one that set it. A rate-limited job released
    early defeats the limit it was deferred for; a business-scheduled
    job released early does something before it was supposed to happen,
    which is usually not recoverable by retrying.
    """
    schema = f"fleet_future_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("a", "b"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        await fleet.fetch(
            "UPDATE \"{schema}\".jobs SET status = 'scheduled', scheduled_at = $1 "
            "WHERE id = $2 RETURNING id",
            datetime.now(UTC) + timedelta(hours=1),
            job_id,
        )

        for name in ("a", "b"):
            early = await fleet.pod(name).claim([_QUEUE], 5)
            assert early == [], (
                f"pod {name} claimed a job scheduled an hour into the future. Whatever "
                "the delay was protecting - a rate limit, an external dependency, a "
                "business time - has been defeated, and the work has already happened by "
                "the time anyone notices."
            )

        promoted = await fleet.pod("a").backend.scheduled_to_pending()
        assert promoted == 0, (
            f"the due-work sweep promoted {promoted} rows although nothing is due. Work "
            "promoted early is work released early."
        )
