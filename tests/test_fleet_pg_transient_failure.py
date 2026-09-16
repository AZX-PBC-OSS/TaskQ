"""A database interruption must cost the fleet time, not work.

Postgres goes away and comes back as a matter of routine: a minor-version
patch, a failover to a replica, a managed-service maintenance window, a
connection reset by something in the network path. Every one of those closes
the connections a worker is holding, and none of them is a reason for a job to
be lost, duplicated, or silently stranded.

The interruption is delivered the way the database delivers it — the server
terminates the worker's backends, which is what a restart or failover does to a
client that is holding connections. The worker finds out at its next statement,
in whatever call happens to be in flight.

Three things are pinned, in the order an operator cares about them:

**No work is destroyed.** A job in flight across the interruption is still the
fleet's to run afterwards. It may be retried, reclaimed, or completed, but it
does not end up in a terminal state that no longer re-dispatches, and it does
not disappear.

**The worker recovers by itself.** After the interruption the pod can claim and
complete work again without being restarted. A worker that needs a restart to
survive a routine database patch turns every maintenance window into an
operator's evening.

**Nothing is double-counted.** A job that completed once stays completed once.
An interruption between an attempt's work and its terminal write must not
produce two executions or two attempt identities.

What is deliberately not pinned is the timing or the path: whether a job comes
back through a retry, through the reclaim sweep, or through its original
attempt finishing is the implementation's business. Where a job ends up is not.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobFilter, JobId
from taskq.context import JobContext
from taskq.testing.assertions import wait_for_condition
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_QUEUE = "fleet_pgfail_q"
_ACTOR = "fleet_pgfail_actor"

#: States that end a job's life without another dispatch. A job that crosses a
#: database interruption must not be in one of these unless it genuinely ran.
_TERMINAL_WITHOUT_RETRY = frozenset({"failed", "crashed", "abandoned", "cancelled"})


async def _status_of(fleet: Fleet, job_id: JobId) -> str:
    rows = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(rows[0]["status"])


async def _interrupt_database(dsn: str) -> int:
    """Terminate every other session on this database.

    This is what a restart, a failover, or a maintenance window does to a
    client holding connections: the backends go away and the client discovers
    it at its next statement. Returns how many sessions were ended, so a test
    can assert the interruption actually happened rather than passing because
    nothing was hit.

    pg_stat_activity is cluster-wide and the container hosts every xdist
    worker's database, so the scope comes from the terminating connection's
    own database rather than a name passed in.
    """
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )
        return len(rows)
    finally:
        await conn.close()


async def test_a_pod_keeps_working_after_the_database_drops_its_connections(
    pg_dsn: str,
) -> None:
    """A pod claims and completes work again after an interruption.

    Nothing restarts the pod: it is the same process, with the same pools, that
    was holding connections when the database dropped them. If it cannot
    recover on its own, every routine patch or failover becomes a fleet-wide
    outage that only a rolling restart clears.
    """
    schema = f"fleet_pgfail_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        dsn = str(fleet.settings.pg_dsn)
        pod = fleet.pod("pod-1")

        # A completed job before the interruption, so the comparison afterwards
        # is against a pod that was demonstrably working.
        before_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        claimed = await pod.claim([_QUEUE], 1)
        assert len(claimed) == 1

        async def work(_payload: FleetPayload, _ctx: JobContext[FleetPayload]) -> object:
            return {"ok": True}

        await pod.run(claimed[0], work, actor_config=fleet_actor_config())
        assert await _status_of(fleet, before_ids[0]) == "succeeded"

        terminated = await _interrupt_database(dsn)
        assert terminated > 0, (
            "the scenario requires the database to have actually dropped the "
            "pod's connections; no sessions were terminated"
        )

        # Enqueueing is the first thing a caller does after an interruption,
        # and it is the seam where a pool hands out a connection the server has
        # already closed. It may fail once, but it must fail as a recognisable
        # database error that a caller can retry — not as an internal driver
        # state error, which tells the caller nothing and matches no except
        # clause written against the database's own error types.
        after_ids: list[JobId] = []

        async def enqueue_recovers() -> bool:
            try:
                after_ids.extend(await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE))
            except asyncpg.InternalClientError as exc:
                raise AssertionError(
                    "enqueueing after the database dropped its connections "
                    f"raised an internal driver state error ({exc}), not a "
                    "database error. A caller cannot distinguish this from a "
                    "bug in its own code, it matches no handler written against "
                    "asyncpg's error types, and the pooled connection that "
                    "produced it was handed out while still mid-operation"
                ) from exc
            except (asyncpg.PostgresConnectionError, OSError, asyncpg.InterfaceError):
                # The recognisable shape: the caller can retry this.
                return False
            return True

        await wait_for_condition(
            enqueue_recovers,
            description="enqueueing succeeded again after the interruption",
            timeout=30.0,
        )

        # The pod is given the chance to notice and rebuild, the way it would
        # between polls; what is pinned is that it does so on its own.
        claimed_after: list[object] = []

        async def can_claim_again() -> bool:
            try:
                rows = await pod.claim([_QUEUE], 1)
            except Exception:
                # A statement issued on a connection the server has already
                # closed fails once; the contract is that the pod recovers,
                # not that no call ever sees the interruption.
                return False
            if rows:
                claimed_after.extend(rows)
                return True
            return False

        await wait_for_condition(
            can_claim_again,
            description=(
                "the pod claimed work again after the database dropped its "
                "connections, without being restarted"
            ),
            timeout=30.0,
        )

        job = claimed_after[0]
        await pod.run(job, work, actor_config=fleet_actor_config())  # pyright: ignore[reportArgumentType]  # Why: claim returns JobRow; the list is untyped only because it is filled inside the polled predicate
        assert await _status_of(fleet, after_ids[0]) == "succeeded", (
            "the pod could claim after the interruption but could not complete "
            "the work, so jobs are picked up and then stranded"
        )


async def test_a_job_in_flight_across_an_interruption_is_not_destroyed(
    pg_dsn: str,
) -> None:
    """A job being worked when the database drops is still the fleet's to run.

    The actor is mid-attempt when every connection is terminated, so the
    attempt's terminal write has nowhere to land. The job must not be left in a
    state that never re-dispatches: a database blip is not a verdict on the
    work, and nothing about it alerts anyone if the row is quietly terminal.
    """
    schema = f"fleet_pgfail_inflight_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2"),
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        dsn = str(fleet.settings.pg_dsn)
        pod = fleet.pod("pod-1")

        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE, max_attempts=3)
        job_id = job_ids[0]
        claimed = await pod.claim([_QUEUE], 1)
        assert len(claimed) == 1, (
            "the scenario requires the pod to be holding the job when the database goes away"
        )

        started = asyncio.Event()
        release = asyncio.Event()

        async def interrupted_work(
            _payload: FleetPayload, _ctx: JobContext[FleetPayload]
        ) -> object:
            started.set()
            await release.wait()
            return {"ok": True}

        attempt = asyncio.create_task(
            pod.run(claimed[0], interrupted_work, actor_config=fleet_actor_config())
        )
        await asyncio.wait_for(started.wait(), timeout=5.0)

        terminated = await _interrupt_database(dsn)
        assert terminated > 0, (
            "the scenario requires the database to have dropped the pod's connections mid-attempt"
        )

        release.set()
        try:
            await asyncio.wait_for(attempt, timeout=30.0)
        except (TimeoutError, Exception):
            # The attempt's own outcome is not the contract — its terminal
            # write had nowhere to land. Where the job ends up is.
            attempt.cancel()

        status = await _status_of(fleet, job_id)
        assert status not in _TERMINAL_WITHOUT_RETRY, (
            f"a job that was mid-attempt when the database dropped its "
            f"connections was left {status!r}, a state that never re-dispatches. "
            f"The work was not tried and found wanting — a routine restart, "
            f"failover or maintenance window destroyed it, and nothing alerts "
            f"because nothing failed"
        )

        rows = await fleet.fetch('SELECT count(*) AS n FROM "{schema}".jobs WHERE id = $1', job_id)
        assert int(rows[0]["n"]) == 1, (
            "the job's row must still exist after a database interruption"
        )


async def test_an_interruption_does_not_duplicate_a_completed_job(
    pg_dsn: str,
) -> None:
    """Work that finished once stays finished once.

    The dangerous direction of a recovery path is running the job again: a
    surviving pod reclaiming a row whose work already happened. Attempt
    identities are the audit an operator reads, so they must not repeat either.
    """
    schema = f"fleet_pgfail_dup_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2"),
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        dsn = str(fleet.settings.pg_dsn)
        pod = fleet.pod("pod-1")

        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        job_id = job_ids[0]
        claimed = await pod.claim([_QUEUE], 1)
        assert len(claimed) == 1

        runs = 0

        async def counted_work(_payload: FleetPayload, _ctx: JobContext[FleetPayload]) -> object:
            nonlocal runs
            runs += 1
            return {"ok": True}

        await pod.run(claimed[0], counted_work, actor_config=fleet_actor_config())
        assert await _status_of(fleet, job_id) == "succeeded"
        assert runs == 1

        terminated = await _interrupt_database(dsn)
        assert terminated > 0, "the scenario requires the database to have dropped every session"

        # Everything the fleet does on its own to find orphaned work.
        survivor = fleet.pod("pod-2")

        last_sweep_error: list[BaseException] = []

        async def recovery_settles() -> bool:
            try:
                await survivor.backend.reclaim_expired_locks(
                    timedelta(seconds=0), timedelta(seconds=0)
                )
            except Exception as exc:  # Why: the sweep's own failure is the observable being polled; the assertion below reports it rather than losing it
                last_sweep_error.clear()
                last_sweep_error.append(exc)
                return False
            return True

        try:
            await wait_for_condition(
                recovery_settles,
                description=("the surviving pod ran a reclaim sweep after the interruption"),
                timeout=30.0,
            )
        except AssertionError as exc:
            detail = (
                f" Last failure: {type(last_sweep_error[0]).__name__}: {last_sweep_error[0]}"
                if last_sweep_error
                else ""
            )
            raise AssertionError(
                "the maintenance sweep never recovered after the database "
                "dropped its connections. Reclaim is how the fleet finds work "
                "orphaned by the very interruption that just happened, so a "
                "sweep that stays broken means orphaned jobs are never "
                f"recovered.{detail}"
            ) from exc

        assert await _status_of(fleet, job_id) == "succeeded", (
            "a job that completed before the interruption is no longer recorded "
            "as succeeded; recovery has reopened finished work, so it will run "
            "a second time"
        )

        attempts = await fleet.fetch(
            'SELECT attempt FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
            job_id,
        )
        numbers = [int(row["attempt"]) for row in attempts]
        assert len(numbers) == len(set(numbers)), (
            f"an attempt identity was recorded twice across the interruption: "
            f"{numbers}. The audit trail an operator reads now claims the same "
            f"attempt ran more than once"
        )
        assert runs == 1, (
            f"the actor ran {runs} times for one job across a database "
            f"interruption; work that completed must not be executed again"
        )


async def test_bulk_cancel_recovers_typed_after_the_database_drops_its_connections(
    pg_dsn: str,
) -> None:
    """``cancel_where`` must not hand the caller a raw driver error either.

    ``_enqueue.py``'s pool-acquire callers are wrapped in
    ``_with_fresh_connection_retry`` (a poisoned connection's first
    statement fails locally with ``asyncpg.InternalClientError`` right
    after a server-side interruption, before ``connection_lost`` has run
    and marked it closed) so the caller sees one clean retry instead of an
    error outside asyncpg's own hierarchy. ``_cancel_bulk.py``'s
    ``_drain_cancel_batches`` acquires from the very same pool with the
    very same ``async with pool.acquire() as conn: async with
    conn.transaction(): ...`` shape, but its retry loop only catches
    ``asyncpg.DeadlockDetectedError`` -- nothing there recognises
    ``InternalClientError``. If a bulk cancel is the first call to reach
    the pool after an interruption, this pins whether the same untyped
    escape the enqueue path was fixed for still reaches this caller.
    """
    schema = f"fleet_pgfail_cancel_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        dsn = str(fleet.settings.pg_dsn)
        pod = fleet.pod("pod-1")

        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        assert len(job_ids) == 1

        terminated = await _interrupt_database(dsn)
        assert terminated > 0, (
            "the scenario requires the database to have actually dropped the "
            "pod's connections; no sessions were terminated"
        )

        try:
            await pod.backend.cancel_where(JobFilter(active=True), reason="fleet interruption drill")
        except asyncpg.InternalClientError as exc:
            raise AssertionError(
                "cancel_where after the database dropped its connections raised "
                f"an internal driver state error ({exc}), not a database error. "
                "A caller cannot distinguish this from a bug in its own code, it "
                "matches no handler written against asyncpg's error types, and "
                "the pooled connection that produced it was handed out while "
                "still mid-operation -- the same race _with_fresh_connection_retry "
                "closes for the enqueue path, unguarded here"
            ) from exc
