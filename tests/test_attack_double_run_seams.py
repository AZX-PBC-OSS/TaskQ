# ruff: noqa: S608  # Why: schema is a test-fixture identifier, validated by render()/open_fleet upstream; every value is $-bound.
"""Red-team pins: the double-run seams of the current combined main.

The hunt class: one (job, attempt) executing twice, or one job running on
two workers concurrently, through the NEW seams - the cancel ladder's
unheld-row ownership (#488), the claim intents' token scoping (#471), and
the identity-fenced maps (#480). Real PG, two pools, barrier starts,
server-side clock anchors.

The seams and the fleet shape each pin:

1. THE PARKED ROW (the unheld walk's ownership test). The walk reads
   ``active_jobs.get(id) is None`` as "unheld: the body exited and its
   outcome write was cancel-fenced". A row PARKED in the local queue (the
   producer's ``mark_enqueued`` mark, no consumer take yet) also has no
   entry - and the walk cannot tell the two apart. The graces run, the
   escalation lands, the abandon applies, and the row is TERMINAL while
   this process still holds it in its queue. The consumer then takes the
   abandoned row and executes the body with no cancellation ever
   delivered: the ladder's poll filters ``status = 'running'``, so the
   abandoned row never appears again. A body run after the job's terminal
   write - the run-after-terminal shape the effects ledger reconciles
   against. The contract: the unheld walk must exclude every row this
   process still owns (the registry's held ids AND the queued ids), the
   same ownership doctrine every hand-back pass applies.

2. THE INTENT WINDOW (the same walk, the take-to-register class). A row
   with a standing claim intent (taken, not yet registered) also reads as
   unheld. The abandon applies while the intent stands; the late
   registration then runs the body on a terminal row. Same contract as
   pin 1, the second map of the ownership pair.

3. THE FENCE ORDER (two workers on one phase-2 unheld row). The walk's
   abandon and worker B's reclaim machinery (the crash-reclaim sweep and
   the dispatch claim) race on the same row with its lease expired.
   Whichever order commits, the row terminalises EXACTLY once, is never
   re-pended, and worker B's claim never takes it: a row carrying an
   in-flight cancel request must never re-enter the fleet through any
   reclaim arm, and the loser of the race must absorb (the abandon's
   guard, the sweep's snap) rather than double-write.

4. THE CLAIM-LOSS RECONCILE under the fleet shape. The heartbeat tick's
   reconcile statement refunds rows "nothing holds"; its exclusion array
   binds the registry's three maps. The unit pins assert the array's
   contents against a mock pool; this pin runs the REAL rendered template
   against a real row with a standing claim intent while worker B
   dispatches at the same instant: the row must not be refunded, must
   stay running and locked, and worker B must not claim it.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.worker.cancel import make_cancel_controller
from tests._fleet import Fleet, open_fleet

pytestmark = pytest.mark.integration

_ACTOR = "seam_actor"
_QUEUE = "default"
# The graces collapsed so the ladder's whole schedule (observe -> escalate
# -> abandon) fits inside a handful of ticks - the repro stays deterministic
# without depending on how long a body takes to reach its first await.
_GRACES = {
    "cancellation_grace_period": "0.05",
    "cleanup_grace_period": "0.05",
}
_TICK = 0.05
_MAX_TICKS = 40
_BARRIER_PODS = 2
_SWEEP_GRACE = timedelta(seconds=0)


class _SeamPayload(BaseModel):
    """The payload the seam actor's rows carry."""


def _make_ctx(job_id: JobId, worker_id: Any) -> JobContext[BaseModel]:
    """A minimal JobContext for a directly-registered attempt task."""
    import structlog

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.testing.actor import FakeBackend

    return JobContext[BaseModel](
        job_id=job_id,
        actor=_ACTOR,
        queue=_QUEUE,
        attempt=1,
        claim_epoch=0,
        worker_id=worker_id,
        payload=_SeamPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=FakeBackend()),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job_id,
            actor=_ACTOR,
            queue=_QUEUE,
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


async def _take_exit_and_strand(fleet: Fleet, job_id: JobId) -> None:
    """Run the exited-body class to its stranded state.

    The consumer take (mark_claimed), a body that exits without an
    outcome write (its write was cancel-fenced in the real sequence), and
    the consumer's unconditional finally deregister. What remains is the
    row the unheld walk exists for: running, locked to this worker,
    holding NOBODY - not parked, not intent-held, truly orphaned.
    """
    pod_a = fleet.pod("pod-a")
    claim = pod_a.deps.active_jobs.mark_claimed(job_id)

    async def _body_exits_unwritten() -> None:
        return None

    task: asyncio.Task[object] = asyncio.create_task(_body_exits_unwritten())
    entry = await pod_a.deps.active_jobs.register(job_id, task, _make_ctx(job_id, pod_a.worker_id))
    await task
    await pod_a.deps.active_jobs.deregister(job_id, entry)
    pod_a.deps.active_jobs.resolve_claim(job_id, claim)
    assert pod_a.deps.active_jobs.get(job_id) is None, (
        "the exited body must have deregistered: the row is now truly unheld"
    )


async def _tick(controller: Any, conn: Any) -> None:
    """One heartbeat-tick shape: the in-tx hook, then the post-tx drain."""
    async with conn.transaction():
        await controller.run_in_tx(conn)
    await controller.run_post_tx()


async def _row_status(fleet: Fleet, job_id: JobId) -> str:
    rows = await fleet.fetch('SELECT status::text AS s FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(rows[0]["s"])


async def _job_phase(fleet: Fleet, job_id: JobId) -> int:
    rows = await fleet.fetch('SELECT cancel_phase AS p FROM "{schema}".jobs WHERE id = $1', job_id)
    return int(rows[0]["p"])


async def _effects_after_terminal(fleet: Fleet, job_id: JobId) -> int:
    """Body-effect rows written AFTER the job's terminal write.

    The exactly-once reconciliation: a body runs under a live claim, so
    every effect it writes precedes the job's terminal write. An effect
    timestamped after ``finished_at`` is a body that executed on a row
    nothing held any more - the run-after-terminal shape.
    """
    rows = await fleet.fetch(
        'SELECT count(*) AS n FROM "{schema}".atk_effects e '
        'JOIN "{schema}".jobs j ON j.id = e.job_id '
        "WHERE e.job_id = $1 AND e.at > j.finished_at",
        job_id,
    )
    return int(rows[0]["n"])


async def _seed_effects_table(fleet: Fleet) -> None:
    await fleet.fetch(
        'CREATE TABLE IF NOT EXISTS "{schema}".atk_effects ('
        "job_id UUID NOT NULL, at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"
    )


async def _claim_one_to_pod(fleet: Fleet, pod_name: str, job_id: JobId) -> None:
    """Claim one enqueued row to a pod through the production claim CTE."""
    pod = fleet.pod(pod_name)
    rows = await pod.claim([_QUEUE], limit=5)
    assert [row.id for row in rows] == [job_id], (
        "the scenario seeds exactly one due row; the claim must take it"
    )


async def _parked_row_ticks(fleet: Fleet, job_id: JobId) -> None:
    """Drive the ladder over a PARKED cancelled row through its graces.

    The row is claimed (running, locked to pod A), the producer's
    ``mark_enqueued`` mark stands, no consumer has taken it: the local
    queue's residence. The operator's cancel request is stamped. The
    ladder is driven through its whole grace schedule WHILE PARKED, and
    the row must survive every tick as 'running'.
    """
    pod_a = fleet.pod("pod-a")
    await _claim_one_to_pod(fleet, "pod-a", job_id)
    # The producer's park mark: the row sits in local_queue, unclaimed by
    # any consumer. The registry has NO entry for it - the parked shape.
    pod_a.deps.active_jobs.mark_enqueued(job_id)

    requested = await pod_a.backend.write_cancel_request(job_id, "operator")
    assert requested, "the cancel request must stamp the running row"

    controller = make_cancel_controller(pod_a.deps, pod_a.worker_id, pod_a.backend)
    conn = await pod_a.deps.heartbeat_pool.acquire()
    try:
        for _ in range(_MAX_TICKS):
            await _tick(controller, conn)
            status = await _row_status(fleet, job_id)
            assert status == "running", (
                f"the ladder abandoned a row PARKED in this worker's local queue "
                f"(status {status!r} while no consumer had taken it): the unheld "
                "walk's ownership test conflates the exited-body class with the "
                "parked class, and the abandon applies while this process still "
                "holds the row - the consumer's later take then executes the body "
                "on a terminal row with no cancellation ever delivered"
            )
            await asyncio.sleep(_TICK)
    finally:
        await pod_a.deps.heartbeat_pool.release(conn)


def _spawn_body(fleet: Fleet, job_id: JobId) -> asyncio.Task[object]:
    """The body a consumer attempt runs: effect on start, then park."""
    pod_a = fleet.pod("pod-a")

    async def _body() -> None:
        # The body's effect, written when the body STARTS - what a real
        # actor's first side effect is. Under the contract this lands
        # while the row is still live (running, held); under the hole it
        # lands after the abandon committed.
        await pod_a.deps.dispatcher_pool.execute(
            f'INSERT INTO "{fleet.schema}".atk_effects (job_id) VALUES ($1)',
            job_id,
        )
        await asyncio.sleep(3600)

    return asyncio.create_task(_body())


async def _register_attempt(fleet: Fleet, job_id: JobId, task: asyncio.Task[object]) -> None:
    """The consumer loop's registration, after its mark_claimed call."""
    pod_a = fleet.pod("pod-a")
    await pod_a.deps.active_jobs.register(job_id, task, _make_ctx(job_id, pod_a.worker_id))


async def _run_ladder_to_terminal(fleet: Fleet, job_id: JobId, task: asyncio.Task[object]) -> str:
    """Drive the ladder until the taken row terminalises (bounded).

    The taken attempt can terminalise through either held arm: the
    staggered path (phase 2 cancels the task, phase 3 abandons the row on
    a later tick) or the same-tick fast path (queue the abandon, the drain
    delivers the cancellation). Both end at the same terminal row; the
    loop reads the ROW, not the task.
    """
    pod_a = fleet.pod("pod-a")
    controller = make_cancel_controller(pod_a.deps, pod_a.worker_id, pod_a.backend)
    conn = await pod_a.deps.heartbeat_pool.acquire()
    try:
        for _ in range(_MAX_TICKS):
            status = await _row_status(fleet, job_id)
            if status in ("abandoned", "cancelled"):
                return status
            await _tick(controller, conn)
            await asyncio.sleep(_TICK)
    finally:
        await pod_a.deps.heartbeat_pool.release(conn)
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), timeout=5)
    status = await _row_status(fleet, job_id)
    assert status in ("abandoned", "cancelled"), (
        f"the taken row must terminalise through the ladder, got {status!r}"
    )
    return status


# ── Pin 1: the parked row ────────────────────────────────────────────────


async def test_parked_row_is_never_abandoned_while_the_queue_holds_it(pg_dsn: str) -> None:
    """The unheld walk must not own a row this worker still has queued.

    Contract: the walk's ownership test excludes the registry's queued ids
    the same way every hand-back pass excludes held ids. A parked row with
    an operator cancel stays parked until a consumer takes it; the ladder
    that then owns it is the HELD walk, whose abandon delivers the
    cancellation to the live entry. The abandon must never apply while the
    row sits in the queue, and no body effect may postdate the job's
    terminal write.
    """
    schema = f"seam_park_{new_uuid().hex[:8]}"
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=["pod-a"],
        actors=[(_ACTOR, _QUEUE)],
        settings_overrides=_GRACES,
    ) as fleet:
        await _seed_effects_table(fleet)
        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        job_id = job_ids[0]

        await _parked_row_ticks(fleet, job_id)

        # The consumer takes the row only now - after the whole parked
        # phase. Under the contract the row is still running and the
        # taken attempt walks the held ladder.
        task = _spawn_body(fleet, job_id)
        await _register_attempt(fleet, job_id, task)
        status = await _run_ladder_to_terminal(fleet, job_id, task)

        late = await _effects_after_terminal(fleet, job_id)
        assert late == 0, (
            f"{late} body effect(s) timestamped after the job's terminal write "
            f"(final status {status!r}): the body executed on a row nothing held "
            "- a run after the terminal state, the exactly-once counter's orphan "
            "shape"
        )


# ── Pin 2: the intent window ────────────────────────────────────────────


async def test_intent_window_row_is_never_abandoned_while_the_claim_stands(
    pg_dsn: str,
) -> None:
    """The unheld walk must not own a row with a standing claim intent.

    Contract: the walk's ownership test excludes the registry's intent ids
    (``held_ids()`` covers both maps). A taken-but-unregistered row with an
    operator cancel is this process's to register and run; the abandon
    must never apply while the intent stands.
    """
    schema = f"seam_intent_{new_uuid().hex[:8]}"
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=["pod-a"],
        actors=[(_ACTOR, _QUEUE)],
        settings_overrides=_GRACES,
    ) as fleet:
        await _seed_effects_table(fleet)
        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        job_id = job_ids[0]

        pod_a = fleet.pod("pod-a")
        await _claim_one_to_pod(fleet, "pod-a", job_id)
        # The take: the intent stands, the register has not happened. The
        # take-to-register window is the map the token scoping fences.
        pod_a.deps.active_jobs.mark_claimed(job_id)

        requested = await pod_a.backend.write_cancel_request(job_id, "operator")
        assert requested, "the cancel request must stamp the running row"

        controller = make_cancel_controller(pod_a.deps, pod_a.worker_id, pod_a.backend)
        conn = await pod_a.deps.heartbeat_pool.acquire()
        try:
            for _ in range(_MAX_TICKS):
                await _tick(controller, conn)
                status = await _row_status(fleet, job_id)
                assert status == "running", (
                    f"the ladder abandoned a row with a STANDING CLAIM INTENT "
                    f"(status {status!r}): the unheld walk's ownership test reads "
                    "the bare registry, the intent map is invisible to it, and "
                    "the abandon applies while the take-to-register window is "
                    "open - the registration then runs the body on a terminal row"
                )
                await asyncio.sleep(_TICK)
        finally:
            await pod_a.deps.heartbeat_pool.release(conn)

        # Register the attempt the standing claim promised, then let the
        # held ladder own the row.
        task = _spawn_body(fleet, job_id)
        await _register_attempt(fleet, job_id, task)
        status = await _run_ladder_to_terminal(fleet, job_id, task)

        late = await _effects_after_terminal(fleet, job_id)
        assert late == 0, (
            f"{late} body effect(s) timestamped after the job's terminal write "
            f"(final status {status!r}): the body executed on a row nothing held "
            "- a run after the terminal state, the exactly-once counter's orphan "
            "shape"
        )


# ── Pin 3: the fence order, two workers, one phase-2 row ────────────────


async def test_phase2_row_never_reenters_the_fleet_while_the_abandon_races_the_sweep(
    pg_dsn: str,
) -> None:
    """Two workers on expired-lease phase-2 unheld rows: exactly one
    terminal write, never a re-pend, never a second claim.

    Two legs, two rows, one contract:

    - SWEEP-FIRST: worker B's reclaim machinery reaches the row BEFORE the
      walk's abandon. The sweep's cancel branch must terminalise the row
      itself ('cancelled'), never re-pend it: a row carrying an in-flight
      cancel request re-enters the claim cycle through no arm.
    - RACED: the walk's abandon and worker B's sweep + dispatch race from
      a barrier start. Whichever order commits, the row terminalises
      EXACTLY once, is never 'pending', and worker B's claim never takes
      it. The loser absorbs (the abandon's guard, the sweep's snap).
    """
    from taskq.backend.postgres import PostgresBackend

    schema = f"seam_fence_{new_uuid().hex[:8]}"
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=["pod-a", "pod-b"],
        actors=[(_ACTOR, _QUEUE)],
        settings_overrides=_GRACES,
    ) as fleet:
        await _seed_effects_table(fleet)
        # ── Leg 1: the sweep reaches the row first ────────────────────
        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        sweep_first_id = job_ids[0]

        pod_a = fleet.pod("pod-a")
        pod_b = fleet.pod("pod-b")

        await _claim_one_to_pod(fleet, "pod-a", sweep_first_id)
        await _take_exit_and_strand(fleet, sweep_first_id)
        assert await pod_a.backend.write_cancel_request(sweep_first_id, "operator")

        controller = make_cancel_controller(pod_a.deps, pod_a.worker_id, pod_a.backend)
        conn = await pod_a.deps.heartbeat_pool.acquire()
        try:
            for _ in range(_MAX_TICKS):
                if await _job_phase(fleet, sweep_first_id) == 2:
                    break
                await _tick(controller, conn)
                await asyncio.sleep(_TICK)
        finally:
            await pod_a.deps.heartbeat_pool.release(conn)
        assert await _job_phase(fleet, sweep_first_id) == 2, (
            "the walk must have escalated the unheld row durably"
        )
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET lock_expires_at = '
            "clock_timestamp() - interval '61 seconds' WHERE id = $1",
            sweep_first_id,
        )

        # Worker B's sweep loop ALONE, to exhaustion.
        for _ in range(_MAX_TICKS):
            async with pod_b.deps.worker_pool.acquire() as sweep_conn:
                count = await PostgresBackend.sweep_expired_locks(
                    sweep_conn,
                    _SWEEP_GRACE,
                    _SWEEP_GRACE,
                    schema=schema,
                    batch_size=8,
                )
            status = await _row_status(fleet, sweep_first_id)
            assert status not in ("pending", "scheduled"), (
                f"the sweep re-pended a phase-2 row (status {status!r}): a row "
                "carrying an in-flight cancel request must never re-enter the "
                "claim cycle - the next claimant would run the job the operator "
                "already cancelled"
            )
            if count == 0:
                break
            await asyncio.sleep(_TICK)
        status = await _row_status(fleet, sweep_first_id)
        assert status == "cancelled", (
            f"the sweep-first leg must terminalise the row 'cancelled' via the "
            f"sweep's cancel branch, got {status!r}"
        )
        rows = await fleet.fetch(
            'SELECT count(*) AS n FROM "{schema}".job_attempts WHERE job_id = $1',
            sweep_first_id,
        )
        assert rows[0]["n"] == 1, (
            "one swept row, one attempt row: the cancel branch writes the ledger exactly once"
        )

        # ── Leg 2: the abandon and the reclaim machinery race ─────────
        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        raced_id = job_ids[0]
        await _claim_one_to_pod(fleet, "pod-a", raced_id)
        await _take_exit_and_strand(fleet, raced_id)
        assert await pod_a.backend.write_cancel_request(raced_id, "operator")
        conn = await pod_a.deps.heartbeat_pool.acquire()
        try:
            for _ in range(_MAX_TICKS):
                if await _job_phase(fleet, raced_id) == 2:
                    break
                await _tick(controller, conn)
                await asyncio.sleep(_TICK)
        finally:
            await pod_a.deps.heartbeat_pool.release(conn)
        assert await _job_phase(fleet, raced_id) == 2, (
            "the walk must have escalated the raced row durably"
        )
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET lock_expires_at = '
            "clock_timestamp() - interval '61 seconds' WHERE id = $1",
            raced_id,
        )

        barrier = asyncio.Barrier(_BARRIER_PODS)

        async def _abandon_side() -> None:
            async with barrier:
                conn = await pod_a.deps.heartbeat_pool.acquire()
                try:
                    await _tick(controller, conn)
                finally:
                    await pod_a.deps.heartbeat_pool.release(conn)

        async def _reclaim_side() -> None:
            async with barrier:
                # The sweep loop and the dispatch rounds, worker B's
                # reclaim machinery, at the same instant as the abandon.
                for _ in range(_MAX_TICKS):
                    async with pod_b.deps.worker_pool.acquire() as sweep_conn:
                        count = await PostgresBackend.sweep_expired_locks(
                            sweep_conn,
                            _SWEEP_GRACE,
                            _SWEEP_GRACE,
                            schema=schema,
                            batch_size=8,
                        )
                    rows = await pod_b.claim([_QUEUE], limit=5)
                    assert rows == [], (
                        "worker B's dispatch claimed a phase-2 cancelled row: a "
                        "row carrying an in-flight cancel request must never "
                        "re-enter the fleet through any reclaim arm"
                    )
                    if count == 0:
                        status = await _row_status(fleet, raced_id)
                        if status in ("abandoned", "cancelled"):
                            return
                    await asyncio.sleep(_TICK)

        await asyncio.gather(_abandon_side(), _reclaim_side())

        status = await _row_status(fleet, raced_id)
        assert status in ("abandoned", "cancelled"), (
            f"the raced row must be terminal, got {status!r}: a re-pend or a "
            "second claim would run the job again after the cancel protocol "
            "already owned it"
        )
        rows = await fleet.fetch(
            'SELECT count(*) AS n FROM "{schema}".job_attempts WHERE job_id = $1',
            raced_id,
        )
        assert rows[0]["n"] == 1, (
            f"one raced row, {rows[0]['n']} attempt rows: the loser of the "
            "fence order must absorb, never double-write the ledger"
        )
        assert await _effects_after_terminal(fleet, raced_id) == 0, (
            "a body effect after the terminal write on the raced row"
        )


# ── Pin 4: the claim-loss reconcile under the fleet shape ───────────────


async def test_reconcile_never_refunds_a_row_a_claim_intent_holds(pg_dsn: str) -> None:
    """The reconcile's real template honours the registry's exclusion array.

    Contract: a running row locked to worker A with a standing claim intent
    is not refundable, no matter how old its started_at is - the intent is
    the row's holder. Worker B dispatching at the same instant must not
    claim it either.
    """
    from taskq.testing.settings import make_integration_settings_dict
    from taskq.worker.heartbeat import (  # pyright: ignore[reportPrivateUsage]  # Why: the reconcile statement is the heartbeat tick's private template; the pin binds the real SQL, not a copy.
        _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE,
    )

    schema = f"seam_reconcile_{new_uuid().hex[:8]}"
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=["pod-a", "pod-b"],
        actors=[(_ACTOR, _QUEUE)],
        settings_overrides=_GRACES,
    ) as fleet:
        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
        job_id = job_ids[0]

        pod_a = fleet.pod("pod-a")
        pod_b = fleet.pod("pod-b")
        await _claim_one_to_pod(fleet, "pod-a", job_id)
        # The intent stands; the register never happens in this window.
        pod_a.deps.active_jobs.mark_claimed(job_id)
        # started_at aged past the lease grace: the reconcile's only row
        # predicate besides ownership and the exclusion array.
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET started_at = '
            "clock_timestamp() - interval '1 hour' WHERE id = $1",
            job_id,
        )

        settings = WorkerSettings.load_from_dict(
            make_integration_settings_dict(pg_dsn, schema_name=schema)
        )
        settings.schema_name = schema
        reconcile_sql = _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE.format(schema=schema)
        lease_seconds = settings.lock_lease

        barrier = asyncio.Barrier(_BARRIER_PODS)

        async def _reconcile_side() -> int:
            async with barrier:
                # The tick's exact exclusion binding: the union of the
                # three holding structures, sorted.
                excluded = sorted(
                    {
                        *pod_a.deps.active_jobs.held_ids(),
                        *pod_a.deps.active_jobs.queued_ids(),
                        *pod_a.deps.disowned_jobs,
                    }
                )
                async with pod_a.deps.heartbeat_pool.acquire() as conn:
                    rows = await conn.fetch(
                        reconcile_sql,
                        pod_a.worker_id,
                        excluded,
                        timedelta(seconds=lease_seconds),
                    )
                return len(rows)

        async def _dispatch_side() -> int:
            async with barrier:
                rows = await pod_b.claim([_QUEUE], limit=5)
                return len(rows)

        refunded, claimed = await asyncio.gather(_reconcile_side(), _dispatch_side())

        assert refunded == 0, (
            f"the reconcile refunded {refunded} row(s) whose claim intent was "
            "standing: the intent is the row's holder, the exclusion array is "
            "bound from the live maps, and the refund would un-charge an "
            "attempt the about-to-register body will spend"
        )
        assert claimed == 0, (
            "worker B claimed a row locked to worker A: the dispatch claim "
            "must never take a running row, whatever the reconcile races"
        )
        assert await _row_status(fleet, job_id) == "running", (
            "the row must still be running after the reconcile tick"
        )
        rows = await fleet.fetch(
            'SELECT started_at IS NOT NULL AS stamped FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert rows[0]["stamped"], "the refund un-stamps started_at; the row kept its stamp"
