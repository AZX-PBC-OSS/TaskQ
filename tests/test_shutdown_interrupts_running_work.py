"""A deploy must never terminalise, strand, or charge the work it interrupts.

A rolling deploy hands each pod a SIGTERM with jobs mid-flight. What happens
to those rows is decided by the shutdown orchestration, and the governing
principle is that a job's terminal state reflects what its *actor* did - an
infrastructure event may delay the work or hand it to another worker, but
never discard it, spend its budget, or choose its outcome.

The interrupted attempt is RELEASED, not terminalised: the row goes back to
the fleet (``pending`` when the actor unwound on the cancel, ``scheduled``
behind the remaining termination budget when it did not), the claim's attempt
increment is NOT refunded (the attempt started executing, so it is spent
a refund would re-create the epoch the interrupted handler holds and let its
zombie terminal write land on the re-dispatched attempt; see,
and the row's ``interrupt_count`` plus one ``job_events`` transition are the
record. An operator cancel that is in flight when the deploy lands still wins
the row: interruption is identified by origin, never by exception type, so a
genuine cancel cannot be laundered into a re-pend.

These drive the real claim CTE (phase-0 rows, exactly as a deploy finds
them), the production ``consume_one_job``, and the production
``orchestrate_shutdown`` against a live schema.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.assertions import wait_for
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job
from taskq.worker.dispatch import (  # pyright: ignore[reportPrivateUsage]  # Why: the production sync-actor dispatch helper: the thread tracking under test is exactly what dispatch_one_job calls for a sync actor.
    _run_sync_actor_tracked,
)
from taskq.worker.shutdown import (  # pyright: ignore[reportPrivateUsage]  # Why: the exit tail is the deadline model the hold assertions recompute.
    _watchdog_exit_tail,
    orchestrate_shutdown,
)

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=60)


class _Payload(BaseModel):
    """Empty payload; these contracts are about row state, not job input."""


async def _enqueue_job(
    backend: PostgresBackend,
    *,
    schedule_to_close: datetime | None = None,
) -> JobId:
    job_id = JobId(new_uuid())
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            schedule_to_close=schedule_to_close,
        )
    )
    return job_id


async def _claim_one(
    backend: PostgresBackend,
    deps: WorkerDeps,
    schema: str,
) -> tuple[UUID, JobRow]:
    """Claim one row through the production claim CTE.

    Returns the worker id and the claimed row. The row carries the exact
    shape a deploy interrupts: ``running``, locked, attempt incremented,
    ``cancel_phase = 0``.
    """
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    claimed = await backend.dispatch_batch(worker_id, ["default"], 1, _LOCK_LEASE)
    assert len(claimed) == 1, "the scenario needs the pod to hold the job"
    row = claimed[0]
    assert row.cancel_phase == 0, "the claim must leave the row outside any operator cancel"
    return worker_id, row


def _run_attempt(
    deps: WorkerDeps,
    backend: PostgresBackend,
    worker_id: UUID,
    row: JobRow,
    actor: object,
) -> asyncio.Task[object]:
    """Start the production attempt path as a task, registered in-flight."""
    return asyncio.create_task(
        consume_one_job(
            backend,
            row,
            worker_id,
            deps=deps,
            run_actor=actor,  # type: ignore[arg-type]  # Why: the test actors are typed against the loose call shape; the runtime contract holds.
            actor_config=StubActorConfig(
                retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
            ),
            payload_type=_Payload,
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
        )
    )


async def _attempt_rows(deps: WorkerDeps, schema: str, job_id: UUID) -> int:
    async with deps.worker_pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_attempts WHERE job_id = $1',  # noqa: S608  # Why: schema is a fixture-owned identifier; the job id is $-bound.
            job_id,
        )


async def _interrupted_events(backend: PostgresBackend, job_id: UUID) -> int:
    events = await backend.get_events(JobId(job_id))
    return sum(
        1 for e in events if e.kind == "state_change" and e.detail.get("reason") == "interrupted"
    )


async def test_shutdown_releases_a_responsive_actor_back_to_pending(
    clean_jobs_app: JobsApp,
) -> None:
    """The actor that unwinds on the cancel is re-pended immediately.

    The cancel event fires at CANCELLING and this actor raises on it - the
    shape the cancellation guide teaches. The deploy must leave the row
    ``pending`` for the surviving fleet with the claim's attempt increment
    returned: nothing ran to completion, so nothing may be spent. A deploy
    that terminalises this row as ``cancelled`` - the old behaviour - spends
    the budget and makes the deploy indistinguishable from a genuine cancel.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    worker_id, row = await _claim_one(backend, deps, schema)

    started = asyncio.Event()

    async def responsive_actor(_job: JobRow, ctx: JobContext[BaseModel]) -> object:
        started.set()
        await ctx.cancel_event.wait()
        raise asyncio.CancelledError

    attempt_task = _run_attempt(deps, backend, worker_id, row, responsive_actor)
    await wait_for(started, timeout=5.0, description="the actor to be running")

    shutdown_event = asyncio.Event()
    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0
    with contextlib.suppress(asyncio.CancelledError):
        await attempt_task

    after = await backend.get(job_id)
    assert after is not None
    assert after.status == "pending", (
        "a job whose actor unwound on the deploy's cancel must go straight back "
        "to the fleet as pending; it is free and the actor is gone. Got "
        f"{after.status!r} - the deploy terminalised work that was only ever "
        "interrupted"
    )
    assert after.attempt == enqueued.attempt + 1, (
        "the interruption must NOT refund the claim's attempt increment: the "
        f"attempt started executing, so it is spent. attempt went "
        f"{enqueued.attempt} -> {after.attempt} across the deploy"
    )
    assert after.locked_by_worker is None and after.lock_expires_at is None, (
        "a released row must not stay locked to the departed pod"
    )
    assert after.interrupt_count == 1, (
        "the release is counted on the row so an operator can see how often a "
        f"job has been interrupted by infrastructure; got {after.interrupt_count}"
    )

    assert await _attempt_rows(deps, schema, job_id) == 0, (
        "an interruption is not an execution outcome: no job_attempts row may "
        "be written for it (the same rule the other non-consuming releases "
        "keep); the attempt number the interrupted handler holds is never "
        "re-stamped by a writer, and the re-claim advances past it"
    )
    assert await _interrupted_events(backend, job_id) == 1, (
        "exactly one job_events transition with reason 'interrupted' records "
        "the release on the job's timeline"
    )


async def test_shutdown_releases_an_unresponsive_actor_behind_the_remaining_budget(
    clean_jobs_app: JobsApp,
) -> None:
    """The actor that never unwinds is released behind a hold.

        A job that ignores the cooperative cancel and the forced cancel is still
        running when the graces expire. Its row is released ``scheduled`` behind
        the rest of this process's termination budget - the window in which the
        watchdog guarantees the process is gone - so no other pod can claim the
        row while its first runner might still be alive. The attempt is NOT
        refunded: the attempt started executing, so its increment stands
    .
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    worker_id, row = await _claim_one(backend, deps, schema)

    started = asyncio.Event()
    release_actor = asyncio.Event()

    async def unresponsive_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        started.set()
        with contextlib.suppress(asyncio.CancelledError):
            # The forced cancel lands here and is swallowed: this actor does
            # not unwind inside either grace window.
            await asyncio.sleep(3600)
        await release_actor.wait()
        return {"done": True}

    attempt_task = _run_attempt(deps, backend, worker_id, row, unresponsive_actor)
    await wait_for(started, timeout=5.0, description="the actor to be running")

    shutdown_event = asyncio.Event()
    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0

    after = await backend.get(job_id)
    assert after is not None
    try:
        assert after.status == "scheduled", (
            "a job whose actor never unwound must be released behind a hold, not "
            f"left locked to a pod that is exiting; got {after.status!r}"
        )
        assert after.attempt == enqueued.attempt + 1, (
            "the interruption must NOT refund the claim's attempt increment; "
            f"the attempt started executing: attempt went "
            f"{enqueued.attempt} -> {after.attempt}"
        )
        assert after.locked_by_worker is None and after.lock_expires_at is None
        assert after.interrupt_count == 1

        # The hold is the remaining termination budget PLUS the watchdog's
        # exit tail: the dump-interval lag before the deadline trip is
        # observed and the bounded flush the trip performs before
        # os._exit, and never exceeds that sum (the remaining share is
        # counted from the start of the shutdown, not a fresh budget at
        # release time).
        hold = after.scheduled_at - datetime.now(UTC)
        termination = deps.settings.termination_grace_period
        tail = _watchdog_exit_tail(deps.settings)
        assert timedelta(0) < hold <= timedelta(seconds=termination + tail), (
            f"the release of a still-running actor must be deferred until the "
            f"releasing process is provably gone - the deadline itself is not "
            f"enough, the watchdog's exit tail ({tail}s) is part of the "
            f"promise: hold reads {hold}, expected within (0, "
            f"{termination + tail}s]"
        )
    finally:
        # Let the zombie actor return; its late success write must be a no-op
        # against the released row: the row is 'scheduled' (the status fence
        # rejects it), and the attempt epoch it holds is never re-created by
        # a refund, so a re-claim advances past it.
        release_actor.set()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await attempt_task

    assert await _attempt_rows(deps, schema, job_id) == 0
    settled = await backend.get(job_id)
    assert settled is not None
    assert settled.status == "scheduled" and settled.interrupt_count == 1, (
        "the released row must survive the interrupted actor's late terminal "
        "write untouched - the fence on the release is what keeps a deploy "
        "from committing an attempt the fleet already took back"
    )


async def test_shutdown_hold_falls_back_to_the_lock_lease_without_the_watchdog(
    clean_jobs_app: JobsApp,
) -> None:
    """With no shutdown watchdog there is no guaranteed exit, so the hold is
    the lock lease - the bound the lease-expiry path already imposes today,
    now without spending the attempt."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    deps.settings.watchdog_enabled = False

    job_id = await _enqueue_job(backend)
    worker_id, row = await _claim_one(backend, deps, schema)

    started = asyncio.Event()
    release_actor = asyncio.Event()

    async def unresponsive_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        started.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(3600)
        await release_actor.wait()
        return {"done": True}

    attempt_task = _run_attempt(deps, backend, worker_id, row, unresponsive_actor)
    await wait_for(started, timeout=5.0, description="the actor to be running")

    shutdown_event = asyncio.Event()
    try:
        exit_code = await orchestrate_shutdown(
            deps, deps.settings, worker_id, shutdown_event, None, backend=backend
        )
        assert exit_code == 0

        after = await backend.get(job_id)
        assert after is not None
        assert after.status == "scheduled"
        hold = after.scheduled_at - datetime.now(UTC)
        lock_lease = timedelta(seconds=deps.settings.lock_lease)
        assert lock_lease - timedelta(seconds=2) < hold <= lock_lease, (
            f"with the watchdog disabled the hold must be the lock lease "
            f"({lock_lease}), the bound a stranded lease already imposes; got {hold}"
        )
    finally:
        release_actor.set()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await attempt_task


async def test_operator_cancel_in_flight_wins_over_the_deploy(
    clean_jobs_app: JobsApp,
) -> None:
    """A row already under an operator cancel is never re-pended by a deploy.

    The operator asked for a terminal state; the deploy must not launder it
    into a release. The row carries ``cancel_phase >= 1`` when SIGTERM lands,
    so the interrupt write's phase fence declines it and the operator ladder -
    escalation at FORCING, abandonment past the graces - owns the outcome:
    ``cancelled`` for the actor that unwinds, ``abandoned`` for the one that
    does not.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    worker_id, row = await _claim_one(backend, deps, schema)

    started = asyncio.Event()
    release_actor = asyncio.Event()

    async def unresponsive_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        started.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(3600)
        await release_actor.wait()
        return {"done": True}

    attempt_task = _run_attempt(deps, backend, worker_id, row, unresponsive_actor)
    await wait_for(started, timeout=5.0, description="the actor to be running")

    # The operator's cancel lands BEFORE the deploy: the row goes to
    # cancel_phase = 1 while the pod is still serving.
    assert await backend.write_cancel_request(JobId(job_id), "operator stop") is True

    shutdown_event = asyncio.Event()
    try:
        exit_code = await orchestrate_shutdown(
            deps, deps.settings, worker_id, shutdown_event, None, backend=backend
        )
        assert exit_code == 0

        after = await backend.get(job_id)
        assert after is not None
        assert after.status in {"cancelled", "abandoned"}, (
            "an operator cancel in flight must reach its terminal state through "
            "the operator ladder, not be released back to the fleet; got "
            f"{after.status!r}"
        )
        assert after.interrupt_count == 0, (
            "the interruption must not touch a row the operator already "
            "cancelled - the interrupt write's cancel_phase = 0 fence declines "
            f"it; interrupt_count reads {after.interrupt_count}"
        )
    finally:
        release_actor.set()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await attempt_task

    assert await _interrupted_events(backend, job_id) == 0, (
        "an operator-cancelled job must not carry an interruption on its timeline"
    )


async def test_an_operator_cancel_landing_mid_shutdown_still_cancels(
    clean_jobs_app: JobsApp,
) -> None:
    """A cancel request racing the deploy itself keeps its terminal claim.

    The deploy's CANCELLING phase has already signalled the actor when the
    operator's cancel lands on the row. The actor unwinds; the release write
    finds the row at ``cancel_phase = 1`` and declines it, and the ordinary
    cancel write terminalises the row instead. The deploy never turns an
    operator's cancel into a re-pend, however the two race.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    worker_id, row = await _claim_one(backend, deps, schema)

    cooperative_signal_seen = asyncio.Event()
    land_operator_cancel = asyncio.Event()

    async def unwind_after_the_cancel_lands(_job: JobRow, ctx: JobContext[BaseModel]) -> object:
        await ctx.cancel_event.wait()
        # The deploy's cooperative signal reached the actor. Hold the unwind
        # until the operator's cancel is on the row, so the two genuinely
        # race in the consumer's terminal routing.
        cooperative_signal_seen.set()
        await land_operator_cancel.wait()
        raise asyncio.CancelledError

    attempt_task = _run_attempt(deps, backend, worker_id, row, unwind_after_the_cancel_lands)

    shutdown_event = asyncio.Event()
    orchestrator = asyncio.create_task(
        orchestrate_shutdown(deps, deps.settings, worker_id, shutdown_event, None, backend=backend)
    )
    await wait_for(
        cooperative_signal_seen,
        timeout=10.0,
        description="the shutdown's cooperative cancel to reach the actor",
    )
    assert await backend.write_cancel_request(JobId(job_id), "operator stop") is True
    land_operator_cancel.set()

    exit_code = await orchestrator
    assert exit_code == 0
    with contextlib.suppress(asyncio.CancelledError):
        await attempt_task

    after = await backend.get(job_id)
    assert after is not None
    assert after.status == "cancelled", (
        "an operator cancel landing while the shutdown's cooperative cancel "
        f"unwinds the actor must still terminalise as cancelled; got {after.status!r}"
    )
    assert after.interrupt_count == 0
    assert await _interrupted_events(backend, job_id) == 0


async def test_a_hold_that_outlives_the_jobs_deadline_fails_it_on_the_deadline(
    clean_jobs_app: JobsApp,
) -> None:
    """A hold that pushes past ``schedule_to_close`` fails the job on the
    deadline - the same terminal exit every deferral arm honours.

    The release cannot park a job beyond its own deadline into a state
    nothing terminalises: the deadline, not the deploy, ends that job. The
    failure is the deadline's (``DeadlineExceeded``), not the shutdown's, and
    no interruption is counted for a release that never happened.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    # The deadline lands inside the hold the release would otherwise carry
    # (the hold is the remaining termination budget, several seconds out at
    # the integration settings).
    deadline = datetime.now(UTC) + timedelta(seconds=1.5)
    job_id = await _enqueue_job(backend, schedule_to_close=deadline)
    worker_id, row = await _claim_one(backend, deps, schema)

    started = asyncio.Event()
    release_actor = asyncio.Event()

    async def unresponsive_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        started.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(3600)
        await release_actor.wait()
        return {"done": True}

    attempt_task = _run_attempt(deps, backend, worker_id, row, unresponsive_actor)
    await wait_for(started, timeout=5.0, description="the actor to be running")

    shutdown_event = asyncio.Event()
    try:
        exit_code = await orchestrate_shutdown(
            deps, deps.settings, worker_id, shutdown_event, None, backend=backend
        )
        assert exit_code == 0

        after = await backend.get(job_id)
        assert after is not None
        assert after.status == "failed", (
            f"a job whose hold would outlive its schedule_to_close must fail on "
            f"the deadline, not be parked past it; got {after.status!r}"
        )
        assert after.error_class == "DeadlineExceeded"
        assert after.interrupt_count == 0, (
            "no interruption is counted: the deadline arm, not the release arm, owns this outcome"
        )
    finally:
        release_actor.set()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await attempt_task


# ── The sync-actor gate: a deploy must never double-run a thread ──


async def _spawn_second_worker(deps: WorkerDeps, schema: str) -> UUID:
    """Register a second worker the way a surviving pod registers itself."""
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    return worker_id


async def _claim_as(
    backend: PostgresBackend,
    worker_id: UUID,
) -> list[JobRow]:
    """One claim round for *worker_id*: exactly what a surviving pod's
    producer loop does the moment a row looks available."""
    return await backend.dispatch_batch(worker_id, ["default"], 1, _LOCK_LEASE)


async def test_a_deploy_never_hands_a_live_sync_actors_row_to_a_second_worker(
    clean_jobs_app: JobsApp,
) -> None:
    """THE sync-actor gate: a thread the cancel cannot reach is never
    concurrently claimable.

    A sync actor runs in an executor thread; the deploy's ``task.cancel()``
    cancels the await, never the thread. The old release wrote
    ``mark_interrupted(hold=0)`` the moment the await died, so the row went
    ``pending`` while the body was still mid-execution, and a second
    worker's claim round took it and ran the job a second time, side by
    side with the dying pod's thread. This is exactly the TAStack shape
    (plain ``def`` actors, transactional workers), and the claim below
    runs while the first attempt's thread is PROVABLY still alive.

    The fixed contract: the release is held back: the consumer parks on
    the tracked thread handle, bounded by the remaining termination budget,
    and the row stays ``scheduled`` behind the process's exit window until
    the body provably exits or the window ends. The row is never stranded
    either: once the hold's ``scheduled_at`` passes (expired surgically
    here: the scheduled_to_pending sweep's job in production), the second
    worker claims it and the re-run buys a fresh attempt increment.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    worker_id, row = await _claim_one(backend, deps, schema)

    body_started = threading.Event()
    release_body = threading.Event()
    body_exited = threading.Event()
    executions: list[float] = []

    def sync_body(payload: _Payload) -> object:
        del payload
        executions.append(datetime.now(UTC).timestamp())
        body_started.set()
        # The deploy's cancel cannot reach this thread: it runs until the
        # test releases it, exactly like an actor mid-body across a SIGTERM.
        release_body.wait(30.0)
        body_exited.set()
        return {"done": True}

    async def run_sync_actor(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        del job_row
        return await _run_sync_actor_tracked(sync_body, {"payload": ctx.payload}, ctx)  # type: ignore[arg-type]  # Why: the production dispatch helper driven with the test's body: the exact call shape dispatch_one_job makes for a registered sync actor.

    attempt_task = _run_attempt(deps, backend, worker_id, row, run_sync_actor)
    await asyncio.to_thread(body_started.wait, 10.0)

    shutdown_started_wall = datetime.now(UTC)
    shutdown_event = asyncio.Event()
    try:
        exit_code = await orchestrate_shutdown(
            deps, deps.settings, worker_id, shutdown_event, None, backend=backend
        )
        assert exit_code == 0
        # The consumer parks (bounded) on the tracked thread before its own
        # release write; the attempt ends cancelled once that is done.
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(attempt_task, timeout=30.0)

        # The thread is PROVABLY still alive: the premise of the whole
        # scenario: the dying process might still touch the row.
        assert not body_exited.is_set()
        assert executions, "the body must have started (and been counted) exactly once"

        after = await backend.get(job_id)
        assert after is not None
        assert after.status == "scheduled", (
            "a sync actor that never provably exited must be released HELD "
            f"(scheduled behind the exit window), not pending; got {after.status!r} "
            "- a pending row here is the double-execution overlap: the "
            "second worker claims it while the first pod's thread still runs"
        )
        assert after.attempt == enqueued.attempt + 1, (
            "the interruption must NOT refund the claim's attempt increment: "
            "the attempt started executing, so it is spent; the "
            "re-run spends its own fresh claim increment"
        )
        assert after.interrupt_count == 1
        # The hold covers the whole exit window: the deadline plus the
        # watchdog's tail past it. The tail is pinned from the settings and
        # the two literals it is built from (the bounded 2s metrics flush
        # and ~1s of stack-render/log slack): NOT from the shutdown
        # module's own helper, so weakening the pad in the source trips
        # this assertion instead of silently moving with it.
        expected_tail = deps.settings.watchdog_dump_interval + 2.0 + 1.0
        hold = after.scheduled_at - datetime.now(UTC)
        min_cover = (
            shutdown_started_wall
            + timedelta(seconds=deps.settings.termination_grace_period)
            + timedelta(seconds=expected_tail)
            - timedelta(seconds=2.0)
        )
        assert after.scheduled_at >= min_cover, (
            f"the hold must keep the row unclaimable until this process is "
            f"provably gone (deadline {deps.settings.termination_grace_period}s "
            f"+ exit tail {expected_tail}s from the shutdown's start - the "
            f"watchdog observes the deadline only once per dump interval and "
            f"flushes metrics before os._exit); hold reads {hold}"
        )

        # The gate itself: a second worker's claim round, run while the
        # first pod's thread is provably still alive, must find nothing.
        second_worker = await _spawn_second_worker(deps, schema)
        claimed = await _claim_as(backend, second_worker)
        assert claimed == [], (
            "a second worker claimed the row while the first attempt's sync "
            f"thread was still running: the job would execute twice at once "
            f"(claimed {[c.id for c in claimed]})"
        )

        # Never stranded either: once the hold's scheduled_at passes (the
        # scheduled_to_pending sweep's transition, applied surgically here),
        # the second worker claims the row and the re-run buys its own
        # attempt increment.
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status='pending', "  # noqa: S608  # Why: schema is a fixture-owned identifier; the job id is $-bound.
                f"scheduled_at = clock_timestamp() - interval '1 second' "
                f"WHERE id = $1",
                job_id,
            )
        reclaimed = await _claim_as(backend, second_worker)
        assert len(reclaimed) == 1 and reclaimed[0].id == job_id, (
            "after the hold expires the row must be claimable again: a held "
            "release that never becomes claimable strands the job"
        )
        assert reclaimed[0].attempt == enqueued.attempt + 2, (
            "the re-run's claim buys a fresh attempt increment on top of the "
            "interrupted attempt's (which is NOT refunded,: "
            "enqueued -> interrupt claim (+1) -> re-claim (+1)"
        )
        assert len(executions) == 1, (
            "the actor body ran exactly once: the deploy interrupted it, "
            "held it, and handed it to the fleet without ever running it "
            "twice at once"
        )
    finally:
        release_body.set()
        await asyncio.to_thread(body_exited.wait, 10.0)


async def test_a_deploy_holds_a_transactional_sync_actor_until_its_thread_exits(
    clean_jobs_app: JobsApp,
) -> None:
    """The transactional shape (ta_worker): a sync actor inside an open
    transaction is released held, its unwind awaited first.

    The transactional path cancelled ``tx_task`` and re-raised without
    waiting, so the release landed while the transaction task was still
    unwinding, and with a sync actor inside, the unwind's to_thread await
    dies while the BODY carries on. The fixed path parks on both the tx
    unwind and the tracked thread; the row stays held until the process is
    provably gone.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    worker_id, row = await _claim_one(backend, deps, schema)

    body_started = threading.Event()
    release_body = threading.Event()
    body_exited = threading.Event()

    def sync_body(payload: _Payload) -> object:
        del payload
        body_started.set()
        release_body.wait(30.0)
        body_exited.set()
        return {"done": True}

    async def run_sync_actor(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        del job_row
        return await _run_sync_actor_tracked(sync_body, {"payload": ctx.payload}, ctx)  # type: ignore[arg-type]  # Why: same production dispatch helper as the autonomous gate test.

    shutdown_event = asyncio.Event()
    async with deps.worker_pool.acquire() as tx_conn:
        attempt_task = asyncio.ensure_future(
            consume_one_job(
                backend,
                row,
                worker_id,
                deps=deps,
                run_actor=run_sync_actor,
                actor_config=StubActorConfig(
                    retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
                ),
                payload_type=_Payload,
                clock=SystemClock(),
                active_jobs=deps.active_jobs,
                transaction_conn=tx_conn,
            )
        )
        await asyncio.to_thread(body_started.wait, 10.0)

        try:
            exit_code = await orchestrate_shutdown(
                deps, deps.settings, worker_id, shutdown_event, None, backend=backend
            )
            assert exit_code == 0
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(attempt_task, timeout=30.0)

            # The thread never exited: the release must have been held.
            assert not body_exited.is_set()
            after = await backend.get(job_id)
            assert after is not None
            assert after.status == "scheduled", (
                "a transactional sync actor whose thread never provably "
                f"exited must be released held; got {after.status!r}"
            )
            assert after.attempt == enqueued.attempt + 1
            assert after.interrupt_count == 1

            second_worker = await _spawn_second_worker(deps, schema)
            assert await _claim_as(backend, second_worker) == [], (
                "the transactional path's row must be as unclaimable as the "
                "autonomous one while the dying process's thread runs"
            )
        finally:
            release_body.set()
            await asyncio.to_thread(body_exited.wait, 10.0)
