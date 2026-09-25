"""The SIGTERM requeue-state matrix: every in-flight work state requeues whole.

For EACH state a job can be in when SIGTERM lands, the shutdown path must
produce a whole requeue: the row re-pended exactly once, the attempt ledger
recording the truth about the interrupted attempt, no claim orphaned, and no
double run with the successor worker.

The matrix (state at SIGTERM -> the shutdown path that owns it -> the
contract pinned here):

1. PARKED in the local queue (the producer's ``mark_enqueued``, no
   consumer take yet). The row is running and locked to this worker, held
   by the registry's ``_queued`` map only. The contract on main: the
   DRAINING pass RE-PENDS it to the fleet (attempt refunded - no actor
   ever took it), and this worker's consumers never run it (they return
   without dispatching once the producer's stop flag is set). The
   successor worker picks it up.
2. TAKEN, in the take-to-register window (the claim intent standing).
   The intent fences the row out of every hand-back pass: the window
   belongs to this worker's running path. The intent's token is
   identity-scoped; when the window resolves without a run, the next
   hand-back pass re-pends the row whole (refund, exactly once).
3. RUNNING pre-terminal. The FORCING phase delivers the cancel; the
   consumer's interruption release (``mark_interrupted``) re-pends the
   row with the spent attempt standing (charged, never refunded, no
   ``job_attempts`` row - an interruption is not an execution outcome),
   and the successor re-runs it under a NEW attempt.
4. RUNNING with the terminal write IN FLIGHT. The write is shielded; the
   drain's exclusion fold (``held_ids``) keeps the DRAINING pass off the
   row; exactly one of the success write and the interruption release
   lands. Two folds of the exclusion are pinned here: a consumer-disowned
   row (the terminal write failed on infrastructure) is NOT the drain's
   population - the drain must not refund an attempt that started
   executing, and the heartbeat reconcile's refunded row (its output is
   ``running`` + locked + ``started_at`` un-stamped) must not be
   re-refunded by the DRAINING pass (the drain's ``started_at IS NOT
   NULL`` conjunct is exactly that fence).
5. CANCELLED during the shutdown (an operator cancel in flight when
   SIGTERM landed). The drain's ``cancel_phase = 0`` fence keeps the row
   off every requeue; the cancel ladder owns the row's exit; the
   RELEASING phase's abandon write no-ops against the terminal row. One
   writer, one verdict, no requeue.

Every test drives the real claim CTE, the production consumer, the
production producer loop, the production heartbeat tick (the reconcile and
the cancel ladder), the real Sweep 1, and the production
``orchestrate_shutdown`` against live PostgreSQL. The subprocess shapes of
states 1, 3 and 5 (a real worker process SIGTERMed, a real successor
picking up within the derived bound, the balancing counter over the whole
population) live in the system tier:
``tests/system_e2e/test_shutdown_requeue_states.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, JobFilter, JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.context import CancelOrigin, JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.assertions import wait_for, wait_for_condition
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry, ClaimIntent, _ActiveJob
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.shutdown import (
    ShutdownPhase,
    drain_local_queue_to_pending,
    orchestrate_shutdown,
)

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=60)
_TAG = "req-states"
_EFFECTS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".sys_effects (
    job_id  UUID NOT NULL,
    attempt INT NOT NULL,
    actor   TEXT NOT NULL,
    kind    TEXT NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
"""


class _Payload(BaseModel):
    """Empty payload; these contracts are about row state, not job input."""


async def _create_effects_table(deps: WorkerDeps) -> None:
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(_EFFECTS_DDL.format(schema=deps.settings.schema_name))


async def _record_effect(deps: WorkerDeps, job_id: UUID, attempt: int, kind: str) -> None:
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{deps.settings.schema_name}".sys_effects '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "(job_id, attempt, actor, kind) VALUES ($1, $2, 'test_actor', $3)",
            job_id,
            attempt,
            kind,
        )


async def _enqueue(
    backend: PostgresBackend,
    *,
    tag: str = _TAG,
    actor: str = "test_actor",
    max_attempts: int = 3,
) -> JobId:
    job_id = JobId(new_uuid())
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=max_attempts,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            tags=(tag,),
        )
    )
    return job_id


async def _new_worker(deps: WorkerDeps, schema: str, worker_id: UUID | None = None) -> UUID:
    wid = worker_id or new_uuid()
    # A successor is a NEW PROCESS: a fresh WorkerDeps carries no shutdown
    # state, and the consumer's take-to-register seam reads exactly that
    # state - a deps object reused across a completed orchestration still
    # carries the finished shutdown's stamps (the orchestrator never
    # clears them; in production the process exits instead), which would
    # release every successor attempt before its body. The same reset the
    # fleet harness's stop_pod applies between generations. No test here
    # creates a worker while an orchestration is in flight - every
    # successor lands after `await orchestrator` - so the reset never
    # races a live phase.
    deps.shutdown_phase = ShutdownPhase.NONE
    deps.shutdown_started_at = None
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, wid)
    return wid


async def _claim(
    backend: PostgresBackend,
    deps: WorkerDeps,
    schema: str,
    *,
    limit: int = 1,
) -> tuple[UUID, list[JobRow]]:
    """Claim through the production claim CTE: running, locked, attempt
    incremented, ``started_at`` stamped, ``cancel_phase = 0``."""
    worker_id = await _new_worker(deps, schema)
    claimed = await backend.dispatch_batch(worker_id, ["default"], limit, _LOCK_LEASE)
    assert len(claimed) == limit, "the scenario needs the pod to hold the job(s)"
    return worker_id, claimed


async def _job_row(deps: WorkerDeps, schema: str, job_id: UUID) -> asyncpg.Record:
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT id, status::text AS status, attempt, locked_by_worker::text AS locked_by, "  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            f"lock_expires_at, started_at, interrupt_count, assignment_routed, cancel_phase, "
            f'finished_at FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None, f"job {job_id} vanished from the live table"
    return row


async def _attempt_rows(deps: WorkerDeps, schema: str, job_id: UUID) -> list[asyncpg.Record]:
    async with deps.worker_pool.acquire() as conn:
        return await conn.fetch(
            f'SELECT attempt, outcome FROM "{schema}".job_attempts WHERE job_id = $1 '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "ORDER BY attempt",
            job_id,
        )


async def _interrupted_events(deps: WorkerDeps, schema: str, job_id: UUID) -> int:
    async with deps.worker_pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_events '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "WHERE job_id = $1 AND kind = 'state_change' "
            "AND detail->>'reason' = 'interrupted'",
            job_id,
        )


async def _age_started_at(deps: WorkerDeps, schema: str, job_id: UUID) -> None:
    lease = deps.settings.lock_lease
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET started_at = clock_timestamp() - $2::interval '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "WHERE id = $1",
            job_id,
            timedelta(seconds=lease + 10),
        )


async def _age_lock_expiry(deps: WorkerDeps, schema: str, job_id: UUID) -> None:
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = clock_timestamp() - $2::interval '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "WHERE id = $1",
            job_id,
            timedelta(seconds=1),
        )


async def _run_one_heartbeat_tick(deps: WorkerDeps, worker_id: UUID) -> None:
    """Run the real heartbeat_loop for exactly one tick, synchronised on
    the tick-duration hook (the idiom test_rt_458 pins)."""
    import taskq.worker.heartbeat as hb_mod

    shutdown = asyncio.Event()
    tick_done = asyncio.Event()
    prev_record = hb_mod._tick_duration.record  # type: ignore[reportPrivateUsage]  # Why: the tick-complete hook the heartbeat unit tests synchronise on.

    def _record_and_signal(value: float, *args: object, **kwargs: object) -> None:
        prev_record(value, *args, **kwargs)
        tick_done.set()

    hb_mod._tick_duration.record = _record_and_signal  # type: ignore[method-assign,reportPrivateUsage]
    try:
        task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
        await asyncio.wait_for(tick_done.wait(), timeout=10.0)
        shutdown.set()
        await task
    finally:
        hb_mod._tick_duration.record = prev_record  # type: ignore[method-assign,reportPrivateUsage]


def _completing_actor(deps: WorkerDeps, runs: dict[UUID, list[int]]) -> object:
    """A body that records its run into the effects ledger and succeeds."""

    async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> dict[str, int]:
        runs.setdefault(job.id, []).append(ctx.attempt)
        await _record_effect(deps, job.id, ctx.attempt, "done")
        return {"ok": 1}

    return actor


def _run_attempt(
    deps: WorkerDeps,
    backend: PostgresBackend,
    worker_id: UUID,
    row: JobRow,
    actor: object,
    active_jobs: ActiveJobRegistry | None = None,
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
            active_jobs=active_jobs if active_jobs is not None else deps.active_jobs,
        )
    )


async def _assert_balanced(deps: WorkerDeps, schema: str, tag: str) -> None:
    """The balancing counter: conservation plus the effects ledger."""
    from tests.system_e2e._invariants import (
        conservation_violations,
        effect_ledger_violations,
    )

    async with deps.worker_pool.acquire() as conn:
        violations = await conservation_violations(conn, schema, tag)
        violations += await effect_ledger_violations(conn, schema, tag)
    assert not violations, "the balancing counter does not balance:\n" + "\n".join(violations)


# ── State 1: PARKED in the local queue ───────────────────────────────────


async def test_state1_parked_rows_are_re_pended_to_the_fleet_not_abandoned(
    clean_jobs_app: JobsApp,
) -> None:
    """A row parked in local_queue (claimed, never taken by a consumer) is
    re-pended by the DRAINING pass with its attempt refunded, and a
    successor worker picks it up.

    The contract on main: the parked row is NOT this worker's to run
    during shutdown and NOT abandoned - it goes back to the fleet
    (``pending``, unlocked, the claim's increment refunded, the row
    routed by its actor's assignment). The refund is exactly-once: the
    producer's exit hand-back pass (the second writer on this path)
    matches nothing.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    parked_ids = [await _enqueue(backend) for _ in range(2)]
    tail_ids = [await _enqueue(backend) for _ in range(3)]

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=2)
    shutdown_event = asyncio.Event()
    deps.producer_stop_event = asyncio.Event()
    deps.shutdown_phase = ShutdownPhase.NONE
    deps.shutdown_started_at = None
    worker_id = await _new_worker(deps, schema)

    producer = asyncio.create_task(_producer(deps, local_queue, shutdown_event, backend, worker_id))
    # Deterministic parked state: no consumers exist, so the queue fills to
    # its maxsize and the producer parks. The two parked rows are claimed
    # (running, locked) but held only by the registry's queued map.
    await wait_for_condition(
        lambda: local_queue.qsize() == 2,
        timeout=10.0,
        description="the producer to park two claimed rows in the local queue",
    )
    parked = set(deps.active_jobs.queued_ids())
    assert parked == set(parked_ids), (
        "the premise: the parked rows must be exactly the claimed-but-untaken ones"
    )
    for job_id in parked_ids:
        row = await _job_row(deps, schema, job_id)
        assert row["status"] == "running" and row["locked_by"] == str(worker_id), (
            "the premise: a parked row is claimed and locked to this worker"
        )
        assert row["attempt"] == 1, "the premise: the claim charged the attempt"
        assert row["started_at"] is not None, "the premise: the claim stamped started_at"

    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0
    await producer

    for job_id in parked_ids:
        row = await _job_row(deps, schema, job_id)
        assert row["status"] == "pending", (
            "a row parked in the local queue must be re-pended to the fleet at "
            f"DRAINING, got {row['status']!r} - abandoned, it stops moving until "
            "a lock lease expires"
        )
        assert row["locked_by"] is None and row["lock_expires_at"] is None, (
            "the re-pend must clear the lock; the departing pod owns nothing"
        )
        assert row["attempt"] == 0, (
            "the re-pend refunds the claim's attempt increment: no actor ever "
            f"took the row, so the claim bought nothing. Got attempt={row['attempt']}"
        )
        assert row["assignment_routed"] is True, (
            "the re-pended row routes by the actor's current assignment from "
            "here on (the re-pend class)"
        )
        assert row["started_at"] is not None, (
            "the drain does not un-stamp started_at: the un-stamp is the "
            "heartbeat reconcile's refund signature, never the drain's"
        )

    # Exactly-once: the producer's exit hand-back (the second pass every
    # shutdown carries) matches nothing and refunds nothing again.
    handed_back = await drain_local_queue_to_pending(deps, worker_id)
    assert handed_back == 0, f"the second hand-back pass must match nothing, got {handed_back} rows"
    for job_id in parked_ids:
        row = await _job_row(deps, schema, job_id)
        assert row["attempt"] == 0, "the refund is exactly-once per claim"

    # The successor picks the whole backlog up: the parked rows are
    # claimable immediately (no lease wait), and run exactly once.
    successor = await _new_worker(deps, schema)
    runs: dict[UUID, list[int]] = {}
    claimed = await backend.dispatch_batch(successor, ["default"], 10, _LOCK_LEASE)
    assert {job.id for job in claimed} == {*parked_ids, *tail_ids}, (
        "the successor must be able to claim every re-pended row plus the untouched backlog"
    )
    for job in claimed:
        outcome = await consume_one_job(
            backend,
            job,
            successor,
            deps=deps,
            run_actor=_completing_actor(deps, runs),
            actor_config=StubActorConfig(
                retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
            ),
            payload_type=_Payload,
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
        )
        assert outcome == "succeeded"
    for job_id in parked_ids:
        assert runs[job_id] == [1], (
            f"the successor runs the parked job exactly once, at the refunded "
            f"claim's attempt number; got {runs[job_id]}"
        )
    await _assert_balanced(deps, schema, _TAG)


async def _producer(
    deps: WorkerDeps,
    local_queue: asyncio.Queue[JobRow],
    shutdown_event: asyncio.Event,
    backend: PostgresBackend,
    worker_id: UUID,
) -> None:
    from taskq.worker.run import producer_loop

    await producer_loop(
        deps,
        local_queue,
        shutdown_event,
        deps.producer_stop_event,
        backend=backend,
        worker_id=worker_id,
    )


# ── State 2: TAKEN, the take-to-register window ──────────────────────────


async def test_state2_claim_intent_fences_the_row_out_of_every_hand_back(
    clean_jobs_app: JobsApp,
) -> None:
    """The take-to-register window belongs to this worker's running path.

    While the claim intent stands, no hand-back pass may re-pend the row
    or refund its attempt, and no successor may claim it. When the window
    resolves without a run (the consumer loop's exit resolves the claim),
    the NEXT hand-back pass re-pends the row whole: refund, exactly once.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue(backend)
    worker_id, _claimed = await _claim(backend, deps, schema)
    assert _claimed, "the premise: the claim landed"

    # The producer's mark, then the consumer's take: the intent stands.
    deps.active_jobs.mark_enqueued(job_id)
    token: ClaimIntent = deps.active_jobs.mark_claimed(job_id)
    assert job_id in deps.active_jobs.held_ids(), (
        "the premise: the take-to-register window is held by the intent map"
    )

    # The DRAINING pass runs with the intent standing: the row is not
    # re-pended, its attempt is not refunded.
    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 0, (
        f"the hand-back pass must not re-pend a row whose claim intent stands, "
        f"got {drained} row(s) - a re-pend here races the consumer that is "
        "about to register and execute it"
    )
    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "running" and after["attempt"] == 1, (
        "the window's attempt accounting rides the intent: no refund while "
        f"the intent stands, got {after}"
    )

    # No successor can claim a row this process holds: it is running.
    other = await _new_worker(deps, schema)
    assert await backend.dispatch_batch(other, ["default"], 5, _LOCK_LEASE) == []

    # The token is identity-scoped (the issue-461 class): a stale
    # generation's resolve removes nothing, the live intent survives.
    stale_token = deps.active_jobs.mark_claimed(job_id)
    deps.active_jobs.resolve_claim(job_id, token)
    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 0, "a stale token's resolve must not release the live claim's intent"
    deps.active_jobs.resolve_claim(job_id, stale_token)

    # The window resolved without a run: the next hand-back pass re-pends
    # the row whole - refunded, exactly once.
    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 1, (
        f"once the intent is released, the hand-back pass re-pends the row, got {drained}"
    )
    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "pending" and after["attempt"] == 0, (
        f"the requeue of a never-started window is refunded and unlocked, got {after}"
    )
    assert await _attempt_rows(deps, schema, job_id) == [], (
        "no ledger row: no execution ever happened"
    )


# ── State 2 -> 3: a late registration torn down by the shutdown ──────────


class _GatedRegisterRegistry(ActiveJobRegistry):
    """A registry whose register() parks until the gate opens.

    The harness hook: it holds a consumer inside the take-to-register
    handoff (after the take, before the install) so a test can run the
    shutdown past the phases that stamp active entries, then release the
    registration - the late-registration shape a slow slot-pool acquire
    or DI resolution produces under load.
    """

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()

    async def register(
        self, job_id: JobId, task: asyncio.Task[object], ctx: JobContext[BaseModel]
    ) -> _ActiveJob:
        await self.gate.wait()
        return await super().register(job_id, task, ctx)


async def test_state2_late_registration_torn_down_by_the_shutdown_requeues(
    clean_jobs_app: JobsApp,
) -> None:
    """A job that registers after the CANCELLING stamping pass still gets
    an interruption, never a verdict.

    The take-to-register window can open onto a slow registration (a
    parked slot-pool acquire, DI resolution). The shutdown's CANCELLING
    pass only stamps entries present at its iteration; a consumer that
    registers later is torn down by the worker's own teardown
    cancellation with ``cancel_origin`` NONE. A deploy is an
    infrastructure event, so that cancellation must release the attempt
    back to the fleet (``mark_interrupted``), never terminalise the row
    ``cancelled``.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    job_id = await _enqueue(backend)
    worker_id, claimed = await _claim(backend, deps, schema)
    row = claimed[0]

    registry = _GatedRegisterRegistry()
    # The gated registry IS the worker's registry: the drain's exclusion
    # folds and the orchestrator read the same instance the consumer
    # registers into (one process, one registry).
    deps.active_jobs = registry
    token = registry.mark_claimed(job_id)
    runs: dict[UUID, list[int]] = {}

    started = asyncio.Event()

    async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> dict[str, int]:
        started.set()
        await asyncio.sleep(60.0)
        return {}

    attempt_task = _run_attempt(deps, backend, worker_id, row, actor, registry)
    # The attempt task parks INSIDE register(): the take-to-register window
    # is held open, the claim intent fences the row.

    shutdown_event = asyncio.Event()

    async def shutdown_task() -> int:
        return await orchestrate_shutdown(
            deps, deps.settings, worker_id, shutdown_event, None, backend=backend
        )

    orchestrator = asyncio.create_task(shutdown_task())
    # The orchestration runs to completion with the registration parked:
    # no phase sees the job. The claim intent fences the DRAINING pass off
    # the row the whole time.
    await orchestrator
    assert deps.shutdown_phase is ShutdownPhase.RELEASING
    drained_row = await _job_row(deps, schema, job_id)
    assert drained_row["status"] == "running" and drained_row["attempt"] == 1, (
        "the parked registration is fenced by its intent: no phase re-pends "
        f"or refunds it, got {drained_row}"
    )

    # The registration lands (the late-registration shape), the actor runs.
    registry.gate.set()
    await wait_for(started, timeout=5.0, description="the actor body to start")
    await wait_for_condition(
        lambda: registry.get(job_id) is not None,
        timeout=5.0,
        description="the consumer to register the late attempt",
    )

    # The worker's teardown cancels the attempt task (the TaskGroup exit).
    attempt_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await attempt_task
    deps.active_jobs.resolve_claim(job_id, token)

    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "pending", (
        "the teardown cancellation of a shutdown-era attempt must release the "
        f"row back to the fleet, got {after['status']!r} - a deploy never "
        "terminalises a job"
    )
    assert after["attempt"] == 1 and after["interrupt_count"] == 1, (
        "the release charges the attempt it ran (no refund) and counts the "
        f"interruption, got {after}"
    )
    assert await _interrupted_events(deps, schema, job_id) == 1, (
        "exactly one interrupted transition records the release"
    )
    assert await _attempt_rows(deps, schema, job_id) == [], (
        "an interruption writes no job_attempts row"
    )

    # The successor picks the row up within the bound (hold=0: the async
    # actor provably unwound) and runs it once at a NEW attempt.
    successor = await _new_worker(deps, schema)
    claimed = await backend.dispatch_batch(successor, ["default"], 5, _LOCK_LEASE)
    assert [job.id for job in claimed] == [job_id], "the successor claims the requeued row"
    outcome = await consume_one_job(
        backend,
        claimed[0],
        successor,
        deps=deps,
        run_actor=_completing_actor(deps, runs),
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
        ),
        payload_type=_Payload,
        clock=SystemClock(),
        active_jobs=deps.active_jobs,
    )
    assert outcome == "succeeded"
    assert runs[job_id] == [2], (
        f"the successor's run is a NEW attempt (the interrupted attempt stands "
        f"charged), got attempt {runs[job_id]}"
    )
    await _assert_balanced(deps, schema, _TAG)


# ── State 2, the consumer side: the take-to-register SHUTDOWN SEAM ───────


async def test_shutdown_seam_releases_the_never_started_claim_whole(
    clean_jobs_app: JobsApp,
    clean_redis_client: Any,
) -> None:
    """A dispatch parked in the take-to-register window when shutdown is
    stamped is released by the seam, and the release is WHOLE.

    The scenario: a row is claimed through the production claim CTE (the
    take), the consumer is dispatched at it, and the deps carries the
    orchestrator's shutdown stamp before the consumer's window walk
    reaches the guard - the consumer is parked inside the
    take-to-register window (registered nowhere) when the seam fires.
    The contract: the body never runs; the row goes back to the fleet
    exactly once (the fenced refund - status ``scheduled``, unlocked, the
    claim's attempt increment refunded, the ``released_reason`` metadata
    proving whose write it was); no ``job_attempts`` ledger row exists (a
    never-started attempt is not an execution); the ``scheduled``
    state-change reaches Redis like every other requeue (the ordinary
    snooze path's publish); and - the leak's tooth - NO
    ``progress_buffers`` entry is left behind: the buffer is installed
    only after the seam declines to fire, so a seam-fired dispatch cannot
    strand an entry the flush tick's dirty-scan would iterate forever.
    """
    from taskq.constants import progress_channel

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue(backend)
    worker_id, claimed = await _claim(backend, deps, schema)
    row = claimed[0]
    claim_row = await _job_row(deps, schema, job_id)
    assert claim_row["status"] == "running" and claim_row["attempt"] == 1, (
        f"the premise: the claim charged the attempt, got {claim_row}"
    )

    # The wire the seam's publish rides: subscribed before the dispatch,
    # so the `scheduled` state-change is captured, not joined in flight.
    pubsub = clean_redis_client.pubsub()
    await pubsub.subscribe(progress_channel(schema, job_id))
    try:
        # The shutdown stamp: the orchestrator's entry stamps
        # shutdown_started_at in the same synchronous block that raises
        # the phase to DRAINING - one signal, no await between them. The
        # consumer's window walk (payload validation, the effective-surface
        # resolution) reaches the seam guard with the stamp standing.
        deps.redis_client = clean_redis_client
        deps.shutdown_phase = ShutdownPhase.DRAINING
        deps.shutdown_started_at = asyncio.get_running_loop().time()

        body_ran: list[int] = []

        async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> dict[str, int]:
            body_ran.append(ctx.attempt)
            return {}

        outcome = await consume_one_job(
            backend,
            row,
            worker_id,
            deps=deps,
            run_actor=actor,
            actor_config=StubActorConfig(
                retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
            ),
            payload_type=_Payload,
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
        )

        assert outcome == "scheduled", (
            f"the seam's release returns the dispatch's scheduled outcome, got {outcome!r}"
        )
        assert body_ran == [], (
            f"the body ran under a shutdown in progress (attempts {body_ran}): the "
            "take-to-register seam must release the claim before any registration"
        )

        after = await _job_row(deps, schema, job_id)
        assert after["status"] == "scheduled" and after["locked_by"] is None, (
            f"the row must be back to the fleet (scheduled, unlocked), got {after}"
        )
        assert after["attempt"] == 0, (
            f"the claim's attempt increment must be refunded whole (charged 1, "
            f"refunded to 0), got attempt {after['attempt']}"
        )
        assert await _attempt_rows(deps, schema, job_id) == [], (
            "no ledger row: a never-started attempt is not an execution outcome"
        )

        # THE LEAK'S TOOTH. Pre-fix, the seam returned from between the
        # buffer install and the ``finally`` that removes it: every
        # shutdown-raced dispatch left one entry in the map for the
        # process's remaining life, and the flush tick's dirty-scan
        # iterated it forever. The pin goes red under that order.
        assert job_id not in deps.progress_buffers and not deps.progress_buffers, (
            f"the seam-fired dispatch left a progress_buffers entry behind "
            f"({len(deps.progress_buffers)}): the install preceded the guard, the "
            "buffer's removal ``finally`` never ran, and the flush tick's "
            "dirty-scan now iterates a dead entry forever"
        )

        # The observability consistency: exactly one `scheduled`
        # state-change on the per-job channel - the same publish the
        # ordinary snooze path applies, carrying no running transition
        # (the body never started, so none was published).
        events: list[dict[str, object]] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5.0
        while loop.time() < deadline:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message is not None and message.get("type") == "message":
                events.append(cast("dict[str, object]", json.loads(message["data"])))
        scheduled_events = [
            e for e in events if e.get("kind") == "state_change" and e.get("status") == "scheduled"
        ]
        assert len(scheduled_events) == 1, (
            f"exactly one scheduled state-change must reach Redis (the ordinary "
            f"snooze path's publish), got {len(scheduled_events)} of {len(events)} events"
        )
        assert scheduled_events[0].get("terminal") is False, (
            "the seam's release is a requeue, never a terminal verdict"
        )
    finally:
        await pubsub.aclose()


# ── State 3: RUNNING pre-terminal ────────────────────────────────────────


async def test_state3_running_job_interrupted_requeues_whole_for_the_successor(
    clean_jobs_app: JobsApp,
) -> None:
    """A job mid-body at SIGTERM: the cancel lands, the outcome write
    re-pends (charged attempt, no refund, no ledger row), and the
    successor re-runs it under a new attempt - exactly once per attempt,
    the ledger whole.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    job_id = await _enqueue(backend)
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    worker_id, claimed = await _claim(backend, deps, schema)
    row = claimed[0]

    started = asyncio.Event()

    async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> dict[str, int]:
        started.set()
        await ctx.cancel_event.wait()
        raise asyncio.CancelledError

    attempt_task = _run_attempt(deps, backend, worker_id, row, actor)
    await wait_for(started, timeout=5.0, description="the actor to be running")

    shutdown_event = asyncio.Event()
    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0
    with contextlib.suppress(asyncio.CancelledError):
        await attempt_task

    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "pending", (
        f"the interrupted row goes back to the fleet pending, got {after['status']!r}"
    )
    assert after["attempt"] == enqueued.attempt + 1, (
        "the interruption charges the attempt it ran: no refund"
    )
    assert after["locked_by"] is None, "no claim survives the departing pod"
    assert after["interrupt_count"] == 1
    assert await _attempt_rows(deps, schema, job_id) == [], (
        "an interruption is not an execution outcome: no job_attempts row"
    )
    assert await _interrupted_events(deps, schema, job_id) == 1

    # Successor pickup within the bound: hold=0 (the async actor provably
    # unwound), so the row is claimable immediately - the derived bound is
    # the successor's own poll floor, nothing else.
    successor = await _new_worker(deps, schema)
    runs: dict[UUID, list[int]] = {}
    claimed = await backend.dispatch_batch(successor, ["default"], 5, _LOCK_LEASE)
    assert [job.id for job in claimed] == [job_id], "the successor claims the requeued row"
    outcome = await consume_one_job(
        backend,
        claimed[0],
        successor,
        deps=deps,
        run_actor=_completing_actor(deps, runs),
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
        ),
        payload_type=_Payload,
        clock=SystemClock(),
        active_jobs=deps.active_jobs,
    )
    assert outcome == "succeeded"
    assert runs[job_id] == [2], (
        f"the re-run is a new attempt: exactly once per attempt, got {runs[job_id]}"
    )
    ledger = await _attempt_rows(deps, schema, job_id)
    assert [(int(r["attempt"]), r["outcome"]) for r in ledger] == [(2, "succeeded")], (
        f"the ledger records the attempt that terminalised, and only it: {ledger}"
    )
    await _assert_balanced(deps, schema, _TAG)


# ── State 4: RUNNING, terminal write in flight ───────────────────────────


async def test_state4_success_write_in_flight_the_drain_never_touches_the_row(
    clean_jobs_app: JobsApp,
) -> None:
    """The DRAINING pass must leave a row whose consumer holds an in-flight
    terminal write to the write's own race: the exclusion fold is
    ``held_ids``, and the row's outcome is exactly one of the shielded
    success write or the interruption release - never a drain re-pend that
    refunds a started attempt and fences the success write out.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    job_id = await _enqueue(backend)
    worker_id, claimed = await _claim(backend, deps, schema)
    row = claimed[0]

    write_gate = asyncio.Event()
    write_started = asyncio.Event()
    real_mark_succeeded = backend.mark_succeeded

    async def gated_mark_succeeded(*args: object, **kwargs: object) -> bool:
        write_started.set()
        await write_gate.wait()
        result = await real_mark_succeeded(*args, **kwargs)  # type: ignore[arg-type]
        return bool(result)

    backend.mark_succeeded = gated_mark_succeeded  # type: ignore[method-assign]  # Why: the harness hook parks the production write mid-flight; the delegate is the real bound method.

    async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> dict[str, int]:
        # The body ran to completion; only the verdict write is still in
        # flight. No effect row here: the interrupted attempt's ledger has
        # no row by design, and the effects-orphan check would flag a body
        # effect on it; the runs dict below is the exactly-once evidence.
        return {"ok": 1}

    attempt_task = _run_attempt(deps, backend, worker_id, row, actor)
    await wait_for(write_started, timeout=5.0, description="the success write to be in flight")

    shutdown_event = asyncio.Event()
    orchestrator = asyncio.create_task(
        orchestrate_shutdown(deps, deps.settings, worker_id, shutdown_event, None, backend=backend)
    )
    # The consumer is parked on the shielded write when FORCING delivers
    # its cancel. The gate stays closed, so the ordering is deterministic:
    # the success write cannot land first, the interruption release wins
    # the row, and the drain (whose pass ran with the entry in held_ids)
    # never touched it.
    await orchestrator
    with contextlib.suppress(asyncio.CancelledError):
        await attempt_task

    write_gate.set()
    await asyncio.sleep(0.2)  # let the detached success write land (fenced out)

    after = await _job_row(deps, schema, job_id)
    ledger = await _attempt_rows(deps, schema, job_id)
    assert after["status"] == "pending", f"the interruption owns the row, got {after}"
    assert after["attempt"] == 1, (
        f"the interruption charges the attempt; a drain re-pend would have "
        f"refunded it to 0, got {after}"
    )
    assert after["interrupt_count"] == 1
    assert await _interrupted_events(deps, schema, job_id) == 1, (
        "exactly one requeue of the row, and it is the interruption's"
    )
    assert ledger == [], "an interrupted attempt writes no ledger row"

    # The successor picks the row up and runs it once, at a NEW attempt.
    successor = await _new_worker(deps, schema)
    runs: dict[UUID, list[int]] = {}
    claimed = await backend.dispatch_batch(successor, ["default"], 5, _LOCK_LEASE)
    assert [job.id for job in claimed] == [job_id]
    result = await consume_one_job(
        backend,
        claimed[0],
        successor,
        deps=deps,
        run_actor=_completing_actor(deps, runs),
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
        ),
        payload_type=_Payload,
        clock=SystemClock(),
        active_jobs=deps.active_jobs,
    )
    assert result == "succeeded"
    assert runs[job_id] == [2]
    await _assert_balanced(deps, schema, _TAG)


async def test_state4_consumer_disowned_row_is_not_the_drains_refund_population(
    clean_jobs_app: JobsApp,
) -> None:
    """A row whose terminal write failed on infrastructure is disowned: the
    attempt STARTED executing, so the DRAINING pass must not refund it.

    Refunding a started attempt re-creates the exact attempt epoch the
    first execution ran under: the successor re-claims and re-runs the
    body at the SAME attempt number - a second run of one attempt, the
    shape the attempt-refund fragment's own header forbids. The disowned
    row's recovery is the lease-expiry sweep (Sweep 1), whose budget
    predicate re-pends it at the charged attempt.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    job_id = await _enqueue(backend)
    worker_id, claimed = await _claim(backend, deps, schema)
    row = claimed[0]

    async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> dict[str, int]:
        # The body ran to completion; only the verdict write is lost.
        await _record_effect(deps, job.id, ctx.attempt, "done")
        return {"ok": 1}

    real_mark_succeeded = backend.mark_succeeded

    async def dead_mark_succeeded(*args: object, **kwargs: object) -> bool:
        raise asyncpg.ConnectionDoesNotExistError("pool torn down mid-write")

    backend.mark_succeeded = dead_mark_succeeded  # type: ignore[method-assign]  # Why: the harness hook makes the production write fail with a member of the infra family; the retry budget runs out and the consumer disowns.

    attempt_task = _run_attempt(deps, backend, worker_id, row, actor)
    outcome = await attempt_task
    assert outcome == "failed"
    # The harness hook only covers the first attempt's loss: the
    # successor's verdict write below must be the real one.
    backend.mark_succeeded = real_mark_succeeded  # type: ignore[method-assign]
    assert job_id in deps.disowned_jobs, "the premise: the infra-failed write disowned the row"
    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "running" and after["attempt"] == 1, (
        "the premise: the row stays running, locked, at the charged attempt"
    )

    shutdown_event = asyncio.Event()
    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0

    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "running", (
        f"the disowned row is not the drain's population: Sweep 1 owns its "
        f"reclaim, got {after['status']!r}"
    )
    assert after["attempt"] == 1, (
        f"the drain must not refund an attempt that started executing, got "
        f"attempt={after['attempt']} - the refund re-creates the epoch the "
        "first execution ran under and the successor's re-run would be a "
        "second run of the SAME attempt"
    )
    assert await _interrupted_events(deps, schema, job_id) == 0, (
        "the drain writes no interruption: the row was never interrupted, it was disowned"
    )

    # The lease lapses; the real Sweep 1 reclaims at the charged attempt.
    await _age_lock_expiry(deps, schema, job_id)
    async with deps.worker_pool.acquire() as conn:
        count = await PostgresBackend.sweep_expired_locks(
            conn, timedelta(0), timedelta(0), schema=schema
        )
    assert count == 1, "the lapsed disowned row must be reclaimed by Sweep 1"
    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "pending" and after["attempt"] == 1, (
        f"the sweep re-pends at the charged attempt, got {after}"
    )
    # The reclaim's re-pend schedules the row on the retry backoff (the
    # same hand-back delay every reclaim arm stamps); anchor it to the PG
    # clock so the successor's claim below is deterministic.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "interval '1 second' WHERE id = $1",
            job_id,
        )

    # The successor re-runs the body under a NEW attempt: exactly once per
    # attempt, the effects ledger reconciles.
    successor = await _new_worker(deps, schema)
    runs: dict[UUID, list[int]] = {}
    claimed = await backend.dispatch_batch(successor, ["default"], 5, _LOCK_LEASE)
    assert [job.id for job in claimed] == [job_id]
    result = await consume_one_job(
        backend,
        claimed[0],
        successor,
        deps=deps,
        run_actor=_completing_actor(deps, runs),
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
        ),
        payload_type=_Payload,
        clock=SystemClock(),
        active_jobs=deps.active_jobs,
    )
    assert result == "succeeded"
    assert runs[job_id] == [2], (
        f"the re-run is a new attempt, never a second run of attempt 1, got {runs[job_id]}"
    )
    await _assert_balanced(deps, schema, _TAG)


async def test_state4_the_drain_never_re_refunds_the_reconciles_refund(
    clean_jobs_app: JobsApp,
) -> None:
    """The reconcile refunds a lost claim and UN-STAMPS ``started_at``,
    leaving the row running and locked for Sweep 1. The DRAINING pass's
    refund must be fenced off that output: a running-and-locked row with a
    NULL stamp is exactly "this claim was already refunded".

    The seeded job carries one spent attempt (a genuine execution's ledger
    row) so a second refund is observable: without the fence the drain
    drops the attempt counter below the epoch the ledger row holds, and
    the next claim re-creates a spent attempt.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    worker_id = await _new_worker(deps, schema)
    job_id = JobId(new_uuid())
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs
                (id, actor, queue, payload, max_attempts, retry_kind, status,
                 attempt, scheduled_at, tags)
                VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, 3, 'transient',
                        'pending', 1, clock_timestamp() - interval '1s', ARRAY[$2::text])""",  # noqa: S608
            job_id,
            _TAG,
        )
        # A genuine earlier execution's ledger row: what a double refund
        # would make the next claim re-create.
        await conn.execute(
            f'INSERT INTO "{schema}".job_attempts '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "(job_id, attempt, started_at, finished_at, outcome) "
            "VALUES ($1, 1, clock_timestamp(), clock_timestamp(), 'succeeded')",
            job_id,
        )

    claimed = await backend.dispatch_batch(worker_id, ["default"], 1, _LOCK_LEASE)
    assert len(claimed) == 1 and claimed[0].attempt == 2, (
        "the premise: the claim charged attempt 2 above the spent attempt 1"
    )

    # The claim response is lost (the row is held by no in-memory
    # structure); the reconcile probes the aged orphan and refunds once.
    await _age_started_at(deps, schema, job_id)
    await _run_one_heartbeat_tick(deps, worker_id)
    assert job_id in deps.disowned_jobs
    row = await _job_row(deps, schema, job_id)
    assert row["status"] == "running" and row["attempt"] == 1 and row["started_at"] is None, (
        f"the premise: the reconcile refunded once and un-stamped, got {row}"
    )

    # SIGTERM: the DRAINING pass. The reconciled output must be left for
    # Sweep 1 - no re-pend, no second refund.
    shutdown_event = asyncio.Event()
    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0

    row = await _job_row(deps, schema, job_id)
    assert row["status"] == "running", (
        f"the drain must leave the reconciled row for Sweep 1's reclaim, got "
        f"{row['status']!r} - a re-pend here yanks the row out of the sweep's "
        "own recovery trail"
    )
    assert row["attempt"] == 1, (
        f"the refund is exactly-once per claim across the two refund writers: "
        f"the drain must not refund the reconcile's claim a second time, got "
        f"attempt={row['attempt']} - the next claim would re-create the spent "
        "attempt epoch its genuine ledger row already holds"
    )

    # Sweep 1 reclaims; the successor claims at a NEW attempt and the
    # ledger reconciles.
    await _age_lock_expiry(deps, schema, job_id)
    async with deps.worker_pool.acquire() as conn:
        count = await PostgresBackend.sweep_expired_locks(
            conn, timedelta(0), timedelta(0), schema=schema
        )
    assert count == 1
    row = await _job_row(deps, schema, job_id)
    assert row["status"] == "pending" and row["attempt"] == 1, f"got {row}"
    # The reclaim's re-pend schedules the row on the retry backoff; anchor
    # it to the PG clock so the successor's claim below is deterministic.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "interval '1 second' WHERE id = $1",
            job_id,
        )

    successor = await _new_worker(deps, schema)
    runs: dict[UUID, list[int]] = {}
    claimed = await backend.dispatch_batch(successor, ["default"], 5, _LOCK_LEASE)
    assert [job.id for job in claimed] == [job_id]
    assert claimed[0].attempt == 2, (
        "the successor's claim is a NEW attempt epoch above the spent one"
    )
    result = await consume_one_job(
        backend,
        claimed[0],
        successor,
        deps=deps,
        run_actor=_completing_actor(deps, runs),
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
        ),
        payload_type=_Payload,
        clock=SystemClock(),
        active_jobs=deps.active_jobs,
    )
    assert result == "succeeded"
    assert runs[job_id] == [2]
    await _assert_balanced(deps, schema, _TAG)


# ── State 5: CANCELLED during the shutdown ───────────────────────────────


async def test_state5_operator_cancel_in_flight_owns_the_rows_exit(
    clean_jobs_app: JobsApp,
) -> None:
    """A job under an operator cancel when SIGTERM lands: the drain's
    ``cancel_phase = 0`` fence keeps it off every requeue, the cancel
    ladder terminalises it ``cancelled``, and the RELEASING phase's
    abandon write no-ops against the terminal row. One writer owns the
    exit; the successor has nothing to pick up.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    job_id = await _enqueue(backend)
    worker_id, claimed = await _claim(backend, deps, schema)
    row = claimed[0]

    started = asyncio.Event()

    async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> dict[str, int]:
        started.set()
        await ctx.cancel_event.wait()
        raise asyncio.CancelledError

    attempt_task = _run_attempt(deps, backend, worker_id, row, actor)
    await wait_for(started, timeout=5.0, description="the actor to be running")

    # The operator's cancel write lands on the row (phase 1), and the
    # SIGTERM lands BEFORE the ladder's poll observes it: the entry's
    # origin is still NONE, the row says who asked. This is the exact
    # window a cancel that races a deploy occupies.
    result = await backend.cancel_where(JobFilter(tags=(_TAG,)), reason="offboard")
    assert result.cancelled_directly == 0 and result.cancel_requested == 1, (
        "the premise: the running row carries the operator's request"
    )
    entry = deps.active_jobs.get(job_id)
    assert entry is not None, "the premise: the attempt is registered in-flight"
    assert entry.cancel_origin is CancelOrigin.NONE, (
        "the premise: the ladder has not observed the cancel yet"
    )
    async with deps.worker_pool.acquire() as conn:
        db_phase = await conn.fetchval(
            f'SELECT cancel_phase FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
    assert db_phase == 1, "the premise: the row carries the operator's cancel"

    # SIGTERM with the cancel in flight.
    shutdown_event = asyncio.Event()
    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0
    with contextlib.suppress(asyncio.CancelledError):
        await attempt_task

    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "cancelled", (
        f"the operator's cancel owns the exit: the row terminalises "
        f"cancelled, got {after['status']!r} - the drain's cancel_phase fence "
        "must never re-pend it, and the RELEASING abandon must not relabel it"
    )
    assert after["locked_by"] is None and after["finished_at"] is not None
    ledger = await _attempt_rows(deps, schema, job_id)
    assert [(int(r["attempt"]), r["outcome"]) for r in ledger] == [(1, "cancelled")], (
        f"one ledger row records the ladder's verdict: {ledger}"
    )
    assert await _interrupted_events(deps, schema, job_id) == 0, (
        "an operator cancel is never laundered into an interruption"
    )

    # No requeue: the successor has nothing to claim, no double run.
    successor = await _new_worker(deps, schema)
    assert await backend.dispatch_batch(successor, ["default"], 5, _LOCK_LEASE) == [], (
        "a cancelled row must not re-enter the fleet"
    )
    await _assert_balanced(deps, schema, _TAG)


async def test_state5_the_cancel_fence_keeps_a_phase_one_row_off_the_drain(
    clean_jobs_app: JobsApp,
) -> None:
    """The fence shape: an operator cancel stamped on a row NO consumer
    holds (the claim response was lost, the ladder has not run) when
    SIGTERM lands.

    Nothing in-memory holds the row, so the held_ids fold does not protect
    it: the drain's ``cancel_phase = 0`` fence is the only thing between
    the row and a re-pend that would hand a cancel-addressed job to the
    fleet. Fenced, the row stays Sweep 1's, and the sweep's cancel branch
    terminalises it 'cancelled' with the operator's audit columns intact:
    one writer owns the exit, and it is never the requeue.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    worker_id = await _new_worker(deps, schema)
    job_id = await _enqueue(backend)
    claimed = await backend.dispatch_batch(worker_id, ["default"], 1, _LOCK_LEASE)
    assert len(claimed) == 1, "the premise: the claim landed"
    # The claim response is lost: no mark_enqueued, no take, no register.

    result = await backend.cancel_where(JobFilter(tags=(_TAG,)), reason="offboard")
    assert result.cancel_requested == 1, "the premise: the row carries the request"
    assert not deps.active_jobs.held_ids() and not deps.active_jobs.queued_ids(), (
        "the premise: nothing in-memory holds the row"
    )

    shutdown_event = asyncio.Event()
    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    assert exit_code == 0

    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "running" and after["attempt"] == 1, (
        f"the cancel fence must keep a phase-1 row off the drain's re-pend, "
        f"got {after} - a re-pend here hands a cancel-addressed job to the "
        "fleet to execute under the next holder"
    )

    # The holder is gone; past the cancel carve-out, Sweep 1's cancel
    # branch owns the exit: 'cancelled', the operator's audit intact.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = clock_timestamp() - '  # noqa: S608
            "$2::interval WHERE id = $1",
            job_id,
            timedelta(seconds=300),
        )
        count = await PostgresBackend.sweep_expired_locks(
            conn, timedelta(0), timedelta(0), schema=schema
        )
    assert count == 1, "the fenced row must be reclaimed once its carve-out lapses"
    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "cancelled", (
        f"the sweep's cancel branch terminalises the honoured request, got {after}"
    )
    ledger = await _attempt_rows(deps, schema, job_id)
    # The reclaim's ledger row records outcome='crashed' on every branch
    # (the sweep's own pinned contract: that IS what happened to the
    # attempt - the holder died mid-protocol), while the ROW says
    # 'cancelled': the honoured request is the row's verdict.
    assert [(int(r["attempt"]), r["outcome"]) for r in ledger] == [(1, "crashed")], (
        f"one ledger row records the reclaim: {ledger}"
    )
    assert await _interrupted_events(deps, schema, job_id) == 0, (
        "an operator cancel is never laundered into an interruption"
    )
    successor = await _new_worker(deps, schema)
    assert await backend.dispatch_batch(successor, ["default"], 5, _LOCK_LEASE) == [], (
        "a cancelled row must not re-enter the fleet"
    )
    await _assert_balanced(deps, schema, _TAG)


async def test_state4_the_ledger_row_blocks_the_drains_refund(
    clean_jobs_app: JobsApp,
) -> None:
    """A running row whose CURRENT attempt already carries a job_attempts
    row (the isolate-self write, the reclaim INSERT, or a retried
    terminal's shape) is NEVER the drain's refund population.

    The refund would erase the executed attempt's charge and the
    successor's re-claim would re-issue the number the ledger's PK
    already holds: the exact collision the ledger guard's NOT EXISTS
    conjunct enforces the premise against. The row stays running and
    locked, its charge and started_at intact, the ledger untouched.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    await _create_effects_table(deps)

    job_id = await _enqueue(backend)
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    worker_id, claimed = await _claim(backend, deps, schema)
    row = claimed[0]
    assert row.attempt == enqueued.attempt + 1, "the premise: the claim charged the attempt"
    assert row.started_at is not None, "the premise: the claim stamped started_at"

    # The attempt's ledger row exists (the isolate-self write's shape: the
    # consumer wrote its own attempt row before the body finished).
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".job_attempts '  # noqa: S608  # Why: the schema identifier is fixture-owned; every value is $-bound.
            "(job_id, attempt, started_at, finished_at, outcome, worker_id, metadata) "
            "VALUES ($1, $2, clock_timestamp(), NULL, NULL, $3, '{}'::jsonb)",
            job_id,
            row.attempt,
            worker_id,
        )
    ledger_before = await _attempt_rows(deps, schema, job_id)
    assert [int(a["attempt"]) for a in ledger_before] == [row.attempt], (
        "fixture broken: the ledger row must be the current attempt's"
    )

    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 0, (
        "a row whose current attempt has a ledger row is not the drain's "
        f"refund population, got {drained} re-pended rows"
    )
    after = await _job_row(deps, schema, job_id)
    assert after["status"] == "running" and after["locked_by"] == str(worker_id), (
        f"the drain must leave the row for its own writer's race, got {after}"
    )
    assert after["attempt"] == row.attempt, "the refund would erase the executed attempt's charge"
    ledger_after = await _attempt_rows(deps, schema, job_id)
    assert ledger_after == ledger_before, "the ledger must be untouched"
