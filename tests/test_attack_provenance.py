"""ATTACK tests for hunt/terminal-provenance (PR 375).

Every test here is an executed attack against the pushed branch state,
asserted through the public surfaces a client/consumer/operator observes:
the job read model (``list_jobs``), the attempt history (``get_attempts``),
the event log (``get_events``), the reclaim feed (``poll_reclaim_events``)
and cancellation visibility (``poll_cancel_flags``). A failure is a RED.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EventRow, JobFilter, JobId
from taskq.backend._sweeps import sweep_expired_locks
from taskq.constants import (
    CANCEL_ORIGIN_COOPERATIVE,
    CANCEL_ORIGIN_FORCED,
    CANCEL_ORIGIN_UNREQUESTED,
)
from taskq.settings import WorkerSettings
from taskq.testing.pg import setup_running_job
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import isolate_self

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration

# The documented origin truth table: phase 2 interrupt -> forced; a request
# on the row -> cooperative; neither -> unrequested (the runtime's own
# cancel, no operator request may be forged). The origin is what the
# operator reads off the job and its history.
FORCED = CANCEL_ORIGIN_FORCED
COOPERATIVE = CANCEL_ORIGIN_COOPERATIVE
UNREQUESTED = CANCEL_ORIGIN_UNREQUESTED


async def _running_row(
    conn: object,
    schema: str,
    *,
    cancel_phase: int = 0,
    cancel_requested_at: datetime | None = None,
    attempt: int = 1,
    max_attempts: int = 3,
) -> tuple[UUID, UUID]:
    worker_id, job_id = await setup_running_job(
        conn,  # type: ignore[arg-type]
        schema,
        worker_id=new_uuid(),
        cancel_phase=cancel_phase,
        cancel_requested_at=cancel_requested_at,
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
    )
    return worker_id, job_id


def _iso_deps(app: "JobsApp") -> WorkerDeps:

    deps = app.deps
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": deps.settings.resolved_pg_dsn_direct,
            "TASKQ_SCHEMA_NAME": deps.settings.schema_name,
        }
    )
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=deps.dispatcher_pool,  # type: ignore[arg-type]
        heartbeat_pool=deps.heartbeat_pool,  # type: ignore[arg-type]
        worker_pool=deps.worker_pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def _observable_state(
    app: "JobsApp", job_id: JobId
) -> tuple[str, str | None, datetime | None]:
    """The public read model of one job: (status, origin marker, cancel
    request visibility)."""
    rows = await app.backend.list_jobs(JobFilter())
    mine = [row for row in rows if row.id == job_id]
    assert len(mine) == 1, f"attack broken: job {job_id} not in the public list"
    row = mine[0]
    return row.status, row.error_class, row.cancel_requested_at


async def _isolate_feed_events(app: "JobsApp", job_id: JobId) -> list[EventRow]:
    """The reclaim feed's deliveries for one job (public feed contract)."""
    feed = await app.backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
    return [event for event in feed if event.job_id == job_id]


async def test_cancel_origin_truth_table_all_phases(clean_jobs_app: "JobsApp") -> None:
    """Cancel in every phase x request evidence: the origin the operator
    reads off the job, its attempt history and its event log is the origin
    the row's own evidence proves. This is the PR's core claim."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    now = datetime.now(UTC)
    matrix = [
        # (cancel_phase, cancel_requested_at, expected origin)
        (0, None, UNREQUESTED),
        (1, now, COOPERATIVE),
        (2, now, FORCED),
    ]
    for phase, requested, expected in matrix:
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await _running_row(
                conn,
                schema,
                cancel_phase=phase,
                cancel_requested_at=requested,
            )
            applied = await clean_jobs_app.backend.mark_cancelled(job_id, worker_id, attempt=1)
            assert applied, f"fixture broken at phase {phase}: cancel did not apply"

        status, origin, _ = await _observable_state(clean_jobs_app, job_id)
        assert status == "cancelled"
        assert origin == expected, (
            f"RED: phase={phase} requested={requested is not None}: the job "
            f"reports origin {origin!r}, its own evidence proves {expected!r}"
        )
        # The attempt history and the event log tell the same story as the
        # job: three surfaces, one origin.
        attempts = await clean_jobs_app.backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].error_class == expected, (
            f"RED: the attempt history disagrees with the job's origin at phase {phase}"
        )
        events = await clean_jobs_app.backend.get_events(job_id)
        cancel_events = [
            event
            for event in events
            if event.kind == "state_change" and event.detail.get("to_state") == "cancelled"
        ]
        assert len(cancel_events) == 1
        assert cancel_events[0].detail["error_class"] == expected, (
            f"RED: the event log disagrees with the job's origin at phase {phase}"
        )


async def test_phase2_without_request_is_not_forged_cooperative(
    clean_jobs_app: "JobsApp",
) -> None:
    """A row escalated to phase 2 with no request stamp carries no request
    evidence; the operator must not read a cooperative cancel off it."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await _running_row(
            conn, schema, cancel_phase=2, cancel_requested_at=None
        )
        await clean_jobs_app.backend.mark_cancelled(job_id, worker_id, attempt=1)

    _status, origin, _ = await _observable_state(clean_jobs_app, job_id)
    assert origin == FORCED, (
        f"RED: a phase-2 row with no request reports {origin!r}; the "
        "interrupt evidence outranks the missing request"
    )


async def test_terminal_write_cannot_resurrect_a_cancelled_row(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: cancel after terminal. mark_cancelled on a succeeded job
    is a clean no-op (False), the job keeps its terminal outcome."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await _running_row(conn, schema)
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'succeeded',"  # Why: fixture setup, not assertion; schema is fixture-derived and _IDENT_RE-validated.
            " finished_at = clock_timestamp(), locked_by_worker = NULL,"
            " lock_expires_at = NULL WHERE id = $1",
            job_id,
        )

    applied = await clean_jobs_app.backend.mark_cancelled(job_id, worker_id, attempt=1)
    assert applied is False, "RED: mark_cancelled terminalised a succeeded job"

    status, origin, _ = await _observable_state(clean_jobs_app, job_id)
    assert status == "succeeded", "RED: the terminal outcome was corrupted"
    assert origin is None


async def test_mark_cancelled_attempt_mismatch_is_a_noop(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: a caller that cannot present the attempt epoch never
    terminalises the job, and the operator's cancel stays visible: the
    cancel-poll surface keeps delivering it."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await _running_row(
            conn, schema, cancel_phase=1, cancel_requested_at=datetime.now(UTC)
        )
    # attempt=None: never matches, exactly as PG's NULL bind never satisfies
    # the equality.
    applied = await clean_jobs_app.backend.mark_cancelled(job_id, worker_id, attempt=None)
    assert applied is False, "RED: the attempt fence did not hold"

    status, _origin, cancel_visible = await _observable_state(clean_jobs_app, job_id)
    assert status == "running", "RED: a fenced-out job terminalised"
    assert cancel_visible is not None, "RED: the operator's cancel request vanished"
    # Cancellation visibility: the cancel-poll surface still hands the
    # request to the worker holding the job.
    flags = await clean_jobs_app.backend.poll_cancel_flags(worker_id)
    assert any(flag.job_id == job_id for flag in flags), (
        "RED: an armed cancel stopped being delivered to the worker"
    )


async def test_isolate_self_zero_running_jobs_is_a_clean_noop(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: isolate with zero running jobs signals shutdown, reports
    no jobs anywhere, and the reclaim feed stays empty."""
    shutdown = asyncio.Event()
    await isolate_self(_iso_deps(clean_jobs_app), new_uuid(), shutdown)
    assert shutdown.is_set()

    rows = await clean_jobs_app.backend.list_jobs(JobFilter())
    assert rows == [], "RED: an empty isolate manufactured jobs"
    feed = await clean_jobs_app.backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
    assert feed == [], "RED: an empty isolate emitted reclaim feed events"


async def test_isolate_self_racing_a_terminal_write(
    clean_jobs_app: "JobsApp",
) -> None:
    """RACE, looped: isolate_self (crash reclaim) vs the worker's own
    mark_cancelled terminal write, same job, same worker.

    Observable invariants under arbitration:
    - the job ends in exactly one legal state (never half-written);
    - the attempt history records the epoch exactly once;
    - the reclaim feed carries an isolate event iff the job was
      isolate-reclaimed (a feed event for a job the terminal write owns is
      a phantom; a missing one orphans every feed consumer);
    - every job is always in exactly one of queued / running / terminal.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    terminal_won = 0
    isolate_won = 0
    for i in range(40):
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await _running_row(conn, schema, attempt=1, max_attempts=3)

        # Alternate the head start so both arbiters really contend: a
        # harness that always lets one side win is not a race.
        terminal_head_start = 0.05 if i % 2 == 0 else 0.0
        isolate_head_start = 0.05 if i % 2 else 0.0

        async def _terminal(
            _job_id: JobId = job_id,
            _worker_id: UUID = worker_id,
            _head_start: float = terminal_head_start,
        ) -> None:
            await asyncio.sleep(_head_start)
            # The worker runtime's own unrequested cancel: phase 0, no request.
            await backend.mark_cancelled(_job_id, _worker_id, attempt=1)

        async def _isolate(
            _worker_id: UUID = worker_id,
            _head_start: float = isolate_head_start,
        ) -> None:
            await asyncio.sleep(_head_start)
            await isolate_self(_iso_deps(clean_jobs_app), _worker_id, asyncio.Event())

        await asyncio.gather(_terminal(), _isolate())

        status, _origin, _requested = await _observable_state(clean_jobs_app, job_id)
        # ── Partition invariant: queued / running / terminal ──
        assert status in {
            "pending",
            "scheduled",
            "running",
            "crashed",
            "cancelled",
        }, f"RED iter {i}: job observed in an illegal state {status!r}"

        attempts = await backend.get_attempts(job_id)
        feed_events = await _isolate_feed_events(clean_jobs_app, job_id)

        if status == "cancelled":
            terminal_won += 1
            assert feed_events == [], (
                f"RED iter {i}: the terminal write owns the outcome but the "
                "feed still advertises an isolate reclaim for the job"
            )
        else:
            # isolate won (its guarded UPDATE is the arbiter): crashed or
            # re-pended with the budget arm.
            isolate_won += 1
            assert status in {"crashed", "pending"}, (
                f"RED iter {i}: the isolate owns the outcome but the job reads {status!r}"
            )
            assert len(feed_events) == 1, (
                f"RED iter {i}: an isolate-owned reclaim delivered "
                f"{len(feed_events)} feed events for one job"
            )
            assert feed_events[0].detail["cause"] == "isolate_self"
        assert len(attempts) == 1, (
            f"RED iter {i}: the attempt history shows {len(attempts)} records "
            "for one attempt (a lost or duplicated write)"
        )
    # Both arbiters must have actually raced; a harness that only ever picks
    # one winner is not a race.
    assert terminal_won > 0, "attack broken: mark_cancelled never won the race"
    assert isolate_won > 0, "attack broken: isolate_self never won the race"


async def test_isolate_self_racing_the_reclaim_sweep(
    clean_jobs_app: "JobsApp",
) -> None:
    """RACE: isolate_self vs the leader's reclaim sweep, both arbiters on
    the same lease-expired job. Exactly one wins; the loser must not crash,
    duplicate the attempt history, or double-deliver the feed event."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    expired = datetime.now(UTC) - timedelta(seconds=5)

    for i in range(25):
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                retry_kind="non_retryable",
                attempt=3,
                max_attempts=3,
                lock_expires_at=expired,
            )

        async def _sweep() -> None:
            async with deps.worker_pool.acquire() as sweep_conn:
                await sweep_expired_locks(sweep_conn, timedelta(0), timedelta(0), schema=schema)

        await asyncio.gather(
            isolate_self(_iso_deps(clean_jobs_app), worker_id, asyncio.Event()),  # type: ignore[arg-type]
            _sweep(),
        )

        status, _origin, _requested = await _observable_state(clean_jobs_app, job_id)
        assert status == "crashed", (
            f"RED iter {i}: an exhausted non_retryable job must read crashed "
            f"under either arbiter, got {status!r}"
        )
        attempts = await clean_jobs_app.backend.get_attempts(job_id)
        assert len(attempts) == 1, (
            f"RED iter {i}: the attempt history shows {len(attempts)} records "
            "for one attempt (a double write)"
        )
        feed = await clean_jobs_app.backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        delivered = [event for event in feed if event.job_id == job_id]
        assert len(delivered) == 1, (
            f"RED iter {i}: the feed delivered {len(delivered)} reclaim events "
            "for one reclaim (a consumer double-counts the job as outstanding)"
        )


async def test_double_isolate_delivers_one_event_per_job(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: isolate twice, concurrently, same worker. The job still
    terminalises once and the feed delivers one event."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(
            conn,
            schema,
            retry_kind="non_retryable",
            attempt=3,
            max_attempts=3,
        )

    await asyncio.gather(
        isolate_self(_iso_deps(clean_jobs_app), worker_id, asyncio.Event()),  # type: ignore[arg-type]
        isolate_self(_iso_deps(clean_jobs_app), worker_id, asyncio.Event()),  # type: ignore[arg-type]
    )

    status, _origin, _requested = await _observable_state(clean_jobs_app, job_id)
    assert status == "crashed"
    attempts = await clean_jobs_app.backend.get_attempts(job_id)
    assert len(attempts) == 1, "RED: the attempt history recorded the epoch twice"
    feed_events = await _isolate_feed_events(clean_jobs_app, job_id)
    assert len(feed_events) == 1, (
        f"RED: a double isolate delivered {len(feed_events)} reclaim events for one job"
    )


async def test_sweep_and_isolate_feed_event_contents_agree(
    clean_jobs_app: "JobsApp",
) -> None:
    """Feed contract differential: every path that reclaims a job delivers
    the same channel key (the slice feed consumers tail) and a truthful
    cause naming the path; the delivered to_state matches the job's fate."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    expired = datetime.now(UTC) - timedelta(seconds=5)

    async with deps.worker_pool.acquire() as conn:
        _sweep_worker, sweep_job = await setup_running_job(
            conn,
            schema,
            retry_kind="non_retryable",
            attempt=3,
            max_attempts=3,
            lock_expires_at=expired,
        )
        iso_worker, iso_job = await setup_running_job(
            conn,
            schema,
            retry_kind="non_retryable",
            attempt=3,
            max_attempts=3,
            lock_expires_at=expired,
        )

    await isolate_self(_iso_deps(clean_jobs_app), iso_worker, asyncio.Event())  # type: ignore[arg-type]
    async with deps.worker_pool.acquire() as conn:
        n = await sweep_expired_locks(conn, timedelta(0), timedelta(0), schema=schema)
    assert n == 1

    feed = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
    delivered = {event.job_id: event for event in feed}
    assert sweep_job in delivered and iso_job in delivered, (
        "RED: a reclaim path's events are missing from the feed"
    )
    sweep_event, iso_event = delivered[sweep_job], delivered[iso_job]
    for event in (sweep_event, iso_event):
        assert event.detail["reason"] == "lock_expired", (
            "RED: the channel key drifted; feed consumers tail this value and "
            "would never see the event"
        )
        assert event.detail["from_state"] == "running"
        assert event.detail["to_state"] == "crashed"
    assert iso_event.detail["cause"] == "isolate_self", (
        "RED: an isolate event forged the sweep's cause; a consumer cannot "
        "tell which path reclaimed the job"
    )
    assert sweep_event.detail["cause"] in {"lock_expired", "heartbeat_timeout"}, (
        f"RED: sweep cause {sweep_event.detail['cause']!r} names no deadline"
    )
    # And the causes are distinguishable: a consumer must be able to tell
    # the paths apart.
    assert iso_event.detail["cause"] != sweep_event.detail["cause"] or (
        sweep_event.detail["cause"] == "lock_expired"
    )


async def test_fenced_out_row_never_carries_a_cancel_stamp_into_a_retry(
    clean_jobs_app: "JobsApp",
) -> None:
    """The cross-branch invariant (375's isolate arms vs an armed cancel):
    a job carrying a phase-1 operator cancel that isolate reclaims must
    terminalise 'cancelled' (the operator's request honoured) and the feed
    must say so, never re-pended with the request laundered away."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(
            conn,
            schema,
            retry_kind="transient",
            attempt=1,
            max_attempts=3,
            cancel_phase=1,
            cancel_requested_at=datetime.now(UTC),
        )

    await isolate_self(_iso_deps(clean_jobs_app), worker_id, asyncio.Event())

    status, _origin, cancel_visible = await _observable_state(clean_jobs_app, job_id)
    assert status == "cancelled", (
        f"RED: an operator cancel in flight was re-pended ({status!r}) by "
        "the isolate's budget arm, the request was laundered"
    )
    assert cancel_visible is not None, (
        "RED: the terminalised job no longer shows the operator's request"
    )
    feed_events = await _isolate_feed_events(clean_jobs_app, job_id)
    assert len(feed_events) == 1
    assert feed_events[0].detail["to_state"] == "cancelled"
