"""ATTACK tests for hunt/fence-gaps (PR 374): the retry fence.

An operator cancel in flight must WIN over a failure retry: the retry's
re-pend resets the cancel columns and its deadline arm stamps
DeadlineExceeded, so a phase-carrying row must never reach either arm.
Every pin asserts behavior on the public surfaces (the job read model,
the event log, the delivered cancel flags), on PG and on the in-memory
twin. A failure is a RED.
"""

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq.backend._protocol import ErrorInfo, JobFilter
from taskq.exceptions import WorkerOwnershipMismatch

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration

_BOOM = ErrorInfo(error_class="Boom", error_message="m", error_traceback=None)
_ONE_S = timedelta(seconds=1)


def _outcome(row: object) -> tuple[str, str | None]:
    return row.status, row.error_class  # type: ignore[attr-defined]


async def test_operator_cancel_racing_a_failure_retry(
    clean_jobs_app: "JobsApp",
) -> None:
    """RACE, looped: the operator's write_cancel_request vs the worker's
    mark_failed_or_retry, same claimed job.

    Observable invariant, every iteration: whichever write lands first,
    the operator's request is never laundered. The job ends either still
    'running' with the request visibly armed, or 'cancelled' with the
    origin marker on it. A job that relaunches (scheduled/pending) or
    terminalises failed/DeadlineExceeded after a successful cancel stamp
    is a RED: the operator asked for a cancel and the retry erased it.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    from taskq.testing.pg import setup_running_job

    cancel_first = 0
    retry_first = 0
    for i in range(60):
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, attempt=1, max_attempts=3)

        # Alternate the head start so both writers really contend.
        cancel_head = 0.04 if i % 2 == 0 else 0.0
        retry_head = 0.04 if i % 2 else 0.0

        async def _cancel(
            _job_id: object = job_id,
            _head: float = cancel_head,
        ) -> bool:
            await asyncio.sleep(_head)
            return await backend.write_cancel_request(_job_id, "operator")  # type: ignore[arg-type]

        async def _retry(
            _job_id: object = job_id,
            _worker_id: object = worker_id,
            _head: float = retry_head,
        ) -> None:
            await asyncio.sleep(_head)
            # The fenced-out retry reads back as the handler's no-op.
            with contextlib.suppress(WorkerOwnershipMismatch):
                await backend.mark_failed_or_retry(  # type: ignore[arg-type]
                    _job_id, _worker_id, _BOOM, _ONE_S, attempt=1
                )

        stamp_applied, _ = await asyncio.gather(_cancel(), _retry())

        rows = await backend.list_jobs(JobFilter())
        mine = [row for row in rows if row.id == job_id]
        assert len(mine) == 1, f"RED iter {i}: the job vanished from the list"
        row = mine[0]

        assert row.status in {"running", "cancelled"}, (
            f"RED iter {i}: after a successful cancel stamp the job relaunched "
            f"or failed: status={row.status!r} (the operator's request was "
            "laundered by the retry's re-pend)"
        )
        if row.status == "running":
            cancel_first += 1
            assert stamp_applied, (
                f"RED iter {i}: the job is running but no cancel is armed and "
                "no retry landed either"
            )
            assert row.cancel_requested_at is not None, (
                f"RED iter {i}: the armed cancel request is no longer visible on the job"
            )
            assert row.error_class != "DeadlineExceeded", (
                f"RED iter {i}: a running job cannot carry a deadline stamp"
            )
        else:
            retry_first += 1
            assert stamp_applied, f"RED iter {i}: the retry won but the cancel reported failure"
            assert row.error_class is not None, (
                f"RED iter {i}: a cancelled job carries no origin marker"
            )
            assert row.error_class != "DeadlineExceeded", (
                f"RED iter {i}: the retry's deadline arm stamped "
                "DeadlineExceeded over an operator cancel in flight"
            )
    assert cancel_first > 0 and retry_first > 0, (
        "attack broken: the harness never raced both orders"
    )


async def test_fenced_retry_keeps_the_cancel_deliverable(
    clean_jobs_app: "JobsApp",
) -> None:
    """After a fenced-out retry, the cancel-poll surface still delivers the
    armed request to the worker holding the job: the ladder stays armed,
    the cancel is never lost."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    from taskq.testing.pg import setup_running_job

    for i in range(10):
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema, attempt=1, max_attempts=3)
        applied = await backend.write_cancel_request(job_id, "operator")
        assert applied
        with contextlib.suppress(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(job_id, worker_id, _BOOM, _ONE_S, attempt=1)
            pytest.fail(f"RED iter {i}: a phase-carrying row reached the retry arm")
        flags = await backend.poll_cancel_flags(worker_id)
        assert any(flag.job_id == job_id for flag in flags), (
            f"RED iter {i}: an armed cancel stopped being delivered after a fenced-out retry"
        )


async def test_cancel_origin_truth_table_differential_pg_vs_memory(
    clean_jobs_app: "JobsApp",
) -> None:
    """Differential: the same row states through mark_failed_or_retry must
    produce the SAME observable outcome on both backends.

    Matrix (retry_delay=1s): phase 0 x schedule_to_close {past, future,
    none}; plus phase 1 and 2 carrying rows; plus the terminal-fail arm
    (retry_delay=None) on a phase-1 row, deliberately UNFENCED (a terminal
    fail writes no re-pend, the cancel columns stay as the audit trail).
    """
    app = clean_jobs_app
    start = datetime(2025, 1, 1, tzinfo=UTC)

    async def pg_case(
        phase: int, stc: datetime | None, delay: timedelta | None
    ) -> tuple[str, str | None]:
        from taskq.testing.pg import setup_running_job

        schema = app.deps.settings.schema_name
        async with app.deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                attempt=1,
                max_attempts=3,
                cancel_phase=phase,
                cancel_requested_at=datetime.now(UTC) if phase else None,
                schedule_to_close=stc,
            )
        try:
            row = await app.backend.mark_failed_or_retry(job_id, worker_id, _BOOM, delay, attempt=1)
        except WorkerOwnershipMismatch:
            return ("mismatch", None)
        return _outcome(row)

    # The in-memory twin runs on the same event loop as the test; drive it
    # inline (no run_until_complete) instead:
    async def memory_case_async(
        phase: int, stc: datetime | None, delay: timedelta | None
    ) -> tuple[str, str | None]:
        from dataclasses import replace

        from taskq._ids import new_job_id
        from taskq.backend._protocol import EnqueueArgs
        from taskq.testing.clock import FakeClock as _Fake
        from taskq.testing.in_memory import InMemoryBackend as _Twin

        backend = _Twin(clock=_Fake(start))
        backend.register_actor_config(actor="attack_actor")
        args = EnqueueArgs(
            id=new_job_id(),
            actor="attack_actor",
            queue="default",
            payload={"k": "v"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=start,
        )
        await backend.enqueue(args)
        worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access for SETUP, the assertions stay public
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert len(dispatched) == 1
        jid = dispatched[0].id
        # SETUP only (the house pattern): the deadline and the cancel stamp
        # are row states the fixtures cannot express through enqueue; every
        # assertion below stays on the public surface.
        row = backend._jobs[jid]  # type: ignore[reportPrivateUsage]
        backend._jobs[jid] = replace(  # type: ignore[reportPrivateUsage]
            row,
            schedule_to_close=stc,
            cancel_phase=phase,
            cancel_requested_at=start if phase else None,
        )
        try:
            row = await backend.mark_failed_or_retry(jid, worker_id, _BOOM, delay, attempt=1)
        except WorkerOwnershipMismatch:
            return ("mismatch", None)
        return _outcome(row)

    # The deadline is a LOGICAL state resolved against each backend's own
    # clock (PG's wall clock vs the twin's FakeClock): 'past' and 'future'
    # mean the same thing on both sides, an absolute 2025 timestamp would
    # only measure the harness's clock skew.
    past, future, none = -1.0, 3600.0, None
    matrix = [
        # (phase, schedule_to_close offset (s from now), retry_delay)
        (0, future, _ONE_S),  # clean row: retried
        (0, past, _ONE_S),  # clean row past deadline: DeadlineExceeded
        (0, none, _ONE_S),  # clean row, no deadline: retried
        (1, future, _ONE_S),  # cancel in flight: fenced on BOTH
        (1, past, _ONE_S),  # fenced even past the deadline
        (2, future, _ONE_S),  # forced interrupt: fenced
        (1, future, None),  # terminal fail arm: NOT fenced
    ]
    for phase, stc_offset, delay in matrix:
        pg_stc = (
            datetime.now(UTC) + timedelta(seconds=stc_offset) if stc_offset is not None else None
        )
        twin_stc = start + timedelta(seconds=stc_offset) if stc_offset is not None else None
        pg = await pg_case(phase, pg_stc, delay)
        twin = await memory_case_async(phase, twin_stc, delay)
        assert pg == twin, (
            f"RED: the backends disagree at phase={phase} "
            f"stc={'past' if stc_offset is not None and stc_offset < 0 else 'future' if stc_offset else 'none'} "
            f"delay={delay}: PG={pg} twin={twin}"
        )
        if phase and delay is not None:
            assert pg == ("mismatch", None), (
                f"RED: a phase-carrying row reached a retry arm on "
                f"{'PG' if pg != ('mismatch', None) else 'the twin'}: {pg}"
            )


async def test_retry_after_cancel_terminal_is_a_clean_noop(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: retry after the cancel ladder terminalised the job. The
    write must refuse (mismatch), and the job must keep its cancelled
    outcome and origin."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    from taskq.testing.pg import setup_running_job

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema, attempt=1, max_attempts=3)
    assert await backend.write_cancel_request(job_id, "operator")
    await backend.write_cancel_escalation(job_id, worker_id, 2)
    assert await backend.mark_cancelled(job_id, worker_id, attempt=1)

    with pytest.raises(WorkerOwnershipMismatch):
        await backend.mark_failed_or_retry(job_id, worker_id, _BOOM, _ONE_S, attempt=1)

    rows = await backend.list_jobs(JobFilter())
    mine = [row for row in rows if row.id == job_id]
    assert len(mine) == 1
    row = mine[0]
    assert row.status == "cancelled", "RED: a terminal job was resurrected"
    assert row.error_class is not None, "RED: the cancel origin was erased"
    # The operator's request survives as the audit trail on the public read.
    assert row.cancel_requested_at is not None


async def test_retry_without_the_attempt_epoch_never_lands(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: a retry that cannot present the attempt epoch never
    reaches any arm, clean row or not."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    from taskq.testing.pg import setup_running_job

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema, attempt=1, max_attempts=3)
    with pytest.raises(WorkerOwnershipMismatch):
        await backend.mark_failed_or_retry(job_id, worker_id, _BOOM, _ONE_S, attempt=None)
    rows = await backend.list_jobs(JobFilter())
    mine = [row for row in rows if row.id == job_id]
    assert len(mine) == 1 and mine[0].status == "running"
