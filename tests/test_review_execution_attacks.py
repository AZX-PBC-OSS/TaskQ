"""Area-3 review attacks: execution & retry policies.
Probes (each predicts a specific outcome; RED pins get fixed in this branch):

1. THE TAKE-TO-REGISTER WINDOW LEAK: between the progress buffer's install
   and the attempt ``try`` whose ``finally`` is its only removal sit two
   awaits (``active_jobs.register``, the ``running`` publish). A
   cancellation landing there exits ``consume_one_job`` without running the
   removal finally — the exact issue-461 map-hygiene class the install's
   comment says the seam fix closed for the shutdown-seam exit.
2. THE FENCED TERMINAL PATH'S DIRTY CLEAR: ``_run_terminal_path`` clears
   ``dirty`` on whatever buffer the job-id key holds, ungated — the
   autonomous and tx paths both gate the same clear on the write landing.
   A stale attempt whose write was fenced must not clear the live
   attempt's buffer (issue-461 identity-scoping residue).
3. MAX-ATTEMPTS counting at the terminal write, real PG: the classifier's
   ``attempt < max_attempts`` boundary against the claim's ``attempt + 1``
   stamp, asserted from attempt ROWS and final status (behavior), not
   internals.
4. KILL-9 CLASS on real PG: lease-expiry reclaim of a running row —
   executions == max_attempts exactly, the attempt row records the
   WorkerCrashed outcome with the claim-time due_at, and the re-pend
   delay honors the row's curve floor.
"""

# ruff: noqa: S608  Why: schema names are validated by WorkerSettings.post_load and _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches the existing integration-test pattern.

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

if TYPE_CHECKING:
    from taskq.backend._protocol import JobRow
    from taskq.testing.fixtures import JobsApp
    from taskq.worker.cancel import (
        _ActiveJob,  # pyright: ignore[reportPrivateUsage]  # Why: the register override must restate the parent's exact return type; the entry class is the registry's private one.
    )

from taskq._ids import new_uuid
from taskq.backend._protocol import DenialReason, SnoozeOutcome
from taskq.backend.clock import Clock
from taskq.constants import MIN_DEFERRAL_INTERVAL
from taskq.context import JobContext
from taskq.exceptions import Snooze
from taskq.progress._buffer import _ProgressBuffer
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)
from taskq.testing.assertions import assert_job_terminal
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_workered_running_job
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})


class _ParkOnRegisterRegistry(ActiveJobRegistry):
    """A registry whose register parks until the consuming task is cancelled.

    A cancellation delivered while suspended at THIS await lands between the
    buffer install and the attempt try — the window under attack.
    """

    async def register(
        self, job_id: UUID, task: asyncio.Task[object], ctx: JobContext[BaseModel]
    ) -> "_ActiveJob":
        await asyncio.Event().wait()
        raise AssertionError("unreachable: the park is cancelled, never set")


# ── Attack 1: the install-to-try window leak ──────────────────────────


async def test_cancel_in_register_window_does_not_leak_progress_buffer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancel landing on the register await must not strand the buffer."""
    active_jobs = _ParkOnRegisterRegistry()
    backend = FakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    deps = _deps_with_buffers(buffers)

    consuming = asyncio.create_task(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,
            run_actor=_never_actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=active_jobs,
        )
    )
    await asyncio.sleep(0.05)
    assert job.id in buffers, "buffer must be installed before the register await"
    consuming.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consuming

    assert job.id not in buffers, (
        "a cancellation landing between the buffer install and the attempt try "
        "leaks the progress buffer (issue-461 map hygiene)"
    )


async def test_cancel_in_running_publish_window_does_not_leak_progress_buffer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same window's second await: the running state-change publish."""
    from taskq.worker import _consumer as consumer_module

    active_jobs = ActiveJobRegistry()
    backend = FakeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    async def _cancelling_publish(*args: object, **kwargs: object) -> None:
        # A cancellation delivered at THIS await: from the task's frame the
        # delivery shape of an external task.cancel() landing while the
        # consumer is suspended on the running publish is exactly a
        # CancelledError raised here.
        raise asyncio.CancelledError

    monkeypatch.setattr(consumer_module, "_publish_state_change_event", _cancelling_publish)
    deps = _deps_with_buffers(buffers)
    deps.redis_client = object()  # any non-None: the publish arm must fire
    deps.settings = _settings()

    consuming = asyncio.create_task(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,
            run_actor=_never_actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            active_jobs=active_jobs,
        )
    )
    with pytest.raises(asyncio.CancelledError):
        await consuming

    assert job.id not in buffers, (
        "a cancellation landing on the running-publish await leaks both the "
        "progress buffer and (if the buffer does) the active-jobs entry"
    )
    assert active_jobs.get(job.id) is None, "the registry entry leaks with the buffer"


# ── Attack 2: the fenced terminal path's dirty clear ──────────────────


class _FencedSnoozeBackend(FakeBackend):
    """A backend whose snooze write is fenced out (the row moved underneath)."""

    async def mark_snoozed(
        self,
        job_id: UUID,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        outcome: SnoozeOutcome = "snoozed",
        attempt: int | None = None,
        claim_epoch: int | None = None,
        denial_reason: DenialReason = "capacity",
    ) -> Literal["scheduled", "failed", "noop"]:
        return "noop"


async def test_fenced_terminal_path_does_not_clear_live_buffers_dirty_flag() -> None:
    """A stale attempt's fenced write must not touch the key's live buffer.

    Same-worker reclaim installs a NEW buffer at the same job-id key. The
    stale attempt's terminal path reads the key by id, not identity, and
    clears whatever it finds: the live attempt's unflushed progress delta
    then sits hidden from the flush tick until the next ctx.progress call.
    The autonomous and transactional paths gate this clear on the terminal
    write actually landing; the exception-routed path does not.
    """
    backend = _FencedSnoozeBackend()
    clock: Clock = FakeClock(_NOW)
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    deps = _deps_with_buffers(buffers)

    async def actor(running: "JobRow", ctx: JobContext[BaseModel]) -> object:
        await ctx.progress(step=1, detail="stale attempt progress")
        # The reclaim race: a same-worker re-claim installs the LIVE
        # attempt's buffer at the key, with its own unflushed delta.
        live = _ProgressBuffer(job_id=running.id, base_seq=10, attempt=2)
        live.pending_seq_delta = 3
        live.dirty = True
        buffers[running.id] = live
        raise Snooze(timedelta(seconds=5))

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clock,
    )
    assert outcome == "noop"

    live = buffers[job.id]
    assert live is not None
    assert live.dirty is True, (
        "a fenced-out terminal write cleared the LIVE attempt's dirty flag; "
        "its unflushed delta is now invisible to the flush tick"
    )


# ── shared doubles ────────────────────────────────────────────────────


async def _never_actor(running: "JobRow", ctx: JobContext[BaseModel]) -> object:
    raise AssertionError("the actor body must never run in these probes")


def _deps_with_buffers(buffers: dict[UUID, _ProgressBuffer]) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = None
    deps.settings = _settings()
    deps.redis_client = None
    deps.disowned_jobs = set()
    deps.shutdown_started_at = None
    deps.shutdown_phase = None
    deps.pending_publish_tasks = None
    return deps


# ── Attack 3+4: real-PG probes (attempt counting, kill-9 reclaim) ────


class TestAttemptCountingRealPG:
    """The retry budget, asserted from attempt rows and final status."""

    async def test_transient_exhaustion_attempt_rows_exact(self, clean_jobs_app: "JobsApp") -> None:
        """max_attempts=3 always-fail: exactly 3 attempt rows, failed.

        The boundary the off-by-one class lives at: the classifier retries
        while ``attempt < max_attempts`` against the row's 1-indexed
        attempt, so attempt 3 itself must land failed with three ledger
        rows — never a fourth retry, never a third-row failure missing.
        """
        from taskq.backend._protocol import ErrorInfo
        from taskq.retry import JobRetryState, Retry, decide_after_failure

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
        actor_config = default_actor_config()
        assert actor_config.retry == policy

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await create_workered_running_job(
                conn, schema, worker_id=worker_id, max_attempts=3, attempt=1
            )

        seen_attempts: list[int] = []
        while True:
            job = await backend.get(job_id)
            assert job is not None and job.status == "running"
            seen_attempts.append(job.attempt)
            exc = RuntimeError(f"attempt {job.attempt} failed")
            decision = decide_after_failure(
                actor_config,
                exc,
                JobRetryState(
                    attempt=job.attempt,
                    max_attempts=job.max_attempts,
                    retry_kind=job.retry_kind,
                    schedule_to_close=job.schedule_to_close,
                    start_to_close=job.start_to_close,
                ),
            )
            error_info = ErrorInfo(
                error_class=type(exc).__name__, error_message=str(exc), error_traceback=None
            )
            row = await backend.mark_failed_or_retry(
                job_id,
                worker_id,
                error_info,
                decision.retry_delay if isinstance(decision, Retry) else None,
                attempt=job.attempt,
                claim_epoch=job.claim_epoch,
            )
            assert row is not None
            if row.status != "scheduled":
                break
            # The next claim: dispatch stamps attempt + 1 and re-claims.
            async with deps.worker_pool.acquire() as conn:
                await conn.execute(
                    f"""UPDATE "{schema}".jobs
                    SET status = 'running', attempt = $2, claim_epoch = claim_epoch + 1,
                        locked_by_worker = $1, lock_expires_at = now() + interval '60 seconds',
                        started_at = now(), last_heartbeat_at = now()
                    WHERE id = $3""",
                    worker_id,
                    row.attempt + 1,
                    job_id,
                )

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, attempt, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            rows = await conn.fetch(
                f'SELECT attempt, outcome FROM "{schema}".job_attempts WHERE job_id = $1 '
                "ORDER BY attempt",
                job_id,
            )
        assert_job_terminal(row, "failed")
        assert row is not None and row["attempt"] == 3
        assert [r["attempt"] for r in rows] == [1, 2, 3], (
            f"the ledger must carry exactly max_attempts rows, got {[r['attempt'] for r in rows]}"
        )
        assert all(r["outcome"] == "failed" for r in rows)

    async def test_kill9_reclaim_budget_is_exact_and_due_at_is_claim_time(
        self, clean_jobs_app: "JobsApp"
    ) -> None:
        """The SIGKILL class: every execution burns one attempt, the
        attempt row records WorkerCrashed with the CLAIM-time due_at.

        A worker killed mid-execution writes nothing; the sweep's reclaim
        is the only ledger entry. This probe claims, kills (by never
        renewing the lease), sweeps, and re-claims in a loop, asserting:
        exactly max_attempts executions; the final row 'crashed'; every
        reclaimed attempt row carries outcome 'crashed' (the sweep's
        WorkerCrashed record) and a due_at equal to the scheduled_at the
        row was claimed against.
        """
        from datetime import datetime as dt

        from taskq.backend._sweeps import sweep_expired_locks

        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        max_attempts = 2

        # expected due_at per attempt number: the scheduled_at the row
        # carries at the moment of each claim.
        due_at_by_attempt: dict[int, datetime] = {}

        async with deps.worker_pool.acquire() as conn:
            worker2, job_id = await create_workered_running_job(
                conn, schema, worker_id=worker_id, max_attempts=max_attempts, attempt=1
            )
            due_at_by_attempt[1] = await conn.fetchval(
                f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
            )

        executions = 0
        while True:
            executions += 1
            # ...the worker is SIGKILLed here: no heartbeat, no terminal
            # write, the lease lapses.
            async with deps.worker_pool.acquire() as conn:
                await conn.execute(
                    f"""UPDATE "{schema}".jobs
                    SET lock_expires_at = now() - interval '1 second'
                    WHERE id = $1 AND status = 'running'""",
                    job_id,
                )
                reclaimed = await sweep_expired_locks(
                    conn, timedelta(0), timedelta(0), schema=schema
                )
                assert reclaimed == 1
                row = await conn.fetchrow(
                    f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
                )
                assert row is not None
                if row["status"] != "pending":
                    break
                # The surviving worker re-claims: attempt + 1. The
                # re-pend arm stamped a new scheduled_at; that is the
                # claim-time due time THIS next attempt is taken against.
                due_at_by_attempt[row["attempt"] + 1] = await conn.fetchval(
                    f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
                )
                await conn.execute(
                    f"""UPDATE "{schema}".jobs
                    SET status = 'running', attempt = $2, claim_epoch = claim_epoch + 1,
                        locked_by_worker = $3, lock_expires_at = now() + interval '60 seconds',
                        started_at = now(), last_heartbeat_at = now()
                    WHERE id = $1""",
                    job_id,
                    row["attempt"] + 1,
                    worker2,
                )

        assert row is not None and row["status"] == "crashed"
        # The off-by-one probe: exactly max_attempts executions of the body.
        assert executions == max_attempts, (
            f"kill-9 reclaim cycle executed the body {executions} times "
            f"against max_attempts={max_attempts}"
        )
        async with deps.worker_pool.acquire() as conn:
            rows = await conn.fetch(
                f'SELECT attempt, outcome, error_class, due_at FROM "{schema}".job_attempts '
                "WHERE job_id = $1 ORDER BY attempt",
                job_id,
            )
        assert [r["attempt"] for r in rows] == [1, 2]
        assert all(r["outcome"] == "crashed" for r in rows)
        assert all(r["error_class"] == "WorkerCrashed" for r in rows)
        for r in rows:
            due_at: dt | None = r["due_at"]
            expected: datetime | None = due_at_by_attempt[r["attempt"]]
            assert expected is not None
            assert due_at is not None, "the reclaimed attempt must stamp the claim-time due_at"
            assert due_at is not None and expected is not None
            delta = abs((due_at - expected).total_seconds())
            assert delta < 5.0, (
                f"due_at {due_at} is not the claim-time scheduled_at "
                f"{expected} for attempt {r['attempt']} (delta {delta}s)"
            )

    async def test_retry_write_floors_degenerate_delay_at_deferral_floor(
        self, clean_jobs_app: "JobsApp"
    ) -> None:
        """The backend write arm's defense-in-depth floor: a caller that
        bypasses the classifier's MIN_DEFERRAL_INTERVAL (direct-SQL parity,
        an older worker) still cannot requeue below the floor.
        """

        from taskq.backend._protocol import ErrorInfo

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            _, job_id = await create_workered_running_job(
                conn, schema, worker_id=worker_id, max_attempts=3, attempt=1
            )
            before = await conn.fetchval("SELECT clock_timestamp()")

        current = await backend.get(job_id)
        assert current is not None
        row = await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            ErrorInfo(error_class="E", error_message="m", error_traceback=None),
            timedelta(0),  # the degenerate delay the classifier would have floored
            attempt=1,
            claim_epoch=current.claim_epoch,
        )
        assert row is not None and row.status == "scheduled"
        async with deps.worker_pool.acquire() as conn:
            after = await conn.fetchval("SELECT clock_timestamp()")
            stamped = await conn.fetchval(
                f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        delay = (stamped - after).total_seconds()
        floor = MIN_DEFERRAL_INTERVAL.total_seconds()
        assert delay >= floor - 0.05, (
            f"scheduled_at {stamped} is {delay}s out; the write arm must floor "
            f"the requeue delay at {floor}s (stamped after {after})"
        )
        assert (stamped - before).total_seconds() >= floor - 0.05
