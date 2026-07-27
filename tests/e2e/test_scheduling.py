"""Scheduling e2e — delayed dispatch, queue routing, retry backoff spacing.

Closes the scheduling coverage gaps in
docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md (the spec covers
dispatch, retries, and cancellation; these three scenarios pin the timing
behaviour underneath them), with semantics verified against the library:

- **Future ``scheduled_at``**: the enqueue INSERT lands the row in
  ``scheduled`` when ``COALESCE($14, clock_timestamp()) > clock_timestamp()``
  (``backend/_sql_templates.py``). The leader's ``_scheduled_wake_loop``
  (1s cadence, ``worker/leader.py``) promotes it to ``pending`` only once
  ``scheduled_at <= clock_timestamp()``
  (``backend/_sweeps.py.sweep_scheduled_to_pending``), and dispatch stamps
  ``started_at = clock_timestamp()`` at claim time
  (``backend/_dispatch_sql.py``) — so ``started_at >= scheduled_at`` must
  hold (a small epsilon covers client-vs-PG clock skew: ``scheduled_at``
  is minted by the test process, ``started_at`` by PG).
- **Queue routing**: the worker consumes only ``TASKQ_QUEUES=e2e``; the
  dispatch CTE claims rows whose ``jobs.queue`` matches the worker's queues.
  A job enqueued on another queue stays ``pending`` indefinitely, and
  ``handle.cancel()`` on it exercises the pre-dispatch path
  (``cancel_pending_scheduled``, ``backend/postgres.py.write_cancel_request``),
  which transitions ``pending``/``scheduled`` straight to ``cancelled``.
- **Retry backoff is real**: ``sync_user_profile`` declares
  ``RetryPolicy(base=200ms)`` with the ``exponential``/``jitter=0.2``
  defaults, so attempt 1's retry delay is 200ms * U(0.8, 1.2) ∈ [160, 240]ms
  (``retry.py.compute_backoff``). ``mark_retry`` stores
  ``scheduled_at = now + delay`` and the sweep cannot promote early, so the
  gap between the ``job_attempts.started_at`` of attempts 1 and 2 (both
  PG-side ``clock_timestamp()`` values, single clock) must be >= ~150ms — a
  generous lower bound under the 160ms theoretical minimum. An
  instant-retry regression would land well under it.

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from .actors import (
    SyncUserProfilePayload,
    WelcomeEmailPayload,
    send_welcome_email,
    sync_user_profile,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

# Scheduled-dispatch knobs: a 3s delay leaves a ~2s margin after the 1s
# negative-observation window, so an early dispatch can never hide behind
# client-vs-PG clock skew. The 1s epsilon on the started_at comparison
# absorbs that same skew (both containers share the host clock; real skew ~0).
_SCHEDULE_DELAY = timedelta(seconds=3)
_PRE_DISPATCH_WINDOW = 1.0
_DISPATCH_EPSILON = timedelta(seconds=1)

# Unconsumed-queue negative-observation window: 6+ polls at 0.5s over 3s —
# long enough that a routing regression (any consumer claiming the row) could
# not slip between polls, short enough to keep the module fast.
_UNCONSUMED_WINDOW = 3.0
_UNCONSUMED_INTERVAL = 0.5

# Backoff lower bound: theoretical minimum spacing is base*2^0*(1-jitter)
# = 200ms*0.8 = 160ms; 150ms keeps a 10ms margin below it.
_MIN_RETRY_SPACING = timedelta(milliseconds=150)

# Backoff soft upper bound (F7): catches an extreme compute_backoff
# regression (e.g. hours) that a lower-bound-only assertion would pass.
# Generous on purpose — the leader sweep's 1s cadence legitimately
# stretches the top end.
_MAX_RETRY_SPACING = timedelta(seconds=30)


async def _observe_statuses(
    read: Callable[[], Awaitable[str]],
    *,
    window: float,
    interval: float,
) -> list[str]:
    """Poll *read* every *interval* for *window* seconds; return all values.

    Bounded negative-assertion evidence (same poll → sleep → deadline style
    as ``_assertions.poll_until``): every observed value is returned so the
    caller can assert a condition held continuously across the whole window
    rather than at a single instant.
    """
    observed: list[str] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + window
    while True:
        observed.append(await read())
        remaining = deadline - loop.time()
        if remaining <= 0:
            return observed
        await asyncio.sleep(min(interval, remaining))


async def test_scheduled_job_dispatches_after_delay(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """``scheduled_at = now + 3s`` → scheduled (never early), then dispatched.

    (a) For ~1s after enqueue (~2s before the scheduled instant) every
    ``handle.status()`` poll reads ``scheduled`` — the job cannot be running
    or succeeded before its time. (b) The job then completes via the real
    sweep → dispatch path. (c) The terminal row's ``started_at`` (stamped by
    dispatch) is at/after ``scheduled_at`` — dispatch honored the delay.
    """
    scheduled_at = datetime.now(UTC) + _SCHEDULE_DELAY
    handle = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(
            run_id=run_id,
            user_id="u-sched",
            email="u-sched@example.com",
        ),
        scheduled_at=scheduled_at,
    )

    observed = await _observe_statuses(handle.status, window=_PRE_DISPATCH_WINDOW, interval=0.1)
    assert len(observed) >= 2
    assert set(observed) == {"scheduled"}

    await handle.wait(timeout=30)

    job = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, scheduled_at, started_at
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert job is not None
    assert job["status"] == "succeeded"
    started_at: datetime | None = job["started_at"]
    assert started_at is not None
    stored_scheduled_at: datetime = job["scheduled_at"]
    assert started_at >= stored_scheduled_at - _DISPATCH_EPSILON


async def test_unconsumed_queue_stays_pending(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Enqueue on ``other-queue`` (no consumer) → pending forever → cancel.

    The enqueue-time ``queue=`` override lands verbatim on the row (routing
    is by row, not by actor registration), and with only the ``e2e`` queue
    consumed the row is never claimed: every poll of the jobs row over ~3s
    reads ``pending``. The cleanup cancel then proves the pre-dispatch cancel
    path (``cancel_pending_scheduled``) transitions it to ``cancelled``
    without any worker involvement — and leaves ``clean_e2e_state`` with
    nothing in flight.
    """
    handle = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(
            run_id=run_id,
            user_id="u-queue",
            email="u-queue@example.com",
        ),
        queue="other-queue",
    )

    async def _row_status() -> str:
        status = await e2e_pg_pool.fetchval(
            f"""
            SELECT status FROM "{e2e_schema.schema_name}".jobs WHERE id = $1
            """,
            handle.job_id,
        )
        assert status is not None  # row exists for the whole window
        return str(status)

    observed = await _observe_statuses(
        _row_status, window=_UNCONSUMED_WINDOW, interval=_UNCONSUMED_INTERVAL
    )
    assert len(observed) >= 2
    assert set(observed) == {"pending"}

    result = await handle.cancel(reason="cleanup")
    assert result.cancellation_initiated
    assert result.previous_status == "pending"
    assert result.new_status == "cancelled"

    job = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, queue
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert job is not None
    assert job["status"] == "cancelled"
    assert job["queue"] == "other-queue"


async def test_retry_backoff_spacing(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Transient fail on attempt 1 → attempt 2 starts >= ~150ms later.

    Exactly two dispatches happen (fail_times=1, then success), so
    ``job_attempts`` holds two rows. Their ``started_at`` spacing is bounded
    below by the retry delay (>= 160ms theory; 150ms asserted): the retry
    went through the ``scheduled`` state and waited for its backoff, it did
    not re-dispatch instantly. A soft 30s upper bound catches extreme
    backoff regressions the lower bound alone would pass.
    """
    handle = await e2e_client.enqueue(
        sync_user_profile,
        SyncUserProfilePayload(
            run_id=run_id,
            user_id="u-retry",
            fail_times=1,
            fail_kind="transient",
        ),
    )

    await handle.wait(timeout=30)

    attempts = await e2e_pg_pool.fetch(
        f"""
        SELECT attempt, started_at
        FROM "{e2e_schema.schema_name}".job_attempts
        WHERE job_id = $1
        ORDER BY attempt
        """,
        handle.job_id,
    )
    assert [row["attempt"] for row in attempts] == [1, 2]
    first_started: datetime = attempts[0]["started_at"]
    second_started: datetime = attempts[1]["started_at"]
    spacing = second_started - first_started
    assert spacing >= _MIN_RETRY_SPACING, (
        f"retry spacing {spacing.total_seconds() * 1000:.1f}ms below the "
        f"{_MIN_RETRY_SPACING.total_seconds() * 1000:.0f}ms backoff floor — "
        "instant-retry regression?"
    )
    # Soft upper bound: extreme backoff regression (e.g. hours) would pass
    # the lower-bound assertion alone. Generous because the leader sweep's
    # 1s cadence legitimately stretches the top end.
    assert spacing < _MAX_RETRY_SPACING, (
        f"retry spacing {spacing.total_seconds():.1f}s exceeds the "
        f"{_MAX_RETRY_SPACING.total_seconds():.0f}s soft ceiling — "
        "extreme backoff regression?"
    )
