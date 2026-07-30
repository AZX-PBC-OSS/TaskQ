"""Retries & failure-taxonomy e2e — transient retry, permanent fail, snooze requeue.

Scenario:
transient ``fail_times=2`` with ``max_attempts=3`` → succeeded on attempt 3;
permanent → ``failed`` (``pytest.raises(JobFailed)`` on ``handle.wait()``);
snooze → completes after requeue. Ground truth: ``jobs.attempt``,
``jobs.error_class``, effects attempt numbers.

Attempt-counter semantics, verified against the library (not guessed):

- The dispatch CTE increments the counter on every dispatch
  (``attempt = j.attempt + 1`` in ``backend/_dispatch_sql.py``) and the
  actor's ``ctx.attempt`` is that post-increment value (``worker/dispatch.py``).
  ``mark_succeeded``/``mark_failed`` leave ``attempt`` untouched
  (``backend/_sql_templates.py``), so a terminal row's ``attempt`` is exactly
  the number of dispatches the job went through.
- ``mark_snoozed`` deliberately leaves ``attempt`` unchanged and bumps
  ``max_attempts = j.max_attempts + 1`` — "Snooze does not consume retry
  budget" (``backend/_sql_templates.py``). The snoozed row lands in
  ``scheduled`` and is re-queued by the leader's ``scheduled_to_pending``
  sweep (~1 s cadence, ``worker/leader.py``).
- ``non_retryable_exceptions`` classify at the first failure:
  ``RetryClassifier.decide`` returns ``Fail`` on isinstance (``retry.py``),
  and the terminal write records ``error_class = type(exc).__name__``
  (``worker/_handlers.py``).
- ``handle.wait()`` raises ``JobFailed`` carrying the terminal row for any
  non-success status (``client/_handle.py._extract_result``); the row is
  inspectable via ``exceptions.py.JobFailed.row``.

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pytest

from taskq import JobFailed

from ._assertions import fetch_effects
from .actors import SyncUserProfilePayload, sync_user_profile

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


def _sync_payload(
    run_id: str,
    *,
    fail_times: int,
    fail_kind: Literal["transient", "permanent", "snooze"],
) -> SyncUserProfilePayload:
    """Deterministic failure-injection payload (each test mints its own
    ``run_id`` via fixture, so effects never correlate across tests)."""
    return SyncUserProfilePayload(
        run_id=run_id,
        user_id="u-1",
        fail_times=fail_times,
        fail_kind=fail_kind,
    )


async def test_transient_failure_retries_then_succeeds(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Transient RuntimeError x2 with max_attempts=3 → success on dispatch 3.

    Each retry is a real re-dispatch across the container boundary
    (``mark_retry`` → ``scheduled`` → leader sweep → dispatch), so the
    effects stream shows exactly three ``fetch`` rows with attempts 1 → 2 → 3
    from the same job, and the terminal jobs row kept the post-increment
    counter at 3.
    """
    handle = await e2e_client.enqueue(
        sync_user_profile,
        _sync_payload(run_id, fail_times=2, fail_kind="transient"),
    )

    await handle.wait(timeout=60)

    fetch_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="fetch")
    assert [row["attempt"] for row in fetch_rows] == [1, 2, 3]
    assert {row["job_id"] for row in fetch_rows} == {handle.job_id}

    synced_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="synced")
    assert len(synced_rows) == 1
    assert synced_rows[0]["attempt"] == 3

    job = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, attempt
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert job is not None
    assert job["status"] == "succeeded"
    assert job["attempt"] == 3


async def test_permanent_failure_lands_failed_without_retry(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """PermanentSyncError is in ``non_retryable_exceptions`` → failed on attempt 1.

    No retry dispatch happens: exactly one ``fetch`` effect, zero ``synced``.
    ``handle.wait()`` raises ``JobFailed`` whose row exposes the terminal
    status and the recorded error class; the jobs row matches.
    """
    handle = await e2e_client.enqueue(
        sync_user_profile,
        _sync_payload(run_id, fail_times=1, fail_kind="permanent"),
    )

    with pytest.raises(JobFailed) as exc_info:
        await handle.wait(timeout=60)
    assert exc_info.value.row.status == "failed"
    assert exc_info.value.row.error_class == "PermanentSyncError"

    fetch_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="fetch")
    assert [row["attempt"] for row in fetch_rows] == [1]
    assert await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="synced") == []

    job = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, attempt, error_class
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert job is not None
    assert job["status"] == "failed"
    assert job["attempt"] == 1
    assert job["error_class"] == "PermanentSyncError"


async def test_snooze_requeues_then_succeeds(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Snooze(200ms) on attempt 1 → requeue via ``scheduled`` → success on attempt 2.

    The snooze does not consume retry budget: ``mark_snoozed`` leaves
    ``attempt`` unchanged (so the second dispatch increments it to 2) and
    refunds ``max_attempts`` (+1) in the same UPDATE. The ``synced`` effect's
    attempt number (2) proves success happened only on the post-snooze
    dispatch — never on the snoozed one.
    """
    handle = await e2e_client.enqueue(
        sync_user_profile,
        _sync_payload(run_id, fail_times=1, fail_kind="snooze"),
    )

    await handle.wait(timeout=60)

    fetch_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="fetch")
    assert [row["attempt"] for row in fetch_rows] == [1, 2]

    synced_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="synced")
    assert len(synced_rows) == 1
    assert synced_rows[0]["attempt"] == 2

    job = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, attempt, max_attempts
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert job is not None
    assert job["status"] == "succeeded"
    assert job["attempt"] == 2
    # Snooze refunded the budget: 3 declared + 1 refund from mark_snoozed.
    assert job["max_attempts"] == 4
