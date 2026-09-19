"""A dispatch claim writes no durable event rows.

A claim (pending→running) is the dispatcher's bookkeeping, not an outcome
transition, and it is the one act every admission-denial cycle repeats:
under the 429 denial contract a denied job is claimed and rescheduled until
capacity frees or its ``schedule_to_close`` expires, so one ``job_events``
row per claim is exactly the unbounded-growth vector the aggregated denial
counters on the job row (``rate_limit_blocked_count`` / ``snooze_count``)
replaced.

These tests pin the contract at the real dispatch seam - a batch of claims
lands the rows ``running`` with the attempt increment and writes ZERO
``job_events`` rows - and pin the other half of the diet with them: the
transitions of record survive, so the execution that follows a claim still
leaves exactly its terminal state_change. (Rewritten from the superseded
claim-event pins: those pinned one event row per claim, the shape the
denial-events decision abolished.)
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._sql_templates import render
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker


def test_schema_is_still_validated_by_render() -> None:
    with pytest.raises(ValueError, match=r"[Ii]nvalid"):
        render('evil"; DROP SCHEMA public CASCADE; --')


@pytest.mark.integration
async def test_dispatch_writes_no_event_rows(clean_jobs_app: JobsApp) -> None:
    """A claim must net zero durable rows: every job dispatches to
    ``running`` with its attempt incremented, and ``job_events`` stays
    empty - any per-claim insert, in any shape, fails here."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_ids = [new_uuid() for _ in range(5)]
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier from the module fixture.
            "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
            "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'pending', 3, 'transient', "
            "clock_timestamp() FROM unnest($1::uuid[]) AS t(id)",
            job_ids,
        )

    rows = await backend.dispatch_batch(
        worker_id,
        ["default"],
        limit=len(job_ids),
        lock_lease=timedelta(seconds=deps.settings.lock_lease),
    )

    assert {r.id for r in rows} == set(job_ids), "all five jobs must dispatch"
    assert all(r.status == "running" for r in rows)
    assert all(r.attempt == 1 for r in rows)
    async with deps.worker_pool.acquire() as conn:
        written: int = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
    assert written == 0, (
        f"a claim wrote {written} job_events rows for {len(job_ids)} dispatches - "
        "a row per claim is the unbounded-growth vector under sustained "
        "admission denial; contention belongs on the row's aggregated "
        "denial counters, not in the event log"
    )


@pytest.mark.integration
async def test_executed_job_leaves_exactly_its_terminal_event(
    clean_jobs_app: JobsApp,
) -> None:
    """The diet cut bookkeeping, not transitions: a job claimed and then
    executed to success has exactly one event row, the terminal
    running→succeeded transition of record."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier from the module fixture.
            "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
            "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 'pending', 3, 'transient', "
            "clock_timestamp())",
            job_id,
        )

    rows = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=deps.settings.lock_lease)
    )
    assert len(rows) == 1
    ok = await backend.mark_succeeded(job_id, worker_id, result={"ok": True}, attempt=1)
    assert ok is True

    async with deps.worker_pool.acquire() as conn:
        events = await conn.fetch(
            f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',  # noqa: S608  # Why: schema is a test-fixture identifier.
            job_id,
        )
    assert len(events) == 1, (
        "an executed job's whole event record is its terminal transition - "
        "no claim bookkeeping row, and the terminal row must not be lost"
    )
    assert events[0]["kind"] == "state_change"
    detail = parse_detail(events[0]["detail"])
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "succeeded"
