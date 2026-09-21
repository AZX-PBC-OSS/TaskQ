"""isolate_self's reclaims must reach the programmatic reclaim feed.

``poll_reclaim_events`` (and ``watch_reclaims``, its streaming consumer)
tails the ``job_events`` slice ``kind='state_change'`` with detail
``reason='lock_expired'``. The leader's reclaim sweep writes that event
for every row it transitions; ``isolate_self`` (a worker walking away
after heartbeat loss) reclaims the same kind of rows and must ride the
same channel, otherwise a consumer fanning out on the feed counts an
isolate-reclaimed job outstanding forever while the row, the attempt
history and the admin views all agree it is finished.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq.backend._sweeps import sweep_expired_locks
from taskq.testing.pg import setup_running_job
from taskq.worker.heartbeat import isolate_self

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


async def test_isolate_reaches_poll_reclaim_events(clean_jobs_app: "JobsApp") -> None:
    """An isolate-reclaimed job is delivered by ``poll_reclaim_events``
    with cause 'isolate_self', while the sweep-reclaimed control job in
    the same run is delivered with its own cause."""
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

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

    # The isolate path reclaims the second worker's job BEFORE the sweep
    # can see it: both rows are lease-expired, so whoever runs first wins.
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": deps.settings.resolved_pg_dsn_direct,
            "TASKQ_SCHEMA_NAME": schema,
        }
    )
    iso_deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=deps.dispatcher_pool,  # type: ignore[arg-type] # Why: isolate_self reads only settings and active_jobs off deps; the pools stay untouched.
        heartbeat_pool=deps.heartbeat_pool,  # type: ignore[arg-type]
        worker_pool=deps.worker_pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    assert iso_deps.active_jobs.held_ids() == []
    shutdown = asyncio.Event()
    await isolate_self(iso_deps, iso_worker, shutdown)
    assert shutdown.is_set()

    # The reclaim itself happened: the row terminalised the way the
    # isolate's crashed arm writes it, so the feed event below is
    # provenance for a transition that actually landed, not an orphan.
    async with deps.worker_pool.acquire() as conn:
        iso_row = await conn.fetchrow(
            f'SELECT status, error_class FROM "{schema}".jobs WHERE id = $1',
            iso_job,
        )
    assert iso_row is not None
    assert iso_row["status"] == "crashed"
    assert iso_row["error_class"] == "HeartbeatLost"

    # Control: the leader sweep reclaims its job.
    async with deps.worker_pool.acquire() as conn:
        n = await sweep_expired_locks(conn, timedelta(0), timedelta(0), schema=schema)
    assert n == 1, "fixture broken: the control sweep must reclaim exactly its own job"

    feed = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
    delivered = {event.job_id: event for event in feed}
    assert sweep_job in delivered, "control: a sweep reclaim must ride the feed"
    assert iso_job in delivered, (
        "an isolate reclaim must ride the reclaim feed like any other: "
        "the row says crashed/HeartbeatLost, the feed must say it too"
    )

    iso_event = delivered[iso_job]
    assert iso_event.detail["reason"] == "lock_expired"
    assert iso_event.detail["cause"] == "isolate_self"
    assert iso_event.detail["to_state"] == "crashed"
    assert iso_event.detail["from_state"] == "running"
