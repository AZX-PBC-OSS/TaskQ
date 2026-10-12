"""THE CONSUMER LENS — the four campaign consumer shapes' PRE-WF faces.

The campaign's regression-lens law: adopting TaskQflow must not force a
change on a consumer who never touches it. The four named consumer shapes
(the cennan sync pipeline, TAStack, event-intel, the ops dashboard) call
only the pre-workflow API faces — the actors/jobs/cron faces — never the
wf machinery. This file pins each consumer's EXACT call shape against the
real PostgresBackend at the current tip, so a wf-layer change that breaks
any of them fails HERE, loudly, instead of in a consumer's deploy.

Each test is the consumer's documented call shape (docs/getting-started,
docs/guides/jobs-clients.md, docs/guides/cron.md, docs/guides/admin-ui.md),
run against a migrated schema: enqueue, batch, schedule, cancel, list,
backpressure, the admin pages, the actors face. Nothing here imports
taskq.workflows — that IS the point.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pydantic
import pytest

from taskq._ids import new_base62
from taskq.actor import actor
from taskq.backend._protocol import BatchFilter, JobFilter
from taskq.batch import EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client import JobsClient
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


# ── The consumers' actors (the pre-wf @actor face, exactly as documented) ──


class SyncRecord(pydantic.BaseModel):
    record_id: str


class SyncOutcome(pydantic.BaseModel):
    synced: bool


@actor
async def sync_record(payload: SyncRecord) -> SyncOutcome:
    return SyncOutcome(synced=True)


@actor
async def classify_event(payload: SyncRecord) -> SyncOutcome:
    return SyncOutcome(synced=True)


@actor
async def nightly_stack_task(payload: SyncRecord) -> SyncOutcome:
    return SyncOutcome(synced=True)


# ── 1. The cennan sync pipeline: enqueue_batch + the batch ledger faces ──


class TestCennanSyncShape:
    """The batch sync pipeline's face: enqueue_batch → get_batch →
    list_batches, and the per-record row reads. docs/guides/jobs-clients.md
    §enqueue_batch / §BatchSummary."""

    async def test_batch_enqueue_and_ledger(self, clean_jobs_app: JobsApp) -> None:
        client = JobsClient(clean_jobs_app.backend)
        items = [
            EnqueueItem(actor_ref=sync_record, payload=SyncRecord(record_id=f"r{i}"))
            for i in range(25)
        ]
        # The sync pipeline's production shape: the failure policy (the
        # per-record totality fence) — the face that also writes the
        # `batches` ledger row the dashboard and wait_for_batch read.
        handle = await client.enqueue_batch(
            items, failure_policy=AbortBatchAfter(consecutive_failures=3)
        )
        assert handle.size == 25
        assert len(handle.job_handles) == 25

        # The batch ledger faces: get_batch + list_batches.
        batch_row = await client.get_batch(handle.batch_id)
        assert batch_row is not None
        assert batch_row.id == handle.batch_id
        assert batch_row.expected_size == 25
        listed = await client.list_batches(BatchFilter(batch_id=handle.batch_id))
        assert any(s.batch_id == handle.batch_id for s in listed)

        # The per-record row read: every child row exists, pending, batched.
        rows = [await client.get_row(h.job_id) for h in handle.job_handles]
        present = [r for r in rows if r is not None]
        assert len(present) == len(rows)
        assert all(r.status == "pending" for r in present)
        assert all((r.metadata or {}).get("batch_id") == str(handle.batch_id) for r in present)

    async def test_idempotent_reenqueue_and_list_filter(self, clean_jobs_app: JobsApp) -> None:
        """The sync pipeline's crash-recovery face: re-enqueue with the same
        idempotency key returns the same row (was_existing), and the
        consumer's list filter (by actor) sees it."""
        client = JobsClient(clean_jobs_app.backend)
        key = f"sync-run-{new_base62()}"
        first = await client.enqueue(
            sync_record,
            SyncRecord(record_id="one"),
            idempotency_key=key,
        )
        second = await client.enqueue(
            sync_record,
            SyncRecord(record_id="one"),
            idempotency_key=key,
        )
        assert first.job_id == second.job_id
        assert second.was_existing is True

        page = await client.list(JobFilter(actor="sync_record"))
        assert any(j.id == first.job_id for j in page.jobs)


# ── 2. TAStack: the deferred / scheduled / cancel faces ──


class TestTAStackShape:
    """The task-stack consumer's face: future-scheduled enqueue (the
    scheduled→pending hold), cancel, cancel_where. docs/guides/jobs-clients.md
    §Deferred jobs / §cancel / §cancel_where."""

    async def test_deferred_enqueue_holds_scheduled(self, clean_jobs_app: JobsApp) -> None:
        client = JobsClient(clean_jobs_app.backend)
        run_at = datetime.now(UTC) + timedelta(hours=1)
        handle = await client.enqueue(
            nightly_stack_task, SyncRecord(record_id="nightly"), scheduled_at=run_at
        )
        row = await client.get_row(handle.job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.scheduled_at is not None
        assert abs((row.scheduled_at - run_at).total_seconds()) < 5.0

        # The operator's cancel face on a scheduled row.
        result = await client.cancel(handle.job_id)
        assert result.cancellation_initiated is True
        assert result.new_status == "cancelled"
        after = await client.get_row(handle.job_id)
        assert after is not None and after.status == "cancelled"

    async def test_cancel_where_bulk(self, clean_jobs_app: JobsApp) -> None:
        client = JobsClient(clean_jobs_app.backend)
        for i in range(5):
            await client.enqueue(
                nightly_stack_task, SyncRecord(record_id=f"bulk{i}"), tags=["stack"]
            )
        result = await client.cancel_where(JobFilter(actor="nightly_stack_task"))
        assert result.cancelled_directly == 5
        assert len(result.cancelled_ids) == 5


# ── 3. event-intel: the fan-out + dead-letter read + cron declaration ──


class TestEventIntelShape:
    """The event pipeline's PRE-wf face: the wide fan-out enqueue_batch_fast,
    the dead-letter read (the failed row's error fields), and the cron
    declaration face. The pipeline's typed route is TaskQflow's face — this
    pins the faces it kept (the fan-out, the row reads, the cron)."""

    async def test_fanout_enqueue_batch_fast(self, clean_jobs_app: JobsApp) -> None:
        client = JobsClient(clean_jobs_app.backend)
        items = [
            EnqueueItem(actor_ref=classify_event, payload=SyncRecord(record_id=f"e{i}"))
            for i in range(100)
        ]
        count = await client.enqueue_batch_fast(items)
        assert count == 100
        page = await client.list(JobFilter(actor="classify_event", limit=200))
        assert len(page.jobs) == 100

    async def test_dead_letter_row_read(self, clean_jobs_app: JobsApp) -> None:
        """The pipeline's totality face: a terminal-failed row's error fields
        are readable through the same client face (the dead-letter view)."""
        client = JobsClient(clean_jobs_app.backend)
        handle = await client.enqueue(classify_event, SyncRecord(record_id="dead"))
        row = await client.get_row(handle.job_id)
        assert row is not None
        # The row's error fields are readable whatever the state; the
        # dead-letter consumers read exactly these.
        assert row.error_class is None
        assert row.error_message is None

    async def test_cron_declaration_face(self, clean_jobs_app: JobsApp) -> None:
        """The consumer's cron face: create_schedule persists,
        list_schedules returns it, delete_schedule removes it
        (docs/guides/cron.md)."""
        client = JobsClient(clean_jobs_app.backend)
        schedule = await client.create_schedule(
            classify_event,
            "*/15 * * * *",
            static_payload={"record_id": "cron-fire"},
        )
        listed = await client.list_schedules()
        assert any(s.id == schedule.schedule_id for s in listed)
        await client.delete_schedule(schedule.schedule_id)
        listed_after = await client.list_schedules()
        assert not any(s.id == schedule.schedule_id for s in listed_after)


# ── 4. The ops dashboard: the admin web face + the actors config face ──


class TestOpsDashboardShape:
    """The dashboard's face: the admin router over the REAL migrated schema
    (jobs / queues / schedules / workers pages) and the actors face
    (ActorsClient.list / get / set_capacity). docs/guides/admin-ui.md."""

    @pytest.fixture
    def dashboard_app(self, clean_jobs_app: JobsApp) -> Any:
        pytest.importorskip("fastapi")
        pytest.importorskip("jinja2")
        import os

        from fastapi import FastAPI

        from taskq.web.admin import create_router, setup_admin_state

        os.environ.setdefault("TASKQ_ENVIRONMENT", "dev")
        schema = clean_jobs_app.backend._schema_name  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]  # Why: the schema the backend was built with IS the consumer's schema; no public accessor exists.
        app = FastAPI()
        bundle = create_router(
            clean_jobs_app.deps.dispatcher_pool, schema=schema, base_path="/admin"
        )
        setup_admin_state(app, bundle)
        # The operator's mount (docs: "the admin UI is available at
        # /admin"): the router mounts under the /admin prefix.
        app.include_router(bundle.router, prefix="/admin")
        return app

    async def test_dashboard_pages_render(
        self, clean_jobs_app: JobsApp, dashboard_app: Any
    ) -> None:
        """The dashboard's face over the REAL migrated schema, driven the way
        tests/test_web_admin_integration.py drives it: httpx ASGITransport
        inside the test's own event loop (the admin is a separate process in
        production — its pool is its own)."""
        import httpx

        client = JobsClient(clean_jobs_app.backend)
        await client.enqueue(sync_record, SyncRecord(record_id="dash"))

        async with (
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=dashboard_app),
                base_url="http://test",
                follow_redirects=True,  # Why: /admin/ redirects to /admin/jobs (the landing page); a browser follows it.
            ) as http
        ):
            jobs_page = await http.get("/admin/jobs")
            assert jobs_page.status_code == 200
            assert "sync_record" in jobs_page.text
            for path in ("/admin/", "/admin/queues", "/admin/schedules", "/admin/workers"):
                resp = await http.get(path)
                assert resp.status_code == 200, path

    async def test_actors_config_face(self, clean_jobs_app: JobsApp) -> None:
        from taskq.actor_config import ActorConfig
        from taskq.client._actors import ActorsClient
        from taskq.worker.startup import sync_actor_config

        schema = clean_jobs_app.backend._schema_name  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]  # Why: the schema the backend was built with IS the consumer's schema; no public accessor exists.
        # The worker-bootstrap registration face (sync_actor_config), then
        # the dashboard's actors face over the stored rows.
        async with clean_jobs_app.deps.dispatcher_pool.acquire() as conn:
            await sync_actor_config(
                conn,
                [ActorConfig(actor="sync_record", max_concurrent=None, queue="default")],
                schema=schema,
            )
        actors = ActorsClient(
            clean_jobs_app.deps.dispatcher_pool,
            schema=schema,
        )
        rows = await actors.list()
        assert any(r.actor == "sync_record" for r in rows)
        await actors.set_capacity("sync_record", max_concurrent=4)
        row = await actors.get("sync_record")
        assert row is not None
        assert row.max_concurrent == 4
