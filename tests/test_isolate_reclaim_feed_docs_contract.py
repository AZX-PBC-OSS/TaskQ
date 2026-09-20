# ruff: noqa: S608  # Why: schema name is fixture-generated and validated by the
# migration runner's _IDENT_RE; asyncpg has no parameter binding for identifiers.

"""Heartbeat-loss reclaims are discoverable off the reclaim feed.

`docs/architecture.md`'s reclaim-coverage caveat and `docs/guides/workers.md`'s
isolate paragraph claim, of `isolate_self` (worker heartbeat loss):

* the isolate path writes no `job_events` row, so heartbeat-loss reclaims
  never appear on `Backend.poll_reclaim_events()` / `TaskQ.watch_reclaims()`;
* the isolate path writes a `job_attempts` row carrying
  `error_class='HeartbeatLost'` whichever arm a job lands on;
* `jobs.status='crashed'` with `error_class='HeartbeatLost'` catches the
  terminal arm only: a job with retry budget left re-pends to
  `status='pending'` and keeps whatever `error_class` the row had before;
* the admin UI's Jobs page (`GET /admin/jobs`) reads the same tables.

This pin drives the real isolate path on a live Postgres and asserts what a
consumer and an operator observe: the feed, the operator-queryable
`jobs`/`job_attempts` rows, and the admin page.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

# The admin surface under test lives behind the [fastapi] extra; the
# vault/aws extras legs collect this file without it, so the guard must
# precede every import that can reach taskq.web.admin (and its jinja2
# templates), skipping the module cleanly instead of erroring collection.
pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq._ids import new_base62, new_uuid  # Why: importorskip guard must precede.
from taskq.backend.clock import SystemClock  # Why: importorskip guard must precede.
from taskq.backend.postgres import PostgresBackend  # Why: importorskip guard must precede.
from taskq.migrate import apply_pending  # Why: importorskip guard must precede.
from taskq.settings import WorkerSettings  # Why: importorskip guard must precede.
from taskq.testing.pg import (  # Why: importorskip guard must precede.
    _create_worker,
    create_running_job,
)
from taskq.web.admin import (  # Why: importorskip guard must precede.
    create_router,
    setup_admin_state,
)
from taskq.worker.deps import WorkerDeps  # Why: importorskip guard must precede.
from taskq.worker.heartbeat import isolate_self  # Why: importorskip guard must precede.

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=0)


@asynccontextmanager
async def _open_isolate_world(
    pg_dsn: str,
) -> AsyncGenerator[tuple[str, WorkerDeps, PostgresBackend], None]:
    schema = f"atk_iso_{new_base62()}"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )
    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(_deps_cm(settings))
    backend = PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    try:
        yield schema, deps, backend
    finally:
        await stack.aclose()


@asynccontextmanager
async def _deps_cm(settings: WorkerSettings) -> AsyncGenerator[WorkerDeps]:
    pool = await asyncpg.create_pool(str(settings.pg_dsn), min_size=1, max_size=3)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    try:
        yield deps
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_isolate_heartbeat_loss_is_off_feed_and_discoverable_in_tables(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The isolate re-pend/crash arms leave no reclaim-feed event, write
    HeartbeatLost attempt rows on every arm, and the admin jobs page reads
    the same tables."""
    async with _open_isolate_world(pg_dsn) as (schema, deps, backend):
        conn = await asyncpg.connect(pg_dsn)
        try:
            worker_id = new_uuid()
            await _create_worker(conn, schema, worker_id)

            # Terminal arm (budget exhausted), cancel arm, and a retry arm
            # carrying a prior attempt's error stamp — the row state after a
            # first real failure, re-claimed running.
            job_crash = await create_running_job(
                conn, schema, worker_id, max_attempts=3, attempt=3, retry_kind="transient"
            )
            job_cancel = await create_running_job(
                conn,
                schema,
                worker_id,
                max_attempts=3,
                attempt=1,
                retry_kind="transient",
                cancel_phase=1,
                cancel_requested_at=datetime.now(UTC),
            )
            job_retry = await create_running_job(
                conn, schema, worker_id, max_attempts=3, attempt=1, retry_kind="transient"
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs SET error_class=$2, error_message=$3 WHERE id=$1',
                job_retry,
                "BoomError",
                "earlier attempt failed",
            )

            # Feed consumer running across the whole isolate: poll_reclaim_events
            # is the durable source watch_reclaims polls.
            events_seen: list[object] = []
            stop = asyncio.Event()

            async def feed_consumer() -> None:
                while not stop.is_set():
                    events_seen.extend(await backend.poll_reclaim_events(after_id=0))
                    await asyncio.sleep(0.02)

            consumer = asyncio.create_task(feed_consumer())
            await isolate_self(deps, worker_id, asyncio.Event())
            await asyncio.sleep(0.2)
            stop.set()
            await consumer

            assert events_seen == [], (
                f"the isolate produced {len(events_seen)} reclaim-feed event(s): "
                "a graceful self-isolation must stay off the crash-reclaim feed, "
                "so consumers counting reclaim events toward zero would stall"
            )

            rows = await conn.fetch(
                f'SELECT id, status, error_class, scheduled_at FROM "{schema}".jobs '
                "WHERE id = ANY($1::uuid[])",
                [job_crash, job_cancel, job_retry],
            )
            by_id = {r["id"]: r for r in rows}
            crash, cancel, retry = by_id[job_crash], by_id[job_cancel], by_id[job_retry]

            assert str(crash["status"]) == "crashed", f"the terminal arm landed {crash['status']!r}"
            assert crash["error_class"] == "HeartbeatLost", (
                f"the terminal arm's row carries error_class {crash['error_class']!r}"
            )
            assert str(cancel["status"]) == "cancelled", (
                f"a cancel-in-flight row must terminalise 'cancelled', got {cancel['status']!r}"
            )
            assert str(retry["status"]) == "pending", (
                f"the retry arm landed {retry['status']!r}: a job with budget left "
                "must re-pend, so a status='crashed' query misses it"
            )
            assert retry["error_class"] == "BoomError", (
                f"the re-pended row's error_class moved to {retry['error_class']!r}: "
                "a heartbeat-lost job with retry budget left must keep whatever "
                "error_class the row had before, so the crashed+HeartbeatLost "
                "query catches the terminal arm only"
            )
            assert retry["scheduled_at"] is not None, (
                "the re-pend must reschedule the row on its retry curve"
            )

            attempts = await conn.fetch(
                f'SELECT DISTINCT job_id, error_class FROM "{schema}".job_attempts '
                "WHERE job_id = ANY($1::uuid[]) AND error_class = 'HeartbeatLost'",
                [job_crash, job_cancel, job_retry],
            )
            assert {r["job_id"] for r in attempts} == {job_crash, job_cancel, job_retry}, (
                f"only {[str(r['job_id'])[-6:] for r in attempts]} carry a HeartbeatLost "
                "attempt row: the doc promises the isolate writes one whichever arm "
                "a job lands on, so job_attempts is the complete HeartbeatLost source"
            )

            # The admin surface: the exact GET path the doc names, reading the
            # same tables through the real pool the router queries with.
            # The env vars arm the TaskQSettings.load() cascade inside
            # create_router (dev-environment fail-closed check + DSN); they
            # go through monkeypatch so nothing outlives this test in
            # os.environ -- a raw os.environ write here leaked the
            # atk_iso_* schema name into later in-process settings loads
            # (e.g. the taskq migrate CLI tests), which only fired when a
            # random ordering ran this pin before them.
            monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
            monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)
            monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
            from fastapi import FastAPI
            from fastapi.testclient import TestClient

            # The admin pool must live on the loop the TestClient runs the
            # app on: open it in the app's own lifespan, then point app.state
            # at it (handlers resolve the pool from app.state per request).
            holder: dict[str, asyncpg.Pool] = {}

            @asynccontextmanager
            async def _admin_lifespan(api: object) -> AsyncGenerator[None]:
                holder["pool"] = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
                yield
                await holder["pool"].close()

            app = FastAPI(lifespan=_admin_lifespan)
            app.include_router(create_router(None, schema=schema).router)  # type: ignore[arg-type]
            with TestClient(app) as client:
                bundle = create_router(holder["pool"], schema=schema)  # type: ignore[arg-type]
                setup_admin_state(app, bundle)
                resp = client.get("/jobs")
            assert resp.status_code == 200, (
                f"GET /admin/jobs (the jobs page the doc names, at its mount) "
                f"returned {resp.status_code}: the admin surface the doc names "
                "must render the jobs tables"
            )
            assert "crashed" in resp.text, (
                "the admin jobs page does not render the crashed terminal arm "
                "the doc says is discoverable there"
            )
        finally:
            await conn.close()
