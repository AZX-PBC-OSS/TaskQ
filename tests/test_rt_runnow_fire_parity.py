"""Red-team parity pins for the admin run-now fire path (real PG).

Run-now (``POST /schedules/{id}/run``) is a fire path for a schedule: it
resolves the schedule's payload and enqueues a job for its actor.  The
docs (``docs/guides/cron.md``) state that cron fires honor the actor's
``max_pending`` cap exactly like client enqueues, and every other fire
path stamps ``metadata["cron_schedule_id"]`` provenance (the tick's
``_plan_fire`` does; the twin-coverage walk and per-schedule attribution
read it back).  Run-now did neither: it passed ``max_pending=None`` (the
single-enqueue path enforces only the CARRIED cap, so the operator's
stored ``actor_config.max_pending`` was silently bypassed) and an empty
metadata dict (the job was unattributable to its schedule).

These pins drive the real FastAPI route against a real Postgres schema
through httpx's ASGI transport (one event loop for the pool and the app,
no TestClient loop split).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

import asyncpg
import httpx
import pytest

from taskq.testing.fixtures import ModulePgSchema

from .test_rt_cron_harness import (
    cron_settings,
    pool_backend,
    seed_schedule,
    server_hour_floor,
)

pytestmark = pytest.mark.integration

_ACTOR = "rt_runnow_actor"


@pytest.fixture(autouse=True)
def _dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed implicitly by the test runner via parameter injection.
    """The web_admin suite's env (its autouse is path-gated to that directory)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")


@asynccontextmanager
async def _admin_client(
    module_pg_schema: ModulePgSchema,
) -> AsyncGenerator[httpx.AsyncClient]:
    """The admin app wired to a real pool and a real pool-backed backend."""
    settings = cron_settings(module_pg_schema.schema_name)
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    try:
        from fastapi import FastAPI

        from taskq.web.admin import create_router, setup_admin_state

        backend = pool_backend(settings, pool)
        bundle = create_router(pool, schema=module_pg_schema.schema_name, backend=backend)
        app = FastAPI()
        setup_admin_state(app, bundle)
        app.include_router(bundle.router)
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver", follow_redirects=False
        ) as client:
            await client.get("/queues")  # arms the CSRF cookie
            yield client
    finally:
        await pool.close()


async def _run_now(client: httpx.AsyncClient, schedule_id: UUID) -> httpx.Response:
    token = client.cookies.get("taskq_csrf_token", "")
    return await client.post(f"/schedules/{schedule_id}/run", data={"csrf_token": token})


async def _seed_runnow_schedule(
    conn: asyncpg.Connection,
    schema: str,
    *,
    max_pending: int | None = None,
) -> UUID:
    """One schedule on an actor whose stored cap (if any) is *max_pending*."""
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
        "(actor, queue, max_attempts, retry_kind, max_pending) "
        "VALUES ($1, 'rt_queue', 5, 'transient', $2)",
        _ACTOR,
        max_pending,
    )
    due = await server_hour_floor(conn)
    return await seed_schedule(
        conn,
        schema,
        actor=_ACTOR,
        name="runnow",
        cron_expr="0 * * * *",
        next_fire_at=due,
    )


class TestRunNowMaxPendingParity:
    """The stored ``actor_config.max_pending`` cap bounds run-now too."""

    async def test_run_now_respects_the_stored_max_pending_cap(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A stored cap of 0 (the emergency drain) refuses run-now.

        The client enqueue path and the cron tick both honor the stored
        cap (resolved over the registry literal); run-now resolved
        nothing, so before the fix an operator pressing run-now during a
        drain landed a job past their own cap.
        """
        schema = module_pg_schema.schema_name
        schedule_id = await _seed_runnow_schedule(clean_pg_conn, schema, max_pending=0)
        async with _admin_client(module_pg_schema) as client:
            resp = await _run_now(client, schedule_id)
        assert resp.status_code == 303, f"run-now must redirect, got {resp.status_code}"
        assert "max_pending" in resp.headers.get("location", ""), (
            f"the redirect must name the cap refusal, got {resp.headers.get('location')!r}"
        )
        jobs = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; actor is $-bound.
            _ACTOR,
        )
        assert jobs == 0, f"the drain cap must keep run-now from landing a job, got {jobs}"

    async def test_run_now_within_cap_fires_normally(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """No stored cap: run-now still fires (guard against overfix)."""
        schema = module_pg_schema.schema_name
        schedule_id = await _seed_runnow_schedule(clean_pg_conn, schema, max_pending=None)
        async with _admin_client(module_pg_schema) as client:
            resp = await _run_now(client, schedule_id)
        assert resp.status_code == 303
        assert "error" not in resp.headers.get("location", "")
        job = await clean_pg_conn.fetchrow(
            f"SELECT queue, max_attempts, metadata->>'cron_schedule_id' AS provenance "  # noqa: S608  # Why: schema is a test-fixture identifier; actor is $-bound.
            f'FROM "{schema}".jobs WHERE actor = $1',
            _ACTOR,
        )
        assert job is not None, "run-now must land exactly one job"
        assert job["queue"] == "rt_queue"
        assert job["max_attempts"] == 5


class TestRunNowProvenance:
    """A run-now job is attributable to its schedule."""

    async def test_run_now_stamps_cron_schedule_id_provenance(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The job's metadata carries ``cron_schedule_id`` like every other
        fire of a schedule: the tick stamps it (``_plan_fire``), the
        ``allof`` twin-coverage walk scopes delivered instants by it, and
        per-schedule attribution reads it.  An empty metadata dict made
        the run-now job invisible to all three.
        """
        schema = module_pg_schema.schema_name
        schedule_id = await _seed_runnow_schedule(clean_pg_conn, schema, max_pending=None)
        async with _admin_client(module_pg_schema) as client:
            resp = await _run_now(client, schedule_id)
        assert resp.status_code == 303
        provenance = await clean_pg_conn.fetchval(
            f"SELECT metadata->>'cron_schedule_id' FROM \"{schema}\".jobs "  # noqa: S608  # Why: schema is a test-fixture identifier; actor is $-bound.
            "WHERE actor = $1",
            _ACTOR,
        )
        assert provenance == str(schedule_id), (
            f"the run-now job must carry its schedule id as provenance, got {provenance!r}"
        )
