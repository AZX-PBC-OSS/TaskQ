"""Tests for the admin UI actors page and deregister route.

Follows the pattern of tests/test_web_admin_integration.py: a per-test
asyncpg pool on the module's migrated schema, a FastAPI app built via
create_router + setup_admin_state + include_router, and httpx.AsyncClient
with ASGITransport.

CSRF uses the synchronizer-token pattern: GET sets the taskq_csrf_token
cookie; POST must include it as the csrf_token form field.
"""

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from taskq.actor_config import ActorConfig
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest_asyncio.fixture
async def admin_pool(module_pg_schema: ModulePgSchema) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        yield pool
    finally:
        await pool.close()


def _make_admin_app(
    pool: asyncpg.Pool,
    schema: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    admin_actions_enabled: bool,
) -> FastAPI:
    # setenv must precede create_router — it calls TaskQSettings.load() internally
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true" if admin_actions_enabled else "false")
    bundle = create_router(pool, schema=schema, base_path="/admin")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    return app


async def _seed_actor_config(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    queue: str = "default",
) -> None:
    await sync_actor_config(
        conn,
        [ActorConfig(actor=actor, max_concurrent=1, queue=queue)],
        schema=schema,
    )


async def _get_csrf_then_post(
    app: FastAPI,
    get_url: str,
    post_url: str,
    data: dict[str, str] | None = None,
) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=False,
    ) as client:
        get_resp = await client.get(get_url)
        assert get_resp.status_code == 200
        csrf_token = get_resp.cookies.get("taskq_csrf_token", "")
        assert csrf_token, "GET must set the taskq_csrf_token cookie"
        return await client.post(post_url, data={"csrf_token": csrf_token, **(data or {})})


async def test_actors_page_lists_actor_config_rows(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "test-actor-1", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=True)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/admin/actors")

    assert resp.status_code == 200
    assert "test-actor-1" in resp.text
    assert "default" in resp.text


async def test_actors_page_shows_deregister_form(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "button-actor", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=True)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/admin/actors")

    assert resp.status_code == 200
    # Targeted assertions — check for the specific form action and form fields,
    # not just the word "Deregister" which could appear anywhere.
    assert 'action="/admin/actors/button-actor/deregister"' in resp.text
    assert 'name="force"' in resp.text
    assert 'name="purge_queue"' in resp.text


async def test_deregister_route_returns_403_when_admin_actions_disabled(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "disabled-actor", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=False)

    resp = await _get_csrf_then_post(
        app, "/admin/actors", "/admin/actors/disabled-actor/deregister"
    )

    assert resp.status_code == 403
    count = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        "disabled-actor",
    )
    assert count == 1


async def test_deregister_route_succeeds_for_clean_actor(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "clean-deregister-actor", queue="default")
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=True)

    resp = await _get_csrf_then_post(
        app, "/admin/actors", "/admin/actors/clean-deregister-actor/deregister"
    )

    assert resp.status_code == 303
    assert "/actors" in resp.headers["location"]

    count = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        "clean-deregister-actor",
    )
    assert count == 0


async def test_deregister_route_returns_409_when_actor_has_active_jobs(
    clean_pg_conn: asyncpg.Connection,
    admin_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST deregister with an active job returns 409 Conflict."""
    from uuid import uuid4

    schema = module_pg_schema.schema_name
    await _seed_actor_config(clean_pg_conn, schema, "blocked-actor", queue="default")
    # Insert a pending job so force=False deregistration refuses.
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, max_attempts, retry_kind) '
        f"VALUES ($1, 'blocked-actor', 'default', '{{}}'::jsonb, 'pending'::\"{schema}\".job_status, 3, 'transient')",
        uuid4(),
    )
    app = _make_admin_app(admin_pool, schema, monkeypatch, admin_actions_enabled=True)

    resp = await _get_csrf_then_post(
        app, "/admin/actors", "/admin/actors/blocked-actor/deregister"
    )

    assert resp.status_code == 409
    assert "non-terminal" in resp.text
    # Row must still exist — deregistration was refused.
    count = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        "blocked-actor",
    )
    assert count == 1
