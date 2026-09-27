# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier, and every value is $-bound.
"""End-to-end test for the batch drilldown page (issue #337): real Postgres.

The drilldown's member queries are hand-written SQL over the
``metadata->>'batch_id'`` expression (the shape
``jobs_batch_open_members_idx`` is built on, bound as the text the
extraction yields -- a uuid bind there would be a type mismatch), so the
unit tier's stubs cannot vouch for it. This pin runs the real router over
a real migrated schema and asserts on the rendered page: the batch's own
facts, the member status counts, the member links, and the archived
members that have left the live jobs table.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state

pytestmark = pytest.mark.integration


async def _seed_batch(conn: asyncpg.Connection, schema: str) -> Any:
    """One active batch: two live members, one archived out of the table."""
    batch_id = new_uuid()
    member_pending, member_running, member_archived = new_uuid(), new_uuid(), new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".batches (
                id, queue, status, expected_size, consecutive_failures,
                failure_threshold, originating_actor, created_at)
            VALUES ($1, 'etl', 'active', 3, 0, NULL, 'load_data', $2)""",
        batch_id,
        now - timedelta(minutes=5),
    )
    for jid, status, attempt in (
        (member_pending, "pending", 0),
        (member_running, "running", 1),
    ):
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs (
                    id, actor, queue, payload, payload_schema_ver, max_attempts,
                    retry_kind, status, priority, attempt, metadata,
                    created_at, scheduled_at)
                VALUES ($1, 'load_data', 'etl', '{{}}'::jsonb, 1, 3,
                    'transient', $2::{schema}.job_status, 0, $3,
                    $4::jsonb, $5, $5)""",
            jid,
            status,
            attempt,
            f'{{"batch_id": "{batch_id}"}}',
            now - timedelta(minutes=4),
        )
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs_archive (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                attempt, metadata, created_at, scheduled_at, started_at,
                finished_at, archived_at, expire_at)
            VALUES ($1, 'load_data', 'etl', '{{}}'::jsonb, 3, 'transient',
                'succeeded'::{schema}.job_status, 1, $2::jsonb, $3, $3, $3,
                $3, $4, $5)""",
        member_archived,
        f'{{"batch_id": "{batch_id}"}}',
        now - timedelta(minutes=6),
        now - timedelta(minutes=5),
        now + timedelta(days=365),
    )
    return batch_id


def _make_admin_app(pool: asyncpg.Pool, schema: str) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def test_batch_detail_renders_members_counts_and_archived_members(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    batch_id = await _seed_batch(clean_pg_conn, schema)

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/batches/{batch_id}")
    assert resp.status_code == 200
    html = resp.text
    # The batch's own facts.
    assert "etl" in html
    assert "load_data" in html
    # The member status counts: pending and running members alive.
    assert "pending" in html and "running" in html
    # The live members link to their job pages.
    assert "/jobs/" in html
    # The archived member is counted, not invisible.
    assert "1 archived member" in html


async def test_batch_detail_unknown_batch_returns_404(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/batches/{new_uuid()}")
    assert resp.status_code == 404


async def test_batch_detail_unknown_param_returns_400(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/batches/{new_uuid()}?bogus=1")
    assert resp.status_code == 400
    assert "bogus" in resp.text
