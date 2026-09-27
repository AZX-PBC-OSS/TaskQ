# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier, and every value is $-bound.
"""Attack probes for the #337 remainder fix (cascade truth, drilldown authz/data)."""

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import (
    FastAPI,
    HTTPException,
    Request,
)

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.worker._leader_shared import _ARCHIVE_CTE_SQL

pytestmark = pytest.mark.integration


def _make_admin_app(pool: asyncpg.Pool, schema: str, **kwargs: Any) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="", **kwargs)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _plant_job_with_events(conn: asyncpg.Connection, schema: str) -> Any:
    jid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts,
                retry_kind, status, attempt, created_at, scheduled_at, started_at,
                finished_at)
            VALUES ($1, 'atk337', 'atkq', '{{}}'::jsonb, 3, 'transient',
                'succeeded'::{schema}.job_status, 1, $2, $2, $2, $2)""",
        jid,
        now - timedelta(hours=1),
    )
    await conn.execute(
        f"""INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail)
            VALUES ($1, $2, 'state_change', '{{"to": "running"}}'::jsonb),
                   ($1, $3, 'state_change', '{{"to": "succeeded"}}'::jsonb)""",
        jid,
        now - timedelta(hours=1) + timedelta(seconds=1),
        now - timedelta(hours=1) + timedelta(seconds=2),
    )
    return jid


async def test_the_archive_sweep_cascades_job_events_away(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """RED PROOF (attack #3): the production archive sweep destroys the
    archived job's event rows via the jobs FK cascade, so the fix's
    archive-arm claim ('event retention is archive-aligned') is false --
    by the time the detail page's archive arm can serve a job, job_events
    is already empty for it."""
    schema = module_pg_schema.schema_name
    jid = await _plant_job_with_events(clean_pg_conn, schema)

    before = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1', jid
    )
    assert before == 2, "the plant must put events in the ledger"

    # THE production archive sweep, byte-for-byte (the write statement the
    # prune loop runs): move to jobs_archive, then DELETE FROM jobs.
    moved = await clean_pg_conn.fetch(
        _ARCHIVE_CTE_SQL.format(schema=schema),
        "succeeded",
        timedelta(days=365),
        [jid],
        timedelta(0),
    )
    assert [r["cnt"] for r in moved] == [1], "the sweep must archive the planted job"

    archived_row = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', jid
    )
    assert archived_row == 1
    live_row = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', jid
    )
    assert live_row == 0

    after = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1', jid
    )
    assert after == 0, (
        "the sweep's DELETE FROM jobs cascades job_events away (the sweep's own "
        "comment names the cascade; there is no job_events_archive): the archived "
        "job's ledger is EMPTY by the time the detail page's archive arm reads it"
    )


async def test_the_archived_detail_page_renders_the_cascade_truth(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """RED PROOF (attack #3, page level): a job archived by the production
    sweep reaches the detail route's archive arm with an EMPTY ledger, and
    the page renders the misleading 'No events recorded.' -- the exact
    rendering the fix's commit message claims cannot happen."""
    schema = module_pg_schema.schema_name
    jid = await _plant_job_with_events(clean_pg_conn, schema)
    await clean_pg_conn.fetch(
        _ARCHIVE_CTE_SQL.format(schema=schema),
        "succeeded",
        timedelta(days=365),
        [jid],
        timedelta(0),
    )

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/jobs/{jid}")
    assert resp.status_code == 200
    assert "Event Log" in resp.text
    # The honest contract (RED until the page says so): production's
    # archive sweep cascade-removes the events -- the page must say the
    # history was removed AT ARCHIVE, not "No events recorded." (which
    # claims the job never emitted any).
    assert "No events recorded." not in resp.text, (
        "an archived job's events were removed by the sweep's cascade: "
        "'No events recorded.' misreads as the job never emitted any"
    )
    assert "archived" in resp.text.lower()


async def test_batch_detail_requires_auth_when_mounted_with_auth_dependency(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Attack #2 (authz): the drilldown inherits the router-level auth
    dependency. Mounted with a 401-ing dependency, the drilldown must
    401 like every other admin route -- never serve member data."""
    schema = module_pg_schema.schema_name

    async def deny(request: Request) -> None:
        raise HTTPException(status_code=401, detail="unauthenticated")

    app = _make_admin_app(module_pg_pool, schema, auth_dependency=deny)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/batches/{new_uuid()}")
    assert resp.status_code == 401, f"the drilldown served {resp.status_code} without a principal"


async def test_batch_detail_rejects_injection_and_job_ids(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Attack #2: a non-UUID batch id is 422 (FastAPI), a well-formed id
    that is a JOB id (the old link's bug) 404s -- never a data page."""
    schema = module_pg_schema.schema_name
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/batches/'; DROP TABLE batches; --")).status_code == 422
        # A real JOB id on the batch drilldown: 404, not a leak.
        jid = await _plant_job_with_events(clean_pg_conn, schema)
        resp = await client.get(f"/batches/{jid}")
        assert resp.status_code == 404, (
            "a JOB id on the batch drilldown must 404, not render job data"
        )
        batches_left = await clean_pg_conn.fetchval(f'SELECT count(*) FROM "{schema}".batches')
        assert batches_left is not None  # table still exists (no injection)


async def test_batch_detail_metadata_extraction_never_cast_errors(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Attack #2: jobs whose metadata is NOT a JSON object (scalar, array)
    must not break the drilldown's member queries for OTHER batches."""
    schema = module_pg_schema.schema_name
    batch_id = new_uuid()
    now = datetime.now(UTC)
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".batches (id, queue, status, expected_size,
                consecutive_failures, failure_threshold, originating_actor, created_at)
            VALUES ($1, 'etl', 'active', 1, 0, NULL, 'load_data', $2)""",
        batch_id,
        now,
    )
    member = new_uuid()
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts,
                retry_kind, status, attempt, metadata, created_at, scheduled_at)
            VALUES ($1, 'load_data', 'etl', '{{}}'::jsonb, 3, 'transient',
                'pending'::{schema}.job_status, 0,
                '{{"batch_id": "{batch_id}"}}'::jsonb, $2, $2)""",
        member,
        now,
    )
    # Pathological siblings: scalar / array / number metadata.
    for junk in ("42", "[1,2,3]", '"a string"'):
        jid = new_uuid()
        await clean_pg_conn.execute(
            f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts,
                    retry_kind, status, attempt, metadata, created_at, scheduled_at)
                VALUES ($1, 'junk', 'etl', '{{}}'::jsonb, 3, 'transient',
                    'pending'::{schema}.job_status, 0, $2::jsonb, $3, $3)""",
            jid,
            junk,
            now,
        )

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/batches/{batch_id}")
    assert resp.status_code == 200, resp.text
    assert str(member) in resp.text, "the real member must render beside the junk rows"


async def test_batch_member_cap_honesty_at_201(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Attack #2 (200-cap honesty): 201 members -> counts say 201, the
    member table caps at 200 AND the page says the cap bit."""
    schema = module_pg_schema.schema_name
    batch_id = new_uuid()
    now = datetime.now(UTC)
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".batches (id, queue, status, expected_size,
                consecutive_failures, failure_threshold, originating_actor, created_at)
            VALUES ($1, 'etl', 'active', 201, 0, NULL, 'load_data', $2)""",
        batch_id,
        now,
    )
    rows = [
        (
            new_uuid(),
            f'{{"batch_id": "{batch_id}"}}',
            i,
        )
        for i in range(201)
    ]
    await clean_pg_conn.executemany(
        f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts,
                retry_kind, status, attempt, metadata, created_at, scheduled_at)
            VALUES ($1, 'load_data', 'etl', '{{}}'::jsonb, 3, 'transient',
                'pending'::{schema}.job_status, 0, $2::jsonb, $3, $3)""",
        [(jid, meta, now + timedelta(milliseconds=seq)) for jid, meta, seq in rows],
    )

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/batches/{batch_id}")
    assert resp.status_code == 200
    html = resp.text
    assert "201" in html, "the status counts must report the FULL member count"
    assert "200" in html, "the member table renders the capped page size"
    assert "most recent members" in html, "the truncation notice must say the cap bit"
