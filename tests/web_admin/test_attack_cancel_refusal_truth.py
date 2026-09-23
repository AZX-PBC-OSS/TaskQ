"""Attack: the admin UI's mutations must show a refusal, never silence.

The cancel button on the job detail page answers a form POST with a 303
back to the same page. When the backend REFUSED the cancel --
``write_cancel_request`` returned False: the row went terminal between
the route's pre-check and the write (a worker finished it first), or a
cancel is already in flight (cancel_phase >= 1, the double-POST shape) --
the redirect carried no trace of the refusal. The operator read "the page
came back" as "the cancel landed", the exact wrong-but-plausible answer
the refused-op contract exists to kill: the UI's success must reflect the
database, and a refusal must be named, not silenced.

The pin runs the real router over a real pool with a backend whose
cancel write refuses, and asserts on the RENDERED page the operator
lands on.
"""

import uuid as uuid_mod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import httpx
import pytest
import pytest_asyncio

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq.backend._protocol import JobId
from taskq.web.admin import create_router, setup_admin_state

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp, ModulePgSchema
else:
    JobsApp = ModulePgSchema = object

pytestmark = pytest.mark.integration


async def _seed_running_job(conn: asyncpg.Connection, schema: str) -> str:
    """One live running job the detail page can render after the redirect."""
    jid = "0199f19a-7c10-7000-8000-000000000001"
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close,
            started_at, metadata, payload_schema_ver, attempt
        ) VALUES (
            $1, 'refusal_probe_actor', 'default', '{{"v": 1}}'::jsonb, 3, 'transient',
            'running'::{schema}.job_status, 0, $2, $3,
            $4, '{{}}'::jsonb, 1, 1
        )""",  # noqa: S608  # Why: schema is fixture-derived and validated; every value is $N-bound.
        uuid_mod.UUID(jid),
        now,
        now + timedelta(hours=1),
        now - timedelta(minutes=1),
    )
    return jid


@pytest_asyncio.fixture
async def refusing_app(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> AsyncIterator[tuple[httpx.AsyncClient, str, list[tuple[str, str | None]]]]:
    """The admin app over the real pool with a refusing cancel write.

    ``backend.get`` answers a RUNNING job row (the route's preflight must
    pass, so the refusal under test is the WRITE's, not the pre-check's
    409) and ``write_cancel_request`` returns False, the backend's
    documented refused-write verdict. Every write attempt is recorded.
    """
    deps, _backend = clean_jobs_app
    schema = module_pg_schema.schema_name

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await _seed_running_job(conn, schema)
    finally:
        await conn.close()

    class _Row:
        status = "running"

    writes: list[tuple[str, str | None]] = []

    class _RefusingBackend:
        async def get(self, job_id: JobId) -> Any:
            return _Row()

        async def write_cancel_request(self, job_id: JobId, reason: str | None) -> bool:
            writes.append((str(job_id), reason))
            return False

    bundle = create_router(deps.worker_pool, schema=schema, backend=_RefusingBackend())
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)  # type: ignore[arg-type]  # Why: ASGITransport takes the ASGI app; FastAPI satisfies it, pyright's protocol view disagrees.
    client = httpx.AsyncClient(transport=transport, base_url="http://atkweb.local")
    try:
        yield client, job_id, writes
    finally:
        await client.aclose()


async def _get_csrf_token(client: httpx.AsyncClient) -> str:
    """GET the queues page to set the CSRF cookie, then return the token."""
    await client.get("/queues")
    token = client.cookies.get("taskq_csrf_token")
    assert token is not None, "the admin app must set the CSRF cookie on a GET"
    return token


async def test_refused_cancel_names_the_refusal_on_the_page_the_operator_lands_on(
    refusing_app: tuple[httpx.AsyncClient, str, list[tuple[str, str | None]]],
) -> None:
    """A refused cancel write must render a refusal, not a bare redirect."""
    client, job_id, writes = refusing_app
    token = await _get_csrf_token(client)
    response = await client.post(
        f"/jobs/{job_id}/cancel",
        data={"csrf_token": token, "reason": "operator stop"},
    )
    assert response.status_code == 303, (
        f"the refused cancel must still land the operator back on the job page, "
        f"got {response.status_code}"
    )
    assert len(writes) == 1, "the route must have attempted exactly one cancel write"
    landed = await client.get(response.headers["location"])
    assert landed.status_code == 200
    text = landed.text.lower()
    assert "not applied" in text or "refused" in text, (
        "the page the operator lands on after a REFUSED cancel write renders no "
        "refusal: silence reads as success (the refused-op contract)"
    )


async def test_a_plain_visit_to_the_job_page_renders_no_refusal_banner(
    refusing_app: tuple[httpx.AsyncClient, str, list[tuple[str, str | None]]],
) -> None:
    """The refusal banner is refusal-keyed, so a plain visit renders none.

    Guards the fix's other half: a banner keyed loosely would lie in the
    opposite direction, calling a job that was never touched a refusal.
    """
    client, job_id, _writes = refusing_app
    landed = await client.get(f"/jobs/{job_id}")
    assert landed.status_code == 200
    assert "not applied" not in landed.text.lower(), (
        "the refusal banner must be keyed to the refusal redirect only; a "
        "plain visit to the job page must not render it"
    )
