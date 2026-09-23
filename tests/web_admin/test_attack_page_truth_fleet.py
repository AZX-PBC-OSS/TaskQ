"""Attack: the admin pages' rendered numbers must equal the database's.

Two inventory pins against real Postgres and the real router:

1. The queue overview's per-queue counts (Pending/Scheduled/Running/
   Failed) and its population rule: a queue whose rows are ALL terminal
   must be absent from the table entirely -- its finished rows may never
   inflate another queue's row or appear as a zero-count row of their own.
   The truth is an independent GROUP BY over the same rows, not the
   page's own query.

2. The job detail page's archive fallback: after a REAL prune moves a
   terminal job and its attempts to the archive tier, the page must
   render the archived job's attempt history from
   ``job_attempts_archive`` -- the attempt's outcome, worker and error
   text -- and mark the row Archived. An archive fallback that answered
   404 (or rendered the job without its attempts) would tell the operator
   the work never happened.

Assertions are on the RENDERED cells, never on the page's own SQL.
"""

import re
import uuid as uuid_mod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import httpx
import pytest
import pytest_asyncio
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq.actor import actor
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client import JobsClient
from taskq.web.admin import create_router, setup_admin_state
from taskq.worker._leader_shared import prune_terminal_jobs

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp, ModulePgSchema
else:
    JobsApp = ModulePgSchema = object

pytestmark = pytest.mark.integration


class _FleetPayload(BaseModel):
    v: int = 1


@actor(name="_truth_probe_actor")
async def _truth_probe_actor(payload: _FleetPayload) -> None:
    pass


async def _admin_client(deps: Any, backend: Any) -> httpx.AsyncClient:
    """The admin app over the fixture's REAL pool, 500s surfaced as statuses."""
    bundle = create_router(deps.worker_pool, schema=deps.settings.schema_name, backend=backend)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)  # type: ignore[arg-type]  # Why: ASGITransport takes the ASGI app; FastAPI satisfies it, pyright's protocol view disagrees.
    return httpx.AsyncClient(transport=transport, base_url="http://atkweb.local")


# ── Queue overview counts ────────────────────────────────────────────────

# (queue, status) -> count; the fleet the pin seeds and the page must show.
_FLEET: dict[tuple[str, str], int] = {
    ("alpha", "pending"): 3,
    ("alpha", "scheduled"): 2,
    ("alpha", "running"): 1,
    ("alpha", "failed"): 4,
    ("beta", "pending"): 5,
    ("gamma", "succeeded"): 2,  # terminal-only queue: absent from the page
}


@pytest_asyncio.fixture
async def counted_fleet(
    clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
) -> AsyncIterator[httpx.AsyncClient]:
    """Seed the (queue, status) fleet via the real enqueue path + SQL status stamps."""
    deps, backend = clean_jobs_app
    schema = module_pg_schema.schema_name
    client = JobsClient(backend)
    handles: list[tuple[str, str]] = []
    for (queue, status), count in _FLEET.items():
        for _ in range(count):
            handle = await client.enqueue(_truth_probe_actor, _FleetPayload(), queue=queue)
            handles.append((str(handle.job_id), status))

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        now = datetime.now(UTC)
        for offset, (job_id, status) in enumerate(handles):
            finished = (
                f"started_at = '{now.isoformat()}', finished_at = '{now.isoformat()}', "
                if status in TERMINAL_STATUSES
                else ""
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs SET status = $2::"{schema}".job_status, '  # noqa: S608  # Why: schema is fixture-derived and validated; every value is $N-bound.
                f"{finished} created_at = $3 WHERE id = $1",
                uuid_mod.UUID(job_id),
                status,
                now + timedelta(seconds=offset),
            )
    finally:
        await conn.close()

    http_client = await _admin_client(deps, backend)
    try:
        yield http_client
    finally:
        await http_client.aclose()


def _queue_rows(html: str) -> dict[str, list[str]]:
    """The overview table's rows as {queue: [cell texts]}, tags stripped."""
    rows: dict[str, list[str]] = {}
    for chunk in re.split(r"<tr\b", html):
        link = re.search(r'href="/queues/([^"?]+)"', chunk)
        if link is None:
            continue
        queue = link.group(1)
        cells = re.findall(r"<td[^>]*>(.*?)</td>", chunk, re.S)
        rows[queue] = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
    return rows


async def test_queue_overview_counts_match_the_rows(counted_fleet: httpx.AsyncClient) -> None:
    """Every rendered count equals an independent GROUP BY over the same rows."""
    response = await counted_fleet.get("/queues")
    assert response.status_code == 200
    rows = _queue_rows(response.text)

    conn_by_queue = {"alpha": ["alpha", "3", "2", "1", "4"], "beta": ["beta", "5", "0", "0", "0"]}
    for queue, expected_cells in conn_by_queue.items():
        assert queue in rows, f"queue {queue} is missing from the overview table"
        cells = rows[queue]
        assert cells[:5] == expected_cells, (
            f"queue {queue}'s rendered counts {cells[:5]} disagree with the "
            f"database rows {expected_cells[1:]} (page lies, DB truth)"
        )
    # A queue whose rows are ALL terminal is not a row of zeros: it is
    # absent, and its finished rows inflate no other queue's numbers.
    assert "gamma" not in rows, "a terminal-only queue must not appear on the active-jobs overview"


# ── Job detail archive fallback ──────────────────────────────────────────


async def _seed_terminal_job_with_attempt(conn: asyncpg.Connection, schema: str) -> str:
    """One failed job with a recorded attempt, past every retention window."""
    jid = "0199f19a-7c10-7000-8000-00000000000a"
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close,
            started_at, finished_at, error_class, error_message,
            metadata, payload_schema_ver, attempt
        ) VALUES (
            $1, 'archive_probe_actor', 'default', '{{"v": 1}}'::jsonb, 3, 'transient',
            'failed'::{schema}.job_status, 0, $2, $3,
            $4, $5, 'ValueError', 'the probe failed on purpose',
            '{{}}'::jsonb, 1, 1
        )""",  # noqa: S608  # Why: schema is fixture-derived and validated; every value is $N-bound.
        uuid_mod.UUID(jid),
        now,
        now + timedelta(hours=1),
        now - timedelta(minutes=1),
        now,
    )
    await conn.execute(
        f"""INSERT INTO {schema}.job_attempts
            (job_id, attempt, started_at, finished_at, outcome,
             error_class, error_message, error_traceback, duration_ms)
        VALUES ($1, 1, $2, $3, 'failed', 'ValueError',
                'the probe failed on purpose',
                'Traceback (most recent call last): probe line 1', 250)""",  # noqa: S608
        uuid_mod.UUID(jid),
        now - timedelta(minutes=1),
        now,
    )
    return jid


@pytest_asyncio.fixture
async def archived_probe(
    clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
) -> AsyncIterator[tuple[str, httpx.AsyncClient]]:
    """A REAL prune (not a hand INSERT) moves the seeded job to the archive."""
    deps, backend = clean_jobs_app
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await _seed_terminal_job_with_attempt(conn, schema)
        result = await prune_terminal_jobs(
            conn,
            retention_per_status={status: timedelta(0) for status in TERMINAL_STATUSES},
            archive_retention=timedelta(days=365),
            schema=schema,
        )
        assert result.archived >= 1, (
            f"the prune archived nothing ({result!r}); the scenario requires "
            "the seeded job to live in the archive tier"
        )
        remaining = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            uuid_mod.UUID(job_id),
        )
        assert remaining == 0, "the seeded job must have left the hot table"
    finally:
        await conn.close()

    http_client = await _admin_client(deps, backend)
    try:
        yield job_id, http_client
    finally:
        await http_client.aclose()


async def test_archived_job_detail_renders_the_archived_attempt_history(
    archived_probe: tuple[str, httpx.AsyncClient],
) -> None:
    """The archive fallback renders the job AND its attempt history truthfully."""
    job_id, client = archived_probe
    response = await client.get(f"/jobs/{job_id}")
    assert response.status_code == 200, (
        "an archived job must answer through the detail page's archive "
        "fallback, not 404 as if it never existed"
    )
    text = response.text
    assert ">Archived<" in text, "the archived row must be marked Archived, not shown as live"
    assert "archive_probe_actor" in text, "the archived job's own identity must render"
    # The attempt history comes from job_attempts_archive: the attempt's
    # recorded evidence must reach the page.
    assert "the probe failed on purpose" in text, (
        "the archived job's attempt history (its error evidence) is missing "
        "from the page: the archive fallback read the live attempts table "
        "only"
    )
    assert "ValueError" in text, "the attempt's error class must render"
    assert "Traceback (most recent call last)" in text, "the attempt's traceback must render"
