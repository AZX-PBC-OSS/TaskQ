"""Attack: the /history page's keyset seam must not skip or repeat rows.

The history list orders its UNION of ``jobs_archive`` and live terminal
``jobs`` by ``(status_priority, finished_at DESC NULLS LAST, created_at
DESC, id DESC)`` but its keyset cursor bound only
``(COALESCE(finished_at, ceiling), id)`` -- the ``created_at`` sort key
never reached the seam predicate. An EPQ-safe keyset predicate has to
cover EVERY column the ORDER BY uses: the row-wise tuple comparison is
the only thing that defines "strictly after the last row I showed you",
and a dropped sort key makes rows whose ``id`` order disagrees with their
``created_at`` order land on the wrong side of the seam.

That disagreement is not exotic: ``id`` is UUIDv7 stamped by the
ENQUEUING worker's clock while ``created_at`` is stamped by the DATABASE
clock, so two workers with skewed clocks (or two enqueues in the same
UUIDv7 millisecond) routinely have ``id`` order and ``created_at`` order
inverted. The walk then repeats rows it already showed AND skips rows it
never shows, the two wrong-but-plausible answers a paginated audit page
can give.

Runs the real query against real Postgres and asserts on the ROWS the
rendered page links -- never on the generated SQL.
"""

import re
import uuid as uuid_mod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import asyncpg
import httpx
import pytest
import pytest_asyncio
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq.actor import actor
from taskq.client import JobsClient
from taskq.web.admin import create_router, setup_admin_state

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.testing.fixtures import JobsApp, ModulePgSchema
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object
    JobsApp = ModulePgSchema = object

pytestmark = pytest.mark.integration

_ROWS = 52  # one over the 50-row page, so the walk needs two pages


class _SeamPayload(BaseModel):
    v: int = 1


@actor(name="_seam_probe_actor")
async def _seam_probe_actor(payload: _SeamPayload) -> None:
    pass


async def _admin_client(
    deps: WorkerDeps,
    backend: PostgresBackend,
) -> httpx.AsyncClient:
    """The admin app over the fixture's REAL pool, 500s surfaced as statuses."""
    bundle = create_router(
        deps.worker_pool,  # pyright: ignore[reportAttributeAccessUsage]
        schema=deps.settings.schema_name,
        backend=backend,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)  # type: ignore[arg-type]  # Why: ASGITransport takes the ASGI app; FastAPI satisfies it, pyright's protocol view disagrees.
    return httpx.AsyncClient(transport=transport, base_url="http://atkweb.local")


@pytest_asyncio.fixture
async def inverted_fleet(
    clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
) -> AsyncIterator[tuple[list[str], httpx.AsyncClient]]:
    """52 terminal rows whose id order is the exact inverse of created_at order.

    Enqueue 52 real jobs (UUIDv7 ids, monotonic with enqueue order), then
    stamp ``created_at`` so the FIRST-enqueued row (lowest id) carries the
    NEWEST created_at and every later row is older -- the DB-vs-worker
    clock disagreement, deterministically. All rows go terminal with the
    SAME finished_at so the seam sits inside one finished_at tie group,
    where the dropped created_at key is the only thing that orders rows.
    """
    deps, backend = clean_jobs_app
    schema = module_pg_schema.schema_name
    client = JobsClient(backend)
    handle_ids: list[str] = []
    for _ in range(_ROWS):
        handle = await client.enqueue(_seam_probe_actor, _SeamPayload())
        handle_ids.append(str(handle.job_id))

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        base = datetime(2026, 3, 1, tzinfo=UTC)
        for offset, job_id in enumerate(handle_ids):
            # Later-enqueued (higher id) rows get OLDER created_at.
            created = base - timedelta(minutes=offset)
            await conn.execute(
                f'UPDATE "{schema}".jobs SET status = $2::"{schema}".job_status, '  # noqa: S608  # Why: schema is fixture-derived and validated; every value is $N-bound.
                "started_at = $3, finished_at = $3, created_at = $4 WHERE id = $1",
                uuid_mod.UUID(job_id),
                "succeeded",
                base,
                created,
            )
        # The seam sits inside one finished_at tie group: assert that.
        distinct = await conn.fetchval(
            f'SELECT count(DISTINCT finished_at) FROM "{schema}".jobs'  # noqa: S608
        )
        assert distinct == 1, (
            f"the scenario needs all rows in ONE finished_at tie group, "
            f"got {distinct} distinct values"
        )
    finally:
        await conn.close()

    http_client = await _admin_client(deps, backend)
    try:
        yield handle_ids, http_client
    finally:
        await http_client.aclose()


def _page_job_ids(html: str) -> list[str]:
    """The job ids the rendered history table links, in render order."""
    return re.findall(
        r'href="/jobs/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"', html
    )


def _next_href(html: str) -> str | None:
    """The Next page link's href from a rendered history page, or None."""
    for anchor in re.findall(r'<a href="([^"]+)"[^>]*>\s*Next page\b', html):
        return anchor
    return None


async def test_history_walk_sees_every_row_exactly_once(
    inverted_fleet: tuple[list[str], httpx.AsyncClient],
) -> None:
    """Walking Next to exhaustion sees each of the 52 rows exactly once.

    A seam that drops the created_at sort key repeats rows whose id sorts
    before the boundary while their created_at sorts after it (they fail
    nothing -- they were served on page 1 -- but the id half of the next
    page's predicate admits them again) and skips the mirror-image rows
    (their id sorts after the boundary, their created_at before it: they
    belong on page 2 but fail the id half and are never served at all).
    """
    handle_ids, http_client = inverted_fleet
    seen: list[str] = []
    url: str | None = "/history?status=succeeded"
    for _ in range(10):  # 52 rows / 50 per page needs 2 pages, never 10
        response = await http_client.get(url)
        assert response.status_code == 200, f"GET {url} -> {response.status_code}"
        seen.extend(_page_job_ids(response.text))
        url = _next_href(response.text)
        if url is None:
            break
    assert url is None, "the walk did not terminate within 10 pages"
    assert len(seen) == len(set(seen)), (
        f"the history walk REPEATED rows across page boundaries: "
        f"{len(seen)} rows served for {_ROWS} seeded jobs"
    )
    assert sorted(seen) == sorted(handle_ids), (
        f"the history walk SKIPPED rows: served {len(set(seen))} of {_ROWS} seeded jobs"
    )


async def test_history_second_page_never_replays_the_first_page_rows(
    inverted_fleet: tuple[list[str], httpx.AsyncClient],
) -> None:
    """The second page shares no row with the first page.

    Pinning the repeat shape on its own: a walk whose page 2 replays
    page 1's rows makes an operator re-read (and re-trust) failure
    evidence they already acted on.
    """
    _handle_ids, http_client = inverted_fleet
    page1 = await http_client.get("/history?status=succeeded")
    assert page1.status_code == 200
    page1_ids = set(_page_job_ids(page1.text))
    assert len(page1_ids) == 50, (
        f"page 1 of a 52-row fleet must render the full 50-row page, got {len(page1_ids)}"
    )
    next_url = _next_href(page1.text)
    assert next_url is not None, "52 rows over a 50-row page must offer a next page"
    page2 = await http_client.get(next_url)
    assert page2.status_code == 200
    page2_ids = set(_page_job_ids(page2.text))
    overlap = page1_ids & page2_ids
    assert not overlap, f"page 2 replayed {len(overlap)} rows page 1 already served"
