"""Worker-clock skew harness: every keyset walk is exactly-once under skew.

The ids are UUIDv7 stamped by the ENQUEUING worker's clock while the
sort keys the admin walks page on (``created_at``, ``finished_at``,
``started_at``) are stamped by the DATABASE clock.  A worker whose
clock lags a fleet-mate mints LATER-enqueued jobs with EARLIER ids, so
``id`` order and server-ts order disagree wherever the fleet's clocks
disagree.  A cursor tuple is skew-immune exactly when the ORDER BY and
the keyset predicate are the SAME full tuple (every sort key carried,
``id`` the unique tiebreak): the row-wise comparison then defines one
total order and no clock assignment can move a row across a seam.
These pins construct the disagreement deterministically -- four
simulated workers with spread clocks (ids minted with shifted embedded
UUIDv7 timestamps, server stamps normal, the whole fleet committed in
one statement so the inverted pairs are also same-commit) -- and walk
every page at page sizes 1 / 7 / 100 asserting each row exactly once.

Also pinned here: the events watermark beside its seq cursor.  A
backward clock step mid-write inverts ``(occurred_at, id)`` across one
commit; the poll must delay the eligible sibling, never skip the
held-back one.

Runs the real queries against real Postgres and asserts on the rows
the rendered pages link / the backend returns -- never on generated SQL.
"""

import asyncio
import re
import secrets
import uuid as uuid_mod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from itertools import pairwise
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
from taskq.backend._cursor import encode_job_cursor
from taskq.backend._protocol import JobFilter, JobSortField
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin import history as history_page
from taskq.web.admin import jobs as jobs_page

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.testing.fixtures import JobsApp, ModulePgSchema
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object
    JobsApp = ModulePgSchema = object

pytestmark = pytest.mark.integration

# Four simulated workers, clocks spread wide enough that id order and
# server-stamp order invert at every worker boundary: one at the true
# time, one 30s behind, one 45s ahead, one two minutes behind.
_WORKER_SKEWS: tuple[timedelta, ...] = (
    timedelta(0),
    timedelta(seconds=-30),
    timedelta(seconds=45),
    timedelta(minutes=-2),
)
_ROWS = 120  # round-robin over the four workers: 30 inverted boundaries
_BASE = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


class _SkewPayload(BaseModel):
    v: int = 1


@actor(name="_skew_probe_actor")
async def _skew_probe_actor(payload: _SkewPayload) -> None:
    pass


def _mint_worker_uuid7(at: datetime) -> uuid_mod.UUID:
    """A UUIDv7 whose embedded millisecond clock reads *at*.

    The stand-in for a worker's own clock: the embedded stamp is the
    worker's (skewed) view of time, exactly what ``new_uuid()`` bakes in
    on a real worker.  Minted from the RFC 9562 section 5.7 layout
    directly (48-bit Unix ms, version 7, 12-bit rand_a, var b10,
    62-bit rand_b) rather than through ``taskq._ids``: the production
    seam always reads the real clock, and the TID251 ban on calling
    ``uuid_utils.uuid7`` outside it is exactly the invariant this file
    exists to stress.
    """
    ms = int(at.timestamp() * 1000)
    value = (
        ((ms & ((1 << 48) - 1)) << 80)
        | (0b0111 << 76)
        | (secrets.randbits(12) << 64)
        | (0b10 << 62)
        | secrets.randbits(62)
    )
    minted = uuid_mod.UUID(int=value)
    assert minted.version == 7, "the harness mints RFC 9562 version-7 ids only"
    return minted


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
async def skewed_fleet(
    clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
) -> AsyncIterator[tuple[list[str], httpx.AsyncClient, str]]:
    """120 terminal rows from four skewed workers, committed in one statement.

    Row *i* belongs to worker ``i % 4``; its id embeds the worker's
    skewed clock at enqueue instant ``_BASE + i//2 * 1s`` while its
    server stamps carry true instants: ``created_at`` in two-row tie
    groups, ``finished_at`` in five-row tie groups (the shape real
    same-millisecond enqueues and same-commit terminal writes land
    in).  Within every tie group the ids run inverted to the stamps,
    and across groups the big per-worker skews invert id order against
    created_at order outright, so every element of each cursor tuple
    (ts, ts, id) is load-bearing: drop any one and a page turn
    replays or skips.  The single-statement insert puts the whole
    fleet in one commit, the mid-batch shape a stepping clock
    produces.
    """
    deps, backend = clean_jobs_app
    schema = module_pg_schema.schema_name

    ids: list[uuid_mod.UUID] = []
    created: list[datetime] = []
    finished: list[datetime] = []
    for i in range(_ROWS):
        true_at = _BASE + timedelta(seconds=i // 2)
        skewed_at = true_at + _WORKER_SKEWS[i % len(_WORKER_SKEWS)]
        ids.append(_mint_worker_uuid7(skewed_at))
        created.append(true_at)
        finished.append(_BASE + timedelta(seconds=(i // 5 + 1) * 5, milliseconds=500))

    values: list[str] = []
    params: list[object] = []
    for i in range(_ROWS):
        base = len(params)
        values.append(
            f"(${base + 1}::uuid, ${base + 2}::text, ${base + 3}::text, ${base + 4}::jsonb, "
            f"${base + 5}::smallint, ${base + 6}::text, "
            f'${base + 7}::text::"{schema}".job_status, '
            f"${base + 8}::timestamptz, ${base + 9}::timestamptz, "
            f"${base + 10}::timestamptz, ${base + 11}::timestamptz)"
        )
        params.extend(
            [
                ids[i],
                "_skew_probe",
                "skew",
                '{"v": 1}',
                3,  # max_attempts
                "transient",
                "succeeded",
                created[i],
                created[i],
                created[i] + timedelta(milliseconds=100),
                finished[i],
            ]
        )
    insert_sql = (
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is fixture-derived and validated; every value is $N-bound.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, "
        "created_at, scheduled_at, started_at, finished_at) "
        f"VALUES {', '.join(values)}"
    )

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(insert_sql, *params)

        # The constructed hazard, asserted: id order and created_at order
        # disagree (adjacent inversions), and the inversions span worker
        # boundaries inside the ONE commit the single statement made.
        order_by_id = await conn.fetch(
            f'SELECT id, created_at FROM "{schema}".jobs ORDER BY id'  # noqa: S608  # Why: schema is fixture-derived and validated.
        )
        stamps = [r["created_at"] for r in order_by_id]
        inversions = sum(1 for a, b in pairwise(stamps) if a > b)
        assert inversions >= len(_WORKER_SKEWS), (
            f"the harness needs the skewed ids to invert id order vs created_at "
            f"order at worker boundaries, got {inversions} inversions"
        )
        tie_groups = await conn.fetchval(
            f'SELECT count(DISTINCT finished_at) FROM "{schema}".jobs'  # noqa: S608
        )
        assert tie_groups < _ROWS, (
            "the harness needs finished_at tie groups (the seam must resolve "
            "them on its id tiebreak), got one distinct value per row"
        )
    finally:
        await conn.close()

    http_client = await _admin_client(deps, backend)
    try:
        yield [str(i) for i in ids], http_client, module_pg_schema.schema_name
    finally:
        await http_client.aclose()


def _history_next_href(html: str) -> str | None:
    """The history page's Next-page link href, or None."""
    for anchor in re.findall(r'<a href="([^"]+)"[^>]*>\s*Next page\b', html):
        return anchor
    return None


def _jobs_next_href(html: str) -> str | None:
    """The jobs table's Next link href (the partial renders `Next <icon>`)."""
    for anchor in re.findall(r'<a href="([^"]+)"[^>]*>\s*Next\s*<', html):
        return anchor
    return None


def _page_job_ids(html: str) -> list[str]:
    """The DISTINCT job ids the rendered table links, in first-seen order.

    The jobs table links each row twice (the id cell and a View-details
    link, job_table.html), so the per-page extraction dedupes to one
    entry per rendered row; a row can never legitimately appear twice
    on one page (the page is a single ORDER BY + LIMIT scan), so a real
    duplicate WITHIN a page is impossible and cross-page replay stays
    fully visible to the walk assertions.
    """
    return list(
        dict.fromkeys(
            re.findall(
                r'href="/jobs/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"',
                html,
            )
        )
    )


# ── Backend walks: list_jobs cursor, every ordering, three page sizes ────


@pytest.mark.parametrize("page_size", [1, 7, 100])
@pytest.mark.parametrize(
    "order_by",
    [None, JobSortField.CREATED_AT_DESC, JobSortField.FINISHED_AT_DESC],
    ids=["scheduled_at_asc", "created_at_desc", "finished_at_desc"],
)
async def test_list_jobs_walk_exact_once_under_worker_clock_skew(
    skewed_fleet: tuple[list[str], httpx.AsyncClient, str],
    page_size: int,
    order_by: JobSortField | None,
    clean_jobs_app: JobsApp,
) -> None:
    """Paging ``list_jobs`` to exhaustion sees each row exactly once.

    For ANY assignment of per-worker clock skews the walk must visit
    each row exactly once and in one total order: the cursor tuple and
    the ORDER BY are the same full tuple, so the skew can only change
    WHERE the rows sit in the order, never whether a page turn
    replays or skips them.
    """
    _ids, _http_client, _schema = skewed_fleet
    _deps, backend = clean_jobs_app

    seen: list[str] = []
    cursor: str | None = None
    for _ in range(_ROWS + 10):
        page = await backend.list_jobs(  # pyright: ignore[reportAttributeAccessUsage]
            JobFilter(status="succeeded", limit=page_size, cursor=cursor, order_by=order_by)
        )
        seen.extend(str(r.id) for r in page)
        if len(page) < page_size:
            break
        cursor = encode_job_cursor(page[-1], order_by)
    else:
        pytest.fail("the list_jobs walk did not terminate within _ROWS + 10 pages")

    assert len(seen) == len(set(seen)), (
        f"the list_jobs {order_by} walk at page size {page_size} REPLAYED rows "
        f"across page boundaries: {len(seen)} served for {_ROWS} seeded jobs"
    )
    assert sorted(seen) == sorted(_ids), (
        f"the list_jobs {order_by} walk at page size {page_size} SKIPPED rows: "
        f"served {len(set(seen))} of {_ROWS} seeded jobs"
    )


# ── Admin /history page walk under skew ─────────────────────────────────


@pytest.mark.parametrize("page_size", [1, 7, 100])
async def test_history_page_walk_exact_once_under_worker_clock_skew(
    skewed_fleet: tuple[list[str], httpx.AsyncClient, str],
    page_size: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Walking the /history page's Next links sees each row exactly once."""
    ids, http_client, _schema = skewed_fleet
    monkeypatch.setattr(history_page, "_PAGE_SIZE", page_size)
    monkeypatch.setattr(history_page, "_FETCH_SIZE", page_size + 1)

    seen: list[str] = []
    url: str | None = "/history?status=succeeded"
    for _ in range(_ROWS + 10):
        response = await http_client.get(url)
        assert response.status_code == 200, f"GET {url} -> {response.status_code}"
        seen.extend(_page_job_ids(response.text))
        url = _history_next_href(response.text)
        if url is None:
            break
    assert url is None, "the history walk did not terminate within _ROWS + 10 pages"

    assert len(seen) == len(set(seen)), (
        f"the history walk at page size {page_size} REPLAYED rows across page "
        f"boundaries: {len(seen)} served for {_ROWS} seeded jobs"
    )
    assert sorted(seen) == sorted(ids), (
        f"the history walk at page size {page_size} SKIPPED rows: "
        f"served {len(set(seen))} of {_ROWS} seeded jobs"
    )


# ── Admin /jobs page walk under skew ────────────────────────────────────


@pytest.mark.parametrize("page_size", [1, 7, 100])
async def test_jobs_page_walk_exact_once_under_worker_clock_skew(
    skewed_fleet: tuple[list[str], httpx.AsyncClient, str],
    page_size: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Walking the /jobs page's Next links sees each row exactly once.

    The live tab sorts ``created_at DESC, id DESC`` through the same
    ``JobOrdering`` row-wise seam the client-facing orderings use.
    """
    ids, http_client, _schema = skewed_fleet
    monkeypatch.setattr(jobs_page, "_PAGE_SIZE", page_size)
    monkeypatch.setattr(jobs_page, "_FETCH_SIZE", page_size + 1)

    seen: list[str] = []
    url: str | None = "/jobs?tab=live&status=succeeded&sort=created_at&order=desc"
    for _ in range(_ROWS + 10):
        response = await http_client.get(url)
        assert response.status_code == 200, f"GET {url} -> {response.status_code}"
        seen.extend(_page_job_ids(response.text))
        url = _jobs_next_href(response.text)
        if url is None:
            break
    assert url is None, "the jobs walk did not terminate within _ROWS + 10 pages"

    assert len(seen) == len(set(seen)), (
        f"the jobs walk at page size {page_size} REPLAYED rows across page "
        f"boundaries: {len(seen)} served for {_ROWS} seeded jobs"
    )
    assert sorted(seen) == sorted(ids), (
        f"the jobs walk at page size {page_size} SKIPPED rows: "
        f"served {len(set(seen))} of {_ROWS} seeded jobs"
    )


# ── The events watermark beside its seq cursor ──────────────────────────


async def test_events_poll_delays_never_skips_an_inverted_stamp_pair(
    skewed_fleet: tuple[list[str], httpx.AsyncClient, str],
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Two lock_expired events in one commit with inverted (occurred_at, id).

    A backward clock step between two of a writer's statements stamps
    the higher-id row with an occurred_at far OLDER than its true time,
    so it clears the visibility margin at once while its lower-id
    sibling (stamped at its true, recent time) is still held back.  The
    poll's held-back ceiling caps the returned ids below the
    still-held-back row, so the eligible sibling is DELAYED (delivered
    in a later poll once the margin clears against its own stamp)
    instead of served past an advanced cursor: the walk then sees both
    events, each exactly once.  Without the ceiling the first poll
    serves the higher-id row, the cursor advances past the lower-id
    row's position, and that row is unreachable to ``id > after_id``
    forever -- a silently missed reclaim.
    """
    _ids, _http_client, _schema = skewed_fleet
    _deps, backend = clean_jobs_app
    schema = module_pg_schema.schema_name
    job_id = _ids[0]

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        # One statement, one commit: nextval evaluates per row in row
        # order, so the first row (true-recent stamp, held back by the
        # margin) carries the LOWER id; the second row's stamp is a
        # step-back lie, far older than its true time.
        sql = (
            f'INSERT INTO "{schema}".job_events (id, job_id, occurred_at, kind, detail) '  # noqa: S608  # Why: schema is fixture-derived and validated; every value is $N-bound.
            "VALUES (nextval('\"{schema}\".job_events_id_seq'), $1, "
            "          clock_timestamp() - interval '1 second', 'state_change', "
            '\'{"reason": "lock_expired"}\'::jsonb), '
            "(nextval('\"{schema}\".job_events_id_seq'), $1, "
            "          clock_timestamp() - interval '60 seconds', 'state_change', "
            '\'{"reason": "lock_expired"}\'::jsonb) '
            "RETURNING id, occurred_at"
        ).replace("{schema}", schema)
        rows = await conn.fetch(
            sql,
            uuid_mod.UUID(job_id),
        )
        held_id = rows[0]["id"]
        eligible_id = rows[1]["id"]
        assert held_id < eligible_id, "nextval must allocate in row order"
        assert rows[0]["occurred_at"] > rows[1]["occurred_at"], (
            "the harness needs the inverted (occurred_at, id) pair: "
            "the lower-id row carries the LATER stamp"
        )
    finally:
        await conn.close()

    # The documented consumer protocol: poll, process, advance the
    # cursor to the last-seen event_id, dedupe on event_id.
    seen: list[int] = []
    cursor = 0
    for _ in range(120):  # ~24s of 0.2s polls, the margin needs ~1s
        events = await backend.poll_reclaim_events(  # pyright: ignore[reportAttributeAccessUsage]
            after_id=cursor, visibility_delay=timedelta(seconds=2)
        )
        if events:
            seen.extend(e.event_id for e in events)
            cursor = events[-1].event_id
        if {held_id, eligible_id} <= set(seen):
            break
        await asyncio.sleep(0.2)

    assert len(seen) == len(set(seen)), (
        f"the events walk REPLAYED an event_id across polls (cursor protocol violation): {seen}"
    )
    assert set(seen) == {held_id, eligible_id}, (
        f"the events walk LOST events: saw {sorted(seen)}, seeded {{{held_id}, {eligible_id}}}"
    )
