# ruff: noqa: S608  # Why: schema is a fixture-derived test identifier, not user input; every value is $-bound.
"""Adversarial pinning for the ``/insights`` admin page's claimed edges.

The page's commit claims: badges render only on the pathological shapes,
the params are locked down, and both storage modes see the same page.
This file attacks the claim's edges rather than its happy path (the
happy path is ``tests/web_admin/test_insights.py``'s subject):

* the utilization verdict's BOUNDARY — exactly at capacity, a hair past
  it, the healthy-idle NULL shapes, and a fleet whose liveness died
  mid-window;
* the single-checkout claim — six reads over ONE pooled connection, the
  slot held across a slow read, exhaustion answering 503 (never a
  hang), and a fetcher raising answering a LOUD failure (never a
  half-rendered page that reads as healthy);
* the render-size bound on a 500-actor fleet (the actors page's
  ``STATS_LIMIT`` discipline is the family precedent);
* byte-identical HTML on plain Postgres AND TimescaleDB hypertables
  across every declared window;
* the HTML-escaping discipline against metacharacter-laden queue and
  actor names seeded into the database.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2", reason="requires taskq[fastapi]")

from fastapi import FastAPI  # Why: importorskip guards the optional extra first.

from taskq._ids import new_uuid
from taskq.insights import INSIGHTS_WINDOWS
from taskq.migrate import apply_pending
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin._actor_stats import STATS_LIMIT
from tests.test_insights import (
    _seed_active_job,
    _seed_config,
    _seed_schedule,
    _seed_terminal_job,
    _seed_worker,
)

pytestmark = pytest.mark.integration

_PG_IMAGE = "postgres:18"
_TIMESCALE_IMAGE_DEFAULT = "timescale/timescaledb:2.30.1-pg18"
_TIMESCALE_IMAGE = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT

# The badge verdicts render as data-verdict attributes: the stable,
# text-independent hooks every verdict claim is pinned through.
_OVER_CAPACITY = 'data-verdict="over-capacity"'
_UNSERVABLE = 'data-verdict="unservable"'

# The injection surface: metacharacter-laden names seeded into the DB.
_XSS_QUEUE = "<script>alert(1)</script>"
_XSS_QUEUE_ESCAPED = "&lt;script&gt;alert(1)&lt;/script&gt;"
_XSS_ACTOR = "<img src=x onerror=alert(1)>"
_XSS_ACTOR_ESCAPED = "&lt;img src=x onerror=alert(1)&gt;"

# The route module's six fetchers (the single checkout's whole body).
_FETCHER_NAMES = (
    "fetch_wait_distribution",
    "fetch_queue_imbalance",
    "fetch_actor_backlog",
    "fetch_overprovisioning",
    "fetch_drain_estimates",
    "fetch_cron_ledger",
)

_SECTIONS = (
    "Wait distribution",
    "Fleet imbalance",
    "Actor backlog",
    "Drain estimates",
    "Overprovisioning",
    "Cron fan-out ledger",
)


# ── Containers + migrated schemas, one per mode ─────────────────────────


@pytest.fixture(scope="module")
def plain_container() -> Iterator[Any]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_PG_IMAGE, username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE, username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def plain_dsn(plain_container: Any) -> str:
    return plain_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
def timescale_dsn(timescale_container: Any) -> str:
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


async def _migrate(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)


# ── The seeded shapes ────────────────────────────────────────────────────


async def _bulk_pending(
    conn: asyncpg.Connection, schema: str, *, queue: str, actor: str, count: int
) -> None:
    """*count* due-now pending rows in one statement (the near-boundary
    utilization shapes need four-digit depth against four-digit capacity)."""
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at
            )
            SELECT gen_random_uuid(), $2, $3, '{{"v": 1}}'::jsonb, 3, 'transient',
                   'pending'::{schema}.job_status,
                   statement_timestamp() - interval '300 seconds'
            FROM generate_series(1, $1)""",
        count,
        actor,
        queue,
    )


async def _seed_bulk_actors(conn: asyncpg.Connection, schema: str, *, count: int = 500) -> None:
    """A 500-actor fleet on ``bulk_q``: one config row and one clean
    terminal job each — the render-size stress the page must bound."""
    await conn.execute(
        f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, max_pending, queue)
            SELECT 'actor_' || lpad(g::text, 4, '0'), 2, NULL, 'bulk_q'
            FROM generate_series(1, $1) g
            ON CONFLICT (actor) DO NOTHING""",
        count,
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                scheduled_at, started_at, finished_at, snooze_count, rate_limit_blocked_count
            )
            SELECT gen_random_uuid(), 'actor_' || lpad(g::text, 4, '0'), 'bulk_q',
                   '{{"v": 1}}'::jsonb, 3, 'transient', 'succeeded'::{schema}.job_status,
                   statement_timestamp() - interval '120 seconds',
                   statement_timestamp() - interval '115 seconds',
                   statement_timestamp() - interval '60 seconds', 0, 0
            FROM generate_series(1, $1) g""",
        count,
    )


async def _seed_edges_fleet(conn: asyncpg.Connection, schema: str) -> None:
    """The utilization verdict's boundary shapes, one queue each.

    * ``edge_exact_q``  — 2 due over (cap 2 x 1 worker) = EXACTLY 1.0: the
      documented balanced edge → NO badge.
    * ``edge_over_q``   — 1001 ÷ 1000 = 1.001 → the badge, whose rounded
      display ALSO reads "100%".
    * ``edge_under_q``  — 999 ÷ 1000 = 0.999 → NO badge, display "100%":
      the badge keys on the true ratio, never the rounded percent.
    * ``edge_idle_q``   — live worker, NO actor_config, zero jobs → NULL
      utilization over ZERO due work → NO badge (healthy-idle).
    * ``edge_orphan_q`` — actor_config only, no worker, no jobs → same
      NULL-over-zero shape → NO badge.
    * ``edge_zero_q``   — worker AND capacity AND zero due → utilization
      exactly 0.0 → NO badge.
    * ``edge_stale_q``  — the worker's liveness died (last_seen 600 s
      ago, outside the 30 s window) with capacity 4 and 3 due: the
      denominator dropped to zero mid-window → the unservable badge.
    * ``edge_fresh_q``  — the identical shape with a LIVE worker → 75%,
      no badge (the contrast that makes the stale verdict honest).
    """
    await _seed_config(conn, schema, actor="edge_exact_a", queue="edge_exact_q", max_concurrent=2)
    await _seed_worker(conn, schema, queues=["edge_exact_q"])
    for _ in range(2):
        await _seed_active_job(
            conn,
            schema,
            queue="edge_exact_q",
            actor="edge_exact_a",
            status="pending",
            scheduled_age_s=300.0,
        )

    await _seed_config(conn, schema, actor="edge_over_a", queue="edge_over_q", max_concurrent=500)
    await _seed_worker(conn, schema, queues=["edge_over_q"])
    await _seed_worker(conn, schema, queues=["edge_over_q"])
    await _bulk_pending(conn, schema, queue="edge_over_q", actor="edge_over_a", count=1001)

    await _seed_config(conn, schema, actor="edge_under_a", queue="edge_under_q", max_concurrent=500)
    await _seed_worker(conn, schema, queues=["edge_under_q"])
    await _seed_worker(conn, schema, queues=["edge_under_q"])
    await _bulk_pending(conn, schema, queue="edge_under_q", actor="edge_under_a", count=999)

    await _seed_worker(conn, schema, queues=["edge_idle_q"])

    await _seed_config(conn, schema, actor="edge_orphan_a", queue="edge_orphan_q", max_concurrent=4)

    await _seed_config(conn, schema, actor="edge_zero_a", queue="edge_zero_q", max_concurrent=4)
    await _seed_worker(conn, schema, queues=["edge_zero_q"])

    await _seed_config(conn, schema, actor="edge_stale_a", queue="edge_stale_q", max_concurrent=4)
    await _seed_worker(conn, schema, queues=["edge_stale_q"], seen_age_s=600.0)
    for _ in range(3):
        await _seed_active_job(
            conn,
            schema,
            queue="edge_stale_q",
            actor="edge_stale_a",
            status="pending",
            scheduled_age_s=300.0,
        )

    await _seed_config(conn, schema, actor="edge_fresh_a", queue="edge_fresh_q", max_concurrent=4)
    await _seed_worker(conn, schema, queues=["edge_fresh_q"])
    for _ in range(3):
        await _seed_active_job(
            conn,
            schema,
            queue="edge_fresh_q",
            actor="edge_fresh_a",
            status="pending",
            scheduled_age_s=300.0,
        )

    # The 500-actor render-size stress rides in the same schema.
    await _seed_bulk_actors(conn, schema)


async def _seed_twin_fleet(conn: asyncpg.Connection, schema: str) -> None:
    """A deterministic fleet whose every rendered figure is jitter-proof
    (ages sit far from a humanize boundary), seeded IDENTICALLY into both
    engines — the byte-identical HTML claim's input.  The metacharacter
    names ride here too, so both engines' escaping is also compared."""
    await _seed_config(conn, schema, actor="twin_a", queue="twin_q", max_concurrent=4)
    await _seed_worker(conn, schema, queues=["twin_q"])
    await _seed_worker(conn, schema, queues=["twin_q"])
    for _ in range(2):
        await _seed_active_job(
            conn,
            schema,
            queue="twin_q",
            actor="twin_a",
            status="pending",
            scheduled_age_s=300.0,
        )
    await _seed_active_job(
        conn, schema, queue="twin_q", actor="twin_a", status="scheduled", scheduled_age_s=None
    )
    for _ in range(5):
        await _seed_terminal_job(
            conn, schema, queue="twin_q", actor="twin_a", wait_s=10.0, finished_age_s=120.0
        )
    sid = await _seed_schedule(conn, schema, actor="twin_sched_a")
    for _ in range(4):
        await _seed_terminal_job(
            conn,
            schema,
            queue="twin_q",
            actor="twin_sched_a",
            wait_s=1.0,
            finished_age_s=60.0,
            schedule_id=sid,
        )
    await _seed_config(conn, schema, actor=_XSS_ACTOR, queue=_XSS_QUEUE, max_concurrent=2)
    await _seed_terminal_job(
        conn, schema, queue=_XSS_QUEUE, actor=_XSS_ACTOR, wait_s=3.0, finished_age_s=90.0
    )


# ── The lab: two schemas per mode, one pool each ─────────────────────────


@dataclass
class _Mounted:
    schema: str
    app: FastAPI
    pool: asyncpg.Pool


@dataclass
class _Lab:
    plain_edges: _Mounted
    plain_twin: _Mounted
    ts_edges: _Mounted
    ts_twin: _Mounted


def _mount(pool: asyncpg.Pool, schema: str) -> _Mounted:
    bundle = create_router(pool, schema=schema, base_path="/admin")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    return _Mounted(schema=schema, app=app, pool=pool)


@pytest.fixture(scope="module")
async def lab(plain_dsn: str, timescale_dsn: str) -> AsyncIterator[_Lab]:
    """Both containers; an ``edges`` and a ``twin`` schema per mode."""
    mp = pytest.MonkeyPatch()
    mp.setenv("TASKQ_ENVIRONMENT", "dev")
    mp.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    try:
        run_tag = new_uuid().hex[:10]
        schemas = {
            "plain_edges": f"insatk_van_edg_{run_tag}",
            "plain_twin": f"insatk_van_twn_{run_tag}",
            "ts_edges": f"insatk_ht_edg_{run_tag}",
            "ts_twin": f"insatk_ht_twn_{run_tag}",
        }
        pools: dict[str, asyncpg.Pool] = {}
        try:
            for dsn_key, dsn in (("plain", plain_dsn), ("ts", timescale_dsn)):
                setup_conn = await asyncpg.connect(dsn)
                try:
                    for suffix, seeder in (
                        ("edges", _seed_edges_fleet),
                        ("twin", _seed_twin_fleet),
                    ):
                        schema = schemas[f"{dsn_key}_{suffix}"]
                        await _migrate(setup_conn, schema)
                        seed_conn = await asyncpg.connect(dsn)
                        try:
                            await seeder(seed_conn, schema)
                        finally:
                            await seed_conn.close()
                finally:
                    await setup_conn.close()
                for suffix in ("edges", "twin"):
                    pools[f"{dsn_key}_{suffix}"] = await asyncpg.create_pool(
                        dsn, min_size=1, max_size=4
                    )
            yield _Lab(
                plain_edges=_mount(pools["plain_edges"], schemas["plain_edges"]),
                plain_twin=_mount(pools["plain_twin"], schemas["plain_twin"]),
                ts_edges=_mount(pools["ts_edges"], schemas["ts_edges"]),
                ts_twin=_mount(pools["ts_twin"], schemas["ts_twin"]),
            )
        finally:
            for pool in pools.values():
                await pool.close()
            for dsn in (plain_dsn, timescale_dsn):
                conn = await asyncpg.connect(dsn)
                try:
                    for schema in schemas.values():
                        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                finally:
                    await conn.close()
    finally:
        mp.undo()


@pytest.fixture(params=["plain", "timescale"], ids=["plain-pg", "hypertable"])
async def matrix(request: pytest.FixtureRequest, lab: _Lab) -> tuple[_Mounted, _Mounted]:
    """The same test body against both containers: (edges, twin)."""
    if request.param == "plain":
        return lab.plain_edges, lab.plain_twin
    return lab.ts_edges, lab.ts_twin


# ── HTTP helpers (the web_admin idiom: httpx ASGI transport) ────────────


async def _get(mounted: _Mounted, path: str) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mounted.app), base_url="http://test"
    ) as client:
        return await client.get(path)


async def _html(mounted: _Mounted, path: str = "/admin/insights") -> str:
    resp = await _get(mounted, path)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}: {resp.text[:400]}"
    return resp.text


def _section(html: str, heading: str) -> str:
    """The rendered ``<section>`` chunk whose heading is *heading*."""
    for chunk in html.split("<section"):
        if heading in chunk:
            return chunk
    raise AssertionError(f"missing section: {heading}")


def _rows_containing(html: str, needle: str) -> list[str]:
    """The rendered ``<tr>`` chunks that mention *needle*."""
    return [chunk for chunk in html.split("<tr") if needle in chunk]


# ── Attack 1: the badge predicates' edges ────────────────────────────────


async def test_utilization_exactly_at_capacity_is_the_documented_balanced_edge(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """2 due ÷ capacity 2 = EXACTLY 1.0: the docs' balanced edge — the
    row renders "100%" and NO badge.  A verdict at exactly-at-capacity
    would cry wolf on the balanced shape the docs define as healthy."""
    edges, _ = matrix
    html = await _html(edges)
    section = _section(html, "Fleet imbalance")
    rows = _rows_containing(section, "edge_exact_q")
    assert rows, "the exactly-at-capacity queue must render"
    for row in rows:
        assert "data-verdict" not in row, f"utilization 1.0 must not badge: {row!r}"
        assert "100%" in row, "utilization 1.0 must render as 100%"


async def test_the_badge_keys_on_the_true_ratio_not_the_rounded_display(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """THE float-boundary pair: 1.001 and 0.999 both display "100%", but
    only the true-ratio side over the high-water mark earns the badge.
    A verdict computed on the ROUNDED display would badge both."""
    edges, _ = matrix
    html = await _html(edges)
    section = _section(html, "Fleet imbalance")
    over_rows = _rows_containing(section, "edge_over_q")
    assert over_rows, "the just-over-capacity queue must render"
    assert any(_OVER_CAPACITY in r for r in over_rows), (
        "utilization 1.001 (display '100%') must render the over-capacity badge"
    )
    assert any("Over capacity (100%)" in r for r in over_rows), (
        "the badge's own rounded label must still read 100%"
    )
    under_rows = _rows_containing(section, "edge_under_q")
    assert under_rows, "the just-under-capacity queue must render"
    for row in under_rows:
        assert "data-verdict" not in row, f"utilization 0.999 must not badge: {row!r}"
        assert "100%" in row, "utilization 0.999 must ALSO display 100% — same text, no badge"


async def test_null_utilization_over_zero_due_work_is_healthy_idle_never_unservable(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The badge must NOT fire on an empty idle queue: all three
    NULL-utilization shapes with ZERO due work (no worker and no config;
    config but no worker; worker AND config but nothing due) render no
    unservable verdict — unservable means DUE WORK nothing can serve."""
    edges, _ = matrix
    html = await _html(edges)
    section = _section(html, "Fleet imbalance")
    for queue in ("edge_idle_q", "edge_orphan_q", "edge_zero_q"):
        rows = _rows_containing(section, queue)
        assert rows, f"{queue} must render an imbalance row"
        for row in rows:
            assert _UNSERVABLE not in row, f"{queue} (no due work) must not badge: {row!r}"
            assert "&mdash;" in row, f"{queue}'s NULL utilization must render the dash: {row!r}"


async def test_a_worker_dropping_out_of_liveness_turns_a_due_queue_unservable(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The denominator dies mid-window: the only worker's last_seen left
    the 30 s liveness window, effective capacity collapses to zero, and
    the queue's due work is genuinely unservable RIGHT NOW — the badge
    is the honest verdict.  The identical shape with a live worker is
    75% and badgeless: the liveness bound, not the badge, moved."""
    edges, _ = matrix
    html = await _html(edges)
    section = _section(html, "Fleet imbalance")
    stale_rows = _rows_containing(section, "edge_stale_q")
    assert stale_rows, "the stale-liveness queue must render"
    assert any(_UNSERVABLE in r for r in stale_rows), (
        "due work whose only worker left the liveness window must badge unservable"
    )
    fresh_rows = _rows_containing(section, "edge_fresh_q")
    assert fresh_rows, "the fresh-worker contrast must render"
    for row in fresh_rows:
        assert "data-verdict" not in row, f"a live worker's 75% must not badge: {row!r}"
        assert "75%" in row


# ── Attack 2: the single-pool checkout and the failure shape ────────────


async def test_six_reads_run_over_one_checkout(
    monkeypatch: pytest.MonkeyPatch, matrix: tuple[_Mounted, _Mounted]
) -> None:
    """The route's claimed shape: all six section reads observe the SAME
    checked-out connection — one consistent-ish snapshot, one slot."""
    edges, _ = matrix
    import taskq.web.admin.insights as route_mod

    checked_out: list[Any] = []

    def _wrap(name: str) -> Any:
        real = getattr(route_mod, name)

        async def fetched(conn: Any, **kwargs: Any) -> Any:
            checked_out.append(conn)
            return await real(conn, **kwargs)

        return fetched

    for name in _FETCHER_NAMES:
        monkeypatch.setattr(route_mod, name, _wrap(name))
    await _html(edges)
    assert len(checked_out) == 6, "all six fetchers must run for one render"
    assert len({id(c) for c in checked_out}) == 1, (
        "the six reads must share ONE pooled connection (the single checkout)"
    )


async def test_a_slow_read_holds_the_slot_and_exhaustion_answers_503_not_a_hang(
    monkeypatch: pytest.MonkeyPatch, plain_dsn: str, lab: _Lab
) -> None:
    """The route holds its one pool slot across ALL six reads: while a
    slow read (the wait percentile over a big archive) holds it, a
    concurrent page view on a one-connection pool cannot check out — and
    the bounded pool answers THAT with 503 + Retry-After, never a hang
    and never a half page.  This pins the cost of the single checkout
    AND its honest exhaustion answer."""
    import taskq.web.admin.insights as route_mod

    schema = lab.plain_twin.schema
    mp = pytest.MonkeyPatch()
    mp.setenv("TASKQ_ENVIRONMENT", "dev")
    mp.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    mp.setenv("TASKQ_ADMIN_ACQUIRE_TIMEOUT", "0.2")
    pool = await asyncpg.create_pool(plain_dsn, min_size=1, max_size=1)
    try:
        bundle = create_router(pool, schema=schema, base_path="/admin")
        app = FastAPI()
        setup_admin_state(app, bundle)
        app.include_router(bundle.router, prefix="/admin")

        real = route_mod.fetch_wait_distribution

        async def slow_wait(conn: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0.5)
            return await real(conn, **kwargs)

        monkeypatch.setattr(route_mod, "fetch_wait_distribution", slow_wait)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            first = asyncio.create_task(client.get("/admin/insights"))
            await asyncio.sleep(0.1)
            second = asyncio.create_task(client.get("/admin/insights"))
            first_resp, second_resp = await asyncio.gather(first, second)
        assert first_resp.status_code == 200
        assert second_resp.status_code == 503, (
            "a view arriving while the slow read holds the only slot must get the "
            "bounded pool's 503, not a hang: " + second_resp.text[:200]
        )
        assert second_resp.headers.get("retry-after") is not None, "the 503 must carry Retry-After"
    finally:
        await pool.close()
        mp.undo()


async def test_a_raising_fetcher_fails_loudly_never_a_healthy_looking_half_page(
    monkeypatch: pytest.MonkeyPatch, matrix: tuple[_Mounted, _Mounted]
) -> None:
    """THE honest failure shape: a fetcher raising (a legacy schema's
    archive table missing a column) must blow the request up — the
    operator gets an error, NEVER a 200 page where five sections render
    and the sixth silently vanished (a half-empty insights page reads as
    a healthy fleet).  The route renders only after ALL six reads
    return, so any raise aborts the whole page."""
    edges, _ = matrix
    import taskq.web.admin.insights as route_mod

    async def boom(conn: Any, **kwargs: Any) -> Any:
        raise RuntimeError("simulated legacy-schema drift: archive column missing")

    monkeypatch.setattr(route_mod, "fetch_cron_ledger", boom)
    with pytest.raises(RuntimeError):
        await _get(edges, "/admin/insights")


async def test_a_legacy_schema_missing_column_500s_and_releases_the_pool_slot(
    plain_dsn: str,
) -> None:
    """The concrete legacy-schema shape: the archive table is missing the
    column the wait SQL's UNION arm names.  The page must fail loudly
    (the driver's error propagates — no partial render) AND the pool
    slot must come back (the checkout's context manager releases on the
    way out), so the failure costs one request, not the pool."""
    run_tag = new_uuid().hex[:10]
    schema = f"insatk_legacy_{run_tag}"
    setup_conn = await asyncpg.connect(plain_dsn)
    try:
        await _migrate(setup_conn, schema)
        # The legacy drift: the archive predates a column the live table has.
        await setup_conn.execute(f'ALTER TABLE "{schema}".jobs_archive DROP COLUMN snooze_count')
    finally:
        await setup_conn.close()

    pool = await asyncpg.create_pool(plain_dsn, min_size=1, max_size=2)
    try:
        mp = pytest.MonkeyPatch()
        mp.setenv("TASKQ_ENVIRONMENT", "dev")
        mp.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
        try:
            bundle = create_router(pool, schema=schema, base_path="/admin")
            app = FastAPI()
            setup_admin_state(app, bundle)
            app.include_router(bundle.router, prefix="/admin")
            mounted = _Mounted(schema=schema, app=app, pool=pool)

            with pytest.raises(asyncpg.UndefinedColumnError):
                await _get(mounted, "/admin/insights")

            in_flight = pool.get_size() - pool.get_idle_size()
            assert in_flight == 0, (
                f"the failed request must release its checkout (still {in_flight} held)"
            )
            async with pool.acquire() as conn:
                assert await conn.fetchval("SELECT 1") == 1, "the pool must still serve"
        finally:
            mp.undo()
    finally:
        await pool.close()
        conn = await asyncpg.connect(plain_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()


# ── Attack 3: render size, window cycling ───────────────────────────────


async def test_per_actor_tables_are_capped_with_an_honest_truncation_note(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """On a 500-actor fleet the per-actor wait grouping and the actor
    backlog table would render 1000+ rows into the operator's browser —
    the actors page caps the same shape at ``STATS_LIMIT`` with a
    visible note, and this page must not be the family's unbounded
    outlier.  The cap is honest: it says what it did."""
    edges, _ = matrix
    html = await _html(edges, "/admin/insights?per_actor=true")
    wait_section = _section(html, "Wait distribution")
    assert f"Showing the first {STATS_LIMIT}" in wait_section, "a capped wait table must say so"
    assert "actor_0200" in wait_section, "the cap keeps the first STATS_LIMIT groups"
    assert "actor_0201" not in wait_section, "groups past the cap are omitted from the page"
    backlog_section = _section(html, "Actor backlog")
    assert f"Showing the first {STATS_LIMIT}" in backlog_section, (
        "a capped backlog table must say so"
    )
    assert "actor_0200" in backlog_section
    assert "actor_0201" not in backlog_section


async def test_sections_below_the_cap_render_no_apology(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The truncation note appears ONLY when a section was actually
    capped — a small fleet's page must not apologize for nothing."""
    _, twin = matrix
    html = await _html(twin)
    assert "Showing the first" not in html


async def test_every_declared_window_renders_the_whole_page(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """All four windows render 200 with all six sections and the picker
    naming the active window — no window degrades the page."""
    edges, _ = matrix
    for window in INSIGHTS_WINDOWS:
        html = await _html(edges, f"/admin/insights?window={window}")
        for heading in _SECTIONS:
            assert heading in html, f"window={window}: missing section: {heading}"
        assert f"?window={window}" in html, f"window={window}: the picker must name the window"


# ── Attack 4: cross-engine identicalness ────────────────────────────────

_CSRFISH = re.compile(r'value="[0-9a-f]{32,}"')


def _normalize(html: str) -> str:
    """The only render input that may legitimately vary between two
    requests: a per-request token value, if one ever reaches the body."""
    return _CSRFISH.sub('value=""', html)


async def test_both_engines_render_the_same_page_byte_for_byte(lab: _Lab) -> None:
    """The admin-on-hypertables differential discipline, page edition:
    the identically-seeded twin schemas must render IDENTICAL HTML on
    plain Postgres and TimescaleDB hypertables, across every declared
    window (minus only a per-request token, if one ever appears)."""
    for window in INSIGHTS_WINDOWS:
        plain = await _html(lab.plain_twin, f"/admin/insights?window={window}")
        ts = await _html(lab.ts_twin, f"/admin/insights?window={window}")
        assert _normalize(plain) == _normalize(ts), (
            f"the engines' renders diverge at window={window}"
        )


# ── The mode badge / refresh story ──────────────────────────────────────


class _HealthyRedis:
    """A redis client stand-in whose ping always answers."""

    async def ping(self) -> bool:
        return True


async def test_the_mode_badge_tells_the_same_truth_as_every_other_page(
    plain_dsn: str, lab: _Lab
) -> None:
    """Every sibling page resolves the deployment's realtime mode and
    renders it (the header badge) and lets ``_base.html`` decide the
    meta-refresh from it.  The insights page must tell the SAME story:
    a page whose badge says "polling mode" while the rest of the UI says
    real-time — and which therefore keeps meta-refreshing six archive
    UNION aggregates every poll interval on a healthy realtime
    deployment — is a page lying at a glance."""
    from taskq.web.admin import _factory

    _factory._redis_health_cache.ok = False  # pyright: ignore[reportPrivateUsage]  # Why: the module-level 5s cache is the reset point the realtime-badge suite uses too.
    _factory._redis_health_cache.expires_at = 0.0  # pyright: ignore[reportPrivateUsage]

    schema = lab.plain_twin.schema
    pool = await asyncpg.create_pool(plain_dsn, min_size=1, max_size=2)
    try:
        bundle = create_router(
            pool, schema=schema, base_path="/admin", redis_client=_HealthyRedis()
        )
        app = FastAPI()
        setup_admin_state(app, bundle)
        app.include_router(bundle.router, prefix="/admin")
        mounted = _Mounted(schema=schema, app=app, pool=pool)

        queues_html = await _html(mounted, "/admin/queues")
        insights_html = await _html(mounted, "/admin/insights")

        assert 'data-mode="realtime"' in queues_html, (
            "the control: a healthy redis ping renders the realtime badge"
        )
        assert 'data-mode="realtime"' in insights_html, (
            "the insights page must resolve the deployment's mode, not default it to polling"
        )
        assert "http-equiv" not in insights_html, (
            "a realtime deployment must not meta-refresh the insights page"
        )
    finally:
        await pool.close()
        _factory._redis_health_cache.ok = False  # pyright: ignore[reportPrivateUsage]
        _factory._redis_health_cache.expires_at = 0.0  # pyright: ignore[reportPrivateUsage]


# ── Attack 5: the template's injection surface ──────────────────────────


async def test_metacharacter_names_seed_renders_escaped(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """Queue and actor names with HTML metacharacters, seeded into the
    DB, render ESCAPED — the jobs pages' discipline.  The raw payload
    must appear nowhere in the body, in either grouping."""
    _, twin = matrix
    for path in ("/admin/insights", "/admin/insights?per_actor=true"):
        html = await _html(twin, path)
        assert _XSS_QUEUE_ESCAPED in html, f"{path}: the queue name must render escaped"
        assert _XSS_ACTOR_ESCAPED in html, f"{path}: the actor name must render escaped"
        assert _XSS_QUEUE not in html, f"{path}: the raw queue payload must not survive"
        assert _XSS_ACTOR not in html, f"{path}: the raw actor payload must not survive"
