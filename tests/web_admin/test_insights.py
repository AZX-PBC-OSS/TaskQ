# Why: schema is a fixture-derived test identifier, not user input; every value is $-bound.
"""The ``/insights`` admin page against real containers, both storage modes.

The page renders the already-merged :mod:`taskq.insights` surfaces — the
wait distribution (clean vs deferred SEPARATED, the confound labeled),
the fleet-imbalance table with its utilization verdict badges, the
overprovisioning flags, the drain estimates with their ``has_traffic``
honesty, and the cron fan-out ledger — through the admin router on a
plain Postgres 18 container AND a TimescaleDB 2.30.1 (PG18) hypertable
conversion, the same both-modes matrix ``tests/test_insights.py`` runs
for the SQL layer itself.

The badge claims are the behavioral ones and are pinned red-first: a
healthy fleet renders NO verdict badge anywhere on the page; a verdict
badge renders only on the pathological shape that earns it
(over-capacity utilization, an unservable queue, an idle fleet, a
runaway schedule).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2", reason="requires taskq[fastapi]")

from fastapi import FastAPI, HTTPException  # Why: importorskip guards the optional extra first.

from taskq._ids import new_uuid
from taskq.insights import INSIGHTS_WINDOWS
from taskq.migrate import apply_pending
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.web.admin import create_router, setup_admin_state
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
# text-independent hooks the badge claims are pinned through.
_OVER_CAPACITY = 'data-verdict="over-capacity"'
_UNSERVABLE = 'data-verdict="unservable"'
_IDLE_FLEET = 'data-verdict="idle-fleet"'
_RUNAWAY = 'data-verdict="runaway"'


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
#
# TWO schemas per mode. The MIXED schema carries every pathological shape
# beside a healthy queue; the HEALTHY schema carries ONLY the healthy
# fleet, so "the healthy fleet renders NO badges" is a whole-page claim,
# not a row-scoped one.


async def _seed_healthy_fleet(conn: asyncpg.Connection, schema: str) -> None:
    """One balanced queue + one clearing-on-schedule cron schedule.

    * 2 workers on ``healthy_q``, per-actor capacity 4 → effective
      capacity 8; 2 due pending → utilization 0.25 (no badge).
    * 5 clean terminal jobs + 4 schedule fires, all terminalised inside
      the window → the drain estimate HAS traffic (eta 2 ÷ (9/3600) =
      800 s → "13m") and the queue is NOT overprovisioned (9 ≥ 2).
    * The schedule's fires == clearance in the current window and its
      prior is empty → ``runaway_trending`` false.
    """
    await _seed_config(conn, schema, actor="healthy_a", queue="healthy_q", max_concurrent=4)
    await _seed_worker(conn, schema, queues=["healthy_q"])
    await _seed_worker(conn, schema, queues=["healthy_q"])
    for _ in range(2):
        await _seed_active_job(
            conn,
            schema,
            queue="healthy_q",
            actor="healthy_a",
            status="pending",
            scheduled_age_s=30.0,
        )
    await _seed_active_job(
        conn, schema, queue="healthy_q", actor="healthy_a", status="scheduled", scheduled_age_s=None
    )
    for _ in range(5):
        await _seed_terminal_job(
            conn, schema, queue="healthy_q", actor="healthy_a", wait_s=10.0, finished_age_s=120.0
        )
    sid = await _seed_schedule(conn, schema, actor="healthy_sched_a")
    for _ in range(2):
        await _seed_terminal_job(
            conn,
            schema,
            queue="healthy_q",
            actor="healthy_sched_a",
            wait_s=1.0,
            finished_age_s=60.0,
            schedule_id=sid,
        )
    for _ in range(2):
        await _seed_terminal_job(
            conn,
            schema,
            queue="healthy_q",
            actor="healthy_sched_a",
            wait_s=1.0,
            finished_age_s=5400.0,
            schedule_id=sid,
        )


async def _seed_mixed_fleet(conn: asyncpg.Connection, schema: str) -> None:
    """Every pathological shape beside the healthy queue of the same name.

    * ``starve_q``  — due work, NO live worker → utilization NULL → the
      unservable badge.
    * ``overcap_q`` — 5 due jobs behind capacity 1 x 1 → utilization 5.0 →
      the over-capacity badge.
    * ``idle_q``    — 2 live workers, zero due depth, zero
      terminalisations in the window → the idle-fleet badge.
    * ``quiet_q``   — due depth, NO window traffic → the drain section
      must say so, never render an eta.
    * ``runaway_a`` — 6 fires this window (1 cleared) and 3 in the prior
      hour (0 cleared): fires > cleared in BOTH windows → the runaway
      badge renders at the 1h window (at the 24h default the prior
      window is empty, so the verdict is honestly false — pinned too).
    * The healthy queue carries a deferred (snoozed) row and a second
      actor's rows so the wait table's clean/deferred separation and the
      per-actor x queue toggle both have something honest to show.
    """
    await _seed_healthy_fleet(conn, schema)

    # A deferred row: snooze_count > 0 → the deferred segment, wait = final leg only.
    await _seed_terminal_job(
        conn,
        schema,
        queue="healthy_q",
        actor="healthy_a",
        wait_s=5.0,
        finished_age_s=150.0,
        snooze_count=1,
    )
    # A second actor whose ONLY trace is wait rows: visible only when the
    # per-actor x queue toggle groups the wait table by (actor, queue).
    for _ in range(2):
        await _seed_terminal_job(
            conn, schema, queue="healthy_q", actor="wait_only_a", wait_s=20.0, finished_age_s=180.0
        )

    await _seed_config(conn, schema, actor="starve_a", queue="starve_q", max_concurrent=4)
    for _ in range(5):
        await _seed_active_job(
            conn, schema, queue="starve_q", actor="starve_a", status="pending", scheduled_age_s=60.0
        )

    await _seed_config(conn, schema, actor="overcap_a", queue="overcap_q", max_concurrent=1)
    await _seed_worker(conn, schema, queues=["overcap_q"])
    for _ in range(5):
        await _seed_active_job(
            conn,
            schema,
            queue="overcap_q",
            actor="overcap_a",
            status="pending",
            scheduled_age_s=30.0,
        )

    await _seed_worker(conn, schema, queues=["idle_q"])
    await _seed_worker(conn, schema, queues=["idle_q"])

    await _seed_config(conn, schema, actor="quiet_a", queue="quiet_q", max_concurrent=4)
    for _ in range(4):
        await _seed_active_job(
            conn, schema, queue="quiet_q", actor="quiet_a", status="pending", scheduled_age_s=30.0
        )

    sid = await _seed_schedule(conn, schema, actor="runaway_a")
    for i in range(6):
        await _seed_terminal_job(
            conn,
            schema,
            queue="ledger_q",
            actor="runaway_a",
            wait_s=1.0,
            finished_age_s=60.0,
            status="succeeded" if i == 0 else "pending",
            schedule_id=sid,
        )
    for _ in range(3):
        await _seed_terminal_job(
            conn,
            schema,
            queue="ledger_q",
            actor="runaway_a",
            wait_s=1.0,
            finished_age_s=5400.0,  # created ~90 min ago: the PRIOR 1h window
            status="pending",
            schedule_id=sid,
        )


# ── The lab: two apps per mode, one pool each ────────────────────────────


@dataclass
class _Mounted:
    schema: str
    app: FastAPI


@dataclass
class _Lab:
    plain_mixed: _Mounted
    plain_healthy: _Mounted
    ts_mixed: _Mounted
    ts_healthy: _Mounted


def _mount(pool: asyncpg.Pool, schema: str) -> _Mounted:
    bundle = create_router(pool, schema=schema, base_path="/admin")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    return _Mounted(schema=schema, app=app)


@pytest.fixture(scope="module")
async def lab(plain_dsn: str, timescale_dsn: str) -> AsyncIterator[_Lab]:
    """Both containers, four schemas (mixed + healthy per mode), four apps.

    The dev-env trio is set HERE because ``create_router`` loads settings
    at module-fixture time, before the function-scoped autouse fixture
    would have run (the hypertables suite's idiom).
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("TASKQ_ENVIRONMENT", "dev")
    mp.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    try:
        run_tag = new_uuid().hex[:10]
        schemas = {
            "plain_mixed": f"insdash_van_mix_{run_tag}",
            "plain_healthy": f"insdash_van_ok_{run_tag}",
            "ts_mixed": f"insdash_ht_mix_{run_tag}",
            "ts_healthy": f"insdash_ht_ok_{run_tag}",
        }
        pools: dict[str, asyncpg.Pool] = {}
        try:
            for dsn_key, dsn in (("plain", plain_dsn), ("ts", timescale_dsn)):
                setup_conn = await asyncpg.connect(dsn)
                try:
                    for suffix in ("mixed", "healthy"):
                        schema = schemas[f"{dsn_key}_{suffix}"]
                        await _migrate(setup_conn, schema)
                        seeder = _seed_mixed_fleet if suffix == "mixed" else _seed_healthy_fleet
                        seed_conn = await asyncpg.connect(dsn)
                        try:
                            await seeder(seed_conn, schema)
                        finally:
                            await seed_conn.close()
                finally:
                    await setup_conn.close()
                for suffix in ("mixed", "healthy"):
                    pools[f"{dsn_key}_{suffix}"] = await asyncpg.create_pool(
                        dsn, min_size=1, max_size=4
                    )
            yield _Lab(
                plain_mixed=_mount(pools["plain_mixed"], schemas["plain_mixed"]),
                plain_healthy=_mount(pools["plain_healthy"], schemas["plain_healthy"]),
                ts_mixed=_mount(pools["ts_mixed"], schemas["ts_mixed"]),
                ts_healthy=_mount(pools["ts_healthy"], schemas["ts_healthy"]),
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
    """The same test body against both containers: (mixed, healthy)."""
    if request.param == "plain":
        return lab.plain_mixed, lab.plain_healthy
    return lab.ts_mixed, lab.ts_healthy


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


def _rows_containing(html: str, needle: str) -> list[str]:
    """The rendered ``<tr>`` chunks that mention *needle*."""
    return [chunk for chunk in html.split("<tr") if needle in chunk]


# ── The sections render from the seeded shapes ──────────────────────────


async def test_page_renders_every_section_from_the_seeded_shapes(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The mixed page carries all six surfaces, each naming its queues."""
    mixed, _ = matrix
    html = await _html(mixed)
    for heading in (
        "Wait distribution",
        "Fleet imbalance",
        "Actor backlog",
        "Overprovisioning",
        "Drain estimates",
        "Cron fan-out ledger",
    ):
        assert heading in html, f"missing section: {heading}"
    # The wait table shows the queue, the imbalance table the starved queue,
    # the backlog table the configured actor, overprovisioning the idle
    # queue, drain the quiet queue, and the ledger the runaway schedule.
    for needle in (
        "healthy_q",
        "starve_q",
        "overcap_q",
        "idle_q",
        "quiet_q",
        "healthy_a",
        "runaway_a",
    ):
        assert needle in html, f"missing seeded shape: {needle}"


async def test_wait_distribution_separates_clean_from_deferred(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The confound is the point: the two segments render SEPARATED, and
    the page says honestly what the deferred segment's wait measures."""
    mixed, _ = matrix
    html = await _html(mixed)
    assert "clean" in html
    assert "deferred" in html
    # The honest labeling: deferred rows measure the FINAL leg only (the
    # deferral moved scheduled_at forward), never the operator's own
    # snooze choice folded into queue latency.
    assert "final leg" in html
    # The segments are separate ROWS for the same queue, not a blend:
    # the healthy queue has both a clean and a deferred row.
    wait_rows = [r for r in _rows_containing(html, "healthy_q") if "deferred" in r or "clean" in r]
    assert any("clean" in r and "deferred" not in r for r in wait_rows), (
        "the clean segment must render as its own row"
    )
    assert any("deferred" in r and "clean" not in r for r in wait_rows), (
        "the deferred segment must render as its own row"
    )


async def test_per_actor_toggle_groups_the_wait_table_by_actor_and_queue(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """``per_actor=true`` adds the (actor, queue) grouping; the default
    per-queue grouping never renders an actor the queue grouping hides."""
    mixed, _ = matrix
    default_html = await _html(mixed)
    assert "wait_only_a" not in default_html, (
        "the per-queue grouping must not render a per-actor row"
    )
    per_actor_html = await _html(mixed, "/admin/insights?per_actor=true")
    assert "wait_only_a" in per_actor_html, "the per-actor grouping must render the actor's row"


async def test_imbalance_badges_render_only_on_pathological_shapes(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """Utilization past the high-water mark (> 1) renders the
    over-capacity badge; NULL utilization over due work renders the
    unservable badge; the healthy queue's rows render neither."""
    mixed, _ = matrix
    html = await _html(mixed)
    assert _OVER_CAPACITY in html, "overcap_q (utilization 5.0) must render the badge"
    assert _UNSERVABLE in html, "starve_q (due work, nothing to serve it) must render the badge"
    # The visual verdict sits on the pathological queue's row only.
    healthy_rows = _rows_containing(html, "healthy_q")
    assert healthy_rows, "the healthy queue must render at all"
    for row in healthy_rows:
        assert "data-verdict" not in row, f"the healthy queue must carry no verdict badge: {row!r}"


async def test_overprovisioning_flags_the_idle_fleet_with_the_window_named(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """idle_q (live workers, zero due, zero completions in the window)
    renders the idle-fleet badge WITH the window named beside it."""
    mixed, _ = matrix
    html = await _html(mixed)
    assert _IDLE_FLEET in html, "idle_q must render the idle-fleet badge"
    assert "Idle fleet" in html
    # The window the verdict was computed over is named on the badge:
    # a single-window TRUE is a hypothesis, and the page says which
    # window the hypothesis is about.
    badge_rows = [r for r in _rows_containing(html, "idle_q") if _IDLE_FLEET in r]
    assert badge_rows and "24h" in badge_rows[0], "the idle-fleet badge must name its window"


async def test_drain_eta_renders_humanly_and_no_traffic_is_honest(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The healthy queue's eta renders as a human duration (2 due ÷
    (9 completions / 3600 s) = 800 s → "13m"); the quiet queue — due
    depth, NO window traffic — says so explicitly and NEVER renders an
    eta, because eta=0 would read as 'already drained'."""
    mixed, _ = matrix
    html = await _html(mixed)
    # healthy_q: 2 due ÷ (12 completions / 86400 s) = 14400 s → "4h".
    assert "4h" in html, "the drain eta must render as a human duration"
    quiet_rows = [r for r in _rows_containing(html, "quiet_q") if "no traffic" in r]
    assert quiet_rows, "an empty-traffic queue must say so"
    for row in quiet_rows:
        assert "4h" not in row and "0s" not in row, "a no-traffic queue must never render an eta"


async def test_cron_ledger_renders_counts_and_the_runaway_badge(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The ledger shows fires/cleared/outstanding per schedule; the
    schedule whose fires outrun clearance in BOTH 1h windows renders the
    runaway badge — at the 24h default the prior window is empty, so the
    verdict is honestly false and NO badge renders (the window
    dependency is part of the claim)."""
    mixed, _ = matrix
    default_html = await _html(mixed)
    assert _RUNAWAY not in default_html, (
        "at the 24h window the prior window is empty: no runaway verdict"
    )
    hour_html = await _html(mixed, "/admin/insights?window=1h")
    assert _RUNAWAY in hour_html, "the two-window runaway shape must render the badge"
    runaway_rows = [r for r in _rows_containing(hour_html, "runaway_a") if _RUNAWAY in r]
    assert runaway_rows, "the badge sits on the runaway schedule's row"
    row = runaway_rows[0]
    for needle in (
        "6",
        "1",
        "3",
        "0",
        "8",
    ):  # fires, cleared, prior fires, prior cleared, outstanding
        assert f">{needle}<" in row, f"the ledger must render the {needle} count"


async def test_healthy_fleet_renders_no_badges(matrix: tuple[_Mounted, _Mounted]) -> None:
    """THE red-first behavioral claim: the healthy fleet's page renders
    NO verdict badge anywhere — no over-capacity, no unservable, no
    idle fleet, no runaway."""
    _, healthy = matrix
    html = await _html(healthy)
    assert "data-verdict" not in html, "a healthy fleet must render no verdict badge: " + next(
        (line for line in html.splitlines() if "data-verdict" in line), ""
    )
    # And the healthy queue's drain eta renders humanly: 2 due ÷
    # (9 completions / 86400 s) = 19200 s → "5h 20m". The rate is the
    # window's OWN length, not an hour: 24h of traffic spread thin.
    assert "5h 20m" in html


async def test_window_picker_lists_the_declared_windows(
    matrix: tuple[_Mounted, _Mounted],
) -> None:
    """The picker offers exactly INSIGHTS_WINDOWS' keys — no all-time
    entry the archive retention would silently shrink."""
    _, healthy = matrix
    html = await _html(healthy)
    for key in INSIGHTS_WINDOWS:
        assert f"/admin/insights?window={key}" in html, f"missing picker entry: {key}"
    assert "All time" not in html


# ── The parameter contract ──────────────────────────────────────────────


async def test_unknown_query_param_is_400(matrix: tuple[_Mounted, _Mounted]) -> None:
    """An undeclared param is refused, never silently dropped."""
    mixed, _ = matrix
    resp = await _get(mixed, "/admin/insights?window=1h&actr=x")
    assert resp.status_code == 400


async def test_unknown_window_is_400(matrix: tuple[_Mounted, _Mounted]) -> None:
    """A window outside the closed set is a clean 400, never a silent
    fallback to some default (the actors page's contract)."""
    mixed, _ = matrix
    resp = await _get(mixed, "/admin/insights?window=42h")
    assert resp.status_code == 400


async def test_invalid_per_actor_value_is_400(matrix: tuple[_Mounted, _Mounted]) -> None:
    """The toggle's value space is closed too."""
    mixed, _ = matrix
    resp = await _get(mixed, "/admin/insights?per_actor=yes")
    assert resp.status_code == 400


# ── Auth ────────────────────────────────────────────────────────────────


@pytest.mark.fastapi
def test_insights_route_is_behind_the_auth_dependency(
    monkeypatch: pytest.MonkeyPatch, make_app: Any
) -> None:
    """The insights page rides the same router-level auth dependency as
    every other page: a denying auth dependency denies THIS route too."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    def deny_auth() -> None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = make_app(auth_dependency=deny_auth)
    response = client.get("/insights")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    assert response.status_code == 401  # pyright: ignore[reportUnknownVariableType]
