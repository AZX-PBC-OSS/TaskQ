# ruff: noqa: S608  # Why: every schema interpolation is a fixed per-run test identifier, and every value is $-bound.
"""The jobs list's absolute time window survives its own pagination.

Two pre-existing defects the ops-surface differential observed end-to-end
(the differential's seed idiom — one plan, fixed timestamps, the real
router driven through httpx ASGI — is this module's too):

1. Page 2 silently lost the window. The jobs list's cursor pagination
   renders its page-turn URLs through the ``filter_qs`` macro in
   ``_partials/job_table.html``, which carried status/queue/actor/tags/
   search/identity/fairness — but NOT ``time_from``/``time_to``. A page 1
   filtered to an absolute window paginated into an UNFILTERED page 2:
   the cursor keys into a result set the next request no longer
   describes, so the operator silently read rows outside the window they
   asked about. The pin: page 1 filtered, then the walk through the REAL
   rendered cursor — page 2 must ALSO be filtered, to the seed oracle.

2. A lone bound was silently ignored. ``_parse_time_range`` required the
   PAIR and returned no filter at all otherwise — no 400, the operator's
   "everything since Tuesday" read as "everything". The code's own intent
   decides the fix: ``_build_where`` binds each bound as its OWN clause,
   and the client surface's ``JobFilter.created_before`` is a standalone
   predicate, so one side of a time window is a valid open-ended filter.
   (The partial-cursor 400s elsewhere in this admin — history.py,
   queues.py — are about cursors: their halves encode ONE seam no subset
   can describe. A time bound is a filter, and a filter narrows.) So a
   lone bound now filters, the other side open — pinned here against the
   seed plan on real Postgres, and at the unit boundary below.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote_plus

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")

from fastapi import FastAPI

from taskq._ids import new_job_id, new_uuid
from taskq.migrate import apply_pending
from taskq.web.admin import create_router, setup_admin_state

pytestmark = [pytest.mark.fastapi]

# Every seeded timestamp derives from this FIXED instant (the
# differential's idiom): the windows are absolute, so their expectations
# must not drift with the wall clock. Days in the past at authoring time.
_BASE = datetime(2026, 9, 20, 12, 0, 0, tzinfo=UTC)

# 51 in-window rows: one more than the page size (50), so a filtered page 1
# has exactly ONE page-2 row — any unfiltered page 2 is loud (it would show
# the older rows instead).
_N_INSIDE = 51
# Rows the window must exclude on each side: enough that an unfiltered
# page 2 (the pre-fix behavior) cannot pass by accident, and enough rows
# on the far side of _BASE for the lone-bound windows to have real
# populations on both sides of the cut.
_N_BEFORE = 6
_N_AFTER = 4


# ── The seed plan (built once, applied identically) ──────────────────────


@dataclass(frozen=True, slots=True)
class _SeedRow:
    """One seeded live job row — the oracle's own record."""

    id: uuid.UUID
    actor: str
    created_at: datetime


@dataclass(slots=True)
class _SeedPlan:
    inside: list[_SeedRow] = field(default_factory=list)
    before: list[_SeedRow] = field(default_factory=list)
    after: list[_SeedRow] = field(default_factory=list)

    @property
    def all_rows(self) -> list[_SeedRow]:
        return [*self.before, *self.inside, *self.after]


def _build_plan() -> _SeedPlan:
    """The three populations the windows partition.

    ``inside`` spans [_BASE, _BASE + 50min] one minute apart;
    ``before`` sits days under _BASE; ``after`` days over it. Distinct
    actors so a failure names which population leaked.
    """
    plan = _SeedPlan()
    for i in range(_N_INSIDE):
        plan.inside.append(_SeedRow(new_job_id(), "inside_actor", _BASE + timedelta(minutes=i)))
    for i in range(_N_BEFORE):
        plan.before.append(_SeedRow(new_job_id(), "before_actor", _BASE - timedelta(days=i + 1)))
    for i in range(_N_AFTER):
        plan.after.append(_SeedRow(new_job_id(), "after_actor", _BASE + timedelta(days=i + 1)))
    return plan


def _expected_order(rows: list[_SeedRow]) -> list[str]:
    """The live tab's ORDER BY (created_at DESC, id DESC), in Python."""
    ordered = sorted(rows, key=lambda r: (r.created_at, r.id), reverse=True)
    return [str(r.id) for r in ordered]


# The windows the pins exercise, as (label, query suffix, expected rows):
# each expectation re-derived from the seed plan, never hand-listed.
def _window_shapes(plan: _SeedPlan) -> list[tuple[str, str, list[_SeedRow]]]:
    from_str = quote_plus(_BASE.isoformat())
    to_str = quote_plus((_BASE + timedelta(minutes=50) + timedelta(seconds=1)).isoformat())
    return [
        # The full bracket: exactly the inside population (the upper bound
        # is INCLUSIVE, created_at <= time_to — hence the +1s so the row
        # at exactly _BASE + 50min stays in).
        (
            "bracketed window",
            f"&time_from={from_str}&time_to={to_str}",
            plan.inside,
        ),
        # Lone lower bound: everything after it — inside + after.
        (
            "lone time_from",
            f"&time_from={from_str}",
            [*plan.inside, *plan.after],
        ),
        # Lone upper bound: everything before it — the before population.
        (
            "lone time_to",
            f"&time_to={quote_plus((_BASE - timedelta(seconds=1)).isoformat())}",
            plan.before,
        ),
    ]


# ── The lab: one schema on the shared container, the real router ─────────


@dataclass
class _Lab:
    plan: _SeedPlan
    app: FastAPI
    pool: asyncpg.Pool


@pytest.fixture(scope="module")
async def lab(pg_dsn: str) -> AsyncIterator[_Lab]:
    """The real admin app on a real migrated+seeded schema.

    The dev-env trio is set HERE (not just via the autouse per-test
    fixture) because ``create_router`` loads settings at module-fixture
    time, which runs before the function-scoped autouse fixture would
    have set them (the differential module's idiom).
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("TASKQ_ENVIRONMENT", "dev")
    mp.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    mp.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    try:
        schema = f"tsadm_win_{new_uuid().hex[:10]}"
        setup_conn = await asyncpg.connect(pg_dsn)
        try:
            await setup_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await apply_pending(setup_conn, schema=schema)
        finally:
            await setup_conn.close()

        plan = _build_plan()
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.executemany(
                f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
                    retry_kind, status, attempt, tags, created_at, scheduled_at,
                    started_at, finished_at)
                VALUES ($1, $2, 'window', '{{"v":1}}'::jsonb, 3, 'transient', 'succeeded',
                    1, '{{}}'::text[], $3, $3, $4, $5)""",
                [
                    (
                        r.id,
                        r.actor,
                        r.created_at,
                        r.created_at + timedelta(minutes=1),
                        r.created_at + timedelta(minutes=5),
                    )
                    for r in plan.all_rows
                ],
            )
        finally:
            await conn.close()

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
        bundle = create_router(pool, schema=schema, base_path="/admin")
        app = FastAPI()
        setup_admin_state(app, bundle)
        app.include_router(bundle.router, prefix="/admin")
        yield _Lab(plan=plan, app=app, pool=pool)
        await pool.close()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
    finally:
        mp.undo()


# ── HTTP helpers (the web_admin idiom: httpx ASGI transport) ─────────────


async def _html(app: FastAPI, path: str) -> str:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(path)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}: {resp.text[:400]}"
    return resp.text


async def _get_json(app: FastAPI, path: str) -> Any:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(path)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}: {resp.text[:400]}"
    return resp.json()


_JOB_HREF_RE = re.compile(
    r'href="/admin/jobs/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"'
)
_NEXT_LINK_RE = re.compile(r'<a href="([^"]+)"[^>]*>\s*Next(?: page)?\s*<')


def _ordered_job_ids(html: str) -> list[str]:
    """The job ids the page renders, in row order (deduped: the id cell
    and the actions cell both link the same row)."""
    seen: list[str] = []
    for m in _JOB_HREF_RE.finditer(html):
        if m.group(1) not in seen:
            seen.append(m.group(1))
    return seen


def _next_link(html: str) -> str | None:
    m = _NEXT_LINK_RE.search(html)
    return m.group(1) if m else None


async def _walk(app: FastAPI, first_path: str, *, max_pages: int = 20) -> list[str]:
    """Follow the rendered Next cursor to exhaustion, collecting row order."""
    ids: list[str] = []
    path: str | None = first_path
    pages = 0
    while path is not None:
        html = await _html(app, path)
        ids.extend(_ordered_job_ids(html))
        path = _next_link(html)
        pages += 1
        assert pages < max_pages, f"the walk from {first_path} did not terminate"
    return ids


# ── Pins: the page turn carries the window; the walk matches the oracle ──


@pytest.mark.integration
async def test_page_turn_carries_the_absolute_time_window(lab: _Lab) -> None:
    """Page 1 filtered + page 2 ALSO filtered, walked via the REAL cursor.

    The seed makes the pre-fix failure maximally loud: 51 in-window rows
    means a filtered page 2 holds exactly ONE row; the pre-fix page 2 (the
    filter dropped) held the OLDER, out-of-window rows instead. The Next
    href itself is pinned too: the macro must carry BOTH bounds, the same
    ride status/queue/actor get.
    """
    (_label, window_qs, _expected) = _window_shapes(lab.plan)[0]
    page1 = await _html(lab.app, f"/admin/jobs?tab=live{window_qs}")

    page1_ids = _ordered_job_ids(page1)
    assert len(page1_ids) == 50, "page 1 is the page-size page of the 51 in-window rows"

    next_href = _next_link(page1)
    assert next_href is not None, "51 in-window rows must paginate"
    assert "time_from=" in next_href, (
        f"the page-turn URL dropped the window's lower bound: {next_href}"
    )
    assert "time_to=" in next_href, (
        f"the page-turn URL dropped the window's upper bound: {next_href}"
    )

    page2 = await _html(lab.app, next_href)
    page2_ids = _ordered_job_ids(page2)
    in_window = {str(r.id) for r in lab.plan.inside}
    leaked = [jid for jid in page2_ids if jid not in in_window]
    assert not leaked, (
        "page 2 served rows OUTSIDE the absolute time window — the page turn "
        f"dropped the filter: {leaked}"
    )
    assert page2_ids == _expected_order(lab.plan.inside)[50:], (
        "page 2 must be exactly the one in-window row the keyset seam points at"
    )


@pytest.mark.integration
async def test_windowed_walks_match_the_seed_oracle(lab: _Lab) -> None:
    """Every window shape, walked to exhaustion, equals the seed plan's
    own Python-derived order — the differential harness's shapes, pinned
    against one engine (the shapes ARE the oracle here, not another
    engine). A pre-fix walk diverges on page 2 for every bracketed shape.
    """
    for label, window_qs, expected_rows in _window_shapes(lab.plan):
        walked = await _walk(lab.app, f"/admin/jobs?tab=live{window_qs}")
        assert walked == _expected_order(expected_rows), (
            f"the walk of the {label} returned rows the seed oracle does not "
            "(pre-fix: page 2 dropped the filter)"
        )


@pytest.mark.integration
async def test_lone_time_from_is_an_open_ended_window(lab: _Lab) -> None:
    """A lone lower bound FILTERS ("everything after X") — it must not be
    silently ignored. Pre-fix: _parse_time_range required the pair, so
    this request served the UNFILTERED list. The count endpoint parses
    through the same function; it must agree with the walk.
    """
    (_label, _qs, _expected) = _window_shapes(lab.plan)[0]
    path = f"/admin/jobs?tab=live&time_from={quote_plus(_BASE.isoformat())}"

    walked = await _walk(lab.app, path)
    expected = _expected_order([*lab.plan.inside, *lab.plan.after])
    assert walked == expected, (
        "a lone time_from must select everything after it (the other bound "
        "open), not degrade to no filter"
    )

    count = await _get_json(
        lab.app, f"/admin/jobs/count?tab=live&time_from={quote_plus(_BASE.isoformat())}"
    )
    assert count["count"] == len(expected), "the count endpoint must agree with the walk"


@pytest.mark.integration
async def test_lone_time_to_is_an_open_ended_window(lab: _Lab) -> None:
    """A lone upper bound FILTERS ("everything before X") — same contract,
    mirrored side."""
    path = f"/admin/jobs?tab=live&time_to={quote_plus((_BASE - timedelta(seconds=1)).isoformat())}"

    walked = await _walk(lab.app, path)
    expected = _expected_order(lab.plan.before)
    assert walked == expected, (
        "a lone time_to must select everything before it (the other bound "
        "open), not degrade to no filter"
    )

    count = await _get_json(
        lab.app,
        f"/admin/jobs/count?tab=live&time_to={quote_plus((_BASE - timedelta(seconds=1)).isoformat())}",
    )
    assert count["count"] == len(expected), "the count endpoint must agree with the walk"


@pytest.mark.integration
async def test_a_lone_absolute_bound_outranks_a_named_range(lab: _Lab) -> None:
    """An explicit bound beside a named range keeps the pair's precedence:
    the caller's explicit instants are the more specific ask. (Pre-fix a
    lone bound fell THROUGH to the named range — the same silent-ignore
    bug wearing a different hat.)"""
    path = f"/admin/jobs?tab=live&time_from={quote_plus(_BASE.isoformat())}&time_range=24h"
    walked = await _walk(lab.app, path)
    # The named range would serve the empty set (every seeded row is days
    # old); the explicit lower bound serves everything after _BASE.
    assert walked == _expected_order([*lab.plan.inside, *lab.plan.after]), (
        "a lone absolute bound must outrank a named range, not fall through to it"
    )


@pytest.mark.integration
async def test_a_malformed_lone_bound_is_still_a_clean_400(lab: _Lab) -> None:
    """Validating a lone bound as a REAL filter must not relax the parse:
    garbage is still the family's clean 400 (parse_time_filter), lone or
    paired."""
    for qs in ("&time_from=not-a-timestamp", "&time_to=not-a-timestamp"):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=lab.app), base_url="http://test"
        ) as client:
            resp = await client.get(f"/admin/jobs?tab=live{qs}")
        assert resp.status_code == 400, f"{qs} alone must still 400 on garbage"


# ── Unit pins (no container): the two source fixes at their seams ────────


def test_parse_time_range_lone_bounds_are_open_ended_windows() -> None:
    """_parse_time_range passes a LONE bound through as the open-ended
    window it is (the other side None). Pre-fix this returned no filter
    at all — the silent-ignore the E2E pins catch from above."""
    from datetime import datetime as dt

    t0 = dt(2026, 9, 20, 12, 0, 0, tzinfo=UTC)
    t1 = dt(2026, 9, 21, 12, 0, 0, tzinfo=UTC)
    from taskq.web.admin.jobs import _parse_time_range

    assert _parse_time_range(time_range=None, time_from=t0, time_to=None) == (t0, None, None)
    assert _parse_time_range(time_range=None, time_from=None, time_to=t1) == (None, t1, None)
    # The pair: unchanged (the original contract).
    assert _parse_time_range(time_range=None, time_from=t0, time_to=t1) == (t0, t1, None)


def test_parse_time_range_lone_bound_outranks_a_named_range() -> None:
    """Either absolute bound wins over a named range — the precedence the
    pair always had, extended to the lone-bound case."""
    from datetime import datetime as dt

    t0 = dt(2026, 9, 20, 12, 0, 0, tzinfo=UTC)
    from taskq.web.admin.jobs import _parse_time_range

    assert _parse_time_range(time_range="1h", time_from=t0, time_to=None) == (t0, None, None)
    assert _parse_time_range(time_range="1h", time_from=None, time_to=t0) == (None, t0, None)


def test_pagination_macro_carries_the_time_bounds(stub_pool: Any) -> None:
    """The pagination macro's filter_qs must render time_from/time_to into
    every page-turn URL — the exact seam that dropped them pre-fix (the
    macro carried status/queue/actor/tags/search and even the relative
    time_range, but not the absolute bounds)."""
    from taskq._ids import new_uuid
    from taskq.web.admin import create_router

    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    row: dict[str, Any] = {
        "id": "abc-123",
        "actor": "send_email",
        "queue": "default",
        "status": "failed",
        "created_at": "2025-01-01T12:00:00",
        "scheduled_at": "2025-01-01T12:00:00",
        "started_at": None,
        "finished_at": None,
        "duration_ms": None,
        "attempt": 1,
        "max_attempts": 3,
        "retry_kind": "transient",
        "priority": 5,
        "identity_key": None,
        "fairness_key": None,
        "progress_state": None,
        "error_message": None,
    }
    html = bundle.templates.get_template("_partials/job_table.html").render(
        jobs=[row],
        tab="live",
        statuses=["failed"],
        all_statuses=["failed"],
        active_statuses=[],
        terminal_statuses=["failed"],
        has_next=True,
        has_prev=False,
        next_cursor_at="2025-01-01T11:00:00",
        next_cursor_id=str(new_uuid()),
        prev_cursor_at="",
        prev_cursor_id="",
        cursor_dir="next",
        sort="",
        order="desc",
        time_from="2025-01-01T00:00:00+00:00",
        time_to="2025-01-02T00:00:00+00:00",
        live="on",
    )
    links = [
        line.split('href="', 1)[1].split('"', 1)[0]
        for line in html.splitlines()
        if "cursor_dir=" in line and 'href="' in line
    ]
    assert links, "the paged table must render a Next link"
    for href in links:
        assert "time_from=2025-01-01T00%3A00%3A00%2B00%3A00" in href, href
        assert "time_to=2025-01-02T00%3A00%3A00%2B00%3A00" in href, href
