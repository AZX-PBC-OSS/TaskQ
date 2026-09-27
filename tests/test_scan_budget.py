"""Scan-regression pins: the hot claim/poll/page paths must run on indexes
at depth, on the REAL seeded tables.

The scale campaign (benchmarks/backlog_throughput.py,
benchmarks/archive_scale.py, artifacts in benchmarks/results/) measured the
hot paths at real backlog and archive depth. This module pins the plan
SHAPES those numbers depend on, so a future regression to a scan goes red
in CI instead of redrawing a curve nobody is watching. The doctrine is the
sibling depth-bound pin's (tests/test_dispatch_backlog_depth_bound.py):
EXPLAIN-based assertions on production SQL rendered against seeded tables,
row counts (not wall time) as the assertion medium.

Pins, one per hot path:

* **claim** — the dispatch CTE (both queue modes) at a 100k due backlog:
  no node may Seq Scan ``jobs``, and the widest node's row work stays
  bounded (the depth-bound pin's contract, re-stated at campaign depth).
* **poll** — the claimable probe (the dispatch loop's every-tick
  existence probe): bounded row work at depth, no ``jobs`` scan.
* **admin archive page** — the archive tab's keyset walk at archive
  depth: page 1 and a deep value-cursor page must both run on
  ``jobs_archive_page_idx`` (migration 01.00.21_01) as Index Scan seeks,
  never a Seq Scan or a Filter-only scan. This is the regression pin for
  the campaign's finding: before 01.00.21_01 NO shipped index served the
  tab's ``finished_at DESC NULLS LAST, id DESC`` ordering (every page
  top-N sorted the whole archive — the committed sweep artifact's
  3.81 ms/page @ 10k → 135.82 ms/page @ 2M plain points; the 10M
  red/green legs in archive-scale.json's pagination_pathology: 690 ms
  vs 0.47 ms), and before the builder's UNION split the OR-wrapped
  cursor predicate defeated index-condition extraction (23.9 ms per
  cursor page, 58k rows removed by filter, at a 100k archive; the bare
  row compare seeks at 0.13 ms —
  benchmarks/results/archive-scale-red-probe.json, the full A/B
  series). The
  behavioral half walks five real pages against a reference ORDER BY and
  asserts row identity, so the two-branch shape cannot quietly drop or
  duplicate rows (the archive seed carries NULL finished_at rows: the
  NULL tail the second branch exists to serve).

The dispatch EXPLAIN ANALYZE legs execute the claim UPDATE — each re-seeds
its own backlog first, exactly like the sibling depth-bound pin.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

pytest.importorskip(
    "fastapi"
)  # Why: this module pins the admin page's builders; the extras legs without fastapi must skip, not error at collection.

from taskq import (
    migrate as migrate_mod,  # Why: importorskip must precede the optional-import chain.
)
from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import (
    DISPATCH_CLAIMABLE_PROBE_SQL,
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: after importorskip.
)
from taskq.web.admin._constants import (  # pyright: ignore[reportPrivateUsage]  # Why: the admin constants module publishes the page size the page statements fetch.
    _FETCH_SIZE,
)
from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]  # Why: the admin module's own builders are the queries being pinned; a hand-copied shape would drift from the real page.
    _ARCHIVE_COLS,
    _SORTABLE_ARCHIVE,
    _build_paginated_sql,
    _build_where,
)

pytestmark = [pytest.mark.integration, pytest.mark.load_sensitive]

_DEPTH = 100_000
_ARCHIVE_ROWS = 100_000
_LIMIT_N = 50
_OVERSAMPLE = 2
_LOCK_LEASE = timedelta(seconds=30)
_ACTOR = "scan_probe"
_QUEUE = "scan_q"

# The depth-bound pin's row-work bound (its arithmetic: limit_n *
# oversample candidate rows + the locked/eligible stages at limit_n ≈ 150
# rows on this module's single (actor, queue) seed; 4x headroom absorbs
# bounded shapes' constant factors — only backlog-proportional work
# exceeds it). Reused verbatim so the two pins state ONE contract.
_NODE_ROW_BOUND = 600

# The archive page pin's filter budget. The page fetch is _FETCH_SIZE
# rows (both union branches, each LIMIT-bound at the fetch size); the
# seed's status filter removes nothing (every archive row is terminal).
# A filter-only scan of the seeded archive removes ~tens of thousands of
# rows per page — four pages' fetch size is generous headroom for the
# seeks, and three orders of magnitude below a filter scan.
_PAGE_FILTER_BOUND = 4 * (_FETCH_SIZE)

# Archive seed shape: 2% of rows carry NULL finished_at (archived-
# unfinished rows — the NULL tail the page walk's second branch serves),
# the rest spread over a 1-360d finished_at window.
_NULL_FINISHED_MOD = 50


# ── module fixture ───────────────────────────────────────────────────────


@pytest.fixture(scope="module")
async def scan_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, one registered actor."""
    schema = f"scan_budget_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            _ACTOR,
            _QUEUE,
        )
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _seed_pending_backlog(conn: asyncpg.Connection, schema: str, depth: int) -> None:
    """Re-seed ``depth`` due pending rows (the sibling's seed shape)."""
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind, fairness_key) "
        "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
        "(g % 3)::smallint, clock_timestamp() - interval '1 minute', 3, "
        "'transient', $3 FROM generate_series(1, $4::int) AS g",
        _ACTOR,
        _QUEUE,
        "cohort_a",
        depth,
    )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')


async def _seed_archive(conn: asyncpg.Connection, schema: str, rows: int) -> None:
    """Seed the archive corpus once per module: terminal rows, 1-360d
    finished_at window, a NULL-finished tail, status mix, one tag."""
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs_archive CASCADE')
    await conn.execute(
        f'INSERT INTO "{schema}".jobs_archive '
        "(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
        "created_at, scheduled_at, started_at, finished_at, archived_at, expire_at, tags, priority) "
        "SELECT md5(('a' || g)::text)::uuid, 'a' || (g % 8), 'q' || (g % 4), "
        "'{\"v\": 1}'::jsonb, "
        "(CASE WHEN g % 20 = 0 THEN 'failed' ELSE 'succeeded' END)::"
        f'"{schema}".job_status, '
        "1, 3, 'transient', "
        "$1::timestamptz - make_interval(days => 1 + (g % 360)), "
        "$1::timestamptz - make_interval(days => 1 + (g % 360)), "
        "$1::timestamptz - make_interval(days => 1 + (g % 360)) + interval '5 min', "
        f"(CASE WHEN g % {_NULL_FINISHED_MOD} = 0 THEN NULL "
        "ELSE $1::timestamptz - make_interval(days => 1 + (g % 360)) END), "
        "$1::timestamptz, $1::timestamptz + interval '365 days', "
        "ARRAY['regular'], (g % 3)::smallint "
        "FROM generate_series(1, $2::int) AS g",
        datetime.now(UTC),
        rows,
    )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs_archive')


@pytest.fixture(scope="module")
async def archive_seeded(pg_dsn: str, scan_schema: str) -> None:
    """The archive corpus, seeded once for the page pins (read-only legs)."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _seed_archive(conn, scan_schema, _ARCHIVE_ROWS)
    finally:
        await conn.close()


# ── plan-walking helpers ─────────────────────────────────────────────────


def _walk(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Every plan node, flattened, parents before children."""
    out: list[dict[str, Any]] = []
    stack = [plan]
    while stack:
        node = stack.pop()
        out.append(node)
        stack.extend(node.get("Plans") or [])
    return out


def _widest_row_touches(plan: dict[str, Any]) -> tuple[float, str]:
    """(rows touched, label) for the widest node — the bounded-work oracle.

    Touched = ``(Actual Rows + Rows Removed by Filter) * Actual Loops``:
    every row the node VISITED, emitted or filtered. The sibling
    depth-bound pin counts emitted rows (its plans emit their work); the
    probe pages pinned here are LIMIT-bounded existences whose cost hides
    in filter removals — a LIMIT-1 seq scan over a dense match visits 1
    row, over a non-matching backlog every row — and only ``Rows Removed
    by Filter`` shows the difference. Panning the touched count pins the
    work itself: a scan regression to unbounded row visits goes red
    regardless of which node shape carries it.
    """
    best = (0.0, "no nodes")
    for node in _walk(plan):
        emitted = float(node.get("Actual Rows", 0) or 0)
        removed = float(node.get("Rows Removed by Filter", 0) or 0)
        loops = int(node.get("Actual Loops", 1) or 1)
        node_type = str(node.get("Node Type", "unknown"))
        relation = node.get("Relation Name")
        label = node_type if relation is None else f"{node_type} on {relation}"
        if (emitted + removed) * loops > best[0]:
            best = ((emitted + removed) * loops, label)
    return best


async def _explain(conn: asyncpg.Connection, sql: str, *params: object) -> dict[str, Any]:
    """EXPLAIN (ANALYZE, FORMAT JSON) with bound params; returns the top Plan."""
    raw = await conn.fetchval(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}", *params)
    document: Any = json.loads(raw) if isinstance(raw, str) else raw
    return document[0]["Plan"]


def _assert_no_scan_on(plan: dict[str, Any], relation: str) -> None:
    """No node of the plan may Seq Scan *relation*."""
    for node in _walk(plan):
        if node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == relation:
            pytest.fail(
                f"plan Seq Scans {relation} — a hot path regressed to a scan at depth: "
                f"nodes={[(n['Node Type'], n.get('Relation Name')) for n in _walk(plan)]}"
            )


# ── claim pins ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("variant", "sql"),
    (
        ("strict_fifo", DISPATCH_STRICT_FIFO_SQL),
        ("round_robin", DISPATCH_ROUND_ROBIN_SQL),
    ),
)
async def test_claim_plan_is_index_served_at_depth(
    pg_dsn: str, scan_schema: str, variant: str, sql: str
) -> None:
    """The dispatch CTE at a 100k due backlog: no Seq Scan of ``jobs``, and
    the widest node's row work at the sibling depth-bound pin's bound."""
    rendered = sql.format(schema=scan_schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _seed_pending_backlog(conn, scan_schema, _DEPTH)
        plan = await _explain(
            conn, rendered, [_QUEUE], _LIMIT_N, new_uuid(), _LOCK_LEASE, _OVERSAMPLE
        )
        widest, label = _widest_row_touches(plan)
        assert widest <= _NODE_ROW_BOUND, (
            f"{variant}: at a {_DEPTH:,}-row due backlog the dispatch plan's widest node "
            f"({label}) did {widest:.0f} rows of work — backlog-proportional, where a "
            f"depth-bounded dispatch does at most ~{_LIMIT_N * _OVERSAMPLE} candidate/locked "
            f"rows (bound {_NODE_ROW_BOUND}, the depth-bound pin's arithmetic). "
            f"Nodes: {[(n['Node Type'], n.get('Relation Name')) for n in _walk(plan)]}"
        )
    finally:
        await conn.close()


async def test_poll_probe_plan_is_index_served_at_depth(pg_dsn: str, scan_schema: str) -> None:
    """The claimable probe — the dispatch loop's every-tick existence
    probe — stays bounded at depth: no Seq Scan of ``jobs``, widest node
    at the row-work bound. The probe is what a poll-only worker executes
    every tick against the backlog; a scan here is a per-tick tax at
    every poll interval."""
    rendered = DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=scan_schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _seed_pending_backlog(conn, scan_schema, _DEPTH)
        plan = await _explain(conn, rendered, [_QUEUE])
        widest, label = _widest_row_touches(plan)
        assert widest <= _NODE_ROW_BOUND, (
            f"the claimable probe's widest node ({label}) did {widest:.0f} rows of work at a "
            f"{_DEPTH:,}-row due backlog — backlog-proportional (bound {_NODE_ROW_BOUND}). "
            f"Nodes: {[(n['Node Type'], n.get('Relation Name')) for n in _walk(plan)]}"
        )
    finally:
        await conn.close()


# ── archive page pins ────────────────────────────────────────────────────


def _archive_page_sql(schema: str, cursor: tuple[str, str] | None) -> tuple[str, list[Any]]:
    """The archive tab's page statement, from the admin's own builders."""
    terminal = sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"})
    where, params = _build_where(terminal, None, None, None, None, None, None, None)
    return _build_paginated_sql(
        schema,
        "jobs_archive",
        _ARCHIVE_COLS,
        _SORTABLE_ARCHIVE,
        where,
        list(params),
        cursor[0] if cursor else None,
        cursor[1] if cursor else None,
        "next",
        "finished_at",
        "desc",
    )


async def _page_rows(
    conn: asyncpg.Connection, schema: str, cursor: tuple[str, str] | None
) -> list[asyncpg.Record]:
    sql, args = _archive_page_sql(schema, cursor)
    return await conn.fetch(sql, *args)


async def test_archive_first_page_is_index_served(
    pg_dsn: str, scan_schema: str, archive_seeded: None
) -> None:
    """Page 1 of the archive tab at a 100k archive: an Index Scan seek on
    the page index, no Seq Scan, no Sort, filter removals bounded.

    Before migration 01.00.21_01 no shipped index served the tab's
    ``finished_at DESC NULLS LAST, id DESC`` ordering: every page — the
    first page included — top-N sorted the whole archive (16.6 ms at
    100k, the red probe in the campaign artifact; the committed sweep's
    3.81→135.82 ms/page over 10k→2M rows is this shape at scale).
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        sql, args = _archive_page_sql(scan_schema, None)
        plan = await _explain(conn, sql, *args)
        _assert_no_scan_on(plan, "jobs_archive")
        sort_nodes = [n for n in _walk(plan) if n.get("Node Type", "").startswith("Sort")]
        assert not sort_nodes, (
            f"the archive tab's first page plans a Sort at a {_ARCHIVE_ROWS:,}-row archive — "
            f"an unserved ordering, the pre-01.00.21_01 shape "
            f"(nodes: {[(n['Node Type'], n.get('Relation Name')) for n in _walk(plan)]})"
        )
        index_names = [
            n.get("Index Name", "") for n in _walk(plan) if "Index" in n.get("Node Type", "")
        ]
        assert "jobs_archive_page_idx" in index_names, (
            f"the archive tab's first page does not run on jobs_archive_page_idx: {index_names}"
        )
    finally:
        await conn.close()


async def test_archive_cursor_page_seeks_not_filters(
    pg_dsn: str, scan_schema: str, archive_seeded: None
) -> None:
    """A deep VALUE-cursor page: the row-wise compare must extract as an
    Index Cond (the seek), not degrade to a Filter over the index.

    This is the pin for the campaign's second finding: the builder's
    single-statement shape wrapped the keyset predicate as
    ``(finished_at IS NULL OR row-compare)``; the OR defeated the
    extraction and every cursor page filter-scanned from the index start
    (23.9 ms/page, 58k rows removed, at a 100k archive — measured in the
    campaign artifact's pagination_pathology). The shipped UNION shape
    seeks both branches; a regression to the OR shape re-appears here as
    Rows Removed by Filter ~ the walked depth.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        # A cursor ~half the archive deep: page 1, take the last row.
        first = await _page_rows(conn, scan_schema, None)
        assert len(first) == _FETCH_SIZE
        last = first[-1]
        assert last["finished_at"] is not None, "seed ordered a NULL row first"
        cursor = (last["finished_at"].isoformat(), str(last["id"]))

        sql, args = _archive_page_sql(scan_schema, cursor)
        plan = await _explain(conn, sql, *args)
        _assert_no_scan_on(plan, "jobs_archive")

        # The seek: some index node carries a row-wise Index Cond on the
        # keyset tuple, and filter removals across the whole plan stay at
        # page scale (a filter scan at this cursor's depth removes ~
        # _ARCHIVE_ROWS / 2 rows per page).
        seeks = [
            n
            for n in _walk(plan)
            if "Index" in n.get("Node Type", "")
            and n.get("Index Cond") is not None
            and "ROW" in str(n.get("Index Cond"))
        ]
        assert seeks, (
            "the archive tab's cursor page lost its row-wise index seek — the OR-wrapped "
            "predicate shape is back (the pre-UNION regression): "
            f"nodes={[(n['Node Type'], n.get('Relation Name'), n.get('Index Cond'), n.get('Filter')) for n in _walk(plan)]}"
        )
        removed = sum(int(n.get("Rows Removed by Filter", 0) or 0) for n in _walk(plan))
        assert removed <= _PAGE_FILTER_BOUND, (
            f"the cursor page filtered {removed} rows at a {_ARCHIVE_ROWS:,}-row archive — "
            f"a filter scan, not a seek (bound {_PAGE_FILTER_BOUND})"
        )
    finally:
        await conn.close()


async def test_archive_walk_matches_reference_order(
    pg_dsn: str, scan_schema: str, archive_seeded: None
) -> None:
    """Behavioral half: five walked pages are exactly the reference
    ordering's next five pages — no dropped rows, no duplicates, NULL
    tail served (the two-branch UNION shape must be row-exact).

    The archive seed carries NULL-finished rows (every
    {_NULL_FINISHED_MOD}-th): the rows only the second branch can reach.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        walked: list[UUID] = []
        cursor: tuple[str, str] | None = None
        for _page in range(5):
            rows = await _page_rows(conn, scan_schema, cursor)
            assert rows, "page walk ran dry before five pages"
            walked.extend(r["id"] for r in rows)
            fin = rows[-1]["finished_at"]
            cursor = (fin.isoformat() if fin else "", str(rows[-1]["id"]))
        reference = await conn.fetch(
            f'SELECT id FROM "{scan_schema}".jobs_archive '
            "WHERE status::text = ANY($1) "
            "ORDER BY finished_at DESC NULLS LAST, id DESC "
            f"LIMIT {len(walked)}",
            sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"}),
        )
        assert [r["id"] for r in reference] == walked, (
            "the walked pages diverge from the reference ordering — the two-branch "
            "page shape dropped, duplicated, or reordered rows"
        )
        # The NULL tail must actually be reachable: keep walking from the
        # point the values run out and confirm NULL-finished rows arrive
        # (they exist in the seed; a shape that drops them strands them).
        while True:
            rows = await _page_rows(conn, scan_schema, cursor)
            if not rows:
                break
            walked.extend(r["id"] for r in rows)
            fin = rows[-1]["finished_at"]
            cursor = (fin.isoformat() if fin else "", str(rows[-1]["id"]))
        null_seen = await conn.fetchval(
            f'SELECT count(*) FROM "{scan_schema}".jobs_archive WHERE finished_at IS NULL'
        )
        assert null_seen > 0, "seed lost its NULL tail"
        reference_all = await conn.fetch(
            f'SELECT id FROM "{scan_schema}".jobs_archive '
            "WHERE status::text = ANY($1) "
            "ORDER BY finished_at DESC NULLS LAST, id DESC",
            sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"}),
        )
        assert [r["id"] for r in reference_all] == walked, (
            "the full walked archive diverges from the reference ordering — the NULL "
            "tail was dropped or reordered by the two-branch page shape"
        )
    finally:
        await conn.close()
