"""The admin jobs page's PREV turn is row-exact: a full round-trip walk
(forward-then-backward, backward-then-forward) drops no row, duplicates
no row, and never reorders one.

Ported red from the #564 red-team finding (their
``test_archive_cursor_union_seam_attacks.py`` xfail pin, on the branch
that carries the UNION archive shape): the backward (``cursor_dir=prev``)
fetch takes the ``_FETCH_SIZE`` rows NEAREST the cursor, re-sorts them
into forward display order, and the display truncation
``rows[:_PAGE_SIZE]`` is direction-blind. On a backward fetch the
overfetch row sits at the END of the re-sorted list (it is the row
nearest the cursor), so ``[:_PAGE_SIZE]`` keeps the FARTHEST row and
drops the row nearest the cursor — every FULL prev-page turn silently
strands exactly one row. Their example: backward from ``reference[95]``,
the fetch is ``reference[44:95]``, display shows ``reference[44:94]``,
and ``reference[94]`` is never served.

The shipped prev tests (``test_web_admin_pagination_order.py``) seed ten
rows — under one page — so the truncation never bites them. These pins
walk full multi-page populations and compare ROW-EXACT against a
reference ``ORDER BY finished_at DESC NULLS LAST, id DESC`` over the
same population: a duplicate, a drop, or a gap anywhere is a failure.

Every shown page below is computed by the route's own truncation
function (``_display_slice``, driven by the route's own
``_paginated_page`` direction resolution), so the pin fails when the
operator's page is wrong — not when a hand-copied slice disagrees with
the route.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest

pytest.importorskip(
    "fastapi"
)  # Why: this module attacks the admin's builders; the extras legs without fastapi must skip, not error at collection.
from jinja2 import Environment, PackageLoader

from taskq import (
    migrate as migrate_mod,  # Why: importorskip must precede the optional-import chain.
)
from taskq._ids import new_base62
from taskq.constants import _IDENT_RE  # pyright: ignore[reportPrivateUsage]
from taskq.web.admin._constants import (  # pyright: ignore[reportPrivateUsage]
    _FETCH_SIZE,
    _PAGE_SIZE,
)
from taskq.web.admin._factory import (  # pyright: ignore[reportPrivateUsage]  # Why: the template filters the route's own Environment registers; the render pins below run the SAME environment shape.
    _iso_attr,
    _time_ago,
)
from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]  # Why: the admin module's own builders are the queries under attack; a hand-copied SQL shape would drift from the real page.
    _ARCHIVE_COLS,
    _SORTABLE_ARCHIVE,
    _build_paginated_sql,
    _build_where,
    _cursor_field,
    _display_slice,
    _paginated_page,
)

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders the admin builders' own SQL; all values are $n-bound.

pytestmark = pytest.mark.integration

_TERMINAL = sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"})

# The exact cursor-width constants the page walk runs with: the boundary
# attacks size their populations from these, so a future change to either
# constant re-lands the attacks at the NEW boundary instead of silently
# testing nothing. The equality is pinned against the admin's own
# _FETCH_SIZE below (test_display_slice_boundary_arithmetic), not assumed.
FETCH = _PAGE_SIZE + 1


# ── module fixture ───────────────────────────────────────────────────────


@pytest.fixture(scope="module")
async def walk_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied."""
    schema = f"prev_walk_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


@pytest.fixture(scope="module")
async def walk_conn(pg_dsn: str, walk_schema: str) -> Any:
    conn = await asyncpg.connect(pg_dsn)
    yield conn
    await conn.close()


# ── the page machinery under attack ──────────────────────────────────────


def _page_sql(
    schema: str, cursor: tuple[str, str] | None, direction: str = "next"
) -> tuple[str, list[Any]]:
    """The archive tab's page statement, from the admin's own builders."""
    where, params = _build_where(_TERMINAL, None, None, None, None, None, None, None)
    return _build_paginated_sql(
        schema,
        "jobs_archive",
        _ARCHIVE_COLS,
        dict(_SORTABLE_ARCHIVE),
        where,
        list(params),
        cursor[0] if cursor else None,
        cursor[1] if cursor else None,
        direction,
        "finished_at",
        "desc",
    )


async def _fetch_page(
    conn: asyncpg.Connection, schema: str, cursor: tuple[str, str] | None, direction: str = "next"
) -> list[asyncpg.Record]:
    sql, args = _page_sql(schema, cursor, direction)
    return list(await conn.fetch(sql, *args))


def _shown(
    rows: list[asyncpg.Record], cursor: tuple[str, str] | None, direction: str
) -> list[asyncpg.Record]:
    """The display page the ROUTE would serve for this fetch: the route's
    own direction resolution and its own truncation function, applied to
    the fetched rows.  (Slicing here by hand would pin a copy of the
    semantics, not the semantics.)"""
    page = _paginated_page(
        dict(_SORTABLE_ARCHIVE),
        cursor[0] if cursor else None,
        cursor[1] if cursor else None,
        direction,
        "finished_at",
        "desc",
    )
    return _display_slice(rows, forward=page.forward)


def _render_job_table(
    *,
    jobs: list[dict[str, Any]],
    has_next: bool,
    has_prev: bool,
    next_cursor_at: str,
    next_cursor_id: str,
    prev_cursor_at: str,
    prev_cursor_id: str,
) -> str:
    """The route's own partial template, rendered with the context shape
    jobs_list builds (jobs.py's ``context`` dict): the same Environment
    construction _factory registers, so a render here is the render the
    route serves."""
    env = Environment(autoescape=True, loader=PackageLoader("taskq.web", "templates"))
    env.globals["base_path"] = ""
    env.filters["time_ago"] = _time_ago
    env.filters["iso_attr"] = _iso_attr
    return env.get_template("_partials/job_table.html").render(
        jobs=jobs,
        tab="archived",
        statuses=_TERMINAL,
        actor_filter="",
        queue_filter="",
        time_range="",
        time_from="",
        time_to="",
        identity_key="",
        fairness_key="",
        search="",
        tags_filter="",
        live="on",
        has_next=has_next,
        has_prev=has_prev,
        next_cursor_at=next_cursor_at,
        next_cursor_id=next_cursor_id,
        prev_cursor_at=prev_cursor_at,
        prev_cursor_id=prev_cursor_id,
        cursor_dir="prev",
        sort="",
        order="desc",
        total_rows=len(jobs),
        realtime_mode="off",
        mode_label="off",
        suppress_refresh=True,
    )


# ── seed shapes ──────────────────────────────────────────────────────────


async def _seed(
    conn: asyncpg.Connection,
    schema: str,
    finished_ats: list[datetime | None],
    *,
    status: str = "succeeded",
) -> list[UUID]:
    """Insert one archive row per entry of *finished_ats* (None = NULL
    finished_at), returning the ids in insertion order."""
    ids: list[UUID] = []
    for i, fin in enumerate(finished_ats):
        row_id = await conn.fetchval(
            f'INSERT INTO "{schema}".jobs_archive '
            "(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
            "created_at, scheduled_at, started_at, finished_at, archived_at, expire_at) "
            # $4 = the row's seed index: the md5 preimage keeps ids
            # deterministic and distinct per population row.
            f"VALUES (md5('prevwalk' || $4::text)::uuid, 'walk_actor', 'walk_q', "
            f"'{{\"v\": 1}}'::jsonb, $1::{schema}.job_status, 1, 3, "
            "'transient', $2::timestamptz - interval '1 day', "
            "$2::timestamptz - interval '1 day', "
            "COALESCE($3::timestamptz, $2::timestamptz - interval '12 hours'), "
            "$3::timestamptz, $2::timestamptz, $2::timestamptz + interval '365 days') "
            "RETURNING id",
            status,
            datetime.now(UTC),
            fin,
            str(i),
        )
        ids.append(row_id)
    return ids


async def _reference_ids(conn: asyncpg.Connection, schema: str) -> list[UUID]:
    """The population's exact total order, from a bare ORDER BY."""
    rows = await conn.fetch(
        f'SELECT id FROM "{schema}".jobs_archive WHERE status::text = ANY($1) '
        "ORDER BY finished_at DESC NULLS LAST, id DESC",
        _TERMINAL,
    )
    return [r["id"] for r in rows]


# ── the walks under attack ───────────────────────────────────────────────

Record = asyncpg.Record


async def _walk_forward_pages(
    conn: asyncpg.Connection, schema: str
) -> tuple[list[list[Record]], list[tuple[str, str]]]:
    """The operator's forward walk from the top: fetch, show the first
    _PAGE_SIZE rows (the route's own truncation), cursor from the last
    SHOWN row. Returns (shown pages, each page's first-row prev cursor)."""
    pages: list[list[Record]] = []
    boundaries: list[tuple[str, str]] = []
    cursor: tuple[str, str] | None = None
    while True:
        rows = await _fetch_page(conn, schema, cursor)
        if not rows:
            break
        assert len(rows) <= FETCH, f"fetch returned {len(rows)} rows (bound {FETCH})"
        shown = _shown(rows, cursor, "next")
        pages.append(shown)
        boundaries.append((_cursor_field(shown[0]["finished_at"]), str(shown[0]["id"])))
        last = shown[-1]
        cursor = (_cursor_field(last["finished_at"]), str(last["id"]))
    return pages, boundaries


async def _walk_backward_pages(
    conn: asyncpg.Connection, schema: str, start: tuple[str, str]
) -> list[list[Record]]:
    """The operator's backward walk from *start*: prev pages until the
    walk runs dry, each page shown in forward order (the route's own
    ``rows[:_PAGE_SIZE]`` truncation — the site under attack)."""
    pages: list[list[Record]] = []
    cursor = start
    while True:
        rows = await _fetch_page(conn, schema, cursor, "prev")
        if not rows:
            break
        shown = _shown(rows, cursor, "prev")
        pages.append(shown)
        first = shown[0]
        cursor = (_cursor_field(first["finished_at"]), str(first["id"]))
    return pages


def _flatten(pages: list[list[Record]]) -> list[UUID]:
    return [r["id"] for page in pages for r in page]


def _flatten_backward(pages: list[list[Record]]) -> list[UUID]:
    """Backward-walk pages in DISPLAY order: the walk fetches the page
    nearest the cursor FIRST, so each fetched page PREPENDS the rows seen
    so far (the operator walking back is rebuilding the forward order
    from the seam toward the top)."""
    result: list[UUID] = []
    for page in pages:
        result[:0] = [r["id"] for r in page]
    return result


def _assert_row_exact(walked: list[UUID], reference: list[UUID], label: str) -> None:
    """The only assertion that matters: walked == reference, with the
    failure named as dup / drop / gap."""
    seen_counts: dict[UUID, int] = {}
    for j in walked:
        seen_counts[j] = seen_counts.get(j, 0) + 1
    dups = {j: c for j, c in seen_counts.items() if c > 1}
    assert not dups, f"{label}: duplicated rows across the walk: {dups}"
    ref_set = set(reference)
    foreign = set(seen_counts) - ref_set
    assert not foreign, f"{label}: walked rows outside the population: {foreign}"
    missing = [j for j in reference if j not in seen_counts]
    assert not missing, f"{label}: DROPPED rows ({len(missing)} of {len(reference)}): {missing[:8]}"
    assert walked == reference, (
        f"{label}: the walk's ROW ORDER diverges from the reference "
        "ORDER BY finished_at DESC NULLS LAST, id DESC"
    )


# ── the attacks ──────────────────────────────────────────────────────────


async def test_backward_walk_row_exact_within_one_page(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """CONTROL (green unguarded): the prev-direction walk is row-exact when
    no page truncates — from a VALUE cursor with fewer than a fetch of
    rows before it, and from a NULL-seam cursor (cursor_at empty)."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i % 9, seconds=i) for i in range(2 * _PAGE_SIZE)]
    rows += [None] * _PAGE_SIZE
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    # Backward from a VALUE cursor 30 rows deep: one prev page, under the
    # truncation boundary, must be exactly reference[:30].
    deep = reference[30]
    value_cursor = await walk_conn.fetchrow(
        f'SELECT finished_at FROM "{walk_schema}".jobs_archive WHERE id = $1', deep
    )
    assert value_cursor is not None
    start = (_cursor_field(value_cursor["finished_at"]), str(deep))
    walked = _flatten_backward(await _walk_backward_pages(walk_conn, walk_schema, start))
    _assert_row_exact(walked, reference[:30], "backward-from-value-cursor walk")

    # Backward from a NULL-seam cursor (the walk's first NULL row): the
    # prev pages must rebuild the whole valued prefix — the OR arm's job.
    # Small population: 40 valued + 10 NULL, so the 40 rows before the
    # seam cursor fit one fetch — this isolates the backward NULL-seam
    # PREDICATE from the display-truncation defect pinned below.
    small = [base - timedelta(days=i % 9, seconds=i) for i in range(40)]
    small += [None] * 10
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, small)
    small_reference = await _reference_ids(walk_conn, walk_schema)
    first_null = await walk_conn.fetchrow(
        f'SELECT id FROM "{walk_schema}".jobs_archive WHERE finished_at IS NULL '
        "ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1"
    )
    assert first_null is not None
    start = ("", str(first_null["id"]))
    walked = _flatten_backward(await _walk_backward_pages(walk_conn, walk_schema, start))
    _assert_row_exact(walked, small_reference[:40], "backward-from-NULL-cursor walk")


async def test_prev_page_keeps_the_row_nearest_the_cursor(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """The #564 finding in its minimal form: 95 valued rows, prev from
    reference[95]'s cursor. The fetch is reference[44:95] re-sorted
    forward; the display must be reference[45:95] — the 50 rows NEAREST
    the cursor — never reference[44:94], which strands reference[94]."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i, seconds=i) for i in range(96)]
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    cursor_row = reference[95]
    row = await walk_conn.fetchrow(
        f'SELECT finished_at FROM "{walk_schema}".jobs_archive WHERE id = $1', cursor_row
    )
    assert row is not None
    page = await _fetch_page(
        walk_conn, walk_schema, (_cursor_field(row["finished_at"]), str(cursor_row)), "prev"
    )
    assert len(page) == FETCH, "the backward fetch overfetches by one"
    shown = [
        r["id"] for r in _shown(page, (_cursor_field(row["finished_at"]), str(cursor_row)), "prev")
    ]
    assert len(shown) == _PAGE_SIZE
    assert shown == reference[45:95], (
        "the prev page from reference[95] is not the 50 rows nearest the cursor: "
        f"got reference[{reference.index(shown[0])}:{reference.index(shown[0]) + len(shown)}], "
        "expected reference[45:95] — the direction-blind truncation dropped the "
        "row NEAREST the cursor"
    )


async def test_backward_walk_row_exact_across_multiple_pages(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """The ported #564 xfail pin, red then strict: the multi-page
    backward walk is row-exact — backward from a value cursor 95 rows
    deep must rebuild reference[:95] exactly, and from a NULL-seam
    cursor, the full valued prefix reference[:100]."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i % 9, seconds=i) for i in range(2 * _PAGE_SIZE)]
    rows += [None] * _PAGE_SIZE
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    deep = reference[2 * _PAGE_SIZE - 5]  # a valued row (the walk's head is values)
    value_cursor = await walk_conn.fetchrow(
        f'SELECT finished_at FROM "{walk_schema}".jobs_archive WHERE id = $1', deep
    )
    assert value_cursor is not None
    start = (_cursor_field(value_cursor["finished_at"]), str(deep))
    walked = _flatten_backward(await _walk_backward_pages(walk_conn, walk_schema, start))
    expected = reference[: 2 * _PAGE_SIZE - 5]
    _assert_row_exact(walked, expected, "backward-from-value-cursor walk")

    first_null = await walk_conn.fetchrow(
        f'SELECT id FROM "{walk_schema}".jobs_archive WHERE finished_at IS NULL '
        "ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1"
    )
    assert first_null is not None
    start = ("", str(first_null["id"]))
    walked = _flatten_backward(await _walk_backward_pages(walk_conn, walk_schema, start))
    expected = reference[: 2 * _PAGE_SIZE]
    _assert_row_exact(walked, expected, "backward-from-NULL-cursor walk")


async def test_full_round_trip_forward_then_backward_is_row_exact(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """Walk the FULL population forward, then turn prev at EVERY page
    boundary: each backward walk must rebuild exactly the rows before
    that boundary — no drop, no dup, no gap, no reorder. The forward leg
    itself is pinned row-exact first, so a red here is the backward
    turn's fault alone."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i % 9, seconds=i) for i in range(2 * _PAGE_SIZE)]
    rows += [None] * (_PAGE_SIZE + 13)  # a partial last page: 163 rows, 4 pages
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    pages, boundaries = await _walk_forward_pages(walk_conn, walk_schema)
    _assert_row_exact(_flatten(pages), reference, "forward walk")

    assert len(pages) >= 4, f"the population must span 4 pages: {[len(p) for p in pages]}"

    # Every page boundary's prev turn: the first row of page *idx* is the
    # seam; the backward walk from it must be exactly the idx * _PAGE_SIZE
    # rows displayed before it.
    for idx, start in enumerate(boundaries):
        walked = _flatten_backward(await _walk_backward_pages(walk_conn, walk_schema, start))
        expected = reference[: idx * _PAGE_SIZE]
        _assert_row_exact(walked, expected, f"backward turn from page {idx}'s boundary")


async def test_full_round_trip_backward_then_forward_is_row_exact(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """The operator lands deep (first row of the last forward page),
    clicks Prev back to the top, then Next back down: the backward leg
    must be exactly the prefix, the forward leg exactly the tail, and the
    union exactly the population — no row dropped or served twice."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i % 9, seconds=i) for i in range(2 * _PAGE_SIZE)]
    rows += [None] * (_PAGE_SIZE + 13)
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    _pages, boundaries = await _walk_forward_pages(walk_conn, walk_schema)
    deep_boundary = boundaries[-1]  # the first row of the LAST forward page
    deep_index = (len(boundaries) - 1) * _PAGE_SIZE

    # The backward leg: from the deep boundary back to the top.
    back_pages = await _walk_backward_pages(walk_conn, walk_schema, deep_boundary)
    back_ids = _flatten_backward(back_pages)
    expected_prefix = reference[:deep_index]
    assert len(back_pages) >= 2, (
        f"the backward leg must span multiple pages: {[len(p) for p in back_pages]}"
    )
    _assert_row_exact(back_ids, expected_prefix, "backward leg")

    # The forward leg resumes from the row immediately BEFORE the deep
    # boundary — the backward display's row nearest the walk's start (the
    # last row of the nearest-cursor page, reference[deep_index - 1]).
    # Resuming from the OLDEST page instead would re-cross pages the
    # backward leg already served — dups by composition, not by defect.
    last_seen = back_pages[0][-1]
    resume_cursor = (_cursor_field(last_seen["finished_at"]), str(last_seen["id"]))
    tail_pages: list[list[Record]] = []
    cursor: tuple[str, str] | None = resume_cursor
    while True:
        rows_page = await _fetch_page(walk_conn, walk_schema, cursor)
        if not rows_page:
            break
        shown = _shown(rows_page, cursor, "next")
        tail_pages.append(shown)
        last = shown[-1]
        cursor = (_cursor_field(last["finished_at"]), str(last["id"]))
    tail_ids = _flatten(tail_pages)

    _assert_row_exact(tail_ids, reference[deep_index:], "forward leg of the round trip")
    _assert_row_exact(back_ids + tail_ids, reference, "full backward-then-forward round trip")


async def test_prev_walk_survives_an_identical_finished_at_tie_seam(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """The tie seam driven through the PREV direction: tie groups sized
    to the fetch/page constants (exactly _PAGE_SIZE, _FETCH_SIZE,
    _FETCH_SIZE + 1, _FETCH_SIZE - 1 rows sharing one finished_at), each
    followed by an older distinct value — a full backward walk across
    every boundary INSIDE the tie groups must stay row-exact."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows: list[datetime | None] = []
    for width in (_PAGE_SIZE, FETCH, FETCH + 1, FETCH - 1):
        rows += [base] * width
        rows += [base - timedelta(days=width)]
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    # Walk backward from a cursor three pages deep: every page turn
    # crosses inside one of the tie groups.
    pages, boundaries = await _walk_forward_pages(walk_conn, walk_schema)
    assert len(pages) >= 4, f"the tie population must span 4+ pages: {[len(p) for p in pages]}"
    deep_boundary = boundaries[3]

    walked = _flatten_backward(await _walk_backward_pages(walk_conn, walk_schema, deep_boundary))
    expected = reference[: 3 * _PAGE_SIZE]
    _assert_row_exact(walked, expected, "backward walk across tie seams")


# ── the slice's boundary arithmetic, against the ACTUAL constants ────────


def _fake_rows(n: int, *, offset: int = 0) -> list[asyncpg.Record]:
    """Rows shaped enough for :func:`_display_slice` (it only slices)."""
    return cast("list[asyncpg.Record]", [{"id": offset + i} for i in range(n)])


def test_display_slice_boundary_arithmetic() -> None:
    """The display slice against the constants the fetch actually runs
    with — including the shapes that left the blind slice looking fine.

    A BACKWARD fetch whose inner LIMIT returned FEWER than _FETCH_SIZE
    rows (the last prev page: fewer than _PAGE_SIZE rows before the
    cursor) must serve the WHOLE fetch: Python's negative slice on a
    3-row list returns 3 — proven here, not assumed.  A fetch EXACTLY
    _PAGE_SIZE long serves whole in both directions.  A full _FETCH_SIZE
    fetch serves the rows NEAREST the cursor backward (its LAST
    _PAGE_SIZE) and the FARTHEST forward (its FIRST _PAGE_SIZE).  An
    empty fetch serves an empty page in both directions — no crash, no
    negative-slice wraparound.
    """
    assert _FETCH_SIZE == _PAGE_SIZE + 1, (
        "the walk's overfetch width is _PAGE_SIZE + 1; the boundary attacks "
        "below are sized from that relationship"
    )

    # The dry LAST prev page: only 3 rows before the cursor.
    short = _fake_rows(3)
    shown = _display_slice(short, forward=False)
    assert len(shown) == 3, "a short backward fetch serves every row it brought back"
    assert [r["id"] for r in shown] == [0, 1, 2], "the short fetch's order is untouched"
    # The explicit negative-slice proof the claim leans on:
    assert short[-_PAGE_SIZE:] == short, "[-_PAGE_SIZE:] on a 3-row list returns all 3"

    # A fetch exactly _PAGE_SIZE long: the whole fetch is the page, both ways.
    exact = _fake_rows(_PAGE_SIZE)
    assert [r["id"] for r in _display_slice(exact, forward=False)] == list(range(_PAGE_SIZE))
    assert [r["id"] for r in _display_slice(exact, forward=True)] == list(range(_PAGE_SIZE))

    # A FULL fetch (51 rows): the two directions drop OPPOSITE ends.
    full = _fake_rows(_FETCH_SIZE)
    back = [r["id"] for r in _display_slice(full, forward=False)]
    fwd = [r["id"] for r in _display_slice(full, forward=True)]
    assert len(back) == _PAGE_SIZE and len(fwd) == _PAGE_SIZE
    assert back == list(range(1, _PAGE_SIZE + 1)), (
        "backward: the LAST _PAGE_SIZE rows — the row nearest the cursor survives"
    )
    assert fwd == list(range(_FETCH_SIZE - 1)), (
        "forward: the FIRST _PAGE_SIZE rows — the overfetch marker is dropped"
    )
    assert back != fwd, "the directions are not the same slice on a full fetch"

    # An empty fetch (the prev turn with nothing before the cursor):
    # empty page, no crash, no wraparound.
    empty: list[asyncpg.Record] = []
    assert _display_slice(empty, forward=False) == []
    assert _display_slice(empty, forward=True) == []


async def test_prev_turn_from_the_population_s_first_row_is_an_honest_empty_page(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """A prev cursor parked ON the population's first row has no rows
    before it: the fetch comes back empty, the page derives
    has_prev=False / has_next=True, and the real template renders the
    empty state without a prev link — no crash, no fabricated turn."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i) for i in range(_PAGE_SIZE + 45)]
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)
    assert len(reference) == _PAGE_SIZE + 45

    top = reference[0]
    row = await walk_conn.fetchrow(
        f'SELECT finished_at FROM "{walk_schema}".jobs_archive WHERE id = $1', top
    )
    assert row is not None
    cursor = (_cursor_field(row["finished_at"]), str(top))

    fetched = await _fetch_page(walk_conn, walk_schema, cursor, "prev")
    assert fetched == [], "there is nothing before the population's first row"

    # The route's own derivation (jobs.py, the lines around _display_slice):
    shown = _shown(fetched, cursor, "prev")
    page = _paginated_page(
        dict(_SORTABLE_ARCHIVE), cursor[0], cursor[1], "prev", "finished_at", "desc"
    )
    overfetched = len(fetched) > _PAGE_SIZE
    has_prev = overfetched if not page.forward else page.paged_in
    has_next = True if not page.forward else overfetched
    assert page.forward is False, "the request asked for prev and the cursor parsed"
    assert shown == []
    assert has_prev is False, "no prev link may render for a turn with nothing before it"
    assert has_next is True, "the walk CAME from a next page; back-pointing is honest"

    # The real template, rendered with exactly this context shape: no
    # crash, no prev href, and the empty state — never a phantom page.
    html = _render_job_table(
        jobs=[dict(r) for r in shown],
        has_next=has_next,
        has_prev=has_prev,
        next_cursor_at="",
        next_cursor_id="",
        prev_cursor_at="",
        prev_cursor_id="",
    )
    assert "cursor_dir=prev" not in html, "the empty turn must not render a prev link"
    assert "No jobs found" in html, "the empty page renders the template's empty state"


async def test_short_prev_fetch_serves_the_rows_nearest_the_cursor(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """The last prev page of a walk — a backward fetch that returns
    FEWER than _PAGE_SIZE rows — serves all of them, row-exact against
    the reference prefix: the short fetch is the page, nothing stranded."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i) for i in range(_PAGE_SIZE + 45)]
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    # The population's row 3: exactly 3 rows before it.
    deep = reference[3]
    row = await walk_conn.fetchrow(
        f'SELECT finished_at FROM "{walk_schema}".jobs_archive WHERE id = $1', deep
    )
    assert row is not None
    cursor = (_cursor_field(row["finished_at"]), str(deep))

    fetched = await _fetch_page(walk_conn, walk_schema, cursor, "prev")
    assert len(fetched) == 3, "exactly 3 rows precede reference[53] — under the page size"
    shown = [r["id"] for r in _shown(fetched, cursor, "prev")]
    assert shown == reference[:3], (
        "the dry last prev page is exactly the 3 rows nearest the cursor, in forward order"
    )


async def test_next_prev_next_cycle_at_a_seam_is_row_exact(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """The resolution-order twin: the route resolves _paginated_page ONCE
    before the truncation, and the NEXT cursor is built from the SHOWN
    rows' last element — so a full next→prev→next cycle around one seam
    must re-serve each page row-exact.  This is the reader the moved
    resolution could have broken while fixing PREV."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i) for i in range(3 * _PAGE_SIZE - 5)]  # 145 rows, 3 pages
    await walk_conn.execute(f'TRUNCATE TABLE "{walk_schema}".jobs_archive CASCADE')
    await _seed(walk_conn, walk_schema, rows)
    reference = await _reference_ids(walk_conn, walk_schema)

    # Cache every row's finished_at first, so cursor construction mirrors
    # the route's (display_rows[-1] under the active sort column).
    value_rows = await walk_conn.fetch(f'SELECT id, finished_at FROM "{walk_schema}".jobs_archive')
    _ids_by_id = {r["id"]: r["finished_at"] for r in value_rows}

    def _cursor_of(row_id: UUID) -> tuple[str, str]:
        return (_cursor_field(_ids_by_id[row_id]), str(row_id))

    # 1. next: unpaged first page, then one turn.
    page1 = [r["id"] for r in _shown(await _fetch_page(walk_conn, walk_schema, None), None, "next")]
    assert page1 == reference[:_PAGE_SIZE]
    # 2. next again from page 1's last SHOWN row.
    page2 = [
        r["id"]
        for r in _shown(
            await _fetch_page(walk_conn, walk_schema, _cursor_of(page1[-1])),
            _cursor_of(page1[-1]),
            "next",
        )
    ]
    assert page2 == reference[_PAGE_SIZE : 2 * _PAGE_SIZE]
    # 3. prev from page 2's FIRST shown row (the seam the operator clicked).
    seam = _cursor_of(page2[0])
    back = [
        r["id"]
        for r in _shown(await _fetch_page(walk_conn, walk_schema, seam, "prev"), seam, "prev")
    ]
    assert back == reference[:_PAGE_SIZE], "prev from page 2's seam re-serves page 1 row-exact"
    # 4. next AGAIN, from the prev page's LAST shown row — the route's
    # next-cursor construction on a prev-served page.
    resume = _cursor_of(back[-1])
    again = [
        r["id"] for r in _shown(await _fetch_page(walk_conn, walk_schema, resume), resume, "next")
    ]
    assert again == reference[_PAGE_SIZE : 2 * _PAGE_SIZE], (
        "next from the prev page's last row re-serves page 2 row-exact"
    )

    # The #564 deep seam through the same cycle: prev from reference[95]
    # serves reference[45:95]; its next turn serves reference[95:].
    deep = _cursor_of(reference[95])
    deep_page = [
        r["id"]
        for r in _shown(await _fetch_page(walk_conn, walk_schema, deep, "prev"), deep, "prev")
    ]
    assert deep_page == reference[45:95]
    resume = _cursor_of(deep_page[-1])
    assert resume == (_cursor_field(_ids_by_id[reference[94]]), str(reference[94])), (
        "the next cursor rides the row NEAREST the seam — the one the blind slice dropped"
    )
    tail = [
        r["id"] for r in _shown(await _fetch_page(walk_conn, walk_schema, resume), resume, "next")
    ]
    assert tail == reference[95:], "next from the #564 prev page resumes exactly at the seam"
