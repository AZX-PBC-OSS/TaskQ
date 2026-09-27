"""Adversarial row-exactness attacks on the archive tab's two-branch UNION
cursor shape (the campaign's product change, src/taskq/web/admin/jobs.py's
``_build_paginated_sql`` + backend/_cursor.py's ``null_tail`` flag).

The UNION split exists for PLAN shape (the OR-wrapped predicate defeated
index-condition extraction), but it CHANGED THE ROW SET the page walk
serves: one statement became two ordered branches appended and re-sorted.
The behavioral pin in test_scan_budget.py walks the campaign's own seed;
this module attacks the seams that seed cannot produce:

* a tie group of IDENTICAL finished_at values spanning a page boundary —
  the branch-one keyset seek must hand the tie's remainder across the
  seam exactly once (the id tiebreak decides membership);
* a tie group sized to land EXACTLY on the fetch boundary (_FETCH_SIZE
  and _PAGE_SIZE widths) — a LIMIT that stops mid-tie and a page turn
  that must resume inside the same tie;
* NULL finished_at rows MIXED into a page's span — the values branch
  running dry mid-page with the NULL branch supplying the rest, the two
  branches' concatenation reconstructing the exact NULLS LAST total order;
* an ALL-NULL population (branch one empty — the walk lives entirely in
  the NULL-seam single-statement shape) and a NO-NULL population (branch
  two empty);
* the BACKWARD (``cursor_dir=prev``) walk across the same seams — the
  UNION is forward-only, but the operator-facing prev page must be
  row-exact too, including a seam INSIDE the NULL range (a NULL
  cursor_at) where the backward predicate grows its
  ``OR finished_at IS NOT NULL`` arm.

Every construction is compared ROW-EXACT against a reference
``ORDER BY finished_at DESC NULLS LAST, id DESC`` over the same
population: a duplicate, a drop, or a gap anywhere is a failure — the
history walk is operator-facing and a seam defect silently corrupts what
an operator believes the archive contains.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

pytest.importorskip(
    "fastapi"
)  # Why: this module attacks the admin's builders; the extras legs without fastapi must skip, not error at collection.

from taskq import (
    migrate as migrate_mod,  # Why: importorskip must precede the optional-import chain.
)
from taskq._ids import new_base62
from taskq.constants import _IDENT_RE  # pyright: ignore[reportPrivateUsage]
from taskq.web.admin._constants import (  # pyright: ignore[reportPrivateUsage]
    _PAGE_SIZE,
)
from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]  # Why: the admin module's own builders are the queries under attack; a hand-copied SQL shape would drift from the real page.
    _ARCHIVE_COLS,
    _SORTABLE_ARCHIVE,
    _build_paginated_sql,
    _build_where,
    _cursor_field,
)

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders the admin builders' own SQL; all values are $n-bound.

pytestmark = pytest.mark.integration

_TERMINAL = sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"})

# The exact cursor-width constants the page walk runs with: the boundary
# attacks size their tie groups from these, so a future change to either
# constant re-lands the boundary attacks at the NEW boundary instead of
# silently testing nothing.
FETCH = _PAGE_SIZE + 1


# ── module fixture ───────────────────────────────────────────────────────


@pytest.fixture(scope="module")
async def seam_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied (01.00.21_01's index in place)."""
    schema = f"seam_attack_{new_base62()}".lower()
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
async def seam_conn(pg_dsn: str, seam_schema: str) -> Any:
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
            f"VALUES (md5('seam' || $4::text)::uuid, 'seam_actor', 'seam_q', "
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


async def _walk_forward(conn: asyncpg.Connection, schema: str) -> tuple[list[UUID], list[int]]:
    """The operator's forward walk, the admin's exact page semantics:
    fetch, show the first _PAGE_SIZE rows, cursor from the last SHOWN row.
    Returns (ordered ids seen, per-page shown counts)."""
    seen: list[UUID] = []
    page_sizes: list[int] = []
    cursor: tuple[str, str] | None = None
    while True:
        rows = await _fetch_page(conn, schema, cursor)
        if not rows:
            break
        assert len(rows) <= FETCH, f"fetch returned {len(rows)} rows (bound {FETCH})"
        shown = rows[:_PAGE_SIZE]
        page_sizes.append(len(shown))
        seen.extend(r["id"] for r in shown)
        last = shown[-1]
        cursor = (_cursor_field(last["finished_at"]), str(last["id"]))
    return seen, page_sizes


async def _walk_backward_from(
    conn: asyncpg.Connection, schema: str, start: tuple[str, str]
) -> list[UUID]:
    """The operator's backward walk from *start*: prev pages until the
    walk runs dry, each page shown in forward order. Returns the ids in
    FORWARD (display) order — for a seam-correct shape this is exactly
    the reference order's prefix strictly before the start cursor."""
    seen: list[UUID] = []
    cursor = start
    while True:
        rows = await _fetch_page(conn, schema, cursor, "prev")
        if not rows:
            break
        shown = rows[:_PAGE_SIZE]
        seen[:0] = [r["id"] for r in shown]  # pages arrive latest-first
        first = shown[0]
        cursor = (_cursor_field(first["finished_at"]), str(first["id"]))
    return seen


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


async def test_identical_finished_at_tie_spans_page_boundary(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """One finished_at value shared by 3 pages' worth of rows: the tie
    group spans pages 1→2→3, every seam INSIDE it. The branch-one keyset
    seek must resume the tie exactly at (F, cursor_id) — no tie row
    dropped or served twice at any seam."""
    base = datetime.now(UTC) - timedelta(days=10)
    tie = [base] * (3 * _PAGE_SIZE + 7)  # deliberately NOT a page multiple
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, tie)
    walked, page_sizes = await _walk_forward(seam_conn, seam_schema)
    reference = await _reference_ids(seam_conn, seam_schema)
    assert len(page_sizes) >= 3, f"the tie group must span 3+ pages: {page_sizes}"
    _assert_row_exact(walked, reference, "uniform-tie walk")


async def test_tie_widths_landing_exactly_on_fetch_and_page_boundaries(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """Tie groups sized to the fetch/page constants: a tie of exactly
    _PAGE_SIZE rows (the whole shown page), exactly _FETCH_SIZE (the whole
    fetch), _FETCH_SIZE + 1 (one tie row spills), and _FETCH_SIZE - 1 —
    each followed by an older page of distinct values, so the page turn
    after a boundary-filling tie must MOVE to finished_at < F and the
    LIMIT-in-tie shapes cannot strand the spill row."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows: list[datetime | None] = []
    for width in (_PAGE_SIZE, FETCH, FETCH + 1, FETCH - 1):
        rows += [base] * width
        rows += [base - timedelta(days=width)]
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    walked, _page_sizes = await _walk_forward(seam_conn, seam_schema)
    reference = await _reference_ids(seam_conn, seam_schema)
    _assert_row_exact(walked, reference, "boundary-tie walk")


async def test_null_rows_enter_a_page_mid_span(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """The values range runs dry INSIDE a fetched page: 1.5 pages of
    valued rows then NULL rows, so a cursor page's answer is branch-one's
    last value rows CONCATENATED with branch-two's NULL rows. The two
    branches must reconstruct the exact NULLS LAST order at the handoff —
    the handoff page is where a branch shape drops or duplicates rows."""
    base = datetime.now(UTC) - timedelta(days=10)
    valued = _PAGE_SIZE + 20
    rows = [base - timedelta(days=i % 5, seconds=i) for i in range(valued)]
    rows += [None] * (2 * _PAGE_SIZE)  # the NULL tail, deep enough to fill pages alone
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    reference = await _reference_ids(seam_conn, seam_schema)

    # The handoff page itself: the cursor on the valued row at index
    # _PAGE_SIZE - 1 leaves 20 valued rows still unpaged — a fetch of 51
    # must take those 20 from branch one and fill the rest (31 rows) from
    # branch two's NULL range, so the answer opens valued and closes NULL.
    handoff_cursor_row = reference[_PAGE_SIZE - 1]
    row = await seam_conn.fetchrow(
        f'SELECT finished_at FROM "{seam_schema}".jobs_archive WHERE id = $1', handoff_cursor_row
    )
    assert row is not None
    cursor = (_cursor_field(row["finished_at"]), str(handoff_cursor_row))
    page = await _fetch_page(seam_conn, seam_schema, cursor)
    fins = [r["finished_at"] for r in page]
    assert fins[0] is not None and fins[-1] is None, (
        "the handoff page must open on valued rows (branch one) and close on "
        f"NULL rows (branch two): {[str(f) for f in fins[:3]]}…{fins[-3:]}"
    )
    valued_head = [f for f in fins if f is not None]
    assert valued_head == sorted(valued_head, reverse=True), (
        "the valued head of the handoff page must stay in descending order"
    )

    walked, _ = await _walk_forward(seam_conn, seam_schema)
    _assert_row_exact(walked, reference, "values→NULL handoff walk")


async def test_seam_cursor_on_the_last_value_row(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """The cursor sits EXACTLY on the population's last value row: branch
    one is EMPTY for the next page, branch two (the NULL range) must serve
    the whole page alone — the empty-values-branch seam."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i) for i in range(_PAGE_SIZE)] + [None] * _PAGE_SIZE
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    # Cursor directly on the last valued row (the oldest value, smallest id side).
    last_value = await seam_conn.fetchrow(
        f'SELECT finished_at, id FROM "{seam_schema}".jobs_archive '
        "WHERE finished_at IS NOT NULL ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1 OFFSET $1",
        _PAGE_SIZE - 1,
    )
    assert last_value is not None
    cursor = (last_value["finished_at"].isoformat(), str(last_value["id"]))
    page = await _fetch_page(seam_conn, seam_schema, cursor)
    shown = [r["id"] for r in page[:_PAGE_SIZE]]
    expected = [
        r["id"]
        for r in await seam_conn.fetch(
            f'SELECT id FROM "{seam_schema}".jobs_archive WHERE status::text = ANY($1) '
            "ORDER BY finished_at DESC NULLS LAST, id DESC OFFSET $2 LIMIT $3",
            _TERMINAL,
            _PAGE_SIZE,
            _PAGE_SIZE,
        )
    ]
    assert shown == expected, (
        "the page after the last value row is not exactly the NULL tail — "
        f"branch two dropped or reordered rows: {shown} vs {expected}"
    )
    walked, _ = await _walk_forward(seam_conn, seam_schema)
    _assert_row_exact(walked, await _reference_ids(seam_conn, seam_schema), "last-value-seam walk")


async def test_all_null_population_lives_in_the_null_seam_shape(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """An ALL-NULL archive: branch one is empty at every page — the walk
    must page the NULL range entirely through the single-statement NULL
    seam (cursor_at rendered empty) and still be row-exact."""
    rows: list[datetime | None] = [None] * (2 * _PAGE_SIZE + 13)
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    walked, page_sizes = await _walk_forward(seam_conn, seam_schema)
    reference = await _reference_ids(seam_conn, seam_schema)
    assert len(page_sizes) >= 3, f"the all-NULL walk must span pages: {page_sizes}"
    _assert_row_exact(walked, reference, "all-NULL walk")

    # And the union shape must NOT be in play for a NULL seam: the page
    # statement past a NULL cursor is the single-statement seam, not the
    # two-branch UNION (branch one would be a dead scan).
    sql, _ = _page_sql(seam_schema, ("", str(reference[0])))
    assert "UNION ALL" not in sql.upper(), (
        "a NULL-seam cursor page rendered the two-branch UNION — branch one "
        "is a dead repeat of the NULL range there"
    )


async def test_no_null_population_keeps_branch_two_empty(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """A fully-valued archive: branch two is empty at every seam — the
    UNION's NULL branch must contribute nothing, drop nothing, and the
    walk stays row-exact."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i % 7, seconds=i) for i in range(3 * _PAGE_SIZE + 1)]
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    walked, _ = await _walk_forward(seam_conn, seam_schema)
    _assert_row_exact(walked, await _reference_ids(seam_conn, seam_schema), "no-NULL walk")


async def test_tie_seam_crossing_between_value_and_null_ranges(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """A tie group of IDENTICAL finished_at whose LAST member is followed
    immediately by the NULL range: the seam page holds the tie's tail AND
    NULL rows together, then the next page lives entirely in the NULL
    range with a NULL cursor_at. The two consecutive seams — value→value
    tie, then value→NULL — are crossed back to back."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=1)] * (FETCH + 3)  # one tie, wider than a fetch
    rows += [base - timedelta(days=2)] * 5  # an older, distinct value group
    rows += [None] * _PAGE_SIZE  # the NULL tail directly behind
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    walked, page_sizes = await _walk_forward(seam_conn, seam_schema)
    reference = await _reference_ids(seam_conn, seam_schema)
    assert len(page_sizes) >= 3, f"the double-seam walk must span pages: {page_sizes}"
    _assert_row_exact(walked, reference, "tie→NULL double-seam walk")


async def test_backward_walk_row_exact_within_one_page(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """The prev-direction walk is row-exact when no page truncates: from
    a VALUE cursor with fewer than a fetch of rows before it, and from a
    NULL-seam cursor (cursor_at empty — the backward predicate's
    ``OR finished_at IS NOT NULL`` arm must reach the valued rows)."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i % 9, seconds=i) for i in range(2 * _PAGE_SIZE)]
    rows += [None] * _PAGE_SIZE
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    reference = await _reference_ids(seam_conn, seam_schema)

    # Backward from a VALUE cursor 30 rows deep: one prev page, under the
    # truncation boundary, must be exactly reference[:30].
    deep = reference[30]
    value_cursor = await seam_conn.fetchrow(
        f'SELECT finished_at, id FROM "{seam_schema}".jobs_archive WHERE id = $1', deep
    )
    assert value_cursor is not None
    start = (_cursor_field(value_cursor["finished_at"]), str(value_cursor["id"]))
    walked = await _walk_backward_from(seam_conn, seam_schema, start)
    _assert_row_exact(walked, reference[:30], "backward-from-value-cursor walk")

    # Backward from a NULL-seam cursor (the walk's first NULL row): the
    # prev pages must rebuild the whole valued prefix — the OR arm's job.
    # Small population: 40 valued + 10 NULL, so the 40 rows before the
    # seam cursor fit one fetch — this isolates the backward NULL-seam
    # PREDICATE from the display-truncation defect pinned below.
    small = [base - timedelta(days=i % 9, seconds=i) for i in range(40)]
    small += [None] * 10
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, small)
    small_reference = await _reference_ids(seam_conn, seam_schema)
    first_null = await seam_conn.fetchrow(
        f'SELECT id FROM "{seam_schema}".jobs_archive WHERE finished_at IS NULL '
        "ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1"
    )
    assert first_null is not None
    start = ("", str(first_null["id"]))
    walked = await _walk_backward_from(seam_conn, seam_schema, start)
    _assert_row_exact(walked, small_reference[:40], "backward-from-NULL-cursor walk")


@pytest.mark.xfail(
    reason="PRE-EXISTING (not this branch's regression): the admin's prev-page "
    "display truncation is direction-blind. _build_paginated_sql's backward "
    "fetch returns the _FETCH_SIZE rows nearest the cursor, re-sorted into "
    "forward display order - so rows[:_PAGE_SIZE] truncates the FARTHEST-"
    "from-cursor end, dropping the row NEAREST the cursor from display. "
    "Every full prev page turn (51 rows available) silently drops exactly "
    "one row: backward from reference[95], the fetch is reference[44:95], "
    "display shows reference[44:94], and reference[94] is never served. The "
    "forward walk truncates the correct end (the farthest row is the "
    "overfetch marker there); prev needs rows[-_PAGE_SIZE:]. The shipped "
    "prev tests (test_web_admin_pagination_order.py) seed under one page, "
    "so the truncation never bites them. Fixing this is a src/jobs.py "
    "change with its own red proof - this pin holds the seat.",
    strict=True,
)
async def test_backward_walk_row_exact_across_multiple_pages(
    seam_conn: asyncpg.Connection, seam_schema: str
) -> None:
    """The multi-page backward walk is row-exact: backward from a value
    cursor 95 rows deep must rebuild reference[:95] exactly - and from a
    NULL-seam cursor, the full valued prefix reference[:100]."""
    base = datetime.now(UTC) - timedelta(days=10)
    rows = [base - timedelta(days=i % 9, seconds=i) for i in range(2 * _PAGE_SIZE)]
    rows += [None] * _PAGE_SIZE
    await seam_conn.execute(f'TRUNCATE TABLE "{seam_schema}".jobs_archive CASCADE')
    await _seed(seam_conn, seam_schema, rows)
    reference = await _reference_ids(seam_conn, seam_schema)

    deep = reference[2 * _PAGE_SIZE - 5]  # a valued row (the walk's head is values)
    value_cursor = await seam_conn.fetchrow(
        f'SELECT finished_at, id FROM "{seam_schema}".jobs_archive WHERE id = $1', deep
    )
    assert value_cursor is not None
    start = (_cursor_field(value_cursor["finished_at"]), str(value_cursor["id"]))
    walked = await _walk_backward_from(seam_conn, seam_schema, start)
    expected = reference[: 2 * _PAGE_SIZE - 5]
    _assert_row_exact(walked, expected, "backward-from-value-cursor walk")

    first_null = await seam_conn.fetchrow(
        f'SELECT id FROM "{seam_schema}".jobs_archive WHERE finished_at IS NULL '
        "ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1"
    )
    assert first_null is not None
    start = ("", str(first_null["id"]))
    walked = await _walk_backward_from(seam_conn, seam_schema, start)
    expected = reference[: 2 * _PAGE_SIZE]
    _assert_row_exact(walked, expected, "backward-from-NULL-cursor walk")
