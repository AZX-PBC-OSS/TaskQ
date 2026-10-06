"""The history and queue-detail walks' PREV turns are row-exact (U3).

The admin fix wave gave the history page (the archive+live union over the
(coalesced finished, created_at, id) DESC order) and the queue-detail page
(the (scheduled_at, id) ASC walk) the direction-aware keyset pair the jobs
table already walks with: a Previous turn fetches the rows NEAREST the
cursor from the other side of the seam, re-sorts them into display order,
and truncates from the cursor-near end. The failure mode this pins is the
same one #564's red-team found on the jobs page: a backward walk that
drops or duplicates the seam row, or a prev turn that silently serves the
forward page again.

Round-trip walks over a multi-page seeded population, compared ROW-EXACT
against the reference order the walk claims to page: no drop, no
duplicate, no gap, in either direction.
"""

from __future__ import annotations

import uuid as uuid_mod
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

pytest.importorskip(
    "fastapi"
)  # Why: this module attacks the admin's walk builders; the extras legs without fastapi must skip, not error at collection.
from taskq import (
    migrate as migrate_mod,
)  # Why: importorskip must precede the optional-import chain.
from taskq._ids import new_base62
from taskq.constants import _IDENT_RE  # pyright: ignore[reportPrivateUsage]
from taskq.web.admin._constants import _PAGE_SIZE  # pyright: ignore[reportPrivateUsage]
from taskq.web.admin.history import _history_list_sql
from taskq.web.admin.queues import _QUEUE_DETAIL_SQL_CURSOR, _QUEUE_DETAIL_SQL_CURSOR_BACKWARD

pytestmark = pytest.mark.integration

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (validated by _IDENT_RE); all values are $n-bound.

_POP = _PAGE_SIZE * 3 + 7  # a full 3-page walk plus a ragged tail
_FETCH = _PAGE_SIZE + 1  # the walk's overfetch width


@pytest.fixture(scope="module")
async def walk_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied."""
    schema = f"walk_rt_{new_base62()}".lower()
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


async def test_history_round_trip_forward_then_backward_is_row_exact(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """Walk the whole population forward page by page, then prev back from
    the last full page's cursor to the top: the backward concatenation is
    the reference order's covered prefix, row-exact, no duplicated seam
    row anywhere."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    rows: list[dict[str, Any]] = []
    for i in range(_POP):
        job_id = uuid_mod.uuid5(uuid_mod.NAMESPACE_URL, f"walk-{i}")
        finished = base + timedelta(minutes=i)
        created = finished - timedelta(seconds=30)
        started = created + timedelta(seconds=1)
        rows.append(
            {
                "id": job_id,
                "finished_at": finished,
                "created_at": created,
                "started_at": started,
            }
        )
        # Half in the archive, half live (the union's both-branch shape).
        # duration_ms is COMPUTED in the walk's SELECT, never stored; the
        # archive carries expire_at, the live table does not.
        target = "jobs_archive" if i < _POP // 2 else "jobs"
        if target == "jobs_archive":
            await walk_conn.execute(
                f'INSERT INTO "{walk_schema}".jobs_archive '
                "(id, actor, queue, status, finished_at, created_at, started_at, "
                "attempt, max_attempts, retry_kind, payload, expire_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, 1, 3, 'transient', '{}'::jsonb, $8)",
                job_id,
                "walk_actor",
                "default",
                "succeeded",
                finished,
                created,
                started,
                finished,
            )
        else:
            await walk_conn.execute(
                f'INSERT INTO "{walk_schema}".jobs '
                "(id, actor, queue, status, finished_at, created_at, started_at, "
                "attempt, max_attempts, retry_kind, payload) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, 1, 3, 'transient', '{}'::jsonb)",
                job_id,
                "walk_actor",
                "default",
                "succeeded",
                finished,
                created,
                started,
            )
    reference = [
        r["id"]
        for r in sorted(
            rows, key=lambda r: (r["finished_at"], r["created_at"], r["id"]), reverse=True
        )
    ]

    first_sql = _history_list_sql(walk_schema, cursor=False, limit=_FETCH)
    next_sql = _history_list_sql(walk_schema, cursor=True, limit=_FETCH)
    prev_sql = _history_list_sql(walk_schema, cursor=True, limit=_FETCH, backward=True)

    # ── the forward walk ──
    seen: list[uuid_mod.UUID] = []
    # Each page's FIRST-row key: exactly the prev cursor the route stamps
    # (the template's Previous link walks from the page's first row).
    prev_cursors: list[tuple[Any, ...]] = []
    cursor: tuple[Any, ...] | None = None
    while True:
        if cursor is None:
            fetched = list(await walk_conn.fetch(first_sql, ["succeeded"], None, None))
        else:
            fetched = list(await walk_conn.fetch(next_sql, ["succeeded"], None, None, *cursor))
        page = fetched[:_PAGE_SIZE]
        if not page:
            break
        seen.extend(r["id"] for r in page)
        if prev_cursors or seen[:-1]:
            first = page[0]
            top = (
                first["finished_at"]
                if first["finished_at"] is not None
                else datetime(9999, 12, 31, tzinfo=UTC)
            )
            prev_cursors.append((top, first["created_at"], first["id"]))
        if len(fetched) <= _PAGE_SIZE:
            break
        last = page[-1]
        top = (
            last["finished_at"]
            if last["finished_at"] is not None
            else datetime(9999, 12, 31, tzinfo=UTC)
        )
        cursor = (top, last["created_at"], last["id"])
    assert seen == reference, "the forward walk must reproduce the reference order"

    # ── the backward round trip: from the LAST page's prev cursor (its
    # first row) back up to the top — the operator's own walk. The rows
    # above that key are exactly the reference's covered prefix.
    covered = len(reference) - (len(reference) % _PAGE_SIZE)
    seen_backward: list[uuid_mod.UUID] = []
    cursor = prev_cursors[-1]
    while True:
        fetched = list(await walk_conn.fetch(prev_sql, ["succeeded"], None, None, *cursor))
        if not fetched:
            break
        # The backward fetch arrives re-sorted into display order; the
        # page is the LAST page-worth (the rows nearest the cursor).
        page = fetched[-_PAGE_SIZE:]
        seen_backward = [r["id"] for r in page] + seen_backward
        if len(fetched) < _PAGE_SIZE:
            break
        first = page[0]
        cursor = (first["finished_at"], first["created_at"], first["id"])
        if len(seen_backward) >= covered:
            break

    assert len(set(seen_backward)) == len(seen_backward), "the prev walk duplicated rows"
    assert seen_backward == reference[:covered], (
        "the prev walk must land back on the reference order's covered "
        f"prefix, row-exact (covered {covered}): got {len(seen_backward)} rows"
    )


async def test_queue_detail_round_trip_is_row_exact(
    walk_conn: asyncpg.Connection, walk_schema: str
) -> None:
    """The queue-detail walk (pending rows by (scheduled_at, id) ASC):
    forward then prev, row-exact, the pair's directions mirrored."""
    base = datetime(2026, 2, 1, tzinfo=UTC)
    ids: list[uuid_mod.UUID] = []
    for i in range(_POP):
        job_id = uuid_mod.uuid5(uuid_mod.NAMESPACE_URL, f"qwalk-{i}")
        ids.append(job_id)
        await walk_conn.execute(
            f'INSERT INTO "{walk_schema}".jobs '
            "(id, actor, queue, status, scheduled_at, created_at, attempt, max_attempts, retry_kind, payload) "
            "VALUES ($1, $2, $3, $4, $5, $6, 0, 3, 'transient', '{}'::jsonb)",
            job_id,
            "walk_actor",
            "walk_queue",
            "pending",
            base + timedelta(minutes=i),
            base,
        )
    reference = list(ids)  # ASC by scheduled_at = insertion order

    async def _fetch(cursor: tuple[Any, ...] | None, direction: str) -> list[asyncpg.Record]:
        if cursor is None:
            sql = (
                f'SELECT id, scheduled_at FROM "{walk_schema}".jobs '
                "WHERE queue = $1 AND status = $2 ORDER BY scheduled_at, id LIMIT $3"
            )
            return list(await walk_conn.fetch(sql, "walk_queue", "pending", _FETCH))
        sql = (
            _QUEUE_DETAIL_SQL_CURSOR if direction == "next" else _QUEUE_DETAIL_SQL_CURSOR_BACKWARD
        ).format(schema=walk_schema, limit=_FETCH)
        return list(await walk_conn.fetch(sql, "walk_queue", "pending", cursor[0], cursor[1]))

    seen: list[uuid_mod.UUID] = []
    # Each page's FIRST-row key: the prev cursor the route stamps.
    prev_cursors: list[tuple[Any, ...]] = []
    cursor: tuple[Any, ...] | None = None
    while True:
        fetched = await _fetch(cursor, "next")
        page = fetched[:_PAGE_SIZE]
        if not page:
            break
        seen.extend(r["id"] for r in page)
        if seen[:-1]:
            first = page[0]
            prev_cursors.append((first["scheduled_at"], first["id"]))
        if len(fetched) <= _PAGE_SIZE:
            break
        last = page[-1]
        cursor = (last["scheduled_at"], last["id"])
    assert seen == reference, "the forward walk must reproduce the ASC order"

    # Prev from the last page's first-row cursor back to the top: the
    # covered prefix of the reference, row-exact.
    covered = len(reference) - (len(reference) % _PAGE_SIZE)
    seen_backward: list[uuid_mod.UUID] = []
    cursor = prev_cursors[-1]
    while True:
        fetched = await _fetch(cursor, "prev")
        if not fetched:
            break
        page = fetched[-_PAGE_SIZE:]
        seen_backward = [r["id"] for r in page] + seen_backward
        if len(fetched) < _PAGE_SIZE:
            break
        first = page[0]
        cursor = (first["scheduled_at"], first["id"])
        if len(seen_backward) >= covered:
            break

    assert len(set(seen_backward)) == len(seen_backward), "the prev walk duplicated rows"
    assert seen_backward == reference[:covered], (
        "the prev walk must return to the top of the queue's order over "
        f"its covered prefix, row-exact (covered {covered})"
    )
