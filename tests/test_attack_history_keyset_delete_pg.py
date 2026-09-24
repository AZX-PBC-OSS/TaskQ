# ruff: noqa: S608  # Why: schema names are generated/validated identifiers, every value is $-bound.
"""ATTACK pins (real PG): the admin /history keyset walk is DELETE-tolerant.

The construct: an admin walks the history pages; between page 1 and page 2
the retention pruner deletes rows - the archive expiry sweep removes archive
rows (the open cursor's own row included), the terminal prune removes a live
row and moves its surviving copy into the archive (the conservation move).
A keyset seam compares TUPLES (``finished-or-ceiling, created_at, id`` <
cursor), never row existence, so the prediction is:

* no error on any later page (the cursor's own row may be gone),
* no surviving row skipped (the seam's strict tuple compare orders every
  survivor the same way with or without the deleted ones),
* no row replayed (the prune's moved copy carries the SAME sort tuple, so
  the seam serves it exactly once),
* page count may shrink, that is the honest answer.

Pinned by driving the exact SQL the route runs
(``taskq.web.admin.history._history_list_sql``) against a real Postgres and
deleting through the real deleter statements (the rendered
``_EXPIRY_CTE_SQL`` archive-expiry arm; the prune's committed effect on the
live table).
"""

from __future__ import annotations

import uuid as uuid_mod
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq._ids import new_uuid
from taskq.web.admin.history import _history_list_sql
from taskq.worker._leader_shared import (
    _EXPIRY_CTE_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the attack binds the exact statement the expiry sweep renders, so drift fails here first.
)

pytestmark = pytest.mark.integration

_PAGE = 5

_NOW = datetime.now(UTC)


async def _seed_archive_row(
    conn: asyncpg.Connection,
    schema: str,
    *,
    finished: datetime,
    created: datetime,
    expire: datetime,
) -> tuple[UUID, datetime, datetime]:
    jid = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, finished_at, created_at, archived_at, expire_at
        ) VALUES ($1, 'gap_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, clock_timestamp(), $4)""",
        jid,
        finished,
        created,
        expire,
    )
    return (jid, finished, created)


async def _seed_live_row(
    conn: asyncpg.Connection,
    schema: str,
    *,
    finished: datetime,
    created: datetime,
) -> tuple[UUID, datetime, datetime]:
    jid = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, finished_at, created_at
        ) VALUES ($1, 'gap_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3)""",
        jid,
        finished,
        created,
    )
    return (jid, finished, created)


def _offset(days: int, minutes: int) -> datetime:
    return _NOW - timedelta(days=days, minutes=minutes)


async def _seed_corpus(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    """Ten rows in strict walk order (finished DESC). The walk-order
    positions below are the pin's oracle; ``expire`` decides which the
    archive expiry sweep deletes mid-walk."""
    past_expire = _NOW - timedelta(hours=1)
    future_expire = _NOW + timedelta(days=30)
    rows: dict[str, Any] = {}
    # position 1
    rows["af6"] = await _seed_archive_row(
        conn, schema, finished=_offset(5, 0), created=_offset(5, -10), expire=future_expire
    )
    # position 2
    rows["lf4"] = await _seed_live_row(
        conn, schema, finished=_offset(6, 0), created=_offset(6, -10)
    )
    # position 3
    rows["af5"] = await _seed_archive_row(
        conn, schema, finished=_offset(7, 0), created=_offset(7, -10), expire=future_expire
    )
    # position 4
    rows["lf3"] = await _seed_live_row(
        conn, schema, finished=_offset(8, 0), created=_offset(8, -10)
    )
    # position 5 - the cursor row, expired (deleted) mid-walk
    rows["af4"] = await _seed_archive_row(
        conn, schema, finished=_offset(9, 0), created=_offset(9, -10), expire=past_expire
    )
    # position 6 - the row page 2 would have served first, prune-moved
    # (deleted from jobs, archived with the SAME sort tuple) mid-walk
    rows["lf2"] = await _seed_live_row(
        conn, schema, finished=_offset(10, 0), created=_offset(10, -10)
    )
    # position 7 - expired archive row, deleted mid-walk
    rows["af3"] = await _seed_archive_row(
        conn, schema, finished=_offset(11, 0), created=_offset(11, -10), expire=past_expire
    )
    # position 8
    rows["lf1"] = await _seed_live_row(
        conn, schema, finished=_offset(12, 0), created=_offset(12, -10)
    )
    # position 9 - expired archive row, deleted mid-walk
    rows["af2"] = await _seed_archive_row(
        conn, schema, finished=_offset(13, 0), created=_offset(13, -10), expire=past_expire
    )
    # position 10 - expired archive row, deleted mid-walk
    rows["af1"] = await _seed_archive_row(
        conn, schema, finished=_offset(14, 0), created=_offset(14, -10), expire=past_expire
    )
    return rows


def _tuple(row: Any) -> tuple[datetime, datetime, uuid_mod.UUID]:
    finished = row["finished_at"] if row["finished_at"] is not None else _CURSOR_CEILING
    return (finished, row["created_at"], row["id"])


#: The walk's ceiling, the same value _history_list_sql's COALESCE uses.
_CURSOR_CEILING = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)


class TestHistoryKeysetDeleteTolerance:
    async def test_delete_mid_walk_never_skips_replays_or_errors(
        self,
        module_pg_schema: Any,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        schema = module_pg_schema.schema_name
        rows = await _seed_corpus(clean_pg_conn, schema)

        first_sql = _history_list_sql(schema, cursor=False, limit=_PAGE)
        cursor_sql = _history_list_sql(schema, cursor=True, limit=_PAGE)
        statuses = ["succeeded"]

        # Page 1: the top of the walk, before any delete.
        page1 = await clean_pg_conn.fetch(first_sql, statuses, None, None)
        assert len(page1) == _PAGE
        cursor = _tuple(page1[-1])
        # The seam's own prediction about which row the cursor is: af4.
        assert cursor[2] == rows["af4"][0]

        # ── the mid-walk deletions ──
        # (a) The archive expiry sweep, the real rendered statement: it
        # deletes af1..af4 (expire_at past), the open cursor's own row
        # among them. (The statement's tag reports the final GROUP BY's
        # row count, not the delete count, so the pin counts the table.)
        await clean_pg_conn.execute(_EXPIRY_CTE_SQL.format(schema=schema), 100)
        assert (
            await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = ANY($1::uuid[])',
                [rows[f"af{n}"][0] for n in (1, 2, 3, 4)],
            )
            == 0
        )
        assert (
            await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', rows["af4"][0]
            )
            == 0
        ), "the cursor row itself must be gone: that is the construct"

        # (b) The prune's committed effect on a live row: deleted from
        # jobs, conserved into jobs_archive with the SAME sort tuple.
        await clean_pg_conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', rows["lf2"][0])
        await clean_pg_conn.execute(
            f"""INSERT INTO "{schema}".jobs_archive (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, finished_at, created_at, archived_at, expire_at
            ) VALUES ($1, 'gap_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
                'succeeded', $2, $3, clock_timestamp(),
                clock_timestamp() + interval '30 days')""",
            rows["lf2"][0],
            rows["lf2"][1],
            rows["lf2"][2],
        )

        # Page 2, resumed from a cursor whose row no longer exists: the
        # seam is a tuple compare, no existence probe, no error.
        page2 = await clean_pg_conn.fetch(
            cursor_sql, statuses, None, None, cursor[0], cursor[1], cursor[2]
        )
        got2 = [r["id"] for r in page2]
        assert got2 == [rows["lf2"][0], rows["lf1"][0]], (
            "page 2 must serve exactly the survivors strictly after the "
            f"cursor tuple, in walk order; got {got2}"
        )

        # A third turn from the last row: empty, no error (the walk ends).
        last = _tuple(page2[-1])
        page3 = await clean_pg_conn.fetch(
            cursor_sql, statuses, None, None, last[0], last[1], last[2]
        )
        assert page3 == []

        # The union-walk equivalence: page 1 survivors + later pages equals
        # a post-delete full walk (an UNLIMITED render, the page limit is
        # the walk's own), no dupes, no gaps.
        full_sql = _history_list_sql(schema, cursor=False, limit=100)
        full = await clean_pg_conn.fetch(full_sql, statuses, None, None)
        assert [r["id"] for r in full] == (
            [r["id"] for r in page1 if r["id"] != rows["af4"][0]] + got2
        )

        # The NULL-finished ceiling branch shares the seam: a row with no
        # finished_at sorts above everything and its sentinel cursor walks
        # the same way. Seed one and resume from its tuple.
        await clean_pg_conn.execute(
            f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, finished_at, created_at
            ) VALUES ($1, 'gap_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
                'running', NULL, $2)""",
            new_uuid(),
            _NOW,
        )
        with_null = await clean_pg_conn.fetch(first_sql, ["succeeded", "running"], None, None)
        assert with_null[0]["finished_at"] is None
        top = _tuple(with_null[0])
        assert top[0] == _CURSOR_CEILING
        after = await clean_pg_conn.fetch(
            _history_list_sql(schema, cursor=True, limit=100),
            ["succeeded", "running"],
            None,
            None,
            top[0],
            top[1],
            top[2],
        )
        full_both = await clean_pg_conn.fetch(full_sql, ["succeeded", "running"], None, None)
        assert [r["id"] for r in after] == [r["id"] for r in full_both[1:]], (
            "the sentinel ceiling cursor must describe the same seam the "
            "ordering uses, deleted rows or not"
        )
