"""Admin keyset pagination must page in the direction the user is sorting.

The keyset predicate has to match the *effective* scan direction, which is
the sort direction combined with the paging direction -- not the paging
direction alone.  Under ``order=asc`` a "Next" click that re-applies a
``<`` filter walks backwards into rows the operator has already seen (or
off the front of the set entirely), so the list page can never be paged
past its first screen in ascending order.

Runs the real query against real Postgres and asserts on the ROWS
returned -- never on the generated SQL.
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq.actor import actor
from taskq.client import JobsClient
from taskq.web.admin._constants import (
    _ALL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: the admin package publishes its shared constants under a private prefix; the route module imports them the same way.
)
from taskq.web.admin.jobs import (
    _SORTABLE_LIVE,  # pyright: ignore[reportPrivateUsage]  # Why: same private-prefix-within-package convention as the route module itself.
    _build_paginated_sql,  # pyright: ignore[reportPrivateUsage]  # Why: the query builder under test; exercised against real PG so the assertions are on rows, not SQL.
)

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object

pytestmark = pytest.mark.integration

_ROWS = 10


class _Payload(BaseModel):
    value: int = 1


@actor(name="_admin_pagination_actor")
async def _admin_pagination_actor(payload: _Payload) -> None:
    pass


async def _seed(deps: WorkerDeps, backend: PostgresBackend) -> list[tuple[UUID, datetime]]:
    """Create ``_ROWS`` jobs with strictly increasing ``created_at``.

    Returns them oldest-first, which is exactly ``order=asc`` order.
    """
    client = JobsClient(backend)
    schema = deps.settings.schema_name
    ids: list[UUID] = []
    for _ in range(_ROWS):
        handle = await client.enqueue(_admin_pagination_actor, _Payload())
        ids.append(handle.job_id)

    base = datetime(2026, 1, 1, tzinfo=UTC)
    async with deps.worker_pool.acquire() as conn:
        for offset, job_id in enumerate(ids):
            await conn.execute(
                f'UPDATE "{schema}".jobs SET created_at = $2 WHERE id = $1',  # noqa: S608  # Why: schema is fixture-derived and validated; values are $1/$2-bound.
                job_id,
                base + timedelta(minutes=offset),
            )
        rows = await conn.fetch(
            f'SELECT id, created_at FROM "{schema}".jobs ORDER BY created_at ASC',  # noqa: S608  # Why: as above.
        )
    return [(r["id"], r["created_at"]) for r in rows]


async def _page(
    deps: WorkerDeps,
    *,
    cursor: tuple[UUID, datetime] | None,
    cursor_dir: str,
    order: str,
    sort: str = "created_at",
    raw_cursor: tuple[str, str] | None = None,
) -> list[tuple[UUID, datetime]]:
    schema = deps.settings.schema_name
    where = "status = ANY($1)"
    params: list[object] = [sorted(_ALL_STATUSES)]
    if raw_cursor is not None:
        cursor_at, cursor_id = raw_cursor
    else:
        cursor_at = cursor[1].isoformat() if cursor else None
        cursor_id = str(cursor[0]) if cursor else None
    sql, query_params = _build_paginated_sql(
        schema,
        "jobs",
        "id, created_at",
        _SORTABLE_LIVE,
        where,
        params,
        cursor_at,
        cursor_id,
        cursor_dir,
        sort,
        order,
    )
    async with deps.worker_pool.acquire() as conn:
        rows = await conn.fetch(sql, *query_params)
    return [(r["id"], r["created_at"]) for r in rows]


async def test_next_page_ascending_advances_forward(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """order=asc + Next must return rows AFTER the cursor, not before it."""
    deps, backend = clean_jobs_app
    seeded = await _seed(deps, backend)

    first_page = await _page(deps, cursor=None, cursor_dir="next", order="asc")
    assert first_page == seeded, "ascending first page must be oldest-first"

    cursor = seeded[3]
    following = await _page(deps, cursor=cursor, cursor_dir="next", order="asc")

    assert following == seeded[4:], (
        f"ascending Next from row 3 returned {[str(i) for i, _ in following]}, "
        f"expected the six rows after it"
    )


async def test_prev_page_ascending_walks_back(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """order=asc + Prev must return rows BEFORE the cursor, still in asc order."""
    deps, backend = clean_jobs_app
    seeded = await _seed(deps, backend)

    cursor = seeded[6]
    preceding = await _page(deps, cursor=cursor, cursor_dir="prev", order="asc")

    assert preceding == seeded[:6], (
        f"ascending Prev from row 6 returned {[str(i) for i, _ in preceding]}, "
        f"expected the six rows before it"
    )


async def test_descending_paging_is_unchanged(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """The default descending direction keeps working in both directions."""
    deps, backend = clean_jobs_app
    seeded = await _seed(deps, backend)
    newest_first = list(reversed(seeded))

    assert await _page(deps, cursor=None, cursor_dir="next", order="desc") == newest_first

    cursor = newest_first[3]
    assert await _page(deps, cursor=cursor, cursor_dir="next", order="desc") == newest_first[4:]
    assert await _page(deps, cursor=cursor, cursor_dir="prev", order="desc") == newest_first[:3]


async def test_next_page_ascending_on_a_text_column(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Every sortable column shares the predicate, not just the timestamp one.

    ``status`` is a text-cursor column; all seeded jobs share one status, so
    the tiebreaker ``id`` carries the whole ordering -- which is exactly the
    case where a direction-blind operator silently returns the wrong page.
    """
    deps, backend = clean_jobs_app
    await _seed(deps, backend)

    by_id = await _page(deps, cursor=None, cursor_dir="next", order="asc", sort="status")
    assert [str(i) for i, _ in by_id] == sorted(str(i) for i, _ in by_id)

    following = await _page(
        deps,
        cursor=None,
        cursor_dir="next",
        order="asc",
        sort="status",
        raw_cursor=("pending", str(by_id[3][0])),
    )
    assert following == by_id[4:], (
        f"ascending Next on a text sort returned {[str(i) for i, _ in following]}, "
        f"expected the rows after the cursor"
    )


async def test_timestamp_cursor_round_trips_from_the_query_string(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """The cursor arrives as text; a timestamp page must still execute.

    The route hands ``cursor_at`` straight through from the query string,
    where every value is a string. asyncpg types each placeholder from its
    ``::`` cast and rejects a ``str`` bound to ``timestamptz``, so the
    default created_at sort could not turn a single page.
    """
    deps, backend = clean_jobs_app
    seeded = await _seed(deps, backend)
    newest_first = list(reversed(seeded))

    following = await _page(
        deps,
        cursor=None,
        cursor_dir="next",
        order="desc",
        raw_cursor=(newest_first[2][1].isoformat(), str(newest_first[2][0])),
    )
    assert following == newest_first[3:]


async def test_unparseable_cursor_falls_back_to_the_first_page(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """A hand-edited or stale cursor must not surface a driver error."""
    deps, backend = clean_jobs_app
    seeded = await _seed(deps, backend)

    rows = await _page(
        deps,
        cursor=None,
        cursor_dir="next",
        order="asc",
        raw_cursor=("not-a-timestamp", str(seeded[0][0])),
    )
    assert rows == seeded
