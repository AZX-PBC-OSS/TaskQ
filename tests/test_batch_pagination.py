"""``list_batches`` must be pageable without re-scanning from the top.

Before this suite the only knob was ``BatchFilter.limit`` with a bare
``ORDER BY b.created_at DESC LIMIT $n`` -- no cursor, no offset -- so
reaching batch 501 meant asking for 501 rows and throwing 500 away. A
``limit`` cap was added and reverted for exactly that reason: with no way
to page, the cap made every batch past it unreachable.

The assertions here are coverage assertions, not shape assertions: page
through with a limit smaller than the row count and require that every
batch is seen **exactly once**. That is what catches an off-by-one at the
page seam (a duplicate) and a tiebreaker that is not part of the sort key
(a dropped row). ``created_at`` alone is not unique -- PG's ``now()`` is
the transaction timestamp and the in-memory clock is a ``FakeClock`` --
so both tiers deliberately include a set of batches sharing one
``created_at``, where ``id`` carries the whole ordering.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._cursor import decode_batch_cursor, encode_batch_cursor
from taskq.backend._protocol import BatchFilter
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)

_PAGE = 3
_TOTAL = 7


# ── cursor codec ────────────────────────────────────────────────────────


def test_batch_cursor_round_trips_to_the_columns_real_types() -> None:
    """The decoded cursor must be a ``datetime`` and a ``UUID``, not text.

    asyncpg types each placeholder from its ``::`` cast and refuses a
    ``str`` for ``timestamptz``/``uuid`` -- the exact failure that 500'd
    every admin job-list page turn before 2569da5.
    """
    created_at = datetime(2025, 6, 1, 12, 30, 45, 123456, tzinfo=UTC)
    batch_id = new_uuid()

    decoded_at, decoded_id = decode_batch_cursor(encode_batch_cursor(created_at, batch_id))

    assert isinstance(decoded_at, datetime)
    assert isinstance(decoded_id, UUID)
    assert (decoded_at, decoded_id) == (created_at, batch_id)


def test_malformed_batch_cursor_is_rejected() -> None:
    with pytest.raises(ValueError, match="cursor"):
        decode_batch_cursor("not-a-cursor")


# ── in-memory tier ──────────────────────────────────────────────────────


async def _page_all_in_memory(backend: InMemoryBackend, limit: int) -> list[UUID]:
    seen: list[UUID] = []
    cursor: str | None = None
    for _ in range(_TOTAL + 2):  # bounded: a non-advancing cursor must not hang
        page = await backend.list_batches(BatchFilter(limit=limit, cursor=cursor))
        if not page:
            break
        seen.extend(row.id for row, _counts in page)
        last, _counts = page[-1]
        cursor = encode_batch_cursor(last.created_at, last.id)
    return seen


async def test_in_memory_paging_sees_every_batch_exactly_once() -> None:
    backend = InMemoryBackend(clock=FakeClock(_START))
    expected = set[UUID]()
    for _ in range(_TOTAL):
        bid = new_uuid()
        await backend.create_batch(bid, "default", 1, None, None, None)
        expected.add(bid)

    seen = await _page_all_in_memory(backend, _PAGE)

    assert len(seen) == len(set(seen)), "a batch was returned on two pages"
    assert set(seen) == expected, "a batch was never returned on any page"


async def test_in_memory_predicates_survive_paging() -> None:
    """The cursor composes with the other predicates rather than replacing
    them -- a page turn must not widen the filter."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    wanted = set[UUID]()
    for i in range(_TOTAL):
        bid = new_uuid()
        await backend.create_batch(bid, "ingest" if i % 2 else "default", 1, None, None, None)
        if i % 2:
            wanted.add(bid)

    seen: list[UUID] = []
    cursor: str | None = None
    for _ in range(_TOTAL + 2):  # bounded: a non-advancing cursor must not hang
        page = await backend.list_batches(BatchFilter(queue="ingest", limit=2, cursor=cursor))
        if not page:
            break
        seen.extend(row.id for row, _counts in page)
        last, _counts = page[-1]
        cursor = encode_batch_cursor(last.created_at, last.id)

    assert len(seen) == len(set(seen))
    assert set(seen) == wanted


# ── Postgres tier ───────────────────────────────────────────────────────


@pytest.mark.integration
class TestPostgresBatchPaging:
    async def _page_all(self, backend: object, limit: int) -> list[UUID]:
        from taskq.backend.postgres import PostgresBackend

        assert isinstance(backend, PostgresBackend)
        seen: list[UUID] = []
        cursor: str | None = None
        for _ in range(_TOTAL + 2):
            page = await backend.list_batches(BatchFilter(limit=limit, cursor=cursor))
            if not page:
                break
            seen.extend(row.id for row, _counts in page)
            last, _counts = page[-1]
            cursor = encode_batch_cursor(last.created_at, last.id)
        return seen

    async def test_paging_sees_every_batch_exactly_once(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        expected = set[UUID]()
        for _ in range(_TOTAL):
            bid = new_uuid()
            await backend.create_batch(bid, "default", 1, None, None, None)
            expected.add(bid)

        seen = await self._page_all(backend, _PAGE)

        assert len(seen) == len(set(seen)), "a batch was returned on two pages"
        assert set(seen) == expected, "a batch was never returned on any page"

    async def test_paging_is_stable_when_every_batch_shares_created_at(
        self, jobs_app: JobsApp, module_pg_schema: ModulePgSchema
    ) -> None:
        """``created_at DESC`` alone is not a total order.

        ``batches.created_at`` defaults to ``now()`` -- the *transaction*
        timestamp -- so a single ``enqueue_batch_atomic`` call stamps
        every row it writes with the same instant. Collapsing the column
        to one value here reproduces that without depending on write
        timing: the keyset must then be carried entirely by the UUIDv7
        ``id`` tiebreaker.
        """
        backend = jobs_app.backend
        schema = module_pg_schema.schema_name
        expected = set[UUID]()
        for _ in range(_TOTAL):
            bid = new_uuid()
            await backend.create_batch(bid, "default", 1, None, None, None)
            expected.add(bid)

        collapse_sql = f'UPDATE "{schema}".batches SET created_at = $1'  # noqa: S608  # Why: test helper — schema is a validated fixture identifier, the value is $1-bound.
        async with jobs_app.deps.worker_pool.acquire() as conn:
            await conn.execute(collapse_sql, datetime(2025, 3, 4, 5, 6, 7, tzinfo=UTC))

        seen = await self._page_all(backend, _PAGE)

        assert len(seen) == len(set(seen)), "a batch was returned on two pages"
        assert set(seen) == expected, "a batch was never returned on any page"

    async def test_cursor_binds_at_the_columns_type(self, jobs_app: JobsApp) -> None:
        """A page turn must not raise asyncpg ``DataError``.

        The cursor round-trips as text; binding it raw against a
        ``timestamptz``/``uuid`` placeholder is what made the admin job
        list 500 on every page turn (2569da5). Asserting on the second
        page's rows is the behavioural form of that regression.
        """
        backend = jobs_app.backend
        for _ in range(3):
            await backend.create_batch(new_uuid(), "default", 1, None, None, None)

        first = await backend.list_batches(BatchFilter(limit=2))
        assert len(first) == 2
        last_row, _counts = first[-1]

        second = await backend.list_batches(
            BatchFilter(limit=2, cursor=encode_batch_cursor(last_row.created_at, last_row.id))
        )

        assert len(second) == 1
        assert second[0][0].id not in {r.id for r, _c in first}
