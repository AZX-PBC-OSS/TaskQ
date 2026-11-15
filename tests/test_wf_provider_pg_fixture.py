"""Direct real-PostgreSQL control for the generic shared paged-provider fixture.

This is the *fixture-local* PostgreSQL control lane, labelled explicitly: it
runs no TaskQ workflow, no worker, no engine migration, no network, and no
external service. It proves the generic provider protocol over a real
PostgreSQL server plus the shared fixture's snapshot/cursor/fault semantics
only.

The fixture owns one bounded scratch schema namespace. The tests create that
namespace themselves (never a TaskQ migration), each scenario is an isolated
row set inside it, and the namespace is dropped at module teardown. The
provider bounds are the accepted local fixture's bounds: records, page size,
retained snapshots, retained cursors, and fault rules.

Role labels ``worker_a``/``worker_b`` below are plain diagnostic labels for
independently constructed provider handles and connection pools. They are not
engine workers, do not use ``map_index``, and do not prove any engine claim
from database state alone. The future sample/fleet composition remains
unbuilt.
"""

from __future__ import annotations

import asyncio
import inspect
import secrets
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from queue import Queue
from threading import Barrier, Event
from typing import cast

import asyncpg
import pytest
from examples.adoption.pg_provider import (
    PgPagedProvider,
    PgProviderScenarioState,
    _canonical_json_dumps,
    _fault_script_to_json,
)
from examples.adoption.provider import (
    MAX_RECORD_CONTENT_BYTES,
    MAX_RECORD_REVISION,
    PagedProvider,
    ProviderContractError,
    ProviderCursor,
    ProviderCursorExpired,
    ProviderFaultRule,
    ProviderFaultScript,
    ProviderMalformedRecord,
    ProviderPage,
    ProviderPageLimitError,
    ProviderPoison,
    ProviderPoisonFault,
    ProviderRateLimited,
    ProviderRateLimitedFault,
    ProviderRecord,
    ProviderRecordLimitExceeded,
    ProviderUnavailable,
    ProviderUnavailableFault,
    ProviderUnknownCursor,
)
from pydantic import ValidationError

pytestmark = pytest.mark.integration

_CURSOR_NONCE = "0123456789abcdef"
_OTHER_NONCE = "fedcba9876543210"


def _admin_statement(pg_dsn: str, statement: str) -> None:
    """Run one fixture-owner DDL statement on a private short-lived loop."""

    async def run() -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(statement)
        finally:
            await conn.close()

    asyncio.run(run())


@pytest.fixture
def pg_provider_schema(pg_dsn: str) -> Iterator[str]:
    """A bounded, fixture-owned scratch schema; never a TaskQ migration schema."""
    schema = "ap9pg_" + secrets.token_hex(6)
    _admin_statement(
        pg_dsn, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
    )  # Why: schema name is a locally generated validated identifier.
    try:
        yield schema
    finally:
        _admin_statement(
            pg_dsn, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )  # Why: schema name is the same locally generated validated identifier.


def _sync_case(
    fn: Callable[[str, str], Coroutine[object, object, object]],
) -> Callable[[str, str], object]:
    """Run one real-asyncpg scenario without the wf-family PG/G7 fixture hook.

    The suite's collection hook appends the engine workflow G7 fixture to
    every *coroutine* test in a ``test_wf_*`` module. This fixture-control
    module owns a scratch schema and no workflow rows, so each scenario is
    wrapped as a plain sync test whose body still executes on a real asyncio
    loop and uses real asyncpg pools.
    """

    @wraps(fn)
    def wrapper(pg_dsn: str, pg_provider_schema: str) -> object:
        return asyncio.run(fn(pg_dsn, pg_provider_schema))

    return wrapper


def _record(index: int, *, revision: int = 0, content: str | None = None) -> ProviderRecord:
    return ProviderRecord(
        record_id=f"rec-{index:03d}",
        revision=revision,
        content=f"content-{index:03d}" if content is None else content,
    )


def _records(*indexes: int) -> list[ProviderRecord]:
    return [_record(index) for index in indexes]


def _range_records(count: int) -> list[ProviderRecord]:
    return [_record(index) for index in range(count)]


def _scenario(label: str) -> str:
    return f"{label}-{secrets.token_hex(4)}"


def _ids(records: Sequence[ProviderRecord]) -> list[str]:
    return [record.record_id for record in records]


async def _create_pool(pg_dsn: str, *, min_size: int = 1, max_size: int = 4) -> asyncpg.Pool:
    return await asyncpg.create_pool(pg_dsn, min_size=min_size, max_size=max_size)


async def _new_provider(
    pool: asyncpg.Pool,
    schema: str,
    scenario: str,
    *,
    max_records: int = 100,
    max_page_size: int = 10,
    max_snapshots: int = 8,
    max_cursors: int = 64,
    cursor_nonce: str | None = None,
    fault_script: ProviderFaultScript | None = None,
) -> PgPagedProvider:
    provider = PgPagedProvider(
        pool,
        schema=schema,
        scenario=scenario,
        max_records=max_records,
        max_page_size=max_page_size,
        max_snapshots=max_snapshots,
        max_cursors=max_cursors,
        cursor_nonce=cursor_nonce,
        fault_script=fault_script,
    )
    await provider.initialize()
    return provider


async def _initialize_on_own_pool(
    pg_dsn: str,
    schema: str,
    scenario: str,
    *,
    max_page_size: int = 10,
    cursor_nonce: str | None = None,
) -> str:
    """One provider instance/pool per call, as an independent worker would."""
    pool = await _create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        provider = PgPagedProvider(
            pool,
            schema=schema,
            scenario=scenario,
            max_records=100,
            max_page_size=max_page_size,
            max_snapshots=8,
            max_cursors=64,
            cursor_nonce=cursor_nonce,
        )
        await provider.initialize()
        return "ok"
    except ProviderContractError:
        return "typed"
    finally:
        await pool.close()


async def _collect(provider: PgPagedProvider, page_limit: int) -> list[ProviderRecord]:
    collected: list[ProviderRecord] = []
    cursor: ProviderCursor | None = None
    for _ in range(1_000):
        page = await provider.fetch(cursor, page_limit)
        collected.extend(page.records)
        if not page.has_more:
            assert page.next_cursor is None
            return collected
        next_cursor = page.next_cursor
        assert next_cursor is not None
        cursor = next_cursor
    raise AssertionError("provider did not terminate within the page bound")


@_sync_case
async def test_pg_server_version_and_fixture_ddl_are_exact_and_idempotent(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        version = await pool.fetchval("SELECT current_setting('server_version')")
        version_num = await pool.fetchval("SELECT current_setting('server_version_num')::integer")
        assert isinstance(version, str) and version
        assert isinstance(version_num, int) and version_num >= 120_000

        scenario = _scenario("init")
        provider = PgPagedProvider(
            pool,
            schema=pg_provider_schema,
            scenario=scenario,
            cursor_nonce=_CURSOR_NONCE,
        )
        await provider.initialize()
        first = await provider.read_scenario_state()
        await provider.initialize()
        second = await provider.read_scenario_state()
        assert first == second
        assert first.scenario == scenario
        assert first.cursor_nonce == _CURSOR_NONCE
        assert first.max_records == 10_000
        assert first.max_page_size == 100
        assert first.max_snapshots == 4
        assert first.max_cursors == 256
        assert first.record_count == 0
        assert first.snapshot_count == 0
        assert first.cursor_count == 0
        assert first.stats.fetch_calls == 0
        assert first.stats.pages_returned == 0

        peer = PgPagedProvider(
            pool,
            schema=pg_provider_schema,
            scenario=scenario,
            cursor_nonce=_CURSOR_NONCE,
        )
        await peer.initialize()
        assert await peer.read_scenario_state() == first

        with pytest.raises(ProviderContractError):
            different = PgPagedProvider(
                pool,
                schema=pg_provider_schema,
                scenario=scenario,
                cursor_nonce=_OTHER_NONCE,
            )
            await different.initialize()
    finally:
        await pool.close()


@_sync_case
async def test_pg_empty_short_full_and_page_limit_bounds(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        empty = await _new_provider(pool, pg_provider_schema, _scenario("empty"), max_page_size=3)
        page = await empty.fetch(None, 3)
        assert page.records == ()
        assert page.has_more is False
        assert page.next_cursor is None
        assert page.payload_bytes == 0
        empty_state = await empty.read_scenario_state()
        assert empty_state.stats.fetch_calls == 1
        assert empty_state.stats.pages_returned == 1
        assert empty_state.stats.records_returned == 0
        assert empty_state.stats.snapshots_created == 0

        short = await _new_provider(pool, pg_provider_schema, _scenario("short"), max_page_size=5)
        await short.replace_records(_records(0, 1, 2))
        short_page = await short.fetch(None, 5)
        assert _ids(short_page.records) == ["rec-000", "rec-001", "rec-002"]
        assert short_page.has_more is False
        assert short_page.next_cursor is None
        assert short_page.payload_bytes == sum(record.payload_bytes for record in _records(0, 1, 2))

        full = await _new_provider(pool, pg_provider_schema, _scenario("full"), max_page_size=3)
        await full.replace_records(_records(0, 1, 2))
        full_page = await full.fetch(None, 3)
        assert _ids(full_page.records) == ["rec-000", "rec-001", "rec-002"]
        assert full_page.has_more is False
        assert full_page.next_cursor is None

        for bad_limit in (0, -1, 4, cast("int", True)):
            with pytest.raises(ProviderPageLimitError):
                await full.fetch(None, bad_limit)

        full_state = await full.read_scenario_state()
        assert full_state.stats.fetch_calls == 5
        assert full_state.stats.pages_returned == 1
        assert full_state.stats.records_returned == 3
        assert full_state.stats.cursors_issued == 0
        assert full_state.stats.snapshots_created == 0
    finally:
        await pool.close()


@_sync_case
async def test_pg_cursor_handoff_between_independent_pools_and_handles(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    scenario = _scenario("handoff")
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        worker_a = await _new_provider(pool_a, pg_provider_schema, scenario, max_page_size=3)
        worker_b = await _new_provider(pool_b, pg_provider_schema, scenario, max_page_size=3)
        records = _range_records(7)
        await worker_a.replace_records(records)
        assert await worker_b.current_records() == tuple(records)

        first = await worker_a.fetch(None, 3)
        assert _ids(first.records) == ["rec-000", "rec-001", "rec-002"]
        cursor_one = first.next_cursor
        assert cursor_one is not None
        assert "rec-" not in str(cursor_one)
        assert "content-" not in str(cursor_one)

        parked = asyncio.Event()

        async def worker_a_continue() -> ProviderPage:
            await parked.wait()
            return await worker_a.fetch(cursor_one, 3)

        parked_handle = asyncio.create_task(worker_a_continue())
        parked.set()
        try:
            second_b = await worker_b.fetch(cursor_one, 3)
            second_a = await parked_handle
        finally:
            if not parked_handle.done():
                parked_handle.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await parked_handle

        assert _ids(second_b.records) == ["rec-003", "rec-004", "rec-005"]
        assert second_a.records == second_b.records
        cursor_two = second_b.next_cursor
        assert cursor_two is not None
        assert second_a.next_cursor == cursor_two
        assert cursor_two != cursor_one

        third_b = await worker_b.fetch(cursor_two, 3)
        third_a = await worker_a.fetch(cursor_two, 3)
        assert _ids(third_a.records) == ["rec-006"]
        assert third_a.records == third_b.records
        assert third_a.has_more is False
        assert third_b.has_more is False

        state_a = await worker_a.read_scenario_state()
        state_b = await worker_b.read_scenario_state()
        assert state_a == state_b
        stats = state_a.stats
        assert stats.fetch_calls == 5
        assert stats.pages_returned == 5
        assert stats.records_returned == 3 + 3 + 3 + 1 + 1
        assert stats.payload_bytes == (
            sum(record.payload_bytes for record in records)
            + sum(record.payload_bytes for record in second_a.records)
            + sum(record.payload_bytes for record in third_a.records)
        )
        assert stats.cursors_issued == 2
        assert stats.snapshots_created == 1
    finally:
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_pg_snapshot_pins_mutations_and_record_ordinals_stay_stable(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        provider = await _new_provider(pool, pg_provider_schema, _scenario("snapshot"))
        initial = _records(10, 20, 30, 40)
        await provider.replace_records(initial)
        before_identities = await provider.record_identities()
        before_by_id = {identity.record_id: identity for identity in before_identities}
        assert len({identity.ordinal for identity in before_identities}) == len(before_identities)

        first = await provider.fetch(None, 2)
        cursor = first.next_cursor
        assert cursor is not None
        snapshot_before = await provider.snapshot_record_identities(cursor)
        assert len(snapshot_before) == 4
        assert len({identity.ordinal for identity in snapshot_before}) == 4

        inserted_before = await provider.upsert("rec-005", "inserted-before")
        inserted_after = await provider.upsert("rec-025", "inserted-after")
        changed_before = await provider.upsert("rec-010", "changed-before")
        changed_after = await provider.upsert("rec-030", "changed-after")
        assert inserted_before.revision == 0
        assert inserted_after.revision == 0
        assert changed_before.revision == 1
        assert changed_after.revision == 1

        second = await provider.fetch(cursor, 2)
        assert _ids(second.records) == ["rec-030", "rec-040"]
        assert second.records[0].content == "content-030"
        assert second.records[1].content == "content-040"
        assert second.has_more is False
        assert [*first.records, *second.records] == initial

        snapshot_after = await provider.snapshot_record_identities(cursor)
        assert snapshot_after == snapshot_before
        snapshot_by_id = {identity.record_id: identity for identity in snapshot_after}
        assert snapshot_by_id["rec-030"].ordinal == before_by_id["rec-030"].ordinal
        assert snapshot_by_id["rec-010"].ordinal == before_by_id["rec-010"].ordinal

        after_identities = await provider.record_identities()
        after_by_id = {identity.record_id: identity for identity in after_identities}
        assert after_by_id["rec-010"].ordinal == before_by_id["rec-010"].ordinal
        assert after_by_id["rec-030"].ordinal == before_by_id["rec-030"].ordinal
        assert after_by_id["rec-010"].revision == 1
        assert after_by_id["rec-030"].revision == 1
        assert after_by_id["rec-005"].ordinal not in {
            identity.ordinal for identity in before_identities
        }
        assert after_by_id["rec-025"].ordinal not in {
            identity.ordinal for identity in before_identities
        }

        fresh = await _collect(provider, 2)
        assert _ids(fresh) == [
            "rec-005",
            "rec-010",
            "rec-020",
            "rec-025",
            "rec-030",
            "rec-040",
        ]
        by_id = {record.record_id: record for record in fresh}
        assert by_id["rec-005"].content == "inserted-before"
        assert by_id["rec-025"].content == "inserted-after"
        assert by_id["rec-010"].content == "changed-before"
        assert by_id["rec-030"].content == "changed-after"

        state = await provider.read_scenario_state()
        assert state.stats.snapshots_created == 2
        assert state.snapshot_count == 2
    finally:
        await pool.close()


@_sync_case
async def test_pg_concurrent_mutation_does_not_tear_the_pinned_page(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    scenario = _scenario("concurrent-mutation")
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        worker_a = await _new_provider(pool_a, pg_provider_schema, scenario, max_page_size=5)
        worker_b = await _new_provider(pool_b, pg_provider_schema, scenario, max_page_size=5)
        records = _range_records(20)
        await worker_a.replace_records(records)
        first = await worker_a.fetch(None, 5)
        cursor = first.next_cursor
        assert cursor is not None

        outcomes = await asyncio.gather(
            worker_a.fetch(cursor, 5),
            worker_b.upsert("rec-003", "changed-before"),
            worker_b.upsert("rec-007", "changed-after"),
            worker_b.upsert("rec-015", "changed-later"),
            worker_b.upsert("rec-099", "inserted"),
        )
        second = outcomes[0]
        assert isinstance(second, ProviderPage)
        assert _ids(second.records) == ["rec-005", "rec-006", "rec-007", "rec-008", "rec-009"]
        assert all(
            record.content == f"content-{int(record.record_id[-3:]):03d}"
            for record in second.records
        )

        fresh = await _collect(worker_b, 5)
        assert len(fresh) == 21
        assert len(set(_ids(fresh))) == 21
        by_id = {record.record_id: record for record in fresh}
        assert by_id["rec-003"].content == "changed-before"
        assert by_id["rec-007"].content == "changed-after"
        assert by_id["rec-015"].content == "changed-later"
        assert by_id["rec-099"].content == "inserted"

        state = await worker_a.read_scenario_state()
        assert state.stats.snapshots_created == 2
        assert state.record_count == 21
        identities = await worker_b.record_identities()
        assert len(identities) == 21
        assert len({identity.ordinal for identity in identities}) == 21
    finally:
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_pg_simultaneous_first_pages_allocate_unique_snapshots_without_cross_talk(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    scenario = _scenario("simultaneous")
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        worker_a = await _new_provider(pool_a, pg_provider_schema, scenario, max_page_size=3)
        worker_b = await _new_provider(pool_b, pg_provider_schema, scenario, max_page_size=3)
        await worker_a.replace_records(_range_records(8))

        page_a, page_b = await asyncio.gather(
            worker_a.fetch(None, 3),
            worker_b.fetch(None, 3),
        )
        assert _ids(page_a.records) == _ids(page_b.records) == ["rec-000", "rec-001", "rec-002"]
        cursor_a = page_a.next_cursor
        cursor_b = page_b.next_cursor
        assert cursor_a is not None and cursor_b is not None
        assert cursor_a != cursor_b
        assert str(cursor_a).startswith("pc1_")
        assert str(cursor_b).startswith("pc1_")

        state = await worker_a.read_scenario_state()
        assert state.snapshot_count == 2
        assert state.cursor_count == 2
        assert state.stats.fetch_calls == 2
        assert state.stats.snapshots_created == 2
        assert state.stats.cursors_issued == 2

        second_a = await worker_a.fetch(cursor_a, 3)
        second_b = await worker_b.fetch(cursor_b, 3)
        assert _ids(second_a.records) == ["rec-003", "rec-004", "rec-005"]
        assert second_a.records == second_b.records
        assert second_a.next_cursor is not None
        assert second_b.next_cursor is not None
        assert second_a.next_cursor != second_b.next_cursor
    finally:
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_pg_concurrent_initialize_and_inflight_work_stays_typed_and_exact(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    """65 concurrent init/fetch/upsert attempts: no raw driver escape, exact state."""
    scenario = _scenario("init-stress")
    pool = await _create_pool(pg_dsn, min_size=2, max_size=8)
    try:
        base = await _new_provider(pool, pg_provider_schema, scenario, max_page_size=3)
        await base.replace_records(_range_records(30))
        attempts = 0
        for trial in range(5):
            tasks: list[Awaitable[object]] = [
                *(base.fetch(None, 3) for _ in range(6)),
                *(
                    _initialize_on_own_pool(
                        pg_dsn, pg_provider_schema, f"{scenario}-join-{trial}-{i}", max_page_size=3
                    )
                    for i in range(3)
                ),
                *(base.upsert(f"rec-{trial:03d}", f"v{trial}") for _ in range(2)),
                *(
                    _initialize_on_own_pool(pg_dsn, pg_provider_schema, scenario, max_page_size=3)
                    for _ in range(2)
                ),
            ]
            attempts += len(tasks)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            untyped = [item for item in results if isinstance(item, BaseException)]
            assert untyped == [], untyped
            pages = results[:6]
            for page in pages:
                assert isinstance(page, ProviderPage)
                assert _ids(page.records) == ["rec-000", "rec-001", "rec-002"]
            join_results = results[6:9] + results[11:13]
            assert join_results == ["ok", "ok", "ok", "ok", "ok"]
        assert attempts == 65
        state = await base.read_scenario_state()
        assert state.stats.fetch_calls == 30
        assert state.stats.pages_returned == 30
        assert state.stats.records_returned == 90
        assert state.stats.snapshots_created == 30
        assert state.stats.cursors_issued == 30
        assert state.stats.fault_gates_tripped == 0
        assert state.record_count == 30
        assert state.snapshot_count <= state.max_snapshots
        assert state.cursor_count <= state.max_cursors
        idle = await pool.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
            "AND state = 'idle in transaction' AND pid <> pg_backend_pid()"
        )
        assert int(idle or 0) == 0
    finally:
        await pool.close()


@_sync_case
async def test_pg_concurrent_contested_nonce_has_one_winner_and_typed_losers(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    prefix = _scenario("contested-nonce")
    outcomes = await asyncio.gather(
        *(
            _initialize_on_own_pool(
                pg_dsn,
                pg_provider_schema,
                f"{prefix}-{index}",
                cursor_nonce=_CURSOR_NONCE,
            )
            for index in range(6)
        )
    )
    assert outcomes.count("ok") == 1
    assert outcomes.count("typed") == 5
    pool = await _create_pool(pg_dsn)
    try:
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT set_config('search_path', $1, true)", pg_provider_schema)
            stored = int(
                await conn.fetchval(
                    "SELECT count(*) FROM adoption_provider_scenario WHERE cursor_nonce = $1",
                    _CURSOR_NONCE,
                )
                or 0
            )
        assert stored == 1
    finally:
        await pool.close()


@_sync_case
async def test_pg_cursor_validation_fails_closed_typed_without_page_metrics(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        scenario_a = _scenario("cursor-a")
        scenario_b = _scenario("cursor-b")
        worker_a = await _new_provider(
            pool, pg_provider_schema, scenario_a, cursor_nonce=_CURSOR_NONCE
        )
        with pytest.raises(ProviderContractError) as nonce_caught:
            await _new_provider(pool, pg_provider_schema, scenario_b, cursor_nonce=_CURSOR_NONCE)
        assert "nonce" in str(nonce_caught.value)
        worker_b = await _new_provider(
            pool, pg_provider_schema, scenario_b, cursor_nonce=_OTHER_NONCE
        )
        await worker_a.replace_records(_records(0, 1, 2, 3))
        await worker_b.replace_records(_records(0, 1, 2, 3))

        page_a = await worker_a.fetch(None, 2)
        page_b = await worker_b.fetch(None, 2)
        cursor_a = page_a.next_cursor
        cursor_b = page_b.next_cursor
        assert cursor_a is not None and cursor_b is not None
        assert str(cursor_a) != str(cursor_b)

        malformed = ProviderCursor.model_construct(root="not-a-provider-cursor")
        forged = ProviderCursor("pc1_0123456789abcdef_00000000000000ff")
        raw = cast("ProviderCursor", "still-not-a-cursor")
        for bad_cursor in (malformed, forged, raw, cursor_b):
            with pytest.raises(ProviderUnknownCursor) as caught:
                await worker_a.fetch(bad_cursor, 2)
            assert type(caught.value) is ProviderUnknownCursor
            assert caught.value.retryable is False

        expired = await _new_provider(
            pool, pg_provider_schema, _scenario("cursor-expired"), max_snapshots=1
        )
        await expired.replace_records(_records(0, 1, 2, 3))
        old_page = await expired.fetch(None, 2)
        old_cursor = old_page.next_cursor
        assert old_cursor is not None
        new_page = await expired.fetch(None, 2)
        new_cursor = new_page.next_cursor
        assert new_cursor is not None
        with pytest.raises(ProviderCursorExpired) as caught:
            await expired.fetch(old_cursor, 2)
        assert type(caught.value) is ProviderCursorExpired
        assert caught.value.retryable is False
        resumed = await expired.fetch(new_cursor, 2)
        assert _ids(resumed.records) == ["rec-002", "rec-003"]
        assert resumed.has_more is False

        state = await worker_a.read_scenario_state()
        assert state.stats.fetch_calls == 5
        assert state.stats.pages_returned == 1
        assert state.stats.records_returned == 2
        assert state.stats.payload_bytes == page_a.payload_bytes
        assert state.stats.cursors_issued == 1
        assert state.stats.snapshots_created == 1
        assert state.stats.fault_gates_tripped == 0

        expired_state = await expired.read_scenario_state()
        assert expired_state.stats.fetch_calls == 4
        assert expired_state.stats.pages_returned == 3
        assert expired_state.stats.records_returned == 6
        assert expired_state.stats.cursors_issued == 2
        assert expired_state.stats.snapshots_created == 2
        assert expired_state.snapshot_count == 1
    finally:
        await pool.close()


@_sync_case
async def test_pg_bounded_cursor_retention_evicts_oldest_and_stays_typed(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        provider = await _new_provider(
            pool,
            pg_provider_schema,
            _scenario("cursor-evict"),
            max_snapshots=4,
            max_cursors=1,
        )
        await provider.replace_records(_records(0, 1, 2, 3))

        first = await provider.fetch(None, 2)
        old_cursor = first.next_cursor
        assert old_cursor is not None
        second = await provider.fetch(None, 2)
        new_cursor = second.next_cursor
        assert new_cursor is not None
        assert new_cursor != old_cursor

        with pytest.raises(ProviderUnknownCursor) as caught:
            await provider.fetch(old_cursor, 2)
        assert type(caught.value) is ProviderUnknownCursor

        resumed = await provider.fetch(new_cursor, 2)
        assert _ids(resumed.records) == ["rec-002", "rec-003"]
        assert resumed.has_more is False

        state = await provider.read_scenario_state()
        assert state.cursor_count == 1
        assert state.snapshot_count == 2
        assert state.cursor_count <= state.max_cursors
        assert state.snapshot_count <= state.max_snapshots
        assert state.stats.cursors_issued == 2
        assert state.stats.snapshots_created == 2
    finally:
        await pool.close()


@_sync_case
async def test_pg_fault_script_is_durable_across_instances_and_all_phases(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    scenario = _scenario("faults")
    script = ProviderFaultScript(
        [
            ProviderFaultRule(
                ordinal=1,
                phase="before_page",
                fault=ProviderUnavailableFault(message="first"),
            ),
            ProviderFaultRule(
                ordinal=2,
                phase="after_page",
                fault=ProviderPoisonFault(message="second"),
            ),
            ProviderFaultRule(
                ordinal=3,
                phase="before_page",
                fault=ProviderRateLimitedFault(delay_seconds=0.25, message="third"),
            ),
        ]
    )
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        worker_a = await _new_provider(
            pool_a, pg_provider_schema, scenario, fault_script=script, max_page_size=2
        )
        records = _records(0, 1, 2, 3)
        await worker_a.replace_records(records)

        with pytest.raises(ProviderUnavailable) as caught:
            await worker_a.fetch(None, 2)
        assert caught.value.retryable is True
        assert str(caught.value) == "first"

        worker_b = await _new_provider(
            pool_b, pg_provider_schema, scenario, max_page_size=2, cursor_nonce=None
        )
        assert worker_b.fault_script.rules == script.rules
        with pytest.raises(ProviderPoison) as caught:
            await worker_b.fetch(None, 2)
        assert caught.value.retryable is False
        assert str(caught.value) == "second"

        expected_withheld = sum(record.payload_bytes for record in records[:2])
        after_poison = await worker_b.read_scenario_state()
        assert after_poison.stats.fetch_calls == 2
        assert after_poison.stats.fault_gates_tripped == 2
        assert after_poison.stats.withheld_pages == 1
        assert after_poison.stats.withheld_payload_bytes == expected_withheld
        assert after_poison.stats.pages_returned == 0
        assert after_poison.stats.records_returned == 0
        assert after_poison.stats.payload_bytes == 0
        assert after_poison.stats.cursors_issued == 0
        assert after_poison.stats.snapshots_created == 0

        with pytest.raises(ProviderRateLimited) as caught:
            await worker_a.fetch(None, 2)
        assert caught.value.retryable is True
        assert caught.value.delay_seconds == 0.25
        assert caught.value.retry_after_seconds == 0.25

        page = await worker_a.fetch(None, 2)
        assert _ids(page.records) == ["rec-000", "rec-001"]
        assert page.has_more is True
        cursor = page.next_cursor
        assert cursor is not None

        state_a = await worker_a.read_scenario_state()
        state_b = await worker_b.read_scenario_state()
        assert state_a == state_b
        assert state_a.stats.fetch_calls == 4
        assert state_a.stats.fault_gates_tripped == 3
        assert state_a.stats.withheld_pages == 1
        assert state_a.stats.withheld_payload_bytes == expected_withheld
        assert state_a.stats.pages_returned == 1
        assert state_a.stats.records_returned == 2
        assert state_a.stats.payload_bytes == page.payload_bytes
        assert state_a.stats.cursors_issued == 1
        assert state_a.stats.snapshots_created == 1

        second = await worker_b.fetch(cursor, 2)
        assert _ids(second.records) == ["rec-002", "rec-003"]
        assert second.has_more is False

        with pytest.raises(ProviderContractError):
            mismatched = PgPagedProvider(
                pool_b,
                schema=pg_provider_schema,
                scenario=scenario,
                cursor_nonce=_CURSOR_NONCE,
                fault_script=ProviderFaultScript(
                    [ProviderFaultRule(ordinal=1, fault=ProviderPoisonFault())]
                ),
            )
            await mismatched.initialize()
    finally:
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_pg_fault_script_jsonb_numeric_round_trip_and_tamper_rejection(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    """A legal 1e16 delay round-trips; tampered JSON types never coerce or wedge."""
    for bad_value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ProviderContractError):
            _canonical_json_dumps({"delay_seconds": bad_value})

    pool = await _create_pool(pg_dsn)
    try:
        scenario = _scenario("fault-canonical")
        large_delay = 1e16
        script = ProviderFaultScript(
            [
                ProviderFaultRule(
                    ordinal=1,
                    fault=ProviderRateLimitedFault(delay_seconds=large_delay),
                )
            ]
        )
        provider = await _new_provider(
            pool,
            pg_provider_schema,
            scenario,
            cursor_nonce=_CURSOR_NONCE,
            fault_script=script,
        )
        await provider.replace_records(_records(0, 1, 2))
        for _ in range(2):
            await provider.initialize()
        assert provider.fault_script.rules == script.rules

        peer = await _new_provider(pool, pg_provider_schema, scenario)
        assert peer.fault_script.rules == script.rules
        with pytest.raises(ProviderRateLimited) as caught:
            await peer.fetch(None, 2)
        assert caught.value.delay_seconds == large_delay
        state = await provider.read_scenario_state()
        assert state.stats.fetch_calls == 1
        assert state.stats.fault_gates_tripped == 1

        common = {
            "max_records": state.max_records,
            "max_page_size": state.max_page_size,
            "max_snapshots": state.max_snapshots,
            "max_cursors": state.max_cursors,
            "cursor_nonce": state.cursor_nonce,
        }
        changed_scripts = (
            ProviderFaultScript(
                [
                    ProviderFaultRule(
                        ordinal=1,
                        fault=ProviderRateLimitedFault(
                            delay_seconds=large_delay, message="changed"
                        ),
                    )
                ]
            ),
            ProviderFaultScript(
                [
                    ProviderFaultRule(
                        ordinal=1,
                        fault=ProviderRateLimitedFault(delay_seconds=1e15),
                    )
                ]
            ),
            ProviderFaultScript([ProviderFaultRule(ordinal=1, fault=ProviderPoisonFault())]),
            ProviderFaultScript(
                [
                    ProviderFaultRule(
                        ordinal=2,
                        fault=ProviderRateLimitedFault(delay_seconds=large_delay),
                    )
                ]
            ),
        )
        for changed_script in changed_scripts:
            with pytest.raises(ProviderContractError) as script_caught:
                await PgPagedProvider(
                    pool,
                    schema=pg_provider_schema,
                    scenario=scenario,
                    fault_script=changed_script,
                    **common,
                ).initialize()
            assert "different fault script" in str(script_caught.value)

        bad_scripts = (
            '[{"operation":"fetch","ordinal":1,"phase":"before_page","fault":'
            '{"kind":"rate_limited","message":"x","delay_seconds":"0.25"}}]',
            '[{"operation":"fetch","ordinal":1,"phase":"before_page","fault":'
            '{"kind":"rate_limited","message":"x","delay_seconds":true}}]',
            '[{"operation":"fetch","ordinal":1,"phase":"before_page","fault":'
            '{"kind":"rate_limited","message":"x","delay_seconds":'
            f"{'9' * 400}}}}}]",
        )
        for bad_script in bad_scripts:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute("SELECT set_config('search_path', $1, true)", pg_provider_schema)
                await conn.execute(
                    "UPDATE adoption_provider_scenario SET fault_script = $2::jsonb "
                    "WHERE scenario_id = $1",
                    scenario,
                    bad_script,
                )
            with pytest.raises(ProviderContractError) as bad_caught:
                await provider.initialize()
            assert "delay" in str(bad_caught.value)

        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT set_config('search_path', $1, true)", pg_provider_schema)
            await conn.execute(
                "UPDATE adoption_provider_scenario SET fault_script = $2::jsonb "
                "WHERE scenario_id = $1",
                scenario,
                _fault_script_to_json(script),
            )

        for non_finite in ("NaN", "Infinity", "-Infinity"):
            bad_json = (
                '[{"operation":"fetch","ordinal":1,"phase":"before_page","fault":'
                '{"kind":"rate_limited","message":"x","delay_seconds":'
                f"{non_finite}}}]"
            )
            async with pool.acquire() as conn:
                await conn.execute(
                    "SELECT set_config('search_path', $1, false)", pg_provider_schema
                )
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "UPDATE adoption_provider_scenario SET fault_script = $2::jsonb "
                        "WHERE scenario_id = $1",
                        scenario,
                        bad_json,
                    )
            await provider.initialize()
            assert provider.fault_script.rules == script.rules

        await provider.initialize()
        page = await provider.fetch(None, 2)
        assert _ids(page.records) == ["rec-000", "rec-001"]
        state = await provider.read_scenario_state()
        assert state.stats.fetch_calls == 2
        assert state.stats.fault_gates_tripped == 1
        assert state.stats.pages_returned == 1
    finally:
        await pool.close()


@_sync_case
async def test_pg_initialize_validation_branches_are_isolated(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    """Bounds, nonce, and fault-script mismatches produce their own diagnostics."""
    pool = await _create_pool(pg_dsn)
    try:
        scenario = _scenario("validation-branches")
        original_script = ProviderFaultScript(
            [ProviderFaultRule(ordinal=4, fault=ProviderUnavailableFault(message="kept"))]
        )
        provider = await _new_provider(
            pool,
            pg_provider_schema,
            scenario,
            cursor_nonce=_CURSOR_NONCE,
            fault_script=original_script,
        )
        await provider.replace_records(_records(0, 1, 2, 3))
        state = await provider.read_scenario_state()
        common = {
            "max_records": state.max_records,
            "max_page_size": state.max_page_size,
            "max_snapshots": state.max_snapshots,
            "max_cursors": state.max_cursors,
            "cursor_nonce": state.cursor_nonce,
        }

        changed_scripts = (
            ProviderFaultScript(
                [
                    ProviderFaultRule(
                        ordinal=4, fault=ProviderUnavailableFault(message="changed-message")
                    )
                ]
            ),
            ProviderFaultScript([ProviderFaultRule(ordinal=4, fault=ProviderPoisonFault())]),
            ProviderFaultScript(
                [ProviderFaultRule(ordinal=4, fault=ProviderRateLimitedFault(delay_seconds=0.25))]
            ),
            ProviderFaultScript(
                [ProviderFaultRule(ordinal=5, fault=ProviderUnavailableFault(message="kept"))]
            ),
        )
        for changed_script in changed_scripts:
            with pytest.raises(ProviderContractError) as caught:
                await PgPagedProvider(
                    pool,
                    schema=pg_provider_schema,
                    scenario=scenario,
                    fault_script=changed_script,
                    **common,
                ).initialize()
            message = str(caught.value)
            assert "different fault script" in message
            assert "max_" not in message

        with pytest.raises(ProviderContractError) as bounds_caught:
            await PgPagedProvider(
                pool,
                schema=pg_provider_schema,
                scenario=scenario,
                fault_script=original_script,
                **{**common, "max_cursors": state.max_cursors - 1},
            ).initialize()
        assert "max_cursors" in str(bounds_caught.value)
        assert "fault script" not in str(bounds_caught.value)

        with pytest.raises(ProviderContractError) as nonce_caught:
            await PgPagedProvider(
                pool,
                schema=pg_provider_schema,
                scenario=scenario,
                fault_script=original_script,
                **{**common, "cursor_nonce": _OTHER_NONCE},
            ).initialize()
        assert "nonce" in str(nonce_caught.value)
        assert "fault script" not in str(nonce_caught.value)

        page = await provider.fetch(None, 2)
        assert _ids(page.records) == ["rec-000", "rec-001"]
        assert (await provider.read_scenario_state()).stats.fetch_calls == 1
    finally:
        await pool.close()


@_sync_case
async def test_pg_fault_ordinal_counts_invalid_input_but_never_masks_it(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    """Invalid input AT poisoned ordinals is typed; the next valid call fires once."""
    script = ProviderFaultScript(
        [
            ProviderFaultRule(ordinal=1, fault=ProviderPoisonFault(message="poison-one")),
            ProviderFaultRule(ordinal=2, fault=ProviderPoisonFault(message="poison-two")),
            ProviderFaultRule(ordinal=3, fault=ProviderPoisonFault(message="poison-three")),
        ]
    )
    scenario = _scenario("fault-input")
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        provider = await _new_provider(pool_a, pg_provider_schema, scenario, fault_script=script)
        await provider.replace_records(_records(0, 1))

        with pytest.raises(ProviderPageLimitError):
            await provider.fetch(None, 0)
        state = await provider.read_scenario_state()
        assert state.stats.fetch_calls == 1
        assert state.stats.fault_gates_tripped == 0

        malformed = ProviderCursor.model_construct(root="not-a-provider-cursor")
        with pytest.raises(ProviderUnknownCursor) as caught:
            await provider.fetch(malformed, 2)
        assert type(caught.value) is ProviderUnknownCursor
        state = await provider.read_scenario_state()
        assert state.stats.fetch_calls == 2
        assert state.stats.fault_gates_tripped == 0

        peer = await _new_provider(pool_b, pg_provider_schema, scenario)
        assert peer.fault_script.rules == script.rules
        with pytest.raises(ProviderPoison) as caught:
            await peer.fetch(None, 2)
        assert str(caught.value) == "poison-three"
        state = await peer.read_scenario_state()
        assert state.stats.fetch_calls == 3
        assert state.stats.fault_gates_tripped == 1
        assert state.stats.pages_returned == 0

        page = await provider.fetch(None, 2)
        assert _ids(page.records) == ["rec-000", "rec-001"]
        state = await provider.read_scenario_state()
        assert state.stats.fetch_calls == 4
        assert state.stats.fault_gates_tripped == 1
        assert state.stats.pages_returned == 1
        assert await peer.read_scenario_state() == state
    finally:
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_pg_record_contract_rejects_smuggle_and_preserves_max_values(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        provider = await _new_provider(pool, pg_provider_schema, _scenario("records"))
        await provider.replace_records(_records(1))
        before = await provider.current_records()

        smuggled = ProviderRecord.model_construct(record_id="", revision=0, content="x")
        with pytest.raises(ProviderMalformedRecord):
            await provider.replace_records([smuggled])
        assert await provider.current_records() == before
        with pytest.raises(ProviderMalformedRecord):
            await provider.replace_records([_record(2), smuggled])
        assert await provider.current_records() == before

        emoji = "\U0001f600" * (MAX_RECORD_CONTENT_BYTES // 4 + 1)
        with pytest.raises(ValidationError):
            ProviderRecord(record_id="rec", revision=0, content=emoji)
        byte_smuggled = ProviderRecord.model_construct(
            record_id="rec-001", revision=0, content=emoji
        )
        with pytest.raises(ProviderMalformedRecord):
            await provider.replace_records([byte_smuggled])
        with pytest.raises(ProviderMalformedRecord):
            await provider.upsert("rec-001", emoji)
        with pytest.raises(ProviderMalformedRecord):
            await provider.upsert("bad id", "x")
        with pytest.raises(ProviderMalformedRecord):
            await provider.upsert(cast("str", 123), "x")
        with pytest.raises(ProviderMalformedRecord):
            await provider.upsert("rec-nul", "a\x00b")
        with pytest.raises(ProviderMalformedRecord):
            await provider.upsert("rec-001", "different-at-same-revision", revision=0)
        with pytest.raises(ProviderMalformedRecord):
            await provider.upsert("rec-001", "bool-revision", revision=cast("int", True))
        assert await provider.current_records() == before

        max_provider = await _new_provider(pool, pg_provider_schema, _scenario("records-max"))
        max_record = await max_provider.upsert("rec-max", "max", revision=MAX_RECORD_REVISION)
        assert max_record.revision == MAX_RECORD_REVISION
        persisted = await max_provider.current_records()
        assert len(persisted) == 1
        assert persisted[0].revision == MAX_RECORD_REVISION
        assert persisted[0].content == "max"
        with pytest.raises(ProviderMalformedRecord):
            await max_provider.upsert("rec-max", "overflow")

        bounded = await _new_provider(
            pool, pg_provider_schema, _scenario("records-bound"), max_records=2
        )
        with pytest.raises(ProviderRecordLimitExceeded):
            await bounded.replace_records(_records(0, 1, 2))
        assert await bounded.current_records() == ()
        with pytest.raises(ProviderRecordLimitExceeded):
            await bounded.replace_records(_records(0, 1))
            await bounded.upsert("rec-002", "overflow")
        assert len(await bounded.current_records()) == 2
    finally:
        await pool.close()


@_sync_case
async def test_pg_concurrent_replay_counts_are_exact_across_pools(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    scenario = _scenario("replay-counts")
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        worker_a = await _new_provider(pool_a, pg_provider_schema, scenario, max_page_size=4)
        worker_b = await _new_provider(pool_b, pg_provider_schema, scenario, max_page_size=4)
        records = _range_records(20)
        await worker_a.replace_records(records)
        first = await worker_a.fetch(None, 4)
        cursor = first.next_cursor
        assert cursor is not None

        replays = await asyncio.gather(
            *(worker_a.fetch(cursor, 4) for _ in range(10)),
            *(worker_b.fetch(cursor, 4) for _ in range(10)),
        )
        for page in replays:
            assert _ids(page.records) == ["rec-004", "rec-005", "rec-006", "rec-007"]
            assert page.has_more is True
        assert len({str(page.next_cursor) for page in replays}) == 1
        second_cursor = replays[0].next_cursor
        assert second_cursor is not None

        state_a = await worker_a.read_scenario_state()
        state_b = await worker_b.read_scenario_state()
        assert state_a == state_b
        stats = state_a.stats
        page_bytes = sum(record.payload_bytes for record in records[:4])
        replay_bytes = sum(record.payload_bytes for record in records[4:8])
        assert stats.fetch_calls == 21
        assert stats.pages_returned == 21
        assert stats.records_returned == 4 + 20 * 4
        assert stats.payload_bytes == page_bytes + 20 * replay_bytes
        assert stats.cursors_issued == 2
        assert stats.snapshots_created == 1
        assert state_a.snapshot_count == 1
        assert state_a.cursor_count == 2
        assert state_a.stats.fault_gates_tripped == 0

        final = await worker_b.fetch(second_cursor, 4)
        assert _ids(final.records) == ["rec-008", "rec-009", "rec-010", "rec-011"]
        assert final.has_more is True
    finally:
        await pool_a.close()
        await pool_b.close()


def test_pg_cursor_handoff_across_separate_event_loops_and_pools(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    scenario = _scenario("thread-loops")
    records = _range_records(7)

    async def seed() -> None:
        pool = await _create_pool(pg_dsn)
        try:
            provider = await _new_provider(pool, pg_provider_schema, scenario, max_page_size=3)
            await provider.replace_records(records)
        finally:
            await pool.close()

    asyncio.run(seed())

    first_ready = Event()
    release_a = Event()
    cursor_queue: Queue[str] = Queue()

    def worker_a() -> ProviderPage:
        async def run() -> ProviderPage:
            pool = await _create_pool(pg_dsn)
            try:
                provider = await _new_provider(pool, pg_provider_schema, scenario, max_page_size=3)
                first = await provider.fetch(None, 3)
                cursor = first.next_cursor
                assert cursor is not None
                cursor_queue.put(str(cursor))
                first_ready.set()
                assert release_a.wait(timeout=60)
                return await provider.fetch(cursor, 3)
            finally:
                await pool.close()

        return asyncio.run(run())

    def worker_b(cursor_token: str) -> ProviderPage:
        async def run() -> ProviderPage:
            pool = await _create_pool(pg_dsn)
            try:
                provider = await _new_provider(pool, pg_provider_schema, scenario, max_page_size=3)
                return await provider.fetch(ProviderCursor(cursor_token), 3)
            finally:
                await pool.close()

        return asyncio.run(run())

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(worker_a)
        assert first_ready.wait(timeout=60), "worker A did not publish its first-page cursor"
        cursor_token = cursor_queue.get(timeout=10)
        future_b = executor.submit(worker_b, cursor_token)
        try:
            second_b = future_b.result(timeout=60)
        finally:
            release_a.set()
        second_a = future_a.result(timeout=60)

    assert _ids(second_a.records) == ["rec-003", "rec-004", "rec-005"]
    assert second_a.records == second_b.records
    assert second_a.next_cursor is not None
    assert second_a.next_cursor == second_b.next_cursor

    async def final_state() -> PgProviderScenarioState:
        pool = await _create_pool(pg_dsn)
        try:
            provider = await _new_provider(pool, pg_provider_schema, scenario, max_page_size=3)
            return await provider.read_scenario_state()
        finally:
            await pool.close()

    state = asyncio.run(final_state())
    assert state.stats.fetch_calls == 3
    assert state.stats.pages_returned == 3
    assert state.stats.records_returned == 3 + 3 + 3
    assert state.stats.cursors_issued == 2
    assert state.stats.snapshots_created == 1


def test_pg_concurrent_initialize_across_threads_and_fresh_schema(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    """Fresh-schema bootstrap and peer adoption from four separate loops/pools."""
    schema = (pg_provider_schema + "_thr")[:63]
    _admin_statement(
        pg_dsn, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
    )  # Why: locally generated validated identifier.
    barrier = Barrier(4)

    def run_init(scenario_name: str) -> str:
        async def run() -> str:
            pool = await _create_pool(pg_dsn, min_size=1, max_size=2)
            try:
                provider = PgPagedProvider(
                    pool,
                    schema=schema,
                    scenario=scenario_name,
                    max_records=100,
                    max_page_size=10,
                    max_snapshots=8,
                    max_cursors=64,
                )
                barrier.wait(timeout=30)
                await provider.initialize()
                return "ok"
            finally:
                await pool.close()

        return asyncio.run(run())

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(run_init, name)
                for name in ("peer", "peer", "distinct-a", "distinct-b")
            ]
            results = [future.result(timeout=120) for future in futures]
        assert results == ["ok", "ok", "ok", "ok"]

        async def verify() -> tuple[PgProviderScenarioState, ProviderPage, int]:
            pool = await _create_pool(pg_dsn, min_size=1, max_size=2)
            try:
                provider = await _new_provider(pool, schema, "peer")
                state = await provider.read_scenario_state()
                await provider.replace_records(_records(0, 1, 2))
                page = await provider.fetch(None, 2)
                async with pool.acquire() as conn:
                    await conn.execute("SELECT set_config('search_path', $1, false)", schema)
                    scenario_count = int(
                        await conn.fetchval("SELECT count(*) FROM adoption_provider_scenario") or 0
                    )
                return state, page, scenario_count
            finally:
                await pool.close()

        state, page, scenario_count = asyncio.run(verify())
        assert scenario_count == 3
        assert state.stats.fetch_calls == 0
        assert _ids(page.records) == ["rec-000", "rec-001"]
        assert page.has_more is True
    finally:
        _admin_statement(
            pg_dsn, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )  # Why: locally generated validated identifier.


@_sync_case
async def test_pg_incompatible_preexisting_tables_fail_closed_without_permanent_blocker(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    bad_schema = f"{pg_provider_schema}_bad"
    pool = await _create_pool(pg_dsn)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                f'DROP SCHEMA IF EXISTS "{bad_schema}" CASCADE'
            )  # Why: locally generated validated identifier.
            await conn.execute(
                f'CREATE SCHEMA "{bad_schema}"'
            )  # Why: locally generated validated identifier.
            await conn.execute(
                f'CREATE TABLE "{bad_schema}".adoption_provider_scenario (scenario_id text)'
            )  # Why: locally generated validated identifier.

        provider = PgPagedProvider(
            pool,
            schema=bad_schema,
            scenario=_scenario("bad-shape"),
            cursor_nonce=_CURSOR_NONCE,
        )
        with pytest.raises(ProviderContractError) as caught:
            await provider.initialize()
        assert "incompatible pre-existing" in str(caught.value)

        async with pool.acquire() as conn:
            await conn.execute(
                f'DROP TABLE "{bad_schema}".adoption_provider_scenario'
            )  # Why: locally generated validated identifier.

        await provider.initialize()
        state = await provider.read_scenario_state()
        assert state.stats.fetch_calls == 0
        page = await provider.fetch(None, 2)
        assert page.records == ()
    finally:
        async with pool.acquire() as conn:
            await conn.execute(
                f'DROP SCHEMA IF EXISTS "{bad_schema}" CASCADE'
            )  # Why: locally generated validated identifier.
        await pool.close()


@_sync_case
async def test_pg_fetch_owns_its_transaction_and_spawns_no_tasks(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    pool = await _create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        provider = await _new_provider(pool, pg_provider_schema, _scenario("tx"))
        await provider.replace_records(_records(0, 1, 2, 3))

        before_tasks = asyncio.all_tasks()
        async with pool.acquire() as conn:
            assert not conn.is_in_transaction()
            page = await provider.fetch(None, 2)
            assert not conn.is_in_transaction()
        assert page.has_more is True
        assert asyncio.all_tasks() == before_tasks
        assert pool.get_idle_size() == pool.get_size()
        assert not pool.is_closing()

        state = await provider.read_scenario_state()
        assert state.stats.pages_returned == 1
        assert state.stats.snapshots_created == 1
        assert state.stats.cursors_issued == 1

        ambient = await _new_provider(pool, pg_provider_schema, _scenario("ambient"))
        await ambient.replace_records(_records(0, 1))
        async with pool.acquire() as held, held.transaction():
            page = await ambient.fetch(None, 2)
            assert held.is_in_transaction()
            assert page.has_more is False
        ambient_state = await ambient.read_scenario_state()
        assert ambient_state.stats.fetch_calls == 1
        assert ambient_state.stats.pages_returned == 1
    finally:
        await pool.close()


@_sync_case
async def test_pg_preexisting_wrong_column_type_fails_closed(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    bad_schema = f"{pg_provider_schema}_types"
    pool = await _create_pool(pg_dsn)
    try:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{bad_schema}" CASCADE')
            await conn.execute(f'CREATE SCHEMA "{bad_schema}"')
            await conn.execute(
                f'CREATE TABLE "{bad_schema}".adoption_provider_record ('
                "scenario_id text, ordinal text, record_id text, "
                "revision numeric(20, 0), content text)"
            )

        provider = PgPagedProvider(
            pool,
            schema=bad_schema,
            scenario=_scenario("bad-type"),
            cursor_nonce=_CURSOR_NONCE,
        )
        with pytest.raises(ProviderContractError) as caught:
            await provider.initialize()
        assert "column" in str(caught.value)
        assert "types" in str(caught.value)
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{bad_schema}" CASCADE')
        await pool.close()


@_sync_case
async def test_pg_setup_driver_failure_is_typed_with_cause_and_recoverable(
    pg_dsn: str, pg_provider_schema: str
) -> None:
    """A duplicate-key DDL failure becomes typed with the driver error as __cause__."""
    schema = (pg_provider_schema + "_cause")[:63]
    pool = await _create_pool(pg_dsn)
    try:
        seed = await _new_provider(pool, schema, "seed")
        assert (await seed.read_scenario_state()).scenario == "seed"
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT set_config('search_path', $1, true)", schema)
            await conn.execute(
                "ALTER TABLE adoption_provider_scenario "
                "DROP CONSTRAINT IF EXISTS adoption_provider_scenario_pkey CASCADE"
            )
            await conn.execute("DROP INDEX IF EXISTS adoption_provider_scenario_id_uidx")
            await conn.execute(
                "INSERT INTO adoption_provider_scenario "
                "(scenario_id, cursor_nonce, max_records, max_page_size, "
                "max_snapshots, max_cursors) VALUES ($1, $2, 100, 10, 8, 64)",
                "seed",
                "aaaaaaaaaaaaaaaa",
            )

        provider = PgPagedProvider(
            pool,
            schema=schema,
            scenario="after-duplicate",
        )
        with pytest.raises(ProviderContractError) as caught:
            await provider.initialize()
        assert isinstance(caught.value.__cause__, asyncpg.UniqueViolationError)

        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT set_config('search_path', $1, true)", schema)
            await conn.execute(
                "DELETE FROM adoption_provider_scenario "
                "WHERE scenario_id = $1 AND cursor_nonce = $2",
                "seed",
                "aaaaaaaaaaaaaaaa",
            )
        await provider.initialize()
        state = await provider.read_scenario_state()
        assert state.scenario == "after-duplicate"
        assert state.stats.fetch_calls == 0
        idle = await pool.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
            "AND state = 'idle in transaction' AND pid <> pg_backend_pid()"
        )
        assert int(idle or 0) == 0
    finally:
        await pool.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await pool.close()


def test_pg_protocol_and_module_boundaries_are_explicit() -> None:
    assert inspect.iscoroutinefunction(PgPagedProvider.fetch)
    assert inspect.iscoroutinefunction(PgPagedProvider.initialize)
    assert issubclass(PgPagedProvider, PagedProvider)
    assert not hasattr(PgPagedProvider, "close")
    unused_pool = cast("asyncpg.Pool", object())
    for bad_schema in ("public", "pg_catalog", "Bad-Schema", "1st"):
        with pytest.raises(ProviderContractError):
            PgPagedProvider(unused_pool, schema=bad_schema)
