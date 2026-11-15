"""Direct in-memory control for the generic paged-provider fixture.

This is the *fixture-local* control lane, labelled explicitly: it runs no
Postgres, no worker, no workflow engine, no network and no filesystem. It
proves the generic provider protocol and the local fail-closed fixture
semantics only. It is not a runnable workflow sample, not adoption
evidence, and it does not prove engine retry/backoff/ledger/parking or
dependency-lifecycle behaviour.

The fixture deliberately chooses an **atomic snapshot** cursor policy:
the first page of a scan snapshots the current provider records; later
pages and replays read that immutable snapshot. Mutations made after the
scan starts (inserts or content revisions, before or after the cursor)
never enter that scan. A fresh scan sees them, so the overall source is
not hidden - the scan boundary is explicit instead of a live keyset that
could silently skip a row inserted behind the cursor.
"""

from __future__ import annotations

import asyncio
import inspect
import math
from collections.abc import Callable, Coroutine, Iterator
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import cast

import pytest
from examples.adoption.provider import (
    MAX_FAULT_RULES,
    MAX_RECORD_CONTENT_BYTES,
    MAX_RECORD_REVISION,
    LocalPagedProvider,
    PagedProvider,
    ProviderContractError,
    ProviderCursor,
    ProviderCursorExpired,
    ProviderFaultRule,
    ProviderFaultScript,
    ProviderMalformedRecord,
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


def _sync_case[T](
    fn: Callable[[], Coroutine[object, object, T]],
) -> Callable[[], T]:
    """Run one hermetic async scenario without the wf-family PG fixtures.

    The suite's collection hook appends the module-scoped workflow G7
    fixture to every *coroutine* test whose filename starts ``test_wf_``.
    This direct fixture-control module needs no Postgres at all, so each
    scenario is wrapped as a plain sync test while still running its body
    on a real asyncio loop.
    """

    @wraps(fn)
    def wrapper() -> T:
        return asyncio.run(fn())

    return wrapper


def _record(index: int, *, revision: int = 0, content: str | None = None) -> ProviderRecord:
    return ProviderRecord(
        record_id=f"rec-{index:03d}",
        revision=revision,
        content=f"content-{index:03d}" if content is None else content,
    )


def _records(count: int) -> list[ProviderRecord]:
    return [_record(index) for index in range(count)]


def _over_bound_records() -> Iterator[ProviderRecord]:
    for index in range(10):
        yield _record(index)
    raise AssertionError("provider consumed records past its declared bound")


def _over_bound_rules() -> Iterator[ProviderFaultRule]:
    for ordinal in range(1, MAX_FAULT_RULES + 10):
        yield ProviderFaultRule(ordinal=ordinal, fault=ProviderPoisonFault())
    raise AssertionError("fault script consumed rules past its declared bound")


def _provider(
    records: list[ProviderRecord] | None = None,
    *,
    max_records: int = 100,
    max_page_size: int = 10,
    max_snapshots: int = 8,
    max_cursors: int = 64,
    cursor_nonce: str = "0123456789abcdef",
    fault_script: ProviderFaultScript | None = None,
) -> LocalPagedProvider:
    return LocalPagedProvider(
        records=() if records is None else records,
        max_records=max_records,
        max_page_size=max_page_size,
        max_snapshots=max_snapshots,
        max_cursors=max_cursors,
        cursor_nonce=cursor_nonce,
        fault_script=fault_script,
    )


async def _collect(provider: PagedProvider, page_limit: int) -> list[ProviderRecord]:
    collected: list[ProviderRecord] = []
    cursor: ProviderCursor | None = None
    pages = 0
    while True:
        page = await provider.fetch(cursor, page_limit)
        pages += 1
        collected.extend(page.records)
        if not page.has_more:
            assert page.next_cursor is None
            break
        next_cursor = page.next_cursor
        assert next_cursor is not None
        cursor = next_cursor
        assert pages <= 1_000
    return collected


def _ids(records: tuple[ProviderRecord, ...] | list[ProviderRecord]) -> list[str]:
    return [record.record_id for record in records]


def test_protocol_is_async_and_fixture_satisfies_it() -> None:
    provider = _provider([])
    assert isinstance(provider, PagedProvider)
    assert inspect.iscoroutinefunction(PagedProvider.fetch)
    assert inspect.iscoroutinefunction(LocalPagedProvider.fetch)


@_sync_case
async def test_empty_provider_returns_one_terminal_empty_page() -> None:
    provider = _provider([])

    page = await provider.fetch(None, 3)

    assert page.records == ()
    assert page.has_more is False
    assert page.next_cursor is None
    assert page.payload_bytes == 0
    stats = provider.stats
    assert stats.fetch_calls == 1
    assert stats.pages_returned == 1
    assert stats.records_returned == 0
    assert stats.payload_bytes == 0
    assert stats.cursors_issued == 0
    assert stats.snapshots_created == 0

    replay = await provider.fetch(None, 3)
    assert replay.records == ()
    assert replay.has_more is False
    assert provider.stats.fetch_calls == 2
    assert provider.stats.pages_returned == 2


@_sync_case
async def test_short_page_is_terminal_and_offers_no_empty_next_page() -> None:
    records = _records(3)
    provider = _provider(records)

    page = await provider.fetch(None, 5)

    assert _ids(page.records) == ["rec-000", "rec-001", "rec-002"]
    assert page.has_more is False
    assert page.next_cursor is None
    assert page.payload_bytes == sum(record.payload_bytes for record in records)
    assert provider.stats.pages_returned == 1
    assert provider.stats.records_returned == 3


@_sync_case
async def test_exactly_full_page_is_terminal_not_has_more() -> None:
    records = _records(3)
    provider = _provider(records)

    page = await provider.fetch(None, 3)

    assert _ids(page.records) == ["rec-000", "rec-001", "rec-002"]
    assert page.has_more is False
    assert page.next_cursor is None
    assert provider.stats.pages_returned == 1
    assert provider.stats.cursors_issued == 0


@_sync_case
async def test_split_cursor_pages_and_replay_are_exact() -> None:
    records = _records(7)
    provider = _provider(records)

    page_one = await provider.fetch(None, 3)
    assert _ids(page_one.records) == ["rec-000", "rec-001", "rec-002"]
    assert page_one.has_more is True
    cursor_one = page_one.next_cursor
    assert cursor_one is not None

    page_two = await provider.fetch(cursor_one, 3)
    assert _ids(page_two.records) == ["rec-003", "rec-004", "rec-005"]
    assert page_two.has_more is True
    cursor_two = page_two.next_cursor
    assert cursor_two is not None

    replay_two = await provider.fetch(cursor_one, 3)
    assert replay_two.records == page_two.records
    assert replay_two.next_cursor == cursor_two

    page_three = await provider.fetch(cursor_two, 3)
    assert _ids(page_three.records) == ["rec-006"]
    assert page_three.has_more is False
    assert page_three.next_cursor is None

    assert _ids(await _collect(provider, 3)) == _ids(records)

    stats = provider.stats
    assert stats.fetch_calls == 7
    assert stats.pages_returned == 7
    assert stats.records_returned == 3 + 3 + 1 + 3 + 7
    assert stats.payload_bytes == (
        sum(record.payload_bytes for record in records) * 2
        + sum(record.payload_bytes for record in page_two.records)
    )
    assert stats.cursors_issued == 4
    assert stats.snapshots_created == 2


@_sync_case
async def test_page_limit_can_change_between_pages_without_cursor_holes() -> None:
    records = _records(8)
    provider = _provider(records)

    page_one = await provider.fetch(None, 2)
    cursor = page_one.next_cursor
    assert cursor is not None
    page_two = await provider.fetch(cursor, 1)
    cursor = page_two.next_cursor
    assert cursor is not None
    page_three = await provider.fetch(cursor, 10)

    seen = [*page_one.records, *page_two.records, *page_three.records]
    assert _ids(seen) == _ids(records)
    assert len(set(_ids(seen))) == len(records)
    assert page_three.has_more is False


@_sync_case
async def test_atomic_snapshot_pins_insert_and_change_before_after_cursor() -> None:
    initial = [_record(10), _record(20), _record(30), _record(40)]
    provider = _provider(initial)

    first = await provider.fetch(None, 2)
    cursor = first.next_cursor
    assert cursor is not None

    inserted_before = provider.upsert("rec-005", "inserted-before")
    inserted_after = provider.upsert("rec-025", "inserted-after")
    changed_before = provider.upsert("rec-010", "changed-before")
    changed_after = provider.upsert("rec-030", "changed-after")
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

    replay = await provider.fetch(cursor, 2)
    assert replay.records == second.records

    fresh = await _collect(provider, 2)
    assert _ids(fresh) == ["rec-005", "rec-010", "rec-020", "rec-025", "rec-030", "rec-040"]
    by_id = {record.record_id: record for record in fresh}
    assert by_id["rec-005"].content == "inserted-before"
    assert by_id["rec-025"].content == "inserted-after"
    assert by_id["rec-010"].content == "changed-before"
    assert by_id["rec-030"].content == "changed-after"
    assert provider.stats.snapshots_created == 2


@_sync_case
async def test_revision_shift_across_page_boundary_leaves_no_false_holes() -> None:
    initial = _records(6)
    provider = _provider(initial)

    first = await provider.fetch(None, 2)
    cursor = first.next_cursor
    assert cursor is not None

    # The revised record sits BEFORE the cursor; the other revision sits
    # AFTER it. A live keyset could skip one and duplicate the other. The
    # pinned snapshot must instead return the original six exactly once.
    provider.upsert("rec-001", "shifted-before")
    provider.upsert("rec-004", "shifted-after")
    provider.upsert("rec-002", "changed-before-cursor")

    second = await provider.fetch(cursor, 4)
    old_scan = [*first.records, *second.records]
    assert _ids(old_scan) == _ids(initial)
    assert len(set(_ids(old_scan))) == len(initial)
    assert old_scan[1].content == "content-001"
    assert old_scan[4].content == "content-004"

    fresh = await _collect(provider, 2)
    assert _ids(fresh) == _ids(initial)
    assert len(set(_ids(fresh))) == len(initial)
    by_id = {record.record_id: record for record in fresh}
    assert by_id["rec-001"].content == "shifted-before"
    assert by_id["rec-002"].content == "changed-before-cursor"
    assert by_id["rec-004"].content == "shifted-after"


@_sync_case
async def test_cursor_is_opaque_and_not_a_record_identity() -> None:
    provider = _provider(
        [
            ProviderRecord(record_id="alpha", revision=0, content="secret-alpha"),
            ProviderRecord(record_id="beta", revision=0, content="secret-beta"),
        ]
    )

    first = await provider.fetch(None, 1)
    cursor = first.next_cursor
    assert cursor is not None
    token = str(cursor)
    assert token.startswith("pc1_")
    assert "alpha" not in token
    assert "beta" not in token
    assert "secret" not in token
    assert token != "alpha"
    assert token != "beta"
    assert not hasattr(cursor, "record_id")

    second = await provider.fetch(cursor, 1)
    assert _ids(second.records) == ["beta"]
    assert second.records[0].record_id == "beta"

    forged = ProviderCursor.model_construct(root=f"pc1_0123456789abcdef_{'0' * 16}")
    with pytest.raises(ProviderUnknownCursor):
        await provider.fetch(forged, 1)

    with pytest.raises(ValidationError):
        ProviderCursor.model_validate("alpha")
    with pytest.raises(ValidationError):
        ProviderCursor.model_validate("pc1_0123456789abcdef_00000000000000zz")


@_sync_case
async def test_unknown_and_malformed_cursors_fail_closed_without_page_metrics() -> None:
    provider = _provider(_records(2))
    foreign = _provider(_records(2), cursor_nonce="fedcba9876543210")
    foreign_page = await foreign.fetch(None, 1)
    foreign_cursor = foreign_page.next_cursor
    assert foreign_cursor is not None

    unissued = ProviderCursor("pc1_0123456789abcdef_00000000000000ff")
    malformed = ProviderCursor.model_construct(root="not-a-provider-cursor")
    raw = cast("ProviderCursor", "still-not-a-cursor")

    for bad_cursor in (unissued, malformed, raw, foreign_cursor):
        with pytest.raises(ProviderUnknownCursor) as caught:
            await provider.fetch(bad_cursor, 1)
        assert caught.value.retryable is False
        assert isinstance(caught.value, ProviderContractError)

    stats = provider.stats
    assert stats.fetch_calls == 4
    assert stats.pages_returned == 0
    assert stats.records_returned == 0
    assert stats.payload_bytes == 0
    assert stats.cursors_issued == 0
    assert stats.snapshots_created == 0


@_sync_case
async def test_bounded_snapshot_cache_expires_old_cursor_with_typed_error() -> None:
    provider = _provider(_records(4), max_snapshots=1, max_cursors=8)

    first = await provider.fetch(None, 2)
    old_cursor = first.next_cursor
    assert old_cursor is not None

    second = await provider.fetch(None, 2)
    new_cursor = second.next_cursor
    assert new_cursor is not None

    with pytest.raises(ProviderCursorExpired) as caught:
        await provider.fetch(old_cursor, 2)
    assert caught.value.retryable is False
    assert isinstance(caught.value, ProviderUnknownCursor)

    next_page = await provider.fetch(new_cursor, 2)
    assert _ids(next_page.records) == ["rec-002", "rec-003"]
    assert provider.stats.fetch_calls == 4
    assert provider.stats.pages_returned == 3


@_sync_case
async def test_bounded_cursor_cache_evicts_oldest_and_fails_closed() -> None:
    provider = _provider(_records(4), max_snapshots=4, max_cursors=1)

    first = await provider.fetch(None, 2)
    old_cursor = first.next_cursor
    assert old_cursor is not None

    second = await provider.fetch(None, 2)
    new_cursor = second.next_cursor
    assert new_cursor is not None

    with pytest.raises(ProviderUnknownCursor) as caught:
        await provider.fetch(old_cursor, 2)
    assert type(caught.value) is ProviderUnknownCursor

    next_page = await provider.fetch(new_cursor, 2)
    assert _ids(next_page.records) == ["rec-002", "rec-003"]
    assert provider.stats.cursors_issued == 2


def test_record_backlog_is_explicitly_bounded_with_no_partial_insert() -> None:
    with pytest.raises(ProviderRecordLimitExceeded):
        _provider(_records(3), max_records=2)

    provider = _provider(_records(2), max_records=2)
    before = provider.current_records()

    with pytest.raises(ProviderRecordLimitExceeded):
        provider.upsert("rec-999", "overflow")

    assert provider.current_records() == before
    assert provider.record_count == 2

    replacement = _provider([_record(0)], max_records=2)
    replacement_before = replacement.current_records()
    with pytest.raises(ProviderRecordLimitExceeded):
        replacement.replace_records(_over_bound_records())
    assert replacement.current_records() == replacement_before


@_sync_case
async def test_page_limit_gate_accepts_exact_bounds_and_refuses_oversized() -> None:
    provider = _provider(_records(4), max_page_size=4)

    for bad_limit in (0, -1, 5, cast("int", True)):
        with pytest.raises(ProviderPageLimitError):
            await provider.fetch(None, bad_limit)

    assert provider.stats.fetch_calls == 4
    assert provider.stats.pages_returned == 0

    one = await provider.fetch(None, 1)
    assert len(one.records) == 1
    exact = await provider.fetch(None, 4)
    assert len(exact.records) == 4
    assert exact.has_more is False


def test_record_contract_rejects_malformed_duplicate_and_stale_revisions() -> None:
    with pytest.raises(ValidationError):
        ProviderRecord(record_id="", revision=0, content="x")
    with pytest.raises(ValidationError):
        ProviderRecord(record_id="bad id", revision=0, content="x")
    with pytest.raises(ValidationError):
        ProviderRecord(record_id=cast("str", 123), revision=0, content="x")
    with pytest.raises(ValidationError):
        ProviderRecord(record_id="rec", revision=0, content=cast("str", b"bytes"))
    with pytest.raises(ValidationError):
        ProviderRecord(record_id="rec", revision=-1, content="x")
    with pytest.raises(ValidationError):
        ProviderRecord(record_id="rec", revision=True, content="x")
    with pytest.raises(ValidationError):
        ProviderRecord(
            record_id="rec",
            revision=0,
            content="x" * (MAX_RECORD_CONTENT_BYTES + 1),
        )
    with pytest.raises(ValidationError):
        ProviderRecord(
            record_id="rec",
            revision=MAX_RECORD_REVISION + 1,
            content="x",
        )

    provider = _provider([_record(1)])
    before = provider.current_records()

    with pytest.raises(ProviderMalformedRecord):
        provider.replace_records([_record(1), _record(1, revision=1)])
    assert provider.current_records() == before

    smuggled = ProviderRecord.model_construct(record_id="", revision=0, content="x")
    with pytest.raises(ProviderMalformedRecord):
        provider.replace_records([smuggled])
    assert provider.current_records() == before

    with pytest.raises(ProviderMalformedRecord):
        provider.upsert("bad id", "x")
    with pytest.raises(ProviderMalformedRecord):
        provider.upsert("rec-001", "x" * (MAX_RECORD_CONTENT_BYTES + 1))
    assert provider.current_records() == before

    updated = provider.upsert("rec-001", "first-change")
    assert updated.record_id == "rec-001"
    assert updated.revision == 1
    assert provider.upsert("rec-001", "first-change", revision=1) == updated
    with pytest.raises(ProviderMalformedRecord):
        provider.upsert("rec-001", "different-at-same-revision", revision=1)
    with pytest.raises(ProviderMalformedRecord):
        provider.upsert("rec-001", "stale", revision=0)
    with pytest.raises(ProviderMalformedRecord):
        provider.upsert("rec-001", "bool-revision", revision=cast("int", True))
    assert provider.record_count == 1


def test_fault_script_is_bounded_sorted_and_fails_closed_on_unknown_operation() -> None:
    script = ProviderFaultScript(
        [
            ProviderFaultRule(ordinal=3, fault=ProviderPoisonFault(message="third")),
            ProviderFaultRule(ordinal=1, fault=ProviderUnavailableFault(message="first")),
        ]
    )
    assert [rule.ordinal for rule in script.rules] == [1, 3]
    assert script.rule_for(2) is None
    assert script.rule_for(1) is not None

    with pytest.raises(ProviderContractError):
        ProviderFaultScript(
            [
                ProviderFaultRule(ordinal=1, fault=ProviderPoisonFault()),
                ProviderFaultRule(ordinal=1, fault=ProviderUnavailableFault()),
            ]
        )

    too_many = [
        ProviderFaultRule(ordinal=index + 1, fault=ProviderPoisonFault())
        for index in range(MAX_FAULT_RULES + 1)
    ]
    with pytest.raises(ProviderContractError):
        ProviderFaultScript(too_many)
    with pytest.raises(ProviderContractError):
        ProviderFaultScript(_over_bound_rules())

    unknown_operation = ProviderFaultRule.model_construct(
        operation="delete",
        ordinal=1,
        phase="before_page",
        fault=ProviderPoisonFault(),
    )
    with pytest.raises(ProviderContractError):
        ProviderFaultScript([unknown_operation])

    for invalid_delay in (-0.1, float("nan"), float("inf")):
        with pytest.raises(ValidationError):
            ProviderRateLimitedFault(delay_seconds=invalid_delay)
    with pytest.raises(ValidationError):
        ProviderRateLimitedFault(delay_seconds=cast("float", True))

    for invalid_delay in (-0.1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            ProviderRateLimited(invalid_delay)


@_sync_case
async def test_unavailable_fault_is_ordinary_retryable_and_counted() -> None:
    script = ProviderFaultScript(
        [ProviderFaultRule(ordinal=1, fault=ProviderUnavailableFault(message="try later"))]
    )
    provider = _provider(_records(3), fault_script=script)

    with pytest.raises(ProviderUnavailable) as caught:
        await provider.fetch(None, 2)
    assert caught.value.retryable is True
    assert str(caught.value) == "try later"
    stats = provider.stats
    assert stats.fetch_calls == 1
    assert stats.fault_gates_tripped == 1
    assert stats.pages_returned == 0

    page = await provider.fetch(None, 2)
    assert _ids(page.records) == ["rec-000", "rec-001"]
    assert provider.stats.fetch_calls == 2
    assert provider.stats.pages_returned == 1


@_sync_case
async def test_rate_limited_fault_carries_finite_nonnegative_delay() -> None:
    script = ProviderFaultScript(
        [
            ProviderFaultRule(
                ordinal=1,
                fault=ProviderRateLimitedFault(delay_seconds=1.25, message="slow down"),
            )
        ]
    )
    provider = _provider(_records(2), fault_script=script)

    with pytest.raises(ProviderRateLimited) as caught:
        await provider.fetch(None, 1)
    assert caught.value.retryable is True
    assert caught.value.delay_seconds == 1.25
    assert caught.value.retry_after_seconds == 1.25
    assert math.isfinite(caught.value.delay_seconds)
    assert provider.stats.fault_gates_tripped == 1

    page = await provider.fetch(None, 1)
    assert _ids(page.records) == ["rec-000"]


@_sync_case
async def test_poison_fault_is_deterministic_and_nonretryable() -> None:
    script = ProviderFaultScript(
        [
            ProviderFaultRule(ordinal=1, fault=ProviderPoisonFault(message="bad record")),
            ProviderFaultRule(ordinal=2, fault=ProviderPoisonFault(message="bad record")),
        ]
    )
    provider = _provider(_records(2), fault_script=script)

    for expected_call in (1, 2):
        with pytest.raises(ProviderPoison) as caught:
            await provider.fetch(None, 1)
        assert caught.value.retryable is False
        assert str(caught.value) == "bad record"
        assert provider.stats.fetch_calls == expected_call
        assert provider.stats.fault_gates_tripped == expected_call

    page = await provider.fetch(None, 1)
    assert _ids(page.records) == ["rec-000"]


@_sync_case
async def test_after_page_fault_withholds_page_and_cursor_without_silent_return() -> None:
    script = ProviderFaultScript(
        [
            ProviderFaultRule(
                ordinal=1,
                phase="after_page",
                fault=ProviderUnavailableFault(message="response lost"),
            )
        ]
    )
    records = _records(4)
    provider = _provider(records, max_snapshots=4, fault_script=script)
    expected_bytes = sum(record.payload_bytes for record in records[:2])

    with pytest.raises(ProviderUnavailable):
        await provider.fetch(None, 2)

    stats = provider.stats
    assert stats.fetch_calls == 1
    assert stats.fault_gates_tripped == 1
    assert stats.withheld_pages == 1
    assert stats.withheld_payload_bytes == expected_bytes
    assert stats.pages_returned == 0
    assert stats.records_returned == 0
    assert stats.payload_bytes == 0
    assert stats.cursors_issued == 0
    assert stats.snapshots_created == 0

    page = await provider.fetch(None, 2)
    assert _ids(page.records) == ["rec-000", "rec-001"]
    assert page.payload_bytes == expected_bytes
    assert page.has_more is True
    cursor = page.next_cursor
    assert cursor is not None

    second = await provider.fetch(cursor, 2)
    assert _ids(second.records) == ["rec-002", "rec-003"]
    assert second.has_more is False
    assert provider.stats.pages_returned == 2
    assert provider.stats.records_returned == 4
    assert provider.stats.payload_bytes == sum(record.payload_bytes for record in records)
    assert provider.stats.cursors_issued == 1


@_sync_case
async def test_fault_ordinal_counts_malformed_calls_and_never_masks_them() -> None:
    script = ProviderFaultScript(
        [ProviderFaultRule(ordinal=2, fault=ProviderPoisonFault(message="ordinal two"))]
    )
    provider = _provider(_records(2), fault_script=script)

    with pytest.raises(ProviderPageLimitError):
        await provider.fetch(None, 0)
    with pytest.raises(ProviderPoison):
        await provider.fetch(None, 1)
    page = await provider.fetch(None, 1)

    assert _ids(page.records) == ["rec-000"]
    assert provider.stats.fetch_calls == 3
    assert provider.stats.fault_gates_tripped == 1


@_sync_case
async def test_concurrent_fetches_replay_safely_and_count_exactly() -> None:
    provider = _provider(_records(20), max_cursors=64)

    first = await provider.fetch(None, 4)
    cursor = first.next_cursor
    assert cursor is not None

    pages = await asyncio.gather(*(provider.fetch(cursor, 4) for _ in range(25)))

    for page in pages:
        assert _ids(page.records) == ["rec-004", "rec-005", "rec-006", "rec-007"]
        assert page.has_more is True
    assert len({str(page.next_cursor) for page in pages}) == 1

    stats = provider.stats
    assert stats.fetch_calls == 26
    assert stats.pages_returned == 26
    assert stats.records_returned == 4 + 25 * 4
    assert stats.cursors_issued == 2
    assert stats.snapshots_created == 1


def test_provider_is_safe_across_threads_and_event_loops() -> None:
    provider = _provider(_records(12), max_snapshots=32, max_cursors=64)
    first = asyncio.run(provider.fetch(None, 3))
    cursor = first.next_cursor
    assert cursor is not None

    def fetch_page(_: int) -> tuple[tuple[str, ...], str | None]:
        page = asyncio.run(provider.fetch(cursor, 3))
        next_cursor = page.next_cursor
        return (
            tuple(_ids(page.records)),
            None if next_cursor is None else str(next_cursor),
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(fetch_page, range(24)))

    assert all(ids == ("rec-003", "rec-004", "rec-005") for ids, _ in results)
    assert len({token for _, token in results}) == 1
    assert provider.stats.fetch_calls == 25
    assert provider.stats.pages_returned == 25
    assert provider.stats.snapshots_created == 1


def test_provider_spawns_no_tasks_and_has_no_close_contract() -> None:
    provider = _provider(_records(4))

    async def use_once() -> int:
        before = asyncio.all_tasks()
        page = await provider.fetch(None, 2)
        assert len(page.records) == 2
        after = asyncio.all_tasks()
        assert after == before
        return len(after)

    assert asyncio.run(use_once()) == 1
    assert asyncio.run(use_once()) == 1
    assert provider.stats.pages_returned == 2
    assert not hasattr(provider, "close")
