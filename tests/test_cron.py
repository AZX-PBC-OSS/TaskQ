"""Tests for :mod:`taskq.cron` and :mod:`taskq.scheduler`.

compute_next_fire_after with UTC.
compute_next_fire_after at minute boundary.
_resolve_factory succeeds and caches.
_resolve_factory failure — ModuleNotFoundError.
_resolve_factory failure — AttributeError.
cron() with both payload_factory and static_payload raises ValueError.
property test — compute_next_fire_after always returns a datetime
       strictly after the `after` argument for valid expressions.
"""

from collections.abc import Iterator
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from croniter import croniter
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from pydantic import BaseModel, ValidationError

from taskq.backend._protocol import ScheduleRecord
from taskq.cron import (
    CronScheduleSpec,
    ScheduleHandle,
    _check_gap,
    _factory_cache,
    _fold_to_utc,
    _is_ambiguous_time,
    _resolve_factory,
    compute_next_fire_after,
    cron,
    resolve_payload,
)
from taskq.scheduler import _CRON_REGISTRY, get_registered_crons, register_cron


def _sample_factory() -> dict[str, int]:
    return {"x": 1}


class _SamplePayload(BaseModel):
    name: str
    count: int


def _sample_model_factory() -> _SamplePayload:
    return _SamplePayload(name="test", count=3)


async def _async_factory() -> "dict[str, str]":
    return {"async": "yes"}


def _bad_factory() -> int:
    return 42


# ── Autouse fixture: restore _CRON_REGISTRY and _factory_cache ──────


@pytest.fixture(autouse=True)
def _restore_module_globals(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Snapshot and restore _CRON_REGISTRY and _factory_cache.

    Tests that use ``register_cron`` or ``_resolve_factory`` mutate
    module-level state; this fixture ensures every test starts clean.
    File-scope autouse is justified because the majority of tests in
    this file touch the registry or cache.
    """
    original_registry = list(_CRON_REGISTRY)
    original_cache = dict(_factory_cache)
    try:
        yield
    finally:
        _CRON_REGISTRY.clear()
        _CRON_REGISTRY.extend(original_registry)
        _factory_cache.clear()
        _factory_cache.update(original_cache)


# ── compute_next_fire_after with UTC ────────────────────────


def testcompute_next_fire_after_utc() -> None:
    """'0 3 * * *' at 10:00 UTC → next 03:00 UTC."""
    after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("0 3 * * *", "UTC", after)
    assert result == [datetime(2025, 1, 2, 3, 0, 0, tzinfo=UTC)]


# ── compute_next_fire_after at minute boundary ───────────────


def testcompute_next_fire_after_minute_boundary() -> None:
    """'*/5 * * * *' at 10:00 UTC → 10:05 UTC."""
    after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("*/5 * * * *", "UTC", after)
    assert result == [datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)]


# ── compute_next_fire_after with a 6-field (seconds) expression ──────


def testcompute_next_fire_after_seconds_precision() -> None:
    """'*/5 * * * * *' (6-field, seconds precision) at 10:00:00 UTC → 10:00:01 UTC."""
    after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("*/5 * * * * *", "UTC", after)
    assert result == [datetime(2025, 1, 1, 10, 0, 1, tzinfo=ZoneInfo("UTC"))]


# ── compute_next_fire_after: candidate.tzinfo is None branch ─────────


def testcompute_next_fire_after_naive_candidate_gets_localized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When croniter.get_next returns a naive datetime, compute_next_fire_after
    localizes it to the schedule's timezone (line: `if candidate.tzinfo is None`).

    Real croniter versions always return a tz-aware candidate when given a
    tz-aware `after_local`; this test forces the naive branch by monkeypatching
    croniter.get_next, since it is otherwise unreachable through real croniter
    behavior with the installed version.
    """

    def fake_get_next(
        self: croniter, ret_type: object = None, *args: object, **kwargs: object
    ) -> datetime:
        return datetime(2025, 6, 15, 10, 30, 0)

    monkeypatch.setattr(croniter, "get_next", fake_get_next)
    after = datetime(2025, 6, 15, 8, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after(
        "30 10 * * *", "America/New_York", after, dst_strategy="firstof"
    )
    tz = ZoneInfo("America/New_York")
    assert result == [datetime(2025, 6, 15, 10, 30, 0, tzinfo=tz)]


# ── compute_next_fire_after: DST gap (spring-forward) ────────────────


def testcompute_next_fire_after_gap_advances_to_next_valid_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On 2025-03-09 America/New_York, 02:00→03:00 (02:30 does not exist).
    Forcing croniter to return the nonexistent 02:30 candidate exercises
    the gap-detection and _check_gap advancement; the result is the next
    valid cron match (2025-03-10 02:30), not the same-day gap time.

    Real croniter already resolves this internally for the installed
    version (it never returns 02:30 for '30 2 * * *' on this date), so the
    gap branch is forced via monkeypatching the first get_next call only;
    the second internal call (cr2.get_next inside compute_next_fire_after)
    falls through to the real implementation.
    """
    tz = ZoneInfo("America/New_York")
    calls = {"n": 0}
    original_get_next = cast(Any, croniter.get_next)

    def fake_get_next(
        self: croniter, ret_type: object = None, *args: object, **kwargs: object
    ) -> datetime:
        calls["n"] += 1
        if calls["n"] == 1:
            return datetime(2025, 3, 9, 2, 30, 0, tzinfo=tz)
        return cast(datetime, original_get_next(self, ret_type, *args, **kwargs))

    monkeypatch.setattr(croniter, "get_next", fake_get_next)
    after = datetime(2025, 3, 8, 20, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("30 2 * * *", "America/New_York", after, dst_strategy="skip")
    assert result == [datetime(2025, 3, 10, 2, 30, 0, tzinfo=tz)]


def testcompute_next_fire_after_gap_naive_next_valid_gets_localized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the second (post-gap) croniter.get_next call also returns a naive
    datetime, compute_next_fire_after localizes it too before delegating to
    _check_gap (line: `if next_valid.tzinfo is None`)."""
    tz = ZoneInfo("America/New_York")
    calls = {"n": 0}

    def fake_get_next(
        self: croniter, ret_type: object = None, *args: object, **kwargs: object
    ) -> datetime:
        calls["n"] += 1
        if calls["n"] == 1:
            return datetime(2025, 3, 9, 2, 30, 0)
        if calls["n"] == 2:
            return datetime(2025, 3, 10, 2, 30, 0)
        raise AssertionError("unexpected third croniter.get_next call")

    monkeypatch.setattr(croniter, "get_next", fake_get_next)
    after = datetime(2025, 3, 8, 20, 0, 0, tzinfo=UTC)
    result = compute_next_fire_after("30 2 * * *", "America/New_York", after, dst_strategy="skip")
    assert result == [datetime(2025, 3, 10, 2, 30, 0, tzinfo=tz)]


# ── compute_next_fire_after: DST overlap (fall-back) ──────────────────


def testcompute_next_fire_after_overlap_allof_returns_both_occurrences() -> None:
    """On 2025-11-02 America/New_York, 02:00→01:00 (01:30 occurs twice).
    dst_strategy='allof' returns both UTC instants, 1 hour apart."""
    tz = ZoneInfo("America/New_York")
    after = datetime(2025, 11, 1, 20, 0, 0, tzinfo=tz).astimezone(UTC)
    result = compute_next_fire_after("30 1 * * *", "America/New_York", after, dst_strategy="allof")
    assert len(result) == 2
    assert result[0].astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)
    assert result[1].astimezone(UTC) == datetime(2025, 11, 2, 6, 30, 0, tzinfo=UTC)


@pytest.mark.parametrize("dst_strategy", ["skip", "firstof"])
def testcompute_next_fire_after_overlap_skip_and_firstof_use_earlier_occurrence(
    dst_strategy: str,
) -> None:
    """For the same DST overlap, 'skip' and 'firstof' both return only the
    earlier (fold=0) occurrence."""
    tz = ZoneInfo("America/New_York")
    after = datetime(2025, 11, 1, 20, 0, 0, tzinfo=tz).astimezone(UTC)
    result = compute_next_fire_after(
        "30 1 * * *",
        "America/New_York",
        after,
        dst_strategy=cast(Any, dst_strategy),
    )
    assert len(result) == 1
    assert result[0].astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)


# ── _is_ambiguous_time converts when tzinfo is not the target tz object ──


def test_is_ambiguous_time_converts_dt_with_different_tzinfo() -> None:
    """_is_ambiguous_time converts *dt* to *tz* first when dt.tzinfo is not
    the same object as *tz* (exercises the `dt = dt.astimezone(tz)` branch)."""
    tz = ZoneInfo("America/New_York")
    ambiguous_instant = datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)
    assert _is_ambiguous_time(ambiguous_instant, tz) is True

    unambiguous_instant = datetime(2025, 6, 15, 14, 30, 0, tzinfo=UTC)
    assert _is_ambiguous_time(unambiguous_instant, tz) is False


# ── _fold_to_utc resolves ambiguous local time by fold value ──────────


def test_fold_to_utc_resolves_by_fold_value() -> None:
    """_fold_to_utc(dt, tz, fold=0) and fold=1 resolve to UTC instants
    1 hour apart for an ambiguous local time."""
    tz = ZoneInfo("America/New_York")
    ambiguous_local = datetime(2025, 11, 2, 1, 30, 0, tzinfo=tz)
    earlier = _fold_to_utc(ambiguous_local, tz, fold=0)
    later = _fold_to_utc(ambiguous_local, tz, fold=1)
    assert earlier.astimezone(UTC) == datetime(2025, 11, 2, 5, 30, 0, tzinfo=UTC)
    assert later.astimezone(UTC) == datetime(2025, 11, 2, 6, 30, 0, tzinfo=UTC)


# ── _check_gap advances past a DST gap, localizes naive input ─────────


def test_check_gap_advances_naive_gap_datetime_to_valid_time() -> None:
    """_check_gap localizes a naive datetime and advances minute-by-minute
    out of a DST gap until the wall-clock time round-trips through UTC."""
    tz = ZoneInfo("America/New_York")
    gap_dt = datetime(2025, 3, 9, 2, 15, 0)
    result = _check_gap(gap_dt, tz)
    assert result == datetime(2025, 3, 9, 3, 0, 0, tzinfo=tz)


def test_check_gap_returns_unchanged_when_already_valid() -> None:
    """_check_gap returns *dt* unchanged when it is not in a DST gap."""
    tz = ZoneInfo("America/New_York")
    valid_dt = datetime(2025, 6, 15, 10, 0, 0)
    result = _check_gap(valid_dt, tz)
    assert result == datetime(2025, 6, 15, 10, 0, 0, tzinfo=tz)


# ── cron() with valid expression returns CronScheduleSpec ────────────


def test_cron_valid_expression() -> None:
    """cron() with valid expression returns CronScheduleSpec with correct fields."""
    spec = cron("*/5 * * * *", "my_actor", timezone="UTC", enabled=True)
    assert isinstance(spec, CronScheduleSpec)
    assert spec.actor == "my_actor"
    assert spec.cron_expr == "*/5 * * * *"
    assert spec.timezone == "UTC"
    assert spec.enabled is True
    assert spec.payload_factory is None
    assert spec.static_payload is None
    assert spec.name == ""


# ── CronScheduleSpec name defaults to "" and identity_key to None ────


def test_cron_schedule_spec_name_defaults_empty_string() -> None:
    """CronScheduleSpec.name defaults to '' so existing (actor-only)
    schedules map to the (actor, '') uniqueness key after migration."""
    spec = CronScheduleSpec(actor="a", cron_expr="0 * * * *")
    assert spec.name == ""


def test_cron_schedule_spec_identity_key_defaults_none() -> None:
    """CronScheduleSpec.identity_key defaults to None."""
    spec = CronScheduleSpec(actor="a", cron_expr="0 * * * *")
    assert spec.identity_key is None


def test_cron_accepts_name_and_identity_key() -> None:
    """cron() accepts name and identity_key and forwards them to CronScheduleSpec."""
    from taskq.backend._protocol import IdentityKey

    spec = cron(
        "0 * * * *",
        "per_property_actor",
        name="prop-123",
        identity_key=IdentityKey("sync:entity:123"),
    )
    assert spec.name == "prop-123"
    assert spec.identity_key is not None
    assert str(spec.identity_key) == "sync:entity:123"


# ── cron() with invalid expression raises ValueError ─────────────────


def test_cron_invalid_expression() -> None:
    """cron() with invalid expression raises ValueError."""
    with pytest.raises(ValueError, match="Invalid cron expression"):
        cron("not valid", "my_actor")


# ── cron() with both payload_factory and static_payload raises ValueError ──


def test_cron_mutually_exclusive_fields() -> None:
    """cron() with both payload_factory and static_payload raises ValueError."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        cron(
            "*/5 * * * *",
            "my_actor",
            payload_factory="some.module.fn",
            static_payload={"key": "value"},
        )


# ── cron() auto-registers via register_cron ─────────────────────────


def test_cron_auto_registers() -> None:
    """cron() automatically calls register_cron."""
    spec = cron("0 * * * *", "auto_actor")
    registered = get_registered_crons()
    assert spec in registered


# ── register_cron adds to registry; get_registered_crons returns it ──


def test_register_cron_adds_to_registry() -> None:
    """register_cron() adds to registry; get_registered_crons() returns it."""
    spec = CronScheduleSpec(
        actor="test_actor",
        cron_expr="0 * * * *",
    )
    register_cron(spec)
    registered = get_registered_crons()
    assert spec in registered


def test_register_cron_duplicate_adds_again() -> None:
    """Duplicate register_cron calls add again (registry is a list)."""
    spec = CronScheduleSpec(
        actor="dup_actor",
        cron_expr="0 * * * *",
    )
    register_cron(spec)
    register_cron(spec)
    registered = get_registered_crons()
    assert registered.count(spec) == 2


def test_register_cron_invalid_expression_raises() -> None:
    """register_cron() with invalid expression raises ValueError."""
    spec = CronScheduleSpec(actor="bad", cron_expr="not valid")
    with pytest.raises(ValueError, match="Invalid cron expression"):
        register_cron(spec)


def test_register_cron_mutually_exclusive_raises() -> None:
    """register_cron() with both payload_factory and static_payload raises ValueError."""
    spec = CronScheduleSpec(
        actor="bad",
        cron_expr="0 * * * *",
        payload_factory="a.b",
        static_payload={"k": 1},
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        register_cron(spec)


# ── get_registered_crons returns snapshot copy ──────────────────────


def test_get_registered_crons_returns_copy() -> None:
    """get_registered_crons() returns a snapshot; mutating it does not affect registry."""
    spec = CronScheduleSpec(actor="snap_actor", cron_expr="0 * * * *")
    register_cron(spec)
    snapshot = get_registered_crons()
    snapshot.clear()
    assert len(get_registered_crons()) >= 1


# ── _resolve_factory succeeds and caches ─────────────────────


def test_resolve_factory_succeeds_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """_resolve_factory resolves a dotted path, returns callable, caches."""
    dotted = f"{_sample_factory.__module__}.{_sample_factory.__qualname__}"
    factory = _resolve_factory(dotted)
    assert factory is _sample_factory
    assert dotted in _factory_cache
    assert _factory_cache[dotted] is _sample_factory
    cached = _resolve_factory(dotted)
    assert cached is _sample_factory


# ── _resolve_factory failure — ModuleNotFoundError ────────────


def test_resolve_factory_module_not_found() -> None:
    """_resolve_factory raises ModuleNotFoundError for nonexistent module."""
    with pytest.raises(ModuleNotFoundError):
        _resolve_factory("nonexistent.module.fn")


# ── _resolve_factory failure — AttributeError ─────────────────


def test_resolve_factory_attribute_error() -> None:
    """_resolve_factory raises AttributeError for missing attribute."""
    with pytest.raises(AttributeError):
        _resolve_factory(f"{_sample_factory.__module__}.nonexistent_attr")


# ── _resolve_factory invalid dotted path ─────────────────────────────


def test_resolve_factory_invalid_dotted_path() -> None:
    """_resolve_factory raises ImportError for a path without a dot."""
    with pytest.raises(ImportError, match="Invalid dotted path"):
        _resolve_factory("no_dots_here")


# ── ScheduleRecord roundtrips through model_validate ─────────────────


def test_schedule_record_roundtrip() -> None:
    """ScheduleRecord roundtrips through model_validate correctly."""
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
    from uuid import uuid4

    uid = uuid4()
    data = {
        "id": uid,
        "actor": "my_actor",
        "cron_expr": "0 3 * * *",
        "timezone": "UTC",
        "payload_factory": None,
        "enabled": True,
        "last_fired_at": None,
        "last_fire_error": None,
        "consecutive_failures": 0,
        "next_fire_at": now,
        "metadata": {},
    }
    record = ScheduleRecord.model_validate(data)
    assert record.id == uid
    assert record.actor == "my_actor"
    assert record.consecutive_failures == 0
    assert record.next_fire_at == now
    assert record.enabled is True
    roundtripped = ScheduleRecord.model_validate(record.model_dump())
    assert roundtripped == record


# ── ScheduleRecord frozen config ─────────────────────────────────────


def test_schedule_record_frozen() -> None:
    """ScheduleRecord is frozen (ConfigDict(frozen=True))."""
    from uuid import uuid4

    record = ScheduleRecord(
        id=uuid4(),
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        payload_factory=None,
        enabled=True,
        last_fired_at=None,
        last_fire_error=None,
        consecutive_failures=0,
        next_fire_at=datetime.now(tz=UTC),
        metadata={},
    )
    with pytest.raises(ValidationError):
        record.actor = "changed"  # type: ignore[misc] # Why: deliberate mutation to verify frozen Pydantic model guard.


# ── CronScheduleSpec frozen ──────────────────────────────────────────


def test_cron_schedule_spec_frozen() -> None:
    """CronScheduleSpec is a frozen dataclass."""
    spec = CronScheduleSpec(actor="a", cron_expr="0 * * * *")
    with pytest.raises(FrozenInstanceError):
        spec.actor = "changed"  # type: ignore[misc] # Why: deliberate mutation to verify frozen dataclass guard.


# ── ScheduleHandle methods ───────────────────────────────────────────


class _StubBackend:
    """Minimal stub for ScheduleHandle method tests.

    Matches the Backend protocol signature: ``update_schedule(schedule_id, args)``.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, UUID, object]] = []

    async def update_schedule(
        self,
        schedule_id: UUID,
        args: object,
    ) -> None:
        self.calls.append(("update", schedule_id, args))

    async def delete_schedule(self, schedule_id: UUID) -> None:
        self.calls.append(("delete", schedule_id, None))


@pytest.mark.asyncio
async def test_schedule_handle_disable() -> None:
    """ScheduleHandle.disable() delegates to backend.update_schedule with ScheduleUpdateArgs(enabled=False)."""
    from uuid import uuid4

    from taskq.backend._protocol import ScheduleUpdateArgs

    stub = _StubBackend()
    uid = uuid4()
    handle = ScheduleHandle(
        schedule_id=uid,
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=True,
        next_fire_at=datetime.now(tz=UTC),
        _backend=stub,
    )
    await handle.disable()
    assert len(stub.calls) == 1
    method, sid, args = stub.calls[0]
    assert method == "update"
    assert sid == uid
    assert isinstance(args, ScheduleUpdateArgs)
    assert args.enabled is False


@pytest.mark.asyncio
async def test_schedule_handle_enable() -> None:
    """ScheduleHandle.enable() delegates to backend.update_schedule with ScheduleUpdateArgs(enabled=True).

    The backend resets consecutive_failures and last_fire_error when enabled=True;
    the handle does not need to pass those fields explicitly.
    """
    from uuid import uuid4

    from taskq.backend._protocol import ScheduleUpdateArgs

    stub = _StubBackend()
    uid = uuid4()
    handle = ScheduleHandle(
        schedule_id=uid,
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=False,
        next_fire_at=datetime.now(tz=UTC),
        _backend=stub,
    )
    await handle.enable()
    assert len(stub.calls) == 1
    method, sid, args = stub.calls[0]
    assert method == "update"
    assert sid == uid
    assert isinstance(args, ScheduleUpdateArgs)
    assert args.enabled is True


@pytest.mark.asyncio
async def test_schedule_handle_delete() -> None:
    """ScheduleHandle.delete() delegates to backend.delete_schedule with schedule_id."""
    from uuid import uuid4

    stub = _StubBackend()
    uid = uuid4()
    handle = ScheduleHandle(
        schedule_id=uid,
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=True,
        next_fire_at=datetime.now(tz=UTC),
        _backend=stub,
    )
    await handle.delete()
    assert stub.calls == [("delete", uid, None)]


# ── ScheduleHandle frozen ────────────────────────────────────────────


def test_schedule_handle_frozen() -> None:
    """ScheduleHandle is frozen (frozen=True, slots=True)."""
    from uuid import uuid4

    handle = ScheduleHandle(
        schedule_id=uuid4(),
        actor="a",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=True,
        next_fire_at=datetime.now(tz=UTC),
        _backend=_StubBackend(),
    )
    with pytest.raises(FrozenInstanceError):
        handle.enabled = False  # type: ignore[misc] # Why: deliberate mutation to verify frozen dataclass guard.


# ── compute_next_fire_after always returns datetime after `after` ──


_KNOWN_EXPRESSIONS = st.sampled_from(
    [
        "*/5 * * * *",
        "0 3 * * *",
        "0 12 * * 1-5",
        "30 8 1 * *",
        "*/10 */2 * * *",
        "0 0 * * *",
        "15 14 * * 1",
        "0 22 * * 1-5",
        "* * * * *",
        "0 */6 * * *",
    ]
)

_REGEX_EXPRESSIONS = st.from_regex(r"[1-5][0-9] [0-9] \* \* \*", fullmatch=True)

_CRON_STRATEGY = _KNOWN_EXPRESSIONS | _REGEX_EXPRESSIONS


@given(expr=_CRON_STRATEGY)
@settings(max_examples=50, deadline=None)
def testcompute_next_fire_after_always_after_now(expr: str) -> None:
    """compute_next_fire_after returns datetimes strictly after `after`
    for all valid 5-field cron expressions, without raising."""
    assume(croniter.is_valid(expr))
    after = datetime.now(UTC)
    result = compute_next_fire_after(expr, "UTC", after)
    assert all(r > after for r in result)


# ── resolve_payload: static payload from metadata ─────────────────────


@pytest.mark.asyncio
async def testresolve_payload_static_from_metadata() -> None:
    """resolve_payload returns static_payload from metadata when no factory."""
    result = await resolve_payload(None, {"static_payload": {"key": "val"}})
    assert result == {"key": "val"}


@pytest.mark.asyncio
async def testresolve_payload_static_from_json_string_metadata() -> None:
    """resolve_payload parses raw_metadata when it is a truthy non-dict (JSON string)."""
    result = await resolve_payload(None, '{"static_payload": {"key": "val"}}')
    assert result == {"key": "val"}


@pytest.mark.asyncio
async def testresolve_payload_empty_dict_without_factory_or_static() -> None:
    """resolve_payload returns {} when no factory and no static_payload."""
    result = await resolve_payload(None, {})
    assert result == {}


@pytest.mark.asyncio
async def testresolve_payload_none_metadata() -> None:
    """resolve_payload returns {} when metadata is None."""
    result = await resolve_payload(None, None)
    assert result == {}


@pytest.mark.asyncio
async def testresolve_payload_factory_returns_dict() -> None:
    """resolve_payload resolves a factory that returns a dict."""
    dotted = f"{_sample_factory.__module__}.{_sample_factory.__qualname__}"
    result = await resolve_payload(dotted, {})
    assert result == {"x": 1}


@pytest.mark.asyncio
async def testresolve_payload_factory_returns_basemodel() -> None:
    """resolve_payload resolves a factory that returns a BaseModel and converts via model_dump."""
    dotted = f"{_sample_model_factory.__module__}.{_sample_model_factory.__qualname__}"
    result = await resolve_payload(dotted, {})
    assert result == {"name": "test", "count": 3}


@pytest.mark.asyncio
async def testresolve_payload_factory_returns_async() -> None:
    """resolve_payload awaits async factories."""
    dotted = f"{_async_factory.__module__}.{_async_factory.__qualname__}"
    result = await resolve_payload(dotted, {})
    assert result == {"async": "yes"}


@pytest.mark.asyncio
async def testresolve_payload_factory_unexpected_type_raises_typeerror() -> None:
    """resolve_payload raises TypeError when factory returns unexpected type."""
    dotted = f"{_bad_factory.__module__}.{_bad_factory.__qualname__}"
    with pytest.raises(TypeError, match="cron factory"):
        await resolve_payload(dotted, {})


@pytest.mark.asyncio
async def testresolve_payload_factory_import_error_propagates() -> None:
    """resolve_payload propagates ImportError from _resolve_factory."""
    with pytest.raises(ImportError):
        await resolve_payload("nonexistent.module.fn", {})
