"""Unit tests for BatchRow, BatchCounts, and BatchFilter data carriers.

These carriers are defined at the protocol layer
(:mod:`taskq.backend._protocol`) so backends do not import the
client-side batch module.
"""

from dataclasses import MISSING, asdict, fields
from datetime import UTC, datetime

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import BatchCounts, BatchFilter, BatchRow

_NOW = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
_FIXED_UUID = new_uuid()

_BATCH_ROW_DEFAULTS: dict[str, object] = {
    "id": _FIXED_UUID,
    "queue": "default",
    "status": "active",
    "expected_size": 10,
    "consecutive_failures": 0,
    "failure_threshold": None,
    "finalizer_job_id": None,
    "originating_actor": None,
    "created_at": _NOW,
    "completed_at": None,
    "metadata": dict[str, object](),
}


def _make_batch_row(**overrides: object) -> BatchRow:
    merged = {**_BATCH_ROW_DEFAULTS, **overrides}
    return BatchRow(**merged)  # type: ignore[arg-type] # Why: override dict has broader value types than field annotations


class TestBatchRow:
    def test_construction_and_asdict(self) -> None:
        row = _make_batch_row()
        d = asdict(row)
        rebuilt = BatchRow(**d)  # type: ignore[arg-type] # Why: asdict produces dicts; runtime types are correct
        assert rebuilt == row

    def test_field_count(self) -> None:
        expected = 11
        assert len(fields(BatchRow)) == expected

    def test_frozen(self) -> None:
        row = _make_batch_row()
        with pytest.raises(AttributeError):
            row.queue = "other"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability

    def test_has_slots(self) -> None:
        assert "__slots__" in BatchRow.__dict__

    def test_all_statuses_round_trip(self) -> None:
        for status in ("active", "complete", "aborted"):
            row = _make_batch_row(status=status)  # type: ignore[arg-type] # Why: str status satisfies Literal at runtime
            assert row.status == status
            d = asdict(row)
            assert BatchRow(**d) == row  # type: ignore[arg-type] # Why: asdict produces dicts; runtime types are correct

    def test_optional_fields_accept_none(self) -> None:
        row = _make_batch_row(
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
            completed_at=None,
        )
        assert row.failure_threshold is None
        assert row.finalizer_job_id is None
        assert row.originating_actor is None
        assert row.completed_at is None


class TestBatchCounts:
    def test_construction_and_asdict(self) -> None:
        counts = BatchCounts(
            total=10,
            pending=3,
            succeeded=5,
            failed=1,
            cancelled=1,
            crashed=0,
            abandoned=0,
        )
        d = asdict(counts)
        rebuilt = BatchCounts(**d)
        assert rebuilt == counts

    def test_field_count(self) -> None:
        expected = 7
        assert len(fields(BatchCounts)) == expected

    def test_frozen(self) -> None:
        counts = BatchCounts(
            total=1,
            pending=0,
            succeeded=1,
            failed=0,
            cancelled=0,
            crashed=0,
            abandoned=0,
        )
        with pytest.raises(AttributeError):
            counts.total = 99  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability

    def test_has_slots(self) -> None:
        assert "__slots__" in BatchCounts.__dict__

    def test_field_names(self) -> None:
        names = {f.name for f in fields(BatchCounts)}
        assert names == {
            "total",
            "pending",
            "succeeded",
            "failed",
            "cancelled",
            "crashed",
            "abandoned",
        }


class TestBatchFilter:
    def test_default_construction(self) -> None:
        f = BatchFilter()
        assert f.queue is None
        assert f.active is None
        assert f.batch_id is None
        assert f.limit == 100

    def test_construction_with_values(self) -> None:
        f = BatchFilter(queue="default", active=True, batch_id=_FIXED_UUID, limit=50)
        assert f.queue == "default"
        assert f.active is True
        assert f.batch_id == _FIXED_UUID
        assert f.limit == 50

    def test_field_count(self) -> None:
        expected = 4
        assert len(fields(BatchFilter)) == expected

    def test_frozen(self) -> None:
        f = BatchFilter()
        with pytest.raises(AttributeError):
            f.queue = "other"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability

    def test_has_slots(self) -> None:
        assert "__slots__" in BatchFilter.__dict__

    def test_negative_limit_raises(self) -> None:
        with pytest.raises(ValueError, match="limit must be >= 0"):
            BatchFilter(limit=-1)

    def test_zero_limit_allowed(self) -> None:
        f = BatchFilter(limit=0)
        assert f.limit == 0

    def test_field_names_exactly(self) -> None:
        names = {f.name for f in fields(BatchFilter)}
        assert names == {"queue", "active", "batch_id", "limit"}

    def test_does_not_have_job_filter_fields(self) -> None:
        names = {f.name for f in fields(BatchFilter)}
        forbidden = {"status", "actor", "tags", "cursor", "order_by", "identity_key"}
        assert names.isdisjoint(forbidden), (
            f"BatchFilter must not carry JobFilter fields {forbidden}, "
            f"but found: {names & forbidden}"
        )

    def test_limit_default_is_missing_not_factory(self) -> None:
        flds = {f.name: f for f in fields(BatchFilter)}
        assert flds["limit"].default == 100
        assert flds["limit"].default_factory is MISSING
