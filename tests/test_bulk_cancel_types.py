import pytest
from pydantic import ValidationError

from taskq._ids import new_uuid
from taskq.exceptions import EmptyFilterError, TaskQError
from taskq.types import BulkCancelResult


class TestBulkCancelResult:
    def test_construction(self) -> None:
        ids = [new_uuid() for _ in range(3)]
        result = BulkCancelResult(
            cancelled_directly=2,
            cancel_requested=1,
            cancelled_ids=ids[:2],
            cancel_requested_ids=ids[2:],
        )
        assert result.cancelled_directly == 2
        assert result.cancel_requested == 1
        assert result.total_affected == 3
        assert len(result.cancelled_ids) == 2
        assert len(result.cancel_requested_ids) == 1

    def test_frozen(self) -> None:
        result = BulkCancelResult(
            cancelled_directly=0,
            cancel_requested=0,
            cancelled_ids=[],
            cancel_requested_ids=[],
        )
        with pytest.raises(ValidationError):
            result.cancelled_directly = 1  # type: ignore[misc]

    def test_zero_counts(self) -> None:
        result = BulkCancelResult(
            cancelled_directly=0,
            cancel_requested=0,
            cancelled_ids=[],
            cancel_requested_ids=[],
        )
        assert result.total_affected == 0

    def test_ids_are_tuples(self) -> None:
        """ID fields are tuples, not lists — frozen immutability."""
        result = BulkCancelResult(
            cancelled_directly=1,
            cancel_requested=0,
            cancelled_ids=[new_uuid()],
            cancel_requested_ids=[],
        )
        assert isinstance(result.cancelled_ids, tuple)
        assert isinstance(result.cancel_requested_ids, tuple)

    def test_list_input_coerced_to_tuple(self) -> None:
        """Pydantic v2 coerces list inputs to tuples."""
        ids = [new_uuid() for _ in range(2)]
        result = BulkCancelResult(
            cancelled_directly=2,
            cancel_requested=0,
            cancelled_ids=ids,
            cancel_requested_ids=[],
        )
        assert isinstance(result.cancelled_ids, tuple)
        assert len(result.cancelled_ids) == 2


class TestEmptyFilterError:
    def test_is_taskq_error(self) -> None:
        assert issubclass(EmptyFilterError, TaskQError)

    def test_message_mentions_guardrail(self) -> None:
        err = EmptyFilterError()
        assert "allow_empty_filter" in str(err)
        assert "filter predicate" in str(err)
