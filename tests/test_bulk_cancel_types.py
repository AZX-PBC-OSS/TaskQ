from uuid import uuid4

import pytest
from pydantic import ValidationError

from taskq.exceptions import EmptyFilterError, TaskQError
from taskq.types import BulkCancelResult


class TestBulkCancelResult:
    def test_construction(self) -> None:
        ids = [uuid4() for _ in range(3)]
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


class TestEmptyFilterError:
    def test_is_taskq_error(self) -> None:
        assert issubclass(EmptyFilterError, TaskQError)

    def test_message_mentions_guardrail(self) -> None:
        err = EmptyFilterError()
        assert "allow_empty_filter" in str(err)
        assert "filter predicate" in str(err)
