"""Unit tests for BatchAbortedError and EmptyBatchError (no PG required)."""

from uuid import UUID

import pytest

from taskq.exceptions import (
    BatchAbortedError,
    EmptyBatchError,
    TaskQError,
)

_BATCH_ID = UUID("12345678-1234-5678-1234-567812345678")


# -- BatchAbortedError hierarchy ----------------------------------------------


def test_batch_aborted_is_subclass_of_taskq_error() -> None:
    assert issubclass(BatchAbortedError, TaskQError)


def test_batch_aborted_caught_as_taskq_error() -> None:
    with pytest.raises(TaskQError):
        raise BatchAbortedError(_BATCH_ID, consecutive_failures=5, threshold=3)


# -- BatchAbortedError attribute storage --------------------------------------


def test_batch_aborted_stores_all_attributes() -> None:
    exc = BatchAbortedError(_BATCH_ID, consecutive_failures=7, threshold=5)
    assert exc.batch_id == _BATCH_ID
    assert exc.consecutive_failures == 7
    assert exc.threshold == 5


# -- BatchAbortedError message ------------------------------------------------


def test_batch_aborted_message_contains_details() -> None:
    exc = BatchAbortedError(_BATCH_ID, consecutive_failures=7, threshold=5)
    msg = str(exc)
    assert str(_BATCH_ID) in msg
    assert "7" in msg
    assert "5" in msg
    assert "aborted" in msg
    assert "consecutive failures" in msg
    assert "threshold=5" in msg


# -- EmptyBatchError hierarchy ------------------------------------------------


def test_empty_batch_is_subclass_of_taskq_error() -> None:
    assert issubclass(EmptyBatchError, TaskQError)


def test_empty_batch_caught_as_taskq_error() -> None:
    with pytest.raises(TaskQError):
        raise EmptyBatchError(_BATCH_ID, expected=1, actual=0)


# -- EmptyBatchError attribute storage ----------------------------------------


def test_empty_batch_stores_all_attributes() -> None:
    exc = EmptyBatchError(_BATCH_ID, expected=1, actual=0)
    assert exc.batch_id == _BATCH_ID
    assert exc.expected == 1
    assert exc.actual == 0


# -- EmptyBatchError message --------------------------------------------------


def test_empty_batch_message_contains_details() -> None:
    exc = EmptyBatchError(_BATCH_ID, expected=1, actual=0)
    msg = str(exc)
    assert str(_BATCH_ID) in msg
    assert "0" in msg
    assert "1" in msg
    assert "expected at least 1" in msg


# -- public import surface ----------------------------------------------------


def test_batch_exceptions_importable_from_taskq() -> None:
    import taskq

    assert taskq.BatchAbortedError is BatchAbortedError
    assert taskq.EmptyBatchError is EmptyBatchError
