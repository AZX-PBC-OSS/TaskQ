"""Unit tests for taskq.types."""

import pytest
from pydantic import ValidationError

from taskq._ids import new_uuid
from taskq._json import dumps, loads
from taskq.types import CancelResult, StateChangeEvent

# ── CancelResult construction and frozen enforcement ────────────


def test_cancel_result_construction() -> None:
    """CancelResult accepts all four fields."""
    job_id = new_uuid()
    result = CancelResult(
        job_id=job_id,
        previous_status="running",
        new_status="cancelled",
        cancellation_initiated=True,
    )
    assert result.job_id == job_id
    assert result.previous_status == "running"
    assert result.new_status == "cancelled"
    assert result.cancellation_initiated is True


def test_cancel_result_rejects_bad_status() -> None:
    """CancelResult raises ValidationError on an invalid previous_status."""
    with pytest.raises(ValidationError, match="previous_status"):
        CancelResult(
            job_id=new_uuid(),
            previous_status="bogus",  # type: ignore[arg-type] # Why: intentional invalid literal to test ValidationError path
            new_status="cancelled",
            cancellation_initiated=True,
        )


def test_cancel_result_frozen() -> None:
    """CancelResult is frozen — assigning to job_id after construction raises."""
    result = CancelResult(
        job_id=new_uuid(),
        previous_status="running",
        new_status="cancelled",
        cancellation_initiated=True,
    )
    with pytest.raises(ValidationError):
        result.job_id = new_uuid()  # type: ignore[misc] # Why: intentional frozen-model mutation to test enforcement


# ── StateChangeEvent serialization through _json ────────────────


def test_state_change_event_roundtrip() -> None:
    """StateChangeEvent round-trips through taskq._json.dumps/loads."""
    worker_id = new_uuid()
    event = StateChangeEvent(
        from_state="running",
        to_state="failed",
        error_class="ValueError",
        worker_id=worker_id,
    )
    serialized = dumps(event)
    deserialized = loads(serialized)
    assert deserialized["from_state"] == "running"
    assert deserialized["to_state"] == "failed"
    assert deserialized["error_class"] == "ValueError"
    assert str(deserialized["worker_id"]) == str(worker_id)


def test_state_change_event_null_fields() -> None:
    """StateChangeEvent with None fields serializes error_class and worker_id as null."""
    event = StateChangeEvent(
        from_state="pending",
        to_state="running",
    )
    serialized = dumps(event)
    deserialized = loads(serialized)
    assert deserialized["from_state"] == "pending"
    assert deserialized["to_state"] == "running"
    assert deserialized["error_class"] is None
    assert deserialized["worker_id"] is None


def test_state_change_event_worker_id_serializes_as_string() -> None:
    """StateChangeEvent worker_id (UUID) round-trips through _json.loads."""
    worker_id = new_uuid()
    event = StateChangeEvent(
        from_state="pending",
        to_state="scheduled",
        worker_id=worker_id,
    )
    serialized = dumps(event)
    deserialized = loads(serialized)
    assert str(deserialized["worker_id"]) == str(worker_id)
