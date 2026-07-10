"""Unit tests for taskq.testing.jobs helper builders."""

from __future__ import annotations

from taskq.backend._protocol import CancelPhase
from taskq.testing.jobs import error_info, make_enqueue_args, make_job_row


def test_make_job_row_cancel_phase_none_defaults_to_none_phase() -> None:
    row = make_job_row(cancel_phase=None)
    assert row.cancel_phase is CancelPhase.NONE


def test_make_job_row_cancel_phase_enum_instance_passthrough() -> None:
    row = make_job_row(cancel_phase=CancelPhase.FORCED)
    assert row.cancel_phase is CancelPhase.FORCED


def test_make_job_row_cancel_phase_int_coerced_to_enum() -> None:
    row = make_job_row(cancel_phase=2)
    assert row.cancel_phase is CancelPhase(2)
    assert isinstance(row.cancel_phase, CancelPhase)


def test_make_job_row_running_status_sets_locked_by_and_started_at() -> None:
    row = make_job_row(status="running")
    assert row.locked_by_worker is not None
    assert row.started_at is not None


def test_make_job_row_non_running_status_leaves_lock_and_start_unset() -> None:
    row = make_job_row(status="pending")
    assert row.locked_by_worker is None
    assert row.started_at is None


def test_make_enqueue_args_defaults() -> None:
    args = make_enqueue_args()
    assert args.actor == "test_actor"
    assert args.payload == {"value": 1}
    assert args.idempotency_key is None
    assert args.identity_key is None


def test_error_info_defaults() -> None:
    info = error_info()
    assert info.error_class == "ValueError"
    assert info.error_message == "boom"
    assert info.error_traceback is None


def test_error_info_custom_values() -> None:
    info = error_info(error_class="TypeError", error_message="oops")
    assert info.error_class == "TypeError"
    assert info.error_message == "oops"
    assert info.error_traceback is None
