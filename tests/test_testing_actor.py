"""Unit tests for taskq.testing.actor — FakeBackend and stub config helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import AttemptRow, ErrorInfo, JobFilter
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)


def test_default_actor_config_returns_transient_policy() -> None:
    config = default_actor_config()
    assert config.retry.kind == "transient"
    assert config.retry.max_attempts == 3
    assert config.retry.jitter == 0.0
    assert config.non_retryable_exceptions == ()
    assert config.on_retry_exhausted is None
    assert config.on_retry_exhausted_timeout == 3.0


def test_empty_payload_is_a_pydantic_model_with_no_fields() -> None:
    payload = EmptyPayload()
    assert payload.model_dump() == {}


async def test_fake_backend_heartbeat_jobs_returns_zero() -> None:
    backend = FakeBackend()
    result = await backend.heartbeat_jobs(new_uuid(), timedelta(seconds=30))
    assert result == 0


async def test_fake_backend_extend_reservation_leases_returns_zero() -> None:
    backend = FakeBackend()
    result = await backend.extend_reservation_leases(new_uuid(), timedelta(seconds=30))
    assert result == 0


async def test_fake_backend_write_cancel_escalation_returns_false() -> None:
    backend = FakeBackend()
    result = await backend.write_cancel_escalation(new_job_id(), new_uuid(), 2)
    assert result is False


async def test_fake_backend_mark_abandoned_returns_false() -> None:
    backend = FakeBackend()
    result = await backend.mark_abandoned(new_job_id())
    assert result is False


async def test_fake_backend_write_attempt_is_a_noop() -> None:
    backend = FakeBackend()
    attempt = AttemptRow(
        job_id=new_job_id(),
        attempt=1,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=None,
        outcome="succeeded",
        error_class=None,
        error_message=None,
        error_traceback=None,
        duration_ms=None,
        worker_id=None,
        metadata={},
    )
    assert await backend.write_attempt(attempt) is None


async def test_fake_backend_get_attempts_returns_empty_list() -> None:
    backend = FakeBackend()
    assert await backend.get_attempts(new_job_id()) == []


async def test_fake_backend_get_events_returns_empty_list() -> None:
    backend = FakeBackend()
    assert await backend.get_events(new_job_id()) == []


async def test_fake_backend_write_cancel_request_returns_false() -> None:
    backend = FakeBackend()
    assert await backend.write_cancel_request(new_job_id(), "reason") is False


async def test_fake_backend_poll_cancel_flags_returns_empty_list() -> None:
    backend = FakeBackend()
    assert await backend.poll_cancel_flags(new_uuid()) == []


async def test_fake_backend_scheduled_to_pending_returns_zero() -> None:
    backend = FakeBackend()
    assert await backend.scheduled_to_pending(datetime(2026, 1, 1, tzinfo=UTC)) == 0


async def test_fake_backend_deadline_sweep_returns_zero() -> None:
    backend = FakeBackend()
    assert await backend.deadline_sweep(datetime(2026, 1, 1, tzinfo=UTC)) == 0


async def test_fake_backend_reclaim_expired_locks_returns_zero() -> None:
    backend = FakeBackend()
    result = await backend.reclaim_expired_locks(
        datetime(2026, 1, 1, tzinfo=UTC), timedelta(seconds=30), timedelta(seconds=60)
    )
    assert result == 0


async def test_fake_backend_get_returns_none() -> None:
    backend = FakeBackend()
    assert await backend.get(new_job_id()) is None


async def test_fake_backend_list_jobs_returns_empty_list() -> None:
    backend = FakeBackend()
    result = await backend.list_jobs(JobFilter())
    assert result == []


async def test_fake_backend_count_pending_jobs_returns_empty_dict() -> None:
    backend = FakeBackend()
    assert await backend.count_pending_jobs(["actor_a"]) == {}


async def test_fake_backend_mark_succeeded_records_call_and_returns_true() -> None:
    backend = FakeBackend()
    job_id = new_job_id()
    worker_id = new_uuid()
    result = await backend.mark_succeeded(job_id, worker_id, {"ok": True})
    assert result is True
    assert backend.mark_succeeded_calls == [(job_id, worker_id, {"ok": True})]


async def test_fake_backend_mark_succeeded_with_conn_delegates_to_mark_succeeded() -> None:
    backend = FakeBackend()
    job_id = new_job_id()
    worker_id = new_uuid()
    result = await backend.mark_succeeded_with_conn(object(), job_id, worker_id, {"ok": True})
    assert result is True
    assert backend.mark_succeeded_calls == [(job_id, worker_id, {"ok": True})]


async def test_fake_backend_mark_failed_or_retry_records_call_and_returns_job_row() -> None:
    backend = FakeBackend()
    job_id = new_job_id()
    worker_id = new_uuid()
    error = ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None)
    row = await backend.mark_failed_or_retry(job_id, worker_id, error, None)
    assert row.id is not None
    assert backend.mark_failed_or_retry_calls == [
        {
            "job_id": job_id,
            "worker_id": worker_id,
            "error_info": error,
            "next_scheduled_at": None,
        }
    ]


async def test_fake_backend_mark_cancelled_records_call_and_returns_true() -> None:
    backend = FakeBackend()
    job_id = new_job_id()
    worker_id = new_uuid()
    result = await backend.mark_cancelled(job_id, worker_id, progress_seq=5)
    assert result is True
    assert backend.mark_cancelled_calls == [
        {
            "job_id": job_id,
            "worker_id": worker_id,
            "progress_seq": 5,
            "progress_state": None,
        }
    ]


async def test_fake_backend_mark_snoozed_uses_configured_return_value() -> None:
    backend = FakeBackend(mark_snoozed_return="failed")
    job_id = new_job_id()
    worker_id = new_uuid()
    result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=10))
    assert result == "failed"
    assert backend.mark_snoozed_calls[0]["outcome"] == "snoozed"


async def test_fake_backend_mark_retry_after_uses_configured_return_value() -> None:
    backend = FakeBackend(mark_retry_after_return="failed:MaxAttemptsExceeded")
    job_id = new_job_id()
    worker_id = new_uuid()
    result = await backend.mark_retry_after(job_id, worker_id, timedelta(seconds=10))
    assert result == "failed:MaxAttemptsExceeded"
    assert backend.mark_retry_after_calls[0]["consume_budget"] is True


def test_as_backend_returns_same_object_cast_to_backend_protocol() -> None:
    fb = FakeBackend()
    backend = as_backend(fb)
    assert backend is fb
