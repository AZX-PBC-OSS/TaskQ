"""A ``JobRow`` stub for the worker-loop unit tests.

Two loop suites (the shutdown stubs and the worker-main wiring tests)
kept near-identical copies of this builder — the copies had already
drifted, one losing the Why comments on its two ``type: ignore`` lines;
this module is their single source. Every field the loop under test does
not read stays ``None`` (the ignores document that the runtime check
they satisfy is the only reader), so a new JobRow field lands here once
instead of twice.
"""

from __future__ import annotations

from uuid import UUID

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId, JobRow

__all__ = ["make_job"]


def make_job(
    *,
    job_id: UUID | None = None,
    actor: str = "test_actor",
    queue: str = "default",
    attempt: int = 1,
) -> JobRow:
    if job_id is None:
        job_id = new_uuid()
    return JobRow(
        id=JobId(job_id),
        actor=actor,
        queue=queue,
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=attempt,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=None,  # type: ignore[arg-type] # Why: not read by stub; None satisfies runtime check.
        scheduled_at=None,  # type: ignore[arg-type]
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=None,  # type: ignore[arg-type] # Why: not read by stub.
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )
