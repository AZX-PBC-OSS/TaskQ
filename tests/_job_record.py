"""A record dict with every field ``_job_row_from_record`` reads.

Two _enqueue/_insert unit suites (the singleton-violation savepoint test
and the unique-for caller-conn serialization test) kept byte-identical
copies of this fixture record; this module is their single source. The
dict's shape IS the pinned contract: every key the row-from-record
adapter reads, with the frozen ``_NOW`` stamp the two suites' assertions
share.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from taskq._ids import new_job_id

__all__ = ["NOW", "full_record"]

NOW = datetime(2025, 1, 1, tzinfo=UTC)


def full_record(*, job_id: UUID | None = None) -> dict[str, object]:
    """A dict with every field ``_job_row_from_record`` reads."""
    jid = job_id or new_job_id()
    return {
        "id": jid,
        "actor": "test_actor",
        "queue": "default",
        "identity_key": None,
        "fairness_key": None,
        "payload": "{}",
        "payload_schema_ver": 1,
        "status": "pending",
        "priority": 0,
        "attempt": 0,
        "max_attempts": 3,
        "retry_kind": "transient",
        "schedule_to_close": None,
        "start_to_close": None,
        "heartbeat_timeout": None,
        "created_at": NOW,
        "scheduled_at": NOW,
        "started_at": None,
        "finished_at": None,
        "last_heartbeat_at": None,
        "locked_by_worker": None,
        "lock_expires_at": None,
        "cancel_requested_at": None,
        "cancel_phase": 0,
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "progress_state": "{}",
        "progress_seq": 0,
        "result": None,
        "result_size_bytes": None,
        "result_expires_at": None,
        "idempotency_key": None,
        "idempotency_scope": "",
        "trace_id": None,
        "span_id": None,
        "metadata": "{}",
        "tags": [],
        "snooze_count": 0,
        "rate_limit_blocked_count": 0,
        "interrupt_count": 0,
        "retry_base_seconds": 5.0,
        "retry_cap_seconds": 3600.0,
        "retry_backoff": "exponential",
        "retry_jitter": 0.2,
        "assignment_routed": False,
        "claim_epoch": 0,
    }
