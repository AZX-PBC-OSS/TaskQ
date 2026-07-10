"""Audit: exactly four code paths write status='scheduled'.

declares that ``status='scheduled'`` is written by EXACTLY four
authorised code paths. This module contains two tripwire tests:

1. A runtime audit that calls every non-authorised Backend method on
   an appropriate pre-state job and asserts the post-state is NOT
   ``'scheduled'``. If a future refactor accidentally adds a fifth
   writer, this test fails.

2. A static-analysis audit that uses ``inspect.getsource`` to grep
   ``mark_*`` methods on both backends for literal ``'scheduled'``
   SQL / status writes outside the four authorised methods.

Maintenance contract: if a new method is added to ``Backend``, add it
to the appropriate audit list below.
"""

import inspect
import re
from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import (
    AttemptRow,
    EnqueueArgs,
    ErrorInfo,
    JobFilter,
    JobId,
    JobRow,
)
from taskq.testing.in_memory import InMemoryBackend

# ── Authorised writers ──────────────────────────────────────────
# These four paths are EXCLUDED from the audit. If a new authorised
# writer is added, update this tuple AND the test docstring.
AUTHORISED_SCHEDULED_WRITERS: tuple[str, ...] = (
    "enqueue(future_scheduled_at)",
    "mark_snoozed",
    "mark_retry_after",
    "mark_failed_or_retry(branch_b)",
)

# ── Helpers ────────────────────────────────────────────────────────────

_CLOCK_START = datetime(2025, 1, 1, tzinfo=UTC)


async def _enqueue_running(
    backend: InMemoryBackend,
    *,
    actor: str = "a",
    max_attempts: int = 3,
    retry_kind: Literal["transient", "indefinite", "non_retryable"] = "transient",
    schedule_to_close: datetime | None = None,
) -> JobRow:
    """Enqueue a job and dispatch it so it ends up ``running``."""
    job_id = new_job_id()
    # Register actor so dispatch_batch finds it (mirrors PG actor_config requirement).
    if actor not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage] # Why: test helper
        backend.register_actor_config(actor=actor)
    args = EnqueueArgs(
        id=job_id,
        actor=actor,
        queue="default",
        payload={},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        scheduled_at=_CLOCK_START,
        schedule_to_close=schedule_to_close,
    )
    await backend.enqueue(args)
    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test needs the internal worker_id for dispatch
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    return dispatched[0]


async def _enqueue_scheduled(
    backend: InMemoryBackend,
    *,
    actor: str = "a",
) -> JobRow:
    """Enqueue with future scheduled_at so status is ``scheduled``."""
    future = _CLOCK_START + timedelta(hours=1)
    job_id = new_job_id()
    args = EnqueueArgs(
        id=job_id,
        actor=actor,
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=future,
    )
    row = await backend.enqueue(args)
    assert row.status == "scheduled"
    return row


async def _enqueue_pending_job(
    backend: InMemoryBackend,
    *,
    actor: str = "a",
    schedule_to_close: datetime | None = None,
) -> JobRow:
    """Enqueue with now-ish scheduled_at so status is ``pending``."""
    job_id = new_job_id()
    args = EnqueueArgs(
        id=job_id,
        actor=actor,
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_CLOCK_START,
        schedule_to_close=schedule_to_close,
    )
    row = await backend.enqueue(args)
    assert row.status == "pending"
    return row


def _get_job(backend: InMemoryBackend, job_id: JobId) -> JobRow:
    row = backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test reads internal storage to inspect post-state without an await
    assert row is not None
    return row


# ── Runtime audit: non-authorised methods do NOT write 'scheduled' ─────


async def test_fr5_audit_only_four_paths_write_scheduled(
    memory_jobs: InMemoryBackend,
) -> None:
    """T-FR5-AUDIT: only four paths write status='scheduled'."""
    backend = memory_jobs
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test needs the internal worker_id for mark_* calls

    # ── mark_succeeded on a running job → succeeded, not scheduled ──
    job = await _enqueue_running(backend)
    result = await backend.mark_succeeded(job.id, worker_id, {"ok": True})
    assert result is True
    post = _get_job(backend, job.id)
    assert post.status == "succeeded"
    assert post.status != "scheduled"

    # ── mark_failed_or_retry Branch A (next_scheduled_at=None) → failed ──
    job = await _enqueue_running(backend)
    error_info = ErrorInfo(
        error_class="TestError",
        error_message="boom",
        error_traceback=None,
    )
    row = await backend.mark_failed_or_retry(job.id, worker_id, error_info, next_scheduled_at=None)
    assert row.status == "failed"
    assert row.status != "scheduled"

    # ── mark_cancelled on a running job → cancelled, not scheduled ──
    job = await _enqueue_running(backend)
    result = await backend.mark_cancelled(job.id, worker_id)
    assert result is True
    post = _get_job(backend, job.id)
    assert post.status == "cancelled"
    assert post.status != "scheduled"

    # ── write_cancel_escalation on a running/cp=1 job → still running ──
    job = await _enqueue_running(backend)
    await backend.write_cancel_request(job.id, reason="test")
    result = await backend.write_cancel_escalation(job.id, worker_id, phase=2)
    assert result is True
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── mark_abandoned on a running/cp=2 job → abandoned, not scheduled ──
    job = await _enqueue_running(backend)
    await backend.write_cancel_request(job.id, reason="test")
    await backend.write_cancel_escalation(job.id, worker_id, phase=2)
    result = await backend.mark_abandoned(job.id)
    assert result is True
    post = _get_job(backend, job.id)
    assert post.status == "abandoned"
    assert post.status != "scheduled"

    # ── write_cancel_request on a pending job → cancelled, not scheduled ──
    job = await _enqueue_pending_job(backend)
    result = await backend.write_cancel_request(job.id, reason="test")
    assert result is True
    post = _get_job(backend, job.id)
    assert post.status == "cancelled"
    assert post.status != "scheduled"

    # ── write_cancel_request on a scheduled job → cancelled, not new scheduled ──
    # Pre-state IS scheduled; cancel transitions it to cancelled.
    # The audit must only flag NEW transitions TO scheduled, not pre-existing ones.
    job = await _enqueue_scheduled(backend)
    pre = _get_job(backend, job.id)
    assert pre.status == "scheduled"
    result = await backend.write_cancel_request(job.id, reason="test")
    assert result is True
    post = _get_job(backend, job.id)
    assert post.status == "cancelled"
    # Not a NEW transition to scheduled — it was already scheduled

    # ── write_cancel_request on a running job → sets cancel_phase=1, still running ──
    job = await _enqueue_running(backend)
    result = await backend.write_cancel_request(job.id, reason="test")
    assert result is True
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── dispatch_batch on pending jobs → running, not scheduled ──
    job = await _enqueue_pending_job(backend)
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=10, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) >= 1
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── heartbeat_jobs on a running job → still running ──
    job = await _enqueue_running(backend)
    count = await backend.heartbeat_jobs(worker_id, timedelta(seconds=60))
    assert count >= 1
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── extend_reservation_leases → no status change ──
    job = await _enqueue_running(backend)
    count = await backend.extend_reservation_leases(worker_id, timedelta(seconds=60))
    assert isinstance(count, int)
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── scheduled_to_pending on a scheduled job → pending, not scheduled ──
    job = await _enqueue_scheduled(backend)
    pre = _get_job(backend, job.id)
    assert pre.status == "scheduled"
    count = await backend.scheduled_to_pending(_CLOCK_START + timedelta(hours=2))
    assert count >= 1
    post = _get_job(backend, job.id)
    assert post.status == "pending"
    assert post.status != "scheduled"

    # ── deadline_sweep on pending job with expired schedule_to_close → failed ──
    past_deadline = _CLOCK_START - timedelta(seconds=1)
    job = await _enqueue_pending_job(backend, schedule_to_close=past_deadline)
    count = await backend.deadline_sweep(_CLOCK_START + timedelta(hours=1))
    assert count >= 1
    post = _get_job(backend, job.id)
    assert post.status == "failed"
    assert post.status != "scheduled"

    # ── deadline_sweep on scheduled job with expired schedule_to_close → failed ──
    past_deadline = _CLOCK_START - timedelta(seconds=1)
    future_sched = _CLOCK_START + timedelta(hours=1)
    job_id = new_job_id()
    args = EnqueueArgs(
        id=job_id,
        actor="deadline_sweep_sched",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=future_sched,
        schedule_to_close=past_deadline,
    )
    await backend.enqueue(args)
    count = await backend.deadline_sweep(_CLOCK_START + timedelta(hours=2))
    assert count >= 1
    post = _get_job(backend, job_id)
    assert post.status == "failed"
    assert post.status != "scheduled"

    # ── reclaim_expired_locks on a running job with expired lock → pending or crashed ──
    job = await _enqueue_running(backend)
    pre = _get_job(backend, job.id)
    assert pre.status == "running"
    count = await backend.reclaim_expired_locks(
        _CLOCK_START + timedelta(hours=2),
        timedelta(seconds=30),
        timedelta(seconds=30),
    )
    assert count >= 1
    post = _get_job(backend, job.id)
    assert post.status in ("pending", "crashed")
    assert post.status != "scheduled"

    # ── poll_cancel_flags → read-only, status unchanged ──
    job = await _enqueue_running(backend)
    await backend.write_cancel_request(job.id, reason="test")
    flags = await backend.poll_cancel_flags(worker_id)
    assert isinstance(flags, list)
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── get → read-only, status unchanged ──
    job = await _enqueue_running(backend)
    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "running"
    assert row.status != "scheduled"

    # ── list_jobs → read-only, status unchanged ──
    job = await _enqueue_running(backend)
    rows = await backend.list_jobs(JobFilter(status="running"))
    assert any(r.id == job.id for r in rows)
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── get_attempts → read-only, status unchanged ──
    job = await _enqueue_running(backend)
    attempts = await backend.get_attempts(job.id)
    assert isinstance(attempts, list)
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── write_attempt → attempt-row write only, status unchanged ──
    job = await _enqueue_running(backend)
    attempt_row = AttemptRow(
        job_id=job.id,
        attempt=job.attempt,
        started_at=_CLOCK_START,
        finished_at=_CLOCK_START + timedelta(seconds=1),
        outcome="succeeded",
        error_class=None,
        error_message=None,
        error_traceback=None,
        duration_ms=1000,
        worker_id=worker_id,
        metadata={},
    )
    await backend.write_attempt(attempt_row)
    post = _get_job(backend, job.id)
    assert post.status == "running"
    assert post.status != "scheduled"

    # ── subscribe_wake → no status change ──
    async with backend.subscribe_wake() as event:
        assert not event.is_set()


# ── Static-analysis audit ──────────────────────────────────────────────

_AUTHORISED_METHOD_NAMES: frozenset[str] = frozenset(
    {"mark_snoozed", "mark_retry_after", "mark_failed_or_retry"}
)


@pytest.mark.skipif(
    not hasattr(inspect, "getsource"),
    reason="inspect.getsource unavailable (frozen / -OO build)",
)
def test_fr5_static_audit_no_extra_scheduled_writers() -> None:
    """Static analysis: mark_* methods outside the four authorised paths
    must not contain literal ``'scheduled'`` status writes.

    This greps the source of each ``mark_*`` method on both
    ``InMemoryBackend`` and ``PostgresBackend`` for ``'scheduled'``
    literals. The enqueue path is excluded (it is not a ``mark_*``
    method). The four authorised methods (mark_snoozed, mark_retry_after,
    mark_failed_or_retry) are allowed to contain ``'scheduled'``.

    If a new ``mark_*`` method is added that writes ``'scheduled'``,
    this test fails and the method must either be added to
    ``AUTHORISED_SCHEDULED_WRITERS`` or the ``'scheduled'`` write must
    be removed.
    """
    from taskq.backend.postgres import PostgresBackend

    backend_classes = [InMemoryBackend, PostgresBackend]

    # Pattern matches string literals containing 'scheduled' — covers
    # "scheduled", status="scheduled", 'scheduled', etc.
    _scheduled_literal_re = re.compile(r"""['"]scheduled['"]""")

    violations: list[str] = []

    for cls in backend_classes:
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not name.startswith("mark_"):
                continue
            if name in _AUTHORISED_METHOD_NAMES:
                continue
            try:
                source = inspect.getsource(method)
            except (OSError, TypeError):
                continue
            for line_no, line in enumerate(source.splitlines(), start=1):
                if _scheduled_literal_re.search(line):
                    violations.append(f"{cls.__name__}.{name} line {line_no}: {line.strip()}")

    assert not violations, (
        "Non-authorised mark_* methods contain 'scheduled' literal writes:\n"
        + "\n".join(f"  - {v}" for v in violations)
        + "\n\nIf this is a new authorised writer, update "
        "AUTHORISED_SCHEDULED_WRITERS and _AUTHORISED_METHOD_NAMES."
    )
