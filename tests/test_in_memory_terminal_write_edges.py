"""Edge pins for the in-memory twin's terminal writes and batch enqueue.

The twin (``taskq.testing``) is a shipped product surface: suites assert
against it in CI and differential runs compare it to Postgres. These tests
pin the boundary behaviors its PG twin carries that no other test reaches:

* a terminal write or admin op naming an unknown job id reports ``False``
  (a no-op), never a KeyError;
* a terminal write with no progress state on a job that has none stores
  NULL progress rather than failing on the merge;
* progress state a UTF-8 encoder cannot accept lands with the lone
  surrogate escaped (the derived-value escape, mirrored from
  ``backend/_terminal.py``), while a value the escape cannot repair
  (over-deep nesting) is refused with the original ``UnencodableValue``;
* ``retry_job`` refuses at the smallint attempt ceiling instead of
  re-pending a row the next claim could only overflow;
* ``retry_job`` reopens a completed batch whose member it re-pends;
* batch enqueue refuses an empty list the way the PG tier's COPY
  preflight does;
* ``enqueue_batch_fast(enforce_max_pending=False)`` is the uncapped COPY
  lane: the cap that partitions (and refuses as a group) on the default
  path does not apply.

Private-access stamps (``backend._jobs[job_id] = replace(...)``) follow the
established twin-test pattern (tests/test_cancel_fence_arms.py): they force
race-window or ceiling states the public API cannot reach directly.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import EnqueueArgs
from taskq.backend._protocol import CancelPhase, JobFilter, JobId
from taskq.exceptions import (
    BatchMaxPendingExceededError,
    SingletonCollisionError,
    UnencodableValue,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2026, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


async def _enqueue_and_dispatch(
    backend: InMemoryBackend,
    *,
    queue: str = "default",
    metadata: dict[str, object] | None = None,
) -> tuple[JobId, UUID]:
    if "test_actor" not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend.register_actor_config(actor="test_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue=queue,
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        schedule_to_close=None,
        **({"metadata": metadata} if metadata is not None else {}),
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend, mirrors tests/test_cancel_fence_arms.py
    dispatched = await backend.dispatch_batch(
        worker_id,
        [queue],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    return dispatched[0].id, worker_id


# ── Unknown job ids are no-ops, never KeyErrors ─────────────────────────


class TestUnknownJobIdIsANoop:
    async def test_mark_cancelled_unknown_job_reports_false(self) -> None:
        backend = _make_backend()
        assert await backend.mark_cancelled(new_job_id(), new_uuid()) is False

    async def test_write_cancel_escalation_unknown_job_reports_false(self) -> None:
        backend = _make_backend()
        assert await backend.write_cancel_escalation(new_job_id(), new_uuid(), 2) is False

    async def test_mark_abandoned_unknown_job_reports_false(self) -> None:
        backend = _make_backend()
        assert await backend.mark_abandoned(new_job_id()) is False


# ── The progress merge's empty state ────────────────────────────────────


class TestAbandonedWithoutProgressState:
    async def test_mark_abandoned_with_no_progress_on_either_side_stores_empty(
        self,
    ) -> None:
        """mark_abandoned carries no progress args on the abandon path, and
        a job the actor never progressed has no buffered state: the merge
        is the ``COALESCE(progress_state,'{}') || '{}'`` mirror's empty
        object on the terminal row — not a crash, not a fabricated
        progress entry."""
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        # Abandon is the forced-cancel terminal: the row must carry the
        # operator's FORCED phase for the guard to admit it.
        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: forcing a race-window state the public API cannot reach directly
        backend._jobs[job_id] = replace(row, cancel_phase=CancelPhase.FORCED)  # type: ignore[reportPrivateUsage]

        assert await backend.mark_abandoned(job_id) is True

        stored = await backend.get(job_id)
        assert stored is not None
        assert stored.status == "abandoned"
        assert stored.progress_state == {}


# ── The surrogate escape family on the terminal write ───────────────────


def _deep_with_shallow_surrogate(depth: int) -> dict[str, object]:
    """A lone surrogate at the TOP level, over-deep nesting BELOW it.

    orjson serializes dict entries in insertion order, so the first
    ``dumps`` attempt hits the surrogate (``UnencodableValue``) before the
    depth ever becomes the interesting failure; the repair walk then dies
    of stack exhaustion on the deep tail (``RecursionError``).
    """
    deep_tail: dict[str, object] = {"bottom": True}
    for _ in range(depth):
        deep_tail = {"next": deep_tail}
    return {"s": "\udcff", "deep": deep_tail}


class TestProgressSurrogateEscape:
    async def test_shallow_surrogate_progress_lands_escaped(self) -> None:
        """A lone surrogate (exactly what ``os.fsdecode`` of a non-UTF-8
        filename byte yields) in the actor's progress state must not
        strand the job running in the crash-reclaim loop: the write lands
        with the defect visible as a backslash escape."""
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)

        landed = await backend.mark_succeeded(
            job_id,
            wid,
            {"ok": True},
            attempt=1,
            claim_epoch=1,
            progress_state={"detail": {"name": "\udcff"}},
        )

        assert landed is True
        stored = await backend.get(job_id)
        assert stored is not None
        assert stored.status == "succeeded"
        assert stored.progress_state is not None
        detail = stored.progress_state["detail"]
        assert detail == {"name": "\\udcff"}, (
            f"the surrogate must land escaped (visible in the stored state), got {detail!r}"
        )

    async def test_progress_the_escape_cannot_repair_is_refused_with_the_original_error(
        self,
    ) -> None:
        """Over-deep nesting is a structural refusal escaping cannot repair:
        the original UnencodableValue (the surrogate) is the truthful
        failure, re-raised with the repair's RecursionError suppressed —
        and the terminal write does not land, so no half-escaped row is
        stored."""
        backend = _make_backend()
        job_id, wid = await _enqueue_and_dispatch(backend)
        poisoned = _deep_with_shallow_surrogate(3000)

        with pytest.raises(UnencodableValue):
            await backend.mark_succeeded(
                job_id,
                wid,
                {"ok": True},
                attempt=1,
                claim_epoch=1,
                progress_state=poisoned,
            )

        stored = await backend.get(job_id)
        assert stored is not None
        assert stored.status == "running", (
            "a progress value the escape cannot repair must not terminalise "
            "the row with a half-written state"
        )


# ── retry_job's ceilings and batch reconciliation ────────────────────────


class TestRetryJobCeilings:
    async def test_retry_at_the_smallint_attempt_ceiling_is_refused(self) -> None:
        """32767 is the ceiling: ``LEAST(GREATEST(max_attempts, attempt+1),
        32767)`` cannot rise past the spent attempt, so the retry must be
        refused and the row stays terminal rather than re-pending a job
        the next claim could only overflow."""
        backend = _make_backend()
        job_id, _wid = await _enqueue_and_dispatch(backend)
        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: forcing the ceiling state the public API cannot reach without 32767 dispatches
        backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]
            row,
            status="failed",
            attempt=32767,
            max_attempts=32767,
            finished_at=_START,
        )

        assert await backend.retry_job(job_id) is False

        stored = await backend.get(job_id)
        assert stored is not None
        assert stored.status == "failed", "the refused retry must leave the row terminal"
        assert stored.attempt == 32767


class TestRetryJobReopensTerminalBatch:
    async def test_retrying_a_completed_batchs_member_reopens_the_batch(self) -> None:
        """A re-pended member makes a completed batch row's claim a lie
        (every batch-status writer guards on 'active'): the reopen happens
        in the same store mutation as the re-pend, idempotently."""
        backend = _make_backend()
        batch_id = new_uuid()
        await backend.create_batch(
            batch_id,
            queue="default",
            expected_size=1,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor="test_actor",
        )
        job_id, _wid = await _enqueue_and_dispatch(backend)
        # Membership is metadata on the job row (the finalizer is never
        # stamped); the twin stamps it at the batch-atomic enqueue.
        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: batch membership arrives through _enqueue_batch_atomic on the real path; the stamp isolates the reopen contract
        backend._jobs[job_id] = replace(row, metadata={**row.metadata, "batch_id": batch_id})  # type: ignore[reportPrivateUsage]
        stored_row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]
        backend._jobs[job_id] = replace(  # type: ignore[reportPrivateUsage]
            stored_row,
            status="failed",
            finished_at=_START,
        )
        await backend.complete_batch(batch_id)
        completed = await backend.get_batch(batch_id)
        assert completed is not None and completed.status == "complete"

        assert await backend.retry_job(job_id) is True

        reopened = await backend.get_batch(batch_id)
        assert reopened is not None
        assert reopened.status == "active", (
            "a terminal batch whose member was re-pended must read 'active' "
            "again: every batch-status writer guards on 'active', so a "
            "batch that stays 'complete' would strand the member's outcome"
        )
        assert reopened.completed_at is None


# ── Batch enqueue refusals ───────────────────────────────────────────────


def _item(actor: str = "test_actor", **overrides: object) -> EnqueueArgs:
    defaults: dict[str, object] = {
        "id": new_job_id(),
        "actor": actor,
        "queue": "default",
        "payload": {"k": "v"},
        "max_attempts": 3,
        "retry_kind": "transient",
        "scheduled_at": _START,
        "schedule_to_close": None,
    }
    defaults.update(overrides)
    if defaults.get("metadata") is None:
        defaults.pop("metadata", None)
    return EnqueueArgs(**defaults)  # type: ignore[arg-type]  # Why: the overrides dict is test-local and always well-typed


class TestBatchEnqueueRefusals:
    async def test_enqueue_batch_refuses_an_empty_list(self) -> None:
        backend = _make_backend()
        with pytest.raises(ValueError, match="args_list must not be empty"):
            await backend.enqueue_batch([])

    async def test_enqueue_batch_fast_refuses_an_empty_list(self) -> None:
        backend = _make_backend()
        with pytest.raises(ValueError, match="args_list must not be empty"):
            await backend.enqueue_batch_fast([])

    async def test_fast_lane_without_max_pending_enforcement_bypasses_the_cap(
        self,
    ) -> None:
        """``enforce_max_pending=False`` is the COPY lane's escape hatch:
        the per-actor cap that partitions (and refuses as a group) on the
        default path does not apply, and every item stores."""
        backend = _make_backend()
        items = [
            _item(max_pending=1),
            _item(max_pending=1),
        ]

        with pytest.raises(BatchMaxPendingExceededError):
            await backend.enqueue_batch_fast(items)

        backend2 = _make_backend()
        items2 = [
            _item(max_pending=1),
            _item(max_pending=1),
        ]
        stored = await backend2.enqueue_batch_fast(items2, enforce_max_pending=False)

        assert stored == 2
        rows = await backend2.list_jobs(JobFilter())
        assert len(rows) == 2


# ── Singleton preflight backpressure ────────────────────────────────────


class TestSingletonCollisionRetryAfter:
    async def test_collision_names_the_deadline_of_the_blocking_job(self) -> None:
        """The typed refusal carries the blocking singleton's remaining
        schedule_to_close as ``retry_after`` so a caller can back off for
        exactly the deadline's remainder instead of polling."""
        backend = _make_backend()
        deadline = _START + timedelta(seconds=60)
        await backend.enqueue(_item(metadata={"singleton": True}, schedule_to_close=deadline))

        with pytest.raises(SingletonCollisionError) as exc_info:
            await backend.enqueue_batch([_item(metadata={"singleton": True})])

        error = exc_info.value
        assert error.retry_after is not None
        assert timedelta(0) < error.retry_after <= timedelta(seconds=60), (
            f"retry_after must be the blocking job's remaining deadline "
            f"budget, got {error.retry_after!r}"
        )
