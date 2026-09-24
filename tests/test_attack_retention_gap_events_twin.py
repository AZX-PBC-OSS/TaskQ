"""ATTACK pins: the event-retention gap is FAIL VISIBLE on the stream.

The loss class: a trailing-watermark consumer (``TaskQ.watch_reclaims``)
parks its cursor at seq K; event retention (the sweep's age arm, the outbox
age-cap arm, or the terminal prune's cascade) deletes ``(K, K+n]``; the
consumer resumes. Before the event-prune watermark
(``job_events_prune_state``, migration 01.00.20_01) the resumed poll simply
returned the surviving rows: the stream read exactly like a quiet fleet,
the loss was silent on both sides. These pins fix the replacement contract
on the in-memory twin (the same gate code the PG transports run):

* a resumed cursor strictly below the watermark ends the stream with
  ``EventRetentionGapError`` BEFORE anything is delivered (no partial
  delivery, no skip to live);
* a caught-up cursor (at or above the watermark) never raises;
* a fresh watcher (``after_id=0``) is a new tail and never raises;
* the backend-level poll itself stays silent (the gate lives at the
  stream layer, the cursor protocol's owner);
* the prune simulation's cascade arm advances the watermark exactly as
  the PG prune's ``event_watermark`` CTE does.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from taskq.backend._protocol import EventRow, JobId
from taskq.client._taskq import TaskQ
from taskq.exceptions import EventRetentionGapError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


async def _make_crashed_job_with_reclaim_event(backend: InMemoryBackend) -> JobId:
    """One crashed job holding one crash-reclaim event (the row the
    outbox poll filters for), the same construction
    test_watch_reclaims._make_running_row applies."""
    from dataclasses import replace as _replace

    args = make_enqueue_args(
        actor="gap_attack_actor",
        queue="default",
        payload={},
        scheduled_at=_START,
        max_attempts=3,
        retry_kind="transient",
        priority=0,
        schedule_to_close=None,
    )
    row = await backend.enqueue(args)
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access, the established fixture pattern
    running_row = _replace(
        row,
        status="running",
        locked_by_worker=worker_id,
        lock_expires_at=_START - timedelta(seconds=1),
        started_at=_START,
        last_heartbeat_at=_START,
    )
    backend._jobs[row.id] = running_row  # pyright: ignore[reportPrivateUsage]
    backend.advance_clock_to(_START + timedelta(seconds=1))
    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 1
    # The reclaim's retry policy re-pends a retryable failure (to_state
    # 'pending', the job row non-terminal). The cascade construct needs a
    # TERMINAL row - the prune never touches a live one - so settle the
    # job into 'crashed' the way an exhausted-reclaim fleet's rows sit,
    # finished_at stamped, the reclaim event already in the log.
    crashed = _replace(
        backend._jobs[row.id],  # pyright: ignore[reportPrivateUsage]
        status="crashed",
        finished_at=_START + timedelta(seconds=1),
        locked_by_worker=None,
        lock_expires_at=None,
    )
    backend._jobs[row.id] = crashed  # pyright: ignore[reportPrivateUsage]
    return row.id


def _inject_poll_only_tq(backend: InMemoryBackend, *, poll_timeout: float = 0.05) -> TaskQ:
    """The test_watch_reclaims transport injection: forces the plain poll
    loop, which runs the same gap gate the PG transports run."""
    settings_dict = {"TASKQ_SCHEMA_NAME": "taskq_test"}
    from taskq.client._jobs import JobsClient
    from taskq.settings import TaskQSettings

    tq = TaskQ.__new__(TaskQ)
    tq._dsn = None
    tq._pool = None
    tq._schema = "taskq_test"
    tq._min_pool_size = 1
    tq._max_pool_size = 5
    tq._redis_url = None
    tq._redis_client = None
    tq._pg_conn_factory = None
    tq._listen_conn = None
    tq._poll_timeout = poll_timeout
    tq._owns_pool = True
    tq._client = JobsClient(backend, settings=TaskQSettings.load_from_dict(settings_dict))
    return tq


async def _collect(gen: Any, *, n: int) -> list[EventRow]:
    collected: list[EventRow] = []
    async with contextlib.aclosing(gen) as agen:
        async for evt in agen:
            collected.append(evt)
            if len(collected) >= n:
                break
    return collected


class TestResultUnavailableReasonBoundary:
    """The reason derivation's own boundary (``result_expires_at <= now``):
    one off-by-one to ``<`` (or an inverted ``>``) would read an expired
    result as "not stored" at the exact instant of expiry - the conflation
    the receipt exists to kill."""

    @staticmethod
    def _row(result_expires_at: datetime | None) -> Any:
        from taskq._ids import new_job_id
        from taskq.backend._protocol import CancelPhase, JobRow

        return JobRow(
            id=new_job_id(),
            actor="reason_boundary_actor",
            queue="default",
            identity_key=None,
            fairness_key=None,
            payload={},
            payload_schema_ver=0,
            status="succeeded",
            priority=0,
            attempt=1,
            max_attempts=3,
            retry_kind="transient",
            schedule_to_close=None,
            start_to_close=None,
            heartbeat_timeout=None,
            created_at=datetime(2025, 1, 1, tzinfo=UTC),
            scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
            started_at=None,
            finished_at=datetime(2025, 1, 1, tzinfo=UTC),
            last_heartbeat_at=None,
            locked_by_worker=None,
            lock_expires_at=None,
            cancel_requested_at=None,
            cancel_phase=CancelPhase.NONE,
            error_class=None,
            error_message=None,
            error_traceback=None,
            progress_state={},
            progress_seq=0,
            result=None,
            result_size_bytes=None,
            result_expires_at=result_expires_at,
            idempotency_key=None,
            idempotency_scope="",
            trace_id=None,
            span_id=None,
            metadata={},
            tags=(),
        )

    def _reason(self, result_expires_at: datetime | None) -> str:
        from taskq.exceptions import ResultUnavailable

        exc = ResultUnavailable(self._row(result_expires_at))
        return exc.reason

    def test_past_expiry_reads_expired(self) -> None:
        now = datetime.now(UTC)
        assert self._reason(now - timedelta(microseconds=1)) == "result_ttl_expired"

    def test_future_expiry_reads_not_stored(self) -> None:
        now = datetime.now(UTC)
        assert self._reason(now + timedelta(seconds=60)) == "not_stored"

    def test_exactly_at_expiry_reads_expired(self) -> None:
        """The comparison is ``<=``: at the expiry instant the result is
        already gone. One off-by-one to ``<`` here reads the exact
        boundary as "not stored"."""
        import unittest.mock as mock

        from taskq import exceptions as exc_mod

        now = datetime.now(UTC)
        with mock.patch.object(exc_mod, "datetime", wraps=exc_mod.datetime) as patched:
            patched.now.return_value = now
            assert self._reason(now) == "result_ttl_expired"

    def test_no_stamp_reads_not_stored(self) -> None:
        assert self._reason(None) == "not_stored"


class TestRetentionGapFailVisible:
    async def test_prune_cascade_advances_watermark_and_gap_ends_stream(self) -> None:
        """Consumer delivered event e1 (cursor e1), retention (the prune's
        cascade) deletes e1's and e2's events: the resumed cursor sits
        strictly below the watermark and the stream must end with
        EventRetentionGapError naming both numbers - never a silent skip
        to live."""
        backend = _make_backend()
        job1 = await _make_crashed_job_with_reclaim_event(backend)
        job2 = await _make_crashed_job_with_reclaim_event(backend)

        events = sorted(backend._events, key=lambda e: e.event_id)  # pyright: ignore[reportPrivateUsage]
        assert len(events) == 2
        cursor = events[0].event_id
        assert cursor < events[1].event_id

        # Before the delete the resume is ordinary: e2 delivers.
        tq = _inject_poll_only_tq(backend)
        got = await asyncio.wait_for(_collect(tq.watch_reclaims(after_id=cursor), n=1), timeout=5.0)
        assert got[0].event_id == events[1].event_id

        # Retention: the prune simulation cascades both jobs' events away
        # (the twin mirror of the PG prune's event_watermark arm).
        backend.advance_clock_to(_START + timedelta(seconds=10))
        from taskq.testing._runner import archive_terminal_jobs

        result = archive_terminal_jobs(
            backend,
            retention=timedelta(seconds=5),
            archive_retention=timedelta(days=1),
        )
        assert result.total_deleted == 2
        assert not backend._events  # pyright: ignore[reportPrivateUsage]
        assert backend._events_pruned_through == events[1].event_id  # pyright: ignore[reportPrivateUsage]

        # The backend poll is silent (it cannot know what a cursor means);
        # the STREAM is where the loss becomes loud.
        assert await backend.poll_reclaim_events(cursor) == []

        tq2 = _inject_poll_only_tq(backend)
        with pytest.raises(EventRetentionGapError) as exc_info:
            await asyncio.wait_for(_collect(tq2.watch_reclaims(after_id=cursor), n=1), timeout=5.0)
        assert exc_info.value.cursor == cursor
        assert exc_info.value.pruned_through_id == events[1].event_id
        assert str(cursor) in str(exc_info.value)
        assert str(events[1].event_id) in str(exc_info.value)

        assert job1 != job2  # silence unused-name probes; the ids pin nothing here

    async def test_caught_up_cursor_never_raises(self) -> None:
        """A cursor at the watermark is safe by construction: every
        deleted id was at or below a position the consumer had already
        been delivered. An off-by-one to ``<=`` here would kill every
        caught-up consumer the first time retention caught up with the
        tail."""
        backend = _make_backend()
        await _make_crashed_job_with_reclaim_event(backend)
        events = sorted(backend._events, key=lambda e: e.event_id)  # pyright: ignore[reportPrivateUsage]
        watermark = events[-1].event_id

        backend.advance_clock_to(_START + timedelta(seconds=10))
        from taskq.testing._runner import archive_terminal_jobs

        archive_terminal_jobs(
            backend, retention=timedelta(seconds=5), archive_retention=timedelta(days=1)
        )
        assert backend._events_pruned_through == watermark  # pyright: ignore[reportPrivateUsage]

        tq = _inject_poll_only_tq(backend, poll_timeout=0.05)
        # Resuming AT the watermark: the gate must pass. Nothing new
        # arrives, so collecting one event can only time out - a
        # TimeoutError is the pass, EventRetentionGapError the fail.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                _collect(tq.watch_reclaims(after_id=watermark), n=1), timeout=0.5
            )

    async def test_fresh_watcher_after_id_zero_never_raises(self) -> None:
        """``after_id=0`` is a new tail: nothing was ever delivered to this
        consumer, so no event it received is behind the horizon. Raising
        here would make a fresh watcher impossible on any schema that has
        ever pruned."""
        backend = _make_backend()
        # Watermark advanced by a prune with no consumer ever attached.
        await _make_crashed_job_with_reclaim_event(backend)
        backend.advance_clock_to(_START + timedelta(seconds=10))
        from taskq.testing._runner import archive_terminal_jobs

        archive_terminal_jobs(
            backend, retention=timedelta(seconds=5), archive_retention=timedelta(days=1)
        )
        assert backend._events_pruned_through > 0  # pyright: ignore[reportPrivateUsage]

        tq = _inject_poll_only_tq(backend, poll_timeout=0.05)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(_collect(tq.watch_reclaims(after_id=0), n=1), timeout=0.5)

    async def test_backend_without_capability_arms_no_signal(self) -> None:
        """A backend without a usable ``event_prune_watermark`` (a
        third-party backend, or a pre-watermark build) disables the gate
        silently: the getattr probe finds nothing callable and the stream
        behaves exactly as before. Deleting the CLASS method is not the
        way to simulate it (``del`` on a class-level method through the
        instance raises), so this shadows it with an instance attribute
        of None - exactly what the probe must treat as absent."""
        backend = _make_backend()
        tq = _inject_poll_only_tq(backend)

        from taskq.client._taskq import _assert_no_retention_gap

        saved = InMemoryBackend.event_prune_watermark  # pyright: ignore[reportAttributeAccessUsage]
        try:
            InMemoryBackend.event_prune_watermark = None  # type: ignore[assignment]  # pyright: ignore[reportAttributeAccessUsage]
            await _assert_no_retention_gap(tq._client, 12345)  # pyright: ignore[reportPrivateUsage]  # must not raise
        finally:
            InMemoryBackend.event_prune_watermark = saved  # pyright: ignore[reportAttributeAccessUsage]
