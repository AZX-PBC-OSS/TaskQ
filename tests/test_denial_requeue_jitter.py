"""Jitter on the denial requeue - same-round denials must desynchronize.

A denial's ``retry_after`` is advisory timing, but until it is jittered it
is also a synchroniser: N jobs denied in one dispatch round carry
near-identical ``retry_after`` (same-round token-deficit denials compute
the same hint; slot denials reported the same constant), so the herd
re-attempts in lockstep - each cycle costing claim + acquire + snooze per
job. The failure-backoff path already applies multiplicative-symmetric
jitter (``compute_backoff``); the denial requeue takes the same
treatment, sourced from the SAME knob (``RetryPolicy.jitter``), so an
actor that has already chosen a jitter policy for failures gets it for
denial requeues too - and the deterministic suites, which construct
policies with ``jitter=0.0``, stay deterministic (``uniform(1, 1) == 1``).
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import structlog
from opentelemetry.trace import INVALID_SPAN_CONTEXT, NonRecordingSpan

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase, EnqueueArgs, JobId, JobRow
from taskq.constants import MIN_DEFERRAL_INTERVAL
from taskq.exceptions import ReservationUnavailable
from taskq.retry import RetryPolicy
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._handlers import (  # pyright: ignore[reportPrivateUsage]  # Why: the handler is the unit under test; no public seam routes a denial without a full consumer stack.
    _handle_reservation_class_denied,
)

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_RAW = timedelta(seconds=30)


def _running_job_row() -> JobRow:
    """A running job row shaped the way the dispatcher produces one."""
    return JobRow(
        id=new_job_id(),
        actor="jitter_actor",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=10,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=_NOW,
        scheduled_at=_NOW,
        started_at=_NOW,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=new_uuid(),
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
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )


async def _field_denial(
    backend: FakeBackend,
    job: JobRow,
    e: ReservationUnavailable,
    *,
    jitter: float,
) -> None:
    """Run one denial through the handler exactly as the consumer does."""
    result = await _handle_reservation_class_denied(
        as_backend(backend),
        job,
        job.locked_by_worker,
        e,
        NonRecordingSpan(INVALID_SPAN_CONTEXT),
        structlog.get_logger("taskq.test"),
        StubActorConfig(retry=RetryPolicy(jitter=jitter)),
        awaiting_prefix="reservation:",
        outcome="reservation_denied",
        debug_event="consume-reservation-denied-noop",
    )
    assert result == "scheduled"


# ── Same-round denials desynchronize ────────────────────────────────────


async def test_same_round_denials_get_desynchronized_retry_after() -> None:
    """N denials fielded with the SAME ``retry_after`` (the mass-denial
    shape: one round, one token deficit) must not requeue at N identical
    delays - the actor's jitter policy spreads them inside its symmetric
    band, exactly as ``compute_backoff`` spreads failure retries.

    Observable: the delay each denial hands to ``mark_snoozed`` - the
    backend records every call, so the spread is read off the write the
    requeue actually made, not off an intermediate.
    """
    backend = FakeBackend()
    denial = ReservationUnavailable("gpu_pool", _RAW, source="reservation")
    n = 8

    for _ in range(n):
        await _field_denial(backend, _running_job_row(), denial, jitter=0.2)

    delays = [c["delay"] for c in backend.mark_snoozed_calls]
    assert len(delays) == n
    raw_s = _RAW.total_seconds()
    for d in delays:
        d_s = d if isinstance(d, timedelta) else timedelta(seconds=float(d))  # pyright: ignore[reportUnknownMemberType]  # Why: the fake backend records the delay as an object; the pin asserts the timedelta band, so coerce the recorded value.
        assert raw_s * 0.8 <= d_s.total_seconds() <= raw_s * 1.2, (
            f"jittered delay {d} escaped the ±20% band around the raw "
            f"{_RAW} - the jitter must be multiplicative-symmetric "
            "(uniform(1 - jitter, 1 + jitter)), the same formula "
            "compute_backoff applies to failure backoff."
        )
    assert len(set(delays)) > 1, (
        f"{n} same-round denials requeued at a single delay {delays[0]!r} - "
        "the herd re-attempts in lockstep and every cycle costs claim + "
        "acquire + snooze per job; the denial requeue must jitter like the "
        "failure backoff does."
    )


async def test_zero_jitter_passes_raw_denial_delay_through() -> None:
    """``jitter=0.0`` is the deterministic-suite contract: the raw
    ``retry_after`` reaches ``mark_snoozed`` unchanged (``uniform(1, 1)``
    is the identity multiplier), so pinned suites stay pinned."""
    backend = FakeBackend()

    await _field_denial(
        backend,
        _running_job_row(),
        ReservationUnavailable("gpu_pool", _RAW, source="reservation"),
        jitter=0.0,
    )

    assert [c["delay"] for c in backend.mark_snoozed_calls] == [_RAW]


# ── The deferral floor still applies under jitter ───────────────────────


async def _mem_running_job(clock: FakeClock) -> tuple[InMemoryBackend, JobId, UUID]:
    """One running job on the in-memory twin, claimed by a fresh worker."""
    backend = InMemoryBackend(clock=clock)
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement - candidates come FROM the registry).
    backend.register_actor_config(actor="jitter_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="jitter_actor",
        queue="default",
        payload={},
        max_attempts=10,
        retry_kind="transient",
        scheduled_at=_NOW - timedelta(seconds=1),
    )
    await backend.enqueue(args)
    worker_id = new_uuid()
    dispatched = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(dispatched) == 1
    return backend, args.id, worker_id


async def test_jittered_denial_reschedules_at_least_min_deferral_interval_out() -> None:
    """Jitter is timing-only: a denial whose jittered delay collapses
    toward zero (``retry_after`` 0.4 s at the maximum jitter band, where
    draws land in [0, 0.8] s) still reschedules at least
    ``MIN_DEFERRAL_INTERVAL`` out - the snooze arm's floor is applied by
    ``mark_snoozed`` downstream of the jitter, never defeated by it.

    The scheduled row is the observable: every one of the sampled
    denials lands ``scheduled`` strictly past the floor.
    """
    for _ in range(6):
        clock = FakeClock(_NOW)
        backend, job_id, worker_id = await _mem_running_job(clock)
        job = await backend.get(job_id)
        assert job is not None
        result = await _handle_reservation_class_denied(
            backend,
            job,
            worker_id,
            ReservationUnavailable("gpu_pool", timedelta(seconds=0.4), source="reservation"),
            NonRecordingSpan(INVALID_SPAN_CONTEXT),
            structlog.get_logger("taskq.test"),
            StubActorConfig(retry=RetryPolicy(jitter=1.0)),
            awaiting_prefix="reservation:",
            outcome="reservation_denied",
            debug_event="consume-reservation-denied-noop",
        )
        assert result == "scheduled"
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.scheduled_at >= _NOW + MIN_DEFERRAL_INTERVAL, (
            f"a jittered denial requeued at {row.scheduled_at - _NOW} out - "
            f"the {MIN_DEFERRAL_INTERVAL} deferral floor must survive the "
            "jitter, or a collapsed draw parks the job pending at "
            "clock_timestamp() at the head of the dispatch order."
        )
