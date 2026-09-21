"""Round-trip lifecycle tests (in-memory backend).

Exercises multi-transition sequences on the in-memory backend to catch
interactions between transitions that per-transition unit tests do not -
e.g. attempt-counter behaviour across snooze cycles, ordering of state
changes through the wake tick. Uses ``run_until_drained`` for clock
auto-advance on the wake tick.
"""

from datetime import UTC, datetime, timedelta

from taskq.backend._protocol import JobId
from taskq.exceptions import RetryAfter, Snooze
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


async def _extract_state_change_transitions(
    backend: InMemoryBackend, job_id: JobId
) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for e in await backend.get_events(job_id):
        if e.kind != "state_change":
            continue
        from_state = e.detail.get("from_state")
        to_state = e.detail.get("to_state")
        if isinstance(from_state, str) and isinstance(to_state, str):
            result.append((from_state, to_state))
    return result


# ── full snooze round-trip ──────────────────────────────────────


async def test_full_snooze_round_trip() -> None:
    """Full snooze round-trip: enqueue → dispatch → Snooze → scheduled_to_pending → dispatch → succeed.

    The snooze refunds the claim's attempt increment, so one snooze
    cycle + one final successful dispatch leaves the attempt at 1 - the
    deferral never walks the counter. A deferred work unit that did not
    execute does not consume an attempt slot.
    """
    backend = _make_backend()

    call_count = 0

    def snooze_then_succeed(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Snooze(timedelta(seconds=30))
        return {"ok": True}

    backend.register_stub("test_actor", snooze_then_succeed)

    args = make_enqueue_args(payload={}, max_attempts=10, scheduled_at=_START)
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert call_count == 2
    assert row.attempt == 1

    attempts = await backend.get_attempts(args.id)
    # The snooze wrote no attempt row (a deferral is not an execution);
    # the only record is the terminal success at the re-claimed attempt.
    assert len(attempts) == 1
    assert attempts[0].outcome == "succeeded"
    assert attempts[0].attempt == 1

    # The snooze's row transition writes no event, and neither the claims
    # nor the wake promotion write one (all scheduler/dispatcher
    # bookkeeping, not outcome transitions): the terminal exit is the
    # transition of record.
    transitions = await _extract_state_change_transitions(backend, args.id)
    assert transitions == [
        ("running", "succeeded"),
    ]
    assert row.snooze_count == 1
    # The row's snooze_count column is the deferral's ONLY counter - no
    # metadata mirror is merged alongside it (one source of truth).
    assert "snooze_count" not in (row.metadata or {})


# ── multiple snooze cycles (3x) ─────────────────────────────────


async def test_multiple_snooze_cycles() -> None:
    """Multiple snooze cycles (3x): each snooze refunds the claim's
    increment and each re-dispatch re-claims it, so after three snooze
    cycles + one final successful dispatch the attempt is 1 (the
    deferral is unbounded and never walks the counter)."""
    backend = _make_backend()

    call_count = 0

    def snooze_three_then_succeed(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            raise Snooze(timedelta(seconds=30))
        return {"ok": True}

    backend.register_stub("test_actor", snooze_three_then_succeed)

    args = make_enqueue_args(payload={}, max_attempts=10, scheduled_at=_START)
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert call_count == 4
    assert row.attempt == 1

    attempts = await backend.get_attempts(args.id)
    # The three snoozes wrote no attempt rows; the coalesced snooze
    # counter on the row is their record.
    succeeded = [a for a in attempts if a.outcome == "succeeded"]
    assert len(succeeded) == 1
    assert len(attempts) == 1
    assert row.snooze_count == 3

    transitions = await _extract_state_change_transitions(backend, args.id)
    assert transitions == [
        ("running", "succeeded"),
    ]


# ── RetryAfter round-trip ──────────────────────────────────────


async def test_retry_after_round_trip() -> None:
    """RetryAfter round-trip: enqueue → dispatch → RetryAfter → wake → dispatch → succeed. Attempt incremented at each RetryAfter."""
    backend = _make_backend()

    call_count = 0

    def retry_then_succeed(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RetryAfter(timedelta(seconds=5), consume_budget=True)
        return {"ok": True}

    backend.register_stub("test_actor", retry_then_succeed)

    args = make_enqueue_args(payload={}, max_attempts=10, scheduled_at=_START)
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert call_count == 2
    assert row.attempt == 2

    attempts = await backend.get_attempts(args.id)
    assert len(attempts) == 2

    retry_attempt = next(a for a in attempts if a.outcome == "snoozed")
    assert retry_attempt.error_class == "RetryAfter"
    assert retry_attempt.attempt == 1

    succeeded_attempt = next(a for a in attempts if a.outcome == "succeeded")
    assert succeeded_attempt.attempt == 2

    transitions = await _extract_state_change_transitions(backend, args.id)
    assert transitions == [
        ("running", "scheduled"),
        ("running", "succeeded"),
    ]


# ── indefinite retry polling pattern ────────────────


async def test_indefinite_retry_polling_pattern() -> None:
    """(unit version). Polling pattern: enqueue → dispatch → Snooze(30s) → wake tick → dispatch → succeed.

    The snooze refunds the claim's increment, so the polling pattern's
    terminal attempt is 1 - one consuming dispatch, no matter how many
    snooze cycles preceded it.
    """
    backend = _make_backend()

    call_count = 0

    def snooze_30_then_succeed(payload: object, ctx: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Snooze(timedelta(seconds=30))
        return {"ok": True}

    backend.register_stub("test_actor", snooze_30_then_succeed)

    args = make_enqueue_args(payload={}, max_attempts=10, scheduled_at=_START)
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded"
    assert call_count == 2
    assert row.attempt == 1

    # The snooze's row transition writes no event row (see
    # test_full_snooze_round_trip); the terminal exit is the only
    # transition of record.
    transitions = await _extract_state_change_transitions(backend, args.id)
    assert transitions == [
        ("running", "succeeded"),
    ]


# ── cancel mid-snooze ────────────────────────────────────────────


async def test_cancel_mid_snooze() -> None:
    """Cancel mid-snooze: enqueue → dispatch → Snooze → cancel(scheduled job) → cancelled."""
    backend = _make_backend()
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement - candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")

    args = make_enqueue_args(payload={}, max_attempts=10, scheduled_at=_START)
    await backend.enqueue(args)

    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access for dispatch_batch

    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) == 1
    job = dispatched[0]
    assert job.status == "running"
    assert job.attempt == 1

    result = await backend.mark_snoozed(
        job.id, worker_id, delay=timedelta(seconds=30), attempt=1, claim_epoch=1
    )
    assert result == "scheduled"

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "scheduled"
    # The refund: the claim's increment (dispatch 0 → 1) is returned.
    assert row.attempt == 0

    ok = await backend.write_cancel_request(args.id, reason="user")
    assert ok is True

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "cancelled"
    assert row.finished_at is not None

    dispatched_after = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched_after) == 0

    transitions = await _extract_state_change_transitions(backend, args.id)
    # The snooze's row transition writes no event row, and a claim writes
    # none either; the sequence records the cancel path's
    # scheduled→cancelled audit entry and nothing else.
    assert transitions == [
        ("scheduled", "cancelled"),
    ]

    events = await backend.get_events(args.id)
    cancel_requests = [e for e in events if e.kind == "cancel_request"]
    assert len(cancel_requests) == 1

    state_changes = [e for e in events if e.kind == "state_change"]
    cancel_sc = [e for e in state_changes if e.detail.get("to_state") == "cancelled"]
    assert len(cancel_sc) == 1
    assert cancel_sc[0].detail["from_state"] == "scheduled"
