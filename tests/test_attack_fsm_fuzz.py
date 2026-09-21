"""Property-fuzzing attack on the job STATE MACHINE (lane atk/fsm-properties).

A :class:`~hypothesis.stateful.RuleBasedStateMachine` drives the in-memory
twin through random operation sequences - enqueue (plain, idempotency-keyed,
unique-for, future-scheduled), batch enqueue, dispatch claims, heartbeats
(and disowns), every terminal/deferral write (succeed, fail, fail-retry,
retry-after consuming and not, snooze, admission denial, interrupt, worker
cancel, operator cancel request / escalation / abandon), operator re-runs,
the three sweeps, and the twin's archive/expiry simulation - and checks the
invariants after EVERY step:

1. CONSERVATION: every enqueued id stays live, archived (with truthful
   columns), or expired-from-archive; terminal rows carry ``finished_at``;
   per (job, attempt) at most one attempt row (the claim-clamped epoch's
   single record, the ``ON CONFLICT DO NOTHING`` doctrine); a terminal
   ``state_change`` event is the last event of its epoch.
2. FENCE COHERENCE: a terminal write presenting a view that mismatches the
   row's current ``(attempt, claim_epoch)`` - or a row that is not running
   at all - no-ops: same status, same counters, no new attempt rows, no
   new events.  An armed cancel is never erased by a deferral (the
   cancel-first arms terminalise 'cancelled' and keep the trail).
3. ORDER: ``progress_seq`` never regresses on the durable row under the
   worker protocol's monotone presentation; row-status transitions stay
   on the documented machine's edges.

Liveness (no injected failures, every run settles) and the twin ≡ PG
differential (serial-order plans through the DiffSide harness, in
``test_attack_fsm_fuzz_pg.py``) run as separate properties.

Fence-presentation discipline: every worker-side write presents the
``(attempt, claim_epoch)`` pair read off a claim view - the dispatch
result's row for the fresh rules, an EARLIER captured view for the stale
rules.  A stale-view no-op is the fence working, and the stale rules
ASSERT the no-op rather than merely tolerating it.
"""

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, precondition, rule

from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.backend._protocol import (
    CancelPhase,
    EnqueueArgs,
    ErrorInfo,
    JobId,
    JobRow,
)
from taskq.backend.statemachine import TERMINAL_STATUSES, VALID_TRANSITIONS
from taskq.exceptions import SingletonCollisionError, WorkerOwnershipMismatch
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

# ── Constants ──────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)
_LEASE = timedelta(seconds=60)
_ACTOR = "fuzz_actor"

# The documented ROW-level transition machine: VALID_TRANSITIONS plus the
# operator re-run edge (retry_job's docstring: "every state a job can come
# to rest in is a valid source", including succeeded and abandoned).
# retry_job re-pends the row WITHOUT a state_change event, so this union
# governs the row's transitions, not the event trail's.
_ROW_EDGES: dict[str, frozenset[str]] = {
    source: frozenset(targets | ({"pending"} if source in TERMINAL_STATUSES else set()))
    for source, targets in VALID_TRANSITIONS.items()
}


def _is_terminal(status: str) -> bool:
    return status in TERMINAL_STATUSES


# ── Shadow model ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class _View:
    """A handler's claim view: the fence triple a terminal write presents."""

    worker: UUID
    attempt: int
    claim_epoch: int


@dataclass
class _Shadow:
    """What the fuzz has OBSERVED about one job.

    ``event_watermark`` is the twin's global event-id high-water mark at
    the job's current epoch start (reset by an operator re-run, the one
    documented transition that writes no event).
    """

    last_status: str
    last_progress: int
    last_phase: int
    last_attempt: int
    event_watermark: int


# ── The twin state machine ─────────────────────────────────────────────


@settings(max_examples=60, stateful_step_count=30, deadline=None)
class FsmFuzzMachine(RuleBasedStateMachine):
    """Random operation sequences against the twin; invariants every step."""

    jobs = Bundle("jobs")

    def __init__(self) -> None:
        super().__init__()
        self.clock = FakeClock(_START)
        self.backend = InMemoryBackend(
            clock=self.clock,
            cancellation_grace_period=_GRACE,
            cleanup_grace_period=_GRACE,
        )
        self.backend.register_actor_configs(
            [ActorConfig(actor=_ACTOR, max_concurrent=None, max_pending=None, queue="default")]
        )
        self.workers: tuple[UUID, UUID] = (new_uuid(), new_uuid())
        self.shadows: dict[JobId, _Shadow] = {}
        self.views: dict[JobId, list[_View]] = {}
        self.expired: set[JobId] = set()
        self.seq = 0

    # ── Helpers ────────────────────────────────────────────────────────

    def _track(self, jid: JobId, row: JobRow) -> None:
        """Register a job never seen before."""
        self.shadows[jid] = _Shadow(
            last_status=row.status,
            last_progress=row.progress_seq,
            last_phase=int(row.cancel_phase),
            last_attempt=row.attempt,
            event_watermark=self._max_event_id(),
        )
        self.views[jid] = []

    def _observe(self, jid: JobId, row: JobRow) -> None:
        """Merge a freshly observed row into the shadow, asserting the
        row-level transition legality and the attempt-delta contract."""
        shadow = self.shadows.get(jid)
        if shadow is None:
            self._track(jid, row)
            return
        if row.status != shadow.last_status:
            assert row.status in _ROW_EDGES[shadow.last_status], (
                f"illegal row transition {shadow.last_status} -> {row.status}"
            )
            shadow.last_status = row.status
        delta = row.attempt - shadow.last_attempt
        assert delta in (0, 1, -1), f"attempt jumped by {delta}"
        shadow.last_attempt = row.attempt
        shadow.last_phase = max(shadow.last_phase, int(row.cancel_phase))

    def _max_event_id(self) -> int:
        events = self.backend._events  # pyright: ignore[reportPrivateUsage]  # Why: test-only epoch watermark; the established same-suite pattern (test_rt_diff_harness.py).
        return events[-1].event_id if events else 0

    def _view_for(self, row: JobRow) -> _View:
        """The claim view a real handler reads off its in-hand row."""
        assert row.locked_by_worker is not None
        return _View(
            worker=row.locked_by_worker,
            attempt=row.attempt,
            claim_epoch=row.claim_epoch,
        )

    def _present_seq(self) -> int:
        """The worker protocol presents a monotonically rising progress seq."""
        self.seq += 1
        return self.seq

    def _new_args(
        self,
        *,
        variant: Literal["plain", "keyed", "unique", "future"],
        key_pool: int,
        max_attempts: int,
        retry_kind: str,
    ) -> EnqueueArgs:
        now = self.clock.now()
        jid = new_job_id()
        scheduled_at = now + timedelta(minutes=5) if variant == "future" else None
        return EnqueueArgs(
            id=jid,
            actor=_ACTOR,
            queue="default",
            payload={"n": 1},
            max_attempts=max_attempts,
            retry_kind=retry_kind,  # type: ignore[arg-type]  # Why: sampled literals are known-valid RetryKind values.
            scheduled_at=scheduled_at,
            idempotency_key=(f"k{key_pool}" if variant == "keyed" else None),  # type: ignore[arg-type]  # Why: IdempotencyKey is a NewType over str.
            identity_key=(f"u{key_pool}" if variant == "unique" else None),  # type: ignore[arg-type]  # Why: IdentityKey is a NewType over str.
            unique_for=timedelta(seconds=30) if variant == "unique" else None,
            schedule_to_close=now + timedelta(seconds=90) if variant == "future" else None,
        )

    def _register(self, row: JobRow) -> JobId:
        jid = JobId(row.id)
        if jid in self.shadows:
            # An idempotency dedup handed back an existing holder row:
            # observe it, never reset its shadow (a reset would launder
            # the job's trail past the invariants).
            self._observe(jid, row)
        else:
            self._track(jid, row)
        return jid

    # ── Enqueue rules ──────────────────────────────────────────────────

    @rule(
        target=jobs,
        variant=st.sampled_from(["plain", "keyed", "unique", "future"]),
        key_pool=st.integers(0, 3),
        max_attempts=st.sampled_from([1, 2, 3, 5]),
        retry_kind=st.sampled_from(["transient", "non_retryable", "indefinite"]),
    )
    def enqueue(self, variant: str, key_pool: int, max_attempts: int, retry_kind: str) -> JobId:
        args = self._new_args(
            variant=variant,  # type: ignore[arg-type]  # Why: the sampled_from list is exactly this Literal's values.
            key_pool=key_pool,
            max_attempts=max_attempts,
            retry_kind=retry_kind,
        )
        try:
            row = asyncio.run(self.backend.enqueue(args))
        except SingletonCollisionError:
            # A unique-for collision on a reused identity key: admission
            # backpressure, no row.  Fall back to a fresh plain job so the
            # bundle never receives an id without a row behind it.
            return self._fallback_enqueue()
        return self._register(row)

    def _fallback_enqueue(self) -> JobId:
        args = self._new_args(variant="plain", key_pool=0, max_attempts=3, retry_kind="transient")
        row = asyncio.run(self.backend.enqueue(args))
        return self._register(row)

    @rule(target=jobs, data=st.lists(st.integers(min_value=1, max_value=3), min_size=2, max_size=3))
    def enqueue_batch(self, data: list[int]) -> JobId:
        args_list = [
            self._new_args(variant="plain", key_pool=n, max_attempts=n, retry_kind="transient")
            for n in data
        ]
        rows = asyncio.run(self.backend.enqueue_batch(args_list))
        registered = [self._register(row) for row in rows]
        # Every batch row is tracked and invariant-checked; the bundle
        # carries the first so later rules can drive it.
        return registered[0]

    # ── Dispatch / heartbeat / sweeps / clock ──────────────────────────

    @rule(worker_idx=st.integers(0, 1), limit=st.integers(1, 3))
    def dispatch(self, worker_idx: int, limit: int) -> None:
        rows = asyncio.run(
            self.backend.dispatch_batch(self.workers[worker_idx], ["default"], limit, _LEASE)
        )
        for row in rows:
            jid = JobId(row.id)
            self._observe(jid, row)
            self.views.setdefault(jid, []).append(self._view_for(row))

    @rule(worker_idx=st.integers(0, 1), disown=st.booleans())
    def heartbeat(self, worker_idx: int, disown: bool) -> None:
        worker = self.workers[worker_idx]
        disowned: set[JobId] = set()
        if disown:
            for jid, history in self.views.items():
                if history and history[-1].worker == worker:
                    disowned.add(jid)
        asyncio.run(self.backend.heartbeat_jobs(worker, _LEASE, disowned=disowned))

    @rule(seconds=st.sampled_from([1, 5, 45, 120, 3600, 90000]))
    def advance(self, seconds: int) -> None:
        self.clock.advance(timedelta(seconds=seconds))

    @rule()
    def sweep_reclaim(self) -> None:
        asyncio.run(self.backend.reclaim_expired_locks(_GRACE, _GRACE))

    @rule()
    def sweep_deadline(self) -> None:
        asyncio.run(self.backend.deadline_sweep())

    @rule()
    def sweep_promote(self) -> None:
        asyncio.run(self.backend.scheduled_to_pending())

    # ── Operator cancel ladder and re-run ──────────────────────────────

    @rule(job=jobs)
    def cancel_request(self, job: JobId) -> None:
        asyncio.run(self.backend.write_cancel_request(job, "operator"))

    @rule(job=jobs, worker_idx=st.integers(0, 1))
    def cancel_escalate(self, job: JobId, worker_idx: int) -> None:
        asyncio.run(self.backend.write_cancel_escalation(job, self.workers[worker_idx], 2))

    @rule(job=jobs)
    def abandon(self, job: JobId) -> None:
        asyncio.run(self.backend.mark_abandoned(job))

    @rule(job=jobs)
    def operator_retry(self, job: JobId) -> None:
        landed = asyncio.run(self.backend.retry_job(job))
        if not landed:
            return
        row = asyncio.run(self.backend.get(job))
        assert row is not None
        shadow = self.shadows[job]
        shadow.last_status = row.status
        shadow.last_phase = int(row.cancel_phase)
        shadow.last_attempt = row.attempt
        shadow.event_watermark = self._max_event_id()

    # ── Worker terminal / deferral writes, fresh fences ────────────────

    @rule(job=jobs, worker_idx=st.integers(0, 1))
    def succeed(self, job: JobId, worker_idx: int) -> None:
        worker = self.workers[worker_idx]
        row = asyncio.run(self.backend.get(job))
        if row is None or row.status != "running" or row.locked_by_worker != worker:
            return
        view = self._view_for(row)
        landed = asyncio.run(
            self.backend.mark_succeeded(
                job,
                worker,
                {"ok": True},
                self._present_seq(),
                attempt=view.attempt,
                claim_epoch=view.claim_epoch,
            )
        )
        assert landed, "a fence-admitted success write must land"
        post = asyncio.run(self.backend.get(job))
        assert post is not None and post.status == "succeeded"
        self._observe(job, post)

    @rule(job=jobs, worker_idx=st.integers(0, 1), retry_s=st.sampled_from([None, 0, 30]))
    def fail(self, job: JobId, worker_idx: int, retry_s: int | None) -> None:
        worker = self.workers[worker_idx]
        row = asyncio.run(self.backend.get(job))
        if row is None or row.status != "running" or row.locked_by_worker != worker:
            return
        view = self._view_for(row)
        try:
            post = asyncio.run(
                self.backend.mark_failed_or_retry(
                    job,
                    worker,
                    ErrorInfo(error_class="E", error_message="m", error_traceback=None),
                    None if retry_s is None else timedelta(seconds=retry_s),
                    self._present_seq(),
                    attempt=view.attempt,
                    claim_epoch=view.claim_epoch,
                )
            )
        except WorkerOwnershipMismatch:
            # Documented: the failure-RETRY arm refuses a phase-carrying
            # row (an operator cancel in flight wins over a budget spend);
            # the row must be exactly where it stood.
            assert row.cancel_phase != CancelPhase.NONE
            assert asyncio.run(self.backend.get(job)) == row
            return
        if retry_s is None:
            assert _is_terminal(post.status)
        else:
            assert post.status in ("scheduled", "pending", "failed")
        self._observe(job, post)

    @rule(
        job=jobs,
        worker_idx=st.integers(0, 1),
        delay_s=st.sampled_from([0, 30]),
        consume=st.booleans(),
    )
    def retry_after(self, job: JobId, worker_idx: int, delay_s: int, consume: bool) -> None:
        worker = self.workers[worker_idx]
        row = asyncio.run(self.backend.get(job))
        if row is None or row.status != "running" or row.locked_by_worker != worker:
            return
        view = self._view_for(row)
        res = asyncio.run(
            self.backend.mark_retry_after(
                job,
                worker,
                timedelta(seconds=delay_s),
                consume_budget=consume,
                progress_seq=self._present_seq(),
                attempt=view.attempt,
                claim_epoch=view.claim_epoch,
            )
        )
        post = asyncio.run(self.backend.get(job))
        assert post is not None
        if res != "noop" and post.status in ("pending", "scheduled"):
            # A landed deferral re-pends the row; it must never carry an
            # armed cancel into the reset (the cancel-first arms own the
            # phase-carrying rows and read back "noop").
            assert post.cancel_phase == CancelPhase.NONE, "deferral reset an armed cancel"
            assert post.locked_by_worker is None
            if not consume:
                # The documented deferral refund: the claim's attempt
                # increment is given back, floored at 0, counted on the row.
                assert post.attempt == max(row.attempt - 1, 0)
                assert post.snooze_count == row.snooze_count + 1
        if res == "noop" and post.status == "running":
            # No arm landed: the row is either untouched or the caller was
            # fenced out (a phase-carrying row stays 'running' with its
            # cancel armed - FENCE COHERENCE).
            assert row == post or post.cancel_phase != CancelPhase.NONE
        self._observe(job, post)

    @rule(
        job=jobs,
        worker_idx=st.integers(0, 1),
        outcome=st.sampled_from(["snoozed", "reservation_denied", "rate_limit_denied"]),
    )
    def snooze(self, job: JobId, worker_idx: int, outcome: str) -> None:
        worker = self.workers[worker_idx]
        row = asyncio.run(self.backend.get(job))
        if row is None or row.status != "running" or row.locked_by_worker != worker:
            return
        view = self._view_for(row)
        res = asyncio.run(
            self.backend.mark_snoozed(
                job,
                worker,
                timedelta(seconds=0),
                outcome=outcome,  # type: ignore[arg-type]  # Why: the sampled list is exactly SnoozeOutcome's values.
                progress_seq=self._present_seq(),
                attempt=view.attempt,
                claim_epoch=view.claim_epoch,
            )
        )
        post = asyncio.run(self.backend.get(job))
        assert post is not None
        if res == "scheduled":
            assert post.status in ("scheduled", "pending")
            assert post.attempt == max(row.attempt - 1, 0)
            assert post.cancel_phase == CancelPhase.NONE, "deferral reset an armed cancel"
        if res == "failed":
            assert _is_terminal(post.status)
        if res == "noop" and post.status == "running":
            assert row == post or post.cancel_phase != CancelPhase.NONE
        self._observe(job, post)

    @rule(job=jobs, worker_idx=st.integers(0, 1))
    def interrupt(self, job: JobId, worker_idx: int) -> None:
        worker = self.workers[worker_idx]
        row = asyncio.run(self.backend.get(job))
        if row is None or row.status != "running" or row.locked_by_worker != worker:
            return
        view = self._view_for(row)
        res = asyncio.run(
            self.backend.mark_interrupted(
                job,
                worker,
                attempt=view.attempt,
                hold=timedelta(seconds=0),
                progress_seq=self._present_seq(),
                claim_epoch=view.claim_epoch,
            )
        )
        post = asyncio.run(self.backend.get(job))
        assert post is not None
        if res == "noop":
            if post.status == "running":
                assert row == post or post.cancel_phase != CancelPhase.NONE
        else:
            assert post.status in ("pending", "scheduled", "failed", "cancelled")
            assert post.locked_by_worker is None
            assert post.interrupt_count == row.interrupt_count + 1
        self._observe(job, post)

    @rule(job=jobs, worker_idx=st.integers(0, 1))
    def worker_cancel(self, job: JobId, worker_idx: int) -> None:
        worker = self.workers[worker_idx]
        row = asyncio.run(self.backend.get(job))
        if row is None or row.status != "running" or row.locked_by_worker != worker:
            return
        view = self._view_for(row)
        landed = asyncio.run(
            self.backend.mark_cancelled(
                job,
                worker,
                self._present_seq(),
                attempt=view.attempt,
                claim_epoch=view.claim_epoch,
            )
        )
        assert landed
        post = asyncio.run(self.backend.get(job))
        assert post is not None and post.status == "cancelled"
        assert post.finished_at is not None
        self._observe(job, post)

    # ── STALE-VIEW writes: the fence must no-op ────────────────────────

    @precondition(lambda self: any(self.views.values()))
    @rule(
        job=jobs,
        idx=st.integers(0, 3),
        which=st.sampled_from(["succeed", "fail", "retry_after", "snooze", "interrupt", "cancel"]),
    )
    def stale_write(self, job: JobId, idx: int, which: str) -> None:
        history = self.views.get(job, [])
        if not history:
            return
        view = history[idx % len(history)]
        worker = view.worker
        pre = asyncio.run(self.backend.get(job))
        if pre is None:
            return
        events_before = len(asyncio.run(self.backend.get_events(job)))
        attempts_before = len(asyncio.run(self.backend.get_attempts(job)))
        fenced_out = (
            pre.status != "running"
            or pre.locked_by_worker != worker
            or pre.attempt != view.attempt
            or pre.claim_epoch != view.claim_epoch
        )
        res: object
        if which == "succeed":
            res = asyncio.run(
                self.backend.mark_succeeded(
                    job, worker, attempt=view.attempt, claim_epoch=view.claim_epoch
                )
            )
        elif which == "fail":
            try:
                asyncio.run(
                    self.backend.mark_failed_or_retry(
                        job,
                        worker,
                        ErrorInfo(error_class="E", error_message="m", error_traceback=None),
                        timedelta(seconds=30),
                        attempt=view.attempt,
                        claim_epoch=view.claim_epoch,
                    )
                )
                res = "landed"
            except WorkerOwnershipMismatch:
                res = "noop"
        elif which == "retry_after":
            res = asyncio.run(
                self.backend.mark_retry_after(
                    job,
                    worker,
                    timedelta(seconds=30),
                    attempt=view.attempt,
                    claim_epoch=view.claim_epoch,
                )
            )
        elif which == "snooze":
            res = asyncio.run(
                self.backend.mark_snoozed(
                    job,
                    worker,
                    timedelta(seconds=30),
                    attempt=view.attempt,
                    claim_epoch=view.claim_epoch,
                )
            )
        elif which == "interrupt":
            res = asyncio.run(
                self.backend.mark_interrupted(
                    job,
                    worker,
                    attempt=view.attempt,
                    hold=timedelta(seconds=30),
                    claim_epoch=view.claim_epoch,
                )
            )
        else:
            res = asyncio.run(
                self.backend.mark_cancelled(
                    job, worker, attempt=view.attempt, claim_epoch=view.claim_epoch
                )
            )
        post = asyncio.run(self.backend.get(job))
        assert post is not None
        events_after = len(asyncio.run(self.backend.get_events(job)))
        attempts_after = len(asyncio.run(self.backend.get_attempts(job)))
        if not fenced_out:
            return  # The view was still current; the fresh rules pin the landing.
        # FENCE COHERENCE: a stale view must not terminalise a live row.
        # Every fenced-out arm must read back as a no-op and leave the row
        # exactly where it stood - no status move, no counter bump, no
        # attempt row, no event.
        if which in ("succeed", "cancel"):
            landed = res is True
        elif which == "fail":
            landed = res == "landed"
        else:
            landed = res != "noop"
        assert not landed, f"stale {which} write landed: view={view!r} res={res!r}"
        assert post == pre
        assert events_after == events_before
        assert attempts_after == attempts_before
        # A fenced-out row keeps its armed cancel visible to the ladder:
        # the worker's cancel poll still sees the phase the stale write
        # was correctly refused to touch.
        if pre.status == "running" and pre.cancel_phase > CancelPhase.NONE:
            flags = asyncio.run(
                self.backend.poll_cancel_flags(pre.locked_by_worker)  # type: ignore[arg-type]  # Why: a claimed row's lock holder is a UUID by construction.
            )
            assert any(f.job_id == job for f in flags), (
                "armed cancel invisible to the ladder after a fenced-out write"
            )

    @rule(job=jobs, worker_idx=st.integers(0, 1))
    def none_fence_write(self, job: JobId, worker_idx: int) -> None:
        """A caller that cannot present the epoch (``attempt=None`` or
        ``claim_epoch=None``) must never terminalise a live row."""
        worker = self.workers[worker_idx]
        pre = asyncio.run(self.backend.get(job))
        if pre is None or pre.status != "running" or pre.locked_by_worker != worker:
            return
        events_before = len(asyncio.run(self.backend.get_events(job)))
        attempts_before = len(asyncio.run(self.backend.get_attempts(job)))
        landed = asyncio.run(
            self.backend.mark_succeeded(job, worker, attempt=None, claim_epoch=None)
        )
        assert landed is False, "an unpresentable epoch must not terminalise a live row"
        try:
            asyncio.run(
                self.backend.mark_failed_or_retry(
                    job,
                    worker,
                    ErrorInfo(error_class="E", error_message="m", error_traceback=None),
                    timedelta(seconds=30),
                    attempt=None,
                )
            )
            raise AssertionError("mark_failed_or_retry with attempt=None must raise Mismatch")
        except WorkerOwnershipMismatch:
            pass
        post = asyncio.run(self.backend.get(job))
        assert post is not None
        assert post == pre
        assert len(asyncio.run(self.backend.get_events(job))) == events_before
        assert len(asyncio.run(self.backend.get_attempts(job))) == attempts_before

    # ── Archive / expiry (the twin's prune-family simulation) ──────────

    @rule(retention_s=st.sampled_from([0, 3600]), archive_s=st.sampled_from([3600, 100000]))
    def archive(self, retention_s: int, archive_s: int) -> None:
        self.backend.archive_terminal_jobs(
            timedelta(seconds=retention_s), timedelta(seconds=archive_s)
        )

    @rule()
    def expire_archived(self) -> None:
        still_there = set(self.backend._archive.keys())  # pyright: ignore[reportPrivateUsage]  # Why: test-only archive enumeration; the established same-suite pattern.
        self.backend.expire_archived_jobs()
        self.expired |= still_there - set(self.backend._archive.keys())  # pyright: ignore[reportPrivateUsage]  # Why: same pattern.

    # ── Invariants (checked after EVERY step) ──────────────────────────

    @invariant()
    def conservation_and_order(self) -> None:
        asyncio.run(self._check_all())

    async def _check_all(self) -> None:
        for jid, shadow in self.shadows.items():
            row = await self.backend.get(jid)
            if row is None:
                archived = await self.backend.get_archived(jid)
                if archived is None:
                    # Only a documented archive expiry may delete a row.
                    assert jid in self.expired, f"job {jid} vanished without an archive trail"
                else:
                    arow = archived.row
                    assert _is_terminal(arow.status), "archived a non-terminal row"
                    assert arow.finished_at is not None, "archived row without finished_at"
                continue
            # Row-level transition legality (documented machine + re-run);
            # sweeps move rows outside any rule, so this re-checks here.
            if row.status != shadow.last_status:
                assert row.status in _ROW_EDGES[shadow.last_status], (
                    f"illegal row transition {shadow.last_status} -> {row.status}"
                )
                shadow.last_status = row.status
            # Truthful terminal columns.
            assert (row.finished_at is not None) == _is_terminal(row.status)
            # progress_seq never regresses under monotone presentation.
            assert row.progress_seq >= shadow.last_progress, (
                f"progress_seq regressed: {shadow.last_progress} -> {row.progress_seq}"
            )
            shadow.last_progress = row.progress_seq
            # Attempt deltas for sweep-driven changes (rule-driven ones are
            # asserted in _observe): +1 per claim/reclaim, -1 on a
            # documented deferral refund, never anything else.
            delta = row.attempt - shadow.last_attempt
            assert delta in (0, 1, -1), f"attempt jumped by {delta}"
            shadow.last_attempt = row.attempt
            # The armed cancel is never erased from a live row: only the
            # operator re-run starts an epoch clean (and it rewrites the
            # shadow), so any phase the fuzz has seen must still be armed.
            if shadow.last_phase > 0 and not _is_terminal(row.status):
                assert int(row.cancel_phase) >= 1, "armed cancel erased from a live row"
                assert row.cancel_requested_at is not None, "armed cancel trail erased"
            shadow.last_phase = max(shadow.last_phase, int(row.cancel_phase))
            # Attempt rows: at most one per (job, attempt) - the
            # claim-clamped epoch's single record.
            attempts = await self.backend.get_attempts(jid)
            seen: set[int] = set()
            for a in attempts:
                assert a.attempt not in seen, f"duplicate attempt row for epoch {a.attempt}"
                seen.add(a.attempt)
            # Event trail: legal edges only; a terminal state_change event
            # is the last event of its epoch (operator re-runs write no
            # event and start a new epoch above the watermark).
            events = await self.backend.get_events(jid)
            terminal_seen = False
            for e in events:
                if e.event_id <= shadow.event_watermark or e.kind != "state_change":
                    continue
                frm = e.detail.get("from_state")
                to = e.detail.get("to_state")
                assert isinstance(frm, str) and isinstance(to, str)
                assert to in VALID_TRANSITIONS[frm], f"illegal event edge {frm} -> {to}"
                assert not terminal_seen, "state_change after a terminal event"
                if _is_terminal(to):
                    terminal_seen = True


TestFsmFuzzMachine = FsmFuzzMachine.TestCase  # type: ignore[reportUnknownVariableType]  # Why: hypothesis generates the TestCase type dynamically; pyright cannot infer it.


# ── Shrunk regression pin: the ceiling's claim-clamped repeat epoch ────


def test_regression_ceiling_repeat_keeps_first_attempt_row() -> None:
    """INVARIANT (conservation, the attempt ledger): at the smallint
    ceiling the claim's ``LEAST(attempt + 1, 32767)`` clamp repeats the
    attempt number, and the ``(job_id, attempt)`` key keeps its FIRST
    record - the ``ON CONFLICT DO NOTHING`` doctrine every PG attempt
    INSERT carries.  Shrunken from the differential counterexample in
    ``test_attack_fsm_fuzz_pg.py`` (plant at 32767 -> reclaim -> claim ->
    consuming RetryAfter -> claim -> succeed): the twin's terminal and
    deferral writers used to append duplicates PG refused to store, so
    the mirror's attempt ledger disagreed with the contract source's."""
    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
    )
    backend.register_actor_configs(
        [ActorConfig(actor=_ACTOR, max_concurrent=None, max_pending=None, queue="default")]
    )
    worker = new_uuid()
    jid = new_job_id()
    backend._jobs[jid] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only planting at the fuzz-unreachable ceiling; the established pattern (test_rt_sweeps_parity.py).
        make_job_row(
            attempt=32767,
            max_attempts=1,
            retry_kind="indefinite",
            status="running",
            actor=_ACTOR,
        ),
        id=jid,
        created_at=_START,
        scheduled_at=_START,
        started_at=_START,
        locked_by_worker=worker,
        lock_expires_at=_START - timedelta(seconds=1),
    )

    # Reclaim: the expired lock's epoch crashes (the 'crashed' record -
    # the FIRST record of the 32767 key) and the row re-queues.
    assert asyncio.run(backend.reclaim_expired_locks(_GRACE, _GRACE)) == 1
    clock.advance(timedelta(hours=2))  # the crash backoff elapses
    asyncio.run(backend.scheduled_to_pending())

    # Claim: the clamp repeats the attempt number, the claim epoch moves.
    claimed = asyncio.run(backend.dispatch_batch(worker, ["default"], 5, _LEASE))
    assert [row.id for row in claimed] == [jid]
    assert claimed[0].attempt == 32767
    first_epoch = claimed[0].claim_epoch

    # A consuming RetryAfter re-pends the row and writes the epoch's
    # attempt row ('snoozed') - which must NOT land: the key is taken.
    res = asyncio.run(
        backend.mark_retry_after(
            jid,
            worker,
            timedelta(seconds=0),
            consume_budget=True,
            attempt=claimed[0].attempt,
            claim_epoch=first_epoch,
        )
    )
    assert res == "scheduled"

    # Claim again at the SAME clamped number under a fresh epoch and
    # terminalise - the 'succeeded' attempt row must not land either.
    claimed2 = asyncio.run(backend.dispatch_batch(worker, ["default"], 5, _LEASE))
    assert [row.id for row in claimed2] == [jid]
    assert claimed2[0].attempt == 32767
    assert claimed2[0].claim_epoch != first_epoch
    assert asyncio.run(
        backend.mark_succeeded(
            jid,
            worker,
            {"ok": True},
            attempt=claimed2[0].attempt,
            claim_epoch=claimed2[0].claim_epoch,
        )
    )

    # The invariant, deterministic: one attempt row per (job, attempt);
    # the first record of the repeated key survives, its successors drop.
    attempts = asyncio.run(backend.get_attempts(jid))
    numbers = [a.attempt for a in attempts]
    assert len(numbers) == len(set(numbers)), f"duplicate attempt rows: {numbers}"
    assert [a.outcome for a in attempts] == ["crashed"]


# ── Liveness: no injected failures, every run settles ──────────────────


@settings(max_examples=60, deadline=None)
@given(
    n_jobs=st.integers(1, 6),
    churn=st.lists(
        st.sampled_from(["fail_terminal", "fail_retry", "retry_after", "snooze", "interrupt"]),
        max_size=12,
    ),
    with_deadline=st.booleans(),
)
def test_liveness_no_injected_failures_settles(
    n_jobs: int, churn: list[str], with_deadline: bool
) -> None:
    """With the fences honestly presented and no injected faults, a bounded
    round count drains every job to terminal or live-claimed."""
    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
    )
    backend.register_actor_configs(
        [ActorConfig(actor=_ACTOR, max_concurrent=None, max_pending=None, queue="default")]
    )
    worker = new_uuid()
    now = clock.now()

    async def build() -> list[JobId]:
        ids: list[JobId] = []
        for i in range(n_jobs):
            row = await backend.enqueue(
                EnqueueArgs(
                    id=new_job_id(),
                    actor=_ACTOR,
                    queue="default",
                    payload={"n": i},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                    schedule_to_close=now + timedelta(seconds=90) if with_deadline else None,
                )
            )
            ids.append(JobId(row.id))
        return ids

    ids = asyncio.run(build())
    churn_idx = 0

    async def churn_round() -> None:
        nonlocal churn_idx
        rows = await backend.dispatch_batch(worker, ["default"], 10, _LEASE)
        for row in rows:
            if churn_idx >= len(churn):
                break
            op = churn[churn_idx]
            churn_idx += 1
            jid = JobId(row.id)
            if op == "fail_terminal":
                await backend.mark_failed_or_retry(
                    jid,
                    worker,
                    ErrorInfo(error_class="E", error_message="m", error_traceback=None),
                    None,
                    attempt=row.attempt,
                    claim_epoch=row.claim_epoch,
                )
            elif op == "fail_retry":
                await backend.mark_failed_or_retry(
                    jid,
                    worker,
                    ErrorInfo(error_class="E", error_message="m", error_traceback=None),
                    timedelta(seconds=5),
                    attempt=row.attempt,
                    claim_epoch=row.claim_epoch,
                )
            elif op == "retry_after":
                await backend.mark_retry_after(
                    jid,
                    worker,
                    timedelta(seconds=5),
                    attempt=row.attempt,
                    claim_epoch=row.claim_epoch,
                )
            elif op == "snooze":
                await backend.mark_snoozed(
                    jid,
                    worker,
                    timedelta(seconds=5),
                    attempt=row.attempt,
                    claim_epoch=row.claim_epoch,
                )
            else:
                await backend.mark_interrupted(
                    jid,
                    worker,
                    attempt=row.attempt,
                    hold=timedelta(seconds=0),
                    claim_epoch=row.claim_epoch,
                )

    asyncio.run(churn_round())

    async def settle_round() -> bool:
        """One drain round; True when nothing is left un-settled."""
        clock.advance(timedelta(hours=2))
        await backend.scheduled_to_pending()
        await backend.deadline_sweep()
        # The leader's reclaim sweep: a churn job abandoned mid-run holds
        # an expired lock no dispatch can claim; the sweep is what hands
        # it back to the queue.
        await backend.reclaim_expired_locks(_GRACE, _GRACE)
        rows = await backend.dispatch_batch(worker, ["default"], 25, _LEASE)
        for row in rows:
            landed = await backend.mark_succeeded(
                JobId(row.id),
                worker,
                {"ok": True},
                attempt=row.attempt,
                claim_epoch=row.claim_epoch,
            )
            assert landed, "a fresh-fence success on a just-claimed row must land"
        return await backend.count_active_jobs(["default"]) == 0

    settled = False
    for _ in range(8):
        if asyncio.run(settle_round()):
            settled = True
            break
    assert settled, "run did not settle within the bounded round count"
    # Conservation holds at settle time too: every enqueued id is terminal,
    # or live-claimed by the draining worker.
    for jid in ids:
        row = asyncio.run(backend.get(jid))
        assert row is not None
        assert _is_terminal(row.status) or (
            row.status == "running" and row.locked_by_worker == worker
        )
