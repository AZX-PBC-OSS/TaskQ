"""Active-job tracking, cancel-poll hook factory.

This module implements ``ActiveJobRegistry``, the loop-scoped in-process map
of running jobs, the ``_ActiveJob`` dataclass, and the ``CancelController``
class that drives the five-phase cancel-poll loop.

``CancelController`` exposes two methods that ``heartbeat_loop`` calls on every
tick:

- ``run_in_tx(conn)``, runs inside the heartbeat transaction.  Phases 1, 2,
  and the phase-3 eligibility check happen here.  Phase-3 jobs are queued into
  ``_pending_abandons`` rather than calling ``mark_abandoned`` directly, because
  ``mark_abandoned`` uses a separate pool connection that would deadlock on the
  row lock that the heartbeat transaction still holds.

- ``run_post_tx()``, called by ``heartbeat_loop`` AFTER the transaction block
  exits (and therefore after the transaction has committed and released its row
  locks).  Drains ``_pending_abandons``, calling ``mark_abandoned`` + deregister
  for each entry.

Key correctness invariants ():
- ``_by_id`` mutations (register/deregister) are protected by ``asyncio.Lock``.
- ``all()`` is synchronous: in asyncio's single-threaded model no other
  coroutine can mutate ``_by_id`` between the list-copy and return *unless*
  there is an intervening ``await``.  ``all()`` has no ``await``, so the
  copy is atomic from the event-loop perspective.  The lock is NOT acquired
  in ``all()``, acquiring an asyncio.Lock requires ``await`` and would force
  a coroutine boundary that breaks the atomicity guarantee.
- ``cancel_observed_at`` uses ``asyncio.get_running_loop().time()`` (monotonic
  event-loop clock), never ``time.time()`` or ``datetime.now()``.
- Phase-2 PG write (``conn.execute``) happens BEFORE ``task.cancel()`` in
  code order with NO intervening ``await`` (PG-first invariant).
- No ``try/except`` inside the phase-2 block, PG-write failures propagate
  to ``heartbeat_loop``'s outer handler.
- ``run_post_tx`` is always called after ``run_in_tx`` on the same tick, even
  when ``run_in_tx`` raises; ``heartbeat_loop`` must call it in a ``finally``
  block (or equivalent) to drain any entries queued before the error.
"""

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel

from taskq._json import dumps_str
from taskq._shield import shield_with_retrieval
from taskq.backend._protocol import Backend, CancelPhase, JobId
from taskq.backend._sql import (
    CANCEL_ESCALATION_SQL,
    INSERT_EVENT_SQL,
    POLL_CANCEL_FLAGS_SQL,
    parse_rowcount,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.context import CancelOrigin, JobContext
from taskq.obs import get_logger, get_meter, log_cancel_phase_change

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps

__all__ = ["ActiveJobRegistry", "CancelController", "_ActiveJob", "make_cancel_controller"]

_log: structlog.stdlib.BoundLogger = get_logger(__name__)

_phase_transitions = get_meter().create_counter(
    "taskq.cancellation.phase_transitions",
)


def _record_phase_transition(from_phase: CancelPhase, to_phase: CancelPhase) -> None:
    """Bump the phase-transitions counter with the given attribute pair.

    The four valid pairs:
    (NONE, COOPERATIVE), phase-1 cooperative observation;
    (COOPERATIVE, FORCED), phase-2 forced escalation;
    (FORCED, ABANDON_PENDING), phase-3 abandonment;
    (NONE, FORCED), PG-observation fast-advance (heartbeat hook
    observes ``db_phase=FORCED`` while local is still ``NONE``).

    PG's ``cancel_phase`` column has ``CHECK (cancel_phase BETWEEN 0 AND 2)``
    so :attr:`CancelPhase.ABANDON_PENDING` is in-process only, never
    persisted. Using the typed enum here keeps the metric attributes
    consistent with the rest of the cancel pipeline; the int conversion
    happens at the OTel attribute boundary. Total cardinality is bounded
    at 4 timeseries.
    """
    _phase_transitions.add(
        1,
        {"from_phase": int(from_phase), "to_phase": int(to_phase)},
    )


@runtime_checkable
class CancelController(Protocol):
    """Structural interface for cancel-poll controllers.

    ``heartbeat_loop`` calls ``run_in_tx`` inside the open heartbeat
    transaction and ``run_post_tx`` immediately after the transaction commits.
    Implementations must satisfy this two-phase contract.

    The concrete production implementation is ``_CancelController``, constructed
    via ``make_cancel_controller``.  Test stubs need only implement these two
    methods to satisfy the type.
    """

    async def run_in_tx(self, conn: asyncpg.Connection) -> None:
        """Execute cancel-poll phases 1-3 inside the heartbeat transaction."""
        ...

    async def run_post_tx(self) -> None:
        """Drain phase-3 abandonment queue after the transaction commits."""
        ...


class _CancelController:
    """Drives the five-phase cancel-poll loop for one worker.

    The ladder is the OPERATOR-cancel ladder: its phases track a row whose
    ``cancel_requested_at`` an operator set, and its PG-observation arms
    stamp the entry's ``cancel_origin`` OPERATOR accordingly. A shutdown
    (SIGTERM / drain monitor) is not an operator cancel and never walks
    this ladder, the shutdown orchestrator's own phases release
    infrastructure-interrupted work instead (see
    :mod:`taskq.worker.shutdown`).

    Constructed once per worker via ``make_cancel_controller``.  Holds the
    SQL strings, grace-period settings, and ``_pending_abandons`` queue as
    instance state rather than closure variables, making them inspectable in
    tests and debuggers.

    Usage by ``heartbeat_loop`` on every tick::

        async with conn.transaction():
            await controller.run_in_tx(conn)
        await controller.run_post_tx()

    ``run_post_tx`` MUST be called after each tick even if ``run_in_tx``
    raises, because phase-3 jobs may have been queued before the error.
    ``heartbeat_loop`` calls it in a finally-equivalent position.

    Five-phase walkthrough:

    Phases 1-3 run inside ``run_in_tx``:

    1. SELECT outstanding cancel flags for this worker.
    2. Phase 1, set ``cancel_event``, record ``cancel_observed_at``,
       set local ``cancel_phase=1`` (no PG write).
    3. PG-observation fast-advance, if PG is already at phase 2, skip
       ahead locally (no PG write, no ``task.cancel()``).
    4. Phase 2, after cancel grace, write ``cancel_phase=2`` to PG,
       then ``task.cancel()`` (PG-first, no intervening ``await``).
    5. Phase 3, after cleanup grace, queue job into ``_pending_abandons``
       (``cancel_phase`` sentinel set to 3).  Actual ``mark_abandoned`` +
       deregister runs in ``run_post_tx`` after the transaction commits.
    """

    def __init__(
        self,
        deps: "WorkerDeps",
        worker_id: UUID,
        backend: Backend,
    ) -> None:
        schema = deps.settings.schema_name
        if not _IDENT_RE.match(schema):
            raise ValueError(f"invalid schema identifier: {schema!r}")

        self._deps = deps
        self._worker_id = worker_id
        self._backend = backend
        self._poll_sql = POLL_CANCEL_FLAGS_SQL.format(schema=schema)
        self._escalation_sql = CANCEL_ESCALATION_SQL.format(schema=schema)
        self._event_sql = INSERT_EVENT_SQL.format(schema=schema)
        self._cancel_grace = deps.settings.cancellation_grace_period
        self._cleanup_grace = deps.settings.cleanup_grace_period

        # Jobs queued for abandonment.
        # Populated by run_in_tx, drained by run_post_tx.  Never cleared
        # wholesale: an entry whose abandon write raised is re-appended by
        # run_post_tx and must survive into the next tick's drain, because
        # its registry entry holds the in-process ABANDON_PENDING sentinel
        # that no phase arm in run_in_tx matches, wiping the queue here
        # would strand the job permanently between phases while its
        # heartbeat keeps renewing the lease.
        #
        # The queue carries the entry object alongside the job id (issue
        # 461): between the queueing tick and the post-commit drain, the
        # same worker can re-claim the lapsed lease and re-register the
        # key with the new attempt's entry. The drain's cancellation
        # delivery and deregister belong to the attempt it queued, never
        # to whatever the key holds by then. The entry is None for the
        # UNHELD class (see run_in_tx's unheld walk): the row carries no
        # registry entry, so the drain's mark_abandoned is the whole
        # job - there is no task to cancel and nothing to deregister;
        # the drain still delivers to a late-registering entry (the
        # claim->register handoff completed between the queueing tick
        # and the drain).
        self._pending_abandons: deque[tuple[JobId, _ActiveJob | None]] = deque()

        # First-sight stamps for the unheld class: polled rows carrying a
        # cancel flag that no registry entry holds. The graces measure
        # THIS worker's observation of the flag exactly as
        # ``_ActiveJob.cancel_observed_at`` does for held rows; a row the
        # poll stops returning (terminalised, reclaimed) drops its stamp,
        # so a reappearance re-observes with fresh graces.
        self._unheld_observed_at: dict[JobId, float] = {}

    def _tick_liveness(self) -> None:
        """Renew the heartbeat loop's detector-2 stamp between round trips.

        A bulk cancel makes every active job's escalation due inside ONE
        heartbeat tick, each costing an escalation UPDATE plus an event
        INSERT round trip; without renewing between them, a healthy drain
        that merely outlasts one staleness budget
        (``max(interval * grace_factor, stale_floor)``) reads as a dead
        loop and detector 2 force-exits the worker mid-drain, after the
        tick's lease renewals already committed, so the sweep cannot yet
        reclaim the work either.  Name and period match heartbeat_loop's
        own registration (the cancel hook runs inside that loop's tick),
        the same discipline as ``_drain_bounded`` in
        ``taskq.worker._leader_sweeps``.
        """
        self._deps.liveness.tick(
            "heartbeat",
            period=self._deps.settings.heartbeat_interval,
        )

    async def run_in_tx(self, conn: asyncpg.Connection) -> None:
        """Execute cancel-poll phases 1-3 inside the heartbeat transaction.

        Phase-3 eligible jobs are queued into ``_pending_abandons``; the actual
        ``mark_abandoned`` call happens in ``run_post_tx`` after the transaction
        commits, avoiding a self-deadlock on the row lock held by this
        transaction.
        """
        loop = asyncio.get_running_loop()
        worker_id = self._worker_id

        rows = await conn.fetch(self._poll_sql, worker_id)
        # The PG check constraint guarantees rows carry phase 0/1/2; we
        # construct CancelPhase here so downstream comparisons stay typed.
        db_phases: dict[UUID, CancelPhase] = {}
        for row in rows:
            db_phases[row["id"]] = CancelPhase(row["cancel_phase"])

        for active in self._deps.active_jobs.all():
            db_phase = db_phases.get(active.job_id, CancelPhase.NONE)

            # ── Phase 1: cooperative observation ─────────────────────
            if (
                db_phase >= CancelPhase.COOPERATIVE
                and active.cancel_phase < CancelPhase.COOPERATIVE
            ):
                active.ctx.cancel_event.set()
                active.ctx._abort_requested.set()  # pyright: ignore[reportPrivateUsage]  # Why: cancel controller intentionally accesses the private _abort_requested Event to signal sync actors.
                active.cancel_observed_at = loop.time()
                active.cancel_phase = CancelPhase.COOPERATIVE
                # The poll only returns rows carrying cancel_requested_at,
                # so this observation is proof the OPERATOR asked, it
                # overrides a SHUTDOWN stamp from a deploy that signalled
                # the job first (the row is the final arbiter of origin).
                active.cancel_origin = CancelOrigin.OPERATOR
                active.ctx._set_cancel_origin(CancelOrigin.OPERATOR)  # pyright: ignore[reportPrivateUsage]  # Why: the controller is the designated writer of the context's origin stamp (set alongside cancel_event.set(), per the field's contract).
                log_cancel_phase_change(
                    _log,
                    from_phase=int(CancelPhase.NONE),
                    to_phase=int(CancelPhase.COOPERATIVE),
                    job_id=str(active.job_id),
                    worker_id=worker_id,
                )
                _record_phase_transition(CancelPhase.NONE, CancelPhase.COOPERATIVE)

            # ── PG-observation fast-advance ────────────────────────────
            if db_phase == CancelPhase.FORCED and active.cancel_phase < CancelPhase.FORCED:
                log_cancel_phase_change(
                    _log,
                    from_phase=int(active.cancel_phase),
                    to_phase=int(CancelPhase.FORCED),
                    job_id=str(active.job_id),
                    worker_id=worker_id,
                )
                _record_phase_transition(active.cancel_phase, CancelPhase.FORCED)
                active.cancel_phase = CancelPhase.FORCED
                # A row at FORCED is only reachable through an operator's
                # cancel request, stamp the origin so the terminal routing
                # never reads the operator's escalation as a deploy.
                active.cancel_origin = CancelOrigin.OPERATOR
                active.ctx._set_cancel_origin(CancelOrigin.OPERATOR)  # pyright: ignore[reportPrivateUsage]  # Why: the controller is the designated writer of the context's origin stamp (set alongside the phase change, per the field's contract).
                continue

            elapsed: float | None = None
            if active.cancel_observed_at is not None:
                elapsed = loop.time() - active.cancel_observed_at

            # ── Phase 2: forced escalation ───────────────────────────
            # Why the second arm: a local phase of FORCED (or beyond) while PG
            # still reads COOPERATIVE is the signature of a phase-2 write that
            # was applied in memory but rolled back with its heartbeat
            # transaction, another job's statement failed inside the same
            # tick, or the COMMIT itself did.  Without re-issuing it, PG would
            # stay at phase 1 forever: the escalation only ever fires from a
            # local COOPERATIVE phase, so mark_abandoned's `cancel_phase = 2`
            # guard could never match and the job would never again be
            # cancellable.  Re-issuing is safe: the escalation UPDATE is itself
            # guarded by `cancel_phase = 1`, and task.cancel() on an
            # already-cancelling task is a no-op.
            phase_2_due = active.cancel_phase == CancelPhase.COOPERATIVE or (
                active.cancel_phase >= CancelPhase.FORCED and db_phase == CancelPhase.COOPERATIVE
            )
            if phase_2_due and elapsed is not None and elapsed >= self._cancel_grace:
                self._tick_liveness()
                tag = await conn.execute(
                    self._escalation_sql,
                    active.job_id,
                    worker_id,
                )
                rowcount = parse_rowcount(tag)
                if rowcount != 1:
                    continue

                detail = dumps_str(
                    {
                        "from_state": "running",
                        "to_state": "running",
                        "cancel_phase_from": int(CancelPhase.COOPERATIVE),
                        "cancel_phase_to": int(CancelPhase.FORCED),
                        "worker_id": str(worker_id),
                    }
                )
                # worker_id included in detail to preserve observability parity
                # with the shutdown path's write_cancel_escalation event shape.
                await conn.execute(
                    self._event_sql,
                    active.job_id,
                    "state_change",
                    detail,
                )
                # If both deadlines are already satisfied on this same
                # tick, set the in-process phase-3 sentinel and queue the
                # abandon WITHOUT delivering the cancellation here. The
                # abandon's write is not durable until the post-commit
                # drain lands it, and a cancellation delivered inside this
                # transaction lets the consumer unwind and deregister
                # (its finally is unconditional) while the escalation is
                # still uncommitted: a rollback then leaves the row
                # running at cancel_phase 1 with the entry gone from the
                # registry, unreachable by the re-issue arm above (it
                # iterates active_jobs.all()) and by every later tick's
                # ladder, stuck for as long as this worker lives. The
                # drain delivers the cancellation after mark_abandoned
                # applies (see run_post_tx); a rollback leaves the entry
                # registered, the handler still running, and the False
                # arm re-arms the entry at FORCED for a later tick's
                # re-issue, exactly the recovery the comment above
                # describes. The consumer's ABANDON_PENDING guard still
                # skips mark_cancelled: by the time the drain cancels the
                # task, the abandon write owns the terminal state.
                queue_for_abandon = elapsed >= self._cancel_grace + self._cleanup_grace
                if queue_for_abandon:
                    active.cancel_phase = CancelPhase.ABANDON_PENDING
                    self._pending_abandons.append((active.job_id, active))
                else:
                    active.cancel_phase = CancelPhase.FORCED
                    active.task.cancel()
                log_cancel_phase_change(
                    _log,
                    from_phase=int(CancelPhase.COOPERATIVE),
                    to_phase=int(CancelPhase.FORCED),
                    job_id=str(active.job_id),
                    worker_id=worker_id,
                )
                _record_phase_transition(CancelPhase.COOPERATIVE, CancelPhase.FORCED)

            # ── Phase 3: queue for post-transaction abandonment ──────
            # mark_abandoned MUST run outside the heartbeat transaction:
            # the heartbeat transaction holds an UPDATE lock on this jobs
            # row, and mark_abandoned (on a separate _worker_pool connection)
            # would block waiting for that lock to release, a self-deadlock.
            # We queue the job here and drain in run_post_tx after the
            # transaction commits.
            #
            # Why db_phase == FORCED: the poll's predicate
            # (locked_by_worker = this worker, cancel_requested_at set,
            # status = 'running') is exactly the set of rows this worker's
            # abandon may touch, mark_abandoned itself is worker-unfenced,
            # so an abandon queued from stale local state alone can
            # terminate another worker's re-dispatched attempt once a
            # reclaim has moved the row.  A row still owned by this worker
            # is always returned by this worker's own poll, so a silent
            # poll with a local FORCED entry means the entry is stale ,
            # the abandon must not be issued (the PG-level proof is
            # tests/test_rt_cancelwatch_cross_worker_abandon.py).
            if (
                active.cancel_phase == CancelPhase.FORCED
                and db_phase == CancelPhase.FORCED
                and elapsed is not None
                and elapsed >= self._cancel_grace + self._cleanup_grace
            ):
                # ABANDON_PENDING is in-process only; never persisted.
                active.cancel_phase = CancelPhase.ABANDON_PENDING
                self._pending_abandons.append((active.job_id, active))

        # ── Unheld rows: the poll's orphan class ─────────────────────
        # A polled row whose registry entry is GONE: the body exited and
        # its outcome write was cancel-fenced (mark_retry's header: a
        # phase-carrying row matches NO arm, the write no-ops, the
        # consumer's unconditional finally deregisters, and "the row
        # stays 'running' carrying its phase for the cancel ladder to
        # terminalise"). Until this walk existed the ladder iterated
        # active_jobs.all() only, so such a row matched NO writer: the
        # heartbeat kept renewing its lease, and once its claim-stamped
        # started_at aged past the lock lease the claim-loss reconcile
        # read it as "a claim that never reached an actor" and REFUNDED
        # the attempt whose body had already run - the executed attempt
        # lost its ledger row and the row terminalised at the refunded
        # attempt number with no attempt row anywhere (the system tier's
        # effects ledger surfaced it: a body run with no claim row behind
        # it). The poll's own predicate is the ownership contract
        # mark_retry's header already grants this ladder; the walk gives
        # every polled row the ladder, entry or not, on the SAME grace
        # schedule the held walk uses, keyed by this controller's
        # observation map.
        #
        # Terminal-state arithmetic (why the reconcile can never win the
        # race this walk closes): the row strands at the fenced outcome
        # write, the walk first observes it within one heartbeat
        # interval, and the abandon lands by interval +
        # cancellation_grace + cleanup_grace + 2 intervals of tick
        # cadence (each stage fires within one tick after its deadline).
        # At the fleet's pinned settings that is 0.5 + 1.0 + 1.0 + 1.0 =
        # 3.5s, inside the lock lease of 8.0s that bounds the reconcile's
        # started_at age test - the same lease the e2e conftest cascade
        # pins, so the ordering holds wherever that cascade's premises
        # hold.
        unheld_ids = [row["id"] for row in rows if self._deps.active_jobs.get(row["id"]) is None]
        if unheld_ids or self._unheld_observed_at:
            now = loop.time()
            polled = set(unheld_ids)
            # A row the poll stopped returning left this class: drop its
            # stamp so a reappearance re-observes with fresh graces (a
            # reclaimed-then-re-dispatched row must not inherit stale
            # elapsed).
            for stale_id in set(self._unheld_observed_at) - polled:
                del self._unheld_observed_at[stale_id]
            for unheld_id in unheld_ids:
                observed = self._unheld_observed_at.setdefault(unheld_id, now)
                unheld_elapsed = now - observed
                unheld_db_phase = db_phases[unheld_id]
                if (
                    unheld_db_phase == CancelPhase.FORCED
                    and unheld_elapsed >= self._cancel_grace + self._cleanup_grace
                ):
                    # The escalation is already durable in PG (this
                    # walk's own earlier tick): queue the abandon with no
                    # entry. The stamp goes with it: an abandon whose
                    # write raises is re-queued by the drain; a row still
                    # polled re-observes here with fresh graces, and
                    # mark_abandoned's phase-2 guard absorbs any
                    # duplicate write.
                    del self._unheld_observed_at[unheld_id]
                    self._pending_abandons.append((unheld_id, None))
                    log_cancel_phase_change(
                        _log,
                        from_phase=int(CancelPhase.FORCED),
                        to_phase=int(CancelPhase.ABANDON_PENDING),
                        job_id=str(unheld_id),
                        worker_id=worker_id,
                    )
                    _record_phase_transition(CancelPhase.FORCED, CancelPhase.ABANDON_PENDING)
                    continue
                if (
                    unheld_db_phase == CancelPhase.COOPERATIVE
                    and unheld_elapsed >= self._cancel_grace
                ):
                    self._tick_liveness()
                    tag = await conn.execute(self._escalation_sql, unheld_id, worker_id)
                    if parse_rowcount(tag) != 1:
                        # The row moved under the fence (reclaimed,
                        # terminalised): the poll's next tick drops the
                        # stamp if it is really gone.
                        continue
                    detail = dumps_str(
                        {
                            "from_state": "running",
                            "to_state": "running",
                            "cancel_phase_from": int(CancelPhase.COOPERATIVE),
                            "cancel_phase_to": int(CancelPhase.FORCED),
                            "worker_id": str(worker_id),
                        }
                    )
                    await conn.execute(
                        self._event_sql,
                        unheld_id,
                        "state_change",
                        detail,
                    )
                    log_cancel_phase_change(
                        _log,
                        from_phase=int(CancelPhase.NONE),
                        to_phase=int(CancelPhase.FORCED),
                        job_id=str(unheld_id),
                        worker_id=worker_id,
                    )
                    _record_phase_transition(CancelPhase.NONE, CancelPhase.FORCED)

    async def run_post_tx(self) -> None:
        """Drain phase-3 abandonment queue after the heartbeat transaction commits.

        Called by ``heartbeat_loop`` after each tick's ``async with
        conn.transaction()`` block exits.  At that point the row locks held by
        the transaction are released, so ``mark_abandoned`` (which opens a
        separate pool connection) can proceed without deadlocking.

        Each entry is processed unconditionally: failures propagate to the
        caller (heartbeat_loop), which counts them toward heartbeat_failures.

        An abandon that did NOT apply (``mark_abandoned`` returns ``False``,
        because its ``cancel_phase = 2`` guard did not match) leaves the job
        registered and its phase back at FORCED, so a later tick can re-issue
        the escalation and re-queue the abandon.  Deregistering there would
        strand a still-running job with no route back to cancellation.

        A False is also the shape of a budget cut whose detached write
        landed: the shield hands the cut to the caller while the inner
        write detaches and commits, the except arm re-queues the entry,
        and the next tick's re-issued abandon cannot match (the row is no
        longer ``running``, and the cancel-poll filters ``status =
        'running'``, so no ladder arm can ever fire again). The False arm
        therefore re-reads the row before re-arming: an ``abandoned`` row
        means the escalation IS durable and the delivery completes there,
        with the same first-delivery-only shape as the applied arm - a
        write this drain landed is never left with its cancellation
        silently undelivered.

        An abandon whose write RAISES is re-queued at the head of the deque
        and the exception still propagates: the write did not land, and the
        entry's ABANDON_PENDING sentinel matches no phase arm in run_in_tx,
        so dropping it here would strand the job between phases forever ,
        the re-queue hands it to the next tick's drain exactly as the
        not-applied path hands a False back for re-issue.

        The drain also OWNS the cancellation for an abandon queued by
        run_in_tx's same-tick fast path: that arm sets the ABANDON_PENDING
        sentinel and queues the job without delivering task.cancel(), so a
        heartbeat-transaction rollback after the arm cannot strand a
        deregistered entry against a row still at cancel_phase 1 (the
        consumer's unconditional finally would have removed the entry the
        re-issue arm needs). The cancellation is delivered here, after
        mark_abandoned has made the abandon durable - first delivery only
        (a task already cancelling or done takes no second cancel).
        """
        worker_id = self._worker_id
        while self._pending_abandons:
            job_id, queued_entry = self._pending_abandons.popleft()
            self._tick_liveness()
            # shield_with_retrieval, not plain asyncio.shield: a second
            # CancelledError landing while this abandon write is detached
            # (shutdown racing a force-cancel escalation) must not orphan
            # the inner outcome, the retrieval callback logs its failure
            # instead of asyncio reporting "Task exception was never retrieved".
            try:
                abandoned = await shield_with_retrieval(self._backend.mark_abandoned(job_id))
                if not abandoned:
                    # A False is ambiguous: the guard no-ops for a job
                    # that finished naturally, for an escalation another
                    # writer moved, and for an abandon that is ALREADY
                    # durable. Only the row tells them apart, and the
                    # durable case still owes its delivery (see the
                    # False arm below). The read joins the try: a cut
                    # read re-queues the entry exactly as a cut write
                    # does, the next tick retries, nothing is half-
                    # handled.
                    row = await self._backend.get(job_id)
                else:
                    row = None
            except BaseException:
                # Why the broad catch: whatever failed, a pool-acquire
                # TimeoutError, a PostgresError, a socket death, or the
                # heartbeat tick's command-budget cut, delivered as a
                # CancelledError through the shield), the write's fate
                # is not observable here: a cut write DETACHES under the
                # shield and can still commit after this re-queue, a
                # failed write did not land. Re-queueing is correct
                # either way: a write that did not land is re-issued by
                # the next tick's drain, and one that did lands on the
                # not-applied guard, which re-reads the row and
                # completes the delivery the durable abandon still
                # owes. CancelledError is caught for the
                # SAME re-queue reason and re-raised unchanged: at real
                # task teardown the queue dies with the controller
                # (nothing drains later, exactly as the old pop-and-lose
                # behaved), while at a budget cut the next tick drains
                # the re-queued entry, the old pop-and-lose would have
                # stranded the job between phases forever. The detached
                # inner write's outcome is retrieved by the shield's
                # callback, and a late-landing duplicate write is
                # absorbed by the not-applied guard below.
                self._pending_abandons.appendleft((job_id, queued_entry))
                raise
            if not abandoned:
                if row is not None and row.status == "abandoned":
                    # The False was the durability of an earlier drain's
                    # write (detached by a budget cut and committed
                    # afterwards), not a guard miss on a live escalation:
                    # the abandon owns the terminal state, the guard
                    # absorbed the duplicate WRITE, and the delivery the
                    # cut dropped completes here. Same first-delivery-
                    # only shape as the applied arm below, and no second
                    # write: the attempt row is the first one's. The
                    # delivery and the deregister are scoped to the
                    # queued entry (issue 461), the same fence the
                    # applied arm applies: a bare-id get() here could
                    # hand back a live attempt's re-registered entry and
                    # cancel it for an abandon this attempt never queued.
                    await _deliver_abandon(self._deps, job_id, queued_entry, worker_id)
                    continue
                entry = self._deps.active_jobs.get(job_id)
                if entry is not None:
                    entry.cancel_phase = CancelPhase.FORCED
                _log.warning(
                    "cancel-abandon-not-applied",
                    kind="state_change",
                    cause="abandon_guard_unmatched",
                    job_id=str(job_id),
                    worker_id=worker_id,
                )
                continue
            # The same-tick fast path defers the cancellation to here (see
            # run_in_tx's queue_for_abandon comment): the abandon write is
            # durable, so the unwinding consumer's ABANDON_PENDING guard
            # skips mark_cancelled, and a rollback has already been
            # survived with the entry registered and re-armed. Deliver the
            # cancellation now, but only as the FIRST delivery: a task
            # already cancelling (the staggered path's phase-2 arm
            # cancelled it a tick earlier) or already done must not take a
            # second cancellation from the drain.
            #
            # Both the delivery and the deregister are scoped to the
            # entry the tick queued (issue 461), not to a fresh bare-id
            # get(): between the queueing tick and this drain, the same
            # worker can re-claim the lapsed lease and re-register the
            # key with the live attempt's entry. The bare-id shape would
            # cancel the live attempt's task and evict its registration,
            # leaving the reconcile, the shutdown hand-back, and the
            # isolate re-pend blind to a row a live handler owns. The
            # queued entry is the abandoned attempt's; if it already
            # exited, the delivery is a no-op and the deregister is
            # idempotent.
            await _deliver_abandon(self._deps, job_id, queued_entry, worker_id)


async def _deliver_abandon(
    deps: "WorkerDeps",
    job_id: JobId,
    queued_entry: "_ActiveJob | None",
    worker_id: UUID,
) -> None:
    """Deliver an applied abandon's cancellation and drop the entry.

    First delivery only: a task already cancelling (the staggered path's
    phase-2 arm cancelled it a tick earlier) or already done takes no
    second cancellation. The delivery and the deregister are scoped to
    the queued entry (issue 461), not to a fresh bare-id get(): between
    the queueing tick and this drain, the same worker can re-claim the
    lapsed lease and re-register the key with the live attempt's entry.
    The bare-id shape would cancel the live attempt's task and evict its
    registration, leaving the reconcile, the shutdown hand-back, and the
    isolate re-pend blind to a row a live handler owns. The queued entry
    is the abandoned attempt's; if it already exited, the delivery is a
    no-op and the deregister is idempotent.

    A None queued entry is the UNHELD class (run_in_tx's unheld walk):
    the row carried no registry entry when it was queued. The delivery
    then resolves the REGISTRY's current entry, so a claim->register
    handoff that completed between the queueing tick and the drain still
    takes its cancellation (identity-scoped: the entry the registry holds
    IS the live attempt's, exactly what the issue-461 fence wants). Still
    None, there is nothing to deliver and nothing registered to drop -
    mark_abandoned was the whole job.
    """
    entry = queued_entry if queued_entry is not None else deps.active_jobs.get(job_id)
    if entry is not None:
        if not entry.task.done() and entry.task.cancelling() == 0:
            entry.task.cancel()
        await deps.active_jobs.deregister(job_id, entry)
    log_cancel_phase_change(
        _log,
        from_phase=int(CancelPhase.FORCED),
        to_phase=int(CancelPhase.ABANDON_PENDING),
        job_id=str(job_id),
        worker_id=worker_id,
    )
    _record_phase_transition(CancelPhase.FORCED, CancelPhase.ABANDON_PENDING)


def make_cancel_controller(
    deps: "WorkerDeps",
    worker_id: UUID,
    backend: Backend,
) -> CancelController:
    """Construct a ``CancelController`` for the given worker.

    Validates ``schema_name`` eagerly (before any ticks run) so misconfiguration
    is surfaced at startup rather than on the first heartbeat.
    """
    return _CancelController(deps, worker_id, backend)


@dataclass
class _ActiveJob:
    """In-flight job entry in the ActiveJobRegistry.

    ``ctx`` holds the job's :class:`JobContext` instance. The registry
    is heterogeneous (one process holds many actor types in flight), so
    the payload parameter is bounded at ``BaseModel``, the tightest
    type that still admits any actor's payload model. ``BaseModel``
    keeps every payload-agnostic access typed
    (``cancel_event``, ``cancellation_requested``) without widening to
    ``Any``; per-actor payload access happens inside the handler where
    the concrete ``JobContext[P]`` is in scope.

    ``cancel_observed_at`` records the event-loop time (``loop.time()``) when
    the job first transitioned to ``cancel_phase >= 1``.  It is ``None`` until
    phase 1 is entered.  Using ``loop.time()`` (monotonic) prevents NTP
    corrections on the host from triggering premature phase-2 escalation.

    ``cancel_origin`` records WHO asked for the cancellation
    (:class:`~taskq.context.CancelOrigin`): the cancel-poll loop stamps
    OPERATOR when the row's cancel request is observed, the shutdown
    orchestrator stamps SHUTDOWN when it signals the job. The consumer's
    ``CancelledError`` routing reads it to tell an operator's terminal
    cancel apart from an infrastructure interruption, the two surface
    identically as ``CancelledError``, so the distinction has to come from
    the recorded origin, not from the raised error's type.
    """

    job_id: JobId
    task: asyncio.Task[object]
    ctx: JobContext[BaseModel]
    cancel_phase: CancelPhase = CancelPhase.NONE
    cancel_observed_at: float | None = field(default=None)  # loop.time(), not wall clock
    cancel_origin: CancelOrigin = CancelOrigin.NONE


@dataclass(frozen=True)
class ClaimIntent:
    """The token a claim take hands back to the taker.

    Pure identity: one instance per ``mark_claimed`` call, held by the
    loop iteration that took the row and returned to ``resolve_claim`` at
    exit, so the resolver drops only ITS OWN claim (the issue-461 class:
    the same key can be re-marked by a later generation's take before the
    stale generation unwinds). Deliberately empty, the value is the
    object identity alone.
    """


class ActiveJobRegistry:
    """Loop-scoped in-process map of running jobs on this worker.

    One instance per worker process, constructed in ``_main()`` / ``WorkerDeps``
    before the TaskGroup is entered.  Multiple workers in the same process (test
    scenarios) each carry their own independent registry.

    Thread-safety: not applicable, asyncio workers are single-threaded.  The
    ``asyncio.Lock`` on ``_by_id`` prevents interleaving between coroutines that
    ``await register`` / ``await deregister`` while the heartbeat or consumer is
    also running.

    Public surface per :
      - ``register(job_id, task, ctx) -> _ActiveJob`` (async)
      - ``deregister(job_id, entry) -> None`` (async, identity-scoped)
      - ``get(job_id) -> _ActiveJob | None`` (sync)
      - ``all() -> list[_ActiveJob]`` (sync snapshot copy)
      - ``count() -> int`` (sync)
    """

    def __init__(self) -> None:
        self._by_id: dict[JobId, _ActiveJob] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        # Claimed-but-not-yet-registered ids: the window between the
        # consumer's queue take (a bare job.id read, no await) and the
        # register() call is invisible to ``all()`` because the DB row
        # carries no "a consumer took it" mark. Any hand-back pass that
        # ran inside the window would re-pend a row this process is about
        # to execute, and the fleet would run it concurrently. The intent
        # map closes that window: the mark lands with no await after the
        # take, and every hand-back pass excludes both maps. Keyed by id
        # to a CLAIM TOKEN, not a bare set: the token is what makes
        # ``resolve_claim`` identity-scoped (the issue-461 class). A
        # stale attempt's loop iteration can unwind AFTER a same-worker
        # re-claim took the row again (the re-claim's own
        # ``mark_claimed`` overwrote the key); a bare-id discard would
        # erase the LIVE claim's intent and the hand-back passes would
        # re-pend a row this process is about to execute. The resolver
        # drops only the claim it holds.
        self._claim_intents: dict[JobId, ClaimIntent] = {}
        # Claimed-but-not-yet-taken ids: the window between the
        # producer's claim commit and the consumer's queue take. A row
        # parked in local_queue (all consumers busy on long jobs) is
        # running, locked to this worker, and invisible to ``all()`` and
        # the intent map alike; a hand-back pass that excludes only
        # those two maps would re-pend a row this process is about to
        # execute locally. ``mark_claimed`` moves the coverage from this
        # map to the intent map at the take, so every window of the
        # claim-to-register chain is fenced by exactly one map.
        self._queued: set[JobId] = set()

    def mark_enqueued(self, job_id: JobId) -> None:
        """Record a claim the producer handed to the local queue.

        Called by the producer before the queue put (the put is the
        first await after the claim's return, and the mark must precede
        any await a hand-back pass could interleave at).
        """
        self._queued.add(job_id)

    def queued_ids(self) -> list[JobId]:
        """Snapshot of the rows parked in local_queue, not yet taken."""
        return list(self._queued)

    def mark_claimed(self, job_id: JobId) -> ClaimIntent:
        """Record a queue take before any await can let a drain observe the gap.

        Must be called with no intervening await after the take: the
        single-threaded loop makes the record atomic with the take, which
        is the whole guarantee. The take also moves the row's coverage
        from the queued map to the intent map, so the claim-to-register
        chain never has an unfenced window.

        Returns the claim token the caller must hand back to
        ``resolve_claim`` at exit: the same key can be re-marked by a
        later generation's take (a same-worker re-claim) before this
        caller unwinds, and the token is the only identity that scopes
        the resolve to this caller's own claim.
        """
        token = ClaimIntent()
        self._claim_intents[job_id] = token
        self._queued.discard(job_id)
        return token

    def resolve_claim(self, job_id: JobId, token: ClaimIntent) -> None:
        """Drop the claim intent once ``register`` covers it or the row is released.

        Identity-scoped: the entry is removed only when the map's current
        token IS the caller's. A stale generation's resolve (its take
        superseded by a same-worker re-claim's new ``mark_claimed``)
        removes nothing, and the live claim's intent survives to fence
        the hand-back passes for the window it exists to cover.
        """
        if self._claim_intents.get(job_id) is token:
            del self._claim_intents[job_id]

    def held_ids(self) -> list[JobId]:
        """Snapshot of every row this process may still execute: registered and intent.

        Deliberately NOT the queued ids (``queued_ids()``): a row parked
        in local_queue at a DRAINING exit must be re-pended by the exit
        hand-back, which excludes this snapshot only — the hand-back
        passes (drain, isolate) and the heartbeat's lost-claim probe
        bind different exclusions on purpose, each the narrower of the
        two its correctness needs.
        """
        return list(self._by_id) + list(self._claim_intents)

    async def register(
        self,
        job_id: JobId,
        task: asyncio.Task[object],
        ctx: JobContext[BaseModel],
    ) -> _ActiveJob:
        """Register a job as in-flight.

        The lock ensures no concurrent ``deregister`` sees an inconsistent state.
        The claim intent (if any) is absorbed: the registry now owns the row.

        Returns the entry it installed. The caller must hand that entry back
        to ``deregister`` at exit: the same key can be re-registered by a
        later attempt of the same job (a lapsed lease re-claimed on this
        worker), and the return value is the only identity that tells the
        caller's own registration from the live attempt's.
        """
        entry = _ActiveJob(job_id=job_id, task=task, ctx=ctx)
        async with self._lock:
            # The absorb is deliberately key-scoped, not token-scoped: the
            # registering attempt's chain created the intent at its own
            # take, and the intent's whole purpose is the pre-registration
            # window this call closes. Any registration of the key covers
            # whatever intent stands (a re-claim's registry overwrite
            # makes a stale claim's intent moot the same way).
            self._claim_intents.pop(job_id, None)
            self._by_id[job_id] = entry
        return entry

    async def deregister(self, job_id: JobId, entry: _ActiveJob) -> None:
        """Remove a job from the registry (idempotent, ignores missing keys).

        Identity-scoped: the pop happens only when the map's current entry
        IS the ``entry`` this caller registered (or holds from ``get()``).
        A same-worker re-claim re-registers the same key with a NEW entry
        (the later attempt's), so a stale attempt's exit removes only its
        own registration and the live attempt's survives. The bare-id pop
        this replaces evicted the live attempt's registration behind every
        defence that reads ``held_ids()`` (issue 461). This is the same
        fence ``taskq.progress._flush._drop_fenced_out_buffer`` applies to
        the progress buffers, the one other same-keyed map whose entries
        are per-attempt.
        """
        async with self._lock:
            if self._by_id.get(job_id) is not entry:
                return
            del self._by_id[job_id]

    def get(self, job_id: JobId) -> _ActiveJob | None:
        """Return the registry entry for ``job_id``, or ``None`` if absent.

        Synchronous and lock-free: safe to call from the heartbeat hook or any
        non-mutating code path between awaits.
        """
        return self._by_id.get(job_id)

    def all(self) -> list[_ActiveJob]:
        """Return a snapshot copy of all in-flight entries.

        Synchronous: in asyncio's cooperative multitasking, no other coroutine
        can mutate ``_by_id`` while this method runs (there is no ``await``
        between ``list(...)`` and ``return``).  The copy ensures the caller's
        ``for`` loop cannot raise ``RuntimeError: dictionary changed size during
        iteration`` even if ``register``/``deregister`` are called in later
        coroutine steps.

        Callers that need a fresh count after iterating must call ``count()``
        separately.
        """
        return list(self._by_id.values())

    def count(self) -> int:
        """Return the number of currently registered in-flight jobs."""
        return len(self._by_id)

    def __len__(self) -> int:
        return len(self._by_id)
