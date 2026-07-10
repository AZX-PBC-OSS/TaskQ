"""Active-job tracking, cancel-poll hook factory.

This module implements ``ActiveJobRegistry`` — the loop-scoped in-process map
of running jobs — the ``_ActiveJob`` dataclass, and the ``CancelController``
class that drives the five-phase cancel-poll loop.

``CancelController`` exposes two methods that ``heartbeat_loop`` calls on every
tick:

- ``run_in_tx(conn)`` — runs inside the heartbeat transaction.  Phases 1, 2,
  and the phase-3 eligibility check happen here.  Phase-3 jobs are queued into
  ``_pending_abandons`` rather than calling ``mark_abandoned`` directly, because
  ``mark_abandoned`` uses a separate pool connection that would deadlock on the
  row lock that the heartbeat transaction still holds.

- ``run_post_tx()`` — called by ``heartbeat_loop`` AFTER the transaction block
  exits (and therefore after the transaction has committed and released its row
  locks).  Drains ``_pending_abandons``, calling ``mark_abandoned`` + deregister
  for each entry.

Key correctness invariants ():
- ``_by_id`` mutations (register/deregister) are protected by ``asyncio.Lock``.
- ``all()`` is synchronous: in asyncio's single-threaded model no other
  coroutine can mutate ``_by_id`` between the list-copy and return *unless*
  there is an intervening ``await``.  ``all()`` has no ``await``, so the
  copy is atomic from the event-loop perspective.  The lock is NOT acquired
  in ``all()`` — acquiring an asyncio.Lock requires ``await`` and would force
  a coroutine boundary that breaks the atomicity guarantee.
- ``cancel_observed_at`` uses ``asyncio.get_running_loop().time()`` (monotonic
  event-loop clock), never ``time.time()`` or ``datetime.now()``.
- Phase-2 PG write (``conn.execute``) happens BEFORE ``task.cancel()`` in
  code order with NO intervening ``await`` (PG-first invariant).
- No ``try/except`` inside the phase-2 block — PG-write failures propagate
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
from taskq.context import JobContext
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
    (NONE, COOPERATIVE) — phase-1 cooperative observation;
    (COOPERATIVE, FORCED) — phase-2 forced escalation;
    (FORCED, ABANDON_PENDING) — phase-3 abandonment;
    (NONE, FORCED) — PG-observation fast-advance (heartbeat hook
    observes ``db_phase=FORCED`` while local is still ``NONE``).

    PG's ``cancel_phase`` column has ``CHECK (cancel_phase BETWEEN 0 AND 2)``
    so :attr:`CancelPhase.ABANDON_PENDING` is in-process only — never
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
    2. Phase 1 — set ``cancel_event``, record ``cancel_observed_at``,
       set local ``cancel_phase=1`` (no PG write).
    3. PG-observation fast-advance — if PG is already at phase 2, skip
       ahead locally (no PG write, no ``task.cancel()``).
    4. Phase 2 — after cancel grace, write ``cancel_phase=2`` to PG,
       then ``task.cancel()`` (PG-first, no intervening ``await``).
    5. Phase 3 — after cleanup grace, queue job into ``_pending_abandons``
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
        # Populated by run_in_tx, drained by run_post_tx.
        self._pending_abandons: deque[JobId] = deque()

    async def run_in_tx(self, conn: asyncpg.Connection) -> None:
        """Execute cancel-poll phases 1-3 inside the heartbeat transaction.

        Phase-3 eligible jobs are queued into ``_pending_abandons``; the actual
        ``mark_abandoned`` call happens in ``run_post_tx`` after the transaction
        commits, avoiding a self-deadlock on the row lock held by this
        transaction.
        """
        self._pending_abandons.clear()
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
                log_cancel_phase_change(
                    _log,
                    from_phase=int(CancelPhase.NONE),
                    to_phase=int(CancelPhase.COOPERATIVE),
                    job_id=active.job_id,
                    worker_id=worker_id,
                )
                _record_phase_transition(CancelPhase.NONE, CancelPhase.COOPERATIVE)

            # ── PG-observation fast-advance ────────────────────────────
            if db_phase == CancelPhase.FORCED and active.cancel_phase < CancelPhase.FORCED:
                log_cancel_phase_change(
                    _log,
                    from_phase=int(active.cancel_phase),
                    to_phase=int(CancelPhase.FORCED),
                    job_id=active.job_id,
                    worker_id=worker_id,
                )
                _record_phase_transition(active.cancel_phase, CancelPhase.FORCED)
                active.cancel_phase = CancelPhase.FORCED
                continue

            elapsed: float | None = None
            if active.cancel_observed_at is not None:
                elapsed = loop.time() - active.cancel_observed_at

            # ── Phase 2: forced escalation ───────────────────────────
            if (
                active.cancel_phase == CancelPhase.COOPERATIVE
                and elapsed is not None
                and elapsed >= self._cancel_grace
            ):
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
                # If both deadlines are already satisfied on this same tick,
                # set the in-process phase-3 sentinel before task.cancel() so
                # the consumer CancelledError path skips mark_cancelled.
                queue_for_abandon = elapsed >= self._cancel_grace + self._cleanup_grace
                if queue_for_abandon:
                    active.cancel_phase = CancelPhase.ABANDON_PENDING
                    self._pending_abandons.append(active.job_id)
                else:
                    active.cancel_phase = CancelPhase.FORCED
                active.task.cancel()
                log_cancel_phase_change(
                    _log,
                    from_phase=int(CancelPhase.COOPERATIVE),
                    to_phase=int(CancelPhase.FORCED),
                    job_id=active.job_id,
                    worker_id=worker_id,
                )
                _record_phase_transition(CancelPhase.COOPERATIVE, CancelPhase.FORCED)

            # ── Phase 3: queue for post-transaction abandonment ──────
            # mark_abandoned MUST run outside the heartbeat transaction:
            # the heartbeat transaction holds an UPDATE lock on this jobs
            # row, and mark_abandoned (on a separate _worker_pool connection)
            # would block waiting for that lock to release — a self-deadlock.
            # We queue the job here and drain in run_post_tx after the
            # transaction commits.
            if (
                active.cancel_phase == CancelPhase.FORCED
                and elapsed is not None
                and elapsed >= self._cancel_grace + self._cleanup_grace
            ):
                # ABANDON_PENDING is in-process only; never persisted.
                active.cancel_phase = CancelPhase.ABANDON_PENDING
                self._pending_abandons.append(active.job_id)

    async def run_post_tx(self) -> None:
        """Drain phase-3 abandonment queue after the heartbeat transaction commits.

        Called by ``heartbeat_loop`` after each tick's ``async with
        conn.transaction()`` block exits.  At that point the row locks held by
        the transaction are released, so ``mark_abandoned`` (which opens a
        separate pool connection) can proceed without deadlocking.

        Each entry is processed unconditionally: failures propagate to the
        caller (heartbeat_loop), which counts them toward heartbeat_failures.
        """
        worker_id = self._worker_id
        while self._pending_abandons:
            job_id = self._pending_abandons.popleft()
            await asyncio.shield(self._backend.mark_abandoned(job_id))
            await self._deps.active_jobs.deregister(job_id)
            log_cancel_phase_change(
                _log,
                from_phase=int(CancelPhase.FORCED),
                to_phase=int(CancelPhase.ABANDON_PENDING),
                job_id=job_id,
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
    the payload parameter is bounded at ``BaseModel`` — the tightest
    type that still admits any actor's payload model. ``BaseModel``
    keeps every payload-agnostic access typed
    (``cancel_event``, ``cancellation_requested``) without widening to
    ``Any``; per-actor payload access happens inside the handler where
    the concrete ``JobContext[P]`` is in scope.

    ``cancel_observed_at`` records the event-loop time (``loop.time()``) when
    the job first transitioned to ``cancel_phase >= 1``.  It is ``None`` until
    phase 1 is entered.  Using ``loop.time()`` (monotonic) prevents NTP
    corrections on the host from triggering premature phase-2 escalation.
    """

    job_id: JobId
    task: asyncio.Task[object]
    ctx: JobContext[BaseModel]
    cancel_phase: CancelPhase = CancelPhase.NONE
    cancel_observed_at: float | None = field(default=None)  # loop.time(), not wall clock


class ActiveJobRegistry:
    """Loop-scoped in-process map of running jobs on this worker.

    One instance per worker process, constructed in ``_main()`` / ``WorkerDeps``
    before the TaskGroup is entered.  Multiple workers in the same process (test
    scenarios) each carry their own independent registry.

    Thread-safety: not applicable — asyncio workers are single-threaded.  The
    ``asyncio.Lock`` on ``_by_id`` prevents interleaving between coroutines that
    ``await register`` / ``await deregister`` while the heartbeat or consumer is
    also running.

    Public surface per :
      - ``register(job_id, task, ctx) -> None`` (async)
      - ``deregister(job_id) -> None`` (async)
      - ``get(job_id) -> _ActiveJob | None`` (sync)
      - ``all() -> list[_ActiveJob]`` (sync snapshot copy)
      - ``count() -> int`` (sync)
    """

    def __init__(self) -> None:
        self._by_id: dict[JobId, _ActiveJob] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def register(
        self,
        job_id: JobId,
        task: asyncio.Task[object],
        ctx: JobContext[BaseModel],
    ) -> None:
        """Register a job as in-flight.

        The lock ensures no concurrent ``deregister`` sees an inconsistent state.
        """
        entry = _ActiveJob(job_id=job_id, task=task, ctx=ctx)
        async with self._lock:
            self._by_id[job_id] = entry

    async def deregister(self, job_id: JobId) -> None:
        """Remove a job from the registry (idempotent — ignores missing keys)."""
        async with self._lock:
            self._by_id.pop(job_id, None)

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
