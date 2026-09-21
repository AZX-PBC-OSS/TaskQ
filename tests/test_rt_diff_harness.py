# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Differential harness: run one scenario against BOTH backends, compare observables.

The suite's per-mirror tests pin each backend against its OWN expectations, so
a mirror can drift from Postgres while every test stays green (the null-lock
no-exit cell proved the failure mode live).  This module is the systematic
differential the suite lacked: a :class:`DiffSide` adapter funnels the SAME
scenario function through ``InMemoryBackend`` (FakeClock domain) and
``PostgresBackend`` (server-clock domain), and :meth:`DiffSide.snapshot`
projects each side onto a normalized observable dict - statuses, attempt-row
tuples, event kinds/details, returned rows' comparable fields - which
:func:`assert_mirror` then asserts equal, with the contract and both sides'
observables in the failure text.  Postgres is the contract source: a mirror
that misleads certifies code that would corrupt on PG.

Clock-domain normalization
=========================

The two sides' clocks are independent (FakeClock is frozen until advanced;
PG advances in wall time).  Scenarios therefore express time as OFFSETS from
a per-side anchor captured at calibration (``side.ts(offset_seconds)``) -
for the PG side re-captured at its own scenario's start, so its offsets,
like the memory side's, measure from where its scenario begins - and
the snapshot normalizes every timestamp to a domain-relative bucket. At
snapshot the memory clock is first advanced by exactly the wall time the PG
side consumed running the scenario, so both sides' action-written
timestamps sit at the same logical elapsed and bucket identically however
loaded the runner is:

* ``None`` - the column is NULL;
* ``"past"`` - materially before the side's own scenario span (beyond the
  0.5 s slack below the span's start);
* ``"now"`` - within the side's own scenario wall span, or within 0.5 s of
  the snapshot's now;
* a rounded integer - seconds AFTER the snapshot's now (retry backoffs,
  lock leases, result expiries; PG's sub-second statement latency is
  absorbed by the rounding).

Where a scenario needs PG's clock to "advance" (a lock expiring, a
unique_for window elapsing, a scheduled job becoming due), the PG side
manipulates the ROW timestamps via ``side.mutate`` (``clock_timestamp()``
arithmetic in SQL) while the memory side rewrites the stored row - the
sanctioned equivalent of driving FakeClock, per the fix plan's
"drive both to the same logical time" rule.

Identity normalization
======================

UUIDs differ per side by construction; the adapter maps them to the
scenario's own tokens (``"j1"``, ``"w1"``, ``"b1"``), so observables compare
token-for-token.  ``side.token_of(job_id)`` reports ``"<new>"`` for a row the
scenario never registered - the dedup-arm differential's signal for "a fresh
row came back".

PG-only surface deliberately normalized away (disposition note, not a
divergence): ``job_attempts.worker_id`` resolves through a ``workers`` FK
probe on PG (NULL when no workers row exists) while the mirror records the
passed id verbatim.  The adapter inserts a ``workers`` row for every worker
token it registers, so both sides record the id - the FK-NULL case is an
observable the mirror cannot represent and no production path hits through
this harness.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.backend._protocol import (
    AttemptOutcome,
    BatchRow,
    CancelPhase,
    DenialReason,
    EnqueueArgs,
    ErrorInfo,
    JobId,
    JobRow,
    RetryKind,
    SnoozeOutcome,
)
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.batch import apply_batch_terminal_outcome
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import seed_actors

pytestmark = pytest.mark.integration

__all__ = [
    "DiffSide",
    "Scenario",
    "assert_mirror",
    "run_differential",
]

_GRACE = timedelta(seconds=30)
_MEM_START = datetime(2025, 1, 1, tzinfo=UTC)
_DEFAULT_ACTOR = "test_actor"


class _StubBackendDeps:
    """Minimal duck-typed BackendDeps: settings + the three pools, nothing else."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None


Scenario = Callable[["DiffSide"], Awaitable[None]]


def _bucket(
    ts: datetime | None,
    now: datetime,
    span_start: datetime | None = None,
) -> Any:
    """Normalize one timestamp onto the domain-relative bucket scale.

    The fence is the side's own scenario wall span (``span_start`` .. ``now``,
    the scenario-end clock reading) plus 0.5 s of slack below the span's
    start; the future fence stays 0.5 s. The reference is each side's own
    scenario-end clock reading, so the two sides' write instants bucket
    against comparable references (the mirror's clock is frozen during its
    scenario; bucketing it against the advanced clock read its writes as
    "past" purely because the PG side's wall time moved on).

    Why the span, and why this width: the mirror's backend-stamped writes all
    sit exactly at its scenario-end reference (the frozen clock), while PG's
    are stamped at wall time THROUGHOUT the scenario - so a single-instant
    fence read the same logical write as "now" on the mirror and "past" on PG
    whenever the runner stretched PG's inter-action wall time past 0.5 s (the
    CI flake on test_diff_sweep1_cancel_carveout_margin, whose cancel request
    is stamped two sweeps before the reference: only the bucket strings
    differed). A write the scenario itself stamped is "at now" on both
    engines - runner latency between actions is not a backend behavioral
    difference - so the "now" window covers the side's own scenario span.
    The 0.5 s slack below the span's start keeps every anchored past offset
    reading "past" (the smallest in the corpus is 1 s before the anchor:
    planted created_at / cancel_requested_at), and the future fence stays
    0.5 s so the smallest asserted backoff (5 s in the sweep corpus) keeps
    its exact rounded bucket - the fence must not widen past the smallest
    asserted backoff. A scenario whose assertion rides the fence still keeps
    its boundary-sensitive offset-bearing write last in the scenario, where
    the reference is captured.
    """
    if ts is None:
        return None
    delta = (ts - now).total_seconds()
    if delta >= 0.5:
        return round(delta)
    if span_start is not None and ts >= span_start - timedelta(seconds=0.5):
        return "now"
    if delta <= -0.5:
        return "past"
    return "now"


def _bucket_ms(duration_ms: int | None) -> Any:
    """Quantize an attempt duration to whole seconds (absorbs PG latency)."""
    if duration_ms is None:
        return None
    return round(duration_ms / 1000)


def _seconds(delta: timedelta | None) -> float | None:
    """Project a duration column onto seconds (None passes through).

    Both backends hand the row a ``timedelta`` (asyncpg decodes interval;
    the mirror stores the EnqueueArgs value), and PG's double-precision
    round-trip is shortest-exact, so the plain float compares cross-domain.
    """
    return None if delta is None else delta.total_seconds()


class DiffSide:
    """One side of a differential run: memory or PG, token-addressed.

    Scenarios address jobs/workers/batches by stable string tokens and time
    by second-offsets; the adapter translates to each domain's ids and
    timestamps and records outcomes for the snapshot comparison.
    """

    def __init__(
        self,
        kind: Literal["memory", "pg"],
        backend: InMemoryBackend | PostgresBackend,
        *,
        conn: asyncpg.Connection | None,
        schema: str | None,
    ) -> None:
        self.kind = kind
        self.backend = backend
        self._conn = conn
        self.schema = schema
        clock: FakeClock | None
        if kind == "memory":
            memory = backend
            assert isinstance(memory, InMemoryBackend)
            raw_clock = memory._clock  # pyright: ignore[reportPrivateUsage]  # Why: the harness owns the twin's clock reference; the established test-seeding pattern.
            assert isinstance(raw_clock, FakeClock)
            clock = raw_clock
        else:
            clock = None
        self._clock = clock
        self._t0: datetime | None = None
        self._jobs_by_token: dict[str, JobId] = {}
        self._token_by_id: dict[UUID, str] = {}
        self._workers_by_token: dict[str, UUID] = {}
        self._token_by_worker: dict[UUID, str] = {}
        self._batches_by_token: dict[str, UUID] = {}
        self._records: dict[str, Any] = {}

    # ── Time domain ────────────────────────────────────────────────────

    async def now(self) -> datetime:
        if self._clock is not None:
            return self._clock.now()
        assert self._conn is not None
        val = await self._conn.fetchval("SELECT clock_timestamp()")
        assert isinstance(val, datetime)
        return val

    async def calibrate(self) -> None:
        """Capture this side's time anchor (memory: the frozen FakeClock start)."""
        if self._clock is not None:
            self._t0 = self._clock.now()
        else:
            self._t0 = await self.now()

    def ts(self, offset_s: float) -> datetime:
        """A datetime at ``anchor + offset`` in THIS side's clock domain."""
        assert self._t0 is not None
        return self._t0 + timedelta(seconds=offset_s)

    # ── Registries ─────────────────────────────────────────────────────

    def token_of(self, job_id: UUID | None) -> str:
        """The scenario token for a job id, or ``"<new>"`` for an unregistered row."""
        if job_id is None:
            return "<none>"
        return self._token_by_id.get(job_id, "<new>")

    def register_job_id(self, token: str, jid: JobId) -> None:
        """Register a scenario-built job id under a token (batch-args pattern)."""
        self._jobs_by_token[token] = jid
        self._token_by_id.setdefault(jid, token)

    def id_rank(self, tokens: list[str]) -> list[int]:
        """Ranks of the tokens' ids within this side's own id order.

        The honest tie-break observable: both backends order equal-priority,
        equal-scheduled_at rows by ``id``, and each side's ids differ, so a
        differential compares each side's dispatch order AGAINST its own id
        order rather than against the other side's token sequence.
        """
        ids = sorted(self._token_by_id)
        ranks: list[int] = []
        for t in tokens:
            jid = self._jobs_by_token.get(t)
            ranks.append(ids.index(jid) if jid is not None and jid in ids else -1)
        return ranks

    async def worker(self, token: str) -> UUID:
        """Register/fetch a worker by token (PG side also inserts its workers row)."""
        if token not in self._workers_by_token:
            wid = new_uuid()
            self._workers_by_token[token] = wid
            self._token_by_worker[wid] = token
            if self.kind == "pg":
                assert self._conn is not None and self.schema is not None
                await self._conn.execute(
                    f'INSERT INTO "{self.schema}".workers (id, hostname, pid, queues) '
                    "VALUES ($1, 'diff-host', 4242, ARRAY['default'])",
                    wid,
                )
        return self._workers_by_token[token]

    def worker_token(self, wid: UUID | None) -> Any:
        if wid is None:
            return None
        return self._token_by_worker.get(wid, f"<unregistered:{wid}>")

    def record(self, label: str, value: Any) -> None:
        """Record a scenario outcome for the snapshot comparison."""
        self._records[label] = value

    # ── Enqueue ────────────────────────────────────────────────────────

    async def enqueue(
        self,
        token: str,
        *,
        actor: str = _DEFAULT_ACTOR,
        queue: str = "default",
        payload: dict[str, object] | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        retry_kind: RetryKind = "transient",
        identity_key: str | None = None,
        fairness_key: str | None = None,
        idempotency_key: str | None = None,
        idempotency_scope: str = "",
        unique_for_s: float | None = None,
        unique_states: tuple[str, ...] = ("pending", "scheduled", "running"),
        max_pending: int | None = None,
        metadata: dict[str, object] | None = None,
        tags: tuple[str, ...] = (),
        scheduled_in: float | None = -1.0,
        stc_in: float | None = None,
        result_ttl_s: float | None = None,
        explicit_id: UUID | None = None,
        retry_jitter: float | None = None,
    ) -> JobRow:
        """Enqueue one job; ``token`` maps to the RETURNED row's id.

        ``scheduled_in=None`` means immediate (each domain's own now stamps
        and decides status); an offset means an absolute instant at
        ``anchor + offset`` in this side's domain.

        ``retry_jitter=None`` leaves the ``EnqueueArgs`` default in place
        (the default is not restated here - one source); a scenario passes
        an explicit value when its observable includes a reclaim-stamped
        ``scheduled_at``: the projection buckets timestamps at
        second-resolution, which cannot resolve a jitter band, so a
        scenario comparing a reclaimed row's ``scheduled_at`` across
        backends pins jitter off. Exact per-(id, attempt) jitter parity
        is pinned separately at full precision by
        ``tests/test_reclaim_backoff_policy_parity.py``.
        """
        retry_kwargs: dict[str, float] = (
            {} if retry_jitter is None else {"retry_jitter": retry_jitter}
        )
        args = EnqueueArgs(
            id=JobId(explicit_id) if explicit_id is not None else new_job_id(),
            actor=actor,
            queue=queue,
            payload=payload if payload is not None else {"value": 1},
            max_attempts=max_attempts,
            retry_kind=retry_kind,
            scheduled_at=None if scheduled_in is None else self.ts(scheduled_in),
            priority=priority,
            max_pending=max_pending,
            schedule_to_close=None if stc_in is None else self.ts(stc_in),
            identity_key=identity_key,  # type: ignore[arg-type]  # Why: IdentityKey is a NewType over str; runtime-transparent.
            fairness_key=fairness_key,
            idempotency_key=idempotency_key,  # type: ignore[arg-type]  # Why: IdempotencyKey is a NewType over str; runtime-transparent.
            idempotency_scope=idempotency_scope,
            unique_for=None if unique_for_s is None else timedelta(seconds=unique_for_s),
            unique_states=unique_states,  # type: ignore[arg-type]  # Why: JobStatus literals supplied by scenarios are known-valid.
            metadata=metadata if metadata is not None else {},
            tags=tags,
            result_ttl=None if result_ttl_s is None else timedelta(seconds=result_ttl_s),
            **retry_kwargs,  # type: ignore[arg-type]  # Why: narrow dict-splat of one optional float field into a dataclass constructor; the key is a literal and the value type-checked above.
        )
        row = await self.backend.enqueue(args)
        self._jobs_by_token[token] = JobId(row.id)
        self._token_by_id.setdefault(row.id, token)
        return row

    # ── Plant / mutate (row-level time control) ────────────────────────

    async def plant(
        self,
        token: str,
        *,
        status: str = "running",
        worker_token: str | None = None,
        lock_expired_ago_s: float | None = None,
        cancel_phase: int = 0,
        attempt: int = 1,
        max_attempts: int = 3,
        retry_kind: RetryKind = "transient",
        started_ago_s: float | None = 30.0,
        stc_in: float | None = None,
        actor: str = _DEFAULT_ACTOR,
        queue: str = "default",
        identity_key: str | None = None,
        metadata: dict[str, object] | None = None,
        priority: int = 0,
        scheduled_in: float = -1.0,
    ) -> JobId:
        """Seed a job row directly in the given state (the sweep corpus pattern)."""
        jid = new_job_id()
        wid = await self.worker(worker_token) if worker_token is not None else None
        if self.kind == "memory":
            row = replace(
                make_job_row(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    retry_kind=retry_kind,
                    status="running",  # type: ignore[arg-type]  # Why: replaced below; make_job_row pins lock-holder fields only for 'running'.
                    priority=priority,
                    queue=queue,
                    actor=actor,
                ),
                # make_job_row generates its OWN id; the twin must store a
                # row whose id IS the key it is stored under - the PG arm's
                # INSERT writes the same id as its pkey. A mismatch leaves
                # the mirror holding a row whose identity disagrees with
                # its storage key (and every attempt/event it later writes).
                id=JobId(jid),
                status=status,  # type: ignore[arg-type]  # Why: scenario-supplied status is a known JobStatus literal.
                identity_key=identity_key,  # type: ignore[arg-type]  # Why: IdentityKey NewType is runtime-transparent.
                created_at=self.ts(-1.0),
                scheduled_at=self.ts(scheduled_in),
                started_at=self.ts(-started_ago_s) if started_ago_s is not None else None,
                locked_by_worker=wid if status == "running" else None,
                lock_expires_at=(
                    self.ts(-lock_expired_ago_s)
                    if lock_expired_ago_s is not None
                    else (self.ts(60.0) if status == "running" else None)
                ),
                cancel_phase=CancelPhase(cancel_phase),
                cancel_requested_at=self.ts(-1.0) if cancel_phase else None,
                schedule_to_close=None if stc_in is None else self.ts(stc_in),
                metadata=metadata if metadata is not None else {},
            )
            memory = self.backend
            assert isinstance(memory, InMemoryBackend)
            memory._jobs[JobId(jid)] = row  # pyright: ignore[reportPrivateUsage]  # Why: test-only private seeding, the established pattern (test_rt_sweeps_parity.py).
        else:
            assert self._conn is not None and self.schema is not None
            # started_at / lock_expires_at / cancel_requested_at are inlined
            # clock_timestamp() expressions (fixed scenario literals, never
            # user input - the same S608 justification as the schema name);
            # every plain value stays $-bound. payload and created_at mirror
            # the memory arm's make_job_row defaults verbatim (empty payload,
            # anchor - 1s): the snapshot projection compares both, so the
            # two arms must plant the same logical row.
            started_sql = (
                "NULL"
                if started_ago_s is None
                else f"clock_timestamp() - interval '{started_ago_s} seconds'"
            )
            if lock_expired_ago_s is not None:
                lock_sql = f"clock_timestamp() - interval '{lock_expired_ago_s} seconds'"
            elif status == "running":
                lock_sql = "clock_timestamp() + interval '60 seconds'"
            else:
                lock_sql = "NULL"
            cancel_req_sql = (
                "NULL" if cancel_phase == 0 else "clock_timestamp() - interval '1 second'"
            )
            await self._conn.execute(
                f'INSERT INTO "{self.schema}".jobs ('
                "id, actor, queue, payload, max_attempts, retry_kind, status, priority, "
                "attempt, created_at, scheduled_at, started_at, locked_by_worker, "
                "lock_expires_at, cancel_phase, cancel_requested_at, schedule_to_close, "
                "identity_key, metadata) VALUES ("
                "$1, $2, $3, '{}'::jsonb, $4, $5, $6::\""
                + self.schema
                + f'".job_status, $7, $8, $15, $9, {started_sql}, $10, '
                f"{lock_sql}, $11, {cancel_req_sql}, $12, $13, $14::jsonb)",
                jid,
                actor,
                queue,
                max_attempts,
                retry_kind,
                status,
                priority,
                attempt,
                self.ts(scheduled_in),
                wid if status == "running" else None,
                cancel_phase,
                None if stc_in is None else self.ts(stc_in),
                identity_key,
                json.dumps(metadata if metadata is not None else {}),
                self.ts(-1.0),
            )
        self._jobs_by_token[token] = JobId(jid)
        self._token_by_id[jid] = token
        return JobId(jid)

    async def mutate(
        self,
        token: str,
        *,
        lock_expired_ago_s: float | None = None,
        created_ago_s: float | None = None,
        scheduled_in_s: float | None = None,
        stc_in_s: float | None = None,
    ) -> None:
        """Rewrite row timestamps to drive this side's clock forward.

        The memory side rewrites the stored row at ``anchor - offset``; the
        PG side binds ``clock_timestamp() - offset`` in SQL.  Both leave the
        row at the same logical instant relative to their own now.
        """
        jid = self._jobs_by_token[token]
        if self.kind == "memory":
            memory = self.backend
            assert isinstance(memory, InMemoryBackend)
            row = memory._jobs.get(jid)  # pyright: ignore[reportPrivateUsage]  # Why: test-only private seeding, the established pattern.
            assert row is not None
            updates: dict[str, datetime] = {}
            if lock_expired_ago_s is not None:
                updates["lock_expires_at"] = self.ts(-lock_expired_ago_s)
            if created_ago_s is not None:
                updates["created_at"] = self.ts(-created_ago_s)
            if scheduled_in_s is not None:
                updates["scheduled_at"] = self.ts(scheduled_in_s)
            if stc_in_s is not None:
                updates["schedule_to_close"] = self.ts(stc_in_s)
            memory._jobs[jid] = replace(row, **updates)  # pyright: ignore[reportPrivateUsage]  # Why: test-only private seeding, the established pattern.
        else:
            assert self._conn is not None and self.schema is not None
            sets: list[str] = []
            if lock_expired_ago_s is not None:
                sets.append(
                    f"lock_expires_at = clock_timestamp() - interval '{lock_expired_ago_s} seconds'"
                )
            if created_ago_s is not None:
                sets.append(f"created_at = clock_timestamp() - interval '{created_ago_s} seconds'")
            if scheduled_in_s is not None:
                sets.append(
                    f"scheduled_at = clock_timestamp() - interval '{-scheduled_in_s} seconds'"
                )
            if stc_in_s is not None:
                sets.append(
                    f"schedule_to_close = clock_timestamp() - interval '{-stc_in_s} seconds'"
                )
            assert sets
            await self._conn.execute(
                f'UPDATE "{self.schema}".jobs SET {", ".join(sets)} WHERE id = $1',
                jid,
            )

    # ── Dispatch ───────────────────────────────────────────────────────

    async def dispatch(
        self,
        worker_token: str,
        queues: list[str],
        limit: int,
        *,
        lease_s: float = 60.0,
    ) -> list[str]:
        rows = await self.backend.dispatch_batch(
            await self.worker(worker_token),
            list(queues),
            limit,
            timedelta(seconds=lease_s),
        )
        return [self.token_of(r.id) for r in rows]

    async def set_round_robin(self, queue: str = "default") -> None:
        """Put one queue into round_robin mode on both domains."""
        if self.kind == "memory":
            memory = self.backend
            assert isinstance(memory, InMemoryBackend)
            memory.set_queue_mode(queue, "round_robin")
        else:
            assert self._conn is not None and self.schema is not None
            await self._conn.execute(
                f'INSERT INTO "{self.schema}".queues (name, mode) VALUES ($1, $2) '
                "ON CONFLICT (name) DO UPDATE SET mode = EXCLUDED.mode",
                queue,
                "round_robin",
            )

    async def register_actor_config(
        self,
        actor: str,
        *,
        max_concurrent: int | None = None,
        max_pending: int | None = None,
        queue: str = "default",
    ) -> None:
        """Register one actor_config row in this side's own domain.

        The memory side registers through the twin's registry (the
        ``taskq actor-config set`` analog); the PG side inserts the
        actor_config row directly. Scenarios never reach for the side's
        private connection to do this.
        """
        if self.kind == "memory":
            memory = self.backend
            assert isinstance(memory, InMemoryBackend)
            memory.register_actor_config(
                actor=actor, max_concurrent=max_concurrent, max_pending=max_pending, queue=queue
            )
        else:
            assert self._conn is not None and self.schema is not None
            await self._conn.execute(
                f'INSERT INTO "{self.schema}".actor_config '
                "(actor, queue, max_concurrent, max_pending) VALUES ($1, $2, $3, $4)",
                actor,
                queue,
                max_concurrent,
                max_pending,
            )

    # ── Sweeps ─────────────────────────────────────────────────────────

    async def sweep_reclaim(self, *, batch_size: int = 100) -> int:
        return await self.backend.reclaim_expired_locks(_GRACE, _GRACE, batch_size=batch_size)

    async def sweep_deadline(self, *, batch_size: int = 100) -> int:
        return await self.backend.deadline_sweep(batch_size=batch_size)

    async def sweep_promote(self, *, batch_size: int = 100) -> int:
        return await self.backend.scheduled_to_pending(batch_size=batch_size)

    # ── Cancel protocol ────────────────────────────────────────────────

    async def write_cancel_request(self, token: str, reason: str | None) -> bool:
        return await self.backend.write_cancel_request(self._jobs_by_token[token], reason)

    async def write_cancel_escalation(self, token: str, worker_token: str) -> bool:
        return await self.backend.write_cancel_escalation(
            self._jobs_by_token[token], await self.worker(worker_token), 2
        )

    async def poll_cancel_flags(self, worker_token: str) -> list[tuple[str, int]]:
        flags = await self.backend.poll_cancel_flags(await self.worker(worker_token))
        return [(self.token_of(f.job_id), int(f.cancel_phase)) for f in flags]

    async def mark_abandoned(self, token: str) -> bool:
        return await self.backend.mark_abandoned(self._jobs_by_token[token])

    # ── Terminal writes ────────────────────────────────────────────────

    async def _attempt_epoch_of(self, token: str) -> int | None:
        """The row's current attempt number - the epoch terminal writes fence on.

        The worker presents ``attempt=job.attempt`` from its in-hand row
        (worker/_consumer.py); the scenario adapter does the same through
        the protocol read, so the differential drives the fencing the
        production caller drives. A missing row yields ``None``, which the
        fence refuses exactly as PG's NULL bind never satisfies the
        equality.
        """
        row = await self.backend.get(self._jobs_by_token[token])
        return None if row is None else row.attempt

    async def _claim_epoch_of(self, token: str) -> int | None:
        """The row's current claim epoch - the terminal writes' second fence
        bind, threaded beside ``attempt`` exactly as the worker threads it
        (the claim-epoch fence, 01.00.18_02)."""
        row = await self.backend.get(self._jobs_by_token[token])
        return None if row is None else row.claim_epoch

    async def mark_succeeded(
        self,
        token: str,
        worker_token: str,
        *,
        result: dict[str, object] | None = None,
        fallback_result_ttl_s: float | None = None,
    ) -> bool:
        return await self.backend.mark_succeeded(
            self._jobs_by_token[token],
            await self.worker(worker_token),
            result,
            fallback_result_ttl=None
            if fallback_result_ttl_s is None
            else timedelta(seconds=fallback_result_ttl_s),
            attempt=await self._attempt_epoch_of(token),
            claim_epoch=await self._claim_epoch_of(token),
        )

    async def mark_failed_or_retry(
        self,
        token: str,
        worker_token: str,
        *,
        error_class: str = "ValueError",
        error_message: str = "boom",
        retry_delay_s: float | None = None,
    ) -> JobRow:
        return await self.backend.mark_failed_or_retry(
            self._jobs_by_token[token],
            await self.worker(worker_token),
            ErrorInfo(error_class=error_class, error_message=error_message, error_traceback=None),
            None if retry_delay_s is None else timedelta(seconds=retry_delay_s),
            attempt=await self._attempt_epoch_of(token),
            claim_epoch=await self._claim_epoch_of(token),
        )

    async def mark_cancelled(self, token: str, worker_token: str) -> bool:
        return await self.backend.mark_cancelled(
            self._jobs_by_token[token],
            await self.worker(worker_token),
            attempt=await self._attempt_epoch_of(token),
            claim_epoch=await self._claim_epoch_of(token),
        )

    async def mark_snoozed(
        self,
        token: str,
        worker_token: str,
        delay_s: float,
        *,
        outcome: SnoozeOutcome = "snoozed",
        metadata_update: dict[str, object] | None = None,
        denial_reason: DenialReason = "capacity",
    ) -> str:
        return await self.backend.mark_snoozed(
            self._jobs_by_token[token],
            await self.worker(worker_token),
            timedelta(seconds=delay_s),
            metadata_update=metadata_update,
            outcome=outcome,
            attempt=await self._attempt_epoch_of(token),
            denial_reason=denial_reason,
            claim_epoch=await self._claim_epoch_of(token),
        )

    async def mark_retry_after(
        self,
        token: str,
        worker_token: str,
        delay_s: float,
        *,
        consume_budget: bool = True,
    ) -> str:
        return await self.backend.mark_retry_after(
            self._jobs_by_token[token],
            await self.worker(worker_token),
            timedelta(seconds=delay_s),
            consume_budget=consume_budget,
            attempt=await self._attempt_epoch_of(token),
            claim_epoch=await self._claim_epoch_of(token),
        )

    async def retry_job(self, token: str) -> bool:
        return await self.backend.retry_job(self._jobs_by_token[token])

    # ── Batch operations ───────────────────────────────────────────────

    async def create_batch(
        self,
        token: str,
        *,
        queue: str = "default",
        expected_size: int,
        failure_threshold: int | None,
        finalizer_token: str | None = None,
    ) -> UUID:
        bid = new_uuid()
        await self.backend.create_batch(
            bid,
            queue,
            expected_size,
            failure_threshold,
            None if finalizer_token is None else self._jobs_by_token[finalizer_token],
            None,
        )
        self._batches_by_token[token] = bid
        return bid

    async def batch_increment(self, token: str) -> tuple[int, int | None, int]:
        return await self.backend.increment_batch_failures(self._batches_by_token[token])

    def batch_id_of(self, token: str) -> UUID:
        """The side-local UUID registered for a batch token."""
        return self._batches_by_token[token]

    async def batch_row(self, token: str) -> BatchRow | None:
        """The batch row through the protocol read."""
        return await self.backend.get_batch(self._batches_by_token[token])

    async def batch_reset(self, token: str) -> int:
        return await self.backend.reset_batch_failures(self._batches_by_token[token])

    async def batch_abort(self, token: str) -> int:
        return await self.backend.abort_batch(self._batches_by_token[token])

    async def batch_complete(self, token: str) -> None:
        await self.backend.complete_batch(self._batches_by_token[token])

    async def batch_non_terminal(self, token: str) -> int:
        return await self.backend.count_batch_non_terminal(self._batches_by_token[token])

    async def apply_outcome(self, token: str, outcome: AttemptOutcome | Literal["noop"]) -> None:
        """The shared batch policy hook, driven through the Backend protocol."""
        row = await self.backend.get(self._jobs_by_token[token])
        assert row is not None
        await apply_batch_terminal_outcome(self.backend, row, outcome)

    # ── Snapshot ───────────────────────────────────────────────────────

    async def _job_observable(
        self,
        jid: JobId,
        now: datetime,
        span_start: datetime | None,
    ) -> dict[str, Any]:
        row = await self.backend.get(jid)
        attempts = await self.backend.get_attempts(jid)
        events = await self.backend.get_events(jid)
        if row is None:
            return {"present": False}
        # metadata.batch_id is a side-local UUID string; normalize it to the
        # scenario's batch token so member observables compare cross-domain.
        metadata = dict(row.metadata)
        raw_bid = metadata.get("batch_id")
        if isinstance(raw_bid, str):
            try:
                bid = UUID(raw_bid)
            except ValueError:
                bid = None
            if bid is not None:
                metadata["batch_id"] = next(
                    (t for t, b in self._batches_by_token.items() if b == bid),
                    "<batch>",
                )
        # The projection is TOTAL over JobRow's fields - one key per field,
        # guarded by test_job_observable_projects_every_jobrow_field: a
        # field the projection cannot serialize is a PG↔memory divergence
        # the harness would never catch. Identity and side-local values
        # normalize to scenario tokens; timestamps bucket against the
        # snapshot's now; durations project to seconds.
        return {
            "present": True,
            "id": self.token_of(row.id),
            "actor": row.actor,
            "queue": row.queue,
            "identity_key": row.identity_key,
            "fairness_key": row.fairness_key,
            "payload": row.payload,
            "payload_schema_ver": row.payload_schema_ver,
            "status": row.status,
            "priority": row.priority,
            "attempt": row.attempt,
            "max_attempts": row.max_attempts,
            "retry_kind": row.retry_kind,
            "schedule_to_close": _bucket(row.schedule_to_close, now, span_start),
            "start_to_close": _seconds(row.start_to_close),
            "heartbeat_timeout": _seconds(row.heartbeat_timeout),
            "created_at": _bucket(row.created_at, now, span_start),
            "scheduled_at": _bucket(row.scheduled_at, now, span_start),
            "started_at": _bucket(row.started_at, now, span_start),
            "finished_at": _bucket(row.finished_at, now, span_start),
            "last_heartbeat_at": _bucket(row.last_heartbeat_at, now, span_start),
            "locked_by_worker": self.worker_token(row.locked_by_worker),
            "lock_expires_at": _bucket(row.lock_expires_at, now, span_start),
            "cancel_requested_at": _bucket(row.cancel_requested_at, now, span_start),
            "cancel_phase": int(row.cancel_phase),
            "error_class": row.error_class,
            "error_message": row.error_message,
            "error_traceback": row.error_traceback,
            "progress_state": row.progress_state,
            "progress_seq": row.progress_seq,
            "result": row.result,
            "result_size_bytes": row.result_size_bytes,
            "result_expires_at": _bucket(row.result_expires_at, now, span_start),
            "idempotency_key": row.idempotency_key,
            "idempotency_scope": row.idempotency_scope,
            "trace_id": row.trace_id,
            "span_id": row.span_id,
            "metadata": metadata,
            "tags": list(row.tags),
            "snooze_count": row.snooze_count,
            "rate_limit_blocked_count": row.rate_limit_blocked_count,
            "interrupt_count": row.interrupt_count,
            "claim_epoch": row.claim_epoch,
            "retry_base": _seconds(row.retry_base),
            "retry_cap": _seconds(row.retry_cap),
            "retry_backoff": row.retry_backoff,
            "retry_jitter": row.retry_jitter,
            "assignment_routed": row.assignment_routed,
            "attempts": [
                {
                    "attempt": a.attempt,
                    "outcome": a.outcome,
                    "error_class": a.error_class,
                    "error_message": a.error_message,
                    "worker": self.worker_token(a.worker_id),
                    "duration_s": _bucket_ms(a.duration_ms),
                    "finished": a.finished_at is not None,
                }
                for a in attempts
            ],
            "events": [{"kind": e.kind, "detail": self._norm_detail(e.detail)} for e in events],
        }

    def _norm_detail(self, detail: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in detail.items():
            if key == "worker_id":
                out[key] = self.worker_token(UUID(str(value))) if value is not None else None
            elif key in ("cancel_phase_from", "cancel_phase_to"):
                out[key] = int(value)
            else:
                out[key] = value
        return out

    def _norm_value(self, value: Any, now: datetime, span_start: datetime | None) -> Any:
        if isinstance(value, UUID):
            if value in self._token_by_id:
                return self._token_by_id[value]
            return self.worker_token(value)
        if isinstance(value, JobRow):
            return self.token_of(value.id)
        if isinstance(value, datetime):
            return _bucket(value, now, span_start)
        if isinstance(value, dict):
            # Why the cast: ``value`` is Any, and isinstance-narrowing leaves
            # its items Unknown (pyright strict) - cast re-declares the
            # narrowed container so every element is Any, not Unknown.
            mapping = cast("dict[Any, Any]", value)
            return {k: self._norm_value(v, now, span_start) for k, v in mapping.items()}
        if isinstance(value, (list, tuple)):
            sequence = cast("list[Any] | tuple[Any, ...]", value)
            return [self._norm_value(v, now, span_start) for v in sequence]
        return value

    async def _batch_observable(
        self,
        bid: UUID,
        now: datetime,
        span_start: datetime | None,
    ) -> dict[str, Any]:
        row = await self.backend.get_batch(bid)
        if row is None:
            return {"present": False}
        return {
            "present": True,
            "status": row.status,
            "expected_size": row.expected_size,
            "consecutive_failures": row.consecutive_failures,
            "failure_threshold": row.failure_threshold,
            "finalizer_job_id": self.token_of(row.finalizer_job_id),
            "completed_at": _bucket(row.completed_at, now, span_start),
        }

    async def snapshot(self, ref: datetime | None = None) -> dict[str, Any]:
        """The normalized observable dict the differential compares.

        ``ref`` is the bucket reference: each side's own scenario-end clock
        reading, captured by :func:`run_differential` before any cross-side
        work. Defaulting to the live clock keeps ad-hoc callers working,
        but the differential must pass the reference: the mirror's clock is
        frozen during its scenario while PG's advances in wall time, so a
        shared reference reads the two sides' identical write instants at
        different buckets whenever a loaded runner stretches the PG
        scenario past the fence. The fence itself is the side's own
        scenario wall span (``self._t0`` .. ``ref``); see :func:`_bucket`.
        """
        now = ref if ref is not None else await self.now()
        span_start = self._t0
        jobs = {
            token: await self._job_observable(jid, now, span_start)
            for token, jid in self._jobs_by_token.items()
        }
        status_counts: dict[str, int] = {}
        for jid in set(self._jobs_by_token.values()):
            row = await self.backend.get(jid)
            if row is not None:
                status_counts[row.status] = status_counts.get(row.status, 0) + 1
        batches = {
            token: await self._batch_observable(bid, now, span_start)
            for token, bid in self._batches_by_token.items()
        }
        return {
            "jobs": jobs,
            "status_counts": status_counts,
            "batches": batches,
            "records": {k: self._norm_value(v, now, span_start) for k, v in self._records.items()},
        }

    async def teardown_pg_schema(self) -> None:
        """Drop this side's PG schema and close its private connection.

        The pool teardown stays with the caller (the harness built the
        pool); this owns the side-local schema + connection pair.
        """
        assert self._conn is not None and self.schema is not None
        await self._conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        await self._conn.close()


# ── Side builders ──────────────────────────────────────────────────────


def _memory_side(actors: Sequence[str]) -> DiffSide:
    clock = FakeClock(_MEM_START)
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    backend.register_actor_configs(
        ActorConfig(actor=actor, max_concurrent=None, max_pending=None, queue="default")
        for actor in actors
    )
    side = DiffSide("memory", backend, conn=None, schema=None)
    # Why the private poke instead of calibrate(): the builder is SYNC (its
    # caller composes it into run_differential without an event loop hop),
    # and calibrate() is async - the frozen FakeClock's now IS the anchor,
    # so the assignment is exactly what calibrate() would await to do.
    side._t0 = _MEM_START  # pyright: ignore[reportPrivateUsage]  # Why: harness-owned anchor seeding; the established same-module pattern.
    return side


async def _pg_side(pg_dsn: str, *, schema: str, actors: Sequence[str]) -> DiffSide:
    conn = await asyncpg.connect(pg_dsn)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)
    if actors:
        await seed_actors(conn, schema, actors=list(actors))
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
        },
        validate=False,
    )
    deps = _StubBackendDeps(settings)
    deps.worker_pool = pool
    deps.heartbeat_pool = pool
    deps.dispatcher_pool = pool
    backend = PostgresBackend(
        deps,  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; only settings + pools are read on the paths under test.
        clock=SystemClock(),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    side = DiffSide("pg", backend, conn=conn, schema=schema)
    await side.calibrate()
    return side


async def _close_pg_side(side: DiffSide) -> None:
    pg = side.backend
    assert isinstance(pg, PostgresBackend)
    pool = pg._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: harness-owned teardown of the pool it built.
    assert isinstance(pool, asyncpg.Pool)
    await pool.close()
    await side.teardown_pg_schema()


async def run_differential(
    scenario: Scenario,
    *,
    pg_dsn: str,
    actors: Sequence[str] = (_DEFAULT_ACTOR,),
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run ``scenario`` on both backends; return ``(memory_observables, pg_observables)``.

    The SAME function drives both sides through the :class:`DiffSide` adapter,
    so the inputs are identical by construction; only the backend differs.
    The PG side gets a random ``tdf_`` schema with migrations applied, and is
    dropped (CASCADE) in the finally - pool closed first.
    """
    schema = f"tdf_{new_base62()}".lower()
    mem = _memory_side(actors)
    pg = await _pg_side(pg_dsn, schema=schema, actors=actors)
    try:
        await scenario(mem)
        # The memory side's bucket reference: its clock reading at its own
        # scenario's end. The FakeClock is frozen during the scenario, so
        # every action-written timestamp sits at this instant; bucketing
        # the mirror against anything later reads its writes as "past"
        # purely because the PG side's wall time moved on.
        ref_mem = await mem.now()
        # Re-anchor the PG side to its OWN scenario's start. The anchor
        # captured at side construction (the calibrate() call) predates the
        # MEMORY scenario, whose wall time is time the PG side's
        # action-written timestamps never spanned - the memory side writes
        # its action timestamps at the frozen FakeClock instant no matter
        # how long its own scenario runs. Measuring the co-drive elapsed
        # from the stale anchor advanced the memory clock by the memory
        # scenario's own duration, so once a loaded runner stretched that
        # scenario past the 0.5 s bucket fence every memory action
        # timestamp read one bucket further into the past than its PG
        # counterpart ('now' -> 'past', a 10 s backoff reading 9) - the
        # mirror diverged on pure runner latency, not backend behavior
        # (the CI failure on test_diff_mark_failed_or_retry_arms). The
        # re-anchor also makes the sides' offset domains symmetric:
        # scenario time offsets (side.ts) now start where each side's own
        # scenario starts, exactly as the memory side's offsets start at
        # its frozen anchor.
        pg._t0 = await pg.now()
        await scenario(pg)
        # Co-drive the memory clock to the PG side's elapsed time. The
        # scenario's action-written timestamps (claims, cancels) anchor to
        # each side's own clock: PG's to the server clock, which advances
        # with real runner time, the memory's to the frozen FakeClock,
        # which does not. Bucketing both sides against their own now then
        # diverges on a loaded runner - a lifecycle field written at the
        # enqueue reads "now" on the frozen clock and "past" on the
        # advanced one, and a lease reads one second lower. This is the
        # harness's own drive-both-to-the-same-logical-time rule, applied
        # at snapshot: the memory clock advances by exactly the elapsed
        # the PG side experienced (measured from the re-anchored t0 -
        # its own scenario's start), so action-written fields bucket
        # identically and the rounding absorbs only sub-second residual.
        assert pg._t0 is not None  # pyright: ignore[reportPrivateUsage]  # Why: harness-owned anchor; the established same-module pattern.
        # The PG side's bucket reference: the server clock at its own
        # scenario's end, captured BEFORE the snapshot round trips (their
        # duration is runner latency, not scenario time).
        ref_pg = await pg.now()
        elapsed = ref_pg - pg._t0
        assert mem._clock is not None  # pyright: ignore[reportPrivateUsage]
        mem._clock.advance(elapsed)
        mem_obs = await mem.snapshot(ref=ref_mem)
        pg_obs = await pg.snapshot(ref=ref_pg)
        return mem_obs, pg_obs
    finally:
        await _close_pg_side(pg)


def assert_mirror(contract: str, mem: dict[str, Any], pg: dict[str, Any]) -> None:
    """Assert the mirror produced PG's observables; PG is the contract source."""
    assert mem == pg, (
        "MIRROR DIVERGENCE - the InMemoryBackend must reproduce PostgresBackend's "
        f"observables for identical inputs.\nContract: {contract}\n"
        f"PostgresBackend (contract source): {pg}\n"
        f"InMemoryBackend (mirror):          {mem}"
    )


# ── Smoke differential: the harness's green baseline ───────────────────


async def _smoke_scenario(side: DiffSide) -> None:
    await side.enqueue("j1", scheduled_in=-3.0, payload={"v": 1})
    await side.enqueue("j2", scheduled_in=-1.0, payload={"v": 2})
    dispatched = await side.dispatch("w1", ["default"], limit=5)
    side.record("dispatched", dispatched)
    retried_row = await side.mark_failed_or_retry("j1", "w1", retry_delay_s=10.0)
    side.record("retry_returned", side.token_of(retried_row.id))
    side.record("retry_status", retried_row.status)
    side.record("failed_j2", await side.mark_failed_or_retry("j2", "w1", retry_delay_s=None))


async def test_diff_harness_baseline_green(pg_dsn: str) -> None:
    """The harness's own baseline: enqueue -> dispatch -> retry/failed compares equal.

    (``mark_succeeded`` is deliberately absent here: its lock-bookkeeping
    divergence is pinned separately in ``tests/test_rt_diff_terminal.py``.)
    """
    mem, pg = await run_differential(_smoke_scenario, pg_dsn=pg_dsn)
    assert_mirror(
        "enqueue -> dispatch -> mark_failed_or_retry leaves identical statuses, "
        "attempt rows, and event trails on both backends",
        mem,
        pg,
    )
    assert pg["records"]["dispatched"] == ["j1", "j2"]
    assert pg["records"]["retry_status"] == "scheduled"
    assert pg["status_counts"] == {"scheduled": 1, "failed": 1}


# ── Bucket-fence regression: mid-scenario writes bucket 'now' on both sides ─


async def _fence_stretch_scenario(side: DiffSide) -> None:
    # The immediate enqueue stamps scheduled_at at each side's own now, and
    # the cancel request stamps cancel_requested_at mid-scenario; the forced
    # 0.75 s wall tail (sleep never wakes early) then puts BOTH writes
    # beyond the 0.5 s fence from the PG side's scenario-end reference - the
    # loaded-runner shape the CI flake on
    # test_diff_sweep1_cancel_carveout_margin hit (its cancel request is
    # stamped two sweeps before the reference; only the bucket strings
    # differed). Offset-bearing writes keep the harness's documented rule:
    # the dispatch's 60 s lease goes LAST in the scenario, where the
    # reference is captured, so its rounded bucket stays exact.
    await side.enqueue("j1", scheduled_in=None)
    await side.write_cancel_request("j1", "fence-probe")
    await asyncio.sleep(0.75)
    await side.dispatch("w1", ["default"], limit=5)


async def test_diff_bucket_fence_absorbs_scenario_wall_time(pg_dsn: str) -> None:
    """A backend-stamped write beyond the fence from the reference still buckets 'now'.

    Deterministic repro of the loaded-runner flake: the mirror's frozen clock
    pins every write at its own scenario-end reference ('now') while PG's
    stamps carry the scenario's wall time and crossed the 0.5 s 'past' fence.
    The forced 0.75 s tail (sleep never wakes early) pins the fence rule:
    mid-scenario writes bucket identically on both engines, and every
    behavioral observable - status, cancel phase, event trail, attempt row -
    stays exact.
    """
    mem, pg = await run_differential(_fence_stretch_scenario, pg_dsn=pg_dsn)
    assert_mirror(
        "backend-stamped writes landing beyond the 0.5s fence from the "
        "reference (a loaded runner's wall-time stretch) bucket identically "
        "on both backends - 'now' on both, never 'past' on one and 'now' on "
        "the other - with every behavioral observable unaffected",
        mem,
        pg,
    )
    assert pg["jobs"]["j1"]["status"] == "cancelled"
    assert pg["jobs"]["j1"]["scheduled_at"] == "now"
    assert pg["jobs"]["j1"]["created_at"] == "now"


# ── Projection completeness: the differential's blind-spot guard ────────


async def test_job_observable_projects_every_jobrow_field() -> None:
    """The job projection is TOTAL: every ``JobRow`` field has a same-named key.

    The differential can only catch PG↔memory divergence in fields the
    projection serializes - a field added to ``JobRow`` but never projected
    here is a silent blind spot (the retry-curve scalars, ``interrupt_count``
    and ``assignment_routed`` were exactly that). The field list derives
    from the dataclass itself - one source of truth - so adding a
    ``JobRow`` field without projecting it fails this test. The planted
    row carries non-default values for the previously unprojected fields
    so their serialization is exercised, not just their key presence.
    """
    side = _memory_side(())
    memory = side.backend
    assert isinstance(memory, InMemoryBackend)
    planted = replace(
        make_job_row(status="pending"),
        actor="obs_actor",
        queue="obs_queue",
        fairness_key="fk",
        payload={"k": 7},
        payload_schema_ver=2,
        start_to_close=timedelta(seconds=31),
        heartbeat_timeout=timedelta(seconds=6),
        error_traceback="tb",
        progress_state={"step": 2},
        progress_seq=3,
        idempotency_scope="scope",
        trace_id="trace",
        span_id="span",
        tags=("t1", "t2"),
        retry_base=timedelta(seconds=17),
        retry_cap=timedelta(seconds=915),
        retry_backoff="linear",
        retry_jitter=0.5,
        interrupt_count=4,
        claim_epoch=7,
        assignment_routed=True,
    )
    memory._jobs[JobId(planted.id)] = planted  # pyright: ignore[reportPrivateUsage]  # Why: test-only private seeding, the established same-module pattern (DiffSide.plant).
    side.register_job_id("j1", JobId(planted.id))

    obs = await side._job_observable(  # pyright: ignore[reportPrivateUsage]  # Why: the self-test inspects the harness's own projection directly.
        JobId(planted.id),
        await side.now(),
        side._t0,  # pyright: ignore[reportPrivateUsage]  # Why: harness-owned anchor, the same reference snapshot() buckets against.
    )

    missing = {f.name for f in fields(JobRow)} - set(obs)
    assert not missing, (
        f"JobRow fields missing from the differential projection: {sorted(missing)} - "
        "a field the projection cannot serialize is a PG↔memory divergence the "
        "harness can never catch"
    )
    # Value fidelity for the fields the projection gained last: durations
    # project to seconds, the identity to the scenario token.
    assert obs["id"] == "j1"
    assert obs["retry_base"] == 17.0
    assert obs["retry_cap"] == 915.0
    assert obs["retry_backoff"] == "linear"
    assert obs["retry_jitter"] == 0.5
    assert obs["interrupt_count"] == 4
    assert obs["claim_epoch"] == 7
    assert obs["assignment_routed"] is True
