"""Fresh red-team eyes on the boundaries of the five deployment-wave fixes.

The fixing pass verified each fix's headline contract; this file re-attacks
the shapes at the EDGE of those pins - the reverse directions, the
combinatorics, the orderings, and the seam-scoping the fixing pass may have
left unpinned.  A green boundary here is release confidence; a red one is a
defect with evidence attached.  Per fix:

1. **The denial-reason boundary** - every ``mark_snoozed`` caller is
   enumerated and driven: a REAL saturation (acquire- or actor-raised)
   carries ``'capacity'`` and only the store-failure synthesis carries
   ``'unavailable'``, so the two causes stay distinguishable on the row.
   Both reasons take the identical non-consuming 429 path: no reason may
   suppress the deadline arm, and no denial - at any attempt number - may
   invent a terminal exit - on PG and on the in-memory twin.
2. **The depth-bounded dispatch SQL** - the combinatorics the one-actor
   oracle never touched: hundreds of actors behind one queue (the
   ``per_actor_capacity`` lateral fan-out), dozens of fairness cohorts at
   depth (the ``rr_keys`` global enumeration and the per-cohort probes),
   mixed-mode queue lists in one call, identity dedup under the new lateral
   shapes, a residual-0 actor beside a NULL-cap actor, and the empty
   queue-list contract.
3. **The mirror's batch atomicity pre-validation** - the two mixed-defect
   orderings (poison id in a capped-REFUSED group vs an admitted group),
   the three-way collision, and the fast tier's inheritance - each driven
   on BOTH backends and diffed.
4. **The lock-budget settings plumbing** - the edge values (0 / negative /
   huge) at the settings boundary and at the advisory-lock seam, and the
   honest scoping fact: the budgets bind the single-enqueue path only; the
   bulk tiers take no per-actor advisory lock at all.
5. **The cron stored-cap resolution** - NULL-stored revert, the LOOSEN
   direction, the singleton flag's registry-only independence, and a stored
   cap on an actor absent from the policy map entirely.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's migration-validated schema identifier (module_pg_schema / the depth-oracle-style throwaway schema), or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import redis
import redis.asyncio as redis_async
import structlog.testing
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.actor_config_ops import set_actor_config_capacity
from taskq.backend._dispatch import _dispatch_batch, _resolve_queue_modes
from taskq.backend._dispatch_sql import (
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.backend._dispatch_sql import (
    dispatch_batch as dispatch_batch_helper,
)
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.backend._sql_templates import render as render_sql_templates
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import (
    BatchMaxPendingExceededError,
    MaxPendingLockTimeoutError,
    ReservationUnavailable,
    Snooze,
)
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_running_job
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron
from taskq.worker.dispatch import dispatch_one_job
from tests._di_scopes import bootstrap_scopes, make_scopes

from .test_rt_cron_harness import (
    _TEN_MINUTELY,
    count_jobs,
    cron_settings,
    make_backend,
    pool_backend,
    seed_actor_config,
    seed_schedule,
    ten_min_floor,
)

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()
_DELAY = timedelta(seconds=30)
_LOCK_LEASE = timedelta(seconds=30)
_OVERSAMPLE = 2

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
async def attack_pool(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    """A small real pool over the invocation's PG database for the seams a
    raw connection cannot serve (``_dispatch_batch``'s pool acquire, the
    custom-settings PostgresBackend)."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    try:
        yield pool
    finally:
        await pool.close()


# ══════════════════════════════════════════════════════════════════════
# Fix 1 - the denial-reason carve-out
# ══════════════════════════════════════════════════════════════════════


class TestDenialReasonCarveOutBoundaries:
    """The ``'unavailable'`` reason must be reachable ONLY by the
    store-failure synthesis, and no denial - of either reason, at any
    attempt number - may terminalise the job outside its own deadline
    arm."""

    # ── PG arms ──────────────────────────────────────────────────────

    @pytest.mark.integration
    async def test_unavailable_denial_with_past_deadline_terminalises_on_pg(
        self,
        clean_jobs_app: JobsApp,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The non-consuming arm must not swallow the deadline exit: an
        ``'unavailable'`` denial on a job whose reschedule point is already
        past ``schedule_to_close`` lands the deadline arm - DeadlineExceeded,
        terminal - not an eternally-rescheduled row whose deadline silently
        stopped meaning anything."""
        schema = module_pg_schema.schema_name
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:
            worker_id = new_uuid()
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                max_attempts=3,
                retry_kind="transient",
                attempt=1,
                schedule_to_close=datetime.now(UTC) - timedelta(minutes=1),
            )
        job = JobId(job_id)

        outcome = await clean_jobs_app.backend.mark_snoozed(
            job,
            worker_id,
            _DELAY,
            outcome="rate_limit_denied",
            attempt=1,
            denial_reason="unavailable",
        )

        assert outcome == "failed", (
            "an 'unavailable' denial past its schedule_to_close deadline must "
            f"terminalise via the deadline arm; got {outcome!r} - the "
            "non-consuming arm's retry-budget protection is about BUDGET, not "
            "about suppressing the job's own deadline exit"
        )
        row = await clean_jobs_app.backend.get(job)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"
        assert row.attempt == 1, "the deadline arm never refunds the increment"
        assert row.rate_limit_blocked_count == 1, (
            "the denial that ran the job out of road still happened to it, "
            "and with no per-occurrence rows the aggregate is its only record "
            "- the terminal row must show the deadline was reached WHILE the "
            "job was starving for admission, not make that last denial vanish"
        )
        attempts = await clean_jobs_app.backend.get_attempts(job)
        assert len(attempts) == 1
        assert attempts[0].error_class == "DeadlineExceeded"

    @pytest.mark.integration
    async def test_unavailable_denial_at_exact_max_attempts_stays_scheduled_on_pg(
        self,
        clean_jobs_app: JobsApp,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """``attempt == max_attempts`` exactly, ``'unavailable'`` reason: the
        job stays scheduled with its claim increment refunded - the
        max_attempts arm is unreachable for an infra denial, so a sustained
        outage keeps the job retryable with its original budget intact."""
        schema = module_pg_schema.schema_name
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:
            worker_id = new_uuid()
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                max_attempts=2,
                retry_kind="transient",
                attempt=2,
            )
        job = JobId(job_id)

        outcome = await clean_jobs_app.backend.mark_snoozed(
            job,
            worker_id,
            _DELAY,
            outcome="rate_limit_denied",
            attempt=2,
            denial_reason="unavailable",
        )

        assert outcome == "scheduled", (
            "an 'unavailable' denial at attempt == max_attempts must stay in "
            f"the non-consuming snooze arm; got {outcome!r} - MaxAttemptsExceeded "
            "asserts the actor ran and failed max_attempts times, which a "
            "store outage can never claim"
        )
        row = await clean_jobs_app.backend.get(job)
        assert row is not None
        assert row.status == "scheduled"
        assert row.attempt == 1, (
            "the 'unavailable' arm refunds the claim's attempt increment "
            "(dispatch stamped attempt=2; the refund returns it to 1) so the "
            "outage consumes no budget"
        )
        assert row.rate_limit_blocked_count == 1
        attempts = await clean_jobs_app.backend.get_attempts(job)
        assert attempts == [], "a non-terminal denial writes no attempt rows"

    @pytest.mark.integration
    async def test_capacity_denial_at_exact_max_attempts_reschedules_on_pg(
        self,
        clean_jobs_app: JobsApp,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A saturation denial at ``attempt == max_attempts`` reschedules -
        the SAME non-consuming path an ``'unavailable'`` denial takes.

        Pinned to the 429 contract: a denial reports that the fleet had no
        slot, which says nothing about the work, so it can neither spend
        the retry budget nor decide the outcome - whatever the reason. The
        attempt ceiling is a bound on EXECUTIONS, and nothing executed.
        """
        schema = module_pg_schema.schema_name
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:
            worker_id = new_uuid()
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                max_attempts=2,
                retry_kind="transient",
                attempt=2,
            )
        job = JobId(job_id)

        outcome = await clean_jobs_app.backend.mark_snoozed(
            job,
            worker_id,
            _DELAY,
            outcome="rate_limit_denied",
            attempt=2,
            denial_reason="capacity",
        )

        assert outcome == "scheduled", (
            "a 'capacity' (saturation) denial at attempt == max_attempts must "
            f"stay in the non-consuming snooze arm; got {outcome!r} - "
            "MaxAttemptsExceeded asserts the actor ran and failed "
            "max_attempts times, which a full bucket can never claim"
        )
        row = await clean_jobs_app.backend.get(job)
        assert row is not None
        assert row.status == "scheduled"
        assert row.error_class is None
        assert row.attempt == 1, (
            "the denial arm refunds the claim's attempt increment (dispatch "
            "stamped attempt=2; the refund returns it to 1) so saturation "
            "consumes no budget"
        )
        assert row.max_attempts == 2, "the ceiling is a bound, never a counter"
        assert row.rate_limit_blocked_count == 1
        attempts = await clean_jobs_app.backend.get_attempts(job)
        assert attempts == [], "a non-terminal denial writes no attempt rows"

    # ── The in-memory twin ───────────────────────────────────────────

    async def test_mirror_matches_the_three_unavailable_boundaries(self) -> None:
        """All three arms on the in-memory twin: past-deadline terminalises,
        and exact-max stays scheduled with the refund for BOTH denial
        reasons - observably identical to the PG arms above."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        backend.register_actor_config(actor="mirror_actor")

        # (b) past deadline + 'unavailable' → deadline arm.
        job_id = new_uuid()
        await backend.enqueue(
            EnqueueArgs(
                id=job_id,
                actor="mirror_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_NOW,
            )
        )
        claimed = await backend.dispatch_batch(_WORKER_ID, ["default"], 1, _LOCK_LEASE)
        assert len(claimed) == 1
        running = await backend.get(job_id)
        assert running is not None
        assert running.status == "running"
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: forcing the row's deadline past the reschedule point, the same setup shape PG gets via create_running_job(schedule_to_close=past).
            running, schedule_to_close=_NOW - timedelta(minutes=1)
        )
        outcome = await backend.mark_snoozed(
            job_id,
            _WORKER_ID,
            _DELAY,
            outcome="rate_limit_denied",
            attempt=running.attempt,
            denial_reason="unavailable",
        )
        assert outcome == "failed", (
            f"mirror deadline arm: got {outcome!r} - the twin must terminalise "
            "an 'unavailable' denial past its deadline, never reschedule it"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"
        # The deadline arm counts the denial that ran the job out of road -
        # the terminal row must show it was starving when its deadline hit.
        assert row.rate_limit_blocked_count == 1

        # (c) attempt == max_attempts exactly + 'unavailable' → stays scheduled.
        cap_job = new_uuid()
        await backend.enqueue(
            EnqueueArgs(
                id=cap_job,
                actor="mirror_actor",
                queue="default",
                payload={},
                max_attempts=1,
                retry_kind="transient",
                scheduled_at=_NOW,
            )
        )
        claimed = await backend.dispatch_batch(_WORKER_ID, ["default"], 1, _LOCK_LEASE)
        assert len(claimed) == 1
        running = await backend.get(cap_job)
        assert running is not None
        assert running.attempt == 1 == running.max_attempts
        outcome = await backend.mark_snoozed(
            cap_job,
            _WORKER_ID,
            _DELAY,
            outcome="reservation_denied",
            attempt=running.attempt,
            denial_reason="unavailable",
        )
        assert outcome == "scheduled", (
            f"mirror exact-max arm: got {outcome!r} - the twin must never "
            "terminalise an 'unavailable' denial at the budget bound"
        )
        row = await backend.get(cap_job)
        assert row is not None
        assert row.status == "scheduled"
        assert row.attempt == 0, "the mirror refunds the increment, floored at 0"
        assert row.rate_limit_blocked_count == 1
        assert await backend.get_attempts(cap_job) == []

        # Same shape, 'capacity': the identical non-consuming path - a
        # saturation denial is a 429 exactly like a store outage.
        sat_job = new_uuid()
        await backend.enqueue(
            EnqueueArgs(
                id=sat_job,
                actor="mirror_actor",
                queue="default",
                payload={},
                max_attempts=1,
                retry_kind="transient",
                scheduled_at=_NOW,
            )
        )
        claimed = await backend.dispatch_batch(_WORKER_ID, ["default"], 1, _LOCK_LEASE)
        assert len(claimed) == 1
        running = await backend.get(sat_job)
        assert running is not None
        outcome = await backend.mark_snoozed(
            sat_job,
            _WORKER_ID,
            _DELAY,
            outcome="reservation_denied",
            attempt=running.attempt,
            denial_reason="capacity",
        )
        assert outcome == "scheduled", (
            f"mirror capacity arm at the budget bound: got {outcome!r} - the "
            "twin must never terminalise a denial at the budget bound either; "
            "the attempt ceiling bounds executions and nothing executed"
        )
        row = await backend.get(sat_job)
        assert row is not None
        assert row.status == "scheduled"
        assert row.error_class is None
        assert row.attempt == 0, "the mirror refunds the increment, floored at 0"
        assert row.rate_limit_blocked_count == 1
        assert await backend.get_attempts(sat_job) == []

    # ── The caller map: every consumer-driven mark_snoozed shape ────

    async def test_every_denial_caller_shape_carries_its_reason(self) -> None:
        """Every path that reaches ``mark_snoozed`` with a denial outcome,
        driven end-to-end through ``dispatch_one_job``, carries the reason
        its provenance demands: a REAL saturation (the limiter answered
        'full', or the actor raised the denial itself) rides ``'capacity'``
        while only the store-failure synthesis rides ``'unavailable'``.
        Both take the identical non-consuming 429 path - the label's job is
        keeping an outage distinguishable from saturation, so an operator
        never answers a dead store with more capacity."""
        # Acquire-path saturation: a one-slot reservation whose slot is
        # already held - the limiter's own 'full' answer.
        saturation = ConcurrencyReservation(
            name="sat_slots",
            slots=1,
            lease=timedelta(minutes=5),
            clock=FakeClock(_NOW),
        )
        await saturation.acquire(new_uuid(), new_uuid())
        fake_backend, actor_runs = await _dispatch_with(
            _noop_actor,
            reservations=[saturation],
        )
        assert actor_runs == 0
        _assert_single_snooze(fake_backend, outcome="reservation_denied", reason="capacity")

        # In-actor saturation: the actor body raises the denial itself.
        fake_backend, actor_runs = await _dispatch_with(_reservation_raising_actor)
        assert actor_runs == 1
        _assert_single_snooze(fake_backend, outcome="reservation_denied", reason="capacity")

        # In-actor saturation from a rate-limit-shaped source: the reason is
        # still 'capacity' (the store answered, and its answer was "full");
        # the source label is the otel counter's dimension, not a reason.
        fake_backend, actor_runs = await _dispatch_with(_rate_limit_raising_actor)
        assert actor_runs == 1
        _assert_single_snooze(fake_backend, reason="capacity")

        # Store-failure synthesis: the limiter's store could not answer.
        fake_backend, actor_runs = await _dispatch_with(
            _noop_actor,
            rate_limits=[
                TokenBucket(
                    name="outage_bucket", capacity=5, refill_per_second=1.0, backend="redis"
                )
            ],
            redis_error=redis.ConnectionError("redis unreachable"),
            fallback_enabled=False,
        )
        assert actor_runs == 0
        _assert_single_snooze(fake_backend, outcome="rate_limit_denied", reason="unavailable")

        # Actor-requested deferral: the 'snoozed' outcome (non-consuming by
        # its own arm, denial_reason immaterial to the arms).
        fake_backend, actor_runs = await _dispatch_with(_snoozing_actor)
        assert actor_runs == 1
        _assert_single_snooze(fake_backend, outcome="snoozed", reason="capacity")


def _assert_single_snooze(
    fake_backend: FakeBackend,
    *,
    outcome: str | None = None,
    reason: str,
) -> None:
    assert fake_backend.mark_failed_or_retry_calls == [], (
        "a denial-class outcome must reach mark_snoozed, never the failure "
        "accounting of mark_failed_or_retry - that burns an attempt and "
        "persists the denial as the job's own error_class"
    )
    snoozes = fake_backend.mark_snoozed_calls
    assert len(snoozes) == 1, f"exactly one snooze write expected; got {len(snoozes)}"
    if outcome is not None:
        assert snoozes[0]["outcome"] == outcome
    assert snoozes[0]["denial_reason"] == reason, (
        f"the denial's provenance rides denial_reason: got "
        f"{snoozes[0]['denial_reason']!r}, expected {reason!r} - both reasons "
        "are non-consuming, so the label is how a store outage stays "
        "distinguishable from real saturation on the row; a saturation "
        "mislabelled 'unavailable' sends the operator hunting an outage "
        "that never happened, and an outage mislabelled 'capacity' gets "
        "answered with more capacity"
    )


# ── The dispatch_one_job drive harness (the depfail family's shape) ──


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


class _FakeWorkerDeps:
    def __init__(self) -> None:
        self.active_jobs = None
        self.worker_pool: Any = None
        self.slot_pool: Any = None
        self.settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
        )
        self.settings.worker_group = "default"
        self.redis_client: Any = None
        self.progress_buffers: dict[Any, Any] = {}
        self.disowned_jobs: set[UUID] = set()


class _RaisingScript:
    """AsyncScript double: every invocation raises the injected error."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    async def __call__(self, **kwargs: object) -> object:
        raise self._error


def _dead_redis_client(error: Exception) -> redis_async.Redis:
    client = redis_async.Redis(host="127.0.0.1", port=1, decode_responses=False)
    client.register_script = lambda script: _RaisingScript(error)  # type: ignore[method-assign]  # Why: injecting the failure at the script-call seam redis-py would use; no connection exists.
    return client


_ACTOR_RUNS = [0]


async def _noop_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1


async def _reservation_raising_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1
    raise ReservationUnavailable("downstream_pool", timedelta(seconds=5))


async def _rate_limit_raising_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1
    raise ReservationUnavailable("downstream_bucket", timedelta(seconds=5), source="rate_limit")


async def _snoozing_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1
    raise Snooze(timedelta(seconds=60))


class _ScopeStack:
    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    async def __aenter__(self) -> _ScopeStack:
        self.registry.validate()
        scopes = make_scopes(self.registry)
        self.process_scope, self.thread_scope, self.loop_scope = scopes
        await bootstrap_scopes(self.registry, *scopes)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()


async def _dispatch_with(
    actor_fn: Any,
    *,
    rate_limits: list[TokenBucket] | None = None,
    reservations: list[ConcurrencyReservation] | None = None,
    redis_error: Exception | None = None,
    fallback_enabled: bool = True,
) -> tuple[FakeBackend, int]:
    """Drive one ``dispatch_one_job`` against a FakeBackend with the given
    actor body and limiter wiring, returning the backend and the actor's
    run count."""
    from taskq.ratelimit._provider import register_rate_limit_registry

    rl_registry = RateLimitRegistry()
    for limit in rate_limits or []:
        rl_registry.register(limit)
    for res in reservations or []:
        rl_registry.register(res)
    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)
    if redis_error is not None:
        di_registry.register_value(redis_async.Redis, Scope.LOOP, _dead_redis_client(redis_error))

    fake_backend = FakeBackend()
    fake_deps = _FakeWorkerDeps()
    fake_deps.settings.rate_limit_pg_fallback_enabled = fallback_enabled
    _ACTOR_RUNS[0] = 0

    async with _ScopeStack(di_registry) as scopes:
        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=fake_deps,  # type: ignore[arg-type]  # Why: same Any-cast seam as the depfail family's dispatch drives.
            job=make_job_row(payload={"value": 42}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=ActorRef(
                name="test_actor",
                queue="default",
                fn=actor_fn,
                wants_ctx=True,
                dependencies={},
                payload_type=_Payload,
                result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; result_adapter is not used on the dispatch path.
                retry=RetryPolicy(),
                result_ttl=None,
                rate_limits=rate_limits or [],
                reservations=reservations or [],
            ),
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )
    return fake_backend, _ACTOR_RUNS[0]


# ══════════════════════════════════════════════════════════════════════
# Fix 2 - the depth-bounded dispatch SQL's combinatorics
# ══════════════════════════════════════════════════════════════════════


async def _register_actor(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    queue: str = "default",
    *,
    max_concurrent: int | None = None,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) VALUES ($1, $2, $3)',
        actor,
        queue,
        max_concurrent,
    )


async def _add_queue(conn: asyncpg.Connection, schema: str, name: str, mode: str) -> None:
    await conn.execute(f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2)', name, mode)


async def _add_jobs(
    conn: asyncpg.Connection,
    schema: str,
    ids: list[UUID],
    *,
    actor: str,
    queue: str,
    fairness_key: str | None = None,
    identity_key: str | None = None,
    priority: int = 0,
    status: str = "pending",
) -> None:
    """Insert job rows with explicit ids, due, identity-free unless given.

    ``scheduled_at`` is stamped a minute in the past so every row is due;
    running rows carry a live lease and a holder so the concurrency counts
    see them.
    """
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, priority, attempt, max_attempts, "
        "retry_kind, scheduled_at, fairness_key, identity_key, locked_by_worker, "
        "lock_expires_at, started_at, last_heartbeat_at) "
        f"VALUES ($1, $2, $3, '{{}}'::jsonb, $4::\"{schema}\".job_status, $5, 0, 3, "
        "'transient', clock_timestamp() - interval '1 minute', $6, $7, $8, "
        "clock_timestamp() + interval '5 minutes', "
        "CASE WHEN $4 = 'running' THEN clock_timestamp() END, "
        "CASE WHEN $4 = 'running' THEN clock_timestamp() END)",
        [
            (
                jid,
                actor,
                queue,
                status,
                priority,
                fairness_key,
                identity_key,
                new_uuid() if status == "running" else None,
            )
            for jid in ids
        ],
    )


async def _claim(
    conn: asyncpg.Connection,
    variant_sql: str,
    schema: str,
    queues: list[str],
    limit_n: int,
    *,
    oversample: int = _OVERSAMPLE,
) -> list[asyncpg.Record]:
    return await dispatch_batch_helper(
        conn,
        sql=variant_sql.format(schema=schema),
        queues=queues,
        limit_n=limit_n,
        worker_id=_WORKER_ID,
        lock_lease=_LOCK_LEASE,
        oversample=oversample,
    )


class TestDispatchCombinatorics:
    """The depth oracle pinned ONE actor/queue/cohort.  These drive the
    combinatorics the restructured CTEs meet in production."""

    pytestmark = pytest.mark.integration

    async def test_hundreds_of_actors_one_queue_claims_the_limit_exactly(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """150 carrying actors plus 300 registered-but-idle actors behind one
        queue: the ``per_actor_capacity`` lateral fan-out probes every
        carrying actor, skips every idle one, and the round claims exactly
        ``limit_n`` rows - the highest-priority one-per-actor set, nothing
        more, nothing from an idle actor."""
        schema = module_pg_schema.schema_name
        expected: dict[int, UUID] = {}
        for i in range(150):
            actor = f"fan_{i:03d}"
            await _register_actor(clean_pg_conn, schema, actor, "fan_q")
            priority = 150 - i  # fan_000 carries 150 … fan_149 carries 1
            job_id = new_uuid()
            await _add_jobs(
                clean_pg_conn, schema, [job_id], actor=actor, queue="fan_q", priority=priority
            )
            expected[priority] = job_id
        for i in range(300):
            await _register_actor(clean_pg_conn, schema, f"idle_{i:03d}", "fan_q")

        rows = await _claim(clean_pg_conn, DISPATCH_STRICT_FIFO_SQL, schema, ["fan_q"], 37)

        assert len(rows) == 37
        claimed = {row["id"] for row in rows}
        expected_ids = {expected[p] for p in range(150, 113, -1)}
        assert claimed == expected_ids, (
            "the round must claim exactly the 37 highest-priority one-per-actor "
            "rows under the lateral fan-out - a lost actor (probe skipped) or "
            "an idle actor's phantom row (probe not skipping) both break this set"
        )
        for row in rows:
            assert row["status"] == "running"
            assert row["locked_by_worker"] == _WORKER_ID

    async def test_global_cohort_enumeration_leaks_no_unsubscribed_queue(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """``rr_keys`` enumerates cohorts GLOBALLY (the recursive term cannot
        be correlated), and the pair filter narrows them in memory - so a
        round scoped to one queue must claim nothing from an actor's rows on
        another queue, and the same actor must be fully claimable when the
        round subscribes to both."""
        schema = module_pg_schema.schema_name
        await _add_queue(clean_pg_conn, schema, "in_round", "round_robin")
        await _add_queue(clean_pg_conn, schema, "other_queue", "round_robin")
        await _register_actor(clean_pg_conn, schema, "split_actor", "in_round")
        in_round_ids = [new_uuid() for _ in range(3)]
        other_ids = [new_uuid() for _ in range(3)]
        for i, jid in enumerate(in_round_ids):
            await _add_jobs(
                clean_pg_conn,
                schema,
                [jid],
                actor="split_actor",
                queue="in_round",
                fairness_key=f"c{i}",
            )
        for i, jid in enumerate(other_ids):
            await _add_jobs(
                clean_pg_conn,
                schema,
                [jid],
                actor="split_actor",
                queue="other_queue",
                fairness_key=f"z{i}",
            )
        await _register_actor(clean_pg_conn, schema, "bystander", "other_queue")
        bystander_ids = [new_uuid() for _ in range(2)]
        for jid in bystander_ids:
            await _add_jobs(
                clean_pg_conn,
                schema,
                [jid],
                actor="bystander",
                queue="other_queue",
                fairness_key="zb",
            )

        rows = await _claim(clean_pg_conn, DISPATCH_ROUND_ROBIN_SQL, schema, ["in_round"], 10)
        assert {row["id"] for row in rows} == set(in_round_ids), (
            "a round scoped to 'in_round' must claim only that queue's rows - "
            "a cohort from the global enumeration leaking through the pair "
            "filter claims rows a worker never subscribed to"
        )

        rows = await _claim(
            clean_pg_conn,
            DISPATCH_ROUND_ROBIN_SQL,
            schema,
            ["in_round", "other_queue"],
            10,
        )
        assert {row["id"] for row in rows} == set(other_ids) | set(bystander_ids), (
            "subscribing to both queues must claim every remaining pending row "
            "of BOTH actors on those queues - the in_round rows are already "
            "running from the round above, so this round's set is exactly the "
            "other_queue rows of both the split actor and the bystander"
        )

    async def test_every_cohort_contributes_candidates_under_depth(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """30 fairness cohorts of 4 due rows each, one actor, one round-robin
        queue, ``limit_n`` 60: the per-cohort bounded probes must surface
        every cohort's top rows - the claimed set carries exactly two rows
        from EVERY cohort (all rank-1s then all rank-2s), the starvation
        invariant under the new probe shape."""
        schema = module_pg_schema.schema_name
        await _add_queue(clean_pg_conn, schema, "cohort_q", "round_robin")
        await _register_actor(clean_pg_conn, schema, "cohort_actor", "cohort_q")
        by_cohort: dict[str, list[UUID]] = {f"fc_{i:02d}": [] for i in range(30)}
        for cohort, ids in by_cohort.items():
            ids.extend(new_uuid() for _ in range(4))
            await _add_jobs(
                clean_pg_conn,
                schema,
                ids,
                actor="cohort_actor",
                queue="cohort_q",
                fairness_key=cohort,
            )

        rows = await _claim(clean_pg_conn, DISPATCH_ROUND_ROBIN_SQL, schema, ["cohort_q"], 60)

        assert len(rows) == 60
        claimed_by_cohort: dict[str, int] = {}
        for row in rows:
            cohort = row["fairness_key"]
            assert cohort is not None
            claimed_by_cohort[cohort] = claimed_by_cohort.get(cohort, 0) + 1
        missing = sorted(set(by_cohort) - set(claimed_by_cohort))
        assert missing == [], (
            f"cohorts {missing} contributed no candidates - a deep cohort "
            "crowded out a shallow one: the per-cohort probe bound exists "
            "precisely so one cohort's depth cannot silence another's rows"
        )
        wrong = {k: v for k, v in claimed_by_cohort.items() if v != 2}
        assert wrong == {}, (
            f"cohort counts {wrong} - with 120 candidates ranked by "
            "fairness_rank first, a 60-row round is exactly every cohort's "
            "rank-1 and rank-2 rows; anything else is a ranking regression"
        )

    async def test_deep_cohort_does_not_crowd_out_shallow_cohort(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """A 200-row cohort beside a 1-row cohort, ``limit_n`` 3: the
        shallow cohort's row is claimed in the same round as the deep
        cohort's top rows - depth delays a cohort's tail, it never removes
        another cohort's head from consideration."""
        schema = module_pg_schema.schema_name
        await _add_queue(clean_pg_conn, schema, "starve_q", "round_robin")
        await _register_actor(clean_pg_conn, schema, "starve_actor", "starve_q")
        deep_ids = [new_uuid() for _ in range(200)]
        await _add_jobs(
            clean_pg_conn,
            schema,
            deep_ids,
            actor="starve_actor",
            queue="starve_q",
            fairness_key="deep",
        )
        shallow_id = new_uuid()
        await _add_jobs(
            clean_pg_conn,
            schema,
            [shallow_id],
            actor="starve_actor",
            queue="starve_q",
            fairness_key="shallow",
            priority=10,
        )

        rows = await _claim(clean_pg_conn, DISPATCH_ROUND_ROBIN_SQL, schema, ["starve_q"], 3)

        assert len(rows) == 3
        assert shallow_id in {row["id"] for row in rows}, (
            "the single-row cohort lost its only row to the 200-row cohort's "
            "depth - the old starvation shape back under the new probe bound"
        )

    async def test_mixed_mode_batch_selects_round_robin_and_claims_both_queues(
        self,
        attack_pool: asyncpg.Pool,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One dispatch call over a strict_fifo queue and a round_robin
        queue: the resolver selects the round-robin CTE for the whole batch
        (the documented superset behaviour), says so observably, and the
        strict queue's rows still claim - its unkeyed rows form one
        ``__null__`` cohort whose rank order is its priority order."""
        schema = module_pg_schema.schema_name
        await _add_queue(clean_pg_conn, schema, "mixed_strict", "strict_fifo")
        await _add_queue(clean_pg_conn, schema, "mixed_rr", "round_robin")
        await _register_actor(clean_pg_conn, schema, "mixed_actor", "mixed_strict")
        strict_ids = {p: new_uuid() for p in (4, 3, 2, 1)}
        for priority, jid in strict_ids.items():
            await _add_jobs(
                clean_pg_conn,
                schema,
                [jid],
                actor="mixed_actor",
                queue="mixed_strict",
                priority=priority,
            )
        rr_ids = [new_uuid() for _ in range(4)]
        for i, jid in enumerate(rr_ids):
            await _add_jobs(
                clean_pg_conn,
                schema,
                [jid],
                actor="mixed_actor",
                queue="mixed_rr",
                fairness_key=f"mr{i}",
                priority=5,
            )

        with structlog.testing.capture_logs() as captured:
            rows = await _dispatch_batch(
                attack_pool,
                render_sql_templates(schema),
                _OVERSAMPLE,
                5.0,
                schema,
                _WORKER_ID,
                ["mixed_strict", "mixed_rr"],
                5,
                _LOCK_LEASE,
            )

        mixed_events = [e for e in captured if e.get("event") == "dispatch-mixed-queue-modes"]
        assert mixed_events, (
            "a mixed-mode queue list must surface the mode selection - the "
            "selection is silent by design in the SQL, so the debug log is "
            "the only observable an operator auditing a misconfigured fleet has"
        )
        assert mixed_events[0].get("selected_sql") == "round_robin"
        claimed = {row.id for row in rows}
        assert len(rows) == 5
        assert set(rr_ids) <= claimed, "every round_robin cohort row is a rank-1 row"
        assert claimed - set(rr_ids) == {strict_ids[4]}, (
            "under the round-robin CTE the strict queue's rows form one "
            "__null__ cohort, so only its rank-1 (priority 4) row survives a "
            "5-row round beside four rr rank-1 rows - the strict variant "
            "would have taken priorities 4, 3, and 2 instead"
        )

    async def test_identity_dedup_under_the_new_lateral_shapes(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """Candidates carrying identity_keys under the restructured
        laterals: a key with a RUNNING row is excluded entirely; two
        pending rows sharing a key admit exactly one (the better row);
        NULL-identity rows pass through untouched."""
        schema = module_pg_schema.schema_name
        await _register_actor(clean_pg_conn, schema, "dedup_actor")
        k1_best, k1_worse = new_uuid(), new_uuid()
        await _add_jobs(
            clean_pg_conn,
            schema,
            [k1_best],
            actor="dedup_actor",
            queue="default",
            identity_key="k1",
            priority=5,
        )
        await _add_jobs(
            clean_pg_conn,
            schema,
            [k1_worse],
            actor="dedup_actor",
            queue="default",
            identity_key="k1",
            priority=4,
        )
        k2_a, k2_b = new_uuid(), new_uuid()
        await _add_jobs(
            clean_pg_conn,
            schema,
            [k2_a, k2_b],
            actor="dedup_actor",
            queue="default",
            identity_key="k2",
            priority=9,
        )
        await _add_jobs(
            clean_pg_conn,
            schema,
            [new_uuid()],
            actor="dedup_actor",
            queue="default",
            identity_key="k2",
            priority=8,
            status="running",
        )
        unkeyed = new_uuid()
        await _add_jobs(
            clean_pg_conn,
            schema,
            [unkeyed],
            actor="dedup_actor",
            queue="default",
            priority=1,
        )

        rows = await _claim(clean_pg_conn, DISPATCH_STRICT_FIFO_SQL, schema, ["default"], 10)

        claimed = {row["id"] for row in rows}
        assert claimed == {k1_best, unkeyed}, (
            f"claimed {claimed}: the running k2 identity must exclude both its "
            "pending rows, the shared k1 identity must admit only its best "
            "row, and the NULL-identity row must pass through - the DISTINCT "
            "ON arm's contract under the new per-cohort probe shapes"
        )

    async def test_residual_zero_actor_coexists_with_null_cap_actor(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """An actor at ``max_concurrent`` (residual 0), an actor OVER it
        (the GREATEST arm must clamp to 0, never negative), a partially
        loaded actor (residual 1 claims exactly one), and a NULL-cap actor
        (residual = limit_n) in one round: each claims exactly what its own
        residual admits."""
        schema = module_pg_schema.schema_name
        await _register_actor(clean_pg_conn, schema, "sat_actor", max_concurrent=2)
        await _register_actor(clean_pg_conn, schema, "over_actor", max_concurrent=1)
        await _register_actor(clean_pg_conn, schema, "part_actor", max_concurrent=3)
        await _register_actor(clean_pg_conn, schema, "nullcap_actor")
        sat_ids = [new_uuid() for _ in range(3)]
        await _add_jobs(clean_pg_conn, schema, sat_ids, actor="sat_actor", queue="default")
        over_ids = [new_uuid() for _ in range(2)]
        await _add_jobs(clean_pg_conn, schema, over_ids, actor="over_actor", queue="default")
        await _add_jobs(
            clean_pg_conn,
            schema,
            [new_uuid() for _ in range(2)],
            actor="sat_actor",
            queue="default",
            status="running",
        )
        await _add_jobs(
            clean_pg_conn,
            schema,
            [new_uuid() for _ in range(3)],
            actor="over_actor",
            queue="default",
            status="running",
        )
        part_ids = [new_uuid() for _ in range(3)]
        await _add_jobs(clean_pg_conn, schema, part_ids, actor="part_actor", queue="default")
        await _add_jobs(
            clean_pg_conn,
            schema,
            [new_uuid() for _ in range(2)],
            actor="part_actor",
            queue="default",
            status="running",
        )
        null_ids = [new_uuid() for _ in range(2)]
        await _add_jobs(clean_pg_conn, schema, null_ids, actor="nullcap_actor", queue="default")

        rows = await _claim(clean_pg_conn, DISPATCH_STRICT_FIFO_SQL, schema, ["default"], 10)

        claimed: dict[str, set[UUID]] = {}
        for row in rows:
            claimed.setdefault(row["actor"], set()).add(row["id"])
        assert claimed.get("sat_actor", set()) == set(), (
            "an actor at max_concurrent (residual 0) must contribute no rows"
        )
        assert claimed.get("over_actor", set()) == set(), (
            "an actor OVER max_concurrent must clamp to residual 0 - the "
            "GREATEST arm exists so in_flight > max can never go negative "
            "and admit rows"
        )
        assert claimed.get("part_actor", set()) <= set(part_ids)
        assert len(claimed.get("part_actor", set())) == 1, (
            "a residual-1 actor claims exactly one row, not its whole oversampled candidate set"
        )
        assert claimed.get("nullcap_actor", set()) == set(null_ids), (
            "a NULL-cap actor's residual is limit_n - it must claim freely "
            "beside saturated siblings"
        )

    async def test_empty_queue_list_claims_nothing(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """The empty-list contract at three seams: the resolver answers
        ``{'strict_fifo'}``, both CTE variants claim nothing, and the
        in-memory twin agrees."""
        schema = module_pg_schema.schema_name
        await _register_actor(clean_pg_conn, schema, "empty_list_actor")
        seeded = [new_uuid() for _ in range(3)]
        await _add_jobs(clean_pg_conn, schema, seeded, actor="empty_list_actor", queue="default")

        modes = await _resolve_queue_modes(clean_pg_conn, [], schema)
        assert modes == {"strict_fifo"}

        for variant in (DISPATCH_STRICT_FIFO_SQL, DISPATCH_ROUND_ROBIN_SQL):
            rows = await _claim(clean_pg_conn, variant, schema, [], 5)
            assert rows == [], (
                "an empty queue list claims nothing on either variant - "
                "unnest of an empty array is no rows, never a default queue"
            )

        remaining = await clean_pg_conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE id = ANY($1) AND status = 'pending'",
            seeded,
        )
        assert remaining == 3

        mirror = InMemoryBackend(clock=FakeClock(_NOW))
        mirror.register_actor_config(actor="empty_list_actor")
        await mirror.enqueue(
            EnqueueArgs(
                id=new_uuid(),
                actor="empty_list_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_NOW,
            )
        )
        claimed = await mirror.dispatch_batch(_WORKER_ID, [], 5, _LOCK_LEASE)
        assert claimed == [], "the in-memory twin agrees: empty list, no claims"


# ══════════════════════════════════════════════════════════════════════
# Fix 3 - the mirror's batch atomicity pre-validation
# ══════════════════════════════════════════════════════════════════════


def _batch_args(
    ids: list[UUID],
    actor: str,
    *,
    max_pending: int | None = None,
) -> list[EnqueueArgs]:
    return [
        EnqueueArgs(
            id=jid,
            actor=actor,
            queue="default",
            payload={"i": str(jid)},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_NOW,
            max_pending=max_pending,
        )
        for jid in ids
    ]


def _single_args(
    jid: UUID,
    actor: str,
    *,
    max_pending: int | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=jid,
        actor=actor,
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_NOW,
        max_pending=max_pending,
    )


async def _store_mirror_row(backend: InMemoryBackend, jid: UUID, actor: str) -> None:
    await backend.enqueue(
        EnqueueArgs(
            id=jid,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_NOW,
        )
    )


async def _mirror_with_pending(actor: str, *, cap: int = 1) -> InMemoryBackend:
    """A mirror holding ONE pre-existing pending row for *actor* - the
    over-cap seed the batch partition refuses."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    await backend.enqueue(
        EnqueueArgs(
            id=new_uuid(),
            actor=actor,
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_NOW,
            max_pending=cap,
        )
    )
    return backend


def _mirror_pending(backend: InMemoryBackend, actor: str) -> int:
    return sum(
        1
        for row in backend._jobs.values()  # pyright: ignore[reportPrivateUsage]  # Why: counting the mirror's stored rows per actor - the store IS the observable under test.
        if row.actor == actor and row.status in ("pending", "scheduled")
    )


async def _pg_pending(app: JobsApp, schema: str, actor: str) -> int:
    async with app.deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE actor = $1 AND status IN ('pending', 'scheduled')",
            actor,
        )
    assert count is not None
    return int(count)


class TestMirrorBatchAtomicityOrderings:
    """The two mixed-defect orderings, the three-way collision, and the fast
    tier - each driven on the mirror AND on PG, and diffed."""

    pytestmark = pytest.mark.integration

    async def test_poison_id_in_admitted_group_aborts_whole_batch_on_both_backends(
        self, clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
    ) -> None:
        """A stored-colliding id among the ADMITTED actor's items: whole-call
        abort with nothing admitted - the PG bulk tier's single-statement
        atomicity, mirrored exactly (exception type and stored-row state)."""
        from asyncpg.exceptions import UniqueViolationError

        # ── mirror ──
        mirror = await _mirror_with_pending("mem_cap")
        poison = new_uuid()
        await _store_mirror_row(mirror, poison, "mem_store")
        with pytest.raises(UniqueViolationError):
            await mirror.enqueue_batch(
                _batch_args([new_uuid(), new_uuid()], "mem_clean")
                + _batch_args([poison], "mem_clean")
                + _batch_args([new_uuid(), new_uuid()], "mem_cap", max_pending=1)
            )
        assert _mirror_pending(mirror, "mem_clean") == 0, (
            "the mirror pre-validates BEFORE its first insert, so a poisoned "
            "admitted group leaves the good prefix unstored - a stored prefix "
            "certifies code that leaves phantom rows behind on PG"
        )
        assert _mirror_pending(mirror, "mem_cap") == 1

        # ── PG ──
        schema = module_pg_schema.schema_name
        backend = clean_jobs_app.backend
        await backend.enqueue(_single_args(new_uuid(), "pg_cap", max_pending=1))
        pg_poison = new_uuid()
        await backend.enqueue(_single_args(pg_poison, "pg_store"))
        with pytest.raises(UniqueViolationError):
            await backend.enqueue_batch(
                _batch_args([new_uuid(), new_uuid()], "pg_clean")
                + _batch_args([pg_poison], "pg_clean")
                + _batch_args([new_uuid(), new_uuid()], "pg_cap", max_pending=1)
            )
        assert await _pg_pending(clean_jobs_app, schema, "pg_clean") == 0, (
            "PG's single unnest INSERT aborts the whole call - the admitted "
            "group's good prefix must not survive the poisoned item"
        )
        assert await _pg_pending(clean_jobs_app, schema, "pg_cap") == 1

    async def test_poison_id_in_capped_refused_group_spares_admitted_group(
        self, clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
    ) -> None:
        """The OTHER ordering: the poison id rides an over-cap actor's items,
        which are refused as a group before any INSERT - so the collision is
        never reached, the admitted group stores, and the typed refusal
        raises after.  Both backends, same observable state."""

        # ── mirror ──
        mirror = await _mirror_with_pending("mem_cap")
        poison = new_uuid()
        await _store_mirror_row(mirror, poison, "mem_store")
        with pytest.raises(BatchMaxPendingExceededError):
            await mirror.enqueue_batch(
                _batch_args([new_uuid(), new_uuid()], "mem_clean")
                + _batch_args([new_uuid(), poison], "mem_cap", max_pending=1)
            )
        assert _mirror_pending(mirror, "mem_clean") == 2, (
            "a poison id among a REFUSED group's items must not abort the "
            "call - those items never reach the INSERT on either backend, "
            "and the admitted group's rows are durable"
        )
        assert _mirror_pending(mirror, "mem_cap") == 1

        # ── PG ──
        schema = module_pg_schema.schema_name
        backend = clean_jobs_app.backend
        await backend.enqueue(_single_args(new_uuid(), "pg_cap", max_pending=1))
        pg_poison = new_uuid()
        await backend.enqueue(_single_args(pg_poison, "pg_store"))
        with pytest.raises(BatchMaxPendingExceededError) as refusal:
            await backend.enqueue_batch(
                _batch_args([new_uuid(), new_uuid()], "pg_clean")
                + _batch_args([new_uuid(), pg_poison], "pg_cap", max_pending=1)
            )
        assert await _pg_pending(clean_jobs_app, schema, "pg_clean") == 2
        assert await _pg_pending(clean_jobs_app, schema, "pg_cap") == 1
        refused_actors = {r.actor for r in refusal.value.refusals}
        assert refused_actors == {"pg_cap"}, (
            "the typed refusal names the over-cap actor's group - the "
            "collision never surfaced because the group never reached the INSERT"
        )

    async def test_three_way_collision_and_in_batch_duplicate_abort_on_both_backends(
        self, clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
    ) -> None:
        """(b) Duplicate ids WITHIN the batch, with and without a stored
        collision: both backends abort the entire call with the same
        exception type and store nothing."""
        from asyncpg.exceptions import UniqueViolationError

        # ── mirror: stored + in-batch (three-way) ──
        mirror = InMemoryBackend(clock=FakeClock(_NOW))
        stored = new_uuid()
        await _store_mirror_row(mirror, stored, "mem_store")
        with pytest.raises(UniqueViolationError):
            await mirror.enqueue_batch(_batch_args([stored, stored], "mem_clean"))
        assert _mirror_pending(mirror, "mem_clean") == 0

        # ── mirror: in-batch only ──
        fresh_twice = new_uuid()
        with pytest.raises(UniqueViolationError):
            await mirror.enqueue_batch(_batch_args([fresh_twice, fresh_twice], "mem_clean"))
        assert _mirror_pending(mirror, "mem_clean") == 0

        # ── PG: stored + in-batch (three-way) ──
        schema = module_pg_schema.schema_name
        backend = clean_jobs_app.backend
        pg_stored = new_uuid()
        await backend.enqueue(_single_args(pg_stored, "pg_store"))
        with pytest.raises(UniqueViolationError):
            await backend.enqueue_batch(_batch_args([pg_stored, pg_stored], "pg_clean"))
        assert await _pg_pending(clean_jobs_app, schema, "pg_clean") == 0

        # ── PG: in-batch only - the single INSERT statement carries both rows,
        # so the second violates the first inside the statement. ──
        pg_fresh = new_uuid()
        with pytest.raises(UniqueViolationError):
            await backend.enqueue_batch(_batch_args([pg_fresh, pg_fresh], "pg_clean"))
        assert await _pg_pending(clean_jobs_app, schema, "pg_clean") == 0

    async def test_fast_tier_inherits_the_admitted_subset_id_check(
        self, clean_jobs_app: JobsApp, module_pg_schema: ModulePgSchema
    ) -> None:
        """(c) ``enqueue_batch_fast``: a poisoned admitted group aborts the
        whole COPY-batch on both backends; a poisoned REFUSED group spares
        the admitted records - the fast tier inherits the same partition and
        the same whole-call atomicity as the unnest tier."""
        from asyncpg.exceptions import UniqueViolationError

        # ── mirror: poison admitted ──
        mirror = InMemoryBackend(clock=FakeClock(_NOW))
        mem_poison = new_uuid()
        await _store_mirror_row(mirror, mem_poison, "mem_store")
        with pytest.raises(UniqueViolationError):
            await mirror.enqueue_batch_fast(_batch_args([new_uuid(), mem_poison], "mem_clean"))
        assert _mirror_pending(mirror, "mem_clean") == 0

        # ── mirror: poison refused ──
        refused_mirror = await _mirror_with_pending("mem_cap")
        with pytest.raises(BatchMaxPendingExceededError):
            await refused_mirror.enqueue_batch_fast(
                _batch_args([new_uuid(), new_uuid()], "mem_clean")
                + _batch_args([new_uuid(), mem_poison], "mem_cap", max_pending=1)
            )
        assert _mirror_pending(refused_mirror, "mem_clean") == 2

        # ── PG: poison admitted - the COPY has no ON CONFLICT arbiter. ──
        schema = module_pg_schema.schema_name
        backend = clean_jobs_app.backend
        pg_poison = new_uuid()
        await backend.enqueue(_single_args(pg_poison, "pg_store"))
        with pytest.raises(UniqueViolationError):
            await backend.enqueue_batch_fast(_batch_args([new_uuid(), pg_poison], "pg_clean"))
        assert await _pg_pending(clean_jobs_app, schema, "pg_clean") == 0

        # ── PG: poison refused ──
        await backend.enqueue(_single_args(new_uuid(), "pg_cap", max_pending=1))
        with pytest.raises(BatchMaxPendingExceededError):
            await backend.enqueue_batch_fast(
                _batch_args([new_uuid(), new_uuid()], "pg_clean")
                + _batch_args([new_uuid(), pg_poison], "pg_cap", max_pending=1)
            )
        assert await _pg_pending(clean_jobs_app, schema, "pg_clean") == 2


# ══════════════════════════════════════════════════════════════════════
# Fix 4 - the lock-budget settings plumbing
# ══════════════════════════════════════════════════════════════════════


class TestLockBudgetSettingsPlumbing:
    """The three enqueue advisory-lock budgets: edge values at the settings
    boundary, the ``<= 0`` convention at the acquire seam, and the honest
    scoping fact - single path only."""

    async def test_zero_negative_and_huge_budgets_are_the_documented_opt_out(
        self,
    ) -> None:
        """0, negative, and huge env values are ACCEPTED by validation - the
        settings field documents ``0 or less waits indefinitely`` (the
        ``lock_timeout`` GUC convention), so the boundary's contract is
        opt-in-infinite, never a silent refusal and never a silent infinite
        the docs do not declare."""
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
                "TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS": "0",
                "TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS": "-2500",
                "TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS": "1000000000",
            }
        )
        assert settings.max_pending_lock_timeout_ms == 0.0
        assert settings.unique_for_lock_timeout_ms == -2500.0
        assert settings.idempotency_lock_timeout_ms == 1_000_000_000.0

    @pytest.mark.integration
    async def test_zero_budget_waits_through_a_briefly_held_lock(
        self,
        attack_pool: asyncpg.Pool,
        module_pg_schema: ModulePgSchema,
        pg_dsn: str,
    ) -> None:
        """A 0 budget at the real seam: the enqueue on a capped actor whose
        advisory lock is held for a few hundred milliseconds WAITS and
        succeeds - the documented indefinite branch - while a 50 ms budget
        on the same contention refuses with the typed error.  The env value
        demonstrably reaches the lock use site."""
        schema = module_pg_schema.schema_name
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS": "0",
            }
        )
        backend = pool_backend(settings, attack_pool)
        lock_key = f"taskq:max_pending:{schema}:budget_actor"
        holder = await asyncpg.connect(pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lock_key
                )
                enqueued = asyncio.ensure_future(
                    backend.enqueue(_single_args(new_uuid(), "budget_actor", max_pending=5))
                )
                await asyncio.sleep(0.3)
            row = await asyncio.wait_for(enqueued, timeout=5.0)
        finally:
            await holder.close()
        assert row.actor == "budget_actor", (
            "a 0 budget must WAIT for the holder's release and complete the "
            "enqueue - the lock_timeout GUC convention the settings field "
            "documents, reachable from the env value"
        )

        refusing_settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS": "50",
            }
        )
        refusing_backend = pool_backend(refusing_settings, attack_pool)
        holder = await asyncpg.connect(pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lock_key
                )
                with pytest.raises(MaxPendingLockTimeoutError):
                    await asyncio.wait_for(
                        refusing_backend.enqueue(
                            _single_args(new_uuid(), "budget_actor", max_pending=5)
                        ),
                        timeout=5.0,
                    )
        finally:
            await holder.close()

    @pytest.mark.integration
    async def test_budgets_bind_the_single_path_only_bulk_tiers_are_lock_free(
        self,
        attack_pool: asyncpg.Pool,
        module_pg_schema: ModulePgSchema,
        pg_dsn: str,
    ) -> None:
        """The scoping boundary, pinned explicitly: with the max_pending
        advisory lock held elsewhere and a 50 ms budget, the SINGLE enqueue
        refuses with the typed error while ``enqueue_batch`` and
        ``enqueue_batch_fast`` on the same capped actor at the same moment
        both succeed - the bulk tiers take no per-actor advisory lock at all
        (the count-then-insert race there is documented, not silent)."""
        schema = module_pg_schema.schema_name
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS": "50",
            }
        )
        backend = pool_backend(settings, attack_pool)
        lock_key = f"taskq:max_pending:{schema}:bulk_actor"
        holder = await asyncpg.connect(pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lock_key
                )
                with pytest.raises(MaxPendingLockTimeoutError):
                    await asyncio.wait_for(
                        backend.enqueue(_single_args(new_uuid(), "bulk_actor", max_pending=5)),
                        timeout=5.0,
                    )
                batch_ids = [new_uuid(), new_uuid()]
                rows = await backend.enqueue_batch(
                    _batch_args(batch_ids, "bulk_actor", max_pending=5)
                )
                assert {row.id for row in rows} == set(batch_ids)
                fast_id = new_uuid()
                count = await backend.enqueue_batch_fast(
                    _batch_args([fast_id], "bulk_actor", max_pending=5)
                )
                assert count == 1
        finally:
            await holder.close()


# ══════════════════════════════════════════════════════════════════════
# Fix 5 - the cron stored-cap resolution
# ══════════════════════════════════════════════════════════════════════


async def _seed_due_schedules(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    count: int,
    *,
    prefix: str,
) -> list[UUID]:
    """Staggered past-due ten-minutely slots, earliest first (the tick reads
    due rows ORDER BY next_fire_at, so admission order is deterministic)."""
    grid = ten_min_floor(datetime.now(UTC))
    ids: list[UUID] = []
    for i in range(count):
        ids.append(
            await seed_schedule(
                conn,
                schema,
                actor=actor,
                name=f"{prefix}-{i:02d}",
                cron_expr=_TEN_MINUTELY,
                next_fire_at=grid - timedelta(minutes=50 - i),
                identity_key=f"{prefix}-{i:02d}",
            )
        )
    return ids


class TestCronStoredCapBoundaries:
    """The stored-over-literal resolution's edges: the NULL revert, the
    LOOSEN direction, the singleton flag's independence, and the
    policy-map-absent actor."""

    pytestmark = pytest.mark.integration

    async def test_stored_null_reverts_to_the_literal_cap(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """(a) stored NULL + literal 5: the literal applies - five due
        schedules admit exactly five.  A NULL stored value is 'no stored
        override', never 'no cap'."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, "cron_revert")
        await _seed_due_schedules(clean_pg_conn, schema, "cron_revert", 5, prefix="rev")

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                _WORKER_ID,
                actor_policies={"cron_revert": ActorFirePolicy(singleton=False, max_pending=5)},
            )

        assert fired == 5
        assert await count_jobs(clean_pg_conn, schema, "cron_revert") == 5

    async def test_stored_cap_loosens_the_literal_cap(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """(b) stored 10 + literal 2: the tick admits up to TEN - the stored
        value is authoritative in BOTH directions, loosening the code
        literal exactly as it tightens it.  Ten due schedules all fire."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, "cron_loosen")
        await set_actor_config_capacity(clean_pg_conn, "cron_loosen", max_pending=10, schema=schema)
        await _seed_due_schedules(clean_pg_conn, schema, "cron_loosen", 10, prefix="loo")

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                _WORKER_ID,
                actor_policies={"cron_loosen": ActorFirePolicy(singleton=False, max_pending=2)},
            )

        assert fired == 10, (
            "a stored cap of 10 over a literal of 2 must LOOSEN the tick's "
            "admission to 10 - resolving the stored value only when it is "
            "tighter would silently strand the operator's loosening on the "
            "one admission surface that reads it"
        )
        assert await count_jobs(clean_pg_conn, schema, "cron_loosen") == 10

    async def test_singleton_gate_stays_registry_only_beside_a_stored_cap(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """(c) No invented coupling, either direction: a singleton actor
        with a stored cap fires exactly ONE of three due schedules (the
        singleton gate, registry-declared, dominating the cap), while an
        actor with the same stored cap but NO singleton flag fires all
        three - a stored max_pending never implies single-flight, and the
        singleton flag never consults the stored row."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, "cron_sing")
        await seed_actor_config(clean_pg_conn, schema, "cron_nosing")
        await set_actor_config_capacity(clean_pg_conn, "cron_sing", max_pending=5, schema=schema)
        await set_actor_config_capacity(clean_pg_conn, "cron_nosing", max_pending=5, schema=schema)
        await _seed_due_schedules(clean_pg_conn, schema, "cron_sing", 3, prefix="sng")
        await _seed_due_schedules(clean_pg_conn, schema, "cron_nosing", 3, prefix="nos")

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                _WORKER_ID,
                actor_policies={"cron_sing": ActorFirePolicy(singleton=True)},
            )

        assert fired == 4, "one singleton fire plus three capped-but-unflagged fires"
        assert await count_jobs(clean_pg_conn, schema, "cron_sing") == 1, (
            "a singleton actor with a stored cap of 5 must still fire exactly "
            "one job per tick - the singleton gate is registry-only and the "
            "stored cap must not override it"
        )
        assert await count_jobs(clean_pg_conn, schema, "cron_nosing") == 3, (
            "an actor with a stored cap but no singleton flag must fire up to "
            "its cap - a stored max_pending must never imply single-flight"
        )
        singleton_rows = await clean_pg_conn.fetch(
            f"SELECT metadata ->> 'singleton' AS singleton FROM \"{schema}\".jobs WHERE actor = $1",
            "cron_sing",
        )
        assert len(singleton_rows) == 1
        assert singleton_rows[0]["singleton"] == "true"

    async def test_stored_cap_alone_bounds_an_actor_absent_from_the_policy_map(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """(d) The drain shape generalized: an actor registered in
        actor_config but absent from the worker's policy map entirely (no
        flags declared, or a worker whose registry never registered it) -
        the stored cap alone must still bound the tick, including the
        stored-0 emergency drain."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, "cron_absent")
        await seed_actor_config(clean_pg_conn, schema, "cron_drain_absent")
        await set_actor_config_capacity(clean_pg_conn, "cron_absent", max_pending=2, schema=schema)
        await set_actor_config_capacity(
            clean_pg_conn, "cron_drain_absent", max_pending=0, schema=schema
        )
        await _seed_due_schedules(clean_pg_conn, schema, "cron_absent", 5, prefix="abs")
        await _seed_due_schedules(clean_pg_conn, schema, "cron_drain_absent", 5, prefix="drn")

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                _WORKER_ID,
                actor_policies={"unrelated_actor": ActorFirePolicy(singleton=False, max_pending=1)},
            )

        assert fired == 2, (
            "an actor absent from the policy map must still be bounded by its "
            "stored actor_config.max_pending alone - the tick reads the "
            "stored row for every schedule's actor, not only for actors the "
            "registry declared"
        )
        assert await count_jobs(clean_pg_conn, schema, "cron_absent") == 2
        assert await count_jobs(clean_pg_conn, schema, "cron_drain_absent") == 0, (
            "the stored-0 emergency drain must hold for a policy-absent actor "
            "too - 'never accept any jobs' cannot depend on the actor also "
            "being registered in this worker's code"
        )
