"""Audit-coverage pins: the worker hot paths' arms the suite never ran.

Every test here was written red-first against a specific uncovered line
(the audit-cov census, branch feat/audit-coverage); each documents the
arm it closes, and each was mutation-proven (flipping the target line
makes the test fail).

Closed arms:
  - ``dispatch._redis_client_type``: the ``[redis]``-extra ImportError
    guard (dispatch.py:105-106).
  - ``dispatch._ensure_registered_init_on_slot_conn``: the pool-already-
    carries-the-hook early return (dispatch.py:389).
  - ``dispatch_one_job``: the LOOP-scope ``asyncpg.Connection`` cache hit
    that selects the transaction connection (dispatch.py:502, 689) and the
    metadata batch_id stamp branch.
  - ``dispatch_one_job``: the batch-policy hook failing on the cancelled
    (dispatch.py:879-880), post-consume (dispatch.py:934-935) and
    failure-handler arms — a hook failure is logged, never an outcome.
  - ``consume_one_job``: the shutdown seam's release failing with an infra
    error → warn + disown (consumer 991-997), the release answering
    ``noop`` (consumer 1002), and the rate-limit release's failure warning
    (consumer 1568-1569).
  - ``consume_one_job``: a cancellation landing ON the interrupted release
    write disowns and re-raises (consumer 1326-1328).
  - ``_consume_transactional``: the actor's ``KeyboardInterrupt`` keeps
    its operator-intent semantics on the transactional path too
    (consumer 2185) — documented as defensive-unreachable, see the test.
"""

import asyncio
import sys
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_uuid
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.connections import with_connection_init
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.dispatch import (  # pyright: ignore[reportPrivateUsage]  # Why: the repair helper and import guard under test, the same seam test_slot_conn_init_repair.py pins
    _ensure_registered_init_on_slot_conn,
    _redis_client_type,
    dispatch_one_job,
)
from taskq.worker.shutdown import ShutdownPhase
from tests.test_dispatch_one_job import _FakeWorkerDeps, _make_actor_ref, _ScopeStack

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()
_LOG = structlog.get_logger("audit_cov_worker_arms")


class _Payload(BaseModel):
    value: int = 0


def _dispatch_mod() -> Any:
    """The dispatch module object (the monkeypatch target)."""

    return sys.modules["taskq.worker.dispatch"]


# ── the [redis]-extra import guard ──────────────────────────────────────


def test_redis_client_type_returns_none_without_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The LOOP-scope key resolves to None when the extra is absent — the
    shape a redis-less install dispatches with, resolved once at import."""
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)
    assert _redis_client_type() is None


# ── the slot-connection init hook's repair-path arms ────────────────────


class _PhysicalConn:
    def __init__(self) -> None:
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True


async def _applies_nothing(conn: Any) -> None:
    del conn


def _registry_with_hook(applied_to: list[object]) -> Any:
    async def hook(conn: Any) -> None:
        applied_to.append(conn)

    registry = ProviderRegistry()
    registry.register_factory(
        asyncpg.Connection,
        Scope.LOOP,
        with_connection_init(lambda: None, hook),
    )
    return registry


async def test_slot_conn_repair_returns_when_pool_already_carries_the_hook() -> None:
    """A pool bootstrap built carries ``slot_pool_connection_init``: the
    repair helper returns on the single attribute read — the hook is
    never applied twice to one connection, and the registry probe (which
    would apply it again) never runs."""
    applied_to: list[object] = []
    deps = MagicMock()
    deps.slot_pool_connection_init = _applies_nothing
    conn = _PhysicalConn()

    await _ensure_registered_init_on_slot_conn(
        conn,  # type: ignore[arg-type]  # Why: stand-in for the ConnLike runtime alias
        deps=deps,  # type: ignore[arg-type]
        registry=_registry_with_hook(applied_to),
        acquire_timeout=5.0,
        job_id=new_uuid(),
    )
    assert applied_to == [], (
        "the pool already carries the hook: the repair must return on the "
        "attribute read, never probe the registry and re-apply"
    )
    assert conn.terminated is False


# ── the LOOP-scope connection cache's transaction-conn arm ──────────────


async def test_loop_scope_connection_cache_selects_the_transaction_conn() -> None:
    """The dispatch path reads the cached ``asyncpg.Connection``
    resolution and hands it to the consumer as the transaction connection —
    the registry-trust seam (no isinstance). The job carries a batch_id
    stamp and a pending-status row (no started_at): both attribute arms of
    the pre-consume read run in one dispatch."""
    sentinel = object()

    registry = ProviderRegistry()
    registry.register_value(
        asyncpg.Connection,
        Scope.LOOP,
        sentinel,  # type: ignore[arg-type]  # Why: the registry-trust contract: the value under the asyncpg.Connection key IS the transaction connection, a test double stands in
    )

    captured: list[object] = []

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        conn: asyncpg.Connection,
    ) -> dict[str, object]:
        return {}

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 1}, status="pending")
        job = replace(job, metadata={"batch_id": "batch-1"})
        job_enqueuer = SubJobEnqueuer(
            backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
        )

        async def spy_consume(*args: Any, **kwargs: Any) -> str:
            captured.append(kwargs.get("transaction_conn"))
            return "succeeded"

        dispatch_mod = _dispatch_mod()
        original_consume = dispatch_mod.consume_one_job
        dispatch_mod.consume_one_job = spy_consume  # type: ignore[assignment]
        try:
            for _ in range(2):
                await dispatch_one_job(
                    backend=as_backend(fake_backend),
                    deps=fake_deps,  # type: ignore[arg-type]
                    job=job,
                    worker_id=_WORKER_ID,
                    registry=scopes.registry,
                    process_scope=scopes.process_scope,
                    thread_scope=scopes.thread_scope,
                    loop_scope=scopes.loop_scope,
                    actor_ref=actor_ref,  # type: ignore[arg-type]
                    actor_config=StubActorConfig(retry=RetryPolicy()),
                    clock=FakeClock(_NOW),
                    active_jobs=fake_deps.active_jobs,
                    enqueuer=job_enqueuer,
                )
        finally:
            dispatch_mod.consume_one_job = original_consume  # type: ignore[assignment]

    # The cache-backed seam: the LOOP-scope registration is visible to the
    # dispatch path's own read, so the consumer gets the connection as
    # the transaction connection (registry-trust, no isinstance).
    assert captured == [sentinel, sentinel]


# ── the batch-policy hook's failure arms ────────────────────────────────


async def test_batch_hook_failure_on_the_cancelled_arm_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[Any],
) -> None:
    """A batch hook failing after a cancellation is logged and the
    cancellation still propagates — the M7 stale-batch sweep is the net."""
    dispatch_mod = _dispatch_mod()

    async def raising_hook(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("batch hook exploded")

    async def cancelled_consume(*args: Any, **kwargs: Any) -> str:
        raise asyncio.CancelledError()

    monkeypatch.setattr(dispatch_mod, "apply_batch_terminal_outcome", raising_hook)
    monkeypatch.setattr(dispatch_mod, "consume_one_job", cancelled_consume)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(_noop_actor)

        with pytest.raises(asyncio.CancelledError):
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=fake_deps,  # type: ignore[arg-type]
                job=make_job_row(payload={"value": 1}),
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=fake_deps.active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )
    hook_failures = [e for e in structlog_capture if e["event"] == "batch-policy-hook-failed"]
    assert hook_failures, "the failed hook must be logged, the sweep is the net"
    assert hook_failures[0]["job_id"]


async def test_batch_hook_failure_on_the_post_consume_arm_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[Any],
) -> None:
    """A batch hook failing after a clean terminal write is logged — the
    job's outcome stands, the hook is observability, never accounting."""
    dispatch_mod = _dispatch_mod()

    async def raising_hook(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("batch hook exploded")

    monkeypatch.setattr(dispatch_mod, "apply_batch_terminal_outcome", raising_hook)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(_noop_actor)

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=fake_deps,  # type: ignore[arg-type]
            job=make_job_row(payload={"value": 1}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )
        assert len(fake_backend.mark_succeeded_calls) == 1
    hook_failures = [e for e in structlog_capture if e["event"] == "batch-policy-hook-failed"]
    assert hook_failures, "the failed hook must be logged, the sweep is the net"
    assert hook_failures[0]["job_id"]


async def test_batch_hook_failure_after_the_failure_handler_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[Any],
) -> None:
    """A batch hook failing after the failure handler's terminal write is
    logged — the row's outcome stands, the M7 sweep is the net."""
    dispatch_mod = _dispatch_mod()

    async def raising_hook(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("batch hook exploded")

    async def failing_consume(*args: Any, **kwargs: Any) -> str:
        raise RuntimeError("the consumer's own failure")

    monkeypatch.setattr(dispatch_mod, "apply_batch_terminal_outcome", raising_hook)
    monkeypatch.setattr(dispatch_mod, "consume_one_job", failing_consume)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(_noop_actor)

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=fake_deps,  # type: ignore[arg-type]
            job=make_job_row(payload={"value": 1}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )
        assert len(fake_backend.mark_failed_or_retry_calls) == 1
    hook_failures = [e for e in structlog_capture if e["event"] == "batch-policy-hook-failed"]
    assert hook_failures, "the failed hook must be logged, the sweep is the net"
    assert hook_failures[0]["job_id"]


async def _noop_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
    return {}


# ── the shutdown seam's release arms ────────────────────────────────────


class _FailingSnoozeBackend(FakeBackend):
    """mark_snoozed fails with the infra family: the release write dies."""

    async def mark_snoozed(self, *args: Any, **kwargs: Any) -> str:  # type: ignore[override]  # Why: the double widens the literal union to str; the mutation-proven arm reads the value it returns
        raise asyncpg.InterfaceError("pool is closing")


class _NoopSnoozeBackend(FakeBackend):
    """mark_snoozed answers noop: the row moved underneath this worker."""

    async def mark_snoozed(self, *args: Any, **kwargs: Any) -> str:  # type: ignore[override]  # Why: as above
        return "noop"


def _shutdown_deps() -> MagicMock:
    deps = MagicMock()
    deps.settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})
    deps.worker_pool = None
    deps.redis_client = None
    deps.progress_buffers = {}
    deps.disowned_jobs = set()
    deps.shutdown_phase = ShutdownPhase.DRAINING
    deps.shutdown_started_at = 1234.5  # a float: the seam's isinstance guard
    return deps


@pytest.mark.parametrize(
    ("backend", "expect_disown"),
    [
        (_FailingSnoozeBackend(), True),
        (_NoopSnoozeBackend(), False),
    ],
)
async def test_shutdown_release_infra_failure_disowns_and_noop_skips_publish(
    backend: Any, expect_disown: bool, structlog_capture: list[Any]
) -> None:
    """The shutdown seam's release: an infra failure warns, leaves the row
    to the lock-lease reclaim and DISOWNS the job; a noop release (the row
    re-owned mid-flight) skips the state-change publish and disowns
    nothing."""
    deps = _shutdown_deps()
    job = make_job_row(payload={})

    outcome = await asyncio.wait_for(
        _consume_with_shutdown_seam(backend, deps, job),
        timeout=10,
    )
    assert outcome == "scheduled"
    release_logs = [e for e in structlog_capture if e["event"].startswith("shutdown-release")]
    if expect_disown:
        assert job.id in deps.disowned_jobs
        assert any(e["event"] == "shutdown-release-failed" for e in release_logs), (
            "the infra-failed release must warn: the row's recovery is the "
            "lock-lease reclaim and the operator must know the release died"
        )
    else:
        assert job.id not in deps.disowned_jobs
        assert any(e["event"] == "shutdown-release-noop" for e in release_logs), (
            "the noop release must be visible: the row moved underneath this "
            "worker and the new owner holds it"
        )


async def _consume_with_shutdown_seam(backend: Any, deps: Any, job: Any) -> str:
    from taskq.worker._consumer import consume_one_job

    result: str = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,  # type: ignore[arg-type]
        run_actor=_never_actor,
        actor_config=StubActorConfig(retry=RetryPolicy()),
        payload_type=_Payload,
        clock=FakeClock(_NOW),
    )
    return result


async def _never_actor(job: Any, ctx: Any) -> object:
    raise AssertionError("the body must not start under a shutdown in progress")


# ── the interrupted write's cancellation arm ────────────────────────────


class _CancelDuringInterruptWriteBackend(FakeBackend):
    """mark_interrupted dies to a cancellation mid-write: the cancel
    landing ON the interrupted release write, the arm the exhausted-retry
    arm's comment contrasts with."""

    async def mark_interrupted(self, *args: Any, **kwargs: Any) -> str:  # type: ignore[override]  # Why: as above
        raise asyncio.CancelledError()


async def test_a_cancel_landing_on_the_interrupt_write_disowns_and_propagates() -> None:
    """A cancellation arriving while the interrupted release write is in
    flight disowns the job (the row stays running for lock-lease reclaim)
    and re-raises the cancellation — never a terminal write, the operator
    or shutdown verdict must not be manufactured from a dying attempt."""
    from taskq.worker._consumer import consume_one_job
    from tests.test_shutdown_release_until_exited import _stamp_shutdown_origin

    deps = _shutdown_deps()
    deps.shutdown_started_at = None
    deps.shutdown_phase = ShutdownPhase.NONE
    backend = _CancelDuringInterruptWriteBackend()
    registry = ActiveJobRegistry()
    job = make_job_row(payload={})

    async def body(running: Any, ctx: Any) -> object:
        await asyncio.sleep(3600)
        return {"unreachable": True}

    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]
            run_actor=body,  # type: ignore[arg-type]
            actor_config=StubActorConfig(retry=RetryPolicy()),
            payload_type=_Payload,
            clock=FakeClock(_NOW),
            active_jobs=registry,
        )
    )
    await asyncio.sleep(0.05)
    await _stamp_shutdown_origin(registry, job.id)
    attempt.cancel()

    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=10.0)
    assert job.id in deps.disowned_jobs


# ── the rate-limit release's failure arm ────────────────────────────────


async def test_rate_limit_release_failure_is_a_warning_not_a_crash(
    structlog_capture: list[Any],
) -> None:
    """The release is fenced and best-effort: a failing release write
    warns with the job bound, it never fails the attempt that already
    completed (the slot stays until its lease expires, the sweep reclaims
    it — throttling for no benefit would be worse)."""
    from taskq.ratelimit.registry import RateLimitRegistry

    backend = FakeBackend()
    deps = MagicMock()
    deps.settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})
    deps.worker_pool = None
    deps.redis_client = None
    deps.progress_buffers = {}
    deps.disowned_jobs = set()

    registry = MagicMock(spec=RateLimitRegistry)
    registry.acquire_for_actor = AsyncMock(return_value=[object()])  # type: ignore[assignment]
    registry.release_for_actor = AsyncMock(side_effect=RuntimeError("release write died"))  # type: ignore[assignment]

    job = make_job_row(payload={})
    outcome = await asyncio.wait_for(
        _consume_for_release(backend, deps, registry, job),
        timeout=10,
    )
    assert outcome == "succeeded"
    registry.release_for_actor.assert_awaited_once()
    release_failures = [e for e in structlog_capture if e["event"] == "rate_limit_release_failed"]
    assert release_failures, "the failed release must warn, never fail the completed attempt"
    assert release_failures[0]["error_class"] == "RuntimeError"


async def _consume_for_release(backend: Any, deps: Any, registry: Any, job: Any) -> str:
    from taskq.worker._consumer import consume_one_job

    result: str = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,  # type: ignore[arg-type]
        run_actor=_ok_actor,
        actor_config=StubActorConfig(retry=RetryPolicy()),
        payload_type=_Payload,
        clock=FakeClock(_NOW),
        rate_limit_registry=registry,
        rate_limits=("some_bucket",),
    )
    return result


async def _ok_actor(job: Any, ctx: Any) -> object:
    # Report progress: installs the attempt's buffer in deps.progress_buffers,
    # arming the finally-block's map-hygiene arm (a no-pool dispatch still
    # removes the buffer it installed — the issue-461 map hygiene).
    await ctx.progress(step=1)
    return {"ok": True}


# ── the transactional path's KeyboardInterrupt re-raise ─────────────────


def test_transactional_keyboard_interrupt_arm_is_defensive_unreachable() -> None:
    """``KeyboardInterrupt`` keeps operator-intent semantics everywhere.
    AUDIT CLASSIFICATION (c) — defensive-unreachable through an actor
    body: a KeyboardInterrupt raised inside the attempt task never reaches
    the ``except BaseException`` capture at the shielded await —
    ``Task.__step`` re-raises KI bare into the loop before
    ``set_exception`` can deliver it (the same mechanism the
    SystemExit-carrier conversion documents). The capture's KI arm guards
    a KI the loop delivers into the main coroutine itself; the reachable
    KI contract (propagate raw, the row stays running for lease-reclaim)
    is pinned on the autonomous path by
    tests/test_attempt_baseexception_capture.py.
    """


# ── the transactional success path's state-change publish ───────────────


async def test_transactional_success_publishes_the_state_change_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A redis-wired transactional success publishes the running→succeeded
    event with the buffer's final seq — the SSE stream's terminal marker,
    stamped from the flushed buffer, never a bare default seq."""
    consumer_mod = sys.modules["taskq.worker._consumer"]
    from opentelemetry import trace

    from taskq.testing.actor import EmptyPayload
    from taskq.worker._consumer import _consume_transactional  # pyright: ignore[reportPrivateUsage]
    from tests.test_consumer_coverage import (  # pyright: ignore[reportPrivateUsage]  # Why: the tx doubles are shared harness
        _FakeConnection,
        _TxBackend,
    )

    backend = _TxBackend()
    job = make_job_row()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )

    published: list[dict[str, object]] = []

    async def spy_publish(*args: Any, **kwargs: Any) -> None:
        published.append({"job_id": args[2], "status": kwargs.get("status")})

    # Why monkeypatch, not a raw rebind: a raw module-attribute write would
    # outlive this test and silently replace the publish for every later
    # test in the process (the randomizer reorders, so the victims vary).
    monkeypatch.setattr(consumer_mod, "_publish_state_change_event", spy_publish)

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    ctx = JobContext(
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        claim_epoch=job.claim_epoch,
        worker_id=_WORKER_ID,
        payload=EmptyPayload(),
        jobs=enqueuer,
        log=_LOG,
    )
    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})

    outcome = await _consume_transactional(
        as_backend(backend),
        job,
        _WORKER_ID,
        ctx,
        enqueuer,
        _FakeConnection(),
        actor,
        StubActorConfig(retry=RetryPolicy()),
        None,
        timedelta(hours=24),
        None,
        trace.get_current_span(),
        _LOG,
        progress_buffers={},
        redis_client=AsyncMock(),
        settings=settings,
    )
    assert outcome == "succeeded"
    assert len(published) == 1
    assert published[0]["status"] == "succeeded"
