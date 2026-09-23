"""BaseException boundary pins: user code's ``SystemExit`` is an outcome at
every seam, never worker death; ``KeyboardInterrupt`` propagates raw.

#459 fixed the actor seams (the sync actor's executor-thread task, the tx
path's ``_run_actor_in_tx`` task): CPython's ``Task.__step`` re-raises
exactly ``(KeyboardInterrupt, SystemExit)`` bare after ``set_exception``,
so a task ending with SystemExit kills the loop before any boundary's
``except`` can run. The same mechanism survives at every OTHER seam user
code crosses, and each has its own documented outcome contract:

  - a cron payload factory (sync: the executor-pool thread; async: the
    coroutine awaited in the tick's frame) is a TICK FAILURE: the strike,
    the failure UPDATE and the telemetry record it, the tick survives;
  - a notify connection factory (the user credential source) is a
    RECONNECT FAILURE: the retry loop survives any factory failure, by
    contract;
  - a retry_classifier hook is a HOOK FAILURE: logged, ignored, the
    classification proceeds with the policy's own decision;
  - an actor lifecycle hook (on_success/on_cancel/on_retry_exhausted,
    sync or async) is a HOOK FAILURE: logged, never propagated, the
    already decided terminal outcome stands;
  - a DI provider factory (sync callable on the loop, sync generator's
    ``__enter__`` on the pinned executor thread) is a JOB-LEVEL TERMINAL
    FAILURE recorded through the same dispatch handler as any pre-actor
    failure, the worker survives.

Every conversion raises a typed carrier (an ordinary ``Exception`` whose
``.original`` is the user code's own ``SystemExit``) and the choke point
that records the outcome unwraps it, so the carrier's own name never
reaches a row, a log or a span. ``KeyboardInterrupt`` is deliberately not
converted anywhere: interpreter/operator intent, never an outcome, it
propagates raw (pinned per boundary).
"""

import asyncio
from typing import Annotated
from unittest.mock import AsyncMock, Mock

import pytest
import structlog.testing
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import (
    ScopeContainer,
    _ProviderSystemExitError,
    _unwrap_provider_system_exit,
    make_resolver,
)
from taskq._ids import new_uuid
from taskq._scope import LifecycleDetectionWarning
from taskq.actor import ActorRef
from taskq.cron import resolve_payload
from taskq.retry import (
    JobRetryState,
    Retry,
    RetryPolicy,
    decide_after_failure,
    invoke_on_success,
)
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import dispatch_one_job
from taskq.worker.notify import _recover_notify_conn
from tests._di_scopes import bootstrap_scopes, make_scopes
from tests.test_cron_loop import (
    _NOW,
    _cron_settings,
    _failure_updates,
    _FakeCronConn,
    _make_actor_config_row,
    _make_schedule_row,
    _tick,
)

_WORKER_ID = new_uuid()


# ── Factory bodies the cron dotted-path resolver imports ──────────────


def _sync_exit_factory() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]  # Why: resolved at runtime via its dotted path (payload_factory), never imported; pyright cannot see the string reference.
    """A sync payload factory whose own bug is ``sys.exit()``."""
    raise SystemExit("boom")


async def _async_exit_factory() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]  # Why: resolved at runtime via its dotted path (payload_factory), never imported; pyright cannot see the string reference.
    raise SystemExit("boom")


def _sync_keyboardinterrupt_factory() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]  # Why: resolved at runtime via its dotted path (payload_factory), never imported; pyright cannot see the string reference.
    raise KeyboardInterrupt


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


class _TransDep:
    pass


def _exit_sync_callable() -> _TransDep:
    raise SystemExit("boom")


def _exit_sync_generator() -> object:
    raise SystemExit("boom")
    yield _TransDep()  # pragma: no cover  # Why: the generator's body raises on __enter__, before the yield.


def _ki_sync_callable() -> _TransDep:
    raise KeyboardInterrupt


# ── Cron: the payload factory seam ────────────────────────────────────


async def test_cron_sync_factory_systemexit_is_a_tick_failure() -> None:
    """A sync payload factory's ``sys.exit()`` (raised on the executor-pool
    thread) is recorded as the schedule's own tick failure: the tick task
    completes, the failure UPDATE carries the factory's own exception text,
    and the worker survives. Pre-fix, the SystemExit crossed the executor
    future into the tick task's frame, escaped the per-schedule ``except
    Exception`` and the tick task ended with the bare re-raise that kills
    the loop: no strike, no telemetry, every sibling cancelled."""
    row = _make_schedule_row(
        actor="exit_actor",
        payload_factory="tests.test_attack_baseexception_boundaries._sync_exit_factory",
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="exit_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    # The tick runs as a sibling task in production (the leader's
    # TaskGroup); the pin drives it the same way so the delivery shape -
    # a task whose step must not end with SystemExit - is the pinned one.
    tick_task = asyncio.create_task(_tick(conn, _cron_settings(), backend))
    fired = await asyncio.wait_for(tick_task, timeout=10.0)

    assert fired == 0, "a factory that raised recorded no fire"
    failures = _failure_updates(conn)
    assert len(failures) == 1, (
        "the schedule takes its strike: the failure UPDATE ran for exactly "
        "this schedule, the consecutive_failures bookkeeping the "
        "auto-disable threshold reads"
    )
    assert failures[0][1][0] == [row["id"]]
    assert failures[0][1][1] == ["boom"], (
        "the recorded error text is the factory's own SystemExit str, never "
        "the carrier's message (the unwrap happened at the choke point)"
    )


async def test_cron_async_factory_systemexit_is_a_tick_failure() -> None:
    """An async payload factory's ``sys.exit()`` is raised in the tick
    task's own frame (the coroutine is awaited in-frame); the same
    conversion applies and the tick survives with the strike recorded."""
    row = _make_schedule_row(
        actor="exit_actor",
        payload_factory="tests.test_attack_baseexception_boundaries._async_exit_factory",
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="exit_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    tick_task = asyncio.create_task(_tick(conn, _cron_settings(), backend))
    fired = await asyncio.wait_for(tick_task, timeout=10.0)

    assert fired == 0
    failures = _failure_updates(conn)
    assert len(failures) == 1
    assert failures[0][1][1] == ["boom"]


async def test_cron_factory_keyboardinterrupt_propagates_raw() -> None:
    """The carve-out, pinned at the cron seam: a factory's
    ``KeyboardInterrupt`` is interpreter/operator intent, never converted
    to an outcome - it propagates raw out of ``resolve_payload``."""
    with pytest.raises(KeyboardInterrupt):
        await resolve_payload(
            "tests.test_attack_baseexception_boundaries._sync_keyboardinterrupt_factory",
            {},
        )


# ── Notify: the connection factory seam ───────────────────────────────


def _notify_deps(backoff_initial: float = 0.001) -> Mock:
    """Mock WorkerDeps like tests/test_notify.py's _make_mock_deps, built
    locally so the pins own their wiring."""
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://localhost:5432/taskq",
            "schema_name": "taskq_test",
            "notify_health_check_interval": "0.001",
            "notify_reconnect_backoff_initial": str(backoff_initial),
        }
    )
    deps = Mock(spec=WorkerDeps)
    deps.settings = settings
    deps.notify_conn = None
    deps.leader_conn_factory = None
    deps.notify_reconnect_lock = asyncio.Lock()
    deps.notify_reconnect_fn = None
    deps.owns_notify_conn = True
    return deps


def _mock_conn() -> Mock:
    conn = Mock()
    conn.execute = AsyncMock()
    conn.add_listener = AsyncMock()
    conn.remove_listener = AsyncMock()
    conn.close = AsyncMock()
    return conn


def _make_channels() -> list[tuple[str, object]]:
    from taskq.constants import events_channel, wake_channel, worker_channel

    def _cb(conn: object, *rest: object) -> None:
        del conn, rest  # Why: asyncpg's callback signature, never invoked in these pins.

    return [
        (wake_channel("taskq_test"), _cb),
        (events_channel("taskq_test"), _cb),
        (worker_channel("taskq_test", _WORKER_ID), _cb),
    ]


async def test_notify_factory_systemexit_is_a_reconnect_failure() -> None:
    """The notify connection factory's ``sys.exit()`` is an ordinary
    reconnect failure: the retry loop logs the attempt (naming the
    factory's own exception, never a carrier's name), backs off, retries,
    and the second attempt's success lands the connection. Pre-fix, the
    SystemExit crossed the bare await into the reconnect frame, escaped
    the loop's ``except Exception`` and the health-check sibling died with
    the bare re-raise that kills the loop."""
    deps = _notify_deps()
    channels = _make_channels()
    shutdown = asyncio.Event()
    dead_conn = _mock_conn()
    deps.notify_conn = dead_conn

    good_conn = _mock_conn()
    calls: list[int] = []

    async def flaky_factory() -> Mock:
        calls.append(1)
        if len(calls) == 1:
            raise SystemExit("boom")
        return good_conn

    deps.notify_conn_factory = flaky_factory

    with structlog.testing.capture_logs() as captured:
        recovered = await _recover_notify_conn(
            deps,
            _notify_backend(),
            shutdown,
            channels,
            dead_conn,
            RuntimeError("conn judged dead"),
        )

    assert recovered is good_conn
    assert deps.notify_conn is good_conn
    assert len(calls) == 2, "the SystemExit attempt is retried, never fatal"
    attempts = [e for e in captured if e["event"] == "notify-reconnect-attempt"]
    assert len(attempts) == 1
    assert attempts[0]["error_type"] == "SystemExit", (
        "the reconnect log names the factory's own exception, never the "
        "carrier's name (the unwrap happened at the choke point)"
    )


def _notify_backend() -> Mock:
    """The backend argument is unused on this reconnect path; the pin passes
    a stub to keep the call shape honest."""
    return Mock()


async def test_notify_factory_keyboardinterrupt_propagates_raw() -> None:
    """The carve-out at the notify seam: a factory's KeyboardInterrupt is
    never converted to a reconnect failure, it propagates raw."""
    deps = _notify_deps()
    channels = _make_channels()
    shutdown = asyncio.Event()
    dead_conn = _mock_conn()
    deps.notify_conn = dead_conn

    async def ki_factory() -> Mock:
        raise KeyboardInterrupt

    deps.notify_conn_factory = ki_factory

    with pytest.raises(KeyboardInterrupt):
        await _recover_notify_conn(
            deps,
            _notify_backend(),
            shutdown,
            channels,
            dead_conn,
            RuntimeError("conn judged dead"),
        )


# ── Retry: the classifier hook seam ───────────────────────────────────


def test_classifier_hook_systemexit_is_a_hook_failure() -> None:
    """A ``retry_classifier`` hook raising ``SystemExit`` is logged and
    ignored: classification proceeds with the policy's own decision. Pre-
    fix, the SystemExit escaped the hook boundary mid-dispatch: the
    in-flight attempt outcome was dropped, the row stranded ``running``,
    and the dispatch task ended with the bare re-raise that kills the
    loop."""
    calls: list[int] = []

    def exit_classifier(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        calls.append(1)
        raise SystemExit("boom")

    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        retry_classifier=exit_classifier,
    )
    job_state = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )

    with structlog.testing.capture_logs() as captured:
        decision = decide_after_failure(actor_config, RuntimeError("x"), job_state)

    assert calls == [1], "the hook ran"
    assert isinstance(decision, Retry), (
        "the policy's own transient decision stands; the hook failure never "
        "changes the outcome the attempt records"
    )
    hook_failed = [e for e in captured if e["event"] == "retry-classifier-hook-failed"]
    assert len(hook_failed) == 1
    assert "SystemExit" in hook_failed[0]["error"], (
        "the log names the hook's own exception (repr), never a carrier's name"
    )


def test_classifier_hook_keyboardinterrupt_propagates_raw() -> None:
    """The carve-out at the classifier seam: KeyboardInterrupt propagates
    raw, never swallowed as a hook failure."""

    def ki_classifier(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        raise KeyboardInterrupt

    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        retry_classifier=ki_classifier,
    )
    job_state = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )

    with pytest.raises(KeyboardInterrupt):
        decide_after_failure(actor_config, RuntimeError("x"), job_state)


# ── Retry: the lifecycle hook seam (sync and async arms) ──────────────


async def test_sync_lifecycle_hook_systemexit_is_a_hook_failure() -> None:
    """A sync ``on_success`` hook raising ``SystemExit`` is logged and
    dropped: the terminal write's outcome stands. Pre-fix, the escapee
    tore the terminal path down after the success write had landed - the
    dispatch misrouted it into the failure handler (a succeeded job
    recorded failed, a re-execution risk) or the dispatch task died with
    the bare re-raise that kills the loop."""

    def exit_hook(job_row: object, result: object) -> None:
        del job_row, result
        raise SystemExit("boom")

    job = make_job_row(payload={"value": 1})

    with structlog.testing.capture_logs() as captured:
        await invoke_on_success(exit_hook, job, {"value": 1}, 5.0)  # type: ignore[arg-type]

    hook_failed = [e for e in captured if e["event"] == "on-success-hook-failed"]
    assert len(hook_failed) == 1
    assert "SystemExit" in hook_failed[0]["error"], (
        "the log names the hook's own exception (repr), never a carrier's name"
    )


async def test_async_lifecycle_hook_systemexit_is_a_hook_failure() -> None:
    """An async ``on_success`` hook raising ``SystemExit``: the await
    delivers the exception into this frame, the boundary logs and drops
    it, the outcome stands."""

    async def exit_hook(job_row: object, result: object) -> None:
        del job_row, result
        raise SystemExit("boom")

    job = make_job_row(payload={"value": 1})

    with structlog.testing.capture_logs() as captured:
        await invoke_on_success(exit_hook, job, {"value": 1}, 5.0)  # type: ignore[arg-type]

    hook_failed = [e for e in captured if e["event"] == "on-success-hook-failed"]
    assert len(hook_failed) == 1
    assert "SystemExit" in hook_failed[0]["error"]


async def test_lifecycle_hook_keyboardinterrupt_propagates_raw() -> None:
    """The carve-out at the lifecycle-hook seam: KeyboardInterrupt
    propagates raw, never swallowed as a hook failure."""

    def ki_hook(job_row: object, result: object) -> None:
        del job_row, result
        raise KeyboardInterrupt

    job = make_job_row(payload={"value": 1})

    with pytest.raises(KeyboardInterrupt):
        await invoke_on_success(ki_hook, job, {"value": 1}, 5.0)  # type: ignore[arg-type]


# ── DI: the provider factory seam ─────────────────────────────────────


async def test_provider_sync_callable_systemexit_is_carried() -> None:
    """Unit shape of the conversion: a SYNC_CALLABLE provider factory's
    ``sys.exit()`` leaves ``get_or_create`` as the typed carrier (an
    ordinary Exception), and the unwrap helper returns the factory's own
    exception for the choke point."""
    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, _exit_sync_callable)
    container = ScopeContainer(
        scope=Scope.TRANSIENT,
        # An empty-registry resolver: the pin's factories declare no
        # parameters, so the resolution step answers an empty kwargs dict
        # and the factory itself is what raises.
        resolver=make_resolver(ProviderRegistry(), {}),
    )

    with pytest.raises(_ProviderSystemExitError) as exc_info:
        await container.get_or_create(_TransDep, registry.get(_TransDep))

    assert isinstance(exc_info.value.original, SystemExit)
    assert exc_info.value.original.code == "boom"
    assert _unwrap_provider_system_exit(exc_info.value) is exc_info.value.original
    non_carrier = RuntimeError("x")
    assert _unwrap_provider_system_exit(non_carrier) is non_carrier


async def test_provider_sync_generator_systemexit_from_the_thread_is_carried() -> None:
    """A SYNC_GENERATOR provider's ``__enter__`` runs on the pinned
    executor thread; its ``sys.exit()`` crosses the bare executor future
    (no task boundary stops it) and must leave ``get_or_create`` as the
    same typed carrier."""
    with pytest.warns(LifecycleDetectionWarning):
        # The registration warns that a sync generator factory's cleanup
        # runs synchronously; irrelevant here, the factory never returns.
        registry = ProviderRegistry()
        registry.register_factory(_TransDep, Scope.TRANSIENT, _exit_sync_generator)
    container = ScopeContainer(
        scope=Scope.TRANSIENT,
        resolver=make_resolver(ProviderRegistry(), {}),
    )

    with pytest.raises(_ProviderSystemExitError) as exc_info:
        await container.get_or_create(_TransDep, registry.get(_TransDep))

    assert isinstance(exc_info.value.original, SystemExit)


async def test_provider_keyboardinterrupt_propagates_raw() -> None:
    """The carve-out at the DI seam: KeyboardInterrupt propagates raw."""
    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, _ki_sync_callable)
    container = ScopeContainer(
        scope=Scope.TRANSIENT,
        resolver=make_resolver(ProviderRegistry(), {}),
    )

    with pytest.raises(KeyboardInterrupt):
        await container.get_or_create(_TransDep, registry.get(_TransDep))


# ── DI: the dispatch-level pin (worker survives, outcome recorded) ────


class _ScopeStack:
    """The scope stack shape tests/test_dispatch_one_job.py drives
    dispatch_one_job with, rebuilt locally so the pin owns its wiring."""

    def __init__(self, registry: ProviderRegistry | None = None) -> None:
        self.registry = registry or ProviderRegistry()

    async def __aenter__(self) -> "_ScopeStack":
        self.registry.validate()
        self.process_scope, self.thread_scope, self.loop_scope = make_scopes(self.registry)
        await bootstrap_scopes(
            self.registry, self.process_scope, self.thread_scope, self.loop_scope
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()


class _FakeWorkerDeps:
    """The minimal WorkerDeps stub shape test_dispatch_one_job uses."""

    def __init__(self) -> None:
        self.active_jobs = ActiveJobRegistry()
        self.worker_pool = None
        self.slot_pool = None
        self.slot_pool_connection_init = None
        self.settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
        )
        self.settings.worker_group = "default"
        self.redis_client = None
        self.progress_buffers: dict[object, object] = {}
        self.disowned_jobs: set[object] = set()


def _make_actor_ref(fn: object) -> ActorRef[_Payload, None]:
    return ActorRef(
        name="test_actor",
        queue="default",
        fn=fn,  # type: ignore[arg-type]  # Why: the pin's actor is Callable[..., Awaitable[dict]]; ActorRef erases to object the same way test_dispatch_one_job's helper documents.
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,  # type: ignore[arg-type]
        retry=RetryPolicy(),
        result_ttl=None,
    )


async def test_provider_systemexit_is_a_job_level_terminal_failure() -> None:
    """The dispatch-level pin: an actor whose TRANSIENT dependency's
    factory raises ``sys.exit()`` is recorded through the SAME failure
    handler as any pre-actor failure (error_class is the factory's own
    SystemExit), the dispatch completes, and the worker survives. Pre-fix,
    the SystemExit escaped ``dispatch_one_job``'s ``except Exception`` and
    the per-job dispatch task ended with the bare re-raise that kills the
    loop: the row stranded ``running`` for lease expiry."""
    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, _exit_sync_callable)

    async def my_actor(
        payload: _Payload,
        ctx: object,
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        del payload, ctx, dep  # pragma: no cover  # Why: the factory fails before the actor runs.
        return {}

    actor_ref = _make_actor_ref(my_actor)
    job = make_job_row(payload={"value": 42})
    fake_backend = FakeBackend()
    fake_deps = _FakeWorkerDeps()

    async with _ScopeStack(registry) as scopes:
        outcome = await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=fake_deps,  # type: ignore[arg-type]  # Why: the stub deps shape test_dispatch_one_job's own pins pass.
            job=job,
            worker_id=_WORKER_ID,
            registry=registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] vs ActorRef[BaseModel, BaseModel | None], the same pyright erasure test_dispatch_one_job documents.
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=None,
        )

    assert outcome == "scheduled", (
        "the transient retry policy re-schedules the job: the failure is "
        "recorded, the worker survived, the row never strands running"
    )
    assert len(fake_backend.mark_failed_or_retry_calls) == 1, (
        "the job's failure is recorded, the row never strands running"
    )
    error_info = fake_backend.mark_failed_or_retry_calls[0]["error_info"]
    assert error_info is not None  # pyright: ignore[reportAttributeAccessIssue]  # Why: mark_failed_or_retry_calls stores untyped objects from the fake; error_info is an ErrorInfo at runtime.
    assert error_info.error_class == "SystemExit", (  # pyright: ignore[reportAttributeAccessIssue]
        "the recorded class is the factory's own SystemExit, never the "
        "carrier's name (the unwrap happened at the choke point)"
    )
