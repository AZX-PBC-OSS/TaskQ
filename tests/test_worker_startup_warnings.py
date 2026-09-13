"""Unit tests for the sub-enqueue startup signals.

Pure-Python tests — no PG required. The helpers under test are
synchronous (``_emit_sub_enqueue_startup_warnings`` reads the LoopScope
resolved cache and WorkerSettings) or drive a stubbed factory
(``_maybe_open_slot_pool`` opens the per-slot transaction pool only
through the factory it is handed, so activation, the mode announcement,
and the open-failure refusal are all decidable without Postgres).

These tests assert on *which signal path is taken* (autonomous-fallback,
dsn-mismatch, the per-slot mode announcement, or none; dsn-mismatch and
the mode announcement can fire together) via the event name only —
never on log message format or field names, which are implementation
details that change independently of behaviour.
"""

from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any

import asyncpg
from pydantic import BaseModel, TypeAdapter

from taskq._di.scopes import LoopScope
from taskq.actor import ActorRef
from taskq.settings import WorkerSettings
from taskq.testing.spy import WarningSpy
from taskq.worker._bootstrap import _maybe_open_slot_pool
from taskq.worker.run import _emit_sub_enqueue_startup_warnings


def _stub_resolver(func: object) -> object:
    return None


async def _make_loop_scope(
    resolved: dict[type, object] | None = None,
) -> LoopScope:
    scope = LoopScope(resolver=_stub_resolver)
    if resolved is not None:
        scope._cache.update(resolved)  # pyright: ignore[reportPrivateUsage] # Why: test helper populates the cache directly to avoid full DI bootstrap; the helper under test only reads resolved_cache()
    return scope


def _make_settings(
    *,
    pg_dsn_pooled: str | None = None,
    pg_dsn_direct: str | None = None,
    max_concurrency: int | None = None,
) -> WorkerSettings:
    base: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
    }
    if pg_dsn_pooled is not None:
        base["TASKQ_PG_DSN_POOLED"] = pg_dsn_pooled
    if pg_dsn_direct is not None:
        base["TASKQ_PG_DSN_DIRECT"] = pg_dsn_direct
    if max_concurrency is not None:
        base["TASKQ_MAX_CONCURRENCY"] = str(max_concurrency)
    return WorkerSettings.load_from_dict(base)


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(*, name: str = "actor") -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=__import__("taskq.retry", fromlist=["RetryPolicy"]).RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


class _StubConn:
    pass


class _EventSpy:
    """Records each warning()/info() event name — WarningSpy counts calls only."""

    def __init__(self) -> None:
        self.events: list[str] = []

    def warning(self, event: str, *_args: object, **_kwargs: object) -> None:
        self.events.append(event)

    def info(self, event: str, *_args: object, **_kwargs: object) -> None:
        self.events.append(event)


class _FakeSlotPool:
    """Structural asyncpg.Pool stand-in for the bootstrap open path.

    Carries the occupancy-gauge surface (get_size / get_idle_size) and
    a bounded-close-compatible async close(); nothing else is reached
    before the test's exit stack unwinds.
    """

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    def terminate(
        self,
    ) -> None:  # pragma: no cover - close_pool_bounded only terminates on close timeout
        self.closed = True

    def get_size(self) -> int:
        return 0

    def get_idle_size(self) -> int:
        return 0


async def _maybe_open(
    loop_scope: LoopScope,
    settings: WorkerSettings,
    *,
    factory_error: Exception | None = None,
    caller_supplied_pg_pools: bool = False,
) -> tuple[bool, _EventSpy, SimpleNamespace]:
    """Drive _maybe_open_slot_pool with a stubbed factory and deps.

    Returns (opened, spy, deps) so a test can assert on the activation
    result, the emitted events, and what landed on deps. The exit stack
    is unwound here (the fake pool's teardown callback runs), and the
    occupancy-gauge source is cleared so no later gauge collection in
    this process observes this test's fake.
    """
    from taskq.obs import set_slot_pool_occupancy_source

    pool = _FakeSlotPool()

    async def factory() -> Any:
        if factory_error is not None:
            raise factory_error
        return pool

    deps = SimpleNamespace(_exit_stack=AsyncExitStack(), slot_pool=None, slot_pool_factory=None)
    spy = _EventSpy()
    try:
        async with deps._exit_stack:  # pyright: ignore[reportPrivateUsage]  # Why: the duck-typed stub owns this stack exactly as WorkerDeps owns the real one; unwinding it here runs the teardown callback the production flow registers.
            opened = await _maybe_open_slot_pool(
                loop_scope,
                settings,
                deps,  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps stub — the function touches only _exit_stack/slot_pool/slot_pool_factory, mirroring the health-test fakes.
                factory=factory,
                pg_credential_provider=None,
                caller_supplied_pg_pools=caller_supplied_pg_pools,
                log=spy,
            )
    finally:
        set_slot_pool_occupancy_source(None)
    return opened, spy, deps


async def test_no_loop_conn_emits_autonomous_fallback_warning() -> None:
    """startup: no LOOP-scope Connection → one warning emitted."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings()
    actor_registry = {
        "alpha": _make_actor_ref(name="alpha"),
        "beta": _make_actor_ref(name="beta"),
    }
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_loop_conn_with_dsn_mismatch_emits_warning() -> None:
    """LOOP-scope conn present + DSNs differ → one warning emitted."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    # Why: max_concurrency pinned to 1 to isolate the DSN check — the
    # helper's warnings are concurrency-blind, but pinning keeps this
    # case orthogonal to the per-slot activation tests.
    settings = _make_settings(
        pg_dsn_pooled="postgresql://user:pass@pgbouncer:6432/taskq",
        pg_dsn_direct="postgresql://user:pass@pg-primary:5432/taskq",
        max_concurrency=1,
    )
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_loop_conn_with_matching_dsns_emits_no_warning() -> None:
    """No warning when LOOP-scope conn is registered and DSNs are equal."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    # Why: max_concurrency pinned to 1 to isolate the DSN check, keeping
    # this case orthogonal to the per-slot activation tests.
    settings = _make_settings(max_concurrency=1)
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 0


async def test_no_loop_conn_with_mismatched_dsns_emits_only_one_warning() -> None:
    """When no LOOP-scope conn, the DSN-mismatch warning must NOT also fire."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings(
        pg_dsn_pooled="postgresql://user:pass@pgbouncer:6432/taskq",
        pg_dsn_direct="postgresql://user:pass@pg-primary:5432/taskq",
    )
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_empty_actor_registry_still_emits_autonomous_fallback_warning() -> None:
    """With no actors registered, the warning still fires."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings()
    actor_registry: dict[str, ActorRef[_Payload, _Result]] = {}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_loop_conn_max_concurrency_one_does_not_activate_slot_pool() -> None:
    """LOOP-scope conn + max_concurrency=1 → the per-slot path stays off.

    The single-slot worker keeps using the registered connection
    directly — the legitimate fleet shape — so no pool opens and no
    mode announcement fires.
    """
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=1)

    opened, spy, deps = await _maybe_open(loop_scope, settings)

    assert opened is False
    assert deps.slot_pool is None
    assert "transactional_consume_per_slot" not in spy.events


async def test_loop_conn_max_concurrency_above_one_announces_per_slot_mode() -> None:
    """LOOP-scope conn + max_concurrency>1 → pool opens, mode announced.

    The announcement names the mode (transactional consume is per-slot),
    which is what an operator confirms from the logs; the pool lands on
    deps for the dispatch path to acquire from.
    """
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=4)

    opened, spy, deps = await _maybe_open(loop_scope, settings)

    assert opened is True
    assert deps.slot_pool is not None
    assert "transactional_consume_per_slot" in spy.events


async def test_no_loop_conn_max_concurrency_above_one_does_not_activate_slot_pool() -> None:
    """No LOOP-scope conn → autonomous-fallback fires; no pool opens."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings(max_concurrency=4)

    opened, spy, deps = await _maybe_open(loop_scope, settings)

    assert opened is False
    assert deps.slot_pool is None
    assert "transactional_consume_per_slot" not in spy.events

    warn_spy = _EventSpy()
    _emit_sub_enqueue_startup_warnings(
        loop_scope, settings, {"alpha": _make_actor_ref(name="alpha")}, warn_spy
    )
    assert "sub_enqueue_autonomous_fallback" in warn_spy.events


async def test_slot_pool_open_failure_refuses_boot_naming_host() -> None:
    """A worker that cannot open the pool fails to boot, loudly.

    The refusal names the pool and the DSN host — the alternative is a
    worker that accepts jobs it cannot transact.
    """
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=4)

    try:
        await _maybe_open(loop_scope, settings, factory_error=RuntimeError("boom"))
    except RuntimeError as exc:
        assert "slot pool failed to open" in str(exc)
        assert "localhost" in str(exc)
    else:
        raise AssertionError("expected the open failure to refuse boot")


async def test_slot_pool_warns_when_credentials_live_in_caller_pools() -> None:
    """Caller-supplied pools + no provider → the slot-pool credential warning.

    The per-slot pool does not read WorkerConnections; a fleet whose
    credential story lives entirely in its own pool objects must hear
    that at startup, not at first dispatch.
    """
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=4)

    _opened, spy, _deps = await _maybe_open(loop_scope, settings, caller_supplied_pg_pools=True)

    assert "slot_pool_own_credentials" in spy.events


async def test_loop_conn_dsn_mismatch_and_max_concurrency_above_one_emits_both() -> None:
    """The dsn-mismatch warning and the mode announcement are not exclusive."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(
        pg_dsn_pooled="postgresql://user:pass@pgbouncer:6432/taskq",
        pg_dsn_direct="postgresql://user:pass@pg-primary:5432/taskq",
        max_concurrency=4,
    )

    _opened, spy, _deps = await _maybe_open(loop_scope, settings)
    warn_spy = _EventSpy()
    _emit_sub_enqueue_startup_warnings(
        loop_scope, settings, {"alpha": _make_actor_ref(name="alpha")}, warn_spy
    )

    assert "loop_scope_conn_dsn_mismatch" in warn_spy.events
    assert "transactional_consume_per_slot" in spy.events
