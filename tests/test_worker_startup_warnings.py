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

The exception is the ``note=`` remediation text of the two signals that
tell an operator what each connection configuration means
(#116): the retired shared-connection guard's note told operators to
"register a pool instead of a single connection", but a LOOP-scope
``asyncpg.Pool`` registration silently disables transactional consume —
the remediation was the defect. Those two notes' load-bearing claims are
pinned directly (see the ``_NoteSpy`` tests at the bottom).
"""

import asyncio
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
    reload_factory_timeout: float | None = None,
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
    if reload_factory_timeout is not None:
        base["TASKQ_RELOAD_FACTORY_TIMEOUT"] = str(reload_factory_timeout)
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


class _NoteSpy(_EventSpy):
    """Also records each event's ``note=`` remediation text.

    The remediation text is itself contract under issue #116: the retired
    guard's note told operators to "register a pool instead of a single
    connection", but a LOOP-scope ``asyncpg.Pool`` registration silently
    DISABLES transactional consume (the transactional path keys on
    ``asyncpg.Connection``). These pins therefore assert on the note's
    load-bearing claims, not just the event name.
    """

    def __init__(self) -> None:
        super().__init__()
        self.notes: dict[str, str | None] = {}

    def warning(self, event: str, *_args: object, **kwargs: object) -> None:
        super().warning(event)
        note = kwargs.get("note")
        self.notes[event] = note if isinstance(note, str) else None

    def info(self, event: str, *_args: object, **kwargs: object) -> None:
        super().info(event)
        note = kwargs.get("note")
        self.notes[event] = note if isinstance(note, str) else None


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
    factory_hangs: bool = False,
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
        if factory_hangs:
            # A pool-factory call that never returns — a wedged credential
            # fetch is the production shape this stands in for.
            await asyncio.Event().wait()
        if factory_error is not None:
            raise factory_error
        return pool

    deps = SimpleNamespace(_exit_stack=AsyncExitStack(), slot_pool=None, slot_pool_factory=None)
    spy = _NoteSpy()
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


async def test_slot_pool_open_is_bounded_by_reload_factory_timeout() -> None:
    """A factory that never returns cannot wedge boot.

    Opening the fully-warmed pool means one connection (and, on a
    managed-identity deployment, one credential fetch) per consumer
    slot; an unbounded wait here would hang worker startup forever —
    the worst failure shape this project ships. The open is bounded by
    ``reload_factory_timeout`` and the expiry refuses boot, loudly,
    naming the host. The outer wait_for keeps a regression (the bound
    removed) a fast test failure rather than a hung suite.
    """
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=4, reload_factory_timeout=0.05)

    try:
        await asyncio.wait_for(_maybe_open(loop_scope, settings, factory_hangs=True), timeout=10)
    except RuntimeError as exc:
        assert "slot pool failed to open" in str(exc)
        assert "localhost" in str(exc)
    else:
        raise AssertionError("a hanging pool factory must refuse boot, not open")


async def test_slot_pool_open_outside_deps_lifecycle_fails_loudly() -> None:
    """Opening the pool without the deps exit stack is a wiring bug, and
    it must stop the caller immediately — a pool with no registered
    teardown would leak max_concurrency + 1 connections silently."""
    from taskq.obs import set_slot_pool_occupancy_source

    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=4)

    async def factory() -> Any:
        raise AssertionError("the factory must not be called when the lifecycle is missing")

    deps = SimpleNamespace(_exit_stack=None, slot_pool=None, slot_pool_factory=None)
    try:
        await _maybe_open_slot_pool(
            loop_scope,
            settings,
            deps,  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps stub, mirroring _maybe_open above.
            factory=factory,
            pg_credential_provider=None,
            caller_supplied_pg_pools=False,
            log=_EventSpy(),
        )
    except RuntimeError as exc:
        assert "outside of open_worker_deps" in str(exc)
    else:
        raise AssertionError("opening outside the deps lifecycle must refuse, not proceed")
    finally:
        set_slot_pool_occupancy_source(None)


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


async def test_autonomous_fallback_note_states_pool_registration_disables_transactional_consume() -> None:
    """The autonomous-fallback warning's remediation must tell the truth
    about a LOOP-scope pool registration (issue #116).

    The transactional path keys on ``asyncpg.Connection``; an operator who
    registers an ``asyncpg.Pool`` at ``Scope.LOOP`` instead still sees this
    warning — the pool does not activate transactional consume, it keeps
    autonomous commit in force. The retired #116 guard's note said
    "register a pool instead of a single connection", sending exactly that
    operator down the silent-disable path; the note must now name the
    consequence of each shape.
    """
    loop_scope = await _make_loop_scope()
    settings = _make_settings()
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = _NoteSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    note = spy.notes.get("sub_enqueue_autonomous_fallback")
    assert note is not None, "the autonomous-fallback warning must fire"
    assert "asyncpg.Pool" in note, (
        "the remediation must name the pool shape operators reach for — a "
        "LOOP-scope asyncpg.Pool registration"
    )
    assert "does NOT activate" in note, (
        "the remediation must state that registering a pool does not activate "
        "transactional consume"
    )
    assert "silently" in note, (
        "the remediation must state that the pool shape's consequence is a "
        "silent disable, not a loud failure"
    )


async def test_per_slot_note_states_actors_transact_on_their_own_slot_connection() -> None:
    """The per-slot mode announcement must state the actor-visible semantics
    (issue #116).

    The per-slot pool carries more than TaskQ's own transactional writes:
    every job's actor resolves its slot connection — the connection that
    job's transaction runs on — so concurrent slots can never interleave on
    one connection, and an actor's own writes join its job's transaction.
    The announcement is the boot-time record of that contract, so its note
    must carry the load-bearing claims (per-slot actor connection, no
    interleaving, and which shape still inherits the registered
    connection's session state).
    """
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=4)

    _opened, spy, _deps = await _maybe_open(loop_scope, settings)

    note = spy.notes.get("transactional_consume_per_slot")
    assert note is not None, "the per-slot mode announcement must fire"
    assert "slot connection" in note, (
        "the announcement must state that actors receive their slot's "
        "connection — the connection their job's transaction runs on"
    )
    assert "never interleave" in note, (
        "the announcement must state the isolation guarantee: concurrent "
        "slots can never interleave on one connection"
    )
    assert "session state" in note, (
        "the announcement must state which shape still inherits the "
        "registered connection's session state (the max_concurrency=1 worker)"
    )
