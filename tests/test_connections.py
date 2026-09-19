"""Unit tests for connection hook points (taskq.connections, worker deps).

These tests do not require a running Postgres/Redis - they use fakes and
mocks to verify the ownership, teardown, and fallback semantics of the
WorkerConnections hook points. Integration tests against real PG live in
test_worker_deps.py (marked ``integration``).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import MagicMock

import asyncpg
import pytest
import structlog.testing
from asyncpg.exceptions import InternalClientError

from taskq.connections import (
    _POOL_RELEASE_RESET_TIMEOUT_SECS,  # pyright: ignore[reportPrivateUsage]  # Why: the checkout's release bound is the unit under test; the constant is the seam the assertion reads.
    DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    DEFAULT_STATEMENT_CACHE_SIZE,
    WorkerConnections,
    _RetryGuard,  # pyright: ignore[reportPrivateUsage]  # Why: the guard handed to the op is the unit under test: pinning it directly keeps this contract off the intermittent full-stack race test.
    _with_fresh_connection_retry,  # pyright: ignore[reportPrivateUsage]  # Why: the shared dead-on-acquire guard is the unit under test - pinning it directly keeps this contract off the intermittent full-stack race test.
    bounded_lock_budget_ms,
    connection_init_hook,
    lock_budget_command_timeout_secs,
    statement_cache_kwargs,
    with_connection_init,
)
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.testing.settings import make_integration_settings
from taskq.worker._bootstrap import (
    _slot_pool_factory,  # pyright: ignore[reportPrivateUsage]  # Why: the DSN-built slot-pool factory is the directly-callable representative of TaskQ's pool-construction sites.
)

# ── WorkerConnections validation ───────────────────────────────────────


async def _fake_pool_factory() -> asyncpg.Pool:
    return MagicMock(spec=asyncpg.Pool)  # type: ignore[return-value]


async def _fake_conn_factory() -> asyncpg.Connection:
    return MagicMock(spec=asyncpg.Connection)  # type: ignore[return-value]


async def _fake_redis_factory() -> Any:
    return MagicMock()


def test_worker_connections_empty_has_any_false() -> None:
    """An empty WorkerConnections reports has_any() == False."""
    assert not WorkerConnections().has_any()


def test_worker_connections_has_any_true_with_concrete() -> None:
    """A concrete pool sets has_any() == True."""
    pool = MagicMock(spec=asyncpg.Pool)
    assert WorkerConnections(worker_pool=pool).has_any()


def test_worker_connections_has_any_true_with_factory() -> None:
    """A factory sets has_any() == True."""
    assert WorkerConnections(worker_pool_factory=_fake_pool_factory).has_any()


def test_worker_connections_rejects_concrete_and_factory_same_role() -> None:
    """Providing both concrete and factory for the same role is a config error."""
    pool = MagicMock(spec=asyncpg.Pool)
    with pytest.raises(ValueError, match="worker_pool"):
        WorkerConnections(worker_pool=pool, worker_pool_factory=_fake_pool_factory)


def test_worker_connections_allows_concrete_one_role_factory_another() -> None:
    """Concrete for one role and factory for a different role is fine."""
    pool = MagicMock(spec=asyncpg.Pool)
    conns = WorkerConnections(
        worker_pool=pool,
        heartbeat_pool_factory=_fake_pool_factory,
    )
    assert conns.has_any()


def test_worker_connections_rejects_all_role_conflicts() -> None:
    """Every role pair is validated, not just the first."""
    pool = MagicMock(spec=asyncpg.Pool)
    conn = MagicMock(spec=asyncpg.Connection)
    for concrete, factory in [
        ("dispatcher_pool", "dispatcher_pool_factory"),
        ("heartbeat_pool", "heartbeat_pool_factory"),
        ("worker_pool", "worker_pool_factory"),
        ("notify_conn", "notify_conn_factory"),
        ("leader_conn", "leader_conn_factory"),
        ("redis_client", "redis_client_factory"),
    ]:
        with pytest.raises(ValueError, match=concrete):
            WorkerConnections(
                **{concrete: pool if "pool" in concrete or "redis" in concrete else conn},  # type: ignore[arg-type]
                **{
                    factory: _fake_pool_factory
                    if "pool" in factory
                    else _fake_redis_factory
                    if "redis" in factory
                    else _fake_conn_factory
                },  # type: ignore[arg-type]
            )


# ── asyncpg statement-cache defaults ───────────────────────────────────


def test_statement_cache_defaults_hold_their_documented_values() -> None:
    """TaskQ's statement-cache defaults: 512 entries, 1 h lifetime.

    512 covers the rendered list_jobs filter-combination space (384+
    distinct texts) that thrashed asyncpg's 100-entry default (measured
    90-96% steady-state miss rate, benchmarks/ab_stmt_cache.py); 3600 s
    stops needless re-prepares on long-running workers.
    """
    assert DEFAULT_STATEMENT_CACHE_SIZE == 512
    assert DEFAULT_MAX_CACHED_STATEMENT_LIFETIME == 3600


async def test_dsn_built_pool_passes_statement_cache_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TaskQ-built pools forward the statement-cache kwargs to create_pool.

    Drives the per-slot pool factory - a representative TaskQ
    pool-construction site - against a fake create_pool and asserts both
    kwargs arrive, so a call site cannot silently drop them.
    """
    captured: dict[str, Any] = {}

    async def _fake_create_pool(**kwargs: Any) -> asyncpg.Pool:
        captured.update(kwargs)
        return MagicMock(spec=asyncpg.Pool)  # type: ignore[return-value]

    monkeypatch.setattr(asyncpg, "create_pool", _fake_create_pool)
    settings = make_integration_settings("postgresql://taskq:taskq@localhost:5432/taskq")

    pool = await _slot_pool_factory(settings, None)()

    assert pool is not None
    assert captured["statement_cache_size"] == DEFAULT_STATEMENT_CACHE_SIZE
    assert captured["max_cached_statement_lifetime"] == DEFAULT_MAX_CACHED_STATEMENT_LIFETIME


async def test_dsn_built_pool_resolves_statement_cache_from_settings_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """env override → WorkerSettings.load() → the slot pool's create_pool kwargs.

    The settings-aware half of the wiring: ``_slot_pool_factory`` resolves
    the pair through ``statement_cache_kwargs(settings)``, so an operator's
    ``TASKQ_STATEMENT_CACHE_SIZE`` / ``TASKQ_MAX_CACHED_STATEMENT_LIFETIME``
    reach the real ``create_pool`` call without a code change.
    (read_dotfiles=False keeps the test hermetic against a developer's .env;
    the test connections.py double is declared in
    test_double_signature_drift._NARROWER_BY_DESIGN.)
    """
    monkeypatch.setenv("TASKQ_STATEMENT_CACHE_SIZE", "1024")
    monkeypatch.setenv("TASKQ_MAX_CACHED_STATEMENT_LIFETIME", "7200")
    monkeypatch.setenv("TASKQ_PG_DSN_DIRECT", "postgresql://taskq:taskq@localhost:5432/taskq")
    settings = WorkerSettings.load(read_dotfiles=False)

    captured: dict[str, Any] = {}

    async def _fake_create_pool(**kwargs: Any) -> asyncpg.Pool:
        captured.update(kwargs)
        return MagicMock(spec=asyncpg.Pool)  # type: ignore[return-value]

    monkeypatch.setattr(asyncpg, "create_pool", _fake_create_pool)

    pool = await _slot_pool_factory(settings, None)()

    assert pool is not None
    assert captured["statement_cache_size"] == 1024
    assert captured["max_cached_statement_lifetime"] == 7200


async def test_pooled_knob_builds_pools_with_statement_cache_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TASKQ_PG_IS_POOLED=true → the TaskQ-built pool's create_pool kwargs are 0/0.

    Layer 2 of the pooler hygiene: a transaction-mode pooler remaps server
    connections between statements, so a cached prepared statement's Parse
    and Bind can land on different server connections (SQLSTATE 26000).
    The only safe cache size is 0, and the resolver override is what a
    TaskQ-built pool actually forwards - pinned at a representative
    construction site so a call site cannot bypass the override by
    forwarding its own stale pair.
    """
    monkeypatch.setenv("TASKQ_PG_IS_POOLED", "true")
    monkeypatch.setenv("TASKQ_STATEMENT_CACHE_SIZE", "1024")
    monkeypatch.setenv("TASKQ_MAX_CACHED_STATEMENT_LIFETIME", "7200")
    monkeypatch.setenv("TASKQ_PG_DSN_DIRECT", "postgresql://taskq:taskq@localhost:5432/taskq")
    settings = WorkerSettings.load(read_dotfiles=False)

    captured: dict[str, Any] = {}

    async def _fake_create_pool(**kwargs: Any) -> asyncpg.Pool:
        captured.update(kwargs)
        return MagicMock(spec=asyncpg.Pool)  # type: ignore[return-value]

    monkeypatch.setattr(asyncpg, "create_pool", _fake_create_pool)

    pool = await _slot_pool_factory(settings, None)()

    assert pool is not None
    assert captured["statement_cache_size"] == 0
    assert captured["max_cached_statement_lifetime"] == 0


# ── statement-cache settings wiring ────────────────────────────────────


def test_statement_cache_kwargs_fallback_without_settings() -> None:
    """No settings in scope → the module constants.

    Call sites without a settings instance (the testing fixtures) keep the
    documented defaults; the resolver's fallback must agree with them.
    """
    assert statement_cache_kwargs() == {
        "statement_cache_size": DEFAULT_STATEMENT_CACHE_SIZE,
        "max_cached_statement_lifetime": DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    }


def test_statement_cache_kwargs_unconfigured_settings_yield_constants() -> None:
    """Settings loaded without the env vars set resolve to the same pair -
    the settings defaults are wired to the constants, not re-hardcoded."""
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq"})
    assert statement_cache_kwargs(settings) == statement_cache_kwargs()


async def test_statement_cache_kwargs_env_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """env var → TaskQSettings field → pool kwarg dict.

    The settings-aware resolution path: an operator setting the env vars
    changes what a settings-consuming pool builder passes to create_pool,
    without any code change. (TaskQSettings.load - not load_from_dict -
    so the process environment is actually read; read_dotfiles=False
    keeps the test hermetic against a developer's .env.)
    """
    monkeypatch.setenv("TASKQ_STATEMENT_CACHE_SIZE", "1024")
    monkeypatch.setenv("TASKQ_MAX_CACHED_STATEMENT_LIFETIME", "7200")
    settings = TaskQSettings.load(read_dotfiles=False)

    # statement_cache_kwargs is the splat a settings-aware builder
    # forwards: asyncpg.create_pool(dsn, **statement_cache_kwargs(settings))
    assert statement_cache_kwargs(settings) == {
        "statement_cache_size": 1024,
        "max_cached_statement_lifetime": 7200,
    }


# ── Enqueue lock budgets vs the pool's per-query bound ────────────────


def test_bounded_lock_budget_stands_when_the_pool_has_no_bound() -> None:
    """A caller-owned pool carries no client-side bound TaskQ can know -
    the budget must stand as configured, never clamped against a guess."""
    assert bounded_lock_budget_ms(5000.0, None) == 5000.0


def test_bounded_lock_budget_stands_for_an_unbounded_budget() -> None:
    """A non-positive budget is the operator asking for an unbounded
    server-side wait (the lock_timeout GUC convention) - the clamp never
    invents a bound for it."""
    assert bounded_lock_budget_ms(0.0, 5.0) == 0.0
    assert bounded_lock_budget_ms(-1.0, 5.0) == -1.0


def test_bounded_lock_budget_clamps_to_the_share_of_the_bound() -> None:
    """The delivered budget fits inside the connection's per-query bound
    with the share of headroom the refusal needs to unwind first - so the
    server-side lock_timeout (typed error) fires before the client-side
    timer (bare TimeoutError)."""
    assert bounded_lock_budget_ms(5000.0, 5.0) == 4000.0
    # Under the bound already: unchanged.
    assert bounded_lock_budget_ms(1000.0, 5.0) == 1000.0


def test_lock_budget_command_timeout_floor_at_the_shipped_defaults() -> None:
    """At the shipped defaults the pool bound IS the floor - a deployment
    that sets nothing keeps the pre-knob 5 s bound exactly."""
    assert lock_budget_command_timeout_secs([(5000.0, 5000.0)] * 3, floor_secs=5.0) == 5.0


def test_lock_budget_command_timeout_follows_a_widened_budget() -> None:
    """A budget widened past its shipped default re-derives the bound so
    the budget occupies the same share of it - the widening is delivered
    end to end instead of being silently clamped back to the floor."""
    bound = lock_budget_command_timeout_secs(
        [(5000.0, 5000.0), (30000.0, 5000.0), (0.0, 5000.0)], floor_secs=5.0
    )
    assert bound == 30000.0 / 1000.0 / 0.8 == 37.5
    # ... and the clamp then delivers the widened budget in full.
    assert bounded_lock_budget_ms(30000.0, bound) == 30000.0
    # A sibling left at its default now fits the larger bound unclamped.
    assert bounded_lock_budget_ms(5000.0, bound) == 5000.0


def test_lock_budget_command_timeout_ignores_narrowed_and_unbounded_budgets() -> None:
    """Narrowing a budget, or setting 0 (unbounded server-side wait),
    never moves the pool bound: the floor already delivers the narrowed
    value's clamped share, and an unbounded wait cannot fit inside any
    finite bound."""
    assert (
        lock_budget_command_timeout_secs([(1000.0, 5000.0), (0.0, 5000.0)], floor_secs=5.0) == 5.0
    )


# ── Inheritable per-connection init hooks (with_connection_init) ──────
#
# The wrapper is the declaring channel for a hand-rolled connection
# factory: the hook runs on every produced connection AND is exposed for
# the worker's per-slot transaction pool to inherit (the
# ``test_slot_pool.py`` integration pair pins the end-to-end contract).
# These unit pins hold the wrapper's own mechanics.


async def test_with_connection_init_applies_the_hook_to_every_produced_connection() -> None:
    """The hook runs exactly once per produced connection, on the
    connection itself - the ``setup=`` hook position, so the LOOP-scope
    connection and the slot connections carry identical setup."""
    applied: list[Any] = []

    async def init(conn: Any) -> None:
        applied.append(conn)

    factory = with_connection_init(_fake_conn_factory, init)

    first, second = await factory(), await factory()

    assert applied == [first, second]


async def test_with_connection_init_declares_the_hook_for_the_worker_to_read() -> None:
    """The wrapped factory exposes the very callable it applies - the
    worker installs THAT hook as the slot pool's ``init``, never a copy
    or a wrapper, so what the registered connection got is what slot
    connections get."""

    async def init(conn: Any) -> None: ...

    factory = with_connection_init(_fake_conn_factory, init)

    assert connection_init_hook(factory) is init


def test_unwrapped_factories_declare_no_init_hook() -> None:
    """A bare factory (or anything else) exposes nothing - the read must
    be a clean None, never a guess, so the worker warns instead of
    inventing a hook."""
    assert connection_init_hook(_fake_conn_factory) is None
    assert connection_init_hook(object()) is None
    assert connection_init_hook(None) is None


async def test_with_connection_init_closes_the_connection_when_the_hook_fails() -> None:
    """A failed hook means no usable connection: the produced connection
    is closed (bounded, never raising over the hook's own error) before
    the error propagates - asyncpg's own hook contract, so a boot-time
    codec failure fails boot without leaking the connection."""
    produced = MagicMock(spec=asyncpg.Connection)

    async def factory() -> Any:
        return produced

    async def init(conn: Any) -> None:
        raise ValueError("codec registration failed")

    wrapped = with_connection_init(factory, init)

    with pytest.raises(ValueError, match="codec registration failed"):
        await wrapped()

    assert produced.close.await_count == 1  # type: ignore[attr-defined]  # Why: MagicMock(spec=...) narrows close to an AsyncMock-shaped attribute at runtime.


# ── Dead-on-acquire retry (_with_fresh_connection_retry) ──────────────
#
# The shared guard behind every "acquire from the pool, use immediately"
# call site (the enqueue paths and the bulk-cancel drain). The full-stack
# race it exists for - a server FATAL parking asyncpg's protocol before
# ``connection_lost`` lands - is pinned against a real interrupted
# Postgres in tests/test_fleet_pg_transient_failure.py, which is
# intermittent by nature; these unit pins hold the wrapper's own contract
# so the coverage does not rest on that race reproducing: when the retry
# runs, when it is REFUSED (a write already acknowledged, the
# duplication half), and what the guard's bounded checkout does with a
# release that fails or hangs after the op's work is done (the
# release/hang half).


class _FakeConn:
    """Structural stand-in for a checked-out pool connection proxy."""

    def __init__(self) -> None:
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True

    async def close(self) -> None:
        return None


class _FakePool:
    """Fake ``asyncpg.Pool`` for the guard's checkout, modelling asyncpg's
    ``PoolConnectionHolder.release`` in miniature. The release records the
    timeout the guard passes and can be told to fail (a parked connection's
    reset raising) or to PARK: a server that never answers, the shape the
    bound exists for. A parked release under a budget raises ``TimeoutError``
    at the budget and TERMINATES the connection (asyncpg's timeout handler
    does exactly this), freeing the holder either way; with no budget it
    simply parks: the unbounded shape that wedged the caller's task and
    ``pool.close()`` (the hang half, reproduced live against a
    SIGSTOP-frozen backend)."""

    def __init__(
        self,
        *,
        release_exc: BaseException | None = None,
        release_park_secs: float | None = None,
    ) -> None:
        self.release_exc = release_exc
        self.release_park_secs = release_park_secs
        self.release_timeouts: list[float | None] = []
        self.releases = 0
        self.in_use = 0
        self.terminated_conns: list[_FakeConn] = []

    async def acquire(self) -> _FakeConn:
        self.in_use += 1
        return _FakeConn()

    async def release(
        self,
        conn: _FakeConn,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: the parameter models asyncpg's Pool.release(timeout=...) signature: the guard's bounded-release channel, not a cancel scope.
    ) -> None:
        self.releases += 1
        self.release_timeouts.append(timeout)
        try:
            if self.release_park_secs is not None:
                # The reset is awaited UNDER the budget, as asyncpg awaits
                # it (compat.timeout inside holder.release).
                await asyncio.wait_for(asyncio.sleep(self.release_park_secs), timeout=timeout)
            if self.release_exc is not None:
                # A reset failure: asyncpg's except clause terminates the
                # connection and re-raises.
                conn.terminate()
                self.terminated_conns.append(conn)
                raise self.release_exc
        except TimeoutError:
            # Budget expiry: the connection is terminated and the timeout
            # re-raised; the holder is freed either way (asyncpg's
            # terminate -> _release_on_close).
            conn.terminate()
            self.terminated_conns.append(conn)
            raise
        finally:
            self.in_use -= 1


async def test_fresh_connection_retry_retries_once_on_a_poisoned_handout() -> None:
    """A first-statement ``InternalClientError`` (the dead-on-acquire
    signature) costs the caller one transparent retry: the operation body
    runs again on the fresh connection and its result is returned."""
    calls = 0

    async def op(guard: object) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InternalClientError(
                "cannot switch to state 15; another operation (2) is in progress"
            )
        return "done"

    result = await _with_fresh_connection_retry(_FakePool(), op, operation="unit-probe")

    assert result == "done"
    assert calls == 2


async def test_fresh_connection_retry_retries_only_once() -> None:
    """A second ``InternalClientError`` is a real driver state bug, not
    this race: it propagates rather than looping on a connection the pool
    keeps poisoning."""
    calls = 0

    async def op(guard: object) -> None:
        nonlocal calls
        calls += 1
        raise InternalClientError("still poisoned")

    with pytest.raises(InternalClientError, match="still poisoned"):
        await _with_fresh_connection_retry(_FakePool(), op, operation="unit-probe")

    assert calls == 2


async def test_fresh_connection_retry_passes_other_errors_through_untried() -> None:
    """The catch is deliberately narrow: the database's own error types
    are the call site's to classify (the drain retries deadlocks itself),
    so the wrapper neither retries nor rewrites them."""
    calls = 0

    async def op(guard: object) -> None:
        nonlocal calls
        calls += 1
        raise asyncpg.DeadlockDetectedError("real deadlock")

    with pytest.raises(asyncpg.DeadlockDetectedError, match="real deadlock"):
        await _with_fresh_connection_retry(_FakePool(), op, operation="unit-probe")

    assert calls == 1


async def test_fresh_connection_retry_logs_the_retry_with_the_operation_name() -> None:
    """The retry is observable: exactly one WARNING naming the operation,
    so an operator counting these can tell which call site is paying for
    a failover."""
    calls = 0

    async def op(guard: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InternalClientError("poisoned")

    with structlog.testing.capture_logs() as logs:
        await _with_fresh_connection_retry(_FakePool(), op, operation="unit-probe")

    warnings = [e for e in logs if e.get("event") == "pool-conn-dead-on-acquire"]
    assert len(warnings) == 1
    assert warnings[0]["kind"] == "pool_conn_dead_on_acquire"
    assert warnings[0]["operation"] == "unit-probe"


async def test_fresh_connection_retry_refuses_the_retry_once_a_write_is_acknowledged() -> None:
    """The refusal half: an ``InternalClientError`` raised AFTER the
    op marked its write durable (the connection died between the INSERT's
    acknowledgement and a LATER statement of the same attempt: a
    post-INSERT read, a savepoint RELEASE) must NOT re-run the op. The
    unguarded wrapper read every ``InternalClientError`` as
    dead-on-acquire and re-issued the write with the same identity;
    against the enqueue table's ``uuid PRIMARY KEY`` that cannot land a
    second row; it raised ``UniqueViolationError`` for an enqueue that
    had committed and would run (observed live under old-contract
    emulation: 11 UniqueViolations in 400 jittered kills), and that
    error-for-committed-work is precisely what invites the caller's
    fresh-id retry that DOES run the job twice."""
    calls = 0

    async def op(guard: _RetryGuard) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            # The write was acknowledged (marked), then a later statement
            # hit the parked dead connection.
            guard.mark_wrote()
            raise InternalClientError("died between the INSERT's ack and the follow-up read")
        raise AssertionError("unreachable: the retry must be refused once a write is durable")

    with pytest.raises(InternalClientError, match="died between the INSERT's ack"):
        await _with_fresh_connection_retry(_FakePool(), op, operation="enqueue")

    assert calls == 1, (
        "a retry after an acknowledged write re-issues it with the same id: "
        "a UniqueViolationError for work that succeeded, and the invitation "
        "for the caller's fresh-id re-enqueue that runs the job twice"
    )


async def test_fresh_connection_retry_retry_starts_a_clean_guard() -> None:
    """The retry attempt's flag starts clear: the FIRST attempt's
    pre-write failure (the dead-on-acquire case this wrapper exists for)
    must not poison the retry's durability accounting: the retried op
    can mark and complete normally."""
    calls = 0

    async def op(guard: _RetryGuard) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InternalClientError("poisoned before any statement was sent")
        guard.mark_wrote()
        return "done-on-retry"

    result = await _with_fresh_connection_retry(_FakePool(), op, operation="unit-probe")

    assert result == "done-on-retry"


async def test_retry_guard_checkout_passes_the_release_bound_to_the_pool() -> None:
    """The checkout replaces the acquire context manager precisely so the
    RELEASE can carry a timeout: asyncpg's context release falls back to
    the (unbounded) acquire timeout, which parks the reset against a
    silently-dead server forever: the hang half."""

    pool = _FakePool()
    guard = _RetryGuard(pool, "unit-probe")

    async with guard.checkout() as conn:
        assert isinstance(conn, _FakeConn)

    assert pool.releases == 1
    assert pool.release_timeouts == [_POOL_RELEASE_RESET_TIMEOUT_SECS]


async def test_retry_guard_checkout_bounds_a_parked_release_and_frees_the_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The hang half as a committed end-to-end pin: the live SIGSTOP
    experiment's shape (a release whose reset is sent into a server that
    never answers), driven through the guard's own checkout against a
    miniature that models what asyncpg's holder.release does at source
    level: the reset is awaited UNDER the budget; on expiry the connection
    is TERMINATED, the holder freed, and the timeout re-raised (which the
    checkout swallows into one WARNING). The op's committed result is
    returned, and a follow-up checkout on the same pool completes
    instantly: the pool is not wedged. The unbounded shape this replaces
    parked the caller's task and ``pool.close()`` forever (live-reproduced:
    release pending past 3 s with close() wedged; the bounded channel
    raised at 2.00 s and close() completed instantly)."""
    from taskq import connections as connections_mod

    shrunk_bound = 0.2
    monkeypatch.setattr(connections_mod, "_POOL_RELEASE_RESET_TIMEOUT_SECS", shrunk_bound)
    # The parked reset would answer in 10 s (it never answers at all, the
    # park is the point); the unbounded release shape waits all of it.
    pool = _FakePool(release_park_secs=10.0)
    guard = _RetryGuard(pool, "enqueue")

    async def op() -> str:
        async with guard.checkout():
            pass  # the op's statements all acknowledged; its work is committed
        return "committed"

    # The TEST budget is the red result for a regression to the unbounded
    # handoff: a checkout that stops passing the bound parks for the full
    # 10 s and the test budget fires at 2 s instead of the shrunk bound.
    budget = asyncio.timeout(2.0)
    started = time.monotonic()
    with structlog.testing.capture_logs() as logs:
        async with budget:
            result = await op()
    elapsed = time.monotonic() - started

    assert result == "committed", "a parked release must not fail the op's committed work"
    assert not budget.expired(), (
        "the checkout parked past the 2 s test budget: the release bound "
        "never reached the pool: the unbounded hang shape"
    )
    assert elapsed < 1.0, (
        f"the parked release cost {elapsed:.2f}s; the bound is {shrunk_bound}s; "
        "only the budget plus epsilon should have elapsed"
    )
    assert pool.release_timeouts == [shrunk_bound], "the bound must be the one handed to release"
    assert len(pool.terminated_conns) == 1, "budget expiry must terminate the parked connection"
    assert pool.terminated_conns[0].terminated
    assert pool.in_use == 0, "the holder must be freed: a wedged holder is pool.close() stuck"
    warnings = [e for e in logs if e.get("event") == "pool-release-failed"]
    assert len(warnings) == 1, "the swallowed timeout is the operator's signal"
    assert warnings[0]["kind"] == "pool_release_failed"
    assert warnings[0]["operation"] == "enqueue"

    # The pool is not wedged: a follow-up checkout completes instantly.
    followup_guard = _RetryGuard(pool, "enqueue")
    async with asyncio.timeout(1.0):
        async with followup_guard.checkout():
            pass
    assert pool.releases == 2


async def test_retry_guard_checkout_swallows_a_failed_release_after_a_committed_op() -> None:
    """A release whose reset fails (the parked-error-consume connection:
    asyncpg terminates it and re-raises) must not hand the caller an
    error for work that committed: that error invited the caller-side
    retry that duplicates the row, which is the other duplication
    route. The op's result stands; the failure is observable as one
    WARNING."""
    pool = _FakePool(release_exc=asyncpg.InterfaceError("pool release: reset failed"))
    guard = _RetryGuard(pool, "enqueue")

    with structlog.testing.capture_logs() as logs:
        async with guard.checkout():
            pass  # the op's statements all acknowledged; its work is committed

    warnings = [e for e in logs if e.get("event") == "pool-release-failed"]
    assert len(warnings) == 1, "the swallowed release failure is the operator's signal"
    assert warnings[0]["kind"] == "pool_release_failed"
    assert warnings[0]["operation"] == "enqueue"
    assert "reset failed" in warnings[0]["error"]


async def test_retry_guard_checkout_release_failure_never_masks_the_ops_own_error() -> None:
    """When the op's body raised, its error is the meaningful one: the
    checkout's release runs in the finally but its failure is swallowed,
    so the body's error is what the caller sees (the acquire context
    manager would have let the release error REPLACE it)."""
    pool = _FakePool(release_exc=asyncpg.InterfaceError("release blew up too"))
    guard = _RetryGuard(pool, "enqueue")

    with pytest.raises(ValueError, match="typed refusal"):
        async with guard.checkout():
            raise ValueError("typed refusal the caller must classify")
