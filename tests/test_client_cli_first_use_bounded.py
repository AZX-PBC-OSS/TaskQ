"""Contract: client/CLI first-use awaits are bounded (the defect-class sweep).

Client, web-admin and CLI processes arm NO watchdogs — the worker's
watchdog net does not exist in them — so every unbounded await on a
caller-supplied factory, or on the first network round trip at
open()/startup, is a process wedge with no signal. This file pins the
bounded discipline for every member of that class the sweep mapped:

* ``TaskQ.open()``'s ``pool_factory()`` call (the AAD first token fetch
  lives inside it) and ``TaskQ.reload_credentials()``'s rotation call —
  bounded with the SAME bound the worker applies to every factory call
  (``WorkerSettings.reload_factory_timeout``'s default, mirrored as
  ``taskq.client._taskq._RELOAD_FACTORY_TIMEOUT_SECS``).
* ``JobsClient._open_redis``'s eager ``initialize()`` — the first broker
  round trip; bounded the way the codebase bounds every redis operation
  (``asyncio.wait_for`` — redis-py socket kwargs are configured nowhere
  in src/taskq, while the worker bounds its redis factory identically).
* ``taskq ui serve`` startup: ``pool_factory()``, ``redis_factory()``,
  and the eager redis ``initialize()``; plus the ``/jobs/health/ready``
  PG probe — bounded with the worker's own readiness discipline
  (``acquire(timeout=...)`` + ``wait_for`` around ``SELECT 1``,
  ``health_pg_ping_timeout``'s default mirrored).
* The UI pool's ``command_timeout`` — the pool-level per-query bound
  that covers every admin-page query on it (the one-line fix the sweep
  mapped to ~20 admin query sites).
* Protocol completeness: ``_ClientSettings`` (and the web-admin test
  double) declare every ``BackendSettings`` member, per the protocol's
  own doctrine — every settings object that reaches a PostgresBackend
  must carry the knobs, so the contract is checkable.

Docker-free: hand-rolled fakes wired through the REAL seams
(``TaskQ.open``/``reload_credentials``, ``_ui_serve``'s lifespan, the
registered ``/jobs/health/ready`` endpoint closure). asyncpg types are
C-extensions — no MagicMock where a hang gate is needed. The budget
pattern is tests/test_notify_bootstrap_bounded.py's: the production
bound is shrunk through a module-global monkeypatch seam, a test-side
``asyncio.timeout`` budget catches the unbounded RED state, and
``budget.expired()`` discriminates production-bounded from test-budget.

No ``pytestmark`` — must run under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from typing import Any, Final, Self

import asyncpg
import pytest
from typer.testing import CliRunner

from taskq._json import loads
from taskq.cli import app
from taskq.client._taskq import TaskQ
from taskq.constants import DEFAULT_MAX_RETRY_BACKOFF

runner = CliRunner()

# ── Test helpers ───────────────────────────────────────────────────────

# Production's documented bounds, shrunk so a production-side bound fires
# well inside the test budget below: a first-use await that applies them
# raises within ~0.5s, while one that does not parks forever and the TEST
# budget is what fires.
_PROD_BOUND_SECS = 0.5
_PROD_PING_BOUND_SECS = 0.05
# 10x the configured production bound — generous. This budget firing is
# the red result: production never bounded the await on its own.
_TEST_BUDGET_SECS = 5.0


class _FakePool:
    """Fake asyncpg.Pool: instant bounded close, no hang gates.

    Awaitable (mirroring the real Pool) so a monkeypatched
    ``asyncpg.create_pool`` can return it. Fake conventions mirror
    tests/test_notify_bootstrap_bounded.py / tests/test_cli_ui.py.
    """

    def __init__(self, name: str = "") -> None:
        self.name = name
        self.close_calls = 0
        self.closed = False
        self.terminated = False

    async def close(self) -> None:
        self.close_calls += 1
        if self.closed or self.terminated:
            return
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True

    def __await__(self) -> Generator[object, None, _FakePool]:  # pyright: ignore[reportInvalidTypeVarUse]  # Why: mirrors tests/test_cli_ui.py's _FakePool; Generator is imported from collections.abc there and re-declared here for locality.
        async def _self() -> _FakePool:
            return self

        return _self().__await__()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass


def _black_hole_pool_factory(entered: asyncio.Event) -> Any:
    """Zero-arg async factory that accepts the call and never returns."""

    async def factory() -> asyncpg.Pool:
        entered.set()
        await asyncio.Event().wait()  # never set: the credential provider hangs
        raise AssertionError("unreachable: the hang gate is never set")

    return factory


def _black_hole_redis_factory(entered: asyncio.Event) -> Any:
    """Redis-factory twin of _black_hole_pool_factory."""

    async def factory() -> Any:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable: the hang gate is never set")

    return factory


class _FakeRedisHangingInitialize:
    """Fake redis.asyncio.Redis whose eager initialize() parks forever.

    aclose() completes instantly so the bounded-close unwind (the
    push-before-initialize doctrine) never eats the test budget.
    Mirrors the _FakeRedisClient conventions in tests/test_jobs_client.py.
    """

    def __init__(self) -> None:
        self.initialize_entered = False
        self.aclose_calls = 0

    async def initialize(self) -> _FakeRedisHangingInitialize:
        self.initialize_entered = True
        await asyncio.Event().wait()  # never set: the broker is black-holed
        raise AssertionError("unreachable: the hang gate is never set")

    async def aclose(self) -> None:
        self.aclose_calls += 1


def _capture_ui_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pool_factory: Any = None,
    redis_factory: Any = None,
    create_pool: Any = None,
    from_url_result: Any = None,
) -> Any:
    """Run ``_ui_serve`` with a stubbed ``uvicorn.run`` and return the app.

    Mirrors ``_capture_app_for_lifespan`` in tests/test_cli_ui.py,
    extended with the first-use seams: ``pool_factory`` /
    ``redis_factory`` pass through to ``_ui_serve`` (setting redis_url so
    the redis branch runs), ``create_pool`` replaces
    ``asyncpg.create_pool`` wholesale (a recording wrapper pins the
    pool-creation kwargs), and ``from_url_result`` replaces
    ``redis.asyncio.from_url``.
    """
    import uvicorn

    captured: dict[str, Any] = {}

    def _fake_uvicorn_run(app: Any, **kwargs: Any) -> None:
        captured["app"] = app

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)

    redis_url: str | None = None
    if create_pool is not None:
        # The module-top asyncpg import is the same module object
        # taskq.cli binds, so patching it patches the cli path.
        monkeypatch.setattr(asyncpg, "create_pool", create_pool)
    if from_url_result is not None:
        import redis.asyncio as aioredis

        monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: from_url_result)
        redis_url = "redis://localhost:6379/0"
    if redis_factory is not None:
        redis_url = "redis://localhost:6379/0"

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    from taskq.cli import _ui_serve
    from taskq.settings import TaskQSettings

    _ui_serve(
        "postgresql://u:p@h:5432/db",
        "taskq",
        redis_url,
        "127.0.0.1",
        9999,
        False,
        TaskQSettings.load(),
        pool_factory=pool_factory,
        redis_factory=redis_factory,
    )
    return captured["app"]


async def _call_ready_endpoint(app: Any) -> Any:
    """Invoke the registered ``/jobs/health/ready`` endpoint closure.

    The route is mounted inside the lifespan, so the app must be inside
    ``app.router.lifespan_context(app)`` when this is called. The raw
    endpoint is the smallest real seam the CLI exposes for the probe —
    driving it directly bounds the RED state with a test-side budget.

    Why the ``original_router`` traversal: this FastAPI mounts routers
    through its ``_IncludedRouter`` lazy wrapper, whose ``original_router``
    holds the concrete APIRoutes (a plain ``app.routes`` walk sees only
    the wrappers). Both layouts are walked so the lookup does not depend
    on the installed FastAPI's internals. A for-loop (never ``next()``):
    a StopIteration raised inside a coroutine becomes an opaque
    RuntimeError.
    """
    from fastapi.routing import APIRoute

    candidates: list[Any] = list(app.routes)
    for route in candidates:
        inner = getattr(route, "original_router", None)
        if inner is not None:
            candidates.extend(inner.routes)
    for route in candidates:
        if isinstance(route, APIRoute) and route.path == "/jobs/health/ready":
            return await route.endpoint()
    pytest.fail("the /jobs/health/ready route was not registered by the UI lifespan")


# ── TaskQ.open(): the first pool-factory call is bounded ───────────────


async def test_taskq_open_pool_factory_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """The FIRST ``pool_factory()`` call during ``TaskQ.open()`` completes
    or raises within the configured bound. A credential provider that
    accepts the call and never returns must fail the open loudly — the
    worker already bounds its identical bootstrap factory calls with
    ``reload_factory_timeout`` — not park the client process forever
    before anything else exists (no watchdog is armed in client space)."""
    import taskq.client._taskq as taskq_mod

    monkeypatch.setattr(taskq_mod, "_RELOAD_FACTORY_TIMEOUT_SECS", _PROD_BOUND_SECS, raising=False)
    entered = asyncio.Event()

    tq = TaskQ(pool_factory=_black_hole_pool_factory(entered), schema="taskq")
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            await tq.open()
    except TimeoutError as exc:
        assert entered.is_set(), "open failed before reaching the pool factory call"
        if budget.expired():
            pytest.fail(
                "TaskQ.open() parks forever in the pool_factory() call: still "
                f"suspended {_TEST_BUDGET_SECS:.0f}s in with a "
                f"{_PROD_BOUND_SECS}s bound configured. The AAD first token "
                "fetch lives inside the factory; the worker bounds its "
                "identical bootstrap factory calls with "
                "reload_factory_timeout, and client processes arm no "
                "watchdogs — a black-holed token endpoint wedges the "
                "client undetected."
            )
        assert "pool_factory" in str(exc), f"the timeout must name the seam, got: {exc!r}"
        # The open failed before anything was constructed — no half-open state.
        assert tq._pool is None
        assert tq._client is None
        return
    pytest.fail("open() returned against a factory that never returns — impossible")


# ── TaskQ.reload_credentials(): the rotation factory call is bounded ───


async def test_taskq_reload_credentials_pool_factory_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``reload_credentials()``'s factory call completes or raises within
    the configured bound. A token endpoint that wedged after a healthy
    first fetch must fail the rotation loudly and leave the live pool
    untouched and serving — the worker's reload bounds its identical
    factory calls — not park the client forever."""
    import taskq.client._taskq as taskq_mod

    monkeypatch.setattr(taskq_mod, "_RELOAD_FACTORY_TIMEOUT_SECS", _PROD_BOUND_SECS, raising=False)
    first = _FakePool("first")
    reload_entered = asyncio.Event()
    calls = {"n": 0}

    async def factory() -> asyncpg.Pool:
        calls["n"] += 1
        if calls["n"] == 1:
            return first  # type: ignore[return-value]  # Why: hand-rolled fake accepted through the PoolFactory seam.
        reload_entered.set()
        await asyncio.Event().wait()  # the rotation fetch black-holes
        raise AssertionError("unreachable: the hang gate is never set")

    tq = TaskQ(pool_factory=factory, schema="taskq")
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.open()
    assert tq._pool is first

    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            await tq.reload_credentials()
    except TimeoutError as exc:
        assert reload_entered.is_set(), "reload failed before reaching the factory call"
        if budget.expired():
            pytest.fail(
                "TaskQ.reload_credentials() parks forever in the "
                f"pool_factory() call: still suspended {_TEST_BUDGET_SECS:.0f}s "
                f"in with a {_PROD_BOUND_SECS}s bound configured. The worker's "
                "reload_credentials bounds its identical factory calls with "
                "reload_factory_timeout; the client-side counterpart must "
                "too, or a wedged token endpoint parks the client with the "
                "live pool held hostage."
            )
        assert "pool_factory" in str(exc), f"the timeout must name the seam, got: {exc!r}"
        # The live pool is untouched and still serving — the docstring contract.
        assert tq._pool is first
        assert tq._deps is not None and tq._deps.worker_pool is first
        assert first.close_calls == 0, "a failed rotation must never close the live pool"
        async with asyncio.timeout(_TEST_BUDGET_SECS):
            await tq.close()
        return
    pytest.fail("reload_credentials() returned against a factory that never returns")


# ── TaskQ.open(): the eager first Redis connection is bounded ──────────


async def test_taskq_open_redis_initialize_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``JobsClient._open_redis``'s eager ``initialize()`` — the first
    broker round trip — completes or raises within the configured bound.
    A black-holed Redis must fail ``TaskQ.open()`` loudly, not park it
    forever. The codebase bounds redis operations with ``asyncio.wait_for``
    (the worker bounds its redis factory that way); redis-py socket kwargs
    are configured nowhere in src/taskq."""
    from unittest.mock import MagicMock

    import redis.asyncio as redis_async

    import taskq.client._jobs as jobs_mod

    monkeypatch.setattr(
        jobs_mod.JobsClient, "_OPEN_REDIS_TIMEOUT_SECS", _PROD_BOUND_SECS, raising=False
    )
    fake = _FakeRedisHangingInitialize()
    monkeypatch.setattr(redis_async, "from_url", lambda *a, **kw: fake)

    tq = TaskQ(
        pool=MagicMock(spec=asyncpg.Pool),
        redis_url="redis://localhost:6379/0",
        schema="taskq",
    )
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            await tq.open()
    except TimeoutError as exc:
        if budget.expired():
            assert fake.initialize_entered, "open failed before reaching initialize()"
            pytest.fail(
                "TaskQ.open() parks forever in the Redis initialize() call: "
                f"still suspended {_TEST_BUDGET_SECS:.0f}s in with a "
                f"{_PROD_BOUND_SECS}s bound configured. initialize() is the "
                "eager first broker round trip; client processes arm no "
                "watchdogs, so a black-holed broker wedges the open with no "
                "signal."
            )
        assert "initialize" in str(exc), f"the timeout must name the seam, got: {exc!r}"
        # The failed eager setup must still be releasable without wedging:
        # the bounded-close callback was pushed BEFORE initialize() (the
        # doctrine pinned by tests/test_jobs_client.py) — close() runs it.
        async with asyncio.timeout(_TEST_BUDGET_SECS):
            await tq.close()
        assert fake.aclose_calls == 1
        return
    pytest.fail("open() returned against a broker that never completes initialize()")


# ── TaskQ-built pools carry the per-query command_timeout ──────────────


async def test_taskq_open_dsn_pool_carries_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The DSN pool ``TaskQ.open()`` builds carries ``command_timeout`` —
    the pool-level per-query bound. Without it, a black-holed Postgres
    parks the client's first enqueue/get/cancel forever (client processes
    arm no watchdogs), the one pool family the worker-side pools all
    carry the bound for."""

    import asyncpg as asyncpg_mod

    pool = _FakePool("dsn")
    captured_kwargs: dict[str, Any] = {}

    def _recording_create_pool(*a: Any, **kw: Any) -> _FakePool:
        captured_kwargs.update(kw)
        return pool

    # open() imports asyncpg lazily (line-local, the optional-driver
    # boundary), so the seam is the asyncpg module itself — the same
    # patching shape the redis-bound test uses for redis_async.from_url.
    monkeypatch.setattr(asyncpg_mod, "create_pool", _recording_create_pool)
    tq = TaskQ(dsn="postgresql://x:x@localhost/x", schema="taskq")
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.open()
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.close()

    assert captured_kwargs.get("command_timeout") == 10.0, (
        "the DSN pool TaskQ builds must carry the pool-level per-query "
        "bound (client._taskq._CLIENT_POOL_COMMAND_TIMEOUT_SECS — set "
        "deliberately above the 5 s enqueue-path lock budgets so the "
        "server-side typed lock refusal wins the race against asyncpg's "
        "client-side cancellation), got "
        f"kwargs: {sorted(captured_kwargs)}"
    )


def test_taskq_pg_provider_pool_factory_carries_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``pg_provider`` sugar's pool factory carries the same
    ``command_timeout``: the sugar collapses into
    ``make_pg_pool_factory``, and a factory that omits the bound leaves
    the provider path the one unbounded client pool left standing. (A
    caller-supplied ``pool_factory`` stays caller-owned — its timeouts
    are its choice, the worker's doctrine for caller pools.)"""

    import taskq.auth as auth_mod
    import taskq.client._taskq as taskq_mod

    captured_kwargs: dict[str, Any] = {}

    def _recording_factory(*a: Any, **kw: Any) -> Any:
        captured_kwargs.update(kw)
        return _black_hole_pool_factory(asyncio.Event())

    monkeypatch.setattr(auth_mod, "make_pg_pool_factory", _recording_factory)
    TaskQ(
        dsn="postgresql://u@h/db",
        pg_provider=_Provider(),
        schema="taskq",
    )

    assert captured_kwargs.get("command_timeout") == 10.0, (
        "make_pg_pool_factory must receive the pool-level per-query bound "
        "(client._taskq._CLIENT_POOL_COMMAND_TIMEOUT_SECS) from the "
        "pg_provider sugar, got kwargs: "
        f"{sorted(captured_kwargs)}"
    )
    assert taskq_mod._CLIENT_POOL_COMMAND_TIMEOUT_SECS == 10.0


# ── Enqueue lock budgets vs the client pool's per-query bound ──────────
#
# The typed lock-timeout refusals (MaxPendingLockTimeoutError and
# siblings) fire server-side via a lock_timeout GUC — they only exist if
# the budget fits inside the pool's per-query command_timeout with the
# 80% share of headroom the refusal needs to unwind first. These pins
# cover the wiring that makes an operator's TASKQ_*_LOCK_TIMEOUT_MS reach
# that arithmetic on the client path: the env overlay, the pool-bound
# derivation, and the clamp.


def _deps_budgets(tq: TaskQ) -> tuple[float, float, float]:
    """The three lock budgets the client's backend was actually handed."""
    deps = tq._deps  # pyright: ignore[reportPrivateUsage]  # Why: the wiring under test is what open() hands the backend; no public accessor exposes it.
    assert deps is not None
    settings = deps.settings
    return (
        settings.max_pending_lock_timeout_ms,
        settings.unique_for_lock_timeout_ms,
        settings.idempotency_lock_timeout_ms,
    )


async def test_taskq_open_delivers_default_budgets_inside_the_pool_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At the shipped defaults the client path delivers each 5000 ms
    budget in full inside the pool's 10.0 s per-query bound — the bound
    is deliberately larger than every 5000 ms lock budget, so the
    server-side lock_timeout fires well before the client-side timer and
    the refusal is the typed one, never a bare TimeoutError."""
    import asyncpg as asyncpg_mod

    captured_kwargs: dict[str, Any] = {}

    def _recording_create_pool(*a: Any, **kw: Any) -> _FakePool:
        captured_kwargs.update(kw)
        return _FakePool("dsn")

    monkeypatch.setattr(asyncpg_mod, "create_pool", _recording_create_pool)
    tq = TaskQ(dsn="postgresql://x:x@localhost/x", schema="taskq")
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.open()
        budgets = _deps_budgets(tq)
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.close()

    assert captured_kwargs.get("command_timeout") == 10.0
    assert budgets == (5000.0, 5000.0, 5000.0)


async def test_taskq_open_operator_widened_budget_is_delivered_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An operator widening a budget past its default re-derives the
    pool's per-query bound to fit it at the same 80% share — the widened
    budget is delivered in full (not silently clamped to fit the floor),
    and the untouched siblings now fit the larger bound unclamped too."""
    import asyncpg as asyncpg_mod

    captured_kwargs: dict[str, Any] = {}

    def _recording_create_pool(*a: Any, **kw: Any) -> _FakePool:
        captured_kwargs.update(kw)
        return _FakePool("dsn")

    monkeypatch.setattr(asyncpg_mod, "create_pool", _recording_create_pool)
    monkeypatch.setenv("TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS", "30000")
    tq = TaskQ(dsn="postgresql://x:x@localhost/x", schema="taskq")
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.open()
        budgets = _deps_budgets(tq)
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.close()

    assert captured_kwargs.get("command_timeout") == 37.5, (
        "a 30000 ms budget needs a 37.5 s pool bound to keep its 80% share; "
        f"got {captured_kwargs.get('command_timeout')!r}"
    )
    assert budgets == (5000.0, 5000.0, 30000.0)


async def test_taskq_caller_supplied_pool_leaves_budgets_as_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller-owned pool's timeouts are the caller's own choice (the
    worker's doctrine for caller pools): no derivation, no clamp — the
    budgets stand as configured."""
    monkeypatch.setenv("TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS", "12000")
    tq = TaskQ(pool=_FakePool("caller"), schema="taskq")  # type: ignore[arg-type]  # Why: the pool seam under test is duck-typed at open(); the fake covers the close path.
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.open()
        budgets = _deps_budgets(tq)
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await tq.close()

    assert budgets == (12000.0, 5000.0, 5000.0)


def test_taskq_pg_provider_pool_factory_derives_the_bound_from_a_widened_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pg_provider sugar's pool factory carries the DERIVED bound too,
    not the bare floor — otherwise the provider path would be the one
    client pool where widening a lock budget silently does nothing."""

    import taskq.auth as auth_mod

    captured_kwargs: dict[str, Any] = {}

    def _recording_factory(*a: Any, **kw: Any) -> Any:
        captured_kwargs.update(kw)
        return _black_hole_pool_factory(asyncio.Event())

    monkeypatch.setattr(auth_mod, "make_pg_pool_factory", _recording_factory)
    monkeypatch.setenv("TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS", "20000")
    TaskQ(
        dsn="postgresql://u@h/db",
        pg_provider=_Provider(),
        schema="taskq",
    )

    assert captured_kwargs.get("command_timeout") == 25.0, (
        "a 20000 ms budget needs a 25 s pool bound to keep its 80% share; "
        f"got {captured_kwargs.get('command_timeout')!r}"
    )


# ── taskq ui serve: startup factory calls and eager redis are bounded ──


async def test_ui_serve_lifespan_pool_factory_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The UI's startup ``pool_factory()`` call completes or raises within
    the configured bound — the same first-use factory discipline as
    ``TaskQ.open()`` and the worker's bootstrap. A hung credential
    provider must fail UI startup loudly, not park ``taskq ui serve``
    forever before any request is served."""
    import taskq.cli as cli_mod

    monkeypatch.setattr(cli_mod, "_UI_FACTORY_TIMEOUT_SECS", _PROD_BOUND_SECS, raising=False)
    entered = asyncio.Event()

    app = _capture_ui_app(monkeypatch, pool_factory=_black_hole_pool_factory(entered))
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget, app.router.lifespan_context(app):
            pass
    except TimeoutError as exc:
        assert entered.is_set(), "startup failed before reaching the pool factory call"
        if budget.expired():
            pytest.fail(
                "taskq ui serve startup parks forever in the pool_factory() "
                f"call: still suspended {_TEST_BUDGET_SECS:.0f}s in with a "
                f"{_PROD_BOUND_SECS}s bound configured. The worker bounds its "
                "bootstrap factory calls with reload_factory_timeout; the UI "
                "process arms no watchdogs, so a black-holed token endpoint "
                "wedges the admin server undetected."
            )
        assert "pool_factory" in str(exc), f"the timeout must name the seam, got: {exc!r}"
        return
    pytest.fail("UI startup completed against a factory that never returns")


async def test_ui_serve_lifespan_redis_factory_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The UI's startup ``redis_factory()`` call completes or raises within
    the configured bound — the worker's reload bounds its redis factory
    identically. A hung Redis credential provider must fail UI startup
    loudly, not park it forever."""
    import taskq.cli as cli_mod

    monkeypatch.setattr(cli_mod, "_UI_FACTORY_TIMEOUT_SECS", _PROD_BOUND_SECS, raising=False)
    pool = _FakePool("ui")
    entered = asyncio.Event()

    app = _capture_ui_app(
        monkeypatch,
        create_pool=lambda *a, **kw: pool,
        redis_factory=_black_hole_redis_factory(entered),
    )
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget, app.router.lifespan_context(app):
            pass
    except TimeoutError as exc:
        assert entered.is_set(), "startup failed before reaching the redis factory call"
        if budget.expired():
            pytest.fail(
                "taskq ui serve startup parks forever in the redis_factory() "
                f"call: still suspended {_TEST_BUDGET_SECS:.0f}s in with a "
                f"{_PROD_BOUND_SECS}s bound configured. The worker's reload "
                "bounds its identical redis factory call with "
                "reload_factory_timeout; the UI process arms no watchdogs."
            )
        assert "redis_factory" in str(exc), f"the timeout must name the seam, got: {exc!r}"
        return
    pytest.fail("UI startup completed against a redis factory that never returns")


async def test_ui_serve_lifespan_redis_initialize_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The UI's eager redis ``initialize()`` (the first broker round trip
    on the from_url path) completes or raises within the configured
    bound. A black-holed broker must fail UI startup loudly — and the
    from_url-allocated client must still be released by the pushed
    bounded-close callback during the unwind (the push-before-initialize
    doctrine)."""
    import taskq.cli as cli_mod

    monkeypatch.setattr(cli_mod, "_UI_FACTORY_TIMEOUT_SECS", _PROD_BOUND_SECS, raising=False)
    pool = _FakePool("ui")
    fake = _FakeRedisHangingInitialize()

    app = _capture_ui_app(monkeypatch, create_pool=lambda *a, **kw: pool, from_url_result=fake)
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget, app.router.lifespan_context(app):
            pass
    except TimeoutError as exc:
        if budget.expired():
            assert fake.initialize_entered, "startup failed before reaching initialize()"
            pytest.fail(
                "taskq ui serve startup parks forever in the Redis "
                f"initialize() call: still suspended {_TEST_BUDGET_SECS:.0f}s "
                f"in with a {_PROD_BOUND_SECS}s bound configured. "
                "initialize() is the eager first broker round trip; the UI "
                "process arms no watchdogs, so a black-holed broker wedges "
                "the admin server undetected."
            )
        assert "initialize" in str(exc), f"the timeout must name the seam, got: {exc!r}"
        # The unwind ran the pushed bounded close: the from_url()-allocated
        # client is released, not leaked by the failed eager setup.
        assert fake.aclose_calls == 1
        return
    pytest.fail("UI startup completed against a broker that never completes initialize()")


# ── taskq ui serve: the readiness probe is bounded ─────────────────────


class _HangOnAcquirePool:
    """Fake asyncpg.Pool honouring the acquire contract: with NO timeout
    kwarg the acquire parks forever (the pre-fix unbounded shape); with
    one, it waits the bound then raises TimeoutError (asyncpg's own
    acquire-timeout shape). close() completes instantly so lifespan
    teardown never eats the test budget."""

    def __init__(self) -> None:
        self.close_calls = 0

    def acquire(self, *, timeout: float | None = None) -> _HangOnAcquire:
        return _HangOnAcquire(timeout)

    async def close(self) -> None:
        self.close_calls += 1

    def terminate(self) -> None:
        pass

    def __await__(self) -> Generator[object, None, _HangOnAcquirePool]:
        async def _self() -> _HangOnAcquirePool:
            return self

        return _self().__await__()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass


class _HangOnAcquire:
    """Acquire CM that parks forever unbounded, or honours the bound."""

    def __init__(self, timeout: float | None) -> None:
        self._timeout = timeout

    async def __aenter__(self) -> Any:
        if self._timeout is None:
            await asyncio.Event().wait()  # pre-fix: no bound was passed
            raise AssertionError("unreachable: the hang gate is never set")
        await asyncio.sleep(self._timeout)
        raise TimeoutError  # asyncpg's acquire-timeout shape

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _HangOnExecuteConn:
    """Fake asyncpg.Connection whose SELECT parks forever (black-holed PG:
    the connection completed but the query never returns)."""

    async def execute(self, sql: str, *args: object) -> str:
        await asyncio.Event().wait()
        raise AssertionError("unreachable: the hang gate is never set")


class _FastAcquireHangExecutePool:
    """Fake asyncpg.Pool whose acquire is instant but whose connections'
    execute() parks forever."""

    def __init__(self) -> None:
        self.close_calls = 0

    def acquire(self, *, timeout: float | None = None) -> _FastAcquire:
        return _FastAcquire(_HangOnExecuteConn())

    async def close(self) -> None:
        self.close_calls += 1

    def terminate(self) -> None:
        pass

    def __await__(self) -> Generator[object, None, _FastAcquireHangExecutePool]:
        async def _self() -> _FastAcquireHangExecutePool:
            return self

        return _self().__await__()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass


class _FastAcquire:
    def __init__(self, conn: _HangOnExecuteConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _HangOnExecuteConn:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> None:
        return None


async def test_ui_serve_ready_hanging_acquire_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``/jobs/health/ready`` bounds its pool acquire: a wedged pool
    (exhausted or black-holed) returns 503 within the bound instead of
    parking the probe forever. The worker's readiness ping bounds the
    identical acquire with ``health_pg_ping_timeout`` (worker/health.py)."""
    import taskq.cli as cli_mod

    monkeypatch.setattr(cli_mod, "_UI_PG_PING_TIMEOUT_SECS", _PROD_PING_BOUND_SECS, raising=False)
    pool = _HangOnAcquirePool()

    app = _capture_ui_app(monkeypatch, create_pool=lambda *a, **kw: pool)
    async with app.router.lifespan_context(app):
        try:
            response = await asyncio.wait_for(_call_ready_endpoint(app), timeout=_TEST_BUDGET_SECS)
        except TimeoutError:
            pytest.fail(
                "the /jobs/health/ready endpoint parks forever in "
                f"pool.acquire(): still suspended {_TEST_BUDGET_SECS:.0f}s in "
                f"with a {_PROD_PING_BOUND_SECS}s bound configured. The "
                "worker's readiness ping bounds the identical acquire with "
                "health_pg_ping_timeout; an unbounded probe turns a wedged "
                "pool into a wedged prober."
            )
    assert response.status_code == 503
    body = loads(response.body)
    assert body["ready"] is False
    assert body["reasons"] == ["pg_ping_timeout"]


async def test_ui_serve_ready_hanging_execute_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``/jobs/health/ready`` bounds its SELECT 1 probe: a connection that
    completes the acquire and then black-holes the query returns 503
    within the bound instead of parking the probe forever. The worker's
    readiness ping wraps the identical execute in wait_for
    (worker/health.py)."""
    import taskq.cli as cli_mod

    monkeypatch.setattr(cli_mod, "_UI_PG_PING_TIMEOUT_SECS", _PROD_PING_BOUND_SECS, raising=False)
    pool = _FastAcquireHangExecutePool()

    app = _capture_ui_app(monkeypatch, create_pool=lambda *a, **kw: pool)
    async with app.router.lifespan_context(app):
        try:
            response = await asyncio.wait_for(_call_ready_endpoint(app), timeout=_TEST_BUDGET_SECS)
        except TimeoutError:
            pytest.fail(
                "the /jobs/health/ready endpoint parks forever in the "
                f"SELECT 1 probe: still suspended {_TEST_BUDGET_SECS:.0f}s in "
                f"with a {_PROD_PING_BOUND_SECS}s bound configured. The "
                "worker's readiness ping wraps the identical execute in "
                "wait_for; an unbounded probe turns a black-holed PG into a "
                "wedged prober."
            )
    assert response.status_code == 503
    body = loads(response.body)
    assert body["ready"] is False
    assert body["reasons"] == ["pg_ping_timeout"]


# ── taskq ui serve: the admin pool carries a per-query bound ───────────


async def test_ui_serve_pool_carries_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The UI's admin pool is created with a ``command_timeout`` — the
    pool-level per-query bound that covers every admin-page query on it.
    Without it, a black-holed PG wedges each admin request forever (the
    sweep mapped ~20 such query sites to this one pool-creation line)."""

    pool = _FakePool("ui")
    captured_kwargs: dict[str, Any] = {}

    def _recording_create_pool(*a: Any, **kw: Any) -> _FakePool:
        captured_kwargs.update(kw)
        return pool

    app = _capture_ui_app(monkeypatch, create_pool=_recording_create_pool)
    async with app.router.lifespan_context(app):
        pass

    assert captured_kwargs.get("command_timeout") == 5.0, (
        f"create_pool must carry the pool-level per-query bound "
        f"(cli._UI_POOL_COMMAND_TIMEOUT_SECS, mirroring "
        f"WorkerSettings.dispatcher_command_timeout's default), got "
        f"kwargs: {sorted(captured_kwargs)}"
    )


class _Provider:  # pyright: ignore[reportUnusedClass]  # Why: reached only through the CLI's dynamic "module:attr" credential-provider ref below — pyright cannot see string-based access.
    """Duck-typed PgCredentialProvider for the ui-serve command wiring test.

    Never actually consulted: the factory built from it is captured, not
    invoked (``_ui_serve`` is stubbed).
    """

    async def get_pg_credential(self) -> Any:
        return None


def test_ui_serve_command_passes_command_timeout_to_pool_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The credential-provider path builds the UI's admin pool through
    ``make_pg_pool_factory`` — that call carries the same per-query
    ``command_timeout``, or the provider path would be the one unbounded
    admin pool left standing. (The migrate ``conn_factory`` is
    deliberately NOT bounded here: DDL may legitimately exceed a
    per-query budget.)"""
    import taskq.cli as cli_mod

    captured: dict[str, Any] = {}

    def _fake_make_pg_pool_factory(dsn: str, provider: Any, **kw: Any) -> Any:
        captured.update(kw)

        async def _never_called() -> Any:  # pyright: ignore[reportUnusedFunction]  # Why: captured factory shape; _ui_serve is stubbed so it is never invoked.
            raise AssertionError("unreachable: _ui_serve is stubbed")

        return _never_called

    monkeypatch.setattr(cli_mod, "make_pg_pool_factory", _fake_make_pg_pool_factory)
    monkeypatch.setattr(
        cli_mod,
        "_ui_serve",
        lambda *a, **kw: captured.setdefault("serve_called", True),
    )
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.delenv("TASKQ_REDIS_URL", raising=False)
    monkeypatch.delenv("TASKQ_PG_CREDENTIAL_PROVIDER", raising=False)
    monkeypatch.delenv("TASKQ_REDIS_CREDENTIAL_PROVIDER", raising=False)

    result = runner.invoke(
        app,
        [
            "ui",
            "serve",
            "--pg-dsn",
            "postgresql://u:p@h:5432/db",
            "--pg-credential-provider",
            "tests.test_client_cli_first_use_bounded:_Provider",
        ],
    )
    assert result.exit_code == 0, result.stderr
    assert captured.get("serve_called") is True, "the command never reached _ui_serve"
    assert captured.get("command_timeout") == 5.0, (
        f"make_pg_pool_factory must carry the per-query bound, got kwargs: {sorted(captured)}"
    )


# ── Protocol completeness: the settings doubles declare every knob ─────

_KNOB_DEFAULT_FIELDS: tuple[tuple[str, Any], ...] = (
    ("max_pending_lock_timeout_ms", 5000.0),
    ("unique_for_lock_timeout_ms", 5000.0),
    ("idempotency_lock_timeout_ms", 5000.0),
    ("max_retry_backoff", DEFAULT_MAX_RETRY_BACKOFF),
)
"""The BackendSettings members whose defaults the doubles must carry —
WorkerSettings' own (5000.0 ms each for the enqueue lock budgets, the
module constants the backend's defensive getattr fallbacks supply;
24 h for the reclaim sweep's backoff ceiling).

Why presence is asserted via getattr and not runtime_checkable isinstance:
a runtime protocol isinstance inspects only METHOD members on Python
3.12+/3.13 — data annotations are ignored — so it cannot pin a settings
double's data fields. Direct presence+value is the checkable form of the
protocol's doctrine (the static check is pyright's, which the tests
execution environment relaxes for argument passing)."""

_MISSING: Final[Any] = object()


def _assert_declared_knobs(settings_object: Any, double_name: str) -> None:
    # The presence set is derived from the protocol, not hand-listed: a
    # knob added to BackendSettings must appear on every settings object
    # the backend can receive, and a hand-list stops at the members its
    # author remembered.
    from taskq.backend._protocol import BackendSettings

    for field in BackendSettings.__annotations__:
        value = getattr(settings_object, field, _MISSING)
        assert value is not _MISSING, (
            f"{double_name} must declare BackendSettings.{field} — the "
            "protocol's doctrine: every settings object that reaches a "
            "PostgresBackend must carry the knobs, so the contract is "
            "checkable rather than hoped for."
        )
    for field, expected in _KNOB_DEFAULT_FIELDS:
        assert getattr(settings_object, field) == expected, (
            f"{double_name}.{field} must mirror WorkerSettings' default ({expected}), got "
            f"{getattr(settings_object, field)!r}"
        )


def test_client_settings_satisfies_backend_settings_protocol() -> None:
    """``_ClientSettings`` declares every ``BackendSettings`` member — the
    protocol's own doctrine: every settings object that reaches a
    PostgresBackend must carry the knobs, so the contract is checkable
    rather than hoped for. The enqueue lock-budget fields and the reclaim
    backoff ceiling carry value pins; the backend's defensive getattr
    fallbacks stay for undeclared doubles."""
    from taskq.client._taskq import _ClientSettings

    _assert_declared_knobs(_ClientSettings(schema_name="taskq"), "_ClientSettings")


def test_web_admin_settings_double_satisfies_backend_settings_protocol() -> None:
    """The web-admin integration double satisfies the same declared
    contract as ``_ClientSettings`` — same doctrine, same fields."""
    from tests.test_web_admin_integration import _TestBackendSettings

    _assert_declared_knobs(_TestBackendSettings(), "_TestBackendSettings")
