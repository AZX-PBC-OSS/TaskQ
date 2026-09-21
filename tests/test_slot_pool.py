"""The per-slot transaction pool: sizing, supply, and readiness probe.

Three behaviours this file pins, each at the level that can actually
see its failure mode:

* **Factory sizing** (unit, spied ``asyncpg.create_pool`` /
  ``make_pg_pool_factory`` - the ``tests/test_auth.py`` precedent): the
  pool the worker opens is fully warmed at ``max_concurrency + 1`` -
  one connection per consumer slot plus the readiness reserve - on the
  direct DSN with the dispatcher command timeout. Under-sizing is the
  defect class where a fully-utilised worker's readiness ping times
  out waiting on a slot-held connection and reports health as unready.
* **Supply rule** (integration, real PG): ``max_concurrency + 1``
  concurrent acquires all succeed - the pool genuinely holds a
  connection per slot plus the reserve, not just a configured number.
* **Single-flight readiness** (unit): concurrent probes share ONE
  in-flight slot-pool ping, which is what makes the one-connection
  reserve sufficient; and a failing shared probe fails every waiter
  that joined it.
* **Registered-connection setup** (integration, real PG): connections
  the pool hands out carry the session state the application configured
  on the LOOP-registered connection, and - when the registration is a
  factory that declares its init hook - its per-connection setup (type
  codecs above all). Losing either turns a concurrency knob into a
  silent behaviour change. The read off the registered connection
  (``_registered_session_state``) and the factory's
  ``session_settings``/``init`` forwarding are pinned at unit level
  below: every read failure must warn and leave the setting
  uninherited, never skip it quietly. The raw channel (a
  ``register_value`` connection) cannot carry codecs - the driver
  seals them - so the contract there is the loud boundary: one WARN
  naming the supported channel, and the slot connection verifiably
  running without the codec.
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
import structlog.testing

from taskq._di import ProviderRegistry, Scope
from taskq.auth import ReloadSchedule
from taskq.connections import with_connection_init
from taskq.obs import set_slot_pool_occupancy_source
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import (
    _maybe_open_slot_pool,
    _registered_session_state,
    _slot_pool_factory,
    _startup_log,
)
from taskq.worker.health import _ping_slot_pool


def _make_settings(*, max_concurrency: int = 4, pg_dsn_direct: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn_direct,
            "TASKQ_PG_DSN_DIRECT": pg_dsn_direct,
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
        }
    )


# ── Factory sizing ───────────────────────────────────────────────────────


async def test_slot_pool_factory_sizes_warm_pool_on_direct_dsn() -> None:
    """min_size == max_size == max_concurrency + 1, direct DSN, command
    timeout - the fully-warmed shape whose absence puts connection
    establishment (and a credential fetch) inside the dispatch hot path."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")
    fake_pool = MagicMock()
    create_pool = AsyncMock(return_value=fake_pool)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.asyncpg.create_pool", create_pool)
        factory = _slot_pool_factory(settings, None)
        pool = await factory()

    assert pool is fake_pool
    call_kwargs = create_pool.call_args.kwargs
    assert call_kwargs["dsn"] == "postgresql://u:p@h:5432/db"
    assert call_kwargs["min_size"] == 5
    assert call_kwargs["max_size"] == 5
    assert call_kwargs["command_timeout"] == settings.dispatcher_command_timeout
    assert call_kwargs["max_inactive_connection_lifetime"] == settings.pool_max_inactive_lifetime
    # Same statement-cache treatment as the provider-backed branch -
    # resolved from settings (the defaults here), so the DSN-built pool
    # never drifts from the pool family's cache behaviour.
    assert call_kwargs["statement_cache_size"] == settings.statement_cache_size
    assert call_kwargs["max_cached_statement_lifetime"] == settings.max_cached_statement_lifetime


async def test_slot_pool_factory_is_provider_backed_when_given_provider() -> None:
    """With a credential provider the factory comes from
    make_pg_pool_factory - the documented managed-identity path - sized
    and timed out identically, so switching authentication never changes
    the pool's budget."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")
    provider = MagicMock()
    fake_factory = MagicMock()
    make_pg_pool_factory = MagicMock(return_value=fake_factory)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.make_pg_pool_factory", make_pg_pool_factory)
        factory = _slot_pool_factory(settings, provider)

    assert factory is fake_factory
    make_pg_pool_factory.assert_called_once_with(
        "postgresql://u:p@h:5432/db",
        provider,
        min_size=5,
        max_size=5,
        max_inactive_connection_lifetime=settings.pool_max_inactive_lifetime,
        command_timeout=settings.dispatcher_command_timeout,
        # Same statement-cache treatment as the DSN-built branch - resolved
        # from settings (the defaults here), so switching authentication
        # never switches cache behaviour.
        statement_cache_size=settings.statement_cache_size,
        max_cached_statement_lifetime=settings.max_cached_statement_lifetime,
        reload_schedule=ANY,
    )
    # The slot pool rotates on the same cadence as the role pools: the
    # operator's interval (unset here), else the lease it is granted.
    schedule = make_pg_pool_factory.call_args.kwargs["reload_schedule"]
    assert isinstance(schedule, ReloadSchedule)
    assert schedule.configured == settings.reload_interval


async def test_slot_pool_factory_carries_session_settings_onto_dsn_built_pool() -> None:
    """``session_settings`` reach ``asyncpg.create_pool`` as
    ``server_settings`` - the channel that puts the registered
    connection's session state on every connection the pool warms.
    Dropped, and raising max_concurrency silently repoints unqualified
    names and RLS roles back to the server defaults."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")
    session = {"search_path": "app,public", "role": "app_role"}
    create_pool = AsyncMock(return_value=MagicMock())

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.asyncpg.create_pool", create_pool)
        factory = _slot_pool_factory(settings, None, session)
        await factory()

    assert create_pool.call_args.kwargs["server_settings"] == session


async def test_slot_pool_factory_carries_init_onto_dsn_built_pool() -> None:
    """``init`` reaches ``asyncpg.create_pool`` verbatim - the channel
    that puts the registration's declared per-connection hook (codecs
    above all) on every connection the pool warms. Dropped, and a codec
    the application declared on its registration silently decodes to the
    driver's default on slot connections only."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")

    async def init(conn: asyncpg.Connection) -> None:
        raise AssertionError("never invoked - create_pool is mocked")

    create_pool = AsyncMock(return_value=MagicMock())

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.asyncpg.create_pool", create_pool)
        factory = _slot_pool_factory(settings, None, None, init=init)
        await factory()

    assert create_pool.call_args.kwargs["init"] is init


def test_slot_pool_factory_forwards_init_to_the_provider_backed_factory() -> None:
    """The managed-identity branch forwards the same hook to
    ``make_pg_pool_factory`` - switching authentication must not switch
    whether slot connections get the registration's per-connection
    setup."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")
    provider = MagicMock()

    async def init(conn: asyncpg.Connection) -> None: ...

    make_pg_pool_factory = MagicMock()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.make_pg_pool_factory", make_pg_pool_factory)
        _slot_pool_factory(settings, provider, None, init=init)

    assert make_pg_pool_factory.call_args.kwargs["init"] is init


def test_slot_pool_factory_forwards_session_settings_to_the_provider_backed_factory() -> None:
    """The managed-identity branch forwards the same mapping to
    ``make_pg_pool_factory`` - switching authentication must not switch
    whether slot connections inherit the session state."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")
    provider = MagicMock()
    session = {"search_path": "app,public"}
    make_pg_pool_factory = MagicMock()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.make_pg_pool_factory", make_pg_pool_factory)
        _slot_pool_factory(settings, provider, session)

    assert make_pg_pool_factory.call_args.kwargs["server_settings"] == session


async def test_slot_pool_factory_passes_no_settings_when_there_is_nothing_to_inherit() -> None:
    """No mapping and an empty mapping behave alike, and no hook behaves
    like no mapping: the worker with nothing to inherit builds exactly
    the pool it always built - the DSN branch passes the driver's own
    defaults, the provider branch adds no kwarg."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")

    for session in (None, {}):
        create_pool = AsyncMock(return_value=MagicMock())
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("taskq.worker._bootstrap.asyncpg.create_pool", create_pool)
            factory = _slot_pool_factory(settings, None, session)
            await factory()
        assert create_pool.call_args.kwargs["server_settings"] is None
        assert create_pool.call_args.kwargs["init"] is None

        provider = MagicMock()
        make_pg_pool_factory = MagicMock()
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("taskq.worker._bootstrap.make_pg_pool_factory", make_pg_pool_factory)
            _slot_pool_factory(settings, provider, session)
        assert "server_settings" not in make_pg_pool_factory.call_args.kwargs
        assert "init" not in make_pg_pool_factory.call_args.kwargs


# ── Registered-connection session state read ─────────────────────────────
#
# Unit pins for the read half of the carry-over: what the live session
# holds, the pool inherits; what cannot be read is warned about and left
# at the server default, never skipped quietly (a silent no-op here is
# indistinguishable from the defect the read exists to eliminate).


async def test_registered_session_state_reads_the_live_values() -> None:
    """Both inherited settings come back as read from the live session -
    not from connect-time parameters, so a post-connect ``SET`` (an
    ``init`` hook, or a statement right after ``connect()``) is seen."""
    settings = _make_settings(max_concurrency=2, pg_dsn_direct="postgresql://u:p@h:5432/db")
    answers = {
        "SHOW search_path": "app,public",
        "SELECT current_setting('role')": "app_role",
    }

    async def fetchval(query: str) -> object:
        return answers[query]

    with structlog.testing.capture_logs() as logs:
        state = await _registered_session_state(
            SimpleNamespace(fetchval=fetchval), settings, _startup_log
        )

    assert state == {"search_path": "app,public", "role": "app_role"}
    assert [e for e in logs if e["event"] == "slot-pool-registered-session-unreadable"] == [], (
        f"a healthy read must not warn: {logs}"
    )


async def test_registered_session_state_warns_and_omits_a_setting_that_cannot_be_read() -> None:
    """A failed read warns naming the setting and inherits the readable
    half - the slot pool falls back to the server default for exactly
    one setting, loudly, never for both and never silently."""
    settings = _make_settings(max_concurrency=2, pg_dsn_direct="postgresql://u:p@h:5432/db")

    async def fetchval(query: str) -> object:
        if "current_setting" in query:
            raise OSError("connection closed mid-read")
        return "app,public"

    with structlog.testing.capture_logs() as logs:
        state = await _registered_session_state(
            SimpleNamespace(fetchval=fetchval), settings, _startup_log
        )

    assert state == {"search_path": "app,public"}
    matches = [e for e in logs if e["event"] == "slot-pool-registered-session-unreadable"]
    assert len(matches) == 1, f"exactly one unreadable warning expected: {logs}"
    assert matches[0]["log_level"] == "warning"
    assert matches[0]["setting"] == "role"
    assert "connection closed mid-read" in matches[0]["error"]


async def test_registered_session_state_warns_and_inherits_nothing_when_unqueryable() -> None:
    """A registration that cannot answer a query (a duck-typed stand-in,
    not a live connection) degrades to the server defaults with one
    warning - the boot harnesses register such stand-ins, and breaking
    boot over a coverage read would be a worse failure."""
    settings = _make_settings(max_concurrency=2, pg_dsn_direct="postgresql://u:p@h:5432/db")

    with structlog.testing.capture_logs() as logs:
        state = await _registered_session_state(object(), settings, _startup_log)

    assert state == {}
    matches = [e for e in logs if e["event"] == "slot-pool-registered-session-unreadable"]
    assert len(matches) == 1, f"exactly one unreadable warning expected: {logs}"
    assert matches[0]["log_level"] == "warning"


# ── Supply rule (real PG) ────────────────────────────────────────────────


@pytest.mark.integration
async def test_slot_pool_supplies_concurrency_plus_one_concurrent_acquires(
    module_pg_schema: Any,
) -> None:
    """max_concurrency + 1 connections are concurrently acquirable.

    The behavioral half of the sizing statement: every consumer slot
    can hold its transaction connection at once, with the one reserve
    still free for the readiness probe.
    """
    settings = _make_settings(max_concurrency=3, pg_dsn_direct=module_pg_schema.pg_dsn)
    factory = _slot_pool_factory(settings, None)
    pool = await factory()
    try:
        conns = await asyncio.wait_for(
            asyncio.gather(*(pool.acquire() for _ in range(4))),
            timeout=settings.dispatcher_command_timeout,
        )
        try:
            # All four holds are live at once - supply, not configuration.
            for conn in conns:
                await conn.execute("SELECT 1")
        finally:
            for conn in conns:
                await pool.release(conn)
    finally:
        await pool.close()


@pytest.mark.integration
async def test_pooled_knob_builds_a_real_pool_with_the_statement_cache_disabled(
    module_pg_schema: Any,
) -> None:
    """Layer 2 against a real pool object: TASKQ_PG_IS_POOLED=true → cache 0/0.

    The spied create_pool pins (tests/test_connections.py) prove TaskQ
    forwards the override; this pin proves the kwargs survive into a real
    asyncpg pool object. asyncpg keeps the resolved connect kwargs on the
    pool's private ``_connect_kwargs`` (no public accessor exists), and a
    real pool answers with a live server connection underneath, so the
    assertion covers the exact construction path a pooled deployment
    runs.
    """
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": module_pg_schema.pg_dsn,
            "TASKQ_PG_DSN_DIRECT": module_pg_schema.pg_dsn,
            "TASKQ_PG_IS_POOLED": "true",
            "TASKQ_STATEMENT_CACHE_SIZE": "512",
            "TASKQ_MAX_CACHED_STATEMENT_LIFETIME": "3600",
        }
    )
    pool = await _slot_pool_factory(settings, None)()
    try:
        # Why private access: asyncpg seals the resolved kwargs on
        # _connect_kwargs with no public read; this is the only place the
        # forwarded statement-cache pair is inspectable after construction.
        connect_kwargs: dict[str, Any] = pool._connect_kwargs  # type: ignore[union-attr,reportAttributeAccessIssue]
        assert connect_kwargs["statement_cache_size"] == 0
        assert connect_kwargs["max_cached_statement_lifetime"] == 0
    finally:
        await pool.close()


# ── Single-flight readiness probe ────────────────────────────────────────


class _SlowPool:
    """Slot-pool stand-in whose acquire is slow enough for callers to
    overlap, counting every acquire - the single-flight discriminator.

    ``acquired`` fires synchronously inside ``acquire()``, so a test can
    await the START of a probe deterministically (no timing sleeps).
    """

    def __init__(self, *, delay: float = 0.05, error: Exception | None = None) -> None:
        self.delay = delay
        self.error = error
        self.acquire_calls = 0
        self.acquired = asyncio.Event()

    def acquire(self, *, timeout: float | None = None) -> _SlowAcquireCtx:
        self.acquire_calls += 1
        self.acquired.set()
        return _SlowAcquireCtx(self)


class _SlowAcquireCtx:
    def __init__(self, pool: _SlowPool) -> None:
        self._pool = pool

    async def __aenter__(self) -> Any:
        await asyncio.sleep(self._pool.delay)
        if self._pool.error is not None:
            raise self._pool.error
        return SimpleNamespace(execute=AsyncMock(return_value="OK"))

    async def __aexit__(self, *_exc: object) -> None:
        return None


def _deps_with_pool(pool: _SlowPool) -> SimpleNamespace:
    return SimpleNamespace(
        slot_pool=pool,
        # The single-flight probe state lives on deps (per-worker), so
        # each test's fresh deps starts with no in-flight probe.
        slot_pool_probe_task=None,
        settings=SimpleNamespace(health_pg_ping_timeout=1.0),
    )


async def test_concurrent_probes_share_one_in_flight_ping() -> None:
    pool = _SlowPool()
    deps = _deps_with_pool(pool)

    results = await asyncio.gather(*(_ping_slot_pool(deps) for _ in range(3)))

    assert pool.acquire_calls == 1, (
        "concurrent readiness requests must join one in-flight probe - "
        "parallel pings contend for the single readiness-reserve "
        "connection and report a healthy, busy worker as unready"
    )
    assert results == [(True, None), (True, None), (True, None)]


async def test_probe_state_is_per_worker_not_process_global() -> None:
    """Two workers can share a process; one worker's in-flight probe must
    never answer another worker's readiness request - worker B would be
    told about worker A's pool. A module-global probe (the shape this
    per-deps state replaced) coalesces B's ping onto A's probe and
    never touches B's pool at all."""
    pool_a = _SlowPool()
    pool_b = _SlowPool()
    deps_a = _deps_with_pool(pool_a)
    deps_b = _deps_with_pool(pool_b)

    results = await asyncio.gather(
        _ping_slot_pool(deps_a),
        _ping_slot_pool(deps_b),
    )

    assert pool_a.acquire_calls == 1, "worker A's probe must ping A's pool once"
    assert pool_b.acquire_calls == 1, (
        "worker B's readiness request was answered by worker A's probe - "
        "probe state has leaked across workers"
    )
    assert results == [(True, None), (True, None)]


async def test_wedged_probe_degrades_to_a_failed_ping_not_a_hang() -> None:
    """A probe task left over from a torn-down event loop can never
    complete; joining it must be bounded by one ping budget and report
    unready, never hang readiness (the every-wait-bounded
    rule). Simulated with a probe task that never completes."""
    pool = _SlowPool()
    deps = _deps_with_pool(pool)
    deps.settings = SimpleNamespace(health_pg_ping_timeout=0.05)

    never_completes = asyncio.ensure_future(asyncio.Event().wait())
    deps.slot_pool_probe_task = never_completes

    result = await _ping_slot_pool(deps)

    assert result == (False, "slot_pool_ping_timeout")
    assert pool.acquire_calls == 0, "the wedged probe must be joined, not replaced"
    never_completes.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await never_completes


async def test_shared_probe_failure_fails_every_waiter() -> None:
    pool = _SlowPool(error=TimeoutError())
    deps = _deps_with_pool(pool)

    results = await asyncio.gather(
        *(_ping_slot_pool(deps) for _ in range(3)), return_exceptions=False
    )

    assert pool.acquire_calls == 1
    ok = [r[0] for r in results]
    assert ok == [False, False, False]
    assert all(r[1] == "slot_pool_ping_timeout" for r in results)


async def test_cancelled_waiter_does_not_cancel_the_shared_probe() -> None:
    """A cancelled readiness request must not take the shared probe with
    it - the next waiter still joins the SAME in-flight ping, which is
    what keeps the one-connection reserve sufficient. A shield-less
    implementation fails here: the first cancellation kills the probe
    and the survivor pays a second acquire (and the reserve contention
    the single flight exists to prevent)."""
    pool = _SlowPool(delay=0.1)
    deps = _deps_with_pool(pool)

    waiter_to_cancel = asyncio.create_task(_ping_slot_pool(deps))
    # Deterministic sequencing, no timing sleeps: wait for the probe to
    # have STARTED (the pool fires `acquired` inside acquire()), then
    # one scheduler yield lets the survivor run to its first await -
    # by which point it has either joined the in-flight probe or
    # started a second one, and the count below says which.
    await pool.acquired.wait()
    survivor = asyncio.create_task(_ping_slot_pool(deps))
    await asyncio.sleep(0)
    waiter_to_cancel.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await waiter_to_cancel

    survivor_result = await survivor

    assert pool.acquire_calls == 1, (
        "the cancelled waiter took the shared probe with it - the "
        "survivor had to start a second acquire"
    )
    assert survivor_result == (True, None)


async def test_sequential_probes_each_ping_freshly() -> None:
    """A probe arriving after the previous one completed starts a new
    one - single-flight coalesces concurrent probes, never serves a
    stale result."""
    pool = _SlowPool(delay=0.0)
    deps = _deps_with_pool(pool)

    await _ping_slot_pool(deps)
    await _ping_slot_pool(deps)

    assert pool.acquire_calls == 2


# ── Registered-connection setup carries onto slot connections ────────────
#
# At max_concurrency == 1 the actor receives the LOOP-registered
# connection itself, so whatever the application configured on it - a
# ``set_type_codec`` registration, an ``init``/``setup`` callback, a
# ``SET ROLE``, a ``search_path`` or any other server setting - is
# present by construction. The moment max_concurrency rises above 1 the
# worker hands actors connections out of its own per-slot pool instead.
# Unless that pool builds its connections with the same setup, a
# deployment's behaviour changes silently with a concurrency knob:
# RLS-driving roles vanish, a custom search_path resolves different
# tables, and a domain type the application registered a codec for comes
# back as a raw string. Nothing fails loudly - the actor just reads and
# writes the wrong thing. These pins are the reason the per-slot pool is
# not allowed to be a bare direct-DSN pool.
#
# The two halves carry differently. Session state (search_path, role) is
# read back off the live connection and replayed as startup GUCs - the
# driver exposes it. Codecs and init hooks are sealed inside the
# driver's per-connection state with no read-back, so they carry only
# when the registration DECLARES them: a LOOP-scope factory registration
# whose factory exposes its init hook (with_connection_init /
# make_dedicated_conn_factory's setup) gets the hook replayed as the
# slot pool's init; a raw register_value connection gets one boot-time
# WARN naming that channel, and the pins below hold both sides of the
# line.


@pytest.mark.integration
async def test_slot_connections_carry_the_registered_connections_server_settings(
    module_pg_schema: Any,
) -> None:
    """A ``search_path`` configured on the registered LOOP-scope connection
    is present on every connection the per-slot pool hands out.

    ``search_path`` stands for the whole server-settings/role family: it
    is observable with a plain ``SHOW``, and an actor whose unqualified
    table references resolve against a different search_path than the one
    the application configured reads and writes the wrong schema without
    raising anything.
    """
    schema = module_pg_schema.schema_name
    settings = _make_settings(max_concurrency=2, pg_dsn_direct=module_pg_schema.pg_dsn)

    registered = await asyncpg.connect(
        module_pg_schema.pg_dsn,
        server_settings={"search_path": f"{schema},public"},
    )
    deps, loop_scope, stack = await _slot_pool_harness(registered)
    try:
        opened = await _maybe_open_slot_pool(
            loop_scope,
            settings,
            deps,
            factory=_slot_pool_factory(settings, None),
            pg_credential_provider=None,
            caller_supplied_pg_pools=False,
            log=_quiet_log(),
        )
        assert opened, "the per-slot path must activate at max_concurrency > 1"

        registered_path = await registered.fetchval("SHOW search_path")
        async with deps.slot_pool.acquire() as slot_conn:
            slot_path = await slot_conn.fetchval("SHOW search_path")

        assert slot_path == registered_path, (
            "the per-slot connection resolves unqualified names against a "
            f"different search_path ({slot_path!r}) than the registered "
            f"connection ({registered_path!r}) - raising max_concurrency "
            "silently repointed every actor's queries at another schema"
        )
    finally:
        await stack.aclose()
        await registered.close()


@pytest.mark.integration
async def test_slot_pool_boot_fails_loudly_when_the_inherited_role_is_unassumable(
    module_pg_schema: Any,
) -> None:
    """A ``role`` read off the registered connection that the slot pool's
    own connecting user is NOT permitted to assume must fail the pool
    build loudly, not silently degrade some slot connections to a
    different role than others.

    ``role`` is carried the same way ``search_path`` is: read live off
    the registered connection and replayed as an ``asyncpg`` startup
    ``server_settings`` GUC on every connection the pool warms
    (``min_size == max_size``, so the whole pool opens during the
    build). If the connecting DSN user lacks ``SET ROLE`` privilege on
    the inherited role, the server rejects the startup ``SET ROLE`` on
    every one of those connections - this pins that the failure
    surfaces as the documented boot-time ``RuntimeError`` naming the
    host, not as a half-warmed pool or a role silently reverted to the
    connecting user's own.
    """
    schema = module_pg_schema.schema_name
    bootstrap_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    unpriv_user = f"unpriv_{schema}"
    target_role = f"unassumable_{schema}"
    try:
        # An unprivileged LOGIN role, deliberately never GRANTed
        # membership in target_role - that missing grant is the
        # boundary condition this test exercises. The slot pool's own
        # connecting DSN user is swapped to this role, not the
        # superuser the container's default DSN authenticates as
        # (which can SET ROLE to anything, so the failure this test
        # pins would never trigger against it).
        await bootstrap_conn.execute(f"CREATE ROLE \"{unpriv_user}\" LOGIN PASSWORD 'unpriv'")
        await bootstrap_conn.execute(f'CREATE ROLE "{target_role}" NOLOGIN')

        parts = urlsplit(module_pg_schema.pg_dsn)
        restricted_netloc = f"{unpriv_user}:unpriv@{parts.hostname}:{parts.port}"
        restricted_dsn = urlunsplit(
            (parts.scheme, restricted_netloc, parts.path, parts.query, parts.fragment)
        )

        unpriv_conn = await asyncpg.connect(restricted_dsn)
        try:
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await unpriv_conn.execute(f'SET ROLE "{target_role}"')
        finally:
            await unpriv_conn.close()

        settings = _make_settings(max_concurrency=2, pg_dsn_direct=restricted_dsn)

        # A registered connection whose live `role` reads back as the
        # unassumable role, via a duck-typed stand-in that answers the
        # two session-state queries _registered_session_state issues -
        # the same callable-guard path _registered_session_state already
        # documents supporting for registrations it cannot introspect
        # more directly.
        async def fake_fetchval(query: str) -> str:
            if "search_path" in query:
                return f"{schema},public"
            if "role" in query:
                return target_role
            raise AssertionError(f"unexpected session-state query: {query!r}")

        fake_registered = SimpleNamespace(fetchval=fake_fetchval)
        deps, loop_scope, stack = await _slot_pool_harness(fake_registered)
        try:
            with pytest.raises(RuntimeError, match="slot pool failed to open"):
                await _maybe_open_slot_pool(
                    loop_scope,
                    settings,
                    deps,
                    factory=_slot_pool_factory(settings, None),
                    pg_credential_provider=None,
                    caller_supplied_pg_pools=False,
                    log=_quiet_log(),
                )
            assert deps.slot_pool is None, (
                "a pool that failed to fully warm must not be left partially installed on deps"
            )
        finally:
            await stack.aclose()
    finally:
        await bootstrap_conn.execute(f'DROP ROLE IF EXISTS "{target_role}"')
        await bootstrap_conn.execute(f'DROP ROLE IF EXISTS "{unpriv_user}"')
        await bootstrap_conn.close()


@pytest.mark.integration
async def test_raw_registered_type_codecs_warn_and_do_not_reach_slot_connections(
    module_pg_schema: Any,
) -> None:
    """The raw-channel boundary: a codec applied to a ``register_value``
    LOOP connection is NOT inherited by slot connections - loudly.

    asyncpg seals per-connection codec state at connect time (probed:
    no public or private read-back exists, and ``asyncpg.connect`` takes
    no hook parameter that a resolved connection could carry), so a codec
    registered by ``set_type_codec`` on an already-built connection is
    mechanically unrecoverable for the pool the worker builds. The
    shipped contract for this channel is therefore a boundary, not an
    inheritance: exactly one boot-time WARN naming the supported channel
    (a hook-declaring factory - pinned by the next test), and a slot
    connection that keeps working with the driver's default
    representation. Both halves are pinned so the boundary cannot
    silently regress either way - the warning going missing, or
    inheritance appearing by accident.
    """
    settings = _make_settings(max_concurrency=2, pg_dsn_direct=module_pg_schema.pg_dsn)

    registered = await asyncpg.connect(module_pg_schema.pg_dsn)
    await registered.set_type_codec(
        "json",
        encoder=lambda value: '{"tagged": true}',
        decoder=lambda value: {"decoded_by": "registered-codec"},
        schema="pg_catalog",
    )
    deps, loop_scope, stack = await _slot_pool_harness(registered)
    try:
        with structlog.testing.capture_logs() as logs:
            opened = await _maybe_open_slot_pool(
                loop_scope,
                settings,
                deps,
                factory=_slot_pool_factory(settings, None),
                pg_credential_provider=None,
                caller_supplied_pg_pools=False,
                di_registry=_value_registering(registered),
                log=_startup_log,
            )
        assert opened, "the per-slot path must activate at max_concurrency > 1"

        warnings = [e for e in logs if e["event"] == "slot_pool_registered_setup_not_inherited"]
        assert len(warnings) == 1, (
            f"exactly one not-inherited warning per boot, no fewer (silent "
            f"divergence) and no more (warning storm): {logs}"
        )
        assert warnings[0]["log_level"] == "warning"
        assert "with_connection_init" in warnings[0]["note"], (
            "the warning must name the supported channel so an operator can act on it"
        )

        # The divergence itself, pinned in both directions: the codec IS
        # live on the registered connection (the registration worked),
        # and the slot connection answers with the driver's default -
        # the documented, warned-about non-inheritance.
        on_registered = await registered.fetchval("SELECT '{\"a\": 1}'::json")
        assert on_registered == {"decoded_by": "registered-codec"}
        async with deps.slot_pool.acquire() as slot_conn:
            on_slot = await slot_conn.fetchval("SELECT '{\"a\": 1}'::json")
        assert on_slot == '{"a": 1}', (
            "a raw-channel codec must NOT reach slot connections - got "
            f"{on_slot!r}. If asyncpg ever grows a codec read-back this "
            "pin fails first, and the documented boundary must be "
            "revisited (the factory channel would then be redundant)."
        )
    finally:
        await stack.aclose()
        await registered.close()


@pytest.mark.integration
async def test_slot_connections_inherit_codecs_through_a_hook_carrying_factory(
    module_pg_schema: Any,
) -> None:
    """The supported channel: register the LOOP-scope connection through
    a factory that DECLARES its per-connection init hook, and every
    connection the slot pool warms decodes with the same codec.

    ``with_connection_init`` applies the hook to the connection it
    produces AND declares it on the factory; ``_maybe_open_slot_pool``
    reads the declaration off the DI registry entry (only a ``factory``
    registration can carry one) and threads it into the slot pool's
    ``init``. This is the inheritance the raw channel cannot provide -
    and with it in force, the not-inherited warning must NOT fire.
    """
    settings = _make_settings(max_concurrency=2, pg_dsn_direct=module_pg_schema.pg_dsn)

    async def install_json_codec(conn: asyncpg.Connection) -> None:
        await conn.set_type_codec(
            "json",
            encoder=lambda value: '{"tagged": true}',
            decoder=lambda value: {"decoded_by": "init-hook-codec"},
            schema="pg_catalog",
        )

    dsn = module_pg_schema.pg_dsn

    async def raw_conn_factory() -> asyncpg.Connection:
        return await asyncpg.connect(dsn)

    conn_factory = with_connection_init(raw_conn_factory, install_json_codec)

    registry = ProviderRegistry()
    registry.register_factory(asyncpg.Connection, Scope.LOOP, conn_factory)

    # Resolved through the same factory the registry holds, mirroring
    # what LoopScope.bootstrap produces - the harness drives
    # _maybe_open_slot_pool directly, so resolution happens here.
    registered = await conn_factory()
    deps, loop_scope, stack = await _slot_pool_harness(registered)
    try:
        with structlog.testing.capture_logs() as logs:
            opened = await _maybe_open_slot_pool(
                loop_scope,
                settings,
                deps,
                factory=_slot_pool_factory(settings, None),
                pg_credential_provider=None,
                caller_supplied_pg_pools=False,
                di_registry=registry,
                log=_startup_log,
            )
        assert opened, "the per-slot path must activate at max_concurrency > 1"

        assert [
            e for e in logs if e["event"] == "slot_pool_registered_setup_not_inherited"
        ] == [], f"the hook-carrying channel must not warn: {logs}"
        inherits = [e for e in logs if e["event"] == "slot_pool_inherits_registered_session"]
        assert len(inherits) == 1 and inherits[0]["init_hook"] is True, (
            f"the boot log must record that an init hook was inherited: {logs}"
        )

        # The wrapper applied the codec to the registered connection…
        on_registered = await registered.fetchval("SELECT '{\"a\": 1}'::json")
        assert on_registered == {"decoded_by": "init-hook-codec"}

        # …and the inherited init applied it to EVERY connection the pool
        # warmed at build time (min_size == max_size, so every acquire is
        # a distinct physical connection) - not just the first acquire.
        conns = await asyncio.gather(
            *(deps.slot_pool.acquire() for _ in range(settings.max_concurrency + 1))
        )
        try:
            on_slots = await asyncio.gather(
                *(conn.fetchval("SELECT '{\"a\": 1}'::json") for conn in conns)
            )
        finally:
            for conn in conns:
                await deps.slot_pool.release(conn)
        assert on_slots == [{"decoded_by": "init-hook-codec"}] * len(conns), (
            f"every warm slot connection must carry the codec: {on_slots!r}"
        )
    finally:
        await stack.aclose()
        await registered.close()


@pytest.mark.integration
async def test_a_failing_inherited_init_hook_refuses_boot(
    module_pg_schema: Any,
) -> None:
    """The inherited init hook runs inside the pool's warm build, so a
    hook the server rejects (a codec naming a missing type, a failing
    SET) fails the build - and the bounded-open guard turns that into a
    boot refusal naming the host. The one thing the worker must never do
    with a half-configured pool is boot anyway and serve connections the
    application would mis-read."""
    settings = _make_settings(max_concurrency=2, pg_dsn_direct=module_pg_schema.pg_dsn)

    async def failing_init(conn: asyncpg.Connection) -> None:
        # A value the server rejects outright (InvalidParameterValue) -
        # a dotted custom-GUC name would NOT do: Postgres accepts those
        # lazily, so the hook would succeed and prove nothing.
        await conn.execute("SET statement_timeout TO 'not-a-duration'")

    registered = await asyncpg.connect(module_pg_schema.pg_dsn)
    deps, loop_scope, stack = await _slot_pool_harness(registered)

    registry = ProviderRegistry()

    async def raw_conn_factory() -> asyncpg.Connection:
        return await asyncpg.connect(module_pg_schema.pg_dsn)

    registry.register_factory(
        asyncpg.Connection,
        Scope.LOOP,
        with_connection_init(raw_conn_factory, failing_init),
    )
    try:
        try:
            await _maybe_open_slot_pool(
                loop_scope,
                settings,
                deps,
                factory=_slot_pool_factory(settings, None),
                pg_credential_provider=None,
                caller_supplied_pg_pools=False,
                di_registry=registry,
                log=_quiet_log(),
            )
        except RuntimeError as exc:
            assert "slot pool failed to open" in str(exc)
        else:
            raise AssertionError(
                "a slot pool whose inherited init hook fails must refuse boot, not serve"
            )
        assert deps.slot_pool is None, "a refused boot leaves no half-built pool behind"
    finally:
        await stack.aclose()
        await registered.close()


async def test_slot_pool_warns_once_when_the_registration_declares_no_init_hook() -> None:
    """Unit half of the boundary: any registration without a declared
    hook - a value provider, a bare factory, no registry at all - opens
    the slot pool WITHOUT the registration's codecs and says so exactly
    once at WARN. The failure mode this guards is the warning going
    missing: the codec divergence itself raises nothing anywhere."""
    settings = _make_settings(max_concurrency=2, pg_dsn_direct="postgresql://u:p@h:5432/db")

    async def fetchval(_query: str) -> object:
        return ""  # queryable but empty: no session state, no session warning

    registered = SimpleNamespace(fetchval=fetchval)
    stack = AsyncExitStack()
    await stack.__aenter__()
    deps = SimpleNamespace(slot_pool=None, slot_pool_factory=None, _exit_stack=stack)
    loop_scope = SimpleNamespace(resolved_cache=lambda: {asyncpg.Connection: registered})
    pool = _FakePool()

    async def factory() -> Any:
        return pool

    for di_registry in (None, _value_registering(registered), ProviderRegistry()):
        try:
            with structlog.testing.capture_logs() as logs:
                opened = await _maybe_open_slot_pool(
                    loop_scope,
                    settings,
                    deps,
                    factory=factory,
                    pg_credential_provider=None,
                    caller_supplied_pg_pools=False,
                    di_registry=di_registry,
                    log=_startup_log,
                )
            assert opened
            events = [e["event"] for e in logs]
            assert events.count("slot_pool_registered_setup_not_inherited") == 1, (
                f"di_registry={di_registry!r}: {events}"
            )
            # Queryable-but-empty session state: neither the inherit line
            # nor the session-unreadable warning belongs to this shape.
            assert "slot_pool_inherits_registered_session" not in events
            assert "slot-pool-registered-session-unreadable" not in events
        finally:
            set_slot_pool_occupancy_source(None)
    await stack.aclose()


def _value_registering(registered: Any) -> ProviderRegistry:
    """The registry the raw channel produces: the connection as a LOOP
    value - the shape whose codecs are mechanically unrecoverable."""
    registry = ProviderRegistry()
    registry.register_value(asyncpg.Connection, Scope.LOOP, registered)
    return registry


class _FakePool:
    """Just enough pool for ``_maybe_open_slot_pool`` to open and close:
    the occupancy gauge reads sizes defensively, and teardown runs the
    bounded close against ``close()``."""

    async def close(self) -> None:
        return None

    def terminate(self) -> None:
        return None

    def get_size(self) -> int:
        return 0

    def get_idle_size(self) -> int:
        return 0


async def _slot_pool_harness(registered: Any) -> tuple[Any, Any, Any]:
    """Minimal deps/loop-scope pair for driving ``_maybe_open_slot_pool``.

    ``_maybe_open_slot_pool`` reads exactly two things off these: the
    LOOP scope's resolved cache (to find the registered
    ``asyncpg.Connection``, its activation signal) and
    ``deps._exit_stack`` (where it registers the pool's teardown). The
    third input - the DI registry the registration came through, where a
    factory's declared init hook lives - is passed separately by the
    tests that exercise it. Standing those up directly keeps the pins on
    the production open path without booting a whole worker.
    """
    stack = AsyncExitStack()
    await stack.__aenter__()
    deps = SimpleNamespace(slot_pool=None, slot_pool_factory=None, _exit_stack=stack)
    loop_scope = SimpleNamespace(
        resolved_cache=lambda: {asyncpg.Connection: registered},
    )
    return deps, loop_scope, stack


def _quiet_log() -> Any:
    return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
