"""The per-slot transaction pool: sizing, supply, and readiness probe.

Three behaviours this file pins, each at the level that can actually
see its failure mode:

* **Factory sizing** (unit, spied ``asyncpg.create_pool`` /
  ``make_pg_pool_factory`` — the ``tests/test_auth.py`` precedent): the
  pool the worker opens is fully warmed at ``max_concurrency + 1`` —
  one connection per consumer slot plus the readiness reserve — on the
  direct DSN with the dispatcher command timeout. Under-sizing is the
  defect class where a fully-utilised worker's readiness ping times
  out waiting on a slot-held connection and reports health as unready.
* **Supply rule** (integration, real PG): ``max_concurrency + 1``
  concurrent acquires all succeed — the pool genuinely holds a
  connection per slot plus the reserve, not just a configured number.
* **Single-flight readiness** (unit): concurrent probes share ONE
  in-flight slot-pool ping, which is what makes the one-connection
  reserve sufficient; and a failing shared probe fails every waiter
  that joined it.
* **Registered-connection setup** (integration, real PG): connections
  the pool hands out carry the session state the application configured
  on the LOOP-registered connection. Losing it turns a concurrency knob
  into a silent behaviour change. The read off the registered
  connection (``_registered_session_state``) and the factory's
  ``session_settings`` forwarding are pinned at unit level below: every
  read failure must warn and leave the setting uninherited, never skip
  it quietly.
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import structlog.testing

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
    timeout — the fully-warmed shape whose absence puts connection
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
    # Same statement-cache treatment as the provider-backed branch —
    # resolved from settings (the defaults here), so the DSN-built pool
    # never drifts from the pool family's cache behaviour.
    assert call_kwargs["statement_cache_size"] == settings.statement_cache_size
    assert call_kwargs["max_cached_statement_lifetime"] == settings.max_cached_statement_lifetime


async def test_slot_pool_factory_is_provider_backed_when_given_provider() -> None:
    """With a credential provider the factory comes from
    make_pg_pool_factory — the documented managed-identity path — sized
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
        # Same statement-cache treatment as the DSN-built branch — resolved
        # from settings (the defaults here), so switching authentication
        # never switches cache behaviour.
        statement_cache_size=settings.statement_cache_size,
        max_cached_statement_lifetime=settings.max_cached_statement_lifetime,
    )


async def test_slot_pool_factory_carries_session_settings_onto_dsn_built_pool() -> None:
    """``session_settings`` reach ``asyncpg.create_pool`` as
    ``server_settings`` — the channel that puts the registered
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


def test_slot_pool_factory_forwards_session_settings_to_the_provider_backed_factory() -> None:
    """The managed-identity branch forwards the same mapping to
    ``make_pg_pool_factory`` — switching authentication must not switch
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
    """No mapping and an empty mapping behave alike: the worker with
    nothing to inherit builds exactly the pool it always built — the
    DSN branch passes the driver's own default, the provider branch
    adds no kwarg."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")

    for session in (None, {}):
        create_pool = AsyncMock(return_value=MagicMock())
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("taskq.worker._bootstrap.asyncpg.create_pool", create_pool)
            factory = _slot_pool_factory(settings, None, session)
            await factory()
        assert create_pool.call_args.kwargs["server_settings"] is None

        provider = MagicMock()
        make_pg_pool_factory = MagicMock()
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("taskq.worker._bootstrap.make_pg_pool_factory", make_pg_pool_factory)
            _slot_pool_factory(settings, provider, session)
        assert "server_settings" not in make_pg_pool_factory.call_args.kwargs


# ── Registered-connection session state read ─────────────────────────────
#
# Unit pins for the read half of the carry-over: what the live session
# holds, the pool inherits; what cannot be read is warned about and left
# at the server default, never skipped quietly (a silent no-op here is
# indistinguishable from the defect the read exists to eliminate).


async def test_registered_session_state_reads_the_live_values() -> None:
    """Both inherited settings come back as read from the live session —
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
    half — the slot pool falls back to the server default for exactly
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
    warning — the boot harnesses register such stand-ins, and breaking
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
            # All four holds are live at once — supply, not configuration.
            for conn in conns:
                await conn.execute("SELECT 1")
        finally:
            for conn in conns:
                await pool.release(conn)
    finally:
        await pool.close()


# ── Single-flight readiness probe ────────────────────────────────────────


class _SlowPool:
    """Slot-pool stand-in whose acquire is slow enough for callers to
    overlap, counting every acquire — the single-flight discriminator.

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
        "concurrent readiness requests must join one in-flight probe — "
        "parallel pings contend for the single readiness-reserve "
        "connection and report a healthy, busy worker as unready"
    )
    assert results == [(True, None), (True, None), (True, None)]


async def test_probe_state_is_per_worker_not_process_global() -> None:
    """Two workers can share a process; one worker's in-flight probe must
    never answer another worker's readiness request — worker B would be
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
        "worker B's readiness request was answered by worker A's probe — "
        "probe state has leaked across workers"
    )
    assert results == [(True, None), (True, None)]


async def test_wedged_probe_degrades_to_a_failed_ping_not_a_hang() -> None:
    """A probe task left over from a torn-down event loop can never
    complete; joining it must be bounded by one ping budget and report
    unready, never hang readiness (the constitution's every-wait-bounded
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
    it — the next waiter still joins the SAME in-flight ping, which is
    what keeps the one-connection reserve sufficient. A shield-less
    implementation fails here: the first cancellation kills the probe
    and the survivor pays a second acquire (and the reserve contention
    the single flight exists to prevent)."""
    pool = _SlowPool(delay=0.1)
    deps = _deps_with_pool(pool)

    waiter_to_cancel = asyncio.create_task(_ping_slot_pool(deps))
    # Deterministic sequencing, no timing sleeps: wait for the probe to
    # have STARTED (the pool fires `acquired` inside acquire()), then
    # one scheduler yield lets the survivor run to its first await —
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
        "the cancelled waiter took the shared probe with it — the "
        "survivor had to start a second acquire"
    )
    assert survivor_result == (True, None)


async def test_sequential_probes_each_ping_freshly() -> None:
    """A probe arriving after the previous one completed starts a new
    one — single-flight coalesces concurrent probes, never serves a
    stale result."""
    pool = _SlowPool(delay=0.0)
    deps = _deps_with_pool(pool)

    await _ping_slot_pool(deps)
    await _ping_slot_pool(deps)

    assert pool.acquire_calls == 2


# ── Registered-connection setup carries onto slot connections ────────────
#
# At max_concurrency == 1 the actor receives the LOOP-registered
# connection itself, so whatever the application configured on it — a
# ``set_type_codec`` registration, an ``init``/``setup`` callback, a
# ``SET ROLE``, a ``search_path`` or any other server setting — is
# present by construction. The moment max_concurrency rises above 1 the
# worker hands actors connections out of its own per-slot pool instead.
# Unless that pool builds its connections with the same setup, a
# deployment's behaviour changes silently with a concurrency knob:
# RLS-driving roles vanish, a custom search_path resolves different
# tables, and a domain type the application registered a codec for comes
# back as a raw string. Nothing fails loudly — the actor just reads and
# writes the wrong thing. These pins are the reason the per-slot pool is
# not allowed to be a bare direct-DSN pool.


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
            f"connection ({registered_path!r}) — raising max_concurrency "
            "silently repointed every actor's queries at another schema"
        )
    finally:
        await stack.aclose()
        await registered.close()


@pytest.mark.integration
async def test_slot_connections_carry_the_registered_connections_type_codecs(
    module_pg_schema: Any,
) -> None:
    """A codec registered on the LOOP-scope connection decodes the same
    values on the connections actors actually receive per slot.

    A codec is the setup an application is least likely to notice losing:
    the query still succeeds, it just returns the driver's default
    representation. An actor written against the decoded form then
    mis-parses every row, on exactly the workers whose concurrency was
    raised.
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

        async with deps.slot_pool.acquire() as slot_conn:
            decoded = await slot_conn.fetchval("SELECT '{\"a\": 1}'::json")

        assert decoded == {"decoded_by": "registered-codec"}, (
            "the registered connection's json codec is absent on the slot "
            f"connection (got {decoded!r}) — the actor receives the driver's "
            "default representation instead of the application's"
        )
    finally:
        await stack.aclose()
        await registered.close()


async def _slot_pool_harness(registered: Any) -> tuple[Any, Any, Any]:
    """Minimal deps/loop-scope pair for driving ``_maybe_open_slot_pool``.

    ``_maybe_open_slot_pool`` reads exactly two things: the LOOP scope's
    resolved cache (to find the registered ``asyncpg.Connection``, its
    activation signal) and ``deps._exit_stack`` (where it registers the
    pool's teardown). Standing those up directly keeps the pins on the
    production open path without booting a whole worker.
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
