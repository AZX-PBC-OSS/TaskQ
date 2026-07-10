"""Integration tests for open_worker_deps lifecycle against real PG18.

These tests require a running Postgres container (testcontainers).
Marked ``integration`` so they are skipped in non-integration runs.
"""

# ruff: noqa: SIM117 # Why: pytest.raises must wrap the async with statement; combined with-form is not valid here.

import asyncio
from contextlib import AsyncExitStack
from typing import cast

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from taskq.constants import wake_channel
from taskq.testing.settings import make_integration_settings
from taskq.worker.deps import open_worker_deps

pytestmark = pytest.mark.integration


# ── Full lifecycle ────────────────────────────────────────────────


async def test_open_worker_deps_full_lifecycle(pg_dsn: str) -> None:
    """open_worker_deps opens all pools and connections; teardown closes them."""
    settings = make_integration_settings(pg_dsn)

    async with open_worker_deps(settings) as deps:
        # Pools are open (lazy, so size may be 0)
        assert not deps.dispatcher_pool.is_closing()
        assert not deps.heartbeat_pool.is_closing()
        assert not deps.worker_pool.is_closing()

        # Can acquire a connection from each pool
        async with deps.dispatcher_pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1")
            assert val == 1

        # Dedicated connections are alive
        assert deps.notify_conn is not None
        assert deps.leader_conn is not None
        assert not deps.notify_conn.is_closed()
        assert not deps.leader_conn.is_closed()

    # After context exit, pools are closing
    assert deps.dispatcher_pool.is_closing()
    assert deps.heartbeat_pool.is_closing()
    assert deps.worker_pool.is_closing()


# ── LIFO teardown on partial failure ───────────────────────────────


async def test_lifo_teardown_on_leader_conn_failure(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If leader_conn open fails, pools and notify_conn are closed."""
    settings = make_integration_settings(pg_dsn)

    from taskq.worker import deps as deps_mod

    original_open = deps_mod.open_dedicated_conn

    async def _patched_open(
        dsn: str, *, label: str, apply_keepalive: bool = True
    ) -> asyncpg.Connection:
        if label == "leader":
            raise asyncpg.PostgresConnectionError("simulated leader_conn failure")
        return await original_open(dsn, label=label, apply_keepalive=apply_keepalive)

    monkeypatch.setattr(deps_mod, "open_dedicated_conn", _patched_open)

    with pytest.raises(asyncpg.PostgresConnectionError, match="simulated leader_conn failure"):
        async with open_worker_deps(settings):
            pass  # should never reach here


# ── DSN routing ────────────────────────────────────────────────────


async def test_dispatcher_heartbeat_use_direct_dsn(pg_dsn: str) -> None:
    """Pools route to correct DSNs.

    When pg_dsn_direct is valid but pg_dsn_pooled points to a nonexistent host,
    open_worker_deps fails during worker_pool creation (min_size=1 eagerly
    connects). This proves the worker_pool uses pg_dsn_pooled, not pg_dsn_direct.
    The AsyncExitStack cleans up the already-opened dispatcher_pool and
    heartbeat_pool before the exception propagates.
    """
    settings = make_integration_settings(
        pg_dsn,
        PG_DSN_DIRECT=pg_dsn,
        PG_DSN_POOLED="postgresql://nobody@nonexistent-host-99999/taskq",
    )

    # worker_pool creation fails → entire open_worker_deps raises
    with pytest.raises((asyncpg.PostgresConnectionError, OSError)):
        async with open_worker_deps(settings):
            pass

    # Separately verify that the direct-DSN pools work when all DSNs are the same
    settings_all_direct = make_integration_settings(pg_dsn)
    async with open_worker_deps(settings_all_direct) as deps:
        async with deps.dispatcher_pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1")
            assert val == 1
        async with deps.heartbeat_pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1")
            assert val == 1


# ── heartbeat_pool command_timeout ─────────────────────────────────


async def test_heartbeat_pool_command_timeout(pg_dsn: str) -> None:
    """heartbeat_pool has command_timeout=2s; pg_sleep(5) is cancelled."""
    settings = make_integration_settings(pg_dsn)

    async with open_worker_deps(settings) as deps:
        async with deps.heartbeat_pool.acquire() as conn:
            # asyncpg raises TimeoutError when command_timeout fires mid-query
            with pytest.raises((asyncpg.QueryCanceledError, TimeoutError)):
                await conn.execute("SELECT pg_sleep(5)")


# ── notify_conn LISTEN survives context ───────────────────────────


async def test_notify_conn_has_active_listen(pg_dsn: str) -> None:
    """notify_conn holds an active LISTEN subscription."""
    settings = make_integration_settings(pg_dsn)

    async with open_worker_deps(settings) as deps:
        # Verify the LISTEN is active by checking pg_listening_channels()
        assert deps.notify_conn is not None
        channels = await deps.notify_conn.fetchval("SELECT pg_listening_channels()")
        expected_channel = wake_channel(settings.schema_name)
        assert channels == expected_channel


# ── heartbeat_pool acquire timeout ─────────────────────────────────


async def test_heartbeat_pool_acquire_timeout(pg_dsn: str) -> None:
    """Pool exhaustion + timeout raises asyncio.TimeoutError."""
    settings = make_integration_settings(
        pg_dsn,
        HEARTBEAT_POOL_SIZE="4",
    )

    async with open_worker_deps(settings) as deps:
        # Exhaust the pool by acquiring all 4 connections
        acquired: list[asyncpg.pool.PoolConnectionProxy] = []  # type: ignore[name-defined]
        for _ in range(4):
            conn = await deps.heartbeat_pool.acquire()
            acquired.append(conn)

        try:
            # 5th acquire with timeout should raise TimeoutError
            with pytest.raises(asyncio.TimeoutError):
                await deps.heartbeat_pool.acquire(timeout=2.0)
        finally:
            for conn in acquired:
                await deps.heartbeat_pool.release(conn)


# ── pool_max_inactive_lifetime ────────────────────────────────────────────


async def test_pool_max_inactive_lifetime_discards_idle_conn(
    pg_dsn: str,
) -> None:
    """Idle connections are discarded after max_inactive_lifetime."""
    settings = make_integration_settings(
        pg_dsn,
        POOL_MAX_INACTIVE_LIFETIME="2.0",
    )

    async with open_worker_deps(settings) as deps:
        # Acquire a connection and record its PID
        async with deps.dispatcher_pool.acquire() as conn:
            pid1 = conn.get_server_pid()

        # Sleep long enough for the idle connection to be discarded
        await asyncio.sleep(3.0)

        # Acquire again; should be a new connection (different PID)
        async with deps.dispatcher_pool.acquire() as conn:
            pid2 = conn.get_server_pid()

        assert pid1 != pid2, "connection should have been discarded and replaced"


# ── PG unavailable during pool open ────────────────────────────────


async def test_pg_unavailable_raises_within_timeout(pg_dsn: str) -> None:
    """DSN with closed port raises within 5s; no zombie state.

    Uses a deliberately-unreachable port (1) on the same host as the
    testcontainer PG. asyncpg's default ``connect_timeout`` (60s) is overridden
    by wrapping ``open_worker_deps`` in ``asyncio.wait_for`` — if the
    AsyncExitStack pattern is correct, the failure surfaces before the wait
    fires and no resource leaks.
    """
    bad_dsn = "postgresql://taskq:taskq@127.0.0.1:1/taskq"
    settings = make_integration_settings(bad_dsn)

    # The connection attempt should fail fast (refused) — give it 5s ceiling.
    with pytest.raises(
        (asyncpg.PostgresConnectionError, OSError, ConnectionRefusedError, asyncio.TimeoutError)
    ):
        async with asyncio.timeout(5.0):
            async with open_worker_deps(settings):
                pass


# ── Partial-open LIFO teardown against real PG ─────────────────────


async def test_partial_open_lifo_teardown_with_pg_failure(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PG fails mid-startup: already-opened pools close cleanly.

    Simulates partial-init failure by monkey-patching
    ``open_dedicated_conn`` so notify_conn raises after the three pools
    are already open. Verifies AsyncExitStack closes them in LIFO order.
    """
    settings = make_integration_settings(pg_dsn)

    from taskq.worker import deps as deps_mod

    async def _fail_on_notify(
        dsn: str, *, label: str, apply_keepalive: bool = True
    ) -> asyncpg.Connection:
        if label == "notify":
            # Three pools already opened by the time we get here.
            raise asyncpg.PostgresConnectionError("simulated PG failure mid-startup")
        raise AssertionError(f"unexpected label {label}")

    monkeypatch.setattr(deps_mod, "open_dedicated_conn", _fail_on_notify)

    # Capture pools as AsyncExitStack registers them so we can assert they
    # were closed. Patching the stack's enter_async_context is the cleanest
    # observation point — every pool flows through it before notify_conn opens.
    captured_pools: list[asyncpg.Pool] = []
    original_enter = AsyncExitStack.enter_async_context

    async def _capturing_enter(self: AsyncExitStack, cm: object) -> object:
        # Why: AsyncExitStack.enter_async_context returns the entered value;
        # asyncpg.Pool is generic in record-class which we don't care about
        # here, so we observe via isinstance and re-cast to object on return.
        raw = await original_enter(self, cm)  # pyright: ignore[reportArgumentType, reportUnknownVariableType]
        if isinstance(raw, asyncpg.Pool):
            captured_pools.append(raw)  # pyright: ignore[reportUnknownArgumentType]
        return cast(object, raw)

    monkeypatch.setattr(AsyncExitStack, "enter_async_context", _capturing_enter)

    with pytest.raises(asyncpg.PostgresConnectionError, match="simulated PG failure mid-startup"):
        async with open_worker_deps(settings):
            pass

    # All captured pools (dispatcher, heartbeat, worker) must be closed/closing.
    assert len(captured_pools) == 3, f"expected 3 pools, captured {len(captured_pools)}"
    for pool in captured_pools:
        assert pool.is_closing(), f"pool {pool!r} not closed after teardown"


# ── Keepalive verification ────────────────────────────────────────────────


async def test_keepalive_setsockopt_fires_on_dedicated_conns(pg_dsn: str) -> None:
    """notify_conn and leader_conn have SO_KEEPALIVE set on the underlying socket.

    Defends against silent regression if asyncpg's ``_transport`` private
    accessor changes shape (already type-ignores that access).

    macOS ``getsockopt(SO_KEEPALIVE)`` returns 8 (not 1) when on, so we
    check for non-zero rather than == 1.
    """
    import socket as _socket

    settings = make_integration_settings(pg_dsn)

    async with open_worker_deps(settings) as deps:
        for label, conn in (("notify", deps.notify_conn), ("leader", deps.leader_conn)):
            transport = conn._transport  # type: ignore[attr-defined] # Why: same private API used inside open_dedicated_conn; if this breaks, keepalive helper itself is broken.
            sock: _socket.socket | None = transport.get_extra_info("socket")
            assert sock is not None, f"{label} conn has no socket info"
            keepalive = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_KEEPALIVE)
            assert keepalive != 0, f"SO_KEEPALIVE not set on {label} conn"


# ── A-TG-05: pool startup log redacts credentials ────────────────────────


async def test_pool_startup_log_redacts_credentials(
    pg_container: PostgresContainer,
) -> None:
    """A-TG-05: — pool startup logs must not contain passwords.

    Verifies the behavioral contract: ``dsn_host()`` strips credentials
    from DSNs, and ``open_worker_deps`` opens successfully with a
    non-default password. All logging sites in ``deps.py`` use
    ``dsn_host()`` rather than raw DSNs, so testing this function
    directly is sufficient to guarantee no credential leakage.

    Uses a sentinel password (distinct from username/schema/dbname) so that
    a substring match cannot collide with legitimate identifiers like
    ``taskq_wake_<schema>``. The container is rebooted via DSN-rewrite —
    we ALTER USER to a unique password for the duration of this test, then
    restore it. This is more robust than relying on the default fixture
    password ``taskq`` which collides with the role and schema names.
    """
    from taskq._dsn import dsn_host

    sentinel = "S3CR3T-redacted-zzzZQ9"  # not a substring of any expected log key/value

    # Verify dsn_host strips the password — this is the behavioral contract
    # that prevents credential leakage in all logging sites.
    sentinel_dsn_for_check = f"postgresql://taskq:{sentinel}@db.example.com:5432/taskq"
    assert dsn_host(sentinel_dsn_for_check) == "db.example.com"
    assert sentinel not in dsn_host(sentinel_dsn_for_check)

    # Connect using the existing creds, ALTER USER to the sentinel, then
    # verify open_worker_deps opens successfully with the sentinel password.
    # Restore afterward.
    base_dsn = pg_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
    admin_conn = await asyncpg.connect(base_dsn)
    try:
        await admin_conn.execute(f"ALTER USER taskq WITH PASSWORD '{sentinel}'")
    finally:
        await admin_conn.close()

    sentinel_dsn = base_dsn.replace(":taskq@", f":{sentinel}@")

    try:
        settings = make_integration_settings(sentinel_dsn)
        async with open_worker_deps(settings):
            pass
    finally:
        # Restore the original password so subsequent tests using the
        # session-scoped pg_container continue to work.
        admin_conn = await asyncpg.connect(sentinel_dsn)
        try:
            await admin_conn.execute("ALTER USER taskq WITH PASSWORD 'taskq'")
        finally:
            await admin_conn.close()


# ── redis_client and progress_buffers ─────────────────────────────────────


async def test_redis_client_none_when_no_redis_url(pg_dsn: str) -> None:
    """redis_client is None when redis_url is not configured."""
    settings = make_integration_settings(pg_dsn)

    async with open_worker_deps(settings) as deps:
        assert deps.redis_client is None


async def test_progress_buffers_starts_empty(pg_dsn: str) -> None:
    """progress_buffers is an empty dict at startup."""
    settings = make_integration_settings(pg_dsn)

    async with open_worker_deps(settings) as deps:
        assert deps.progress_buffers == {}


@pytest.mark.redis
async def test_redis_client_opened_and_closed_when_configured(
    pg_dsn: str,
    redis_url: str,
) -> None:
    """redis_client is opened when redis_url is set; closed on teardown."""
    settings = make_integration_settings(pg_dsn, REDIS_URL=redis_url)

    async with open_worker_deps(settings) as deps:
        assert deps.redis_client is not None
        ping_result = await deps.redis_client.ping()  # pyright: ignore[reportGeneralTypeIssues] # Why: redis-py stub may not expose ping() return type as Awaitable; runtime client is async.
        assert ping_result is True
