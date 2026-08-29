"""Integration tests: rotating Postgres credentials refresh per connection.

The regression these guard against: ``make_pg_pool_factory`` /
``make_dedicated_conn_factory`` used to resolve the credential once, at factory
invocation, and pass it to asyncpg as a fixed ``password=`` string. asyncpg
reuses that string for every physical connection it opens afterwards, and
Postgres authenticates at **connect time only** — so with a finite-lifetime
token (Entra ID, AWS IAM RDS) the pool is healthy at deploy and then, roughly
one token lifetime later, every NEW physical connection fails to authenticate
while the already-open ones keep working. With
``max_inactive_connection_lifetime`` (300s by default) recycling idle
connections continuously, the pool degrades to fully unusable. It deploys green
and dies hours later.

These tests run against real Postgres and drive real authentication, so they
distinguish "the credential callable is consulted per physical connection" from
"the first connection worked" — the latter passes against the bug.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from taskq.auth import PgCredential, make_dedicated_conn_factory, make_pg_pool_factory

pytestmark = pytest.mark.integration

# The pg_container fixture's static credential. sslmode=disable because the
# factory injects sslmode=require only when the DSN carries no explicit
# sslmode, and the test container serves no TLS.
_CORRECT_PASSWORD = "taskq"


class _RotatingPgProvider:
    """Provider whose credential the test mutates mid-flight.

    Stands in for a token issuer: ``password`` is what the next
    ``get_pg_credential`` call will hand out, so a test can simulate a token
    rotating (or expiring into an invalid value) while a pool is alive.
    """

    def __init__(self, password: str) -> None:
        self.password = password
        self.calls = 0

    async def get_pg_credential(self) -> PgCredential:
        self.calls += 1
        return PgCredential(password=self.password)


async def test_provider_is_called_for_every_physical_connection(pg_dsn: str) -> None:
    """Pool growth and idle recycling each re-fetch the credential.

    Against the old one-shot behaviour the provider was called exactly once
    (``calls == 1``) no matter how many physical connections were opened.
    """
    provider = _RotatingPgProvider(_CORRECT_PASSWORD)
    factory = make_pg_pool_factory(
        f"{pg_dsn}?sslmode=disable",
        provider,
        min_size=1,
        max_size=2,
        max_inactive_connection_lifetime=0.5,
    )

    pool = await factory()
    try:
        # One fetch resolves `user=` at construction; the pool then opens
        # min_size=1 physical connections, each of which calls the callable.
        after_construction = provider.calls
        assert after_construction >= 2, (
            "expected a construction fetch plus one per initial physical connection"
        )

        # Pool growth: two concurrent acquires force a second physical
        # connection, which must fetch its own credential.
        async with pool.acquire() as c1, pool.acquire() as c2:
            pid1 = await c1.fetchval("SELECT pg_backend_pid()")
            pid2 = await c2.fetchval("SELECT pg_backend_pid()")
            assert pid1 != pid2
        after_growth = provider.calls
        assert after_growth > after_construction

        # Idle recycling: both connections are terminated after 0.5s, so the
        # next acquire opens a NEW physical connection and re-fetches again.
        await asyncio.sleep(1.0)
        async with pool.acquire() as c3:
            pid3 = await c3.fetchval("SELECT pg_backend_pid()")
            assert pid3 not in {pid1, pid2}
        assert provider.calls > after_growth
    finally:
        await pool.close()


async def test_rotated_credential_is_used_by_new_connections(pg_dsn: str) -> None:
    """A credential that changes while the pool is alive reaches new connections.

    This is the behavioural core of the fix, in three moves:

    1. the pool is built and works with the correct credential;
    2. the provider starts handing out an INVALID credential (a token that has
       expired and been replaced by a bad value) and, once the idle connection
       is recycled, the next physical connection fails to authenticate. Under
       the old code this step SUCCEEDED, because the good password captured at
       construction was reused forever — that success is precisely the bug;
    3. the provider is restored and the pool recovers on its own, with no
       rebuild and no SIGHUP.
    """
    provider = _RotatingPgProvider(_CORRECT_PASSWORD)
    factory = make_pg_pool_factory(
        f"{pg_dsn}?sslmode=disable",
        provider,
        min_size=1,
        max_size=1,
        max_inactive_connection_lifetime=0.5,
    )

    pool = await factory()
    try:
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT 1") == 1

        # The issued credential rotates to something the server will reject.
        provider.password = "expired-token-replaced-by-garbage"  # deliberately invalid

        # Let the idle connection be recycled so the next acquire must open a
        # new physical connection, which authenticates with the CURRENT
        # credential rather than the one captured at pool construction.
        await asyncio.sleep(1.0)
        with pytest.raises(asyncpg.InvalidPasswordError):
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")

        # The token is renewed. New connections pick it up with no pool
        # rebuild — the whole point of per-connection refresh.
        provider.password = _CORRECT_PASSWORD
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT 1") == 1
    finally:
        await pool.close()


async def test_dedicated_conn_factory_refreshes_on_each_open(pg_dsn: str) -> None:
    """The LISTEN / advisory-lock connection path works end-to-end unchanged.

    Honest scope: this one does NOT discriminate the fix — it passes against
    the old code too, because a dedicated connection is opened once per factory
    invocation and the old code already re-fetched per invocation. It is here
    as the end-to-end guard that handing asyncpg a *callable* ``password=``
    still authenticates against a real server on this path (a callable asyncpg
    mishandled would fail here). The discriminating proof that the callable is
    re-consulted per physical connection is
    ``test_dedicated_conn_password_is_callable_refetched_per_connection`` in
    tests/test_auth.py.
    """
    provider = _RotatingPgProvider(_CORRECT_PASSWORD)
    factory = make_dedicated_conn_factory(f"{pg_dsn}?sslmode=disable", provider)

    conn = await factory()
    try:
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()
    after_first = provider.calls

    # Rotation to an invalid credential: re-opening now fails at the server,
    # proving the second open did not reuse the first open's password.
    provider.password = "expired-token-replaced-by-garbage"  # deliberately invalid
    with pytest.raises(asyncpg.InvalidPasswordError):
        await factory()
    assert provider.calls > after_first

    provider.password = _CORRECT_PASSWORD
    conn2 = await factory()
    try:
        assert await conn2.fetchval("SELECT 1") == 1
    finally:
        await conn2.close()


async def test_provider_failure_surfaces_as_connection_error(pg_dsn: str) -> None:
    """A token fetch that fails mid-connect surfaces, never hangs or falls back.

    The connection attempt must raise the provider's own error rather than
    retrying silently or connecting unauthenticated.
    """

    class _FailAfterFirstProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def get_pg_credential(self) -> PgCredential:
            self.calls += 1
            if self.calls == 1:
                # The construction fetch, which resolves `user=`.
                return PgCredential(password=_CORRECT_PASSWORD)
            raise TimeoutError("token endpoint unreachable")

    provider = _FailAfterFirstProvider()
    factory = make_dedicated_conn_factory(f"{pg_dsn}?sslmode=disable", provider)

    with pytest.raises(TimeoutError, match="token endpoint unreachable"):
        await factory()
