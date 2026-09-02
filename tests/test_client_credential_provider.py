"""The client-side credential-provider path: ``pool_factory`` / ``pg_provider``.

The gap these guard: :class:`taskq.TaskQ` accepted only ``dsn=`` or ``pool=``,
so a deployment on rotating credentials (Entra ID, AWS IAM RDS, Vault) had no
way to swap the pool once its token expired. Real consumers reached into
``tq._pool`` / ``tq._client._backend._deps`` from a background refresh task —
the exact private surface these tests replace.

The proof that matters is the last one: a client built from a factory survives
a credential rotation with **no private attribute access anywhere in the
test**, and the pool it hands to every subsystem (backend deps, ActorsClient,
LISTEN transport) is the new one.
"""

from __future__ import annotations

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import TaskQ, actor
from taskq._ids import new_base62
from taskq.auth import PgCredential, ensure_sslmode_require, make_pg_pool_factory
from taskq.backend._protocol import JobFilter
from taskq.migrate import apply_pending

pytestmark = pytest.mark.integration

_SCHEMA_LABEL = f"tccp_{new_base62()}".lower()

# The pg_container fixture's static credential. The container serves no TLS,
# so the DSN carries an explicit sslmode=disable that the auth layer must not
# override.
_CONTAINER_PASSWORD = "taskq"


class _Payload(BaseModel):
    value: int = 1


@actor(name="tq_client_cred_actor")
async def _cred_actor(_payload: _Payload) -> None:
    pass


class _StaticProvider:
    """Minimal :class:`taskq.PgCredentialProvider` handing out a fixed password.

    ``calls`` counts fetches so a test can assert the credential path is
    genuinely consulted rather than the DSN's own userinfo being used.
    """

    def __init__(self, password: str) -> None:
        self.password = password
        self.calls = 0

    async def get_pg_credential(self) -> PgCredential:
        self.calls += 1
        return PgCredential(password=self.password)


async def _prepare_schema(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA_LABEL}" CASCADE')
        await apply_pending(conn, schema=_SCHEMA_LABEL)
    finally:
        await conn.close()


def _disable_ssl(dsn: str) -> str:
    sep = "&" if "?" in dsn else "?"
    return f"{dsn}{sep}sslmode=disable"


# ── Constructor validation ────────────────────────────────────────────


def test_pool_factory_and_pool_are_mutually_exclusive() -> None:
    async def _factory() -> asyncpg.Pool:  # pragma: no cover - never invoked
        raise AssertionError

    with pytest.raises(ValueError, match="not both"):
        TaskQ(pool=object(), pool_factory=_factory)  # type: ignore[arg-type]  # Why: the guard fires before the pool is ever used.


def test_pool_factory_and_dsn_are_mutually_exclusive() -> None:
    async def _factory() -> asyncpg.Pool:  # pragma: no cover - never invoked
        raise AssertionError

    with pytest.raises(ValueError, match="not both"):
        TaskQ(dsn="postgresql://x/y", pool_factory=_factory)


def test_pg_provider_requires_a_dsn() -> None:
    with pytest.raises(ValueError, match="pg_provider"):
        TaskQ(pool_factory=lambda: None, pg_provider=_StaticProvider("x"))  # type: ignore[arg-type]  # Why: the guard fires before either is used.


def test_pool_factory_satisfies_the_dsn_or_pool_requirement() -> None:
    """``pool_factory`` alone is a complete construction — no dsn/pool needed."""

    async def _factory() -> asyncpg.Pool:  # pragma: no cover - never invoked
        raise AssertionError

    TaskQ(pool_factory=_factory)


# ── Ownership ─────────────────────────────────────────────────────────


async def test_factory_built_pool_is_taskq_owned(pg_dsn: str) -> None:
    """A pool TaskQ built from a factory is closed by ``close()``."""
    await _prepare_schema(pg_dsn)
    dsn = _disable_ssl(pg_dsn)
    built: list[asyncpg.Pool] = []

    async def _factory() -> asyncpg.Pool:
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
        assert pool is not None
        built.append(pool)
        return pool

    tq = TaskQ(pool_factory=_factory, schema=_SCHEMA_LABEL)
    await tq.open()
    await tq.list(JobFilter())
    await tq.close()

    assert len(built) == 1
    assert built[0].is_closing()


async def test_caller_supplied_pool_is_still_never_closed(pg_dsn: str) -> None:
    """The existing ownership rule is unchanged by the new path."""
    await _prepare_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=_disable_ssl(pg_dsn), min_size=1, max_size=2)
    assert pool is not None
    try:
        tq = TaskQ(pool=pool, schema=_SCHEMA_LABEL)
        await tq.open()
        await tq.close()
        assert not pool.is_closing()
    finally:
        await pool.close()


# ── pg_provider sugar ─────────────────────────────────────────────────


async def test_pg_provider_authenticates_through_the_credential_path(pg_dsn: str) -> None:
    """``TaskQ(dsn=..., pg_provider=...)`` really authenticates with the provider.

    The DSN is stripped of its password, so the client can only connect if the
    provider's credential reached asyncpg.
    """
    await _prepare_schema(pg_dsn)
    stripped = _disable_ssl(pg_dsn).replace(f":{_CONTAINER_PASSWORD}@", "@")
    provider = _StaticProvider(_CONTAINER_PASSWORD)

    async with TaskQ(dsn=stripped, pg_provider=provider, schema=_SCHEMA_LABEL) as tq:
        await tq.list(JobFilter())

    assert provider.calls >= 1


# ── Rotation without private access ───────────────────────────────────


async def test_reload_credentials_swaps_the_pool_everywhere(pg_dsn: str) -> None:
    """The headline proof: rotate the pool through the public API only.

    After ``reload_credentials()`` the client must keep working, the OLD pool
    must be closed, and every subsystem that holds a pool reference (backend
    deps for enqueue/list, ActorsClient) must be on the NEW one. No private
    attribute is touched anywhere in this test — that is the point.
    """
    await _prepare_schema(pg_dsn)
    stripped = _disable_ssl(pg_dsn).replace(f":{_CONTAINER_PASSWORD}@", "@")
    provider = _StaticProvider(_CONTAINER_PASSWORD)
    factory = make_pg_pool_factory(stripped, provider, min_size=1, max_size=2)

    built: list[asyncpg.Pool] = []

    async def _tracking_factory() -> asyncpg.Pool:
        pool = await factory()
        built.append(pool)
        return pool

    async with TaskQ(pool_factory=_tracking_factory, schema=_SCHEMA_LABEL) as tq:
        handle = await tq.enqueue(_cred_actor, _Payload(value=1))
        assert handle.job_id is not None

        await tq.reload_credentials()

        assert len(built) == 2
        assert built[0].is_closing(), "the old pool must be closed after a reload"
        assert not built[1].is_closing()

        # Every public surface still works, on the new pool.
        after = await tq.enqueue(_cred_actor, _Payload(value=2))
        assert after.job_id is not None
        assert await tq.get(after.job_id) is not None
        await tq.actors.list()

    assert built[1].is_closing()


async def test_reload_credentials_rejects_a_caller_owned_pool(pg_dsn: str) -> None:
    """Rotation is refused when TaskQ has no factory — the pool is the caller's."""
    await _prepare_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=_disable_ssl(pg_dsn), min_size=1, max_size=2)
    assert pool is not None
    try:
        async with TaskQ(pool=pool, schema=_SCHEMA_LABEL) as tq:
            with pytest.raises(RuntimeError, match="pool_factory"):
                await tq.reload_credentials()
        assert not pool.is_closing()
    finally:
        await pool.close()


async def test_reload_credentials_requires_an_open_client(pg_dsn: str) -> None:
    async def _factory() -> asyncpg.Pool:  # pragma: no cover - never invoked
        raise AssertionError

    tq = TaskQ(pool_factory=_factory, schema=_SCHEMA_LABEL)
    with pytest.raises(RuntimeError, match="not open"):
        await tq.reload_credentials()


async def test_failed_reload_keeps_the_working_pool(pg_dsn: str) -> None:
    """A provider outage must not leave the client without a pool.

    A failed rotation raises, but the still-valid pool keeps serving — the
    alternative (a half-swapped client) turns a transient token-endpoint blip
    into an outage.
    """
    await _prepare_schema(pg_dsn)
    dsn = _disable_ssl(pg_dsn)
    calls = 0

    async def _factory() -> asyncpg.Pool:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("token endpoint down")
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
        assert pool is not None
        return pool

    async with TaskQ(pool_factory=_factory, schema=_SCHEMA_LABEL) as tq:
        with pytest.raises(RuntimeError, match="token endpoint down"):
            await tq.reload_credentials()
        await tq.list(JobFilter())


# ── ensure_sslmode_require is public ──────────────────────────────────


def test_ensure_sslmode_require_adds_when_missing() -> None:
    assert "sslmode=require" in ensure_sslmode_require("postgresql://u:p@h/db")


def test_ensure_sslmode_require_never_downgrades_verify_full() -> None:
    dsn = "postgresql://u:p@h/db?sslmode=verify-full"
    assert ensure_sslmode_require(dsn) == dsn


def test_ensure_sslmode_require_is_exported() -> None:
    import taskq

    assert taskq.ensure_sslmode_require is ensure_sslmode_require
    assert "ensure_sslmode_require" in taskq.__all__
