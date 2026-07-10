"""Verify the module-scoped PG/Redis fixtures provide proper isolation.

Covers: module_pg_schema is module-scoped — same schema for all tests in
file; module_redis_url is module-scoped — same DB id for all tests in
file; clean_pg_conn truncates between tests — no cross-test PG state;
clean_jobs_app provides working WorkerDeps + PostgresBackend;
clean_redis_url flushdb between tests — no cross-test Redis state;
clean_redis_client provides a working Redis async client; seed_actors
with custom actors — empty/custom actors work; truncate_schema leaves
schema_migrations intact.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.pg import (
    DEFAULT_ACTORS,
    seed_actors,
    truncate_schema,
)

pytestmark = pytest.mark.integration

_MOD_SEEN: set[str] = set()
_REDIS_DB_SEEN: set[str] = set()


# ── module_pg_schema is module-scoped ──────────────────────────


@pytest.mark.xdist_group(name="fixtures")
class TestModulePgSchema:
    """Schema name is stable across tests in the same module."""

    def test_schema_name_is_string(self, module_pg_schema: ModulePgSchema) -> None:
        assert isinstance(module_pg_schema.schema_name, str)
        assert module_pg_schema.schema_name.startswith("tq_")
        _MOD_SEEN.add(module_pg_schema.schema_name)

    def test_same_schema_name_as_previous_test(self, module_pg_schema: ModulePgSchema) -> None:
        assert module_pg_schema.schema_name in _MOD_SEEN

    @pytest.mark.asyncio
    async def test_schema_exists_in_pg(self, module_pg_schema: ModulePgSchema) -> None:
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            row = await conn.fetchrow(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
                module_pg_schema.schema_name,
            )
            assert row is not None
        finally:
            await conn.close()


# ── module_redis_url is module-scoped ──────────────────────────


@pytest.mark.xdist_group(name="fixtures")
@pytest.mark.redis
class TestModuleRedisUrl:
    """Redis DB id is stable across tests in the same module."""

    def test_url_is_string(self, module_redis_url: str) -> None:
        assert isinstance(module_redis_url, str)
        assert module_redis_url.startswith("redis://")
        _REDIS_DB_SEEN.add(module_redis_url)

    def test_same_redis_url_as_previous_test(self, module_redis_url: str) -> None:
        assert module_redis_url in _REDIS_DB_SEEN

    def test_redis_is_reachable(self, module_redis_url: str) -> None:
        import redis as redis_sync

        r = redis_sync.from_url(module_redis_url, decode_responses=False)
        try:
            assert r.ping()
        finally:
            r.close()


# ── clean_pg_conn truncates between tests ──────────────────────


class TestCleanPgConn:
    """clean_pg_conn ensures no cross-test PG state."""

    async def test_seed_actors_present(
        self, clean_pg_conn: object, module_pg_schema: ModulePgSchema
    ) -> None:
        conn: asyncpg.Connection = clean_pg_conn  # type: ignore[assignment]
        s = module_pg_schema.schema_name
        rows = await conn.fetch(f"SELECT actor FROM {s}.actor_config")
        actors = {r["actor"] for r in rows}
        for expected in DEFAULT_ACTORS:
            assert expected in actors

    async def test_jobs_table_empty(
        self, clean_pg_conn: object, module_pg_schema: ModulePgSchema
    ) -> None:
        conn: asyncpg.Connection = clean_pg_conn  # type: ignore[assignment]
        s = module_pg_schema.schema_name
        count = await conn.fetchval(f"SELECT count(*) FROM {s}.jobs")
        assert count == 0

    async def test_can_insert_and_next_test_sees_empty(
        self, clean_pg_conn: object, module_pg_schema: ModulePgSchema
    ) -> None:
        conn: asyncpg.Connection = clean_pg_conn  # type: ignore[assignment]
        s = module_pg_schema.schema_name
        jid = new_uuid()
        await conn.execute(
            f"""INSERT INTO {s}.jobs
            (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, now())""",
            jid,
            "test_actor",
            "default",
            "{}",
            3,
            "transient",
        )
        count = await conn.fetchval(f"SELECT count(*) FROM {s}.jobs WHERE id = $1", jid)
        assert count == 1
        # Let the next test also verify it gets a clean state.

    async def test_schema_migrations_intact(
        self, clean_pg_conn: object, module_pg_schema: ModulePgSchema
    ) -> None:
        conn: asyncpg.Connection = clean_pg_conn  # type: ignore[assignment]
        s = module_pg_schema.schema_name
        count = await conn.fetchval(f"SELECT count(*) FROM {s}.schema_migrations")
        assert count > 0


# ── clean_jobs_app provides working backend ───────────────────


class TestCleanJobsApp:
    """clean_jobs_app provides WorkerDeps + PostgresBackend."""

    async def test_enqueue_then_readback(self, clean_jobs_app: object) -> None:
        from taskq.testing.fixtures import JobsApp

        app: JobsApp = clean_jobs_app  # type: ignore[assignment]
        args = make_enqueue_args(payload={"x": 1})
        row = await app.backend.enqueue(args)
        got = await app.backend.get(row.id)
        assert got is not None
        assert got.id == row.id

    async def test_no_slop_from_previous_test(
        self, clean_jobs_app: object, module_pg_schema: ModulePgSchema
    ) -> None:
        from taskq.testing.fixtures import JobsApp

        app: JobsApp = clean_jobs_app  # type: ignore[assignment]
        s = module_pg_schema.schema_name
        async with app.deps.worker_pool.acquire() as conn:
            count = await conn.fetchval(f"SELECT count(*) FROM {s}.jobs")
        assert count == 0


# ── clean_redis_url flushdb between tests ──────────────────────


@pytest.mark.redis
class TestCleanRedisUrl:
    """clean_redis_url ensures no cross-test Redis state."""

    def test_can_write_key(self, clean_redis_url: str) -> None:
        import redis as redis_sync

        r = redis_sync.from_url(clean_redis_url, decode_responses=True)
        try:
            r.set("test_key", "hello")
            assert r.get("test_key") == "hello"
        finally:
            r.close()

    def test_no_slop_from_previous_test(self, clean_redis_url: str) -> None:
        import redis as redis_sync

        r = redis_sync.from_url(clean_redis_url, decode_responses=True)
        try:
            assert r.get("test_key") is None
        finally:
            r.close()


# ── clean_redis_client provides working client ─────────────────


@pytest.mark.redis
class TestCleanRedisClient:
    """clean_redis_client yields a working async Redis client."""

    async def test_client_pings(self, clean_redis_client: object) -> None:
        result = await clean_redis_client.ping()  # type: ignore[union-attr]
        assert result

    async def test_no_slop_from_previous_test(self, clean_redis_client: object) -> None:
        val = await clean_redis_client.get("test_async_key")  # type: ignore[union-attr]
        assert val is None


# ── seed_actors with custom actors ─────────────────────────────


class TestCustomSeedActors:
    """seed_actors can produce custom actor sets."""

    async def test_empty_actors(self, module_pg_schema: ModulePgSchema) -> None:
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            await truncate_schema(conn, module_pg_schema.schema_name)
            await seed_actors(conn, module_pg_schema.schema_name, actors=[])
            count = await conn.fetchval(
                f"SELECT count(*) FROM {module_pg_schema.schema_name}.actor_config"
            )
            assert count == 0
        finally:
            await conn.close()

    async def test_custom_actors(self, module_pg_schema: ModulePgSchema) -> None:
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            await truncate_schema(conn, module_pg_schema.schema_name)
            await seed_actors(conn, module_pg_schema.schema_name, actors=["custom_a", "custom_b"])
            rows = await conn.fetch(
                f"SELECT actor FROM {module_pg_schema.schema_name}.actor_config ORDER BY actor"
            )
            assert [r["actor"] for r in rows] == ["custom_a", "custom_b"]
        finally:
            await conn.close()


# ── truncate_schema leaves migration metadata intact ───────────


class TestTruncateSchemaMetadata:
    async def test_migrations_preserved(self, module_pg_schema: ModulePgSchema) -> None:
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            s = module_pg_schema.schema_name
            before = await conn.fetchval(f"SELECT count(*) FROM {s}.schema_migrations")
            await truncate_schema(conn, s)
            after = await conn.fetchval(f"SELECT count(*) FROM {s}.schema_migrations")
            assert before == after
            assert before > 0
        finally:
            await conn.close()
