"""Verify the module-scoped PG/Redis fixtures provide proper isolation.

Covers: module_pg_schema is module-scoped - same schema for all tests in
file; module_redis_url is module-scoped - same DB id for all tests in
file; clean_pg_conn truncates between tests - no cross-test PG state;
clean_jobs_app provides working WorkerDeps + PostgresBackend;
clean_redis_url flushdb between tests - no cross-test Redis state;
clean_redis_client provides a working Redis async client; seed_actors
with custom actors - empty/custom actors work; truncate_schema leaves
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


@pytest.fixture(scope="module")
def pg_schema_seen_at_setup(module_pg_schema: ModulePgSchema) -> str:
    """The module schema, recorded once per (worker, module) at fixture SETUP.

    The recording lives here, not in a test body, because pytest-randomly
    reshuffles order within each xdist worker: any "a previous test body ran
    first in this process" premise is an ordering dependence. Setup time is
    order-invariant - every test that depends on this recorder compares
    against the instance the module fixture actually handed out.
    """
    _MOD_SEEN.add(module_pg_schema.schema_name)
    return module_pg_schema.schema_name


@pytest.fixture(scope="module")
def redis_url_seen_at_setup(module_redis_url: str) -> str:
    """The module Redis URL, recorded once per (worker, module) at fixture
    SETUP - same order-invariant recording as :func:`pg_schema_seen_at_setup`.
    """
    _REDIS_DB_SEEN.add(module_redis_url)
    return module_redis_url


# ── module_pg_schema is module-scoped ──────────────────────────


@pytest.mark.xdist_group(name="fixtures")
class TestModulePgSchema:
    """Schema name is stable across tests in the same module."""

    def test_schema_name_is_string(
        self, module_pg_schema: ModulePgSchema, pg_schema_seen_at_setup: str
    ) -> None:
        assert isinstance(module_pg_schema.schema_name, str)
        assert module_pg_schema.schema_name.startswith("tq_")
        assert module_pg_schema.schema_name == pg_schema_seen_at_setup

    def test_same_schema_name_as_previous_test(
        self, module_pg_schema: ModulePgSchema, pg_schema_seen_at_setup: str
    ) -> None:
        # The membership assert is the module-scope proof: a re-instantiated
        # fixture would hand this test a different (or re-created) schema, so
        # the URL/schema this test sees must be the one recorded at setup.
        assert module_pg_schema.schema_name == pg_schema_seen_at_setup
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

    def test_url_is_string(self, module_redis_url: str, redis_url_seen_at_setup: str) -> None:
        assert isinstance(module_redis_url, str)
        assert module_redis_url.startswith("redis://")
        assert module_redis_url == redis_url_seen_at_setup

    def test_same_redis_url_as_previous_test(
        self, module_redis_url: str, redis_url_seen_at_setup: str
    ) -> None:
        # The membership assert is the module-scope proof: the fixture
        # allocates a never-reused DB id per instantiation, so a
        # re-instantiated fixture would hand this test a different URL. The
        # recorded value is captured at fixture SETUP (order-invariant under
        # pytest-randomly's per-worker shuffle), not by a sibling test body.
        assert module_redis_url == redis_url_seen_at_setup
        assert module_redis_url in _REDIS_DB_SEEN

    def test_redis_is_reachable(self, module_redis_url: str) -> None:
        import redis as redis_sync

        r = redis_sync.from_url(module_redis_url, decode_responses=False, socket_timeout=None)
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

        r = redis_sync.from_url(clean_redis_url, decode_responses=True, socket_timeout=None)
        try:
            r.set("test_key", "hello")
            assert r.get("test_key") == "hello"
        finally:
            r.close()

    def test_no_slop_from_previous_test(self, clean_redis_url: str) -> None:
        import redis as redis_sync

        r = redis_sync.from_url(clean_redis_url, decode_responses=True, socket_timeout=None)
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


# ── truncate_schema restores the DDL, safely ───────────────────
#
# A test that installs a trigger changes the schema's DDL, which no
# TRUNCATE undoes, so the reset drops any trigger the migrations did not
# install.  The trigger and table names come from the catalog, and the
# DROP interpolates them - the project's identifier rule (validate
# against the canonical identifier regex before any interpolation)
# applies to catalog-sourced names exactly as to user-sourced ones.


class TestTruncateSchemaTriggerReset:
    async def test_test_added_trigger_is_dropped(self, module_pg_schema: ModulePgSchema) -> None:
        """A trigger a test installed is gone after the reset - the DDL
        the next test meets is the migrated one."""
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        s = module_pg_schema.schema_name
        try:
            await truncate_schema(conn, s)
            await conn.execute(
                f'CREATE OR REPLACE FUNCTION "{s}".trg_probe() '  # Why: schema is the test-fixture identifier.
                "RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
            )
            await conn.execute(
                f'CREATE TRIGGER trg_probe AFTER INSERT ON "{s}".jobs '  # Why: schema is the test-fixture identifier; trigger/function names are test-authored literals.
                f'FOR EACH ROW EXECUTE FUNCTION "{s}".trg_probe()'
            )

            await truncate_schema(conn, s)

            remaining = await conn.fetchval(
                "SELECT count(*) FROM pg_trigger t "
                "JOIN pg_class c ON c.oid = t.tgrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = $1 AND t.tgname = 'trg_probe' AND NOT t.tgisinternal",
                s,
            )
            assert remaining == 0, (
                "a trigger the migrations did not install must not leak into the next test"
            )
        finally:
            await conn.execute(
                f'DROP TRIGGER IF EXISTS trg_probe ON "{s}".jobs'
            )  # Why: schema is the test-fixture identifier; trigger name is a test-authored literal.
            await conn.execute(
                f'DROP FUNCTION IF EXISTS "{s}".trg_probe()'
            )  # Why: schema is the test-fixture identifier; function name is a test-authored literal.
            await conn.close()

    async def test_trigger_name_failing_identifier_validation_raises(
        self, module_pg_schema: ModulePgSchema
    ) -> None:
        """A catalog-sourced trigger name the identifier rule cannot
        admit must fail the reset loudly rather than be interpolated raw
        into the DROP - an unvalidated name breaks out of the quoting."""
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        s = module_pg_schema.schema_name
        try:
            await truncate_schema(conn, s)
            await conn.execute(
                f'CREATE OR REPLACE FUNCTION "{s}".trg_probe_unsafe() '  # Why: schema is the test-fixture identifier.
                "RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
            )
            # The quote in the name is the point: interpolated raw it
            # terminates the quoted identifier in the DROP.
            await conn.execute(
                f'CREATE TRIGGER "trg""unsafe" AFTER INSERT ON "{s}".jobs '  # Why: schema is the test-fixture identifier; trigger/function names are test-authored literals.
                f'FOR EACH ROW EXECUTE FUNCTION "{s}".trg_probe_unsafe()'
            )

            with pytest.raises(ValueError, match="trg"):
                await truncate_schema(conn, s)
        finally:
            await conn.execute(
                f'DROP TRIGGER IF EXISTS "trg""unsafe" ON "{s}".jobs'
            )  # Why: schema is the test-fixture identifier; trigger name is a test-authored literal.
            await conn.execute(
                f'DROP FUNCTION IF EXISTS "{s}".trg_probe_unsafe()'
            )  # Why: schema is the test-fixture identifier; function name is a test-authored literal.
            await conn.close()
