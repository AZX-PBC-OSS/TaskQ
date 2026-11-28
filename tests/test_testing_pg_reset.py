"""Unit and integration tests for ``truncate_schema``, ``seed_actors``, and ``reset_schema``."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
from asyncpg.exceptions import (
    InFailedSQLTransactionError,
    InvalidSchemaNameError,
    UndefinedTableError,
)

from taskq._ids import new_base62, new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import (
    DEFAULT_ACTORS,
    reset_schema,
    seed_actors,
    truncate_schema,
)


class _FailIfCalledConn:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"unexpected DB call: {name}")


class _CountingConnProxy:
    """Thin pass-through proxy around a real ``asyncpg.Connection`` counting DB calls."""

    def __init__(self, inner: asyncpg.Connection) -> None:
        self._inner = inner
        self.calls: int = 0
        self.executemany_calls: int = 0

    def reset_counts(self) -> None:
        self.calls = 0
        self.executemany_calls = 0

    async def execute(self, query: str, *args: object, **kwargs: Any) -> str:
        self.calls += 1
        return await self._inner.execute(query, *args, **kwargs)

    async def fetch(self, query: str, *args: object, **kwargs: Any) -> list[asyncpg.Record]:
        self.calls += 1
        return await self._inner.fetch(query, *args, **kwargs)

    async def fetchval(self, query: str, *args: object, **kwargs: Any) -> Any:
        self.calls += 1
        return await self._inner.fetchval(query, *args, **kwargs)

    async def fetchrow(self, query: str, *args: object, **kwargs: Any) -> asyncpg.Record | None:
        self.calls += 1
        return await self._inner.fetchrow(query, *args, **kwargs)

    async def executemany(
        self,
        command: str,
        args: Iterable[Sequence[object]],
        **kwargs: Any,
    ) -> None:
        self.calls += 1
        self.executemany_calls += 1
        await self._inner.executemany(command, args, **kwargs)

    def transaction(self, **kwargs: Any) -> Any:
        return self._inner.transaction(**kwargs)

    def is_in_transaction(self) -> bool:
        return self._inner.is_in_transaction()


def _dsn_for_database(dsn: str, db_name: str) -> str:
    parts = urlsplit(dsn)
    return urlunsplit((parts.scheme, parts.netloc, f"/{db_name}", parts.query, parts.fragment))


async def _setup_trigger_schema(
    conn: asyncpg.Connection,
    schema: str,
    baseline_trigger: str,
) -> None:
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await conn.execute(f'CREATE TABLE "{schema}".jobs (id int PRIMARY KEY)')
    await conn.execute(
        f'CREATE FUNCTION "{schema}".noop() RETURNS trigger '
        "AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
    )
    await conn.execute(
        f'CREATE TRIGGER "{baseline_trigger}" AFTER INSERT ON "{schema}".jobs '
        f'FOR EACH ROW EXECUTE FUNCTION "{schema}".noop()'
    )


async def _list_user_triggers(conn: asyncpg.Connection, schema: str) -> list[str]:
    rows = await conn.fetch(
        "SELECT t.tgname FROM pg_trigger t "
        "JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND NOT t.tgisinternal "
        "ORDER BY t.tgname",
        schema,
    )
    return [str(r["tgname"]) for r in rows]


# ── Pure unit test (input validation & empty fast-path) ─────────────────


async def test_seed_actors_empty_validates_schema_and_makes_zero_db_calls() -> None:
    stub: Any = _FailIfCalledConn()
    with pytest.raises(ValueError, match="invalid schema name"):
        await seed_actors(stub, "bad;name", actors=[])
    await seed_actors(stub, "valid_schema", actors=[])


# ── Integration tests against real PostgreSQL ───────────────────────────


@pytest.mark.integration
async def test_reset_helpers_o1_round_trip_budget_pg(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Reset helpers execute in O(1) DB round-trips without executemany and produce expected DB state."""
    s = module_pg_schema.schema_name
    proxy = _CountingConnProxy(clean_pg_conn)
    proxy_conn: Any = proxy

    # Seed dirty state via raw connection so setup does not count toward the budget.
    await clean_pg_conn.execute(
        f'INSERT INTO "{s}".jobs '  # noqa: S608  # Why: validated fixture schema identifier.
        "(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at) "
        "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 3, 'transient', now())",
        new_uuid(),
    )
    await clean_pg_conn.execute(
        f'DELETE FROM "{s}".wf_emit_intent_rotation'  # noqa: S608  # Why: validated fixture schema identifier.
    )

    # 1. truncate_schema: <= 3 round-trips, zero executemany.
    proxy.reset_counts()
    await truncate_schema(proxy_conn, s)
    assert proxy.calls <= 3
    assert proxy.executemany_calls == 0

    assert (
        await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{s}".jobs'  # noqa: S608  # Why: validated fixture schema identifier.
        )
        == 0
    )
    assert (
        await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{s}".actor_config'  # noqa: S608  # Why: validated fixture schema identifier.
        )
        == 0
    )
    rotation_rows = await clean_pg_conn.fetch(
        f'SELECT id FROM "{s}".wf_emit_intent_rotation'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    assert [r["id"] for r in rotation_rows] == [True]
    assert (
        await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{s}".schema_migrations'  # noqa: S608  # Why: validated fixture schema identifier.
        )
        > 0
    )

    # 2. seed_actors (default actors): <= 1 round-trip, zero executemany.
    proxy.reset_counts()
    await seed_actors(proxy_conn, s)
    assert proxy.calls <= 1
    assert proxy.executemany_calls == 0

    seeded_rows = await clean_pg_conn.fetch(
        f'SELECT actor, queue FROM "{s}".actor_config ORDER BY actor'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    assert [(r["actor"], r["queue"]) for r in seeded_rows] == sorted(
        (actor, "default") for actor in DEFAULT_ACTORS
    )

    # 3. seed_actors with empty list: 0 round-trips.
    proxy.reset_counts()
    await seed_actors(proxy_conn, s, actors=[])
    assert proxy.calls == 0
    assert proxy.executemany_calls == 0

    # 4. reset_schema (default actors): <= 4 round-trips, zero executemany.
    await clean_pg_conn.execute(
        f'INSERT INTO "{s}".jobs '  # noqa: S608  # Why: validated fixture schema identifier.
        "(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at) "
        "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 3, 'transient', now())",
        new_uuid(),
    )
    await clean_pg_conn.execute(
        f'DELETE FROM "{s}".wf_emit_intent_rotation'  # noqa: S608  # Why: validated fixture schema identifier.
    )

    proxy.reset_counts()
    await reset_schema(proxy_conn, s)
    assert proxy.calls <= 4
    assert proxy.executemany_calls == 0

    assert (
        await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{s}".jobs'  # noqa: S608  # Why: validated fixture schema identifier.
        )
        == 0
    )
    rotation_after = await clean_pg_conn.fetch(
        f'SELECT id FROM "{s}".wf_emit_intent_rotation'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    assert [r["id"] for r in rotation_after] == [True]
    actors_after = await clean_pg_conn.fetch(
        f'SELECT actor, queue FROM "{s}".actor_config ORDER BY actor'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    assert [(r["actor"], r["queue"]) for r in actors_after] == sorted(
        (actor, "default") for actor in DEFAULT_ACTORS
    )


@pytest.mark.integration
async def test_reset_schema_full_and_inside_transaction_pg(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full-schema truncate + re-seed succeeds cleanly inside an open transaction."""
    s = module_pg_schema.schema_name
    await clean_pg_conn.execute(
        f'INSERT INTO "{s}".jobs '  # noqa: S608  # Why: validated fixture schema identifier.
        "(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at) "
        "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 3, 'transient', now())",
        new_uuid(),
    )
    await clean_pg_conn.execute(
        f'DELETE FROM "{s}".wf_emit_intent_rotation'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    await clean_pg_conn.execute(
        f"INSERT INTO \"{s}\".actor_config (actor, queue) VALUES ('stale_actor', 'custom_q')"  # noqa: S608  # Why: validated fixture schema identifier.
    )

    async with clean_pg_conn.transaction():
        await reset_schema(clean_pg_conn, s)

    assert (
        await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{s}".jobs'  # noqa: S608  # Why: validated fixture schema identifier.
        )
        == 0
    )
    rotation_rows = await clean_pg_conn.fetch(
        f'SELECT id FROM "{s}".wf_emit_intent_rotation'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    assert [r["id"] for r in rotation_rows] == [True]
    actor_rows = await clean_pg_conn.fetch(
        f'SELECT actor, queue FROM "{s}".actor_config ORDER BY actor'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    assert [(r["actor"], r["queue"]) for r in actor_rows] == sorted(
        (actor, "default") for actor in DEFAULT_ACTORS
    )
    assert (
        await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{s}".schema_migrations'  # noqa: S608  # Why: validated fixture schema identifier.
        )
        > 0
    )


@pytest.mark.integration
async def test_truncate_schema_partial_empty_and_missing_schemas_pg(
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """Partial schemas truncate outside a tx, surface UndefinedTableError inside a tx,
    empty schemas raise UndefinedTableError, and nonexistent schemas raise InvalidSchemaNameError.
    """
    scratch = f"tq_partial_{new_base62()}".lower()
    empty_schema = f"tq_empty_{new_base62()}".lower()
    missing = f"tq_missing_{new_base62()}".lower()
    await clean_pg_conn.execute(f'CREATE SCHEMA "{scratch}"')
    await clean_pg_conn.execute(f'CREATE SCHEMA "{empty_schema}"')
    try:
        # Pre-42_01 subset of tables (no wf_emit_intent_rotation).
        await clean_pg_conn.execute(f'CREATE TABLE "{scratch}".jobs (id int PRIMARY KEY)')
        await clean_pg_conn.execute(
            f'CREATE TABLE "{scratch}".actor_config (actor text PRIMARY KEY, queue text NOT NULL)'
        )
        await clean_pg_conn.execute(
            f'INSERT INTO "{scratch}".jobs (id) VALUES (1)'  # noqa: S608  # Why: test-local scratch schema identifier.
        )
        await clean_pg_conn.execute(
            f"INSERT INTO \"{scratch}\".actor_config (actor, queue) VALUES ('a', 'default')"  # noqa: S608  # Why: test-local scratch schema identifier.
        )

        # Outside a transaction: truncates existing tables and rotation restore is a zero-error no-op.
        await truncate_schema(clean_pg_conn, scratch)
        assert (
            await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{scratch}".jobs'  # noqa: S608  # Why: test-local scratch schema identifier.
            )
            == 0
        )
        assert (
            await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{scratch}".actor_config'  # noqa: S608  # Why: test-local scratch schema identifier.
            )
            == 0
        )

        # Inside a transaction: must surface UndefinedTableError directly rather than issuing
        # a fallback query on the aborted transaction (which would raise InFailedSQLTransactionError).
        with pytest.raises(UndefinedTableError) as exc_info:
            async with clean_pg_conn.transaction():
                await truncate_schema(clean_pg_conn, scratch)
        assert not isinstance(exc_info.value, InFailedSQLTransactionError)

        # An existing schema with none of the known tables must raise UndefinedTableError.
        with pytest.raises(UndefinedTableError):
            await truncate_schema(clean_pg_conn, empty_schema)

        # A nonexistent schema must fail loudly with InvalidSchemaNameError.
        with pytest.raises(InvalidSchemaNameError):
            await truncate_schema(clean_pg_conn, missing)
    finally:
        await clean_pg_conn.execute(f'DROP SCHEMA IF EXISTS "{scratch}" CASCADE')
        await clean_pg_conn.execute(f'DROP SCHEMA IF EXISTS "{empty_schema}" CASCADE')


@pytest.mark.integration
async def test_seed_actors_duplicates_and_conflict_preservation_pg(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Duplicate input names deduplicate cleanly and existing custom queues are preserved."""
    s = module_pg_schema.schema_name
    await truncate_schema(clean_pg_conn, s)

    await clean_pg_conn.execute(
        f"INSERT INTO \"{s}\".actor_config (actor, queue) VALUES ('keep_me', 'custom_q')"  # noqa: S608  # Why: validated fixture schema identifier.
    )

    await seed_actors(clean_pg_conn, s, actors=["dup", "dup", "keep_me", "other"])

    rows = await clean_pg_conn.fetch(
        f'SELECT actor, queue FROM "{s}".actor_config ORDER BY actor'  # noqa: S608  # Why: validated fixture schema identifier.
    )
    assert [(r["actor"], r["queue"]) for r in rows] == [
        ("dup", "default"),
        ("keep_me", "custom_q"),
        ("other", "default"),
    ]


@pytest.mark.integration
async def test_reset_triggers_isolates_same_schema_across_databases_pg(
    clean_pg_conn: asyncpg.Connection,
    pg_dsn: str,
) -> None:
    """Two databases sharing an identical schema name keep independent trigger baselines."""
    shared_schema = f"tq_xiso_{new_base62()}".lower()
    scratch_db = f"tq_xiso_db_{new_base62()}".lower()
    scratch_conn: asyncpg.Connection | None = None

    await clean_pg_conn.execute(f'CREATE DATABASE "{scratch_db}"')
    try:
        scratch_conn = await asyncpg.connect(_dsn_for_database(pg_dsn, scratch_db))

        await _setup_trigger_schema(clean_pg_conn, shared_schema, "mig_db_a")
        await _setup_trigger_schema(scratch_conn, shared_schema, "mig_db_b")

        # Initial resets snapshot distinct baselines under the identical schema name.
        await truncate_schema(clean_pg_conn, shared_schema)
        await truncate_schema(scratch_conn, shared_schema)

        assert await _list_user_triggers(clean_pg_conn, shared_schema) == ["mig_db_a"]
        assert await _list_user_triggers(scratch_conn, shared_schema) == ["mig_db_b"]

        # Add a test-only trigger in each database, then reset both.
        await clean_pg_conn.execute(
            f'CREATE TRIGGER "test_extra_a" AFTER INSERT ON "{shared_schema}".jobs '
            f'FOR EACH ROW EXECUTE FUNCTION "{shared_schema}".noop()'
        )
        await scratch_conn.execute(
            f'CREATE TRIGGER "test_extra_b" AFTER INSERT ON "{shared_schema}".jobs '
            f'FOR EACH ROW EXECUTE FUNCTION "{shared_schema}".noop()'
        )

        await truncate_schema(clean_pg_conn, shared_schema)
        await truncate_schema(scratch_conn, shared_schema)

        # Each database preserves its own baseline trigger while dropping its test-added trigger.
        assert await _list_user_triggers(clean_pg_conn, shared_schema) == ["mig_db_a"]
        assert await _list_user_triggers(scratch_conn, shared_schema) == ["mig_db_b"]
    finally:
        if scratch_conn is not None:
            await scratch_conn.close()
        await clean_pg_conn.execute(f'DROP SCHEMA IF EXISTS "{shared_schema}" CASCADE')
        await clean_pg_conn.execute(f'DROP DATABASE IF EXISTS "{scratch_db}" WITH (FORCE)')


@pytest.mark.integration
async def test_reset_triggers_recreated_schema_same_name_pg(
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """Dropping and recreating a schema with the same name takes a fresh trigger baseline."""
    scratch = f"tq_trg_oid_{new_base62()}".lower()
    try:
        await _setup_trigger_schema(clean_pg_conn, scratch, "mig_a")

        # First reset snapshots mig_a for the first schema OID.
        await truncate_schema(clean_pg_conn, scratch)

        # Drop and recreate under the exact same schema name with a different migrated trigger.
        await clean_pg_conn.execute(f'DROP SCHEMA "{scratch}" CASCADE')
        await _setup_trigger_schema(clean_pg_conn, scratch, "mig_b")

        # New schema OID must snapshot mig_b rather than dropping it against the old OID's baseline.
        await truncate_schema(clean_pg_conn, scratch)
        assert await _list_user_triggers(clean_pg_conn, scratch) == ["mig_b"]

        # A trigger added after the new baseline is still dropped on the next reset.
        await clean_pg_conn.execute(
            f'CREATE TRIGGER "added_later" AFTER INSERT ON "{scratch}".jobs '
            f'FOR EACH ROW EXECUTE FUNCTION "{scratch}".noop()'
        )
        await truncate_schema(clean_pg_conn, scratch)
        assert await _list_user_triggers(clean_pg_conn, scratch) == ["mig_b"]
    finally:
        await clean_pg_conn.execute(f'DROP SCHEMA IF EXISTS "{scratch}" CASCADE')


async def test_reset_triggers_isolates_concurrent_servers_with_identical_names_and_oids() -> None:
    """Two concurrent servers sharing (db_name, schema, db_oid, schema_oid) keep distinct
    trigger baselines and never adopt leaked triggers from interleaving resets."""
    from datetime import UTC, datetime

    from taskq.testing.pg import (
        _reset_triggers,  # pyright: ignore[reportPrivateUsage]  # Why: unit pin for concurrent server_start isolation in the trigger cache.
    )

    class _FakeServerConn:
        def __init__(self, server_start: datetime, triggers: set[tuple[str, str]]) -> None:
            self.server_start = server_start
            self.triggers = set(triggers)
            self.dropped: list[str] = []

        async def fetch(self, _query: str, *_args: object, **_kwargs: Any) -> list[dict[str, Any]]:
            if not self.triggers:
                return [
                    {
                        "db_name": "taskq",
                        "db_oid": 16384,
                        "schema_oid": 2200,
                        "server_start": self.server_start,
                        "table_name": None,
                        "trigger_name": None,
                    }
                ]
            return [
                {
                    "db_name": "taskq",
                    "db_oid": 16384,
                    "schema_oid": 2200,
                    "server_start": self.server_start,
                    "table_name": tbl,
                    "trigger_name": trg,
                }
                for tbl, trg in sorted(self.triggers)
            ]

        async def execute(self, query: str, *_args: object, **_kwargs: Any) -> str:
            self.dropped.append(query)
            return "DROP TRIGGER"

    schema = f"tq_conc_{new_base62()}".lower()
    server_a = _FakeServerConn(datetime(2026, 11, 27, 1, 0, tzinfo=UTC), {("jobs", "mig_a")})
    server_b = _FakeServerConn(datetime(2026, 11, 27, 2, 0, tzinfo=UTC), {("jobs", "mig_b")})

    # Snapshot both concurrent servers under identical (db_name, schema, oids).
    await _reset_triggers(server_a, schema)
    await _reset_triggers(server_b, schema)

    # Test adds an extra trigger on server A; server B resets in between.
    server_a.triggers.add(("jobs", "leaked_a"))
    await _reset_triggers(server_b, schema)
    await _reset_triggers(server_a, schema)

    assert server_b.dropped == []
    assert server_a.dropped == [f'DROP TRIGGER "leaked_a" ON "{schema}"."jobs"']
