"""Tests for `taskq.actor_config_ops`: the operator read/tune surface
that backs `taskq actor-config get/set/list`.

These are integration-tier (real Postgres) because the whole point of the
module is a hand-written SQL UPDATE with conditional column assignment —
a fake connection would only prove the query string looks right, not that
Postgres executes it the way we think.
"""

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import (
    get_actor_config,
    list_actor_configs,
    set_actor_config_capacity,
)
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _ensure_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}".actor_config (
            actor          text PRIMARY KEY,
            max_concurrent int,
            max_pending    int,
            queue          text NOT NULL,
            result_ttl     float,
            metadata       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            updated_at     timestamptz NOT NULL DEFAULT now()
        )
    """)


async def test_get_returns_none_for_unknown_actor(pg_conn: asyncpg.Connection) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    assert await get_actor_config(pg_conn, "missing", schema=schema) is None


async def test_set_returns_none_for_unknown_actor(pg_conn: asyncpg.Connection) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    result = await set_actor_config_capacity(pg_conn, "missing", max_concurrent=5, schema=schema)
    assert result is None


async def test_set_rejects_negative_max_concurrent(pg_conn: asyncpg.Connection) -> None:
    """Mirrors @actor(...)'s own decoration-time guard (taskq/actor.py) —
    a negative value here would silently floor the dispatch CTE's residual
    to zero and pause the actor with no error anywhere in the path.
    """
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="a", max_concurrent=5, queue="default")],
        schema=schema,
    )

    with pytest.raises(ValueError, match="max_concurrent must be a non-negative integer"):
        await set_actor_config_capacity(pg_conn, "a", max_concurrent=-1, schema=schema)


async def test_set_rejects_negative_max_pending(pg_conn: asyncpg.Connection) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="a", max_concurrent=5, queue="default")],
        schema=schema,
    )

    with pytest.raises(ValueError, match="max_pending must be a non-negative integer"):
        await set_actor_config_capacity(pg_conn, "a", max_pending=-1, schema=schema)


async def test_set_rejects_negative_result_ttl(pg_conn: asyncpg.Connection) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="a", max_concurrent=5, queue="default")],
        schema=schema,
    )

    with pytest.raises(ValueError, match="result_ttl must be a non-negative number"):
        await set_actor_config_capacity(pg_conn, "a", result_ttl=-1.0, schema=schema)


async def test_set_changes_only_the_passed_field(pg_conn: asyncpg.Connection) -> None:
    """Setting max_concurrent must not disturb max_pending or result_ttl."""
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [
            ActorConfig(
                actor="a", max_concurrent=5, max_pending=10, queue="default", result_ttl=60.0
            )
        ],
        schema=schema,
    )

    updated = await set_actor_config_capacity(pg_conn, "a", max_concurrent=9, schema=schema)

    assert updated is not None
    assert updated.max_concurrent == 9
    assert updated.max_pending == 10
    assert updated.result_ttl == 60.0


async def test_set_can_clear_a_field_to_null(pg_conn: asyncpg.Connection) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="a", max_concurrent=5, queue="default")],
        schema=schema,
    )

    cleared = await set_actor_config_capacity(pg_conn, "a", max_concurrent=None, schema=schema)

    assert cleared is not None
    assert cleared.max_concurrent is None


async def test_set_all_three_capacity_fields_at_once(pg_conn: asyncpg.Connection) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [
            ActorConfig(
                actor="a", max_concurrent=5, max_pending=10, queue="default", result_ttl=60.0
            )
        ],
        schema=schema,
    )

    updated = await set_actor_config_capacity(
        pg_conn, "a", max_concurrent=7, max_pending=20, result_ttl=120.0, schema=schema
    )

    assert updated is not None
    assert updated.max_concurrent == 7
    assert updated.max_pending == 20
    assert updated.result_ttl == 120.0


async def test_list_returns_all_rows_ordered_by_actor(pg_conn: asyncpg.Connection) -> None:
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [
            ActorConfig(actor="b", max_concurrent=1, queue="default"),
            ActorConfig(actor="a", max_concurrent=2, queue="default"),
        ],
        schema=schema,
    )

    rows = await list_actor_configs(pg_conn, schema=schema)

    assert [r.actor for r in rows] == ["a", "b"]


async def test_get_reflects_a_prior_set(pg_conn: asyncpg.Connection) -> None:
    """End-to-end: sync seeds the row, set changes it, get sees the change —
    this is the exact call sequence `taskq actor-config set` / `get` drive.
    """
    schema = f"taco_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="a", max_concurrent=5, queue="default")],
        schema=schema,
    )

    await set_actor_config_capacity(pg_conn, "a", max_concurrent=42, schema=schema)

    row = await get_actor_config(pg_conn, "a", schema=schema)
    assert row is not None
    assert row.max_concurrent == 42
