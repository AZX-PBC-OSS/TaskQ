"""Full-stack integration tests for actor deregistration via TaskQ.actors.

Exercises the complete client path: TaskQ → ActorsClient → pool →
deregister_actor → real Postgres. No Docker worker container needed —
the tests seed actor_config rows directly and call through the client.
"""

from uuid import uuid4

import asyncpg
import pytest

from taskq.actor_config import ActorConfig
from taskq.exceptions import (
    ActorHasActiveJobsError,
    ActorNotFoundError,
)
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _seed_actor(
    conn: asyncpg.Connection, schema: str, actor: str, queue: str = "default"
) -> None:
    await sync_actor_config(
        conn,
        [ActorConfig(actor=actor, max_concurrent=5, queue=queue)],
        schema=schema,
    )


async def _insert_job(conn: asyncpg.Connection, schema: str, actor: str, status: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, max_attempts, retry_kind) '  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; actor/status are test constants.
        f"VALUES ($1, $2, 'default', '{{}}'::jsonb, $3::\"{schema}\".job_status, 3, 'transient')",
        uuid4(),
        actor,
        status,
    )


async def _insert_queue(conn: asyncpg.Connection, schema: str, name: str) -> None:
    await conn.execute(f'INSERT INTO "{schema}".queues (name) VALUES ($1)', name)  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; name is a test constant.


async def test_client_deregister_clean_actor(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: deregister an actor with no jobs or schedules."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _seed_actor(clean_pg_conn, schema, "client_clean_actor")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        result = await tq.actors.deregister("client_clean_actor")

    assert result.actor_config_deleted is True
    assert result.actor == "client_clean_actor"


async def test_client_deregister_not_found(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: ActorNotFoundError for unknown actor."""
    from taskq import TaskQ

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=module_pg_schema.schema_name) as tq:
        with pytest.raises(ActorNotFoundError, match="no stored actor_config row"):
            await tq.actors.deregister("nonexistent_actor")


async def test_client_deregister_refuses_with_active_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: ActorHasActiveJobsError when pending jobs exist."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _seed_actor(clean_pg_conn, schema, "client_busy_actor")
    await _insert_job(clean_pg_conn, schema, "client_busy_actor", "pending")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        with pytest.raises(ActorHasActiveJobsError) as exc_info:
            await tq.actors.deregister("client_busy_actor")

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"pending": 1}


async def test_client_deregister_force_cancels_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: force=True cancels pending jobs through the client."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _seed_actor(clean_pg_conn, schema, "client_force_actor")
    await _insert_job(clean_pg_conn, schema, "client_force_actor", "pending")
    await _insert_job(clean_pg_conn, schema, "client_force_actor", "scheduled")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        result = await tq.actors.deregister("client_force_actor", force=True)

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 2

    # Verify jobs are cancelled in DB
    cancelled_count = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = 'client_force_actor' AND status = 'cancelled'"  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; actor/status are test constants.
    )
    assert cancelled_count == 2


async def test_client_deregister_with_purge_queue(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: purge_queue=True deletes orphaned queue."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _insert_queue(clean_pg_conn, schema, "client_solo_queue")
    await _seed_actor(clean_pg_conn, schema, "client_purge_actor", queue="client_solo_queue")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        result = await tq.actors.deregister("client_purge_actor", purge_queue=True)

    assert result.queue_purged is True
    assert result.queue == "client_solo_queue"

    queue_count = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".queues WHERE name = 'client_solo_queue'"  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; name is a test constant.
    )
    assert queue_count == 0


async def test_client_double_deregister_raises_not_found(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: second deregister raises ActorNotFoundError (idempotency)."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _seed_actor(clean_pg_conn, schema, "client_idem_actor")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        result = await tq.actors.deregister("client_idem_actor")
        assert result.actor_config_deleted is True

        with pytest.raises(ActorNotFoundError):
            await tq.actors.deregister("client_idem_actor")


async def test_client_actors_list_returns_rows(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: tq.actors.list() returns seeded actor_config rows."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _seed_actor(clean_pg_conn, schema, "client_list_actor")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        rows = await tq.actors.list()

    actors = [r.actor for r in rows]
    assert "client_list_actor" in actors


async def test_client_actors_get_returns_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: tq.actors.get() returns the specific actor row."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _seed_actor(clean_pg_conn, schema, "client_get_actor")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        row = await tq.actors.get("client_get_actor")
        assert row is not None
        assert row.actor == "client_get_actor"

        missing = await tq.actors.get("nonexistent")
        assert missing is None


async def test_client_actors_set_capacity(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Full client path: tq.actors.set_capacity() updates max_concurrent."""
    from taskq import TaskQ

    schema = module_pg_schema.schema_name
    await _seed_actor(clean_pg_conn, schema, "client_setcap_actor")

    async with TaskQ(dsn=module_pg_schema.pg_dsn, schema=schema) as tq:
        row = await tq.actors.set_capacity("client_setcap_actor", max_concurrent=10)

    assert row is not None
    assert row.max_concurrent == 10
