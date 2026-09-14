"""Red-team attacks on SET LOCAL statement_timeout scoping in the sweeps.

Every bounded sweep opens its own ``conn.transaction()`` and issues
``SET LOCAL statement_timeout = <ms>`` inside it.  When the caller
already has a transaction open on that connection (the embedder shape
pinned by ``test_clock_domain_isolation.py``'s D12 — the sweep runs as a
nested SAVEPOINT), ``SET LOCAL``'s scope is the *transaction*, not the
savepoint: a savepoint RELEASE keeps the setting, so the sweep's timeout
outlives the sweep and bounds the caller's subsequent statements in the
same transaction.  Measured live: a nested sweep with a 300 ms timeout
leaves the outer transaction at ``'300ms'``, and a following
``pg_sleep(1)`` is cancelled with SQLSTATE 57014 — the caller's own
execution environment was changed underneath it.

The asymmetry is real and worth pinning both ways:

* RELEASE (sweep success): the timeout LEAKS unless the sweep restores
  the previous value — the red finding this file exists to prove;
* ROLLBACK TO SAVEPOINT (sweep failure): PostgreSQL's subtransaction
  GUC stack restores the previous value automatically — pinned here as
  existing behaviour so a future refactor away from savepoints (or to a
  plain ``SET``) cannot silently regress it.

The companion attack proves the timeout genuinely applies to the batch's
own statements while nested: the sweep-1 attempt INSERT's
``FOR KEY SHARE`` probe of ``workers`` blocks on a competing ``FOR
UPDATE`` holder, and the nested sweep's timeout cancels it — a
deterministic, sleep-free way to hold a batch statement open past its
bound.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)
# Small enough that the leaked-timeout probe (pg_sleep(1)) is cancelled
# quickly when the leak exists; large enough that the sweep's own fast
# statements never touch it.
_SWEEP_TIMEOUT_MS = 300


# ── Seeding: one eligible row per sweep kind ─────────────────────────────


async def _seed_due_scheduled(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid()],
    )


async def _seed_overdue_deadline(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, "
        " schedule_to_close) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp() - interval '60 seconds', clock_timestamp() - interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid()],
    )


async def _seed_expired_lock(conn: asyncpg.Connection, schema: str) -> UUID:
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, attempt, "
        " scheduled_at, locked_by_worker, lock_expires_at, started_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'running', 3, 'transient', 1, "
        "clock_timestamp(), $2, clock_timestamp() - interval '10 seconds', "
        "clock_timestamp() - interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid()],
        worker_id,
    )
    return worker_id


_SWEEP_CALLS: dict[str, Callable[..., Awaitable[int]]] = {
    "scheduled_to_pending": lambda conn, schema: PostgresBackend.sweep_scheduled_to_pending(
        conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        statement_timeout_ms=_SWEEP_TIMEOUT_MS,
    ),
    "deadline_exceeded": lambda conn, schema: PostgresBackend.sweep_deadline_exceeded(
        conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        statement_timeout_ms=_SWEEP_TIMEOUT_MS,
    ),
    "expired_locks": lambda conn, schema: PostgresBackend.sweep_expired_locks(
        conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
        schema=schema,
        statement_timeout_ms=_SWEEP_TIMEOUT_MS,
    ),
}

_SEEDERS: dict[str, Callable[..., Awaitable[Any]]] = {
    "scheduled_to_pending": _seed_due_scheduled,
    "deadline_exceeded": _seed_overdue_deadline,
    "expired_locks": _seed_expired_lock,
}


@pytest.mark.parametrize("sweep_name", sorted(_SWEEP_CALLS))
async def test_nested_sweep_does_not_leak_its_timeout_past_the_savepoint(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    sweep_name: str,
) -> None:
    """A nested sweep must leave the caller's transaction timeout alone.

    The sweep promises "one short transaction with a server-side
    statement_timeout included" — the timeout bounds the sweep's OWN
    batch, not the caller's remaining statements.  Today the savepoint
    RELEASE keeps the SET LOCAL, so after the sweep returns, the outer
    transaction runs under the sweep's 300 ms bound and the caller's
    next moderately slow statement is cancelled with 57014 — an error
    the caller has no way to attribute to the sweep it called.
    """
    schema = module_pg_schema.schema_name
    await _SEEDERS[sweep_name](clean_pg_conn, schema)

    before = await clean_pg_conn.fetchval("SELECT current_setting('statement_timeout')")
    async with clean_pg_conn.transaction():
        count = await _SWEEP_CALLS[sweep_name](clean_pg_conn, schema)
        assert count == 1, "the seeded row must be swept by the nested call"
        after = await clean_pg_conn.fetchval("SELECT current_setting('statement_timeout')")
        assert after == before, (
            f"the sweep leaked its statement_timeout into the caller's "
            f"transaction: {before!r} before the sweep, {after!r} after the "
            "savepoint RELEASE — the caller's subsequent statements now run "
            f"under the sweep's {_SWEEP_TIMEOUT_MS} ms bound"
        )
        # Behavioural confirmation: a caller statement longer than the
        # sweep's timeout must survive inside its own transaction.
        await clean_pg_conn.execute("SELECT pg_sleep(1)")


async def test_nested_sweep_timeout_bounds_the_batch_itself(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The timeout must APPLY to the sweep's own statements while nested.

    The sweep-1 attempt INSERT probes ``workers`` under ``FOR KEY
    SHARE``; a competing ``FOR UPDATE`` holder on the same workers row
    blocks that probe deterministically (no sleeps, no timing races).
    With the sweep's timeout at 400 ms the blocked batch must be
    cancelled with ``QueryCanceledError`` — proving the SET LOCAL takes
    effect for the batch statements themselves in the nested case, not
    only after it.
    """
    schema = module_pg_schema.schema_name
    worker_id = await _seed_expired_lock(clean_pg_conn, schema)
    dsn = module_pg_schema.pg_dsn

    holder = await asyncpg.connect(dsn)
    try:
        async with holder.transaction():
            await holder.execute(
                f'SELECT id FROM "{schema}".workers WHERE id = $1 FOR UPDATE',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
                worker_id,
            )
            async with clean_pg_conn.transaction():
                with pytest.raises(asyncpg.QueryCanceledError):
                    await PostgresBackend.sweep_expired_locks(
                        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
                        _CANCEL_GRACE,
                        _CLEANUP_GRACE,
                        schema=schema,
                        statement_timeout_ms=400,
                    )
                # The savepoint rollback left the outer transaction usable
                # and the victim un-swept: the whole batch, including the
                # driving UPDATE, rolled back.
                status = await clean_pg_conn.fetchval(
                    f'SELECT status::text FROM "{schema}".jobs',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
                )
                assert status == "running", (
                    "a cancelled batch must roll back the driving UPDATE too, "
                    f"not leave a partial write; job is {status!r}"
                )
    finally:
        await holder.close()

    # Contention gone: the same sweep now completes.
    reclaimed = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
        schema=schema,
        statement_timeout_ms=400,
    )
    assert reclaimed == 1, "the re-run after contention must reclaim the row"


async def test_savepoint_rollback_restores_the_caller_timeout(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A FAILED nested sweep leaves the caller's timeout untouched.

    The mirror of the leak attack: PostgreSQL's subtransaction GUC stack
    restores statement_timeout on ROLLBACK TO SAVEPOINT, so the error
    path never leaked even before any fix.  Pinned so a refactor away
    from savepoint semantics (or to a plain SET) cannot regress it
    silently.
    """
    schema = module_pg_schema.schema_name
    worker_id = await _seed_expired_lock(clean_pg_conn, schema)
    before = await clean_pg_conn.fetchval("SELECT current_setting('statement_timeout')")
    dsn = module_pg_schema.pg_dsn

    holder = await asyncpg.connect(dsn)
    try:
        async with holder.transaction():
            await holder.execute(
                f'SELECT id FROM "{schema}".workers WHERE id = $1 FOR UPDATE',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
                worker_id,
            )
            async with clean_pg_conn.transaction():
                with pytest.raises(asyncpg.QueryCanceledError):
                    await PostgresBackend.sweep_expired_locks(
                        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
                        _CANCEL_GRACE,
                        _CLEANUP_GRACE,
                        schema=schema,
                        statement_timeout_ms=400,
                    )
                after = await clean_pg_conn.fetchval("SELECT current_setting('statement_timeout')")
                assert after == before, (
                    f"rollback must restore the caller timeout: {before!r} -> {after!r}"
                )
                await clean_pg_conn.execute("SELECT pg_sleep(1)")
    finally:
        await holder.close()


async def test_unnested_sweep_leaves_the_connection_timeout_untouched(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A sweep on its own transaction must not touch the session timeout.

    ``SET LOCAL`` evaporates at the sweep's own COMMIT, so the session
    default survives the call.  Pinned because the failure mode of a
    regression to plain ``SET`` (session-scoped) is every subsequent
    caller on a pooled connection inheriting the sweep's bound.
    """
    schema = module_pg_schema.schema_name
    await _seed_due_scheduled(clean_pg_conn, schema)

    before = await clean_pg_conn.fetchval("SELECT current_setting('statement_timeout')")
    count = await PostgresBackend.sweep_scheduled_to_pending(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        statement_timeout_ms=_SWEEP_TIMEOUT_MS,
    )
    assert count == 1
    after = await clean_pg_conn.fetchval("SELECT current_setting('statement_timeout')")
    assert after == before, (
        f"an own-transaction sweep must leave the session timeout alone: {before!r} -> {after!r}"
    )
    await asyncio.sleep(0)  # yield so the connection is fully settled
    await clean_pg_conn.execute("SELECT pg_sleep(1)")
