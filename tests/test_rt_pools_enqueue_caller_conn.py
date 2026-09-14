# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""Red-team attacks on ``enqueue_with_conn``'s caller-owned connection.

The caller owns the connection handed to ``enqueue_with_conn``; the
contract under attack is what TaskQ hands BACK on a typed refusal.
Every typed enqueue refusal on this path is an outcome the caller is
meant to catch and continue from (a singleton actor refused once, a cap
rejection shed to a backpressure handler, a lock-budget exhaustion
retried) — so the caller's session and transaction must come back
exactly as TaskQ found it: usable, with no GUC residue.

Three refusals, three shapes:

* **Singleton preflight hit** (Python raise after a SELECT — no
  statement error): leaves the caller's transaction usable today. Pinned
  green so a refactor cannot regress it.
* **Singleton unique-violation catch** (the INSERT races a concurrent
  singleton insert and loses): the raw UniqueViolationError is a
  STATEMENT error — it aborts the caller's whole transaction, and the
  typed ``SingletonCollisionError`` conversion does not undo that. The
  red finding: the SAME typed error leaves the caller's transaction
  dead on this detection path but alive on the preflight path. The
  desired observable is parity — a catchable typed refusal that leaves
  the transaction usable (savepoint-protect the insert, or roll back to
  a savepoint before converting).
* **unique_for lock-budget exhaustion** (bounded advisory wait gives
  up): raises ``UniqueForLockTimeoutError`` from a savepoint-rolled-back
  state. Pinned green on a BARE caller conn — the conn must come back
  not-in-transaction with ``lock_timeout`` at its session default (the
  savepoint rollback is what restores the GUC; a refactor away from
  savepoints would leak the 250 ms bound onto the session's next
  statements).

The cap-refusal twin (``MaxPendingExceededError`` inside the caller's
open transaction) is pinned green alongside: it is the caller-tx shape
the singleton red should converge to.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id
from taskq.backend._enqueue import _enqueue_with_conn
from taskq.backend._sql_templates import SqlTemplates, render
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import (
    MaxPendingExceededError,
    SingletonCollisionError,
    UniqueForLockTimeoutError,
)
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration

_BOUNDED_WAIT_S = 8.0


async def _bounded(awaitable: Awaitable[Any]) -> Any:
    """Every wait in this file is bounded so a red can present, never hang."""
    return await asyncio.wait_for(awaitable, timeout=_BOUNDED_WAIT_S)


async def _fresh_schema(pg_dsn: str) -> str:
    schema = f"tpl_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    return schema


def _settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema},
        validate=False,
    )


def _backend(pg_dsn: str, schema: str) -> PostgresBackend:
    deps = SimpleNamespace(settings=_settings(pg_dsn, schema))
    return PostgresBackend(
        deps,  # type: ignore[arg-type]  # Why: BackendDeps is a Protocol satisfied structurally by a settings-carrying SimpleNamespace; only deps.settings.schema_name is read on the enqueue paths.
        SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


def _args(
    actor: str,
    *,
    id: UUID | None = None,
    singleton: bool = False,
    max_pending: int | None = None,
    identity_key: str | None = None,
    unique_for: timedelta | None = None,
) -> Any:
    from taskq.backend._protocol import EnqueueArgs

    metadata: dict[str, object] = {"singleton": True} if singleton else {}
    return EnqueueArgs(
        id=id if id is not None else new_job_id(),
        actor=actor,
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=None,
        metadata=metadata,
        max_pending=max_pending,
        identity_key=identity_key,
        unique_for=unique_for,
    )


async def _seed_job(conn: asyncpg.Connection, schema: str, actor: str) -> None:
    """One pending row for *actor* — committed or in the caller's open tx."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, metadata) "
        "VALUES ($1, $2, 'default', '{}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp(), $3::jsonb)",
        new_job_id(),
        actor,
        '{"singleton": true}' if actor.startswith("singleton") else "{}",
    )


async def _assert_caller_tx_usable(conn: asyncpg.Connection, context: str) -> None:
    """The caller's transaction must still accept statements after a typed refusal."""
    try:
        await _bounded(conn.execute("SELECT 1"))
    except asyncpg.PostgresError as exc:
        pytest.fail(
            f"contract violated ({context}): enqueue_with_conn raised a typed, "
            "catchable refusal but left the CALLER's transaction aborted — "
            f"the follow-up SELECT 1 failed with {type(exc).__name__}: {exc}. "
            "The caller owns this transaction; a refusal it is meant to catch "
            "and continue from must not poison it (savepoint-protect the "
            "enqueue's statements or roll back to a savepoint before raising)."
        )


async def test_singleton_preflight_refusal_keeps_caller_transaction_usable(
    pg_dsn: str,
) -> None:
    """GREEN pin: the preflight-path singleton refusal is a plain Python raise
    after a SELECT — no statement error — so the caller's transaction stays
    usable. The violation-catch path (next test) must converge to this."""
    schema = await _fresh_schema(pg_dsn)
    backend = _backend(pg_dsn, schema)
    actor = "singleton_preflight_actor"
    seeder = await asyncpg.connect(pg_dsn)
    caller = await asyncpg.connect(pg_dsn)
    try:
        await _seed_job(seeder, schema, actor)  # committed blocker
        tx = caller.transaction()
        await tx.start()
        with pytest.raises(SingletonCollisionError):
            await _bounded(backend.enqueue_with_conn(caller, _args(actor, singleton=True)))
        await _assert_caller_tx_usable(
            caller, "singleton preflight refusal inside the caller's transaction"
        )
        await tx.rollback()
        await _bounded(caller.execute("SELECT 1"))
    finally:
        await seeder.close()
        await caller.close()


async def test_singleton_violation_refusal_keeps_caller_transaction_usable(
    pg_dsn: str,
) -> None:
    """RED: the unique-violation-catch singleton refusal aborts the caller's tx.

    The caller holds the transaction; a racing transaction inserts the
    singleton row UNCOMMITTED (invisible to our READ COMMITTED
    preflight), then commits while our INSERT waits on the partial
    unique index. The INSERT's UniqueViolationError is a statement
    error: Postgres aborts the CALLER's transaction, and converting the
    error to the typed SingletonCollisionError does not undo that. The
    same typed error raised from the preflight leaves the transaction
    usable — so a caller that catches SingletonCollisionError and moves
    on works or breaks depending on which detection path fired, with no
    signal from the API. The desired observable: the catchable refusal
    leaves the caller's transaction usable.
    """
    schema = await _fresh_schema(pg_dsn)
    backend = _backend(pg_dsn, schema)
    actor = "singleton_violation_actor"
    racer = await asyncpg.connect(pg_dsn)
    caller = await asyncpg.connect(pg_dsn, command_timeout=10)
    try:
        racer_tx = racer.transaction()
        await racer_tx.start()
        await _seed_job(racer, schema, actor)  # uncommitted → invisible to preflight
        caller_tx = caller.transaction()
        await caller_tx.start()

        task = asyncio.create_task(backend.enqueue_with_conn(caller, _args(actor, singleton=True)))
        await asyncio.sleep(0.15)  # let the caller's INSERT reach its blocked wait
        await racer_tx.commit()  # the wait ends in UniqueViolationError(jobs_singleton_uniq)

        with pytest.raises(SingletonCollisionError):
            await asyncio.wait_for(task, timeout=_BOUNDED_WAIT_S)

        await _assert_caller_tx_usable(
            caller, "singleton unique-violation refusal inside the caller's transaction"
        )
        await caller_tx.rollback()
    finally:
        await racer.close()
        await caller.close()


async def test_cap_refusal_keeps_caller_transaction_usable(pg_dsn: str) -> None:
    """GREEN pin: a max_pending refusal raised after the count SELECT (a
    Python raise, no statement error) leaves the caller's open transaction
    usable — the shape the singleton red must converge to."""
    schema = await _fresh_schema(pg_dsn)
    backend = _backend(pg_dsn, schema)
    actor = "capped_actor"
    seeder = await asyncpg.connect(pg_dsn)
    caller = await asyncpg.connect(pg_dsn)
    try:
        await _seed_job(seeder, schema, actor)
        await _seed_job(seeder, schema, actor)  # 2 pending rows
        tx = caller.transaction()
        await tx.start()
        with pytest.raises(MaxPendingExceededError):
            await _bounded(backend.enqueue_with_conn(caller, _args(actor, max_pending=2)))
        await _assert_caller_tx_usable(
            caller, "max_pending refusal inside the caller's transaction"
        )
        await tx.rollback()
        await _bounded(caller.execute("SELECT 1"))
    finally:
        await seeder.close()
        await caller.close()


async def test_unique_for_lock_budget_exhaustion_returns_bare_caller_conn_clean(
    pg_dsn: str,
) -> None:
    """GREEN pin: a budget-exhausted unique_for wait hands the BARE caller
    conn back exactly as found — not in a transaction, ``lock_timeout`` at
    its session default.

    The bounded advisory acquire sets ``lock_timeout`` via set_config(...,
    true) inside a savepoint; the 55P03 rollback restores it before the
    typed UniqueForLockTimeoutError raises, and the enqueue's own wrapper
    transaction rolls back. A refactor away from the savepoint (or a
    second GUC set outside it) would leak the 250 ms bound onto the
    caller's session — every later statement of the caller would run
    under a lock wait budget it never chose.
    """
    schema = await _fresh_schema(pg_dsn)
    actor = "unique_for_actor"
    identity_key = "idem-1"
    lock_key = f"taskq:unique_for:{schema}:{actor}:{identity_key}"
    sql: SqlTemplates = render(schema)
    racer = await asyncpg.connect(pg_dsn)
    caller = await asyncpg.connect(pg_dsn)
    try:
        racer_tx = racer.transaction()
        await racer_tx.start()
        await racer.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lock_key)

        with pytest.raises(UniqueForLockTimeoutError):
            await _bounded(
                _enqueue_with_conn(
                    caller,
                    sql,
                    schema,
                    SystemClock(),
                    _args(actor, identity_key=identity_key, unique_for=timedelta(seconds=60)),
                    unique_for_lock_timeout_ms=250,
                )
            )

        assert caller.is_in_transaction() is False, (
            "the caller handed enqueue_with_conn a BARE connection; after the "
            "typed UniqueForLockTimeoutError the conn must be back outside "
            "any transaction — a dangling transaction would hold the "
            "advisory lock and pin the caller's next use."
        )
        lock_timeout_after = await _bounded(
            caller.fetchval("SELECT current_setting('lock_timeout')")
        )
        assert lock_timeout_after == "0", (
            "the bounded advisory acquire set lock_timeout=250ms via "
            "set_config(local=true) inside a savepoint; the savepoint "
            f"rollback must restore it, but the session reads {lock_timeout_after!r} "
            "— the wait budget leaked onto the caller's session and now "
            "bounds every statement the caller issues."
        )
        await _bounded(caller.execute("SELECT 1"))
        await racer_tx.rollback()
    finally:
        await racer.close()
        await caller.close()
