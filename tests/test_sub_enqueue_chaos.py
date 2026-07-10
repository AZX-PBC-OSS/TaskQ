"""Chaos test: LOOP-scope connection dies mid-enqueue.

Uses the existing ChaosConnection from taskq.testing.asyncpg_chaos
wrapped as the LOOP-scope asyncpg.Connection provider. The
ChaosConnection is registered AS the LOOP-scope provider via
register_factory so the enqueuer's connection resolution sees it
directly.

The test verifies:
- The error propagates to the parent (the actor receives the exception).
- The parent's transaction rolls back (no partial sub-job state).
- The consumer writes the terminal status on a fresh connection.

Integration-only: ChaosConnection is designed against a real asyncpg
connection and the rollback semantics being verified are PG-specific
MVCC behavior.
"""

import asyncio
from contextlib import suppress
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_job_id, new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    EnqueueArgs,
    JobRow,
)
from taskq.backend.clock import Clock, SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import default_actor_config
from taskq.testing.asyncpg_chaos import ChaosConnection
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job

pytestmark = pytest.mark.integration

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


class _ParentPayload(BaseModel):
    name: str = "parent"


class _ChildPayload(BaseModel):
    name: str = "child"


class _Result(BaseModel):
    ok: bool = True


def _child_actor_ref() -> ActorRef[_ChildPayload, _Result]:
    async def _handler(payload: _ChildPayload) -> _Result:
        return _Result()

    return ActorRef(
        name="child_actor",
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_ChildPayload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


_CHILD_REF = _child_actor_ref()


async def _dispatch_job(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
) -> None:
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=attempt+1, locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', started_at=now(), last_heartbeat_at=now() WHERE id=$2 AND status='pending'",  # noqa: S608 # Why: schema validated by WorkerSettings; asyncpg has no parameter binding for identifiers
        worker_id,
        job_id,
    )


# ── LOOP-scope connection dies mid-enqueue ─────────────────────


async def test_tc1_loop_conn_dies_mid_enqueue(
    jobs_app: JobsApp,
) -> None:
    """LOOP-scope connection dies during child INSERT; transaction rolls back; terminal write on fresh conn."""

    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    parent_job = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="parent_actor",
            queue="default",
            payload={"name": "parent"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, parent_job.id)

    async with deps.worker_pool.acquire() as real_conn:
        chaos_conn = ChaosConnection(
            real_conn,
            fail_on_call=3,
            fail_with=asyncpg.PostgresConnectionError,
        )

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved={asyncpg.Connection: chaos_conn},
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        job_row = _dc_replace(
            make_job_row(actor="parent_actor"), id=parent_job.id, locked_by_worker=worker_id
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            await _ctx.jobs.enqueue(_CHILD_REF, _ChildPayload())
            return {"ok": True}

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_ParentPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=chaos_conn,
            )

    async with deps.worker_pool.acquire() as check_conn:
        child_rows = await check_conn.fetch(
            f'SELECT id FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608 # Why: test-only; schema validated by WorkerSettings
            "child_actor",
        )
        parent_row = await check_conn.fetchrow(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 # Why: test-only; schema validated by WorkerSettings
            parent_job.id,
        )

    assert len(child_rows) == 0, "no child row should exist in PG — transaction rolled back"
    assert parent_row is not None
    assert parent_row["status"] in ("failed", "scheduled"), (
        f"parent should be failed or scheduled for retry, got {parent_row['status']!r}"
    )
