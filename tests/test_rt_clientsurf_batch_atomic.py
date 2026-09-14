# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team pins: the client-facing observables of the atomic batch path.

Three observables of ``JobsClient.enqueue_batch``'s single-transaction
arm (``Backend.enqueue_batch_atomic``, src/taskq/backend/_batch_sql.py::
enqueue_batch_atomic) that no existing test drives end-to-end through
the CLIENT:

1. **Duplicate batch_id rolls back the whole second call.** The backend-
   level rollback pin (tests/test_batch_pg.py::
   TestPostgresEnqueueBatchAtomicRollback) fails the transaction via a
   raising GENERATOR mid-stream; the duplicate-batch_id failure is a
   different mid-transaction failure point — every member chunk and the
   finalizer INSERT succeed, then ``create_batch`` hits the
   ``batches`` primary key (src/taskq/backend/_batch_sql.py:613-626) —
   and it is the failure a real caller produces by retrying a batch_id.
   The client docstring promises "If any insert fails, no rows are
   committed" (src/taskq/client/_jobs.py:516-518); this pins that the
   promise holds for the LATE failure too, not just mid-stream ones.

2. **A capped FINALIZER actor refuses the whole atomic batch.** The
   atomic arm documents "a capped finalizer actor must abort the whole
   atomic batch, not raise a one-item partition refusal"
   (src/taskq/backend/_batch_sql.py:595-599, ``refuse_whole_batch_on_cap=True``).
   Unpinned anywhere: the observable is a PLAIN
   :class:`~taskq.exceptions.MaxPendingExceededError` (all-or-nothing,
   matching the client docstring at src/taskq/client/_jobs.py:550-555)
   with zero member rows committed — NOT
   :class:`~taskq.exceptions.BatchMaxPendingExceededError` (the
   partition error whose contract assumes "part of the batch is already
   stored", which would be a lie inside one transaction).

3. **A finalizer idempotency collision records the EXISTING row's id
   (M4).** The atomic wrapper inserts the finalizer BEFORE creating the
   batch row "so the returned row's id can be used for
   finalizer_job_id (M4: idempotency collision may return a different
   id than finalizer_args.id)" (src/taskq/backend/_batch_sql.py:584-586).
   The happy-path finalizer stamping is pinned in-memory
   (tests/test_batch_enqueue.py::test_finalizer_job_id_on_batch_row);
   the collision arm — batch row must name the collided row, the
   finalizer handle must report ``was_existing=True``, and the collided
   row must NOT be retro-stamped with ``batch_id`` metadata — is not.

All three are expected GREEN (documented behavior); any RED here is a
real defect in the atomic arm. PG tier because each observable turns on
real SQL semantics — one transaction's rollback, ON CONFLICT idempotency
dedup, and the cap admission count. Every wait is bounded.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from taskq import actor
from taskq._ids import new_base62, new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.batch import EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._jobs import JobsClient
from taskq.exceptions import BatchIdExistsError, MaxPendingExceededError
from taskq.testing.fixtures import (
    _open_pg_backend,  # pyright: ignore[reportPrivateUsage]  # Why: deliberate test seam — the canonical drop+apply_pending+pools+backend sequence under a caller-chosen schema; no public equivalent.
)
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration

_CALL_BOUND = 20.0
"""Bound on every client call: far above any healthy statement on the
test container, low enough that a wedged call fails the test by name."""


class _Payload(BaseModel):
    value: int = 0


@actor(name="rt_cs_plain_member")
async def _plain_member(_payload: _Payload) -> None:
    """Uncapped member/finalizer stand-in."""


@actor(name="rt_cs_capped_finalizer", max_pending=1)
async def _capped_finalizer(_payload: _Payload) -> None:
    """Finalizer actor whose max_pending=1 cap is one job deep."""


@dataclass
class _TcsApp:
    deps: WorkerDeps
    backend: PostgresBackend
    client: JobsClient
    schema: str

    async def count_jobs_with_batch(self, conn: asyncpg.Connection, batch_id: UUID) -> int:
        return await self._count_jobs(conn, {"batch_id": str(batch_id)})

    async def _count_jobs(self, conn: asyncpg.Connection, containment: dict[str, str]) -> int:
        value: int = await conn.fetchval(
            f'SELECT count(*) FROM "{self.schema}".jobs WHERE metadata @> $1::jsonb',
            json.dumps(containment),
        )
        return value

    async def count_actor_jobs(self, conn: asyncpg.Connection, actor_name: str) -> int:
        value: int = await conn.fetchval(
            f'SELECT count(*) FROM "{self.schema}".jobs WHERE actor = $1', actor_name
        )
        return value


@pytest_asyncio.fixture
async def tcs_app(pg_dsn: str) -> AsyncIterator[_TcsApp]:
    """A JobsClient on a PostgresBackend over a fresh random ``tcs_``
    schema (drop CASCADE + apply_pending happen inside _open_pg_backend,
    the same sequence the jobs_app fixture uses)."""
    schema = f"tcs_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        yield _TcsApp(deps=deps, backend=backend, client=JobsClient(backend), schema=schema)
    finally:
        await stack.aclose()
        with contextlib.suppress(Exception):
            conn = await asyncpg.connect(pg_dsn)
            try:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                await conn.close()


def _member(payload_value: int = 0, **kwargs: Any) -> EnqueueItem:
    return EnqueueItem(actor_ref=_plain_member, payload=_Payload(value=payload_value), **kwargs)


async def test_duplicate_batch_id_rolls_back_the_whole_second_call(tcs_app: _TcsApp) -> None:
    """PIN: retrying a caller-chosen batch_id raises BatchIdExistsError and
    commits NOTHING from the second call — not even the rows its member
    chunks already inserted inside the still-open transaction.

    The client contract (src/taskq/client/_jobs.py:516-518): 'the entire
    operation (batch row + all child jobs + finalizer) is inserted in a
    single transaction ... If any insert fails, no rows are committed.'
    The duplicate-batch_id failure fires at create_batch — AFTER all
    member and finalizer inserts succeeded (src/taskq/backend/
    _batch_sql.py:604-626) — so this is the arm where a rollback bug
    would strand an orphaned member set under a foreign batch id."""
    app = tcs_app
    batch_id = new_uuid()

    first = await asyncio.wait_for(
        app.client.enqueue_batch(
            [_member(1)],
            batch_id=batch_id,
            failure_policy=AbortBatchAfter(2),
        ),
        timeout=_CALL_BOUND,
    )
    assert first.size == 1

    with pytest.raises(BatchIdExistsError):
        await asyncio.wait_for(
            app.client.enqueue_batch(
                [_member(2), _member(3)],
                batch_id=batch_id,
                failure_policy=AbortBatchAfter(2),
                finalizer=_member(4),
            ),
            timeout=_CALL_BOUND,
        )

    async with app.deps.worker_pool.acquire() as conn:
        batch_members = await app._count_jobs(conn, {"batch_id": str(batch_id)})
        plain_jobs = await app.count_actor_jobs(conn, "rt_cs_plain_member")

    assert batch_members == 1, (
        f"CONTRACT: a duplicate batch_id must leave the batch exactly as the "
        f"first call wrote it (1 member); found {batch_members} member rows — "
        f"the second call's members survived the BatchIdExistsError rollback"
    )
    assert plain_jobs == 1, (
        f"CONTRACT: the rolled-back call must leave no finalizer row either — "
        f"the finalizer ('rt_cs_plain_member') was the last statement before "
        f"create_batch; found {plain_jobs} jobs for the actor, expected 1 "
        f"(the first call's member only)"
    )

    batch_row = await asyncio.wait_for(app.backend.get_batch(batch_id), timeout=_CALL_BOUND)
    assert batch_row is not None and batch_row.expected_size == 1, (
        "the pre-existing batches row must be untouched by the failed retry"
    )


async def test_capped_finalizer_actor_refuses_the_whole_atomic_batch(tcs_app: _TcsApp) -> None:
    """PIN: when the FINALIZER actor is over its max_pending cap, the atomic
    path refuses the whole call — plain MaxPendingExceededError, zero
    member rows, no batch row — never the partition error
    (BatchMaxPendingExceededError) whose contract says 'part of the
    batch is already stored'.

    src/taskq/backend/_batch_sql.py:595-599 documents the intent; the
    member chunks insert BEFORE the finalizer's cap check runs
    (src/taskq/backend/_batch_sql.py:549-582 vs :587-601), so this is
    exactly the shape where a missing rollback or a partition-style
    refusal would corrupt the all-or-nothing contract."""
    app = tcs_app

    # Fill the capped finalizer actor's max_pending=1 budget.
    await asyncio.wait_for(
        app.client.enqueue(_capped_finalizer, _Payload()),
        timeout=_CALL_BOUND,
    )
    batch_id = new_uuid()

    from taskq.exceptions import BatchMaxPendingExceededError

    with pytest.raises(MaxPendingExceededError) as excinfo:
        await asyncio.wait_for(
            app.client.enqueue_batch(
                [_member(1), _member(2)],
                batch_id=batch_id,
                failure_policy=AbortBatchAfter(2),
                finalizer=EnqueueItem(actor_ref=_capped_finalizer, payload=_Payload(), metadata={}),
            ),
            timeout=_CALL_BOUND,
        )
    assert not isinstance(excinfo.value, BatchMaxPendingExceededError), (
        "the atomic path must raise the PLAIN MaxPendingExceededError "
        f"(all-or-nothing refusal), got {type(excinfo.value).__name__} — the "
        "partition error promises admitted rows are already stored, which is "
        "false inside one transaction"
    )

    async with app.deps.worker_pool.acquire() as conn:
        plain_jobs = await app.count_actor_jobs(conn, "rt_cs_plain_member")
        capped_jobs = await app.count_actor_jobs(conn, "rt_cs_capped_finalizer")

    assert plain_jobs == 0, (
        f"CONTRACT: both member rows were inserted BEFORE the finalizer's cap "
        f"refusal; only the transaction rollback can remove them. Found "
        f"{plain_jobs} member rows after the refusal — the atomic path leaked "
        f"a partially-admitted batch."
    )
    assert capped_jobs == 1, (
        f"the capped actor must hold exactly its one occupying job; found "
        f"{capped_jobs} — the refused finalizer must not have written"
    )

    batch_row = await asyncio.wait_for(app.backend.get_batch(batch_id), timeout=_CALL_BOUND)
    assert batch_row is None, "no batches row may survive the refused atomic call"


async def test_finalizer_idempotency_collision_records_the_existing_row(tcs_app: _TcsApp) -> None:
    """PIN (M4): a finalizer whose idempotency_key collides with an existing
    job dedups onto that row — the batch row's finalizer_job_id names the
    EXISTING row (not the never-inserted args id), the finalizer handle
    reports was_existing=True, and the existing row is not retro-stamped
    with batch_id metadata.

    src/taskq/backend/_batch_sql.py:584-586 inserts the finalizer before
    creating the batch row precisely so the collided id can be recorded;
    this drives that arm through the client, which no existing test
    does (the in-memory pins cover the no-collision happy path only)."""
    app = tcs_app
    fin_key = "rt-cs-m4-finalizer-key"

    existing = await asyncio.wait_for(
        app.client.enqueue(_plain_member, _Payload(value=7), idempotency_key=fin_key),
        timeout=_CALL_BOUND,
    )
    assert existing.was_existing is False

    handle = await asyncio.wait_for(
        app.client.enqueue_batch(
            [_member(1)],
            finalizer=EnqueueItem(
                actor_ref=_plain_member, payload=_Payload(value=8), idempotency_key=fin_key
            ),
        ),
        timeout=_CALL_BOUND,
    )

    fin = handle.finalizer_handle
    assert fin is not None, "finalizer-only enqueue_batch must return a finalizer handle"
    assert fin.was_existing is True, (
        "CONTRACT (M4): a finalizer deduping onto an existing idempotency_key "
        "must report was_existing=True — a False here means the handle was "
        "paired against the never-inserted args id"
    )
    assert fin.job_id == existing.job_id, (
        f"CONTRACT (M4): the finalizer handle must carry the EXISTING row's id "
        f"{existing.job_id}, got {fin.job_id} — src/taskq/backend/_batch_sql.py:"
        f"584-586 inserts the finalizer first precisely so the collided id "
        f"can be recorded"
    )

    batch_row = await asyncio.wait_for(app.backend.get_batch(handle.batch_id), timeout=_CALL_BOUND)
    assert batch_row is not None
    assert batch_row.finalizer_job_id == existing.job_id, (
        f"CONTRACT (M4): the batches row's finalizer_job_id must name the "
        f"existing collided row {existing.job_id}, got "
        f"{batch_row.finalizer_job_id} — wait_for_batch's auto-exclusion "
        f"keys off this column, so a wrong id makes the finalizer count "
        f"itself as a child (the deadlock the unstamped design prevents)"
    )

    existing_row = await asyncio.wait_for(app.backend.get(existing.job_id), timeout=_CALL_BOUND)
    assert existing_row is not None
    assert existing_row.metadata.get("batch_id") is None, (
        "CONTRACT: the collided existing row must NOT be retro-stamped with "
        "batch_id metadata — stamping it would make wait_for_batch count the "
        "finalizer as its own child"
    )
