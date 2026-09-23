# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""RED-TEAM pins: crash redelivery is at-least-once, side effects stay fenced (S5).

The documented delivery contract (``docs/guides/ops.md``: "Delivery is
at-least-once. A worker that dies mid-job has its jobs reclaimed after the
lock lease expires and retried"; ``docs/architecture.md``: the same): a
worker crash mid-execution REDIVERS the job and it runs AGAIN. The
idempotency key is an ENQUEUE-dedup channel, not an execution-dedup channel -
it must not block the redelivery, and the redelivery must not duplicate the
row. What must NOT survive the crash are the claimed side effects: a stale
attempt's progress flush and a stale attempt's terminal write are fenced out
by the attempt epoch, so the redelivered attempt's writes are the only ones
that land.

Pinned against real Postgres, one deterministic crash (the lock expiry the
reclaim sweep arbitrates, the same boundary a real process death reaches):

* the redelivery claims the SAME job id and runs it again (at-least-once),
* still exactly one row for the idempotency pair (no enqueue-side duplicate),
* a stale attempt's progress flush is fenced out (no double progress write),
* a stale attempt's terminal write is fenced out (no double result).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.progress._flush import _flush_update_sql
from taskq.testing.fixtures import _open_pg_backend
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_GRACE = timedelta(seconds=30)


async def _fresh_backend(pg_dsn: str) -> tuple[Any, Any, Any, str]:
    """A backend on a fresh schema: ``(stack, deps, backend, schema)``."""
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    return stack, deps, backend, schema


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


async def _flush_progress(
    deps: Any, schema: str, job_id: Any, attempt: int, worker_id: Any
) -> list[Any]:
    """Run one bounded progress-flush UPDATE for a single row."""
    async with deps.worker_pool.acquire() as conn:
        return list(
            await conn.fetch(
                _flush_update_sql(schema),
                [job_id],
                [1],
                ['{"step": 1}'],
                [attempt],
                worker_id,
            )
        )


async def test_crash_redelivery_runs_again_and_stale_writes_stay_fenced(pg_dsn: str) -> None:
    """Worker crash mid-execution with an idempotency key: the reclaim sweep
    re-pends the job, the redelivery runs it AGAIN on the same id (the
    documented at-least-once contract, the key dedupes enqueue not
    execution), the row count never doubles, and the dead attempt's progress
    and terminal writes are fenced out."""
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    key = f"rt-crash-{new_base62()}"
    worker_id = new_uuid()
    try:
        args = make_enqueue_args(idempotency_key=key)
        row = await backend.enqueue(args)

        claimed1 = await backend.dispatch_batch(worker_id, ["default"], 10, _LEASE)
        assert [j.id for j in claimed1] == [row.id], "the first claim takes the job"
        attempt1 = claimed1[0].attempt
        assert claimed1[0].claim_epoch is not None

        # The crash: the holder dies without a terminal write and without a
        # heartbeat; the per-worker lease lapses.
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET lock_expires_at = clock_timestamp() - '
                "interval '10 seconds' WHERE id = $1",
                row.id,
            )
            reclaimed = await PostgresBackend.sweep_expired_locks(
                conn,
                _GRACE,
                _GRACE,
                schema=schema,
            )
            assert reclaimed >= 1, "the crashed job must be reclaim-eligible"
            # The re-pend's scheduled_at carries the retry backoff; backdate
            # it so the redelivery claim below is immediate.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - '
                "interval '1 second' WHERE id = $1",
                row.id,
            )

        # The redelivery: SAME job id, a later attempt epoch.
        claimed2 = await backend.dispatch_batch(worker_id, ["default"], 10, _LEASE)
        assert [j.id for j in claimed2] == [row.id], (
            f"CONTRACT (at-least-once): the crash redelivery re-claims the SAME "
            f"job id {row.id}; got {[str(j.id) for j in claimed2]} - a fresh id "
            f"here would be a duplicate side effect."
        )
        attempt2 = claimed2[0].attempt
        assert attempt2 is not None and attempt1 is not None and attempt2 > attempt1, (
            f"the redelivery is a NEW attempt epoch: {attempt1} -> {attempt2}"
        )

        async with deps.worker_pool.acquire() as conn:
            pair_rows: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                "WHERE idempotency_scope = $1 AND idempotency_key = $2",
                "",
                key,
            )
        assert pair_rows == 1, (
            f"CONTRACT: the idempotency key dedupes the ENQUEUE, never the "
            f"execution - exactly one row survives the crash redelivery; got "
            f"{pair_rows}."
        )

        # Side-effect fence 1: the dead attempt's progress flush. The flush
        # UPDATE is fenced per row on (running, worker, attempt epoch); the
        # stale epoch matches nothing and writes nothing.
        stale_flush = await _flush_progress(deps, schema, row.id, attempt1, worker_id)
        assert stale_flush == [], (
            f"CONTRACT: the crashed attempt's progress flush is fenced out "
            f"(stale epoch {attempt1}); it returned {stale_flush} - a double "
            f"progress write."
        )
        live_flush = await _flush_progress(deps, schema, row.id, attempt2, worker_id)
        assert len(live_flush) == 1 and live_flush[0]["id"] == row.id, (
            f"the live epoch's flush writes: got {live_flush}"
        )
        assert live_flush[0]["progress_seq"] == 1, "the flush advanced the seq exactly once"

        # Side-effect fence 2: the dead attempt's terminal write. The stale
        # attempt cannot land its result on the row the redelivery now owns.
        stale_terminal = await backend.mark_succeeded(
            row.id, worker_id, attempt=attempt1, claim_epoch=claimed1[0].claim_epoch
        )
        assert stale_terminal is False, (
            f"CONTRACT: the crashed attempt's terminal write is fenced out "
            f"(stale attempt {attempt1}); it returned {stale_terminal} - a "
            f"double result."
        )
        live_terminal = await backend.mark_succeeded(
            row.id,
            worker_id,
            attempt=attempt2,
            claim_epoch=claimed2[0].claim_epoch,
        )
        assert live_terminal is True, "the live attempt's terminal write lands"
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)
