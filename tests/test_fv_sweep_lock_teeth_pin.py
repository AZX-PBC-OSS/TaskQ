"""FV-DELIVERABLE — THE SWEEP LOCK-FIRST TEETH PIN (the findings-validation round).

THE LAW: the sweep's lock-first re-derive must SKIP a join-wait child
whose row lock is HELD by an in-flight transaction (the finalize's tx1/tx2
holds the child's row lock across the count) — never block on it, never
reconcile it from a stale snapshot.

THE VALIDATED GAP (the validator's mutation drill, head b7774945):
stripping ``FOR UPDATE SKIP LOCKED`` from REDERIVE_SWEEP_SQL and
SWEEP_FIRE_SQL leaves ALL 15 sweep+finalize pins GREEN — no pin holds a
child lock across a sweep pass, so the lock-first claim had no teeth.
THIS pin is the tooth: with the shipped statement the sweep skips the
locked child this pass (no block, no reconcile) and the NEXT pass (the
lock released) heals it; the mutant (plain FOR UPDATE — no SKIP LOCKED)
BLOCKS on the held lock and blows the lock budget → this pin reds.

Run the drill: delete ``SKIP LOCKED`` from both sweep statements in
``_sql_sweep.py`` and re-run — this pin must go red while the legacy 15
stay green.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import sweep_join_rederive


@pytest.mark.integration
async def test_child_lock_held_across_a_sweep_pass_is_skipped_then_healed(
    module_pg_schema: Any, module_pg_pool: asyncpg.Pool
) -> None:
    """THE TOOTH: hold the child's row lock across a sweep pass — the
    sweep must SKIP it (no block within the lock budget, counter
    untouched this pass); the lock released, the NEXT pass heals the
    counter. The no-SKIP-LOCKED mutant blocks and times out."""
    schema = module_pg_schema.schema_name
    wsql = WorkflowSql.build(schema)
    flow_id, parent_id, child_id = new_uuid(), new_uuid(), new_uuid()
    meta = json.dumps({"flow_id": str(flow_id), "blocking_reason": "join"})
    async with module_pg_pool.acquire() as c:
        await c.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', "
            "'__flow__', $2, 'workflow-run', $3)",
            flow_id,
            json.dumps({"flow_id": str(flow_id)}),
            f"flow:{flow_id}",
        )
        await c.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', "
            "'stage1', $2, $3, $4)",
            parent_id,
            json.dumps({"flow_id": str(flow_id)}),
            f"flow:{flow_id}:stage1",
            str(parent_id),
        )
        # the child: join-wait, a STALE counter (deps_pending=5, the edge
        # ledger's truth is 1) — the pass must WANT to reconcile it (that
        # is the write the lock protects), scheduled in the PAST
        # (held-row exclusivity satisfied — the arm's only timer is live)
        await c.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, deps_pending, metadata, scheduled_at) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'pending', 'joined', "
            f"5, $2::jsonb, now() - interval '1 hour')",
            child_id,
            meta,
        )
        await c.execute(
            f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id) '
            "VALUES ($1, $2, $3)",
            child_id,
            parent_id,
            flow_id,
        )

    # THE HELD LOCK: one connection, a tx, the child's row lock held.
    async with module_pg_pool.acquire() as holder:
        await holder.execute("BEGIN")
        await holder.execute(
            f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE', child_id
        )

        # THE PASS ACROSS THE HELD LOCK: a short lock budget — a blocking
        # statement (the mutant) blows it; the shipped SKIP LOCKED never
        # waits at all.
        sweeper_pool = await asyncpg.create_pool(
            module_pg_schema.pg_dsn, min_size=1, max_size=1
        )
        try:
            sweeper = await sweeper_pool.acquire()
            await sweeper.execute("SET lock_timeout = '250ms'")
            await sweeper.execute("SET statement_timeout = '2s'")
            await sweeper_pool.release(sweeper)  # the session settings ride the pooled conn
            started = time.monotonic()
            result = await asyncio.wait_for(
                sweep_join_rederive(sweeper_pool, wsql), timeout=10.0
            )
            elapsed = time.monotonic() - started
            _ = result
            # The skip, not the block: the pass returned under the budget…
            assert elapsed < 2.0, (
                f"the sweep BLOCKED on the held child lock for {elapsed:.2f}s — "
                "the FOR UPDATE SKIP LOCKED leg is missing (the mutant's shape)"
            )
            # …and the locked child was NOT reconciled this pass (the
            # counter untouched — the reconcile waits for the lock owner).
            untouched = await module_pg_pool.fetchval(
                f'SELECT deps_pending FROM "{schema}".jobs WHERE id = $1', child_id
            )
            assert untouched == 5, (
                f"the sweep reconciled a child whose row lock it does NOT hold "
                f"(deps_pending={untouched} after a pass across the held lock) — "
                "the snapshot-sweep dragon (the stale-counter write) is loose"
            )
        finally:
            await sweeper_pool.close()

        # RELEASE: the lock owner's tx ends.
        await holder.execute("ROLLBACK")

    # THE NEXT PASS heals: terminalize the parent, the child's count
    # re-derives from the edge ledger → deps_pending = 0.
    await module_pg_pool.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'succeeded' WHERE id = $1", parent_id
    )
    await sweep_join_rederive(module_pg_pool, wsql)
    final = await module_pg_pool.fetchval(
        f'SELECT deps_pending FROM "{schema}".jobs WHERE id = $1', child_id
    )
    assert final == 0, f"the released child's next pass did not heal (deps_pending={final})"
