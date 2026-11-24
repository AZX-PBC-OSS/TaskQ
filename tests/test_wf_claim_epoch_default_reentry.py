"""An unsupplied (or zero) ``claim_epoch`` is a loud defect, never a silent no-op.

``build_step_context(claim_epoch=…)`` and ``StepContext.claim_epoch`` once
defaulted to ``0``, while the only concrete provider of ``_SpawnHost`` /
``WaitHost`` is ``StepContext`` and both production construction sites pass a
real claim epoch. A caller that omitted the keyword got ``0``, and both of this
release's cures degraded without a single error:

* ``reassert_hold``'s fence is ``claim_epoch = $4`` — a row under a real claim
  (epoch >= 1) matches nothing, so the re-park writes nothing and the node stays
  ``running`` with nothing executing it (issue #748's own wedge), while the
  caller still raises ``NodeHeldError`` and the runner logs ``node.held`` as if
  the held representation stood;
* ``_SPAWN_SLOT_CLAIM_SQL``'s ``CASE WHEN stored_epoch >= $3`` is then always
  true, so the cursor never resets and every re-claim derives a fresh slot —
  the duplicate-child bug the per-claim cursor exists to prevent.

The default is GONE: the factory requires the keyword (an omission is a
``TypeError`` at the construction), and the two consumers — ``reassert_hold``
and ``spawn_flow``'s slot claim — refuse a non-positive epoch with a
``WorkflowRunError`` naming the real claim epoch. This module drives the REAL
door (``build_step_context`` -> ``ctx.wait_signal`` -> ``_hitl.reassert_hold``)
and asserts on the row the door writes, so the symptom is the production one
rather than a mock's.

The state staged below is the *reachable* re-entry state (the one the shipped
control ``tests/test_wf_untimed_join_fleet_control.py`` builds through a lease
lapse): a node row claimed at epoch N with a standing ``held`` signal whose
stamp the previous claim never wrote.
"""

# ruff: noqa: S608  # Why: the f-strings interpolate only throwaway schema identifiers; every value is $-bound.

from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.migrate import apply_pending
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.api._ctx import StepContext, build_step_context
from taskq.workflows.api._ctx_wait import NodeHeldError
from taskq.workflows.api._runner_errors import WorkflowRunError
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import seed_flow, seed_running_node

pytestmark = pytest.mark.integration

#: The epoch a real claim grants on a fresh row: both claim doors write
#: ``claim_epoch = claim_epoch + 1`` (``_sql_runner.NODE_CLAIM_SQL_TEMPLATE``,
#: ``_dispatch_sql``'s terminal claim UPDATE), so an executing row never sits at
#: epoch 0.
GRANTED_EPOCH = 1


class Decision(BaseModel):
    """The wait site's declared payload contract (a hold never mints without one)."""

    approved: bool = True


@pytest_asyncio.fixture
async def reentry(
    pg_dsn: str,
) -> AsyncIterator[tuple[asyncpg.Connection, str, WorkflowSql, JobId, JobId]]:
    """A schema, a flow, and one node row claimed at ``GRANTED_EPOCH``."""
    control = await asyncpg.connect(pg_dsn)
    schema = "epoch_def_" + new_base62(8, precision="random").lower()
    await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(control, schema=schema, phase="pre")
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        flow = await seed_flow(control, schema)
        node = await seed_running_node(control, schema, flow, step_key="source")
        # the claim's own grant (the seed writes the finalize pin's epoch 0).
        await control.execute(
            f'UPDATE "{schema}".jobs SET claim_epoch = $2 WHERE id = $1', node, GRANTED_EPOCH
        )
        yield control, schema, render_workflow_sql(schema), flow, node
    finally:
        await pool.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()


def _kwargs(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    flow_id: JobId,
    node_id: JobId,
    *,
    claim_epoch: int | None,
) -> dict[str, object]:
    """The runner's own construction as keyword data, so a case can OMIT the
    required epoch and observe the factory's refusal."""
    kwargs: dict[str, object] = {
        "flow_id": flow_id,
        "job_id": node_id,
        "node_key": "source",
        "attempt": 1,
        "pool": pool,
        "wsql": wsql,
        "input": None,
    }
    if claim_epoch is not None:
        kwargs["claim_epoch"] = claim_epoch
    return kwargs


def _ctx(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    flow_id: JobId,
    node_id: JobId,
    *,
    claim_epoch: int,
) -> StepContext:
    """The runner's own construction with the claim epoch supplied."""
    return build_step_context(
        **_kwargs(pool, wsql, flow_id, node_id, claim_epoch=claim_epoch),  # pyright: ignore[reportArgumentType]  # Why: the dict is the construction under test.
    )


async def _mint_one_standing_hold(
    pool: asyncpg.Pool, wsql: WorkflowSql, flow_id: JobId, node_id: JobId
) -> str:
    """Stage the reachable re-entry state: a standing ``held`` signal on the
    node, with the node row ``running`` and unstamped (what a lease reclaim
    between the mint's signal INSERT and a later claim leaves behind).

    The mint runs through the real door at the granted epoch (so the hold row
    is production's), then the row is restored to the running shape the
    successor claim sees — ``register_hold``'s node UPDATE is what parks it, so
    only that part is undone, never the signal row."""
    conn = await pool.acquire()
    try:
        ctx = _ctx(pool, wsql, flow_id, node_id, claim_epoch=GRANTED_EPOCH)
        with pytest.raises(NodeHeldError):
            await ctx.wait_signal(Decision, reason="stage the standing hold")
    finally:
        await pool.release(conn)
    hold_id: str = await pool.fetchval(
        f'SELECT id::text FROM "{wsql.schema}".wf_signals '
        "WHERE workflow_id = $1 AND node_key = $2 AND status = 'held'",
        flow_id,
        "source",
    )
    assert hold_id
    # back to the running shape a successor claim sees: unstamped, running.
    await pool.execute(
        f'UPDATE "{wsql.schema}".jobs '
        "SET status = 'running', locked_by_worker = $2::uuid, lock_expires_at = now() + interval '60 seconds', "
        "scheduled_at = now(), metadata = metadata - 'hold' - 'held_signal', budget_paused = false "
        "WHERE id = $1",
        node_id,
        new_uuid(),
    )
    return hold_id


async def _row(pool: asyncpg.Pool, schema: str, node_id: JobId) -> tuple[str, str | None]:
    rec = await pool.fetchrow(
        f"SELECT status, metadata->>'hold' AS hold FROM \"{schema}\".jobs WHERE id = $1", node_id
    )
    assert rec is not None
    return str(rec["status"]), rec["hold"]


async def test_an_unsupplied_claim_epoch_is_refused_at_the_factory(
    reentry: tuple[asyncpg.Connection, str, WorkflowSql, JobId, JobId],
    pg_dsn: str,
) -> None:
    """The factory has no default: omitting the keyword is a ``TypeError`` at
    construction, before any door can be reached."""
    _control, _schema, wsql, flow, node = reentry
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        with pytest.raises(TypeError) as refused:
            build_step_context(
                **_kwargs(pool, wsql, flow, node, claim_epoch=None),  # pyright: ignore[reportArgumentType]  # Why: the omission IS the case under test.
            )
        assert "claim_epoch" in str(refused.value), refused.value
    finally:
        await pool.close()


async def test_a_zero_claim_epoch_is_refused_by_the_re_park(
    reentry: tuple[asyncpg.Connection, str, WorkflowSql, JobId, JobId],
    pg_dsn: str,
) -> None:
    """A context carrying the old default ``0`` cannot park the row it just
    found a standing hold for — the refusal is loud, and the row is untouched
    (`running`, no stamp), so the failure is visible instead of a node with
    nothing executing it wearing the held representation."""
    _control, _schema, wsql, flow, node = reentry
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        hold_id = await _mint_one_standing_hold(pool, wsql, flow, node)
        assert await _row(pool, wsql.schema, node) == ("running", None)

        ctx = _ctx(pool, wsql, flow, node, claim_epoch=0)
        with pytest.raises(WorkflowRunError) as refused:
            await ctx.wait_signal(Decision, reason="re-enter onto the standing hold")
        assert "claim_epoch" in str(refused.value), refused.value
        assert str(hold_id) in str(refused.value), refused.value

        # nothing was parked, and nothing claims the row is held
        status, stamp = await _row(pool, wsql.schema, node)
        assert (status, stamp) == ("running", None), (status, stamp)
    finally:
        await pool.close()


async def test_the_supplied_claim_epoch_parks_the_same_row(
    reentry: tuple[asyncpg.Connection, str, WorkflowSql, JobId, JobId],
    pg_dsn: str,
) -> None:
    """The contrast that isolates the default: the identical staged state, the
    identical call, the epoch supplied as both production sites supply it."""
    _control, _schema, wsql, flow, node = reentry
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        hold_id = await _mint_one_standing_hold(pool, wsql, flow, node)

        ctx = _ctx(pool, wsql, flow, node, claim_epoch=GRANTED_EPOCH)
        with pytest.raises(NodeHeldError):
            await ctx.wait_signal(Decision, reason="re-enter onto the standing hold")

        assert await _row(pool, wsql.schema, node) == ("pending", hold_id)
        # and the stamp's sibling key came along (the held representation is one write)
        rec = await pool.fetchrow(
            f"SELECT metadata->>'held_signal' AS s FROM \"{wsql.schema}\".jobs WHERE id = $1", node
        )
        assert rec is not None and rec["s"] == "Decision"
    finally:
        await pool.close()
