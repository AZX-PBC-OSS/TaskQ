"""The widened identity's activation transition: a stale pre-POST emit-capability
verdict must be invalidated by a live five-carrier width read, and the re-verify
must accept the recreated bigint arbiter.

This is the cross-state path the widening introduces (mechanism 6 of the fix):
a worker that cached the all-smallint object-set verdict before ``01.00.42_02``
POST must not keep serving it after the POST lands. The plain capability pins
cover each width independently; this one covers the in-process transition.

Real PostgreSQL; a throwaway schema built through the product migration runner.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.constants import SMALLINT_MAX
from taskq.migrate import apply_pending
from taskq.workflows._capability import (
    WorkflowWorkerCapabilityError,
    reset_emit_capability_cache,
    verify_emit_capacity_objects,
)
from taskq.workflows._types import resolve_map_index_domain

pytestmark = pytest.mark.integration

POST_MIGRATION = "01.00.42_02"


async def test_pre_post_verdict_is_invalidated_and_the_bigint_arbiter_accepted(
    pg_dsn: str,
) -> None:
    schema = "cap_activation_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")

        # Pre-POST: the all-smallint object set verifies and caches.
        await verify_emit_capacity_objects(conn, schema)
        assert await resolve_map_index_domain(conn, schema) == SMALLINT_MAX

        # The POST widens the five carriers and recreates the claim arbiter.
        await apply_pending(conn, schema=schema, target=POST_MIGRATION)

        # A live five-carrier read observes the new width and must invalidate
        # the stale pre-POST verdict...
        assert await resolve_map_index_domain(conn, schema) > SMALLINT_MAX
        # ...and the re-verify must accept the recreated bigint arbiter (not
        # the stale int4 rendering), so bounded emit work keeps running.
        await verify_emit_capacity_objects(conn, schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_the_invalidation_is_what_forces_the_post_transition_reverify(
    pg_dsn: str,
) -> None:
    """The invalidation itself, not just a transition that happens to work.

    The pin above ends with a re-verify that RE-CACHES an OK, so it still
    passes when ``note_emit_carrier_width`` is mutated to a no-op (verified by
    a mutant run: the stale pre-POST verdict is served and nothing refuses).
    This leg leaves the cache exactly as the live width read left it and then
    installs the botched state a stale verdict would wave through — the
    widened column carrying the OLD int4 arbiter rendering — so the refusal
    below can only come from a re-verify the invalidation forced.
    """
    schema = "cap_activation_bite_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")

        # The pre-POST verdict caches as "smallint".
        await verify_emit_capacity_objects(conn, schema)

        await apply_pending(conn, schema=schema, target=POST_MIGRATION)
        # The live five-carrier read must DROP that cached verdict; nothing
        # re-verifies and re-caches before the botch below.
        assert await resolve_map_index_domain(conn, schema) > SMALLINT_MAX

        await conn.execute(f'DROP INDEX "{schema}".wf_step_ledger_claim_uniq')
        await conn.execute(
            f'CREATE UNIQUE INDEX wf_step_ledger_claim_uniq ON "{schema}".wf_step_ledger '
            "USING btree (flow_id, step_key, COALESCE((map_index)::integer, '-1'::integer), "
            "attempt, claim_epoch)"
        )
        with pytest.raises(WorkflowWorkerCapabilityError, match=r"01\.00\.42_02"):
            await verify_emit_capacity_objects(conn, schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
