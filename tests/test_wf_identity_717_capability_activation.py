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
from taskq.workflows._types import MapIndexSchemaError, resolve_map_index_domain
from taskq.workflows.api._runner_ladder import is_deterministic_authoring_failure

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


async def _low_identity_bounded_emit(
    conn: asyncpg.Connection,
    pool: asyncpg.Pool,
    schema: str,
) -> tuple[object, ...]:
    """Drive the shipped low-only bounded emit path.

    No child identity is above ``MAP_INDEX_CEILING``, so ``emit_batch`` never
    calls ``resolve_map_index_domain``; the bounded admission still calls
    ``verify_emit_capacity_objects``. This is the worker shape the stale-cache
    defect exempts from every live width read.
    """
    from taskq.workflows._emit import emit_batch
    from taskq.workflows._types import EmitChild
    from taskq.workflows.engine import render_workflow_sql
    from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)
    children = [
        EmitChild(
            step_key="record",
            actor="wf",
            queue="default",
            payload={"record": index},
            trace_id=f"record-{index}",
            map_index=index,
        )
        for index in (0, 1)
    ]
    return await emit_batch(
        pool,
        wsql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=children,
        cursor={"page": 0},
        max_in_flight=8,
    )


async def _recreate_botched_int4_arbiter(conn: asyncpg.Connection, schema: str) -> None:
    """The hand-run widening's botch: the column is bigint but the arbiter
    still carries the old int4 coercion, so an ON CONFLICT dies 42P10."""
    await conn.execute(f'DROP INDEX "{schema}".wf_step_ledger_claim_uniq')
    await conn.execute(
        f'CREATE UNIQUE INDEX wf_step_ledger_claim_uniq ON "{schema}".wf_step_ledger '
        "USING btree (flow_id, step_key, COALESCE((map_index)::integer, '-1'::integer), "
        "attempt, claim_epoch)"
    )


async def test_low_only_bounded_emit_revalidates_a_botched_widening_before_the_claim(
    pg_dsn: str,
) -> None:
    """A low-only worker's cached pre-POST OK must not survive a hand-run
    widening that skipped the claim-arbiter recreation.

    The worker verifies at the PRE width, the POST migration widens the five
    carriers, and the arbiter is hand-restored to the old int4 rendering. The
    next low-identity bounded emit never resolves a high identity, so no
    authoring-door five-carrier live read runs. The cached smallint verdict
    must nevertheless be compared against the current carrier width and
    dropped, so the re-verify refuses with the typed POST remedy instead of
    letting the next ledger claim die with raw PostgreSQL 42P10.
    """
    schema = "cap_activation_low_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")

        # The low-only worker's pre-POST verdict caches as "smallint".
        await verify_emit_capacity_objects(conn, schema)

        # The POST widens the five carriers and recreates the bigint arbiter.
        await apply_pending(conn, schema=schema, target=POST_MIGRATION)
        # The hand-run botch: the widened column still carries int4 coercion.
        await _recreate_botched_int4_arbiter(conn, schema)

        from taskq.workflows._emit import emit_batch
        from taskq.workflows._types import EmitChild
        from taskq.workflows.engine import render_workflow_sql
        from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

        wsql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema)
        source = await seed_running_node(conn, schema, flow, step_key="source")
        worker, attempt, epoch = await claim_view(conn, schema, source)
        children = [
            EmitChild(
                step_key="record",
                actor="wf",
                queue="default",
                payload={"record": index},
                trace_id=f"record-{index}",
                map_index=index,
            )
            for index in (0, 1)
        ]
        try:
            await emit_batch(
                pool,
                wsql,
                flow_id=flow,
                source_id=source,
                worker_id=worker,
                attempt=attempt,
                claim_epoch=epoch,
                children=children,
                cursor={"page": 0},
                max_in_flight=8,
            )
        except WorkflowWorkerCapabilityError as exc:
            # The fixed path: the width comparison drops the stale OK and the
            # re-verify names the POST file that recreates the arbiter.
            assert "01.00.42_02" in str(exc), str(exc)
        else:
            # RED BY DESIGN: reaching this branch means the stale pre-POST OK
            # was served and the low page was written. The botched arbiter then
            # surfaces at the very next ledger claim as raw PostgreSQL 42P10 —
            # an asyncpg driver error, never the typed capability refusal — so
            # the expectation below cannot hold and this test fails with the
            # defect's own raw error in the traceback.
            from taskq.workflows.ledger import claim_step_ledger

            with pytest.raises(WorkflowWorkerCapabilityError, match=r"01\.00\.42_02"):
                await claim_step_ledger(
                    conn,
                    wsql,
                    flow_id=flow,
                    job_id=source,
                    step_key="record",
                    map_index=0,
                    attempt=1,
                    claim_epoch=0,
                )
    finally:
        reset_emit_capability_cache(schema)
        await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_low_only_bounded_emit_revalidates_a_partial_five_carrier_widening(
    pg_dsn: str,
) -> None:
    """A five-carrier revalidation, not a ``wf_step_ledger``-only one.

    The stale pre-POST OK must not be trusted when a hand-run widening changed
    one of the OTHER four carriers while ``wf_step_ledger`` stayed smallint. A
    single-carrier width read sees the unchanged ledger column, matches the
    cached verdict, and waves the low page through; the five-carrier read sees
    the mixed widths. The resolver raises ``MapIndexSchemaError`` for that
    state, which the emit door must translate to the typed
    ``WorkflowWorkerCapabilityError`` — never let a mixed schema escape
    untranslated and never write the page.
    """
    schema = "cap_activation_partial_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        await verify_emit_capacity_objects(conn, schema)

        # A partial widening: jobs widens, the ledger and the other three
        # carriers stay smallint. A wf_step_ledger-only read cannot see this.
        await conn.execute(f'ALTER TABLE "{schema}".jobs ALTER COLUMN map_index TYPE bigint')

        with pytest.raises(
            WorkflowWorkerCapabilityError, match=r"partially widened/mixed"
        ) as excinfo:
            await _low_identity_bounded_emit(conn, pool, schema)
        # The translation preserves the resolver's condition-specific remedy
        # verbatim: a mixed schema is never misdirected to the PRE migration.
        assert "01.00.42_01" not in str(excinfo.value)
    finally:
        reset_emit_capability_cache(schema)
        await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_the_carrier_translation_stays_a_deterministic_authoring_failure(
    pg_dsn: str,
) -> None:
    """The mixed-carrier translation must keep the resolver's failure class.

    A broken carrier shape is a deployment error no retry can repair, so the
    runner's deterministic leg must see it and terminalize the source on its
    first attempt. That leg reads the embedded leaf through ``__cause__``, and
    the translated ``WorkflowWorkerCapabilityError`` is not itself in the
    deterministic class — the ``from exc`` chain is what carries
    ``MapIndexSchemaError`` across. Dropping that chain (a plausible tidy-up of
    the translation) would turn the same refusal into a body failure that burns
    every bounded run's attempts on a half-migrated schema."""
    schema = "cap_activation_deterministic_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        # A partial widening: the resolver sees the mixed widths and raises.
        await conn.execute(f'ALTER TABLE "{schema}".jobs ALTER COLUMN map_index TYPE bigint')

        with pytest.raises(
            WorkflowWorkerCapabilityError, match=r"partially widened/mixed"
        ) as excinfo:
            await verify_emit_capacity_objects(conn, schema)
        refusal = excinfo.value
        assert isinstance(refusal.__cause__, MapIndexSchemaError), type(refusal.__cause__)
        assert is_deterministic_authoring_failure(refusal) is True, (
            "the mixed-carrier refusal no longer carries the deterministic class: "
            "the retry ladder would re-run a deployment error on every attempt"
        )
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_low_only_bounded_emit_serves_the_correctly_widened_post_schema(
    pg_dsn: str,
) -> None:
    """The same cached-pre-POST/low-only path on a CORRECT POST keeps working:
    the width comparison drops the stale verdict, the bigint arbiter verifies,
    and the low page commits without any high-identity resolve."""
    schema = "cap_activation_low_post_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        await verify_emit_capacity_objects(conn, schema)

        await apply_pending(conn, schema=schema, target=POST_MIGRATION)
        child_ids = await _low_identity_bounded_emit(conn, pool, schema)
        assert len(child_ids) == 2
        assert await verify_emit_capacity_objects(conn, schema) is None
    finally:
        reset_emit_capability_cache(schema)
        await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
