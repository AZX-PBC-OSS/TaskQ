"""#717 P3 warm-prepared migration and explicit unsupported-old-SQL proof."""
# ruff: noqa: S608  # Why: only generated throwaway schema identifiers are interpolated; values are bound.

from __future__ import annotations

import asyncio
import contextlib

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.constants import BIGINT_MAX
from taskq.migrate import Migration, MigrationLockTimeoutError, apply_pending
from taskq.workflows._emit import EMIT_MAX_IN_FLIGHT_DEFAULT, emit_batch
from taskq.workflows._fork import insert_fork
from taskq.workflows._sweep import drain_outbox
from taskq.workflows._types import ChildSpec, EmitChild, ForkSpec
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _child(index: int) -> EmitChild:
    return EmitChild(
        step_key="record",
        actor="wf",
        queue="default",
        payload={"record": index},
        trace_id=f"record-{index}",
        map_index=index,
    )


async def test_real_old_schema_warm_pool_heals_during_post_and_conserves_high_page(
    pg_dsn: str,
) -> None:
    schema = "idp3warm_" + new_base62(8, precision="random").lower()
    control = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    cache_probe: asyncpg.Pool | None = None
    try:
        await apply_pending(control, schema=schema, phase="pre")
        wsql = render_workflow_sql(schema)
        flow = await seed_flow(control, schema)
        source = await seed_running_node(control, schema, flow, step_key="source")
        fork_parent = await seed_running_node(control, schema, flow, step_key="fork-parent")
        worker, attempt, epoch = await claim_view(control, schema, source)
        cache_options = {
            "min_size": 1,
            "max_size": 1,
            "statement_cache_size": 512,
            "max_cached_statement_lifetime": 3600.0,
        }
        pool = await asyncpg.create_pool(pg_dsn, **cache_options)  # pyright: ignore[reportCallIssue]  # Why: shipped asyncpg cache options are accepted at runtime.
        cache_probe = await asyncpg.create_pool(pg_dsn, **cache_options)  # pyright: ignore[reportCallIssue]
        assert pool is not None and cache_probe is not None

        # Warm actual emit/claim/terminal statements and a second pool whose
        # statement result row will physically change width during ALTER TYPE.
        low_ids = await emit_batch(
            pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=(_child(0),),
            cursor={"page": 0},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
        claim = await pool.fetchrow(
            wsql.ledger_claim, new_uuid(), flow, low_ids[0], "record", 0, 1, 0
        )
        assert claim is not None
        terminal = await pool.execute(
            wsql.ledger_terminal_by_id,
            claim["id"],
            "succeeded",
            None,
            None,
            None,
            None,
            claim["updated_at"],
        )
        assert terminal == "UPDATE 1"
        warm_result = await cache_probe.fetchrow(
            f'SELECT map_index FROM "{schema}".wf_step_ledger WHERE id = $1',
            claim["id"],
        )
        assert warm_result is not None and warm_result["map_index"] == 0

        # Warm the parallel-array fork and the real outbox drain too, on the
        # exact same pool; these are not deferred to fresh post connections.
        async with pool.acquire() as conn:
            await insert_fork(
                conn,
                wsql,
                flow_id=flow,
                parent_id=fork_parent,
                parent_step_key="fork-parent",
                fork=ForkSpec(
                    children=(
                        ChildSpec(
                            step_key="fork-low",
                            actor="wf",
                            queue="default",
                            map_index=1,
                        ),
                    )
                ),
            )
        await pool.execute(
            wsql.outbox_insert,
            new_uuid(),
            fork_parent,
            flow,
            "outbox-low",
            2,
            '{"actor":"wf","queue":"default","payload":{},"trace_id":"outbox-low"}',
        )
        assert await drain_outbox(pool, wsql) == 1

        before = await control.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE parent_id = $1', source
        )
        await apply_pending(control, schema=schema, phase="post")

        # No fresh process or pool is substituted here: the exact warm pool runs
        # the first high page. No fake failure is installed; whether this shape
        # emits 0A000 is observed from real PG, not inferred. The separate warm
        # result-type statement below is the forced, physical invalidation leg.
        high_ids = await emit_batch(
            pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=(_child(32768),),
            cursor={"page": 1},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
        async with pool.acquire() as conn:
            high_fork, high_join = await insert_fork(
                conn,
                wsql,
                flow_id=flow,
                parent_id=fork_parent,
                parent_step_key="fork-parent",
                fork=ForkSpec(
                    children=(
                        ChildSpec(
                            step_key="fork-high",
                            actor="wf",
                            queue="default",
                            map_index=32768,
                        ),
                    )
                ),
            )
        assert high_join is None and len(high_fork) == 1
        await pool.execute(
            wsql.outbox_insert,
            new_uuid(),
            fork_parent,
            flow,
            "outbox-high",
            32768,
            '{"actor":"wf","queue":"default","payload":{},"trace_id":"outbox-high"}',
        )
        assert await drain_outbox(pool, wsql) == 1
        assert (
            await control.fetchval(
                f'SELECT map_index FROM "{schema}".jobs WHERE id = $1', high_fork[0]
            )
            == 32768
        )
        assert (
            await control.fetchval(
                f'SELECT map_index FROM "{schema}".jobs WHERE step_key = $1',
                "outbox-high",
            )
            == 32768
        )
        assert len(high_ids) == 1 and high_ids[0] != low_ids[0]
        high_claim = await pool.fetchrow(
            wsql.ledger_claim, new_uuid(), flow, high_ids[0], "record", 32768, 1, 0
        )
        assert high_claim is not None and high_claim["job_id"] == high_ids[0]
        assert (
            await pool.execute(
                wsql.ledger_terminal_by_id,
                high_claim["id"],
                "succeeded",
                None,
                None,
                None,
                None,
                high_claim["updated_at"],
            )
            == "UPDATE 1"
        )
        rows = await control.fetch(
            f'SELECT id, map_index FROM "{schema}".jobs WHERE parent_id = $1 ORDER BY map_index',
            source,
        )
        assert int(before or 0) == 1
        assert [row["map_index"] for row in rows] == [0, 32768]
        assert (
            await control.fetchval(
                f"SELECT metadata->'emit_cursor' FROM \"{schema}\".jobs WHERE id = $1", source
            )
            == '{"page": 1}'
        )

        # A distinct independently-warm pool demonstrates the real 0A000 rather
        # than assuming every statement shape invalidates. asyncpg aborts the
        # block, clears its cache, and the following same-pool read succeeds.
        with pytest.raises(asyncpg.InvalidCachedStatementError) as raised:
            async with cache_probe.acquire() as conn, conn.transaction():
                await conn.fetchrow(
                    f'SELECT map_index FROM "{schema}".wf_step_ledger WHERE id = $1',
                    claim["id"],
                )
        assert raised.value.sqlstate == "0A000"
        healed = await cache_probe.fetchrow(wsql.ledger_memoized, flow, "record", 32768)
        assert healed is not None
        assert healed["status"] == "succeeded" and healed["job_id"] == high_ids[0]
        healed_low = await cache_probe.fetchrow(wsql.ledger_memoized, flow, "record", 0)
        assert healed_low is not None and healed_low["status"] == "succeeded"
    finally:
        if pool is not None:
            await pool.close()
        if cache_probe is not None:
            await cache_probe.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()


async def test_verbatim_old_smallint_terminal_and_outbox_array_fail_after_post(
    pg_dsn: str,
) -> None:
    """UNSUPPORTED negative control, never a safe-old-worker claim."""
    schema = "idp3old_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema, phase="pre")
        wsql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema)
        source = await seed_running_node(conn, schema, flow, step_key="source")
        await apply_pending(conn, schema=schema, phase="post")
        # The control below derives the old statement by replacing this exact
        # token: if the production spelling drifts the replace goes vacuous and
        # the control silently runs the NEW statement.
        assert "$5::bigint" in wsql.ledger_claim
        old_claim = wsql.ledger_claim.replace("$5::bigint", "$5")
        await conn.execute(old_claim, new_uuid(), flow, source, "source", 32768, 1, 0)
        # The ledger's map-identity bind rides the claim/memoized statements
        # now (the terminal/fence are id+instant keyed — no map bind): the
        # old smallint-typed variant overflows on the same high identity.
        old_memo = wsql.ledger_memoized.replace("$3::bigint", "$3::smallint")
        with pytest.raises(asyncpg.DataError, match="int16") as terminal_error:
            await conn.fetchrow(old_memo, flow, "source", 32768)
        assert type(terminal_error.value) is asyncpg.DataError
        assert type(terminal_error.value.__cause__) is OverflowError
        ledger = await conn.fetchrow(
            f'SELECT status FROM "{schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND map_index = 32768",
            flow,
        )
        assert ledger is not None and ledger["status"] == "running"

        outbox_id = new_uuid()
        await conn.execute(
            wsql.outbox_insert,
            outbox_id,
            source,
            flow,
            "consumer",
            32768,
            '{"actor":"wf","queue":"default","payload":{},"trace_id":"old-negative"}',
        )
        old_drain = wsql.outbox_drain_consumers.replace("$8::bigint[]", "$8::smallint[]")
        with pytest.raises(asyncpg.DataError, match="int16") as drain_error:
            await conn.execute(
                old_drain,
                [new_uuid()],
                ["wf"],
                ["default"],
                ["{}"],
                [3],
                ["transient"],
                [source],
                [32768],
                ["consumer"],
                ["old-negative"],
                ['{"flow_id":"' + str(flow) + '"}'],
                [f"workflow:{flow}"],
                ["wf:consumer:32768"],
                [0],
                [None],
                [str(flow)],
                [5.0],
                [3600.0],
                ["exponential"],
                [0.2],
            )
        assert type(drain_error.value) is asyncpg.DataError
        assert type(drain_error.value.__cause__) is OverflowError
        assert (
            int(
                await conn.fetchval(
                    f"SELECT count(*) FROM \"{schema}\".jobs WHERE step_key = 'consumer'"
                )
                or 0
            )
            == 0
        )
        assert (
            await conn.fetchval(
                f'SELECT delivered FROM "{schema}".wf_outbox WHERE id = $1', outbox_id
            )
            is False
        )

        # The source with current bigint binds succeeds at the opposite extreme.
        assert BIGINT_MAX > 32767
        repaired_claim = await conn.fetchrow(
            wsql.ledger_claim, new_uuid(), flow, source, "source", 32768, 1, 0
        )
        assert repaired_claim is not None
        repaired = await conn.execute(
            wsql.ledger_terminal_by_id,
            repaired_claim["id"],
            "succeeded",
            None,
            None,
            None,
            None,
            repaired_claim["updated_at"],
        )
        assert repaired == "UPDATE 1"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_lock_wait_rolls_back_all_five_and_retry_applies_atomically(
    pg_dsn: str,
) -> None:
    schema = "idp3lock_" + new_base62(8, precision="random").lower()
    migrator = await asyncpg.connect(pg_dsn)
    blocker = await asyncpg.connect(pg_dsn)
    task: asyncio.Task[list[Migration]] | None = None
    try:
        await apply_pending(migrator, schema=schema, phase="pre")
        await apply_pending(
            migrator, schema=schema, phase="post", target="01.00.37_01"
        )  # only the new identity POST remains pending
        async with blocker.transaction():
            await blocker.execute(f'SELECT count(*) FROM "{schema}".jobs')
            task = asyncio.create_task(
                apply_pending(
                    migrator,
                    schema=schema,
                    phase="post",
                    ddl_lock_timeout=0.05,
                )
            )
            with pytest.raises(MigrationLockTimeoutError, match=r"01\.00\.42_02"):
                await task
            task = None

        observed = await migrator.fetch(
            "SELECT table_name, data_type FROM information_schema.columns "
            "WHERE table_schema = $1 AND column_name = 'map_index' "
            "AND table_name IN ('jobs', 'jobs_archive', 'wf_step_ledger', 'wf_signals', 'wf_outbox')",
            schema,
        )
        assert len(observed) == 5
        assert {row["data_type"] for row in observed} == {"smallint"}
        assert (
            await migrator.fetchval(
                f'SELECT count(*) FROM "{schema}".schema_migrations '
                "WHERE version = '01.00.42_02:post'"
            )
            == 0
        )
        indexes = await migrator.fetch(
            "SELECT c.relname, pg_get_indexdef(c.oid) AS definition "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname IN "
            "('wf_step_ledger_claim_uniq', 'wf_signals_hold_identity_uniq')",
            schema,
        )
        assert len(indexes) == 2
        assert all(")::bigint" not in row["definition"] for row in indexes)

        applied = await apply_pending(migrator, schema=schema, phase="post")
        assert "01.00.42_02:post" in {migration.key for migration in applied}
        healed = await migrator.fetch(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = $1 AND column_name = 'map_index' "
            "AND table_name IN ('jobs', 'jobs_archive', 'wf_step_ledger', 'wf_signals', 'wf_outbox')",
            schema,
        )
        assert len(healed) == 5
        assert {row["data_type"] for row in healed} == {"bigint"}
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            # Teardown retrieves only an expected observed outcome/cancel.
            with contextlib.suppress(asyncio.CancelledError, MigrationLockTimeoutError):
                await task
        await blocker.close()
        await migrator.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
