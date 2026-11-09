"""Real-PostgreSQL pins for the P0 workflow worker-capability primitive.

The guarded rows in this module are internal test inputs. No public workflow
feature opts into them yet; later feature packages must supply the semantics
associated with higher schema levels before raising the capability version.
"""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.migrate import apply_pending, discover, split_statements
from taskq.settings import WorkerSettings
from taskq.worker.run import register_worker
from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, step
from taskq.workflows._capability import (
    REQUIRED_WORKER_SCHEMA_MAX,
    WORKER_BOOT_WORKFLOW_SCHEMA,
    WORKFLOW_EXECUTION_SCHEMA,
    WorkflowWorkerCapabilityError,
    create_guarded_flow,
    postgres_capability_error,
)
from taskq.workflows._fork import insert_fork
from taskq.workflows._types import ChildSpec, ForkSpec, JoinSpec, NodeSpec
from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE, render_sql
from taskq.workflows.engine import insert_node, render_workflow_sql

pytestmark = pytest.mark.integration


async def _guarded_pair(conn: asyncpg.Connection, schema: str) -> tuple[JobId, JobId]:
    """Seed one live root and one directly claimable guarded body row."""
    flow_id = new_uuid()
    node_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: validated test schema identifier.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, step_key, "
        "metadata, idempotency_scope, idempotency_key, required_worker_schema) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', '__flow__', "
        "$2::jsonb, 'workflow-run', $3, 1)",
        flow_id,
        json.dumps({"flow_id": str(flow_id)}),
        f"test:{flow_id}",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: validated test schema identifier.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, step_key, "
        "metadata, fairness_key, idempotency_scope, idempotency_key, required_worker_schema) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'pending', 'only', "
        "$2::jsonb, $3, $4, 'wf:only', 1)",
        node_id,
        json.dumps({"flow_id": str(flow_id)}),
        str(flow_id),
        f"workflow:{flow_id}",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '  # noqa: S608
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    return JobId(flow_id), JobId(node_id)


async def test_old_claim_sql_hits_the_guard_and_actual_boot_stamps_current_schema(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    """The new DDL is already installed for this red-first control.

    A legacy worker with workflow_execution=true but no schema version must
    fail at both verbatim old claim doors. The production registration call,
    not a synthetic version oracle, must then stamp the current primitive's
    schema before the same fleet statement can claim.
    """
    _flow_id, node_id = await _guarded_pair(wf_conn, wf_schema)
    legacy_worker = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
        "VALUES ($1, 'legacy', 1, '{default}', $2::jsonb)",
        legacy_worker,
        json.dumps({"workflow_execution": True, "operator_note": "preserved"}),
    )

    with pytest.raises(asyncpg.PostgresError) as direct_error:
        await wf_conn.fetchval(
            render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), node_id, legacy_worker
        )
    assert direct_error.value.sqlstate == "WF001"
    state = await wf_conn.fetchrow(
        f'SELECT status, attempt, locked_by_worker FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
        node_id,
    )
    assert state is not None
    assert dict(state) == {"status": "pending", "attempt": 0, "locked_by_worker": None}

    with pytest.raises(asyncpg.PostgresError) as fleet_error:
        await dispatch_batch(
            wf_conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
            queues=["default"],
            limit_n=1,
            worker_id=legacy_worker,
            lock_lease=timedelta(seconds=30),
        )
    assert fleet_error.value.sqlstate == "WF001"

    current_worker = await register_worker(
        wf_pool,
        WorkerSettings.load_from_dict({"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": wf_schema}),
        workflow_execution=True,
    )
    boot = await wf_conn.fetchrow(
        f'SELECT metadata FROM "{wf_schema}".workers WHERE id = $1',  # noqa: S608
        current_worker,
    )
    assert boot is not None
    metadata = json.loads(boot["metadata"])
    # The boot stamps the level this build actually implements: level 2
    # adds the guarded durable terminal effects.
    assert metadata["workflow_execution_schema"] == WORKER_BOOT_WORKFLOW_SCHEMA == 2
    assert metadata["workflow_execution"] is True
    assert "notify_enabled" in metadata and "max_concurrency" in metadata

    claimed = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=1,
        worker_id=current_worker,
        lock_lease=timedelta(seconds=30),
    )
    assert [row["id"] for row in claimed] == [node_id]
    assert claimed[0]["required_worker_schema"] == 1


class _CapabilityInput(BaseModel):
    value: int


class _CapabilityOutput(BaseModel):
    value: int


def _single_step_runner(
    pool: asyncpg.Pool, schema: str, *, worker_id: JobId | None = None
) -> tuple[FlowRunner, list[int]]:
    calls: list[int] = []

    async def body(ctx: Any, params: _CapabilityInput) -> _CapabilityOutput:
        calls.append(params.value)
        return _CapabilityOutput(value=params.value)

    app = WorkflowApp()
    name = f"capability-{new_base62(10)}"

    @app.workflow(name)
    def workflow() -> Promise[object]:
        return build(step(body, _CapabilityInput(value=7), key="only"))

    return (
        FlowRunner(app.get(name), pool, schema, worker_id=worker_id),
        calls,
    )


@pytest.mark.parametrize(
    "raw_metadata",
    [
        None,
        "{}",
        '{"workflow_execution":true}',
        '{"workflow_execution":true,"workflow_execution_schema":null}',
        '{"workflow_execution":true,"workflow_execution_schema":true}',
        '{"workflow_execution":true,"workflow_execution_schema":"1"}',
        '{"workflow_execution":true,"workflow_execution_schema":1.0}',
        '{"workflow_execution":true,"workflow_execution_schema":-1}',
        '{"workflow_execution":true,"workflow_execution_schema":0}',
        '{"workflow_execution":true,"workflow_execution_schema":[1]}',
        '{"workflow_execution":true,"workflow_execution_schema":{"major":1}}',
        '{"workflow_execution":true,"workflow_execution_schema":2147483648}',
    ],
)
async def test_malformed_missing_or_old_worker_metadata_fails_closed_before_claim(
    wf_conn: asyncpg.Connection, wf_schema: str, raw_metadata: str | None
) -> None:
    """No JSON shape other than a canonical in-range integer can unlock work."""
    _flow_id, node_id = await _guarded_pair(wf_conn, wf_schema)
    worker_id = JobId(new_uuid())
    if raw_metadata is not None:
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
            "VALUES ($1, 'capability-attack', 1, '{default}', $2::jsonb)",
            worker_id,
            raw_metadata,
        )
    with pytest.raises(asyncpg.PostgresError) as caught:
        await wf_conn.fetchval(render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), node_id, worker_id)
    assert caught.value.sqlstate == "WF001"
    typed = postgres_capability_error(caught.value, job_id=node_id)
    assert isinstance(typed, WorkflowWorkerCapabilityError)
    assert "Upgrade every worker" in str(typed)
    assert "No workflow body was started" in str(typed)
    state = await wf_conn.fetchrow(
        f'SELECT status, attempt, locked_by_worker FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
        node_id,
    )
    assert state is not None
    assert dict(state) == {"status": "pending", "attempt": 0, "locked_by_worker": None}


async def test_guarded_root_direct_refusal_precedes_body_and_claim(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    runner, calls = _single_step_runner(wf_pool, wf_schema)
    claim = await create_guarded_flow(
        runner, required_worker_schema=1, run_key="capability:guarded-root"
    )
    assert claim.created and claim.required_worker_schema == 1

    with pytest.raises(WorkflowWorkerCapabilityError) as caught:
        await runner.drive(claim.flow_id)
    assert caught.value.required_worker_schema == 1
    assert caught.value.flow_id == claim.flow_id
    assert "fleet" in str(caught.value)
    assert "No workflow body was started" in str(caught.value)
    assert calls == []
    rows = await wf_conn.fetch(
        f'SELECT step_key, status, attempt, required_worker_schema FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1 ORDER BY step_key",
        claim.flow_id,
    )
    assert [dict(row) for row in rows] == [
        {"step_key": "__flow__", "status": "running", "attempt": 0, "required_worker_schema": 1},
        {"step_key": "only", "status": "pending", "attempt": 0, "required_worker_schema": 1},
    ]
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1',  # noqa: S608
            claim.flow_id,
        )
        == 0
    )


async def test_guarded_row_direct_refusal_does_not_depend_on_root_stamp(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """A manually seeded guarded body refuses even when its root is legacy NULL."""
    runner, calls = _single_step_runner(wf_pool, wf_schema)
    claim = await runner.create_flow(run_key="capability:guarded-row")
    assert claim.required_worker_schema is None
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET required_worker_schema = 1 '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'only'",
        claim.flow_id,
    )
    with pytest.raises(WorkflowWorkerCapabilityError) as caught:
        await runner.tick(claim.flow_id)
    assert caught.value.job_id is not None
    assert calls == []
    row = await wf_conn.fetchrow(
        f'SELECT status, attempt FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'only'",
        claim.flow_id,
    )
    assert row is not None
    assert dict(row) == {"status": "pending", "attempt": 0}


async def test_stale_direct_read_shape_maps_physical_refusal_before_body(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """A stale row dict lacking the new select field still cannot smuggle a body."""
    runner, calls = _single_step_runner(wf_pool, wf_schema)
    claim = await runner.create_flow(run_key="capability:stale-direct-read")
    row = await wf_conn.fetchrow(
        f"SELECT id, step_key, map_index, attempt, trace_id, payload, start_to_close, metadata "  # noqa: S608
        f"FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'only'",
        claim.flow_id,
    )
    assert row is not None
    stale_row = dict(row)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET required_worker_schema = 1 WHERE id = $1',  # noqa: S608
        stale_row["id"],
    )
    with pytest.raises(WorkflowWorkerCapabilityError) as caught:
        await runner._run_node(claim.flow_id, stale_row)  # pyright: ignore[reportPrivateUsage]  # Why: direct old-read compatibility is the boundary under test.
    cause = caught.value.__cause__
    assert isinstance(cause, asyncpg.PostgresError)
    assert cause.sqlstate == "WF001"
    assert calls == []
    state = await wf_conn.fetchrow(
        f'SELECT status, attempt, required_worker_schema FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
        stale_row["id"],
    )
    assert state is not None
    assert dict(state) == {"status": "pending", "attempt": 0, "required_worker_schema": 1}


async def test_ordinary_unguarded_direct_run_remains_supported(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    runner, calls = _single_step_runner(wf_pool, wf_schema)
    claim = await runner.create_flow(run_key="capability:ordinary")
    assert claim.required_worker_schema is None
    assert await runner.drive(claim.flow_id) == "terminal"
    assert calls == [7]
    assert (
        await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
            claim.flow_id,
        )
        == "succeeded"
    )
    requirements = await wf_conn.fetch(
        f'SELECT required_worker_schema FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1",
        claim.flow_id,
    )
    assert [row[0] for row in requirements] == [None, None]


async def test_capable_fleet_claim_executes_the_guarded_body_and_preserves_requirement(
    wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_schema: str, pg_dsn: str
) -> None:
    initial_runner, calls = _single_step_runner(wf_pool, wf_schema)
    claim = await create_guarded_flow(
        initial_runner,
        required_worker_schema=WORKFLOW_EXECUTION_SCHEMA,
        run_key="capability:fleet-body",
    )
    worker_id = await register_worker(
        wf_pool,
        WorkerSettings.load_from_dict({"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": wf_schema}),
        workflow_execution=True,
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '  # noqa: S608
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    claimed = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=1,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert len(claimed) == 1
    row = dict(claimed[0])
    fleet_runner = FlowRunner(
        initial_runner.compiled, wf_pool, wf_schema, worker_id=JobId(worker_id)
    )
    assert (
        await fleet_runner.run_fleet_claimed_step(
            claim.flow_id,
            row,
            attempt=row["attempt"],
            claim_epoch=row["claim_epoch"],
        )
        == "succeeded"
    )
    assert calls == [7]
    node = await wf_conn.fetchrow(
        f'SELECT status, required_worker_schema, locked_by_worker FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
        row["id"],
    )
    assert node is not None
    assert dict(node) == {
        "status": "succeeded",
        "required_worker_schema": WORKFLOW_EXECUTION_SCHEMA,
        "locked_by_worker": worker_id,
    }


async def test_legacy_workflow_and_vanilla_rows_are_inert(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    _flow_id, guarded_id = await _guarded_pair(wf_conn, wf_schema)
    old_worker = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
        "VALUES ($1, 'unguarded-worker', 1, '{default}', '{}')",
        old_worker,
    )
    # Legacy workflow rows carry NULL and old binaries can still claim them.
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET required_worker_schema = NULL WHERE id = $1',  # noqa: S608
        guarded_id,
    )
    assert (
        await wf_conn.fetchval(
            render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), guarded_id, old_worker
        )
        == 1
    )

    # Even a synthetic non-NULL requirement on a vanilla row is outside the
    # workflow-only WHEN clause; there is no vanilla hot-path capability tax.
    vanilla_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '  # noqa: S608
        "(id, actor, queue, payload, max_attempts, retry_kind, status, metadata, "
        "required_worker_schema) VALUES ($1, 'wf', 'default', '{}', 3, "
        "'transient', 'pending', '{}', 1)",
        vanilla_id,
    )
    other_old_worker = JobId(new_uuid())
    assert (
        await wf_conn.fetchval(
            render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), vanilla_id, other_old_worker
        )
        == 1
    )


async def test_no_capability_check_on_heartbeat_ownership_restamp_or_terminal_update(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    _flow_id, node_id = await _guarded_pair(wf_conn, wf_schema)
    claimant = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
        "VALUES ($1, 'claim-then-vanish', 1, '{default}', "
        "'{\"workflow_execution_schema\":1}'::jsonb)",
        claimant,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "  # noqa: S608
        "locked_by_worker = $2, metadata = metadata || '{\"existing\":true}'::jsonb "
        "WHERE id = $1",
        node_id,
        claimant,
    )
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".workers WHERE id = $1', claimant)  # noqa: S608
    # The worker is now missing; prove the
    # three non-entry UPDATE classes never consult the missing worker row.
    assert (
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET metadata = metadata || '  # noqa: S608
            "'{\"heartbeat\":true}'::jsonb, last_heartbeat_at = clock_timestamp() WHERE id = $1",
            node_id,
        )
        == "UPDATE 1"
    )
    assert (
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'running', "  # noqa: S608
            "metadata = metadata || '{\"ownership_restamp\":true}'::jsonb WHERE id = $1",
            node_id,
        )
        == "UPDATE 1"
    )
    assert (
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', "  # noqa: S608
            "finished_at = clock_timestamp() WHERE id = $1",
            node_id,
        )
        == "UPDATE 1"
    )


@pytest.mark.parametrize(
    "value",
    [True, "1", 1.0, 0, -1, REQUIRED_WORKER_SCHEMA_MAX + 1],
)
def test_node_spec_rejects_non_exact_or_out_of_range_requirement(value: object) -> None:
    with pytest.raises(ValueError, match="required_worker_schema"):
        NodeSpec(
            flow_id=JobId(new_uuid()),
            step_key="only",
            actor="wf",
            queue="default",
            required_worker_schema=value,  # type: ignore[arg-type]  # Why: runtime poison inputs deliberately violate the declared face.
        )


async def test_node_insert_writes_only_the_validated_requirement(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    flow_id = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '  # noqa: S608
        "(id, actor, queue, payload, max_attempts, retry_kind, status, step_key, "
        "metadata, required_worker_schema) VALUES ($1, 'wf', 'default', '{}', 3, "
        "'transient', 'running', '__flow__', $2::jsonb, $3)",
        flow_id,
        json.dumps({"flow_id": str(flow_id)}),
        REQUIRED_WORKER_SCHEMA_MAX,
    )
    node_id = await insert_node(
        wf_conn,
        render_workflow_sql(wf_schema),
        NodeSpec(
            flow_id=flow_id,
            step_key="only",
            actor="wf",
            queue="default",
            required_worker_schema=REQUIRED_WORKER_SCHEMA_MAX,
        ),
    )
    assert (
        await wf_conn.fetchval(
            f'SELECT required_worker_schema FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
            node_id,
        )
        == REQUIRED_WORKER_SCHEMA_MAX
    )


async def test_guarded_create_on_old_schema_fails_at_insert_without_a_root(
    pg_dsn: str,
) -> None:
    schema = f"cap_old_{new_base62(8)}".lower()
    admin = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await apply_pending(admin, schema=schema, target="01.00.37_01")
        pool = await asyncpg.create_pool(pg_dsn)
        runner, calls = _single_step_runner(pool, schema)
        with pytest.raises(asyncpg.UndefinedColumnError):
            await create_guarded_flow(
                runner, required_worker_schema=1, run_key="capability:pre-ddl"
            )
        assert calls == []
        assert (
            await admin.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: generated test schema identifier.
            )
            == 0
        )
    finally:
        if pool is not None:
            await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()


async def test_populated_old_schema_upgrades_without_touching_existing_rows(
    pg_dsn: str,
) -> None:
    schema = f"cap_pop_{new_base62(8)}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        previous = await apply_pending(conn, schema=schema, target="01.00.37_01")
        assert previous and previous[-1].key == "01.00.37_01:post"
        job_id = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608
            "(id, actor, queue, payload, metadata, max_attempts, retry_kind) VALUES "
            "($1, 'populated', 'default', '{\"secret-free\":1}', '{\"keep\":true}', "
            "3, 'transient')",
            job_id,
        )
        applied = await apply_pending(conn, schema=schema)
        assert [migration.key for migration in applied] == [
            "01.00.38_01:pre",
            "01.00.41_01:pre",
            "01.00.41_02:pre",
        ]
        row = await conn.fetchrow(
            f'SELECT payload, metadata, status, required_worker_schema, run_epoch FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
        assert row is not None
        assert json.loads(row["payload"]) == {"secret-free": 1}
        assert json.loads(row["metadata"]) == {"keep": True}
        assert row["status"] == "pending" and row["required_worker_schema"] is None
        assert row["run_epoch"] is None  # legacy rows need no effect incarnation
        column = await conn.fetchrow(
            "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = 'jobs' "
            "AND column_name = 'required_worker_schema'",
            schema,
        )
        assert column is not None
        assert dict(column) == {
            "data_type": "integer",
            "is_nullable": "YES",
            "column_default": None,
        }
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_capability_migration_is_atomic_on_trigger_name_collision(pg_dsn: str) -> None:
    schema = f"cap_rollback_{new_base62(8)}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema, target="01.00.37_01")
        await conn.execute(
            f'CREATE FUNCTION "{schema}".preexisting_trigger() RETURNS trigger '
            "LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$"
        )
        await conn.execute(
            f"CREATE TRIGGER jobs_wf_worker_capability_guard BEFORE UPDATE OF status "
            f'ON "{schema}".jobs FOR EACH ROW EXECUTE FUNCTION '
            f'"{schema}".preexisting_trigger()'
        )
        with pytest.raises(asyncpg.DuplicateObjectError):
            await apply_pending(conn, schema=schema)
        assert (
            await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = 'jobs' "
                "AND column_name = 'required_worker_schema')",
                schema,
            )
            is False
        )
        assert (
            await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.routines "
                "WHERE routine_schema = $1 AND routine_name = 'enforce_wf_worker_capability')",
                schema,
            )
            is False
        )
        assert (
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".schema_migrations WHERE version = $1',  # noqa: S608  # Why: generated test schema identifier.
                "01.00.38_01:pre",
            )
            == 0
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_dynamic_fork_emit_and_outbox_rows_inherit_requirement(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """Existing statement bind lists stay stable; persisted parents drive inheritance."""
    flow_id, parent_id = await _guarded_pair(wf_conn, wf_schema)
    wsql = render_workflow_sql(wf_schema)
    await insert_fork(
        wf_conn,
        wsql,
        flow_id=flow_id,
        parent_id=parent_id,
        parent_step_key="only",
        fork=ForkSpec(
            children=(ChildSpec(step_key="forked", actor="wf", queue="default"),),
            join=JoinSpec(step_key="forked.join", actor="wf", queue="default"),
        ),
    )
    await wf_conn.execute(
        wsql.emit_children,
        [new_uuid()],
        ["wf"],
        ["default"],
        ["{}"],
        [3],
        ["transient"],
        [parent_id],
        [0],
        ["emitted"],
        ["emit-trace"],
        [json.dumps({"flow_id": str(flow_id)})],
        [f"workflow:{flow_id}"],
        [f"wf:{flow_id}:emit:0:emitted"],
        [str(flow_id)],
        [0],
        [None],
        [5.0],
        [3600.0],
        ["exponential"],
        [0.2],
    )
    await wf_conn.execute(
        wsql.outbox_drain_consumers,
        [new_uuid()],
        ["wf"],
        ["default"],
        ["{}"],
        [3],
        ["transient"],
        [parent_id],
        [None],
        ["outboxed"],
        ["outbox-trace"],
        [json.dumps({"flow_id": str(flow_id)})],
        [f"workflow:{flow_id}"],
        [f"wf:{flow_id}:outboxed"],
        [0],
        [None],
        [str(flow_id)],
        [5.0],
        [3600.0],
        ["exponential"],
        [0.2],
    )
    rows = await wf_conn.fetch(
        f'SELECT step_key, status, required_worker_schema FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key IN "
        "('forked', 'forked.join', 'emitted', 'outboxed') ORDER BY step_key",
        flow_id,
    )
    assert [dict(row) for row in rows] == [
        {"step_key": key, "status": "pending", "required_worker_schema": 1}
        for key in ("emitted", "forked", "forked.join", "outboxed")
    ]
    # Status/re-pend updates preserve the field physically, without rewriting
    # it from a mutable Python declaration.
    changed = await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', scheduled_at = now() "  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'forked'",
        flow_id,
    )
    assert changed == "UPDATE 1"
    assert (
        await wf_conn.fetchval(
            f'SELECT required_worker_schema FROM "{wf_schema}".jobs '  # noqa: S608
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'forked'",
            flow_id,
        )
        == 1
    )


async def test_existing_claim_statements_explain_with_the_guard_installed(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The trigger adds no SQL predicate or extra plan to either old claim door."""
    _flow_id, _node_id = await _guarded_pair(wf_conn, wf_schema)
    worker_id = new_uuid()
    fleet_sql = DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema)
    fleet_plan = await wf_conn.fetchval(
        f"EXPLAIN (FORMAT JSON) {fleet_sql}",
        ["default"],
        1,
        worker_id,
        timedelta(seconds=30),
        2,
    )
    direct_plan = await wf_conn.fetchval(
        f"EXPLAIN (FORMAT JSON) {render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema)}",
        new_uuid(),
        worker_id,
    )
    assert isinstance(fleet_plan, str) and '"Plan"' in fleet_plan
    assert isinstance(direct_plan, str) and '"Plan"' in direct_plan
    assert "required_worker_schema" not in fleet_sql
    assert "required_worker_schema" not in NODE_CLAIM_SQL_TEMPLATE


def test_migration_pre_registration_checksum_and_trigger_shape() -> None:
    migrations = discover()
    capability = next(
        migration
        for migration in migrations
        if migration.filename == "01.00.38_01_pre_wf_worker_capability.sql"
    )
    assert capability.filename == "01.00.38_01_pre_wf_worker_capability.sql"
    assert capability.key == "01.00.38_01:pre"
    assert not any(m.version == capability.version and m.phase == "post" for m in migrations)
    manifest = Path("tests/data/released_migrations.sha256").read_text()
    expected = hashlib.sha256(capability.sql_template.encode()).hexdigest()
    assert f"{expected}  {capability.filename}" in manifest.splitlines()
    statements = split_statements(capability.sql_template)
    assert any("BEFORE UPDATE OF status" in statement for statement in statements)
    assert not any("DROP TABLE" in statement.upper() for statement in statements)
    assert not any("DROP TRIGGER" in statement.upper() for statement in statements)
