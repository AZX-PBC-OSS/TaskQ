# ruff: noqa: S608  # Schema names are generated test identifiers; values remain $-bound.
"""Capability-level pins for the transactional execution door.

The transactional declaration stamps the LITERAL level 3 on every row of its
run (the root, the static nodes, and every emitted/forked child inheriting
them); an ordinary workflow's rows stay NULL (unguarded, unchanged); the
worker boot stamps the level this build actually implements."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq.settings import WorkerSettings
from taskq.worker.run import deregister_worker, register_worker
from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    step,
    transactional_step,
)
from taskq.workflows._capability import (
    WORKER_BOOT_WORKFLOW_SCHEMA,
    WORKFLOW_EXECUTION_SCHEMA_TRANSACTIONAL,
)
from tests._wf_atomicity_fixture import new_label

if TYPE_CHECKING:
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration


def _settings(dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict({"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema})


async def test_boot_stamps_the_implemented_transactional_level(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The boot stamp is the implemented level, raised by implementing code only."""
    dsn = module_pg_schema.pg_dsn
    worker_id = await register_worker(wf_pool, _settings(dsn, wf_schema), workflow_execution=True)
    try:
        raw = await wf_pool.fetchval(
            f'SELECT metadata FROM "{wf_schema}".workers WHERE id=$1', worker_id
        )
        metadata = json.loads(raw) if isinstance(raw, str) else raw
        assert isinstance(metadata, dict)
        stamped = metadata["workflow_execution_schema"]
        assert type(stamped) is int
        assert stamped == WORKER_BOOT_WORKFLOW_SCHEMA == 3
        assert metadata["workflow_execution"] is True
    finally:
        await deregister_worker(wf_pool, _settings(dsn, wf_schema), worker_id)


async def test_public_transactional_declaration_stamps_literal_three_and_ordinary_null(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The declaration is the only writer: level 3 on its run's rows, NULL beside it."""
    ordinary_calls: list[str] = []

    async def ordinary_body(ctx: StepContext) -> str:
        ordinary_calls.append("ordinary")
        return "ordinary"

    ordinary_app = WorkflowApp()
    ordinary_name = new_label("ordinary_null")

    @ordinary_app.workflow(ordinary_name)
    def ordinary_flow() -> Promise[object]:
        return build(step(ordinary_body, key="only"))

    ordinary_runner = FlowRunner(ordinary_app.get(ordinary_name), wf_pool, wf_schema)
    ordinary_id = (await ordinary_runner.create_flow()).flow_id
    ordinary_rows = await wf_conn.fetch(
        f'SELECT required_worker_schema FROM "{wf_schema}".jobs '
        "WHERE id=$1 OR (metadata->>'flow_id')::uuid=$1",
        ordinary_id,
    )
    assert ordinary_rows, "the ordinary run materialized no rows to read"
    assert [row[0] for row in ordinary_rows] == [None] * len(ordinary_rows)

    calls: list[str] = []

    async def source(ctx: StepContext) -> object:
        calls.append("source")
        return {"pages": 0, "token": "capability"}

    async def settle(ctx: StepContext, source_result: object) -> None:
        calls.append("settle")

    async def screen(ctx: StepContext, item: object) -> str:
        return "screened"

    def records() -> Chain:
        return Chain(name=new_label("records"), start="screen", steps={"screen": Step(screen, str)})

    app = WorkflowApp()
    name = new_label("transactional_capability")

    @app.workflow(name)
    def flow() -> Promise[object]:
        source_declaration = chain_source(records(), source, key="source", max_attempts=1)
        return build(transactional_step(settle, source_declaration, key="settle", max_attempts=1))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    rows = await wf_conn.fetch(
        f'SELECT step_key, required_worker_schema FROM "{wf_schema}".jobs '
        "WHERE id=$1 OR (metadata->>'flow_id')::uuid=$1 ORDER BY step_key",
        flow_id,
    )
    assert [row["step_key"] for row in rows] == ["__flow__", "settle", "source"]
    assert [row["required_worker_schema"] for row in rows] == [
        WORKFLOW_EXECUTION_SCHEMA_TRANSACTIONAL,
        WORKFLOW_EXECUTION_SCHEMA_TRANSACTIONAL,
        WORKFLOW_EXECUTION_SCHEMA_TRANSACTIONAL,
    ]
    assert all(type(row["required_worker_schema"]) is int for row in rows)
