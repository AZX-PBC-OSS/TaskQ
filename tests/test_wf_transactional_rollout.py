# ruff: noqa: S608  # Schema names are generated test identifiers; values remain $-bound.
"""The mixed-rollout control: an ACTUAL old-binary worker process against
level-3 rows.

The old process is the base revision's own ``src/taskq`` tree materialized
from git (never synthetic metadata): its worker boot stamps capability
level 2, its dispatch loop warms its own prepared claim statements, and its
claim of a level-3 row must fail at the physical entry-to-running trigger
(WF001) before any body — on the first dispatch and again on the warmed
prepared-statement dispatch. A capable new-binary worker then completes the
same row."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import asyncpg
import pytest

from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    transactional_step,
)
from tests._wf_atomicity_fixture import (
    RealWorker,
    application_tables,
    new_label,
    node_id,
)
from tests.test_wf_atomicity_controls import _chain

pytestmark = pytest.mark.integration

REPO = "/tmp/opencode/ship-wt"  # noqa: S108  # Why: the adoption worktree's fixed location is this program's own constant, not a user-data path.

_PROGRAM_TEMPLATE = """
import asyncio
import json
from datetime import timedelta

sys_path_entry = {old_tree!r}
import sys

sys.path.insert(0, sys_path_entry)

import asyncpg  # after the path insert: the old tree's own dependency surface

from taskq.worker.run import register_worker, deregister_worker  # the old tree's own code
from taskq.settings import WorkerSettings
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch


async def main() -> None:
    settings = WorkerSettings.load_from_dict(
        {{"TASKQ_PG_DSN": {dsn!r}, "TASKQ_SCHEMA_NAME": {schema!r}}}
    )
    conn = await asyncpg.connect({dsn!r})
    pool = await asyncpg.create_pool({dsn!r}, min_size=1, max_size=1)
    worker_id = await register_worker(pool, settings, workflow_execution=True)
    try:
        raw = await conn.fetchval(
            'SELECT metadata FROM "{schema}".workers WHERE id=$1', worker_id
        )
        print("OLD_BOOT_METADATA=" + raw, flush=True)
        for round_index in range(2):
            try:
                claimed = await dispatch_batch(
                    conn,
                    sql=DISPATCH_STRICT_FIFO_SQL.format(schema={schema!r}),
                    queues=["default"],
                    limit_n=1,
                    worker_id=worker_id,
                    lock_lease=timedelta(seconds=30),
                )
                print(f"OLD_DISPATCH_{{round_index}}=CLAIMED {{claimed}}", flush=True)
            except Exception as exc:
                state = getattr(exc, "sqlstate", None)
                print(
                    f"OLD_DISPATCH_{{round_index}}=REFUSED {{type(exc).__name__}} {{state}}",
                    flush=True,
                )
    finally:
        await deregister_worker(pool, settings, worker_id)
        await pool.close()
        await conn.close()


asyncio.run(main())
"""


def _materialize_base_tree(destination: Path) -> Path:
    """Materialize the base revision's src/taskq (read-only git use; no
    history mutation)."""
    destination.mkdir(parents=True, exist_ok=True)
    archive = (
        subprocess.run(  # Why: fixed argv, no shell; the archive is this repo's own HEAD tree.
            ["git", "archive", "HEAD", "src/taskq"],  # noqa: S607  # Why: git from PATH by fixed argv, no shell.
            check=True,
            capture_output=True,
            cwd=REPO,
        )
    )
    subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the target is this test's own tmp directory.
        ["tar", "-x", "-C", str(destination)],  # noqa: S607  # Why: tar from PATH by fixed argv, no shell.
        input=archive.stdout,
        check=True,
    )
    tree = destination / "src" / "taskq"
    assert tree.is_dir()
    return tree


async def test_old_binary_process_refuses_level3_rows_before_any_body(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: object,
    tmp_path: Path,
) -> None:
    dsn = module_pg_schema.pg_dsn  # pyright: ignore[reportAttributeAccessIssue]  # Why: the module fixture is the sibling tests' NamedTuple; the attribute is the same one every sibling reads.

    async def source(ctx: StepContext) -> object:
        return {"pages": 0, "token": "rollout"}

    async def settle(ctx: StepContext, source_result: object) -> None:
        return None

    app = WorkflowApp()
    name = new_label("rollout_flow")

    @app.workflow(name)
    def flow() -> Promise[object]:
        source_declaration = chain_source(
            _chain(new_label("records")), source, key="source", max_attempts=1
        )
        return build(transactional_step(settle, source_declaration, key="settle", max_attempts=1))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
    async with application_tables(wf_conn, wf_schema):
        await wf_conn.execute(
            f"INSERT INTO \"{wf_schema}\".actor_config (actor,queue) VALUES ('wf','default') "
            "ON CONFLICT (actor) DO NOTHING"
        )
        old_tree = _materialize_base_tree(tmp_path / "old-tree")
        program_path = tmp_path / "old_worker_program.py"
        program_path.write_text(
            _PROGRAM_TEMPLATE.format(
                old_tree=str(old_tree.parent),  # the directory containing the taskq package
                dsn=dsn,
                schema=wf_schema,
            )
        )
        completed = await asyncio.to_thread(  # Why: the blocking subprocess wait runs off the event loop (ASYNC221); the argv is fixed, no shell, this test's own program under this interpreter.
            subprocess.run,  # Why: fixed argv, no shell; the program is this test's own written file under this interpreter.
            [sys.executable, str(program_path)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        facts: dict[str, str] = {}
        for line in (completed.stdout + completed.stderr).splitlines():
            if line.startswith("OLD_") and "=" in line:
                key, _, value = line.partition("=")
                facts[key] = value
        assert "OLD_BOOT_METADATA" in facts, (completed.stdout, completed.stderr)
        boot = json.loads(facts["OLD_BOOT_METADATA"])
        # The old process's boot is the OLD CODE's own stamp (the base
        # revision predates the capability stamp entirely), not synthetic
        # metadata written behind the test's back. It does not advertise the
        # transactional level, so its claim must refuse at the trigger.
        stamped = boot.get("workflow_execution_schema")
        assert stamped is None or (type(stamped) is int and stamped < 3), boot
        for round_index in (0, 1):
            verdict = facts[f"OLD_DISPATCH_{round_index}"]
            assert verdict.startswith("REFUSED") and "WF001" in verdict, (
                f"the old binary's warmed dispatch round {round_index} did not fail "
                f"closed at the physical capability trigger: {verdict}"
            )
        # The row is untouched: still pending, never claimed, no ledger row.
        persisted = await wf_conn.fetchrow(
            f'SELECT status::text AS status, attempt, locked_by_worker FROM "{wf_schema}".jobs '
            "WHERE id=$1",
            source_id,
        )
        assert persisted is not None
        assert (
            persisted["status"] == "pending"
            and persisted["attempt"] == 0
            and persisted["locked_by_worker"] is None
        )
        ledger_rows = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1', flow_id
        )
        assert ledger_rows == 0

        # The capable new-binary worker completes the same row.
        capable = await RealWorker.open(wf_pool, wf_conn, dsn, wf_schema)
        try:
            assert await capable.run(runner.compiled, flow_id, "source") == "succeeded"
            assert await capable.run(runner.compiled, flow_id, "settle") == "succeeded"
        finally:
            await capable.close()
        _ = asyncpg  # the module's own import stays load-bearing for the type surface
        _ = timedelta
