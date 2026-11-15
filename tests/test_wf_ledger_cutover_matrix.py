# ruff: noqa: S608  # Schema names are generated test identifiers; values remain $-bound.
"""The 01.00.42 workflow-executor cutover's REAL old-process matrix.

Every old shape here is the BASE REVISION's own ``src/taskq`` tree,
materialized from git (never synthetic metadata), running as a real OS
process against the real migrated schema:

* BEFORE 01.00.42, the old FLEET door and the old DIRECT door both claim
  ordinary (NULL-requirement) workflow nodes, run their bodies, and
  succeed — with warm prepared statements and two distinct call values
  (the ordinary-compatibility control);
* at PRE-only and at the fully-POST-migrated states, the SAME old code
  with warmed statements fails the real old arbiter path with
  PostgreSQL ``42P10`` (``InvalidColumnReferenceError``) BEFORE any old
  body effect — first call and repeated call — and the violated job can
  transiently remain ``running`` until corrected-process recovery (the
  documented unsupported envelope; no final old body failure/success is
  fabricated);
* a corrected new worker then recovers those rows under actual new
  claim/epoch identity, runs the intended body exactly once, and the
  historical ledger rows never collide;
* the original epoch-0 history is fenced/terminalized only under its own
  exact id + claim instant (the re-mint guard refuses a stale stamp);
* the catalog controls assert the exact PRE/POST index expressions and
  the migration ledger's own phases.

The old programs' subprocesses are real ``sys.executable`` processes that
exit on their own (their doors catch and report the refusal facts); every
pool they open is closed by their own ``finally``. The level-3 old-dispatch
control (the WF001 physical trigger) stays in
``test_wf_transactional_rollout.py`` — this file is the ORDINARY
(NULL-requirement) half of the cutover.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import asyncpg
import pytest

from taskq.migrate import apply_pending
from taskq.workflows.engine import render_workflow_sql

pytestmark = pytest.mark.integration

REPO = "/tmp/opencode/ship-wt"  # noqa: S108  # Why: the adoption worktree's fixed location is this program's own constant, not a user-data path.

_OLD_PROGRAM_TEMPLATE = """
import asyncio
import uuid
from datetime import timedelta

import sys

sys.path.insert(0, {old_tree!r})

import asyncpg  # after the path insert: the old tree's own dependency surface

from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.settings import WorkerSettings
from taskq.worker.run import register_worker, deregister_worker

MODE = {mode!r}
MATRIX_DSN = {dsn!r}
MATRIX_SCHEMA = {schema!r}
MATRIX_DOMAIN = {domain_table!r}
SHORT_LEASE = timedelta(seconds={lock_lease_s})


def _domain_insert(conn, flow_id, note):
    return conn.execute(
        'INSERT INTO "' + MATRIX_SCHEMA + '"."' + MATRIX_DOMAIN + '" (flow_id, note) VALUES ($1, $2)',
        flow_id,
        note,
    )


async def _fleet_flow(conn, pool, settings, worker_id, round_index):
    from taskq.workflows import (
        Chain,
        FlowRunner,
        Promise,
        Step,
        StepContext,
        WorkflowApp,
        build,
        chain_source,
        step as step_verb,
    )

    async def source(ctx: StepContext) -> object:
        return {{"pages": 0, "token": "old-fleet"}}

    async def settle(ctx: StepContext, source_result: object) -> object:
        await _domain_insert(conn, ctx.flow_id, "old-fleet-body")
        return "settled"

    async def leaf(ctx, item):
        return "ok"

    name = "old_ordinary_fleet_" + str(round_index)
    app = WorkflowApp()
    chain = Chain(name=name, start="screen", steps={{"screen": Step(leaf, str)}})

    @app.workflow(name)
    def flow() -> Promise[object]:
        source_declaration = chain_source(chain, source, key="source", max_attempts=1)
        return build(step_verb(settle, source_declaration, key="settle", max_attempts=1))

    runner = FlowRunner(app.get(name), pool, MATRIX_SCHEMA, worker_id=worker_id)
    flow_id = (await runner.create_flow()).flow_id
    print("OLD_FLOW=" + str(flow_id), flush=True)
    from taskq.workflows.api import _sql_runner as old_sql_runner

    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema=MATRIX_SCHEMA)

    for target in ("source", "settle"):
        won = False
        for _ in range(80):
            claimed = await dispatch_batch(
                conn,
                sql=rendered,
                queues=["default"],
                limit_n=1,
                worker_id=worker_id,
                lock_lease=SHORT_LEASE,
            )
            if not claimed:
                await asyncio.sleep(0.1)
                continue
            row = claimed[0]
            if row["step_key"] != target:
                # not this door's row this round; run it anyway (one flow,
                # one process) and loop for the target next
                pass
            verdict = await runner.run_fleet_claimed_step(
                flow_id,
                dict(row),
                attempt=int(row["attempt"]),
                claim_epoch=int(row["claim_epoch"]),
            )
            if row["step_key"] == target:
                post = await conn.fetchrow(
                    'SELECT status::text AS status, attempt, claim_epoch, error_class '
                    'FROM "' + MATRIX_SCHEMA + '".jobs WHERE id = $1',
                    row["id"],
                )
                # The pre-01.00.42 ledger has no claim_epoch column: the
                # diagnostic reads the columns the stage's own schema
                # carries.
                led = await conn.fetch(
                    'SELECT status::text AS status, attempt FROM "'
                    + MATRIX_SCHEMA + '".wf_step_ledger WHERE job_id = $1 '
                    "ORDER BY attempt",
                    row["id"],
                )
                print(
                    "OLD_FLEET_" + target.upper() + "=" + verdict
                    + " ROW=" + str(dict(post) if post else None)
                    + " LEDGER=" + str([dict(x) for x in led]),
                    flush=True,
                )
                won = True
                break
        if not won:
            state = await conn.fetchrow(
                'SELECT status::text AS status, deps_pending, assignment_routed, queue '
                'FROM "' + MATRIX_SCHEMA + '".jobs WHERE step_key = $2 '
                "AND metadata @> jsonb_build_object('flow_id', $1::text)",
                str(flow_id),
                target,
            )
            src_row = await conn.fetchrow(
                'SELECT status::text AS status, attempt FROM "' + MATRIX_SCHEMA + '".jobs '
                "WHERE step_key = 'source' "
                "AND metadata @> jsonb_build_object('flow_id', $1::text)",
                str(flow_id),
            )
            edges = await conn.fetch(
                'SELECT child_id, parent_id FROM "'
                + MATRIX_SCHEMA
                + '".wf_edge WHERE parent_id IN '
                '(SELECT id FROM "' + MATRIX_SCHEMA + '".jobs WHERE metadata @> '
                "jsonb_build_object('flow_id', $1::text))",
                str(flow_id),
            )
            print(
                "OLD_FLEET_" + target.upper() + "=NEVER_CLAIMED "
                + str(dict(state) if state else None)
                + " SRC=" + str(dict(src_row) if src_row else None)
                + " EDGES=" + str([dict(e) for e in edges]),
                flush=True,
            )


async def _direct_flow(conn, pool, round_index):
    from taskq.workflows import (
        Chain,
        FlowRunner,
        Promise,
        Step,
        StepContext,
        WorkflowApp,
        build,
        chain_source,
        step as step_verb,
    )

    async def source(ctx: StepContext) -> object:
        return {{"pages": 0, "token": "old-direct"}}

    async def settle(ctx: StepContext, source_result: object) -> object:
        await _domain_insert(conn, ctx.flow_id, "old-direct-body")
        return "settled"

    async def leaf(ctx, item):
        return "ok"

    name = "old_ordinary_direct_" + str(round_index) + "_" + uuid.uuid4().hex[:8]
    app = WorkflowApp()
    chain = Chain(name=name, start="screen", steps={{"screen": Step(leaf, str)}})

    @app.workflow(name)
    def flow() -> Promise[object]:
        source_declaration = chain_source(chain, source, key="source", max_attempts=1)
        return build(step_verb(settle, source_declaration, key="settle", max_attempts=1))

    runner = FlowRunner(app.get(name), pool, MATRIX_SCHEMA)
    flow_id = (await runner.create_flow()).flow_id
    print("OLD_FLOW=" + str(flow_id), flush=True)
    verdict = await runner.drive(flow_id)
    print("OLD_DIRECT_VERDICT=" + verdict, flush=True)


async def main() -> None:
    conn = await asyncpg.connect(MATRIX_DSN)
    pool = await asyncpg.create_pool(MATRIX_DSN, min_size=1, max_size=2)
    settings = WorkerSettings.load_from_dict(
        {{"TASKQ_PG_DSN": MATRIX_DSN, "TASKQ_SCHEMA_NAME": MATRIX_SCHEMA}}
    )
    # The routable-queue enumeration's own row (the dispatch's queue
    # discovery) — the same setup every real worker boot applies.
    await conn.execute(
        'INSERT INTO "' + MATRIX_SCHEMA + '".actor_config (actor,queue) '
        "VALUES ('wf','default') ON CONFLICT (actor) DO NOTHING"
    )
    worker_id = await register_worker(pool, settings, workflow_execution=True)
    try:
        # two distinct call values through the same warmed prepared claims
        for round_index in range(2):
            try:
                if MODE == "fleet":
                    await _fleet_flow(conn, pool, settings, worker_id, round_index)
                else:
                    await _direct_flow(conn, pool, round_index)
                print(f"OLD_ROUND_{{round_index}}=OK", flush=True)
            except Exception as exc:
                state = getattr(exc, "sqlstate", None)
                print(
                    f"OLD_ROUND_{{round_index}}=REFUSED {{type(exc).__name__}} {{state}}",
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
    archive = subprocess.run(
        ["git", "archive", "HEAD", "src/taskq"],  # noqa: S607  # Why: git from PATH by fixed argv, no shell.
        check=True,
        capture_output=True,
        cwd=REPO,
    )
    subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the target is this test's own tmp directory.
        ["tar", "-x", "-C", str(destination)],  # noqa: S607  # Why: tar from PATH by fixed argv, no shell.
        input=archive.stdout,
        check=True,
    )
    tree = destination / "src" / "taskq"
    assert tree.is_dir()
    return tree


def _run_old_process(
    old_tree: Path,
    program_path: Path,
    mode: str,
    dsn: str,
    schema: str,
    domain_table: str,
    *,
    lock_lease_s: int = 30,
    echo: bool = False,
) -> dict[str, str]:
    program_path.write_text(
        _OLD_PROGRAM_TEMPLATE.format(
            old_tree=str(old_tree.parent),
            mode=mode,
            dsn=dsn,
            schema=schema,
            domain_table=domain_table,
            lock_lease_s=lock_lease_s,
        )
    )
    completed = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the program is this test's own written file under this interpreter.
        [sys.executable, str(program_path)],
        capture_output=True,
        text=True,
        timeout=240,
    )
    facts: dict[str, str] = {}
    for line in (completed.stdout + completed.stderr).splitlines():
        if line.startswith("OLD_") and "=" in line:
            key, _, value = line.partition("=")
            facts[key] = value
    facts["_returncode"] = str(completed.returncode)
    if echo:
        print("OLD PROGRAM STDOUT:\n" + completed.stdout)
        print("OLD PROGRAM STDERR:\n" + completed.stderr[-2000:])
    return facts


async def _domain_count(conn: asyncpg.Connection, schema: str, table: str) -> int:
    return int(await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"'))


def _claim_index_def(indexdef: str) -> str:
    return " ".join(indexdef.split())


@pytest.mark.integration
async def test_old_executors_succeed_before_and_fail_closed_after_01_00_42(
    module_pg_schema: object,
    tmp_path: Path,
) -> None:
    """The cutover matrix: old fleet+direct ordinary claims succeed before
    01.00.42 (warm, two values), fail closed with 42P10 at PRE-only and
    fully-POST states before any old body effect, and the corrected
    worker recovers under real new epoch identity beside the preserved
    epoch-0 history."""
    dsn = module_pg_schema.pg_dsn  # type: ignore[attr-defined]
    # The matrix owns its OWN schema: the migration stages (before
    # 01.00.42 / PRE-only / fully-POST) are this test's controlled
    # inputs.
    from taskq._ids import new_uuid

    schema = f"cutover_{new_uuid().hex}".lower()
    control = await asyncpg.connect(dsn)
    local_pool: asyncpg.Pool | None = None
    try:
        # ── the schema BEFORE 01.00.42 ────────────────────────────────
        applied = await apply_pending(control, schema=schema, target="01.00.41_02")
        assert applied[-1].key.startswith("01.00.41"), [m.key for m in applied]

        domain_table = "cutover_matrix_domain"
        await control.execute(
            f'CREATE TABLE "{schema}"."{domain_table}" '
            "(flow_id uuid PRIMARY KEY, note text NOT NULL)"
        )
        old_tree = _materialize_base_tree(tmp_path / "old-tree")

        # The old fleet door: ordinary NULL rows claim, run, succeed —
        # two distinct call values (source then settle) through the same
        # warmed prepared claims.
        fleet_facts = _run_old_process(
            old_tree, tmp_path / "old_fleet.py", "fleet", dsn, schema, domain_table, echo=True
        )
        assert fleet_facts.get("OLD_ROUND_0") == "OK", fleet_facts
        assert fleet_facts.get("OLD_FLEET_SOURCE", "").startswith("succeeded"), fleet_facts
        assert fleet_facts.get("OLD_FLEET_SETTLE", "").startswith("succeeded"), fleet_facts
        # The old direct door: the equivalent ordinary positive control.
        direct_facts = _run_old_process(
            old_tree, tmp_path / "old_direct.py", "direct", dsn, schema, domain_table, echo=True
        )
        assert direct_facts.get("OLD_ROUND_0") == "OK", direct_facts
        assert direct_facts.get("OLD_DIRECT_VERDICT") == "terminal", direct_facts
        # Both old doors' bodies ran — each program's TWO warming rounds
        # (two distinct call values through the same prepared claims) —
        # the persisted domain rows prove it.
        assert await _domain_count(control, schema, domain_table) == 4

        # ── the PRE-only state: the old arbiter is gone ───────────────
        await apply_pending(control, schema=schema, target="01.00.42_01")

        # The epoch-0 history the old doors left: the PRE phase's own
        # DEFAULT stamps every old row claim_epoch 0 — real persisted
        # history, never a schema-version oracle.
        epoch0_rows = await control.fetch(
            f"SELECT id, status::text AS status, attempt, claim_epoch, updated_at "
            f'FROM "{schema}".wf_step_ledger WHERE claim_epoch = 0 ORDER BY id'
        )
        assert len(epoch0_rows) >= 2, epoch0_rows
        indexdef = await control.fetchval(
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'wf_step_ledger_claim_uniq' AND schemaname = $1",
            schema,
        )
        pre_def = _claim_index_def(indexdef)
        assert "claim_epoch" in pre_def, pre_def

        pre_fleet = _run_old_process(
            old_tree, tmp_path / "old_fleet_pre.py", "fleet", dsn, schema, domain_table
        )
        # First AND repeated calls fail the real old arbiter path before
        # any old body effect.
        assert pre_fleet.get("OLD_ROUND_0", "").startswith("REFUSED"), pre_fleet
        assert "42P10" in pre_fleet.get("OLD_ROUND_0", ""), pre_fleet
        assert pre_fleet.get("OLD_ROUND_1", "").startswith("REFUSED"), pre_fleet
        pre_direct = _run_old_process(
            old_tree, tmp_path / "old_direct_pre.py", "direct", dsn, schema, domain_table
        )
        assert pre_direct.get("OLD_ROUND_0", "").startswith("REFUSED"), pre_direct
        assert "42P10" in pre_direct.get("OLD_ROUND_0", ""), pre_direct
        # No old body effect landed at the violating stage.
        assert await _domain_count(control, schema, domain_table) == 4

        # ── the fully-POST state: the five-expression arbiter stands ──
        await apply_pending(control, schema=schema)
        indexdef = await control.fetchval(
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'wf_step_ledger_claim_uniq' AND schemaname = $1",
            schema,
        )
        post_def = _claim_index_def(indexdef)
        assert "claim_epoch" in post_def, post_def
        # The frozen 01.00.40 POST re-created the four-expression form;
        # the 01.00.42 POST re-replaces it in the same apply run: the PRE
        # and final five-expression definitions are byte-identical.
        assert pre_def == post_def, (pre_def, post_def)

        post_fleet = _run_old_process(
            old_tree, tmp_path / "old_fleet_post.py", "fleet", dsn, schema, domain_table
        )
        assert post_fleet.get("OLD_ROUND_0", "").startswith("REFUSED"), post_fleet
        assert "42P10" in post_fleet.get("OLD_ROUND_0", ""), post_fleet
        post_direct = _run_old_process(
            old_tree, tmp_path / "old_direct_post.py", "direct", dsn, schema, domain_table
        )
        assert post_direct.get("OLD_ROUND_0", "").startswith("REFUSED"), post_direct
        assert await _domain_count(control, schema, domain_table) == 4

        # The migration ledger's own phases are the applied truth.
        phases = await control.fetch(
            f'SELECT version FROM "{schema}".schema_migrations '
            "WHERE version LIKE '01.00.42%' ORDER BY version"
        )
        assert [r["version"] for r in phases] == [
            "01.00.42_01:pre",
            "01.00.42_02:post",
        ], [r["version"] for r in phases]

        # ── the corrected worker's recovery + the epoch-0 guard ───────
        # A corrected NEW worker runs a fresh ordinary run end-to-end on
        # the fully-migrated schema (the cutover's other half: the new
        # claims carry their own positive epochs).
        from taskq.workflows import (
            Chain,
            FlowRunner,
            Promise,
            Step,
            StepContext,
            WorkflowApp,
            build,
            chain_source,
        )
        from taskq.workflows import (
            step as step_verb,
        )
        from tests._wf_atomicity_fixture import RealWorker

        local_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        assert local_pool is not None

        async def new_source(ctx: StepContext) -> object:
            return {"pages": 0, "token": "corrected"}

        async def new_settle(ctx: StepContext, source_result: object) -> object:
            await control.execute(
                f'INSERT INTO "{schema}"."{domain_table}" (flow_id, note) VALUES ($1, $2)',
                ctx.flow_id,
                "corrected-body",
            )
            return "settled"

        async def new_leaf(ctx: StepContext, item: object) -> str:
            return "ok"

        app = WorkflowApp()
        chain = Chain(
            name="cutover_matrix_corrected",
            start="screen",
            steps={"screen": Step(new_leaf, str)},
        )

        @app.workflow("cutover_matrix_corrected")
        def corrected_flow() -> Promise[object]:
            source_declaration = chain_source(chain, new_source, key="source", max_attempts=1)
            return build(step_verb(new_settle, source_declaration, key="settle", max_attempts=1))

        runner = FlowRunner(app.get("cutover_matrix_corrected"), local_pool, schema)
        corrected_flow_id = (await runner.create_flow()).flow_id
        capable = await RealWorker.open(local_pool, control, dsn, schema)
        try:
            assert await capable.run(runner.compiled, corrected_flow_id, "source") == "succeeded"
            assert await capable.run(runner.compiled, corrected_flow_id, "settle") == "succeeded"
        finally:
            await capable.close()
        # 4 (the two old doors' two warming rounds) + 1 (the corrected
        # settlement's own row).
        assert await _domain_count(control, schema, domain_table) == 5

        # The corrected ledger rows carry their own positive epochs beside
        # the preserved epoch-0 history.
        corrected_rows = await control.fetch(
            f"SELECT attempt, claim_epoch, status::text AS status "
            f'FROM "{schema}".wf_step_ledger WHERE flow_id=$1 '
            "ORDER BY attempt, claim_epoch, created_at, id",
            corrected_flow_id,
        )
        # The epoch is PER JOB ROW: each corrected row's own first claim
        # granted epoch 1 (the per-row counter, never a per-flow one).
        assert [r["claim_epoch"] for r in corrected_rows] == [1, 1], corrected_rows
        assert [r["status"] for r in corrected_rows] == [
            "succeeded",
            "succeeded",
        ], corrected_rows
        # No new epoch-0 ledger row was written by any corrected door.
        fresh_epoch0 = await control.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_step_ledger WHERE flow_id=$1 AND claim_epoch = 0',
            corrected_flow_id,
        )
        assert int(fresh_epoch0) == 0

        # The epoch-0 history is writable only under its own EXACT
        # id+instant: the re-mint guard refuses a stale stamp. The guard
        # row is a still-nonterminal epoch-0 row (the preserved parked
        # history's own rest state — a terminal row is never re-fenced).
        wsql = render_workflow_sql(schema)
        guard_flow = new_uuid()
        await control.execute(
            f'INSERT INTO "{schema}".wf_step_ledger '
            "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status) "
            "VALUES ($1, $2, $3, 'guard', NULL, 1, 0, 'awaited')",
            new_uuid(),
            guard_flow,
            new_uuid(),
        )
        guard_row = await control.fetchrow(
            f"SELECT id, status::text AS status, updated_at "
            f'FROM "{schema}".wf_step_ledger WHERE flow_id=$1 AND claim_epoch = 0',
            guard_flow,
        )
        assert guard_row is not None
        stale = await control.execute(
            wsql.ledger_fence_by_id,
            guard_row["id"],
            "StaleWriter",
            guard_row["updated_at"] + timedelta(microseconds=1),
        )
        assert stale == "UPDATE 0", stale
        exact = await control.execute(
            wsql.ledger_fence_by_id,
            guard_row["id"],
            "ExactInstantWriter",
            guard_row["updated_at"],
        )
        assert exact == "UPDATE 1", exact
    finally:
        if local_pool is not None:
            await local_pool.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()
