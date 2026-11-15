"""Real two-process P5 cancellation and durable terminal-effect scenarios.

Wait bounds describe responsive async bodies only. No claim is made for blocking
or non-yielding application code, and root terminal is not treated as body exit.
"""

# ruff: noqa: S608  # migrated fixture schema identifiers; every value binds

from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import FlowRunner
from taskq.workflows.api._runner_exit import cancel_workflow_run
from tests.system_e2e._harness import WorkerProc, graceful_stop, reap
from tests.system_e2e._p5_cancel_app import P5_FLOWS
from tests.system_e2e._wf_harness import spawn_wf_fleet

pytestmark = [pytest.mark.system, pytest.mark.integration]

_OBSERVE_BOUND_S = 120.0
_TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


def _doc(raw: object) -> dict[str, object]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


async def _ddl(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS "{schema}".p5_cancel_observations (
            flow_id uuid NOT NULL,
            node_key text NOT NULL,
            attempt int NOT NULL,
            observation text NOT NULL,
            origin text NOT NULL,
            reason text,
            principal text,
            worker_id uuid,
            incarnations int,
            PRIMARY KEY (flow_id,node_key,attempt,observation)
        )"""
    )
    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS "{schema}".p5_effect_results (
            flow_id uuid NOT NULL,
            node_key text NOT NULL,
            run_epoch bigint NOT NULL,
            record_id int NOT NULL,
            error_class text NOT NULL,
            invocations int NOT NULL,
            PRIMARY KEY (flow_id,node_key,run_epoch)
        )"""
    )
    await conn.execute(f'TRUNCATE "{schema}".p5_cancel_observations')
    await conn.execute(f'TRUNCATE "{schema}".p5_effect_results')


async def _create(pool: asyncpg.Pool, schema: str, kind: str, mode: str) -> str:
    runner = FlowRunner(P5_FLOWS[kind], pool, schema)
    claim = await runner.create_flow(input=mode, run_key=f"p5:{kind}:{new_uuid()}")
    await runner.aclose()
    return str(claim.flow_id)


async def _observations(
    conn: asyncpg.Connection, schema: str, flow_id: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f"""SELECT node_key,attempt,observation,origin,reason,principal,
                   worker_id::text,incarnations
            FROM "{schema}".p5_cancel_observations
            WHERE flow_id=$1::uuid ORDER BY node_key,attempt,observation""",
        flow_id,
    )
    return [dict(row) for row in rows]


async def _wait_observation(
    conn: asyncpg.Connection, schema: str, flow_id: str, observation: str = "entered"
) -> list[dict[str, Any]]:
    try:
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while True:
                rows = await _observations(conn, schema, flow_id)
                if any(row["observation"] == observation for row in rows):
                    return rows
                await asyncio.sleep(0.02)
    except TimeoutError as exc:
        root = await _root(conn, schema, flow_id)
        jobs = await conn.fetch(
            f"SELECT id,step_key,status,attempt,error_class,error_message,locked_by_worker "
            f"FROM \"{schema}\".jobs WHERE id=$1::uuid OR metadata->>'flow_id'=$1::text ORDER BY id",
            flow_id,
        )
        workers = await conn.fetch(f'SELECT id,metadata FROM "{schema}".workers')
        raise AssertionError(
            f"no {observation!r} row for {flow_id}; root={root!r}; "
            f"jobs={[dict(row) for row in jobs]!r}; "
            f"workers={[dict(row) for row in workers]!r}"
        ) from exc


async def _root(conn: asyncpg.Connection, schema: str, flow_id: str) -> dict[str, Any]:
    row = await conn.fetchrow(
        f"SELECT id,status::text AS status,run_epoch,required_worker_schema "
        f'FROM "{schema}".jobs WHERE id=$1::uuid',
        flow_id,
    )
    assert row is not None
    return dict(row)


async def _wait_root(
    conn: asyncpg.Connection, schema: str, flow_id: str, expected: str | None = None
) -> str:
    async with asyncio.timeout(_OBSERVE_BOUND_S):
        while True:
            root = await _root(conn, schema, flow_id)
            status = str(root["status"])
            if status in _TERMINAL and (expected is None or status == expected):
                return status
            await asyncio.sleep(0.05)


async def _settled_nodes(
    conn: asyncpg.Connection, schema: str, flow_id: str
) -> list[dict[str, Any]]:
    async with asyncio.timeout(_OBSERVE_BOUND_S):
        while True:
            rows = await conn.fetch(
                f"""SELECT id::text,step_key,map_index,status::text AS status,attempt,
                           locked_by_worker::text,error_class,metadata
                    FROM "{schema}".jobs
                    WHERE metadata->>'flow_id'=$1 ORDER BY id""",
                flow_id,
            )
            if rows and all(row["status"] in _TERMINAL for row in rows):
                return [dict(row) for row in rows]
            await asyncio.sleep(0.02)


async def _cancel(pool: asyncpg.Pool, schema: str, flow_id: str, kind: str) -> None:
    await cancel_workflow_run(
        pool,
        schema=schema,
        flow_id=flow_id,
        reason=f"operator-{kind}",
        principal="p5-normalized-principal",
    )


async def _holder(
    conn: asyncpg.Connection, schema: str, flow_id: str
) -> tuple[dict[str, Any], int]:
    """The actual executing claim's worker PID, read after body entry."""
    async with asyncio.timeout(_OBSERVE_BOUND_S):
        while True:
            row = await conn.fetchrow(
                f"""SELECT j.id::text,j.step_key,j.locked_by_worker::text,j.attempt,
                           j.error_class,w.pid
                    FROM "{schema}".jobs AS j
                    JOIN "{schema}".workers AS w ON w.id=j.locked_by_worker
                    WHERE j.metadata->>'flow_id'=$1 AND j.status='running'
                    ORDER BY j.id LIMIT 1""",
                flow_id,
            )
            if row is not None:
                return dict(row), int(row["pid"])
            await asyncio.sleep(0.02)


def _replace_holder(fleet: dict[str, WorkerProc], holder_pid: int) -> WorkerProc:
    matches = [pod for pod in fleet.values() if pod.proc.pid == holder_pid]
    assert len(matches) == 1, f"holder PID {holder_pid} is not a real fleet child"
    return matches[0]


@pytest.mark.timeout(600)
async def test_p5_operator_all_five_kinds_effect_and_parked_terminal_fence(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    schema = module_pg_schema.schema_name
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=6)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _ddl(conn, schema)
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["p5-a", "p5-b"])
        for kind in ("p5_static", "p5_map", "p5_chain", "p5_chain_source", "p5_loop"):
            flow_id = await _create(pool, schema, kind, "operator")
            entered = await _wait_observation(conn, schema, flow_id)
            assert entered and all(row["origin"] == "NONE" for row in entered)
            await _cancel(pool, schema, flow_id, kind)
            observed = await _wait_observation(conn, schema, flow_id, "observed")
            operator_rows = [row for row in observed if row["observation"] == "observed"]
            assert operator_rows, (kind, observed)
            for row in operator_rows:
                assert row["origin"] == "OPERATOR", (kind, row)
                assert row["reason"] == f"operator-{kind}", (kind, row)
                assert row["principal"] == "p5-normalized-principal", (kind, row)
                assert row["worker_id"] is not None
            assert await _wait_root(conn, schema, flow_id, "cancelled") == "cancelled"
            nodes = await _settled_nodes(conn, schema, flow_id)
            assert all(row["status"] in _TERMINAL for row in nodes)
            assert not any(row["status"] in ("pending", "scheduled") for row in nodes)
            # Cooperative running-success is a receipt, never a universal
            # all-cancelled assertion. Other ordinary rows are terminal.
            assert any(row["status"] == "succeeded" for row in nodes), (kind, nodes)
            print(
                f"[p5-operator] kind={kind} observations={len(operator_rows)} "
                f"nodes={[(row['step_key'], row['status']) for row in nodes]!r}"
            )

        for kind in ("p5_static", "p5_map", "p5_chain", "p5_chain_source", "p5_loop"):
            flow_id = await _create(pool, schema, kind, "operator-raise")
            await _wait_observation(conn, schema, flow_id)
            await _cancel(pool, schema, flow_id, f"{kind}-raise")
            raised = await _wait_observation(conn, schema, flow_id, "observed")
            observed_nodes = {row["node_key"] for row in raised if row["observation"] == "observed"}
            assert await _wait_root(conn, schema, flow_id) == "cancelled"
            nodes = await _settled_nodes(conn, schema, flow_id)
            for node_key in observed_nodes:
                body_rows = [row for row in nodes if row["step_key"] == node_key]
                assert body_rows
                for row in body_rows:
                    assert row["status"] == "cancelled" and row["attempt"] == 1
                    assert row["error_class"] == "WorkflowCancelled"
            assert not any(
                row["status"] == "failed" and row["step_key"] in observed_nodes for row in nodes
            ), (kind, nodes)
            print(f"[p5-operator-raise] kind={kind} nodes={observed_nodes!r}")

        # A deferred source no longer has a body; the retained carrier cannot
        # manufacture an observation. There is no second page or wake/reclaim.
        parked_id = await _create(pool, schema, "p5_parked", "operator")
        await _wait_observation(conn, schema, parked_id, "entered")
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while True:
                parked = await conn.fetchrow(
                    f"""SELECT status::text AS status,attempt,scheduled_at,metadata
                        FROM "{schema}".jobs
                        WHERE metadata->>'flow_id'=$1 AND step_key='parked_source'""",
                    parked_id,
                )
                assert parked is not None
                metadata = _doc(parked["metadata"])
                if (
                    parked["status"] == "scheduled"
                    and metadata.get("blocking_reason") == "backpressure"
                ):
                    break
                await asyncio.sleep(0.02)
        assert parked["attempt"] == 0 and parked["scheduled_at"] is None
        assert metadata["wf_source_logical_attempt"] == 1
        await _cancel(pool, schema, parked_id, "p5_parked")
        assert await _wait_root(conn, schema, parked_id, "cancelled") == "cancelled"
        parked_nodes = await _settled_nodes(conn, schema, parked_id)
        parked_observations = await _observations(conn, schema, parked_id)
        assert any(row["observation"] == "first-page" for row in parked_observations)
        assert not any(row["observation"] == "second-page" for row in parked_observations)
        parked_row = next(row for row in parked_nodes if row["step_key"] == "parked_source")
        assert parked_row["status"] == "cancelled"
        assert _doc(parked_row["metadata"])["wf_source_logical_attempt"] == 1
        assert not any(row["step_key"] == "work" and row["map_index"] == 2 for row in parked_nodes)
        ledger = await conn.fetchval(
            f'SELECT status FROM "{schema}".wf_step_ledger '
            "WHERE flow_id=$1::uuid AND step_key='parked_source'",
            parked_id,
        )
        assert ledger == "fenced"

        # The effect queue job is created from its durable intent after a
        # genuinely failed policy terminal and executes on the surviving fleet.
        effect_id = await _create(pool, schema, "p5_effect", "operator")
        effect_result: asyncpg.Record | None = None
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while effect_result is None:
                effect_result = await conn.fetchrow(
                    f'SELECT * FROM "{schema}".p5_effect_results WHERE flow_id=$1::uuid',
                    effect_id,
                )
                if effect_result is None:
                    await asyncio.sleep(0.02)
        assert effect_result["invocations"] == 1
        assert effect_result["run_epoch"] == 1
        assert effect_result["error_class"] == "RuntimeError"
        effect_nodes = await _settled_nodes(conn, schema, effect_id)
        effect_row = next(row for row in effect_nodes if row["step_key"] == "work.__on_exhaustion")
        assert effect_row["status"] == "succeeded"
        assert _doc(effect_row["metadata"])["wf_after_terminal_effect"] is True
        assert await _wait_root(conn, schema, effect_id, "failed") == "failed"

        deadline_id = await _create(pool, schema, "p5_effect_deadline", "operator")
        deadline_result: asyncpg.Record | None = None
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while deadline_result is None:
                deadline_result = await conn.fetchrow(
                    f'SELECT * FROM "{schema}".p5_effect_results WHERE flow_id=$1::uuid',
                    deadline_id,
                )
                if deadline_result is None:
                    await asyncio.sleep(0.05)
        deadline_nodes = await _settled_nodes(conn, schema, deadline_id)
        deadline_row = next(
            row for row in deadline_nodes if row["step_key"] == "work.__on_exhaustion"
        )
        assert deadline_row["status"] == "succeeded" and deadline_row["attempt"] == 2
        assert deadline_result["invocations"] == 1
        deadline_attempts = await conn.fetch(
            f"""SELECT attempt,status,error_class,error_message
                FROM "{schema}".wf_step_ledger
                WHERE flow_id=$1::uuid AND step_key='work.__on_exhaustion'
                ORDER BY attempt""",
            deadline_id,
        )
        assert len(deadline_attempts) == 2
        assert deadline_attempts[0]["status"] == "failed"
        assert deadline_attempts[0]["error_class"] == "TimeoutError"
        assert deadline_attempts[0]["error_message"] == "start_to_close"
        assert deadline_attempts[1]["status"] == "succeeded"

        kill_id = await _create(pool, schema, "p5_effect", "kill-after-domain")
        killed_before = {pod.proc.pid: pod.returncode for pod in fleet.values()}
        # One worker kills itself only after its guarded application commit.
        # The surviving worker re-claims the exact effect job after lease loss;
        # there is one result row but two actual effect invocations.
        kill_result: asyncpg.Record | None = None
        async with asyncio.timeout(_OBSERVE_BOUND_S):
            while kill_result is None or kill_result["invocations"] < 2:
                kill_result = await conn.fetchrow(
                    f'SELECT * FROM "{schema}".p5_effect_results WHERE flow_id=$1::uuid',
                    kill_id,
                )
                if kill_result is None or kill_result["invocations"] < 2:
                    await asyncio.sleep(0.05)
        kill_nodes = await _settled_nodes(conn, schema, kill_id)
        kill_effect = next(row for row in kill_nodes if row["step_key"] == "work.__on_exhaustion")
        assert kill_effect["status"] == "succeeded" and kill_effect["attempt"] == 2
        assert (
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".wf_outbox WHERE flow_id=$1::uuid', kill_id
            )
            == 1
        )
        actual_kills = [
            pod
            for pod in fleet.values()
            if (returncode := pod.poll()) is not None and returncode < 0
        ]
        assert len(actual_kills) == 1, (
            f"no actual effect-holder process was killed: before={killed_before!r}; "
            f"after={[(pod.proc.pid, pod.returncode) for pod in fleet.values()]!r}"
        )
        assert actual_kills[0].poll() == -9
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()


@pytest.mark.timeout(900)
async def test_p5_shutdown_started_attempt_stands_and_all_five_kinds_resume(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    schema = module_pg_schema.schema_name
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=6)
    conn = await asyncpg.connect(pg_dsn)
    generation = 0
    try:
        await _ddl(conn, schema)
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["p5-shutdown-a", "p5-shutdown-b"])
        for kind in ("p5_static", "p5_map", "p5_chain", "p5_chain_source", "p5_loop"):
            flow_id = await _create(pool, schema, kind, "shutdown")
            await _wait_observation(conn, schema, flow_id)
            holder, pid = await _holder(conn, schema, flow_id)
            pod = _replace_holder(fleet, pid)
            observed_before = await _observations(conn, schema, flow_id)
            assert all(row["origin"] == "NONE" for row in observed_before)
            # No cooperative observation performer's task is force-cancelled by
            # the run operator here: this is the real worker's SIGTERM path.
            rc = graceful_stop(pod, timeout=180)
            assert rc == 0, f"graceful {kind} holder exited {rc}"
            fleet = {name: other for name, other in fleet.items() if other is not pod}
            generation += 1
            fleet.update(await spawn_wf_fleet(conn, pg_dsn, schema, [f"p5-resume-{generation}"]))
            assert await _wait_root(conn, schema, flow_id, "succeeded") == "succeeded"
            observations = await _observations(conn, schema, flow_id)
            origins = [row for row in observations if row["observation"] == "observed"]
            assert origins and all(row["origin"] == "SHUTDOWN" for row in origins), (
                kind,
                observations,
            )
            assert all(row["reason"] is None and row["principal"] is None for row in origins)
            assert any(row["observation"] == "replayed" for row in observations)
            replay_workers = {
                row["worker_id"]
                for row in observations
                if row["observation"] in ("entered", "observed", "replayed")
            }
            assert len(replay_workers) >= 2, f"{kind} did not cross processes: {observations!r}"
            target_id = holder["id"]
            completed = await conn.fetchrow(
                f'SELECT attempt,status::text AS status,error_class FROM "{schema}".jobs WHERE id=$1::uuid',
                target_id,
            )
            assert completed is not None and completed["status"] == "succeeded"
            assert completed["attempt"] >= 2, (
                "the started shutdown claim was refunded instead of standing: "
                f"holder={holder!r}; completed={dict(completed)!r}"
            )
            assert (
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".wf_step_ledger '
                    "WHERE flow_id=$1::uuid AND job_id=$2::uuid AND status='failed'",
                    flow_id,
                    target_id,
                )
                == 0
            )
            print(
                f"[p5-shutdown] kind={kind} holder_pid={pid} "
                f"attempt={completed['attempt']} processes={len(replay_workers)}"
            )
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()
