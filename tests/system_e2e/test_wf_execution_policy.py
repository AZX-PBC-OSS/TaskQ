"""Two-process execution-policy controls on the real workflow dispatch fleet.

This is a short focused system scenario, not the separate ten-minute saturation
or 200k-record acceptance artifacts.
"""

# ruff: noqa: S608 — all schema identifiers are validated fixture values; data is bound.

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import FlowRunner
from tests.system_e2e._harness import WorkerProc, reap
from tests.system_e2e._wf_app import MARCH_FLOWS
from tests.system_e2e._wf_harness import spawn_wf_fleet

pytestmark = [pytest.mark.system, pytest.mark.integration]


async def _run(pool: asyncpg.Pool, schema: str, name: str) -> str:
    runner = FlowRunner(MARCH_FLOWS[name], pool, schema)
    return str((await runner.create_flow()).flow_id)


async def _wait_policy_root(
    conn: asyncpg.Connection, schema: str, flow_id: str, *, bound_s: float
) -> tuple[str, float]:
    """Read the maintained root, not the shim's key-collapsed chain projection."""
    async with asyncio.timeout(bound_s):
        start = asyncio.get_running_loop().time()
        while True:
            status = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid', flow_id
            )
            if status in ("succeeded", "failed", "cancelled", "crashed", "abandoned"):
                return str(status), asyncio.get_running_loop().time() - start
            await asyncio.sleep(0.05)
        # asyncio.timeout exposes the deadline only for cancellation handling;
        # the timeout's failure text below is provided by the caller.
    raise AssertionError("unreachable root wait")


def _max_overlap(rows: list[dict[str, Any]]) -> int:
    transitions: list[tuple[datetime, int]] = []
    for row in rows:
        assert isinstance(row["started_at"], datetime)
        assert isinstance(row["finished_at"], datetime)
        transitions.append((row["started_at"], 1))
        # At an equal timestamp, finishing releases before the next entry begins.
        transitions.append((row["finished_at"], -1))
    current = 0
    maximum = 0
    for _at, delta in sorted(transitions, key=lambda item: (item[0], item[1])):
        current += delta
        maximum = max(maximum, current)
    return maximum


async def _attempt_rows(
    conn: asyncpg.Connection, schema: str, flow_id: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f"""SELECT a.job_id, a.attempt, a.started_at, a.finished_at, a.outcome,
                   a.worker_id::text AS worker_id, j.map_index, j.created_at
            FROM "{schema}".job_attempts AS a
            JOIN "{schema}".jobs AS j ON j.id = a.job_id
            WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'
            ORDER BY a.started_at, j.map_index""",
        flow_id,
    )
    return [dict(row) for row in rows]


async def _wait_for_high_priority_row(
    conn: asyncpg.Connection, schema: str, flow_id: str, *, bound_s: float
) -> None:
    async with asyncio.timeout(bound_s):
        while True:
            terminal = await conn.fetchval(
                f"""SELECT count(*) FROM "{schema}".jobs
                    WHERE metadata->>'flow_id' = $1 AND step_key = 'work'
                      AND status = 'succeeded' AND finished_at IS NOT NULL""",
                flow_id,
            )
            if terminal == 1:
                return
            await asyncio.sleep(0.05)


@pytest.mark.timeout(300)
async def test_two_process_chain_policy_capacity_and_late_priority(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    schema = module_pg_schema.schema_name
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    conn = await asyncpg.connect(pg_dsn)
    try:
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["policy-a", "policy-b"])

        # Eight real chain records share one strict slot across two processes.
        reserved_flow = await _run(pool, schema, "policy_reserved_fleet")
        verdict, measured = await _wait_policy_root(conn, schema, reserved_flow, bound_s=90)
        print(f"[policy-reserved] terminal measured={measured:.3f}s bound=90s")
        assert verdict == "succeeded"
        attempts = await _attempt_rows(conn, schema, reserved_flow)
        if len(attempts) != 8:
            every_row = await conn.fetch(
                f"""SELECT id, parent_id, step_key, map_index, attempt, status::text AS status,
                           actor, locked_by_worker, error_class, error_message, metadata
                    FROM "{schema}".jobs
                    WHERE id = $1::uuid OR (metadata->>'flow_id')::uuid = $1::uuid
                    ORDER BY id""",
                reserved_flow,
            )
            raise AssertionError(
                f"reserved run did not have all eight attempts: attempts={attempts!r}; "
                f"all rows={[dict(row) for row in every_row]!r}"
            )
        assert all(row["outcome"] == "succeeded" for row in attempts)
        windows = await conn.fetch(
            f"""SELECT created_at AS started_at, updated_at AS finished_at
                FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1::uuid AND step_key = 'policy-reserved-window'
                ORDER BY created_at""",
            reserved_flow,
        )
        typed_windows = [dict(row) for row in windows]
        assert len(typed_windows) == 8
        observed_overlap = _max_overlap(typed_windows)
        if observed_overlap != 1:
            diagnostic = await conn.fetch(
                f"""SELECT j.id, j.map_index, j.actor, j.metadata, j.locked_by_worker,
                           j.status::text AS status, a.started_at, a.finished_at,
                           a.worker_id::text AS worker_id
                    FROM "{schema}".job_attempts AS a
                    JOIN "{schema}".jobs AS j ON j.id = a.job_id
                    WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'
                    ORDER BY a.started_at""",
                reserved_flow,
            )
            slots = await conn.fetch(
                f"""SELECT * FROM "{schema}".reservation_slots
                    WHERE bucket_name = 'wf-policy-reservation' ORDER BY slot_index"""
            )
            raise AssertionError(
                f"reserved overlap={observed_overlap}; attempts={[dict(r) for r in attempts]!r}; "
                f"rows={[dict(r) for r in diagnostic]!r}; slots={[dict(r) for r in slots]!r}"
            )
        workers = {row["worker_id"] for row in attempts}
        assert len(workers) == 2, workers
        held = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".reservation_slots
                WHERE bucket_name = 'wf-policy-reservation'
                  AND (job_id IS NOT NULL OR held_by_worker_id IS NOT NULL)"""
        )
        assert held == 0
        policy = await conn.fetchrow(
            f"""SELECT max_concurrent, result_ttl FROM "{schema}".actor_config
                WHERE actor = 'wf-policy-reserved'"""
        )
        assert policy is not None  # the chain cohort was projected at real boot
        assert policy["max_concurrent"] == 1  # the capped handle survived resolution/boot
        node = await conn.fetchrow(
            f"""SELECT start_to_close, priority, max_attempts FROM "{schema}".jobs
                WHERE metadata->>'flow_id' = $1 AND step_key = 'work' LIMIT 1""",
            reserved_flow,
        )
        assert node is not None
        assert node["start_to_close"] == timedelta(seconds=5)
        assert node["priority"] == 4 and node["max_attempts"] == 1

        # Active workers must stop before the priority fixture is born, so its
        # deeper backlog remains pending until both new processes start.
        for pod in fleet.values():
            reap(pod)
        fleet = {}

        priority_flow = await _run(pool, schema, "policy_priority_fleet")
        await conn.executemany(
            f"""INSERT INTO "{schema}".jobs
                (id, actor, queue, status, payload, max_attempts, retry_kind,
                 scheduled_at, idempotency_scope, idempotency_key, priority)
                VALUES ($1, 'wf_policy_backlog', 'default', 'pending',
                        '{{}}', 3, 'transient', clock_timestamp(), $2, $3, 0)""",
            [
                (
                    new_uuid(),
                    f"policy-priority-backlog:{schema}",
                    f"backlog-{index}",
                )
                for index in range(12)
            ],
        )
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["policy-pa", "policy-pb"])
        await _wait_for_high_priority_row(conn, schema, priority_flow, bound_s=45)
        high = await _attempt_rows(conn, schema, priority_flow)
        assert len(high) == 1
        assert all(row["outcome"] == "succeeded" for row in high)
        root_created = await conn.fetchval(
            f'SELECT created_at FROM "{schema}".jobs WHERE id = $1::uuid', priority_flow
        )
        assert all(row["created_at"] > root_created for row in high), (
            "all these policy rows must be source-materialized after create_flow"
        )

        low = await conn.fetch(
            f"""SELECT j.id, j.started_at FROM "{schema}".jobs AS j
                WHERE j.actor = 'wf_policy_backlog' AND j.started_at IS NOT NULL
                ORDER BY j.started_at, j.id"""
        )
        high_claims = sorted(row["started_at"] for row in high)
        async with asyncio.timeout(20):
            while not low:
                await asyncio.sleep(0.05)
                low = await conn.fetch(
                    f"""SELECT j.id, j.started_at FROM "{schema}".jobs AS j
                        WHERE j.actor = 'wf_policy_backlog' AND j.started_at IS NOT NULL
                        ORDER BY j.started_at, j.id"""
                )
        # A backlog claim that began before the source materialized its first
        # high row is legal. Every claim made after that row exists must respect
        # its priority; there must still be an actual later low claim to read.
        first_materialized = min(row["created_at"] for row in high)
        later_low = [row for row in low if row["started_at"] >= first_materialized]
        assert later_low, (first_materialized, low)
        assert max(high_claims) <= later_low[0]["started_at"], (
            f"high claims={high_claims!r}; materialized={first_materialized!r}; "
            f"later backlog={later_low[0]['started_at']!r}; earlier legal backlog may exist"
        )
        observed = await conn.fetchrow(
            f"""SELECT min(j.started_at) AS first, max(j.started_at) AS last
                FROM "{schema}".jobs AS j
                WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'""",
            priority_flow,
        )
        assert observed is not None and observed["first"] is not None
        actual_order = await conn.fetch(
            f"""SELECT j.actor, j.map_index, a.started_at
                FROM "{schema}".job_attempts AS a
                JOIN "{schema}".jobs AS j ON j.id = a.job_id
                WHERE (j.metadata->>'flow_id' = $1 AND j.step_key = 'work')
                   OR (j.actor = 'wf_policy_backlog' AND a.started_at >= $2)
                ORDER BY a.started_at, j.actor, j.map_index""",
            priority_flow,
            first_materialized,
        )
        high_positions = [
            index for index, row in enumerate(actual_order) if row["actor"] == "wf-policy-priority"
        ]
        assert high_positions and all(
            row["map_index"] in (0, 1, 2, 3) for row in actual_order[: max(high_positions) + 1]
        )

        for pod in fleet.values():
            reap(pod)
        fleet = {}
        # The release observation is read after both healthy processes stop;
        # enforcement itself was proved above, before cleanup.
        released = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".reservation_slots
                WHERE bucket_name = 'wf-policy-reservation'
                  AND (job_id IS NOT NULL OR held_by_worker_id IS NOT NULL)"""
        )
        assert released == 0
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()


def _repend_delays(metadata: str | dict[str, Any]) -> dict[str, float]:
    """The row's applied re-pend curve, keyed by attempt ordinal."""
    import json

    if isinstance(metadata, str):
        decoded = json.loads(metadata)
        assert isinstance(decoded, dict)
        metadata = decoded
    repend = metadata.get("wf_repend")
    assert isinstance(repend, dict)
    applied: dict[str, float] = {}
    for key, value in repend.items():
        assert isinstance(value, dict)
        applied[str(key)] = float(value["backoff"])  # type: ignore[union-attr]
    return applied


def _row_metadata_of(row: dict[str, Any]) -> dict[str, Any]:
    import json

    raw = row["metadata"]
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


async def _settled_work_row(
    conn: asyncpg.Connection, schema: str, flow_id: str, step_key: str, *, bound_s: float
) -> dict[str, Any]:
    """The named chain step's row once EVERY run row is terminal."""
    async with asyncio.timeout(bound_s):
        while True:
            rows = await conn.fetch(
                f"""SELECT id, step_key, map_index, status::text AS status, attempt,
                           max_attempts, snooze_count, priority, start_to_close,
                           error_class, error_message, metadata
                    FROM "{schema}".jobs
                    WHERE metadata->>'flow_id' = $1 AND step_key <> '__flow__'""",
                flow_id,
            )
            if rows and all(row["status"] in ("succeeded", "failed") for row in rows):
                return dict(next(row for row in rows if row["step_key"] == step_key))
            await asyncio.sleep(0.02)
    raise AssertionError("unreachable settled-row wait")


def _log_has_event(log_path: str | None, event: str, actor: str) -> bool:
    assert log_path is not None, "the capacity-owner diagnostic requires the worker's log sink"
    with open(log_path, "rb") as handle:
        log = handle.read().decode("utf-8", errors="replace")
    return event in log and actor in log


@pytest.mark.timeout(300)
async def test_two_process_chain_deadline_retry_and_capacity_owner_roll(
    pg_dsn: str, module_pg_schema: Any, tmp_path: Any
) -> None:
    """The #716 fleet surface on two real worker processes and real PostgreSQL:
    the declared exponential/linear/fixed curves land their exact applied rungs;
    an advertised non-consuming delay never spends the attempt budget; the
    engine's own start_to_close (not a body raise) owns the deadline terminal;
    and the chain cohorts' capacity owners roll by the declared law across a
    real worker replacement."""
    schema = module_pg_schema.schema_name
    log_sink = str(tmp_path / "policy-roll-worker.log")
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    conn = await asyncpg.connect(pg_dsn)
    try:
        # The operator pre-seeds both cohort rows (as `taskq actor-config set`
        # would); ownership is the boot-time DECLARATION's property, never a
        # stored column: the code-owned cohort's literal deploys by code on
        # every boot, the operator-owned cohort's stored row is never
        # capacity-touched by the boot.
        for actor in ("wf-policy-code-cap", "wf-policy-operator-cap"):
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config '
                "(actor, queue, max_concurrent, metadata) "
                "VALUES ($1, 'default', 2, '{\"workflow_actor\": true}'::jsonb) "
                "ON CONFLICT (actor) DO UPDATE SET max_concurrent = 2",
                actor,
            )

        fleet = await spawn_wf_fleet(
            conn,
            pg_dsn,
            schema,
            ["roll-a"],
            log_sink=log_sink,
            extra_env={"TASKQ_LOG_LEVEL": "INFO"},
        )

        # The code-owned cohort's literal deployed over the operator's stored 2;
        # the operator-owned cohort's stored 2 was never overwritten by the
        # code's 1.
        code_row = await conn.fetchrow(
            f'SELECT max_concurrent FROM "{schema}".actor_config '
            "WHERE actor = 'wf-policy-code-cap'"
        )
        operator_row = await conn.fetchrow(
            f'SELECT max_concurrent FROM "{schema}".actor_config '
            "WHERE actor = 'wf-policy-operator-cap'"
        )
        assert code_row is not None
        code_config = dict(code_row)
        assert code_config["max_concurrent"] == 1, code_config
        assert operator_row is not None
        operator_config = dict(operator_row)
        assert operator_config["max_concurrent"] == 2, operator_config

        # The rolling-curve flow: three records, one per declared curve; each
        # fails attempts 1-3 and succeeds on attempt 4, so each row's applied
        # wf_repend carries the curve's DISTINCT third rung
        # (exponential base*4 = 0.2, linear base*3 = 0.15, fixed base = 0.05).
        flow_by_curve = {
            "exponential": ("policy_exp_fleet", "exp", 0.2),
            "linear": ("policy_lin_fleet", "lin", 0.15),
            "fixed": ("policy_fix_fleet", "fix", 0.05),
        }
        for curve, (flow_name, step_key, expected_third_rung) in flow_by_curve.items():
            flow_id = await _run(pool, schema, flow_name)
            row = await _settled_work_row(conn, schema, flow_id, step_key, bound_s=90)
            assert row["status"] == "succeeded", (curve, row)
            assert row["step_key"] == step_key and row["attempt"] == 4, (curve, row)
            assert row["max_attempts"] == 4 and row["snooze_count"] == 0, (curve, row)
            delays = _repend_delays(row["metadata"])
            assert set(delays) == {"1", "2", "3"}, (curve, delays)
            # The declared curve's own rungs land as the applied values.
            # jitter=0: the applied value IS the curve's rung.
            assert delays["3"] == expected_third_rung, (curve, delays)
            rungs = [delays[key] for key in sorted(delays, key=int)]
            assert rungs == sorted(rungs), (curve, rungs)
            failed_ledger = await conn.fetchval(
                f"""SELECT count(*) FROM "{schema}".wf_step_ledger
                    WHERE flow_id = $1::uuid AND status = 'failed'""",
                flow_id,
            )
            assert failed_ledger == 3, (curve, failed_ledger)
            print(f"[policy-roll] curve={curve} rungs={delays!r}")

        # The non-consuming advertised delay: two waits, no attempt spend, no
        # snooze_count move, then the bounded terminal.
        nc_flow = await _run(pool, schema, "policy_non_consuming_fleet")
        nc_row = await _settled_work_row(conn, schema, nc_flow, "work", bound_s=90)
        assert nc_row["status"] == "failed"
        assert nc_row["error_class"] == "RetryDeferralBudgetExceeded", nc_row
        assert nc_row["attempt"] == 1, ("the advertised waits never consumed the budget", nc_row)
        assert nc_row["max_attempts"] == 1 and nc_row["snooze_count"] == 0, nc_row
        metadata = _row_metadata_of(nc_row)
        retry_meta = metadata.get("wf_retry")
        assert isinstance(retry_meta, dict)
        assert retry_meta["non_consuming_count"] == 2, retry_meta
        assert retry_meta["non_consuming_limit"] == 2, retry_meta
        # The deferral stamp carries the ADVERTISED (non-consuming) signal;
        # no wf_repend ladder history and no snooze_count move exist — the
        # advertised wait is not a consumed retry.
        deferral = metadata.get("wf_deferral")
        assert isinstance(deferral, dict)
        assert deferral["consume_budget"] is False, deferral
        assert deferral["signal"] == "PolicyRollTransientError", deferral
        assert "wf_repend" not in metadata, metadata.get("wf_repend")
        nc_failed = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1::uuid AND status = 'failed'""",
            nc_flow,
        )
        assert nc_failed == 1, nc_failed
        print(f"[policy-roll] non-consuming count={retry_meta} deferral={deferral!r}")

        # The engine's own deadline: the body never raises; the row's terminal
        # is the enforcement's TimeoutError ('start_to_close').
        deadline_flow = await _run(pool, schema, "policy_deadline_fleet")
        deadline_row = await _settled_work_row(conn, schema, deadline_flow, "work", bound_s=90)
        assert deadline_row["status"] == "failed"
        assert deadline_row["error_class"] == "TimeoutError", deadline_row
        assert deadline_row["error_message"] == "start_to_close", deadline_row
        assert deadline_row["start_to_close"] == timedelta(seconds=0.4)
        assert deadline_row["attempt"] == 1, deadline_row
        print("[policy-roll] deadline row:", deadline_row["error_class"], "engine-enforced")

        # The code-owned cap re-converges across a real worker replacement:
        # the operator drifts the stored row, the replacement worker's boot
        # re-deploys the code's literal and logs the change diagnostic.
        await conn.execute(
            f'UPDATE "{schema}".actor_config SET max_concurrent = 5 '
            "WHERE actor = 'wf-policy-code-cap'"
        )
        for pod in fleet.values():
            reap(pod)
        fleet = {}
        fleet = await spawn_wf_fleet(
            conn,
            pg_dsn,
            schema,
            ["roll-b"],
            log_sink=log_sink,
            extra_env={"TASKQ_LOG_LEVEL": "INFO"},
        )
        code_row = await conn.fetchrow(
            f'SELECT max_concurrent FROM "{schema}".actor_config '
            "WHERE actor = 'wf-policy-code-cap'"
        )
        assert code_row is not None
        code_config = dict(code_row)
        assert code_config["max_concurrent"] == 1, (
            "the code-owned cap must re-converge at the replacement worker's boot",
            code_config,
        )
        assert _log_has_event(
            log_sink + "-roll-b", "actor-config-capacity-code-change", "wf-policy-code-cap"
        ), "the code-owned cap's change diagnostic must name the actor in the worker's log"
        # The operator-owned cohort's override event is INFO-level
        # (startup's sync); the worker entry's default root logging level
        # filters it from the log file, so its non-overwrite law is asserted
        # on the stored row above, and the diagnostic pair itself is pinned
        # in-process by test_actor_config_capacity_owner_sync.py.
        assert not _log_has_event(
            log_sink + "-roll-b", "actor-config-capacity-code-change", "wf-policy-operator-cap"
        ), "an operator-owned cohort must never log the code-change diagnostic"
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()
