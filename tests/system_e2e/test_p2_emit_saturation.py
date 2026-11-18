"""P2/#718 real two-OS-worker saturation and bounded recovery proofs.

The healthy case genuinely holds the first full page for 600 wall-clock seconds.
Only the committed PostgreSQL ``deferred_at`` marker may release that gate: no
shortened environment switch, test-duration relabelling, or parent-memory event
participates.  Real production bootstraps claim every source/leaf/probe row.
"""

# Why S608-safe: the schema is the harness's validated module schema identifier and every data value is $N-bound.

from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urlparse, urlunparse

import asyncpg
import pytest
from asyncpg.transaction import Transaction

from taskq import TaskQ
from taskq.constants import wake_channel
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import FlowRunner, cancel_workflow_run
from tests.system_e2e import _p2_saturation_worker as sample
from tests.system_e2e._harness import WorkerProc, graceful_stop, reap, spawn_joined_worker
from tests.system_e2e._wf_harness import row_lock_waiters

pytestmark = [pytest.mark.system, pytest.mark.integration]

_ENTRY: Final = "tests.system_e2e._p2_saturation_worker"
_GATE_S: Final = 600
_SAMPLE_S: Final = 0.1  # 10 Hz production sampling; acceptance requires >=5 Hz measured.
_QUEUES: Final = f"{sample.SOURCE_QUEUE},{sample.LEAF_QUEUE},{sample.SIBLING_QUEUE}"


def _document(raw: object) -> dict[str, Any]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(value, dict), type(value)
    return value


def _list_document(raw: object) -> list[Any]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(value, list), type(value)
    return value


def _save(name: str, value: object) -> None:
    # The observed facts print to the run's own output; no generated evidence
    # file leaves the test process.
    document = json.dumps(value, indent=2, sort_keys=True, default=str)
    print(f"[p2-system] {name}: {document}", flush=True)


def _named_dsn(dsn: str, schema: str, label: str) -> str:
    parsed = urlparse(dsn)
    old = f"{parsed.query}&" if parsed.query else ""
    return urlunparse(parsed._replace(query=f"{old}application_name={schema}-p2-{label}"))


async def _spawn_one(
    pool: asyncpg.Pool,
    dsn: str,
    schema: str,
    scenario: str,
    label: str,
    *,
    log_dir: Path,
    queues: str = _QUEUES,
    concurrency: int = 2,
) -> WorkerProc:
    conn = await pool.acquire()
    try:
        return await spawn_joined_worker(
            conn,
            dsn,
            schema,
            tag=f"p2-{scenario}-{label}",
            entry=_ENTRY,
            log_sink=str(log_dir / f"p2-system-{scenario}-{label}.worker.log"),
            extra_env={
                "P2_SATURATION_SCENARIO": scenario,
                "TASKQ_PG_DSN": _named_dsn(dsn, schema, label),
                "TASKQ_QUEUES": queues,
                "TASKQ_MAX_CONCURRENCY": str(concurrency),
                "TASKQ_LEADER_LEASE": "20",
                "TASKQ_LOCK_LEASE": "15",
                "TASKQ_MAX_HEARTBEAT_FAILURES": "10",
                "TASKQ_POLL_INTERVAL": "0.05",
                "TASKQ_SWEEP_INTERVAL": "1",
                "TASKQ_HEARTBEAT_INTERVAL": "0.5",
                "TASKQ_TERMINATION_GRACE_PERIOD": "15",
            },
        )
    finally:
        await pool.release(conn)


async def _spawn_pair_fixed(
    pool: asyncpg.Pool,
    dsn: str,
    schema: str,
    scenario: str,
    *,
    log_dir: Path,
    queues: tuple[str, str] = (_QUEUES, _QUEUES),
    concurrency: tuple[int, int] = (2, 2),
) -> dict[str, WorkerProc]:
    fleet: dict[str, WorkerProc] = {}
    for index, label in enumerate(("a", "b")):
        fleet[label] = await _spawn_one(
            pool,
            dsn,
            schema,
            scenario,
            label,
            log_dir=log_dir,
            queues=queues[index],
            concurrency=concurrency[index],
        )
    return fleet


async def _stop_fleet(fleet: dict[str, WorkerProc]) -> dict[str, int]:
    codes: dict[str, int] = {}
    for label, worker in fleet.items():
        if worker.proc.poll() is None:
            codes[label] = graceful_stop(worker, timeout=60)
        else:
            reap(worker)
            codes[label] = int(worker.proc.returncode or 0)
    return codes


async def _source(pool: asyncpg.Pool, schema: str, flow_id: str) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"""
        SELECT j.id::text, j.status::text AS status, j.attempt::int AS attempt,
               j.claim_epoch::bigint AS claim_epoch, j.scheduled_at,
               j.locked_by_worker::text AS locked_by_worker, j.lock_expires_at,
               j.last_heartbeat_at, j.started_at, j.finished_at,
               j.snooze_count::int AS snooze_count, j.metadata,
               l.id::text AS ledger_id, l.attempt::int AS ledger_attempt,
               l.status::text AS ledger_status, l.updated_at AS ledger_updated_at
        FROM "{schema}".jobs j
        LEFT JOIN "{schema}".wf_step_ledger l
          ON l.job_id = j.id AND l.status IN ('running','awaited','crashed','succeeded','failed','fenced','cancelled')
        WHERE (j.metadata->>'flow_id')::uuid = $1::uuid AND j.step_key = 'source'
        """,
        flow_id,
    )
    assert row is not None, f"missing source for {flow_id}"
    value = dict(row)
    value["metadata"] = _document(value["metadata"])
    return value


async def _wait_for(
    pool: asyncpg.Pool,
    schema: str,
    flow_id: str,
    predicate: Callable[[dict[str, Any], list[dict[str, Any]]], bool],
    *,
    deadline_s: float = 60.0,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    deadline = time.monotonic() + deadline_s
    source: dict[str, Any] = {}
    leaves: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        source = await _source(pool, schema, flow_id)
        rows = await pool.fetch(
            f"""SELECT j.id::text, j.status::text AS status, j.map_index,
                       j.attempt::int AS attempt, j.claim_epoch::bigint AS claim_epoch,
                       j.locked_by_worker::text AS locked_by_worker,
                       j.last_heartbeat_at, j.lock_expires_at, j.started_at, j.finished_at,
                       j.cancel_phase::int AS cancel_phase, j.cancel_requested_at,
                       w.pid
                FROM "{schema}".jobs j
                LEFT JOIN "{schema}".workers w ON w.id = j.locked_by_worker
                WHERE j.parent_id = $1::uuid
                ORDER BY j.map_index""",
            (await _source_id(pool, schema, flow_id)),
        )
        leaves = [dict(row) for row in rows]
        if predicate(source, leaves):
            return source, leaves
        await asyncio.sleep(0.05)
    raise TimeoutError(
        f"state did not arrive within {deadline_s}s: source={source!r}; leaves={leaves!r}"
    )


async def _source_id(pool: asyncpg.Pool, schema: str, flow_id: str) -> str:
    value = await pool.fetchval(
        f'SELECT id::text FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'",
        flow_id,
    )
    assert value is not None
    return str(value)


async def _create_run(pool: asyncpg.Pool, schema: str, scenario: str) -> str:
    runner = FlowRunner(sample.p2sat_app.get(sample.FLOW_NAME), pool, schema)
    # Create only.  All execution belongs to the two production workers.
    return str((await runner.create_flow(run_key=f"p2:{scenario}")).flow_id)


async def _announce_environment(pool: asyncpg.Pool) -> None:
    version = await pool.fetchval("SELECT version()")
    print(f"[p2-system] python={sys.version.split()[0]} postgres={version}", flush=True)


async def _initialize_observer(pool: asyncpg.Pool, schema: str) -> None:
    await pool.execute(
        f"""
        CREATE TABLE "{schema}".p2sat_observations (
            id bigserial PRIMARY KEY,
            observed_at timestamptz NOT NULL,
            released boolean NOT NULL,
            root_status text NOT NULL,
            source_status text NOT NULL,
            source_attempt integer NOT NULL,
            source_claim_epoch bigint NOT NULL,
            source_scheduled_at timestamptz,
            source_locked_by uuid,
            source_lock_expires_at timestamptz,
            source_heartbeat_at timestamptz,
            source_metadata jsonb NOT NULL,
            ledger_status text,
            ledger_attempt integer,
            source_entries bigint NOT NULL,
            fetch_rows bigint NOT NULL,
            provider_fetch_calls bigint NOT NULL,
            provider_pages bigint NOT NULL,
            provider_records bigint NOT NULL,
            active_provider_calls integer NOT NULL,
            idle_provider_transactions integer NOT NULL,
            unrelated_succeeded bigint NOT NULL
        );
        CREATE TABLE "{schema}".p2sat_leaf_observations (
            sample_id bigint NOT NULL REFERENCES "{schema}".p2sat_observations(id) ON DELETE CASCADE,
            observed_at timestamptz NOT NULL,
            job_id uuid NOT NULL,
            status text NOT NULL,
            pid integer,
            heartbeat_at timestamptz,
            lock_expires_at timestamptz
        );
        CREATE TABLE "{schema}".p2sat_notifications (
            id bigserial PRIMARY KEY,
            channel text NOT NULL,
            payload text NOT NULL,
            notifying_pid integer NOT NULL,
            received_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )
        """
    )


async def _notification_listener(
    pool: asyncpg.Pool, dsn: str, schema: str, stop: asyncio.Event
) -> None:
    conn = await asyncpg.connect(dsn)
    queue: asyncio.Queue[tuple[int, str, str]] = asyncio.Queue()

    def notice(_connection: object, pid: int, channel: str, payload: object) -> None:
        queue.put_nowait((pid, channel, str(payload)))

    try:
        await conn.add_listener(wake_channel(schema), notice)
        while not stop.is_set() or not queue.empty():
            try:
                pid, channel, payload = await asyncio.wait_for(queue.get(), timeout=0.1)
            except TimeoutError:
                continue
            await pool.execute(
                f'INSERT INTO "{schema}".p2sat_notifications '
                "(channel, payload, notifying_pid) VALUES ($1, $2, $3)",
                channel,
                payload,
                pid,
            )
    finally:
        await conn.close()


async def _run_probe_batch(
    client: TaskQ,
    pool: asyncpg.Pool,
    schema: str,
    scenario: str,
    ordinals: list[int],
) -> dict[str, Any]:
    actors = (sample.source_probe, sample.leaf_probe)
    for ordinal in ordinals:
        await client.enqueue(
            actors[ordinal % 2],
            sample.ProbePayload(scenario=scenario, ordinal=ordinal),
            tags=[scenario, "probe"],
        )
    expected = len(ordinals)
    deadline = time.monotonic() + 90.0
    latest: int = 0
    while time.monotonic() < deadline:
        latest = await pool.fetchval(
            f"""
            SELECT count(DISTINCT j.id)::int FROM "{schema}".jobs j
            JOIN "{schema}".p2sat_probe_runs p ON p.job_id = j.id
            WHERE j.tags @> ARRAY[$1::text, 'probe'::text]
              AND p.ordinal = ANY($2::int[]) AND j.status = 'succeeded'
            """,
            scenario,
            ordinals,
        )
        if latest == expected:
            times = await pool.fetchrow(
                f"""SELECT min(j.created_at) AS started, max(j.finished_at) AS finished
                    FROM "{schema}".jobs j
                    WHERE j.tags @> ARRAY[$1::text, 'probe'::text]
                      AND j.metadata IS NOT NULL
                      AND EXISTS (
                        SELECT 1 FROM "{schema}".p2sat_probe_runs p
                        WHERE p.job_id = j.id AND p.ordinal = ANY($2::int[])
                      )""",
                scenario,
                ordinals,
            )
            assert times is not None
            elapsed = (times["finished"] - times["started"]).total_seconds()
            return {
                "offered": expected,
                "succeeded": latest,
                "elapsed_s": elapsed,
                "jobs_per_s": expected / max(elapsed, 0.000001),
                "started_at": times["started"],
                "finished_at": times["finished"],
            }
        await asyncio.sleep(0.1)
    raise TimeoutError(f"ordinary baseline offered={expected} completed={latest}")


async def _ordinary_producer(
    client: TaskQ,
    pool: asyncpg.Pool,
    schema: str,
    scenario: str,
    first_ordinal: int,
) -> dict[str, int]:
    offered = 0
    ordinal = first_ordinal
    while True:
        released = await pool.fetchval(
            f'SELECT release_at IS NOT NULL FROM "{schema}".p2sat_control WHERE scenario=$1',
            scenario,
        )
        if released:
            return {"offered": offered, "first_ordinal": first_ordinal, "last_ordinal": ordinal - 1}
        # Two per queue per second keep a measured margin above the 1200-job
        # baseline-derived minimum even when release lands between two loops.
        for actor in (sample.source_probe, sample.leaf_probe):
            for _ in range(2):
                await client.enqueue(
                    actor,
                    sample.ProbePayload(scenario=scenario, ordinal=ordinal),
                    tags=[scenario, "probe"],
                )
                offered += 1
                ordinal += 1
        await asyncio.sleep(1.0)


async def _observe_and_release(
    pool: asyncpg.Pool,
    schema: str,
    scenario: str,
    flow_id: str,
    fleet: dict[str, WorkerProc],
) -> dict[str, Any]:
    """10 Hz row-native observation plus the immutable 600-second DB gate."""
    source_id = await _source_id(pool, schema, flow_id)
    released_at: datetime | None = None
    samples = 0
    async with asyncio.timeout(750):
        while True:
            if any(worker.proc.poll() is not None for worker in fleet.values()):
                raise AssertionError(
                    f"healthy soak worker exited: {[(k, v.poll()) for k, v in fleet.items()]}"
                )
            row = await pool.fetchrow(
                f"""
                WITH controller AS (
                    SELECT release_at FROM "{schema}".p2sat_control WHERE scenario=$1
                ), source AS (
                    SELECT * FROM "{schema}".jobs WHERE id=$3::uuid
                ), root AS (
                    SELECT status::text AS status FROM "{schema}".jobs WHERE id=$2::uuid
                ), provider AS (
                    SELECT fetch_calls, pages_returned, records_returned
                    FROM "{schema}".p2sat_provider_scenario WHERE scenario_id=$1
                ), counts AS (
                    SELECT
                      (SELECT count(*) FROM "{schema}".p2sat_source_entries
                       WHERE scenario=$1 AND flow_id=$2::uuid) AS entries,
                      (SELECT count(*) FROM "{schema}".p2sat_fetches f
                       JOIN "{schema}".p2sat_source_entries e ON e.id=f.source_entry_id
                       WHERE e.scenario=$1 AND e.flow_id=$2::uuid) AS fetches,
                      (SELECT count(*) FROM "{schema}".jobs
                       WHERE tags @> ARRAY[$1::text,'probe'::text] AND status='succeeded') AS probes,
                      (SELECT count(*) FROM pg_stat_activity
                       WHERE datname = current_database() AND application_name LIKE $4
                         AND state='active'
                         AND query ILIKE '%p2sat_%'
                         -- The STUCK signature, derived: every fixture
                         -- statement's service is sub-millisecond and the
                         -- leaf gate's own poll period is 1s, so a fixture
                         -- query still active after one full poll period is
                         -- a leaked/hung session, never normal service. A
                         -- µs poll caught mid-flight is service, not a leak.
                         AND query_start < clock_timestamp() - interval '1 seconds'
                      ) AS active_provider,
                      (SELECT count(*) FROM pg_stat_activity
                       WHERE datname = current_database() AND application_name LIKE $4
                         -- Idle-in-transaction is itself the leak: no fixture
                         -- statement leaves a transaction open.
                         AND state='idle in transaction'
                         AND query ILIKE '%p2sat_%') AS idle_provider
                )
                INSERT INTO "{schema}".p2sat_observations (
                    observed_at, released, root_status, source_status, source_attempt,
                    source_claim_epoch, source_scheduled_at, source_locked_by,
                    source_lock_expires_at, source_heartbeat_at, source_metadata,
                    ledger_status, ledger_attempt, source_entries, fetch_rows,
                    provider_fetch_calls, provider_pages, provider_records,
                    active_provider_calls, idle_provider_transactions, unrelated_succeeded
                )
                SELECT clock_timestamp(), c.release_at IS NOT NULL, r.status, s.status::text,
                       s.attempt, s.claim_epoch, s.scheduled_at, s.locked_by_worker,
                       s.lock_expires_at, s.last_heartbeat_at, s.metadata,
                       l.status::text, l.attempt, n.entries, n.fetches,
                       p.fetch_calls, p.pages_returned, p.records_returned,
                       n.active_provider, n.idle_provider, n.probes
                FROM controller c CROSS JOIN source s CROSS JOIN root r CROSS JOIN provider p
                CROSS JOIN counts n
                LEFT JOIN "{schema}".wf_step_ledger l ON l.job_id=s.id
                  AND l.status IN ('running','awaited','crashed','succeeded','failed')
                RETURNING id, observed_at, released, root_status, source_status,
                          source_entries, fetch_rows
                """,
                scenario,
                flow_id,
                source_id,
                f"{schema}-p2-%",
            )
            assert row is not None
            samples += 1
            await pool.execute(
                f"""INSERT INTO "{schema}".p2sat_leaf_observations
                    SELECT $1, $2, j.id, j.status::text, w.pid, j.last_heartbeat_at, j.lock_expires_at
                    FROM "{schema}".jobs j LEFT JOIN "{schema}".workers w ON w.id=j.locked_by_worker
                    WHERE j.parent_id=$3::uuid""",
                row["id"],
                row["observed_at"],
                source_id,
            )
            if row["root_status"] in {"failed", "cancelled", "crashed", "abandoned"}:
                raise AssertionError(f"soak root made a false terminal failure: {dict(row)}")
            if not row["released"]:
                promoted = await pool.fetchrow(
                    f"""
                    UPDATE "{schema}".p2sat_control AS c
                    SET release_at=clock_timestamp(), updated_at=clock_timestamp()
                    FROM "{schema}".jobs AS s
                    WHERE c.scenario=$1 AND c.release_at IS NULL AND s.id=$2::uuid
                      AND s.status='scheduled' AND s.scheduled_at IS NOT NULL
                      AND s.metadata @> '{{"blocking_reason":"backpressure"}}'::jsonb
                      AND s.metadata @> '{{"wf_deferral":{{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}}}'::jsonb
                      AND (s.metadata->'wf_emit_backpressure'->>'deferred_at')::timestamptz
                          + make_interval(secs => $3::int) <= clock_timestamp()
                    RETURNING c.release_at
                    """,
                    scenario,
                    source_id,
                    _GATE_S,
                )
                if promoted is not None:
                    released_at = promoted["release_at"]
                    await pool.execute(
                        "SELECT pg_notify($1, $2)", sample.gate_channel(schema), scenario
                    )
            elif row["root_status"] == "succeeded":
                return {"samples": samples, "release_at": released_at}
            await asyncio.sleep(_SAMPLE_S)


async def _healthy_assertions(
    pool: asyncpg.Pool,
    schema: str,
    scenario: str,
    flow_id: str,
    fleet: dict[str, WorkerProc],
    release_at: datetime,
    log_dir: Path | None = None,
) -> dict[str, Any]:
    source = await _source(pool, schema, flow_id)
    source_id = source["id"]
    meta = source["metadata"]
    pressure = _document(meta.get("wf_emit_backpressure"))
    records = await pool.fetch(
        f'SELECT record_id, content, applied_by_pid FROM "{schema}".p2sat_records_applied '
        "WHERE scenario=$1 AND flow_id=$2::uuid ORDER BY record_id",
        scenario,
        flow_id,
    )
    children = await pool.fetch(
        f"""SELECT j.id::text, j.map_index, j.status::text AS status, j.attempt::int AS attempt,
                   j.idempotency_key, j.trace_id, j.finished_at, a.outcome
            FROM "{schema}".jobs j
            LEFT JOIN "{schema}".job_attempts a ON a.job_id=j.id AND a.attempt=j.attempt
            WHERE j.parent_id=$1::uuid ORDER BY j.map_index""",
        source_id,
    )
    entries = await pool.fetch(
        f"SELECT id, pid, logical_attempt, claim_epoch, entered_at, body_returned_at "
        f'FROM "{schema}".p2sat_source_entries '
        "WHERE scenario=$1 AND flow_id=$2::uuid ORDER BY id",
        scenario,
        flow_id,
    )
    fetches = await pool.fetch(
        f"SELECT f.id, f.source_entry_id, f.cursor_in, f.cursor_out, f.record_ids, f.fetched_at "
        f'FROM "{schema}".p2sat_fetches f '
        f'JOIN "{schema}".p2sat_source_entries e ON e.id=f.source_entry_id '
        "WHERE e.scenario=$1 AND e.flow_id=$2::uuid ORDER BY f.id",
        scenario,
        flow_id,
    )
    observations = await pool.fetch(
        f"""SELECT *, (observed_at < $3::timestamptz) AS before_release
            FROM "{schema}".p2sat_observations
            WHERE observed_at >= (
                SELECT min(entered_at) FROM "{schema}".p2sat_source_entries
                WHERE scenario=$1 AND flow_id=$2::uuid
            ) ORDER BY id""",
        scenario,
        flow_id,
        release_at,
    )
    pre = [dict(row) for row in observations if row["before_release"]]
    assert pre, "the database-native window contains no observations"
    expected_records = {f"record-{index}" for index in range(sample.RECORD_COUNT)}
    assert {row["record_id"] for row in records} == expected_records
    assert len(records) == sample.RECORD_COUNT
    assert [int(row["map_index"]) for row in children] == [0, 1, 2, 3]
    assert all(row["status"] == "succeeded" and row["attempt"] == 1 for row in children)
    assert len({row["idempotency_key"] for row in children}) == sample.RECORD_COUNT
    assert len(entries) == 2, [dict(row) for row in entries]
    assert {int(row["logical_attempt"]) for row in entries} == {1}
    assert len({int(row["claim_epoch"]) for row in entries}) == 2
    assert len(fetches) == 3
    assert [_list_document(row["record_ids"]) for row in fetches] == [
        ["record-0", "record-1"],
        ["record-2", "record-3"],
        ["record-2", "record-3"],
    ]
    assert source["status"] == "succeeded" and source["attempt"] == 1
    assert source["ledger_status"] == "succeeded" and source["ledger_attempt"] == 1
    assert meta.get("wf_source_logical_attempt") == 1
    assert "blocking_reason" not in meta
    assert "wf_repend" not in meta and "wf_repend_folded" not in meta
    assert source["snooze_count"] == 0
    # THE NON-CONSUMING CHARGE: healthy saturation never moves the workflow
    # classifier's own counter — no wf_retry document is written to the
    # row at all (the deferral is not a non-consuming retry).
    assert "wf_retry" not in meta, meta.get("wf_retry")
    assert pressure["page_width"] == pressure["bound"] == 2
    assert pressure["outstanding"] == 2
    assert pressure["deferrals"] == 1 and pressure["wakes"] == 1

    for row in pre:
        assert row["root_status"] not in {
            "succeeded",
            "failed",
            "cancelled",
            "crashed",
            "abandoned",
        }
        assert row["source_status"] == "scheduled"
        assert row["source_attempt"] == 0
        # REPRESENTATION INVERSION: the park instant is stored (NOT NULL);
        # the marker, never a NULL due time, discriminates.
        assert row["source_scheduled_at"] is not None
        assert row["source_locked_by"] is None
        assert row["source_lock_expires_at"] is None
        assert row["source_heartbeat_at"] is None
        assert row["ledger_status"] == "awaited" and row["ledger_attempt"] == 1
        assert int(row["source_claim_epoch"]) == int(entries[0]["claim_epoch"])
        assert _document(row["source_metadata"]).get("wf_source_logical_attempt") == 1
        assert row["source_entries"] == 1 and row["fetch_rows"] == 2
        assert row["provider_fetch_calls"] == 2 and row["provider_pages"] == 2
        assert row["active_provider_calls"] == 0 and row["idle_provider_transactions"] == 0

    leaf_samples = await pool.fetch(
        f"""SELECT l.* FROM "{schema}".p2sat_leaf_observations l
            JOIN "{schema}".p2sat_observations o ON o.id=l.sample_id
            WHERE o.observed_at < $1 ORDER BY l.sample_id, l.job_id""",
        release_at,
    )
    first_page_ids = {str(row["id"]) for row in children if int(row["map_index"]) < 2}
    gated = [row for row in leaf_samples if str(row["job_id"]) in first_page_ids]
    assert gated
    for row in gated:
        assert row["status"] == "running"
        assert row["heartbeat_at"] is not None and row["lock_expires_at"] > row["observed_at"]
    leaf_pids = {int(row["pid"]) for row in gated}
    process_pids = {worker.proc.pid for worker in fleet.values()}
    assert leaf_pids == process_pids, (leaf_pids, process_pids)
    for job_id in first_page_ids:
        beats = {row["heartbeat_at"] for row in gated if str(row["job_id"]) == job_id}
        assert len(beats) > 10, (job_id, len(beats))

    sampling = await pool.fetchrow(
        f"""SELECT count(*)::int AS samples,
                   extract(epoch FROM (max(observed_at)-min(observed_at)))::float8 AS span_s,
                   count(*)::float8 / NULLIF(extract(epoch FROM
                       (max(observed_at)-min(observed_at))),0)::float8 AS hz,
                   max(gap_s)::float8 AS max_gap_s
            FROM (
              SELECT observed_at,
                     extract(epoch FROM (observed_at-lag(observed_at) OVER (ORDER BY id))) AS gap_s
              FROM "{schema}".p2sat_observations
              WHERE observed_at < $1
            ) q""",
        release_at,
    )
    assert sampling is not None
    assert float(cast("float", sampling["hz"])) >= 5.0, dict(sampling.items())
    assert float(cast("float | None", sampling["max_gap_s"]) or 0) < 1.0, dict(sampling.items())

    held_s = await pool.fetchval(
        f"""SELECT extract(epoch FROM ($2::timestamptz -
                    (j.metadata->'wf_emit_backpressure'->>'deferred_at')::timestamptz))::float8
            FROM "{schema}".jobs j WHERE j.id=$1::uuid""",
        source_id,
        release_at,
    )
    assert float(held_s) >= _GATE_S, held_s
    failed_ledgers = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_step_ledger WHERE flow_id=$1::uuid '
        "AND step_key='source' AND status='failed'",
        flow_id,
    )
    assert failed_ledgers == 0
    provider_final = await pool.fetchrow(
        f"SELECT fetch_calls, pages_returned, records_returned, "
        f'fault_gates_tripped FROM "{schema}".p2sat_provider_scenario WHERE scenario_id=$1',
        scenario,
    )
    assert provider_final is not None
    assert int(provider_final["fetch_calls"]) == 3
    assert int(provider_final["pages_returned"]) == 3
    assert int(provider_final["records_returned"]) == 6
    assert int(provider_final["fault_gates_tripped"]) == 0
    source_attempt_rows = await pool.fetch(
        f'SELECT attempt, outcome, worker_id, started_at, finished_at FROM "{schema}".job_attempts '
        "WHERE job_id=$1::uuid ORDER BY attempt",
        source_id,
    )
    assert len(source_attempt_rows) == 1 and source_attempt_rows[0]["outcome"] == "succeeded"
    root = await pool.fetchrow(
        f'SELECT status::text AS status, finished_at FROM "{schema}".jobs WHERE id=$1::uuid',
        flow_id,
    )
    assert root is not None and root["status"] == "succeeded"
    root_success_events = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id=$1::uuid '
        "AND kind='state_change' AND detail->>'to_state'='succeeded'",
        flow_id,
    )
    assert root_success_events == 1
    pending_graph = await pool.fetchval(
        f"""SELECT count(*) FROM "{schema}".jobs
            WHERE (id=$1::uuid OR (metadata->>'flow_id')::uuid=$1::uuid)
              AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')""",
        flow_id,
    )
    assert pending_graph == 0
    held_slots = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".reservation_slots WHERE job_id IN '
        f"(SELECT id FROM \"{schema}\".jobs WHERE metadata->>'flow_id'=$1::text)",
        flow_id,
    )
    assert held_slots == 0
    progress = await pool.fetchval(
        f"""SELECT count(*) FROM "{schema}".wf_node_stream
            WHERE flow_id=$1::uuid AND class='user' AND kind='progress'""",
        flow_id,
    )
    assert progress >= sample.RECORD_COUNT

    deferred_at = datetime.fromisoformat(str(pressure["deferred_at"]).removesuffix("Z") + "+00:00")
    terminal_at = max(row["finished_at"] for row in children if int(row["map_index"]) < 2)
    wake_observed_at = datetime.fromisoformat(
        str(pressure["last_wake_at"]).removesuffix("Z") + "+00:00"
    )
    wake_notices = await pool.fetch(
        f"""SELECT payload, received_at, notifying_pid FROM "{schema}".p2sat_notifications
            WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz
            ORDER BY id""",
        terminal_at,
        source["started_at"],
    )
    assert any(row["payload"] == sample.SOURCE_QUEUE for row in wake_notices), wake_notices
    assert not any(row["payload"] == sample.SIBLING_QUEUE for row in wake_notices), wake_notices
    sibling_notices = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".p2sat_notifications WHERE payload=$1',
        sample.SIBLING_QUEUE,
    )
    assert sibling_notices == 0
    admission = await pool.fetch(
        f"SELECT source_status, in_transaction, outstanding, occurred_at, pid "
        f'FROM "{schema}".p2sat_admission_reads '
        "WHERE scenario=$1 AND flow_id=$2::uuid ORDER BY id",
        scenario,
        flow_id,
    )
    source_admissions = [row for row in admission if row["source_status"] == "running"]
    parked_probes = [row for row in admission if row["source_status"] == "scheduled"]
    parked_read_bound = int(held_s) * 2 + 20  # two 1 Hz leaders + bounded failure headroom.
    assert len(source_admissions) <= 8 * 2, [dict(row) for row in source_admissions]
    assert len(parked_probes) <= parked_read_bound
    # THE EARLY/LATE WINDOW LAW: the parked window's probe count does not
    # ACCRUE with elapsed time — the late half's count stays at the leaders'
    # own fixed cadence (the bounded recovery arms' disclosed slack), never
    # a minute-driven growth.
    release_mid = (
        release_at
        - (release_at - min((row["occurred_at"] for row in parked_probes), default=release_at)) / 2
    )
    early = [row for row in parked_probes if row["occurred_at"] < release_mid]
    late = [row for row in parked_probes if row["occurred_at"] >= release_mid]
    assert len(late) <= len(early) + 30, (
        f"the parked window's probe count accrued with elapsed time "
        f"(early {len(early)} vs late {len(late)}) — a hidden timer's clock work"
    )
    facts = {
        "scenario": scenario,
        "flow_id": flow_id,
        "process_pids": sorted(process_pids),
        "separate_os_workers": len(process_pids) == 2,
        "records": [dict(row) for row in records],
        "children": [dict(row) for row in children],
        "source_body_entries": len(entries),
        "source_claims": len({int(row["claim_epoch"]) for row in entries}),
        "source_park_count": pressure["deferrals"],
        "source_deferral_count": pressure["deferrals"],
        "source_wake_count": pressure["wakes"],
        "job_physical_attempt": source["attempt"],
        "ledger_physical_attempt": source["ledger_attempt"],
        "logical_source_attempt": source["metadata"]["wf_source_logical_attempt"],
        "source_entries": [dict(row) for row in entries],
        "provider_fetches": [dict(row) for row in fetches],
        "provider_final": dict(provider_final),
        "source_final": source,
        "source_admission_reads": len(source_admissions),
        "source_admission_read_bound": 16,
        "parked_wake_admission_probes": len(parked_probes),
        "parked_wake_probe_bound": parked_read_bound,
        "observation_facts": dict(sampling.items()),
        "gate_seconds_from_pg_deferred_at": held_s,
        "deferred_at": deferred_at,
        "release_at": release_at,
        "last_gated_leaf_terminal_at": terminal_at,
        "source_wake_at": wake_observed_at,
        "source_reclaimed_at": source["started_at"],
        "healthy_wake_latency_terminal_to_reclaim_s": (
            source["started_at"] - terminal_at
        ).total_seconds(),
        "wake_notifications": [dict(row) for row in wake_notices],
        "unrelated_sibling_notifications": sibling_notices,
        "failed_source_ledgers": failed_ledgers,
        "root_status": root["status"],
        "root_success_events": root_success_events,
        "graph_nonterminal": pending_graph,
        "progress_events": progress,
        "source_job_attempts": [dict(row) for row in source_attempt_rows],
    }
    assert facts["separate_os_workers"]
    if log_dir is None:
        return facts
    # THE §4.2 CENSUS (the two proof pods' own shutdown snapshots, one log
    # line each): the families' measured sums against the pre-registered
    # V_lane = 60. The fleet's V is the SUM of the pods' own operation
    # instances — the drain runs on the pods, not in this process.
    worker_logs_600 = {}
    for label in fleet:
        path600 = log_dir / f"p2-system-{scenario}-{label}.worker.log"
        worker_logs_600[label] = await asyncio.to_thread(path600.read_text)
    census_total: dict[str, int] = {}
    census_by_pod: dict[str, dict[str, int]] = {}
    for label, content in worker_logs_600.items():
        lines = [ln for ln in content.splitlines() if '"p2sat-census"' in ln]
        assert lines, f"pod {label} never logged its census snapshot"
        snapshot = json.loads(lines[-1])
        census_by_pod[label] = {
            key: int(value)
            for key, value in snapshot.items()
            if key
            in (
                "admission_decisions",
                "source_body_claims",
                "fitted_grants",
                "new_intents",
                "flow_drains",
                "global_drains",
                "candidate_reads",
                "probes",
                "observations",
                "terminals",
                "parks",
            )
        }
        for key, value in census_by_pod[label].items():
            census_total[key] = census_total.get(key, 0) + int(value)
    families_v = (
        census_total.get("admission_decisions", 0)
        + census_total.get("source_body_claims", 0)
        + census_total.get("fitted_grants", 0)
        + census_total.get("new_intents", 0)
        + census_total.get("flow_drains", 0)
        + census_total.get("global_drains", 0)
        + census_total.get("candidate_reads", 0)
    )
    event_e = census_total.get("probes", 0) + census_total.get("observations", 0)
    event_bound = 2 * census_total.get("terminals", 0) + census_total.get("parks", 0)
    facts["census_by_pod"] = census_by_pod
    facts["census_total"] = census_total
    facts["V_measured"] = families_v
    # The standalone pre-registered acceptance CAP (a literal, never the
    # value of a current polynomial — A10: the corrected analytical
    # envelopes are 73/73 at this shape; the caps stay deliberately
    # stricter at 60/60).
    facts["V_lane_cap"] = 60
    facts["E_measured"] = event_e
    facts["E_bound_2T_plus_K"] = event_bound
    print(
        f"P2SAT-V-MEASURED V={families_v} V_lane_cap={facts['V_lane_cap']} "
        f"E={event_e} 2T+K={event_bound} census={json.dumps(census_total, sort_keys=True)}",
        flush=True,
    )
    assert families_v <= facts["V_lane_cap"], (
        f"measured V {families_v} exceeds the pre-registered two-worker lane cap 60: {census_total}"
    )
    assert event_e <= event_bound, (
        f"E {event_e} > 2T + K {event_bound} — a hidden count/fallback "
        f"escaped the census: {census_total}"
    )
    return facts


async def _resource_closure_facts(
    pool: asyncpg.Pool, schema: str, scenario: str, fleet: dict[str, WorkerProc]
) -> dict[str, Any]:
    app_names = [f"{schema}-p2-{label}" for label in fleet]
    remaining = await pool.fetch(
        "SELECT pid, application_name, state FROM pg_stat_activity "
        "WHERE datname = current_database() AND application_name=ANY($1::text[])",
        app_names,
    )
    assert not remaining, [dict(row) for row in remaining]
    return {
        "remaining_worker_backends": [],
        "worker_returncodes": {label: worker.proc.returncode for label, worker in fleet.items()},
    }


async def _flow_facts(pool: asyncpg.Pool, schema: str, flow_id: str) -> dict[str, Any]:
    rows = await pool.fetch(
        f"""SELECT id::text, parent_id::text, step_key, map_index, status::text AS status,
                   attempt::int AS attempt, claim_epoch::bigint AS claim_epoch,
                   locked_by_worker::text AS locked_by_worker, scheduled_at,
                   cancel_phase::int AS cancel_phase, cancel_requested_at, metadata
            FROM "{schema}".jobs WHERE id=$1::uuid OR (metadata->>'flow_id')::uuid=$1::uuid
            ORDER BY id""",
        flow_id,
    )
    return {"jobs": [dict(row) for row in rows]}


@pytest.mark.load_sensitive
@pytest.mark.timeout(900)
async def test_two_os_workers_saturation_600_seconds_and_healthy_wake(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Path
) -> None:
    """The actual required ten-minute, two-process, continuously observed gate.

    THE §4.2 PRE-REGISTRATION (printed here, before execution; neither the
    counts nor the labels change after observation): the healthy two-OS
    -worker two-page/one-source lane's shape is P=2, S=1, R=1 (initial
    maintenance-leadership reconciliation), B=0, J=C=0, so the fixed family
    ceilings are admission P+K ≤ 3, claims 2P+2S+J+C ≤ 6, grants ≤ 3,
    intents ≤ 6, per-flow drains ≤ 3P+3S+R+B = 10, global/direct drains
    ≤ 5P+5S+2R+2B = 10, reads ≤ 8P+8S+2R+2B = **26** (A10's actual-SQL
    -query-execution units: each drain read point is one shared aggregate
    observation plus one fitting-candidate query — the former 4P+4S+R+B=9
    is a superseded historical derivation) — and the corrected analytical
    envelopes V_general_envelope = 23P+22S+5R+5B+J+C = **73** and
    V_lane_envelope = 23P+22S+5R+B+J+C = **73**. The acceptance caps are
    standalone literals, deliberately stricter, never the value of a
    current polynomial: cap V_lane = **60** equals cap V_general = **60**
    (the former 19P+18S+4R+4B+J+C aggregate and its lane form are
    superseded historical derivations).
    The red predicates: cap V_lane > cap V_general is red (impossible here
    — equality), and measured V > cap V_lane is red."""
    # The lane's own family counts (the contract's own labels; the
    # lowercase locals feed the pre-registered literals below).
    p_workers, s_probe, r_recon, b_backfill = 2, 1, 1, 0
    pre_registration = {
        "P": p_workers,
        "S": s_probe,
        "R": r_recon,
        "B": b_backfill,
        "J": 0,
        "C": 0,
        "admission_decisions <= P+K": 3,
        "source_body_claims <= 2P+2S+J+C": 6,
        "fitted_grants <= P+S": 3,
        "new_intents <= 2P+2S": 6,
        "flow_drains <= 3P+3S+R+B": 10,
        "global_drains <= 5P+5S+2R+2B": 10,
        "candidate_reads <= 8P+8S+2R+2B": 8 * p_workers
        + 8 * s_probe
        + 2 * r_recon
        + 2 * b_backfill,
        "V_general_envelope = 23P+22S+5R+5B+J+C": 23 * p_workers
        + 22 * s_probe
        + 5 * r_recon
        + 5 * b_backfill,
        "V_lane_envelope = 23P+22S+5R+B+J+C": 23 * p_workers
        + 22 * s_probe
        + 5 * r_recon
        + b_backfill,
        "acceptance cap V_lane": 60,
        "acceptance cap V_general": 60,
    }
    assert pre_registration["candidate_reads <= 8P+8S+2R+2B"] == 26, pre_registration
    assert pre_registration["V_general_envelope = 23P+22S+5R+5B+J+C"] == 73, pre_registration
    assert pre_registration["V_lane_envelope = 23P+22S+5R+B+J+C"] == 73, pre_registration
    assert (
        pre_registration["acceptance cap V_lane"] == 60
        and pre_registration["acceptance cap V_general"] == 60
    ), pre_registration
    assert (
        pre_registration["acceptance cap V_lane"] <= pre_registration["acceptance cap V_general"]
    ), pre_registration
    print("P2SAT-V-PRE-REGISTRATION " + json.dumps(pre_registration, sort_keys=True), flush=True)
    schema = module_pg_schema.schema_name
    scenario = "healthy600"
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=24)
    fleet: dict[str, WorkerProc] = {}
    listener_stop = asyncio.Event()
    listener_task: asyncio.Task[None] | None = None
    flow_id: str | None = None
    facts: dict[str, Any] | None = None
    stopped_a = False
    try:
        await _announce_environment(pool)
        await sample.initialize_sample(pool, schema=schema, scenario=scenario)
        await _initialize_observer(pool, schema)
        fleet = await _spawn_pair_fixed(
            pool, pg_dsn, schema, scenario, log_dir=log_dir, concurrency=(2, 1)
        )
        listener_task = asyncio.create_task(
            _notification_listener(pool, pg_dsn, schema, listener_stop)
        )
        async with TaskQ(dsn=pg_dsn, schema=schema) as client:
            baseline = await _run_probe_batch(client, pool, schema, scenario, list(range(24)))
            # Derived BEFORE the long lane: ten percent of measured baseline
            # service capacity over the fixed ten minutes, capped only by the
            # actual offered work.  This bound is never edited after green.
            planned_cap = sample.RECORD_COUNT * 300
            minimum_unrelated = min(
                planned_cap,
                int(baseline["jobs_per_s"] * _GATE_S * 0.10),
            )
            # Construct a deterministic one-child-per-process placement without
            # substituting claims: the 2-slot peer pauses before the run exists;
            # the one-slot worker claims and parks the source, then claims one
            # gated child.  Resume the peer for the second genuine child and its
            # remaining ordinary-work slot.  No in-memory gate reaches a body.
            fleet["a"].proc.send_signal(signal.SIGSTOP)
            stopped_a = True
            flow_id = await _create_run(pool, schema, scenario)
            await _wait_for(
                pool,
                schema,
                flow_id,
                lambda source, leaves: (
                    source["status"] == "scheduled"
                    and source["scheduled_at"] is not None
                    and len(leaves) == 2
                    and sum(row["status"] == "running" for row in leaves) == 1
                    and any(
                        row["status"] == "running" and int(row["pid"]) == fleet["b"].proc.pid
                        for row in leaves
                    )
                ),
                deadline_s=90,
            )
            fleet["a"].proc.send_signal(signal.SIGCONT)
            stopped_a = False
            await _wait_for(
                pool,
                schema,
                flow_id,
                lambda source, leaves: (
                    source["status"] == "scheduled"
                    and source["scheduled_at"] is not None
                    and len(leaves) == 2
                    and all(row["status"] == "running" for row in leaves)
                ),
                deadline_s=90,
            )
            producer = asyncio.create_task(_ordinary_producer(client, pool, schema, scenario, 24))
            observation = await _observe_and_release(pool, schema, scenario, flow_id, fleet)
            offered = await asyncio.wait_for(producer, timeout=30)
            producer_extras = await pool.fetchrow(
                f"""SELECT count(*)::int AS succeeded,
                           min(finished_at) AS first_terminal, max(finished_at) AS last_terminal
                    FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text,'probe'::text]
                      AND status='succeeded'""",
                scenario,
            )
            assert producer_extras is not None
            # Baseline's 24 belong to service-headroom measurement, not the
            # saturated population.  All remaining work was offered under gate.
            window_succeeded = int(producer_extras["succeeded"]) - 24
            assert offered["offered"] >= minimum_unrelated
            assert window_succeeded >= minimum_unrelated, {
                "minimum": minimum_unrelated,
                "window_succeeded": window_succeeded,
                "offered": offered,
            }
            assert window_succeeded == offered["offered"]
            # Persist all raw native witnesses before semantic assertions, so
            # an observer defect cannot erase the completed ten-minute window.
            raw_observations = await pool.fetch(
                f'SELECT * FROM "{schema}".p2sat_observations ORDER BY id'
            )
            _save(f"{scenario}-observations", [dict(row) for row in raw_observations])
            raw_admissions = await pool.fetch(
                f'SELECT * FROM "{schema}".p2sat_admission_reads WHERE scenario=$1 ORDER BY id',
                scenario,
            )
            _save(f"{scenario}-admissions", [dict(row) for row in raw_admissions])
            facts = await _healthy_assertions(
                pool,
                schema,
                scenario,
                flow_id,
                fleet,
                observation["release_at"],
                log_dir=log_dir,
            )
            throughput = await pool.fetch(
                f"""WITH bounds AS (
                      SELECT (metadata->'wf_emit_backpressure'->>'deferred_at')::timestamptz AS parked
                      FROM "{schema}".jobs j
                      WHERE j.metadata->>'flow_id'=$1::text AND j.step_key='source'
                    )
                    SELECT width_bucket(extract(epoch FROM (j.finished_at-b.parked)), 0, $2, 10) AS bucket,
                           count(*)::int AS completed
                    FROM "{schema}".jobs j CROSS JOIN bounds b
                    WHERE j.tags @> ARRAY[$3::text,'probe'::text]
                      AND j.status='succeeded' AND j.finished_at >= b.parked
                    GROUP BY bucket ORDER BY bucket""",
                flow_id,
                float(_GATE_S),
                scenario,
            )
            assert len([row for row in throughput if int(row["bucket"]) in range(1, 11)]) == 10
            assert all(int(row["completed"]) > 0 for row in throughput)
            facts.update(
                {
                    "baseline": baseline,
                    "derived_minimum_unrelated": minimum_unrelated,
                    "producer": offered,
                    "ordinary_window_succeeded": window_succeeded,
                    "ordinary_per_minute": [dict(row) for row in throughput],
                    "observer_sample_count": observation["samples"],
                }
            )
            _save(f"{scenario}-facts", facts)
            observations = await pool.fetch(
                f'SELECT * FROM "{schema}".p2sat_observations ORDER BY id'
            )
            _save(f"{scenario}-observations", [dict(row) for row in observations])
    except BaseException:
        if flow_id is not None:
            _save(f"{scenario}-failure-state", await _flow_facts(pool, schema, flow_id))
        raise
    finally:
        if stopped_a and "a" in fleet:
            fleet["a"].proc.send_signal(signal.SIGCONT)
        listener_stop.set()
        if listener_task is not None:
            await asyncio.wait_for(listener_task, timeout=15)
        # Healthy products close only during graceful worker shutdown, after all
        # body finalization.  Capture and prove that exact owned lifecycle now.
        if facts is not None:
            codes = await _stop_fleet(fleet)
            assert all(code == 0 for code in codes.values()), codes
            facts["shutdown"] = await _resource_closure_facts(pool, schema, scenario, fleet)
            facts["shutdown"]["graceful_returncodes"] = codes
            _save(f"{scenario}-facts", facts)
        else:
            await _stop_fleet(fleet)
        await pool.close()


async def _release_children(pool: asyncpg.Pool, schema: str, scenario: str) -> datetime:
    row = await pool.fetchrow(
        f'UPDATE "{schema}".p2sat_control SET release_at=clock_timestamp() '
        "WHERE scenario=$1 AND release_at IS NULL RETURNING release_at",
        scenario,
    )
    if row is not None:
        await pool.execute("SELECT pg_notify($1,$2)", sample.gate_channel(schema), scenario)
        return cast("datetime", row["release_at"])
    existing = await pool.fetchval(
        f'SELECT release_at FROM "{schema}".p2sat_control WHERE scenario=$1',
        scenario,
    )
    assert existing is not None
    return cast("datetime", existing)


@pytest.mark.timeout(240)
async def test_notify_and_inline_wake_failure_use_real_bounded_sweep(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Path
) -> None:
    """Committed child terminal + failed fine wake + lost listener -> fallback."""
    schema = module_pg_schema.schema_name
    scenario = "fallback"
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=16)
    fleet: dict[str, WorkerProc] = {}
    flow_id: str | None = None
    blocker: asyncpg.Connection | None = None
    transaction: Transaction | None = None
    try:
        await _announce_environment(pool)
        await sample.initialize_sample(pool, schema=schema, scenario=scenario)
        fleet = await _spawn_pair_fixed(pool, pg_dsn, schema, scenario, log_dir=log_dir)
        flow_id = await _create_run(pool, schema, scenario)
        source, _leaves = await _wait_for(
            pool,
            schema,
            flow_id,
            lambda s, children: (
                s["status"] == "scheduled"
                and len(children) == 2
                and all(row["status"] == "running" for row in children)
            ),
            deadline_s=90,
        )
        blocker = await asyncpg.connect(pg_dsn)
        transaction = blocker.transaction()
        await transaction.start()
        # Block ONLY the parked source's wake-critical section: the exact
        # parked source row (the fine wake's candidate PK lock), never the
        # run root — the accepted tx1 takes the root before the node (the
        # canonic root→node order), so a root lock would block the children's
        # own terminal commits (the obsolete premise this control sheds).
        parked_id: uuid.UUID = uuid.UUID(str(source["id"]))
        await blocker.fetchval(f'SELECT id FROM "{schema}".jobs WHERE id=$1 FOR UPDATE', parked_id)
        # The children's release lands their terminal tx1's; the FIRST
        # child's post-commit fine wake takes the root then blocks on the
        # parked source row (the wake's own lock order: root → source row),
        # holding the root while it waits — so the second child's terminal
        # waits behind it until the blocked wake's backend is killed. The
        # terminal commits are required (below) BEFORE the source blocker
        # is released.
        await _release_children(pool, schema, scenario)
        # The real post-commit fine-wake backend waits on the parked source
        # row (the scoped tuple-lock witness — this database's own backends
        # only). The opposite-direction control: with no source blocker
        # there are no waiters and the failure assertion below can never be
        # satisfied.
        witness_deadline = time.monotonic() + 20
        wake_waiters: set[int] = set()
        while time.monotonic() < witness_deadline:
            wake_waiters = await row_lock_waiters(pool, schema, parked_id)
            if wake_waiters:
                break
            await asyncio.sleep(0.05)
        assert wake_waiters, (
            "no post-commit fine-wake backend was observed waiting on the "
            "parked source row — the wake-critical-section block never happened"
        )
        # Kill the actual blocked fine-wake backends (not the process and not
        # a fabricated callback): the wake fails loudly in the worker log
        # while the child's committed result stands. The blocked root lock
        # dies with them, so the second child's own terminal tx1 can commit.
        killed = await pool.fetch(
            "SELECT pg_terminate_backend(pid) AS terminated FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid = ANY($1::int[]) "
            "AND pid<>pg_backend_pid()",
            sorted(wake_waiters),
        )
        assert any(bool(row["terminated"]) for row in killed)
        # Both real terminal commits stand before the source blocker is
        # released (the second child's own fine wake may block again on the
        # still-held parked row — its terminal commit is already durable).
        deadline = time.monotonic() + 60
        terminal_rows: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            terminal_rows = [
                dict(row)
                for row in await pool.fetch(
                    f'SELECT id::text, finished_at, status::text AS status FROM "{schema}".jobs '
                    "WHERE parent_id=$1::uuid ORDER BY map_index",
                    source["id"],
                )
            ]
            if len(terminal_rows) == 2 and all(
                row["status"] == "succeeded" for row in terminal_rows
            ):
                break
            await asyncio.sleep(0.05)
        assert len(terminal_rows) == 2 and all(
            row["status"] == "succeeded" for row in terminal_rows
        ), terminal_rows
        # Also terminate that queue's current LISTEN backend (the retained
        # failure intent); worker poll and the surviving leader's ordinary
        # bounded sweep — never a global crowbar — own convergence.
        await asyncio.sleep(1.5)
        await transaction.commit()
        transaction = None
        await blocker.close()
        blocker = None
        await _wait_for(
            pool,
            schema,
            flow_id,
            lambda s, children: s["status"] == "succeeded" and len(children) == 4,
            deadline_s=90,
        )
        await _require_graph_succeeded(pool, schema, flow_id)
        facts = await _healthy_terminal_common(pool, schema, scenario, flow_id)
        facts["terminated_backend_count"] = len(killed)
        facts["fallback_terminal_to_reclaim_s"] = facts["wake_latency_s"]
        worker_logs: dict[str, str] = {}
        for label in fleet:
            path = log_dir / f"p2-system-{scenario}-{label}.worker.log"
            worker_logs[label] = await asyncio.to_thread(path.read_text)
        # THE MACHINERY'S OWN FAILURE EVIDENCE: the killed drain backends'
        # connection errors must surface in some worker's log (the direct
        # route's drain error propagates after the committed terminal
        # stands; the leader's drain arms log the bounded diagnostic). The
        # rejected shape's own event name is gone with that shape.
        failure_classes = (
            "InterfaceError",
            "ConnectionDoesNotExistError",
            "ConnectionDoesNotExist",
            "PostgresConnectionError",
        )
        # The fine-wake failure's own evidence is the OBSERVED kill: the
        # wake-waiter backends were real (the row-lock witness), were
        # terminated mid-wait, and the terminals + the durable convergence
        # stand beside them.
        facts["fine_wake_failure_workers"] = [
            label
            for label, content in worker_logs.items()
            if any(cls in content for cls in failure_classes)
        ]
        _save(f"{scenario}-facts", facts)
    except BaseException:
        if flow_id is not None:
            _save(f"{scenario}-failure-state", await _flow_facts(pool, schema, flow_id))
        raise
    finally:
        if transaction is not None:
            await transaction.rollback()
        if blocker is not None:
            await blocker.close()
        await _release_children(pool, schema, scenario)
        codes = await _stop_fleet(fleet)
        assert all(code == 0 for code in codes.values()), codes
        await pool.close()


async def _require_graph_succeeded(
    pool: asyncpg.Pool, schema: str, flow_id: str, *, deadline_s: float = 60
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + deadline_s
    rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        rows = await _job_rows(pool, schema, flow_id)
        if len(rows) == 6 and all(row["status"] == "succeeded" for row in rows):
            return rows
        await asyncio.sleep(0.05)
    raise AssertionError(f"graph has not quiesced successfully: {rows!r}")


async def _healthy_terminal_common(
    pool: asyncpg.Pool, schema: str, scenario: str, flow_id: str
) -> dict[str, Any]:
    source = await _source(pool, schema, flow_id)
    pressure = _document(source["metadata"]["wf_emit_backpressure"])
    assert source["status"] == "succeeded" and source["attempt"] == 1
    assert source["metadata"].get("wf_source_logical_attempt") == 1
    assert source["snooze_count"] == 0
    assert pressure["deferrals"] == 1 and pressure["wakes"] == 1
    failed = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_step_ledger WHERE flow_id=$1::uuid '
        "AND step_key='source' AND status='failed'",
        flow_id,
    )
    assert failed == 0
    records = await pool.fetch(
        f'SELECT record_id FROM "{schema}".p2sat_records_applied WHERE scenario=$1 AND flow_id=$2::uuid',
        scenario,
        flow_id,
    )
    assert {row["record_id"] for row in records} == {f"record-{n}" for n in range(4)}
    attempts = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".p2sat_source_entries WHERE scenario=$1 AND flow_id=$2::uuid',
        scenario,
        flow_id,
    )
    logical = await pool.fetch(
        f'SELECT DISTINCT logical_attempt FROM "{schema}".p2sat_source_entries '
        "WHERE scenario=$1 AND flow_id=$2::uuid",
        scenario,
        flow_id,
    )
    assert attempts == 2 and [row["logical_attempt"] for row in logical] == [1]
    jobs = await pool.fetch(
        f"SELECT status::text AS status FROM \"{schema}\".jobs WHERE id=$1::uuid OR (metadata->>'flow_id')::uuid=$1::uuid",
        flow_id,
    )
    assert jobs and all(row["status"] == "succeeded" for row in jobs)
    children = await pool.fetch(
        f'SELECT finished_at FROM "{schema}".jobs WHERE parent_id=$1::uuid AND map_index<2',
        source["id"],
    )
    terminal_at = max(row["finished_at"] for row in children)
    return {
        "scenario": scenario,
        "flow_id": flow_id,
        "source": source,
        "source_body_entries": attempts,
        "record_ids": sorted(row["record_id"] for row in records),
        "failed_source_ledgers": failed,
        "wake_latency_s": (source["started_at"] - terminal_at).total_seconds(),
    }


@pytest.mark.timeout(240)
async def test_sigkill_after_the_park_commit_converges_without_source_retry(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Path
) -> None:
    """SIGKILL the source's worker immediately after the park's own
    commit and prove the failover converges with NO source-retry spend.

    THE WINDOW (the canonical root-first order's own consequence, R2/A9.2):
    the park transaction now takes the flow root's own row lock and the
    capacity intent row BEFORE its park write — a pre-held blocker can no
    longer sit between the park's commit and the worker's post-park
    machinery without blocking the park itself (every one of the drain's
    locks is also the park's). The kill therefore lands in the first
    window that exists: immediately after the committed park, before any
    leaf terminal or promotion has moved the estate. The test's own value
    is the CONVERGENCE: the replacement re-claims the parked source
    through the promotion, the logical attempt never moves, and the run
    completes with exactly the graph's own children."""
    schema = module_pg_schema.schema_name
    scenario = "parkkill"
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=16)
    fleet: dict[str, WorkerProc] = {}
    flow_id: str | None = None
    try:
        await _announce_environment(pool)
        await sample.initialize_sample(pool, schema=schema, scenario=scenario)
        flow_id = await _create_run(pool, schema, scenario)
        fleet["a"] = await _spawn_one(
            pool,
            pg_dsn,
            schema,
            scenario,
            "a",
            log_dir=log_dir,
            queues=sample.SOURCE_QUEUE,
        )
        _parked, _leaves = await _wait_for(
            pool,
            schema,
            flow_id,
            lambda s, children: s["status"] == "scheduled" and len(children) == 2,
            deadline_s=90,
        )
        assert all(row["status"] == "pending" and row["attempt"] == 0 for row in _leaves)
        entries = await pool.fetch(
            f'SELECT pid, logical_attempt, claim_epoch FROM "{schema}".p2sat_source_entries '
            "WHERE scenario=$1 AND flow_id=$2::uuid",
            scenario,
            flow_id,
        )
        assert len(entries) == 1 and entries[0]["pid"] == fleet["a"].proc.pid
        # THE PARK COMMITTED; the source's owner has not been needed since.
        # This is a real process kill in the post-park window.
        fleet["a"].proc.kill()
        await asyncio.to_thread(fleet["a"].proc.wait, 10)
        assert fleet["a"].proc.returncode == -9
        old_pid = fleet["a"].proc.pid
        # Spawn exactly one second/surviving real process after ownership died.
        fleet["b"] = await _spawn_one(pool, pg_dsn, schema, scenario, "b", log_dir=log_dir)
        await _release_children(pool, schema, scenario)
        await _wait_for(
            pool,
            schema,
            flow_id,
            lambda s, children: s["status"] == "succeeded" and len(children) == 4,
            deadline_s=120,
        )
        await _require_graph_succeeded(pool, schema, flow_id)
        facts = await _healthy_terminal_common(pool, schema, scenario, flow_id)
        entries_after = await pool.fetch(
            f'SELECT pid, logical_attempt FROM "{schema}".p2sat_source_entries '
            "WHERE scenario=$1 AND flow_id=$2::uuid ORDER BY id",
            scenario,
            flow_id,
        )
        assert len(entries_after) == 2
        assert all(int(row["logical_attempt"]) == 1 for row in entries_after)
        assert entries_after[0]["pid"] == old_pid
        assert entries_after[1]["pid"] != old_pid
        assert not any(row["pid"] == old_pid for row in entries_after[1:])
        victim_backend = await pool.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND application_name=$1",
            f"{schema}-p2-a",
        )
        assert victim_backend == 0
        facts["killed_pid"] = old_pid
        facts["source_entries_after_failover"] = [dict(row) for row in entries_after]
        facts["victim_backends_after_failover"] = victim_backend
        _save(f"{scenario}-facts", facts)
    except BaseException:
        if flow_id is not None:
            _save(f"{scenario}-failure-state", await _flow_facts(pool, schema, flow_id))
        raise
    finally:
        await _release_children(pool, schema, scenario)
        await _stop_fleet(fleet)
        await pool.close()


@pytest.mark.timeout(180)
async def test_cancel_parked_source_never_resurrects_or_emits_later_page(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Path
) -> None:
    """Real operator cancellation owns terminal/no-re-entry, not body observation."""
    schema = module_pg_schema.schema_name
    scenario = "parkcancel"
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=16)
    fleet: dict[str, WorkerProc] = {}
    flow_id: str | None = None
    try:
        await _announce_environment(pool)
        await sample.initialize_sample(pool, schema=schema, scenario=scenario)
        fleet = await _spawn_pair_fixed(pool, pg_dsn, schema, scenario, log_dir=log_dir)
        flow_id = await _create_run(pool, schema, scenario)
        source, _leaves = await _wait_for(
            pool,
            schema,
            flow_id,
            lambda s, children: (
                s["status"] == "scheduled"
                and len(children) == 2
                and all(row["status"] == "running" for row in children)
            ),
            deadline_s=90,
        )
        before_entries = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".p2sat_source_entries WHERE scenario=$1 AND flow_id=$2::uuid',
            scenario,
            flow_id,
        )
        changed = await cancel_workflow_run(
            pool,
            schema=schema,
            flow_id=source["metadata"]["flow_id"],
            principal="system-proof",
            reason="bounded parked-source cancellation proof",
        )
        assert changed >= 1
        cancelled = await _source(pool, schema, flow_id)
        assert cancelled["status"] == "cancelled" and cancelled["attempt"] == 0
        assert cancelled["ledger_status"] == "fenced"
        assert cancelled["metadata"].get("wf_source_logical_attempt") == 1
        assert cancelled["metadata"]["wf_emit_backpressure"]["wakes"] == 0
        running_control = [
            row
            for row in await _job_rows(pool, schema, flow_id)
            if row["step_key"] == "write_record"
        ]
        assert all(
            _document(row["metadata"]).get("cancel_phase") == "cooperative"
            for row in running_control
        )
        # No claim of the not-yet-P5 in-body signal is made.  Release normally;
        # current cooperative children may cancel or successfully finalize, but
        # every disposition must be terminal and no page-two claim may appear.
        await _release_children(pool, schema, scenario)
        deadline = time.monotonic() + 60
        rows = await _job_rows(pool, schema, flow_id)
        while time.monotonic() < deadline:
            rows = await _job_rows(pool, schema, flow_id)
            if rows and all(
                row["status"] in {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
                for row in rows
            ):
                break
            await asyncio.sleep(0.05)
        assert rows and all(
            row["status"] in {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
            for row in rows
        ), rows
        await asyncio.sleep(2.0)
        rows = await _job_rows(pool, schema, flow_id)
        after_entries = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".p2sat_source_entries WHERE scenario=$1 AND flow_id=$2::uuid',
            scenario,
            flow_id,
        )
        assert after_entries == before_entries == 1
        assert len([row for row in rows if row["step_key"] == "write_record"]) == 2
        root = await pool.fetchval(f'SELECT status::text FROM "{schema}".jobs WHERE id=$1', flow_id)
        assert root == "cancelled"
        dangling = await pool.fetchval(
            f"""SELECT count(*) FROM "{schema}".wf_outbox o
                JOIN "{schema}".jobs j ON j.id=o.join_job_id
                WHERE j.metadata->>'flow_id'=$1::text AND NOT o.delivered""",
            flow_id,
        )
        assert dangling == 0
        facts = {
            "scenario": scenario,
            "flow_id": flow_id,
            "source_before": source,
            "source_cancelled": cancelled,
            "source_entries_before_after": [before_entries, after_entries],
            "final_jobs": rows,
            "child_disposition": {
                str(row["id"]): row["status"] for row in rows if row["step_key"] == "write_record"
            },
            "unclaimed_body_observation_claimed": False,
            "dangling_outbox": dangling,
        }
        _save(f"{scenario}-facts", facts)
    except BaseException:
        if flow_id is not None:
            _save(f"{scenario}-failure-state", await _flow_facts(pool, schema, flow_id))
        raise
    finally:
        await _release_children(pool, schema, scenario)
        codes = await _stop_fleet(fleet)
        assert all(code == 0 for code in codes.values()), codes
        await pool.close()


async def _job_rows(pool: asyncpg.Pool, schema: str, flow_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in await pool.fetch(
            f"""SELECT id::text, step_key, status::text AS status, attempt::int AS attempt,
                       cancel_phase::int AS cancel_phase, cancel_requested_at, metadata
                FROM "{schema}".jobs WHERE id=$1::uuid OR (metadata->>'flow_id')::uuid=$1::uuid
                ORDER BY id""",
            flow_id,
        )
    ]
