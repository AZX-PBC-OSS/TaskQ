"""#717 real 200k record identity/default-backpressure scale proof.

The default invocation is exactly 200,000 records. ``TASKQ_P3_TEST_RECORDS`` is
only an explicit developer-debug override; final evidence uses the default and
asserts that persisted count. The source always pages from its durable cursor,
1000 records per page, and the workflow leaves max_in_flight at its shipped
1000 default. Two production-configured worker pods share real dispatch claims;
this is not represented as a two-process worker soak.
"""

# ruff: noqa: S608  # Why: the generated fixture schema is the only interpolated identifier; all values are bound.

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from contextlib import AsyncExitStack
from decimal import Decimal

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
)
from taskq.workflows._emit import EMIT_MAX_IN_FLIGHT_DEFAULT
from taskq.workflows.chain import DONE, Route
from tests._fleet import open_fleet
from tests._wf_fixtures import write_band_artifact

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_RECORDS = 200_000
_PAGE = 1000


async def _exercise(pg_dsn: str, record_count: int) -> dict[str, object]:
    assert record_count > 0 and record_count % _PAGE == 0
    previous_log_disable = logging.root.manager.disable
    logging.disable(logging.INFO)
    schema = "idp3scale_" + new_base62(8, precision="random").lower()
    control = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    fleet_resources = AsyncExitStack()
    body_calls = 0
    source_calls = 0
    maximum_logical_attempt = 0
    try:
        fleet = await fleet_resources.enter_async_context(
            open_fleet(
                pg_dsn,
                schema=schema,
                pods=("worker_a", "worker_b"),
                actors=(("wf", "default"),),
                settings_overrides={"log_level": "WARNING"},
            )
        )
        # The generic fleet helper creates bare worker rows; production workers
        # that import workflows register this persisted dispatch capability.
        await control.execute(
            f'UPDATE "{schema}".workers SET metadata = metadata || $2::jsonb '
            "WHERE id = ANY($1::uuid[])",
            [pod.worker_id for pod in fleet.pods],
            '{"workflow_execution":true}',
        )
        pool = await asyncpg.create_pool(
            pg_dsn,
            min_size=1,
            max_size=4,
            statement_cache_size=512,
            max_cached_statement_lifetime=3600.0,
        )

        # Route keys are enum members; a one-outcome local enum is sufficient.
        import enum

        class RecordDone(enum.Enum):
            OK = "ok"

        async def record_body(ctx: StepContext, item: object) -> RecordDone:
            nonlocal body_calls
            del ctx
            assert isinstance(item, dict) and type(item.get("record_id")) is int
            body_calls += 1
            return RecordDone.OK

        chain = Chain(
            name="identity-scale-records",
            start="record",
            steps={
                "record": Step(
                    body=record_body,
                    outcomes=RecordDone,
                    route=Route({RecordDone.OK: DONE}),
                )
            },
        )

        async def source_body(ctx: StepContext) -> None:
            nonlocal source_calls, maximum_logical_attempt
            source_calls += 1
            assert ctx.attempt == 1
            maximum_logical_attempt = max(maximum_logical_attempt, ctx.attempt)
            cursor = await ctx.cursor()
            previous = cursor.get("page", -1)
            assert type(previous) is int
            for page in range(previous + 1, record_count // _PAGE):
                first = page * _PAGE
                # Exactly one current page exists in process memory. The next
                # page's stable ordinals derive from the committed cursor.
                await ctx.emit_batch(
                    [
                        chain_start(
                            chain,
                            {"record_id": index},
                            map_index=index,
                            trace_id=f"record-{index}",
                        )
                        for index in range(first, first + _PAGE)
                    ],
                    cursor={"page": page},
                )

        app = WorkflowApp()

        @app.workflow("identity_scale_records")
        def workflow() -> Promise[object]:
            return build(chain_source(chain, source_body, key="source"))

        compiled = app.get("identity_scale_records")
        assert compiled.max_in_flight == EMIT_MAX_IN_FLIGHT_DEFAULT == 1000
        runners = [
            FlowRunner(compiled, pod.deps.worker_pool, schema, worker_id=JobId(pod.worker_id))
            for pod in fleet.pods
        ]
        creator = FlowRunner(compiled, pool, schema)
        created = await creator.create_flow()
        flow = created.flow_id
        started = time.perf_counter_ns()
        stop = asyncio.Event()

        async def work(pod_index: int) -> None:
            pod = fleet.pods[pod_index]
            runner = runners[pod_index]
            while not stop.is_set():
                # Production dispatch arbitrates these two pods. Execution uses
                # the workflow fleet-claimed door with the exact physical fence.
                claimed = await pod.claim(["default"], _PAGE)
                if not claimed:
                    await asyncio.sleep(0.001)
                    continue
                # The backend's typed JobRow intentionally omits workflow-only
                # fields. Re-read this pod's just-claimed physical rows through
                # its own pool, exactly retaining attempt/epoch ownership.
                async with pod.deps.worker_pool.acquire() as conn:
                    documents = await conn.fetch(
                        f"SELECT id, step_key, map_index, attempt, trace_id, payload, "
                        "start_to_close, metadata, required_worker_schema, claim_epoch "
                        f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[]) '
                        "AND status = 'running'",
                        [row.id for row in claimed],
                    )
                assert len(documents) == len(claimed)
                outcomes = await asyncio.gather(
                    *(
                        runner.run_fleet_claimed_step(
                            flow,
                            document,
                            attempt=int(document["attempt"]),
                            claim_epoch=int(document["claim_epoch"]),
                        )
                        for document in documents
                    )
                )
                assert set(outcomes) <= {"succeeded", "deferred"}, outcomes

        async def orchestrate() -> None:
            while True:
                status = await control.fetchval(
                    f'SELECT status FROM "{schema}".jobs WHERE id = $1', flow
                )
                if status == "succeeded":
                    return
                assert status in {"pending", "running"}, status
                # Orchestration-only: all bodies above run on the two claimed
                # production worker pods. This seat maintains roots and wakes.
                await creator.tick(flow, execute=False)
                await asyncio.sleep(0.001)

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(work(0))
            tasks.create_task(work(1))
            await asyncio.wait_for(orchestrate(), timeout=1800.0)
            stop.set()
        elapsed_s = (time.perf_counter_ns() - started) / 1e9

        record_facts = await control.fetchrow(
            f"SELECT count(*)::bigint AS rows, count(DISTINCT map_index)::bigint AS identities, "
            "count(DISTINCT trace_id)::bigint AS traces, "
            "count(DISTINCT idempotency_key)::bigint AS keys, "
            "min(map_index) AS minimum, max(map_index) AS maximum, "
            "sum(map_index) AS ordinal_sum, "
            "count(*) FILTER (WHERE status <> 'succeeded')::bigint AS nonterminal, "
            "count(*) FILTER (WHERE error_class IS NOT NULL)::bigint AS errors "
            f"FROM \"{schema}\".jobs WHERE parent_id IS NOT NULL AND step_key = 'record' "
            "AND (metadata->>'flow_id')::uuid = $1",
            flow,
        )
        assert record_facts is not None
        assert int(record_facts["rows"]) == record_count
        assert int(record_facts["identities"]) == record_count
        assert int(record_facts["traces"]) == record_count
        assert int(record_facts["keys"]) == record_count
        assert record_facts["minimum"] == 0
        assert record_facts["maximum"] == record_count - 1
        expected_sum = record_count * (record_count - 1) // 2
        assert int(Decimal(record_facts["ordinal_sum"])) == expected_sum
        assert record_facts["nonterminal"] == 0
        assert record_facts["errors"] == 0

        ledger_facts = await control.fetchrow(
            f"SELECT count(*)::bigint AS rows, count(DISTINCT job_id)::bigint AS jobs, "
            "count(*) FILTER (WHERE status <> 'succeeded')::bigint AS nonterminal, "
            "count(*) FILTER (WHERE attempt <> 1)::bigint AS unexpected_attempt "
            f"FROM \"{schema}\".wf_step_ledger WHERE flow_id = $1 AND step_key = 'record'",
            flow,
        )
        assert ledger_facts is not None
        assert int(ledger_facts["rows"]) == record_count
        assert int(ledger_facts["jobs"]) == record_count
        assert ledger_facts["nonterminal"] == 0
        assert ledger_facts["unexpected_attempt"] == 0
        duplicate_identities = await control.fetchval(
            f'SELECT count(*) FROM (SELECT map_index FROM "{schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'record' GROUP BY map_index "
            "HAVING count(*) <> 1) AS duplicates",
            flow,
        )
        assert duplicate_identities == 0
        assert (
            int(
                await control.fetchval(
                    f'SELECT count(*) FROM "{schema}".wf_edge WHERE flow_id = $1', flow
                )
                or 0
            )
            == record_count
        )
        assert body_calls == record_count
        assert maximum_logical_attempt == 1

        roots = await control.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', flow
        )
        source = await control.fetchrow(
            f'SELECT status, metadata, attempt FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
            flow,
        )
        assert roots is not None and roots["status"] == "succeeded"
        assert source is not None and source["status"] == "succeeded"
        metadata = source["metadata"]
        document = json.loads(metadata) if isinstance(metadata, str) else metadata
        assert document["emit_cursor"] == {"page": record_count // _PAGE - 1}
        # The ledger records one row per claim (the epoch arbiter): the
        # shipped-default bound parks the source at saturation — its parked
        # attempt's 'awaited' record survives beside the resumed attempt's
        # terminal — so the read asserts the ordered history, the latest row
        # terminal-succeeded at the same physical attempt.
        source_ledger = await control.fetch(
            f'SELECT status, attempt, claim_epoch FROM "{schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'source' "
            "ORDER BY claim_epoch, id",
            flow,
        )
        assert source_ledger, "the source's ledger lost every row"
        assert source_ledger[-1]["status"] == "succeeded"
        assert source_ledger[-1]["attempt"] == 1
        assert [row["status"] for row in source_ledger] == [
            *(["awaited"] * (len(source_ledger) - 1)),
            "succeeded",
        ], [dict(row) for row in source_ledger]
        failed_sources = await control.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'source' AND status = 'failed'",
            flow,
        )
        assert failed_sources == 0
        root_terminal_events = await control.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events '
            "WHERE job_id = $1 AND kind = 'state_change' "
            "AND detail->>'to_state' = 'succeeded'",
            flow,
        )
        assert root_terminal_events == 1

        storage = {
            table: int(
                await control.fetchval("SELECT pg_total_relation_size($1)", f'"{schema}"."{table}"')
                or 0
            )
            for table in ("jobs", "wf_edge", "wf_step_ledger", "wf_outbox", "wf_signals")
        }
        facts: dict[str, object] = {
            "pin": "p3-real-200k-default-bounded",
            "requested_records": record_count,
            "page_size": _PAGE,
            "max_in_flight": compiled.max_in_flight,
            "elapsed_s": elapsed_s,
            "worker_pods": [pod.name for pod in fleet.pods],
            "body_calls": body_calls,
            "source_calls": source_calls,
            "maximum_logical_attempt": maximum_logical_attempt,
            "records": dict(record_facts),
            "ledger": dict(ledger_facts),
            "storage_bytes": storage,
            "cursor": document["emit_cursor"],
            "root_terminal_events": root_terminal_events,
            "driver": "two production-configured worker pods sharing real dispatch claims and run_fleet_claimed_step; not a two-process soak",
            "python": sys.version,
            "postgres": await control.fetchval("SELECT version()"),
            "server_version_num": await control.fetchval("SHOW server_version_num"),
        }
        print("P3_IDENTITY_SCALE_FACTS=" + json.dumps(facts, default=str))
        write_band_artifact("p3-identity-scale.json", facts)
        return facts
    finally:
        if pool is not None:
            await pool.close()
        await fleet_resources.aclose()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()
        logging.disable(previous_log_disable)


@pytest.mark.timeout(1800)
@pytest.mark.xdist_group("wf_identity_p3_scale")
async def test_real_200k_unique_records_at_shipped_default_bound(pg_dsn: str) -> None:
    configured = os.environ.get("TASKQ_P3_TEST_RECORDS")
    count = _RECORDS if configured is None else int(configured)
    facts = await _exercise(pg_dsn, count)
    assert facts["requested_records"] == count
    # An if-free default guard: final output identifies any debug override and
    # cannot pass it off as the obligated 200k artifact.
    assert configured is not None or count == _RECORDS
