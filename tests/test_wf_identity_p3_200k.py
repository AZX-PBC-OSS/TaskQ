"""#717 real 200k record identity/default-backpressure scale proof.

The default invocation is exactly 200,000 records. ``TASKQ_P3_TEST_RECORDS`` is
only an explicit developer-debug override; final evidence uses the default and
asserts that persisted count, and only the default writes (and claims) the
obligated ``p3-identity-scale`` capture — a debug run's short capture never
becomes that stem's live claim. The source always pages from its durable cursor,
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

#: The proof's own orchestration deadline. 200 pages at the issue's slowest
#: reported CI page rate (11.1 s/page; see ``_MEASURED_SLOWEST_PAGE_SECONDS`` in
#: ``tests/test_wf_identity_p3_200k_harness.py``) project linearly to ~2,220 s,
#: and the page rate is bounded only from below, so this budget clears that
#: projection by ~22%.
_INNER_TIMEOUT_S = 2700.0
#: ``pytest-timeout`` spans bootstrap, orchestration, post-flight verification,
#: and schema teardown as one window while the inner deadline wraps
#: ``orchestrate()`` alone. The margin keeps them strictly ordered, so an
#: orchestration overrun exits through the inner ``TimeoutError`` instead of
#: this marker's asynchronous ``SIGALRM``.
_OUTER_TIMEOUT_S = _INNER_TIMEOUT_S + 300.0


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

        # A whole-page claim lets one pod take every record while the other
        # spins on empty dispatch rounds; an even share of the page each keeps
        # both pods working every dispatched page without touching the page
        # size (the source still emits 1,000 records per cursor advance).
        claim_limit = _PAGE // len(fleet.pods)

        async def work(pod_index: int) -> None:
            pod = fleet.pods[pod_index]
            runner = runners[pod_index]
            while not stop.is_set():
                # Production dispatch arbitrates these two pods. Execution uses
                # the workflow fleet-claimed door with the exact physical fence.
                claimed = await pod.claim(["default"], claim_limit)
                if not claimed:
                    await asyncio.sleep(0.01)
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
                # A bare ``gather`` resolves on the first sibling that raises or
                # finishes cancelling and then returns to ``_exercise``'s
                # teardown with the rest of the batch still holding connections
                # and transactions. The group cancels and awaits every sibling
                # before this batch returns, on both failure and cancellation.
                async with asyncio.TaskGroup() as batch:
                    steps = [
                        batch.create_task(
                            runner.run_fleet_claimed_step(
                                flow,
                                document,
                                attempt=int(document["attempt"]),
                                claim_epoch=int(document["claim_epoch"]),
                            )
                        )
                        for document in documents
                    ]
                outcomes = [step.result() for step in steps]
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
                # ``tick`` runs five sweep passes; polling it every millisecond
                # piles redundant sweep queries onto the same Postgres the
                # claimed pods are working, so this seat idles between passes.
                await asyncio.sleep(0.05)

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(work(0))
            tasks.create_task(work(1))
            await asyncio.wait_for(orchestrate(), timeout=_INNER_TIMEOUT_S)
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
        # That history is satisfied vacuously by a single row, so pin the
        # runtime trace that the shipped default bound actually refused a page:
        # ``source_calls`` counts one entry per source attempt (``1 + parks``).
        # A one-page run parks nothing by construction — the harness drives
        # call ``_exercise(pg_dsn, _ONE_PAGE)`` — so the floor applies only
        # above one page.
        if record_count > _PAGE:
            assert source_calls > 1, (
                f"the shipped default bound never refused a page: {record_count} records "
                f"emitted in one source attempt (source_calls={source_calls})"
            )
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
            "source_parks": source_calls - 1,
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
        return facts
    finally:
        if pool is not None:
            await pool.close()
        await fleet_resources.aclose()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()
        logging.disable(previous_log_disable)


@pytest.mark.timeout(_OUTER_TIMEOUT_S)
@pytest.mark.xdist_group("wf_identity_p3_scale")
async def test_real_200k_unique_records_at_shipped_default_bound(pg_dsn: str) -> None:
    configured = os.environ.get("TASKQ_P3_TEST_RECORDS")
    count = int(configured) if configured is not None else _RECORDS
    facts = await _exercise(pg_dsn, count)
    records = facts["records"]
    assert isinstance(records, dict), records
    assert facts["requested_records"] == count
    assert int(records["rows"]) == count
    assert int(records["identities"]) == count
    if count > _PAGE:
        # The band artifact must carry *checked* evidence that #717's shipped
        # default bound bit: one source attempt for a multi-page run means the
        # source never parked at saturation.
        assert int(facts["source_calls"]) > 1, (
            f"the obligated {count}-record run never parked its source "
            f"(source_calls={facts['source_calls']}, source_parks={facts['source_parks']}), so "
            "it exercised none of the shipped default backpressure bound"
        )
    # The obligated evidence run is the 200,000 default: the default arm pins
    # the persisted count to that number, so a shortened ``_RECORDS`` cannot
    # pass itself off as the artifact. An explicit override is a debug run and
    # is asserted against its own configured target above.
    assert configured is not None or int(records["rows"]) == 200_000
    if configured is None:
        # The obligated capture, and the manifest claim it appends, belong to
        # the 200,000 default alone, recorded only once this run's own verdict
        # is green: the evidence verifier reads a stem's NEWEST claim, so a
        # debug override's capture must never become that stem's live claim.
        write_band_artifact("p3-identity-scale.json", facts)
