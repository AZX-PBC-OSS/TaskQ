"""#717 P3 populated old/post plans, arbiter definitions and migration facts."""

# ruff: noqa: S608  # Why: only the generated fixture schema is interpolated; all row values are bound.

from __future__ import annotations

import json
import sys
import time
from collections.abc import Iterator
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.migrate import apply_pending
from taskq.workflows._policy import row_retry_curve
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import seed_flow, write_band_artifact

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def _plan(conn: asyncpg.Connection, sql: str, *args: object) -> tuple[dict[str, Any], str]:
    raw = await conn.fetchval(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}", *args)
    text = raw if isinstance(raw, str) else json.dumps(raw)
    decoded = json.loads(text)
    return decoded[0]["Plan"], text


def _walk(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans", []):
        yield from _walk(child)


def _assert_index_backed(plan: dict[str, Any], text: str, index: str) -> None:
    nodes = list(_walk(plan))
    assert not any(node.get("Node Type") == "Seq Scan" for node in nodes), text
    assert index in text, text


async def test_populated_old_and_post_crosswidth_plans_and_rewrite_measurements(
    pg_dsn: str,
) -> None:
    schema = "idp3plan_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema, phase="pre")
        wsql = render_workflow_sql(schema)
        flows = [await seed_flow(conn, schema) for _ in range(500)]
        node_args: list[tuple[object, ...]] = []
        ledger_args: list[tuple[object, ...]] = []
        for flow in flows:
            for index in range(50):
                node = new_uuid()
                metadata = json.dumps({"flow_id": str(flow)})
                ledger_args.append((new_uuid(), flow, node, "record", index, 1, 0))
                node_args.append(
                    (
                        node,
                        "wf",
                        "default",
                        "{}",
                        3,
                        "transient",
                        None,
                        index,
                        "record",
                        0,
                        f"record-{index}",
                        metadata,
                        "",
                        None,
                        0,
                        None,
                        str(flow),
                        None,
                        *row_retry_curve(None),
                    )
                )
        await conn.executemany(wsql.node_insert, node_args)
        await conn.executemany(wsql.ledger_claim, ledger_args)
        await conn.execute(f'ANALYZE "{schema}".jobs')
        await conn.execute(f'ANALYZE "{schema}".wf_step_ledger')

        arbiter_sql = (
            f'SELECT id FROM "{schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = $2 "
            "AND COALESCE(map_index, -1) = COALESCE($3::bigint, -1) "
            "AND attempt = $4 AND status IN ('running', 'awaited', 'crashed')"
        )
        flow_sql = f'SELECT id FROM "{schema}".wf_step_ledger WHERE flow_id = $1'
        # The actual hot outstanding-row count uses the flow/status composite
        # index; a first-rows LIMIT probe is not this path and is intentionally
        # not substituted for the production statement here.
        jobs_sql = wsql.emit_in_flight
        plans: list[dict[str, object]] = []
        before_counts: dict[str, int] = {}
        after_counts: dict[str, int] = {}
        before_bytes: dict[str, int] = {}
        after_bytes: dict[str, int] = {}
        rewrite_ms = 0.0
        for phase in ("old", "post"):
            if phase == "post":
                await conn.execute(
                    wsql.ledger_claim, new_uuid(), flows[0], new_uuid(), "record", 32768, 1, 0
                )
            arbiter_bound = 32768 if phase == "post" else 7
            arbiter, arbiter_text = await _plan(
                conn, arbiter_sql, flows[0], "record", arbiter_bound, 1
            )
            flow, flow_text = await _plan(conn, flow_sql, flows[0])
            jobs, jobs_text = await _plan(conn, jobs_sql, flows[0], new_uuid())
            _assert_index_backed(arbiter, arbiter_text, "wf_step_ledger_claim_uniq")
            _assert_index_backed(flow, flow_text, "wf_step_ledger_claim_uniq")
            _assert_index_backed(jobs, jobs_text, "jobs_wf_flow_nodes_idx")
            plans.extend(
                (
                    {"phase": phase, "query": "ledger_identity", "plan": arbiter_text},
                    {"phase": phase, "query": "ledger_flow", "plan": flow_text},
                    {"phase": phase, "query": "jobs_flow", "plan": jobs_text},
                )
            )
            if phase == "old":
                before_counts = {
                    table: int(
                        await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') or 0
                    )
                    for table in (
                        "jobs",
                        "jobs_archive",
                        "wf_step_ledger",
                        "wf_signals",
                        "wf_outbox",
                    )
                }
                before_bytes = {
                    table: int(
                        await conn.fetchval(
                            "SELECT pg_total_relation_size($1)", f'"{schema}"."{table}"'
                        )
                        or 0
                    )
                    for table in before_counts
                }
                started = time.perf_counter_ns()
                await apply_pending(conn, schema=schema, phase="post")
                rewrite_ms = (time.perf_counter_ns() - started) / 1e6
                after_counts = {
                    table: int(
                        await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') or 0
                    )
                    for table in before_counts
                }
                assert after_counts == before_counts
                after_bytes = {
                    table: int(
                        await conn.fetchval(
                            "SELECT pg_total_relation_size($1)", f'"{schema}"."{table}"'
                        )
                        or 0
                    )
                    for table in before_counts
                }
                await conn.execute(f'ANALYZE "{schema}".jobs')
                await conn.execute(f'ANALYZE "{schema}".wf_step_ledger')

        index_rows = await conn.fetch(
            "SELECT c.relname, pg_get_indexdef(c.oid) AS definition "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname IN "
            "('wf_step_ledger_claim_uniq', 'wf_signals_hold_identity_uniq')",
            schema,
        )
        # Populate the unused signal table before measuring its final index.
        await conn.execute(
            f'INSERT INTO "{schema}".wf_signals '
            "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status, map_index) "
            "VALUES ($1, $2, 'hold', 'approve', 1, 'call', 'held', 32768)",
            new_uuid(),
            flows[0],
        )
        assert len(index_rows) == 2
        for row in index_rows:
            definition = str(row["definition"])
            assert "COALESCE" in definition and "bigint" in definition, definition
            if row["relname"] == "wf_signals_hold_identity_uniq":
                assert "resolved_at IS NULL" in definition

        operators = await conn.fetch(
            "SELECT o.oprname, o.oprcode::regproc::text AS function, "
            "l.typname AS left_type, r.typname AS right_type "
            "FROM pg_operator o "
            "JOIN pg_type l ON l.oid = o.oprleft JOIN pg_type r ON r.oid = o.oprright "
            "WHERE o.oprname = '=' AND o.oprcode::regproc::text IN ('int48eq', 'int8eq') "
            "ORDER BY function"
        )
        operator_rows = [dict(row) for row in operators]
        assert {row["function"] for row in operator_rows} == {"int48eq", "int8eq"}
        assert {(row["left_type"], row["right_type"]) for row in operator_rows} == {
            ("int4", "int8"),
            ("int8", "int8"),
        }
        facts = {
            "pin": "p3-map-index-populated-plans-and-post",
            "python": sys.version,
            "postgres": await conn.fetchval("SELECT version()"),
            "server_version_num": await conn.fetchval("SHOW server_version_num"),
            "rows_per_flow": 50,
            "flows": 500,
            "before_counts": before_counts,
            "after_counts": after_counts,
            "before_total_bytes": before_bytes,
            "after_total_bytes": after_bytes,
            "post_elapsed_ms": rewrite_ms,
            "operators": operator_rows,
            "plans": plans,
        }
        print("P3_IDENTITY_MIGRATION_FACTS=" + json.dumps(facts))
        write_band_artifact("p3-map-index-post.json", facts)
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
