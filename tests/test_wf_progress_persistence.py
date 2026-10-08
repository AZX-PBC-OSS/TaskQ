"""T21 — THE PERSISTENCE PINS: the two-channel progress substrate.

The provenance: the PoC's PROOF (t21-20261008T033005Z, PROVEN - DH1-DH8
closed, 3 consecutive clean rounds) is the spec's proven shape; these pins
port its obligations onto the BUILT surface, in the estate's pin idiom.

The red-first evidence: each pin's CONVICTED VARIANT (the spike's dragons)
is named in the docstring and — where the variant is observable — drilled
with the red captured to ``.measurements/t21-pin-reds.json``. The unfenced
variants stay RED FOREVER (they are the dragons, not bugs).

THE CHANNELS (T21 decision d):
* the STATE channel — ``wf_node_progress``: ONE row per (node, channel),
  upserted latest-wins + the honest occurrence counter. The row count is
  nodes x channels, CONSTANT whatever the emission rate (DH1's fence).
* the STREAM channel — ``wf_node_stream``: append rows in a bounded
  per-node ring, drop-oldest with the dropped counter ON THE RECORD
  (DH2's fence), ONE seq space for both classes (DH3's fence).
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._progress import (
    CLASS_AUTO,
    CLASS_USER,
    KIND_NODE_TERMINAL,
    KIND_PROGRESS,
    PROGRESS_RING_BOUND,
    project_auto_event,
)
from taskq.workflows._sql import WorkflowSql
from tests._wf_fixtures import RedLog, seed_flow, seed_running_node

pytestmark = pytest.mark.integration


async def _append(
    conn: asyncpg.Connection,
    wsql: WorkflowSql,
    node_id: JobId,
    flow_id: JobId,
    *,
    cls: str = CLASS_USER,
    kind: str = KIND_PROGRESS,
    payload: dict[str, Any] | None = None,
) -> tuple[int, int]:
    """One stream append through the BUILT statement (the drop count RETURNED)."""
    row = await conn.fetchrow(
        wsql.progress_stream_append,
        node_id,
        flow_id,
        cls,
        kind,
        json.dumps(payload or {}),
        PROGRESS_RING_BOUND,
    )
    assert row is not None
    return int(row["seq"]), int(row["dropped"])


# ── THE STRUCTURAL PINS ──────────────────────────────────────────────────


async def test_two_channel_tables_exact_shape(wf_conn: asyncpg.Connection, wf_schema: str) -> None:
    """The migration's exact shape (01.00.28): the STATE channel's PK is
    (node_id, channel) — the latest-wins row's IDENTITY, the fence that
    makes the row count nodes x channels; the STREAM channel's PK is the
    bigserial seq — THE ONE SEQ SPACE (DH3's fence), DB-side because a
    position is not an identity (the 01.00.20_03 progress_seq precedent).
    And exactly ONE sequence generator backs the stream: a second
    seq-cursor space is the fleet's vocabulary fragmentation re-created."""
    cols = await wf_conn.fetch(
        f"""SELECT table_name, column_name, data_type FROM information_schema.columns
            WHERE table_schema = '{wf_schema}'
            AND table_name IN ('wf_node_progress', 'wf_node_stream')
            ORDER BY table_name, ordinal_position"""
    )
    names = {(r["table_name"], r["column_name"]): r["data_type"] for r in cols}
    for c in (
        "node_id",
        "channel",
        "pct",
        "message",
        "data",
        "occurrences",
        "dropped",
        "last_seq",
        "updated_at",
    ):
        assert ("wf_node_progress", c) in names, f"the STATE channel lacks {c!r}"
    for c in ("seq", "node_id", "flow_id", "class", "kind", "payload", "emitted_at"):
        assert ("wf_node_stream", c) in names, f"the STREAM channel lacks {c!r}"
    assert names[("wf_node_progress", "occurrences")] == "bigint", (
        "the occurrence counter is a bigint: an int4 counter is the "
        "01.00.20_03 overflow incident's shape"
    )
    pks = await wf_conn.fetch(
        f"""SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tc.constraint_name
             AND kcu.table_schema = tc.table_schema
            WHERE tc.table_schema = '{wf_schema}' AND tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name IN ('wf_node_progress', 'wf_node_stream')
            ORDER BY tc.table_name, kcu.ordinal_position"""
    )
    by_table: dict[str, list[str]] = {}
    for r in pks:
        by_table.setdefault(r["table_name"], []).append(r["column_name"])
    assert by_table["wf_node_progress"] == ["node_id", "channel"], by_table
    assert by_table["wf_node_stream"] == ["seq"], by_table
    seqs = await wf_conn.fetch(
        f"""SELECT sequencename FROM pg_sequences WHERE schemaname = '{wf_schema}'
            AND sequencename LIKE 'wf_node_stream%'"""
    )
    assert len(seqs) == 1, (
        f"exactly ONE sequence generator backs the stream, found {len(seqs)} "
        "— a second seq space is DH3's fragmentation dragon"
    )


async def test_stream_vocabulary_closed_at_the_storage_domain(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """DH3's TEETH: the class/kind CHECK constraints refuse a third
    vocabulary at the STORAGE domain — the Python emit path validates the
    same frozenset first, and a bypassing writer (a future migration, a
    manual INSERT) is refused by the row's own table, not by review."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    for cls, kind in (("bogus", KIND_PROGRESS), (CLASS_USER, "bogus.kind")):
        with pytest.raises(asyncpg.CheckViolationError):
            await wf_conn.execute(
                f'INSERT INTO "{wf_schema}".wf_node_stream '
                "(node_id, flow_id, class, kind) VALUES ($1, $2, $3, $4)",
                node,
                flow,
                cls,
                kind,
            )


async def test_state_upsert_latest_wins_counter_honest(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """The STATE channel's contract: N upserts → ONE row, latest-wins
    payload, ``occurrences`` == N (the coalescing is HONEST — the counter
    counts every emission that coalesced into the row, so the record shows
    both the delivered state and the emission rate)."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    for i, pct in ((1, 10), (2, 50), (3, 90)):
        row = await wf_conn.fetchrow(
            wf_sql.progress_state_upsert,
            node,
            "progress",
            pct,
            f"step {i}",
            None,
            1,
            0,
            None,
        )
        assert row is not None
    rows = await wf_conn.fetch(
        f'SELECT * FROM "{wf_schema}".wf_node_progress WHERE node_id = $1', node
    )
    assert len(rows) == 1, "N upserts made N rows — the every-emission-a-row shape"
    assert rows[0]["pct"] == 90 and rows[0]["message"] == "step 3"
    assert rows[0]["occurrences"] == 3


async def test_stream_ring_bound_and_drop_counter_honest(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """DH2's fence on the BUILT statement: the ring retains exactly the
    bound and the drop counter's accounting is EXACT —
    appended == retained + dropped. A dropped row that vanished uncounted
    would be the Temporal-leak's honest-face lie."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    appended = 0
    dropped = 0
    for i in range(100):
        _seq, d = await _append(
            wf_conn, wf_sql, node, flow, payload={"pct": i, "message": "m", "data": None}
        )
        appended += 1
        dropped += d
    retained = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', node
    )
    assert retained == PROGRESS_RING_BOUND
    assert appended == retained + dropped, (
        f"appended {appended} != retained {retained} + dropped {dropped} — the "
        "drop counter's accounting is not exact (the honest pair broken)"
    )
    assert dropped == 100 - PROGRESS_RING_BOUND


async def test_one_seq_space_classes_interleave(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql, wf_pool: asyncpg.Pool
) -> None:
    """DH3 on the BUILT projection: the engine's auto-class events and the
    body's user-class emissions interleave in ONE seq range — a single
    cursor serves both classes (the two-vocabulary fragmentation cannot
    re-appear)."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    seqs: list[int] = []
    seqs.append(
        await project_auto_event(
            wf_pool,
            wf_sql,
            flow_id=flow,
            node_id=node,
            kind="wf.node.started",
            payload={"step_key": "a"},
        )
    )
    s, _d = await _append(wf_conn, wf_sql, node, flow, payload={"pct": 50})
    seqs.append(s)
    seqs.append(
        await project_auto_event(
            wf_pool,
            wf_sql,
            flow_id=flow,
            node_id=node,
            kind=KIND_NODE_TERMINAL,
            payload={"outcome": "succeeded"},
        )
    )
    assert seqs == sorted(seqs), "the classes do not share one monotone seq space"
    rows = await wf_conn.fetch(
        f"""SELECT class, kind FROM "{wf_schema}".wf_node_stream
            WHERE node_id = $1 ORDER BY seq""",
        node,
    )
    assert [(r["class"], r["kind"]) for r in rows] == [
        (CLASS_AUTO, "wf.node.started"),
        (CLASS_USER, KIND_PROGRESS),
        (CLASS_AUTO, KIND_NODE_TERMINAL),
    ]


async def test_project_auto_event_refuses_vocabulary_escape(
    wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The projection's Python gate: a kind outside the closed vocabulary
    is REFUSED (the typed refusal) — no third vocabulary can sprout even
    from engine code written next week."""
    flow_id = JobId(new_uuid())
    node_id = JobId(new_uuid())
    with pytest.raises(ValueError, match="closed vocabulary"):
        await project_auto_event(
            wf_pool,
            wf_sql,
            flow_id=flow_id,
            node_id=node_id,
            kind="wf.node.custom-thing",
            payload={},
        )


async def test_ring_prune_arm_prunes_leaked_ring(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
    wf_pool: asyncpg.Pool,
    progress_redlog: RedLog,
) -> None:
    """THE RETENTION ARM (DH1's "prunes on schedule"): rings that leaked
    past the bound — the red world's shape, seeded here by bypassing the
    append-trim — are pruned back to the bound in ONE pass. The LEAK is
    observed first (the red): rows growing past the bound with no trim."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    leak = PROGRESS_RING_BOUND + 737  # the PoC's red world's shape (801)
    for _i in range(leak):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_node_stream (node_id, flow_id, class, kind, payload) '
            "VALUES ($1, $2, $3, $4, '{}')",
            node,
            flow,
            CLASS_USER,
            KIND_PROGRESS,
        )
    before = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', node
    )
    progress_redlog.red(
        "ring_prune_arm_prunes_leaked_ring",
        "the append-trim bypassed (the red world's unpruned ring)",
        {"rows_before": before, "bound": PROGRESS_RING_BOUND},
    )
    assert before == leak, "the leak did not leak"
    head_before = await wf_conn.fetchval(
        f'SELECT max(seq) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', node
    )
    from taskq.workflows import sweep_progress_ring_prune

    n = await sweep_progress_ring_prune(wf_pool, wf_sql)
    after = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', node
    )
    assert n == leak - PROGRESS_RING_BOUND
    assert after == PROGRESS_RING_BOUND, (
        f"the arm pruned to {after}, not the bound — the backstop's own trim is broken"
    )
    # The NEWEST rows survive (drop-OLDEST): the node's own head seq — the
    # global seq space's position at its last append — is untouched.
    head = await wf_conn.fetchval(
        f'SELECT max(seq) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', node
    )
    assert head == head_before, "the arm dropped the newest rows — drop-oldest, not drop-newest"


async def test_map_progress_line_reads_the_state_channel(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """THE AGGREGATION'S MAP LINE (decision c): the certified grouped read
    LEFT JOINed to the state channel — the children's emitted progress
    (avg pct, the freshest update) rides the SAME read, computed IN the
    query, never a second instrument. A child that never emitted reads
    NULL avg_pct and the counts stay complete (the LEFT JOIN's law)."""
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow)
    child_ids = [new_uuid() for _ in range(3)]
    for cid in child_ids:  # the map children: rows WITH the parent link
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, parent_id, metadata) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'item', $2, $3::jsonb)",
            cid,
            source,
            json.dumps({"flow_id": str(flow)}),
        )
    for i, cid in enumerate(child_ids[:2]):  # two emitted, one silent
        await wf_conn.fetchrow(
            wf_sql.progress_state_upsert, cid, "progress", 40 + i * 20, "m", None, 1, 0, None
        )
    rows = await wf_conn.fetch(wf_sql.workflow_map_progress, source)
    assert len(rows) == 1
    r = rows[0]
    assert r["total"] == 3 and r["running"] == 3 and r["done"] == 0
    assert r["avg_pct"] == 50, f"avg pct over the emitted two, got {r['avg_pct']}"
    assert r["freshest"] is not None


async def test_child_results_read_is_the_aggregate_input(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """The ``aggregate=`` fn's input (decision c): the children's RESULT
    rows — SUCCEEDED-with-result only. A READ; the fn runs over these at
    read time; no join node exists for it (DH8's fence)."""
    flow = await seed_flow(wf_conn, wf_schema)
    parent = await seed_running_node(wf_conn, wf_schema, flow)
    ok = new_uuid()
    empty = new_uuid()
    for jid, status, result in (
        (ok, "succeeded", '{"risk": 3}'),
        (empty, "running", None),
    ):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, parent_id, result, metadata) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', $2, 'item', $3, "
            "$4::jsonb, $5::jsonb)",
            jid,
            status,
            parent,
            result,
            json.dumps({"flow_id": str(flow)}),
        )
    rows = await wf_conn.fetch(wf_sql.progress_child_results, parent)
    assert len(rows) == 1 and rows[0]["id"] == ok
    # jsonb arrives as str on un-coded connections — the estate's _json
    # seam parses (the fixtures' own note).
    raw = rows[0]["result"]
    result = raw if isinstance(raw, dict) else json.loads(raw)
    assert result == {"risk": 3}


# ── THE ZERO-FINALIZE-CHANGES PROBE ──────────────────────────────────────


def test_zero_finalize_changes_probe() -> None:
    """THE ASYMMETRY, structurally pinned: the finalize's terminal-mark
    statement touches NO progress table — the emission path can never
    block the finalize because the finalize does not know the emission
    path exists (the PoC's P4.finalize_touches_no_progress_table, ported
    to the built statement)."""
    from taskq.workflows._sql_finalize import TERMINAL_MARK_SQL

    lowered = TERMINAL_MARK_SQL.lower()
    assert "wf_node_progress" not in lowered
    assert "wf_node_stream" not in lowered
    assert "progress" not in lowered, (
        "the terminal-mark statement mentions progress — the finalize path "
        "has grown a dependency on the emission substrate (the asymmetry broken)"
    )


# ── THE RED (DH1's convicted variant — kept RED FOREVER) ─────────────────


async def test_red_every_emission_a_row_grows_unbounded(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql, progress_redlog: RedLog
) -> None:
    """DH1's red, observed on the BUILT substrate: the convicted variant —
    one row PER EMISSION (the 6.5M-row incident's shape) — grows the store
    with emissions; the two-channel design holds it at nodes x channels +
    the ring bound. The unfenced variant stays RED FOREVER (the drill
    writes a scratch log table, never the shipped tables)."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    emissions = 500
    await wf_conn.execute(
        f"""CREATE TABLE "{wf_schema}".wf_progress_log_red (
                node_id uuid, pct int, message text)"""
    )
    try:
        for i in range(emissions):
            await wf_conn.execute(
                f'INSERT INTO "{wf_schema}".wf_progress_log_red VALUES ($1, $2, $3)',
                node,
                i,
                "m",
            )
            # the BUILT state channel rides the same emissions (the upsert)
            await wf_conn.fetchrow(
                wf_sql.progress_state_upsert, node, "progress", i, "m", None, 1, 0, None
            )
        log_rows = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_progress_log_red')
        state_rows = await wf_conn.fetchval(
            f"""SELECT count(*) FROM "{wf_schema}".wf_node_progress
                WHERE node_id = $1""",
            node,
        )
        progress_redlog.red(
            "red_every_emission_a_row_grows_unbounded",
            "one row per emission (the 6.5M-row incident's shape)",
            {
                "emissions": emissions,
                "log_rows": log_rows,
                "state_rows": state_rows,
                "verdict": "the log grows with emissions; the state channel does not",
            },
        )
        assert log_rows == emissions, "the convicted variant did not convict"
        assert state_rows == 1, "the state channel grew with emissions"
    finally:
        await wf_conn.execute(f'DROP TABLE "{wf_schema}".wf_progress_log_red')
