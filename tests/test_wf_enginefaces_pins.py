"""THE ENGINE-FACES PINS — the observability front's reported ENGINE
findings, fenced red-first (the engine lane's evidence).

The 3am pins (``tests/test_wf_3am_pins.py``) fenced the CLI/explorer
faces; the ENGINE findings behind them ride here:

1. THE RECLAIM-REQUIRES-A-LEADER SILENCE — a lapsed lease with a DEAD
   fleet waits forever, silently: the rows say "running" to the end of
   time (the reclaim sweep runs only under a leader — with the fleet
   down nothing will ever stamp the row, so a SWEEP-STAMP face is the
   marker that lies by absence). The row-visible marker is the STATUS
   DERIVATION'S INPUT: the node rollup's own read carries the lease
   columns and the surfaces derive "reclaimable, awaiting a leader" from
   them — the operator's "is this work slow or is everyone dead" is
   answerable from the surfaces alone.
2. WORKFLOW_NODES_SQL'S MISSING COLUMNS — the rollup didn't select
   ``started_at`` / ``failed_parent``, so the CLI bought them with a
   SECOND read (the supplement read: two reads where one suffices). The
   columns are selected AT THE SOURCE; the CLI's workaround is retired —
   ONE read, and the supplement's faces keep working through the engine
   query.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from typer.testing import CliRunner

from taskq.cli import app
from taskq.testing.assertions import plain_cli_output
from taskq.workflows._cli import FlowNodeRow, format_flow_status
from taskq.workflows._sql import WorkflowSql

runner = CliRunner()

_NOW = datetime(2026, 10, 12, 17, 0, 0, tzinfo=UTC)

_JOB_ID = "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"
_RUN_ID = "019f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"


# ── 1. the reclaim-requires-a-leader marker (the dead-fleet face) ───────


def _lapsed_running_row(**overrides: Any) -> FlowNodeRow:
    started = _NOW - timedelta(minutes=5, seconds=31)
    lapsed = _NOW - timedelta(minutes=2)
    fields: dict[str, Any] = {
        "step_key": "hang",
        "status": "running",
        "started_at": started,
        "lock_expires_at": lapsed,
        "job_id": _JOB_ID,
    }
    fields.update(overrides)
    return FlowNodeRow(**fields)


def test_lapsed_lease_running_row_names_reclaimable_awaiting_a_leader() -> None:
    """A running row whose lock_expires_at has passed — with the fleet
    dead, no sweep will EVER touch it — must surface 'reclaimable,
    awaiting a leader' on the status face: the 3am question ('is this
    work slow or is everyone dead') is answerable from the surface
    alone. The convicted variant: the row read as the ordinary RUNNING
    face (or a bare '1 running' count) — the lease's lapse, the one fact
    that names the fleet's death, was invisible on every surface."""
    lines = format_flow_status(
        run_id=_RUN_ID,
        workflow="drill_hang",
        root_status="running",
        nodes=[_lapsed_running_row()],
        now=_NOW,
    )
    joined = "\n".join(lines)
    assert "RECLAIMABLE" in joined
    assert "awaiting a leader" in joined
    assert _JOB_ID in joined
    # The marker must NOT read as the ordinary running face: the lapsed
    # row's verdict is the fleet's death, not the work's age.
    assert "RUNNING for" not in joined


def test_live_lease_running_row_keeps_the_running_face() -> None:
    """The negative face: a running row whose lease is LIVE (or carries
    no lease columns at all — the historical row shape) reads as the
    ordinary RUNNING face, never the fleet's death (a false RECLAIMABLE
    on every healthy running row would cry wolf)."""
    live = _lapsed_running_row(lock_expires_at=_NOW + timedelta(minutes=5))
    for row in (live, _lapsed_running_row(lock_expires_at=None)):
        lines = format_flow_status(
            run_id=_RUN_ID,
            workflow="w",
            root_status="running",
            nodes=[row],
            now=_NOW,
        )
        joined = "\n".join(lines)
        assert "RECLAIMABLE" not in joined
        if row.lock_expires_at is not None:
            assert "RUNNING for" in joined


def test_lapsed_lease_pending_row_stays_the_join_face() -> None:
    """The marker is the RUNNING row's face: a lapsed LEASE cannot exist
    on a pending row (only a running row holds a lock) — a pending row's
    liveness is the join's, and the marker must not leak onto it."""
    pending = FlowNodeRow(
        step_key="collect",
        status="pending",
        deps_pending=1,
        blocking_reason="join",
        lock_expires_at=_NOW - timedelta(minutes=2),
        job_id=_JOB_ID,
    )
    lines = format_flow_status(
        run_id=_RUN_ID,
        workflow="w",
        root_status="running",
        nodes=[pending],
        now=_NOW,
    )
    assert not any("RECLAIMABLE" in line for line in lines)


def test_lapsed_lease_without_a_row_id_still_names_the_verdict() -> None:
    """A row double without its id (a hand-built row, a partial read)
    still renders the marker — the verdict is the LEASE's derivation,
    never the id's presence; only the remedy line is id-less."""
    lines = format_flow_status(
        run_id=_RUN_ID,
        workflow="w",
        root_status="running",
        nodes=[_lapsed_running_row(job_id=None)],
        now=_NOW,
    )
    joined = "\n".join(lines)
    assert "RECLAIMABLE" in joined
    assert "taskq job events" not in joined


def test_running_row_with_a_live_lease_and_no_start_is_silent() -> None:
    """A running row carrying a live lease but no started_at (the
    historical shape) renders no running-face line at all — no line is
    the honest 'nothing derivable', never a guessed verdict."""
    bare = FlowNodeRow(
        step_key="hang",
        status="running",
        started_at=None,
        lock_expires_at=_NOW + timedelta(minutes=5),
        job_id=_JOB_ID,
    )
    lines = format_flow_status(
        run_id=_RUN_ID,
        workflow="w",
        root_status="running",
        nodes=[bare],
        now=_NOW,
    )
    assert not any("RUNNING for" in line for line in lines)
    assert not any("RECLAIMABLE" in line for line in lines)


# ── 2. the columns at the source: ONE read, no supplement ──────────────


class _CountingConn:
    """The flows-status read's connection double: counts every jobs-table
    fetch (the node face's reads), answers from the canned rows."""

    def __init__(self, node_rows: list[dict[str, Any]]) -> None:
        self.node_rows = node_rows
        self.jobs_fetches = 0
        self.jobs_fetch_sqls: list[str] = []

    async def fetchrow(self, sql: str, *args: object) -> Any:
        return {
            "status": "running",
            "actor": "wf-drill",
            "workflow": "drill_hang",
            "cancel_requested_at": None,
        }

    async def fetch(self, sql: str, *args: object) -> Any:
        if "wf_signals" in sql:
            return []
        self.jobs_fetches += 1
        self.jobs_fetch_sqls.append(sql)
        return self.node_rows

    async def close(self) -> None: ...


def _node_row(**overrides: Any) -> dict[str, Any]:
    # The CLI's `flows status` derives `now` from the real clock (no
    # `now=` injection through the typer seam) — the fixture's instants
    # ride the REAL now so the lease is live and the age exact.
    real_now = datetime.now(UTC)
    started = real_now - timedelta(minutes=5, seconds=31)
    row: dict[str, Any] = {
        "id": _JOB_ID,
        "step_key": "hang",
        "map_index": None,
        "status": "running",
        "deps_pending": 0,
        "parent_id": None,
        "blocking_reason": None,
        "attempt": 1,
        "max_attempts": 3,
        "absorbed": False,
        "error_jsonb": None,
        "error_class": None,
        "error_message": None,
        "started_at": started,
        "failed_parent": None,
        "lock_expires_at": real_now + timedelta(minutes=5),
        "last_heartbeat_at": started,
        # THE §22.1 STAMP (rv5-t5's cure — the drift face's data): the
        # node row's code_version rides the row; the stamps' read names
        # a run whose rows straddle a redeploy.
        "code_version": None,
    }
    row.update(overrides)
    return row


def _run_flows_status(
    monkeypatch: pytest.MonkeyPatch,
    conn: _CountingConn,
) -> str:
    async def fake_connect(dsn: str) -> Any:
        return conn

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setenv("TASKQ_PG_DSN", "postgresql://postgres:taskq@localhost:5844/taskq")
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "taskq")
    result = runner.invoke(app, ["flows", "status", _RUN_ID])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    return plain_cli_output(result.output)


def test_the_node_face_rides_one_read_through_the_engine_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE ONE-READ LAW: the CLI's status face is fed by the ENGINE's
    WORKFLOW_NODES_SQL — the query selects the 3am columns (started_at,
    failed_parent, the lease face's lock_expires_at) at the SOURCE, and
    the CLI issues ONE jobs fetch for the node rows. The convicted
    variant: the engine query lacked the columns and the CLI bought them
    with its own supplement read — two reads where one suffices, the
    workaround the engine query's shape forced."""
    conn = _CountingConn([_node_row()])
    out = _run_flows_status(monkeypatch, conn)
    assert conn.jobs_fetches == 1, (
        f"the node face must ride ONE jobs read, got {conn.jobs_fetches}: {conn.jobs_fetch_sqls}"
    )
    # The supplement's faces keep working THROUGH the engine query: the
    # running age (started_at) and the row's id render from the one read.
    assert "RUNNING for 00:05:31" in out
    assert _JOB_ID in out


def test_failed_parent_rides_the_engine_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failed_parent stamp (the T06 cascade's write — the dead
    parent's id, NAMED on the record) rides the engine query too: the
    why-stuck line prints it from the ONE read."""
    parent_id = "01a130ef-813b-7f50-86b2-14d8c01a61e5"
    conn = _CountingConn(
        [
            _node_row(
                status="pending",
                deps_pending=1,
                blocking_reason="failed_parent",
                failed_parent=parent_id,
                started_at=None,
                lock_expires_at=None,
                last_heartbeat_at=None,
            )
        ]
    )
    out = _run_flows_status(monkeypatch, conn)
    assert conn.jobs_fetches == 1
    assert parent_id in out


def test_the_engine_query_names_the_3am_columns() -> None:
    """THE SOURCE LAW: WORKFLOW_NODES_SQL — the node rollup every status
    surface reads — selects the columns the faces need (started_at, the
    failed_parent stamp, the lease face's lock_expires_at). The CLI's
    supplement read existed BECAUSE the engine query didn't; the columns
    at the source are the cure's substance (the string law pins the
    query, the one-read pins above pin the wiring)."""
    sql = WorkflowSql.build("taskq").workflow_nodes
    assert "j.started_at" in sql
    assert "failed_parent" in sql
    assert "j.lock_expires_at" in sql
    assert "j.last_heartbeat_at" in sql
