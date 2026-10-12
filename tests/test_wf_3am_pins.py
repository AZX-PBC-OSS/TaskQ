"""THE 3AM DRILL PINS — the operator-truth faces fenced (the
observability lane's evidence, red-first).

Each pin is a drill gap's fence: the surface MUST say X, where X is the
truth an operator needs at 3am with ONLY the documented surfaces (the
CLI, the explorer, the rows). Collected against the drilled failures:

1. THE STUCK RUN — a healthy join-wait row must get the HONEST join-wait
   arm (the join fires when its parents finalize), never the
   retry-the-failed-upstream remedy (a false remedy the blocking-reason
   arm's order was printing for reason='join'; the drill's planted hang
   showed the face). And a RUNNING node's age must be NAMED (stuck vs
   slow is the 3am question; a silent running row is indistinguishable
   from a dead one).
2. THE WEDGED FAN-IN — the failed_parent stamp carries the dead parent's
   id (metadata.failed_parent) and the flow_dead stamp is the fence's
   record: the lines must NAME both (the parent's id; the dead path and
   its cancel remedy), never a bare retry line that reads as if a retry
   revives a dead path.
3. THE LOST WORKER — ``job show`` must render the lease face
   (locked_by_worker / last_heartbeat_at / lock_expires_at) and, past
   the lease, the LAPSED verdict (the worker died; the reclaim sweep
   repends within the sweep interval once a leader is up) — the
   distinction "the worker died" vs "the work is slow" must not require
   raw SQL.
4. THE POISON/AUDIT — the cross-run cache hit's receipt
   (metadata.wf_cache_hit: the address + the producing run + the stored
   instant) must be visible on ``job show`` and on the explorer's node
   panel: a step that never executed must not read like one that did.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.cli import app
from taskq.testing.assertions import plain_cli_output
from taskq.workflows import Promise
from taskq.workflows._cli import FlowNodeRow, format_flow_status, stuck_lines

runner = CliRunner()

_NOW = datetime(2026, 10, 12, 17, 0, 0, tzinfo=UTC)


# ── 1. the healthy join-wait gets the honest arm ────────────────────────


def test_healthy_join_wait_gets_the_join_wait_arm_not_the_retry_remedy() -> None:
    """A join-wait row whose blocking_reason is the bare 'join' stamp (the
    HEALTHY shape — every emitted join carries it) must read as a join
    that fires when its parents finalize. The convicted variant: the
    blocking-reason arm caught reason='join' first and printed
    'remedy: retry the failed upstream node' — a false remedy for a
    healthy join (no upstream has failed; the retry command names no
    failed node)."""
    row = FlowNodeRow(step_key="collect", status="pending", deps_pending=1, blocking_reason="join")
    lines = stuck_lines(row, "run-1")
    joined = "\n".join(lines)
    assert "JOIN-WAIT" in joined
    assert "the join fires when its parents finalize" in joined
    assert "retry the failed upstream node" not in joined


# ── 2. the wedged fan-in names its dead parent / its dead path ──────────


def test_a_crashed_terminal_node_renders_its_story() -> None:
    """The terminal-crash fold's face: a crashed node (the lost worker's
    row — the reclaim's budget-exhausted death, error_class
    WorkerCrashed) must render the why-stuck line (class + message +
    remedy), because the derivation folds it into the failed class and
    the run reads '1 crashed' with no explanation otherwise. The
    convicted variant: the crashed row produced NO line — the operator
    saw the count and nothing else."""
    row = FlowNodeRow(
        step_key="hang",
        status="crashed",
        error_class="WorkerCrashed",
        error_message="lock expired before worker reported terminal state",
        max_attempts=2,
        attempt=2,
        job_id="018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f",
    )
    lines = stuck_lines(row, "run-1")
    joined = "\n".join(lines)
    assert "WorkerCrashed" in joined
    assert "taskq flows retry run-1 hang" in joined


def test_failed_parent_line_names_the_dead_parent() -> None:
    """The failed_parent stamp NAMES the failed parent on the record
    (metadata.failed_parent, the T06 cascade's own write) — the why-stuck
    line must print that id, not send the operator hunting through the
    run's other nodes to discover which parent died."""
    parent_id = "01a130ef-813b-7f50-86b2-14d8c01a61e5"
    row = FlowNodeRow(
        step_key="collect",
        status="pending",
        deps_pending=1,
        blocking_reason="failed_parent",
        failed_parent=parent_id,
    )
    lines = stuck_lines(row, "run-1")
    joined = "\n".join(lines)
    assert parent_id in joined


def test_flow_dead_names_the_fence_and_the_dead_path() -> None:
    """flow_dead is the fence's record — the flow died and this join can
    never fire. The line must say the path is DEAD and name the cancel
    remedy beside the retry (a bare 'retry the failed upstream node'
    reads as if a retry revives a dead path)."""
    row = FlowNodeRow(
        step_key="collect", status="pending", deps_pending=1, blocking_reason="flow_dead"
    )
    lines = stuck_lines(row, "run-1")
    joined = "\n".join(lines)
    assert "flow_dead" in joined
    assert "cancel" in joined


# ── 3. the running face: stuck vs slow, at a glance ────────────────────


def test_running_node_age_is_named_with_the_lease_check_remedy() -> None:
    """A running node must have its running AGE named on the status
    report (since started_at, source-named) with the lease-check remedy
    (job show's lease face + job events' lock_expired record). The
    convicted variant: a hung body read as a bare '1 running' count —
    the operator could not name the hung node or distinguish stuck from
    slow on ANY surface."""
    started = _NOW - timedelta(minutes=5, seconds=31)
    running = FlowNodeRow(
        step_key="hang",
        status="running",
        started_at=started,
        job_id="018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f",
    )
    lines = format_flow_status(
        run_id="run-1",
        workflow="drill_hang",
        root_status="running",
        nodes=[running],
        now=_NOW,
    )
    joined = "\n".join(lines)
    assert "RUNNING for 00:05:31" in joined
    assert "hang" in joined
    assert "taskq job show 018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f" in joined


def test_running_face_is_silent_for_a_fresh_row() -> None:
    """A row that started moments ago is live work, not a finding: the
    running face names no age line for it (the age IS the finding's
    substance — a fresh row's line would be noise on every progress
    report)."""
    fresh = FlowNodeRow(
        step_key="quick",
        status="running",
        started_at=_NOW - timedelta(seconds=2),
        job_id="018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f",
    )
    lines = format_flow_status(
        run_id="run-1",
        workflow="w",
        root_status="running",
        nodes=[fresh],
        now=_NOW,
        running_face_after_s=60,
    )
    assert not any("RUNNING for" in line for line in lines)


# ── 4. job show: the lease face + the cache receipt ────────────────────


_JOB_ID = "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"


def _job_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": _JOB_ID,
        "actor": "wf-drill",
        "queue": "drill",
        "status": "running",
        "priority": 0,
        "attempt": 1,
        "max_attempts": 2,
        "retry_kind": "transient",
        "created_at": "2026-01-01 00:00:00+00:00",
        "scheduled_at": "2026-01-01 00:00:00+00:00",
        "started_at": "2026-01-01 00:00:01+00:00",
        "finished_at": None,
        "error_class": None,
        "error_message": None,
        "idempotency_key": None,
        "locked_by_worker": None,
        "lock_expires_at": None,
        "last_heartbeat_at": None,
        "metadata": {},
    }
    row.update(overrides)
    return row


def _patch_db(monkeypatch: pytest.MonkeyPatch, row: dict[str, Any] | None) -> None:
    class _FakeConn:
        async def fetchrow(self, sql: str, *args: object) -> Any:
            if "jobs_archive" in sql:
                return None
            return row

        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setenv("TASKQ_PG_DSN", "postgresql://postgres:taskq@localhost:5836/taskq")
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "taskq")


def test_job_show_names_the_lapsed_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    """A running row whose lock_expires_at has passed: job show must say
    the lease LAPSED and what happens next (the reclaim sweep repends it
    within the sweep interval once a leader is up) — 'the worker died',
    not 'the work is slow'. The convicted variant: the lease columns were
    invisible on the CLI; the operator needed raw SQL for the
    distinction."""
    _patch_db(
        monkeypatch,
        _job_row(
            locked_by_worker=uuid.UUID(int=1),
            last_heartbeat_at="2026-01-01 00:00:30+00:00",
            lock_expires_at="2026-01-01 00:01:30+00:00",
        ),
    )
    result = runner.invoke(app, ["job", "show", _JOB_ID])
    out = plain_cli_output(result.output)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "lock_expires_at: 2026-01-01 00:01:30" in out
    assert "LAPSED" in out
    assert "reclaim" in out


def test_job_show_names_a_fresh_lease_as_held(monkeypatch: pytest.MonkeyPatch) -> None:
    """A running row with a LIVE lease reads as held — the same face,
    the opposite verdict (the distinction is the point)."""
    _patch_db(
        monkeypatch,
        _job_row(
            locked_by_worker=uuid.UUID(int=1),
            last_heartbeat_at="2999-01-01 00:00:30+00:00",
            lock_expires_at="2999-01-01 00:01:30+00:00",
        ),
    )
    result = runner.invoke(app, ["job", "show", _JOB_ID])
    out = plain_cli_output(result.output)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "LAPSED" not in out
    assert "lock_expires_at: 2999-01-01 00:01:30" in out


def test_job_show_renders_the_cache_hit_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    """The cross-run cache hit's receipt (metadata.wf_cache_hit) must be
    visible on job show: a step that never executed must not read like
    one that did (the audit drill's provenance question)."""
    _patch_db(
        monkeypatch,
        _job_row(
            status="succeeded",
            finished_at="2026-01-01 00:00:02+00:00",
            metadata={
                "wf_cache_hit": {
                    "address": "57fe1e01",
                    "run_id": "01a130f9-f20a-73a3-bb37-d5eac4d34b32",
                    "stored_at": "2026-10-12T16:36:28.241707+00:00",
                }
            },
        ),
    )
    result = runner.invoke(app, ["job", "show", _JOB_ID])
    out = plain_cli_output(result.output)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "cache_hit:" in out
    assert "01a130f9-f20a-73a3-bb37-d5eac4d34b32" in out
    assert "57fe1e01" in out


def test_job_show_omits_the_lease_face_for_terminal_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A terminal row's lease fields are history, not liveness: the face
    belongs to non-terminal rows (a succeeded row printing 'LAPSED' would
    be a lie — the lease lapsed because the work FINISHED)."""
    _patch_db(
        monkeypatch,
        _job_row(
            status="succeeded",
            finished_at="2026-01-01 00:00:02+00:00",
            locked_by_worker=uuid.UUID(int=1),
            lock_expires_at="2026-01-01 00:00:00+00:00",
        ),
    )
    result = runner.invoke(app, ["job", "show", _JOB_ID])
    out = plain_cli_output(result.output)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "LAPSED" not in out


# ── 5. the explorer's node panel carries the cache receipt (live) ───────


class _CachePanelOne(BaseModel):
    """The panel probe's step payload — MODULE scope: the runner's typed
    decode resolves model classes by their module + qualname, and a
    function-local class is unresolvable (the probe's lesson)."""

    v: int = 1


async def _cache_panel_body(ctx: Any, params: _CachePanelOne) -> str:
    return f"ran:{params.v}"


async def _cache_panel_sink(ctx: Any, s: str) -> str:
    return s


@pytest.mark.integration
async def test_node_panel_renders_the_cache_hit_receipt(
    module_pg_pool: Any,
    module_pg_schema: Any,
    clean_pg_conn: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The provenance drill's explorer face: a run over the SAME input as
    an earlier run reads the cross-run cache — its ``cached`` node NEVER
    executed, and the panel must say so (the receipt: the producing run +
    the address). The convicted variant: the panel had no cache_hit key —
    a cache-hit step read exactly like an executed one."""
    import httpx

    from taskq.web.admin import create_router, setup_admin_state
    from taskq.workflows import FlowRunner, WorkflowApp, build, step

    # The unauthenticated admin router needs the dev label (the
    # fail-closed auth gate's carve-out — the live-run pins' convention
    # in tests/web_admin rides the same label via its conftest).
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "development")

    app_obj = WorkflowApp()

    @app_obj.workflow("cache_panel_flow")
    def cache_flow() -> Promise[object]:
        solo = step(_cache_panel_body, _CachePanelOne(), key="cached", cache=True)
        return build(step(_cache_panel_sink, solo, key="sink"))

    schema = module_pg_schema.schema_name
    runner = FlowRunner(app_obj.get("cache_panel_flow"), module_pg_pool, schema)
    first = (await runner.create_flow()).flow_id
    assert await runner.drive(first) == "terminal"
    second = (await runner.create_flow()).flow_id
    assert await runner.drive(second) == "terminal"

    bundle = create_router(module_pg_pool, schema=schema, base_path="", workflow_app=None)
    fastapp = __import__("fastapi").FastAPI()
    setup_admin_state(fastapp, bundle)
    fastapp.include_router(bundle.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=fastapp), base_url="http://test"
    ) as client:
        hit = await client.get(f"/api/runs/{second}/nodes/cached")
        assert hit.status_code == 200
        hit_body = hit.json()
        receipt = hit_body.get("cache_hit")
        assert receipt, "a cache-hit node must carry its receipt on the panel"
        assert receipt["run_id"] == str(first)
        ran = await client.get(f"/api/runs/{first!s}/nodes/cached")
        assert ran.status_code == 200
        assert ran.json().get("cache_hit") is None, "an EXECUTED step carries no receipt"
