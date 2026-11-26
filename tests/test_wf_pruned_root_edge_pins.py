# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""Pins for the fail-closed EDGE of the claim fences' root-existence leg: a
step-key-bearing row is work only while its ``flow_id`` names a PRESENT
``__flow__`` root row.

Two populations changed with the leg's positive form and neither was seeded
by any test in the tree when the leg landed:

* (b) a step-key row whose metadata carries no ``flow_id`` at all — the cast
  yields NULL, so the retired ``NOT EXISTS (... status IN terminal)`` leg
  found no pointee, its negation held, and every claim door admitted the row;
* (c) a row whose ``flow_id`` names a job that is not the run's ``__flow__``
  root — same shape, same admission.

Both are refused by the fleet probe, fleet dispatch, and the in-process claim
CAS now, and the execution door classifies (c) as a missing root rather than
an unstamped body. A red here means the fence fell back to asking a negative
question about a pointee it never proved exists.

The file also pins two adjacent contracts the same edge implies: the
``WorkflowRootMissingError`` seat dispositions (a closed seat set, each seat
describing the transaction it actually owns) and the cross-door fence truth
table over every flow-link population, so a future edit to the root rule
cannot diverge one door from the others silently.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import (
    DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL,
    DISPATCH_CLAIMABLE_PROBE_SQL,
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_CURSOR_SQL,
    DISPATCH_STRICT_FIFO_SQL,
    dispatch_batch,
)
from taskq.backend._protocol import JobId, JobRow
from taskq.constants import WF_LOOP_ESCALATION_STEP_KEY
from taskq.workflows._worker_execution import execute_flow_job
from taskq.workflows.api._runner_errors import RootMissingSeat, WorkflowRootMissingError
from taskq.workflows.api._sql_runner import (
    CLAIMABLE_NODES_SQL_TEMPLATE,
    NODE_CLAIM_SQL_TEMPLATE,
    render_sql,
)

pytestmark = pytest.mark.integration


async def _register_capable_worker(
    conn: asyncpg.Connection, schema: str, *, queues: list[str]
) -> uuid.UUID:
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'edge-host', 1, $2::text[], $3::jsonb)",
        worker_id,
        queues,
        json.dumps({"workflow_execution": True}),
    )
    return worker_id


async def _register_actor(conn: asyncpg.Connection, schema: str, *, actor: str, queue: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        actor,
        queue,
    )


async def _seed_step_row(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    step_key: str,
    metadata: dict[str, object],
    status: str = "pending",
) -> uuid.UUID:
    """A claimable-shaped step row: due an hour ago, no deps, cohort keyed to
    the ``flow_id`` the metadata carries (absent for the no-flow-id case)."""
    job_id = new_uuid()
    flow_id = metadata.get("flow_id")
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata, fairness_key, scheduled_at) "
        "VALUES ($1, $2, $3, '{}', 3, 'transient', $4, $5, 0, $6::jsonb, $7, "
        "now() - interval '1 hour')",
        job_id,
        actor,
        queue,
        status,
        step_key,
        json.dumps(metadata),
        None if flow_id is None else str(flow_id),
    )
    return job_id


async def _probe_rows(conn: asyncpg.Connection, schema: str, queue: str) -> list[object]:
    return list(await conn.fetch(DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=schema), [queue]))


async def _dispatch(
    pool: asyncpg.Pool, schema: str, queue: str, worker_id: uuid.UUID
) -> list[asyncpg.Record]:
    return list(
        await dispatch_batch(
            pool,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
            queues=[queue],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )
    )


async def test_fleet_doors_refuse_a_step_row_carrying_no_flow_id(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A step row whose metadata has no ``flow_id`` names no run, so no claim
    fence can admit it (the negative leg admitted it because the pointee
    subquery found nothing to contradict)."""
    queue = "edge-no-flow-id"
    await _register_actor(wf_conn, wf_schema, actor="edge_actor", queue=queue)
    worker_id = await _register_capable_worker(wf_conn, wf_schema, queues=[queue])
    row = await _seed_step_row(
        wf_conn, wf_schema, actor="edge_actor", queue=queue, step_key="node", metadata={}
    )

    assert await _probe_rows(wf_conn, wf_schema, queue) == [], (
        "the claimable probe admitted a workflow row that names no run root"
    )
    assert await _dispatch(wf_pool, wf_schema, queue, worker_id) == [], (
        f"dispatch claimed workflow row {row}, which names no run root"
    )

    # The positive control: the same row shape, with a live root, is claimed.
    live_root = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', '__flow__', "
        "$2::jsonb, 'workflow-run', $3)",
        live_root,
        json.dumps({"flow_id": str(live_root)}),
        f"flow:{live_root}",
    )
    live_row = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="edge_actor",
        queue=queue,
        step_key="node",
        metadata={"flow_id": str(live_root)},
    )
    assert [r["id"] for r in await _dispatch(wf_pool, wf_schema, queue, worker_id)] == [live_row]


async def test_fleet_doors_refuse_a_row_pointing_at_a_non_root_job(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``flow_id`` that resolves to a job which is not the run's ``__flow__``
    row is not a run root either: the fences require the pointee's KIND, not
    merely that some row answers the id."""
    queue = "edge-non-root-pointee"
    await _register_actor(wf_conn, wf_schema, actor="edge_actor", queue=queue)
    worker_id = await _register_capable_worker(wf_conn, wf_schema, queues=[queue])
    impostor = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="edge_actor",
        queue=queue,
        step_key="impostor",
        metadata={"flow_id": str(new_uuid())},
    )
    child = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="edge_actor",
        queue=queue,
        step_key="node",
        metadata={"flow_id": str(impostor)},
    )

    assert await _probe_rows(wf_conn, wf_schema, queue) == [], (
        "the claimable probe admitted a row whose flow_id names a non-root job"
    )
    assert await _dispatch(wf_pool, wf_schema, queue, worker_id) == [], (
        f"dispatch claimed workflow row {child}, whose flow_id names non-root job {impostor}"
    )


async def test_in_process_claim_cas_refuses_both_rootless_edges(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The in-process claim CAS carries the same positive leg: neither edge
    row is claimable, while a live-root sibling still is."""
    no_flow_id = await _seed_step_row(
        wf_conn, wf_schema, actor="wf", queue="default", step_key="node", metadata={}
    )
    impostor = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="wf",
        queue="default",
        step_key="impostor",
        metadata={"flow_id": str(new_uuid())},
    )
    points_at_non_root = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="wf",
        queue="default",
        step_key="node",
        metadata={"flow_id": str(impostor)},
    )
    live_root = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', '__flow__', "
        "$2::jsonb, 'workflow-run', $3)",
        live_root,
        json.dumps({"flow_id": str(live_root)}),
        f"flow:{live_root}",
    )
    live_row = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="wf",
        queue="default",
        step_key="node",
        metadata={"flow_id": str(live_root)},
    )

    for job_id in (no_flow_id, points_at_non_root):
        claimed = await wf_conn.fetchrow(
            render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), job_id, new_uuid()
        )
        assert claimed is None, (
            f"the in-process claim CAS claimed workflow row {job_id}, which names no run root"
        )
    claimed_live = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), live_row, new_uuid()
    )
    assert claimed_live is not None, "the positive control must be claimable"


async def test_execution_door_classifies_a_non_root_pointee_as_root_missing(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The door's root read is pinned to a real ``__flow__`` row: a pointee
    that exists but is not the root is absent durable state, not an unstamped
    body (the two get different dispositions)."""
    impostor = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="wf",
        queue="default",
        step_key="impostor",
        metadata={"flow_id": str(new_uuid())},
    )
    node = await _seed_step_row(
        wf_conn,
        wf_schema,
        actor="wf",
        queue="default",
        step_key="node",
        metadata={"flow_id": str(impostor)},
        status="running",
    )
    job = JobRow(
        id=JobId(node),
        actor="wf",
        queue="default",
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        metadata={"flow_id": str(impostor)},
        claim_epoch=1,
    )
    with pytest.raises(WorkflowRootMissingError) as exc:
        await execute_flow_job(pool=wf_pool, schema=wf_schema, worker_id=JobId(new_uuid()), job=job)
    assert exc.value.seat == "execution-door"
    assert str(impostor) in str(exc.value)


def test_workflow_root_missing_seat_dispositions() -> None:
    """Each seat describes the transaction it actually owns.

    ``failed-parent`` (the default) and ``park`` raise at their transaction's
    entry and roll it back whole, so they name the rollback. ``execution-door``
    runs after dispatch already admitted and charged the claim, so it names the
    worker's bounded release — never a refused claim it did not refuse.
    """
    transactional = WorkflowRootMissingError("flow-1", "node-1")
    assert transactional.seat == "failed-parent"
    assert "the transaction rolled back without any write" in str(transactional)

    parked = WorkflowRootMissingError("flow-1", "node-1", seat="park")
    assert parked.seat == "park"
    assert "the transaction rolled back without any write" in str(parked)

    door = WorkflowRootMissingError("flow-1", "node-1", seat="execution-door")
    message = str(door)
    assert door.seat == "execution-door"
    assert "the row's claim was already spent and no body ran" in message
    assert "bounded release" in message
    assert "the claim is refused" not in message
    assert "rolled back" not in message


def test_workflow_root_missing_rejects_an_unknown_seat() -> None:
    """An unrecognised seat is a caller bug, refused at construction.

    A free-form ``seat`` with a silent fallback would let the next raise site
    assert a transactional rollback at a seat that owns no transaction — the
    exact false claim the seat split exists to remove.
    """
    unknown = cast("RootMissingSeat", "execution_door")
    with pytest.raises(ValueError, match="unknown WorkflowRootMissingError seat"):
        WorkflowRootMissingError("flow-1", "node-1", seat=unknown)


# ── fence parity: one truth table over every door and flow-link population ──

_PARITY_DOORS = ("fifo", "fcursor", "rr", "enum", "cas")


async def _parity_capable_worker(
    conn: asyncpg.Connection, schema: str, queues: list[str]
) -> uuid.UUID:
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'parity-host', 1, $2::text[], $3::jsonb)",
        worker_id,
        queues,
        json.dumps({"workflow_execution": True}),
    )
    return worker_id


async def _parity_insert_root(conn: asyncpg.Connection, schema: str, *, status: str) -> uuid.UUID:
    root_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, '
        "status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', $2, '__flow__', "
        "$3::jsonb, 'workflow-run', $4)",
        root_id,
        status,
        json.dumps({"flow_id": str(root_id)}),
        f"flow:{root_id}",
    )
    return root_id


async def _parity_insert_row(
    conn: asyncpg.Connection,
    schema: str,
    *,
    step_key: str | None,
    metadata: dict[str, object],
    actor: str,
    queue: str,
    status: str = "pending",
) -> uuid.UUID:
    row_id = new_uuid()
    fairness = metadata.get("flow_id")
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, '
        "status, step_key, deps_pending, metadata, fairness_key, scheduled_at) "
        "VALUES ($1, $2, $3, '{}', 3, 'transient', $4, $5, 0, $6::jsonb, $7, "
        "now() - interval '1 hour')",
        row_id,
        actor,
        queue,
        status,
        step_key,
        json.dumps(metadata),
        None if fairness is None else str(fairness),
    )
    return row_id


async def _parity_door_verdict(
    conn: asyncpg.Connection,
    pool: asyncpg.Pool,
    schema: str,
    *,
    door: str,
    row_id: uuid.UUID,
    flow_key: uuid.UUID | None,
    worker_id: uuid.UUID,
    queue: str,
) -> bool:
    """Whether one door admits one row. Each door reads a row of its own so a
    mutating door (the two claims) cannot change what a later door sees."""
    if door in ("fifo", "fcursor"):
        cursor = uuid.UUID(int=0) if door == "fcursor" else None
        claimed = await dispatch_batch(
            pool,
            sql=(
                DISPATCH_STRICT_FIFO_SQL if cursor is None else DISPATCH_STRICT_FIFO_CURSOR_SQL
            ).format(schema=schema),
            queues=[queue],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
            claim_cursor=cursor,
        )
        return [record["id"] for record in claimed] == [row_id]
    if door == "rr":
        claimed = await dispatch_batch(
            pool,
            sql=DISPATCH_ROUND_ROBIN_SQL.format(schema=schema),
            queues=[queue],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )
        return [record["id"] for record in claimed] == [row_id]
    if door == "enum":
        if flow_key is None:
            # the enumeration is flow-keyed: a row with no usable flow link
            # is not addressable through it at all
            return False
        rows = await conn.fetch(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, schema), flow_key)
        return row_id in [record["id"] for record in rows]
    if door == "cas":
        claimed = await conn.fetchrow(
            render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), row_id, new_uuid()
        )
        return claimed is not None
    raise AssertionError(f"unknown door {door}")


async def _parity_probe_verdicts(
    conn: asyncpg.Connection, schema: str, queue: str
) -> tuple[int, int]:
    plain = await conn.fetch(DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=schema), [queue])
    cursor = await conn.fetch(
        DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL.format(schema=schema),
        [queue],
        uuid.UUID(int=0),
    )
    return len(plain), len(cursor)


async def test_claim_fence_parity_over_the_flow_link_populations(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The four fence sites agree on every flow-link population.

    One truth table over the fleet claim, its cursor twin, round-robin, the
    claimable probes, the in-process enumeration, and the in-process claim CAS.
    The honest populations are all refused; the live-root controls are all
    admitted; the documented variation (the enumeration's node-only
    pre-filter) is the only divergence a vanilla row sees.
    """
    live_root = await _parity_insert_root(wf_conn, wf_schema, status="running")
    terminal_root = await _parity_insert_root(wf_conn, wf_schema, status="failed")
    cancelled_root = await _parity_insert_root(wf_conn, wf_schema, status="cancelled")
    missing = new_uuid()
    non_root_live = await _parity_insert_row(
        wf_conn,
        wf_schema,
        step_key="ordinary_node",
        metadata={"flow_id": str(live_root)},
        actor="pointee",
        queue="default",
        status="running",
    )
    non_root_terminal = await _parity_insert_row(
        wf_conn,
        wf_schema,
        step_key="ordinary_node",
        metadata={"flow_id": str(live_root)},
        actor="pointee",
        queue="default",
        status="failed",
    )

    populations: tuple[tuple[str, str | None, dict[str, object], uuid.UUID | None], ...] = (
        ("vanilla", None, {}, None),
        ("vanilla-stale-flow-id", None, {"flow_id": str(missing)}, missing),
        ("root-row", "__flow__", {"flow_id": str(missing)}, None),
        ("child-live-root", "step", {"flow_id": str(live_root)}, live_root),
        ("child-terminal-root", "step", {"flow_id": str(terminal_root)}, terminal_root),
        ("child-rootless", "step", {"flow_id": str(missing)}, missing),
        (
            "escalation-terminal-root",
            WF_LOOP_ESCALATION_STEP_KEY,
            {"flow_id": str(terminal_root)},
            terminal_root,
        ),
        ("escalation-rootless", WF_LOOP_ESCALATION_STEP_KEY, {"flow_id": str(missing)}, missing),
        (
            "effect-terminal-root",
            "work.__on_exhaustion",
            {"flow_id": str(cancelled_root), "wf_after_terminal_effect": True},
            cancelled_root,
        ),
        (
            "effect-rootless",
            "work.__on_exhaustion",
            {"flow_id": str(missing), "wf_after_terminal_effect": True},
            missing,
        ),
        ("no-flow-id-key", "step", {}, None),
        ("flow-id-to-non-root-live", "step", {"flow_id": str(non_root_live)}, non_root_live),
        (
            "flow-id-to-non-root-terminal",
            "step",
            {"flow_id": str(non_root_terminal)},
            non_root_terminal,
        ),
    )

    worker_id = await _parity_capable_worker(wf_conn, wf_schema, ["unused"])

    rows: list[tuple[str, dict[str, bool], tuple[int, int]]] = []
    for index, (label, step_key, metadata, flow_key) in enumerate(populations):
        verdicts: dict[str, bool] = {}
        for door in _PARITY_DOORS:
            queue = f"{door}-{index}"
            actor = f"{label}-{door}"
            await _register_actor(wf_conn, wf_schema, actor=actor, queue=queue)
            row_id = await _parity_insert_row(
                wf_conn,
                wf_schema,
                step_key=step_key,
                metadata=dict(metadata),
                actor=actor,
                queue=queue,
            )
            verdicts[door] = await _parity_door_verdict(
                wf_conn,
                wf_pool,
                wf_schema,
                door=door,
                row_id=row_id,
                flow_key=flow_key,
                worker_id=worker_id,
                queue=queue,
            )
        probe_queue = f"probe-{index}"
        await _register_actor(wf_conn, wf_schema, actor=f"{label}-probe", queue=probe_queue)
        await _parity_insert_row(
            wf_conn,
            wf_schema,
            step_key=step_key,
            metadata=dict(metadata),
            actor=f"{label}-probe",
            queue=probe_queue,
        )
        probes = await _parity_probe_verdicts(wf_conn, wf_schema, probe_queue)
        rows.append((label, verdicts, probes))

    table = "\n".join(
        f"{label:30s} fifo={int(v['fifo'])} fcursor={int(v['fcursor'])} "
        f"rr={int(v['rr'])} probe={p[0]}/{p[1]} enum={int(v['enum'])} cas={int(v['cas'])}"
        for label, v, p in rows
    )
    by_label = {label: (v, p) for label, v, p in rows}

    for label in (
        "root-row",
        "child-live-root",
        "child-terminal-root",
        "child-rootless",
        "escalation-terminal-root",
        "escalation-rootless",
        "effect-terminal-root",
        "effect-rootless",
        "no-flow-id-key",
        "flow-id-to-non-root-live",
        "flow-id-to-non-root-terminal",
    ):
        verdicts, probes = by_label[label]
        assert verdicts["fifo"] == verdicts["rr"], table
        assert verdicts["fifo"] == verdicts["fcursor"], table
        assert verdicts["fifo"] == verdicts["cas"], table
        assert verdicts["fifo"] == verdicts["enum"], table
        assert probes == (int(verdicts["fifo"]), int(verdicts["fifo"])), table

    for label in (
        "root-row",
        "child-rootless",
        "escalation-rootless",
        "effect-rootless",
        "no-flow-id-key",
        "flow-id-to-non-root-live",
        "flow-id-to-non-root-terminal",
    ):
        verdicts, probes = by_label[label]
        assert not any(verdicts.values()), (label, verdicts)
        assert probes == (0, 0), (label, probes)

    for label in ("child-live-root", "escalation-terminal-root", "effect-terminal-root"):
        verdicts, probes = by_label[label]
        assert all(verdicts.values()), (label, verdicts)
        assert probes == (1, 1), (label, probes)

    for label in ("vanilla", "vanilla-stale-flow-id"):
        verdicts, _probes = by_label[label]
        assert verdicts["fifo"] and verdicts["cas"], (label, verdicts)
        assert not verdicts["enum"], (label, verdicts)


async def test_hostile_flow_links_are_unrepresentable_on_a_step_key_row(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The fences cast ``(metadata->>'flow_id')::uuid`` inline, so one
    uncastable value would poison a whole batched dispatch statement. Two
    schema-level guards make the hostile populations unrepresentable, so no
    reachable row can raise inside a dispatch round: the partial expression
    index refuses an uncastable link at INSERT/UPDATE, and the fairness cohort
    check refuses a JSON-null link."""
    await _register_actor(wf_conn, wf_schema, actor="malformed", queue="mq")
    with pytest.raises(asyncpg.PostgresError) as uncastable:
        await _parity_insert_row(
            wf_conn,
            wf_schema,
            step_key="step",
            metadata={"flow_id": "not-a-uuid"},
            actor="malformed",
            queue="mq",
        )
    assert "invalid input syntax for type uuid" in str(uncastable.value)

    with pytest.raises(asyncpg.CheckViolationError) as json_null:
        await _parity_insert_row(
            wf_conn,
            wf_schema,
            step_key="step",
            metadata={"flow_id": None},
            actor="malformed",
            queue="mq",
        )
    assert "jobs_wf_fairness_cohort_check" in str(json_null.value)
