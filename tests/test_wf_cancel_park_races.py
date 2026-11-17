"""The contract §9 control 12: cancellation races, terminal/failed roots,
and sibling claim-exactness. The claim-exact sibling `ctx.substep`
protection: a cancelled or failed job must never fence or rewrite a
SIBLING's ledger row (the cancelled job's own current claim is resolved by
its own id + step_key + null-safe map_index + attempt + epoch — never a
job-wide or flow-wide match). Real PostgreSQL; the real cancel path."""

from __future__ import annotations

import json as _json
from typing import Any

import pytest

from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


def new_uuid_str() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


async def _seed_flow_root(conn: Any, schema: str) -> JobId:
    flow = JobId(new_uuid_str())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, metadata, fairness_key) "
        "VALUES ($1, 'c12', 'c12', '{}', 'running', 3, 'transient', '__flow__', "
        "$2::jsonb, $3)",
        flow,
        _json.dumps({"flow_id": str(flow), "workflow": "c12_flow"}),
        str(flow),
    )
    return flow


async def _seed_node(
    conn: Any, schema: str, flow: JobId, step_key: str, *, status: str = "running"
) -> JobId:
    job = JobId(new_uuid_str())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, metadata, fairness_key, attempt, "
        "locked_by_worker, lock_expires_at, claim_epoch) "
        "VALUES ($1, 'c12', 'c12', '{}', $2, 3, 'transient', $3, "
        "$4::jsonb, $5, 1, $1, now() + interval '90 seconds', 1)",
        job,
        status,
        step_key,
        _json.dumps({"flow_id": str(flow)}),
        str(flow),
    )
    return job


async def test_cancel_before_park_and_after_park_never_reenters_and_spares_siblings(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """Cancel immediately before and after the park commit: the node
    terminalizes, its exact ledger claim fences, no later event or due time
    re-enters it, and the SIBLING ctx.substep ledger rows are never
    cross-written. A failed/cancelled root never wakes its parked
    work."""
    from taskq.workflows.api._sql_runner import (
        CANCEL_NODES_SQL_TEMPLATE,
        NODE_CLAIM_SQL_TEMPLATE,
        render_sql,
    )
    from taskq.workflows.engine import finalize_node
    from taskq.workflows.ledger import claim_step_ledger

    # The flow: the parked source + a claimed sibling with TWO ledger rows
    # sharing the job (the ctx.substep shape: same job id, different step
    # keys) + the sibling's own row.
    flow = await _seed_flow_root(wf_conn, wf_schema)
    source = await _seed_node(wf_conn, wf_schema, flow, "source", status="scheduled")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb, '
        "attempt = 0, locked_by_worker = NULL, lock_expires_at = NULL, "
        "last_heartbeat_at = NULL WHERE id = $1",
        source,
        _json.dumps(
            {
                "blocking_reason": "backpressure",
                "emit_cursor": {"page": 0},
                "wf_deferral": {
                    "signal": "EmitBackpressure",
                    "consume_budget": False,
                    "attempt": 0,
                },
                "wf_emit_backpressure": {"version": 1, "page_width": 2, "bound": 2},
                "wf_source_logical_attempt": 1,
            }
        ),
    )
    sibling = await _seed_node(wf_conn, wf_schema, flow, "sibling")
    sub_job = sibling  # the ctx.substep shape: one job, two step keys

    async with wf_pool.acquire() as conn:
        sibling_claim = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=sub_job,
            step_key="sibling",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
        sub_claim = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=sub_job,
            step_key="sibling.sub",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
    assert sibling_claim.ledger_id is not None and sub_claim.ledger_id is not None

    # CANCEL BEFORE the park's own resolution: the operator's flip marks
    # the cooperative phase (the root's own terminal follows its
    # cooperative unwind) and TERMINALIZES the parked row.
    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow)
    root = await wf_conn.fetchrow(
        f"SELECT status::text AS status, metadata->>'cancel_phase' AS phase "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        flow,
    )
    assert root is not None and root["status"] in ("running", "cancelled"), root
    assert root["phase"] == "cooperative", root

    # The sibling's OWN ledger rows: the sibling's claim stays 'running'
    # (its job's cancel is the row's own resolution — the substep's row is
    # never rewritten by the sibling's terminal, and neither is fenced by
    # the OTHER's identity).
    rows = await wf_conn.fetch(
        f'SELECT step_key, status::text AS status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 ORDER BY step_key",
        flow,
    )
    by_key = {r["step_key"]: r["status"] for r in rows}
    assert by_key["sibling"] == "running", by_key
    assert by_key["sibling.sub"] == "running", by_key

    # The parked source: the cancel terminalized it (the exact current
    # claim fenced with the cancel's vocabulary) — and it never re-enters:
    # a later claim attempt and the promotion both refuse.
    source_row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, attempt::int AS attempt, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        source,
    )
    assert source_row is not None
    assert source_row["status"] == "cancelled", source_row
    claim = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), source, JobId(new_uuid_str())
    )
    assert claim is None, "a cancelled parked row is unclaimable"
    # The failed/cancelled root never wakes its parked work: the drain
    # sees real rows — the source's own row is terminal cancelled (no park
    # to promote), so the intent completes without a promotion.
    from taskq.workflows._emit import ensure_emit_capacity, wake_emit_backpressure

    async with wf_pool.acquire() as conn:
        await ensure_emit_capacity(conn, wf_sql, flow_id=flow, bound=2)
    result = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow)
    assert result.promoted == 0, result

    # The substep's zero-cross-write: a terminal on the SIBLING's claim
    # never touches the substep's row (the exact-claim write).
    await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow,
        job_id=sub_job,
        step_key="sibling",
        worker_id=sub_job,
        attempt=1,
        claim_epoch=1,
        outcome="failed",
        error_class="RuntimeError",
        ledger_id=sibling_claim.ledger_id,
        ledger_claimed_at=sibling_claim.claimed_at,
    )
    rows = await wf_conn.fetch(
        f'SELECT step_key, status::text AS status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 ORDER BY step_key",
        flow,
    )
    by_key = {r["step_key"]: r["status"] for r in rows}
    assert by_key["sibling"] == "failed", by_key
    assert by_key["sibling.sub"] == "running", (
        f"the sibling's terminal never rewrites the substep's own claim row: {by_key}"
    )


async def test_park_and_cancel_race_resolves_by_row_state(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """Cancel racing the park: whichever wins, the row state is the truth —
    the park's fenced arm answers cancel_fenced and the runner terminalizes;
    the cancel's flip on a parked row fences its exact awaited claim and
    the row disappears from every derived predicate."""
    from datetime import timedelta

    from taskq.workflows._emit import EmitBackpressure, observe_emit_capacity
    from taskq.workflows.api._runner_defer import defer_row_law
    from taskq.workflows.api._sql_runner import render_sql
    from taskq.workflows.ledger import claim_step_ledger

    flow = await _seed_flow_root(wf_conn, wf_schema)
    source = await _seed_node(wf_conn, wf_schema, flow, "source")
    async with wf_pool.acquire() as conn:
        claim = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=source,
            step_key="source",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
    assert claim.ledger_id is not None

    outcome = await defer_row_law(
        wf_pool,
        wf_sql,
        job_id=source,
        worker_id=source,
        delay=timedelta(0),
        attempt=1,
        claim_epoch=1,
        disposition="emit_backpressure",
        signal_name="EmitBackpressure",
        ledger_id=claim.ledger_id,
        ledger_claimed_at=claim.claimed_at,
        backpressure=EmitBackpressure(page_width=2, bound=2, outstanding=1),
        source_logical_attempt=1,
        flow_id=flow,
    )
    assert outcome.verdict == "deferred", outcome

    observation = await observe_emit_capacity(
        wf_pool.acquire() if False else _Conn(wf_pool), wf_sql, flow_id=flow
    )
    assert observation.min_waiting_width == 2, observation

    # The cancel lands on the parked row: the exact awaited claim fences.
    from taskq.workflows.api._sql_runner import CANCEL_NODES_SQL_TEMPLATE

    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow)
    source_row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, attempt::int AS attempt FROM "{wf_schema}".jobs WHERE id = $1',
        source,
    )
    assert source_row is not None and source_row["status"] == "cancelled", source_row
    assert int(source_row["attempt"]) == 0, "the parked refund stands through the cancel"
    ledger_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    assert ledger_status == "fenced", ledger_status
    observation = await observe_emit_capacity(_Conn(wf_pool), wf_sql, flow_id=flow)
    assert observation.live_rows == 0 and observation.reservations == 0, observation
    assert not observation.waiter_present


class _Conn:
    """The observation's minimal connection view over the pool."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)

    async def fetchrow(self, *args: Any) -> Any:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(*args)


# ── R4/A9.4: the claim-exact operator cancel + the truthful old state ────


async def _seed_nonrunning_target_with_decoys(
    conn: Any, schema: str, flow: JobId, *, status: str, parked: bool
) -> tuple[JobId, str, str, str, str]:
    """One non-running target job + its exact ledger estate: the outer
    current claim (epoch 2, the ordinary physical attempt), the
    ``ctx.substep`` decoy (the SAME job/epoch/attempt/map, a distinct
    step_key), the same-key/different-map decoy, and the older-epoch
    decoy. Returns (job, outer, substep, map_sibling, older)."""
    job = JobId(new_uuid_str())
    meta: dict[str, Any] = {"flow_id": str(flow)}
    if parked:
        meta.update(
            {
                "blocking_reason": "backpressure",
                "wf_deferral": {
                    "signal": "EmitBackpressure",
                    "consume_budget": False,
                    "attempt": 0,
                },
                "wf_emit_backpressure": {
                    "version": 1,
                    "page_width": 2,
                    "bound": 2,
                    "outstanding": 1,
                    "deferrals": 1,
                    "wakes": 0,
                },
                "wf_source_logical_attempt": 1,
            }
        )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, claim_epoch, scheduled_at, "
        "created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'c12', 'c12', '{}', $2, $3, 3, 'transient', 'target', "
        "$4::jsonb, 2, clock_timestamp(), clock_timestamp(), $5, 'workflow-run', $6)",
        job,
        status,
        0 if parked else 1,
        _json.dumps(meta),
        str(flow),
        f"wf:{flow}:target",
    )

    async def _ledger(step_key: str, map_index: int | None, attempt: int, epoch: int) -> str:
        ledger_id = new_uuid_str()
        await conn.execute(
            f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
            "map_index, attempt, claim_epoch, status, created_at, updated_at) "
            "VALUES ($1, $2, $3, $4, $5::smallint, $6::int, $7::bigint, 'awaited', "
            "clock_timestamp(), clock_timestamp())",
            ledger_id,
            flow,
            job,
            step_key,
            map_index,
            attempt,
            epoch,
        )
        return ledger_id

    outer = await _ledger("target", None, 1, 2)
    substep = await _ledger("target.sub", None, 1, 2)
    map_sibling = await _ledger("target", 0, 1, 2)
    older = await _ledger("target", None, 1, 1)
    return job, outer, substep, map_sibling, older


@pytest.mark.parametrize(
    ("status", "parked"), [("pending", False), ("scheduled", True), ("scheduled", False)]
)
async def test_operator_cancel_of_nonrunning_targets_is_claim_exact(
    wf_conn: Any, wf_schema: str, status: str, parked: bool
) -> None:
    """R4's control-12 extension: the non-running target's cancel fences
    ONLY its own outer current-epoch claim — the ``ctx.substep`` row, the
    map sibling, and the older epoch keep their prior status, class, and
    instant; the event records the target's ACTUAL pre-update state and
    the additive details. On the pre-fix shape the incomplete join fenced
    the substep decoy (the parked variant) or resolved zero rows (the
    ordinary attempt)."""
    from taskq.workflows.api._sql_runner import CANCEL_NODES_SQL_TEMPLATE, render_sql

    flow = await _seed_flow_root(wf_conn, wf_schema)
    job, outer, substep, map_sibling, older = await _seed_nonrunning_target_with_decoys(
        wf_conn, wf_schema, flow, status=status, parked=parked
    )
    before = await wf_conn.fetch(
        f'SELECT id, updated_at FROM "{wf_schema}".wf_step_ledger WHERE id = ANY($1::uuid[])',
        [substep, map_sibling, older],
    )
    before_stamps = {str(r["id"]): str(r["updated_at"]) for r in before}

    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow)

    row = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class, metadata FROM "{wf_schema}".jobs
            WHERE id = $1""",
        job,
    )
    assert row is not None
    assert row["status"] == "cancelled", dict(row)
    assert row["error_class"] == "WorkflowCancelled", dict(row)
    outer_row = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class FROM "{wf_schema}".wf_step_ledger
            WHERE id = $1""",
        outer,
    )
    assert outer_row is not None
    assert outer_row["status"] == "fenced", (parked, status, dict(outer_row))
    assert outer_row["error_class"] == "WorkflowCancelled", dict(outer_row)
    for decoy in (substep, map_sibling, older):
        d = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, error_class, updated_at
                FROM "{wf_schema}".wf_step_ledger WHERE id = $1""",
            decoy,
        )
        assert d is not None
        assert d["status"] == "awaited", (
            f"the fence never rewrites a decoy claim (parked={parked}, status={status}): {dict(d)}"
        )
        assert d["error_class"] is None, dict(d)
        assert str(d["updated_at"]) == before_stamps[str(decoy)], dict(d)
    # THE TRUTHFUL EVENT: the captured pre-update state + the additive
    # details (the parked variant's released width is its validated 2; the
    # others omit the field through jsonb_strip_nulls).
    events = await wf_conn.fetch(
        f"""SELECT detail FROM "{wf_schema}".job_events
            WHERE job_id = $1 AND kind = 'state_change'""",
        job,
    )
    assert len(events) == 1, [dict(r) for r in events]
    detail = events[0]["detail"]
    detail = _json.loads(detail) if isinstance(detail, str) else dict(detail)
    assert detail["from_state"] == status, detail
    assert detail["to_state"] == "cancelled", detail
    assert detail["error_class"] == "WorkflowCancelled", detail
    assert int(detail["claim_attempt"]) == 1, detail
    # These targets carry NO reservation (the width-present direction is
    # the peer control's own parked peer): the absent field is omitted
    # through jsonb_strip_nulls.
    assert "released_emit_reservation_width" not in detail, detail


async def test_operator_cancel_after_refunded_deferrals_fences_the_current_claim(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """The N-1 minimum mainstream matrix: an ordinary Snooze and a legacy
    ``RetryAfter(consume_budget=False)`` each refund the job's displayed
    ordinal (A-1) while the current epoch stands and the ledger's current
    claim rests ``awaited`` at the recorded attempt — the cancel fences
    that EXACT current claim and records the truthful event. On the
    pre-fix shape the attempt-equality leg resolved zero rows and the
    current claim stayed unfenced."""
    from datetime import timedelta

    from taskq.exceptions import RetryAfter, Snooze
    from taskq.workflows.api._runner_defer import defer_row_law
    from taskq.workflows.api._sql_runner import CANCEL_NODES_SQL_TEMPLATE, render_sql
    from taskq.workflows.ledger import claim_step_ledger

    for signal, disposition in (
        (Snooze(timedelta(seconds=5)), "snooze"),
        (RetryAfter(timedelta(seconds=5), consume_budget=False), "snooze"),
    ):
        flow = await _seed_flow_root(wf_conn, wf_schema)
        source = await _seed_node(wf_conn, wf_schema, flow, "source", status="running")
        async with wf_pool.acquire() as conn:
            claim = await claim_step_ledger(
                conn,
                wf_sql,
                flow_id=flow,
                job_id=source,
                step_key="source",
                map_index=None,
                attempt=1,
                claim_epoch=1,
            )
        assert claim.ledger_id is not None
        outcome = await defer_row_law(
            wf_pool,
            wf_sql,
            job_id=source,
            worker_id=source,
            delay=signal.delay,
            attempt=1,
            claim_epoch=1,
            disposition=disposition,
            signal_name=type(signal).__name__,
            ledger_id=claim.ledger_id,
            ledger_claimed_at=claim.claimed_at,
            flow_id=flow,
        )
        assert outcome.verdict == "deferred", (signal, outcome)
        # THE REFUND REPRESENTATION: the displayed ordinal is refunded
        # (A-1 = 0), the epoch unchanged, the current claim awaited at the
        # recorded attempt.
        job_row = await wf_conn.fetchrow(
            f'SELECT attempt::int AS attempt, claim_epoch::bigint AS epoch FROM "{wf_schema}".jobs WHERE id = $1',
            source,
        )
        assert job_row is not None
        assert int(job_row["attempt"]) == 0, (signal, dict(job_row))
        assert int(job_row["epoch"]) == 1, (signal, dict(job_row))
        current = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, attempt::int AS attempt
                FROM "{wf_schema}".wf_step_ledger WHERE id = $1""",
            claim.ledger_id,
        )
        assert current is not None
        assert current["status"] == "awaited" and int(current["attempt"]) == 1, (
            signal,
            dict(current),
        )

        # THE CANCEL: the exact current claim fences (no attempt-equality
        # narrowing), with the truthful from_state ('scheduled' — the
        # deferred row) and the non-narrowing claim_attempt=1 detail.
        await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow)
        fenced = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
            claim.ledger_id,
        )
        assert fenced == "fenced", (signal, fenced)
        event = await wf_conn.fetchrow(
            f"""SELECT detail FROM "{wf_schema}".job_events
                WHERE job_id = $1 AND kind = 'state_change' AND detail->>'to_state' = 'cancelled'""",
            source,
        )
        assert event is not None, signal
        detail = event["detail"]
        detail = _json.loads(detail) if isinstance(detail, str) else dict(detail)
        assert detail["from_state"] == "scheduled", (signal, detail)
        assert int(detail["claim_attempt"]) == 1, (signal, detail)


async def test_deadline_repend_failed_history_is_never_fenced(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """The negative direction: a deadline re-pend's ledger row is terminal
    ``failed`` history at an unchanged attempt — the cancel leaves it
    byte-identical (its status is outside the current nonterminal
    domain)."""

    from taskq.workflows.api._sql_runner import CANCEL_NODES_SQL_TEMPLATE, render_sql

    flow = await _seed_flow_root(wf_conn, wf_schema)
    child = await _seed_node(wf_conn, wf_schema, flow, "screen", status="pending")
    failed_id = new_uuid_str()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "map_index, attempt, claim_epoch, status, error_class, created_at, updated_at) "
        "VALUES ($1, $2, $3, 'screen', NULL, 1, 1, 'failed', 'DeadlineExceeded', "
        "clock_timestamp(), clock_timestamp())",
        failed_id,
        flow,
        child,
    )
    before = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class, updated_at
            FROM "{wf_schema}".wf_step_ledger WHERE id = $1""",
        failed_id,
    )
    assert before is not None
    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow)
    after = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class, updated_at
            FROM "{wf_schema}".wf_step_ledger WHERE id = $1""",
        failed_id,
    )
    assert after is not None
    assert str(after["status"]) == str(before["status"]) == "failed", dict(after)
    assert str(after["updated_at"]) == str(before["updated_at"]), dict(after)


async def test_fail_closed_peer_cancel_is_a_complete_root_ordered_transition(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """F12/R4: the fail-closed peer cancellation is ONE root-ordered
    atomic transition per actual winner — the job terminal, the
    reservation removal, the exact current-claim ledger fence, and ONE
    event with the captured pre-update state and the peer-failure cause.
    Pending, parked, and running peers each carry their exact current
    claim; the never-claimed pending peer records its event with NO
    ledger row fabricated; the substep decoy is untouched. On the
    pre-fix shape: events=0 and the current claims left running/awaited."""
    from taskq.workflows.engine import _resolve_failed_parent, render_workflow_sql
    from taskq.workflows.ledger import claim_step_ledger

    wsql = render_workflow_sql(wf_schema)
    flow = await _seed_flow_root(wf_conn, wf_schema)
    failed_parent = JobId(new_uuid_str())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, error_class, finished_at, metadata, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'c12', 'c12', '{}', 'failed', 1, 1, 'transient', 'screen', "
        "'ValueError', clock_timestamp(), $2::jsonb, $3, 'workflow-run', $4)",
        failed_parent,
        _json.dumps({"flow_id": str(flow)}),
        str(flow),
        f"wf:{flow}:screen",
    )
    join = JobId(new_uuid_str())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, deps_pending, metadata, scheduled_at, "
        "created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'c12', 'c12', '{}', 'pending', 2, 'transient', 'join_step', 2, "
        "$2::jsonb, clock_timestamp() + interval '1 hour', clock_timestamp(), $3, "
        "'workflow-run', $4)",
        join,
        _json.dumps({"flow_id": str(flow), "blocking_reason": "join"}),
        str(flow),
        f"wf:{flow}:join",
    )
    for parent in (failed_parent,):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, '
            "failure_policy) VALUES ($1, $2, $3, 'fail_closed')",
            join,
            parent,
            flow,
        )

    async def _peer(step_key: str, status: str, *, parked: bool, claimed: bool) -> JobId:
        peer = JobId(new_uuid_str())
        meta: dict[str, Any] = {"flow_id": str(flow)}
        if parked:
            meta.update(
                {
                    "blocking_reason": "backpressure",
                    "wf_deferral": {
                        "signal": "EmitBackpressure",
                        "consume_budget": False,
                        "attempt": 0,
                    },
                    "wf_emit_backpressure": {
                        "version": 1,
                        "page_width": 2,
                        "bound": 4,
                        "outstanding": 1,
                        "deferrals": 1,
                        "wakes": 0,
                    },
                    "wf_source_logical_attempt": 1,
                }
            )
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, '
            "attempt, max_attempts, retry_kind, step_key, metadata, claim_epoch, "
            "scheduled_at, created_at, fairness_key, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'c12', 'c12', '{}', $2, $3, 3, 'transient', $4, $5::jsonb, "
            "$6, clock_timestamp(), clock_timestamp(), $7, 'workflow-run', $8)",
            peer,
            status,
            0 if parked else 1,
            step_key,
            _json.dumps(meta),
            1 if claimed else 0,
            str(flow),
            f"wf:{flow}:{step_key}",
        )
        # THE JOIN'S OWN EDGE: every peer is the blocked join's other
        # parent (the peers arm's own winner set).
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, '
            "failure_policy) VALUES ($1, $2, $3, 'fail_closed')",
            join,
            peer,
            flow,
        )
        return peer

    pending_peer = await _peer("peer_pending", "pending", parked=False, claimed=False)
    parked_peer = await _peer("peer_parked", "scheduled", parked=True, claimed=True)
    running_peer = await _peer("peer_running", "running", parked=False, claimed=True)
    # THE RESERVED PEER (the width-detail's own subject): a PROMOTED
    # pending row carrying its page-width reservation (a park never
    # carries one — §2.3's own law — so the promoted shape is where the
    # released-width capture applies).
    reserved_peer = await _peer("peer_reserved", "pending", parked=False, claimed=True)
    await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb
            WHERE id = $1""",
        reserved_peer,
        _json.dumps({"wf_emit_reservation": {"version": 1, "width": 2}}),
    )
    # The running peer's exact current claim + THE SUBSTEP DECOY (the SAME
    # job id, epoch, attempt, and map — a distinct step_key: the rows the
    # pre-fix incomplete join cross-wrote) + the parked and reserved
    # peers' awaited claims.
    running_claim = None
    async with wf_pool.acquire() as conn:
        rc = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=running_peer,
            step_key="peer_running",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
        running_claim = rc
        await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=running_peer,
            step_key="peer_running.sub",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
        for job, step in ((parked_peer, "peer_parked"), (reserved_peer, "peer_reserved")):
            await conn.execute(
                f'INSERT INTO "{wf_schema}".wf_step_ledger (id, flow_id, job_id, '
                "step_key, map_index, attempt, claim_epoch, status, created_at, "
                "updated_at) VALUES ($1, $2, $3, $4, NULL, 1, 1, 'awaited', "
                "clock_timestamp(), clock_timestamp())",
                new_uuid_str(),
                flow,
                job,
                step,
            )
    assert running_claim is not None and running_claim.ledger_id is not None

    # THE DERIVED CAPACITY before the transition (each terminal moves it
    # exactly once): the pending + running + reserved peers are live rows
    # (the reserved peer's width in reservations); the parked peer is a
    # park (excluded from live).
    from taskq.workflows._emit import observe_emit_capacity

    before_obs = await observe_emit_capacity(_Conn(wf_pool), wsql, flow_id=flow)

    tx = wf_conn.transaction()
    await tx.start()
    try:
        await _resolve_failed_parent(
            wf_conn,
            wsql,
            flow_id=flow,
            parent_id=failed_parent,
            step_key="screen",
            map_index=None,
            error_class="ValueError",
            error_message="exhausted",
            error_traceback=None,
        )
        await tx.commit()
    except BaseException:
        await tx.rollback()
        raise

    for peer, expected_from in (
        (pending_peer, "pending"),
        (parked_peer, "scheduled"),
        (running_peer, "running"),
        (reserved_peer, "pending"),
    ):
        row = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, error_class, metadata FROM "{wf_schema}".jobs
                WHERE id = $1""",
            peer,
        )
        assert row is not None
        assert row["status"] == "cancelled", (str(peer), dict(row))
        assert row["error_class"] == "CancelledByPeerFailure", dict(row)
        meta = (
            _json.loads(row["metadata"])
            if isinstance(row["metadata"], str)
            else dict(row["metadata"])
        )
        assert meta.get("peer_cancel", {}).get("by") == "peer_failure", meta
        assert "wf_emit_reservation" not in meta, meta
        events = await wf_conn.fetch(
            f"""SELECT detail FROM "{wf_schema}".job_events
                WHERE job_id = $1 AND kind = 'state_change'""",
            peer,
        )
        assert len(events) == 1, (str(peer), [dict(r) for r in events])
        detail = events[0]["detail"]
        detail = _json.loads(detail) if isinstance(detail, str) else dict(detail)
        assert detail["from_state"] == expected_from, (str(peer), detail)
        assert detail["to_state"] == "cancelled", detail
        assert detail["error_class"] == "CancelledByPeerFailure", detail
        assert detail.get("peer_cancel", {}).get("cascade_from") == str(failed_parent), detail
        if peer is pending_peer:
            # THE NEVER-CLAIMED PEER: no current claim exists, so the
            # optional details are omitted through jsonb_strip_nulls.
            assert "claim_attempt" not in detail, detail
            assert "released_emit_reservation_width" not in detail, detail
        else:
            assert int(detail["claim_attempt"]) == 1, detail
        if peer is reserved_peer:
            # THE WIDTH-PRESENT DIRECTION: the promoted winner's validated
            # old width, captured before the reservation-removing UPDATE.
            assert int(detail["released_emit_reservation_width"]) == 2, detail
        elif peer is parked_peer:
            # A park never carries a reservation (§2.3): the field omits.
            assert "released_emit_reservation_width" not in detail, detail
        else:
            assert "released_emit_reservation_width" not in detail, detail

    # THE EXACT LEDGER FENCES: the running and parked peers' OWN current
    # claims are fenced; the SAME JOB's substep decoy row keeps its prior
    # state (the step_key leg's own direction).
    running_ledger = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        running_claim.ledger_id,
    )
    assert running_ledger == "fenced", running_ledger
    parked_fenced = await wf_conn.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".wf_step_ledger
            WHERE job_id = $1 AND step_key = 'peer_parked' LIMIT 1""",
        parked_peer,
    )
    assert parked_fenced == "fenced", parked_fenced
    reserved_fenced = await wf_conn.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".wf_step_ledger
            WHERE job_id = $1 AND step_key = 'peer_reserved' LIMIT 1""",
        reserved_peer,
    )
    assert reserved_fenced == "fenced", reserved_fenced
    sub_ledger = await wf_conn.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".wf_step_ledger
            WHERE job_id = $1 AND step_key = 'peer_running.sub' LIMIT 1""",
        running_peer,
    )
    assert sub_ledger == "running", (
        f"the same job's substep claim is never the cancel's own fence: {sub_ledger}"
    )

    # THE NEVER-CLAIMED PENDING PEER: no ledger row was fabricated for it.
    never_rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE job_id = $1',
        pending_peer,
    )
    assert int(never_rows) == 0, never_rows

    # THE DERIVED CAPACITY after: the three non-park live peers' rows and
    # the reserved peer's reservation are gone, each moved exactly once;
    # the blocked-stamped join remains the run's own live row.
    after_obs = await observe_emit_capacity(_Conn(wf_pool), wsql, flow_id=flow)
    assert before_obs.live_rows - after_obs.live_rows == 3, (before_obs, after_obs)
    assert before_obs.reservations == 2 and after_obs.reservations == 0, (
        before_obs,
        after_obs,
    )
