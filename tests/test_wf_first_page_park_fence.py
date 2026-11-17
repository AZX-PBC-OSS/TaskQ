# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""R1/A9.1: the §5.4 non-drainage fence owns EVERY exact park — a
first-page park (no committed ``emit_cursor``) is resumable work.

The defect this control pins (DeepSeek round-3 F3): the §5.4 fence's
proof required ``metadata ? 'emit_cursor'`` — the fence selected only
cursor-bearing parks. A source whose FIRST page cannot fit parks with
NO cursor (zero pages have committed; the default cursor identifies the
first page). Under a genuinely failed root that park was unclaimable,
unpromotable, unfenceable and unprunable — a durable leak the marker
sweep re-observed forever.

THE RULE (A9.1): an exact park ``P(j)`` represents un-emitted resumable
source work whether or not ``emit_cursor`` is present. Both root-failure
seats (the cascade's Python statement and the maintenance seat's SQL
function) fence every such park in the root-failure transaction, with
the exact current-claim ledger fence (job + current ``claim_epoch`` +
``step_key`` + null-safe ``map_index``, status ``awaited``) — older
epochs, sibling ``ctx.substep`` rows, map siblings, and terminal history
are never rewritten.

Real PostgreSQL; the real runner (the nested-gather shape) drives the
maintenance seat; the seeded estates drive both fence forms directly."""

from __future__ import annotations

import asyncio
import enum
import json as _json
from datetime import timedelta
from typing import Any

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.constants import WORKFLOW_NOT_DRAINING_CLASS
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration

_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


class OkOut:
    """A minimal ok outcome (the enum-typed doors take any object)."""


def _park_meta(flow_id: str, *, width: int, bound: int, cursor: bool) -> dict[str, Any]:
    """The exact park's metadata. ``cursor=False`` is the FIRST-PAGE
    shape: zero pages have committed, the ``emit_cursor`` key is absent —
    the park still represents the whole un-emitted suffix."""
    meta: dict[str, Any] = {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": width,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }
    if cursor:
        meta["emit_cursor"] = {"page": 0}
    return meta


async def _seed_flow_root(conn: Any, schema: str, *, status: str = "failed") -> str:
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, error_class, finished_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r1', 'default', '{}', 3, 'transient', $2, '__flow__', "
        "$3::jsonb, 'UnabsorbedNodeFailure', clock_timestamp(), $4, 'workflow-run', $5)",
        flow_id,
        status,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"flow:{flow_id}",
    )
    return flow_id


async def _seed_parked_source(
    conn: Any, schema: str, flow_id: str, *, cursor: bool, step_key: str = "source"
) -> str:
    source_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, assignment_routed, claim_epoch, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r1', 'default', '{}', 'scheduled', 0, 2, 'transient', $2, "
        "$3::jsonb, clock_timestamp(), clock_timestamp(), $4, true, 2, 'workflow-run', $5)",
        source_id,
        step_key,
        _json.dumps(_park_meta(flow_id, width=2, bound=2, cursor=cursor)),
        flow_id,
        f"wf:{flow_id}:{step_key}",
    )
    return source_id


async def _seed_awaited_ledger(
    conn: Any,
    schema: str,
    flow_id: str,
    job_id: str,
    *,
    step_key: str,
    map_index: int | None,
    attempt: int,
    claim_epoch: int,
) -> str:
    ledger_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "map_index, attempt, claim_epoch, status, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, $5::smallint, $6::int, $7::bigint, 'awaited', "
        "clock_timestamp(), clock_timestamp())",
        ledger_id,
        flow_id,
        job_id,
        step_key,
        map_index,
        attempt,
        claim_epoch,
    )
    return ledger_id


async def test_first_page_no_cursor_park_is_fenced_when_the_root_fails(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql, module_pg_pool: Any
) -> None:
    """The real-runner first-page proof (DeepSeek's nested-gather shape):
    A's FIRST page cannot fit and parks with no ``emit_cursor``; B
    genuinely exhausts its declared failure policy; the root terminal
    ``failed`` commits and the SAME transaction family fences A — the
    exact class/reason, a non-null causal child/root record, one event,
    and A's exact ledger claim ``fenced``. Retention then collects the
    capacity record, and further marker sweeps never re-observe A."""
    from taskq.workflows import DONE, Chain, Route, Step, WorkflowApp, build
    from taskq.workflows.api._graph import chain_source, gather

    class Ok(enum.Enum):
        OK = "ok"

    async def screen_ok(ctx: Any, item: object) -> Ok:
        del ctx, item
        return Ok.OK

    async def screen_fail(ctx: Any, item: object) -> Ok:
        del ctx, item
        raise RuntimeError("the genuine child failure")

    chain_a = Chain(
        name=f"r1-a-{new_uuid().hex}",
        start="screen_a",
        steps={"screen_a": Step(body=screen_ok, outcomes=Ok, route=Route({Ok.OK: DONE}))},
    )
    chain_c = Chain(
        name=f"r1-c-{new_uuid().hex}",
        start="screen_c",
        steps={"screen_c": Step(body=screen_ok, outcomes=Ok, route=Route({Ok.OK: DONE}))},
    )
    # B's own chain: the genuine failure, ONE declared attempt — the
    # exhaustion is real, never staged.
    chain_b = Chain(
        name=f"r1-b-{new_uuid().hex}",
        start="screen_b",
        steps={
            "screen_b": Step(
                body=screen_fail,
                outcomes=Ok,
                route=Route({Ok.OK: DONE}),
            )
        },
    )

    async def source_a(ctx: Any) -> None:
        from taskq.workflows.chain import chain_start

        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1
        # THE FIRST PAGE (W=2) under bound 2: the flow's own live rows
        # (the sibling sources and the gather joins) hold charge >= 2, so
        # 2 + 2 > 2 refuses BEFORE ANY write — no child, no edge, no
        # cursor. The park carries NO emit_cursor: zero pages have
        # committed.
        await ctx.emit_batch(
            [
                chain_start(
                    chain_a,
                    {"app_id": offset},
                    map_index=offset,
                    trace_id=f"r1-a-{offset}",
                )
                for offset in (0, 1)
            ],
            cursor={"page": page},
        )

    async def source_b(ctx: Any) -> None:
        # B's genuine failure: the source body itself raises, and with ONE
        # declared attempt the failure exhausts its declared retry policy
        # — the §5.4 proof's causal child.
        raise RuntimeError("the genuine child failure")

    async def source_c(ctx: Any) -> None:
        del ctx

    name = f"r1_first_page_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=2)
    def r1_flow() -> object:
        a = chain_source(chain_a, source_a, key="source_a", max_attempts=2)
        b = chain_source(chain_b, source_b, key="source_b", max_attempts=1)
        c = chain_source(chain_c, source_c, key="source_c", max_attempts=2)
        # THE NESTED GATHER: B's own fail_closed join (the outer gather's
        # join counts J1 and B as parents) — B's exhaustion cascades to
        # the outer join; A and C are grandparents, never peers, so the
        # peers arm cannot touch the parked A.
        return build(gather([gather([a, c]), b]))

    from taskq.workflows import FlowRunner

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Drive: A's first claim → the first page refuses → A parks with NO
    # cursor. B and C claim and run — B exhausts.
    a_status = ""
    for _ in range(30):
        await runner.tick(flow_id)
        a_row = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, metadata FROM "{wf_schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND step_key = 'source_a'""",
            flow_id,
        )
        assert a_row is not None
        a_status = str(a_row["status"])
        if a_status in ("scheduled", "failed", "succeeded", "cancelled"):
            break
        await asyncio.sleep(0.02)
    # The park phase and the fence can land inside ONE tick (the park's
    # committed state is resolved-blocked, so the same tick's maintenance
    # pass may finalize the root and fence the park immediately). The
    # park phase's own record — the backpressure deferral stamp with
    # attempt 0 — is the premise's proof; the disposition assertions
    # below pin the fence's outcome.
    assert a_status in ("scheduled", "failed"), a_status
    assert a_row is not None
    a_meta = (
        _json.loads(a_row["metadata"]) if isinstance(a_row["metadata"], str) else a_row["metadata"]
    )
    # THE FIRST-PARK SHAPE: the exact marker, NO cursor, attempt 0, the
    # logical ordinal unchanged, and zero child/cursor writes. When the
    # fence already fired inside the same tick the marker keys are gone
    # (the fence's own disposition removed them) — the deferral stamp and
    # the refunded ordinal are the park phase's own proof either way.
    assert "emit_cursor" not in a_meta, a_meta
    assert a_meta.get("wf_emit_backpressure", {}).get("deferrals", 0) >= 1, a_meta
    if a_status == "scheduled":
        assert a_meta.get("blocking_reason") == "backpressure", a_meta
    else:
        assert a_status == "failed", a_status
        assert "wf_not_draining" in a_meta, a_meta
    a_id = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source_a'""",
        flow_id,
    )
    assert a_id is not None
    source_a_id = str(a_id)
    a_full = await wf_conn.fetchrow(
        f"SELECT attempt::int AS attempt, metadata->'wf_source_logical_attempt' AS ord "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        a_id,
    )
    assert a_full is not None
    assert int(a_full["attempt"]) == 0, a_full
    assert int(a_full["ord"]) == 1, a_full
    children = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE parent_id = $1::uuid AND id <> $1::uuid""",
        a_id,
    )
    assert int(children) == 0, "the refused first page wrote zero children"
    assert "emit_cursor" not in a_meta, "no cursor checkpoint rode the refused page"

    # THE EXHAUSTION: B's genuine failure exhausts its single declared
    # attempt through the real ladder; the maintenance seat (the drive's
    # own rederive pass) finalizes the root and the §5.4 fence — the SQL
    # function seat — runs in that same transaction.
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
    )
    for _ in range(60):
        if root == "failed":
            break
        await runner.tick(flow_id)
        root = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
        )
        if root == "failed":
            break
        await asyncio.sleep(0.02)
    assert root == "failed", "the exhausted child failure must finalize the root"

    # THE FENCED SOURCE (the same transaction family's observable outcome):
    a_after = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class, error_message, attempt::int AS attempt,
                   metadata FROM "{wf_schema}".jobs WHERE id = $1::uuid""",
        a_id,
    )
    assert a_after is not None
    assert a_after["status"] == "failed", (
        f"the first-page park must take the §5.4 disposition: {dict(a_after)}"
    )
    assert a_after["error_class"] == WORKFLOW_NOT_DRAINING_CLASS, dict(a_after)
    after_meta = (
        _json.loads(a_after["metadata"])
        if isinstance(a_after["metadata"], str)
        else dict(a_after["metadata"])
    )
    refusal = after_meta.get("wf_not_draining")
    assert isinstance(refusal, dict), after_meta
    assert refusal.get("reason") == "terminal_root_after_child_failure_exhaustion", after_meta
    assert refusal.get("root_id") == str(flow_id), after_meta
    # THE CAUSAL CHILD: B's own node — the failed child whose exhaustion
    # made the proof.
    b_id = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source_b'""",
        flow_id,
    )
    assert b_id is not None
    assert refusal.get("child_id") == str(b_id), after_meta
    assert "blocking_reason" not in after_meta, after_meta
    assert "wf_emit_reservation" not in after_meta, after_meta
    # No logical-attempt spend: the parked ordinal and the refunded
    # physical attempt stand.
    assert int(a_after["attempt"]) == 0, dict(a_after)

    # ONE state-change event with the truthful pre-update state.
    events = await wf_conn.fetch(
        f"""SELECT detail FROM "{wf_schema}".job_events
            WHERE job_id = $1::uuid AND kind = 'state_change'
              AND detail->>'error_class' = $2""",
        source_a_id,
        WORKFLOW_NOT_DRAINING_CLASS,
    )
    assert len(events) == 1, [dict(r) for r in events]
    detail = events[0]["detail"]
    detail = _json.loads(detail) if isinstance(detail, str) else dict(detail)
    assert detail.get("from_state") == "scheduled", detail
    assert detail.get("to_state") == "failed", detail

    # THE EXACT LEDGER CLAIM: A's current outer claim is `fenced`; the
    # §5.4 fence never rewrote older epochs, sibling substeps, or map
    # siblings (the decoy matrix owns that direction below).
    ledger = await wf_conn.fetch(
        f"""SELECT step_key, map_index, claim_epoch, status::text AS status, error_class
            FROM "{wf_schema}".wf_step_ledger
            WHERE job_id = $1::uuid ORDER BY claim_epoch, step_key""",
        a_id,
    )
    awaited_now = [r for r in ledger if r["status"] == "awaited"]
    fenced_now = [r for r in ledger if r["status"] == "fenced"]
    assert len(fenced_now) == 1, [(dict(r) for r in ledger)]
    assert fenced_now[0]["error_class"] == WORKFLOW_NOT_DRAINING_CLASS, dict(fenced_now[0])
    _ = awaited_now

    # THE REAL NODE CLAIM returns no row and a real promotion grants zero:
    from taskq.workflows._emit import wake_emit_backpressure
    from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE, render_sql

    claim = await wf_pool.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), a_id, JobId(str(new_uuid()))
    )
    assert claim is None, "a fenced source is unclaimable"
    from taskq.workflows._emit import ensure_emit_capacity

    async with wf_pool.acquire() as conn:
        await ensure_emit_capacity(conn, wf_sql, flow_id=JobId(str(flow_id)), bound=2)
    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=JobId(str(flow_id)))
    assert wake.promoted == 0, wake
    # The marker sweep no longer re-observes the fenced source: it is not
    # P(j) anymore, so the sweep's selection never selects the flow.
    from taskq.workflows._emit import sweep_emit_markers

    repaired = await sweep_emit_markers(module_pg_pool, wf_sql)
    assert repaired == 0, "the fenced source never re-enters the marker sweep"
    a_final = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', a_id
    )
    assert a_final == "failed", a_final


async def test_fence_spares_substep_map_and_older_epoch_decoys_both_forms(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql, module_pg_schema: Any
) -> None:
    """The exact current-claim ledger fence (the R1/F3 supplement): BOTH
    fence forms (the cascade's Python statement and the maintenance
    seat's SQL function) fence ONLY the parked job's outer current-epoch
    awaited claim — an older-epoch awaited row, a current-epoch
    ``ctx.substep`` row, and a current-epoch different-map row keep their
    prior status, class, and update instant. On the pre-fix shape the
    epoch/step/map omission fenced every awaited row of the job."""
    from taskq.workflows._emit import fence_not_draining_parked_sources

    for form in ("python_statement", "sql_function"):
        flow_id = await _seed_flow_root(wf_conn, wf_schema, status="failed")
        source_id = await _seed_parked_source(
            wf_conn, wf_schema, flow_id, cursor=True, step_key="source"
        )
        # The current outer claim (epoch 2): the park's own awaited row.
        current_ledger = await _seed_awaited_ledger(
            wf_conn,
            wf_schema,
            flow_id,
            source_id,
            step_key="source",
            map_index=None,
            attempt=1,
            claim_epoch=2,
        )
        # THE DECOYS — never rewritten by the fence:
        older = await _seed_awaited_ledger(
            wf_conn,
            wf_schema,
            flow_id,
            source_id,
            step_key="source",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
        substep = await _seed_awaited_ledger(
            wf_conn,
            wf_schema,
            flow_id,
            source_id,
            step_key="source.sub",
            map_index=None,
            attempt=1,
            claim_epoch=2,
        )
        map_sibling = await _seed_awaited_ledger(
            wf_conn,
            wf_schema,
            flow_id,
            source_id,
            step_key="source",
            map_index=0,
            attempt=1,
            claim_epoch=2,
        )
        before = await wf_conn.fetch(
            f'SELECT id, updated_at FROM "{wf_schema}".wf_step_ledger WHERE id = ANY($1::uuid[])',
            [older, substep, map_sibling],
        )
        before_stamps = {str(r["id"]): str(r["updated_at"]) for r in before}

        # The run's capacity record exists (the parked source's own intent
        # record) — the retention arm's subject.
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_emit_capacity '
            "(flow_id, bound, wake_generation, handled_generation) "
            "VALUES ($1::uuid, 2, 1, 1) ON CONFLICT DO NOTHING",
            flow_id,
        )

        if form == "python_statement":
            from taskq.workflows.engine import render_workflow_sql

            fenced = await fence_not_draining_parked_sources(
                wf_conn,
                render_workflow_sql(wf_schema),
                flow_id=JobId(flow_id),
                child_id=JobId(source_id),
                child_step_key="screen",
            )
        else:
            fenced = await wf_conn.fetchval(
                f'SELECT "{wf_schema}".fence_not_draining_parked($1::uuid, $1::uuid)',
                flow_id,
            )
        assert int(fenced) == 1, (form, fenced)

        current_row = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, error_class FROM "{wf_schema}".wf_step_ledger
                WHERE id = $1::uuid""",
            current_ledger,
        )
        assert current_row is not None
        assert current_row["status"] == "fenced", (form, dict(current_row))
        assert current_row["error_class"] == WORKFLOW_NOT_DRAINING_CLASS, (form, dict(current_row))
        for decoy_id in (older, substep, map_sibling):
            row = await wf_conn.fetchrow(
                f"""SELECT status::text AS status, error_class, updated_at
                    FROM "{wf_schema}".wf_step_ledger WHERE id = $1::uuid""",
                decoy_id,
            )
            assert row is not None
            assert row["status"] == "awaited", (
                f"{form}: the fence never rewrites a decoy claim: {dict(row)}"
            )
            assert row["error_class"] is None, (form, dict(row))
            assert str(row["updated_at"]) == before_stamps[str(decoy_id)], (
                f"{form}: the decoy's own instant is untouched: {dict(row)}"
            )

        # THE RETENTION LAW (R1): the fenced source is terminal — it no
        # longer satisfies the retention pass's live-node blocker — so
        # once the root is terminal-aged the run's capacity record is
        # collected in the same retention pass, and further marker sweeps
        # never re-observe the fenced source.
        from taskq.workflows._emit import sweep_emit_markers
        from taskq.workflows._sweep import prune_wf_step_ledger

        await wf_conn.execute(
            f"""UPDATE "{wf_schema}".jobs SET finished_at = clock_timestamp()
                - interval '2 hours' WHERE id = $1::uuid AND step_key = '__flow__'""",
            flow_id,
        )
        await prune_wf_step_ledger(wf_pool, wf_sql, retention=timedelta(hours=1))
        capacity_rows = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
            flow_id,
        )
        assert int(capacity_rows) == 0, (
            f"{form}: the capacity record must be collected once the fenced "
            f"source is terminal (no live node references the run): {capacity_rows}"
        )
        repaired = await sweep_emit_markers(wf_pool, wf_sql)
        assert repaired == 0, f"{form}: the fenced source never re-enters the marker sweep"


async def test_healthy_first_page_park_completes_its_first_page_exactly_once(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """The opposite direction: a healthy live-root first-page park never
    fails due to waiting — the capacity release wakes it, the replay
    commits the first page exactly once, and no refusal record exists."""
    from taskq.workflows import DONE, Chain, FlowRunner, Route, Step, WorkflowApp, build
    from taskq.workflows.api._graph import chain_source

    class Ok(enum.Enum):
        OK = "ok"

    async def screen_ok(ctx: Any, item: object) -> Ok:
        del ctx, item
        return Ok.OK

    chain = Chain(
        name=f"r1-healthy-{new_uuid().hex}",
        start="screen",
        steps={"screen": Step(body=screen_ok, outcomes=Ok, route=Route({Ok.OK: DONE}))},
    )

    async def source_body(ctx: Any) -> None:
        from taskq.workflows.chain import chain_start

        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1
        # ONE page (W=2) under bound 2 with the flow's own due-scheduled
        # decoy row live: charge 1 + 2 > 2 refuses BEFORE ANY write — the
        # FIRST-page park, no cursor. The replay after the release commits
        # the page exactly once.
        await ctx.emit_batch(
            [
                chain_start(
                    chain,
                    {"app_id": offset},
                    map_index=offset,
                    trace_id=f"r1-h-{offset}",
                )
                for offset in (0, 1)
            ],
            cursor={"page": page},
        )

    name = f"r1_healthy_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=2)
    def r1_healthy() -> object:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
        flow_id,
    )
    assert source_id is not None
    # THE LIVE COMPETITOR: one ordinary due-scheduled row of the same flow
    # (a real live row the derived charge counts; its due time is far
    # future so the tick never claims it) — the first page cannot fit
    # while it holds charge 1.
    decoy_id = str(new_uuid())
    await wf_conn.execute(
        f"""INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status,
            max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at,
            fairness_key, idempotency_scope, idempotency_key)
            VALUES ($1, 'r1', 'default', '{{}}', 'scheduled', 3, 'transient', 'decoy',
            $2::jsonb, statement_timestamp() + interval '1 hour', clock_timestamp(),
            $3, 'workflow-run', $4)""",
        decoy_id,
        _json.dumps({"flow_id": str(flow_id)}),
        str(flow_id),
        f"wf:{flow_id}:decoy",
    )
    parked = False
    for _ in range(40):
        await runner.tick(flow_id)
        status = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', source_id
        )
        if status == "scheduled":
            parked = True
            break
        await asyncio.sleep(0.02)
    assert parked, "the healthy first-page saturation parks"
    meta_raw = await wf_conn.fetchval(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', source_id
    )
    meta = _json.loads(meta_raw) if isinstance(meta_raw, str) else dict(meta_raw)
    assert "emit_cursor" not in meta, meta

    # THE RELEASE: the decoy's own terminal (its due work finished) frees
    # the charge; the wake promotes A and the replay commits the first
    # page exactly once.
    await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs SET status = 'succeeded',
            finished_at = clock_timestamp() WHERE id = $1::uuid""",
        decoy_id,
    )
    from taskq.workflows._emit import ensure_emit_capacity, wake_emit_backpressure

    async with wf_pool.acquire() as conn:
        await ensure_emit_capacity(conn, wf_sql, flow_id=JobId(str(flow_id)), bound=2)
    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=JobId(str(flow_id)))
    assert wake.promoted == 1, wake
    for _ in range(60):
        await runner.tick(flow_id)
        root = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
        )
        if root == "succeeded":
            break
        await asyncio.sleep(0.02)
    assert root == "succeeded", root
    page_children = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE parent_id = $1::uuid AND id <> $1::uuid""",
        source_id,
    )
    assert int(page_children) == 2, "the first page committed exactly once (no duplicates)"
    refused = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND error_class = '{WORKFLOW_NOT_DRAINING_CLASS}'""",
        flow_id,
    )
    assert int(refused) == 0, "a healthy run never derives the non-drainage refusal"
    _ = wf_conn, wf_sql


def test_migration_function_fences_the_same_current_claim_core() -> None:
    """N-8a's parity control: the catalog PL/pgSQL function cannot call the
    Python renderer, so it inlines the shared current-claim core — this
    control proves the inline spelling matches
    :func:`taskq.workflows._sql_claim.current_claim_join` exactly (the
    same core, never a third identity policy), and that the Python
    statement's rendered ledger association is byte-identical to the
    core's own output."""
    from pathlib import Path

    from taskq.workflows._sql_claim import current_claim_join
    from taskq.workflows._sql_finalize import WF_NOT_DRAINING_FENCE_SQL

    rendered = current_claim_join("fenced", "l")
    assert "l.job_id = fenced.id" in rendered
    assert "l.claim_epoch = fenced.claim_epoch" in rendered
    assert "l.step_key = fenced.step_key" in rendered
    assert "l.map_index IS NOT DISTINCT FROM fenced.map_index" in rendered
    assert "attempt" not in rendered, "the core never compares physical ordinals"
    # The Python statement's own ledger CTE carries the rendered core (the
    # constant is assembled from the renderer, so the equality is the
    # drift-proof).
    assert rendered in WF_NOT_DRAINING_FENCE_SQL

    # The migration function's INLINE spelling: every leg, the same
    # aliases, the EXPLICIT causal-child argument (F11) with its own
    # fail-closed null refusal, and no emitted-cursor gate anywhere in
    # the function.
    migration = next(Path("src/taskq/migrations").glob("01.00.42_01_*.sql"))
    function_sql = migration.read_text()
    assert "fence_not_draining_parked(p_root uuid, p_child uuid)" in function_sql
    fence_body = function_sql.split("fence_not_draining_parked(p_root uuid, p_child uuid)", 1)[1]
    assert "p_child IS NULL" in fence_body, (
        "the explicit child id fails closed when the failed-root proof has none"
    )
    assert "j.metadata ? 'emit_cursor'" not in fence_body, (
        "the §5.4 fence owns EVERY exact park — the cursor gate must not exist"
    )
    assert "l.job_id = fenced.id" in fence_body
    assert "l.claim_epoch = fenced.claim_epoch" in fence_body
    assert "l.step_key = fenced.step_key" in fence_body
    assert "l.map_index IS NOT DISTINCT FROM fenced.map_index" in fence_body
    assert "AND l.status = 'awaited'" in fence_body


async def test_cancelled_parked_peer_is_never_failed_a_second_time(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """The opposite direction: a parked peer already CANCELLED (the R2/R4
    peers arm's own disposition) is not P(j) anymore — the §5.4 fence
    never selects it, never re-fails it, and writes no second event."""
    from taskq.workflows._emit import fence_not_draining_parked_sources
    from taskq.workflows.engine import render_workflow_sql

    flow_id = await _seed_flow_root(wf_conn, wf_schema, status="failed")
    cancelled_peer = await _seed_parked_source(
        wf_conn, wf_schema, flow_id, cursor=True, step_key="source"
    )
    # The peers arm's own disposition: cancelled, the reservation removed,
    # the marker keys retained (the fence's P(j) requires scheduled).
    await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs SET status = 'cancelled',
            error_class = 'CancelledByPeerFailure', finished_at = clock_timestamp(),
            metadata = metadata - 'wf_emit_reservation'
            WHERE id = $1::uuid""",
        cancelled_peer,
    )
    events_before = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".job_events WHERE job_id = $1::uuid',
        cancelled_peer,
    )
    fenced = await fence_not_draining_parked_sources(
        wf_conn,
        render_workflow_sql(wf_schema),
        flow_id=JobId(flow_id),
        child_id=JobId(cancelled_peer),
        child_step_key="screen",
    )
    assert int(fenced) == 0, fenced
    row = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class FROM "{wf_schema}".jobs
            WHERE id = $1::uuid""",
        cancelled_peer,
    )
    assert row is not None
    assert row["status"] == "cancelled", dict(row)
    assert row["error_class"] == "CancelledByPeerFailure", dict(row)
    events_after = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".job_events WHERE job_id = $1::uuid',
        cancelled_peer,
    )
    assert int(events_after) == int(events_before), "no second disposition event"
    _ = wf_pool, wf_sql
