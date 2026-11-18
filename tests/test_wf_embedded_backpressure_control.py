# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""R3/A9.3: an ``EmitBackpressure`` carried by an ``ExceptionGroup`` or an
explicit ``__cause__`` is the SAME control signal — the park door
receives the ACTUAL leaf.

The defect this control pins (DeepSeek round-3 F1, executed): only the
DIRECT raise reached the park door (`_runner.py`'s `except
EmitBackpressure`); a body emitting a page inside `asyncio.TaskGroup`
(or nested groups, or a `raise Wrapper(...) from emit_backpressure`
wrapper) delivered the group to the generic ladder — the identical page
burned three retry-consuming `ExceptionGroup` failures and terminalized
the source while the direct form parked.

THE FIX: one typed cycle-bounded shared leaf walker
(`find_embedded_exception` — match-before-descent, groups
left-to-right, then `__cause__`, one identity seen-set, NEVER
`__context__`) returns the actual first matching exception OBJECT; the
body boundary routes it through the SAME `_defer_step` park door as the
direct catch (one disposition owner). A deterministic leaf carried by
the same outer exception keeps its N2 precedence over parking.

Real runner, real `emit_batch`, real PostgreSQL; the four forms below
run the identical first-refusal body."""

from __future__ import annotations

import asyncio
import enum
import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid

pytestmark = pytest.mark.integration


class Ok(enum.Enum):
    OK = "ok"


def _chain() -> Any:
    from datetime import timedelta

    from taskq.retry import RetryPolicy
    from taskq.workflows import DONE, Chain, Route, Step
    from taskq.workflows._policy import ExecutionPolicy, RetrySpec

    return Chain(
        name=f"r3-{new_uuid().hex}",
        start="screen",
        steps={
            "screen": Step(
                body=_screen_ok,
                outcomes=Ok,
                route=Route({Ok.OK: DONE}),
                policy=ExecutionPolicy(
                    retry=RetrySpec(
                        policy=RetryPolicy(
                            max_attempts=3,
                            base=timedelta(seconds=0.01),
                            cap=timedelta(seconds=0.02),
                            jitter=0,
                        )
                    )
                ),
            )
        },
    )


async def _screen_ok(ctx: Any, item: object) -> Ok:
    del ctx, item
    return Ok.OK


async def _run_form(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection, form: str
) -> dict[str, Any]:
    """One form's full scenario: the first refusal parks (or — the
    pre-fix embedded defect — ladders), then the release + replay
    recovers exactly once. Returns the observed facts."""
    from taskq.workflows import WorkflowApp, build
    from taskq.workflows.api._graph import chain_source
    from taskq.workflows.chain import chain_start

    chain = _chain()
    body_calls: list[int] = []

    async def _emit_page(ctx: Any, page: int) -> None:
        await ctx.emit_batch(
            [
                chain_start(
                    chain,
                    {"app_id": page * 2 + offset},
                    map_index=page * 2 + offset,
                    trace_id=f"r3-{page}-{offset}",
                )
                for offset in (0, 1)
            ],
            cursor={"page": page},
        )

    async def source_body(ctx: Any) -> None:
        body_calls.append(1)
        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1
        # THE FORMS (the identical first refusal, four deliveries):
        if form == "direct":
            await _emit_page(ctx, page)
            return
        if form == "taskgroup":

            async def _one() -> None:
                await _emit_page(ctx, page)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(_one())
            return
        if form == "nested":

            async def _inner() -> None:
                async with asyncio.TaskGroup() as tg2:
                    tg2.create_task(_one_nested(ctx, page))

            async def _one_nested(ctx: Any, page: int) -> None:
                await _emit_page(ctx, page)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(_inner())
            return
        if form == "cause":
            try:
                await _emit_page(ctx, page)
            except Exception as exc:  # Why: the wrapper's own shape — the explicit __cause__ delivery the ruling requires.
                raise RuntimeError("the explicit wrapper") from exc
            return
        raise AssertionError(f"unknown form {form}")

    # THE LIVE COMPETITOR: one ordinary due-scheduled row of the same
    # flow (charge 1) so the first page (W=2) cannot fit under bound 2 —
    # the refusal is real, never staged; its terminal is the release.
    from taskq.workflows import FlowRunner

    name = f"r3_{form}_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=2)
    def r3_flow() -> object:
        return build(chain_source(chain, source_body, key="source", max_attempts=3))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
        flow_id,
    )
    assert source_id is not None
    root = ""
    decoy_id = str(new_uuid())
    await wf_conn.execute(
        f"""INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status,
            max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at,
            fairness_key, idempotency_scope, idempotency_key)
            VALUES ($1, 'r3', 'default', '{{}}', 'scheduled', 3, 'transient', 'decoy',
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
        row = await wf_conn.fetchrow(
            f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1',
            source_id,
        )
        assert row is not None
        if row["status"] in ("scheduled", "failed"):
            parked = row["status"] == "scheduled"
            break
        await asyncio.sleep(0.02)
    src = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class, attempt::int AS attempt,
                   metadata FROM "{wf_schema}".jobs WHERE id = $1""",
        source_id,
    )
    assert src is not None
    meta = (
        _json.loads(src["metadata"]) if isinstance(src["metadata"], str) else dict(src["metadata"])
    )
    ledgers = await wf_conn.fetch(
        f"""SELECT attempt::int AS a, status::text AS status, error_class
            FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'source' ORDER BY attempt""",
        flow_id,
    )
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1 AND id <> $1',
        source_id,
    )
    facts = {
        "form": form,
        "status": src["status"],
        "error_class": src["error_class"],
        "attempt": int(src["attempt"]),
        "park": meta.get("blocking_reason") == "backpressure",
        "cursor": "emit_cursor" in meta,
        "reservation": meta.get("wf_emit_reservation"),
        "ledger": [(int(r["a"]), r["status"], r["error_class"]) for r in ledgers],
        "children": int(children),
        "body_calls": len(body_calls),
    }
    if not parked:
        return facts

    # THE RELEASE + REPLAY: the competitor terminalizes; the capacity
    # wake promotes; the replay commits the page exactly once (the
    # ordinary committed-cursor recovery).
    await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs SET status = 'succeeded',
            finished_at = clock_timestamp() WHERE id = $1::uuid""",
        decoy_id,
    )
    from taskq.workflows._emit import ensure_emit_capacity, wake_emit_backpressure
    from taskq.workflows.engine import render_workflow_sql

    wsql = render_workflow_sql(wf_schema)
    async with wf_pool.acquire() as conn:
        await ensure_emit_capacity(conn, wsql, flow_id=flow_id, bound=2)
    wake = await wake_emit_backpressure(wf_pool, wsql, flow_id=flow_id)
    facts["promoted"] = wake.promoted
    root: str | None = None
    for _ in range(80):
        await runner.tick(flow_id)
        root = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
        )
        if root in ("succeeded", "failed"):
            break
        await asyncio.sleep(0.02)
    facts["root"] = root
    children_after = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1 AND id <> $1',
        source_id,
    )
    facts["children_after"] = int(children_after)
    cursor_after = await wf_conn.fetchval(
        f"SELECT metadata->'emit_cursor'->>'page' FROM \"{wf_schema}\".jobs WHERE id = $1",
        source_id,
    )
    facts["cursor_page_after"] = cursor_after
    return facts


@pytest.mark.parametrize("form", ["direct", "taskgroup", "nested", "cause"])
async def test_embedded_emit_backpressure_parks_like_the_direct_raise(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection, form: str
) -> None:
    """The four forms' one law: the first refusal parks ONCE with the
    exact marker and no reservation; the logical attempt is unchanged and
    the ledger history is ``awaited`` — never
    ``failed='ExceptionGroup'``; the refused page wrote zero children and
    no cursor; the release promotes and the replay commits the page
    exactly once."""
    facts = await _run_form(wf_pool, wf_schema, wf_conn, form)
    assert facts["status"] == "scheduled", facts
    assert facts["park"] is True, facts
    assert facts["cursor"] is False, facts
    assert facts["reservation"] is None, facts
    assert facts["attempt"] == 0, (
        "the park refunds the physical attempt; the logical ordinal stands"
    )
    assert facts["children"] == 0, "the refused page wrote zero children"
    assert facts["body_calls"] == 1, f"the first refusal parks — no ladder re-execution: {facts}"
    assert facts["ledger"] == [(1, "awaited", None)], facts
    assert facts["promoted"] == 1, facts
    assert facts["root"] == "succeeded", facts
    assert facts["children_after"] == 2, "the replay committed the page exactly once"
    assert facts["cursor_page_after"] == "0", facts


async def test_ordinary_grouped_body_failure_still_takes_the_genuine_ladder(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The opposite direction: the embedded park door never swallows an
    ORDINARY grouped body failure — a TaskGroup whose leaf is a plain
    RuntimeError rides the genuine retry ladder to its terminal."""
    from taskq.workflows import WorkflowApp, build
    from taskq.workflows.api._graph import chain_source

    chain = _chain()

    async def source_body(ctx: Any) -> None:
        del ctx

        async def _boom() -> None:
            raise RuntimeError("the ordinary grouped body failure")

        async with asyncio.TaskGroup() as tg:
            tg.create_task(_boom())

    name = f"r3_plain_group_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=2)
    def r3_plain() -> object:
        return build(chain_source(chain, source_body, key="source", max_attempts=3))

    from taskq.workflows import FlowRunner

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
        flow_id,
    )
    verdict: tuple[str, str | None] | None = None
    for _ in range(80):
        await runner.tick(flow_id)
        row = await wf_conn.fetchrow(
            f'SELECT status::text AS status, error_class FROM "{wf_schema}".jobs WHERE id = $1',
            source_id,
        )
        assert row is not None
        if row["status"] in ("failed", "succeeded"):
            verdict = (row["status"], row["error_class"])
            break
        await asyncio.sleep(0.02)
    assert verdict is not None, "the grouped failure never terminalized"
    assert verdict == ("failed", "ExceptionGroup"), (
        f"the ordinary grouped body failure takes the genuine ladder: {verdict}"
    )
    ledgers = await wf_conn.fetch(
        f"""SELECT attempt::int AS a, status::text AS status FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'source' ORDER BY attempt""",
        flow_id,
    )
    failed_rows = [r for r in ledgers if r["status"] == "failed"]
    assert len(failed_rows) == 3, (
        f"the ladder burned its declared budget (3 attempts), never parked: "
        f"{[(int(r['a']), r['status']) for r in ledgers]}"
    )
