# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The D2/D5/D6 admission-boundary controls: the grant-violation route,
the stored bound's authority, and the drive's adoption.

* **D2 (§9 control 10's replay widths).** A reserved replay that
  presents a WIDER page than its stored reservation grants is fenced
  through the NAMED deterministic grant-violation route on the FIRST
  violation: the source terminalizes with
  ``EmitReservationExceededError`` on the record, the reservation key
  removed in the same winning terminal statement, no retry budget
  spent, and the violating page's children never written. The pre-fix
  defect: the violation rode the ordinary ladder — three attempts,
  three ``failed`` ledger rows, the key retained across them.
* **D5 (the stored bound is the admission's authority).** A stored
  ``bound=4`` whose compiled declaration drifted to 6 must fail CLOSED
  at the admission (a typed corruption refusal, nothing written), never
  admit a page the stored bound never granted.
* **D6 (the drive adopts an existing active run).** A bounded run
  missing its capacity record (an old binary's active run) is ADOPTED
  by ``drive``'s own initial backfill — stamped from the validated
  compiled bound — and the drain then proceeds; the run completes on
  the supported direct driver with no out-of-band claim.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import json as _json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from taskq._ids import new_uuid
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
from taskq.workflows.chain import DONE, Route

pytestmark = pytest.mark.integration


_WORKTREE_SRC = str(Path(__file__).resolve().parents[2] / "src")


class _Outcome(enum.Enum):
    OK = "ok"


async def _screen_body(ctx: StepContext, item: object) -> _Outcome:
    del ctx, item
    return _Outcome.OK


def _chain(suffix: str) -> Chain:
    return Chain(
        name=f"abc-chain-{suffix}",
        start="screen",
        queue="default",
        steps={
            "screen": Step(
                body=_screen_body,
                outcomes=_Outcome,
                route=Route({_Outcome.OK: DONE}),
            )
        },
    )


async def _source_id(conn: Any, schema: str, flow_id: JobId) -> JobId:
    raw = await conn.fetchval(
        f'SELECT id FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'",
        flow_id,
    )
    assert raw is not None
    return JobId(str(raw))


async def _source_meta(conn: Any, schema: str, source_id: JobId) -> dict[str, Any]:
    raw = await conn.fetchval(f'SELECT metadata FROM "{schema}".jobs WHERE id = $1', source_id)
    assert raw is not None
    doc: dict[str, Any] = _json.loads(raw) if isinstance(raw, str) else dict(raw)
    return doc


async def test_reserved_replay_wider_than_grant_fences_on_the_first_violation(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: Any
) -> None:
    """D2: the W' > W replay terminalizes through the deterministic
    grant-violation route on the FIRST violation — one ledger row, the
    key removed, the violating page never written. (The pre-fix defect:
    three laddered attempts, three failed rows, the key retained.)"""
    chain = _chain(new_uuid().hex)
    replay = {"active": False}

    async def source_body(ctx: StepContext) -> None:

        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1
        if page == 0:
            # Page 0: three children fill the bound. Page 1's two-record
            # attempt then raises the body-raised EmitBackpressure — the
            # runner's own park door (the parked page width is 2).
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in (0, 1, 2)
                ],
                cursor={"page": 0},
            )
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in (100, 101)
                ],
                cursor={"page": 1},
            )
            return
        if replay["active"]:
            # THE REPLAY: present a WIDER page than the stored
            # reservation grants (3 > 2) — the named deterministic
            # grant-violation route.
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": 200 + i},
                        map_index=200 + i,
                        trace_id=f"record-{200 + i}",
                    )
                    for i in range(3)
                ],
                cursor={"page": 1},
            )
            return
        # The non-replay resume: the same honest two-record page.
        await ctx.emit_batch(
            [
                chain_start(
                    chain,
                    {"record_id": record_id},
                    map_index=record_id,
                    trace_id=f"record-{record_id}",
                )
                for record_id in (100, 101)
            ],
            cursor={"page": 1},
        )

    name = f"d2_replay_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=3)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=3))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)

    # Drive the run to the park (page 0's three children hold the bound).
    meta: dict[str, Any] = {}
    for _ in range(40):
        await runner.tick(flow_id)
        meta = await _source_meta(wf_conn, wf_schema, source_id)
        pressure = meta.get("wf_emit_backpressure") or {}
        if int(pressure.get("deferrals", 0)) >= 1:
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail(f"the source never parked: {meta}")

    # THE PROMOTION: terminalize page 0's children (the charge frees);
    # the drain grants the reservation at the parked page's own width (2).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', "
        "finished_at = clock_timestamp() WHERE parent_id = $1::uuid AND map_index < 200",
        source_id,
    )
    from taskq.workflows._emit import wake_emit_backpressure

    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
    assert wake.promoted == 1, wake
    meta = await _source_meta(wf_conn, wf_schema, source_id)
    assert meta["wf_emit_reservation"] == {"version": 1, "width": 2}, meta

    # THE REPLAY: the source re-claims and presents 3 > 2.
    replay["active"] = True
    verdict: tuple[str, str | None] | None = None
    for _ in range(40):
        await runner.tick(flow_id)
        row = await wf_conn.fetchrow(
            f'SELECT status::text AS status, error_class FROM "{wf_schema}".jobs WHERE id = $1',
            source_id,
        )
        assert row is not None
        if row["status"] in ("failed", "succeeded"):
            verdict = (row["status"], row["error_class"])
            break
        await asyncio.sleep(0.05)
    assert verdict is not None, "the replay never terminalized"
    assert verdict == ("failed", "EmitReservationExceededError"), verdict

    # THE DETERMINISTIC ROUTE'S OWN RECORD: the ledger's failed rows for
    # the source are EXACTLY ONE (the first violation) — the ladder
    # unburned; the key removed; the violating page's children never
    # written.
    ledgers = await wf_conn.fetch(
        f"""SELECT attempt::int AS a, status::text AS status, error_class
            FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'source' ORDER BY attempt""",
        flow_id,
    )
    failed_rows = [r for r in ledgers if r["status"] == "failed"]
    assert len(failed_rows) == 1, [(r["a"], r["status"]) for r in ledgers]
    assert failed_rows[0]["error_class"] == "EmitReservationExceededError"
    meta = await _source_meta(wf_conn, wf_schema, source_id)
    assert "wf_emit_reservation" not in meta, meta
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1::uuid AND map_index >= 200',
        flow_id,
    )
    assert int(children) == 0, children


async def test_stored_bound_contradicting_compiled_declaration_fails_closed(
    wf_conn: Any, wf_schema: str, wf_pool: Any
) -> None:
    """D5: the stored bound is the admission's authority — a compiled
    declaration that drifted WIDE must fail closed at the admission (a
    typed corruption refusal, nothing written), never admit a page the
    stored bound never granted.

    THE DETERMINISTIC OBSERVABLE (no timing window): the corruption is
    a deterministic authoring failure (the D2 law — a retry budget
    spent on it is the record's lie), so the FIRST tick's own emit
    refusal terminalizes the source on the record; the control asserts
    the STRUCTURAL facts — the ledger's failed row carries the typed
    corruption class at attempt 1, the stored bound is unchanged, the
    violating page committed nothing — and never a duration."""
    name = f"d5_stored_bound_{new_uuid().hex}"
    chain = _chain(new_uuid().hex)

    async def source_body(ctx: StepContext) -> None:
        # The drifted declaration's wide page: five records — the
        # stored bound (4) never granted them.
        await ctx.emit_batch(
            [
                chain_start(chain, {"record_id": i}, map_index=i, trace_id=f"d5-{i}")
                for i in range(5)
            ],
            cursor={"page": 0},
        )

    app = WorkflowApp()

    @app.workflow(name, max_in_flight=4)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)

    # THE DRIFT: the same run driven by a compiled declaration whose
    # max_in_flight moved 4 → 6 (a declaration change / deploy skew).
    drifted = dataclasses.replace(runner.compiled, max_in_flight=6)
    runner2 = FlowRunner(drifted, wf_pool, wf_schema)

    # ONE TICK: the claim, the body, the emit's pre-check passes (the
    # compiled bound), the locked recheck refuses (the stored bound), and
    # the deterministic terminal owns the death on the record.
    await runner2.tick(flow_id)

    # THE STRUCTURAL INVARIANTS.
    ledger_rows = await wf_conn.fetch(
        f"""SELECT attempt::int AS a, status::text AS status, error_class
            FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'source' ORDER BY attempt""",
        flow_id,
    )
    assert len(ledger_rows) == 1, [(r["a"], r["status"]) for r in ledger_rows]
    assert ledger_rows[0]["status"] == "failed", ledger_rows
    assert ledger_rows[0]["error_class"] == "EmitCapacityCorruptionError", ledger_rows
    assert int(ledger_rows[0]["a"]) == 1, ledger_rows
    stored = await wf_conn.fetchval(
        f'SELECT bound FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow_id,
    )
    assert int(stored) == 4, stored
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1::uuid', source_id
    )
    assert int(children) == 0, children
    source = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        source_id,
    )
    assert source is not None and source["status"] == "failed", source
    meta = _meta_of(source["metadata"])
    assert "wf_emit_reservation" not in meta, meta


async def test_drive_adopts_existing_active_run_missing_capacity_record(
    wf_conn: Any, wf_schema: str, wf_pool: Any
) -> None:
    """D6: the drive's own initial backfill adopts an existing active
    bounded run that lacks its capacity record (an old binary's run) —
    stamped from the validated compiled bound — and the run then
    completes on the supported direct driver."""
    name = f"d6_adopt_{new_uuid().hex}"
    chain = _chain(new_uuid().hex)

    def source_body(ctx: StepContext) -> Any:
        async def source() -> None:
            # One page of two records; the children's own terminals (the
            # same drive's executions) complete the run.
            await ctx.emit_batch(
                [
                    chain_start(chain, {"record_id": i}, map_index=i, trace_id=f"d6-{i}")
                    for i in range(2)
                ],
                cursor={"page": 0},
            )

        return source()

    app = WorkflowApp()

    @app.workflow(name, max_in_flight=3)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # THE OLD-BINARY SHAPE: an active bounded run with NO capacity
    # record (the create path's stamp is missing — the run predates the
    # new binary).
    await wf_conn.execute(
        f'DELETE FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid', flow_id
    )
    before = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow_id,
    )
    assert int(before) == 0, before

    # THE ADOPTED DRIVE: no corruption refusal — the record is stamped
    # and the run completes.
    verdict = await runner.drive(flow_id, tick=0.05, max_ticks=60)
    assert verdict == "terminal", verdict
    after = await wf_conn.fetchrow(
        f'SELECT bound FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow_id,
    )
    assert after is not None and int(after["bound"]) == 3, after
    source = await wf_conn.fetchrow(
        f'SELECT status::text AS status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'",
        flow_id,
    )
    assert source is not None and source["status"] == "succeeded", source


async def test_admission_boundary_commits_or_parks_through_the_real_emit_path(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: Any
) -> None:
    """Control 1 (C2): the admission boundary through the REAL
    ``ctx.emit_batch`` on cloned rows — the exact-fit page commits all W
    children + the cursor checkpoint; the one-over shape parks with ZERO
    child, edge, or cursor writes; the locked-recheck overage (a peer
    inserted between the pre-check and the locked recheck) parks with
    zero writes; a peer's valid reservation charges the derived
    arithmetic; and the checked-in legacy oracle is the byte-exact
    binding-main text, executed on the same rows."""
    from taskq.workflows._emit import observe_emit_capacity
    from tests.test_wf_emit_capacity_controls import LEGACY_EMIT_IN_FLIGHT_SQL

    # THE MECHANICAL ORACLE PIN: the checked-in constant is the binding
    # main (7f2bd32ca:_sql_finalize.py:331-338) text BYTE FOR BYTE — a
    # drift reds here (not a substring check).
    assert LEGACY_EMIT_IN_FLIGHT_SQL == (
        "SELECT count(*) FROM {schema}.jobs\n"
        "WHERE (metadata->>'flow_id')::uuid = $1\n"
        "  AND metadata ? 'flow_id'\n"
        "  AND step_key <> '__flow__'\n"
        "  AND id <> $2::uuid\n"
        "  AND status IN ('pending', 'scheduled', 'running')\n"
    ), LEGACY_EMIT_IN_FLIGHT_SQL

    def build_case(pages: list[tuple[int, ...]], bound: int, name: str) -> Any:
        chain = _chain(new_uuid().hex)

        async def source_body(ctx: StepContext) -> None:
            cursor = await ctx.cursor()
            page = int(cursor.get("page", -1)) + 1
            for p in range(page, len(pages)):
                await ctx.emit_batch(
                    [
                        chain_start(
                            chain,
                            {"record_id": record_id},
                            map_index=record_id,
                            trace_id=f"record-{record_id}",
                        )
                        for record_id in pages[p]
                    ],
                    cursor={"page": p},
                )

        app = WorkflowApp()

        @app.workflow(name, max_in_flight=bound)
        def flow() -> Promise[object]:
            return build(chain_source(chain, source_body, key="source", max_attempts=2))

        return app, name

    # ── the EXACT FIT: the page commits all W children + the cursor ──
    name = f"c2_exact_{new_uuid().hex}"
    app, _ = build_case([(0, 1)], bound=2, name=name)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    for _ in range(40):
        await runner.tick(flow_id)
        src = await wf_conn.fetchrow(
            f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1', source_id
        )
        assert src is not None
        if src["status"] in ("succeeded", "failed", "scheduled"):
            break
        await asyncio.sleep(0.05)
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1::uuid', source_id
    )
    assert int(children) == 2, children
    meta = await _source_meta(wf_conn, wf_schema, source_id)
    assert meta.get("emit_cursor", {}).get("page") == 0, meta
    # The verbatim legacy oracle EXECUTED on the same cloned rows: the
    # count equals the derived charge (the own-row exclusion holds).
    legacy_count = await wf_conn.fetchval(
        LEGACY_EMIT_IN_FLIGHT_SQL.replace("{schema}", wf_schema), flow_id, source_id
    )
    obs = await observe_emit_capacity(wf_pool, wf_sql, flow_id=flow_id, source_id=source_id)
    assert int(legacy_count) == obs.charge, (legacy_count, obs.charge)

    # ── the ONE-OVER: parks with ZERO child/edge/cursor writes. THE
    # SHAPE IS THE CHARGE-OVER (§2.3's own law): the page FITS the bound
    # (2 records under bound 2 — a wider page is the impossible-admission
    # ValueError, a separate refusal) but a live peer's charge leaves no
    # room: charge 1 + page 2 > bound 2 refuses/parks. ──
    name = f"c2_over_{new_uuid().hex}"
    app, _ = build_case([(0, 1)], bound=2, name=name)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    await _seed_reserved_peer(wf_conn, wf_schema, flow_id, width=1)
    for _ in range(40):
        await runner.tick(flow_id)
        src = await wf_conn.fetchrow(
            f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1', source_id
        )
        assert src is not None
        if src["status"] == "scheduled":
            break
        await asyncio.sleep(0.05)
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        source_id,
    )
    assert row is not None and row["status"] == "scheduled", row
    meta = _meta_of(row["metadata"])
    assert meta.get("blocking_reason") == "backpressure", meta
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1::uuid', source_id
    )
    assert int(children) == 0, children
    edges = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_edge e JOIN "{wf_schema}".jobs j '
        "ON j.id = e.child_id WHERE j.parent_id = $1::uuid",
        source_id,
    )
    assert int(edges) == 0, edges
    assert "emit_cursor" not in meta, meta

    # ── the LOCKED-RECHECK OVERAGE: the page fits the pre-check
    # (charge 0: 0 + 2 <= 2); a peer inserted while the emit blocks on
    # the advisory lock makes the locked recheck refuse (1 + 2 > 2) —
    # the park, with zero writes ──
    name = f"c2_overage_{new_uuid().hex}"
    app, _ = build_case([(0, 1)], bound=2, name=name)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    # THE SEAM: the run's admission advisory lock, held from BEFORE the
    # tick — the emit's pre-check (no lock) passes, the locked recheck
    # blocks here.
    lock_conn = await wf_pool.acquire()
    blocker_tx = lock_conn.transaction()
    await blocker_tx.start()
    await lock_conn.fetchval("SELECT pg_advisory_lock(hashtext($1::text))", str(flow_id))
    tick_task = asyncio.create_task(runner.tick(flow_id))
    try:

        async def _running() -> bool:
            src = await wf_conn.fetchrow(
                f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1',
                source_id,
            )
            return src is not None and src["status"] == "running"

        deadline = asyncio.get_running_loop().time() + 10.0
        while not await _running():
            assert asyncio.get_running_loop().time() < deadline, "the source never claimed"
            await asyncio.sleep(0.02)
        # THE OVERAGE: a peer lands between the pre-check and the locked
        # recheck (the emit is blocked on the advisory lock right now).
        await _seed_reserved_peer(wf_conn, wf_schema, flow_id, width=1)
    finally:
        await lock_conn.fetchval("SELECT pg_advisory_unlock(hashtext($1::text))", str(flow_id))
        await blocker_tx.rollback()
        await wf_pool.release(lock_conn)
    await tick_task
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        source_id,
    )
    assert row is not None and row["status"] == "scheduled", row
    meta = _meta_of(row["metadata"])
    assert meta.get("blocking_reason") == "backpressure", meta
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1::uuid', source_id
    )
    assert int(children) == 0, children

    # ── the RESERVATION ARITHMETIC: a peer's valid reservation charges ──
    flow_r = JobId(new_uuid())
    await _seed_reserved_peer(wf_conn, wf_schema, flow_r, width=2)
    obs = await observe_emit_capacity(wf_pool, wf_sql, flow_id=flow_r, source_id=None)
    assert obs.live_rows == 1, obs
    assert obs.reservations == 2, obs
    assert obs.charge == 3, obs


def _meta_of(raw: object) -> dict[str, Any]:
    decoded: Any = _json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


async def _seed_reserved_peer(conn: Any, schema: str, flow: JobId, *, width: int) -> JobId:
    """One live peer row carrying a valid two-key reservation — join-blocked
    (deps_pending=1), so the runner's own tick can never claim or resolve
    it: the peer's charge is STABLE for the arms that need it stable."""
    job = JobId(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "deps_pending, metadata, fairness_key, attempt) "
        f"VALUES ($1, 'wf-node', 'default', '{{}}', 'pending', 3, 'transient', 'peer', "
        "1, $2::jsonb, $3, 1)",
        job,
        _json.dumps(
            {
                "flow_id": str(flow),
                "blocking_reason": "join",
                "wf_emit_reservation": {"version": 1, "width": width},
            }
        ),
        str(flow),
    )
    return job


async def test_reserved_replay_narrower_than_grant_commits_and_removes_the_key(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: Any
) -> None:
    """Control 10's W' < W arm: the reserved replay presents a SMALLER
    page — the emit commits exactly W' children, removes the
    reservation, and commits normally (not divergence, no new fit
    test)."""
    chain = _chain(new_uuid().hex)
    replay = {"active": False}

    async def source_body(ctx: StepContext) -> None:
        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1
        if page == 0:
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in (0, 1)
                ],
                cursor={"page": 0},
            )
            # The page-1 attempt raises the body-raised EmitBackpressure —
            # the runner's own park door (never swallowed: the park IS the
            # deferral's truthful face).
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in (100, 101)
                ],
                cursor={"page": 1},
            )
            return
        if replay["active"]:
            # THE NARROW REPLAY: one record where two were granted.
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": 200},
                        map_index=200,
                        trace_id="record-200",
                    )
                ],
                cursor={"page": 1},
            )
            return
        await ctx.emit_batch(
            [
                chain_start(
                    chain,
                    {"record_id": record_id},
                    map_index=record_id,
                    trace_id=f"record-{record_id}",
                )
                for record_id in (100, 101)
            ],
            cursor={"page": 1},
        )

    name = f"c3_narrow_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=2)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=3))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)

    meta: dict[str, Any] = {}
    for _ in range(40):
        await runner.tick(flow_id)
        meta = await _source_meta(wf_conn, wf_schema, source_id)
        pressure = meta.get("wf_emit_backpressure") or {}
        if int(pressure.get("deferrals", 0)) >= 1:
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail(f"the source never parked: {meta}")

    # THE PROMOTION at the parked page's own width (2).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', "
        "finished_at = clock_timestamp() WHERE parent_id = $1::uuid",
        source_id,
    )
    from taskq.workflows._emit import wake_emit_backpressure

    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
    assert wake.promoted == 1, wake
    meta = await _source_meta(wf_conn, wf_schema, source_id)
    assert meta["wf_emit_reservation"] == {"version": 1, "width": 2}, meta

    # THE NARROW REPLAY: commits exactly W'=1 child, removes the key.
    replay["active"] = True
    for _ in range(40):
        await runner.tick(flow_id)
        src = await wf_conn.fetchrow(
            f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1', source_id
        )
        assert src is not None
        if src["status"] in ("succeeded", "failed"):
            break
        await asyncio.sleep(0.05)
    src = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        source_id,
    )
    assert src is not None and src["status"] == "succeeded", src
    meta = _meta_of(src["metadata"])
    assert "wf_emit_reservation" not in meta, meta
    narrow = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1::uuid AND map_index = 200',
        source_id,
    )
    assert int(narrow) == 1, narrow


def _build_c13_flow(name: str) -> Any:
    """Control 13's own declaration (both processes build it: the run's
    rows reference the workflow name + the step keys, which the factory's
    fresh compile reproduces exactly)."""
    chain = _chain(new_uuid().hex)

    async def source_body(ctx: StepContext) -> None:
        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1
        for p in range(page, 2):
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in ([0, 1], [2])[p]
                ],
                cursor={"page": p},
            )

    app = WorkflowApp()

    @app.workflow(name, max_in_flight=2)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    return app


async def test_direct_drive_producing_verdict_and_cursor_resume_after_death(
    wf_conn: Any, wf_schema: str, wf_pool: Any, tmp_path: Any, pg_dsn: str
) -> None:
    """§9 control 13: the direct drive counts its own initial backfill
    (the B term), the producing drive's verdict is terminal, and a
    producer DEATH mid-run resumes from the PERSISTED CURSOR in a second
    real OS process — no re-emit, no duplicate child, the run completes."""
    name = f"c13_drive_{new_uuid().hex}"
    app = _build_c13_flow(name)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)

    # THE PRODUCING DRIVE (this process): the backfill (B=1) + page 0's
    # own children run + page 1's refusal parks the source — the
    # producing drive's own verdict over its own service.
    verdict = await runner.drive(flow_id, tick=0.05, max_ticks=40)
    assert verdict in ("terminal", "max_ticks"), verdict
    meta = await _source_meta(wf_conn, wf_schema, source_id)
    cursor_page = meta.get("emit_cursor", {}).get("page", -1)
    assert cursor_page in (0, 1), meta

    # THE SECOND OS PROCESS: the driver resumes from the PERSISTED
    # cursor, terminalizes the run, and prints its own census (its own
    # B-term backfill counted there).
    driver = tmp_path / "c13_driver.py"
    # The subprocess drives THIS flow's own declaration: rebuild the app
    # import path — the driver imports THIS module's app through the
    # test module's own file name.
    module_name = "tests.test_wf_admission_boundary_controls"
    await asyncio.to_thread(
        driver.write_text,
        "import asyncio, importlib, json, sys\n"
        "import asyncpg\n"
        f"sys.path.insert(0, {_WORKTREE_SRC!r})\n"
        f"sys.path.insert(0, {os.getcwd()!r})\n"
        f"mod = importlib.import_module({module_name!r})\n"
        "from taskq.workflows import FlowRunner\n"
        "from taskq.workflows._emit import COUNTERS\n"
        "\n"
        "async def main() -> None:\n"
        "    pool = await asyncpg.create_pool(sys.argv[2], min_size=2, max_size=4)\n"
        "    app = mod._build_c13_flow(sys.argv[4])\n"
        "    runner = FlowRunner(app.get(sys.argv[4]), pool, sys.argv[3])\n"
        "    verdict = await runner.drive(sys.argv[1], tick=0.05, max_ticks=200)\n"
        "    print('C13-CENSUS ' + json.dumps(\n"
        "        {'verdict': verdict, 'census': COUNTERS.snapshot()}), flush=True)\n"
        "    await pool.close()\n"
        "\n"
        "asyncio.run(main())\n",
        encoding="utf-8",
    )
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(driver),
        str(flow_id),
        pg_dsn,
        wf_schema,
        name,
        cwd=os.getcwd(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env={**os.environ, "TASKQ_TEST_PG_DSN": pg_dsn},
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=240)
    lines = [ln for ln in stdout.decode().splitlines() if ln.startswith("C13-CENSUS")]
    assert lines, stdout.decode()[-2000:]
    payload = _json.loads(lines[-1].split(" ", 1)[1])
    assert payload["verdict"] == "terminal", payload
    # THE CURSOR'S OWN RESUME LAW: no re-emit — the total children are
    # EXACTLY the two pages' records (0,1,2), each once.
    children = await wf_conn.fetch(
        f'SELECT map_index::int AS m, count(*) AS n FROM "{wf_schema}".jobs '
        "WHERE parent_id = $1::uuid GROUP BY map_index ORDER BY map_index",
        source_id,
    )
    assert [(int(r["m"]), int(r["n"])) for r in children] == [(0, 1), (1, 1), (2, 1)], children
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
    )
    assert root == "succeeded", root
    src_final = await wf_conn.fetchrow(
        f'SELECT status::text AS status, attempt::int AS a, metadata FROM "{wf_schema}".jobs '
        "WHERE id = $1::uuid",
        source_id,
    )
    print(
        "C13-SOURCE "
        + _json.dumps({**dict(src_final), "metadata": _meta_of(src_final["metadata"])}),
        flush=True,
    )


_APP_HOLDER: dict[str, Any] = {}


async def test_grouped_deterministic_error_terminals_on_the_first_violation(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: Any
) -> None:
    """N2: a deterministic refusal delivered inside an ``ExceptionGroup``
    (an ``asyncio.TaskGroup`` body's own shape) is still deterministic —
    the classification walks the group's leaves; the source terminalizes
    on the FIRST violation (one failed ledger row at attempt 1, the key
    removed, the wide page never written), never a 3-attempt ladder with
    ``error_class='ExceptionGroup'``."""
    chain = _chain(new_uuid().hex)

    async def source_body(ctx: StepContext) -> None:
        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1

        async def _emit_wide() -> None:
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": 200 + i},
                        map_index=200 + i,
                        trace_id=f"record-{200 + i}",
                    )
                    for i in range(3)
                ],
                cursor={"page": 1},
            )

        if page == 0:
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in (0, 1)
                ],
                cursor={"page": 0},
            )
            # The page-1 attempt PROPAGATES its refusal — the runner's own
            # park door (never swallowed).
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in (100, 101)
                ],
                cursor={"page": 1},
            )
            return
        # THE GROUPED REPLAY: the wide page's refusal delivered inside a
        # TaskGroup (the author's own multi-coroutine shape).
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_emit_wide())

    name = f"n2_grouped_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=3)
    def flow() -> Promise[object]:
        # THE BOUND IS 3: a 3-wide page passes the impossible-admission
        # door (3 <= 3); the RESERVATION's own check (granted 2) is what
        # fires the typed refusal — the grouped leaf.
        return build(chain_source(chain, source_body, key="source", max_attempts=3))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)

    meta: dict[str, Any] = {}
    for _ in range(40):
        await runner.tick(flow_id)
        meta = await _source_meta(wf_conn, wf_schema, source_id)
        pressure = meta.get("wf_emit_backpressure") or {}
        if int(pressure.get("deferrals", 0)) >= 1:
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail(f"the source never parked: {meta}")

    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', "
        "finished_at = clock_timestamp() WHERE parent_id = $1::uuid",
        source_id,
    )
    from taskq.workflows._emit import wake_emit_backpressure

    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
    assert wake.promoted == 1, wake
    meta = await _source_meta(wf_conn, wf_schema, source_id)
    assert meta["wf_emit_reservation"] == {"version": 1, "width": 2}, meta

    # THE GROUPED REPLAY: the deterministic leaf routes the terminal.
    verdict: tuple[str, str | None] | None = None
    for _ in range(40):
        await runner.tick(flow_id)
        row = await wf_conn.fetchrow(
            f'SELECT status::text AS status, error_class FROM "{wf_schema}".jobs WHERE id = $1',
            source_id,
        )
        assert row is not None
        if row["status"] in ("failed", "succeeded"):
            verdict = (row["status"], row["error_class"])
            break
        await asyncio.sleep(0.05)
    assert verdict is not None, "the grouped replay never terminalized"
    assert verdict == ("failed", "ExceptionGroup") or verdict == (
        "failed",
        "EmitReservationExceededError",
    ), verdict
    ledgers = await wf_conn.fetch(
        f"""SELECT attempt::int AS a, status::text AS status, error_class
            FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'source' ORDER BY attempt""",
        flow_id,
    )
    failed_rows = [r for r in ledgers if r["status"] == "failed"]
    assert len(failed_rows) == 1, [(r["a"], r["status"], r["error_class"]) for r in ledgers]
    assert "EmitReservationExceededError" in (failed_rows[0]["error_class"] or ""), (
        failed_rows[0]
    )
    assert int(failed_rows[0]["a"]) == 1
    meta = await _source_meta(wf_conn, wf_schema, source_id)
    assert "wf_emit_reservation" not in meta, meta
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1::uuid AND map_index >= 200',
        source_id,
    )
    assert int(children) == 0, children


async def test_bare_tick_never_scans_the_parked_estate(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: Any
) -> None:
    """N11 (control 13's own law): a BARE `tick()` performs NO parked
    -estate scan — the parked rows' states are bit-identical across bare
    ticks and the drain/observation families never move (the marker
    sweep is the LEADER's arm, never the driver's own dispatch pass);
    and the drive's own initial backfill is its FIRST drain (the B
    term)."""
    chain = _chain(new_uuid().hex)

    async def source_body(ctx: StepContext) -> None:
        cursor = await ctx.cursor()
        page = int(cursor.get("page", -1)) + 1
        for p in range(page, 2):
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in ([0, 1], [2])[p]
                ],
                cursor={"page": p},
            )

    name = f"n11_bare_tick_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=2)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)

    # The producing drive: EXACTLY ONE TICK — the source's own claim
    # runs its body (page 0's emit; page 1's refusal PARKS the source)
    # and the drive exhausts its own bound; the children stay UNCLAIMED
    # (pending, charge-holding) — the parked estate, no blocked bodies.
    verdict = await runner.drive(flow_id, tick=0.05, max_ticks=1)
    assert verdict == "max_ticks", verdict
    meta_before = await _source_meta(wf_conn, wf_schema, source_id)
    assert "wf_emit_backpressure" in meta_before, meta_before
    parked_before = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND status = 'scheduled'
              AND metadata @> '{{"blocking_reason":"backpressure"}}'""",
        flow_id,
    )
    assert int(parked_before) >= 1, parked_before

    # THE BARE TICKS: no estate scan — the parked rows' states and the
    # drain families are bit-identical across the ticks.
    from taskq.workflows._emit import COUNTERS
    from taskq.workflows._emit import reset_counters as _rc

    _rc()
    pre = COUNTERS.snapshot()
    for _ in range(20):
        await runner.tick(flow_id, execute=False)
    post = COUNTERS.snapshot()
    for family in (
        "flow_drains",
        "candidate_reads",
        "fitted_grants",
        "new_intents",
        "observations",
        "probes",
    ):
        assert post[family] == pre[family], (
            f"the bare tick moved the {family} family (an estate scan "
            f"escaped into the driver): {pre} -> {post}"
        )
    meta_after = await _source_meta(wf_conn, wf_schema, source_id)
    assert meta_after == meta_before, (meta_before, meta_after)
    parked_after = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND status = 'scheduled'
              AND metadata @> '{{"blocking_reason":"backpressure"}}'""",
        flow_id,
    )
    assert int(parked_after) == int(parked_before)

    # THE B TERM: the drive's own initial backfill is the drive's FIRST
    # drain (exactly one flow drain on the fresh drive's first act, the
    # same primitive the arms call).
    _rc()
    pre2 = COUNTERS.snapshot()
    from taskq.workflows._emit import wake_emit_backpressure

    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
    _ = wake
    post2 = COUNTERS.snapshot()
    assert post2["flow_drains"] - pre2["flow_drains"] == 1, (pre2, post2)
