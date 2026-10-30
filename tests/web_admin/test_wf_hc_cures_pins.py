# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""THE H-C WAVE'S PINS — the ledger §H-C's convicted admin-UI defects,
each pinned red-first against the unfixed tree, then cured:

- H-C1 THE BRICKED RUN: ``POST /nodes/__flow__/retry`` re-armed the
  RUN'S ROOT — the CAS had no root guard, the root flipped failed→
  pending with the verdict ERASED, the maintenance rollup never saw it
  again. The cure: the CAS gains ``AND step_key <> '__flow__'`` (the
  verb-level guard — the CLI's flow-retry rides it free); the route
  answers the not-applied banner; the verb's docstring states the law
  (the root's re-run is the RUN-rerun's own concern).
- H-C2 THE REDACTION SYMMETRY: the I/O panel's RESULT face rendered
  token-head secrets RAW (the input face's chain includes the token-head
  pass; the result face's was chain-only). The cure: BOTH faces run the
  SAME chain (mask_credentials → the token-head pass), over VALUES and
  KEY NAMES both — the canary shapes (deep-nested, list-element,
  key-name) mask symmetrically.
- H-C3 THE CANCEL/RETRY TOCTOU: the verb's root status read was a bare
  SELECT — a cancel committing between the read and the CAS left a
  pending node under a cancelled root, wedged forever. The cure: the
  read takes FOR UPDATE (the CAS serializes behind the cancel's flip).
- H-C4 THE EXIT-SKIPPED BRANCH: the run-rerun's census re-arms the
  skip-envelope rows (``result ? 'skipped'``) — the envelope cleared on
  re-open — so the resumed run re-runs the cut-short work the confirm
  copy promised.
- H-C5 THE HALF-RERUN'S BANNER: the rerun composition tracks the
  applied/refused counts; a mid-composition refusal names the partial
  state ("N of M re-opened before the refusal") and lands its audit row.
- H-C6 THE CAPTURE READ: the panel prefers the LATEST FAILED attempt's
  capture (a later success's NULL capture no longer hides it), and the
  panel labels WHICH attempt's capture it shows.
- H-C7 THE RETENTION-vs-READER HONESTY: the panel distinguishes
  policy-suppressed from RETENTION-PRUNED (the run's step history gone,
  the root terminal) — the honest "pruned" arm, never the suppression
  lie.
- H-C8 THE COMPILE'S KEY LAW: the wiring key refuses the
  affordance-hostile charset (/, ?, #, %, quotes, control chars, edge
  whitespace) with the remedy named; the mermaid label writer escapes
  ``"`` and newlines (the render-death + the injected-structure fix)
  byte-stably.
- H-C9 THE NITS: the not-applied copy drops the impossible attempt-
  ceiling cause; the SSE snapshot carries the Error column's class and
  the JS adds/removes the Retry affordance live; the truncated flag
  computes POST-redaction; a refused rerun writes its audit row; the
  panel's children render is capped with the "N more" arm.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import types
import uuid
from collections.abc import Iterator
from typing import Any

import asyncpg
import httpx
import pytest
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.workflows import (
    Exit,
    FlowRunner,
    Promise,
    WorkflowApp,
    build,
    map_source,
    sink,
    step,
)

pytestmark = pytest.mark.integration

MODULE_NAME = "hc_cure_flows"


class IngestDoc(BaseModel):
    doc_id: str


async def _ok_body(ctx: Any, params: IngestDoc) -> str:
    return "src-ok"


async def _doomed_mid(ctx: Any, params: Any) -> str:
    raise RuntimeError("mid's permanent doom")


async def _doomed_a(ctx: Any, params: Any) -> str:
    raise RuntimeError("doom a")


async def _doomed_b(ctx: Any, params: Any) -> str:
    raise RuntimeError("doom b")


async def _doomed_c(ctx: Any, params: Any) -> str:
    raise RuntimeError("doom c")


async def _tail_body(ctx: Any, params: Any) -> str:
    return "tail-ok"


async def _early_exit(ctx: Any, params: Any) -> Exit[str]:
    return Exit("cut-short")


async def _canary_body(ctx: Any, params: IngestDoc) -> dict[str, object]:
    # THE CANARY SHAPES (H-C2's pin): a bare token-head VALUE, a
    # DEEP-NESTED one, a LIST-ELEMENT one, and a token-head KEY NAME —
    # plus the bulk band whose PRE-redaction text crosses the panel's
    # display cap while the POST-redaction text does not (H-C9's
    # truncated-flag conviction).
    return {
        "note": "sk-canary-value-1234567890",
        "nested": {"deep": ["sk-canary-list-9876543210"]},
        "sk-canary-key-123456789": "the wired value",
        "bulk": [f"sk-canary-big-{i:06d}" for i in range(700)],
    }


async def _map_source_body(ctx: Any, params: IngestDoc) -> list[str]:
    return ["doc-a", "doc-b", "doc-doomed"]


async def _map_item_permanently_doomed(ctx: Any, doc_id: str) -> str:
    if doc_id == "doc-doomed":
        raise RuntimeError("the doomed child's permanent failure")
    return f"done:{doc_id}"


async def _map_item_armed(ctx: Any, doc_id: str) -> str:
    # THE C6 SHAPE: attempt 1 TERMINAL-fails (max_attempts=1 — the
    # capture writes at the failure finalize), the manual resume buys
    # attempt 2 and SUCCEEDS — the node's terminal state is succeeded
    # with the failed attempt's capture behind it.
    if doc_id == "doc-doomed" and ctx.attempt < 2:
        raise RuntimeError("the armed transient failure")
    return f"done:{doc_id}"


async def _eight_src(ctx: Any, params: IngestDoc) -> list[str]:
    return [f"doc-{i}" for i in range(8)]


@pytest.fixture
def hc_app_module() -> Iterator[types.ModuleType]:
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("hc_doomed_flow")
    def doomed_flow() -> Promise[object]:
        src = step(_ok_body, IngestDoc(doc_id="d1"), key="src")
        mid = step(_doomed_mid, src, key="mid")
        return build(step(_tail_body, mid, key="tail"))

    @app_obj.workflow("hc_canary_flow")
    def canary_flow() -> Promise[object]:
        return build(step(_canary_body, IngestDoc(doc_id="d1"), key="canary-out"))

    @app_obj.workflow("hc_exit_skip_flow")
    def exit_skip_flow() -> Promise[object]:
        src = step(_ok_body, IngestDoc(doc_id="d1"), key="src")
        doomed = step(_doomed_mid, src, key="doomed")
        exiter = step(_early_exit, src, key="exiter")
        tail = step(_tail_body, exiter, key="tail")
        sink(doomed)
        return build(tail)

    @app_obj.workflow("hc_three_doomed_flow")
    def three_doomed_flow() -> Promise[object]:
        src = step(_ok_body, IngestDoc(doc_id="d1"), key="src")
        # max_attempts=1: each branch burns its single attempt and
        # terminal-fails — no backoff rounds (the three-parallel-ladder
        # drive stays deterministic under load).
        f1 = step(_doomed_a, src, key="f1", max_attempts=1)
        f2 = step(_doomed_b, src, key="f2", max_attempts=1)
        f3 = step(_doomed_c, src, key="f3", max_attempts=1)
        sink(f1, f2)
        return build(f3)

    @app_obj.workflow("hc_capture_map_flow")
    def capture_map_flow() -> Promise[object]:
        src = step(_map_source_body, IngestDoc(doc_id="d1"), key="ingest")
        items = map_source(src, _map_item_permanently_doomed, max_attempts=1)
        return build(items)

    @app_obj.workflow("hc_armed_map_flow")
    def armed_map_flow() -> Promise[object]:
        src = step(_map_source_body, IngestDoc(doc_id="d1"), key="ingest")
        items = map_source(src, _map_item_armed, max_attempts=1)
        return build(items)

    @app_obj.workflow("hc_eight_map_flow")
    def eight_map_flow() -> Promise[object]:
        src = step(_eight_src, IngestDoc(doc_id="d1"), key="ingest")
        items = map_source(src, _map_item_permanently_doomed, max_attempts=1)
        return build(items)

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


def _make_admin_app(pool: asyncpg.Pool, schema: str, wf_app: Any = None) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="", workflow_app=wf_app)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


def _csrf_of(client: Any) -> str:
    token = client.cookies.get("taskq_csrf_token", "")
    assert token, "the GET must set the CSRF cookie before any POST"
    return token


async def _seed_run(pool: asyncpg.Pool, schema: str, *, name: str, flow_input: Any = None) -> str:
    compiled = sys.modules[MODULE_NAME].app.get(name)  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    claim = await runner.create_flow(input=flow_input)
    flow_id = claim.flow_id
    await runner.drive(flow_id)
    return str(flow_id)


async def _node_row(pool: asyncpg.Pool, schema: str, run_id: str, step_key: str) -> dict[str, Any]:
    row = await pool.fetchrow(
        f'SELECT id, status, attempt, result, error_class FROM "{schema}".jobs '
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        uuid.UUID(run_id),
        step_key,
    )
    assert row is not None, f"the node row {step_key!r} must exist"
    return dict(row)


async def _map_child_row(
    pool: asyncpg.Pool, schema: str, run_id: str, step_key: str, map_index: int
) -> dict[str, Any]:
    row = await pool.fetchrow(
        f'SELECT id, status, attempt, result, error_class FROM "{schema}".jobs '
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2 AND map_index = $3",
        uuid.UUID(run_id),
        step_key,
        map_index,
    )
    assert row is not None, f"the map child row {step_key!r}#{map_index} must exist"
    return dict(row)


# ── H-C1: the root key is not a node ────────────────────────────────────


async def test_retry_verb_refuses_the_run_root_key_and_moves_nothing(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C1, VERB LEVEL: ``retry_workflow_node`` with the RUN'S OWN ROOT
    key (``__flow__``) grants NOTHING — returns 0 and leaves the root
    row BYTE-IDENTICAL (the status, the verdict, the finished stamp).
    The red: the unguarded CAS flipped the root failed→pending with the
    verdict ERASED — the maintenance rollup never saw the run again, the
    run bricked. The CLI's flow-retry rides the same verb, so this guard
    is the CLI's too."""
    from taskq.backend._protocol import JobId
    from taskq.workflows import retry_workflow_node

    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_doomed_flow")
    rid = uuid.UUID(run_id)
    root_before = await module_pg_pool.fetchrow(
        "SELECT status, error_class, error_message, result, finished_at, attempt, "
        f'scheduled_at, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
        rid,
    )
    assert root_before is not None
    assert root_before["status"] == "failed"

    granted = await retry_workflow_node(
        module_pg_pool,
        schema=schema,
        flow_id=JobId(run_id),
        node_key="__flow__",
        reason="the root-key pin",
    )

    assert granted == 0, (
        f"CONTRACT: the run's own root key is NOT a node — the verb must grant "
        f"nothing, got {granted} (the red: the CAS re-armed the ROOT and bricked the run)"
    )
    root_after = await module_pg_pool.fetchrow(
        "SELECT status, error_class, error_message, result, finished_at, attempt, "
        f'scheduled_at, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
        rid,
    )
    assert root_after is not None
    assert root_after == root_before, (
        f"CONTRACT: the refused retry must move NO byte of the root row — "
        f"before={tuple(root_before)} after={tuple(root_after)}"
    )


async def test_retry_route_answers_the_not_applied_banner_for_the_root_key(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    hc_app_module: Any,
) -> None:
    """H-C1, ROUTE LEVEL: ``POST /api/runs/{id}/nodes/__flow__/retry``
    redirects with the not-applied banner (the defined refusal state —
    never a clean 303), the root row keeps its verdict, and the refusal
    is AUDITED (H-C9's audited-refusal law at the rerun door)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_doomed_flow")
    rid = uuid.UUID(run_id)
    root_before = await module_pg_pool.fetchrow(
        f'SELECT status, error_class, error_message FROM "{schema}".jobs WHERE id = $1',
        rid,
    )
    assert root_before is not None and root_before["status"] == "failed"

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")  # the CSRF cookie's set
        r = await http.post(
            f"/api/runs/{run_id}/nodes/__flow__/retry",
            data={"csrf_token": _csrf_of(http), "reason": "the root-key route pin"},
        )
        assert r.status_code == 303, r.text
        assert "error=rerun-not-applied" in r.headers["location"], (
            f"CONTRACT: the root-key retry is a REFUSED rerun — the redirect carries "
            f"the not-applied banner, got {r.headers['location']}"
        )
        page = await http.get(r.headers["location"])
        assert page.status_code == 200
        assert "Rerun not applied" in page.text, (
            "the operator lands on the DEFINED refusal banner, not a silent clean redirect"
        )

    root_after = await module_pg_pool.fetchrow(
        f'SELECT status, error_class, error_message FROM "{schema}".jobs WHERE id = $1',
        rid,
    )
    assert root_after is not None and root_after == root_before, (
        f"the route's refusal must move no byte — before={tuple(root_before)} "
        f"after={tuple(root_after)}"
    )
    # THE AUDITED REFUSAL (H-C9): a refused rerun writes its audit row.
    refusal_row = await clean_pg_conn.fetchrow(
        f'SELECT action, target_id, detail FROM "{schema}".admin_audit '
        "WHERE action = 'workflow.retry_node' AND target_id = $1 "
        "ORDER BY id DESC LIMIT 1",
        f"{run_id}:__flow__",
    )
    assert refusal_row is not None, "a refused rerun must write its audit row (G4)"
    detail = refusal_row["detail"]
    if isinstance(detail, str):  # asyncpg hands jsonb back as str without a codec
        detail = json.loads(detail)
    assert detail.get("refused"), f"the refusal's reason rides the row: {detail}"


# ── H-C2: the I/O redaction symmetry ────────────────────────────────────


async def test_the_node_io_faces_mask_the_canary_shapes_symmetrically(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C2: the RESULT face runs the INPUT face's full chain (the
    token-head pass included) and KEY NAMES mask on BOTH faces — the
    canary shapes (bare value, deep-nested, list-element, key-name) are
    masked symmetrically. The red: the result face's chain was
    chain-only, so bare ``sk-…`` heads rode the panel RAW, and a
    key-named canary leaked on the input face."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(
        module_pg_pool,
        schema,
        name="hc_canary_flow",
        flow_input={"sk-canary-key-987654321": "v", "note": "sk-canary-input-1234567890"},
    )
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{run_id}/nodes/canary-out")
    assert resp.status_code == 200
    io = resp.json()["io"]

    dumped_input = json.dumps(io["input"])
    assert "canary" not in dumped_input, (
        f"CONTRACT: the INPUT face masks the token-head canary (value AND key "
        f"name), got {dumped_input}"
    )
    assert "***" in dumped_input, "the masked form is what the operator reads"

    result = io["result"]
    assert result is not None, "the succeeded node's result face must render"
    assert "canary" not in result["text"], (
        f"CONTRACT: the RESULT face runs the input face's FULL chain (the "
        f"token-head pass included) — no bare sk-… head rides the panel raw, "
        f"got {result['text']}"
    )
    assert "***" in result["text"], "the result face's masked form is what the operator reads"


async def test_the_truncated_flag_computes_post_redaction(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C9 (the truncated flag): the result face's ``truncated`` bit
    describes the text ACTUALLY SERVED — computed POST-redaction. The
    red: the flag measured the pre-redaction length, so a redaction-
    heavy payload reported TRUNCATED while the served text was whole."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_canary_flow")
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{run_id}/nodes/canary-out")
    assert resp.status_code == 200
    result = resp.json()["io"]["result"]
    assert result is not None
    assert len(result["text"]) <= 10_000 or result["truncated"] is True, (
        "an over-cap SERVED text must carry the truncated flag"
    )
    assert result["truncated"] is False, (
        f"CONTRACT: the truncated flag measures the POST-redaction text — the "
        f"served text is {len(result['text'])} chars (under the cap once the "
        f"canaries masked), yet the flag said truncated (the red measured the "
        f"pre-redaction length)"
    )


# ── H-C3: the cancel/retry TOCTOU ───────────────────────────────────────


class _SpyAcquire:
    """The pool.acquire() context-manager face the verb's ``async with``
    rides (the spy wraps the REAL connection)."""

    def __init__(self, pool: asyncpg.Pool, fire: Any) -> None:
        self._pool = pool
        self._fire = fire
        self._conn: Any = None

    async def __aenter__(self) -> Any:
        self._conn = await self._pool.acquire()
        return _SpyConn(self._conn, self._fire)

    async def __aexit__(self, *exc: object) -> Any:
        assert self._conn is not None
        return await self._pool.release(self._conn)


class _SpyConn:
    """The verb's connection, spied: the root-status read (the verb's
    first ``SELECT status`` read) fires the cancel on a SECOND connection
    the moment the read returns, then gives the cancel a BOUNDED window
    to commit before the CAS runs — the interleaving the TOCTOU
    conviction names: the cancel commits BETWEEN the read and the CAS.

    THE WINDOW'S TWO OUTCOMES (why this is deterministic, not a race):
    the bare read (the red) holds no lock — the cancel's whole cascade
    commits inside the window, and the CAS then re-arms the node under
    the cancelled root (the wedge, deterministic); the FOR UPDATE read
    (the cure) HOLDS the root's row lock — the cancel's root flip blocks
    for the whole window (the task stays parked, expected), the CAS
    commits first, and the cancel's own cascade flips the re-armed node
    (no wedge, deterministic)."""

    def __init__(self, real: asyncpg.Connection, fire: Any) -> None:
        self._real = real
        self._fire = fire
        self.cancel_task: asyncio.Task[Any] | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    async def fetchval(self, sql: str, *args: object) -> Any:
        value = await self._real.fetchval(sql, *args)
        if sql.lstrip().startswith("SELECT status FROM") and "jobs" in sql:
            self.cancel_task = self._fire()
            assert self.cancel_task is not None
            # THE BOUNDED WINDOW: the cancel either commits inside it
            # (the red's interleaving) or is BLOCKED on the verb's own
            # lock (the cure's serialization — the window expires).
            await asyncio.wait({self.cancel_task}, timeout=0.5)
        return value


class _SpyPool:
    def __init__(self, real: asyncpg.Pool, fire: Any) -> None:
        self._real = real
        self._fire = fire

    def acquire(self) -> _SpyAcquire:
        return _SpyAcquire(self._real, self._fire)


async def test_the_retry_serializes_behind_a_concurrent_cancel_no_pending_under_cancelled(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C3: the verb's root read takes FOR UPDATE — a cancel committing
    between the read and the CAS can no longer strand a PENDING node
    under a CANCELLED root (the wedged-forever shape). The pin drives
    the TRUE interleaving on two connections: the verb's root read
    returns, the cancel commits (its own connection), THEN the CAS runs.

    The red (the bare read): the cancel wins the gap — the node re-arms
    under the cancelled root and wedges. The cured verb: the FOR UPDATE
    lock serializes the cancel BEHIND the retry's commit, and the
    cancel's own cascade flips the re-armed node — nothing pending under
    a cancelled root."""
    from taskq.backend._protocol import JobId
    from taskq.workflows import retry_workflow_node

    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_doomed_flow")
    rid = uuid.UUID(run_id)
    # THE PRE-ROLLUP WINDOW (the live race's state): the node failed, the
    # root row still reads running (the maintenance rollup has not
    # terminalized it). The test plays that window by hand.
    await module_pg_pool.execute(
        f"""UPDATE "{schema}".jobs SET status = 'running' WHERE id = $1""", rid
    )

    fired: dict[str, asyncio.Task[Any]] = {}

    def fire() -> asyncio.Task[Any]:
        from taskq.workflows import cancel_workflow_run as _cancel

        task = asyncio.create_task(_cancel(module_pg_pool, schema=schema, flow_id=JobId(run_id)))
        fired["t"] = task
        return task

    spy_pool = _SpyPool(module_pg_pool, fire)
    granted = await retry_workflow_node(
        spy_pool,
        schema=schema,
        flow_id=JobId(run_id),
        node_key="mid",
        reason="the toctou pin",
    )
    assert granted == 1, "the node's CAS granted (the node was terminal-failed)"
    assert "t" in fired, "the pin's interleaving must have fired the cancel mid-verb"
    await fired["t"]

    root_after = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', rid
    )
    mid_after = await _node_row(module_pg_pool, schema, run_id, "mid")
    assert root_after == "cancelled", f"the cancel wins the race's tail: {root_after}"
    assert mid_after["status"] != "pending", (
        f"CONTRACT: NO pending node under a cancelled root — the wedged-forever "
        f"shape the TOCTOU conviction names. Got mid={mid_after['status']} "
        f"(the red: the bare root read let the cancel land between the read "
        f"and the CAS)"
    )


# ── H-C4: the exit-skipped branch re-arms on the run rerun ──────────────


async def test_the_run_rerun_re_arms_the_exit_skipped_rows(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C4: the run-rerun's census gains the skip-envelope leg — the
    rows carrying ``result ? 'skipped'`` re-arm (pending + the envelope
    CLEARED) when the run re-opens. The red: the exit-skipped branch
    stayed succeeded-with-envelope forever — cut-short work rendered
    green while the confirm copy promised "the run resumes"."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_exit_skip_flow")
    doomed_before = await _node_row(module_pg_pool, schema, run_id, "doomed")
    tail_before = await _node_row(module_pg_pool, schema, run_id, "tail")
    assert doomed_before["status"] == "failed"
    assert tail_before["status"] == "succeeded" and tail_before["result"] is not None
    assert "skipped" in (tail_before["result"] or ""), (
        f"the pin's premise: the exit's downstream carries the skip envelope, "
        f"got {tail_before['result']!r}"
    )

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            f"/api/runs/{run_id}/rerun",
            data={"csrf_token": _csrf_of(http), "reason": "the skip re-arm pin"},
        )
        assert r.status_code == 303, r.text

    tail_after = await _node_row(module_pg_pool, schema, run_id, "tail")
    assert tail_after["status"] == "pending", (
        f"CONTRACT: the exit-skipped branch RE-ARMS on the rerun (the run the "
        f"confirm copy promised resumes), got {tail_after['status']}"
    )
    assert tail_after["result"] is None, (
        f"CONTRACT: the skip envelope is CLEARED on re-open (the envelope must "
        f"never outlive the re-run it lied about), got {tail_after['result']!r}"
    )
    doomed_after = await _node_row(module_pg_pool, schema, run_id, "doomed")
    assert doomed_after["status"] == "pending", "the failed branch re-arms as before"
    root_after = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(run_id)
    )
    assert root_after == "running", "the run re-opens"


# ── H-C5: the half-rerun's honest banner ────────────────────────────────


async def test_a_mid_composition_refusal_names_the_partial_state(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    hc_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H-C5: a refusal landing MID-composition (the verb raising on the
    2nd of 3 failed keys) redirects with the banner that NAMES the
    partial state — "1 of 3 re-opened before the refusal" — and the
    audit row agrees (the counts on the record). The red: the banner
    implied nothing moved while the run was HALF-rerunned."""
    import taskq.workflows as wf_pkg
    from taskq.workflows import WorkflowRunError

    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_three_doomed_flow")
    failed_keys = [
        r["step_key"]
        for r in await module_pg_pool.fetch(
            f'SELECT DISTINCT step_key FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'failed' "
            "AND step_key <> '__flow__' ORDER BY step_key",
            uuid.UUID(run_id),
        )
    ]
    assert len(failed_keys) == 3, f"the pin's premise: three failed keys, got {failed_keys}"

    real = wf_pkg.retry_workflow_node
    calls = {"n": 0}

    async def refusing_on_the_second(*args: Any, **kwargs: Any) -> int:
        calls["n"] += 1
        if calls["n"] == 2:
            raise WorkflowRunError("the mid-composition refusal")
        return await real(*args, **kwargs)

    monkeypatch.setattr(wf_pkg, "retry_workflow_node", refusing_on_the_second)

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            f"/api/runs/{run_id}/rerun",
            data={"csrf_token": _csrf_of(http), "reason": "the partial pin"},
        )
        assert r.status_code == 303, r.text
        location = r.headers["location"]
        assert "rerun-partial" in location, (
            f"CONTRACT: the half-rerun's redirect names the PARTIAL state, got {location}"
        )
        page = await http.get(location)
        assert page.status_code == 200
        assert "1 of 3 re-opened before the refusal" in page.text, (
            f"the banner names the counts (the red implied nothing moved): {page.text[:2000]}"
        )

    # THE AUDIT ROWS AGREE: one granted row for f1 + the refusal's own
    # row carrying the counts.
    granted_row = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".admin_audit '
        f"WHERE action = 'workflow.retry_node' AND detail->>'run_id' = $1",
        run_id,
    )
    assert int(granted_row) == 1, f"exactly one key re-opened before the refusal: {granted_row}"
    refusal_row = await clean_pg_conn.fetchrow(
        f'SELECT detail FROM "{schema}".admin_audit '
        f"WHERE action = 'workflow.rerun' AND target_id = $1 ORDER BY id DESC LIMIT 1",
        run_id,
    )
    assert refusal_row is not None, "the partial refusal lands its own audit row"
    detail = refusal_row["detail"]
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert detail.get("applied") == 1 and detail.get("total") == 3, (
        f"the audit row's counts agree with the banner: {detail}"
    )


# ── H-C6: the failed attempt's capture surfaces, labeled ────────────────


async def test_the_capture_read_surfaces_the_failed_attempt_labeled(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C6: a node whose attempt 1 TERMINAL-FAILED with a capture and
    whose (manual-resumed) attempt 2 succeeded shows the FAILED
    attempt's capture (the diagnostic's whole point — "what did the
    failure look like?"), LABELED with which attempt it shows. The red:
    the read took the LATEST terminal row — the success's NULL capture
    hid the failure's."""
    from taskq.backend._protocol import JobId
    from taskq.workflows import retry_workflow_node

    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_armed_map_flow")
    # THE PIN'S PREMISE: the doomed child terminal-failed ATTEMPT 1 with
    # the capture on its ledger row.
    child = await _map_child_row(module_pg_pool, schema, run_id, "ingest.item", 2)
    assert child["status"] == "failed", child
    ledger = await module_pg_pool.fetchrow(
        f'SELECT attempt, status, capture IS NOT NULL AS has_cap FROM "{schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'ingest.item' AND status = 'failed' "
        "ORDER BY attempt DESC LIMIT 1",
        uuid.UUID(run_id),
    )
    assert ledger is not None and ledger["attempt"] == 1 and ledger["has_cap"], (
        f"the pin's premise: the failed attempt carries the capture, got {ledger}"
    )
    # THE MANUAL RESUME: the node re-arms, the drive succeeds it — the
    # node's TERMINAL state is now succeeded, with the failed attempt's
    # capture behind it (the diagnostic's whole point).
    granted = await retry_workflow_node(
        module_pg_pool,
        schema=schema,
        flow_id=JobId(run_id),
        node_key="ingest.item",
        reason="the capture-read pin",
    )
    assert granted == 1
    compiled = sys.modules[MODULE_NAME].app.get("hc_armed_map_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    await runner.drive(uuid.UUID(run_id))
    child = await _map_child_row(module_pg_pool, schema, run_id, "ingest.item", 2)
    assert child["status"] == "succeeded", f"the pin's premise: attempt 2 succeeded, got {child}"

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{run_id}/nodes/ingest.item?map_index=2")
        assert resp.status_code == 200
        io = resp.json()["io"]
    assert io["capture"] is not None, (
        f"CONTRACT: the LATEST FAILED attempt's capture surfaces (attempt 1's), got io={io}"
    )
    assert "the armed transient failure" in io["capture"]["error"], io["capture"]
    assert io["capture_attempt"] == 1, (
        f"CONTRACT: the panel labels WHICH attempt's capture it shows, got "
        f"{io.get('capture_attempt')}"
    )
    assert io["capture_status"] == "failed", io


# ── H-C7: the pruned-history arm, not the suppression lie ───────────────


async def test_the_panel_names_retention_pruned_history_not_policy_suppression(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C7 (the UI side): when the run's step history is GONE (the
    retention arms' prune — simulated by the row delete) while the ROOT
    row is terminal, the panel's capture block names the PRUNED arm —
    never the suppression lie (``capture_suppressed`` asserted a policy
    that never ran). The red: the pruned run read as
    ``capture_suppressed: true``."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_capture_map_flow")
    root_status = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(run_id)
    )
    assert root_status == "failed", f"the pin's premise: a terminal root, got {root_status}"
    ledger_rows = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_step_ledger WHERE flow_id = $1',
        uuid.UUID(run_id),
    )
    assert int(ledger_rows) > 0, "the pin's premise: the history exists BEFORE the prune"
    # THE RETENTION ARMS' EFFECT, played by hand (the arms' own SQL is
    # another lane's territory): the run's step history is deleted.
    await module_pg_pool.execute(
        f'DELETE FROM "{schema}".wf_step_ledger WHERE flow_id = $1', uuid.UUID(run_id)
    )

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{run_id}/nodes/ingest.item?map_index=2")
        assert resp.status_code == 200
        io = resp.json()["io"]

    assert io.get("capture_pruned") is True, (
        f"CONTRACT: the pruned-history run's panel names the PRUNED arm, got io={io}"
    )
    assert io["capture_suppressed"] is False, (
        "the suppression lie is dead: the capture was not policy-suppressed, retention pruned it"
    )
    assert io["capture"] is None


# ── H-C8: the compile's key charset law + the mermaid label escape ──────


def test_the_wiring_key_refuses_the_affordance_hostile_charset() -> None:
    """H-C8 (the compile half): the wiring key's charset law — the dot
    precedent's own shape — refuses ``/``, ``?``, ``#``, ``%``, both
    quotes, control characters, and leading/trailing whitespace, with
    the remedy NAMED. The red: every one of these compiled and then 404'd
    the affordances (the key is a URL path segment) or killed the
    Mermaid render."""
    from taskq.workflows import WorkflowBuildError

    async def body(ctx: Any, params: Any) -> str:
        return "ok"

    def _compile_with_key(key: str) -> None:
        app = WorkflowApp()

        @app.workflow("hc8_key_flow")
        def key_flow() -> Promise[object]:
            return build(step(body, IngestDoc(doc_id="d1"), key=key))

        app.get("hc8_key_flow")  # the build runs at COMPILE

    for bad in (
        "/",
        "?",
        "#",
        "%",
        '"',
        "'",
        "\n",
        "\t",
        "x/y",
        "x?q",
        "a#b",
        "a%b",
        'q"q',
        "q'q",
        " lead",
        "trail ",
    ):
        with pytest.raises(WorkflowBuildError) as exc_info:
            _compile_with_key(bad)
        assert "rename" in str(exc_info.value), (
            f"key {bad!r}: the refusal carries the remedy, got: {exc_info.value}"
        )

    # THE DOT LAW still stands (the precedent's own site).
    with pytest.raises(WorkflowBuildError, match="dot"):
        _compile_with_key("bad.key")


def test_the_mermaid_label_writer_escapes_quotes_and_newlines() -> None:
    """H-C8 (the rows half): the mermaid label writer escapes ``"`` and
    newlines in labels — a hostile row's key can neither KILL the render
    (the raw quote inside the quoted label) nor inject structure through
    the label's own bytes. The emission stays byte-stable for identical
    rows."""
    from taskq.web.admin._wf_rows import RunNode, RunView, rows_mermaid

    def _view(key: str) -> RunView:
        return RunView(
            run_id="r",
            workflow="wf",
            root_status="failed",
            created_at=None,
            finished_at=None,
            cancel_requested_at=None,
            error_class=None,
            error_message=None,
            nodes=[RunNode(key=key, status="failed")],
            holds=[],
        )

    quote_view = _view('bad"key')
    quote_text = rows_mermaid(quote_view)
    assert '["bad&quot;key"]' in quote_text, (
        f"CONTRACT: the label escapes the raw quote (the render-death fix), got {quote_text!r}"
    )

    newline_view = _view("bad\nkey")
    newline_text = rows_mermaid(newline_view)
    assert '["bad key"]' in newline_text, (
        f"CONTRACT: the label's newline cannot inject a phantom mermaid line "
        f"through the label's own bytes, got {newline_text!r}"
    )

    # BYTE-STABLE: identical rows, identical bytes.
    assert rows_mermaid(quote_view) == quote_text
    assert rows_mermaid(newline_view) == newline_text


# ── H-C9: the nits ──────────────────────────────────────────────────────


def test_the_not_applied_copy_drops_the_impossible_attempt_ceiling_cause() -> None:
    """H-C9 (the copy): the not-applied banner no longer names the
    attempt-ceiling refusal — the CAS never consults a ceiling (the
    attempt ordinal CONTINUES); an impossible cause on the record sent
    operators hunting a knob that does not exist."""
    from taskq.web.admin._wf_actions import RERUN_NOT_APPLIED_BANNER
    from taskq.web.admin.workflows import REFUSAL_BANNERS

    copy = REFUSAL_BANNERS[RERUN_NOT_APPLIED_BANNER]
    assert "attempt ceiling" not in copy, (
        f"CONTRACT: the impossible cause is off the record, got: {copy}"
    )
    assert RERUN_NOT_APPLIED_BANNER in REFUSAL_BANNERS


async def test_the_run_page_renders_the_error_cell_and_action_hooks(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
) -> None:
    """H-C9 (the SSE patch's server half + the affordances' hooks): the
    page's Error cell and Actions cell carry the patch hooks (``data-wf-
    node-error`` / ``data-wf-node-actions``) and the node table stamps
    the CSRF token the JS's live-built retry form needs."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_doomed_flow")
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        html = (await client.get(f"/workflows/{run_id}")).text
    assert "data-wf-node-error" in html, (
        "CONTRACT: the Error cell is patch-addressable (the SSE snapshot "
        "carries the class; the cell must be able to receive it)"
    )
    assert "data-wf-node-actions" in html, (
        "CONTRACT: the Actions cell is patch-addressable (the Retry "
        "affordance adds/removes LIVE with the state)"
    )
    assert "data-csrf=" in html, "the JS's live retry form rides the stamped CSRF token"


def test_the_sse_snapshot_carries_the_error_class() -> None:
    """H-C9 (the SSE patch): the snapshot's node entries carry the
    bounded error class — the Error column patches live (the red: a node
    that failed while you watched showed its class only after a
    reload)."""
    from taskq.web.admin._wf_rows import RunNode, RunView, run_state_json

    view = RunView(
        run_id="r",
        workflow="wf",
        root_status="failed",
        created_at=None,
        finished_at=None,
        cancel_requested_at=None,
        error_class=None,
        error_message=None,
        nodes=[
            RunNode(key="ok", status="succeeded"),
            RunNode(key="bad", status="failed", error_class="RuntimeError"),
        ],
        holds=[],
    )
    state = run_state_json(view, seq=1)
    by_key = {n["key"]: n for n in state["nodes"]}
    assert by_key["bad"]["error_class"] == "RuntimeError", (
        f"CONTRACT: the frame carries the Error column's class, got {by_key['bad']}"
    )
    assert by_key["ok"]["error_class"] is None


def test_the_page_js_patches_the_error_column_and_the_retry_affordance_live() -> None:
    """H-C9 (the SSE patch's client half): the page script (1) patches
    the Error cell from the frame's error_class, (2) adds the Retry
    affordance to a row that ARRIVES at failed and removes it from a row
    that LEAVES failed, and (3) renders the children census's "N more"
    arm (the bounded render)."""
    from pathlib import Path

    js = Path(__file__).parents[2].joinpath("src/taskq/web/static/workflows.js").read_text()
    assert "data-wf-node-error" in js, (
        "CONTRACT: the Error cell patches from the frame's error_class"
    )
    assert "data-wf-node-actions" in js, (
        "CONTRACT: the Retry affordance adds/removes with the row's live state"
    )
    assert "children_total" in js, (
        "CONTRACT: the children census renders its 'N more' arm (the bounded render)"
    )
    assert "capture_pruned" in js, (
        "CONTRACT: the I/O render names the pruned arm (never the suppression lie)"
    )
    assert "capture_attempt" in js, (
        "CONTRACT: the capture block labels WHICH attempt's capture it shows"
    )


_AFFORDANCE_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 1], "utf8");

// A recordable fake element (the harness's own DOM: the page script's
// touched surface only). Attribute selectors resolve against the
// appended children — the affordance's stale-form probe is the one the
// add/remove law leans on.
function el(tag) {
  const set = new Set();
  const node = {
    tag: tag || "el",
    textContent: "",
    value: undefined,
    children: [],
    attrs: {},
    setAttribute(k, v) { node.attrs[k] = v; },
    getAttribute(k) { return node.attrs[k] !== undefined ? node.attrs[k] : null; },
    removeAttribute(k) { delete node.attrs[k]; },
    appendChild(c) { node.children.push(c); return c; },
    querySelector(sel) {
      const m = sel.match(/^\[([^=\]]+)="([^"]*)"\]$/);
      if (!m) return null;
      return node.children.find((c) => c.attrs && c.attrs[m[1]] === m[2]) || null;
    },
    addEventListener() {},
    remove() {},
  };
  node.classList = {
    add: (...names) => names.forEach((n) => set.add(n)),
    remove: (...names) => names.forEach((n) => set.delete(n)),
    contains: (n) => set.has(n),
  };
  return node;
}

const rows = {};
function makeRow(key) {
  const row = el("tr");
  row.parts = {
    chip: el("span"),
    map: el("td"),
    hold: el("td"),
    error: el("td"),
    actions: el("td"),
  };
  row.querySelector = (sel) => {
    if (sel === "[data-wf-node-chip]") return row.parts.chip;
    if (sel === "[data-wf-node-map]") return row.parts.map;
    if (sel === "[data-wf-node-hold]") return row.parts.hold;
    if (sel === "[data-wf-node-error]") return row.parts.error;
    if (sel === "[data-wf-node-actions]") return row.parts.actions;
    return null;
  };
  rows[key] = row;
  return row;
}
const midRow = makeRow("mid");

const bootEl = el("script");
bootEl.textContent = JSON.stringify({
  basePath: "",
  runId: "r-1",
  state: { seq: 0, nodes: [] },
  streamUrl: "/api/runs/r-1/stream",
});
const tableEl = el("div");
tableEl.attrs["data-csrf"] = "the-token";
const graphHost = el("div");

let es = null;
global.window = {
  __taskqStatusClasses: { chip: { failed: "chip-failed", pending: "chip-pending" }, badgeBase: "b " },
  confirm: () => true,
};
global.CSS = { escape: (s) => s };
global.document = {
  getElementById(id) {
    if (id === "wf-boot") return bootEl;
    if (id === "wf-graph-host") return graphHost;
    return null;
  },
  querySelector(sel) {
    if (sel === "[data-wf-node-table]") return tableEl;
    if (sel && sel.startsWith('[data-wf-node-row="')) return rows[sel.split('"')[1]] || null;
    return null;
  },
  createElement(tag) { return el(tag); },
  querySelectorAll() { return []; },
};
// THE SCRIPT PROBES window.EventSource (not the bare global) — the stub
// rides BOTH.
const ESClass = class {
  constructor(url) { es = this; this.url = url; this.handlers = {}; }
  addEventListener(name, fn) { this.handlers[name] = fn; }
  close() {}
};
global.window.EventSource = ESClass;
global.EventSource = ESClass;

eval(src);

function frame(state) {
  es.handlers["run_state"]({ data: JSON.stringify(state) });
}

// FRAME 1: mid ARRIVES at failed — the affordance must be ADDED.
frame({
  seq: 1, run_id: "r-1", status: "failed", root_status: "failed",
  map_done: 0, map_children: 0, holds: [],
  nodes: [{ key: "mid", status: "failed", error_class: "RuntimeError", hold: false, map_done: 0, map_children: 0 }],
});
const form = midRow.parts.actions.children.find(
  (c) => c.attrs && c.attrs["data-wf-form"] === "rerun-node"
);
const out = { added: null, errorCell: midRow.parts.error.textContent };
out.added = form ? { action: form.action, csrf: (form.children[0] || {}).value } : null;

// FRAME 2: mid LEAVES failed — the affordance must be REMOVED.
frame({
  seq: 2, run_id: "r-1", status: "running", root_status: "running",
  map_done: 0, map_children: 0, holds: [],
  nodes: [{ key: "mid", status: "running", error_class: null, hold: false, map_done: 0, map_children: 0 }],
});
out.removed = midRow.parts.actions.textContent;
out.errorAfter = midRow.parts.error.textContent;
console.log(JSON.stringify(out));
"""


def test_the_retry_affordance_adds_and_removes_live_under_node() -> None:
    """H-C9 (the affordance's BEHAVIOR, driven under Node): a row that
    ARRIVES at failed gains the retry form (native POST, the stamped
    CSRF, the retry action); a row that LEAVES failed loses it; the
    Error cell patches from the frame's class both ways. The red: the
    affordance rendered once at page load and never tracked the state."""
    import subprocess
    from pathlib import Path

    node = shutil.which("node")
    if node is None:
        pytest.skip("the JS behaviour is driven under Node")
    js = Path(__file__).parents[2].joinpath("src/taskq/web/static/workflows.js")
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the paths are this file's own constants.
        [node, "-e", _AFFORDANCE_HARNESS, "--", str(js)],
        capture_output=True,
        text=True,
        check=True,
    )
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out["added"] is not None, (
        f"CONTRACT: a row arriving at failed GAINS the retry form, got {out}"
    )
    assert out["added"]["action"] == "/api/runs/r-1/nodes/mid/retry", out["added"]
    assert out["added"]["csrf"] == "the-token", (
        f"the live form rides the stamped CSRF token, got {out['added']}"
    )
    assert out["removed"] == "—", (
        f"CONTRACT: a row leaving failed LOSES the affordance, got {out['removed']!r}"
    )
    assert out["errorCell"] == "RuntimeError", out
    assert out["errorAfter"] == "—", out


async def test_the_panel_children_render_is_capped_with_the_more_arm(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    hc_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H-C9 (the bounded render): the panel's children census is CAPPED
    (the red: an unbounded render — a 10k fan-out shipped 10k rows per
    panel read) and the response carries the total so the JS can render
    the "N more" arm."""
    from taskq.web.admin import _wf_actions

    monkeypatch.setattr(_wf_actions, "_PANEL_CHILDREN_CAP", 5)

    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="hc_eight_map_flow")
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        # THE CENSUS ADDRESSES THE CHILD'S STEP KEY (the map's children
        # share one): the bare read lands on the first child and carries
        # the children census.
        resp = await client.get(f"/api/runs/{run_id}/nodes/ingest.item")
        assert resp.status_code == 200
        body = resp.json()
    children = body["children"]
    assert len(children) <= 5, (
        f"CONTRACT: the render is CAPPED, got {len(children)} children (the red: unbounded)"
    )
    assert body.get("children_total") == 8, (
        f"CONTRACT: the total rides the response (the 'N more' arm's count), "
        f"got {body.get('children_total')}"
    )
