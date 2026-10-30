# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""THE ROUND-2 RED-TEAM CURES' PINS — the §H-C ROUND-2 subsection's
convictions, each cured and pinned (red-first against the unfixed tree):

- R2-1 THE DEADLOCK'S NAME: the retry CLI (``flows retry``) and the
  admin retry routes caught only ``WorkflowRunError`` — a laggard
  writer's deadlock landed a rich traceback (CLI) or a 500 wall (route).
  The cure: BOTH faces catch the SAME named pair the cancel/resolve
  verbs carry (``DeadlockDetectedError``/``SerializationError``) → the
  named refusal (the CLI) / the audited refusal row + the ``rerun-raced``
  banner (the route — the audited-refusal law).
- R2-2 THE CAPTURE FACE'S DISPLAY MASK: the input/result faces re-run
  ``_redact_io_doc`` at display; the capture face trusted the write. The
  cure: the capture face runs the SAME display-time chain (a
  pre-chain-release write's raw canary renders masked).
- R2-3 THE BOUNDED WALK: the result face walked/dumped the ENTIRE doc
  before the 10k display bound (a multi-MB row = a multi-MB walk per
  drill-down). The cure: ``_bounded_io_projection`` pre-trims the doc
  (depth/node/char budgets) BEFORE the redaction walk, the served text
  stays POST-redaction (the honesty law), and the named size arm rides
  the face (``walk_note``) — never a silent discard.
- R2-4 THE SKIP-REARM'S PREDICATE: ``result ? 'skipped'`` was
  key-existence — a foreign writer's ``{"skipped": false}`` matched and
  had its result DELETED. The cure: the engine's own SKIPPED_RESULT
  containment (``result @> '{"skipped": true}'::jsonb``).
- R2-5 THE NITS: (a) the dead ``ExitOps.retry_node`` parallel
  implementation delegates to the ONE verb (the H-C3/H-C4 laws ride the
  compiled face); (b) masked-key collisions disambiguate ``***[2]`` —
  no entry silently overwrites; (c) the rerun's audit row carries the
  ROWS count (the verb's returned row count summed) and the partial
  banner copy says rows-where-honest; (d) the CSRF-mismatch 403 names
  the remedy (reload the page).
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.workflows import (
    Exit,
    FlowRunner,
    Promise,
    WorkflowApp,
    build,
    sink,
    step,
)

pytestmark = pytest.mark.integration

MODULE_NAME = "r2_cure_flows"

THE_DEADLOCK = ("40P01", "deadlock detected")


class IngestDoc(BaseModel):
    doc_id: str


async def _ok_body(ctx: Any, params: IngestDoc) -> str:
    return "src-ok"


async def _doomed_mid(ctx: Any, params: Any) -> str:
    raise RuntimeError("mid's permanent doom")


async def _early_exit(ctx: Any, params: Any) -> Exit[str]:
    return Exit("cut-short")


async def _tail_body(ctx: Any, params: Any) -> str:
    return "tail-ok"


@pytest.fixture
def r2_app_module() -> Iterator[types.ModuleType]:
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("r2_doomed_flow")
    def doomed_flow() -> Promise[object]:
        src = step(_ok_body, IngestDoc(doc_id="d1"), key="src")
        mid = step(_doomed_mid, src, key="mid")
        return build(step(_tail_body, mid, key="tail"))

    @app_obj.workflow("r2_exit_skip_flow")
    def exit_skip_flow() -> Promise[object]:
        src = step(_ok_body, IngestDoc(doc_id="d1"), key="src")
        doomed = step(_doomed_mid, src, key="doomed")
        exiter = step(_early_exit, src, key="exiter")
        tail = step(_tail_body, exiter, key="tail")
        sink(doomed)
        return build(tail)

    @app_obj.workflow("r2_three_doomed_flow")
    def three_doomed_flow() -> Promise[object]:
        # THREE PARALLEL doomed keys (the H-C5 pin's own shape): the
        # rerun composition's failed-key census, with the FIRST sorted
        # key's rows scripted by the spy (the route sums the verb's
        # returned rows — that counting law is the pin's subject).
        src = step(_ok_body, IngestDoc(doc_id="d1"), key="src")
        f1 = step(_doomed_mid, src, key="f1", max_attempts=1)
        f2 = step(_doomed_mid, src, key="f2", max_attempts=1)
        f3 = step(_doomed_mid, src, key="f3", max_attempts=1)
        sink(f1, f2)
        return build(f3)

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


async def _seed_run(pool: asyncpg.Pool, schema: str, *, name: str) -> str:
    compiled = sys.modules[MODULE_NAME].app.get(name)  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    claim = await runner.create_flow()
    flow_id = claim.flow_id
    await runner.drive(flow_id)
    return str(flow_id)


async def _seed_root_row(pool: asyncpg.Pool, schema: str, *, status: str = "failed") -> str:
    """A synthetic RUN ROOT row (the Q5 pin's shape): the route's
    existence probe and the view's root read are the subject — the
    engine's drive is not (and the pins stay hermetic to the other
    lanes' mid-flight state)."""
    run_id = new_uuid()
    await pool.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, attempt, max_attempts, '
        f"retry_kind, step_key, metadata, status) "
        f"VALUES ($1,'wf','q','{{}}'::jsonb,0,1,'transient','__flow__',$2::jsonb,$3)",
        run_id,
        json.dumps({"flow_id": str(run_id), "workflow": "r2_doomed_flow"}),
        status,
    )
    return str(run_id)


async def _seed_node_row(
    pool: asyncpg.Pool,
    schema: str,
    run_id: str,
    *,
    step_key: str,
    status: str,
    result: dict[str, object] | None = None,
) -> str:
    """A synthetic node row (the Q5 pin's shape): a hostile/foreign row
    the engine did not write — the display's own defenses are the
    subject. The id is uuid4 ON PURPOSE: the synthetic row must not be
    confounded with the engine's own uuid7 inserts."""
    node_id = new_uuid()
    await pool.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, attempt, max_attempts, '
        f"retry_kind, step_key, metadata, status, result) "
        f"VALUES ($1,'wf','q','{{}}'::jsonb,1,1,'transient',$2,$3::jsonb,$4,$5::jsonb)",
        node_id,
        step_key,
        json.dumps({"flow_id": run_id}),
        status,
        json.dumps(result) if result is not None else None,
    )
    return str(node_id)


async def _seed_capture_row(
    pool: asyncpg.Pool, schema: str, run_id: str, node_id: str, capture: dict[str, object]
) -> None:
    """A synthetic FAILED ledger row carrying the raw capture (the
    pre-chain-release write's shape: the row predates the display chain
    and carries what the write put there)."""
    await pool.execute(
        f'INSERT INTO "{schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, attempt, status, capture) "
        "VALUES ($1, $2, $3, 'capnode', 1, 'failed', $4::jsonb)",
        new_uuid(),
        uuid.UUID(run_id),
        uuid.UUID(node_id),
        json.dumps(capture),
    )


# ── R2-1: the retry faces catch the deadlock pair ────────────────────────


async def test_the_retry_cli_answers_the_deadlock_with_the_named_refusal(
    module_pg_schema: ModulePgSchema,
    r2_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R2-1, THE CLI'S FACE: a laggard writer's deadlock (the verb
    raising the named pair) lands the NAMED refusal — exit 1, the race's
    honest-loser copy with the remedy, NEVER a rich traceback. The red:
    the retry CLI caught only ``WorkflowRunError`` — the deadlock
    surfaced as the exception face (the rich traceback the
    diagnostics-first law forbids)."""
    from typer.testing import CliRunner

    import taskq.workflows as wf_pkg
    from taskq.cli import app as cli_app

    monkeypatch.setenv("TASKQ_PG_DSN", module_pg_schema.pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)

    async def racing_verb(*args: Any, **kwargs: Any) -> int:
        raise asyncpg.exceptions.DeadlockDetectedError(*THE_DEADLOCK)

    monkeypatch.setattr(wf_pkg, "retry_workflow_node", racing_verb)

    result = await asyncio.to_thread(
        CliRunner().invoke, cli_app, ["flows", "retry", str(new_uuid()), "mid"]
    )
    assert result.exit_code == 1, result.output
    assert not isinstance(result.exception, asyncpg.PostgresError), (
        f"CONTRACT: the deadlock lands the NAMED refusal (exit 1, the remedy "
        f"copy) — never the rich-traceback face of the raw error, got "
        f"{result.exception!r}"
    )
    assert "Traceback" not in result.output, result.output
    assert "deadlock" in result.output and "re-run this verb" in result.output, (
        f"the refusal names the race and the remedy, got: {result.output!r}"
    )


async def test_the_retry_route_answers_the_deadlock_banner_not_a_500(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    r2_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R2-1, THE ROUTE'S FACE: the node-retry route catches the SAME
    named pair — the audited refusal row + the ``rerun-raced`` banner —
    NEVER a 500 wall. The red: the route caught nothing; the deadlock
    propagated to a 500 (the operator's wall)."""
    import taskq.workflows as wf_pkg

    async def racing_verb(*args: Any, **kwargs: Any) -> int:
        raise asyncpg.exceptions.DeadlockDetectedError(*THE_DEADLOCK)

    monkeypatch.setattr(wf_pkg, "retry_workflow_node", racing_verb)

    schema = module_pg_schema.schema_name
    run_id = await _seed_root_row(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            f"/api/runs/{run_id}/nodes/mid/retry",
            data={"csrf_token": _csrf_of(http), "reason": "the deadlock pin"},
        )
        assert r.status_code == 303, (
            f"CONTRACT: the deadlock is a REFUSED rerun (the named banner), "
            f"never a 500 wall — got {r.status_code}"
        )
        assert "error=rerun-raced" in r.headers["location"], (
            f"the raced banner is the redirect's key, got {r.headers['location']}"
        )
        page = await http.get(r.headers["location"])
        assert page.status_code == 200
        assert "deadlock" in page.text and "Reload the page" in page.text, (
            "the operator lands on the race's named refusal with the remedy"
        )

    # THE AUDITED REFUSAL (the law): the race's loser writes its row.
    refusal_row = await clean_pg_conn.fetchrow(
        f'SELECT detail FROM "{schema}".admin_audit '
        "WHERE action = 'workflow.retry_node' AND target_id = $1 "
        "ORDER BY id DESC LIMIT 1",
        f"{run_id}:mid",
    )
    assert refusal_row is not None, "the raced refusal is AUDITED (the law)"
    detail = refusal_row["detail"]
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert "deadlock" in (detail.get("refused") or ""), f"the row names the race: {detail}"


async def test_the_run_rerun_route_catches_the_deadlock_pair_too(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    r2_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R2-1, THE COMPOSITION'S FACE: the run-rerun route composes the
    same verb — its deadlock catches the same pair (the raced banner
    when nothing was granted yet; the partial banner + the audit row's
    refusal when some keys landed), never a 500 wall."""
    import taskq.workflows as wf_pkg

    async def racing_verb(*args: Any, **kwargs: Any) -> int:
        raise asyncpg.exceptions.DeadlockDetectedError(*THE_DEADLOCK)

    monkeypatch.setattr(wf_pkg, "retry_workflow_node", racing_verb)

    schema = module_pg_schema.schema_name
    run_id = await _seed_root_row(module_pg_pool, schema)
    # THE FAILED KEY (the composition's census needs one): the spy's
    # deadlock fires on the FIRST key's call.
    await _seed_node_row(module_pg_pool, schema, run_id, step_key="mid", status="failed")
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            f"/api/runs/{run_id}/rerun",
            data={"csrf_token": _csrf_of(http), "reason": "the composition deadlock pin"},
        )
        assert r.status_code == 303, (
            f"CONTRACT: the composition's deadlock is the named banner, never "
            f"a 500 wall — got {r.status_code}"
        )
        assert "error=rerun-raced" in r.headers["location"], (
            f"the raced banner rides the redirect, got {r.headers['location']}"
        )


# ── R2-2: the capture face's display-time mask ───────────────────────────


async def test_the_capture_face_masks_the_raw_canary_at_display(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    r2_app_module: Any,
) -> None:
    """R2-2: the capture face runs the SAME display-time chain the
    input/result faces run. The red: the capture face trusted the write —
    a row written before the chain's release (or by any foreign hand)
    rendered its raw canary on the panel. The pin plays the pre-chain
    write by hand: the ledger row carries the RAW canary, the panel
    renders the MASKED form."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_root_row(module_pg_pool, schema)
    node_id = await _seed_node_row(
        module_pg_pool, schema, run_id, step_key="capnode", status="failed"
    )
    await _seed_capture_row(
        module_pg_pool,
        schema,
        run_id,
        node_id,
        {
            "input": "the wired input sk-canary-capture-1234567890",
            "error": "boom sk-canary-err-0987654321",
        },
    )

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{run_id}/nodes/capnode")
    assert resp.status_code == 200
    capture = resp.json()["io"]["capture"]
    assert capture is not None, "the capture face renders"
    dumped = json.dumps(capture)
    assert "canary" not in dumped, (
        f"CONTRACT: the capture face runs the display-time chain — no raw "
        f"canary byte rides the panel, got {dumped}"
    )
    assert "***" in dumped, "the masked form is what the operator reads"
    # THE H-C6 LABELS SURVIVE THE MASK (the chain masks the strings, not
    # the attempt's identity).
    assert resp.json()["io"]["capture_attempt"] == 1


# ── R2-3: the bounded walk ───────────────────────────────────────────────


def test_the_walk_projection_bounds_a_multi_mb_doc() -> None:
    """R2-3, THE PROJECTION'S OWN TEETH: a 2MB doc's projection walks a
    BOUNDED node/char budget (the red: the result face walked and dumped
    the ENTIRE doc before the 10k display bound — a multi-MB walk per
    drill-down), and a pathological DEPTH cannot recurse the walk into
    a RecursionError. An under-budget doc is untouched (no note, no
    trim)."""
    from taskq.web.admin import _wf_actions

    blob = "A" * 2_000_000
    _projected, note, stats = _wf_actions._bounded_io_projection({"blob": blob})
    assert stats["chars"] <= _wf_actions._WALK_CHAR_BUDGET, (
        f"CONTRACT: the walk is CHAR-bounded — processed {stats['chars']} for a "
        f"2MB doc (the red walked all of it)"
    )
    assert stats["nodes"] <= _wf_actions._WALK_NODE_BUDGET
    assert note is not None and "walk budget" in note, f"the trim is NAMED, never silent: {note!r}"
    assert "showing the first" in note, note

    # THE DEPTH ARM: a 50k-deep doc cannot RecursionError the display.
    deep: object = "leaf"
    for _ in range(50_000):
        deep = [deep]
    _p2, note2, stats2 = _wf_actions._bounded_io_projection(deep)
    assert stats2["nodes"] <= _wf_actions._WALK_NODE_BUDGET
    assert note2 is not None, "the deep trim is named too"

    # THE UNDER-BUDGET DOC: untouched, no note (the common case pays
    # nothing and says nothing).
    small_doc = {"small": "ok", "list": [1, 2, 3]}
    p3, note3, _s3 = _wf_actions._bounded_io_projection(small_doc)
    assert note3 is None
    assert p3 == small_doc


async def test_the_result_face_answers_bounded_with_the_named_arm(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    r2_app_module: Any,
) -> None:
    """R2-3, THE ENDPOINT'S FACE: a 2MB synthetic result row's drill-down
    answers BOUNDED (the walk's budget bounds the work; the response
    never carries the multi-MB doc) WITH the named size arm riding the
    face — and the honesty law holds: the served text is still
    POST-redaction (a canary inside the kept band masks)."""
    schema = module_pg_schema.schema_name
    # THE G7-CONSISTENT ROOT: the succeeded child's run derives
    # 'complete' — the root reports it (a 'failed' root over these rows
    # would red the always-on G7 check, correctly).
    run_id = await _seed_root_row(module_pg_pool, schema, status="succeeded")
    canary_head = "sk-canary-head-1234567890"
    await _seed_node_row(
        module_pg_pool,
        schema,
        run_id,
        step_key="bigres",
        status="succeeded",
        result={"value": {"blob": f"{canary_head} {'A' * 2_000_000}"}},
    )

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{run_id}/nodes/bigres")
    assert resp.status_code == 200
    assert len(resp.content) < 100_000, (
        f"CONTRACT: the 2MB row's drill-down answers BOUNDED — got "
        f"{len(resp.content)} bytes (the red walked and dumped the whole doc)"
    )
    face = resp.json()["io"]["result"]
    assert face is not None
    assert "walk_note" in face and "walk budget" in face["walk_note"], (
        f"CONTRACT: the named size arm rides the face — got {face.keys()}"
    )
    assert "showing the first" in face["walk_note"]
    assert "not shown" in face["text"], (
        "the cut is named at its own site too — a partial doc never reads as whole"
    )
    assert len(face["text"]) < 10_000, "the served text is bounded"
    # THE HONESTY LAW: the served text stays POST-redaction — the
    # canary inside the KEPT band is masked, never served raw.
    assert "canary" not in face["text"], face["text"][:200]
    assert "***" in face["text"]


def test_the_page_js_renders_the_walk_budget_arm() -> None:
    """R2-3's client half: the JS renders the walk_note arm (the named
    size arm is the operator's answer, not a hidden field)."""
    js = Path(__file__).parents[2].joinpath("src/taskq/web/static/workflows.js").read_text()
    assert "walk_note" in js, (
        "CONTRACT: the I/O render names the walk-budget cut (never a silent discard)"
    )


# ── R2-4: the skip-rearm's predicate is the true shape ───────────────────


async def test_the_skip_rearm_predicate_is_the_true_skipped_true_shape(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    r2_app_module: Any,
) -> None:
    """R2-4: the re-arm's predicate is the engine's OWN SKIPPED_RESULT
    contract — ``result @> '{\"skipped\": true}'::jsonb``. The red: bare
    KEY-EXISTENCE (``result ? 'skipped'``) matched a foreign writer's
    ``{\"skipped\": false}`` and DELETED its result. The pin: false/"no"
    envelopes are NOT re-armed (their bytes survive untouched); the true
    envelope re-arms."""
    from taskq.backend._protocol import JobId
    from taskq.workflows import retry_workflow_node

    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="r2_exit_skip_flow")
    tail_before = await module_pg_pool.fetchrow(
        f'SELECT status, result FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'tail'",
        uuid.UUID(run_id),
    )
    assert tail_before is not None and tail_before["status"] == "succeeded"
    assert "skipped" in (tail_before["result"] or ""), (
        f"the pin's premise: the exit's downstream carries the true envelope, "
        f"got {tail_before['result']!r}"
    )

    # THE FOREIGN WRITERS' ROWS: result CARRIES the key, but the shape is
    # not the engine's true envelope.
    false_id = await _seed_node_row(
        module_pg_pool,
        schema,
        run_id,
        step_key="false_skip",
        status="succeeded",
        result={"skipped": False},
    )
    no_id = await _seed_node_row(
        module_pg_pool,
        schema,
        run_id,
        step_key="no_skip",
        status="succeeded",
        result={"skipped": "no"},
    )

    granted = await retry_workflow_node(
        module_pg_pool,
        schema=schema,
        flow_id=JobId(run_id),
        node_key="doomed",
        reason="the predicate pin",
    )
    assert granted == 1, "the failed node's own CAS granted"

    tail_after = await module_pg_pool.fetchrow(
        f'SELECT status, result FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'tail'",
        uuid.UUID(run_id),
    )
    assert tail_after is not None
    assert tail_after["status"] == "pending", (
        f"CONTRACT: the TRUE skipped envelope re-arms (the exit's own mark), "
        f"got {tail_after['status']}"
    )
    assert tail_after["result"] is None, "the envelope is CLEARED on re-open (H-C4's law)"

    for row_id, key in ((false_id, "false_skip"), (no_id, "no_skip")):
        row = await module_pg_pool.fetchrow(
            f'SELECT status, result FROM "{schema}".jobs WHERE id = $1', uuid.UUID(row_id)
        )
        assert row is not None and row["status"] == "succeeded", (
            f"CONTRACT: the foreign writer's {key!r} row is NOT re-armed (key-"
            f"existence is not the true shape), got {row}"
        )
        assert row["result"] is not None, (
            f"CONTRACT: the foreign writer's {key!r} result SURVIVES (the red "
            f"deleted a {key!r} envelope)"
        )


# ── R2-5: the nits ───────────────────────────────────────────────────────


async def test_the_compiled_retry_face_delegates_to_the_verb(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    r2_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R2-5a: the compiled face's ``retry_node`` DELEGATES to the one
    id-addressed verb (the parallel implementation was stale — it lacked
    the H-C3 FOR-UPDATE serialization and the H-C4 skip re-arm). The pin
    proves the call-through (the SAME public seam the CLI/routes ride —
    one name to patch) and the bool mapping of the verb's row count.
    Hermetic: the spy touches no rows."""
    import taskq.workflows as wf_pkg

    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("r2_doomed_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)

    seen: dict[str, object] = {}

    async def spy_verb(*args: Any, **kwargs: Any) -> int:
        seen["pool"] = args[0] if args else kwargs.get("pool")
        seen["flow_id"] = kwargs.get("flow_id")
        seen["node_key"] = kwargs.get("node_key")
        return 2  # the verb's ROW count (a map key's grant)

    monkeypatch.setattr(wf_pkg, "retry_workflow_node", spy_verb)
    flow_id = new_uuid()
    assert await runner.retry_node(flow_id, "mid") is True, (
        "CONTRACT: the compiled face delegates to the verb — a granted grant (rows > 0) maps True"
    )
    assert seen["node_key"] == "mid" and seen["pool"] is module_pg_pool, seen
    assert str(seen["flow_id"]) == str(flow_id), seen

    async def spy_zero(*args: Any, **kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(wf_pkg, "retry_workflow_node", spy_zero)
    assert await runner.retry_node(flow_id, "mid") is False, "0 rows maps False"


async def test_the_verb_returns_the_true_row_count_for_a_map_key(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    r2_app_module: Any,
) -> None:
    """R2-5c, THE VERB'S HALF: a map's failed children share one step
    key — the verb returns the ROWS it re-armed (2 failed rows of the
    key → 2), not a grant bit (the red: the applied-count counted KEYS
    while rows moved unseen), and the verb's own audit row carries the
    count. The failed rows are SEEDED BY HAND (the Q5 synthetic-row
    precedent): the map's own fail_closed cascade cancels the pending
    peers at the first doomed child — the real fan-out cannot
    deterministically land two failed rows of one key, and the CAS's
    RETURNING count is the pin's subject."""
    from taskq.backend._protocol import JobId
    from taskq.workflows import retry_workflow_node

    schema = module_pg_schema.schema_name
    run_id = await _seed_root_row(module_pg_pool, schema)
    first = await _seed_node_row(module_pg_pool, schema, run_id, step_key="a.item", status="failed")
    second = await _seed_node_row(
        module_pg_pool, schema, run_id, step_key="a.item", status="failed"
    )

    rows = await retry_workflow_node(
        module_pg_pool,
        schema=schema,
        flow_id=JobId(run_id),
        node_key="a.item",
        reason="the row-count pin",
    )
    assert rows == 2, f"CONTRACT: the verb returns the ROWS re-armed for the map key, got {rows}"
    detail = await clean_pg_conn.fetchval(
        f'SELECT detail FROM "{schema}".admin_audit '
        "WHERE action = 'workflow.retry_node' AND target_id = $1 "
        "ORDER BY id DESC LIMIT 1",
        f"{run_id}:a.item",
    )
    assert detail is not None, "the verb's own audit row rides"
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert detail.get("rows_reopened") == 2, f"the row count is ON THE ROW: {detail}"
    # BOTH rows re-armed (the CAS's write set is the key's failed rows).
    for row_id in (first, second):
        status = await module_pg_pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(row_id)
        )
        assert status == "pending", f"the failed row re-armed, got {status}"


async def test_the_rerun_audit_row_gains_the_rows_count_and_the_banner_says_rows(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    r2_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R2-5c, THE ROUTE'S HALF: the rerun composition's audit row carries
    the ROWS count (the verb's returned row count summed) beside the key
    counts, and the partial banner copy says ROWS-WHERE-HONEST (a map
    key's grant moved several rows — "2 node rows re-opened across 1 of
    3 failed keys", never a key count wearing a row count's clothes)."""
    import taskq.workflows as wf_pkg
    from taskq.workflows import WorkflowRunError

    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="r2_three_doomed_flow")
    failed_keys = await module_pg_pool.fetch(
        f'SELECT DISTINCT step_key FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'failed' "
        "AND step_key <> '__flow__' ORDER BY step_key",
        uuid.UUID(run_id),
    )
    assert len(failed_keys) == 3, f"the pin's premise: three failed keys, got {failed_keys}"

    async def grant_rows_then_refuse(*args: Any, **kwargs: Any) -> int:
        if kwargs.get("node_key") == "f1":
            return 2  # the MAP KEY's row count (the verb's own returned shape)
        raise WorkflowRunError("the mid-composition refusal")

    monkeypatch.setattr(wf_pkg, "retry_workflow_node", grant_rows_then_refuse)

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            f"/api/runs/{run_id}/rerun",
            data={"csrf_token": _csrf_of(http), "reason": "the rows pin"},
        )
        assert r.status_code == 303, r.text
        location = r.headers["location"]
        assert "rerun-partial:2:1:3" in location, (
            f"CONTRACT: the banner key carries the ROWS count first, got {location}"
        )
        page = await http.get(location)
        assert page.status_code == 200
        assert "2 node rows re-opened across 1 of 3 failed keys" in page.text, (
            f"CONTRACT: the copy says ROWS-WHERE-HONEST, got: {page.text[:2000]}"
        )

    detail = await clean_pg_conn.fetchval(
        f'SELECT detail FROM "{schema}".admin_audit '
        "WHERE action = 'workflow.rerun' AND target_id = $1 ORDER BY id DESC LIMIT 1",
        run_id,
    )
    assert detail is not None, "the partial refusal lands its audit row"
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert detail.get("rows") == 2, (
        f"CONTRACT: the audit row carries the ROWS count (the verb's returned "
        f"row count summed), got {detail}"
    )
    assert detail.get("applied") == 1 and detail.get("total") == 3, (
        f"the KEY counts still ride (H-C5's shape preserved): {detail}"
    )


def test_the_partial_banner_copy_says_rows_only_where_honest() -> None:
    """R2-5c's copy law, both arms: the row/key counts AGREE → the plain
    "N of M" (the H-C5 pin's shape preserved); they DIVERGE → the copy
    names the rows."""
    from types import SimpleNamespace

    from taskq.web.admin._wf_actions import rerun_partial_banner
    from taskq.web.admin.workflows import _refusal_banner

    request = SimpleNamespace(query_params={"error": rerun_partial_banner(1, 1, 3)})
    copy = _refusal_banner(request)  # type: ignore[arg-type]
    assert copy is not None and "1 of 3 re-opened before the refusal" in copy, copy

    request_rows = SimpleNamespace(query_params={"error": rerun_partial_banner(2, 1, 2)})
    copy_rows = _refusal_banner(request_rows)  # type: ignore[arg-type]
    assert copy_rows is not None and "2 node rows re-opened across 1 of 2 failed keys" in (
        copy_rows
    ), copy_rows


def test_masked_key_collisions_keep_every_entry() -> None:
    """R2-5b: two keys masking to the same ``***`` must not overwrite —
    the walk disambiguates ``***[2]`` (until unique): every entry
    survives, no raw key byte rides the disambiguation. The red: the
    second entry silently REPLACED the first (the dict keyed on the
    masked name)."""
    from taskq.web.admin._wf_actions import _redact_io_doc

    doc = {
        "sk-canary-one-123456789": "value-one",
        "sk-canary-two-987654321": "value-two",
        "plain": "kept",
    }
    out = _redact_io_doc(doc)
    assert isinstance(out, dict)
    assert len(out) == 3, (
        f"CONTRACT: both masked entries SURVIVE (the red kept only the last overwrite), got {out}"
    )
    keys = sorted(out)
    assert keys[0] == "***" or keys[0].startswith("***"), keys
    assert keys[1].startswith("***[2]"), (
        f"CONTRACT: the colliding masked key is disambiguated with the ordinal "
        f"(no entry dropped, no raw byte leaked), got {keys}"
    )
    assert set(out.values()) >= {"value-one", "value-two", "kept"}, (
        f"no value is silently dropped: {out}"
    )
    dumped = json.dumps(out)
    assert "canary" not in dumped, f"the disambiguation leaks no raw key byte: {dumped}"


async def test_the_csrf_mismatch_refusal_names_the_reload(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    r2_app_module: Any,
) -> None:
    """R2-5d: the CSRF-mismatch 403's remedy text names the reload (the
    cookie-loss 403 loop's cure — the page's token is stale; the
    operator's only fix is a fresh page, and the refusal SAYS so instead
    of looping the submit)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_root_row(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        stale = await http.post(
            f"/api/runs/{run_id}/nodes/mid/retry",
            data={"csrf_token": "the-stale-token", "reason": "the csrf pin"},
        )
        assert stale.status_code == 403
        detail = stale.json()["detail"]
        assert "the page's token is stale" in detail and "reload the page" in detail, (
            f"CONTRACT: the mismatch refusal names the remedy (the cookie-loss "
            f"403 loop's cure), got {detail!r}"
        )

        # THE MISSING-COOKIE ARM (the loop's other door): a client with
        # no cookie jar at all — the same remedy.
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            follow_redirects=False,
        ) as no_cookie:
            resp = await no_cookie.post(
                f"/api/runs/{run_id}/nodes/mid/retry",
                data={"csrf_token": "any-token"},
            )
            assert resp.status_code == 403
            detail_lost = resp.json()["detail"]
            assert "reload the page" in detail_lost, detail_lost
