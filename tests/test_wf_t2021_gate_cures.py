"""THE T20/T21 GATE-CURE PINS — the progress gates + the typed deaths +
the failure-class routing rule.

THE CONVICTIONS THESE HOLD CLOSED (the T20/T21 front's finding, each
cured in src and pinned here):

* THE PRE-VALIDATION DICT (the lax-pydantic hole): the schema gate ran
  and its answer was THROWN AWAY — ``{"page": "42"}`` against a
  declared ``page: int`` landed the STRING in the state row and the
  stream ring. THE CURE: the VALIDATED model's own dump
  (``model_dump(mode="json")``) is the record.
* THE PUBLIC VALIDATING-NOTHING DOOR: ``ProgressEmitter.submit`` was
  PUBLIC and validated nothing — a ``pct=999`` reached the buffer and
  persisted (the migration's comment claimed a CHECK constraint that
  did not exist). THE CURE: the buffer write is PRIVATE (``_submit`` —
  the public op is :meth:`emit`/``ctx.progress`` and it validates);
  the CHECK is REAL (``wf_node_progress_pct_domain``, 01.00.32_01 —
  the comment's promise shipped).
* THE AUTO-CLASS DISCARDED RECEIPT: ``project_auto_event`` threw the
  append's drop count away — the user class's receipt discipline
  (DH2's honest appended==retained+dropped pair) was user-only. THE
  CURE: the counters row rides every projection.
* THE DETERMINISTIC-AS-TRANSIENT LADDER BURN: ``ProgressRefusedError``
  (a DETERMINISTIC authoring bug) laddered like a flake and burned the
  retry budget. THE CURE: the failure-class routing rule —
  deterministic = the NAMED terminal (the ladder unburned), infra =
  reclaim, transient = the ladder — pinned per class.
* THE TWO UNTYPED DEATHS: the divergent-page resume died as the raw
  ``UniqueViolationError`` (laddered to death, never named) — now
  :class:`PageDivergedError` (the remedy rides the message); record
  #32768 died as the raw DataError mid-tx (the valid page-mates rolled
  back with it) — now :class:`MapIndexExhaustedError` (the poison
  record kills ITSELF loudly, never its mates — the mates COMMIT).
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, StepContext, WorkflowApp, build, step
from taskq.workflows._progress import (
    PROGRESS_RING_BOUND,
    STREAM_CHANNEL,
    ProgressEmitter,
    ProgressRefusedError,
)
from taskq.workflows._types import EmitChild
from tests._wf_fixtures import RedLog, seed_flow

pytestmark = pytest.mark.integration


class Page(BaseModel):
    page: int


class Ingest(BaseModel):
    doc_id: str


# ── THE VALIDATED DUMP IS THE RECORD ────────────────────────────────────


async def test_the_validated_dump_lands_not_the_pre_validation_dict(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    progress_redlog: RedLog,
) -> None:
    """THE LAX-PYDANTIC HOLE, pinned shut: a body emits ``{"page": "42"}``
    (a STRING) against the declared ``page: int`` — pydantic's lax
    validation coerces, and the VALIDATED dump (page == 42, the int) is
    what lands in the STATE row and the STREAM payload. RED (the
    pre-cure tree): the raw dict was stored and the string landed where
    the int is declared — the schema was decoration, the storage lied."""
    app = WorkflowApp()

    async def lax_body(ctx: StepContext, params: Ingest) -> dict[str, object]:
        # The string is the convicted shape: lax pydantic coerces it —
        # the RECORD must carry the coercion, never the input.
        await ctx.progress(50, "the lax page", {"page": "42"})
        return {"ok": True}

    @app.workflow("t21_validated_dump")
    def t21_validated_dump() -> object:
        return build(step(lax_body, Ingest(doc_id="d1"), key="lax", progress_schema=Page))

    runner = FlowRunner(app.get("t21_validated_dump"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    await runner.drive(flow_id)

    node = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs WHERE step_key = 'lax'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    state = await wf_conn.fetchrow(
        f"""SELECT data, pct FROM "{wf_schema}".wf_node_progress
            WHERE node_id = $1 AND channel = 'progress'""",
        node,
    )
    assert state is not None, "the emission never landed"
    data = state["data"]
    doc = json.loads(data) if isinstance(data, str) else data
    progress_redlog.red(
        "t21-validated-dump",
        "the PRE-VALIDATION dict stored (the lax gate's answer thrown "
        "away): the string landed where the int is declared",
        {"stored": doc},
    )
    assert doc == {"page": 42} and not isinstance(doc.get("page"), str), (
        f"THE PRE-VALIDATION DICT LANDED: the state row carries {doc!r} — "
        "the declared schema's coercion must BE the record (the validated "
        "dump, never the raw input)"
    )
    # The STREAM ring's payload is the same validated dump.
    stream_payload = await wf_conn.fetchval(
        f"""SELECT payload FROM "{wf_schema}".wf_node_stream
            WHERE node_id = $1 AND class = 'user' ORDER BY seq DESC LIMIT 1""",
        node,
    )
    sdoc = json.loads(stream_payload) if isinstance(stream_payload, str) else stream_payload
    assert sdoc["data"] == {"page": 42}, (
        f"the stream ring carried {sdoc.get('data')!r} — the validated "
        "dump is the record in BOTH channels"
    )


# ── THE DOORS: the buffer private, the public op validating, the CHECK ──


def test_the_buffer_door_is_private_the_public_op_validates() -> None:
    """THE STRUCTURAL PIN: ``ProgressEmitter.submit`` no longer exists as
    a public unvalidated door — the buffer write is ``_submit`` (private
    by law) and the PUBLIC op (:meth:`emit`) validates. The convicted
    hole: a public ``submit`` that validated nothing admitted
    ``pct=999`` to the buffer (and it persisted — the storage CHECK was
    a comment's lie)."""
    assert not hasattr(ProgressEmitter, "submit"), (
        "ProgressEmitter.submit is public again — the unvalidated buffer "
        "door (the pct=999 hole) is re-opened: the buffer write is "
        "_submit, the public op is emit and it validates"
    )
    assert hasattr(ProgressEmitter, "_submit")
    assert hasattr(ProgressEmitter, "emit")


async def test_the_public_op_refuses_pct_out_of_domain(wf_pool: Any, wf_sql: Any) -> None:
    """The public op's own gate: ``pct=999`` (and ``-1``) is the typed
    refusal BEFORE the buffer — the emission the gate never saw must
    never reach the record."""
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=JobId(new_uuid()), node_id=JobId(new_uuid()))
    for bad in (999, -1, True, 50.5):
        with pytest.raises(ProgressRefusedError):
            await emitter.emit(bad, None, None)  # pyright: ignore[reportArgumentType]
    assert emitter._pending is None, "a refused emission reached the buffer"


async def test_the_pct_domain_check_is_real(wf_conn: asyncpg.Connection, wf_schema: str) -> None:
    """THE MIGRATION'S PROMISE SHIPPED (01.00.28's comment claimed the
    column was the storage-domain twin of the 0..100 bound; smallint's
    domain is ±32767): the CHECK constraint ``wf_node_progress_pct_domain``
    refuses a raw out-of-domain pct at the STORAGE domain — the
    bypassing writer (a future writer bug, a manual INSERT) is refused
    by the row's own table, never by review."""
    from tests._wf_fixtures import seed_running_node

    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    with pytest.raises(asyncpg.CheckViolationError):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_node_progress '
            "(node_id, channel, pct) VALUES ($1, 'progress', 999)",
            node,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_node_progress '
            "(node_id, channel, pct) VALUES ($1, 'progress', -1)",
            node,
        )
    # The legal domain passes (the bounds inclusive; NULL carries).
    for pct in (0, 100, None):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_node_progress '
            "(node_id, channel, pct, updated_at) VALUES ($1, $3, $2, clock_timestamp())",
            node,
            pct,
            f"probe-{pct}",
        )


# ── THE AUTO-CLASS DROP COUNT IS ON THE RECORD ──────────────────────────


async def test_the_auto_class_drop_count_is_on_the_record(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: Any,
    wf_pool: Any,
    progress_redlog: RedLog,
) -> None:
    """THE DISCARDED RECEIPT, pinned shut: the auto projection's appends
    past the ring bound are COUNTED on the node's STREAM-channel counters
    row (occurrences == the projections, dropped == the trims — the
    honest appended == retained + dropped pair, BOTH classes). RED (the
    pre-cure tree): the drop count was discarded — the auto class had no
    receipt."""
    from tests._wf_fixtures import seed_running_node

    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    projections = PROGRESS_RING_BOUND + 6  # 6 trims past the bound
    for i in range(projections):
        from taskq.workflows._progress import project_auto_event

        await project_auto_event(
            wf_pool,
            wf_sql,
            flow_id=flow,
            node_id=node,
            kind="wf.node.started",
            payload={"n": i},
        )
    counters = await wf_conn.fetchrow(
        f"""SELECT occurrences, dropped FROM "{wf_schema}".wf_node_progress
            WHERE node_id = $1 AND channel = '{STREAM_CHANNEL}'""",
        node,
    )
    progress_redlog.red(
        "t21-auto-class-drop-count",
        "the auto projection discarding the append's drop count — the "
        "honest emitted-vs-delivered pair was user-class-only",
        {
            "occurrences": counters["occurrences"] if counters else None,
            "dropped": counters["dropped"] if counters else None,
        },
    )
    assert counters is not None, "the auto class has NO counters row (the receipt discarded)"
    assert int(counters["occurrences"]) == projections
    assert int(counters["dropped"]) == projections - PROGRESS_RING_BOUND, (
        f"the auto class's drop count is not on the record: "
        f"{counters['dropped']} — the honest pair broken for the engine's "
        "own class"
    )


# ── THE FAILURE-CLASS ROUTING RULE (pinned per class) ───────────────────


async def _routed_node(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: Any,
    flow_id: JobId,
    *,
    step_key: str = "routed",
) -> tuple[FlowRunner, dict[str, Any]]:
    """A claimed node row + the runner, for the ladder's own call shape
    (the ledger claim first — the ladder's terminal write updates the
    claim's OWN row, the arbiter's key)."""
    from tests._wf_fixtures import claim_view, seed_running_node

    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key=step_key)
    worker, attempt, _epoch = await claim_view(wf_conn, wf_schema, node_id)
    from taskq.workflows.ledger import claim_step_ledger

    async with wf_pool.acquire() as conn:
        await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow_id,
            job_id=node_id,
            step_key=step_key,
            map_index=None,
            attempt=attempt,
        )
    runner = FlowRunner.__new__(FlowRunner)
    runner.pool = wf_pool  # type: ignore[attr-defined]  # Why: the pin drives the LADDER's own route (the mixin's host fields), not the driver loop.
    runner.wsql = wf_sql  # type: ignore[attr-defined]
    runner.schema = wf_schema  # type: ignore[attr-defined]
    runner._worker_id = worker  # type: ignore[attr-defined]
    runner.compiled = type("C", (), {"capture": "none", "name": "routing-pin"})()  # type: ignore[attr-defined]
    row = {"id": str(node_id), "step_key": step_key, "map_index": None}
    return runner, row


async def _terminal_shape(
    wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId, step_key: str
) -> tuple[str, str | None, int, int]:
    """(node status, error_class, attempt, ledger failed rows)."""
    rec = await wf_conn.fetchrow(
        f"""SELECT status::text, error_class, attempt FROM "{wf_schema}".jobs
            WHERE step_key = $1 AND (metadata->>'flow_id')::uuid = $2::uuid""",
        step_key,
        flow_id,
    )
    assert rec is not None
    failed_rows = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = $2 AND status = 'failed'""",
        flow_id,
        step_key,
    )
    return rec["status"], rec["error_class"], int(rec["attempt"]), int(failed_rows or 0)


class _FakeNode:
    max_attempts = 3
    retry_kind = "transient"


async def test_routing_progress_refused_is_the_named_terminal_not_a_flake(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: Any,
    progress_redlog: RedLog,
) -> None:
    """THE ROUTING RULE, deterministic leg (pinned per class): a
    ``ProgressRefusedError`` — a DETERMINISTIC authoring bug — finalizes
    the node on the FIRST attempt with the named class; the ladder is
    unburned (no re-pends, ONE ledger failed row). RED (the pre-cure
    tree): the deterministic death laddered as transient — three
    identical failures pretending to be a flake, the retry budget spent
    on a bug no retry can cure."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    runner, row = await _routed_node(wf_conn, wf_schema, wf_pool, wf_sql, flow_id)

    exc = ProgressRefusedError("pct must be int 0..100, got 999")
    await runner._ladder_or_fail(flow_id, row, 1, _FakeNode(), exc)  # pyright: ignore[reportPrivateUsage,reportUnknownArgumentType]  # Why: the routing pin drives the ladder's own method — the runner's real record is the subject.

    status, error_class, attempt, failed_rows = await _terminal_shape(
        wf_conn, wf_schema, flow_id, row["step_key"]
    )
    progress_redlog.red(
        "t21-routing-deterministic",
        "the DETERMINISTIC authoring failure laddered as transient (the "
        "retry budget burned on a bug no retry cures)",
        {
            "status": status,
            "error_class": error_class,
            "attempt": attempt,
            "ledger_failed_rows": failed_rows,
        },
    )
    assert (status, error_class) == ("failed", "ProgressRefusedError"), (
        f"the deterministic death did not terminalize named: {status}/{error_class}"
    )
    assert attempt == 1, "the ladder BURNED on a deterministic authoring bug (re-pends ran)"
    assert failed_rows == 1, "the deterministic death wrote more than its one ledger row"


async def test_routing_page_diverged_is_the_named_terminal(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: Any,
) -> None:
    """THE ROUTING RULE, PageDivergedError leg: the divergent page (the
    cursor vs the row history) is a DETERMINISTIC death — the named
    terminal on the first attempt, the ladder unburned. The raw
    UniqueViolationError used to escape, ladder, re-collide, and burn."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    runner, row = await _routed_node(wf_conn, wf_schema, wf_pool, wf_sql, flow_id)

    from taskq.workflows._emit import PageDivergedError

    exc = PageDivergedError(JobId(row["id"]), JobId(row["id"]), "the drill")
    await runner._ladder_or_fail(flow_id, row, 1, _FakeNode(), exc)  # pyright: ignore[reportPrivateUsage,reportUnknownArgumentType]

    status, error_class, attempt, failed_rows = await _terminal_shape(
        wf_conn, wf_schema, flow_id, row["step_key"]
    )
    assert (status, error_class) == ("failed", "PageDivergedError")
    assert attempt == 1, "the divergent page laddered (the re-collide burn)"
    assert failed_rows == 1


async def test_routing_map_index_exhausted_is_the_named_terminal(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: Any,
) -> None:
    """THE ROUTING RULE, MapIndexExhaustedError leg: the smallint ceiling
    is a DETERMINISTIC death — the named terminal, the ladder unburned
    (re-running cannot grow a record's identity past the column)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    runner, row = await _routed_node(wf_conn, wf_schema, wf_pool, wf_sql, flow_id)

    from taskq.workflows._emit import MapIndexExhaustedError

    exc = MapIndexExhaustedError("record #32768 is past the ceiling")
    await runner._ladder_or_fail(flow_id, row, 1, _FakeNode(), exc)  # pyright: ignore[reportPrivateUsage,reportUnknownArgumentType]

    status, error_class, attempt, failed_rows = await _terminal_shape(
        wf_conn, wf_schema, flow_id, row["step_key"]
    )
    assert (status, error_class) == ("failed", "MapIndexExhaustedError")
    assert attempt == 1
    assert failed_rows == 1


async def test_routing_transient_body_failure_still_ladders(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: Any,
) -> None:
    """THE ROUTING RULE, transient leg: an ordinary body failure still
    rides the ladder (the re-pend, no terminal) — the rule's cure must
    not eat the retry curve it exists to protect."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    runner, row = await _routed_node(wf_conn, wf_schema, wf_pool, wf_sql, flow_id)

    await runner._ladder_or_fail(flow_id, row, 1, _FakeNode(), RuntimeError("boom"))  # pyright: ignore[reportPrivateUsage,reportUnknownArgumentType]

    status, error_class, _attempt, failed_rows = await _terminal_shape(
        wf_conn, wf_schema, flow_id, row["step_key"]
    )
    assert status == "scheduled", "the transient failure did not re-pend (the ladder eaten)"
    assert error_class is None, "a laddered attempt emitted a terminal (P3 rule 7)"
    assert failed_rows == 1, "the attempt's own ledger row is the only failure record"


# ── THE TYPED DEATHS: the poison record kills ITSELF, never its mates ───


async def test_map_index_poison_record_its_mates_commit(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: Any,
    t20_redlog: RedLog,
) -> None:
    """RECORD #32768'S CURE (the batch semantics): a page carrying a
    poison record (map_index past the smallint ceiling) emits the VALID
    page-mates — their tx COMMITS, the valid work survives on the
    record — then raises :class:`MapIndexExhaustedError` NAMING the
    offender. RED (the pre-cure tree): the raw DataError escaped
    MID-TX and the whole page rolled back — the valid mates died with
    the poison record (the accidental untyped ceiling)."""
    from taskq.workflows._emit import MapIndexExhaustedError, emit_batch
    from tests._wf_fixtures import claim_view, seed_running_node

    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow)
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)

    children = [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": i}},
            trace_id=f"app-{i}",
            map_index=map_index,
        )
        for i, map_index in enumerate((0, 1, 32768))
    ]
    with pytest.raises(MapIndexExhaustedError, match="32768"):
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=children,
            cursor={"page": 0},
        )
    # THE MATES SURVIVE: the valid records are ON THE RECORD.
    mates = await wf_conn.fetch(
        f"""SELECT map_index FROM "{wf_schema}".jobs
            WHERE parent_id = $1 AND step_key = 'screen' ORDER BY map_index""",
        source,
    )
    t20_redlog.red(
        "t20-poison-record-mates",
        "the smallint ceiling as the raw DataError MID-TX: the whole page "
        "rolled back — the valid page-mates died with the poison record",
        {"committed_mates": [r["map_index"] for r in mates]},
    )
    assert [r["map_index"] for r in mates] == [0, 1], (
        f"the valid page-mates did not survive the poison record: "
        f"{[r['map_index'] for r in mates]} — a poison record killed its "
        "mates (the batch semantics broken)"
    )
    # The cursor checkpoint COMMITTED with the mates (the atomic page).
    cursor_raw = await wf_conn.fetchval(
        f"""SELECT metadata->'emit_cursor' FROM "{wf_schema}".jobs WHERE id = $1""",
        source,
    )
    doc = json.loads(cursor_raw) if isinstance(cursor_raw, str) else cursor_raw
    assert doc == {"page": 0}, "the mates' page did not checkpoint"


async def test_map_index_all_poison_page_refused_before_work(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: Any,
) -> None:
    """An ALL-poison page is refused BEFORE any work — nothing written,
    the ceiling named, no tx opened."""
    from taskq.workflows._emit import MapIndexExhaustedError, emit_batch
    from tests._wf_fixtures import claim_view, seed_running_node

    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow)
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)

    children = [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": i}},
            trace_id=f"app-{i}",
            map_index=32768 + i,
        )
        for i in range(2)
    ]
    with pytest.raises(MapIndexExhaustedError, match="nothing was written"):
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=children,
            cursor={"page": 0},
        )
    rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1', source
    )
    assert int(rows or 0) == 0, "an all-poison page wrote rows"
