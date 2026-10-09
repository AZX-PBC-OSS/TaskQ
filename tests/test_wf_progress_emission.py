"""T21 — THE EMISSION OP PINS: ``ctx.progress``, the coalesce, the
asymmetry.

The provenance: the PoC's PROOF (PROOF 1/2/4) — the chain, the fan-out,
the chatty body — ported onto the BUILT surface. Each pin's CONVICTED
VARIANT is named; the unfenced variants stay RED FOREVER (the redlog).

THE LAW (T21 decision e): observability degrades FIRST, never
correctness. The emission is best-effort — never in the finalize path
(the zero-finalize-changes probe, ``test_wf_progress_persistence.py``),
never raised into the body on infrastructure failure, never blocking the
node's terminal.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows._progress import (
    CLASS_AUTO,
    CLASS_USER,
    KIND_NODE_STARTED,
    PROGRESS_RING_BOUND,
    STREAM_CHANNEL,
    ProgressEmitter,
    ProgressRefusedError,
)
from tests._wf_fixtures import RedLog

pytestmark = pytest.mark.integration


class Page(BaseModel):
    page: int


class Ingest(BaseModel):
    doc_id: str


# ── THE CHAIN (PROOF 1): the body's emission → the rows → the replay ────


async def test_the_chain_body_emission_lands_in_both_channels(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """THE CHAIN, end-to-end on the built runner: a body's
    ``ctx.progress`` calls land in the STREAM ring (class='user') AND the
    STATE channel (latest-wins), and the engine's auto projections
    (started + terminal) interleave in the SAME seq space — ONE stream,
    ONE cursor. The occurrence counter is HONEST: it counts every
    emission, coalesced or not."""
    app = WorkflowApp()
    seen: dict[str, Any] = {}

    async def work(ctx: StepContext, params: Ingest) -> dict[str, object]:
        await ctx.progress(10, "starting", None)
        for i in range(5):
            await ctx.progress(20 + i * 10, f"page {i}", {"page": i})
        seen["emitted"] = 6
        return {"ok": True}

    @app.workflow("t21_chain")
    def t21_chain() -> object:
        return build(step(work, Ingest(doc_id="d1"), key="work", progress_schema=Page))

    runner = FlowRunner(app.get("t21_chain"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)

    node = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs WHERE step_key = 'work'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    state = await wf_conn.fetchrow(
        f"""SELECT pct, message, data, occurrences FROM "{wf_schema}".wf_node_progress
            WHERE node_id = $1 AND channel = 'progress'""",
        node,
    )
    assert state is not None
    assert state["pct"] == 60 and state["message"] == "page 4"
    assert state["occurrences"] == 6
    data = state["data"]
    assert (json.loads(data) if isinstance(data, str) else data) == {"page": 4}

    stream = await wf_conn.fetch(
        f"""SELECT seq, class, kind, payload FROM "{wf_schema}".wf_node_stream
            WHERE node_id = $1 ORDER BY seq""",
        node,
    )
    kinds = [(r["class"], r["kind"]) for r in stream]
    # the auto STARTED first, the user emissions, the auto TERMINAL last —
    # ONE interleaved range. The STREAM ring receives the COALESCED
    # snapshots (one per flush — the cadence's, not the emissions'), while
    # the occurrence counter keeps the honest total (6, above): the
    # emitted-vs-delivered pair is the design, never a loss hidden in the
    # count.
    assert kinds[0] == (CLASS_AUTO, KIND_NODE_STARTED)
    assert kinds[-1] == (CLASS_AUTO, "wf.node.terminal")
    user_kinds = [k for c, k in kinds if c == CLASS_USER]
    assert 1 <= len(user_kinds) < 6, "the ring coalesced into a per-emission log"
    assert set(user_kinds) == {"progress"}
    seqs = [r["seq"] for r in stream]
    assert seqs == sorted(seqs)


async def test_declared_schema_door_refuses_wrong_shape(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """THE TYPED DOOR (decision a — the TypedGate-door pattern): a node
    that declared ``progress_schema=Page`` refuses a wrong-shaped data
    emission with :class:`ProgressRefusedError` — raised INTO the body,
    which is the authoring error's place. The refusal ladders like any
    body failure (the node's correctness path owns it)."""
    app = WorkflowApp()

    async def lying_body(ctx: StepContext, params: Ingest) -> dict[str, object]:
        await ctx.progress(50, "m", {"not_a_page": True})
        return {"ok": True}

    @app.workflow("t21_lying")
    def t21_lying() -> object:
        return build(step(lying_body, Ingest(doc_id="d1"), key="lying", progress_schema=Page))

    runner = FlowRunner(app.get("t21_lying"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    status = await wf_conn.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".jobs WHERE step_key = 'lying'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    assert status == "failed", "a wrong-shaped emission passed the declared-schema door"
    error_class = await wf_conn.fetchval(
        f"""SELECT error_class FROM "{wf_schema}".jobs WHERE step_key = 'lying'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    assert error_class == "ProgressRefusedError"


async def test_typed_gates_pct_and_data(
    wf_pool: asyncpg.Pool,
    wf_sql: Any,
    wf_schema: str,
    progress_redlog: RedLog,
) -> None:
    """The typed gate's other refusals, unit-shaped: a non-int pct, an
    out-of-range pct, a non-dict data — each REFUSED (the convicted
    variant — silent coercion — is the lie class: a str pct stores as a
    string in the row and the two progress surfaces diverge)."""
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=JobId(new_uuid()), node_id=JobId(new_uuid()))
    for pct in ("50", 50.0, True, -1, 101):
        with pytest.raises(ProgressRefusedError):
            await emitter.emit(pct, None, None)  # pyright: ignore[reportArgumentType]
    with pytest.raises(ProgressRefusedError):
        await emitter.emit(None, None, ["not", "a", "dict"])  # pyright: ignore[reportArgumentType]
    assert emitter.emitted == 0, "a refused emission must not count as emitted"


async def test_data_cap_truncates_with_the_marker(wf_pool: asyncpg.Pool, wf_sql: Any) -> None:
    """T18's D5 shape on the emission op: oversize ``data`` truncated WITH
    the ``__truncated__`` marker — never silently (the reader must know
    it read a truncation)."""
    from taskq.workflows._progress import DATA_MAX_BYTES

    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=JobId(new_uuid()), node_id=JobId(new_uuid()))
    big = {"blob": "x" * (DATA_MAX_BYTES * 2)}
    await emitter.emit(50, None, big)
    pending = emitter._pending  # pyright: ignore[reportPrivateUsage]
    assert pending is not None
    assert "__truncated__" in pending["data"]
    assert pending["data"]["__truncated__"] >= DATA_MAX_BYTES
    await emitter.aclose()  # the flush task must not outlive the test


# ── THE COALESCE (PROOF 4): the chatty body held at the cadence ─────────


async def test_chatty_body_coalesce_cadence_and_constant_rows(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    progress_redlog: RedLog,
) -> None:
    """THE CHATTY BODY on the BUILT op (the PoC's 10k-emission proof,
    re-proven): 10,000 emissions in a tight loop →
    * the flush count is the CADENCE's, not the emissions' (the coalesce
      holds; the storm shape — a write per emission — is the convicted
      variant, kept red in the redlog as the PoC's measured 2,000-row
      counterfactual);
    * ``occurrences == 10_000`` EXACTLY (the counter is honest);
    * the STATE channel stays at 2 rows (channels progress + __stream__)
      — CONSTANT under 10k (DH1's fence, PoC P4/P6)."""
    app = WorkflowApp()

    async def chatty(ctx: StepContext, params: Ingest) -> dict[str, object]:
        for i in range(10_000):
            await ctx.progress(i % 101, f"e{i}", None)
        return {"ok": True}

    @app.workflow("t21_chatty")
    def t21_chatty() -> object:
        return build(step(chatty, Ingest(doc_id="d1"), key="chatty"))

    runner = FlowRunner(app.get("t21_chatty"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    status = await runner.drive(flow_id)
    assert status == "terminal"

    node = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs WHERE step_key = 'chatty'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    state = await wf_conn.fetchrow(
        f"""SELECT occurrences, last_seq FROM "{wf_schema}".wf_node_progress
            WHERE node_id = $1 AND channel = 'progress'""",
        node,
    )
    assert state is not None
    assert state["occurrences"] == 10_000, "the occurrence counter lied"
    rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_progress WHERE node_id = $1', node
    )
    assert rows == 2, (
        f"the STATE channel grew to {rows} rows under 10k emissions — "
        "DH1's unbounded-history dragon is alive"
    )
    ring = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', node
    )
    assert ring <= PROGRESS_RING_BOUND, "the ring outgrew its bound"
    progress_redlog.red(
        "chatty_body_coalesce_cadence",
        "the write-per-emission counterfactual (the PoC's measured red: "
        "2,000 emissions → 2,000 rows + the body degraded to 49-197/s)",
        {
            "emissions": 10_000,
            "state_writes": "coalesced at the cadence (see the occurrence counter)",
            "state_rows": rows,
            "ring_rows": ring,
            "verdict": "the coalesce holds; the convicted variant stays red",
        },
    )


async def test_flush_failure_never_fails_the_body(wf_sql: Any, wf_schema: str) -> None:
    """THE ASYMMETRY (decision e), unit-shaped: a flush that CANNOT write
    (the pool is broken) is counted — ``write_errors`` — and NEVER raised
    into the body; the first loss is WARNED (the loudness half — the
    PoC's red-team note: a systematically failing flush must not look
    healthy). The all-dropped node still terminalizes: correctness is
    unaffected."""
    from taskq.workflows._progress import ProgressEmitter

    class BrokenPool:
        async def acquire(self) -> Any:
            raise ConnectionError("the pool is broken (the simulated outage)")

    emitter = ProgressEmitter(
        BrokenPool(),  # pyright: ignore[reportArgumentType]
        wf_sql,
        flow_id=JobId(new_uuid()),
        node_id=JobId(new_uuid()),
        cadence_s=0.01,
    )
    await emitter.emit(50, "m", None)
    await emitter.emit(60, "m2", None)
    await emitter.aclose()  # must NOT raise
    assert emitter.write_errors >= 1
    assert emitter.emitted == 2
    assert emitter.appended == 0


async def test_emission_disabled_counts_and_writes_nothing(
    wf_pool: asyncpg.Pool, wf_sql: Any
) -> None:
    """The best-effort proof's cleanest shape (the PoC's all-dropped
    variant): a disabled emitter counts every emission honestly and
    writes NOTHING — and the node's own path is unaffected (the emission
    was never in it)."""
    emitter = ProgressEmitter(
        wf_pool,
        wf_sql,
        flow_id=JobId(new_uuid()),
        node_id=JobId(new_uuid()),
        enabled=False,
    )
    for i in range(500):
        await emitter.emit(i % 101, f"e{i}", None)
    assert emitter.emitted == 500
    assert emitter.flushes == 0 and emitter.appended == 0
    await emitter.aclose()
    assert emitter.write_errors == 0


async def test_drop_counter_on_the_record(
    wf_pool: asyncpg.Pool, wf_sql: Any, wf_schema: str
) -> None:
    """DH2's honest pair, on the BUILT emitter: a small ring + a chatty
    body → the __stream__ counters row accumulates the dropped count and
    appended == retained + dropped (a dropped row that vanished uncounted
    would be the honest-face lie)."""
    emitter = ProgressEmitter(
        wf_pool,
        wf_sql,
        flow_id=JobId(new_uuid()),
        node_id=JobId(new_uuid()),
        ring_bound=8,
        cadence_s=0.01,
    )
    for i in range(200):
        await emitter.emit(i % 101, f"e{i}", None)
    await emitter.aclose()
    assert emitter.appended > 0
    assert emitter.appended <= 8 + emitter.dropped_total + 1, "the accounting leaked"
    retained = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1',
        emitter._node_id,  # pyright: ignore[reportPrivateUsage]
    )
    assert retained <= 8
    counters = await wf_pool.fetchrow(
        f"""SELECT dropped, occurrences FROM "{wf_schema}".wf_node_progress
            WHERE node_id = $1 AND channel = '{STREAM_CHANNEL}'""",
        emitter._node_id,  # pyright: ignore[reportPrivateUsage]
    )
    assert counters is not None
    assert counters["dropped"] == emitter.dropped_total, (
        "the on-record drop count disagrees with the emitter's — the honest pair broken"
    )


async def test_terminal_projection_lands_after_the_finalize(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE PROJECTION'S ORDER (the display-fence's substrate): the auto
    TERMINAL event lands only AFTER the finalize applied (the ledger owns
    the state; the projection publishes it) — and a FAILING node's
    terminal carries the outcome, so the display can derive the failure
    from the stream too (the ledger remains the authority)."""
    app = WorkflowApp()

    async def boom(ctx: StepContext, params: Ingest) -> dict[str, object]:
        await ctx.progress(99, "almost done", None)
        raise ValueError("the body failed at 99%")

    @app.workflow("t21_boom")
    def t21_boom() -> object:
        return build(step(boom, Ingest(doc_id="d1"), key="boom", max_attempts=1))

    runner = FlowRunner(app.get("t21_boom"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)

    node = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs WHERE step_key = 'boom'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', node
    )
    assert status == "failed"
    # THE LIE'S SUBSTRATE, on the record: the node's last progress said
    # 99% — and the STATE row keeps it (advisory), while the LEDGER says
    # failed. The display pins (the faces file) convict the liar that
    # derives the state from HERE.
    state = await wf_conn.fetchrow(
        f"""SELECT pct, message FROM "{wf_schema}".wf_node_progress
            WHERE node_id = $1 AND channel = 'progress'""",
        node,
    )
    assert state is not None and state["pct"] == 99 and state["message"] == "almost done"
    terminal = await wf_conn.fetchval(
        f"""SELECT payload FROM "{wf_schema}".wf_node_stream
            WHERE node_id = $1 AND class = '{CLASS_AUTO}'
            AND kind = 'wf.node.terminal' ORDER BY seq DESC LIMIT 1""",
        node,
    )
    payload = terminal if isinstance(terminal, dict) else json.loads(terminal or "{}")
    assert payload["outcome"] == "failed"
    assert payload["error_class"] == "ValueError"
