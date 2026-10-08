"""The workflow progress READ surfaces (T21): the faces + the aggregation
— every face a VIEW over the same rows, no face owns state (decision f).

THE FACES (decision f):
* :func:`progress_sse_face` — the SSE face: the seq-cursor replay over
  the ONE stream, with the NAMED partial mode (the reconnect-after-prune)
  + the state channel's re-sync payload. THE CONNECT SHAPE (the DH6 law
  applied at EVERY connect, not only after prune): the consumer loads the
  display from the STATE channel + the LEDGER first, then applies the
  replayed tail — latest-wins needs no history.
* :func:`rebuild_display` — the run explorer's display model, the PURE
  function over (ledger rows, state rows, replayed events). THE
  PROGRESS-LIE FENCE (DH4/DH7): the node's STATUS renders from the LEDGER
  rows — the user progress renders INSIDE the terminal state, never over
  it; the progress row is ADVISORY. THE LEDGER WINS: the fold's last word
  on status is the ledger row's, so a projection that missed it cannot
  lie.
* :func:`map_progress_line` — the map's progress: the certified grouped
  read (T08's law — "417/1000 · 3 retrying · 580 blocked" IS the map's
  progress), the state channel's avg pct riding the same read.
* :func:`read_map_aggregate` — the user's DECLARED ``aggregate=`` fn: a
  pure, READ-SIDE fn evaluated AT READ TIME over the children's RESULT
  rows (decision c — DH8's fence: the join node is for DATAFLOW; progress
  aggregation is OBSERVABILITY, never a blocking node). Writes nothing.

THE GAUGE'S LAW (DH5 — the metrics doctrine): per-child progress lives in
THESE ROWS, read on demand — it is NEVER a metric label series (the
workflow gauge stays workflow-dimensioned; the pin convicts the
per-child-label counterfactual).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

import asyncpg

from taskq._json import loads as _loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows._progress import CLASS_USER, KIND_PROGRESS
from taskq.workflows._sql import WorkflowSql

__all__ = [
    "AggregateRead",
    "map_progress_line",
    "progress_sse_face",
    "progress_stream_generator",
    "read_map_aggregate",
    "rebuild_display",
    "run_display",
]


# ── THE SSE FACE (decision f) ────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ReplayFace:
    """One replay read's outcome.

    * ``mode='full'`` — the cursor is inside the retained window; the
      replay alone reconstructs the display.
    * ``mode='partial'`` — NAMED (DH6): the cursor predates the ring's
      oldest retained seq (the window was pruned under it). The partial
      tail is served AND the state channel's latest-wins rows ride along
      as the re-sync payload — NEVER a silent empty-success (the
      silent-gap shape — full + zero events over a pruned window — is
      structurally impossible: a pruned-past cursor NAMES the mode).
    """

    mode: str
    cursor: int
    oldest_retained: int | None
    events: list[dict[str, Any]]
    state_sync: list[dict[str, Any]]


async def progress_sse_face(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId | None = None,
    node_id: JobId | None = None,
    last_event_id: int = 0,
    limit: int = 5000,
) -> ReplayFace:
    """The SSE face's read: the seq-cursor replay, node- or run-scoped
    (exactly one scope binds). See :class:`ReplayFace` for the modes."""
    assert (flow_id is None) != (node_id is None), "scope: exactly one of flow/node"
    async with pool.acquire() as conn:
        if node_id is not None:
            events = await conn.fetch(wsql.progress_replay_node, last_event_id, node_id, limit)
            oldest: int | None = await conn.fetchval(wsql.progress_ring_oldest_node, node_id)
        else:
            events = await conn.fetch(wsql.progress_replay_run, last_event_id, flow_id, limit)
            oldest = await conn.fetchval(wsql.progress_ring_oldest_run, flow_id)
    pruned_past_cursor = last_event_id > 0 and oldest is not None and oldest > last_event_id + 1
    state_sync: list[dict[str, Any]] = []
    if pruned_past_cursor:
        async with pool.acquire() as conn:
            if node_id is not None:
                rows = await conn.fetch(wsql.progress_state_read_node, node_id)
            else:
                rows = await conn.fetch(wsql.progress_state_read_run, flow_id)
        state_sync = [dict(r) for r in rows]
    return ReplayFace(
        mode="partial" if pruned_past_cursor else "full",
        cursor=last_event_id,
        oldest_retained=int(oldest) if oldest is not None else None,
        events=[dict(r) for r in events],
        state_sync=state_sync,
    )


# ── THE DISPLAY MODEL (the progress-lie fence, DH4/DH7) ─────────────────


def rebuild_display(
    ledger_rows: list[dict[str, Any]],
    state_rows: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """The run explorer's display model, reconstructed FROM THE ROWS — the
    PURE function (the connect shape and the replay fold are THIS).

    THE FENCE (DH4/DH7): the node's STATUS renders from the LEDGER rows —
    the user progress renders INSIDE whatever the state is; it can never
    flip a terminal state back to running. The progress row is ADVISORY.

    * ``ledger_rows`` — the LEDGER read (the authority: the jobs rows).
    * ``state_rows`` — the STATE channel's latest-wins backfill (the
      pruned window's cure — latest-wins needs no history).
    * ``events`` — the replayed stream (the freshness), folded in SEQ
      ORDER (the seq-cursor law); the LEDGER WINS last: where a ledger
      row exists its status is the final word, so a projection that
      missed it cannot lie.
    """
    disp: dict[str, dict[str, Any]] = {}
    for r in ledger_rows:
        disp[str(r["id"])] = {
            "status": r["status"],
            "pct": None,
            "message": None,
            "error_class": r.get("error_class"),
        }
    # The STATE channel's latest-wins backfill (the pruned window's cure).
    for s in state_rows:
        if s["channel"] != "progress":
            continue
        d = disp.setdefault(
            str(s["node_id"]),
            {"status": None, "pct": None, "message": None, "error_class": None},
        )
        if d["pct"] is None:
            d["pct"] = s["pct"]
            d["message"] = s["message"]
    # The replay, in SEQ ORDER — the seq-cursor law.
    for e in sorted(events, key=lambda e: int(e["seq"])):
        d = disp.setdefault(
            str(e["node_id"]),
            {"status": None, "pct": None, "message": None, "error_class": None},
        )
        p = e["payload"]
        if isinstance(p, str):
            import json

            p = json.loads(p)
        if e["class"] == CLASS_USER and e["kind"] == KIND_PROGRESS:
            # Progress renders INSIDE whatever the state is — the LIE's
            # fence: an emission can never flip a terminal state back to
            # running (the fold writes pct/message only).
            d["pct"] = p.get("pct", d["pct"])
            d["message"] = p.get("message", d["message"])
        elif e["class"] == "auto" and e["kind"] == "wf.node.terminal":
            d["status"] = p.get("outcome", d["status"])
    # THE LEDGER WINS (the fence's teeth): where a ledger row exists its
    # status is the final word — a projection that missed it cannot lie.
    for r in ledger_rows:
        if str(r["id"]) in disp:
            disp[str(r["id"])]["status"] = r["status"]
    return disp


async def run_display(
    pool: asyncpg.Pool, wsql: WorkflowSql, flow_id: JobId
) -> dict[str, dict[str, Any]]:
    """The run explorer's display READ (the connect shape): the LEDGER +
    the STATE channel — the replayed tail applies on top via
    :func:`rebuild_display` as the SSE delivers it. THE DISPLAY'S STATE IS
    LEDGER-DERIVED AT EVERY CONNECT (the DH7 law) — a stale progress row
    costs freshness, never the state."""
    async with pool.acquire() as conn:
        ledger_rows = await conn.fetch(wsql.workflow_nodes, flow_id)
        state_rows = await conn.fetch(wsql.progress_state_read_run, flow_id)
    return rebuild_display([dict(r) for r in ledger_rows], [dict(r) for r in state_rows], [])


# ── THE AGGREGATION (decision c — derived at read, DH8's fence) ──────────


@dataclass(frozen=True, slots=True)
class AggregateRead:
    """One read-side aggregate's outcome (the PoC's measured shape — the
    fn's time is ON THE RECORD: a read-side user fn runs user code at
    read time, bounded by the result cap; the GIL/short-input discipline
    is the docs' note)."""

    value: object
    children: int
    read_ms: float
    fn_ms: float


async def read_map_aggregate(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    flow_id: JobId,
    parent_id: JobId,
    fn: Callable[[list[Any]], object] | None = None,
) -> AggregateRead:
    """The user's DECLARED ``aggregate=`` fn (or the explicit *fn*), over
    the map's children's RESULT rows — AT READ TIME, writing NOTHING (the
    PoC's proof: row counts identical before/after).

    The declared fn resolves from the REGISTERED DEFINITION (the durable
    leg — the flow root's stamped workflow name, the same doctrine the
    fired join's body uses); an explicit *fn* is the read-side caller's
    own door. A map with no declared aggregate and no explicit fn reads
    the raw rows' count only (``value`` is the rows)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(wsql.progress_child_results, parent_id)
        if fn is None:
            fn = await _resolve_declared_aggregate(conn, wsql, flow_id, parent_id)
    # THE MAP'S ITEMS ARE THE INPUT, never the map's own auto-join: the
    # join row rides the same parent and its result IS the collected list
    # (the items again — doubly counted). The items' step key is the
    # parent's own ``<parent>.item``; the fork's join is the parent's
    # ``<parent>.join`` — excluded BY NAME (the runner's fork names them).
    item_keys = {r["step_key"] for r in rows if r["step_key"].endswith(".item")}
    parents = {k[: -len(".item")] for k in item_keys}
    rows = [
        r for r in rows if r["step_key"].endswith(".item") or r["step_key"] not in {
            p + ".join" for p in parents
        }
    ]
    read_ms = time.perf_counter() * 1000
    results: list[Any] = []
    for r in rows:
        # the runner's own envelope ({"value": …}) unwraps — the fn sees
        # EXACTLY what the body returned, decoded once (cut #14's shape)
        raw: Any = _loads(r["result"])
        assert isinstance(raw, dict)  # the Any-contract walk: the envelope's shape
        envelope: dict[str, Any] = {str(k): v for k, v in raw.items()}  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]  # Why: the Any-contract walk's boundary — the parse hands back Unknown members; the assert above is the runtime guard.
        if "value" in envelope and len(envelope) == 1:
            raw = envelope["value"]
        results.append(raw)
    value: object = results
    fn_ms = 0.0
    if fn is not None:
        t0 = time.perf_counter()
        value = fn(results)
        fn_ms = (time.perf_counter() - t0) * 1000
    return AggregateRead(value=value, children=len(results), read_ms=read_ms, fn_ms=fn_ms)


async def _resolve_declared_aggregate(
    conn: ConnLike,
    wsql: WorkflowSql,
    flow_id: JobId,
    parent_id: JobId,
) -> Callable[[list[Any]], object] | None:
    """The declared aggregate's DURABLE resolution: the flow root's
    stamped workflow name → the REGISTERED DEFINITION's ``aggregates`` →
    the join step key. ``None`` when undeclared (the read returns the raw
    rows) or unresolvable (a cold registry — the loudness doctrine lives
    in the caller's surfaces, the value never lies: it IS the rows)."""
    root = await conn.fetchval(
        f"""SELECT metadata->>'workflow' FROM {wsql.schema}.jobs
            WHERE id = $1::uuid AND step_key = '__flow__'""",
        flow_id,
    )
    if not root:
        return None
    join_key = await conn.fetchval(
        f"SELECT step_key FROM {wsql.schema}.jobs WHERE id = $1::uuid", parent_id
    )
    from taskq.workflows.definitions import get_registry

    try:
        definition = get_registry().get(root)
    except KeyError:
        return None
    return definition.aggregates.get(join_key or "")


async def map_progress_line(
    pool: asyncpg.Pool, wsql: WorkflowSql, parent_id: JobId
) -> list[dict[str, Any]]:
    """The map's progress LINE (decision c): the certified grouped read —
    done/running/blocked IN the query — LEFT JOINed to the state channel
    (the children's avg pct). ONE read, never a second instrument, never
    a per-child metric series (DH5's fence — per-child progress lives in
    the rows this serves on demand)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(wsql.workflow_map_progress, parent_id)
    return [dict(r) for r in rows]


# ── THE SSE STREAM GENERATOR (the face's wire shape) ─────────────────────


async def progress_stream_generator(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    last_event_id: int = 0,
    poll_s: float = 1.0,
    stop: asyncio.Event | None = None,
) -> AsyncIterator[dict[str, str]]:
    """The run's progress SSE stream — the frames the HTTP face maps onto
    ``sse-starlette`` (the route is the thin mapping; this generator is
    the testable machine, T11's seq-cursor law applied to the progress
    stream).

    THE CONNECT SHAPE (the DH6 law at EVERY connect): the DISPLAY frame
    first (the ledger + the state channel — the state is ledger-derived,
    never stream-derived), then the replayed tail as ``progress`` frames
    in seq order (``id:`` the seq — the SSE reconnect contract). A cursor
    the ring pruned past yields the NAMED ``resync`` frame (the partial
    mode + the state payload) BEFORE the tail — never a silent
    empty-success. Idle ticks emit nothing (the keepalive is the HTTP
    layer's)."""
    import asyncio as _asyncio

    cursor = last_event_id
    display = await run_display(pool, wsql, flow_id)
    yield {
        "event": "display",
        "id": str(cursor),
        "data": json.dumps({"nodes": display}, default=str),
    }
    while stop is None or not stop.is_set():
        face = await progress_sse_face(pool, wsql, flow_id=flow_id, last_event_id=cursor)
        if face.mode == "partial":
            # THE NAMED PARTIAL: the window was pruned under the cursor —
            # the re-sync rides BEFORE the tail (the display converges
            # from the state rows; latest-wins needs no history).
            yield {
                "event": "resync",
                "id": str(cursor),
                "data": json.dumps(
                    {
                        "mode": "partial",
                        "oldest_retained": face.oldest_retained,
                        "state_sync": face.state_sync,
                    },
                    default=str,
                ),
            }
        for e in face.events:
            seq = int(e["seq"])
            if seq > cursor:
                cursor = seq
            yield {
                "event": "progress",
                "id": str(seq),
                "data": json.dumps(e, default=str),
            }
        await _asyncio.sleep(poll_s)
