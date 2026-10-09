"""ATTACK PINS — the T20 emit/chain subsystem's landed findings.

Provenance: the T20/T21 attack front's live reproductions against the
consolidated head (``af1b8779``; pinned here at the review tree's
``b2a1c122``). Every pin below was run RED FIRST — written as the SAFE
behavior, executed against the real PG, the failure captured — then
marked ``xfail(strict=True)``. The cure flips the pin to XPASS-strict
(a red that tells you to remove the marker WITH the cure).

The surfaces under conviction: ``taskq/workflows/_emit.py`` (the emit
tx) and ``taskq/workflows/chain.py`` (the router's record identity).

THE FINDINGS (each landed live; the probe transcripts are in this pack's
RECEIPTS.md):

* F-EMIT-1 — a resume that re-emits a COMMITTED page (or re-pages at a
  different width) dies on a RAW ``asyncpg.UniqueViolationError``,
  laddered to terminal: the defect "your page diverges from the emitted
  history" is never typed nor named.
* F-EMIT-2 — the ``map_index`` int16 ceiling: record #32768 dies mid-tx
  on a raw ``asyncpg.DataError`` (the codec's "value out of int16
  range") and rolls its VALID page-mates back with it — the ceiling is
  nowhere a typed, named, documented refusal at the emit door.

THE GUARD THAT IS NOT HERE: this front's kill-at-the-post-cursor/
pre-commit-window repro (kill inside the emit tx after the cursor
checkpoint, whole-tx rollback, the reclaim re-emitting exactly the lost
page — zero dup, zero lost) is ALREADY PINNED by the shipped suite:
``tests/test_wf_t20_emit_pins.py``'s
``test_t20_emit_tx_atomic_at_every_statement_window[3]`` kills the
backend at exactly the ``emit:3`` window and drives the real reclaim +
resume to the zero-re-emitted/zero-lost verdict. Not duplicated.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._emit import EMIT_CURSOR_KEY, emit_batch
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import EmitChild, NodeSpec
from tests._wf_fixtures import seed_flow

pytestmark = pytest.mark.integration

#: a short lock lease (the shipped emit pins' shape; no reclaim is driven
#: here — the door pins never resume — but the claim fence's shape is the
#: real dispatch claim's).
_LEASE = timedelta(milliseconds=250)


# ── the drivers (re-derived from the shipped emit pins so this file stands
# alone — the attack-file discipline) ─────────────────────────────────────


async def make_source(
    conn: asyncpg.Connection, wf_sql: WorkflowSql, flow_id: JobId
) -> JobId:
    """The source node: a normal workflow node (the real ``insert_node``
    shape)."""
    from taskq.workflows.engine import insert_node

    return await insert_node(
        conn, wf_sql, NodeSpec(flow_id=flow_id, step_key="source", actor="wf", queue="default")
    )


async def claim_source(
    conn: asyncpg.Connection, schema: str, source_id: JobId
) -> tuple[JobId, int, int]:
    """Claim the source in the real dispatch-claim's fence shape (the
    shipped pins' own helper, re-derived)."""
    worker = new_uuid()
    rec = await conn.fetchrow(
        f"UPDATE \"{schema}\".jobs SET status = 'running', started_at = now(), "
        "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
        "locked_by_worker = $2, lock_expires_at = now() + $3::interval, "
        "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
        "AND deps_pending = 0 RETURNING attempt, claim_epoch",
        source_id,
        worker,
        _LEASE,
    )
    assert rec is not None, "the source did not claim (not pending?)"
    return JobId(worker), int(rec["attempt"]), int(rec["claim_epoch"])


def page_children(page: list[int]) -> list[EmitChild]:
    """One page's chain starts — the per-record identity stamped at emit
    (``map_index`` + ``trace_id``, the refuted-claim discipline)."""
    return [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": i}},
            trace_id=f"app-{i}",
            map_index=i,
        )
        for i in page
    ]


async def read_cursor(conn: asyncpg.Connection, schema: str, source_id: JobId) -> Any:
    raw = await conn.fetchval(
        f"SELECT metadata->'{EMIT_CURSOR_KEY}' FROM \"{schema}\".jobs WHERE id = $1",
        source_id,
    )
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


async def committed_children(conn: asyncpg.Connection, schema: str, source_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE parent_id = $1 AND step_key = 'screen'",
            source_id,
        )
    )


# ── F-EMIT-1: the page divergence is never typed nor named ───────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING (the attack landed at af1b8779) F-EMIT-1: a resume that "
    "re-emits a COMMITTED page dies on a RAW asyncpg UniqueViolationError "
    "(the idempotency pair's unique key) — the defect 'your page diverges from "
    "the emitted history' is never typed nor named, and through the runner the "
    "raw driver error is what ladders to terminal. The cure (a typed, named "
    "refusal at the emit door — e.g. PageDivergedError — never a raw DB error) "
    "flips this to XPASS-strict — remove the marker WITH the cure.",
)
async def test_f_emit_1_reemitting_a_committed_page_is_a_typed_refusal(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The buggy-resume shape: page 0 committed; the re-claim's body lost
    the checkpoint and re-emits page 0. The divergence is an AUTHORING
    defect the emit must NAME (the caller cannot distinguish a raw
    UniqueViolation from a real constraint bug — and the ladder cannot
    either). The atomicity half (nothing new written, cursor unmoved)
    holds today and must hold after the cure."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_sql, flow_id)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    await emit_batch(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([0, 1, 2]),
        cursor={"page": 0},
    )
    assert await committed_children(wf_conn, wf_schema, source_id) == 3

    raised: Exception | None = None
    try:
        # THE DIVERGENCE: the same page re-emitted (the resume that lost
        # its checkpoint).
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([0, 1, 2]),
            cursor={"page": 0},
        )
    except Exception as exc:  # noqa: BLE001 — the pin discriminates the class below
        raised = exc
    assert raised is not None, "F-EMIT-1: a re-emitted committed page LANDED — a silent double-emit"
    assert "asyncpg" not in type(raised).__module__, (
        f"F-EMIT-1: the page divergence escaped as the RAW driver error "
        f"{type(raised).__module__}.{type(raised).__name__} "
        f"({str(raised)[:120]!r}) — it must be a typed, NAMED refusal at the "
        "emit door (e.g. PageDivergedError), never a raw asyncpg error"
    )
    # The failed emit wrote nothing new (the whole-tx law — holds today,
    # must hold after the cure).
    assert await committed_children(wf_conn, wf_schema, source_id) == 3
    assert await read_cursor(wf_conn, wf_schema, source_id) == {"page": 0}


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING (the attack landed at af1b8779) F-EMIT-1 (second face): "
    "a resume that re-pages at a DIFFERENT WIDTH (the cursor says page 0 done at "
    "width 3; the re-claimed body re-pages [0..5] as one page) collides on the "
    "per-record keys and dies on a RAW asyncpg UniqueViolationError — the "
    "divergence is never typed nor named. The cure (a typed, named refusal at "
    "the emit door) flips this to XPASS-strict — remove the marker WITH the cure.",
)
async def test_f_emit_1_repaging_at_a_different_width_is_a_typed_refusal(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The non-deterministic re-paging shape (the front's F2d): the
    cursor checkpoint says page 0 committed at width 3; the resumed body
    re-derives its pages DIFFERENTLY (one width-6 page). The emit must
    NAME the divergence against the emitted history — today the fresh
    records (3, 4, 5) roll back with the colliding ones on a raw driver
    error."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_sql, flow_id)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    await emit_batch(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([0, 1, 2]),
        cursor={"page": 0},
    )

    raised: Exception | None = None
    try:
        # THE DIVERGENCE: the re-paged width-6 replay of the same records.
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([0, 1, 2, 3, 4, 5]),
            cursor={"page": 0, "width": 6},
        )
    except Exception as exc:  # noqa: BLE001 — the pin discriminates the class below
        raised = exc
    assert raised is not None, (
        "F-EMIT-1: a re-paged divergent emit LANDED — the emitted history meant nothing"
    )
    assert "asyncpg" not in type(raised).__module__, (
        f"F-EMIT-1: the re-paging divergence escaped as the RAW driver error "
        f"{type(raised).__module__}.{type(raised).__name__} "
        f"({str(raised)[:120]!r}) — it must be a typed, NAMED refusal at the "
        "emit door, never a raw asyncpg error"
    )
    # The fresh siblings rolled back with the colliding ones; the
    # committed page stands (the whole-tx law — both today and cured).
    assert await committed_children(wf_conn, wf_schema, source_id) == 3
    assert await read_cursor(wf_conn, wf_schema, source_id) == {"page": 0}


# ── F-EMIT-2: the map_index int16 ceiling ────────────────────────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING (the attack landed at af1b8779) F-EMIT-2: map_index is a "
    "smallint, and the emit's door range-checks NOTHING — record #32768 dies "
    "mid-tx on a raw asyncpg DataError ('value out of int16 range', the codec's "
    "bind-time refusal) and its VALID page-mate (map_index=32767) rolls back "
    "with it. The cure (a typed, named refusal at the emit door BEFORE any row "
    "writes, the 32767 ceiling named in the refusal AND in the docs) flips this "
    "to XPASS-strict — remove the marker WITH the cure.",
)
async def test_f_emit_2_the_map_index_ceiling_is_a_typed_documented_refusal(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The ceiling is real (``jobs.map_index smallint``, 01.00.23) and
    UNDOCUMENTED where an author reads. The law the door already lives
    by (the map_index-None refusal: a ``ValueError`` BEFORE any row
    exists) is exactly the shape the ceiling refusal must take — typed,
    named, naming the ceiling's number, and the number in the docs."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_sql, flow_id)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    raised: Exception | None = None
    try:
        # A page pairing a VALID record (32767, the smallint max) with
        # the record that crosses the ceiling (32768).
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([32767, 32768]),
            cursor={"page": 0},
        )
    except Exception as exc:  # noqa: BLE001 — the pin discriminates the class below
        raised = exc
    assert raised is not None, "F-EMIT-2: map_index=32768 LANDED — the smallint ceiling is gone?!"
    assert "asyncpg" not in type(raised).__module__, (
        f"F-EMIT-2: the int16 ceiling surfaced as the RAW driver/codec error "
        f"{type(raised).__module__}.{type(raised).__name__} "
        f"({str(raised)[:120]!r}) — it must be a typed, NAMED refusal at the "
        "emit door (the door's own ValueError idiom or a dedicated error)"
    )
    assert "32767" in str(raised), (
        "F-EMIT-2: the ceiling refusal must NAME the ceiling (32767) — an author "
        "reading 'map_index out of range' learns nothing"
    )
    # BEFORE any row writes: the valid page-mate never rolled through a
    # doomed tx; nothing landed, the cursor never moved.
    assert await committed_children(wf_conn, wf_schema, source_id) == 0
    assert await read_cursor(wf_conn, wf_schema, source_id) is None

    # THE DOCUMENTED CEILING: the number an author can plan around, named
    # where the emit surface is documented (the module docstrings, the
    # EmitChild contract, or the workflows guide's map_index prose).
    import re
    from pathlib import Path

    import taskq.workflows._emit as emit_mod
    import taskq.workflows.chain as chain_mod

    surfaces = {
        "_emit.py docstring": emit_mod.__doc__ or "",
        "chain.py docstring": chain_mod.__doc__ or "",
        "EmitChild docstring": EmitChild.__doc__ or "",
    }
    guide = Path(__file__).resolve().parents[1] / "docs" / "guides" / "workflows.md"
    guide_text = guide.read_text() if guide.exists() else ""
    documented = any("32767" in text for text in surfaces.values()) or any(
        "map_index" in guide_text[max(0, m.start() - 200) : m.start() + 200]
        for m in re.finditer(r"32767", guide_text)
    )
    assert documented, (
        "F-EMIT-2: the int16 ceiling (32767) is documented NOWHERE a map/chain "
        "author reads — not the emit module, not the chain module, not the "
        "EmitChild contract, not the workflows guide's map_index prose"
    )
