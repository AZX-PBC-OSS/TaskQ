"""The widening runbook's fail-closed claim and the shipped probe policy agree.

The five-carrier identity check is gated on a candidate above
``MAP_INDEX_CEILING`` (32767): an ordinary UNBOUNDED page takes the
no-catalog-read fast path by design, so it does not detect a missing, mixed,
``NOT NULL`` or unsupported carrier shape. Bounded emit is the shipped default
(``max_in_flight`` is ``EMIT_MAX_IN_FLIGHT_DEFAULT`` unless a workflow declares
``None``) and its capability preflight re-reads the five carriers on every
call, so this pin's low leg drives the explicit unbounded declaration. The
docs name that conditional contract —
``docs/guides/workflows.md`` says "A candidate above 32767 makes the authoring
doors ... consult that resolved bound, and they fail closed with
``MapIndexSchemaError`` ..." and names the hold door's type/lower-bound-only
contract; ``docs/guides/upgrading.md``'s POST runbook states the same condition.

This pin holds the two halves together on a real half-migrated schema: a HIGH
candidate is refused with ``MapIndexSchemaError`` before any row is written, and
an ordinary unbounded page is written without paying the catalog read. A door
that stops refusing the high candidate, a low unbounded page that starts probing
(the shipped fast-path contract regressing), or a doc that drifts back to the
unconditional "every door fails closed" sentence all red this.
"""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending
from taskq.workflows._emit import emit_batch
from taskq.workflows._types import EmitChild, MapIndexSchemaError
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

pytestmark = pytest.mark.integration

_DOC_CLAIM = "A candidate above 32767 makes the authoring doors"
_DOC_FAILURE_CLASS = (
    "fail closed with `MapIndexSchemaError` (the admin panel answers HTTP 503) "
    "when the five carriers are missing, mixed, `NOT NULL`, or of an unsupported type"
)
_UPGRADING_CLAIM = "A candidate above 32767 against a schema whose five carriers are mixed"
_UPGRADING_LOW_PATH = "An ordinary unbounded page at or below 32767 takes the no-catalog-read path"


async def _emit_page(
    conn: asyncpg.Connection,
    pool: asyncpg.Pool,
    schema: str,
    indices: list[int],
    *,
    page: int,
) -> bool:
    """Drive the shipped emit door; True iff it was refused with the doc's class."""
    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)
    children = [
        EmitChild(
            step_key="record",
            actor="wf",
            queue="default",
            payload={"record": index},
            trace_id=f"record-{page}-{index}",
            map_index=index,
        )
        for index in indices
    ]
    try:
        await emit_batch(
            pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=children,
            cursor={"page": page},
        )
    except MapIndexSchemaError:
        return True
    return False


def _normalized_doc(name: str) -> str:
    """One static docs read (the corpus's prose, whitespace-normalized)."""
    path = Path(__file__).resolve().parents[1] / "docs" / "guides" / name
    return " ".join(path.read_text().split())


async def test_the_docs_fail_closed_claim_matches_the_shipped_low_page_behaviour(
    pg_dsn: str,
) -> None:
    normalized = _normalized_doc("workflows.md")
    # The sentence must name the CONDITIONAL contract the doors ship: the
    # five-carrier read is gated on a candidate above MAP_INDEX_CEILING, so an
    # ordinary page neither consults the bound nor fails closed on a broken
    # carrier shape.
    assert _DOC_CLAIM in normalized and _DOC_FAILURE_CLASS in normalized, (
        "docs/guides/workflows.md must name the conditional fail-closed contract "
        "(a candidate above 32767 consults the resolved bound); the unconditional "
        "'every door fails closed' sentence overstates the shipped low-page path"
    )
    upgrading = _normalized_doc("upgrading.md")
    assert _UPGRADING_CLAIM in upgrading and _UPGRADING_LOW_PATH in upgrading, (
        "docs/guides/upgrading.md's POST runbook must scope MapIndexSchemaError to "
        "a candidate above 32767 and name the low-identity unbounded "
        "no-catalog-read path"
    )

    schema = "docs_failclosed_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await apply_pending(conn, schema=schema, phase="pre")
        pool = await asyncpg.create_pool(pg_dsn)
        # A half-applied/unsupported carrier shape: one of the five is `integer`.
        await conn.execute(
            f'ALTER TABLE "{schema}".jobs_archive ALTER COLUMN map_index TYPE integer'
        )
        high_refused = await _emit_page(conn, pool, schema, [0, 32768], page=0)
        assert high_refused, (
            "a HIGH candidate on an unsupported carrier shape must be the doc's "
            "typed MapIndexSchemaError — the sentence is true for that leg"
        )

        low_refused = await _emit_page(conn, pool, schema, [0, 1, 2], page=0)
        assert not low_refused, (
            "an ordinary 0..32767 page took the five-carrier catalog read and "
            "refused — the shipped fast path (and the docs' 'ordinary pages pay "
            "no catalog read') regressed"
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
