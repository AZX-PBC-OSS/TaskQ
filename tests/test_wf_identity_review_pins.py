"""#717 P1 review obligations O1-O4 — exact existing-source behavior pins.

This is the review follow-up's own NEW pin module beside
``tests/test_wf_identity_p1.py`` (which it deliberately does not edit and
does not import from). Every pin below states behavior the shipped source
already has; nothing here widens a column, raises the ceiling, or edits a
product file. The schema states the pins need are produced by per-test
ALTER/DROP statements on a THROWAWAY per-test schema (migrated by the real
``apply_pending`` path) and are restored by that fixture's teardown alone
— no destructive production migration is authored or applied.

Obligations (REVIEW_IDENTITY_P1.md §6):

* O1 — for every one of the five identity carriers left at ``smallint``
  while the other four are ``bigint``, ``resolve_map_index_ceiling``
  returns exactly 32767: the bound is the minimum over ALL five carriers,
  so a resolver computing it from any proper subset (e.g. ``jobs`` alone)
  fails here;
* O2 — the fail-closed doors end-to-end against a deliberately missing
  identity carrier: a mixed low+high emit page raises the TYPED
  :class:`MapIndexSchemaError` and writes zero children (the low mate
  included) and no cursor; the ``insert_node`` high door and the
  ``insert_fork`` high-``ConsumerBinding`` door do the same with zero new
  records — schema errors refuse the whole page/unit, they never take
  poison-mate semantics;
* O3 — the ledger boundary pins at 32767: ``ledger_terminal`` returns
  ``UPDATE 1`` on the correct attempt/map_index (and ``UPDATE 0`` on a
  wrong key), the memoized read RETURNS the terminal row after the write,
  and ``_FAILED_COUNT_SQL`` counts a seeded not-vacuous failed row as
  exactly 1;
* O4 — ``ConsumerBinding``'s constructor refuses the bad identities
  (``True``, ``1.0``, ``"1"``, ``-1``) exactly like the other spec
  constructors, with a NULL/0/32767 positive control.

(O5 is documentation-only: see ``/tmp/opencode/identity/REVIEW_TEST_CLOSURE.md``.)

Only synthetic, generic record markers are used.
"""

# ruff: noqa: S608  # Why: the f-string DDL below interpolates only the fixture's throwaway schema identifier and the product's own carrier names; every value is $-bound.

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._emit import emit_batch
from taskq.workflows._fork import insert_fork
from taskq.workflows._types import (
    _MAP_INDEX_IDENTITY_TABLES,
    MAP_INDEX_CEILING,
    ChildSpec,
    ConsumerBinding,
    EmitChild,
    ForkSpec,
    JoinSpec,
    MapIndexSchemaError,
    NodeSpec,
    resolve_map_index_ceiling,
)
from taskq.workflows.engine import insert_node, render_workflow_sql
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


# ── the throwaway-schema fixture (per test; teardown-only restoration) ──


@pytest_asyncio.fixture
async def review_schema(pg_dsn: str) -> AsyncIterator[tuple[asyncpg.Connection, str]]:
    """One throwaway schema per test on the module's database, migrated by
    the REAL migration path (``apply_pending`` — never a hand-built schema)
    and dropped WHOLE at teardown. Every schema state a pin below needs is
    produced by ALTER/DROP statements on this scratch schema; the only
    restoration is this fixture's own cleanup — no migration file is
    authored, no shared/production schema is touched."""
    from taskq.migrate import apply_pending

    schema = "idr_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        yield conn, schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── the drivers ─────────────────────────────────────────────────────────


def review_children(page: list[int]) -> list[EmitChild]:
    """The emit page's children (the constructor's own validation passes —
    the ceiling is the door's concern, not the constructor's)."""
    return [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"record": {"marker": i}},
            trace_id=f"record-{i}",
            map_index=i,
        )
        for i in page
    ]


async def map_index_column_type(conn: asyncpg.Connection, schema: str, table: str) -> str | None:
    """The catalog's own answer for one carrier's ``map_index`` type —
    the verification read that keeps every ALTER/DROP below honest (an
    ALTER that silently no-ops would make the pins vacuous)."""
    raw = await conn.fetchval(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 AND column_name = 'map_index'",
        schema,
        table,
    )
    return raw if isinstance(raw, str) else None


async def drop_map_index_column(conn: asyncpg.Connection, schema: str, table: str) -> None:
    await conn.execute(f'ALTER TABLE "{schema}"."{table}" DROP COLUMN map_index')


async def flow_node_rows(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> int:
    """Every jobs row of the run except the root. Deliberately NO
    ``map_index`` reference — this count must stay legal even when a pin
    has dropped ``jobs.map_index``."""
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
            flow_id,
        )
        or 0
    )


async def flow_edge_rows(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_edge WHERE flow_id = $1',
            flow_id,
        )
        or 0
    )


async def emitted_child_rows(conn: asyncpg.Connection, schema: str, source_id: JobId) -> int:
    """The source's emitted child rows. Deliberately NO ``map_index``
    reference — this count must stay legal even when a pin has dropped
    ``jobs.map_index``."""
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE parent_id = $1',
            source_id,
        )
        or 0
    )


async def emit_cursor(conn: asyncpg.Connection, schema: str, source_id: JobId) -> object:
    raw = await conn.fetchval(
        f"SELECT metadata->'emit_cursor' FROM \"{schema}\".jobs WHERE id = $1",
        source_id,
    )
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


# ── O1: the resolved bound is the minimum over all five carriers ────────


@pytest.mark.parametrize(
    "kept_smallint", _MAP_INDEX_IDENTITY_TABLES, ids=_MAP_INDEX_IDENTITY_TABLES
)
async def test_one_smallint_among_bigint_resolves_the_narrow_bound(
    review_schema: tuple[asyncpg.Connection, str],
    kept_smallint: str,
) -> None:
    """With exactly one carrier left at smallint and the other four widened
    to bigint, the resolver returns exactly 32767. A resolver that requires
    all five carriers' PRESENCE but computes the bound from a proper subset
    (jobs alone, or any four) resolves bigint here and fails this pin."""
    conn, schema = review_schema

    # the pristine baseline: the migrated schema is all-smallint.
    assert await map_index_column_type(conn, schema, kept_smallint) == "smallint"
    assert await resolve_map_index_ceiling(conn, schema) == 32767

    widened = [table for table in _MAP_INDEX_IDENTITY_TABLES if table != kept_smallint]
    for table in widened:
        await conn.execute(f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN map_index TYPE bigint')

    # the fixture verification: the four targets really were altered and
    # the kept carrier really is still smallint.
    observed = {
        table: await map_index_column_type(conn, schema, table)
        for table in _MAP_INDEX_IDENTITY_TABLES
    }
    expected: dict[str, str | None] = dict.fromkeys(widened, "bigint")
    expected[kept_smallint] = "smallint"
    assert observed == expected, (
        f"the fixture did not produce the one-smallint-among-bigint state: {observed}"
    )

    assert await resolve_map_index_ceiling(conn, schema) == 32767, (
        f"{kept_smallint} left at smallint must hold the resolved bound at "
        "exactly 32767 — a resolver reading any proper subset of the five "
        "carriers would report the bigint domain here"
    )


# ── O2a: the emit door fails closed on a missing carrier ────────────────


@pytest.mark.parametrize("dropped", _MAP_INDEX_IDENTITY_TABLES, ids=_MAP_INDEX_IDENTITY_TABLES)
async def test_emit_refuses_typed_and_writes_nothing_when_a_carrier_is_missing(
    review_schema: tuple[asyncpg.Connection, str],
    pg_dsn: str,
    dropped: str,
) -> None:
    """A mixed low+high page against a schema missing one carrier's
    ``map_index`` column raises the TYPED ``MapIndexSchemaError`` (never a
    raw asyncpg error, never ``MapIndexExhaustedError``) and writes NOTHING:
    zero children — the low mate included — and no cursor checkpoint. A
    schema error refuses the whole page; poison-mate semantics never apply
    to one."""
    conn, schema = review_schema
    await drop_map_index_column(conn, schema, dropped)

    # the fixture verification: the named carrier's column is really gone
    # and the other four are still in place.
    observed = {
        table: await map_index_column_type(conn, schema, table)
        for table in _MAP_INDEX_IDENTITY_TABLES
    }
    assert observed[dropped] is None, f"the fixture did not actually drop {dropped}.map_index"
    for table, column_type in observed.items():
        if table != dropped:
            assert column_type is not None, f"{table}.map_index unexpectedly missing"

    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    pool = await asyncpg.create_pool(pg_dsn)
    try:
        with pytest.raises(MapIndexSchemaError) as raised:
            await emit_batch(
                pool,
                wsql,
                flow_id=flow,
                source_id=source,
                worker_id=worker,
                attempt=attempt,
                claim_epoch=epoch,
                children=review_children([8, MAP_INDEX_CEILING + 1]),
                cursor={"page": 0},
            )
    finally:
        await pool.close()

    assert type(raised.value) is MapIndexSchemaError, (
        f"the refusal must be the typed schema error itself, got "
        f"{type(raised.value).__name__}: {raised.value}"
    )
    assert dropped in str(raised.value), str(raised.value)
    assert "missing" in str(raised.value), str(raised.value)
    assert await emitted_child_rows(conn, schema, source) == 0, (
        "the low page-mate must be refused with the page — a schema error is not a poison record"
    )
    assert await emit_cursor(conn, schema, source) is None, (
        "a refused page wrote a cursor checkpoint"
    )


# ── O2b: the low-level insert doors fail closed on a missing carrier ────


@pytest.mark.parametrize("dropped", _MAP_INDEX_IDENTITY_TABLES, ids=_MAP_INDEX_IDENTITY_TABLES)
async def test_insert_doors_refuse_typed_and_write_nothing_when_a_carrier_is_missing(
    review_schema: tuple[asyncpg.Connection, str],
    dropped: str,
) -> None:
    """The ``insert_node`` high door and the ``insert_fork``
    high-``ConsumerBinding`` door propagate the TYPED
    ``MapIndexSchemaError`` from the five-carrier probe and write zero new
    records — no node, no fork child, no join, no edge."""
    conn, schema = review_schema
    await drop_map_index_column(conn, schema, dropped)
    assert await map_index_column_type(conn, schema, dropped) is None, (
        f"the fixture did not actually drop {dropped}.map_index"
    )

    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)

    high_node = NodeSpec(
        flow_id=flow,
        step_key="high",
        actor="wf",
        queue="default",
        map_index=MAP_INDEX_CEILING + 1,
    )
    with pytest.raises(MapIndexSchemaError) as node_raised:
        await insert_node(conn, wsql, high_node)
    assert type(node_raised.value) is MapIndexSchemaError, (
        f"the node door's refusal must be the typed schema error itself, got "
        f"{type(node_raised.value).__name__}: {node_raised.value}"
    )
    assert dropped in str(node_raised.value), str(node_raised.value)
    assert await flow_node_rows(conn, schema, flow) == 0, "the refused node insert wrote a row"
    assert await flow_edge_rows(conn, schema, flow) == 0

    parent = await seed_running_node(conn, schema, flow, step_key="parent")
    before = await flow_node_rows(conn, schema, flow)
    fork = ForkSpec(
        children=(ChildSpec(step_key="child", actor="wf", queue="default", map_index=0),),
        join=JoinSpec(
            step_key="join",
            actor="wf",
            queue="default",
            consumers=(
                ConsumerBinding(
                    step_key="consumer",
                    actor="wf",
                    queue="default",
                    map_index=MAP_INDEX_CEILING + 1,
                ),
            ),
        ),
    )
    with pytest.raises(MapIndexSchemaError) as fork_raised:
        await insert_fork(
            conn,
            wsql,
            flow_id=flow,
            parent_id=parent,
            parent_step_key="parent",
            fork=fork,
        )
    assert type(fork_raised.value) is MapIndexSchemaError, (
        f"the fork door's refusal must be the typed schema error itself, got "
        f"{type(fork_raised.value).__name__}: {fork_raised.value}"
    )
    assert dropped in str(fork_raised.value), str(fork_raised.value)
    assert await flow_node_rows(conn, schema, flow) == before, (
        "the refused fork wrote children or a join"
    )
    assert await flow_edge_rows(conn, schema, flow) == 0, "the refused fork wrote edges"


# ── O3: the ledger boundary pins at 32767 ───────────────────────────────


async def test_ledger_terminal_arbiter_and_memoized_recovery_at_32767(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """The boundary terminal write touches exactly its claimed row
    (``UPDATE 1`` on the correct attempt/map_index; ``UPDATE 0`` on a wrong
    key — the arbiter discriminates), and the memoized read RETURNS the
    terminal row after the write (the replay's truth, not just no-error)."""
    flow = await seed_flow(wf_conn, wf_schema)
    parent = await insert_node(
        wf_conn, wf_sql, NodeSpec(flow_id=flow, step_key="parent", actor="wf", queue="default")
    )

    ledger_id = new_uuid()
    claim = await wf_conn.fetchrow(
        wf_sql.ledger_claim, ledger_id, flow, parent, "screen", 32767, 1, 0
    )
    assert claim is not None

    # before the terminal write the memoized read finds nothing terminal.
    assert await wf_conn.fetchrow(wf_sql.ledger_memoized, flow, "screen", 32767) is None

    # a terminal keyed to the WRONG claim identity touches nothing: a stale
    # instant (the re-mint guard's own discriminator) refuses the write.
    wrong_key = await wf_conn.execute(
        wf_sql.ledger_terminal_by_id,
        ledger_id,
        "succeeded",
        None,
        None,
        None,
        None,
        claim["updated_at"] + timedelta(microseconds=1),
    )
    assert wrong_key == "UPDATE 0", wrong_key
    assert await wf_conn.fetchrow(wf_sql.ledger_memoized, flow, "screen", 32767) is None

    # the correctly keyed terminal writes exactly the claimed row.
    status = await wf_conn.execute(
        wf_sql.ledger_terminal_by_id,
        ledger_id,
        "succeeded",
        None,
        None,
        None,
        None,
        claim["updated_at"],
    )
    assert status == "UPDATE 1", (
        f"the boundary terminal write did not land on its claimed row: {status}"
    )

    row = await wf_conn.fetchrow(wf_sql.ledger_memoized, flow, "screen", 32767)
    assert row is not None, "the memoized read did not recover the terminal row at 32767"
    assert row["attempt"] == 1
    assert row["status"] == "succeeded"
    assert row["job_id"] == parent


async def test_failed_count_sql_counts_exactly_the_seeded_failed_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """``_FAILED_COUNT_SQL``'s ordinal arbiter is exact: a seeded not-vacuous
    'failed' ledger row at 32767 counts exactly 1, while a failed sibling at
    a different map_index and a terminal (non-failed) row at 32767 do not
    count."""
    from taskq.workflows.api._runner_ladder import _FAILED_COUNT_SQL

    flow = await seed_flow(wf_conn, wf_schema)
    parent = await insert_node(
        wf_conn, wf_sql, NodeSpec(flow_id=flow, step_key="parent", actor="wf", queue="default")
    )

    async def claim_and_terminal(map_index: int, attempt: int, status: str) -> None:
        seeded = await wf_conn.fetchrow(
            wf_sql.ledger_claim,
            new_uuid(),
            flow,
            parent,
            "screen",
            map_index,
            attempt,
            0,
        )
        assert seeded is not None
        terminal = await wf_conn.execute(
            wf_sql.ledger_terminal_by_id,
            seeded["id"],
            status,
            None,
            None,
            None,
            None,
            seeded["updated_at"],
        )
        assert terminal == "UPDATE 1", (
            f"the seeded ledger row (map_index={map_index}, attempt={attempt}) "
            f"did not terminalize: {terminal}"
        )

    await claim_and_terminal(32767, 1, "failed")
    # the siblings that must NOT count: a failed row at a different ordinal,
    # and a terminal non-failed row at the named ordinal.
    await claim_and_terminal(0, 1, "failed")
    await claim_and_terminal(32767, 2, "succeeded")

    counted = await wf_conn.fetchval(
        _FAILED_COUNT_SQL.format(schema=wf_schema), flow, "screen", 32767
    )
    assert int(counted or 0) == 1, (
        "the failed-ordinal count must be exactly the one seeded failed row "
        f"at 32767, got {counted}"
    )


# ── O4: the ConsumerBinding constructor's identity validation ───────────


@pytest.mark.parametrize("bad", [True, 1.0, "1", -1])
def test_consumer_binding_constructor_refuses_bad_identity(bad: object) -> None:
    """``ConsumerBinding`` carries the same exact-int, non-negative identity
    validation as ``EmitChild``/``NodeSpec``/``ChildSpec`` — the outbox
    authoring derivation reads bindings back through this constructor, so
    ``True``, ``1.0``, ``"1"`` and ``-1`` are refused without coercion."""
    with pytest.raises(ValueError, match=r"exact int|non-negative"):
        ConsumerBinding(
            step_key="consumer",
            actor="wf",
            queue="default",
            map_index=cast("int", bad),
        )


def test_consumer_binding_constructor_keeps_null_and_in_range_identity() -> None:
    """The positive control: NULL and in-range identities stay legal on the
    binding (the pre-existing legal shapes, byte-for-byte)."""
    assert ConsumerBinding(step_key="consumer", actor="wf", queue="default").map_index is None
    assert (
        ConsumerBinding(step_key="consumer", actor="wf", queue="default", map_index=0).map_index
        == 0
    )
    assert (
        ConsumerBinding(step_key="consumer", actor="wf", queue="default", map_index=32767).map_index
        == 32767
    )
