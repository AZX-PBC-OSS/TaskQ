"""#717 P1 identity pins: exact input validation and bigint-bind groundwork.

These are the retained P1 boundary pins, now running beside P3 activation.
The all-smallint controls deliberately use real pre-only schemas. The shipped
resolver reads the actual five carriers: all-smallint uses 32767, all-bigint
activates BIGINT_MAX, and mixed/missing shapes fail the public authoring batch
closed rather than being guessed safe.

Pins:

* a non-``int`` / ``bool`` / negative / over-bound identity is a typed poison
  record, never ``int(...)``-coerced into a different key or a raw codec
  error that rolls its valid page-mates back;
* an all-poison page writes no child and no cursor;
* the poison partition uses direct values, never deep payload equality;
* raw ``NodeSpec``/``ChildSpec`` doors validate before persistence and the
  low-level async insert doors schema-gate an above-ceiling identity;
* the five-carrier resolver returns the minimum supported bound, fails
  closed on missing/unknown types, and is not called for ordinary pages;
* the eight identity cast sites plus the admin child bind use bigint and
  still work against the old smallint schema at 32767.

Only synthetic, generic record markers are used.
"""

# ruff: noqa: S608  # Why: the f-string SQL below interpolates only the module fixture's throwaway schema identifier; every value is $-bound.

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, cast

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.constants import BIGINT_MAX
from taskq.workflows._emit import MapIndexExhaustedError, emit_batch
from taskq.workflows._fork import insert_fork
from taskq.workflows._types import (
    ChildSpec,
    ConsumerBinding,
    EmitChild,
    ForkSpec,
    JoinSpec,
    NodeSpec,
    _jsonb,
)
from taskq.workflows.engine import insert_node, render_workflow_sql
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


# ── the drivers ─────────────────────────────────────────────────────────


def page_children(page: list[int]) -> list[EmitChild]:
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


def corrupt_identity(child: EmitChild, value: object) -> EmitChild:
    """Simulate a corrupted runtime attribute the typed door must still
    refuse. Construction-time validation is a separate pin; this bypass
    proves the finished *emission* path does not trust the annotation."""
    object.__setattr__(child, "map_index", value)
    return child


def corrupt_child_identity(child: ChildSpec, value: object) -> ChildSpec:
    object.__setattr__(child, "map_index", value)
    return child


def corrupt_node_identity(spec: NodeSpec, value: object) -> NodeSpec:
    object.__setattr__(spec, "map_index", value)
    return spec


async def read_cursor(conn: asyncpg.Connection, schema: str, source_id: JobId) -> object:
    raw = await conn.fetchval(
        f"SELECT metadata->'emit_cursor' FROM \"{schema}\".jobs WHERE id = $1",
        source_id,
    )
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


async def committed_children(
    conn: asyncpg.Connection, schema: str, source_id: JobId, *, step_key: str = "screen"
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT map_index FROM "{schema}".jobs WHERE parent_id = $1 AND step_key = $2 '
        "ORDER BY map_index",
        source_id,
        step_key,
    )


async def _flow_node_count(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
            "AND step_key <> '__flow__'",
            flow_id,
        )
    )


# ── exact identity at the emit door ─────────────────────────────────────


async def test_bad_type_identity_is_poison_and_the_valid_mate_commits(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """A float identity must not mint a divergent ``...:32768.0:...`` key or
    die raw inside the page tx. It is a poison record: the valid mate's page
    commits and the typed refusal names the invalid identity."""
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)

    valid = page_children([8])[0]
    poison = corrupt_identity(page_children([7])[0], 7.0)
    with pytest.raises(MapIndexExhaustedError) as raised:
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=[valid, poison],
            cursor={"page": 0},
        )
    assert "exact int" in str(raised.value), str(raised.value)
    rows = await committed_children(wf_conn, wf_schema, source)
    assert [int(row["map_index"]) for row in rows] == [8], (
        "the valid page-mate did not survive the bad-type poison record: "
        f"{[row['map_index'] for row in rows]}"
    )
    assert await read_cursor(wf_conn, wf_schema, source) == {"page": 0}
    divergent = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1 '
        "AND idempotency_key LIKE '%:7.0:%'",
        source,
    )
    assert int(divergent or 0) == 0, "a float identity minted a divergent key"


async def test_bool_and_string_identities_are_all_poison_and_write_nothing(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)

    first = corrupt_identity(page_children([1])[0], True)
    second = corrupt_identity(page_children([2])[0], "8")
    with pytest.raises(MapIndexExhaustedError, match="nothing was written"):
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=[first, second],
            cursor={"page": 0},
        )
    assert await committed_children(wf_conn, wf_schema, source) == []
    assert await read_cursor(wf_conn, wf_schema, source) is None


class _ExplodingEq:
    """A payload member whose equality raises: the door must partition by
    the direct ``map_index`` value, never by full dataclass/payload equality."""

    def __eq__(self, other: object) -> bool:
        raise AssertionError("the poison partition compared payload dataclasses")

    __hash__ = object.__hash__


async def test_poison_partition_does_not_compare_payload_dataclasses(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)

    valid = EmitChild(
        step_key="screen",
        actor="wf",
        queue="default",
        payload={"record": {"marker": 1}},
        trace_id="record-1",
        map_index=5,
    )
    poison = EmitChild(
        step_key="screen",
        actor="wf",
        queue="default",
        payload={"record": {"marker": _ExplodingEq()}},
        trace_id="record-2",
        map_index=BIGINT_MAX + 1,
    )
    with pytest.raises(MapIndexExhaustedError):
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=[valid, poison],
            cursor={"page": 0},
        )
    rows = await committed_children(wf_conn, wf_schema, source)
    assert [int(row["map_index"]) for row in rows] == [5]


# ── synchronous spec doors and the async low-level insert gates ─────────


@pytest.mark.parametrize("bad", [True, 1.0, "1", -1])
def test_spec_constructors_refuse_bad_identity_before_persistence(bad: object) -> None:
    with pytest.raises(ValueError, match=r"exact int|non-negative"):
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload=None,
            trace_id="record",
            map_index=cast("int", bad),
        )
    with pytest.raises(ValueError, match=r"exact int|non-negative"):
        NodeSpec(
            flow_id=JobId(new_uuid()),
            step_key="node",
            actor="wf",
            queue="default",
            map_index=cast("int", bad),
        )
    with pytest.raises(ValueError, match=r"exact int|non-negative"):
        ChildSpec(
            step_key="child",
            actor="wf",
            queue="default",
            map_index=cast("int", bad),
        )


def test_spec_constructors_keep_null_and_in_range_identity() -> None:
    assert (
        NodeSpec(flow_id=JobId(new_uuid()), step_key="node", actor="wf", queue="default").map_index
        is None
    )
    assert ChildSpec(step_key="child", actor="wf", queue="default").map_index is None
    assert ChildSpec(step_key="child", actor="wf", queue="default", map_index=0).map_index == 0
    assert (
        ChildSpec(step_key="child", actor="wf", queue="default", map_index=32767).map_index == 32767
    )


async def test_low_level_insert_doors_gate_above_ceiling_identity(
    old_schema: tuple[asyncpg.Connection, str, asyncpg.Pool, WorkflowSql],
) -> None:
    wf_conn, wf_schema, _pool, wf_sql = old_schema
    flow = await seed_flow(wf_conn, wf_schema)
    node = corrupt_node_identity(
        NodeSpec(flow_id=flow, step_key="source", actor="wf", queue="default"), 40000
    )
    with pytest.raises(MapIndexExhaustedError) as raised:
        await insert_node(wf_conn, wf_sql, node)
    assert "32767" in str(raised.value)
    assert await _flow_node_count(wf_conn, wf_schema, flow) == 0

    bad_node = corrupt_node_identity(
        NodeSpec(flow_id=flow, step_key="bad", actor="wf", queue="default"), True
    )
    with pytest.raises(ValueError, match="exact int"):
        await insert_node(wf_conn, wf_sql, bad_node)
    assert await _flow_node_count(wf_conn, wf_schema, flow) == 0

    parent = await insert_node(
        wf_conn, wf_sql, NodeSpec(flow_id=flow, step_key="parent", actor="wf", queue="default")
    )
    child = corrupt_child_identity(
        ChildSpec(step_key="child", actor="wf", queue="default", map_index=0), 40000
    )
    with pytest.raises(MapIndexExhaustedError):
        await insert_fork(
            wf_conn,
            wf_sql,
            flow_id=flow,
            parent_id=parent,
            parent_step_key="parent",
            fork=ForkSpec(children=(child,)),
        )
    bad_child = corrupt_child_identity(
        ChildSpec(step_key="bad", actor="wf", queue="default", map_index=0), True
    )
    with pytest.raises(ValueError, match="exact int"):
        await insert_fork(
            wf_conn,
            wf_sql,
            flow_id=flow,
            parent_id=parent,
            parent_step_key="parent",
            fork=ForkSpec(children=(bad_child,)),
        )
    high_consumer = ConsumerBinding(
        step_key="consumer", actor="wf", queue="default", map_index=40000
    )
    with pytest.raises(MapIndexExhaustedError):
        await insert_fork(
            wf_conn,
            wf_sql,
            flow_id=flow,
            parent_id=parent,
            parent_step_key="parent",
            fork=ForkSpec(
                children=(ChildSpec(step_key="child", actor="wf", queue="default", map_index=0),),
                join=JoinSpec(
                    step_key="join",
                    actor="wf",
                    queue="default",
                    consumers=(high_consumer,),
                ),
            ),
        )
    children = await committed_children(wf_conn, wf_schema, parent, step_key="child")
    assert children == []


# ── the schema-derived ceiling helper ───────────────────────────────────


async def _fresh_schema(pg_dsn: str) -> tuple[asyncpg.Connection, str]:
    from taskq.migrate import apply_pending

    schema = "idp_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema, phase="pre")
    return conn, schema


@pytest_asyncio.fixture
async def old_schema(
    pg_dsn: str,
) -> AsyncIterator[tuple[asyncpg.Connection, str, asyncpg.Pool, WorkflowSql]]:
    """Real pre-only schema for P1's retained old-storage-domain controls."""
    conn, schema = await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        yield conn, schema, pool, render_workflow_sql(schema)
    finally:
        await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_schema_ceiling_is_the_minimum_over_all_five_carriers(pg_dsn: str) -> None:
    from taskq.constants import BIGINT_MAX
    from taskq.workflows._types import MapIndexSchemaError, resolve_map_index_ceiling

    conn, schema = await _fresh_schema(pg_dsn)
    try:
        assert await resolve_map_index_ceiling(conn, schema) == 32767
        await conn.execute(f'ALTER TABLE "{schema}".wf_signals ALTER COLUMN map_index TYPE bigint')
        assert await resolve_map_index_ceiling(conn, schema) == 32767, (
            "a partial widening must report the narrow carrier's bound"
        )
        for table in ("jobs", "jobs_archive", "wf_step_ledger", "wf_outbox", "wf_signals"):
            await conn.execute(
                f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN map_index TYPE bigint'
            )
        assert await resolve_map_index_ceiling(conn, schema) == BIGINT_MAX

        await conn.execute(f'ALTER TABLE "{schema}".wf_signals ALTER COLUMN map_index TYPE integer')
        with pytest.raises(MapIndexSchemaError, match="unsupported"):
            await resolve_map_index_ceiling(conn, schema)

        await conn.execute(f'ALTER TABLE "{schema}".wf_outbox DROP COLUMN map_index')
        with pytest.raises(MapIndexSchemaError, match="missing"):
            await resolve_map_index_ceiling(conn, schema)
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_all_bigint_schema_activates_the_ceiling_without_restart(
    pg_dsn: str,
) -> None:
    """P3's explicit positive half of P1's former down-stopping pin.

    The old-schema lower-bound behavior remains in the activation control;
    here the same five manual ALTERs now permit 32768 in this live process.
    """
    from taskq.workflows._types import MAP_INDEX_CEILING

    conn, schema = await _fresh_schema(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        for table in ("jobs", "jobs_archive", "wf_step_ledger", "wf_outbox", "wf_signals"):
            await conn.execute(
                f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN map_index TYPE bigint'
            )
        wf_sql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema)
        source = await seed_running_node(conn, schema, flow, step_key="source")
        worker, attempt, epoch = await claim_view(conn, schema, source)
        pool = await asyncpg.create_pool(pg_dsn)
        emitted = await emit_batch(
            pool,
            wf_sql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=[
                EmitChild(
                    step_key="screen",
                    actor="wf",
                    queue="default",
                    payload={"record": {"marker": MAP_INDEX_CEILING + 1}},
                    trace_id="record-boundary",
                    map_index=MAP_INDEX_CEILING + 1,
                )
            ],
            cursor={"page": 0},
        )
        assert len(emitted) == 1
        stored = await conn.fetchrow(
            f'SELECT id, map_index, idempotency_key FROM "{schema}".jobs WHERE parent_id = $1',
            source,
        )
        assert stored is not None
        assert stored["id"] == emitted[0]
        assert stored["map_index"] == MAP_INDEX_CEILING + 1
        assert stored["idempotency_key"] == (f"wf:{flow}:emit:{MAP_INDEX_CEILING + 1}:screen")
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


class _CatalogProbeSpy:
    """Delegating connection wrapper: a query other than the five-carrier
    catalog read fails loudly, and every call is counted."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[Any]:
        if "information_schema.columns" not in query:
            raise AssertionError(f"unexpected catalog statement: {query}")
        self.calls.append((query, args))
        return await self._conn.fetch(query, *args)


async def test_ordinary_page_does_not_probe_and_high_page_probes_once(
    old_schema: tuple[asyncpg.Connection, str, asyncpg.Pool, WorkflowSql],
) -> None:
    wf_conn, wf_schema, wf_pool, wf_sql = old_schema
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)
    spy = _CatalogProbeSpy(wf_conn)

    ids = await emit_batch(
        wf_pool,
        wf_sql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([0, 1]),
        cursor={"page": 0},
        _schema_conn=spy,
    )
    assert len(ids) == 2
    assert spy.calls == [], "an in-range page made a catalog query"

    with pytest.raises(MapIndexExhaustedError):
        await emit_batch(
            wf_pool,
            wf_sql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([2, 32768]),
            cursor={"page": 1},
            _schema_conn=spy,
        )
    assert len(spy.calls) == 1, f"expected one batch probe, got {len(spy.calls)}"
    query, args = spy.calls[0]
    assert "$1" in query and "$2::text[]" in query
    assert args[0] == wf_schema
    assert set(cast("list[str]", args[1])) == {
        "jobs",
        "jobs_archive",
        "wf_step_ledger",
        "wf_outbox",
        "wf_signals",
    }


# ── bigint binds against the old smallint schema ────────────────────────


async def test_bigint_identity_casts_work_against_old_smallint_schema(
    old_schema: tuple[asyncpg.Connection, str, asyncpg.Pool, WorkflowSql],
) -> None:
    """The compatibility half of the rollout: bigint binds are assignment-
    compatible with the old smallint columns for in-range values. This is
    the functional form, not a catalog-only string pin."""
    wf_conn, wf_schema, wf_pool, wf_sql = old_schema
    from taskq.web.admin._wf_actions import _NODE_PANEL_CHILD_SQL
    from taskq.workflows.api._runner_ladder import _FAILED_COUNT_SQL

    # The identity tokens are bigint; the neighbouring smallint
    # domains (max_attempts/priority/deps_pending) stay smallint. The
    # ledger's terminal/fence writes are ID+INSTANT keyed (the exact
    # claim identity — no four-expression arbiter-tuple statement
    # exists), so the map identity's bigint face rides the claim and the
    # memoized read.
    assert "map_index = $3::bigint" in wf_sql.ledger_memoized
    assert "$3::bigint IS NULL" in wf_sql.ledger_memoized
    assert "$5::bigint" in wf_sql.ledger_claim  # the claim's map_index bind
    assert "updated_at = $7::timestamptz" in wf_sql.ledger_terminal_by_id
    assert "updated_at = $3::timestamptz" in wf_sql.ledger_fence_by_id
    assert "$8::bigint[]" in wf_sql.fork_children
    assert "$8::bigint[]" in wf_sql.emit_children
    assert "$4::bigint" in wf_sql.collect_fan_in
    assert "$8::bigint[]" in wf_sql.outbox_drain_consumers
    assert "COALESCE($3::bigint, -1)" in _FAILED_COUNT_SQL
    assert "map_index = $3::bigint" in _NODE_PANEL_CHILD_SQL
    assert "$5::smallint[]" in wf_sql.fork_children  # max_attempts
    assert "$14::smallint[]" in wf_sql.fork_children  # priority

    flow = await seed_flow(wf_conn, wf_schema)
    parent = await insert_node(
        wf_conn, wf_sql, NodeSpec(flow_id=flow, step_key="parent", actor="wf", queue="default")
    )
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)
    ledger_id = new_uuid()

    # _sql_ledger.py — the memoized sibling read.
    claimed = await wf_conn.fetchrow(
        wf_sql.ledger_claim, ledger_id, flow, parent, "screen", 32767, 1, 0
    )
    assert claimed is not None
    assert await wf_conn.fetchrow(wf_sql.ledger_memoized, flow, "screen", 32767) is None

    # _sql_ledger.py — the terminal write on the claim's own id + instant.
    await wf_conn.execute(
        wf_sql.ledger_terminal_by_id,
        ledger_id,
        "succeeded",
        None,
        None,
        None,
        None,
        claimed["updated_at"],
    )
    # _sql_ledger.py — the fence write on a fresh claim's own identity.
    fence_claim = await wf_conn.fetchrow(
        wf_sql.ledger_claim, new_uuid(), flow, parent, "screen", 32767, 2, 0
    )
    assert fence_claim is not None
    fence_status = await wf_conn.execute(
        wf_sql.ledger_fence_by_id, fence_claim["id"], "fenced", fence_claim["updated_at"]
    )
    assert fence_status == "UPDATE 1", fence_status

    # _runner_ladder.py _FAILED_COUNT_SQL — the failed ordinal count.
    assert (
        int(
            await wf_conn.fetchval(
                _FAILED_COUNT_SQL.format(schema=wf_schema), flow, "screen", 32767
            )
            or 0
        )
        >= 0
    )

    # _sql_finalize.py:238 — the fork children array.
    fork_child = new_uuid()
    await wf_conn.execute(
        wf_sql.fork_children,
        [fork_child],
        ["wf"],
        ["default"],
        [_jsonb({"record": {"marker": 32767}})],
        [3],
        ["transient"],
        [parent],
        [32767],
        ["child"],
        ["record-32767"],
        [_jsonb({"flow_id": str(flow)})],
        [f"workflow:{flow}"],
        [f"fork:{flow}:32767"],
        [0],
        [None],
        [str(flow)],
        [5.0],
        [3600.0],
        ["exponential"],
        [0.2],
    )

    # _sql_finalize.py:483 — the collect fan-in read.
    assert await wf_conn.fetchrow(wf_sql.collect_fan_in, parent, flow, "child", 32767) is None

    # _sql_sweep.py:362 — the outbox consumer drain array.
    consumer = new_uuid()
    await wf_conn.execute(
        wf_sql.outbox_drain_consumers,
        [consumer],
        ["wf"],
        ["default"],
        [_jsonb({"record": {"marker": 32767}})],
        [3],
        ["transient"],
        [parent],
        [32767],
        ["consumer"],
        ["record-32767"],
        [_jsonb({"flow_id": str(flow)})],
        [f"workflow:{flow}"],
        [f"outbox:{flow}:32767"],
        [0],
        [None],
        [str(flow)],
        [5.0],
        [3600.0],
        ["exponential"],
        [0.2],
    )
    panel = await wf_conn.fetchrow(
        _NODE_PANEL_CHILD_SQL.format(schema=wf_schema), "consumer", flow, 32767
    )
    assert panel is not None
    assert (
        await wf_conn.fetchrow(
            _NODE_PANEL_CHILD_SQL.format(schema=wf_schema), "consumer", flow, 40000
        )
        is None
    ), "the bigint bind must also make an old-schema 40000 read a safe miss"

    # _sql_finalize.py:278 — the live emit at the boundary (the 14-bind
    # emit statement).
    emitted = await emit_batch(
        wf_pool,
        wf_sql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([32766, 32767]),
        cursor={"page": 0},
    )
    assert len(emitted) == 2
    keys = await wf_conn.fetch(
        f'SELECT idempotency_key FROM "{wf_schema}".jobs WHERE parent_id = $1 ORDER BY map_index',
        source,
    )
    assert [row["idempotency_key"] for row in keys] == [
        f"wf:{flow}:emit:32766:screen",
        f"wf:{flow}:emit:32767:screen",
    ]
