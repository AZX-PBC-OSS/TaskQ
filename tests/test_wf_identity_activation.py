"""#717 P3 identity activation/migration controls.

The independent control author captured six genuine failures and one old-schema
green before product edits. This file now states and executes the shipping
post-migration contract, with the original base failure history preserved in
/tmp/opencode's immutable red artifacts:

* the old smallint schema keeps the existing smallint ceiling — 32,767 lands,
  32,768 is a typed poison record and its valid page-mate survives (this is
  the functional old-schema control; it passes on the current tree);
* an all-bigint five-carrier schema activates the bigint identity WITHOUT a
  restart: a mixed page commits 32,767 and 32,768 with exact parent/key bytes,
  writes both ledger attempts, and a same-process re-emit collides as
  ``PageDivergedError`` instead of re-executing or dropping a record;
* the shipping path is the ACTUAL bundled post migration — discovered by its
  five ALTER statements and its ``post`` phase, never by a frozen filename —
  applied through the real ``apply_pending`` runner on a fresh schema; the
  five catalog columns are ``bigint``/nullable and the migration-ledger row is
  the file's own checksum;
* a deliberately mixed partial schema after post (``jobs_archive`` left at
  smallint, the other four bigint) fails CLOSED as ``MapIndexSchemaError`` with
  zero low mate, child, cursor, edge, ledger, outbox or signal writes — never
  poison-mate semantics;
* ``BIGINT_MAX`` itself binds, serializes, stores and reads back through the
  real emit + ledger statements; ``BIGINT_MAX + 1`` is an exact-int
  ``MapIndexExhaustedError`` before any write, never a database/driver
  surprise;
* equal numeric identities at 32,768 in two separate flows remain distinct
  rows, keys and ledger claims (the root/flow scope is the collision boundary);
* the post migration preserves pre-existing 32,767 and NULL values in all
  five carriers.

Every schema below is a throwaway per-test schema created by the fixture and
migrated by the real ``apply_pending`` path; the fixture drops it whole at
teardown. The partial/mixed states are produced only by ALTER/DROP on that
throwaway schema. No product, migration, source or existing test file is
edited by this module. Only synthetic generic markers are used.
"""

# ruff: noqa: S608  # Why: the f-string SQL interpolates only the fixture's throwaway schema identifier and the product's own carrier names; every value is $-bound.

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from typing import cast

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.constants import BIGINT_MAX
from taskq.migrate import Migration, apply_pending, discover
from taskq.workflows._emit import (
    EMIT_MAX_IN_FLIGHT_DEFAULT,
    MapIndexExhaustedError,
    PageDivergedError,
    emit_batch,
)
from taskq.workflows._fork import insert_fork
from taskq.workflows._types import (
    _MAP_INDEX_IDENTITY_TABLES,
    ChildSpec,
    EmitChild,
    ForkSpec,
    MapIndexSchemaError,
    NodeSpec,
    resolve_map_index_ceiling,
)
from taskq.workflows.engine import insert_node, render_workflow_sql
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

pytestmark = pytest.mark.integration

#: The exact five identity carriers, hardcoded here so this control cannot
#: silently shrink if the product tuple is edited; the helper below pins the
#: two sets together.
FIVE_CARRIERS: tuple[str, ...] = (
    "jobs",
    "jobs_archive",
    "wf_step_ledger",
    "wf_outbox",
    "wf_signals",
)

#: The last smallint identity and the first bigint-only identity.
LAST_SMALLINT: int = 32767
FIRST_BIGINT: int = 32768


def _assert_product_carrier_set() -> None:
    """The product's resolver tuple must still be exactly these five names.

    Hardcoding our own tuple above makes every widening/discovery assertion
    in this file independent of a product tuple that could shrink; this check
    makes a drift a loud failure rather than a silently narrower test.
    """
    assert tuple(_MAP_INDEX_IDENTITY_TABLES) == FIVE_CARRIERS, (
        "the product's identity carrier tuple drifted from the five-column "
        f"contract: {_MAP_INDEX_IDENTITY_TABLES!r}"
    )


# ── the throwaway pre-migration schema fixture ──────────────────────────


@pytest_asyncio.fixture
async def pre_schema(pg_dsn: str) -> AsyncIterator[tuple[asyncpg.Connection, str]]:
    """One fresh schema per test, migrated through the REAL pre phase.

    ``apply_pending(..., phase="pre")`` is the product runner, not a
    hand-built schema. The fixture names the schema it supplies; teardown
    drops it whole. Tests that need the P3 post state explicitly apply the
    REAL bundled post file through the same runner.
    """
    schema = "idr3_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        yield conn, schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── post-migration discovery: content and phase, never a frozen name ────

_ALTER_IDENTITY_RE = re.compile(
    r"\bALTER\s+TABLE\s+(?:\"\{schema\}\"|\{schema\})\.\"?"
    r"(?P<table>jobs_archive|jobs|wf_step_ledger|wf_outbox|wf_signals)\"?"
    r"\s+ALTER\s+COLUMN\s+\"?map_index\"?\s+(?:SET\s+DATA\s+)?TYPE\s+bigint\b",
    re.IGNORECASE,
)


def _identity_post_migration() -> Migration:
    """The one bundled POST file that widens ALL FIVE carriers to bigint.

    Discovery is by CONTENT + PHASE, not by a version number reserved now:
    P3 has not allocated its filename yet. A missing file is a hard failure
    (never a skip) naming the current latest bundled migration so the P3
    author can allocate the post after it. Two matching files are ambiguous
    and also fail closed.
    """
    _assert_product_carrier_set()
    migrations = discover()
    matches: list[Migration] = []
    seen: dict[str, set[str]] = {}
    for migration in migrations:
        if migration.phase != "post":
            continue
        tables = {
            match.group("table").lower()
            for match in _ALTER_IDENTITY_RE.finditer(migration.sql_template)
        }
        if tables:
            seen[migration.filename] = tables
        if tables == set(FIVE_CARRIERS):
            matches.append(migration)

    if len(matches) == 1:
        return matches[0]
    latest = migrations[-1]
    observed = (
        ", ".join(f"{filename} -> {sorted(tables)}" for filename, tables in sorted(seen.items()))
        or "none"
    )
    raise AssertionError(
        "the P3 identity activation POST migration is not bundled: no post-phase "
        f"file alters all five carriers {list(FIVE_CARRIERS)} with "
        "ALTER COLUMN map_index TYPE bigint "
        f"(post files carrying any identity ALTER: {observed}). The current "
        f"latest bundled migration is {latest.filename!r}; P3 must allocate "
        "the real post file after the P2 migration. This control fails closed: "
        "absence is never a pass or a skip."
    )


async def _apply_identity_post(conn: asyncpg.Connection, schema: str) -> Migration:
    """Apply the discovered P3 post through the real runner and verify it.

    The post must be one transactional file (the five ALTERs and the
    migration-ledger row commit together), and the ledger's checksum must be
    the file's own rendered checksum. This is application through
    ``apply_pending`` — never a fixture-side raw ALTER standing in for the
    migration.
    """
    migration = _identity_post_migration()
    assert migration.use_transaction, (
        f"the P3 identity post {migration.filename!r} is marked "
        "non-transactional; the five-column widening and its ledger record "
        "must commit atomically in one transaction"
    )
    applied = await apply_pending(conn, schema=schema, phase="post")
    applied_keys = {item.key for item in applied}
    assert migration.key in applied_keys, (
        f"the discovered P3 post {migration.filename!r} ({migration.key}) was "
        f"not applied by the real post-phase runner; applied={sorted(applied_keys)}"
    )
    ledger = await conn.fetchrow(
        f'SELECT version, checksum FROM "{schema}".schema_migrations WHERE version = $1',
        migration.key,
    )
    assert ledger is not None, f"the P3 post {migration.key} left no schema_migrations row"
    assert ledger["checksum"] == migration.checksum(schema), (
        "the migration ledger checksum is not the bundled file's own checksum"
    )
    return migration


# ── catalog / state readers ─────────────────────────────────────────────


async def _map_index_columns(conn: asyncpg.Connection, schema: str) -> dict[str, tuple[str, str]]:
    _assert_product_carrier_set()
    rows = await conn.fetch(
        "SELECT table_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = $1 AND column_name = 'map_index' "
        "AND table_name = ANY($2::text[])",
        schema,
        list(FIVE_CARRIERS),
    )
    return {
        str(row["table_name"]): (str(row["data_type"]), str(row["is_nullable"])) for row in rows
    }


def _assert_all_bigint_nullable(observed: dict[str, tuple[str, str]]) -> None:
    assert set(observed) == set(FIVE_CARRIERS), (
        f"the five identity carriers are not all present: {sorted(observed)}"
    )
    assert {table: kind for table, (kind, _nullable) in observed.items()} == dict.fromkeys(
        FIVE_CARRIERS, "bigint"
    ), f"not all five identity carriers are bigint: {observed}"
    nullable = {table for table, (_kind, is_nullable) in observed.items() if is_nullable != "YES"}
    assert not nullable, f"identity carriers must stay nullable: {sorted(nullable)}"


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


async def _child_rows(
    conn: asyncpg.Connection, schema: str, source_id: JobId
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"SELECT id, parent_id, map_index, step_key, trace_id, payload, "
        f"idempotency_scope, idempotency_key, status, required_worker_schema "
        f'FROM "{schema}".jobs WHERE parent_id = $1 ORDER BY map_index',
        source_id,
    )


async def _cursor(conn: asyncpg.Connection, schema: str, source_id: JobId) -> object:
    raw = await conn.fetchval(
        f"SELECT metadata->'emit_cursor' FROM \"{schema}\".jobs WHERE id = $1",
        source_id,
    )
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


async def _table_count(
    conn: asyncpg.Connection, schema: str, table: str, *, flow_id: JobId | None = None
) -> int:
    if flow_id is None:
        return int(await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') or 0)
    if table == "jobs" or table == "jobs_archive":
        where = "WHERE (metadata->>'flow_id')::uuid = $1"
    elif table == "wf_step_ledger" or table == "wf_outbox":
        where = "WHERE flow_id = $1"
    elif table == "wf_signals":
        where = "WHERE workflow_id = $1"
    elif table == "wf_edge":
        where = "WHERE flow_id = $1"
    else:  # pragma: no cover - the helper's own closed vocabulary
        raise AssertionError(f"unexpected count table {table!r}")
    return int(
        await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}" {where}', flow_id) or 0
    )


async def _flow_counts(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> dict[str, int]:
    """Every authoring carrier's row count for one flow (the no-membership-
    change assertion): jobs, archive, edge, ledger, outbox, signals."""
    tables = ("jobs", "jobs_archive", "wf_edge", "wf_step_ledger", "wf_outbox", "wf_signals")
    return {table: await _table_count(conn, schema, table, flow_id=flow_id) for table in tables}


# ── the old-schema functional control (passes before P3) ────────────────


async def test_old_smallint_schema_refuses_32768_functionally(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
) -> None:
    """The code-before-post contract: the ordinary old schema still admits
    32,767 (exact key/parent bytes) and refuses 32,768 as a typed poison
    record whose valid mate commits and whose cursor advances. An all-poison
    page writes no child and no cursor.

    This is the retained half of
    ``test_wf_identity_p1.py::test_all_bigint_schema_does_not_activate_the_ceiling_in_this_slice``:
    P3 must not delete that old pin; this functional old-schema control is
    what the old-schema side becomes after activation.
    """
    conn, schema = pre_schema
    observed = await _map_index_columns(conn, schema)
    assert {kind for kind, _nullable in observed.values()} == {"smallint"}, observed
    assert await resolve_map_index_ceiling(conn, schema) == LAST_SMALLINT

    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    with pytest.raises(MapIndexExhaustedError) as raised:
        await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([LAST_SMALLINT, FIRST_BIGINT]),
            cursor={"page": 0},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
    assert type(raised.value) is MapIndexExhaustedError, (
        f"the old-schema refusal must be the typed ceiling error itself, got "
        f"{type(raised.value).__name__}: {raised.value}"
    )
    assert str(LAST_SMALLINT) in str(raised.value), str(raised.value)

    rows = await _child_rows(conn, schema, source)
    assert [row["map_index"] for row in rows] == [LAST_SMALLINT], (
        f"the valid old-schema page-mate did not survive: {[r['map_index'] for r in rows]}"
    )
    assert type(rows[0]["map_index"]) is int
    assert rows[0]["parent_id"] == source
    assert rows[0]["trace_id"] == f"record-{LAST_SMALLINT}"
    assert rows[0]["idempotency_key"] == f"wf:{flow}:emit:{LAST_SMALLINT}:screen"
    assert rows[0]["idempotency_scope"] == f"workflow:{flow}"
    assert rows[0]["status"] == "pending"
    assert rows[0]["required_worker_schema"] is None, (
        "a plain identity row must not acquire a worker-schema requirement"
    )
    assert await _cursor(conn, schema, source) == {"page": 0}
    assert await _table_count(conn, schema, "wf_edge", flow_id=flow) == 1

    # Negative all-invalid page: nothing written, cursor unchanged.
    with pytest.raises(MapIndexExhaustedError, match="nothing was written") as all_poison:
        await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([FIRST_BIGINT, FIRST_BIGINT + 1]),
            cursor={"page": 1},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
    assert type(all_poison.value) is MapIndexExhaustedError
    assert [row["map_index"] for row in await _child_rows(conn, schema, source)] == [
        LAST_SMALLINT
    ], "the all-poison page wrote a child"
    assert await _cursor(conn, schema, source) == {"page": 0}, (
        "the all-poison page advanced the cursor"
    )
    assert await _table_count(conn, schema, "wf_edge", flow_id=flow) == 1


# ── the missing-activation functional control (red on the current tree) ─


@pytest.mark.parametrize("kept_smallint", FIVE_CARRIERS, ids=FIVE_CARRIERS)
async def test_each_mixed_partial_before_post_refuses_whole_page(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
    kept_smallint: str,
) -> None:
    """Before POST, four manual bigint columns are not permission either.

    The resolver still exposes its minimum (O1); the public batch door raises
    the precise typed schema error and does not commit the low valid mate.
    """
    conn, schema = pre_schema
    for table in FIVE_CARRIERS:
        if table != kept_smallint:
            await conn.execute(
                f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN map_index TYPE bigint'
            )
    assert await resolve_map_index_ceiling(conn, schema) == LAST_SMALLINT
    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)
    before = await _flow_counts(conn, schema, flow)
    with pytest.raises(MapIndexSchemaError, match=kept_smallint) as raised:
        await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([8, FIRST_BIGINT]),
            cursor={"page": 0},
            _schema_conn=conn,
        )
    assert type(raised.value) is MapIndexSchemaError
    assert await _child_rows(conn, schema, source) == []
    assert await _cursor(conn, schema, source) is None
    assert await _flow_counts(conn, schema, flow) == before


async def test_manual_all_bigint_columns_activate_32768_without_restart(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
) -> None:
    """This was the genuine functional missing-activation red on the base:
    all five manually-bigint carriers still clamped at 32767. The shipping
    contract now commits 32767 and 32768 without a process restart.

    This manual ALTER state is a CONTROL only; the authoritative shipping
    proof is ``test_actual_post_migration_activates_32768_emit_ledger_replay``
    below, which applies the real bundled post migration.
    """
    conn, schema = pre_schema
    for table in FIVE_CARRIERS:
        await conn.execute(f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN map_index TYPE bigint')
    observed = await _map_index_columns(conn, schema)
    _assert_all_bigint_nullable(observed)
    assert await resolve_map_index_ceiling(conn, schema) == BIGINT_MAX

    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    ids = await emit_batch(
        wf_pool,
        wsql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([LAST_SMALLINT, FIRST_BIGINT]),
        cursor={"page": 0},
        max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
    )
    assert len(ids) == 2
    rows = await _child_rows(conn, schema, source)
    assert [row["map_index"] for row in rows] == [LAST_SMALLINT, FIRST_BIGINT], (
        "the all-bigint schema did not activate the 32768 identity: "
        f"{[row['map_index'] for row in rows]}"
    )
    assert await _cursor(conn, schema, source) == {"page": 0}


# ── the actual bundled post migration (the shipping proof) ─────────────


async def test_actual_post_migration_activates_32768_emit_ledger_replay(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
) -> None:
    """The real post file applied by the real runner makes the mixed page
    commit 32,767 and 32,768 with exact parent/idempotency bytes; the 32,768
    record claims, memoizes and replays through the real ledger; the same
    process's re-emit collides as ``PageDivergedError`` with no new row and
    an unchanged cursor."""
    conn, schema = pre_schema
    migration = await _apply_identity_post(conn, schema)
    assert migration.phase == "post"
    observed = await _map_index_columns(conn, schema)
    _assert_all_bigint_nullable(observed)
    assert await resolve_map_index_ceiling(conn, schema) == BIGINT_MAX

    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    ids = await emit_batch(
        wf_pool,
        wsql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([LAST_SMALLINT, FIRST_BIGINT]),
        cursor={"page": 0},
        max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
    )
    assert len(ids) == 2
    rows = await _child_rows(conn, schema, source)
    assert [row["map_index"] for row in rows] == [LAST_SMALLINT, FIRST_BIGINT], (
        "the post migration did not activate the 32768 identity: "
        f"{[row['map_index'] for row in rows]}"
    )
    assert [type(row["map_index"]) for row in rows] == [int, int]
    assert [row["parent_id"] for row in rows] == [source, source]
    assert [row["trace_id"] for row in rows] == [
        f"record-{LAST_SMALLINT}",
        f"record-{FIRST_BIGINT}",
    ]
    assert [row["idempotency_key"] for row in rows] == [
        f"wf:{flow}:emit:{LAST_SMALLINT}:screen",
        f"wf:{flow}:emit:{FIRST_BIGINT}:screen",
    ]
    assert [row["idempotency_scope"] for row in rows] == [f"workflow:{flow}"] * 2
    assert [row["status"] for row in rows] == ["pending", "pending"]
    assert [row["required_worker_schema"] for row in rows] == [None, None]
    assert json.loads(rows[1]["payload"]) == {"record": {"marker": FIRST_BIGINT}}
    assert await _cursor(conn, schema, source) == {"page": 0}
    assert await _table_count(conn, schema, "wf_edge", flow_id=flow) == 2

    # The other real authoring door: NodeSpec/insert_node must accept the
    # activated 32,768 identity too (the P1 constructor validation remains:
    # exact non-negative int, no coercion), and the row keeps the plain
    # identity's NULL worker-schema requirement.
    node_id = await insert_node(
        conn,
        wsql,
        NodeSpec(
            flow_id=flow,
            step_key="node-high",
            actor="wf",
            queue="default",
            map_index=FIRST_BIGINT,
        ),
    )
    node_row = await conn.fetchrow(
        f"SELECT map_index, required_worker_schema, parent_id, status "
        f'FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert node_row is not None
    assert node_row["map_index"] == FIRST_BIGINT
    assert type(node_row["map_index"]) is int
    assert node_row["required_worker_schema"] is None
    assert node_row["parent_id"] is None
    assert node_row["status"] == "pending"

    # The P1 constructor validation remains after activation: the door is
    # exact-int only and does not coerce a float that looks numeric.
    with pytest.raises(ValueError, match="exact int"):
        NodeSpec(
            flow_id=flow,
            step_key="bad-node",
            actor="wf",
            queue="default",
            map_index=cast("int", float(FIRST_BIGINT)),
        )

    # The 32,768 identity's own ledger row: claim, no terminal yet, terminal
    # exactly once, memoized replay returns the committed attempt.
    high_child = ids[1]
    high_claim = await conn.fetchrow(
        wsql.ledger_claim,
        new_uuid(),
        flow,
        high_child,
        "screen",
        FIRST_BIGINT,
        1,
        0,
    )
    assert high_claim is not None
    assert await conn.fetchrow(wsql.ledger_memoized, flow, "screen", FIRST_BIGINT) is None
    terminal = await conn.execute(
        wsql.ledger_terminal_by_id,
        high_claim["id"],
        "succeeded",
        None,
        None,
        None,
        None,
        high_claim["updated_at"],
    )
    assert terminal == "UPDATE 1", terminal
    memoized = await conn.fetchrow(wsql.ledger_memoized, flow, "screen", FIRST_BIGINT)
    assert memoized is not None
    assert memoized["attempt"] == 1
    assert memoized["status"] == "succeeded"
    assert memoized["job_id"] == high_child

    # Same-process idempotent replay: the semantic page is already committed,
    # so it surfaces PageDivergedError and never re-mints/double-executes.
    before_ids = [row["id"] for row in rows]
    with pytest.raises(PageDivergedError) as diverged:
        await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([LAST_SMALLINT, FIRST_BIGINT]),
            cursor={"page": 0},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
    assert type(diverged.value) is PageDivergedError, (
        f"a committed page re-emit must be PageDivergedError, got "
        f"{type(diverged.value).__name__}: {diverged.value}"
    )
    after_rows = await _child_rows(conn, schema, source)
    assert [row["id"] for row in after_rows] == before_ids, (
        "the replay minted or removed a child row"
    )
    assert len(after_rows) == 2
    assert [row["map_index"] for row in after_rows].count(FIRST_BIGINT) == 1
    assert await _cursor(conn, schema, source) == {"page": 0}


# ── mixed partial schema after post: fail closed, never poison-mate ─────


@pytest.mark.parametrize("kept_smallint", FIVE_CARRIERS, ids=FIVE_CARRIERS)
async def test_mixed_partial_carrier_after_post_refuses_whole_page(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
    kept_smallint: str,
) -> None:
    """After the real post, leaving ANY ONE required carrier at smallint
    makes a high-candidate page a typed ``MapIndexSchemaError``:
    zero low mate, zero child, no cursor advance, and no membership change in
    jobs/archive/edge/ledger/outbox/signals. The narrowed carrier already
    holds real referenced 32,767 history, so this is not a synthetic empty
    table comparison; the other four carriers remain bigint in the same
    schema/database (no cross-database minimum)."""
    conn, schema = pre_schema
    await _apply_identity_post(conn, schema)
    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    # Real pre-narrow history at the last smallint identity: an emitted live
    # child, its terminal ledger row, and an archive row referencing the flow.
    live_ids = await emit_batch(
        wf_pool,
        wsql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([LAST_SMALLINT]),
        cursor={"page": 0},
        max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
    )
    assert len(live_ids) == 1
    live_claim = await conn.fetchrow(
        wsql.ledger_claim, new_uuid(), flow, live_ids[0], "screen", LAST_SMALLINT, 1, 0
    )
    assert live_claim is not None
    terminal = await conn.execute(
        wsql.ledger_terminal_by_id,
        live_claim["id"],
        "succeeded",
        None,
        None,
        None,
        None,
        live_claim["updated_at"],
    )
    assert terminal == "UPDATE 1"
    await conn.execute(
        f'INSERT INTO "{schema}".jobs_archive '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, map_index, "
        "step_key, metadata, expire_at) "
        "VALUES ($1, 'wf', 'default', $2::jsonb, 'succeeded', 3, 'transient', $3, "
        "'screen', $4::jsonb, now() + interval '1 day')",
        new_uuid(),
        json.dumps({"record": {"marker": "archived"}}),
        LAST_SMALLINT,
        json.dumps({"flow_id": str(flow)}),
    )

    await conn.execute(
        f'ALTER TABLE "{schema}"."{kept_smallint}" ALTER COLUMN map_index TYPE smallint'
    )
    observed = await _map_index_columns(conn, schema)
    assert observed[kept_smallint] == ("smallint", "YES"), observed
    assert {table for table, (kind, _nullable) in observed.items() if kind == "bigint"} == set(
        FIVE_CARRIERS
    ) - {kept_smallint}
    assert await resolve_map_index_ceiling(conn, schema) == LAST_SMALLINT

    before_rows = await _child_rows(conn, schema, source)
    before_counts = await _flow_counts(conn, schema, flow)
    with pytest.raises(MapIndexSchemaError) as raised:
        await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([8, FIRST_BIGINT]),
            cursor={"page": 1},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
    assert type(raised.value) is MapIndexSchemaError, (
        f"a mixed partial schema must fail closed as MapIndexSchemaError, got "
        f"{type(raised.value).__name__}: {raised.value}"
    )
    assert kept_smallint in str(raised.value), str(raised.value)
    assert await _child_rows(conn, schema, source) == before_rows, (
        "the partial-schema refusal wrote the low page-mate or a child"
    )
    assert await _cursor(conn, schema, source) == {"page": 0}, (
        "the partial-schema refusal advanced the cursor"
    )
    assert await _flow_counts(conn, schema, flow) == before_counts, (
        "the partial-schema refusal changed membership/records"
    )

    # F3: the other raw authoring doors share the same one-query distinction,
    # not just emit. Neither can turn the mixed state into low/old-native mode.
    with pytest.raises(MapIndexSchemaError, match=kept_smallint):
        await insert_node(
            conn,
            wsql,
            NodeSpec(
                flow_id=flow,
                step_key="mixed-node",
                actor="wf",
                queue="default",
                map_index=FIRST_BIGINT,
            ),
        )
    with pytest.raises(MapIndexSchemaError, match=kept_smallint):
        await insert_fork(
            conn,
            wsql,
            flow_id=flow,
            parent_id=source,
            parent_step_key="source",
            fork=ForkSpec(
                children=(
                    ChildSpec(
                        step_key="mixed-child",
                        actor="wf",
                        queue="default",
                        map_index=FIRST_BIGINT,
                    ),
                )
            ),
        )
    assert await _flow_counts(conn, schema, flow) == before_counts


# ── the exact bigint domain ─────────────────────────────────────────────


async def test_bigint_max_round_trips_and_max_plus_one_is_typed_refusal(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
) -> None:
    """``BIGINT_MAX`` itself is a real bind/serialize/store/retrieve identity
    through the ordinary emit and ledger statements. ``BIGINT_MAX + 1`` is an
    exact-int ``MapIndexExhaustedError`` with no child, edge or cursor write —
    never an asyncpg/database surprise. Only one child is used; this test
    does not widen ``deps_pending``/fan-in or create one child per map index.
    """
    conn, schema = pre_schema
    await _apply_identity_post(conn, schema)
    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    ids = await emit_batch(
        wf_pool,
        wsql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([BIGINT_MAX]),
        cursor={"page": 0},
        max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
    )
    assert len(ids) == 1
    rows = await _child_rows(conn, schema, source)
    assert [row["map_index"] for row in rows] == [BIGINT_MAX], rows
    assert type(rows[0]["map_index"]) is int
    assert rows[0]["idempotency_key"] == f"wf:{flow}:emit:{BIGINT_MAX}:screen"
    assert rows[0]["parent_id"] == source
    assert rows[0]["required_worker_schema"] is None
    assert await _cursor(conn, schema, source) == {"page": 0}

    # The giant i64 identity in the ledger's claim arbiter is not treated
    # as a smallint anywhere (the terminal/fence are id+instant keyed).
    max_claim = await conn.fetchrow(
        wsql.ledger_claim, new_uuid(), flow, ids[0], "screen", BIGINT_MAX, 1, 0
    )
    assert max_claim is not None
    assert await conn.fetchrow(wsql.ledger_memoized, flow, "screen", BIGINT_MAX) is None
    terminal = await conn.execute(
        wsql.ledger_terminal_by_id,
        max_claim["id"],
        "succeeded",
        None,
        None,
        None,
        None,
        max_claim["updated_at"],
    )
    assert terminal == "UPDATE 1", terminal
    memoized = await conn.fetchrow(wsql.ledger_memoized, flow, "screen", BIGINT_MAX)
    assert memoized is not None
    assert memoized["job_id"] == ids[0]
    assert memoized["attempt"] == 1
    assert memoized["status"] == "succeeded"

    # A fresh flow/source for the out-of-domain page: zero writes, exact int,
    # typed refusal before any SQL reaches the database.
    flow_high = await seed_flow(conn, schema)
    source_high = await seed_running_node(conn, schema, flow_high, step_key="source")
    worker_high, attempt_high, epoch_high = await claim_view(conn, schema, source_high)
    before_counts = await _flow_counts(conn, schema, flow_high)
    with pytest.raises(MapIndexExhaustedError) as raised:
        await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow_high,
            source_id=source_high,
            worker_id=worker_high,
            attempt=attempt_high,
            claim_epoch=epoch_high,
            children=page_children([BIGINT_MAX + 1]),
            cursor={"page": 0},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
    assert type(raised.value) is MapIndexExhaustedError, (
        f"BIGINT_MAX+1 must be the typed identity refusal, got "
        f"{type(raised.value).__name__}: {raised.value}"
    )
    assert str(BIGINT_MAX) in str(raised.value), str(raised.value)
    assert str(BIGINT_MAX + 1) in str(raised.value), str(raised.value)
    assert await _child_rows(conn, schema, source_high) == []
    assert await _cursor(conn, schema, source_high) is None
    assert await _flow_counts(conn, schema, flow_high) == before_counts

    # F1: the exact mixed-mate shape, not only the separate positive and
    # all-poison brackets. BIGINT_MAX commits and checkpoints; its successor is
    # the exact typed poison and neither truncates nor rolls back the mate.
    flow_mixed = await seed_flow(conn, schema)
    source_mixed = await seed_running_node(conn, schema, flow_mixed, step_key="source")
    worker_mixed, attempt_mixed, epoch_mixed = await claim_view(conn, schema, source_mixed)
    with pytest.raises(MapIndexExhaustedError) as mixed_raised:
        await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow_mixed,
            source_id=source_mixed,
            worker_id=worker_mixed,
            attempt=attempt_mixed,
            claim_epoch=epoch_mixed,
            children=page_children([BIGINT_MAX, BIGINT_MAX + 1]),
            cursor={"page": 0},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
    assert type(mixed_raised.value) is MapIndexExhaustedError
    assert str(BIGINT_MAX + 1) in str(mixed_raised.value)
    mixed_rows = await _child_rows(conn, schema, source_mixed)
    assert [row["map_index"] for row in mixed_rows] == [BIGINT_MAX]
    assert mixed_rows[0]["idempotency_key"] == f"wf:{flow_mixed}:emit:{BIGINT_MAX}:screen"
    assert await _cursor(conn, schema, source_mixed) == {"page": 0}
    assert await _table_count(conn, schema, "wf_edge", flow_id=flow_mixed) == 1


# ── equal numeric identity in separate flows stays distinct ─────────────


async def test_equal_32768_in_two_flows_stays_distinct(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
) -> None:
    """The same numeric identity 32,768 in two separate flows must be two
    distinct jobs rows/keys and two distinct ledger claims — the flow/root
    scope is the equality boundary, never bare map_index."""
    conn, schema = pre_schema
    await _apply_identity_post(conn, schema)
    wsql = render_workflow_sql(schema)

    rows_by_flow: list[tuple[JobId, JobId, asyncpg.Record]] = []
    for _ in range(2):
        flow = await seed_flow(conn, schema)
        source = await seed_running_node(conn, schema, flow, step_key="source")
        worker, attempt, epoch = await claim_view(conn, schema, source)
        ids = await emit_batch(
            wf_pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children([FIRST_BIGINT]),
            cursor={"page": 0},
            max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
        )
        assert len(ids) == 1
        rows = await _child_rows(conn, schema, source)
        assert len(rows) == 1
        rows_by_flow.append((flow, ids[0], rows[0]))

    first_flow, first_id, first_row = rows_by_flow[0]
    second_flow, second_id, second_row = rows_by_flow[1]
    assert first_row["map_index"] == second_row["map_index"] == FIRST_BIGINT
    assert first_id != second_id
    assert first_row["id"] != second_row["id"]
    assert first_row["parent_id"] != second_row["parent_id"]
    assert first_row["idempotency_scope"] == f"workflow:{first_flow}"
    assert second_row["idempotency_scope"] == f"workflow:{second_flow}"
    assert first_row["idempotency_key"] == f"wf:{first_flow}:emit:{FIRST_BIGINT}:screen"
    assert second_row["idempotency_key"] == f"wf:{second_flow}:emit:{FIRST_BIGINT}:screen"
    assert first_row["idempotency_key"] != second_row["idempotency_key"]

    # The arbiter's full tuple includes flow_id: the same (step, map, attempt)
    # in two flows is two claims, and each terminal/memoized read finds its
    # own job.
    for flow, child_id in ((first_flow, first_id), (second_flow, second_id)):
        flow_claim = await conn.fetchrow(
            wsql.ledger_claim, new_uuid(), flow, child_id, "screen", FIRST_BIGINT, 1, 0
        )
        assert flow_claim is not None
        terminal = await conn.execute(
            wsql.ledger_terminal_by_id,
            flow_claim["id"],
            "succeeded",
            None,
            None,
            None,
            None,
            flow_claim["updated_at"],
        )
        assert terminal == "UPDATE 1", terminal
        memoized = await conn.fetchrow(wsql.ledger_memoized, flow, "screen", FIRST_BIGINT)
        assert memoized is not None
        assert memoized["job_id"] == child_id


# ── migration preservation of the old boundary and NULLs ────────────────


async def test_post_migration_preserves_seeded_32767_and_null_on_all_five(
    pre_schema: tuple[asyncpg.Connection, str],
    wf_pool: asyncpg.Pool,
) -> None:
    """Seed the last smallint identity and NULL in all five carriers BEFORE
    the real post, apply it, and read them back: every 32,767 value and every
    NULL survives while all five catalog columns become nullable bigint. The
    values are seeded through the real emit/ledger statements where those
    exist; archive/outbox/signals use their documented row shapes."""
    conn, schema = pre_schema
    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    # jobs: one emitted 32,767 child plus the NULL root/source rows.
    emitted = await emit_batch(
        wf_pool,
        wsql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children([LAST_SMALLINT]),
        cursor={"page": 0},
        max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
    )
    assert len(emitted) == 1

    # jobs_archive: one 32,767 row and one NULL row, both flow-referenced.
    for archive_map in (LAST_SMALLINT, None):
        await conn.execute(
            f'INSERT INTO "{schema}".jobs_archive '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, map_index, "
            "step_key, metadata, expire_at) "
            "VALUES ($1, 'wf', 'default', $2::jsonb, 'succeeded', 3, 'transient', $3, "
            "'screen', $4::jsonb, now() + interval '1 day')",
            new_uuid(),
            json.dumps({"record": {"marker": archive_map}}),
            archive_map,
            json.dumps({"flow_id": str(flow)}),
        )

    # wf_step_ledger: one 32,767 claim and one NULL claim (COALESCE keeps the
    # NULL identity a distinct arbiter row).
    await conn.execute(wsql.ledger_claim, new_uuid(), flow, source, "screen", LAST_SMALLINT, 1, 0)
    await conn.execute(wsql.ledger_claim, new_uuid(), flow, source, "screen", None, 1, 0)

    # wf_outbox: one 32,767 binding and one NULL binding.
    for outbox_map in (LAST_SMALLINT, None):
        await conn.execute(
            f'INSERT INTO "{schema}".wf_outbox '
            "(id, join_job_id, flow_id, consumer_step_key, map_index) "
            "VALUES ($1, $2, $3, 'consumer', $4)",
            new_uuid(),
            source,
            flow,
            outbox_map,
        )

    # wf_signals: one open 32,767 hold and one open NULL hold.
    for index, signal_map in enumerate((LAST_SMALLINT, None), start=1):
        await conn.execute(
            f'INSERT INTO "{schema}".wf_signals '
            "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status, map_index) "
            "VALUES ($1, $2, 'wait', 'approve', $3, $4, 'held', $5)",
            new_uuid(),
            flow,
            index,
            f"call-{index}",
            signal_map,
        )

    before = {
        "jobs": await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE map_index = $1', LAST_SMALLINT
        ),
        "jobs_null": await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE map_index IS NULL'
        ),
        "archive": await conn.fetch(
            f'SELECT map_index FROM "{schema}".jobs_archive '
            "WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY map_index NULLS FIRST",
            flow,
        ),
        "ledger": await conn.fetch(
            f'SELECT map_index FROM "{schema}".wf_step_ledger WHERE flow_id = $1 '
            "ORDER BY map_index NULLS FIRST",
            flow,
        ),
        "outbox": await conn.fetch(
            f'SELECT map_index FROM "{schema}".wf_outbox WHERE flow_id = $1 '
            "ORDER BY map_index NULLS FIRST",
            flow,
        ),
        "signals": await conn.fetch(
            f'SELECT map_index FROM "{schema}".wf_signals WHERE workflow_id = $1 '
            "ORDER BY map_index NULLS FIRST",
            flow,
        ),
    }
    assert int(before["jobs"] or 0) == 1
    assert [row["map_index"] for row in before["archive"]] == [None, LAST_SMALLINT]
    assert [row["map_index"] for row in before["ledger"]] == [None, LAST_SMALLINT]
    assert [row["map_index"] for row in before["outbox"]] == [None, LAST_SMALLINT]
    assert [row["map_index"] for row in before["signals"]] == [None, LAST_SMALLINT]

    await _apply_identity_post(conn, schema)
    observed = await _map_index_columns(conn, schema)
    _assert_all_bigint_nullable(observed)
    assert await resolve_map_index_ceiling(conn, schema) == BIGINT_MAX

    after = {
        "jobs": await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE map_index = $1', LAST_SMALLINT
        ),
        "jobs_null": await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE map_index IS NULL'
        ),
        "archive": await conn.fetch(
            f'SELECT map_index FROM "{schema}".jobs_archive '
            "WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY map_index NULLS FIRST",
            flow,
        ),
        "ledger": await conn.fetch(
            f'SELECT map_index FROM "{schema}".wf_step_ledger WHERE flow_id = $1 '
            "ORDER BY map_index NULLS FIRST",
            flow,
        ),
        "outbox": await conn.fetch(
            f'SELECT map_index FROM "{schema}".wf_outbox WHERE flow_id = $1 '
            "ORDER BY map_index NULLS FIRST",
            flow,
        ),
        "signals": await conn.fetch(
            f'SELECT map_index FROM "{schema}".wf_signals WHERE workflow_id = $1 '
            "ORDER BY map_index NULLS FIRST",
            flow,
        ),
    }
    assert after["jobs"] == before["jobs"] == 1
    assert after["jobs_null"] == before["jobs_null"]
    for carrier in ("archive", "ledger", "outbox", "signals"):
        assert [row["map_index"] for row in after[carrier]] == [
            row["map_index"] for row in before[carrier]
        ], f"{carrier} lost or changed a seeded 32767/NULL identity"
