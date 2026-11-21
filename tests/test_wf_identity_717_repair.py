"""The widened record identity's door pins (the fast corrective tier).

Every write door that asserts or persists ``map_index`` must resolve the
domain from the five live carriers instead of a compile-time 16-bit constant:
the emit, ``insert_fork``, ``insert_node``, and the map/route finalize forks.
The join counter ``jobs.deps_pending`` is a SEPARATE ``smallint`` bound and
must stay separate.

The fixtures are real PostgreSQL. ``wf_*`` is the fully migrated (post-POST,
all-``bigint``) module schema; the ``pre_only_*`` tests build a throwaway
schema through the product runner's ``phase="pre"`` path so the pre-POST
(all-``smallint``) window is exercised as a real schema state.
"""

# ruff: noqa: S608  # Why: the f-strings interpolate only fixture-derived throwaway schema identifiers; every value is $-bound.

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, cast

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.constants import BIGINT_MAX, SMALLINT_MAX
from taskq.migrate import apply_pending
from taskq.workflows import FlowRunner
from taskq.workflows._emit import MapIndexExhaustedError, emit_batch
from taskq.workflows._fork import insert_fork
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import (
    ChildSpec,
    EmitChild,
    ForkSpec,
    JoinSpec,
    MapIndexSchemaError,
    NodeSpec,
    resolve_map_index_ceiling,
    resolve_map_index_domain,
)
from taskq.workflows.definitions import (
    FanInBoundExceededError,
    validate_fork,
)
from taskq.workflows.engine import insert_node, render_workflow_sql
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

pytestmark = pytest.mark.integration

CARRIERS: tuple[str, ...] = (
    "jobs",
    "jobs_archive",
    "wf_step_ledger",
    "wf_outbox",
    "wf_signals",
)


# ── helpers ──────────────────────────────────────────────────────────────


async def _carrier_widths(conn: asyncpg.Connection, schema: str) -> dict[str, str]:
    rows = await conn.fetch(
        """
        SELECT c.relname AS table_name, format_type(a.atttypid, a.atttypmod) AS type_name
        FROM pg_attribute AS a
        JOIN pg_class AS c ON c.oid = a.attrelid
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND a.attname = 'map_index'
          AND c.relname = ANY($2::text[])
        """,
        schema,
        list(CARRIERS),
    )
    return {row["table_name"]: row["type_name"] for row in rows}


def _page(indices: list[int], *, step_key: str = "screen") -> list[EmitChild]:
    return [
        EmitChild(
            step_key=step_key,
            actor="wf",
            queue="default",
            payload={"record": index},
            trace_id=f"record-{index}",
            map_index=index,
        )
        for index in indices
    ]


async def _emit_page(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    conn: asyncpg.Connection,
    schema: str,
    indices: list[int],
    *,
    step_key: str = "source",
) -> tuple[JobId, ...]:
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key=step_key)
    worker, attempt, epoch = await claim_view(conn, schema, source)
    return await emit_batch(
        pool,
        wsql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=_page(indices),
        cursor={"page": 0},
    )


@pytest_asyncio.fixture
async def pre_only_schema(pg_dsn: str) -> AsyncIterator[tuple[asyncpg.Connection, str]]:
    """A throwaway schema migrated only to the PRE phase (all-smallint)."""
    schema = "pre_only_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        yield conn, schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


@pytest_asyncio.fixture
async def pre_only_pool(
    pg_dsn: str, pre_only_schema: tuple[asyncpg.Connection, str]
) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        yield pool
    finally:
        await pool.close()


# ── the emit door: zero-probe fast path and one probe on the high page ───


async def test_ordinary_page_makes_zero_carrier_probes_and_high_page_one(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import taskq.workflows._emit as emit_module
    from taskq.workflows._types import resolve_map_index_domain as real_resolver

    calls: list[int] = []

    async def counting_resolver(conn: Any, schema: str) -> int:
        calls.append(1)
        return await real_resolver(conn, schema)

    monkeypatch.setattr(emit_module, "resolve_map_index_domain", counting_resolver)

    await _emit_page(wf_pool, wf_sql, wf_conn, wf_schema, [0, 1, 2])
    assert calls == [], "an ordinary 0..32767 page must not read the catalog"

    await _emit_page(wf_pool, wf_sql, wf_conn, wf_schema, [32768])
    assert calls == [1], "a high page resolves the five-carrier domain exactly once"


async def test_pre_post_schema_keeps_the_typed_ceiling_and_admits_low_pages(
    pre_only_schema: tuple[asyncpg.Connection, str],
    pre_only_pool: asyncpg.Pool,
) -> None:
    conn, schema = pre_only_schema
    wsql = render_workflow_sql(schema)
    assert set(await _carrier_widths(conn, schema)) == set(CARRIERS)

    # The widened ::bigint[] identity binds work against the still-smallint
    # columns for the whole legacy window (0..32767).
    await _emit_page(pre_only_pool, wsql, conn, schema, [0, SMALLINT_MAX])

    with pytest.raises(MapIndexExhaustedError, match=str(SMALLINT_MAX)):
        await _emit_page(pre_only_pool, wsql, conn, schema, [SMALLINT_MAX + 1])


async def test_mixed_carrier_shape_refuses_the_whole_batch_before_writing(
    pre_only_schema: tuple[asyncpg.Connection, str],
    pre_only_pool: asyncpg.Pool,
) -> None:
    conn, schema = pre_only_schema
    wsql = render_workflow_sql(schema)
    await conn.execute(f'ALTER TABLE "{schema}".jobs_archive ALTER COLUMN map_index TYPE integer')
    flow = await seed_flow(conn, schema)
    source = await seed_running_node(conn, schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(conn, schema, source)

    with pytest.raises(MapIndexSchemaError, match="unsupported"):
        await emit_batch(
            pre_only_pool,
            wsql,
            flow_id=flow,
            source_id=source,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=_page([0, 32768]),
            cursor={"page": 0},
        )
    rows = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE parent_id = $1', source)
    assert int(rows or 0) == 0, "a mixed carrier shape wrote rows before refusing"


async def test_post_migration_preserves_rows_and_activates_high_identity(
    pre_only_schema: tuple[asyncpg.Connection, str],
    pre_only_pool: asyncpg.Pool,
) -> None:
    conn, schema = pre_only_schema
    wsql = render_workflow_sql(schema)

    # Pre-existing identities in all five carriers, including NULLs.
    child_ids = await _emit_page(pre_only_pool, wsql, conn, schema, [SMALLINT_MAX])
    child = child_ids[0]
    flow_id = await conn.fetchval(
        f"SELECT (metadata->>'flow_id')::uuid FROM \"{schema}\".jobs WHERE id = $1", child
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs_archive '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, expire_at, map_index) "
        "VALUES ($1, 'wf', 'default', '{}', 'succeeded', 3, 'transient', now(), $2)",
        new_uuid(),
        SMALLINT_MAX,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs_archive '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, expire_at, map_index) "
        "VALUES ($1, 'wf', 'default', '{}', 'succeeded', 3, 'transient', now(), NULL)",
        new_uuid(),
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, status) "
        "VALUES ($1, $2, $3, 'kept', $4, 1, 'succeeded')",
        new_uuid(),
        flow_id,
        child,
        SMALLINT_MAX,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_outbox '
        "(id, join_job_id, flow_id, consumer_step_key, map_index) "
        "VALUES ($1, $2, $2, 'consumer', $3)",
        new_uuid(),
        child,
        None,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status, map_index) "
        "VALUES ($1, $2, 'node', 'sig', 0, 'call', 'resolved', $3)",
        new_uuid(),
        flow_id,
        None,
    )

    await apply_pending(conn, schema=schema, target="01.00.42_02")
    assert await _carrier_widths(conn, schema) == dict.fromkeys(CARRIERS, "bigint")

    preserved = {
        "jobs": await conn.fetchval(
            f'SELECT map_index::text FROM "{schema}".jobs WHERE map_index IS NOT NULL LIMIT 1'
        ),
        "jobs_archive": await conn.fetchval(
            f'SELECT map_index::text FROM "{schema}".jobs_archive '
            "WHERE map_index IS NOT NULL LIMIT 1"
        ),
        "wf_step_ledger": await conn.fetchval(
            f"SELECT map_index::text FROM \"{schema}\".wf_step_ledger WHERE step_key = 'kept'"
        ),
        "wf_outbox": await conn.fetchval(
            f'SELECT map_index::text FROM "{schema}".wf_outbox '
            "WHERE consumer_step_key = 'consumer'"
        ),
        "wf_signals": await conn.fetchval(
            f"SELECT map_index::text FROM \"{schema}\".wf_signals WHERE signal_name = 'sig'"
        ),
    }
    assert preserved["jobs"] == "32767"
    assert preserved["jobs_archive"] == "32767"
    assert preserved["wf_step_ledger"] == "32767"
    assert preserved["wf_outbox"] is None
    assert preserved["wf_signals"] is None

    # Activation in the same process/schema, no restart: 32768 now emits.
    await _emit_page(pre_only_pool, wsql, conn, schema, [32768])


async def test_diagnostic_ceiling_keeps_the_minimum_on_a_mixed_schema(
    pre_only_schema: tuple[asyncpg.Connection, str],
) -> None:
    """The inspection helper reports the minimum; authoring refuses the mix.

    Widening one carrier (``jobs``) leaves a mixed schema: the diagnostic
    ``resolve_map_index_ceiling`` truthfully reports the narrower supported
    domain (32767) for inspection, while ``resolve_map_index_domain`` — the
    authoring contract — refuses the inconsistent shape outright.
    """
    conn, schema = pre_only_schema
    await conn.execute(f'ALTER TABLE "{schema}".jobs ALTER COLUMN map_index TYPE bigint')

    assert await resolve_map_index_ceiling(conn, schema) == SMALLINT_MAX
    with pytest.raises(MapIndexSchemaError, match="mixed"):
        await resolve_map_index_domain(conn, schema)


async def test_resolver_propagates_transport_fault_unwrapped() -> None:
    class _BoomConn:
        async def fetch(self, *_args: object, **_kwargs: object) -> object:
            raise ConnectionError("connection lost during the catalog read")

    with pytest.raises(ConnectionError):
        await resolve_map_index_domain(cast("Any", _BoomConn()), "some_schema")


# ── engine write doors ───────────────────────────────────────────────────


async def test_insert_fork_and_node_admit_high_identity_on_a_widened_schema(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    fork = ForkSpec(
        children=(
            ChildSpec(step_key="m.item", actor="wf", queue="default", map_index=32768),
            ChildSpec(step_key="m.item", actor="wf", queue="default", map_index=BIGINT_MAX),
        )
    )
    ids, _join = await insert_fork(
        wf_conn,
        wf_sql,
        flow_id=flow,
        parent_id=flow,
        parent_step_key="m",
        fork=fork,
    )
    stored = await wf_conn.fetch(
        f'SELECT map_index FROM "{wf_schema}".jobs WHERE id = ANY($1::uuid[]) ORDER BY map_index',
        ids,
    )
    assert [int(row["map_index"]) for row in stored] == [32768, BIGINT_MAX]

    node_id = await insert_node(
        wf_conn,
        wf_sql,
        NodeSpec(
            flow_id=flow,
            step_key="high",
            actor="wf",
            queue="default",
            map_index=32768,
        ),
    )
    assert (
        await wf_conn.fetchval(f'SELECT map_index FROM "{wf_schema}".jobs WHERE id = $1', node_id)
        == 32768
    )


async def test_insert_fork_and_node_refuse_high_identity_on_a_pre_post_schema(
    pre_only_schema: tuple[asyncpg.Connection, str],
) -> None:
    conn, schema = pre_only_schema
    wsql = render_workflow_sql(schema)
    flow = await seed_flow(conn, schema)
    before = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')

    with pytest.raises(MapIndexExhaustedError, match=str(SMALLINT_MAX)):
        await insert_fork(
            conn,
            wsql,
            flow_id=flow,
            parent_id=flow,
            parent_step_key="m",
            fork=ForkSpec(
                children=(
                    ChildSpec(step_key="m.item", actor="wf", queue="default", map_index=32768),
                )
            ),
        )
    with pytest.raises(MapIndexExhaustedError, match=str(SMALLINT_MAX)):
        await insert_node(
            conn,
            wsql,
            NodeSpec(
                flow_id=flow,
                step_key="high",
                actor="wf",
                queue="default",
                map_index=32768,
            ),
        )
    after = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
    assert int(after) == int(before), "a refused high identity wrote rows"


# ── the X3 split: join counter vs record identity ────────────────────────


def test_child_driven_join_counter_bound_is_separate_from_identity() -> None:
    join = JoinSpec(step_key="j", actor="wf", queue="default", child_driven=True)

    def fork(n: int) -> ForkSpec:
        return ForkSpec(
            children=tuple(
                ChildSpec(step_key=f"c{i}", actor="wf", queue="default") for i in range(n)
            ),
            join=join,
        )

    # The join counter can hold exactly SMALLINT_MAX unterminal children.
    validate_fork(fork(SMALLINT_MAX))
    with pytest.raises(FanInBoundExceededError, match="deps_pending"):
        validate_fork(fork(SMALLINT_MAX + 1))


# ── the map finalize fork consults the caller-resolved bound ─────────────


def test_map_fork_uses_the_resolved_ceiling() -> None:
    runner = FlowRunner.__new__(FlowRunner)

    with pytest.raises(MapIndexExhaustedError, match="identity ceiling"):
        runner._map_fork(  # pyright: ignore[reportPrivateUsage]  # Why: the fork builder is the seam this pin protects; no DB state is read before the guard.
            {"step_key": "m"}, None, {"value": list(range(4))}, map_index_ceiling=2
        )


# ── the admin child-addressing door ──────────────────────────────────────


async def test_admin_node_panel_resolves_identity_against_the_live_schema(
    pre_only_schema: tuple[asyncpg.Connection, str],
    pre_only_pool: asyncpg.Pool,
) -> None:
    pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
    from fastapi import FastAPI, HTTPException

    from taskq.web.admin import create_router, setup_admin_state

    async def _allow(_request: Any) -> None:
        return None

    conn, schema = pre_only_schema
    app = FastAPI()
    bundle = create_router(pre_only_pool, schema=schema, base_path="", auth_dependency=_allow)
    setup_admin_state(app, bundle)
    endpoint = next(
        route.endpoint  # pyright: ignore[reportAttributeAccessIssue]  # Why: the matched route is an APIRoute (which carries ``endpoint``); BaseRoute does not declare it.
        for route in bundle.router.routes
        if getattr(route, "name", "") == "node_panel"
    )
    run_id = new_uuid()

    # Pre-POST (all-smallint): 40000 can never name a child — the typed 400,
    # not the int2 codec's overflow 500 and not a silent fall-through.
    with pytest.raises(HTTPException) as narrow:
        await endpoint(run_id, "review", pre_only_pool, schema, 40000)
    assert narrow.value.status_code == 400
    assert str(SMALLINT_MAX) in str(narrow.value.detail)

    # A broken carrier shape is a typed deployment refusal, never a 500.
    await conn.execute(f'ALTER TABLE "{schema}".jobs_archive ALTER COLUMN map_index TYPE integer')
    with pytest.raises(HTTPException) as broken:
        await endpoint(run_id, "review", pre_only_pool, schema, 40000)
    assert broken.value.status_code == 503


# ── the finalize face routes deterministic refusals to the ladder ────────


async def test_miss_finalize_identity_refusal_terminalizes_not_reclaims(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic import BaseModel

    from taskq.workflows import Promise, WorkflowApp, build, step

    class _In(BaseModel):
        value: int

    async def _body(_ctx: Any, params: _In) -> _In:
        return params

    app = WorkflowApp()
    name = f"identity-finalize-{new_base62(10)}"

    @app.workflow(name)
    def _flow() -> Promise[object]:
        return build(step(_body, _In(value=1), key="only"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow = (await runner.create_flow()).flow_id

    async def _refuse(*_args: object, **_kwargs: object) -> Any:
        raise MapIndexExhaustedError("the finalize face refused the identity")

    monkeypatch.setattr(runner, "_finalize_success", _refuse)
    await runner.drive(flow)

    row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'only'",
        flow,
    )
    assert row is not None
    assert row["status"] == "failed", (
        "a deterministic finalize refusal must terminal-fail, never re-pend into "
        f"machinery reclaim: {dict(row)}"
    )
    assert row["error_class"] == "MapIndexExhaustedError"


async def test_widened_claim_arbiter_infers_on_conflict_at_32768(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    from taskq.workflows.ledger import claim_step_ledger

    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    _worker, attempt, epoch = await claim_view(wf_conn, wf_schema, node)

    async with wf_pool.acquire() as conn:
        first = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=node,
            step_key="source",
            map_index=32768,
            attempt=attempt,
            claim_epoch=int(epoch),
        )
        second = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=node,
            step_key="source",
            map_index=32768,
            attempt=attempt,
            claim_epoch=int(epoch),
        )
    assert first.ledger_id == second.ledger_id, (
        "the recreated bigint claim arbiter must infer the ON CONFLICT target; a "
        "stale int4 rendering fails 42P10, and a mismatched expression would "
        "duplicate the claim row"
    )
    rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'source' AND map_index = 32768",
        flow,
    )
    assert int(rows) == 1


async def test_non_exact_int_identity_is_poison_and_the_valid_mate_commits(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)

    valid = _page([8])[0]
    poison = _page([7])[0]
    object.__setattr__(poison, "map_index", 7.0)  # bypass the spec's own door
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
    assert "exact int" in str(raised.value)
    rows = await wf_conn.fetch(
        f'SELECT map_index FROM "{wf_schema}".jobs WHERE parent_id = $1', source
    )
    assert [int(row["map_index"]) for row in rows] == [8]
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

    first = _page([1])[0]
    object.__setattr__(first, "map_index", True)
    second = _page([2])[0]
    object.__setattr__(second, "map_index", "8")
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
    rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1', source
    )
    assert int(rows or 0) == 0


async def test_chain_finalize_identity_refusal_terminalizes_not_reclaims(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic import BaseModel

    from taskq.workflows import Promise, WorkflowApp, build, step

    class _In(BaseModel):
        value: int

    async def _body(_ctx: Any, params: _In) -> _In:
        return params

    app = WorkflowApp()
    name = f"identity-chain-finalize-{new_base62(10)}"

    @app.workflow(name)
    def _flow() -> Promise[object]:
        return build(step(_body, _In(value=1), key="only"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    # A chain step row resolves its route from the compiled chains; the branch
    # under test is the chain finalize face, not chain dispatch.
    runner._chain_steps = {"only": object()}  # pyright: ignore[reportAttributeAccessIssue,reportPrivateUsage]  # Why: installs the branch's precondition on the runner host.
    flow = (await runner.create_flow()).flow_id

    async def _refuse(*_args: object, **_kwargs: object) -> Any:
        raise MapIndexExhaustedError("the chain finalize face refused the identity")

    async def _wrong_face(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("the miss/cache finalize face ran instead of the chain face")

    monkeypatch.setattr(runner, "_finalize_chain_step", _refuse)
    monkeypatch.setattr(runner, "_finalize_success", _wrong_face)
    await runner.drive(flow)

    row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'only'",
        flow,
    )
    assert row is not None
    assert row["status"] == "failed", dict(row)
    assert row["error_class"] == "MapIndexExhaustedError"


async def test_cache_hit_finalize_identity_refusal_terminalizes_not_reclaims(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic import BaseModel

    from taskq.workflows import Promise, WorkflowApp, build, step

    class _In(BaseModel):
        value: int

    calls: list[int] = []

    async def _body(_ctx: Any, params: _In) -> _In:
        calls.append(1)
        return params

    app = WorkflowApp()
    name = f"identity-cache-finalize-{new_base62(10)}"

    @app.workflow(name)
    def _flow() -> Promise[object]:
        return build(step(_body, _In(value=1), key="only", cache=True))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    first_flow = (await runner.create_flow()).flow_id
    await runner.drive(first_flow)
    assert calls == [1], "the first run must execute the body and fill the cache"

    async def _refuse(*_args: object, **_kwargs: object) -> Any:
        raise MapIndexExhaustedError("the cache-hit finalize face refused the identity")

    monkeypatch.setattr(runner, "_finalize_success", _refuse)
    second_flow = (await runner.create_flow()).flow_id
    await runner.drive(second_flow)
    assert calls == [1], "the second run must be a cache HIT (the body never reruns)"

    row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'only'",
        second_flow,
    )
    assert row is not None
    assert row["status"] == "failed", dict(row)
    assert row["error_class"] == "MapIndexExhaustedError"


# ── the two carriers no other pin writes a high identity through ─────────


async def test_high_identity_outbox_drain_lands_the_consumer_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """``wf_outbox`` carries a fire's identity to the consumer insert.

    The drain's ``$8::bigint[]`` bind and the widened column must both hold
    the domain's maximum end to end: the outbox row, the drained consumer
    ``jobs`` row, and the identity it lands with. A carrier widened in DDL
    but narrowed on its own write path is the defect class this ticket
    exists for (a widened ``jobs`` alone is not permission).
    """
    import json

    from taskq.workflows._sweep import drain_outbox
    from taskq.workflows._types import ConsumerBinding, _consumer_binding_doc

    flow = await seed_flow(wf_conn, wf_schema)
    binding_doc = _consumer_binding_doc(
        ConsumerBinding(step_key="consumer", actor="wf", queue="default")
    )
    await wf_conn.execute(
        wf_sql.outbox_insert,
        new_uuid(),
        flow,
        flow,
        "consumer",
        BIGINT_MAX,
        json.dumps(binding_doc),
    )

    delivered = await drain_outbox(
        wf_pool, wf_sql, default_max_attempts=3, default_retry_kind="transient"
    )
    assert delivered >= 1, delivered
    row = await wf_conn.fetchrow(
        f'SELECT map_index FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'consumer'",
        flow,
    )
    assert row is not None and int(row["map_index"]) == BIGINT_MAX, dict(row) if row else None


async def test_high_identity_signal_holds_discriminate_map_siblings(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
) -> None:
    """``wf_signals`` keys its hold arbiter on the same identity.

    ``register_hold``'s own contract — map siblings of one source hold
    CONCURRENTLY as DISTINCT rows — is the per-item approval shape, so the
    recreated ``wf_signals_hold_identity_uniq`` must discriminate at the
    domain's maximum: two high siblings land as two rows, the same high
    sibling held twice is the arbiter's collision, and the recreated
    expression carries no ``int4`` coercion of the identity (the
    botched-widening rendering that overflows a high sibling).
    """
    from taskq.workflows.api._hitl import register_hold

    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow, step_key="review")

    indexdef = await wf_conn.fetchval(
        "SELECT pg_get_indexdef(($1 || '.wf_signals_hold_identity_uniq')::regclass)",
        f'"{wf_schema}"',
    )
    assert indexdef is not None
    assert "(map_index)::integer" not in str(indexdef), indexdef

    payload_schema: dict[str, object] = {"Approval": {"type": "object", "properties": {}}}
    first = await register_hold(
        wf_conn,
        schema=wf_schema,
        workflow_id=flow,
        node_id=node,
        node_key="review",
        signal_name="Approval",
        hold_epoch=0,
        call_id="call:high-sibling-a",
        payload_schema=payload_schema,
        timeout_s=120.0,
        map_index=BIGINT_MAX,
    )
    second = await register_hold(
        wf_conn,
        schema=wf_schema,
        workflow_id=flow,
        node_id=JobId(new_uuid()),
        node_key="review",
        signal_name="Approval",
        hold_epoch=0,
        call_id="call:high-sibling-b",
        payload_schema=payload_schema,
        timeout_s=120.0,
        map_index=BIGINT_MAX - 1,
    )
    assert first != second
    stored = await wf_conn.fetch(
        f'SELECT map_index FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 ORDER BY map_index',
        flow,
    )
    assert [int(r["map_index"]) for r in stored] == [BIGINT_MAX - 1, BIGINT_MAX], [
        dict(r) for r in stored
    ]

    with pytest.raises(asyncpg.UniqueViolationError):
        await register_hold(
            wf_conn,
            schema=wf_schema,
            workflow_id=flow,
            node_id=JobId(new_uuid()),
            node_key="review",
            signal_name="Approval",
            hold_epoch=0,
            call_id="call:high-sibling-duplicate",
            payload_schema=payload_schema,
            timeout_s=120.0,
            map_index=BIGINT_MAX,
        )
