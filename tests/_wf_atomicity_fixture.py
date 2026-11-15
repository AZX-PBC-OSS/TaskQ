# ruff: noqa: S608  # Schema/table identifiers are generated fixture constants; values are $-bound.
"""Real-PostgreSQL helpers for caller transactions and atomic settlements."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol, cast
from urllib.parse import urlencode, urlsplit, urlunsplit

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import ConnLike, JobId
from taskq.settings import WorkerSettings
from taskq.worker.run import deregister_worker, register_worker
from taskq.workflows import (
    Chain,
    CompiledWorkflow,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
)
from taskq.workflows._capability import WORKFLOW_EXECUTION_SCHEMA, create_guarded_flow
from taskq.workflows._types import EmitChild
from taskq.workflows.api._sql_runner import render_sql

TABLES = (
    "atomicity_domain",
    "atomicity_history",
    "atomicity_applied_cursor",
    "atomicity_stage",
    "atomicity_stage_cursor",
    "atomicity_deadlock",
    "atomicity_park_rollback_witness",
)


class JoinedEmit(Protocol):
    async def __call__(
        self,
        children: Sequence[EmitChild],
        *,
        cursor: dict[str, object],
        connection: ConnLike,
        schema: str,
    ) -> tuple[JobId, ...]: ...


class JoinedCursor(Protocol):
    async def __call__(self, *, connection: ConnLike, schema: str) -> dict[str, object]: ...


class TransactionalBody(Protocol):
    def __call__(
        self, body: Callable[..., Awaitable[object]], *args: object, key: str, max_attempts: int
    ) -> Promise[object]: ...


async def emit_joined(
    ctx: StepContext,
    children: Sequence[EmitChild],
    *,
    cursor: dict[str, object],
    connection: ConnLike,
    schema: str,
) -> tuple[JobId, ...]:
    """Use the supported explicit-schema, stable-session page door."""
    door = cast("JoinedEmit", ctx.emit_batch)
    return await door(children, cursor=cursor, connection=connection, schema=schema)


async def read_joined_cursor(
    ctx: StepContext, *, connection: ConnLike, schema: str
) -> dict[str, object]:
    door = cast("JoinedCursor", ctx.cursor)
    return await door(connection=connection, schema=schema)


def _transactional_barrier_door() -> object:
    import taskq.workflows as workflows

    return getattr(workflows, "transactional_step", None)


def transactional_barrier(
    body: Callable[..., Awaitable[object]], parent: Promise[object], *, key: str
) -> Promise[object]:
    """Declare an all-emitted settlement successor through the public door."""
    door = _transactional_barrier_door()
    if door is None:
        raise AssertionError(
            "taskq.workflows.transactional_step is absent — the public "
            "transactional declaration door is the only supported path"
        )
    return cast("TransactionalBody", door)(body, parent, key=key, max_attempts=1)


def native_barrier_declared() -> bool:
    return _transactional_barrier_door() is not None


def json_object(raw: object) -> object:
    return json.loads(raw) if isinstance(raw, str) else raw


def json_dict(raw: object) -> dict[str, object]:
    value = json_object(raw)
    assert isinstance(value, dict), f"expected a JSON object, got {type(value).__name__}"
    return cast("dict[str, object]", value)


def page(records: Sequence[int], *, step_key: str = "screen") -> list[EmitChild]:
    return [
        EmitChild(
            step_key=step_key,
            actor="wf",
            queue="default",
            payload={"record_id": record},
            trace_id=f"record-{record}",
            map_index=record,
        )
        for record in records
    ]


def execution_path(ctx: StepContext) -> str:
    """Name both public transaction door facts without replacing either behavior."""
    return (
        f"transactional_step={'present' if native_barrier_declared() else 'absent'}; "
        f"ctx.connection={'present' if getattr(ctx, 'connection', None) is not None else 'absent'}"
    )


@asynccontextmanager
async def managed_pg_pool(
    dsn: str,
    control: asyncpg.Connection,
    *,
    max_size: int,
    command_timeout: float | None = None,
    pool_factory: Callable[[str], Awaitable[asyncpg.Pool[asyncpg.Record]]] | None = None,
) -> AsyncGenerator[asyncpg.Pool[asyncpg.Record]]:
    """Close a labelled pool and prove that none of its backends survive."""
    address = urlsplit(dsn)
    label = new_label("atomicity_pool")
    query = urlencode({"application_name": label})
    scoped_dsn = urlunsplit(
        address._replace(query=f"{address.query}&{query}" if address.query else query)
    )
    if pool_factory is None:
        pool = await asyncpg.create_pool(
            scoped_dsn,
            min_size=1,
            max_size=max_size,
            command_timeout=command_timeout,
        )
    else:
        pool = await pool_factory(scoped_dsn)
    try:
        yield pool
    finally:
        await pool.close()
        survivors = int(
            await control.fetchval(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE application_name=$1 AND datname = current_database()",
                label,
            )
        )
        assert survivors == 0, f"pool {label} left {survivors} PostgreSQL backend(s)"


@asynccontextmanager
async def application_tables(conn: asyncpg.Connection, schema: str) -> AsyncGenerator[None]:
    definitions = (
        ("atomicity_domain", "flow_id uuid PRIMARY KEY,value text NOT NULL"),
        (
            "atomicity_history",
            "id bigserial PRIMARY KEY,flow_id uuid NOT NULL,note text NOT NULL",
        ),
        ("atomicity_applied_cursor", "flow_id uuid PRIMARY KEY,cursor jsonb NOT NULL"),
        (
            "atomicity_stage",
            "flow_id uuid NOT NULL,ordinal integer NOT NULL,digest text NOT NULL,"
            "PRIMARY KEY(flow_id,ordinal)",
        ),
        ("atomicity_stage_cursor", "flow_id uuid PRIMARY KEY,cursor jsonb NOT NULL"),
        (
            "atomicity_deadlock",
            "flow_id uuid NOT NULL,ordinal integer PRIMARY KEY,digest text NOT NULL",
        ),
        (
            "atomicity_park_rollback_witness",
            "flow_id uuid PRIMARY KEY,rolled_back boolean NOT NULL,"
            "observed_at timestamptz NOT NULL DEFAULT clock_timestamp()",
        ),
    )
    try:
        for table, definition in definitions:
            await conn.execute(f'CREATE TABLE IF NOT EXISTS "{schema}".{table} ({definition})')
        for table in reversed(TABLES):
            await conn.execute(f'DELETE FROM "{schema}".{table}')
        yield
    finally:
        for table in reversed(TABLES):
            await conn.execute(f'DROP TABLE IF EXISTS "{schema}".{table} CASCADE')


async def settlement_counts(conn: ConnLike, schema: str, flow_id: JobId) -> tuple[int, int, int]:
    return (
        int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".atomicity_domain WHERE flow_id=$1', flow_id
            )
        ),
        int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".atomicity_history WHERE flow_id=$1', flow_id
            )
        ),
        int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".atomicity_applied_cursor WHERE flow_id=$1',
                flow_id,
            )
        ),
    )


async def stage_count(conn: ConnLike, schema: str, flow_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".atomicity_stage WHERE flow_id=$1', flow_id
        )
    )


async def insert_domain(conn: ConnLike, schema: str, flow_id: JobId, value: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".atomicity_domain (flow_id,value) VALUES ($1,$2) '
        "ON CONFLICT (flow_id) DO UPDATE SET value=EXCLUDED.value",
        flow_id,
        value,
    )


async def insert_stage(conn: ConnLike, schema: str, flow_id: JobId, ordinal: int) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".atomicity_stage (flow_id,ordinal,digest) VALUES ($1,$2,$3)',
        flow_id,
        ordinal,
        f"page-{ordinal}-digest",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".atomicity_stage_cursor (flow_id,cursor) VALUES ($1,$2::jsonb) '
        "ON CONFLICT (flow_id) DO UPDATE SET cursor=EXCLUDED.cursor",
        flow_id,
        json.dumps({"page": ordinal}),
    )


async def write_settlement(
    conn: ConnLike, schema: str, flow_id: JobId, *, page_number: int
) -> None:
    await insert_domain(conn, schema, flow_id, "settled")
    await conn.execute(
        f'INSERT INTO "{schema}".atomicity_history (flow_id,note) VALUES ($1,$2)',
        flow_id,
        "committed settlement",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".atomicity_applied_cursor (flow_id,cursor) VALUES ($1,$2::jsonb) '
        "ON CONFLICT (flow_id) DO UPDATE SET cursor=EXCLUDED.cursor",
        flow_id,
        json.dumps({"page": page_number}),
    )


async def row(conn: ConnLike, schema: str, flow_id: JobId, step_key: str) -> asyncpg.Record:
    found = await conn.fetchrow(
        f"SELECT * FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid=$1 AND step_key=$2",
        flow_id,
        step_key,
    )
    assert found is not None, f"the {step_key!r} row must exist"
    return found


async def node_id(conn: ConnLike, schema: str, flow_id: JobId, step_key: str) -> JobId:
    return JobId((await row(conn, schema, flow_id, step_key))["id"])


async def children(conn: ConnLike, schema: str, source_id: JobId) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"SELECT id,step_key,map_index,status::text AS status,attempt,claim_epoch,required_worker_schema "
        f'FROM "{schema}".jobs WHERE parent_id=$1 ORDER BY map_index',
        source_id,
    )


async def edge_count(conn: ConnLike, schema: str, source_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_edge WHERE parent_id=$1', source_id
        )
    )


async def cursor(conn: ConnLike, schema: str, source_id: JobId) -> object:
    return json_object(
        await conn.fetchval(
            f"SELECT metadata->'emit_cursor' FROM \"{schema}\".jobs WHERE id=$1", source_id
        )
    )


async def barrier_state(conn: ConnLike, schema: str, barrier_id: JobId) -> dict[str, object]:
    found = await conn.fetchrow(
        f"SELECT status::text AS status,deps_pending,metadata,attempt,claim_epoch,"
        f'locked_by_worker,required_worker_schema FROM "{schema}".jobs WHERE id=$1',
        barrier_id,
    )
    assert found is not None
    return {**dict(found), "metadata": json_dict(found["metadata"])}


async def fire_count(conn: ConnLike, schema: str, barrier_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_join_fire WHERE join_job_id=$1', barrier_id
        )
    )


async def row_lock_waiters(conn: ConnLike, schema: str, node_id: JobId) -> set[int]:
    """Return only backends waiting for this exact row's current tuple lock."""
    location = await conn.fetchval(f'SELECT ctid::text FROM "{schema}".jobs WHERE id=$1', node_id)
    assert isinstance(location, str)
    page, ordinal = location.strip("()").split(",")
    xmax_value = await conn.fetchval(f'SELECT xmax::text FROM "{schema}".jobs WHERE id=$1', node_id)
    target_transaction: int | None = None
    if isinstance(xmax_value, str) and xmax_value.isdigit() and int(xmax_value) != 0:
        target_transaction = int(xmax_value)
    records = await conn.fetch(
        "SELECT pid FROM pg_locks WHERE NOT granted AND ("
        "(locktype='tuple' AND relation=$1::regclass AND page=$2::int "
        "AND tuple=$3::int AND mode='RowExclusiveLock') OR "
        "(locktype='transactionid' AND transactionid::text::bigint=$4::bigint))",
        f'"{schema}".jobs',
        int(page),
        int(ordinal),
        target_transaction,
    )
    return {int(record["pid"]) for record in records}


async def wait_event(event: asyncio.Event, label: str, *, bound_s: float = 10.0) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=bound_s)
    except TimeoutError:
        raise AssertionError(f"timed out waiting for {label}") from None


async def wait_until(
    predicate: Callable[[], Awaitable[bool]], *, label: str, bound_s: float = 10.0
) -> None:
    async def poll() -> None:
        wait = asyncio.Event()
        while not await predicate():
            with suppress(TimeoutError):
                await asyncio.wait_for(wait.wait(), timeout=0.01)

    try:
        await asyncio.wait_for(poll(), timeout=bound_s)
    except TimeoutError:
        raise AssertionError(f"timed out waiting for {label}") from None


@asynccontextmanager
async def settlement_connection(ctx: StepContext) -> AsyncGenerator[tuple[ConnLike, bool]]:
    """Yield the claim-owned connection the execution holds and whether the
    execution owns its commit scope (the transactional subtype always does)."""
    supplied: object = getattr(ctx, "connection", None)
    assert isinstance(supplied, (asyncpg.Connection, asyncpg.pool.PoolConnectionProxy)), type(
        supplied
    )
    yield supplied, True


@dataclass(slots=True)
class RealWorker:
    worker_id: JobId
    settings: WorkerSettings
    pool: asyncpg.Pool
    schema: str
    control: asyncpg.Connection
    closed: bool = False

    @classmethod
    async def open(
        cls,
        pool: asyncpg.Pool,
        control: asyncpg.Connection,
        dsn: str,
        schema: str,
    ) -> RealWorker:
        settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema})
        worker_id = JobId(await register_worker(pool, settings, workflow_execution=True))
        try:
            await control.execute(
                f"INSERT INTO \"{schema}\".actor_config (actor,queue) VALUES ('wf','default') "
                "ON CONFLICT (actor) DO NOTHING"
            )
            return cls(worker_id, settings, pool, schema, control)
        except BaseException:
            # Registration already committed: a partial open must not leave a worker row.
            await deregister_worker(pool, settings, worker_id)
            raise

    async def close(self) -> None:
        if self.closed:
            raise RuntimeError(f"worker {self.worker_id} was closed twice")
        await deregister_worker(self.pool, self.settings, self.worker_id)
        self.closed = True
        remains = await self.control.fetchval(
            f'SELECT id FROM "{self.schema}".workers WHERE id=$1', self.worker_id
        )
        assert remains is None, f"worker registration {self.worker_id} survived close"

    async def assert_boot_supports(self, requirement: object) -> None:
        metadata = json_dict(
            await self.control.fetchval(
                f'SELECT metadata FROM "{self.schema}".workers WHERE id=$1', self.worker_id
            )
        )
        boot = metadata.get("workflow_execution_schema")
        assert type(boot) is int
        if requirement is not None:
            assert type(requirement) is int
            assert boot >= requirement, (boot, requirement)

    async def claim(self, expected_key: str) -> dict[str, object]:
        claimed = await dispatch_batch(
            self.control,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=self.schema),
            queues=["default"],
            limit_n=1,
            worker_id=self.worker_id,
            lock_lease=timedelta(seconds=30),
        )
        assert len(claimed) == 1, f"expected one claimable row, got {[dict(r) for r in claimed]!r}"
        result = dict(claimed[0])
        assert result["step_key"] == expected_key
        assert result["status"] == "running"
        persisted = await self.control.fetchrow(
            f"SELECT status::text AS status,locked_by_worker,attempt,claim_epoch,"
            f'required_worker_schema FROM "{self.schema}".jobs WHERE id=$1',
            result["id"],
        )
        assert persisted is not None
        assert persisted["status"] == "running"
        assert persisted["locked_by_worker"] == self.worker_id
        assert persisted["attempt"] == result["attempt"]
        assert persisted["claim_epoch"] == result["claim_epoch"]
        assert persisted["required_worker_schema"] == result["required_worker_schema"]
        await self.assert_boot_supports(result["required_worker_schema"])
        return result

    async def execute_claim(
        self,
        compiled: CompiledWorkflow,
        flow_id: JobId,
        claim: dict[str, object],
        *,
        expect_same_claim: bool = True,
    ) -> str:
        runner = FlowRunner(compiled, self.pool, self.schema, worker_id=self.worker_id)
        verdict = await runner.run_fleet_claimed_step(
            flow_id,
            claim,
            attempt=int(claim["attempt"]),
            claim_epoch=int(claim["claim_epoch"]),
        )
        persisted = await self.control.fetchrow(
            f'SELECT attempt,claim_epoch,required_worker_schema FROM "{self.schema}".jobs WHERE id=$1',
            claim["id"],
        )
        assert persisted is not None
        if expect_same_claim:
            if verdict == "deferred":
                # Dedicated capacity parking refunds to physical attempt zero
                # and preserves the claim epoch: the park's own fence matches
                # the exact current epoch and the next dispatch claim remints
                # it; the owning controls assert the logical carrier after.
                assert persisted["attempt"] == 0
                assert persisted["claim_epoch"] == int(claim["claim_epoch"])
            else:
                assert persisted["attempt"] == claim["attempt"]
                assert persisted["claim_epoch"] == claim["claim_epoch"]
        assert persisted["required_worker_schema"] == claim["required_worker_schema"]
        return verdict

    async def run(self, compiled: CompiledWorkflow, flow_id: JobId, key: str) -> str:
        return await self.execute_claim(compiled, flow_id, await self.claim(key))


async def create_guarded(runner: FlowRunner, *, run_key: str | None = None) -> JobId:
    """Internal guarded-create behavior fixture, not public declaration evidence."""
    claim = await create_guarded_flow(
        runner,
        required_worker_schema=WORKFLOW_EXECUTION_SCHEMA,
        run_key=run_key,
    )
    return claim.flow_id


def two_node_flow(
    name: str,
    source_body: Callable[..., Awaitable[object]],
    settle_body: Callable[..., Awaitable[object]],
    chain: Chain,
) -> tuple[WorkflowApp, CompiledWorkflow]:
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        source = chain_source(chain, source_body, key="source", max_attempts=1)
        barrier = transactional_barrier(settle_body, source, key="settle")
        return build(barrier)

    return app, app.get(name)


def render_direct_claim(schema: str) -> str:
    from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE

    return render_sql(NODE_CLAIM_SQL_TEMPLATE, schema)


def new_label(prefix: str) -> str:
    return f"{prefix}_{new_uuid().hex[:12]}"
